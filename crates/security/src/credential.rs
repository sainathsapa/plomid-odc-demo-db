//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID — Platform for Modern Intelligence and Data
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0.

//! Password credential derivation and verification (audited primitives).
//!
//! Beta 2 Phase 3 replaced every hand-rolled primitive here with
//! maintained, audited RustCrypto crates behind IDENTICAL signatures, so
//! no caller changed: `sha2` (SHA-256), `hmac` (HMAC-SHA-256), `pbkdf2`
//! (PBKDF2-HMAC-SHA-256), `subtle` (constant-time equality), `rand_core`
//! `OsRng` (CSPRNG salts — no fallback), `base64` (RFC 4648), `md-5`
//! (legacy-MD5 response path only).
//!
//! Do not add hand-rolled cryptography to this module. Ever.
//!
//! What this module guarantees:
//!
//! * passwords are never stored: only `(salt, iterations, verifier)`
//!   tuples persist in the catalog ([`RoleCredential`]);
//! * every secret comparison runs in constant time via `subtle`
//!   ([`constant_time_eq`]);
//! * salts/nonces come ONLY from the OS CSPRNG; exhaustion returns
//!   [`SaltError`] and every caller fails closed (no weak fallback —
//!   the old time/pid/counter fallback was deleted, not kept).
//!
//! Compliance note: audited primitives are necessary security evidence
//! but do NOT constitute FIPS 140-3 validation. A FIPS claim requires a
//! validated module boundary plus external validation — neither is
//! claimed here.

use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// SCRAM iteration count for newly derived verifiers (matches the
/// historical server default; stored per-credential so a future
/// increase does not invalidate old verifiers).
pub const SCRAM_ITERATIONS: u32 = 4096;
/// Derived verifier length in bytes (SHA-256 output).
pub const VERIFIER_LEN: usize = 32;

/// CSPRNG exhaustion. Returned — never worked around — whenever OS
/// randomness is unavailable, so callers fail closed instead of
/// generating predictable salts/nonces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaltError(String);

impl SaltError {
    /// Reports OS randomness exhaustion as a [`SaltError`].
    pub fn os_unavailable(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for SaltError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CSPRNG unavailable for salt/nonce generation, refusing to continue: {}",
            self.0
        )
    }
}

impl std::error::Error for SaltError {}

/// Computes the SHA-256 digest of `data` (`sha2` crate).
pub fn sha256(data: &[u8]) -> [u8; 32] {
    Sha256::digest(data).into()
}

/// HMAC-SHA-256 (RFC 2104) over `data` with `key` (`hmac` crate).
pub fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// PBKDF2-HMAC-SHA-256 (RFC 8018) producing `dk_len` bytes (`pbkdf2` crate).
pub fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32, dk_len: usize) -> Vec<u8> {
    let mut out = vec![0u8; dk_len];
    pbkdf2::pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out);
    out
}

/// Standard Base64 encoding (RFC 4648), used for SCRAM attribute values.
pub fn base64_encode(data: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(data)
}

/// Standard Base64 decoding (RFC 4648). Returns `None` on invalid input.
pub fn base64_decode(input: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::STANDARD
        .decode(input.trim())
        .ok()
}

/// Constant-time byte equality for secret comparison (`subtle` crate).
///
/// Length mismatch returns false without comparing (lengths of derived
/// keys are fixed at 32 bytes, so no oracle exists on the hot path;
/// legacy variable-length compares are deprecated along with MD5).
pub fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    use subtle::ConstantTimeEq as _;
    left.ct_eq(right).unwrap_u8() == 1
}

/// Computes the PostgreSQL MD5 auth response for a plaintext secret.
///
/// Legacy path only (bootstrap + pre-Phase-2 roles): implemented with the
/// maintained `md-5` crate, not hand-rolled. New code must use SCRAM
/// verifiers instead; see the MD5 deprecation plan.
pub fn md5_password_response(password: &str, user: &str, salt: [u8; 4]) -> String {
    use md5::{Digest as _, Md5};
    let inner = Md5::digest(format!("{password}{user}").as_bytes());
    let mut hasher = Md5::new();
    hasher.update(format!("{:x}", inner));
    hasher.update(salt);
    format!("md5{:x}", hasher.finalize())
}

/// A stored password verifier: everything the catalog persists for
/// password authentication. The password itself never persists anywhere.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleCredential {
    pub salt: [u8; 16],
    pub iterations: u32,
    pub verifier: Vec<u8>,
}

impl RoleCredential {
    /// Derives a fresh verifier for `password` with a caller-supplied salt.
    pub fn derive(password: &[u8], salt: [u8; 16]) -> Self {
        Self::derive_with_iterations(password, salt, SCRAM_ITERATIONS)
    }

    pub fn derive_with_iterations(password: &[u8], salt: [u8; 16], iterations: u32) -> Self {
        Self {
            salt,
            iterations,
            verifier: pbkdf2_sha256(password, &salt, iterations, VERIFIER_LEN),
        }
    }

    /// Verifies a candidate password in constant time.
    pub fn verify(&self, password: &[u8]) -> bool {
        let candidate = pbkdf2_sha256(password, &self.salt, self.iterations, self.verifier.len());
        constant_time_eq(&candidate, &self.verifier)
    }

    /// Compact persisted form: `iterations:salt_b64:verifier_b64`.
    pub fn encode(&self) -> String {
        format!(
            "{}:{}:{}",
            self.iterations,
            base64_encode(&self.salt),
            base64_encode(&self.verifier)
        )
    }

    pub fn decode(text: &str) -> Option<Self> {
        let (iterations, rest) = text.split_once(':')?;
        let (salt_b64, verifier_b64) = rest.split_once(':')?;
        let iterations = iterations.parse::<u32>().ok()?;
        if !(1..=1_000_000).contains(&iterations) {
            return None;
        }
        let salt = base64_decode(salt_b64)?;
        let verifier = base64_decode(verifier_b64)?;
        if salt.len() != 16 || verifier.is_empty() || verifier.len() > 128 {
            return None;
        }
        let mut salt_arr = [0u8; 16];
        salt_arr.copy_from_slice(&salt);
        Some(Self {
            salt: salt_arr,
            iterations,
            verifier,
        })
    }
}

/// Random per-credential salt from OS entropy ONLY.
///
/// Fail-closed: any CSPRNG error returns [`SaltError`] — there is no
/// time/pid/counter fallback (deleted in Phase 3). Callers propagate
/// with `?`; startup paths exit non-zero, wire paths close the connection.
pub fn random_salt() -> Result<[u8; 16], SaltError> {
    use rand_core::{OsRng, TryRngCore as _};
    let mut salt = [0u8; 16];
    OsRng
        .try_fill_bytes(&mut salt)
        .map_err(|e| SaltError::os_unavailable(e.to_string()))?;
    if salt == [0u8; 16] {
        // Paranoia against a broken RNG that "succeeds" with zeros: a
        // zero salt is a fail-closed error, never a usable salt.
        return Err(SaltError::os_unavailable("CSPRNG returned all zeros"));
    }
    Ok(salt)
}
