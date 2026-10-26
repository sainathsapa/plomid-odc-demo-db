//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID — Platform for Modern Intelligence and Data
//
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0;
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! SCRAM-SHA-256 server-side authentication (RFC 5802 / RFC 7677).
//!
//! PostgreSQL 10+ clients negotiate SCRAM-SHA-256 during startup. This module
//! implements the server half of the exchange on top of the audited
//! primitives in `plomid-security::credential` (`sha2`, `hmac`, `pbkdf2`,
//! `subtle`, `base64`) — no hand-rolled cryptography here since Phase 3.
//!
//! # Wire flow
//!
//! ```text
//! server: AuthenticationSASL (mechanism list)
//! client: SASLInitialResponse (mechanism + "n,,n=user,r=clientnonce")
//! server: AuthenticationSASLContinue ("r=...,s=...,i=...")
//! client: SASLResponse ("c=...,r=...,p=...")
//! server: AuthenticationSASLFinal ("v=...") then AuthenticationOk
//! ```
//!
//! No plaintext password is ever written to the wire or to logs.

/// Password primitives (SHA-256, HMAC-SHA-256, PBKDF2, Base64).
///
/// Moved to `plomid-security::credential` (Beta 2 Phase 2) so the catalog,
/// executor, and wire layers share one derivation. Re-exported here so
/// existing `crate::scram::` call sites — including integration tests —
/// keep working unchanged. Phase 3 swaps the implementations for audited
/// crates behind these signatures.
pub use plomid_security::credential::{
    base64_decode, base64_encode, hmac_sha256, pbkdf2_sha256, sha256,
};

/// Errors raised while parsing or verifying a SCRAM exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramError {
    /// The client message could not be parsed.
    Malformed,
    /// The client proof did not match the expected proof.
    InvalidProof,
    /// The client supplied a nonce that does not match the server nonce.
    NonceMismatch,
    /// The OS CSPRNG was unavailable, so no fresh nonce could be minted.
    /// Fail closed: the exchange must not continue on predictable material.
    SaltUnavailable,
}

impl std::fmt::Display for ScramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Malformed => write!(f, "malformed SCRAM message"),
            Self::InvalidProof => write!(f, "invalid SCRAM proof"),
            Self::NonceMismatch => write!(f, "SCRAM nonce mismatch"),
            Self::SaltUnavailable => write!(f, "SCRAM unavailable: no secure randomness"),
        }
    }
}

/// Parses a comma-separated `name=value` attribute list into an ordered vec.
pub fn parse_attributes(message: &str) -> Vec<(&str, &str)> {
    message
        .split(',')
        .filter_map(|part| part.split_once('='))
        .collect()
}
/// The server-side state of a single SCRAM-SHA-256 authentication exchange.
///
/// A fresh instance is created per authentication attempt, so no state leaks
/// between connections.
pub struct ScramSession {
    password: String,
    salt: [u8; 16],
    iterations: u32,
    client_first_bare: String,
    server_first: String,
    server_nonce: String,
    salted_password: Option<[u8; 32]>,
}

impl ScramSession {
    /// Creates a new server-side SCRAM exchange for `password`.
    ///
    /// The salt comes from the OS CSPRNG (`random_salt`, fail closed):
    /// predictable time-mixed salts must never seed an exchange.
    pub fn new(password: &str) -> Result<Self, plomid_security::credential::SaltError> {
        let salt = plomid_security::credential::random_salt()?;
        Ok(Self {
            password: password.to_owned(),
            salt,
            iterations: 4096,
            client_first_bare: String::new(),
            server_first: String::new(),
            server_nonce: String::new(),
            salted_password: None,
        })
    }

    pub fn with_verifier(password: &str, salt: [u8; 16], salted_password: &[u8]) -> Self {
        Self {
            password: password.to_owned(),
            salt,
            iterations: 4096,
            client_first_bare: String::new(),
            server_first: String::new(),
            server_nonce: String::new(),
            salted_password: salted_password.try_into().ok(),
        }
    }

    /// Builds an exchange from a stored catalog verifier (no plaintext).
    ///
    /// The advertised iteration count comes from the credential so the
    /// client derives with the same parameters the verifier was created
    /// with; verification uses the stored verifier directly.
    pub fn with_stored_verifier(salt: [u8; 16], iterations: u32, salted_password: &[u8]) -> Self {
        Self {
            password: String::new(),
            salt,
            iterations: iterations.max(1),
            client_first_bare: String::new(),
            server_first: String::new(),
            server_nonce: String::new(),
            salted_password: salted_password.try_into().ok(),
        }
    }

    /// Processes the SASLInitialResponse payload (the `n,,n=user,r=nonce`
    /// client-first message) and returns the server-first message.
    pub fn handle_initial(&mut self, client_first: &str) -> Result<String, ScramError> {
        // Strip the gs2 header. The header is `flag,authzid,`; with no
        // authzid the client sends `n,,`. Splitting on commas and rejoining
        // everything after the second field yields the client-first-bare.
        let parts = client_first.split(',').collect::<Vec<_>>();
        if parts.len() < 3 {
            return Err(ScramError::Malformed);
        }
        let bare = parts[2..].join(",");
        let attrs = parse_attributes(&bare);
        let client_nonce = attrs
            .iter()
            .find(|(name, _)| *name == "r")
            .map(|(_, value)| *value)
            .ok_or(ScramError::Malformed)?;
        if client_nonce.is_empty() {
            return Err(ScramError::Malformed);
        }

        // Server nonce = client nonce + server-generated suffix. The
        // suffix is 128 bits from the OS CSPRNG (hex): wall-clock
        // nanoseconds are predictable to a network observer and must
        // never seed a nonce. CSPRNG failure fails the exchange closed.
        let suffix_bytes =
            plomid_security::credential::random_salt().map_err(|_| ScramError::SaltUnavailable)?;
        let mut suffix = String::with_capacity(32);
        for byte in suffix_bytes {
            use std::fmt::Write as _;
            let _ = write!(suffix, "{byte:02x}");
        }
        let server_nonce = format!("{client_nonce}{suffix}");

        self.client_first_bare = bare.to_owned();
        self.server_nonce = server_nonce.clone();
        self.server_first = format!(
            "r={},s={},i={}",
            server_nonce,
            base64_encode(&self.salt),
            self.iterations
        );
        Ok(self.server_first.clone())
    }

    /// Processes the SASLResponse payload (the `c=...,r=...,p=...` client-final
    /// message) and returns the server-final message (`v=...`) on success.
    pub fn handle_final(&self, client_final: &str) -> Result<String, ScramError> {
        let attrs = parse_attributes(client_final);
        let client_nonce = attrs
            .iter()
            .find(|(name, _)| *name == "r")
            .map(|(_, value)| *value)
            .ok_or(ScramError::Malformed)?;
        if client_nonce != self.server_nonce {
            return Err(ScramError::NonceMismatch);
        }
        let client_proof_b64 = attrs
            .iter()
            .find(|(name, _)| *name == "p")
            .map(|(_, value)| *value)
            .ok_or(ScramError::Malformed)?;
        let client_proof = base64_decode(client_proof_b64).ok_or(ScramError::Malformed)?;

        // client-final-without-proof = everything up to the ",p=" marker.
        let without_proof = client_final
            .split(",p=")
            .next()
            .ok_or(ScramError::Malformed)?;

        let salted_password = self.salted_password.unwrap_or_else(|| {
            pbkdf2_sha256(self.password.as_bytes(), &self.salt, self.iterations, 32)
                .try_into()
                .expect("PBKDF2-SHA-256 must produce 32 bytes")
        });
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);

        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut expected_proof = [0u8; 32];
        for i in 0..32 {
            expected_proof[i] = client_key[i] ^ client_signature[i];
        }
        // Constant-time proof comparison: a short-circuit `==` would let a
        // network observer distinguish near-miss proofs by timing.
        if !plomid_security::credential::constant_time_eq(&expected_proof, &client_proof) {
            return Err(ScramError::InvalidProof);
        }

        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let server_signature = hmac_sha256(&server_key, auth_message.as_bytes());
        Ok(format!("v={}", base64_encode(&server_signature)))
    }
}
