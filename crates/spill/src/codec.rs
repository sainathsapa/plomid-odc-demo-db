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

//! Row codecs for spilled data.
//!
//! Row values are opaque: operators are generic over `T: SpillEncode`, and
//! join payloads are opaque `Vec<u8>` encoded rows. This crate deliberately
//! does NOT invent BLOB/vector data-model types; it only frames bytes.
//!
//! [`SpillEncode`] has small blanket impls for the scalar models that exist
//! (`u64`, `i64`, `u32`, `i32`, `String`, opaque `Vec<u8>`). Engine types can
//! implement it by delegating to their existing row encoding.

use crate::error::SpillError;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Encode/decode contract for a spillable value.
///
/// `encode` must be deterministic; `decode(encode(v))` must round-trip.
pub trait SpillEncode: Sized {
    /// Serialize to bytes (without the record frame prefix).
    fn encode(&self) -> Vec<u8>;
    /// Deserialize from bytes produced by [`SpillEncode::encode`].
    fn decode(bytes: &[u8]) -> Result<Self, SpillError>;
}

impl SpillEncode for u64 {
    fn encode(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Result<Self, SpillError> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| SpillError::Decode("u64 needs 8 bytes".to_string()))?;
        Ok(Self::from_le_bytes(arr))
    }
}

impl SpillEncode for i64 {
    fn encode(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Result<Self, SpillError> {
        let arr: [u8; 8] = bytes
            .try_into()
            .map_err(|_| SpillError::Decode("i64 needs 8 bytes".to_string()))?;
        Ok(Self::from_le_bytes(arr))
    }
}

impl SpillEncode for u32 {
    fn encode(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Result<Self, SpillError> {
        let arr: [u8; 4] = bytes
            .try_into()
            .map_err(|_| SpillError::Decode("u32 needs 4 bytes".to_string()))?;
        Ok(Self::from_le_bytes(arr))
    }
}

impl SpillEncode for i32 {
    fn encode(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
    fn decode(bytes: &[u8]) -> Result<Self, SpillError> {
        let arr: [u8; 4] = bytes
            .try_into()
            .map_err(|_| SpillError::Decode("i32 needs 4 bytes".to_string()))?;
        Ok(Self::from_le_bytes(arr))
    }
}

impl SpillEncode for String {
    fn encode(&self) -> Vec<u8> {
        let b = self.as_bytes();
        let mut out = Vec::with_capacity(4 + b.len());
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b);
        out
    }
    fn decode(bytes: &[u8]) -> Result<Self, SpillError> {
        let (len, rest) = split_prefix_u32(bytes)?;
        if rest.len() < len {
            return Err(SpillError::Decode("string payload truncated".to_string()));
        }
        String::from_utf8(rest[..len].to_vec())
            .map_err(|e| SpillError::Decode(format!("string not utf-8: {e}")))
    }
}

/// Opaque encoded row: identity codec.
impl SpillEncode for Vec<u8> {
    fn encode(&self) -> Vec<u8> {
        self.clone()
    }
    fn decode(bytes: &[u8]) -> Result<Self, SpillError> {
        Ok(bytes.to_vec())
    }
}

/// Hash-partition a value into `0..n`. Used by grace join/agg/distinct/set-op
/// so both sides agree on partition assignment.
pub fn partition_for<T: Hash + ?Sized>(v: &T, n: usize) -> usize {
    debug_assert!(n > 0);
    if n == 0 {
        return 0;
    }
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    (h.finish() % n as u64) as usize
}

/// Split a leading little-endian `u32` length prefix.
pub fn split_prefix_u32(bytes: &[u8]) -> Result<(usize, &[u8]), SpillError> {
    if bytes.len() < 4 {
        return Err(SpillError::Decode("missing u32 prefix".to_string()));
    }
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&bytes[..4]);
    Ok((u32::from_le_bytes(arr) as usize, &bytes[4..]))
}

/// Read a little-endian `u64` at an offset with bounds checking.
pub fn read_u64_le(bytes: &[u8], off: usize) -> Result<u64, SpillError> {
    if bytes.len() < off + 8 {
        return Err(SpillError::Decode("u64 out of bounds".to_string()));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[off..off + 8]);
    Ok(u64::from_le_bytes(arr))
}

/// Read a little-endian `i64` at an offset with bounds checking.
pub fn read_i64_le(bytes: &[u8], off: usize) -> Result<i64, SpillError> {
    if bytes.len() < off + 8 {
        return Err(SpillError::Decode("i64 out of bounds".to_string()));
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[off..off + 8]);
    Ok(i64::from_le_bytes(arr))
}
