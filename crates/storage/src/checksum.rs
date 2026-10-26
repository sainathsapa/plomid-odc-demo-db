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
//! CRC32C checksums for persisted pages and WAL records.
//!
//! Thin, allocation-free facade over the isolated [`crc32c`] component. The
//! storage crate never touches CPU-specific code: `#![forbid(unsafe_code)]`
//! holds for this crate, and the hardware/software selection, tables, and
//! intrinsics live entirely inside `crates/crc32c`.
//!
//! Algorithm, polynomial, width, initialization, and finalization are
//! unchanged from the previous in-crate implementation, so persisted page,
//! block, pack, and WAL checksums remain bit-for-bit compatible.

#![forbid(unsafe_code)]

use plomid_core::{ErrorKind, PlomidError, Result};

/// Computes the CRC32C checksum of `bytes`.
#[must_use]
pub fn compute_checksum(bytes: &[u8]) -> u32 {
    crc32c::compute(bytes)
}

/// Computes the CRC32C checksum of `bytes`.
#[must_use]
pub fn compute(bytes: &[u8]) -> u32 {
    crc32c::compute(bytes)
}

/// Returns the running CRC32C state initial value.
#[must_use]
pub fn crc_init() -> u32 {
    crc32c::crc_init()
}

/// Advances `crc` across `bytes` and returns the new running state.
#[must_use]
pub fn crc_update(crc: u32, bytes: &[u8]) -> u32 {
    crc32c::crc_update(crc, bytes)
}

/// Produces the final checksum from a running state.
#[must_use]
pub fn crc_finalize(crc: u32) -> u32 {
    crc32c::crc_finalize(crc)
}

/// Verifies that `bytes` has the expected CRC32C checksum.
pub fn verify_checksum(bytes: &[u8], expected: u32) -> Result<()> {
    crc32c::verify(bytes, expected).map_err(|mismatch| {
        PlomidError::new(
            ErrorKind::Corruption,
            format!(
                "checksum mismatch (expected {expected:#010x}, got {actual:#010x})",
                actual = mismatch.actual
            ),
        )
    })
}

/// Verifies that `bytes` has the expected CRC32C checksum.
pub fn verify(bytes: &[u8], expected: u32) -> Result<()> {
    verify_checksum(bytes, expected)
}
