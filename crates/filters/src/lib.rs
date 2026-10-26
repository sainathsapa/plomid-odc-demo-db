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
//! PLOMID 1G: SQL filtering data structures.
//!
//! This crate owns one structure: the [`xor`] XOR filter used for
//! sealed-segment negative pruning on the storage read path
//! (`plomid-storage` builds one per sealed segment, persists it as a
//! sidecar, and skips segments whose filter proves the key absent).
//!
//! # Ownership
//!
//! * Durable membership pruning → [`xor::XorFilter`] here (canonical; the
//!   storage segment sidecar format depends on it).
//! * Ephemeral per-query join pre-filtering → the executor's own
//!   `join::bloom::BloomFilter` (deliberately separate: infallible
//!   construction, no cross-crate dependency on the query hot path).
//! * Exact candidate row-position sets → exact evaluation; no bitmap
//!   structure is owned here (the former Roaring implementation was
//!   removed: zero callers, speculative weight).
//!
//! # XOR filter ([`xor`])
//!
//! A probabilistic membership filter with the following guarantees:
//!
//! - **No false negatives**: if `contains(x)` returns `false`, then `x` is
//!   definitely not in the set.
//! - **Possible false positives**: `contains(x)` may return `true` for an
//!   element that was never inserted; the false-positive rate is ~0.83% with
//!   8-bit fingerprints.
//! - **Controlled construction failure**: construction either succeeds with a
//!   valid filter or returns a controlled error; an invalid filter is never
//!   silently constructed.
//!
//! XOR filters are never authoritative query-result structures. They exist
//! only to cheaply eliminate candidates — a `false` answer from the XOR
//! filter allows a caller to skip expensive exact evaluation, but a `true`
//! answer always falls through to exact processing.
//!
//! # Persistence
//!
//! The filter serializes to a self-describing little-endian binary format
//! with:
//!
//! - a 4-byte magic (`"PLXF"`),
//! - a format version (rejected if unknown),
//! - explicit counts, lengths, and reserved fields,
//! - a CRC32C checksum over the payload,
//! - cross-validated length fields.
//!
//! CRC32C comes directly from the isolated [`crc32c`] component; no second
//! checksum implementation is introduced.
//!
//! # Physical placement
//!
//! Filter metadata belongs to the logical SQL object / generation
//! architecture. Physical bytes are ultimately allocated through the existing
//! storage layer — the directory `PLOMID_DATA/shared/filters/` is managed by
//! the generation / storage manager, and filter objects carry no hardcoded
//! device paths.
//!
//! Do not create a filter-specific WAL, MVCC, checkpoint, or recovery
//! pipeline. Filters are rebuilt from the materialized columns at flush time
//! and are therefore recoverable from the existing generation machinery.
//!
//! The crate must not depend on [`plomid_storage`]: storage segments use these
//! filters for sealed-segment negative pruning, so the dependency runs
//! storage → filters, never the reverse.

#![forbid(unsafe_code)]
// Pedantic/nursery lints stay at workspace policy (casts, pub(crate), and
// const-fn shapes are deliberate project style); only missing-docs remains
// on: public API must stay documented.
#![warn(missing_docs)]

mod constants;
pub mod layout;

pub mod xor;

pub use constants::{
    XOR_ALPHA_DEN, XOR_ALPHA_NUM, XOR_FILTER_FORMAT_VERSION, XOR_FILTER_MAGIC,
    XOR_FINGERPRINT_BITS, XOR_FINGERPRINT_MODULUS, XOR_FOOTER_LEN, XOR_MAX_BUILD_ATTEMPTS,
    XOR_MAX_ELEMENTS, XOR_MAX_SLOTS, XOR_SLOTS_SLACK,
};
pub use xor::{XorError, XorFilter};

/// CRC32C convenience for callers that verify filter images externally.
#[must_use]
pub fn compute_checksum(bytes: &[u8]) -> u32 {
    crc32c::compute(bytes)
}

/// Verifies `bytes` against an expected CRC32C checksum.
///
/// # Errors
///
/// Returns [`XorError::Corrupt`] when the computed checksum differs from
/// `expected`.
pub fn verify_checksum(bytes: &[u8], expected: u32) -> Result<(), XorError> {
    crc32c::verify(bytes, expected).map_err(|_| XorError::Corrupt("checksum mismatch"))
}
