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
//! Deterministic little-endian bit packing.
//!
//! Bits fill the output least-significant-bit first: bit `i` of the packed
//! stream lives at byte `i / 8`, bit `i % 8`. That single rule is what makes the
//! encoding reproducible across machines and is asserted by the round-trip
//! tests.
//!
//! The reader is fully bounds-checked: every read either succeeds or reports a
//! structured corruption error, so a truncated or malformed packed region can
//! never panic or read past its buffer.

use crate::layout::corruption;
use plomid_core::Result;

/// Returns the number of bytes needed to hold `bits` bits.
#[must_use]
pub fn packed_len(bits: usize) -> usize {
    bits.div_ceil(8)
}

/// Returns the bits required to represent every value in `0..=max`, or `0` when
/// `max == 0` (a single distinct value needs no index bits at all).
#[must_use]
pub fn width_for_max(max: u64) -> u32 {
    if max == 0 {
        0
    } else {
        u64::BITS - max.leading_zeros()
    }
}

/// Appends fixed-width values to a little-endian, LSB-first bit stream.
#[derive(Debug, Default)]
pub struct BitWriter {
    out: Vec<u8>,
    current: u8,
    used: u32,
}

impl BitWriter {
    /// Creates an empty writer.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    /// Creates a writer that will hold `bytes` packed bytes before reallocating.
    #[must_use]
    pub fn with_capacity(bytes: usize) -> Self {
        Self {
            out: Vec::with_capacity(bytes),
            current: 0,
            used: 0,
        }
    }

    /// Appends the low `width` bits of `value`.
    ///
    /// `width` must not exceed 64. Higher bits of `value` are ignored, so a
    /// caller that over-supplies a value still writes the declared field width.
    pub fn write(&mut self, value: u64, width: u32) {
        debug_assert!(width <= u64::BITS);
        let mut remaining = width;
        let mut rest = value;
        while remaining > 0 {
            let take = (8 - self.used).min(remaining);
            let mask = (1_u64 << take) - 1;
            let bits = (rest & mask) as u8;
            self.current |= bits << self.used;
            self.used += take;
            rest >>= take;
            remaining -= take;
            if self.used == 8 {
                self.out.push(self.current);
                self.current = 0;
                self.used = 0;
            }
        }
    }

    /// Flushes the trailing partial byte and returns the packed stream.
    #[must_use]
    pub fn finish(mut self) -> Vec<u8> {
        if self.used > 0 {
            self.out.push(self.current);
        }
        self.out
    }
}

/// Reads fixed-width values from a little-endian, LSB-first bit stream.
#[derive(Debug)]
pub struct BitReader<'a> {
    bytes: &'a [u8],
    position: usize,
    used: u32,
}

impl<'a> BitReader<'a> {
    /// Creates a reader over `bytes`.
    #[must_use]
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            position: 0,
            used: 0,
        }
    }

    /// Reads the next `width` bits, rejecting a stream that ends early.
    pub fn read(&mut self, width: u32) -> Result<u64> {
        let mut remaining = width;
        let mut shift = 0_u32;
        let mut out = 0_u64;
        while remaining > 0 {
            let available = 8 - self.used;
            if available == 0 {
                self.position += 1;
                self.used = 0;
                continue;
            }
            let byte = *self
                .bytes
                .get(self.position)
                .ok_or_else(|| corruption("packed bit stream is truncated"))?;
            let take = available.min(remaining);
            let mask = ((1_u16 << take) - 1) as u8;
            let bits = u64::from((byte >> self.used) & mask);
            out |= bits << shift;
            shift += take;
            self.used += take;
            remaining -= take;
        }
        Ok(out)
    }

    /// Returns the number of whole bytes consumed so far.
    #[must_use]
    pub fn consumed(&self) -> usize {
        if self.used == 0 {
            self.position
        } else {
            self.position + 1
        }
    }
}
