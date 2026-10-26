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
//! Delta + bit-packed encoding for fixed-width 64-bit integer columns.
//!
//! Time-series and analytical integer columns are dominated by one of two
//! shapes: near-monotonic sequences (timestamps, sequence numbers, keys) and
//! small-valued repeats. Both collapse under delta-then-pack, and a time series
//! sampled on a regular interval collapses completely: the encoding stores the
//! first value, the smallest delta in the chunk, and then bit-packs each
//! `delta - min_delta` at the narrowest width the chunk needs. A constant step
//! therefore costs zero bits per row and a varying-but-narrow step costs only
//! its variation.
//!
//! ```text
//! body: width[u8]                    bits per packed delta, 0..=64
//!       min_delta[i64 little-endian]
//!       first[i64 little-endian]     both present only when value_count > 0
//!       packed (delta - min_delta)   (value_count - 1) * width bits, LSB-first
//! ```
//!
//! The encoding applies only when every value entry is exactly eight bytes wide,
//! which is precisely the persisted form of an integer column. Every value is
//! treated as an `i64` bit pattern and all delta arithmetic wraps, so
//! `i64::MIN`/`i64::MAX` and arbitrary bit patterns round-trip exactly rather
//! than being rejected or saturating.

use crate::encoding::bitpack::{packed_len, width_for_max, BitReader, BitWriter};
use crate::encoding::entries::{append_entry, entry_len, EntryReader};
use crate::layout::{corruption, get_u64, to_usize};
use plomid_core::Result;

/// Fixed width of an encoded integer value, mirroring the persisted form.
pub const INTEGER_WIDTH: usize = 8;
/// Body header: the bit width byte, the minimum delta, and the first value.
const HEADER_LEN: usize = 1 + INTEGER_WIDTH + INTEGER_WIDTH;

/// Encodes an integer value stream, or `None` when it is not fixed-width.
pub fn encode(raw: &[u8], value_count: u64) -> Result<Option<Vec<u8>>> {
    let mut reader = EntryReader::new(raw, value_count);
    let mut first: Option<i64> = None;
    let mut previous = 0_i64;
    let mut deltas: Vec<i64> = Vec::new();
    while let Some(value) = reader.next_entry()? {
        let array: [u8; INTEGER_WIDTH] = match value.try_into() {
            Ok(array) => array,
            Err(_) => return Ok(None),
        };
        let current = i64::from_le_bytes(array);
        match first {
            None => first = Some(current),
            Some(_) => deltas.push(current.wrapping_sub(previous)),
        }
        previous = current;
    }
    reader.finish()?;
    let Some(first) = first else {
        // An empty chunk carries only the width byte: there is no value to lead
        // with and no delta to pack.
        return Ok(Some(vec![0_u8]));
    };

    // Smallest delta, so a regular series packs to zero bits per row.
    let min_delta = deltas.iter().copied().min().unwrap_or(0);
    let mut width_max = 0_u64;
    for delta in &deltas {
        width_max = width_max.max((*delta as u64).wrapping_sub(min_delta as u64));
    }
    let width = width_for_max(width_max);

    let packed_bits = packed_len(deltas.len() * width as usize);
    let mut body = Vec::with_capacity(HEADER_LEN + packed_bits);
    body.push(width as u8);
    body.extend_from_slice(&min_delta.to_le_bytes());
    body.extend_from_slice(&first.to_le_bytes());
    let mut writer = BitWriter::with_capacity(packed_bits);
    for delta in deltas {
        writer.write((delta as u64).wrapping_sub(min_delta as u64), width);
    }
    body.extend_from_slice(&writer.finish());
    Ok(Some(body))
}

/// Expands a delta body back into the original value stream.
pub fn decode(body: &[u8], value_count: u64, raw_len: u64) -> Result<Vec<u8>> {
    let expected_raw = entry_len(INTEGER_WIDTH)?
        .checked_mul(to_usize(value_count, "value count")?)
        .ok_or_else(|| corruption("integer chunk length overflows"))?;
    if expected_raw as u64 != raw_len {
        return Err(corruption(format!(
            "integer chunk declares {raw_len} raw bytes but {value_count} fixed-width values need {expected_raw}"
        )));
    }
    if value_count == 0 {
        if body != [0_u8] {
            return Err(corruption("empty integer chunk body is malformed"));
        }
        return Ok(Vec::new());
    }

    let width = u32::from(
        *body
            .first()
            .ok_or_else(|| corruption("delta body is empty but the chunk declares values"))?,
    );
    if width > u64::BITS {
        return Err(corruption(format!("delta bit width {width} exceeds 64")));
    }
    let min_delta = get_u64(body, 1, "delta minimum")? as i64;
    let first = get_u64(body, 1 + INTEGER_WIDTH, "delta first value")? as i64;
    let delta_count = value_count - 1;
    let expected_packed = packed_len(
        to_usize(delta_count, "delta count")?
            .checked_mul(width as usize)
            .ok_or_else(|| corruption("delta bit length overflows"))?,
    );
    if body.len() != HEADER_LEN + expected_packed {
        return Err(corruption(format!(
            "delta body is {} bytes but {delta_count} deltas at {width} bits need {}",
            body.len(),
            HEADER_LEN + expected_packed
        )));
    }

    let mut out = Vec::with_capacity(expected_raw);
    let mut previous = first;
    append_entry(&mut out, &previous.to_le_bytes())?;
    let mut reader = BitReader::new(&body[HEADER_LEN..]);
    for _ in 0..delta_count {
        let delta = min_delta.wrapping_add(reader.read(width)? as i64);
        previous = previous.wrapping_add(delta);
        append_entry(&mut out, &previous.to_le_bytes())?;
    }
    if reader.consumed() != expected_packed {
        return Err(corruption("packed deltas are not fully consumed"));
    }
    Ok(out)
}
