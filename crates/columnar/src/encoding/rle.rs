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
//! Run-length encoding over the value stream.
//!
//! A column of few distinct, long-adjacent values — status codes, tag names,
//! sorted time-series keys, and any column with long NULL runs — collapses to
//! one record per run. Nothing about NULL state is stored here: a NULL row is
//! simply a zero-length entry, and the column's null bitmap remains
//! authoritative.
//!
//! ```text
//! body: run_count[u64]
//!       run_count * { run_len[u64], value_len[u32], value[value_len] }
//! ```
//!
//! Every field is little-endian. A run is refused if its length is zero, if the
//! run lengths do not sum to exactly the chunk's row count, or if the expanded
//! byte count does not equal the declared value-stream length, so a malformed
//! body can never steer the decoder into a wrong-sized or oversized allocation.

use crate::encoding::entries::{append_entry, entry_len, EntryReader};
use crate::layout::{corruption, get_u32, get_u64, put_u64, to_usize};
use plomid_core::Result;

/// Fixed part of the body: the run count.
const HEADER_LEN: usize = 8;
/// Fixed part of one run record, excluding its value bytes.
const RUN_HEADER_LEN: usize = 12;

/// One run: how many rows repeat the same entry, and the entry's bytes.
struct Run<'a> {
    count: u64,
    value: &'a [u8],
}

/// Encodes `raw` as runs.
pub fn encode(raw: &[u8], value_count: u64) -> Result<Option<Vec<u8>>> {
    let mut reader = EntryReader::new(raw, value_count);
    let mut body = Vec::with_capacity(HEADER_LEN);
    body.extend_from_slice(&0_u64.to_le_bytes());

    let mut run_count = 0_u64;
    let mut current: Option<(&[u8], u64)> = None;
    while let Some(value) = reader.next_entry()? {
        match current {
            Some((previous, count)) if previous == value => {
                current = Some((previous, count + 1));
            }
            Some((previous, count)) => {
                write_run(&mut body, previous, count)?;
                run_count += 1;
                current = Some((value, 1));
            }
            None => current = Some((value, 1)),
        }
    }
    if let Some((value, count)) = current {
        write_run(&mut body, value, count)?;
        run_count += 1;
    }
    reader.finish()?;
    put_u64(&mut body, 0, run_count);
    Ok(Some(body))
}

/// Appends one run record.
fn write_run(body: &mut Vec<u8>, value: &[u8], count: u64) -> Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| corruption("run value exceeds the maximum encodable length"))?;
    body.extend_from_slice(&count.to_le_bytes());
    body.extend_from_slice(&length.to_le_bytes());
    body.extend_from_slice(value);
    Ok(())
}

/// Expands a run body back into the original value stream.
pub fn decode(body: &[u8], value_count: u64, raw_len: u64) -> Result<Vec<u8>> {
    let run_count = get_u64(body, 0, "run count")?;
    let mut runs: Vec<Run<'_>> = Vec::new();
    let mut cursor = HEADER_LEN;
    let mut rows = 0_u64;
    for index in 0..run_count {
        let count = get_u64(body, cursor, "run length")?;
        let length = get_u32(body, cursor + 8, "run value length")?;
        cursor = cursor
            .checked_add(RUN_HEADER_LEN)
            .ok_or_else(|| corruption("run record overflows the body"))?;
        if count == 0 {
            return Err(corruption(format!("run {index} has a zero length")));
        }
        let end = cursor
            .checked_add(length as usize)
            .ok_or_else(|| corruption("run value overflows the body"))?;
        let value = body
            .get(cursor..end)
            .ok_or_else(|| corruption("run value lies outside the body"))?;
        cursor = end;
        rows = rows
            .checked_add(count)
            .ok_or_else(|| corruption("run lengths overflow"))?;
        if rows > value_count {
            return Err(corruption(
                "run lengths cover more rows than the chunk declares",
            ));
        }
        runs.push(Run { count, value });
    }
    if cursor != body.len() {
        return Err(corruption("run body holds trailing bytes"));
    }
    if rows != value_count {
        return Err(corruption(format!(
            "run lengths cover {rows} rows but the chunk declares {value_count}"
        )));
    }

    // Size the output from the run records themselves, then require that exact
    // length to match the framing. The allocation is therefore known-good
    // before a byte is written into it.
    let mut total = 0_usize;
    for run in &runs {
        let expanded = entry_len(run.value.len())?
            .checked_mul(to_usize(run.count, "run length")?)
            .ok_or_else(|| corruption("run expansion overflows"))?;
        total = total
            .checked_add(expanded)
            .ok_or_else(|| corruption("run expansion overflows"))?;
    }
    if total as u64 != raw_len {
        return Err(corruption(format!(
            "run body expands to {total} bytes but the chunk declares {raw_len}"
        )));
    }

    let mut out = Vec::with_capacity(total);
    for run in &runs {
        for _ in 0..run.count {
            append_entry(&mut out, run.value)?;
        }
    }
    Ok(out)
}
