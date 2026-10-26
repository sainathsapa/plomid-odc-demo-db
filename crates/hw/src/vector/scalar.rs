#![forbid(unsafe_code)]
//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0;
//
// Portable scalar fallback. This module re-forbids `unsafe` (see crate
// docs): it must compile and run correctly on every target, including
// ones with no SIMD backend at all. It is also the bit-identity
// reference: every SIMD backend is tested against these functions,
// and the f64 path uses the same canonical 4-lane order as the
// vectorized backends (never the naive sequential fold, which would
// round differently).

use super::finish_f64_lanes;
use crate::profile::VectorIsa;

/// Lane count of the canonical f64 reduction (mirrors the backends).
pub const LANES: usize = VectorIsa::F64_LANES;

/// Wrapping i64 sum.
#[must_use]
pub fn sum_i64(values: &[i64]) -> i64 {
    let mut acc = [0i64; LANES];
    let mut chunks = values.chunks_exact(LANES);
    for block in &mut chunks {
        for (lane, value) in acc.iter_mut().zip(block.iter()) {
            *lane = lane.wrapping_add(*value);
        }
    }
    let mut total = 0i64;
    for lane in acc {
        total = total.wrapping_add(lane);
    }
    for value in chunks.remainder() {
        total = total.wrapping_add(*value);
    }
    total
}

/// Canonical 4-lane f64 sum (bit-identical to every SIMD backend).
#[must_use]
pub fn sum_f64(values: &[f64]) -> f64 {
    let mut lanes = [0.0f64; LANES];
    let mut chunks = values.chunks_exact(LANES);
    for block in &mut chunks {
        for (lane, value) in lanes.iter_mut().zip(block.iter()) {
            *lane += *value;
        }
    }
    finish_f64_lanes(lanes, chunks.remainder())
}

/// i64 minimum.
#[must_use]
pub fn min_i64(values: &[i64]) -> Option<i64> {
    let mut chunks = values.chunks_exact(LANES);
    let mut best: Option<i64> = None;
    let mut fold = |v: i64| {
        best = Some(match best {
            Some(b) => b.min(v),
            None => v,
        });
    };
    for block in &mut chunks {
        for value in block {
            fold(*value);
        }
    }
    for value in chunks.remainder() {
        fold(*value);
    }
    best
}

/// i64 maximum.
#[must_use]
pub fn max_i64(values: &[i64]) -> Option<i64> {
    let mut chunks = values.chunks_exact(LANES);
    let mut best: Option<i64> = None;
    let mut fold = |v: i64| {
        best = Some(match best {
            Some(b) => b.max(v),
            None => v,
        });
    };
    for block in &mut chunks {
        for value in block {
            fold(*value);
        }
    }
    for value in chunks.remainder() {
        fold(*value);
    }
    best
}

// ---------------------------------------------------------------------------
// Kernel primitives: elementwise compare / arithmetic / masks.
//
// Conventions (shared by every backend, enforced by parity tests):
// - Comparisons emit one byte per row (`1` = true), NaN compares false.
// - Integer arithmetic wraps (`wrapping_*`); float uses plain operators.
// - `*_scalar` forms broadcast one operand; vec/vec forms require equal
//   lengths (asserted).
// - `div_i64` returns `None` when any divisor is zero (SQL raises
//   "division by zero"; the kernel never invents a value).
// ---------------------------------------------------------------------------

fn check_pair(a: &[i64], b: &[i64]) {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
}

/// Elementwise `==` (0/1 bytes).
#[must_use]
pub fn eq_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    check_pair(a, b);
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| u8::from(x == y))
        .collect()
}

/// Elementwise `==` against a broadcast scalar.
#[must_use]
pub fn eq_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    a.iter().map(|x| u8::from(*x == scalar)).collect()
}

/// Elementwise `<` (0/1 bytes).
#[must_use]
pub fn lt_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    check_pair(a, b);
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| u8::from(x < y))
        .collect()
}

/// Elementwise `<` against a broadcast scalar.
#[must_use]
pub fn lt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    a.iter().map(|x| u8::from(*x < scalar)).collect()
}

/// Elementwise `>` (0/1 bytes).
#[must_use]
pub fn gt_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    check_pair(a, b);
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| u8::from(x > y))
        .collect()
}

/// Elementwise `>` against a broadcast scalar.
#[must_use]
pub fn gt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    a.iter().map(|x| u8::from(*x > scalar)).collect()
}

/// Inclusive `lo <= v <= hi` (SQL `BETWEEN`), 0/1 bytes.
#[must_use]
pub fn between_i64(values: &[i64], lo: i64, hi: i64) -> Vec<u8> {
    values
        .iter()
        .map(|v| u8::from(*v >= lo && *v <= hi))
        .collect()
}

/// Wrapping elementwise add.
#[must_use]
pub fn add_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    check_pair(a, b);
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| x.wrapping_add(*y))
        .collect()
}

/// Wrapping add of a broadcast scalar.
#[must_use]
pub fn add_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64> {
    a.iter().map(|x| x.wrapping_add(scalar)).collect()
}

/// Wrapping elementwise subtract.
#[must_use]
pub fn sub_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    check_pair(a, b);
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| x.wrapping_sub(*y))
        .collect()
}

/// Wrapping subtract of a broadcast scalar.
#[must_use]
pub fn sub_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64> {
    a.iter().map(|x| x.wrapping_sub(scalar)).collect()
}

/// Wrapping elementwise multiply.
#[must_use]
pub fn mul_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    check_pair(a, b);
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| x.wrapping_mul(*y))
        .collect()
}

/// Wrapping multiply by a broadcast scalar.
#[must_use]
pub fn mul_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64> {
    a.iter().map(|x| x.wrapping_mul(scalar)).collect()
}

/// Checked elementwise divide (`None` when any divisor is zero, matching
/// SQL's "division by zero" instead of inventing values or panicking).
/// `i64::MIN / -1` also yields `None` (its true quotient is unrepresentable).
#[must_use]
pub fn div_i64(a: &[i64], b: &[i64]) -> Option<Vec<i64>> {
    check_pair(a, b);
    let mut out = Vec::with_capacity(a.len());
    for (x, y) in a.iter().zip(b.iter()) {
        out.push(x.checked_div(*y)?);
    }
    Some(out)
}

/// Elementwise f64 `==` (plain operators: NaN compares false, matching
/// scalar executor semantics).
#[must_use]
pub fn eq_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| u8::from(x == y))
        .collect()
}

/// Elementwise f64 `==` against a broadcast scalar.
#[must_use]
pub fn eq_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    a.iter().map(|x| u8::from(*x == scalar)).collect()
}

/// Elementwise f64 `<` (NaN compares false).
#[must_use]
pub fn lt_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| u8::from(x < y))
        .collect()
}

/// Elementwise f64 `<` against a broadcast scalar.
#[must_use]
pub fn lt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    a.iter().map(|x| u8::from(*x < scalar)).collect()
}

/// Elementwise f64 `>` (NaN compares false).
#[must_use]
pub fn gt_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
    a.iter()
        .zip(b.iter())
        .map(|(x, y)| u8::from(x > y))
        .collect()
}

/// Elementwise f64 `>` against a broadcast scalar.
#[must_use]
pub fn gt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    a.iter().map(|x| u8::from(*x > scalar)).collect()
}

/// Inclusive f64 `BETWEEN` (NaN never matches).
#[must_use]
pub fn between_f64(values: &[f64], lo: f64, hi: f64) -> Vec<u8> {
    values
        .iter()
        .map(|v| u8::from(*v >= lo && *v <= hi))
        .collect()
}

/// Elementwise f64 add (plain operators: per-element, order-free).
#[must_use]
pub fn add_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
    a.iter().zip(b.iter()).map(|(x, y)| x + y).collect()
}

/// f64 add of a broadcast scalar.
#[must_use]
pub fn add_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    a.iter().map(|x| x + scalar).collect()
}

/// Elementwise f64 subtract.
#[must_use]
pub fn sub_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
    a.iter().zip(b.iter()).map(|(x, y)| x - y).collect()
}

/// f64 subtract of a broadcast scalar.
#[must_use]
pub fn sub_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    a.iter().map(|x| x - scalar).collect()
}

/// Elementwise f64 multiply.
#[must_use]
pub fn mul_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
    a.iter().zip(b.iter()).map(|(x, y)| x * y).collect()
}

/// f64 multiply by a broadcast scalar.
#[must_use]
pub fn mul_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    a.iter().map(|x| x * scalar).collect()
}

/// Elementwise f64 divide (IEEE: zero divisors yield inf/NaN exactly
/// like the scalar executor — never an error, never a trap).
#[must_use]
pub fn div_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
    a.iter().zip(b.iter()).map(|(x, y)| x / y).collect()
}

/// f64 divide by a broadcast scalar.
#[must_use]
pub fn div_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    a.iter().map(|x| x / scalar).collect()
}

/// Elementwise mask AND.
#[must_use]
pub fn and_mask(a: &[u8], b: &[u8]) -> Vec<u8> {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
    a.iter().zip(b.iter()).map(|(x, y)| x & y).collect()
}

/// Elementwise mask OR.
#[must_use]
pub fn or_mask(a: &[u8], b: &[u8]) -> Vec<u8> {
    assert_eq!(
        a.len(),
        b.len(),
        "kernel vec/vec operands must share a length"
    );
    a.iter().zip(b.iter()).map(|(x, y)| x | y).collect()
}

/// Elementwise mask NOT (nonzero becomes 0, zero becomes 1).
#[must_use]
pub fn not_mask(a: &[u8]) -> Vec<u8> {
    a.iter().map(|x| u8::from(*x == 0)).collect()
}

/// Rows marked NULL in a bitmap (`1` bit = NULL), expanded to 0/1 bytes.
#[must_use]
pub fn is_null_mask(nulls: &[u8], len: usize) -> Vec<u8> {
    (0..len)
        .map(|row| {
            u8::from(
                nulls
                    .get(row / 8)
                    .is_some_and(|b| b & (1 << (row % 8)) != 0),
            )
        })
        .collect()
}

/// Rows NOT marked NULL, expanded to 0/1 bytes.
#[must_use]
pub fn is_not_null_mask(nulls: &[u8], len: usize) -> Vec<u8> {
    (0..len)
        .map(|row| {
            u8::from(
                !nulls
                    .get(row / 8)
                    .is_some_and(|b| b & (1 << (row % 8)) != 0),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Selection engine: indices instead of copied rows.
//
// A filter produces a byte mask; `select_indices` compresses it to the
// surviving row numbers ONCE; every downstream operator gathers through
// those indices and never materializes rejected rows. Aggregation
// operates only on selected values (`*_selected` variants).
// ---------------------------------------------------------------------------

/// Compresses a 0/1 mask to surviving row indices, ascending.
#[must_use]
pub fn select_indices(mask: &[u8]) -> Vec<u32> {
    let mut out = Vec::new();
    for (row, keep) in mask.iter().enumerate() {
        if *keep != 0 {
            out.push(row as u32);
        }
    }
    out
}

/// Counts selected rows (no gather needed).
#[must_use]
pub fn count_selected(len: usize, sel: Option<&[u32]>) -> u64 {
    sel.map_or(len as u64, |indices| indices.len() as u64)
}

/// Gathers i64 values through indices (bounds-checked; out-of-range
/// indices are skipped, never trusted).
#[must_use]
pub fn gather_i64(values: &[i64], sel: &[u32]) -> Vec<i64> {
    let mut out = Vec::with_capacity(sel.len());
    for index in sel {
        if let Some(value) = values.get(*index as usize) {
            out.push(*value);
        }
    }
    out
}

/// Wrapping sum over selected values only.
#[must_use]
pub fn sum_selected_i64(values: &[i64], sel: &[u32]) -> i64 {
    let mut total = 0i64;
    for index in sel {
        if let Some(value) = values.get(*index as usize) {
            total = total.wrapping_add(*value);
        }
    }
    total
}

/// Minimum over selected values (`None` when nothing selected).
#[must_use]
pub fn min_selected_i64(values: &[i64], sel: &[u32]) -> Option<i64> {
    let mut best: Option<i64> = None;
    for index in sel {
        if let Some(value) = values.get(*index as usize) {
            best = Some(match best {
                Some(current) => current.min(*value),
                None => *value,
            });
        }
    }
    best
}

/// Maximum over selected values (`None` when nothing selected).
#[must_use]
pub fn max_selected_i64(values: &[i64], sel: &[u32]) -> Option<i64> {
    let mut best: Option<i64> = None;
    for index in sel {
        if let Some(value) = values.get(*index as usize) {
            best = Some(match best {
                Some(current) => current.max(*value),
                None => *value,
            });
        }
    }
    best
}
