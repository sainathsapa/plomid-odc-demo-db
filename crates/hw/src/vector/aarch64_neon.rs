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
// AArch64 NEON backend. Callable only after the runtime `neon` probe
// (`VectorOps` re-probes at selection). Unaligned vector loads/stores
// are legal on AArch64 for these instructions, so inputs load straight
// from caller slices (no staging copies) and results store into output
// buffers the same way.

use core::arch::aarch64::{
    int64x2_t, uint64x2_t, vaddq_f64, vaddq_s64, vandq_u8, vbslq_s64, vceqq_f64, vceqq_s64,
    vceqq_u8, vcgeq_f64, vcgtq_f64, vcgtq_s64, vcleq_f64, vcltq_f64, vcltq_s64, vdivq_f64,
    vdupq_n_f64, vdupq_n_s64, vdupq_n_u8, vgetq_lane_u64, vld1q_f64, vld1q_s64, vld1q_u8,
    vmulq_f64, vorrq_u8, vst1q_f64, vst1q_s64, vst1q_u8, vsubq_f64, vsubq_s64,
};

/// Lane-wise i64 minimum via compare-plus-select (NEON has no 64-bit
/// vector MIN/MAX instruction): lanes where `candidate < current` take
/// the candidate, the rest keep current. Exact for integers.
#[inline]
unsafe fn vmin_i64(current: int64x2_t, candidate: int64x2_t) -> int64x2_t {
    unsafe {
        let mask = vcltq_s64(candidate, current);
        vbslq_s64(mask, candidate, current)
    }
}

/// Lane-wise i64 maximum: lanes where `current < candidate` take the
/// candidate. Exact for integers.
#[inline]
unsafe fn vmax_i64(current: int64x2_t, candidate: int64x2_t) -> int64x2_t {
    unsafe {
        let mask = vcltq_s64(current, candidate);
        vbslq_s64(mask, candidate, current)
    }
}

use super::finish_f64_lanes;

/// Copies one 4-element block through an aligned stack array.
#[inline]
unsafe fn block4_i64(ptr: *const i64) -> [i64; 4] {
    unsafe {
        let mut words = [0i64; 4];
        core::ptr::copy_nonoverlapping(ptr, words.as_mut_ptr(), 4);
        words
    }
}

/// Wrapping i64 sum (order-free mod 2^64: identical to scalar).
///
/// # Safety
///
/// Caller must have confirmed the `neon` CPU feature. No memory
/// preconditions: all loads are register-local copies of input bytes.
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn sum_i64(values: &[i64]) -> i64 {
    unsafe {
        let mut chunks = values.chunks_exact(4);
        let mut lo = vld1q_s64([0i64; 2].as_ptr());
        let mut hi = vld1q_s64([0i64; 2].as_ptr());
        for block in &mut chunks {
            let words = block4_i64(block.as_ptr());
            lo = vaddq_s64(lo, vld1q_s64(words.as_ptr()));
            hi = vaddq_s64(hi, vld1q_s64(words.as_ptr().add(2)));
        }
        let mut lanes = [0i64; 4];
        vst1q_s64(lanes.as_mut_ptr(), lo);
        vst1q_s64(lanes.as_mut_ptr().add(2), hi);
        // Horizontal wrapping fold (any lane order is exact mod 2^64).
        let mut total = 0i64;
        for lane in lanes {
            total = total.wrapping_add(lane);
        }
        for value in chunks.remainder() {
            total = total.wrapping_add(*value);
        }
        total
    }
}

/// Canonical 4-lane f64 sum (bit-identical to scalar).
///
/// # Safety
///
/// As in [`sum_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn sum_f64(values: &[f64]) -> f64 {
    unsafe {
        let mut chunks = values.chunks_exact(4);
        let mut lo = vld1q_f64([0.0f64; 2].as_ptr());
        let mut hi = vld1q_f64([0.0f64; 2].as_ptr());
        for block in &mut chunks {
            let mut words = [0.0f64; 4];
            core::ptr::copy_nonoverlapping(block.as_ptr(), words.as_mut_ptr(), 4);
            lo = vaddq_f64(lo, vld1q_f64(words.as_ptr()));
            hi = vaddq_f64(hi, vld1q_f64(words.as_ptr().add(2)));
        }
        let mut lanes = [0.0f64; 4];
        vst1q_f64(lanes.as_mut_ptr(), lo);
        vst1q_f64(lanes.as_mut_ptr().add(2), hi);
        finish_f64_lanes(lanes, chunks.remainder())
    }
}

/// i64 minimum (order-free: exact).
///
/// # Safety
///
/// As in [`sum_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn min_i64(values: &[i64]) -> Option<i64> {
    unsafe {
        let (first, rest) = values.split_first()?;
        let mut best = *first;
        let mut chunks = rest.chunks_exact(4);
        let mut acc = vld1q_s64([best; 2].as_ptr());
        let mut acc_hi = vld1q_s64([best; 2].as_ptr());
        for block in &mut chunks {
            let words = block4_i64(block.as_ptr());
            acc = vmin_i64(acc, vld1q_s64(words.as_ptr()));
            acc_hi = vmin_i64(acc_hi, vld1q_s64(words.as_ptr().add(2)));
        }
        let mut lanes = [0i64; 4];
        vst1q_s64(lanes.as_mut_ptr(), acc);
        vst1q_s64(lanes.as_mut_ptr().add(2), acc_hi);
        for lane in lanes {
            best = best.min(lane);
        }
        for value in chunks.remainder() {
            best = best.min(*value);
        }
        Some(best)
    }
}

/// i64 maximum (order-free: exact).
///
/// # Safety
///
/// As in [`sum_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn max_i64(values: &[i64]) -> Option<i64> {
    unsafe {
        let (first, rest) = values.split_first()?;
        let mut best = *first;
        let mut chunks = rest.chunks_exact(4);
        let mut acc = vld1q_s64([best; 2].as_ptr());
        let mut acc_hi = vld1q_s64([best; 2].as_ptr());
        for block in &mut chunks {
            let words = block4_i64(block.as_ptr());
            acc = vmax_i64(acc, vld1q_s64(words.as_ptr()));
            acc_hi = vmax_i64(acc_hi, vld1q_s64(words.as_ptr().add(2)));
        }
        let mut lanes = [0i64; 4];
        vst1q_s64(lanes.as_mut_ptr(), acc);
        vst1q_s64(lanes.as_mut_ptr().add(2), acc_hi);
        for lane in lanes {
            best = best.max(lane);
        }
        for value in chunks.remainder() {
            best = best.max(*value);
        }
        Some(best)
    }
}

// ---------------------------------------------------------------------------
// NEON kernel primitives. Same contracts as `scalar::`
// (documented there); bit-identical outputs proven by parity tests.
// ISA gaps (documented, not hidden): i64 multiply and checked integer
// divide have no NEON instruction — those route to the shared scalar
// loop at dispatch time.

/// Lane mask bit: 1 when the uint64 lane is all-ones (comparison true).
#[inline]
fn lane_set<const LANE: i32>(mask: uint64x2_t) -> u8 {
    u8::from(unsafe { vgetq_lane_u64::<LANE>(mask) } != 0)
}

/// Compares two lanes with `op(a, b)` elementwise into out[0..2].
/// Unaligned loads are legal for these instructions on AArch64.
unsafe fn cmp2_i64(
    a: *const i64,
    b: *const i64,
    out: &mut Vec<u8>,
    op: unsafe fn(int64x2_t, int64x2_t) -> uint64x2_t,
) {
    unsafe {
        let mask = op(vld1q_s64(a), vld1q_s64(b));
        out.push(lane_set::<0>(mask));
        out.push(lane_set::<1>(mask));
    }
}

/// # Safety
///
/// Caller must have confirmed `neon`. Loads are register-local copies.
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn eq_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            cmp2_i64(x.as_ptr(), y.as_ptr(), &mut out, vceqq_s64);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| u8::from(x == y)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn eq_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    unsafe {
        let s = vdupq_n_s64(scalar);
        let n = a.len();
        // Raw fill: per-lane push costs a bounds check each (~5x here);
        // every index below is written exactly once with no panicking
        // operation in between, so the final length is exact.
        let mut out: Vec<u8> = Vec::with_capacity(n);
        let dst = out.as_mut_ptr();
        let mut chunks = a.chunks_exact(2);
        let mut base = 0usize;
        for x in &mut chunks {
            let mask = vceqq_s64(vld1q_s64(x.as_ptr()), s);
            *dst.add(base) = lane_set::<0>(mask);
            *dst.add(base + 1) = lane_set::<1>(mask);
            base += 2;
        }
        for x in chunks.remainder() {
            *dst.add(base) = u8::from(*x == scalar);
            base += 1;
        }
        debug_assert_eq!(base, n);
        out.set_len(n);
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn lt_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            cmp2_i64(x.as_ptr(), y.as_ptr(), &mut out, vcltq_s64);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| u8::from(x < y)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn lt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    unsafe {
        let s = vdupq_n_s64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let mask = vcltq_s64(vld1q_s64(x.as_ptr()), s);
            out.push(lane_set::<0>(mask));
            out.push(lane_set::<1>(mask));
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x < scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn gt_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            cmp2_i64(x.as_ptr(), y.as_ptr(), &mut out, vcgtq_s64);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| u8::from(x > y)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn gt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    unsafe {
        let s = vdupq_n_s64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let mask = vcgtq_s64(vld1q_s64(x.as_ptr()), s);
            out.push(lane_set::<0>(mask));
            out.push(lane_set::<1>(mask));
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x > scalar)));
        out
    }
}

/// Inclusive `BETWEEN` (scalar bounds).
///
/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn between_i64(values: &[i64], lo: i64, hi: i64) -> Vec<u8> {
    unsafe {
        let vlo = vdupq_n_s64(lo);
        let vhi = vdupq_n_s64(hi);
        let mut out = Vec::with_capacity(values.len());
        let mut chunks = values.chunks_exact(2);
        for x in &mut chunks {
            let v = vld1q_s64(x.as_ptr());
            let below = vcltq_s64(v, vlo);
            let above = vcgtq_s64(v, vhi);
            out.push(u8::from(
                lane_set::<0>(below) == 0 && lane_set::<0>(above) == 0,
            ));
            out.push(u8::from(
                lane_set::<1>(below) == 0 && lane_set::<1>(above) == 0,
            ));
        }
        out.extend(
            chunks
                .remainder()
                .iter()
                .map(|v| u8::from(*v >= lo && *v <= hi)),
        );
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn add_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = vaddq_s64(vld1q_s64(x.as_ptr()), vld1q_s64(y.as_ptr()));
            let mut w = [0i64; 2];
            vst1q_s64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| x.wrapping_add(*y)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn add_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64> {
    unsafe {
        let s = vdupq_n_s64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let r = vaddq_s64(vld1q_s64(x.as_ptr()), s);
            let mut w = [0i64; 2];
            vst1q_s64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        out.extend(chunks.remainder().iter().map(|x| x.wrapping_add(scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn sub_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = vsubq_s64(vld1q_s64(x.as_ptr()), vld1q_s64(y.as_ptr()));
            let mut w = [0i64; 2];
            vst1q_s64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| x.wrapping_sub(*y)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn sub_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64> {
    unsafe {
        let s = vdupq_n_s64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let r = vsubq_s64(vld1q_s64(x.as_ptr()), s);
            let mut w = [0i64; 2];
            vst1q_s64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        out.extend(chunks.remainder().iter().map(|x| x.wrapping_sub(scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn eq_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let mask = vceqq_f64(vld1q_f64(x.as_ptr()), vld1q_f64(y.as_ptr()));
            out.push(lane_set::<0>(mask));
            out.push(lane_set::<1>(mask));
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| u8::from(x == y)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn eq_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    unsafe {
        let s = vdupq_n_f64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let mask = vceqq_f64(vld1q_f64(x.as_ptr()), s);
            out.push(lane_set::<0>(mask));
            out.push(lane_set::<1>(mask));
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x == scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn lt_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let mask = vcltq_f64(vld1q_f64(x.as_ptr()), vld1q_f64(y.as_ptr()));
            out.push(lane_set::<0>(mask));
            out.push(lane_set::<1>(mask));
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| u8::from(x < y)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn lt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    unsafe {
        let s = vdupq_n_f64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let mask = vcltq_f64(vld1q_f64(x.as_ptr()), s);
            out.push(lane_set::<0>(mask));
            out.push(lane_set::<1>(mask));
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x < scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn gt_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let mask = vcgtq_f64(vld1q_f64(x.as_ptr()), vld1q_f64(y.as_ptr()));
            out.push(lane_set::<0>(mask));
            out.push(lane_set::<1>(mask));
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| u8::from(x > y)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn gt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    unsafe {
        let s = vdupq_n_f64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let mask = vcgtq_f64(vld1q_f64(x.as_ptr()), s);
            out.push(lane_set::<0>(mask));
            out.push(lane_set::<1>(mask));
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x > scalar)));
        out
    }
}

/// Inclusive f64 `BETWEEN` (NaN never matches).
///
/// Ordered `>=` / `<=` directly (NOT negated `<` / `>` — De Morgan
/// fails on NaN: `NOT(NaN < lo)` is true, wrongly admitting NaN).
///
/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn between_f64(values: &[f64], lo: f64, hi: f64) -> Vec<u8> {
    unsafe {
        let vlo = vdupq_n_f64(lo);
        let vhi = vdupq_n_f64(hi);
        let mut out = Vec::with_capacity(values.len());
        let mut chunks = values.chunks_exact(2);
        for x in &mut chunks {
            let v = vld1q_f64(x.as_ptr());
            let ge = vcgeq_f64(v, vlo);
            let le = vcleq_f64(v, vhi);
            out.push(u8::from(lane_set::<0>(ge) == 1 && lane_set::<0>(le) == 1));
            out.push(u8::from(lane_set::<1>(ge) == 1 && lane_set::<1>(le) == 1));
        }
        out.extend(
            chunks
                .remainder()
                .iter()
                .map(|v| u8::from(*v >= lo && *v <= hi)),
        );
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn add_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = vaddq_f64(vld1q_f64(x.as_ptr()), vld1q_f64(y.as_ptr()));
            let mut w = [0.0f64; 2];
            vst1q_f64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| x + y));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn add_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = vdupq_n_f64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let r = vaddq_f64(vld1q_f64(x.as_ptr()), s);
            let mut w = [0.0f64; 2];
            vst1q_f64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        out.extend(chunks.remainder().iter().map(|x| x + scalar));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn sub_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = vsubq_f64(vld1q_f64(x.as_ptr()), vld1q_f64(y.as_ptr()));
            let mut w = [0.0f64; 2];
            vst1q_f64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| x - y));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn sub_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = vdupq_n_f64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let r = vsubq_f64(vld1q_f64(x.as_ptr()), s);
            let mut w = [0.0f64; 2];
            vst1q_f64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        out.extend(chunks.remainder().iter().map(|x| x - scalar));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn mul_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = vmulq_f64(vld1q_f64(x.as_ptr()), vld1q_f64(y.as_ptr()));
            let mut w = [0.0f64; 2];
            vst1q_f64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| x * y));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn mul_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = vdupq_n_f64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let r = vmulq_f64(vld1q_f64(x.as_ptr()), s);
            let mut w = [0.0f64; 2];
            vst1q_f64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        out.extend(chunks.remainder().iter().map(|x| x * scalar));
        out
    }
}

/// Elementwise f64 divide (IEEE: zero divisors yield inf/NaN exactly
/// like scalar — never an error, never a trap).
///
/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn div_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        let mut bchunks = b.chunks_exact(2);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = vdivq_f64(vld1q_f64(x.as_ptr()), vld1q_f64(y.as_ptr()));
            let mut w = [0.0f64; 2];
            vst1q_f64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| x / y));
        out
    }
}

/// f64 divide by a broadcast scalar.
///
/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn div_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = vdupq_n_f64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(2);
        for x in &mut chunks {
            let r = vdivq_f64(vld1q_f64(x.as_ptr()), s);
            let mut w = [0.0f64; 2];
            vst1q_f64(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        out.extend(chunks.remainder().iter().map(|x| x / scalar));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn and_mask(a: &[u8], b: &[u8]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(16);
        let mut bchunks = b.chunks_exact(16);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = vandq_u8(vld1q_u8(x.as_ptr()), vld1q_u8(y.as_ptr()));
            let mut w = [0u8; 16];
            vst1q_u8(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| x & y));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn or_mask(a: &[u8], b: &[u8]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(16);
        let mut bchunks = b.chunks_exact(16);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = vorrq_u8(vld1q_u8(x.as_ptr()), vld1q_u8(y.as_ptr()));
            let mut w = [0u8; 16];
            vst1q_u8(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| x | y));
        out
    }
}

/// Nonzero becomes 0, zero becomes 1.
///
/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn not_mask(a: &[u8]) -> Vec<u8> {
    unsafe {
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(16);
        let one = vdupq_n_u8(1);
        let zero = vdupq_n_u8(0);
        for x in &mut chunks {
            let is_zero = vceqq_u8(vld1q_u8(x.as_ptr()), zero);
            let r = vandq_u8(is_zero, one);
            let mut w = [0u8; 16];
            vst1q_u8(w.as_mut_ptr(), r);
            out.extend_from_slice(&w);
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x == 0)));
        out
    }
}
