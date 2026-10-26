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
// x86_64 AVX2 backend. Callable only after the runtime `avx2` probe
// (`VectorOps` re-probes at selection). All loads are unaligned
// (`loadu`); unaligned input is never assumed.
//
// NOTE: AVX2 has no 64-bit integer min/max instruction, so min/max use
// compare-plus-blend (`cmpgt` + `blendv`), which is exact for integers.

use core::arch::x86_64::{
    __m256d, __m256i, _mm256_add_epi64, _mm256_add_pd, _mm256_and_si256, _mm256_blendv_epi8,
    _mm256_cmp_pd, _mm256_cmpeq_epi64, _mm256_cmpeq_epi8, _mm256_cmpgt_epi64, _mm256_div_pd,
    _mm256_loadu_pd, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_movemask_pd, _mm256_mul_pd,
    _mm256_or_si256, _mm256_set1_epi64x, _mm256_set1_epi8, _mm256_set1_pd, _mm256_setzero_pd,
    _mm256_setzero_si256, _mm256_storeu_pd, _mm256_storeu_si256, _mm256_sub_epi64, _mm256_sub_pd,
    _CMP_EQ_OQ, _CMP_GE_OQ, _CMP_GT_OQ, _CMP_LE_OQ, _CMP_LT_OQ,
};

use super::finish_f64_lanes;

/// Wrapping i64 sum (order-free mod 2^64: identical to scalar).
///
/// # Safety
///
/// Caller must have confirmed the `avx2` CPU feature. No memory
/// preconditions: unaligned loads of input bytes only.
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn sum_i64(values: &[i64]) -> i64 {
    unsafe {
        let mut chunks = values.chunks_exact(4);
        let mut acc = _mm256_setzero_si256();
        for block in &mut chunks {
            let mut words = [0i64; 4];
            core::ptr::copy_nonoverlapping(block.as_ptr(), words.as_mut_ptr(), 4);
            let v = _mm256_loadu_si256(words.as_ptr() as *const __m256i);
            acc = _mm256_add_epi64(acc, v);
        }
        // Horizontal wrapping fold (any lane order is exact mod 2^64).
        let mut lanes = [0i64; 4];
        _mm256_storeu_si256(lanes.as_mut_ptr() as *mut __m256i, acc);
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
#[target_feature(enable = "avx2")]
pub unsafe fn sum_f64(values: &[f64]) -> f64 {
    unsafe {
        let mut chunks = values.chunks_exact(4);
        let mut acc = _mm256_setzero_pd();
        for block in &mut chunks {
            let mut words = [0.0f64; 4];
            core::ptr::copy_nonoverlapping(block.as_ptr(), words.as_mut_ptr(), 4);
            let v: __m256d = _mm256_loadu_pd(words.as_ptr());
            acc = _mm256_add_pd(acc, v);
        }
        let mut lanes = [0.0f64; 4];
        _mm256_storeu_pd(lanes.as_mut_ptr(), acc);
        finish_f64_lanes(lanes, chunks.remainder())
    }
}

/// Lane-wise select: `blendv(a, b, mask)` picks bytes from `b` where the
/// mask byte's sign bit is set, else from `a`.
#[inline]
unsafe fn blend_min_i64(current: __m256i, candidate: __m256i) -> __m256i {
    unsafe {
        // `candidate < current` mask, then pick candidate where set.
        let gt = _mm256_cmpgt_epi64(current, candidate);
        _mm256_blendv_epi8(current, candidate, gt)
    }
}

/// i64 minimum (order-free: exact).
///
/// # Safety
///
/// As in [`sum_i64`].
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn min_i64(values: &[i64]) -> Option<i64> {
    unsafe {
        let (first, rest) = values.split_first()?;
        let mut best = *first;
        let mut chunks = rest.chunks_exact(4);
        let mut acc = _mm256_set1_epi64x(best);
        for block in &mut chunks {
            let mut words = [0i64; 4];
            core::ptr::copy_nonoverlapping(block.as_ptr(), words.as_mut_ptr(), 4);
            let v = _mm256_loadu_si256(words.as_ptr() as *const __m256i);
            acc = blend_min_i64(acc, v);
        }
        let mut lanes = [0i64; 4];
        _mm256_storeu_si256(lanes.as_mut_ptr() as *mut __m256i, acc);
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
#[target_feature(enable = "avx2")]
pub unsafe fn max_i64(values: &[i64]) -> Option<i64> {
    unsafe {
        let (first, rest) = values.split_first()?;
        let mut best = *first;
        let mut chunks = rest.chunks_exact(4);
        // max(a, b) == -min(-a, -b) is awkward in lanes; instead select
        // `candidate` where `candidate > current`, i.e. blend with the
        // swapped comparison.
        let mut acc = _mm256_set1_epi64x(best);
        for block in &mut chunks {
            let mut words = [0i64; 4];
            core::ptr::copy_nonoverlapping(block.as_ptr(), words.as_mut_ptr(), 4);
            let v = _mm256_loadu_si256(words.as_ptr() as *const __m256i);
            // `current > candidate` mask picks `current` where set…
            let gt = _mm256_cmpgt_epi64(acc, v);
            // …so blending (candidate=v, current=acc) with that mask
            // keeps acc where acc>v and takes v where v>=acc. Exact max.
            acc = _mm256_blendv_epi8(v, acc, gt);
        }
        let mut lanes = [0i64; 4];
        _mm256_storeu_si256(lanes.as_mut_ptr() as *mut __m256i, acc);
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
// AVX2 kernel primitives. Same contracts as `scalar::`;
// bit-identical outputs proven by parity tests. ISA gaps (documented,
// not hidden): 64-bit multiply and checked integer divide have no AVX2
// instruction — those route to the shared scalar loop at dispatch time.

/// Extracts the four lane-validity bits from a movemask_epi8 result
/// (byte 8*i carries lane i for 64-bit compares).
#[inline]
fn lanes4_i64(mask: i32, out: &mut Vec<u8>) {
    let bits = mask as u32;
    for lane in 0..4 {
        out.push(u8::from((bits >> (8 * lane)) & 1 == 1));
    }
}

/// # Safety
///
/// Caller must have confirmed `avx2`. Loads are register-local copies.
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn eq_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm256_cmpeq_epi64(
                _mm256_loadu_si256(x.as_ptr() as *const __m256i),
                _mm256_loadu_si256(y.as_ptr() as *const __m256i),
            );
            lanes4_i64(_mm256_movemask_epi8(m), &mut out);
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
#[target_feature(enable = "avx2")]
pub unsafe fn eq_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    unsafe {
        let s = _mm256_set1_epi64x(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let m = _mm256_cmpeq_epi64(_mm256_loadu_si256(x.as_ptr() as *const __m256i), s);
            lanes4_i64(_mm256_movemask_epi8(m), &mut out);
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x == scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn lt_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            // a < b  <=>  b > a.
            let m = _mm256_cmpgt_epi64(
                _mm256_loadu_si256(y.as_ptr() as *const __m256i),
                _mm256_loadu_si256(x.as_ptr() as *const __m256i),
            );
            lanes4_i64(_mm256_movemask_epi8(m), &mut out);
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
#[target_feature(enable = "avx2")]
pub unsafe fn lt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    unsafe {
        let s = _mm256_set1_epi64x(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let m = _mm256_cmpgt_epi64(s, _mm256_loadu_si256(x.as_ptr() as *const __m256i));
            lanes4_i64(_mm256_movemask_epi8(m), &mut out);
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x < scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn gt_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm256_cmpgt_epi64(
                _mm256_loadu_si256(x.as_ptr() as *const __m256i),
                _mm256_loadu_si256(y.as_ptr() as *const __m256i),
            );
            lanes4_i64(_mm256_movemask_epi8(m), &mut out);
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
#[target_feature(enable = "avx2")]
pub unsafe fn gt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    unsafe {
        let s = _mm256_set1_epi64x(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let m = _mm256_cmpgt_epi64(_mm256_loadu_si256(x.as_ptr() as *const __m256i), s);
            lanes4_i64(_mm256_movemask_epi8(m), &mut out);
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
#[target_feature(enable = "avx2")]
pub unsafe fn between_i64(values: &[i64], lo: i64, hi: i64) -> Vec<u8> {
    unsafe {
        let vlo = _mm256_set1_epi64x(lo);
        let vhi = _mm256_set1_epi64x(hi);
        let mut out = Vec::with_capacity(values.len());
        let mut chunks = values.chunks_exact(4);
        for x in &mut chunks {
            let v = _mm256_loadu_si256(x.as_ptr() as *const __m256i);
            // below = (v < lo), above = (v > hi), via swapped cmpgt.
            let below = _mm256_movemask_epi8(_mm256_cmpgt_epi64(vlo, v)) as u32;
            let above = _mm256_movemask_epi8(_mm256_cmpgt_epi64(v, vhi)) as u32;
            for lane in 0..4 {
                let bad = ((below >> (8 * lane)) | (above >> (8 * lane))) & 1;
                out.push(u8::from(bad == 0));
            }
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
#[target_feature(enable = "avx2")]
pub unsafe fn add_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm256_add_epi64(
                _mm256_loadu_si256(x.as_ptr() as *const __m256i),
                _mm256_loadu_si256(y.as_ptr() as *const __m256i),
            );
            let mut w = [0i64; 4];
            _mm256_storeu_si256(w.as_mut_ptr() as *mut __m256i, r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn add_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64> {
    unsafe {
        let s = _mm256_set1_epi64x(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let r = _mm256_add_epi64(_mm256_loadu_si256(x.as_ptr() as *const __m256i), s);
            let mut w = [0i64; 4];
            _mm256_storeu_si256(w.as_mut_ptr() as *mut __m256i, r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn sub_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm256_sub_epi64(
                _mm256_loadu_si256(x.as_ptr() as *const __m256i),
                _mm256_loadu_si256(y.as_ptr() as *const __m256i),
            );
            let mut w = [0i64; 4];
            _mm256_storeu_si256(w.as_mut_ptr() as *mut __m256i, r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn sub_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64> {
    unsafe {
        let s = _mm256_set1_epi64x(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let r = _mm256_sub_epi64(_mm256_loadu_si256(x.as_ptr() as *const __m256i), s);
            let mut w = [0i64; 4];
            _mm256_storeu_si256(w.as_mut_ptr() as *mut __m256i, r);
            out.extend_from_slice(&w);
        }
        out.extend(chunks.remainder().iter().map(|x| x.wrapping_sub(scalar)));
        out
    }
}

/// Extracts the four per-double mask bits (movemask_pd gives bit i).
#[inline]
fn lanes4_pd(mask: i32, out: &mut Vec<u8>) {
    for lane in 0..4 {
        out.push(u8::from((mask >> lane) & 1 == 1));
    }
}

/// # Safety
///
/// As in [`eq_i64`]. Ordered comparisons: NaN compares false.
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn eq_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm256_cmp_pd(
                _mm256_loadu_pd(x.as_ptr()),
                _mm256_loadu_pd(y.as_ptr()),
                _CMP_EQ_OQ,
            );
            lanes4_pd(_mm256_movemask_pd(m), &mut out);
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
#[target_feature(enable = "avx2")]
pub unsafe fn eq_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    unsafe {
        let s = _mm256_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let m = _mm256_cmp_pd(_mm256_loadu_pd(x.as_ptr()), s, _CMP_EQ_OQ);
            lanes4_pd(_mm256_movemask_pd(m), &mut out);
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x == scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn lt_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm256_cmp_pd(
                _mm256_loadu_pd(x.as_ptr()),
                _mm256_loadu_pd(y.as_ptr()),
                _CMP_LT_OQ,
            );
            lanes4_pd(_mm256_movemask_pd(m), &mut out);
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
#[target_feature(enable = "avx2")]
pub unsafe fn lt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    unsafe {
        let s = _mm256_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let m = _mm256_cmp_pd(_mm256_loadu_pd(x.as_ptr()), s, _CMP_LT_OQ);
            lanes4_pd(_mm256_movemask_pd(m), &mut out);
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x < scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn gt_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm256_cmp_pd(
                _mm256_loadu_pd(x.as_ptr()),
                _mm256_loadu_pd(y.as_ptr()),
                _CMP_GT_OQ,
            );
            lanes4_pd(_mm256_movemask_pd(m), &mut out);
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
#[target_feature(enable = "avx2")]
pub unsafe fn gt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    unsafe {
        let s = _mm256_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let m = _mm256_cmp_pd(_mm256_loadu_pd(x.as_ptr()), s, _CMP_GT_OQ);
            lanes4_pd(_mm256_movemask_pd(m), &mut out);
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x > scalar)));
        out
    }
}

/// Inclusive f64 `BETWEEN` (NaN never matches).
///
/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn between_f64(values: &[f64], lo: f64, hi: f64) -> Vec<u8> {
    unsafe {
        let vlo = _mm256_set1_pd(lo);
        let vhi = _mm256_set1_pd(hi);
        let mut out = Vec::with_capacity(values.len());
        let mut chunks = values.chunks_exact(4);
        for x in &mut chunks {
            let v = _mm256_loadu_pd(x.as_ptr());
            let ge = _mm256_movemask_pd(_mm256_cmp_pd(v, vlo, _CMP_GE_OQ)) as u32;
            let le = _mm256_movemask_pd(_mm256_cmp_pd(v, vhi, _CMP_LE_OQ)) as u32;
            let both = (ge & le) & 0xF;
            for lane in 0..4 {
                out.push(u8::from((both >> lane) & 1 == 1));
            }
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
#[target_feature(enable = "avx2")]
pub unsafe fn add_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm256_add_pd(_mm256_loadu_pd(x.as_ptr()), _mm256_loadu_pd(y.as_ptr()));
            let mut w = [0.0f64; 4];
            _mm256_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn add_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = _mm256_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let r = _mm256_add_pd(_mm256_loadu_pd(x.as_ptr()), s);
            let mut w = [0.0f64; 4];
            _mm256_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn sub_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm256_sub_pd(_mm256_loadu_pd(x.as_ptr()), _mm256_loadu_pd(y.as_ptr()));
            let mut w = [0.0f64; 4];
            _mm256_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn sub_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = _mm256_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let r = _mm256_sub_pd(_mm256_loadu_pd(x.as_ptr()), s);
            let mut w = [0.0f64; 4];
            _mm256_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn mul_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm256_mul_pd(_mm256_loadu_pd(x.as_ptr()), _mm256_loadu_pd(y.as_ptr()));
            let mut w = [0.0f64; 4];
            _mm256_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn mul_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = _mm256_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let r = _mm256_mul_pd(_mm256_loadu_pd(x.as_ptr()), s);
            let mut w = [0.0f64; 4];
            _mm256_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn div_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        let mut bchunks = b.chunks_exact(4);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm256_div_pd(_mm256_loadu_pd(x.as_ptr()), _mm256_loadu_pd(y.as_ptr()));
            let mut w = [0.0f64; 4];
            _mm256_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn div_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = _mm256_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(4);
        for x in &mut chunks {
            let r = _mm256_div_pd(_mm256_loadu_pd(x.as_ptr()), s);
            let mut w = [0.0f64; 4];
            _mm256_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn and_mask(a: &[u8], b: &[u8]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(32);
        let mut bchunks = b.chunks_exact(32);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let mut xb = [0u8; 32];
            let mut yb = [0u8; 32];
            core::ptr::copy_nonoverlapping(x.as_ptr(), xb.as_mut_ptr(), 32);
            core::ptr::copy_nonoverlapping(y.as_ptr(), yb.as_mut_ptr(), 32);
            let r = _mm256_and_si256(
                _mm256_loadu_si256(xb.as_ptr() as *const __m256i),
                _mm256_loadu_si256(yb.as_ptr() as *const __m256i),
            );
            let mut w = [0u8; 32];
            _mm256_storeu_si256(w.as_mut_ptr() as *mut __m256i, r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn or_mask(a: &[u8], b: &[u8]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(32);
        let mut bchunks = b.chunks_exact(32);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let mut xb = [0u8; 32];
            let mut yb = [0u8; 32];
            core::ptr::copy_nonoverlapping(x.as_ptr(), xb.as_mut_ptr(), 32);
            core::ptr::copy_nonoverlapping(y.as_ptr(), yb.as_mut_ptr(), 32);
            let r = _mm256_or_si256(
                _mm256_loadu_si256(xb.as_ptr() as *const __m256i),
                _mm256_loadu_si256(yb.as_ptr() as *const __m256i),
            );
            let mut w = [0u8; 32];
            _mm256_storeu_si256(w.as_mut_ptr() as *mut __m256i, r);
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
#[target_feature(enable = "avx2")]
pub unsafe fn not_mask(a: &[u8]) -> Vec<u8> {
    unsafe {
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(32);
        let ones = _mm256_set1_epi8(-1);
        for x in &mut chunks {
            let mut xb = [0u8; 32];
            core::ptr::copy_nonoverlapping(x.as_ptr(), xb.as_mut_ptr(), 32);
            let v = _mm256_loadu_si256(xb.as_ptr() as *const __m256i);
            // Zero lanes compare equal against zero; invert to 0/1.
            let is_zero = _mm256_cmpeq_epi8(v, _mm256_setzero_si256());
            let r = _mm256_and_si256(is_zero, ones);
            // is_zero lanes are 0xFF/0x00; AND with 0x01 keeps bit 0.
            let mut w = [0u8; 32];
            _mm256_storeu_si256(w.as_mut_ptr() as *mut __m256i, r);
            for byte in w {
                out.push(byte & 1);
            }
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x == 0)));
        out
    }
}
