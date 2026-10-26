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
// x86_64 AVX-512 backend. Callable only after the runtime `avx512f`
// probe (`VectorOps` re-probes at selection). All loads are unaligned.
//
// Lane discipline: i64 sum/min/max are order-free, so the full 8-lane
// width is used. f64 deliberately uses FOUR lanes (via 256-bit ops,
// legal wherever AVX-512 holds) to preserve the canonical reduction
// order shared with every other backend — an 8-lane f64 fold would
// round differently and break ISA bit-identity.

use core::arch::x86_64::{
    __m256d, __m512i, _mm256_add_pd, _mm256_loadu_pd, _mm256_setzero_pd, _mm256_storeu_pd,
    _mm512_add_epi64, _mm512_add_pd, _mm512_and_si512, _mm512_cmp_pd_mask, _mm512_cmpeq_epi64_mask,
    _mm512_cmpgt_epi64_mask, _mm512_div_pd, _mm512_loadu_pd, _mm512_loadu_si512, _mm512_max_epi64,
    _mm512_min_epi64, _mm512_mul_pd, _mm512_or_si512, _mm512_set1_epi64, _mm512_set1_pd,
    _mm512_setzero_si512, _mm512_storeu_pd, _mm512_storeu_si512, _mm512_sub_epi64, _mm512_sub_pd,
    _CMP_EQ_OQ, _CMP_GE_OQ, _CMP_GT_OQ, _CMP_LE_OQ, _CMP_LT_OQ,
};

use super::finish_f64_lanes;

/// Wrapping i64 sum (order-free mod 2^64: identical to scalar).
///
/// # Safety
///
/// Caller must have confirmed the `avx512f` CPU feature. No memory
/// preconditions: unaligned loads of input bytes only.
#[must_use]
#[target_feature(enable = "avx512f")]
pub unsafe fn sum_i64(values: &[i64]) -> i64 {
    unsafe {
        let mut chunks = values.chunks_exact(8);
        let mut acc = _mm512_setzero_si512();
        for block in &mut chunks {
            let mut words = [0i64; 8];
            core::ptr::copy_nonoverlapping(block.as_ptr(), words.as_mut_ptr(), 8);
            let v = _mm512_loadu_si512(words.as_ptr() as *const __m512i);
            acc = _mm512_add_epi64(acc, v);
        }
        let mut lanes = [0i64; 8];
        _mm512_storeu_si512(lanes.as_mut_ptr() as *mut __m512i, acc);
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
#[target_feature(enable = "avx512f")]
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

/// i64 minimum (order-free: exact).
///
/// # Safety
///
/// As in [`sum_i64`].
#[must_use]
#[target_feature(enable = "avx512f")]
pub unsafe fn min_i64(values: &[i64]) -> Option<i64> {
    unsafe {
        let (first, rest) = values.split_first()?;
        let mut best = *first;
        let mut chunks = rest.chunks_exact(8);
        let mut acc = _mm512_set1_epi64(best);
        for block in &mut chunks {
            let mut words = [0i64; 8];
            core::ptr::copy_nonoverlapping(block.as_ptr(), words.as_mut_ptr(), 8);
            let v = _mm512_loadu_si512(words.as_ptr() as *const __m512i);
            acc = _mm512_min_epi64(acc, v);
        }
        let mut lanes = [0i64; 8];
        _mm512_storeu_si512(lanes.as_mut_ptr() as *mut __m512i, acc);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn max_i64(values: &[i64]) -> Option<i64> {
    unsafe {
        let (first, rest) = values.split_first()?;
        let mut best = *first;
        let mut chunks = rest.chunks_exact(8);
        let mut acc = _mm512_set1_epi64(best);
        for block in &mut chunks {
            let mut words = [0i64; 8];
            core::ptr::copy_nonoverlapping(block.as_ptr(), words.as_mut_ptr(), 8);
            let v = _mm512_loadu_si512(words.as_ptr() as *const __m512i);
            acc = _mm512_max_epi64(acc, v);
        }
        let mut lanes = [0i64; 8];
        _mm512_storeu_si512(lanes.as_mut_ptr() as *mut __m512i, acc);
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
// AVX-512 kernel primitives. Same contracts as `scalar::`;
// bit-identical outputs proven by parity tests. ISA gaps (documented,
// not hidden): 64-bit multiply needs AVX512DQ (only AVX512F is probed),
// and checked integer divide has no instruction on any target — both
// route to the shared scalar loop at dispatch time.

/// Extracts the eight lane bits from an 8-bit mask.
#[inline]
fn lanes8(mask: u8, out: &mut Vec<u8>) {
    for lane in 0..8 {
        out.push(u8::from((mask >> lane) & 1 == 1));
    }
}

/// # Safety
///
/// Caller must have confirmed `avx512f`. Loads are register-local copies.
#[must_use]
#[target_feature(enable = "avx512f")]
pub unsafe fn eq_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm512_cmpeq_epi64_mask(
                _mm512_loadu_si512(x.as_ptr() as *const __m512i),
                _mm512_loadu_si512(y.as_ptr() as *const __m512i),
            );
            lanes8(m, &mut out);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn eq_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    unsafe {
        let s = _mm512_set1_epi64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            lanes8(
                _mm512_cmpeq_epi64_mask(_mm512_loadu_si512(x.as_ptr() as *const __m512i), s),
                &mut out,
            );
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x == scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx512f")]
pub unsafe fn lt_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            // a < b  <=>  b > a.
            let m = _mm512_cmpgt_epi64_mask(
                _mm512_loadu_si512(y.as_ptr() as *const __m512i),
                _mm512_loadu_si512(x.as_ptr() as *const __m512i),
            );
            lanes8(m, &mut out);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn lt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    unsafe {
        let s = _mm512_set1_epi64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            lanes8(
                _mm512_cmpgt_epi64_mask(s, _mm512_loadu_si512(x.as_ptr() as *const __m512i)),
                &mut out,
            );
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x < scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx512f")]
pub unsafe fn gt_i64(a: &[i64], b: &[i64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm512_cmpgt_epi64_mask(
                _mm512_loadu_si512(x.as_ptr() as *const __m512i),
                _mm512_loadu_si512(y.as_ptr() as *const __m512i),
            );
            lanes8(m, &mut out);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn gt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8> {
    unsafe {
        let s = _mm512_set1_epi64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            lanes8(
                _mm512_cmpgt_epi64_mask(_mm512_loadu_si512(x.as_ptr() as *const __m512i), s),
                &mut out,
            );
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
#[target_feature(enable = "avx512f")]
pub unsafe fn between_i64(values: &[i64], lo: i64, hi: i64) -> Vec<u8> {
    unsafe {
        let vlo = _mm512_set1_epi64(lo);
        let vhi = _mm512_set1_epi64(hi);
        let mut out = Vec::with_capacity(values.len());
        let mut chunks = values.chunks_exact(8);
        for x in &mut chunks {
            let v = _mm512_loadu_si512(x.as_ptr() as *const __m512i);
            let below = _mm512_cmpgt_epi64_mask(vlo, v);
            let above = _mm512_cmpgt_epi64_mask(v, vhi);
            let bad = below | above;
            for lane in 0..8 {
                out.push(u8::from((bad >> lane) & 1 == 0));
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
#[target_feature(enable = "avx512f")]
pub unsafe fn add_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm512_add_epi64(
                _mm512_loadu_si512(x.as_ptr() as *const __m512i),
                _mm512_loadu_si512(y.as_ptr() as *const __m512i),
            );
            let mut w = [0i64; 8];
            _mm512_storeu_si512(w.as_mut_ptr() as *mut __m512i, r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn add_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64> {
    unsafe {
        let s = _mm512_set1_epi64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            let r = _mm512_add_epi64(_mm512_loadu_si512(x.as_ptr() as *const __m512i), s);
            let mut w = [0i64; 8];
            _mm512_storeu_si512(w.as_mut_ptr() as *mut __m512i, r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn sub_i64(a: &[i64], b: &[i64]) -> Vec<i64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm512_sub_epi64(
                _mm512_loadu_si512(x.as_ptr() as *const __m512i),
                _mm512_loadu_si512(y.as_ptr() as *const __m512i),
            );
            let mut w = [0i64; 8];
            _mm512_storeu_si512(w.as_mut_ptr() as *mut __m512i, r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn sub_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64> {
    unsafe {
        let s = _mm512_set1_epi64(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            let r = _mm512_sub_epi64(_mm512_loadu_si512(x.as_ptr() as *const __m512i), s);
            let mut w = [0i64; 8];
            _mm512_storeu_si512(w.as_mut_ptr() as *mut __m512i, r);
            out.extend_from_slice(&w);
        }
        out.extend(chunks.remainder().iter().map(|x| x.wrapping_sub(scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`]. Ordered comparisons: NaN compares false.
#[must_use]
#[target_feature(enable = "avx512f")]
pub unsafe fn eq_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm512_cmp_pd_mask(
                _mm512_loadu_pd(x.as_ptr()),
                _mm512_loadu_pd(y.as_ptr()),
                _CMP_EQ_OQ,
            );
            lanes8(m, &mut out);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn eq_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    unsafe {
        let s = _mm512_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            lanes8(
                _mm512_cmp_pd_mask(_mm512_loadu_pd(x.as_ptr()), s, _CMP_EQ_OQ),
                &mut out,
            );
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x == scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx512f")]
pub unsafe fn lt_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm512_cmp_pd_mask(
                _mm512_loadu_pd(x.as_ptr()),
                _mm512_loadu_pd(y.as_ptr()),
                _CMP_LT_OQ,
            );
            lanes8(m, &mut out);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn lt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    unsafe {
        let s = _mm512_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            lanes8(
                _mm512_cmp_pd_mask(_mm512_loadu_pd(x.as_ptr()), s, _CMP_LT_OQ),
                &mut out,
            );
        }
        out.extend(chunks.remainder().iter().map(|x| u8::from(*x < scalar)));
        out
    }
}

/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx512f")]
pub unsafe fn gt_f64(a: &[f64], b: &[f64]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let m = _mm512_cmp_pd_mask(
                _mm512_loadu_pd(x.as_ptr()),
                _mm512_loadu_pd(y.as_ptr()),
                _CMP_GT_OQ,
            );
            lanes8(m, &mut out);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn gt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8> {
    unsafe {
        let s = _mm512_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            lanes8(
                _mm512_cmp_pd_mask(_mm512_loadu_pd(x.as_ptr()), s, _CMP_GT_OQ),
                &mut out,
            );
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
#[target_feature(enable = "avx512f")]
pub unsafe fn between_f64(values: &[f64], lo: f64, hi: f64) -> Vec<u8> {
    unsafe {
        let vlo = _mm512_set1_pd(lo);
        let vhi = _mm512_set1_pd(hi);
        let mut out = Vec::with_capacity(values.len());
        let mut chunks = values.chunks_exact(8);
        for x in &mut chunks {
            let v = _mm512_loadu_pd(x.as_ptr());
            let ge = _mm512_cmp_pd_mask(v, vlo, _CMP_GE_OQ);
            let le = _mm512_cmp_pd_mask(v, vhi, _CMP_LE_OQ);
            // Both masks are __mmask8 (u8): already 8 bits, no truncation.
            let both = ge & le;
            for lane in 0..8 {
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
#[target_feature(enable = "avx512f")]
pub unsafe fn add_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm512_add_pd(_mm512_loadu_pd(x.as_ptr()), _mm512_loadu_pd(y.as_ptr()));
            let mut w = [0.0f64; 8];
            _mm512_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn add_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = _mm512_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            let r = _mm512_add_pd(_mm512_loadu_pd(x.as_ptr()), s);
            let mut w = [0.0f64; 8];
            _mm512_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn sub_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm512_sub_pd(_mm512_loadu_pd(x.as_ptr()), _mm512_loadu_pd(y.as_ptr()));
            let mut w = [0.0f64; 8];
            _mm512_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn sub_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = _mm512_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            let r = _mm512_sub_pd(_mm512_loadu_pd(x.as_ptr()), s);
            let mut w = [0.0f64; 8];
            _mm512_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn mul_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm512_mul_pd(_mm512_loadu_pd(x.as_ptr()), _mm512_loadu_pd(y.as_ptr()));
            let mut w = [0.0f64; 8];
            _mm512_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn mul_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = _mm512_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            let r = _mm512_mul_pd(_mm512_loadu_pd(x.as_ptr()), s);
            let mut w = [0.0f64; 8];
            _mm512_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn div_f64(a: &[f64], b: &[f64]) -> Vec<f64> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        let mut bchunks = b.chunks_exact(8);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let r = _mm512_div_pd(_mm512_loadu_pd(x.as_ptr()), _mm512_loadu_pd(y.as_ptr()));
            let mut w = [0.0f64; 8];
            _mm512_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn div_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64> {
    unsafe {
        let s = _mm512_set1_pd(scalar);
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(8);
        for x in &mut chunks {
            let r = _mm512_div_pd(_mm512_loadu_pd(x.as_ptr()), s);
            let mut w = [0.0f64; 8];
            _mm512_storeu_pd(w.as_mut_ptr(), r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn and_mask(a: &[u8], b: &[u8]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(64);
        let mut bchunks = b.chunks_exact(64);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let mut xb = [0u8; 64];
            let mut yb = [0u8; 64];
            core::ptr::copy_nonoverlapping(x.as_ptr(), xb.as_mut_ptr(), 64);
            core::ptr::copy_nonoverlapping(y.as_ptr(), yb.as_mut_ptr(), 64);
            let r = _mm512_and_si512(
                _mm512_loadu_si512(xb.as_ptr() as *const __m512i),
                _mm512_loadu_si512(yb.as_ptr() as *const __m512i),
            );
            let mut w = [0u8; 64];
            _mm512_storeu_si512(w.as_mut_ptr() as *mut __m512i, r);
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
#[target_feature(enable = "avx512f")]
pub unsafe fn or_mask(a: &[u8], b: &[u8]) -> Vec<u8> {
    unsafe {
        assert_eq!(
            a.len(),
            b.len(),
            "kernel vec/vec operands must share a length"
        );
        let mut out = Vec::with_capacity(a.len());
        let mut chunks = a.chunks_exact(64);
        let mut bchunks = b.chunks_exact(64);
        for (x, y) in (&mut chunks).zip(&mut bchunks) {
            let mut xb = [0u8; 64];
            let mut yb = [0u8; 64];
            core::ptr::copy_nonoverlapping(x.as_ptr(), xb.as_mut_ptr(), 64);
            core::ptr::copy_nonoverlapping(y.as_ptr(), yb.as_mut_ptr(), 64);
            let r = _mm512_or_si512(
                _mm512_loadu_si512(xb.as_ptr() as *const __m512i),
                _mm512_loadu_si512(yb.as_ptr() as *const __m512i),
            );
            let mut w = [0u8; 64];
            _mm512_storeu_si512(w.as_mut_ptr() as *mut __m512i, r);
            out.extend_from_slice(&w);
        }
        let (xr, yr) = (chunks.remainder(), bchunks.remainder());
        out.extend(xr.iter().zip(yr.iter()).map(|(x, y)| x | y));
        out
    }
}

/// Nonzero becomes 0, zero becomes 1.
///
/// Implemented as a scalar loop on purpose: byte-wise equality needs
/// AVX512BW (`vpcmpeqb`), but only AVX512F is probed, so no 512-bit
/// byte compare exists here. Masks are metadata-cheap; correctness
/// first. (The 64-byte chunking is kept so a future BW-gated path can
/// drop straight in.)
///
/// # Safety
///
/// As in [`eq_i64`].
#[must_use]
#[target_feature(enable = "avx512f")]
pub unsafe fn not_mask(a: &[u8]) -> Vec<u8> {
    a.iter().map(|x| u8::from(*x == 0)).collect()
}
