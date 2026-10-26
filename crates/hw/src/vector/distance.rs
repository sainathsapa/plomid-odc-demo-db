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

//! f32 distance kernels: inner product and squared Euclidean distance.
//!
//! Safe wrappers dispatch at runtime: AVX2 on x86_64, NEON on aarch64,
//! portable scalar elsewhere (or when the probe fails). The SIMD tiers use
//! `f32` lane accumulation, so results agree with the `f64`-accumulator
//! scalar oracle within floating-point tolerance — not bit-identically.
//! Contract: relative error `<= 1e-4` (absolute floor `1e-5`); callers
//! compare with tolerance, never `==`. Length mismatches compute over the
//! shared prefix (`min` length); production callers (vector dispatch)
//! validate dimensions before calling.
//!
//! Loads are unaligned (`loadu`); no alignment is ever assumed. Only
//! plain multiply+add (no FMA), so no `fma` target feature is required.

use super::super::profile::CpuFeatures;

#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{
    __m256, _mm256_add_ps, _mm256_loadu_ps, _mm256_mul_ps, _mm256_setzero_ps, _mm256_storeu_ps,
    _mm256_sub_ps,
};

#[cfg(target_arch = "aarch64")]
use core::arch::aarch64::{vaddq_f32, vld1q_f32, vmulq_f32, vst1q_f32, vsubq_f32};

/// Portable scalar inner product (also the `#[cfg]` fallback tier).
#[must_use]
pub fn scalar_dot(a: &[f32], b: &[f32]) -> f32 {
    let mut total = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        total += x * y;
    }
    total
}

/// Portable scalar squared Euclidean distance.
#[must_use]
pub fn scalar_l2_squared(a: &[f32], b: &[f32]) -> f32 {
    let mut total = 0.0f32;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = x - y;
        total += d * d;
    }
    total
}

/// AVX2 inner product, 8 lanes of `f32` multiply+add.
///
/// # Safety
///
/// Caller must have confirmed the `avx2` CPU feature. Unaligned loads of
/// input bytes only; the shared-prefix loop never reads past either slice.
#[cfg(target_arch = "x86_64")]
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn avx2_dot(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        // Zipped chunks stop at the shorter slice (shared prefix); equal
        // lengths in production (callers validate dimensions first).
        let mut chunks_a = a.chunks_exact(8);
        let mut chunks_b = b.chunks_exact(8);
        let mut acc: __m256 = _mm256_setzero_ps();
        for (x, y) in (&mut chunks_a).zip(&mut chunks_b) {
            let va = _mm256_loadu_ps(x.as_ptr());
            let vb = _mm256_loadu_ps(y.as_ptr());
            acc = _mm256_add_ps(acc, _mm256_mul_ps(va, vb));
        }
        let mut lanes = [0.0f32; 8];
        _mm256_storeu_ps(lanes.as_mut_ptr(), acc);
        let mut total = 0.0f32;
        for lane in lanes {
            total += lane;
        }
        for (x, y) in chunks_a.remainder().iter().zip(chunks_b.remainder().iter()) {
            total += x * y;
        }
        total
    }
}

/// AVX2 squared Euclidean distance.
///
/// # Safety
///
/// As in [`avx2_dot`].
#[cfg(target_arch = "x86_64")]
#[must_use]
#[target_feature(enable = "avx2")]
pub unsafe fn avx2_l2_squared(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let n = a.len().min(b.len());
        let (head_a, _) = a.split_at(n);
        let mut chunks_a = head_a.chunks_exact(8);
        let mut chunks_b = b.chunks_exact(8);
        let mut acc: __m256 = _mm256_setzero_ps();
        for (x, y) in (&mut chunks_a).zip(&mut chunks_b) {
            let va = _mm256_loadu_ps(x.as_ptr());
            let vb = _mm256_loadu_ps(y.as_ptr());
            let d = _mm256_sub_ps(va, vb);
            acc = _mm256_add_ps(acc, _mm256_mul_ps(d, d));
        }
        let mut lanes = [0.0f32; 8];
        _mm256_storeu_ps(lanes.as_mut_ptr(), acc);
        let mut total = 0.0f32;
        for lane in lanes {
            total += lane;
        }
        for (x, y) in chunks_a.remainder().iter().zip(chunks_b.remainder().iter()) {
            let d = x - y;
            total += d * d;
        }
        total
    }
}

/// NEON inner product, 4 lanes.
///
/// # Safety
///
/// Caller must have confirmed the `neon` CPU feature. Unaligned loads of
/// input bytes only; the shared-prefix loop never reads past either slice.
#[cfg(target_arch = "aarch64")]
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn neon_dot(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let n = a.len().min(b.len());
        let (head_a, _) = a.split_at(n);
        let mut chunks_a = head_a.chunks_exact(4);
        let mut chunks_b = b.chunks_exact(4);
        let mut acc = vld1q_f32([0.0f32; 4].as_ptr());
        for (x, y) in (&mut chunks_a).zip(&mut chunks_b) {
            let va = vld1q_f32(x.as_ptr());
            let vb = vld1q_f32(y.as_ptr());
            acc = vaddq_f32(acc, vmulq_f32(va, vb));
        }
        let mut lanes = [0.0f32; 4];
        vst1q_f32(lanes.as_mut_ptr(), acc);
        let mut total = 0.0f32;
        for lane in lanes {
            total += lane;
        }
        for (x, y) in chunks_a.remainder().iter().zip(chunks_b.remainder().iter()) {
            total += x * y;
        }
        total
    }
}

/// NEON squared Euclidean distance.
///
/// # Safety
///
/// As in [`neon_dot`].
#[cfg(target_arch = "aarch64")]
#[must_use]
#[target_feature(enable = "neon")]
pub unsafe fn neon_l2_squared(a: &[f32], b: &[f32]) -> f32 {
    unsafe {
        let n = a.len().min(b.len());
        let (head_a, _) = a.split_at(n);
        let mut chunks_a = head_a.chunks_exact(8);
        let mut acc = vld1q_f32([0.0f32; 4].as_ptr());
        // 8-wide unroll over two 4-lane accumulators for issue bandwidth.
        let mut acc_hi = vld1q_f32([0.0f32; 4].as_ptr());
        let mut chunks_b = b.chunks_exact(8);
        for (x, y) in (&mut chunks_a).zip(&mut chunks_b) {
            let (xa, xb) = x.split_at(4);
            let (ya, yb) = y.split_at(4);
            let da = vsubq_f32(vld1q_f32(xa.as_ptr()), vld1q_f32(ya.as_ptr()));
            let db = vsubq_f32(vld1q_f32(xb.as_ptr()), vld1q_f32(yb.as_ptr()));
            acc = vaddq_f32(acc, vmulq_f32(da, da));
            acc_hi = vaddq_f32(acc_hi, vmulq_f32(db, db));
        }
        // Remainder below 8 lanes: scalar shared-prefix tail.
        let mut total = 0.0f32;
        let mut lanes = [0.0f32; 4];
        vst1q_f32(lanes.as_mut_ptr(), acc);
        for lane in lanes {
            total += lane;
        }
        vst1q_f32(lanes.as_mut_ptr(), acc_hi);
        for lane in lanes {
            total += lane;
        }
        // `chunks_exact(8)` remainders of both slices, zipped (shared prefix).
        for (x, y) in chunks_a.remainder().iter().zip(chunks_b.remainder().iter()) {
            let d = x - y;
            total += d * d;
        }
        total
    }
}

/// Runtime-dispatched inner product (safe: probes before dispatching).
///
/// Selects the widest probed tier; scalar otherwise. Tolerance contract
/// (relative `1e-4`, absolute floor `1e-5`) holds on every tier.
#[must_use]
pub fn dot_f32(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if CpuFeatures::detect().avx2 {
            // SAFETY: dispatched only after the runtime avx2 probe;
            // unaligned loads; shared-prefix loop.
            return unsafe { avx2_dot(a, b) };
        }
        scalar_dot(a, b)
    }
    #[cfg(target_arch = "aarch64")]
    {
        if CpuFeatures::detect().neon {
            // SAFETY: as above, for neon (additionally mandatory on aarch64).
            return unsafe { neon_dot(a, b) };
        }
        scalar_dot(a, b)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        scalar_dot(a, b)
    }
}

/// Runtime-dispatched squared Euclidean distance (safe; same contract).
#[must_use]
pub fn l2_squared_f32(a: &[f32], b: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if CpuFeatures::detect().avx2 {
            // SAFETY: as in [`dot_f32`].
            return unsafe { avx2_l2_squared(a, b) };
        }
        scalar_l2_squared(a, b)
    }
    #[cfg(target_arch = "aarch64")]
    {
        if CpuFeatures::detect().neon {
            // SAFETY: as in [`dot_f32`].
            return unsafe { neon_l2_squared(a, b) };
        }
        scalar_l2_squared(a, b)
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        scalar_l2_squared(a, b)
    }
}
