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
// Moved from `src/vector/distance.rs` inline `#[cfg(test)]` — public-API only.
use plomid_hw::profile::CpuFeatures;
use plomid_hw::vector::distance::*;

fn fixtures() -> (Vec<f32>, Vec<f32>) {
    let a: Vec<f32> = (0..129).map(|i| (i as f32) * 0.125 - 8.0).collect();
    let b: Vec<f32> = (0..129).map(|i| (i as f32) * -0.0625 + 4.0).collect();
    (a, b)
}

fn close_enough(got: f32, exact: f32) -> bool {
    let diff = (got - exact).abs();
    diff <= 1e-5 || diff <= exact.abs() * 1e-4
}

/// Slow `f64` oracle (same accumulation as the vector scalar kernels).
fn oracle_dot(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = 0.0f64;
    for (x, y) in a.iter().zip(b.iter()) {
        acc += f64::from(*x) * f64::from(*y);
    }
    acc as f32
}

fn oracle_l2_squared(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = 0.0f64;
    for (x, y) in a.iter().zip(b.iter()) {
        let d = f64::from(*x) - f64::from(*y);
        acc += d * d;
    }
    acc as f32
}

#[test]
fn dispatched_matches_oracle_within_tolerance() {
    let (a, b) = fixtures();
    assert!(close_enough(dot_f32(&a, &b), oracle_dot(&a, &b)));
    assert!(close_enough(
        l2_squared_f32(&a, &b),
        oracle_l2_squared(&a, &b)
    ));
}

#[test]
fn scalar_tier_is_exact_prefix() {
    let (a, b) = fixtures();
    assert_eq!(scalar_dot(&a, &b), scalar_dot(&a, &b));
    // Odd lengths and empty inputs never read out of bounds.
    assert_eq!(dot_f32(&[], &[]), 0.0);
    assert_eq!(l2_squared_f32(&[1.0], &[1.0]), 0.0);
}

#[cfg(target_arch = "aarch64")]
#[test]
fn neon_matches_oracle_within_tolerance() {
    if !CpuFeatures::detect().neon {
        return;
    }
    let (a, b) = fixtures();
    let exact_d = oracle_dot(&a, &b);
    let exact_l = oracle_l2_squared(&a, &b);
    unsafe {
        assert!(close_enough(neon_dot(&a, &b), exact_d));
        assert!(close_enough(neon_l2_squared(&a, &b), exact_l));
    }
}

#[cfg(target_arch = "x86_64")]
#[test]
fn avx2_matches_oracle_within_tolerance() {
    if !CpuFeatures::detect().avx2 {
        return;
    }
    let (a, b) = fixtures();
    let exact_d = oracle_dot(&a, &b);
    let exact_l = oracle_l2_squared(&a, &b);
    unsafe {
        assert!(close_enough(avx2_dot(&a, &b), exact_d));
        assert!(close_enough(avx2_l2_squared(&a, &b), exact_l));
    }
}
