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
// Moved from `src/math/dispatch.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::math::dispatch::*;

#[test]
fn scalar_supported_everywhere() {
    assert!(CpuBackend::Scalar.supported_here());
    assert_eq!(CpuBackend::Scalar.name(), "scalar");
    assert!(!CpuBackend::Scalar.is_accelerated());
}

#[test]
fn dispatch_matches_scalar_within_tolerance() {
    // Tiers use `f32` lane accumulation vs the oracle's `f64`: agreement
    // within relative 1e-4 (absolute floor 1e-5), never bit-identity.
    let a: Vec<f32> = (0..129).map(|i| (i as f32) * 0.125 - 8.0).collect();
    let b: Vec<f32> = (0..129).map(|i| (i as f32) * -0.0625 + 4.0).collect();
    let close = |got: f32, exact: f32| {
        let diff = (got - exact).abs();
        diff <= 1e-5 || diff <= exact.abs() * 1e-4
    };
    let (tier, d) = dot(&a, &b);
    assert!(close(
        d.unwrap(),
        plomid_vector::math::scalar::dot_f32(&a, &b).unwrap()
    ));
    // The reported tier is the executed tier (never an aspiration).
    let host = plomid_hw::profile::CpuFeatures::detect();
    if tier != CpuBackend::Scalar {
        assert!(
            (tier == CpuBackend::Neon || tier == CpuBackend::Sve) && host.neon
                || (tier == CpuBackend::Avx2 || tier == CpuBackend::Avx512)
                    && (host.avx2 || host.avx512)
        );
    }
    let (_, l2) = l2_squared(&a, &b);
    assert!(close(
        l2.unwrap(),
        plomid_vector::math::scalar::l2_squared_f32(&a, &b).unwrap()
    ));
    let (_, cos) = cosine_distance(&a, &b);
    match (
        cos.unwrap(),
        plomid_vector::math::scalar::cosine_distance_f32(&a, &b).unwrap(),
    ) {
        (Some(got), Some(exact)) => assert!(close(got, exact)),
        (None, None) => {}
        (got, exact) => panic!("cosine presence diverged: {got:?} vs {exact:?}"),
    }
}

#[test]
fn unknown_env_falls_back_to_auto() {
    std::env::set_var("PLOMID_VECTOR_SIMD_TEST_SENTINEL", "1");
    // Unknown values must not panic; result equals auto-detection.
    let _ = from_name("definitely-not-a-tier");
    std::env::remove_var("PLOMID_VECTOR_SIMD_TEST_SENTINEL");
}
