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
// Moved from `src/math/bfloat16.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::math::bfloat16::*;

#[test]
fn decode_known_values() {
    assert_eq!(bf16_to_f32(0x3f80), 1.0);
    assert_eq!(bf16_to_f32(0xbf80), -1.0);
    assert_eq!(bf16_to_f32(0x4000), 2.0);
    assert_eq!(bf16_to_f32(0x0000), 0.0);
    assert_eq!(bf16_to_f32(0x7f80), f32::INFINITY);
    assert!(bf16_to_f32(0x7fc0).is_nan());
}

#[test]
fn encode_rounding_ties_to_even() {
    assert_eq!(f32_to_bf16(1.0), 0x3f80);
    assert_eq!(f32_to_bf16(-1.0), 0xbf80);
    assert_eq!(f32_to_bf16(f32::INFINITY), 0x7f80);
    // Just below halfway between 0x3f80 and 0x3f81 -> rounds down.
    assert_eq!(f32_to_bf16(f32::from_bits(0x3f80_7fff)), 0x3f80);
    // Exact halfway, kept LSB even (0x3f80) -> stays.
    assert_eq!(f32_to_bf16(f32::from_bits(0x3f80_8000)), 0x3f80);
    // Just above halfway -> rounds up.
    assert_eq!(f32_to_bf16(f32::from_bits(0x3f80_8001)), 0x3f81);
    // Exact halfway, kept LSB odd (0x3f81) -> rounds up to even 0x3f82.
    assert_eq!(f32_to_bf16(f32::from_bits(0x3f81_8000)), 0x3f82);
    // NaN stays NaN.
    assert!(bf16_to_f32(f32_to_bf16(f32::NAN)).is_nan());
}

#[test]
fn kernels() {
    let a = convert_from_f32_vec(&[1.0, 2.0, 3.0]);
    let b = convert_from_f32_vec(&[4.0, 5.0, 6.0]);
    assert!((dot(&a, &b).unwrap() - 32.0).abs() < 0.05);
    assert!((l2_squared(&a, &b).unwrap() - 27.0).abs() < 0.05);
    assert!((l2_distance(&a, &b).unwrap() - 27.0f32.sqrt()).abs() < 0.01);
    let zero = convert_from_f32_vec(&[0.0, 0.0]);
    let one = convert_from_f32_vec(&[1.0, 0.0]);
    assert_eq!(cosine_distance(&zero, &one), Ok(None));
    assert!(matches!(cosine_distance(&one, &one).unwrap(), Some(v) if v.abs() < 1e-6));
    assert!(dot(&a, &b[..2]).is_err());
}
