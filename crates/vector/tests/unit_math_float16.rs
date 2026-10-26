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
// Moved from `src/math/float16.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::math::float16::*;

#[test]
fn decode_known_values() {
    assert_eq!(f16_to_f32(0x3c00), 1.0);
    assert_eq!(f16_to_f32(0xbc00), -1.0);
    assert_eq!(f16_to_f32(0x4000), 2.0);
    assert_eq!(f16_to_f32(0x0000), 0.0);
    assert_eq!(f16_to_f32(0x8000).to_bits(), (-0.0f32).to_bits());
    assert_eq!(f16_to_f32(0x7c00), f32::INFINITY);
    assert_eq!(f16_to_f32(0xfc00), f32::NEG_INFINITY);
    assert!(f16_to_f32(0x7e00).is_nan());
    assert_eq!(f16_to_f32(0x7bff), 65504.0);
    assert_eq!(f16_to_f32(0x0400), 2.0f32.powi(-14));
    // Smallest subnormal: 2^-24.
    assert_eq!(f16_to_f32(0x0001), 2.0f32.powi(-24));
    assert_eq!(f16_to_f32(0x0002), 2.0 * 2.0f32.powi(-24));
}

#[test]
fn encode_rounding() {
    assert_eq!(f32_to_f16(1.0), 0x3c00);
    assert_eq!(f32_to_f16(-2.0), 0xc000);
    assert_eq!(f32_to_f16(0.0), 0x0000);
    assert_eq!(f32_to_f16(f32::INFINITY), 0x7c00);
    assert_eq!(f32_to_f16(1e6), 0x7c00); // overflow -> inf
    assert_eq!(f32_to_f16(1e-9), 0x0000); // underflow -> zero
    assert!((f32_to_f16(f32::NAN) & 0x7c00) == 0x7c00);
    // Ties to even: adjacent f16 values near 1.0 are 0x3c00/0x3c01,
    // i.e. f32 0x3f800000/0x3f802000; halfway is 0x3f801000.
    let halfway_down = f32::from_bits(0x3f80_0fff); // just below halfway
    let halfway = f32::from_bits(0x3f80_1000); // exact halfway
    let halfway_up = f32::from_bits(0x3f80_1001); // just above
    assert_eq!(f32_to_f16(halfway_down), 0x3c00);
    assert_eq!(f32_to_f16(halfway), 0x3c00); // tie -> even
    assert_eq!(f32_to_f16(halfway_up), 0x3c01);
    // Halfway between 0x3c01 (odd LSB) and 0x3c02 -> rounds up to even.
    // 0x3c01/0x3c02 are f32 0x3f802000/0x3f804000; halfway 0x3f803000.
    let halfway_odd = f32::from_bits(0x3f80_3000);
    assert_eq!(f32_to_f16(halfway_odd), 0x3c02);
}

#[test]
fn roundtrip_representable() {
    for bits in [0x3c00u16, 0x4200, 0xc000, 0x7bff, 0x0400, 0x0001, 0x3555] {
        assert_eq!(f32_to_f16(f16_to_f32(bits)), bits, "bits {bits:#x}");
    }
}

#[test]
fn kernels() {
    let a = convert_from_f32_vec(&[1.0, 2.0, 3.0]);
    let b = convert_from_f32_vec(&[4.0, 5.0, 6.0]);
    assert!((dot(&a, &b).unwrap() - 32.0).abs() < 1e-3);
    assert!((l2_squared(&a, &b).unwrap() - 27.0).abs() < 1e-3);
    assert!((l2_distance(&a, &b).unwrap() - 27.0f32.sqrt()).abs() < 1e-4);
    let ortho_a = convert_from_f32_vec(&[1.0, 0.0]);
    let ortho_b = convert_from_f32_vec(&[0.0, 1.0]);
    assert!(
        matches!(cosine_distance(&ortho_a, &ortho_b).unwrap(), Some(v) if (v - 1.0).abs() < 1e-3)
    );
    let zero = convert_from_f32_vec(&[0.0, 0.0]);
    assert_eq!(cosine_distance(&zero, &ortho_a), Ok(None));
    assert!(dot(&a, &b[..2]).is_err());
}
