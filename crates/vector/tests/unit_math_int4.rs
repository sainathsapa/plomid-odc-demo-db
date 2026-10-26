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
// Moved from `src/math/int4.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::math::int4::*;
use plomid_vector::math::MathError;

#[test]
fn pack_unpack_roundtrip_even_and_odd() {
    let v: Vec<i8> = vec![-8, -1, 0, 1, 7, -4];
    let p = pack_i4(&v).unwrap();
    assert_eq!(p.len(), 3);
    assert_eq!(unpack_i4(&p, v.len()).unwrap(), v);

    let odd: Vec<i8> = vec![7, -8, 0];
    let po = pack_i4(&odd).unwrap();
    assert_eq!(po.len(), 2);
    assert_eq!(unpack_i4(&po, odd.len()).unwrap(), odd);

    let u: Vec<u8> = vec![0, 15, 8, 1, 9];
    let pu = pack_u4(&u).unwrap();
    assert_eq!(unpack_u4(&pu, u.len()).unwrap(), u);
}

#[test]
fn pack_validates_range() {
    assert_eq!(
        pack_i4(&[8]),
        Err(MathError::OutOfRange {
            value: 8,
            lo: -8,
            hi: 7
        })
    );
    assert_eq!(
        pack_i4(&[-9]),
        Err(MathError::OutOfRange {
            value: -9,
            lo: -8,
            hi: 7
        })
    );
    assert!(pack_u4(&[16]).is_err());
}

#[test]
fn packed_dot_matches_unpacked() {
    let a: Vec<i8> = vec![-8, 3, -1, 7, 0];
    let b: Vec<i8> = vec![2, -4, 5, -7, 6];
    let pa = pack_i4(&a).unwrap();
    let pb = pack_i4(&b).unwrap();
    let expect: i64 = a
        .iter()
        .zip(&b)
        .map(|(&x, &y)| i64::from(x) * i64::from(y))
        .sum();
    assert_eq!(dot_i4_packed(&pa, &pb, a.len()).unwrap(), expect);
    let expect_l2: i64 = a
        .iter()
        .zip(&b)
        .map(|(&x, &y)| {
            let d = i64::from(x) - i64::from(y);
            d * d
        })
        .sum();
    assert_eq!(l2_squared_i4_packed(&pa, &pb, a.len()).unwrap(), expect_l2);
    // Wrong packed length is an error, not a misread.
    assert!(dot_i4_packed(&pa[..1], &pb, a.len()).is_err());
}
