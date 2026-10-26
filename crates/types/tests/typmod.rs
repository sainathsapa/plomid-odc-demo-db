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
// Moved from `src/typmod.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::{
    apply_typmod, decode_typmod, encode_typmod, Numeric, PgType, PgValue, TypmodSpec, NO_TYPEMOD,
};

#[test]
fn spec_parsing() {
    assert_eq!(TypmodSpec::parse("").unwrap().count, 0);
    assert_eq!(TypmodSpec::parse("(10, 2)").unwrap().args, [10, 2]);
    assert!(TypmodSpec::parse("(10,2,3)").is_err());
    assert!(TypmodSpec::parse("(-1)").is_err());
    assert!(TypmodSpec::parse("(a)").is_err());
}

#[test]
fn string_typmods() {
    let spec = TypmodSpec::parse("(5)").unwrap();
    assert_eq!(encode_typmod(PgType::VarChar, spec).unwrap(), 9);
    assert_eq!(
        encode_typmod(
            PgType::VarChar,
            TypmodSpec {
                args: [0, 0],
                count: 0
            }
        )
        .unwrap(),
        NO_TYPEMOD
    );
    assert!(encode_typmod(PgType::VarChar, TypmodSpec::parse("(0)").unwrap()).is_err());
    assert!(encode_typmod(PgType::VarChar, TypmodSpec::parse("(1,2)").unwrap()).is_err());
    let v = PgValue::VarChar("hello!".into());
    assert!(apply_typmod(v.clone(), 9, PgType::VarChar).is_err());
    assert!(apply_typmod(PgValue::VarChar("hi".into()), 9, PgType::VarChar).is_ok());
    // char(n) blank pads.
    let padded = apply_typmod(PgValue::BpChar("ab".into()), 9, PgType::BpChar).unwrap();
    assert_eq!(padded, PgValue::BpChar("ab   ".to_string()));
}

#[test]
fn numeric_typmods() {
    let spec = TypmodSpec::parse("(10,2)").unwrap();
    let typmod = encode_typmod(PgType::Numeric, spec).unwrap();
    assert_eq!(decode_typmod(PgType::Numeric, typmod), Some((10, 2)));
    assert!(encode_typmod(PgType::Numeric, TypmodSpec::parse("(10,11)").unwrap()).is_err());
    assert!(encode_typmod(PgType::Numeric, TypmodSpec::parse("(0)").unwrap()).is_err());
    // Value coerced to scale 2 with PostgreSQL-compatible rounding.
    let out = apply_typmod(
        PgValue::Numeric(Numeric::parse("1.999").unwrap()),
        typmod,
        PgType::Numeric,
    )
    .unwrap();
    // 1.999 rounds to 2.00 (half away from zero), displayed as "2.00"
    assert_eq!(out.to_sql_text(), "2.00");
    // Precision overflow errors.
    let big = encode_typmod(PgType::Numeric, TypmodSpec::parse("(5,2)").unwrap()).unwrap();
    assert!(apply_typmod(
        PgValue::Numeric(Numeric::parse("123456.78").unwrap()),
        big,
        PgType::Numeric
    )
    .is_err());
}

#[test]
fn time_typmods() {
    let typmod = encode_typmod(PgType::Timestamp, TypmodSpec::parse("(3)").unwrap()).unwrap();
    // PostgreSQL rounds 1.234567 s to the nearest millisecond (1.235000).
    let out = apply_typmod(PgValue::Timestamp(1_234_567), typmod, PgType::Timestamp).unwrap();
    assert_eq!(out, PgValue::Timestamp(1_235_000));
    // A value already on a whole millisecond is unchanged.
    let exact = apply_typmod(PgValue::Timestamp(1_234_000), typmod, PgType::Timestamp).unwrap();
    assert_eq!(exact, PgValue::Timestamp(1_234_000));
    // Rounding may carry across a second/day boundary (PostgreSQL 17).
    let bound = apply_typmod(PgValue::Timestamp(1_234_567_999), typmod, PgType::Timestamp).unwrap();
    assert_eq!(bound, PgValue::Timestamp(1_234_568_000));
    let p0 = encode_typmod(PgType::Timestamp, TypmodSpec::parse("(0)").unwrap()).unwrap();
    let carry = apply_typmod(PgValue::Timestamp(1_234_999_999), p0, PgType::Timestamp).unwrap();
    assert_eq!(carry, PgValue::Timestamp(1_235_000_000));
    assert!(encode_typmod(PgType::Timestamp, TypmodSpec::parse("(7)").unwrap()).is_err());
    assert!(encode_typmod(PgType::Int4, TypmodSpec::parse("(3)").unwrap()).is_err());
}

#[test]
fn bit_typmods() {
    let typmod = encode_typmod(PgType::Bit, TypmodSpec::parse("(4)").unwrap()).unwrap();
    let ok = apply_typmod(
        PgValue::Bit {
            len: 4,
            bytes: vec![0xF0],
        },
        typmod,
        PgType::Bit,
    );
    assert!(ok.is_ok());
    let bad = apply_typmod(
        PgValue::Bit {
            len: 5,
            bytes: vec![0xF8],
        },
        typmod,
        PgType::Bit,
    );
    assert!(bad.is_err());
}

#[test]
fn vector_typmod_formats_with_declared_dimension() {
    let typmod = encode_typmod(PgType::Vector, TypmodSpec::parse("(768)").unwrap()).unwrap();

    assert_eq!(decode_typmod(PgType::Vector, typmod), Some((768, 0)));
    assert_eq!(
        plomid_types::coerce::format_type_typmod_suffix(PgType::Vector, typmod),
        Some("(768)".to_string())
    );
}
