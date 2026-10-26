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
// Moved from `src/cast.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::PgValue::*;
use plomid_types::{apply_cast, decode_typmod, resolve_type_name, PgType, NO_TYPEMOD};

#[test]
fn cast_int_to_text() {
    let v = apply_cast(Int4(42), PgType::Int4.oid(), PgType::Text.oid(), NO_TYPEMOD).unwrap();
    assert_eq!(v, Text("42".into()));
}

#[test]
fn cast_text_to_int4() {
    let v = apply_cast(
        Text("123".into()),
        PgType::Text.oid(),
        PgType::Int4.oid(),
        NO_TYPEMOD,
    )
    .unwrap();
    assert_eq!(v, Int4(123));
}

#[test]
fn cast_int4_numeric_promotion() {
    let v = apply_cast(
        Int4(10),
        PgType::Int4.oid(),
        PgType::Numeric.oid(),
        NO_TYPEMOD,
    )
    .unwrap();
    assert!(matches!(v, Numeric(_)));
}

#[test]
fn cast_varchar_with_typmod_error() {
    let (_, tm) = resolve_type_name("varchar(5)").unwrap();
    let v = apply_cast(
        Text("hello world".into()),
        PgType::Text.oid(),
        PgType::VarChar.oid(),
        tm,
    );
    assert!(v.is_err());
}

#[test]
fn cast_null_propagates() {
    let v = apply_cast(Null, PgType::Int4.oid(), PgType::Text.oid(), NO_TYPEMOD).unwrap();
    assert_eq!(v, Null);
}

#[test]
fn resolve_type_name_with_typmod() {
    let (ty, tm) = resolve_type_name("numeric(10,2)").unwrap();
    assert_eq!(ty, PgType::Numeric);
    assert_eq!(decode_typmod(ty, tm), Some((10, 2)));
}

#[test]
fn oid_to_int4_and_back() {
    let v = apply_cast(
        Oid(12345),
        PgType::Oid.oid(),
        PgType::Int4.oid(),
        NO_TYPEMOD,
    )
    .unwrap();
    assert_eq!(v, Int4(12345));
    let v = apply_cast(v, PgType::Int4.oid(), PgType::Oid.oid(), NO_TYPEMOD).unwrap();
    assert_eq!(v, Oid(12345));
}
