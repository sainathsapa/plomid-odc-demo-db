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
// Moved from `src/text.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::text::{format_value_with_typmod, parse_value};
use plomid_types::{PgType, PgValue};

#[test]
fn text_roundtrip_scalars() {
    let cases = [
        (PgType::Int4, "42", "42"),
        (PgType::Int8, "-9000000000000000001", "-9000000000000000001"),
        (PgType::Numeric, "3.140", "3.14"),
        (PgType::Bool, "yes", "t"),
        (PgType::Bool, "off", "f"),
        (PgType::Date, "2024-02-29", "2024-02-29"),
        (PgType::Time, "13:30", "13:30:00"),
        (
            PgType::Timestamp,
            "2000-01-01 00:00:00",
            "2000-01-01 00:00:00",
        ),
        (PgType::Interval, "1 year 2 mons", "1 year 2 mons"),
        (PgType::Interval, "04:05:06", "04:05:06"),
        (PgType::Money, "$12.05", "$12.05"),
        (PgType::Text, "hello", "hello"),
        (
            PgType::Uuid,
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
            "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
        ),
    ];
    for (ty, input, expected) in cases {
        let value = parse_value(input, ty).expect(input);
        assert_eq!(
            format_value_with_typmod(&value, None),
            expected,
            "{ty:?} from {input}"
        );
    }
}

#[test]
fn invalid_literals_error() {
    assert!(parse_value("nope", PgType::Int4).is_err());
    assert!(parse_value("2023-02-29", PgType::Date).is_err());
    assert!(parse_value("25:00:00", PgType::Time).is_err());
    assert!(parse_value("xx", PgType::Uuid).is_err());
    assert!(parse_value("maybe", PgType::Bool).is_err());
    assert!(parse_value("x", PgType::Trigger).is_err());
}

#[test]
fn special_floats_and_bytea() {
    let nan = parse_value("NaN", PgType::Float8).unwrap();
    assert_eq!(format_value_with_typmod(&nan, None), "NaN");
    let inf = parse_value("-Infinity", PgType::Float8).unwrap();
    assert_eq!(format_value_with_typmod(&inf, None), "-Infinity");
    let bytes = parse_value("\\xdeadbeef", PgType::Bytea).unwrap();
    assert_eq!(format_value_with_typmod(&bytes, None), "\\xdeadbeef");
}

#[test]
fn null_and_unknown() {
    assert_eq!(format_value_with_typmod(&PgValue::Null, None), "NULL");
    let unknown = parse_value("abc", PgType::Unknown).unwrap();
    assert_eq!(format_value_with_typmod(&unknown, None), "abc");
}

#[test]
fn inet_text_input() {
    // IPv4 without prefix — PostgreSQL renders as "127.0.0.1" (prefix=32
    // suppressed when it equals the max for the address family).
    let v = parse_value("127.0.0.1", PgType::Inet).expect("parse inet");
    assert_eq!(format_value_with_typmod(&v, None), "127.0.0.1");
    // IPv4 with /24 prefix.
    let v = parse_value("192.168.1.0/24", PgType::Inet).expect("parse inet/24");
    assert_eq!(format_value_with_typmod(&v, None), "192.168.1.0/24");
    // IPv6 without prefix — rendered as "::1" (prefix=128 suppressed).
    let v = parse_value("::1", PgType::Inet).expect("parse ipv6");
    assert_eq!(format_value_with_typmod(&v, None), "::1");
    // CIDR type: prefix is preserved by construction and rendered the same way.
    let v = parse_value("10.0.0.0/8", PgType::Cidr).expect("parse cidr");
    assert_eq!(format_value_with_typmod(&v, None), "10.0.0.0/8");
}

#[test]
fn inet_invalid_input() {
    assert!(parse_value("not-an-ip", PgType::Inet).is_err());
    // Prefix exceeding the address family width is rejected.
    assert!(parse_value("127.0.0.1/99", PgType::Inet).is_err());
}

#[test]
fn inet_roundtrip_matches_format() {
    let cases = [
        ("127.0.0.1", "127.0.0.1"),
        ("192.168.1.0/24", "192.168.1.0/24"),
        ("::1", "::1"),
        ("2001:db8::/32", "2001:db8::/32"),
    ];
    for (input, expected) in cases {
        let v = parse_value(input, PgType::Inet).expect(input);
        assert_eq!(
            format_value_with_typmod(&v, None),
            expected,
            "inet roundtrip {input}"
        );
    }
}
