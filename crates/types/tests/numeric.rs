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
// Moved from `src/numeric.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::Numeric;

#[test]
fn parse_and_display() {
    let cases = [
        ("0", "0"),
        ("42", "42"),
        ("-42", "-42"),
        ("3.14", "3.14"),
        ("0.10", "0.1"),
        ("1e3", "1000"),
        ("1.5e2", "150"),
        ("2.5e-2", "0.025"),
        ("007.5", "7.5"),
        ("-0.000", "0"),
    ];
    for (input, expected) in cases {
        let value = Numeric::parse(input).expect(input);
        assert_eq!(value.to_string(), expected, "input {input}");
    }
    assert!(Numeric::parse("abc").is_err());
    assert!(Numeric::parse("1.2.3").is_err());
    assert!(Numeric::parse("").is_err());
    assert!(Numeric::parse("1e").is_err());
}

#[test]
fn arithmetic() {
    let a = Numeric::parse("1.10").unwrap();
    let b = Numeric::parse("2.05").unwrap();
    assert_eq!(a.clone().add(b.clone()).unwrap().to_string(), "3.15");
    assert_eq!(b.sub(a.clone()).unwrap().to_string(), "0.95");
    let c = Numeric::parse("1.5").unwrap();
    let d = Numeric::from_i64(2);
    assert_eq!(c.clone().mul(d.clone()).unwrap().to_string(), "3.0");
    assert_eq!(c.div(d).unwrap().to_string(), "0.7500");
    assert_eq!(
        Numeric::from_i64(7)
            .rem(Numeric::from_i64(3))
            .unwrap()
            .to_string(),
        "1"
    );
    assert!(Numeric::from_i64(1).div(Numeric::from_i64(0)).is_err());
}

#[test]
fn ordering() {
    let a = Numeric::parse("-1.5").unwrap();
    let b = Numeric::from_i64(2);
    assert!(a < b);
    assert_eq!(
        a.cmp(&Numeric::parse("-1.50").unwrap()),
        std::cmp::Ordering::Equal
    );
    assert!(Numeric::from_i64(10) > Numeric::parse("9.99").unwrap());
}

#[test]
fn conversions() {
    assert_eq!(Numeric::from_i64(-5).to_i64(), Some(-5));
    assert_eq!(Numeric::parse("1.5").unwrap().to_i64(), None);
    assert_eq!(Numeric::from_f64(1.25).to_f64(), 1.25);
}
