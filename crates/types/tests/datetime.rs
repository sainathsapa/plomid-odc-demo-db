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
// Moved from `src/datetime.rs` inline `#[cfg(test)]` — public-API only.
use plomid_types::datetime::{
    date_to_parts, days_in_month, format_date, format_time, format_timestamp, is_leap_year,
    parts_to_date, parts_to_time, parts_to_timestamp, time_to_parts, timestamp_to_parts, DateParts,
    TimeParts,
};

#[test]
fn date_roundtrip() {
    let cases = [
        ("2000-01-01", 0),
        ("1999-12-31", -1),
        ("1970-01-01", -10_957),
        ("2024-02-29", 8825),
        ("2026-09-03", 9742),
        ("10000-01-01", 2_921_940),
        ("0001-01-01", -730_119),
    ];
    for (text, days) in cases {
        let mut split = text.split('-');
        let computed = parts_to_date(DateParts {
            year: split.next().unwrap().parse().unwrap(),
            month: split.next().unwrap().parse().unwrap(),
            day: split.next().unwrap().parse().unwrap(),
        })
        .expect(text);
        assert_eq!(computed, days, "{text}");
        assert_eq!(format_date(days), text);
        assert_eq!(date_to_parts(days), date_to_parts(computed));
    }
    assert!(parts_to_date(DateParts {
        year: 2023,
        month: 2,
        day: 29
    })
    .is_none());
    assert!(parts_to_date(DateParts {
        year: 2023,
        month: 13,
        day: 1
    })
    .is_none());
    assert!(is_leap_year(2000) && is_leap_year(2024) && !is_leap_year(1900));
    assert_eq!(days_in_month(2024, 2), 29);
}

#[test]
fn time_and_timestamp() {
    let t = parts_to_time(TimeParts {
        hour: 13,
        minute: 30,
        second: 5,
        micros: 123_456,
    })
    .unwrap();
    assert_eq!(format_time(t, None), "13:30:05.123456");
    assert_eq!(
        time_to_parts(t),
        TimeParts {
            hour: 13,
            minute: 30,
            second: 5,
            micros: 123_456
        }
    );
    assert!(parts_to_time(TimeParts {
        hour: 24,
        minute: 0,
        second: 0,
        micros: 0
    })
    .is_none());

    let ts = parts_to_timestamp(0, t);
    assert_eq!(format_timestamp(ts, None), "2000-01-01 13:30:05.123456");
    let (d, tt) = timestamp_to_parts(ts);
    assert_eq!(
        d,
        DateParts {
            year: 2000,
            month: 1,
            day: 1
        }
    );
    assert_eq!(tt.micros, 123_456);
    // Negative timestamps (before epoch).
    assert_eq!(format_timestamp(-1, None), "1999-12-31 23:59:59.999999");
}
