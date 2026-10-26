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
// Moved from src/join/statement.rs inline #[cfg(test)].
mod generate_series_cap_tests {

    use plomid_executor::join::statement::{
        generate_series_element_values, generate_series_row_count, MAX_GENERATE_SERIES_ROWS,
    };

    use plomid_sql::Value;

    fn ints(values: &[Value]) -> Vec<i64> {
        values
            .iter()
            .map(|v| match v {
                Value::Int8(n) => *n,
                other => panic!("expected Int8, got {other:?}"),
            })
            .collect()
    }

    #[test]
    fn row_count_math_is_exact() {
        assert_eq!(generate_series_row_count(1, 3, 1).unwrap(), 3);
        assert_eq!(generate_series_row_count(1, 10, 3).unwrap(), 4);
        assert_eq!(generate_series_row_count(3, 1, -1).unwrap(), 3);
        assert_eq!(generate_series_row_count(10, 1, -3).unwrap(), 4);
        assert_eq!(generate_series_row_count(5, 1, 1).unwrap(), 0);
        assert_eq!(generate_series_row_count(1, 5, -1).unwrap(), 0);
        assert_eq!(generate_series_row_count(7, 7, 1).unwrap(), 1);
        assert_eq!(
            generate_series_row_count(1, MAX_GENERATE_SERIES_ROWS as i64, 1).unwrap(),
            MAX_GENERATE_SERIES_ROWS as u64
        );
    }

    #[test]
    fn row_count_over_cap_fails_closed() {
        let over = MAX_GENERATE_SERIES_ROWS as i64 + 1;
        for (start, stop, step) in [(1, over, 1), (over, 1, -1), (1, 2 * over, 2)] {
            let err = generate_series_row_count(start, stop, step).unwrap_err();
            let message = err.to_string();
            assert!(
                message.contains("exceeds the maximum"),
                "unexpected error: {message}"
            );
        }
    }

    #[test]
    fn element_values_boundary() {
        let small = generate_series_element_values(&[Value::Int8(1), Value::Int8(3)]).unwrap();
        assert_eq!(ints(&small), vec![1, 2, 3]);
        // Exactly at the cap: allowed.
        let at_cap = generate_series_element_values(&[
            Value::Int8(1),
            Value::Int8(MAX_GENERATE_SERIES_ROWS as i64),
        ])
        .unwrap();
        assert_eq!(at_cap.len(), MAX_GENERATE_SERIES_ROWS);
        // One past the cap: graceful error, no billion-element allocation.
        let err = generate_series_element_values(&[
            Value::Int8(1),
            Value::Int8(MAX_GENERATE_SERIES_ROWS as i64 + 1),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("exceeds the maximum"));
    }
}
