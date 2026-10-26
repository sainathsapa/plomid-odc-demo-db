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
// Moved from src/join/support.rs inline #[cfg(test)].
mod parallel_join_tests {

    use plomid_executor::join::support::*;
    use plomid_executor::join::{
        reorder::{choose_build_side, BuildSide},
        scope::{synthetic_schema, JoinScope},
    };

    use plomid_sql::{Expression, JoinKind, Value};

    fn test_scopes(left_cols: &[&str], right_cols: &[&str]) -> Vec<JoinScope> {
        let left_width = left_cols.len();
        let lschema = synthetic_schema(
            "jl",
            &left_cols.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            &vec![None; left_cols.len()],
        );
        let rschema = synthetic_schema(
            "jr",
            &right_cols.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            &vec![None; right_cols.len()],
        );
        vec![
            JoinScope {
                alias: "jl".to_string(),
                schema: lschema,
                offset: 0,
                is_srf: false,
            },
            JoinScope {
                alias: "jr".to_string(),
                schema: rschema,
                offset: left_width,
                is_srf: false,
            },
        ]
    }

    fn equi_on(left: &str, right: &str) -> Expression {
        Expression::Equal(
            Box::new(Expression::ColumnRef(left.to_string())),
            Box::new(Expression::ColumnRef(right.to_string())),
        )
    }

    /// Uniform probe rows above the parallel threshold: NULL every 11th key,
    /// else `Int4(i % 500)` with payload `Int8(i)`.
    fn big_probe(n: usize) -> Vec<Vec<Value>> {
        let mut rows = Vec::with_capacity(n);
        for i in 0..n as i64 {
            let key = if i % 11 == 0 {
                Value::Null
            } else {
                Value::Int4(i as i32 % 500)
            };
            rows.push(vec![key, Value::Int8(i)]);
        }
        rows
    }

    /// Unique build keys `0..500` with a text payload.
    fn build_500() -> Vec<Vec<Value>> {
        (0..500_i32)
            .map(|k| vec![Value::Int4(k), Value::Text(format!("w{k}"))])
            .collect()
    }

    #[test]
    fn parallel_gate_declines_small_inputs() {
        let scopes = test_scopes(&["k", "v"], &["k", "w"]);
        let on = equi_on("jl.k", "jr.k");
        let lrows = vec![vec![Value::Int4(1), Value::Int8(1)]];
        let rrows = vec![vec![Value::Int4(1), Value::Text("w".to_string())]];
        assert!(
            perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                .expect("parallel join must not error")
                .is_none(),
            "sub-threshold inputs must decline so the sequential path runs"
        );
    }

    #[test]
    fn tiered_join_threshold_gates_per_tier() {
        // Threshold gating per tier with mocked permits: the same 3_000-row
        // hashable equi-INNER join declines on Edge (threshold 8_000) and
        // engages on Laptop (2_000) and Server (1_000). Each pin holds the
        // crate serial, so concurrent gate readers cannot interleave.
        use plomid_executor::parallel_budget::{pinned_tier_for_tests, with_test_tier};
        use plomid_hw::RuntimeTier;
        let scopes = test_scopes(&["k", "v"], &["k", "w"]);
        let on = equi_on("jl.k", "jr.k");
        let lrows = big_probe(2_500);
        let rrows = build_500();
        assert_eq!(lrows.len() + rrows.len(), 3_000);
        with_test_tier(pinned_tier_for_tests(RuntimeTier::Edge), || {
            assert!(
                perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                    .expect("parallel join must not error")
                    .is_none(),
                "edge tier must decline a 3_000-row join (threshold 8_000)"
            );
        });
        with_test_tier(pinned_tier_for_tests(RuntimeTier::Laptop), || {
            assert!(
                perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                    .expect("parallel join must not error")
                    .is_some(),
                "laptop tier must engage a 3_000-row join (threshold 2_000)"
            );
        });
        with_test_tier(pinned_tier_for_tests(RuntimeTier::Server), || {
            assert!(
                perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                    .expect("parallel join must not error")
                    .is_some(),
                "large tier must engage a 3_000-row join (threshold 1_000)"
            );
        });
    }

    #[test]
    fn parallel_gate_declines_single_worker() {
        let scopes = test_scopes(&["k", "v"], &["k", "w"]);
        let on = equi_on("jl.k", "jr.k");
        let lrows = big_probe(PARALLEL_JOIN_THRESHOLD);
        let rrows = build_500();
        assert!(
            perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 1)
                .expect("parallel join must not error")
                .is_none(),
            "fewer than 2 workers must decline"
        );
    }

    #[test]
    fn parallel_gate_declines_non_inner() {
        let scopes = test_scopes(&["k", "v"], &["k", "w"]);
        let on = equi_on("jl.k", "jr.k");
        let lrows = big_probe(PARALLEL_JOIN_THRESHOLD);
        let rrows = build_500();
        for kind in [JoinKind::Left, JoinKind::Right, JoinKind::Full] {
            assert!(
                perform_join_parallel(kind, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                    .expect("parallel join must not error")
                    .is_none(),
                "{kind:?} must decline so outer-join NULL padding stays sequential"
            );
        }
    }

    #[test]
    fn parallel_gate_declines_non_equi() {
        let scopes = test_scopes(&["k", "v"], &["k", "w"]);
        let lrows = big_probe(PARALLEL_JOIN_THRESHOLD);
        let rrows = build_500();
        // No predicate at all.
        assert!(
            perform_join_parallel(JoinKind::Inner, None, &scopes, 2, &lrows, &rrows, 4)
                .expect("parallel join must not error")
                .is_none()
        );
        // Both columns on the same side: not a cross-side equi-join.
        let same_side = equi_on("jl.k", "jl.v");
        assert!(perform_join_parallel(
            JoinKind::Inner,
            Some(&same_side),
            &scopes,
            2,
            &lrows,
            &rrows,
            4
        )
        .expect("parallel join must not error")
        .is_none());
    }

    #[test]
    fn parallel_gate_declines_unhashable_keys() {
        let scopes = test_scopes(&["k", "v"], &["k", "w"]);
        let on = equi_on("jl.k", "jr.k");
        // Unhashable build key (`Point` has no `join_hash_key`).
        let mut lrows = big_probe(PARALLEL_JOIN_THRESHOLD);
        let mut rrows = build_500();
        rrows[7][0] = Value::Point { x: 1.0, y: 2.0 };
        assert!(
            perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                .expect("parallel join must not error")
                .is_none(),
            "unhashable build key must decline to sequential nested-loop"
        );
        // Unhashable probe key.
        let rrows = build_500();
        lrows[13][0] = Value::Point { x: 3.0, y: 4.0 };
        assert!(
            perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                .expect("parallel join must not error")
                .is_none(),
            "unhashable probe key must decline to sequential nested-loop"
        );
    }

    #[test]
    fn parallel_gate_declines_session_function_keys() {
        // A column named like a session function: the evaluator substitutes
        // the session value instead of reading the column, so the hash
        // partitioner would read a different value than the sequential path.
        let scopes = test_scopes(&["current_user", "v"], &["k", "w"]);
        let on = equi_on("jl.current_user", "jr.k");
        let lrows = big_probe(PARALLEL_JOIN_THRESHOLD);
        let rrows = build_500();
        assert!(
            perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                .expect("parallel join must not error")
                .is_none()
        );
    }

    #[test]
    fn parallel_cross_with_equi_engages() {
        // Tier-sensitive reader: pin Laptop so concurrent tier tests
        // cannot move the gate mid-flight.
        let _tier = plomid_executor::parallel_budget::TestTierGuard::hold(
            plomid_executor::parallel_budget::pinned_tier_for_tests(plomid_hw::RuntimeTier::Laptop),
        );
        // The pinned Laptop tier exposes exactly the historical gate.
        assert_eq!(
            plomid_executor::parallel_budget::join_threshold(),
            PARALLEL_JOIN_THRESHOLD
        );
        assert_eq!(
            plomid_executor::parallel_budget::build_min_rows(),
            PARALLEL_BUILD_MIN_ROWS
        );
        let scopes = test_scopes(&["k", "v"], &["k", "w"]);
        let on = equi_on("jl.k", "jr.k");
        let lrows = big_probe(PARALLEL_JOIN_THRESHOLD);
        let rrows = build_500();
        assert!(
            perform_join_parallel(JoinKind::Cross, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                .expect("parallel join must not error")
                .is_some(),
            "Cross with an equi-ON takes the same path as Inner today"
        );
    }

    #[test]
    fn parallel_hashable_inner_is_exact_and_deterministic() {
        let _tier = plomid_executor::parallel_budget::TestTierGuard::hold(
            plomid_executor::parallel_budget::pinned_tier_for_tests(plomid_hw::RuntimeTier::Laptop),
        );
        let scopes = test_scopes(&["k", "v"], &["k", "w"]);
        let on = equi_on("jl.k", "jr.k");
        let n = PARALLEL_JOIN_THRESHOLD + 2_000;
        let lrows = big_probe(n);
        let rrows = build_500();
        let mut expect_count = 0_i64;
        let mut expect_sum = 0_i64;
        for (i, row) in lrows.iter().enumerate() {
            if !row[0].is_null() {
                expect_count += 1;
                expect_sum += i as i64;
            }
        }
        let first =
            perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                .expect("parallel join must not error")
                .expect("hashable above-threshold inner must engage");
        assert_eq!(first.len(), expect_count as usize);
        let sum: i64 = first
            .iter()
            .map(|row| match row[1] {
                Value::Int8(v) => v,
                ref other => panic!("expected Int8 payload, got {other:?}"),
            })
            .sum();
        assert_eq!(sum, expect_sum);
        // Every output row pairs a non-NULL probe key with its equal build key.
        for row in &first {
            assert!(!row[0].is_null());
            assert_eq!(row[0], row[2]);
        }
        let second =
            perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                .expect("parallel join must not error")
                .expect("second run must engage");
        assert_eq!(first, second, "parallel output must be deterministic");
    }

    #[test]
    fn parallel_builds_smaller_left_side() {
        let _tier = plomid_executor::parallel_budget::TestTierGuard::hold(
            plomid_executor::parallel_budget::pinned_tier_for_tests(plomid_hw::RuntimeTier::Laptop),
        );
        // Left (400 rows) is smaller than right (2400 rows), so
        // `choose_build_side` builds LEFT and probes RIGHT. Total 2800 clears
        // the threshold, so the parallel path engages; output must still be
        // left-major `concat(left, right)` with equal keys paired.
        let scopes = test_scopes(&["k", "v"], &["k", "w"]);
        let on = equi_on("jl.k", "jr.k");
        assert_eq!(
            choose_build_side(400, 2400),
            BuildSide::Left,
            "test premise: the left side must be the build side"
        );
        let lrows: Vec<Vec<Value>> = (0..400_i32)
            .map(|i| vec![Value::Int4(i % 100), Value::Int8(i as i64)])
            .collect();
        let rrows: Vec<Vec<Value>> = (0..2400_i32)
            .map(|i| vec![Value::Int4(i % 100), Value::Int8(i as i64)])
            .collect();
        let out = perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
            .expect("parallel join must not error")
            .expect("above-threshold hashable inner must engage");
        // 100 keys x 4 left copies x 24 right copies = 9600 rows.
        assert_eq!(out.len(), 9600);
        for row in &out {
            assert_eq!(row.len(), 4);
            assert_eq!(row[0], row[2], "probe key must equal build key");
            assert!(
                matches!(row[1], Value::Int8(_)),
                "column 1 must be the left payload, got {row:?}"
            );
            assert!(
                matches!(row[3], Value::Int8(_)),
                "column 3 must be the right payload, got {row:?}"
            );
        }
        let again =
            perform_join_parallel(JoinKind::Inner, Some(&on), &scopes, 2, &lrows, &rrows, 4)
                .expect("parallel join must not error")
                .expect("second run must engage");
        assert_eq!(out, again, "parallel output must be deterministic");
    }

    #[test]
    fn parallel_partitions_cover_inputs_and_show_skew() {
        // 80%-hot-key distribution over 4 workers: the hot partition must
        // dominate, which is the no-salting imbalance made visible.
        let workers = 4;
        let n = PARALLEL_JOIN_THRESHOLD;
        let mut lrows = Vec::with_capacity(n);
        for i in 0..n as i64 {
            let key = if i % 11 == 0 {
                Value::Null
            } else if i % 10 < 8 {
                Value::Int4(42)
            } else {
                Value::Int4(i as i32 % 500)
            };
            lrows.push(vec![key, Value::Int8(i)]);
        }
        let rrows = build_500();
        let (tables, parts) =
            partition_hash_join_inputs(&lrows, &rrows, 0, 0, workers, BuildSide::Right)
                .expect("hashable");
        let routed_left: usize = parts.iter().map(Vec::len).sum();
        let nulls = lrows.iter().filter(|row| row[0].is_null()).count();
        assert_eq!(
            routed_left,
            n - nulls,
            "every non-NULL probe row is owned once"
        );
        let routed_right_rows: usize = tables
            .iter()
            .map(|t| t.values().map(Vec::len).sum::<usize>())
            .sum();
        assert_eq!(
            routed_right_rows,
            rrows.len(),
            "every build row is owned once"
        );
        let counts: Vec<usize> = parts.iter().map(Vec::len).collect();
        let max = *counts.iter().max().unwrap();
        let min = *counts.iter().min().unwrap().max(&1);
        let ratio = max as f64 / min as f64;
        eprintln!("skew partition probe counts (P=4, 80% hot key): {counts:?} max/min={ratio:.2}");
        assert!(
            ratio > 1.5,
            "hot key must visibly imbalance partitions without salting"
        );
    }
}
