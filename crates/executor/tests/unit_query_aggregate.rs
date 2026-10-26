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
// Moved from src/query/aggregate.rs inline #[cfg(test)].
use plomid_executor::query::aggregate::*;
use plomid_sql::Expression;
use plomid_sql::Value;

/// Deterministic xorshift64: randomized coverage without new dev-deps.
struct Rng(u64);

impl Rng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        if x == 0 {
            x = 0x9E37_79B9_7F4A_7C15;
        }
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0);
        self.next_u64() % n
    }

    fn small_int(&mut self) -> i64 {
        self.below(2001) as i64 - 1000
    }

    /// Exactly representable binary fractions: small dyadic sums never
    /// round, so merged and single-pass `f64` results must be bitwise
    /// identical (unlike general floats, which may differ in the last
    /// ulp by documented reduction-order effects).
    fn dyadic(&mut self) -> f64 {
        self.small_int() as f64 / 8.0
    }

    /// General finite floats of moderate magnitude (sums stay finite, so
    /// merged `f64` finalization must be finite, never `NULL`).
    fn mag_float(&mut self) -> f64 {
        let unit = self.next_u64() as f64 / u64::MAX as f64;
        (unit * 2_000_000.0 - 1_000_000.0) + self.below(1000) as f64 / 1000.0
    }

    fn numeric(&mut self) -> Value {
        let int = self.small_int();
        let frac = self.below(100);
        let text = format!("{int}.{frac:02}");
        let parsed = plomid_types::Numeric::parse(&text).expect("valid numeric");
        Value::Numeric(parsed)
    }
}

fn feed_values(fresh: &AggregateAccumulator, values: &[Value]) -> AggregateAccumulator {
    let mut acc = fresh.clone();
    for value in values {
        acc.step_value(Some(value));
    }
    acc
}

/// Contiguous split of `values` into `nparts` partitions (some possibly
/// empty, exercising empty-partial merges); partition order preserves the
/// input row order.
fn partitions_of(values: &[Value], nparts: usize) -> Vec<Vec<Value>> {
    let mut parts: Vec<Vec<Value>> = vec![Vec::new(); nparts.max(1)];
    let (n, m) = (values.len(), parts.len());
    if n == 0 {
        return parts;
    }
    for (index, value) in values.iter().enumerate() {
        parts[index * m / n].push(value.clone());
    }
    parts
}

fn merge_partitioned(
    fresh: &AggregateAccumulator,
    values: &[Value],
    nparts: usize,
) -> AggregateAccumulator {
    let mut acc = fresh.clone();
    for part in partitions_of(values, nparts) {
        acc.merge_into(feed_values(fresh, &part));
    }
    acc
}

/// Merge-vs-single-pass bitwise equality over 1-8 partitions plus the
/// empty input. Only valid where merging is order-free (see `merge_into`
/// docs); float tests use the finite/dyadic variants instead.
fn assert_merge_eq(fresh: &AggregateAccumulator, values: &[Value]) {
    let single = feed_values(fresh, values).finalize();
    for nparts in [1usize, 2, 3, 5, 8] {
        let merged = merge_partitioned(fresh, values, nparts).finalize();
        assert_eq!(merged, single, "partition count {nparts}");
    }
}

fn assert_finite_numeric(value: &Value, context: &str) {
    match value {
        Value::Numeric(numeric) => assert!(
            numeric.clone().to_f64().is_finite(),
            "{context}: expected finite numeric, got {numeric:?}"
        ),
        // §P2: float aggregates now finalize as Float8 (PG typing).
        Value::Float8(v) => assert!(v.is_finite(), "{context}: expected finite float, got {v:?}"),
        other => panic!("{context}: expected Numeric/Float8, got {other:?}"),
    }
}

fn fresh_count_star() -> AggregateAccumulator {
    AggregateAccumulator::new("count", Some(&Expression::Star), false)
}

fn fresh_count() -> AggregateAccumulator {
    AggregateAccumulator::new("count", Some(&Expression::Literal(Value::Null)), false)
}

fn fresh_distinct() -> AggregateAccumulator {
    AggregateAccumulator::new("count", Some(&Expression::Literal(Value::Null)), true)
}

fn fresh_sum() -> AggregateAccumulator {
    AggregateAccumulator::new("sum", Some(&Expression::Literal(Value::Null)), false)
}

fn fresh_avg() -> AggregateAccumulator {
    AggregateAccumulator::new("avg", Some(&Expression::Literal(Value::Null)), false)
}

fn mixed_values(rng: &mut Rng, len: usize) -> Vec<Value> {
    let mut values = Vec::with_capacity(len);
    for i in 0..len {
        values.push(match rng.below(6) {
            0 => Value::Null,
            1 => Value::Int8(rng.small_int()),
            2 => Value::Float8(rng.dyadic()),
            3 => rng.numeric(),
            4 => Value::Text(format!("k{}", rng.below(7))),
            _ => Value::Bool(i % 2 == 0),
        });
    }
    values
}

#[test]
fn count_star_merge_exact() {
    let mut rng = Rng(11);
    let values = mixed_values(&mut rng, 60);
    assert_merge_eq(&fresh_count_star(), &values);
    assert_merge_eq(&fresh_count_star(), &[]);
}

#[test]
fn count_merge_exact() {
    let mut rng = Rng(22);
    let values = mixed_values(&mut rng, 60);
    assert_merge_eq(&fresh_count(), &values);
    assert_merge_eq(&fresh_count(), &[Value::Null, Value::Null]);
    assert_merge_eq(&fresh_count(), &[]);
}

#[test]
fn count_distinct_merge_exact() {
    let mut rng = Rng(33);
    let values = mixed_values(&mut rng, 80);
    assert_merge_eq(&fresh_distinct(), &values);
    // NULLs never enter the set; unify Int/Float/Numeric spellings.
    assert_merge_eq(
        &fresh_distinct(),
        &[
            Value::Null,
            Value::Int8(1),
            Value::Float8(1.0),
            Value::Int8(1),
            Value::Null,
        ],
    );
    assert_merge_eq(&fresh_distinct(), &[]);
}

#[test]
fn sum_int_merge_exact() {
    let mut rng = Rng(44);
    // Extremes included exactly once each; their sum (-1) plus small
    // offsets can never overflow, in any partition or in the fold.
    let mut values = vec![Value::Int8(i64::MAX), Value::Int8(i64::MIN)];
    for _ in 0..200 {
        values.push(if rng.below(5) == 0 {
            Value::Null
        } else {
            Value::Int8(rng.small_int())
        });
    }
    values.push(Value::Int8(0));
    values.push(Value::Int8(1));
    values.push(Value::Int8(-1));
    assert_merge_eq(&fresh_sum(), &values);
    assert_merge_eq(&fresh_sum(), &[]);
    assert_merge_eq(&fresh_sum(), &[Value::Null]);
}

#[test]
fn sum_int_triple_merge_associative() {
    let fresh = fresh_sum();
    let (a, b, c) = (
        vec![Value::Int8(3), Value::Int8(-7), Value::Null],
        vec![Value::Int8(100), Value::Int8(42)],
        vec![Value::Int8(-5), Value::Null, Value::Int8(9)],
    );
    let mut left = feed_values(&fresh, &a);
    left.merge_into(feed_values(&fresh, &b));
    left.merge_into(feed_values(&fresh, &c));
    let mut right_inner = feed_values(&fresh, &b);
    right_inner.merge_into(feed_values(&fresh, &c));
    let mut right = feed_values(&fresh, &a);
    right.merge_into(right_inner);
    let single = feed_values(&fresh, &[a, b, c].concat()).finalize();
    assert_eq!(left.finalize(), single);
    assert_eq!(right.finalize(), single);
}

#[test]
fn sum_float_dyadic_merge_exact() {
    let mut rng = Rng(55);
    let mut values = Vec::with_capacity(200);
    for _ in 0..200 {
        values.push(if rng.below(5) == 0 {
            Value::Null
        } else {
            Value::Float8(rng.dyadic())
        });
    }
    assert_merge_eq(&fresh_sum(), &values);
}

#[test]
fn sum_float_general_stays_finite() {
    let mut rng = Rng(56);
    let mut values = Vec::with_capacity(200);
    for _ in 0..200 {
        values.push(if rng.below(5) == 0 {
            Value::Null
        } else {
            Value::Float8(rng.mag_float())
        });
    }
    for nparts in [1usize, 2, 3, 5, 8] {
        let merged = merge_partitioned(&fresh_sum(), &values, nparts);
        assert_finite_numeric(&merged.finalize(), &format!("sum float {nparts}"));
    }
    // Degenerate single-partition merge adopts the partial bitwise.
    let single = feed_values(&fresh_sum(), &values).finalize();
    assert_eq!(
        merge_partitioned(&fresh_sum(), &values, 1).finalize(),
        single
    );
}

#[test]
fn sum_float_nonfinite_robustness() {
    let cases: Vec<Vec<Value>> = vec![
        vec![Value::Float8(1.5), Value::Float8(f64::INFINITY)],
        vec![Value::Float8(f64::NAN)],
        vec![
            Value::Float8(f64::INFINITY),
            Value::Float8(f64::NEG_INFINITY),
        ],
        vec![
            Value::Float8(5.0),
            Value::Float8(f64::NAN),
            Value::Float8(2.5),
        ],
        vec![Value::Float8(f64::NEG_INFINITY), Value::Float8(1.0)],
    ];
    for values in &cases {
        // Every partial holding the inf/NaN stays non-finite under any
        // association. Float sums finalize as Float8 (§P2), where NaN != NaN
        // by IEEE-754, so compare via bit pattern / is_nan instead of `==`.
        let single = feed_values(&fresh_sum(), values).finalize();
        for nparts in [1usize, 2, 3] {
            let merged = merge_partitioned(&fresh_sum(), values, nparts).finalize();
            match (&merged, &single) {
                (Value::Float8(a), Value::Float8(b)) => {
                    if a.is_nan() || b.is_nan() {
                        assert!(a.is_nan() && b.is_nan(), "NaN mismatch: {a:?} vs {b:?}");
                    } else {
                        assert_eq!(merged, single);
                    }
                }
                _ => assert_eq!(merged, single),
            }
        }
    }
}

#[test]
fn sum_numeric_merge_exact() {
    let mut rng = Rng(66);
    let mut values = Vec::with_capacity(120);
    for _ in 0..120 {
        values.push(if rng.below(5) == 0 {
            Value::Null
        } else {
            rng.numeric()
        });
    }
    values.push(Value::Numeric(
        plomid_types::Numeric::parse("0").expect("valid numeric"),
    ));
    values.push(Value::Numeric(
        plomid_types::Numeric::parse("-99999999999999999999.99").expect("valid"),
    ));
    assert_merge_eq(&fresh_sum(), &values);
    assert_merge_eq(&fresh_sum(), &[]);
}

#[test]
fn avg_int_merge_exact() {
    let mut rng = Rng(77);
    let mut values = vec![Value::Int8(i64::MAX), Value::Int8(i64::MIN)];
    for _ in 0..200 {
        values.push(if rng.below(4) == 0 {
            Value::Null
        } else {
            Value::Int8(rng.small_int())
        });
    }
    assert_merge_eq(&fresh_avg(), &values);
    assert_merge_eq(&fresh_avg(), &[]);
    assert_merge_eq(&fresh_avg(), &[Value::Null, Value::Null]);
}

#[test]
fn avg_float_dyadic_merge_exact() {
    let mut rng = Rng(88);
    let mut values = Vec::with_capacity(160);
    for _ in 0..160 {
        values.push(if rng.below(4) == 0 {
            Value::Null
        } else {
            Value::Float8(rng.dyadic())
        });
    }
    assert_merge_eq(&fresh_avg(), &values);
}

#[test]
fn avg_float_general_stays_finite() {
    let mut rng = Rng(89);
    let mut values = Vec::with_capacity(160);
    for _ in 0..160 {
        values.push(if rng.below(4) == 0 {
            Value::Null
        } else {
            Value::Float8(rng.mag_float())
        });
    }
    for nparts in [1usize, 2, 3, 5, 8] {
        let merged = merge_partitioned(&fresh_avg(), &values, nparts);
        assert_finite_numeric(&merged.finalize(), &format!("avg float {nparts}"));
    }
    let single = feed_values(&fresh_avg(), &values).finalize();
    assert_eq!(
        merge_partitioned(&fresh_avg(), &values, 1).finalize(),
        single
    );
}

#[test]
fn avg_numeric_merge_exact() {
    let mut rng = Rng(99);
    let mut values = Vec::with_capacity(120);
    for _ in 0..120 {
        values.push(if rng.below(4) == 0 {
            Value::Null
        } else {
            rng.numeric()
        });
    }
    assert_merge_eq(&fresh_avg(), &values);
    assert_merge_eq(&fresh_avg(), &[]);
}

#[test]
fn min_max_merge_exact() {
    let mut rng = Rng(111);
    let mut values = vec![
        Value::Int8(i64::MIN),
        Value::Int8(i64::MAX),
        Value::Float8(f64::NAN),
        Value::Float8(f64::INFINITY),
        Value::Float8(f64::NEG_INFINITY),
        Value::Null,
        Value::Text("mango".to_string()),
        Value::Text("apple".to_string()),
    ];
    for _ in 0..80 {
        values.push(match rng.below(5) {
            0 => Value::Null,
            1 => Value::Int8(rng.small_int()),
            2 => Value::Float8(rng.dyadic()),
            3 => Value::Text(format!("s{}", rng.below(9))),
            _ => rng.numeric(),
        });
    }
    let fresh_min =
        AggregateAccumulator::new("min", Some(&Expression::Literal(Value::Null)), false);
    let fresh_max =
        AggregateAccumulator::new("max", Some(&Expression::Literal(Value::Null)), false);
    assert_merge_eq(&fresh_min, &values);
    assert_merge_eq(&fresh_max, &values);
    assert_merge_eq(&fresh_min, &[Value::Null]);
    assert_merge_eq(&fresh_max, &[]);
}

#[test]
fn json_values_merge_exact() {
    let mut rng = Rng(122);
    let fresh = AggregateAccumulator::new("json_agg", None, false);
    let values = mixed_values(&mut rng, 40);
    for nparts in [1usize, 2, 3, 5, 8] {
        let single = feed_values(&fresh, &values);
        let merged = merge_partitioned(&fresh, &values, nparts);
        assert_eq!(
            merged.finalize_json("json_agg"),
            single.finalize_json("json_agg"),
            "json_agg {nparts}"
        );
        assert_eq!(
            merged.finalize_json("jsonb_agg"),
            single.finalize_json("jsonb_agg"),
            "jsonb_agg {nparts}"
        );
    }
    let empty = merge_partitioned(&fresh, &[], 3);
    assert_eq!(empty.finalize_json("json_agg"), Value::Null);
}

#[test]
fn json_pairs_merge_exact() {
    let mut rng = Rng(133);
    let fresh = AggregateAccumulator::new("json_object_agg", None, false);
    let mut pairs = Vec::with_capacity(30);
    for i in 0..30 {
        let key = match rng.below(6) {
            0 => Value::Null,
            1 => Value::Text("dup".to_string()),
            _ => Value::Text(format!("k{i}")),
        };
        let value = if rng.below(5) == 0 {
            Value::Null
        } else {
            Value::Int8(rng.small_int())
        };
        pairs.push((key, value));
    }
    let feed = |rows: &[(Value, Value)]| {
        let mut acc = fresh.clone();
        for (key, value) in rows {
            acc.step_pair(key.clone(), value.clone());
        }
        acc
    };
    let single = feed(&pairs);
    // Contiguous split mirroring `partitions_of` (pairs carry two values,
    // so partition here directly on the pair vec).
    for nparts in [1usize, 2, 3, 5, 8] {
        let mut acc = fresh.clone();
        let m = nparts.max(1);
        let mut partials: Vec<Vec<(Value, Value)>> = vec![Vec::new(); m];
        for (index, pair) in pairs.iter().enumerate() {
            partials[index * m / pairs.len()].push(pair.clone());
        }
        for part in &partials {
            acc.merge_into(feed(part));
        }
        assert_eq!(
            acc.finalize_json("json_object_agg"),
            single.finalize_json("json_object_agg"),
            "json_object_agg {nparts}"
        );
    }
    assert_eq!(feed(&[]).finalize_json("json_object_agg"), Value::Null);
}

#[test]
fn empty_merge_identities() {
    let num =
        |text: &str| Value::Numeric(plomid_types::Numeric::parse(text).expect("valid numeric"));
    let states: Vec<AggregateAccumulator> = vec![
        feed_values(&fresh_count_star(), &[Value::Int8(1), Value::Null]),
        feed_values(&fresh_count(), &[Value::Int8(1), Value::Null]),
        feed_values(&fresh_distinct(), &[Value::Int8(1), Value::Int8(1)]),
        feed_values(&fresh_sum(), &[Value::Int8(1), Value::Int8(2)]),
        feed_values(&fresh_sum(), &[Value::Float8(0.5)]),
        feed_values(&fresh_sum(), &[num("1.25")]),
        feed_values(&fresh_avg(), &[Value::Int8(1), Value::Int8(2)]),
        feed_values(&fresh_avg(), &[Value::Float8(0.5)]),
        feed_values(&fresh_avg(), &[num("1.25")]),
        feed_values(
            &AggregateAccumulator::new("min", Some(&Expression::Literal(Value::Null)), false),
            &[Value::Int8(2), Value::Int8(1)],
        ),
        feed_values(
            &AggregateAccumulator::new("max", Some(&Expression::Literal(Value::Null)), false),
            &[Value::Int8(2), Value::Int8(1)],
        ),
        feed_values(
            &AggregateAccumulator::new("json_agg", None, false),
            &[Value::Int8(1), Value::Null],
        ),
        AggregateAccumulator::SumF(0.0, false),
        AggregateAccumulator::SumNumeric(None, false),
        AggregateAccumulator::AvgF { sum: 0.0, count: 0 },
        AggregateAccumulator::AvgNumeric {
            sum: plomid_types::Numeric::from_i64(0),
            count: 0,
        },
    ];
    // Matching fresh state per fed state: SUM-family states pair with a
    // fresh `SumN`, AVG-family with `AvgN`, the rest with their own kind.
    let fresh_for = |fed: &AggregateAccumulator| match fed {
        AggregateAccumulator::SumN(..)
        | AggregateAccumulator::SumF(..)
        | AggregateAccumulator::SumNumeric(..) => fresh_sum(),
        AggregateAccumulator::AvgN { .. }
        | AggregateAccumulator::AvgF { .. }
        | AggregateAccumulator::AvgNumeric { .. } => fresh_avg(),
        AggregateAccumulator::CountStar(_) => fresh_count_star(),
        AggregateAccumulator::Count(_) => fresh_count(),
        AggregateAccumulator::CountDistinct(_) => fresh_distinct(),
        AggregateAccumulator::Min(_) => {
            AggregateAccumulator::new("min", Some(&Expression::Literal(Value::Null)), false)
        }
        AggregateAccumulator::Max(_) => {
            AggregateAccumulator::new("max", Some(&Expression::Literal(Value::Null)), false)
        }
        AggregateAccumulator::JsonValues(_) => AggregateAccumulator::new("json_agg", None, false),
        AggregateAccumulator::JsonPairs(_) => {
            AggregateAccumulator::new("json_object_agg", None, false)
        }
    };
    for fed in &states {
        let fresh = fresh_for(fed);
        let mut empty_empty = fresh.clone();
        empty_empty.merge_into(fresh.clone());
        assert_eq!(empty_empty.finalize(), fresh.finalize());
        let mut full_empty = fed.clone();
        full_empty.merge_into(fresh.clone());
        assert_eq!(full_empty.finalize(), fed.finalize());
        let mut empty_full = fresh.clone();
        empty_full.merge_into(fed.clone());
        assert_eq!(empty_full.finalize(), fed.finalize());
    }
    // JSON pairs use `finalize_json`, not `finalize`.
    let pair_fresh = AggregateAccumulator::new("json_object_agg", None, false);
    let mut pair_fed = pair_fresh.clone();
    pair_fed.step_pair(Value::Text("a".to_string()), Value::Int8(1));
    for (left, right) in [
        (pair_fresh.clone(), pair_fresh.clone()),
        (pair_fed.clone(), pair_fresh.clone()),
        (pair_fresh.clone(), pair_fed.clone()),
    ] {
        let mut acc = left;
        let expected = if acc.finalize_json("json_object_agg") == Value::Null
            && right.finalize_json("json_object_agg") == Value::Null
        {
            Value::Null
        } else {
            pair_fed.finalize_json("json_object_agg")
        };
        acc.merge_into(right);
        assert_eq!(acc.finalize_json("json_object_agg"), expected);
    }
}

#[test]
fn mixed_sum_promotion_matches_single_pass() {
    let fresh = fresh_sum();
    // Integer head, NUMERIC middle, integer tail: §P2 fix consumes the tail
    // (previously ignored after promotion: expected 3.5, now 6.5).
    let values = vec![
        Value::Int8(1),
        Value::Numeric(plomid_types::Numeric::parse("2.5").expect("valid")),
        Value::Int8(3),
        Value::Null,
    ];
    let single = feed_values(&fresh, &values).finalize();
    assert_eq!(
        single,
        Value::Numeric(plomid_types::Numeric::parse("6.5").expect("valid"))
    );
    for nparts in [1usize, 2, 3, 4] {
        assert_eq!(
            merge_partitioned(&fresh, &values, nparts).finalize(),
            single,
            "numeric promotion {nparts}"
        );
    }
    // NUMERIC first: trailing integers now contribute (previously ignored).
    let values = vec![
        Value::Numeric(plomid_types::Numeric::parse("2.5").expect("valid")),
        Value::Int8(1),
        Value::Int8(2),
    ];
    let single = feed_values(&fresh, &values).finalize();
    assert_eq!(
        single,
        Value::Numeric(plomid_types::Numeric::parse("5.5").expect("valid"))
    );
    for nparts in [1usize, 2, 3] {
        assert_eq!(
            merge_partitioned(&fresh, &values, nparts).finalize(),
            single,
            "numeric first {nparts}"
        );
    }
    // Integer head, dyadic float middle and tail: exactly representable,
    // so promotion and tail addition are bitwise exact in every order.
    let values = vec![
        Value::Int8(1),
        Value::Int8(2),
        Value::Float8(0.5),
        Value::Int8(4),
        Value::Null,
    ];
    let single = feed_values(&fresh, &values).finalize();
    for nparts in [1usize, 2, 3, 4, 5] {
        assert_eq!(
            merge_partitioned(&fresh, &values, nparts).finalize(),
            single,
            "float promotion {nparts}"
        );
    }
}

#[test]
fn mixed_avg_promotion_matches_single_pass() {
    let fresh = fresh_avg();
    let values = vec![
        Value::Int8(1),
        Value::Int8(3),
        Value::Numeric(plomid_types::Numeric::parse("1.5").expect("valid")),
        Value::Int8(5),
        Value::Null,
    ];
    let single = feed_values(&fresh, &values).finalize();
    for nparts in [1usize, 2, 3, 4, 5] {
        assert_eq!(
            merge_partitioned(&fresh, &values, nparts).finalize(),
            single,
            "avg promotion {nparts}"
        );
    }
}
