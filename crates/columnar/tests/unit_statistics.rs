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
// Moved from src/statistics.rs inline #[cfg(test)].
use plomid_columnar::column::ColumnType;
use plomid_columnar::pruning::candidate_chunks_with_stats;
use plomid_columnar::pruning::{PruneOperator, PrunePredicate};
use plomid_columnar::statistics::*;
use plomid_core::ColumnId;
use plomid_storage::Field;

fn column() -> ColumnId {
    ColumnId::new(7)
}

fn tracked_stats() -> ColumnStatistics {
    // NOTE: `with_type` only tracks bounds when `null_count < row_count`,
    // so the template must describe a non-empty column.
    ColumnStatistics::with_type(column(), 1, 0, ColumnType::Integer)
}

#[test]
fn distinct_estimate_is_within_tolerance_on_10k_uniques() {
    let mut stats = tracked_stats();
    for value in 0..10_000_i64 {
        stats.observe(&Field::Integer(value));
    }
    let estimate = stats.distinct_estimate_value().expect("sketch exists");
    let actual = 10_000_f64;
    let error = (estimate as f64 - actual).abs() / actual;
    assert!(
        error <= 0.30,
        "estimate {estimate} is more than 30% off 10000"
    );
}

#[test]
fn distinct_sketch_ignores_nulls_and_counts_repeated_values_once() {
    let mut stats = tracked_stats();
    for _ in 0..5_000 {
        stats.observe(&Field::Null);
        stats.observe(&Field::Integer(42));
    }
    let estimate = stats.distinct_estimate_value().expect("sketch exists");
    assert!(estimate <= 10, "one distinct value estimated as {estimate}");
    assert!(stats.distinct_estimator.is_some());
    // NULLs never reach the sketch: only the integer was hashed.
    assert_eq!(stats.null_count, 0, "observe() does not count rows");
}

#[test]
fn distinct_observation_survives_unavailable_bounds() {
    let mut stats = ColumnStatistics::new(column(), 0, 0);
    stats.observe(&Field::Integer(1));
    stats.observe(&Field::Integer(2));
    assert!(stats.distinct_estimate_value().is_some_and(|d| d >= 1));
}

#[test]
fn histogram_selectivity_is_sane() {
    let sample: Vec<Field> = (1..=100_i64).map(Field::Integer).collect();
    let histogram = Histogram::build_equidepth(&sample, 10).expect("histogram");
    assert_eq!(histogram.bucket_count, 10);
    assert_eq!(histogram.total_count(), 100);

    let point = PrunePredicate::compare(column(), PruneOperator::Equal, Field::Integer(50));
    let point_selectivity = histogram.selectivity(&point);
    assert!(
        point_selectivity > 0.0 && point_selectivity < 0.2,
        "point selectivity {point_selectivity} should be small but nonzero"
    );

    let full =
        PrunePredicate::compare(column(), PruneOperator::GreaterOrEqual, Field::Integer(1)).and(
            PrunePredicate::compare(column(), PruneOperator::LessOrEqual, Field::Integer(100)),
        );
    assert!(
        histogram.selectivity(&full) > 0.9,
        "full-range selectivity should be near 1.0"
    );

    let outside =
        PrunePredicate::compare(column(), PruneOperator::GreaterOrEqual, Field::Integer(200));
    assert_eq!(histogram.selectivity(&outside), 0.0);

    // Column-level helpers scale by the non-NULL fraction and stay sane.
    let mut stats = tracked_stats();
    for value in sample {
        stats.observe(&value);
    }
    stats.row_count = 100;
    stats.null_count = 0;
    stats.set_histogram(histogram);
    let equals = stats.selectivity_equals(&Field::Integer(50));
    assert!(
        equals > 0.0 && equals < 0.2,
        "column equality selectivity {equals} should be small but nonzero"
    );
    assert_eq!(stats.selectivity_equals(&Field::Integer(500)), 0.0);
    let range = stats.selectivity_range(Some(&Field::Integer(1)), Some(&Field::Integer(100)));
    assert!(range > 0.9, "column full-range selectivity {range}");
    assert_eq!(
        stats.selectivity_range(Some(&Field::Integer(200)), None),
        0.0
    );
}

#[test]
fn selectivity_without_evidence_is_conservative() {
    let stats = ColumnStatistics::new(column(), 100, 10);
    assert_eq!(stats.selectivity_equals(&Field::Integer(1)), 1.0);
    assert_eq!(stats.selectivity_range(Some(&Field::Integer(1)), None), 1.0);
}

#[test]
fn selectivity_uses_distinct_when_bounds_are_held() {
    let mut stats = tracked_stats();
    for value in 0..100_i64 {
        stats.observe(&Field::Integer(value));
    }
    stats.row_count = 100;
    stats.null_count = 0;
    let selectivity = stats.selectivity_equals(&Field::Integer(42));
    // Uniform over ~100 distinct values: near 0.01, far from 1.0.
    assert!(
        selectivity > 0.0 && selectivity < 0.2,
        "distinct-based selectivity {selectivity}"
    );
}

#[test]
fn chunk_prune_eliminates_irrelevant_timestamp_chunks() {
    let column_id = column();
    let mut old = ChunkStatistics::new(0, column_id);
    for timestamp in 0..100_i64 {
        old.observe(&Field::Integer(timestamp));
    }
    let mut recent = ChunkStatistics::new(1, column_id);
    for timestamp in 200..300_i64 {
        recent.observe(&Field::Integer(timestamp));
    }
    let mut set = ChunkSetStatistics::new();
    set.add_chunk(old);
    set.add_chunk(recent);

    // WHERE timestamp >= 150: only the recent chunk can match.
    let predicate = PrunePredicate::compare(
        column_id,
        PruneOperator::GreaterOrEqual,
        Field::Integer(150),
    );
    assert_eq!(set.prune(&predicate), vec![false, true]);
    assert_eq!(candidate_chunks_with_stats(&set, &predicate), vec![1]);

    // WHERE timestamp < 150: only the old chunk can match.
    let before = PrunePredicate::compare(column_id, PruneOperator::Less, Field::Integer(150));
    assert_eq!(set.prune(&before), vec![true, false]);

    // A predicate over a foreign column keeps every chunk.
    let foreign = PrunePredicate::eq(ColumnId::new(99), Field::Integer(50));
    assert_eq!(set.prune(&foreign), vec![true, true]);
}

#[test]
fn merge_combines_counts_bounds_and_distinct() {
    let mut left = tracked_stats();
    for value in 0..50_i64 {
        left.observe(&Field::Integer(value));
    }
    left.row_count = 50;
    let mut right = tracked_stats();
    for value in 50..100_i64 {
        right.observe(&Field::Integer(value));
    }
    right.row_count = 50;

    left.merge_from(&right);
    assert_eq!(left.row_count, 100);
    assert_eq!(left.min, Some(Field::Integer(0)));
    assert_eq!(left.max, Some(Field::Integer(99)));
    let estimate = left.distinct_estimate_value().expect("merged sketch");
    let error = (estimate as f64 - 100.0).abs() / 100.0;
    assert!(error <= 0.30, "merged distinct estimate {estimate}");
}

#[test]
fn encode_decode_roundtrip_survives_memory_only_fields() {
    let mut stats = tracked_stats();
    for value in [3_i64, 1, 2] {
        stats.observe(&Field::Integer(value));
    }
    stats.row_count = 3;
    let sample: Vec<Field> = (1..=10_i64).map(Field::Integer).collect();
    stats.set_histogram(Histogram::build_equidepth(&sample, 2).expect("histogram"));

    let (bytes, flags) = stats.encode_with_flags().expect("encode");
    let template = ColumnStatistics::with_type(column(), 3, 0, ColumnType::Integer);
    let decoded = template.decode_with(&bytes, flags).expect("decode");
    assert_eq!(decoded.min, Some(Field::Integer(1)));
    assert_eq!(decoded.max, Some(Field::Integer(3)));
    assert_eq!(decoded.flags(), flags);
    assert_eq!(decoded.row_count, 3);
    // Memory-only fields are not part of the v1 payload.
    assert!(decoded.histogram.is_none());
    assert!(decoded.distinct_estimator.is_none());
}

#[test]
fn histogram_builder_rejects_empty_input() {
    assert!(Histogram::build_equidepth(&[], 4).is_none());
    assert!(Histogram::build_equidepth(&[Field::Null], 4).is_none());
    let sample = vec![Field::Integer(1)];
    assert!(Histogram::build_equidepth(&sample, 0).is_none());
    let single = Histogram::build_equidepth(&sample, 8).expect("one bucket");
    assert_eq!(single.bucket_count, 1);
    assert_eq!(single.total_count(), 1);
}
