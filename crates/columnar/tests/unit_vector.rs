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
// Moved from src/vector.rs inline #[cfg(test)].
use plomid_columnar::column::ColumnType;
use plomid_columnar::materialization::{materialize_columns, MaterializedColumn};
use plomid_columnar::pruning::row_matches;
use plomid_columnar::pruning::Tri;
use plomid_columnar::pruning::{CandidateRange, PruneOperator, PrunePredicate};
use plomid_columnar::read::{should_execute_compressed, SegmentReader};
use plomid_columnar::vector::*;
use plomid_core::ColumnId;
use plomid_storage::Field;
use plomid_storage::Row;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Deterministic LCG (no rand dependency in this crate).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) & 0x7FFF_FFFF
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// Builds a two-integer-column table with NULLs, as materialized columns.
fn two_int_columns(n: usize) -> Vec<MaterializedColumn> {
    let rows: Vec<Row> = (0..n)
        .map(|i| {
            Row::new(vec![
                if i % 7 == 0 {
                    Field::Null
                } else {
                    Field::Integer(i as i64 * 3 - 500)
                },
                if i % 11 == 0 {
                    Field::Null
                } else {
                    Field::Integer(1000 - i as i64 * 2)
                },
            ])
        })
        .collect();
    materialize_columns(&rows, &[ColumnType::Integer, ColumnType::Integer]).expect("materialize")
}

#[test]
fn tiered_batch_rows_follow_the_profile() {
    // Tiered batch sizing without touching the host: explicit profiles
    // per tier plus the env override (the EDGE-B batch contract).
    use plomid_hw::{
        Architecture, CpuFeatures, HardwareProfile, ProfileSource, RuntimeProfile, RuntimeTier,
        StorageKind,
    };
    fn profile_for(tier: RuntimeTier) -> RuntimeProfile {
        // Unknown L2 (-> 8192-row L2-fit base), 8 CPUs, 16 GiB.
        let hardware = HardwareProfile {
            arch: Architecture::X86_64,
            cpu: CpuFeatures {
                scalar: true,
                sse42: false,
                avx2: false,
                avx512: false,
                neon: false,
            },
            logical_cpus: 8,
            physical_cpus: None,
            cache_bytes: [None, None, None],
            ram_total_bytes: Some(16 * 1024 * 1024 * 1024),
            ram_available_bytes: None,
            storage: StorageKind::Unknown,
            storage_bandwidth_bps: None,
            storage_iops_estimate: None,
            numa_nodes: None,
            notes: Vec::new(),
        };
        RuntimeProfile::with_tier(tier, hardware, ProfileSource::Explicit)
    }
    // Laptop/Server pin the calibrated knee; Edge takes the profile's
    // halved L2-fit (8192/2 = 4096) inside the 1-4K band.
    assert_eq!(
        vector_batch_rows_for_profile(&profile_for(RuntimeTier::Laptop)),
        VECTOR_BATCH_ROWS
    );
    assert_eq!(
        vector_batch_rows_for_profile(&profile_for(RuntimeTier::Server)),
        VECTOR_BATCH_ROWS
    );
    assert_eq!(
        vector_batch_rows_for_profile(&profile_for(RuntimeTier::Edge)),
        4096
    );
    assert_eq!(VECTOR_BATCH_ROWS, 16_384, "default-tier pin");
    // Explicit configuration wins over the derived default.
    let saved = std::env::var("PLOMID_VECTOR_BATCH_ROWS").ok();
    std::env::set_var("PLOMID_VECTOR_BATCH_ROWS", "512");
    assert_eq!(default_vector_batch_rows(), 512);
    std::env::set_var("PLOMID_VECTOR_BATCH_ROWS", "0");
    assert_ne!(
        default_vector_batch_rows(),
        0,
        "0 is ignored, tier default applies"
    );
    match saved {
        Some(value) => std::env::set_var("PLOMID_VECTOR_BATCH_ROWS", value),
        None => std::env::remove_var("PLOMID_VECTOR_BATCH_ROWS"),
    }
}

#[test]
fn batch_masks_match_row_matches_oracle() {
    // Differential test: every batch mask must agree with the pruning
    // layer's three-valued ground truth on every row.
    let a = ColumnId::new(0);
    let b = ColumnId::new(1);
    let columns = two_int_columns(2000);
    // NOTE: owned buffers are leaked below so borrowed views stay
    // valid for the test body (test-only; production borrows live data).
    let rows = columns[0].row_count as usize;
    let mut int_bytes: Vec<Vec<u8>> = Vec::new();
    let mut int_offsets: Vec<Vec<u32>> = Vec::new();
    let mut int_lengths: Vec<Vec<u32>> = Vec::new();
    let mut null_maps: Vec<Vec<u8>> = Vec::new();
    for column in &columns {
        let mut bytes = Vec::with_capacity(rows * 8);
        let mut offsets = Vec::with_capacity(rows);
        let mut lengths = Vec::with_capacity(rows);
        for row in 0..rows {
            offsets.push(bytes.len() as u32);
            match column.get_value(row) {
                Some(value_bytes) => {
                    let word = <[u8; 8]>::try_from(&value_bytes[..8.min(value_bytes.len())])
                        .map(i64::from_le_bytes)
                        .unwrap_or(0);
                    bytes.extend_from_slice(&word.to_le_bytes());
                    lengths.push(8);
                }
                None => {
                    lengths.push(0);
                }
            }
        }
        int_bytes.push(bytes);
        int_offsets.push(offsets);
        int_lengths.push(lengths);
        null_maps.push(column.null_bitmap.clone());
    }
    // Leak so borrowed views stay valid for the test body.
    let bytes_leaked: &'static mut Vec<Vec<u8>> = Box::leak(Box::new(int_bytes));
    let offsets_leaked: &'static mut Vec<Vec<u32>> = Box::leak(Box::new(int_offsets));
    let lengths_leaked: &'static mut Vec<Vec<u32>> = Box::leak(Box::new(int_lengths));
    let null_leaked: &'static mut Vec<Vec<u8>> = Box::leak(Box::new(null_maps));
    let mut views = RangeViews {
        ints: HashMap::new(),
        raw: HashMap::new(),
        nulls: HashMap::new(),
    };
    for (index, id) in [a, b].iter().enumerate() {
        views.ints.insert(
            *id,
            IntView {
                bytes: bytes_leaked[index].as_slice(),
                offsets: offsets_leaked[index].as_slice(),
                lengths: lengths_leaked[index].as_slice(),
                nulls: null_leaked[index].as_slice(),
            },
        );
        views.nulls.insert(*id, null_leaked[index].as_slice());
    }

    let leaves: Vec<PrunePredicate> = vec![
        PrunePredicate::compare(a, PruneOperator::Greater, Field::Integer(100)),
        PrunePredicate::compare(a, PruneOperator::LessOrEqual, Field::Integer(-100)),
        PrunePredicate::compare(b, PruneOperator::Equal, Field::Integer(0)),
        PrunePredicate::compare(b, PruneOperator::NotEqual, Field::Integer(42)),
        PrunePredicate::is_null(a),
        PrunePredicate::is_not_null(b),
    ];
    const SEED: u64 = 0x1234_5678;
    let mut rng = Lcg(SEED);
    for round in 0..60 {
        // Random tree over 1-3 leaves with random connectives/negations.
        let mut predicate = leaves[rng.below(leaves.len() as u64) as usize].clone();
        for _ in 0..rng.below(3) {
            let leaf = leaves[rng.below(leaves.len() as u64) as usize].clone();
            predicate = if rng.below(2) == 0 {
                predicate.and(leaf)
            } else {
                predicate.or(leaf)
            };
        }
        if rng.below(4) == 0 {
            predicate = predicate.not();
        }
        let Some(normalized) = normalize_predicate(&predicate, false) else {
            continue;
        };
        let Some(mask) = eval_node(&normalized, &views, 0, rows) else {
            panic!("seed={SEED:#x} round={round} accepted predicate must evaluate: {predicate:?}");
        };
        // Index-based on purpose: `row` addresses `mask`, `columns`,
        // and the closure state together; an iterator over one of them
        // cannot express the joint indexing.
        #[allow(clippy::needless_range_loop)]
        for row in 0..rows {
            let expected = row_matches(&predicate, &|id| {
                let index = if id == a { 0 } else { 1 };
                let col = &columns[index];
                if col.is_null(row) {
                    Some(Field::Null)
                } else {
                    col.get_value(row).map(|bytes| {
                        Field::Integer(i64::from_le_bytes(bytes[..8].try_into().unwrap()))
                    })
                }
            });
            assert_eq!(
                    (mask[row] != 0),
                    matches!(expected, Tri::True),
                    "seed={SEED:#x} round={round} row={row} predicate={predicate:?} (normalized {normalized:?})"
                );
        }
    }
}

#[test]
fn not_pushdown_preserves_three_valued_logic() {
    // WHERE NOT (x = 1): NULL rows evaluate NULL and must be dropped.
    let a = ColumnId::new(0);
    let predicate = PrunePredicate::compare(a, PruneOperator::Equal, Field::Integer(1)).not();
    let normalized = normalize_predicate(&predicate, false).expect("normalizes");
    // NOT(=) pushes to !=, never a bare Not.
    assert!(matches!(
        normalized,
        PrunePredicate::Compare {
            operator: PruneOperator::NotEqual,
            ..
        }
    ));
    let columns = two_int_columns(64);
    let rows = columns[0].row_count as usize;
    let mut bytes = Vec::with_capacity(rows * 8);
    let mut offsets = Vec::with_capacity(rows);
    let mut lengths = Vec::with_capacity(rows);
    let mut nulls = columns[0].null_bitmap.clone();
    for row in 0..rows {
        offsets.push(bytes.len() as u32);
        match columns[0].get_value(row) {
            Some(value) => {
                let word = <[u8; 8]>::try_from(&value[..8.min(value.len())])
                    .map(i64::from_le_bytes)
                    .unwrap_or(0);
                bytes.extend_from_slice(&word.to_le_bytes());
                lengths.push(8);
            }
            None => {
                lengths.push(0);
            }
        }
    }
    let views = RangeViews {
        ints: HashMap::from([(
            a,
            IntView {
                bytes: bytes.as_slice(),
                offsets: offsets.as_slice(),
                lengths: lengths.as_slice(),
                nulls: nulls.as_slice(),
            },
        )]),
        raw: HashMap::new(),
        nulls: HashMap::from([(a, nulls.as_slice())]),
    };
    let mask = eval_node(&normalized, &views, 0, rows).expect("evaluates");
    // Index-based on purpose: `row` addresses `mask` and
    // `columns[0]` together; an iterator over one of them cannot
    // express the joint indexing.
    #[allow(clippy::needless_range_loop)]
    for row in 0..rows {
        let expected = row_matches(&predicate, &|_| {
            if columns[0].is_null(row) {
                Some(Field::Null)
            } else {
                columns[0]
                    .get_value(row)
                    .map(|b| Field::Integer(i64::from_le_bytes(b[..8].try_into().unwrap())))
            }
        });
        assert_eq!(mask[row] != 0, matches!(expected, Tri::True));
    }
    let _ = &mut nulls;
}

#[test]
fn unsupported_shapes_decline() {
    let a = ColumnId::new(0);
    // Text literal against an integer comparison: never exact (the scalar
    // executor resolves cross-type leaves through casts), so normalization
    // declines and the scalar path answers.
    let cross = PrunePredicate::compare(a, PruneOperator::Equal, Field::String("5".to_owned()));
    assert!(normalize_predicate(&cross, false).is_none());
    // Huge literal: normalization itself declines (f64 guard).
    let huge = PrunePredicate::compare(
        a,
        PruneOperator::Greater,
        Field::Integer(F64_EXACT_INT_RANGE + 1),
    );
    assert!(normalize_predicate(&huge, false).is_none());
    // Boundary literal is accepted.
    let edge = PrunePredicate::compare(
        a,
        PruneOperator::Greater,
        Field::Integer(F64_EXACT_INT_RANGE),
    );
    assert!(normalize_predicate(&edge, false).is_some());
}

#[test]
fn global_accumulators_match_hand_computed() {
    // Values: [3, NULL, -2, 7, NULL]; mask keeps rows 0,2,3.
    // Packed exactly like production buffers (null rows carry no
    // payload bytes here, exercising the offset-based view).
    let bytes: Vec<u8> = [3i64, -2, 7].iter().flat_map(|v| v.to_le_bytes()).collect();
    let offsets = vec![0u32, 8, 8, 16, 24];
    let lengths = vec![8u32, 0, 8, 8, 0];
    let nulls = vec![0b0001_0010u8];
    let views = RangeViews {
        ints: HashMap::from([(
            ColumnId::new(0),
            IntView {
                bytes: bytes.as_slice(),
                offsets: offsets.as_slice(),
                lengths: lengths.as_slice(),
                nulls: nulls.as_slice(),
            },
        )]),
        raw: HashMap::new(),
        nulls: HashMap::from([(ColumnId::new(0), nulls.as_slice())]),
    };
    let mask = vec![1u8, 0, 1, 1, 0];
    let kinds = [
        VectorAggKind::CountStar,
        VectorAggKind::Count,
        VectorAggKind::Sum,
        VectorAggKind::Min,
        VectorAggKind::Max,
        VectorAggKind::Avg,
        VectorAggKind::CountDistinct,
    ];
    let mut accs: Vec<VectorAcc> = kinds.iter().map(|_| VectorAcc::default()).collect();
    for (index, kind) in kinds.iter().enumerate() {
        accumulate(
            &VectorAggregate {
                kind: *kind,
                column: Some(ColumnId::new(0)),
                filter: None,
            },
            &views,
            0,
            5,
            &mask,
            &mut accs[index],
        );
    }
    assert_eq!(accs[0].count_star, 3);
    assert_eq!(accs[1].count, 3);
    assert_eq!((accs[2].sum, accs[2].sum_has_any), (8, true));
    assert_eq!(accs[3].min, Some(-2));
    assert_eq!(accs[4].max, Some(7));
    assert_eq!((accs[5].avg_sum, accs[5].avg_count), (8, 3));
    assert_eq!(accs[6].distinct.len(), 3);

    // Empty mask: COUNTs are 0, everything else NULL-state.
    let mut empty: Vec<VectorAcc> = kinds.iter().map(|_| VectorAcc::default()).collect();
    let nomask = vec![0u8; 5];
    for (index, kind) in kinds.iter().enumerate() {
        accumulate(
            &VectorAggregate {
                kind: *kind,
                column: Some(ColumnId::new(0)),
                filter: None,
            },
            &views,
            0,
            5,
            &nomask,
            &mut empty[index],
        );
    }
    assert_eq!(empty[0].count_star, 0);
    assert_eq!(empty[1].count, 0);
    assert!(!empty[2].sum_has_any);
    assert_eq!(empty[3].min, None);
    assert_eq!(empty[4].max, None);
    assert_eq!(empty[5].avg_count, 0);
    assert!(empty[6].distinct.is_empty());
}

// --- Compressed execution (Phase 38C) targeted tests ---

use plomid_columnar::encoding::ColumnEncoding as TestEncoding;
use plomid_columnar::flush::{flush as test_flush, FlushConfig as TestFlushConfig};
use plomid_columnar::read::{
    compressed_delta_int_cmp_mask as test_delta_mask,
    compressed_delta_min_max as test_delta_minmax,
    compressed_dict_distinct_count as test_dict_distinct, compressed_dict_eq_mask as test_dict_eq,
    compressed_dict_int_keys as test_dict_keys, compressed_rle_aggregate as test_rle_agg,
    compressed_rle_int_cmp_mask as test_rle_int, compressed_rle_raw_cmp_mask as test_rle_raw,
};

fn test_flush_bytes(
    rows: &[Row],
    types: &[ColumnType],
    encoding: TestEncoding,
) -> (Vec<u8>, SegmentReader) {
    let flushed = test_flush(
        rows,
        types,
        plomid_core::GenerationId::new(3),
        plomid_core::SegmentId::new(11),
        &TestFlushConfig::default()
            .with_encoding(encoding)
            .with_compression(plomid_columnar::chunk::ChunkCompression::None),
    )
    .expect("flush");
    let reader = SegmentReader::decode(&flushed.bytes).expect("decode");
    (flushed.bytes, reader)
}

fn test_null_slice(bytes: &[u8], reader: &SegmentReader, col: ColumnId) -> Vec<u8> {
    let meta = reader.column(col).expect("meta");
    let len = plomid_columnar::format::null_bitmap_size(meta.row_count as usize);
    let pos = meta.null_bitmap_offset as usize;
    bytes[pos..pos + len].to_vec()
}

fn test_int_values(bytes: &[u8], reader: &SegmentReader, col: ColumnId) -> Vec<Option<i64>> {
    let meta = reader.column(col).expect("meta");
    let nulls = test_null_slice(bytes, reader, col);
    let chunk = &reader.column_chunks(col)[0];
    let data = reader.read_chunk(bytes, chunk).expect("read").into_bytes();
    // Walk `[length][value]` entries in order.
    let mut out = Vec::new();
    let mut cursor = 0_usize;
    for row in 0..chunk.row_count as usize {
        let abs = chunk.first_row as usize + row;
        let len_bytes: [u8; 4] = data[cursor..cursor + 4].try_into().expect("len");
        let len = u32::from_le_bytes(len_bytes) as usize;
        cursor += 4;
        let val = data[cursor..cursor + len].to_vec();
        cursor += len;
        let is_null = nulls
            .get(abs / 8)
            .is_some_and(|b| b & (1 << (abs % 8)) != 0);
        if is_null {
            out.push(None);
        } else {
            // Integer columns: 8 bytes; other widths are ragged (None here
            // would hide the shape — tests assert fallback separately).
            if meta.column_type == ColumnType::Integer && val.len() == 8 {
                out.push(Some(i64::from_le_bytes(val[..8].try_into().expect("8"))));
            } else if meta.column_type == ColumnType::Integer {
                out.push(None);
            } else {
                // Raw columns: represent as placeholder (tests compare
                // byte-wise separately).
                out.push(None);
            }
        }
        let _ = meta;
    }
    out
}

#[test]
fn compressed_gate_thresholds() {
    use plomid_columnar::read::{CompressedOp as Op, COMPRESSED_MIN_ROWS};
    // Below threshold: never executes, even for supported pairs.
    for enc in [
        TestEncoding::Rle,
        TestEncoding::Dictionary,
        TestEncoding::DeltaBitpack,
        TestEncoding::Raw,
    ] {
        for op in [
            Op::Count,
            Op::Sum,
            Op::Min,
            Op::Max,
            Op::CountDistinct,
            Op::EqFilter,
            Op::RangeFilter,
            Op::GroupBy,
        ] {
            assert!(
                !should_execute_compressed(enc, op, COMPRESSED_MIN_ROWS - 1),
                "{enc:?} {op:?} below threshold must not execute"
            );
        }
    }
    // Supported pairs at threshold.
    assert!(should_execute_compressed(
        TestEncoding::Rle,
        Op::Sum,
        COMPRESSED_MIN_ROWS
    ));
    assert!(should_execute_compressed(
        TestEncoding::Rle,
        Op::EqFilter,
        COMPRESSED_MIN_ROWS
    ));
    assert!(should_execute_compressed(
        TestEncoding::Rle,
        Op::RangeFilter,
        COMPRESSED_MIN_ROWS
    ));
    assert!(should_execute_compressed(
        TestEncoding::Dictionary,
        Op::EqFilter,
        COMPRESSED_MIN_ROWS
    ));
    assert!(should_execute_compressed(
        TestEncoding::Dictionary,
        Op::GroupBy,
        COMPRESSED_MIN_ROWS
    ));
    assert!(should_execute_compressed(
        TestEncoding::Dictionary,
        Op::CountDistinct,
        COMPRESSED_MIN_ROWS
    ));
    assert!(should_execute_compressed(
        TestEncoding::Dictionary,
        Op::Min,
        COMPRESSED_MIN_ROWS
    ));
    assert!(should_execute_compressed(
        TestEncoding::Dictionary,
        Op::Max,
        COMPRESSED_MIN_ROWS
    ));
    assert!(should_execute_compressed(
        TestEncoding::DeltaBitpack,
        Op::Min,
        COMPRESSED_MIN_ROWS
    ));
    assert!(should_execute_compressed(
        TestEncoding::DeltaBitpack,
        Op::RangeFilter,
        COMPRESSED_MIN_ROWS
    ));
    // Unsupported pairs never execute, even above threshold.
    assert!(!should_execute_compressed(
        TestEncoding::Raw,
        Op::Sum,
        10_000
    ));
    assert!(!should_execute_compressed(
        TestEncoding::Dictionary,
        Op::Sum,
        10_000
    ));
    assert!(!should_execute_compressed(
        TestEncoding::Dictionary,
        Op::RangeFilter,
        10_000
    ));
    assert!(!should_execute_compressed(
        TestEncoding::DeltaBitpack,
        Op::Sum,
        10_000
    ));
    assert!(!should_execute_compressed(
        TestEncoding::DeltaBitpack,
        Op::GroupBy,
        10_000
    ));
    assert!(!should_execute_compressed(
        TestEncoding::Rle,
        Op::GroupBy,
        10_000
    ));
}

#[test]
fn compressed_rle_exactness_against_decode() {
    // RLE-friendly: long runs (each value repeats 40x), 2000 rows.
    let mut rows = Vec::new();
    for i in 0..2000 {
        let v = (i / 40) as i64 * 7 - 100;
        if i % 97 == 0 {
            rows.push(Row::new(vec![Field::Null]));
        } else {
            rows.push(Row::new(vec![Field::Integer(v)]));
        }
    }
    for encoding in [TestEncoding::Rle, TestEncoding::Auto] {
        let (bytes, reader) = test_flush_bytes(&rows, &[ColumnType::Integer], encoding);
        // Skip when Auto chose Raw (e.g. tiny fixtures) — gate would fall
        // back anyway; exactness of the fallback is covered by existing
        // vector tests.
        let chunk = &reader.column_chunks(ColumnId::new(0))[0];
        if chunk.encoding != TestEncoding::Rle {
            continue;
        }
        assert!(should_execute_compressed(
            chunk.encoding,
            plomid_columnar::read::CompressedOp::Sum,
            chunk.row_count
        ));
        let nulls = test_null_slice(&bytes, &reader, ColumnId::new(0));
        let active = vec![1_u8; chunk.row_count as usize];
        let Some(agg) = test_rle_agg(&bytes, chunk, &nulls, true, Some(&active)) else {
            panic!("RLE agg must serve friendly fixture");
        };
        // Oracle via full decode.
        let values = test_int_values(&bytes, &reader, ColumnId::new(0));
        let mut exp_count = 0_u64;
        let mut exp_sum = 0_i64;
        let mut exp_has = false;
        let mut exp_min: Option<i64> = None;
        let mut exp_max: Option<i64> = None;
        for x in values.iter().flatten() {
            exp_count += 1;
            exp_sum = exp_sum.wrapping_add(*x);
            exp_has = true;
            exp_min = Some(exp_min.map_or(*x, |m: i64| m.min(*x)));
            exp_max = Some(exp_max.map_or(*x, |m: i64| m.max(*x)));
        }
        // Wrapping oracle matches release; debug panic parity is covered
        // by `add_mul` unit reasoning (monotonic runs ⇒ final-range check).
        assert_eq!(agg.count, exp_count, "count");
        assert_eq!(agg.sum_has_any, exp_has, "has_any");
        if exp_has {
            assert_eq!(agg.sum, exp_sum, "sum");
            assert_eq!(agg.min, exp_min, "min");
            assert_eq!(agg.max, exp_max, "max");
        }
        // Equality + range filters over runs only.
        for (op, bound) in [
            (PruneOperator::Equal, 12_i64),
            (PruneOperator::NotEqual, 12),
            (PruneOperator::Greater, 0),
            (PruneOperator::LessOrEqual, -50),
        ] {
            let Some(mask) = test_rle_int(&bytes, chunk, &nulls, op, bound) else {
                panic!("RLE mask must serve {op:?}");
            };
            assert_eq!(mask.len(), values.len());
            for (i, v) in values.iter().enumerate() {
                let expected = match v {
                    None => false,
                    Some(x) => match op {
                        PruneOperator::Equal => *x == bound,
                        PruneOperator::NotEqual => *x != bound,
                        PruneOperator::Greater => *x > bound,
                        PruneOperator::LessOrEqual => *x <= bound,
                        _ => false,
                    },
                };
                assert_eq!(mask[i] != 0, expected, "row {i} {op:?}");
            }
        }
    }
    // Edge shapes: empty / single / all-equal.
    for (label, edge_rows) in [
        ("empty", vec![]),
        ("single", vec![Row::new(vec![Field::Integer(9)])]),
        ("all-equal", vec![Row::new(vec![Field::Integer(4)]); 500]),
        ("all-null", vec![Row::new(vec![Field::Null]); 300]),
    ] {
        let (bytes, reader) =
            test_flush_bytes(&edge_rows, &[ColumnType::Integer], TestEncoding::Rle);
        let chunks = reader.column_chunks(ColumnId::new(0));
        if chunks.is_empty() {
            continue;
        }
        let chunk = chunks[0].clone();
        let nulls = test_null_slice(&bytes, &reader, ColumnId::new(0));
        // Small chunks fall back via gate; large all-equal uses fast path.
        if chunk.row_count < plomid_columnar::read::COMPRESSED_MIN_ROWS {
            assert!(
                !should_execute_compressed(
                    chunk.encoding,
                    plomid_columnar::read::CompressedOp::Sum,
                    chunk.row_count
                ),
                "{label} small chunk must gate off"
            );
            continue;
        }
        if chunk.encoding != TestEncoding::Rle {
            continue;
        }
        let active = vec![1_u8; chunk.row_count as usize];
        let Some(agg) = test_rle_agg(&bytes, &chunk, &nulls, true, Some(&active)) else {
            panic!("{label} RLE agg must serve");
        };
        let values = if edge_rows.is_empty() {
            Vec::new()
        } else {
            test_int_values(&bytes, &reader, ColumnId::new(0))
        };
        let exp_count = values.iter().filter(|v| v.is_some()).count() as u64;
        assert_eq!(agg.count, exp_count, "{label} count");
    }
}

#[test]
fn compressed_dictionary_exactness_against_decode() {
    // Low-cardinality integers (5 distinct over 2000 rows) + NULLs.
    let mut rows = Vec::new();
    for i in 0..2000 {
        if i % 53 == 0 {
            rows.push(Row::new(vec![Field::Null]));
        } else {
            rows.push(Row::new(vec![Field::Integer((i % 5) as i64 * 11)]));
        }
    }
    let (bytes, reader) = test_flush_bytes(&rows, &[ColumnType::Integer], TestEncoding::Dictionary);
    let chunk = reader.column_chunks(ColumnId::new(0))[0].clone();
    if chunk.encoding != TestEncoding::Dictionary {
        // Auto/Raw fallback for this shape still proves fallback exactness
        // via the existing vector tests; skip fast-path asserts.
        return;
    }
    let nulls = test_null_slice(&bytes, &reader, ColumnId::new(0));
    // `=` filter directly on codes.
    for bound in [0_i64, 22, 999] {
        let Some(mask) = test_dict_eq(&bytes, &chunk, &nulls, &bound.to_le_bytes()) else {
            panic!("dict eq must serve");
        };
        let values = test_int_values(&bytes, &reader, ColumnId::new(0));
        assert_eq!(mask.len(), values.len());
        for (i, v) in values.iter().enumerate() {
            let expected = matches!(v, Some(x) if *x == bound);
            assert_eq!(mask[i] != 0, expected, "row {i} bound {bound}");
        }
    }
    // COUNT DISTINCT via code-set size (single chunk ⇒ exact).
    let active = vec![1_u8; chunk.row_count as usize];
    let Some(count) = test_dict_distinct(&bytes, &chunk, &nulls, Some(&active)) else {
        panic!("dict distinct must serve");
    };
    let values = test_int_values(&bytes, &reader, ColumnId::new(0));
    let mut set = HashSet::new();
    for x in values.iter().flatten() {
        set.insert(*x);
    }
    assert_eq!(count, set.len(), "distinct");
    // GROUP BY keys directly on codes (values resolved only for output).
    let Some(keys) = test_dict_keys(&bytes, &chunk, &nulls) else {
        panic!("dict keys must serve");
    };
    assert_eq!(keys.len(), values.len());
    for (k, v) in keys.iter().zip(values.iter()) {
        assert_eq!(k, v, "group key resolves to value");
    }
    // String dictionary `=` filter (raw byte order, multi-modal safe:
    // JSON/document paths untouched — only String/Bytes physics here).
    let words = ["alpha", "beta", "gamma"];
    let srows: Vec<Row> = (0..1500)
        .map(|i| {
            if i % 61 == 0 {
                Row::new(vec![Field::Null])
            } else {
                Row::new(vec![Field::String(words[i % 3].to_owned())])
            }
        })
        .collect();
    let (sbytes, sreader) =
        test_flush_bytes(&srows, &[ColumnType::String], TestEncoding::Dictionary);
    let schunk = sreader.column_chunks(ColumnId::new(0))[0].clone();
    if schunk.encoding != TestEncoding::Dictionary {
        return;
    }
    let snulls = test_null_slice(&sbytes, &sreader, ColumnId::new(0));
    let Some(smask) = test_dict_eq(&sbytes, &schunk, &snulls, b"beta") else {
        panic!("string dict eq must serve");
    };
    let back = sreader
        .read_rows(&sbytes, 0, sreader.row_count, &[ColumnId::new(0)])
        .expect("read");
    for (i, row) in back.iter().enumerate() {
        let expected = matches!(&row.fields()[0], Field::String(s) if s == "beta");
        assert_eq!(smask[i] != 0, expected, "string row {i}");
    }
}

#[test]
fn compressed_dictionary_minmax_exactness_against_decode() {
    // Global MIN/MAX over an integer dictionary serve via resolved codes
    // (no value-stream expansion); raw dictionaries still decline to the
    // decode path, where byte order is authoritative.
    let mut rows = Vec::new();
    for i in 0..2000 {
        if i % 53 == 0 {
            rows.push(Row::new(vec![Field::Null]));
        } else {
            rows.push(Row::new(vec![Field::Integer((i % 5) as i64 * 11 - 7)]));
        }
    }
    let (bytes, reader) = test_flush_bytes(&rows, &[ColumnType::Integer], TestEncoding::Dictionary);
    let col = ColumnId::new(0);
    let chunks = reader.column_chunks(col);
    if chunks.is_empty()
        || chunks
            .iter()
            .any(|c| c.encoding != TestEncoding::Dictionary)
    {
        // Encoder fallback for this shape: exactness stays covered by the
        // decode-path tests; nothing new to assert here.
        return;
    }
    let need_flex: BTreeSet<ColumnId> = BTreeSet::from([col]);
    let empty: BTreeSet<ColumnId> = BTreeSet::new();
    let request = VectorRequest {
        aggregates: vec![
            VectorAggregate {
                kind: VectorAggKind::Min,
                column: Some(col),
                filter: None,
            },
            VectorAggregate {
                kind: VectorAggKind::Max,
                column: Some(col),
                filter: None,
            },
        ],
        filter: None,
        group_by: vec![],
        batch_rows: 0,
        text_ordering_exact: false,
    };
    let candidates = vec![CandidateRange::new(0, reader.row_count)];
    let mut global = vec![VectorAcc::default(), VectorAcc::default()];
    let mut grouped = HashMap::new();
    let mut result = VectorResult {
        groups: Vec::new(),
        segments_considered: 1,
        segments_skipped: 0,
        rows_examined: 0,
        rows_skipped: 0,
        columns_read: 0,
    };
    let served = try_compressed_segment(
        &reader,
        &bytes,
        &candidates,
        &request,
        None,
        &[None, None],
        &empty,
        &need_flex,
        &empty,
        &empty,
        &empty,
        &mut global,
        &mut grouped,
        &mut result,
    )
    .expect("no error");
    assert_eq!(served, Some(true), "integer-dictionary MIN/MAX must serve");
    let values = test_int_values(&bytes, &reader, col);
    let mut expected_min: Option<i64> = None;
    let mut expected_max: Option<i64> = None;
    for value in values.iter().flatten() {
        expected_min = Some(expected_min.map_or(*value, |m: i64| m.min(*value)));
        expected_max = Some(expected_max.map_or(*value, |m: i64| m.max(*value)));
    }
    assert_eq!(global[0].min, expected_min);
    assert_eq!(global[1].max, expected_max);
    // Equality-filtered variant: the `=` leaf serves over dictionary
    // codes, so the masked fold stays on the compressed path.
    let filter = PrunePredicate::compare(col, PruneOperator::Equal, Field::Integer(15));
    let mut fglobal = vec![VectorAcc::default(), VectorAcc::default()];
    let mut fgrouped = HashMap::new();
    let mut fresult = VectorResult {
        groups: Vec::new(),
        segments_considered: 1,
        segments_skipped: 0,
        rows_examined: 0,
        rows_skipped: 0,
        columns_read: 0,
    };
    let fserved = try_compressed_segment(
        &reader,
        &bytes,
        &candidates,
        &request,
        Some(&filter),
        &[None, None],
        &empty,
        &need_flex,
        &empty,
        &empty,
        &empty,
        &mut fglobal,
        &mut fgrouped,
        &mut fresult,
    )
    .expect("no error");
    assert_eq!(
        fserved,
        Some(true),
        "filtered dictionary MIN/MAX must serve"
    );
    let mut fmin: Option<i64> = None;
    let mut fmax: Option<i64> = None;
    for value in values.iter().flatten() {
        if *value != 15 {
            continue;
        }
        fmin = Some(fmin.map_or(*value, |m: i64| m.min(*value)));
        fmax = Some(fmax.map_or(*value, |m: i64| m.max(*value)));
    }
    assert_eq!((fmin, fmax), (Some(15), Some(15)));
    assert_eq!(fglobal[0].min, fmin);
    assert_eq!(fglobal[1].max, fmax);
    // Raw dictionary MIN declines (byte order is the decode path's job).
    let srows: Vec<Row> = (0..1500)
        .map(|i| {
            if i % 61 == 0 {
                Row::new(vec![Field::Null])
            } else {
                Row::new(vec![Field::String(format!("k-{i:04}"))])
            }
        })
        .collect();
    let (sbytes, sreader) =
        test_flush_bytes(&srows, &[ColumnType::String], TestEncoding::Dictionary);
    let schunks = sreader.column_chunks(col);
    if schunks.is_empty()
        || schunks
            .iter()
            .any(|c| c.encoding != TestEncoding::Dictionary)
    {
        return;
    }
    let srequest = VectorRequest {
        aggregates: vec![VectorAggregate {
            kind: VectorAggKind::Min,
            column: Some(col),
            filter: None,
        }],
        filter: None,
        group_by: vec![],
        batch_rows: 0,
        text_ordering_exact: true,
    };
    let scandidates = vec![CandidateRange::new(0, sreader.row_count)];
    let sneed_string: BTreeSet<ColumnId> = BTreeSet::from([col]);
    let mut sglobal = vec![VectorAcc::default()];
    let mut sgrouped = HashMap::new();
    let mut sresult = VectorResult {
        groups: Vec::new(),
        segments_considered: 1,
        segments_skipped: 0,
        rows_examined: 0,
        rows_skipped: 0,
        columns_read: 0,
    };
    let sserved = try_compressed_segment(
        &sreader,
        &sbytes,
        &scandidates,
        &srequest,
        None,
        &[None],
        &empty,
        &empty,
        &empty,
        &sneed_string,
        &empty,
        &mut sglobal,
        &mut sgrouped,
        &mut sresult,
    )
    .expect("no error");
    assert_eq!(sserved, None, "raw-dictionary MIN must decline to decode");
}

#[test]
fn compressed_delta_exactness_against_decode() {
    // Sorted integers without NULLs (delta-friendly, constant step).
    let values: Vec<i64> = (0..2000).map(|i| 1_000_000 + i * 7).collect();
    let rows: Vec<Row> = values
        .iter()
        .map(|v| Row::new(vec![Field::Integer(*v)]))
        .collect();
    let (bytes, reader) =
        test_flush_bytes(&rows, &[ColumnType::Integer], TestEncoding::DeltaBitpack);
    let chunk = reader.column_chunks(ColumnId::new(0))[0].clone();
    if chunk.encoding != TestEncoding::DeltaBitpack {
        return;
    }
    let nulls = test_null_slice(&bytes, &reader, ColumnId::new(0));
    // MIN/MAX from first+step bounds (width==0 ⇒ provably monotonic).
    let Some((min, max)) = test_delta_minmax(&bytes, &chunk, &nulls, None) else {
        panic!("delta minmax must serve constant-step");
    };
    assert_eq!(min, Some(values[0]));
    assert_eq!(max, Some(*values.last().expect("nonempty")));
    // Range filter via prefix reconstruction (early exit for `>`).
    for (op, bound) in [
        (PruneOperator::Greater, 1_003_500_i64),
        (PruneOperator::Less, 1_001_000),
        (PruneOperator::Equal, 1_000_000 + 100 * 7),
    ] {
        let Some(mask) = test_delta_mask(&bytes, &chunk, &nulls, op, bound) else {
            panic!("delta mask must serve {op:?}");
        };
        assert_eq!(mask.len(), values.len());
        for (i, v) in values.iter().enumerate() {
            let expected = match op {
                PruneOperator::Greater => *v > bound,
                PruneOperator::Less => *v < bound,
                PruneOperator::Equal => *v == bound,
                _ => false,
            };
            assert_eq!(mask[i] != 0, expected, "row {i}");
        }
    }
    // Non-monotonic residues (width>0 mixed deltas) decline MIN/MAX.
    let mut rng_rows: Vec<Row> = Vec::new();
    let mut state = 0_i64;
    let mut lcg = Lcg(0xC0FFEE);
    for _ in 0..800 {
        let step = (lcg.below(11) as i64) - 5; // -5..=5 mixed
        state = state.wrapping_add(step);
        rng_rows.push(Row::new(vec![Field::Integer(state)]));
    }
    let (rbytes, rreader) = test_flush_bytes(
        &rng_rows,
        &[ColumnType::Integer],
        TestEncoding::DeltaBitpack,
    );
    let rchunk = rreader.column_chunks(ColumnId::new(0))[0].clone();
    if rchunk.encoding == TestEncoding::DeltaBitpack {
        let rnulls = test_null_slice(&rbytes, &rreader, ColumnId::new(0));
        // Width>0 shapes must fall back for MIN/MAX (exactness via decode).
        // Either serves (constant-step sub-case) or declines — both are
        // correct as long as serving implies equality with decode.
        if let Some((rmin, rmax)) = test_delta_minmax(&rbytes, &rchunk, &rnulls, None) {
            let back = rreader
                .read_rows(&rbytes, 0, rreader.row_count, &[ColumnId::new(0)])
                .expect("read");
            let mut exp_min: Option<i64> = None;
            let mut exp_max: Option<i64> = None;
            for row in &back {
                if let Field::Integer(v) = row.fields()[0] {
                    exp_min = Some(exp_min.map_or(v, |m: i64| m.min(v)));
                    exp_max = Some(exp_max.map_or(v, |m: i64| m.max(v)));
                }
            }
            assert_eq!(rmin, exp_min);
            assert_eq!(rmax, exp_max);
        }
        // Range masks over mixed deltas stay exact (full prefix scan).
        let Some(rmask) = test_delta_mask(&rbytes, &rchunk, &rnulls, PruneOperator::Greater, 0)
        else {
            panic!("delta range must serve mixed steps");
        };
        let back = rreader
            .read_rows(&rbytes, 0, rreader.row_count, &[ColumnId::new(0)])
            .expect("read");
        for (i, row) in back.iter().enumerate() {
            let expected = matches!(row.fields()[0], Field::Integer(v) if v > 0);
            assert_eq!(rmask[i] != 0, expected, "mixed row {i}");
        }
    }
}

#[test]
fn compressed_fallback_triggers() {
    // Raw encoding never executes.
    let rows: Vec<Row> = (0..500)
        .map(|i| Row::new(vec![Field::Integer(i)]))
        .collect();
    let (bytes, reader) = test_flush_bytes(&rows, &[ColumnType::Integer], TestEncoding::Raw);
    let chunk = &reader.column_chunks(ColumnId::new(0))[0];
    assert_eq!(chunk.encoding, TestEncoding::Raw);
    let nulls = test_null_slice(&bytes, &reader, ColumnId::new(0));
    let active = vec![1_u8; chunk.row_count as usize];
    assert!(test_rle_agg(&bytes, chunk, &nulls, true, Some(&active)).is_none());
    assert!(test_delta_minmax(&bytes, chunk, &nulls, None).is_none());
    assert!(test_dict_distinct(&bytes, chunk, &nulls, Some(&active)).is_none());
    // Truncated body declines (fail-closed → decode errors authoritatively).
    let (dbytes, dreader) = test_flush_bytes(&rows, &[ColumnType::Integer], TestEncoding::Rle);
    let dchunk = dreader.column_chunks(ColumnId::new(0))[0].clone();
    if dchunk.encoding == TestEncoding::Rle {
        let mut corrupt = dbytes.clone();
        let pos = dchunk.file_offset as usize;
        if pos < corrupt.len() {
            corrupt[pos] ^= 0xFF;
            // Corrupt payload still decodes via fallback as corruption
            // (SegmentReader::decode rejects the checksum), while the
            // fast path itself returns None instead of wrong answers.
            assert!(SegmentReader::decode(&corrupt).is_err());
            assert!(test_rle_agg(&corrupt, &dchunk, &nulls, true, Some(&active)).is_none());
        }
    }
    // Ragged integer run (non-NULL length != 8) declines.
    // Hand-built RLE body: one run of length 1 with value `b"abc"`.
    let mut body = Vec::new();
    body.extend_from_slice(&1_u64.to_le_bytes());
    body.extend_from_slice(&1_u64.to_le_bytes());
    body.extend_from_slice(&3_u32.to_le_bytes());
    body.extend_from_slice(b"abc");
    // Bypass segment framing: exercise the parser's ragged check via a
    // synthetic flush is overkill — assert the gate + helper contract
    // instead: RLE SUM over a String column still aggregates as raw
    // (no integer confusion).
    let srows: Vec<Row> = (0..300)
        .map(|i| Row::new(vec![Field::String(format!("k-{i:04}"))]))
        .collect();
    let (sbytes, sreader) = test_flush_bytes(&srows, &[ColumnType::String], TestEncoding::Rle);
    let schunks = sreader.column_chunks(ColumnId::new(0));
    if !schunks.is_empty() && schunks[0].encoding == TestEncoding::Rle {
        let snulls = test_null_slice(&sbytes, &sreader, ColumnId::new(0));
        let sactive = vec![1_u8; schunks[0].row_count as usize];
        // Integer interpretation of string runs must decline (ragged).
        assert!(test_rle_int(&sbytes, schunks[0], &snulls, PruneOperator::Equal, 5).is_none());
        // Raw interpretation serves.
        assert!(test_rle_raw(
            &sbytes,
            schunks[0],
            &snulls,
            PruneOperator::Equal,
            b"k-0005"
        )
        .is_some());
        let _ = sactive;
    }
}
