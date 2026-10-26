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
//! PLOMID columnar compression benchmarks (phase 38D).
//!
//! Run with: `cargo bench -p plomid-columnar --bench compression`.
//!
//! Per fixture (sorted ints, random ints, timestamps, booleans, low-cardinality
//! strings, high-cardinality strings) this reports, for every value encoding
//! (`Raw`/`Rle`/`DeltaBitpack`/`Dictionary`/`Auto`) crossed with every
//! available byte codec (`None`, plus `Zstd` when the `zstd` feature is built):
//!
//! * `encode`: throughput in rows/s plus stored bytes and the stored/raw ratio.
//! * `decode`: throughput in rows/s of `decode_chunk` over the stored payload.
//! * `scan+agg`: end-to-end rows/s of flush → `SegmentReader::decode` →
//!   `read_column` → aggregate over the materialized column (the public
//!   read/vector API surface, Raw vs Auto only to bound runtime).
//!
//! Encode/decode go through the stable public API (`encode_chunk` /
//! `decode_chunk`) only, so this bench keeps working while sibling phases
//! refactor internals. Every measured payload is decoded and asserted equal
//! to its input, so a passing bench is also a round-trip correctness proof.

use plomid_columnar::{
    decode_chunk, encode_chunk, flush, is_compression_available, materialize_columns,
    ChunkCompression, ColumnEncoding, ColumnType, FlushConfig, SegmentReader,
};
use plomid_core::{CatalogVersion, GenerationId, Lsn, ObjectId, SchemaId, SegmentId};
use plomid_storage::{Field, Row};
use std::time::Instant;

const ROWS: usize = 10_000;

struct Fixture {
    label: &'static str,
    rows: Vec<Row>,
    types: Vec<ColumnType>,
}

fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

fn fixtures() -> Vec<Fixture> {
    let int = vec![ColumnType::Integer];
    let str_ = vec![ColumnType::String];

    // Sorted integers: 0..ROWS (delta-friendly, monotonically increasing).
    let sorted: Vec<Row> = (0..ROWS)
        .map(|i| Row::new(vec![Field::Integer(i as i64)]))
        .collect();

    // Random integers: LCG full-range values (incompressible control).
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let random: Vec<Row> = (0..ROWS)
        .map(|_| Row::new(vec![Field::Integer(lcg(&mut state) as i64)]))
        .collect();

    // Timestamps: regular 1ms-spaced micros since an epoch (time-series shape).
    let timestamps: Vec<Row> = (0..ROWS)
        .map(|i| {
            Row::new(vec![Field::Integer(
                1_700_000_000_000_000 + i as i64 * 1_000,
            )])
        })
        .collect();

    // Booleans: alternating 0/1 stored as integers (RLE-friendly runs of one
    // value only when run together; alternating is the adversarial case, long
    // runs of a single value are covered by the low-card fixture).
    let booleans: Vec<Row> = (0..ROWS)
        .map(|i| Row::new(vec![Field::Integer((i % 2) as i64)]))
        .collect();

    // Low-cardinality strings: 3 distinct values cycling.
    let low_card: Vec<Row> = (0..ROWS)
        .map(|i| {
            Row::new(vec![Field::String(
                ["alpha", "beta", "gamma"][i % 3].to_owned(),
            )])
        })
        .collect();

    // High-cardinality strings: every value unique.
    let high_card: Vec<Row> = (0..ROWS)
        .map(|i| Row::new(vec![Field::String(format!("user-{i:08}-suffix"))]))
        .collect();

    vec![
        Fixture {
            label: "sorted-ints",
            rows: sorted,
            types: int.clone(),
        },
        Fixture {
            label: "random-ints",
            rows: random,
            types: int.clone(),
        },
        Fixture {
            label: "timestamps",
            rows: timestamps,
            types: int.clone(),
        },
        Fixture {
            label: "booleans",
            rows: booleans,
            types: int,
        },
        Fixture {
            label: "low-card-strings",
            rows: low_card,
            types: str_.clone(),
        },
        Fixture {
            label: "high-card-strings",
            rows: high_card,
            types: str_,
        },
    ]
}

/// Builds the chunk value stream (`[length][value]` entries in row order) for
/// a single-column fixture, exactly as the flush path lays it out.
fn value_stream(fixture: &Fixture) -> (Vec<u8>, u64) {
    let cols = materialize_columns(&fixture.rows, &fixture.types).expect("materialize");
    assert_eq!(cols.len(), 1);
    let col = &cols[0];
    let mut stream = Vec::with_capacity(col.values.len() + 4 * ROWS);
    for row in 0..col.row_count as usize {
        let bytes = col.get_value(row).unwrap_or(&[]);
        stream.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        stream.extend_from_slice(bytes);
    }
    (stream, col.row_count)
}

fn rows_per_sec(rows: u64, secs: f64) -> f64 {
    if secs > 0.0 {
        rows as f64 / secs
    } else {
        0.0
    }
}

fn available_codecs() -> Vec<ChunkCompression> {
    let mut codecs = vec![ChunkCompression::None];
    if is_compression_available(ChunkCompression::Zstd) {
        codecs.push(ChunkCompression::Zstd);
    }
    codecs
}

fn bench_fixture(fixture: &Fixture, codecs: &[ChunkCompression]) {
    println!("fixture: {} ({ROWS} rows)", fixture.label);
    let (stream, count) = value_stream(fixture);
    let raw_len = stream.len();

    let encodings = [
        ColumnEncoding::Raw,
        ColumnEncoding::Rle,
        ColumnEncoding::DeltaBitpack,
        ColumnEncoding::Dictionary,
        ColumnEncoding::Auto,
    ];

    for codec in codecs {
        let codec_name = match codec {
            ChunkCompression::None => "none",
            ChunkCompression::Zstd => "zstd",
        };
        for encoding in encodings {
            let encoded = match encode_chunk(&stream, count, encoding, *codec) {
                Ok(out) => out,
                Err(_) => {
                    println!("  {encoding:>12?} x {codec_name:<4}: not applicable in this build");
                    continue;
                }
            };
            // Correctness: every measured payload must round-trip exactly.
            let back = decode_chunk(
                &encoded.payload,
                encoded.encoding,
                encoded.codec,
                encoded.uncompressed_len,
                count,
            )
            .expect("decode");
            assert_eq!(back, stream, "{:?} x {codec_name} round trip", encoding);
            let stored = encoded.payload.len();
            let ratio = stored as f64 / raw_len.max(1) as f64;

            // Encode throughput.
            let iters = 20_u64;
            let start = Instant::now();
            for _ in 0..iters {
                let out = encode_chunk(&stream, count, encoding, *codec).expect("encode");
                std::hint::black_box(out.payload.len());
            }
            let encode_secs = start.elapsed().as_secs_f64();

            // Decode throughput.
            let decode_iters = 50_u64;
            let start = Instant::now();
            for _ in 0..decode_iters {
                let out = decode_chunk(
                    &encoded.payload,
                    encoded.encoding,
                    encoded.codec,
                    encoded.uncompressed_len,
                    count,
                )
                .expect("decode");
                std::hint::black_box(out.len());
            }
            let decode_secs = start.elapsed().as_secs_f64();

            println!(
                "  {encoding:>12?} x {codec_name:<4}: encode {:>12.0} rows/s | \
                 decode {:>12.0} rows/s | stored {stored:>7}B ratio {ratio:.3} (won: {:?})",
                rows_per_sec(ROWS as u64 * iters, encode_secs),
                rows_per_sec(ROWS as u64 * decode_iters, decode_secs),
                encoded.encoding,
            );
        }
    }

    // End-to-end scan+agg through the public read API: flush a segment, then
    // decode + read_column + aggregate. Raw vs Auto only (bounds runtime).
    for encoding in [ColumnEncoding::Raw, ColumnEncoding::Auto] {
        let cfg = FlushConfig::default().with_encoding(encoding);
        let flushed = flush(
            &fixture.rows,
            &fixture.types,
            GenerationId::new(1),
            SegmentId::new(1),
            &cfg,
        )
        .expect("flush");
        let image = flushed.bytes;
        let reader = SegmentReader::decode(&image).expect("segment decode");
        assert_eq!(reader.row_count, ROWS as u64);
        let meta = &reader.columns[0];
        let iters = 20_u64;
        let start = Instant::now();
        let mut sink = 0_i64;
        for _ in 0..iters {
            let col = reader
                .read_column(&image, meta, 0, reader.row_count)
                .expect("read");
            // Aggregate: wrapping sum over 8-byte integer cells, byte count
            // otherwise. Same loop shape as the vector executor's hot path.
            if fixture.types[0] == ColumnType::Integer {
                for row in 0..col.row_count as usize {
                    if col.is_null(row) {
                        continue;
                    }
                    if let Some(bytes) = col.get_value(row) {
                        if bytes.len() == 8 {
                            sink = sink.wrapping_add(i64::from_le_bytes(
                                bytes.try_into().expect("8 bytes"),
                            ));
                        }
                    }
                }
            } else {
                for row in 0..col.row_count as usize {
                    if let Some(bytes) = col.get_value(row) {
                        sink = sink.wrapping_add(bytes.len() as i64);
                    }
                }
            }
            std::hint::black_box(sink);
        }
        let secs = start.elapsed().as_secs_f64();
        println!(
            "  scan+agg {encoding:>12?} x none: {:>12.0} rows/s | image {:>7}B",
            rows_per_sec(ROWS as u64 * iters, secs),
            image.len(),
        );
    }
    println!();
}

fn main() {
    println!("PLOMID columnar compression benchmarks ({ROWS} rows/fixture)");
    let codecs = available_codecs();
    if codecs.len() == 1 {
        println!("(zstd not available in this build: codec=None only)");
    } else {
        println!("(codecs: none + zstd)");
    }
    // Touch rarely-used imports so the bench tracks the public surface it
    // claims: schema/object identities used by the flush path.
    std::hint::black_box((
        SchemaId::new(7),
        CatalogVersion::new(1),
        ObjectId::new(1),
        Lsn::new(1),
    ));
    println!();
    for fixture in &fixtures() {
        bench_fixture(fixture, &codecs);
    }
    println!("Benchmarks complete.");
}
