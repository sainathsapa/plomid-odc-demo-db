//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID — Platform for Modern Intelligence and Data
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0.

//! Adaptive-encoding fixtures: encode time and stored size per shape.
//!
//! Run with: `cargo bench -p plomid-columnar --bench adaptive_encoding`.
//!
//! Five shapes the adaptive policy must route correctly: sorted integers,
//! random integers, low-cardinality strings, booleans (1-byte 0/1 entries),
//! and regular-interval timestamps. Each fixture reports the raw stream size,
//! the per-candidate stored sizes, the `Auto` winner, and the encode latency
//! of `Auto` versus the sum of all candidates (the brute-force cost).

use plomid_columnar::{encode_chunk, encode_chunk_with_hints, ChunkCompression, ColumnEncoding};
use plomid_columnar::{ColumnType, EncodingHints};
use std::time::Instant;

fn entry_stream(values: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(value);
    }
    out
}

fn int_values(values: &[i64]) -> Vec<Vec<u8>> {
    values.iter().map(|v| v.to_le_bytes().to_vec()).collect()
}

fn fixture_int_sorted(n: usize) -> (String, Vec<u8>, u64) {
    let values: Vec<i64> = (0..n as i64).collect();
    let raw = entry_stream(&int_values(&values));
    (format!("int-sorted {n} rows"), raw, n as u64)
}

fn fixture_int_random(n: usize) -> (String, Vec<u8>, u64) {
    let mut state = 0x2545_F491_4F6C_DD1D_u64;
    let values: Vec<i64> = (0..n)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state as i64
        })
        .collect();
    let raw = entry_stream(&int_values(&values));
    (format!("int-random {n} rows"), raw, n as u64)
}

fn fixture_low_card_string(n: usize) -> (String, Vec<u8>, u64) {
    let words = ["red", "green", "blue"];
    let values: Vec<Vec<u8>> = (0..n)
        .map(|i| words[i % words.len()].as_bytes().to_vec())
        .collect();
    let raw = entry_stream(&values);
    (format!("low-cardinality-string {n} rows"), raw, n as u64)
}

fn fixture_boolean(n: usize) -> (String, Vec<u8>, u64) {
    // 1-byte 0/1 entries in blocks of 8: run-friendly but dictionary-tiny.
    let values: Vec<Vec<u8>> = (0..n).map(|i| vec![u8::from((i / 8) % 2 == 0)]).collect();
    let raw = entry_stream(&values);
    (format!("boolean {n} rows"), raw, n as u64)
}

fn fixture_timestamp(n: usize) -> (String, Vec<u8>, u64) {
    let values: Vec<i64> = (0..n as i64)
        .map(|i| 1_700_000_000_000_000 + i * 1_000)
        .collect();
    let raw = entry_stream(&int_values(&values));
    (format!("timestamp-regular {n} rows"), raw, n as u64)
}

fn ms_per_iter(secs: f64, iters: u64) -> f64 {
    if iters == 0 {
        0.0
    } else {
        secs * 1000.0 / iters as f64
    }
}

/// Folds [`EncodingHints`] for one raw stream of known value widths.
///
/// `widths` holds each row's value length (0 for NULL rows); the bytes are
/// walked straight from the length-prefixed stream so the fold mirrors the
/// flush pass.
fn fold_hints(raw: &[u8], count: u64, column_type: ColumnType) -> EncodingHints {
    let mut hints = EncodingHints::new();
    let mut cursor = 0_usize;
    for _ in 0..count {
        let len = u32::from_le_bytes(raw[cursor..cursor + 4].try_into().expect("prefix")) as usize;
        cursor += 4;
        let bytes = &raw[cursor..cursor + len];
        cursor += len;
        // Zero-length entries on integer-typed streams stand for NULL rows in
        // these single-type fixtures (no empty-string ambiguity by shape).
        let is_null = len == 0 && matches!(column_type, ColumnType::Integer);
        hints.observe(if is_null { None } else { Some(bytes) }, column_type);
    }
    assert_eq!(cursor, raw.len());
    hints
}

fn p99(mut samples: Vec<f64>) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    samples[samples.len() * 99 / 100]
}

fn bench_fixture(name: &str, raw: &[u8], count: u64, column_type: ColumnType) {
    const CANDIDATES: [ColumnEncoding; 4] = [
        ColumnEncoding::Raw,
        ColumnEncoding::DeltaBitpack,
        ColumnEncoding::Dictionary,
        ColumnEncoding::Rle,
    ];
    println!("dataset: {name} (raw={}B rows={count})", raw.len());
    let mut brute_ms = 0.0;
    for encoding in CANDIDATES {
        let iters = 20_u64;
        let start = Instant::now();
        let mut stored = 0_usize;
        let mut applicable = true;
        for _ in 0..iters {
            match encode_chunk(raw, count, encoding, ChunkCompression::None) {
                Ok(out) => {
                    stored = out.payload.len();
                    std::hint::black_box(stored);
                }
                Err(_) => {
                    applicable = false;
                    break;
                }
            }
        }
        let secs = start.elapsed().as_secs_f64();
        let ms = ms_per_iter(secs, iters);
        brute_ms += ms;
        if applicable {
            println!(
                "  candidate {encoding:>12?}: stored={stored:>7}B ratio={:5.3} {:7.3} ms/encode",
                stored as f64 / raw.len().max(1) as f64,
                ms
            );
        } else {
            println!("  candidate {encoding:>12?}: inapplicable");
        }
    }
    // Brute-force Auto: every applicable encoding trialed per chunk.
    let iters = 100_u64;
    let mut samples = Vec::with_capacity(iters as usize);
    let mut winner = ColumnEncoding::Raw;
    let mut stored = raw.len();
    for _ in 0..iters {
        let start = Instant::now();
        let out = encode_chunk(raw, count, ColumnEncoding::Auto, ChunkCompression::None)
            .expect("auto encode");
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        winner = out.encoding;
        stored = out.payload.len();
        std::hint::black_box(stored);
    }
    let brute_mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let brute_p99 = p99(samples);
    // Adaptive Auto: the flush live path (hints order and prune trials).
    let hints = fold_hints(raw, count, column_type);
    let mut samples = Vec::with_capacity(iters as usize);
    let mut winner_ad = ColumnEncoding::Raw;
    let mut stored_ad = raw.len();
    for _ in 0..iters {
        let start = Instant::now();
        let out = encode_chunk_with_hints(
            raw,
            count,
            ColumnEncoding::Auto,
            ChunkCompression::None,
            Some(&hints),
            column_type,
        )
        .expect("adaptive encode");
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
        winner_ad = out.encoding;
        stored_ad = out.payload.len();
        std::hint::black_box(stored_ad);
    }
    let ad_mean = samples.iter().sum::<f64>() / samples.len() as f64;
    let ad_p99 = p99(samples);
    println!(
        "  Auto winner={winner:?} stored={stored}B ratio={:.3} mean={:7.3} ms p99={:7.3} ms (brute-force trial sum {:7.3} ms)",
        stored as f64 / raw.len().max(1) as f64,
        brute_mean,
        brute_p99,
        brute_ms,
    );
    println!(
        "  Adaptive winner={winner_ad:?} stored={stored_ad}B ratio={:.3} mean={:7.3} ms p99={:7.3} ms (encode CPU {:+.1}% vs brute Auto)",
        stored_ad as f64 / raw.len().max(1) as f64,
        ad_mean,
        ad_p99,
        if brute_mean > 0.0 {
            (ad_mean - brute_mean) / brute_mean * 100.0
        } else {
            0.0
        },
    );
    assert_eq!(
        (winner_ad, stored_ad),
        (winner, stored),
        "adaptive must store exactly what brute force stores"
    );
}

fn main() {
    println!("PLOMID adaptive-encoding fixtures (brute-force vs hinted Auto)");
    println!();
    const N: usize = 5_000;
    for (name, raw, count, column_type) in [
        fixture_int_sorted(N),
        fixture_int_random(N),
        fixture_low_card_string(N),
        fixture_boolean(N),
        fixture_timestamp(N),
    ]
    .into_iter()
    .map(|(name, raw, count)| {
        let column_type = if name.starts_with("int-") || name.starts_with("timestamp") {
            ColumnType::Integer
        } else if name.starts_with("boolean") {
            ColumnType::Bool
        } else {
            ColumnType::String
        };
        (name, raw, count, column_type)
    }) {
        bench_fixture(&name, &raw, count, column_type);
        println!();
    }
    println!("Benchmarks complete.");
}
