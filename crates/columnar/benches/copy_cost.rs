//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0;
//
// DIAGNOSTIC microbench: isolates the owned-copy cost in the
// int vector path (values-bytes Vec -> Vec<i64> -> sum) vs a borrowed
// slice sum. Decides whether eliminating copy #2 is worth a production
// change. Not a gate; numbers go to the phase report.
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

fn status_quo(values: &[u8], lengths_ok: bool) -> i64 {
    assert!(lengths_ok);
    let mut out = Vec::with_capacity(values.len() / 8);
    for chunk in values.chunks_exact(8) {
        out.push(i64::from_le_bytes(chunk.try_into().unwrap()));
    }
    out.iter().fold(0i64, |a, v| a.wrapping_add(*v))
}

fn borrowed_sum(bytes: &[u8]) -> i64 {
    // No intermediate Vec: decode 8-byte entries in place over the
    // borrowed buffer (the most production code can do without `unsafe`;
    // the crate forbids it on all targets, benches included).
    let mut total = 0i64;
    for chunk in bytes.chunks_exact(8) {
        total = total.wrapping_add(i64::from_le_bytes(chunk.try_into().unwrap()));
    }
    total
}

fn bench_copy_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("int_copy_cost_1M");
    group.throughput(Throughput::Elements(1_000_000));
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut bytes = Vec::with_capacity(8_000_000);
    for _ in 0..1_000_000 {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        bytes.extend_from_slice(&(state as i64).to_le_bytes());
    }
    let bytes = &bytes[..8_000_000];
    group.bench_function("copy_then_sum", |b| {
        b.iter(|| status_quo(black_box(bytes), true))
    });
    group.bench_function("borrowed_sum", |b| {
        b.iter(|| borrowed_sum(black_box(bytes)))
    });
    group.finish();
}

criterion_group!(benches, bench_copy_cost);
criterion_main!(benches);
