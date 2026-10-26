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
// Backend-vs-backend comparison on one host (same input, all backends
// the host can execute). Numbers go into the phase report; the harness
// `compare.py` turns reruns into deltas. Correctness (bit-identity) is
// asserted by `crates/hw/tests/parity.rs`, never by this bench.
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use plomid_hw::profile::VectorIsa;
use plomid_hw::VectorOps;

fn inputs() -> (Vec<i64>, Vec<f64>) {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    let ints: Vec<i64> = (0..1_000_000).map(|_| next() as i64).collect();
    let floats: Vec<f64> = (0..1_000_000)
        .map(|_| (next() % 200_000) as f64 / 97.0)
        .collect();
    (ints, floats)
}

fn bench_backends(c: &mut Criterion) {
    let (ints, floats) = inputs();
    let mut group = c.benchmark_group("vector_ops_1M");
    group.throughput(Throughput::Elements(1_000_000));
    for isa in [
        VectorIsa::Scalar,
        VectorIsa::Neon,
        VectorIsa::Avx2,
        VectorIsa::Avx512,
    ] {
        let ops = VectorOps::force(isa);
        // Backends that cannot execute here still construct; running them
        // would be UB-free but meaningless, so only bench live ones.
        let live = match isa {
            VectorIsa::Scalar => true,
            #[cfg(target_arch = "aarch64")]
            VectorIsa::Neon => plomid_hw::profile::CpuFeatures::detect().neon,
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx2 => plomid_hw::profile::CpuFeatures::detect().avx2,
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx512 => plomid_hw::profile::CpuFeatures::detect().avx512,
            _ => false,
        };
        if !live {
            continue;
        }
        let name = isa.to_string();
        group.bench_function(format!("sum_i64/{name}"), |b| {
            b.iter(|| ops.sum_i64(black_box(&ints)))
        });
        group.bench_function(format!("sum_f64/{name}"), |b| {
            b.iter(|| ops.sum_f64(black_box(&floats)))
        });
        group.bench_function(format!("eq_scalar_i64/{name}"), |b| {
            b.iter(|| ops.eq_scalar_i64(black_box(&ints), 12345))
        });
        group.bench_function(format!("add_i64/{name}"), |b| {
            b.iter(|| ops.add_i64(black_box(&ints), black_box(&ints)))
        });
        group.bench_function(format!("mul_f64/{name}"), |b| {
            b.iter(|| ops.mul_f64(black_box(&floats), black_box(&floats)))
        });
        group.bench_function(format!("and_mask/{name}"), |b| {
            b.iter(|| {
                let m = ops.eq_scalar_i64(black_box(&ints), 12345);
                ops.and_mask(black_box(&m), black_box(&m))
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_backends);
criterion_main!(benches);
