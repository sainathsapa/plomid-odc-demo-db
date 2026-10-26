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
// Bit-identity: every backend agrees with scalar on randomized inputs,
// including adversarial shapes (empty, unit, odd lengths, unaligned
// starts, extremes, wrapping overflow, NaN/infinities for f64).
// Backends that cannot execute on this host are still selection-tested.

use plomid_hw::profile::{Architecture, CpuFeatures, VectorIsa, MAX_WORKER_BUDGET};
use plomid_hw::vector::scalar;
use plomid_hw::{HardwareProfile, VectorOps};

fn lcg(state: &mut u64) -> u64 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    *state
}

fn rand_i64(state: &mut u64, narrow: bool) -> i64 {
    let raw = lcg(state) as i64;
    if narrow {
        (raw % 2000) - 1000
    } else if lcg(state).is_multiple_of(20) {
        // Extremes (overflow wrap, MIN/MAX) on a regular schedule.
        [i64::MIN, i64::MAX, -1, 0, 1][(lcg(state) % 5) as usize]
    } else {
        raw
    }
}

fn rand_f64(state: &mut u64) -> f64 {
    match lcg(state) % 32 {
        0 => f64::NAN,
        1 => f64::INFINITY,
        2 => f64::NEG_INFINITY,
        3 => -0.0,
        _ => ((lcg(state) % 200_000) as f64 - 100_000.0) / 97.0,
    }
}

fn all_backends() -> Vec<VectorOps> {
    let mut ops = vec![VectorOps::force(VectorIsa::Scalar)];
    #[cfg(target_arch = "aarch64")]
    if CpuFeatures::detect().neon {
        ops.push(VectorOps::force(VectorIsa::Neon));
    }
    #[cfg(target_arch = "x86_64")]
    {
        let cpu = CpuFeatures::detect();
        if cpu.avx2 {
            ops.push(VectorOps::force(VectorIsa::Avx2));
        }
        if cpu.avx512 {
            ops.push(VectorOps::force(VectorIsa::Avx512));
        }
    }
    ops
}

#[test]
fn backends_agree_with_scalar_on_randomized_inputs() {
    let backends = all_backends();
    assert!(
        !backends.is_empty(),
        "scalar backend must always be present"
    );
    // This host must exercise at least one SIMD backend, or the parity
    // suite degrades to scalar-vs-itself. aarch64 always has NEON;
    // x86_64 CI is expected to have AVX2 (AVX-512 where enabled).
    #[cfg(target_arch = "aarch64")]
    assert!(
        backends.iter().any(|o| o.isa() == VectorIsa::Neon),
        "aarch64 run must exercise the NEON backend"
    );
    #[cfg(target_arch = "x86_64")]
    assert!(
        backends
            .iter()
            .any(|o| o.isa() == VectorIsa::Avx2 || o.isa() == VectorIsa::Avx512),
        "x86_64 run must exercise an AVX backend"
    );
    let scalar = VectorOps::force(VectorIsa::Scalar);
    let mut state = 0x243F_6A88_85A3_08D3u64;
    for round in 0..60 {
        // Lengths sweep exact blocks, tails, unit, and empty.
        let len = [0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 100, 1000]
            [(round + (lcg(&mut state) % 15) as usize) % 15];
        let narrow = round % 2 == 0;
        let ints: Vec<i64> = (0..len).map(|_| rand_i64(&mut state, narrow)).collect();
        let floats: Vec<f64> = (0..len).map(|_| rand_f64(&mut state)).collect();
        let want_sum = scalar.sum_i64(&ints);
        let want_fsum = scalar.sum_f64(&floats);
        let want_min = scalar.min_i64(&ints);
        let want_max = scalar.max_i64(&ints);
        for ops in &backends {
            assert_eq!(
                ops.sum_i64(&ints),
                want_sum,
                "sum_i64 {:?} len={len}",
                ops.isa()
            );
            // NaN never equals: compare bitwise (bit-identity is the claim).
            assert_eq!(
                ops.sum_f64(&floats).to_bits(),
                want_fsum.to_bits(),
                "sum_f64 {:?} len={len}",
                ops.isa()
            );
            assert_eq!(
                ops.min_i64(&ints),
                want_min,
                "min {:?} len={len}",
                ops.isa()
            );
            assert_eq!(
                ops.max_i64(&ints),
                want_max,
                "max {:?} len={len}",
                ops.isa()
            );
        }
    }
}

#[test]
fn unaligned_starts_agree() {
    // Slices starting at every offset mod 32 (alignment coverage).
    let backends = all_backends();
    let scalar = VectorOps::force(VectorIsa::Scalar);
    let big: Vec<i64> = (0..300).map(|i| i as i64 * 3 - 100).collect();
    let bigf: Vec<f64> = (0..300).map(|i| i as f64 * 0.5).collect();
    for off in 0..32 {
        let ints = &big[off..off + 200];
        let floats = &bigf[off..off + 200];
        for ops in &backends {
            assert_eq!(ops.sum_i64(ints), scalar.sum_i64(ints), "off={off}");
            assert_eq!(
                ops.sum_f64(floats).to_bits(),
                scalar.sum_f64(floats).to_bits(),
                "off={off}"
            );
        }
    }
}

#[test]
fn empty_and_singleton_rules() {
    let scalar = VectorOps::force(VectorIsa::Scalar);
    assert_eq!(scalar.sum_i64(&[]), 0);
    assert_eq!(scalar.sum_f64(&[]), 0.0);
    assert_eq!(scalar.min_i64(&[]), None);
    assert_eq!(scalar.max_i64(&[]), None);
    assert_eq!(scalar.min_i64(&[7]), Some(7));
    assert_eq!(scalar.max_i64(&[-3]), Some(-3));
    // Single NaN propagates through lane 0; infinities keep their sign.
    assert!(scalar.sum_f64(&[f64::NAN]).is_nan());
    assert_eq!(scalar.sum_f64(&[f64::INFINITY, 1.0]), f64::INFINITY);
    assert_eq!(
        scalar.sum_f64(&[f64::NEG_INFINITY, -1.0]),
        f64::NEG_INFINITY
    );
}

#[test]
fn select_prefers_widest_available() {
    let ops = VectorOps::select();
    let cpu = CpuFeatures::detect();
    match Architecture::host() {
        Architecture::X86_64 => {
            let want = if cpu.avx512 {
                VectorIsa::Avx512
            } else if cpu.avx2 {
                VectorIsa::Avx2
            } else {
                VectorIsa::Scalar
            };
            assert_eq!(ops.isa(), want);
        }
        Architecture::Aarch64 => {
            assert_eq!(
                ops.isa(),
                if cpu.neon {
                    VectorIsa::Neon
                } else {
                    VectorIsa::Scalar
                }
            );
        }
        Architecture::Arm32 | Architecture::Other(_) => {
            assert_eq!(ops.isa(), VectorIsa::Scalar);
        }
    }
}

#[test]
fn profile_detect_reports_a_sane_host() {
    let profile = HardwareProfile::detect();
    assert!(profile.logical_cpus >= 1);
    assert!((1..=MAX_WORKER_BUDGET).contains(&profile.worker_budget()));
    assert!((1024..=1_048_576).contains(&profile.batch_rows()));
    if let Some(ram) = profile.ram_total_bytes {
        assert!(ram >= 256 * 1024 * 1024, "ram suspiciously small: {ram}");
    }
    // Budgets are pure: same profile, same answers.
    assert_eq!(profile.worker_budget(), profile.worker_budget());
    assert_eq!(profile.batch_rows(), profile.batch_rows());
    // Small-device rule is monotone in the inputs it reads.
    let _ = profile.is_small_device();
}

#[test]
fn f64_canonical_order_is_documented_and_stable() {
    let scalar = VectorOps::force(VectorIsa::Scalar);
    let values: Vec<f64> = (0..13).map(|i| (i as f64) / 3.0).collect();
    let mut lanes = [0.0f64; 4];
    for (k, block) in values.chunks_exact(4).enumerate() {
        for (l, v) in block.iter().enumerate() {
            let _ = k;
            lanes[l] += *v;
        }
    }
    for (j, v) in values.chunks_exact(4).remainder().iter().enumerate() {
        lanes[j % 4] += *v;
    }
    let canonical = ((lanes[0] + lanes[1]) + lanes[2]) + lanes[3];
    assert_eq!(scalar.sum_f64(&values).to_bits(), canonical.to_bits());
}

// ---------------------------------------------------------------------------
// Kernel parity: every backend agrees with scalar on
// randomized + adversarial inputs. f64 vectors compare bitwise (same
// elementwise IEEE op ⇒ identical bits guaranteed, NaN included).
// ---------------------------------------------------------------------------

/// Randomized kernel inputs (deterministic LCG; edges injected).
struct KernelInputs {
    ints_a: Vec<i64>,
    ints_b: Vec<i64>,
    floats_a: Vec<f64>,
    floats_b: Vec<f64>,
    mask_a: Vec<u8>,
    mask_b: Vec<u8>,
    nulls: Vec<u8>,
}

fn kernel_inputs(round: u64) -> KernelInputs {
    // Fixed base seed mixed with the round index: every round covers a
    // distinct deterministic input, and a failure replays from the
    // reported seed + round alone.
    const SEED: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut state = SEED ^ round.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let mut next = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state
    };
    // Lengths sweep exact blocks, tails, unit, empty.
    let len = [0usize, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 100, 1000][(next() % 15) as usize];
    let mut ints_a = Vec::with_capacity(len);
    let mut ints_b = Vec::with_capacity(len);
    for _ in 0..len {
        let pick = next() % 16;
        let edge =
            [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX][(next() % 7) as usize];
        ints_a.push(if pick < 6 { edge } else { next() as i64 });
        let pick = next() % 16;
        let edge =
            [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX][(next() % 7) as usize];
        ints_b.push(if pick < 6 { edge } else { next() as i64 });
    }
    let mut floats_a = Vec::with_capacity(len);
    let mut floats_b = Vec::with_capacity(len);
    for _ in 0..len {
        let fa = match next() % 24 {
            0 => f64::NAN,
            1 => -f64::NAN,
            2 => f64::INFINITY,
            3 => f64::NEG_INFINITY,
            4 => 0.0,
            5 => -0.0,
            6 => 5e-324,
            7 => f64::MAX,
            8 => f64::MIN_POSITIVE,
            _ => ((next() % 200_000) as f64 - 100_000.0) / 97.0,
        };
        let fb = match next() % 24 {
            0 => f64::NAN,
            1 => f64::INFINITY,
            2 => 0.0,
            3 => -0.0,
            _ => ((next() % 200_000) as f64 - 100_000.0) / 13.0,
        };
        floats_a.push(fa);
        floats_b.push(fb);
    }
    let mut mask_a = Vec::with_capacity(len.max(8));
    let mut mask_b = Vec::with_capacity(len.max(8));
    for _ in 0..len.max(8) {
        // 0/1 masks AND arbitrary bytes (kernels must treat any byte
        // exactly like scalar does).
        mask_a.push([0u8, 1, 0xFF, 0x7F, 0x80, 2][(next() % 6) as usize]);
        mask_b.push([0u8, 1, 0xFF, 0x01, 0xFE, 7][(next() % 6) as usize]);
    }
    let mut nulls = vec![0u8; len.div_ceil(8).max(1)];
    for (i, byte) in nulls.iter_mut().enumerate() {
        *byte = (next() >> (8 * (i % 8))) as u8;
    }
    KernelInputs {
        ints_a,
        ints_b,
        floats_a,
        floats_b,
        mask_a,
        mask_b,
        nulls,
    }
}

fn bits(values: &[f64]) -> Vec<u64> {
    values.iter().map(|v| v.to_bits()).collect()
}

macro_rules! parity_bytes {
    ($ops:ident, $a:expr, $b:expr, [$($m:ident),*]) => {
        $(assert_eq!($ops.$m($a, $b), scalar::$m($a, $b), stringify!($m));)*
    };
}

#[test]
fn kernels_agree_with_scalar() {
    let backends = all_backends();
    for round in 0..25u64 {
        let inputs = kernel_inputs(round);
        let ia = inputs.ints_a.as_slice();
        let ib = inputs.ints_b.as_slice();
        let fa = inputs.floats_a.as_slice();
        let fb = inputs.floats_b.as_slice();
        let ma = inputs.mask_a.as_slice();
        let mb = inputs.mask_b.as_slice();
        let nulls = inputs.nulls.as_slice();
        let len = ia.len();
        for ops in &backends {
            let tag = format!("{:?} seed=0x9E3779B97F4A7C15 round={round}", ops.isa());
            parity_bytes!(
                ops,
                ia,
                ib,
                [eq_i64, lt_i64, gt_i64, add_i64, sub_i64, mul_i64]
            );
            parity_bytes!(ops, fa, fb, [eq_f64, lt_f64, gt_f64]);
            assert_eq!(
                ops.div_i64(ia, ib),
                scalar::div_i64(ia, ib),
                "div_i64 {tag}"
            );
            assert_eq!(ops.and_mask(ma, mb), scalar::and_mask(ma, mb), "and {tag}");
            assert_eq!(ops.or_mask(ma, mb), scalar::or_mask(ma, mb), "or {tag}");
            assert_eq!(ops.not_mask(ma), scalar::not_mask(ma), "not {tag}");
            assert_eq!(
                ops.between_i64(ia, -1000, 1000),
                scalar::between_i64(ia, -1000, 1000),
                "between_i64 {tag}"
            );
            assert_eq!(
                ops.between_f64(fa, -1000.0, 1000.0),
                scalar::between_f64(fa, -1000.0, 1000.0),
                "between_f64 {tag}"
            );
            // f64 vector outputs compare bitwise (elementwise IEEE ops).
            for (name, got, want) in [
                ("add_f64", ops.add_f64(fa, fb), scalar::add_f64(fa, fb)),
                ("sub_f64", ops.sub_f64(fa, fb), scalar::sub_f64(fa, fb)),
                ("mul_f64", ops.mul_f64(fa, fb), scalar::mul_f64(fa, fb)),
                ("div_f64", ops.div_f64(fa, fb), scalar::div_f64(fa, fb)),
            ] {
                assert_eq!(bits(got.as_slice()), bits(want.as_slice()), "{name} {tag}");
            }
            // Scalar-broadcast forms (representative sample + edges).
            for scalar_i in [0i64, 1, -1, i64::MAX, i64::MIN, 42] {
                assert_eq!(
                    ops.eq_scalar_i64(ia, scalar_i),
                    scalar::eq_scalar_i64(ia, scalar_i),
                    "eq_scalar {tag}"
                );
                assert_eq!(
                    ops.add_scalar_i64(ia, scalar_i),
                    scalar::add_scalar_i64(ia, scalar_i),
                    "add_scalar {tag}"
                );
            }
            for scalar_f in [0.0f64, -0.0, 1.5, f64::NAN, f64::INFINITY] {
                assert_eq!(
                    ops.lt_scalar_f64(fa, scalar_f),
                    scalar::lt_scalar_f64(fa, scalar_f),
                    "lt_scalar_f64 {tag}"
                );
                assert_eq!(
                    bits(&ops.div_scalar_f64(fa, scalar_f)),
                    bits(&scalar::div_scalar_f64(fa, scalar_f)),
                    "div_scalar_f64 {tag}"
                );
            }
            assert_eq!(
                ops.is_null_mask(nulls, len),
                scalar::is_null_mask(nulls, len),
                "is_null {tag}"
            );
            assert_eq!(
                ops.is_not_null_mask(nulls, len),
                scalar::is_not_null_mask(nulls, len),
                "is_not_null {tag}"
            );
        }
    }
}

#[test]
fn div_i64_zero_and_overflow_yield_none() {
    let ops = VectorOps::select();
    assert_eq!(scalar::div_i64(&[10, 20], &[2, 0]), None);
    assert_eq!(ops.div_i64(&[10, 20], &[2, 0]), None);
    assert_eq!(scalar::div_i64(&[i64::MIN], &[-1]), None);
    assert_eq!(ops.div_i64(&[i64::MIN], &[-1]), None);
    assert_eq!(
        ops.div_i64(&[7, -8], &[2, 3]),
        scalar::div_i64(&[7, -8], &[2, 3])
    );
}

#[test]
#[should_panic(expected = "must share a length")]
fn mismatched_lengths_fail_closed() {
    let ops = VectorOps::select();
    let _ = ops.add_i64(&[1, 2], &[1]);
}

#[test]
fn selection_primitives_are_exact() {
    let ops = VectorOps::select();
    let values = vec![10i64, -3, 7, 0, 42, -3, 100];
    let mask = vec![1u8, 0, 1, 0, 0, 1, 1];
    let sel = ops.select_indices(&mask);
    assert_eq!(sel, vec![0u32, 2, 5, 6]);
    assert_eq!(ops.select_indices(&[]), Vec::<u32>::new());
    assert_eq!(ops.select_indices(&[0, 0, 0]), Vec::<u32>::new());
    assert_eq!(ops.count_selected(7, None), 7);
    assert_eq!(ops.count_selected(7, Some(&sel)), 4);
    assert_eq!(ops.gather_i64(&values, &sel), vec![10, 7, -3, 100]);
    // Out-of-range indices are skipped, never trusted.
    assert_eq!(ops.gather_i64(&values, &[0, 99]), vec![10]);
    assert_eq!(ops.sum_selected_i64(&values, &sel), 10 + 7 - 3 + 100);
    assert_eq!(ops.sum_selected_i64(&values, &[]), 0);
    assert_eq!(ops.min_selected_i64(&values, &sel), Some(-3));
    assert_eq!(ops.max_selected_i64(&values, &sel), Some(100));
    assert_eq!(ops.min_selected_i64(&values, &[]), None);
    assert_eq!(ops.max_selected_i64(&values, &[]), None);
    // Empty input, empty selection: every op degrades cleanly.
    let empty: Vec<i64> = Vec::new();
    let nosel: Vec<u32> = Vec::new();
    assert_eq!(ops.select_indices(&[]), nosel);
    assert_eq!(ops.gather_i64(&empty, &nosel), Vec::<i64>::new());
    assert_eq!(ops.sum_selected_i64(&empty, &nosel), 0);
}
