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
//! PLOMID filter benchmarks: XOR filter build and probes.
//!
//! Run with: `cargo bench -p plomid-filters --bench filters`.
//!
//! The harness is deliberately dependency-free (no criterion): the report needs
//! operations/sec *and* p50/p95/p99 latencies, which are measured directly with
//! `Instant` over per-operation samples. This matches the plain-harness style
//! used by `crates/index/benches/art.rs` and
//! `crates/columnar/benches/flush_pipeline.rs`.
//!
//! Datasets come from a deterministic splitmix64 PRNG, so a run is reproducible
//! from its seed alone. Every number printed was produced by running this
//! benchmark, not estimated.

use plomid_filters::XorFilter;
use std::time::Instant;

/// Deterministic splitmix64 PRNG.
///
/// Written out here on purpose: a filter benchmark should not force a
/// random-number dependency into the crate graph.
struct Rng(u64);

impl Rng {
    /// Creates a generator with the given seed.
    const fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// Returns the next pseudo-random `u64`.
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a pseudo-random `u32` over the whole 32-bit space.
    fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Returns a pseudo-random index in `0..bound`.
    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() % bound as u64) as usize
    }
}

/// Latency of one operation in nanoseconds.
type Nanos = u64;

/// Returns the requested percentile from an unsorted latency sample.
///
/// Nearest-rank method: the first sample at or above the requested share of the
/// sorted sample.
fn percentile(samples: &mut [Nanos], pct: f64) -> Nanos {
    if samples.is_empty() {
        return 0;
    }
    samples.sort_unstable();
    let rank = ((samples.len() as f64) * pct).ceil() as usize;
    let index = rank.saturating_sub(1).min(samples.len() - 1);
    samples[index]
}

/// Prints one measured operation.
fn report(label: &str, ops: usize, elapsed_s: f64, samples: &mut [Nanos]) {
    let per_sec = if elapsed_s > 0.0 {
        ops as f64 / elapsed_s
    } else {
        0.0
    };
    let mean_us = if samples.is_empty() {
        0.0
    } else {
        samples.iter().sum::<Nanos>() as f64 / samples.len() as f64 / 1_000.0
    };
    println!(
        "  {label:<34} {ops:>8} ops {per_sec:>12.0} ops/s  mean {mean_us:>7.3} us  \
         p50 {:>7.3} us  p95 {:>7.3} us  p99 {:>7.3} us",
        percentile(samples, 0.50) as f64 / 1_000.0,
        percentile(samples, 0.95) as f64 / 1_000.0,
        percentile(samples, 0.99) as f64 / 1_000.0,
    );
}

/// Measures XOR filter construction and membership probes.
fn bench_xor(count: usize) {
    let mut rng = Rng::new(0x1234_5678);
    let values: Vec<u32> = (0..count).map(|_| rng.next_u32()).collect();

    let start = Instant::now();
    // Build keys as 4-byte little-endian slices: XorFilter works over byte
    // strings, so we encode each `u32` value as `[u8; 4]` in LE order.
    //
    // From ~77k values up, a random u32 dataset is expected to contain repeats
    // (birthday bound), and repeats are what used to make construction fail
    // here; the filter collapses them before peeling.
    let keys: Vec<[u8; 4]> = values.iter().map(|v| v.to_le_bytes()).collect();
    let filter = XorFilter::build(keys.iter().map(|key| &key[..]))
        .unwrap_or_else(|error| panic!("construction failed for {count} keys: {error}"));
    let build = start.elapsed().as_secs_f64();
    // size_bytes() does not exist on XorFilter; use the serialized image length
    // instead. The image includes the 40-byte footer, which is negligible at
    // these sizes.
    let image_len = filter.serialize().len();
    println!(
        "  {:<34} {count:>8} values {build:>10.4} s  {} bits/value",
        "build",
        image_len as u64 * 8 / count.max(1) as u64
    );

    let probes = count.min(100_000);
    let mut hit_samples = Vec::with_capacity(probes);
    let start = Instant::now();
    for _ in 0..probes {
        let value = values[rng.below(values.len())];
        let key = value.to_le_bytes();
        let op = Instant::now();
        let found = filter.contains(&key);
        hit_samples.push(op.elapsed().as_nanos() as Nanos);
        assert!(found, "no false negatives are allowed");
    }
    let elapsed = start.elapsed().as_secs_f64();
    report("xor contains (hit)", probes, elapsed, &mut hit_samples);

    // Misses come from a disjoint value space: the lowest 2^20 values that the
    // random probes do not cover are rare, so use the low range and count how
    // many probe as present (the measured false-positive rate).
    let mut miss_samples = Vec::with_capacity(probes);
    let mut positives = 0usize;
    let start = Instant::now();
    for index in 0..probes {
        let value = u32::try_from(index).expect("probe index fits a u32");
        let key = value.to_le_bytes();
        let op = Instant::now();
        let found = filter.contains(&key);
        miss_samples.push(op.elapsed().as_nanos() as Nanos);
        if found {
            positives += 1;
        }
    }
    let elapsed = start.elapsed().as_secs_f64();
    report("xor contains (miss)", probes, elapsed, &mut miss_samples);
    println!(
        "  {:<34} {positives} of {probes} absent probes reported present ({:.4}%)",
        "false-positive rate",
        positives as f64 * 100.0 / probes as f64
    );
}

fn main() {
    println!("PLOMID filter benchmarks");
    println!();
    println!("XOR filter");
    for count in [1_000usize, 10_000, 100_000] {
        bench_xor(count);
    }

    println!();
    println!("Benchmarks complete.");
}
