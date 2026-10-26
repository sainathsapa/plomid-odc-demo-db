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
//! CPU backend selection with safe scalar fallback.
//!
//! Tiers execute real kernels: AVX2 on x86_64 and NEON on aarch64 run in
//! `plomid-hw` (the sanctioned `unsafe` owner — this crate forbids
//! `unsafe_code`, so intrinsics live there behind runtime feature probes).
//! AVX-512/SVE tiers execute the AVX2/NEON-width kernels on their hosts
//! (correct and faster than scalar; documented, not silent) until
//! dedicated 512-bit/scalable kernels land.
//!
//! Numerical contract: SIMD tiers use `f32` lane accumulation while the
//! scalar oracle uses `f64`, so tiers agree within floating-point
//! tolerance — relative `1e-4`, absolute floor `1e-5` — not bit-identically.
//! Ordering-sensitive paths (Top-K) keep deterministic `(distance,
//! row_id)` tie-breaking, so tolerance never flips stable orderings
//! outside genuinely tied distances.
//!
//! Selection order:
//! 1. `PLOMID_VECTOR_SIMD` env override (`scalar|off`, `avx2`, `avx512`,
//!    `neon`, `sve`; case-insensitive, parsed once at first use — restart
//!    to change it). A tier unavailable for the current `cfg(target_arch)`
//!    falls back to [`CpuBackend::Scalar`].
//! 2. Compile-target-gated auto-detection via `std::arch` feature macros
//!    (x86_64: AVX-512 → AVX2; aarch64: SVE → NEON).
//! 3. [`CpuBackend::Scalar`].
//!
//! Host features are probed once per process: hot-path dispatch pays one
//! atomic load per call, never a repeated probe or environment read.

use super::Result;

/// CPU dispatch tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CpuBackend {
    /// Portable scalar fallback. Always available.
    Scalar,
    /// x86_64 AVX2 distance kernels (`plomid-hw`).
    Avx2,
    /// x86_64 AVX-512 detected; executes AVX2-width kernels.
    Avx512,
    /// aarch64 NEON distance kernels (`plomid-hw`).
    Neon,
    /// aarch64 SVE detected; executes NEON-width kernels.
    Sve,
}

impl CpuBackend {
    /// Stable short name for logging / `EXPLAIN` output.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Scalar => "scalar",
            Self::Avx2 => "avx2",
            Self::Avx512 => "avx512",
            Self::Neon => "neon",
            Self::Sve => "sve",
        }
    }

    /// Whether this tier represents an accelerated path.
    #[must_use]
    pub fn is_accelerated(self) -> bool {
        !matches!(self, Self::Scalar)
    }

    /// Whether this tier can be selected on the current compile target.
    #[must_use]
    pub fn supported_here(self) -> bool {
        match self {
            Self::Scalar => true,
            Self::Avx2 | Self::Avx512 => cfg!(target_arch = "x86_64"),
            Self::Neon | Self::Sve => cfg!(target_arch = "aarch64"),
        }
    }
}

#[cfg(target_arch = "x86_64")]
fn auto_detect() -> CpuBackend {
    if std::arch::is_x86_feature_detected!("avx512f") {
        CpuBackend::Avx512
    } else if std::arch::is_x86_feature_detected!("avx2") {
        CpuBackend::Avx2
    } else {
        CpuBackend::Scalar
    }
}

#[cfg(target_arch = "aarch64")]
fn auto_detect() -> CpuBackend {
    if std::arch::is_aarch64_feature_detected!("sve") {
        CpuBackend::Sve
    } else if std::arch::is_aarch64_feature_detected!("neon") {
        CpuBackend::Neon
    } else {
        CpuBackend::Scalar
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
fn auto_detect() -> CpuBackend {
    CpuBackend::Scalar
}

/// Parse a dispatch-tier name (case-insensitive, `PLOMID_VECTOR_SIMD` values).
/// Unknown names yield `None`; known-but-unsupported tiers fall back to scalar.
pub fn from_name(name: &str) -> Option<CpuBackend> {
    let tier = match name {
        "scalar" | "off" | "none" | "0" => CpuBackend::Scalar,
        "avx2" => CpuBackend::Avx2,
        "avx512" => CpuBackend::Avx512,
        "neon" => CpuBackend::Neon,
        "sve" => CpuBackend::Sve,
        _ => return None,
    };
    Some(if tier.supported_here() {
        tier
    } else {
        CpuBackend::Scalar
    })
}

/// Host features, probed once: CPU capabilities cannot change under a
/// running process, so every dispatch pays one atomic load — never a
/// repeated system probe on the hot path.
fn host_features() -> &'static plomid_hw::profile::CpuFeatures {
    static HOST: std::sync::OnceLock<plomid_hw::profile::CpuFeatures> = std::sync::OnceLock::new();
    HOST.get_or_init(plomid_hw::profile::CpuFeatures::detect)
}

/// `PLOMID_VECTOR_SIMD` override, parsed once at first use (restart the
/// process to change it — hot-path dispatch never re-reads the environment).
fn env_override() -> Option<CpuBackend> {
    static OVERRIDE: std::sync::OnceLock<Option<CpuBackend>> = std::sync::OnceLock::new();
    *OVERRIDE.get_or_init(|| {
        std::env::var("PLOMID_VECTOR_SIMD")
            .ok()
            .and_then(|raw| from_name(raw.to_ascii_lowercase().trim()))
    })
}

/// Detect the dispatch tier: parsed env override first, then target-gated
/// auto-detection, else scalar.
#[must_use]
pub fn detect_backend() -> CpuBackend {
    if let Some(tier) = env_override() {
        return tier;
    }
    auto_detect()
}

/// Active backend for this call (env override parsed once; auto-detection
/// re-evaluated per call from cheap target-gated probes).
#[must_use]
pub fn active_backend() -> CpuBackend {
    detect_backend()
}

/// Resolve the requested tier to the tier that will actually execute.
///
/// A requested tier only runs when the host probes positive for it;
/// otherwise execution (and the reported tier) degrades: AVX-512 to AVX2,
/// anything else to scalar. The reported tier therefore always describes
/// the executed kernel — never an aspiration.
fn effective_tier(tier: CpuBackend) -> CpuBackend {
    let host = host_features();
    match tier {
        CpuBackend::Scalar => CpuBackend::Scalar,
        CpuBackend::Avx2 if host.avx2 => CpuBackend::Avx2,
        // AVX-512 hosts execute the AVX2-width kernels until dedicated
        // 512-bit kernels land;still faster than scalar, and labelled so.
        CpuBackend::Avx512 if host.avx512 => CpuBackend::Avx512,
        CpuBackend::Avx512 if host.avx2 => CpuBackend::Avx2,
        CpuBackend::Neon if host.neon => CpuBackend::Neon,
        // SVE hosts execute the NEON-width kernels (ASIMD is mandatory on
        // aarch64); labelled SVE so dispatch intent stays visible.
        CpuBackend::Sve if host.neon => CpuBackend::Sve,
        _ => CpuBackend::Scalar,
    }
}

/// Inner product on the active tier; returns the tier used.
///
/// Lengths are validated first (scalar oracle errors); the hardware
/// kernels then run on the validated equal-length slices.
pub fn dot(a: &[f32], b: &[f32]) -> (CpuBackend, Result<f32>) {
    let tier = effective_tier(active_backend());
    if super::scalar::dot_f32(a, b).is_err() {
        return (tier, super::scalar::dot_f32(a, b));
    }
    let value = match tier {
        CpuBackend::Scalar => return (tier, super::scalar::dot_f32(a, b)),
        _ => plomid_hw::vector::distance::dot_f32(a, b),
    };
    (tier, Ok(value))
}

/// Squared Euclidean distance on the active tier; returns the tier used.
pub fn l2_squared(a: &[f32], b: &[f32]) -> (CpuBackend, Result<f32>) {
    let tier = effective_tier(active_backend());
    if super::scalar::l2_squared_f32(a, b).is_err() {
        return (tier, super::scalar::l2_squared_f32(a, b));
    }
    let value = match tier {
        CpuBackend::Scalar => return (tier, super::scalar::l2_squared_f32(a, b)),
        _ => plomid_hw::vector::distance::l2_squared_f32(a, b),
    };
    (tier, Ok(value))
}

/// Euclidean distance on the active tier; returns the tier used.
pub fn l2_distance(a: &[f32], b: &[f32]) -> (CpuBackend, Result<f32>) {
    let tier = effective_tier(active_backend());
    if super::scalar::l2_distance_f32(a, b).is_err() {
        return (tier, super::scalar::l2_distance_f32(a, b));
    }
    // sqrt in `f64`, exactly as the scalar oracle; only the squared term
    // comes from the tier kernel.
    let (_, squared) = l2_squared(a, b);
    match squared {
        Ok(s) => (tier, Ok((s as f64).sqrt() as f32)),
        Err(e) => (tier, Err(e)),
    }
}

/// Cosine distance (`None` on zero norm); returns the tier used.
pub fn cosine_distance(a: &[f32], b: &[f32]) -> (CpuBackend, Result<Option<f32>>) {
    let tier = effective_tier(active_backend());
    if super::scalar::cosine_distance_f32(a, b).is_err() {
        return (tier, super::scalar::cosine_distance_f32(a, b));
    }
    // Tier dot products with scalar-shape normalization: norms and the
    // final division match the oracle; only the three inner products run
    // on the tier kernels.
    let (_, d) = dot(a, b);
    let (_, na) = dot(a, a);
    let (_, nb) = dot(b, b);
    let (d, na, nb) = match (d, na, nb) {
        (Ok(d), Ok(x), Ok(y)) => (d, x, y),
        _ => return (tier, super::scalar::cosine_distance_f32(a, b)),
    };
    let denom = (f64::from(na) * f64::from(nb)).sqrt();
    if denom == 0.0 {
        return (tier, Ok(None));
    }
    (tier, Ok(Some(1.0 - (f64::from(d) / denom) as f32)))
}

/// Cosine similarity on the active tier; returns the tier used.
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> (CpuBackend, Result<Option<f32>>) {
    let tier = effective_tier(active_backend());
    if super::scalar::cosine_similarity_f32(a, b).is_err() {
        return (tier, super::scalar::cosine_similarity_f32(a, b));
    }
    let (_, d) = dot(a, b);
    let (_, na) = dot(a, a);
    let (_, nb) = dot(b, b);
    let (d, na, nb) = match (d, na, nb) {
        (Ok(d), Ok(x), Ok(y)) => (d, x, y),
        _ => return (tier, super::scalar::cosine_similarity_f32(a, b)),
    };
    let denom = (f64::from(na) * f64::from(nb)).sqrt();
    if denom == 0.0 {
        return (tier, Ok(None));
    }
    (tier, Ok(Some((f64::from(d) / denom) as f32)))
}
