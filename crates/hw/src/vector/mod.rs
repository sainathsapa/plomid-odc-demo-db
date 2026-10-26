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
// ISA-dispatched vector reductions. Queries call `VectorOps` only;
// the active backend is selected once at startup from runtime CPU
// features, with the portable scalar path as the universal fallback.
//
// BIT-IDENTITY CONTRACT (why f64 is lane-canonical):
// wrapping i64 addition and i64 min/max are associative and commutative,
// so any lane layout yields identical bits. f64 addition is NOT: lane
// order changes rounding. Every backend — including scalar — therefore
// implements the same canonical 4-lane blocked reduction
// (`VectorIsa::F64_LANES`): lane l accumulates elements
// `4k + l`, the scalar tail folds into lanes in index order, and the
// final fold is `((a0 + a1) + a2) + a3`. No backend may use a wider or
// narrower lane count for f64, however tempting the register width.
//
// Exception: `distance` (f32 dot / L2) cannot be bit-identical across
// widths — `f32` accumulation order changes rounding. That module carries
// its own tolerance contract (relative `1e-4`, absolute floor `1e-5`)
// instead of bit-identity.

pub mod scalar;

pub mod distance;

#[cfg(target_arch = "aarch64")]
pub mod aarch64_neon;
#[cfg(target_arch = "x86_64")]
pub mod x86_avx2;
#[cfg(target_arch = "x86_64")]
pub mod x86_avx512;

use crate::profile::{Architecture, CpuFeatures, VectorIsa};

/// ISA-dispatched reductions over contiguous slices.
///
/// All methods are pure over their input slice: same input, same output
/// bits, on every ISA. Empty-slice rules: sums are zero (`0` / `0.0`),
/// min/max are `None`.
pub struct VectorOps {
    isa: VectorIsa,
}

impl VectorOps {
    /// Selects the widest backend the host CPU supports. Never fails:
    /// scalar always qualifies.
    #[must_use]
    pub fn select() -> Self {
        Self {
            isa: VectorIsa::select(&CpuFeatures::detect(), Architecture::host()),
        }
    }

    /// Forces one backend (tests/benchmarks only — production uses
    /// [`Self::select`]).
    #[must_use]
    pub fn force(isa: VectorIsa) -> Self {
        Self { isa }
    }

    /// Active backend.
    #[must_use]
    pub fn isa(&self) -> VectorIsa {
        self.isa
    }

    /// Wrapping i64 sum. Bit-identical on every backend (modular
    /// arithmetic admits no rounding).
    #[must_use]
    pub fn sum_i64(&self, values: &[i64]) -> i64 {
        match self.isa {
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx512 if CpuFeatures::detect().avx512 => {
                // SAFETY: selected only after the runtime avx512f probe;
                // unaligned loads; no memory preconditions.
                unsafe { x86_avx512::sum_i64(values) }
            }
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx2 if CpuFeatures::detect().avx2 => {
                // SAFETY: as above, for avx2.
                unsafe { x86_avx2::sum_i64(values) }
            }
            #[cfg(target_arch = "aarch64")]
            VectorIsa::Neon if CpuFeatures::detect().neon => {
                // SAFETY: as above; NEON/ASIMD is mandatory on aarch64
                // and additionally probed.
                unsafe { aarch64_neon::sum_i64(values) }
            }
            _ => scalar::sum_i64(values),
        }
    }

    /// f64 sum under the canonical 4-lane order. Bit-identical on every
    /// backend by construction (see module docs). NaN results canonicalize
    /// to quiet `f64::NAN`: NaN sign/payload propagation through SIMD
    /// reduction trees is implementation-defined (AVX2 yields `-NaN` where
    /// scalar yields `+NaN` for inf-mixed inputs), and SQL only observes
    /// NaN-ness, never the sign.
    #[must_use]
    pub fn sum_f64(&self, values: &[f64]) -> f64 {
        let sum = match self.isa {
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx512 if CpuFeatures::detect().avx512 => {
                // SAFETY: as in `sum_i64`.
                unsafe { x86_avx512::sum_f64(values) }
            }
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx2 if CpuFeatures::detect().avx2 => {
                // SAFETY: as in `sum_i64`.
                unsafe { x86_avx2::sum_f64(values) }
            }
            #[cfg(target_arch = "aarch64")]
            VectorIsa::Neon if CpuFeatures::detect().neon => {
                // SAFETY: as in `sum_i64`.
                unsafe { aarch64_neon::sum_f64(values) }
            }
            _ => scalar::sum_f64(values),
        };
        if sum.is_nan() {
            f64::NAN
        } else {
            sum
        }
    }

    /// i64 minimum (`None` when empty). Order-independent: exact on
    /// every backend.
    #[must_use]
    pub fn min_i64(&self, values: &[i64]) -> Option<i64> {
        match self.isa {
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx512 if CpuFeatures::detect().avx512 => {
                // SAFETY: as in `sum_i64`.
                unsafe { x86_avx512::min_i64(values) }
            }
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx2 if CpuFeatures::detect().avx2 => {
                // SAFETY: as in `sum_i64`.
                unsafe { x86_avx2::min_i64(values) }
            }
            #[cfg(target_arch = "aarch64")]
            VectorIsa::Neon if CpuFeatures::detect().neon => {
                // SAFETY: as in `sum_i64`.
                unsafe { aarch64_neon::min_i64(values) }
            }
            _ => scalar::min_i64(values),
        }
    }

    /// i64 maximum (`None` when empty). Order-independent: exact on
    /// every backend.
    #[must_use]
    pub fn max_i64(&self, values: &[i64]) -> Option<i64> {
        match self.isa {
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx512 if CpuFeatures::detect().avx512 => {
                // SAFETY: as in `sum_i64`.
                unsafe { x86_avx512::max_i64(values) }
            }
            #[cfg(target_arch = "x86_64")]
            VectorIsa::Avx2 if CpuFeatures::detect().avx2 => {
                // SAFETY: as in `sum_i64`.
                unsafe { x86_avx2::max_i64(values) }
            }
            #[cfg(target_arch = "aarch64")]
            VectorIsa::Neon if CpuFeatures::detect().neon => {
                // SAFETY: as in `sum_i64`.
                unsafe { aarch64_neon::max_i64(values) }
            }
            _ => scalar::max_i64(values),
        }
    }
}

/// `lanes` holds the four lane accumulators after the vectorized
/// prefix; `tail` holds the remaining elements in index order. Every
/// backend calls this (or replicates it exactly): tail element `j`
/// folds into lane `j % 4`, then lanes fold left `((a0+a1)+a2)+a3`.
/// The tail/lane assignment is part of the canonical order —
/// backends must not "optimize" it.
#[must_use]
pub fn finish_f64_lanes(mut lanes: [f64; 4], tail: &[f64]) -> f64 {
    for (j, value) in tail.iter().enumerate() {
        lanes[j % 4] += *value;
    }
    ((lanes[0] + lanes[1]) + lanes[2]) + lanes[3]
}

// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Kernel dispatch: one selection point per operation.
//
// Every method below resolves to the active backend selected by
// `VectorOps::select` (re-probing the CPU at dispatch, so a forced
// backend on incapable hardware still falls back safely). Backends
// that an ISA cannot implement (64-bit integer multiply on
// NEON/AVX2, AVX512DQ-gated multiply, checked integer divide
// anywhere, null-bitmap expansion) route explicitly to the shared
// scalar loop — documented at each site, never silently scalar, and
// covered by the same parity tests.
//
// SAFETY (all unsafe arms): each executes only after its runtime
// CPU-feature probe succeeded; loads are unaligned-tolerant
// register-local copies with no other memory preconditions.
// ---------------------------------------------------------------------------

/// Stamps one dispatched kernel: `$doc` becomes the method docs.
macro_rules! kernel {
    ($doc:expr, $name:ident ( $($arg:ident : $ty:ty),* ) -> $ret:ty) => {
        #[doc = $doc]
        #[must_use]
        pub fn $name(&self, $($arg: $ty),*) -> $ret {
            match self.isa {
                #[cfg(target_arch = "x86_64")]
                VectorIsa::Avx512 if CpuFeatures::detect().avx512 => unsafe {
                    self::x86_avx512::$name($($arg),*)
                },
                #[cfg(target_arch = "x86_64")]
                VectorIsa::Avx2 if CpuFeatures::detect().avx2 => unsafe {
                    self::x86_avx2::$name($($arg),*)
                },
                #[cfg(target_arch = "aarch64")]
                VectorIsa::Neon if CpuFeatures::detect().neon => unsafe {
                    self::aarch64_neon::$name($($arg),*)
                },
                _ => self::scalar::$name($($arg),*),
            }
        }
    };
}

impl VectorOps {
    kernel!(
        "Elementwise i64 `==` (0/1 bytes). Wrapping-free; exact everywhere.",
        eq_i64(a: &[i64], b: &[i64]) -> Vec<u8>
    );
    kernel!(
        "Elementwise i64 `==` against a broadcast scalar.",
        eq_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8>
    );
    kernel!(
        "Elementwise i64 `<` (0/1 bytes).",
        lt_i64(a: &[i64], b: &[i64]) -> Vec<u8>
    );
    kernel!(
        "Elementwise i64 `<` against a broadcast scalar.",
        lt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8>
    );
    kernel!(
        "Elementwise i64 `>` (0/1 bytes).",
        gt_i64(a: &[i64], b: &[i64]) -> Vec<u8>
    );
    kernel!(
        "Elementwise i64 `>` against a broadcast scalar.",
        gt_scalar_i64(a: &[i64], scalar: i64) -> Vec<u8>
    );
    kernel!(
        "Inclusive i64 `BETWEEN` over scalar bounds (SQL semantics).",
        between_i64(values: &[i64], lo: i64, hi: i64) -> Vec<u8>
    );
    kernel!(
        "Wrapping elementwise i64 add.",
        add_i64(a: &[i64], b: &[i64]) -> Vec<i64>
    );
    kernel!(
        "Wrapping i64 add of a broadcast scalar.",
        add_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64>
    );
    kernel!(
        "Wrapping elementwise i64 subtract.",
        sub_i64(a: &[i64], b: &[i64]) -> Vec<i64>
    );
    kernel!(
        "Wrapping i64 subtract of a broadcast scalar.",
        sub_scalar_i64(a: &[i64], scalar: i64) -> Vec<i64>
    );
    kernel!(
        "Elementwise f64 `==` (plain operators: NaN compares false).",
        eq_f64(a: &[f64], b: &[f64]) -> Vec<u8>
    );
    kernel!(
        "Elementwise f64 `==` against a broadcast scalar.",
        eq_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8>
    );
    kernel!(
        "Elementwise f64 `<` (NaN compares false).",
        lt_f64(a: &[f64], b: &[f64]) -> Vec<u8>
    );
    kernel!(
        "Elementwise f64 `<` against a broadcast scalar.",
        lt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8>
    );
    kernel!(
        "Elementwise f64 `>` (NaN compares false).",
        gt_f64(a: &[f64], b: &[f64]) -> Vec<u8>
    );
    kernel!(
        "Elementwise f64 `>` against a broadcast scalar.",
        gt_scalar_f64(a: &[f64], scalar: f64) -> Vec<u8>
    );
    kernel!(
        "Inclusive f64 `BETWEEN` (NaN never matches).",
        between_f64(values: &[f64], lo: f64, hi: f64) -> Vec<u8>
    );
    kernel!(
        "Elementwise f64 add (per-element order: exact everywhere).",
        add_f64(a: &[f64], b: &[f64]) -> Vec<f64>
    );
    kernel!(
        "f64 add of a broadcast scalar.",
        add_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64>
    );
    kernel!(
        "Elementwise f64 subtract.",
        sub_f64(a: &[f64], b: &[f64]) -> Vec<f64>
    );
    kernel!(
        "f64 subtract of a broadcast scalar.",
        sub_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64>
    );
    kernel!(
        "Elementwise f64 multiply.",
        mul_f64(a: &[f64], b: &[f64]) -> Vec<f64>
    );
    kernel!(
        "f64 multiply by a broadcast scalar.",
        mul_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64>
    );
    kernel!(
        "Elementwise f64 divide (IEEE: zero divisors yield inf/NaN).",
        div_f64(a: &[f64], b: &[f64]) -> Vec<f64>
    );
    kernel!(
        "f64 divide by a broadcast scalar.",
        div_scalar_f64(a: &[f64], scalar: f64) -> Vec<f64>
    );
    kernel!(
        "Elementwise mask AND.",
        and_mask(a: &[u8], b: &[u8]) -> Vec<u8>
    );
    kernel!(
        "Elementwise mask OR.",
        or_mask(a: &[u8], b: &[u8]) -> Vec<u8>
    );
    kernel!(
        "Elementwise mask NOT (nonzero becomes 0).",
        not_mask(a: &[u8]) -> Vec<u8>
    );

    /// Wrapping elementwise i64 multiply. No target ISA in the probed
    /// set has a 64-bit integer multiply instruction reachable here
    /// (NEON/AVX2 lack it; AVX512DQ is not probed), so every backend —
    /// including a forced SIMD one — runs the shared scalar loop.
    /// Still dispatched through `VectorOps` so a future DQ-gated path
    /// lands in one place and every operator keeps one call shape.
    #[must_use]
    pub fn mul_i64(&self, a: &[i64], b: &[i64]) -> Vec<i64> {
        self::scalar::mul_i64(a, b)
    }

    /// Wrapping i64 multiply by a broadcast scalar (same ISA-gap note
    /// as [`Self::mul_i64`]).
    #[must_use]
    pub fn mul_scalar_i64(&self, a: &[i64], scalar: i64) -> Vec<i64> {
        self::scalar::mul_scalar_i64(a, scalar)
    }

    /// Checked elementwise i64 divide: `None` when any divisor is zero
    /// or `MIN / -1` (SQL raises "division by zero"; the kernel never
    /// invents a value). No target ISA has an integer divide
    /// instruction, so this is scalar-shared on every backend.
    #[must_use]
    pub fn div_i64(&self, a: &[i64], b: &[i64]) -> Option<Vec<i64>> {
        self::scalar::div_i64(a, b)
    }

    /// Rows marked NULL in a bitmap, expanded to 0/1 bytes. Bit work
    /// is scalar-shared (memory-bound either way).
    #[must_use]
    pub fn is_null_mask(&self, nulls: &[u8], len: usize) -> Vec<u8> {
        self::scalar::is_null_mask(nulls, len)
    }

    /// Rows NOT marked NULL, expanded to 0/1 bytes (same note as
    /// [`Self::is_null_mask`]).
    #[must_use]
    pub fn is_not_null_mask(&self, nulls: &[u8], len: usize) -> Vec<u8> {
        self::scalar::is_not_null_mask(nulls, len)
    }

    /// Compresses a 0/1 mask to surviving row indices, ascending.
    /// No target ISA has a byte compress-store in the probed set
    /// (AVX512VBMI unprobed), so this is scalar-shared on every
    /// backend; the win is structural (downstream work scales with
    /// matches, not rows), not per-element.
    #[must_use]
    pub fn select_indices(&self, mask: &[u8]) -> Vec<u32> {
        self::scalar::select_indices(mask)
    }

    /// Counts selected rows (no gather needed).
    #[must_use]
    pub fn count_selected(&self, len: usize, sel: Option<&[u32]>) -> u64 {
        self::scalar::count_selected(len, sel)
    }

    /// Gathers i64 values through indices (bounds-checked).
    #[must_use]
    pub fn gather_i64(&self, values: &[i64], sel: &[u32]) -> Vec<i64> {
        self::scalar::gather_i64(values, sel)
    }

    /// Wrapping sum over selected values only.
    #[must_use]
    pub fn sum_selected_i64(&self, values: &[i64], sel: &[u32]) -> i64 {
        self::scalar::sum_selected_i64(values, sel)
    }

    /// Minimum over selected values.
    #[must_use]
    pub fn min_selected_i64(&self, values: &[i64], sel: &[u32]) -> Option<i64> {
        self::scalar::min_selected_i64(values, sel)
    }

    /// Maximum over selected values.
    #[must_use]
    pub fn max_selected_i64(&self, values: &[i64], sel: &[u32]) -> Option<i64> {
        self::scalar::max_selected_i64(values, sel)
    }
}
