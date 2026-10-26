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
//
// Hardware-adaptive runtime: host detection plus ISA-dispatched vector
// reductions. Queries never touch ISA specifics; they call `VectorOps`
// (runtime-selected) or read sizing budgets derived from
// `HardwareProfile`. Every SIMD backend is bit-identical to the scalar
// fallback for the implemented ops (see `vector::` docs for the one
// place this constrains the algorithm: f64 lane order).
pub mod profile;
pub mod runtime;
pub mod vector;

pub use profile::{Architecture, CpuFeatures, HardwareProfile, StorageKind, VectorIsa};
pub use runtime::{ProfileSource, RuntimeProfile, RuntimeTier, PROFILE_ENV_VAR, RECHECK_INTERVAL};
pub use vector::VectorOps;
