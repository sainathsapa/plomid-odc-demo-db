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
//! Centralized metric/type capability matrix (Final Gap A).
//!
//! Every `(kind, element, metric, index)` combination classifies as exactly
//! one of [`Capability::Supported`], [`Capability::SupportedWithConversion`],
//! or [`Capability::Unsupported`]. Support never emerges accidentally from
//! scattered `match` statements: SQL binding, index creation, the planner,
//! execution, GPU dispatch, docs, and error messages all consult this module.
//!
//! The matrix mirrors the per-type predicates on [`VectorType`]
//! (`supports_exact`, `supports_hnsw`, `supports_ivf`, `supports_pq`,
//! `supports_sq`) plus the index-owned realities (sparse inverted and binary
//! indexes only serve their own kinds). `Supported` means the **index can
//! serve acceleration** for the triple, not merely that the query is
//! answered (every query is answered correctly by the exact scan even when
//! its index entry is [`Capability::Unsupported`]).
//! `SupportedWithConversion` marks combinations
//! that are correct only after an explicit, documented promotion (e.g. INT8 lanes
//! promoted to float codebooks for IVF-PQ); callers must perform that conversion
//! rather than silently reinterpreting bytes.
//!
//! Naming honesty (v0.1.0): the SQL method names `ivf_sq` and `ivf_pq` are
//! compatibility labels. Their implementations execute a **global quantized
//! scan** (SQ8 codes / PQ ADC) — there is no coarse quantizer, no posting
//! lists and no `nprobe` in those paths. `ivf_flat` is the only method that
//! actually partitions (k-means centroids + posting lists). The matrix below
//! classifies what executes, never what a name suggests. All dense ANN
//! methods (`hnsw`, `ivf_flat`, `ivf_sq`, `ivf_pq`, `diskann`) execute the
//! `Dense` kind only.

use crate::index::IndexKind;
use crate::r#type::{VectorElementType, VectorKind, VectorMetric};
use crate::{Result, VectorError};

/// Support level for one `(kind, element, metric, index)` combination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    /// Natively supported with no conversion.
    Supported,
    /// Supported after an explicit, documented value promotion.
    SupportedWithConversion,
    /// Not supported; index creation/planning must reject it.
    Unsupported,
}

impl Capability {
    /// `true` for [`Capability::Supported`].
    #[must_use]
    pub const fn is_supported(self) -> bool {
        matches!(self, Self::Supported)
    }

    /// `true` when usable (natively or after documented conversion).
    #[must_use]
    pub const fn is_usable(self) -> bool {
        matches!(self, Self::Supported | Self::SupportedWithConversion)
    }

    /// Stable short name for diagnostics and docs.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Supported => "SUPPORTED",
            Self::SupportedWithConversion => "SUPPORTED_WITH_CONVERSION",
            Self::Unsupported => "UNSUPPORTED",
        }
    }
}

/// True for dense (non-binary) numeric lanes served by graph/IVF/quantized
/// indexes. `Multi` values keep their exact constituent kernel; sparse and
/// binary kinds have their own indexes.
const fn ann_numeric(kind: VectorKind, element: VectorElementType, metric: VectorMetric) -> bool {
    matches!(kind, VectorKind::Dense)
        && matches!(
            metric,
            VectorMetric::L2 | VectorMetric::Cosine | VectorMetric::Dot
        )
        && !matches!(element, VectorElementType::Binary)
}

/// Classify one `(kind, element, metric, index)` combination.
#[must_use]
pub const fn capability(
    kind: VectorKind,
    element: VectorElementType,
    metric: VectorMetric,
    index: IndexKind,
) -> Capability {
    match index {
        IndexKind::Flat => {
            if matches!(metric, VectorMetric::Hamming | VectorMetric::Jaccard) {
                if matches!(kind, VectorKind::Binary)
                    && matches!(element, VectorElementType::Binary)
                {
                    Capability::Supported
                } else {
                    Capability::Unsupported
                }
            } else if matches!(
                (kind, element),
                (VectorKind::Binary, _) | (_, VectorElementType::Binary)
            ) {
                Capability::Unsupported
            } else {
                Capability::Supported
            }
        }
        IndexKind::Hnsw | IndexKind::IvfFlat | IndexKind::DiskAnn => {
            if ann_numeric(kind, element, metric) {
                Capability::Supported
            } else {
                Capability::Unsupported
            }
        }
        IndexKind::IvfSq => {
            match (kind, metric, element) {
                (
                    VectorKind::Dense,
                    VectorMetric::L2 | VectorMetric::Cosine | VectorMetric::Dot,
                    _,
                ) => match element {
                    VectorElementType::Float32
                    | VectorElementType::Float16
                    | VectorElementType::BFloat16
                    | VectorElementType::Int8
                    | VectorElementType::Uint8 => Capability::Supported,
                    // INT4 lanes quantize via documented float promotion.
                    VectorElementType::Int4 => Capability::SupportedWithConversion,
                    VectorElementType::Binary => Capability::Unsupported,
                },
                _ => Capability::Unsupported,
            }
        }
        IndexKind::IvfPq => {
            match (kind, metric, element) {
                (
                    VectorKind::Dense,
                    VectorMetric::L2 | VectorMetric::Cosine | VectorMetric::Dot,
                    _,
                ) => match element {
                    VectorElementType::Float32
                    | VectorElementType::Float16
                    | VectorElementType::BFloat16 => Capability::Supported,
                    // 8-bit integer lanes promote to float codebooks.
                    VectorElementType::Int8 | VectorElementType::Uint8 => {
                        Capability::SupportedWithConversion
                    }
                    VectorElementType::Int4 | VectorElementType::Binary => Capability::Unsupported,
                },
                _ => Capability::Unsupported,
            }
        }
        IndexKind::SparseInverted => match (kind, metric, element) {
            (VectorKind::Sparse, VectorMetric::L2, _)
            | (VectorKind::Sparse, VectorMetric::Cosine, _)
            | (VectorKind::Sparse, VectorMetric::Dot, _) => match element {
                VectorElementType::Binary => Capability::Unsupported,
                _ => Capability::Supported,
            },
            _ => Capability::Unsupported,
        },
        IndexKind::Binary => match (kind, element, metric) {
            // Hamming only: multi-index-hashing block equality carries no
            // Jaccard signal, so the binary index cannot serve Jaccard.
            // Jaccard queries stay on the exact scan (correct, unaccelerated);
            // classifying them `Supported` here would be a false capability.
            (VectorKind::Binary, VectorElementType::Binary, VectorMetric::Hamming) => {
                Capability::Supported
            }
            _ => Capability::Unsupported,
        },
    }
}

/// All index kinds usable (natively or with conversion) for `(kind, element, metric)`.
#[must_use]
pub fn supported_indexes_for(
    kind: VectorKind,
    element: VectorElementType,
    metric: VectorMetric,
) -> Vec<IndexKind> {
    const ALL: [IndexKind; 8] = [
        IndexKind::Flat,
        IndexKind::Hnsw,
        IndexKind::IvfFlat,
        IndexKind::IvfSq,
        IndexKind::IvfPq,
        IndexKind::DiskAnn,
        IndexKind::SparseInverted,
        IndexKind::Binary,
    ];
    ALL.into_iter()
        .filter(|i| capability(kind, element, metric, *i).is_usable())
        .collect()
}

/// Enforce the matrix, mapping the outcome to a typed [`VectorError`].
///
/// `Supported` passes; `SupportedWithConversion` passes (the caller owns the
/// documented promotion); `Unsupported` fails closed with `IndexUnsupported`.
pub fn check(
    kind: VectorKind,
    element: VectorElementType,
    metric: VectorMetric,
    index: IndexKind,
) -> Result<()> {
    match capability(kind, element, metric, index) {
        Capability::Supported | Capability::SupportedWithConversion => Ok(()),
        Capability::Unsupported => Err(VectorError::IndexUnsupported(format!(
            "{kind:?}/{element:?}/{metric:?} is {u} for {index:?}",
            u = Capability::Unsupported.name()
        ))),
    }
}
