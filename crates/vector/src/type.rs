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
//! Vector types: kinds, elements, `VectorType`, typmod, SQL names, capabilities. Dimension is type identity.

use crate::error::{Result, VectorError};
use crate::validation::{MAX_DIMENSION, MIN_DIMENSION};

/// Storage/semantic kind of a vector column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum VectorKind {
    /// Contiguous dense payload.
    Dense = 1,
    /// Sorted `(index, value)` pairs; zeros omitted.
    Sparse = 2,
    /// Up to `max_vectors` dense constituents per row.
    Multi = 3,
    /// Packed bits; dimension counts bits.
    Binary = 4,
}

impl VectorKind {
    /// Stable wire discriminant.
    #[must_use]
    pub const fn code(self) -> u8 {
        self as u8
    }
    /// Decode a wire discriminant.
    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::Dense),
            2 => Ok(Self::Sparse),
            3 => Ok(Self::Multi),
            4 => Ok(Self::Binary),
            o => Err(VectorError::InvalidElementType(format!(
                "unknown kind code {o}"
            ))),
        }
    }
}

/// Logical scalar per coordinate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum VectorElementType {
    /// 4 bytes/lane.
    Float32 = 1,
    /// 2 bytes/lane.
    Float16 = 2,
    /// 2 bytes/lane.
    BFloat16 = 3,
    /// 1 byte/lane.
    Int8 = 4,
    /// 1 byte/lane.
    Uint8 = 5,
    /// Nibble, two per byte.
    Int4 = 6,
    /// One bit; only with [`VectorKind::Binary`].
    Binary = 7,
}

impl VectorElementType {
    /// Stable wire discriminant.
    #[must_use]
    pub const fn code(self) -> u8 {
        self as u8
    }
    /// Decode a wire discriminant.
    pub fn from_code(code: u8) -> Result<Self> {
        match code {
            1 => Ok(Self::Float32),
            2 => Ok(Self::Float16),
            3 => Ok(Self::BFloat16),
            4 => Ok(Self::Int8),
            5 => Ok(Self::Uint8),
            6 => Ok(Self::Int4),
            7 => Ok(Self::Binary),
            o => Err(VectorError::InvalidElementType(format!(
                "unknown element code {o}"
            ))),
        }
    }
    /// Parse SQL name (case-insensitive).
    pub fn parse_sql(name: &str) -> Result<Self> {
        match name.trim().to_ascii_uppercase().as_str() {
            "FLOAT32" | "FLOAT" | "REAL" => Ok(Self::Float32),
            "FLOAT16" => Ok(Self::Float16),
            "BFLOAT16" | "BF16" => Ok(Self::BFloat16),
            "INT8" => Ok(Self::Int8),
            "UINT8" => Ok(Self::Uint8),
            "INT4" => Ok(Self::Int4),
            "BINARY" | "BIT" => Ok(Self::Binary),
            o => Err(VectorError::InvalidElementType(format!(
                "unknown element '{o}'"
            ))),
        }
    }
    /// Canonical SQL name.
    #[must_use]
    pub const fn sql_name(self) -> &'static str {
        match self {
            Self::Float32 => "FLOAT32",
            Self::Float16 => "FLOAT16",
            Self::BFloat16 => "BFLOAT16",
            Self::Int8 => "INT8",
            Self::Uint8 => "UINT8",
            Self::Int4 => "INT4",
            Self::Binary => "BINARY",
        }
    }
    /// Dense-equivalent payload bytes for `dimension`, overflow-checked.
    pub fn payload_bytes(self, dimension: u32) -> Result<usize> {
        crate::validation::validate_dimension(dimension)?;
        let dim = dimension as usize;
        let of = || VectorError::ResourceLimit("payload size overflow".into());
        match self {
            Self::Float32 => dim.checked_mul(4).ok_or_else(of),
            Self::Float16 | Self::BFloat16 => dim.checked_mul(2).ok_or_else(of),
            Self::Int8 | Self::Uint8 => Ok(dim),
            Self::Int4 => Ok(dim.saturating_add(1) / 2),
            Self::Binary => Ok(dim.saturating_add(7) / 8),
        }
    }
}

/// Distance metric shared by search and indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VectorMetric {
    /// Euclidean distance.
    L2,
    /// `1 - cosine_similarity`.
    Cosine,
    /// Negative inner product (ascending = nearest).
    Dot,
    /// `popcount(a XOR b)`.
    Hamming,
    /// `1 - |a&b|/|a|b|`; empty union = distance 0.
    Jaccard,
}

impl VectorMetric {
    /// Parse metric or operator name (`<=>`, `<->`, `<#>`, `<~>`, `<%>`, …).
    pub fn parse(name: &str) -> Result<Self> {
        match name.trim().to_ascii_uppercase().as_str() {
            "L2" | "<->" | "EUCLIDEAN" => Ok(Self::L2),
            "COSINE" | "<=>" => Ok(Self::Cosine),
            "DOT" | "IP" | "<#>" => Ok(Self::Dot),
            "HAMMING" | "<~>" => Ok(Self::Hamming),
            "JACCARD" | "<%>" => Ok(Self::Jaccard),
            o => Err(VectorError::MetricMismatch(format!("unknown metric '{o}'"))),
        }
    }
    /// Canonical SQL operator.
    #[must_use]
    pub const fn operator(self) -> &'static str {
        match self {
            Self::L2 => "<->",
            Self::Cosine => "<=>",
            Self::Dot => "<#>",
            Self::Hamming => "<~>",
            Self::Jaccard => "<%>",
        }
    }
}

/// Declared column type. `dimension` counts coordinates (bits for Binary);
/// `max_vectors` is 1 unless kind is Multi.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct VectorType {
    /// Storage kind.
    pub kind: VectorKind,
    /// Logical element.
    pub element: VectorElementType,
    /// Coordinates (bits for Binary).
    pub dimension: u32,
    /// Constituent cap; 1 unless Multi.
    pub max_vectors: u32,
}

impl VectorType {
    /// Construct + validate (non-Multi).
    pub fn new(kind: VectorKind, element: VectorElementType, dimension: u32) -> Result<Self> {
        Self::with_max_vectors(kind, element, dimension, 1)
    }
    /// Construct + validate with explicit cap.
    pub fn with_max_vectors(k: VectorKind, e: VectorElementType, d: u32, m: u32) -> Result<Self> {
        let t = Self {
            kind: k,
            element: e,
            dimension: d,
            max_vectors: m,
        };
        t.validate()?;
        Ok(t)
    }
    /// Validate all invariants.
    pub fn validate(&self) -> Result<()> {
        if self.dimension < MIN_DIMENSION || self.dimension > MAX_DIMENSION {
            return Err(VectorError::InvalidDimension(format!(
                "dimension {} out of range 1..=65536",
                self.dimension
            )));
        }
        if self.max_vectors < 1 || self.max_vectors > MAX_DIMENSION {
            return Err(VectorError::InvalidDimension(format!(
                "max_vectors {} out of range 1..=65536",
                self.max_vectors
            )));
        }
        let bad = |m: &str| VectorError::UnsupportedCombination(m.into());
        if self.kind != VectorKind::Multi && self.max_vectors != 1 {
            return Err(bad("only multi-vectors allow max_vectors != 1"));
        }
        if self.element == VectorElementType::Binary && self.kind != VectorKind::Binary {
            return Err(bad(
                "BINARY element requires Binary kind; BINARY sparse/multi unsupported",
            ));
        }
        if self.kind == VectorKind::Binary && self.element != VectorElementType::Binary {
            return Err(bad("Binary kind requires BINARY element"));
        }
        Ok(())
    }
    /// Row payload bytes (sparse depends on nnz: use `validation::sparse_payload_bytes`).
    pub fn row_payload_bytes(&self) -> Result<usize> {
        match self.kind {
            VectorKind::Dense => self.element.payload_bytes(self.dimension),
            VectorKind::Binary => Ok(self.dimension.saturating_add(7) as usize / 8),
            VectorKind::Sparse => Err(VectorError::UnsupportedCombination(
                "sparse size depends on nnz".into(),
            )),
            VectorKind::Multi => {
                let per = self.element.payload_bytes(self.dimension)?;
                per.checked_mul(self.max_vectors as usize)
                    .ok_or_else(|| VectorError::ResourceLimit("multi payload overflow".into()))
            }
        }
    }
    /// Exact scan support.
    #[must_use]
    pub const fn supports_exact(self, metric: VectorMetric) -> bool {
        if matches!(metric, VectorMetric::Hamming | VectorMetric::Jaccard) {
            return matches!(self.kind, VectorKind::Binary);
        }
        !matches!(
            (self.kind, self.element),
            (VectorKind::Binary, _) | (_, VectorElementType::Binary)
        )
    }
    /// Shared envelope for the dense ANN methods with wired executor
    /// execution: `Dense` kind only (execution materializes dense `f32`
    /// lanes; `Multi` values keep their constituent exact kernel, `Sparse`
    /// and `Binary` never reach these paths), non-binary element, and one of
    /// L2/Cosine/Dot. The metric gate is uniform across that set, so probing
    /// with any one of the three answers for all three.
    const fn ann_numeric(self, metric: VectorMetric) -> bool {
        matches!(self.kind, VectorKind::Dense)
            && !matches!(self.element, VectorElementType::Binary)
            && matches!(
                metric,
                VectorMetric::L2 | VectorMetric::Cosine | VectorMetric::Dot
            )
    }
    /// HNSW: dense numeric lanes + L2/Cosine/Dot.
    ///
    /// `Multi`, `Sparse` and `Binary` columns have no HNSW execution path in
    /// v0.1.0, so this predicate — and the DDL gate built on it — reject
    /// them instead of accepting metadata that can never execute.
    #[must_use]
    pub const fn supports_hnsw(self, metric: VectorMetric) -> bool {
        self.ann_numeric(metric)
    }
    /// IVF-Flat follows the HNSW envelope in v1 (and, like HNSW, really does
    /// partition: deterministic k-means centroids plus exact posting lists).
    #[must_use]
    pub const fn supports_ivf(self, metric: VectorMetric) -> bool {
        self.ann_numeric(metric)
    }
    /// PQ: dense float lanes natively, INT8/UINT8 via documented `f32`
    /// promotion, INT4/binary rejected.
    ///
    /// The `ivf_pq` SQL method name is a compatibility label: v0.1.0
    /// executes a global product-quantized ADC scan, **not** inverted-list
    /// partitioning.
    #[must_use]
    pub const fn supports_pq(self, metric: VectorMetric) -> bool {
        self.ann_numeric(metric)
            && matches!(
                self.element,
                VectorElementType::Float32
                    | VectorElementType::Float16
                    | VectorElementType::BFloat16
                    | VectorElementType::Int8
                    | VectorElementType::Uint8
            )
    }
    /// SQ: dense lanes; FLOAT32/16, BFLOAT16, INT8/UINT8 natively, INT4 via
    /// documented `f32` promotion.
    ///
    /// The `ivf_sq` SQL method name is a compatibility label: v0.1.0
    /// executes a global scalar-quantized scan, **not** inverted-list
    /// partitioning.
    #[must_use]
    pub const fn supports_sq(self, metric: VectorMetric) -> bool {
        self.ann_numeric(metric)
            && matches!(
                self.element,
                VectorElementType::Float32
                    | VectorElementType::Float16
                    | VectorElementType::BFloat16
                    | VectorElementType::Int8
                    | VectorElementType::Uint8
                    | VectorElementType::Int4
            )
    }
    /// Encode as Postgres-style typmod (kind bits 31..24, elem 23..16, dim-1 in 15..0).
    /// Multi `max_vectors` travels out-of-band in the catalog.
    pub fn typmod_encode(self) -> Result<i32> {
        self.validate()?;
        let dim_part = self.dimension.wrapping_sub(1);
        if dim_part > 0xFFFF {
            return Err(VectorError::InvalidDimension(
                "dimension out of typmod range".into(),
            ));
        }
        let (kc, ec) = (i32::from(self.kind.code()), i32::from(self.element.code()));
        Ok((kc << 24) | (ec << 16) | (dim_part as i32))
    }
    /// Decode [`Self::typmod_encode`] output (`max_vectors` defaults to 1).
    pub fn typmod_decode(typmod: i32) -> Result<Self> {
        if typmod < 0 {
            return Err(VectorError::InvalidDimension(format!(
                "reserved typmod {typmod}"
            )));
        }
        let kind = VectorKind::from_code(((typmod >> 24) & 0xFF) as u8)?;
        let element = VectorElementType::from_code(((typmod >> 16) & 0xFF) as u8)?;
        Self::with_max_vectors(kind, element, ((typmod & 0xFFFF) as u32).wrapping_add(1), 1)
    }
    /// Parse `VECTOR(..)`, `SPARSE_VECTOR(..)`, `MULTI_VECTOR(..)` (case-insensitive).
    pub fn parse_sql(s: &str) -> Result<Self> {
        let (head, args) = split_head_args(s.trim())?;
        let arity = |n: usize, what: &str| {
            if args.len() != n {
                Err(VectorError::InvalidLiteral(format!(
                    "{what} wants {n} args, got {}",
                    args.len()
                )))
            } else {
                Ok(())
            }
        };
        match head.to_ascii_uppercase().as_str() {
            "VECTOR" => {
                arity(2, "VECTOR")?;
                let e = VectorElementType::parse_sql(&args[0])?;
                let k = if e == VectorElementType::Binary {
                    VectorKind::Binary
                } else {
                    VectorKind::Dense
                };
                Self::with_max_vectors(k, e, parse_dim(&args[1])?, 1)
            }
            "SPARSE_VECTOR" | "SPARSEVECTOR" => {
                arity(2, "SPARSE_VECTOR")?;
                Self::with_max_vectors(
                    VectorKind::Sparse,
                    VectorElementType::parse_sql(&args[0])?,
                    parse_dim(&args[1])?,
                    1,
                )
            }
            "MULTI_VECTOR" | "MULTIVECTOR" => {
                arity(3, "MULTI_VECTOR")?;
                Self::with_max_vectors(
                    VectorKind::Multi,
                    VectorElementType::parse_sql(&args[0])?,
                    parse_dim(&args[2])?,
                    parse_dim(&args[1])?,
                )
            }
            o => Err(VectorError::InvalidLiteral(format!(
                "unknown vector type '{o}'"
            ))),
        }
    }
    /// Canonical declaration, e.g. `VECTOR(FLOAT32, 768)`.
    pub fn sql_name(self) -> String {
        let (e, d) = (self.element.sql_name(), self.dimension);
        match self.kind {
            VectorKind::Dense => format!("VECTOR({e}, {d})"),
            VectorKind::Sparse => format!("SPARSE_VECTOR({e}, {d})"),
            VectorKind::Multi => format!("MULTI_VECTOR({e}, {}, {d})", self.max_vectors),
            VectorKind::Binary => format!("VECTOR(BINARY, {d})"),
        }
    }
}

// Flat comma-separated `HEAD(a, b, …)` splitter (type args never nest).
fn split_head_args(s: &str) -> Result<(String, Vec<String>)> {
    let open = s
        .find('(')
        .ok_or_else(|| VectorError::InvalidLiteral(format!("missing '(' in '{s}'")))?;
    if !s.trim_end().ends_with(')') {
        return Err(VectorError::InvalidLiteral(format!("missing ')' in '{s}'")));
    }
    let head = s[..open].trim().to_string();
    if head.is_empty() {
        return Err(VectorError::InvalidLiteral("empty type name".into()));
    }
    let args = s[open + 1..s.len() - 1]
        .split(',')
        .map(|a| a.trim().to_string())
        .collect();
    Ok((head, args))
}

fn parse_dim(s: &str) -> Result<u32> {
    let v: i64 = s
        .trim()
        .parse()
        .map_err(|_| VectorError::InvalidLiteral(format!("bad int '{s}'")))?;
    if !(1..=65_536).contains(&v) {
        return Err(VectorError::InvalidDimension(format!(
            "dimension {v} out of range 1..=65536"
        )));
    }
    Ok(v as u32)
}
