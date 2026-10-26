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
#![forbid(unsafe_code)]
//! Single-node wire contract for vectors (Gap C, in-crate half).
//!
//! The contract SQL → parser → planner → execution → result → PGWire
//! encoding must hold for every supported operation, and the wire layer
//! must fail closed. The encoding functions themselves live in
//! [`crate::format`]; this module owns the **contract**: typmod/OID
//! identity, text/binary round-trips for every kind/element/dimension,
//! NULL/empty/invalid rules, and the documented binary-parameter refusal.
//!
//! SQLSTATE behavior and the full request/response matrix are exercised
//! in `crates/network/tests/vector_wire.rs` and `tests/VECTOR.sql`; the
//! pure checks here run in unit tests without a server.

use crate::r#type::{VectorElementType, VectorKind, VectorType};
use crate::{Result, VectorError, VectorValue};

/// Wire OID for `VECTOR` (mirrors `plomid-types` `PgType::Vector`;
/// kept as a literal so this crate stays dependency-free).
pub const WIRE_OID_VECTOR: u32 = 16384;
/// Wire OID for `VECTOR[]` (mirrors `VECTOR_ARRAY`).
pub const WIRE_OID_VECTOR_ARRAY: u32 = 16385;

/// Binary vector parameters are NOT supported: the row-payload codec has
/// no self-describing header, so a `Bind` cannot decode OID 16384 without
/// the column kind/element/dimension. Send vectors in text format.
/// The server fails these closed; this constant pins the rule in tests.
pub const BINARY_PARAMS_SUPPORTED: bool = false;

/// One value in all three wire-visible forms.
#[derive(Debug, Clone, PartialEq)]
pub struct WireVector {
    /// Catalog typmod (`VectorType::typmod_encode`).
    pub typmod: i32,
    /// Wire OID (always [`WIRE_OID_VECTOR`]).
    pub oid: u32,
    /// Text form (canonical SQL literal).
    pub text: String,
    /// Binary form (row-payload bytes).
    pub binary: Vec<u8>,
}

/// Encode `value` (of `vector_type`) into every wire-visible form.
///
/// Fails closed on kind/element/dimension mismatch: what the `RowDescription`
/// advertises (typmod/OID) always matches the `DataRow` bytes.
///
/// Binary form carries Float32 lanes for dense vectors: non-Float32 dense
/// elements travel text-only on the wire and fail here with
/// `UnsupportedCombination` (fail closed, never silently requantized).
pub fn encode_for_wire(value: &VectorValue, vector_type: &VectorType) -> Result<WireVector> {
    if value.kind() != vector_type.kind {
        return Err(VectorError::TypeMismatch(format!(
            "wire encode: value kind {:?} != column kind {:?}",
            value.kind(),
            vector_type.kind
        )));
    }
    if value.dimension() != vector_type.dimension {
        return Err(VectorError::DimensionMismatch {
            expected: vector_type.dimension,
            got: value.dimension(),
        });
    }
    // Element identity: a Float32-tagged value must not ride an INT8 (or
    // any non-Float32) column's wire bytes.
    if let VectorValue::Dense(d) = value {
        if d.element != vector_type.element {
            return Err(VectorError::TypeMismatch(format!(
                "wire encode: value element {} != column element {}",
                d.element.sql_name(),
                vector_type.element.sql_name()
            )));
        }
    }
    Ok(WireVector {
        typmod: vector_type.typmod_encode()?,
        oid: WIRE_OID_VECTOR,
        text: crate::format::pgwire_text_encode(value),
        binary: crate::format::pgwire_binary_encode(value)?,
    })
}

/// Decode a text `DataRow` field under the `RowDescription` shape.
pub fn decode_text_wire(
    text: &str,
    kind: VectorKind,
    dimension: u32,
    max_vectors: u32,
) -> Result<VectorValue> {
    crate::format::pgwire_text_decode(text, kind, dimension, max_vectors)
}

/// Decode a binary `DataRow` field under the `RowDescription` shape.
pub fn decode_binary_wire(
    bytes: &[u8],
    kind: VectorKind,
    element: VectorElementType,
    dimension: u32,
    max_vectors: u32,
) -> Result<VectorValue> {
    crate::format::pgwire_binary_decode(bytes, kind, element, dimension, max_vectors)
}

/// Expected binary payload length for a column shape, or `None` for
/// variable shapes (sparse `nnz`, multi `count` vary per row).
pub fn expected_binary_len(
    kind: VectorKind,
    element: VectorElementType,
    dimension: u32,
) -> Result<Option<usize>> {
    match kind {
        VectorKind::Dense => Ok(Some(element.payload_bytes(dimension)?)),
        VectorKind::Binary => Ok(Some(crate::validation::binary_payload_bytes(dimension)?)),
        VectorKind::Sparse | VectorKind::Multi => Ok(None),
    }
}

/// Verify a [`WireVector`] against its column type: typmod decodes to the
/// same kind/element/dimension, OID is the vector OID, text parses back to
/// an equal value, and fixed-shape binary length matches the shape contract.
///
/// `max_vectors` is intentionally NOT compared: the catalog typmod packs
/// only kind/element/dimension (`max_vectors` travels out-of-band in the
/// column definition), so a multi typmod always decodes with
/// `max_vectors == 1`. Constituent-count enforcement happens at parse
/// time (`parse_multi` caps against the declared maximum).
pub fn check_wire_vector(wire: &WireVector, vector_type: &VectorType) -> Result<()> {
    if wire.oid != WIRE_OID_VECTOR {
        return Err(VectorError::IndexCorrupted(format!(
            "wire contract: oid {} != {WIRE_OID_VECTOR}",
            wire.oid
        )));
    }
    let back = VectorType::typmod_decode(wire.typmod)?;
    if back.kind != vector_type.kind
        || back.element != vector_type.element
        || back.dimension != vector_type.dimension
    {
        return Err(VectorError::TypeMismatch(format!(
            "wire contract: typmod decodes to {} != {}",
            back.sql_name(),
            vector_type.sql_name()
        )));
    }
    let text_back = decode_text_wire(
        &wire.text,
        vector_type.kind,
        vector_type.dimension,
        vector_type.max_vectors,
    )?;
    let binary_back = decode_binary_wire(
        &wire.binary,
        vector_type.kind,
        vector_type.element,
        vector_type.dimension,
        vector_type.max_vectors,
    )?;
    // Text and binary must describe the same value.
    if format!("{text_back:?}") != format!("{binary_back:?}") {
        return Err(VectorError::Corruption(
            "wire contract: text/binary decode divergence".to_string(),
        ));
    }
    if let Some(want) =
        expected_binary_len(vector_type.kind, vector_type.element, vector_type.dimension)?
    {
        if wire.binary.len() != want {
            return Err(VectorError::Corruption(format!(
                "wire contract: binary len {} != shape len {want}",
                wire.binary.len()
            )));
        }
    }
    Ok(())
}
