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
//! SQL literal formatting and PGWire text/binary helpers.
//!
//! Text on the wire is the canonical literal from [`crate::literal`];
//! binary on the wire is the row payload from [`crate::codec`] (payload
//! bytes only, type carried by the column descriptor).

use crate::codec;
use crate::error::{Result, VectorError};
use crate::literal;
use crate::r#type::{VectorElementType, VectorKind};
use crate::value::VectorValue;

/// Canonical SQL literal text, e.g. `[1,2]`, `{1:2}/8`, `'0101'`.
#[must_use]
pub fn format_sql_literal(value: &VectorValue) -> String {
    literal::format_vector_value(value)
}

/// Typed SQL literal with an explicit cast, e.g. `'[1,2]'::VECTOR(FLOAT32, 2)`.
#[must_use]
pub fn format_sql_typed(value: &VectorValue, type_sql: &str) -> String {
    format!("'{}'::{type_sql}", format_sql_literal(value))
}

/// PGWire text encoding (same bytes as the SQL literal).
#[must_use]
pub fn pgwire_text_encode(value: &VectorValue) -> String {
    format_sql_literal(value)
}

/// PGWire text decoding with an explicit expected shape.
pub fn pgwire_text_decode(
    text: &str,
    kind: VectorKind,
    dimension: u32,
    max_vectors: u32,
) -> Result<VectorValue> {
    match kind {
        VectorKind::Dense => literal::parse_dense(text, Some(dimension)).map(VectorValue::Dense),
        VectorKind::Sparse => literal::parse_sparse(text)
            .and_then(|s| {
                if s.dimension != dimension {
                    return Err(VectorError::DimensionMismatch {
                        expected: dimension,
                        got: s.dimension,
                    });
                }
                Ok(s)
            })
            .map(VectorValue::Sparse),
        VectorKind::Multi => literal::parse_multi(text, max_vectors)
            .and_then(|m| {
                if m.dimension != dimension {
                    return Err(VectorError::DimensionMismatch {
                        expected: dimension,
                        got: m.dimension,
                    });
                }
                Ok(m)
            })
            .map(VectorValue::Multi),
        VectorKind::Binary => {
            let b = literal::parse_binary(text)?;
            if b.dimension_bits != dimension {
                return Err(VectorError::DimensionMismatch {
                    expected: dimension,
                    got: b.dimension_bits,
                });
            }
            Ok(VectorValue::Binary(b))
        }
    }
}

/// PGWire binary encoding: row payload bytes.
pub fn pgwire_binary_encode(value: &VectorValue) -> Result<Vec<u8>> {
    codec::encode_row_payload(value)
}

/// PGWire binary decoding under a declared column shape.
pub fn pgwire_binary_decode(
    bytes: &[u8],
    kind: VectorKind,
    element: VectorElementType,
    dimension: u32,
    max_vectors: u32,
) -> Result<VectorValue> {
    codec::decode_row_payload(bytes, kind, element, dimension, max_vectors)
}
