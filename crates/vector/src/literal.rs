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
//! Canonical SQL-facing literal parsing and deterministic formatting.
//!
//! Dense:   `[0.1, 0.2]` (whitespace, signs, exponents allowed)
//! Sparse:  `{15:0.82, 381:0.41}/30000`
//! Binary:  `'010101'` or `[0, 1, 1]`
//! Multi:   `[[0.1, 0.2], [0.3, 0.4]]`
//!
//! Indices are zero-based. `NaN`/`Inf` are always rejected. Empty dense
//! vectors are rejected; empty sparse (`{}/dim`) is allowed.

use crate::error::{Result, VectorError};
use crate::r#type::VectorElementType;
use crate::validation;
use crate::value::{
    BinaryVectorValue, DenseVectorValue, MultiVectorValue, SparseVectorValue, VectorValue,
};

/// Parse a dense literal, optionally enforcing an expected dimension.
pub fn parse_dense(s: &str, expected_dim: Option<u32>) -> Result<DenseVectorValue> {
    let t = s.trim();
    if !t.starts_with('[') || !t.ends_with(']') {
        return Err(VectorError::InvalidLiteral(format!(
            "dense literal must be [..]: '{s}'"
        )));
    }
    let inner = &t[1..t.len() - 1];
    if inner.trim().is_empty() {
        return Err(VectorError::InvalidLiteral(
            "dense vector must not be empty".into(),
        ));
    }
    let mut values = Vec::new();
    for tok in inner.split(',') {
        let tok = tok.trim();
        if tok.is_empty() {
            return Err(VectorError::InvalidLiteral("empty dense coordinate".into()));
        }
        let v: f32 = tok
            .parse()
            .map_err(|_| VectorError::InvalidLiteral(format!("bad float coordinate '{tok}'")))?;
        if !v.is_finite() {
            return Err(VectorError::NonFiniteValue(format!("coordinate '{tok}'")));
        }
        values.push(v);
    }
    if let Some(d) = expected_dim {
        if values.len() as u32 != d {
            return Err(VectorError::DimensionMismatch {
                expected: d,
                got: values.len() as u32,
            });
        }
    }
    DenseVectorValue::from_f32(VectorElementType::Float32, values)
}

/// Deterministic dense formatting: `[v,v,…]` via `f32` shortest round-trip.
#[must_use]
pub fn format_dense(v: &DenseVectorValue) -> String {
    let parts: Vec<String> = v.values_f32.iter().map(|x| format_f32(*x)).collect();
    format!("[{}]", parts.join(","))
}

fn format_f32(x: f32) -> String {
    // Shortest round-trip form; integral values print without a fraction.
    if x == 0.0 {
        return "0".to_string();
    }
    format!("{x}")
}

/// Parse `{idx:val, …}/dim`.
pub fn parse_sparse(s: &str) -> Result<SparseVectorValue> {
    let t = s.trim();
    let slash = t.rfind('/').ok_or_else(|| {
        VectorError::InvalidLiteral(format!("sparse literal needs /dim suffix: '{s}'"))
    })?;
    let dim: u32 = t[slash + 1..]
        .trim()
        .parse()
        .map_err(|_| VectorError::InvalidLiteral(format!("bad sparse dimension in '{s}'")))?;
    validation::validate_dimension(dim)?;
    let body = t[..slash].trim();
    if !body.starts_with('{') || !body.ends_with('}') {
        return Err(VectorError::InvalidLiteral(format!(
            "sparse body must be {{..}}: '{s}'"
        )));
    }
    let inner = body[1..body.len() - 1].trim();
    if inner.is_empty() {
        return SparseVectorValue::new(dim, Vec::new(), Vec::new());
    }
    let mut indices = Vec::new();
    let mut values = Vec::new();
    for entry in inner.split(',') {
        let entry = entry.trim();
        let colon = entry.find(':').ok_or_else(|| {
            VectorError::InvalidLiteral(format!("sparse entry needs idx:val: '{entry}'"))
        })?;
        let idx: u32 = entry[..colon]
            .trim()
            .parse()
            .map_err(|_| VectorError::InvalidLiteral(format!("bad sparse index in '{entry}'")))?;
        let val: f32 = entry[colon + 1..]
            .trim()
            .parse()
            .map_err(|_| VectorError::InvalidLiteral(format!("bad sparse value in '{entry}'")))?;
        if !val.is_finite() {
            return Err(VectorError::NonFiniteValue(format!(
                "sparse value in '{entry}'"
            )));
        }
        indices.push(idx);
        values.push(val);
    }
    SparseVectorValue::new(dim, indices, values)
}

/// Deterministic sparse formatting.
#[must_use]
pub fn format_sparse(v: &SparseVectorValue) -> String {
    let parts: Vec<String> = v
        .indices
        .iter()
        .zip(v.values.iter())
        .map(|(i, x)| format!("{i}:{}", format_f32(*x)))
        .collect();
    format!("{{{}}}/{}", parts.join(","), v.dimension)
}

/// Parse binary: `'0101…'` or `[0, 1, 1]`.
pub fn parse_binary(s: &str) -> Result<BinaryVectorValue> {
    let t = s.trim();
    if (t.starts_with('\'') && t.ends_with('\'')) || (t.starts_with('"') && t.ends_with('"')) {
        let bits_str = &t[1..t.len() - 1];
        if bits_str.is_empty() {
            return Err(VectorError::InvalidLiteral(
                "binary literal must not be empty".into(),
            ));
        }
        let mut bytes: Vec<u8> = Vec::new();
        let mut cur: u8 = 0;
        let mut nbits: u32 = 0;
        for ch in bits_str.chars() {
            match ch {
                '0' | '1' => {
                    cur = (cur << 1) | u32::from(ch == '1') as u8;
                    nbits += 1;
                    if nbits.is_multiple_of(8) {
                        bytes.push(cur);
                        cur = 0;
                    }
                }
                c if c.is_whitespace() => {}
                _ => {
                    return Err(VectorError::InvalidLiteral(format!(
                        "bad binary digit '{ch}'"
                    )));
                }
            }
        }
        if !nbits.is_multiple_of(8) {
            cur <<= 8 - (nbits % 8);
            bytes.push(cur);
        }
        return BinaryVectorValue::new(nbits, bytes);
    }
    if t.starts_with('[') && t.ends_with(']') {
        let inner = t[1..t.len() - 1].trim();
        if inner.is_empty() {
            return Err(VectorError::InvalidLiteral(
                "binary literal must not be empty".into(),
            ));
        }
        let mut bytes: Vec<u8> = Vec::new();
        let mut cur: u8 = 0;
        let mut nbits: u32 = 0;
        for tok in inner.split(',') {
            match tok.trim() {
                "0" | "1" => {
                    let bit = tok.trim() == "1";
                    cur = (cur << 1) | u32::from(bit) as u8;
                    nbits += 1;
                    if nbits.is_multiple_of(8) {
                        bytes.push(cur);
                        cur = 0;
                    }
                }
                other => {
                    return Err(VectorError::InvalidLiteral(format!(
                        "binary list entries must be 0/1, got '{other}'"
                    )));
                }
            }
        }
        if !nbits.is_multiple_of(8) {
            cur <<= 8 - (nbits % 8);
            bytes.push(cur);
        }
        return BinaryVectorValue::new(nbits, bytes);
    }
    Err(VectorError::InvalidLiteral(format!(
        "bad binary literal '{s}'"
    )))
}

/// Canonical binary formatting as `'0101…'`.
#[must_use]
pub fn format_binary(v: &BinaryVectorValue) -> String {
    let mut s = String::with_capacity(v.dimension_bits as usize + 2);
    s.push('\'');
    for i in 0..v.dimension_bits {
        s.push(if v.get_bit(i) { '1' } else { '0' });
    }
    s.push('\'');
    s
}

/// Parse `[[…], […]]` with a shared inner dimension.
pub fn parse_multi(s: &str, max_vectors: u32) -> Result<MultiVectorValue> {
    validation::validate_max_vectors(max_vectors)?;
    let inners = split_multi(s)?;
    if inners.is_empty() {
        return Err(VectorError::InvalidLiteral(
            "multi-vector needs ≥1 constituent".into(),
        ));
    }
    if inners.len() > max_vectors as usize {
        return Err(VectorError::InvalidLiteral(format!(
            "constituent count {} exceeds max_vectors {max_vectors}",
            inners.len()
        )));
    }
    let mut vectors = Vec::with_capacity(inners.len());
    let mut dim: Option<u32> = None;
    for inner in &inners {
        let d = parse_dense(inner, None)?;
        match dim {
            None => dim = Some(d.dimension),
            Some(e) if e != d.dimension => {
                return Err(VectorError::DimensionMismatch {
                    expected: e,
                    got: d.dimension,
                });
            }
            _ => {}
        }
        vectors.push(d.values_f32);
    }
    let dimension = dim.unwrap_or(1);
    MultiVectorValue::new(VectorElementType::Float32, dimension, max_vectors, vectors)
}

fn split_multi(s: &str) -> Result<Vec<String>> {
    let t = s.trim();
    if !t.starts_with('[') || !t.ends_with(']') {
        return Err(VectorError::InvalidLiteral(format!(
            "bad multi-vector '{s}'"
        )));
    }
    let body = t[1..t.len() - 1].trim();
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut cur = String::new();
    let mut in_vec = false;
    for ch in body.chars() {
        match ch {
            '[' => {
                depth += 1;
                in_vec = true;
                cur.push(ch);
            }
            ']' => {
                depth -= 1;
                cur.push(ch);
                if depth == 0 && in_vec {
                    out.push(cur.trim().to_string());
                    cur.clear();
                    in_vec = false;
                }
                if depth < 0 {
                    return Err(VectorError::InvalidLiteral("unbalanced brackets".into()));
                }
            }
            ',' if depth == 0 => {}
            _ => {
                if !in_vec && ch.is_whitespace() {
                    continue;
                }
                if depth == 0 && !in_vec {
                    return Err(VectorError::InvalidLiteral(format!(
                        "expected '[..]' constituents in '{s}'"
                    )));
                }
                cur.push(ch);
            }
        }
    }
    if depth != 0 || in_vec {
        return Err(VectorError::InvalidLiteral("truncated multi-vector".into()));
    }
    Ok(out)
}

/// Deterministic multi formatting.
#[must_use]
pub fn format_multi(v: &MultiVectorValue) -> String {
    let parts: Vec<String> = v
        .vectors
        .iter()
        .map(|row| {
            let cells: Vec<String> = row.iter().map(|x| format_f32(*x)).collect();
            format!("[{}]", cells.join(","))
        })
        .collect();
    format!("[{}]", parts.join(","))
}

/// Auto-detect literal kind and parse (binary `[0,1]` lists are parsed as
/// dense; call [`parse_binary`] explicitly for bit semantics).
pub fn parse_vector_literal(s: &str, max_vectors: u32) -> Result<VectorValue> {
    let t = s.trim();
    if t.starts_with('{') {
        return parse_sparse(t).map(VectorValue::Sparse);
    }
    if t.starts_with('\'') || t.starts_with('"') {
        return parse_binary(t).map(VectorValue::Binary);
    }
    if t.starts_with("[[") {
        return parse_multi(t, max_vectors).map(VectorValue::Multi);
    }
    parse_dense(t, None).map(VectorValue::Dense)
}

/// Deterministic formatting for any value.
#[must_use]
pub fn format_vector_value(v: &VectorValue) -> String {
    match v {
        VectorValue::Dense(d) => format_dense(d),
        VectorValue::Sparse(s) => format_sparse(s),
        VectorValue::Multi(m) => format_multi(m),
        VectorValue::Binary(b) => format_binary(b),
    }
}
