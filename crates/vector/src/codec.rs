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
//! Versioned binary codec: standalone artifacts (magic + checksum) and row payloads.
//! Layout (LE, golden-test stable): `b"PVEC" | ver u8=1 | kind u8 | elem u8 | rsv u8=0`
//! `| dim u32 | extra u32 | payload_len u32 | payload | crc32c u32` (Castagnoli, software).
//! `extra` = nnz (sparse) | count (multi) | bits (binary) | 0 (dense).

use crate::error::{Result, VectorError};
use crate::r#type::{VectorElementType, VectorKind};
use crate::validation;
use crate::value::{
    BinaryVectorValue, DenseVectorValue, MultiVectorValue, SparseVectorValue, VectorValue,
};

/// Magic prefix for standalone artifacts.
pub const MAGIC: [u8; 4] = *b"PVEC";
/// Current artifact version.
pub const VERSION: u8 = 1;
/// Header bytes before the payload (`4 + 1 + 1 + 1 + 1 + 4 + 4 + 4`).
pub const HEADER_LEN: usize = 20;

/// Software CRC-32C (Castagnoli, reflected poly `0x82F63B78`).
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

/// Encode a standalone artifact (magic + header + payload + CRC).
pub fn encode_standalone(value: &VectorValue) -> Result<Vec<u8>> {
    let (kind, elem, dim, extra, payload) = encode_parts(value)?;
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len() + 4);
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.push(kind.code());
    out.push(elem.code());
    out.push(0u8); // reserved
    out.extend_from_slice(&dim.to_le_bytes());
    out.extend_from_slice(&extra.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    let crc = crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Alias kept for storage/index persistence call sites.
pub fn encode_artifact(value: &VectorValue) -> Result<Vec<u8>> {
    encode_standalone(value)
}

/// Decode a standalone artifact, verifying framing, sizes, and CRC.
/// Multi `max_vectors` decodes as 65536: artifacts store the count, not the catalog cap.
pub fn decode_standalone(bytes: &[u8]) -> Result<VectorValue> {
    if bytes.len() < HEADER_LEN + 4 {
        return Err(VectorError::Corruption("artifact truncated".into()));
    }
    if bytes[0..4] != MAGIC {
        return Err(VectorError::Corruption("bad magic".into()));
    }
    if bytes[4] != VERSION {
        return Err(VectorError::Corruption(format!(
            "unsupported version {}",
            bytes[4]
        )));
    }
    if bytes[7] != 0 {
        return Err(VectorError::Corruption("nonzero reserved byte".into()));
    }
    let kind = VectorKind::from_code(bytes[5])
        .map_err(|_| VectorError::Corruption("unknown kind".into()))?;
    let elem = VectorElementType::from_code(bytes[6])
        .map_err(|_| VectorError::Corruption("unknown element".into()))?;
    let dim = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let extra = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    let payload_len = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]) as usize;
    validation::check_budget(payload_len)?;
    if bytes.len() != HEADER_LEN + payload_len + 4 {
        let body = bytes.len().saturating_sub(HEADER_LEN + 4);
        return Err(VectorError::Corruption(format!(
            "length mismatch: {payload_len} vs {body}"
        )));
    }
    let expect = u32::from_le_bytes([
        bytes[HEADER_LEN + payload_len],
        bytes[HEADER_LEN + payload_len + 1],
        bytes[HEADER_LEN + payload_len + 2],
        bytes[HEADER_LEN + payload_len + 3],
    ]);
    let got = crc32c(&bytes[..HEADER_LEN + payload_len]);
    if expect != got {
        return Err(VectorError::Corruption("crc32c mismatch".into()));
    }
    decode_parts(
        kind,
        elem,
        dim,
        extra,
        &bytes[HEADER_LEN..HEADER_LEN + payload_len],
    )
}

/// Row payload only (no magic/version/CRC); the catalog supplies the type.
pub fn encode_row_payload(value: &VectorValue) -> Result<Vec<u8>> {
    let (_, _, _, _, payload) = encode_parts(value)?;
    Ok(payload)
}

/// Decode a row payload given its declared type context.
pub fn decode_row_payload(
    payload: &[u8],
    kind: VectorKind,
    element: VectorElementType,
    dimension: u32,
    max_vectors: u32,
) -> Result<VectorValue> {
    validation::check_budget(payload.len())?;
    match kind {
        VectorKind::Dense => {
            let want = element.payload_bytes(dimension)?;
            // Dense f32 lanes on the wire; other logical types are a
            // storage concern handled above this layer.
            if element != VectorElementType::Float32 {
                return Err(VectorError::UnsupportedCombination(
                    "row payloads currently carry Float32 lanes".into(),
                ));
            }
            if payload.len() != want {
                let n = payload.len();
                return Err(VectorError::Corruption(format!(
                    "dense payload {n}, expected {want}"
                )));
            }
            decode_f32_row(element, dimension, payload).map(|v| {
                VectorValue::Dense(DenseVectorValue {
                    element,
                    dimension,
                    values_f32: v,
                })
            })
        }
        VectorKind::Sparse => decode_sparse_payload(dimension, payload),
        VectorKind::Multi => {
            if element == VectorElementType::Binary {
                return Err(VectorError::UnsupportedCombination(
                    "BINARY multi-vectors are not supported".into(),
                ));
            }
            if !payload.len().is_multiple_of(4) {
                return Err(VectorError::Corruption(
                    "multi payload not f32-aligned".into(),
                ));
            }
            let lanes = payload.len() / 4;
            if dimension == 0 || !lanes.is_multiple_of(dimension as usize) {
                return Err(VectorError::Corruption(
                    "multi payload shape mismatch".into(),
                ));
            }
            let count = (lanes / dimension as usize) as u32;
            decode_multi_payload(element, dimension, max_vectors, count, payload)
        }
        VectorKind::Binary => {
            let want = validation::binary_payload_bytes(dimension)?;
            if payload.len() != want {
                return Err(VectorError::Corruption(format!(
                    "binary payload {} bytes, expected {want}",
                    payload.len()
                )));
            }
            BinaryVectorValue::new(dimension, payload.to_vec()).map(VectorValue::Binary)
        }
    }
}

fn encode_parts(value: &VectorValue) -> Result<(VectorKind, VectorElementType, u32, u32, Vec<u8>)> {
    match value {
        VectorValue::Dense(v) => {
            if v.element != VectorElementType::Float32 {
                return Err(VectorError::UnsupportedCombination(
                    "codec v1 carries Float32 dense lanes".into(),
                ));
            }
            validation::validate_dimension(v.dimension)?;
            if v.values_f32.len() as u32 != v.dimension {
                return Err(VectorError::DimensionMismatch {
                    expected: v.dimension,
                    got: v.values_f32.len() as u32,
                });
            }
            validation::check_finite_slice(&v.values_f32)?;
            let mut payload = Vec::with_capacity(v.values_f32.len() * 4);
            for x in &v.values_f32 {
                payload.extend_from_slice(&x.to_le_bytes());
            }
            Ok((VectorKind::Dense, v.element, v.dimension, 0, payload))
        }
        VectorValue::Sparse(s) => {
            validation::validate_dimension(s.dimension)?;
            let mut payload = Vec::with_capacity(s.indices.len() * 8);
            for (i, x) in s.indices.iter().zip(s.values.iter()) {
                payload.extend_from_slice(&i.to_le_bytes());
                payload.extend_from_slice(&x.to_le_bytes());
            }
            Ok((
                VectorKind::Sparse,
                VectorElementType::Float32,
                s.dimension,
                s.nnz(),
                payload,
            ))
        }
        VectorValue::Multi(m) => {
            if m.element != VectorElementType::Float32 {
                return Err(VectorError::UnsupportedCombination(
                    "codec v1 carries Float32 multi lanes".into(),
                ));
            }
            let mut payload = Vec::new();
            for row in &m.vectors {
                for x in row {
                    payload.extend_from_slice(&x.to_le_bytes());
                }
            }
            Ok((
                VectorKind::Multi,
                m.element,
                m.dimension,
                m.count(),
                payload,
            ))
        }
        VectorValue::Binary(b) => Ok((
            VectorKind::Binary,
            VectorElementType::Binary,
            b.dimension_bits,
            b.dimension_bits,
            b.bytes.clone(),
        )),
    }
}

fn decode_parts(
    kind: VectorKind,
    elem: VectorElementType,
    dim: u32,
    extra: u32,
    payload: &[u8],
) -> Result<VectorValue> {
    match kind {
        VectorKind::Dense => {
            if elem != VectorElementType::Float32 {
                return Err(VectorError::Corruption("unsupported dense element".into()));
            }
            validation::validate_dimension(dim)?;
            if extra != 0 {
                return Err(VectorError::Corruption("dense extra must be 0".into()));
            }
            let want = elem.payload_bytes(dim)?;
            if payload.len() != want {
                let n = payload.len();
                return Err(VectorError::Corruption(format!(
                    "dense payload {n}, expected {want}"
                )));
            }
            decode_f32_row(elem, dim, payload)
                .and_then(|v| DenseVectorValue::from_f32(elem, v).map(VectorValue::Dense))
        }
        VectorKind::Sparse => {
            if elem != VectorElementType::Float32 {
                return Err(VectorError::Corruption("unsupported sparse element".into()));
            }
            validation::validate_nnz(extra, dim)?;
            if payload.len() != extra as usize * 8 {
                return Err(VectorError::Corruption(
                    "sparse payload length mismatch".into(),
                ));
            }
            decode_sparse_payload(dim, payload)
        }
        VectorKind::Multi => {
            if elem == VectorElementType::Binary {
                return Err(VectorError::Corruption("binary multi not supported".into()));
            }
            validation::validate_dimension(dim)?;
            decode_multi_payload(elem, dim, validation::MAX_DIMENSION, extra, payload)
        }
        VectorKind::Binary => {
            if elem != VectorElementType::Binary {
                return Err(VectorError::Corruption(
                    "binary kind needs BINARY element".into(),
                ));
            }
            if extra != dim {
                return Err(VectorError::Corruption(
                    "binary extra must equal bits".into(),
                ));
            }
            BinaryVectorValue::new(dim, payload.to_vec()).map(VectorValue::Binary)
        }
    }
}

fn decode_f32_row(element: VectorElementType, dim: u32, payload: &[u8]) -> Result<Vec<f32>> {
    if payload.len() != dim as usize * 4 {
        return Err(VectorError::Corruption("f32 row length mismatch".into()));
    }
    let mut out = Vec::with_capacity(dim as usize);
    for chunk in payload.chunks_exact(4) {
        let v = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        if !v.is_finite() {
            return Err(VectorError::NonFiniteValue(
                "decoded lane non-finite".into(),
            ));
        }
        out.push(v);
    }
    let _ = element;
    Ok(out)
}

fn decode_sparse_payload(dimension: u32, payload: &[u8]) -> Result<VectorValue> {
    if !payload.len().is_multiple_of(8) {
        return Err(VectorError::Corruption(
            "sparse payload not entry-aligned".into(),
        ));
    }
    let mut indices = Vec::with_capacity(payload.len() / 8);
    let mut values = Vec::with_capacity(payload.len() / 8);
    for chunk in payload.chunks_exact(8) {
        let idx = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        let val = f32::from_le_bytes([chunk[4], chunk[5], chunk[6], chunk[7]]);
        indices.push(idx);
        values.push(val);
    }
    SparseVectorValue::new(dimension, indices, values).map(VectorValue::Sparse)
}

fn decode_multi_payload(
    element: VectorElementType,
    dimension: u32,
    max_vectors: u32,
    count: u32,
    payload: &[u8],
) -> Result<VectorValue> {
    validation::validate_max_vectors(max_vectors)?;
    if count > max_vectors {
        return Err(VectorError::Corruption(format!(
            "multi count {count} exceeds cap {max_vectors}"
        )));
    }
    let want = (count as usize)
        .checked_mul(dimension as usize)
        .and_then(|n| n.checked_mul(4))
        .ok_or_else(|| VectorError::ResourceLimit("multi payload overflow".into()))?;
    if payload.len() != want {
        let n = payload.len();
        return Err(VectorError::Corruption(format!(
            "multi payload {n}, expected {want}"
        )));
    }
    let mut vectors = Vec::with_capacity(count as usize);
    for row in payload.chunks_exact(dimension as usize * 4) {
        let mut v = Vec::with_capacity(dimension as usize);
        for chunk in row.chunks_exact(4) {
            let x = f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            if !x.is_finite() {
                return Err(VectorError::NonFiniteValue(
                    "decoded lane non-finite".into(),
                ));
            }
            v.push(x);
        }
        vectors.push(v);
    }
    MultiVectorValue::new(element, dimension, max_vectors, vectors).map(VectorValue::Multi)
}
