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
//! Type modifier (`typmod`) parsing, encoding and validation.
//!
//! PostgreSQL encodes typmods as an `i32`; `NO_TYPEMOD` (-1) means
//! unspecified. This module parses SQL-level typmod lists (e.g. `(10,2)`),
//! computes the binary encoding, and validates values against the modifier.

use crate::value::PgValue;
use crate::{ColumnType, PgType};

// Typmod constants are defined once in `plomid_core::constants` and
// re-exported here so the public `plomid_types::` paths keep working.
pub use plomid_core::{
    MAX_LENGTH, MAX_TIME_PRECISION, NO_TYPEMOD, NUMERIC_MAX_PRECISION, NUMERIC_MAX_SCALE,
};

/// A parsed SQL-level typmod: list of modifier arguments.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TypmodSpec {
    /// Modifier arguments, e.g. `[10, 2]` for `numeric(10,2)`.
    pub args: [i32; 2],
    /// How many entries of `args` are meaningful.
    pub count: usize,
}

impl TypmodSpec {
    /// Parses `(p, s)` / `(n)` argument lists.
    pub fn parse(text: &str) -> Result<Self, String> {
        let text = text.trim();
        if text.is_empty() {
            return Ok(Self {
                args: [0, 0],
                count: 0,
            });
        }
        let bad = || format!("invalid typmod list: {text}");
        let inner = text
            .strip_prefix('(')
            .and_then(|t| t.strip_suffix(')'))
            .ok_or_else(bad)?;
        let parts: Vec<&str> = inner.split(',').map(str::trim).collect();
        if parts.len() > 2 {
            return Err(bad());
        }
        let mut args = [0i32; 2];
        for (i, part) in parts.iter().enumerate() {
            if part.is_empty() {
                return Err(bad());
            }
            args[i] = part.parse().map_err(|_| bad())?;
            if args[i] < 0 {
                return Err("typmod must not be negative".into());
            }
        }
        Ok(Self {
            args,
            count: parts.len(),
        })
    }
}

/// Encodes a typmod spec for a type into the binary typmod.
///
/// # Errors
/// Returns a PostgreSQL-style message when the modifier is invalid for the
/// type (wrong arity, out-of-range precision/length).
pub fn encode_typmod(ty: PgType, spec: TypmodSpec) -> Result<i32, String> {
    match ty {
        PgType::Vector => encode_vector_typmod(spec),
        PgType::VarChar | PgType::BpChar => match spec.count {
            0 => Ok(NO_TYPEMOD),
            1 => {
                let n = spec.args[0] as u32;
                if n == 0 || n > MAX_LENGTH {
                    Err(format!(
                        "length for type {ty} must be between 1 and {MAX_LENGTH}"
                    ))
                } else {
                    Ok(spec.args[0] + 4)
                }
            }
            _ => Err(format!("invalid modifiers for type {ty}")),
        },
        PgType::Char => match spec.count {
            0 => Ok(NO_TYPEMOD),
            1 => Ok(spec.args[0] + 4),
            _ => Err("invalid modifiers for type char".into()),
        },
        PgType::Bit | PgType::VarBit => match spec.count {
            0 => Ok(NO_TYPEMOD),
            1 => {
                let n = spec.args[0] as u32;
                if n == 0 || n > MAX_LENGTH {
                    Err(format!(
                        "length for type {ty} must be between 1 and {MAX_LENGTH}"
                    ))
                } else {
                    Ok(spec.args[0])
                }
            }
            _ => Err(format!("invalid modifiers for type {ty}")),
        },
        PgType::Numeric => encode_numeric_typmod(spec),
        PgType::Time
        | PgType::TimeTz
        | PgType::Timestamp
        | PgType::Timestamptz
        | PgType::Interval => encode_time_typmod(spec),
        _ => {
            if spec.count == 0 {
                Ok(NO_TYPEMOD)
            } else {
                Err(format!("type {ty} does not accept type modifiers"))
            }
        }
    }
}

fn encode_vector_typmod(spec: TypmodSpec) -> Result<i32, String> {
    // VECTOR typmods carry the full kind/element/dimension identity packed
    // by the authoritative `plomid-vector` scheme (kind 31..24, elem 23..16,
    // dim-1 15..0). The single-int form here is the dense FLOAT32 shorthand
    // `VECTOR(n)`; full forms go through the SQL parser directly.
    match spec.count {
        0 => Err("type vector requires a dimension, e.g. VECTOR(FLOAT32, 1536)".into()),
        1 => {
            let dim = spec.args[0];
            plomid_vector::VectorType::new(
                plomid_vector::VectorKind::Dense,
                plomid_vector::VectorElementType::Float32,
                u32::try_from(dim).map_err(|_| format!("invalid vector dimension {dim}"))?,
            )
            .map_err(|e| e.to_string())?
            .typmod_encode()
            .map_err(|e| e.to_string())
        }
        _ => Err("invalid modifiers for type vector".into()),
    }
}

fn encode_numeric_typmod(spec: TypmodSpec) -> Result<i32, String> {
    let check_precision = |p: u16| {
        if !(1..=NUMERIC_MAX_PRECISION).contains(&p) {
            Err(format!(
                "NUMERIC precision {p} must be between 1 and {NUMERIC_MAX_PRECISION}"
            ))
        } else {
            Ok(())
        }
    };
    match spec.count {
        0 => Ok(NO_TYPEMOD),
        1 => {
            let p = spec.args[0] as u16;
            check_precision(p)?;
            Ok(((p as i32) << 16) | i32::from(NUMERIC_MAX_SCALE))
        }
        2 => {
            let (p, s) = (spec.args[0] as u16, spec.args[1] as u16);
            check_precision(p)?;
            if s > p {
                Err(format!(
                    "NUMERIC scale {s} must be between 0 and precision {p}"
                ))
            } else {
                Ok(((p as i32) << 16) | i32::from(s))
            }
        }
        _ => Err("invalid modifiers for type numeric".into()),
    }
}

fn encode_time_typmod(spec: TypmodSpec) -> Result<i32, String> {
    match spec.count {
        0 => Ok(NO_TYPEMOD),
        1 => {
            let p = spec.args[0] as u16;
            if p > MAX_TIME_PRECISION {
                Err(format!(
                    "precision {p} must be between 0 and {MAX_TIME_PRECISION}"
                ))
            } else {
                Ok(i32::from(p))
            }
        }
        _ => Err("too many type modifiers".into()),
    }
}

/// Re-decodes a binary typmod into `(primary, secondary)` values:
/// `(precision, scale)` for `numeric`, `(length, 0)` for strings/bits,
/// `(precision, 0)` for datetimes. `None` when no typmod is set.
#[must_use]
pub fn decode_typmod(ty: PgType, typmod: i32) -> Option<(u16, u16)> {
    if typmod == NO_TYPEMOD {
        return None;
    }
    match ty {
        PgType::Numeric => Some(((typmod >> 16) as u16, (typmod & 0xFFFF) as u16)),
        // Packed vector identity decodes to (dimension, 0); full
        // kind/element identity is available via `VectorType::typmod_decode`.
        PgType::Vector => plomid_vector::VectorType::typmod_decode(typmod)
            .ok()
            .map(|t| (t.dimension.min(65535) as u16, 0)),
        PgType::VarChar | PgType::BpChar | PgType::Char => Some(((typmod - 4).max(0) as u16, 0)),
        PgType::Bit | PgType::VarBit => Some((typmod.max(0) as u16, 0)),
        PgType::Time
        | PgType::TimeTz
        | PgType::Timestamp
        | PgType::Timestamptz
        | PgType::Interval => Some((typmod.max(0) as u16, 0)),
        _ => None,
    }
}

/// Validates a vector value against the full column type: typmod identity
/// (kind/element/dimension) plus the out-of-band multi-vector cap.
///
/// This is the DML choke point (`row.rs` calls it for every written
/// vector): the packed typmod cannot carry `max_vectors`, so the column
/// type's dedicated field is the authority. Non-multi columns ignore it.
pub fn apply_column_typmod(value: PgValue, col_type: &ColumnType) -> Result<PgValue, String> {
    let value = apply_typmod(value, col_type.typmod, PgType::Vector)?;
    if let PgValue::Vector(plomid_vector::VectorValue::Multi(m)) = &value {
        if m.count() > col_type.vector_max_vectors {
            return Err(format!(
                "vector count {} exceeds max_vectors {}",
                m.count(),
                col_type.vector_max_vectors
            ));
        }
    }
    Ok(value)
}

/// Validates a value against a typmod, coercing where PostgreSQL does
/// (blank-padding `char(n)`, rounding fractional seconds, rescaling).
///
/// # Errors
/// Returns an error for over-length strings/bit values or numeric overflow.
pub fn apply_typmod(value: PgValue, typmod: i32, ty: PgType) -> Result<PgValue, String> {
    if typmod == NO_TYPEMOD {
        return Ok(value);
    }
    match (&value, ty) {
        (PgValue::VarChar(s), PgType::VarChar | PgType::BpChar) => {
            let max = (typmod - 4) as usize;
            if s.chars().count() > max {
                Err(format!("value too long for type {}", ty.name()))
            } else {
                Ok(value)
            }
        }
        (PgValue::BpChar(s), PgType::BpChar) => {
            let max = (typmod - 4) as usize;
            let len = s.chars().count();
            if len > max {
                Err(format!("value too long for type {}", ty.name()))
            } else if len < max {
                let mut padded = s.clone();
                padded.push_str(&" ".repeat(max - len));
                Ok(PgValue::BpChar(padded))
            } else {
                Ok(value)
            }
        }
        (PgValue::Numeric(n), PgType::Numeric) => {
            let precision = (typmod >> 16) as u16;
            let scale = (typmod & 0xFFFF) as u16;
            // Round to the requested scale (PostgreSQL-compatible rounding)
            let rounded = n.clone().round_to_scale(scale);
            if rounded.mantissa.unsigned_abs().to_string().len() as u16 > precision {
                Err(format!(
                    "numeric field overflow (precision {precision}, scale {scale})"
                ))
            } else {
                Ok(PgValue::Numeric(rounded))
            }
        }
        (PgValue::Time(micros), PgType::Time) => Ok(PgValue::Time(round_micros(*micros, typmod))),
        (
            PgValue::TimeTz {
                micros,
                offset_secs,
            },
            PgType::TimeTz,
        ) => Ok(PgValue::TimeTz {
            micros: round_micros(*micros, typmod),
            offset_secs: *offset_secs,
        }),
        (PgValue::Timestamp(t), PgType::Timestamp) => {
            Ok(PgValue::Timestamp(round_micros(*t, typmod)))
        }
        (PgValue::Timestamptz(t), PgType::Timestamptz) => {
            Ok(PgValue::Timestamptz(round_micros(*t, typmod)))
        }
        (PgValue::Bit { len, bytes: _ }, PgType::Bit) => {
            if *len != typmod as u32 {
                Err(format!(
                    "bit string length {len} does not match type {}",
                    ty.name()
                ))
            } else {
                Ok(value)
            }
        }
        (PgValue::Bit { len, .. }, PgType::VarBit) => {
            if i64::from(*len) > i64::from(typmod) {
                Err(format!("bit string length {len} exceeds maximum {typmod}"))
            } else {
                Ok(value)
            }
        }
        (PgValue::Vector(v), PgType::Vector) => {
            // Full kind/element/dimension identity check, owned by the
            // vector crate: decode the column typmod and compare against
            // the value's own shape.
            let declared = plomid_vector::VectorType::typmod_decode(typmod)
                .map_err(|e| format!("invalid vector typmod: {e}"))?;
            if v.kind() != declared.kind {
                return Err(format!(
                    "vector kind mismatch: value is {:?}, column is {:?}",
                    v.kind(),
                    declared.kind
                ));
            }
            if v.dimension() != declared.dimension {
                return Err(format!(
                    "vector dimension {} does not match column dimension {}",
                    v.dimension(),
                    declared.dimension
                ));
            }
            // Dense/multi lanes travel as f32 working precision; retag them
            // with the declared element (quantization/packing is a codec
            // concern). Sparse/binary carry no per-lane element to coerce.
            match v {
                plomid_vector::VectorValue::Dense(d) if d.element != declared.element => {
                    // Integer elements enforce range + integrality here;
                    // float elements accept any finite working-precision lane.
                    plomid_vector::validation::check_element_lanes(declared.element, &d.values_f32)
                        .map_err(|e| format!("invalid vector value: {e}"))?;
                    let retagged = plomid_vector::DenseVectorValue::from_f32(
                        declared.element,
                        d.values_f32.clone(),
                    )
                    .map_err(|e| format!("invalid vector value: {e}"))?;
                    Ok(PgValue::Vector(plomid_vector::VectorValue::Dense(retagged)))
                }
                plomid_vector::VectorValue::Multi(m) if m.element != declared.element => {
                    // NOTE: the declared multi `max_vectors` cap travels
                    // out-of-band in the catalog (typmod carries kind/elem/dim
                    // only); until that catalog field lands, the value's own
                    // construction-time cap applies. Enforced at DDL parse.
                    for row in &m.vectors {
                        plomid_vector::validation::check_element_lanes(declared.element, row)
                            .map_err(|e| format!("invalid vector value: {e}"))?;
                    }
                    let retagged = plomid_vector::MultiVectorValue::new(
                        declared.element,
                        m.dimension,
                        m.max_vectors,
                        m.vectors.clone(),
                    )
                    .map_err(|e| format!("invalid vector value: {e}"))?;
                    Ok(PgValue::Vector(plomid_vector::VectorValue::Multi(retagged)))
                }
                _ => Ok(value),
            }
        }
        _ => Ok(value),
    }
}

fn round_micros(micros: i64, typmod: i32) -> i64 {
    let precision = typmod.max(0) as u32;
    if precision >= u32::from(MAX_TIME_PRECISION) {
        return micros;
    }
    let factor = 10_i64.pow(6 - precision);
    // PostgreSQL's `AdjustTimestampForTypmod` rounds fractional seconds to
    // the nearest representable unit (half up on the remainder), NOT truncation.
    // Rounding lets `23:59:59.999999` at precision 0 carry into the next second
    // ('24:00:00' for time, the next day for timestamp), matching PostgreSQL 17.
    ((micros + factor / 2) / factor) * factor
}
