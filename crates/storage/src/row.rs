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
//! Stable binary encoding for OLTP rows stored in page payloads.
//!
//! Layout version 1 is little-endian and consists of:
//!
//! ```text
//! header       12 bytes: magic[2], version[1], flags[1], field_count[u16],
//!                         null_bitmap_bytes[u16], total_length[u32]
//! null bitmap  ceil(field_count / 8) bytes; bit N means field N is NULL
//! fields       non-NULL fields in ordinal order:
//!              tag[u8] + i64[u64] for Integer
//!              tag[u8] + length[u32] + bytes for Bytes/String
//! ```
//!
//! `String` values are UTF-8. Flags are reserved and must be zero in version 1.
//! The encoded row is intended to fit in [`crate::PAGE_DATA_SIZE`]; overflow
//! rows and external storage are deferred to a later format revision.

use plomid_core::{ErrorKind, PlomidError, Result};

// Row-encoding constants are defined once in `plomid_core::constants`; the
// module-local names below keep the body of this file unchanged.
use plomid_core::{
    ROW_BYTES_TAG as BYTES_TAG, ROW_HEADER_SIZE as HEADER_SIZE, ROW_INTEGER_TAG as INTEGER_TAG,
    ROW_MAGIC as MAGIC, ROW_STRING_TAG as STRING_TAG, ROW_VERSION as VERSION,
};

/// One logical row field supported by the initial on-disk format.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Field {
    /// SQL NULL.
    Null,
    /// A signed 64-bit integer.
    Integer(i64),
    /// Arbitrary binary data.
    Bytes(Vec<u8>),
    /// UTF-8 variable-length text.
    String(String),
}

/// A sequence of fields encoded in ordinal order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Row {
    fields: Vec<Field>,
}

impl Row {
    /// Creates a row from its fields.
    #[must_use]
    pub fn new(fields: Vec<Field>) -> Self {
        Self { fields }
    }

    /// Returns the row fields in ordinal order.
    #[must_use]
    pub fn fields(&self) -> &[Field] {
        &self.fields
    }

    /// Consumes the row and returns its fields.
    #[must_use]
    pub fn into_fields(self) -> Vec<Field> {
        self.fields
    }

    /// Encodes this row using the version 1 layout documented above.
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode(self)
    }

    /// Decodes and validates one complete encoded row.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode(bytes)
    }
}

/// Encodes a row into an owned byte buffer.
pub fn encode(row: &Row) -> Result<Vec<u8>> {
    let field_count = u16::try_from(row.fields.len())
        .map_err(|_| PlomidError::new(ErrorKind::InvalidArgument, "row has too many fields"))?;
    let bitmap_bytes = usize::from(field_count).div_ceil(8);
    let bitmap_bytes_u16 = u16::try_from(bitmap_bytes).map_err(|_| {
        PlomidError::new(ErrorKind::InvalidArgument, "row null bitmap is too large")
    })?;
    let mut output = vec![0; HEADER_SIZE + bitmap_bytes];
    output[..2].copy_from_slice(&MAGIC);
    output[2] = VERSION;
    output[4..6].copy_from_slice(&field_count.to_le_bytes());
    output[6..8].copy_from_slice(&bitmap_bytes_u16.to_le_bytes());

    for (index, field) in row.fields.iter().enumerate() {
        match field {
            Field::Null => {
                output[HEADER_SIZE + index / 8] |= 1 << (index % 8);
            }
            Field::Integer(value) => {
                output.push(INTEGER_TAG);
                output.extend_from_slice(&value.to_le_bytes());
            }
            Field::Bytes(value) => append_variable(&mut output, BYTES_TAG, value)?,
            Field::String(value) => append_variable(&mut output, STRING_TAG, value.as_bytes())?,
        }
    }

    let total_length = u32::try_from(output.len())
        .map_err(|_| PlomidError::new(ErrorKind::InvalidArgument, "encoded row is too large"))?;
    output[8..12].copy_from_slice(&total_length.to_le_bytes());
    Ok(output)
}

fn append_variable(output: &mut Vec<u8>, tag: u8, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len())
        .map_err(|_| PlomidError::new(ErrorKind::InvalidArgument, "variable field is too large"))?;
    output.push(tag);
    output.extend_from_slice(&length.to_le_bytes());
    output.extend_from_slice(value);
    Ok(())
}

/// Decodes and validates one complete encoded row.
pub fn decode(bytes: &[u8]) -> Result<Row> {
    if bytes.len() < HEADER_SIZE {
        return Err(corruption("row header is truncated"));
    }
    if bytes[..2] != MAGIC {
        return Err(corruption("invalid row magic"));
    }
    if bytes[2] != VERSION {
        return Err(corruption("unsupported row format version"));
    }
    if bytes[3] != 0 {
        return Err(corruption("row contains unknown flags"));
    }
    let field_count = usize::from(u16::from_le_bytes([bytes[4], bytes[5]]));
    let bitmap_bytes = usize::from(u16::from_le_bytes([bytes[6], bytes[7]]));
    let expected_bitmap_bytes = field_count.div_ceil(8);
    if bitmap_bytes != expected_bitmap_bytes {
        return Err(corruption("row null bitmap length is invalid"));
    }
    let total_length = usize::try_from(u32::from_le_bytes([
        bytes[8], bytes[9], bytes[10], bytes[11],
    ]))
    .map_err(|_| corruption("row length is invalid"))?;
    if total_length != bytes.len() {
        return Err(corruption("row length does not match input"));
    }
    let fields_start = HEADER_SIZE
        .checked_add(bitmap_bytes)
        .ok_or_else(|| corruption("row header length overflow"))?;
    if fields_start > bytes.len() {
        return Err(corruption("row null bitmap is truncated"));
    }
    let bitmap = &bytes[HEADER_SIZE..fields_start];
    let mut cursor = fields_start;
    let mut fields = Vec::with_capacity(field_count);
    for index in 0..field_count {
        if bitmap[index / 8] & (1 << (index % 8)) != 0 {
            fields.push(Field::Null);
            continue;
        }
        let tag = take_u8(bytes, &mut cursor)?;
        match tag {
            INTEGER_TAG => {
                let raw = take_exact(bytes, &mut cursor, 8)?;
                fields.push(Field::Integer(i64::from_le_bytes(
                    raw.try_into()
                        .map_err(|_| corruption("invalid integer field"))?,
                )));
            }
            BYTES_TAG => fields.push(Field::Bytes(take_variable(bytes, &mut cursor)?)),
            STRING_TAG => {
                let value = take_variable(bytes, &mut cursor)?;
                let value = String::from_utf8(value)
                    .map_err(|_| corruption("string field is not valid UTF-8"))?;
                fields.push(Field::String(value));
            }
            _ => return Err(corruption("unknown row field tag")),
        }
    }
    if cursor != bytes.len() {
        return Err(corruption("row contains trailing bytes"));
    }
    Ok(Row { fields })
}

fn take_u8(bytes: &[u8], cursor: &mut usize) -> Result<u8> {
    let value = *bytes
        .get(*cursor)
        .ok_or_else(|| corruption("row field tag is truncated"))?;
    *cursor += 1;
    Ok(value)
}

fn take_exact<'a>(bytes: &'a [u8], cursor: &mut usize, length: usize) -> Result<&'a [u8]> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| corruption("row field length overflow"))?;
    let value = bytes
        .get(*cursor..end)
        .ok_or_else(|| corruption("row field is truncated"))?;
    *cursor = end;
    Ok(value)
}

fn take_variable(bytes: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let raw_length = take_exact(bytes, cursor, 4)?;
    let length = usize::try_from(u32::from_le_bytes(
        raw_length
            .try_into()
            .map_err(|_| corruption("invalid field length"))?,
    ))
    .map_err(|_| corruption("field length is invalid"))?;
    Ok(take_exact(bytes, cursor, length)?.to_vec())
}

fn corruption(message: &'static str) -> PlomidError {
    PlomidError::new(ErrorKind::Corruption, message)
}
