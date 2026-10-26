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
//! Portable storage pages for vector payloads.
//!
//! NON-AUTHORITATIVE derived encoding (see [`crate::durability`]): page
//! framing for rebuildable images only, never a second page store of record.
//! Authoritative pages live in the storage engine.
//!
//! Layout is fully explicit little-endian; nothing depends on
//! struct `repr` or host endianness. Every page is `PAGE_SIZE`
//! bytes on disk: a 32-byte header followed by payload bytes
//! (zero-padded to the page boundary).

use crate::{Result, VectorError};

/// On-disk page size in bytes.
pub const PAGE_SIZE: usize = 8192;
/// Length of the encoded page header in bytes.
pub const PAGE_HEADER_LEN: usize = 32;
/// Page magic: ASCII `PLMV`.
pub const PAGE_MAGIC: [u8; 4] = *b"PLMV";
/// Current page format version.
pub const PAGE_FORMAT_VERSION: u16 = 1;

/// Maximum payload bytes that fit in one page.
pub const PAGE_MAX_PAYLOAD: usize = PAGE_SIZE - PAGE_HEADER_LEN;

/// Logical page kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PageKind {
    /// Row-payload data page.
    Data = 1,
    /// Manifest page.
    Manifest = 2,
    /// Index page.
    Index = 3,
}

impl PageKind {
    fn to_u16(self) -> u16 {
        self as u16
    }

    fn from_u16(v: u16) -> Result<Self> {
        match v {
            1 => Ok(Self::Data),
            2 => Ok(Self::Manifest),
            3 => Ok(Self::Index),
            other => Err(VectorError::Corruption(format!(
                "unknown page kind {other}"
            ))),
        }
    }
}

/// Decoded page header (host struct only; encoding is manual LE).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    /// Page format version (must equal [`PAGE_FORMAT_VERSION`]).
    pub version: u16,
    /// Logical page kind.
    pub kind: PageKind,
    /// Stable page identifier.
    pub page_id: u64,
    /// LSN of the last write applied to this page.
    pub lsn: u64,
    /// Payload length in bytes (`<= PAGE_MAX_PAYLOAD`).
    pub payload_len: u32,
    /// `checksum32` of the payload bytes.
    pub checksum: u32,
}

impl PageHeader {
    /// Build a header for a payload of `payload_len` bytes.
    #[must_use]
    pub fn new(kind: PageKind, page_id: u64, lsn: u64, payload_len: u32, checksum: u32) -> Self {
        Self {
            version: PAGE_FORMAT_VERSION,
            kind,
            page_id,
            lsn,
            payload_len,
            checksum,
        }
    }

    /// Encode to exactly [`PAGE_HEADER_LEN`] bytes (little-endian).
    #[must_use]
    pub fn encode(&self) -> [u8; PAGE_HEADER_LEN] {
        let mut out = [0u8; PAGE_HEADER_LEN];
        out[0..4].copy_from_slice(&PAGE_MAGIC);
        out[4..6].copy_from_slice(&self.version.to_le_bytes());
        out[6..8].copy_from_slice(&self.kind.to_u16().to_le_bytes());
        out[8..16].copy_from_slice(&self.page_id.to_le_bytes());
        out[16..24].copy_from_slice(&self.lsn.to_le_bytes());
        out[24..28].copy_from_slice(&self.payload_len.to_le_bytes());
        out[28..32].copy_from_slice(&self.checksum.to_le_bytes());
        out
    }

    /// Decode and validate a header slice (`>= PAGE_HEADER_LEN` bytes).
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < PAGE_HEADER_LEN {
            return Err(VectorError::Corruption(format!(
                "page header too short: {} < {PAGE_HEADER_LEN}",
                bytes.len()
            )));
        }
        if bytes[0..4] != PAGE_MAGIC {
            return Err(VectorError::Corruption("bad page magic".to_string()));
        }
        let version = u16::from_le_bytes([bytes[4], bytes[5]]);
        if version != PAGE_FORMAT_VERSION {
            return Err(VectorError::Corruption(format!(
                "unsupported page version {version}"
            )));
        }
        let kind = PageKind::from_u16(u16::from_le_bytes([bytes[6], bytes[7]]))?;
        let page_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap_or([0; 8]));
        let lsn = u64::from_le_bytes(bytes[16..24].try_into().unwrap_or([0; 8]));
        let payload_len = u32::from_le_bytes(bytes[24..28].try_into().unwrap_or([0; 4]));
        let checksum = u32::from_le_bytes(bytes[28..32].try_into().unwrap_or([0; 4]));
        if (payload_len as usize) > PAGE_MAX_PAYLOAD {
            return Err(VectorError::Corruption(format!(
                "payload_len {} exceeds max {PAGE_MAX_PAYLOAD}",
                payload_len
            )));
        }
        Ok(Self {
            version,
            kind,
            page_id,
            lsn,
            payload_len,
            checksum,
        })
    }
}

/// FNV-1a 32-bit checksum (portable, dependency-free).
#[must_use]
pub fn checksum32(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for &b in data {
        hash ^= u32::from(b);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// An in-memory page: header plus owned payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page {
    /// Validated page header.
    pub header: PageHeader,
    /// Payload bytes (`header.payload_len` long).
    pub payload: Vec<u8>,
}

impl Page {
    /// Build a page, computing the payload checksum.
    pub fn new(kind: PageKind, page_id: u64, lsn: u64, payload: Vec<u8>) -> Result<Self> {
        if payload.len() > PAGE_MAX_PAYLOAD {
            return Err(VectorError::IndexParameterInvalid(format!(
                "payload {} exceeds page capacity {PAGE_MAX_PAYLOAD}",
                payload.len()
            )));
        }
        let header = PageHeader::new(
            kind,
            page_id,
            lsn,
            payload.len() as u32,
            checksum32(&payload),
        );
        Ok(Self { header, payload })
    }

    /// Serialize to exactly [`PAGE_SIZE`] bytes (header + payload + zero pad).
    #[must_use]
    pub fn to_buffer(&self) -> Vec<u8> {
        let mut out = vec![0u8; PAGE_SIZE];
        out[0..PAGE_HEADER_LEN].copy_from_slice(&self.header.encode());
        out[PAGE_HEADER_LEN..PAGE_HEADER_LEN + self.payload.len()].copy_from_slice(&self.payload);
        out
    }

    /// Parse and validate a `PAGE_SIZE`-byte buffer.
    pub fn from_buffer(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < PAGE_SIZE {
            return Err(VectorError::Corruption(format!(
                "page buffer too short: {} < {PAGE_SIZE}",
                bytes.len()
            )));
        }
        let header = PageHeader::decode(&bytes[0..PAGE_HEADER_LEN])?;
        let end = PAGE_HEADER_LEN + header.payload_len as usize;
        let payload = bytes[PAGE_HEADER_LEN..end].to_vec();
        if checksum32(&payload) != header.checksum {
            return Err(VectorError::Corruption(
                "page checksum mismatch".to_string(),
            ));
        }
        Ok(Self { header, payload })
    }
}
