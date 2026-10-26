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
//! Durable device identity and capacity record.
//!
//! `DeviceRecord` lives in `devices/D-*/DEVICE.dat`. It is *not* a duplicate of
//! the physical device header that [`StorageDevice`] manages inside its own file:
//! the physical header holds runtime capacity and allocation state, while
//! `DEVICE.dat` is the *registered* identity and capacity of the device as seen
//! by the database-wide device registry. The registry validates that the record
//! and the physical header agree before a device is admitted for placement.
//!
//! Record format (little-endian). Every field has an explicit offset and width,
//! so no Rust struct layout is observable on disk:
//!
//! ```text
//! offset  size  field
//!      0     4  magic       "PLDV", device record tag
//!      4     4  version     device metadata format version
//!      8     4  header_len  total bytes of this record (40)
//!     12     8  device_id   logical device identity
//!     20     8  capacity    configured device capacity in bytes
//!     28     8  created_at  creation stamp (seconds since the Unix epoch)
//!     36     4  checksum    CRC32C over bytes[0..36]
//! ```
//!
//! The checksum is the final field and covers every byte that precedes it, so a
//! record whose identity, capacity, or creation stamp was altered is rejected.

use crate::checksum::{compute_checksum, verify_checksum};
use crate::codec::{put_u32, put_u64};
use crate::layout::{metadata as meta, DatabaseLayout};
use plomid_core::{DeviceId, ErrorKind, PlomidError, Result};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Magic for every `DEVICE.dat` record.
pub const DEVICE_META_MAGIC: [u8; 4] = *b"PLDV";
/// Format version of the `DEVICE.dat` record.
pub const DEVICE_META_VERSION: u32 = 1;
/// Total on-disk length of a `DEVICE.dat` record.
pub const DEVICE_META_HEADER_LEN: usize = 40;
/// Byte offset of the checksum field within a `DEVICE.dat` record.
const CHECKSUM_OFFSET: usize = 36;

/// Durable record identifying one physical device.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceRecord {
    /// Logical identity of the device.
    pub device_id: DeviceId,
    /// Configured capacity of the device in bytes.
    pub capacity: u64,
    /// Creation stamp (seconds since the Unix epoch).
    pub created_at: u64,
}

impl DeviceRecord {
    /// Creates a record for a new device with the current time.
    pub fn new(device_id: DeviceId, capacity: u64) -> Result<Self> {
        if device_id.is_zero() {
            return Err(PlomidError::with_detail(
                ErrorKind::Corruption,
                "device identity is zero",
                String::new(),
            ));
        }
        if capacity == 0 {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                "device capacity is zero",
            ));
        }
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        Ok(Self {
            device_id,
            capacity,
            created_at,
        })
    }

    /// Encodes the record deterministically.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(DEVICE_META_HEADER_LEN);
        meta::encode_prefix(
            &mut out,
            DEVICE_META_MAGIC,
            DEVICE_META_VERSION,
            DEVICE_META_HEADER_LEN as u32,
        );
        put_u64(&mut out, self.device_id.get());
        put_u64(&mut out, self.capacity);
        put_u64(&mut out, self.created_at);
        // The checksum is the last field and covers every preceding byte.
        let checksum = compute_checksum(&out[..CHECKSUM_OFFSET]);
        put_u32(&mut out, checksum);
        Ok(out)
    }

    /// Decodes and validates a record image.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (mut cursor, header_len) = meta::decode_prefix(
            bytes,
            DEVICE_META_MAGIC,
            DEVICE_META_VERSION,
            "device metadata",
        )?;
        meta::ensure_header_len(bytes, header_len, "device metadata")?;
        let device_id = DeviceId::new(cursor.u64("device metadata")?);
        let capacity = cursor.u64("device metadata")?;
        let created_at = cursor.u64("device metadata")?;
        let stored = cursor.u32("device metadata")?;
        verify_checksum(&bytes[..CHECKSUM_OFFSET], stored)?;
        meta::ensure_reserved_zero(&bytes[cursor.position()..header_len], "device metadata")?;
        if device_id.is_zero() {
            return Err(PlomidError::with_detail(
                ErrorKind::Corruption,
                "device identity is zero",
                String::new(),
            ));
        }
        Ok(Self {
            device_id,
            capacity,
            created_at,
        })
    }

    /// Publishes the record atomically at `path`.
    pub fn publish(&self, path: &Path) -> Result<()> {
        let bytes = self.encode()?;
        // VERIFY: a record that cannot be decoded is never published.
        Self::decode(&bytes)?;
        meta::publish(path, &bytes)
    }
}

/// Reads a device record, reporting absence as `ErrorKind::NotFound`.
pub fn read_device_record(path: &Path) -> Result<DeviceRecord> {
    let bytes = meta::read(path, "device metadata")?;
    DeviceRecord::decode(&bytes)
}

/// Reads a device record, reporting absence as `Ok(None)`.
pub fn read_device_record_optional(path: &Path) -> Result<Option<DeviceRecord>> {
    match meta::read_optional(path)? {
        Some(bytes) => DeviceRecord::decode(&bytes).map(Some),
        None => Ok(None),
    }
}

/// Path of the `DEVICE.dat` record for the given device.
pub fn device_record_path(layout: &DatabaseLayout, device_id: DeviceId) -> PathBuf {
    layout.device_meta_path(device_id)
}
