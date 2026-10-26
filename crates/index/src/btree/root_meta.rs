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
//! Root-metadata block for a persistent SQL index.
//!
//! Stored at page 0, this block identifies *which* index a file holds and
//! where the B+Tree root lives. It is the persistence boundary for future
//! generation integration: the `generation` field binds the index to a data
//! generation so readers cannot combine incompatible generations.
//!
//! Layout (52 bytes, little-endian):
//! ```text
//! magic[4]        = "PLRT"     identifier for the root block
//! version[u32]    = 1          index format version
//! flags[u32]      = bitset     (bit 0 = INDEX_FLAG_UNIQUE)
//! index_id[u64]   = IndexId    SQL index identity
//! object_id[u64]  = ObjectId   owning table identity
//! generation[u64] = GenerationId generation token
//! root_page[u64]  = PageId     current B+Tree root page
//! entry_count[u64]= usize      total index entries
//! ```
//! The whole block is CRC32C-protected by the surrounding [`Page`].

use plomid_core::{ErrorKind, GenerationId, IndexId, ObjectId, PageId, PlomidError, Result};

use super::constants::{
    INDEX_FLAG_MASK, INDEX_FLAG_UNIQUE, INDEX_FORMAT_VERSION, ROOT_MAGIC, ROOT_META_SIZE,
};

/// On-disk representation of the root-metadata block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RootMeta {
    pub version: u32,
    pub flags: u32,
    pub index_id: IndexId,
    pub object_id: ObjectId,
    pub generation: GenerationId,
    pub root_page: PageId,
    pub entry_count: u64,
}

impl RootMeta {
    /// Returns true if this index enforces key uniqueness.
    pub fn is_unique(&self) -> bool {
        self.flags & INDEX_FLAG_UNIQUE != 0
    }

    /// Encodes the metadata block into a fixed-width little-endian byte vector.
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.version != INDEX_FORMAT_VERSION {
            return Err(PlomidError::new(
                ErrorKind::Unsupported,
                "unsupported index format version",
            ));
        }
        if self.flags & !INDEX_FLAG_MASK != 0 {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                "unknown index flags in root metadata",
            ));
        }
        let mut buffer = vec![0u8; ROOT_META_SIZE];
        buffer[..4].copy_from_slice(&ROOT_MAGIC);
        buffer[4..8].copy_from_slice(&self.version.to_le_bytes());
        buffer[8..12].copy_from_slice(&self.flags.to_le_bytes());
        buffer[12..20].copy_from_slice(&self.index_id.to_le_bytes());
        buffer[20..28].copy_from_slice(&self.object_id.to_le_bytes());
        buffer[28..36].copy_from_slice(&self.generation.to_le_bytes());
        buffer[36..44].copy_from_slice(&self.root_page.to_le_bytes());
        buffer[44..52].copy_from_slice(&self.entry_count.to_le_bytes());
        Ok(buffer)
    }

    /// Decodes and validates a root-metadata block from raw bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < ROOT_META_SIZE {
            return Err(PlomidError::new(
                ErrorKind::Corruption,
                "root metadata block is truncated",
            ));
        }
        if data[..4] != ROOT_MAGIC {
            return Err(PlomidError::new(
                ErrorKind::Corruption,
                "invalid root metadata magic",
            ));
        }
        let version = u32::from_le_bytes(data[4..8].try_into().unwrap());
        if version != INDEX_FORMAT_VERSION {
            return Err(PlomidError::new(
                ErrorKind::Unsupported,
                "unsupported index format version",
            ));
        }
        let flags = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let index_id = IndexId::new(u64::from_le_bytes(data[12..20].try_into().unwrap()));
        let object_id = ObjectId::new(u64::from_le_bytes(data[20..28].try_into().unwrap()));
        let generation = GenerationId::new(u64::from_le_bytes(data[28..36].try_into().unwrap()));
        let root_page = PageId::new(u64::from_le_bytes(data[36..44].try_into().unwrap()));
        let entry_count = u64::from_le_bytes(data[44..52].try_into().unwrap());
        Ok(RootMeta {
            version,
            flags,
            index_id,
            object_id,
            generation,
            root_page,
            entry_count,
        })
    }
}
