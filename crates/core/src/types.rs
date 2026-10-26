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
//! Strongly typed numeric primitives shared by PLOMID subsystems.
//!
//! These types intentionally do not implement conversions between one
//! another. In particular, a [`PageId`] cannot be passed where an [`Lsn`],
//! [`TxnId`], or [`Timestamp`] is expected. All `u64` values, including zero,
//! are currently representable; allocation and persistence layers may define
//! subsystem-specific validity rules later.

use std::fmt;

macro_rules! define_id {
    ($(#[$meta:meta])* $name:ident, $display_name:literal) => {
        $(#[$meta])*
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(u64);

        impl $name {
            /// Creates a value from its underlying counter.
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Creates a value from its underlying counter.
            #[must_use]
            pub const fn from_u64(value: u64) -> Self {
                Self::new(value)
            }

            /// Returns the underlying counter.
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }

            /// Returns the underlying counter.
            #[must_use]
            pub const fn as_u64(self) -> u64 {
                self.get()
            }

            /// Serializes to little-endian bytes.
            #[must_use]
            pub const fn to_le_bytes(self) -> [u8; 8] {
                self.0.to_le_bytes()
            }

            /// Deserializes from little-endian bytes.
            #[must_use]
            pub const fn from_le_bytes(bytes: [u8; 8]) -> Self {
                Self(u64::from_le_bytes(bytes))
            }

            /// Returns true when the underlying value is zero.
            #[must_use]
            pub const fn is_zero(self) -> bool {
                self.0 == 0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, concat!($display_name, "({})"), self.0)
            }
        }
    };
}

define_id!(
    /// Identifies a page in a physical storage structure.
    PageId,
    "PageId"
);

define_id!(
    /// Identifies a position in the write-ahead log.
    Lsn,
    "LSN"
);

define_id!(
    /// Identifies a transaction.
    TxnId,
    "TxnId"
);

/// Alias for [`TxnId`] using the architectural name `TxID`.
///
/// Both names refer to the same fixed-width 64-bit logical identifier.
pub type TxId = TxnId;

define_id!(
    /// Identifies a logical timestamp used for visibility and ordering.
    Timestamp,
    "Timestamp"
);

define_id!(
    /// Identifies a logical commit timestamp used for visibility and ordering.
    CommitTimestamp,
    "CommitTimestamp"
);

// Catalog identifiers are deliberately distinct types. A database ID must
// never be accepted where a table or schema ID is expected, especially once
// catalog records are replicated between nodes.
define_id!(
    /// Stable identity of a database catalog object.
    DatabaseId,
    "DatabaseId"
);
define_id!(
    /// Stable identity of a schema catalog object.
    SchemaId,
    "SchemaId"
);

define_id!(
    /// Stable identity of a table catalog object.
    TableId,
    "TableId"
);

/// Storage layout identity of a table directory.
///
/// A `TableIdentity` carries the full logical path of a table so that
/// generation, segment, and index paths can resolve under the
/// database → schema → table hierarchy without re-deriving it from
/// `ObjectId` alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableIdentity {
    /// Database that owns the schema owning the table.
    pub database_id: DatabaseId,
    /// Schema that owns the table.
    pub schema_id: SchemaId,
    /// Storage layout identity of the table directory.
    pub table_id: TableId,
}

impl TableIdentity {
    #[must_use]
    pub const fn new(database_id: DatabaseId, schema_id: SchemaId, table_id: TableId) -> Self {
        Self {
            database_id,
            schema_id,
            table_id,
        }
    }

    /// Derives the owning identity from an object ID and its schema ID.
    ///
    /// Preservation rule: pre-hierarchy objects used `object_id` as the
    /// table counter directly. The owning database is the default database
    /// (`DatabaseId(1)`), which is the only database that such legacy
    /// objects can belong to. This keeps every existing publication,
    /// bench, and test compiling without inventing ownership.
    #[must_use]
    pub const fn from_object_and_schema(object_id: ObjectId, schema_id: SchemaId) -> Self {
        Self {
            database_id: DatabaseId::new(1),
            schema_id,
            table_id: TableId::new(object_id.get()),
        }
    }

    /// Returns true when any component is zero (invalid).
    #[must_use]
    pub fn is_zero(self) -> bool {
        self.database_id.is_zero() || self.schema_id.is_zero() || self.table_id.is_zero()
    }
}

impl Default for TableIdentity {
    fn default() -> Self {
        Self::new(DatabaseId::new(0), SchemaId::new(0), TableId::new(0))
    }
}
define_id!(
    /// Stable identity of a column catalog object.
    ColumnId,
    "ColumnId"
);
define_id!(
    /// Stable identity of an index catalog object.
    IndexId,
    "IndexId"
);
define_id!(
    /// Monotonic catalog metadata version.
    CatalogVersion,
    "CatalogVersion"
);

// ---------------------------------------------------------------------------
// Object IDs are allocated from a database-local counter and are stable for
// the lifetime of the object. They do not encode physical location.
define_id!(
    /// Unique identity of a persistent database object.
    ObjectId,
    "ObjectId"
);

// Generation counter for an object or storage structure.
//
// When a persistent structure is rebuilt, replaced, or relocated, its
// generation ID changes so that stale references can be detected without
// scanning the structure itself.
define_id!(
    /// Generation counter for a persistent structure.
    GenerationId,
    "GenerationId"
);

// Unique identity of a storage segment (a contiguous on-disk region).
//
// Segment IDs are assigned by the storage manager and are stable for the
// lifetime of the segment. They do not encode filesystem paths.
define_id!(
    /// Unique identity of a storage segment.
    SegmentId,
    "SegmentId"
);

// Unique identity of a server pack (a .dat container file).
//
// Pack IDs are assigned when a pack is created and are stable for its
// lifetime. The same pack file may be reopened on a different device; the
// pack ID remains the same.
define_id!(
    /// Unique identity of a server pack.
    PackId,
    "PackId"
);

// Unique identity of a logical block (256 KiB = 16 pages).
//
// Block IDs are stable for the lifetime of the block. They do not encode
// byte offsets; physical offset is resolved through pack metadata.
define_id!(
    /// Unique identity of a logical block.
    BlockId,
    "BlockId"
);

// Unique identity of a physical storage device.
//
// Device IDs are logical identifiers assigned when a device is created and
// remain stable for the device lifetime. They never encode filesystem paths,
// filenames, byte offsets, capacity, or platform-specific information;
// physical location is resolved separately by the storage layer.
define_id!(
    /// Unique identity of a physical storage device.
    DeviceId,
    "DeviceId"
);

// Logical row identifier.
//
// Row IDs are assigned by the storage engine when a row is inserted and are
// stable for the lifetime of the row within a generation. They do not encode
// physical location; page/offset resolution happens through storage metadata.
define_id!(
    /// Logical row identifier.
    RowId,
    "RowId"
);

// Commit sequence number.
//
// Monotonically increasing number assigned to each committed transaction,
// used for commit ordering and visibility checks. Distinct from LSN (which
// is a WAL position) and from CommitTimestamp (which is a wall-clock proxy).
define_id!(
    /// Commit sequence number.
    CSN,
    "CSN"
);
