#![forbid(unsafe_code)]
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
//! Core primitives and shared kernel types for PLOMID.
//!
//! # Ownership
//!
//! Leaf crate: depends on nothing internal, so every kernel crate may
//! depend on it and none may be depended upon in return. Owns the stable
//! error contract (`ErrorKind` → `PL-*`), process-wide observability
//! counters, and shared constants. Never owns storage, transactions, or
//! wire behavior.

pub mod constants;
pub mod error;
pub mod observability;
pub mod types;

pub use constants::*;
pub use error::{ErrorKind, PlomidError, Result, SqlError, SqlResult};
pub use types::{
    BlockId, CatalogVersion, ColumnId, CommitTimestamp, DatabaseId, DeviceId, GenerationId,
    IndexId, Lsn, ObjectId, PackId, PageId, RowId, SchemaId, SegmentId, TableId, TableIdentity,
    Timestamp, TxId, TxnId, CSN,
};
