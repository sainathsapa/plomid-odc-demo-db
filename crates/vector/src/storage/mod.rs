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
//! Portable vector storage: pages, manifests, snapshots, adapters.
//!
//! NON-AUTHORITATIVE legacy helpers (see [`crate::durability`]).
//! Authoritative vector values live in storage-engine rows
//! (txn/MVCC/WAL/generations); nothing here is a second durable system of
//! record. The production SQL path never depends on these adapters: it
//! builds every ANN graph from snapshot-visible storage-engine rows.
//! `FileVectorStorage` and friends remain only for offline tests and as
//! explicitly rebuildable derived caches — deleting their files must never
//! lose data that the storage engine still holds.

pub mod adapter;
pub mod file;
pub mod manifest;
pub mod pages;
pub mod policy;
pub mod snapshot;

pub use adapter::{InMemoryVectorStorage, VectorStorageAdapter};
pub use file::{FileRecovery, FileVectorStorage};
pub use manifest::VectorManifest;
pub use pages::{Page, PageHeader, PageKind};
pub use policy::{
    ImageKind, ManagedVectorStorage, MmapConfig, StorageCapability, StorageClass, StorageMode,
    StorageModeRequest, StoragePolicyDecision, ValidatedImage, VectorStoragePolicy,
    DEFAULT_INDEX_MEMORY_BUDGET_BYTES, DEFAULT_MAX_MMAP_BYTES, MMAP_ENV_VAR, MMAP_MIN_BYTES,
    TINY_MAX_BYTES,
};
pub use snapshot::Snapshot;
