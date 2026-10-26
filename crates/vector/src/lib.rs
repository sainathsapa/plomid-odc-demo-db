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
//! `plomid-vector`: first-class dense / sparse / multi / binary vector
//! support for the PLOMID HTAP database.
//!
//! This crate owns all vector semantics, math-adjacent validation, storage
//! codecs, search options, index lifecycle types, and planner interfaces.
//! It depends only on `std` and never on SQL, catalog, or storage crates.
//!
//! Durability convergence ([`durability`]): authoritative vector values live
//! in storage-engine rows (txn/MVCC/WAL/generations); vector index payloads
//! are derived, non-authoritative, rebuildable artifacts gated by
//! [`freshness`], and ANN is acceleration only. Nothing in this crate is a
//! second authoritative WAL, MVCC, backup, recovery, or GC system.
//!
//! Quick start:
//! ```rust
//! use plomid_vector::{VectorType, parse_dense, encode_standalone, decode_standalone};
//!
//! let t = VectorType::parse_sql("VECTOR(FLOAT32, 2)").unwrap();
//! let v = parse_dense("[1, 2]", Some(t.dimension)).unwrap();
//! let bytes = encode_standalone(&plomid_vector::VectorValue::Dense(v)).unwrap();
//! let back = decode_standalone(&bytes).unwrap();
//! assert_eq!(format!("{back:?}").len() > 0, true);
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod admin;
pub mod backend;
pub mod backup;
pub mod capability;
pub mod codec;
pub mod distance;
pub mod durability;
pub mod error;
pub mod format;
pub mod freshness;
pub mod htap;
pub mod hybrid;
pub mod index;
pub mod literal;
pub mod math;
pub mod metrics;
pub mod migration;
pub mod multivector;
pub mod mvcc;
pub mod numa;
pub mod operator;
pub mod options;
pub mod planner_api;
pub mod scale;
pub mod search;
pub mod sparse;
pub mod stats;
pub mod storage;
pub mod r#type;
pub mod validation;
pub mod value;
pub mod wire;

pub use admin::{IndexManifest, IndexState, IndexStats};
pub use backup::{
    capture_mode, create_backup, open_restored_diskann, restore_backup, restore_storage_policy,
    validate_restored_blob, BackupFailPoint, BackupFileEntry, BackupIndexEntry, BackupManifest,
    BackupReport, IndexBackupInput, IndexRestoreAction, IndexRestoreMode, RestoreFailPoint,
    RestoreReport, RestoredIndexAction, BACKUP_ENGINE_VERSION, BACKUP_FORMAT_VERSION, BACKUP_MAGIC,
};
pub use capability::{capability, check as check_capability, supported_indexes_for, Capability};
pub use codec::{
    crc32c, decode_row_payload, decode_standalone, encode_artifact, encode_row_payload,
    encode_standalone, HEADER_LEN, MAGIC, VERSION,
};
pub use durability::{
    authority_table, may_serve_ann, permitted_derived_artifacts, DerivedArtifact, AUTHORITY_NOTE,
};
pub use error::{Result, VectorError};
pub use format::{
    format_sql_literal, format_sql_typed, pgwire_binary_decode, pgwire_binary_encode,
    pgwire_text_decode, pgwire_text_encode,
};
pub use freshness::{
    check_index_freshness, check_stats_freshness, is_index_queryable, maintenance_recovery_state,
    Freshness, FreshnessCause, FreshnessTracker,
};
pub use htap::{
    admission_error, admit, deadline_cancel_at, explain_htap, htap_disabled, percentile_ns,
    run_mixed, worker_cap, LatencySummary, MixedWorkloadReport, PressureLevel, Throttle,
    VectorAdmission, HTAP_ENV_VAR,
};
pub use literal::{
    format_binary, format_dense, format_multi, format_sparse, format_vector_value, parse_binary,
    parse_dense, parse_multi, parse_sparse, parse_vector_literal,
};
pub use metrics::{VectorMetrics, VectorMetricsSnapshot};
pub use migration::{
    check_transition, classify_index_blob, enforce, explain_matrix, matrix, Compatibility,
    MatrixRow, VersionedArtifact,
};
pub use numa::{NumaMode, NumaPolicy, NumaTopology, NUMA_ENV_VAR};
pub use operator::{
    categorize_search_error, categorize_vector_error, config_inventory, sqlstate_for_category,
    support_checklist, EngineLiveness, ErrorCategory, IndexStatusView, SettingInfo,
};
pub use options::{ComputeBackend, FilterMode, SearchMode, VectorSearchOptions};
pub use planner_api::{
    choose_exact_vs_ann, choose_exact_vs_ann_with_freshness, estimate_cost, estimate_cost_for_tier,
    explain_filter_plan, explain_radius_plan, explain_vector_plan, explain_vector_plan_full,
    index_radius_bound, index_usable, index_usable_with_freshness, order_by_supported,
    plan_filtered_search, plan_filtered_search_with_freshness, plan_radius_search,
    plan_radius_search_with_freshness, similarity_to_distance_bound, FilterPlan, FilterStrategy,
    RadiusPlan, RadiusStrategy,
};
pub use r#type::{VectorElementType, VectorKind, VectorMetric, VectorType};
pub use scale::{ScaleConfig, ScaleDecision, ScaleInputs, ScaleTier, SCALE_ENV_VAR};
pub use stats::{VectorStats, RECALL_UNKNOWN, SELECTIVITY_UNKNOWN, STATS_VERSION};
pub use validation::{
    binary_payload_bytes, check_finite, check_finite_slice, dense_payload_bytes,
    multi_payload_bytes, sparse_payload_bytes, validate_dimension, validate_max_vectors,
    validate_nnz, MAX_DIMENSION, MAX_PAYLOAD_BYTES, MIN_DIMENSION,
};
pub use value::{
    BinaryVectorValue, DenseVectorValue, MultiVectorValue, SparseVectorValue, VectorValue,
};
pub use wire::{
    check_wire_vector, decode_binary_wire, decode_text_wire, encode_for_wire, expected_binary_len,
    WireVector, BINARY_PARAMS_SUPPORTED, WIRE_OID_VECTOR, WIRE_OID_VECTOR_ARRAY,
};
