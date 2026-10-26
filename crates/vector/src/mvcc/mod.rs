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
//! MVCC support: deltas, visibility, maintenance.
//!
//! NON-AUTHORITATIVE legacy helpers (see [`crate::durability`]).
//! MVCC visibility authority lives in `plomid-txn`; the production SQL path
//! resolves visibility through the engine snapshot plus
//! [`crate::freshness`] gating, never through these types. They remain for
//! offline index-build protocol tests as derived-state bookkeeping only.

pub mod delta;
pub mod maintenance;
pub mod visibility;

pub use delta::{CommittedDelta, CommittedEntry, DeltaOp, PendingWrites};
pub use maintenance::{
    advise, fragmentation_metric, fragmentation_metric_full, should_cleanup_tombstones,
    should_rebuild, tombstone_ratio, MaintenanceAdvice, MaintenanceConfig,
};
pub use visibility::{is_version_visible, is_visible, VisibilitySnapshot};
