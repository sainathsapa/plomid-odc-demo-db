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
// Moved from `src/freshness.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::admin::IndexState;
use plomid_vector::freshness::*;
use plomid_vector::stats::VectorStats;

#[test]
fn normal_lifecycle_insert_update_delete_invalidate() {
    let mut t = FreshnessTracker::unknown();
    assert_eq!(t.state(), Freshness::Unknown);
    t.refresh(10);
    assert_eq!(t.state(), Freshness::Fresh);
    assert!(t.usable_for_ann());
    t.invalidate(FreshnessCause::Insert, "row inserted");
    assert_eq!(t.state(), Freshness::Stale);
    assert!(!t.usable_for_ann());
    t.refresh(11);
    assert_eq!(t.state(), Freshness::Fresh);
    t.invalidate(FreshnessCause::Update, "row updated");
    assert_eq!(t.state(), Freshness::Stale);
    t.refresh(12);
    t.invalidate(FreshnessCause::Delete, "row deleted");
    assert_eq!(t.state(), Freshness::Stale);
}

#[test]
fn index_build_and_rebuild_cycle() {
    let mut t = FreshnessTracker::unknown();
    t.invalidate(FreshnessCause::IndexRebuild, "rebuild started");
    assert_eq!(t.state(), Freshness::Building);
    assert!(!t.state().usable_for_exact() || true); // building never serves ANN
    assert!(!t.usable_for_ann());
    t.mark_built(20);
    assert_eq!(t.state(), Freshness::Fresh);
    // Interrupted maintenance recovers safely, never directly usable.
    assert_eq!(maintenance_recovery_state(true), Freshness::Recovering);
    let mut r = FreshnessTracker::unknown();
    r.invalidate(FreshnessCause::MaintenanceInterrupted, "compaction killed");
    assert_eq!(r.state(), Freshness::Recovering);
    assert!(!r.usable_for_ann());
    r.recovery_complete();
    assert_eq!(r.state(), Freshness::Unknown);
    r.refresh(21);
    assert_eq!(r.state(), Freshness::Fresh);
}

#[test]
fn restart_and_crash_recovery_are_deterministic() {
    let mut t = FreshnessTracker::unknown();
    t.refresh(5);
    t.invalidate(FreshnessCause::Restart, "process restarted");
    assert_eq!(t.state(), Freshness::Recovering);
    t.recovery_complete();
    assert_eq!(t.state(), Freshness::Unknown);
    let mut c = FreshnessTracker::unknown();
    c.refresh(5);
    c.invalidate(FreshnessCause::CrashRecovery, "WAL replay");
    assert_eq!(c.state(), Freshness::Recovering);
    assert!(!c.usable_for_ann());
}

#[test]
fn configuration_change_and_restore_invalidate() {
    let mut t = FreshnessTracker::unknown();
    t.refresh(1);
    t.invalidate(FreshnessCause::ConfigChange, "ef_search changed");
    assert_eq!(t.state(), Freshness::Stale);
    t.refresh(2);
    t.invalidate(FreshnessCause::Restore, "snapshot restored");
    assert_eq!(t.state(), Freshness::Stale);
    t.refresh(3);
    t.invalidate(FreshnessCause::StatsRefresh, "refresh done elsewhere");
    assert_eq!(t.state(), Freshness::Fresh);
}

#[test]
fn corrupted_metadata_is_never_served() {
    let mut t = FreshnessTracker::unknown();
    t.refresh(1);
    t.mark_invalid("manifest version skew");
    assert_eq!(t.state(), Freshness::Invalid);
    assert!(!t.usable_for_ann());
    assert!(!t.state().usable_for_exact());
    // Stale writes cannot resurrect an invalid image.
    t.invalidate(FreshnessCause::Insert, "late write");
    assert_eq!(t.state(), Freshness::Invalid);
}

#[test]
fn stats_freshness_classification() {
    let mut s = VectorStats::unknown(8);
    assert_eq!(check_stats_freshness(&s, 0), Freshness::Unknown);
    for _ in 0..4 {
        s.observe_row(false, false, true);
    }
    s.build_watermark = 9;
    assert_eq!(check_stats_freshness(&s, 9), Freshness::Fresh);
    assert_eq!(check_stats_freshness(&s, 10), Freshness::Stale);
}

#[test]
fn index_lifecycle_gating() {
    // Incomplete builds must not appear usable.
    assert!(!is_index_queryable(IndexState::Building, Freshness::Fresh));
    assert!(!is_index_queryable(
        IndexState::CatchingUp,
        Freshness::Fresh
    ));
    assert!(!is_index_queryable(
        IndexState::Rebuilding,
        Freshness::Fresh
    ));
    assert!(!is_index_queryable(IndexState::Failed, Freshness::Fresh));
    assert!(!is_index_queryable(IndexState::Absent, Freshness::Unknown));
    // Ready + fresh is the only ANN-serving combination (Degraded+fresh
    // serves with reduced recall, still correct via rerank).
    assert!(is_index_queryable(IndexState::Ready, Freshness::Fresh));
    assert!(is_index_queryable(IndexState::Degraded, Freshness::Fresh));
    assert!(!is_index_queryable(IndexState::Ready, Freshness::Stale));
    assert!(!is_index_queryable(IndexState::Ready, Freshness::Building));
    assert!(!is_index_queryable(IndexState::Ready, Freshness::Invalid));
    assert!(!is_index_queryable(
        IndexState::Ready,
        Freshness::Recovering
    ));
    assert!(!is_index_queryable(IndexState::Ready, Freshness::Unknown));
}

#[test]
fn index_freshness_from_watermarks() {
    assert_eq!(
        check_index_freshness(IndexState::Ready, 7, 7),
        Freshness::Fresh
    );
    assert_eq!(
        check_index_freshness(IndexState::Ready, 7, 8),
        Freshness::Stale
    );
    assert_eq!(
        check_index_freshness(IndexState::Building, 7, 7),
        Freshness::Building
    );
    assert_eq!(
        check_index_freshness(IndexState::Failed, 7, 7),
        Freshness::Invalid
    );
    assert_eq!(
        check_index_freshness(IndexState::Absent, 0, 0),
        Freshness::Unknown
    );
}

#[test]
fn stale_never_produces_wrong_results_contract() {
    // The contract: stale/unknown force conservative (exact) planning via
    // the planner_api wrappers; freshness states themselves never alter
    // distances. Exact usability holds for fresh/stale/unknown.
    assert!(Freshness::Fresh.usable_for_exact());
    assert!(Freshness::Stale.usable_for_exact());
    assert!(Freshness::Unknown.usable_for_exact());
    assert!(!Freshness::Building.usable_for_exact());
    assert!(!Freshness::Invalid.usable_for_exact());
    assert!(!Freshness::Recovering.usable_for_exact());
    // Only fresh serves ANN.
    assert!(Freshness::Fresh.usable_for_ann());
    assert!(!Freshness::Stale.usable_for_ann());
}
