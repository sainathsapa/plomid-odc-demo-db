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
// Moved from `src/compaction_guard.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::compaction_guard::*;
use plomid_htap::{MaintenanceThrottle, PressureState, ResourceGovernor};

#[test]
fn open_gate_never_yields_or_aborts() {
    let gate = CompactionGate::open();
    assert_eq!(gate.check(0), GateDecision::Proceed);
    assert_eq!(gate.check(1_000_000), GateDecision::Proceed);
    assert_eq!(gate.yield_count(), 0);
    assert!(gate.enforce(100).expect("open enforces").eq(&false));
}

#[test]
fn paused_gate_refuses_at_entry() {
    let gate = CompactionGate::paused();
    assert_eq!(gate.check(0), GateDecision::AbortPaused);
    assert_eq!(gate.check_abort(), GateDecision::AbortPaused);
    assert!(gate.enforce(0).is_err());
}

#[test]
fn cancelled_gate_aborts() {
    let cancel = CancelToken::new();
    let gate = CompactionGate::new(
        CompactionThrottle::full(),
        cancel.clone(),
        Deadline::never(),
    );
    assert_eq!(gate.check(0), GateDecision::Proceed);
    cancel.cancel();
    assert_eq!(gate.check(0), GateDecision::AbortCancelled);
    let err = gate.enforce(0).expect_err("cancelled must abort");
    assert_eq!(err.kind(), plomid_core::ErrorKind::Aborted);
}

#[test]
fn expired_deadline_aborts() {
    let gate = CompactionGate::new(
        CompactionThrottle::full(),
        CancelToken::new(),
        Deadline::expired(),
    );
    assert_eq!(gate.check(0), GateDecision::AbortExpired);
    assert!(gate.check_abort().is_abort());
}

#[test]
fn degraded_yields_periodically_and_counts() {
    let gate = CompactionGate::new(
        CompactionThrottle::degraded(),
        CancelToken::new(),
        Deadline::never(),
    );
    assert_eq!(gate.check(0), GateDecision::Proceed);
    assert_eq!(gate.check(256), GateDecision::YieldAndRecheck);
    assert!(gate.enforce(256).expect("yield ok"));
    assert_eq!(gate.yield_count(), 1);
    // Non-boundary counts proceed.
    assert_eq!(gate.check(255), GateDecision::Proceed);
}

#[test]
fn throttle_mapping_reuses_shared_ladder() {
    assert_eq!(
        CompactionThrottle::from_pressure(PressureState::Normal).throttle,
        MaintenanceThrottle::Full
    );
    assert_eq!(
        CompactionThrottle::from_pressure(PressureState::Pressure).throttle,
        MaintenanceThrottle::Degraded
    );
    assert_eq!(
        CompactionThrottle::from_pressure(PressureState::Critical).throttle,
        MaintenanceThrottle::Paused
    );
}

#[test]
fn governor_refusal_maps_to_paused_abort() {
    // Critical pressure pauses maintenance in the governor: admit refuses.
    let config = plomid_htap::config::HtapConfig {
        global_budget_bytes: 1000,
        oltp_reservation_bytes: 200,
        olap_budget_bytes: 650,
        maintenance_budget_bytes: 100,
        system_reserve_bytes: 50,
        oltp_max_concurrency: 256,
        olap_max_concurrency: 8,
        maintenance_max_concurrency: 4,
        system_max_concurrency: 8,
        oltp_max_parallelism: 4,
        olap_max_parallelism: 8,
        olap_max_bytes_per_query: 650,
        statement_timeout_ms: None,
        oltp_priority_latency: true,
        warning_threshold_pct: 70,
        pressure_threshold_pct: 85,
        critical_threshold_pct: 95,
        small_txn_row_threshold: 1024,
    };
    let governor = ResourceGovernor::new(config).expect("governor");
    governor.account_cache_bytes(950);
    assert_eq!(governor.pressure(), PressureState::Critical);
    let decision = CompactionGate::admit_or_refuse(&governor, 10).expect_err("must refuse");
    assert_eq!(decision, GateDecision::AbortPaused);
}
