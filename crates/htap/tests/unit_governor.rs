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
// Moved from `src/governor.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::governor::*;
use plomid_htap::{HtapConfig, PressureState, WorkloadClass};

fn small_config() -> HtapConfig {
    HtapConfig {
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
    }
}

#[test]
fn rejects_invalid_config() {
    let mut cfg = small_config();
    cfg.olap_budget_bytes = 10_000;
    assert!(ResourceGovernor::new(cfg).is_err());
}

#[test]
fn per_query_limit_rejects_starving_queries() {
    let gov = ResourceGovernor::new(small_config()).unwrap();
    let decision = gov.admit(WorkloadClass::Olap, 651, 1);
    assert!(matches!(decision, Admission::Reject { .. }), "{decision:?}");
}

#[test]
fn maintenance_paused_at_critical() {
    let gov = ResourceGovernor::new(small_config()).unwrap();
    gov.account_cache_bytes(950);
    assert_eq!(gov.pressure(), PressureState::Critical);
    let decision = gov.admit(WorkloadClass::Maintenance, 10, 1);
    assert!(matches!(decision, Admission::Queue { .. }), "{decision:?}");
    // OLTP still admitted while global budget remains.
    assert!(gov.admit(WorkloadClass::Oltp, 10, 1).is_admitted());
}
