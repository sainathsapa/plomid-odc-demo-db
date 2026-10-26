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

//! End-to-end HTAP governance tests.

use plomid_htap::{
    classify, parallelism_for, throttle, Admission, CacheClass, HtapConfig, HtapSnapshot,
    MaintenanceThrottle, PressureState, ResourceGovernor, StatementPolicy, TenantId, TenantQuota,
    UnifiedBufferManager, WorkloadClass,
};

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
fn safe_inference_never_requires_manual_classification() {
    // write/small => Oltp, else hint or Olap.
    assert_eq!(classify(None, true, None), WorkloadClass::Oltp);
    assert_eq!(classify(None, false, Some(10)), WorkloadClass::Oltp);
    assert_eq!(classify(None, false, Some(5_000_000)), WorkloadClass::Olap);
    assert_eq!(classify(None, false, None), WorkloadClass::Olap);
    assert_eq!(
        classify(Some(WorkloadClass::Batch), false, None),
        WorkloadClass::Batch
    );
}

#[test]
fn config_validation_rejects_overcommit() {
    assert!(small_config().validate().is_ok());
    let mut bad = small_config();
    // reserve+budgets = 201+650+100+50 > 1000
    bad.oltp_reservation_bytes = 201;
    let err = bad.validate().expect_err("overcommit must be rejected");
    assert!(err.reason().contains("overcommit"), "{}", err.reason());
}

#[test]
fn oltp_admitted_during_olap_saturation() {
    let gov = ResourceGovernor::new(small_config()).unwrap();
    // Saturate the OLAP pool both by bytes and by concurrency slots.
    for _ in 0..8 {
        assert!(gov.admit(WorkloadClass::Olap, 80, 1).is_admitted());
    }
    assert_eq!(gov.olap_used_bytes(), 640);
    // Pool is effectively full: one more OLAP must not be admitted...
    assert!(!gov.admit(WorkloadClass::Olap, 80, 1).is_admitted());
    // ...but OLTP sails through: it is always preferred.
    let decision = gov.admit(WorkloadClass::Oltp, 100, 1);
    assert!(decision.is_admitted(), "{decision:?}");
}

#[test]
fn olap_cannot_touch_oltp_reserve() {
    let gov = ResourceGovernor::new(small_config()).unwrap();
    // Create OLTP pressure: OLTP holds > half the 200B reservation.
    assert!(gov.admit(WorkloadClass::Oltp, 150, 1).is_admitted());
    assert!(gov.oltp_pressure_exists());
    assert!(gov.admit(WorkloadClass::Olap, 600, 1).is_admitted());
    assert!(gov.admit(WorkloadClass::Maintenance, 100, 1).is_admitted());
    assert!(gov.admit(WorkloadClass::System, 50, 1).is_admitted());
    // Unpooled cache occupancy pushes global usage to 960/1000 while the
    // OLAP pool itself still has room (610/650).
    gov.account_cache_bytes(60);
    assert_eq!(gov.utilization_pct(), 96);

    // OLAP would breach the fenced reserve: hard reject.
    let decision = gov.admit(WorkloadClass::Olap, 10, 1);
    match &decision {
        Admission::Reject { reason } => assert!(
            reason.contains("reservation"),
            "expected reserve reason, got: {reason}"
        ),
        other => panic!("OLAP must not touch the OLTP reserve, got {other:?}"),
    }
    // OLTP still admitted from the reserve.
    let oltp = gov.admit(WorkloadClass::Oltp, 10, 1);
    assert!(oltp.is_admitted(), "{oltp:?}");
}

#[test]
fn olap_concurrency_reduced_under_pressure() {
    let mut cfg = small_config();
    cfg.olap_max_concurrency = 4;
    let gov = ResourceGovernor::new(cfg).unwrap();
    // Warning pressure (75%) from unpooled cache bytes, no OLTP pressure,
    // so the reserve fence stays open and only derating applies.
    gov.account_cache_bytes(750);
    assert_eq!(gov.pressure(), PressureState::Warning);
    assert!(!gov.oltp_pressure_exists());
    assert_eq!(gov.effective_olap_concurrency(), 2);

    let first = gov.admit(WorkloadClass::Olap, 10, 1);
    match first {
        Admission::Admit {
            reduced_parallelism,
        } => assert_eq!(reduced_parallelism, 4, "8->4 under Warning"),
        other => panic!("expected admit, got {other:?}"),
    }
    assert!(gov.admit(WorkloadClass::Olap, 10, 1).is_admitted());
    // Reduced cap (2) reached: the third OLAP queues instead of starving
    // the admitted ones.
    let third = gov.admit(WorkloadClass::Olap, 10, 1);
    assert!(matches!(third, Admission::Queue { .. }), "{third:?}");
}

#[test]
fn adaptive_parallelism_ladder_and_oltp_immune() {
    use PressureState::*;
    assert_eq!(parallelism_for(WorkloadClass::Olap, Normal, 8), 8);
    assert_eq!(parallelism_for(WorkloadClass::Olap, Warning, 8), 4);
    assert_eq!(parallelism_for(WorkloadClass::Olap, Pressure, 8), 2);
    assert_eq!(parallelism_for(WorkloadClass::Olap, Critical, 8), 1);
    for pressure in [Normal, Warning, Pressure, Critical] {
        assert_eq!(
            parallelism_for(WorkloadClass::Oltp, pressure, 8),
            8,
            "OLTP unaffected by {pressure:?}"
        );
    }
}

#[test]
fn maintenance_throttled_then_paused() {
    assert_eq!(throttle(PressureState::Normal), MaintenanceThrottle::Full);
    assert_eq!(
        throttle(PressureState::Pressure),
        MaintenanceThrottle::Degraded,
        "throttled under pressure"
    );
    assert_eq!(
        throttle(PressureState::Critical),
        MaintenanceThrottle::Paused,
        "paused at critical"
    );
    // Governor mirrors it: maintenance queues at critical, OLTP proceeds.
    let gov = ResourceGovernor::new(small_config()).unwrap();
    gov.account_cache_bytes(960);
    assert!(matches!(
        gov.admit(WorkloadClass::Maintenance, 10, 1),
        Admission::Queue { .. }
    ));
    assert!(gov.admit(WorkloadClass::Oltp, 10, 1).is_admitted());
}

#[test]
fn statement_timeout_is_distinct_from_row_limits() {
    // Unlimited policy: no deadline regardless of any LIMIT clause.
    assert!(StatementPolicy::unlimited().deadline().is_none());
    // Bounded policy: wall-clock deadline ~100ms out.
    let policy = StatementPolicy::with_timeout(100);
    let deadline = policy.deadline().expect("deadline must exist");
    assert!(deadline > std::time::Instant::now());
    // Row limits and timeouts are orthogonal concepts; the policy only
    // constrains time, never row counts (see StatementPolicy docs).
    assert_eq!(policy.statement_timeout_ms, Some(100));
}

#[test]
fn cache_sheds_bytes_under_pressure_and_counts_globally() {
    let gov = ResourceGovernor::new(small_config()).unwrap();
    // Eight 100B metadata entries: never bypass, so all 800B occupy.
    let manager = UnifiedBufferManager::new(1000, 0);
    for i in 0..8 {
        let key = format!("meta/{i}");
        assert!(
            manager
                .admit(&key, CacheClass::Metadata, 100, None)
                .is_admitted(),
            "admit {key}"
        );
    }
    assert_eq!(manager.occupancy(), 800);
    gov.account_cache_bytes(manager.occupancy());
    assert_eq!(gov.cache_bytes(), 800);
    assert_eq!(gov.utilization_pct(), 80);
    // Pressure target is 25% of 800 = 200: exactly two entries shed.
    let evicted = manager.evict_under_pressure(PressureState::Pressure);
    gov.release_cache_bytes(evicted);
    assert_eq!(evicted, 200);
    assert_eq!(manager.occupancy(), 600);
    assert_eq!(gov.cache_bytes(), 600);
}

#[test]
fn tenant_hooks_default_unlimited_and_enforce_when_configured() {
    let gov = ResourceGovernor::new(small_config()).unwrap();
    let tenant = TenantId::new("acme");
    // Default: single-tenant unlimited, no registration needed.
    assert!(gov
        .admit_for_tenant(&tenant, WorkloadClass::Olap, 100, 1)
        .is_admitted());
    gov.release_for_tenant(&tenant, WorkloadClass::Olap, 100, 1);

    gov.register_tenant(tenant.clone(), TenantQuota::new(100, 2));
    let over_budget = gov.admit_for_tenant(&tenant, WorkloadClass::Olap, 150, 1);
    assert!(
        matches!(over_budget, Admission::Reject { .. }),
        "{over_budget:?}"
    );
    assert!(gov
        .admit_for_tenant(&tenant, WorkloadClass::Olap, 60, 1)
        .is_admitted());
    let over_slots = gov.admit_for_tenant(&tenant, WorkloadClass::Olap, 10, 2);
    assert!(
        matches!(over_slots, Admission::Reject { .. }),
        "{over_slots:?}"
    );
}

#[test]
fn snapshot_answers_why_oltp_slow_and_who_uses() {
    let gov = ResourceGovernor::new(small_config()).unwrap();
    assert!(gov.admit(WorkloadClass::Olap, 500, 1).is_admitted());
    gov.account_cache_bytes(100);
    gov.record_spill_bytes(4096);
    gov.account_result_buffer_bytes(64);

    let snap = HtapSnapshot::collect(&gov);
    assert_eq!(snap.global_used_bytes, 664);
    assert_eq!(snap.olap_used_bytes, 500);
    assert_eq!(snap.cache_bytes, 100);
    assert_eq!(snap.result_buffer_bytes, 64);
    assert_eq!(snap.spill_total_bytes, 4096);
    // Admission is governor-owned: one OLAP slot held, no queue exists.
    assert_eq!(snap.admitted_olap, gov.olap_in_use_slots());
    assert_eq!(snap.admitted_olap, 1);
    assert_eq!(snap.waiting_count, 0);
    assert!(snap.waiting.is_empty());
    assert!(snap.per_query.is_empty());
    let text = snap.describe();
    assert!(text.contains("pressure="), "{text}");
    assert!(text.contains("top_consumer="), "{text}");
    assert!(text.contains("oltp_headroom="), "{text}");
}
