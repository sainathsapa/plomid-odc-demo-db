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
// Moved from src/parallel_budget.rs inline #[cfg(test)].
use plomid_executor::parallel_budget::*;
use plomid_hw::{ProfileSource, RuntimeProfile, RuntimeTier};
use std::sync::Barrier;

/// Run `f` with the free-permit count set to `permits`, restoring the
/// full budget afterwards even on panic (RAII reset, not a trailing
/// statement). Pins the Laptop tier too, so budget tests are hermetic
/// on any host class (detection could otherwise resolve Edge/Server and
/// change the clamp).
fn with_test_budget<R>(permits: usize, f: impl FnOnce() -> R) -> R {
    with_test_tier(pinned_tier_for_tests(RuntimeTier::Laptop), || {
        set_budget_for_tests(permits);
        f()
    })
}

#[test]
fn tier_mapping_from_hw_profile() {
    // Edge (Pi-class mock): elastic cap 8 (2 lanes x 4 cpus; the RAM term
    // is 128 so the CPU term binds), permits 2, every row threshold UP,
    // small batch/buffer/pool. Mocked through an explicit profile —
    // no host measurement, no env reads.
    let edge = pinned_tier_for_tests(RuntimeTier::Edge);
    assert_eq!(edge.tier, RuntimeTier::Edge);
    assert_eq!(edge.budget, 8);
    assert_eq!(edge.worker_permits, 2);
    assert_eq!(edge.join_threshold, 8_000);
    assert_eq!(edge.scan_min_rows, 400_000);
    assert_eq!(edge.sort_min_rows, 200_000);
    assert_eq!(edge.build_min_rows, 200_000);
    assert_eq!(edge.vector_batch_rows, 4_096);
    assert_eq!(edge.buffer_capacity_bytes, 64 * 1024 * 1024);
    assert_eq!(edge.buffer_reserve_bytes, 16 * 1024 * 1024);
    assert_eq!(edge.pool_capacity_pages, 256);
    // Laptop (developer-box mock): exactly the historical constants.
    let laptop = pinned_tier_for_tests(RuntimeTier::Laptop);
    assert_eq!(laptop.tier, RuntimeTier::Laptop);
    assert_eq!(laptop.budget, 32);
    assert_eq!(laptop.worker_permits, 8);
    assert_eq!(laptop.join_threshold, 2_000);
    assert_eq!(laptop.scan_min_rows, 100_000);
    assert_eq!(laptop.sort_min_rows, 50_000);
    assert_eq!(laptop.build_min_rows, 50_000);
    assert_eq!(laptop.vector_batch_rows, 16_384);
    assert_eq!(laptop.buffer_capacity_bytes, 256 * 1024 * 1024);
    assert_eq!(laptop.buffer_reserve_bytes, 64 * 1024 * 1024);
    assert_eq!(laptop.pool_capacity_pages, 1024);
    // Server (big-iron mock): cap up to 64, every row threshold
    // at/below current; batch/buffer/pool pinned to current constants.
    let server = pinned_tier_for_tests(RuntimeTier::Server);
    assert_eq!(server.tier, RuntimeTier::Server);
    assert_eq!(server.budget, 64);
    assert_eq!(server.worker_permits, 32);
    assert_eq!(server.join_threshold, 1_000);
    assert_eq!(server.scan_min_rows, 50_000);
    assert_eq!(server.sort_min_rows, 25_000);
    assert_eq!(server.build_min_rows, 25_000);
    assert_eq!(server.vector_batch_rows, 16_384);
    assert_eq!(server.buffer_capacity_bytes, 256 * 1024 * 1024);
    assert_eq!(server.buffer_reserve_bytes, 64 * 1024 * 1024);
    assert_eq!(server.pool_capacity_pages, 1024);
}

#[test]
fn laptop_tier_is_the_historical_constants() {
    // The no-behavior-change proof: Laptop carries today's values.
    let laptop = pinned_tier_for_tests(RuntimeTier::Laptop);
    assert_eq!(laptop.budget, ANALYTICAL_WORKER_BUDGET);
    assert_eq!(laptop.budget, 32);
    assert_eq!(laptop.join_threshold, 2_000);
    assert_eq!(laptop.scan_min_rows, 100_000);
    assert_eq!(laptop.sort_min_rows, 50_000);
    assert_eq!(laptop.build_min_rows, 50_000);
    assert_eq!(laptop.vector_batch_rows, 16_384);
    assert_eq!(laptop.buffer_capacity_bytes, 256 * 1024 * 1024);
    assert_eq!(laptop.buffer_reserve_bytes, 64 * 1024 * 1024);
    assert_eq!(laptop.pool_capacity_pages, 1024);
}

#[test]
fn delegates_match_laptop_under_pinned_tier() {
    // The global delegates read the pinned tier: on the pinned Laptop
    // tier every gate sees exactly its historical constant.
    with_test_tier(pinned_tier_for_tests(RuntimeTier::Laptop), || {
        assert_eq!(budget_cap(), ANALYTICAL_WORKER_BUDGET);
        assert_eq!(parallel_permits(), 8);
        assert_eq!(join_threshold(), 2_000);
        assert_eq!(scan_min_rows(), 100_000);
        assert_eq!(sort_min_rows(), 50_000);
        assert_eq!(build_min_rows(), 50_000);
        assert_eq!(available(), ANALYTICAL_WORKER_BUDGET);
        let machine = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        assert_eq!(available_workers(), machine.min(8).min(32));
        // Oversized wants clamp to the Laptop cap instead of
        // declining: the pre-tier clamping behavior, pinned.
        let big = claim_dispatch_workers(usize::MAX).expect("clamped want must grant");
        assert_eq!(big.workers(), ANALYTICAL_WORKER_BUDGET);
    });
}

#[test]
fn budget_clamp_scales_with_hardware() {
    with_test_tier(pinned_tier_for_tests(RuntimeTier::Edge), || {
        assert_eq!(budget_cap(), 8);
        assert_eq!(parallel_permits(), 2);
        assert_eq!(available(), 8);
        // An 8-wide want on the Edge mock fans out 8-wide instead of
        // declining; oversized wants clamp to the elastic cap.
        let dispatch = claim_dispatch_workers(8).expect("edge must grant 8-wide");
        assert_eq!(dispatch.workers(), 8);
        drop(dispatch);
        let big = claim_dispatch_workers(usize::MAX).expect("clamped want must grant");
        assert_eq!(big.workers(), 8);
        drop(big);
        assert_eq!(available(), 8, "leases release back to the elastic cap");
    });
    with_test_tier(pinned_tier_for_tests(RuntimeTier::Server), || {
        // Server mock (32c/64GiB): elastic cap 64 (2 lanes x 32 cpus).
        assert_eq!(budget_cap(), 64);
        assert_eq!(available(), 64);
        let big = claim_dispatch_workers(usize::MAX).expect("clamped want must grant");
        assert_eq!(big.workers(), 64);
    });
}

#[test]
fn elastic_cap_formula_worked_examples() {
    const GIB: u64 = 1024 * 1024 * 1024;
    // Pi-shaped: the CPU term binds (8 < RAM term 32); small, never
    // hundreds of permits.
    assert_eq!(
        elastic_cap_for_hardware(&mock_hardware_profile(4, Some(GIB))),
        8
    );
    // 16-core reference: exactly the historical 32.
    assert_eq!(
        elastic_cap_for_hardware(&mock_hardware_profile(16, Some(16 * GIB))),
        32
    );
    // Big iron: the CPU term binds (256 < RAM term 16384); far past 64.
    assert_eq!(
        elastic_cap_for_hardware(&mock_hardware_profile(128, Some(512 * GIB))),
        256
    );
    // Unknown RAM: conservative fallback (32), never zero, even on many
    // cores; tiny boxes stay CPU-bound underneath it.
    assert_eq!(
        elastic_cap_for_hardware(&mock_hardware_profile(64, None)),
        32
    );
    assert_eq!(elastic_cap_for_hardware(&mock_hardware_profile(4, None)), 8);
    // Degenerate readings fail safe to the floor, never zero.
    assert_eq!(
        elastic_cap_for_hardware(&mock_hardware_profile(1, Some(64 * 1024 * 1024))),
        MIN_ANALYTICAL_WORKER_BUDGET
    );
    assert_eq!(
        elastic_cap_for_hardware(&mock_hardware_profile(2, Some(0))),
        MIN_ANALYTICAL_WORKER_BUDGET
    );
    // Absurd core counts hit the safety ceiling, never thread-explosion.
    assert_eq!(
        elastic_cap_for_hardware(&mock_hardware_profile(4096, Some(8 * 1024 * GIB))),
        MAX_ANALYTICAL_WORKER_BUDGET
    );
}

#[test]
fn elastic_cap_end_to_end_via_classification() {
    use plomid_hw::ProfileSource;
    const GIB: u64 = 1024 * 1024 * 1024;
    // No pinned tier: real classification from measured-shaped hardware.
    let pi = ParallelBudget::from_profile(&RuntimeProfile::from_hardware(
        mock_hardware_profile(4, Some(GIB)),
        ProfileSource::Explicit,
    ));
    assert_eq!(pi.tier, RuntimeTier::Edge);
    assert_eq!(
        pi.budget, 8,
        "a 4c/1GiB box must not get hundreds of permits"
    );
    assert_eq!(pi.worker_permits, 2);

    let big = ParallelBudget::from_profile(&RuntimeProfile::from_hardware(
        mock_hardware_profile(128, Some(512 * GIB)),
        ProfileSource::Explicit,
    ));
    assert_eq!(big.tier, RuntimeTier::Server);
    assert!(
        big.budget >= 128,
        "128c big iron must scale far past 64, got {}",
        big.budget
    );

    let unknown = ParallelBudget::from_profile(&RuntimeProfile::from_hardware(
        mock_hardware_profile(64, None),
        ProfileSource::Explicit,
    ));
    assert_eq!(unknown.tier, RuntimeTier::Laptop);
    assert_eq!(
        unknown.budget, ANALYTICAL_WORKER_BUDGET,
        "unknown RAM stays conservative"
    );
}

#[test]
fn tier_threshold_delegates_gate_more_on_edge_and_less_on_server() {
    with_test_tier(pinned_tier_for_tests(RuntimeTier::Edge), || {
        assert_eq!(join_threshold(), 8_000);
        assert_eq!(scan_min_rows(), 400_000);
        assert_eq!(sort_min_rows(), 200_000);
        assert_eq!(build_min_rows(), 200_000);
    });
    with_test_tier(pinned_tier_for_tests(RuntimeTier::Server), || {
        assert_eq!(join_threshold(), 1_000);
        assert_eq!(scan_min_rows(), 50_000);
        assert_eq!(sort_min_rows(), 25_000);
        assert_eq!(build_min_rows(), 25_000);
    });
}

#[test]
fn exhaustion_declines_to_sequential() {
    with_test_budget(0, || {
        // Zero free permits: every dispatch shape declines, so the caller
        // runs the sequential path.
        assert!(try_acquire(4).is_none());
        assert!(claim_dispatch_workers(8).is_none());
    });
    with_test_budget(1, || {
        // One permit is below the 2-worker fan-out floor: the raw claim
        // succeeds (a permit is free) but dispatch still declines, so the
        // caller runs sequential and the permit is not pointlessly held.
        let lease = try_acquire(1);
        assert!(lease.is_some());
        assert_eq!(available(), 0);
        assert!(claim_dispatch_workers(8).is_none());
        drop(lease);
    });
    with_test_budget(ANALYTICAL_WORKER_BUDGET, || {
        // Full budget: dispatch claims and clamps oversized wants.
        let dispatch = claim_dispatch_workers(8).expect("idle budget must grant");
        assert_eq!(dispatch.workers(), 8);
        assert_eq!(available(), ANALYTICAL_WORKER_BUDGET - 8);
        drop(dispatch);
        assert_eq!(available(), ANALYTICAL_WORKER_BUDGET);
        // Oversized wants clamp to the cap instead of declining: large
        // boxes keep reduced fan-out.
        let big = claim_dispatch_workers(usize::MAX).expect("clamped want must grant");
        assert_eq!(big.workers(), ANALYTICAL_WORKER_BUDGET);
    });
}

#[test]
fn concurrent_claims_never_overgrant_and_fully_release() {
    with_test_budget(4, || {
        const CONTENDERS: usize = 16;
        const PERMITS: usize = 4;
        // Every contender attempts exactly once and holds any granted
        // lease across `release`: no release can sneak in an extra grant
        // before the count is taken, so the count equals the number of
        // concurrently held permits. Exactly PERMITS: no over-grant, no
        // spurious denial at exact fit.
        let attempted = Barrier::new(CONTENDERS + 1);
        let release = Barrier::new(CONTENDERS + 1);
        let (tx, rx) = std::sync::mpsc::channel::<bool>();
        // Share by reference: `move` closures capture the `&Barrier`
        // (still `Send`), so the main thread can wait on the same gates.
        let attempted = &attempted;
        let release = &release;
        std::thread::scope(|scope| {
            for _ in 0..CONTENDERS {
                let tx = tx.clone();
                scope.spawn(move || {
                    let guard = try_acquire(1);
                    tx.send(guard.is_some()).expect("report");
                    attempted.wait();
                    release.wait();
                    drop(guard);
                });
            }
            drop(tx);
            attempted.wait();
            let granted: usize = rx.iter().take(CONTENDERS).filter(|held| *held).count();
            assert_eq!(
                granted, PERMITS,
                "exact-fit contention must grant exactly the free permits"
            );
            assert_eq!(available(), 0, "all permits concurrently held");
            release.wait();
        });
        assert_eq!(
            available(),
            PERMITS,
            "every lease released after the scope joins"
        );
    });
}

#[test]
fn guard_releases_on_panic() {
    with_test_budget(ANALYTICAL_WORKER_BUDGET, || {
        let guard = try_acquire(2).expect("idle budget must grant");
        assert_eq!(available(), ANALYTICAL_WORKER_BUDGET - 2);
        let outcome = std::panic::catch_unwind(move || {
            let _leased = guard;
            panic!("simulated worker panic while holding permits");
        });
        assert!(outcome.is_err());
        // Unwinding dropped the guard: permits are back, and the mutex
        // was never poisoned (the guard holds a count, not the lock).
        assert_eq!(available(), ANALYTICAL_WORKER_BUDGET);
        assert!(try_acquire(ANALYTICAL_WORKER_BUDGET).is_some());
    });
}

#[test]
fn degenerate_wants_decline() {
    with_test_budget(ANALYTICAL_WORKER_BUDGET, || {
        assert!(try_acquire(0).is_none());
        assert!(claim_dispatch_workers(0).is_none());
        assert!(claim_dispatch_workers(1).is_none());
        assert_eq!(
            available(),
            ANALYTICAL_WORKER_BUDGET,
            "declines must not consume permits"
        );
    });
}

#[test]
fn live_grow_applies_to_new_admissions() {
    // Start Pi-class (Edge, cap 8, full budget), then mock a resize to
    // big iron: the cap grows and free permits top up so new admissions
    // immediately fan out wider. Uses the Auto source so the tier may
    // flip; the cap assertion is the load-bearing one.
    let edge = RuntimeProfile::from_hardware(
        mock_hardware_profile(4, Some(4 * 1024 * 1024 * 1024)),
        ProfileSource::Auto,
    );
    let _guard = TestTierGuard::hold(ParallelBudget::from_profile(&edge));
    // `hold` synthesizes an Explicit profile; swap in the Auto one so
    // this test exercises auto-shift.
    set_profile_for_tests(edge);
    assert_eq!(budget_cap(), 8);
    assert_eq!(available(), 8);
    recheck_with_hardware_for_tests(mock_hardware_profile(32, Some(64 * 1024 * 1024 * 1024)));
    assert_eq!(budget_cap(), 64, "grown hardware must grow the live cap");
    assert_eq!(available(), 64, "grow tops up free permits");
    assert_eq!(current_tier().tier, RuntimeTier::Server);
    let dispatch = claim_dispatch_workers(64).expect("grown budget must grant 64-wide");
    assert_eq!(dispatch.workers(), 64);
}

#[test]
fn live_shrink_keeps_inflight_permits_and_limits_new_admissions() {
    // Start big (cap 64, Auto), hold 16 permits in-flight, then mock a
    // downscale to Pi-class (cap 8): the 16 held leases keep theirs
    // (never revoked), free permits saturate to 0, and new admissions
    // decline until leases return. On return the surplus saturates at
    // the new cap instead of overflowing it.
    let big = RuntimeProfile::from_hardware(
        mock_hardware_profile(32, Some(64 * 1024 * 1024 * 1024)),
        ProfileSource::Auto,
    );
    let _guard = TestTierGuard::hold(ParallelBudget::from_profile(&big));
    set_profile_for_tests(big);
    assert_eq!(budget_cap(), 64);
    let held = try_acquire(16).expect("idle big budget must grant 16");
    assert_eq!(available(), 48);
    recheck_with_hardware_for_tests(mock_hardware_profile(4, Some(4 * 1024 * 1024 * 1024)));
    assert_eq!(budget_cap(), 8, "shrunk hardware must shrink the live cap");
    // Shrink saturates free permits, never revokes held ones: the free
    // count becomes min(prior free, shrunk cap).
    let prior_free = 48;
    let shrunk_cap = 8;
    assert_eq!(available(), prior_free.min(shrunk_cap));
    assert_eq!(available(), 8);
    // New admissions above the shrunk free count decline.
    assert!(
        try_acquire(16).is_none(),
        "16-wide no longer fits the shrunk cap's free permits"
    );
    // In-flight keeps working: the held 16 are untouched by the shrink.
    drop(held);
    // 16 returned against an 8-cap saturates instead of overflowing.
    assert_eq!(available(), 8, "release saturates at the shrunk cap");
    let dispatch = claim_dispatch_workers(8).expect("shrunk cap still grants 8-wide");
    assert_eq!(dispatch.workers(), 8);
}

#[test]
fn live_override_pins_tier_while_cap_follows_hardware() {
    // Explicit Edge pin on Pi-class hardware, then mock big-iron
    // hardware: the tier (and its thresholds) stay pinned while the
    // elastic cap still follows the hardware underneath.
    let pinned = RuntimeProfile::with_tier(
        RuntimeTier::Edge,
        mock_hardware_profile(4, Some(4 * 1024 * 1024 * 1024)),
        ProfileSource::Explicit,
    );
    let _guard = TestTierGuard::hold(ParallelBudget::from_profile(&pinned));
    set_profile_for_tests(pinned);
    assert_eq!(current_tier().tier, RuntimeTier::Edge);
    assert_eq!(join_threshold(), 8_000);
    recheck_with_hardware_for_tests(mock_hardware_profile(32, Some(64 * 1024 * 1024 * 1024)));
    assert_eq!(
        current_tier().tier,
        RuntimeTier::Edge,
        "explicit pin disables auto-shift"
    );
    assert_eq!(
        join_threshold(),
        8_000,
        "pinned thresholds stay for new queries"
    );
    assert_eq!(parallel_permits(), 2, "Edge permits stay railed at 2");
    assert_eq!(budget_cap(), 64, "elastic cap still follows grown hardware");
}
