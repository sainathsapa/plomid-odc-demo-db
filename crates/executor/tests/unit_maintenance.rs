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
// Moved from src/maintenance.rs inline #[cfg(test)].
mod tests {
    use plomid_executor::maintenance::*;

    #[test]
    fn default_policy_is_documented_and_configurable() {
        assert_eq!(
            MaintenancePolicy::default().committed_mutations_per_generation(),
            MaintenancePolicy::DEFAULT_COMMITTED_MUTATIONS_PER_GENERATION
        );
        assert_eq!(
            MaintenancePolicy::new(7).committed_mutations_per_generation(),
            7
        );
        // Zero is clamped to the most eager valid policy rather than disabled.
        assert_eq!(
            MaintenancePolicy::new(0).committed_mutations_per_generation(),
            1
        );
    }

    #[test]
    fn writes_accumulate_and_only_become_due_at_the_bound() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenancePolicy::new(3));
        assert!(!coordinator.record_committed_mutation("public.t"));
        assert!(!coordinator.record_committed_mutation("public.t"));
        assert_eq!(coordinator.committed_mutations("public.t"), 2);
        assert!(coordinator.due_tables().is_empty());

        // The third commit reaches the bound: the table is due, and the
        // accounting still shows all three committed mutations.
        assert!(coordinator.record_committed_mutation("public.t"));
        assert_eq!(coordinator.due_tables(), vec!["public.t".to_string()]);
        assert_eq!(coordinator.committed_mutations("public.t"), 3);
    }

    #[test]
    fn a_pass_consumes_the_accumulated_mutations() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenancePolicy::new(2));
        coordinator.record_committed_mutation("t");
        coordinator.record_committed_mutation("t");
        assert!(coordinator.any_due());
        coordinator.record_maintenance_pass("t");
        assert_eq!(coordinator.committed_mutations("t"), 0);
        assert_eq!(coordinator.maintenance_passes("t"), 1);
        assert!(!coordinator.any_due());
    }

    #[test]
    fn tables_are_accounted_and_ordered_independently() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenancePolicy::new(1));
        coordinator.record_committed_mutation("b");
        coordinator.record_committed_mutation("a");
        coordinator.record_committed_mutation("b");
        // Deterministic order, and "a" stays due only once regardless of count.
        assert_eq!(
            coordinator.due_tables(),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(coordinator.committed_mutations("a"), 1);
        assert_eq!(coordinator.committed_mutations("b"), 2);
    }

    #[test]
    fn changing_the_policy_preserves_recorded_work() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenancePolicy::new(10));
        coordinator.record_committed_mutation("t");
        assert!(!coordinator.any_due());
        coordinator.set_policy(MaintenancePolicy::new(1));
        assert!(coordinator.any_due());
        assert_eq!(coordinator.committed_mutations("t"), 1);
    }
}

mod row_count_tests {
    use plomid_executor::maintenance::*;

    #[test]
    fn bulk_commit_trips_the_bound_in_one_statement() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenancePolicy::new(64));
        // A single 5,000-row bulk commit accrues row debt, not one unit.
        assert!(coordinator.record_committed_mutations("public.t", 5000));
        assert_eq!(coordinator.committed_mutations("public.t"), 5000);
        assert_eq!(coordinator.due_tables(), vec!["public.t".to_string()]);
    }

    #[test]
    fn single_row_commits_accrue_exactly_as_before() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenancePolicy::new(3));
        assert!(!coordinator.record_committed_mutations("public.t", 1));
        assert!(!coordinator.record_committed_mutations("public.t", 1));
        assert!(coordinator.record_committed_mutations("public.t", 1));
        assert_eq!(coordinator.committed_mutations("public.t"), 3);
    }

    #[test]
    fn empty_writes_accrue_no_debt() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenancePolicy::new(1));
        assert!(!coordinator.record_committed_mutations("public.t", 0));
        assert!(coordinator.due_tables().is_empty());
    }
}

mod auto_interval_tests {
    use plomid_executor::maintenance::*;

    use std::time::Duration;

    #[test]
    fn zero_interval_stays_zero() {
        let policy = MaintenancePolicy::new(1);
        assert!(policy.min_auto_interval().is_zero());
        assert!(effective_auto_interval(&policy, Duration::from_secs(3600)).is_zero());
    }

    #[test]
    fn interval_scales_with_pass_duration() {
        let policy = MaintenancePolicy::production();
        assert_eq!(
            effective_auto_interval(&policy, Duration::from_secs(1)),
            Duration::from_secs(60)
        );
        assert_eq!(
            effective_auto_interval(&policy, Duration::from_secs(100)),
            Duration::from_secs(300)
        );
    }

    #[test]
    fn gate_denies_within_scaled_window() {
        let mut coordinator = MaintenanceCoordinator::new(MaintenancePolicy::production());
        let key = "public.t";
        // A 100s pass just completed: the 60s floor stretches to 300s.
        coordinator.record_committed_mutations(key, 10_000);
        assert!(coordinator.due_tables().contains(&key.to_string()));
        // Simulate the completed pass by recording directly.
        record_auto_pass(key, Duration::from_secs(100));
        assert!(!auto_pass_allowed(key, &MaintenancePolicy::production()));
        assert_eq!(last_pass_duration(key), Duration::from_secs(100));
    }
}

mod tier_tests {
    use plomid_executor::maintenance::*;

    #[test]
    fn edge_policy_is_production_scaled_by_four() {
        assert_eq!(
            MaintenancePolicy::EDGE_COMMITTED_MUTATIONS_PER_GENERATION,
            MaintenancePolicy::DEFAULT_COMMITTED_MUTATIONS_PER_GENERATION * 4
        );
        assert_eq!(MaintenancePolicy::EDGE_INTERVAL_SCALE, 4);
        let edge = MaintenancePolicy::edge();
        assert_eq!(
            edge.committed_mutations_per_generation(),
            MaintenancePolicy::DEFAULT_COMMITTED_MUTATIONS_PER_GENERATION * 4
        );
        assert_eq!(
            edge.min_auto_interval(),
            MaintenancePolicy::PRODUCTION_MIN_AUTO_INTERVAL
                * MaintenancePolicy::EDGE_INTERVAL_SCALE
        );
        // Explicit construction is unaffected by tiers.
        assert_eq!(
            MaintenancePolicy::new(7).committed_mutations_per_generation(),
            7
        );
    }

    #[test]
    fn for_tier_maps_edge_to_edge_and_keeps_current_values_elsewhere() {
        use plomid_hw::RuntimeTier::{Edge, Laptop, Server};
        assert_eq!(MaintenancePolicy::for_tier(Edge), MaintenancePolicy::edge());
        // Laptop and server tiers keep exactly today's aggressive values:
        // the tier default never sharpens or relaxes them.
        assert_eq!(
            MaintenancePolicy::for_tier(Laptop),
            MaintenancePolicy::production()
        );
        assert_eq!(
            MaintenancePolicy::for_tier(Server),
            MaintenancePolicy::production()
        );
        let production = MaintenancePolicy::production();
        assert_eq!(
            production.committed_mutations_per_generation(),
            MaintenancePolicy::DEFAULT_COMMITTED_MUTATIONS_PER_GENERATION
        );
        assert_eq!(
            production.min_auto_interval(),
            MaintenancePolicy::PRODUCTION_MIN_AUTO_INTERVAL
        );
    }

    #[test]
    fn explicit_policy_still_wins_over_the_tier_default() {
        // Override semantics are ordering-based: whatever is set last wins,
        // so an operator-explicit set after the tier default governs.
        let mut coordinator =
            MaintenanceCoordinator::new(MaintenancePolicy::for_tier(plomid_hw::RuntimeTier::Edge));
        assert_eq!(
            coordinator.policy().committed_mutations_per_generation(),
            256
        );
        coordinator.set_policy(MaintenancePolicy::new(3));
        assert_eq!(coordinator.policy().committed_mutations_per_generation(), 3);
    }
}
