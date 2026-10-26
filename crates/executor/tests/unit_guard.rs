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
// Moved from src/guard.rs inline #[cfg(test)].
use plomid_executor::guard::*;
use plomid_htap::StatementPolicy;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::{Duration, Instant};

#[test]
fn zero_means_disabled() {
    assert_eq!(parse_timeout(Some("0")), None);
    assert_eq!(parse_timeout(Some("0s")), None);
    assert_eq!(parse_timeout(Some("0ms")), None);
    assert_eq!(parse_timeout(None), None);
    assert_eq!(parse_timeout(Some("")), None);
}

#[test]
fn bare_number_is_milliseconds() {
    assert_eq!(parse_timeout(Some("100")), Some(Duration::from_millis(100)));
    assert_eq!(parse_timeout(Some("1s")), Some(Duration::from_secs(1)));
    assert_eq!(
        parse_timeout(Some("1.5s")),
        Some(Duration::from_millis(1500))
    );
    assert_eq!(parse_timeout(Some("2min")), Some(Duration::from_secs(120)));
}

#[test]
fn unparseable_is_fail_open_disabled() {
    assert_eq!(parse_timeout(Some("nonsense")), None);
    assert_eq!(parse_timeout(Some("10 lightyears")), None);
}

#[test]
fn guard_reports_timeout_and_cancel() {
    // No guard: always ok.
    assert!(check_guard().is_ok());
    // Expired deadline.
    let past = Instant::now() - Duration::from_secs(1);
    with_guard(Some(past), None, || {
        let err = check_guard().expect_err("expired deadline must fail");
        assert!(is_guard_error(&err));
        assert!(!is_cancel_error(&err));
    });
    // Cancel flag.
    let flag = Arc::new(AtomicBool::new(true));
    with_guard(None, Some(flag), || {
        let err = check_guard().expect_err("set flag must fail");
        assert!(is_cancel_error(&err));
    });
    // Guard is restored afterwards.
    assert!(check_guard().is_ok());
}

#[test]
fn policy_from_param_round_trips_through_htap() {
    assert!(policy_from_param(None).is_unlimited());
    assert!(policy_from_param(Some("0")).is_unlimited());
    assert!(policy_from_param(Some("nonsense")).is_unlimited());
    assert_eq!(
        policy_from_param(Some("100")),
        StatementPolicy::with_timeout(100)
    );
    assert_eq!(
        policy_from_param(Some("1.5s")),
        StatementPolicy::with_timeout(1500)
    );
    // Deadline path executes the HTAP policy (not an ad-hoc add).
    let deadline = deadline_from_param(Some("100")).expect("must have deadline");
    assert!(deadline > Instant::now());
    assert!(deadline_from_param(None).is_none());
    assert!(deadline_from_param(Some("0")).is_none());
}

#[test]
fn check_policy_maps_timeout_error_to_guard_error() {
    let start = Instant::now();
    assert!(check_policy(start, StatementPolicy::unlimited()).is_ok());
    assert!(check_policy(start, StatementPolicy::with_timeout(60_000)).is_ok());
    let past = Instant::now() - Duration::from_secs(5);
    let err = check_policy(past, StatementPolicy::with_timeout(100))
        .expect_err("expired policy must fail");
    assert!(is_guard_error(&err));
    assert!(!is_cancel_error(&err));
    // Conversion preserves guard classification on its own.
    let policy_err = StatementPolicy::with_timeout(10)
        .check(past)
        .expect_err("must time out");
    let sql_err = timeout_error_to_sql(policy_err);
    assert!(is_guard_error(&sql_err));
    assert!(!is_cancel_error(&sql_err));
}
