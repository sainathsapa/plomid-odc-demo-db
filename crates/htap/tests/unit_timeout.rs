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
// Moved from `src/timeout.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::timeout::*;
use std::time::{Duration, Instant};

#[test]
fn unlimited_never_expires() {
    let policy = StatementPolicy::unlimited();
    assert!(policy.is_unlimited());
    assert!(policy.deadline().is_none());
    let ancient = Instant::now() - Duration::from_secs(3600);
    assert!(!policy.is_expired(ancient));
    assert!(policy.check(ancient).is_ok());
}

#[test]
fn deadline_is_start_plus_timeout() {
    let policy = StatementPolicy::with_timeout(100);
    let start = Instant::now();
    let deadline = policy.deadline_from(start).expect("must have deadline");
    assert!(deadline >= start + Duration::from_millis(100));
    assert!(deadline < start + Duration::from_millis(200));
    assert!(!policy.is_expired(start));
    let past = Instant::now() - Duration::from_millis(500);
    assert!(policy.is_expired(past));
    assert!(policy.check(past).is_err());
}
