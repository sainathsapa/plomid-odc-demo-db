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
// Moved from `src/server/guardrails.rs` inline `#[cfg(test)]` — public-API only.
use plomid_network::server::guardrails::*;
use std::net::IpAddr;
use std::time::Duration;

fn ip(n: u8) -> IpAddr {
    IpAddr::from([127, 0, 0, n])
}

#[test]
fn delay_grows_and_resets_on_success() {
    let mut limiter = AuthRateLimiter::with_defaults();
    let addr = ip(1);
    assert_eq!(limiter.delay_for(&addr), Duration::ZERO);
    limiter.record_failure(addr);
    assert_eq!(limiter.delay_for(&addr), Duration::from_millis(100));
    limiter.record_failure(addr);
    assert_eq!(limiter.delay_for(&addr), Duration::from_millis(200));
    limiter.record_failure(addr);
    assert_eq!(limiter.delay_for(&addr), Duration::from_millis(400));
    limiter.record_success(addr);
    assert_eq!(limiter.delay_for(&addr), Duration::ZERO);
    assert_eq!(limiter.failure_count(&addr), 0);
}

#[test]
fn delay_is_capped() {
    let mut limiter = AuthRateLimiter::new(16, Duration::from_millis(250));
    let addr = ip(2);
    for _ in 0..10 {
        limiter.record_failure(addr);
    }
    assert!(limiter.delay_for(&addr) <= Duration::from_millis(250));
}

#[test]
fn table_is_bounded() {
    let mut limiter = AuthRateLimiter::new(4, Duration::from_secs(5));
    for n in 1..=6u8 {
        limiter.record_failure(ip(n));
    }
    assert!(limiter.len() <= 4);
}
