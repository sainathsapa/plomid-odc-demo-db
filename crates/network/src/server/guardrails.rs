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
//! Query guardrails shared by the server: cancel registry + auth rate limit.
//!
//! * Cancel: each connection registers `(pid, secret, cancel-flag)`. A
//!   CancelRequest connection looks the target up and sets its flag. The
//!   running query polls the flag through the executor guard and aborts
//!   with a clean cancel error. All engine mutations are transactional, so
//!   aborting a query never corrupts shared state.
//! * Auth rate limit: per-IP failed-attempt counter with growing,
//!   capped delay, bounded to the last N IPs. Success resets the counter.

use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::sync::{atomic::AtomicBool, Arc};
use std::time::{Duration, Instant};

/// One live connection's cancel identity.
#[derive(Debug, Clone)]
pub struct CancelEntry {
    pub secret: u32,
    pub cancel: Arc<AtomicBool>,
}

/// `pid -> CancelEntry`, shared by all connections on one listener.
pub type CancelRegistry = Arc<std::sync::Mutex<HashMap<u32, CancelEntry>>>;

/// Creates an empty cancel registry.
pub fn new_cancel_registry() -> CancelRegistry {
    Arc::new(std::sync::Mutex::new(HashMap::new()))
}

/// Maximum tracked IPs for auth rate limiting.
pub const AUTH_RATE_LIMIT_CAPACITY: usize = 128;
/// Base delay for the first failure; doubles per consecutive failure.
pub const AUTH_RATE_LIMIT_BASE: Duration = Duration::from_millis(100);
/// Upper bound for any single pre-auth delay.
pub const AUTH_RATE_LIMIT_MAX: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct AuthEntry {
    failures: u32,
    last_failure: Instant,
}

/// Per-IP failed-auth counter with growing, capped delay.
///
/// Bounded: only the last `capacity` distinct IPs are tracked; the oldest
/// is evicted on overflow. `delay_for` is pure (no mutation) so the handler
/// can drop the lock before sleeping. `record_failure`/`record_success`
/// mutate.
#[derive(Debug)]
pub struct AuthRateLimiter {
    capacity: usize,
    max_delay: Duration,
    entries: HashMap<IpAddr, AuthEntry>,
    order: VecDeque<IpAddr>,
}

impl AuthRateLimiter {
    pub fn new(capacity: usize, max_delay: Duration) -> Self {
        Self {
            capacity: capacity.max(1),
            max_delay,
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(AUTH_RATE_LIMIT_CAPACITY, AUTH_RATE_LIMIT_MAX)
    }

    /// Delay to apply before the next auth attempt from `ip`.
    pub fn delay_for(&self, ip: &IpAddr) -> Duration {
        let Some(entry) = self.entries.get(ip) else {
            return Duration::ZERO;
        };
        if entry.failures == 0 {
            return Duration::ZERO;
        }
        // Growing: base * 2^(failures-1), capped.
        let shift = (entry.failures.saturating_sub(1)).min(10);
        let delay = AUTH_RATE_LIMIT_BASE.saturating_mul(1 << shift);
        delay.min(self.max_delay)
    }

    /// Records a failed attempt from `ip`.
    pub fn record_failure(&mut self, ip: IpAddr) {
        let now = Instant::now();
        if let Some(entry) = self.entries.get_mut(&ip) {
            entry.failures = entry.failures.saturating_add(1);
            entry.last_failure = now;
            // Refresh recency.
            if let Some(pos) = self.order.iter().position(|known| known == &ip) {
                self.order.remove(pos);
            }
            self.order.push_back(ip);
            return;
        }
        // Evict oldest when at capacity.
        while self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
        self.entries.insert(
            ip,
            AuthEntry {
                failures: 1,
                last_failure: now,
            },
        );
        self.order.push_back(ip);
    }

    /// Records a successful authentication from `ip` (resets the counter).
    pub fn record_success(&mut self, ip: IpAddr) {
        self.entries.remove(&ip);
        if let Some(pos) = self.order.iter().position(|known| known == &ip) {
            self.order.remove(pos);
        }
    }

    pub fn failure_count(&self, ip: &IpAddr) -> u32 {
        self.entries.get(ip).map(|e| e.failures).unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

pub type SharedAuthRateLimiter = Arc<tokio::sync::Mutex<AuthRateLimiter>>;

pub fn new_auth_rate_limiter() -> SharedAuthRateLimiter {
    Arc::new(tokio::sync::Mutex::new(AuthRateLimiter::with_defaults()))
}

/// Consecutive failures that lock an account.
pub const ACCOUNT_LOCKOUT_THRESHOLD: u32 = 5;
/// How long a lockout lasts. Successful authentication is impossible while
/// locked; the counter resets on the next success after expiry.
pub const ACCOUNT_LOCKOUT_DURATION: Duration = Duration::from_secs(300);
/// Maximum tracked usernames (bounded like the IP limiter).
pub const ACCOUNT_LOCKOUT_CAPACITY: usize = 1024;

#[derive(Debug)]
struct LockoutEntry {
    consecutive: u32,
    locked_until: Option<Instant>,
}

/// Per-username account lockout (brute-force backstop).
///
/// Complements the per-IP rate limiter: the limiter slows guessing from one
/// source, the lockout stops guessing at one account no matter the source.
/// State is in-memory and process-local (a restart clears lockouts —
/// documented; durable lockout is a non-goal that would risk permanent
/// self-lockout of the only superuser).
#[derive(Debug)]
pub struct AccountLockout {
    capacity: usize,
    entries: HashMap<String, LockoutEntry>,
    order: VecDeque<String>,
}

impl AccountLockout {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            entries: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(ACCOUNT_LOCKOUT_CAPACITY)
    }

    fn touch(&mut self, user: &str) {
        if let Some(pos) = self.order.iter().position(|known| known == user) {
            self.order.remove(pos);
        }
        self.order.push_back(user.to_string());
        while self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }
    }

    /// True while the account is locked (expired lockouts clear lazily).
    pub fn is_locked(&mut self, user: &str) -> bool {
        let now = Instant::now();
        if let Some(entry) = self.entries.get_mut(user) {
            if let Some(until) = entry.locked_until {
                if now < until {
                    return true;
                }
                entry.locked_until = None;
                entry.consecutive = 0;
            }
        }
        false
    }

    /// Records a failed attempt. Returns true when this failure newly locks
    /// the account (the caller emits the lockout audit event + counter).
    pub fn record_failure(&mut self, user: &str) -> bool {
        self.touch(user);
        let entry = self
            .entries
            .entry(user.to_string())
            .or_insert(LockoutEntry {
                consecutive: 0,
                locked_until: None,
            });
        entry.consecutive = entry.consecutive.saturating_add(1);
        if entry.consecutive >= ACCOUNT_LOCKOUT_THRESHOLD && entry.locked_until.is_none() {
            entry.locked_until = Some(Instant::now() + ACCOUNT_LOCKOUT_DURATION);
            return true;
        }
        false
    }

    /// Records a success (resets the consecutive counter, never locks).
    pub fn record_success(&mut self, user: &str) {
        self.entries.remove(user);
        if let Some(pos) = self.order.iter().position(|known| known == user) {
            self.order.remove(pos);
        }
    }
}

pub type SharedAccountLockout = Arc<tokio::sync::Mutex<AccountLockout>>;

pub fn new_account_lockout() -> SharedAccountLockout {
    Arc::new(tokio::sync::Mutex::new(AccountLockout::with_defaults()))
}
