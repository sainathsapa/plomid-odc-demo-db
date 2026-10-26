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

//! HTAP configuration and memory-pressure states.

/// Gibibyte helper for readable defaults.
const GIB: u64 = 1024 * 1024 * 1024;

/// Global memory-pressure state derived from budget utilization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PressureState {
    /// Utilization below the warning threshold. Full speed.
    Normal,
    /// Utilization above warning: OLAP concurrency/parallelism derated.
    Warning,
    /// Utilization above pressure threshold: maintenance degraded, OLAP
    /// heavily derated.
    Pressure,
    /// Utilization above critical threshold: maintenance paused, OLAP load
    /// shed (queued), OLTP still admitted while global budget remains.
    Critical,
}

impl PressureState {
    /// Map a utilization percent (0-100) to a state using the default
    /// thresholds 70 / 85 / 95. See [`HtapConfig::pressure_for`] for the
    /// configurable-threshold variant.
    pub fn for_utilization(pct: u8) -> Self {
        if pct < 70 {
            PressureState::Normal
        } else if pct < 85 {
            PressureState::Warning
        } else if pct < 95 {
            PressureState::Pressure
        } else {
            PressureState::Critical
        }
    }
}

/// Configuration validation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    reason: String,
}

impl ConfigError {
    fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    /// Human-readable reason the config was rejected.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid HTAP config: {}", self.reason)
    }
}

impl std::error::Error for ConfigError {}

/// All HTAP governance knobs in one place.
#[derive(Debug, Clone)]
pub struct HtapConfig {
    /// Total memory budget across pools, caches, and result buffers.
    pub global_budget_bytes: u64,
    /// Bytes fenced off for OLTP (+system) use. OLAP/batch/background/
    /// maintenance traffic cannot push global usage into this reserve
    /// while OLTP pressure exists.
    pub oltp_reservation_bytes: u64,
    /// Hard byte cap for the OLAP pool.
    pub olap_budget_bytes: u64,
    /// Hard byte cap for the maintenance pool.
    pub maintenance_budget_bytes: u64,
    /// Hard byte cap for the system pool.
    pub system_reserve_bytes: u64,
    /// Max concurrent OLTP workers (never derated).
    pub oltp_max_concurrency: usize,
    /// Max concurrent OLAP workers at [`PressureState::Normal`]; derated
    /// under pressure.
    pub olap_max_concurrency: usize,
    /// Max concurrent maintenance workers.
    pub maintenance_max_concurrency: usize,
    /// Max concurrent system workers.
    pub system_max_concurrency: usize,
    /// OLTP degree of parallelism (never derated for pressure).
    pub oltp_max_parallelism: usize,
    /// OLAP degree of parallelism at [`PressureState::Normal`]; derated
    /// 8 -> 4 -> 2 -> 1 as pressure rises.
    pub olap_max_parallelism: usize,
    /// Per-query byte cap for OLAP-family queries (no single query may
    /// starve the pool).
    pub olap_max_bytes_per_query: u64,
    /// Default statement timeout; `None` = unlimited.
    pub statement_timeout_ms: Option<u64>,
    /// Documented intent flag: OLTP takes the latency-priority path.
    pub oltp_priority_latency: bool,
    /// Utilization % at which [`PressureState::Warning`] begins.
    pub warning_threshold_pct: u8,
    /// Utilization % at which [`PressureState::Pressure`] begins.
    pub pressure_threshold_pct: u8,
    /// Utilization % at which [`PressureState::Critical`] begins.
    pub critical_threshold_pct: u8,
    /// Row-count threshold for OLTP inference (see `workload::classify`).
    pub small_txn_row_threshold: u64,
}

impl Default for HtapConfig {
    fn default() -> Self {
        Self {
            global_budget_bytes: 40 * GIB,
            oltp_reservation_bytes: 8 * GIB,
            olap_budget_bytes: 24 * GIB,
            maintenance_budget_bytes: 4 * GIB,
            system_reserve_bytes: GIB,
            oltp_max_concurrency: 256,
            olap_max_concurrency: 16,
            maintenance_max_concurrency: 4,
            system_max_concurrency: 8,
            oltp_max_parallelism: 4,
            olap_max_parallelism: 8,
            olap_max_bytes_per_query: 8 * GIB,
            statement_timeout_ms: None,
            oltp_priority_latency: true,
            warning_threshold_pct: 70,
            pressure_threshold_pct: 85,
            critical_threshold_pct: 95,
            small_txn_row_threshold: super::workload::SMALL_TXN_ROW_THRESHOLD,
        }
    }
}

impl HtapConfig {
    /// Map a utilization percent to a [`PressureState`] using this
    /// config's thresholds.
    pub fn pressure_for(&self, pct: u8) -> PressureState {
        if pct < self.warning_threshold_pct {
            PressureState::Normal
        } else if pct < self.pressure_threshold_pct {
            PressureState::Warning
        } else if pct < self.critical_threshold_pct {
            PressureState::Pressure
        } else {
            PressureState::Critical
        }
    }

    /// Reject overcommit and nonsense: reserve + pool budgets must fit in
    /// the global budget, thresholds must be ordered, caps must be usable.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.global_budget_bytes == 0 {
            return Err(ConfigError::new("global_budget_bytes must be > 0"));
        }
        let sum = self
            .oltp_reservation_bytes
            .checked_add(self.olap_budget_bytes)
            .and_then(|s| s.checked_add(self.maintenance_budget_bytes))
            .and_then(|s| s.checked_add(self.system_reserve_bytes))
            .ok_or_else(|| ConfigError::new("budget sum overflows u64"))?;
        if sum > self.global_budget_bytes {
            return Err(ConfigError::new(format!(
                "overcommit: reserve+budgets={sum} exceed global={}",
                self.global_budget_bytes
            )));
        }
        for (name, v) in [
            ("oltp_max_concurrency", self.oltp_max_concurrency),
            ("olap_max_concurrency", self.olap_max_concurrency),
            (
                "maintenance_max_concurrency",
                self.maintenance_max_concurrency,
            ),
            ("system_max_concurrency", self.system_max_concurrency),
            ("oltp_max_parallelism", self.oltp_max_parallelism),
            ("olap_max_parallelism", self.olap_max_parallelism),
        ] {
            if v == 0 {
                return Err(ConfigError::new(format!("{name} must be >= 1")));
            }
        }
        if self.warning_threshold_pct == 0
            || self.warning_threshold_pct >= self.pressure_threshold_pct
            || self.pressure_threshold_pct >= self.critical_threshold_pct
            || self.critical_threshold_pct > 100
        {
            return Err(ConfigError::new(
                "thresholds must satisfy 0 < warning < pressure < critical <= 100",
            ));
        }
        if self.olap_max_bytes_per_query == 0 {
            return Err(ConfigError::new("olap_max_bytes_per_query must be > 0"));
        }
        if self.olap_max_bytes_per_query > self.olap_budget_bytes {
            return Err(ConfigError::new(
                "olap_max_bytes_per_query exceeds olap_budget_bytes",
            ));
        }
        Ok(())
    }
}
