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

//! [`MemoryConfig`] and [`PressureThresholds`].
//!
//! All byte values are raw `u64` counts. The [`Default`] implementation
//! provides test-sized values only (1 GiB global); production deployments
//! must size every budget explicitly for their hardware.

/// Fractional usage thresholds at which [`crate::PressureState`] escalates.
///
/// Defaults are 0.70 (warning), 0.85 (pressure), 0.95 (critical).
/// Bounds are inclusive: a usage ratio `>= warning` reports
/// [`crate::PressureState::Warning`], and so on.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PressureThresholds {
    /// Usage fraction at or above which state becomes `Warning`.
    pub warning: f64,
    /// Usage fraction at or above which state becomes `Pressure`.
    pub pressure: f64,
    /// Usage fraction at or above which state becomes `Critical`.
    pub critical: f64,
}

impl PressureThresholds {
    /// Build thresholds explicitly. Values are used as-is; callers should
    /// keep `warning <= pressure <= critical` inside `[0.0, 1.0]`.
    pub fn new(warning: f64, pressure: f64, critical: f64) -> Self {
        Self {
            warning,
            pressure,
            critical,
        }
    }
}

impl Default for PressureThresholds {
    fn default() -> Self {
        Self {
            warning: 0.70,
            pressure: 0.85,
            critical: 0.95,
        }
    }
}

/// Top-level configuration for [`crate::MemoryManager`].
///
/// Field values are defaults-only placeholders, not production sizing
/// guidance. Every field is configurable at construction time.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MemoryConfig {
    /// Total bytes the engine may hold across all contexts.
    pub global_budget: u64,
    /// Bytes conceptually reserved for OLTP workload-class contexts.
    pub oltp_reservation: u64,
    /// Budget for OLAP workload-class contexts.
    pub olap_budget: u64,
    /// Budget for the maintenance branch (vacuum, checkpoint, …).
    pub maintenance_budget: u64,
    /// Default budget for a single query context.
    pub query_budget_default: u64,
    /// Default budget for a single connection context.
    pub connection_budget: u64,
    /// Default budget for a single worker context.
    pub worker_budget: u64,
    /// Default budget for `ResultBuffer` operator contexts.
    pub result_buffer_budget: u64,
    /// Budget for temp-table / temp-file operator contexts.
    pub temp_limit: u64,
    /// Pressure-state escalation thresholds.
    pub pressure_thresholds: PressureThresholds,
}

impl MemoryConfig {
    /// Override just the global budget, keeping every other default.
    pub fn with_global_budget(mut self, bytes: u64) -> Self {
        self.global_budget = bytes;
        self
    }
}

impl Default for MemoryConfig {
    /// Test-sized defaults (1 GiB global). Deployments must configure
    /// real values; these exist so unit tests need no sizing knowledge.
    fn default() -> Self {
        Self {
            global_budget: 1 << 30,
            oltp_reservation: 256 << 20,
            olap_budget: 512 << 20,
            maintenance_budget: 128 << 20,
            query_budget_default: 64 << 20,
            connection_budget: 16 << 20,
            worker_budget: 8 << 20,
            result_buffer_budget: 32 << 20,
            temp_limit: 256 << 20,
            pressure_thresholds: PressureThresholds::default(),
        }
    }
}
