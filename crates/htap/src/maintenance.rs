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

//! Maintenance governance: throttle background upkeep under pressure.
//!
//! Maintenance (compaction, vacuum, reindex, stats) must never starve OLTP:
//! it is degraded at [`PressureState::Pressure`] and fully paused at
//! [`PressureState::Critical`]. Mild [`PressureState::Warning`] pressure
//! keeps maintenance at full speed so compaction debt does not accumulate.

use crate::config::PressureState;

/// How much maintenance work may proceed right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaintenanceThrottle {
    /// Run normally.
    Full,
    /// Run with reduced resources (smaller batches, fewer workers).
    Degraded,
    /// Stop entirely until pressure drops.
    Paused,
}

impl MaintenanceThrottle {
    /// True only for [`MaintenanceThrottle::Paused`].
    pub fn is_paused(self) -> bool {
        self == MaintenanceThrottle::Paused
    }

    /// True for [`MaintenanceThrottle::Full`] and [`MaintenanceThrottle::Degraded`].
    pub fn allows_work(self) -> bool {
        self != MaintenanceThrottle::Paused
    }
}

/// Throttle maintenance for the given pressure state.
pub fn throttle(pressure: PressureState) -> MaintenanceThrottle {
    match pressure {
        PressureState::Normal | PressureState::Warning => MaintenanceThrottle::Full,
        PressureState::Pressure => MaintenanceThrottle::Degraded,
        PressureState::Critical => MaintenanceThrottle::Paused,
    }
}
