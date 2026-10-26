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

//! [`PressureState`]: engine-wide memory pressure derived from global
//! usage.

use std::fmt;

use crate::config::PressureThresholds;

/// Engine-wide memory pressure, computed from
/// `global.current / global.budget` against [`PressureThresholds`].
///
/// Bounds are inclusive: `Normal < Warning < Pressure < Critical`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum PressureState {
    /// Usage below the warning threshold.
    Normal = 0,
    /// Usage at or above warning, below pressure.
    Warning = 1,
    /// Usage at or above pressure, below critical.
    Pressure = 2,
    /// Usage at or above critical.
    Critical = 3,
}

impl PressureState {
    /// Map a usage ratio to a state. `NaN` maps to `Normal`;
    /// `INFINITY` (over a zero budget) maps to `Critical`.
    pub fn from_usage(ratio: f64, thresholds: &PressureThresholds) -> Self {
        if ratio >= thresholds.critical {
            Self::Critical
        } else if ratio >= thresholds.pressure {
            Self::Pressure
        } else if ratio >= thresholds.warning {
            Self::Warning
        } else {
            Self::Normal
        }
    }

    /// Stable name (`"NORMAL"`, `"WARNING"`, `"PRESSURE"`, `"CRITICAL"`).
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Warning => "WARNING",
            Self::Pressure => "PRESSURE",
            Self::Critical => "CRITICAL",
        }
    }

    /// Decode a `#[repr(u8)]` discriminant; unknown values map to `Normal`.
    pub(crate) fn from_u8(v: u8) -> Self {
        match v {
            1 => Self::Warning,
            2 => Self::Pressure,
            3 => Self::Critical,
            _ => Self::Normal,
        }
    }
}

impl fmt::Display for PressureState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}
