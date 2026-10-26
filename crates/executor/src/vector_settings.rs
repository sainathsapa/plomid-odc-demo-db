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
//! Server-scope vector settings (`SET` / `SHOW` / `pg_show_all_settings`).
//!
//! Every knob carries its default, minimum, maximum and scope in one table
//! ([`ALL`]), so operators never guess a range and validation cannot drift
//! from documentation. Values are validated on write: out-of-range input
//! fails the `SET` with the allowed interval instead of clamping silently.
//!
//! Scope in v1 is server-global (process-wide defaults shared by all
//! sessions); per-session and per-transaction overrides are the documented
//! follow-up. The ANN execution path reads these on every query, so a `SET`
//! takes effect immediately without restart.

use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use plomid_core::{ErrorKind, PlomidError};

/// One validated knob: name, bounds and default.
#[derive(Clone, Debug)]
pub struct SettingDef {
    /// Lowercase `vector.*` name used by `SET`/`SHOW`.
    pub name: &'static str,
    /// Human-readable value kind (`bool`, `integer`, `enum`).
    pub vartype: &'static str,
    /// Inclusive minimum (integers only; `None` for bools/enums).
    pub min: Option<i64>,
    /// Inclusive maximum (integers only; `None` for bools/enums).
    pub max: Option<i64>,
    /// Allowed spellings (enums only, lowercase-compared; `None` otherwise).
    pub allowed: Option<&'static [&'static str]>,
    /// Default spelling returned before any `SET`.
    pub default: &'static str,
}

/// The full knob table: the single source of truth for validation,
/// `SHOW`, `pg_show_all_settings` and user documentation.
pub const ALL: &[SettingDef] = &[
    SettingDef {
        name: "vector.ann_enabled",
        vartype: "bool",
        min: None,
        max: None,
        allowed: None,
        default: "on",
    },
    SettingDef {
        name: "vector.ef_search",
        vartype: "integer",
        min: Some(1),
        max: Some(4096),
        allowed: None,
        default: "100",
    },
    SettingDef {
        name: "vector.candidate_oversample",
        vartype: "integer",
        min: Some(1),
        max: Some(64),
        allowed: None,
        default: "8",
    },
    SettingDef {
        name: "vector.max_ann_rows",
        vartype: "integer",
        min: Some(0),
        max: Some(16_777_216),
        allowed: None,
        default: "1000000",
    },
    SettingDef {
        name: "vector.cache_graphs",
        vartype: "integer",
        min: Some(1),
        max: Some(64),
        allowed: None,
        default: "8",
    },
    SettingDef {
        name: "vector.max_index_vectors",
        vartype: "integer",
        min: Some(1),
        max: Some(16_000_000),
        allowed: None,
        default: "1000000",
    },
    SettingDef {
        name: "vector.auto_ann_min_rows",
        vartype: "integer",
        min: Some(0),
        max: Some(1_000_000_000),
        allowed: None,
        default: "4096",
    },
    SettingDef {
        name: "vector.compute",
        vartype: "enum",
        min: None,
        max: None,
        allowed: Some(&["auto", "cpu", "gpu"]),
        default: "auto",
    },
    SettingDef {
        name: "vector.max_memory_bytes",
        vartype: "integer",
        min: Some(1_048_576),
        max: Some(1_099_511_627_776),
        allowed: None,
        default: "1073741824",
    },
];

fn overrides() -> &'static RwLock<HashMap<String, String>> {
    static OVERRIDES: OnceLock<RwLock<HashMap<String, String>>> = OnceLock::new();
    OVERRIDES.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Looks up a knob definition (case-insensitive).
#[must_use]
pub fn definition(name: &str) -> Option<&'static SettingDef> {
    ALL.iter()
        .find(|def| def.name.eq_ignore_ascii_case(name.trim()))
}

/// Reads the effective value: override if `SET`, else the default.
#[must_use]
pub fn get(name: &str) -> Option<String> {
    let def = definition(name)?;
    overrides()
        .read()
        .ok()
        .and_then(|guard| guard.get(def.name).cloned())
        .or_else(|| Some(def.default.to_string()))
}

/// Reads the effective value as an integer (definitions guarantee parsing).
#[must_use]
pub fn get_int(name: &str) -> Option<i64> {
    get(name)?.parse().ok()
}

/// Whether ANN execution is enabled (`vector.ann_enabled`).
#[must_use]
pub fn ann_enabled() -> bool {
    get("vector.ann_enabled")
        .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "on" | "true" | "1"))
}

/// Compute backend preference (`vector.compute`): `auto` (default), `cpu`
/// or `gpu`. There is no GPU kernel linked in v1, so `gpu` executes on the
/// CPU through the documented graceful-degradation path and records a
/// fallback event; it never fails the query.
#[must_use]
pub fn compute() -> String {
    get("vector.compute").unwrap_or_else(|| "auto".to_string())
}

/// Validates and stores a knob. Unknown names and out-of-range values fail
/// with the allowed set spelled out; nothing is clamped silently.
pub fn set(name: &str, value: &str) -> Result<(), PlomidError> {
    let def = definition(name).ok_or_else(|| {
        PlomidError::new(
            ErrorKind::InvalidArgument,
            format!("unrecognized setting \"{name}\""),
        )
    })?;
    let trimmed = value.trim();
    if def.vartype == "bool" {
        match trimmed.to_ascii_lowercase().as_str() {
            "on" | "off" | "true" | "false" | "1" | "0" => {}
            _ => {
                return Err(PlomidError::new(
                    ErrorKind::InvalidArgument,
                    format!(
                        "invalid value \"{trimmed}\" for \"{}\": expected on/off",
                        def.name
                    ),
                ));
            }
        }
    } else if def.vartype == "enum" {
        let allowed = def.allowed.unwrap_or(&[]);
        if !allowed
            .iter()
            .any(|option| option.eq_ignore_ascii_case(trimmed))
        {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                format!(
                    "invalid value \"{trimmed}\" for \"{}\": expected one of {}",
                    def.name,
                    allowed.join(", ")
                ),
            ));
        }
    } else if let (Some(lo), Some(hi)) = (def.min, def.max) {
        let parsed: i64 = trimmed.parse().map_err(|_| {
            PlomidError::new(
                ErrorKind::InvalidArgument,
                format!("invalid integer \"{trimmed}\" for \"{}\"", def.name),
            )
        })?;
        if parsed < lo || parsed > hi {
            return Err(PlomidError::new(
                ErrorKind::InvalidArgument,
                format!(
                    "value {parsed} out of range for \"{}\": {lo}..={hi}",
                    def.name
                ),
            ));
        }
    }
    overrides()
        .write()
        .map(|mut guard| {
            guard.insert(def.name.to_string(), trimmed.to_string());
        })
        .map_err(|_| {
            PlomidError::new(
                ErrorKind::Internal,
                "vector settings lock poisoned".to_string(),
            )
        })
}
