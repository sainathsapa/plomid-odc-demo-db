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
//! Shared CLI helpers: connection options, percentile math, JSON escaping.
//!
//! Extracted from the `plomid` binary so unit tests live in `tests/`
//! (integration tests link this lib; a binary-only crate has no importable
//! target). The binary re-exports these; behavior is unchanged.

#![forbid(unsafe_code)]

use clap::Args;
use std::path::PathBuf;
use std::time::Duration;

use plomid_core::{DEFAULT_DATABASE, DEFAULT_DATA_DIR, DEFAULT_HOST, DEFAULT_PORT, DEFAULT_USER};

/// Connection options shared by every CLI subcommand.
#[derive(Args, Clone)]
pub struct ConnectionOptions {
    #[arg(long, env = "PLOMID_HOST", default_value = DEFAULT_HOST)]
    pub host: String,
    #[arg(long, env = "PLOMID_PORT", default_value_t = DEFAULT_PORT)]
    pub port: u16,
    #[arg(long, env = "PLOMID_USER", default_value = DEFAULT_USER)]
    pub user: String,
    #[arg(
        long,
        env = "PLOMID_PASSWORD",
        default_value = "secret",
        hide_env_values = true
    )]
    pub password: String,
    #[arg(long, env = "PLOMID_DATABASE", default_value = DEFAULT_DATABASE)]
    pub database: String,
    #[arg(long, env = "PLOMID_DATA_DIR", default_value = DEFAULT_DATA_DIR)]
    pub data_dir: PathBuf,
}

impl std::fmt::Debug for ConnectionOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionOptions")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"<redacted>")
            .field("database", &self.database)
            .field("data_dir", &self.data_dir)
            .finish()
    }
}

/// p-th percentile of `latencies` in milliseconds (nearest-rank).
pub fn percentile_ms(latencies: &mut [Duration], percentile: f64) -> f64 {
    if latencies.is_empty() {
        return 0.0;
    }
    latencies.sort_unstable();
    let index = ((latencies.len() - 1) as f64 * percentile).round() as usize;
    latencies[index].as_secs_f64() * 1000.0
}

/// Minimal JSON string escaper for CLI output (no serde dependency).
pub fn json_escape(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
