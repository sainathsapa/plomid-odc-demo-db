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
// Moved from `src/main.rs` inline `#[cfg(test)]` (binary-only crate: helpers
// now live in `src/lib.rs` so tests can link them).
use plomid_cli::{json_escape, percentile_ms, ConnectionOptions};
use std::path::PathBuf;
use std::time::Duration;

#[test]
fn percentile_is_order_independent() {
    let mut values = [
        Duration::from_millis(30),
        Duration::from_millis(10),
        Duration::from_millis(20),
    ];
    assert_eq!(percentile_ms(&mut values, 0.5), 20.0);
}

#[test]
fn json_escape_does_not_emit_raw_quotes() {
    assert_eq!(json_escape("a\"b"), "a\\\"b");
}

#[test]
fn connection_options_debug_redacts_password() {
    let opts = ConnectionOptions {
        host: "127.0.0.1".to_string(),
        port: 5432,
        user: "plomid".to_string(),
        password: "s3cr3t-pw".to_string(),
        database: "plomid".to_string(),
        data_dir: PathBuf::from("./data"),
    };
    let rendered = format!("{opts:?}");
    assert!(!rendered.contains("s3cr3t-pw"), "got: {rendered}");
    assert!(rendered.contains("<redacted>"), "got: {rendered}");
}
