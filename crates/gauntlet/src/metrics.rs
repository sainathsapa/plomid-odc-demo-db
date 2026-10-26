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

//! Shared latency/throughput statistics for the gauntlet.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LatencyStats {
    pub n: u64,
    pub throughput_rps: f64,
    pub elapsed_secs: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub mean_ms: f64,
    pub min_ms: f64,
    pub max_ms: f64,
}

pub fn lat_stats(mut samples_ms: Vec<f64>, elapsed_secs: f64) -> LatencyStats {
    samples_ms.sort_by(|a, b| a.total_cmp(b));
    let n = samples_ms.len() as u64;
    if n == 0 {
        return LatencyStats {
            elapsed_secs,
            ..Default::default()
        };
    }
    let rank = |q: f64| {
        let idx = (((q * n as f64).ceil() as usize).max(1) - 1).min(samples_ms.len() - 1);
        samples_ms[idx]
    };
    let sum: f64 = samples_ms.iter().sum();
    LatencyStats {
        n,
        throughput_rps: n as f64 / elapsed_secs.max(f64::EPSILON),
        elapsed_secs,
        p50_ms: rank(0.50),
        p95_ms: rank(0.95),
        p99_ms: rank(0.99),
        p999_ms: rank(0.999),
        mean_ms: sum / n as f64,
        min_ms: samples_ms[0],
        max_ms: samples_ms[samples_ms.len() - 1],
    }
}
