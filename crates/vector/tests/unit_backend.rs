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
// Moved from `src/backend.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::backend::*;
use plomid_vector::VectorError;

#[test]
fn cpu_topk_and_discovery() {
    let cpu = CpuBackend::new();
    let base = vec![1.0, 0.0, 0.0, 1.0, 1.0, 1.0];
    let d = cpu
        .distance_batch(0, &[1.0, 0.0], &base, 2, DistanceKind::L2Squared)
        .unwrap();
    assert_eq!(d.len(), 3);
    let top = cpu
        .topk_exact(0, &[1.0, 0.0], &base, 2, 1, DistanceKind::L2Squared)
        .unwrap();
    assert_eq!(top[0].0, 0);
    let report = discover_capabilities(&cpu);
    assert_eq!(report.devices.len(), 1);
    let caps = BackendCapabilities::from_report(cpu.name(), &report);
    assert!(!caps.has_gpu);
    // Fallback helper: Unsupported primary degrades to CPU.
    let v = execute_with_cpu_fallback(
        || Err(VectorError::IndexUnsupported("no gpu".into())),
        |c| c.distance_batch(0, &[1.0, 0.0], &base, 2, DistanceKind::L2Squared),
    )
    .unwrap();
    assert_eq!(v.len(), 3);
}

#[test]
fn cancel_flows() {
    let cpu = CpuBackend::new();
    cpu.request_cancel();
    assert!(cpu.is_cancelled());
    assert!(cpu
        .distance_batch(0, &[1.0], &[1.0], 1, DistanceKind::L2Squared)
        .is_err());
    cpu.clear_cancel();
    assert!(!cpu.is_cancelled());
}
