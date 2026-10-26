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
// Moved from `src/workload.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::workload::*;

#[test]
fn writes_are_always_oltp() {
    assert_eq!(classify(None, true, None), WorkloadClass::Oltp);
    assert_eq!(
        classify(Some(WorkloadClass::Batch), true, Some(1_000_000)),
        WorkloadClass::Oltp
    );
}

#[test]
fn small_reads_are_oltp_without_hint_required() {
    assert_eq!(classify(None, false, Some(10)), WorkloadClass::Oltp);
    assert_eq!(
        classify(None, false, Some(SMALL_TXN_ROW_THRESHOLD)),
        WorkloadClass::Oltp
    );
}

#[test]
fn large_or_unknown_reads_default_to_olap() {
    assert_eq!(classify(None, false, None), WorkloadClass::Olap);
    assert_eq!(classify(None, false, Some(10_000_000)), WorkloadClass::Olap);
}

#[test]
fn hints_win_for_large_reads_and_ops_intent_is_honored() {
    assert_eq!(
        classify(Some(WorkloadClass::Batch), false, None),
        WorkloadClass::Batch
    );
    assert_eq!(
        classify(Some(WorkloadClass::Maintenance), true, None),
        WorkloadClass::Maintenance
    );
    assert_eq!(
        classify(Some(WorkloadClass::System), false, None),
        WorkloadClass::System
    );
}

#[test]
fn priorities_are_sane() {
    assert_eq!(WorkloadClass::Oltp.priority(), Priority::LatencySensitive);
    assert_eq!(WorkloadClass::Olap.priority(), Priority::Throughput);
    assert_eq!(WorkloadClass::Maintenance.priority(), Priority::Background);
    assert!(WorkloadClass::InteractiveAnalytics.is_latency_sensitive());
    assert!(!WorkloadClass::Batch.is_latency_sensitive());
}
