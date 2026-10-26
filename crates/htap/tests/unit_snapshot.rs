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
// Moved from `src/snapshot.rs` inline `#[cfg(test)]` — public-API only.
use plomid_htap::config::HtapConfig;
use plomid_htap::snapshot::*;
use plomid_htap::{ResourceGovernor, WorkloadClass};

#[test]
fn snapshot_describes_state() {
    let gov = ResourceGovernor::new(HtapConfig::default()).unwrap();
    gov.admit(WorkloadClass::Olap, 1024, 1);
    gov.admit(WorkloadClass::Olap, 512, 1);
    let snap = HtapSnapshot::collect(&gov);
    assert_eq!(snap.olap_used_bytes, 1536);
    assert_eq!(snap.admitted_olap, gov.olap_in_use_slots());
    assert_eq!(snap.admitted_olap, 2);
    assert_eq!(snap.waiting_count, 0);
    assert!(snap.waiting.is_empty());
    assert!(snap.per_query.is_empty());
    let text = snap.describe();
    assert!(text.contains("pressure="));
    assert!(text.contains("top_consumer="));
    assert!(text.contains("oltp_headroom="));
}
