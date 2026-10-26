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
// Moved from `src/layout/names.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::layout::names::{parse_layout_id, render_layout_id, LAYOUT_ID_DIGITS};

#[test]
fn rendering_is_fixed_width_and_round_trips() {
    let rendered = render_layout_id(1);
    assert_eq!(rendered.len(), LAYOUT_ID_DIGITS);
    assert_eq!(rendered, "00000000000000000001");
    assert_eq!(parse_layout_id("T-00000000000000000001", "T-"), Some(1));
    assert_eq!(
        parse_layout_id(&format!("GEN-{}", render_layout_id(u64::MAX)), "GEN-"),
        Some(u64::MAX)
    );
}

#[test]
fn unrelated_names_are_rejected() {
    assert_eq!(parse_layout_id("T-1", "T-"), None);
    assert_eq!(parse_layout_id("T-00000000000000000000", "T-"), None);
    assert_eq!(parse_layout_id("I-00000000000000000001", "T-"), None);
    assert_eq!(parse_layout_id("T-0000000000000000000a", "T-"), None);
    assert_eq!(parse_layout_id("META.dat", "T-"), None);
}
