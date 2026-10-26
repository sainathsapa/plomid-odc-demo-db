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
// Moved from `src/privilege.rs` inline `#[cfg(test)]` — public-API only.
use plomid_security::privilege::*;

#[test]
fn table_privilege_bits_are_stable_and_disjoint() {
    let bits = [
        TablePrivilege::Select.bit(),
        TablePrivilege::Insert.bit(),
        TablePrivilege::Update.bit(),
        TablePrivilege::Delete.bit(),
        TablePrivilege::Truncate.bit(),
    ];
    assert_eq!(
        TablePrivilege::all_bits(),
        bits.iter().fold(0, |a, b| a | b)
    );
    for (i, a) in bits.iter().enumerate() {
        for b in &bits[i + 1..] {
            assert_eq!(a & b, 0);
        }
    }
}

#[test]
fn privilege_lists_parse_and_reject() {
    let words = ["select".to_string(), "INSERT".to_string()];
    let parsed = TablePrivilege::parse_list(&words).expect("valid list");
    assert_eq!(parsed, vec![TablePrivilege::Select, TablePrivilege::Insert]);
    assert!(TablePrivilege::parse_list(&["REFERENCES".to_string()]).is_none());
    assert!(TablePrivilege::parse_list(&[]).is_none());
    let all = TablePrivilege::parse_list(&["ALL".to_string()]).expect("ALL");
    assert_eq!(all.len(), 5);
}
