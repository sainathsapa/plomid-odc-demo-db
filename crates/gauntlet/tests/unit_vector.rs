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
// Moved from `src/vector.rs` inline `#[cfg(test)]` — public-API only.
use plomid_gauntlet::vector::*;

#[test]
#[ignore = "scale: live vector benchmark suites take minutes; runs in CI via make test-scale"]
fn vector_qualification_is_green() {
    let q = run_vector_qualification();
    for s in &q.suites {
        assert!(s.passed, "suite {} failed: {}", s.name, s.detail);
    }
    assert!(q.passed());
    assert_eq!(q.passed_count(), q.suites.len());
}
