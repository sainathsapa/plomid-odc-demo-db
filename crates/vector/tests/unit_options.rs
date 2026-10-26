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
// Moved from `src/options.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::options::*;

#[test]
fn defaults_validate() {
    let o = VectorSearchOptions::default();
    assert_eq!((o.ef_search, o.nprobe), (100, 8));
    assert!(o.candidate_k.is_none());
    o.validate().unwrap();
    assert_eq!(o.effective_candidate_k(20), 160);
}

#[test]
fn rejects_bad_ranges() {
    let mut o = VectorSearchOptions {
        ef_search: 0,
        ..VectorSearchOptions::default()
    };
    assert!(o.validate().is_err());
    o.ef_search = 100;
    o.candidate_k = Some(10);
    o.rerank_k = Some(20);
    assert!(o.validate().is_err());
}
