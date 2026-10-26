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
// Moved from src/join/bloom.rs inline #[cfg(test)].
use plomid_executor::join::bloom::*;

#[test]
fn no_false_negatives() {
    let mut filter = BloomFilter::new(128, 0.01);
    let hashes: Vec<u64> = (0..128)
        .map(|i| hash_key(format!("key{i}").as_bytes()))
        .collect();
    for hash in &hashes {
        filter.insert(*hash);
    }
    for hash in &hashes {
        assert!(filter.may_contain(*hash), "false negative for {hash}");
    }
}

#[test]
fn false_positive_rate_is_sane() {
    let mut filter = BloomFilter::new(1000, 0.01);
    for i in 0..1000 {
        filter.insert(hash_key(format!("present-{i}").as_bytes()));
    }
    let mut false_positives = 0;
    for i in 0..1000 {
        if filter.may_contain(hash_key(format!("absent-{i}").as_bytes())) {
            false_positives += 1;
        }
    }
    // 1% target; allow generous headroom so the test is deterministic.
    assert!(
        false_positives < 100,
        "too many false positives: {false_positives}/1000"
    );
}

#[test]
fn empty_sized_filter_still_works() {
    let mut filter = BloomFilter::new(0, 0.01);
    let h = hash_key(b"hello");
    assert!(!filter.may_contain(h));
    filter.insert(h);
    assert!(filter.may_contain(h));
}
