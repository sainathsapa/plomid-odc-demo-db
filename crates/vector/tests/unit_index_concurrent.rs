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
// Moved from `src/index/concurrent.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::index::concurrent::*;
use plomid_vector::index::{FlatIndex, IndexConfig, IndexKind, Metric};
use plomid_vector::index::{IndexStats, VectorIndex};

fn flat(dim: usize) -> ConcurrentIndex<FlatIndex> {
    ConcurrentIndex::new(
        FlatIndex::new(IndexConfig::new(IndexKind::Flat, Metric::L2, dim)).unwrap(),
    )
}

#[test]
fn concurrent_reads_agree() {
    let mut idx = flat(2);
    idx.build(&[(1, vec![1.0, 0.0]), (2, vec![0.0, 1.0])])
        .unwrap();
    let idx = std::sync::Arc::new(idx);
    let handles: Vec<_> = (0..8)
        .map(|_| {
            let idx = idx.clone();
            std::thread::spawn(move || idx.search(&[1.0, 0.0], 1).unwrap())
        })
        .collect();
    for h in handles {
        let hits = h.join().expect("reader thread panicked");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].row_id, 1);
    }
    idx.validate().unwrap();
}

#[test]
fn stats_survives_poison_as_default() {
    let idx = flat(2);
    assert_eq!(idx.stats(), IndexStats::default());
}
