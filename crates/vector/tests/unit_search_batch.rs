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
// Moved from `src/search/batch.rs` inline `#[cfg(test)]` — public-API only.
use plomid_vector::search::batch::*;
use plomid_vector::search::SearchMetric;

fn rows() -> Vec<(u64, Option<Vec<f32>>)> {
    vec![
        (1, Some(vec![1.0, 0.0])),
        (2, Some(vec![0.0, 1.0])),
        (3, None),
        (4, Some(vec![0.5, 0.5])),
    ]
}

#[test]
fn scratch_reuse_and_counts() {
    let rows = rows();
    let mut buf = Vec::new();
    let n = compute_distances_into(
        &[1.0, 0.0],
        rows.iter().map(|(id, v)| (*id, v.as_deref())),
        SearchMetric::L2,
        &mut buf,
        None,
    )
    .unwrap();
    assert_eq!(n, 3); // NULL row skipped
    let cap = buf.capacity();
    assert!(cap > 0);
    let n2 = compute_distances_into(
        &[0.0, 1.0],
        rows.iter().map(|(id, v)| (*id, v.as_deref())),
        SearchMetric::L2,
        &mut buf,
        None,
    )
    .unwrap();
    assert_eq!(n2, 3);
    assert_eq!(buf.capacity(), cap); // no reallocation
}

#[test]
fn searcher_matches_exact_and_reuses() {
    let rows = rows();
    let mut s = BatchSearcher::new(SearchMetric::L2, 2).unwrap();
    let q: &[f32] = &[1.0, 0.0];
    let out = s.search_one(q, &rows, None).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].row_id, 1);
    // Second query reuses allocations and agrees with exact_top_k.
    let q2: &[f32] = &[0.0, 1.0];
    let out2 = s.search_one(q2, &rows, None).unwrap();
    assert_eq!(out2[0].row_id, 2);
    let many = s.search_many(&[q, q2], &rows, None).unwrap();
    assert_eq!(many.len(), 2);
    assert_eq!(many[0], out);
    // Scratch view borrows the reusable buffer.
    let d = s.distances_scratch(q, &rows, None).unwrap();
    assert_eq!(d.len(), 3);
}

#[test]
fn invalid_k_rejected_and_cancel_aborts() {
    assert!(BatchSearcher::new(SearchMetric::L2, plomid_vector::search::MAX_TOP_K + 1).is_err());
    let rows = rows();
    let mut s = BatchSearcher::new(SearchMetric::L2, 2).unwrap();
    let stop = || true;
    assert_eq!(
        s.search_one(&[1.0, 0.0], &rows, Some(&stop)),
        Err(plomid_vector::search::SearchError::Cancelled)
    );
}

#[test]
fn parallel_matches_serial() {
    let rows = rows();
    let queries = vec![vec![1.0f32, 0.0], vec![0.0, 1.0], vec![0.5, 0.5]];
    let mut serial = BatchSearcher::new(SearchMetric::L2, 2).unwrap();
    let borrowed: Vec<&[f32]> = queries.iter().map(Vec::as_slice).collect();
    let expected = serial.search_many(&borrowed, &rows, None).unwrap();
    for max_threads in [0usize, 1, 2, 64] {
        let got =
            search_many_parallel(&queries, &rows, SearchMetric::L2, 2, None, max_threads).unwrap();
        assert_eq!(got, expected, "threads={max_threads}");
    }
}

#[test]
fn numa_modes_agree_bit_identically() {
    use plomid_vector::metrics::VectorMetrics;
    let rows = rows();
    let queries = vec![
        vec![1.0f32, 0.0],
        vec![0.0, 1.0],
        vec![0.5, 0.5],
        vec![1.0, 1.0],
    ];
    let mut serial = BatchSearcher::new(SearchMetric::L2, 2).unwrap();
    let borrowed: Vec<&[f32]> = queries.iter().map(Vec::as_slice).collect();
    let expected = serial.search_many(&borrowed, &rows, None).unwrap();
    let metrics = VectorMetrics::new();
    // UMA, dual-socket seam, and oversized topology must all agree.
    for topo in [
        plomid_vector::numa::NumaTopology::test_seam(1, 4).unwrap(),
        plomid_vector::numa::NumaTopology::test_seam(2, 16).unwrap(),
        plomid_vector::numa::NumaTopology::test_seam(4, 64).unwrap(),
    ] {
        for max_threads in [0usize, 1, 2, 64] {
            let got = search_many_parallel_numa(
                &queries,
                &rows,
                SearchMetric::L2,
                2,
                None,
                max_threads,
                &topo,
                Some(&metrics),
            )
            .unwrap();
            assert_eq!(got, expected, "topo={topo:?} threads={max_threads}");
        }
    }
    let snap = metrics.snapshot();
    assert!(snap.numa_single_node + snap.numa_multi_node > 0);
}
