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
// Moved from src/join/hash.rs inline #[cfg(test)].
use plomid_executor::join::bloom::hash_key;
use plomid_executor::join::hash::*;
use plomid_sql::Value;

fn row(id: i64, tag: &str) -> JoinRow {
    vec![Value::Int8(id), Value::Text(tag.to_string())]
}

#[test]
fn build_and_probe_match() {
    let build = vec![row(1, "a"), row(2, "b"), row(1, "c")];
    let table = HashJoinTable::from_rows(0, &build);
    assert_eq!(table.row_count(), 3);
    assert_eq!(table.len(), 2);
    assert!(!table.is_empty());
    // Index API: probes return build-side indices, resolved by the caller.
    let matches = table.probe(&Value::Int8(1));
    assert_eq!(matches, &[0, 2]);
    assert_eq!(build[matches[0]][1], Value::Text("a".to_string()));
    assert_eq!(build[matches[1]][1], Value::Text("c".to_string()));
    // probe_row reads the key out of a probe row.
    let probe_row = vec![Value::Int8(2), Value::Int8(99)];
    assert_eq!(table.probe_row(&probe_row, 0), &[1]);
    assert!(table.probe(&Value::Int8(99)).is_empty());
    assert!(table.probe_row(&[], 0).is_empty());
    // NULL never joins.
    assert!(table.probe(&Value::Null).is_empty());
    // Integer spellings share the canonical key.
    assert_eq!(table.probe(&Value::Int4(1)), &[0, 2]);
    // An empty build holds no keys.
    let empty = HashJoinTable::from_rows(0, &[]);
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
    assert_eq!(empty.row_count(), 0);
}

#[test]
fn parallel_build_equals_sequential() {
    let build: Vec<JoinRow> = (0..500).map(|i| row(i % 23, "w")).collect();
    let seq = HashJoinTable::from_rows(0, &build);
    let par = HashJoinTable::parallel_build(&build, 0, 8);
    assert_eq!(par.row_count(), seq.row_count());
    assert_eq!(par.len(), seq.len());
    assert_eq!(par.map, seq.map);
    // Single-threaded parallel build matches too.
    let one = HashJoinTable::parallel_build(&build, 0, 1);
    assert_eq!(one.map, seq.map);
}

#[test]
fn build_with_bloom_has_no_false_negatives() {
    let build: Vec<JoinRow> = (0..100).map(|i| row(i, "b")).collect();
    let (table, filter) = HashJoinTable::build_with_bloom(&build, 0);
    assert_eq!(table.row_count(), 100);
    assert!(!table.is_empty());
    for row in &build {
        let key = join_key_bytes(&row[0]).expect("non-null");
        assert!(filter.may_contain(hash_key(&key)));
        // Every build row is reachable through the index probe.
        assert_eq!(table.probe(&row[0]).len(), 1);
    }
    // An absent key may or may not hit the bloom, but the table is exact.
    assert!(table.probe(&Value::Int8(10_000)).is_empty());
}
