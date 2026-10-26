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
//! F-010 regression: EXPLAIN's vector strategy must follow the same live
//! `vector.auto_ann_min_rows` / `vector.max_ann_rows` gates as execution,
//! not a second constant planner floor.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn setup(tag: &str) -> (Executor<PlomidStorageEngine>, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!("plomid-vec-explain-{tag}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!(
        "plomid-vec-explain-{tag}-wal-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
    let engine = PlomidStorageEngine::open(&dir, &wal, 32).unwrap();
    (Executor::new(engine).unwrap(), dir)
}

fn rows(ex: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    match ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}")) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("{sql} -> {other:?}"),
    }
}

fn plan_text(ex: &mut Executor<PlomidStorageEngine>) -> String {
    rows(
        ex,
        "EXPLAIN SELECT id FROM docs ORDER BY e <-> [1,0,0,0] LIMIT 2",
    )
    .into_iter()
    .map(|row| match &row[0] {
        Value::Text(line) => line.clone(),
        value => value.to_sql_text(),
    })
    .collect::<Vec<_>>()
    .join("\n")
}

#[test]
fn explain_vector_strategy_follows_live_execution_gates() {
    let (mut ex, dir) = setup("gate");
    ex.execute("CREATE TABLE docs (id INTEGER, e VECTOR(FLOAT32, 4))")
        .unwrap();
    ex.execute(
        "INSERT INTO docs VALUES \
         (1, [1,0,0,0]), (2, [0.9,0.1,0,0]), (3, [0.8,0.2,0,0]), \
         (4, [0,0,1,0]), (5, [0,0,0.9,0.1]), (6, [0,0,0.8,0.2])",
    )
    .unwrap();
    ex.execute("CREATE INDEX docs_e_hnsw ON docs USING HNSW (e)")
        .unwrap();

    // Below the default 4096-row floor: exact.
    let plan = plan_text(&mut ex);
    assert!(
        plan.contains("Vector Exact scan"),
        "expected exact plan at the default floor:\n{plan}"
    );

    // Lowering the live gate below the constant floor must switch the plan
    // to ANN, matching execution instead of only changing execution.
    ex.execute("SET vector.auto_ann_min_rows = 0").unwrap();
    let plan = plan_text(&mut ex);
    assert!(
        plan.contains("Vector ANN scan"),
        "expected ANN plan after lowering auto_ann_min_rows:\n{plan}"
    );

    // The resource ceiling is also part of execution eligibility.
    ex.execute("SET vector.max_ann_rows = 1").unwrap();
    let plan = plan_text(&mut ex);
    assert!(
        plan.contains("Vector Exact scan"),
        "expected exact plan above max_ann_rows:\n{plan}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}
