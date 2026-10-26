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
//! DDL executable-envelope contract.
//!
//! Every `CREATE INDEX ... USING <method>` configuration is either accepted
//! because the runtime has a wired execution path for that column type (for
//! at least one metric), or rejected at DDL time with the executable
//! alternative named. No accepted configuration may fall back to exact
//! forever; `FLAT` is accepted everywhere by design because it *is* the
//! exact baseline.

use plomid_executor::Executor;
use plomid_sql::{QueryResult, Value};
use plomid_txn::PlomidStorageEngine;

fn setup(
    tag: &str,
) -> (
    Executor<PlomidStorageEngine>,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    let dir = std::env::temp_dir().join(format!("plomid-vec-ctr-{tag}-{}", std::process::id()));
    let wal = std::env::temp_dir().join(format!("plomid-vec-ctr-{tag}-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
    let engine = PlomidStorageEngine::open(&dir, &wal, 32).unwrap();
    (Executor::new(engine).unwrap(), dir, wal)
}

fn rows(ex: &mut Executor<PlomidStorageEngine>, sql: &str) -> Vec<Vec<Value>> {
    match ex.execute(sql).unwrap_or_else(|e| panic!("{sql} -> {e}")) {
        QueryResult::Rows { rows, .. } => rows,
        other => panic!("{sql} -> {other:?}"),
    }
}

fn text(row: &[Value], col: usize) -> String {
    match &row[col] {
        Value::Text(s) => s.clone(),
        v => v.to_sql_text(),
    }
}

fn ddl_ok(ex: &mut Executor<PlomidStorageEngine>, sql: &str) {
    ex.execute(sql)
        .unwrap_or_else(|e| panic!("{sql} must be accepted; got {e}"));
}

fn ddl_rejected(ex: &mut Executor<PlomidStorageEngine>, sql: &str) {
    let err = ex.execute(sql).unwrap_err().to_string();
    assert!(
        err.contains("VECTOR_INDEX_UNSUPPORTED"),
        "{sql} must be rejected as VECTOR_INDEX_UNSUPPORTED; got {err}"
    );
    // Every rejection names a next step, never leaves the caller guessing.
    assert!(
        err.contains(";"),
        "{sql} rejection must carry an operator hint; got {err}"
    );
}

/// One declared type per row of the contract matrix; `methods` maps every
/// method to its expected disposition (accepted / rejected).
const ALL_METHODS: [&str; 8] = [
    "FLAT",
    "HNSW",
    "IVF_FLAT",
    "IVF_SQ",
    "IVF_PQ",
    "DISKANN",
    "SPARSE_INVERTED",
    "BINARY",
];

struct TypeCase {
    tag: &'static str,
    column_type: &'static str,
    /// Methods that must be accepted (subset of ALL_METHODS).
    accepted: &'static [&'static str],
}

const CASES: [TypeCase; 7] = [
    TypeCase {
        tag: "dense",
        column_type: "VECTOR(FLOAT32, 8)",
        accepted: &["FLAT", "HNSW", "IVF_FLAT", "IVF_SQ", "IVF_PQ", "DISKANN"],
    },
    TypeCase {
        tag: "dense_i4",
        column_type: "VECTOR(INT4, 8)",
        // INT4 only promotes for SQ; PQ rejects it; IVFFlat/HNSW/DiskANN run
        // on the promoted f32 lanes.
        accepted: &["FLAT", "HNSW", "IVF_FLAT", "IVF_SQ", "DISKANN"],
    },
    TypeCase {
        tag: "dense_i8",
        column_type: "VECTOR(INT8, 8)",
        accepted: &["FLAT", "HNSW", "IVF_FLAT", "IVF_SQ", "IVF_PQ", "DISKANN"],
    },
    TypeCase {
        // The runtime always builds PQ with the default 8 segments: a
        // dimension they do not divide can never train, so DDL must reject
        // it instead of recording an index that would only fall back.
        tag: "dense_dim4",
        column_type: "VECTOR(FLOAT32, 4)",
        accepted: &["FLAT", "HNSW", "IVF_FLAT", "IVF_SQ", "DISKANN"],
    },
    TypeCase {
        tag: "binary",
        column_type: "VECTOR(BINARY, 8)",
        accepted: &["FLAT", "BINARY"],
    },
    TypeCase {
        tag: "sparse",
        column_type: "SPARSE_VECTOR(FLOAT32, 8)",
        accepted: &["FLAT", "SPARSE_INVERTED"],
    },
    TypeCase {
        tag: "multi",
        column_type: "MULTI_VECTOR(FLOAT32, 4, 2)",
        accepted: &["FLAT"],
    },
];

#[test]
fn ddl_method_type_envelope_matrix() {
    let (mut ex, dir, wal) = setup("matrix");
    for case in &CASES {
        let table = format!("t_{}", case.tag);
        ddl_ok(
            &mut ex,
            &format!(
                "CREATE TABLE {table} (id BIGINT PRIMARY KEY, e {})",
                case.column_type
            ),
        );
        for method in ALL_METHODS {
            let sql = format!(
                "CREATE INDEX {table}_{} ON {table} USING {method} (e)",
                method.to_ascii_lowercase()
            );
            if case.accepted.contains(&method) {
                ddl_ok(&mut ex, &sql);
            } else if case.tag == "dense_dim4" && method == "IVF_PQ" {
                // The one dimension-level rejection: the error must name the
                // divisor rule, not just say "unsupported".
                let err = ex.execute(&sql).unwrap_err().to_string();
                assert!(
                    err.contains("VECTOR_INDEX_UNSUPPORTED") && err.contains("multiple of 8"),
                    "{sql} must be rejected naming the multiple-of-8 rule; got {err}"
                );
            } else {
                ddl_rejected(&mut ex, &sql);
            }
        }
        // Every accepted index reports READY in the operator view: a live
        // decodable column AND a method that can execute on its type.
        // The operator view reports schema-qualified table names.
        let listed = rows(
            &mut ex,
            &format!(
                "SELECT index_name, state FROM plomid_vector_indexes() WHERE table_name = 'public.{table}'"
            ),
        );
        for row in &listed {
            assert_eq!(
                text(row, 1),
                "READY",
                "accepted index must be READY: {row:?}"
            );
        }
        let expected = case.accepted.len() + usize::from(case.tag.starts_with("dense"));
        assert_eq!(listed.len(), expected, "{table}: {listed:?}");
    }

    // Auto-index contract: every dense column receives its deterministic
    // default; kinds with no HNSW execution path receive none.
    let auto = rows(
        &mut ex,
        "SELECT tablename, indexname FROM pg_indexes WHERE indexname LIKE '%vector_idx' ORDER BY tablename",
    );
    let auto_tables: Vec<String> = auto.iter().map(|row| text(row, 0)).collect();
    assert_eq!(
        auto_tables,
        vec!["t_dense", "t_dense_dim4", "t_dense_i4", "t_dense_i8"],
        "one HNSW default per dense column, none elsewhere: {auto:?}"
    );
    for row in &auto {
        assert_eq!(text(row, 1), format!("{}_e_vector_idx", text(row, 0)));
    }
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&wal);
}
