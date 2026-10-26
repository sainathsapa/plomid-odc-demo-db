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
//! F-006 scale guard: opening a session on a large sharded catalog.
//!
//! The wire server creates one executor per connection, so session-open cost
//! is connection-establishment cost. The audit saw ~5 s/connect on a 7.5 MB
//! catalog before catalog sharding. This test builds the same image size
//! (60,000 three-column tables) and requires session open to stay well below
//! the audit failure mode. It is scale-tier only because building the catalog
//! costs a few seconds; see `make test-scale`.

use plomid_executor::Executor;
use plomid_sql::ast::{Constraint, ConstraintKind};
use plomid_sql::{save_catalog, Catalog, ColumnDef, ColumnType, InMemoryCatalog};
use plomid_txn::PlomidStorageEngine;
use std::time::{Duration, Instant};

#[test]
#[ignore = "scale tier: builds a 7.5 MB / 60k-table catalog; run via make test-scale"]
fn session_open_on_a_large_sharded_catalog_stays_fast() {
    let base = std::env::temp_dir().join(format!("plomid-f006-open-{}", std::process::id()));
    let storage = base.join("data");
    let wal = base.join("wal");
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&storage).expect("scratch dir");

    let result = (|| {
        let mut engine = PlomidStorageEngine::create(&storage, &wal, 64)?;
        let mut catalog = InMemoryCatalog::new();
        const TABLES: usize = 60_000;
        for i in 0..TABLES {
            let name = format!("public.t{i:05}");
            catalog.create_table(
                name.clone(),
                vec![
                    ColumnDef {
                        name: "id".into(),
                        col_type: ColumnType::bigint(),
                        constraints: Vec::new(),
                    },
                    ColumnDef {
                        name: "s".into(),
                        col_type: ColumnType::text(),
                        constraints: Vec::new(),
                    },
                    ColumnDef {
                        name: "amount".into(),
                        col_type: ColumnType::bigint(),
                        constraints: Vec::new(),
                    },
                ],
                vec![Constraint {
                    name: None,
                    kind: ConstraintKind::PrimaryKey,
                    columns: vec!["id".into()],
                    expr: None,
                }],
            )?;
            catalog.set_table_owner(&name, "plomid");
        }
        save_catalog(&mut catalog, &mut engine)?;
        drop(engine);

        // Reopen exactly what the wire server does per connection: a fresh
        // engine handle plus `Executor::new` (catalog + statistics load).
        for round in 0..3 {
            let engine = PlomidStorageEngine::open(&storage, &wal, 64)?;
            let started = Instant::now();
            let executor = Executor::new(engine)?;
            let elapsed = started.elapsed();
            println!("round {round}: Executor::new={elapsed:?}");
            assert!(
                elapsed < Duration::from_secs(2),
                "session open took {elapsed:?} on a ~7.5 MB catalog; \
                 the F-006 audit saw ~5 s and the target is well below 100 ms \
                 in release"
            );
            drop(executor);
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    })();

    let _ = std::fs::remove_dir_all(&base);
    result.expect("large-catalog session open");
}
