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

//! Freshness family: commit -> analytics-visible latency on the live path.
//!
//! Each sample inserts a row (a live autocommit transaction: WAL, apply,
//! `publish_commit`, install) and then immediately reads it back through the
//! live MVCC snapshot path. A committed value that is not visible on the
//! immediate read is a freshness violation and panics. The reported
//! percentiles are the post-commit read latencies (the commit -> visible
//! gap as observable through the row-store snapshot path); an aggregate
//! `COUNT(*)` visibility check runs every 10 samples to prove the committed
//! prefix is visible to analytics-style scans as well.

use crate::harness::open_live_db;
use crate::metrics::lat_stats;
use std::time::Instant;

/// Commit -> analytics-visible latency distribution.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FreshnessResult {
    /// p50 of the read-after-commit latencies, in ms.
    pub p50_ms: f64,
    /// p95 of the read-after-commit latencies, in ms.
    pub p95_ms: f64,
    /// p99 of the read-after-commit latencies, in ms.
    pub p99_ms: f64,
    /// Number of read-after-commit samples collected.
    pub samples: u64,
}

/// Measures commit -> visible latency for `samples` single-row commits.
///
/// Every INSERT and every verification read runs against the live executor,
/// so visibility flows through the real MVCC snapshot path (own-writes,
/// `publish_commit`, install). Panics on any freshness violation.
pub fn run_freshness(samples: usize) -> FreshnessResult {
    let mut db = open_live_db("fresh");
    db.exec("CREATE TABLE ga_fresh(id BIGINT PRIMARY KEY, v BIGINT)");

    let mut latencies = Vec::with_capacity(samples);
    let start = Instant::now();
    for i in 0..samples {
        let id = i as i64;
        let v = row_value(id);
        db.exec(&format!("INSERT INTO ga_fresh(id, v) VALUES ({id}, {v})"));

        // Post-commit read on the live path; timed manually (instead of
        // `timed_exec`) so the returned value can be asserted visible.
        let t = Instant::now();
        let result = db
            .executor
            .execute(&format!("SELECT v FROM ga_fresh WHERE id = {id}"))
            .unwrap_or_else(|e| panic!("freshness read id={id} failed: {e}"));
        let ms = t.elapsed().as_secs_f64() * 1000.0;
        let seen = scalar_i64(&result).unwrap_or_else(|| {
            panic!("freshness violation: committed row id={id} not visible to read-after-commit")
        });
        assert_eq!(
            seen, v,
            "freshness violation: committed row id={id} has v={seen}, expected {v}"
        );
        latencies.push(ms);

        // Aggregate-visibility check every 10 samples: the full committed
        // prefix must be visible to an analytics-style scan.
        if (i + 1) % 10 == 0 {
            let count = table_count(&mut db);
            assert_eq!(
                count,
                (i + 1) as u64,
                "freshness violation: COUNT(*) {count} != committed prefix {}",
                i + 1
            );
        }
    }
    let elapsed_secs = start.elapsed().as_secs_f64();

    let stats = lat_stats(latencies, elapsed_secs);
    let result = FreshnessResult {
        p50_ms: stats.p50_ms,
        p95_ms: stats.p95_ms,
        p99_ms: stats.p99_ms,
        samples: stats.n,
    };
    db.teardown();
    result
}

/// Deterministic row payload for a given id.
fn row_value(id: i64) -> i64 {
    id.wrapping_mul(11).wrapping_add(7)
}

/// Live `SELECT COUNT(*)` scalar over `ga_fresh`.
fn table_count(db: &mut crate::harness::LiveDb) -> u64 {
    let result = db
        .executor
        .execute("SELECT COUNT(*) FROM ga_fresh")
        .expect("freshness COUNT(*) must execute");
    scalar_i64(&result).expect("COUNT(*) must be an integer scalar") as u64
}

/// First scalar of a one-row `Rows` result, or `None` when absent/non-integer.
fn scalar_i64(result: &plomid_sql::QueryResult) -> Option<i64> {
    match result {
        plomid_sql::QueryResult::Rows { rows, .. } => {
            let value = rows.first()?.first()?;
            value_to_i64(value)
        }
        _ => None,
    }
}

/// Lenient integer coercion across runtime value encodings.
fn value_to_i64(value: &plomid_sql::Value) -> Option<i64> {
    use plomid_sql::Value as V;
    match value {
        V::Int2(x) => Some(*x as i64),
        V::Int4(x) => Some(*x as i64),
        V::Int8(x) => Some(*x),
        V::Float4(x) => Some(*x as i64),
        V::Float8(x) => Some(*x as i64),
        V::Numeric(n) => n.clone().to_i64(),
        V::Money(x) => Some(*x),
        V::Bool(b) => Some(i64::from(*b)),
        V::Text(s)
        | V::VarChar(s)
        | V::BpChar(s)
        | V::Name(s)
        | V::Xml(s)
        | V::Cstring(s)
        | V::Unknown(s) => s.trim().parse::<i64>().ok(),
        V::Oid(x) | V::Xid(x) | V::Cid(x) => Some(*x as i64),
        V::Date(x) => Some(*x as i64),
        V::Time(x) | V::Timestamp(x) | V::Timestamptz(x) => Some(*x),
        _ => None,
    }
}
