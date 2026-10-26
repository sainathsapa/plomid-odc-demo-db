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

//! HTAP mix family: OLTP-under-OLAP interference on the live executor path.
//!
//! Every statement runs through the single live choke point
//! (`Executor::execute` -> `execute_all`, with HTAP admission via
//! `governance::admit_query` and result-buffer charging), so the OLTP
//! latency samples honestly share the executor, governor slots, and
//! columnar-freshness gates with the interleaved OLAP work.
//!
//! Interleaving is deterministic round-robin over a 10-slot window (no RNG):
//! for iteration `i`, slot `r = i % 10` dispatches to OLTP when
//! `r < oltp_share * 10`, to a point read when
//! `r < (oltp_share + point_share) * 10`, and to an analytics aggregate
//! otherwise. OLTP slots alternate PK `SELECT` / single-row `INSERT`;
//! analytics slots alternate `COUNT(*)` / `SUM(v)`.
//!
//! NOTE (future work): this is single-threaded interleaving, so it measures
//! interference through shared live state (governor, freshness generations,
//! buffer charges), not true SMP parallelism. A multi-threaded variant that
//! drives OLTP and OLAP concurrently from several threads is the honest
//! next step for throughput-side HTAP claims.

use crate::harness::{open_live_db, LiveDb};
use crate::metrics::lat_stats;
use std::time::Instant;

/// Per-mix outcome: OLTP latency percentiles under the mix plus the OLAP side.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HtapMixResult {
    /// Mix label (e.g. `"70/20/10"`).
    pub name: String,
    /// p50 of the OLTP (PK SELECT / single-row INSERT) samples, in ms.
    pub oltp_p50_ms: f64,
    /// p95 of the OLTP samples, in ms.
    pub oltp_p95_ms: f64,
    /// p99 of the OLTP samples, in ms.
    pub oltp_p99_ms: f64,
    /// OLAP statements (point reads + analytics) per second over the mix loop.
    pub olap_throughput_rps: f64,
    /// Mean latency of the OLAP samples (point reads + analytics), in ms.
    pub olap_latency_ms: f64,
}

/// Whole HTAP run: pure-OLTP baseline plus one entry per mix.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HtapResult {
    /// Pure-OLTP p99 with no OLAP interleaved, in ms (the penalty denominator).
    pub oltp_alone_p99_ms: f64,
    /// Per-mix results, in mix-table order.
    pub mixes: Vec<HtapMixResult>,
    /// `(mix_name, penalty)` with `penalty = mix.oltp_p99 / oltp_alone_p99`.
    pub penalties: Vec<(String, f64)>,
    /// Headline interference penalty: the worst (max) finite per-mix
    /// penalty, or NaN when none is usable. This is the gauntlet's most
    /// important metric: `OLTP p99 under HTAP / OLTP p99 alone`.
    pub htap_penalty: f64,
}

/// OLTP slowdown under an HTAP mix relative to the pure-OLTP baseline.
///
/// Returns `1.0` (no measurable penalty) when the baseline is not positive,
/// which only happens with degenerate inputs (e.g. zero timed samples).
pub fn htap_penalty(oltp_p99_under_htap: f64, oltp_p99_alone: f64) -> f64 {
    if oltp_p99_alone <= 0.0 {
        return 1.0;
    }
    oltp_p99_under_htap / oltp_p99_alone
}

/// Mix table: `(name, oltp_share, point_read_share, analytics_share)`.
const MIXES: [(&str, f64, f64, f64); 3] = [
    ("70/20/10", 0.7, 0.2, 0.1),
    ("50/50", 0.5, 0.5, 0.0),
    ("90/10", 0.9, 0.0, 0.1),
];

/// Runs the HTAP mixes on fresh live databases.
///
/// `seed_rows` controls the seeded `ga_htap` table size and `iters_per_mix`
/// the interleaved statements per mix. Both default small for CI speed;
/// every statement executes against the live executor.
pub fn run_htap(seed_rows: usize, iters_per_mix: usize) -> HtapResult {
    let oltp_alone_p99_ms = measure_oltp_alone(seed_rows, iters_per_mix);

    let mut mixes = Vec::with_capacity(MIXES.len());
    let mut penalties = Vec::with_capacity(MIXES.len());
    for (name, oltp_share, point_share, _analytics_share) in MIXES {
        let mix = run_mix(name, seed_rows, iters_per_mix, oltp_share, point_share);
        penalties.push((
            mix.name.clone(),
            htap_penalty(mix.oltp_p99_ms, oltp_alone_p99_ms),
        ));
        mixes.push(mix);
    }
    HtapResult {
        oltp_alone_p99_ms,
        htap_penalty: penalties
            .iter()
            .map(|(_, p)| *p)
            .filter(|p| p.is_finite() && *p > 0.0)
            .fold(f64::NAN, f64::max),
        mixes,
        penalties,
    }
}

/// Pure-OLTP baseline: alternating PK SELECT / single-row INSERT, no OLAP.
fn measure_oltp_alone(seed_rows: usize, iters: usize) -> f64 {
    let mut db = open_live_db("htap-alone");
    seed_htap_table(&mut db, seed_rows);
    let mut next_id = seed_rows as i64;
    let mut samples = Vec::with_capacity(iters);
    for i in 0..iters {
        if i % 2 == 0 {
            let id = if next_id > 0 { (i as i64) % next_id } else { 0 };
            samples.push(db.timed_exec(&format!("SELECT v FROM ga_htap WHERE id = {id}")));
        } else {
            let id = next_id;
            next_id += 1;
            samples.push(db.timed_exec(&format!(
                "INSERT INTO ga_htap(id, v) VALUES ({id}, {})",
                row_value(id)
            )));
        }
    }
    let elapsed_secs = samples.iter().sum::<f64>() / 1000.0;
    let p99 = lat_stats(samples, elapsed_secs).p99_ms;
    db.teardown();
    p99
}

/// One deterministic HTAP mix on a fresh live database.
fn run_mix(
    name: &str,
    seed_rows: usize,
    iters: usize,
    oltp_share: f64,
    point_share: f64,
) -> HtapMixResult {
    let tag = format!("htap-mix-{}", name.replace('/', "-"));
    let mut db = open_live_db(&tag);
    seed_htap_table(&mut db, seed_rows);

    let oltp_slots = (oltp_share * 10.0).round() as usize;
    let point_slots = (point_share * 10.0).round() as usize;
    let point_end = oltp_slots + point_slots;

    let mut next_id = seed_rows as i64;
    let mut inserts: u64 = 0;
    let mut oltp_toggle: usize = 0;
    let mut analytics_toggle: usize = 0;
    let mut oltp_samples = Vec::new();
    let mut olap_samples = Vec::new();

    let start = Instant::now();
    for i in 0..iters {
        let r = i % 10;
        if r < oltp_slots {
            // OLTP bucket: alternate PK SELECT / single-row INSERT.
            if oltp_toggle.is_multiple_of(2) {
                let id = if next_id > 0 { (i as i64) % next_id } else { 0 };
                oltp_samples.push(db.timed_exec(&format!("SELECT v FROM ga_htap WHERE id = {id}")));
            } else {
                let id = next_id;
                next_id += 1;
                inserts += 1;
                oltp_samples.push(db.timed_exec(&format!(
                    "INSERT INTO ga_htap(id, v) VALUES ({id}, {})",
                    row_value(id)
                )));
            }
            oltp_toggle += 1;
        } else if r < point_end {
            // Point reads count toward the OLAP bucket, so every mix in the
            // table (including "50/50", which has no analytics share) still
            // reports a meaningful OLAP throughput.
            let id = if next_id > 0 { (i as i64) % next_id } else { 0 };
            olap_samples.push(db.timed_exec(&format!("SELECT v FROM ga_htap WHERE id = {id}")));
        } else if analytics_toggle.is_multiple_of(2) {
            olap_samples.push(db.timed_exec("SELECT COUNT(*) FROM ga_htap"));
            analytics_toggle += 1;
        } else {
            olap_samples.push(db.timed_exec("SELECT SUM(v) FROM ga_htap"));
            analytics_toggle += 1;
        }
    }
    let elapsed_secs = start.elapsed().as_secs_f64();

    let oltp_stats = lat_stats(oltp_samples, elapsed_secs);
    let olap_count = olap_samples.len() as f64;
    let olap_throughput_rps = if elapsed_secs > 0.0 && olap_count > 0.0 {
        olap_count / elapsed_secs
    } else {
        0.0
    };
    let olap_latency_ms = if olap_samples.is_empty() {
        0.0
    } else {
        olap_samples.iter().sum::<f64>() / olap_samples.len() as f64
    };

    // Correctness: every deterministic INSERT must be visible at the end.
    let final_count = table_count(&mut db);
    assert_eq!(
        final_count,
        seed_rows as u64 + inserts,
        "HTAP mix {name}: final COUNT(*) {final_count} != seed {seed_rows} + inserts {inserts}"
    );
    db.teardown();

    HtapMixResult {
        name: name.to_string(),
        oltp_p50_ms: oltp_stats.p50_ms,
        oltp_p95_ms: oltp_stats.p95_ms,
        oltp_p99_ms: oltp_stats.p99_ms,
        olap_throughput_rps,
        olap_latency_ms,
    }
}

/// Creates `ga_htap(id BIGINT PRIMARY KEY, v BIGINT)` and seeds `n` rows.
fn seed_htap_table(db: &mut LiveDb, n: usize) {
    db.exec("CREATE TABLE ga_htap(id BIGINT PRIMARY KEY, v BIGINT)");
    // Multi-row INSERT batches keep seeding fast while staying on the live path.
    const BATCH: usize = 50;
    let mut start = 0usize;
    while start < n {
        let end = (start + BATCH).min(n);
        let mut sql = String::from("INSERT INTO ga_htap(id, v) VALUES ");
        for (k, id) in (start..end).enumerate() {
            if k > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&format!("({id}, {})", row_value(id as i64)));
        }
        db.exec(&sql);
        start = end;
    }
}

/// Deterministic row payload for a given id.
fn row_value(id: i64) -> i64 {
    id.wrapping_mul(7).wrapping_add(3)
}

/// Live `SELECT COUNT(*)` scalar.
fn table_count(db: &mut LiveDb) -> u64 {
    let result = db
        .executor
        .execute("SELECT COUNT(*) FROM ga_htap")
        .expect("HTAP COUNT(*) must execute");
    scalar_u64(&result)
}

/// Extracts the single scalar of a one-row, one-column `Rows` result.
fn scalar_u64(result: &plomid_sql::QueryResult) -> u64 {
    match result {
        plomid_sql::QueryResult::Rows { rows, .. } => {
            let value = rows
                .first()
                .and_then(|row| row.first())
                .expect("COUNT(*) must return one row");
            value_to_i64(value).expect("COUNT(*) must be an integer scalar") as u64
        }
        other => panic!("COUNT(*) returned unexpected result: {other:?}"),
    }
}

/// Lenient integer coercion for aggregate/PK scalars across value encodings.
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
