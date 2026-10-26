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
//! Plan cost model.
//!
//! Every plan shape decomposes into three additive components:
//!
//! - `cpu`: abstract row-processing units (`rows x cpu_cost_per_row`,
//!   scaled by operator and vectorization factors below).
//! - `io`: page reads (`pages x io_cost_per_page`), discounted by the cache
//!   hit rate and — for columnar scans — by the compression ratio.
//! - `mem`: peak resident rows (hash tables, group state). Memory is free up
//!   to `mem_limit_rows`; past that each excess row pays
//!   [`SPILL_PENALTY_PER_ROW`] to model temp-storage spill.
//!
//! `total = CPU_WEIGHT x cpu + IO_WEIGHT x io + spill(mem)` with
//! [`CPU_WEIGHT`] = 1.0 and [`IO_WEIGHT`] = 8.0: one random-ish page read
//! costs roughly eight row evaluations, the long-standing storage-layer rule
//! of thumb that sequential bandwidth is plentiful while seeks are not.
//!
//! Rows-per-page is derived from [`plomid_core::PAGE_SIZE`] over an assumed
//! 160-byte average row instead of being a magic number, so the model tracks
//! the real page layout. The 4x random-page multiplier, 3-page B-tree
//! descent, and the `sqrt(parallelism)` parallel speedup with linear
//! coordination overhead are documented heuristics, not measurements —
//! calibration follows measurement (same precedent as the executor's
//! `PAR_MIN_ROWS` gate).

use plomid_core::PAGE_SIZE;

/// Weight of one CPU unit in [`PlanCost::total`].
pub const CPU_WEIGHT: f64 = 1.0;
/// Weight of one IO unit in [`PlanCost::total`]: seeks dominate row probes.
pub const IO_WEIGHT: f64 = 8.0;
/// Extra total-cost units per resident row past [`CostInputs::mem_limit_rows`].
pub const SPILL_PENALTY_PER_ROW: f64 = 2.0;
/// Cost of one random row-store page relative to a sequential page.
pub const RANDOM_PAGE_COST_MULTIPLIER: f64 = 4.0;
/// B-tree descent pages paid by every index scan, independent of selectivity.
pub const INDEX_DESCENT_PAGES: f64 = 3.0;
/// Assumed average row width used to derive rows-per-page from `PAGE_SIZE`.
pub const ASSUMED_AVG_ROW_BYTES: f64 = 160.0;
/// Per-worker fraction of the base cost paid as parallel coordination.
pub const PARALLEL_COORDINATION_FRACTION: f64 = 0.02;
/// Vectorized columnar execution cost relative to scalar row execution.
pub const COLUMNAR_VECTOR_CPU_FACTOR: f64 = 0.4;
/// Hash operator CPU relative to a plain row visit (hash + compare).
pub const HASH_CPU_FACTOR: f64 = 1.5;
/// Distance-kernel CPU per lane relative to a plain row visit: one row costs
/// `dim * factor` (SIMD ladder + `f64` accumulators, not scalar emulation).
pub const VECTOR_DISTANCE_CPU_FACTOR: f64 = 2.0;
/// ANN probe overhead multiplier on the `log2(rows) * dim` descent term
/// (graph hops / centroid probes per query, before rerank).
pub const VECTOR_ANN_PROBE_FACTOR: f64 = 8.0;

/// Tunable inputs to the cost functions.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostInputs {
    /// CPU units per processed row.
    pub cpu_cost_per_row: f64,
    /// IO units per page read.
    pub io_cost_per_page: f64,
    /// Rows that fit in the operator memory budget before spilling.
    pub mem_limit_rows: u64,
    /// Fraction of page reads served from cache, `0.0..=1.0`.
    pub cache_hit_rate: f64,
    /// Columnar bytes shrink factor (`>= 1.0`); divides columnar IO.
    pub compression_ratio: f64,
    /// Worker count used when a caller omits an explicit parallelism.
    pub parallelism: usize,
}

impl Default for CostInputs {
    fn default() -> Self {
        Self {
            cpu_cost_per_row: 1.0,
            io_cost_per_page: 1.0,
            mem_limit_rows: 1_000_000,
            cache_hit_rate: 0.0,
            compression_ratio: 3.0,
            parallelism: 1,
        }
    }
}

/// Decomposed cost of one plan shape; compare [`PlanCost::total`].
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PlanCost {
    /// Weighted CPU component (already includes [`CPU_WEIGHT`]).
    pub cpu: f64,
    /// Weighted IO component (already includes [`IO_WEIGHT`]).
    pub io: f64,
    /// Peak resident rows driving the spill penalty.
    pub mem: f64,
    /// `cpu + io + spill(mem)`: the single comparison key.
    pub total: f64,
}

impl PlanCost {
    /// Zero cost (empty input).
    #[must_use]
    pub fn zero() -> Self {
        Self {
            cpu: 0.0,
            io: 0.0,
            mem: 0.0,
            total: 0.0,
        }
    }

    /// Returns true when `self.total` is strictly the cheaper plan.
    #[must_use]
    pub fn cheaper_than(&self, other: &Self) -> bool {
        self.total < other.total
    }
}

/// Rows per storage page, derived from the real page size.
fn rows_per_page() -> f64 {
    (PAGE_SIZE as f64 / ASSUMED_AVG_ROW_BYTES).max(1.0)
}

fn uncached(inputs: &CostInputs) -> f64 {
    (1.0 - inputs.cache_hit_rate.clamp(0.0, 1.0)).max(0.0)
}

fn assemble(cpu_units: f64, io_units: f64, mem_rows: f64, inputs: &CostInputs) -> PlanCost {
    let cpu = (cpu_units.max(0.0)) * CPU_WEIGHT;
    let io = (io_units.max(0.0)) * IO_WEIGHT;
    let spill = (mem_rows - inputs.mem_limit_rows as f64).max(0.0).max(0.0) * SPILL_PENALTY_PER_ROW;
    PlanCost {
        cpu,
        io,
        mem: mem_rows.max(0.0),
        total: cpu + io + spill,
    }
}

/// Full sequential scan of `rows` row-store rows (streaming, no hash state).
#[must_use]
pub fn cost_row_seq_scan(rows: u64, inputs: &CostInputs) -> PlanCost {
    if rows == 0 {
        return PlanCost::zero();
    }
    let rows = rows as f64;
    let cpu_units = rows * inputs.cpu_cost_per_row;
    let io_units = rows / rows_per_page() * inputs.io_cost_per_page * uncached(inputs);
    assemble(cpu_units, io_units, 0.0, inputs)
}

/// B-tree index scan returning `selectivity x rows` row-store rows.
///
/// IO touches only the matched fraction of the table (one random page per
/// matched row's share, at [`RANDOM_PAGE_COST_MULTIPLIER`]) plus a fixed
/// [`INDEX_DESCENT_PAGES`] descent; CPU evaluates just the matched rows.
/// Cheap at low selectivity, worse than a sequential scan past the crossover.
#[must_use]
pub fn cost_row_index_scan(selectivity: f64, rows: u64, inputs: &CostInputs) -> PlanCost {
    if rows == 0 {
        return PlanCost::zero();
    }
    let selectivity = selectivity.clamp(0.0, 1.0);
    let rows = rows as f64;
    let matched = selectivity * rows;
    let cpu_units = matched * inputs.cpu_cost_per_row * HASH_CPU_FACTOR
        + INDEX_DESCENT_PAGES * inputs.cpu_cost_per_row;
    let io_units = (matched / rows_per_page() * RANDOM_PAGE_COST_MULTIPLIER + INDEX_DESCENT_PAGES)
        * inputs.io_cost_per_page
        * uncached(inputs);
    assemble(cpu_units, io_units, 0.0, inputs)
}

/// Columnar scan of `cols_touched` of `total_cols` over `rows` rows.
///
/// IO scales with the projection width and is divided by the compression
/// ratio; CPU uses the vectorized [`COLUMNAR_VECTOR_CPU_FACTOR`] over all
/// scanned rows plus scalar output cost for the surviving fraction. Mild
/// zone-map pruning discounts IO halfway toward the surviving fraction.
#[must_use]
pub fn cost_column_scan(
    rows: u64,
    cols_touched: usize,
    total_cols: usize,
    selectivity: f64,
    parallelism: usize,
    inputs: &CostInputs,
) -> PlanCost {
    if rows == 0 {
        return PlanCost::zero();
    }
    let total_cols = total_cols.max(1);
    let width = (cols_touched.min(total_cols) as f64) / (total_cols as f64);
    let width = width.clamp(0.0, 1.0).max(1.0 / total_cols as f64);
    let selectivity = selectivity.clamp(0.0, 1.0);
    let rows = rows as f64;
    let compression = inputs.compression_ratio.max(1.0);
    // Zone maps skip fully-pruned row groups: halfway between full column
    // IO and perfectly proportional IO.
    let pruning = 0.5 + 0.5 * selectivity;
    let io_units = rows / rows_per_page() * width / compression
        * pruning
        * inputs.io_cost_per_page
        * uncached(inputs);
    let cpu_units = rows * width * inputs.cpu_cost_per_row * COLUMNAR_VECTOR_CPU_FACTOR
        + selectivity * rows * inputs.cpu_cost_per_row;
    let sequential = assemble(cpu_units, io_units, 0.0, inputs);
    apply_parallelism(sequential, cpu_units, io_units, parallelism)
}

/// Parallel columnar scan: explicit entry for the Phase-30 parallel path.
///
/// Identical to [`cost_column_scan`] with `parallelism >= 2`; values below 2
/// degrade to the sequential cost so callers need no special-casing.
#[must_use]
pub fn cost_column_scan_parallel(
    rows: u64,
    cols_touched: usize,
    total_cols: usize,
    selectivity: f64,
    parallelism: usize,
    inputs: &CostInputs,
) -> PlanCost {
    cost_column_scan(
        rows,
        cols_touched,
        total_cols,
        selectivity,
        parallelism.max(2),
        inputs,
    )
}

/// Vector execution mode for [`cost_vector_scan`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VectorMode {
    /// Brute-force exact scan: `rows * dim` lane visits.
    Exact,
    /// ANN candidate path: `log2(rows) * dim * probe` descent plus
    /// `rerank_rows * dim` exact rerank from authoritative rows.
    Ann,
}

/// Vector nearest-neighbour scan: `rows` candidate rows of `dim` lanes.
///
/// Exact mode costs `rows * dim * DISTANCE_FACTOR` CPU with a sequential
/// read of the vector payload (`dim * 4` bytes per row at full width).
/// ANN mode costs `log2(rows) * dim * PROBE_FACTOR` descent CPU plus
/// `rerank_rows * dim * DISTANCE_FACTOR` exact-rerank CPU over the same
/// payload IO. Memory peaks at `rerank_rows` (bounded heap + output), so
/// ANN wins on large estates and loses on small ones — the same crossover
/// the executor's `vector.auto_ann_min_rows` gate implements. `rerank_rows`
/// of `0` means "no rerank" (pure approximate candidates).
#[must_use]
pub fn cost_vector_scan(
    rows: u64,
    dim: usize,
    rerank_rows: u64,
    mode: VectorMode,
    inputs: &CostInputs,
) -> PlanCost {
    if rows == 0 || dim == 0 {
        return PlanCost::zero();
    }
    let lanes = rows as f64 * dim.max(1) as f64;
    let payload_pages = lanes * 4.0 / PAGE_SIZE as f64;
    match mode {
        VectorMode::Exact => {
            let cpu_units = lanes * inputs.cpu_cost_per_row * VECTOR_DISTANCE_CPU_FACTOR;
            let io_units = payload_pages * inputs.io_cost_per_page * uncached(inputs);
            assemble(cpu_units, io_units, 0.0, inputs)
        }
        VectorMode::Ann => {
            let descent = (rows.max(2) as f64).log2() * dim.max(1) as f64 * VECTOR_ANN_PROBE_FACTOR;
            let rerank = rerank_rows.min(rows) as f64
                * dim.max(1) as f64
                * inputs.cpu_cost_per_row
                * VECTOR_DISTANCE_CPU_FACTOR;
            let cpu_units = descent + rerank;
            let io_units = payload_pages * inputs.io_cost_per_page * uncached(inputs);
            assemble(cpu_units, io_units, rerank_rows.min(rows) as f64, inputs)
        }
    }
}

/// Filtered vector scan: `rows` survivors of an estimated `selectivity`
/// filter over a `dim`-lane working set.
///
/// Cost = filter evaluation over the working set plus the cheaper of the
/// exact scan and the ANN path over the survivors (same crossover as
/// [`cost_vector_scan`); the caller supplies the survivor count it planned
/// with. `selectivity` outside `0.0..=1.0` degrades to the unfiltered cost
/// rather than inventing survivors.
#[must_use]
pub fn cost_filtered_scan(
    rows: u64,
    dim: usize,
    selectivity: f64,
    limit: u32,
    inputs: &CostInputs,
) -> PlanCost {
    if rows == 0 || dim == 0 {
        return PlanCost::zero();
    }
    let survivors = if selectivity.is_finite() {
        (rows as f64 * selectivity.clamp(0.0, 1.0)) as u64
    } else {
        rows
    };
    let filter_cpu = rows as f64 * inputs.cpu_cost_per_row;
    let exact = cost_vector_scan(survivors, dim, 0, VectorMode::Exact, inputs);
    let ann = cost_vector_scan(survivors, dim, limit as u64, VectorMode::Ann, inputs);
    let best = if ann.cheaper_than(&exact) { ann } else { exact };
    PlanCost {
        cpu: best.cpu + filter_cpu * CPU_WEIGHT,
        io: best.io,
        mem: best.mem,
        total: best.total + filter_cpu * CPU_WEIGHT,
    }
}

/// Radius vector scan: every row within `bound` of the query is returned.
///
/// Radius work is not prunable without distribution statistics (which the
/// planner does not collect): the scan covers the working set in both
/// modes, so this costs the exact scan plus a verification term carried
/// by `well_rows` (the bounded candidate well the ANN path verifies).
/// `mode` selects which side of that comparison to price.
#[must_use]
pub fn cost_radius_scan(
    rows: u64,
    dim: usize,
    well_rows: u64,
    mode: VectorMode,
    inputs: &CostInputs,
) -> PlanCost {
    if rows == 0 || dim == 0 {
        return PlanCost::zero();
    }
    cost_vector_scan(rows, dim, well_rows.min(rows), mode, inputs)
}

/// Hash join: build over `build_rows`, probe with `probe_rows`.
///
/// `selectivity` is the fraction of probe rows emitting output (times one
/// match each). Memory peaks at the build side and spills past the limit.
#[must_use]
pub fn cost_hash_join(
    build_rows: u64,
    probe_rows: u64,
    selectivity: f64,
    inputs: &CostInputs,
) -> PlanCost {
    if build_rows == 0 || probe_rows == 0 {
        return PlanCost::zero();
    }
    let selectivity = selectivity.clamp(0.0, 1.0);
    let build = build_rows as f64;
    let probe = probe_rows as f64;
    let cpu_units =
        (build + probe) * inputs.cpu_cost_per_row * HASH_CPU_FACTOR + selectivity * probe;
    let io_units = (build + probe) / rows_per_page() * inputs.io_cost_per_page * uncached(inputs);
    assemble(cpu_units, io_units, build, inputs)
}

/// Nested-loop join: quadratic in the inputs; the fallback the hash join
/// must beat. No hash state, but catastrophic IO on large inputs.
#[must_use]
pub fn cost_nested_loop(outer_rows: u64, inner_rows: u64, inputs: &CostInputs) -> PlanCost {
    if outer_rows == 0 || inner_rows == 0 {
        return PlanCost::zero();
    }
    let pairs = outer_rows as f64 * inner_rows as f64;
    let cpu_units = pairs * inputs.cpu_cost_per_row;
    let io_units =
        (outer_rows as f64 + pairs / rows_per_page()) * inputs.io_cost_per_page * uncached(inputs);
    assemble(cpu_units, io_units, 0.0, inputs)
}

/// Join algorithm choice for an equi-join shape.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum JoinStrategy {
    /// Hash join: linear CPU plus build-side hash state (spills past the limit).
    Hash,
    /// Nested loop: quadratic pairs, no hash state; streams under memory pressure.
    NestedLoop,
}

/// Chooses the cheaper join algorithm for `build_rows` build rows probed by
/// `probe_rows` rows with output fraction `selectivity`.
///
/// Compares [`cost_hash_join`] against [`cost_nested_loop`] (outer = probe
/// rows driving the loop, inner = build rows scanned per outer row) with
/// [`PlanCost::cheaper_than`]; ties go to [`JoinStrategy::Hash`], the
/// sequential-scan-friendly default.
#[must_use]
pub fn choose_join(
    build_rows: u64,
    probe_rows: u64,
    selectivity: f64,
    inputs: &CostInputs,
) -> JoinStrategy {
    let hash = cost_hash_join(build_rows, probe_rows, selectivity, inputs);
    let nested = cost_nested_loop(probe_rows, build_rows, inputs);
    if nested.cheaper_than(&hash) {
        JoinStrategy::NestedLoop
    } else {
        JoinStrategy::Hash
    }
}

/// Hash aggregation of `rows` input rows into `groups` groups.
///
/// Memory peaks at `groups`; past the limit the spill penalty dominates,
/// which is what steers huge-group aggregations toward the columnar engine.
#[must_use]
pub fn cost_hash_aggregate(groups: u64, rows: u64, inputs: &CostInputs) -> PlanCost {
    if rows == 0 {
        return PlanCost::zero();
    }
    let rows = rows as f64;
    let groups = (groups as f64).min(rows).max(1.0);
    let cpu_units = rows * inputs.cpu_cost_per_row * HASH_CPU_FACTOR + groups;
    let io_units = rows / rows_per_page() * inputs.io_cost_per_page * uncached(inputs);
    assemble(cpu_units, io_units, groups, inputs)
}

/// Applies the parallel speedup to a sequential CPU/IO split.
///
/// Effective cost is `(cpu + io) / sqrt(p)` plus a coordination overhead of
/// [`PARALLEL_COORDINATION_FRACTION`] `x p x base`: parallelism always helps
/// at small worker counts and always saturates, matching the executor's
/// measured fan-out experience. Memory (hash state) is unaffected.
fn apply_parallelism(
    sequential: PlanCost,
    cpu_units: f64,
    io_units: f64,
    parallelism: usize,
) -> PlanCost {
    if parallelism <= 1 {
        return sequential;
    }
    let workers = parallelism as f64;
    let base = cpu_units * CPU_WEIGHT + io_units * IO_WEIGHT;
    let scaled = base / workers.sqrt() + PARALLEL_COORDINATION_FRACTION * workers * base;
    let spill = sequential.total - (sequential.cpu + sequential.io);
    PlanCost {
        cpu: sequential.cpu / workers.sqrt(),
        io: sequential.io / workers.sqrt(),
        mem: sequential.mem,
        total: scaled + spill.max(0.0),
    }
}
