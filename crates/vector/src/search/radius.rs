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
//! Radius (range) search with inclusive boundary and optional limit.
//!
//! A row matches when `distance <= radius + TOLERANCE` ([`crate::distance::TOLERANCE`]).
//! NaN distances never match; `NULL` rows are skipped. Output is either sorted
//! by `(distance, row_id)` or input order (both deterministic); an optional
//! `limit` truncates after ordering.

use super::{candidate::Candidate, check_cancel, CancelFn, Result, SearchError, SearchMetric};
use crate::distance::TOLERANCE;

/// Radius query configuration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RadiusConfig {
    /// Inclusive distance bound (must be finite and `>= 0.0`).
    pub radius: f32,
    /// Maximum rows to return after ordering (`None` = unbounded, still
    /// streamed — output memory is `O(matches)`).
    pub limit: Option<usize>,
    /// Sort matches by `(distance, row_id)`; otherwise input order.
    pub sort: bool,
}

impl RadiusConfig {
    /// Build a sorted, unbounded radius query, validating the bound.
    pub fn new(radius: f32) -> Result<Self> {
        // NaN compares false against everything, so `is_nan() || < 0.0`
        // covers both negative and NaN without partial-ord negation.
        if radius.is_nan() || radius < 0.0 {
            return Err(SearchError::InvalidRadius { radius });
        }
        Ok(Self {
            radius,
            limit: None,
            sort: true,
        })
    }

    /// Set the row limit.
    #[must_use]
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set whether output is sorted.
    #[must_use]
    pub fn with_sort(mut self, sort: bool) -> Self {
        self.sort = sort;
        self
    }
}

/// Stream `rows`, collecting those within `config.radius` of `query`.
///
/// `rows` items are `(row_id, vector)` with `None` vectors (`NULL`) skipped.
/// Cancellation is checked every [`super::CANCEL_CHECK_EVERY`] rows.
pub fn radius_search(
    query: &[f32],
    rows: &[(u64, Option<Vec<f32>>)],
    metric: SearchMetric,
    config: &RadiusConfig,
    cancel: Option<&CancelFn>,
) -> Result<Vec<Candidate>> {
    radius_search_borrowed(
        query,
        rows.iter().map(|(id, v)| (*id, v.as_deref())),
        metric,
        config,
        cancel,
    )
}

/// Borrowed-slice variant of [`radius_search`] (no per-row allocation).
pub fn radius_search_borrowed<'a, I>(
    query: &[f32],
    rows: I,
    metric: SearchMetric,
    config: &RadiusConfig,
    cancel: Option<&CancelFn>,
) -> Result<Vec<Candidate>>
where
    I: IntoIterator<Item = (u64, Option<&'a [f32]>)>,
{
    radius_search_iter(query, rows.into_iter(), metric, config, cancel)
}

fn radius_search_iter<'a, I>(
    query: &[f32],
    rows: I,
    metric: SearchMetric,
    config: &RadiusConfig,
    cancel: Option<&CancelFn>,
) -> Result<Vec<Candidate>>
where
    I: Iterator<Item = (u64, Option<&'a [f32]>)>,
{
    if config.radius.is_nan() || config.radius < 0.0 {
        return Err(SearchError::InvalidRadius {
            radius: config.radius,
        });
    }
    super::check_query(query)?;
    if config.limit == Some(0) {
        return Ok(Vec::new());
    }
    let bound = config.radius + TOLERANCE;
    let mut out = Vec::new();
    let mut seen: usize = 0;
    for (row_id, vec) in rows {
        check_cancel(cancel, seen)?;
        seen = seen.saturating_add(1);
        let Some(d) = super::distance_for_metric(metric, query, vec)? else {
            continue; // NULL / undefined distance: not orderable, skip.
        };
        if d <= bound {
            out.push(Candidate::new(row_id, d));
        }
    }
    if config.sort {
        out.sort();
    }
    if let Some(limit) = config.limit {
        out.truncate(limit);
    }
    Ok(out)
}
