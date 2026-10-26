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

//! Row-authorization hook for result streams.

use std::sync::Arc;

/// Row-authorization predicate: return `false` to deny a row.
pub type AuthorizeRow<T> = Arc<dyn Fn(&T) -> bool + Send + Sync>;

/// Authorization policy applied to every row before buffering.
///
/// Rows rejected by `authorize_row` are **skipped**: they are never buffered,
/// never counted toward [`crate::StreamConfig::limit`], and never delivered.
/// This is where the executor must install its RLS predicate — this crate
/// never sees table ACLs, so without a predicate installed here no
/// row-level filtering happens at the transport layer.
pub struct StreamPolicy<T> {
    /// Return `false` to deny a row. `None` (default) authorizes every row.
    pub authorize_row: Option<AuthorizeRow<T>>,
}

// Manual Clone: the derive would add a spurious `T: Clone` bound.
impl<T> Clone for StreamPolicy<T> {
    fn clone(&self) -> Self {
        Self {
            authorize_row: self.authorize_row.clone(),
        }
    }
}

impl<T> StreamPolicy<T> {
    /// Authorize every row (default; no filtering).
    pub fn allow_all() -> Self {
        Self {
            authorize_row: None,
        }
    }

    /// Authorize rows with a predicate (e.g. the executor's RLS check).
    /// Rows for which `f` returns `false` are skipped, never leaked.
    pub fn from_fn(f: impl Fn(&T) -> bool + Send + Sync + 'static) -> Self {
        Self {
            authorize_row: Some(Arc::new(f)),
        }
    }

    /// True when the row may be buffered and delivered.
    pub fn authorize(&self, row: &T) -> bool {
        match &self.authorize_row {
            None => true,
            Some(predicate) => predicate(row),
        }
    }
}

impl<T> Default for StreamPolicy<T> {
    fn default() -> Self {
        Self::allow_all()
    }
}
