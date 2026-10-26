#![forbid(unsafe_code)]
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
//! Server protocol and session transport foundations for PLOMID.
//!
//! # Ownership
//!
//! Owns the PostgreSQL wire surface: framing, authentication, session
//! parameters, extended-protocol state, and the single internal→SQLSTATE
//! mapping. Never owns query planning, execution, storage, or
//! transactions — those live behind the executor/engine handles each
//! connection holds. Depends on executor, txn, and storage; nothing
//! below it may depend on it.

pub mod protocol;
pub mod scram;
pub mod server;
pub mod session;
pub mod tls;

/// Background maintenance worker types (re-exported so the server binary can
/// spawn the worker without depending on the executor crate directly).
pub use plomid_executor::{MaintenanceWorker, WorkerLink};
