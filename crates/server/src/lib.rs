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
//! Library crate for the PLOMID server binary.
//!
//! This crate contains the shared configuration and orchestration logic
//! for the `plomid-server` binary. The entry point is `main.rs`.
//!
//! # Ownership
//!
//! Top of the dependency chain: wires listener, engine, maintenance, and
//! TLS together and owns nothing reusable. Operator configuration
//! (`config::ServerConfig`) is parsed and validated here; enforcement
//! itself lives in the components (network/session/engine).

pub mod backup_tool;
pub mod banner;
pub mod config;
