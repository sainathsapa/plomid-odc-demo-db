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
//! Authorization model, credential shapes, and security audit events.
//!
//! This crate is the single home for the RBAC vocabulary shared by the
//! catalog (`plomid-sql`), the enforcement point (`plomid-executor`), and
//! the wire authentication layer (`plomid-network`). It owns:
//!
//! * the privilege taxonomy (table / schema / database) with parsing,
//!   bit encoding (for catalog persistence), and display;
//! * the security audit event shape (who did what to which object, and
//!   whether it was allowed) — emission is via `tracing` at call sites
//!   plus counters in `plomid-core::observability`, with an optional durable
//!   append-only JSONL file sink (`sink`, installed once at server startup
//!   via `--audit-log`);
//! * the denial error constructor (`42501 insufficient_privilege`).
//!
//! It deliberately owns NO storage and NO policy state: role membership
//! and grants live in the SQL catalog, and enforcement decisions happen in
//! the executor against a per-statement snapshot. It DOES own the single
//! audited-crypto surface (`credential`: SHA-256/HMAC/PBKDF2/Base64/
//! constant-time compare/CSPRNG salts via maintained RustCrypto crates —
//! Phase 3), shared by all consumers without crate cycles.
//!
//! Product contract (Beta 2): single database cluster, role-based access
//! with inheritance, owner-full-rights, superuser bypass, fail-closed
//! defaults (new roles hold no privileges; `public` schema USAGE is the
//! only default grant, matching PostgreSQL). File `COPY` is superuser-only
//! (PostgreSQL default behavior). There is no grant-option, no column-level
//! privilege, no row-level security, and no `SET ROLE` yet — all documented
//! in the RBAC runbook, none silently emulated.

pub mod audit;
pub mod credential;
pub mod privilege;
pub mod sink;

pub use audit::{denial_error, AuditAction, AuditDecision, AuditEvent};
pub use credential::{
    base64_decode, base64_encode, constant_time_eq, hmac_sha256, md5_password_response,
    pbkdf2_sha256, random_salt, sha256, RoleCredential, SaltError, SCRAM_ITERATIONS, VERIFIER_LEN,
};
pub use privilege::{
    DatabasePrivilege, GrantObject, RoleAttribute, SchemaPrivilege, TablePrivilege,
};
