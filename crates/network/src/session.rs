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
//! Session state management for a single PLOMID client connection.
//!
//! A session tracks the authenticated identity, transaction state, and
//! execution context for one client connection. The server creates one
//! session per accepted TCP connection.
//!
//! Sessions are not shared between connections. All state in this module
//! is local to a single connection and its associated SQL execution.

use crate::protocol::TransactionStatus;
use std::collections::HashMap;
use std::sync::Arc;

/// Transaction isolation level governing one explicit session transaction.
///
/// Only two levels exist: the default read-committed behavior (every
/// statement observes fresh committed state) and repeatable-read (all
/// statements observe the snapshot captured at BEGIN). SERIALIZABLE is
/// rejected with an explicit error before it can be stored here — it is
/// never silently served as anything weaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    ReadCommitted,
    RepeatableRead,
}

/// Session identity and state for a single client connection.
pub struct Session {
    pub id: u64,
    pub user: String,
    pub database: String,
    pub transaction_status: TransactionStatus,
    pub parameters: HashMap<String, String>,
    /// Statements staged by an explicit session transaction. They are sent
    /// to the existing executor as one transaction at COMMIT, so no
    /// uncommitted page changes reach storage in V1.
    /// Session default for explicit transactions without a level
    /// (`SET SESSION CHARACTERISTICS ...`). Reset only by another SET.
    pub default_isolation: IsolationLevel,
    /// Level of the currently open explicit transaction.
    pub isolation: IsolationLevel,
    /// Manager transaction id held active for the open REPEATABLE READ
    /// transaction. Keeps version GC and snapshot horizons valid for the
    /// pinned snapshot lifetime. `None` unless an RR transaction whose
    /// snapshot was successfully installed is open.
    pub rr_txn_id: Option<u64>,
}

impl Session {
    pub fn new(id: u64, user: impl Into<String>, database: impl Into<String>) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("server_version".to_string(), "17.0".to_string());
        parameters.insert("server_version_num".to_string(), "170000".to_string());
        parameters.insert("server_encoding".to_string(), "UTF8".to_string());
        parameters.insert("client_encoding".to_string(), "UTF8".to_string());
        parameters.insert("application_name".to_string(), "plomid".to_string());
        parameters.insert("DateStyle".to_string(), "ISO, MDY".to_string());
        parameters.insert("IntervalStyle".to_string(), "postgres".to_string());
        parameters.insert("TimeZone".to_string(), "UTC".to_string());
        parameters.insert("integer_datetimes".to_string(), "on".to_string());
        parameters.insert("standard_conforming_strings".to_string(), "on".to_string());
        parameters.insert("search_path".to_string(), "\"$user\", public".to_string());
        parameters.insert("datestyle".to_string(), "ISO".to_string());
        parameters.insert("isolation_level".to_string(), "read committed".to_string());
        // Query guardrail knobs (resource B6): accepted via SET and enforced
        // by the connection layer. `0` disables. Bare numbers are
        // milliseconds; `ms`/`s`/`min`/`h` suffixes are also accepted.
        parameters.insert("statement_timeout".to_string(), "0".to_string());
        parameters.insert(
            "idle_in_transaction_session_timeout".to_string(),
            "0".to_string(),
        );
        parameters.insert("lock_timeout".to_string(), "0".to_string());
        let user = user.into();
        let database = database.into();
        parameters.insert("user".to_string(), user.clone());
        parameters.insert("database".to_string(), database.clone());
        Self {
            id,
            user,
            database,
            transaction_status: TransactionStatus::Idle,
            parameters,
            default_isolation: IsolationLevel::ReadCommitted,
            isolation: IsolationLevel::ReadCommitted,
            rr_txn_id: None,
        }
    }

    pub fn set_parameter(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.parameters.insert(name.into(), value.into());
    }

    /// Applies operator-configured session timeout defaults (milliseconds,
    /// `0` disables). Called once for a fresh session before client startup
    /// parameters and `SET` are applied, so both still win. Values render
    /// as bare milliseconds, the guardrail parser's unit.
    pub fn apply_timeout_defaults(
        &mut self,
        statement_timeout_ms: u64,
        idle_in_transaction_timeout_ms: u64,
    ) {
        self.set_parameter("statement_timeout", statement_timeout_ms.to_string());
        self.set_parameter(
            "idle_in_transaction_session_timeout",
            idle_in_transaction_timeout_ms.to_string(),
        );
    }

    pub fn get_parameter(&self, name: &str) -> Option<&str> {
        self.parameters
            .get(name)
            .or_else(|| {
                self.parameters
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case(name))
                    .map(|(_, value)| value)
            })
            .map(|s| s.as_str())
    }

    pub fn mark_in_transaction(&mut self) {
        self.mark_in_transaction_with(IsolationLevel::ReadCommitted);
    }

    pub fn mark_in_transaction_with(&mut self, level: IsolationLevel) {
        self.transaction_status = TransactionStatus::InTransaction;
        self.isolation = level;
    }

    pub fn mark_idle(&mut self) {
        self.transaction_status = TransactionStatus::Idle;
        self.isolation = IsolationLevel::ReadCommitted;
        self.rr_txn_id = None;
    }

    pub fn mark_failed_transaction(&mut self) {
        self.transaction_status = TransactionStatus::InFailedTransaction;
    }

    pub fn is_in_transaction(&self) -> bool {
        self.transaction_status == TransactionStatus::InTransaction
    }

    /// True when the open transaction must observe a pinned snapshot.
    pub fn is_repeatable_read(&self) -> bool {
        self.is_in_transaction() && self.isolation == IsolationLevel::RepeatableRead
    }

    /// Takes the held repeatable-read transaction id for release.
    /// `mark_idle` also clears it, but release (abort + unpin) must run
    /// first — call this before `mark_idle` on COMMIT/ROLLBACK/disconnect.
    pub fn take_rr_txn(&mut self) -> Option<u64> {
        self.rr_txn_id.take()
    }
}

/// Server-side authentication configuration.
///
/// V1 supports a single username and password pair for all connections.
/// Authentication can be disabled entirely for local development. Future
/// versions will support per-user credentials, roles, and external
/// identity providers.
#[derive(Clone)]
pub struct ServerAuthConfig {
    pub username: String,
    pub password: String,
    pub enabled: bool,
    pub method: AuthMethod,
    pub scram_salt: [u8; 16],
    pub scram_salted_password: Arc<[u8]>,
}

impl std::fmt::Debug for ServerAuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerAuthConfig")
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("enabled", &self.enabled)
            .field("method", &self.method)
            .field("scram_salt", &"<redacted>")
            .field("scram_salted_password", &"<redacted>")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMethod {
    Cleartext,
    Md5,
    Scram,
}

impl ServerAuthConfig {
    /// Bootstrap credential pair. Fallible: the SCRAM verifier needs a
    /// CSPRNG salt, and exhaustion fails closed (startup refuses to run
    /// with predictable material).
    pub fn new(
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, plomid_security::SaltError> {
        let password = password.into();
        let (salt, verifier) = scram_salt_and_verifier(&password)?;
        Ok(Self {
            username: username.into(),
            scram_salt: salt,
            scram_salted_password: verifier,
            password,
            enabled: true,
            method: AuthMethod::Cleartext,
        })
    }

    pub fn md5(
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, plomid_security::SaltError> {
        let password = password.into();
        let (salt, verifier) = scram_salt_and_verifier(&password)?;
        Ok(Self {
            username: username.into(),
            scram_salt: salt,
            scram_salted_password: verifier,
            password,
            enabled: true,
            method: AuthMethod::Md5,
        })
    }

    pub fn scram(
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self, plomid_security::SaltError> {
        let password = password.into();
        let (salt, verifier) = scram_salt_and_verifier(&password)?;
        Ok(Self {
            username: username.into(),
            scram_salt: salt,
            scram_salted_password: verifier,
            password,
            enabled: true,
            method: AuthMethod::Scram,
        })
    }

    pub fn disabled() -> Self {
        Self {
            username: String::new(),
            password: String::new(),
            enabled: false,
            method: AuthMethod::Cleartext,
            scram_salt: [0; 16],
            scram_salted_password: Arc::from(Vec::<u8>::new()),
        }
    }

    pub fn authenticate(&self, user: &str, password: &str) -> bool {
        if !self.enabled {
            return true;
        }
        // No early exit: the password comparison always runs in constant
        // time, so a wrong username is indistinguishable from a wrong
        // password by timing (`&`, not `&&`, keeps both evaluations).
        let user_ok = user == self.username;
        let password_ok =
            plomid_security::constant_time_eq(password.as_bytes(), self.password.as_bytes());
        user_ok & password_ok
    }

    pub fn authenticate_md5(&self, user: &str, response: &str, salt: [u8; 4]) -> bool {
        if !self.enabled {
            return true;
        }
        // Legacy comparison helper (unit tests + legacy wiring); the
        // production path compares constant-time at the call site.
        // Same no-early-exit rule as `authenticate`: both sides evaluate.
        let user_ok = user == self.username;
        let response_ok = plomid_security::constant_time_eq(
            response.as_bytes(),
            md5_password_response(&self.password, user, salt).as_bytes(),
        );
        user_ok & response_ok
    }
}

/// Computes the PostgreSQL MD5 auth response for a plaintext secret.
///
/// Legacy path only: implemented with the maintained `md-5` crate via
/// `plomid-security` (the hand-rolled MD5 below was deleted in Phase 3).
/// `pub(crate)` for the wire layer's legacy-MD5 path (bootstrap + legacy
/// roles). Comparison stays constant-time at the call site.
pub fn md5_password_response(password: &str, user: &str, salt: [u8; 4]) -> String {
    plomid_security::md5_password_response(password, user, salt)
}

fn scram_salt_and_verifier(
    password: &str,
) -> Result<([u8; 16], Arc<[u8]>), plomid_security::SaltError> {
    // Single derivation site (see `plomid-security::credential`): the
    // bootstrap pair's SCRAM verifier, used only until first-run binding
    // stores a catalog verifier. Salt failure propagates (fail closed).
    let salt = plomid_security::random_salt()?;
    let verifier = Arc::from(plomid_security::pbkdf2_sha256(
        password.as_bytes(),
        &salt,
        4096,
        32,
    ));
    Ok((salt, verifier))
}
