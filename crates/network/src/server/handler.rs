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
//! Per-connection PostgreSQL frontend/backend message handling.

use super::guardrails::{SharedAccountLockout, SharedAuthRateLimiter};
use super::registry::{self, database_path, safe_database_name};
use super::statement_type;
use super::DatabaseEngines;
use crate::protocol::{
    AuthMechanism, ErrorResponse, FrontendTag, MessageDecoder, MessageEncoder, TransactionStatus,
};
use crate::session::{AuthMethod, IsolationLevel, ServerAuthConfig, Session};
use plomid_core::{ErrorKind, PlomidError, TxnId};
use plomid_executor::result_stream::rows_to_stream;
use plomid_executor::Executor;
use plomid_executor::SqlError;
use plomid_txn::{ConcurrentPlomidStorageEngine, PlomidStorageEngine};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter, ReadHalf, WriteHalf};
use tracing::warn;

/// Tiered automatic-maintenance default for server sessions.
///
/// Edge hosts (small devices, slow storage) get the relaxed edge policy
/// (larger mutation bound, longer burst-gate interval); laptop and server
/// tiers keep the current aggressive production values. Detected once per
/// process: the tier cannot change under a running server.
///
/// This is a *default*: any later explicit `set_maintenance_policy` call
/// wins by ordering, so operator-explicit configuration always overrides
/// it. Explicit `VACUUM` stays ungated on every tier.
fn tiered_maintenance_policy() -> plomid_executor::MaintenancePolicy {
    use std::sync::OnceLock;
    static CACHED: OnceLock<plomid_executor::MaintenancePolicy> = OnceLock::new();
    *CACHED.get_or_init(plomid_executor::MaintenancePolicy::tiered_default)
}

/// Transport quantum bounding one wire-send batch.
///
/// Both row-send loops (`COPY TO STDOUT` and extended-query `DataRow`) pull
/// the materialized result through the [`rows_to_stream`] bridge in batches
/// of this size for the encode+send loop. Wire bytes are unchanged (same
/// encoder calls, same order); only the per-iteration encode quantum is
/// bounded. Slow-client backpressure is unchanged — the `.await` writes below
/// still apply per batch. The completion-tag count comes from delivered
/// batches rather than `rows.len()`.
pub const WIRE_SEND_BATCH_SIZE: usize = 1000;

/// Cap for one `COPY ... FROM <file>` server-side read, in bytes.
///
/// Matches the wire `COPY FROM STDIN` accumulation cap
/// (`MAX_COPY_BUFFER_BYTES`): a file larger than this fails closed instead
/// of OOMing the connection with an unbounded `read_to_end`. `COPY TO`
/// output larger than this also fails closed (disk-fill guard).
pub const MAX_COPY_FILE_BYTES: usize = 64 * 1024 * 1024;

/// Cap for the raw client-supplied COPY path, in bytes. PostgreSQL
/// `PATH_MAX`-style bound: rejects absurd inputs before any filesystem
/// touch.
const MAX_COPY_PATH_BYTES: usize = 4096;

/// Runs a synchronous storage-engine call without starving the async runtime.
///
/// Engine work is blocking: a commit fsyncs the WAL inline, and a committer
/// that arrives while another flush is in flight parks on a `std::sync::Condvar`.
/// Polling that straight from a connection task pins the tokio worker for the
/// whole fsync. Two measured consequences: an unrelated connection scheduled on
/// the same worker cannot reach its own commit until the fsync returns, and —
/// because it therefore never arrives while a flush is in flight — the
/// group-commit coordinator sees a single committer per round (`group=1`, one
/// fsync per commit) instead of sharing one fsync across the commits that are
/// ready at the same time. `block_in_place` tells the multi-thread runtime this
/// worker is about to block, so its other tasks run on sibling threads.
fn blocking<R>(f: impl FnOnce() -> R) -> R {
    match tokio::runtime::Handle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(f)
        }
        // A current-thread runtime (the `#[tokio::test]` default) has no sibling
        // worker to hand tasks to, and calling `block_in_place` there panics.
        _ => f(),
    }
}

pub struct ConnectionHandler {
    connection_id: u64,
    reader: BufReader<ReadHalf<crate::tls::Transport>>,
    writer: BufWriter<WriteHalf<crate::tls::Transport>>,
    engine: Arc<Mutex<PlomidStorageEngine>>,
    executor: Option<Executor<ConcurrentPlomidStorageEngine>>,
    auth_config: ServerAuthConfig,
    /// True when the client upgraded via `SSLRequest` + rustls handshake.
    used_tls: bool,
    /// True when the listener requires TLS: a plaintext startup is refused
    /// with `FATAL 28000` before authentication.
    tls_required: bool,
    process_id: u32,
    secret_key: u32,
    prepared_statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    active_result_formats: Vec<u16>,
    active_result_type_oids: Vec<u32>,
    extended_error: bool,
    extended_description_sent: bool,
    query_counter: Arc<std::sync::atomic::AtomicU64>,
    database_context: Option<(PathBuf, Arc<tokio::sync::Mutex<registry::DatabaseRegistry>>)>,
    /// Storage engines shared per database (see `DatabaseEngines`): secondary
    /// databases resolve here instead of opening a private engine per
    /// connection, which raced the WAL/segment files.
    database_engines: Option<DatabaseEngines>,
    copy_state: Option<CopyState>,
    /// Directory `COPY ... FROM/TO <file>` paths are jailed to. `None`
    /// denies every file COPY (fail closed).
    copy_dir: Option<PathBuf>,
    /// Background maintenance endpoint for this connection's sessions.
    /// Installed from the server; sessions submit due tables instead of
    /// running passes on committing connections.
    maintenance_link: Option<crate::WorkerLink>,
    /// Operator-configured enforcement budgets for this connection.
    enforcement: crate::server::ServerEnforcement,
    /// INSERT statements received through one extended-protocol pipeline.
    /// psycopg executemany sends Parse once, then many Bind/Execute messages,
    /// followed by one Sync.  Keep those compatible writes together so the
    /// executor can coalesce them into one transaction and one durability
    /// boundary.
    /// Per-connection cancel token. Registered in the server-wide
    /// [`CancelRegistry`] under `process_id` by the server task; a
    /// CancelRequest holding (`process_id`, `secret_key`) sets this flag and
    /// the running query aborts cooperatively through the executor guard.
    /// Safe: all engine mutations are transactional.
    cancel_flag: Arc<AtomicBool>,
    /// Peer IP for auth rate limiting. `None` skips limiting
    /// (tests that construct handlers directly).
    peer_ip: Option<std::net::IpAddr>,
    auth_limiter: Option<SharedAuthRateLimiter>,
    /// Per-username account lockout (brute-force backstop). `None` skips
    /// lockout accounting (unit-constructed handlers).
    account_lockout: Option<SharedAccountLockout>,
}

/// Maps CSPRNG exhaustion to a loud connection error (fail closed:
/// the connection closes with a clear message instead of proceeding with
/// predictable material).
fn salt_io(error: plomid_security::SaltError) -> std::io::Error {
    std::io::Error::other(error.to_string())
}

/// Method name for audit events (never carries secrets).
fn method_name(method: &AuthMethod) -> &'static str {
    match method {
        AuthMethod::Cleartext => "password",
        AuthMethod::Md5 => "md5",
        AuthMethod::Scram => "scram",
    }
}

/// Per-attempt authentication diagnostics for F-007 recurrence capture.
///
/// The fields separate cost and state that the `security_decision` line
/// alone cannot distinguish: pre-auth catalog/session load (`executor_ms`),
/// the credential exchange (`auth_ms`), rate-limit throttling, lockout
/// state, and which credential source the catalog presented. Never carries
/// a password, salt, or verifier.
#[derive(Debug, Default)]
struct AuthDiagnostics {
    method: &'static str,
    total_ms: u128,
    executor_ms: u128,
    auth_ms: u128,
    rate_limit_failures: u32,
    rate_limit_delay_ms: u128,
    locked_out: bool,
    lockout_triggered: bool,
    role_present: bool,
    has_verifier: bool,
    has_legacy_password: bool,
    bootstrap_name: bool,
}

struct CopyState {
    table: String,
    columns: Option<String>,
    data: Vec<u8>,
    format: CopyFileFormat,
    delimiter: u8,
    header: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyFileDirection {
    From,
    To,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyFileFormat {
    Text,
    Csv,
}

#[derive(Debug, Clone)]
struct CopyFileSpec {
    table: String,
    columns: Option<String>,
    path: PathBuf,
    direction: CopyFileDirection,
    format: CopyFileFormat,
    delimiter: u8,
    header: bool,
}

#[derive(Debug, Clone)]
struct PreparedStatement {
    query: String,
    parameter_types: Vec<u32>,
    /// Result shape learned by a statement/portal Describe. Prepared clients
    /// re-Describe the portal before every Execute, so re-running the probe
    /// would execute read-only statements (including ANN queries) twice.
    describe_shape: Option<CachedDescribeShape>,
}

/// Result-column metadata cached for a prepared statement.
///
/// PostgreSQL fixes a prepared statement's result shape at Describe time; a
/// portal Describe reports the same shape with the portal's output formats.
/// Caching it avoids executing the statement again merely to rediscover
/// metadata. The generation/search-path/database key invalidates the cache
/// when DDL or name resolution could change what the statement means.
#[derive(Debug, Clone)]
struct CachedDescribeShape {
    columns: Vec<String>,
    column_types: Vec<Option<plomid_sql::ColumnType>>,
    catalog_generation: u64,
    database: String,
    search_path: String,
}

/// One Describe outcome: either the cached shape or a freshly probed result.
enum DescribeShapeOutcome {
    Cached(CachedDescribeShape),
    Probed {
        result: plomid_sql::QueryResult,
        catalog_generation: u64,
    },
}

#[derive(Debug, Clone)]
struct Portal {
    statement: String,
    parameters: Vec<Option<String>>,
    result_formats: Vec<u16>,
}

impl ConnectionHandler {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        connection_id: u64,
        transport: crate::tls::Transport,
        used_tls: bool,
        tls_required: bool,
        engine: Arc<Mutex<PlomidStorageEngine>>,
        auth_config: ServerAuthConfig,
        process_id: u32,
        secret_key: u32,
        query_counter: Arc<std::sync::atomic::AtomicU64>,
        database_context: Option<(PathBuf, Arc<tokio::sync::Mutex<registry::DatabaseRegistry>>)>,
        database_engines: Option<DatabaseEngines>,
        maintenance_link: Option<crate::WorkerLink>,
        cancel_flag: Arc<AtomicBool>,
        peer_ip: Option<std::net::IpAddr>,
        auth_limiter: SharedAuthRateLimiter,
        account_lockout: SharedAccountLockout,
        enforcement: crate::server::ServerEnforcement,
    ) -> Self {
        // PostgreSQL clients issue many small request/response exchanges.
        // Disable Nagle buffering so a response is not delayed behind a
        // subsequent packet on loopback or low-latency networks.
        let _ = transport.set_nodelay(true);
        let (reader, writer) = tokio::io::split(transport);
        Self {
            connection_id,
            reader: BufReader::new(reader),
            writer: BufWriter::new(writer),
            engine,
            executor: None,
            auth_config,
            used_tls,
            tls_required,
            process_id,
            secret_key,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            active_result_formats: Vec::new(),
            active_result_type_oids: Vec::new(),
            extended_error: false,
            extended_description_sent: false,
            query_counter,
            database_context,
            database_engines,
            copy_state: None,
            copy_dir: None,
            maintenance_link,
            cancel_flag,
            peer_ip,
            auth_limiter: Some(auth_limiter),
            account_lockout: Some(account_lockout),
            enforcement,
        }
    }

    /// Session timeout defaults for a fresh session, from the operator's
    /// enforcement budgets. Values render as bare milliseconds (the
    /// guardrail parser's unit); `0` disables, matching the library
    /// default, and per-session `SET` still overrides afterwards.
    pub(crate) fn stamp_session_defaults(&self, session: &mut Session) {
        session.apply_timeout_defaults(
            self.enforcement.statement_timeout_ms,
            self.enforcement.idle_in_transaction_timeout_ms,
        );
    }

    /// Runs the connection from an already-negotiated startup payload.
    ///
    /// Pre-startup negotiation (`SSLRequest`/`GSSENCRequest`/`CancelRequest`)
    /// happens in the server task before construction (see
    /// [`crate::tls::negotiate`]): by the time this runs the transport is
    /// final and `startup` holds the bytes after the 4-byte length word.
    pub(crate) async fn run(&mut self, startup: Vec<u8>) -> std::io::Result<()> {
        let run_started = Instant::now();
        // TLS-required listeners refuse plaintext before authentication:
        // no credential exchange happens on an unencrypted channel.
        if self.tls_required && !self.used_tls {
            plomid_core::observability::record_tls_plaintext_refusal();
            self.send_error_response_fatal(
                crate::tls::TLS_REQUIRED_SQLSTATE,
                crate::tls::TLS_REQUIRED_MESSAGE,
            )
            .await?;
            return Ok(());
        }
        let params = MessageDecoder::parse_startup(&startup)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let user = params
            .get("user")
            .cloned()
            .unwrap_or_else(|| "plomid".to_string());
        let database = params
            .get("database")
            .cloned()
            .unwrap_or_else(|| "plomid".to_string());

        let mut diag = AuthDiagnostics {
            method: method_name(&self.auth_config.method),
            ..AuthDiagnostics::default()
        };

        // Auth rate limit: per-IP failed-attempt counter with a
        // growing, capped delay applied in front of the three auth exchanges.
        // Bounded to the last N IPs; success resets the counter.
        if self.auth_config.enabled {
            if let (Some(limiter), Some(ip)) = (self.auth_limiter.clone(), self.peer_ip) {
                let (failures, delay) = {
                    let limiter = limiter.lock().await;
                    (limiter.failure_count(&ip), limiter.delay_for(&ip))
                };
                diag.rate_limit_failures = failures;
                diag.rate_limit_delay_ms = delay.as_millis();
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
        }

        // Secondary databases resolve to the single shared engine for that
        // database (see `DatabaseEngines`). A private engine per connection
        // races the WAL/segment files with independent writers and LSN
        // counters and corrupts them under any concurrent use.
        if !database.eq_ignore_ascii_case("plomid") {
            if let Some((data_dir, registry)) = self.database_context.clone() {
                let database_dir = {
                    let mut registry = registry.lock().await;
                    match registry.open_path(&data_dir, &database) {
                        Ok(path) => path,
                        Err(message) => {
                            drop(registry);
                            self.send_error_response_fatal("3D000", message.to_string())
                                .await?;
                            return Ok(());
                        }
                    }
                };
                let database_names = registry
                    .lock()
                    .await
                    .names
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                match self.shared_engine_for(&database, &database_dir, 1024).await {
                    Ok(engine) => {
                        self.engine = engine;
                    }
                    Err(error) => {
                        self.send_error_response_fatal("3D000", error.to_string())
                            .await?;
                        return Ok(());
                    }
                }
                let _ = database_names;
            }
        }

        let executor_started = Instant::now();
        let mut executor = match Executor::new_shared(self.engine.clone()) {
            Ok(executor) => executor,
            Err(error) => {
                diag.executor_ms = executor_started.elapsed().as_millis();
                diag.total_ms = run_started.elapsed().as_millis();
                self.log_auth_diagnostics(&diag, "deny", "session_init_failed");
                return Err(std::io::Error::other(error.to_string()));
            }
        };
        diag.executor_ms = executor_started.elapsed().as_millis();
        executor.set_session_context(&database, &user);
        // Tiered automatic-maintenance default: edge hosts burst-gate longer
        // with a larger mutation bound; laptop/server tiers keep the
        // production values (one pass per 64 commits max, per-minute gate).
        // Explicit VACUUM stays ungated.
        executor.set_maintenance_policy(tiered_maintenance_policy());
        // Background maintenance when the server installed it: due tables go
        // to the worker instead of the committing connection.
        if let Some(link) = self.maintenance_link.clone() {
            executor.set_background_maintenance(link);
        }
        if let Some((_, registry)) = self.database_context.as_ref() {
            let names = registry.lock().await.names.iter().cloned().collect();
            executor.set_database_names(names);
        }
        self.executor = Some(executor);

        // CONNECT gate on the startup database (denied → FATAL 28000 with
        // an audit event, before any session state is established).
        {
            let executor = self.executor.as_ref().expect("executor initialized");
            if executor.check_database_connect(&user, &database).is_err() {
                let event = plomid_security::AuditEvent::deny(
                    &user,
                    plomid_security::AuditAction::Authorize {
                        statement: "CONNECT".to_string(),
                    },
                    Some(format!("DATABASE {database}")),
                    "connect_revoked",
                );
                tracing::warn!(target: "security", "{}", event.log_line());
                let _ = plomid_security::sink::emit(&event);
                plomid_core::observability::record_authz_denial();
                self.send_error_response_fatal(
                    "28000",
                    format!("permission denied for database \"{database}\""),
                )
                .await?;
                return Ok(());
            }
        }

        // Catalog-aware authentication. Trust mode (`auth=none`, loopback
        // only) accepts the startup identity and still authorizes every
        // statement below; password modes verify against the catalog and
        // bind the server secret on first run (bootstrap).
        if !self.auth_config.enabled {
            self.write_message(MessageEncoder::encode_authentication(AuthMechanism::Ok))
                .await?;
        } else {
            let (role_present, has_verifier, has_legacy_password) = self.auth_subject_state(&user);
            diag.role_present = role_present;
            diag.has_verifier = has_verifier;
            diag.has_legacy_password = has_legacy_password;
            diag.bootstrap_name = user == self.auth_config.username;
            if let Some(lockout) = self.account_lockout.clone() {
                if lockout.lock().await.is_locked(&user) {
                    diag.locked_out = true;
                    diag.total_ms = run_started.elapsed().as_millis();
                    self.log_auth_diagnostics(&diag, "deny", "locked_out");
                    plomid_core::observability::record_auth_failure();
                    plomid_core::observability::record_auth_locked_out();
                    let event = plomid_security::AuditEvent::deny(
                        &user,
                        plomid_security::AuditAction::Authenticate {
                            method: method_name(&self.auth_config.method).to_string(),
                        },
                        None,
                        "locked_out",
                    );
                    tracing::warn!(target: "security", "{}", event.log_line());
                    let _ = plomid_security::sink::emit(&event);
                    self.send_error_response_fatal(
                        "28P01",
                        format!("account \"{user}\" is temporarily locked after repeated authentication failures"),
                    )
                    .await?;
                    return Ok(());
                }
            }
            let auth_started = Instant::now();
            let authenticated = match self.auth_config.method {
                AuthMethod::Cleartext => self.authenticate_cleartext(&user).await?,
                AuthMethod::Md5 => self.authenticate_md5(&user).await?,
                AuthMethod::Scram => self.authenticate_scram(&user).await?,
            };
            diag.auth_ms = auth_started.elapsed().as_millis();
            if let (Some(limiter), Some(ip)) = (self.auth_limiter.clone(), self.peer_ip) {
                let mut limiter = limiter.lock().await;
                if authenticated {
                    limiter.record_success(ip);
                } else {
                    limiter.record_failure(ip);
                }
            }
            if let Some(lockout) = self.account_lockout.clone() {
                let mut lockout = lockout.lock().await;
                if authenticated {
                    lockout.record_success(&user);
                } else if lockout.record_failure(&user) {
                    diag.lockout_triggered = true;
                    diag.total_ms = run_started.elapsed().as_millis();
                    self.log_auth_diagnostics(&diag, "deny", "too_many_failures");
                    plomid_core::observability::record_account_lockout();
                    let event = plomid_security::AuditEvent::deny(
                        &user,
                        plomid_security::AuditAction::Lockout { locked: true },
                        None,
                        "too_many_failures",
                    );
                    tracing::warn!(target: "security", "{}", event.log_line());
                    let _ = plomid_security::sink::emit(&event);
                }
            }
            if !authenticated {
                diag.total_ms = run_started.elapsed().as_millis();
                self.log_auth_diagnostics(&diag, "deny", "bad_credentials");
                plomid_core::observability::record_auth_failure();
                let event = plomid_security::AuditEvent::deny(
                    &user,
                    plomid_security::AuditAction::Authenticate {
                        method: method_name(&self.auth_config.method).to_string(),
                    },
                    None,
                    "bad_credentials",
                );
                tracing::warn!(target: "security", "{}", event.log_line());
                let _ = plomid_security::sink::emit(&event);
                // FATAL already sent (or the peer went away): fail closed.
                // Never continue to session setup after a failed login.
                return Ok(());
            }
            // Successful logins are audited too: the sink records both sides
            // of the authentication gate, never the credential itself.
            diag.total_ms = run_started.elapsed().as_millis();
            self.log_auth_diagnostics(&diag, "allow", "-");
            let event = plomid_security::AuditEvent::allow(
                &user,
                plomid_security::AuditAction::Authenticate {
                    method: method_name(&self.auth_config.method).to_string(),
                },
                None,
            );
            tracing::info!(target: "security", "{}", event.log_line());
            let _ = plomid_security::sink::emit(&event);
        }

        let mut session = Session::new(self.connection_id, user, database);
        // Operator production defaults first: client-supplied startup
        // parameters (applied below) and per-session SET still win.
        self.stamp_session_defaults(&mut session);
        for name in [
            "application_name",
            "client_encoding",
            "DateStyle",
            "TimeZone",
            "search_path",
        ] {
            if let Some(value) = params.get(name) {
                session.set_parameter(name, value);
            }
        }

        for (name, value) in &session.parameters {
            self.write_message_buffered(MessageEncoder::encode_parameter_status(name, value))
                .await?;
        }

        self.write_message_buffered(MessageEncoder::encode_backend_key_data(
            self.process_id,
            self.secret_key,
        ))
        .await?;
        self.write_message_buffered(MessageEncoder::encode_ready_for_query(
            TransactionStatus::Idle,
        ))
        .await?;
        self.writer.flush().await?;

        loop {
            // Idle-in-transaction reap (txn M5): an idle-in-txn connection
            // holds its repeatable-read pin and blocks GC/horizon progress.
            // `idle_in_transaction_session_timeout=0` disables. On expiry the
            // pin is released and the connection is terminated with FATAL
            // 25P03 instead of lingering.
            let (tag, payload) = match self.read_message_bounded(&mut session).await? {
                Some(pair) => pair,
                None => break,
            };
            if self.extended_error && !matches!(tag, FrontendTag::Sync | FrontendTag::Terminate) {
                continue;
            }
            match tag {
                FrontendTag::Query => {
                    let query_id = self.next_query_id();
                    let sql = String::from_utf8(payload)
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
                        .trim_end_matches('\0')
                        .to_owned();
                    if sql.trim().is_empty() {
                        self.write_message(MessageEncoder::encode_empty_query())
                            .await?;
                        self.write_message(MessageEncoder::encode_ready_for_query(
                            session.transaction_status,
                        ))
                        .await?;
                        continue;
                    }
                    tracing::debug!(target: "server::protocol", event = "query_received", query_id, connection_id = self.connection_id, statement_type = statement_type(&sql), sql_len = sql.len());
                    if let Some(copy) = parse_copy_from_stdin(&sql) {
                        self.copy_state = Some(copy);
                        self.write_message(MessageEncoder::encode_copy_in_response())
                            .await?;
                    } else if let Some(table) = parse_copy_to_stdout(&sql) {
                        self.finish_copy_to(&mut session, &table).await?;
                    } else if let Some(spec) = parse_copy_file(&sql) {
                        self.finish_copy_file(&mut session, spec).await?;
                    } else {
                        self.handle_query(&mut session, &sql, true, query_id)
                            .await?;
                    }
                }
                FrontendTag::Parse => {
                    if let Err(error) = self.handle_parse(&payload).await {
                        self.extended_error = true;
                        self.send_protocol_error(error.to_string()).await?;
                    }
                }
                FrontendTag::Bind => {
                    if let Err(error) = self.handle_bind(&payload).await {
                        self.extended_error = true;
                        self.send_protocol_error(error.to_string()).await?;
                    }
                }
                FrontendTag::Describe => {
                    if let Err(error) = self.handle_describe(&payload, &mut session).await {
                        self.extended_error = true;
                        self.send_protocol_error(error.to_string()).await?;
                    }
                }
                FrontendTag::Execute => {
                    let query_id = self.next_query_id();
                    tracing::debug!(target: "server::protocol", event = "ext_execute", query_id, txn_status = %session.transaction_status);
                    let (portal_name, max_rows) = parse_execute(&payload)?;
                    let portal = self
                        .portals
                        .get(&portal_name)
                        .cloned()
                        .ok_or_else(|| protocol_error("portal does not exist"))?;
                    let statement = self
                        .prepared_statements
                        .get(&portal.statement)
                        .cloned()
                        .ok_or_else(|| protocol_error("prepared statement does not exist"))?;
                    let mut sql = bind_query(&statement.query, &portal.parameters);
                    if max_rows > 0 && should_apply_result_limit(&sql) {
                        sql.push_str(&format!(" LIMIT {max_rows}"));
                    }
                    self.active_result_formats = portal.result_formats.clone();
                    // Result-column OIDs come from the executor's result
                    // metadata.  Bind parameter OIDs describe inputs only;
                    // reusing them here corrupts RowDescription and binary
                    // output for expressions such as `$1::text`.
                    self.active_result_type_oids.clear();
                    self.handle_query(&mut session, &sql, false, query_id)
                        .await?;
                }
                FrontendTag::Sync => {
                    self.extended_error = false;
                    tracing::debug!(target: "server::protocol", event = "ext_sync", txn_status = %session.transaction_status);
                    self.write_message(MessageEncoder::encode_ready_for_query(
                        session.transaction_status,
                    ))
                    .await?;
                }
                FrontendTag::Flush => {}
                FrontendTag::CopyDone => {
                    if self.copy_state.is_some() {
                        self.finish_copy(&mut session).await?;
                    } else {
                        self.send_protocol_error("COPY done received outside COPY FROM STDIN")
                            .await?;
                    }
                }
                FrontendTag::Close => {
                    if let Some((kind, name)) = parse_close(&payload)? {
                        if kind == b'S' {
                            self.prepared_statements.remove(&name);
                            self.portals.retain(|_, portal| portal.statement != name);
                        } else {
                            self.portals.remove(&name);
                        }
                    }
                    self.write_message(MessageEncoder::encode_close_complete())
                        .await?;
                }
                FrontendTag::CopyData => {
                    if let Some(copy) = self.copy_state.as_mut() {
                        // Fail closed: COPY input accumulates until CopyDone.
                        if copy.data.len().saturating_add(payload.len())
                            > self.enforcement.max_copy_buffer_bytes
                        {
                            self.copy_state = None;
                            self.send_protocol_error("COPY input is too large").await?;
                        } else {
                            copy.data.extend_from_slice(&payload);
                        }
                    } else {
                        self.send_protocol_error("COPY data received outside COPY FROM STDIN")
                            .await?;
                    }
                }
                FrontendTag::CopyFail => {
                    self.copy_state = None;
                    self.write_message(MessageEncoder::encode_ready_for_query(
                        session.transaction_status,
                    ))
                    .await?;
                }
                FrontendTag::Terminate => {
                    tracing::info!(target: "server::protocol", "connection_terminate connection_id={}", self.connection_id);
                    break;
                }
                _ => {
                    warn!(target: "server::protocol", "unexpected_message connection_id={} tag={:?}", self.connection_id, tag);
                }
            }
        }
        // Connection teardown: never orphan an open session transaction
        // (its manager transaction id would pin GC and horizons forever,
        // and its write lanes would strand other writers). Best-effort
        // abort: staged effects are discarded, catalog images are dropped
        // with the executor.
        if let Some(engine) = self.executor.as_mut() {
            let _ = blocking(|| engine.abort_session_txn());
        }
        self.release_rr_pin(&mut session);
        Ok(())
    }

    async fn read_password_message(&mut self) -> std::io::Result<Option<String>> {
        let password_bytes = match self.read_message().await? {
            Some((FrontendTag::Password, payload)) => payload,
            Some(_) => {
                self.send_error_response_fatal("08P01", "expected PasswordMessage")
                    .await?;
                return Ok(None);
            }
            None => return Ok(None),
        };
        let password = String::from_utf8(password_bytes)
            .map(|password| password.trim_end_matches('\0').to_owned())
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid password encoding")
            })?;
        Ok(Some(password))
    }

    /// Sends a COPY-path error, marking the session failed when inside an
    /// explicit transaction (the executor aborts the session transaction on
    /// any statement error, so the session must reflect it like PostgreSQL).
    async fn send_copy_error(
        &mut self,
        session: &mut Session,
        error: SqlError,
    ) -> std::io::Result<()> {
        if session.is_in_transaction() {
            session.mark_failed_transaction();
        }
        self.send_error_response(error, session.transaction_status)
            .await
    }

    async fn finish_copy(&mut self, session: &mut Session) -> std::io::Result<()> {
        let Some(copy) = self.copy_state.take() else {
            return Ok(());
        };
        let copy_spec = CopyFileSpec {
            table: copy.table.clone(),
            columns: copy.columns.clone(),
            path: PathBuf::new(),
            direction: CopyFileDirection::From,
            format: copy.format,
            delimiter: copy.delimiter,
            header: copy.header,
        };
        let rows = match parse_copy_file_rows(&copy.data, &copy_spec) {
            Ok(rows) => rows
                .into_iter()
                .filter(|row| !row.is_empty())
                .map(|row| row.join(", "))
                .collect::<Vec<_>>(),
            Err(message) => {
                self.send_error_response(
                    sql_error_from_io(format!("invalid COPY input: {message}")),
                    session.transaction_status,
                )
                .await?;
                return Ok(());
            }
        };
        if rows.is_empty() {
            self.write_message(MessageEncoder::encode_command_complete("COPY 0"))
                .await?;
            self.write_message(MessageEncoder::encode_ready_for_query(
                session.transaction_status,
            ))
            .await?;
            return Ok(());
        }
        let columns = copy.columns.unwrap_or_default();
        let sql = format!(
            "INSERT INTO {}{} VALUES {}",
            copy.table,
            columns,
            rows.into_iter()
                .map(|row| format!("({row})"))
                .collect::<Vec<_>>()
                .join(", ")
        );
        let result = {
            let engine = self.executor.as_mut().expect("executor initialized");
            engine.execute_session_aware(&sql)
        };
        match result {
            Ok(plomid_sql::QueryResult::Inserted(count)) => {
                self.write_message(MessageEncoder::encode_command_complete(&format!(
                    "COPY {count}"
                )))
                .await?;
            }
            Ok(_) => {
                self.write_message(MessageEncoder::encode_command_complete("COPY 0"))
                    .await?;
            }
            Err(error) => {
                // COPY is a frontend protocol exchange. Even when the
                // generated INSERT fails, the client must receive both the
                // SQL error and ReadyForQuery; otherwise psycopg remains
                // blocked waiting for the command to finish.
                self.send_copy_error(session, error).await?;
                return Ok(());
            }
        }
        self.write_message(MessageEncoder::encode_ready_for_query(
            session.transaction_status,
        ))
        .await
    }

    async fn finish_copy_to(&mut self, session: &mut Session, table: &str) -> std::io::Result<()> {
        let sql = format!("SELECT * FROM {table}");
        let result = {
            let engine = self.executor.as_mut().expect("executor initialized");
            engine.execute_session_aware(&sql)
        };
        // `rows_to_stream` consumes the `QueryResult`, so non-row outcomes
        // are normalized to an empty row set first (same empty COPY as the
        // previous `Vec::new()` path).
        let rebuilt = match result {
            Ok(plomid_sql::QueryResult::Rows { rows, .. }) => plomid_sql::QueryResult::Rows {
                columns: Vec::new(),
                column_types: Vec::new(),
                rows,
            },
            Ok(_) => plomid_sql::QueryResult::Rows {
                columns: Vec::new(),
                column_types: Vec::new(),
                rows: Vec::new(),
            },
            Err(error) => {
                self.send_copy_error(session, error).await?;
                return Ok(());
            }
        };
        self.write_message(MessageEncoder::encode_copy_out_response())
            .await?;
        // Bounded batches through the result-stream bridge: identical encode
        // calls in identical order; only the per-iteration encode quantum is
        // bounded. (`Default` here is the bridge `StreamConfig`, inferred
        // from the call signature — no new dependency.)
        let Some((_, mut stream)) = rows_to_stream(rebuilt, Default::default()) else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "result stream rejected row result",
            ));
        };
        let mut delivered = 0usize;
        while let Some(batch) = stream
            .next_batch(WIRE_SEND_BATCH_SIZE)
            .map_err(|error| std::io::Error::other(error.to_string()))?
        {
            for row in &batch {
                let mut line = Vec::new();
                for (index, value) in row.iter().enumerate() {
                    if index != 0 {
                        line.push(b'\t');
                    }
                    copy_escape_value(value, &mut line);
                }
                line.push(b'\n');
                self.write_message_buffered(MessageEncoder::encode_copy_data(&line))
                    .await?;
            }
            delivered += batch.len();
        }
        self.write_message_buffered(MessageEncoder::encode_copy_done())
            .await?;
        self.write_message_buffered(MessageEncoder::encode_command_complete(&format!(
            "COPY {delivered}"
        )))
        .await?;
        self.write_message_buffered(MessageEncoder::encode_ready_for_query(
            session.transaction_status,
        ))
        .await?;
        self.writer.flush().await
    }

    /// Installs the directory `COPY ... FROM/TO <file>` paths are jailed to.
    /// Called by the server once per connection; tests set an isolated dir.
    pub(crate) fn set_copy_dir(&mut self, dir: PathBuf) {
        self.copy_dir = Some(dir);
    }

    /// Canonicalized COPY jail for this connection: the explicit `--copy-dir`
    /// when set, otherwise the connection's data directory. `None` denies
    /// every file COPY (fail closed).
    ///
    /// The canonical path is re-validated on every call: it must exist, be
    /// a real directory (never a symlink), and (Unix) open cleanly with
    /// `O_NOFOLLOW | O_DIRECTORY`. A jail root swapped for a link between
    /// connections fails closed here instead of redirecting the next COPY.
    fn copy_jail_root(&self) -> Option<PathBuf> {
        let dir = if let Some(dir) = self.copy_dir.as_ref() {
            dir.clone()
        } else if let Some((data_dir, _)) = self.database_context.as_ref() {
            data_dir.clone()
        } else {
            return None;
        };
        let canonical = std::fs::canonicalize(&dir).ok()?;
        let meta = std::fs::symlink_metadata(&canonical).ok()?;
        if !meta.is_dir() || meta.file_type().is_symlink() {
            return None;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            let nofollow: libc::c_int = libc::O_NOFOLLOW;
            let dir_flag: libc::c_int = libc::O_DIRECTORY;
            if std::fs::File::options()
                .read(true)
                .custom_flags(nofollow | dir_flag)
                .open(&canonical)
                .is_err()
            {
                return None;
            }
        }
        Some(canonical)
    }

    async fn finish_copy_file(
        &mut self,
        session: &mut Session,
        spec: CopyFileSpec,
    ) -> std::io::Result<()> {
        // File COPY touches the server filesystem: superuser-only, plus
        // the table privilege matching the direction. Checked before any
        // file is opened, with an audited denial.
        {
            let executor = self.executor.as_ref().expect("executor initialized");
            if let Err(error) = executor.check_file_copy(
                &session.user,
                &spec.table,
                spec.direction == CopyFileDirection::To,
            ) {
                self.send_copy_error(session, error).await?;
                return Ok(());
            }
        }
        // Filesystem metadata (jail canonicalization, path resolution,
        // file read/write below) blocks: run under the blocking policy so
        // a slow disk never pins the async worker (see `blocking`).
        let root = match blocking(|| self.copy_jail_root()) {
            Some(root) => root,
            None => {
                audit_copy_jail_denial(&session.user, &spec.table, &spec.direction);
                self.send_error_response(
                    sql_error_from_io(format!(
                        "COPY file path is not permitted: {}",
                        copy_display_name(&spec.path)
                    )),
                    session.transaction_status,
                )
                .await?;
                return Ok(());
            }
        };
        let resolved = match blocking(|| resolve_copy_path(&root, &spec.path)) {
            Ok(path) => path,
            Err(message) => {
                audit_copy_jail_denial(&session.user, &spec.table, &spec.direction);
                self.send_error_response(sql_error_from_io(message), session.transaction_status)
                    .await?;
                return Ok(());
            }
        };
        let copy_limit = self.enforcement.max_copy_buffer_bytes;
        let result = match spec.direction {
            CopyFileDirection::From => {
                let data = match blocking(|| read_pinned_file(&root, &resolved, copy_limit)) {
                    Ok(data) => data,
                    Err(error) => {
                        // Jail denials (planted link, swapped parent,
                        // directory, FIFO/device — see `is_copy_denial`)
                        // are filename-only denials, never filesystem
                        // errors: the message must not echo where a link
                        // pointed or which special file was probed.
                        #[cfg(unix)]
                        if is_copy_denial(&error) {
                            audit_copy_jail_denial(&session.user, &spec.table, &spec.direction);
                            self.send_error_response(
                                sql_error_from_io(format!(
                                    "COPY file path is not permitted: {}",
                                    copy_display_name(&spec.path)
                                )),
                                session.transaction_status,
                            )
                            .await?;
                            return Ok(());
                        }
                        #[cfg(not(unix))]
                        if is_copy_denial(&error) {
                            audit_copy_jail_denial(&session.user, &spec.table, &spec.direction);
                            self.send_error_response(
                                sql_error_from_io(format!(
                                    "COPY file path is not permitted: {}",
                                    copy_display_name(&spec.path)
                                )),
                                session.transaction_status,
                            )
                            .await?;
                            return Ok(());
                        }
                        self.send_error_response(
                            sql_error_from_io(format!(
                                "could not read COPY source file {}: {error}",
                                copy_display_name(&spec.path)
                            )),
                            session.transaction_status,
                        )
                        .await?;
                        return Ok(());
                    }
                };
                let rows = parse_copy_file_rows(&data, &spec)
                    .map_err(|message| sql_error_from_io(format!("invalid COPY input: {message}")));
                match rows {
                    Ok(rows) => {
                        let count = rows.len();
                        if count == 0 {
                            Ok(count)
                        } else {
                            let values = rows
                                .into_iter()
                                .map(|row| format!("({})", row.join(", ")))
                                .collect::<Vec<_>>()
                                .join(", ");
                            let sql = format!(
                                "INSERT INTO {}{} VALUES {values}",
                                spec.table,
                                spec.columns.clone().unwrap_or_default()
                            );
                            let engine = self.executor.as_mut().expect("executor initialized");
                            match engine.execute_session_aware(&sql) {
                                Ok(plomid_sql::QueryResult::Inserted(_)) => Ok(count),
                                Ok(_) => Ok(0),
                                Err(error) => Err(error),
                            }
                        }
                    }
                    Err(error) => Err(error),
                }
            }
            CopyFileDirection::To => {
                let sql = format!("SELECT * FROM {}", spec.table);
                let result = {
                    let engine = self.executor.as_mut().expect("executor initialized");
                    engine.execute_session_aware(&sql)
                };
                let rows = match result {
                    Ok(plomid_sql::QueryResult::Rows { rows, .. }) => rows,
                    Ok(_) => Vec::new(),
                    Err(error) => {
                        self.send_copy_error(session, error).await?;
                        return Ok(());
                    }
                };
                let mut output = Vec::new();
                for row in &rows {
                    copy_encode_file_row(row, &mut output, spec.format, spec.delimiter);
                }
                if let Err(error) = blocking(|| write_pinned_file(&root, &resolved, &output)) {
                    #[cfg(unix)]
                    if is_copy_denial(&error) {
                        audit_copy_jail_denial(&session.user, &spec.table, &spec.direction);
                        self.send_error_response(
                            sql_error_from_io(format!(
                                "COPY file path is not permitted: {}",
                                copy_display_name(&spec.path)
                            )),
                            session.transaction_status,
                        )
                        .await?;
                        return Ok(());
                    }
                    #[cfg(not(unix))]
                    if is_copy_denial(&error) {
                        audit_copy_jail_denial(&session.user, &spec.table, &spec.direction);
                        self.send_error_response(
                            sql_error_from_io(format!(
                                "COPY file path is not permitted: {}",
                                copy_display_name(&spec.path)
                            )),
                            session.transaction_status,
                        )
                        .await?;
                        return Ok(());
                    }
                    self.send_error_response(
                        sql_error_from_io(format!(
                            "could not write COPY destination file {}: {error}",
                            copy_display_name(&spec.path)
                        )),
                        session.transaction_status,
                    )
                    .await?;
                    return Ok(());
                }
                Ok(rows.len())
            }
        };
        match result {
            Ok(count) => {
                self.write_message(MessageEncoder::encode_command_complete(&format!(
                    "COPY {count}"
                )))
                .await?;
                self.write_message(MessageEncoder::encode_ready_for_query(
                    session.transaction_status,
                ))
                .await?;
            }
            Err(error) => {
                self.send_copy_error(session, error).await?;
            }
        }
        Ok(())
    }

    /// Snapshot of the credential state the catalog will present for `user`.
    ///
    /// Captured before the exchange so a post-crash recurrence shows whether
    /// the role row and its verifier were reloaded, independent of whether
    /// the supplied password matched.
    fn auth_subject_state(&self, user: &str) -> (bool, bool, bool) {
        let executor = self.executor.as_ref().expect("executor initialized");
        (
            executor.has_role(user),
            executor.auth_credential(user).is_some(),
            executor.legacy_password(user).is_some(),
        )
    }

    /// Emits one structured diagnostics line per authentication decision.
    ///
    /// `total_ms` covers the whole connection setup through the decision;
    /// `executor_ms` isolates catalog/statistics load; `auth_ms` isolates the
    /// credential exchange. This lets F-007 recurrence evidence distinguish
    /// a slow catalog open from a real credential/lockout failure.
    fn log_auth_diagnostics(&self, diag: &AuthDiagnostics, decision: &str, reason: &str) {
        tracing::info!(
            target: "security",
            event = "auth_diagnostics",
            connection_id = self.connection_id,
            peer = ?self.peer_ip,
            method = %diag.method,
            decision = %decision,
            reason = %reason,
            total_ms = diag.total_ms,
            executor_ms = diag.executor_ms,
            auth_ms = diag.auth_ms,
            rate_limit_failures = diag.rate_limit_failures,
            rate_limit_delay_ms = diag.rate_limit_delay_ms,
            locked_out = diag.locked_out,
            lockout_triggered = diag.lockout_triggered,
            role_present = diag.role_present,
            has_verifier = diag.has_verifier,
            has_legacy_password = diag.has_legacy_password,
            bootstrap_name = diag.bootstrap_name,
            "security_auth_diagnostics"
        );
    }

    /// Runs the cleartext-password authentication exchange.
    ///
    /// Verification order: catalog verifier (all roles, SCRAM-derived) →
    /// legacy plaintext (one-shot upgrade to verifier on success) →
    /// first-run bootstrap (server secret binds to the configured name and
    /// promotes it to superuser). Every comparison is constant-time.
    /// Returns `true` when the client authenticated (continue to the session),
    /// `false` when authentication failed or the connection closed (the
    /// caller must stop; a FATAL was already sent on credential failure).
    async fn authenticate_cleartext(&mut self, user: &str) -> std::io::Result<bool> {
        self.write_message(MessageEncoder::encode_authentication(
            AuthMechanism::CleartextPassword,
        ))
        .await?;
        let password = match self.read_password_message().await? {
            Some(password) => password,
            None => return Ok(false),
        };
        if !self.verify_password(user, &password)? {
            self.send_error_response_fatal(
                "28P01",
                format!("password authentication failed for user \"{user}\""),
            )
            .await?;
            return Ok(false);
        }
        self.write_message(MessageEncoder::encode_authentication(AuthMechanism::Ok))
            .await?;
        Ok(true)
    }

    /// Verifies a cleartext candidate without leaking which store matched.
    /// Response timing is dominated by PBKDF2 in every branch that can
    /// succeed, and failures share one message (no user enumeration).
    fn verify_password(&mut self, user: &str, password: &str) -> std::io::Result<bool> {
        use plomid_security::{constant_time_eq, random_salt, RoleCredential};
        let executor = self.executor.as_mut().expect("executor initialized");
        if let Some(credential) = executor.auth_credential(user) {
            return Ok(credential.verify(password.as_bytes()));
        }
        if let Some(legacy) = executor.legacy_password(user) {
            if constant_time_eq(password.as_bytes(), legacy.as_bytes()) {
                let upgraded =
                    RoleCredential::derive(password.as_bytes(), random_salt().map_err(salt_io)?);
                executor
                    .store_role_credential(user, &upgraded)
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
                return Ok(true);
            }
            return Ok(false);
        }
        // First-run bootstrap: the configured server secret binds to the
        // configured name exactly once (no verifier stored yet).
        if user == self.auth_config.username
            && constant_time_eq(password.as_bytes(), self.auth_config.password.as_bytes())
        {
            let bound =
                RoleCredential::derive(password.as_bytes(), random_salt().map_err(salt_io)?);
            executor
                .bootstrap_superuser(user, &bound)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Runs the MD5-password authentication exchange.
    ///
    /// Method boundary (documented): MD5 needs a server-side plaintext
    /// secret, so it serves only the bootstrap identity and legacy
    /// plaintext roles (upgraded to verifiers on success). Roles with
    /// stored verifiers must use SCRAM or cleartext-password.
    async fn authenticate_md5(&mut self, user: &str) -> std::io::Result<bool> {
        // Per-exchange CSPRNG salt (replaces the historical pid-derived
        // salt): MD5 remains legacy-only, but its challenge is fresh.
        let salt_bytes = plomid_security::random_salt().map_err(salt_io)?;
        let salt = [salt_bytes[0], salt_bytes[1], salt_bytes[2], salt_bytes[3]];
        self.write_message(MessageEncoder::encode_authentication_md5(salt))
            .await?;
        let response = match self.read_password_message().await? {
            Some(response) => response,
            None => return Ok(false),
        };
        if !self.verify_md5(user, &response, salt)? {
            self.send_error_response_fatal(
                "28P01",
                format!("password authentication failed for user \"{user}\""),
            )
            .await?;
            return Ok(false);
        }
        // Legacy-use observability: every MD5 success is counted and
        // logged loud (deprecation pressure + compromise detection).
        plomid_core::observability::record_legacy_auth_use();
        tracing::warn!(
            target: "security",
            user = %user,
            "legacy MD5 login accepted; migrate the client to SCRAM"
        );
        self.write_message(MessageEncoder::encode_authentication(AuthMechanism::Ok))
            .await?;
        Ok(true)
    }

    fn verify_md5(&mut self, user: &str, response: &str, salt: [u8; 4]) -> std::io::Result<bool> {
        use plomid_security::{constant_time_eq, random_salt, RoleCredential};
        // Verifier-held roles cannot do MD5 (no server-side plaintext):
        // direct them to SCRAM/password instead of failing opaquely.
        {
            let executor = self.executor.as_ref().expect("executor initialized");
            if executor.auth_credential(user).is_some() {
                return Ok(false);
            }
        }
        let secret = {
            let executor = self.executor.as_ref().expect("executor initialized");
            if let Some(legacy) = executor.legacy_password(user) {
                Some((legacy, false))
            } else if user == self.auth_config.username {
                // Bootstrap only when nothing is stored for the name.
                if executor.has_role(user)
                    && (executor.auth_credential(user).is_some()
                        || executor.legacy_password(user).is_some())
                {
                    None
                } else {
                    Some((self.auth_config.password.clone(), true))
                }
            } else {
                None
            }
        };
        let Some((secret, is_bootstrap)) = secret else {
            return Ok(false);
        };
        let expected = crate::session::md5_password_response(&secret, user, salt);
        if !constant_time_eq(response.as_bytes(), expected.as_bytes()) {
            return Ok(false);
        }
        // Success upgrades legacy/bootstrap secrets to verifiers (MD5
        // itself stays legacy; the stored form moves forward).
        let executor = self.executor.as_mut().expect("executor initialized");
        let bound = RoleCredential::derive(secret.as_bytes(), random_salt().map_err(salt_io)?);
        if is_bootstrap {
            executor
                .bootstrap_superuser(user, &bound)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        } else {
            executor
                .store_role_credential(user, &bound)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        }
        Ok(true)
    }
    /// Runs a SCRAM-SHA-256 authentication exchange.
    ///
    /// Credential source, in order: the role's stored catalog verifier
    /// (all roles) → first-run bootstrap (server secret, binds once) →
    /// dummy exchange against random material for unknown roles (identical
    /// wire shape, always fails: no user enumeration via SASL behavior).
    /// Flow: advertise SCRAM-SHA-256, receive the client-first message, send
    /// the server-first message, receive the client-final message, verify the
    /// proof, and send the server-final verifier plus AuthenticationOk.
    async fn authenticate_scram(&mut self, user: &str) -> std::io::Result<bool> {
        enum Source {
            Stored,
            Bootstrap,
            Dummy,
        }
        let (mut scram, source) = {
            let executor = self.executor.as_ref().expect("executor initialized");
            if let Some(credential) = executor.auth_credential(user) {
                (
                    crate::scram::ScramSession::with_stored_verifier(
                        credential.salt,
                        credential.iterations,
                        &credential.verifier,
                    ),
                    Source::Stored,
                )
            } else if user == self.auth_config.username && executor.legacy_password(user).is_none()
            {
                // Bootstrap only when nothing is stored for the name (a
                // rotated-away server secret must not linger as a backdoor).
                (
                    crate::scram::ScramSession::with_verifier(
                        &self.auth_config.password,
                        self.auth_config.scram_salt,
                        &self.auth_config.scram_salted_password,
                    ),
                    Source::Bootstrap,
                )
            } else {
                let salt = plomid_security::random_salt().map_err(salt_io)?;
                let mut verifier = [0u8; 32];
                verifier.copy_from_slice(&plomid_security::sha256(&salt));
                (
                    crate::scram::ScramSession::with_stored_verifier(salt, 4096, &verifier),
                    Source::Dummy,
                )
            }
        };

        self.write_message(MessageEncoder::encode_authentication_sasl(&[
            "SCRAM-SHA-256",
        ]))
        .await?;

        // SASLInitialResponse arrives on the wire as a PasswordMessage whose
        // payload is mechanism + \0 + client-first.
        let initial_raw = match self.read_message().await? {
            Some((FrontendTag::Password, payload)) => payload,
            Some(_) => {
                self.send_error_response_fatal("08P01", "expected SASL initial response")
                    .await?;
                return Ok(false);
            }
            None => return Ok(false),
        };
        let client_first = parse_sasl_initial(&initial_raw);
        if client_first.is_empty() {
            self.send_error_response_fatal("08P01", "malformed SASL initial response")
                .await?;
            return Ok(false);
        }

        let server_first = match scram.handle_initial(&client_first) {
            Ok(server_first) => server_first,
            Err(scram_error) => {
                // Salt failure is a server-side outage, not a client
                // protocol error: report it distinctly, still fatal.
                let message = match scram_error {
                    crate::scram::ScramError::SaltUnavailable => {
                        "SASL unavailable: no secure randomness"
                    }
                    _ => "malformed SCRAM client-first message",
                };
                self.send_error_response_fatal("08P01", message).await?;
                return Ok(false);
            }
        };
        self.write_message(MessageEncoder::encode_authentication_sasl_continue(
            &server_first,
        ))
        .await?;

        // SASLResponse arrives as a PasswordMessage carrying only the
        // client-final message text.
        let final_raw = match self.read_message().await? {
            Some((FrontendTag::Password, payload)) => payload,
            Some(_) => {
                self.send_error_response_fatal("08P01", "expected SASL response")
                    .await?;
                return Ok(false);
            }
            None => return Ok(false),
        };
        let client_final = match String::from_utf8(final_raw) {
            Ok(client_final) => client_final.trim_end_matches('\0').to_owned(),
            Err(_) => {
                self.send_error_response_fatal("08P01", "invalid SASL response encoding")
                    .await?;
                return Ok(false);
            }
        };

        let verified = scram.handle_final(&client_final);
        // The dummy exchange always lands here (random verifier), exactly
        // like a wrong password: one message, no oracle.
        let server_final = match (&source, verified) {
            (_, Err(_)) | (Source::Dummy, _) => {
                self.send_error_response_fatal(
                    "28P01",
                    format!("password authentication failed for user \"{user}\""),
                )
                .await?;
                return Ok(false);
            }
            (_, Ok(server_final)) => server_final,
        };
        if matches!(source, Source::Bootstrap) {
            // First-run binding: the server secret becomes this role's
            // credential and the role becomes superuser, exactly once.
            let credential = plomid_security::RoleCredential::derive(
                self.auth_config.password.as_bytes(),
                plomid_security::random_salt().map_err(salt_io)?,
            );
            let executor = self.executor.as_mut().expect("executor initialized");
            if executor.bootstrap_superuser(user, &credential).is_err() {
                self.send_error_response_fatal("XX000", "authentication store unavailable")
                    .await?;
                return Ok(false);
            }
        }
        self.write_message(MessageEncoder::encode_authentication_sasl_final(
            &server_final,
        ))
        .await?;
        self.write_message(MessageEncoder::encode_authentication(AuthMechanism::Ok))
            .await?;
        Ok(true)
    }

    async fn handle_parse(&mut self, payload: &[u8]) -> std::io::Result<()> {
        let (name, remainder) = read_cstring(payload, "statement name")?;
        let (query, remainder) = read_cstring(remainder, "query")?;
        // PostgreSQL extended-protocol contract: one Parse message carries
        // exactly one SQL statement. A body with several top-level statements
        // must fail explicitly (PostgreSQL: 42601 "cannot insert multiple
        // commands into a prepared statement"). Executing just the leading
        // statement would silently drop the remainder of the batch —
        // data-loss shaped — so reject before storing anything. The boundary
        // check uses the real SQL lexer, the same tokenizer the parser runs
        // on, so quoting/comments can never desynchronize the count from
        // what execution would see. Unlexable bodies keep today's flow: the
        // syntax error surfaces at Bind/Execute/Describe as before.
        if extended_parse_statement_count(&query).is_some_and(|count| count > 1) {
            self.extended_error = true;
            let err = SqlError::Syntax(PlomidError::new(
                ErrorKind::Syntax,
                "cannot insert multiple commands into a prepared statement",
            ));
            let response = executor_error_response_with_datadir(
                err,
                self.database_context.as_ref().map(|(dir, _)| dir.as_path()),
            );
            self.write_message_buffered(MessageEncoder::encode_error_response(&response))
                .await?;
            self.writer.flush().await?;
            return Ok(());
        }
        if remainder.len() < 2 {
            return Err(protocol_error("truncated Parse parameter types"));
        }
        let count = u16::from_be_bytes([remainder[0], remainder[1]]) as usize;
        let bytes = remainder
            .get(2..)
            .ok_or_else(|| protocol_error("truncated Parse parameter types"))?;
        if bytes.len() < count * 4 {
            return Err(protocol_error("truncated Parse parameter types"));
        }
        let parameter_types = (0..count)
            .map(|index| {
                u32::from_be_bytes([
                    bytes[index * 4],
                    bytes[index * 4 + 1],
                    bytes[index * 4 + 2],
                    bytes[index * 4 + 3],
                ])
            })
            .collect();
        // Fail closed before allocating per-connection state without bound:
        // distinct Parse names accumulate until explicit Close.
        if !self.prepared_statements.contains_key(&name)
            && self.prepared_statements.len() >= self.enforcement.max_prepared_statements
        {
            return Err(protocol_error("too many prepared statements"));
        }
        self.prepared_statements.insert(
            name,
            PreparedStatement {
                query,
                parameter_types,
                describe_shape: None,
            },
        );
        self.extended_description_sent = false;
        self.write_message(MessageEncoder::encode_parse_complete())
            .await
    }

    async fn handle_bind(&mut self, payload: &[u8]) -> std::io::Result<()> {
        let (portal_name, rest) = read_cstring(payload, "portal name")?;
        let (statement_name, mut rest) = read_cstring(rest, "statement name")?;
        let statement = self
            .prepared_statements
            .get(&statement_name)
            .ok_or_else(|| protocol_error("prepared statement does not exist"))?
            .clone();
        if rest.len() < 2 {
            return Err(protocol_error("truncated Bind message"));
        }
        let format_count = u16::from_be_bytes([rest[0], rest[1]]) as usize;
        let parameter_formats = rest[2..]
            .get(..format_count * 2)
            .ok_or_else(|| protocol_error("truncated Bind formats"))?
            .chunks_exact(2)
            .map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]))
            .collect::<Vec<_>>();
        rest = rest
            .get(2 + format_count * 2..)
            .ok_or_else(|| protocol_error("truncated Bind formats"))?;
        if rest.len() < 2 {
            return Err(protocol_error("truncated Bind parameters"));
        }
        let count = u16::from_be_bytes([rest[0], rest[1]]) as usize;
        rest = &rest[2..];
        let mut parameters = Vec::new();
        for _ in 0..count {
            if rest.len() < 4 {
                return Err(protocol_error("truncated Bind value"));
            }
            let length = i32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
            rest = &rest[4..];
            if length < 0 {
                parameters.push(None);
            } else {
                let length = length as usize;
                if rest.len() < length {
                    return Err(protocol_error("truncated Bind value"));
                }
                let format = parameter_formats
                    .get(if parameter_formats.len() == 1 {
                        0
                    } else {
                        parameters.len()
                    })
                    .copied()
                    .unwrap_or(0);
                let oid = statement
                    .parameter_types
                    .get(parameters.len())
                    .copied()
                    .unwrap_or(0);
                parameters.push(Some(decode_parameter(&rest[..length], format, oid)?));
                rest = &rest[length..];
            }
        }
        if rest.len() < 2 {
            return Err(protocol_error("truncated Bind result formats"));
        }
        let result_count = u16::from_be_bytes([rest[0], rest[1]]) as usize;
        rest = &rest[2..];
        if rest.len() < result_count * 2 {
            return Err(protocol_error("truncated Bind result formats"));
        }
        let result_formats = (0..result_count)
            .map(|index| u16::from_be_bytes([rest[index * 2], rest[index * 2 + 1]]))
            .collect();
        // Fail closed before accumulating per-connection portal state.
        if !self.portals.contains_key(&portal_name)
            && self.portals.len() >= self.enforcement.max_portals
        {
            return Err(protocol_error("too many portals"));
        }
        self.portals.insert(
            portal_name,
            Portal {
                statement: statement_name,
                parameters,
                result_formats,
            },
        );
        self.write_message(MessageEncoder::encode_bind_complete())
            .await
    }

    async fn handle_describe(
        &mut self,
        payload: &[u8],
        session: &mut Session,
    ) -> std::io::Result<()> {
        let (kind, remainder) = payload
            .split_first()
            .ok_or_else(|| protocol_error("truncated Describe message"))?;
        let (name, _) = read_cstring(remainder, "Describe name")?;
        let (statement_name, statement, described_sql, result_formats) = if *kind == b'P' {
            let portal = self
                .portals
                .get(&name)
                .ok_or_else(|| protocol_error("portal does not exist"))?;
            let statement = self
                .prepared_statements
                .get(&portal.statement)
                .ok_or_else(|| protocol_error("prepared statement does not exist"))?
                .clone();
            // Describe a portal with NULL placeholders, never the real bound
            // parameter values: executing the bound statement here would be a
            // side-effecting double-execute for DML and could raise a spurious
            // error (e.g. an invalid array cast) that PostgreSQL would not
            // surface until portal execution. NULL keeps Describe read-only.
            // Sized by the query's actual `$N` references, not by the declared
            // parameter types: clients routinely declare zero types and supply
            // values only at Bind, and sizing by declarations left raw `$1`
            // tokens in the described SQL.
            let placeholders =
                vec![None; max_placeholder(&statement.query).max(statement.parameter_types.len())];
            (
                portal.statement.clone(),
                statement.clone(),
                bind_query(&statement.query, &placeholders),
                portal.result_formats.clone(),
            )
        } else {
            let statement = self
                .prepared_statements
                .get(&name)
                .ok_or_else(|| protocol_error("prepared statement does not exist"))?
                .clone();
            let placeholders =
                vec![None; max_placeholder(&statement.query).max(statement.parameter_types.len())];
            (
                name.clone(),
                statement.clone(),
                bind_query(&statement.query, &placeholders),
                Vec::new(),
            )
        };
        if statement_type(&described_sql) == "SET" {
            if *kind == b'S' {
                self.write_message(MessageEncoder::encode_parameter_description(
                    &statement.parameter_types,
                ))
                .await?;
            }
            return self.write_message(MessageEncoder::encode_no_data()).await;
        }
        if *kind == b'S' {
            self.write_message(MessageEncoder::encode_parameter_description(
                &statement.parameter_types,
            ))
            .await?;
        }
        // Describe must NEVER side-effect. Only read-only result-producing
        // statements are executed (with NULL placeholders) so the client can
        // learn the result-column shape; every other statement type is
        // described with an empty result set.
        if !is_describe_query(statement_type(&described_sql)) {
            return self.write_message(MessageEncoder::encode_no_data()).await;
        }
        let engine = self.executor.as_mut().expect("executor initialized");
        let cancel = Arc::clone(&self.cancel_flag);
        let search_path = session
            .get_parameter("search_path")
            .unwrap_or_default()
            .to_string();
        let database = session.database.clone();
        let cached_shape = statement
            .describe_shape
            .as_ref()
            .filter(|shape| shape.database == database && shape.search_path == search_path)
            .cloned();
        let describe_kind = statement_type(&described_sql);
        let describe_path = parse_search_path(session.get_parameter("search_path"));
        let cache_database = database.clone();
        let cache_search_path = search_path.clone();
        let outcome = Self::exec_guarded(&cancel, session, move || {
            let catalog_generation = engine.refresh_catalog_generation()?;
            if let Some(cached) = cached_shape {
                if cached.catalog_generation == catalog_generation {
                    return Ok(DescribeShapeOutcome::Cached(cached));
                }
            }
            let result = describe_shape(engine, &described_sql, describe_kind, &describe_path)?;
            Ok(DescribeShapeOutcome::Probed {
                result,
                catalog_generation,
            })
        });
        match outcome {
            Ok(DescribeShapeOutcome::Cached(shape)) => {
                self.write_message(MessageEncoder::encode_row_description(&row_descriptions(
                    &shape.columns,
                    &shape.column_types,
                    &result_formats,
                )))
                .await?;
                self.extended_description_sent = true;
                Ok(())
            }
            Ok(DescribeShapeOutcome::Probed {
                result:
                    plomid_sql::QueryResult::Rows {
                        columns,
                        column_types,
                        rows,
                    },
                catalog_generation,
            }) => {
                let column_types =
                    infer_describe_column_types(column_types, &rows, &statement.query);
                if let Some(entry) = self.prepared_statements.get_mut(&statement_name) {
                    entry.describe_shape = Some(CachedDescribeShape {
                        columns: columns.clone(),
                        column_types: column_types.clone(),
                        catalog_generation,
                        database: cache_database,
                        search_path: cache_search_path,
                    });
                }
                self.write_message(MessageEncoder::encode_row_description(&row_descriptions(
                    &columns,
                    &column_types,
                    &result_formats,
                )))
                .await?;
                self.extended_description_sent = true;
                Ok(())
            }
            Ok(DescribeShapeOutcome::Probed { .. }) => {
                self.write_message(MessageEncoder::encode_no_data()).await
            }
            Err(err) => {
                // Guard aborts (timeout/cancel) surface with SQLSTATE 57014.
                // Extended-protocol errors never carry ReadyForQuery (see
                // above); cancel additionally drops the connection.
                if plomid_executor::guard::is_guard_error(&err) {
                    if session.is_in_transaction() {
                        session.mark_failed_transaction();
                    }
                    self.extended_description_sent = false;
                    let drop_conn = self
                        .send_guard_error(err, session.transaction_status, false)
                        .await?;
                    if drop_conn {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "cancelled",
                        ));
                    }
                    return Ok(());
                }
                // A failing SELECT still aborts the surrounding transaction,
                // and the client must see the true state (InFailed, not Idle)
                // or it will skip recovery and leave the session stuck.
                if session.is_in_transaction() {
                    session.mark_failed_transaction();
                }
                // Extended-protocol errors must NOT carry ReadyForQuery: the
                // client recovers at the next Sync, which sends Ready itself.
                // Emitting Ready here desynchronizes Parse/Bind/Describe/
                // Execute pipelines (the client reads Ready as the end of the
                // whole sequence while its Sync is still outstanding).
                let response = executor_error_response_with_datadir(
                    err,
                    self.database_context.as_ref().map(|(dir, _)| dir.as_path()),
                );
                self.extended_description_sent = false;
                self.write_message(MessageEncoder::encode_error_response(&response))
                    .await
            }
        }
    }

    async fn handle_query(
        &mut self,
        session: &mut Session,
        sql: &str,
        ready: bool,
        query_id: u64,
    ) -> std::io::Result<()> {
        let started = std::time::Instant::now();
        let statement = statement_type(sql);
        // A cancel that arrives while no query runs is a no-op (PostgreSQL
        // reports "no query running"); only in-flight queries abort. Clear
        // any stale flag so the next query starts clean.
        self.cancel_flag.store(false, Ordering::Relaxed);
        tracing::debug!(target: "server::protocol", event = "query_started", query_id, connection_id = session.id, statement_type = statement, sql_len = sql.len());
        // Multi-statement strings run every part in order, each with its own
        // response (PostgreSQL emits CommandComplete per statement). The
        // first-word dispatch below classifies only a single statement: never
        // let a leading BEGIN/COMMIT/ROLLBACK (or any other opener) swallow
        // the rest of the batch — that silently drops writes. Single
        // statements skip this untouched. ReadyForQuery belongs to the
        // synchronization owner: simple Query (`ready`) takes it here,
        // extended Execute leaves it to Sync.
        if sql.contains(';') {
            let parts = split_simple_protocol_statements(sql);
            if parts.len() > 1 {
                for part in parts {
                    Box::pin(self.handle_query(session, &part, false, query_id)).await?;
                    self.writer.flush().await?;
                }
                if ready {
                    self.write_message(MessageEncoder::encode_ready_for_query(
                        session.transaction_status,
                    ))
                    .await?;
                    self.writer.flush().await?;
                }
                return Ok(());
            }
        }
        if self.database_context.is_some() {
            if let Some(ddl) = parse_database_ddl(sql) {
                return self
                    .handle_database_ddl(ddl, session, ready, query_id, started)
                    .await;
            }
            if statement == "USE" {
                if let Some(name) = parse_use_database(sql) {
                    return self
                        .handle_database_switch(&name, session, ready, query_id, started)
                        .await;
                }
            }
            if statement == "SHOW" && is_show_databases(sql) {
                return self
                    .handle_show_databases(session, ready, query_id, started)
                    .await;
            }
        }
        if statement == "DEALLOCATE" || statement == "DISCARD" || statement == "UNLISTEN" {
            // Housekeeping statements: acknowledged without engine round-trip
            // so client-side cleanup never aborts a transaction.
            let tag = match statement {
                "DISCARD" => "DISCARD ALL",
                other => other,
            };
            return self
                .send_query_result(
                    session,
                    plomid_sql::QueryResult::Created(tag.to_string()),
                    ready,
                )
                .await;
        }
        if statement == "LISTEN" || statement == "NOTIFY" {
            return self
                .send_query_result(
                    session,
                    plomid_sql::QueryResult::Created(statement.to_string()),
                    ready,
                )
                .await;
        }
        // COMMENT/ANALYZE/REINDEX/LOCK/CLUSTER/REFRESH are deliberately not
        // short-circuited here: the executor implements every one of them
        // (analyze collects real statistics; reindex drops cached ANN graphs
        // and revalidates vector index metadata; comment/lock/refresh/
        // cluster are engine-side compatibility acknowledgments). A
        // fabricated wire-level CommandComplete would make the network
        // contract diverge from the engine (F-002/F-003).
        // SAVEPOINT operations act on the open session transaction's
        // overlay/catalog images (true savepoints, not text markers).
        // Outside a transaction they fail like PostgreSQL.
        if statement == "SAVEPOINT"
            || statement == "RELEASE"
            || statement == "ROLLBACK_TO_SAVEPOINT"
        {
            let Some(name) = savepoint_name(statement, sql) else {
                let err = SqlError::Syntax(PlomidError::new(
                    ErrorKind::InvalidArgument,
                    format!("syntax error: {statement} requires a savepoint name"),
                ));
                self.log_query_failure(query_id, session.id, sql, &err, started);
                self.send_query_error(err, session.transaction_status, ready)
                    .await?;
                return Ok(());
            };
            let engine = self.executor.as_mut().expect("executor initialized");
            let outcome = match statement {
                "SAVEPOINT" => blocking(|| engine.savepoint_session_txn(&name)),
                "RELEASE" => blocking(|| engine.release_session_savepoint(&name)),
                "ROLLBACK_TO_SAVEPOINT" => blocking(|| engine.rollback_to_session_savepoint(&name)),
                _ => Ok(()),
            };
            match outcome {
                Ok(()) => {
                    let tag = match statement {
                        "SAVEPOINT" => "SAVEPOINT",
                        "RELEASE" => "RELEASE",
                        "ROLLBACK_TO_SAVEPOINT" => "ROLLBACK TO SAVEPOINT",
                        other => other,
                    };
                    return self
                        .send_query_result(
                            session,
                            plomid_sql::QueryResult::Created(tag.to_string()),
                            ready,
                        )
                        .await;
                }
                Err(err) => {
                    // Savepoint-operation errors abort the session
                    // transaction like any other statement error
                    // (PostgreSQL parity), unless there is nothing open.
                    if session.is_in_transaction() {
                        let engine = self.executor.as_mut().expect("executor initialized");
                        blocking(|| engine.abort_session_txn_on_error());
                        session.mark_failed_transaction();
                    }
                    self.log_query_failure(query_id, session.id, sql, &err, started);
                    self.send_query_error(err, session.transaction_status, ready)
                        .await?;
                    return Ok(());
                }
            }
        }
        if statement == "BEGIN" || statement == "COMMIT" || statement == "ROLLBACK" {
            return self
                .handle_session_transaction(session, sql, ready, query_id, started)
                .await;
        }
        if statement == "SET" || statement == "SHOW" {
            // A single simple-protocol Query string may batch several
            // statements (`SET ...; SELECT ...`). Split it at top-level
            // semicolons (ignoring those inside quotes/parens) so each
            // statement is classified, executed, and answered separately.
            // Otherwise SET would swallow the rest of the batch and SHOW
            // would return the SET text as its value.
            let parts = split_simple_protocol_statements(sql);
            if parts.len() > 1 {
                for part in parts {
                    Box::pin(self.handle_query(session, &part, false, query_id)).await?;
                }
                // ReadyForQuery is the caller's synchronization point: the
                // simple-protocol Query owns it (`ready`), while an extended
                // Execute must stay silent here so the client's Sync remains
                // the single boundary. An unsolicited Z mid-pipeline
                // desynchronizes strict clients.
                if ready {
                    self.write_message(MessageEncoder::encode_ready_for_query(
                        session.transaction_status,
                    ))
                    .await?;
                    self.writer.flush().await?;
                }
                return Ok(());
            }
        }
        if statement == "SHOW" {
            if let Some(name) = show_parameter_name(sql) {
                // Registered vector knobs resolve through the settings
                // registry (override if SET, else the documented default) —
                // never the empty session fallback below.
                if plomid_executor::vector_settings::definition(&name).is_some() {
                    let value = plomid_executor::vector_settings::get(&name).unwrap_or_default();
                    return self
                        .send_query_result(
                            session,
                            plomid_sql::QueryResult::Rows {
                                columns: vec![name],
                                column_types: vec![Some(plomid_sql::ColumnType::text())],
                                rows: vec![vec![plomid_sql::Value::Text(value)]],
                            },
                            ready,
                        )
                        .await;
                }
                let value = if name.eq_ignore_ascii_case("search_path") {
                    let engine = self.executor.as_mut().expect("executor initialized");
                    let search_path = parse_search_path(session.get_parameter("search_path"));
                    match blocking(|| {
                        engine.execute_all_with_search_path("SHOW search_path;", &search_path)
                    }) {
                        Ok((results, _)) => {
                            let value = results
                                .into_iter()
                                .find_map(|result| match result {
                                    plomid_sql::QueryResult::Rows { rows, .. } => rows
                                        .into_iter()
                                        .next()
                                        .and_then(|row| row.into_iter().next())
                                        .map(|value| match value {
                                            plomid_sql::Value::Text(text) => text,
                                            other => format!("{other:?}"),
                                        }),
                                    _ => None,
                                })
                                .unwrap_or_default();
                            return self
                                .send_query_result(
                                    session,
                                    plomid_sql::QueryResult::Rows {
                                        columns: vec![name],
                                        column_types: vec![Some(plomid_sql::ColumnType::text())],
                                        rows: vec![vec![plomid_sql::Value::Text(value)]],
                                    },
                                    ready,
                                )
                                .await;
                        }
                        Err(err) => {
                            self.log_query_failure(query_id, session.id, sql, &err, started);
                            self.send_query_error(err, session.transaction_status, ready)
                                .await?;
                            return Ok(());
                        }
                    }
                } else {
                    session
                        .get_parameter(&name)
                        .or_else(|| session.get_parameter(&name.to_ascii_lowercase()))
                        .unwrap_or("")
                        .to_string()
                };
                return self
                    .send_query_result(
                        session,
                        plomid_sql::QueryResult::Rows {
                            columns: vec![name],
                            column_types: vec![Some(plomid_sql::ColumnType::text())],
                            rows: vec![vec![plomid_sql::Value::Text(value)]],
                        },
                        ready,
                    )
                    .await;
            }
        }
        if statement == "SET" {
            if let Some((name, value)) = set_parameter(sql) {
                if name == "isolation_level" {
                    match parse_isolation_level(sql) {
                        Some(ParsedIsolation::Serializable) => {
                            let err = SqlError::Storage(PlomidError::new(
                                ErrorKind::InvalidArgument,
                                "SERIALIZABLE isolation is not supported",
                            ));
                            self.log_query_failure(query_id, session.id, sql, &err, started);
                            self.send_query_error(err, session.transaction_status, ready)
                                .await?;
                            return Ok(());
                        }
                        Some(ParsedIsolation::Invalid) => {
                            let err = SqlError::Storage(PlomidError::new(
                                ErrorKind::InvalidArgument,
                                "invalid isolation level",
                            ));
                            self.log_query_failure(query_id, session.id, sql, &err, started);
                            self.send_query_error(err, session.transaction_status, ready)
                                .await?;
                            return Ok(());
                        }
                        Some(level) => {
                            let level = match level {
                                ParsedIsolation::ReadCommitted => IsolationLevel::ReadCommitted,
                                ParsedIsolation::RepeatableRead => IsolationLevel::RepeatableRead,
                                ParsedIsolation::Serializable | ParsedIsolation::Invalid => {
                                    unreachable!("handled above")
                                }
                            };
                            match isolation_set_scope(sql) {
                                IsolationSetScope::SessionDefault => {
                                    session.default_isolation = level;
                                    session.set_parameter(name, value);
                                }
                                IsolationSetScope::CurrentTransaction => {
                                    let err = SqlError::Storage(PlomidError::new(
                                        ErrorKind::InvalidArgument,
                                        if session.transaction_status == TransactionStatus::Idle {
                                            "SET TRANSACTION can only be used in transaction blocks"
                                        } else {
                                            "isolation level cannot be changed mid-transaction"
                                        },
                                    ));
                                    self.log_query_failure(
                                        query_id, session.id, sql, &err, started,
                                    );
                                    self.send_query_error(err, session.transaction_status, ready)
                                        .await?;
                                    return Ok(());
                                }
                            }
                            return self
                                .send_query_result(session, plomid_sql::QueryResult::Set, ready)
                                .await;
                        }
                        None => {
                            session.set_parameter(name, value);
                        }
                    }
                } else if name.to_ascii_lowercase().starts_with("vector.") {
                    // Registered vector knobs validate through the settings
                    // registry (unknown names and out-of-range values fail);
                    // the registry is also what query execution reads, so a
                    // successful SET takes effect instead of echoing locally.
                    match plomid_executor::vector_settings::set(&name, &value) {
                        Ok(()) => {}
                        Err(error) => {
                            let err = SqlError::Storage(error);
                            self.log_query_failure(query_id, session.id, sql, &err, started);
                            self.send_query_error(err, session.transaction_status, ready)
                                .await?;
                            return Ok(());
                        }
                    }
                } else {
                    session.set_parameter(name, value);
                }
            }
            // PostgreSQL clients commonly send session-level SET commands
            // that PLOMID does not need to act on (for example SET ROLE,
            // SET SESSION AUTHORIZATION, or a client-specific timeout).
            // They must not fall through to SQL execution, where an
            // identifier value can be mistaken for a table name.
            return self
                .send_query_result(session, plomid_sql::QueryResult::Set, ready)
                .await;
        }
        if session.transaction_status == TransactionStatus::InFailedTransaction {
            let err = SqlError::Storage(PlomidError::new(
                ErrorKind::Conflict,
                "current transaction is aborted, commands ignored until ROLLBACK",
            ));
            self.log_query_failure(query_id, session.id, sql, &err, started);
            self.send_query_error(err, session.transaction_status, ready)
                .await?;
            return Ok(());
        }
        if session.is_in_transaction() {
            // True staging: every statement executes immediately into the
            // open session transaction (effects staged in the write
            // overlay, reads merge overlay + snapshot). Multi-statement
            // messages split into parts first — transaction control parts
            // route back through session ops, everything else runs here.
            // Results are real (counts, RETURNING, rows), computed once at
            // statement time; COMMIT only publishes.
            if sql.contains(';') {
                let parts = split_simple_protocol_statements(sql);
                if parts.len() > 1 {
                    for part in parts {
                        Box::pin(self.handle_query(session, &part, false, query_id)).await?;
                        self.writer.flush().await?;
                    }
                    // ReadyForQuery belongs to the synchronization owner (see
                    // the SET/SHOW split above): simple Query (`ready`) takes
                    // it here, extended Execute leaves it to Sync.
                    if ready {
                        self.write_message(MessageEncoder::encode_ready_for_query(
                            session.transaction_status,
                        ))
                        .await?;
                        self.writer.flush().await?;
                    }
                    return Ok(());
                }
            }
            let engine = self.executor.as_mut().expect("executor initialized");
            let cancel = Arc::clone(&self.cancel_flag);
            match Self::exec_guarded(&cancel, session, || {
                blocking(|| engine.execute_one_in_session_txn(sql))
            }) {
                Ok(result) => {
                    self.send_query_result(session, result, ready).await?;
                    return Ok(());
                }
                Err(err) => {
                    if plomid_executor::guard::is_guard_error(&err) {
                        session.mark_failed_transaction();
                        self.log_query_failure(query_id, session.id, sql, &err, started);
                        let drop_conn = self
                            .send_guard_error(err, session.transaction_status, ready)
                            .await?;
                        if drop_conn {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::Interrupted,
                                "cancelled",
                            ));
                        }
                        return Ok(());
                    }
                    // The executor already aborted the session transaction
                    // on error; mark the session failed (only ROLLBACK
                    // accepted afterwards, like PostgreSQL).
                    session.mark_failed_transaction();
                    self.log_query_failure(query_id, session.id, sql, &err, started);
                    self.send_query_error(err, session.transaction_status, ready)
                        .await?;
                    return Ok(());
                }
            }
        }
        // A simple-protocol Query string may carry several independent
        // top-level statements. Running them through `execute_all` collects
        // every statement's result first and only then writes any of them, so
        // the client sees nothing until the whole batch has finished (for a
        // 20-statement batch of single-row INSERTs that is one WAL fsync per
        // statement before the first CommandComplete). PostgreSQL emits
        // CommandComplete as each statement completes; do the same here.
        //
        // Splitting is applied only when it is provably equivalent to running
        // the batch as one call: the session must be idle (so every statement
        // is its own autocommit transaction in both paths) and the batch must
        // contain no transaction control (an explicit `BEGIN ... COMMIT` must
        // stay a single transaction with a single commit, so it keeps the
        // buffered path below).
        if session.transaction_status == TransactionStatus::Idle && sql.contains(';') {
            let parts = split_simple_protocol_statements(sql);
            if parts.len() > 1 && !parts.iter().any(|part| is_transaction_control(part)) {
                for part in parts {
                    Box::pin(self.handle_query(session, &part, false, query_id)).await?;
                    // The response path buffers; flush so this statement's
                    // result is actually on the wire before the next one runs.
                    self.writer.flush().await?;
                }
                // ReadyForQuery belongs to the synchronization owner (see the
                // SET/SHOW split above): simple Query (`ready`) takes it here,
                // extended Execute leaves it to Sync.
                if ready {
                    self.write_message(MessageEncoder::encode_ready_for_query(
                        session.transaction_status,
                    ))
                    .await?;
                    self.writer.flush().await?;
                }
                return Ok(());
            }
        }
        let engine = self.executor.as_mut().expect("executor initialized");
        let search_path = parse_search_path(session.get_parameter("search_path"));
        // Statement timeout + cancel (resource B6): enforced cooperatively —
        // the guard is installed here at the dispatch layer and polled by
        // the `join/*` probe loops. `statement_timeout=0` disables.
        let cancel = Arc::clone(&self.cancel_flag);
        let (results, changed) = match Self::exec_guarded(&cancel, session, || {
            engine.execute_all_with_search_path(sql, &search_path)
        }) {
            Ok(result) => result,
            Err(err) => {
                if plomid_executor::guard::is_guard_error(&err) {
                    if session.is_in_transaction() {
                        session.mark_failed_transaction();
                    }
                    self.log_query_failure(query_id, session.id, sql, &err, started);
                    let drop_conn = self
                        .send_guard_error(err, session.transaction_status, ready)
                        .await?;
                    if drop_conn {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "cancelled",
                        ));
                    }
                    return Ok(());
                }
                if session.is_in_transaction() {
                    session.mark_failed_transaction();
                }
                self.log_query_failure(query_id, session.id, sql, &err, started);
                self.send_query_error(err, session.transaction_status, ready)
                    .await?;
                return Ok(());
            }
        };
        let row_count = results
            .iter()
            .map(|result| match result {
                plomid_sql::QueryResult::Rows { rows, .. } => rows.len(),
                _ => 0,
            })
            .sum::<usize>();
        let result_set = results
            .iter()
            .any(|result| matches!(result, plomid_sql::QueryResult::Rows { .. }));
        tracing::debug!(
            target: "server::protocol",
            event = "query_completed",
            query_id,
            elapsed_ms = started.elapsed().as_millis() as u64,
            result_count = results.len(),
            row_count,
            row_description_sent = result_set,
            data_rows_sent = result_set && row_count > 0,
        );
        // Persist an in-band `SET search_path` into the wire session so later
        // queries on this connection keep resolving against the updated path.
        // `$user` is re-quoted to match PostgreSQL's `SHOW search_path` format.
        if let Some(next) = changed {
            let display = next
                .iter()
                .map(|schema| {
                    if schema == "$user" {
                        "\"$user\"".to_string()
                    } else {
                        schema.clone()
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            session.set_parameter("search_path", display);
        }
        let result_count = results.len();
        for (index, result) in results.into_iter().enumerate() {
            self.send_query_result(session, result, ready && index + 1 == result_count)
                .await?;
        }
        Ok(())
    }

    async fn handle_database_ddl(
        &mut self,
        ddl: DatabaseDdl,
        session: &mut Session,
        ready: bool,
        query_id: u64,
        started: std::time::Instant,
    ) -> std::io::Result<()> {
        let create = ddl.create;
        let name = ddl.name.as_str();
        let operation = if create {
            "CREATE DATABASE"
        } else {
            "DROP DATABASE"
        };
        // Database DDL cannot run inside an explicit transaction
        // (PostgreSQL parity): it would fork storage engines and the
        // shared registry outside any rollback scope.
        if session.is_in_transaction() {
            let error = SqlError::Storage(PlomidError::new(
                ErrorKind::Unsupported,
                "CREATE/DROP DATABASE cannot be executed inside a transaction block",
            ));
            return self
                .send_error_response(error, session.transaction_status)
                .await;
        }
        // Authorization BEFORE any mutation: superusers pass, everyone else
        // needs the CREATEDB role attribute (the same rule the executor's
        // `authorize_create` applies to `C::Database`). Denied → 42501 with
        // an audit event; the registry and filesystem are untouched.
        if let Some(executor) = self.executor.as_mut() {
            if let Err(error) = executor.check_database_ddl(&session.user, create, name) {
                self.log_query_failure(query_id, session.id, operation, &error, started);
                return self
                    .send_error_response(error, session.transaction_status)
                    .await;
            }
        }
        let Some((data_dir, registry)) = self.database_context.clone() else {
            return Ok(());
        };
        let mut registry = registry.lock().await;
        let result: Result<(), PlomidError> = if create {
            if registry
                .names
                .iter()
                .any(|registered| registered.eq_ignore_ascii_case(name))
            {
                if ddl.conditional {
                    // `IF NOT EXISTS`: already there is success with no effect.
                    Ok(())
                } else {
                    Err(PlomidError::new(
                        ErrorKind::AlreadyExists,
                        format!("database \"{name}\" already exists"),
                    ))
                }
            } else {
                let path = database_path(&data_dir, name);
                // Filesystem + engine creation block: run under the
                // blocking policy so a slow disk never pins the async
                // worker (see `blocking`).
                if let Err(error) = blocking(|| std::fs::create_dir_all(&path)) {
                    return self
                        .send_database_ddl_error(
                            session,
                            ready,
                            query_id,
                            sql_error_from_io(error.to_string()),
                            started,
                        )
                        .await;
                }
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt as _;
                    let _ = blocking(|| {
                        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                    });
                }
                if let Err(error) =
                    blocking(|| PlomidStorageEngine::create(&path, &path.join("wal"), 64))
                {
                    let _ = blocking(|| std::fs::remove_dir_all(&path));
                    return self
                        .send_database_ddl_error(
                            session,
                            ready,
                            query_id,
                            sql_error_from_io(error.to_string()),
                            started,
                        )
                        .await;
                }
                registry.names.insert(name.to_string());
                if let Err(error) = blocking(|| registry.persist(&data_dir)) {
                    registry.names.remove(name);
                    let _ = blocking(|| std::fs::remove_dir_all(&path));
                    Err(PlomidError::new(ErrorKind::Io, error.to_string()))
                } else {
                    Ok(())
                }
            }
        } else if name.eq_ignore_ascii_case("plomid") {
            Err(PlomidError::new(
                ErrorKind::PermissionDenied,
                "cannot drop the default database",
            ))
        } else if let Some(actual_name) = registry
            .names
            .iter()
            .find(|registered| registered.eq_ignore_ascii_case(name))
            .cloned()
        {
            registry.names.remove(&actual_name);
            if let Err(error) =
                blocking(|| std::fs::remove_dir_all(database_path(&data_dir, &actual_name)))
            {
                return self
                    .send_database_ddl_error(
                        session,
                        ready,
                        query_id,
                        sql_error_from_io(error.to_string()),
                        started,
                    )
                    .await;
            }
            if let Err(error) = blocking(|| registry.persist(&data_dir)) {
                return self
                    .send_database_ddl_error(
                        session,
                        ready,
                        query_id,
                        sql_error_from_io(error.to_string()),
                        started,
                    )
                    .await;
            }
            // Evict the shared engine so later connections reopen fresh state
            // (or fail cleanly when the database is gone) instead of serving
            // through an engine rooted at deleted files. Live connections keep
            // their Arc, so eviction races nothing.
            if let Some(engines) = self.database_engines.clone() {
                engines.lock().await.remove(&actual_name.to_lowercase());
            }
            Ok(())
        } else if ddl.conditional {
            // `IF EXISTS`: already gone is success with no effect.
            Ok(())
        } else {
            Err(PlomidError::new(
                ErrorKind::NotFound,
                format!("database \"{name}\" does not exist"),
            ))
        };
        let database_names = registry.names.iter().cloned().collect::<Vec<_>>();
        drop(registry);
        match result {
            Ok(()) => {
                let executor = self.executor.as_mut().expect("executor initialized");
                executor.set_database_names(database_names);
                // Persist the name sync: without this the durable catalog
                // keeps the old list, every subsequently-connected session
                // starts with a dirty image (names differ from the registry),
                // and live-session refresh stays silently disabled for it.
                // The save also bumps the durable generation so other
                // sessions notice committed changes (roles, grants, tables).
                // A failed save keeps the marks, so a later save retries the
                // same delta instead of losing it.
                if let Err(err) = executor.persist_catalog() {
                    self.log_query_failure(query_id, session.id, "", &err, started);
                    return self
                        .send_error_response(err, session.transaction_status)
                        .await;
                }
                let tag = if create {
                    "CREATE DATABASE"
                } else {
                    "DROP DATABASE"
                };
                tracing::info!(target: "server", event = "database_ddl", query_id, database = name, operation = tag,
                    elapsed_ms = started.elapsed().as_millis() as u64);
                self.send_staged_command(tag, 0, session, ready).await
            }
            Err(error) => {
                // Semantic error kinds flow through untouched so the wire
                // reports PostgreSQL SQLSTATEs: AlreadyExists → 42P04,
                // NotFound-naming-a-database → 3D000, PermissionDenied →
                // 42501 (see `sqlstate_for`).
                let err = SqlError::Storage(error);
                self.log_query_failure(query_id, session.id, "", &err, started);
                self.send_error_response(err, session.transaction_status)
                    .await
            }
        }
    }

    /// Resolves the single shared engine for `database`, opening it once.
    ///
    /// Keys are lowercased database names. The open holds the map lock, so
    /// concurrent first-contacts serialize and exactly one engine is created;
    /// every later connection reuses it. Without this sharing, independent
    /// engines append to one database's WAL with independent LSN counters and
    /// corrupt it. Returns the engine and whether this call created it.
    async fn shared_engine_for(
        &self,
        database: &str,
        database_dir: &std::path::Path,
        pool_capacity: usize,
    ) -> std::io::Result<Arc<Mutex<PlomidStorageEngine>>> {
        let Some(engines) = self.database_engines.clone() else {
            // Single-database mode (no registry): keep the previous behavior
            // of opening directly; there is no map to share through.
            let opened =
                PlomidStorageEngine::open(database_dir, &database_dir.join("wal"), pool_capacity)
                    .map_err(|error| std::io::Error::other(error.to_string()))?;
            return Ok(Arc::new(Mutex::new(opened)));
        };
        let key = database.to_lowercase();
        let mut map = engines.lock().await;
        if let Some(engine) = map.get(&key) {
            return Ok(Arc::clone(engine));
        }
        let opened =
            PlomidStorageEngine::open(database_dir, &database_dir.join("wal"), pool_capacity)
                .map_err(|error| std::io::Error::other(error.to_string()))?;
        let shared = Arc::new(Mutex::new(opened));
        map.insert(key, Arc::clone(&shared));
        Ok(shared)
    }

    async fn handle_database_switch(
        &mut self,
        name: &str,
        session: &mut Session,
        ready: bool,
        query_id: u64,
        started: std::time::Instant,
    ) -> std::io::Result<()> {
        if session.transaction_status != TransactionStatus::Idle {
            let error = SqlError::Storage(PlomidError::new(
                ErrorKind::Conflict,
                "cannot change database during a transaction",
            ));
            self.log_query_failure(query_id, session.id, "USE", &error, started);
            return self
                .send_error_response(error, session.transaction_status)
                .await;
        }
        // USE of the already-selected database is a no-op in PostgreSQL
        // client workflows. Reopening the segment/WAL files here makes GUI
        // discovery appear hung and can contend with concurrent metadata
        // queries.
        if session.database.eq_ignore_ascii_case(name) {
            return self
                .send_query_result(session, plomid_sql::QueryResult::Set, ready)
                .await;
        }
        // CONNECT gate (the executor gate never sees USE: switching rebuilds
        // the executor below). Denied → 42501 with an audit event, no switch.
        if let Some(executor) = self.executor.as_ref() {
            if let Err(error) = executor.check_database_connect(&session.user, name) {
                plomid_core::observability::record_authz_denial();
                let event = plomid_security::AuditEvent::deny(
                    &session.user,
                    plomid_security::AuditAction::Authorize {
                        statement: "CONNECT".to_string(),
                    },
                    Some(format!("DATABASE {name}")),
                    "connect_revoked",
                );
                tracing::warn!(target: "security", "{}", event.log_line());
                let _ = plomid_security::sink::emit(&event);
                self.log_query_failure(query_id, session.id, "USE", &error, started);
                return self
                    .send_error_response(error, session.transaction_status)
                    .await;
            }
        }
        let Some((data_dir, registry)) = self.database_context.clone() else {
            let error = SqlError::Storage(PlomidError::new(
                ErrorKind::Unsupported,
                "database switching is unavailable",
            ));
            return self
                .send_error_response(error, TransactionStatus::Idle)
                .await;
        };
        let database_dir = {
            let mut registry = registry.lock().await;
            match registry.open_path(&data_dir, name) {
                Ok(path) => path,
                Err(error) => {
                    let sql_error = sql_error_from_io(error.to_string());
                    self.log_query_failure(query_id, session.id, "USE", &sql_error, started);
                    return self
                        .send_error_response(sql_error, TransactionStatus::Idle)
                        .await;
                }
            }
        };
        let opened = match self.shared_engine_for(name, &database_dir, 64).await {
            Ok(engine) => engine,
            Err(error) => {
                let sql_error =
                    SqlError::Storage(PlomidError::new(ErrorKind::Internal, error.to_string()));
                self.log_query_failure(query_id, session.id, "USE", &sql_error, started);
                return self
                    .send_error_response(sql_error, TransactionStatus::Idle)
                    .await;
            }
        };
        let database_names = {
            let registry = registry.lock().await;
            registry.names.iter().cloned().collect::<Vec<_>>()
        };
        self.engine = opened;
        self.executor = Some(
            Executor::new_shared(self.engine.clone())
                .map_err(|error| std::io::Error::other(error.to_string()))?,
        );
        self.executor
            .as_mut()
            .expect("executor initialized")
            .set_session_context(name, &session.user);
        self.executor
            .as_mut()
            .expect("executor initialized")
            .set_maintenance_policy(tiered_maintenance_policy());
        if let Some(link) = self.maintenance_link.clone() {
            self.executor
                .as_mut()
                .expect("executor initialized")
                .set_background_maintenance(link);
        }
        self.executor
            .as_mut()
            .expect("executor initialized")
            .set_database_names(database_names);
        session.database = name.to_string();
        session.set_parameter("database", name);
        tracing::info!(target: "server", event = "database_switch", connection_id = session.id, database = name,
            elapsed_ms = started.elapsed().as_millis() as u64);
        self.send_query_result(session, plomid_sql::QueryResult::Set, ready)
            .await
    }

    async fn handle_show_databases(
        &mut self,
        session: &mut Session,
        ready: bool,
        query_id: u64,
        started: std::time::Instant,
    ) -> std::io::Result<()> {
        let Some((_data_dir, registry)) = &self.database_context else {
            return self
                .send_query_result(
                    session,
                    plomid_sql::QueryResult::Rows {
                        columns: vec!["database".to_string()],
                        column_types: vec![Some(plomid_sql::ColumnType::text())],
                        rows: vec![vec![plomid_sql::Value::Text("plomid".to_string())]],
                    },
                    ready,
                )
                .await;
        };
        let names = {
            let registry = registry.lock().await;
            registry.names.iter().cloned().collect::<Vec<_>>()
        };
        let rows: Vec<Vec<plomid_sql::Value>> = names
            .into_iter()
            .map(|name| vec![plomid_sql::Value::Text(name)])
            .collect();
        tracing::debug!(target: "server::protocol", event = "query_completed", query_id,
            elapsed_ms = started.elapsed().as_millis() as u64, result = "rows", row_count = rows.len(),
            row_description_sent = true, data_rows_sent = !rows.is_empty());
        self.send_query_result(
            session,
            plomid_sql::QueryResult::Rows {
                columns: vec!["database".to_string()],
                column_types: vec![Some(plomid_sql::ColumnType::text())],
                rows,
            },
            ready,
        )
        .await
    }

    async fn send_database_ddl_error(
        &mut self,
        session: &mut Session,
        _ready: bool,
        query_id: u64,
        error: SqlError,
        started: std::time::Instant,
    ) -> std::io::Result<()> {
        self.log_query_failure(query_id, session.id, "CREATE DATABASE", &error, started);
        self.send_error_response(error, session.transaction_status)
            .await
    }

    /// Releases any held repeatable-read snapshot: unpins facade reads and
    /// aborts the held manager transaction id.
    ///
    /// Legacy pin path (pre-Phase-4 sessions pinned facade reads around
    /// COMMIT replay). Session transactions now carry their own snapshots;
    /// this remains for connection teardown and single-batch group closes,
    /// which never create pins (no-op there). Fail-open: a release failure
    /// is logged, never returned.
    fn release_rr_pin(&mut self, session: &mut Session) {
        let Some(txn_id) = session.take_rr_txn() else {
            return;
        };
        if let Some(engine) = self.executor.as_mut() {
            let _ = engine.unpin_read_snapshot();
            if let Err(error) = engine.abort_pinned_read(TxnId::new(txn_id)) {
                tracing::warn!(
                    target: "server::protocol",
                    event = "rr_release_failed",
                    error = %error.to_string(),
                );
            }
        }
        session.set_parameter(
            "isolation_level",
            isolation_display(session.default_isolation).to_string(),
        );
    }

    async fn handle_session_transaction(
        &mut self,
        session: &mut Session,
        sql: &str,
        ready: bool,
        query_id: u64,
        started: std::time::Instant,
    ) -> std::io::Result<()> {
        match statement_type(sql) {
            "BEGIN" => {
                if session.transaction_status == TransactionStatus::InTransaction {
                    self.send_staged_command("BEGIN", 0, session, ready).await
                } else if session.transaction_status == TransactionStatus::InFailedTransaction {
                    let err = SqlError::Storage(PlomidError::new(
                        ErrorKind::Conflict,
                        "current transaction is aborted, commands ignored until ROLLBACK",
                    ));
                    self.log_query_failure(query_id, session.id, sql, &err, started);
                    self.send_error_response(err, session.transaction_status)
                        .await
                } else {
                    let level = match parse_isolation_level(sql) {
                        None => session.default_isolation,
                        Some(ParsedIsolation::ReadCommitted) => IsolationLevel::ReadCommitted,
                        Some(ParsedIsolation::RepeatableRead) => IsolationLevel::RepeatableRead,
                        Some(ParsedIsolation::Serializable) => {
                            let err = SqlError::Storage(PlomidError::new(
                                ErrorKind::InvalidArgument,
                                "SERIALIZABLE isolation is not supported",
                            ));
                            self.log_query_failure(query_id, session.id, sql, &err, started);
                            return self
                                .send_error_response(err, session.transaction_status)
                                .await;
                        }
                        Some(ParsedIsolation::Invalid) => {
                            let err = SqlError::Storage(PlomidError::new(
                                ErrorKind::InvalidArgument,
                                "invalid isolation level",
                            ));
                            self.log_query_failure(query_id, session.id, sql, &err, started);
                            return self
                                .send_error_response(err, session.transaction_status)
                                .await;
                        }
                    };
                    // Open a real session transaction: effects stage into
                    // its write overlay from here on; COMMIT publishes
                    // without re-execution. The snapshot behavior follows
                    // the isolation level (pinned for RR, refreshed per
                    // statement for RC).
                    let isolation = match level {
                        IsolationLevel::RepeatableRead => {
                            plomid_executor::session_txn::TxnIsolation::RepeatableRead
                        }
                        IsolationLevel::ReadCommitted => {
                            plomid_executor::session_txn::TxnIsolation::ReadCommitted
                        }
                    };
                    let engine = self.executor.as_mut().expect("executor initialized");
                    if let Err(err) = blocking(|| engine.begin_session_txn(isolation)) {
                        self.log_query_failure(query_id, session.id, sql, &err, started);
                        return self
                            .send_error_response(err, session.transaction_status)
                            .await;
                    }
                    if level == IsolationLevel::RepeatableRead {
                        session.mark_in_transaction_with(IsolationLevel::RepeatableRead);
                        session.set_parameter("isolation_level", "repeatable read");
                    } else {
                        session.mark_in_transaction();
                        session.set_parameter("isolation_level", "read committed");
                    }
                    self.send_staged_command("BEGIN", 0, session, ready).await
                }
            }
            "ROLLBACK" => {
                if matches!(session.transaction_status, TransactionStatus::Idle) {
                    let err = SqlError::Storage(PlomidError::new(
                        ErrorKind::Conflict,
                        "no active transaction",
                    ));
                    self.log_query_failure(query_id, session.id, sql, &err, started);
                    self.send_error_response(err, session.transaction_status)
                        .await
                } else {
                    // True rollback: drops staged effects, restores the
                    // BEGIN catalog image. Idempotent recovery after a
                    // failed COMMIT (nothing open → no-op Ok).
                    let engine = self.executor.as_mut().expect("executor initialized");
                    match blocking(|| engine.abort_session_txn()) {
                        Ok(_) => {
                            session.mark_idle();
                            session.set_parameter(
                                "isolation_level",
                                isolation_display(session.default_isolation).to_string(),
                            );
                            self.send_staged_command("ROLLBACK", 0, session, ready)
                                .await
                        }
                        Err(err) => {
                            session.mark_idle();
                            self.log_query_failure(query_id, session.id, sql, &err, started);
                            self.send_error_response(err, session.transaction_status)
                                .await
                        }
                    }
                }
            }
            "COMMIT" => {
                if !session.is_in_transaction() {
                    let err = SqlError::Storage(PlomidError::new(
                        ErrorKind::Conflict,
                        "no active transaction",
                    ));
                    self.log_query_failure(query_id, session.id, sql, &err, started);
                    self.send_error_response(err, session.transaction_status)
                        .await
                } else {
                    // True commit: validates + publishes staged effects
                    // (WAL + MVCC) with no re-execution. Conflict and
                    // constraint failures surface here, once, for the whole
                    // transaction; the transaction is over either way.
                    let engine = self.executor.as_mut().expect("executor initialized");
                    match blocking(|| engine.commit_session_txn()) {
                        Ok(_) => {
                            session.mark_idle();
                            session.set_parameter(
                                "isolation_level",
                                isolation_display(session.default_isolation).to_string(),
                            );
                            self.send_staged_command("COMMIT", 0, session, ready).await
                        }
                        Err(err) => {
                            session.mark_failed_transaction();
                            self.log_query_failure(query_id, session.id, sql, &err, started);
                            self.send_error_response(err, session.transaction_status)
                                .await
                        }
                    }
                }
            }
            _ => Ok(()),
        }
    }

    /// Collapse adjacent INSERT ... VALUES statements produced by the
    /// extended-protocol executemany path.  The wire protocol may deliver
    /// each row as its own Execute, but all of those statements are already
    /// held in the session transaction.  Replaying them as one multi-row
    /// statement lets the executor allocate transaction state, validate the
    /// schema, encode rows, and apply storage/index mutations once per batch.
    ///
    /// Only statements with an identical INSERT prefix are combined.  This
    /// keeps ordering, statement semantics, and rollback behavior unchanged
    /// for all other SQL forms.
    async fn send_staged_command(
        &mut self,
        statement: &str,
        count: u64,
        session: &mut Session,
        ready: bool,
    ) -> std::io::Result<()> {
        let tag = match statement {
            "INSERT" => format!("INSERT 0 {count}"),
            "UPDATE" => format!("UPDATE {count}"),
            "DELETE" => format!("DELETE {count}"),
            _ => statement.to_string(),
        };
        self.write_message_buffered(MessageEncoder::encode_command_complete(&tag))
            .await?;
        if ready {
            self.write_message_buffered(MessageEncoder::encode_ready_for_query(
                session.transaction_status,
            ))
            .await?;
        }
        self.writer.flush().await?;
        self.active_result_formats.clear();
        self.active_result_type_oids.clear();
        Ok(())
    }

    fn log_query_failure(
        &self,
        query_id: u64,
        connection_id: u64,
        sql: &str,
        err: &SqlError,
        started: std::time::Instant,
    ) {
        let (code, message) = match err {
            SqlError::Syntax(e) | SqlError::Storage(e) => (e.code(), e.message()),
        };
        tracing::error!(target: "server::protocol", event = "query_failed", connection_id, query_id,
            error_code = code, error_message = message, elapsed_ms = started.elapsed().as_millis() as u64,
            statement_type = statement_type(sql), sql_len = sql.len());
    }

    async fn send_query_result(
        &mut self,
        session: &mut Session,
        result: plomid_sql::QueryResult,
        ready: bool,
    ) -> std::io::Result<()> {
        match result {
            plomid_sql::QueryResult::Rows {
                columns,
                column_types,
                rows,
            } => {
                let column_types = infer_missing_result_types(column_types, &rows);
                if columns.len() != column_types.len()
                    || rows.iter().any(|row| row.len() != columns.len())
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "result column metadata does not match row width",
                    ));
                }
                if !self.extended_description_sent {
                    self.write_message_buffered(MessageEncoder::encode_row_description(
                        &row_descriptions(&columns, &column_types, &self.active_result_formats),
                    ))
                    .await?;
                }
                // `rows_to_stream` consumes the `QueryResult`; metadata was
                // already validated and described above, so only the rows are
                // re-wrapped. Bounded batches through the streaming bridge:
                // identical encoder calls in identical order, with the
                // completion-tag count taken from delivered batches.
                // (`Default` here is the bridge `StreamConfig`, inferred from
                // the call signature — no new dependency.)
                let rebuilt = plomid_sql::QueryResult::Rows {
                    columns: Vec::new(),
                    column_types: Vec::new(),
                    rows,
                };
                let Some((_, mut stream)) = rows_to_stream(rebuilt, Default::default()) else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "result stream rejected row result",
                    ));
                };
                let mut delivered = 0usize;
                while let Some(batch) = stream
                    .next_batch(WIRE_SEND_BATCH_SIZE)
                    .map_err(|error| std::io::Error::other(error.to_string()))?
                {
                    for row in &batch {
                        let values: Vec<Option<Vec<u8>>> = row
                            .iter()
                            .enumerate()
                            .map(|(index, v)| match v {
                                plomid_sql::Value::Null => None,
                                other => {
                                    let result_oid = self
                                        .active_result_type_oids
                                        .get(index)
                                        .copied()
                                        .or_else(|| {
                                            column_types
                                                .get(index)
                                                .and_then(|column| column.as_ref())
                                                .map(|column| column.type_oid.0)
                                        })
                                        .or_else(|| {
                                            plomid_sql::value_pg_type(other).map(|ty| ty.oid().0)
                                        });
                                    Some(encode_result_value(
                                        other,
                                        wire_format(
                                            self.active_result_formats
                                                .get(index)
                                                .copied()
                                                .or_else(|| {
                                                    self.active_result_formats.first().copied()
                                                })
                                                .unwrap_or(0),
                                            result_oid,
                                        ),
                                        result_oid,
                                    ))
                                }
                            })
                            .collect();
                        self.write_message_buffered(MessageEncoder::encode_data_row(&values))
                            .await?;
                    }
                    delivered += batch.len();
                }
                let tag = format!("SELECT {delivered}");
                self.write_message_buffered(MessageEncoder::encode_command_complete(&tag))
                    .await?;
                self.extended_description_sent = false;
            }
            plomid_sql::QueryResult::Inserted(count) => {
                let tag = format!("INSERT 0 {}", count);
                self.write_message_buffered(MessageEncoder::encode_command_complete(&tag))
                    .await?;
            }
            plomid_sql::QueryResult::Updated(count) => {
                let tag = format!("UPDATE {}", count);
                self.write_message_buffered(MessageEncoder::encode_command_complete(&tag))
                    .await?;
            }
            plomid_sql::QueryResult::Deleted(count) => {
                let tag = format!("DELETE {}", count);
                self.write_message_buffered(MessageEncoder::encode_command_complete(&tag))
                    .await?;
            }
            plomid_sql::QueryResult::Set => {
                self.write_message_buffered(MessageEncoder::encode_command_complete("SET"))
                    .await?;
            }
            plomid_sql::QueryResult::Created(tag) => {
                self.write_message_buffered(MessageEncoder::encode_command_complete(&tag))
                    .await?;
            }
            plomid_sql::QueryResult::Committed => {
                self.release_rr_pin(session);
                self.write_message_buffered(MessageEncoder::encode_command_complete("COMMIT"))
                    .await?;
            }
            plomid_sql::QueryResult::RolledBack => {
                self.write_message_buffered(MessageEncoder::encode_command_complete("ROLLBACK"))
                    .await?;
                self.release_rr_pin(session);
                session.mark_idle();
            }
        }
        if ready {
            self.write_message_buffered(MessageEncoder::encode_ready_for_query(
                session.transaction_status,
            ))
            .await?;
        }
        self.writer.flush().await?;
        Ok(())
    }

    async fn send_error_response_fatal(
        &mut self,
        code: &str,
        message: impl Into<String>,
    ) -> std::io::Result<()> {
        let response = ErrorResponse::new("FATAL", code, message);
        self.send_error(response).await
    }

    async fn send_protocol_error(&mut self, message: impl Into<String>) -> std::io::Result<()> {
        self.send_error(ErrorResponse::new("ERROR", "08P01", message))
            .await
    }

    async fn send_error_response(
        &mut self,
        err: SqlError,
        status: TransactionStatus,
    ) -> std::io::Result<()> {
        self.extended_description_sent = false;
        let response = match err {
            SqlError::Syntax(_) | SqlError::Storage(_) => executor_error_response_with_datadir(
                err,
                self.database_context.as_ref().map(|(dir, _)| dir.as_path()),
            ),
        };
        self.write_message_buffered(MessageEncoder::encode_error_response(&response))
            .await?;
        self.write_message_buffered(MessageEncoder::encode_ready_for_query(status))
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn send_query_error(
        &mut self,
        err: SqlError,
        status: TransactionStatus,
        ready: bool,
    ) -> std::io::Result<()> {
        // A failed extended-protocol Execute must not leak the RowDescription
        // flag that Describe set; otherwise the next statement would skip its
        // own RowDescription and send DataRow without a prior T message.
        self.extended_description_sent = false;
        let response = executor_error_response_with_datadir(
            err,
            self.database_context.as_ref().map(|(dir, _)| dir.as_path()),
        );
        self.write_message_buffered(MessageEncoder::encode_error_response(&response))
            .await?;
        if ready {
            self.write_message_buffered(MessageEncoder::encode_ready_for_query(status))
                .await?;
        }
        self.writer.flush().await?;
        Ok(())
    }

    async fn send_error(&mut self, error: ErrorResponse) -> std::io::Result<()> {
        self.write_message(MessageEncoder::encode_error_response(&error))
            .await
    }

    async fn write_message(&mut self, data: Vec<u8>) -> std::io::Result<()> {
        tracing::debug!(target: "server::protocol", event = "wire_out", tag = *data.first().unwrap_or(&0u8));
        self.writer.write_all(&data).await?;
        self.writer.flush().await?;
        Ok(())
    }

    async fn write_message_buffered(&mut self, data: Vec<u8>) -> std::io::Result<()> {
        tracing::debug!(target: "server::protocol", event = "wire_out", tag = *data.first().unwrap_or(&0u8));
        self.writer.write_all(&data).await
    }

    async fn read_message(&mut self) -> std::io::Result<Option<(FrontendTag, Vec<u8>)>> {
        MessageDecoder::read_frontend(&mut self.reader).await
    }

    /// Reads the next frontend message, bounded by
    /// `idle_in_transaction_session_timeout` while a transaction is open.
    ///
    /// Only the idle-in-txn wait is bounded: `statement_timeout` is enforced
    /// separately around executor dispatch, and an idle (non-transaction)
    /// connection holds no RR pin or staged state worth reaping. On expiry
    /// the RR pin is released (bounds M5 retention), staged state is
    /// discarded, FATAL 25P03 is sent, and an error is returned so the
    /// connection closes.
    async fn read_message_bounded(
        &mut self,
        session: &mut Session,
    ) -> std::io::Result<Option<(FrontendTag, Vec<u8>)>> {
        let timeout = if session.is_in_transaction() {
            plomid_executor::guard::parse_timeout(
                session.get_parameter("idle_in_transaction_session_timeout"),
            )
        } else {
            None
        };
        let Some(bound) = timeout else {
            return self.read_message().await;
        };
        match tokio::time::timeout(bound, self.read_message()).await {
            Ok(message) => message,
            Err(_) => {
                // An expired idle-in-transaction hold must release the
                // session transaction too (lanes + manager id), not just
                // the legacy pin — otherwise the timed-out session strands
                // other writers and pins GC forever.
                if let Some(engine) = self.executor.as_mut() {
                    let _ = blocking(|| engine.abort_session_txn());
                }
                self.release_rr_pin(session);
                session.mark_idle();
                let _ = self
                    .send_error_response_fatal(
                        "25P03",
                        "terminating connection due to idle-in-transaction timeout",
                    )
                    .await;
                Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "idle-in-transaction timeout",
                ))
            }
        }
    }

    /// Installs the session's `statement_timeout` (plus the given cancel flag)
    /// around a synchronous executor call and runs it.
    ///
    /// Choice of layer (documented per task): the timeout lives at the
    /// connection-dispatch layer, not threaded through sibling-owned
    /// `executor/src/query/*` operators. Enforcement inside long-running
    /// operators is still cooperative — the guard is thread-local and the
    /// freely-owned `join/*` probe loops poll it — but no forbidden file is
    /// touched to plumb a deadline parameter. `statement_timeout=0`
    /// disables. `lock_timeout` is accepted via SET for protocol
    /// compatibility but not separately enforced: gate waits park on a
    /// `Condvar` with no cheap hook, and the statement deadline still bounds
    /// total time for queries that reach a poll point.
    fn statement_deadline(session: &Session) -> Option<std::time::Instant> {
        plomid_executor::guard::deadline_from_param(session.get_parameter("statement_timeout"))
    }

    /// Runs a synchronous executor call with the session's statement timeout
    /// and the connection's cancel flag installed (see
    /// [`Self::statement_deadline`]). Associated function (not a method) so
    /// callers holding a `&mut` borrow of `self.executor` can still use it.
    fn exec_guarded<R>(cancel: &Arc<AtomicBool>, session: &Session, f: impl FnOnce() -> R) -> R {
        let deadline = Self::statement_deadline(session);
        plomid_executor::guard::with_guard(deadline, Some(Arc::clone(cancel)), || blocking(f))
    }

    /// Sends a guard (timeout/cancel) failure with the PostgreSQL `57014`
    /// SQLSTATE instead of the generic mapping. Returns `true` when the
    /// connection must close (user cancel): the query is gone, and the
    /// client expects the connection to drop.
    async fn send_guard_error(
        &mut self,
        err: SqlError,
        status: TransactionStatus,
        ready: bool,
    ) -> std::io::Result<bool> {
        self.extended_description_sent = false;
        let message = err.to_string();
        let response = ErrorResponse::new("ERROR", "57014", message);
        let is_cancel = plomid_executor::guard::is_cancel_error(&err);
        if is_cancel {
            // Cancel drops the connection: error, no ReadyForQuery.
            self.write_message(MessageEncoder::encode_error_response(&response))
                .await?;
            return Ok(true);
        }
        self.write_message_buffered(MessageEncoder::encode_error_response(&response))
            .await?;
        if ready {
            self.write_message_buffered(MessageEncoder::encode_ready_for_query(status))
                .await?;
        }
        self.writer.flush().await?;
        Ok(false)
    }

    fn next_query_id(&self) -> u64 {
        self.query_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }
}

fn infer_missing_result_types(
    mut column_types: Vec<Option<plomid_sql::ColumnType>>,
    rows: &[Vec<plomid_sql::Value>],
) -> Vec<Option<plomid_sql::ColumnType>> {
    for (index, column_type) in column_types.iter_mut().enumerate() {
        if column_type.is_some_and(|ty| ty.type_oid.0 != 25) {
            continue;
        }
        if let Some(value) = rows
            .iter()
            .filter_map(|row| row.get(index))
            .find(|value| !matches!(value, plomid_sql::Value::Null | plomid_sql::Value::Text(_)))
        {
            if let plomid_sql::Value::Array { element_oid, .. } = value {
                if let Some(element) = plomid_types::PgType::by_oid(*element_oid) {
                    if let Some(array_oid) = element.array_oid() {
                        *column_type = Some(plomid_sql::ColumnType::new(
                            array_oid,
                            plomid_types::NO_TYPEMOD,
                        ));
                    }
                }
            } else if let Some(pg_type) = plomid_sql::value_pg_type(value) {
                *column_type = Some(plomid_sql::ColumnType::new(
                    pg_type.oid(),
                    plomid_types::NO_TYPEMOD,
                ));
            }
        }
    }
    column_types
}

/// Final Describe-time column metadata: infer types that the NULL-placeholder
/// probe could not determine, then honor explicit parameter casts. Keeping
/// this in one helper ensures a cached shape is byte-identical to the first
/// probe's RowDescription.
fn infer_describe_column_types(
    column_types: Vec<Option<plomid_sql::ColumnType>>,
    rows: &[Vec<plomid_sql::Value>],
    query: &str,
) -> Vec<Option<plomid_sql::ColumnType>> {
    let mut column_types = infer_missing_result_types(column_types, rows);
    // A Parse parameter's declared type is input metadata, not result
    // metadata. For a typed parameter expression, infer the result from the
    // expression's explicit PostgreSQL cast while describing the statement
    // with NULL placeholders.
    for (index, column_type) in column_types.iter_mut().enumerate() {
        if column_type.is_none() || column_type.is_some_and(|ty| ty.type_oid.0 == 25) {
            if let Some(oid) = explicit_parameter_cast(query, index + 1) {
                *column_type = Some(plomid_sql::ColumnType::new(
                    plomid_types::TypeOid(oid),
                    plomid_types::NO_TYPEMOD,
                ));
            }
        }
    }
    column_types
}

/// A `CREATE/DROP DATABASE` statement intercepted before the executor.
///
/// `conditional` is `IF NOT EXISTS` on CREATE and `IF EXISTS` on DROP:
/// both make the statement succeed (with no effect) when the name is
/// already present/absent, per PostgreSQL. Any other trailing option on
/// DROP (or an unknown option on CREATE) returns `None` so the statement
/// falls through to the executor, which rejects it fail-closed instead of
/// silently ignoring the option.
struct DatabaseDdl {
    create: bool,
    name: String,
    conditional: bool,
}

fn parse_database_ddl(sql: &str) -> Option<DatabaseDdl> {
    let words: Vec<&str> = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect();
    if words.len() < 3
        || !words[0].eq_ignore_ascii_case("CREATE") && !words[0].eq_ignore_ascii_case("DROP")
    {
        return None;
    }
    if !words[1].eq_ignore_ascii_case("DATABASE") {
        return None;
    }
    let is_create = words[0].eq_ignore_ascii_case("CREATE");
    if !is_create {
        // `DROP DATABASE [IF EXISTS] name` and nothing else.
        let conditional = words.len() == 5
            && words[2].eq_ignore_ascii_case("IF")
            && words[3].eq_ignore_ascii_case("EXISTS");
        if words.len() != 3 && !conditional {
            return None;
        }
        let name_index = if conditional { 4 } else { 2 };
        let name = words[name_index].trim_matches('"');
        return safe_database_name(name).map(|name| DatabaseDdl {
            create: false,
            name: name.to_string(),
            conditional,
        });
    }
    // `CREATE DATABASE [IF NOT EXISTS] name [options...]`. The conditional
    // flag sits at words 2-4 and shifts the name to words[5]; without it
    // the name is words[2]. A bare `CREATE DATABASE IF NOT EXISTS` (no
    // name) falls through.
    let conditional = words.len() >= 6
        && words[2].eq_ignore_ascii_case("IF")
        && words[3].eq_ignore_ascii_case("NOT")
        && words[4].eq_ignore_ascii_case("EXISTS");
    let mut index = if conditional { 6 } else { 3 };
    let Some(name) = words.get(if conditional { 5 } else { 2 }) else {
        return None;
    };
    let name = name.trim_matches('"');
    while index < words.len() {
        let option = words[index].to_ascii_lowercase();
        let consumed = match option.as_str() {
            "encoding" | "template" | "owner" | "tablespace" | "strategy" | "lc_collate"
            | "lc_ctype" | "is_template" | "allow_connections" => 2,
            "connection"
                if words
                    .get(index + 1)
                    .is_some_and(|word| word.eq_ignore_ascii_case("limit")) =>
            {
                3
            }
            _ => return None,
        };
        if index + consumed > words.len() {
            return None;
        }
        index += consumed;
    }
    safe_database_name(name).map(|name| DatabaseDdl {
        create: true,
        name: name.to_string(),
        conditional,
    })
}

fn parse_use_database(sql: &str) -> Option<String> {
    let words: Vec<&str> = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect();
    if words.len() == 2 && words[0].eq_ignore_ascii_case("USE") {
        return safe_database_name(words[1].trim_matches('"')).map(str::to_owned);
    }
    None
}

fn is_show_databases(sql: &str) -> bool {
    let words: Vec<&str> = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect();
    words.len() == 2
        && words[0].eq_ignore_ascii_case("SHOW")
        && (words[1].eq_ignore_ascii_case("DATABASE") || words[1].eq_ignore_ascii_case("DATABASES"))
}

fn sql_error_from_io(message: String) -> SqlError {
    SqlError::Storage(PlomidError::new(ErrorKind::Io, message))
}

/// Isolation level named by a `BEGIN`/`START TRANSACTION` or isolation
/// `SET` statement, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParsedIsolation {
    ReadCommitted,
    RepeatableRead,
    Serializable,
    Invalid,
}

/// Finds `ISOLATION LEVEL <level>` in `sql` (case-insensitive, optional
/// quoting). Returns `None` when no isolation clause is present.
/// `READ UNCOMMITTED` maps to `ReadCommitted`: serving committed state to
/// a caller that asked for uncommitted visibility errs toward safety, so
/// it is never a silent downgrade.
pub fn parse_isolation_level(sql: &str) -> Option<ParsedIsolation> {
    let words: Vec<&str> = sql.split_whitespace().collect();
    let mut index = 0;
    while index + 1 < words.len() {
        if words[index].eq_ignore_ascii_case("isolation")
            && words[index + 1].eq_ignore_ascii_case("level")
        {
            let rest: Vec<String> = words[index + 2..]
                .iter()
                .map(|word| {
                    word.trim_matches(|c| c == ';' || c == '\'' || c == '"')
                        .to_ascii_lowercase()
                })
                .collect();
            let joined = rest.join(" ");
            if joined.starts_with("serializable") {
                return Some(ParsedIsolation::Serializable);
            }
            if joined.starts_with("repeatable read") {
                return Some(ParsedIsolation::RepeatableRead);
            }
            if joined.starts_with("read committed") || joined.starts_with("read uncommitted") {
                return Some(ParsedIsolation::ReadCommitted);
            }
            return Some(ParsedIsolation::Invalid);
        }
        index += 1;
    }
    None
}

/// Scope of an isolation `SET`: `SET SESSION ...` changes the session
/// default for later transactions; bare `SET TRANSACTION` / `SET LOCAL`
/// (and anything else resolving to `isolation_level`) targets the
/// session default as well, except an explicit per-transaction SET,
/// which PostgreSQL restricts to open transaction blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationSetScope {
    SessionDefault,
    CurrentTransaction,
}

pub fn isolation_set_scope(sql: &str) -> IsolationSetScope {
    let second = sql
        .split_whitespace()
        .nth(1)
        .unwrap_or("")
        .trim_matches(|c| c == ';' || c == '\'' || c == '"');
    if second.eq_ignore_ascii_case("transaction") || second.eq_ignore_ascii_case("local") {
        IsolationSetScope::CurrentTransaction
    } else {
        IsolationSetScope::SessionDefault
    }
}

/// Display spelling for `SHOW`/parameter status, matching PostgreSQL's
/// lowercase multi-word form.
pub fn isolation_display(level: IsolationLevel) -> &'static str {
    match level {
        IsolationLevel::ReadCommitted => "read committed",
        IsolationLevel::RepeatableRead => "repeatable read",
    }
}

fn protocol_error(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

/// Parses a SASLInitialResponse PasswordMessage payload, returning the
/// client-first SCRAM message. The authentication identity comes from the
/// startup message (the `user` parameter), exactly as PostgreSQL does, so
/// the username carried inside the SCRAM exchange is not re-validated here.
fn parse_sasl_initial(payload: &[u8]) -> String {
    if let Some(index) = payload.iter().position(|byte| *byte == 0) {
        let mechanism = String::from_utf8(payload[..index].to_vec()).unwrap_or_default();
        let rest = &payload[index + 1..];
        let client_first = String::from_utf8(rest.to_vec()).unwrap_or_default();
        if mechanism != "SCRAM-SHA-256" {
            return String::new();
        }
        client_first
    } else {
        String::new()
    }
}

pub fn quote_bound_value(value: &str) -> String {
    // A text parameter whose content happens to spell "null" must stay a quoted
    // string: genuine SQL NULL arrives as `None` and is rendered by the caller.
    // (Previously this mapped the *text* "null" to NULL, corrupting data.)
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        value.to_string()
    } else {
        format!("'{}'", value.replace('\'', "''"))
    }
}

/// Highest `$N` placeholder index referenced *outside* string literals,
/// quoted identifiers, dollar-quoted bodies, and line comments.
///
/// The extended protocol lets clients declare zero parameter types and supply
/// values only at Bind, so the query text itself is the authority for how many
/// placeholders exist. Sizing substitution by the declared-type count instead
/// left raw `$1` tokens in the executed SQL whenever a client declared no
/// types (the common JDBC/pgx/psycopg3 shape), failing even `SELECT $1`.
pub fn max_placeholder(query: &str) -> usize {
    let bytes = query.as_bytes();
    let mut max = 0usize;
    let mut index = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_dollar = false;
    while index < bytes.len() {
        let ch = bytes[index];
        if in_line_comment {
            if ch == b'\n' {
                in_line_comment = false;
            }
            index += 1;
            continue;
        }
        if in_dollar {
            if ch == b'$' && index + 1 < bytes.len() && bytes[index + 1] == b'$' {
                in_dollar = false;
                index += 2;
            } else {
                index += 1;
            }
            continue;
        }
        if in_single {
            if ch == b'\'' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    index += 2;
                } else {
                    in_single = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        if in_double {
            if ch == b'"' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'"' {
                    index += 2;
                } else {
                    in_double = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        match ch {
            b'-' if index + 1 < bytes.len() && bytes[index + 1] == b'-' => {
                in_line_comment = true;
                index += 2;
            }
            b'\'' => {
                in_single = true;
                index += 1;
            }
            b'"' => {
                in_double = true;
                index += 1;
            }
            b'$' if index + 1 < bytes.len() && bytes[index + 1] == b'$' => {
                in_dollar = true;
                index += 2;
            }
            b'$' => {
                let mut end = index + 1;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if end > index + 1 {
                    if let Ok(number) = query[index + 1..end].parse::<usize>() {
                        max = max.max(number);
                    }
                    index = end;
                } else {
                    index += 1;
                }
            }
            _ => index += 1,
        }
    }
    max
}

fn copy_sql_value(value: &str) -> String {
    if value == r#"\N"# {
        return "NULL".into();
    }
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        return value.to_ascii_uppercase();
    }
    if value.parse::<i64>().is_ok() || value.parse::<f64>().is_ok() {
        return value.into();
    }
    format!("'{}'", value.replace('\\', "\\\\").replace('\'', "''"))
}

fn copy_escape_value(value: &plomid_sql::Value, output: &mut Vec<u8>) {
    if matches!(value, plomid_sql::Value::Null) {
        output.extend_from_slice(br"\N");
        return;
    }
    for byte in value.to_sql_text().bytes() {
        match byte {
            b'\\' => output.extend_from_slice(br"\\"),
            b'\t' => output.extend_from_slice(br"\t"),
            b'\n' => output.extend_from_slice(br"\n"),
            b'\r' => output.extend_from_slice(br"\r"),
            other => output.push(other),
        }
    }
}

fn parse_copy_from_stdin(sql: &str) -> Option<CopyState> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    let suffix = lower.rfind(" from stdin")?;
    if !lower.starts_with("copy ") {
        return None;
    }
    let source = trimmed[5..suffix].trim();
    let options = trimmed[suffix + " from stdin".len()..].trim();
    let open = source.find('(');
    let (table, columns) = if let Some(open) = open {
        let close = source.rfind(')')?;
        let table = source[..open].trim().to_string();
        let columns = format!("({})", source[open + 1..close].trim());
        (table, Some(columns))
    } else {
        (source.trim().to_string(), None)
    };
    if table.is_empty() {
        None
    } else {
        let (format, delimiter, header) = parse_copy_options(options, CopyFileFormat::Text)?;
        Some(CopyState {
            table,
            columns,
            data: Vec::new(),
            format,
            delimiter,
            header,
        })
    }
}

fn parse_copy_to_stdout(sql: &str) -> Option<String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let words: Vec<&str> = trimmed.split_whitespace().collect();
    if words.len() == 4
        && words[0].eq_ignore_ascii_case("copy")
        && words[2].eq_ignore_ascii_case("to")
        && words[3].eq_ignore_ascii_case("stdout")
    {
        let table = words[1];
        if !table.contains('(') && !table.contains(')') {
            return Some(table.to_string());
        }
    }
    None
}

fn parse_copy_file(sql: &str) -> Option<CopyFileSpec> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let lower = trimmed.to_ascii_lowercase();
    if !lower.starts_with("copy ") {
        return None;
    }
    let (direction, marker, marker_len) = if let Some(index) = lower.find(" from ") {
        (CopyFileDirection::From, index, " from ".len())
    } else {
        let index = lower.find(" to ")?;
        (CopyFileDirection::To, index, " to ".len())
    };
    let source = trimmed[5..marker].trim();
    let rest = trimmed[marker + marker_len..].trim();
    let (path, options) = parse_copy_path_and_options(rest)?;
    let (table, columns) = if let Some(open) = source.find('(') {
        let close = source.rfind(')')?;
        if close <= open || !source[close + 1..].trim().is_empty() {
            return None;
        }
        (
            source[..open].trim().to_string(),
            Some(format!("({})", source[open + 1..close].trim())),
        )
    } else {
        (source.to_string(), None)
    };
    if table.is_empty() {
        return None;
    }
    let (format, delimiter, header) = parse_copy_options(&options, CopyFileFormat::Text)?;
    Some(CopyFileSpec {
        table,
        columns,
        path: PathBuf::from(path),
        direction,
        format,
        delimiter,
        header,
    })
}

fn parse_copy_options(
    options: &str,
    default_format: CopyFileFormat,
) -> Option<(CopyFileFormat, u8, bool)> {
    let options_lower = options.to_ascii_lowercase();
    let format = if options_lower.contains("format csv") {
        CopyFileFormat::Csv
    } else if options_lower.contains("format text") || options.is_empty() {
        default_format
    } else {
        return None;
    };
    let delimiter = parse_copy_delimiter(options, format)?;
    let header = options_lower.contains("header")
        && (!options_lower.contains("header false") && !options_lower.contains("header = false"));
    Some((format, delimiter, header))
}

fn parse_copy_path_and_options(input: &str) -> Option<(String, String)> {
    let bytes = input.as_bytes();
    if bytes.first().copied()? != b'\'' {
        return None;
    }
    let mut end = 1;
    while end < bytes.len() {
        if bytes[end] == b'\'' {
            if bytes.get(end + 1) == Some(&b'\'') {
                end += 2;
                continue;
            }
            let path = input[1..end].replace("''", "'");
            let options = input[end + 1..].trim();
            return Some((path, options.to_string()));
        }
        end += 1;
    }
    None
}

/// Filename-only display for COPY wire errors.
///
/// COPY failures must never echo a server filesystem path (jail prefix,
/// `..` traversal, or symlink target) back over the wire. Only the final
/// path component is reported, sanitized (control/NUL → `?`) and truncated
/// so embedded NUL/control bytes cannot corrupt the NUL-terminated wire
/// error fields.
pub fn copy_display_name(path: &std::path::Path) -> String {
    let raw = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "unknown file".to_string());
    let mut clean: String = raw
        .chars()
        .map(|c| if c == '\0' || c.is_control() { '?' } else { c })
        .collect();
    // Bound the echoed leaf: a 4 KiB path of `a`s must not become a 4 KiB
    // wire error.
    if clean.len() > 128 {
        clean.truncate(128);
        clean.push_str("...");
    }
    if clean.is_empty() {
        "unknown file".to_string()
    } else {
        clean
    }
}

/// Whether a COPY I/O error is a jail denial (never a filesystem error).
///
/// `open_pinned` returns `PermissionDenied/"not permitted"` for planted
/// links, swapped parents, directories, and non-regular files; the Unix
/// `O_NOFOLLOW` layer returns raw `ELOOP`. Both map to the filename-only
/// jail denial so the wire never echoes where a link pointed.
#[cfg(unix)]
pub fn is_copy_denial(error: &std::io::Error) -> bool {
    error.to_string().contains("not permitted") || is_symlink_escape(error)
}

#[cfg(not(unix))]
pub fn is_copy_denial(error: &std::io::Error) -> bool {
    error.to_string().contains("not permitted")
}

/// Audited jail denial for COPY path refusals.
///
/// Role denials already audit inside `Executor::check_file_copy`; path
/// denials (traversal, absolute, symlink, swapped parent, directory,
/// FIFO/device, missing jail) audit here with the same `CopyFile` action
/// so operators see every filesystem refusal. Never carries a path —
/// only `TABLE <name>` — mirroring the filename-only wire errors.
fn audit_copy_jail_denial(user: &str, table: &str, direction: &CopyFileDirection) {
    let dir = if *direction == CopyFileDirection::To {
        "to".to_string()
    } else {
        "from".to_string()
    };
    let event = plomid_security::AuditEvent::deny(
        user,
        plomid_security::AuditAction::CopyFile { direction: dir },
        Some(format!("TABLE {table}")),
        "copy_path_not_permitted",
    );
    tracing::warn!(target: "security", "{}", event.log_line());
    let _ = plomid_security::sink::emit(&event);
    plomid_core::observability::record_authz_denial();
}

/// Strict syntactic gate for client-supplied COPY paths.
///
/// Denies before any filesystem touch: empty, absolute (including any
/// root/prefix component), NUL-bearing, overlong, trailing-slash
/// (directory), and any `.`/`..` component (`a/../b`, `./b`, `..`).
/// Canonicalization + `O_NOFOLLOW` opens below remain as defense in depth;
/// this gate makes `..` handling explicit rather than normalization-
/// dependent.
pub fn check_copy_path_syntax(requested: &std::path::Path) -> Result<(), String> {
    let name = copy_display_name(requested);
    let denied = format!("COPY file path is not permitted: {name}");
    if requested.as_os_str().is_empty() {
        return Err(denied);
    }
    if requested.is_absolute() {
        return Err(denied);
    }
    let bytes = requested.as_os_str().as_encoded_bytes();
    if bytes.contains(&0) {
        return Err(denied);
    }
    if bytes.len() > MAX_COPY_PATH_BYTES {
        return Err(denied);
    }
    // Trailing slash = directory intent (`subdir/` loses its slash in
    // components but `file_name` would then resolve to the parent).
    if bytes.last() == Some(&b'/') {
        return Err(denied);
    }
    #[cfg(windows)]
    if bytes.last() == Some(&b'\\') {
        return Err(denied);
    }
    // `Path::components` normalizes interior `.` away (`a/./b` yields
    // `[a, b]` with no `CurDir`), so check the raw slash-separated
    // segments explicitly: any `.`/`..`/empty segment denies. (`//`
    // collapses in components but is still rejected here as non-canonical.)
    let raw_lossy = requested.as_os_str().to_string_lossy();
    for segment in raw_lossy.split('/') {
        if segment == "." || segment == ".." || segment.is_empty() && raw_lossy.contains('/') {
            // Empty segments arise from `//`, leading `/` (already denied
            // as absolute), or trailing `/` (already denied): any of them
            // is non-canonical input — deny.
            return Err(denied);
        }
    }
    for component in requested.components() {
        match component {
            std::path::Component::Normal(part) => {
                let part_bytes = part.as_encoded_bytes();
                if part_bytes.is_empty() || part_bytes.len() > 255 || part_bytes.contains(&0) {
                    return Err(denied);
                }
            }
            // Prefix/RootDir = absolute; CurDir/ParentDir = `.`/`..`.
            _ => return Err(denied),
        }
    }
    // `file_name` is None for `.`/`..`/`/`/trailing-`..` shapes; those are
    // directories, never COPY files.
    if requested.file_name().is_none() {
        return Err(denied);
    }
    Ok(())
}

/// Resolves a client-supplied `COPY ... FROM/TO <path>` inside `jail`.
///
/// `jail` must already be canonicalized. The request is denied when it is
/// absolute, contains `.`/`..`/NUL, names a directory, or when neither the
/// path nor its parent canonicalizes inside the jail (covering `..`
/// traversal), and when canonicalization reveals a symlink escape. A
/// not-yet-existing destination (`COPY TO`) resolves through its
/// canonicalized parent, so a symlink in the parent chain cannot lift the
/// write outside the jail either; missing intermediate directories fail
/// closed instead of being created. Directories (existing or implied by a
/// trailing slash) are always denied: COPY moves file bytes, never trees.
pub fn resolve_copy_path(
    jail: &std::path::Path,
    requested: &std::path::Path,
) -> Result<PathBuf, String> {
    let name = copy_display_name(requested);
    let denied = format!("COPY file path is not permitted: {name}");
    check_copy_path_syntax(requested)?;
    let candidate = jail.join(requested);
    // An existing file (or symlink) is canonicalized itself so every
    // symlink resolves to its real location before the jail check.
    if let Ok(meta) = std::fs::symlink_metadata(&candidate) {
        if meta.is_dir() {
            return Err(denied);
        }
        let canonical = std::fs::canonicalize(&candidate).map_err(|_| denied.clone())?;
        if !canonical.starts_with(jail) {
            return Err(denied);
        }
        // A symlink that resolves to a directory is still a directory.
        if std::fs::symlink_metadata(&canonical)
            .map(|m| m.is_dir())
            .unwrap_or(false)
        {
            return Err(denied);
        }
        return Ok(canonical);
    }
    // A new destination resolves through its parent, which must already
    // exist inside the jail, and the leaf must be a plain file name.
    let parent = candidate.parent().unwrap_or(jail);
    let canonical_parent = std::fs::canonicalize(parent).map_err(|_| denied.clone())?;
    if !canonical_parent.starts_with(jail) {
        return Err(denied);
    }
    let leaf = candidate.file_name().ok_or_else(|| denied.clone())?;
    if leaf.is_empty() {
        return Err(denied);
    }
    let validated = canonical_parent.join(leaf);
    // A leaf colliding with an existing directory must not become a
    // directory open (which would fail with a confusing EISDIR instead of
    // the jail denial).
    if std::fs::symlink_metadata(&validated)
        .map(|m| m.is_dir())
        .unwrap_or(false)
    {
        return Err(denied);
    }
    Ok(validated)
}

/// Whether an I/O error is a symlink-escape refusal (`ELOOP` from an
/// `O_NOFOLLOW` open). Callers translate this to the jail denial, never to
/// a filesystem error message (which could echo attacker paths).
#[cfg(unix)]
fn is_symlink_escape(error: &std::io::Error) -> bool {
    error.raw_os_error() == Some(libc::ELOOP)
}
/// Opens a validated jail path without following symlinks (Unix).
///
/// `jail` must be the canonical jail root and `validated` the output of
/// [`resolve_copy_path`] for it. The validated path is re-expressed
/// relative to the jail; every ancestor directory is opened with
/// `O_NOFOLLOW | O_DIRECTORY` (a symlink anywhere in the chain fails
/// closed with `ELOOP`), and the leaf opens with `O_NOFOLLOW |
/// O_NONBLOCK`, so a symlink planted between validation and use cannot
/// escape the jail and a planted FIFO can never block the connection.
/// Ancestor handles are held open across the leaf open and the parent is
/// re-validated by device/inode afterwards, shrinking the residual
/// ancestor-swap window to adjacent syscalls inside one `blocking()`
/// closure with no `.await` (previously a validate-then-`SELECT`-then-use
/// gap). The leaf is `fstat`ed and must be a regular file: directories,
/// FIFOs, sockets, and devices are denied. Non-Unix callers use plain
/// opens (documented residual: pathname validation only).
///
/// Write path preserves historical overwrite semantics without ever
/// following a symlink: `create_new` (`O_EXCL`, mode `0600`) wins the
/// create-vs-plant race atomically; a pre-existing path opens *without*
/// truncate, is `fstat`ed for regular-file, then truncated via the fd
/// (`ftruncate`) so a non-regular never sees a truncate. Created and
/// overwritten files are tightened to `0600` (table contents can be
/// sensitive; the pre-existing umask-default `0644` leaked them).
///
/// No `unsafe`: all opens go through `std::fs::OpenOptions` with
/// `O_NOFOLLOW`/`O_DIRECTORY`/`O_NONBLOCK` supplied as data (`libc`
/// constants only), keeping the crate's `forbid(unsafe_code)` intact.
/// True `openat2`-style descriptor-relative pinning would need a safe
/// `openat` wrapper (e.g. `rustix`); the post-open dev/ino revalidation
/// below is the std-only approximation — see the residual note in
/// [`read_pinned_file`].
#[cfg(unix)]
fn open_pinned(
    jail: &std::path::Path,
    validated: &std::path::Path,
    write: bool,
) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::MetadataExt as _;
    use std::os::unix::fs::OpenOptionsExt as _;
    use std::os::unix::fs::PermissionsExt as _;

    fn denied_io() -> std::io::Error {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "COPY file path is not permitted",
        )
    }

    fn map_leaf_error(path: &std::path::Path, error: std::io::Error) -> std::io::Error {
        if error.raw_os_error() == Some(libc::ELOOP) {
            return denied_io();
        }
        // A leaf that exists as anything but a regular file (directory →
        // EISDIR, FIFO-with-no-reader → ENXIO, socket/device, symlink) is a
        // jail denial, never a filesystem error: probing special files must
        // not leak type-dependent errno shapes either.
        if let Ok(meta) = std::fs::symlink_metadata(path) {
            let ft = meta.file_type();
            if ft.is_symlink() || ft.is_dir() || !ft.is_file() {
                return denied_io();
            }
        }
        // ENXIO/EISDIR without visible metadata (raced unlink) still
        // indicates a special-file probe — deny rather than leak errno.
        if matches!(
            error.raw_os_error(),
            Some(e) if e == libc::ENXIO || e == libc::EISDIR
        ) {
            return denied_io();
        }
        error
    }

    let nofollow: libc::c_int = libc::O_NOFOLLOW;
    let dir_flag: libc::c_int = libc::O_DIRECTORY;
    let nonblock: libc::c_int = libc::O_NONBLOCK;
    let rel = validated.strip_prefix(jail).map_err(|_| denied_io())?;
    let mut parts: Vec<&std::ffi::OsStr> = rel
        .components()
        .filter_map(|component| match component {
            // Canonical validated paths carry no `.`/`..`/prefix/root.
            std::path::Component::Normal(part) => Some(part),
            _ => None,
        })
        .collect();
    let leaf = parts.pop().ok_or_else(denied_io)?;

    // Prove every ancestor is a real directory (never a symlink) before
    // touching the leaf. Each open is `O_NOFOLLOW | O_DIRECTORY`: a link
    // fails closed instead of being traversed. Handles are *held* across
    // the leaf open so the parent can be re-validated by dev/ino below.
    // Platforms report that refusal as either `ELOOP` or `ENOTDIR`; a
    // failed component that is in fact a link is always translated to the
    // jail denial, while any other I/O error propagates untouched.
    fn is_link(path: &std::path::Path) -> bool {
        std::fs::symlink_metadata(path)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(false)
    }
    let mut current = jail.to_path_buf();
    let mut held = Vec::new();
    let jail_fd = std::fs::File::options()
        .read(true)
        .custom_flags(nofollow | dir_flag)
        .open(&current)
        .map_err(|error| {
            if is_link(&current) {
                denied_io()
            } else {
                error
            }
        })?;
    held.push(jail_fd);
    for part in &parts {
        current.push(part);
        match std::fs::File::options()
            .read(true)
            .custom_flags(nofollow | dir_flag)
            .open(&current)
        {
            Ok(fd) => held.push(fd),
            Err(error) => {
                if is_link(&current) {
                    return Err(denied_io());
                }
                return Err(error);
            }
        }
    }
    current.push(leaf);
    let file = if !write {
        std::fs::File::options()
            .read(true)
            .custom_flags(nofollow | nonblock)
            .open(&current)
            .map_err(|error| map_leaf_error(&current, error))?
    } else {
        // Create-or-truncate preserving historical overwrite semantics,
        // never following a symlink: `create_new` (`O_EXCL`) wins the
        // create-vs-plant race atomically, and a pre-existing symlink
        // fails closed instead of being followed or replaced.
        match std::fs::File::options()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .custom_flags(nofollow | nonblock)
            .open(&current)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                // Open WITHOUT truncate first so the type check below runs
                // before any truncation: a FIFO/device must never see
                // `O_TRUNC` side effects.
                let file = std::fs::File::options()
                    .write(true)
                    .custom_flags(nofollow | nonblock)
                    .open(&current)
                    .map_err(|error| map_leaf_error(&current, error))?;
                let meta = file.metadata()?;
                if !meta.file_type().is_file() {
                    return Err(denied_io());
                }
                // Truncate via the fd (no path re-resolution).
                file.set_len(0)?;
                file
            }
            Err(error) => return Err(map_leaf_error(&current, error)),
        }
    };
    // `fstat`, not path stat: the fd is what we read/write. Anything but a
    // regular file (directory, FIFO, socket, device) is denied — in
    // particular a FIFO opened `O_NONBLOCK` above succeeds without
    // blocking and is refused here instead of hanging the connection.
    let meta = file.metadata()?;
    if !meta.file_type().is_file() {
        return Err(denied_io());
    }
    // Ancestor-swap detection: the held parent fd must still be the
    // canonical parent on disk. A `rename`/`symlink`-swap of any ancestor
    // between the walk and the leaf open changes either the jail-prefix
    // check or the (dev, ino) identity.
    if let Some(parent) = current.parent() {
        let canonical_parent = std::fs::canonicalize(parent).map_err(|_| denied_io())?;
        if !canonical_parent.starts_with(jail) {
            return Err(denied_io());
        }
        let fresh = std::fs::symlink_metadata(&canonical_parent).map_err(|_| denied_io())?;
        if let Some(held_parent) = held.last() {
            if let Ok(held_meta) = held_parent.metadata() {
                if (held_meta.dev(), held_meta.ino()) != (fresh.dev(), fresh.ino()) {
                    return Err(denied_io());
                }
            }
        }
    }
    if write {
        // Tighten pre-existing files (which may carry the old umask-default
        // mode) and assert the mode on created ones.
        let _ = file.set_permissions(std::fs::Permissions::from_mode(0o600));
    }
    // `held` drops here: the leaf fd already pins the file; ancestors are
    // no longer needed after the dev/ino check above.
    let _ = held;
    Ok(file)
}

/// Reads a validated jail file through a symlink-pinned open (Unix).
///
/// The open, not a pathname, is what gets read: swaps after validation
/// cannot redirect it. Reads are capped at `limit` bytes (fail closed)
/// instead of unbounded `read_to_end`. Non-Unix callers fall back to
/// metadata-checked reads (regular-file + size cap + jail prefix, no
/// `O_NOFOLLOW` pinning — documented residual: pathname validation only).
pub fn read_pinned_file(
    jail: &std::path::Path,
    validated: &std::path::Path,
    limit: usize,
) -> std::io::Result<Vec<u8>> {
    #[cfg(unix)]
    {
        use std::io::Read as _;
        let file = open_pinned(jail, validated, false)?;
        let len = file.metadata()?.len();
        if len > limit as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "COPY file is too large",
            ));
        }
        let mut data = Vec::new();
        file.take(limit as u64 + 1).read_to_end(&mut data)?;
        if data.len() > limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "COPY file is too large",
            ));
        }
        Ok(data)
    }
    #[cfg(not(unix))]
    {
        let meta = std::fs::symlink_metadata(validated)?;
        if !meta.file_type().is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "COPY file path is not permitted",
            ));
        }
        if meta.len() > limit as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "COPY file is too large",
            ));
        }
        if !validated.starts_with(jail) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "COPY file path is not permitted",
            ));
        }
        std::fs::read(validated)
    }
}

/// Writes a validated jail file through a symlink-pinned open (Unix).
///
/// Create-or-truncate preserves the historical overwrite semantics without
/// ever following a symlink (see `open_pinned`); files are created/tightened
/// to `0600`. Durability matches the historical path: no fsync here — COPY
/// TO durability comes from later checkpoints, not the copy itself.
/// Non-Unix callers fall back to plain writes (documented residual).
pub fn write_pinned_file(
    jail: &std::path::Path,
    validated: &std::path::Path,
    data: &[u8],
) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write as _;
        if data.len() > MAX_COPY_FILE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "COPY output is too large",
            ));
        }
        let mut file = open_pinned(jail, validated, true)?;
        file.write_all(data)
    }
    #[cfg(not(unix))]
    {
        let _ = jail;
        std::fs::write(validated, data)
    }
}

fn parse_copy_delimiter(options: &str, format: CopyFileFormat) -> Option<u8> {
    let lower = options.to_ascii_lowercase();
    let Some(index) = lower.find("delimiter") else {
        return Some(match format {
            CopyFileFormat::Text => b'\t',
            CopyFileFormat::Csv => b',',
        });
    };
    let quoted = options[index..].split('\'').nth(1)?;
    let mut chars = quoted.chars();
    let value = match chars.next()? {
        '\\' => match chars.next()? {
            't' => b'\t',
            'n' => b'\n',
            other => other as u8,
        },
        ch => ch as u8,
    };
    Some(value)
}

fn parse_copy_file_rows(data: &[u8], spec: &CopyFileSpec) -> Result<Vec<Vec<String>>, String> {
    let text = std::str::from_utf8(data).map_err(|_| "input is not UTF-8")?;
    let mut rows = match spec.format {
        CopyFileFormat::Text => text
            .lines()
            .map(|line| {
                line.split(spec.delimiter as char)
                    .map(|field| {
                        if field == r"\N" {
                            "NULL".to_string()
                        } else {
                            copy_sql_value(&field.replace(r"\t", "\t").replace(r"\n", "\n"))
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>(),
        CopyFileFormat::Csv => parse_csv_rows(text, spec.delimiter)?,
    };
    if spec.header && !rows.is_empty() {
        rows.remove(0);
    }
    Ok(rows)
}

fn parse_csv_rows(input: &str, delimiter: u8) -> Result<Vec<Vec<String>>, String> {
    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut field = String::new();
    let mut quoted = false;
    let bytes = input.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if quoted {
            match byte {
                b'"' if bytes.get(index + 1) == Some(&b'"') => {
                    field.push('"');
                    index += 1;
                }
                b'"' => quoted = false,
                other => field.push(other as char),
            }
        } else {
            match byte {
                b'"' if field.is_empty() => quoted = true,
                b if b == delimiter => {
                    row.push(copy_sql_value(&field));
                    field.clear();
                }
                b'\n' => {
                    row.push(copy_sql_value(&field));
                    field.clear();
                    rows.push(std::mem::take(&mut row));
                }
                b'\r' => {}
                other => field.push(other as char),
            }
        }
        index += 1;
    }
    if quoted {
        return Err("unterminated CSV quoted field".to_string());
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(copy_sql_value(&field));
        rows.push(row);
    }
    Ok(rows)
}

fn copy_encode_file_row(
    row: &[plomid_sql::Value],
    output: &mut Vec<u8>,
    format: CopyFileFormat,
    delimiter: u8,
) {
    for (index, value) in row.iter().enumerate() {
        if index != 0 {
            output.push(delimiter);
        }
        let text = if matches!(value, plomid_sql::Value::Null) {
            String::new()
        } else {
            value.to_sql_text()
        };
        if format == CopyFileFormat::Csv
            && (text.contains(',') || text.contains('"') || text.contains('\n'))
        {
            output.push(b'"');
            output.extend_from_slice(text.replace('"', "\"\"").as_bytes());
            output.push(b'"');
        } else {
            output.extend_from_slice(text.as_bytes());
        }
    }
    output.push(b'\n');
}

fn encode_result_value(
    value: &plomid_sql::Value,
    format: u16,
    expected_oid: Option<u32>,
) -> Vec<u8> {
    if format == 0 {
        let text = match value {
            // Numeric is retained internally with scale for arithmetic and
            // executor compatibility. PostgreSQL's text output omits an
            // unnecessary fractional zero suffix for integral EXTRACT values.
            plomid_sql::Value::Numeric(_) => canonical_numeric_text(&value.to_sql_text()),
            _ => value.to_sql_text(),
        };
        return text.into_bytes();
    }
    match value {
        plomid_sql::Value::Text(text) if expected_oid == Some(23) => text
            .parse::<i32>()
            .unwrap_or_default()
            .to_be_bytes()
            .to_vec(),
        plomid_sql::Value::Vector(vector) => {
            // Binary vector payload: dense `dim * 4` LE `f32` lanes (sparse:
            // `nnz * (u32 index + f32 value)`; binary: packed bits) per the
            // row-payload codec. Encode failures fall back to the canonical
            // text literal so the row is never dropped.
            plomid_vector::pgwire_binary_encode(vector)
                .unwrap_or_else(|_| value.to_sql_text().into_bytes())
        }
        plomid_sql::Value::Bool(value) => vec![u8::from(*value)],
        plomid_sql::Value::Int2(value) => value.to_be_bytes().to_vec(),
        plomid_sql::Value::Int4(value) => value.to_be_bytes().to_vec(),
        plomid_sql::Value::Int8(value) => value.to_be_bytes().to_vec(),
        plomid_sql::Value::Float4(value) => value.to_bits().to_be_bytes().to_vec(),
        plomid_sql::Value::Float8(value) => value.to_bits().to_be_bytes().to_vec(),
        plomid_sql::Value::Oid(value) => value.to_be_bytes().to_vec(),
        plomid_sql::Value::Date(value) => value.to_be_bytes().to_vec(),
        plomid_sql::Value::Time(value)
        | plomid_sql::Value::Timestamp(value)
        | plomid_sql::Value::Timestamptz(value) => value.to_be_bytes().to_vec(),
        plomid_sql::Value::Bytea(value) => value.clone(),
        // PostgreSQL's binary representation for these types is not exposed
        // by the current type facade; use their canonical text payload.
        other => other.to_sql_text().into_bytes(),
    }
}

fn canonical_numeric_text(text: &str) -> String {
    let Some(dot) = text.find('.') else {
        return text.to_string();
    };
    let mut result = text[..dot + 1].to_string();
    result.push_str(text[dot + 1..].trim_end_matches('0'));
    if result.ends_with('.') {
        result.pop();
    }
    if result == "-0" {
        "0".to_string()
    } else {
        result
    }
}

fn wire_format(requested: u16, oid: Option<u32>) -> u16 {
    if requested == 1 && !oid.is_some_and(supports_binary_oid) {
        0
    } else {
        requested
    }
}

fn supports_binary_oid(oid: u32) -> bool {
    matches!(
        oid,
        16 | 17
            | 20
            | 21
            | 23
            | 25
            | 700
            | 701
            | 1042
            | 1043
            | 1082
            | 1083
            | 1114
            | 1184
            | 26
            | 1700
            | 2950
            // Vector row-payload codec (`pgwire_binary_encode`); the column
            // typmod carries kind/element/dimension for decoding.
            | 16384
    )
}

fn decode_parameter(bytes: &[u8], format: u16, oid: u32) -> std::io::Result<String> {
    if format == 0 {
        return Ok(String::from_utf8_lossy(bytes).into_owned());
    }
    let invalid = || protocol_error("unsupported or malformed binary parameter");
    match oid {
        16 if bytes.len() == 1 => Ok(if bytes[0] == 0 { "false" } else { "true" }.into()),
        21 if bytes.len() == 2 => Ok(i16::from_be_bytes([bytes[0], bytes[1]]).to_string()),
        23 if bytes.len() == 4 => {
            Ok(i32::from_be_bytes(bytes.try_into().map_err(|_| invalid())?).to_string())
        }
        26 if bytes.len() == 4 => {
            Ok(u32::from_be_bytes(bytes.try_into().map_err(|_| invalid())?).to_string())
        }
        20 if bytes.len() == 8 => {
            Ok(i64::from_be_bytes(bytes.try_into().map_err(|_| invalid())?).to_string())
        }
        700 if bytes.len() == 4 => {
            let bits = u32::from_be_bytes(bytes.try_into().map_err(|_| invalid())?);
            Ok(f32::from_bits(bits).to_string())
        }
        701 if bytes.len() == 8 => {
            let bits = u64::from_be_bytes(bytes.try_into().map_err(|_| invalid())?);
            Ok(f64::from_bits(bits).to_string())
        }
        1082 if bytes.len() == 4 => {
            let days = i32::from_be_bytes(bytes.try_into().map_err(|_| invalid())?);
            Ok(plomid_types::datetime::format_date(days))
        }
        1083 if bytes.len() == 8 => {
            let micros = i64::from_be_bytes(bytes.try_into().map_err(|_| invalid())?);
            Ok(plomid_types::datetime::format_time(micros, None))
        }
        1114 if bytes.len() == 8 => {
            let micros = i64::from_be_bytes(bytes.try_into().map_err(|_| invalid())?);
            Ok(plomid_types::datetime::format_timestamp(micros, None))
        }
        1184 if bytes.len() == 8 => {
            let micros = i64::from_be_bytes(bytes.try_into().map_err(|_| invalid())?);
            Ok(plomid_types::datetime::format_timestamp(micros, None))
        }
        17 => {
            let mut hex = String::with_capacity(2 + bytes.len() * 2);
            hex.push_str("\\x");
            for byte in bytes {
                use std::fmt::Write;
                let _ = write!(hex, "{byte:02x}");
            }
            Ok(hex)
        }
        2950 if bytes.len() == 16 => {
            let b = bytes;
            use std::fmt::Write;
            let mut s = String::with_capacity(36);
            let _ = write!(
                s,
                "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                b[0], b[1], b[2], b[3],
                b[4], b[5],
                b[6], b[7],
                b[8], b[9],
                b[10], b[11], b[12], b[13], b[14], b[15],
            );
            Ok(s)
        }
        1700 => decode_binary_numeric(bytes).ok_or_else(invalid),
        // Binary vector parameters carry a payload-only codec with no
        // self-describing header: without the target column's
        // kind/element/dimension (which this site does not receive) they
        // cannot be decoded safely. Fail closed with a clear error instead
        // of the previous lossy UTF-8 passthrough; clients send vectors in
        // text format (`[0.1,0.2]`).
        16384 => Err(protocol_error(
            "binary vector parameters are not supported; send vectors in text format",
        )),
        25 | 1043 | 1042 | 114 | 3802 => Ok(String::from_utf8_lossy(bytes).into_owned()),
        _ => Err(invalid()),
    }
}

fn decode_binary_numeric(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 8 {
        return None;
    }
    let read_i16 = |offset: usize| -> Option<i16> {
        let end = offset.checked_add(2)?;
        Some(i16::from_be_bytes(bytes.get(offset..end)?.try_into().ok()?))
    };
    let ndigits = usize::try_from(read_i16(0)?).ok()?;
    let weight = i32::from(read_i16(2)?);
    let sign = u16::from_be_bytes(bytes.get(4..6)?.try_into().ok()?);
    let dscale = usize::try_from(read_i16(6)?).ok()?;
    if sign == 0xC000 {
        return Some("NaN".to_string());
    }
    if sign != 0 && sign != 0x4000 || bytes.len() != 8 + ndigits * 2 {
        return None;
    }
    let mut groups = Vec::with_capacity(ndigits);
    for index in 0..ndigits {
        let digit = u16::from_be_bytes(bytes[8 + index * 2..10 + index * 2].try_into().ok()?);
        if digit >= 10_000 {
            return None;
        }
        groups.push(digit);
    }
    if groups.iter().all(|digit| *digit == 0) {
        return Some(if dscale == 0 {
            "0".to_string()
        } else {
            format!("0.{}", "0".repeat(dscale))
        });
    }
    let first_len = groups.first().map_or(1, |digit| digit.to_string().len());
    let mut digits = groups
        .iter()
        .enumerate()
        .map(|(index, digit)| {
            if index == 0 {
                digit.to_string()
            } else {
                format!("{digit:04}")
            }
        })
        .collect::<String>();
    let decimal_pos = first_len as i32 + weight * 4;
    if decimal_pos <= 0 {
        digits = format!("0.{}{}", "0".repeat((-decimal_pos) as usize), digits);
    } else if decimal_pos as usize >= digits.len() {
        digits.push_str(&"0".repeat(decimal_pos as usize - digits.len()));
    } else {
        digits.insert(decimal_pos as usize, '.');
    }
    if let Some(dot) = digits.find('.') {
        let actual_scale = digits.len() - dot - 1;
        if actual_scale < dscale {
            digits.push_str(&"0".repeat(dscale - actual_scale));
        } else if actual_scale > dscale {
            digits.truncate(dot + 1 + dscale);
        }
    }
    if sign == 0x4000 {
        digits.insert(0, '-');
    }
    Some(digits)
}

/// Maps an internal PLOMID error to a PostgreSQL SQLSTATE.
///
/// The classification is derived from the internal error kind plus the error's
/// own class keywords (constraint, object type), mirroring how PostgreSQL
/// assigns errcodes from error classes. No client or query introspection is
/// involved.
pub fn sqlstate_for(kind: ErrorKind, message: &str) -> &'static str {
    let message = message.to_ascii_lowercase();
    match kind {
        ErrorKind::Syntax => "42601",
        ErrorKind::Catalog | ErrorKind::NotFound => {
            if message.contains("database") {
                "3D000"
            } else if message.contains("column") {
                "42703"
            } else if message.contains("function") || message.contains("procedure") {
                "42883"
            } else {
                "42P01"
            }
        }
        ErrorKind::AlreadyExists => {
            if message.contains("database") {
                "42P04"
            } else {
                "42710"
            }
        }
        ErrorKind::Conflict => {
            if message.contains("null value") || message.contains("not-null") {
                "23502"
            } else if message.contains("unique") || message.contains("duplicate") {
                "23505"
            } else if message.contains("check") {
                "23514"
            } else if message.contains("foreign") {
                "23503"
            } else if message.contains("aborted") || message.contains("commands ignored") {
                "25P02"
            } else {
                "40001"
            }
        }
        ErrorKind::Transaction | ErrorKind::Aborted => "25P02",
        ErrorKind::InvalidArgument => {
            if message.contains("division by zero") {
                "22012"
            } else if message.contains("out of range") {
                "22003"
            } else {
                "22023"
            }
        }
        ErrorKind::RowEncoding => "22000",
        ErrorKind::PermissionDenied => "42501",
        ErrorKind::Unsupported => "0A000",
        ErrorKind::Io => {
            if message.contains("no such file") || message.contains("not found") {
                "58P01"
            } else {
                "58000"
            }
        }
        ErrorKind::Wal => "58000",
        ErrorKind::Corruption => "XX001",
        ErrorKind::Internal => "XX000",
        _ => "XX000",
    }
}

/// Builds the wire error for an executor failure, translating the internal
/// error code into a PostgreSQL SQLSTATE and carrying the internal diagnostic
/// context as the `DETAIL` field.
///
/// Server-absolute-path prefixes are stripped from `DETAIL`: the data-dir
/// prefix becomes `<datadir>` and any other absolute path becomes `<path>`,
/// so the wire never echoes the server filesystem layout. Message codes are
/// untouched.
#[allow(dead_code)]
fn executor_error_response(err: SqlError) -> ErrorResponse {
    executor_error_response_with_datadir(err, None)
}

pub fn executor_error_response_with_datadir(
    err: SqlError,
    data_dir: Option<&std::path::Path>,
) -> ErrorResponse {
    let (kind, message, detail) = match err {
        SqlError::Syntax(plomid) => (
            plomid.kind(),
            plomid.message().to_owned(),
            plomid.detail().map(str::to_owned),
        ),
        SqlError::Storage(plomid) => (
            plomid.kind(),
            plomid.message().to_owned(),
            plomid.detail().map(str::to_owned),
        ),
    };
    let code = sqlstate_for(kind, &message);
    let mut response = ErrorResponse::new("ERROR", code, message);
    if let Some(detail) = detail {
        response = response.with_detail(redact_detail(&detail, data_dir));
    }
    response
}

/// Redacts server filesystem paths from a wire `DETAIL` string.
pub fn redact_detail(detail: &str, data_dir: Option<&std::path::Path>) -> String {
    let mut redacted = detail.to_owned();
    if let Some(dir) = data_dir {
        let raw = dir.to_string_lossy().into_owned();
        if !raw.is_empty() {
            redacted = redacted.replace(raw.as_str(), "<datadir>");
        }
        if let Ok(canonical) = blocking(|| std::fs::canonicalize(dir)) {
            let canon = canonical.to_string_lossy().into_owned();
            if !canon.is_empty() && canon != raw {
                redacted = redacted.replace(canon.as_str(), "<datadir>");
            }
        }
    }
    redact_absolute_paths(&redacted)
}

/// Replaces remaining Unix-absolute-path substrings with `<path>`.
fn redact_absolute_paths(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'/' && i + 1 < bytes.len() && is_path_char(bytes[i + 1]) {
            let mut j = i + 1;
            while j < bytes.len() && is_path_char(bytes[j]) {
                j += 1;
            }
            // Require something path-like (a slash, dot, or at least a few
            // chars) so a bare "a/b" fragment or division never redacts.
            let token = &input[i..j];
            if token.contains('/') || token.contains('.') || token.len() > 8 {
                out.push_str("<path>");
            } else {
                out.push_str(token);
            }
            i = j;
        } else {
            // Preserve UTF-8 boundaries by pushing the next char.
            let ch = input[i..].chars().next().unwrap_or('?');
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    out
}

fn is_path_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'/' | b'+')
}

/// Returns true when a query returns a result set whose rows may be capped by
/// the extended-protocol Execute `max_rows`.
///
/// PostgreSQL's `max_rows` only caps the number of rows a portal *returns*; it
/// is never spliced into the SQL text and is ignored for statements that do not
/// produce a result set. Splicing ` LIMIT n` onto DDL/DML (e.g.
/// `CREATE SCHEMA x` -> `CREATE SCHEMA x LIMIT n`) is a syntax error, so the
/// projector must only be applied to genuinely row-returning queries.
fn should_apply_result_limit(sql: &str) -> bool {
    matches!(
        statement_type(sql),
        "SELECT" | "VALUES" | "SHOW" | "EXPLAIN" | "DESCRIBE"
    ) || sql
        .split_whitespace()
        .next()
        .map(|word| word.trim_end_matches(';').eq_ignore_ascii_case("WITH"))
        .unwrap_or(false)
}

pub fn bind_query(query: &str, parameters: &[Option<String>]) -> String {
    // Single-pass substitution. The previous implementation called
    // `str::replace` once per parameter, which corrupted multi-digit
    // placeholders (`$10` became `<value-of-$1>0`) and rewrote `$N` tokens
    // inside string literals. Placeholders inside literals, quoted
    // identifiers, dollar-quoted bodies, and comments are left intact; a
    // missing value renders as NULL, matching the old behavior for Bind NULLs.
    // The output is assembled as bytes so non-ASCII query text passes through
    // untouched (placeholder syntax is pure ASCII, so every slice boundary is
    // a char boundary).
    let bytes = query.as_bytes();
    let mut bound: Vec<u8> = Vec::with_capacity(query.len() + parameters.len() * 4);
    let mut index = 0usize;
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_dollar = false;
    while index < bytes.len() {
        let ch = bytes[index];
        if in_line_comment {
            bound.push(ch);
            if ch == b'\n' {
                in_line_comment = false;
            }
            index += 1;
            continue;
        }
        if in_dollar {
            if ch == b'$' && index + 1 < bytes.len() && bytes[index + 1] == b'$' {
                bound.extend_from_slice(b"$$");
                in_dollar = false;
                index += 2;
            } else {
                bound.push(ch);
                index += 1;
            }
            continue;
        }
        if in_single {
            bound.push(ch);
            if ch == b'\'' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    bound.push(b'\'');
                    index += 2;
                } else {
                    in_single = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        if in_double {
            bound.push(ch);
            if ch == b'"' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'"' {
                    bound.push(b'"');
                    index += 2;
                } else {
                    in_double = false;
                    index += 1;
                }
            } else {
                index += 1;
            }
            continue;
        }
        match ch {
            b'-' if index + 1 < bytes.len() && bytes[index + 1] == b'-' => {
                bound.extend_from_slice(b"--");
                in_line_comment = true;
                index += 2;
            }
            b'\'' => {
                bound.push(b'\'');
                in_single = true;
                index += 1;
            }
            b'"' => {
                bound.push(b'"');
                in_double = true;
                index += 1;
            }
            b'$' if index + 1 < bytes.len() && bytes[index + 1] == b'$' => {
                bound.extend_from_slice(b"$$");
                in_dollar = true;
                index += 2;
            }
            b'$' => {
                let mut end = index + 1;
                while end < bytes.len() && bytes[end].is_ascii_digit() {
                    end += 1;
                }
                if end > index + 1 {
                    if let Ok(number) = query[index + 1..end].parse::<usize>() {
                        if number >= 1 {
                            if let Some(value) = parameters.get(number - 1) {
                                bound.extend_from_slice(
                                    value
                                        .as_deref()
                                        .map(quote_bound_value)
                                        .unwrap_or_else(|| "NULL".to_string())
                                        .as_bytes(),
                                );
                            } else {
                                bound.extend_from_slice(&bytes[index..end]);
                            }
                            index = end;
                            continue;
                        }
                    }
                    bound.extend_from_slice(&bytes[index..end]);
                    index = end;
                } else {
                    bound.push(ch);
                    index += 1;
                }
            }
            _ => {
                bound.push(ch);
                index += 1;
            }
        }
    }
    String::from_utf8(bound).unwrap_or_else(|_| query.to_string())
}

fn explicit_parameter_cast(query: &str, parameter: usize) -> Option<u32> {
    let marker = format!("${parameter}");
    let lower = query.to_ascii_lowercase();
    let start = lower.find(&marker)? + marker.len();
    let suffix = lower[start..].trim_start();
    let suffix = suffix.strip_prefix("::")?.trim_start();
    let type_name = suffix
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .next()
        .filter(|name| !name.is_empty())?;
    let is_array = suffix[type_name.len()..].starts_with("[]");
    let ty = plomid_types::PgType::by_name(type_name)?;
    if is_array {
        ty.array_oid().map(|oid| oid.0)
    } else {
        Some(ty.oid().0)
    }
}

fn parse_execute(payload: &[u8]) -> std::io::Result<(String, u32)> {
    let (portal, rest) = read_cstring(payload, "portal name")?;
    if rest.len() != 4 {
        return Err(protocol_error("invalid Execute message"));
    }
    Ok((
        portal,
        u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]),
    ))
}

fn parse_close(payload: &[u8]) -> std::io::Result<Option<(u8, String)>> {
    let (kind, rest) = payload
        .split_first()
        .ok_or_else(|| protocol_error("truncated Close message"))?;
    let (name, _) = read_cstring(rest, "Close name")?;
    Ok(Some((*kind, name)))
}

fn show_parameter_name(sql: &str) -> Option<String> {
    let words = sql
        .trim()
        .trim_end_matches(';')
        .split_whitespace()
        .collect::<Vec<_>>();
    if words.len() == 2 && words[0].eq_ignore_ascii_case("show") {
        if matches!(
            words[1].to_ascii_lowercase().as_str(),
            "tables" | "table" | "database" | "databases" | "schema" | "schemas" | "schema_name"
        ) {
            None
        } else {
            Some(words[1].to_string())
        }
    } else {
        None
    }
}

/// Returns true when a Describe can safely execute the statement to infer
/// result metadata. Only read-only result-producing queries (SELECT, set
/// operations, EXPLAIN, VALUES) qualify. Transaction-control statements
/// (BEGIN/COMMIT/ROLLBACK/SAVEPOINT/RELEASE) and DML (INSERT/UPDATE/DELETE)
/// must never be run during Describe: transaction statements have no result
/// set, and executing DML would either double-apply writes or, for BEGIN,
/// raise "BEGIN without COMMIT or ROLLBACK" from the executor.
fn is_describe_query(stmt: &str) -> bool {
    // SHOW is read-only and returns a result set (e.g. "search_path"), so it
    // is safe to describe by executing.
    matches!(stmt, "SELECT" | "EXPLAIN" | "VALUES" | "SHOW")
}

/// Executes a Describe shape probe: runs just enough of the statement to
/// learn result-column names/types, without scanning rows.
///
/// For SELECT/VALUES the probe appends `LIMIT 0` to the unchanged statement:
/// identical SELECT list (hence identical column order, names, and types),
/// but the executor may stop before reading rows. That matters because the
/// probe binds NULL placeholders — e.g. `WHERE id = NULL` matches nothing
/// yet still scans every row to prove it, so each Describe of a prepared
/// point lookup paid a full table scan (measured ~4 ms cold on a 1.7k-row
/// table, scaling with table size). Appending (rather than subquery
/// wrapping, which demonstrably drops expression type metadata: `::DATE`
/// came back unknown) keeps the exact plan nodes. EXPLAIN/SHOW keep direct
/// execution: EXPLAIN ANALYZE must really execute, and SHOW is a single
/// system row. Statements that already carry a LIMIT/OFFSET or that reject
/// the suffix fall back to direct execution — today's behavior — so Describe
/// can never regress by trying to be cheaper.
fn describe_shape(
    engine: &mut Executor<ConcurrentPlomidStorageEngine>,
    sql: &str,
    stmt: &str,
    search_path: &[String],
) -> plomid_sql::SqlResult<plomid_sql::QueryResult> {
    if stmt == "SELECT" || stmt == "VALUES" {
        let trimmed = sql.trim().trim_end_matches(';').trim_end();
        // Only probe when the statement has no row cap of its own; parsing
        // SQL here to strip an existing LIMIT would risk changing semantics,
        // so those (already bounded) statements keep direct execution.
        if !has_row_cap(trimmed) {
            let probed = format!("{trimmed} LIMIT 0");
            match execute_describe_sql(engine, &probed, search_path) {
                ok @ Ok(_) => return ok,
                Err(_) => { /* fall through to direct execution below */ }
            }
        }
    }
    execute_describe_sql(engine, sql, search_path)
}

/// Runs one shape probe under the session search path. Describe must resolve
/// unqualified names exactly as execution would; `execute` uses the catalog's
/// persistent (default) path and would describe a different table when the
/// session has a custom `search_path`.
fn execute_describe_sql(
    engine: &mut Executor<ConcurrentPlomidStorageEngine>,
    sql: &str,
    search_path: &[String],
) -> plomid_sql::SqlResult<plomid_sql::QueryResult> {
    let (mut results, _) = engine.execute_all_with_search_path(sql, search_path)?;
    Ok(results
        .pop()
        .unwrap_or_else(|| plomid_sql::QueryResult::Created(String::new())))
}

/// True when the statement text already bounds its own row count, making a
/// probe suffix redundant or invalid.
fn has_row_cap(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    upper.contains("LIMIT") || upper.contains("OFFSET") || upper.contains("FETCH")
}

fn set_parameter(sql: &str) -> Option<(String, String)> {
    let body = sql.trim().trim_end_matches(';');
    let mut words = body.split_whitespace().peekable();
    if !words.next()?.eq_ignore_ascii_case("set") {
        return None;
    }
    if words.peek().is_some_and(|word| {
        word.eq_ignore_ascii_case("session") || word.eq_ignore_ascii_case("local")
    }) {
        words.next();
    }
    let first = words.next()?.to_string();
    if first.eq_ignore_ascii_case("transaction") {
        return parse_set_transaction_options(words);
    }
    let mut name = first;
    if name.eq_ignore_ascii_case("characteristics")
        && words
            .peek()
            .is_some_and(|word| word.eq_ignore_ascii_case("as"))
    {
        words.next();
        if words
            .peek()
            .is_some_and(|word| word.eq_ignore_ascii_case("transaction"))
        {
            words.next();
        }
        return parse_set_transaction_options(words);
    }
    if name.eq_ignore_ascii_case("time")
        && words
            .peek()
            .is_some_and(|word| word.eq_ignore_ascii_case("zone"))
    {
        words.next();
        name = "TimeZone".to_string();
    } else if name.eq_ignore_ascii_case("names") {
        name = "client_encoding".to_string();
    }
    let operator = words.next()?;
    if operator != "=" && !operator.eq_ignore_ascii_case("to") {
        return None;
    }
    let value = words
        .collect::<Vec<_>>()
        .join(" ")
        .trim_matches('\'')
        .to_string();
    Some((name, value))
}

fn parse_set_transaction_options<'a, I>(mut words: I) -> Option<(String, String)>
where
    I: Iterator<Item = &'a str> + Clone,
{
    let mut result: Option<(String, String)> = None;
    while let Some(word) = words.next() {
        if word.eq_ignore_ascii_case("isolation") {
            if words
                .next()
                .is_some_and(|w| w.eq_ignore_ascii_case("level"))
            {
                let mut level_words: Vec<&str> = Vec::new();
                for w in words.by_ref() {
                    if w.eq_ignore_ascii_case("read")
                        || w.eq_ignore_ascii_case("write")
                        || w.eq_ignore_ascii_case("only")
                        || w.eq_ignore_ascii_case("deferrable")
                        || w.eq_ignore_ascii_case("not")
                    {
                        break;
                    }
                    level_words.push(w);
                }
                if !level_words.is_empty() {
                    result = Some((
                        "isolation_level".to_string(),
                        level_words.join(" ").to_ascii_lowercase(),
                    ));
                }
            }
        } else if word.eq_ignore_ascii_case("read") {
            if let Some(next) = words.next() {
                if next.eq_ignore_ascii_case("write") {
                    result = Some(("transaction_read_only".to_string(), "off".to_string()));
                } else if next.eq_ignore_ascii_case("only") {
                    result = Some(("transaction_read_only".to_string(), "on".to_string()));
                }
            }
        } else if word.eq_ignore_ascii_case("deferrable") {
            result = Some(("transaction_deferrable".to_string(), "on".to_string()));
        } else if word.eq_ignore_ascii_case("not") {
            if words
                .next()
                .is_some_and(|w| w.eq_ignore_ascii_case("deferrable"))
            {
                result = Some(("transaction_deferrable".to_string(), "off".to_string()));
            }
        }
    }
    result
}

fn parse_search_path(value: Option<&str>) -> Vec<String> {
    let Some(value) = value else {
        return vec!["public".to_string()];
    };
    let mut path = Vec::new();
    for item in value.split(',') {
        let schema = item.trim().trim_matches('"').trim_matches('\'');
        if !schema.is_empty() && !path.iter().any(|known| known == schema) {
            path.push(schema.to_string());
        }
    }
    if path.is_empty() {
        path.push("public".to_string());
    }
    path
}

/// Splits a simple-protocol Query string at top-level semicolons, ignoring
/// semicolons inside single/double-quoted strings, dollar-quoted bodies, line
/// comments, and parenthesized expressions. Used to keep multi-statement
/// batches containing SET/SHOW classified per statement.
/// True when `sql` is a transaction-control statement, which must never be
/// separated from the rest of its batch: it either opens a transaction the
/// following statements belong to, or ends one for all of them.
fn is_transaction_control(sql: &str) -> bool {
    let leading = sql
        .split_whitespace()
        .next()
        .map(|word| word.trim_end_matches(';'));
    matches!(
        leading,
        Some(word)
            if word.eq_ignore_ascii_case("BEGIN")
                || word.eq_ignore_ascii_case("COMMIT")
                || word.eq_ignore_ascii_case("ROLLBACK")
                || word.eq_ignore_ascii_case("SAVEPOINT")
                || word.eq_ignore_ascii_case("RELEASE")
                || word.eq_ignore_ascii_case("START")
                || word.eq_ignore_ascii_case("END")
    )
}

/// Counts the top-level statements in an extended-protocol Parse body using
/// the real SQL lexer — the same tokenizer the parser runs on — so string
/// literals, quoted identifiers, dollar-quoted bodies, line comments and
/// parenthesis nesting can never desynchronize the count from what execution
/// would see. (Block comments are not valid SQL in this engine: the lexer
/// surfaces them as ordinary tokens, exactly as the parser would, so both
/// sides stay loud together.) Fragments holding only semicolons/whitespace
/// do not count, so a lone trailing `;` stays a single statement.
/// Returns `None` when the body does not lex; such bodies keep today's flow
/// and their syntax error surfaces at Bind/Execute/Describe as before.
fn extended_parse_statement_count(sql: &str) -> Option<usize> {
    let tokens = plomid_sql::Lexer::new(sql).lex().ok()?;
    let mut count = 0usize;
    let mut depth = 0usize;
    let mut has_token = false;
    for token in &tokens {
        match token.kind {
            plomid_sql::TokenKind::LParen => {
                depth = depth.saturating_add(1);
                has_token = true;
            }
            plomid_sql::TokenKind::RParen => {
                depth = depth.saturating_sub(1);
                has_token = true;
            }
            plomid_sql::TokenKind::SemiColon if depth == 0 => {
                if has_token {
                    count += 1;
                }
                has_token = false;
            }
            plomid_sql::TokenKind::Eof => break,
            _ => {
                has_token = true;
            }
        }
    }
    if has_token {
        count += 1;
    }
    Some(count)
}

fn split_simple_protocol_statements(sql: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = sql.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut in_line_comment = false;
    let mut in_dollar = false;
    let mut depth: usize = 0;
    while let Some(ch) = chars.next() {
        if in_line_comment {
            current.push(ch);
            if ch == '\n' {
                in_line_comment = false;
            }
            continue;
        }
        if in_dollar {
            current.push(ch);
            if ch == '$' && chars.peek() == Some(&'$') {
                current.push(chars.next().unwrap());
                in_dollar = false;
            }
            continue;
        }
        if in_single {
            current.push(ch);
            if ch == '\'' {
                if chars.peek() == Some(&'\'') {
                    current.push(chars.next().unwrap());
                } else {
                    in_single = false;
                }
            }
            continue;
        }
        if in_double {
            current.push(ch);
            if ch == '"' {
                if chars.peek() == Some(&'"') {
                    current.push(chars.next().unwrap());
                } else {
                    in_double = false;
                }
            }
            continue;
        }
        match ch {
            '-' if chars.peek() == Some(&'-') => {
                current.push(ch);
                current.push(chars.next().unwrap());
                in_line_comment = true;
            }
            '\'' => {
                in_single = true;
                current.push(ch);
            }
            '"' => {
                in_double = true;
                current.push(ch);
            }
            '$' if chars.peek() == Some(&'$') => {
                current.push(ch);
                current.push(chars.next().unwrap());
                in_dollar = true;
            }
            '(' => {
                depth = depth.saturating_add(1);
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ';' if depth == 0 => {
                current.push(ch);
                if !current.trim().is_empty() {
                    parts.push(current.trim().to_string());
                }
                current = String::new();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        parts.push(current.trim().to_string());
    }
    parts
}

fn row_descriptions(
    columns: &[String],
    column_types: &[Option<plomid_sql::ColumnType>],
    result_formats: &[u16],
) -> Vec<crate::protocol::RowFieldDescription> {
    columns
        .iter()
        .zip(column_types.iter())
        .enumerate()
        .map(|(index, (name, column_type))| {
            let (type_oid, type_size) = match column_type {
                Some(ct) => {
                    let oid = ct.type_oid.0;
                    let size = match ct.pg_type() {
                        Some(plomid_types::PgType::Bool) => 1,
                        Some(plomid_types::PgType::Int2) => 2,
                        Some(plomid_types::PgType::Int4) => 4,
                        Some(plomid_types::PgType::Int8) => 8,
                        Some(plomid_types::PgType::Float4) => 4,
                        Some(plomid_types::PgType::Float8) => 8,
                        Some(plomid_types::PgType::Uuid) => 16,
                        _ => -1,
                    };
                    (oid, size)
                }
                None => (25, -1),
            };
            crate::protocol::RowFieldDescription {
                name: name.clone(),
                type_oid,
                type_size,
                type_modifier: column_type.as_ref().map_or(-1, |column| column.typmod),
                format: result_formats
                    .get(index)
                    .copied()
                    .or_else(|| result_formats.first().copied())
                    .map(|format| {
                        if format == 1 && !supports_binary_oid(type_oid) {
                            0
                        } else {
                            format
                        }
                    })
                    .unwrap_or(0),
            }
        })
        .collect()
}

fn read_cstring<'a>(bytes: &'a [u8], field: &str) -> std::io::Result<(String, &'a [u8])> {
    let end = bytes.iter().position(|byte| *byte == 0).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("truncated extended-query {field}"),
        )
    })?;
    let value = std::str::from_utf8(&bytes[..end])
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid UTF-8"))?
        .to_owned();
    Ok((value, &bytes[end + 1..]))
}

/// Extracts the savepoint name for each statement shape:
/// `SAVEPOINT name` | `RELEASE [SAVEPOINT] name` |
/// `ROLLBACK TO [SAVEPOINT] name`. Returns `None` when the name is
/// missing so the caller reports a syntax error instead of looking up
/// an empty-named savepoint.
pub fn savepoint_name(statement: &str, sql: &str) -> Option<String> {
    let stripped: Vec<String> = sql
        .split_whitespace()
        .map(|token| token.trim_end_matches(';').trim_matches('"').to_string())
        .collect();
    let name_index = match statement {
        "SAVEPOINT" => 1,
        "RELEASE" => {
            if stripped
                .get(1)
                .is_some_and(|word| word.eq_ignore_ascii_case("SAVEPOINT"))
            {
                2
            } else {
                1
            }
        }
        _ => {
            if stripped
                .get(2)
                .is_some_and(|word| word.eq_ignore_ascii_case("SAVEPOINT"))
            {
                3
            } else {
                2
            }
        }
    };
    stripped
        .get(name_index)
        .filter(|name| !name.is_empty())
        .cloned()
}
