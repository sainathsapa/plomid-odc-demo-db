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
//! TCP connection handling and server lifecycle for PLOMID.
//!
//! [`PlomidServer`] owns the TCP listener and dispatches each accepted
//! connection onto a dedicated async task. The per-connection PostgreSQL
//! frontend/backend message flow lives in [`handler::ConnectionHandler`],
//! while multi-database routing state is encapsulated by
//! [`registry::DatabaseRegistry`].

pub mod guardrails;
pub mod handler;
pub mod registry;

pub use guardrails::{
    new_account_lockout, new_auth_rate_limiter, new_cancel_registry, AccountLockout,
    CancelRegistry, SharedAccountLockout, SharedAuthRateLimiter,
};

use crate::session::ServerAuthConfig;
use plomid_txn::PlomidStorageEngine;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tracing::{info, info_span, warn, Instrument};

pub use handler::ConnectionHandler;

/// Storage engines shared per database across connections.
///
/// Secondary databases used to open a fresh engine per connection, which
/// raced the WAL/segment files (independent writers, independent LSN
/// counters) and corrupted them under any concurrent use. Every database —
/// including the default — maps to exactly one engine here, opened once and
/// shared by all of its connections like the default database always was.
/// Keys are lowercased database names. Entries are evicted on DROP DATABASE;
/// live connections keep their Arc, so eviction is safe.
pub(crate) type DatabaseEngines = Arc<
    tokio::sync::Mutex<
        std::collections::HashMap<String, Arc<std::sync::Mutex<PlomidStorageEngine>>>,
    >,
>;

/// A running PLOMID server instance.
pub struct PlomidServer {
    listener: tokio::net::TcpListener,
    auth_config: ServerAuthConfig,
    /// TLS accept state. `None` means plaintext-only: every `SSLRequest`
    /// is answered `N`. Installed from `--tls --tls-cert/--tls-key`.
    tls: Option<crate::tls::TlsAcceptor>,
    connection_counter: Arc<AtomicU64>,
    query_counter: Arc<AtomicU64>,
    connection_permits: Arc<Semaphore>,
    /// Connections currently being served. A graceful shutdown waits for this to
    /// reach zero (bounded) so in-flight statements finish before the final
    /// checkpoint is taken, instead of being cut off mid-commit.
    live_connections: Arc<AtomicU64>,
    /// Background maintenance endpoint handed to every session. When absent
    /// (tests, embedded use), sessions run maintenance passes inline.
    maintenance_link: Option<crate::WorkerLink>,
    /// Per-connection enforcement budgets (see [`ServerEnforcement`]).
    enforcement: ServerEnforcement,
    /// Directory `COPY ... FROM/TO <file>` paths are jailed to. When `None`
    /// each connection falls back to its data directory, or denies every
    /// file COPY when no data directory is known.
    copy_dir: Option<PathBuf>,
}

/// Admission limits for client work. Idle connections consume only a Tokio
/// task, protocol buffers, and this permit; database execution state is
/// created after authentication and is not held by the listener.
#[derive(Clone, Copy, Debug)]
pub struct ConnectionLimits {
    pub max_connections: usize,
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        // A bounded, deployable default (PostgreSQL parity): one hundred
        // concurrent connections is ample for tests and embedding, while
        // each idle connection still costs a Tokio task, socket buffers,
        // and a semaphore permit. Production servers pass an explicit
        // operator-configured value through `bind_with_limits` instead of
        // relying on any default.
        Self {
            max_connections: 100,
        }
    }
}

/// Per-connection enforcement budgets stamped from the operator's server
/// configuration. The server installs these via
/// [`PlomidServer::set_enforcement`]; every connection handler applies them
/// to its session defaults and extended-protocol caps. Timeouts are
/// milliseconds, `0` disables; session `SET` still overrides per session.
#[derive(Clone, Copy, Debug)]
pub struct ServerEnforcement {
    /// Default `statement_timeout` for new sessions (ms, `0` disables).
    pub statement_timeout_ms: u64,
    /// Default `idle_in_transaction_session_timeout` (ms, `0` disables).
    pub idle_in_transaction_timeout_ms: u64,
    /// Cap for one COPY data accumulation buffer, in bytes.
    pub max_copy_buffer_bytes: usize,
    /// Cap for prepared statements held per connection.
    pub max_prepared_statements: usize,
    /// Cap for portals held per connection.
    pub max_portals: usize,
}

impl Default for ServerEnforcement {
    fn default() -> Self {
        // Safe production posture: runaway statements die, abandoned
        // transactions release their locks, and per-connection protocol
        // state stays bounded. Matches the server binary's own defaults so
        // embedding behaves like production unless retuned.
        Self {
            statement_timeout_ms: 30_000,
            idle_in_transaction_timeout_ms: 60_000,
            max_copy_buffer_bytes: 64 * 1024 * 1024,
            max_prepared_statements: 1024,
            max_portals: 1024,
        }
    }
}

impl PlomidServer {
    pub async fn bind(addr: SocketAddr, auth_config: ServerAuthConfig) -> std::io::Result<Self> {
        Self::bind_with_limits(addr, auth_config, ConnectionLimits::default()).await
    }

    /// Binds an ephemeral loopback port directly (no discover-drop-rebind).
    ///
    /// Test and embedding helper: binding `:0` on the real listener is
    /// race-free, unlike picking a port with a throwaway socket, dropping
    /// it, and rebinding (a parallel test can steal the port in between and
    /// fail with `AddrInUse`). Prefer this over the manual dance.
    pub async fn bind_ephemeral(auth_config: ServerAuthConfig) -> std::io::Result<Self> {
        let addr: SocketAddr = "127.0.0.1:0".parse().expect("loopback ephemeral addr");
        Self::bind_with_limits(addr, auth_config, ConnectionLimits::default()).await
    }

    pub async fn bind_with_limits(
        addr: SocketAddr,
        auth_config: ServerAuthConfig,
        limits: ConnectionLimits,
    ) -> std::io::Result<Self> {
        if limits.max_connections == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "max_connections must be greater than zero",
            ));
        }
        let listener = tokio::net::TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        info!(target: "server", "listening on {}", local_addr);
        // Note: BackendKeyData is per-connection (pid = connection id,
        // fresh secret per connection; see `run_inner`/`fresh_cancel_secret`).
        // No server-wide secret is kept: a single shared secret would let any
        // observer cancel any session.
        Ok(Self {
            listener,
            auth_config,
            tls: None,
            connection_counter: Arc::new(AtomicU64::new(0)),
            query_counter: Arc::new(AtomicU64::new(0)),
            connection_permits: Arc::new(Semaphore::new(limits.max_connections)),
            live_connections: Arc::new(AtomicU64::new(0)),
            maintenance_link: None,
            copy_dir: None,
            enforcement: ServerEnforcement::default(),
        })
    }

    /// Installs the TLS accept state for all subsequently accepted
    /// connections. Existing connections are unaffected.
    pub fn set_tls_config(&mut self, tls: crate::tls::TlsAcceptor) {
        self.tls = Some(tls);
    }

    /// Installs per-connection enforcement budgets (session timeout
    /// defaults, COPY buffer cap, prepared/portal caps) for connections
    /// accepted afterwards. Existing connections keep their budgets.
    pub fn set_enforcement(&mut self, enforcement: ServerEnforcement) {
        self.enforcement = enforcement;
    }

    /// Restricts `COPY ... FROM/TO <file>` to `dir` for every connection.
    /// When unset, connections fall back to their data directory.
    pub fn set_copy_dir(&mut self, dir: PathBuf) {
        self.copy_dir = Some(dir);
    }

    /// Installs the background maintenance endpoint for all sessions.
    /// Connections created afterwards submit due tables to the worker instead
    /// of running passes on the committing connection.
    pub fn set_maintenance_link(&mut self, link: crate::WorkerLink) {
        self.maintenance_link = Some(link);
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.listener
            .local_addr()
            .expect("listener must be bound before local_addr is called")
    }

    /// Handle to the live-connection gauge, usable after [`Self::run`] has
    /// consumed the server.
    #[must_use]
    pub fn live_connections(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.live_connections)
    }

    /// Waits (bounded) for in-flight connections to finish.
    ///
    /// Returns the number of connections still being served when `timeout`
    /// elapses, so a shutdown can record whether it drained cleanly.
    pub async fn drain(live: &Arc<AtomicU64>, timeout: std::time::Duration) -> u64 {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let current = live.load(Ordering::Relaxed);
            if current == 0 {
                return 0;
            }
            if tokio::time::Instant::now() >= deadline {
                return current;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    pub async fn run(self, engine: Arc<Mutex<PlomidStorageEngine>>) {
        self.run_inner(engine, None, None).await;
    }

    /// Runs with per-connection database routing rooted at `data_dir`.
    /// The legacy `run` entry point remains shared-engine mode for protocol
    /// tests and callers that intentionally operate one database.
    pub async fn run_with_database_root(
        self,
        engine: Arc<Mutex<PlomidStorageEngine>>,
        data_dir: PathBuf,
    ) {
        let loaded_registry = registry::DatabaseRegistry::load(&data_dir);
        // The executor owns the live `pg_database` projection. Populate it
        // from the durable registry before accepting connections; otherwise a
        // server restart exposes only the default database until the next
        // CREATE/DROP DATABASE statement mutates the in-memory projection.
        let registry = Arc::new(tokio::sync::Mutex::new(loaded_registry));
        let engines: DatabaseEngines = Arc::new(tokio::sync::Mutex::new(
            [(String::from("plomid"), engine.clone())]
                .into_iter()
                .collect(),
        ));
        self.run_inner(engine, Some((data_dir, registry)), Some(engines))
            .await;
    }

    async fn run_inner(
        self,
        engine: Arc<Mutex<PlomidStorageEngine>>,
        database_context: Option<(PathBuf, Arc<tokio::sync::Mutex<registry::DatabaseRegistry>>)>,
        database_engines: Option<DatabaseEngines>,
    ) {
        let connection_counter = self.connection_counter.clone();
        let query_counter = self.query_counter.clone();
        let connection_permits = self.connection_permits.clone();
        let copy_dir = self.copy_dir.clone();
        let enforcement = self.enforcement;
        // Query guardrails:
        // per-connection cancel tokens + per-IP auth rate limiting. Both are
        // shared by every connection on this listener. All engine mutations
        // are transactional, so aborting a query via the cancel flag never
        // corrupts shared engine state.
        let cancel_registry = new_cancel_registry();
        let auth_limiter = new_auth_rate_limiter();
        let account_lockout = new_account_lockout();
        loop {
            let (socket, peer_addr) = match self.listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(target: "server", "accept_failed error={}", e);
                    continue;
                }
            };
            let permit = match connection_permits.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    warn!(target: "server", event = "connection_rejected", reason = "connection_limit", peer = %peer_addr);
                    let mut socket = socket;
                    let _ = socket.shutdown().await;
                    continue;
                }
            };
            let id = connection_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            // Observability: process-wide opened counter next to the
            // listener's own accounting.
            plomid_core::observability::record_connection_open();
            info!(target: "server", "connection_open connection_id={} peer={}", id, peer_addr);

            let engine = engine.clone();
            let maintenance_link = self.maintenance_link.clone();
            let auth_config = self.auth_config.clone();
            // Per-connection BackendKeyData (PG protocol): pid is the
            // connection id, secret is fresh per connection. A CancelRequest
            // carries (pid, secret); only the holder of both can cancel.
            let process_id = id as u32;
            let secret_key = fresh_cancel_secret(id);
            let cancel_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
            {
                let mut registry = cancel_registry.lock().expect("cancel registry");
                registry.insert(
                    process_id,
                    crate::server::guardrails::CancelEntry {
                        secret: secret_key,
                        cancel: Arc::clone(&cancel_flag),
                    },
                );
            }
            let peer_ip = peer_addr.ip();
            let connection_query_counter = query_counter.clone();
            let connection_database_context = database_context.clone();
            let connection_database_engines = database_engines.clone();
            let connection_copy_dir = copy_dir.clone();
            let connection_enforcement = enforcement;
            let connection_span = info_span!(
                target: "server",
                "connection",
                connection_id = id,
                peer = %peer_addr,
            );

            let live = Arc::clone(&self.live_connections);
            live.fetch_add(1, Ordering::Relaxed);
            let task_registry = Arc::clone(&cancel_registry);
            let task_limiter = Arc::clone(&auth_limiter);
            let task_lockout = Arc::clone(&account_lockout);
            let task_tls = self.tls.clone();
            tokio::spawn(async move {
                let _permit = permit;
                // Pre-startup negotiation runs here (inside the task, so a
                // stalled client never blocks the accept loop): SSLRequest /
                // GSSENC / CancelRequest are consumed and the transport is
                // final before the handler is constructed.
                let incoming = match crate::tls::negotiate(socket, task_tls.as_ref()).await {
                    Ok(incoming) => incoming,
                    Err(e) => {
                        warn!(target: "server", "negotiation_failed connection_id={} error={}", id, e);
                        live.fetch_sub(1, Ordering::Relaxed);
                        plomid_core::observability::record_connection_close();
                        return;
                    }
                };
                // CancelRequest is a complete, untagged 16-byte message on
                // its own connection: set the target flag and close quietly.
                // Unknown pid/secret fails closed (nothing happens).
                let (transport, startup, used_tls, tls_required) = match incoming {
                    crate::tls::Incoming::Cancel { pid, secret } => {
                        if let Ok(registry) = task_registry.lock() {
                            if let Some(entry) = registry.get(&pid) {
                                if entry.secret == secret {
                                    entry.cancel.store(true, Ordering::Relaxed);
                                }
                            }
                        }
                        live.fetch_sub(1, Ordering::Relaxed);
                        plomid_core::observability::record_connection_close();
                        return;
                    }
                    crate::tls::Incoming::Close => {
                        live.fetch_sub(1, Ordering::Relaxed);
                        plomid_core::observability::record_connection_close();
                        return;
                    }
                    crate::tls::Incoming::Startup {
                        transport,
                        payload,
                        used_tls,
                    } => {
                        let required = task_tls.as_ref().is_some_and(|t| t.is_required());
                        (transport, payload, used_tls, required)
                    }
                };
                let mut handler = ConnectionHandler::new(
                    id,
                    transport,
                    used_tls,
                    tls_required,
                    engine,
                    auth_config,
                    process_id,
                    secret_key,
                    connection_query_counter,
                    connection_database_context,
                    connection_database_engines,
                    maintenance_link.clone(),
                    Arc::clone(&cancel_flag),
                    Some(peer_ip),
                    task_limiter,
                    task_lockout,
                    connection_enforcement,
                );
                if let Some(dir) = connection_copy_dir {
                    handler.set_copy_dir(dir);
                }
                let result = handler.run(startup).instrument(connection_span).await;
                // Cancel token lifetime ends with the connection: a late
                // CancelRequest for a closed pid finds nothing (fail-closed).
                task_registry
                    .lock()
                    .expect("cancel registry")
                    .remove(&process_id);
                match result {
                    Ok(()) => {
                        info!(target: "server", "connection_close connection_id={} reason=normal", id);
                    }
                    Err(e) => {
                        warn!(target: "server", "connection_close connection_id={} error={}", id, e);
                    }
                }
                live.fetch_sub(1, Ordering::Relaxed);
                // Observability: every served connection closes exactly
                // once here, on both the normal and error paths above.
                plomid_core::observability::record_connection_close();
            });
        }
    }
}

/// Fresh per-connection cancel secret.
///
/// `id` (the connection counter) guarantees uniqueness even when two
/// connections open in the same nanosecond; wall-clock bits make the secret
/// unpredictable to anyone who did not observe this connection's
/// BackendKeyData.
fn fresh_cancel_secret(id: u64) -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() ^ (d.subsec_nanos() as u64))
        .unwrap_or(0x9e3779b9);
    let mixed = now
        .wrapping_mul(0x9e3779b97f4a7c15)
        .wrapping_add(id.wrapping_mul(0xbf58476d1ce4e5b9));
    ((mixed >> 32) as u32) ^ (id as u32).wrapping_mul(2654435761)
}

pub(crate) fn statement_type(sql: &str) -> &'static str {
    let word = sql
        .split_whitespace()
        .next()
        .map(|word| word.trim_end_matches(';'))
        .unwrap_or("");
    // Zero-allocation dispatch: case-insensitive compare without building an
    // uppercase String (the previous version allocated once per call, and the
    // request path calls this several times per statement).
    if word.eq_ignore_ascii_case("BEGIN") || word.eq_ignore_ascii_case("START") {
        return "BEGIN";
    }
    if word.eq_ignore_ascii_case("COMMIT") {
        return "COMMIT";
    }
    if word.eq_ignore_ascii_case("CREATE") {
        return "CREATE";
    }
    if word.eq_ignore_ascii_case("INSERT") {
        return "INSERT";
    }
    if word.eq_ignore_ascii_case("SELECT") {
        return "SELECT";
    }
    if word.eq_ignore_ascii_case("UPDATE") {
        return "UPDATE";
    }
    if word.eq_ignore_ascii_case("DELETE") {
        return "DELETE";
    }
    if word.eq_ignore_ascii_case("SET") {
        return "SET";
    }
    if word.eq_ignore_ascii_case("SHOW") {
        return "SHOW";
    }
    if word.eq_ignore_ascii_case("USE") {
        return "USE";
    }
    if word.eq_ignore_ascii_case("ALTER") {
        return "ALTER";
    }
    if word.eq_ignore_ascii_case("DROP") {
        return "DROP";
    }
    if word.eq_ignore_ascii_case("COMMENT") {
        return "COMMENT";
    }
    if word.eq_ignore_ascii_case("VACUUM") {
        return "VACUUM";
    }
    if word.eq_ignore_ascii_case("ANALYZE") {
        return "ANALYZE";
    }
    if word.eq_ignore_ascii_case("REINDEX") {
        return "REINDEX";
    }
    if word.eq_ignore_ascii_case("LOCK") {
        return "LOCK";
    }
    if word.eq_ignore_ascii_case("CLUSTER") {
        return "CLUSTER";
    }
    if word.eq_ignore_ascii_case("TRUNCATE") {
        return "TRUNCATE";
    }
    if word.eq_ignore_ascii_case("REFRESH") {
        return "REFRESH";
    }
    if word.eq_ignore_ascii_case("EXPLAIN") {
        return "EXPLAIN";
    }
    if word.eq_ignore_ascii_case("COPY") {
        return "COPY";
    }
    if word.eq_ignore_ascii_case("GRANT") {
        return "GRANT";
    }
    if word.eq_ignore_ascii_case("REVOKE") {
        return "REVOKE";
    }
    if word.eq_ignore_ascii_case("VALUES") {
        return "VALUES";
    }
    if word.eq_ignore_ascii_case("DESCRIBE") {
        return "DESCRIBE";
    }
    if word.eq_ignore_ascii_case("DEALLOCATE") {
        return "DEALLOCATE";
    }
    if word.eq_ignore_ascii_case("DISCARD") {
        return "DISCARD";
    }
    if word.eq_ignore_ascii_case("UNLISTEN") {
        return "UNLISTEN";
    }
    if word.eq_ignore_ascii_case("LISTEN") {
        return "LISTEN";
    }
    if word.eq_ignore_ascii_case("NOTIFY") {
        return "NOTIFY";
    }
    if word.eq_ignore_ascii_case("CLOSE") {
        return "CLOSE";
    }
    if word.eq_ignore_ascii_case("SAVEPOINT") {
        return "SAVEPOINT";
    }
    if word.eq_ignore_ascii_case("RELEASE") {
        return "RELEASE";
    }
    if word.eq_ignore_ascii_case("ROLLBACK") {
        let mut tokens = sql.split_whitespace();
        let second = tokens.nth(1).unwrap_or("").trim_end_matches(';');
        let third = tokens.next().unwrap_or("").trim_end_matches(';');
        if second.eq_ignore_ascii_case("TO") {
            // `ROLLBACK TO [SAVEPOINT] name`: the SAVEPOINT keyword is
            // optional in PostgreSQL, so `ROLLBACK TO sp1` is a savepoint
            // rollback, not a full-transaction rollback. A missing name is
            // reported as a syntax error by the savepoint path.
            return "ROLLBACK_TO_SAVEPOINT";
        }
        let _ = third;
        return "ROLLBACK";
    }
    "UNKNOWN"
}
