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
//! PLOMID database server binary.
//!
//! Entry point for the `plomid-server` command. Parses command-line
//! arguments, initializes the storage engine, opens the executor, and
//! starts the TCP listener.

use plomid_network::server::PlomidServer;
use plomid_network::server::{ConnectionLimits, ServerEnforcement};
use plomid_network::session::ServerAuthConfig;
use plomid_server::backup_tool;
use plomid_txn::PlomidStorageEngine;
use std::sync::Arc;
use std::sync::Mutex;
use tracing::{error, info, info_span, warn};
use tracing_subscriber::{
    fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

mod banner;
mod config;

use config::ServerConfig;

#[tokio::main]
async fn main() {
    // Offline cold backup tools run before server parsing: `backup`,
    // `restore`, `backup-inspect` never start a listener.
    let argv: Vec<String> = std::env::args().collect();
    if let Some(code) = backup_tool::dispatch_offline_tool(&argv) {
        std::process::exit(code);
    }
    let config = match ServerConfig::parse() {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("configuration error: {e}");
            std::process::exit(1);
        }
    };

    init_logging(&config);

    // Runtime tier: detected once at startup (CLI --profile wins over
    // PLOMID_PROFILE wins over auto-detect). Everything tier-sized below
    // derives from this value; volatile limits (CPUs, RAM) re-detect live
    // every 60 s at admission boundaries (executor `maybe_recheck_if_due`),
    // with explicit pins keeping their tier while the elastic cap still
    // follows the hardware.
    let cli_tier = match config.profile_tier() {
        Ok(tier) => tier,
        Err(e) => {
            eprintln!("configuration error: {e}");
            std::process::exit(1);
        }
    };
    // Fold the CLI flag into the env so the executor's lazy detection and
    // its live rechecks (which only read `PLOMID_PROFILE`) honor the same
    // pin: `--profile` wins over env, and the pin survives re-detection.
    if let Some(tier) = cli_tier {
        std::env::set_var(plomid_hw::PROFILE_ENV_VAR, tier.as_str());
    }
    let runtime = match plomid_hw::RuntimeProfile::resolve(cli_tier) {
        Ok(runtime) => runtime,
        Err(e) => {
            eprintln!("configuration error: {e}");
            std::process::exit(1);
        }
    };
    {
        let hw = runtime.hardware();
        info!(
            target: "server",
            event = "runtime_profile",
            tier = %runtime.tier(),
            source = %runtime.source(),
            cores = hw.logical_cpus,
            ram_bytes = hw.ram_total_bytes.unwrap_or(0),
            ram_known = hw.ram_total_bytes.is_some(),
            storage = ?hw.storage,
            pool_pages = runtime.pool_capacity_pages(),
            "detected runtime tier",
        );
    }

    print!(
        "{}",
        banner::render_banner(
            &config,
            &runtime,
            env!("CARGO_PKG_VERSION"),
            std::io::IsTerminal::is_terminal(&std::io::stdout()),
        )
    );
    let boot_started = std::time::Instant::now();
    let startup_span = info_span!(
        target: "server",
        "server",
        service = "plomid",
        host = %config.host,
        port = config.port,
        data_dir = %config.data_dir.display(),
    );
    startup_span.in_scope(|| {
        info!(target: "server", event = "server_start", auth = true);
    });

    if let Err(e) = std::fs::create_dir_all(&config.data_dir) {
        error!(target: "server", "data_dir_create_failed error={}", e);
        std::process::exit(1);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&config.data_dir, std::fs::Permissions::from_mode(0o700));
    }

    // Data-directory advisory lock (P1-004): exactly one server per data
    // directory, and the offline backup/restore tools can fence live
    // servers. Held for the process lifetime (forgotten, never closed);
    // the kernel releases it on death so crashes cannot wedge it.
    match backup_tool::hold_data_dir_lock(&config.data_dir) {
        Ok(guard) => std::mem::forget(guard),
        Err(e) => {
            error!(target: "server", "data_dir_lock_failed error={}", e);
            std::process::exit(1);
        }
    }

    if let Err(e) = std::fs::create_dir_all(&config.copy_dir) {
        error!(target: "server", "copy_dir_create_failed error={}", e);
        std::process::exit(1);
    }
    // Fail closed when `--copy-dir` resolves through a symlink or is not a
    // real directory: a link here would redirect every jailed COPY outside
    // the intended tree. `create_dir_all` follows links, so check after.
    match std::fs::symlink_metadata(&config.copy_dir) {
        Ok(meta) if meta.is_dir() && !meta.file_type().is_symlink() => {}
        Ok(_) => {
            error!(target: "server", "copy_dir_not_a_directory");
            std::process::exit(1);
        }
        Err(e) => {
            error!(target: "server", "copy_dir_create_failed error={}", e);
            std::process::exit(1);
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let _ = std::fs::set_permissions(&config.copy_dir, std::fs::Permissions::from_mode(0o700));
    }

    // Durable audit sink (F-09): installed once, process-global, before any
    // connection is accepted. Fail closed when the path cannot be opened;
    // unset (the default) leaves tracing + counters as the only record.
    if let Some(path) = config.audit_log.as_ref() {
        if let Err(e) = plomid_security::sink::set_audit_log(path) {
            error!(target: "server", "audit_log_open_failed error={}", e);
            std::process::exit(1);
        }
        info!(target: "server", event = "audit_log_ready", path = %path.display());
    }

    let storage_path = config.data_dir.clone();
    let wal_path = config.data_dir.join("wal");

    // Keep the hot working set resident during bulk inserts and scans. The
    // previous 64-page pool caused avoidable eviction/reload churn once a
    // table and its catalog/index pages exceeded the tiny default. Sized
    // from the runtime tier (a 16 GiB laptop yields exactly the 1024 pages
    // this default used to hardcode).
    // Stage latency for the ready screen (display only).
    let storage_open_started = std::time::Instant::now();
    let mut engine =
        match PlomidStorageEngine::open(&storage_path, &wal_path, runtime.pool_capacity_pages()) {
            Ok(engine) => engine,
            Err(e) => {
                error!(target: "server", "storage_open_failed error={}", e);
                std::process::exit(1);
            }
        };
    // Automatic WAL checkpointing. Without it, a long-running server retains
    // WAL until an explicit checkpoint, which is the difference between a
    // bounded restart and one that replays every commit since startup.
    engine.set_checkpoint_policy(config.checkpoint_policy());
    let storage_ms = storage_open_started.elapsed().as_millis() as u64;
    info!(target: "server", event = "storage_ready", path = %storage_path.display());

    let engine = Arc::new(Mutex::new(engine));
    // Bootstrap credential pair. Construction needs a CSPRNG salt and fails
    // closed when OS randomness is unavailable (fail closed, never weak).
    let auth_config = match config.auth_method.as_ref() {
        "none" | "disabled" => ServerAuthConfig::disabled(),
        "md5" => {
            // Reached only with --allow-legacy-md5 (validated above): the
            // downgrade path stays loud on every start.
            warn!(
                target: "server",
                event = "legacy_md5_enabled",
                "MD5 authentication is deprecated and will be removed; \
                 migrate clients to SCRAM (--auth scram). Every MD5 login \
                 is counted (legacy_auth_uses)"
            );
            match ServerAuthConfig::md5(config.username.clone(), config.password.clone()) {
                Ok(config) => config,
                Err(e) => {
                    error!(target: "server", "auth_config_failed error={}", e);
                    std::process::exit(1);
                }
            }
        }
        "password" | "cleartext" => {
            match ServerAuthConfig::new(config.username.clone(), config.password.clone()) {
                Ok(config) => config,
                Err(e) => {
                    error!(target: "server", "auth_config_failed error={}", e);
                    std::process::exit(1);
                }
            }
        }
        // Default and preferred: SCRAM-SHA-256.
        "scram" => {
            match ServerAuthConfig::scram(config.username.clone(), config.password.clone()) {
                Ok(config) => config,
                Err(e) => {
                    error!(target: "server", "auth_config_failed error={}", e);
                    std::process::exit(1);
                }
            }
        }
        other => {
            error!(target: "server", "unknown_auth_method method={}", other);
            std::process::exit(1);
        }
    };

    if config.tls.enabled {
        let cert_empty = config.tls.cert.display().to_string().is_empty();
        let key_empty = config.tls.key.display().to_string().is_empty();
        if cert_empty || key_empty {
            error!(
                target: "server",
                "tls_requires_cert_and_key; pass --tls-cert and --tls-key"
            );
            std::process::exit(1);
        }
    }

    // Load + validate the TLS identity before binding: expired material,
    // unparsable PEM, or a key that does not match the chain fails closed
    // here with a startup error rather than at the first client handshake.
    let tls = if config.tls.enabled {
        match plomid_network::tls::load_server_config(&config.tls.cert, &config.tls.key) {
            Ok(rustls_config) => {
                info!(
                    target: "server",
                    event = "tls_ready",
                    required = config.tls.required,
                    cert = %config.tls.cert.display(),
                    "TLS identity loaded",
                );
                Some(plomid_network::tls::TlsAcceptor::new(
                    rustls_config,
                    config.tls.required,
                ))
            }
            Err(e) => {
                error!(target: "server", "tls_load_failed error={}", e);
                std::process::exit(1);
            }
        }
    } else {
        None
    };

    let addr = std::net::SocketAddr::new(config.host, config.port);
    if config.allow_remote_plaintext && !config.host.is_loopback() {
        // Repeated on every start on purpose: this is the one configuration in
        // which credentials cross the network unencrypted. Authentication is
        // still enforced, so the warning is about transport, not access.
        warn!(
            target: "server",
            event = "insecure_remote_bind",
            host = %config.host,
            "accepting remote clients without TLS; credentials are sent in the clear. \
             Restrict this listener to trusted networks"
        );
    }
    let bind_started = std::time::Instant::now();
    let mut server = match PlomidServer::bind_with_limits(
        addr,
        auth_config,
        ConnectionLimits {
            max_connections: config.max_connections,
        },
    )
    .await
    {
        Ok(server) => server,
        Err(e) => {
            error!(target: "server", "bind_failed error={}", e);
            std::process::exit(1);
        }
    };
    let bind_ms = bind_started.elapsed().as_millis() as u64;
    server.set_copy_dir(config.copy_dir.clone());
    server.set_enforcement(ServerEnforcement {
        statement_timeout_ms: config.statement_timeout_ms,
        idle_in_transaction_timeout_ms: config.idle_in_transaction_timeout_ms,
        max_copy_buffer_bytes: config.max_copy_buffer_bytes,
        max_prepared_statements: config.max_prepared_statements,
        max_portals: config.max_portals,
    });
    // Configuration provenance: the redacted effective configuration is the
    // operator's audit trail for "what is this server actually running?".
    // Secrets never appear here (`effective_summary` redacts them; TLS
    // material is paths only).
    for (name, value) in config.effective_summary() {
        info!(target: "server", event = "effective_config", name = %name, value = %value);
    }
    if let Some(tls) = tls {
        server.set_tls_config(tls);
    }
    // Background maintenance moves generation passes off committing
    // connections: sessions submit due tables instead of materializing them
    // inline. The worker derives its own engine facade, so passes never
    // borrow session state; a full queue (or a stopped worker) degrades to
    // inline passes, exactly the historical behavior.
    let maintenance_worker = plomid_network::MaintenanceWorker::spawn(Arc::clone(&engine));
    server.set_maintenance_link(maintenance_worker.link());
    info!(target: "server", event = "network_ready", listening_on = %server.local_addr());

    let actual_addr = server.local_addr();
    startup_span.in_scope(|| {
        info!(
            target: "server",
            event = "server_ready",
            listening_on = %actual_addr,
            storage = %storage_path.display(),
            wal = %wal_path.display(),
            "PLOMID is ready to accept connections"
        );
    });

    // Final boot states with measured stage latencies. The WAL mounts inside
    // the engine open and the executor has no open step in this binary, so
    // those rows report `ready` without a separate timer rather than
    // inventing numbers.
    print!(
        "{}",
        banner::render_ready(
            &[
                banner::StageStatus {
                    name: "storage engine",
                    millis: Some(storage_ms),
                },
                banner::StageStatus {
                    name: "write-ahead log",
                    millis: None,
                },
                banner::StageStatus {
                    name: "query executor",
                    millis: None,
                },
                banner::StageStatus {
                    name: "network listener",
                    millis: Some(bind_ms),
                },
            ],
            boot_started.elapsed().as_millis() as u64,
            &actual_addr.to_string(),
            std::io::IsTerminal::is_terminal(&std::io::stdout()),
        )
    );

    // Graceful shutdown. Without a signal handler the process dies wherever the
    // kernel's SIGTERM lands, leaving every commit since the last checkpoint for
    // the next start to replay -- seconds of WAL replay on a busy system, and a
    // startup that grows with the write history. On SIGINT/SIGTERM the listener
    // stops accepting, in-flight statements are given a bounded window to
    // finish, and one final checkpoint flushes data pages, publishes the
    // recovery boundary, and reclaims the WAL it made unreachable. Durability is
    // unchanged: the checkpoint is the same crash-safe path used at runtime, and
    // a kill between commits is still recovered from the WAL.
    let live = server.live_connections();
    let engine_for_shutdown = Arc::clone(&engine);
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut drained_cleanly = true;
    let mut server_run = Box::pin(server.run_with_database_root(engine, config.data_dir.clone()));
    tokio::select! {
        _ = &mut server_run => {}
        _ = &mut shutdown => {
            info!(target: "server", event = "shutdown_requested");
            // Dropping the accept loop closes the listener; existing handlers
            // keep running until they finish or the grace period expires.
            drop(server_run);
            let outstanding = plomid_network::server::PlomidServer::drain(
                &live,
                std::time::Duration::from_secs(SHUTDOWN_GRACE_SECS),
            )
            .await;
            if outstanding > 0 {
                drained_cleanly = false;
                warn!(
                    target: "server",
                    event = "shutdown_drain_timeout",
                    connections = outstanding,
                    "grace period expired with connections still active"
                );
            }
        }
    }

    // Stop background maintenance before the final checkpoint: queued items
    // are dropped (their debt re-derives from session accounting), and the
    // join is bounded by one in-flight pass, which is crash-safe at every
    // boundary like a kill during VACUUM.
    let (submitted, completed, inline_fallbacks) = maintenance_worker.link().stats();
    maintenance_worker.shutdown();
    info!(
        target: "server",
        event = "maintenance_worker_shutdown",
        submitted,
        completed,
        inline_fallbacks,
        queue_depth = 0,
    );

    // Single lifecycle authority: the engine demounts itself (stop mutations
    // → flush → checkpoint → sync) through the same crash-safe checkpoint
    // path used at runtime. A kill between commits is still recovered from
    // the WAL; a clean shutdown minimizes the replay to the post-checkpoint
    // tail (possibly nothing).
    let shutdown = match engine_for_shutdown.lock() {
        Ok(mut engine) => Some(engine.shutdown()),
        Err(_) => None,
    };
    match shutdown {
        Some(Ok(report)) => {
            let outcome = report.checkpoint;
            info!(
                target: "server",
                event = "shutdown_checkpoint",
                stages = report.stages.len(),
                lsn = outcome.as_ref().map(|o| o.lsn.get()).unwrap_or(0),
                duration_ms = report.elapsed.as_millis() as u64,
                reclaimed_segments =
                    outcome.as_ref().map(|o| o.reclaimed_segments).unwrap_or(0),
                wal_bytes = outcome.as_ref().map(|o| o.wal_bytes).unwrap_or(0),
            );
        }
        Some(Err(error)) => warn!(
            target: "server",
            event = "shutdown_checkpoint_failed",
            error_kind = error.kind().code(),
            error_message = error.message(),
            "final checkpoint did not complete; the next start will replay the WAL tail"
        ),
        None => warn!(
            target: "server",
            event = "shutdown_checkpoint_failed",
            error_kind = "PL-POISONED",
            "final checkpoint did not complete; the next start will replay the WAL tail"
        ),
    }

    info!(
        target: "server",
        event = "server_shutdown",
        drained_cleanly,
    );
}

/// How long a shutdown waits for in-flight statements before checkpointing.
const SHUTDOWN_GRACE_SECS: u64 = 5;

/// Resolves when the process is asked to stop (Ctrl-C or SIGTERM).
///
/// `docker stop`, systemd, Kubernetes and a plain `kill` all send SIGTERM, so
/// handling only Ctrl-C would leave every deploy path with an unclean stop.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(e) => {
                error!(target: "server", "sigterm_handler_failed error={}", e);
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

fn init_logging(config: &ServerConfig) {
    // INFO stays the default so operators keep visibility, but every
    // per-operation event is emitted at DEBUG: the `plomid::perf` stage timers
    // (`mvcc_get`, `mvcc_begin`, `commit_prepare`, `commit_publish`,
    // `mvcc_install`, `storage_apply`, `group_flush`, `txn_commit`, the
    // update/delete stage timers), the `sql::txn` transaction lifecycle, the
    // `write_gate` events, and `wire_out`/query-lifecycle protocol messages.
    //
    // Several of those timers run once per point read / per commit *inside*
    // the shared engine mutex, so emitting them at INFO did not just add log
    // volume -- it serialized every connection behind stdout writes. INFO now
    // carries the low-volume operational record (startup, connections, DDL,
    // maintenance passes and failures); the detailed per-operation timings are
    // one flag away via `PLOMID_LOG_LEVEL=debug` or
    // `RUST_LOG=plomid::perf=debug`.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new(match config.log_level.as_deref() {
            Some("trace") => "trace",
            Some("debug") => "debug",
            Some("warn") => "warn",
            Some("error") => "error",
            _ => "info",
        })
    });

    let ansi = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(ansi)
        .with_target(true)
        .with_thread_ids(true)
        .with_file(false)
        .with_line_number(false)
        .with_span_events(FmtSpan::CLOSE);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .init();
}
