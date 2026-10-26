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
//! Server configuration for the PLOMID database server.
//!
//! Configuration is provided via command-line arguments and environment
//! variables. The design is intentionally simple for V1 and can be
//! extended with configuration files in future releases.

use clap::{Arg, ArgAction};
use std::path::PathBuf;

/// The built-in password shipped as the server default.
///
/// Public and documented: loopback development may keep it, but
/// [`ServerConfig::validate_security`] refuses it on any non-loopback
/// listener so production configuration always carries a unique secret.
pub const DEFAULT_PASSWORD: &str = "plomid";

/// Parses an operator timeout flag into milliseconds, mirroring the session
/// guardrail syntax (`plomid_executor::guard::parse_timeout`): bare numbers
/// are milliseconds, `ms`/`s`/`min`/`h` suffixes (and long forms) scale,
/// `0` disables. Unlike the per-session path (fail-open so a client value
/// never breaks a session), operator configuration fails closed on
/// unparseable input.
fn parse_timeout_ms(raw: &str) -> Result<u64, String> {
    let trimmed = raw.trim().trim_matches('\'').trim_matches('"').trim();
    if trimmed.is_empty() {
        return Err("timeout must not be empty (use 0 to disable)".to_string());
    }
    let lower = trimmed.to_ascii_lowercase();
    let split = lower
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '+' || c == '-'))
        .unwrap_or(lower.len());
    let (number, unit) = (lower[..split].trim(), lower[split..].trim());
    let value: f64 = number
        .parse()
        .map_err(|_| format!("invalid timeout value: {raw}"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("invalid timeout value: {raw}"));
    }
    if value == 0.0 {
        return Ok(0);
    }
    let millis = match unit {
        "" | "ms" | "millisecond" | "milliseconds" => value,
        "s" | "sec" | "secs" | "second" | "seconds" => value * 1_000.0,
        "min" | "mins" | "minute" | "minutes" => value * 60_000.0,
        "h" | "hr" | "hrs" | "hour" | "hours" => value * 3_600_000.0,
        _ => return Err(format!("invalid timeout unit in: {raw}")),
    };
    if !millis.is_finite() || millis <= 0.0 {
        return Err(format!("invalid timeout value: {raw}"));
    }
    Ok((millis.min(u64::MAX as f64 / 2.0).max(1.0)) as u64)
}

/// TLS configuration for the PLOMID server.
///
/// When disabled the server answers every `SSLRequest` with an `N` (SSL not
/// available) response, which conforms to the PostgreSQL protocol: clients
/// then fall back to a plaintext connection. When enabled (`--tls` with
/// `--tls-cert`/`--tls-key`), the server answers `S` and performs a real
/// rustls handshake; the certificate/key files are loaded and validated
/// (parse + expiry + key match) at startup, fail closed.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert: PathBuf,
    pub key: PathBuf,
    /// If true, reject plaintext connections and require TLS (PostgreSQL
    /// `ssl = require` semantics).
    pub required: bool,
}

/// Configuration for the PLOMID server process.
#[derive(Clone)]
pub struct ServerConfig {
    pub host: std::net::IpAddr,
    pub port: u16,
    pub data_dir: PathBuf,
    /// Directory `COPY ... FROM/TO <file>` paths are restricted to.
    /// Defaults to `<data_dir>/copy`.
    pub copy_dir: PathBuf,
    pub username: String,
    pub password: String,
    pub auth_method: String,
    pub tls: TlsConfig,
    /// Explicit operator opt-in for the legacy MD5 password method.
    ///
    /// MD5 is a compatibility downgrade path (deprecated, removal planned):
    /// it needs a server-side plaintext secret and offers no verifier
    /// storage. `--auth md5` without this flag fails closed at startup.
    /// Prefer `--auth scram` (default). Every successful MD5 login is
    /// counted (`legacy_auth_uses`) and logged as a warning.
    pub allow_legacy_md5: bool,
    /// Explicit operator opt-in that permits a non-loopback bind **without**
    /// TLS.
    ///
    /// PLOMID fails closed here: a non-loopback listener would put the
    /// single configured credential pair on the network, where clients send it
    /// unencrypted. Deployments that cannot use TLS yet (containers, trusted
    /// LANs) must bind `0.0.0.0` to be reachable, so they opt in explicitly
    /// and the server logs a warning on every start. Authentication is still
    /// required and cannot be disabled remotely, so this never yields an
    /// unauthenticated listener. Prefer `--tls --tls-cert/--tls-key`
    /// (optionally with `--tls-required`) for anything beyond a trusted
    /// network.
    pub allow_remote_plaintext: bool,
    pub log_level: Option<String>,
    /// Whether the automatic WAL checkpoint policy is active.
    pub checkpoint_enabled: bool,
    /// WAL bytes appended since the last checkpoint that make one due.
    pub checkpoint_wal_bytes: u64,
    /// Retained WAL segments that make a checkpoint due (backstop signal).
    pub checkpoint_segments: u64,
    /// Maximum seconds between checkpoints (`0` disables the interval signal).
    pub checkpoint_interval_secs: u64,
    /// Runtime-profile override (`edge`|`laptop`|`server`|`auto`).
    ///
    /// `None` (the default) means `auto`: the tier is detected from the
    /// host. `--profile` wins over `PLOMID_PROFILE`; both are validated here
    /// so a typo fails closed at startup instead of silently detecting.
    pub profile: Option<String>,
    /// Maximum concurrent client connections (production default 100;
    /// PostgreSQL parity). Rejections fail closed with the listener kept.
    pub max_connections: usize,
    /// Default `statement_timeout` for new sessions, in milliseconds
    /// (`0` disables; per-session `SET` still overrides).
    pub statement_timeout_ms: u64,
    /// Default `idle_in_transaction_session_timeout`, in milliseconds
    /// (`0` disables; per-session `SET` still overrides).
    pub idle_in_transaction_timeout_ms: u64,
    /// Per-connection COPY accumulation cap, in bytes.
    pub max_copy_buffer_bytes: usize,
    /// Per-connection prepared-statement cap.
    pub max_prepared_statements: usize,
    /// Per-connection portal cap.
    pub max_portals: usize,
    /// Durable security-audit log path (`--audit-log` / `PLOMID_AUDIT_LOG`).
    ///
    /// `None` (the default) disables the file sink: audit events still emit
    /// via `tracing` and counters, but nothing is appended durably. An
    /// explicitly empty value is normalized to `None` for the same reason.
    pub audit_log: Option<PathBuf>,
}

impl std::fmt::Debug for ServerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerConfig")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("data_dir", &self.data_dir)
            .field("copy_dir", &self.copy_dir)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .field("auth_method", &self.auth_method)
            .field("tls", &self.tls)
            .field("allow_remote_plaintext", &self.allow_remote_plaintext)
            .field("allow_legacy_md5", &self.allow_legacy_md5)
            .field("log_level", &self.log_level)
            .field("checkpoint_enabled", &self.checkpoint_enabled)
            .field("checkpoint_wal_bytes", &self.checkpoint_wal_bytes)
            .field("checkpoint_segments", &self.checkpoint_segments)
            .field("checkpoint_interval_secs", &self.checkpoint_interval_secs)
            .field("profile", &self.profile)
            .field("max_connections", &self.max_connections)
            .field("statement_timeout_ms", &self.statement_timeout_ms)
            .field(
                "idle_in_transaction_timeout_ms",
                &self.idle_in_transaction_timeout_ms,
            )
            .field("max_copy_buffer_bytes", &self.max_copy_buffer_bytes)
            .field("max_prepared_statements", &self.max_prepared_statements)
            .field("max_portals", &self.max_portals)
            .field("audit_log", &self.audit_log)
            .finish()
    }
}

/// One redacted effective-configuration row: `(name, value)`.
///
/// Secrets never appear here: see [`ServerConfig::effective_summary`].
pub type ConfigSummaryRow = (String, String);

/// True when the operator set `flag` explicitly (CLI or `PLOMID_*` env).
///
/// clap records a [`clap::parser::ValueSource`] per flag: anything other
/// than `DefaultValue` (i.e. `CommandLine` or `Env`) means the parsed value
/// came from the operator and must win over any tier default.
fn is_explicit_flag(matches: &clap::ArgMatches, flag: &str) -> bool {
    matches
        .value_source(flag)
        .is_some_and(|source| source != clap::parser::ValueSource::DefaultValue)
}

/// Applies tier-derived checkpoint defaults to parsed thresholds.
///
/// On the edge tier, each threshold the operator did NOT set explicitly is
/// replaced with the edge default ([`plomid_txn::CheckpointPolicy::EDGE_*`]);
/// explicit values pass through untouched on every tier, and non-edge tiers
/// pass everything through untouched. Pure over its inputs so tests can
/// mock tiers without touching the host.
pub fn apply_tier_checkpoint_defaults(
    wal_bytes: u64,
    wal_explicit: bool,
    segments: u64,
    segments_explicit: bool,
    interval_secs: u64,
    interval_explicit: bool,
    tier: plomid_hw::RuntimeTier,
) -> (u64, u64, u64) {
    if !matches!(tier, plomid_hw::RuntimeTier::Edge) {
        return (wal_bytes, segments, interval_secs);
    }
    let edge = plomid_txn::CheckpointPolicy::edge_defaults();
    (
        if wal_explicit {
            wal_bytes
        } else {
            edge.wal_bytes()
        },
        if segments_explicit {
            segments
        } else {
            edge.wal_segments()
        },
        if interval_explicit {
            interval_secs
        } else {
            edge.max_interval()
                .map(|d| d.as_secs())
                .unwrap_or(interval_secs)
        },
    )
}

impl ServerConfig {
    /// Parses the process arguments (`PLOMID_*` environment variables are
    /// applied by clap).
    pub fn parse() -> Result<Self, String> {
        Self::parse_from(std::env::args())
    }

    /// Parses an explicit argument list.
    ///
    /// Split out from [`Self::parse`] so tests can exercise flags without
    /// mutating the process environment.
    pub fn parse_from<I, T>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = T>,
        T: Into<std::ffi::OsString> + Clone,
    {
        let matches = clap::Command::new("plomid-server")
            .version(env!("CARGO_PKG_VERSION"))
            .about("PLOMID database server")
            .arg(
                Arg::new("host")
                    .long("host")
                    .short('H')
                    .env("PLOMID_HOST")
                    .value_name("ADDR")
                    .default_value("127.0.0.1")
                    .help("IP address to listen on"),
            )
            .arg(
                Arg::new("port")
                    .long("port")
                    .short('p')
                    .env("PLOMID_PORT")
                    .value_name("PORT")
                    .default_value("5432")
                    .help("TCP port to listen on"),
            )
            .arg(
                Arg::new("data")
                    .long("data")
                    .short('D')
                    .env("PLOMID_DATA_DIR")
                    .value_name("DIR")
                    .default_value("./data")
                    .help("Data directory for storage and WAL"),
            )
            .arg(
                Arg::new("copy-dir")
                    .long("copy-dir")
                    .env("PLOMID_COPY_DIR")
                    .value_name("DIR")
                    .help(
                        "Directory COPY ... FROM/TO file paths are restricted to \
                         (defaults to <data-dir>/copy)",
                    ),
            )
            .arg(
                Arg::new("username")
                    .long("username")
                    .short('U')
                    .env("PLOMID_USER")
                    .value_name("USER")
                    .default_value("plomid")
                    .help("Required username for authentication"),
            )
            .arg(
                Arg::new("password")
                    .long("password")
                    .short('W')
                    .env("PLOMID_PASSWORD")
                    .value_name("PASS")
                    .default_value("plomid")
                    .help("Password for authentication"),
            )
            .arg(
                Arg::new("log-level")
                    .long("log-level")
                    .value_name("LEVEL")
                    .action(ArgAction::Set)
                    .help("Log level (trace, debug, info, warn, error)"),
            )
            .arg(
                Arg::new("auth")
                    .long("auth")
                    .env("PLOMID_AUTH")
                    .value_name("METHOD")
                    .default_value("scram")
                    .help(
                        "Authentication method: scram (default, audited crypto), \
                         password, md5 (deprecated legacy path, needs \
                         --allow-legacy-md5), none (loopback only)",
                    ),
            )
            .arg(
                Arg::new("tls")
                    .long("tls")
                    .help("Enable TLS (requires --tls-cert and --tls-key)")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("tls-cert")
                    .long("tls-cert")
                    .env("PLOMID_TLS_CERT")
                    .value_name("FILE")
                    .default_value("")
                    .help("Path to the TLS certificate (PEM)"),
            )
            .arg(
                Arg::new("tls-key")
                    .long("tls-key")
                    .env("PLOMID_TLS_KEY")
                    .value_name("FILE")
                    .default_value("")
                    .help("Path to the TLS private key (PEM)"),
            )
            .arg(
                Arg::new("tls-required")
                    .long("tls-required")
                    .help("Refuse plaintext connections when TLS is enabled")
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("checkpoint-wal-bytes")
                    .long("checkpoint-wal-bytes")
                    .env("PLOMID_CHECKPOINT_WAL_BYTES")
                    .value_name("BYTES")
                    .default_value("67108864")
                    .help(
                        "WAL bytes since the last checkpoint that make one due \
                         (0 disables this signal; default 64 MiB)",
                    ),
            )
            .arg(
                Arg::new("checkpoint-segments")
                    .long("checkpoint-segments")
                    .env("PLOMID_CHECKPOINT_SEGMENTS")
                    .value_name("N")
                    .default_value("16")
                    .help(
                        "Retained WAL segment count that makes a checkpoint due \
                         (0 disables this signal; default 16)",
                    ),
            )
            .arg(
                Arg::new("checkpoint-interval-secs")
                    .long("checkpoint-interval-secs")
                    .env("PLOMID_CHECKPOINT_INTERVAL_SECS")
                    .value_name("SECONDS")
                    .default_value("900")
                    .help(
                        "Maximum seconds between automatic checkpoints \
                         (0 disables this signal; default 900)",
                    ),
            )
            .arg(
                Arg::new("allow-remote-plaintext")
                    .long("allow-remote-plaintext")
                    .env("PLOMID_ALLOW_INSECURE_REMOTE")
                    .help(
                        "Allow a non-loopback bind without TLS (container / trusted-network \
                         deployments). Authentication is still required; only the transport is \
                         unencrypted. Prefer TLS once it is available",
                    )
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("allow-legacy-md5")
                    .long("allow-legacy-md5")
                    .env("PLOMID_ALLOW_LEGACY_MD5")
                    .help(
                        "Allow the deprecated MD5 password method (--auth md5) \
                         for legacy clients. MD5 is a compatibility downgrade \
                         path with a removal plan; prefer --auth scram. Every \
                         MD5 login is counted and logged as a warning",
                    )
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("no-auto-checkpoint")
                    .long("no-auto-checkpoint")
                    .help(
                        "Disable automatic checkpointing entirely; WAL is then \
                         reclaimed only by an explicit checkpoint",
                    )
                    .action(ArgAction::SetTrue),
            )
            .arg(
                Arg::new("profile")
                    .long("profile")
                    .env("PLOMID_PROFILE")
                    .value_name("TIER")
                    .help(
                        "Runtime tier override: edge, laptop, server, or auto \
                         (default: auto-detect from the host)",
                    ),
            )
            .arg(
                Arg::new("max-connections")
                    .long("max-connections")
                    .env("PLOMID_MAX_CONNECTIONS")
                    .value_name("N")
                    .default_value("100")
                    .help(
                        "Maximum concurrent client connections (default 100; \
                         0 or unparseable fails closed at startup)",
                    ),
            )
            .arg(
                Arg::new("statement-timeout")
                    .long("statement-timeout")
                    .env("PLOMID_STATEMENT_TIMEOUT")
                    .value_name("DURATION")
                    .default_value("30s")
                    .help(
                        "Default statement_timeout for new sessions: bare \
                         milliseconds, or ms/s/min/h suffixes (default 30s; \
                         0 disables; per-session SET still overrides)",
                    ),
            )
            .arg(
                Arg::new("idle-in-transaction-timeout")
                    .long("idle-in-transaction-timeout")
                    .env("PLOMID_IDLE_IN_TRANSACTION_TIMEOUT")
                    .value_name("DURATION")
                    .default_value("60s")
                    .help(
                        "Default idle_in_transaction_session_timeout: same \
                         syntax (default 60s; 0 disables; SET overrides)",
                    ),
            )
            .arg(
                Arg::new("max-copy-buffer-bytes")
                    .long("max-copy-buffer-bytes")
                    .env("PLOMID_MAX_COPY_BUFFER_BYTES")
                    .value_name("BYTES")
                    .default_value("67108864")
                    .help(
                        "Per-connection COPY accumulation cap in bytes \
                         (default 64 MiB; rejects past the cap)",
                    ),
            )
            .arg(
                Arg::new("max-prepared-statements")
                    .long("max-prepared-statements")
                    .env("PLOMID_MAX_PREPARED_STATEMENTS")
                    .value_name("N")
                    .default_value("1024")
                    .help("Per-connection prepared-statement cap (default 1024)"),
            )
            .arg(
                Arg::new("max-portals")
                    .long("max-portals")
                    .env("PLOMID_MAX_PORTALS")
                    .value_name("N")
                    .default_value("1024")
                    .help("Per-connection portal cap (default 1024)"),
            )
            .arg(
                Arg::new("audit-log")
                    .long("audit-log")
                    .env("PLOMID_AUDIT_LOG")
                    .value_name("FILE")
                    .help(
                        "Append-only JSONL security-audit log path (disabled by \
                         default; when set, every allow/deny decision is \
                         fsynced to this file with size-based rotation)",
                    ),
            )
            .get_matches_from(args);

        let host = matches
            .get_one::<String>("host")
            .expect("host has default")
            .parse()
            .map_err(|e| format!("invalid host address: {e}"))?;

        let port = matches
            .get_one::<String>("port")
            .expect("port has default")
            .parse()
            .map_err(|e| format!("invalid port: {e}"))?;

        let data_dir: PathBuf = matches
            .get_one::<String>("data")
            .expect("data has default")
            .into();

        let copy_dir: PathBuf = matches
            .get_one::<String>("copy-dir")
            .map(PathBuf::from)
            .unwrap_or_else(|| data_dir.join("copy"));

        let username = matches
            .get_one::<String>("username")
            .expect("username has default")
            .clone();

        let password = matches
            .get_one::<String>("password")
            .expect("password has default")
            .clone();

        let log_level = matches.get_one::<String>("log-level").cloned();

        // Fail closed on typos: `plomid_hw::RuntimeTier::parse_override` is
        // the single authority for accepted values (`auto` = detect).
        let profile = matches.get_one::<String>("profile").cloned();
        if let Some(ref raw) = profile {
            plomid_hw::RuntimeTier::parse_override(raw)
                .map_err(|e| format!("invalid --profile: {e}"))?;
        }

        let auth_method = matches
            .get_one::<String>("auth")
            .expect("auth has default")
            .to_ascii_lowercase();

        let checkpoint_enabled = !matches.get_flag("no-auto-checkpoint");
        let allow_remote_plaintext = matches.get_flag("allow-remote-plaintext");
        let allow_legacy_md5 = matches.get_flag("allow-legacy-md5");
        let checkpoint_wal_bytes: u64 = matches
            .get_one::<String>("checkpoint-wal-bytes")
            .expect("checkpoint-wal-bytes has default")
            .parse()
            .map_err(|e| format!("invalid --checkpoint-wal-bytes: {e}"))?;
        let checkpoint_segments: u64 = matches
            .get_one::<String>("checkpoint-segments")
            .expect("checkpoint-segments has default")
            .parse()
            .map_err(|e| format!("invalid --checkpoint-segments: {e}"))?;
        let checkpoint_interval_secs: u64 = matches
            .get_one::<String>("checkpoint-interval-secs")
            .expect("checkpoint-interval-secs has default")
            .parse()
            .map_err(|e| format!("invalid --checkpoint-interval-secs: {e}"))?;

        // Tier-derived checkpoint defaults: only flags the operator did NOT
        // set (neither CLI nor `PLOMID_CHECKPOINT_*` env — both count as
        // explicit) fall back to the tier default on edge hardware, where
        // larger thresholds spare slow storage. Previously every default
        // came from the clap literals, so default-vs-explicit was
        // indistinguishable; `value_source` is the distinction now.
        let cli_tier = match &profile {
            None => None,
            Some(raw) => plomid_hw::RuntimeTier::parse_override(raw)
                .map_err(|e| format!("invalid --profile: {e}"))?,
        };
        let tier = plomid_hw::RuntimeProfile::resolve(cli_tier)
            .map(|runtime| runtime.tier())
            .map_err(|e| format!("configuration error: {e}"))?;
        let (checkpoint_wal_bytes, checkpoint_segments, checkpoint_interval_secs) =
            apply_tier_checkpoint_defaults(
                checkpoint_wal_bytes,
                is_explicit_flag(&matches, "checkpoint-wal-bytes"),
                checkpoint_segments,
                is_explicit_flag(&matches, "checkpoint-segments"),
                checkpoint_interval_secs,
                is_explicit_flag(&matches, "checkpoint-interval-secs"),
                tier,
            );

        let tls_enabled = matches.get_flag("tls");
        let tls_required = matches.get_flag("tls-required");
        let tls = TlsConfig {
            enabled: tls_enabled,
            cert: matches
                .get_one::<String>("tls-cert")
                .expect("tls-cert has default")
                .clone()
                .into(),
            key: matches
                .get_one::<String>("tls-key")
                .expect("tls-key has default")
                .clone()
                .into(),
            required: tls_required,
        };

        let max_connections: usize = matches
            .get_one::<String>("max-connections")
            .expect("max-connections has default")
            .parse()
            .map_err(|e| format!("invalid --max-connections: {e}"))?;
        if max_connections == 0 {
            return Err("--max-connections must be greater than zero".to_string());
        }
        let statement_timeout_ms = parse_timeout_ms(
            matches
                .get_one::<String>("statement-timeout")
                .expect("statement-timeout has default"),
        )
        .map_err(|e| format!("invalid --statement-timeout: {e}"))?;
        let idle_in_transaction_timeout_ms = parse_timeout_ms(
            matches
                .get_one::<String>("idle-in-transaction-timeout")
                .expect("idle-in-transaction-timeout has default"),
        )
        .map_err(|e| format!("invalid --idle-in-transaction-timeout: {e}"))?;
        let max_copy_buffer_bytes: usize = matches
            .get_one::<String>("max-copy-buffer-bytes")
            .expect("max-copy-buffer-bytes has default")
            .parse()
            .map_err(|e| format!("invalid --max-copy-buffer-bytes: {e}"))?;
        if max_copy_buffer_bytes == 0 {
            return Err("--max-copy-buffer-bytes must be greater than zero".to_string());
        }
        let max_prepared_statements: usize = matches
            .get_one::<String>("max-prepared-statements")
            .expect("max-prepared-statements has default")
            .parse()
            .map_err(|e| format!("invalid --max-prepared-statements: {e}"))?;
        if max_prepared_statements == 0 {
            return Err("--max-prepared-statements must be greater than zero".to_string());
        }
        let max_portals: usize = matches
            .get_one::<String>("max-portals")
            .expect("max-portals has default")
            .parse()
            .map_err(|e| format!("invalid --max-portals: {e}"))?;
        if max_portals == 0 {
            return Err("--max-portals must be greater than zero".to_string());
        }

        // Empty means disabled: an operator who exports
        // `PLOMID_AUDIT_LOG=""` gets the default (no sink), not a file with
        // a junk name.
        let audit_log: Option<PathBuf> = matches
            .get_one::<String>("audit-log")
            .filter(|raw| !raw.trim().is_empty())
            .map(PathBuf::from);

        let config = Self {
            host,
            port,
            data_dir,
            copy_dir,
            username,
            password,
            auth_method,
            tls,
            log_level,
            allow_remote_plaintext,
            allow_legacy_md5,
            checkpoint_enabled,
            checkpoint_wal_bytes,
            checkpoint_segments,
            checkpoint_interval_secs,
            profile,
            max_connections,
            statement_timeout_ms,
            idle_in_transaction_timeout_ms,
            max_copy_buffer_bytes,
            max_prepared_statements,
            max_portals,
            audit_log,
        };
        config.validate_security()?;
        Ok(config)
    }

    /// Builds the automatic checkpoint policy from the configured thresholds.
    ///
    /// A threshold of `0` (or an interval of `0` seconds) disables that signal,
    /// which lets an operator keep, for example, byte-triggered checkpointing
    /// while disabling the interval. `--no-auto-checkpoint` disables the whole
    /// policy; explicit checkpoints remain available either way, so the
    /// durability boundary itself is never configurable.
    ///
    /// Tier defaults are already baked into the stored thresholds at parse
    /// time (see [`apply_tier_checkpoint_defaults`]): this constructor just
    /// translates stored values, so operator-explicit values always win.
    #[must_use]
    pub fn checkpoint_policy(&self) -> plomid_txn::CheckpointPolicy {
        if !self.checkpoint_enabled {
            return plomid_txn::CheckpointPolicy::disabled();
        }
        let interval = match self.checkpoint_interval_secs {
            0 => None,
            seconds => Some(std::time::Duration::from_secs(seconds)),
        };
        plomid_txn::CheckpointPolicy::new(
            self.checkpoint_wal_bytes,
            self.checkpoint_segments,
            interval,
        )
    }

    /// The `--profile` / `PLOMID_PROFILE` override as a tier, if pinned.
    ///
    /// `None` means `auto` (or unset): the caller detects from the host.
    /// Parsing was validated in [`Self::parse_from`], so this only fails if
    /// the field was built by hand with a bad value.
    pub fn profile_tier(&self) -> Result<Option<plomid_hw::RuntimeTier>, String> {
        match &self.profile {
            None => Ok(None),
            Some(raw) => plomid_hw::RuntimeTier::parse_override(raw)
                .map_err(|e| format!("invalid profile: {e}")),
        }
    }

    /// Redacted effective configuration for the startup summary.
    ///
    /// Every operator-visible knob appears with its resolved value; secrets
    /// never do: the password renders as `<redacted>`, and TLS material
    /// appears as file paths only (contents are never read into logs).
    /// Render `0` timeouts as `disabled` so the summary states enforcement
    /// posture plainly.
    #[must_use]
    pub fn effective_summary(&self) -> Vec<ConfigSummaryRow> {
        let timeout = |ms: u64| {
            if ms == 0 {
                "disabled".to_string()
            } else {
                format!("{ms}ms")
            }
        };
        vec![
            ("host".to_string(), self.host.to_string()),
            ("port".to_string(), self.port.to_string()),
            ("data_dir".to_string(), self.data_dir.display().to_string()),
            ("copy_dir".to_string(), self.copy_dir.display().to_string()),
            ("username".to_string(), self.username.clone()),
            ("password".to_string(), "<redacted>".to_string()),
            ("auth_method".to_string(), self.auth_method.clone()),
            ("tls_enabled".to_string(), self.tls.enabled.to_string()),
            ("tls_cert".to_string(), self.tls.cert.display().to_string()),
            ("tls_key".to_string(), self.tls.key.display().to_string()),
            ("tls_required".to_string(), self.tls.required.to_string()),
            (
                "allow_remote_plaintext".to_string(),
                self.allow_remote_plaintext.to_string(),
            ),
            (
                "allow_legacy_md5".to_string(),
                self.allow_legacy_md5.to_string(),
            ),
            (
                "log_level".to_string(),
                self.log_level.clone().unwrap_or_default(),
            ),
            (
                "checkpoint_enabled".to_string(),
                self.checkpoint_enabled.to_string(),
            ),
            (
                "checkpoint_wal_bytes".to_string(),
                self.checkpoint_wal_bytes.to_string(),
            ),
            (
                "checkpoint_segments".to_string(),
                self.checkpoint_segments.to_string(),
            ),
            (
                "checkpoint_interval_secs".to_string(),
                self.checkpoint_interval_secs.to_string(),
            ),
            (
                "profile".to_string(),
                self.profile.clone().unwrap_or_else(|| "auto".to_string()),
            ),
            (
                "max_connections".to_string(),
                self.max_connections.to_string(),
            ),
            (
                "statement_timeout".to_string(),
                timeout(self.statement_timeout_ms),
            ),
            (
                "idle_in_transaction_timeout".to_string(),
                timeout(self.idle_in_transaction_timeout_ms),
            ),
            (
                "max_copy_buffer_bytes".to_string(),
                self.max_copy_buffer_bytes.to_string(),
            ),
            (
                "max_prepared_statements".to_string(),
                self.max_prepared_statements.to_string(),
            ),
            ("max_portals".to_string(), self.max_portals.to_string()),
            (
                "audit_log".to_string(),
                self.audit_log
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "disabled".to_string()),
            ),
        ]
    }

    pub fn validate_security(&self) -> Result<(), String> {
        if self.tls.required && !self.tls.enabled {
            return Err("--tls-required requires --tls".to_string());
        }
        let auth_disabled = matches!(self.auth_method.as_str(), "none" | "disabled");
        if auth_disabled && !self.host.is_loopback() {
            return Err("authentication cannot be disabled on a non-loopback bind address".into());
        }
        // MD5 is a deprecated compatibility downgrade path: disabled by
        // default, available only behind the explicit legacy switch.
        if self.auth_method == "md5" && !self.allow_legacy_md5 {
            return Err(
                "MD5 authentication is disabled by default (deprecated downgrade path); \
                 pass --allow-legacy-md5 (or set PLOMID_ALLOW_LEGACY_MD5=1) only for \
                 legacy clients, and prefer --auth scram"
                    .into(),
            );
        }
        if !self.host.is_loopback() && !self.tls.enabled && !self.allow_remote_plaintext {
            return Err(
                "non-loopback binds require TLS; pass --tls --tls-cert/--tls-key \
                 (optionally with --tls-required), or pass --allow-remote-plaintext \
                 (or set PLOMID_ALLOW_INSECURE_REMOTE=1) only on a trusted network \
                 to accept unencrypted remote clients"
                    .into(),
            );
        }
        // The built-in password is public (documented defaults). Loopback
        // single-user development may keep it; any non-loopback listener
        // must use an explicitly chosen secret so a production deployment
        // can never ship the public pair.
        if !self.host.is_loopback() && self.password == DEFAULT_PASSWORD {
            return Err(
                "refusing the built-in default password on a non-loopback bind; \
                 set PLOMID_PASSWORD (or --password) to a unique secret"
                    .into(),
            );
        }
        // V1 has exactly one configured credential pair and defaults to a
        // loopback listener. Keep local single-user development usable with
        // the built-in pair; remote deployment is rejected above unless TLS
        // is enabled or the plaintext opt-in is given with a unique password.
        Ok(())
    }
}
