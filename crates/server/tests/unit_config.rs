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
// Moved from `src/config.rs` inline `#[cfg(test)]` — public-API only.
use plomid_server::config::{ServerConfig, TlsConfig};

fn config(host: &str, auth_method: &str, password: &str) -> ServerConfig {
    ServerConfig {
        host: host.parse().unwrap(),
        port: 5432,
        data_dir: "./data".into(),
        copy_dir: "./data/copy".into(),
        username: "plomid".into(),
        password: password.into(),
        auth_method: auth_method.into(),
        tls: TlsConfig {
            enabled: false,
            cert: "".into(),
            key: "".into(),
            required: false,
        },
        log_level: None,
        allow_remote_plaintext: false,
        allow_legacy_md5: false,
        checkpoint_enabled: true,
        checkpoint_wal_bytes: plomid_txn::CheckpointPolicy::DEFAULT_WAL_BYTES,
        checkpoint_segments: plomid_txn::CheckpointPolicy::DEFAULT_WAL_SEGMENTS,
        checkpoint_interval_secs: plomid_txn::CheckpointPolicy::DEFAULT_MAX_INTERVAL.as_secs(),
        profile: None,
        max_connections: 100,
        statement_timeout_ms: 30_000,
        idle_in_transaction_timeout_ms: 60_000,
        max_copy_buffer_bytes: 64 * 1024 * 1024,
        max_prepared_statements: 1024,
        max_portals: 1024,
        audit_log: None,
    }
}

#[test]
fn default_checkpoint_policy_matches_the_engine_defaults() {
    let policy = config("127.0.0.1", "scram", "plomid").checkpoint_policy();
    assert!(policy.is_enabled());
    assert_eq!(
        policy.wal_bytes(),
        plomid_txn::CheckpointPolicy::DEFAULT_WAL_BYTES
    );
    assert_eq!(
        policy.wal_segments(),
        plomid_txn::CheckpointPolicy::DEFAULT_WAL_SEGMENTS
    );
    assert_eq!(
        policy.max_interval(),
        Some(plomid_txn::CheckpointPolicy::DEFAULT_MAX_INTERVAL)
    );
}

#[test]
fn checkpoint_thresholds_are_operator_configurable() {
    let mut cfg = config("127.0.0.1", "scram", "plomid");
    cfg.checkpoint_enabled = false;
    assert!(!cfg.checkpoint_policy().is_enabled());

    cfg.checkpoint_enabled = true;
    cfg.checkpoint_wal_bytes = 1024;
    cfg.checkpoint_segments = 0;
    cfg.checkpoint_interval_secs = 0;
    let policy = cfg.checkpoint_policy();
    assert_eq!(policy.wal_bytes(), 1024);
    assert_eq!(policy.wal_segments(), 0);
    assert_eq!(policy.max_interval(), None);
}

#[test]
fn unsafe_remote_defaults_fail_closed() {
    assert!(config("0.0.0.0", "scram", "secret")
        .validate_security()
        .is_err());
    assert!(config("0.0.0.0", "none", "secret")
        .validate_security()
        .is_err());
    // The opt-in relaxes the transport rule only: remote listeners still
    // require authentication.
    let mut opted_in = config("0.0.0.0", "scram", "secret");
    opted_in.allow_remote_plaintext = true;
    assert!(opted_in.validate_security().is_ok());
    opted_in.auth_method = "none".into();
    assert!(opted_in.validate_security().is_err());
    // TLS-required still wins over the opt-in.
    let mut tls_required = config("0.0.0.0", "scram", "secret");
    tls_required.allow_remote_plaintext = true;
    tls_required.tls.required = true;
    assert!(tls_required.validate_security().is_err());
}

#[test]
fn remote_plaintext_opt_in_is_required_to_bind_beyond_loopback() {
    let base = ["plomid-server", "--host", "0.0.0.0", "--password", "secret"];
    let err = ServerConfig::parse_from(base)
        .expect_err("a non-loopback bind must fail closed by default");
    assert!(err.contains("non-loopback binds require TLS"), "got: {err}");

    let cfg = ServerConfig::parse_from(base.into_iter().chain(["--allow-remote-plaintext"]))
        .expect("the explicit opt-in must permit the bind");
    assert!(cfg.allow_remote_plaintext);
    assert!(!cfg.host.is_loopback());

    // Loopback never needs the opt-in.
    let local = ServerConfig::parse_from(["plomid-server", "--host", "127.0.0.1"])
        .expect("loopback binds stay valid without the opt-in");
    assert!(!local.allow_remote_plaintext);
}

#[test]
fn remote_bind_refuses_the_built_in_default_password() {
    // The default pair stays usable on loopback (developer mode).
    let local = ServerConfig::parse_from(["plomid-server", "--host", "127.0.0.1"])
        .expect("loopback keeps the built-in pair");
    assert_eq!(local.password, plomid_server::config::DEFAULT_PASSWORD);

    // A remote bind with the built-in password fails closed even with
    // the plaintext opt-in: production must carry a unique secret.
    let err = ServerConfig::parse_from([
        "plomid-server",
        "--host",
        "0.0.0.0",
        "--allow-remote-plaintext",
    ])
    .expect_err("remote + default password must fail closed");
    assert!(err.contains("default password"), "got: {err}");

    // The same bind with a unique password is accepted.
    let cfg = ServerConfig::parse_from([
        "plomid-server",
        "--host",
        "0.0.0.0",
        "--password",
        "8f3c-unique-secret",
        "--allow-remote-plaintext",
    ])
    .expect("remote + unique password + opt-in must parse");
    assert!(!cfg.host.is_loopback());
}

#[test]
fn md5_is_disabled_by_default_with_an_explicit_legacy_switch() {
    // `--auth md5` alone fails closed with a deprecation redirect.
    let err = ServerConfig::parse_from(["plomid-server", "--auth", "md5"])
        .expect_err("MD5 without the legacy switch must fail closed");
    assert!(
        err.contains("MD5 authentication is disabled by default")
            && err.contains("--allow-legacy-md5"),
        "got: {err}"
    );
    // The explicit switch (flag or env) re-enables the downgrade path.
    let cfg = ServerConfig::parse_from(["plomid-server", "--auth", "md5", "--allow-legacy-md5"])
        .expect("explicit legacy switch must parse");
    assert!(cfg.allow_legacy_md5);
    assert_eq!(cfg.auth_method, "md5");
}

#[test]
fn loopback_single_user_accepts_the_default_pair() {
    assert!(config("127.0.0.1", "scram", "plomid")
        .validate_security()
        .is_ok());
    assert!(config("127.0.0.1", "scram", "secret")
        .validate_security()
        .is_ok());
}

#[test]
fn debug_redacts_password() {
    let cfg = config("127.0.0.1", "scram", "s3cr3t-pw");
    let rendered = format!("{cfg:?}");
    assert!(!rendered.contains("s3cr3t-pw"), "got: {rendered}");
    assert!(rendered.contains("<redacted>"), "got: {rendered}");
}

#[test]
fn production_defaults_are_safe_not_permissive() {
    let cfg = ServerConfig::parse_from(["plomid-server"]).expect("defaults parse");
    assert_eq!(cfg.max_connections, 100);
    assert_eq!(cfg.statement_timeout_ms, 30_000);
    assert_eq!(cfg.idle_in_transaction_timeout_ms, 60_000);
    assert_eq!(cfg.max_copy_buffer_bytes, 64 * 1024 * 1024);
    assert_eq!(cfg.max_prepared_statements, 1024);
    assert_eq!(cfg.max_portals, 1024);
}

#[test]
fn timeout_flags_accept_documented_syntax_and_zero_disables() {
    let cfg = ServerConfig::parse_from([
        "plomid-server",
        "--statement-timeout",
        "500ms",
        "--idle-in-transaction-timeout",
        "2min",
    ])
    .expect("suffix syntax parses");
    assert_eq!(cfg.statement_timeout_ms, 500);
    assert_eq!(cfg.idle_in_transaction_timeout_ms, 120_000);

    let cfg = ServerConfig::parse_from([
        "plomid-server",
        "--statement-timeout",
        "0",
        "--idle-in-transaction-timeout",
        "0",
    ])
    .expect("zero disables");
    assert_eq!(cfg.statement_timeout_ms, 0);
    assert_eq!(cfg.idle_in_transaction_timeout_ms, 0);

    for bad in ["forever", "10fortnights", "-5s", ""] {
        let flag = format!("--statement-timeout={bad}");
        assert!(
            ServerConfig::parse_from(["plomid-server", flag.as_str()]).is_err(),
            "{bad} must fail closed"
        );
    }
}

#[test]
fn zero_capacity_limits_fail_closed() {
    for flag in [
        "--max-connections",
        "--max-copy-buffer-bytes",
        "--max-prepared-statements",
        "--max-portals",
    ] {
        assert!(
            ServerConfig::parse_from(["plomid-server", flag, "0"]).is_err(),
            "{flag}=0 must fail closed"
        );
    }
    let cfg = ServerConfig::parse_from([
        "plomid-server",
        "--max-connections",
        "25",
        "--max-copy-buffer-bytes",
        "1048576",
        "--max-prepared-statements",
        "64",
        "--max-portals",
        "64",
    ])
    .expect("tightened limits parse");
    assert_eq!(cfg.max_connections, 25);
    assert_eq!(cfg.max_copy_buffer_bytes, 1_048_576);
    assert_eq!(cfg.max_prepared_statements, 64);
    assert_eq!(cfg.max_portals, 64);
}

#[test]
fn effective_summary_never_carries_secrets() {
    let cfg = ServerConfig::parse_from([
        "plomid-server",
        "--password",
        "sup3r-s3cret-t0ken",
        "--tls-cert",
        "/etc/plomid/cert.pem",
        "--tls-key",
        "/etc/plomid/private-key.pem",
    ])
    .expect("config parses");
    let summary: String = cfg
        .effective_summary()
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("\n");
    for secret in ["sup3r-s3cret-t0ken", "t0ken", "s3cret"] {
        assert!(
            !summary.contains(secret),
            "secret leaked in summary: {summary}"
        );
    }
    // TLS material appears as paths only (contents never enter logs).
    assert!(summary.contains("/etc/plomid/cert.pem"), "{summary}");
    assert!(summary.contains("password=<redacted>"), "{summary}");
    // Enforcement posture is explicit.
    assert!(summary.contains("statement_timeout=30000ms"), "{summary}");
    assert!(
        summary.contains("idle_in_transaction_timeout=60000ms"),
        "{summary}"
    );
    assert!(summary.contains("max_connections=100"), "{summary}");
}

#[test]
fn disabled_timeouts_render_plainly_in_summary() {
    let cfg = ServerConfig::parse_from([
        "plomid-server",
        "--statement-timeout",
        "0",
        "--idle-in-transaction-timeout",
        "0",
    ])
    .expect("zero parses");
    let summary: String = cfg
        .effective_summary()
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(summary.contains("statement_timeout=disabled"), "{summary}");
    assert!(
        summary.contains("idle_in_transaction_timeout=disabled"),
        "{summary}"
    );
}
#[test]
fn audit_log_defaults_to_disabled_and_parses_a_path() {
    let cfg = ServerConfig::parse_from(["plomid-server"]).expect("defaults parse");
    assert_eq!(cfg.audit_log, None);
    let summary: String = cfg
        .effective_summary()
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(summary.contains("audit_log=disabled"), "{summary}");

    let cfg =
        ServerConfig::parse_from(["plomid-server", "--audit-log", "/var/log/plomid/audit.log"])
            .expect("--audit-log parses");
    assert_eq!(
        cfg.audit_log.as_deref(),
        Some(std::path::Path::new("/var/log/plomid/audit.log"))
    );
}
#[test]
fn profile_flag_defaults_to_auto_and_validates() {
    let auto = ServerConfig::parse_from(["plomid-server"]).expect("defaults must parse");
    assert_eq!(auto.profile, None);
    assert_eq!(auto.profile_tier().expect("auto parses"), None);

    let edge = ServerConfig::parse_from(["plomid-server", "--profile", "edge"])
        .expect("--profile edge must parse");
    assert_eq!(edge.profile.as_deref(), Some("edge"));
    assert_eq!(
        edge.profile_tier().expect("edge parses"),
        Some(plomid_hw::RuntimeTier::Edge)
    );

    let err = ServerConfig::parse_from(["plomid-server", "--profile", "turbo"])
        .expect_err("an unknown tier must fail closed");
    assert!(err.contains("invalid --profile"), "got: {err}");
}

#[test]
fn tier_checkpoint_defaults_apply_only_to_unset_flags_on_edge() {
    use plomid_hw::RuntimeTier::{Edge, Laptop, Server};
    use plomid_server::config::apply_tier_checkpoint_defaults;
    let server_vals = (
        plomid_txn::CheckpointPolicy::DEFAULT_WAL_BYTES,
        plomid_txn::CheckpointPolicy::DEFAULT_WAL_SEGMENTS,
        plomid_txn::CheckpointPolicy::DEFAULT_MAX_INTERVAL.as_secs(),
    );
    // Edge, nothing set: all three become the edge defaults.
    let (wal, segs, interval) = apply_tier_checkpoint_defaults(
        server_vals.0,
        false,
        server_vals.1,
        false,
        server_vals.2,
        false,
        Edge,
    );
    assert_eq!(wal, plomid_txn::CheckpointPolicy::EDGE_WAL_BYTES);
    assert_eq!(segs, plomid_txn::CheckpointPolicy::EDGE_WAL_SEGMENTS);
    assert_eq!(
        interval,
        plomid_txn::CheckpointPolicy::EDGE_MAX_INTERVAL.as_secs()
    );
    // Edge, everything set: operator values win untouched.
    let explicit = (1234, 7, 60);
    assert_eq!(
        apply_tier_checkpoint_defaults(explicit.0, true, explicit.1, true, explicit.2, true, Edge),
        explicit
    );
    // Edge, partially set: only the unset flags move.
    let (wal, segs, interval) = apply_tier_checkpoint_defaults(
        1234,
        true,
        server_vals.1,
        false,
        server_vals.2,
        false,
        Edge,
    );
    assert_eq!(wal, 1234);
    assert_eq!(segs, plomid_txn::CheckpointPolicy::EDGE_WAL_SEGMENTS);
    assert_eq!(
        interval,
        plomid_txn::CheckpointPolicy::EDGE_MAX_INTERVAL.as_secs()
    );
    // Non-edge tiers never substitute, even for unset flags.
    for tier in [Laptop, Server] {
        assert_eq!(
            apply_tier_checkpoint_defaults(
                server_vals.0,
                false,
                server_vals.1,
                false,
                server_vals.2,
                false,
                tier
            ),
            server_vals,
            "tier={tier:?}"
        );
    }
}

#[test]
fn explicit_checkpoint_flags_win_over_the_tier_default_at_parse() {
    // Hermetic on any host/tier: explicit values pass through parse
    // untouched, so the operator always beats the tier default.
    let cfg = ServerConfig::parse_from([
        "plomid-server",
        "--profile",
        "edge",
        "--checkpoint-wal-bytes",
        "1234",
        "--checkpoint-segments",
        "0",
        "--checkpoint-interval-secs",
        "0",
    ])
    .expect("explicit flags must parse");
    let policy = cfg.checkpoint_policy();
    assert_eq!(policy.wal_bytes(), 1234);
    assert_eq!(policy.wal_segments(), 0);
    assert_eq!(policy.max_interval(), None);
}

#[test]
fn unset_checkpoint_flags_follow_the_pinned_edge_tier_at_parse() {
    // `--profile edge` pins the tier without touching the host, so this
    // holds on any machine: unset flags become the edge defaults.
    let cfg = ServerConfig::parse_from(["plomid-server", "--profile", "edge"])
        .expect("edge profile must parse");
    let policy = cfg.checkpoint_policy();
    assert!(policy.is_enabled());
    assert_eq!(
        policy.wal_bytes(),
        plomid_txn::CheckpointPolicy::EDGE_WAL_BYTES
    );
    assert_eq!(
        policy.wal_segments(),
        plomid_txn::CheckpointPolicy::EDGE_WAL_SEGMENTS
    );
    assert_eq!(
        policy.max_interval(),
        Some(plomid_txn::CheckpointPolicy::EDGE_MAX_INTERVAL)
    );
}
