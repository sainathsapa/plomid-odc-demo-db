//
// © 2026 PLOMID Technologies INDIA
//
// PLOMID — Platform for Modern Intelligence and Data
// Author: Sainath Sapa
// Email: info.djsai@gmail.com
// GitHub: https://github.com/sainathsapa
// Organization: https://github.com/plomid
//
// Licensed under the Apache License, Version 2.0.

//! Durable append-only audit log sink (finding F-09, smallest safe slice).
//!
//! Security audit events ([`super::audit::AuditEvent`]) previously existed
//! only as `tracing` emissions plus in-memory counters: a crash or a log
//! rotation gap could lose the record of who was allowed or denied what.
//! This module adds an additive, durable sink: one JSON object per line
//! (`{ts,user,action,detail,object,decision,reason}`), appended to a file,
//! `fsync`ed per append, with size-based rotation.
//!
//! Design notes:
//!
//! * **Append-only.** The sink never rewrites or truncates history; rotation
//!   renames the full file aside (`audit-<unix-secs>.log` in the same
//!   directory) and starts fresh, `fsync`ing the directory entry after the
//!   rename so the rotation itself survives a crash.
//! * **fsync per append is the simplest correct policy.** Every [`AuditSink::append`]
//!   ends with `sync_all`, so a returned `Ok` means the record is on stable
//!   storage. Cost: one file `fsync` per security decision (authentication,
//!   authorization, COPY gate, admin action). Security decisions are rare
//!   relative to rows read/written, so the cost is negligible in practice;
//!   if audit volume ever matters, batching would trade durability for
//!   throughput and must be an explicit operator opt-in, not a silent change.
//! * **Process-global handle.** [`set_audit_log`] installs one process-wide
//!   sink behind a [`std::sync::OnceLock`], mirroring the
//!   `plomid-core::observability` counter pattern: audit call sites span the
//!   executor, the network handler, and DDL helpers, and threading a handle
//!   through every one of them would touch hot paths for no durability gain.
//!   [`emit`] is a no-op (tracing only) when no sink is installed, so
//!   libraries and tests behave unchanged with the sink disabled (the
//!   default).
//! * **No secrets by construction.** The sink serializes only the structured
//!   [`super::audit::AuditEvent`] fields; passwords, verifiers, and tokens
//!   never enter an event (see the `audit` module docs), so there is nothing
//!   to redact here. The unit tests pin the exact JSON key set.
//! * **Sink failure never fails the statement.** [`emit`] returns the
//!   append error to the caller; call sites keep their `tracing` line and
//!   deliberately ignore it (`let _ =`), because the sink is a record,
//!   never a gate — enforcement decisions stand regardless of sink health.
//! * **Out of scope:** at-rest encryption and retention/TTL are explicitly
//!   NOT handled here (see F-09); operators own file permissions (the server
//!   creates its directories `0700`) and log retention.
//!
//! Standard library only, like the rest of this crate.

use std::fs::{File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use super::audit::AuditEvent;

/// Default rotation cap: 64 MiB per audit file.
pub const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// One JSON audit record per line, appended and fsynced.
///
/// `Mutex<File>` serializes concurrent appends from executor threads and
/// connection handlers; each append is a single `write_all` + `sync_all`
/// while holding the lock, so lines never interleave.
pub struct AuditSink {
    path: PathBuf,
    file: Mutex<File>,
    max_bytes: u64,
}

impl AuditSink {
    /// Opens (creating parents as needed) the audit file for append with
    /// the default size cap ([`DEFAULT_MAX_BYTES`]).
    pub fn open(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        Self::open_with_max_bytes(path, DEFAULT_MAX_BYTES)
    }

    /// Opens the audit file with an explicit rotation cap (bytes).
    ///
    /// `max_bytes` below one byte is clamped to one: rotation is by size,
    /// and a zero cap would rotate on every append.
    pub fn open_with_max_bytes(path: impl Into<PathBuf>, max_bytes: u64) -> std::io::Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self {
            path,
            file: Mutex::new(file),
            max_bytes: max_bytes.max(1),
        })
    }

    /// Appends one event as a single JSON line and `fsync`s the file.
    ///
    /// Rotates first when the new line would exceed the size cap (the
    /// rotated file keeps history; see the module docs).
    pub fn append(&self, event: &AuditEvent) -> std::io::Result<()> {
        let mut line = render_line(event);
        line.push('\n');
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if file.metadata()?.len() + line.len() as u64 > self.max_bytes {
            drop(file);
            self.rotate()?;
            file = self
                .file
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
        }
        file.write_all(line.as_bytes())?;
        // Durability boundary: one fsync per append. See module docs for
        // why batching is deliberately not done here.
        file.sync_all()?;
        Ok(())
    }

    /// Closes the sink, flushing and `fsync`ing any buffered tail.
    pub fn close(self) -> std::io::Result<()> {
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        file.flush()?;
        file.sync_all()?;
        Ok(())
    }

    /// Current path (the live file, never a rotated one).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Renames the live file to `audit-<unix-secs>.log` beside itself and
    /// opens a fresh live file, `fsync`ing the directory entry afterwards.
    fn rotate(&self) -> std::io::Result<()> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let rotated = self.path.with_file_name(format!("audit-{timestamp}.log"));
        let mut file = self
            .file
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        file.flush()?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&self.path, &rotated)?;
        let fresh = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        *self
            .file
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = fresh;
        // Crash safety of the rename itself: fsync the directory entry.
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = File::open(parent) {
                let _ = dir.sync_all();
            }
        } else if let Ok(dir) = File::open(".") {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}

/// Renders one event as a single JSON object line.
///
/// Keys are pinned: `ts,user,action,detail,object,decision,reason`.
/// `detail` carries the variant payload (auth method, statement, COPY
/// direction, admin operation, lockout state) so no decision context is
/// lost; `object` is `null` when the event names none. Only these
/// structured fields are serialized — the sink never sees secrets.
fn render_line(event: &AuditEvent) -> String {
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let (action, detail) = split_action(event);
    let object = match &event.object {
        Some(object) => format!("\"{}\"", escape(object)),
        None => "null".to_string(),
    };
    format!(
        "{{\"ts\":{timestamp},\"user\":\"{}\",\"action\":\"{action}\",\"detail\":\"{}\",\"object\":{object},\"decision\":\"{}\",\"reason\":\"{}\"}}",
        escape(&event.user),
        escape(&detail),
        event.decision.as_str(),
        escape(&event.reason),
    )
}

/// Splits an action into its stable name plus its variant payload.
fn split_action(event: &AuditEvent) -> (&'static str, String) {
    match &event.action {
        super::audit::AuditAction::Authenticate { method } => ("authenticate", method.clone()),
        super::audit::AuditAction::Authorize { statement } => ("authorize", statement.clone()),
        super::audit::AuditAction::CopyFile { direction } => ("copy_file", direction.clone()),
        super::audit::AuditAction::Admin { operation } => ("admin", operation.clone()),
        super::audit::AuditAction::Lockout { locked } => ("lockout", locked.to_string()),
    }
}

/// Minimal JSON string escaper (std only): quotes, backslashes, control
/// characters, and DEL; everything else passes through verbatim.
fn escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for c in raw.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 || (c as u32) == 0x7f => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

static SINK: OnceLock<AuditSink> = OnceLock::new();

/// Installs the process-global audit sink (server startup path).
///
/// Fails when a sink is already installed (startup installs exactly once)
/// or when the file cannot be opened (fail closed at startup). Empty paths
/// are rejected: "disabled" is represented by never calling this.
pub fn set_audit_log(path: impl Into<PathBuf>) -> std::io::Result<()> {
    let path = path.into();
    if path.as_os_str().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "audit log path must not be empty (leave --audit-log unset to disable)",
        ));
    }
    let sink = AuditSink::open(path)?;
    SINK.set(sink).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "audit log sink is already installed",
        )
    })?;
    Ok(())
}

/// True when [`set_audit_log`] installed a sink.
pub fn is_enabled() -> bool {
    SINK.get().is_some()
}

/// Emits one audit event to the durable sink, if one is installed.
///
/// This crate is std-only (no `tracing` dependency by design), so the
/// operational `tracing` event stays at the call site — the sink is purely
/// additive. Returns `Ok` when disabled or appended; on append failure
/// returns the error and lets the caller decide (call sites use `let _ =`
/// and keep their tracing line: the sink is a record, never a gate, so
/// enforcement decisions stand regardless of sink health).
pub fn emit(event: &AuditEvent) -> std::io::Result<()> {
    if let Some(sink) = SINK.get() {
        sink.append(event)?;
    }
    Ok(())
}
