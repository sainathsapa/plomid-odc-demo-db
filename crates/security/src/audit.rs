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

//! Security audit events: one structured record per security decision.
//!
//! Emission is two-fold: a `tracing` event at the call site (operational
//! log, no secrets — passwords and verifiers never enter an event) and a
//! monotonic counter in `plomid-core::observability` surfaced via
//! `plomid_stat_security()`. Tests observe denials through the counters;
//! operators observe them through logs.

/// What was being attempted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditAction {
    /// Password/SCRAM authentication attempt.
    Authenticate { method: String },
    /// A SQL statement gated by object privileges.
    Authorize { statement: String },
    /// File `COPY` (server-filesystem access).
    CopyFile { direction: String },
    /// Role administration (create/alter/grant).
    Admin { operation: String },
    /// Account lockout state change.
    Lockout { locked: bool },
}

impl AuditAction {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Authenticate { .. } => "authenticate",
            Self::Authorize { .. } => "authorize",
            Self::CopyFile { .. } => "copy_file",
            Self::Admin { .. } => "admin",
            Self::Lockout { .. } => "lockout",
        }
    }
}

/// Whether the attempt was allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditDecision {
    Allow,
    Deny,
}

impl AuditDecision {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Deny => "deny",
        }
    }
}

/// One security decision. Never carries secrets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
    /// Authenticated-or-claimed identity (`startup user`, never verified
    /// here — verification is the `Authenticate` action itself).
    pub user: String,
    pub action: AuditAction,
    /// Object of the decision (`TABLE t`, `SCHEMA s`, …) if any.
    pub object: Option<String>,
    pub decision: AuditDecision,
    /// Short machine-stable reason (`missing_privilege`, `bad_password`,
    /// `locked_out`, …). Human detail goes to logs, not here.
    pub reason: String,
}

impl AuditEvent {
    pub fn deny(
        user: impl Into<String>,
        action: AuditAction,
        object: Option<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            user: user.into(),
            action,
            object,
            decision: AuditDecision::Deny,
            reason: reason.into(),
        }
    }

    pub fn allow(user: impl Into<String>, action: AuditAction, object: Option<String>) -> Self {
        Self {
            user: user.into(),
            action,
            object,
            decision: AuditDecision::Allow,
            reason: String::new(),
        }
    }

    /// One-line log rendering without secrets.
    pub fn log_line(&self) -> String {
        format!(
            "security_decision user={} action={} object={} decision={} reason={}",
            self.user,
            self.action.name(),
            self.object.as_deref().unwrap_or("-"),
            self.decision.as_str(),
            if self.reason.is_empty() {
                "-"
            } else {
                &self.reason
            },
        )
    }
}

/// Builds the wire-visible denial error (`42501 insufficient_privilege`).
///
/// The message names the missing privilege and object but never leaks
/// catalog internals, other roles' grants, or data values.
pub fn denial_error(user: &str, privilege: &str, object: &str) -> plomid_core::PlomidError {
    plomid_core::PlomidError::new(
        plomid_core::ErrorKind::PermissionDenied,
        format!("permission denied for {object}: role \"{user}\" lacks {privilege}"),
    )
}
