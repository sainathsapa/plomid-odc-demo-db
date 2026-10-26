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
// Moved from `src/sink.rs` inline `#[cfg(test)]` — public-API only.
use plomid_security::audit::AuditEvent;
use plomid_security::sink::*;
use plomid_security::{AuditAction, AuditDecision};
use std::path::{Path, PathBuf};

fn unique_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "plomid-audit-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

fn read_lines(path: &Path) -> Vec<String> {
    let text = std::fs::read_to_string(path).expect("read audit file");
    text.lines().map(str::to_string).collect()
}

#[test]
fn append_allow_and_deny_both_land_durably() {
    // Integration pin for F-09: one allow AND one deny through the same
    // sink both survive on stable storage (fsync per append).
    let dir = unique_dir("allow-deny");
    let path = dir.join("audit.log");
    let sink = AuditSink::open(&path).expect("open");
    let allow = AuditEvent::allow(
        "alice",
        AuditAction::Authorize {
            statement: "SELECT".to_string(),
        },
        Some("TABLE public.t".to_string()),
    );
    let deny = AuditEvent::deny(
        "bob",
        AuditAction::Authorize {
            statement: "DELETE".to_string(),
        },
        Some("TABLE public.t".to_string()),
        "missing_privilege",
    );
    assert_eq!(allow.decision, AuditDecision::Allow);
    assert_eq!(deny.decision, AuditDecision::Deny);
    sink.append(&allow).expect("append allow");
    sink.append(&deny).expect("append deny");
    sink.close().expect("close");
    let lines = read_lines(&path);
    assert_eq!(lines.len(), 2, "got: {lines:?}");
    assert!(
        lines[0].contains("\"decision\":\"allow\""),
        "got: {}",
        lines[0]
    );
    assert!(lines[0].contains("\"user\":\"alice\""), "got: {}", lines[0]);
    assert!(
        lines[1].contains("\"decision\":\"deny\""),
        "got: {}",
        lines[1]
    );
    assert!(lines[1].contains("missing_privilege"), "got: {}", lines[1]);
}

#[test]
fn records_survive_reopen() {
    let dir = unique_dir("reopen");
    let path = dir.join("audit.log");
    {
        let sink = AuditSink::open(&path).expect("open");
        sink.append(&AuditEvent::deny(
            "alice",
            AuditAction::Authenticate {
                method: "scram".to_string(),
            },
            None,
            "bad_credentials",
        ))
        .expect("append");
        sink.close().expect("close");
    }
    {
        let sink = AuditSink::open(&path).expect("reopen");
        sink.append(&AuditEvent::allow(
            "alice",
            AuditAction::CopyFile {
                direction: "to".to_string(),
            },
            Some("TABLE public.t".to_string()),
        ))
        .expect("append");
        sink.close().expect("close");
    }
    let lines = read_lines(&path);
    assert_eq!(lines.len(), 2, "got: {lines:?}");
    assert!(
        lines[0].contains("\"action\":\"authenticate\""),
        "got: {}",
        lines[0]
    );
    assert!(
        lines[1].contains("\"action\":\"copy_file\""),
        "got: {}",
        lines[1]
    );
}

#[test]
fn rotation_renames_aside_and_starts_fresh() {
    let dir = unique_dir("rotate");
    let path = dir.join("audit.log");
    let sink = AuditSink::open_with_max_bytes(&path, 200).expect("open");
    for i in 0..8 {
        sink.append(&AuditEvent::deny(
            format!("user{i}"),
            AuditAction::Admin {
                operation: "vacuum".to_string(),
            },
            None,
            "test_rotation",
        ))
        .expect("append");
    }
    sink.close().expect("close");
    let rotated: Vec<_> = std::fs::read_dir(&dir)
        .expect("list")
        .filter_map(|entry| entry.ok().map(|e| e.file_name()))
        .filter(|name| name.to_string_lossy().starts_with("audit-"))
        .collect();
    assert!(
        !rotated.is_empty(),
        "rotation must rename the full file aside"
    );
    let live = read_lines(&path);
    assert!(
        !live.is_empty(),
        "live file keeps receiving appends after rotation"
    );
    let live_bytes = std::fs::metadata(&path).expect("stat").len();
    assert!(
        live_bytes <= 200 + 1024,
        "live file stays bounded, got {live_bytes}"
    );
}

#[test]
fn serialized_lines_carry_only_structured_fields() {
    // No-secret-leakage pin by construction: the sink serializes exactly
    // these keys from the structured event — there is no free-text field
    // a password, verifier, or token could flow through.
    let dir = unique_dir("keys");
    let path = dir.join("audit.log");
    let sink = AuditSink::open(&path).expect("open");
    sink.append(&AuditEvent::deny(
        "bob",
        AuditAction::Authenticate {
            method: "scram".to_string(),
        },
        None,
        "bad_password",
    ))
    .expect("append");
    sink.close().expect("close");
    let lines = read_lines(&path);
    assert_eq!(lines.len(), 1);
    let line = &lines[0];
    for key in [
        "ts", "user", "action", "detail", "object", "decision", "reason",
    ] {
        assert!(
            line.contains(&format!("\"{key}\":")) || line.contains(&format!("\"{key}\" :")),
            "missing key {key} in {line}"
        );
    }
    for forbidden in ["password", "secret", "token", "verifier", "credential"] {
        // Key-set pin, matched as JSON keys not substrings: legitimate
        // reason tokens like `bad_password` contain "password" as a
        // value, which is the fixed machine-stable vocabulary, not a
        // secret.
        assert!(
            !line.contains(&format!("\"{forbidden}\":")),
            "forbidden key {forbidden} in {line}"
        );
    }
    assert!(line.contains("\"object\":null"), "got: {line}");
}

#[test]
fn escaping_keeps_json_to_one_line_per_event() {
    let dir = unique_dir("escape");
    let path = dir.join("audit.log");
    let sink = AuditSink::open(&path).expect("open");
    sink.append(&AuditEvent::deny(
        "bob\"\\\n injections",
        AuditAction::Authorize {
            statement: "SELECT".to_string(),
        },
        Some("TABLE \"weird\"\nname".to_string()),
        "missing_privilege",
    ))
    .expect("append");
    sink.close().expect("close");
    let lines = read_lines(&path);
    assert_eq!(
        lines.len(),
        1,
        "one event must stay one line, got: {lines:?}"
    );
    assert!(
        lines[0].contains("\\\"") && lines[0].contains("\\n"),
        "got: {}",
        lines[0]
    );
}

#[test]
fn empty_path_is_rejected() {
    // "Disabled" is represented by never installing a sink: an empty
    // path must fail instead of creating a file with a junk name.
    assert!(
        set_audit_log("").is_err(),
        "global install must reject empty paths"
    );
}
