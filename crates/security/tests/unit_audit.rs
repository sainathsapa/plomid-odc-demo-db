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
// Moved from `src/audit.rs` inline `#[cfg(test)]` — public-API only.
use plomid_security::audit::*;

#[test]
fn denial_error_is_permission_denied_without_leakage() {
    let err = denial_error("bob", "SELECT", "TABLE t");
    assert_eq!(err.kind(), plomid_core::ErrorKind::PermissionDenied);
    assert_eq!(err.code(), "PL-PERMISSION-DENIED");
    let text = err.message().to_string();
    assert!(text.contains("bob") && text.contains("TABLE t"));
}

#[test]
fn audit_event_renders_without_secrets() {
    let event = AuditEvent::deny(
        "bob",
        AuditAction::Authenticate {
            method: "scram".to_string(),
        },
        None,
        "bad_password",
    );
    let line = event.log_line();
    assert!(line.contains("decision=deny") && line.contains("bad_password"));
    assert!(!line.contains("secret"));
}
