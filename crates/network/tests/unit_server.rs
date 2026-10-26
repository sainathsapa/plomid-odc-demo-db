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
// Moved from `src/server/mod.rs` inline `#[cfg(test)]` — public-API only.
use plomid_network::protocol::{ErrorResponse, MessageEncoder, TransactionStatus};
use plomid_network::server::*;
use plomid_network::session::{ServerAuthConfig, Session};

#[test]
fn session_tracks_transaction_status() {
    let mut session = Session::new(1, "alice", "mydb");
    assert_eq!(session.transaction_status, TransactionStatus::Idle);
    session.mark_in_transaction();
    assert_eq!(session.transaction_status, TransactionStatus::InTransaction);
    session.mark_idle();
    assert_eq!(session.transaction_status, TransactionStatus::Idle);
}

#[test]
fn session_parameters_are_populated() {
    let session = Session::new(1, "alice", "mydb");
    assert_eq!(session.get_parameter("server_version"), Some("17.0"));
    assert_eq!(session.get_parameter("client_encoding"), Some("UTF8"));
}

#[test]
fn library_session_defaults_stay_disabled() {
    // `Session::new` is the library default (tests, embedding): timeouts
    // stay disabled here; production defaults are stamped by the server
    // via `apply_timeout_defaults`, never by changing this constructor.
    let session = Session::new(1, "alice", "mydb");
    assert_eq!(session.get_parameter("statement_timeout"), Some("0"));
    assert_eq!(
        session.get_parameter("idle_in_transaction_session_timeout"),
        Some("0")
    );
}

#[test]
fn timeout_defaults_stamp_and_client_set_still_wins() {
    let mut session = Session::new(1, "alice", "mydb");
    session.apply_timeout_defaults(30_000, 60_000);
    assert_eq!(session.get_parameter("statement_timeout"), Some("30000"));
    assert_eq!(
        session.get_parameter("idle_in_transaction_session_timeout"),
        Some("60000")
    );
    // Client startup parameters / SET applied afterwards still win.
    session.set_parameter("statement_timeout", "5000");
    assert_eq!(session.get_parameter("statement_timeout"), Some("5000"));
}

#[test]
fn production_enforcement_defaults_are_safe() {
    let enforcement = ServerEnforcement::default();
    assert_eq!(enforcement.statement_timeout_ms, 30_000);
    assert_eq!(enforcement.idle_in_transaction_timeout_ms, 60_000);
    assert_eq!(enforcement.max_copy_buffer_bytes, 64 * 1024 * 1024);
    assert_eq!(enforcement.max_prepared_statements, 1024);
    assert_eq!(enforcement.max_portals, 1024);
    assert_eq!(ConnectionLimits::default().max_connections, 100);
}

#[test]
fn auth_config_validates_credentials() {
    let config = ServerAuthConfig::new("alice", "secret").expect("CSPRNG for auth bootstrap");
    assert!(config.authenticate("alice", "secret"));
    assert!(!config.authenticate("alice", "wrong"));
    assert!(!config.authenticate("bob", "secret"));
}

#[test]
fn auth_config_disabled_accepts_any() {
    let config = ServerAuthConfig::disabled();
    assert!(config.authenticate("anyone", "anything"));
}

#[test]
fn protocol_encoder_tag_and_length_are_correct() {
    let data = MessageEncoder::encode_ready_for_query(TransactionStatus::Idle);
    assert_eq!(data[0], b'Z');
    let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
    assert_eq!(len as usize, data.len() - 1);
}

#[test]
fn error_response_serializes_severity_code_and_message() {
    let err = ErrorResponse::new("ERROR", "42P01", "relation \"x\" does not exist");
    let data = MessageEncoder::encode_error_response(&err);
    let payload = std::str::from_utf8(&data[5..]).unwrap();
    assert!(payload.contains("SERROR"));
    assert!(payload.contains("C42P01"));
    assert!(payload.contains("Mrelation \"x\" does not exist"));
}

#[tokio::test]
async fn server_binds_to_configured_address() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let server = PlomidServer::bind(addr, ServerAuthConfig::disabled())
        .await
        .expect("server binds");
    assert_eq!(server.local_addr(), addr);
}
