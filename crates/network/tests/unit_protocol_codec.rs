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
// Moved from `src/protocol/codec.rs` inline `#[cfg(test)]` — public-API only.
use plomid_network::protocol::codec::*;
use plomid_network::protocol::{
    AuthMechanism, ErrorResponse, RowFieldDescription, TransactionStatus,
};

#[test]
fn encode_authentication_ok_has_correct_tag_and_length() {
    let data = MessageEncoder::encode_authentication(AuthMechanism::Ok);
    assert_eq!(data[0], b'R');
    let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
    assert_eq!(len, 8);
    assert_eq!(data[5..9], [0, 0, 0, 0]);
}

#[test]
fn encode_authentication_cleartext_has_correct_tag_and_length() {
    let data = MessageEncoder::encode_authentication(AuthMechanism::CleartextPassword);
    assert_eq!(data[0], b'R');
    let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
    assert_eq!(len, 8);
    assert_eq!(data[5..9], [0, 0, 0, 3]);
}

#[test]
fn encode_ready_for_query_idle() {
    let data = MessageEncoder::encode_ready_for_query(TransactionStatus::Idle);
    assert_eq!(data[0], b'Z');
    let len = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
    assert_eq!(len, 5);
    assert_eq!(data[5], b'I');
}

#[test]
fn encode_ready_for_query_in_transaction() {
    let data = MessageEncoder::encode_ready_for_query(TransactionStatus::InTransaction);
    assert_eq!(data[5], b'T');
}

#[test]
fn encode_command_complete() {
    let data = MessageEncoder::encode_command_complete("CREATE TABLE");
    assert_eq!(data[0], b'C');
    let payload = &data[5..];
    assert!(payload.starts_with(b"CREATE TABLE\0"));
}

#[test]
fn encode_error_response_has_required_fields() {
    let error = ErrorResponse::new("ERROR", "42P01", "relation \"missing\" does not exist");
    let data = MessageEncoder::encode_error_response(&error);
    assert_eq!(data[0], b'E');
    let payload_str = std::str::from_utf8(&data[5..]).unwrap();
    assert!(payload_str.contains("SERROR"));
    assert!(payload_str.contains("C42P01"));
    assert!(payload_str.contains("Mrelation \"missing\" does not exist"));
}

#[test]
fn encode_error_response_with_detail_and_hint() {
    let error = ErrorResponse::new("ERROR", "42601", "syntax error")
        .with_detail("missing semicolon")
        .with_hint("add a semicolon at the end");
    let data = MessageEncoder::encode_error_response(&error);
    let payload_str = std::str::from_utf8(&data[5..]).unwrap();
    assert!(payload_str.contains("Dmissing semicolon"));
    assert!(payload_str.contains("Hadd a semicolon at the end"));
}

#[test]
fn encode_row_description_maps_columns() {
    let fields = vec![
        RowFieldDescription {
            name: "id".to_string(),
            type_oid: 23,
            type_size: 4,
            type_modifier: -1,
            format: 0,
        },
        RowFieldDescription {
            name: "name".to_string(),
            type_oid: 25,
            type_size: -1,
            type_modifier: -1,
            format: 0,
        },
    ];
    let data = MessageEncoder::encode_row_description(&fields);
    assert_eq!(data[0], b'T');
    let field_count = u16::from_be_bytes([data[5], data[6]]);
    assert_eq!(field_count, 2);
}

#[test]
fn encode_data_row_handles_null_and_values() {
    let values = vec![Some(b"1".to_vec()), None, Some(b"Alice".to_vec())];
    let data = MessageEncoder::encode_data_row(&values);
    assert_eq!(data[0], b'D');
    let col_count = u16::from_be_bytes([data[5], data[6]]);
    assert_eq!(col_count, 3);
}

#[test]
fn encode_parameter_status() {
    let data = MessageEncoder::encode_parameter_status("server_version", "14.0");
    assert_eq!(data[0], b'S');
    let payload = std::str::from_utf8(&data[5..]).unwrap();
    assert!(payload.starts_with("server_version\0"));
    assert!(payload.ends_with("14.0\0"));
}

#[test]
fn encode_backend_key_data() {
    let data = MessageEncoder::encode_backend_key_data(12345, 67890);
    assert_eq!(data[0], b'K');
    let pid = u32::from_be_bytes([data[5], data[6], data[7], data[8]]);
    assert_eq!(pid, 12345);
    let key = u32::from_be_bytes([data[9], data[10], data[11], data[12]]);
    assert_eq!(key, 67890);
}

#[test]
fn parse_startup_extracts_parameters() {
    let mut payload = Vec::new();
    payload.extend_from_slice(&196608u32.to_be_bytes());
    payload.extend_from_slice(b"user\0plomid\0database\0mydb\0\0");
    let params = MessageDecoder::parse_startup(&payload).unwrap();
    assert_eq!(params.get("user"), Some(&"plomid".to_string()));
    assert_eq!(params.get("database"), Some(&"mydb".to_string()));
}

#[test]
fn parse_startup_handles_empty_payload() {
    let result = MessageDecoder::parse_startup(&[]);
    assert!(result.is_err(), "empty payload must be rejected");
}

#[test]
fn parse_startup_returns_error_for_short_payload() {
    let result = MessageDecoder::parse_startup(&[1, 2, 3]);
    assert!(
        result.is_err(),
        "payload shorter than version must be rejected"
    );
}

fn frontend_frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(5 + payload.len());
    frame.push(tag);
    frame.extend_from_slice(&((payload.len() as u32 + 4).to_be_bytes()));
    frame.extend_from_slice(payload);
    frame
}

/// Several complete protocol messages arriving in one TCP read must each be
/// delivered intact and in order: read boundaries are not message boundaries.
#[tokio::test]
async fn read_frontend_delivers_coalesced_messages_in_order() {
    use tokio::io::AsyncWriteExt;
    let (mut writer, mut reader) = tokio::io::duplex(65536);
    let mut bytes = frontend_frame(b'Q', b"SELECT 1;\x00");
    bytes.extend_from_slice(&frontend_frame(b'S', b""));
    bytes.extend_from_slice(&frontend_frame(b'Q', b"SELECT 2;\x00"));
    writer.write_all(&bytes).await.expect("write");
    use plomid_network::protocol::FrontendTag;
    for (tag, body) in [
        (FrontendTag::Query, b"SELECT 1;\x00".as_slice()),
        (FrontendTag::Sync, b"".as_slice()),
        (FrontendTag::Query, b"SELECT 2;\x00".as_slice()),
    ] {
        let (got_tag, got_body) = MessageDecoder::read_frontend(&mut reader)
            .await
            .expect("read")
            .expect("message");
        assert_eq!(got_tag, tag);
        assert_eq!(got_body, body);
    }
}

/// One message fragmented into 1-byte reads — including splits inside the
/// 5-byte header — must still reassemble exactly.
#[tokio::test]
async fn read_frontend_reassembles_byte_fragmented_message() {
    use tokio::io::AsyncWriteExt;
    let (mut writer, mut reader) = tokio::io::duplex(65536);
    let frame = frontend_frame(b'Q', b"SELECT 'a;b';\x00");
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u8>(1);
    tokio::spawn(async move {
        for byte in frame {
            tx.send(byte).await.expect("send");
        }
    });
    let writer_task = tokio::spawn(async move {
        while let Some(byte) = rx.recv().await {
            writer.write_all(&[byte]).await.expect("write");
        }
    });
    let (tag, body) = MessageDecoder::read_frontend(&mut reader)
        .await
        .expect("read")
        .expect("message");
    writer_task.await.expect("writer drains");
    assert_eq!(tag, plomid_network::protocol::FrontendTag::Query);
    assert_eq!(body, b"SELECT 'a;b';\x00");
}

/// Absurd lengths are rejected before any allocation: no hang, no panic,
/// no uncontrolled memory growth.
#[tokio::test]
async fn read_frontend_rejects_oversized_and_short_lengths() {
    use tokio::io::AsyncWriteExt;
    for len in [0u32, 1, 3, u32::MAX] {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let mut header = vec![b'Q'];
        header.extend_from_slice(&len.to_be_bytes());
        writer.write_all(&header).await.expect("write");
        drop(writer);
        assert!(
            MessageDecoder::read_frontend(&mut reader).await.is_err(),
            "length {len} must be rejected"
        );
    }
}

/// EOF mid-header is a clean termination (no partial command exists yet).
#[tokio::test]
async fn read_frontend_eof_mid_header_terminates_cleanly() {
    let (_writer, mut reader) = tokio::io::duplex(64);
    drop(_writer);
    assert!(MessageDecoder::read_frontend(&mut reader)
        .await
        .expect("read")
        .is_none());
}

/// EOF mid-payload is an error, never a partial dispatch: the caller closes
/// the connection without executing anything (see the mid-frame live test).
#[tokio::test]
async fn read_frontend_eof_mid_payload_is_an_error() {
    use tokio::io::AsyncWriteExt;
    let (mut writer, mut reader) = tokio::io::duplex(64);
    let mut header = vec![b'Q'];
    header.extend_from_slice(&100u32.to_be_bytes());
    header.extend_from_slice(b"short");
    writer.write_all(&header).await.expect("write");
    drop(writer);
    assert!(MessageDecoder::read_frontend(&mut reader).await.is_err());
}
