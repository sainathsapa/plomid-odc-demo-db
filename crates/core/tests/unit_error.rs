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
// Moved from `src/error.rs` inline `#[cfg(test)]` — public-API only.
use plomid_core::error::{ErrorKind, PlomidError};
use std::{error::Error, io};

#[test]
fn displays_stable_codes_for_major_kinds() {
    let cases = [
        (ErrorKind::Io, "PL-IO"),
        (ErrorKind::Corruption, "PL-CORRUPTION"),
        (ErrorKind::InvalidArgument, "PL-INVALID-ARGUMENT"),
        (ErrorKind::NotFound, "PL-NOT-FOUND"),
        (ErrorKind::AlreadyExists, "PL-ALREADY-EXISTS"),
        (ErrorKind::Conflict, "PL-CONFLICT"),
        (ErrorKind::Aborted, "PL-ABORTED"),
        (ErrorKind::Internal, "PL-INTERNAL"),
        (ErrorKind::Unsupported, "PL-UNSUPPORTED"),
        (ErrorKind::Syntax, "PL-SYNTAX"),
        (ErrorKind::Catalog, "PL-CATALOG"),
        (ErrorKind::Transaction, "PL-TRANSACTION"),
        (ErrorKind::Wal, "PL-WAL"),
        (ErrorKind::RowEncoding, "PL-ROW-ENCODING"),
        (ErrorKind::PermissionDenied, "PL-PERMISSION-DENIED"),
    ];

    for (kind, code) in cases {
        let error = PlomidError::new(kind, "operation failed");
        assert_eq!(error.code(), code);
        assert_eq!(error.kind(), kind);
        assert_eq!(error.to_string(), format!("{code}: operation failed"));
    }
}

#[test]
fn converts_io_error_and_preserves_source_without_path_leakage() {
    let source = io::Error::new(io::ErrorKind::PermissionDenied, "/private/secret.key");
    let error = PlomidError::from(source);

    assert_eq!(error.kind(), ErrorKind::Io);
    assert!(error.source().is_some());
    assert!(error.to_string().contains("PermissionDenied"));
    assert!(!error.to_string().contains("/private/secret.key"));
}

#[test]
fn carries_internal_detail() {
    let error = PlomidError::with_detail(
        ErrorKind::Corruption,
        "corrupt row data",
        "table=users key=users:1 version=1",
    );
    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert_eq!(error.message(), "corrupt row data");
    assert_eq!(error.detail(), Some("table=users key=users:1 version=1"));
    assert!(error.to_string().contains("PL-CORRUPTION"));
    assert!(!error.to_string().contains("table=users"));
}

#[test]
fn is_send_and_sync() {
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<PlomidError>();
}
