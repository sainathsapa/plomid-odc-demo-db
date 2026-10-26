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
// Moved from src/encoding/entries.rs inline #[cfg(test)].
use plomid_columnar::encoding::entries::{append_entry, validate_stream, EntryReader};
use plomid_core::ErrorKind;

fn stream(values: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        append_entry(&mut out, value).expect("append");
    }
    out
}

#[test]
fn reads_entries_in_order() {
    let bytes = stream(&[b"alpha", b"", b"z"]);
    let mut reader = EntryReader::new(&bytes, 3);
    assert_eq!(reader.next_entry().expect("entry"), Some(&b"alpha"[..]));
    assert_eq!(reader.next_entry().expect("entry"), Some(&b""[..]));
    assert_eq!(reader.next_entry().expect("entry"), Some(&b"z"[..]));
    assert_eq!(reader.next_entry().expect("end"), None);
    reader.finish().expect("well formed");
}

#[test]
fn rejects_truncation_and_trailing_bytes() {
    let bytes = stream(&[b"alpha", b"beta"]);
    let short = &bytes[..bytes.len() - 1];
    assert_eq!(
        validate_stream(short, 2).expect_err("truncated").kind(),
        ErrorKind::Corruption
    );
    assert!(validate_stream(&bytes, 3).is_err());
    assert!(validate_stream(&bytes, 1).is_err());
}

#[test]
fn empty_stream_holds_no_entries() {
    assert!(validate_stream(&[], 0).is_ok());
    assert!(validate_stream(&[], 1).is_err());
    assert!(validate_stream(&[0, 0, 0, 0], 0).is_err());
}

#[test]
fn rejects_an_impossible_length_prefix() {
    // A length prefix that promises more bytes than the stream holds.
    let bytes = [0xFF_u8, 0xFF, 0xFF, 0xFF, 1, 2, 3];
    assert!(validate_stream(&bytes, 1).is_err());
}
