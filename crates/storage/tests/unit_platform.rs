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
// Moved from `src/platform.rs` inline `#[cfg(test)]` — public-API only.
use plomid_storage::platform::FileExt;
use std::fs::OpenOptions;
use std::io;

fn scratch(name: &str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("plomid-platform-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

#[test]
fn positioned_write_and_read_round_trip() {
    let path = scratch("round-trip");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();

    file.write_all_at(b"hello world", 8).unwrap();
    let mut buffer = [0_u8; 11];
    file.read_exact_at(&mut buffer, 8).unwrap();
    assert_eq!(&buffer, b"hello world");

    // Overwrite a slice in place, leaving neighbours intact.
    file.write_all_at(b"WORLD", 14).unwrap();
    let mut buffer = [0_u8; 11];
    file.read_exact_at(&mut buffer, 8).unwrap();
    assert_eq!(&buffer, b"hello WORLD");

    let _ = std::fs::remove_file(&path);
}

#[test]
fn short_read_at_end_of_file_is_unexpected_eof() {
    let path = scratch("eof");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    file.write_all_at(b"abc", 0).unwrap();

    let mut buffer = [0_u8; 8];
    let error = file.read_exact_at(&mut buffer, 0).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn write_all_at_extends_a_sparse_file() {
    let path = scratch("sparse");
    let file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap();

    // Writing past the end (device/extent layout does this) must extend
    // the file durably at the requested offset.
    file.write_all_at(&[0xAB], 1 << 20).unwrap();
    assert_eq!(file.metadata().unwrap().len(), (1 << 20) + 1);
    let mut byte = [0_u8; 1];
    file.read_exact_at(&mut byte, 1 << 20).unwrap();
    assert_eq!(byte[0], 0xAB);

    let _ = std::fs::remove_file(&path);
}
