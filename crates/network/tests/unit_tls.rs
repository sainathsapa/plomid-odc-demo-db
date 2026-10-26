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
// Moved from `src/tls.rs` inline `#[cfg(test)]` — public-API only.
use plomid_network::tls::*;
use std::path::Path;

fn invalid_cert_dir(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let id = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("plomid-tls-{label}-{}-{id}", std::process::id()))
}

fn write_files(dir: &std::path::Path, cert_pem: &str, key_pem: &str) {
    std::fs::create_dir_all(dir).expect("fixture dir");
    std::fs::write(dir.join("cert.pem"), cert_pem).expect("cert fixture");
    std::fs::write(dir.join("key.pem"), key_pem).expect("key fixture");
}

#[test]
fn missing_files_fail_closed() {
    let dir = invalid_cert_dir("missing");
    let err = load_server_config(&dir.join("cert.pem"), &dir.join("key.pem"))
        .expect_err("missing files must fail");
    assert!(err.to_string().contains("cannot read"), "got: {err}");
}

#[test]
fn empty_paths_fail_closed() {
    let err = load_server_config(Path::new(""), Path::new("")).expect_err("empty paths must fail");
    assert!(
        err.to_string().contains("tls_requires_cert_and_key"),
        "got: {err}"
    );
}

#[test]
fn garbage_pem_fails_closed() {
    let dir = invalid_cert_dir("garbage");
    write_files(&dir, "not a pem file\n", "also not a pem file\n");
    let err = load_server_config(&dir.join("cert.pem"), &dir.join("key.pem"))
        .expect_err("garbage PEM must fail");
    let text = err.to_string();
    assert!(
        text.contains("no PEM certificate") || text.contains("invalid TLS certificate"),
        "got: {text}"
    );
    assert!(!text.contains("not a pem"), "error must not echo material");
}

#[test]
fn cert_without_key_fails_closed() {
    let dir = invalid_cert_dir("nokey");
    let key =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).expect("fixture cert");
    write_files(&dir, &key.cert.pem(), "no key here\n");
    let err = load_server_config(&dir.join("cert.pem"), &dir.join("key.pem"))
        .expect_err("missing key must fail");
    assert!(err.to_string().contains("private key"), "got: {err}");
}
