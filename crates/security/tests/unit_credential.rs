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
// Moved from `src/credential.rs` inline `#[cfg(test)]` — public-API only.
use plomid_security::credential::*;

fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn official_sha256_vectors() {
    // FIPS 180-4 §B, now exercised against the `sha2` crate.
    let abc = sha256(b"abc");
    assert_eq!(
        hex_of(&abc),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    let long = sha256(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
    assert_eq!(
        hex_of(&long),
        "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
    );
    // One-block / multi-block boundary plus empty input.
    assert_eq!(
        hex_of(&sha256(b"")),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn official_hmac_sha256_vectors() {
    // RFC 4231 test cases 1–3 (HMAC-SHA-256), cross-checked against
    // Python hashlib and OpenSSL (all three agree; hand transcription
    // from memory is NOT trusted — see Phase 3 notes).
    let cases = [
            ("0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b", "4869205468657265",
             "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"),
            ("4a656665", "7768617420646f2079612077616e7420666f72206e6f7468696e673f",
             "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"),
            ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
             "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
             "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe"),
        ];
    for (key_hex, data_hex, expected) in cases {
        let key = hex_decode(key_hex);
        let data = hex_decode(data_hex);
        assert_eq!(hex_of(&hmac_sha256(&key, &data)), expected);
    }
    // Long-key path (> block size) hashes the key first.
    let long_key = vec![0xAAu8; 131];
    assert_eq!(
        hex_of(&hmac_sha256(
            &long_key,
            b"Test Using Larger Than Block-Size Key - Hash Key First"
        )),
        "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
    );
}

fn hex_decode(hex: &str) -> Vec<u8> {
    let hex: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn official_pbkdf2_vectors() {
    // PBKDF2-HMAC-SHA-256 (RFC 7914 §11 test vectors), cross-checked
    // against Python hashlib (dkLen-exact: 32 bytes = 64 hex, 40 = 80).
    assert_eq!(
        hex_of(&pbkdf2_sha256(b"password", b"salt", 1, 32)),
        "120fb6cffcf8b32c43e7225256c4f837a86548c92ccc35480805987cb70be17b"
    );
    assert_eq!(
        hex_of(&pbkdf2_sha256(b"password", b"salt", 2, 32)),
        "ae4d0c95af6b46d32d0adff928f06dd02a303f8ef3c251dfd6e2d85a95474c43"
    );
    assert_eq!(
        hex_of(&pbkdf2_sha256(b"password", b"salt", 4096, 40)),
        "c5e478d59288c841aa530db6845c4c8d962893a001ce4e11a4963873aa98134af7ad98c1b458ce3f"
    );
    assert_eq!(
        hex_of(&pbkdf2_sha256(
            b"passwordPASSWORDpassword",
            b"saltSALTsaltSALTsaltSALTsaltSALTsalt",
            4096,
            40
        )),
        "348c89dbcbd32b2f32d814b8116e84cf2b17347ebc1800181c4e2a1fb8dd53e1c635518c7dac47e9"
    );
}

#[test]
fn official_base64_vectors() {
    // RFC 4648 §10.
    for (plain, encoded) in [
        ("", ""),
        ("f", "Zg=="),
        ("fo", "Zm8="),
        ("foo", "Zm9v"),
        ("foob", "Zm9vYg=="),
        ("fooba", "Zm9vYmE="),
        ("foobar", "Zm9vYmFy"),
    ] {
        assert_eq!(base64_encode(plain.as_bytes()), encoded);
        assert_eq!(base64_decode(encoded).unwrap(), plain.as_bytes());
    }
    assert!(base64_decode("!!!").is_none());
}

#[test]
fn official_md5_vectors() {
    // RFC 1321 A.5, via the `md-5` crate (legacy path only).
    for (input, expected) in [
        ("", "d41d8cd98f00b204e9800998ecf8427e"),
        ("a", "0cc175b9c0f1b6a831c399e269772661"),
        ("abc", "900150983cd24fb0d6963f7d28e17f72"),
        ("message digest", "f96b697d7cb7938d525a2f31aaf161d0"),
    ] {
        use md5::{Digest as _, Md5};
        assert_eq!(format!("{:x}", Md5::digest(input.as_bytes())), expected);
    }
    // The PG wire response wrapper preserves PostgreSQL's format.
    let response = md5_password_response("secret", "alice", [1, 2, 3, 4]);
    assert!(response.starts_with("md5") && response.len() == 35);
}

#[test]
fn credential_round_trip_and_verify() {
    let cred = RoleCredential::derive(b"correct horse", [7u8; 16]);
    assert!(cred.verify(b"correct horse"));
    assert!(!cred.verify(b"correct horsf"));
    assert!(!cred.verify(b""));
    let encoded = cred.encode();
    assert_eq!(RoleCredential::decode(&encoded), Some(cred));
    assert!(RoleCredential::decode("nope").is_none());
    assert!(RoleCredential::decode("0:AA:AA").is_none());
}

#[test]
fn constant_time_eq_basics() {
    assert!(constant_time_eq(b"abc", b"abc"));
    assert!(!constant_time_eq(b"abc", b"abd"));
    assert!(!constant_time_eq(b"abc", b"abcd"));
    assert!(!constant_time_eq(b"", b"x"));
    assert!(constant_time_eq(b"", b""));
    // 32-byte derived-key shape (the hot path): equal lengths compare.
    assert!(constant_time_eq(&[1u8; 32], &[1u8; 32]));
    assert!(!constant_time_eq(&[1u8; 32], &[2u8; 32]));
}

#[test]
fn csprng_salts_are_unique_and_sane() {
    // OsRng smoke: two salts must differ (2^-128 collision odds) and
    // must never be degenerate.
    let first = random_salt().expect("CSPRNG available in test");
    let second = random_salt().expect("CSPRNG available in test");
    assert_ne!(first, second);
    assert_ne!(first, [0u8; 16]);
}

#[test]
fn salt_error_is_explicit_and_fail_closed() {
    // The error carries no sensitive material and names the policy.
    let err = SaltError::os_unavailable("test-injected failure");
    let text = err.to_string();
    assert!(text.contains("CSPRNG unavailable") && text.contains("refusing to continue"));
    assert!(format!("{err:?}").contains("test-injected failure"));
}
