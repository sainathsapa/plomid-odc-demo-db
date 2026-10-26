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
// Moved from `src/scram.rs` inline `#[cfg(test)]` — public-API only.
use plomid_network::scram::*;

#[test]
fn sha256_known_vector() {
    // SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223...
    let digest = sha256(b"abc");
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(
        hex,
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
}

#[test]
fn sha256_empty_and_long_input() {
    let empty = sha256(b"");
    assert_eq!(
        empty,
        [
            0xe3, 0xb0, 0xc4, 0x42, 0x98, 0xfc, 0x1c, 0x14, 0x9a, 0xfb, 0xf4, 0xc8, 0x99, 0x6f,
            0xb9, 0x24, 0x27, 0xae, 0x41, 0xe4, 0x64, 0x9b, 0x93, 0x4c, 0xa4, 0x95, 0x99, 0x1b,
            0x78, 0x52, 0xb8, 0x55,
        ]
    );
    // RFC 4231 test case 1: key=0x0b*20, data="Hi There"
    let hmac = hmac_sha256(&[0x0b; 20], b"Hi There");
    let hex: String = hmac.iter().map(|b| format!("{b:02x}")).collect();
    assert_eq!(
        hex,
        "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
    );
}

#[test]
fn base64_round_trip() {
    for input in [
        b"".as_slice(),
        b"f",
        b"fo",
        b"foo",
        b"foob",
        b"fooba",
        b"foobar",
    ] {
        let encoded = base64_encode(input);
        let decoded = base64_decode(&encoded).unwrap();
        assert_eq!(decoded, input, "round trip failed for {input:?}");
    }
    assert_eq!(base64_encode(b"n,,"), "biws");
}

#[test]
fn scram_exchange_verifies_valid_client() {
    // Simulate a full client/server SCRAM exchange.        // Simulate a full client/server SCRAM exchange.
    let mut server = ScramSession::new("secret").expect("CSPRNG salt");
    let client_first = "n,,n=alice,r=abcdef";
    let server_first = server.handle_initial(client_first).unwrap();
    let attrs = parse_attributes(&server_first);
    let salt = attrs.iter().find(|(n, _)| *n == "s").unwrap().1;
    let iterations: u32 = attrs
        .iter()
        .find(|(n, _)| *n == "i")
        .unwrap()
        .1
        .parse()
        .unwrap();
    let nonce = attrs.iter().find(|(n, _)| *n == "r").unwrap().1;
    let salt_bytes = base64_decode(salt).unwrap();

    // Client-side computation.
    let salted = pbkdf2_sha256(b"secret", &salt_bytes, iterations, 32);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let client_first_bare = "n=alice,r=abcdef";
    let without_proof = format!("c=biws,r={nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let client_sig = hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_sig[i];
    }
    let client_final = format!("{},p={}", without_proof, base64_encode(&proof));

    let server_final = server.handle_final(&client_final).unwrap();
    assert!(server_final.starts_with("v="), "got {server_final}");

    // Verify the server signature independently.
    let server_key = hmac_sha256(&salted, b"Server Key");
    let server_sig = hmac_sha256(&server_key, auth_message.as_bytes());
    assert_eq!(server_final, format!("v={}", base64_encode(&server_sig)));
}

#[test]
fn scram_rejects_wrong_password() {
    let mut server = ScramSession::new("correct").expect("CSPRNG salt");
    let server_first = server.handle_initial("n,,n=alice,r=nonce1").unwrap();
    let attrs = parse_attributes(&server_first);
    let salt = base64_decode(attrs.iter().find(|(n, _)| *n == "s").unwrap().1).unwrap();
    let iterations: u32 = attrs
        .iter()
        .find(|(n, _)| *n == "i")
        .unwrap()
        .1
        .parse()
        .unwrap();
    let nonce = attrs.iter().find(|(n, _)| *n == "r").unwrap().1;

    // Client uses the wrong password.
    let salted = pbkdf2_sha256(b"wrong", &salt, iterations, 32);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let auth_message = format!("n=alice,r=nonce1,{server_first},c=biws,r={nonce}");
    let client_sig = hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_sig[i];
    }
    let client_final = format!("c=biws,r={nonce},p={}", base64_encode(&proof));
    assert_eq!(
        server.handle_final(&client_final),
        Err(ScramError::InvalidProof)
    );
}

#[test]
fn scram_rejects_nonce_mismatch() {
    let mut server = ScramSession::new("secret").expect("CSPRNG salt");
    let _ = server.handle_initial("n,,n=alice,r=nonceA").unwrap();
    let client_final = "c=biws,r=nonceB,p=AAAA";
    assert_eq!(
        server.handle_final(client_final),
        Err(ScramError::NonceMismatch)
    );
}

#[test]
fn scram_rejects_malformed_initial() {
    let mut server = ScramSession::new("secret").expect("CSPRNG salt");
    assert_eq!(
        server.handle_initial("not-a-scram-message"),
        Err(ScramError::Malformed)
    );
    assert_eq!(server.handle_initial("n,,r="), Err(ScramError::Malformed));
}

#[test]
fn server_nonces_are_unique_per_exchange() {
    // Nonce suffixes come from the OS CSPRNG, never the clock: two
    // exchanges with the same client nonce must still differ, and the
    // suffix must be hex (comma-free for the attribute framing).
    let mut first = ScramSession::new("secret").expect("CSPRNG salt");
    let mut second = ScramSession::new("secret").expect("CSPRNG salt");
    let first_message = first
        .handle_initial("n,,n=alice,r=sameclientnonce")
        .unwrap();
    let second_message = second
        .handle_initial("n,,n=alice,r=sameclientnonce")
        .unwrap();
    assert_ne!(first_message, second_message);
    for message in [&first_message, &second_message] {
        let nonce = message
            .split(',')
            .find_map(|part| part.strip_prefix("r="))
            .expect("server-first carries r=");
        assert!(nonce.starts_with("sameclientnonce"));
        let suffix = &nonce["sameclientnonce".len()..];
        assert_eq!(suffix.len(), 32, "128-bit hex suffix");
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
