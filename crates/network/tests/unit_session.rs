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
// Moved from `src/session.rs` inline `#[cfg(test)]` — public-API only.
mod auth_tests {
    use plomid_network::session::*;

    #[test]
    fn md5_authentication_matches_postgres_response_shape() {
        let config = ServerAuthConfig::md5("alice", "secret").expect("CSPRNG");
        let salt = [1, 2, 3, 4];
        // Expected response computed via the audited `md-5` path (same
        // function the wire layer uses), not a parallel implementation.
        let response = plomid_network::session::md5_password_response("secret", "alice", salt);
        assert!(response.starts_with("md5") && response.len() == 35);
        assert!(config.authenticate_md5("alice", &response, salt));
        assert!(!config.authenticate_md5("alice", "md5bad", salt));
    }

    #[test]
    fn debug_redacts_password_and_verifier() {
        let config = ServerAuthConfig::scram("alice", "s3cr3t-pw").expect("CSPRNG");
        let rendered = format!("{config:?}");
        assert!(!rendered.contains("s3cr3t-pw"), "got: {rendered}");
        assert!(rendered.contains("<redacted>"), "got: {rendered}");
    }

    #[test]
    fn scram_salts_are_random_per_role_but_both_authenticate() {
        let first = ServerAuthConfig::scram("alice", "same-password").expect("CSPRNG");
        let second = ServerAuthConfig::scram("bob", "same-password").expect("CSPRNG");
        assert_ne!(
            first.scram_salt, second.scram_salt,
            "per-role salts must differ for the same password"
        );
        assert_ne!(
            first.scram_salted_password.as_ref(),
            second.scram_salted_password.as_ref()
        );
        assert!(first.authenticate("alice", "same-password"));
        assert!(second.authenticate("bob", "same-password"));
        for config in [&first, &second] {
            let mut session = plomid_network::scram::ScramSession::with_verifier(
                &config.password,
                config.scram_salt,
                &config.scram_salted_password,
            );
            let server_first = session
                .handle_initial("n,,n=user,r=clientnonce123")
                .expect("server-first");
            let attrs: Vec<(&str, &str)> = server_first
                .split(',')
                .filter_map(|part| part.split_once('='))
                .collect();
            let salt_b64 = attrs.iter().find(|(k, _)| *k == "s").unwrap().1;
            let iterations: u32 = attrs
                .iter()
                .find(|(k, _)| *k == "i")
                .unwrap()
                .1
                .parse()
                .unwrap();
            let nonce = attrs.iter().find(|(k, _)| *k == "r").unwrap().1;
            let salt_bytes = plomid_network::scram::base64_decode(salt_b64).unwrap();
            let salted =
                plomid_network::scram::pbkdf2_sha256(b"same-password", &salt_bytes, iterations, 32);
            let client_key = plomid_network::scram::hmac_sha256(&salted, b"Client Key");
            let stored_key = plomid_network::scram::sha256(&client_key);
            let without_proof = format!("c=biws,r={nonce}");
            let auth_message = format!("n=user,r=clientnonce123,{server_first},{without_proof}");
            let client_sig =
                plomid_network::scram::hmac_sha256(&stored_key, auth_message.as_bytes());
            let mut proof = [0u8; 32];
            for i in 0..32 {
                proof[i] = client_key[i] ^ client_sig[i];
            }
            let client_final = format!(
                "{},p={}",
                without_proof,
                plomid_network::scram::base64_encode(&proof)
            );
            assert!(session.handle_final(&client_final).is_ok());
        }
    }
}

mod staging_cap_tests {
    use plomid_network::session::*;

    #[test]
    fn isolation_defaults_to_read_committed_without_rr_pin() {
        let mut session = Session::new(1, "plomid", "plomid");
        assert_eq!(session.default_isolation, IsolationLevel::ReadCommitted);
        assert_eq!(session.isolation, IsolationLevel::ReadCommitted);
        assert!(!session.is_repeatable_read());
        session.mark_in_transaction_with(IsolationLevel::RepeatableRead);
        assert!(session.is_repeatable_read());
        session.rr_txn_id = Some(42);
        assert_eq!(session.take_rr_txn(), Some(42));
        assert_eq!(session.take_rr_txn(), None);
        session.mark_idle();
        assert!(!session.is_repeatable_read());
        assert_eq!(session.isolation, IsolationLevel::ReadCommitted);
    }
}
