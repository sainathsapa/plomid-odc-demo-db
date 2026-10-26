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
//
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//! TLS transport boundary for the PostgreSQL wire protocol (Phase 1).
//!
//! The TLS stack is [`rustls`] (audited, supported) via [`tokio_rustls`].
//! No TLS or cryptographic primitive is implemented here: this module only
//! loads operator-supplied PEM material, validates it fail-closed, answers
//! the PostgreSQL `SSLRequest` negotiation, and upgrades the transport.
//!
//! Protocol behavior (PostgreSQL-compatible):
//!
//! * TLS disabled: every `SSLRequest` is answered `N` (plaintext), exactly
//!   as before. `GSSENCRequest` is always answered `N` (GSSAPI unsupported).
//! * TLS enabled (optional): `SSLRequest` is answered `S`, the rustls
//!   handshake runs, and the startup message arrives over the encrypted
//!   stream. Plaintext startup still works (compatibility mode).
//! * TLS required: as above, but a client that sends a plaintext startup
//!   without negotiating TLS is refused with `FATAL 28000` before
//!   authentication. Refusal happens before any credential exchange.
//!
//! Certificate/key validation at load (fail closed):
//!
//! * both files must exist and be readable;
//! * at least one PEM certificate block must parse;
//! * exactly one private key (PKCS#8 / RSA / SEC1) must parse;
//! * the leaf certificate must be within its validity window
//!   (expired or not-yet-valid material is rejected at startup, not at the
//!   first client handshake);
//! * the key must be accepted for the certificate chain by rustls.
//!
//! This module never logs key material. Errors name the file and the
//! failure class only.

use std::fmt;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

/// PostgreSQL `SSLRequest` code (client asks to upgrade to TLS).
pub const SSL_REQUEST_CODE: u32 = 80877103;
/// PostgreSQL `GSSENCRequest` code (always declined: no GSSAPI support).
pub const GSSENC_REQUEST_CODE: u32 = 80877104;
/// PostgreSQL `CancelRequest` code (16-byte form carries pid + secret).
pub const CANCEL_REQUEST_CODE: u32 = 80877102;

/// SQLSTATE used when plaintext is refused under `--tls-required`.
///
/// Mirrors PostgreSQL's own `hostssl` refusal (`no pg_hba.conf entry ...
/// SSL off`), so drivers and operators see a familiar code.
pub const TLS_REQUIRED_SQLSTATE: &str = "28000";

/// Message sent when a plaintext startup arrives while TLS is required.
pub const TLS_REQUIRED_MESSAGE: &str =
    "TLS is required for this listener (SSL off); reconnect with sslmode=require";

/// Certificate/key load failure. Carries file + failure class only —
/// never key material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsLoadError(String);

impl TlsLoadError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for TlsLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TlsLoadError {}

/// Loads and validates the TLS server identity from PEM files.
///
/// See the module documentation for the validation checklist. Returns a
/// rustls [`rustls::ServerConfig`] ready to wrap in [`TlsAcceptor`].
pub fn load_server_config(
    cert_path: &Path,
    key_path: &Path,
) -> Result<rustls::ServerConfig, TlsLoadError> {
    let cert_display = cert_path.display().to_string();
    let key_display = key_path.display().to_string();
    if cert_display.is_empty() || key_display.is_empty() {
        return Err(TlsLoadError::new(
            "tls_requires_cert_and_key; pass --tls-cert and --tls-key",
        ));
    }
    let cert_bytes = std::fs::read(cert_path).map_err(|e| {
        TlsLoadError::new(format!("cannot read TLS certificate {cert_display}: {e}"))
    })?;
    let key_bytes = std::fs::read(key_path).map_err(|e| {
        TlsLoadError::new(format!("cannot read TLS private key {key_display}: {e}"))
    })?;

    // PEM parsing via rustls-pki-types (rustls-pemfile is unmaintained,
    // RUSTSEC-2025-0134, and fails `cargo deny`).
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> = {
        use rustls_pki_types::pem::PemObject as _;
        rustls::pki_types::CertificateDer::pem_slice_iter(&cert_bytes)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                TlsLoadError::new(format!(
                    "invalid TLS certificate PEM in {cert_display}: {e}"
                ))
            })?
    };
    if certs.is_empty() {
        return Err(TlsLoadError::new(format!(
            "no PEM certificate found in {cert_display}"
        )));
    }
    let key = {
        use rustls_pki_types::pem::PemObject as _;
        rustls::pki_types::PrivateKeyDer::from_pem_slice(&key_bytes).map_err(|e| {
            TlsLoadError::new(format!("invalid TLS private key PEM in {key_display}: {e}"))
        })?
    };

    check_leaf_validity(&certs[0], &cert_display)?;

    rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            TlsLoadError::new(format!(
                "TLS certificate/key rejected for {cert_display}: {e}"
            ))
        })
}

/// Rejects expired or not-yet-valid leaf certificates at startup.
///
/// rustls accepts the chain regardless of dates (expiry is a client-side
/// concern), so without this check an operator could deploy dead material
/// and only learn about it from failing clients. Fail closed here instead.
fn check_leaf_validity(
    leaf: &rustls::pki_types::CertificateDer<'_>,
    cert_display: &str,
) -> Result<(), TlsLoadError> {
    use x509_parser::prelude::FromDer as _;
    let (_, cert) =
        x509_parser::prelude::X509Certificate::from_der(leaf.as_ref()).map_err(|e| {
            TlsLoadError::new(format!("cannot parse X.509 leaf in {cert_display}: {e}"))
        })?;
    if !cert.validity().is_valid() {
        return Err(TlsLoadError::new(format!(
            "TLS certificate in {cert_display} is expired or not yet valid"
        )));
    }
    Ok(())
}

/// Cloneable TLS accept state shared by every connection task.
#[derive(Clone)]
pub struct TlsAcceptor {
    inner: tokio_rustls::TlsAcceptor,
    required: bool,
}

impl TlsAcceptor {
    /// Wraps a validated rustls server config. `required` mirrors
    /// `--tls-required`: plaintext startups are refused (FATAL 28000).
    pub fn new(config: rustls::ServerConfig, required: bool) -> Self {
        Self {
            inner: tokio_rustls::TlsAcceptor::from(std::sync::Arc::new(config)),
            required,
        }
    }

    /// True when plaintext connections must be refused.
    pub fn is_required(&self) -> bool {
        self.required
    }
}

/// The negotiated per-connection transport: plaintext TCP or a
/// rustls-encrypted stream. Implements [`AsyncRead`] + [`AsyncWrite`] by
/// delegation, so the PGWire handler stays transport-agnostic after the
/// pre-startup negotiation completes.
pub enum Transport {
    /// Unencrypted TCP (loopback dev or explicit trusted-network opt-in).
    Plain(TcpStream),
    /// rustls-encrypted stream after a successful `SSLRequest` upgrade.
    /// Boxed: the TLS session state dwarfs a plain socket, and this enum
    /// travels by value through negotiation into the handler.
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl Transport {
    /// True for the encrypted variant (used for metrics + required-mode).
    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }

    /// Disables Nagle buffering on the underlying TCP socket, both variants.
    pub fn set_nodelay(&self, nodelay: bool) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.set_nodelay(nodelay),
            Self::Tls(s) => s.get_ref().0.set_nodelay(nodelay),
        }
    }
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Outcome of the pre-startup negotiation (runs inside the per-connection
/// task, so a stalled client never blocks the accept loop).
pub enum Incoming {
    /// A `CancelRequest`: look the target up, set its flag, close quietly.
    Cancel { pid: u32, secret: u32 },
    /// A defensive close (8-byte `CancelRequest` shape): no pid/secret is
    /// carried, so there is nothing to look up. Preserves the historical
    /// quiet-close behavior for this malformed shape.
    Close,
    /// A normal startup payload plus the negotiated transport.
    Startup {
        transport: Transport,
        /// Bytes after the 4-byte length word (protocol version + params).
        payload: Vec<u8>,
        /// True when the client upgraded via `SSLRequest` + rustls.
        used_tls: bool,
    },
}

/// Runs `SSLRequest`/`GSSENCRequest`/`CancelRequest` negotiation on a fresh
/// connection and returns the startup payload with its transport.
///
/// Reads are exact-sized and unbuffered (no `BufReader` readahead), so no
/// handshake byte can be stranded in userspace buffers during the
/// plaintext→TLS upgrade. Every rejection path is fail-closed: unknown
/// codes, oversized lengths, and failed handshakes close the connection
/// without reaching authentication.
pub async fn negotiate(stream: TcpStream, tls: Option<&TlsAcceptor>) -> std::io::Result<Incoming> {
    let mut transport = Transport::Plain(stream);
    let mut used_tls = false;
    loop {
        let mut len_buf = [0u8; 4];
        transport.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        if len < 4 || len - 4 > crate::protocol::MAX_FRONTEND_MESSAGE_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid or oversized startup length: {len}"),
            ));
        }
        // CancelRequest is a complete, untagged 16-byte frontend message
        // (len + code + pid + secret). There is no response.
        if len == 16 {
            let mut rest = [0u8; 12];
            transport.read_exact(&mut rest).await?;
            let code = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
            if code == CANCEL_REQUEST_CODE {
                let pid = u32::from_be_bytes([rest[4], rest[5], rest[6], rest[7]]);
                let secret = u32::from_be_bytes([rest[8], rest[9], rest[10], rest[11]]);
                return Ok(Incoming::Cancel { pid, secret });
            }
            let mut payload = Vec::with_capacity(12);
            payload.extend_from_slice(&rest);
            return Ok(Incoming::Startup {
                transport,
                payload,
                used_tls,
            });
        }
        if len == 8 {
            let mut code_buf = [0u8; 4];
            transport.read_exact(&mut code_buf).await?;
            let code = u32::from_be_bytes(code_buf);
            if code == SSL_REQUEST_CODE {
                match tls {
                    Some(acceptor) => {
                        if used_tls {
                            return Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "duplicate SSLRequest on an encrypted connection",
                            ));
                        }
                        transport.write_all(b"S").await?;
                        transport.flush().await?;
                        let Transport::Plain(plain) = transport else {
                            unreachable!("used_tls is false, so transport is plaintext");
                        };
                        match acceptor.inner.accept(plain).await {
                            Ok(tls_stream) => {
                                plomid_core::observability::record_tls_handshake();
                                transport = Transport::Tls(Box::new(tls_stream));
                                used_tls = true;
                                continue;
                            }
                            Err(e) => {
                                plomid_core::observability::record_tls_handshake_failure();
                                return Err(e);
                            }
                        }
                    }
                    None => {
                        transport.write_all(b"N").await?;
                        transport.flush().await?;
                        continue;
                    }
                }
            }
            if code == GSSENC_REQUEST_CODE {
                // GSSAPI is unsupported: always decline, stay plaintext.
                transport.write_all(b"N").await?;
                transport.flush().await?;
                continue;
            }
            // A CancelRequest code in the 8-byte shape carries no pid/secret;
            // close quietly (historical behavior).
            if code == CANCEL_REQUEST_CODE {
                return Ok(Incoming::Close);
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "unknown SSL request code",
            ));
        }
        let payload_len = len - 4;
        let mut payload = vec![0u8; payload_len];
        transport.read_exact(&mut payload).await?;
        return Ok(Incoming::Startup {
            transport,
            payload,
            used_tls,
        });
    }
}
