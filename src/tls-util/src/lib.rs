// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A tiny utility library for making TLS connectors.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use mz_ore::secure::{Zeroize, Zeroizing};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::config::SslMode;
use tokio_postgres::tls::{ChannelBinding, TlsStream};
use tokio_rustls::TlsConnector;

macro_rules! bail_generic {
    ($err:expr $(,)?) => {
        return Err(TlsError::Generic(anyhow::anyhow!($err)))
    };
}

/// An error representing tls failures.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// Any other error we bail on.
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
    /// Error setting up TLS.
    #[error("TLS configuration error: {0}")]
    Rustls(#[from] rustls::Error),
}

/// Wrapper around `tokio_rustls::client::TlsStream` that implements
/// `tokio_postgres::tls::TlsStream`.
pub struct RustlsTlsStream<S>(tokio_rustls::client::TlsStream<S>);

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncRead for RustlsTlsStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_read(cx, buf)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> AsyncWrite for RustlsTlsStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().0).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().0).poll_shutdown(cx)
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> TlsStream for RustlsTlsStream<S> {
    fn channel_binding(&self) -> ChannelBinding {
        ChannelBinding::none()
    }
}

/// A `MakeTlsConnect` implementation backed by rustls.
#[derive(Clone)]
pub struct MakeRustlsConnect {
    config: Arc<rustls::ClientConfig>,
}

impl MakeRustlsConnect {
    pub fn new(config: rustls::ClientConfig) -> Self {
        Self {
            config: Arc::new(config),
        }
    }
}

impl<S> tokio_postgres::tls::MakeTlsConnect<S> for MakeRustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsTlsStream<S>;
    type TlsConnect = RustlsConnect;
    type Error = rustls_pki_types::InvalidDnsNameError;

    fn make_tls_connect(&mut self, domain: &str) -> Result<Self::TlsConnect, Self::Error> {
        let server_name = ServerName::try_from(domain.to_owned())?;
        Ok(RustlsConnect {
            connector: TlsConnector::from(self.config.clone()),
            server_name,
        })
    }
}

pub struct RustlsConnect {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl<S> tokio_postgres::tls::TlsConnect<S> for RustlsConnect
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Stream = RustlsTlsStream<S>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Stream, Self::Error>> + Send>>;

    fn connect(self, stream: S) -> Self::Future {
        Box::pin(async move {
            let tls_stream = self.connector.connect(self.server_name, stream).await?;
            Ok(RustlsTlsStream(tls_stream))
        })
    }
}

/// Creates a TLS connector for the given [`Config`](tokio_postgres::Config).
pub fn make_tls(config: &tokio_postgres::Config) -> Result<MakeRustlsConnect, TlsError> {
    let mut root_store = rustls::RootCertStore::empty();

    // The mode dictates whether we verify peer certs and hostnames. By default, Postgres is
    // pretty relaxed and recommends SslMode::VerifyCa or SslMode::VerifyFull for security.
    //
    // For more details, check out Table 33.1. SSL Mode Descriptions in
    // https://postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION.
    let verify_mode = match config.get_ssl_mode() {
        SslMode::Disable | SslMode::Prefer => false,
        SslMode::Require => config.get_ssl_root_cert().is_some(),
        SslMode::VerifyCa | SslMode::VerifyFull => true,
        _ => panic!("unexpected sslmode {:?}", config.get_ssl_mode()),
    };

    // Load root certificates if verification is needed.
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(ssl_root_cert)
            .collect::<Result<_, _>>()
            .map_err(|e| TlsError::Generic(anyhow::anyhow!("failed to parse root certs: {e}")))?;
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| TlsError::Generic(anyhow::anyhow!("invalid root cert: {e}")))?;
        }
    }

    let builder = if verify_mode {
        rustls::ClientConfig::builder().with_root_certificates(root_store)
    } else {
        // When not verifying, use a custom verifier that accepts all certs.
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
    };

    // Configure client certificate if provided.
    let tls_config = match (config.get_ssl_cert(), config.get_ssl_key()) {
        (Some(ssl_cert), Some(ssl_key)) => {
            let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(ssl_cert)
                .collect::<Result<_, _>>()
                .map_err(|e| {
                    TlsError::Generic(anyhow::anyhow!("failed to parse client cert: {e}"))
                })?;
            let key = PrivateKeyDer::from_pem_slice(ssl_key).map_err(|e| {
                TlsError::Generic(anyhow::anyhow!("failed to parse client key: {e}"))
            })?;
            builder.with_client_auth_cert(certs, key)?
        }
        (None, Some(_)) => {
            bail_generic!("must provide both sslcert and sslkey, but only provided sslkey")
        }
        (Some(_), None) => {
            bail_generic!("must provide both sslcert and sslkey, but only provided sslcert")
        }
        _ => builder.with_no_client_auth(),
    };

    Ok(MakeRustlsConnect::new(tls_config))
}

/// A certificate verifier that accepts all certificates.
/// Used when SslMode is Disable, Prefer, or Require without a root cert.
#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls_pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::aws_lc_rs::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub struct Pkcs12Archive {
    pub der: Vec<u8>,
    pub pass: String,
}

impl Zeroize for Pkcs12Archive {
    fn zeroize(&mut self) {
        self.der.zeroize();
        self.pass.zeroize();
    }
}

impl Drop for Pkcs12Archive {
    fn drop(&mut self) {
        self.zeroize();
    }
}

/// Constructs a PEM identity from a key and certificate.
///
/// Returns a `Pkcs12Archive` for backward compatibility with callers that
/// expect the PKCS#12 DER + password format. The DER field now contains the
/// concatenated PEM key+cert bytes, and the pass field is empty.
pub fn pkcs12der_from_pem(key: &[u8], cert: &[u8]) -> Result<Pkcs12Archive, anyhow::Error> {
    let mut buf = Zeroizing::new(Vec::new());
    buf.extend(key);
    buf.push(b'\n');
    buf.extend(cert);

    // Validate the key and cert can be parsed.
    let _key = PrivateKeyDer::from_pem_slice(&buf)
        .map_err(|e| anyhow::anyhow!("failed to parse private key PEM: {e}"))?;
    let certs: Vec<CertificateDer<'static>> = CertificateDer::pem_slice_iter(&buf)
        .collect::<Result<_, _>>()
        .map_err(|e| anyhow::anyhow!("failed to parse certificate PEM: {e}"))?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found in PEM");
    }

    Ok(Pkcs12Archive {
        der: buf.to_vec(),
        pass: String::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn pkcs12_archive_needs_drop() {
        assert!(std::mem::needs_drop::<Pkcs12Archive>());
    }

    #[mz_ore::test]
    fn pkcs12_archive_zeroize_clears_fields() {
        let mut archive = Pkcs12Archive {
            der: vec![0xDE, 0xAD, 0xBE, 0xEF],
            pass: String::from("hunter2"),
        };

        archive.zeroize();

        assert!(archive.der.is_empty(), "der was not zeroed");
        assert!(archive.pass.is_empty(), "pass was not zeroed");
    }

    #[mz_ore::test]
    fn pkcs12_archive_implements_zeroize() {
        fn assert_zeroize<T: mz_ore::secure::Zeroize>() {}
        assert_zeroize::<Pkcs12Archive>();
    }
}
