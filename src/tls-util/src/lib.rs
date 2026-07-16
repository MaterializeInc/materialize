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
use openssl::pkcs12::Pkcs12;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::stack::Stack;
use openssl::x509::X509;
use postgres_openssl::MakeTlsConnector;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_postgres::config::SslMode;
use tokio_postgres::tls::ChannelBinding;
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
    /// Error setting up postgres ssl.
    #[error(transparent)]
    OpenSsl(#[from] openssl::error::ErrorStack),
}

/// Creates a TLS connector for the given [`Config`](tokio_postgres::Config).
pub fn make_tls(config: &tokio_postgres::Config) -> Result<MakeTlsConnector, TlsError> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    // The mode dictates whether we verify peer certs and hostnames. By default, Postgres is
    // pretty relaxed and recommends SslMode::VerifyCa or SslMode::VerifyFull for security.
    //
    // For more details, check out Table 33.1. SSL Mode Descriptions in
    // https://postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION.
    let (verify_mode, verify_hostname) = match config.get_ssl_mode() {
        SslMode::Disable | SslMode::Prefer => (SslVerifyMode::NONE, false),
        SslMode::Require => match config.get_ssl_root_cert() {
            // If a root CA file exists, the behavior of sslmode=require will be the same as
            // that of verify-ca, meaning the server certificate is validated against the CA.
            //
            // For more details, check out the note about backwards compatibility in
            // https://postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES.
            Some(_) => (SslVerifyMode::PEER, false),
            None => (SslVerifyMode::NONE, false),
        },
        SslMode::VerifyCa => (SslVerifyMode::PEER, false),
        SslMode::VerifyFull => (SslVerifyMode::PEER, true),
        _ => panic!("unexpected sslmode {:?}", config.get_ssl_mode()),
    };

    // Configure peer verification
    builder.set_verify(verify_mode);

    // Configure certificates
    match (config.get_ssl_cert(), config.get_ssl_key()) {
        (Some(ssl_cert), Some(ssl_key)) => {
            builder.set_certificate(&*X509::from_pem(ssl_cert)?)?;
            builder.set_private_key(&*PKey::private_key_from_pem(ssl_key)?)?;
        }
        (None, Some(_)) => {
            bail_generic!("must provide both sslcert and sslkey, but only provided sslkey")
        }
        (Some(_), None) => {
            bail_generic!("must provide both sslcert and sslkey, but only provided sslcert")
        }
        _ => {}
    }
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        for cert in X509::stack_from_pem(ssl_root_cert)? {
            builder.cert_store_mut().add_cert(cert)?;
        }
    }

    let mut tls_connector = MakeTlsConnector::new(builder.build());

    // Configure hostname verification
    match (verify_mode, verify_hostname) {
        (SslVerifyMode::PEER, false) => tls_connector.set_callback(|connect, _| {
            connect.set_verify_hostname(false);
            Ok(())
        }),
        _ => {}
    }

    Ok(tls_connector)
}

/// Creates a rustls-based TLS connector for the given
/// [`Config`](tokio_postgres::Config).
///
/// The rustls equivalent of [`make_tls`]. The two coexist while consumers
/// migrate from openssl to rustls one at a time.
pub fn make_tls_rustls(config: &tokio_postgres::Config) -> Result<MakeRustlsConnect, TlsError> {
    let mut root_store = rustls::RootCertStore::empty();

    // The mode dictates whether we verify peer certs and hostnames. By default, Postgres is
    // pretty relaxed and recommends SslMode::VerifyCa or SslMode::VerifyFull for security.
    //
    // For more details, check out Table 33.1. SSL Mode Descriptions in
    // https://postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION.
    let verify_mode = match config.get_ssl_mode() {
        SslMode::Disable | SslMode::Prefer => false,
        // If a root CA file exists, the behavior of sslmode=require will be the same as
        // that of verify-ca, meaning the server certificate is validated against the CA.
        //
        // For more details, check out the note about backwards compatibility in
        // https://postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES.
        SslMode::Require => config.get_ssl_root_cert().is_some(),
        SslMode::VerifyCa | SslMode::VerifyFull => true,
        _ => panic!("unexpected sslmode {:?}", config.get_ssl_mode()),
    };

    // When verifying, seed the trust store with the platform's native roots.
    // This matches the openssl connector, whose builder trusts the system
    // default CA paths. Without it, verify modes with no explicit sslrootcert
    // would reject every server certificate.
    if verify_mode {
        let native = rustls_native_certs::load_native_certs();
        for error in &native.errors {
            tracing::warn!("failed to load native root certs: {error}");
        }
        let (added, ignored) = root_store.add_parsable_certificates(native.certs);
        tracing::debug!("loaded {added} native root certs, ignored {ignored}");
    }

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
            builder.with_client_auth_cert(certs, key).map_err(|e| {
                TlsError::Generic(anyhow::anyhow!("failed to configure client auth: {e}"))
            })?
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

impl<S: AsyncRead + AsyncWrite + Unpin> tokio_postgres::tls::TlsStream for RustlsTlsStream<S> {
    fn channel_binding(&self) -> ChannelBinding {
        let (_, session) = self.0.get_ref();
        match session
            .peer_certificates()
            .and_then(|certs| certs.first())
            .and_then(tls_server_end_point)
        {
            Some(hash) => ChannelBinding::tls_server_end_point(hash),
            None => ChannelBinding::none(),
        }
    }
}

/// Computes the RFC 5929 `tls-server-end-point` channel binding data for a
/// server certificate: a hash of the DER certificate using the hash function
/// of its signature algorithm, with MD5 and SHA-1 upgraded to SHA-256.
///
/// Returns `None` when the signature algorithm has no well-defined hash
/// function (e.g. Ed25519, RSASSA-PSS). The openssl connector also reports no
/// channel binding for those, so this preserves parity.
fn tls_server_end_point(cert: &CertificateDer<'_>) -> Option<Vec<u8>> {
    use sha2::{Digest, Sha256, Sha384, Sha512};
    use x509_cert::der::Decode;
    use x509_cert::der::oid::ObjectIdentifier;

    const MD5_RSA: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.113549.1.1.4");
    const SHA1_RSA: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.113549.1.1.5");
    const SHA256_RSA: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.113549.1.1.11");
    const SHA384_RSA: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.113549.1.1.12");
    const SHA512_RSA: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.113549.1.1.13");
    const SHA1_ECDSA: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.4.1");
    const SHA256_ECDSA: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.4.3.2");
    const SHA384_ECDSA: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.4.3.3");
    const SHA512_ECDSA: ObjectIdentifier = ObjectIdentifier::new_unwrap("1.2.840.10045.4.3.4");

    let parsed = x509_cert::Certificate::from_der(cert.as_ref()).ok()?;
    let oid = parsed.signature_algorithm.oid;
    if oid == MD5_RSA
        || oid == SHA1_RSA
        || oid == SHA256_RSA
        || oid == SHA1_ECDSA
        || oid == SHA256_ECDSA
    {
        Some(Sha256::digest(cert.as_ref()).to_vec())
    } else if oid == SHA384_RSA || oid == SHA384_ECDSA {
        Some(Sha384::digest(cert.as_ref()).to_vec())
    } else if oid == SHA512_RSA || oid == SHA512_ECDSA {
        Some(Sha512::digest(cert.as_ref()).to_vec())
    } else {
        None
    }
}

/// A `MakeTlsConnect` implementation backed by rustls.
#[derive(Clone)]
pub struct MakeRustlsConnect {
    config: Arc<rustls::ClientConfig>,
}

impl MakeRustlsConnect {
    /// Creates a connector factory from the given client config.
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
        // For Unix socket connections, tokio-postgres passes an empty string as
        // the domain. Socket paths starting with "/" are also not valid DNS names.
        // TLS is never negotiated over Unix sockets, so use a placeholder.
        let server_name = if domain.is_empty() || domain.starts_with('/') {
            ServerName::try_from("localhost").unwrap()
        } else {
            ServerName::try_from(domain.to_owned())?
        };
        Ok(RustlsConnect {
            connector: TlsConnector::from(Arc::clone(&self.config)),
            server_name,
        })
    }
}

/// A pending rustls TLS connection to a specific server.
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

impl Pkcs12Archive {
    pub fn into_parts(self) -> (Vec<u8>, String) {
        let mut md = std::mem::ManuallyDrop::new(self);
        let der = std::mem::take(&mut md.der);
        let pass = std::mem::take(&mut md.pass);
        (der, pass)
    }
}

/// Constructs an identity from a PEM-formatted key and certificate using OpenSSL.
pub fn pkcs12der_from_pem(
    key: &[u8],
    cert: &[u8],
) -> Result<Pkcs12Archive, openssl::error::ErrorStack> {
    let mut buf = Zeroizing::new(Vec::new());
    buf.extend(key);
    buf.push(b'\n');
    buf.extend(cert);
    let pem = buf.as_slice();
    let pkey = PKey::private_key_from_pem(pem)?;
    let mut certs = Stack::new()?;

    // `X509::stack_from_pem` in openssl as of at least versions <= 0.10.48
    // does not guarantee that it will either error or return at least 1
    // element; in fact, it doesn't if the `pem` is not a well-formed
    // representation of a PEM file. For example, if the represented file
    // contains a well-formed key but a malformed certificate.
    //
    // To circumvent this issue, if `X509::stack_from_pem` returns no
    // certificates, rely on getting the error message from
    // `X509::from_pem`.
    let mut cert_iter = X509::stack_from_pem(pem)?.into_iter();
    let cert = match cert_iter.next() {
        Some(cert) => cert,
        None => X509::from_pem(pem)?,
    };
    for cert in cert_iter {
        certs.push(cert)?;
    }
    // We build a PKCS #12 archive solely to have something to pass to
    // `reqwest::Identity::from_pkcs12_der`, so the password and friendly
    // name don't matter.
    let pass = String::new();
    let friendly_name = "";
    let der = Pkcs12::builder()
        .name(friendly_name)
        .pkey(&pkey)
        .cert(&cert)
        .ca(certs)
        .build2(&pass)?
        .to_der()?;
    Ok(Pkcs12Archive { der, pass })
}

#[cfg(test)]
mod tests {
    use openssl::asn1::Asn1Time;
    use openssl::ec::{EcGroup, EcKey};
    use openssl::hash::MessageDigest;
    use openssl::nid::Nid;
    use openssl::pkey::Private;
    use openssl::rsa::Rsa;
    use openssl::x509::X509NameBuilder;

    use super::*;

    fn self_signed_der(key: &PKey<Private>, digest: MessageDigest) -> Vec<u8> {
        let mut name = X509NameBuilder::new().unwrap();
        name.append_entry_by_text("CN", "localhost").unwrap();
        let name = name.build();
        let mut builder = X509::builder().unwrap();
        builder.set_version(2).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder.set_pubkey(key).unwrap();
        builder
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        builder
            .set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        builder.sign(key, digest).unwrap();
        builder.build().to_der().unwrap()
    }

    /// Asserts that our `tls-server-end-point` hash of a cert signed with
    /// `sign_digest` matches openssl's `X509_digest` with `expected`, which is
    /// what postgres-openssl uses. `None` expects no channel binding.
    fn check_end_point_parity(
        key: &PKey<Private>,
        sign_digest: MessageDigest,
        expected: Option<MessageDigest>,
    ) {
        let der = self_signed_der(key, sign_digest);
        let computed = tls_server_end_point(&CertificateDer::from(der.clone()));
        let expected =
            expected.map(|md| X509::from_der(&der).unwrap().digest(md).unwrap().to_vec());
        assert_eq!(computed, expected);
    }

    #[mz_ore::test]
    fn tls_server_end_point_digests() {
        let rsa = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        check_end_point_parity(&rsa, MessageDigest::sha256(), Some(MessageDigest::sha256()));
        check_end_point_parity(&rsa, MessageDigest::sha384(), Some(MessageDigest::sha384()));
        check_end_point_parity(&rsa, MessageDigest::sha512(), Some(MessageDigest::sha512()));
        // RFC 5929 upgrades SHA-1 to SHA-256.
        check_end_point_parity(&rsa, MessageDigest::sha1(), Some(MessageDigest::sha256()));

        let group = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        let ec = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        check_end_point_parity(&ec, MessageDigest::sha256(), Some(MessageDigest::sha256()));

        // Ed25519 has no hash in its signature algorithm, so no channel
        // binding, matching openssl.
        let ed = PKey::generate_ed25519().unwrap();
        check_end_point_parity(&ed, MessageDigest::null(), None);
    }

    #[mz_ore::test]
    fn tls_server_end_point_rejects_garbage() {
        let garbage = CertificateDer::from(vec![0x30, 0x00]);
        assert_eq!(tls_server_end_point(&garbage), None);
    }

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
