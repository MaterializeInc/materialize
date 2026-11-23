// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A tiny utility library for making TLS connectors.

use mz_ore::secure::{Zeroize, Zeroizing};
use openssl::pkcs12::Pkcs12;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::stack::Stack;
use openssl::x509::X509;
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::config::SslMode;

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
    use std::io::Write;
    use std::net::{SocketAddr, TcpListener};

    use openssl::asn1::{Asn1Integer, Asn1Time};
    use openssl::bn::{BigNum, MsbOption};
    use openssl::hash::MessageDigest;
    use openssl::pkey::Private;
    use openssl::rsa::Rsa;
    use openssl::ssl::SslAcceptor;
    use openssl::x509::X509NameBuilder;
    use openssl::x509::extension::{BasicConstraints, SubjectAlternativeName};
    use tokio::io::{AsyncRead, AsyncReadExt};
    use tokio::net::TcpStream;
    use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};

    use super::*;

    /// A throwaway CA that can issue leaf certificates.
    struct TestCa {
        cert: X509,
        key: PKey<Private>,
    }

    fn random_serial() -> Asn1Integer {
        let mut bn = BigNum::new().unwrap();
        bn.rand(64, MsbOption::MAYBE_ZERO, false).unwrap();
        bn.to_asn1_integer().unwrap()
    }

    impl TestCa {
        fn new() -> TestCa {
            let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
            let mut name = X509NameBuilder::new().unwrap();
            name.append_entry_by_text("CN", "mz-tls-util test CA")
                .unwrap();
            let name = name.build();
            let mut builder = X509::builder().unwrap();
            builder.set_version(2).unwrap();
            builder.set_serial_number(&random_serial()).unwrap();
            builder.set_subject_name(&name).unwrap();
            builder.set_issuer_name(&name).unwrap();
            builder.set_pubkey(&key).unwrap();
            builder
                .set_not_before(&Asn1Time::days_from_now(0).unwrap())
                .unwrap();
            builder
                .set_not_after(&Asn1Time::days_from_now(1).unwrap())
                .unwrap();
            builder
                .append_extension(BasicConstraints::new().critical().ca().build().unwrap())
                .unwrap();
            builder.sign(&key, MessageDigest::sha256()).unwrap();
            TestCa {
                cert: builder.build(),
                key,
            }
        }

        /// Issues a leaf certificate, returning its PEM cert and PKCS#8 key.
        fn issue(&self, cn: &str, dns_san: Option<&str>) -> (Vec<u8>, Vec<u8>) {
            let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
            let mut name = X509NameBuilder::new().unwrap();
            name.append_entry_by_text("CN", cn).unwrap();
            let name = name.build();
            let mut builder = X509::builder().unwrap();
            builder.set_version(2).unwrap();
            builder.set_serial_number(&random_serial()).unwrap();
            builder.set_subject_name(&name).unwrap();
            builder.set_issuer_name(self.cert.subject_name()).unwrap();
            builder.set_pubkey(&key).unwrap();
            builder
                .set_not_before(&Asn1Time::days_from_now(0).unwrap())
                .unwrap();
            builder
                .set_not_after(&Asn1Time::days_from_now(1).unwrap())
                .unwrap();
            if let Some(dns) = dns_san {
                let san = SubjectAlternativeName::new()
                    .dns(dns)
                    .build(&builder.x509v3_context(Some(&self.cert), None))
                    .unwrap();
                builder.append_extension(san).unwrap();
            }
            builder.sign(&self.key, MessageDigest::sha256()).unwrap();
            (
                builder.build().to_pem().unwrap(),
                key.private_key_to_pem_pkcs8().unwrap(),
            )
        }
    }

    /// Starts a blocking OpenSSL TLS server on an ephemeral port, serving
    /// connections one at a time on a background thread. Each connection
    /// receives "ok" once the server side of the handshake has completed. When
    /// `client_ca_pem` is given the server requires a client certificate
    /// signed by that CA, so the "ok" also confirms the client presented one.
    fn run_tls_server(cert_pem: &[u8], key_pem: &[u8], client_ca_pem: Option<&[u8]>) -> SocketAddr {
        let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server()).unwrap();
        acceptor
            .set_certificate(&X509::from_pem(cert_pem).unwrap())
            .unwrap();
        acceptor
            .set_private_key(&PKey::private_key_from_pem(key_pem).unwrap())
            .unwrap();
        if let Some(ca_pem) = client_ca_pem {
            for cert in X509::stack_from_pem(ca_pem).unwrap() {
                acceptor.cert_store_mut().add_cert(cert).unwrap();
            }
            acceptor.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
        }
        let acceptor = acceptor.build();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                // A failed handshake drops the connection, which the client
                // observes as a read error in `assert_server_ok`.
                if let Ok(mut tls) = acceptor.accept(stream.unwrap()) {
                    tls.write_all(b"ok").unwrap();
                    let _ = tls.shutdown();
                }
            }
        });
        addr
    }

    /// Completes a TLS handshake for `host` via `make_tls`, returning the
    /// established stream. Panics if certificate validation fails.
    async fn openssl_connect(
        config: &tokio_postgres::Config,
        host: &str,
        addr: SocketAddr,
    ) -> postgres_openssl::TlsStream<TcpStream> {
        let mut make = make_tls(config).unwrap();
        let connect = MakeTlsConnect::<TcpStream>::make_tls_connect(&mut make, host).unwrap();
        let tcp = TcpStream::connect(addr).await.unwrap();
        TlsConnect::<TcpStream>::connect(connect, tcp)
            .await
            .unwrap()
    }

    async fn assert_server_ok<S: AsyncRead + Unpin>(mut tls: S) {
        let mut buf = [0u8; 2];
        tls.read_exact(&mut buf)
            .await
            .expect("server did not complete the handshake");
        assert_eq!(&buf, b"ok");
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // uses the network and openssl FFI
    async fn verify_full_trusts_default_store() {
        let ca = TestCa::new();
        let (server_cert, server_key) = ca.issue("server", Some("localhost"));

        let mut ca_file = tempfile::NamedTempFile::new().unwrap();
        ca_file.write_all(&ca.cert.to_pem().unwrap()).unwrap();
        // SAFETY: the environment is process global. nextest, which CI uses,
        // runs each test in its own process. Under plain `cargo test` this can
        // race with other tests reading the environment.
        unsafe { std::env::set_var("SSL_CERT_FILE", ca_file.path()) };

        let addr = run_tls_server(&server_cert, &server_key, None);
        let mut config = tokio_postgres::Config::new();
        config.ssl_mode(SslMode::VerifyFull);

        assert_server_ok(openssl_connect(&config, "localhost", addr).await).await;
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // uses the network and openssl FFI
    async fn client_certs_accepted() {
        let ca = TestCa::new();
        let (server_cert, server_key) = ca.issue("server", Some("localhost"));
        let (client_cert, client_key) = ca.issue("client", None);
        let ca_pem = ca.cert.to_pem().unwrap();

        let addr = run_tls_server(&server_cert, &server_key, Some(&ca_pem));
        let mut config = tokio_postgres::Config::new();
        config.ssl_mode(SslMode::VerifyFull);
        config.ssl_root_cert(&ca_pem);
        config.ssl_cert(&client_cert);
        config.ssl_key(&client_key);

        assert_server_ok(openssl_connect(&config, "localhost", addr).await).await;
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
