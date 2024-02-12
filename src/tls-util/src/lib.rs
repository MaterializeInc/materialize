// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A tiny utility library for making TLS connectors.

use openssl::pkcs12::Pkcs12;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::stack::Stack;
use openssl::x509::X509;
use postgres_openssl::MakeTlsConnector;
use tokio_postgres::config::SslMode;

macro_rules! bail_generic {
    ($fmt:expr, $($arg:tt)*) => {
        return Err(TlsError::Generic(anyhow::anyhow!($fmt, $($arg)*)))
    };
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
        builder
            .cert_store_mut()
            .add_cert(X509::from_pem(ssl_root_cert)?)?;
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

/// Constructs an identity from a PEM-formatted key and certificate using OpenSSL.
pub fn pkcs12der_from_pem(
    key: &[u8],
    cert: &[u8],
) -> Result<Pkcs12Archive, openssl::error::ErrorStack> {
    let mut buf = Vec::new();
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
