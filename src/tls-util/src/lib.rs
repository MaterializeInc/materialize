// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A tiny utility library for making TLS connectors.

use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
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
