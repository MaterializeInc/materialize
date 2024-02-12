// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TLS certificates and identities.

use serde::{Deserialize, Serialize};

use mz_tls_util::pkcs12der_from_pem;

/// A [Serde][serde]-enabled wrapper around [`reqwest::Identity`].
///
/// [Serde]: serde
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Identity {
    der: Vec<u8>,
    pass: String,
}

impl Identity {
    /// Constructs an identity from a PEM-formatted key and certificate using OpenSSL.
    pub fn from_pem(key: &[u8], cert: &[u8]) -> Result<Self, openssl::error::ErrorStack> {
        let archive = pkcs12der_from_pem(key, cert)?;
        Ok(Identity {
            der: archive.der,
            pass: archive.pass,
        })
    }

    /// Wraps [`reqwest::Identity::from_pkcs12_der`].
    pub fn from_pkcs12_der(der: Vec<u8>, pass: String) -> Result<Self, reqwest::Error> {
        let _ = reqwest::Identity::from_pkcs12_der(&der, &pass)?;
        Ok(Identity { der, pass })
    }
}

impl From<Identity> for reqwest::Identity {
    fn from(id: Identity) -> Self {
        reqwest::Identity::from_pkcs12_der(&id.der, &id.pass).expect("known to be a valid identity")
    }
}

/// A [Serde][serde]-enabled wrapper around [`reqwest::Certificate`].
///
/// [Serde]: serde
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Certificate {
    der: Vec<u8>,
}

impl Certificate {
    /// Wraps [`reqwest::Certificate::from_pem`].
    pub fn from_pem(pem: &[u8]) -> native_tls::Result<Certificate> {
        Ok(Certificate {
            der: native_tls::Certificate::from_pem(pem)?.to_der()?,
        })
    }

    /// Wraps [`reqwest::Certificate::from_der`].
    pub fn from_der(der: &[u8]) -> native_tls::Result<Certificate> {
        let _ = native_tls::Certificate::from_der(der)?;
        Ok(Certificate { der: der.into() })
    }
}

impl From<Certificate> for reqwest::Certificate {
    fn from(cert: Certificate) -> Self {
        reqwest::Certificate::from_der(&cert.der).expect("known to be a valid cert")
    }
}
