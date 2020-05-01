// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TLS certificates and identities.

use serde::{Deserialize, Serialize};

// Encodes the type of certificate file, as well as the certificate's bytes. In
// the case of der certificates, it also stores the password.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) enum CertDetails {
    Pem(Vec<u8>),
    Der(Vec<u8>, String),
}

/// A [Serde][serde]-enabled wrapper around [`reqwest::Identity`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    pub(crate) cert: CertDetails,
}

impl Identity {
    /// Wraps [`reqwest::Identity::from_pem`].
    pub fn from_pem(pem: &[u8]) -> Result<Self, reqwest::Error> {
        let _ = reqwest::Identity::from_pem(&pem)?;
        Ok(Identity {
            cert: CertDetails::Pem(pem.into()),
        })
    }

    /// Wraps [`reqwest::Identity::from_pem`].
    pub fn from_pkcs12_der(der: &[u8], password: &str) -> Result<Self, reqwest::Error> {
        let _ = reqwest::Identity::from_pkcs12_der(&der, password)?;
        Ok(Identity {
            cert: CertDetails::Der(der.into(), password.to_string()),
        })
    }
}

impl Into<reqwest::Identity> for Identity {
    fn into(self) -> reqwest::Identity {
        match self.cert {
            CertDetails::Pem(pem) => {
                reqwest::Identity::from_pem(&pem).expect("known to be a valid identity")
            }
            CertDetails::Der(der, pass) => reqwest::Identity::from_pkcs12_der(&der, &pass)
                .expect("known to be a valid identity"),
        }
    }
}

/// A [Serde][serde]-enabled wrapper around [`reqwest::Certificate`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

impl Into<reqwest::Certificate> for Certificate {
    fn into(self) -> reqwest::Certificate {
        reqwest::Certificate::from_der(&self.der).expect("known to be a valid cert")
    }
}
