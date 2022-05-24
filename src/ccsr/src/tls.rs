// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TLS certificates and identities.

use openssl::pkcs12::Pkcs12;
use openssl::pkey::PKey;
use openssl::stack::Stack;
use openssl::x509::X509;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use mz_repr::proto::{RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_ccsr.tls.rs"));

/// A [Serde][serde]-enabled wrapper around [`reqwest::Identity`].
///
/// [Serde]: serde
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    der: Vec<u8>,
    pass: String,
}

impl Identity {
    /// Reimplements [`reqwest::Certificate::from_pem`] in terms of OpenSSL.
    ///
    /// The implementation in reqwest requires rustls.
    pub fn from_pem(pem: &[u8]) -> Result<Self, openssl::error::ErrorStack> {
        let pkey = PKey::private_key_from_pem(pem)?;
        let mut certs = Stack::new()?;
        let mut cert_iter = X509::stack_from_pem(pem)?.into_iter();
        let cert = cert_iter
            .next()
            .expect("X509::stack_from_pem always returns at least one certificate");
        for cert in cert_iter {
            certs.push(cert)?;
        }
        let mut pkcs_builder = Pkcs12::builder();
        pkcs_builder.ca(certs);
        // We build a PKCS #12 archive solely to have something to pass to
        // `reqwest::Identity::from_pkcs12_der`, so the password and friendly
        // name don't matter.
        let pass = String::new();
        let friendly_name = "";
        let der = pkcs_builder
            .build(&pass, friendly_name, &pkey, &cert)?
            .to_der()?;
        Ok(Identity { der, pass })
    }

    /// Wraps [`reqwest::Identity::from_pkcs12_der`].
    pub fn from_pkcs12_der(der: Vec<u8>, pass: String) -> Result<Self, reqwest::Error> {
        let _ = reqwest::Identity::from_pkcs12_der(&der, &pass)?;
        Ok(Identity { der, pass })
    }
}

impl Into<reqwest::Identity> for Identity {
    fn into(self) -> reqwest::Identity {
        reqwest::Identity::from_pkcs12_der(&self.der, &self.pass)
            .expect("known to be a valid identity")
    }
}

impl RustType<ProtoIdentity> for Identity {
    fn into_proto(self: &Self) -> ProtoIdentity {
        ProtoIdentity {
            der: self.der.clone(),
            pass: self.pass.clone(),
        }
    }

    fn from_proto(proto: ProtoIdentity) -> Result<Self, TryFromProtoError> {
        Ok(Identity {
            der: proto.der,
            pass: proto.pass,
        })
    }
}

/// A [Serde][serde]-enabled wrapper around [`reqwest::Certificate`].
///
/// [Serde]: serde
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
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

impl RustType<ProtoCertificate> for Certificate {
    fn into_proto(self: &Self) -> ProtoCertificate {
        ProtoCertificate {
            der: self.der.clone(),
        }
    }

    fn from_proto(proto: ProtoCertificate) -> Result<Self, TryFromProtoError> {
        Ok(Certificate { der: proto.der })
    }
}
