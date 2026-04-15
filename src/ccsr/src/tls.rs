// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TLS certificates and identities.

use base64::Engine;
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
    /// Constructs an identity from a PEM-formatted key and certificate.
    pub fn from_pem(key: &[u8], cert: &[u8]) -> Result<Self, anyhow::Error> {
        let mut archive = pkcs12der_from_pem(key, cert)
            .map_err(|e| anyhow::anyhow!("failed to build PKCS#12 identity: {e}"))?;
        // Also validate that reqwest can parse the PEM identity, since the
        // From<Identity> conversion uses expect() and must not panic.
        reqwest::Identity::from_pem(&archive.der)
            .map_err(|e| anyhow::anyhow!("failed to build reqwest identity: {e}"))?;
        Ok(Identity {
            der: std::mem::take(&mut archive.der),
            pass: std::mem::take(&mut archive.pass),
        })
    }

    /// Constructs an identity from PEM-encoded key+cert data.
    ///
    /// The `der` field stores the raw PEM bytes, `pass` is unused (kept for
    /// backward compatibility with serialized data).
    pub fn from_pkcs12_der(der: Vec<u8>, pass: String) -> Result<Self, reqwest::Error> {
        // Validate by trying to construct a reqwest Identity.
        let _ = reqwest::Identity::from_pem(&der)?;
        Ok(Identity { der, pass })
    }
}

impl From<Identity> for reqwest::Identity {
    fn from(id: Identity) -> Self {
        reqwest::Identity::from_pem(&id.der).expect("known to be a valid identity")
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
    /// Constructs a certificate from PEM-encoded data.
    pub fn from_pem(pem: &[u8]) -> Result<Certificate, anyhow::Error> {
        // Parse PEM to DER by stripping headers and base64-decoding.
        let pem_str = std::str::from_utf8(pem)
            .map_err(|e| anyhow::anyhow!("invalid CERTIFICATE PEM: not valid UTF-8: {e}"))?;
        let b64: String = pem_str
            .lines()
            .filter(|l| !l.starts_with("-----"))
            .collect();
        let der = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| anyhow::anyhow!("invalid CERTIFICATE PEM: bad base64: {e}"))?;
        // Explicitly parse the DER to validate the certificate structure.
        let cert_der = rustls_pki_types::CertificateDer::from(der.clone());
        rustls::server::ParsedCertificate::try_from(&cert_der)
            .map_err(|e| anyhow::anyhow!("invalid CERTIFICATE: {e}"))?;
        Ok(Certificate { der })
    }

    /// Constructs a certificate from DER-encoded data.
    pub fn from_der(der: &[u8]) -> Result<Certificate, anyhow::Error> {
        // Explicitly parse the DER to validate it. reqwest with the rustls
        // backend defers DER validation to TLS handshake time, but we want
        // to catch invalid certificates early at construction time.
        let cert_der = rustls_pki_types::CertificateDer::from(der.to_vec());
        rustls::server::ParsedCertificate::try_from(&cert_der)
            .map_err(|e| anyhow::anyhow!("invalid DER certificate: {e}"))?;
        Ok(Certificate { der: der.into() })
    }
}

impl From<Certificate> for reqwest::Certificate {
    fn from(cert: Certificate) -> Self {
        reqwest::Certificate::from_der(&cert.der).expect("known to be a valid cert")
    }
}
