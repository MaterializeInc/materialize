// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TLS certificates and identities.

use std::fmt;
use std::sync::Arc;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use zeroize::{Zeroize, Zeroizing};

/// An error constructing a [`Certificate`] or [`Identity`].
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("invalid PEM: {0}")]
    Pem(#[from] rustls::pki_types::pem::Error),
    #[error("no certificate found in PEM input")]
    NoCertificate,
    #[error("no private key found in PEM input")]
    NoPrivateKey,
    #[error("invalid certificate: {0}")]
    Certificate(rustls::CertificateError),
    #[error("invalid TLS identity: {0}")]
    Identity(rustls::Error),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
}

/// A [Serde][serde]-enabled wrapper around [`reqwest::Identity`].
///
/// Holds the PEM-encoded private key and certificate chain. The buffer is
/// zeroized on drop.
///
/// [Serde]: serde
#[derive(Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct Identity {
    pem: Vec<u8>,
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity").finish_non_exhaustive()
    }
}

impl Zeroize for Identity {
    fn zeroize(&mut self) {
        self.pem.zeroize();
    }
}

impl Drop for Identity {
    fn drop(&mut self) {
        self.zeroize();
    }
}

impl Identity {
    /// Constructs an identity from a PEM-formatted private key and certificate
    /// chain, leaf certificate first.
    ///
    /// The key may be PKCS #8, PKCS #1 (RSA) or SEC1 (EC). Returns an error if
    /// the key does not match the leaf certificate.
    pub fn from_pem(key: &[u8], cert: &[u8]) -> Result<Self, TlsError> {
        let mut pem = Zeroizing::new(Vec::with_capacity(key.len() + cert.len() + 1));
        pem.extend_from_slice(key);
        pem.push(b'\n');
        pem.extend_from_slice(cert);

        // Mirror `reqwest::Identity::from_pem`, which uses the last private
        // key in the buffer.
        let mut keys = PrivateKeyDer::pem_slice_iter(&pem).collect::<Result<Vec<_>, _>>()?;
        let key = keys.pop().ok_or(TlsError::NoPrivateKey)?;
        keys.iter_mut().for_each(Zeroize::zeroize);
        let certs = CertificateDer::pem_slice_iter(&pem).collect::<Result<Vec<_>, _>>()?;
        if certs.is_empty() {
            return Err(TlsError::NoCertificate);
        }

        // reqwest only checks that the key matches the certificate when the
        // client is built, so check here to report the error up front.
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        rustls::sign::CertifiedKey::from_der(certs, key, &provider).map_err(TlsError::Identity)?;
        let _ = reqwest::Identity::from_pem(&pem)?;

        Ok(Identity {
            pem: std::mem::take(&mut *pem),
        })
    }
}

impl From<Identity> for reqwest::Identity {
    fn from(id: Identity) -> Self {
        reqwest::Identity::from_pem(&id.pem).expect("known to be a valid identity")
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
    /// Constructs a certificate from the first certificate in a PEM-formatted
    /// buffer.
    pub fn from_pem(pem: &[u8]) -> Result<Certificate, TlsError> {
        let der = CertificateDer::pem_slice_iter(pem)
            .next()
            .ok_or(TlsError::NoCertificate)??;
        Self::from_der(&der)
    }

    /// Constructs a certificate from a DER-formatted buffer.
    pub fn from_der(der: &[u8]) -> Result<Certificate, TlsError> {
        // Parse the certificate as a trust anchor, as the verifier does when
        // the client is built.
        rustls::RootCertStore::empty()
            .add(CertificateDer::from_slice(der).into_owned())
            .map_err(|e| match e {
                rustls::Error::InvalidCertificate(e) => TlsError::Certificate(e),
                e => TlsError::Certificate(rustls::CertificateError::Other(rustls::OtherError(
                    Arc::new(e),
                ))),
            })?;
        Ok(Certificate { der: der.into() })
    }
}

impl From<Certificate> for reqwest::Certificate {
    fn from(cert: Certificate) -> Self {
        reqwest::Certificate::from_der(&cert.der).expect("known to be a valid cert")
    }
}
