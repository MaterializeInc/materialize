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

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::verify_server_name;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::server::ParsedCertificate;
use rustls::{CertificateError, DigitallySignedStruct, SignatureScheme};
use serde::{Deserialize, Serialize};
use x509_cert::der::asn1::{Ia5StringRef, PrintableStringRef, Utf8StringRef};
use x509_cert::der::oid::db::rfc4519;
use x509_cert::der::{Decode, Tag, Tagged};
use x509_cert::ext::pkix::SubjectAltName;
use x509_cert::ext::pkix::name::GeneralName;
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
    #[error("invalid TLS configuration: {0}")]
    Config(rustls::Error),
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

        let (certs, key) = parse_identity_pem(&pem)?;
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

/// Splits an identity PEM buffer into its certificate chain and private key.
fn parse_identity_pem(
    pem: &[u8],
) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), TlsError> {
    // Mirror `reqwest::Identity::from_pem`, which uses the last private key in
    // the buffer.
    let mut keys = PrivateKeyDer::pem_slice_iter(pem).collect::<Result<Vec<_>, _>>()?;
    let key = keys.pop().ok_or(TlsError::NoPrivateKey)?;
    keys.iter_mut().for_each(Zeroize::zeroize);
    let certs = CertificateDer::pem_slice_iter(pem).collect::<Result<Vec<_>, _>>()?;
    if certs.is_empty() {
        return Err(TlsError::NoCertificate);
    }
    Ok((certs, key))
}

/// Builds the rustls configuration for a client that trusts `roots` in
/// addition to the platform's trust store, and that presents `identity`, if
/// any, for client authentication.
///
/// Server certificates are verified by `rustls-platform-verifier`, as reqwest
/// does by default, with the [`ExactRootMatch`] fallback.
pub(crate) fn rustls_config(
    roots: &[Certificate],
    identity: Option<&Identity>,
) -> Result<rustls::ClientConfig, TlsError> {
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let roots: Vec<_> = roots
        .iter()
        .map(|cert| CertificateDer::from(cert.der.clone()))
        .collect();
    let inner = rustls_platform_verifier::Verifier::new_with_extra_roots(
        roots.clone(),
        Arc::clone(&provider),
    )
    .map_err(TlsError::Config)?;
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(TlsError::Config)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(ExactRootMatch { inner, roots }));
    let mut config = match identity {
        Some(identity) => {
            let (certs, key) = parse_identity_pem(&identity.pem)?;
            builder
                .with_client_auth_cert(certs, key)
                .map_err(TlsError::Identity)?
        }
        None => builder.with_no_client_auth(),
    };
    // reqwest only sets ALPN on TLS configurations it builds itself. This
    // mirrors its choice while the workspace enables reqwest's `http2` feature.
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

/// A server certificate verifier that accepts a server certificate that is
/// byte-for-byte identical to one of `roots`, and otherwise defers to `inner`.
///
/// An exact match must still be within its validity period and valid for the
/// server name, but skips the chain, basic constraints and key usage checks.
/// This keeps a self-signed `CA:TRUE` certificate working when it is supplied
/// as its own certificate authority, which webpki rejects as
/// `CaUsedAsEndEntity`. The name check falls back to the subject common names
/// when an exact match has no DNS or IP subjectAltName, see
/// [`common_name_matches`]. Every other certificate is checked against
/// subjectAltNames only. An exact match is decided without consulting
/// `inner`. Handshake signatures are always verified by `inner`.
#[derive(Debug)]
struct ExactRootMatch {
    inner: rustls_platform_verifier::Verifier,
    roots: Vec<CertificateDer<'static>>,
}

/// Returns whether `server_name` is a DNS name equal, ignoring ASCII case, to
/// any subject common name of `cert`, and `cert` has no DNS or IP
/// subjectAltName.
///
/// This is stricter than OpenSSL's fallback, which also applies when only IP
/// subjectAltNames are present, matches wildcard common names, and decodes
/// string types other than UTF8String, PrintableString and IA5String.
fn common_name_matches(cert: &x509_cert::Certificate, server_name: &ServerName<'_>) -> bool {
    let ServerName::DnsName(name) = server_name else {
        return false;
    };
    let tbs = &cert.tbs_certificate;
    // An undecodable subjectAltName extension counts as present.
    let has_san = tbs.filter::<SubjectAltName>().any(|san| match san {
        Ok((_, SubjectAltName(names))) => names
            .iter()
            .any(|name| matches!(name, GeneralName::DnsName(_) | GeneralName::IpAddress(_))),
        Err(_) => true,
    });
    if has_san {
        return false;
    }
    tbs.subject
        .0
        .iter()
        .flat_map(|rdn| rdn.0.iter())
        .filter(|atv| atv.oid == rfc4519::CN)
        .any(|cn| {
            let cn = match cn.value.tag() {
                Tag::Utf8String => cn
                    .value
                    .decode_as::<Utf8StringRef<'_>>()
                    .map(|s| s.as_str().to_owned()),
                Tag::PrintableString => cn
                    .value
                    .decode_as::<PrintableStringRef<'_>>()
                    .map(|s| s.as_str().to_owned()),
                Tag::Ia5String => cn
                    .value
                    .decode_as::<Ia5StringRef<'_>>()
                    .map(|s| s.as_str().to_owned()),
                _ => return false,
            };
            cn.is_ok_and(|cn| cn.eq_ignore_ascii_case(name.as_ref()))
        })
}

impl ServerCertVerifier for ExactRootMatch {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        // Checked before `inner`, which would reject a `CA:TRUE` certificate and
        // logs every rejection at error level.
        if self
            .roots
            .iter()
            .any(|root| root.as_ref() == end_entity.as_ref())
        {
            let cert = x509_cert::Certificate::from_der(end_entity)
                .map_err(|_| rustls::Error::InvalidCertificate(CertificateError::BadEncoding))?;
            let validity = &cert.tbs_certificate.validity;
            let now = now.as_secs();
            if now < validity.not_before.to_unix_duration().as_secs() {
                return Err(rustls::Error::InvalidCertificate(
                    CertificateError::NotValidYet,
                ));
            }
            if now > validity.not_after.to_unix_duration().as_secs() {
                return Err(rustls::Error::InvalidCertificate(CertificateError::Expired));
            }
            // Checks subjectAltNames only, not basic constraints, so a
            // `CA:TRUE` certificate passes. Errors match `inner`'s on Linux.
            let parsed = ParsedCertificate::try_from(end_entity)?;
            match verify_server_name(&parsed, server_name) {
                Ok(()) => {}
                Err(_) if common_name_matches(&cert, server_name) => {}
                Err(e) => return Err(e),
            }
            return Ok(ServerCertVerified::assertion());
        }
        self.inner
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.inner.supported_verify_schemes()
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
