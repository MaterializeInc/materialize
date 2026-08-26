// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wire format for forwarding a client's X.509 chain across a proxy hop.
//!
//! A proxy that terminates TLS is the only party that can obtain proof the
//! client holds its certificate's private key, but it is not necessarily the
//! party that knows which issuers are acceptable. This module carries the chain
//! from the terminating proxy to whoever evaluates it. The trust decision lives
//! in `mz_authenticator::client_cert`; nothing here judges a certificate.

use base64::prelude::*;
use openssl::error::ErrorStack;
use openssl::x509::X509;

/// Startup parameter carrying the client's chain, leaf first, as base64-encoded
/// concatenated PEM.
///
/// Only meaningful when asserted by a peer the receiver has authenticated as a
/// trusted proxy. A client that supplies it directly must be rejected, the same
/// way [`crate::MZ_FORWARDED_FOR_KEY`] is.
pub const MZ_CLIENT_CERT_KEY: &str = "mz_client_cert";

/// Upper bound on the encoded chain, so a hostile client cannot make a proxy
/// buffer and forward an unbounded certificate chain.
pub const MAX_ENCODED_CHAIN_LEN: usize = 64 * 1024;

/// A client's certificate chain, leaf first.
#[derive(Debug, Clone)]
pub struct ClientCertChain {
    /// The client's own certificate.
    pub leaf: X509,
    /// Intermediates the client sent, in the order sent. May be empty; a client
    /// is not obliged to send a complete chain, and the verifier may hold the
    /// intermediates itself.
    pub intermediates: Vec<X509>,
}

/// Why a forwarded chain could not be decoded.
#[derive(Debug, thiserror::Error)]
pub enum ChainDecodeError {
    #[error("forwarded certificate chain exceeds {MAX_ENCODED_CHAIN_LEN} bytes")]
    TooLong,
    #[error("forwarded certificate chain is not valid base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("forwarded certificate chain is not valid PEM: {0}")]
    Pem(#[from] ErrorStack),
    #[error("forwarded certificate chain is empty")]
    Empty,
}

impl ClientCertChain {
    /// Encodes the chain for transport in [`MZ_CLIENT_CERT_KEY`].
    ///
    /// Returns `None` if the encoded form would exceed
    /// [`MAX_ENCODED_CHAIN_LEN`]; the caller forwards nothing rather than a
    /// truncated chain, which would fail validation in a confusing way.
    pub fn encode(&self) -> Result<Option<String>, ErrorStack> {
        let mut pem = self.leaf.to_pem()?;
        for cert in &self.intermediates {
            pem.extend_from_slice(&cert.to_pem()?);
        }
        // base64 inflates by 4/3; check before encoding to avoid the large
        // intermediate allocation for an absurd chain.
        if pem.len().saturating_mul(4) / 3 > MAX_ENCODED_CHAIN_LEN {
            return Ok(None);
        }
        Ok(Some(BASE64_STANDARD.encode(&pem)))
    }

    /// Decodes a chain produced by [`Self::encode`].
    pub fn decode(encoded: &str) -> Result<ClientCertChain, ChainDecodeError> {
        if encoded.len() > MAX_ENCODED_CHAIN_LEN {
            return Err(ChainDecodeError::TooLong);
        }
        let pem = BASE64_STANDARD.decode(encoded)?;
        let mut certs = X509::stack_from_pem(&pem)?;
        if certs.is_empty() {
            return Err(ChainDecodeError::Empty);
        }
        let leaf = certs.remove(0);
        Ok(ClientCertChain {
            leaf,
            intermediates: certs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A chain survives the encode/decode round trip with its order intact.
    /// Order matters: the leaf must stay first, since the verifier treats
    /// everything after it as an untrusted intermediate.
    #[mz_ore::test]
    fn chain_round_trips_leaf_first() {
        let (leaf, intermediate) = (test_cert("leaf"), test_cert("intermediate"));
        let chain = ClientCertChain {
            leaf: leaf.clone(),
            intermediates: vec![intermediate.clone()],
        };
        let encoded = chain.encode().unwrap().unwrap();
        let decoded = ClientCertChain::decode(&encoded).unwrap();
        assert_eq!(decoded.leaf.to_pem().unwrap(), leaf.to_pem().unwrap());
        assert_eq!(decoded.intermediates.len(), 1);
        assert_eq!(
            decoded.intermediates[0].to_pem().unwrap(),
            intermediate.to_pem().unwrap()
        );
    }

    #[mz_ore::test]
    fn decode_rejects_garbage() {
        assert!(ClientCertChain::decode("not base64!!!").is_err());
        assert!(ClientCertChain::decode(&BASE64_STANDARD.encode("not a pem")).is_err());
        assert!(ClientCertChain::decode(&"A".repeat(MAX_ENCODED_CHAIN_LEN + 1)).is_err());
    }

    fn test_cert(cn: &str) -> X509 {
        use openssl::asn1::Asn1Time;
        use openssl::hash::MessageDigest;
        use openssl::nid::Nid;
        use openssl::pkey::PKey;
        use openssl::rsa::Rsa;
        use openssl::x509::{X509, X509NameBuilder};

        let pkey = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
        let name = {
            let mut builder = X509NameBuilder::new().unwrap();
            builder.append_entry_by_nid(Nid::COMMONNAME, cn).unwrap();
            builder.build()
        };
        let mut builder = X509::builder().unwrap();
        builder.set_version(2).unwrap();
        builder.set_pubkey(&pkey).unwrap();
        builder.set_subject_name(&name).unwrap();
        builder.set_issuer_name(&name).unwrap();
        builder
            .set_not_before(&Asn1Time::days_from_now(0).unwrap())
            .unwrap();
        builder
            .set_not_after(&Asn1Time::days_from_now(1).unwrap())
            .unwrap();
        builder.sign(&pkey, MessageDigest::sha256()).unwrap();
        builder.build()
    }
}
