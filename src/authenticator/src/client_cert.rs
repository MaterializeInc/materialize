// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mutual TLS client authentication: deciding whether a presented X.509 chain
//! admits a connection.
//!
//! The trust decision is deliberately separated from the TLS handshake. The
//! handshake proves the client holds its leaf's private key, and that proof can
//! only be obtained by whoever terminated TLS. Deciding whether the *issuer* is
//! acceptable is a pure function of a chain and a set of trust anchors, so it
//! can happen later, elsewhere, against configuration that changes at runtime.
//! That separation is what lets `balancerd` obtain the proof for a tenant whose
//! trust anchors it has never heard of.

use std::sync::{Arc, Mutex};

use mz_adapter_types::dyncfgs::{MTLS_CLIENT_CA, MTLS_IDENTITY_BINDING, MTLS_MODE};
use mz_dyncfg::ConfigSet;
use mz_pgwire_common::ClientCertChain;
use openssl::error::ErrorStack;
use openssl::nid::Nid;
use openssl::stack::Stack;
use openssl::x509::store::{X509Store, X509StoreBuilder};
use openssl::x509::verify::{X509VerifyFlags, X509VerifyParam};
use openssl::x509::{X509, X509StoreContext};
use tracing::warn;

/// How strictly to enforce mutual TLS on external logins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MtlsMode {
    /// Ignore client certificates entirely.
    Disable,
    /// Evaluate a certificate if one is presented, but admit connections
    /// without one.
    ///
    /// The rollout mode: an operator turns this on, watches the metric to
    /// confirm every client is presenting a trusted certificate, and only then
    /// moves to [`MtlsMode::Require`].
    Allow,
    /// Reject external logins that do not present a trusted certificate.
    Require,
}

impl MtlsMode {
    fn parse(s: &str) -> Option<MtlsMode> {
        match s {
            "disable" => Some(MtlsMode::Disable),
            "allow" => Some(MtlsMode::Allow),
            "require" => Some(MtlsMode::Require),
            _ => None,
        }
    }
}

/// Which certificate field, if any, must agree with the connecting username.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityBinding {
    /// The certificate is an admission gate only; any trusted certificate
    /// admits any username.
    None,
    /// The leaf's Subject Common Name must equal the username, as with
    /// PostgreSQL's `clientcert=verify-full`.
    CommonName,
}

impl IdentityBinding {
    fn parse(s: &str) -> Option<IdentityBinding> {
        match s {
            "none" => Some(IdentityBinding::None),
            "common-name" => Some(IdentityBinding::CommonName),
            _ => None,
        }
    }
}

/// Where a chain came from, which determines whether it may be believed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertSource {
    /// Presented on this connection's own TLS handshake.
    Direct,
    /// Forwarded by a peer that authenticated as a trusted proxy.
    ForwardedByTrustedProxy,
}

/// Why a connection was refused on certificate grounds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MtlsError {
    /// No certificate was presented, and one is required.
    Absent,
    /// A certificate was presented but does not chain to a trust anchor. Also
    /// covers an expired certificate, which OpenSSL reports as a chain failure.
    Untrusted {
        subject: String,
        issuer: String,
        reason: String,
    },
    /// The certificate is trusted but names someone other than the connecting
    /// user.
    IdentityMismatch { expected: String, found: String },
    /// `require` is set but no trust anchors are configured, so no certificate
    /// can ever be trusted.
    NoTrustAnchors,
}

impl MtlsError {
    /// The message shown to the rejected client.
    ///
    /// Deliberately coarse: a client learns that its certificate was not
    /// accepted, not which anchors are configured or how far up the chain
    /// validation reached.
    pub fn client_message(&self) -> String {
        match self {
            MtlsError::Absent => "a client certificate is required".into(),
            MtlsError::Untrusted { .. } | MtlsError::NoTrustAnchors => {
                "client certificate is not trusted".into()
            }
            MtlsError::IdentityMismatch { .. } => {
                "client certificate does not match the requested user".into()
            }
        }
    }

    /// A short, stable label for metrics.
    pub fn metric_label(&self) -> &'static str {
        match self {
            MtlsError::Absent => "absent",
            MtlsError::Untrusted { .. } => "untrusted_issuer",
            MtlsError::IdentityMismatch { .. } => "identity_mismatch",
            MtlsError::NoTrustAnchors => "no_trust_anchors",
        }
    }
}

/// The identity a trusted certificate established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedIdentity {
    /// The leaf's Subject Common Name, if it has one. A SPIFFE-shaped PKI
    /// leaves this empty and puts the identity in a URI SAN instead.
    pub common_name: Option<String>,
    /// The leaf's issuer, for logging and audit.
    pub issuer: String,
    /// Whether the chain arrived directly or through a proxy.
    pub source: CertSource,
}

/// The mutual TLS policy in force, read from system configuration.
#[derive(Clone)]
pub struct MtlsPolicy {
    pub mode: MtlsMode,
    pub identity_binding: IdentityBinding,
    trust_store: Option<Arc<X509Store>>,
}

// `X509Store` is not `Debug`; report whether anchors are loaded rather than
// dumping them.
impl std::fmt::Debug for MtlsPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MtlsPolicy")
            .field("mode", &self.mode)
            .field("identity_binding", &self.identity_binding)
            .field("has_trust_anchors", &self.trust_store.is_some())
            .finish()
    }
}

impl MtlsPolicy {
    /// Reads the policy from system configuration, parsing the trust anchors
    /// through `cache`.
    ///
    /// An unparseable `mtls_mode` or `mtls_identity_binding` falls back to the
    /// strictest interpretation of what was probably meant, rather than to
    /// `disable`: a typo in a security setting must not quietly disable it.
    pub fn from_configs(configs: &ConfigSet, cache: &TrustStoreCache) -> MtlsPolicy {
        let mode_raw = MTLS_MODE.get(configs);
        let mode = MtlsMode::parse(&mode_raw).unwrap_or_else(|| {
            warn!(
                mode = %mode_raw,
                "unrecognized mtls_mode, failing closed with 'require'"
            );
            MtlsMode::Require
        });
        let binding_raw = MTLS_IDENTITY_BINDING.get(configs);
        let identity_binding = IdentityBinding::parse(&binding_raw).unwrap_or_else(|| {
            warn!(
                binding = %binding_raw,
                "unrecognized mtls_identity_binding, failing closed with 'common-name'"
            );
            IdentityBinding::CommonName
        });
        let trust_store = match MTLS_CLIENT_CA.get(configs) {
            Some(pem) if !pem.trim().is_empty() => cache.get(&pem),
            _ => None,
        };
        MtlsPolicy {
            mode,
            identity_binding,
            trust_store,
        }
    }

    /// Whether this policy needs to look at certificates at all.
    pub fn is_enabled(&self) -> bool {
        self.mode != MtlsMode::Disable
    }

    /// Applies the policy to whatever the connection presented.
    ///
    /// `chain` is `None` when no certificate reached us, either because the
    /// client sent none or because it was forwarded by a peer we did not
    /// authenticate as a proxy. Those cases are indistinguishable here on
    /// purpose: an unauthenticated assertion is worth exactly as much as no
    /// assertion.
    pub fn check(
        &self,
        chain: Option<&ClientCertChain>,
        source: CertSource,
        user: &str,
    ) -> Result<Option<VerifiedIdentity>, MtlsError> {
        if self.mode == MtlsMode::Disable {
            return Ok(None);
        }
        let Some(store) = &self.trust_store else {
            // Configuring `require` without anchors denies everyone. That is the
            // correct reading of the configuration, but it is almost certainly a
            // mistake, so say so.
            return match self.mode {
                MtlsMode::Require => {
                    warn!(
                        "mtls_mode is 'require' but mtls_client_ca is empty; all external logins \
                         will be denied"
                    );
                    Err(MtlsError::NoTrustAnchors)
                }
                MtlsMode::Disable | MtlsMode::Allow => Ok(None),
            };
        };
        let Some(chain) = chain else {
            return match self.mode {
                MtlsMode::Require => Err(MtlsError::Absent),
                MtlsMode::Disable | MtlsMode::Allow => Ok(None),
            };
        };

        let identity = verify_chain(store, chain, source)?;

        match self.identity_binding {
            IdentityBinding::None => {}
            IdentityBinding::CommonName => {
                // A leaf with no CN cannot satisfy a CN binding. Treating a
                // missing CN as the empty string would admit a certificate from
                // a SPIFFE-shaped PKI whenever the username is also empty, and
                // pgwire defaults an absent `user` parameter to the empty string.
                let Some(cn) = identity.common_name.clone() else {
                    return Err(MtlsError::IdentityMismatch {
                        expected: user.to_string(),
                        found: String::new(),
                    });
                };
                if cn != user {
                    return Err(MtlsError::IdentityMismatch {
                        expected: user.to_string(),
                        found: cn,
                    });
                }
            }
        }
        Ok(Some(identity))
    }
}

/// Verifies `chain` against `store`, returning the identity it establishes.
fn verify_chain(
    store: &X509Store,
    chain: &ClientCertChain,
    source: CertSource,
) -> Result<VerifiedIdentity, MtlsError> {
    let issuer = format_name(&chain.leaf, |c| c.issuer_name());
    // The subject is formatted lazily, since it is only wanted on the failure
    // path while this runs on every connection.
    let untrusted = |reason: String| MtlsError::Untrusted {
        subject: format_name(&chain.leaf, |c| c.subject_name()),
        issuer: issuer.clone(),
        reason,
    };

    let mut intermediates = Stack::new().map_err(|e| untrusted(e.to_string()))?;
    for cert in &chain.intermediates {
        intermediates
            .push(cert.clone())
            .map_err(|e| untrusted(e.to_string()))?;
    }

    let mut ctx = X509StoreContext::new().map_err(|e| untrusted(e.to_string()))?;
    // `verify_cert` checks the signature chain up to an anchor and the validity
    // window of every certificate in it, so expiry lands here as a chain error.
    // `ctx.error()` is only meaningful inside the closure, while the context
    // still holds the failed verification's state.
    let failure = ctx
        .init(store, &chain.leaf, &intermediates, |ctx| {
            Ok(match ctx.verify_cert()? {
                true => None,
                false => Some(ctx.error().to_string()),
            })
        })
        .map_err(|e| untrusted(e.to_string()))?;
    if let Some(reason) = failure {
        return Err(untrusted(reason));
    }

    Ok(VerifiedIdentity {
        common_name: common_name(&chain.leaf),
        issuer,
        source,
    })
}

/// The leaf's Subject Common Name, if present.
///
/// NOTE: decoded strictly from the raw bytes rather than via OpenSSL's
/// `as_utf8`, which stops at an interior NUL. Truncating here would let a
/// certificate whose CN is `admin\0attacker.example` compare equal to `admin`.
fn common_name(cert: &X509) -> Option<String> {
    cert.subject_name()
        .entries_by_nid(Nid::COMMONNAME)
        .next()
        .and_then(|entry| std::str::from_utf8(entry.data().as_slice()).ok())
        .map(|cn| cn.to_string())
}

fn format_name<'a, F>(cert: &'a X509, f: F) -> String
where
    F: FnOnce(&'a X509) -> &'a openssl::x509::X509NameRef,
{
    f(cert)
        .entries()
        .map(|e| {
            let value = String::from_utf8_lossy(e.data().as_slice()).into_owned();
            format!("{}={}", e.object().nid().short_name().unwrap_or("?"), value)
        })
        .collect::<Vec<_>>()
        .join(",")
}

/// Caches the parsed trust store for a PEM bundle.
///
/// Parsing a bundle on every connection is wasted work: the bundle changes when
/// an operator runs `ALTER SYSTEM SET`, which is rare, while connections are
/// not. The cache holds one entry, keyed on the bundle text, because there is
/// one bundle in force at a time.
#[derive(Default)]
pub struct TrustStoreCache {
    inner: Mutex<Option<(String, Arc<X509Store>)>>,
}

impl std::fmt::Debug for TrustStoreCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TrustStoreCache")
    }
}

impl TrustStoreCache {
    pub fn new() -> TrustStoreCache {
        TrustStoreCache::default()
    }

    /// The trust store for `pem`, or `None` if it contains no usable anchors.
    ///
    /// A bundle that fails to parse yields `None`, which under `require` denies
    /// every connection. Failing closed on a malformed trust anchor is the only
    /// safe reading.
    pub fn get(&self, pem: &str) -> Option<Arc<X509Store>> {
        let mut guard = self.inner.lock().expect("poisoned");
        if let Some((cached_pem, store)) = &*guard {
            if cached_pem == pem {
                return Some(Arc::clone(store));
            }
        }
        match build_trust_store(pem) {
            Ok(store) => {
                let store = Arc::new(store);
                *guard = Some((pem.to_string(), Arc::clone(&store)));
                Some(store)
            }
            Err(e) => {
                warn!("mtls_client_ca is not a usable PEM bundle: {e}");
                // Drop any previously cached store: leaving a stale one in place
                // would keep honouring anchors the operator has replaced.
                *guard = None;
                None
            }
        }
    }
}

fn build_trust_store(pem: &str) -> Result<X509Store, anyhow::Error> {
    let certs = X509::stack_from_pem(pem.as_bytes()).map_err(anyhow::Error::from)?;
    if certs.is_empty() {
        anyhow::bail!("no certificates found");
    }
    let mut builder = X509StoreBuilder::new().map_err(anyhow::Error::from)?;
    for cert in certs {
        builder.add_cert(cert).map_err(anyhow::Error::from)?;
    }
    // PARTIAL_CHAIN makes every certificate in the bundle a valid terminus,
    // rather than requiring the chain to reach a self-signed root. Without it,
    // an operator who pins an intermediate (a sub-CA scoped to their team, say)
    // gets "unable to get issuer certificate" even though they named the exact
    // authority they meant to trust. "These are my trust anchors" is what the
    // configuration says, so it is what it should mean.
    let mut params = X509VerifyParam::new().map_err(anyhow::Error::from)?;
    params
        .set_flags(X509VerifyFlags::PARTIAL_CHAIN)
        .map_err(anyhow::Error::from)?;
    builder.set_param(&params).map_err(anyhow::Error::from)?;
    Ok(builder.build())
}

/// The authority that issues identities to trusted proxies.
///
/// A newtype rather than a bare `X509Store` so that the config structs carrying
/// it can still derive `Debug`, and so the type itself says which of the two
/// trust roles it fills. It is not interchangeable with the `mtls_client_ca`
/// anchors: this one vouches for infrastructure, those vouch for end clients.
#[derive(Clone)]
pub struct ProxyCa(Arc<X509Store>);

impl std::fmt::Debug for ProxyCa {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProxyCa")
    }
}

impl ProxyCa {
    /// Loads a proxy authority from a PEM bundle.
    pub fn from_pem(pem: &[u8]) -> Result<ProxyCa, ErrorStack> {
        let certs = X509::stack_from_pem(pem)?;
        let mut builder = X509StoreBuilder::new()?;
        for cert in certs {
            builder.add_cert(cert)?;
        }
        Ok(ProxyCa(Arc::new(builder.build())))
    }

    /// Whether `peer` is a proxy trusted to forward client certificates on
    /// another party's behalf, i.e. its own certificate chains to this authority.
    pub fn trusts(&self, peer: &ClientCertChain) -> bool {
        match verify_chain(&self.0, peer, CertSource::Direct) {
            Ok(_) => true,
            Err(e) => {
                warn!(
                    "peer asserted a forwarded client certificate but is not a trusted proxy: {e:?}"
                );
                false
            }
        }
    }
}

/// Whether `peer` may forward a client certificate on another party's behalf.
///
/// Returns `false` when no proxy authority is configured. A deployment that has
/// not named a proxy authority has no way to tell a proxy from anyone else who
/// can reach the port, so it must believe no one.
pub fn is_trusted_proxy(proxy_ca: Option<&ProxyCa>, peer: Option<&ClientCertChain>) -> bool {
    match (proxy_ca, peer) {
        (Some(ca), Some(peer)) => ca.trusts(peer),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use openssl::asn1::Asn1Time;
    use openssl::hash::MessageDigest;
    use openssl::pkey::{PKey, Private};
    use openssl::rsa::Rsa;
    use openssl::x509::extension::BasicConstraints;
    use openssl::x509::{X509Name, X509NameBuilder};

    use super::*;

    /// A certificate authority for the tests, able to sign leaves and
    /// intermediate authorities.
    struct TestCa {
        name: X509Name,
        cert: X509,
        pkey: PKey<Private>,
    }

    impl TestCa {
        fn root(cn: &str) -> TestCa {
            TestCa::new(cn, None)
        }

        fn new(cn: &str, parent: Option<&TestCa>) -> TestCa {
            let pkey = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
            let name = name_with_cn(cn);
            let mut builder = X509::builder().unwrap();
            builder.set_version(2).unwrap();
            builder.set_pubkey(&pkey).unwrap();
            builder
                .set_issuer_name(parent.map(|p| &p.name).unwrap_or(&name))
                .unwrap();
            builder.set_subject_name(&name).unwrap();
            builder
                .set_not_before(&Asn1Time::days_from_now(0).unwrap())
                .unwrap();
            builder
                .set_not_after(&Asn1Time::days_from_now(365).unwrap())
                .unwrap();
            builder
                .append_extension(BasicConstraints::new().critical().ca().build().unwrap())
                .unwrap();
            builder
                .sign(
                    parent.map(|p| &p.pkey).unwrap_or(&pkey),
                    MessageDigest::sha256(),
                )
                .unwrap();
            TestCa {
                name,
                cert: builder.build(),
                pkey,
            }
        }

        fn intermediate(&self, cn: &str) -> TestCa {
            TestCa::new(cn, Some(self))
        }

        /// Signs a leaf with the given subject and validity window.
        fn sign_leaf(
            &self,
            subject: &X509Name,
            not_before: &Asn1Time,
            not_after: &Asn1Time,
        ) -> X509 {
            let pkey = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
            let mut builder = X509::builder().unwrap();
            builder.set_version(2).unwrap();
            builder.set_pubkey(&pkey).unwrap();
            builder.set_issuer_name(&self.name).unwrap();
            builder.set_subject_name(subject).unwrap();
            builder.set_not_before(not_before).unwrap();
            builder.set_not_after(not_after).unwrap();
            builder.sign(&self.pkey, MessageDigest::sha256()).unwrap();
            builder.build()
        }

        fn leaf(&self, cn: &str) -> X509 {
            self.sign_leaf(
                &name_with_cn(cn),
                &Asn1Time::days_from_now(0).unwrap(),
                &Asn1Time::days_from_now(365).unwrap(),
            )
        }

        /// A leaf whose validity window closed in 1970. `Asn1Time` cannot express
        /// a past offset from now, so the window comes from explicit instants.
        fn expired_leaf(&self, cn: &str) -> X509 {
            self.sign_leaf(
                &name_with_cn(cn),
                &Asn1Time::from_unix(0).unwrap(),
                &Asn1Time::from_unix(1).unwrap(),
            )
        }

        /// A leaf with an empty subject, as a SPIFFE-shaped PKI would issue.
        fn leaf_without_cn(&self) -> X509 {
            self.sign_leaf(
                &X509NameBuilder::new().unwrap().build(),
                &Asn1Time::days_from_now(0).unwrap(),
                &Asn1Time::days_from_now(1).unwrap(),
            )
        }

        fn pem(&self) -> String {
            String::from_utf8(self.cert.to_pem().unwrap()).unwrap()
        }
    }

    fn name_with_cn(cn: &str) -> X509Name {
        let mut builder = X509NameBuilder::new().unwrap();
        builder.append_entry_by_nid(Nid::COMMONNAME, cn).unwrap();
        builder.build()
    }

    fn chain(leaf: X509, intermediates: Vec<X509>) -> ClientCertChain {
        ClientCertChain {
            leaf,
            intermediates,
        }
    }

    /// Builds a policy directly, bypassing the `ConfigSet` plumbing.
    fn make_policy(anchors: Option<&str>, mode: MtlsMode, binding: IdentityBinding) -> MtlsPolicy {
        MtlsPolicy {
            mode,
            identity_binding: binding,
            trust_store: anchors.and_then(|pem| TrustStoreCache::new().get(pem)),
        }
    }

    #[mz_ore::test]
    fn trusted_leaf_is_admitted() {
        let ca = TestCa::root("test ca");
        let policy = make_policy(Some(&ca.pem()), MtlsMode::Require, IdentityBinding::None);
        let identity = policy
            .check(
                Some(&chain(ca.leaf("client"), vec![])),
                CertSource::Direct,
                "materialize",
            )
            .expect("trusted chain admitted")
            .expect("identity established");
        assert_eq!(identity.common_name.as_deref(), Some("client"));
    }

    /// A leaf from an authority that is not an anchor is refused even though it
    /// is perfectly valid and its own CA would vouch for it. This is the property
    /// that makes the feature worth anything.
    #[mz_ore::test]
    fn leaf_from_another_authority_is_refused() {
        let trusted = TestCa::root("trusted ca");
        let other = TestCa::root("some other ca");
        let policy = make_policy(
            Some(&trusted.pem()),
            MtlsMode::Require,
            IdentityBinding::None,
        );
        let err = policy
            .check(
                Some(&chain(other.leaf("client"), vec![])),
                CertSource::Direct,
                "materialize",
            )
            .expect_err("untrusted issuer refused");
        assert!(matches!(err, MtlsError::Untrusted { .. }), "{err:?}");
    }

    /// A chain through an intermediate validates when the client supplies the
    /// intermediate, and fails when it does not, since the verifier holds only
    /// the root.
    #[mz_ore::test]
    fn intermediate_chain_requires_the_intermediate() {
        let root = TestCa::root("root ca");
        let intermediate = root.intermediate("intermediate ca");
        let leaf = intermediate.leaf("client");
        let policy = make_policy(Some(&root.pem()), MtlsMode::Require, IdentityBinding::None);

        policy
            .check(
                Some(&chain(leaf.clone(), vec![intermediate.cert.clone()])),
                CertSource::Direct,
                "materialize",
            )
            .expect("complete chain admitted");

        let err = policy
            .check(
                Some(&chain(leaf, vec![])),
                CertSource::Direct,
                "materialize",
            )
            .expect_err("incomplete chain refused");
        assert!(matches!(err, MtlsError::Untrusted { .. }), "{err:?}");

        // Trusting the intermediate directly also works, which is what a
        // customer pinning a sub-CA would configure.
        let pinned = make_policy(
            Some(&String::from_utf8(intermediate.cert.to_pem().unwrap()).unwrap()),
            MtlsMode::Require,
            IdentityBinding::None,
        );
        pinned
            .check(
                Some(&chain(intermediate.leaf("client"), vec![])),
                CertSource::Direct,
                "materialize",
            )
            .expect("pinned intermediate admitted");
    }

    /// An expired certificate from a trusted authority is refused. OpenSSL
    /// reports expiry as a chain failure, so this guards that the validity
    /// window is actually checked rather than only the signature.
    #[mz_ore::test]
    fn expired_leaf_is_refused() {
        let ca = TestCa::root("test ca");
        let policy = make_policy(Some(&ca.pem()), MtlsMode::Require, IdentityBinding::None);
        let err = policy
            .check(
                Some(&chain(ca.expired_leaf("client"), vec![])),
                CertSource::Direct,
                "materialize",
            )
            .expect_err("expired leaf refused");
        assert!(matches!(err, MtlsError::Untrusted { .. }), "{err:?}");
    }

    #[mz_ore::test]
    fn require_refuses_a_connection_with_no_certificate() {
        let ca = TestCa::root("test ca");
        let policy = make_policy(Some(&ca.pem()), MtlsMode::Require, IdentityBinding::None);
        assert_eq!(
            policy.check(None, CertSource::Direct, "materialize"),
            Err(MtlsError::Absent)
        );
    }

    /// `allow` is the rollout mode: a trusted certificate is recorded, an absent
    /// one is tolerated, but a certificate from the wrong authority is still
    /// refused. Tolerating an untrusted certificate would make the mode useless
    /// for validating a rollout, since the metric would not distinguish clients
    /// that are ready from clients that are misconfigured.
    #[mz_ore::test]
    fn allow_tolerates_absence_but_not_a_bad_certificate() {
        let ca = TestCa::root("test ca");
        let other = TestCa::root("other ca");
        let policy = make_policy(Some(&ca.pem()), MtlsMode::Allow, IdentityBinding::None);

        assert_eq!(
            policy.check(None, CertSource::Direct, "materialize"),
            Ok(None)
        );
        assert!(
            policy
                .check(
                    Some(&chain(ca.leaf("client"), vec![])),
                    CertSource::Direct,
                    "materialize"
                )
                .expect("trusted chain admitted")
                .is_some()
        );
        assert!(
            policy
                .check(
                    Some(&chain(other.leaf("client"), vec![])),
                    CertSource::Direct,
                    "materialize"
                )
                .is_err()
        );
    }

    #[mz_ore::test]
    fn disable_ignores_everything() {
        let other = TestCa::root("untrusted ca");
        let policy = make_policy(None, MtlsMode::Disable, IdentityBinding::None);
        assert_eq!(
            policy.check(
                Some(&chain(other.leaf("nobody"), vec![])),
                CertSource::Direct,
                "materialize"
            ),
            Ok(None)
        );
        assert_eq!(
            policy.check(None, CertSource::Direct, "materialize"),
            Ok(None)
        );
    }

    /// `require` with no anchors denies rather than admitting. Getting this
    /// backwards would turn a misconfiguration into an open door.
    #[mz_ore::test]
    fn require_without_anchors_denies() {
        let policy = make_policy(None, MtlsMode::Require, IdentityBinding::None);
        assert_eq!(
            policy.check(None, CertSource::Direct, "materialize"),
            Err(MtlsError::NoTrustAnchors)
        );
        let ca = TestCa::root("test ca");
        assert_eq!(
            policy.check(
                Some(&chain(ca.leaf("client"), vec![])),
                CertSource::Direct,
                "materialize"
            ),
            Err(MtlsError::NoTrustAnchors)
        );
    }

    /// A malformed bundle yields no anchors, so `require` denies. Falling back to
    /// admitting everyone when the operator's PEM is broken would be the worst
    /// possible failure mode.
    #[mz_ore::test]
    fn malformed_anchors_deny_under_require() {
        let policy = make_policy(
            Some("-----BEGIN CERTIFICATE-----\nnot a certificate\n-----END CERTIFICATE-----"),
            MtlsMode::Require,
            IdentityBinding::None,
        );
        assert_eq!(
            policy.check(None, CertSource::Direct, "materialize"),
            Err(MtlsError::NoTrustAnchors)
        );
    }

    #[mz_ore::test]
    fn common_name_binding_matches_the_username() {
        let ca = TestCa::root("test ca");
        let policy = make_policy(
            Some(&ca.pem()),
            MtlsMode::Require,
            IdentityBinding::CommonName,
        );
        policy
            .check(
                Some(&chain(ca.leaf("materialize"), vec![])),
                CertSource::Direct,
                "materialize",
            )
            .expect("matching CN admitted");

        let err = policy
            .check(
                Some(&chain(ca.leaf("someone_else"), vec![])),
                CertSource::Direct,
                "materialize",
            )
            .expect_err("mismatched CN refused");
        assert_eq!(
            err,
            MtlsError::IdentityMismatch {
                expected: "materialize".into(),
                found: "someone_else".into(),
            }
        );
    }

    /// A leaf with no CN cannot satisfy a CN binding. Without this, a
    /// certificate from a SPIFFE-shaped PKI would compare its empty CN against
    /// the username and, for an empty username, match.
    #[mz_ore::test]
    fn common_name_binding_refuses_a_leaf_without_one() {
        let ca = TestCa::root("test ca");
        let policy = make_policy(
            Some(&ca.pem()),
            MtlsMode::Require,
            IdentityBinding::CommonName,
        );
        assert!(
            policy
                .check(
                    Some(&chain(ca.leaf_without_cn(), vec![])),
                    CertSource::Direct,
                    ""
                )
                .is_err()
        );
    }

    /// A bundle with several anchors admits leaves from any of them, which is
    /// what makes a CA rotation possible without a flag day.
    #[mz_ore::test]
    fn multiple_anchors_are_all_trusted() {
        let old = TestCa::root("old ca");
        let new = TestCa::root("new ca");
        let bundle = format!("{}{}", old.pem(), new.pem());
        let policy = make_policy(Some(&bundle), MtlsMode::Require, IdentityBinding::None);
        for ca in [&old, &new] {
            policy
                .check(
                    Some(&chain(ca.leaf("client"), vec![])),
                    CertSource::Direct,
                    "materialize",
                )
                .expect("leaf from a bundled anchor admitted");
        }
    }

    #[mz_ore::test]
    fn trust_store_cache_reuses_and_invalidates() {
        let first = TestCa::root("first ca");
        let second = TestCa::root("second ca");
        let cache = TrustStoreCache::new();

        let a = cache.get(&first.pem()).expect("built");
        let b = cache.get(&first.pem()).expect("cached");
        assert!(Arc::ptr_eq(&a, &b), "same bundle should hit the cache");

        let c = cache.get(&second.pem()).expect("rebuilt");
        assert!(!Arc::ptr_eq(&a, &c), "a new bundle should rebuild");

        // A bundle that fails to parse must not leave the previous store in
        // place, or an operator replacing their anchors with a typo would keep
        // honouring the anchors they meant to remove.
        assert!(cache.get("garbage").is_none());
        let d = cache.get(&second.pem()).expect("rebuilt after failure");
        assert!(!Arc::ptr_eq(&c, &d));
    }

    /// Only a peer whose own certificate chains to the proxy authority may
    /// forward a client certificate.
    #[mz_ore::test]
    fn proxy_trust_is_scoped_to_the_proxy_authority() {
        let proxy_ca = TestCa::root("proxy ca");
        let client_ca = TestCa::root("client ca");
        let ca = ProxyCa::from_pem(proxy_ca.pem().as_bytes()).expect("loads");

        assert!(ca.trusts(&chain(proxy_ca.leaf("balancerd"), vec![])));
        // A certificate from the *client* authority does not make a proxy. The
        // two roles are separate trust decisions over the same handshake.
        assert!(!ca.trusts(&chain(client_ca.leaf("balancerd"), vec![])));

        // With no proxy authority configured, nobody may forward.
        assert!(!is_trusted_proxy(
            None,
            Some(&chain(proxy_ca.leaf("x"), vec![]))
        ));
        // And a proxy that presents nothing is not a proxy.
        assert!(!is_trusted_proxy(Some(&ca), None));
    }
}
