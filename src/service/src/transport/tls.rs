// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Mutual TLS for CTP connections.
//!
//! CTP assumes a trusted network between controller and replica. When that assumption does not
//! hold (pod-to-pod traffic in an orchestrated deployment), connections can instead be protected
//! with mutual TLS: both endpoints present an X.509 certificate signed by a shared, deployment
//! internal certificate authority, and both endpoints verify the peer's certificate chain and
//! identity before any CTP bytes are exchanged.
//!
//! Identities are DNS-shaped names carried in the certificate's subject alternative name (SAN).
//! Each side is configured with the exact identity its peer must present. The CTP `Hello`
//! handshake still runs inside the TLS channel, but it is a compatibility check only. Peer
//! authentication is the certificate's job.
//!
//! Certificates are expected to be issued by [`CertificateAuthority`], which the controller owns.
//! The CA is not a public-web CA: it signs only leaf certificates (path length zero) for the
//! endpoints of a single deployment, and both sides trust nothing else.
//!
//! TLS 1.3 only, with the aws-lc-rs provider. The provider is selected explicitly, so this module
//! works regardless of the process-wide default provider.

use std::fmt;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use mz_ore::secure::SecureString;
use rustls::client::verify_server_name;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::{ParsedCertificate, WebPkiClientVerifier};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

/// PEM-encoded key material for one CTP endpoint.
///
/// `cert_pem` must chain directly to `ca_cert_pem`: the verifiers built from this type trust
/// exactly one CA and accept no intermediates.
#[derive(Debug)]
pub struct TlsCredentials {
    /// The CA certificate that signed both this endpoint's and the peer's certificate.
    pub ca_cert_pem: String,
    /// This endpoint's certificate.
    pub cert_pem: String,
    /// This endpoint's private key.
    pub key_pem: SecureString,
}

impl TlsCredentials {
    fn ca_root_store(&self) -> anyhow::Result<RootCertStore> {
        let ca_cert = CertificateDer::from_pem_slice(self.ca_cert_pem.as_bytes())
            .context("parsing CA certificate")?;
        let mut roots = RootCertStore::empty();
        roots.add(ca_cert).context("adding CA certificate")?;
        Ok(roots)
    }

    fn cert_chain(&self) -> anyhow::Result<Vec<CertificateDer<'static>>> {
        let cert = CertificateDer::from_pem_slice(self.cert_pem.as_bytes())
            .context("parsing certificate")?;
        Ok(vec![cert])
    }

    fn key(&self) -> anyhow::Result<PrivateKeyDer<'static>> {
        PrivateKeyDer::from_pem_slice(self.key_pem.unsecure().as_bytes())
            .context("parsing private key")
    }
}

/// The rustls provider and protocol versions shared by both endpoint configs.
fn config_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// TLS configuration for the client (controller) side of a CTP connection.
#[derive(Clone)]
pub struct ClientTlsConfig {
    connector: TlsConnector,
    server_name: ServerName<'static>,
}

impl fmt::Debug for ClientTlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientTlsConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl ClientTlsConfig {
    /// Create a client TLS config.
    ///
    /// `server_identity` is the DNS-shaped identity the server's certificate must carry in its
    /// SAN. It is also sent as SNI, so it must be a valid DNS name.
    pub fn new(credentials: &TlsCredentials, server_identity: &str) -> anyhow::Result<Self> {
        let server_name =
            ServerName::try_from(server_identity.to_string()).context("invalid server identity")?;

        let config = ClientConfig::builder_with_provider(config_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .context("selecting TLS versions")?
            .with_root_certificates(credentials.ca_root_store()?)
            .with_client_auth_cert(credentials.cert_chain()?, credentials.key()?)
            .context("configuring client certificate")?;

        Ok(Self {
            connector: TlsConnector::from(Arc::new(config)),
            server_name,
        })
    }

    /// Perform a TLS handshake over the given stream.
    ///
    /// Verification of the server's certificate chain and identity (against `server_identity`)
    /// happens inside the handshake. On success the returned stream is mutually authenticated.
    pub(super) async fn connect<S>(&self, stream: S) -> io::Result<client::TlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.connector
            .connect(self.server_name.clone(), stream)
            .await
    }
}

/// TLS configuration for the server (replica) side of a CTP connection.
#[derive(Clone)]
pub struct ServerTlsConfig {
    acceptor: TlsAcceptor,
    client_identity: ServerName<'static>,
}

impl fmt::Debug for ServerTlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ServerTlsConfig")
            .field("client_identity", &self.client_identity)
            .finish_non_exhaustive()
    }
}

impl ServerTlsConfig {
    /// Create a server TLS config.
    ///
    /// `client_identity` is the DNS-shaped identity the client's certificate must carry in its
    /// SAN. Clients presenting a certificate that chains to the CA but carries a different
    /// identity are rejected.
    pub fn new(credentials: &TlsCredentials, client_identity: &str) -> anyhow::Result<Self> {
        let client_identity =
            ServerName::try_from(client_identity.to_string()).context("invalid client identity")?;

        let verifier = WebPkiClientVerifier::builder_with_provider(
            Arc::new(credentials.ca_root_store()?),
            config_provider(),
        )
        .build()
        .context("building client certificate verifier")?;

        let config = ServerConfig::builder_with_provider(config_provider())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .context("selecting TLS versions")?
            .with_client_cert_verifier(verifier)
            .with_single_cert(credentials.cert_chain()?, credentials.key()?)
            .context("configuring server certificate")?;

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            client_identity,
        })
    }

    /// Perform a TLS handshake over the given stream and authenticate the client.
    ///
    /// The handshake verifies that the client's certificate chains to the CA. Its identity is
    /// verified against `client_identity` afterwards, from the SAN of the presented certificate.
    /// The handshake is bounded by `timeout` so a stalled client cannot occupy the server
    /// indefinitely.
    pub(super) async fn accept<S>(
        &self,
        stream: S,
        timeout: Duration,
    ) -> anyhow::Result<server::TlsStream<S>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let stream = mz_ore::future::timeout(timeout, self.acceptor.accept(stream))
            .await
            .context("TLS handshake")?;

        let (_, session) = stream.get_ref();
        let Some(certs) = session.peer_certificates() else {
            // The verifier requires client auth, so a completed handshake implies a certificate.
            bail!("TLS handshake completed without client certificate");
        };
        let Some(end_entity) = certs.first() else {
            bail!("TLS handshake completed with empty certificate chain");
        };
        let parsed = ParsedCertificate::try_from(end_entity)?;
        verify_server_name(&parsed, &self.client_identity).context("verifying client identity")?;

        Ok(stream)
    }
}

/// A certificate authority for CTP endpoints.
///
/// The CA is deployment internal: it signs only leaf certificates (path length zero), and CTP
/// endpoints trust exactly one such CA. The holder of the CA key mints one identity per endpoint
/// and distributes them out of band.
pub struct CertificateAuthority {
    cert_pem: String,
    key_pem: SecureString,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

impl fmt::Debug for CertificateAuthority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CertificateAuthority")
            .field("cert_pem", &self.cert_pem)
            .finish_non_exhaustive()
    }
}

/// Certificate and key for one endpoint identity, as issued by a [`CertificateAuthority`].
#[derive(Debug)]
pub struct IssuedIdentity {
    /// The identity's certificate.
    pub cert_pem: String,
    /// The identity's private key.
    pub key_pem: SecureString,
}

impl CertificateAuthority {
    /// The certificate parameters shared by [`generate`](Self::generate) and
    /// [`from_pem`](Self::from_pem).
    ///
    /// Reconstruction relies on these being a pure function of the common name: the issuer
    /// (subject) DN they produce must match the persisted CA certificate, or issued leaves fail
    /// chain validation.
    fn ca_params(common_name: &str) -> rcgen::CertificateParams {
        let mut params = rcgen::CertificateParams::default();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, common_name);
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Constrained(0));
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        params
    }

    /// Generate a new CA with an ECDSA P-256 key.
    pub fn generate(common_name: &str) -> anyhow::Result<Self> {
        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .context("generating CA key")?;
        let params = Self::ca_params(common_name);
        let cert = params
            .clone()
            .self_signed(&key)
            .context("signing CA certificate")?;

        Ok(Self {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem().into(),
            issuer: rcgen::Issuer::new(params, key),
        })
    }

    /// Reconstruct a CA from its PEM-encoded certificate and key.
    ///
    /// `common_name` must be the name that was passed to [`generate`](Self::generate): the
    /// signing state is rebuilt from it rather than parsed out of the certificate. A mismatch
    /// fails closed. Certificates issued by the reconstructed CA carry the wrong issuer name,
    /// so peers verifying against `cert_pem` reject them.
    pub fn from_pem(
        common_name: &str,
        cert_pem: &str,
        key_pem: SecureString,
    ) -> anyhow::Result<Self> {
        let key = rcgen::KeyPair::from_pem(key_pem.unsecure()).context("parsing CA key")?;
        Ok(Self {
            cert_pem: cert_pem.to_string(),
            key_pem,
            issuer: rcgen::Issuer::new(Self::ca_params(common_name), key),
        })
    }

    /// The CA's certificate, for distribution to endpoints.
    pub fn cert_pem(&self) -> &str {
        &self.cert_pem
    }

    /// The CA's private key, for persisting the CA.
    pub fn key_pem(&self) -> &SecureString {
        &self.key_pem
    }

    /// Issue a certificate for the given identity, valid for `validity` from now.
    ///
    /// The identity becomes the certificate's only SAN, so it is the exact name peers configured
    /// with this identity will accept. `not_before` is backdated a small amount to tolerate clock
    /// skew between the issuer and the endpoints.
    pub fn issue(&self, identity: &str, validity: Duration) -> anyhow::Result<IssuedIdentity> {
        const CLOCK_SKEW_SLACK: Duration = Duration::from_secs(5 * 60);

        let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256)
            .context("generating key")?;
        let mut params = rcgen::CertificateParams::new(vec![identity.to_string()])
            .context("invalid identity")?;
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, identity);
        params.use_authority_key_identifier_extension = true;
        params.key_usages = vec![rcgen::KeyUsagePurpose::DigitalSignature];
        // Every endpoint identity may be used on either side of a connection, so include both
        // EKUs. Which side presents which certificate is enforced by identity, not by EKU.
        params.extended_key_usages = vec![
            rcgen::ExtendedKeyUsagePurpose::ServerAuth,
            rcgen::ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let now = time::OffsetDateTime::now_utc();
        params.not_before = now - CLOCK_SKEW_SLACK;
        params.not_after = now + validity;

        let cert = params
            .signed_by(&key, &self.issuer)
            .context("signing certificate")?;

        Ok(IssuedIdentity {
            cert_pem: cert.pem(),
            key_pem: key.serialize_pem().into(),
        })
    }
}

/// The internal secret name under which the environment CA is persisted.
pub const CA_SECRET_NAME: &str = "ctp-ca";

/// The certificate identity presented by controllers.
///
/// There is one identity for all controllers of an environment (rather than per-controller
/// identities) because every controller has the same authority over replicas. Environment
/// scoping comes from the per-environment CA, not from the identity name.
pub const CONTROLLER_IDENTITY: &str = "ctp-controller";

/// The internal secret name holding the credentials of the replica with the given service name.
pub fn replica_secret_name(service_name: &str) -> String {
    format!("ctp-{service_name}")
}

/// The JSON representation of persisted credentials.
///
/// Used both for the CA secret (where `cert_pem` is the CA's own certificate and `ca_cert_pem`
/// is empty) and for replica credential secrets. `not_after` records the certificate's expiry as
/// seconds since the Unix epoch, so that holders can report expiry without parsing X.509.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedCredentials {
    ca_cert_pem: String,
    cert_pem: String,
    key_pem: String,
    not_after: u64,
}

/// TLS state for the controller side of an environment: the environment CA, the controller's
/// own client credentials, and the handle for distributing replica credentials.
pub struct ClusterTlsContext {
    ca: CertificateAuthority,
    client_credentials: TlsCredentials,
    secrets: Arc<dyn mz_secrets::SecretsController>,
}

impl fmt::Debug for ClusterTlsContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterTlsContext").finish_non_exhaustive()
    }
}

/// The validity of certificates issued for cluster transport.
///
/// Deliberately long: replica certificates are re-minted whenever a replica process is created
/// (and controller certificates on every controller boot), so rotation rides process lifecycle
/// rather than expiry. Expiry of a live credential would break replica reconnects with no
/// automated recovery short of a replica restart, so it functions as a backstop, not as the
/// rotation mechanism.
const CERT_VALIDITY: Duration = Duration::from_secs(5 * 365 * 24 * 60 * 60);

impl ClusterTlsContext {
    /// Load the environment CA from the secrets controller, creating it if it does not exist,
    /// and mint this controller's client credentials.
    ///
    /// `ca_common_name` scopes the CA and must be stable across controller restarts (e.g. the
    /// environment ID). See [`CertificateAuthority::from_pem`] for the reconstruction contract.
    ///
    /// NOTE: Creation is not atomic. Two processes bootstrapping a brand-new environment
    /// concurrently can each create a CA, with one overwriting the other. Restarting the loser's
    /// replicas recovers, since every process re-reads the CA at boot. In practice only one
    /// controller bootstraps a new environment.
    pub async fn bootstrap(
        secrets: Arc<dyn mz_secrets::SecretsController>,
        ca_common_name: &str,
    ) -> anyhow::Result<Self> {
        let reader = secrets.reader();
        let ca = match reader.read_internal(CA_SECRET_NAME).await? {
            Some(bytes) => {
                let persisted: PersistedCredentials =
                    serde_json::from_slice(&bytes).context("decoding CA secret")?;
                CertificateAuthority::from_pem(
                    ca_common_name,
                    &persisted.cert_pem,
                    persisted.key_pem.into(),
                )?
            }
            None => {
                let ca = CertificateAuthority::generate(ca_common_name)?;
                let persisted = PersistedCredentials {
                    ca_cert_pem: String::new(),
                    cert_pem: ca.cert_pem().to_string(),
                    key_pem: ca.key_pem().unsecure().to_string(),
                    not_after: 0,
                };
                let bytes = serde_json::to_vec(&persisted).expect("serializable");
                secrets
                    .ensure_internal(CA_SECRET_NAME, &bytes)
                    .await
                    .context("persisting CA")?;
                ca
            }
        };

        let issued = ca.issue(CONTROLLER_IDENTITY, CERT_VALIDITY)?;
        let client_credentials = TlsCredentials {
            ca_cert_pem: ca.cert_pem().to_string(),
            cert_pem: issued.cert_pem,
            key_pem: issued.key_pem,
        };

        Ok(Self {
            ca,
            client_credentials,
            secrets,
        })
    }

    /// Build the client TLS config for connecting to the replica with the given service name.
    pub fn client_config(&self, replica_service_name: &str) -> anyhow::Result<ClientTlsConfig> {
        ClientTlsConfig::new(&self.client_credentials, replica_service_name)
    }

    /// Mint credentials for the replica with the given service name and write them to the
    /// replica's internal secret.
    ///
    /// Runs asynchronously relative to replica creation. The replica retries reading its secret
    /// at boot, so it converges once the write lands.
    pub async fn mint_replica_credentials(&self, service_name: &str) -> anyhow::Result<()> {
        let issued = self.ca.issue(service_name, CERT_VALIDITY)?;
        let not_after = u64::try_from(time::OffsetDateTime::now_utc().unix_timestamp())
            .expect("post-1970")
            + CERT_VALIDITY.as_secs();
        let persisted = PersistedCredentials {
            ca_cert_pem: self.ca.cert_pem().to_string(),
            cert_pem: issued.cert_pem,
            key_pem: issued.key_pem.unsecure().to_string(),
            not_after,
        };
        let bytes = serde_json::to_vec(&persisted).expect("serializable");
        self.secrets
            .ensure_internal(&replica_secret_name(service_name), &bytes)
            .await
    }

    /// Delete the credentials of the replica with the given service name.
    pub async fn delete_replica_credentials(&self, service_name: &str) -> anyhow::Result<()> {
        self.secrets
            .delete_internal(&replica_secret_name(service_name))
            .await
    }
}

/// A replica's server TLS configuration, as loaded from its credential secret.
pub struct LoadedServerTls {
    /// The server TLS config, expecting clients with the controller identity.
    pub config: ServerTlsConfig,
    /// The expiry of the server's certificate, as seconds since the Unix epoch.
    pub cert_not_after: u64,
}

/// Build a replica's server TLS config from the contents of its credential secret.
pub fn server_tls_from_secret(bytes: &[u8]) -> anyhow::Result<LoadedServerTls> {
    let persisted: PersistedCredentials =
        serde_json::from_slice(bytes).context("decoding credential secret")?;
    let credentials = TlsCredentials {
        ca_cert_pem: persisted.ca_cert_pem,
        cert_pem: persisted.cert_pem,
        key_pem: persisted.key_pem.into(),
    };
    let config = ServerTlsConfig::new(&credentials, CONTROLLER_IDENTITY)?;
    Ok(LoadedServerTls {
        config,
        cert_not_after: persisted.not_after,
    })
}
