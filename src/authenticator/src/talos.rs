// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Ory Talos authentication for pgwire/HTTP connections.
//!
//! Talos issues opaque API keys (the Ory-native replacement for Frontegg app
//! passwords). Rather than verifying a presented key on every connection (a
//! remote call on the hot path), we follow the "derive, not verify" design:
//! exchange the key once via the Talos `apiKeys:derive` admin endpoint for a
//! short-lived JWT, then validate that JWT locally against the derived-keys
//! JWKS. This mirrors the frontegg-auth architecture (exchange → local JWT
//! validation), so the session-cache and background-refresh machinery that
//! bounds revocation latency transfers directly; that layer is added together
//! with the pgwire/HTTP listener wiring, which is what consumes the
//! per-session `expired()` signal.
//!
//! Two contract facts about Talos shape this module (validated against Talos
//! OSS in the cloud repo's `src/sync/tests/talos_contract.rs`):
//!
//!   * The derived JWT carries `sub` = the key id, `act` = the actor id, and
//!     the key's issuance `metadata` verbatim under `meta`. The sync-server
//!     stamps the user's email and organization id into that metadata at
//!     issuance, so we read the connecting identity straight off the token —
//!     no directory lookup on the connection hot path.
//!
//!   * The derived-keys JWKS endpoint wraps the key set in a non-standard
//!     `{"jwks": {"keys": [...]}}` envelope rather than a bare RFC 7517
//!     `{"keys": [...]}`, and signs with EdDSA/Ed25519 in the reference
//!     config. We unwrap the envelope and let `jsonwebtoken` pick the
//!     algorithm from the JWK/header so Ed25519 validates without any
//!     algorithm-specific code (as the OIDC authenticator already does).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jsonwebtoken::DecodingKey;
use jsonwebtoken::jwk::JwkSet;
use mz_adapter::{AdapterError, AuthenticationError, Client as AdapterClient};
use mz_auth::Authenticated;
use mz_pgwire_common::{ErrorResponse, Severity};
use reqwest::Client as HttpClient;
use serde::Deserialize;
use tokio_postgres::error::SqlState;
use tracing::{debug, warn};
use url::Url;

/// Path of the Talos admin endpoint that exchanges an API key for a JWT.
const DERIVE_PATH: &str = "v2alpha1/admin/apiKeys:derive";
/// Path of the derived-keys JWKS used to validate derived JWTs locally.
const JWKS_PATH: &str = "v2alpha1/derivedKeys/jwks.json";
/// TTL requested for derived tokens. Kept short: the token only has to live
/// long enough to be validated once (and, once the session cache lands, to
/// pace re-derivation), so a short TTL tightens the revocation window.
const DEFAULT_DERIVED_TOKEN_TTL: Duration = Duration::from_secs(300);
/// Timeout for the derive and JWKS HTTP calls.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors that can occur during Talos authentication.
#[derive(Debug)]
pub enum TalosError {
    /// The presented password does not carry the Talos key prefix. Callers use
    /// this to fall through to another authenticator rather than fail the
    /// connection outright.
    NotTalosKey,
    /// Failed to reach Talos to derive a token from the presented key.
    DeriveRequestFailed { error_message: String },
    /// Talos rejected the presented key (revoked, expired, or unknown).
    InvalidKey,
    /// Failed to fetch the derived-keys JWKS.
    FetchJwksFailed { url: String, error_message: String },
    /// The derived token header has no key id.
    MissingKid,
    /// No JWKS key matched the derived token's key id.
    NoMatchingKey { key_id: String },
    /// The derived token failed signature/shape validation.
    Jwt,
    /// The derived token's signature has expired.
    ExpiredSignature,
    /// The derived token is missing the email metadata needed to map it to a
    /// Materialize role (the sync-server stamps this at issuance).
    MissingEmail,
    /// The connecting username does not match the key owner's email.
    WrongUser,
    /// The role exists but does not have the LOGIN attribute.
    NonLogin,
    /// Unexpected error while checking whether the role can log in.
    LoginCheckError,
}

impl std::fmt::Display for TalosError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TalosError::NotTalosKey => write!(f, "not a Talos app password"),
            TalosError::DeriveRequestFailed { .. } => write!(f, "failed to contact Talos"),
            TalosError::InvalidKey => write!(f, "invalid app password"),
            TalosError::FetchJwksFailed { .. } => write!(f, "failed to fetch Talos signing keys"),
            TalosError::MissingKid => write!(f, "missing key ID in derived token header"),
            TalosError::NoMatchingKey { .. } => write!(f, "no matching Talos signing key"),
            TalosError::Jwt => write!(f, "failed to validate derived token"),
            TalosError::ExpiredSignature => write!(f, "authentication credentials have expired"),
            TalosError::MissingEmail => write!(f, "app password is missing owner metadata"),
            TalosError::WrongUser => write!(f, "wrong user"),
            TalosError::NonLogin => write!(f, "role is not allowed to login"),
            TalosError::LoginCheckError => write!(f, "unexpected error checking if role can login"),
        }
    }
}

impl std::error::Error for TalosError {}

impl TalosError {
    pub fn code(&self) -> SqlState {
        SqlState::INVALID_AUTHORIZATION_SPECIFICATION
    }

    pub fn detail(&self) -> Option<String> {
        match self {
            TalosError::NoMatchingKey { key_id } => {
                Some(format!("Derived token key ID \"{key_id}\" was not found."))
            }
            TalosError::MissingEmail => Some(
                "The app password was issued without an owner email; re-create it from the console."
                    .into(),
            ),
            TalosError::NonLogin => Some("The role does not have the LOGIN attribute.".into()),
            _ => None,
        }
    }

    pub fn into_response(self) -> ErrorResponse {
        ErrorResponse {
            severity: Severity::Fatal,
            code: self.code(),
            message: self.to_string(),
            detail: self.detail(),
            hint: None,
            position: None,
        }
    }
}

/// Issuance metadata Talos surfaces under `meta` in the derived JWT. The
/// sync-server stamps these at key creation (see the cloud repo's
/// `ory::talos::TalosClient::issue_api_key`).
#[derive(Debug, Clone, Default, Deserialize)]
struct TalosMeta {
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    organization_id: Option<String>,
}

/// Claims of a Talos-derived JWT. See the module docs for the contract.
#[derive(Debug, Clone, Deserialize)]
struct TalosClaims {
    /// Subject: the Talos key id.
    sub: String,
    /// Actor: the Ory identity (actor) the key is bound to.
    #[serde(default)]
    act: Option<String>,
    /// Issuance metadata carried verbatim from the key.
    #[serde(default)]
    meta: TalosMeta,
}

/// The identity established by a validated Talos key.
#[derive(Debug, Clone)]
pub struct ValidatedTalosClaims {
    /// The login user: the key owner's email.
    pub user: String,
    /// The Ory actor (identity) id the key is bound to.
    pub actor_id: Option<String>,
    /// The organization the key owner belongs to, if stamped.
    pub organization_id: Option<String>,
    /// The Talos key id (derived-token subject); useful for audit/logging.
    pub key_id: String,
    // Prevent construction outside of `TalosAuthenticator::validate`.
    _private: (),
}

/// Credential Talos requires to call the `apiKeys:derive` admin endpoint.
///
/// How this credential is provisioned to environmentd/balancerd (a scoped
/// project key, a per-environment key, or a region-api proxy) is a deployment
/// decision tracked in the Ory migration design doc; this type only carries
/// whatever credential that decision produces, with a redacted `Debug`.
#[derive(Clone)]
pub struct DeriveCredential(String);

impl DeriveCredential {
    pub fn new(credential: String) -> Self {
        DeriveCredential(credential)
    }
}

impl std::fmt::Debug for DeriveCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("DeriveCredential")
            .field(&"<redacted>")
            .finish()
    }
}

/// Configuration for a [`TalosAuthenticator`].
#[derive(Clone, Debug)]
pub struct TalosConfig {
    /// Base URL of the Talos API (e.g. the Ory project URL). The derive and
    /// JWKS endpoints are resolved relative to this.
    pub base_url: Url,
    /// Credential for the `apiKeys:derive` admin endpoint.
    pub derive_credential: DeriveCredential,
    /// Prefix that identifies a presented password as a Talos key.
    pub key_prefix: String,
    /// TTL requested for derived tokens.
    pub derived_token_ttl: Duration,
}

impl TalosConfig {
    pub fn new(base_url: Url, derive_credential: DeriveCredential, key_prefix: String) -> Self {
        TalosConfig {
            base_url,
            derive_credential,
            key_prefix,
            derived_token_ttl: DEFAULT_DERIVED_TOKEN_TTL,
        }
    }
}

/// Authenticates pgwire/HTTP connections presenting an Ory Talos API key.
#[derive(Clone, Debug)]
pub struct TalosAuthenticator {
    inner: Arc<TalosAuthenticatorInner>,
}

#[derive(Debug)]
struct TalosAuthenticatorInner {
    adapter_client: AdapterClient,
    http_client: HttpClient,
    derive_url: Url,
    jwks_url: Url,
    derive_credential: DeriveCredential,
    key_prefix: String,
    derived_token_ttl: Duration,
    /// kid -> decoding key cache for the derived-keys JWKS, refreshed on miss.
    decoding_keys: Mutex<BTreeMap<String, DecodingKey>>,
}

impl TalosAuthenticator {
    pub fn new(
        config: TalosConfig,
        adapter_client: AdapterClient,
    ) -> Result<Self, url::ParseError> {
        let derive_url = config.base_url.join(DERIVE_PATH)?;
        let jwks_url = config.base_url.join(JWKS_PATH)?;
        Ok(TalosAuthenticator {
            inner: Arc::new(TalosAuthenticatorInner {
                adapter_client,
                http_client: HttpClient::new(),
                derive_url,
                jwks_url,
                derive_credential: config.derive_credential,
                key_prefix: config.key_prefix,
                derived_token_ttl: config.derived_token_ttl,
                decoding_keys: Mutex::new(BTreeMap::new()),
            }),
        })
    }

    /// Whether a presented password looks like a Talos key. Dispatch uses this
    /// to route between authenticators without attempting a derive.
    pub fn is_talos_key(&self, password: &str) -> bool {
        password.starts_with(&self.inner.key_prefix)
    }

    /// Authenticates a presented Talos key: derive a JWT, validate it locally,
    /// map it to an identity, and confirm the role may log in.
    pub async fn authenticate(
        &self,
        password: &str,
        expected_user: Option<&str>,
    ) -> Result<(ValidatedTalosClaims, Authenticated), TalosError> {
        if !self.is_talos_key(password) {
            return Err(TalosError::NotTalosKey);
        }
        let token = self.inner.derive(password).await?;
        let validated = self.inner.validate(&token, expected_user).await?;
        self.inner.check_role_login(&validated.user).await?;
        Ok((validated, Authenticated))
    }
}

impl TalosAuthenticatorInner {
    /// Exchanges an opaque Talos key for a short-lived JWT via `apiKeys:derive`.
    async fn derive(&self, credential: &str) -> Result<String, TalosError> {
        let ttl = format!("{}s", self.derived_token_ttl.as_secs());
        let response = self
            .http_client
            .post(self.derive_url.clone())
            .timeout(HTTP_TIMEOUT)
            .bearer_auth(&self.derive_credential.0)
            .json(&serde_json::json!({
                "credential": credential,
                "algorithm": "TOKEN_ALGORITHM_JWT",
                "ttl": ttl,
            }))
            .send()
            .await
            .map_err(|e| TalosError::DeriveRequestFailed {
                error_message: e.to_string(),
            })?;

        // A rejected key (revoked/expired/unknown) is a client auth failure,
        // distinct from Talos being unreachable.
        let status = response.status();
        if status == reqwest::StatusCode::UNAUTHORIZED
            || status == reqwest::StatusCode::FORBIDDEN
            || status == reqwest::StatusCode::NOT_FOUND
            || status == reqwest::StatusCode::BAD_REQUEST
        {
            debug!(%status, "Talos rejected the presented key");
            return Err(TalosError::InvalidKey);
        }
        let response =
            response
                .error_for_status()
                .map_err(|e| TalosError::DeriveRequestFailed {
                    error_message: e.to_string(),
                })?;

        let derived: DeriveResponse =
            response
                .json()
                .await
                .map_err(|e| TalosError::DeriveRequestFailed {
                    error_message: e.to_string(),
                })?;
        Ok(derived.token.token)
    }

    /// Validates a derived JWT against the derived-keys JWKS and maps it to an
    /// identity.
    async fn validate(
        &self,
        token: &str,
        expected_user: Option<&str>,
    ) -> Result<ValidatedTalosClaims, TalosError> {
        let header = jsonwebtoken::decode_header(token).map_err(|e| {
            debug!("failed to decode derived token header: {e:?}");
            TalosError::Jwt
        })?;
        let kid = header.kid.ok_or(TalosError::MissingKid)?;
        let decoding_key = self.find_key(&kid).await?;
        let claims = validate_derived_token(token, &decoding_key)?;
        map_claims(claims, expected_user)
    }

    /// Finds the decoding key for `kid`, refetching the JWKS on a cache miss.
    async fn find_key(&self, kid: &str) -> Result<DecodingKey, TalosError> {
        {
            let keys = self.decoding_keys.lock().expect("lock poisoned");
            if let Some(key) = keys.get(kid) {
                return Ok(key.clone());
            }
        }

        let fresh = self.fetch_jwks().await?;
        let found = fresh.get(kid).cloned();
        {
            let mut keys = self.decoding_keys.lock().expect("lock poisoned");
            *keys = fresh;
        }
        found.ok_or_else(|| TalosError::NoMatchingKey {
            key_id: kid.to_string(),
        })
    }

    /// Fetches and parses the derived-keys JWKS.
    async fn fetch_jwks(&self) -> Result<BTreeMap<String, DecodingKey>, TalosError> {
        let url = self.jwks_url.clone();
        let response = self
            .http_client
            .get(url.clone())
            .timeout(HTTP_TIMEOUT)
            .send()
            .await
            .map_err(|e| TalosError::FetchJwksFailed {
                url: url.to_string(),
                error_message: e.to_string(),
            })?
            .error_for_status()
            .map_err(|e| TalosError::FetchJwksFailed {
                url: url.to_string(),
                error_message: e.to_string(),
            })?;
        let body = response
            .text()
            .await
            .map_err(|e| TalosError::FetchJwksFailed {
                url: url.to_string(),
                error_message: e.to_string(),
            })?;
        parse_derived_jwks(&body).map_err(|e| TalosError::FetchJwksFailed {
            url: url.to_string(),
            error_message: e,
        })
    }

    /// Checks whether the role has the LOGIN attribute. Mirrors the OIDC
    /// authenticator: an unknown role is auto-provisioned at startup, so it is
    /// allowed here.
    async fn check_role_login(&self, role_name: &str) -> Result<(), TalosError> {
        match self.adapter_client.role_can_login(role_name).await {
            Ok(()) => Ok(()),
            Err(AdapterError::AuthenticationError(AuthenticationError::RoleNotFound)) => Ok(()),
            Err(AdapterError::AuthenticationError(AuthenticationError::NonLogin)) => {
                Err(TalosError::NonLogin)
            }
            Err(e) => {
                warn!(?e, "unexpected error checking Talos role login");
                Err(TalosError::LoginCheckError)
            }
        }
    }
}

/// Parses the non-standard `{"jwks": {"keys": [...]}}` derived-keys envelope
/// into a kid -> decoding key map. Keys that fail to parse are skipped rather
/// than failing the whole set.
fn parse_derived_jwks(body: &str) -> Result<BTreeMap<String, DecodingKey>, String> {
    #[derive(Deserialize)]
    struct Envelope {
        jwks: JwkSet,
    }
    let envelope: Envelope = serde_json::from_str(body).map_err(|e| e.to_string())?;
    let mut keys = BTreeMap::new();
    for jwk in envelope.jwks.keys {
        let Some(kid) = jwk.common.key_id.clone() else {
            continue;
        };
        match DecodingKey::from_jwk(&jwk) {
            Ok(key) => {
                keys.insert(kid, key);
            }
            Err(e) => warn!("failed to parse derived-keys JWK {kid}: {e}"),
        }
    }
    Ok(keys)
}

/// Validates a derived token's signature and expiry against `decoding_key`.
///
/// The algorithm is taken from the token header (as the OIDC authenticator
/// does): the decoding key comes from the JWKS, so its key type fixes the
/// admissible algorithm family and Ed25519 validates with no special-casing.
fn validate_derived_token(
    token: &str,
    decoding_key: &DecodingKey,
) -> Result<TalosClaims, TalosError> {
    let header = jsonwebtoken::decode_header(token).map_err(|e| {
        debug!("failed to decode derived token header: {e:?}");
        TalosError::Jwt
    })?;
    let mut validation = jsonwebtoken::Validation::new(header.alg);
    // Derived tokens are minted by our own derive call, not a third party, so
    // there is no meaningful audience/issuer to pin; expiry is still enforced.
    validation.validate_aud = false;
    validation.required_spec_claims = ["exp"].into_iter().map(String::from).collect();

    let data = jsonwebtoken::decode::<TalosClaims>(token, decoding_key, &validation).map_err(
        |e| match e.kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => TalosError::ExpiredSignature,
            other => {
                debug!("derived token validation failed: {other:?}");
                TalosError::Jwt
            }
        },
    )?;
    Ok(data.claims)
}

/// Maps validated claims to an identity, enforcing that the login user matches
/// the key owner's email.
fn map_claims(
    claims: TalosClaims,
    expected_user: Option<&str>,
) -> Result<ValidatedTalosClaims, TalosError> {
    let user = claims.meta.email.ok_or(TalosError::MissingEmail)?;
    if let Some(expected) = expected_user {
        if expected != user {
            return Err(TalosError::WrongUser);
        }
    }
    Ok(ValidatedTalosClaims {
        user,
        actor_id: claims.act,
        organization_id: claims.meta.organization_id,
        key_id: claims.sub,
        _private: (),
    })
}

#[derive(Debug, Deserialize)]
struct DeriveResponse {
    token: DeriveToken,
}

#[derive(Debug, Deserialize)]
struct DeriveToken {
    token: String,
}

#[cfg(test)]
mod tests {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use serde_json::json;

    use super::*;

    /// A fixed Ed25519 keypair so tests can sign a derived token and validate
    /// it end-to-end without a live Talos or a keygen dependency. Generated
    /// once with the `cryptography` library; the JWK below is the public half.
    const TEST_ED25519_PKCS8_DER: &[u8] = &[
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20, 0x75, 0xc3, 0xe0, 0x9b, 0xf5, 0x7a, 0x4e, 0x39, 0x11, 0x6c, 0x53, 0x15, 0xe3, 0x89,
        0x24, 0xaa, 0x88, 0xe9, 0xbe, 0x17, 0x27, 0x1e, 0x3b, 0x43, 0x41, 0x56, 0x45, 0x06, 0x9b,
        0x32, 0x74, 0x16,
    ];
    const TEST_JWK_X: &str = "vnBhUCmNjKrAQ3c84khoNqGwC9AYusg8ZtLezZ-hts8";
    const TEST_KID: &str = "test-derived-key-1";

    fn derived_keys_jwks() -> String {
        json!({
            "jwks": {
                "keys": [{
                    "kty": "OKP",
                    "crv": "Ed25519",
                    "use": "sig",
                    "alg": "EdDSA",
                    "kid": TEST_KID,
                    "x": TEST_JWK_X,
                }]
            }
        })
        .to_string()
    }

    /// Sign a derived-token payload with the test key.
    fn sign(claims: serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(TEST_KID.to_string());
        let key = EncodingKey::from_ed_der(TEST_ED25519_PKCS8_DER);
        jsonwebtoken::encode(&header, &claims, &key).expect("sign derived token")
    }

    fn future_exp() -> i64 {
        // Comfortably in the future so default expiry validation passes.
        4_102_444_800 // 2100-01-01
    }

    #[mz_ore::test]
    fn parse_derived_jwks_unwraps_envelope() {
        let keys = parse_derived_jwks(&derived_keys_jwks()).expect("parse envelope");
        assert!(
            keys.contains_key(TEST_KID),
            "kid must be extracted from the jwks envelope"
        );
    }

    #[mz_ore::test]
    fn parse_bare_jwks_is_rejected() {
        // A bare RFC 7517 key set (no `jwks` envelope) must not parse — this is
        // the shape difference we are guarding against.
        let bare = json!({ "keys": [] }).to_string();
        assert!(parse_derived_jwks(&bare).is_err());
    }

    #[mz_ore::test]
    fn validate_and_map_full_identity() {
        let keys = parse_derived_jwks(&derived_keys_jwks()).unwrap();
        let key = keys.get(TEST_KID).unwrap();
        let token = sign(json!({
            "sub": "key-abc",
            "act": "actor-xyz",
            "exp": future_exp(),
            "meta": {
                "source": "console",
                "email": "user@example.com",
                "organization_id": "org-123",
            },
        }));
        let claims = validate_derived_token(&token, key).expect("validate derived token");
        let identity = map_claims(claims, Some("user@example.com")).expect("map claims");
        assert_eq!(identity.user, "user@example.com");
        assert_eq!(identity.actor_id.as_deref(), Some("actor-xyz"));
        assert_eq!(identity.organization_id.as_deref(), Some("org-123"));
        assert_eq!(identity.key_id, "key-abc");
    }

    #[mz_ore::test]
    fn expired_token_is_rejected() {
        let keys = parse_derived_jwks(&derived_keys_jwks()).unwrap();
        let key = keys.get(TEST_KID).unwrap();
        let token = sign(json!({
            "sub": "key-abc",
            "act": "actor-xyz",
            "exp": 1_000_000_000, // 2001, well in the past
            "meta": { "email": "user@example.com" },
        }));
        assert!(matches!(
            validate_derived_token(&token, key),
            Err(TalosError::ExpiredSignature)
        ));
    }

    #[mz_ore::test]
    fn wrong_signing_key_is_rejected() {
        // A token whose kid we know but signed by a different key must fail.
        let keys = parse_derived_jwks(&derived_keys_jwks()).unwrap();
        let key = keys.get(TEST_KID).unwrap();
        // Tamper with the payload after signing to break the signature.
        let token = sign(json!({ "sub": "k", "exp": future_exp(), "meta": {"email": "u@e.com"} }));
        let mut parts: Vec<&str> = token.split('.').collect();
        parts[1] = "eyJzdWIiOiJoYWNrZWQifQ"; // {"sub":"hacked"}
        let tampered = parts.join(".");
        assert!(matches!(
            validate_derived_token(&tampered, key),
            Err(TalosError::Jwt)
        ));
    }

    #[mz_ore::test]
    fn missing_email_is_rejected() {
        let claims = TalosClaims {
            sub: "key-abc".to_string(),
            act: Some("actor-xyz".to_string()),
            meta: TalosMeta::default(),
        };
        assert!(matches!(
            map_claims(claims, None),
            Err(TalosError::MissingEmail)
        ));
    }

    #[mz_ore::test]
    fn wrong_user_is_rejected() {
        let claims = TalosClaims {
            sub: "key-abc".to_string(),
            act: None,
            meta: TalosMeta {
                email: Some("owner@example.com".to_string()),
                organization_id: None,
            },
        };
        assert!(matches!(
            map_claims(claims, Some("someone-else@example.com")),
            Err(TalosError::WrongUser)
        ));
    }

    #[mz_ore::test]
    fn derive_credential_debug_is_redacted() {
        let cred = DeriveCredential::new("super-secret-derive-key".to_string());
        let rendered = format!("{cred:?}");
        assert!(
            !rendered.contains("super-secret"),
            "credential must not leak in Debug: {rendered}"
        );
    }
}
