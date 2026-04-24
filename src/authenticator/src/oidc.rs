// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! OIDC Authentication for pgwire connections.
//!
//! This module provides JWT-based authentication using OpenID Connect (OIDC).
//! JWTs are validated locally using JWKS fetched from the configured provider.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use jsonwebtoken::jwk::JwkSet;
use mz_adapter::{AdapterError, AuthenticationError, Client as AdapterClient};
use mz_adapter_types::dyncfgs::{
    OIDC_AUDIENCE, OIDC_AUTHENTICATION_CLAIM, OIDC_GROUP_CLAIM, OIDC_ISSUER,
};
use mz_auth::Authenticated;
use mz_ore::secure::{Zeroize, ZeroizeOnDrop};
use mz_ore::soft_panic_or_log;
use mz_pgwire_common::{ErrorResponse, Severity};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Deserializer, Serialize};
use tokio_postgres::error::SqlState;

use tracing::{debug, warn};
use url::Url;
/// Errors that can occur during OIDC authentication.
#[derive(Debug)]
pub enum OidcError {
    MissingIssuer,
    /// Failed to parse OIDC configuration URL.
    InvalidIssuerUrl(String),
    AudienceParseError,
    /// Failed to fetch from the identity provider.
    FetchFromProviderFailed {
        url: String,
        error_message: String,
    },
    /// The key ID is missing in the token header.
    MissingKid,
    /// No matching key found in JWKS.
    NoMatchingKey {
        /// Key ID that was found in the JWT header.
        key_id: String,
    },
    /// Configured authentication claim is not found in the JWT.
    NoMatchingAuthenticationClaim {
        authentication_claim: String,
    },
    /// JWT validation error
    Jwt,
    WrongUser,
    InvalidAudience {
        expected_audiences: Vec<String>,
    },
    InvalidIssuer {
        expected_issuer: String,
    },
    ExpiredSignature,
    /// The role exists but does not have the LOGIN attribute.
    NonLogin,
    LoginCheckError,
}

impl std::fmt::Display for OidcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OidcError::MissingIssuer => write!(f, "OIDC issuer is not configured"),
            OidcError::InvalidIssuerUrl(_) => write!(f, "invalid OIDC issuer URL"),
            OidcError::AudienceParseError => {
                write!(f, "failed to parse OIDC_AUDIENCE system variable")
            }
            OidcError::FetchFromProviderFailed { .. } => {
                write!(f, "failed to fetch OIDC provider configuration")
            }
            OidcError::MissingKid => write!(f, "missing key ID in JWT header"),
            OidcError::NoMatchingKey { .. } => write!(f, "no matching key found in the JWKS"),
            OidcError::NoMatchingAuthenticationClaim { .. } => {
                write!(f, "no matching authentication claim found in the JWT")
            }
            OidcError::Jwt => write!(f, "failed to validate JWT"),
            OidcError::WrongUser => write!(f, "wrong user"),
            OidcError::InvalidAudience { .. } => write!(f, "invalid audience"),
            OidcError::InvalidIssuer { .. } => write!(f, "invalid issuer"),
            OidcError::ExpiredSignature => write!(f, "authentication credentials have expired"),
            OidcError::NonLogin => write!(f, "role is not allowed to login"),
            OidcError::LoginCheckError => write!(f, "unexpected error checking if role can login"),
        }
    }
}

impl std::error::Error for OidcError {}

impl OidcError {
    pub fn code(&self) -> SqlState {
        SqlState::INVALID_AUTHORIZATION_SPECIFICATION
    }

    pub fn detail(&self) -> Option<String> {
        match self {
            OidcError::InvalidIssuerUrl(issuer) => {
                Some(format!("Could not parse \"{issuer}\" as a URL."))
            }
            OidcError::FetchFromProviderFailed { url, error_message } => {
                Some(format!("Fetching \"{url}\" failed. {error_message}"))
            }
            OidcError::NoMatchingKey { key_id } => {
                Some(format!("JWT key ID \"{key_id}\" was not found."))
            }
            OidcError::InvalidAudience { expected_audiences } => Some(format!(
                "Expected one of audiences {:?} in the JWT.",
                expected_audiences,
            )),
            OidcError::InvalidIssuer { expected_issuer } => {
                Some(format!("Expected issuer \"{expected_issuer}\" in the JWT.",))
            }
            OidcError::NoMatchingAuthenticationClaim {
                authentication_claim,
            } => Some(format!(
                "Expected authentication claim \"{authentication_claim}\" in the JWT.",
            )),
            OidcError::NonLogin => Some("The role does not have the LOGIN attribute.".into()),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            OidcError::MissingIssuer => {
                Some("Configure the OIDC issuer using the oidc_issuer system variable.".into())
            }
            _ => None,
        }
    }

    pub fn into_response(self) -> ErrorResponse {
        ErrorResponse {
            severity: Severity::Fatal,
            code: self.code(),
            message: self.to_string(),
            detail: self.detail(),
            hint: self.hint(),
            position: None,
        }
    }
}

fn deserialize_string_or_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrVec {
        String(String),
        Vec(Vec<String>),
    }

    match StringOrVec::deserialize(deserializer)? {
        StringOrVec::String(s) => Ok(vec![s]),
        StringOrVec::Vec(v) => Ok(v),
    }
}
/// Claims extracted from a validated JWT.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OidcClaims {
    /// Issuer.
    pub iss: String,
    /// Expiration time (Unix timestamp).
    pub exp: i64,
    /// Issued at time (Unix timestamp).
    #[serde(default)]
    pub iat: Option<i64>,
    /// Audience claim (can be single string or array in JWT).
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub aud: Vec<String>,
    /// Additional claims from the JWT, captured for flexible username extraction.
    #[serde(flatten)]
    pub unknown_claims: BTreeMap<String, serde_json::Value>,
}

impl Zeroize for OidcClaims {
    fn zeroize(&mut self) {
        self.iss.zeroize();
        self.exp.zeroize();
        self.iat.zeroize();
        for s in &mut self.aud {
            s.zeroize();
        }
        self.aud.clear();
        // serde_json::Value doesn't implement Zeroize; drain entries and
        // zeroize keys/values before the backing allocations are freed.
        while let Some((mut k, mut v)) = self.unknown_claims.pop_first() {
            k.zeroize();
            zeroize_json_value(&mut v);
        }
    }
}

impl Drop for OidcClaims {
    fn drop(&mut self) {
        self.zeroize();
    }
}

/// `OidcClaims` implements both `Zeroize` and `Drop` (which calls `zeroize()`),
/// satisfying the `ZeroizeOnDrop` contract.
impl ZeroizeOnDrop for OidcClaims {}

fn zeroize_json_value(v: &mut serde_json::Value) {
    use serde_json::Value;
    match v {
        Value::String(s) => s.zeroize(),
        Value::Array(a) => {
            for item in a.iter_mut() {
                zeroize_json_value(item);
            }
            a.clear();
        }
        Value::Object(map) => {
            let taken = std::mem::take(map);
            for (mut k, mut nested) in taken {
                k.zeroize();
                zeroize_json_value(&mut nested);
            }
        }
        Value::Number(_) => {
            *v = Value::Number(serde_json::Number::from(0u8));
        }
        Value::Bool(b) => *b = false,
        Value::Null => {}
    }
}

impl OidcClaims {
    /// Extract the username from the OIDC claims.
    fn user(&self, authentication_claim: &str) -> Option<&str> {
        self.unknown_claims
            .get(authentication_claim)
            .and_then(|value| value.as_str())
    }

    /// Extracts group names from the specified JWT claim for group-to-role sync.
    ///
    /// Returns `None` if the claim is absent (skip sync, preserve current state),
    /// `Some(vec![])` if the claim is present but empty (revoke all sync-granted
    /// roles), or `Some(vec![...])` with normalized (lowercased, deduplicated,
    /// sorted) group names.
    ///
    /// Accepts arrays of strings, single strings, or mixed arrays (non-string
    /// elements are filtered out). Other JSON types are treated as absent.
    pub fn groups(&self, claim_name: &str) -> Option<Vec<String>> {
        let value = self.unknown_claims.get(claim_name)?;

        let raw_groups: Vec<String> = match value {
            // Most IdPs send groups as a JSON array of strings.
            // Non-string elements (e.g., numbers injected by misconfigured
            // claim mappings) are silently dropped rather than failing the
            // entire sync.
            serde_json::Value::Array(arr) => arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect(),
            // Some IdPs represent a single group membership as a plain string
            // rather than a one-element array. An empty string means
            // "no groups" (equivalent to an empty array).
            serde_json::Value::String(s) => {
                if s.is_empty() {
                    vec![]
                } else {
                    vec![s.clone()]
                }
            }
            // Any other JSON type can't represent group names — treat as
            // if the claim were absent so we don't accidentally revoke all
            // roles based on garbage data.
            _ => return None,
        };

        // Normalize: lowercase for case-insensitive role matching,
        // filter out empty strings (not valid role names), deduplicate
        // via BTreeSet (which also sorts), then collect into a Vec for
        // the caller. Sorted order makes downstream diff computation
        // deterministic.
        let normalized: Vec<String> = raw_groups
            .into_iter()
            .map(|g| g.to_lowercase())
            .filter(|g| !g.is_empty())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();

        Some(normalized)
    }
}

#[derive(Zeroize, ZeroizeOnDrop)]
pub struct ValidatedClaims {
    pub user: String,
    /// Groups extracted from the JWT group claim. None if claim absent.
    #[zeroize(skip)]
    pub groups: Option<Vec<String>>,
    // Prevent construction outside of `GenericOidcAuthenticator::validate_token`.
    _private: (),
}

/// Wrapper around `jsonwebtoken::DecodingKey` with a redacted `Debug` impl.
#[derive(Clone)]
struct OidcDecodingKey(jsonwebtoken::DecodingKey);

impl std::fmt::Debug for OidcDecodingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcDecodingKey")
            .field("key", &"<redacted>")
            .finish()
    }
}

/// OIDC Authenticator that validates JWTs using JWKS.
///
/// This implementation pre-fetches JWKS at construction time for synchronous
/// token validation.
#[derive(Clone, Debug)]
pub struct GenericOidcAuthenticator {
    inner: Arc<GenericOidcAuthenticatorInner>,
}

/// OpenID Connect Discovery document.
/// See: <https://openid.net/specs/openid-connect-discovery-1_0.html>
#[derive(Debug, Deserialize)]
struct OpenIdConfiguration {
    /// URL of the JWKS endpoint.
    jwks_uri: String,
}

#[derive(Debug)]
pub struct GenericOidcAuthenticatorInner {
    adapter_client: AdapterClient,
    decoding_keys: Mutex<BTreeMap<String, OidcDecodingKey>>,
    http_client: HttpClient,
}

impl GenericOidcAuthenticator {
    /// Create a new [`GenericOidcAuthenticator`] with an [`AdapterClient`].
    ///
    /// The OIDC issuer and audience are fetched from system variables on each
    /// authentication attempt.
    pub fn new(adapter_client: AdapterClient) -> Self {
        let http_client = HttpClient::new();

        Self {
            inner: Arc::new(GenericOidcAuthenticatorInner {
                adapter_client,
                decoding_keys: Mutex::new(BTreeMap::new()),
                http_client,
            }),
        }
    }
}

impl GenericOidcAuthenticatorInner {
    async fn fetch_jwks_uri(&self, issuer: &str) -> Result<String, OidcError> {
        let openid_config_url = build_openid_config_url(issuer)?;

        let openid_config_url_str = openid_config_url.to_string();

        // Fetch OpenID configuration to get the JWKS URI
        let response = self
            .http_client
            .get(openid_config_url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| OidcError::FetchFromProviderFailed {
                url: openid_config_url_str.clone(),
                error_message: e.to_string(),
            })?;

        if !response.status().is_success() {
            return Err(OidcError::FetchFromProviderFailed {
                url: openid_config_url_str.clone(),
                error_message: response
                    .error_for_status()
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "Unknown error".to_string()),
            });
        }

        let openid_config: OpenIdConfiguration =
            response
                .json()
                .await
                .map_err(|e| OidcError::FetchFromProviderFailed {
                    url: openid_config_url_str,
                    error_message: e.to_string(),
                })?;

        Ok(openid_config.jwks_uri)
    }

    /// Fetch JWKS from the provider and parse into a map of key IDs to decoding keys.
    async fn fetch_jwks(
        &self,
        issuer: &str,
    ) -> Result<BTreeMap<String, OidcDecodingKey>, OidcError> {
        let jwks_uri = self.fetch_jwks_uri(issuer).await?;
        let response = self
            .http_client
            .get(&jwks_uri)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| OidcError::FetchFromProviderFailed {
                url: jwks_uri.clone(),
                error_message: e.to_string(),
            })?;

        if !response.status().is_success() {
            return Err(OidcError::FetchFromProviderFailed {
                url: jwks_uri.clone(),
                error_message: response
                    .error_for_status()
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "Unknown error".to_string()),
            });
        }

        let jwks: JwkSet =
            response
                .json()
                .await
                .map_err(|e| OidcError::FetchFromProviderFailed {
                    url: jwks_uri.clone(),
                    error_message: e.to_string(),
                })?;

        let mut keys = BTreeMap::new();

        for jwk in jwks.keys {
            match jsonwebtoken::DecodingKey::from_jwk(&jwk) {
                Ok(key) => {
                    if let Some(kid) = jwk.common.key_id {
                        keys.insert(kid, OidcDecodingKey(key));
                    }
                }
                Err(e) => {
                    warn!("Failed to parse JWK: {}", e);
                }
            }
        }

        Ok(keys)
    }

    /// Find a decoding key matching the given key ID.
    /// If the key is not found, fetch the JWKS and cache the keys.
    async fn find_key(&self, kid: &str, issuer: &str) -> Result<OidcDecodingKey, OidcError> {
        // Get the cached decoding key.
        {
            let decoding_keys = self.decoding_keys.lock().expect("lock poisoned");

            if let Some(key) = decoding_keys.get(kid) {
                return Ok(key.clone());
            }
        }

        // If not found, fetch the JWKS and cache the keys.
        let new_decoding_keys = self.fetch_jwks(issuer).await?;

        let decoding_key = new_decoding_keys.get(kid).cloned();

        {
            let mut decoding_keys = self.decoding_keys.lock().expect("lock poisoned");
            *decoding_keys = new_decoding_keys;
        }

        if let Some(key) = decoding_key {
            return Ok(key);
        }

        {
            let decoding_keys = self.decoding_keys.lock().expect("lock poisoned");
            debug!(
                "No matching key found in JWKS for key ID: {kid}. Available keys: {decoding_keys:?}."
            );
            Err(OidcError::NoMatchingKey {
                key_id: kid.to_string(),
            })
        }
    }

    pub async fn validate_token(
        &self,
        token: &str,
        expected_user: Option<&str>,
    ) -> Result<ValidatedClaims, OidcError> {
        // Fetch current OIDC configuration from system variables
        let system_vars = self.adapter_client.get_system_vars().await;
        let Some(issuer) = OIDC_ISSUER.get(system_vars.dyncfgs()) else {
            return Err(OidcError::MissingIssuer);
        };

        let authentication_claim = OIDC_AUTHENTICATION_CLAIM.get(system_vars.dyncfgs());

        let expected_audiences: Vec<String> = {
            let audiences: Vec<String> =
                serde_json::from_value(OIDC_AUDIENCE.get(system_vars.dyncfgs()))
                    .map_err(|_| OidcError::AudienceParseError)?;

            if audiences.is_empty() {
                warn!(
                    "Audience validation skipped. It is discouraged \
                    to skip audience validation since it allows anyone \
                    with a JWT issued by the same issuer to authenticate."
                );
            }
            audiences
        };

        // Decode header to get key ID (kid) and the
        // decoding algorithm
        let header = jsonwebtoken::decode_header(token).map_err(|e| {
            debug!("Failed to decode JWT header: {:?}", e);
            OidcError::Jwt
        })?;

        let kid = header.kid.ok_or(OidcError::MissingKid)?;
        // Find the matching key from our set of cached keys. If not found,
        // fetch the JWKS from the provider and cache the keys
        let decoding_key = self.find_key(&kid, &issuer).await?;

        // Set up audience and issuer validation
        let mut validation = jsonwebtoken::Validation::new(header.alg);
        validation.set_issuer(&[&issuer]);
        if !expected_audiences.is_empty() {
            validation.set_audience(&expected_audiences);
        } else {
            validation.validate_aud = false;
        }

        // Decode and validate the token
        let token_data = jsonwebtoken::decode::<OidcClaims>(token, &(decoding_key.0), &validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::InvalidAudience => {
                    if !expected_audiences.is_empty() {
                        OidcError::InvalidAudience {
                            expected_audiences
                        }
                    } else {
                        soft_panic_or_log!(
                            "received an audience validation error when audience validation is disabled"
                        );
                        OidcError::Jwt
                    }
                }
                jsonwebtoken::errors::ErrorKind::InvalidIssuer => OidcError::InvalidIssuer {
                    expected_issuer: issuer.clone(),
                },
                jsonwebtoken::errors::ErrorKind::ExpiredSignature => OidcError::ExpiredSignature,
                _ => OidcError::Jwt,
            })?;

        let user = token_data.claims.user(&authentication_claim).ok_or(
            OidcError::NoMatchingAuthenticationClaim {
                authentication_claim,
            },
        )?;

        // Optionally validate the expected user
        if let Some(expected) = expected_user {
            if user != expected {
                return Err(OidcError::WrongUser);
            }
        }

        // Extract groups from the configured claim name for group-to-role sync.
        let group_claim = OIDC_GROUP_CLAIM.get(system_vars.dyncfgs());
        let groups = token_data.claims.groups(&group_claim);

        Ok(ValidatedClaims {
            user: user.to_string(),
            groups,
            _private: (),
        })
    }

    /// Checks whether the role has the LOGIN attribute. This is needed otherwise
    /// a user can authenticate with an OIDC token to a role that isn't recognized
    /// as a user.
    async fn check_role_login(&self, role_name: &str) -> Result<(), OidcError> {
        match self.adapter_client.role_can_login(role_name).await {
            Ok(()) => Ok(()),
            Err(AdapterError::AuthenticationError(AuthenticationError::RoleNotFound)) => {
                // Role will be auto-provisioned during startup; allow login.
                Ok(())
            }
            Err(AdapterError::AuthenticationError(AuthenticationError::NonLogin)) => {
                Err(OidcError::NonLogin)
            }
            Err(e) => {
                warn!(?e, "unexpected error checking OIDC role login");
                Err(OidcError::LoginCheckError)
            }
        }
    }
}

impl GenericOidcAuthenticator {
    pub async fn authenticate(
        &self,
        token: &str,
        expected_user: Option<&str>,
    ) -> Result<(ValidatedClaims, Authenticated), OidcError> {
        let validated_claims = self.inner.validate_token(token, expected_user).await?;
        self.inner.check_role_login(&validated_claims.user).await?;
        Ok((validated_claims, Authenticated))
    }
}

fn build_openid_config_url(issuer: &str) -> Result<Url, OidcError> {
    let mut openid_config_url =
        Url::parse(issuer).map_err(|_| OidcError::InvalidIssuerUrl(issuer.to_string()))?;
    {
        let mut segments = openid_config_url
            .path_segments_mut()
            .map_err(|_| OidcError::InvalidIssuerUrl(issuer.to_string()))?;
        // Remove trailing slash if it exists
        segments.pop_if_empty();
        segments.push(".well-known");
        segments.push("openid-configuration");
    }
    Ok(openid_config_url)
}
#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_aud_single_string() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"my-app"}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.aud, vec!["my-app"]);
    }

    #[mz_ore::test]
    fn test_aud_array() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":["app1","app2"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.aud, vec!["app1", "app2"]);
    }

    #[mz_ore::test]
    fn test_user() {
        let json = r#"{"sub":"user-123","iss":"issuer","exp":1234,"aud":["app"],"email":"alice@example.com"}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.user("sub"), Some("user-123"));
        assert_eq!(claims.user("email"), Some("alice@example.com"));
        assert_eq!(claims.user("missing"), None);
    }

    #[mz_ore::test]
    fn test_build_openid_config_url() {
        let issuer = "https://dev-123456.okta.com/oauth2/default";
        let url = build_openid_config_url(issuer).unwrap();
        assert_eq!(
            url.to_string(),
            "https://dev-123456.okta.com/oauth2/default/.well-known/openid-configuration"
        );
    }

    #[mz_ore::test]
    fn test_build_openid_config_url_trailing_slash() {
        let issuer = "https://dev-123456.okta.com/oauth2/default/";
        let url = build_openid_config_url(issuer).unwrap();
        assert_eq!(
            url.to_string(),
            "https://dev-123456.okta.com/oauth2/default/.well-known/openid-configuration"
        );
    }

    #[mz_ore::test]
    fn zeroize_clears_validated_claims() {
        use mz_ore::secure::Zeroize;
        let mut claims = ValidatedClaims {
            user: "alice@example.com".to_string(),
            groups: Some(vec!["eng".to_string()]),
            _private: (),
        };
        claims.zeroize();
        assert!(claims.user.is_empty());
    }

    #[mz_ore::test]
    fn oidc_claims_implements_zeroize_on_drop() {
        fn assert_zod<T: ZeroizeOnDrop>() {}
        assert_zod::<OidcClaims>();
        assert_zod::<ValidatedClaims>();
    }

    #[mz_ore::test]
    fn zeroize_clears_oidc_claims() {
        use mz_ore::secure::Zeroize;
        let mut claims = OidcClaims {
            iss: "https://issuer.example.com".to_string(),
            exp: 1234567890,
            iat: Some(1234567800),
            aud: vec!["app1".to_string(), "app2".to_string()],
            unknown_claims: BTreeMap::from([(
                "email".to_string(),
                serde_json::Value::String("alice@example.com".to_string()),
            )]),
        };
        claims.zeroize();
        assert!(claims.iss.is_empty());
        assert_eq!(claims.exp, 0);
        assert!(claims.iat.is_none());
        assert!(claims.aud.is_empty());
        assert!(claims.unknown_claims.is_empty());
    }

    #[mz_ore::test]
    fn test_groups_array() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["analytics","platform_eng"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["analytics".to_string(), "platform_eng".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_single_string() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":"analytics"}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["analytics".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_missing() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app"}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_empty_array() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":[]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), Some(vec![]));
    }

    #[mz_ore::test]
    fn test_groups_mixed_case() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["Analytics","PLATFORM_ENG","analytics"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["analytics".to_string(), "platform_eng".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_duplicates() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["a","b","a","c","b"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["a".to_string(), "b".to_string(), "c".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_custom_claim_name() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","roles":["admin","viewer"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("roles"),
            Some(vec!["admin".to_string(), "viewer".to_string()])
        );
        assert_eq!(claims.groups("groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_non_string_values_in_array() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["valid",123,true,"also_valid"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["also_valid".to_string(), "valid".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_non_array_non_string() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":42}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_empty_string() {
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":""}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), Some(vec![]));
    }

    #[mz_ore::test]
    fn test_groups_null_claim() {
        // Explicit null → treated as absent (not a valid group representation)
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":null}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_boolean_claim() {
        // Boolean value → not array or string, treated as absent
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":true}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_object_claim() {
        // JSON object → not array or string, treated as absent
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":{"team":"eng"}}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_array_all_non_strings() {
        // Array with zero valid string elements → Some(vec![]), not None
        // (the claim *is* present, it just has no usable group names)
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":[1,2,true,null]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), Some(vec![]));
    }

    #[mz_ore::test]
    fn test_groups_array_with_nested_arrays() {
        // Nested arrays are not strings, so they're filtered out
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":[["nested"],"valid"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["valid".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_array_with_empty_strings() {
        // Empty strings are not valid role names and are filtered out,
        // consistent with the single-string case where "" → Some(vec![]).
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["","eng",""]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["eng".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_whitespace_names() {
        // Whitespace is preserved (not trimmed) — role names with spaces
        // would need to be quoted in SQL. Lowercasing doesn't affect spaces.
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["  spaces  ","eng"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["  spaces  ".to_string(), "eng".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_unicode_names() {
        // Unicode group names should be lowercased correctly
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["Développeurs","INGÉNIEURS"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["développeurs".to_string(), "ingénieurs".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_special_characters() {
        // Group names with special characters (hyphens, underscores, dots)
        // are common in enterprise IdPs like Azure AD / Okta
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["team-platform.eng","org_data-science","role/admin"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec![
                "org_data-science".to_string(),
                "role/admin".to_string(),
                "team-platform.eng".to_string(),
            ])
        );
    }

    #[mz_ore::test]
    fn test_groups_case_insensitive_dedup() {
        // "Eng" and "eng" and "ENG" should all collapse to one "eng"
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["Eng","eng","ENG","eNg"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), Some(vec!["eng".to_string()]));
    }

    #[mz_ore::test]
    fn test_groups_large_array() {
        // Verify we handle a reasonably large group list without issues
        let groups: Vec<String> = (0..100).map(|i| format!("\"group_{}\"", i)).collect();
        let json = format!(
            r#"{{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":[{}]}}"#,
            groups.join(",")
        );
        let claims: OidcClaims = serde_json::from_str(&json).unwrap();
        let result = claims.groups("groups").unwrap();
        assert_eq!(result.len(), 100);
        // BTreeSet ensures sorted order
        assert_eq!(result[0], "group_0");
        assert_eq!(result[99], "group_99");
    }

    #[mz_ore::test]
    fn test_groups_float_claim() {
        // Float value → not array or string, treated as absent
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":3.14}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.groups("groups"), None);
    }

    #[mz_ore::test]
    fn test_groups_array_with_null_elements() {
        // Null elements in array are not strings, filtered out
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["eng",null,"ops",null]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["eng".to_string(), "ops".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_array_with_object_elements() {
        // Object elements in array are not strings, filtered out
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["eng",{"name":"ops"},"analytics"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec!["analytics".to_string(), "eng".to_string()])
        );
    }

    #[mz_ore::test]
    fn test_groups_sorted_output() {
        // Verify output is sorted alphabetically regardless of input order
        let json = r#"{"sub":"user","iss":"issuer","exp":1234,"aud":"app","groups":["zebra","alpha","mango","beta"]}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(
            claims.groups("groups"),
            Some(vec![
                "alpha".to_string(),
                "beta".to_string(),
                "mango".to_string(),
                "zebra".to_string(),
            ])
        );
    }
}
