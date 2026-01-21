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

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use jsonwebtoken::{DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use mz_authenticator_types::{OidcAuthSessionHandle, OidcAuthenticator};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Deserializer, Serialize};

use tracing::warn;
use url::Url;

/// Command line arguments for OIDC authentication.
#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// OIDC issuer URL (e.g., `https://accounts.google.com`).
    /// This is validated against the `iss` claim in the JWT.
    pub oidc_issuer: String,
    /// Optional OIDC audience (client ID).
    /// If set, validates that the JWT's `aud` claim contains this value.
    pub oidc_audience: Option<String>,
}

/// Errors that can occur during OIDC authentication.
#[derive(Debug)]
pub enum OidcError {
    /// Failed to parse OIDC configuration URL.
    InvalidIssuerUrl(url::ParseError),
    /// Failed to fetch OpenID configuration from provider.
    OpenIdConfigFetchFailed(String),
    /// Failed to fetch JWKS from provider.
    JwksFetchFailed(String),
    /// The key ID is missing in the token header.
    MissingKid,
    /// No matching key found in JWKS.
    NoMatchingKey,
    /// JWT validation error from jsonwebtoken.
    Jwt(jsonwebtoken::errors::Error),
    /// User does not match expected value.
    WrongUser,
}

impl std::fmt::Display for OidcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OidcError::InvalidIssuerUrl(e) => {
                write!(f, "failed to parse OIDC issuer URL: {}", e)
            }
            OidcError::OpenIdConfigFetchFailed(e) => {
                write!(f, "failed to fetch OpenID configuration: {}", e)
            }
            OidcError::JwksFetchFailed(e) => write!(f, "failed to fetch JWKS: {}", e),
            OidcError::MissingKid => write!(f, "missing key ID in token header"),
            OidcError::NoMatchingKey => write!(f, "no matching key in JWKS"),
            OidcError::Jwt(e) => write!(f, "JWT error: {}", e),
            OidcError::WrongUser => write!(f, "user does not match expected value"),
        }
    }
}

impl std::error::Error for OidcError {}

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
    /// Subject (user identifier).
    pub sub: String,
    /// Issuer.
    pub iss: String,
    /// Expiration time (Unix timestamp).
    pub exp: i64,
    /// Issued at time (Unix timestamp).
    #[serde(default)]
    pub iat: Option<i64>,
    /// Email claim (commonly used for username).
    #[serde(default)]
    pub email: Option<String>,
    /// Audience claim (can be single string or array in JWT).
    #[serde(default, deserialize_with = "deserialize_string_or_vec")]
    pub aud: Vec<String>,
}

impl OidcClaims {
    /// Extract the username to use for the session.
    ///
    /// Priority: email > sub
    // TODO (SangJunBak): Add a configuration variable to use a different username field.
    pub fn username(&self) -> &str {
        self.email.as_deref().unwrap_or(&self.sub)
    }
}

#[derive(Clone)]
struct OidcDecodingKey(DecodingKey);

impl std::fmt::Debug for OidcDecodingKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JWKS").field("key", &"<redacted>").finish()
    }
}

/// Session handle for generic OIDC authentication.
#[derive(Debug)]
pub struct GenericOidcSessionHandle {
    user: String,
}

#[async_trait]
impl OidcAuthSessionHandle for GenericOidcSessionHandle {
    fn user(&self) -> &str {
        &self.user
    }

    async fn expired(&mut self) {
        // This session never expires - wait forever
        // TODO (SangJunBak): Implement expiration.
        std::future::pending().await
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
    issuer: String,
    audience: Option<String>,
    decoding_keys: Mutex<BTreeMap<String, OidcDecodingKey>>,
    http_client: HttpClient,
}

impl GenericOidcAuthenticator {
    /// Create a new [`GenericOidcAuthenticator`] from [`OidcConfig`].
    pub fn new(config: OidcConfig) -> Result<Self, OidcError> {
        let http_client = HttpClient::new();

        Ok(Self {
            inner: Arc::new(GenericOidcAuthenticatorInner {
                issuer: config.oidc_issuer,
                audience: config.oidc_audience,
                decoding_keys: Mutex::new(BTreeMap::new()),
                http_client,
            }),
        })
    }
}

impl GenericOidcAuthenticatorInner {
    async fn fetch_jwks_uri(&self) -> Result<String, OidcError> {
        let openid_config_url = Url::parse(&self.issuer)
            .and_then(|url| url.join(".well-known/openid-configuration"))
            .map_err(OidcError::InvalidIssuerUrl)?;

        // Fetch OpenID configuration to get the JWKS URI
        let response = self
            .http_client
            .get(openid_config_url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| OidcError::OpenIdConfigFetchFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(OidcError::OpenIdConfigFetchFailed(format!(
                "HTTP {}",
                response.status()
            )));
        }

        let openid_config: OpenIdConfiguration = response
            .json()
            .await
            .map_err(|e| OidcError::OpenIdConfigFetchFailed(e.to_string()))?;

        Ok(openid_config.jwks_uri)
    }
    /// Fetch JWKS from the provider and parse into a map of key IDs to decoding keys.
    async fn fetch_jwks(&self) -> Result<BTreeMap<String, OidcDecodingKey>, OidcError> {
        let jwks_uri = self.fetch_jwks_uri().await?;
        let response = self
            .http_client
            .get(&jwks_uri)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| OidcError::JwksFetchFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(OidcError::JwksFetchFailed(format!(
                "HTTP {}",
                response.status()
            )));
        }

        let jwks: JwkSet = response
            .json()
            .await
            .map_err(|e| OidcError::JwksFetchFailed(e.to_string()))?;

        let mut keys = BTreeMap::new();
        for jwk in jwks.keys {
            match DecodingKey::from_jwk(&jwk) {
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

        if keys.is_empty() {
            return Err(OidcError::JwksFetchFailed(
                "no valid keys found in JWKS".to_string(),
            ));
        }

        Ok(keys)
    }

    /// Find a decoding key matching the given key ID.
    /// If the key is not found, fetch the JWKS and cache the keys.
    async fn find_key(&self, kid: &String) -> Result<OidcDecodingKey, OidcError> {
        {
            let mut decoding_keys = self.decoding_keys.lock().expect("lock poisoned");

            if let Some(key) = decoding_keys.get_mut(kid) {
                return Ok(key.clone());
            }
        }

        let new_decoding_keys = self.fetch_jwks().await?;

        let decoding_key = new_decoding_keys.get(kid).cloned();

        {
            let mut decoding_keys = self.decoding_keys.lock().expect("lock poisoned");
            decoding_keys.extend(new_decoding_keys);
        }

        if let Some(key) = decoding_key {
            return Ok(key);
        }

        Err(OidcError::NoMatchingKey)
    }

    async fn validate_access_token(
        &self,
        token: &str,
        expected_user: Option<&str>,
    ) -> Result<OidcClaims, OidcError> {
        // Decode header to get key ID (kid) and algorithm
        let header = decode_header(token).map_err(OidcError::Jwt)?;

        let kid = header.kid.ok_or(OidcError::MissingKid)?;
        // Find matching key from cached keys
        let decoding_key = self.find_key(&kid).await?;

        // Set up validation
        // TODO (SangJunBak): Make JWT expiration configurable.
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.issuer]);
        if let Some(ref audience) = self.audience {
            validation.set_audience(&[audience]);
        } else {
            validation.validate_aud = false;
        }

        // Decode and validate the token
        let token_data =
            decode::<OidcClaims>(token, &(decoding_key.0), &validation).map_err(OidcError::Jwt)?;

        // Optionally validate expected user
        if let Some(expected) = expected_user {
            if token_data.claims.username() != expected {
                return Err(OidcError::WrongUser);
            }
        }

        Ok(token_data.claims)
    }
}

#[async_trait]
impl OidcAuthenticator for GenericOidcAuthenticator {
    type Error = OidcError;
    type SessionHandle = GenericOidcSessionHandle;
    type ValidatedClaims = OidcClaims;

    async fn authenticate(
        &self,
        expected_user: &str,
        password: &str,
    ) -> Result<Self::SessionHandle, Self::Error> {
        // The password is the JWT token
        let claims = self
            .validate_access_token(password, Some(expected_user))
            .await?;

        Ok(GenericOidcSessionHandle {
            user: claims.username().to_string(),
        })
    }

    async fn validate_access_token(
        &self,
        token: &str,
        expected_user: Option<&str>,
    ) -> Result<OidcClaims, OidcError> {
        self.inner.validate_access_token(token, expected_user).await
    }
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
}
