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

use jsonwebtoken::jwk::JwkSet;
use mz_adapter::Client as AdapterClient;
use mz_adapter_types::dyncfgs::{OIDC_AUDIENCE, OIDC_ISSUER};
use mz_auth::Authenticated;
use reqwest::Client as HttpClient;
use serde::{Deserialize, Deserializer, Serialize};

use tracing::warn;
use url::Url;
/// Errors that can occur during OIDC authentication.
#[derive(Debug)]
pub enum OidcError {
    /// The issuer is missing.
    MissingIssuer,
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
            OidcError::MissingIssuer => write!(f, "missing OIDC issuer URL"),
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
    // TODO (Oidc): Add a configuration variable to use a different username field.
    pub fn username(&self) -> &str {
        self.email.as_deref().unwrap_or(&self.sub)
    }
}

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
        let openid_config_url = Url::parse(issuer)
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

        if keys.is_empty() {
            return Err(OidcError::JwksFetchFailed(
                "no valid keys found in JWKS".to_string(),
            ));
        }

        Ok(keys)
    }

    /// Find a decoding key matching the given key ID.
    /// If the key is not found, fetch the JWKS and cache the keys.
    async fn find_key(&self, kid: &str, issuer: &str) -> Result<OidcDecodingKey, OidcError> {
        {
            let decoding_keys = self.decoding_keys.lock().expect("lock poisoned");

            if let Some(key) = decoding_keys.get(kid) {
                return Ok(key.clone());
            }
        }

        let new_decoding_keys = self.fetch_jwks(issuer).await?;

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

    pub async fn validate_token(
        &self,
        token: &str,
        expected_user: Option<&str>,
    ) -> Result<OidcClaims, OidcError> {
        // Fetch current OIDC configuration from system variables
        let system_vars = self.adapter_client.get_system_vars().await;
        let Some(issuer) = OIDC_ISSUER.get(system_vars.dyncfgs()) else {
            return Err(OidcError::MissingIssuer);
        };

        let audience = {
            let aud = OIDC_AUDIENCE.get(system_vars.dyncfgs());
            if aud.is_none() {
                warn!(
                    "Audience validation skipped. It is discouraged
                    to skip audience validation since it allows
                    anyone with a JWT issued by the same issuer
                    to authenticate."
                );
            }
            aud
        };

        // Decode header to get key ID (kid) and algorithm
        let header = jsonwebtoken::decode_header(token).map_err(OidcError::Jwt)?;

        let kid = header.kid.ok_or(OidcError::MissingKid)?;
        // Find matching key from cached keys
        let decoding_key = self.find_key(&kid, &issuer).await?;

        // Set up validation
        // TODO (Oidc): Make JWT expiration configurable.
        let mut validation = jsonwebtoken::Validation::new(header.alg);
        validation.set_issuer(&[&issuer]);
        if let Some(ref audience) = audience {
            validation.set_audience(&[audience]);
        } else {
            validation.validate_aud = false;
        }

        // Decode and validate the token
        let token_data = jsonwebtoken::decode::<OidcClaims>(token, &(decoding_key.0), &validation)
            .map_err(OidcError::Jwt)?;

        // Optionally validate expected user
        if let Some(expected) = expected_user {
            if token_data.claims.username() != expected {
                return Err(OidcError::WrongUser);
            }
        }

        Ok(token_data.claims)
    }
}

impl GenericOidcAuthenticator {
    pub async fn authenticate(
        &self,
        token: &str,
        expected_user: Option<&str>,
    ) -> Result<(OidcClaims, Authenticated), OidcError> {
        let claims = self.inner.validate_token(token, expected_user).await?;
        Ok((claims, Authenticated))
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
