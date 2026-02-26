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
use mz_adapter_types::dyncfgs::{OIDC_AUDIENCE, OIDC_AUTHENTICATION_CLAIM, OIDC_ISSUER};
use mz_auth::Authenticated;
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
    MissingAuthenticationClaim,
    /// Failed to parse OIDC configuration URL.
    InvalidIssuerUrl(String),
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
        expected_audience: String,
    },
    InvalidIssuer {
        expected_issuer: String,
    },
    ExpiredSignature,
}

impl std::fmt::Display for OidcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OidcError::MissingIssuer => write!(f, "OIDC issuer is not configured"),
            OidcError::MissingAuthenticationClaim => {
                write!(f, "OIDC authentication claim is not configured")
            }
            OidcError::InvalidIssuerUrl(_) => write!(f, "invalid OIDC issuer URL"),
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
            OidcError::InvalidAudience { expected_audience } => Some(format!(
                "Expected audience \"{expected_audience}\" in the JWT.",
            )),
            OidcError::InvalidIssuer { expected_issuer } => {
                Some(format!("Expected issuer \"{expected_issuer}\" in the JWT.",))
            }
            OidcError::NoMatchingAuthenticationClaim {
                authentication_claim,
            } => Some(format!(
                "Expected authentication claim \"{authentication_claim}\" in the JWT.",
            )),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            OidcError::MissingIssuer => {
                Some("Configure the OIDC issuer using the oidc_issuer system variable.".into())
            }
            OidcError::MissingAuthenticationClaim => {
                Some("Configure which token claim to use as the username using the oidc_authentication_claim system variable.".into())
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
    /// Subject (user identifier).
    pub sub: String,
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

impl OidcClaims {
    /// Extract the username from the OIDC claims.
    fn user(&self, authentication_claim: &str) -> Option<&str> {
        match authentication_claim {
            "sub" => Some(&self.sub),
            _ => self
                .unknown_claims
                .get(authentication_claim)
                .and_then(|value| value.as_str()),
        }
    }
}

pub struct ValidatedClaims {
    pub user: String,
    // Prevent construction outside of `GenericOidcAuthenticator::validate_token`.
    _private: (),
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
        let openid_config_url = Url::parse(&format!("{issuer}/.well-known/openid-configuration"))
            .map_err(|_| OidcError::InvalidIssuerUrl(issuer.to_string()))?;

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
            decoding_keys.extend(new_decoding_keys);
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

        let Some(authentication_claim) = OIDC_AUTHENTICATION_CLAIM.get(system_vars.dyncfgs())
        else {
            return Err(OidcError::MissingAuthenticationClaim);
        };

        let audience = {
            let aud = OIDC_AUDIENCE.get(system_vars.dyncfgs());
            if aud.is_none() {
                warn!(
                    "Audience validation skipped. It is discouraged \
                    to skip audience validation since it allows anyone \
                    with a JWT issued by the same issuer to authenticate."
                );
            }
            aud
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
        if let Some(audience) = &audience {
            validation.set_audience(&[audience]);
        } else {
            validation.validate_aud = false;
        }

        // Decode and validate the token
        let token_data = jsonwebtoken::decode::<OidcClaims>(token, &(decoding_key.0), &validation)
            .map_err(|e| match e.kind() {
                jsonwebtoken::errors::ErrorKind::InvalidAudience => {
                    if let Some(audience) = &audience {
                        OidcError::InvalidAudience {
                            expected_audience: audience.clone(),
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

        Ok(ValidatedClaims {
            user: user.to_string(),
            _private: (),
        })
    }
}

impl GenericOidcAuthenticator {
    pub async fn authenticate(
        &self,
        token: &str,
        expected_user: Option<&str>,
    ) -> Result<(ValidatedClaims, Authenticated), OidcError> {
        let validated_claims = self.inner.validate_token(token, expected_user).await?;
        Ok((validated_claims, Authenticated))
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

    #[mz_ore::test]
    fn test_user() {
        let json = r#"{"sub":"user-123","iss":"issuer","exp":1234,"aud":["app"],"email":"alice@example.com"}"#;
        let claims: OidcClaims = serde_json::from_str(json).unwrap();
        assert_eq!(claims.user("sub"), Some("user-123"));
        assert_eq!(claims.user("email"), Some("alice@example.com"));
        assert_eq!(claims.user("missing"), None);
    }
}
