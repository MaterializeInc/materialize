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

use std::time::Duration;

use jsonwebtoken::{DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use tracing::warn;

/// Command line arguments for OIDC authentication.
#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// OIDC issuer URL (e.g., "https://accounts.google.com").
    /// This is validated against the `iss` claim in the JWT.
    pub oidc_issuer: String,
    /// JWKS URI for fetching public keys.
    /// (e.g., "https://www.googleapis.com/oauth2/v3/certs")
    pub oidc_jwks_uri: String,
}

/// Errors that can occur during OIDC authentication.
#[derive(Debug)]
pub enum OidcError {
    /// JWT token has expired.
    TokenExpired,
    /// JWT signature is invalid.
    InvalidSignature,
    /// JWT issuer does not match expected value.
    InvalidIssuer,
    /// Failed to fetch JWKS from provider.
    JwksFetchFailed(String),
    /// OIDC configuration is incomplete.
    IncompleteConfig(String),
    /// JWT is malformed or could not be parsed.
    MalformedToken(String),
    /// No matching key found in JWKS.
    NoMatchingKey,
}

impl std::fmt::Display for OidcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OidcError::TokenExpired => write!(f, "token expired"),
            OidcError::InvalidSignature => write!(f, "invalid signature"),
            OidcError::InvalidIssuer => write!(f, "invalid issuer"),
            OidcError::JwksFetchFailed(e) => write!(f, "failed to fetch JWKS: {}", e),
            OidcError::IncompleteConfig(e) => write!(f, "incomplete OIDC config: {}", e),
            OidcError::MalformedToken(e) => write!(f, "malformed token: {}", e),
            OidcError::NoMatchingKey => write!(f, "no matching key in JWKS"),
        }
    }
}

impl std::error::Error for OidcError {}

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
    /// Whether the email is verified.
    #[serde(default)]
    pub email_verified: Option<bool>,
    /// Preferred username claim.
    #[serde(default)]
    pub preferred_username: Option<String>,
    /// Name claim.
    #[serde(default)]
    pub name: Option<String>,
}

impl OidcClaims {
    /// Extract the username to use for the session.
    ///
    /// Priority: email > preferred_username > sub
    pub fn username(&self) -> &str {
        self.email
            .as_deref()
            .or(self.preferred_username.as_deref())
            .unwrap_or(&self.sub)
    }
}

/// OIDC Authenticator that validates JWTs using JWKS.
#[derive(Debug, Clone)]
pub struct OidcAuthenticator {
    issuer: String,
    jwks_uri: String,
    http_client: HttpClient,
}

impl OidcAuthenticator {
    /// Create a new [`OidcAuthenticator`] from [`OidcConfig`].
    pub fn new(config: OidcConfig) -> Self {
        Self {
            issuer: config.oidc_issuer,
            jwks_uri: config.oidc_jwks_uri,
            http_client: HttpClient::new(),
        }
    }

    /// Validate a JWT token and return the claims.
    ///
    /// This performs the following validations:
    /// 1. Decode the JWT header to get the key ID (kid) and algorithm
    /// 2. Fetch JWKS and find the matching key
    /// 3. Verify the signature
    /// 4. Validate claims (exp, iss)
    pub async fn validate_token(&self, token: &str) -> Result<OidcClaims, OidcError> {
        // 1. Decode header to get key ID (kid) and algorithm
        let header = decode_header(token).map_err(|e| OidcError::MalformedToken(e.to_string()))?;

        // 2. Fetch JWKS and get the matching key
        let decoding_key = self.fetch_decoding_key(&header.kid).await?;

        // 3. Set up validation
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[&self.issuer]);
        validation.validate_aud = false;

        // 4. Decode and validate the token
        let token_data = decode::<OidcClaims>(token, &decoding_key, &validation).map_err(|e| {
            use jsonwebtoken::errors::ErrorKind;
            match e.kind() {
                ErrorKind::ExpiredSignature => OidcError::TokenExpired,
                ErrorKind::InvalidSignature => OidcError::InvalidSignature,
                ErrorKind::InvalidIssuer => OidcError::InvalidIssuer,
                _ => OidcError::MalformedToken(e.to_string()),
            }
        })?;

        Ok(token_data.claims)
    }

    /// Fetch JWKS from the provider and return the decoding key.
    async fn fetch_decoding_key(&self, kid: &Option<String>) -> Result<DecodingKey, OidcError> {
        let response = self
            .http_client
            .get(&self.jwks_uri)
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

        // Find the matching key
        for jwk in jwks.keys {
            let jwk_kid = jwk.common.key_id.as_ref();

            // Match by kid if provided, otherwise use the first key
            let is_match = match kid {
                Some(k) => jwk_kid == Some(k),
                None => true,
            };

            if is_match {
                match DecodingKey::from_jwk(&jwk) {
                    Ok(key) => return Ok(key),
                    Err(e) => {
                        warn!("Failed to parse JWK: {}", e);
                        continue;
                    }
                }
            }
        }

        Err(OidcError::NoMatchingKey)
    }
}
