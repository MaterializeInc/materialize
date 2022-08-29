// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::time::Duration;

use derivative::Derivative;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use reqwest::Client;
use thiserror::Error;
use tracing::warn;
use uuid::Uuid;

use mz_ore::{now::NowFn, retry::Retry};

pub struct FronteggConfig {
    /// URL for the token endpoint, including full path.
    pub admin_api_token_url: String,
    /// JWK used to validate JWTs.
    pub decoding_key: DecodingKey,
    /// Tenant id used to validate JWTs.
    pub tenant_id: Uuid,
    /// Function to provide system time to validate exp (expires at) field of JWTs.
    pub now: NowFn,
    /// Number of seconds before which to attempt to renew an expiring token.
    pub refresh_before_secs: i64,
    /// Prefix that is expected to be present on all passwords.
    pub password_prefix: String,
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FronteggAuthentication {
    admin_api_token_url: String,
    #[derivative(Debug = "ignore")]
    decoding_key: DecodingKey,
    tenant_id: Uuid,
    now: NowFn,
    validation: Validation,
    refresh_before_secs: i64,
    password_prefix: String,

    // Reqwest HTTP client pool.
    client: Client,
}

pub const REFRESH_SUFFIX: &str = "/token/refresh";

impl FronteggAuthentication {
    /// Creates a new frontegg auth. `jwk_rsa_pem` is the RSA public key to
    /// validate the JWTs. `tenant_id` must be parseable as a UUID.
    pub fn new(config: FronteggConfig) -> Self {
        let mut validation = Validation::new(Algorithm::RS256);
        // We validate with our own now function.
        validation.validate_exp = false;
        Self {
            admin_api_token_url: config.admin_api_token_url,
            decoding_key: config.decoding_key,
            tenant_id: config.tenant_id,
            now: config.now,
            validation,
            refresh_before_secs: config.refresh_before_secs,
            password_prefix: config.password_prefix,
            client: Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("must build Client"),
        }
    }

    async fn request<T, F, U>(&self, f: F) -> Result<T, FronteggError>
    where
        F: Copy + Fn(Client) -> U,
        U: Future<Output = Result<T, FronteggError>>,
    {
        Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .max_duration(Duration::from_secs(30))
            .retry_async_canceling(|state| async move {
                // Return a Result<Result<T, FronteggError>, FronteggError> so we can
                // differentiate a retryable or not error.
                match f(self.client.clone()).await {
                    Err(FronteggError::ReqwestError(err)) if err.is_timeout() => {
                        warn!("frontegg timeout, attempt {}: {}", state.i, err);
                        Err(FronteggError::from(err))
                    }
                    v @ _ => Ok(v),
                }
            })
            .await?
    }

    /// Exchanges a password for a JWT token.
    ///
    /// Somewhat unusually, the password encodes both the client ID and secret
    /// for the API key in use. Both the client ID and secret are UUIDs. The
    /// password can have one of two formats:
    ///
    ///   * The URL-safe base64 encoding of the concatenated bytes of the UUIDs.
    ///
    ///     This format is a very compact representation (only 43 or 44 bytes)
    ///     that is safe to use in a connection string without escaping.
    ///
    ///   * The concatenated hex-encoding of the UUIDs, with any number of
    ///     special characters that are ignored.
    ///
    ///     This format allows for the UUIDs to be formatted with hyphens, or
    ///     not, and for the two
    pub async fn exchange_password_for_token(
        &self,
        password: &str,
    ) -> Result<ApiTokenResponse, FronteggError> {
        let password = password
            .strip_prefix(&self.password_prefix)
            .ok_or(FronteggError::InvalidPasswordFormat)?;
        let (client_id, secret) = if password.len() == 43 || password.len() == 44 {
            // If it's exactly 43 or 44 bytes, assume we have base64-encoded
            // UUID bytes without or with padding, respectively.
            let buf = base64::decode_config(password, base64::URL_SAFE)
                .map_err(|_| FronteggError::InvalidPasswordFormat)?;
            let client_id =
                Uuid::from_slice(&buf[..16]).map_err(|_| FronteggError::InvalidPasswordFormat)?;
            let secret =
                Uuid::from_slice(&buf[16..]).map_err(|_| FronteggError::InvalidPasswordFormat)?;
            (client_id, secret)
        } else if password.len() >= 64 {
            // If it's more than 64 bytes, assume we have concatenated
            // hex-encoded UUIDs, possibly with some special characters mixed
            // in.
            let mut chars = password.chars().filter(|c| c.is_alphanumeric());
            let client_id = Uuid::parse_str(&chars.by_ref().take(32).collect::<String>())
                .map_err(|_| FronteggError::InvalidPasswordFormat)?;
            let secret = Uuid::parse_str(&chars.take(32).collect::<String>())
                .map_err(|_| FronteggError::InvalidPasswordFormat)?;
            (client_id, secret)
        } else {
            // Otherwise it's definitely not a password format we understand.
            return Err(FronteggError::InvalidPasswordFormat);
        };
        self.exchange_client_secret_for_token(client_id, secret)
            .await
    }

    /// Exchanges a client id and secret for a jwt token.
    pub async fn exchange_client_secret_for_token(
        &self,
        client_id: Uuid,
        secret: Uuid,
    ) -> Result<ApiTokenResponse, FronteggError> {
        self.request(|client| async move {
            let res: ApiTokenResponse = client
                .post(&self.admin_api_token_url)
                .json(&ApiTokenArgs { client_id, secret })
                .send()
                .await?
                .error_for_status()?
                .json::<ApiTokenResponse>()
                .await?;
            Ok(res)
        })
        .await
    }

    /// Validates an access token and its `tenant_id`.
    pub fn validate_access_token(
        &self,
        token: &str,
        expected_email: Option<&str>,
    ) -> Result<Claims, FronteggError> {
        let msg = decode::<Claims>(&token, &self.decoding_key, &self.validation)?;
        if msg.claims.exp < self.now.as_secs() {
            return Err(FronteggError::TokenExpired);
        }
        if msg.claims.tenant_id != self.tenant_id {
            return Err(FronteggError::UnauthorizedTenant);
        }
        if let Some(expected_email) = expected_email {
            if msg.claims.email != expected_email {
                return Err(FronteggError::WrongEmail);
            }
        }
        Ok(msg.claims)
    }

    /// Returns a future that resolves if the token has expired.
    pub fn check_expiry(
        &self,
        mut token: ApiTokenResponse,
        expected_email: String,
    ) -> Result<impl Future<Output = ()>, FronteggError> {
        // Do an initial full validity check of the token.
        let mut claims = self.validate_access_token(&token.access_token, Some(&expected_email))?;
        let frontegg = self.clone();

        // This future resolves once the token expiry time has been reached. It will
        // repeatedly attempt to refresh the token before it expires.
        Ok(async move {
            let refresh_url = format!("{}{}", frontegg.admin_api_token_url, REFRESH_SUFFIX);
            loop {
                let expire_in = claims.exp - frontegg.now.as_secs();
                // Using max(0, X) here ensures we don't have a negative, and thus have a
                // lossless conversion to u64.
                let check_in = std::cmp::max(0, expire_in - frontegg.refresh_before_secs) as u64;
                tokio::time::sleep(Duration::from_secs(check_in)).await;

                let refresh_request = async {
                    let refresh = RefreshToken {
                        refresh_token: token.refresh_token,
                    };
                    loop {
                        let resp = async {
                            let token = frontegg
                                .client
                                .post(&refresh_url)
                                .json(&refresh)
                                .send()
                                .await?
                                .error_for_status()?
                                .json::<ApiTokenResponse>()
                                .await?;
                            let claims = frontegg.validate_access_token(
                                &token.access_token,
                                Some(&expected_email),
                            )?;
                            Ok::<(ApiTokenResponse, Claims), anyhow::Error>((token, claims))
                        };
                        match resp.await {
                            Ok((token, claims)) => {
                                return (token, claims);
                            }
                            Err(_) => {
                                // Some error occurred, retry again later. 5 seconds chosen arbitrarily.
                                tokio::time::sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }
                };
                let expire_in = std::cmp::max(0, claims.exp - frontegg.now.as_secs()) as u64;
                let expire_in = tokio::time::sleep(Duration::from_secs(expire_in));

                tokio::select! {
                    _ = expire_in => return (),
                    (refresh_token, refresh_claims) = refresh_request => {
                        token = refresh_token;
                        claims = refresh_claims;
                    },
                };
            }
        })
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiTokenArgs {
    pub client_id: Uuid,
    pub secret: Uuid,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RefreshToken {
    pub refresh_token: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiTokenResponse {
    pub expires: String,
    pub expires_in: i64,
    pub access_token: String,
    pub refresh_token: String,
}

// TODO: Do we care about the sub? Do we need to validate the sub or other
// things, even if unused?
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Claims {
    pub exp: i64,
    pub email: String,
    pub tenant_id: Uuid,
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
}

#[derive(Error, Debug)]
pub enum FronteggError {
    #[error("invalid password format")]
    InvalidPasswordFormat,
    #[error("invalid token format: {0}")]
    InvalidTokenFormat(#[from] jsonwebtoken::errors::Error),
    #[error("authentication token exchange failed: {0}")]
    ReqwestError(#[from] reqwest::Error),
    #[error("authentication token expired")]
    TokenExpired,
    #[error("unauthorized organization")]
    UnauthorizedTenant,
    #[error("email in access token did not match the expected email")]
    WrongEmail,
    #[error("request timeout")]
    Timeout(#[from] tokio::time::error::Elapsed),
}
