// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;
use derivative::Derivative;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use uuid::Uuid;

use mz_ore::now::NowFn;

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct FronteggAuthentication {
    admin_api_token_url: String,
    #[derivative(Debug = "ignore")]
    decoding_key: DecodingKey,
    tenant_id: Uuid,
    now: NowFn,
    validation: Validation,
}

impl FronteggAuthentication {
    /// Creates a new frontegg auth. `jwk_rsa_pem` is the RSA public key to
    /// validate the JWTs. `tenant_id` must be parseable as a UUID.
    pub fn new(
        admin_api_token_url: String,
        jwk_rsa_pem: &[u8],
        tenant_id: Uuid,
        now: NowFn,
    ) -> Result<Self, anyhow::Error> {
        let decoding_key = DecodingKey::from_rsa_pem(&jwk_rsa_pem)?;
        let mut validation = Validation::new(Algorithm::RS256);
        // We validate with our own now function.
        validation.validate_exp = false;
        Ok(Self {
            admin_api_token_url,
            decoding_key,
            tenant_id,
            now,
            validation,
        })
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
    ) -> Result<ApiTokenResponse, anyhow::Error> {
        let (client_id, secret) = if password.len() == 43 || password.len() == 44 {
            // If it's exactly 43 or 44 bytes, assume we have base64-encoded
            // UUID bytes without or with padding, respectively.
            let buf = base64::decode_config(password, base64::URL_SAFE)?;
            let client_id = Uuid::from_slice(&buf[..16])?;
            let secret = Uuid::from_slice(&buf[16..])?;
            (client_id, secret)
        } else if password.len() >= 64 {
            // If it's more than 64 bytes, assume we have concatenated
            // hex-encoded UUIDs, possibly with some special characters mixed
            // in.
            let mut chars = password.chars().filter(|c| c.is_alphanumeric());
            let client_id = Uuid::parse_str(&chars.by_ref().take(32).collect::<String>())?;
            let secret = Uuid::parse_str(&chars.take(32).collect::<String>())?;
            (client_id, secret)
        } else {
            // Otherwise it's definitely not a password format we understand.
            bail!("invalid password");
        };
        self.exchange_client_secret_for_token(client_id, secret)
            .await
    }

    /// Exchanges a client id and secret for a jwt token.
    pub async fn exchange_client_secret_for_token(
        &self,
        client_id: Uuid,
        secret: Uuid,
    ) -> Result<ApiTokenResponse, anyhow::Error> {
        let resp = reqwest::Client::new()
            .post(&self.admin_api_token_url)
            .json(&ApiTokenArgs { client_id, secret })
            .send()
            .await?
            .error_for_status()?
            .json::<ApiTokenResponse>()
            .await?;

        Ok(resp)
    }

    /// Validates an access token and its `tenant_id`.
    pub fn validate_access_token(&self, token: &str) -> Result<Claims, anyhow::Error> {
        let msg = decode::<Claims>(&token, &self.decoding_key, &self.validation)?;
        if msg.claims.exp < self.now.as_secs() {
            bail!("token expired")
        }
        if msg.claims.tenant_id != self.tenant_id {
            bail!("tenant ids don't match")
        }
        Ok(msg.claims)
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
