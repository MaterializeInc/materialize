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
use mz_ore::now::NowFn;
use uuid::Uuid;

use crate::{ApiTokenResponse, AppPassword, Client, Error};

pub struct AuthenticationConfig {
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
    /// Name of admin role.
    pub admin_role: String,
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct Authentication {
    admin_api_token_url: String,
    #[derivative(Debug = "ignore")]
    decoding_key: DecodingKey,
    tenant_id: Uuid,
    now: NowFn,
    validation: Validation,
    refresh_before_secs: i64,
    admin_role: String,
    client: Client,
}

pub const REFRESH_SUFFIX: &str = "/token/refresh";

impl Authentication {
    /// Creates a new frontegg auth. `jwk_rsa_pem` is the RSA public key to
    /// validate the JWTs. `tenant_id` must be parseable as a UUID.
    pub fn new(config: AuthenticationConfig, client: Client) -> Self {
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
            admin_role: config.admin_role,
            client,
        }
    }

    /// Exchanges a password for a JWT token.
    pub async fn exchange_password_for_token(
        &self,
        password: &str,
    ) -> Result<ApiTokenResponse, Error> {
        let password: AppPassword = password.parse()?;
        self.client
            .exchange_client_secret_for_token(
                password.client_id,
                password.secret_key,
                &self.admin_api_token_url,
            )
            .await
    }

    /// Validates an access token and its `tenant_id`.
    pub fn validate_access_token(
        &self,
        token: &str,
        expected_email: Option<&str>,
    ) -> Result<Claims, Error> {
        let msg = decode::<Claims>(token, &self.decoding_key, &self.validation)?;
        if msg.claims.exp < self.now.as_secs() {
            return Err(Error::TokenExpired);
        }
        if msg.claims.tenant_id != self.tenant_id {
            return Err(Error::UnauthorizedTenant);
        }
        if let Some(expected_email) = expected_email {
            if msg.claims.email != expected_email {
                return Err(Error::WrongEmail);
            }
        }
        Ok(msg.claims)
    }

    /// Continuously validates and refreshes an access token.
    ///
    /// Validates the provided access token once, as `validate_access_token`
    /// does. If it is valid, returns a future that will attempt to refresh
    /// the access token before it expires, resolving iff the token expires
    /// or fails to refresh.
    ///
    /// The claims contained in the provided access token and all updated
    /// claims will be processed by `claims_processor`.
    pub fn continuously_validate_access_token(
        &self,
        mut token: ApiTokenResponse,
        expected_email: String,
        mut claims_processor: impl FnMut(Claims),
    ) -> Result<impl Future<Output = ()>, Error> {
        // Do an initial full validity check of the token.
        let mut claims = self.validate_access_token(&token.access_token, Some(&expected_email))?;
        claims_processor(claims.clone());
        let frontegg = self.clone();

        // This future resolves once the token expiry time has been reached. It will
        // repeatedly attempt to refresh the token before it expires.
        let expire_future = async move {
            let refresh_url = format!("{}{}", frontegg.admin_api_token_url, REFRESH_SUFFIX);
            loop {
                let expire_in = claims.exp - frontegg.now.as_secs();
                let check_in = u64::try_from(expire_in - frontegg.refresh_before_secs).unwrap_or(0);
                tokio::time::sleep(Duration::from_secs(check_in)).await;

                let refresh_request = async {
                    loop {
                        let resp = async {
                            let token = frontegg
                                .client
                                .refresh_token(&refresh_url, &token.refresh_token)
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
                let expire_in = u64::try_from(claims.exp - frontegg.now.as_secs()).unwrap_or(0);
                let expire_in = tokio::time::sleep(Duration::from_secs(expire_in));

                tokio::select! {
                    _ = expire_in => return (),
                    (refresh_token, refresh_claims) = refresh_request => {
                        token = refresh_token;
                        claims = refresh_claims;
                        claims_processor(claims.clone());
                    },
                };
            }
        };
        Ok(expire_future)
    }

    pub fn tenant_id(&self) -> Uuid {
        self.tenant_id
    }

    pub fn admin_role(&self) -> &str {
        &self.admin_role
    }
}

// TODO: Do we care about the sub? Do we need to validate the sub or other
// things, even if unused?
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Claims {
    pub exp: i64,
    pub email: String,
    pub sub: Uuid,
    pub user_id: Option<Uuid>,
    pub tenant_id: Uuid,
    pub roles: Vec<String>,
    pub permissions: Vec<String>,
}

impl Claims {
    /// Extracts the most specific user ID present in the token.
    pub fn best_user_id(&self) -> Uuid {
        self.user_id.unwrap_or(self.sub)
    }

    /// Returns true if the claims belong to a frontegg admin.
    pub fn admin(&self, admin_name: &str) -> bool {
        self.roles.iter().any(|role| role == admin_name)
    }
}
