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
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use mz_ore::now::NowFn;
use mz_sql::session::user::ExternalUserMetadata;
use serde::{Deserialize, Serialize};
use tokio::time;
use tracing::info;
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
    /// Creates a new frontegg auth.
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

    /// Exchanges a password for an access token and a refresh token.
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

    /// Validates an API token response, returning a validated response
    /// containing the validated claims.
    ///
    /// Like [`Authentication::validate_access_token`], but operates on an
    /// [`ApiTokenResponse`] rather than a raw access token.
    pub fn validate_api_token_response(
        &self,
        response: ApiTokenResponse,
        expected_email: Option<&str>,
    ) -> Result<ValidatedApiTokenResponse, Error> {
        let claims = self.validate_access_token(&response.access_token, expected_email)?;
        Ok(ValidatedApiTokenResponse {
            claims,
            refresh_token: response.refresh_token,
        })
    }

    /// Validates an access token, returning the validated claims.
    ///
    /// The following validations are always performed:
    ///
    ///   * The token is not expired, according to the `Authentication`'s clock.
    ///
    ///   * The tenant ID in the token matches the `Authentication`'s tenant ID.
    ///
    /// If `expected_email` is provided, the token's email is additionally
    /// validated to match `expected_email`.
    pub fn validate_access_token(
        &self,
        token: &str,
        expected_email: Option<&str>,
    ) -> Result<ValidatedClaims, Error> {
        let msg = jsonwebtoken::decode::<Claims>(token, &self.decoding_key, &self.validation)?;
        if msg.claims.exp < self.now.as_secs() {
            return Err(Error::TokenExpired);
        }
        if msg.claims.tenant_id != self.tenant_id {
            return Err(Error::UnauthorizedTenant);
        }
        if let Some(expected_email) = expected_email {
            // To match Frontegg, email addresses are compared case
            // insensitively.
            //
            // NOTE(benesch): we could save some allocations by using
            // `unicase::eq` here, but the `unicase` crate has had some critical
            // correctness bugs that make it scary to use in such
            // security-sensitive code.
            //
            // See: https://github.com/seanmonstar/unicase/pull/39
            if msg.claims.email.to_lowercase() != expected_email.to_lowercase() {
                return Err(Error::WrongEmail);
            }
        }
        Ok(ValidatedClaims {
            exp: msg.claims.exp,
            email: msg.claims.email,
            // If the claims come from the exchange of an API token, the `sub`
            // will be the ID of the API token and the user ID will be in the
            // `user_id` field. If the claims come from the exchange of a
            // username and password, the `sub` is the user ID and the `user_id`
            // field will not be present. This makes sense once you think about
            // it, but is confusing enough that we massage into a single
            // `user_id` field that always contains the user ID.
            user_id: msg.claims.user_id.unwrap_or(msg.claims.sub),
            // The user is an administrator if they have the admin role that the
            // `Authenticator` has been configured with.
            is_admin: msg.claims.roles.iter().any(|r| *r == self.admin_role),
            _private: (),
        })
    }

    /// Continuously refreshes and revalidates a [`ValidatedApiTokenResponse`].
    ///
    /// The returned future will attempt to refresh the access token before it
    /// expires, resolving iff the token expires or fails to refresh.
    ///
    /// Whenever the access token is successfully refreshed and revalidated,
    /// `claims_processor` will be invoked with the new validated claims.
    pub fn continuously_revalidate_api_token_response(
        &self,
        mut response: ValidatedApiTokenResponse,
        mut claims_processor: impl FnMut(&ValidatedClaims),
    ) -> impl Future<Output = ()> {
        let frontegg = self.clone();

        // This future resolves once the token expiry time has been reached. It will
        // repeatedly attempt to refresh the token before it expires.
        async move {
            let refresh_url = format!("{}{}", frontegg.admin_api_token_url, REFRESH_SUFFIX);
            loop {
                let expire_in = response.claims.exp - frontegg.now.as_secs();
                let check_in = u64::try_from(expire_in - frontegg.refresh_before_secs).unwrap_or(0);
                time::sleep(Duration::from_secs(check_in)).await;

                let refresh_request = async {
                    loop {
                        let res = async {
                            let res = frontegg
                                .client
                                .refresh_token(&refresh_url, &response.refresh_token)
                                .await?;
                            frontegg.validate_api_token_response(res, Some(&response.claims.email))
                        };
                        match res.await {
                            Ok(res) => return res,
                            Err(e) => {
                                // Log at info level, not warn or error level,
                                // because the error could be a result of a
                                // user's permissions being intentionally
                                // revoked.
                                //
                                // TODO: report Frontegg errors at `error!`
                                // level or via a Prometheus metric so that
                                // we can alert on them.
                                info!(
                                    "failed to refresh token for {}: {:#}",
                                    response.claims.email, e
                                );
                                // Retry again later. 5 seconds chosen arbitrarily.
                                time::sleep(Duration::from_secs(5)).await;
                            }
                        }
                    }
                };
                let expire_in =
                    u64::try_from(response.claims.exp - frontegg.now.as_secs()).unwrap_or(0);
                let expire_in = time::sleep(Duration::from_secs(expire_in));

                tokio::select! {
                    _ = expire_in => return,
                    res = refresh_request => {
                        response = res;
                        claims_processor(&response.claims);
                    },
                };
            }
        }
    }
}

/// An [`ApiTokenResponse`] that has been validated by
/// [`Authentication::validate_api_token_response`].
pub struct ValidatedApiTokenResponse {
    /// The validated claims.
    pub claims: ValidatedClaims,
    /// The refresh token from the API response.
    pub refresh_token: String,
}

// TODO: Do we care about the sub? Do we need to validate the sub or other
// things, even if unused?
/// The raw claims encoded in a Frontegg access token.
///
/// Consult the JSON Web Token specification and the Frontegg documentation to
/// determine the precise semantics of these fields.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

/// [`Claims`] that have been validated by
/// [`Authentication::validate_access_token`].
#[derive(Clone, Debug)]
pub struct ValidatedClaims {
    /// The time at which the claims expire, represented in seconds since the
    /// Unix epoch.
    pub exp: i64,
    /// The ID of the authenticated user.
    pub user_id: Uuid,
    /// The email address of the authenticated user.
    pub email: String,
    /// Whether the authenticated user is an administrator.
    pub is_admin: bool,
    // Prevent construction outside of `Authentication::validate_access_token`.
    _private: (),
}

impl From<&ValidatedClaims> for ExternalUserMetadata {
    fn from(claims: &ValidatedClaims) -> ExternalUserMetadata {
        ExternalUserMetadata {
            user_id: claims.user_id,
            admin: claims.is_admin,
        }
    }
}
