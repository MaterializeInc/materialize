// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use derivative::Derivative;
use futures::future::BoxFuture;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use mz_ore::cast::{CastLossy, ReinterpretCast};
use mz_ore::collections::HashMap;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_repr::user::ExternalUserMetadata;
use serde::{Deserialize, Serialize};
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use crate::metrics::Metrics;
use crate::{ApiTokenArgs, ApiTokenResponse, AppPassword, Client, Error, FronteggCliArgs};

pub const REFRESH_SUFFIX: &str = "/token/refresh";

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct AuthenticationConfig {
    /// URL for the token endpoint, including full path.
    pub admin_api_token_url: String,
    /// JWK used to validate JWTs.
    #[derivative(Debug = "ignore")]
    pub decoding_key: DecodingKey,
    /// Optional tenant id used to validate JWTs.
    pub tenant_id: Option<Uuid>,
    /// Function to provide system time to validate exp (expires at) field of JWTs.
    pub now: NowFn,
    /// Name of admin role.
    pub admin_role: String,
}

/// Facilitates authenticating users via Frontegg, and verifying returned JWTs.
#[derive(Clone, Debug)]
pub struct Authentication {
    /// Configuration for validating JWTs.
    validation_config: ValidationConfig,

    /// Metrics to track the performance of Frontegg.
    metrics: Metrics,

    /// URL for the token endpoint, including full path.
    admin_api_token_url: String,
    /// HTTP client for making requests.
    client: Client,
    /// Map of inflight authentication requests, used for de-duplicating requests.
    inflight_auth_requests: Arc<Mutex<HashMap<ApiTokenArgs, ResponseHandles>>>,
}

type ResponseHandles =
    Vec<oneshot::Sender<Result<(ExchangePasswordForTokenResponse, RefreshTaskId), Error>>>;

/// An ID emitted in logs to help debug.
#[derive(Copy, Clone, Debug)]
pub struct RefreshTaskId(Uuid);

impl RefreshTaskId {
    /// Generates a new [`RefreshTaskId`].
    pub fn new() -> Self {
        RefreshTaskId(uuid::Uuid::new_v4())
    }
}

impl fmt::Display for RefreshTaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ExchangePasswordForTokenResponse {
    /// Validated JWT.
    pub claims: ValidatedClaims,
    /// Recieves periodic updates of [`ExternalUserMetadata`] when the initial JWT is refreshed.
    pub refresh_updates: watch::Receiver<ExternalUserMetadata>,
    /// Our current authentication is valid, until this Future resolves.
    #[derivative(Debug = "ignore")]
    pub valid_until_fut: BoxFuture<'static, ()>,
}

impl Authentication {
    /// Creates a new frontegg auth.
    pub fn new(config: AuthenticationConfig, client: Client, registry: &MetricsRegistry) -> Self {
        // We validate with our own now function.
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = false;
        let validation_config = ValidationConfig {
            decoding_key: config.decoding_key,
            tenant_id: config.tenant_id,
            now: config.now,
            admin_role: config.admin_role,
            validation,
        };

        let inflight_auth_requests = Arc::new(Mutex::new(HashMap::new()));
        let metrics = Metrics::register_into(registry);

        Self {
            validation_config,
            metrics,
            admin_api_token_url: config.admin_api_token_url,
            client,
            inflight_auth_requests,
        }
    }

    /// Create an [`Authentication`] from [`FronteggCliArgs`].
    pub fn from_args(
        args: FronteggCliArgs,
        registry: &MetricsRegistry,
    ) -> Result<Option<Self>, Error> {
        let config = match (
            args.frontegg_tenant,
            args.frontegg_api_token_url,
            args.frontegg_admin_role,
        ) {
            (None, None, None) => {
                return Ok(None);
            }
            (Some(tenant_id), Some(admin_api_token_url), Some(admin_role)) => {
                let decoding_key = match (args.frontegg_jwk, args.frontegg_jwk_file) {
                    (None, Some(path)) => {
                        let jwk = std::fs::read(&path)
                            .with_context(|| format!("reading {path:?} for --frontegg-jwk-file"))?;
                        DecodingKey::from_rsa_pem(&jwk)?
                    }
                    (Some(jwk), None) => DecodingKey::from_rsa_pem(jwk.as_bytes())?,
                    _ => {
                        return Err(anyhow::anyhow!(
                            "expected exactly one of --frontegg-jwk or --frontegg-jwk-file"
                        )
                        .into())
                    }
                };
                AuthenticationConfig {
                    admin_api_token_url,
                    decoding_key,
                    tenant_id: Some(tenant_id),
                    now: mz_ore::now::SYSTEM_TIME.clone(),
                    admin_role,
                }
            }
            _ => unreachable!("clap enforced"),
        };
        let client = Client::environmentd_default();

        Ok(Some(Self::new(config, client, registry)))
    }

    /// Exchanges a password for an access token and a refresh token.
    pub async fn exchange_password_for_token(
        &self,
        password: &str,
        expected_email: String,
    ) -> Result<ExchangePasswordForTokenResponse, Error> {
        let password: AppPassword = password.parse()?;
        let req = ApiTokenArgs {
            client_id: password.client_id,
            secret: password.secret_key,
        };

        // Note: we get the reciever in a block to scope the access to the mutex.
        let rx = {
            let mut inflight = self
                .inflight_auth_requests
                .lock()
                .expect("Frontegg Auth Client panicked");
            let (tx, rx) = tokio::sync::oneshot::channel();

            match inflight.get_mut(&req) {
                // Already have an inflight request, add to our list of waiters.
                Some(senders) => {
                    tracing::debug!(?req, "reusing request");
                    senders.push(tx);
                    rx
                }
                // New request! Need to queue one up.
                None => {
                    tracing::debug!(?req, "spawning new request");

                    inflight.insert(req.clone(), vec![tx]);
                    // Explicitly drop the lock guard.
                    drop(inflight);

                    let client = self.client.clone();
                    let inflight = Arc::clone(&self.inflight_auth_requests);
                    let req_ = req.clone();
                    let url = self.admin_api_token_url.clone();
                    let validate_config = self.validation_config.clone();
                    let metrics = self.metrics.clone();

                    let name = format!("frontegg-auth-request-{}", req.client_id);
                    mz_ore::task::spawn(move || name, async move {
                        // Make the actual request.
                        let result = client
                            .exchange_client_secret_for_token(req_, &url, &metrics)
                            .await;

                        // Validate the returned JWT.
                        let validated_result = result.and_then(|api_resp| {
                            validate_access_token(
                                &validate_config,
                                &api_resp.access_token,
                                Some(&expected_email),
                            )
                            .map(|claims| (api_resp, claims))
                        });

                        // Get all of our waiters.
                        let mut inflight = inflight.lock().expect("Frontegg Auth Client panicked");
                        let Some(waiters) = inflight.remove(&req) else {
                            tracing::error!(?req, "Inflight entry already removed?");
                            return;
                        };

                        // If the request failed then notify and bail.
                        let (api_resp, claims) = match validated_result {
                            Err(err) => {
                                tracing::warn!(?err, "failed to exchange secret for token");
                                for tx in waiters {
                                    let _ = tx.send(Err(err.clone()));
                                }
                                return;
                            }
                            Ok(result) => result,
                        };

                        // Spawn a task to continuously refresh our token.
                        let external_metadata = ExternalUserMetadata {
                            user_id: claims.user_id,
                            admin: claims.is_admin,
                        };
                        let (refresh_tx, refresh_rx) = watch::channel(external_metadata);
                        let id = continuously_refresh(
                            &url,
                            client,
                            metrics,
                            api_resp,
                            expected_email,
                            validate_config,
                            refresh_tx,
                        );

                        // Respond to all of our waiters.
                        for tx in waiters {
                            let valid_until_fut = make_valid_until_future(&refresh_rx);
                            let resp = ExchangePasswordForTokenResponse {
                                claims: claims.clone(),
                                refresh_updates: refresh_rx.clone(),
                                valid_until_fut,
                            };
                            let _ = tx.send(Ok((resp, id)));
                        }
                    });

                    rx
                }
            }
        };

        let resp = rx.await.context("waiting for inflight response")?;

        let (result, id) = resp?;
        tracing::debug!(?id, "token being refreshed");

        Ok(result)
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
        validate_access_token(&self.validation_config, token, expected_email)
    }
}

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct ValidationConfig {
    /// Fields of a JWT that we validate.
    pub validation: Validation,
    /// JWK used to validate JWTs.
    #[derivative(Debug = "ignore")]
    pub decoding_key: DecodingKey,
    /// Tenant id used to validate JWTs.
    pub tenant_id: Option<Uuid>,
    /// Function to provide system time to validate exp (expires at) field of JWTs.
    pub now: NowFn,
    /// Name of admin role.
    pub admin_role: String,
}

/// Validates the provided JSON web token, returning [`ValidatedClaims`].
fn validate_access_token(
    config: &ValidationConfig,
    jwt: &str,
    expected_email: Option<&str>,
) -> Result<ValidatedClaims, Error> {
    let msg = jsonwebtoken::decode::<Claims>(jwt, &config.decoding_key, &config.validation)?;
    if msg.claims.exp < config.now.as_secs() {
        return Err(Error::TokenExpired);
    }
    if let Some(expected_tenant_id) = config.tenant_id {
        if msg.claims.tenant_id != expected_tenant_id {
            return Err(Error::UnauthorizedTenant);
        }
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
        tenant_id: msg.claims.tenant_id,
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
        is_admin: msg.claims.roles.iter().any(|r| *r == config.admin_role),
        _private: (),
    })
}

/// Returns a [`Duration`] for how long the provided claims are valid for.
fn valid_for(now: &NowFn, claims: &ValidatedClaims) -> Duration {
    let valid_for = claims.exp - now.as_secs();
    let valid_for = u64::try_from(valid_for).unwrap_or(0);

    Duration::from_secs(valid_for)
}

/// Returns a `Future` that resolves when the sending side of the provided channel closes.
fn make_valid_until_future(
    refresh_rx: &watch::Receiver<ExternalUserMetadata>,
) -> BoxFuture<'static, ()> {
    // Our auth is valid for as long as the sender is alive.
    let mut refresh_rx = refresh_rx.clone();
    let future = async move { while let Ok(_) = refresh_rx.changed().await {} };
    Box::pin(future)
}

/// Spawns a task that will continuously refresh the `RefreshToken` provided in `first_response`.
fn continuously_refresh(
    admin_api_token_url: &str,
    client: Client,
    metrics: Metrics,
    first_response: ApiTokenResponse,
    expected_email: String,
    validation_config: ValidationConfig,
    refresh_tx: watch::Sender<ExternalUserMetadata>,
) -> RefreshTaskId {
    let refresh_url = format!("{}{}", admin_api_token_url, REFRESH_SUFFIX);
    let id = RefreshTaskId::new();

    // Continuously refresh.
    let name = format!("frontegg-auth-refresh-{}", id);
    mz_ore::task::spawn(|| name, async move {
        tracing::debug!(?id, "starting refresh task");
        let mut latest_response = first_response;

        let gauge = metrics.refresh_tasks_active.with_label_values(&[]);
        gauge.inc();

        loop {
            // If the token is not valid, close the channel.
            let validated = validate_access_token(
                &validation_config,
                &latest_response.access_token,
                Some(&expected_email),
            );
            let claims = match validated {
                Err(err) => {
                    tracing::warn!(?id, ?err, "Token expired!");
                    drop(refresh_tx);
                    return;
                }
                Ok(claims) => claims,
            };
            let valid_for = valid_for(&validation_config.now, &claims);

            // Notify listeners that we've refreshed.
            let metadata = ExternalUserMetadata {
                user_id: claims.user_id,
                admin: claims.is_admin,
            };
            if let Err(_) = refresh_tx.send(metadata) {
                tracing::info!(?id, "All listeners disappeared");
                break;
            }

            // Odd, but guards against a negative "expires_in" value.
            let expires_in = latest_response.expires_in.max(60);
            // Safe because we know expires_in will always be positive.
            let expires_in = u64::reinterpret_cast(expires_in);
            // Need to to be a float so we can scale the value.
            let expires_in = f64::cast_lossy(expires_in);

            // The Frontegg Python SDK scales the expires_in this way.
            //
            // <https://github.com/frontegg/python-sdk/blob/840f8318aced35cea6a41d83270597edfceb4019/frontegg/common/frontegg_authenticator.py#L45>
            let scaled_expires_in = expires_in * 0.8;
            let scaled_expires_in = Duration::from_secs_f64(scaled_expires_in);
            tracing::debug!(?id, ?expires_in, ?scaled_expires_in, "waiting to refresh");

            // Weird. The token would expire before the refresh token window.
            let refresh_in = if valid_for < scaled_expires_in {
                tracing::warn!(
                    ?id,
                    ?valid_for,
                    ?scaled_expires_in,
                    "refresh after token expires"
                );
                valid_for.saturating_sub(Duration::from_secs(10))
            } else {
                scaled_expires_in
            };

            // While we wait for the refresh, continuously check if our listeners went away.
            tokio::select! {
                // Waiting on a tokio::Sleep is cancel safe.
                _ = tokio::time::sleep(refresh_in) => (),
                // Checking if the channel closed is cancel safe.
                _ = refresh_tx.closed() => {
                    tracing::debug!(?id, "channel closed, exiting");
                    break;
                },
            }

            let current_response = client
                .refresh_token(&refresh_url, &latest_response.refresh_token, &metrics)
                .await;

            // If we failed to refresh, then close the channel.
            let current_response = match current_response {
                Ok(resp) => {
                    tracing::debug!(?id, "refresh successful");
                    resp
                }
                Err(err) => {
                    tracing::warn!(?id, ?err, "failed to refresh");

                    // Dropping the channel will close it.
                    drop(refresh_tx);
                    return;
                }
            };

            latest_response = current_response;
        }

        gauge.dec();
        tracing::debug!(?id, "shutting down refresh task");
    });

    id
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
    /// The tenant id of the authenticated user.
    pub tenant_id: Uuid,
    /// Whether the authenticated user is an administrator.
    pub is_admin: bool,
    // Prevent construction outside of `Authentication::validate_access_token`.
    _private: (),
}
