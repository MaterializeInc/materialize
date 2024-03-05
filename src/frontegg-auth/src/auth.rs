// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use derivative::Derivative;
use futures::future::Shared;
use futures::FutureExt;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use lru::LruCache;
use mz_ore::instrument;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::time::DurationExt;
use mz_repr::user::ExternalUserMetadata;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::time;
use uuid::Uuid;

use crate::metrics::Metrics;
use crate::{ApiTokenArgs, AppPassword, Client, Error, FronteggCliArgs};

/// SAFETY: Value is known to be non-zero.
pub const DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE: NonZeroUsize =
    unsafe { NonZeroUsize::new_unchecked(1024) };

/// If a session is dropped within [`DEFAULT_REFRESH_DROP_FACTOR`] `* valid_for` seconds of an
/// authentication token expiring, then we'll continue to refresh the auth token, with the
/// assumption that a new instance of this session will be started soon.
pub const DEFAULT_REFRESH_DROP_FACTOR: f64 = 0.05;

/// Configures an [`Authenticator`].
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct AuthenticatorConfig {
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
    /// How many [`AppPassword`]s we'll track the last dropped time for.
    ///
    /// TODO(parkmycar): Wire this up to LaunchDarkly.
    pub refresh_drop_lru_size: NonZeroUsize,
    /// How large of a window we'll use for determining if a session was dropped "recently", and if
    /// we should refresh the session, even if there are not any active handles to it.
    ///
    /// TODO(parkmycar): Wire this up to LaunchDarkly.
    pub refresh_drop_factor: f64,
}

/// Facilitates authenticating users via Frontegg, and verifying returned JWTs.
#[derive(Clone, Debug)]
pub struct Authenticator {
    inner: Arc<AuthenticatorInner>,
}

impl Authenticator {
    /// Creates a new authenticator.
    pub fn new(config: AuthenticatorConfig, client: Client, registry: &MetricsRegistry) -> Self {
        let mut validation = Validation::new(Algorithm::RS256);

        // We validate the token expiration with our own now function.
        validation.validate_exp = false;

        // We don't validate the audience because:
        //
        //   1. We don't have easy access to the expected audience ID here.
        //
        //   2. There is no meaningful security improvement to doing so, because
        //      Frontegg always sets the audience to the ID of the workspace
        //      that issued the token. Since we only trust the signing keys from
        //      a single Frontegg workspace, the audience is redundant.
        //
        // See this conversation [0] from the Materialize–Frontegg shared Slack
        // channel on 1 January 2024.
        //
        // [0]: https://materializeinc.slack.com/archives/C02940WNMRQ/p1704131331041669
        validation.validate_aud = false;

        let metrics = Metrics::register_into(registry);
        let active_sessions = Mutex::new(BTreeMap::new());
        let dropped_sessions = Mutex::new(LruCache::new(config.refresh_drop_lru_size));

        Authenticator {
            inner: Arc::new(AuthenticatorInner {
                admin_api_token_url: config.admin_api_token_url,
                client,
                validation,
                decoding_key: config.decoding_key,
                tenant_id: config.tenant_id,
                admin_role: config.admin_role,
                now: config.now,
                active_sessions,
                dropped_sessions,
                refresh_drop_factor: config.refresh_drop_factor,
                metrics,
            }),
        }
    }

    /// Create an [`Authenticator`] from [`FronteggCliArgs`].
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
                AuthenticatorConfig {
                    admin_api_token_url,
                    decoding_key,
                    tenant_id: Some(tenant_id),
                    now: mz_ore::now::SYSTEM_TIME.clone(),
                    admin_role,
                    refresh_drop_lru_size: DEFAULT_REFRESH_DROP_LRU_CACHE_SIZE,
                    refresh_drop_factor: DEFAULT_REFRESH_DROP_FACTOR,
                }
            }
            _ => unreachable!("clap enforced"),
        };
        let client = Client::environmentd_default();

        Ok(Some(Self::new(config, client, registry)))
    }

    /// Establishes a new authentication session.
    ///
    /// If successful, returns a handle to the authentication session.
    /// Otherwise, returns the authentication error.
    pub async fn authenticate(
        &self,
        expected_email: &str,
        password: &str,
    ) -> Result<AuthSessionHandle, Error> {
        let password: AppPassword = password.parse()?;
        match self.authenticate_inner(expected_email, password).await {
            Ok(handle) => {
                tracing::debug!("authentication successful");
                Ok(handle)
            }
            Err(e) => {
                tracing::debug!(error = ?e, "authentication failed");
                Err(e)
            }
        }
    }

    #[instrument(level = "debug", fields(client_id = %password.client_id))]
    async fn authenticate_inner(
        &self,
        expected_email: &str,
        password: AppPassword,
    ) -> Result<AuthSessionHandle, Error> {
        let request = {
            let mut sessions = self.inner.active_sessions.lock().expect("lock poisoned");
            match sessions.get_mut(&password) {
                // We have an existing session for this app password.
                Some(AuthSession::Active {
                    ident,
                    external_metadata_tx,
                    ..
                }) => {
                    tracing::debug!(?password.client_id, "joining active session");

                    validate_email(&ident.user, expected_email)?;
                    self.inner
                        .metrics
                        .session_request_count
                        .with_label_values(&["active"])
                        .inc();

                    // Return a handle to the existing session.
                    return Ok(AuthSessionHandle {
                        ident: Arc::clone(ident),
                        external_metadata_rx: external_metadata_tx.subscribe(),
                        authenticator: Arc::clone(&self.inner),
                        app_password: password,
                    });
                }

                // We have an in flight request to establish a session.
                Some(AuthSession::Pending(request)) => {
                    // Latch on to the existing session.
                    tracing::debug!(?password.client_id, "joining pending session");
                    self.inner
                        .metrics
                        .session_request_count
                        .with_label_values(&["pending"])
                        .inc();
                    request.clone()
                }

                // We do not have an existing session for this API key.
                None => {
                    tracing::debug!(?password.client_id, "starting new session");

                    // Prepare the request to create a new session.
                    let request: Pin<Box<AuthFuture>> = Box::pin({
                        let inner = Arc::clone(&self.inner);
                        let expected_email = String::from(expected_email);
                        async move { inner.authenticate(expected_email, password).await }
                    });

                    // Store the future so that future requests can latch on.
                    let request = request.shared();
                    sessions.insert(password, AuthSession::Pending(request.clone()));
                    self.inner
                        .metrics
                        .session_request_count
                        .with_label_values(&["new"])
                        .inc();

                    // Wait for the request to complete.
                    request
                }
            }
        };
        request.await
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
        self.inner.validate_access_token(token, expected_email)
    }
}

/// A handle to an authentication session.
///
/// An authentication session represents a duration of time during which a
/// user's authentication is known to be valid.
///
/// An authentication session begins with a successful API key exchange with
/// Frontegg. While there is at least one outstanding handle to the session, the
/// session's metadata and validity are refreshed with Frontegg at a regular
/// interval. The session ends when all outstanding handles are dropped and the
/// refresh interval is reached.
///
/// [`AuthSessionHandle::external_metadata_rx`] can be used to receive events if
/// the session's metadata is updated.
///
/// [`AuthSessionHandle::expired`] can be used to learn if the session has
/// failed to refresh the validity of the API key.
#[derive(Debug, Clone)]
pub struct AuthSessionHandle {
    ident: Arc<AuthSessionIdent>,
    external_metadata_rx: watch::Receiver<ExternalUserMetadata>,
    /// Hold a handle to the [`AuthenticatorInner`] so we can record when this session was dropped.
    authenticator: Arc<AuthenticatorInner>,
    /// Used to record when the session linked with this [`AppPassword`] was dropped.
    app_password: AppPassword,
}

impl AuthSessionHandle {
    /// Returns the name of the user that created the session.
    pub fn user(&self) -> &str {
        &self.ident.user
    }

    /// Returns the ID of the tenant that created the session.
    pub fn tenant_id(&self) -> Uuid {
        self.ident.tenant_id
    }

    /// Mints a receiver for updates to the session user's external metadata.
    pub fn external_metadata_rx(&self) -> watch::Receiver<ExternalUserMetadata> {
        self.external_metadata_rx.clone()
    }

    /// Completes when the authentication session has expired.
    pub async fn expired(&mut self) {
        // We piggyback on the external metadata channel to determine session
        // expiration. The external metadata channel is closed when the session
        // expires.
        let _ = self.external_metadata_rx.wait_for(|_| false).await;
    }
}

impl Drop for AuthSessionHandle {
    fn drop(&mut self) {
        self.authenticator.record_dropped_session(self.app_password);
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
struct AuthenticatorInner {
    /// Frontegg API fields.
    admin_api_token_url: String,
    client: Client,
    /// JWT decoding and validation fields.
    validation: Validation,
    #[derivative(Debug = "ignore")]
    decoding_key: DecodingKey,
    tenant_id: Option<Uuid>,
    admin_role: String,
    now: NowFn,
    /// Session tracking.
    active_sessions: Mutex<BTreeMap<AppPassword, AuthSession>>,
    /// Most recent time at which a session created with an [`AppPassword`] was dropped.
    ///
    /// We track when a session was dropped to handle the case of many one-shot queries being
    /// issued in rapid succession. If it comes time to refresh an auth token, and there are no
    /// currently alive sessions, but one was recently dropped, we'll pre-emptively refresh to get
    /// ahead of another session being created with the same [`AppPassword`].
    dropped_sessions: Mutex<LruCache<AppPassword, Instant>>,
    /// How large of a window we'll use for determining if a session was dropped "recently", and if
    /// we should refresh the session, even if there are not any active handles to it.
    refresh_drop_factor: f64,
    /// Metrics.
    metrics: Metrics,
}

impl AuthenticatorInner {
    async fn authenticate(
        self: &Arc<Self>,
        expected_email: String,
        password: AppPassword,
    ) -> Result<AuthSessionHandle, Error> {
        // Attempt initial app password exchange.
        let mut claims = self
            .exchange_app_password(&expected_email, password)
            .await?;

        // Prep session information.
        let ident = Arc::new(AuthSessionIdent {
            user: claims.email.clone(),
            tenant_id: claims.tenant_id,
        });
        let external_metadata = claims.to_external_user_metadata();
        let (external_metadata_tx, external_metadata_rx) = watch::channel(external_metadata);
        let external_metadata_tx = Arc::new(external_metadata_tx);

        // Store session to make it available for future requests to latch on
        // to.
        {
            let mut sessions = self.active_sessions.lock().expect("lock poisoned");
            sessions.insert(
                password,
                AuthSession::Active {
                    ident: Arc::clone(&ident),
                    external_metadata_tx: Arc::clone(&external_metadata_tx),
                },
            );
        }

        // Start background refresh task.
        let name = format!("frontegg-auth-refresh-{}", password.client_id);
        mz_ore::task::spawn(|| name, {
            let inner = Arc::clone(self);
            async move {
                tracing::debug!(?password.client_id, "starting refresh task");
                let gauge = inner.metrics.refresh_tasks_active.with_label_values(&[]);
                gauge.inc();

                loop {
                    let valid_for = Duration::try_from_secs_i64(claims.exp - inner.now.as_secs())
                        .unwrap_or(Duration::from_secs(60));

                    // If we have no outstanding handling to this session, but a handle was dropped
                    // within this window, then we'll still refresh.
                    let drop_window = valid_for
                        .saturating_mul_f64(inner.refresh_drop_factor)
                        .max(Duration::from_secs(1));
                    // Scale the validity duration by 0.8. The Frontegg Python
                    // SDK scales the expires_in this way.
                    //
                    // <https://github.com/frontegg/python-sdk/blob/840f8318aced35cea6a41d83270597edfceb4019/frontegg/common/frontegg_authenticator.py#L45>
                    let valid_for = valid_for.saturating_mul_f64(0.8);

                    if valid_for < Duration::from_secs(60) {
                        tracing::warn!(?valid_for, "unexpectedly low token validity");
                    }

                    tracing::debug!(
                        ?valid_for,
                        ?drop_window,
                        "waiting for token validity period"
                    );

                    // Wait out validity duration.
                    time::sleep(valid_for).await;

                    // Check to see if all external metadata receivers have gone away, or if a
                    // session created with this password was recently dropped. If no one is
                    // listening nor any recent handles were dropped we can clean up the session.
                    let receiver_count = external_metadata_tx.receiver_count();
                    let last_drop = inner.last_dropped_session(&password);
                    let recent_drop = last_drop
                        .map(|dropped_at| dropped_at.elapsed() <= drop_window)
                        .unwrap_or(false);
                    if receiver_count == 0 && !recent_drop {
                        tracing::debug!(
                            ?last_drop,
                            ?password.client_id,
                            "all listeners have dropped and none of them were recent!"
                        );
                        break;
                    }

                    let outstanding_receivers = bool_as_str(receiver_count > 0);
                    inner
                        .metrics
                        .session_refresh_count
                        .with_label_values(&[outstanding_receivers, bool_as_str(recent_drop)])
                        .inc();
                    tracing::debug!(
                        receiver_count,
                        ?last_drop,
                        ?password.client_id,
                        "refreshing due to interest in the session"
                    );

                    // We still have interest, attempt to refresh the session.
                    let res = inner.exchange_app_password(&expected_email, password).await;
                    claims = match res {
                        Ok(claims) => {
                            tracing::debug!("refresh successful");
                            claims
                        }
                        Err(e) => {
                            tracing::warn!(error = ?e, "refresh failed");
                            break;
                        }
                    };
                    external_metadata_tx.send_replace(ExternalUserMetadata {
                        admin: claims.is_admin,
                        user_id: claims.user_id,
                    });
                }

                // The session has expired. Clean up the state.
                {
                    let mut sessions = inner.active_sessions.lock().expect("lock poisoned");
                    sessions.remove(&password);
                }
                {
                    let mut dropped_session = inner.dropped_sessions.lock().expect("lock poisoned");
                    dropped_session.pop(&password);
                }

                tracing::debug!(?password.client_id, "shutting down refresh task");
                gauge.dec();
            }
        });

        // Return handle to session.
        Ok(AuthSessionHandle {
            ident,
            external_metadata_rx,
            authenticator: Arc::clone(self),
            app_password: password,
        })
    }

    #[instrument]
    async fn exchange_app_password(
        &self,
        expected_email: &str,
        password: AppPassword,
    ) -> Result<ValidatedClaims, Error> {
        let req = ApiTokenArgs {
            client_id: password.client_id,
            secret: password.secret_key,
        };
        let res = self
            .client
            .exchange_client_secret_for_token(req, &self.admin_api_token_url, &self.metrics)
            .await?;
        self.validate_access_token(&res.access_token, Some(expected_email))
    }

    fn validate_access_token(
        &self,
        token: &str,
        expected_email: Option<&str>,
    ) -> Result<ValidatedClaims, Error> {
        let msg = jsonwebtoken::decode::<Claims>(token, &self.decoding_key, &self.validation)?;
        if msg.claims.exp < self.now.as_secs() {
            return Err(Error::TokenExpired);
        }
        if let Some(expected_tenant_id) = self.tenant_id {
            if msg.claims.tenant_id != expected_tenant_id {
                return Err(Error::UnauthorizedTenant);
            }
        }
        if let Some(expected_email) = expected_email {
            validate_email(&msg.claims.email, expected_email)?;
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
            is_admin: msg.claims.roles.iter().any(|r| *r == self.admin_role),
            _private: (),
        })
    }

    /// Records an [`AuthSessionHandle`] that was recently dropped.
    fn record_dropped_session(&self, app_password: AppPassword) {
        let now = Instant::now();
        let Ok(mut dropped_sessions) = self.dropped_sessions.lock() else {
            return;
        };
        dropped_sessions.push(app_password, now);
    }

    /// Returns the instant that an [`AuthSessionHandle`] created with the provided [`AppPassword`]
    /// was last dropped.
    fn last_dropped_session(&self, app_password: &AppPassword) -> Option<Instant> {
        let Ok(dropped_sessions) = self.dropped_sessions.lock() else {
            return None;
        };
        dropped_sessions.peek(app_password).copied()
    }
}

type AuthFuture = dyn Future<Output = Result<AuthSessionHandle, Error>> + Send;

#[derive(Derivative)]
#[derivative(Debug)]
enum AuthSession {
    Pending(Shared<Pin<Box<AuthFuture>>>),
    Active {
        ident: Arc<AuthSessionIdent>,
        external_metadata_tx: Arc<watch::Sender<ExternalUserMetadata>>,
    },
}

#[derive(Debug)]
struct AuthSessionIdent {
    user: String,
    tenant_id: Uuid,
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
    pub iss: String,
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

impl ValidatedClaims {
    /// Constructs an [`ExternalUserMetadata`] from the claims data.
    fn to_external_user_metadata(&self) -> ExternalUserMetadata {
        ExternalUserMetadata {
            admin: self.is_admin,
            user_id: self.user_id,
        }
    }
}

fn validate_email(email: &str, expected_email: &str) -> Result<(), Error> {
    // To match Frontegg, email addresses are compared case insensitively.
    //
    // NOTE(benesch): we could save some allocations by using `unicase::eq`
    // here, but the `unicase` crate has had some critical correctness bugs that
    // make it scary to use in such security-sensitive code.
    //
    // See: https://github.com/seanmonstar/unicase/pull/39
    if email.to_lowercase() != expected_email.to_lowercase() {
        return Err(Error::WrongEmail);
    }
    Ok(())
}

const fn bool_as_str(x: bool) -> &'static str {
    if x {
        "true"
    } else {
        "false"
    }
}
