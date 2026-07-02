// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS configuration for sources and sinks.

use std::future::Future;
use std::time::{Duration, SystemTime};

use anyhow::{Context, anyhow, bail};
use aws_config::sts::AssumeRoleProvider;
use aws_credential_types::Credentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sdk_sts::error::SdkError;
use aws_sdk_sts::operation::get_caller_identity::GetCallerIdentityError;
use aws_types::SdkConfig;
use aws_types::region::Region;
use mz_dyncfg::ConfigSet;
use mz_ore::error::ErrorExt;
use mz_ore::future::{InTask, OreFutureExt};
use mz_ore::retry::{Retry, RetryResult};
use mz_ore::task::AbortOnDropHandle;
use mz_repr::{CatalogItemId, GlobalId};
#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::watch;
use tracing::{debug, warn};

use crate::AlterCompatible;
use crate::connections::inline::{
    ConnectionAccess, ConnectionResolver, InlinedConnection, IntoInlineConnection,
    ReferencedConnection,
};
use crate::controller::AlterError;
use crate::dyncfgs;
use crate::{
    configuration::StorageConfiguration,
    connections::{ConnectionContext, StringOrSecret},
};

/// AWS connection configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct AwsConnection {
    pub auth: AwsAuth,
    /// The AWS region to use.
    ///
    /// Uses the default region (looking at env vars, config files, etc) if not
    /// provided.
    pub region: Option<String>,
    /// The custom AWS endpoint to use, if any.
    pub endpoint: Option<String>,
}

impl AlterCompatible for AwsConnection {
    fn alter_compatible(&self, _id: GlobalId, _other: &Self) -> Result<(), AlterError> {
        // Every element of the AWS connection is configurable.
        Ok(())
    }
}

/// Describes how to authenticate with AWS.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub enum AwsAuth {
    /// Authenticate with an access key.
    Credentials(AwsCredentials),
    //// Authenticate via assuming an IAM role.
    AssumeRole(AwsAssumeRole),
}

/// AWS credentials to access an AWS account using user access keys.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct AwsCredentials {
    /// The AWS API Access Key required to connect to the AWS account.
    pub access_key_id: StringOrSecret,
    /// The Secret Access Key required to connect to the AWS account.
    pub secret_access_key: CatalogItemId,
    /// Optional session token to connect to the AWS account.
    pub session_token: Option<StringOrSecret>,
}

impl AwsCredentials {
    /// Loads a credentials provider with the configured credentials.
    async fn load_credentials_provider(
        &self,
        connection_context: &ConnectionContext,
        // Whether or not to do IO in a separate Tokio task.
        in_task: InTask,
    ) -> Result<impl ProvideCredentials + use<>, anyhow::Error> {
        let secrets_reader = &connection_context.secrets_reader;
        Ok(Credentials::from_keys(
            self.access_key_id
                // We will already be contained within a tokio task from `load_sdk_config`.
                .get_string(in_task, secrets_reader)
                .await
                .map_err(|_| {
                    anyhow!("internal error: failed to read access key ID from secret store")
                })?,
            connection_context
                .secrets_reader
                .read_string(self.secret_access_key)
                .await
                .map_err(|_| {
                    anyhow!("internal error: failed to read secret access key from secret store")
                })?,
            match &self.session_token {
                Some(t) => {
                    let t = t.get_string(in_task, secrets_reader).await.map_err(|_| {
                        anyhow!("internal error: failed to read session token from secret store")
                    })?;
                    Some(t)
                }
                None => None,
            },
        ))
    }
}

/// Describes an AWS IAM role to assume.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(any(test, feature = "proptest"), derive(Arbitrary))]
pub struct AwsAssumeRole {
    /// The Amazon Resource Name of the role to assume.
    pub arn: String,
    /// The optional session name for the session.
    pub session_name: Option<String>,
}

impl AwsAssumeRole {
    /// Loads a credentials provider that will assume the specified role
    /// with the appropriate external ID.
    async fn load_credentials_provider(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
    ) -> Result<impl ProvideCredentials + use<>, anyhow::Error> {
        let external_id = self
            .external_id(connection_context, connection_id)
            .with_context(|| {
                format!(
                    "failed to compute external ID for AWS AssumeRole connection {} (role arn {})",
                    connection_id, self.arn
                )
            })?;
        // It's okay to use `dangerously_load_credentials_provider` here, as
        // this is the method that provides a safe wrapper by forcing use of the
        // correct external ID.
        self.dangerously_load_credentials_provider(
            connection_context,
            connection_id,
            Some(external_id),
        )
        .await
    }

    /// Returns a provider that serves cached credentials for this role, kept
    /// fresh by a background task that assumes the role ahead of expiry.
    ///
    /// The caller's path never performs network IO: building the provider
    /// chain and every fetch happen on the background task. Callers of the
    /// returned provider wait only while the first fetch is still in flight,
    /// and get an error once fetching has failed and no unexpired credentials
    /// are cached. The background task stops when the returned provider and
    /// all clones of it are dropped.
    ///
    /// `diagnostic` labels the background task and its log lines.
    ///
    /// Panics when called outside a tokio runtime context.
    pub fn prefetch_credentials(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
        config: CredentialPrefetchConfig,
        diagnostic: String,
    ) -> SharedCredentialsProvider {
        let assume_role = self.clone();
        let connection_context = connection_context.clone();
        let build_provider = async move {
            let provider = assume_role
                .load_credentials_provider(&connection_context, connection_id)
                .await
                .with_context(|| {
                    format!(
                        "failed to initialize AssumeRole credential provider for \
                         connection {} (role arn {})",
                        connection_id, assume_role.arn
                    )
                })?;
            Ok(SharedCredentialsProvider::new(provider))
        };
        SharedCredentialsProvider::new(CredentialPrefetcher::new(
            build_provider,
            config,
            diagnostic,
        ))
    }

    /// DANGEROUS: only for internal use!
    ///
    /// Like `load_credentials_provider`, but accepts an arbitrary external ID.
    /// Only for use in the internal implementation of AWS connections. Using
    /// this method incorrectly can result in violating our AWS security
    /// requirements.
    async fn dangerously_load_credentials_provider(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
        external_id: Option<String>,
    ) -> Result<impl ProvideCredentials + use<>, anyhow::Error> {
        let Some(aws_connection_role_arn) = &connection_context.aws_connection_role_arn else {
            bail!(
                "internal error: no AWS connection role configured while loading AssumeRole \
                 credentials for connection {} (role arn {})",
                connection_id,
                self.arn
            );
        };

        // Load the default SDK configuration to use for the assume role
        // operations themselves.
        let assume_role_sdk_config = mz_aws_util::defaults().load().await;

        // The default session name identifies the environment and the
        // connection.
        let default_session_name =
            format!("{}-{}", &connection_context.environment_id, connection_id);

        // First we create a credentials provider that will assume the "jump
        // role" provided to this Materialize environment. This is the role that
        // we've told the end user to allow in their role trust policy. No need
        // to specify the external ID here as we're still within the Materialize
        // sphere of trust. The ambient AWS credentials provided to this
        // environment will be provided via the default credentials change and
        // allow us to assume the jump role. We always use the default session
        // name here, so that we can identify the specific environment and
        // connection ID that initiated the session in our internal CloudTrail
        // logs. This session isn't visible to the end user.
        let jump_credentials = AssumeRoleProvider::builder(aws_connection_role_arn)
            .configure(&assume_role_sdk_config)
            .session_name(default_session_name.clone())
            .build()
            .await;

        // Then we create the provider that will assume the end user's role.
        // Here, we *must* install the external ID, as we're using the jump role
        // to hop into the end user's AWS account, and the external ID is the
        // only thing that allows them to limit their trust of the jump role to
        // this specific Materialize environment and AWS connection. We also
        // respect the user's configured session name, if any, as this is the
        // session that will be visible to them.
        let mut credentials = AssumeRoleProvider::builder(&self.arn)
            .configure(&assume_role_sdk_config)
            .session_name(self.session_name.clone().unwrap_or(default_session_name));
        if let Some(external_id) = external_id {
            credentials = credentials.external_id(external_id);
        }
        Ok(credentials.build_from_provider(jump_credentials).await)
    }

    pub fn external_id(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
    ) -> Result<String, anyhow::Error> {
        let Some(aws_external_id_prefix) = &connection_context.aws_external_id_prefix else {
            bail!("internal error: no AWS external ID prefix configured");
        };
        Ok(format!("mz_{}_{}", aws_external_id_prefix, connection_id))
    }

    pub fn example_trust_policy(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
    ) -> Result<serde_json::Value, anyhow::Error> {
        let Some(aws_connection_role_arn) = &connection_context.aws_connection_role_arn else {
            bail!("internal error: no AWS connection role configured");
        };
        Ok(json!(
            {
            "Version": "2012-10-17",
            "Statement": [
              {
                "Effect": "Allow",
                "Principal": {
                  "AWS": aws_connection_role_arn
                },
                "Action": "sts:AssumeRole",
                "Condition": {
                  "StringEquals": {
                    "sts:ExternalId": self.external_id(connection_context, connection_id)?
                  }
                }
              }
            ]
          }
        ))
    }
}

impl AwsConnection {
    /// Returns a string describing the authentication method of this connection, for use in error messages.
    pub(crate) fn auth_method(&self) -> &'static str {
        match self.auth {
            AwsAuth::Credentials(_) => "credentials",
            AwsAuth::AssumeRole(_) => "assume_role",
        }
    }

    /// Loads the AWS SDK configuration with the configuration specified on this
    /// object.
    pub async fn load_sdk_config(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
        in_task: InTask,
        enforce_external_addresses: bool,
    ) -> Result<SdkConfig, anyhow::Error> {
        let connection_context = connection_context.clone();
        let this = self.clone();
        // This entire block is wrapped in a `run_in_task_if`, so the inner futures are
        // run in-line with `InTask::No`.
        async move {
            let credentials = match &this.auth {
                AwsAuth::Credentials(credentials) => SharedCredentialsProvider::new(
                    credentials
                        .load_credentials_provider(&connection_context, InTask::No)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to load static AWS credentials for connection {}",
                                connection_id
                            )
                        })?,
                ),
                AwsAuth::AssumeRole(assume_role) => SharedCredentialsProvider::new(
                    assume_role
                        .load_credentials_provider(&connection_context, connection_id)
                        .await
                        .with_context(|| {
                            format!(
                                "failed to initialize AssumeRole credential provider for \
                                 connection {} (role arn {})",
                                connection_id, assume_role.arn
                            )
                        })?,
                ),
            };
            this.load_sdk_config_from_credentials(credentials, enforce_external_addresses)
                .await
                .with_context(|| {
                    format!(
                        "failed to build AWS SDK config for connection {} \
                         (auth method: {}, region: {:?}, endpoint: {:?})",
                        connection_id,
                        this.auth_method(),
                        this.region,
                        this.endpoint
                    )
                })
        }
        .run_in_task_if(in_task, || "load_sdk_config".to_string())
        .await
    }

    async fn load_sdk_config_from_credentials(
        &self,
        credentials: impl ProvideCredentials + 'static,
        enforce_external_addresses: bool,
    ) -> Result<SdkConfig, anyhow::Error> {
        let mut loader = mz_aws_util::defaults().credentials_provider(credentials);
        if let Some(region) = &self.region {
            loader = loader.region(Region::new(region.clone()));
        }
        if let Some(endpoint) = &self.endpoint {
            // The custom DNS resolver wired into the AWS HTTP client is only
            // invoked for hostnames; IP-literal endpoints (e.g. `http://127.0.0.1`)
            // bypass it. Validate IP literals here so they cannot circumvent
            // the global-address enforcement.
            if enforce_external_addresses && let Ok(url) = url::Url::parse(endpoint) {
                mz_ore::netio::ensure_url_ip_global(&url)?;
            }
            loader = loader.http_client(mz_aws_util::http_client_with_resolver(
                enforce_external_addresses,
            ));
            loader = loader.endpoint_url(endpoint);
        }
        Ok(loader.load().await)
    }

    pub(crate) async fn validate(
        &self,
        id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), AwsConnectionValidationError> {
        let enforce_external_addresses =
            crate::dyncfgs::ENFORCE_EXTERNAL_ADDRESSES.get(storage_configuration.config_set());
        let aws_config = self
            .load_sdk_config(
                &storage_configuration.connection_context,
                id,
                // We are in a normal tokio context during validation, already.
                InTask::No,
                enforce_external_addresses,
            )
            .await?;
        let sts_client = aws_sdk_sts::Client::new(&aws_config);
        let _ = sts_client.get_caller_identity().send().await?;

        if let AwsAuth::AssumeRole(assume_role) = &self.auth {
            // Per AWS's recommendation, when validating a connection using
            // `AssumeRole` authentication, we should ensure that the
            // role rejects `AssumeRole` requests that don't specify an
            // external ID.
            let external_id = None;
            let credentials = assume_role
                .dangerously_load_credentials_provider(
                    &storage_configuration.connection_context,
                    id,
                    external_id,
                )
                .await?;
            let aws_config = self
                .load_sdk_config_from_credentials(credentials, enforce_external_addresses)
                .await?;
            let sts_client = aws_sdk_sts::Client::new(&aws_config);
            if sts_client.get_caller_identity().send().await.is_ok() {
                return Err(AwsConnectionValidationError::RoleDoesNotRequireExternalId {
                    role_arn: assume_role.arn.clone(),
                });
            }
        }

        Ok(())
    }

    pub(crate) fn validate_by_default(&self) -> bool {
        false
    }
}

/// An error returned by `AwsConnection::validate`.
#[derive(thiserror::Error, Debug)]
pub enum AwsConnectionValidationError {
    #[error("role trust policy does not require an external ID")]
    RoleDoesNotRequireExternalId { role_arn: String },
    #[error("{}", .0.display_with_causes())]
    StsGetCallerIdentityError(#[from] SdkError<GetCallerIdentityError>),
    #[error("{}", .0.display_with_causes())]
    Other(#[from] anyhow::Error),
}

impl AwsConnectionValidationError {
    /// Reports additional details about the error, if any are available.
    pub fn detail(&self) -> Option<String> {
        match self {
            AwsConnectionValidationError::RoleDoesNotRequireExternalId { role_arn } => {
                Some(format!(
                    "The trust policy for the connection's role ({role_arn}) is insecure and allows any Materialize customer to assume the role."
                ))
            }
            _ => None,
        }
    }

    /// Reports a hint for the user about how the error could be fixed.
    pub fn hint(&self) -> Option<String> {
        match self {
            AwsConnectionValidationError::RoleDoesNotRequireExternalId { .. } => {
                Some("See: https://materialize.com/s/aws-connection-role-trust-policy".into())
            }
            _ => None,
        }
    }
}

/// References an AWS connection.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct AwsConnectionReference<C: ConnectionAccess = InlinedConnection> {
    /// ID of the AWS connection.
    pub connection_id: CatalogItemId,
    /// AWS connection object.
    pub connection: C::Aws,
}

impl<R: ConnectionResolver> IntoInlineConnection<AwsConnectionReference, R>
    for AwsConnectionReference<ReferencedConnection>
{
    fn into_inline_connection(self, r: R) -> AwsConnectionReference {
        let AwsConnectionReference {
            connection,
            connection_id,
        } = self;

        AwsConnectionReference {
            connection: r.resolve_connection(connection).unwrap_aws(),
            connection_id,
        }
    }
}

/// Cadence and resilience knobs for a [`CredentialPrefetcher`].
#[derive(Debug, Clone, Copy)]
pub struct CredentialPrefetchConfig {
    /// Fraction of a credential's remaining lifetime at which to refresh it. A
    /// value of `0.5` refreshes when half of the remaining lifetime is left.
    pub refresh_fraction: f64,
    /// Upper bound on the wait between refreshes. Caps `refresh_fraction` so we
    /// still refresh periodically even for long-lived credentials.
    pub max_refresh_interval: Duration,
    /// Lower bound on the wait between refreshes. Keeps a near-expiry or
    /// failing refresh from busy-looping.
    pub min_refresh_interval: Duration,
    /// Per-attempt timeout for a single underlying credential fetch. Set
    /// generously so a momentarily starved connect does not fail the fetch. The
    /// AWS SDK default connect timeout (3.1s) is too tight under load.
    pub fetch_timeout: Duration,
    /// Attempts per fetch before giving up, for both the initial fetch and each
    /// background refresh.
    pub fetch_max_tries: usize,
}

impl CredentialPrefetchConfig {
    /// Reads the config from the `aws_credential_prefetch_*` dyncfgs.
    pub fn from_dyncfgs(configs: &ConfigSet) -> Self {
        CredentialPrefetchConfig {
            refresh_fraction: dyncfgs::AWS_CREDENTIAL_PREFETCH_REFRESH_FRACTION.get(configs),
            max_refresh_interval: dyncfgs::AWS_CREDENTIAL_PREFETCH_MAX_REFRESH_INTERVAL
                .get(configs),
            min_refresh_interval: dyncfgs::AWS_CREDENTIAL_PREFETCH_MIN_REFRESH_INTERVAL
                .get(configs),
            fetch_timeout: dyncfgs::AWS_CREDENTIAL_PREFETCH_FETCH_TIMEOUT.get(configs),
            fetch_max_tries: dyncfgs::AWS_CREDENTIAL_PREFETCH_FETCH_MAX_TRIES.get(configs),
        }
    }
}

/// A [`ProvideCredentials`] that serves credentials from an in-memory cache kept
/// warm by a background task.
///
/// Callers get the last successfully fetched credentials with no STS call on
/// their path. The background task does all fetching, including the first: a
/// call that arrives before the first fetch completes waits for it. A failed
/// refresh keeps the last-good value, so a transient STS or network failure
/// does not surface to callers as long as the cached credentials are still
/// valid. Once no unexpired credentials are cached, callers get the most
/// recent fetch error instead, so a persistent failure shows its real cause
/// rather than an AWS-side rejection of expired credentials.
///
/// Dropping the prefetcher aborts the background task.
pub struct CredentialPrefetcher {
    cache: watch::Receiver<CredentialState>,
    _refresh_task: AbortOnDropHandle<()>,
}

/// The cache shared between the background task and callers. Both fields
/// `None` means the first fetch has not completed yet.
#[derive(Debug, Clone, Default)]
struct CredentialState {
    /// The last successfully fetched credentials.
    creds: Option<Credentials>,
    /// The error of the most recent failed fetch. Cleared by a successful
    /// fetch.
    last_error: Option<String>,
}

impl std::fmt::Debug for CredentialPrefetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialPrefetcher")
            .finish_non_exhaustive()
    }
}

impl CredentialPrefetcher {
    /// Spawns the background task and returns immediately. The task awaits
    /// `build_provider` to obtain the underlying provider, then fetches
    /// through it, so all IO (including any in `build_provider`) stays off
    /// the caller's thread. A `build_provider` error is served to callers
    /// like a fetch error.
    ///
    /// `diagnostic` labels the spawned task and log lines.
    ///
    /// Panics when called outside a tokio runtime context.
    pub fn new(
        build_provider: impl Future<Output = Result<SharedCredentialsProvider, anyhow::Error>>
        + Send
        + 'static,
        config: CredentialPrefetchConfig,
        diagnostic: String,
    ) -> CredentialPrefetcher {
        let (tx, cache) = watch::channel(CredentialState::default());
        let task_name = format!("credential-prefetch:{diagnostic}");
        let refresh_task = mz_ore::task::spawn(|| task_name, async move {
            let inner = match build_provider.await {
                Ok(inner) => inner,
                Err(e) => {
                    let error = e.display_with_causes().to_string();
                    warn!(
                        %diagnostic,
                        %error,
                        "failed to build the AWS credentials provider"
                    );
                    tx.send_modify(|state| state.last_error = Some(error));
                    return;
                }
            };
            Self::refresh_loop(inner, tx, config, diagnostic).await;
        })
        .abort_on_drop();
        CredentialPrefetcher {
            cache,
            _refresh_task: refresh_task,
        }
    }

    /// Fetches a single credential through `inner`, retrying transient failures
    /// and bounding each attempt with `fetch_timeout`.
    async fn fetch(
        inner: &SharedCredentialsProvider,
        config: &CredentialPrefetchConfig,
        diagnostic: &str,
    ) -> Result<Credentials, anyhow::Error> {
        Retry::default()
            .max_tries(config.fetch_max_tries)
            .retry_async(|retry_state| {
                let attempt = retry_state.i + 1;
                async move {
                    let err = match tokio::time::timeout(
                        config.fetch_timeout,
                        inner.provide_credentials(),
                    )
                    .await
                    {
                        Ok(Ok(creds)) => return RetryResult::Ok(creds),
                        Ok(Err(e)) => anyhow::Error::new(e),
                        Err(_elapsed) => anyhow!(
                            "credential fetch timed out after {:?}",
                            config.fetch_timeout
                        ),
                    };
                    // Log every failed attempt. A transient failure that a
                    // later attempt absorbs is otherwise invisible, and those
                    // are exactly the events that show whether the underlying
                    // connectivity problem still occurs.
                    warn!(
                        %diagnostic,
                        attempt,
                        max_tries = config.fetch_max_tries,
                        error = %err.display_with_causes(),
                        "credential fetch attempt failed"
                    );
                    RetryResult::RetryableErr(err)
                }
            })
            .await
    }

    async fn refresh_loop(
        inner: SharedCredentialsProvider,
        tx: watch::Sender<CredentialState>,
        config: CredentialPrefetchConfig,
        diagnostic: String,
    ) {
        loop {
            match Self::fetch(&inner, &config, &diagnostic).await {
                Ok(creds) => {
                    let expiry = creds.expiry();
                    tx.send_modify(|state| {
                        state.creds = Some(creds);
                        state.last_error = None;
                    });
                    let wait = next_refresh_wait(tx.borrow().creds.as_ref(), &config);
                    debug!(
                        %diagnostic,
                        ?expiry,
                        next_refresh_in = ?wait,
                        "credential fetch succeeded"
                    );
                    tokio::time::sleep(wait).await;
                }
                Err(e) => {
                    // Keep the last-good credentials. As they approach expiry
                    // the computed wait shrinks toward `min_refresh_interval`,
                    // so retries speed up exactly when a refresh becomes
                    // urgent.
                    let error = e.display_with_causes().to_string();
                    let wait = next_refresh_wait(tx.borrow().creds.as_ref(), &config);
                    warn!(
                        %diagnostic,
                        %error,
                        next_retry_in = ?wait,
                        "credential fetch failed; keeping last-good credentials"
                    );
                    tx.send_modify(|state| state.last_error = Some(error));
                    tokio::time::sleep(wait).await;
                }
            }
            // Nothing holds the cache anymore (the dataflow tore down). Stop.
            if tx.is_closed() {
                return;
            }
        }
    }
}

/// Computes how long to wait before the next fetch, given the currently cached
/// credentials.
///
/// Waits `refresh_fraction` of the credentials' remaining lifetime, bounded by
/// `[min_refresh_interval, max_refresh_interval]`. Credentials with no expiry
/// wait the maximum. No credentials at all (fetching has not succeeded yet)
/// waits the minimum.
fn next_refresh_wait(creds: Option<&Credentials>, config: &CredentialPrefetchConfig) -> Duration {
    let Some(creds) = creds else {
        return config.min_refresh_interval;
    };
    let Some(expiry) = creds.expiry() else {
        return config.max_refresh_interval;
    };
    let remaining = expiry
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO);
    // Bound with `max`/`min` rather than `clamp`, which panics when a
    // misconfigured minimum exceeds the maximum. Bound the fraction too, since
    // `mul_f64` panics on negative values.
    remaining
        .mul_f64(config.refresh_fraction.clamp(0.0, 1.0))
        .max(config.min_refresh_interval)
        .min(config.max_refresh_interval)
}

impl ProvideCredentials for CredentialPrefetcher {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async move {
            let mut cache = self.cache.clone();
            loop {
                // Clone out of the `watch::Ref` so the read guard is not held
                // across an await.
                let state = cache.borrow_and_update().clone();
                match state.creds {
                    Some(creds) => {
                        return match creds.expiry() {
                            Some(expiry) if expiry <= SystemTime::now() => {
                                let cause = match state.last_error {
                                    Some(error) => {
                                        format!("most recent fetch attempt failed: {error}")
                                    }
                                    None => "no refresh has completed since".to_string(),
                                };
                                Err(CredentialsError::provider_error(format!(
                                    "cached AWS credentials expired at {expiry:?} and {cause}"
                                )))
                            }
                            _ => Ok(creds),
                        };
                    }
                    None => {
                        if let Some(error) = state.last_error {
                            return Err(CredentialsError::provider_error(format!(
                                "failed to fetch AWS credentials: {error}"
                            )));
                        }
                        // The first fetch is still in flight. Wait for it to
                        // finish, either way.
                        if cache.changed().await.is_err() {
                            return Err(CredentialsError::provider_error(
                                "credential prefetcher task stopped before producing credentials",
                            ));
                        }
                    }
                }
            }
        })
    }

    fn fallback_on_interrupt(&self) -> Option<Credentials> {
        self.cache.borrow().creds.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use aws_credential_types::provider::future;

    use super::*;

    /// A test [`ProvideCredentials`] with per-call behavior keyed on the
    /// cumulative call count `n` (starting at 1): the first `hang_calls` calls
    /// never resolve, the next `ok_calls` calls succeed, and every call after
    /// that fails. Each success returns a distinct access key id (`akid-{n}`)
    /// with `ttl` until expiry.
    #[derive(Debug)]
    struct CountingProvider {
        calls: Arc<AtomicUsize>,
        hang_calls: usize,
        ok_calls: usize,
        ttl: Duration,
    }

    impl CountingProvider {
        fn shared(
            hang_calls: usize,
            ok_calls: usize,
            ttl: Duration,
        ) -> (Arc<AtomicUsize>, SharedCredentialsProvider) {
            let calls = Arc::new(AtomicUsize::new(0));
            let provider = CountingProvider {
                calls: Arc::clone(&calls),
                hang_calls,
                ok_calls,
                ttl,
            };
            (calls, SharedCredentialsProvider::new(provider))
        }
    }

    impl ProvideCredentials for CountingProvider {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            let calls = Arc::clone(&self.calls);
            let hang_calls = self.hang_calls;
            let ok_calls = self.ok_calls;
            let ttl = self.ttl;
            future::ProvideCredentials::new(async move {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n <= hang_calls {
                    return std::future::pending().await;
                }
                if n > hang_calls.saturating_add(ok_calls) {
                    return Err(CredentialsError::provider_error("injected failure"));
                }
                Ok(Credentials::new(
                    format!("akid-{n}"),
                    "secret",
                    None,
                    Some(SystemTime::now() + ttl),
                    "test",
                ))
            })
        }
    }

    /// A config with intervals long enough that the background task never
    /// refreshes during a test. Tests override the fields they exercise.
    fn test_config() -> CredentialPrefetchConfig {
        CredentialPrefetchConfig {
            refresh_fraction: 0.5,
            max_refresh_interval: Duration::from_secs(3600),
            min_refresh_interval: Duration::from_secs(3600),
            fetch_timeout: Duration::from_secs(5),
            fetch_max_tries: 1,
        }
    }

    fn prefetcher(
        provider: SharedCredentialsProvider,
        config: CredentialPrefetchConfig,
    ) -> CredentialPrefetcher {
        CredentialPrefetcher::new(async move { Ok(provider) }, config, "test".to_string())
    }

    /// Polls `cond` until it returns true or the deadline elapses.
    async fn wait_until(mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !cond() {
            if tokio::time::Instant::now() >= deadline {
                panic!("condition not met within deadline");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn cache_serves_without_recalling_inner() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, Duration::from_secs(3600));
        let prefetcher = prefetcher(inner, test_config());

        // The first call may arrive before the background task's initial
        // fetch completes; it must wait for it rather than error.
        for _ in 0..5 {
            let creds = prefetcher.provide_credentials().await.unwrap();
            assert_eq!(creds.access_key_id(), "akid-1");
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "cached reads must not call the inner provider"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn background_refresh_advances_the_value() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, Duration::from_secs(60));
        let config = CredentialPrefetchConfig {
            max_refresh_interval: Duration::from_millis(50),
            min_refresh_interval: Duration::from_millis(20),
            ..test_config()
        };
        let prefetcher = prefetcher(inner, config);

        wait_until(|| calls.load(Ordering::SeqCst) >= 2).await;
        let creds = prefetcher.provide_credentials().await.unwrap();
        assert_ne!(
            creds.access_key_id(),
            "akid-1",
            "a refresh should have replaced the initial credentials"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn failed_refresh_keeps_last_good() {
        // Only the initial fetch succeeds; every refresh fails. The ttl is
        // long enough that the credentials stay unexpired for the whole test.
        let (calls, inner) = CountingProvider::shared(0, 1, Duration::from_secs(60));
        let config = CredentialPrefetchConfig {
            max_refresh_interval: Duration::from_millis(50),
            min_refresh_interval: Duration::from_millis(20),
            ..test_config()
        };
        let prefetcher = prefetcher(inner, config);

        // Wait for at least one refresh attempt to have happened (and failed).
        wait_until(|| calls.load(Ordering::SeqCst) >= 2).await;
        let creds = prefetcher.provide_credentials().await.unwrap();
        assert_eq!(
            creds.access_key_id(),
            "akid-1",
            "a failed refresh must keep serving the last-good credentials"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn dropping_prefetcher_stops_the_task() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, Duration::from_secs(60));
        let config = CredentialPrefetchConfig {
            max_refresh_interval: Duration::from_millis(50),
            min_refresh_interval: Duration::from_millis(20),
            ..test_config()
        };
        let prefetcher = prefetcher(inner, config);

        wait_until(|| calls.load(Ordering::SeqCst) >= 2).await;
        drop(prefetcher);

        // Let any in-flight fetch settle, then confirm no further fetches occur.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let after_drop = calls.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            after_drop,
            "the refresh task must stop once the prefetcher is dropped"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn fetch_timeout_is_retried() {
        // The first call hangs; the retry succeeds. This is the shape of a
        // hung connect to STS.
        let (calls, inner) = CountingProvider::shared(1, usize::MAX, Duration::from_secs(60));
        let config = CredentialPrefetchConfig {
            fetch_timeout: Duration::from_millis(50),
            fetch_max_tries: 2,
            ..test_config()
        };
        let prefetcher = prefetcher(inner, config);

        let creds = prefetcher.provide_credentials().await.unwrap();
        assert_eq!(
            creds.access_key_id(),
            "akid-2",
            "the timed-out attempt must be retried"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[mz_ore::test(tokio::test)]
    async fn initial_fetch_failure_returns_error() {
        // Every fetch fails, so callers must get the fetch error rather than
        // wait forever.
        let (_calls, inner) = CountingProvider::shared(0, 0, Duration::from_secs(60));
        let prefetcher = prefetcher(inner, test_config());

        let err = prefetcher.provide_credentials().await.unwrap_err();
        assert!(
            format!("{}", err.display_with_causes()).contains("injected failure"),
            "the error must carry the fetch failure cause: {err}"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn expired_credentials_return_error() {
        // One successful fetch with a tiny ttl, then only failures.
        let (calls, inner) = CountingProvider::shared(0, 1, Duration::from_millis(30));
        let config = CredentialPrefetchConfig {
            max_refresh_interval: Duration::from_millis(50),
            min_refresh_interval: Duration::from_millis(20),
            ..test_config()
        };
        let prefetcher = prefetcher(inner, config);

        // Wait until the credentials are expired and a refresh has failed.
        wait_until(|| calls.load(Ordering::SeqCst) >= 2).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let err = prefetcher.provide_credentials().await.unwrap_err();
        let msg = format!("{}", err.display_with_causes());
        assert!(msg.contains("expired"), "{msg}");
        assert!(
            msg.contains("injected failure"),
            "the error must carry the refresh failure cause: {msg}"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn provider_build_failure_returns_error() {
        let prefetcher = CredentialPrefetcher::new(
            async { Err(anyhow!("role chain unavailable")) },
            test_config(),
            "test".to_string(),
        );

        let err = prefetcher.provide_credentials().await.unwrap_err();
        assert!(
            format!("{}", err.display_with_causes()).contains("role chain unavailable"),
            "the error must carry the build failure cause: {err}"
        );
    }

    #[mz_ore::test]
    fn refresh_wait_schedule() {
        let config = CredentialPrefetchConfig {
            refresh_fraction: 0.5,
            max_refresh_interval: Duration::from_secs(900),
            min_refresh_interval: Duration::from_secs(30),
            fetch_timeout: Duration::from_secs(30),
            fetch_max_tries: 5,
        };

        // No credentials yet: retry at the floor.
        assert_eq!(next_refresh_wait(None, &config), Duration::from_secs(30));

        // No expiry: refresh at the cap.
        let creds = Credentials::new("akid", "secret", None, None, "test");
        assert_eq!(
            next_refresh_wait(Some(&creds), &config),
            Duration::from_secs(900)
        );

        // Mid-life: the configured fraction of the remaining lifetime.
        let creds = Credentials::new(
            "akid",
            "secret",
            None,
            Some(SystemTime::now() + Duration::from_secs(600)),
            "test",
        );
        let wait = next_refresh_wait(Some(&creds), &config);
        assert!(
            wait > Duration::from_secs(290) && wait <= Duration::from_secs(300),
            "{wait:?}"
        );

        // Expired: retry at the floor.
        let creds = Credentials::new(
            "akid",
            "secret",
            None,
            Some(SystemTime::now() - Duration::from_secs(10)),
            "test",
        );
        assert_eq!(
            next_refresh_wait(Some(&creds), &config),
            Duration::from_secs(30)
        );
    }
}
