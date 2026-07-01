// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS configuration for sources and sinks.

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

    /// Builds a [`CredentialPrefetcher`] over this connection's credential chain
    /// and returns it as a [`SharedCredentialsProvider`].
    ///
    /// The initial fetch happens before this returns, so the resulting provider
    /// serves credentials immediately. A background task keeps them fresh until
    /// the provider is dropped.
    pub async fn prefetch_credentials(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
        config: CredentialPrefetchConfig,
    ) -> Result<SharedCredentialsProvider, anyhow::Error> {
        let inner = match &self.auth {
            AwsAuth::Credentials(credentials) => SharedCredentialsProvider::new(
                credentials
                    .load_credentials_provider(connection_context, InTask::No)
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
                    .load_credentials_provider(connection_context, connection_id)
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
        let prefetcher =
            CredentialPrefetcher::new(inner, config, format!("aws-connection-{connection_id}"))
                .await?;
        Ok(SharedCredentialsProvider::new(prefetcher))
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

impl Default for CredentialPrefetchConfig {
    fn default() -> Self {
        CredentialPrefetchConfig {
            refresh_fraction: 0.5,
            max_refresh_interval: Duration::from_secs(15 * 60),
            min_refresh_interval: Duration::from_secs(30),
            fetch_timeout: Duration::from_secs(30),
            fetch_max_tries: 5,
        }
    }
}

/// A [`ProvideCredentials`] that serves credentials from an in-memory cache kept
/// warm by a background task.
///
/// Callers always get the last successfully fetched credentials with no STS call
/// on their path. A background task refreshes ahead of expiry. A failed refresh
/// keeps the last-good value, so a transient STS or network failure does not
/// surface to callers as long as the cached credentials are still valid.
///
/// Dropping the prefetcher aborts the refresh task.
pub struct CredentialPrefetcher {
    cache: watch::Receiver<Option<Credentials>>,
    _refresh_task: AbortOnDropHandle<()>,
}

impl std::fmt::Debug for CredentialPrefetcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialPrefetcher")
            .finish_non_exhaustive()
    }
}

impl CredentialPrefetcher {
    /// Fetches an initial credential (with retry) and spawns a background task to
    /// keep it fresh. Returns an error if the initial fetch cannot produce a
    /// credential, so the caller can fail before relying on an empty cache.
    ///
    /// `diagnostic` labels the spawned task and log lines, e.g. the connection
    /// id.
    pub async fn new(
        inner: SharedCredentialsProvider,
        config: CredentialPrefetchConfig,
        diagnostic: String,
    ) -> Result<CredentialPrefetcher, anyhow::Error> {
        let initial = Self::fetch(&inner, &config)
            .await
            .with_context(|| format!("initial credential prefetch failed for {diagnostic}"))?;
        debug!(
            connection = %diagnostic,
            expiry = ?initial.expiry(),
            "initial credential prefetch succeeded"
        );
        let (tx, cache) = watch::channel(Some(initial));
        let task_name = format!("credential-prefetch:{diagnostic}");
        let refresh_task = mz_ore::task::spawn(
            || task_name,
            Self::refresh_loop(inner, tx, config, diagnostic),
        )
        .abort_on_drop();
        Ok(CredentialPrefetcher {
            cache,
            _refresh_task: refresh_task,
        })
    }

    /// Fetches a single credential through `inner`, retrying transient failures
    /// and bounding each attempt with `fetch_timeout`.
    async fn fetch(
        inner: &SharedCredentialsProvider,
        config: &CredentialPrefetchConfig,
    ) -> Result<Credentials, anyhow::Error> {
        Retry::default()
            .max_tries(config.fetch_max_tries)
            .retry_async(|_| async {
                match tokio::time::timeout(config.fetch_timeout, inner.provide_credentials()).await
                {
                    Ok(Ok(creds)) => RetryResult::Ok(creds),
                    Ok(Err(e)) => RetryResult::RetryableErr(anyhow::Error::new(e)),
                    Err(_elapsed) => RetryResult::RetryableErr(anyhow!(
                        "credential fetch timed out after {:?}",
                        config.fetch_timeout
                    )),
                }
            })
            .await
    }

    async fn refresh_loop(
        inner: SharedCredentialsProvider,
        tx: watch::Sender<Option<Credentials>>,
        config: CredentialPrefetchConfig,
        diagnostic: String,
    ) {
        loop {
            let wait = next_refresh_wait(tx.borrow().as_ref(), &config);
            debug!(
                connection = %diagnostic,
                ?wait,
                "sleeping until next credential refresh"
            );
            tokio::time::sleep(wait).await;
            // Nothing holds the cache anymore (the dataflow tore down). Stop.
            if tx.is_closed() {
                return;
            }
            match Self::fetch(&inner, &config).await {
                Ok(creds) => {
                    let expiry = creds.expiry();
                    if tx.send(Some(creds)).is_err() {
                        return;
                    }
                    debug!(
                        connection = %diagnostic,
                        ?expiry,
                        "credential prefetch refresh succeeded"
                    );
                }
                Err(e) => {
                    // Keep the last-good credentials. The next iteration's wait
                    // is recomputed from the unchanged expiry, so as the cached
                    // credentials approach expiry we retry within
                    // `min_refresh_interval`.
                    warn!(
                        connection = %diagnostic,
                        error = %e.display_with_causes(),
                        "credential prefetch refresh failed; serving last-good credentials"
                    );
                }
            }
        }
    }
}

/// Computes how long to wait before the next refresh, given the currently cached
/// credentials. Refreshes at `refresh_fraction` of the remaining lifetime,
/// clamped to `[min_refresh_interval, max_refresh_interval]`. Credentials with no
/// expiry refresh at the cap.
fn next_refresh_wait(creds: Option<&Credentials>, config: &CredentialPrefetchConfig) -> Duration {
    let Some(expiry) = creds.and_then(|c| c.expiry()) else {
        return config.max_refresh_interval;
    };
    let remaining = expiry
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO);
    remaining
        .mul_f64(config.refresh_fraction)
        .clamp(config.min_refresh_interval, config.max_refresh_interval)
}

impl ProvideCredentials for CredentialPrefetcher {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async move {
            // Fast path: the cache is populated (the common case, since `new`
            // fetches before returning). Clone out of the `watch::Ref` before
            // matching so the read guard is not held across the branch.
            let cached = self.cache.borrow().clone();
            if let Some(creds) = cached {
                return Ok(creds);
            }
            // The cache is empty only if the initial value was never produced.
            // Wait for the background task to populate one.
            let mut cache = self.cache.clone();
            loop {
                if cache.changed().await.is_err() {
                    return Err(CredentialsError::provider_error(
                        "credential prefetcher refresh task stopped before producing credentials",
                    ));
                }
                let cached = cache.borrow().clone();
                if let Some(creds) = cached {
                    return Ok(creds);
                }
            }
        })
    }

    fn fallback_on_interrupt(&self) -> Option<Credentials> {
        self.cache.borrow().clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use aws_credential_types::provider::future;

    use super::*;

    /// A test [`ProvideCredentials`] that counts calls and can be made to fail
    /// after a given number of successful fetches. Each successful fetch returns
    /// a distinct access key id (`akid-{n}`) with `ttl` until expiry.
    #[derive(Debug)]
    struct CountingProvider {
        calls: Arc<AtomicUsize>,
        /// Fail once the cumulative call count exceeds this.
        ok_calls: usize,
        ttl: Duration,
    }

    impl ProvideCredentials for CountingProvider {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            let calls = Arc::clone(&self.calls);
            let ok_calls = self.ok_calls;
            let ttl = self.ttl;
            future::ProvideCredentials::new(async move {
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                if n > ok_calls {
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
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = SharedCredentialsProvider::new(CountingProvider {
            calls: Arc::clone(&calls),
            ok_calls: usize::MAX,
            ttl: Duration::from_secs(3600),
        });
        // Long refresh intervals so the background task never fires during the test.
        let config = CredentialPrefetchConfig {
            refresh_fraction: 0.5,
            max_refresh_interval: Duration::from_secs(3600),
            min_refresh_interval: Duration::from_secs(3600),
            fetch_timeout: Duration::from_secs(5),
            fetch_max_tries: 1,
        };
        let prefetcher = CredentialPrefetcher::new(inner, config, "test".to_string())
            .await
            .unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 1, "initial fetch is one call");
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
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = SharedCredentialsProvider::new(CountingProvider {
            calls: Arc::clone(&calls),
            ok_calls: usize::MAX,
            ttl: Duration::from_millis(40),
        });
        let config = CredentialPrefetchConfig {
            refresh_fraction: 0.5,
            max_refresh_interval: Duration::from_secs(1),
            min_refresh_interval: Duration::from_millis(20),
            fetch_timeout: Duration::from_secs(1),
            fetch_max_tries: 1,
        };
        let prefetcher = CredentialPrefetcher::new(inner, config, "test".to_string())
            .await
            .unwrap();

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
        let calls = Arc::new(AtomicUsize::new(0));
        // Only the initial fetch succeeds; every refresh fails.
        let inner = SharedCredentialsProvider::new(CountingProvider {
            calls: Arc::clone(&calls),
            ok_calls: 1,
            ttl: Duration::from_millis(40),
        });
        let config = CredentialPrefetchConfig {
            refresh_fraction: 0.5,
            max_refresh_interval: Duration::from_secs(1),
            min_refresh_interval: Duration::from_millis(20),
            fetch_timeout: Duration::from_millis(200),
            fetch_max_tries: 1,
        };
        let prefetcher = CredentialPrefetcher::new(inner, config, "test".to_string())
            .await
            .unwrap();

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
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = SharedCredentialsProvider::new(CountingProvider {
            calls: Arc::clone(&calls),
            ok_calls: usize::MAX,
            ttl: Duration::from_millis(40),
        });
        let config = CredentialPrefetchConfig {
            refresh_fraction: 0.5,
            max_refresh_interval: Duration::from_secs(1),
            min_refresh_interval: Duration::from_millis(20),
            fetch_timeout: Duration::from_secs(1),
            fetch_max_tries: 1,
        };
        let prefetcher = CredentialPrefetcher::new(inner, config, "test".to_string())
            .await
            .unwrap();

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
}
