// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS configuration for sources and sinks.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use anyhow::{Context, anyhow, bail};
use aws_config::sts::AssumeRoleProvider;
use aws_config::timeout::TimeoutConfig;
use aws_credential_types::Credentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider};
use aws_sdk_sts::error::SdkError;
use aws_sdk_sts::operation::get_caller_identity::GetCallerIdentityError;
use aws_types::SdkConfig;
use aws_types::region::Region;
use mz_aws_util::rds::RdsIamAuth;
use mz_dyncfg::ConfigSet;
use mz_ore::error::ErrorExt;
use mz_ore::future::{InTask, OreFutureExt};
use mz_ore::now::{NowFn, SYSTEM_TIME};
use mz_ore::task::AbortOnDropHandle;
use mz_repr::{CatalogItemId, GlobalId};
#[cfg(any(test, feature = "proptest"))]
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::watch;
use tokio::time::Instant;
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
    ///
    /// `sts_connect_timeout`, if set, replaces the AWS SDK's default connect timeout.
    async fn load_credentials_provider(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
        sts_connect_timeout: Option<Duration>,
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
            sts_connect_timeout,
        )
        .await
    }

    /// Returns the process-wide caching credentials provider for this role,
    /// from [`ConnectionContext::aws_credentials_cache`].
    ///
    /// No STS call happens here, so success does not mean the role can be
    /// assumed.
    async fn cached_credentials_provider(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
    ) -> Result<LazyCredentialCache, anyhow::Error> {
        let key = AwsCredentialsCacheKey {
            connection_id,
            role_arn: self.arn.clone(),
            session_name: self.session_name.clone(),
            external_id: self
                .external_id(connection_context, connection_id)
                .with_context(|| {
                    format!(
                        "failed to compute external ID for AWS AssumeRole connection {} \
                         (role arn {})",
                        connection_id, self.arn
                    )
                })?,
            connection_role_arn: connection_context.aws_connection_role_arn.clone(),
        };
        if let Some(cache) = connection_context.aws_credentials_cache.get(&key) {
            return Ok(cache);
        }
        let provider = self
            .load_credentials_provider(connection_context, connection_id, None)
            .await?;
        let cache = LazyCredentialCache::new(SharedCredentialsProvider::new(provider));
        Ok(connection_context
            .aws_credentials_cache
            .get_or_insert(key, cache))
    }

    /// Returns a provider that serves cached credentials for this role,
    /// kept fresh by a background task that assumes the role ahead of expiry.
    ///
    /// Errors if the provider cannot be constructed (environment misconfiguration).
    /// No STS call happens here, so success does not mean the role can be assumed.
    ///
    /// The provider's callers wait only while the first fetch is still in flight.
    /// If fetching has failed and no unexpired credentials remain, callers receive an error.
    pub async fn prefetch_credentials(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
        configs: &ConfigSet,
        diagnostic_label: String,
    ) -> Result<SharedCredentialsProvider, anyhow::Error> {
        let mut sts_connect_timeout = dyncfgs::AWS_PREFETCH_STS_CONNECT_TIMEOUT.get(configs);
        if sts_connect_timeout < Duration::from_secs(1) {
            // Treat sub-second values as misconfiguration and use the default instead.
            // A timeout of zero would fail every connect outright,
            // and this knob exists to raise the timeout, not to lower it.
            let default = *dyncfgs::AWS_PREFETCH_STS_CONNECT_TIMEOUT.default();
            warn!(
                %diagnostic_label,
                configured = ?sts_connect_timeout,
                ?default,
                "ignoring aws_prefetch_sts_connect_timeout below 1s"
            );
            sts_connect_timeout = default;
        }
        let provider = self
            .load_credentials_provider(connection_context, connection_id, Some(sts_connect_timeout))
            .await
            .with_context(|| {
                format!(
                    "failed to initialize AssumeRole credential provider for \
                     connection {} (role arn {})",
                    connection_id, self.arn
                )
            })?;
        Ok(SharedCredentialsProvider::new(CredentialPrefetcher::new(
            SharedCredentialsProvider::new(provider),
            sts_connect_timeout,
            diagnostic_label,
        )))
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
        sts_connect_timeout: Option<Duration>,
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
        let mut loader = mz_aws_util::defaults();
        if let Some(timeout) = sts_connect_timeout {
            // Replaces the SDK's 3.1 second connect-timeout default. The SDK
            // classifies timed-out connections as transient and retries them
            // itself.
            loader =
                loader.timeout_config(TimeoutConfig::builder().connect_timeout(timeout).build());
        }
        let assume_role_sdk_config = loader.load().await;

        // The default session name identifies the environment and the
        // connection.
        let default_session_name =
            format!("{}-{}", connection_context.environment_id, connection_id);

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

    // NOTE: the `mz_internal.mz_aws_connections` builtin materialized view
    // reconstructs this `mz_<prefix>_<conn>` format in SQL (see
    // MZ_AWS_CONNECTIONS in src/catalog/src/builtin/mz_internal.rs). Keep the
    // two in sync.
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

    // NOTE: the `mz_internal.mz_aws_connections` builtin materialized view
    // reconstructs this trust-policy JSON with `jsonb_build_object` (see
    // MZ_AWS_CONNECTIONS in src/catalog/src/builtin/mz_internal.rs). Keep the
    // structure (keys, nesting) in sync.
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

    /// The custom DNS resolver wired into the AWS HTTP client is only
    /// invoked for hostnames. IP-literal endpoints (e.g. `http://127.0.0.1`)
    /// bypass it. Validate IP literals here so they cannot circumvent
    /// the global-address enforcement.
    pub(crate) fn validate_endpoint(
        &self,
        enforce_external_addresses: bool,
    ) -> Result<(), anyhow::Error> {
        if enforce_external_addresses
            && let Some(endpoint) = &self.endpoint
            && let Ok(url) = url::Url::parse(endpoint)
        {
            mz_ore::netio::ensure_url_ip_global(&url)?;
        }
        Ok(())
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
                        .load_credentials_provider(&connection_context, connection_id, None)
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

    /// Returns how to authenticate to Amazon RDS with AWS IAM through this
    /// connection.
    ///
    /// An `AssumeRole` connection's credentials are cached and shared across
    /// this process. Signing a token asks the credentials provider directly,
    /// which bypasses the identity cache of AWS SDK clients, so without the
    /// cache every token assumes the role again, against an STS quota that AWS
    /// enforces per account.
    ///
    /// When the database rejects a token, new credentials are fetched for the
    /// next one, at most once every five minutes. The cached credentials stay
    /// in use until that fetch succeeds.
    pub async fn load_rds_iam_auth(
        &self,
        connection_context: &ConnectionContext,
        connection_id: CatalogItemId,
        in_task: InTask,
        enforce_external_addresses: bool,
    ) -> Result<RdsIamAuth, anyhow::Error> {
        let AwsAuth::AssumeRole(assume_role) = &self.auth else {
            // Static credentials are not fetched from STS, so there is
            // nothing to cache.
            let sdk_config = self
                .load_sdk_config(
                    connection_context,
                    connection_id,
                    in_task,
                    enforce_external_addresses,
                )
                .await?;
            return Ok(RdsIamAuth::new(sdk_config));
        };
        let connection_context = connection_context.clone();
        let this = self.clone();
        let assume_role = assume_role.clone();
        // Wrapped in a `run_in_task_if` for the same reason as the block in
        // `load_sdk_config`.
        async move {
            let cache = assume_role
                .cached_credentials_provider(&connection_context, connection_id)
                .await
                .with_context(|| {
                    format!(
                        "failed to initialize cached AssumeRole credential provider for \
                         connection {} (role arn {})",
                        connection_id, assume_role.arn
                    )
                })?;
            let sdk_config = this
                .load_sdk_config_from_credentials(cache.clone(), enforce_external_addresses)
                .await
                .with_context(|| {
                    format!("failed to build AWS SDK config for connection {connection_id}")
                })?;
            let hook = move |access_key_id: &str| cache.request_refresh(access_key_id);
            Ok(RdsIamAuth::new(sdk_config).on_token_rejected(hook))
        }
        .run_in_task_if(in_task, || "load_rds_iam_auth".to_string())
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
            self.validate_endpoint(enforce_external_addresses)?;
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
                    None,
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

/// A process-wide cache of caching credentials providers for `AssumeRole` AWS
/// connections. See [`AwsConnection::load_rds_iam_auth`].
///
/// An entry is dropped the next time an entry is inserted, if no config holds
/// it and it has no credentials that can still be served. That bounds what an
/// unused entry costs, not how many entries there are: connections that were
/// used within the lifetime of their credentials all stay.
#[derive(Debug, Clone, Default)]
pub struct AwsCredentialsCache {
    entries: Arc<Mutex<BTreeMap<AwsCredentialsCacheKey, LazyCredentialCache>>>,
}

impl AwsCredentialsCache {
    fn get(&self, key: &AwsCredentialsCacheKey) -> Option<LazyCredentialCache> {
        let entries = self.entries.lock().expect("lock poisoned");
        entries.get(key).cloned()
    }

    /// Returns the cache under `key`, inserting `cache` if there is none.
    /// Racing callers all get the one inserted first, so they share its
    /// cached credentials.
    fn get_or_insert(
        &self,
        key: AwsCredentialsCacheKey,
        cache: LazyCredentialCache,
    ) -> LazyCredentialCache {
        let mut entries = self.entries.lock().expect("lock poisoned");
        // An entry that a config holds stays, whatever its credentials.
        // Evicting it would leave that config on the old entry and give the
        // next config for the same connection a second one, with its own
        // fetches. An entry that nobody holds is only worth its credentials.
        //
        // NOTE: This takes each entry's state lock under the map lock. That
        // cannot deadlock, because the state lock is never held across an
        // await and nothing takes the map lock while holding it.
        entries.retain(|_, entry| entry.has_other_holders() || entry.has_fresh_credentials());
        entries.entry(key).or_insert(cache).clone()
    }
}

/// Everything that determines which credentials an `AssumeRole` AWS connection
/// receives. Altering any of it must not serve the old entry's credentials.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct AwsCredentialsCacheKey {
    connection_id: CatalogItemId,
    role_arn: String,
    session_name: Option<String>,
    external_id: String,
    connection_role_arn: Option<String>,
}

/// Cached credentials are not served once they are this close to expiry.
///
/// Callers sign with the credentials immediately, so this only has to cover
/// clock skew and the time until the signature is checked. `AssumeRole`
/// credentials last an hour by default.
const CREDENTIAL_CACHE_EXPIRY_BUFFER: Duration = Duration::from_mins(5);

/// [`LazyCredentialCache::request_refresh`] does nothing if the credentials
/// were fetched, or a refresh of them last failed, less than this long ago.
///
/// Most authentication failures are not caused by the credentials: a missing
/// database grant fails the same way on every retry. Without this floor, each
/// such retry would assume the role again, which is the load the cache exists
/// to remove. The floor is also the longest that credentials AWS no longer
/// honors keep being reused.
const CREDENTIAL_CACHE_MIN_REUSE: Duration = Duration::from_mins(5);

/// Bounds a single credential fetch.
///
/// The fetch holds the cache's lock, and the AWS SDK bounds only the connect
/// phase of an `AssumeRole` call. A call that connects and then stalls would
/// otherwise block every user of the connection.
const CREDENTIAL_CACHE_FETCH_TIMEOUT: Duration = Duration::from_secs(60);

/// A [`ProvideCredentials`] that fetches credentials from `inner` on demand
/// and serves them from memory until [`CREDENTIAL_CACHE_EXPIRY_BUFFER`] before
/// they expire. Clones share the cached credentials.
///
/// It does no background work, so holding one costs no STS requests. A
/// [`CredentialPrefetcher`] held for the life of a source would refresh for
/// as long as the source runs.
///
/// A failed fetch is not cached, but callers that were already waiting for it
/// receive its error. Credentials past the expiry buffer are never served,
/// even if the fetch that would replace them fails.
///
/// The cache is shared by every user of an AWS connection, so one user's
/// trouble must not take working credentials from the others. See
/// [`LazyCredentialCache::request_refresh`].
#[derive(Debug, Clone)]
struct LazyCredentialCache {
    inner: SharedCredentialsProvider,
    /// NOTE: Never held across an await. That is what lets
    /// [`LazyCredentialCache::request_refresh`] always record a request,
    /// including while a fetch is in flight.
    state: Arc<Mutex<LazyCredentialState>>,
    /// Held across a fetch, so that concurrent callers wait for one fetch.
    /// Without it, each would start a fetch of its own.
    fetch_lock: Arc<tokio::sync::Mutex<()>>,
    /// The clock that credential expiry is compared against.
    now: NowFn,
}

#[derive(Debug, Default)]
struct LazyCredentialState {
    // AWS's `Debug` impl for `Credentials` sanitizes secrets.
    creds: Option<Credentials>,
    /// When `creds` were fetched, or when a requested refresh of them last
    /// failed. [`CREDENTIAL_CACHE_MIN_REUSE`] counts from here.
    fetched_at: Option<Instant>,
    /// Set by [`LazyCredentialCache::request_refresh`]. The next call fetches
    /// new credentials even though `creds` have not expired.
    refresh_requested: bool,
    /// The error of the most recent fetch, if it failed.
    last_error: Option<String>,
    /// The number of failed fetches. Lets a caller tell whether a fetch
    /// failed while it waited for `fetch_lock`.
    failed_fetches: u64,
}

impl LazyCredentialState {
    /// The cached credentials, if they can still be served at `now`.
    fn fresh(&self, now: SystemTime) -> Option<&Credentials> {
        self.creds
            .as_ref()
            .filter(|creds| credentials_are_fresh(creds, now))
    }

    /// The cached credentials, if they can be served at `now` without a
    /// fetch.
    fn servable(&self, now: SystemTime) -> Option<Credentials> {
        if self.refresh_requested {
            return None;
        }
        self.fresh(now).cloned()
    }
}

impl LazyCredentialCache {
    fn new(inner: SharedCredentialsProvider) -> LazyCredentialCache {
        LazyCredentialCache::with_clock(inner, SYSTEM_TIME.clone())
    }

    fn with_clock(inner: SharedCredentialsProvider, now: NowFn) -> LazyCredentialCache {
        LazyCredentialCache {
            inner,
            state: Default::default(),
            fetch_lock: Default::default(),
            now,
        }
    }

    fn state(&self) -> MutexGuard<'_, LazyCredentialState> {
        self.state.lock().expect("lock poisoned")
    }

    fn now(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_millis((self.now)())
    }

    /// Reports whether anything other than this handle shares the cached
    /// credentials.
    fn has_other_holders(&self) -> bool {
        Arc::strong_count(&self.state) > 1
    }

    /// Reports whether the cache holds credentials that can still be served.
    fn has_fresh_credentials(&self) -> bool {
        self.state().fresh(self.now()).is_some()
    }

    /// Asks for the cached credentials to be replaced by the next call, if
    /// their access key ID is `access_key_id`.
    ///
    /// Call this when a service rejects a request signed with those
    /// credentials. AWS can stop honoring credentials before they expire, for
    /// example when the role's active sessions are revoked.
    ///
    /// The request is ignored within [`CREDENTIAL_CACHE_MIN_REUSE`] of the
    /// last fetch. A revocation can therefore fail connections for that long.
    ///
    /// The credentials are not dropped. If the fetch that should replace them
    /// fails, they keep being served. The rejection may have had another
    /// cause, such as one database user lacking a grant, and other users of
    /// the connection would otherwise lose credentials that still work for
    /// them.
    fn request_refresh(&self, access_key_id: &str) {
        let mut state = self.state();
        // Comparing the access key ID keeps a caller that signed with older
        // credentials from replacing the ones that replaced them.
        let is_current = state
            .creds
            .as_ref()
            .is_some_and(|creds| creds.access_key_id() == access_key_id);
        let is_old_enough = state
            .fetched_at
            .is_some_and(|fetched_at| fetched_at.elapsed() >= CREDENTIAL_CACHE_MIN_REUSE);
        if is_current && is_old_enough {
            debug!("refreshing cached AWS credentials after an authentication failure");
            state.refresh_requested = true;
        }
    }
}

/// Reports whether `creds` can still be served at `now`. Credentials with no
/// expiry are always fresh.
fn credentials_are_fresh(creds: &Credentials, now: SystemTime) -> bool {
    match creds.expiry() {
        None => true,
        Some(expiry) => now + CREDENTIAL_CACHE_EXPIRY_BUFFER < expiry,
    }
}

impl ProvideCredentials for LazyCredentialCache {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async move {
            let failed_before = {
                let state = self.state();
                if let Some(creds) = state.servable(self.now()) {
                    return Ok(creds);
                }
                state.failed_fetches
            };
            let _fetching = self.fetch_lock.lock().await;
            {
                let state = self.state();
                let now = self.now();
                // Another caller fetched while this one waited.
                if let Some(creds) = state.servable(now) {
                    return Ok(creds);
                }
                // A fetch failed while this caller waited. Fetching again
                // here would make N callers that arrive together wait
                // through N failed fetches in a row.
                if state.fresh(now).is_none()
                    && state.failed_fetches != failed_before
                    && let Some(error) = &state.last_error
                {
                    return Err(CredentialsError::provider_error(format!(
                        "failed to fetch AWS credentials: {error}"
                    )));
                }
            }
            let fetch = self.inner.provide_credentials();
            let fetched = match tokio::time::timeout(CREDENTIAL_CACHE_FETCH_TIMEOUT, fetch).await {
                Ok(fetched) => fetched,
                Err(_elapsed) => Err(CredentialsError::provider_error(format!(
                    "credential fetch timed out after {CREDENTIAL_CACHE_FETCH_TIMEOUT:?}"
                ))),
            };
            let mut state = self.state();
            let error = match fetched {
                Ok(creds) => {
                    state.creds = Some(creds.clone());
                    state.fetched_at = Some(Instant::now());
                    state.refresh_requested = false;
                    state.last_error = None;
                    return Ok(creds);
                }
                Err(error) => error,
            };
            // A requested refresh failed. Keep serving the credentials it
            // should have replaced, and restart the floor so that a failing
            // STS is not asked again on every call. Freshness is checked now
            // and not before the fetch, because the fetch can take up to
            // `CREDENTIAL_CACHE_FETCH_TIMEOUT`.
            if let Some(creds) = state.fresh(self.now()).cloned() {
                warn!(
                    error = %error.display_with_causes(),
                    "failed to refresh AWS credentials, keeping the cached ones"
                );
                state.fetched_at = Some(Instant::now());
                state.refresh_requested = false;
                return Ok(creds);
            }
            state.last_error = Some(error.display_with_causes().to_string());
            state.failed_fetches += 1;
            Err(error)
        })
    }
}

/// If we have N seconds before expiry,
/// wait (N * refresh_fraction) seconds before the next refresh attempt.
const CREDENTIAL_REFRESH_FRACTION: f64 = 0.5;

/// Regardless of [`CREDENTIAL_REFRESH_FRACTION`],
/// refresh credentials at least this often.
///
/// 15 minutes is an arbitrary choice.
const CREDENTIAL_MAX_REFRESH_INTERVAL: Duration = Duration::from_mins(15);

/// Lower bound on the wait between refreshes.
const CREDENTIAL_MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// (MULTIPLE * sts_connect_timeout) is how long we wait for the AWS client
/// to try (and retry) fetching STS credentials.
const CREDENTIAL_FETCH_BACKSTOP_MULTIPLE: u32 = 10;

/// A [`ProvideCredentials`] that serves credentials from an in-memory cache kept
/// warm by a background task.
///
/// The background task does all STS credentials fetches, including the first.
/// A call that arrives before the first fetch completes waits for it.
///
/// A failed refresh leaves the cached credentials alone.
/// If the cached credentials are allowed to expire, callers get the most recent fetch error instead,
/// so a persistent failure shows its real cause.
struct CredentialPrefetcher {
    cache: watch::Receiver<CredentialState>,

    /// Dropping the prefetcher aborts the background task.
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
            // AWS's `Debug` impl for `Credentials` sanitizes secrets, so we can include it here.
            .field("cache", &*self.cache.borrow())
            .finish_non_exhaustive()
    }
}

impl CredentialPrefetcher {
    /// Spawns the background task and returns immediately.
    fn new(
        inner: SharedCredentialsProvider,
        sts_connect_timeout: Duration,
        diagnostic_label: String,
    ) -> CredentialPrefetcher {
        let (tx, cache) = watch::channel(CredentialState::default());
        let task_name = format!("credential-prefetch:{diagnostic_label}");
        let refresh_task = mz_ore::task::spawn(|| task_name, async move {
            Self::refresh_loop(inner, tx, sts_connect_timeout, diagnostic_label).await;
        })
        .abort_on_drop();
        CredentialPrefetcher {
            cache,
            _refresh_task: refresh_task,
        }
    }

    /// Fetches a single credential through `inner`.
    ///
    /// Connect timeout and retry policy live in the SDK: the prefetcher's
    /// provider carries `sts_connect_timeout` as its connect timeout, and the SDK
    /// retries timed-out connects and other transient failures itself. The
    /// timeout here is a backstop against the one case the SDK leaves
    /// unbounded, a connection that establishes and then stalls. A fetch that
    /// trips it is left to the refresh loop's schedule like any other
    /// failure.
    async fn fetch(
        inner: &SharedCredentialsProvider,
        sts_connect_timeout: Duration,
    ) -> Result<Credentials, anyhow::Error> {
        let backstop = sts_connect_timeout.saturating_mul(CREDENTIAL_FETCH_BACKSTOP_MULTIPLE);
        match tokio::time::timeout(backstop, inner.provide_credentials()).await {
            Ok(Ok(creds)) => Ok(creds),
            Ok(Err(e)) => Err(anyhow::Error::new(e)),
            Err(_elapsed) => Err(anyhow!("credential fetch timed out after {backstop:?}")),
        }
    }

    async fn refresh_loop(
        inner: SharedCredentialsProvider,
        tx: watch::Sender<CredentialState>,
        sts_connect_timeout: Duration,
        diagnostic_label: String,
    ) {
        loop {
            match Self::fetch(&inner, sts_connect_timeout).await {
                Ok(creds) => {
                    let expiry = creds.expiry();
                    let wait = next_refresh_wait(Some(&creds));
                    tx.send_modify(|state| {
                        state.creds = Some(creds);
                        state.last_error = None;
                    });
                    debug!(
                        %diagnostic_label,
                        ?expiry,
                        next_refresh_in = ?wait,
                        "credential fetch succeeded"
                    );
                    tokio::time::sleep(wait).await;
                }
                Err(e) => {
                    let error = e.display_with_causes().to_string();

                    // We use the same wait-duration logic regardless of whether the fetch succeeds.
                    // As the credentials expiry gets closer, the wait time gets shorter.
                    let wait = next_refresh_wait(tx.borrow().creds.as_ref());
                    warn!(
                        %diagnostic_label,
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

/// Computes how long to wait before the next fetch, given the currently cached credentials.
///
/// Waits [`CREDENTIAL_REFRESH_FRACTION`] of the credentials' remaining lifetime,
/// bounded by [`CREDENTIAL_MIN_REFRESH_INTERVAL`] and [`CREDENTIAL_MAX_REFRESH_INTERVAL`].
///
/// Credentials with no expiry wait the maximum.
/// No credentials at all (fetching has not succeeded yet) waits the minimum.
fn next_refresh_wait(creds: Option<&Credentials>) -> Duration {
    let Some(creds) = creds else {
        return CREDENTIAL_MIN_REFRESH_INTERVAL;
    };
    let Some(expiry) = creds.expiry() else {
        return CREDENTIAL_MAX_REFRESH_INTERVAL;
    };
    let remaining = expiry
        .duration_since(SystemTime::now())
        .unwrap_or(Duration::ZERO);
    remaining.mul_f64(CREDENTIAL_REFRESH_FRACTION).clamp(
        CREDENTIAL_MIN_REFRESH_INTERVAL,
        CREDENTIAL_MAX_REFRESH_INTERVAL,
    )
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
            // Wait for the first fetch to conclude, one way or the other.
            // Clone out of the `watch::Ref` so the read guard is dropped
            // right away.
            let state = match cache
                .wait_for(|state| state.creds.is_some() || state.last_error.is_some())
                .await
            {
                Ok(state) => state.clone(),
                Err(_closed) => {
                    return Err(CredentialsError::provider_error(
                        "credential prefetcher task stopped before producing credentials",
                    ));
                }
            };
            match state.creds {
                Some(creds) => {
                    let now = SystemTime::now();
                    match creds.expiry() {
                        Some(expiry) if expiry <= now => {
                            let age = now.duration_since(expiry).unwrap_or(Duration::ZERO);
                            let cause = match state.last_error {
                                Some(error) => {
                                    format!("most recent fetch attempt failed: {error}")
                                }
                                None => "no refresh has completed since".to_string(),
                            };
                            Err(CredentialsError::provider_error(format!(
                                "cached AWS credentials expired {age:?} ago and {cause}"
                            )))
                        }
                        _ => Ok(creds),
                    }
                }
                None => {
                    let error = state
                        .last_error
                        .expect("wait_for guarantees creds or last_error");
                    Err(CredentialsError::provider_error(format!(
                        "failed to fetch AWS credentials: {error}"
                    )))
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
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use aws_credential_types::provider::future;
    use tokio::sync::Semaphore;

    use super::*;

    /// A test [`ProvideCredentials`] with per-call behavior keyed on the
    /// cumulative call count `n` (starting at 1): the first `hang_calls` calls
    /// never resolve, the next `ok_calls` calls succeed, and every call after
    /// that fails. Each success returns a distinct access key id (`akid-{n}`)
    /// that expires `ttl` from now, or never for a `ttl` of `None`.
    #[derive(Debug)]
    struct CountingProvider {
        calls: Arc<AtomicUsize>,
        hang_calls: usize,
        ok_calls: usize,
        ttl: Option<Duration>,
    }

    impl CountingProvider {
        fn shared(
            hang_calls: usize,
            ok_calls: usize,
            ttl: Option<Duration>,
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
                    ttl.map(|ttl| SystemTime::now() + ttl),
                    "test",
                ))
            })
        }
    }

    fn prefetcher(provider: SharedCredentialsProvider) -> CredentialPrefetcher {
        CredentialPrefetcher::new(provider, Duration::from_secs(30), "test".to_string())
    }

    /// One paused-clock sleep that outlasts a scheduled refresh. Credentials
    /// without expiry refresh at `CREDENTIAL_MAX_REFRESH_INTERVAL`.
    const PAST_NEXT_REFRESH: Duration =
        CREDENTIAL_MAX_REFRESH_INTERVAL.saturating_add(Duration::from_secs(60));

    // NOTE: These tests run on tokio's paused clock: a sleep completes as soon
    // as the runtime is otherwise idle, so refresh schedules elapse instantly
    // and deterministically. Credential expiry uses `SystemTime`, which does
    // not pause, so tests that need unexpired credentials use `ttl: None` and
    // the expiry test uses a ttl of zero.

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn cache_serves_without_recalling_inner() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, None);
        let prefetcher = prefetcher(inner);

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

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn background_refresh_advances_the_value() {
        let (_calls, inner) = CountingProvider::shared(0, usize::MAX, None);
        let prefetcher = prefetcher(inner);

        let creds = prefetcher.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-1");

        tokio::time::sleep(PAST_NEXT_REFRESH).await;
        let creds = prefetcher.provide_credentials().await.unwrap();
        assert_ne!(
            creds.access_key_id(),
            "akid-1",
            "a refresh should have replaced the initial credentials"
        );
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn failed_refresh_keeps_last_good() {
        // Only the initial fetch succeeds; every refresh fails.
        let (calls, inner) = CountingProvider::shared(0, 1, None);
        let prefetcher = prefetcher(inner);

        let creds = prefetcher.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-1");

        tokio::time::sleep(PAST_NEXT_REFRESH).await;
        assert!(
            calls.load(Ordering::SeqCst) >= 2,
            "a refresh attempt should have happened"
        );
        let creds = prefetcher.provide_credentials().await.unwrap();
        assert_eq!(
            creds.access_key_id(),
            "akid-1",
            "a failed refresh must keep serving the last-good credentials"
        );
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn dropping_prefetcher_stops_the_task() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, None);
        let prefetcher = prefetcher(inner);

        prefetcher.provide_credentials().await.unwrap();
        drop(prefetcher);

        let after_drop = calls.load(Ordering::SeqCst);
        tokio::time::sleep(PAST_NEXT_REFRESH).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            after_drop,
            "the refresh task must stop once the prefetcher is dropped"
        );
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn hung_fetch_fails_then_recovers() {
        // The first call hangs forever. The backstop timeout fails the fetch,
        // and the next scheduled refresh succeeds.
        let (calls, inner) = CountingProvider::shared(1, usize::MAX, None);
        let prefetcher = prefetcher(inner);

        let err = prefetcher.provide_credentials().await.unwrap_err();
        assert!(
            format!("{}", err.display_with_causes()).contains("timed out"),
            "the error must name the backstop timeout: {err}"
        );

        // A fetch that has never succeeded is retried at the floor interval.
        tokio::time::sleep(CREDENTIAL_MIN_REFRESH_INTERVAL.saturating_mul(2)).await;
        let creds = prefetcher.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn initial_fetch_failure_returns_error() {
        // Every fetch fails, so callers must get the fetch error rather than wait forever.
        let (calls, inner) = CountingProvider::shared(0, 0, None);
        let prefetcher = prefetcher(inner);

        let err = prefetcher.provide_credentials().await.unwrap_err();
        assert!(
            format!("{}", err.display_with_causes()).contains("injected failure"),
            "the error must carry the fetch failure cause: {err}"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "the caller sees the error after a single fetch attempt, retries belong to the refresh loop"
        );
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn expired_credentials_return_error() {
        // One successful fetch of already-expired credentials, then only
        // failures.
        let (calls, inner) = CountingProvider::shared(0, 1, Some(Duration::ZERO));
        let prefetcher = prefetcher(inner);

        // Expired credentials retry at the floor interval. Outwait one retry
        // so `last_error` is populated.
        tokio::time::sleep(PAST_NEXT_REFRESH).await;
        assert!(
            calls.load(Ordering::SeqCst) >= 2,
            "a refresh attempt should have happened"
        );
        let err = prefetcher.provide_credentials().await.unwrap_err();
        let msg = format!("{}", err.display_with_causes());
        assert!(msg.contains("expired"), "{msg}");
        assert!(
            msg.contains("injected failure"),
            "the error must carry the refresh failure cause: {msg}"
        );
    }

    #[mz_ore::test]
    fn refresh_wait_schedule() {
        // No credentials yet: retry at the floor.
        assert_eq!(next_refresh_wait(None), CREDENTIAL_MIN_REFRESH_INTERVAL);

        // No expiry: refresh at the cap.
        let creds = Credentials::new("akid", "secret", None, None, "test");
        assert_eq!(
            next_refresh_wait(Some(&creds)),
            CREDENTIAL_MAX_REFRESH_INTERVAL
        );

        // Mid-life: the refresh fraction of the remaining lifetime.
        let remaining = Duration::from_secs(600);
        let creds = Credentials::new(
            "akid",
            "secret",
            None,
            Some(SystemTime::now() + remaining),
            "test",
        );
        let expected = remaining.mul_f64(CREDENTIAL_REFRESH_FRACTION);
        let wait = next_refresh_wait(Some(&creds));
        assert!(
            wait > expected - Duration::from_secs(10) && wait <= expected,
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
            next_refresh_wait(Some(&creds)),
            CREDENTIAL_MIN_REFRESH_INTERVAL
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn lazy_cache_fetches_once_for_credentials_without_expiry() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, None);
        let cache = LazyCredentialCache::new(inner);

        for _ in 0..5 {
            let creds = cache.provide_credentials().await.unwrap();
            assert_eq!(creds.access_key_id(), "akid-1");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[mz_ore::test(tokio::test)]
    async fn lazy_cache_does_not_fetch_until_asked() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, None);
        let _cache = LazyCredentialCache::new(inner);
        tokio::task::yield_now().await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "building the cache must not fetch credentials"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn lazy_cache_serves_credentials_that_expire_after_the_buffer() {
        let ttl = CREDENTIAL_CACHE_EXPIRY_BUFFER.saturating_add(Duration::from_mins(30));
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, Some(ttl));
        let cache = LazyCredentialCache::new(inner);

        for _ in 0..3 {
            let creds = cache.provide_credentials().await.unwrap();
            assert_eq!(creds.access_key_id(), "akid-1");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[mz_ore::test(tokio::test)]
    async fn lazy_cache_refetches_credentials_inside_the_buffer() {
        // These credentials are unexpired, but expire sooner than the buffer
        // allows, so every call must fetch a new set.
        let ttl = CREDENTIAL_CACHE_EXPIRY_BUFFER / 2;
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, Some(ttl));
        let cache = LazyCredentialCache::new(inner);

        let first = cache.provide_credentials().await.unwrap();
        let second = cache.provide_credentials().await.unwrap();
        assert_eq!(first.access_key_id(), "akid-1");
        assert_eq!(second.access_key_id(), "akid-2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[mz_ore::test(tokio::test)]
    async fn lazy_cache_does_not_serve_stale_credentials_when_refetch_fails() {
        // The first fetch succeeds with credentials that are already inside
        // the buffer. Every later fetch fails.
        let (calls, inner) = CountingProvider::shared(0, 1, Some(Duration::ZERO));
        let cache = LazyCredentialCache::new(inner);

        let creds = cache.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-1");

        let err = cache.provide_credentials().await.unwrap_err();
        assert!(
            format!("{}", err.display_with_causes()).contains("injected failure"),
            "the caller must see the fetch failure: {err}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[mz_ore::test(tokio::test)]
    async fn lazy_cache_does_not_cache_failures() {
        // Every fetch fails, and each call must try again.
        let (calls, inner) = CountingProvider::shared(0, 0, None);
        let cache = LazyCredentialCache::new(inner);

        cache.provide_credentials().await.unwrap_err();
        cache.provide_credentials().await.unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn lazy_cache_times_out_a_hung_fetch() {
        // The first fetch never resolves. The paused clock runs to the
        // timeout as soon as the runtime is idle.
        let (calls, inner) = CountingProvider::shared(1, usize::MAX, None);
        let cache = LazyCredentialCache::new(inner);

        let err = cache.provide_credentials().await.unwrap_err();
        assert!(
            format!("{}", err.display_with_causes()).contains("timed out"),
            "the error must name the timeout: {err}"
        );

        let creds = cache.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// A test [`ProvideCredentials`] whose fetches wait for a permit on `gate`
    /// before they succeed, or fail if `fail` is set. A fetch returns its
    /// permit, so once one permit is added every fetch gets through.
    #[derive(Debug)]
    struct GatedProvider {
        calls: Arc<AtomicUsize>,
        gate: Arc<Semaphore>,
        fail: bool,
    }

    impl GatedProvider {
        fn cache(fail: bool) -> (Arc<AtomicUsize>, Arc<Semaphore>, LazyCredentialCache) {
            let calls = Arc::new(AtomicUsize::new(0));
            let gate = Arc::new(Semaphore::new(0));
            let provider = GatedProvider {
                calls: Arc::clone(&calls),
                gate: Arc::clone(&gate),
                fail,
            };
            let cache = LazyCredentialCache::new(SharedCredentialsProvider::new(provider));
            (calls, gate, cache)
        }
    }

    impl ProvideCredentials for GatedProvider {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            future::ProvideCredentials::new(async move {
                let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                let _permit = self.gate.acquire().await.expect("gate is never closed");
                if self.fail {
                    return Err(CredentialsError::provider_error("injected failure"));
                }
                Ok(Credentials::new(
                    format!("akid-{n}"),
                    "secret",
                    None,
                    None,
                    "test",
                ))
            })
        }
    }

    /// Lets the other branches of a `tokio::join!` run until they block, checks
    /// that exactly one fetch has started, and then opens the gate.
    async fn open_gate_after_one_fetch(calls: &AtomicUsize, gate: &Semaphore) {
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "callers that overlap must share one fetch"
        );
        gate.add_permits(1);
    }

    #[mz_ore::test(tokio::test)]
    async fn lazy_cache_shares_one_fetch_between_concurrent_callers() {
        let (calls, gate, cache) = GatedProvider::cache(false);

        let (a, b, c, ()) = tokio::join!(
            cache.provide_credentials(),
            cache.provide_credentials(),
            cache.provide_credentials(),
            open_gate_after_one_fetch(&calls, &gate),
        );
        for creds in [a.unwrap(), b.unwrap(), c.unwrap()] {
            assert_eq!(creds.access_key_id(), "akid-1");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[mz_ore::test(tokio::test)]
    async fn lazy_cache_shares_a_failed_fetch_with_waiting_callers() {
        let (calls, gate, cache) = GatedProvider::cache(true);

        let (a, b, c, ()) = tokio::join!(
            cache.provide_credentials(),
            cache.provide_credentials(),
            cache.provide_credentials(),
            open_gate_after_one_fetch(&calls, &gate),
        );
        for result in [a, b, c] {
            let err = result.unwrap_err();
            assert!(
                format!("{}", err.display_with_causes()).contains("injected failure"),
                "every waiting caller must see the fetch failure: {err}"
            );
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "callers that waited for a failed fetch must not each retry it"
        );

        // A caller that arrives after the failure does try again.
        cache.provide_credentials().await.unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    // NOTE: The refresh request tests run on tokio's paused clock, which the
    // cache uses to age credentials. Expiry uses `SystemTime`, which does not
    // pause, so these tests use credentials without expiry.

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn request_refresh_replaces_matching_credentials_after_min_reuse() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, None);
        let cache = LazyCredentialCache::new(inner);

        let creds = cache.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-1");

        tokio::time::sleep(CREDENTIAL_CACHE_MIN_REUSE).await;
        cache.request_refresh("akid-1");

        let creds = cache.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn request_refresh_is_ignored_within_min_reuse() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, None);
        let cache = LazyCredentialCache::new(inner);

        cache.provide_credentials().await.unwrap();
        tokio::time::sleep(CREDENTIAL_CACHE_MIN_REUSE / 2).await;
        cache.request_refresh("akid-1");

        let creds = cache.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn request_refresh_ignores_credentials_that_were_already_replaced() {
        let (calls, inner) = CountingProvider::shared(0, usize::MAX, None);
        let cache = LazyCredentialCache::new(inner);

        cache.provide_credentials().await.unwrap();
        tokio::time::sleep(CREDENTIAL_CACHE_MIN_REUSE).await;
        // The caller signed with credentials that the cache no longer holds.
        cache.request_refresh("akid-0");

        let creds = cache.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-1");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn request_refresh_keeps_serving_credentials_when_the_refresh_fails() {
        // Only the first fetch succeeds, so every refresh fails.
        let (calls, inner) = CountingProvider::shared(0, 1, None);
        let cache = LazyCredentialCache::new(inner);

        cache.provide_credentials().await.unwrap();
        tokio::time::sleep(CREDENTIAL_CACHE_MIN_REUSE).await;
        cache.request_refresh("akid-1");

        // The refresh is attempted, fails, and the cached credentials are
        // served.
        let creds = cache.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // The failed refresh restarts the floor, so neither another call nor
        // another rejection asks the failing provider again right away.
        cache.request_refresh("akid-1");
        let creds = cache.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        // Once the floor has passed, a rejection asks again.
        tokio::time::sleep(CREDENTIAL_CACHE_MIN_REUSE).await;
        cache.request_refresh("akid-1");
        let creds = cache.provide_credentials().await.unwrap();
        assert_eq!(creds.access_key_id(), "akid-1");
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    /// A test [`ProvideCredentials`] that reads and moves a fake clock, in
    /// milliseconds since the Unix epoch. Its first fetch returns credentials
    /// that leave the expiry buffer `margin` after the clock's current time.
    /// Each later fetch moves the clock past that point and then fails, as a
    /// refresh that takes a long time would.
    #[derive(Debug)]
    struct SlowFailingRefresh {
        calls: Arc<AtomicUsize>,
        clock: Arc<AtomicU64>,
        margin: Duration,
    }

    impl ProvideCredentials for SlowFailingRefresh {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            future::ProvideCredentials::new(async move {
                let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
                let margin = u64::try_from(self.margin.as_millis()).expect("fits");
                if n > 1 {
                    self.clock.fetch_add(margin.saturating_mul(2), Ordering::SeqCst);
                    return Err(CredentialsError::provider_error("injected failure"));
                }
                let now = Duration::from_millis(self.clock.load(Ordering::SeqCst));
                let expiry =
                    SystemTime::UNIX_EPOCH + now + CREDENTIAL_CACHE_EXPIRY_BUFFER + self.margin;
                Ok(Credentials::new(
                    "akid-1",
                    "secret",
                    None,
                    Some(expiry),
                    "test",
                ))
            })
        }
    }

    // NOTE: Expiry is compared against the fake clock, so this test does not
    // depend on how long anything takes. The paused tokio clock only ages the
    // credentials past the floor for `request_refresh`.
    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn failed_refresh_does_not_serve_credentials_that_entered_the_buffer() {
        let calls = Arc::new(AtomicUsize::new(0));
        let clock = Arc::new(AtomicU64::new(1_700_000_000_000));
        let provider = SlowFailingRefresh {
            calls: Arc::clone(&calls),
            clock: Arc::clone(&clock),
            margin: Duration::from_secs(1),
        };
        let now = NowFn::from(move || clock.load(Ordering::SeqCst));
        let cache = LazyCredentialCache::with_clock(SharedCredentialsProvider::new(provider), now);

        cache.provide_credentials().await.unwrap();
        assert!(cache.has_fresh_credentials());
        tokio::time::sleep(CREDENTIAL_CACHE_MIN_REUSE).await;
        cache.request_refresh("akid-1");

        // The cached credentials enter the expiry buffer while the refresh
        // runs, so its failure must not fall back to them.
        let err = cache.provide_credentials().await.unwrap_err();
        assert!(
            format!("{}", err.display_with_causes()).contains("injected failure"),
            "the caller must see the fetch failure: {err}"
        );
        assert!(!cache.has_fresh_credentials());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    fn cache_key(role_arn: &str) -> AwsCredentialsCacheKey {
        AwsCredentialsCacheKey {
            connection_id: CatalogItemId::User(1),
            role_arn: role_arn.to_string(),
            session_name: None,
            external_id: "mz_prefix_u1".to_string(),
            connection_role_arn: Some("arn:aws:iam::123456789000:role/Connection".to_string()),
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn credentials_cache_shares_entries_by_key() {
        let cache = AwsCredentialsCache::default();
        assert!(cache.get(&cache_key("role-a")).is_none());

        let (calls_a, provider_a) = CountingProvider::shared(0, usize::MAX, None);
        let (calls_a2, provider_a2) = CountingProvider::shared(0, usize::MAX, None);
        let (calls_b, provider_b) = CountingProvider::shared(0, usize::MAX, None);

        // The second insert for the same key loses, and both callers get the
        // first entry, so they share its cached credentials.
        let first = cache.get_or_insert(cache_key("role-a"), LazyCredentialCache::new(provider_a));
        let second =
            cache.get_or_insert(cache_key("role-a"), LazyCredentialCache::new(provider_a2));
        first.provide_credentials().await.unwrap();
        second.provide_credentials().await.unwrap();
        cache
            .get(&cache_key("role-a"))
            .expect("inserted above")
            .provide_credentials()
            .await
            .unwrap();
        assert_eq!(calls_a.load(Ordering::SeqCst), 1);
        assert_eq!(calls_a2.load(Ordering::SeqCst), 0);

        // A different role is a different entry.
        let other = cache.get_or_insert(cache_key("role-b"), LazyCredentialCache::new(provider_b));
        other.provide_credentials().await.unwrap();
        assert_eq!(calls_b.load(Ordering::SeqCst), 1);
        assert_eq!(calls_a.load(Ordering::SeqCst), 1);
    }

    #[mz_ore::test(tokio::test)]
    async fn credentials_cache_evicts_unheld_entries_without_fresh_credentials() {
        let cache = AwsCredentialsCache::default();
        let (_calls, stale) = CountingProvider::shared(0, usize::MAX, Some(Duration::ZERO));
        let (_calls, fresh) = CountingProvider::shared(0, usize::MAX, None);
        let (_calls, unused) = CountingProvider::shared(0, usize::MAX, None);
        let (_calls, other) = CountingProvider::shared(0, usize::MAX, None);

        // These credentials are inside the expiry buffer as soon as they are
        // fetched.
        let entry = cache.get_or_insert(cache_key("stale"), LazyCredentialCache::new(stale));
        entry.provide_credentials().await.unwrap();
        drop(entry);

        let entry = cache.get_or_insert(cache_key("fresh"), LazyCredentialCache::new(fresh));
        entry.provide_credentials().await.unwrap();
        drop(entry);

        // This entry never fetches credentials.
        let entry = cache.get_or_insert(cache_key("unused"), LazyCredentialCache::new(unused));
        drop(entry);

        let _held = cache.get_or_insert(cache_key("other"), LazyCredentialCache::new(other));
        assert!(cache.get(&cache_key("stale")).is_none());
        assert!(cache.get(&cache_key("unused")).is_none());
        assert!(cache.get(&cache_key("fresh")).is_some());
        assert!(cache.get(&cache_key("other")).is_some());
    }

    #[mz_ore::test(tokio::test)]
    async fn credentials_cache_keeps_held_entries_without_fresh_credentials() {
        let cache = AwsCredentialsCache::default();
        let ttl = Some(Duration::ZERO);
        let (calls_a, provider_a) = CountingProvider::shared(0, usize::MAX, ttl);
        let (_calls, provider_b) = CountingProvider::shared(0, usize::MAX, None);
        let (calls_a2, provider_a2) = CountingProvider::shared(0, usize::MAX, ttl);

        // A config holds the entry. Its credentials are never fresh.
        let held = cache.get_or_insert(cache_key("role-a"), LazyCredentialCache::new(provider_a));
        held.provide_credentials().await.unwrap();

        let entry = cache.get_or_insert(cache_key("role-b"), LazyCredentialCache::new(provider_b));
        drop(entry);

        // The next config for the same connection shares the held entry.
        let next = cache.get_or_insert(cache_key("role-a"), LazyCredentialCache::new(provider_a2));
        next.provide_credentials().await.unwrap();
        assert_eq!(calls_a.load(Ordering::SeqCst), 2);
        assert_eq!(calls_a2.load(Ordering::SeqCst), 0);
    }
}
