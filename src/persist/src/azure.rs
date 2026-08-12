// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An Azure Blob Storage implementation of [Blob] storage.

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use azure_core::auth::{AccessToken, TokenCredential};
use azure_core::error::ErrorKind;
use azure_core::{ExponentialRetryOptions, RetryOptions, StatusCode, TransportOptions};
use azure_identity::{
    TokenCredentialOptions, create_default_credential, federated_credentials_flow,
};
use azure_storage::{CloudLocation, EMULATOR_ACCOUNT, prelude::*};
use azure_storage_blobs::blob::operations::GetBlobResponse;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::FuturesOrdered;
use futures_util::{FutureExt, StreamExt};
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tracing::{info, warn};
use url::Url;
use uuid::Uuid;

use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;

use crate::cfg::BlobKnobs;
use crate::error::Error;
use crate::location::{Blob, BlobMetadata, Determinate, ExternalError};
use crate::metrics::S3BlobMetrics;

/// Environment variables that configure AKS-style workload identity. The
/// names match the ones `azure_identity`'s credential chain reads.
const AZURE_TENANT_ID: &str = "AZURE_TENANT_ID";
const AZURE_CLIENT_ID: &str = "AZURE_CLIENT_ID";
const AZURE_FEDERATED_TOKEN_FILE: &str = "AZURE_FEDERATED_TOKEN_FILE";

/// Slop subtracted from an access token's expiry when deciding whether to
/// refresh it, so we never present a token that expires while a request is in
/// flight. Matches the slop in `azure_identity`'s internal token cache.
const TOKEN_EXPIRY_SLOP: Duration = Duration::from_secs(20);

fn is_expired(token: &AccessToken) -> bool {
    token.expires_on < OffsetDateTime::now_utc() + TOKEN_EXPIRY_SLOP
}

/// Exchanges a client assertion (the projected service account token) for an
/// AAD access token with the given scopes.
type ExchangeFn = Box<
    dyn Fn(String, Vec<String>) -> BoxFuture<'static, azure_core::Result<AccessToken>>
        + Send
        + Sync,
>;

/// A [TokenCredential] for AKS-style workload identity that re-reads the
/// projected service account token file on every AAD access token refresh.
///
/// `azure_identity`'s `WorkloadIdentityCredential` reads
/// `AZURE_FEDERATED_TOKEN_FILE` once at construction and holds the contents
/// for the life of the process. Kubernetes rotates the projected token, so
/// once the last cached AAD access token expires, every refresh presents an
/// expired client assertion and fails, permanently locking a long-running
/// process out of blob storage. Deferring the file read to refresh time picks
/// up rotations.
struct RefreshingWorkloadIdentityCredential {
    federated_token_file: PathBuf,
    exchange: ExchangeFn,
    /// AAD access tokens by requested scopes, refreshed when within
    /// [TOKEN_EXPIRY_SLOP] of expiry. Mirrors the caching semantics of
    /// `azure_identity`'s internal token cache.
    cache: RwLock<BTreeMap<Vec<String>, AccessToken>>,
}

impl Debug for RefreshingWorkloadIdentityCredential {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RefreshingWorkloadIdentityCredential")
            .field("federated_token_file", &self.federated_token_file)
            .finish_non_exhaustive()
    }
}

impl RefreshingWorkloadIdentityCredential {
    /// Returns a credential if the workload identity environment variables
    /// are present, or `None` to indicate that a different credential type
    /// must be used.
    fn from_env() -> Option<azure_core::Result<Self>> {
        let (Ok(tenant_id), Ok(client_id), Ok(token_file)) = (
            std::env::var(AZURE_TENANT_ID),
            std::env::var(AZURE_CLIENT_ID),
            std::env::var(AZURE_FEDERATED_TOKEN_FILE),
        ) else {
            return None;
        };
        Some(Self::new(tenant_id, client_id, PathBuf::from(token_file)))
    }

    fn new(
        tenant_id: String,
        client_id: String,
        federated_token_file: PathBuf,
    ) -> azure_core::Result<Self> {
        let options = TokenCredentialOptions::default();
        let http_client = options.http_client();
        let authority_host = options.authority_host()?;
        let exchange: ExchangeFn = Box::new(move |assertion, scopes| {
            let http_client = Arc::clone(&http_client);
            let authority_host = authority_host.clone();
            let tenant_id = tenant_id.clone();
            let client_id = client_id.clone();
            async move {
                let scopes: Vec<&str> = scopes.iter().map(String::as_str).collect();
                let res = federated_credentials_flow::perform(
                    http_client,
                    &client_id,
                    &assertion,
                    &scopes,
                    &tenant_id,
                    &authority_host,
                )
                .await
                .map_err(|err| {
                    azure_core::error::Error::full(
                        ErrorKind::Credential,
                        err,
                        "request token error",
                    )
                })?;
                Ok(AccessToken::new(
                    res.access_token().clone(),
                    OffsetDateTime::now_utc() + Duration::from_secs(res.expires_in),
                ))
            }
            .boxed()
        });
        Ok(Self::with_exchange(federated_token_file, exchange))
    }

    fn with_exchange(federated_token_file: PathBuf, exchange: ExchangeFn) -> Self {
        Self {
            federated_token_file,
            exchange,
            cache: RwLock::new(BTreeMap::new()),
        }
    }
}

#[async_trait]
impl TokenCredential for RefreshingWorkloadIdentityCredential {
    async fn get_token(&self, scopes: &[&str]) -> azure_core::Result<AccessToken> {
        let scopes_key: Vec<String> = scopes.iter().map(ToString::to_string).collect();

        {
            let cache = self.cache.read().await;
            if let Some(token) = cache.get(&scopes_key) {
                if !is_expired(token) {
                    return Ok(token.clone());
                }
            }
        }

        // Refresh under the write lock so concurrent callers don't stampede
        // AAD with token requests.
        let mut cache = self.cache.write().await;
        if let Some(token) = cache.get(&scopes_key) {
            if !is_expired(token) {
                return Ok(token.clone());
            }
        }

        let assertion = tokio::fs::read_to_string(&self.federated_token_file)
            .await
            .map_err(|err| {
                azure_core::error::Error::full(
                    ErrorKind::Credential,
                    err,
                    format!(
                        "failed to read federated token from file {}",
                        self.federated_token_file.display()
                    ),
                )
            })?;
        // Kubernetes writes the projected token without surrounding
        // whitespace, but a hand-provisioned file may have a trailing
        // newline, which would corrupt the client assertion.
        let assertion = assertion.trim().to_string();

        let token = (self.exchange)(assertion, scopes_key.clone()).await?;
        cache.insert(scopes_key, token.clone());
        Ok(token)
    }

    async fn clear_cache(&self) -> azure_core::Result<()> {
        self.cache.write().await.clear();
        Ok(())
    }
}

/// Returns the token credential to use when the blob URL carries no SAS
/// token.
///
/// Prefers [RefreshingWorkloadIdentityCredential] when its environment
/// variables are present, because the workload identity credential in
/// `azure_identity`'s default chain never re-reads the rotated token file.
/// Otherwise falls back to the default chain, whose remaining credential
/// types (e.g. managed identity via IMDS) refresh correctly.
fn token_credential() -> Arc<dyn TokenCredential> {
    match RefreshingWorkloadIdentityCredential::from_env() {
        Some(credential) => {
            info!("azure: using refreshing workload identity credentials");
            Arc::new(credential.expect("Azure workload identity credentials"))
        }
        None => create_default_credential().expect("Azure default credentials"),
    }
}

/// Configuration for opening an [AzureBlob].
#[derive(Clone, Debug)]
pub struct AzureBlobConfig {
    metrics: S3BlobMetrics,
    client: ContainerClient,
    prefix: String,
}

impl AzureBlobConfig {
    const EXTERNAL_TESTS_AZURE_CONTAINER: &'static str =
        "MZ_PERSIST_EXTERNAL_STORAGE_TEST_AZURE_CONTAINER";

    /// Returns a new [AzureBlobConfig] for use in production.
    ///
    /// Stores objects in the given container prepended with the (possibly empty)
    /// prefix. Azure credentials must be available in the process or environment.
    pub fn new(
        account: String,
        container: String,
        prefix: String,
        metrics: S3BlobMetrics,
        url: Url,
        knobs: Box<dyn BlobKnobs>,
    ) -> Result<Self, Error> {
        let transport = TransportOptions::new(Arc::new(
            reqwest::ClientBuilder::new()
                .timeout(knobs.operation_attempt_timeout())
                .read_timeout(knobs.read_timeout())
                .connect_timeout(knobs.connect_timeout())
                .build()
                .expect("valid config for azure HTTP client"),
        ));
        let retry = RetryOptions::exponential(
            ExponentialRetryOptions::default().max_total_elapsed(knobs.operation_timeout()),
        );

        let client = if account == EMULATOR_ACCOUNT {
            info!("Connecting to Azure emulator");
            ClientBuilder::with_location(
                CloudLocation::Emulator {
                    address: url.domain().expect("domain for Azure emulator").to_string(),
                    port: url.port().expect("port for Azure emulator"),
                },
                StorageCredentials::emulator(),
            )
        } else {
            let sas_credentials = match url.query() {
                Some(query) => Some(StorageCredentials::sas_token(query)),
                None => None,
            };

            let credentials = match sas_credentials {
                Some(Ok(credentials)) => credentials,
                Some(Err(err)) => {
                    warn!("Failed to parse SAS token: {err}");
                    // TODO: should we fallback here? Or can we fully rely on query params
                    // to determine whether a SAS token was provided?
                    StorageCredentials::token_credential(token_credential())
                }
                None => StorageCredentials::token_credential(token_credential()),
            };

            ClientBuilder::new(account, credentials)
        }
        .transport(transport)
        .retry(retry)
        .blob_service_client()
        .container_client(container);

        // NOTE: a SAS token provided via the URL query string is static and
        // never refreshed, so callers must provision one that outlives the
        // process. Token credentials (workload identity and managed identity)
        // refresh themselves.

        Ok(AzureBlobConfig {
            metrics,
            client,
            prefix,
        })
    }

    /// Returns a new [AzureBlobConfig] for use in unit tests.
    pub fn new_for_test() -> Result<Option<Self>, Error> {
        struct TestBlobKnobs;
        impl Debug for TestBlobKnobs {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TestBlobKnobs").finish_non_exhaustive()
            }
        }
        impl BlobKnobs for TestBlobKnobs {
            fn operation_timeout(&self) -> Duration {
                Duration::from_secs(30)
            }

            fn operation_attempt_timeout(&self) -> Duration {
                Duration::from_secs(10)
            }

            fn connect_timeout(&self) -> Duration {
                Duration::from_secs(5)
            }

            fn read_timeout(&self) -> Duration {
                Duration::from_secs(5)
            }

            fn is_cc_active(&self) -> bool {
                false
            }
        }

        let container_name = match std::env::var(Self::EXTERNAL_TESTS_AZURE_CONTAINER) {
            Ok(container) => container,
            Err(_) => {
                assert!(
                    !mz_ore::env::is_var_truthy("CI"),
                    "CI is supposed to run this test but something has gone wrong!"
                );
                return Ok(None);
            }
        };

        let prefix = Uuid::new_v4().to_string();
        let metrics = S3BlobMetrics::new(&MetricsRegistry::new());

        let config = AzureBlobConfig::new(
            EMULATOR_ACCOUNT.to_string(),
            container_name.clone(),
            prefix,
            metrics,
            Url::parse(&format!("http://localhost:40111/{}", container_name)).expect("valid url"),
            Box::new(TestBlobKnobs),
        )?;

        Ok(Some(config))
    }
}

/// Implementation of [Blob] backed by Azure Blob Storage.
#[derive(Debug)]
pub struct AzureBlob {
    metrics: S3BlobMetrics,
    client: ContainerClient,
    prefix: String,
}

impl AzureBlob {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: AzureBlobConfig) -> Result<Self, ExternalError> {
        if config.client.service_client().account() == EMULATOR_ACCOUNT {
            // TODO: we could move this logic into the test harness.
            // it's currently here because it's surprisingly annoying to
            // create the container out-of-band
            if let Err(error) = config.client.create().await {
                info!(
                    ?error,
                    "failed to create emulator container; this is expected on repeat runs"
                );
            }
        }

        let ret = AzureBlob {
            metrics: config.metrics,
            client: config.client,
            prefix: config.prefix,
        };

        Ok(ret)
    }

    fn get_path(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }
}

#[async_trait]
impl Blob for AzureBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(path);

        /// Fetch the body of a single [`GetBlobResponse`].
        async fn fetch_chunk(
            response: GetBlobResponse,
            metrics: S3BlobMetrics,
        ) -> Result<Vec<Bytes>, ExternalError> {
            let content_length = response.blob.properties.content_length;

            let mut parts: Vec<Bytes> = Vec::new();
            let mut total_len: u64 = 0;
            let mut body = response.data;
            while let Some(value) = body.next().await {
                let value = value
                    .map_err(|e| ExternalError::from(e.context("azure blob get body error")))?;
                total_len += u64::cast_from(value.len());
                parts.push(value);
            }

            // Report if the content-length header didn't match the number of
            // bytes we read from the network.
            if content_length != total_len {
                metrics.get_invalid_resp.inc();
            }

            Ok(parts)
        }

        let mut requests = FuturesOrdered::new();
        // TODO: the default chunk size is 1MB. We have not tried tuning it,
        // but making this configurable / running some benchmarks could be
        // valuable.
        let mut stream = blob.get().into_stream();

        while let Some(value) = stream.next().await {
            // Return early if any of the individual fetch requests return an error.
            let response = match value {
                Ok(v) => v,
                Err(e) => {
                    if let Some(e) = e.as_http_error() {
                        if e.status() == StatusCode::NotFound {
                            return Ok(None);
                        }
                    }

                    return Err(ExternalError::from(e.context("azure blob get error")));
                }
            };

            // Drive all of the fetch requests concurrently.
            let metrics = self.metrics.clone();
            requests.push_back(fetch_chunk(response, metrics));
        }

        // Await on all of our chunks.
        let mut segments = SegmentedBytes::with_capacity(requests.len());
        while let Some(body) = requests.next().await {
            for part in body.context("azure blob get body err")? {
                segments.push(part);
            }
        }

        Ok(Some(segments))
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        let blob_key_prefix = self.get_path(key_prefix);
        let strippable_root_prefix = format!("{}/", self.prefix);

        let mut stream = self
            .client
            .list_blobs()
            .prefix(blob_key_prefix.clone())
            .into_stream();

        while let Some(response) = stream.next().await {
            let response =
                response.map_err(|e| ExternalError::from(e.context("azure blob list error")))?;

            for blob in response.blobs.items {
                let azure_storage_blobs::container::operations::list_blobs::BlobItem::Blob(blob) =
                    blob
                else {
                    continue;
                };

                if let Some(key) = blob.name.strip_prefix(&strippable_root_prefix) {
                    let size_in_bytes = blob.properties.content_length;
                    f(BlobMetadata { key, size_in_bytes });
                }
            }
        }

        Ok(())
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(path);

        blob.put_block_blob(value)
            .await
            .map_err(|e| ExternalError::from(e.context("azure blob put error")))?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(path);

        match blob.get_properties().await {
            Ok(props) => {
                let size = usize::cast_from(props.blob.properties.content_length);
                blob.delete()
                    .await
                    .map_err(|e| ExternalError::from(e.context("azure blob delete error")))?;
                Ok(Some(size))
            }
            Err(e) => {
                if let Some(e) = e.as_http_error() {
                    if e.status() == StatusCode::NotFound {
                        return Ok(None);
                    }
                }

                Err(ExternalError::from(e.context("azure blob error")))
            }
        }
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(&path);

        match blob.get_properties().await {
            Ok(_) => Ok(()),
            Err(e) => {
                if let Some(e) = e.as_http_error() {
                    if e.status() == StatusCode::NotFound {
                        return Err(Determinate::new(anyhow!(
                            "azure blob error: unable to restore non-existent key {key}"
                        ))
                        .into());
                    }
                }

                Err(ExternalError::from(e.context("azure blob error")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use azure_core::auth::Secret;
    use std::sync::Mutex;
    use tracing::info;

    use crate::location::tests::blob_impl_test;

    use super::*;

    /// Tests that the credential re-reads the token file on every refresh,
    /// reuses cached tokens until they near expiry, and trims whitespace from
    /// the file contents.
    #[mz_ore::test(tokio::test)]
    async fn refreshing_workload_identity_credential() {
        struct MockExchange {
            /// Client assertions passed to each exchange call.
            assertions: Vec<String>,
            /// Validity of the next issued token.
            validity: Duration,
        }

        let token_file = tempfile::NamedTempFile::new().expect("create temp token file");
        std::fs::write(token_file.path(), "token-a\n").expect("write token file");

        let state = Arc::new(Mutex::new(MockExchange {
            assertions: Vec::new(),
            validity: Duration::ZERO,
        }));
        let exchange: ExchangeFn = Box::new({
            let state = Arc::clone(&state);
            move |assertion, _scopes| {
                let state = Arc::clone(&state);
                async move {
                    let mut state = state.lock().unwrap();
                    state.assertions.push(assertion);
                    Ok(AccessToken::new(
                        Secret::new(format!("aad-{}", state.assertions.len())),
                        OffsetDateTime::now_utc() + state.validity,
                    ))
                }
                .boxed()
            }
        });
        let credential = RefreshingWorkloadIdentityCredential::with_exchange(
            token_file.path().to_path_buf(),
            exchange,
        );
        let scopes = &["https://storage.azure.com/"];

        // Already-expired tokens are refreshed on every call, and the token
        // file is re-read (and trimmed) each time.
        let token = credential.get_token(scopes).await.expect("token");
        assert_eq!(token.token.secret(), "aad-1");
        std::fs::write(token_file.path(), "token-b").expect("write token file");
        let token = credential.get_token(scopes).await.expect("token");
        assert_eq!(token.token.secret(), "aad-2");
        assert_eq!(
            state.lock().unwrap().assertions,
            vec!["token-a".to_string(), "token-b".to_string()]
        );

        // Unexpired tokens are served from the cache without an exchange.
        state.lock().unwrap().validity = Duration::from_secs(3600);
        let token = credential.get_token(scopes).await.expect("token");
        assert_eq!(token.token.secret(), "aad-3");
        let token = credential.get_token(scopes).await.expect("token");
        assert_eq!(token.token.secret(), "aad-3");
        assert_eq!(state.lock().unwrap().assertions.len(), 3);

        // Clearing the cache forces a refresh.
        credential.clear_cache().await.expect("clear cache");
        let token = credential.get_token(scopes).await.expect("token");
        assert_eq!(token.token.secret(), "aad-4");
    }

    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn azure_blob() -> Result<(), ExternalError> {
        let config = match AzureBlobConfig::new_for_test()? {
            Some(client) => client,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    AzureBlobConfig::EXTERNAL_TESTS_AZURE_CONTAINER
                );
                return Ok(());
            }
        };

        blob_impl_test(move |_path| {
            let config = config.clone();
            async move {
                let config = AzureBlobConfig {
                    metrics: config.metrics.clone(),
                    client: config.client.clone(),
                    prefix: config.prefix.clone(),
                };
                AzureBlob::open(config).await
            }
        })
        .await
    }
}
