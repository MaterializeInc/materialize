// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An Azure Blob Storage implementation of [Blob] storage.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_trait::async_trait;
use azure_core::credentials::{AccessToken, TokenCredential, TokenRequestOptions};
use azure_core::error::ErrorKind;
use azure_core::http::headers::HeaderName;
use azure_core::http::{ExponentialRetryOptions, RetryOptions, StatusCode, Transport};
use azure_identity::{
    DeveloperToolsCredential, ManagedIdentityCredential, WorkloadIdentityCredential,
};
use azure_storage_blob::models::{
    BlobClientDownloadResult, BlobClientGetPropertiesResultHeaders,
    BlobContainerClientListBlobsOptions,
};
use azure_storage_blob::{BlobContainerClient, BlobContainerClientOptions};
use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use tracing::info;
use url::Url;
use uuid::Uuid;

use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;

use crate::cfg::BlobKnobs;
use crate::error::Error;
use crate::location::{Blob, BlobMetadata, Determinate, ExternalError};
use crate::metrics::S3BlobMetrics;

mod azurite;

/// A [TokenCredential] that tries each of its sources in order and returns the
/// first token one of them produces.
///
/// `azure_identity` 1.x offers no chaining credential, and the sources cannot
/// be picked once at construction time instead: [ManagedIdentityCredential]
/// constructs successfully on any host, so an eager choice would shadow the
/// developer-tools fallback on a laptop. Sources are re-tried on every token
/// request rather than latching onto the first that worked, so an identity that
/// becomes available later is picked up.
#[derive(Debug)]
struct ChainedTokenCredential(Vec<Arc<dyn TokenCredential>>);

#[async_trait]
impl TokenCredential for ChainedTokenCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        options: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        let mut errors = Vec::new();
        for source in &self.0 {
            match source.get_token(scopes, options.clone()).await {
                Ok(token) => return Ok(token),
                Err(err) => errors.push(format!("{source:?}: {err}")),
            }
        }
        Err(azure_core::Error::with_message_fn(
            ErrorKind::Credential,
            || {
                format!(
                    "no Azure credential produced a token:\n{}",
                    errors.join("\n")
                )
            },
        ))
    }
}

/// Returns the token credential to use when the blob URL carries no SAS token.
///
/// The chain mirrors the credential types the SDK's own default chain covered
/// before it was removed in 1.x: workload identity (AKS), managed identity
/// (App Service and VM/IMDS), then the local developer tools (`az login`,
/// `azd auth login`). Client secrets read from the environment are not
/// included; 1.x dropped that credential type, and we never provisioned one.
fn token_credential() -> Arc<dyn TokenCredential> {
    let mut sources: Vec<Arc<dyn TokenCredential>> = Vec::new();
    // Construction fails when the credential's environment is absent, e.g.
    // workload identity outside of a pod with a projected token. Log and skip:
    // a later source may still authenticate.
    match WorkloadIdentityCredential::new(None) {
        Ok(credential) => sources.push(credential),
        Err(err) => info!("azure: workload identity credentials unavailable: {err}"),
    }
    match ManagedIdentityCredential::new(None) {
        Ok(credential) => sources.push(credential),
        Err(err) => info!("azure: managed identity credentials unavailable: {err}"),
    }
    match DeveloperToolsCredential::new(None) {
        Ok(credential) => sources.push(credential),
        Err(err) => info!("azure: developer tools credentials unavailable: {err}"),
    }
    Arc::new(ChainedTokenCredential(sources))
}

/// Builds the HTTP client the Azure SDK transports its requests over.
///
/// The SDK's own client hardcodes 20s connect and 60s read timeouts, so we
/// supply a client that honors [BlobKnobs] instead.
///
/// NOTE: automatic decompression must stay off. `BlobClient::download`
/// reassembles a blob from range requests keyed by byte offset, which a
/// transparently decompressed body would invalidate. reqwest turns
/// decompression on by default for every codec whose feature is enabled, and
/// the SDK enables gzip and deflate, so we have to opt out explicitly.
fn http_client(knobs: &dyn BlobKnobs) -> reqwest_0_13::Client {
    reqwest_0_13::ClientBuilder::new()
        // The SDK defaults to rustls; pin it so an unrelated dependency
        // enabling native-tls cannot silently move Azure traffic onto it.
        .tls_backend_rustls()
        .timeout(knobs.operation_attempt_timeout())
        .read_timeout(knobs.read_timeout())
        .connect_timeout(knobs.connect_timeout())
        // Azure's REST API does not redirect, and following one would leak the
        // Authorization header to the redirect target.
        .redirect(reqwest_0_13::redirect::Policy::none())
        .no_gzip()
        .no_deflate()
        .no_brotli()
        .no_zstd()
        .build()
        .expect("valid config for azure HTTP client")
}

/// Configuration for opening an [AzureBlob].
#[derive(Clone)]
pub struct AzureBlobConfig {
    // The metrics struct here is a bit of a misnomer. We only need access
    // to the LgBytes metrics, which has an Azure-specific field. For now,
    // it saves considerable plumbing to reuse [S3BlobMetrics].
    //
    // TODO: spin up an AzureBlobMetrics and do the plumbing.
    metrics: S3BlobMetrics,
    // `BlobContainerClient` is neither `Clone` nor `Debug`, so it is shared
    // behind an `Arc` and `Debug` is implemented by hand.
    client: Arc<BlobContainerClient>,
    prefix: String,
}

impl Debug for AzureBlobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobConfig")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
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
        let mut options = BlobContainerClientOptions::default();
        options.client_options.transport = Some(Transport::new(Arc::new(http_client(&*knobs))));
        options.client_options.retry = RetryOptions::exponential(ExponentialRetryOptions {
            max_total_elapsed: azure_core::time::Duration::try_from(knobs.operation_timeout())
                .map_err(|e| Error::from(format!("operation timeout out of range: {e}")))?,
            ..Default::default()
        });

        let (container_url, credential) = if account == azurite::ACCOUNT {
            info!("Connecting to Azure emulator");
            // Azurite rejects Entra ID tokens, so requests are signed with the
            // Shared Key scheme by a policy instead of by a credential.
            options
                .client_options
                .per_try_policies
                .push(Arc::new(azurite::SharedKeyPolicy));
            options.version = azurite::API_VERSION.to_string();
            (azurite::container_url(&url, &container)?, None)
        } else {
            let endpoint = format!("https://{account}.blob.core.windows.net/{container}");
            match url.query() {
                // A SAS token is self-authenticating: it travels in the query
                // string and no credential is attached.
                //
                // NOTE: a SAS token provided this way is static and never
                // refreshed, so callers must provision one that outlives the
                // process. Token credentials refresh themselves.
                Some(sas) => {
                    let url = Url::parse(&format!("{endpoint}?{sas}"))
                        .map_err(|e| Error::from(format!("bad Azure container URL: {e}")))?;
                    (url, None)
                }
                None => {
                    let url = Url::parse(&endpoint)
                        .map_err(|e| Error::from(format!("bad Azure container URL: {e}")))?;
                    (url, Some(token_credential()))
                }
            }
        };

        let client = BlobContainerClient::new(container_url, credential, Some(options))
            .map_err(|e| Error::from(format!("azure container client: {e}")))?;

        Ok(AzureBlobConfig {
            metrics,
            client: Arc::new(client),
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
            azurite::ACCOUNT.to_string(),
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
pub struct AzureBlob {
    metrics: S3BlobMetrics,
    client: Arc<BlobContainerClient>,
    prefix: String,
}

impl Debug for AzureBlob {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlob")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl AzureBlob {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: AzureBlobConfig) -> Result<Self, ExternalError> {
        if azurite::is_emulator_url(config.client.url()) {
            // TODO: we could move this logic into the test harness.
            // it's currently here because it's surprisingly annoying to
            // create the container out-of-band
            if let Err(error) = config.client.create(None).await {
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

/// The blob's total size according to a download's initial response, or `None`
/// if the response did not report one.
///
/// `download` fetches a blob as a sequence of range requests, so its
/// `content_length` covers only the first range. `Content-Range` carries the
/// total after the slash (`bytes 0-1023/4096`). A blob served in a single
/// unranged response has no `Content-Range`, and its `content_length` is then
/// the whole blob.
fn total_len(response: &BlobClientDownloadResult) -> Option<u64> {
    const CONTENT_RANGE: HeaderName = HeaderName::from_static("content-range");
    match response.headers.get_optional_str(&CONTENT_RANGE) {
        Some(content_range) => content_range
            .rsplit_once('/')
            .and_then(|(_, total)| total.parse().ok()),
        None => response.properties.content_length,
    }
}

#[async_trait]
impl Blob for AzureBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(&path);

        let response = match blob.download(None).await {
            Ok(response) => response,
            Err(e) => {
                if e.http_status() == Some(StatusCode::NotFound) {
                    return Ok(None);
                }
                return Err(ExternalError::from(e.with_context("azure blob get error")));
            }
        };

        let expected_len = total_len(&response);
        let mut body = response.body;

        let mut segments = SegmentedBytes::new();
        while let Some(value) = body.next().await {
            let value = value
                .map_err(|e| ExternalError::from(e.with_context("azure blob get body error")))?;
            segments.push(value);
        }

        // Report if the length the service told us to expect didn't match the
        // number of bytes we read from the network.
        if expected_len.is_some_and(|len| len != u64::cast_from(segments.len())) {
            self.metrics.get_invalid_resp.inc();
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

        let mut pager = self
            .client
            .list_blobs(Some(BlobContainerClientListBlobsOptions {
                prefix: Some(blob_key_prefix),
                ..Default::default()
            }))
            .map_err(|e| ExternalError::from(e.with_context("azure blob list error")))?;

        while let Some(blob) = pager
            .try_next()
            .await
            .map_err(|e| ExternalError::from(e.with_context("azure blob list error")))?
        {
            let Some(name) = blob.name.as_deref() else {
                continue;
            };
            if let Some(key) = name.strip_prefix(&strippable_root_prefix) {
                let size_in_bytes = blob
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.content_length)
                    .unwrap_or(0);
                f(BlobMetadata { key, size_in_bytes });
            }
        }

        Ok(())
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(&path);

        // `.into()` selects `From<Bytes>`; the inherent `from(Vec<u8>)` would
        // shadow it and copy.
        blob.upload(value.into(), None)
            .await
            .map_err(|e| ExternalError::from(e.with_context("azure blob put error")))?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(&path);

        let properties = match blob.get_properties(None).await {
            Ok(properties) => properties,
            Err(e) => {
                if e.http_status() == Some(StatusCode::NotFound) {
                    return Ok(None);
                }
                return Err(ExternalError::from(e.with_context("azure blob error")));
            }
        };

        let size = usize::cast_from(
            properties
                .content_length()
                .map_err(|e| ExternalError::from(e.with_context("azure blob error")))?
                .unwrap_or(0),
        );
        blob.delete(None)
            .await
            .map_err(|e| ExternalError::from(e.with_context("azure blob delete error")))?;
        Ok(Some(size))
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(&path);

        match blob.get_properties(None).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.http_status() == Some(StatusCode::NotFound) {
                    return Err(Determinate::new(anyhow!(
                        "azure blob error: unable to restore non-existent key {key}"
                    ))
                    .into());
                }
                Err(ExternalError::from(e.with_context("azure blob error")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::location::tests::blob_impl_test;

    use super::*;

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
                    client: Arc::clone(&config.client),
                    prefix: config.prefix.clone(),
                };
                AzureBlob::open(config).await
            }
        })
        .await
    }
}
