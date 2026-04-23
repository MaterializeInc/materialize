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

use anyhow::anyhow;
use async_trait::async_trait;
use azure_core::Bytes;
use azure_core::credentials::TokenCredential;
use azure_core::http::{ExponentialRetryOptions, RetryOptions, StatusCode};
use azure_identity::{DeveloperToolsCredential, WorkloadIdentityCredential};
use azure_storage_blob::models::{
    BlobClientDownloadOptions, BlobClientGetPropertiesResultHeaders,
    BlobContainerClientListBlobsOptions,
};
use azure_storage_blob::{BlobContainerClient, BlobContainerClientOptions};
use futures_util::{StreamExt, TryStreamExt};
use url::Url;

use mz_dyncfg::ConfigSet;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;

use crate::cfg::BlobKnobs;
use crate::error::Error;
use crate::location::{Blob, BlobMetadata, Determinate, ExternalError};
use crate::metrics::S3BlobMetrics;

/// The well-known account name used by Azurite, the Azure Storage emulator.
const AZURITE_ACCOUNT: &str = "devstoreaccount1";

/// Configuration for opening an [AzureBlob].
#[derive(Clone)]
pub struct AzureBlobConfig {
    // The metrics struct here is a bit of a misnomer. We only need access
    // to the LgBytes metrics, which has an Azure-specific field. For now,
    // it saves considerable plumbing to reuse [S3BlobMetrics].
    //
    // TODO: spin up an AzureBlobMetrics and do the plumbing.
    metrics: S3BlobMetrics,
    // `BlobContainerClient` is neither `Clone` nor `Debug`; wrap in `Arc`
    // and implement `Debug` manually.
    client: Arc<BlobContainerClient>,
    prefix: String,
    cfg: Arc<ConfigSet>,
}

impl Debug for AzureBlobConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureBlobConfig")
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl AzureBlobConfig {
    #[cfg(test)]
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
        cfg: Arc<ConfigSet>,
    ) -> Result<Self, Error> {
        let mut options = BlobContainerClientOptions::default();

        // Custom reqwest client to plumb BlobKnobs timeouts, and to pin
        // rustls + aws-lc-rs for Azure traffic ahead of the workspace-wide
        // HTTP-clients migration.
        let reqwest_client = reqwest::ClientBuilder::new()
            .tls_backend_rustls()
            .timeout(knobs.operation_attempt_timeout())
            .read_timeout(knobs.read_timeout())
            .connect_timeout(knobs.connect_timeout())
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("valid config for azure HTTP client");
        options.client_options.transport =
            Some(azure_core::http::Transport::new(Arc::new(reqwest_client)));

        options.client_options.retry = RetryOptions::exponential(ExponentialRetryOptions {
            max_total_elapsed: azure_core::time::Duration::try_from(knobs.operation_timeout())
                .expect("operation_timeout in representable range"),
            ..Default::default()
        });

        let client = if account == AZURITE_ACCOUNT {
            // Azurite test support is restored in a follow-up commit (it
            // needs the new SDK's `Policy` trait to implement Shared Key
            // signing, which the old SDK provided built-in).
            let _ = (url, container);
            return Err(Error::from(
                "Azurite emulator temporarily unsupported; restored in the \
                 Azurite shared-key follow-up commit",
            ));
        } else {
            let endpoint = format!("https://{account}.blob.core.windows.net/");
            if let Some(sas) = url.query() {
                // SAS is self-authenticating; pass via query string with no credential.
                let container_url = Url::parse(&format!("{endpoint}{container}?{sas}"))
                    .map_err(|e| Error::from(format!("bad Azure container URL: {e}")))?;
                BlobContainerClient::from_url(container_url, None, Some(options))
                    .map_err(|e| Error::from(format!("azure container client: {e}")))?
            } else {
                let credential = create_default_credential().expect("Azure default credentials");
                BlobContainerClient::new(&endpoint, &container, Some(credential), Some(options))
                    .map_err(|e| Error::from(format!("azure container client: {e}")))?
            }
        };

        // TODO: some auth modes like user-delegated SAS tokens are time-limited
        // and need to be refreshed. This requires plumbing an updatable
        // credential through per_try_policies.

        Ok(AzureBlobConfig {
            metrics,
            client: Arc::new(client),
            cfg,
            prefix,
        })
    }

    /// Returns a new [AzureBlobConfig] for use in unit tests.
    ///
    /// Stubbed for now: Azurite requires Shared Key signing, which is
    /// restored in a follow-up commit. Returning `None` makes all
    /// Azurite-dependent tests skip.
    #[cfg(test)]
    pub fn new_for_test() -> Result<Option<Self>, Error> {
        Ok(None)
    }
}

/// Try `WorkloadIdentityCredential` (k8s with Azure AD) first, then fall back
/// to `DeveloperToolsCredential` (`az login`, etc.) for local use.
fn create_default_credential() -> azure_core::Result<Arc<dyn TokenCredential>> {
    if let Ok(cred) = WorkloadIdentityCredential::new(None) {
        return Ok(cred);
    }
    DeveloperToolsCredential::new(None).map(|c| -> Arc<dyn TokenCredential> { c })
}

/// Implementation of [Blob] backed by Azure Blob Storage.
pub struct AzureBlob {
    metrics: S3BlobMetrics,
    client: Arc<BlobContainerClient>,
    prefix: String,
    _cfg: Arc<ConfigSet>,
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
    // `open` is intentionally `async`: the follow-up commit adds a
    // `#[cfg(test)]` Azurite container-create call that awaits. The
    // production path currently performs no awaits.
    #[allow(clippy::unused_async)]
    pub async fn open(config: AzureBlobConfig) -> Result<Self, ExternalError> {
        let ret = AzureBlob {
            metrics: config.metrics,
            client: config.client,
            prefix: config.prefix,
            _cfg: config.cfg,
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
        let blob = self.client.blob_client(&path);

        // Pre-allocate an lgalloc region sized to content-length and stream into it.
        let response = match blob
            .download(Some(BlobClientDownloadOptions::default()))
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if e.http_status() == Some(StatusCode::NotFound) {
                    return Ok(None);
                }
                return Err(ExternalError::from(e.with_context("azure blob get error")));
            }
        };

        let content_length = response.properties.content_length.unwrap_or(0);
        let mut body = response.body;

        let mut buffer = if content_length > 0 {
            PreSizedBuffer::Sized(
                self.metrics
                    .lgbytes
                    .persist_azure
                    .new_region(usize::cast_from(content_length)),
            )
        } else {
            // Size unknown: grow into a segmented buffer, then copy into lgalloc.
            PreSizedBuffer::Unknown(SegmentedBytes::new())
        };

        while let Some(value) = body.next().await {
            let value = value
                .map_err(|e| ExternalError::from(e.with_context("azure blob get body error")))?;
            match &mut buffer {
                PreSizedBuffer::Sized(region) => region.extend_from_slice(&value),
                PreSizedBuffer::Unknown(segments) => segments.push(value),
            }
        }

        let lgbytes: Bytes = match buffer {
            PreSizedBuffer::Sized(region) => region.into(),
            PreSizedBuffer::Unknown(segments) => {
                let mut region = self
                    .metrics
                    .lgbytes
                    .persist_azure
                    .new_region(segments.len());
                for segment in segments.into_segments() {
                    region.extend_from_slice(segment.as_ref());
                }
                region.into()
            }
        };

        if content_length > 0 && content_length != u64::cast_from(lgbytes.len()) {
            self.metrics.get_invalid_resp.inc();
        }

        let mut out = SegmentedBytes::with_capacity(1);
        out.push(lgbytes);
        Ok(Some(out))
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
                    .and_then(|p| p.content_length)
                    .unwrap_or(0);
                f(BlobMetadata { key, size_in_bytes });
            }
        }

        Ok(())
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(&path);

        // `.into()` selects `From<Bytes>`; the inherent `From<Vec<u8>>` would shadow it.
        blob.upload(value.into(), None)
            .await
            .map_err(|e| ExternalError::from(e.with_context("azure blob put error")))?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(&path);

        let props = match blob.get_properties(None).await {
            Ok(p) => p,
            Err(e) => {
                if e.http_status() == Some(StatusCode::NotFound) {
                    return Ok(None);
                }
                return Err(ExternalError::from(e.with_context("azure blob error")));
            }
        };

        let size = usize::cast_from(
            props
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

/// If possible we'll pre-allocate a chunk of memory in lgalloc and write into
/// that as we read bytes off the network.
enum PreSizedBuffer {
    Sized(mz_ore::lgbytes::MetricsRegion<u8>),
    Unknown(SegmentedBytes),
}

#[cfg(test)]
mod tests {
    use tracing::info;

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
                    cfg: Arc::new(ConfigSet::default()),
                    prefix: config.prefix.clone(),
                };
                AzureBlob::open(config).await
            }
        })
        .await
    }
}
