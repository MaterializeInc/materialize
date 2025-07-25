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
use azure_core::{ExponentialRetryOptions, RetryOptions, StatusCode, TransportOptions};
use azure_identity::create_default_credential;
use azure_storage::{CloudLocation, EMULATOR_ACCOUNT, prelude::*};
use azure_storage_blobs::blob::operations::GetBlobResponse;
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesOrdered;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
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

/// Configuration for opening an [AzureBlob].
#[derive(Clone, Debug)]
pub struct AzureBlobConfig {
    // The metrics struct here is a bit of a misnomer. We only need access
    // to the LgBytes metrics, which has an Azure-specific field. For now,
    // it saves considerable plumbing to reuse [S3BlobMetrics].
    //
    // TODO: spin up an AzureBlobMetrics and do the plumbing.
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
        let client = if account == EMULATOR_ACCOUNT {
            info!("Connecting to Azure emulator");
            ClientBuilder::with_location(
                CloudLocation::Emulator {
                    address: url.domain().expect("domain for Azure emulator").to_string(),
                    port: url.port().expect("port for Azure emulator"),
                },
                StorageCredentials::emulator(),
            )
            .transport({
                // Azure uses reqwest / hyper internally, but we specify a client explicitly to
                // plumb through our timeouts.
                TransportOptions::new(Arc::new(
                    reqwest::ClientBuilder::new()
                        .timeout(knobs.operation_attempt_timeout())
                        .read_timeout(knobs.read_timeout())
                        .connect_timeout(knobs.connect_timeout())
                        .build()
                        .expect("valid config for azure HTTP client"),
                ))
            })
            .retry(RetryOptions::exponential(
                ExponentialRetryOptions::default().max_total_elapsed(knobs.operation_timeout()),
            ))
            .blob_service_client()
            .container_client(container)
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
                    StorageCredentials::token_credential(
                        create_default_credential().expect("Azure default credentials"),
                    )
                }
                None => {
                    // Fall back to default credential stack to support auth modes like
                    // workload identity that are injected into the environment
                    StorageCredentials::token_credential(
                        create_default_credential().expect("Azure default credentials"),
                    )
                }
            };

            let service_client = BlobServiceClient::new(account, credentials);
            service_client.container_client(container)
        };

        // TODO: some auth modes like user-delegated SAS tokens are time-limited
        // and need to be refreshed. This can be done through `service_client.update_credentials`
        // but there'll be a fair bit of plumbing needed to make each mode work

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
        }

        let container_name = match std::env::var(Self::EXTERNAL_TESTS_AZURE_CONTAINER) {
            Ok(container) => container,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
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
        ) -> Result<Bytes, ExternalError> {
            let content_length = response.blob.properties.content_length;

            // Here we're being quite defensive. If `content_length` comes back
            // as 0 it's most likely incorrect. In that case we'll copy bytes
            // of the network into a growable buffer, then copy the entire
            // buffer into lgalloc.
            let mut buffer = match content_length {
                1.. => {
                    let region = Vec::with_capacity(usize::cast_from(content_length));
                    PreSizedBuffer::Sized(region)
                }
                0 => PreSizedBuffer::Unknown(SegmentedBytes::new()),
            };

            let mut body = response.data;
            while let Some(value) = body.next().await {
                let value = value
                    .map_err(|e| ExternalError::from(e.context("azure blob get body error")))?;

                match &mut buffer {
                    PreSizedBuffer::Sized(region) => region.extend_from_slice(&value),
                    PreSizedBuffer::Unknown(segments) => segments.push(value),
                }
            }

            // Spill our bytes to lgalloc, if they aren't already.
            let lgbytes: Bytes = match buffer {
                PreSizedBuffer::Sized(region) => region.into(),
                // Now that we've collected all of the segments, we know the size of our region.
                PreSizedBuffer::Unknown(segments) => {
                    let mut region = Vec::with_capacity(segments.len());
                    for segment in segments.into_segments() {
                        region.extend_from_slice(segment.as_ref());
                    }
                    region.into()
                }
            };

            // Report if the content-length header didn't match the number of
            // bytes we read from the network.
            if content_length != u64::cast_from(lgbytes.len()) {
                metrics.get_invalid_resp.inc();
            }

            Ok(lgbytes)
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
            let segment = body.context("azure blob get body err")?;
            segments.push(segment);
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

/// If possible we'll pre-allocate a chunk of memory in lgalloc and write into
/// that as we read bytes off the network.
enum PreSizedBuffer {
    Sized(Vec<u8>),
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
                    client: config.client.clone(),
                    prefix: config.prefix.clone(),
                };
                AzureBlob::open(config).await
            }
        })
        .await
    }
}
