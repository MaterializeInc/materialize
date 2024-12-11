// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An Azure Blob Storage implementation of [Blob] storage.

use std::cmp;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use azure_identity::{create_default_credential, DefaultAzureCredential};
use azure_storage::{prelude::*, EMULATOR_ACCOUNT};
use azure_storage_blobs::prelude::*;
use bytes::Bytes;
use futures_util::stream::FuturesOrdered;
use futures_util::{FutureExt, StreamExt};
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::bytes::{MaybeLgBytes, SegmentedBytes};
use mz_ore::cast::CastFrom;
use mz_ore::lgbytes::{LgBytes, MetricsRegion};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::task::RuntimeExt;
use tokio::runtime::Handle as AsyncHandle;
use tracing::{debug, debug_span, trace, trace_span, Instrument};
use uuid::Uuid;

use crate::cfg::BlobKnobs;
use crate::error::Error;
use crate::location::{Blob, BlobMetadata, Determinate, ExternalError};
use crate::metrics::{ABSBlobMetrics, S3BlobMetrics};

/// Configuration for opening an [ABSBlob].
#[derive(Clone, Debug)]
pub struct ABSBlobConfig {
    metrics: S3BlobMetrics,
    client: ContainerClient,
    cfg: Arc<ConfigSet>,
}

impl ABSBlobConfig {
    const EXTERNAL_TESTS_ABS_CONTAINER: &'static str =
        "MZ_PERSIST_EXTERNAL_STORAGE_TEST_ABS_CONTAINER";

    /// Returns a new [ABSBlobConfig] for use in production.
    ///
    /// Stores objects in the given container prepended with the (possibly empty)
    /// prefix. Azure credentials must be available in the process or environment.
    pub fn new(
        account: String,
        container: String,
        prefix: String,
        metrics: S3BlobMetrics,
        cfg: Arc<ConfigSet>,
    ) -> Result<Self, Error> {
        // let is_cc_active = knobs.is_cc_active();

        // TODO: might need to pull out service client
        // to periodically refresh credentials
        let client = if account == EMULATOR_ACCOUNT {
            let credentials = StorageCredentials::emulator();
            let service_client = BlobServiceClient::new(account, credentials);
            service_client.container_client(container)
        } else {
            let credentials =
                create_default_credential().expect("default Azure credentials working");
            let service_client = BlobServiceClient::new(account, credentials);
            service_client.container_client(container)
        };

        let credential = create_default_credential().expect("default Azure credentials working");

        Ok(ABSBlobConfig {
            metrics,
            client,
            cfg,
        })
    }

    /// Returns a new [ABSBlobConfig] for use in unit tests.
    pub async fn new_for_test() -> Result<Option<Self>, Error> {
        let container_name = match std::env::var(Self::EXTERNAL_TESTS_ABS_CONTAINER) {
            Ok(container) => container,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        // let prefix = Uuid::new_v4().to_string();
        let metrics = S3BlobMetrics::new(&MetricsRegistry::new());

        let config = ABSBlobConfig::new(
            EMULATOR_ACCOUNT.to_string(),
            container_name,
            "".to_string(),
            metrics,
            Arc::new(ConfigSet::default()),
        )?;

        Ok(Some(config))
    }

    /// Returns a clone of Self with a new v4 uuid prefix.
    pub fn clone_with_new_uuid_prefix(&self) -> Self {
        let mut ret = self.clone();
        // ret.prefix = Uuid::new_v4().to_string();
        ret
    }
}

/// Implementation of [Blob] backed by Azure Blob Storage.
#[derive(Debug)]
pub struct ABSBlob {
    metrics: S3BlobMetrics,
    client: ContainerClient,
    container_name: String,
    prefix: String,
    cfg: Arc<ConfigSet>,
}

impl ABSBlob {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: ABSBlobConfig) -> Result<Self, ExternalError> {
        let container_name = config.client.container_name().to_string();
        let ret = ABSBlob {
            metrics: config.metrics,
            client: config.client,
            container_name,
            prefix: "".to_string(),
            cfg: config.cfg,
        };

        // Test connection before returning success
        let _ = ret.get("HEALTH_CHECK").await?;
        Ok(ret)
    }

    fn get_path(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }
}

#[async_trait]
impl Blob for ABSBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let path = self.get_path(key);

        let blob = self.client.blob_client(path);

        let mut segments: Vec<MaybeLgBytes> = vec![];

        let mut stream = blob.get().into_stream();
        while let Some(value) = stream.next().await {
            let response =
                value.map_err(|e| ExternalError::from(anyhow!("Azure blob get error: {}", e)))?;

            let content_length = response.blob.properties.content_length;
            let mut buffer = self
                .metrics
                .lgbytes
                .persist_abs
                .new_region(usize::cast_from(content_length));

            let mut body = response.data;
            while let Some(value) = body.next().await {
                let value = value.map_err(|e| {
                    ExternalError::from(anyhow!("Azure blob get body error: {}", e))
                })?;
                buffer.extend_from_slice(&value);
            }

            segments.push(MaybeLgBytes::LgBytes(LgBytes::from(Arc::new(buffer))));
        }

        Ok(Some(SegmentedBytes::from(segments)))
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
            .prefix(blob_key_prefix)
            .into_stream();

        while let Some(response) = stream.next().await {
            let response = response
                .map_err(|e| ExternalError::from(anyhow!("Azure list blobs error: {}", e)))?;

            for blob in response.blobs.items {
                let azure_storage_blobs::container::operations::list_blobs::BlobItem::Blob(blob) =
                    blob
                else {
                    continue;
                };

                if let Some(key) = blob.name.strip_prefix(&strippable_root_prefix) {
                    let size_in_bytes = blob.properties.content_length;
                    f(BlobMetadata {
                        key: key,
                        size_in_bytes,
                    });
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
            .map_err(|e| ExternalError::from(anyhow!("Azure blob put error: {}", e)))?;

        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(path);

        match blob.get_properties().await {
            Ok(props) => {
                let size = props.blob.properties.content_length as usize;
                blob.delete()
                    .await
                    .map_err(|e| ExternalError::from(anyhow!("Azure blob delete error: {}", e)))?;
                Ok(Some(size))
            }
            // WIP: figure out what this is if empty
            // Err(e) if e.status_code() == 404 => Ok(None),
            Err(e) => Err(ExternalError::from(anyhow!("Azure blob error: {}", e))),
        }
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        let blob = self.client.blob_client(&path);

        match blob.get_properties().await {
            Ok(_) => Ok(()),
            // WIP
            // Err(e) if e.status_code() == 404 => Err(Determinate::new(anyhow!(
            //     "unable to restore {key} in Azure Blob Storage: blob does not exist"
            // ))
            // .into()),
            Err(e) => Err(ExternalError::from(anyhow!("Azure blob error: {}", e))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::location::tests::blob_impl_test;
    use tracing::info;

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn abs_blob() -> Result<(), ExternalError> {
        let config = match ABSBlobConfig::new_for_test().await? {
            Some(client) => client,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    ABSBlobConfig::EXTERNAL_TESTS_ABS_CONTAINER
                );
                return Ok(());
            }
        };

        blob_impl_test(move |path| {
            let path = path.to_owned();
            let config = config.clone();
            async move {
                let config = ABSBlobConfig {
                    metrics: config.metrics.clone(),
                    client: config.client.clone(),
                    container_name: config.container_name.clone(),
                    prefix: format!("{}/abs_blob_impl_test/{}", config.prefix, path),
                    cfg: Arc::new(
                        ConfigSet::default()
                            .add(&ENABLE_ABS_LGALLOC_CC_SIZES)
                            .add(&ENABLE_ABS_LGALLOC_NONCC_SIZES)
                            .add(&ENABLE_ONE_ALLOC_PER_REQUEST),
                    ),
                    is_cc_active: true,
                };
                ABSBlob::open(config).await
            }
        })
        .await
    }
}
