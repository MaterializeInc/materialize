// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Blob] backed by Google Cloud Storage.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use bytes::Bytes;
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_storage::client::{Storage, StorageControl};
use uuid::Uuid;

use mz_dyncfg::ConfigSet;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::metrics::MetricsRegistry;

use crate::location::{Blob, BlobMetadata, Determinate, ExternalError};
use crate::metrics::S3BlobMetrics;

/// Configuration for opening a [GcsBlob]
#[derive(Clone, Debug)]
pub struct GcsBlobConfig {
    // The metrics struct here is a bit of a misnomer. We only need access
    // to the LgBytes metrics, which has a GCS-specific field. For now,
    // it saves considerable plumbing to reuse [S3BlobMetrics].
    //
    // TODO: spin up an GcsBlobMetrics and do the plumbing.
    metrics: S3BlobMetrics,
    storage_client: Storage,
    storage_control_client: StorageControl,
    // TODO: project?
    // TODO prefix bucket as /projects/_/buckets/{bucket}?
    bucket: String,
    prefix: String,
    cfg: Arc<ConfigSet>,
}

impl GcsBlobConfig {
    const EXTERNAL_TESTS_GCS_BUCKET: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_GCS_BUCKET";

    /// Returns a new [GcsBlobConfig] for use in production.
    ///
    /// Stores objects in the given bucket prepended with the (possibly empty)
    /// prefix. GCS credentials and region must be available in the process or
    /// environment.
    pub async fn new(
        bucket: String,
        prefix: String,
        // TODO optional credentials?
        // TODO override URL/endpoints?
        metrics: S3BlobMetrics,
        cfg: Arc<ConfigSet>,
    ) -> Result<Self, ExternalError> {
        let storage_client = Storage::builder()
            .build()
            .await
            .map_err(|e| ExternalError::from(anyhow!("GCS Storage builder error: {}", e)))?;
        let storage_control_client = StorageControl::builder()
            .build()
            .await
            .map_err(|e| ExternalError::from(anyhow!("GCS StorageControl builder error: {}", e)))?;
        Ok(GcsBlobConfig {
            metrics,
            storage_client,
            storage_control_client,
            bucket,
            prefix,
            cfg,
        })
    }

    /// Returns a new [GcsBlobConfig] for use in tests.
    pub async fn new_for_test() -> Result<Option<Self>, ExternalError> {
        let bucket = match std::env::var(Self::EXTERNAL_TESTS_GCS_BUCKET) {
            Ok(bucket) => bucket,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };
        let prefix = Uuid::new_v4().to_string();
        let metrics = S3BlobMetrics::new(&MetricsRegistry::new());
        let cfg = Arc::new(ConfigSet::default());
        Ok(Some(
            GcsBlobConfig::new(bucket, prefix, metrics, cfg).await?,
        ))
    }
}

/// Implementation of [Blob] backed by GCS
#[derive(Debug)]
pub struct GcsBlob {
    metrics: S3BlobMetrics,
    storage_client: Storage,
    storage_control_client: StorageControl,
    // TODO: project?
    bucket: String,
    prefix: String,
    cfg: Arc<ConfigSet>,
}

impl GcsBlob {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: GcsBlobConfig) -> Result<Self, ExternalError> {
        let ret = GcsBlob {
            metrics: config.metrics,
            storage_client: config.storage_client,
            storage_control_client: config.storage_control_client,
            bucket: config.bucket,
            prefix: config.prefix,
            cfg: config.cfg,
        };
        // Connect before returning success. We don't particularly care about
        // what's stored in this blob (nothing writes to it, so presumably it's
        // empty) just that we were able and allowed to fetch it.
        let _ = ret.get("HEALTH_CHECK").await?;
        Ok(ret)
    }

    fn get_path(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }
}

#[async_trait]
impl Blob for GcsBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let path = self.get_path(key);

        let size = usize::try_from(
            self.storage_control_client
                .get_object()
                .set_bucket(&self.bucket)
                .set_object(&path)
                .send()
                .await
                // TODO inspect_err
                .context("GCS get_object err")?
                .size,
        )
        .context("GCS sent negative size value")?;
        let mut buf: Vec<u8> = Vec::with_capacity(size);

        let mut stream = self
            .storage_client
            .read_object(&self.bucket, path)
            .send()
            .await
            .context("GCS read_object err")?;
        while let Some(value) = stream.next().await {
            let data = match value {
                Ok(v) => v,
                Err(e) => {
                    if let Some(status) = e.status() {
                        if status.code == Code::NotFound {
                            return Ok(None);
                        }
                    }
                    return Err(ExternalError::from(anyhow!("GCS blob get error: {:?}", e)));
                }
            };
            buf.extend_from_slice(&data[..])
        }
        Ok(Some(SegmentedBytes::from(buf)))
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        let blob_key_prefix = self.get_path(key_prefix);
        let strippable_root_prefix = format!("{}/", self.prefix);

        let mut stream = self
            .storage_control_client
            .list_objects()
            .set_parent(&self.bucket)
            .set_prefix(blob_key_prefix)
            .by_item();
        while let Some(response) = stream.next().await {
            let obj = response
                .map_err(|e| ExternalError::from(anyhow!("GCS list blobs error: {}", e)))?;
            if let Some(key) = obj.name.strip_prefix(&strippable_root_prefix) {
                let size_in_bytes =
                    u64::try_from(obj.size).context("GCS sent negative size value")?;
                f(BlobMetadata { key, size_in_bytes })
            } else {
                return Err(ExternalError::from(anyhow!(
                    "found key with invalid prefix: {}",
                    obj.name
                )));
            }
        }
        Ok(())
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        self.storage_client
            .write_object(&self.bucket, path, value)
            .send_unbuffered()
            .await
            .map_err(|e| ExternalError::from(anyhow!("GCS blob put error: {}", e)))?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let path = self.get_path(key);
        // TODO metrics

        match self
            .storage_control_client
            .get_object()
            .set_bucket(&self.bucket)
            .set_object(&path)
            .send()
            .await
        {
            Ok(obj) => {
                let size = usize::try_from(obj.size).context("GCS sent negative size value")?;
                self.storage_control_client
                    .delete_object()
                    .set_bucket(&self.bucket)
                    .set_object(path)
                    .send()
                    .await
                    .map_err(|e| ExternalError::from(anyhow!("GCS blob delete error: {:?}", e)))?;
                Ok(Some(size))
            }
            Err(e) => {
                if let Some(status) = e.status() {
                    if status.code == Code::NotFound {
                        return Ok(None);
                    }
                }
                Err(ExternalError::from(anyhow!("GCS blob get error: {:?}", e)))
            }
        }
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        let mut stream = self
            .storage_control_client
            .list_objects()
            .set_parent(&self.bucket)
            .set_prefix(&path)
            .set_versions(true)
            .by_item();
        let mut generation = i64::MIN;
        let mut latest_obj = None;
        while let Some(response) = stream.next().await {
            let obj = response.map_err(|e| {
                ExternalError::from(anyhow!("GCS restore list_objects error: {}", e))
            })?;
            if obj.name != path {
                // We found something with the same prefix, but not that exact blob.
                continue;
            }
            if obj.generation > generation {
                generation = obj.generation;
                latest_obj = Some(obj);
            }
        }
        match latest_obj {
            Some(obj) => match obj.delete_time {
                // Object is deleted, so try to restore it
                Some(_) => {
                    self.storage_control_client
                        .restore_object()
                        .set_bucket(&self.bucket)
                        .set_object(obj.name)
                        .set_generation(obj.generation)
                        .set_if_generation_match(0)
                        .send()
                        .await
                        .map_err(|e| {
                            ExternalError::from(anyhow!("GCS restore_object error: {}", e))
                        })?;
                    Ok(())
                }
                // Object exists and is not deleted
                None => Ok(()),
            },
            None => Err(Determinate::new(anyhow!(
                "unable to restore {key} in GCS: no valid version exists"
            ))
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::location::tests::blob_impl_test;
    use tracing::info;
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn gcs_blob() -> Result<(), ExternalError> {
        let config = match GcsBlobConfig::new_for_test().await? {
            Some(client) => client,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    GcsBlobConfig::EXTERNAL_TESTS_GCS_BUCKET
                );
                return Ok(());
            }
        };
        blob_impl_test(move |_path| {
            let config = config.clone();
            async move {
                let config = GcsBlobConfig {
                    metrics: config.metrics.clone(),
                    storage_client: config.storage_client.clone(),
                    storage_control_client: config.storage_control_client.clone(),
                    bucket: config.bucket.clone(),
                    prefix: config.prefix.clone(),
                    cfg: Arc::new(ConfigSet::default()),
                };
                GcsBlob::open(config).await
            }
        })
        .await
    }
}
