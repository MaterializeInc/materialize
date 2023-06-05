// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-process caches of [Blob].

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use moka::notification::RemovalCause;
use moka::sync::Cache;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_persist::location::{Atomicity, Blob, BlobMetadata, ExternalError};
use tracing::error;

use crate::cfg::PersistConfig;
use crate::internal::metrics::Metrics;

// In-memory cache for [Blob].
#[derive(Debug)]
pub struct BlobMemCache {
    metrics: Arc<Metrics>,
    cache: Cache<String, SegmentedBytes>,
    blob: Arc<dyn Blob + Send + Sync>,
}

impl BlobMemCache {
    pub fn new(
        cfg: &PersistConfig,
        metrics: Arc<Metrics>,
        blob: Arc<dyn Blob + Send + Sync>,
    ) -> Arc<dyn Blob + Send + Sync> {
        let eviction_metrics = Arc::clone(&metrics);
        // TODO: Make this react dynamically to changes in configuration.
        let cache = Cache::<String, SegmentedBytes>::builder()
            .max_capacity(u64::cast_from(cfg.dynamic.blob_cache_mem_limit_bytes()))
            .weigher(|k, v| {
                u32::try_from(v.len()).unwrap_or_else(|_| {
                    // We chunk off blobs at 128MiB, so the length should easily
                    // fit in a u32.
                    error!(
                        "unexpectedly large blob in persist cache {} bytes: {}",
                        v.len(),
                        k
                    );
                    u32::MAX
                })
            })
            .eviction_listener(move |_k, _v, cause| match cause {
                RemovalCause::Size => eviction_metrics.blob_cache_mem.evictions.inc(),
                RemovalCause::Expired | RemovalCause::Explicit | RemovalCause::Replaced => {}
            })
            .build();
        let blob = BlobMemCache {
            metrics,
            cache,
            blob,
        };
        Arc::new(blob)
    }

    fn update_size_metrics(&self) {
        self.metrics
            .blob_cache_mem
            .size_blobs
            .set(self.cache.entry_count());
        self.metrics
            .blob_cache_mem
            .size_bytes
            .set(self.cache.weighted_size());
    }
}

#[async_trait]
impl Blob for BlobMemCache {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        // First check if the blob is in the cache. If it is, return it. If not,
        // fetch it and put it in the cache.
        //
        // Blobs are write-once modify-never, so we don't have to worry about
        // any races or cache invalidations here. If the value is in the cache,
        // it's also what's in s3 (if not, then there's a horrible bug somewhere
        // else).
        if let Some(cached_value) = self.cache.get(key) {
            self.metrics.blob_cache_mem.hits_blobs.inc();
            self.metrics
                .blob_cache_mem
                .hits_bytes
                .inc_by(u64::cast_from(cached_value.len()));
            return Ok(Some(cached_value));
        }

        // This could maybe use moka's async cache to unify any concurrent
        // fetches for the same key? That's not particularly expected in
        // persist's workload, so punt for now.
        let res = self.blob.get(key).await?;
        if let Some(blob) = res.as_ref() {
            self.cache.insert(key.to_owned(), blob.clone());
            self.update_size_metrics();
        }
        Ok(res)
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.blob.list_keys_and_metadata(key_prefix, f).await
    }

    async fn set(&self, key: &str, value: Bytes, atomic: Atomicity) -> Result<(), ExternalError> {
        let () = self.blob.set(key, value.clone(), atomic).await?;
        self.cache
            .insert(key.to_owned(), SegmentedBytes::from(value));
        self.update_size_metrics();
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let res = self.blob.delete(key).await;
        self.cache.invalidate(key);
        self.update_size_metrics();
        res
    }
}
