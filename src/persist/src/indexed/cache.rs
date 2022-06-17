// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A disk-backed cache for objects in blob storage.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use mz_ore::cast::CastFrom;
use mz_persist_types::Codec;

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::encoding::{BlobTraceBatchPart, TraceBatchMeta};
use crate::indexed::metrics::Metrics;
use crate::location::{Atomicity, BlobMulti};

/// User hints for [BlobCache] operations.
#[derive(Debug)]
pub enum CacheHint {
    /// Attempt to add the result of the given operation to the in-memory cache.
    MaybeAdd,
    /// Never add the result of the given operation to the in-memory cache.
    NeverAdd,
}

/// A disk-backed cache for objects in [BlobMulti] storage.
///
/// The data for the objects in the cache is stored on disk, mmap'd, and a
/// validated handle is stored in-memory to avoid repeatedly decoding it.
///
/// TODO: Add a limit to bound how much disk this cache can use. The `Arc`
/// return type for `get_batch` seems correct, but means that a bad user could
/// starve the cache by indefinitely holding handles. The Arcs could be made
/// into weak references so the cache could forcefully reclaim the backing data,
/// but this is going to make performance of using the cached batches
/// unpredictable. I think we probably want a soft limit and a hard limit where
/// the soft limit does some alerting and the hard limit starts blocking (or
/// erroring) until disk space frees up.
#[derive(Debug)]
pub struct BlobCache<B> {
    metrics: Arc<Metrics>,
    blob: Arc<B>,
    cache: BlobCacheInner,
}

impl<B> Clone for BlobCache<B> {
    fn clone(&self) -> Self {
        BlobCache {
            metrics: Arc::clone(&self.metrics),
            blob: Arc::clone(&self.blob),
            cache: self.cache.clone(),
        }
    }
}

const MB: usize = 1024 * 1024;
const GB: usize = 1024 * MB;

impl<B: BlobMulti + Send + Sync + 'static> BlobCache<B> {
    const DEFAULT_CACHE_SIZE_LIMIT: usize = 2 * GB;

    /// Returns a new, empty cache for the given [BlobMulti] storage.
    pub fn new(metrics: Arc<Metrics>, blob: B, cache_size_limit: Option<usize>) -> Self {
        BlobCache {
            metrics,
            blob: Arc::new(blob),
            cache: BlobCacheInner::new(cache_size_limit.unwrap_or(Self::DEFAULT_CACHE_SIZE_LIMIT)),
        }
    }

    /// Synchronously fetches the batch for the given key.
    async fn fetch_trace_batch(
        &self,
        key: &str,
        hint: CacheHint,
    ) -> Result<Arc<BlobTraceBatchPart>, Error> {
        let deadline = Instant::now() + Duration::from_secs(1_000_000_000);
        let bytes = self
            .blob
            .get(deadline, key)
            .await?
            .ok_or_else(|| Error::from(format!("no blob for trace batch at key: {}", key)))?;
        let bytes_len = bytes.len();
        self.metrics
            .blob_read_cache_fetch_bytes
            .inc_by(u64::cast_from(bytes_len));
        let batch: BlobTraceBatchPart = BlobTraceBatchPart::decode(&bytes)
            .map_err(|err| Error::from(format!("invalid trace batch at key {}: {}", key, err)))?;

        debug_assert_eq!(batch.validate(), Ok(()), "{:?}", &batch);
        // NB: Batch blobs are write-once, so we're not worried about the race
        // of two get calls for the same key.
        let ret = Arc::new(batch);
        if let CacheHint::MaybeAdd = hint {
            self.cache
                .maybe_add_trace(key.to_owned(), bytes_len, Arc::clone(&ret))?;
        }
        Ok(ret)
    }

    /// Asynchronously returns the batch for the given key, fetching in another
    /// thread if it's not already in the cache.
    pub async fn get_trace_batch_async(
        &self,
        key: &str,
        hint: CacheHint,
    ) -> Result<Arc<BlobTraceBatchPart>, Error> {
        match self.cache.get_trace(key) {
            Err(err) => {
                // TODO: if there's an error reading from cache we could just
                // fetch the batch directly from blob storage.
                return Err(err);
            }
            Ok(Some(entry)) => {
                self.metrics.blob_read_cache_hit_count.inc();
                return Ok(entry);
            }
            Ok(None) => {
                // If the batch doesn't exist in the cache, fallback to fetching
                // it directly from blob storage.
                self.metrics.blob_read_cache_miss_count.inc();
            }
        }

        // TODO: If a fetch for this key is already in progress join that one
        // instead of starting another.
        self.fetch_trace_batch(&key, hint).await
    }
}

impl<B: BlobMulti + Send + Sync + 'static> BlobCache<B> {
    /// Writes a batch to backing [BlobMulti] storage.
    ///
    /// Returns the size of the encoded blob value in bytes.
    pub async fn set_trace_batch(
        &self,
        key: String,
        batch: BlobTraceBatchPart,
        format: ProtoBatchFormat,
    ) -> Result<u64, Error> {
        if format != ProtoBatchFormat::ParquetKvtd {
            return Err(format!(
                "cannot write trace batch with unsupported format {:?}",
                format
            )
            .into());
        }
        debug_assert_eq!(batch.validate(), Ok(()), "{:?}", &batch);

        let mut val = Vec::new();
        batch.encode(&mut val);
        let val = Bytes::from(val);
        let val_len = u64::cast_from(val.len());

        let write_start = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(1_000_000_000);
        self.blob
            .set(deadline, &key, val, Atomicity::AllowNonAtomic)
            .await
            .map_err(|err| self.metric_set_error(err.into()))?;
        self.metrics
            .trace
            .blob_write_seconds
            .inc_by(write_start.elapsed().as_secs_f64());
        self.metrics.trace.blob_write_count.inc();
        self.metrics.trace.blob_write_bytes.inc_by(val_len);

        self.cache
            .maybe_add_trace(key, usize::cast_from(val_len), Arc::new(batch))?;
        Ok(val_len)
    }

    /// Removes a batch from both [BlobMulti] storage and the local cache.
    pub async fn delete_trace_batch(&mut self, batch: &TraceBatchMeta) -> Result<(), Error> {
        let delete_start = Instant::now();
        for key in batch.keys.iter() {
            self.cache.remove_trace(&key)?;
            let deadline = Instant::now() + Duration::from_secs(1_000_000_000);
            self.blob.delete(deadline, key).await?;
        }
        self.metrics
            .trace
            .blob_delete_seconds
            .inc_by(delete_start.elapsed().as_secs_f64());
        self.metrics.trace.blob_delete_count.inc();
        self.metrics
            .trace
            .blob_delete_bytes
            .inc_by(batch.size_bytes);
        Ok(())
    }

    fn metric_set_error(&self, err: Error) -> Error {
        match &err {
            &Error::OutOfQuota(_) => self.metrics.blob_write_error_quota_count.inc(),
            _ => self.metrics.blob_write_error_other_count.inc(),
        };
        err
    }
}

/// Internal, in-memory cache for objects in [BlobMulti] storage that back an
/// arrangement.
#[derive(Clone, Debug)]
struct BlobCacheInner {
    // TODO: Use a disk-backed LRU cache.
    trace: Arc<Mutex<BlobCacheCore<BlobTraceBatchPart>>>,
}

impl BlobCacheInner {
    fn new(limit: usize) -> Self {
        BlobCacheInner {
            trace: Arc::new(Mutex::new(BlobCacheCore::new(limit / 2))),
        }
    }

    fn maybe_add_trace(
        &self,
        key: String,
        size: usize,
        data: Arc<BlobTraceBatchPart>,
    ) -> Result<(), Error> {
        let mut trace = self.trace.lock()?;
        trace.add(key, size, data);
        Ok(())
    }

    fn get_trace(&self, key: &str) -> Result<Option<Arc<BlobTraceBatchPart>>, Error> {
        let trace = self.trace.lock()?;
        Ok(trace.get(key))
    }

    fn remove_trace(&self, key: &str) -> Result<(), Error> {
        let mut trace = self.trace.lock()?;
        trace.remove(key);
        Ok(())
    }
}

/// In-memory cache for arbitrary objects that can be shared across multiple
/// threads.
///
/// TODO: this cache accounts for the serialized sizes of data it contains, but
/// perhaps should look at the in-memory size instead.
#[derive(Debug)]
struct BlobCacheCore<D> {
    // Map from key -> (data, size of data)
    dataz: HashMap<String, (Arc<D>, usize)>,
    size: usize,
    limit: usize,
}

impl<D> BlobCacheCore<D> {
    fn new(limit: usize) -> Self {
        BlobCacheCore {
            dataz: HashMap::new(),
            size: 0,
            limit,
        }
    }

    /// Add an object to the cache, if doing so would not exceed the cache's
    /// size limit.
    fn add(&mut self, key: String, size: usize, data: Arc<D>) {
        let new_size = self.size + size;
        if new_size > self.limit {
            return;
        }

        let prev = self.dataz.insert(key, (data, size));
        if prev.is_none() {
            self.size = new_size;
        }

        debug_assert!(self.limit >= self.size);
    }

    fn remove(&mut self, key: &str) {
        let prev = self.dataz.remove(key);
        if let Some((_, size)) = prev {
            debug_assert!(self.size >= size);
            self.size -= size;
        }

        debug_assert!(self.limit >= self.size);
    }

    fn get(&self, key: &str) -> Option<Arc<D>> {
        self.dataz.get(key).map(|(data, _)| Arc::clone(&data))
    }
}
