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
use std::thread;
use std::time::Instant;

use futures_executor::block_on;
use mz_build_info::BuildInfo;
use mz_ore::cast::CastFrom;
use mz_persist_types::Codec;
use semver::Version;
use tokio::runtime::Runtime as AsyncRuntime;

use crate::error::Error;
use crate::gen::persist::{ProtoBatchFormat, ProtoMeta};
use crate::indexed::encoding::{
    BlobMeta, BlobTraceBatchPart, BlobUnsealedBatch, TraceBatchMeta, UnsealedBatchMeta,
};
use crate::indexed::metrics::Metrics;
use crate::location::{Atomicity, Blob, BlobRead};
use crate::pfuture::PFuture;

/// User hints for [BlobCache] operations.
#[derive(Debug)]
pub enum CacheHint {
    /// Attempt to add the result of the given operation to the in-memory cache.
    MaybeAdd,
    /// Never add the result of the given operation to the in-memory cache.
    NeverAdd,
}

/// A disk-backed cache for objects in [Blob] storage.
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
    build_version: Version,
    metrics: Arc<Metrics>,
    blob: Arc<Mutex<B>>,
    async_runtime: Arc<AsyncRuntime>,
    cache: BlobCacheInner,
    prev_meta_len: u64,
}

impl<B> Clone for BlobCache<B> {
    fn clone(&self) -> Self {
        BlobCache {
            build_version: self.build_version.clone(),
            metrics: Arc::clone(&self.metrics),
            blob: Arc::clone(&self.blob),
            async_runtime: Arc::clone(&self.async_runtime),
            cache: self.cache.clone(),
            prev_meta_len: self.prev_meta_len,
        }
    }
}

const MB: usize = 1024 * 1024;
const GB: usize = 1024 * MB;

impl<B: BlobRead> BlobCache<B> {
    const META_KEY: &'static str = "META";
    const DEFAULT_CACHE_SIZE_LIMIT: usize = 2 * GB;

    /// Returns a new, empty cache for the given [Blob] storage.
    pub fn new(
        build: BuildInfo,
        metrics: Arc<Metrics>,
        async_runtime: Arc<AsyncRuntime>,
        blob: B,
        cache_size_limit: Option<usize>,
    ) -> Self {
        BlobCache {
            build_version: build.semver_version(),
            metrics,
            blob: Arc::new(Mutex::new(blob)),
            async_runtime,
            cache: BlobCacheInner::new(cache_size_limit.unwrap_or(Self::DEFAULT_CACHE_SIZE_LIMIT)),
            prev_meta_len: 0,
        }
    }

    /// Synchronously closes the cache, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// This method is idempotent. Returns true if the blob had not
    /// previously been closed.
    pub fn close(&mut self) -> Result<bool, Error> {
        let async_guard = self.async_runtime.enter();
        let ret = block_on(self.blob.lock()?.close());
        drop(async_guard);
        ret
    }

    /// Synchronously fetches the batch for the given key.
    fn fetch_unsealed_batch_sync(
        &self,
        key: &str,
        hint: CacheHint,
    ) -> Result<Arc<BlobUnsealedBatch>, Error> {
        let async_guard = self.async_runtime.enter();

        let bytes = block_on(self.blob.lock()?.get(key))?
            .ok_or_else(|| Error::from(format!("no blob for unsealed batch at key: {}", key)))?;
        let bytes_len = bytes.len();
        self.metrics
            .blob_read_cache_fetch_bytes
            .inc_by(u64::cast_from(bytes_len));
        let batch: BlobUnsealedBatch = BlobUnsealedBatch::decode(&bytes).map_err(|err| {
            Error::from(format!("invalid unsealed batch at key {}: {}", key, err))
        })?;

        debug_assert_eq!(batch.validate(), Ok(()), "{:?}", &batch);
        // NB: Batch blobs are write-once, so we're not worried about the race
        // of two get calls for the same key.
        let ret = Arc::new(batch);
        if let CacheHint::MaybeAdd = hint {
            self.cache
                .maybe_add_unsealed(key.to_owned(), bytes_len, Arc::clone(&ret))?;
        }

        drop(async_guard);
        Ok(ret)
    }

    /// Asynchronously returns the batch for the given key, fetching in another
    /// thread if it's not already in the cache.
    pub fn get_unsealed_batch_async(
        &self,
        key: &str,
        hint: CacheHint,
    ) -> PFuture<Arc<BlobUnsealedBatch>> {
        let (tx, rx) = PFuture::new();
        match self.cache.get_unsealed(key) {
            Err(err) => {
                // TODO: if there's an error reading from cache we could just
                // fetch the batch directly from blob storage.
                tx.fill(Err(err));
                return rx;
            }
            Ok(Some(entry)) => {
                self.metrics.blob_read_cache_hit_count.inc();
                tx.fill(Ok(entry));
                return rx;
            }
            Ok(None) => {
                // If the object doesn't exist in the cache, fallback to fetching
                // it directly from blob storage.
                self.metrics.blob_read_cache_miss_count.inc();
            }
        }

        // TODO: If a fetch for this key is already in progress join that one
        // instead of starting another.
        let cache = self.clone();
        let key = key.to_owned();
        // TODO: IO thread pool for persist instead of spawning one here.
        let _ = thread::spawn(move || {
            let async_guard = cache.async_runtime.enter();
            let res = cache.fetch_unsealed_batch_sync(&key, hint);
            tx.fill(res);
            drop(async_guard);
        });
        rx
    }

    /// Synchronously fetches the batch for the given key.
    fn fetch_trace_batch_sync(
        &self,
        key: &str,
        hint: CacheHint,
    ) -> Result<Arc<BlobTraceBatchPart>, Error> {
        let async_guard = self.async_runtime.enter();

        let bytes = block_on(self.blob.lock()?.get(key))?
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

        drop(async_guard);
        Ok(ret)
    }

    /// Asynchronously returns the batch for the given key, fetching in another
    /// thread if it's not already in the cache.
    pub fn get_trace_batch_async(
        &self,
        key: &str,
        hint: CacheHint,
    ) -> PFuture<Arc<BlobTraceBatchPart>> {
        let (tx, rx) = PFuture::new();
        match self.cache.get_trace(key) {
            Err(err) => {
                // TODO: if there's an error reading from cache we could just
                // fetch the batch directly from blob storage.
                tx.fill(Err(err));
                return rx;
            }
            Ok(Some(entry)) => {
                self.metrics.blob_read_cache_hit_count.inc();
                tx.fill(Ok(entry));
                return rx;
            }
            Ok(None) => {
                // If the batch doesn't exist in the cache, fallback to fetching
                // it directly from blob storage.
                self.metrics.blob_read_cache_miss_count.inc();
            }
        }

        // TODO: If a fetch for this key is already in progress join that one
        // instead of starting another.
        let cache = self.clone();
        let key = key.to_owned();
        // TODO: IO thread pool for persist instead of spawning one here.
        let _ = thread::spawn(move || {
            let async_guard = cache.async_runtime.enter();
            let res = cache.fetch_trace_batch_sync(&key, hint);
            tx.fill(res);
            drop(async_guard);
        });
        rx
    }

    /// Fetches metadata about what batches are in [Blob] storage.
    pub fn get_meta(&self) -> Result<Option<BlobMeta>, Error> {
        let async_guard = self.async_runtime.enter();

        let blob = self.blob.lock()?;
        let bytes = match block_on(blob.get(Self::META_KEY))? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let meta = ProtoMeta::decode(&bytes).map_err(|err| {
            Error::from(format!("invalid meta at key {}: {}", Self::META_KEY, err))
        })?;
        self.check_meta_build_version(&meta)?;
        let meta = BlobMeta::from(meta);
        debug_assert_eq!(meta.validate(), Ok(()), "{:?}", &meta);

        drop(async_guard);
        Ok(Some(meta))
    }

    fn check_meta_build_version(&self, meta: &ProtoMeta) -> Result<(), Error> {
        let meta_version = meta
            .version
            .parse::<Version>()
            .map_err(|err| err.to_string())?;

        // Allow data written by any previous version of persist (backward
        // compatible for all time) but disallow data written by a future
        // version of persist (aka we're currently *not* forward compatible).
        // Note that at some point, mz will need to be forward compatible to
        // allow for rollbacks but this policy is not yet settled.
        //
        // NB: Since ProtoMeta is the entrypoint for all written persist
        // metadata and data, it's an upper bound on versions involved in any
        // persist data.
        if meta_version > self.build_version {
            return Err(format!(
                "persist v{} cannot read data written by future persist v{}",
                self.build_version, meta_version
            )
            .into());
        }
        Ok(())
    }

    /// Returns the list of keys known to the underlying [Blob].
    pub fn list_keys(&self) -> Result<Vec<String>, Error> {
        let async_guard = self.async_runtime.enter();
        let ret = block_on(self.blob.lock()?.list_keys());
        drop(async_guard);
        ret
    }
}

impl<B: Blob> BlobCache<B> {
    /// Writes a batch to backing [Blob] storage.
    ///
    /// Returns the size of the encoded blob value in bytes.
    pub fn set_unsealed_batch(
        &mut self,
        key: String,
        batch: BlobUnsealedBatch,
        format: ProtoBatchFormat,
    ) -> Result<u64, Error> {
        let async_guard = self.async_runtime.enter();

        if key == Self::META_KEY {
            return Err(format!(
                "cannot write unsealed batch to meta key: {}",
                Self::META_KEY
            )
            .into());
        }

        if format != ProtoBatchFormat::ParquetKvtd {
            return Err(format!(
                "cannot write unsealed batch with unsupported format {:?}",
                format
            )
            .into());
        }
        debug_assert_eq!(batch.validate(), Ok(()), "{:?}", &batch);

        let mut val = Vec::new();
        batch.encode(&mut val);
        let val_len = u64::cast_from(val.len());

        let write_start = Instant::now();
        block_on(self.blob.lock()?.set(&key, val, Atomicity::AllowNonAtomic))
            .map_err(|err| self.metric_set_error(err))?;
        self.metrics
            .unsealed
            .blob_write_seconds
            .inc_by(write_start.elapsed().as_secs_f64());
        self.metrics.unsealed.blob_write_count.inc();
        self.metrics.unsealed.blob_write_bytes.inc_by(val_len);

        self.cache
            .maybe_add_unsealed(key, usize::cast_from(val_len), Arc::new(batch))?;

        drop(async_guard);
        Ok(val_len)
    }

    /// Removes a batch from both [Blob] storage and the local cache.
    pub fn delete_unsealed_batch(&mut self, batch: &UnsealedBatchMeta) -> Result<(), Error> {
        let async_guard = self.async_runtime.enter();

        let delete_start = Instant::now();
        self.cache.remove_unsealed(&batch.key)?;
        block_on(self.blob.lock()?.delete(&batch.key))?;
        self.metrics
            .unsealed
            .blob_delete_seconds
            .inc_by(delete_start.elapsed().as_secs_f64());
        self.metrics.unsealed.blob_delete_count.inc();
        self.metrics
            .unsealed
            .blob_delete_bytes
            .inc_by(batch.size_bytes);

        drop(async_guard);
        Ok(())
    }

    /// Writes a batch to backing [Blob] storage.
    ///
    /// Returns the size of the encoded blob value in bytes.
    pub fn set_trace_batch(
        &self,
        key: String,
        batch: BlobTraceBatchPart,
        format: ProtoBatchFormat,
    ) -> Result<u64, Error> {
        let async_guard = self.async_runtime.enter();

        if key == Self::META_KEY {
            return Err(format!("cannot write trace batch to meta key: {}", Self::META_KEY).into());
        }
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
        let val_len = u64::cast_from(val.len());

        let write_start = Instant::now();
        block_on(self.blob.lock()?.set(&key, val, Atomicity::AllowNonAtomic))
            .map_err(|err| self.metric_set_error(err))?;
        self.metrics
            .trace
            .blob_write_seconds
            .inc_by(write_start.elapsed().as_secs_f64());
        self.metrics.trace.blob_write_count.inc();
        self.metrics.trace.blob_write_bytes.inc_by(val_len);

        self.cache
            .maybe_add_trace(key, usize::cast_from(val_len), Arc::new(batch))?;

        drop(async_guard);
        Ok(val_len)
    }

    /// Removes a batch from both [Blob] storage and the local cache.
    pub fn delete_trace_batch(&mut self, batch: &TraceBatchMeta) -> Result<(), Error> {
        let async_guard = self.async_runtime.enter();

        let delete_start = Instant::now();
        for key in batch.keys.iter() {
            self.cache.remove_trace(&key)?;
            block_on(self.blob.lock()?.delete(key))?;
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

        drop(async_guard);
        Ok(())
    }

    /// Overwrites metadata about what batches are in [Blob] storage.
    pub fn set_meta(&mut self, meta: &BlobMeta) -> Result<(), Error> {
        let async_guard = self.async_runtime.enter();

        debug_assert_eq!(meta.validate(), Ok(()), "{:?}", &meta);
        let meta = ProtoMeta::from((meta, &self.build_version));

        let mut val = Vec::new();
        meta.encode(&mut val);
        let val_len = u64::cast_from(val.len());
        self.metrics.meta_size_bytes.set(val_len);

        let write_start = Instant::now();
        block_on(
            self.blob
                .lock()?
                .set(Self::META_KEY, val, Atomicity::RequireAtomic),
        )
        .map_err(|err| self.metric_set_error(err))?;
        self.metrics
            .meta
            .blob_write_seconds
            .inc_by(write_start.elapsed().as_secs_f64());
        self.metrics.meta.blob_write_count.inc();
        self.metrics.meta.blob_write_bytes.inc_by(val_len);

        // Meta overwrites itself. Pretend like that's a delete so the graphs
        // make sense.
        if self.prev_meta_len > 0 {
            self.metrics.meta.blob_delete_count.inc();
            self.metrics
                .meta
                .blob_delete_bytes
                .inc_by(self.prev_meta_len);
        }
        self.prev_meta_len = val_len;

        // Don't bother caching meta, nothing reads it after startup.
        drop(async_guard);
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

/// Internal, in-memory cache for objects in [Blob] storage that back an
/// arrangement.
#[derive(Clone, Debug)]
struct BlobCacheInner {
    // TODO: Use a disk-backed LRU cache.
    unsealed: Arc<Mutex<BlobCacheCore<BlobUnsealedBatch>>>,
    trace: Arc<Mutex<BlobCacheCore<BlobTraceBatchPart>>>,
}

impl BlobCacheInner {
    fn new(limit: usize) -> Self {
        BlobCacheInner {
            unsealed: Arc::new(Mutex::new(BlobCacheCore::new(limit / 2))),
            trace: Arc::new(Mutex::new(BlobCacheCore::new(limit / 2))),
        }
    }

    fn maybe_add_unsealed(
        &self,
        key: String,
        size: usize,
        data: Arc<BlobUnsealedBatch>,
    ) -> Result<(), Error> {
        let mut unsealed = self.unsealed.lock()?;
        unsealed.add(key, size, data);
        Ok(())
    }

    fn get_unsealed(&self, key: &str) -> Result<Option<Arc<BlobUnsealedBatch>>, Error> {
        let unsealed = self.unsealed.lock()?;
        Ok(unsealed.get(key))
    }

    fn remove_unsealed(&self, key: &str) -> Result<(), Error> {
        let mut unsealed = self.unsealed.lock()?;
        unsealed.remove(key);
        Ok(())
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

#[cfg(test)]
mod tests {
    use crate::location::SeqNo;
    use crate::mem::MemRegistry;

    use super::*;

    #[test]
    fn build_version() -> Result<(), Error> {
        let mut cache = BlobCache::new(
            mz_build_info::DUMMY_BUILD_INFO,
            Arc::new(Metrics::default()),
            Arc::new(AsyncRuntime::new()?),
            MemRegistry::new().blob_no_reentrance()?,
            None,
        );

        // Whatever we write down roundtrips.
        cache.build_version = Version::new(1, 0, 0);
        let m = BlobMeta {
            seqno: SeqNo(1),
            ..Default::default()
        };
        cache.set_meta(&m)?;
        assert_eq!(cache.get_meta(), Ok(Some(m.clone())));

        // A later version of persist handles what we wrote down (backward
        // compatible). NB: Remember that the blob currently has v1.0.0 data.
        cache.build_version = Version::new(1, 0, 1);
        assert_eq!(cache.get_meta(), Ok(Some(m)));

        // An earlier version of persist fails, because who knows what important
        // fields might be written down that it doesn't know to parse (*not*
        // forward compatible). Also note that at some point, mz will need to be
        // forward compatible to allow for rollbacks but this policy is not yet
        // settled. NB: Remember that the blob still has v1.0.0 data.
        cache.build_version = Version::new(0, 9, 9);
        assert_eq!(
            cache.get_meta(),
            Err("persist v0.9.9 cannot read data written by future persist v1.0.0".into())
        );

        Ok(())
    }
}
