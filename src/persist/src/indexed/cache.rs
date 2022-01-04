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

use build_info::BuildInfo;
use futures_executor::block_on;
use ore::cast::CastFrom;
use persist_types::Codec;
use semver::Version;

use crate::error::Error;
use crate::gen::persist::ProtoMeta;
use crate::indexed::encoding::{
    BlobMeta, BlobTraceBatch, BlobUnsealedBatch, TraceBatchMeta, UnsealedBatchMeta,
};
use crate::indexed::metrics::Metrics;
use crate::pfuture::PFuture;
use crate::storage::{Atomicity, Blob};

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
pub struct BlobCache<B: Blob> {
    build_version: Version,
    metrics: Metrics,
    blob: Arc<Mutex<B>>,
    // TODO: Use a disk-backed LRU cache.
    unsealed: Arc<Mutex<HashMap<String, Arc<BlobUnsealedBatch>>>>,
    trace: Arc<Mutex<HashMap<String, Arc<BlobTraceBatch>>>>,
    prev_meta_len: u64,
}

impl<B: Blob> Clone for BlobCache<B> {
    fn clone(&self) -> Self {
        BlobCache {
            build_version: self.build_version.clone(),
            metrics: self.metrics.clone(),
            blob: self.blob.clone(),
            unsealed: self.unsealed.clone(),
            trace: self.trace.clone(),
            prev_meta_len: self.prev_meta_len,
        }
    }
}

impl<B: Blob> BlobCache<B> {
    const META_KEY: &'static str = "META";

    /// Returns a new, empty cache for the given [Blob] storage.
    pub fn new(build: BuildInfo, metrics: Metrics, blob: B) -> Self {
        BlobCache {
            build_version: build.semver_version(),
            metrics,
            blob: Arc::new(Mutex::new(blob)),
            unsealed: Arc::new(Mutex::new(HashMap::new())),
            trace: Arc::new(Mutex::new(HashMap::new())),
            prev_meta_len: 0,
        }
    }

    /// Synchronously closes the cache, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// This method is idempotent. Returns true if the blob had not
    /// previously been closed.
    pub fn close(&mut self) -> Result<bool, Error> {
        block_on(self.blob.lock()?.close())
    }

    /// Synchronously fetches the batch for the given key.
    fn fetch_unsealed_batch_sync(&self, meta: &UnsealedBatchMeta) -> Result<Arc<BlobUnsealedBatch>, Error> {
        let bytes = block_on(self.blob.lock()?.get(&meta.key))?
            .ok_or_else(|| Error::from(format!("no blob for unsealed batch at key: {}", meta.key)))?;
        self.metrics
            .blob_read_cache_fetch_bytes
            .inc_by(u64::cast_from(bytes.len()));
        let batch: BlobUnsealedBatch = BlobUnsealedBatch::decode(&bytes).map_err(|err| {
            Error::from(format!("invalid unsealed batch at key {}: {}", meta.key, err))
        })?;

        // NB: Batch blobs are write-once, so we're not worried about the race
        // of two get calls for the same key.
        let mut cache = self.unsealed.lock()?;
        debug_assert_eq!(batch.validate(meta), Ok(()), "{:?}", &batch);
        cache.insert(meta.key.to_owned(), Arc::new(batch));
        Ok(cache.get(&meta.key).unwrap().clone())
    }

    /// Asynchronously returns the batch for the given key, fetching in another
    /// thread if it's not already in the cache.
    pub fn get_unsealed_batch_async(&self, meta: &UnsealedBatchMeta) -> PFuture<Arc<BlobUnsealedBatch>> {
        let (tx, rx) = PFuture::new();
        let key = &meta.key;
        {
            // New scope to ensure the cache lock is dropped during the
            // (expensive) get.
            let unsealed = match self.unsealed.lock() {
                Ok(unsealed) => unsealed,
                Err(err) => {
                    tx.fill(Err(err.into()));
                    return rx;
                }
            };
            if let Some(entry) = unsealed.get(key) {
                self.metrics.blob_read_cache_hit_count.inc();
                tx.fill(Ok(entry.clone()));
                return rx;
            }
            self.metrics.blob_read_cache_miss_count.inc();
        }

        // TODO: If a fetch for this key is already in progress join that one
        // instead of starting another.
        let cache = self.clone();
        let meta = meta.to_owned();
        // TODO: IO thread pool for persist instead of spawning one here.
        let _ = thread::spawn(move || {
            let res = cache.fetch_unsealed_batch_sync(&meta);
            tx.fill(res);
        });
        rx
    }

    /// Writes a batch to backing [Blob] storage.
    ///
    /// Returns the size of the encoded blob value in bytes.
    pub fn set_unsealed_batch(
        &mut self,
        meta: &UnsealedBatchMeta,
        batch: BlobUnsealedBatch,
    ) -> Result<u64, Error> {
        let key = meta.key.to_owned();
        if key == Self::META_KEY {
            return Err(format!(
                "cannot write unsealed batch to meta key: {}",
                Self::META_KEY
            )
            .into());
        }
        debug_assert_eq!(batch.validate(meta), Ok(()), "{:?}", &batch);

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

        self.unsealed.lock()?.insert(key, Arc::new(batch));
        Ok(val_len)
    }

    /// Removes a batch from both [Blob] storage and the local cache.
    pub fn delete_unsealed_batch(&mut self, batch: &UnsealedBatchMeta) -> Result<(), Error> {
        let delete_start = Instant::now();
        self.unsealed.lock()?.remove(&batch.key);
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

        Ok(())
    }

    /// Synchronously fetches the batch for the given key.
    fn fetch_trace_batch_sync(&self, key: &str) -> Result<Arc<BlobTraceBatch>, Error> {
        let bytes = block_on(self.blob.lock()?.get(key))?
            .ok_or_else(|| Error::from(format!("no blob for trace batch at key: {}", key)))?;
        self.metrics
            .blob_read_cache_fetch_bytes
            .inc_by(u64::cast_from(bytes.len()));
        let batch: BlobTraceBatch = BlobTraceBatch::decode(&bytes)
            .map_err(|err| Error::from(format!("invalid trace batch at key {}: {}", key, err)))?;

        // NB: Batch blobs are write-once, so we're not worried about the race
        // of two get calls for the same key.
        let mut cache = self.trace.lock()?;
        debug_assert_eq!(batch.validate(), Ok(()), "{:?}", &batch);
        cache.insert(key.to_owned(), Arc::new(batch));
        Ok(cache.get(key).unwrap().clone())
    }

    /// Asynchronously returns the batch for the given key, fetching in another
    /// thread if it's not already in the cache.
    pub fn get_trace_batch_async(&self, key: &str) -> PFuture<Arc<BlobTraceBatch>> {
        let (tx, rx) = PFuture::new();
        {
            // New scope to ensure the cache lock is dropped during the
            // (expensive) get.
            let trace = match self.trace.lock() {
                Ok(trace) => trace,
                Err(err) => {
                    tx.fill(Err(err.into()));
                    return rx;
                }
            };
            if let Some(entry) = trace.get(key) {
                self.metrics.blob_read_cache_hit_count.inc();
                tx.fill(Ok(entry.clone()));
                return rx;
            }
            self.metrics.blob_read_cache_miss_count.inc();
        }

        // TODO: If a fetch for this key is already in progress join that one
        // instead of starting another.
        let cache = self.clone();
        let key = key.to_owned();
        // TODO: IO thread pool for persist instead of spawning one here.
        let _ = thread::spawn(move || {
            let res = cache.fetch_trace_batch_sync(&key);
            tx.fill(res);
        });
        rx
    }

    /// Writes a batch to backing [Blob] storage.
    ///
    /// Returns the size of the encoded blob value in bytes.
    pub fn set_trace_batch(&self, key: String, batch: BlobTraceBatch) -> Result<u64, Error> {
        if key == Self::META_KEY {
            return Err(format!("cannot write trace batch to meta key: {}", Self::META_KEY).into());
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

        self.trace.lock()?.insert(key, Arc::new(batch));
        Ok(val_len)
    }

    /// Removes a batch from both [Blob] storage and the local cache.
    pub fn delete_trace_batch(&mut self, batch: &TraceBatchMeta) -> Result<(), Error> {
        let delete_start = Instant::now();
        self.trace.lock()?.remove(&batch.key);
        block_on(self.blob.lock()?.delete(&batch.key))?;
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

    /// Fetches metadata about what batches are in [Blob] storage.
    pub fn get_meta(&self) -> Result<Option<BlobMeta>, Error> {
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
        Ok(Some(meta))
    }

    /// Overwrites metadata about what batches are in [Blob] storage.
    pub fn set_meta(&mut self, meta: &BlobMeta) -> Result<(), Error> {
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
        Ok(())
    }

    fn check_meta_build_version(&self, meta: &ProtoMeta) -> Result<(), Error> {
        // TODO: After ENCODING_VERSION is bumped to 8 or higher, this can be
        // removed.
        let meta_version = if meta.version.is_empty() {
            // Any build that includes this check comes after a ProtoMeta that
            // was written with no version set.
            Version::new(0, 0, 0)
        } else {
            meta.version
                .parse::<Version>()
                .map_err(|err| err.to_string())?
        };
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

    fn metric_set_error(&self, err: Error) -> Error {
        match &err {
            &Error::OutOfQuota(_) => self.metrics.blob_write_error_quota_count.inc(),
            _ => self.metrics.blob_write_error_other_count.inc(),
        };
        err
    }

    /// Returns the list of keys known to the underlying [Blob].
    pub fn list_keys(&self) -> Result<Vec<String>, Error> {
        block_on(self.blob.lock()?.list_keys())
    }
}

#[cfg(test)]
mod tests {
    use crate::mem::MemRegistry;
    use crate::storage::SeqNo;

    use super::*;

    #[test]
    fn build_version() -> Result<(), Error> {
        let mut cache = BlobCache::new(
            build_info::DUMMY_BUILD_INFO,
            Metrics::default(),
            MemRegistry::new().blob_no_reentrance()?,
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

        // Holdover until ENCODING_VERSION gets bumped to 8. We have to handle a
        // ProtoMeta that was written with no version.
        assert_eq!(
            cache.check_meta_build_version(&ProtoMeta::default()),
            Ok(())
        );

        Ok(())
    }
}
