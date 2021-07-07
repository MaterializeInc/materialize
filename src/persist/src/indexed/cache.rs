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

use abomonation::abomonated::Abomonated;

use crate::error::Error;
use crate::indexed::encoding::{BlobFutureBatch, BlobMeta, BlobTraceBatch};
use crate::storage::Blob;
use crate::Data;

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
pub struct BlobCache<K, V, L: Blob> {
    blob: Arc<Mutex<L>>,
    // TODO: Use a disk-backed LRU cache.
    future: Arc<Mutex<HashMap<String, Arc<BlobFutureBatch<K, V>>>>>,
    trace: Arc<Mutex<HashMap<String, Arc<BlobTraceBatch<K, V>>>>>,
}

impl<K, V, L: Blob> Clone for BlobCache<K, V, L> {
    fn clone(&self) -> Self {
        BlobCache {
            blob: self.blob.clone(),
            future: self.future.clone(),
            trace: self.trace.clone(),
        }
    }
}

impl<K: Data, V: Data, L: Blob> BlobCache<K, V, L> {
    const META_KEY: &'static str = "META";

    /// Returns a new, empty cache for the given [Blob] storage.
    pub fn new(blob: L) -> Self {
        BlobCache {
            blob: Arc::new(Mutex::new(blob)),
            future: Arc::new(Mutex::new(HashMap::new())),
            trace: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Synchronously closes the cache, releasing exclusive-writer locks and
    /// causing all future commands to error.
    ///
    /// This method is idempotent.
    pub fn close(&mut self) -> Result<(), Error> {
        self.blob.lock()?.close()
    }

    /// Returns the batch for the given key, blocking to fetch if it's not
    /// already in the cache.
    pub fn get_future_batch(&self, key: &str) -> Result<Arc<BlobFutureBatch<K, V>>, Error> {
        {
            // New scope to ensure the cache lock is dropped during the
            // (expensive) get.
            if let Some(entry) = self.future.lock()?.get(key) {
                return Ok(entry.clone());
            }
        }

        let bytes = self
            .blob
            .lock()?
            .get(key)?
            .ok_or_else(|| Error::from(format!("no blob for key: {}", key)))?;
        let batch: Abomonated<BlobFutureBatch<K, V>, Vec<u8>> =
            unsafe { Abomonated::new(bytes) }
                .ok_or_else(|| Error::from(format!("invalid batch at key: {}", key)))?;

        // NB: Batch blobs are write-once, so we're not worried about the race
        // of two get calls for the same key.
        let mut cache = self.future.lock()?;
        // TODO: Well this is ugly.
        let batch = (*batch).clone();
        debug_assert_eq!(batch.validate(), Ok(()), "{:?}", &batch);
        cache.insert(key.to_owned(), Arc::new(batch));
        Ok(cache.get(key).unwrap().clone())
    }

    /// Writes a batch to backing [Blob] storage.
    pub fn set_future_batch(
        &mut self,
        key: String,
        batch: BlobFutureBatch<K, V>,
    ) -> Result<(), Error> {
        if key == Self::META_KEY {
            return Err(format!("cannot write trace batch to meta key: {}", Self::META_KEY).into());
        }
        debug_assert_eq!(batch.validate(), Ok(()), "{:?}", &batch);

        let mut val = Vec::new();
        unsafe { abomonation::encode(&batch, &mut val) }.expect("write to Vec is infallible");
        self.blob.lock()?.set(&key, val, false)?;
        self.future.lock()?.insert(key, Arc::new(batch));
        Ok(())
    }

    /// Returns the batch for the given key, blocking to fetch if it's not
    /// already in the cache.
    pub fn get_trace_batch(&self, key: &str) -> Result<Arc<BlobTraceBatch<K, V>>, Error> {
        {
            // New scope to ensure the cache lock is dropped during the
            // (expensive) get.
            if let Some(entry) = self.trace.lock()?.get(key) {
                return Ok(entry.clone());
            }
        }

        let bytes = self
            .blob
            .lock()?
            .get(key)?
            .ok_or_else(|| Error::from(format!("no blob for key: {}", key)))?;
        let batch: Abomonated<BlobTraceBatch<K, V>, Vec<u8>> = unsafe { Abomonated::new(bytes) }
            .ok_or_else(|| Error::from(format!("invalid batch at key: {}", key)))?;

        // NB: Batch blobs are write-once, so we're not worried about the race
        // of two get calls for the same key.
        let mut cache = self.trace.lock()?;
        // TODO: Well this is ugly.
        let batch = (*batch).clone();
        debug_assert_eq!(batch.validate(), Ok(()), "{:?}", &batch);
        cache.insert(key.to_owned(), Arc::new(batch));
        Ok(cache.get(key).unwrap().clone())
    }

    /// Writes a batch to backing [Blob] storage.
    pub fn set_trace_batch(
        &mut self,
        key: String,
        batch: BlobTraceBatch<K, V>,
    ) -> Result<(), Error> {
        if key == Self::META_KEY {
            return Err(format!("cannot write trace batch to meta key: {}", Self::META_KEY).into());
        }
        debug_assert_eq!(batch.validate(), Ok(()), "{:?}", &batch);

        let mut val = Vec::new();
        unsafe { abomonation::encode(&batch, &mut val) }.expect("write to Vec is infallible");
        self.blob.lock()?.set(&key, val, false)?;
        self.trace.lock()?.insert(key, Arc::new(batch));
        Ok(())
    }

    /// Fetches metadata about what batches are in [Blob] storage.
    pub fn get_meta(&self) -> Result<Option<BlobMeta>, Error> {
        let blob = self.blob.lock()?;
        let bytes = match blob.get(Self::META_KEY)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let meta: Abomonated<BlobMeta, Vec<u8>> = unsafe { Abomonated::new(bytes) }
            .ok_or_else(|| Error::from(format!("invalid meta at key: {}", Self::META_KEY)))?;
        let meta = (*meta).clone();
        debug_assert_eq!(meta.validate(), Ok(()), "{:?}", &meta);
        Ok(Some(meta))
    }

    /// Overwrites metadata about what batches are in [Blob] storage.
    pub fn set_meta(&self, meta: BlobMeta) -> Result<(), Error> {
        debug_assert_eq!(meta.validate(), Ok(()), "{:?}", &meta);
        let mut val = Vec::new();
        unsafe { abomonation::encode(&meta, &mut val) }.expect("write to Vec is infallible");
        self.blob.lock()?.set(Self::META_KEY, val, true)
        // Don't bother caching meta, nothing reads it after startup.
    }
}
