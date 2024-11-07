// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-process caches of [Blob].

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_persist::location::{Blob, BlobMetadata, ExternalError};

use crate::cfg::PersistConfig;
use crate::internal::metrics::Metrics;

// In-memory cache for [Blob].
#[derive(Debug)]
pub struct BlobMemCache {
    cfg: Arc<ConfigSet>,
    metrics: Arc<Metrics>,
    cache: Mutex<lru::Lru<String, SegmentedBytes>>,
    blob: Arc<dyn Blob>,
}

pub(crate) const BLOB_CACHE_MEM_LIMIT_BYTES: Config<usize> = Config::new(
    "persist_blob_cache_mem_limit_bytes",
    128 * 1024 * 1024,
    "Capacity of in-mem blob cache in bytes (Materialize).",
);

impl BlobMemCache {
    pub fn new(cfg: &PersistConfig, metrics: Arc<Metrics>, blob: Arc<dyn Blob>) -> Arc<dyn Blob> {
        let eviction_metrics = Arc::clone(&metrics);
        let cache = lru::Lru::new(BLOB_CACHE_MEM_LIMIT_BYTES.get(cfg), move |_, _, _| {
            eviction_metrics.blob_cache_mem.evictions.inc()
        });
        let blob = BlobMemCache {
            cfg: Arc::clone(&cfg.configs),
            metrics,
            cache: Mutex::new(cache),
            blob,
        };
        Arc::new(blob)
    }

    fn resize_and_update_size_metrics(&self, cache: &mut lru::Lru<String, SegmentedBytes>) {
        cache.update_capacity(BLOB_CACHE_MEM_LIMIT_BYTES.get(&self.cfg));
        self.metrics
            .blob_cache_mem
            .size_blobs
            .set(u64::cast_from(cache.entry_count()));
        self.metrics
            .blob_cache_mem
            .size_bytes
            .set(u64::cast_from(cache.entry_weight()));
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
        // any value in S3 is guaranteed to match (if not, then there's a
        // horrible bug somewhere else).
        if let Some((_, cached_value)) = self.cache.lock().expect("lock poisoned").get(key) {
            self.metrics.blob_cache_mem.hits_blobs.inc();
            self.metrics
                .blob_cache_mem
                .hits_bytes
                .inc_by(u64::cast_from(cached_value.len()));
            return Ok(Some(cached_value.clone()));
        }

        let res = self.blob.get(key).await?;
        if let Some(blob) = res.as_ref() {
            // TODO: It would likely be useful to allow a caller to opt out of
            // adding the data to the cache (e.g. compaction inputs, perhaps
            // some read handles).
            let mut cache = self.cache.lock().expect("lock poisoned");
            // If the weight of this single blob is greater than the capacity of
            // the cache, it will push out everything in the cache and then
            // immediately get evicted itself. So, skip adding it in that case.
            if blob.len() <= cache.capacity() {
                cache.insert(key.to_owned(), blob.clone(), blob.len());
                self.resize_and_update_size_metrics(&mut cache);
            }
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

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let () = self.blob.set(key, value.clone()).await?;
        let weight = value.len();
        let mut cache = self.cache.lock().expect("lock poisoned");
        // If the weight of this single blob is greater than the capacity of
        // the cache, it will push out everything in the cache and then
        // immediately get evicted itself. So, skip adding it in that case.
        if weight <= cache.capacity() {
            cache.insert(key.to_owned(), SegmentedBytes::from(value), weight);
            self.resize_and_update_size_metrics(&mut cache);
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let res = self.blob.delete(key).await;
        let mut cache = self.cache.lock().expect("lock poisoned");
        cache.remove(key);
        self.resize_and_update_size_metrics(&mut cache);
        res
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        self.blob.restore(key).await
    }
}

mod lru {
    use std::borrow::Borrow;
    use std::collections::BTreeMap;
    use std::hash::Hash;

    use mz_ore::collections::HashMap;

    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Weight(usize);

    #[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct Time(usize);

    /// A weighted cache, evicting the least recently used entries.
    ///
    /// This is reimplemented here, instead of using an existing crate, because
    /// existing options seem to either not support weights or they use unsafe.
    pub struct Lru<K, V> {
        evict_fn: Box<dyn Fn(K, V, usize) + Send>,
        capacity: Weight,

        next_time: Time,
        entries: HashMap<K, (V, Weight, Time)>,
        by_time: BTreeMap<Time, K>,
        total_weight: Weight,
    }

    impl<K: Hash + Eq + Clone, V> Lru<K, V> {
        /// Returns a new [Lru] with the requested configuration.
        ///
        /// `evict_fn` is called for every entry evicted by the least recently
        /// used policy. It is not called for entries replaced by the same key
        /// in `insert` nor entries explictly removed by `remove`.
        pub fn new<F>(capacity: usize, evict_fn: F) -> Self
        where
            F: Fn(K, V, usize) + Send + 'static,
        {
            Lru {
                evict_fn: Box::new(evict_fn),
                capacity: Weight(capacity),
                next_time: Time::default(),
                entries: HashMap::new(),
                by_time: BTreeMap::new(),
                total_weight: Weight(0),
            }
        }

        /// Returns the capacity of the cache.
        pub fn capacity(&self) -> usize {
            self.capacity.0
        }

        /// Returns the total number of entries in the cache.
        pub fn entry_count(&self) -> usize {
            debug_assert_eq!(self.entries.len(), self.by_time.len());
            self.entries.len()
        }

        /// Returns the sum of weights of entries in the cache.
        pub fn entry_weight(&self) -> usize {
            self.total_weight.0
        }

        /// Changes the weighted capacity of the cache, evicting as necessary if
        /// the new value is smaller.
        pub fn update_capacity(&mut self, capacity: usize) {
            self.capacity = Weight(capacity);
            self.resize();
            assert!(self.total_weight <= self.capacity);

            // Intentionally not run with debug_assert, because validate is
            // `O(n)` in the size of the cache.
            #[cfg(test)]
            self.validate();
        }

        /// Returns a reference to entry with the given key, if present, marking
        /// it as most recently used.
        pub fn get<Q>(&mut self, key: &Q) -> Option<(&K, &V)>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            {
                let (key, val, weight) = self.remove(key)?;
                self.insert_not_exists(key, val, Weight(weight));
            }
            let (key, (val, _, _)) = self
                .entries
                .get_key_value(key)
                .expect("internal lru invariant violated");

            // Intentionally not run with debug_assert, because validate is
            // `O(n)` in the size of the cache.
            #[cfg(test)]
            self.validate();

            Some((key, val))
        }

        /// Inserts the given key and value into the cache, marking it as most
        /// recently used.
        ///
        /// If the key already exists in the cache, the existing value and
        /// weight are first removed.
        pub fn insert(&mut self, key: K, val: V, weight: usize) {
            let _ = self.remove(&key);
            self.insert_not_exists(key, val, Weight(weight));

            // Intentionally not run with debug_assert, because validate is
            // `O(n)` in the size of the cache.
            #[cfg(test)]
            self.validate();
        }

        /// Removes the entry with the given key from the cache, if present.
        ///
        /// Returns None if the entry was not in the cache.
        pub fn remove<Q>(&mut self, k: &Q) -> Option<(K, V, usize)>
        where
            K: Borrow<Q>,
            Q: Hash + Eq + ?Sized,
        {
            let (_, _, time) = self.entries.get(k)?;
            let (key, val, weight) = self.remove_exists(time.clone());

            // Intentionally not run with debug_assert, because validate is
            // `O(n)` in the size of the cache.
            #[cfg(test)]
            self.validate();

            Some((key, val, weight.0))
        }

        /// Returns an iterator over the entries in the cache in order from most
        /// recently used to least.
        #[allow(dead_code)]
        pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V, usize)> {
            self.by_time.iter().rev().map(|(_, key)| {
                let (val, _, weight) = self
                    .entries
                    .get(key)
                    .expect("internal lru invariant violated");
                (key, val, weight.0)
            })
        }

        fn insert_not_exists(&mut self, key: K, val: V, weight: Weight) {
            let time = self.next_time.clone();
            self.next_time.0 += 1;

            self.total_weight.0 = self
                .total_weight
                .0
                .checked_add(weight.0)
                .expect("weight overflow");
            assert!(self
                .entries
                .insert(key.clone(), (val, weight, time.clone()))
                .is_none());
            assert!(self.by_time.insert(time, key).is_none());
            self.resize();
        }

        fn remove_exists(&mut self, time: Time) -> (K, V, Weight) {
            let key = self
                .by_time
                .remove(&time)
                .expect("internal list invariant violated");
            let (val, weight, _time) = self
                .entries
                .remove(&key)
                .expect("internal list invariant violated");
            self.total_weight.0 = self
                .total_weight
                .0
                .checked_sub(weight.0)
                .expect("internal lru invariant violated");

            (key, val, weight)
        }

        fn resize(&mut self) {
            while self.total_weight > self.capacity {
                let (time, _) = self
                    .by_time
                    .first_key_value()
                    .expect("internal lru invariant violated");
                let (key, val, weight) = self.remove_exists(time.clone());
                (self.evict_fn)(key, val, weight.0);
            }
        }

        /// Checks internal invariants.
        ///
        /// TODO: Give this persist's usual `-> Result<(), String>` signature
        /// instead of panic-ing.
        #[cfg(test)]
        pub(crate) fn validate(&self) {
            assert!(self.total_weight <= self.capacity);

            let mut count = 0;
            let mut weight = 0;
            for (time, key) in self.by_time.iter() {
                let (_val, w, t) = self
                    .entries
                    .get(key)
                    .expect("internal lru invariant violated");
                count += 1;
                weight += w.0;
                assert_eq!(time, t);
            }
            assert_eq!(count, self.by_time.len());
            assert_eq!(weight, self.total_weight.0);
        }
    }

    impl<K: std::fmt::Debug, V> std::fmt::Debug for Lru<K, V> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            let Lru {
                evict_fn: _,
                capacity,
                next_time,
                entries: _,
                by_time,
                total_weight,
            } = self;
            f.debug_struct("Lru")
                .field("capacity", &capacity)
                .field("total_weight", &total_weight)
                .field("next_time", &next_time)
                .field("by_time", &by_time)
                .finish_non_exhaustive()
        }
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_none;
    use proptest::arbitrary::any;
    use proptest::proptest;
    use proptest_derive::Arbitrary;

    use super::lru::*;

    #[derive(Debug, Arbitrary)]
    enum LruOp {
        Get { key: u8 },
        Insert { key: u8, weight: u8 },
        Remove { key: u8 },
    }

    fn prop_testcase(ops: Vec<LruOp>) {
        // In the long run, we'd expect maybe 1/2s of the `u8::MAX` possible
        // keys to be present and for the average weight to be `u8::MAX / 2`.
        // Select a capacity that is somewhat less than that.
        let capacity = usize::from(u8::MAX / 2) * usize::from(u8::MAX / 2) / 2;
        let mut cache = Lru::new(capacity, |_, _, _| {});
        for op in ops {
            match op {
                LruOp::Get { key } => {
                    let _ = cache.get(&key);
                }
                LruOp::Insert { key, weight } => {
                    cache.insert(key, (), usize::from(weight));
                }
                LruOp::Remove { key } => {
                    let _ = cache.remove(&key);
                }
            }
            cache.validate();
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn lru_cache_prop() {
        proptest!(|(state in proptest::collection::vec(any::<LruOp>(), 0..100))| prop_testcase(state));
    }

    impl Lru<&'static str, ()> {
        fn keys(&self) -> Vec<&'static str> {
            self.iter().map(|(k, _, _)| *k).collect()
        }
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn lru_cache_usage() {
        let mut cache = Lru::<&'static str, ()>::new(3, |_, _, _| {});

        // Empty
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(cache.entry_weight(), 0);

        // Insert into empty.
        cache.insert("a", (), 2);
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(cache.entry_weight(), 2);
        assert_eq!(cache.keys(), &["a"]);

        // Insert and push out previous.
        cache.insert("b", (), 2);
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(cache.entry_weight(), 2);
        assert_eq!(cache.keys(), &["b"]);

        // Insert and don't push out previous.
        cache.insert("c", (), 1);
        assert_eq!(cache.entry_count(), 2);
        assert_eq!(cache.entry_weight(), 3);
        assert_eq!(cache.keys(), &["c", "b"]);

        // More than two elements.
        cache.insert("d", (), 1);
        cache.insert("e", (), 1);
        assert_eq!(cache.entry_count(), 3);
        assert_eq!(cache.entry_weight(), 3);
        assert_eq!(cache.keys(), &["e", "d", "c"]);

        // Get the head.
        cache.get("e");
        assert_eq!(cache.entry_count(), 3);
        assert_eq!(cache.entry_weight(), 3);
        assert_eq!(cache.keys(), &["e", "d", "c"]);

        // Get the tail.
        cache.get("c");
        assert_eq!(cache.entry_count(), 3);
        assert_eq!(cache.entry_weight(), 3);
        assert_eq!(cache.keys(), &["c", "e", "d"]);

        // Get the mid.
        cache.get("e");
        assert_eq!(cache.entry_count(), 3);
        assert_eq!(cache.entry_weight(), 3);
        assert_eq!(cache.keys(), &["e", "c", "d"]);

        // Get a non-existent element.
        cache.get("f");
        assert_eq!(cache.entry_count(), 3);
        assert_eq!(cache.entry_weight(), 3);
        assert_eq!(cache.keys(), &["e", "c", "d"]);

        // Remove an element.
        assert!(cache.remove("c").is_some());
        assert_eq!(cache.entry_count(), 2);
        assert_eq!(cache.entry_weight(), 2);
        assert_eq!(cache.keys(), &["e", "d"]);

        // Remove a non-existent element.
        assert_none!(cache.remove("f"));
        assert_eq!(cache.entry_count(), 2);
        assert_eq!(cache.entry_weight(), 2);
        assert_eq!(cache.keys(), &["e", "d"]);

        // Push out everything with a big weight
        cache.insert("f", (), 3);
        assert_eq!(cache.entry_count(), 1);
        assert_eq!(cache.entry_weight(), 3);
        assert_eq!(cache.keys(), &["f"]);

        // Push out everything with a weight so big it doesn't even fit. (Is
        // this even the behavior we want?)
        cache.insert("g", (), 4);
        assert_eq!(cache.entry_count(), 0);
        assert_eq!(cache.entry_weight(), 0);

        // Resize up
        cache.insert("h", (), 2);
        cache.insert("i", (), 1);
        cache.update_capacity(4);
        cache.insert("j", (), 1);
        assert_eq!(cache.entry_count(), 3);
        assert_eq!(cache.entry_weight(), 4);
        assert_eq!(cache.keys(), &["j", "i", "h"]);

        // Resize down
        cache.update_capacity(2);
        assert_eq!(cache.entry_count(), 2);
        assert_eq!(cache.entry_weight(), 2);
        assert_eq!(cache.keys(), &["j", "i"]);
    }
}
