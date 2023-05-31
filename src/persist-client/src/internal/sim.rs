// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! WIP

#![allow(dead_code)] // WIP

use std::collections::hash_map::{DefaultHasher, Entry};
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::collections::HashMap;
use mz_persist::location::{Atomicity, Blob, BlobMetadata, ExternalError};

use crate::internal::metrics::Metrics;

#[derive(Debug, Default)]
struct CacheSim<K> {
    keys: HashMap<K, usize>,
    weights: BTreeMap<usize, u64>,
    idx: usize,
}

impl<K: Hash + Eq> CacheSim<K> {
    // WIP respect entries_limit
    // WIP panic on overflows
    pub fn insert(&mut self, key: K, weight: u64) -> Option<u64> {
        let idx = self.idx;
        self.idx += 1;

        match self.keys.entry(key) {
            Entry::Vacant(x) => {
                x.insert(idx);
                self.weights.insert(idx, weight);
                None
            }
            Entry::Occupied(mut x) => {
                let prev_idx = *x.get();
                *x.get_mut() = idx;

                let necessary_size = self.weights.range(prev_idx..).map(|(_, v)| *v).sum::<u64>();
                assert!(self.weights.remove(&prev_idx).is_some());
                assert!(self.weights.insert(idx, weight).is_none());
                Some(necessary_size)
            }
        }
    }
}

/// A [Blob] delegate that simulates the benefits of caching at various sizes.
#[derive(Debug)]
pub struct BlobCacheSim {
    sim: Mutex<CacheSim<u64>>,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
}

impl BlobCacheSim {
    /// Returns a new BlobCacheSim delegating to the given Blob.
    pub fn new(
        blob: Arc<dyn Blob + Send + Sync>,
        metrics: Arc<Metrics>,
    ) -> Arc<dyn Blob + Send + Sync> {
        let blob = BlobCacheSim {
            sim: Default::default(),
            blob,
            metrics,
        };
        Arc::new(blob)
    }

    fn sim_key(key: &str) -> u64 {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        h.finish()
    }
}

#[async_trait]
impl Blob for BlobCacheSim {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let blob = self.blob.get(key).await?;
        if let Some(blob) = blob.as_ref() {
            let sim_key = Self::sim_key(key);
            let necessary_size = self
                .sim
                .lock()
                .expect("lock")
                .insert(sim_key, u64::cast_from(blob.len()));
            if let Some(necessary_size) = necessary_size {
                self.metrics
                    .cache_sim
                    .cached_blobs
                    .observe(f64::cast_lossy(necessary_size));
                self.metrics
                    .cache_sim
                    .observe_bytes(u64::cast_from(blob.len()), necessary_size)
            }
        }
        Ok(blob)
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.blob.list_keys_and_metadata(key_prefix, f).await
    }

    async fn set(&self, key: &str, value: Bytes, atomic: Atomicity) -> Result<(), ExternalError> {
        let value_len = value.len();
        let () = self.blob.set(key, value, atomic).await?;
        let sim_key = Self::sim_key(key);
        let _ = self
            .sim
            .lock()
            .expect("lock")
            .insert(sim_key, u64::cast_from(value_len));
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        // WIP delete key from sim
        self.blob.delete(key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_sim() {
        #[track_caller]
        fn testcase(entries: &[(&str, u64, Option<u64>)]) {
            let mut sim = CacheSim::<&str>::default();
            for (key, weight, expected) in entries {
                assert_eq!(sim.insert(*key, *weight), *expected);
            }
        }

        testcase(&[("a", 1, None), ("a", 1, Some(1)), ("a", 1, Some(1))]);
        testcase(&[
            ("a", 1, None),
            ("b", 2, None),
            ("c", 4, None),
            ("c", 4, Some(4)),
            ("a", 1, Some(7)),
            ("d", 8, None),
            ("b", 1, Some(15)),
        ]);
    }
}
