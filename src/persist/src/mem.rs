// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-memory implementations for testing and benchmarking.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, stream};
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::future::yield_now;

use crate::error::Error;
use crate::location::{
    Blob, BlobMetadata, CaSResult, Consensus, Determinate, ExternalError, ResultStream, SeqNo,
    VersionedData,
};

/// An in-memory representation of a set of [Log]s and [Blob]s that can be reused
/// across dataflows
#[cfg(test)]
#[derive(Debug)]
pub struct MemMultiRegistry {
    blob_by_path: BTreeMap<String, Arc<tokio::sync::Mutex<MemBlobCore>>>,
    tombstone: bool,
}

#[cfg(test)]
impl MemMultiRegistry {
    /// Constructs a new, empty [MemMultiRegistry].
    pub fn new(tombstone: bool) -> Self {
        MemMultiRegistry {
            blob_by_path: BTreeMap::new(),
            tombstone,
        }
    }

    /// Opens a [MemBlob] associated with `path`.
    ///
    /// TODO: Replace this with PersistClientCache once they're in the same
    /// crate.
    pub fn blob(&mut self, path: &str) -> MemBlob {
        if let Some(blob) = self.blob_by_path.get(path) {
            MemBlob::open(MemBlobConfig {
                core: Arc::clone(blob),
            })
        } else {
            let blob = Arc::new(tokio::sync::Mutex::new(MemBlobCore {
                dataz: Default::default(),
                tombstone: self.tombstone,
            }));
            self.blob_by_path
                .insert(path.to_string(), Arc::clone(&blob));
            MemBlob::open(MemBlobConfig { core: blob })
        }
    }
}

#[derive(Debug, Default)]
struct MemBlobCore {
    dataz: BTreeMap<String, (Bytes, bool)>,
    tombstone: bool,
}

impl MemBlobCore {
    fn get(&self, key: &str) -> Result<Option<Bytes>, ExternalError> {
        Ok(self
            .dataz
            .get(key)
            .and_then(|(x, exists)| exists.then(|| Bytes::clone(x))))
    }

    fn set(&mut self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        self.dataz.insert(key.to_owned(), (value, true));
        Ok(())
    }

    fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        for (key, (value, exists)) in &self.dataz {
            if !*exists || !key.starts_with(key_prefix) {
                continue;
            }

            f(BlobMetadata {
                key,
                size_in_bytes: u64::cast_from(value.len()),
            });
        }

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<Option<usize>, ExternalError> {
        let bytes = if self.tombstone {
            self.dataz.get_mut(key).and_then(|(x, exists)| {
                let deleted_size = exists.then(|| x.len());
                *exists = false;
                deleted_size
            })
        } else {
            self.dataz.remove(key).map(|(x, _)| x.len())
        };
        Ok(bytes)
    }

    fn restore(&mut self, key: &str) -> Result<(), ExternalError> {
        match self.dataz.get_mut(key) {
            None => Err(
                Determinate::new(anyhow!("unable to restore {key} from in-memory state")).into(),
            ),
            Some((_, exists)) => {
                *exists = true;
                Ok(())
            }
        }
    }
}

/// Configuration for opening a [MemBlob].
#[derive(Debug, Default)]
pub struct MemBlobConfig {
    core: Arc<tokio::sync::Mutex<MemBlobCore>>,
}

impl MemBlobConfig {
    /// Create a new instance.
    pub fn new(tombstone: bool) -> Self {
        Self {
            core: Arc::new(tokio::sync::Mutex::new(MemBlobCore {
                dataz: Default::default(),
                tombstone,
            })),
        }
    }
}

/// An in-memory implementation of [Blob].
#[derive(Clone, Debug)]
pub struct MemBlob {
    core: Arc<tokio::sync::Mutex<MemBlobCore>>,
}

impl MemBlob {
    /// Opens the given location for non-exclusive read-write access.
    pub fn open(config: MemBlobConfig) -> Self {
        MemBlob { core: config.core }
    }
}

#[async_trait]
impl Blob for MemBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        // Yield to maximize our chances for getting interesting orderings.
        let () = yield_now().await;
        let maybe_bytes = self.core.lock().await.get(key)?;
        Ok(maybe_bytes.map(SegmentedBytes::from))
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        // Yield to maximize our chances for getting interesting orderings.
        let () = yield_now().await;
        self.core.lock().await.list_keys_and_metadata(key_prefix, f)
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        // Yield to maximize our chances for getting interesting orderings.
        let () = yield_now().await;
        // NB: This is always atomic, so we're free to ignore the atomic param.
        self.core.lock().await.set(key, value)
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        // Yield to maximize our chances for getting interesting orderings.
        let () = yield_now().await;
        self.core.lock().await.delete(key)
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        // Yield to maximize our chances for getting interesting orderings.
        let () = yield_now().await;
        self.core.lock().await.restore(key)
    }
}

/// An in-memory implementation of [Consensus].
#[derive(Clone, Debug)]
pub struct MemConsensus {
    // TODO: This was intended to be a tokio::sync::Mutex but that seems to
    // regularly deadlock in the `concurrency` test.
    data: Arc<Mutex<BTreeMap<String, Vec<VersionedData>>>>,
}

impl Default for MemConsensus {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }
}

impl MemConsensus {
    fn scan_store(
        store: &BTreeMap<String, Vec<VersionedData>>,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let results = if let Some(values) = store.get(key) {
            let from_idx = values.partition_point(|x| x.seqno < from);
            let from_values = &values[from_idx..];
            let from_values = &from_values[..usize::min(limit, from_values.len())];
            from_values.to_vec()
        } else {
            Vec::new()
        };
        Ok(results)
    }
}

#[async_trait]
impl Consensus for MemConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        // Yield to maximize our chances for getting interesting orderings.
        let store = self.data.lock().expect("lock poisoned");
        let keys: Vec<_> = store.keys().cloned().collect();
        Box::pin(stream::iter(keys).map(Ok))
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        // Yield to maximize our chances for getting interesting orderings.
        let () = yield_now().await;
        let store = self.data.lock().map_err(Error::from)?;
        let values = match store.get(key) {
            None => return Ok(None),
            Some(values) => values,
        };

        Ok(values.last().cloned())
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        // Yield to maximize our chances for getting interesting orderings.
        let () = yield_now().await;
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(ExternalError::from(anyhow!(
                    "new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                    new.seqno,
                    expected
                )));
            }
        }

        if new.seqno.0 > i64::MAX.try_into().expect("i64::MAX known to fit in u64") {
            return Err(ExternalError::from(anyhow!(
                "sequence numbers must fit within [0, i64::MAX], received: {:?}",
                new.seqno
            )));
        }
        let mut store = self.data.lock().map_err(Error::from)?;

        let data = match store.get(key) {
            None => None,
            Some(values) => values.last(),
        };

        let seqno = data.as_ref().map(|data| data.seqno);

        if seqno != expected {
            return Ok(CaSResult::ExpectationMismatch);
        }

        store.entry(key.to_string()).or_default().push(new);

        Ok(CaSResult::Committed)
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        // Yield to maximize our chances for getting interesting orderings.
        let () = yield_now().await;
        let store = self.data.lock().map_err(Error::from)?;
        Self::scan_store(&store, key, from, limit)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        // Yield to maximize our chances for getting interesting orderings.
        let () = yield_now().await;
        let current = self.head(key).await?;
        if current.map_or(true, |data| data.seqno < seqno) {
            return Err(ExternalError::from(anyhow!(
                "upper bound too high for truncate: {:?}",
                seqno
            )));
        }

        let mut store = self.data.lock().map_err(Error::from)?;

        let mut deleted = 0;
        if let Some(values) = store.get_mut(key) {
            let count_before = values.len();
            values.retain(|val| val.seqno >= seqno);
            deleted += count_before - values.len();
        }

        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use crate::location::tests::{blob_impl_test, consensus_impl_test};

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn mem_blob() -> Result<(), ExternalError> {
        let registry = Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new(false)));
        blob_impl_test(move |path| {
            let path = path.to_owned();
            let registry = Arc::clone(&registry);
            async move { Ok(registry.lock().await.blob(&path)) }
        })
        .await?;

        let registry = Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new(true)));
        blob_impl_test(move |path| {
            let path = path.to_owned();
            let registry = Arc::clone(&registry);
            async move { Ok(registry.lock().await.blob(&path)) }
        })
        .await?;

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn mem_consensus() -> Result<(), ExternalError> {
        consensus_impl_test(|| async { Ok(MemConsensus::default()) }).await
    }
}
