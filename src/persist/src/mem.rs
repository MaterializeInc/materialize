// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-memory implementations for testing and benchmarking.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use mz_ore::cast::CastFrom;

use crate::error::Error;
use crate::location::{
    Atomicity, Blob, BlobMetadata, Consensus, ExternalError, SeqNo, VersionedData,
};

/// An in-memory representation of a set of [Log]s and [Blob]s that can be reused
/// across dataflows
#[cfg(test)]
#[derive(Debug)]
pub struct MemMultiRegistry {
    blob_by_path: HashMap<String, Arc<tokio::sync::Mutex<MemBlobCore>>>,
}

#[cfg(test)]
impl MemMultiRegistry {
    /// Constructs a new, empty [MemMultiRegistry].
    pub fn new() -> Self {
        MemMultiRegistry {
            blob_by_path: HashMap::new(),
        }
    }

    /// Opens a [MemBlob] associated with `path`.
    ///
    /// TODO: Replace this with PersistClientCache once they're in the same
    /// crate.
    pub async fn blob(&mut self, path: &str) -> MemBlob {
        if let Some(blob) = self.blob_by_path.get(path) {
            MemBlob::open(MemBlobConfig {
                core: Arc::clone(&blob),
            })
        } else {
            let blob = Arc::new(tokio::sync::Mutex::new(MemBlobCore::default()));
            self.blob_by_path
                .insert(path.to_string(), Arc::clone(&blob));
            MemBlob::open(MemBlobConfig { core: blob })
        }
    }
}

#[derive(Debug, Default)]
struct MemBlobCore {
    dataz: HashMap<String, Bytes>,
}

impl MemBlobCore {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        Ok(self.dataz.get(key).map(|x| x.to_vec()))
    }

    fn set(&mut self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        self.dataz.insert(key.to_owned(), value);
        Ok(())
    }

    fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        for (key, value) in &self.dataz {
            if !key.starts_with(key_prefix) {
                continue;
            }

            f(BlobMetadata {
                key: &key,
                size_in_bytes: u64::cast_from(value.len()),
            });
        }

        Ok(())
    }

    fn delete(&mut self, key: &str) -> Result<(), ExternalError> {
        self.dataz.remove(key);
        Ok(())
    }
}

/// Configuration for opening a [MemBlob].
#[derive(Debug, Default)]
pub struct MemBlobConfig {
    core: Arc<tokio::sync::Mutex<MemBlobCore>>,
}

/// An in-memory implementation of [Blob].
#[derive(Debug)]
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
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        self.core.lock().await.get(key)
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.core.lock().await.list_keys_and_metadata(key_prefix, f)
    }

    async fn set(&self, key: &str, value: Bytes, _atomic: Atomicity) -> Result<(), ExternalError> {
        // NB: This is always atomic, so we're free to ignore the atomic param.
        self.core.lock().await.set(key, value)
    }

    async fn delete(&self, key: &str) -> Result<(), ExternalError> {
        self.core.lock().await.delete(key)
    }
}

/// An in-memory implementation of [Consensus].
#[derive(Debug)]
pub struct MemConsensus {
    // TODO: This was intended to be a tokio::sync::Mutex but that seems to
    // regularly deadlock in the `concurrency` test.
    data: Arc<Mutex<HashMap<String, Vec<VersionedData>>>>,
}

impl Default for MemConsensus {
    fn default() -> Self {
        Self {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl Consensus for MemConsensus {
    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
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
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(ExternalError::from(
                        anyhow!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, expected)));
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
            return Ok(Err(data.cloned()));
        }

        store.entry(key.to_string()).or_default().push(new);

        Ok(Ok(()))
    }

    async fn scan(&self, key: &str, from: SeqNo) -> Result<Vec<VersionedData>, ExternalError> {
        let store = self.data.lock().map_err(Error::from)?;
        let mut results = vec![];
        if let Some(values) = store.get(key) {
            // TODO: we could instead binary search to find the first valid
            // key and then binary search the rest.
            for value in values {
                if value.seqno >= from {
                    results.push(value.clone());
                }
            }
        }

        if results.is_empty() {
            Err(ExternalError::from(anyhow!(
                "sequence number lower bound too high for scan: {:?}",
                from
            )))
        } else {
            Ok(results)
        }
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<(), ExternalError> {
        let current = self.head(key).await?;
        if current.map_or(true, |data| data.seqno < seqno) {
            return Err(ExternalError::from(anyhow!(
                "upper bound too high for truncate: {:?}",
                seqno
            )));
        }

        let mut store = self.data.lock().map_err(Error::from)?;

        if let Some(values) = store.get_mut(key) {
            values.retain(|val| val.seqno >= seqno);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::location::tests::{blob_impl_test, consensus_impl_test};

    use super::*;

    #[tokio::test]
    async fn mem_blob() -> Result<(), ExternalError> {
        let registry = Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new()));
        blob_impl_test(move |path| {
            let path = path.to_owned();
            let registry = Arc::clone(&registry);
            async move { Ok(registry.lock().await.blob(&path).await) }
        })
        .await
    }

    #[tokio::test]
    async fn mem_consensus() -> Result<(), ExternalError> {
        consensus_impl_test(|| async { Ok(MemConsensus::default()) }).await
    }
}
