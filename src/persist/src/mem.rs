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
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use once_cell::sync::Lazy;

use crate::location::{Atomicity, BlobMulti, Consensus, ExternalError, SeqNo, VersionedData};

type BlobData = Arc<Mutex<HashMap<String, Bytes>>>;

/// An in-memory representation of [BlobMulti]s that can be reused across dataflows
static BLOB_REGISTRY: Lazy<Mutex<HashMap<PathBuf, BlobData>>> = Lazy::new(Default::default);

/// Configuration for opening a [MemBlobMulti].
#[derive(Clone, Debug, Default)]
pub struct MemBlobMultiConfig {
    path: PathBuf,
}

impl<P: AsRef<Path>> From<P> for MemBlobMultiConfig {
    fn from(path: P) -> Self {
        MemBlobMultiConfig {
            path: path.as_ref().to_path_buf(),
        }
    }
}

/// An in-memory implementation of [BlobMulti].
#[derive(Debug)]
pub struct MemBlobMulti {
    dataz: BlobData,
}

impl MemBlobMulti {
    /// Opens the given location for non-exclusive read-write access.
    pub fn open(config: MemBlobMultiConfig) -> Self {
        let dataz = Arc::clone(
            BLOB_REGISTRY
                .lock()
                .unwrap()
                .entry(config.path)
                .or_default(),
        );
        MemBlobMulti { dataz }
    }
}

#[async_trait]
impl BlobMulti for MemBlobMulti {
    async fn get(&self, _deadline: Instant, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        let dataz = self.dataz.lock().unwrap();
        Ok(dataz.get(key).map(|x| x.to_vec()))
    }

    async fn list_keys(&self, _deadline: Instant) -> Result<Vec<String>, ExternalError> {
        let dataz = self.dataz.lock().unwrap();
        Ok(dataz.keys().cloned().collect())
    }

    async fn set(
        &self,
        _deadline: Instant,
        key: &str,
        value: Bytes,
        _atomic: Atomicity,
    ) -> Result<(), ExternalError> {
        // NB: This is always atomic, so we're free to ignore the atomic param.
        let mut dataz = self.dataz.lock().unwrap();
        dataz.insert(key.to_owned(), value);
        Ok(())
    }

    async fn delete(&self, _deadline: Instant, key: &str) -> Result<(), ExternalError> {
        let mut dataz = self.dataz.lock().unwrap();
        dataz.remove(key);
        Ok(())
    }
}

/// Configuration to construct an in-memory implementation of [Consensus].
#[derive(Clone, Debug, Default)]
pub struct MemConsensusConfig {
    path: PathBuf,
}

impl<P: AsRef<Path>> From<P> for MemConsensusConfig {
    fn from(path: P) -> Self {
        MemConsensusConfig {
            path: path.as_ref().to_path_buf(),
        }
    }
}

type ConensusData = Arc<Mutex<HashMap<String, Vec<VersionedData>>>>;

/// An in-memory representation of a set of [Consensus]s that can be reused across dataflows
static CONSENSUS_REGISTRY: Lazy<Mutex<HashMap<PathBuf, ConensusData>>> =
    Lazy::new(Default::default);

/// An in-memory implementation of [Consensus].
#[derive(Debug)]
pub struct MemConsensus {
    store: ConensusData,
}

impl Default for MemConsensus {
    fn default() -> Self {
        Self::open(MemConsensusConfig::default())
    }
}

impl MemConsensus {
    fn open(config: MemConsensusConfig) -> Self {
        let store = Arc::clone(
            CONSENSUS_REGISTRY
                .lock()
                .unwrap()
                .entry(config.path)
                .or_default(),
        );
        Self { store }
    }
}

#[async_trait]
impl Consensus for MemConsensus {
    async fn head(
        &self,
        _deadline: Instant,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError> {
        let store = self.store.lock().unwrap();
        let values = match store.get(key) {
            None => return Ok(None),
            Some(values) => values,
        };

        Ok(values.last().cloned())
    }

    async fn compare_and_set(
        &self,
        _deadline: Instant,
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
        let mut store = self.store.lock().unwrap();

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

    async fn scan(
        &self,
        _deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let store = self.store.lock().unwrap();
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

    async fn truncate(
        &self,
        deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError> {
        let current = self.head(deadline, key).await?;
        if current.map_or(true, |data| data.seqno < seqno) {
            return Err(ExternalError::from(anyhow!(
                "upper bound too high for truncate: {:?}",
                seqno
            )));
        }

        let mut store = self.store.lock().unwrap();

        if let Some(values) = store.get_mut(key) {
            values.retain(|val| val.seqno >= seqno);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use crate::location::tests::{blob_multi_impl_test, consensus_impl_test};

    use super::*;

    #[tokio::test]
    async fn mem_blob_multi() -> Result<(), ExternalError> {
        blob_multi_impl_test(|path| ready(Ok(MemBlobMulti::open(MemBlobMultiConfig::from(path)))))
            .await
    }

    #[tokio::test]
    async fn mem_consensus() -> Result<(), ExternalError> {
        consensus_impl_test(|| ready(Ok(MemConsensus::open(MemConsensusConfig::default())))).await
    }
}
