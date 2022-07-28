// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations of Maelstrom services as persist external durability

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use mz_persist::location::{
    Atomicity, Blob, BlobMetadata, Consensus, ExternalError, SeqNo, VersionedData,
};

use crate::maelstrom::api::{ErrorCode, MaelstromError};
use crate::maelstrom::node::Handle;

/// An adaptor for [VersionedData] that implements [Serialize] and
/// [Deserialize].
#[derive(Clone, Debug, Serialize, Deserialize)]
struct MaelstromVersionedData {
    seqno: u64,
    data: Vec<u8>,
}

/// Implementation of [Consensus] backed by the Maelstrom lin-kv service.
#[derive(Debug)]
pub struct MaelstromConsensus {
    handle: Handle,
    // A cache of SeqNo -> the data for that SeqNo. Here because the [Consensus]
    // interface uses only a SeqNo for expected/from, but the lin-kv Maelstrom
    // service requires the whole VersionedData. This is only used to hydrate
    // the expected/from in the impl of `compare_and_set`.
    //
    // This cache exists primarily to keep our usage of maelstrom services
    // "clean". Instead of the cache, we could use the consensus `head` call to
    // hydrate SeqNos, but it's really nice to have a 1:1 relationship between
    // Consensus calls and Maelstrom service calls when looking at the Lamport
    // diagrams that Maelstrom emits.
    //
    // It also secondarily means that more stale expectations make it to the
    // Maelstrom CaS call (with the `head` alternative we'd discover some then),
    // but this is mostly a side benefit.
    cache: Mutex<HashMap<(String, SeqNo), Vec<u8>>>,
}

impl MaelstromConsensus {
    pub fn new(handle: Handle) -> Arc<dyn Consensus + Send + Sync> {
        Arc::new(MaelstromConsensus {
            handle,
            cache: Mutex::new(HashMap::new()),
        }) as Arc<dyn Consensus + Send + Sync>
    }

    pub async fn hydrate_seqno(
        &self,
        key: &str,
        expected: SeqNo,
    ) -> Result<Result<VersionedData, Option<VersionedData>>, ExternalError> {
        if let Some(data) = self.cache.lock().await.get(&(key.to_string(), expected)) {
            let value = VersionedData {
                seqno: expected.clone(),
                data: Bytes::from(data.clone()),
            };
            return Ok(Ok(value));
        }

        // It wasn't in the cache (must have been set by another process), fetch
        // head and see if that matches.
        match self.head(key).await? {
            Some(current) if current.seqno == expected => Ok(Ok(current)),
            x => Ok(Err(x)),
        }
    }
}

#[async_trait]
impl Consensus for MaelstromConsensus {
    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let value = match self
            .handle
            .lin_kv_read(Value::from(format!("consensus/{}", key)))
            .await
            .map_err(anyhow::Error::new)?
        {
            Some(x) => x,
            None => return Ok(None),
        };
        let value = VersionedData::from(MaelstromVersionedData::try_from(&value)?);
        self.cache
            .lock()
            .await
            .insert((key.to_string(), value.seqno), value.data.to_vec());
        Ok(Some(value))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        let create_if_not_exists = expected.is_none();

        let from = match expected {
            Some(expected) => match self.hydrate_seqno(key, expected).await? {
                Ok(x) => Value::from(&MaelstromVersionedData::from(x)),
                Err(x) => return Ok(Err(x)),
            },
            None => Value::Null,
        };
        let new = MaelstromVersionedData::from(new);
        let to = Value::from(&new);
        let cas_res = self
            .handle
            .lin_kv_compare_and_set(
                Value::from(format!("consensus/{}", key)),
                from,
                to,
                Some(create_if_not_exists),
            )
            .await;
        match cas_res {
            Ok(()) => {
                self.cache
                    .lock()
                    .await
                    .insert((key.to_string(), SeqNo(new.seqno)), new.data.clone());
                Ok(Ok(()))
            }
            Err(MaelstromError {
                code: ErrorCode::PreconditionFailed,
                ..
            }) => {
                // TODO: Parse the current value out of the error string instead
                // of another service fetch.
                let current = self.head(key).await?;
                Ok(Err(current))
            }
            Err(err) => Err(ExternalError::from(anyhow::Error::new(err))),
        }
    }

    async fn scan(&self, _key: &str, _from: SeqNo) -> Result<Vec<VersionedData>, ExternalError> {
        unimplemented!("not yet used")
    }

    async fn truncate(&self, _key: &str, _seqno: SeqNo) -> Result<usize, ExternalError> {
        // No-op until we implement `scan`.
        Ok(0)
    }
}

/// Implementation of [Blob] backed by the Maelstrom lin-kv service.
#[derive(Debug)]
pub struct MaelstromBlob {
    handle: Handle,
}

impl MaelstromBlob {
    pub fn new(handle: Handle) -> Arc<dyn Blob + Send + Sync> {
        Arc::new(MaelstromBlob { handle }) as Arc<dyn Blob + Send + Sync>
    }
}

#[async_trait]
impl Blob for MaelstromBlob {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        let value = match self
            .handle
            .lin_kv_read(Value::from(format!("blob/{}", key)))
            .await
            .map_err(anyhow::Error::new)?
        {
            Some(x) => x,
            None => return Ok(None),
        };
        let value = value
            .as_str()
            .ok_or_else(|| anyhow!("invalid blob at {}: {:?}", key, value))?;
        let value = serde_json::from_str(value)
            .map_err(|err| anyhow!("invalid blob at {}: {}", key, err))?;
        Ok(Some(value))
    }

    async fn list_keys_and_metadata(
        &self,
        _key_prefix: &str,
        _f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        unimplemented!("not yet used")
    }

    async fn set(&self, key: &str, value: Bytes, _atomic: Atomicity) -> Result<(), ExternalError> {
        // lin_kv_write is always atomic, so we're free to ignore the atomic
        // param.
        let value = serde_json::to_string(value.as_ref()).expect("failed to serialize value");
        self.handle
            .lin_kv_write(Value::from(format!("blob/{}", key)), Value::from(value))
            .await
            .map_err(anyhow::Error::new)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), ExternalError> {
        // Setting the value to Null is as close as we can get with lin_kv.
        self.handle
            .lin_kv_write(Value::from(format!("blob/{}", key)), Value::Null)
            .await
            .map_err(anyhow::Error::new)?;
        Ok(())
    }
}

/// Implementation of [Blob] that caches get calls.
///
/// NB: It intentionally does not store successful set calls in the cache, so
/// that a blob gets fetched at least once (exercising those code paths).
#[derive(Debug)]
pub struct CachingBlob {
    blob: Arc<dyn Blob + Send + Sync>,
    cache: Mutex<HashMap<String, Vec<u8>>>,
}

impl CachingBlob {
    pub fn new(blob: Arc<dyn Blob + Send + Sync>) -> Arc<dyn Blob + Send + Sync> {
        Arc::new(CachingBlob {
            blob,
            cache: Mutex::new(HashMap::new()),
        }) as Arc<dyn Blob + Send + Sync>
    }
}

#[async_trait]
impl Blob for CachingBlob {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        // Fetch the cached value if there is one.
        let cache = self.cache.lock().await;
        if let Some(value) = cache.get(key) {
            return Ok(Some(value.clone()));
        }
        // Make sure not to hold the cache lock across the forwarded `get` call.
        drop(cache);

        // We didn't get a cache hit, fetch the value and update the cache.
        let value = self.blob.get(key).await?;
        if let Some(value) = &value {
            // Everything in persist is write-once modify-never, so until we add
            // support for deletions to CachingBlob, we're free to blindly
            // overwrite whatever is in the cache.
            self.cache
                .lock()
                .await
                .insert(key.to_owned(), value.clone());
        }

        Ok(value)
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.blob.list_keys_and_metadata(key_prefix, f).await
    }

    async fn set(&self, key: &str, value: Bytes, atomic: Atomicity) -> Result<(), ExternalError> {
        // Intentionally don't put this in the cache on set, so that this blob
        // gets fetched at least once (exercising those code paths).
        self.blob.set(key, value, atomic).await
    }

    async fn delete(&self, key: &str) -> Result<(), ExternalError> {
        self.cache.lock().await.remove(key);
        self.blob.delete(key).await
    }
}

mod from_impls {
    use bytes::Bytes;
    use mz_persist::location::{ExternalError, SeqNo, VersionedData};
    use serde_json::Value;

    use crate::maelstrom::services::MaelstromVersionedData;

    impl From<VersionedData> for MaelstromVersionedData {
        fn from(x: VersionedData) -> Self {
            MaelstromVersionedData {
                seqno: x.seqno.0,
                data: x.data.to_vec(),
            }
        }
    }

    impl From<MaelstromVersionedData> for VersionedData {
        fn from(x: MaelstromVersionedData) -> Self {
            VersionedData {
                seqno: SeqNo(x.seqno),
                data: Bytes::from(x.data),
            }
        }
    }

    impl From<&MaelstromVersionedData> for Value {
        fn from(x: &MaelstromVersionedData) -> Self {
            let json = serde_json::to_string(x).expect("MaelstromVersionedData wasn't valid json");
            serde_json::from_str(&json).expect("MaelstromVersionedData wasn't valid json")
        }
    }

    impl TryFrom<&Value> for MaelstromVersionedData {
        type Error = ExternalError;

        fn try_from(x: &Value) -> Result<Self, Self::Error> {
            let json = serde_json::to_string(x)
                .map_err(|err| ExternalError::from(anyhow::Error::new(err)))?;
            serde_json::from_str(&json).map_err(|err| ExternalError::from(anyhow::Error::new(err)))
        }
    }
}
