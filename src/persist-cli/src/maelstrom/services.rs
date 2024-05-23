// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementations of Maelstrom services as persist external durability

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use mz_ore::bytes::SegmentedBytes;
use mz_persist::location::{
    Blob, BlobMetadata, CaSResult, Consensus, Determinate, ExternalError, ResultStream, SeqNo,
    VersionedData,
};
use mz_repr::TimestampManipulation;
use mz_timestamp_oracle::{TimestampOracle, WriteTimestamp};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;
use tracing::debug;

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
    cache: Mutex<BTreeMap<(String, SeqNo), Vec<u8>>>,
}

impl MaelstromConsensus {
    pub fn new(handle: Handle) -> Arc<dyn Consensus> {
        Arc::new(MaelstromConsensus {
            handle,
            cache: Mutex::new(BTreeMap::new()),
        })
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
    fn list_keys(&self) -> ResultStream<String> {
        unimplemented!("TODO")
    }

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
    ) -> Result<CaSResult, ExternalError> {
        let create_if_not_exists = expected.is_none();

        let from = match expected {
            Some(expected) => match self.hydrate_seqno(key, expected).await? {
                Ok(x) => Value::from(&MaelstromVersionedData::from(x)),
                Err(_) => {
                    return Ok(CaSResult::ExpectationMismatch);
                }
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
                Ok(CaSResult::Committed)
            }
            Err(MaelstromError {
                code: ErrorCode::PreconditionFailed,
                ..
            }) => Ok(CaSResult::ExpectationMismatch),
            Err(err) => Err(ExternalError::from(anyhow::Error::new(err))),
        }
    }

    async fn scan(
        &self,
        _key: &str,
        _from: SeqNo,
        _limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        unimplemented!("TODO")
    }

    async fn truncate(&self, _key: &str, _seqno: SeqNo) -> Result<usize, ExternalError> {
        unimplemented!("TODO")
    }
}

/// Implementation of [Blob] backed by the Maelstrom lin-kv service.
#[derive(Debug)]
pub struct MaelstromBlob {
    handle: Handle,
}

impl MaelstromBlob {
    pub fn new(handle: Handle) -> Arc<dyn Blob> {
        Arc::new(MaelstromBlob { handle })
    }
}

#[async_trait]
impl Blob for MaelstromBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
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
        let value: Vec<u8> = serde_json::from_str(value)
            .map_err(|err| anyhow!("invalid blob at {}: {}", key, err))?;
        Ok(Some(SegmentedBytes::from(value)))
    }

    async fn list_keys_and_metadata(
        &self,
        _key_prefix: &str,
        _f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        unimplemented!("not yet used")
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let value = serde_json::to_string(value.as_ref()).expect("failed to serialize value");
        self.handle
            .lin_kv_write(Value::from(format!("blob/{}", key)), Value::from(value))
            .await
            .map_err(anyhow::Error::new)?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        // Setting the value to Null is as close as we can get with lin_kv.
        self.handle
            .lin_kv_write(Value::from(format!("blob/{}", key)), Value::Null)
            .await
            .map_err(anyhow::Error::new)?;
        // The "existed" return value only controls our metrics, so I suppose
        // it's okay if its wrong in Maelstrom.
        Ok(Some(0))
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        let read = self
            .handle
            .lin_kv_read(Value::from(format!("blob/{}", key)))
            .await
            .map_err(anyhow::Error::new)?;
        if read.is_none() {
            return Err(
                Determinate::new(anyhow!("key {key} not present in the maelstrom store")).into(),
            );
        }
        Ok(())
    }
}

/// Implementation of [Blob] that caches get calls.
///
/// NB: It intentionally does not store successful set calls in the cache, so
/// that a blob gets fetched at least once (exercising those code paths).
#[derive(Debug)]
pub struct CachingBlob {
    blob: Arc<dyn Blob>,
    cache: Mutex<BTreeMap<String, SegmentedBytes>>,
}

impl CachingBlob {
    pub fn new(blob: Arc<dyn Blob>) -> Arc<dyn Blob> {
        Arc::new(CachingBlob {
            blob,
            cache: Mutex::new(BTreeMap::new()),
        })
    }
}

#[async_trait]
impl Blob for CachingBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
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

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        // Intentionally don't put this in the cache on set, so that this blob
        // gets fetched at least once (exercising those code paths).
        self.blob.set(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        self.cache.lock().await.remove(key);
        self.blob.delete(key).await
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        self.blob.restore(key).await
    }
}

#[derive(Debug)]
pub struct MaelstromOracle {
    read_ts: MaelstromOracleKey,
    write_ts: MaelstromOracleKey,
}

#[derive(Debug)]
pub struct MaelstromOracleKey {
    handle: Handle,
    key: String,
    expected: u64,
}

// TODO: Make this implement TimestampOracle.
impl MaelstromOracle {
    pub async fn new(handle: Handle) -> Result<Self, ExternalError> {
        let read_ts = MaelstromOracleKey::new(handle.clone(), "tso_read", 0).await?;
        let write_ts = MaelstromOracleKey::new(handle.clone(), "tso_write", 0).await?;
        Ok(MaelstromOracle { read_ts, write_ts })
    }

    pub async fn write_ts(&mut self) -> Result<u64, ExternalError> {
        let prev = self.write_ts.cas(|expected| Some(expected + 1)).await?;
        Ok(prev)
    }

    pub async fn peek_write_ts(&mut self) -> Result<u64, ExternalError> {
        self.write_ts.peek().await
    }

    pub async fn read_ts(&mut self) -> Result<u64, ExternalError> {
        self.read_ts.peek().await
    }

    pub async fn apply_write(&mut self, lower_bound: u64) -> Result<(), ExternalError> {
        let write_prev = self
            .write_ts
            .cas(|expected| (expected < lower_bound).then_some(lower_bound))
            .await?;
        let read_prev = self
            .read_ts
            .cas(|expected| (expected < lower_bound).then_some(lower_bound))
            .await?;
        debug!(
            "apply_write {} write_prev={} write_new={} read_prev={} read_new={}",
            lower_bound, write_prev, self.write_ts.expected, read_prev, self.read_ts.expected
        );
        Ok(())
    }
}

impl MaelstromOracleKey {
    async fn new(handle: Handle, key: &str, init_ts: u64) -> Result<Self, ExternalError> {
        let res = handle
            .lin_kv_compare_and_set(
                Value::from(key),
                Value::Null,
                Value::from(init_ts),
                Some(true),
            )
            .await;
        // Either of these answers indicate that we created the key.
        match Self::cas_res(res)? {
            CaSResult::Committed => {}
            CaSResult::ExpectationMismatch => {}
        }
        Ok(MaelstromOracleKey {
            handle,
            key: key.to_owned(),
            expected: init_ts,
        })
    }

    fn cas_res(res: Result<(), MaelstromError>) -> Result<CaSResult, ExternalError> {
        match res {
            Ok(()) => Ok(CaSResult::Committed),
            Err(MaelstromError {
                code: ErrorCode::PreconditionFailed,
                ..
            }) => Ok(CaSResult::ExpectationMismatch),
            Err(err) => Err(anyhow::Error::new(err).into()),
        }
    }

    async fn peek(&mut self) -> Result<u64, ExternalError> {
        let value = self
            .handle
            .lin_kv_read(Value::from(self.key.as_str()))
            .await
            .map_err(anyhow::Error::new)?
            .expect("ts oracle should be initialized");
        let current = value
            .as_u64()
            .ok_or_else(|| anyhow!("invalid {} value: {:?}", self.key, value))?;
        assert!(self.expected <= current);
        self.expected = current;
        Ok(current)
    }

    async fn cas(&mut self, new_fn: impl Fn(u64) -> Option<u64>) -> Result<u64, ExternalError> {
        loop {
            let new = match new_fn(self.expected) {
                Some(x) => x,
                None => {
                    // The latest cached value is good enough, early exit.
                    return Ok(self.expected);
                }
            };
            let res = self
                .handle
                .lin_kv_compare_and_set(
                    Value::from(self.key.as_str()),
                    Value::from(self.expected),
                    Value::from(new),
                    None,
                )
                .await;
            match Self::cas_res(res)? {
                CaSResult::Committed => {
                    let prev = self.expected;
                    self.expected = new;
                    return Ok(prev);
                }
                CaSResult::ExpectationMismatch => {
                    self.expected = self.peek().await?;
                    continue;
                }
            };
        }
    }
}

#[derive(Debug, Default)]
pub struct MemTimestampOracle<T> {
    read_write_ts: Arc<std::sync::Mutex<(T, T)>>,
}

#[async_trait]
impl<T: TimestampManipulation> TimestampOracle<T> for MemTimestampOracle<T> {
    async fn write_ts(&self) -> WriteTimestamp<T> {
        let (read_ts, write_ts) = &mut *self.read_write_ts.lock().expect("lock poisoned");
        let new_write_ts = TimestampManipulation::step_forward(std::cmp::max(read_ts, write_ts));
        write_ts.clone_from(&new_write_ts);
        WriteTimestamp {
            advance_to: TimestampManipulation::step_forward(&new_write_ts),
            timestamp: new_write_ts,
        }
    }

    async fn peek_write_ts(&self) -> T {
        let (_, write_ts) = &*self.read_write_ts.lock().expect("lock poisoned");
        write_ts.clone()
    }

    async fn read_ts(&self) -> T {
        let (read_ts, _) = &*self.read_write_ts.lock().expect("lock poisoned");
        read_ts.clone()
    }

    async fn apply_write(&self, lower_bound: T) {
        let (read_ts, write_ts) = &mut *self.read_write_ts.lock().expect("lock poisoned");
        *read_ts = std::cmp::max(read_ts.clone(), lower_bound);
        *write_ts = std::cmp::max(read_ts, write_ts).clone();
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
