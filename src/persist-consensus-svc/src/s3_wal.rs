// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! S3 WAL and snapshot read/write for the consensus service.

use std::collections::HashMap;

use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use prost::Message;

use mz_persist::generated::consensus_service::{
    ProtoShardState, ProtoSnapshot, ProtoVersionedData, ProtoWalBatch,
};

use crate::actor::{ShardState, VersionedEntry};

/// Trait for WAL writing, enabling mock implementations in tests.
#[async_trait::async_trait]
pub trait WalWriter: Send {
    /// Write a WAL batch to durable storage.
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), anyhow::Error>;
    /// Write a full snapshot to durable storage.
    async fn write_snapshot(
        &self,
        shards: &HashMap<String, ShardState>,
        through_batch: u64,
    ) -> Result<(), anyhow::Error>;
    /// Read a snapshot from durable storage.
    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error>;
    /// Read a WAL batch by number.
    async fn read_batch(&self, batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error>;
}

#[async_trait::async_trait]
impl<W: WalWriter + Sync> WalWriter for std::sync::Arc<W> {
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), anyhow::Error> {
        (**self).write_batch(batch).await
    }
    async fn write_snapshot(
        &self,
        shards: &HashMap<String, ShardState>,
        through_batch: u64,
    ) -> Result<(), anyhow::Error> {
        (**self).write_snapshot(shards, through_batch).await
    }
    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error> {
        (**self).read_snapshot().await
    }
    async fn read_batch(&self, batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error> {
        (**self).read_batch(batch_number).await
    }
}

/// S3-backed WAL and snapshot writer.
pub struct S3WalWriter {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3WalWriter {
    /// Creates a new S3WalWriter.
    pub async fn new(
        bucket: &str,
        prefix: &str,
        endpoint: Option<&str>,
        region: &str,
    ) -> Self {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new(region.to_owned()));
        if let Some(endpoint) = endpoint {
            config_loader = config_loader.endpoint_url(endpoint);
        }
        let config = config_loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();
        let client = Client::from_conf(s3_config);
        S3WalWriter {
            client,
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
        }
    }

    fn wal_key(&self, batch_number: u64) -> String {
        format!("{}wal/{:020}", self.prefix, batch_number)
    }

    fn snapshot_key(&self) -> String {
        format!("{}snapshot", self.prefix)
    }
}

#[async_trait::async_trait]
impl WalWriter for S3WalWriter {
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), anyhow::Error> {
        let key = self.wal_key(batch.batch_number);
        let body = batch.encode_to_vec();
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(body))
            .if_none_match("*")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("S3 PUT wal/{}: {}", batch.batch_number, e))?;
        Ok(())
    }

    async fn write_snapshot(
        &self,
        shards: &HashMap<String, ShardState>,
        through_batch: u64,
    ) -> Result<(), anyhow::Error> {
        let snapshot = serialize_snapshot(shards, through_batch);
        let body = snapshot.encode_to_vec();
        let key = self.snapshot_key();
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(body))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("S3 PUT snapshot: {}", e))?;
        Ok(())
    }

    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error> {
        let key = self.snapshot_key();
        match self.client.get_object().bucket(&self.bucket).key(&key).send().await {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes();
                let snapshot = ProtoSnapshot::decode(body)?;
                Ok(Some(snapshot))
            }
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("S3 GET snapshot: {}", service_err))
                }
            }
        }
    }

    async fn read_batch(&self, batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error> {
        let key = self.wal_key(batch_number);
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(output) => {
                let body = output.body.collect().await?.into_bytes();
                let batch = ProtoWalBatch::decode(body)?;
                Ok(Some(batch))
            }
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("S3 GET wal/{}: {}", batch_number, service_err))
                }
            }
        }
    }
}

/// Serializes in-memory shard state to a `ProtoSnapshot`.
pub fn serialize_snapshot(
    shards: &HashMap<String, ShardState>,
    through_batch: u64,
) -> ProtoSnapshot {
    let proto_shards = shards
        .iter()
        .map(|(key, state)| {
            let entries = state
                .entries
                .iter()
                .map(|e| ProtoVersionedData {
                    seqno: e.seqno,
                    data: e.data.to_vec(),
                })
                .collect();
            (key.clone(), ProtoShardState { entries })
        })
        .collect();
    ProtoSnapshot {
        through_batch,
        shards: proto_shards,
    }
}

/// Deserializes a `ProtoSnapshot` into in-memory shard state.
pub fn deserialize_snapshot(
    snapshot: &ProtoSnapshot,
) -> (HashMap<String, ShardState>, u64) {
    let shards = snapshot
        .shards
        .iter()
        .map(|(key, proto_state)| {
            let entries = proto_state
                .entries
                .iter()
                .map(|e| VersionedEntry {
                    seqno: e.seqno,
                    data: bytes::Bytes::from(e.data.clone()),
                })
                .collect();
            (key.clone(), ShardState { entries })
        })
        .collect();
    (shards, snapshot.through_batch)
}

/// A no-op WAL writer for testing.
#[cfg(test)]
pub struct NoopWalWriter;

#[cfg(test)]
#[async_trait::async_trait]
impl WalWriter for NoopWalWriter {
    async fn write_batch(&self, _batch: &ProtoWalBatch) -> Result<(), anyhow::Error> {
        Ok(())
    }
    async fn write_snapshot(
        &self,
        _shards: &HashMap<String, ShardState>,
        _through_batch: u64,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error> {
        Ok(None)
    }
    async fn read_batch(&self, _batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error> {
        Ok(None)
    }
}

/// A recording WAL writer for testing that records calls.
#[cfg(test)]
pub struct RecordingWalWriter {
    pub batches: std::sync::Mutex<Vec<ProtoWalBatch>>,
    pub snapshots: std::sync::Mutex<Vec<(HashMap<String, ShardState>, u64)>>,
}

#[cfg(test)]
impl RecordingWalWriter {
    pub fn new() -> Self {
        RecordingWalWriter {
            batches: std::sync::Mutex::new(Vec::new()),
            snapshots: std::sync::Mutex::new(Vec::new()),
        }
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl WalWriter for RecordingWalWriter {
    async fn write_batch(&self, batch: &ProtoWalBatch) -> Result<(), anyhow::Error> {
        self.batches.lock().unwrap().push(batch.clone());
        Ok(())
    }
    async fn write_snapshot(
        &self,
        shards: &HashMap<String, ShardState>,
        through_batch: u64,
    ) -> Result<(), anyhow::Error> {
        self.snapshots
            .lock()
            .unwrap()
            .push((shards.clone(), through_batch));
        Ok(())
    }
    async fn read_snapshot(&self) -> Result<Option<ProtoSnapshot>, anyhow::Error> {
        Ok(None)
    }
    async fn read_batch(&self, _batch_number: u64) -> Result<Option<ProtoWalBatch>, anyhow::Error> {
        Ok(None)
    }
}
