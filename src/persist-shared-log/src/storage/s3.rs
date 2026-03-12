// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! S3-backed storage for log batches and snapshots.

use std::collections::BTreeMap;

use aws_sdk_s3::primitives::ByteStream;
use prost::Message;

use mz_persist::generated::consensus_service::{ProtoSnapshot, ProtoLogBatch};

use crate::ShardState;
use crate::storage::{Storage, StorageError, serialize_snapshot};

/// S3-backed storage for log batches and snapshots.
pub struct S3Storage {
    client: mz_aws_util::s3::Client,
    bucket: String,
    prefix: String,
}

impl S3Storage {
    /// Creates a new S3-backed storage backend.
    pub async fn new(bucket: &str, prefix: &str, endpoint: Option<&str>, region: &str) -> Self {
        let mut config_loader =
            mz_aws_util::defaults().region(aws_sdk_s3::config::Region::new(region.to_owned()));
        if let Some(endpoint) = endpoint {
            config_loader = config_loader.endpoint_url(endpoint);
        }
        let config = config_loader.load().await;
        let client = mz_aws_util::s3::new_client(&config);
        S3Storage {
            client,
            bucket: bucket.to_owned(),
            prefix: prefix.to_owned(),
        }
    }

    fn log_key(&self, batch_number: u64) -> String {
        format!("{}log/{:020}", self.prefix, batch_number)
    }

    fn snapshot_key(&self) -> String {
        format!("{}snapshot", self.prefix)
    }
}

#[async_trait::async_trait]
impl Storage for S3Storage {
    async fn write_batch(&self, batch: &ProtoLogBatch) -> Result<(), StorageError> {
        let key = self.log_key(batch.batch_number);
        let body = batch.encode_to_vec();
        match self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(body))
            .if_none_match("*")
            .send()
            .await
        {
            Ok(_) => Ok(()),
            Err(sdk_err) => {
                let service_err = sdk_err.into_service_error();
                // S3 Express returns "ConditionalRequestConflict",
                // standard S3 returns "PreconditionFailed".
                let code = service_err.meta().code().unwrap_or("");
                if code == "PreconditionFailed" || code == "ConditionalRequestConflict" {
                    Err(StorageError::AlreadyExists)
                } else {
                    Err(StorageError::Failed(anyhow::anyhow!(
                        "S3 PUT log/{}: {}",
                        batch.batch_number,
                        service_err
                    )))
                }
            }
        }
    }

    async fn write_snapshot(
        &self,
        shards: &BTreeMap<String, ShardState>,
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

    async fn read_batch(&self, batch_number: u64) -> Result<Option<ProtoLogBatch>, anyhow::Error> {
        let key = self.log_key(batch_number);
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
                let batch = ProtoLogBatch::decode(body)?;
                Ok(Some(batch))
            }
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!(
                        "S3 GET log/{}: {}",
                        batch_number,
                        service_err
                    ))
                }
            }
        }
    }
}
