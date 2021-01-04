// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functionality for creating S3 sources

use std::convert::{From, TryInto};
use std::default::Default;
use std::ops::AddAssign;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};

use anyhow::{anyhow, Error};
use globset::GlobMatcher;
use rusoto_s3::{GetObjectRequest, S3};
use timely::scheduling::{Activator, SyncActivator};
use tokio::io::AsyncReadExt;

use dataflow_types::{
    AwsConnectInfo, Consistency, DataEncoding, ExternalSourceConnector, MzOffset,
};
use expr::{PartitionId, SourceInstanceId};

use crate::logging::materialized::Logger;
use crate::server::{
    TimestampDataUpdate, TimestampDataUpdates, TimestampMetadataUpdate, TimestampMetadataUpdates,
};
use crate::source::{
    ConsistencyInfo, NextMessage, PartitionMetrics, SourceConstructor, SourceInfo, SourceMessage,
};

type Out = Vec<u8>;

/// Information required to load data from S3
pub struct S3SourceInfo {
    /// The name of the source that the user entered
    source_name: String,
    /// The name of the S3 bucket we are pulling from
    bucket: String,

    // differential control
    /// Unique source ID
    id: SourceInstanceId,
    /// Field is set if this operator is responsible for ingesting data
    is_activated_reader: bool,
    /// Receiver channel that ingests records
    receiver_stream: Receiver<Result<Out, Error>>,
    /// Buffer: store message that cannot yet be timestamped
    buffer: Option<SourceMessage<Out>>,
    /// BucketOffset
    offset: BucketOffset,
}

#[derive(Clone, Copy, Debug)]
struct BucketOffset(i64);

impl AddAssign<i64> for BucketOffset {
    fn add_assign(&mut self, other: i64) {
        self.0 += other;
    }
}

impl From<BucketOffset> for MzOffset {
    fn from(offset: BucketOffset) -> MzOffset {
        MzOffset { offset: offset.0 }
    }
}

impl SourceConstructor<Vec<u8>> for S3SourceInfo {
    fn new(
        source_name: String,
        source_id: SourceInstanceId,
        active: bool,
        worker_id: usize,
        _worker_count: usize,
        logger: Option<Logger>,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        consistency_info: &mut ConsistencyInfo,
        encoding: DataEncoding,
    ) -> Result<S3SourceInfo, anyhow::Error> {
        if !matches!(encoding, DataEncoding::Text | DataEncoding::Bytes) {
            anyhow::bail!("S3 sources only support 'text' or 'bytes' encodings");
        }
        let s3_conn = match connector {
            ExternalSourceConnector::S3(s3_conn) => s3_conn,
            _ => {
                panic!("S3 is the only legitimate ExternalSourceConnector for S3SourceInfo")
            }
        };

        // a single arbitrary worker is responsible for scanning the bucket
        let receiver = if active {
            log::debug!("reading bucket={} worker={}", s3_conn.bucket, worker_id);
            let (tx, rx) = std::sync::mpsc::sync_channel(10000);
            let bucket = s3_conn.bucket.clone();
            let glob = s3_conn.pattern.clone();
            let aws_info = s3_conn.aws_info;
            tokio::spawn(read_bucket_task(
                bucket,
                glob.compile_matcher(),
                aws_info,
                tx,
                Some(consumer_activator),
            ));
            rx
        } else {
            let (_tx, rx) = std::sync::mpsc::sync_channel(0);
            rx
        };

        consistency_info.partition_metrics.insert(
            PartitionId::S3,
            PartitionMetrics::new(&source_name, source_id, &s3_conn.bucket, logger),
        );
        consistency_info.update_partition_metadata(PartitionId::S3);

        Ok(S3SourceInfo {
            source_name,
            bucket: s3_conn.bucket,
            id: source_id,
            is_activated_reader: active,
            receiver_stream: receiver,
            buffer: None,
            offset: BucketOffset(0),
        })
    }
}

async fn read_bucket_task(
    bucket: String,
    glob: GlobMatcher,
    aws_info: AwsConnectInfo,
    tx: SyncSender<anyhow::Result<Vec<u8>>>,
    activator: Option<SyncActivator>,
) {
    let client = match aws_util::s3::client(
        aws_info.region.clone(),
        aws_info.access_key_id,
        aws_info.secret_access_key,
        aws_info.token,
    )
    .await
    {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(anyhow!("Unable to create s3 client: {}", e)))
                .unwrap_or_else(|e| {
                    log::trace!("unable to send error on stream creating s3 client: {}", e)
                });
            return;
        }
    };

    let obj = match client
        .get_object(GetObjectRequest {
            bucket,
            key: glob.glob().to_string(),
            ..Default::default()
        })
        .await
    {
        Ok(obj) => obj,
        Err(e) => {
            tx.send(Err(anyhow!("Unable to get object {}: {}", glob.glob(), e)))
                .unwrap_or_else(|e| log::debug!("unable to send error on stream: {}", e));
            return;
        }
    };

    if let Some(body) = obj.body {
        let mut reader = body.into_async_read();
        // unwrap is safe because content length is not allowed to be negative
        let mut buf = Vec::with_capacity(obj.content_length.unwrap_or(1024).try_into().unwrap());
        match reader.read_to_end(&mut buf).await {
            Ok(_) => {
                let activate = !buf.is_empty();
                let mut lines = 0;
                for line in buf.split(|b| *b == b'\n').map(|s| s.to_vec()) {
                    if let Err(e) = tx.send(Ok(line)) {
                        log::debug!("unable to send read line on stream: {}", e);
                        break;
                    } else {
                        lines += 1;
                    }
                }
                log::trace!("sent {} lines to reader", lines);
                if activate {
                    if let Some(activator) = activator {
                        activator.activate().expect("s3 reader activation failed");
                    }
                }
            }
            Err(e) => {
                tx.send(Err(anyhow!("Unable to read object: {} {}", glob.glob(), e)))
                    .unwrap_or_else(|e| log::debug!("unable to send error on stream: {}", e));
            }
        }
    } else {
        log::info!("get object response had no body: {}", glob.glob());
    }
}

impl SourceInfo<Vec<u8>> for S3SourceInfo {
    fn activate_source_timestamping(
        id: &SourceInstanceId,
        consistency: &Consistency,
        active: bool,
        timestamp_data_updates: TimestampDataUpdates,
        timestamp_metadata_channel: TimestampMetadataUpdates,
    ) -> Option<TimestampMetadataUpdates>
    where
        Self: Sized,
    {
        // Putting source information on the Timestamp channel lets this
        // Dataflow worker communicate that it has created a source.
        if let Consistency::BringYourOwn(_) = consistency {
            log::error!("S3 sources do not currently support BYO consistency");
            None
        } else if active {
            timestamp_data_updates
                .borrow_mut()
                .insert(id.clone(), TimestampDataUpdate::RealTime(1));
            timestamp_metadata_channel
                .as_ref()
                .borrow_mut()
                .push(TimestampMetadataUpdate::StartTimestamping(*id));
            Some(timestamp_metadata_channel)
        } else {
            None
        }
    }

    fn get_next_message(
        &mut self,
        _consistency_info: &mut ConsistencyInfo,
        _activator: &Activator,
    ) -> Result<NextMessage<Out>, anyhow::Error> {
        if let Some(message) = self.buffer.take() {
            return Ok(NextMessage::Ready(message));
        }
        match self.receiver_stream.try_recv() {
            Ok(Ok(record)) => {
                self.offset += 1;
                Ok(NextMessage::Ready(SourceMessage {
                    partition: PartitionId::S3,
                    offset: self.offset.into(),
                    upstream_time_millis: None,
                    key: None,
                    payload: Some(record),
                }))
            }
            Ok(Err(e)) => {
                log::warn!(
                    "when reading bucket '{}' for source '{}' ({}): {}",
                    self.bucket,
                    self.source_name,
                    self.id,
                    e
                );
                Err(e)
            }
            Err(TryRecvError::Empty) => Ok(NextMessage::Pending),
            Err(TryRecvError::Disconnected) => Ok(NextMessage::Finished),
        }
    }

    fn can_close_timestamp(
        &self,
        consistency_info: &ConsistencyInfo,
        pid: &PartitionId,
        offset: MzOffset,
    ) -> bool {
        if !self.is_activated_reader {
            true
        } else {
            // TODO: when is this ever not true for S3?
            let last_offset = consistency_info
                .partition_metadata
                .get(&pid)
                // Guaranteed to exist for S3
                .unwrap()
                .offset;
            last_offset >= offset
        }
    }

    fn get_worker_partition_count(&self) -> i32 {
        panic!("s3 sources do not support BYO consistency: get_worker_partition_count")
    }

    fn has_partition(&self, _partition_id: PartitionId) -> bool {
        panic!("s3 sources do not support BYO consistency: has_partition")
    }

    fn ensure_has_partition(&mut self, _consistency_info: &mut ConsistencyInfo, _pid: PartitionId) {
        panic!("s3 sources do not support BYO consistency: ensure_has_partition")
    }

    fn update_partition_count(
        &mut self,
        consistency_info: &mut ConsistencyInfo,
        partition_count: i32,
    ) {
        log::debug!(
            "ignoring partition count update type={:?} partition_count={}",
            consistency_info.source_type,
            partition_count,
        )
    }

    fn buffer_message(&mut self, message: SourceMessage<Out>) {
        if self.buffer.is_some() {
            panic!("Internal error: S3 buffer is not empty when asked to buffer message");
        }
        self.buffer = Some(message);
    }
}
