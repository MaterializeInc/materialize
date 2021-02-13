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
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use timely::scheduling::{Activator, SyncActivator};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::time::{self, Duration};

use aws_util::aws;
use dataflow_types::{Consistency, DataEncoding, ExternalSourceConnector, MzOffset, S3KeySource};
use expr::{PartitionId, SourceInstanceId};

use crate::logging::materialized::Logger;
use crate::server::{
    TimestampDataUpdate, TimestampDataUpdates, TimestampMetadataUpdate, TimestampMetadataUpdates,
};
use crate::source::{
    ConsistencyInfo, NextMessage, PartitionMetrics, SourceConstructor, SourceInfo, SourceMessage,
};

type Out = Vec<u8>;
struct InternalMessage {
    bucket: String,
    record: Out,
}

/// Information required to load data from S3
pub struct S3SourceInfo {
    /// The name of the source that the user entered
    source_name: String,

    // differential control
    /// Unique source ID
    id: SourceInstanceId,
    /// Field is set if this operator is responsible for ingesting data
    is_activated_reader: bool,
    /// Receiver channel that ingests records
    receiver_stream: Receiver<Result<InternalMessage, Error>>,
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
        _encoding: DataEncoding,
    ) -> Result<S3SourceInfo, anyhow::Error> {
        let s3_conn = match connector {
            ExternalSourceConnector::S3(s3_conn) => s3_conn,
            _ => {
                panic!("S3 is the only legitimate ExternalSourceConnector for S3SourceInfo")
            }
        };

        // a single arbitrary worker is responsible for scanning the bucket
        let receiver = if active {
            let (dataflow_tx, dataflow_rx) = std::sync::mpsc::sync_channel(10_000);
            let (keys_tx, keys_rx) = tokio_mpsc::channel(10_000);
            let glob = s3_conn.pattern.map(|g| g.compile_matcher());
            let aws_info = s3_conn.aws_info;
            tokio::spawn(download_objects_task(
                keys_rx,
                dataflow_tx,
                aws_info.clone(),
                consumer_activator,
            ));
            for key_source in s3_conn.key_sources {
                match key_source {
                    S3KeySource::Scan { bucket } => {
                        log::debug!("reading s3 bucket={} worker={}", bucket, worker_id);

                        let pid = PartitionId::S3 {
                            bucket: bucket.clone(),
                        };
                        consistency_info.partition_metrics.insert(
                            pid.clone(),
                            PartitionMetrics::new(&source_name, source_id, &bucket, logger.clone()),
                        );
                        consistency_info.update_partition_metadata(pid);

                        tokio::spawn(scan_bucket_task(
                            bucket,
                            glob.clone(),
                            aws_info.clone(),
                            keys_tx.clone(),
                        ));
                    }
                }
            }
            dataflow_rx
        } else {
            let (_tx, rx) = std::sync::mpsc::sync_channel(0);
            rx
        };

        Ok(S3SourceInfo {
            source_name,
            id: source_id,
            is_activated_reader: active,
            receiver_stream: receiver,
            buffer: None,
            offset: BucketOffset(0),
        })
    }
}

struct KeyInfo {
    bucket: String,
    key: String,
}

async fn download_objects_task(
    mut rx: tokio_mpsc::Receiver<anyhow::Result<KeyInfo>>,
    tx: SyncSender<anyhow::Result<InternalMessage>>,
    aws_info: aws::ConnectInfo,
    activator: SyncActivator,
) {
    let client = match aws_util::s3::client(aws_info).await {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(anyhow!("Unable to create s3 client: {}", e)))
                .unwrap_or_else(|e| {
                    log::debug!("unable to send error on stream creating s3 client: {}", e)
                });
            return;
        }
    };

    while let Some(record) = rx.recv().await {
        match record {
            Ok(record) => {
                download_object(&tx, &activator, &client, record.bucket, record.key).await
            }
            Err(e) => tx
                .send(Err(e))
                .unwrap_or_else(|e| log::debug!("unable to send error: {}", e)),
        }
    }
}

async fn scan_bucket_task(
    bucket: String,
    glob: Option<GlobMatcher>,
    aws_info: aws::ConnectInfo,
    tx: tokio_mpsc::Sender<anyhow::Result<KeyInfo>>,
) {
    let client = match aws_util::s3::client(aws_info).await {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(anyhow!("Unable to create s3 client: {}", e)))
                .await
                .unwrap_or_else(|e| {
                    log::debug!("unable to send error on stream creating s3 client: {}", e)
                });
            return;
        }
    };

    let glob = glob.as_ref();
    let prefix = glob.map(|g| find_prefix(g.glob().glob()));

    let mut continuation_token = None;
    let mut allowed_errors = 10;
    loop {
        let response = client
            .list_objects_v2(ListObjectsV2Request {
                bucket: bucket.clone(),
                prefix: prefix.clone(),
                continuation_token: continuation_token.clone(),
                ..Default::default()
            })
            .await;

        match response {
            Ok(response) => {
                allowed_errors = 10;

                if let Some(c) = response.contents {
                    let keys = c
                        .into_iter()
                        .filter_map(|obj| obj.key)
                        .filter(|k| glob.map(|g| g.is_match(k)).unwrap_or(true));

                    for key in keys {
                        tx.send(Ok(KeyInfo {
                            bucket: bucket.clone(),
                            key,
                        }))
                        .await
                        .unwrap_or_else(|e| {
                            log::debug!("unable to send keys to downloader: {}", e)
                        });
                    }
                }

                if response.next_continuation_token.is_none() {
                    break;
                }
                continuation_token = response.next_continuation_token;
            }
            Err(e) => {
                allowed_errors -= 1;
                if allowed_errors == 0 {
                    log::error!("failed to list bucket {}: {}", bucket, e);
                    break;
                } else {
                    log::warn!(
                        "unable to list bucket {}: {} ({} retries remaining)",
                        bucket,
                        e,
                        allowed_errors
                    );
                }

                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn download_object(
    tx: &SyncSender<anyhow::Result<InternalMessage>>,
    activator: &SyncActivator,
    client: &S3Client,
    bucket: String,
    key: String,
) {
    let obj = match client
        .get_object(GetObjectRequest {
            bucket: bucket.clone(),
            key,
            ..Default::default()
        })
        .await
    {
        Ok(obj) => obj,
        Err(e) => {
            tx.send(Err(anyhow!("Unable to GET object: {}", e)))
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
                    if let Err(e) = tx.send(Ok(InternalMessage {
                        bucket: bucket.clone(),
                        record: line,
                    })) {
                        log::debug!("unable to send read line on stream: {}", e);
                        break;
                    } else {
                        lines += 1;
                    }
                }
                log::trace!("sent {} lines to reader", lines);
                if activate {
                    activator.activate().expect("s3 reader activation failed");
                }
            }
            Err(e) => {
                tx.send(Err(anyhow!("Unable to read object: {}", e)))
                    .unwrap_or_else(|e| log::debug!("unable to send error on stream: {}", e));
            }
        }
    } else {
        log::warn!("get object response had no body");
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
            Ok(Ok(InternalMessage { bucket, record })) => {
                self.offset += 1;
                Ok(NextMessage::Ready(SourceMessage {
                    partition: PartitionId::S3 { bucket },
                    offset: self.offset.into(),
                    upstream_time_millis: None,
                    key: None,
                    payload: Some(record),
                }))
            }
            Ok(Err(e)) => {
                log::warn!(
                    "when reading source '{}' ({}): {}",
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

// Helper utilities

/// Find the unambiguous prefix of a glob
fn find_prefix(glob: &str) -> String {
    let mut escaped = false;
    let mut escaped_filter = false;
    glob.chars()
        .take_while(|c| match (c, &escaped) {
            ('*', false) => false,
            ('[', false) => false, // a character class is a form of glob
            ('{', false) => false, // a group class is a form of glob
            ('\\', false) => {
                escaped = true;
                true
            }
            (_, false) => true,
            (_, true) => {
                escaped = false;
                true
            }
        })
        .filter(|c| match (c, &escaped_filter) {
            (_, true) => {
                escaped_filter = false;
                true
            }
            ('\\', false) => {
                escaped_filter = true;
                false
            }
            (_, _) => true,
        })
        .collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn glob_prefix() {
        assert_eq!(&find_prefix("foo/**"), "foo/");
        assert_eq!(&find_prefix("foo/"), "foo/");
        assert_eq!(&find_prefix(""), "");
        assert_eq!(&find_prefix("**/*.json"), "");
        assert_eq!(&find_prefix(r"foo/\*/bar/*.json"), r"foo/*/bar/");
        assert_eq!(&find_prefix("foo/[*]/**"), "foo/");
        assert_eq!(&find_prefix("foo/{a,b}"), "foo/");
        assert_eq!(&find_prefix(r"class/\[*.json"), "class/[");
        assert_eq!(&find_prefix(r"class/\[ab]/**"), "class/[ab]/");
        assert_eq!(&find_prefix(r"alt/\{a,b}/**"), "alt/{a,b}/");
    }
}
