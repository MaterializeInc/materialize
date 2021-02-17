// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functionality for creating S3 sources

use std::collections::{HashMap, HashSet};
use std::convert::{From, TryInto};
use std::default::Default;
use std::ops::AddAssign;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};

use anyhow::{anyhow, Error};
use globset::GlobMatcher;
use metrics::BucketMetrics;
use notifications::Event;
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use rusoto_sqs::{DeleteMessageRequest, GetQueueUrlRequest, ReceiveMessageRequest, Sqs};
use timely::scheduling::{Activator, SyncActivator};
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::time::{self, Duration};

use aws_util::aws;
use dataflow_types::{DataEncoding, ExternalSourceConnector, MzOffset, S3KeySource};
use expr::{PartitionId, SourceInstanceId};

use crate::logging::materialized::Logger;
use crate::source::{
    ConsistencyInfo, NextMessage, PartitionMetrics, SourceConstructor, SourceInfo, SourceMessage,
};

use self::metrics::ScanBucketMetrics;
use self::notifications::{EventType, TestEvent};

mod metrics;
mod notifications;

type Out = Vec<u8>;
struct InternalMessage {
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
    /// Total number of records that this source has read
    offset: S3Offset,
}

/// Number of records This source has downloaded
///
/// Possibly this should be per-bucket or per-object, depending on the needs
/// for deterministic timestamping on restarts: issue #5715
#[derive(Clone, Copy, Debug)]
struct S3Offset(i64);

impl AddAssign<i64> for S3Offset {
    fn add_assign(&mut self, other: i64) {
        self.0 += other;
    }
}

impl From<S3Offset> for MzOffset {
    fn from(offset: S3Offset) -> MzOffset {
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
                source_name.clone(),
                source_id.to_string(),
                keys_rx,
                dataflow_tx,
                aws_info.clone(),
                consumer_activator,
            ));
            for key_source in s3_conn.key_sources {
                match key_source {
                    S3KeySource::Scan { bucket } => {
                        log::debug!("reading s3 bucket={} worker={}", bucket, worker_id);
                        tokio::spawn(scan_bucket_task(
                            bucket,
                            source_id.to_string(),
                            glob.clone(),
                            aws_info.clone(),
                            keys_tx.clone(),
                        ));
                    }
                    S3KeySource::SqsNotifications { queue } => {
                        tokio::spawn(read_sqs_task(
                            source_id.to_string(),
                            glob.clone(),
                            queue,
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

        let pid = PartitionId::S3;
        consistency_info.partition_metrics.insert(
            pid.clone(),
            PartitionMetrics::new(&source_name, source_id, "s3", logger),
        );
        consistency_info.update_partition_metadata(pid);

        Ok(S3SourceInfo {
            source_name,
            id: source_id,
            is_activated_reader: active,
            receiver_stream: receiver,
            buffer: None,
            offset: S3Offset(0),
        })
    }
}

struct KeyInfo {
    bucket: String,
    key: String,
}

async fn download_objects_task(
    source_name: String,
    source_id: String,
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

    struct BucketInfo {
        keys: HashSet<String>,
        metrics: BucketMetrics,
    }
    let mut seen_buckets: HashMap<String, BucketInfo> = HashMap::new();

    while let Some(msg) = rx.recv().await {
        match msg {
            Ok(msg) => {
                if let Some(bi) = seen_buckets.get_mut(&msg.bucket) {
                    let is_new = bi.keys.insert(msg.key.clone());
                    if !is_new {
                        bi.metrics.objects_duplicate.inc();
                        continue;
                    }
                } else {
                    let mut keys = HashSet::new();
                    keys.insert(msg.key.clone());
                    let bi = BucketInfo {
                        keys,
                        metrics: BucketMetrics::new(&source_id, &msg.bucket),
                    };
                    seen_buckets.insert(msg.bucket.clone(), bi);
                };

                let update = download_object(
                    &source_name,
                    &tx,
                    &activator,
                    &client,
                    msg.bucket.clone(),
                    msg.key,
                )
                .await;

                if let Some(update) = update {
                    seen_buckets
                        .get_mut(&msg.bucket)
                        .expect("just inserted")
                        .metrics
                        .inc(1, update.bytes, update.messages)
                }
            }
            Err(e) => tx
                .send(Err(e))
                .unwrap_or_else(|e| log::debug!("unable to send error: {}", e)),
        }
    }
}

async fn scan_bucket_task(
    bucket: String,
    source_id: String,
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

    let scan_metrics = ScanBucketMetrics::new(&source_id, &bucket);

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
                        let res = tx
                            .send(Ok(KeyInfo {
                                bucket: bucket.clone(),
                                key,
                            }))
                            .await;

                        match res {
                            Ok(_) => scan_metrics.objects_discovered.inc(),
                            Err(e) => {
                                log::debug!("unable to send keys to downloader: {}", e);
                                break;
                            }
                        }
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

async fn read_sqs_task(
    source_id: String,
    glob: Option<GlobMatcher>,
    queue: String,
    aws_info: aws::ConnectInfo,
    tx: tokio_mpsc::Sender<anyhow::Result<KeyInfo>>,
) {
    let client = match aws_util::sqs::client(aws_info).await {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(anyhow!("Unable to create sqs client: {}", e)))
                .await
                .unwrap_or_else(|e| {
                    log::debug!("unable to send error on stream creating sqs client: {}", e)
                });
            return;
        }
    };

    let glob = glob.as_ref();

    // TODO: accept a full url
    let queue_url = match client
        .get_queue_url(GetQueueUrlRequest {
            queue_name: queue.clone(),
            queue_owner_aws_account_id: None,
        })
        .await
    {
        Ok(response) => {
            if let Some(url) = response.queue_url {
                url
            } else {
                log::error!("Empty queue url response for queue {}", queue);
                return;
            }
        }
        Err(e) => {
            log::error!("Unable to retrieve queue url for queue {}: {}", queue, e);
            return;
        }
    };

    let mut metrics: HashMap<String, ScanBucketMetrics> = HashMap::new();

    let mut allowed_errors = 10;
    loop {
        let response = client
            .receive_message(ReceiveMessageRequest {
                max_number_of_messages: Some(10),
                queue_url: queue_url.clone(),
                visibility_timeout: Some(500),
                // the maximum possible time for a long poll
                wait_time_seconds: Some(20),
                ..Default::default()
            })
            .await;

        match response {
            Ok(response) => {
                let messages = if let Some(m) = response.messages {
                    m
                } else {
                    continue;
                };

                allowed_errors = 10;

                for message in messages {
                    if let Some(body) = message.body.as_ref() {
                        let event: Result<Event, _> = serde_json::from_str(body);
                        match event {
                            Ok(event) => {
                                for record in event.records {
                                    if matches!(
                                        record.event_type,
                                        EventType::ObjectCreatedPut
                                            | EventType::ObjectCreatedPost
                                            | EventType::ObjectCreatedCompleteMultipartUpload
                                    ) {
                                        let key = record.s3.object.key;
                                        if glob.map(|g| g.is_match(&key)).unwrap_or(true) {
                                            if let Some(m) = metrics.get(&record.s3.bucket.name) {
                                                m.objects_discovered.inc()
                                            } else {
                                                let m = ScanBucketMetrics::new(
                                                    &source_id,
                                                    &record.s3.bucket.name,
                                                );
                                                m.objects_discovered.inc();
                                                metrics.insert(record.s3.bucket.name.clone(), m);
                                            }

                                            let ki = Ok(KeyInfo {
                                                bucket: record.s3.bucket.name,
                                                key,
                                            });
                                            if tx.send(ki).await.is_err() {
                                                log::debug!(
                                                    "Downloader queue is closed, exiting sqs reader",
                                                );
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                let test: Result<TestEvent, _> = serde_json::from_str(&body);
                                match test {
                                    Ok(_) => {} // expected when connecting to a new queue
                                    Err(_) => {
                                        log::error!(
                                            "[customer-data] Unrecognized message from SQS queue {}: {}",
                                            queue, body,
                                        )
                                    }
                                }
                            }
                        }
                    }

                    if let Err(e) = client
                        .delete_message(DeleteMessageRequest {
                            queue_url: queue_url.clone(),
                            receipt_handle: message
                                .receipt_handle
                                .clone()
                                .expect("receipt handle is always returned"),
                        })
                        .await
                    {
                        log::warn!("Error deleting processed SQS message: {}", e)
                    }
                }
            }
            Err(e) => {
                allowed_errors -= 1;
                if allowed_errors == 0 {
                    log::error!("failed to read from SQS queue {}: {}", queue, e);
                    break;
                } else {
                    log::warn!(
                        "unable to read from SQS queue {}: {} ({} retries remaining)",
                        queue,
                        e,
                        allowed_errors
                    );
                }

                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

#[derive(Debug)]
struct DownloadMetricUpdate {
    bytes: u64,
    messages: u64,
}

async fn download_object(
    source_name: &str,
    tx: &SyncSender<anyhow::Result<InternalMessage>>,
    activator: &SyncActivator,
    client: &S3Client,
    bucket: String,
    key: String,
) -> Option<DownloadMetricUpdate> {
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
            return None;
        }
    };

    if let Some(body) = obj.body {
        let mut reader = body.into_async_read();
        // unwrap is safe because content length is not allowed to be negative
        let mut buf = Vec::with_capacity(obj.content_length.unwrap_or(1024).try_into().unwrap());
        let mut messages = 0;

        match reader.read_to_end(&mut buf).await {
            Ok(_) => {
                let activate = !buf.is_empty();
                for line in buf.split(|b| *b == b'\n').map(|s| s.to_vec()) {
                    if let Err(e) = tx.send(Ok(InternalMessage { record: line })) {
                        log::debug!(
                            "Source receiver has been closed, exiting ingest source={}: {}",
                            source_name,
                            e
                        );
                        break;
                    } else {
                        messages += 1;
                    }
                }
                log::trace!("sent {} messages to reader", messages);
                if activate {
                    activator.activate().expect("s3 reader activation failed");
                }
            }
            Err(e) => {
                tx.send(Err(anyhow!("Unable to read object: {}", e)))
                    .unwrap_or_else(|e| log::debug!("unable to send error on stream: {}", e));
            }
        }

        Some(DownloadMetricUpdate {
            bytes: buf.len() as u64,
            messages,
        })
    } else {
        log::warn!("get object response had no body");
        None
    }
}

impl SourceInfo<Vec<u8>> for S3SourceInfo {
    fn get_next_message(
        &mut self,
        _consistency_info: &mut ConsistencyInfo,
        _activator: &Activator,
    ) -> Result<NextMessage<Out>, anyhow::Error> {
        if let Some(message) = self.buffer.take() {
            return Ok(NextMessage::Ready(message));
        }
        match self.receiver_stream.try_recv() {
            Ok(Ok(InternalMessage { record })) => {
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
        _consistency_info: &mut ConsistencyInfo,
        _partition_count: i32,
    ) {
        // We can't do anything with just the number of "partitions" that we
        // know about, fundamentally we know more than the timestamper about
        // how many partitions there are.
        //
        // https://github.com/MaterializeInc/materialize/issues/5715
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
