// Copyright Materialize, Inc. and contributors. All rights reserved.
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
use std::fmt::Formatter;
use std::io::Read;
use std::ops::AddAssign;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};

use anyhow::anyhow;
use flate2::read::MultiGzDecoder;
use globset::GlobMatcher;
use rusoto_core::RusotoError;
use rusoto_s3::{GetObjectRequest, ListObjectsV2Request, S3Client, S3};
use rusoto_sqs::{
    ChangeMessageVisibilityBatchRequest, ChangeMessageVisibilityBatchRequestEntry,
    DeleteMessageRequest, GetQueueUrlRequest, ReceiveMessageRequest, Sqs,
};
use timely::scheduling::SyncActivator;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc as tokio_mpsc;
use tokio::time::{self, Duration};

use aws_util::aws;
use dataflow_types::{
    Compression, ExternalSourceConnector, MzOffset, S3KeySource, SourceDataEncoding,
};
use expr::{PartitionId, SourceInstanceId};
use metrics::BucketMetrics;
use notifications::Event;
use repr::MessagePayload;

use crate::logging::materialized::Logger;
use crate::source::{NextMessage, SourceMessage, SourceReader};

use self::metrics::ScanBucketMetrics;
use self::notifications::{EventType, TestEvent};
use ore::retry::Retry;

use super::metrics::SourceBaseMetrics;

mod metrics;
mod notifications;

type Out = MessagePayload;
struct InternalMessage {
    record: Out,
}

/// Information required to load data from S3
pub struct S3SourceReader {
    /// The name of the source that the user entered
    source_name: String,

    // differential control
    /// Unique source ID
    id: SourceInstanceId,
    /// Receiver channel that ingests records
    receiver_stream: Receiver<S3Result<InternalMessage>>,
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

struct KeyInfo {
    bucket: String,
    key: String,
}

async fn download_objects_task(
    source_id: String,
    mut rx: tokio_mpsc::Receiver<S3Result<KeyInfo>>,
    tx: SyncSender<S3Result<InternalMessage>>,
    aws_info: aws::ConnectInfo,
    activator: SyncActivator,
    compression: Compression,
    metrics: SourceBaseMetrics,
) {
    let client = match aws_util::client::s3(aws_info) {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(S3Error::ClientConstructionFailed(e)))
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
                    if bi.keys.contains(&msg.key) {
                        bi.metrics.objects_duplicate.inc();
                        log::debug!(
                            "skipping object because it was already seen: {}/{}",
                            msg.bucket,
                            msg.key
                        );
                        continue;
                    }
                } else {
                    let bi = BucketInfo {
                        keys: HashSet::new(),
                        metrics: BucketMetrics::new(&metrics, &source_id, &msg.bucket),
                    };
                    seen_buckets.insert(msg.bucket.clone(), bi);
                };

                let (tx, activator, client, msg_ref) = (&tx, &activator, &client, &msg);

                let result = Retry::default()
                    .retry(|state| async move {
                        let (download_status, update) = download_object(
                            tx,
                            &activator,
                            &client,
                            &msg_ref.bucket,
                            &msg_ref.key,
                            compression,
                        )
                        .await;

                        match download_status {
                            // Exit retry loop
                            DownloadStatus::Ok => Ok((DownloadStatus::Ok, update)),
                            DownloadStatus::SendFailed => Ok((DownloadStatus::SendFailed, update)),
                            // Retriable error
                            DownloadStatus::Retry(e) => {
                                log::debug!(
                                    "Failed to download object: {}/{} (attempt {})",
                                    msg_ref.bucket,
                                    msg_ref.key,
                                    state.i
                                );
                                Err((DownloadStatus::Retry(e), update))
                            }
                        }
                    })
                    .await;
                // We use Result to communicate with Retry, both variants have the same data
                let (status, update) = match result {
                    Err((status, update)) | Ok((status, update)) => (status, update),
                };
                let bucket_info = seen_buckets.get_mut(&msg.bucket).expect("just inserted");
                if let Some(update) = update {
                    bucket_info.metrics.inc(1, update.bytes, update.messages);
                }
                // Extract and handle status updates
                match status {
                    // Retry making it out of the retry loop means retries failed
                    DownloadStatus::Retry(e) => {
                        if tx.send(Err(e)).is_err() {
                            rx.close();
                            break;
                        };
                    }
                    DownloadStatus::SendFailed => {
                        rx.close();
                        break;
                    }
                    DownloadStatus::Ok => {
                        log::debug!(
                            "source_id={} successfully downloaded {}/{}",
                            source_id,
                            msg.bucket,
                            msg.key
                        );
                        bucket_info.keys.insert(msg.key);
                    }
                };
            }
            Err(e) => {
                if tx.send(Err(e)).is_err() {
                    rx.close();
                    break;
                }
            }
        }
    }
    log::debug!("exiting download objects task source_id={}", source_id);
}

async fn scan_bucket_task(
    bucket: String,
    source_id: String,
    glob: Option<GlobMatcher>,
    aws_info: aws::ConnectInfo,
    tx: tokio_mpsc::Sender<S3Result<KeyInfo>>,
    base_metrics: SourceBaseMetrics,
) {
    let client = match aws_util::client::s3(aws_info) {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(S3Error::ClientConstructionFailed(e)))
                .await
                .unwrap_or_else(|e| {
                    log::debug!("unable to send error on stream creating s3 client: {}", e)
                });
            return;
        }
    };

    let glob = glob.as_ref();
    let prefix = glob.map(|g| find_prefix(g.glob().glob()));

    // for the special case of a single object in a matching clause, don't go through the ListObject
    // dance.
    //
    // This isn't a meaningful performance optimization, it just makes it easy for folks to import a
    // single object without grantin materialized the ListObjects IAM permission
    let is_literal_object = glob.is_some() && prefix.as_deref() == glob.map(|g| g.glob().glob());
    if is_literal_object {
        let key = glob.unwrap().glob().glob();
        log::debug!(
            "downloading single object from s3 bucket={} key={}",
            bucket,
            key
        );
        if let Err(e) = tx
            .send(Ok(KeyInfo {
                bucket,
                key: key.to_string(),
            }))
            .await
        {
            log::debug!("Unable to send single key to downloader: {}", e);
        };

        return;
    } else {
        log::debug!(
            "scanning bucket to find objects to download bucket={}",
            bucket
        );
    }

    let scan_metrics = ScanBucketMetrics::new(&base_metrics, &source_id, &bucket);

    let mut continuation_token = None;
    loop {
        let response = Retry::default()
            .retry(|_| {
                client.list_objects_v2(ListObjectsV2Request {
                    bucket: bucket.clone(),
                    prefix: prefix.clone(),
                    continuation_token: continuation_token.clone(),
                    ..Default::default()
                })
            })
            .await;

        match response {
            Ok(response) => {
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
                let err_string = format!("{}", e);
                tx.send(Err(S3Error::ListObjectsFailed(e)))
                    .await
                    .unwrap_or_else(|e| {
                        log::debug!("unable to send error on listing objects: {}", e)
                    });

                log::error!("failed to list bucket {}: {}", bucket, err_string);
                tx.send(Err(S3Error::RetryFailed))
                    .await
                    .unwrap_or_else(|e| {
                        log::debug!("unable to send error on retries failed: {}", e)
                    });
                break;
            }
        }
    }
    log::debug!(
        "exiting bucket scan task source_id={} bucket={}",
        source_id,
        bucket
    );
}

async fn read_sqs_task(
    source_id: String,
    glob: Option<GlobMatcher>,
    queue: String,
    aws_info: aws::ConnectInfo,
    tx: tokio_mpsc::Sender<S3Result<KeyInfo>>,
    base_metrics: SourceBaseMetrics,
) {
    log::debug!(
        "starting read sqs task queue={} source_id={}",
        queue,
        source_id
    );

    let client = match aws_util::client::sqs(aws_info) {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(S3Error::ClientConstructionFailed(e)))
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
    'outer: loop {
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
                    if tx.is_closed() {
                        release_messages(&client, None, m.into_iter(), queue_url.clone()).await;
                        break;
                    }

                    m
                } else {
                    if tx.is_closed() {
                        break;
                    }
                    continue;
                };
                allowed_errors = 10;

                let mut msgs_iter = messages.into_iter();
                while let Some(message) = msgs_iter.next() {
                    let cancelled_message = process_message(
                        message,
                        glob,
                        base_metrics.clone(),
                        &mut metrics,
                        &source_id,
                        &tx,
                        &client,
                        &queue_url,
                    )
                    .await;
                    if cancelled_message.is_some() {
                        release_messages(&client, cancelled_message, msgs_iter, queue_url.clone())
                            .await;
                        break 'outer;
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
    log::debug!("exiting sqs reader source_id={} queue={}", source_id, queue);
}

/// Send the relevant parts of the message to the download objects task
///
/// Returns any message that wasn't able to be processed to be released back to
/// the SQS service.
async fn process_message(
    message: rusoto_sqs::Message,
    glob: Option<&GlobMatcher>,
    base_metrics: SourceBaseMetrics,
    metrics: &mut HashMap<String, ScanBucketMetrics>,
    source_id: &str,
    tx: &tokio_mpsc::Sender<S3Result<KeyInfo>>,
    client: &rusoto_sqs::SqsClient,
    queue_url: &str,
) -> Option<rusoto_sqs::Message> {
    if let Some(body) = message.body.as_ref() {
        let event: Result<Event, _> = serde_json::from_str(body);
        match event {
            Ok(event) => {
                if event.records.is_empty() {
                    log::debug!("sqs event is surprisingly empty {:#?}", event);
                }

                for record in event.records {
                    log::trace!(
                        "processing message from sqs for key={} type={:?}",
                        record.s3.object.key,
                        record.event_type
                    );

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
                                    &base_metrics,
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
                                log::info!("sqs reader is closed, marking message as visible");
                                return Some(message);
                            }
                        }
                    }
                }
            }
            Err(_) => {
                let test: Result<TestEvent, _> = serde_json::from_str(&body);
                match test {
                    Ok(_) => {
                        log::trace!("got test event for new queue");
                    }
                    Err(_) => {
                        log::error!(
                            "[customer-data] Unrecognized message from SQS queue {}: {}",
                            queue_url,
                            body,
                        )
                    }
                }
            }
        }
    }

    if let Err(e) = client
        .delete_message(DeleteMessageRequest {
            queue_url: queue_url.to_string(),
            receipt_handle: message
                .receipt_handle
                .expect("receipt handle is always returned"),
        })
        .await
    {
        log::warn!("Error deleting processed SQS message: {}", e)
    }

    None
}

#[derive(Debug)]
struct DownloadMetricUpdate {
    bytes: u64,
    messages: u64,
}

#[derive(Debug)]
enum DownloadStatus {
    Ok,
    Retry(S3Error),
    SendFailed,
}

#[derive(Debug)]
enum S3Error {
    BodyMissing(String),
    ClientConstructionFailed(anyhow::Error),
    Decode(String, std::io::Error),
    GetObjectError {
        bucket: String,
        key: String,
        err: RusotoError<rusoto_s3::GetObjectError>,
    },
    ListObjectsFailed(RusotoError<rusoto_s3::ListObjectsV2Error>),
    Read(std::io::Error),
    RetryFailed,
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Error::BodyMissing(body) => write!(f, "Get object response had no body: {}", body),
            S3Error::ClientConstructionFailed(err) => err.fmt(f),
            S3Error::Decode(key, err) => {
                write!(f, "Failed to decode object {} using gzip: {}", key, err)
            }
            S3Error::GetObjectError { bucket, key, err } => {
                write!(f, "getting object {}/{}: {}", bucket, key, err)
            }
            S3Error::ListObjectsFailed(err) => err.fmt(f),
            S3Error::Read(err) => err.fmt(f),
            S3Error::RetryFailed => write!(f, "Retry failed to produce result"),
        }
    }
}

type S3Result<R> = Result<R, S3Error>;

async fn download_object(
    tx: &SyncSender<S3Result<InternalMessage>>,
    activator: &SyncActivator,
    client: &S3Client,
    bucket: &str,
    key: &str,
    compression: Compression,
) -> (DownloadStatus, Option<DownloadMetricUpdate>) {
    let obj = match client
        .get_object(GetObjectRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            ..Default::default()
        })
        .await
    {
        Ok(obj) => obj,
        Err(err) => {
            return (
                DownloadStatus::Retry(S3Error::GetObjectError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    err,
                }),
                None,
            );
        }
    };

    // If the Content-Type does not match the compression specified for this
    // source, emit a debug message and trust the user-specified compression
    if let Some(s) = obj.content_encoding.as_deref() {
        match (s, compression) {
            ("gzip", Compression::Gzip) => (),
            ("identity", Compression::None) => (),
            ("identity" | "gzip", _) => {
                log::debug!("object {} has mismatched Content-Type: {}", key, s)
            }
            _ => log::debug!("object {} has unrecognized Content-Type: {}", key, s),
        };
    };

    if let Some(body) = obj.body {
        let mut reader = body.into_async_read();
        // unwrap is safe because content length is not allowed to be negative
        let mut buf = Vec::with_capacity(obj.content_length.unwrap_or(1024).try_into().unwrap());

        match reader.read_to_end(&mut buf).await {
            Ok(_) => {
                if buf.is_empty() {
                    return (DownloadStatus::Ok, None);
                }

                let bytes_read = buf.len() as u64;

                let buf = match compression {
                    Compression::None => buf,
                    Compression::Gzip => {
                        let mut decoded = Vec::new();
                        let mut decoder = MultiGzDecoder::new(&*buf);
                        match decoder.read_to_end(&mut decoded) {
                            Ok(_) => {}
                            Err(e) => {
                                let metrics = DownloadMetricUpdate {
                                    bytes: bytes_read,
                                    messages: 0,
                                };
                                return (
                                    DownloadStatus::Retry(S3Error::Decode(key.into(), e)),
                                    Some(metrics),
                                );
                            }
                        }
                        decoded
                    }
                };

                let mut messages = 0;
                let mut chunk_idx = 0;
                const CHUNK_SIZE: usize = 4096;
                let mut download_status = DownloadStatus::Ok;
                while chunk_idx < buf.len() {
                    let chunk_bound = std::cmp::min(chunk_idx + CHUNK_SIZE, buf.len());
                    let chunk = (&buf[chunk_idx..chunk_bound]).to_vec();
                    if let Err(e) = tx.send(Ok(InternalMessage {
                        record: MessagePayload::Data(chunk),
                    })) {
                        log::debug!("unable to send error on stream: {}", e);
                        download_status = DownloadStatus::SendFailed;
                        break;
                    } else {
                        messages += 1;
                    }
                    chunk_idx = chunk_bound;
                }
                log::trace!("sent {} chunks to reader", messages);
                if !matches!(download_status, DownloadStatus::SendFailed) {
                    if let Err(e) = tx.send(Ok(InternalMessage {
                        record: MessagePayload::EOF,
                    })) {
                        log::debug!("unable to send EOF on stream: {}", e);
                        download_status = DownloadStatus::SendFailed;
                    }
                }
                activator.activate().expect("s3 reader activation failed");
                (
                    download_status,
                    Some(DownloadMetricUpdate {
                        bytes: bytes_read,
                        messages,
                    }),
                )
            }
            Err(e) => {
                return (DownloadStatus::Retry(S3Error::Read(e)), None);
            }
        }
    } else {
        return (
            DownloadStatus::Retry(S3Error::BodyMissing(key.into())),
            None,
        );
    }
}

impl SourceReader for S3SourceReader {
    fn new(
        source_name: String,
        source_id: SourceInstanceId,
        worker_id: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        _encoding: SourceDataEncoding,
        _: Option<Logger>,
        metrics: SourceBaseMetrics,
    ) -> Result<(S3SourceReader, Option<PartitionId>), anyhow::Error> {
        let s3_conn = match connector {
            ExternalSourceConnector::S3(s3_conn) => s3_conn,
            _ => {
                panic!("S3 is the only legitimate ExternalSourceConnector for S3SourceReader")
            }
        };

        // a single arbitrary worker is responsible for scanning the bucket
        let receiver = {
            let (dataflow_tx, dataflow_rx) = std::sync::mpsc::sync_channel(10_000);
            let (keys_tx, keys_rx) = tokio_mpsc::channel(10_000);
            let glob = s3_conn.pattern.map(|g| g.compile_matcher());
            let aws_info = s3_conn.aws_info;
            tokio::spawn(download_objects_task(
                source_id.to_string(),
                keys_rx,
                dataflow_tx,
                aws_info.clone(),
                consumer_activator,
                s3_conn.compression,
                metrics.clone(),
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
                            metrics.clone(),
                        ));
                    }
                    S3KeySource::SqsNotifications { queue } => {
                        log::debug!("reading sqs queue={} worker={}", queue, worker_id);
                        tokio::spawn(read_sqs_task(
                            source_id.to_string(),
                            glob.clone(),
                            queue,
                            aws_info.clone(),
                            keys_tx.clone(),
                            metrics.clone(),
                        ));
                    }
                }
            }
            dataflow_rx
        };

        Ok((
            S3SourceReader {
                source_name,
                id: source_id,
                receiver_stream: receiver,
                offset: S3Offset(0),
            },
            Some(PartitionId::None),
        ))
    }

    fn get_next_message(&mut self) -> Result<NextMessage, anyhow::Error> {
        match self.receiver_stream.try_recv() {
            Ok(Ok(InternalMessage { record })) => {
                self.offset += 1;
                Ok(NextMessage::Ready(SourceMessage {
                    partition: PartitionId::None,
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
                match e {
                    S3Error::ClientConstructionFailed(err) => {
                        Err(anyhow!("Client construction failed: {}", err))
                    }
                    S3Error::RetryFailed => Err(anyhow!("Retry failed")),
                    S3Error::BodyMissing(_)
                    | S3Error::Decode(_, _)
                    | S3Error::GetObjectError { .. }
                    | S3Error::ListObjectsFailed(_)
                    | S3Error::Read(_) => Ok(NextMessage::Pending),
                }
            }
            Err(TryRecvError::Empty) => Ok(NextMessage::Pending),
            Err(TryRecvError::Disconnected) => Ok(NextMessage::Finished),
        }
    }
}

// Helper utilities

/// Set the SQS visibility timeout back to zero, allowing the messages to be sent to other clients
async fn release_messages(
    client: &rusoto_sqs::SqsClient,
    message: Option<rusoto_sqs::Message>,
    messages: impl Iterator<Item = rusoto_sqs::Message>,
    queue_url: String,
) {
    if let Err(e) = client
        .change_message_visibility_batch(ChangeMessageVisibilityBatchRequest {
            entries: message
                .into_iter()
                .chain(messages.into_iter())
                .filter_map(|m| m.receipt_handle)
                .enumerate()
                .map(|(i, receipt_handle)| {
                    log::debug!(
                        "releasing message receipt_handle={} queue_url={}",
                        receipt_handle,
                        queue_url
                    );
                    ChangeMessageVisibilityBatchRequestEntry {
                        id: i.to_string(),
                        receipt_handle,
                        visibility_timeout: Some(0),
                    }
                })
                .collect(),
            queue_url,
        })
        .await
    {
        log::warn!("unexpected error releasing SQS messages: {}", e);
    };
}

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
