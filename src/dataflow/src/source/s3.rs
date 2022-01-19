// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functionality for creating S3 sources
//!
//! This source is constructed as a collection of Tokio tasks that communicate over local
//! (worker-pinned) queues to send data into dataflow. We spin up a single "downloader" task which
//! is responsible for performing s3 object downloads and shuffling the data into dataflow. Then,
//! for each object source, we spin up another task which is responsible for collecting object names
//! from an object name source and sending that name to the downloader.
//!
//! ```text
//! +----------------+
//! | bucket scanner +-                               -------
//! +----------------+ \---                         -/       \-
//! +----------------+     \--   +------------+    /           \
//! | sqs listener   +--------X->| downloader +--->| dataflow  |
//! +----------------+     /--   +------------+    \           /
//!        .  .  .  .   /--                         -\       /-
//!       etc .  .  . --                              -------
//!        .  .  .  .
//! ```

use std::collections::{HashMap, HashSet};
use std::convert::{From, TryInto};
use std::default::Default;
use std::fmt::Formatter;
use std::ops::AddAssign;

use async_compression::tokio::bufread::GzipDecoder;
use aws_sdk_s3::error::{GetObjectError, ListObjectsV2Error};
use aws_sdk_s3::{Client as S3Client, SdkError};
use aws_sdk_sqs::model::{ChangeMessageVisibilityBatchRequestEntry, Message as SqsMessage};
use aws_sdk_sqs::Client as SqsClient;
use futures::{FutureExt, StreamExt, TryStreamExt};
use globset::GlobMatcher;
use timely::scheduling::SyncActivator;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{self, Duration};
use tokio_util::io::{ReaderStream, StreamReader};

use dataflow_types::sources::{
    encoding::SourceDataEncoding, AwsConfig, Compression, ExternalSourceConnector, MzOffset,
    S3KeySource,
};
use expr::{PartitionId, SourceInstanceId};
use ore::retry::{Retry, RetryReader};
use repr::MessagePayload;

use crate::logging::materialized::Logger;
use crate::source::{NextMessage, SourceMessage, SourceReader};

use self::metrics::{BucketMetrics, ScanBucketMetrics};
use self::notifications::{Event, EventType, TestEvent};

use super::metrics::SourceBaseMetrics;

mod metrics;
mod notifications;

type Out = MessagePayload;
struct InternalMessage {
    record: Out,
}
/// Size of data chunks we send to dataflow
const CHUNK_SIZE: usize = 4096;

/// Information required to load data from S3
pub struct S3SourceReader {
    /// The name of the source that the user entered
    source_name: String,

    // differential control
    /// Unique source ID
    id: SourceInstanceId,
    /// Receiver channel that ingests records
    receiver_stream: Receiver<S3Result<InternalMessage>>,
    dataflow_status: tokio::sync::watch::Sender<DataflowStatus>,
    /// Total number of records that this source has read
    offset: S3Offset,
}

/// Current dataflow status
///
/// Used to signal the S3 and SQS services to shut down
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum DataflowStatus {
    Running,
    Stopped,
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
    mut rx: Receiver<S3Result<KeyInfo>>,
    tx: Sender<S3Result<InternalMessage>>,
    mut shutdown_rx: tokio::sync::watch::Receiver<DataflowStatus>,
    aws_config: AwsConfig,
    activator: SyncActivator,
    compression: Compression,
    metrics: SourceBaseMetrics,
) {
    let client = match aws_config
        .load()
        .await
        .and_then(|config| mz_aws_util::s3::client(&config))
    {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(S3Error::ClientConstructionFailed(e)))
                .await
                .unwrap_or_else(|e| {
                    tracing::debug!("unable to send error on stream creating s3 client: {}", e)
                });
            return;
        }
    };

    struct BucketInfo {
        keys: HashSet<String>,
        metrics: BucketMetrics,
    }
    let mut seen_buckets: HashMap<String, BucketInfo> = HashMap::new();

    loop {
        let msg = tokio::select! {
            msg = rx.recv() => {
                if let Some(msg) = msg {
                    msg
                } else {
                    break;
                }
            }
            status = shutdown_rx.changed() => {
                if status.is_ok() {
                    if let DataflowStatus::Stopped = *shutdown_rx.borrow() {
                        tracing::debug!("source_id={} download_objects received dataflow shutdown message", source_id);
                        break;
                    }
                }
                continue;
            }
        };

        match msg {
            Ok(msg) => {
                if let Some(bi) = seen_buckets.get_mut(&msg.bucket) {
                    if bi.keys.contains(&msg.key) {
                        bi.metrics.objects_duplicate.inc();
                        tracing::debug!(
                            "source_id={} skipping object because it was already seen: {}/{}",
                            source_id,
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

                let (tx, activator, client, msg_ref, sid) =
                    (&tx, &activator, &client, &msg, &source_id);

                let download_result = download_object(
                    tx,
                    &activator,
                    &client,
                    &msg_ref.bucket,
                    &msg_ref.key,
                    compression,
                    sid,
                )
                .await;

                // Extract and handle status updates
                match download_result {
                    Ok(update) => {
                        let bucket_info = seen_buckets.get_mut(&msg.bucket).expect("just inserted");
                        bucket_info.metrics.inc(1, update.bytes, update.messages);
                        tracing::debug!(
                            "source_id={} successfully downloaded {}/{}",
                            source_id,
                            msg.bucket,
                            msg.key
                        );
                        bucket_info.keys.insert(msg.key);
                    }
                    Err(DownloadError::Failed { err }) => {
                        if tx
                            .send(Err(S3Error::IoError {
                                bucket: msg_ref.bucket.clone(),
                                err,
                            }))
                            .await
                            .is_err()
                        {
                            rx.close();
                            break;
                        };
                    }
                    Err(DownloadError::SendFailed) => {
                        rx.close();
                        break;
                    }
                };
            }
            Err(e) => {
                if tx.send(Err(e)).await.is_err() {
                    rx.close();
                    break;
                }
            }
        }
    }
    tracing::debug!("source_id={} exiting download objects task", source_id);
}

async fn scan_bucket_task(
    bucket: String,
    source_id: String,
    glob: Option<GlobMatcher>,
    aws_config: AwsConfig,
    tx: Sender<S3Result<KeyInfo>>,
    base_metrics: SourceBaseMetrics,
) {
    let client = match aws_config
        .load()
        .await
        .and_then(|config| mz_aws_util::s3::client(&config))
    {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(S3Error::ClientConstructionFailed(e)))
                .await
                .unwrap_or_else(|e| {
                    tracing::debug!("unable to send error on stream creating s3 client: {}", e)
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
        tracing::debug!(
            "source_id={} downloading single object from s3 bucket={} key={}",
            source_id,
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
            tracing::debug!(
                "source_id={} Unable to send single key to downloader: {}",
                source_id,
                e
            );
        };

        return;
    } else {
        tracing::debug!(
            "source_id={} scanning bucket to find objects to download bucket={}",
            source_id,
            bucket
        );
    }

    let scan_metrics = ScanBucketMetrics::new(&base_metrics, &source_id, &bucket);

    let mut continuation_token = None;
    loop {
        let response = Retry::default()
            .retry_async(|_| {
                client
                    .list_objects_v2()
                    .bucket(&bucket)
                    .set_prefix(prefix.clone())
                    .set_continuation_token(continuation_token.clone())
                    .send()
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
                                tracing::debug!("unable to send keys to downloader: {}", e);
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
            Err(err) => {
                tx.send(Err(S3Error::ListObjectsFailed {
                    bucket: bucket.clone(),
                    err,
                }))
                .await
                .unwrap_or_else(|e| tracing::debug!("Source queue has been shut down: {}", e));

                break;
            }
        }
    }
    tracing::debug!(
        "source_id={} exiting bucket scan task bucket={}",
        source_id,
        bucket
    );
}

async fn read_sqs_task(
    source_id: String,
    glob: Option<GlobMatcher>,
    queue: String,
    aws_config: AwsConfig,
    tx: Sender<S3Result<KeyInfo>>,
    mut shutdown_rx: tokio::sync::watch::Receiver<DataflowStatus>,
    base_metrics: SourceBaseMetrics,
) {
    tracing::debug!(
        "source_id={} starting read sqs task queue={}",
        source_id,
        queue,
    );

    let client = match aws_config
        .load()
        .await
        .and_then(|config| mz_aws_util::sqs::client(&config))
    {
        Ok(client) => client,
        Err(e) => {
            tx.send(Err(S3Error::ClientConstructionFailed(e)))
                .await
                .unwrap_or_else(|e| {
                    tracing::debug!(
                        "source_id={} unable to send error on stream creating sqs client: {}",
                        source_id,
                        e
                    )
                });
            return;
        }
    };

    let glob = glob.as_ref();

    // TODO: accept a full url
    let queue_url = match client.get_queue_url().queue_name(&queue).send().await {
        Ok(response) => {
            if let Some(url) = response.queue_url {
                url
            } else {
                tracing::error!("Empty queue url response for queue {}", queue);
                return;
            }
        }
        Err(e) => {
            tracing::error!("Unable to retrieve queue url for queue {}: {}", queue, e);
            return;
        }
    };

    let mut metrics: HashMap<String, ScanBucketMetrics> = HashMap::new();

    let mut allowed_errors = 10;
    'outer: loop {
        let sqs_fut = client
            .receive_message()
            .max_number_of_messages(10)
            .queue_url(&queue_url)
            .visibility_timeout(500)
            // the maximum possible time for a long poll
            .wait_time_seconds(20)
            .send();
        let response = tokio::select! {
            response = sqs_fut => response,
            status = shutdown_rx.changed() => {
                if status.is_ok() {
                    if let DataflowStatus::Stopped = *shutdown_rx.borrow() {
                        tracing::debug!("source_id={} scan_sqs received dataflow shutdown message", source_id);
                        break;
                    }
                }
                continue;
            }
        };

        match response {
            Ok(response) => {
                let messages = if let Some(m) = response.messages {
                    if tx.is_closed() {
                        release_messages(
                            &client,
                            None,
                            m.into_iter(),
                            queue_url.clone(),
                            &source_id,
                            None,
                        )
                        .await;
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
                    let canceled = process_message(
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
                    if let Some((canceled_message, key)) = canceled {
                        release_messages(
                            &client,
                            Some(canceled_message),
                            msgs_iter,
                            queue_url.clone(),
                            &source_id,
                            Some(key),
                        )
                        .await;
                        break 'outer;
                    }
                }
            }

            Err(e) => {
                allowed_errors -= 1;
                if allowed_errors == 0 {
                    tracing::error!("failed to read from SQS queue {}: {}", queue, e);
                    break;
                } else {
                    tracing::warn!(
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
    tracing::debug!("source_id={} exiting sqs reader queue={}", source_id, queue);
}

/// Send the relevant parts of the message to the download objects task
///
/// Returns any message that wasn't able to be processed to be released back to
/// the SQS service, as well as the specific key that we failed to process from
/// that message.
async fn process_message(
    message: SqsMessage,
    glob: Option<&GlobMatcher>,
    base_metrics: SourceBaseMetrics,
    metrics: &mut HashMap<String, ScanBucketMetrics>,
    source_id: &str,
    tx: &Sender<S3Result<KeyInfo>>,
    client: &SqsClient,
    queue_url: &str,
) -> Option<(SqsMessage, String)> {
    if let Some(body) = message.body.as_ref() {
        let event: Result<Event, _> = serde_json::from_str(body);
        match event {
            Ok(event) => {
                if event.records.is_empty() {
                    tracing::debug!(
                        "source_id={} sqs event is surprisingly empty {:#?}",
                        source_id,
                        event
                    );
                }

                for record in event.records {
                    tracing::trace!(
                        "source_id={} processing message from sqs for key={} type={:?}",
                        source_id,
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
                                key: key.clone(),
                            });
                            if tx.send(ki).await.is_err() {
                                tracing::debug!(
                                    "source_id={} sqs reader is closed, marking message as visible",
                                    source_id
                                );
                                return Some((message, key));
                            }
                        }
                    }
                }
            }
            Err(_) => {
                let test: Result<TestEvent, _> = serde_json::from_str(&body);
                match test {
                    Ok(_) => {
                        tracing::trace!("got test event for new queue");
                    }
                    Err(_) => {
                        tracing::error!(
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
        .delete_message()
        .queue_url(queue_url)
        .receipt_handle(
            message
                .receipt_handle
                .expect("receipt handle is always returned"),
        )
        .send()
        .await
    {
        tracing::warn!(
            "source_id={} Error deleting processed SQS message: {}",
            source_id,
            e
        )
    }

    None
}

#[derive(Debug, Default)]
struct DownloadMetricUpdate {
    bytes: u64,
    messages: u64,
}

#[derive(Debug)]
enum DownloadError {
    Failed {
        err: std::io::Error,
    },
    /// Unable to send data to the `get_next_message` function, dataflow has shut down
    SendFailed,
}

#[derive(Debug)]
enum S3Error {
    ClientConstructionFailed(anyhow::Error),
    GetObjectError {
        bucket: String,
        key: String,
        err: SdkError<GetObjectError>,
    },
    ListObjectsFailed {
        bucket: String,
        err: SdkError<ListObjectsV2Error>,
    },
    IoError {
        bucket: String,
        err: std::io::Error,
    },
}

impl std::error::Error for S3Error {}

impl From<S3Error> for std::io::Error {
    fn from(err: S3Error) -> Self {
        Self::new(std::io::ErrorKind::Other, Box::new(err))
    }
}

impl std::fmt::Display for S3Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            S3Error::ClientConstructionFailed(err) => {
                write!(f, "Unable to build S3 client: {}", err)
            }
            S3Error::GetObjectError { bucket, key, err } => {
                write!(f, "Unable to get S3 object {}/{}: {}", bucket, key, err)
            }
            S3Error::ListObjectsFailed { bucket, err } => {
                write!(f, "Unable to list S3 bucket {}: {}", bucket, err)
            }
            S3Error::IoError { bucket, err } => {
                write!(f, "IO Error for S3 bucket {}: {}", bucket, err)
            }
        }
    }
}

type S3Result<R> = Result<R, S3Error>;

async fn download_object(
    tx: &Sender<S3Result<InternalMessage>>,
    activator: &SyncActivator,
    client: &S3Client,
    bucket: &str,
    key: &str,
    compression: Compression,
    source_id: &str,
) -> Result<DownloadMetricUpdate, DownloadError> {
    let retry_reader: RetryReader<_, _, _> = RetryReader::new(|state, offset| async move {
        let range = if offset == 0 {
            None
        } else {
            tracing::debug!(
                "Failed to download object: {}/{} attempt={} read={}",
                bucket,
                key,
                state.i,
                offset,
            );
            Some(format!("bytes={}-", offset))
        };

        let obj = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .set_range(range)
            .send()
            .await
            .or_else(|err| {
                Err(S3Error::GetObjectError {
                    bucket: bucket.to_string(),
                    key: key.to_string(),
                    err,
                })
            })?;

        // If the Content-Encoding does not match the compression specified for this
        // source, emit a debug message and trust the user-specified compression
        if let Some(s) = obj.content_encoding.as_deref() {
            match (s, compression) {
                ("gzip", Compression::Gzip) => (),
                ("identity", Compression::None) => (),
                ("identity" | "gzip", _) => {
                    tracing::debug!("object {} has mismatched Content-Encoding: {}", key, s)
                }
                _ => tracing::debug!("object {} has unrecognized Content-Encoding: {}", key, s),
            }
        }

        Ok(StreamReader::new(obj.body.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, e)
        })))
    });

    let mut reader = Box::pin(BufReader::new(retry_reader));

    // Check for empty files by filling up the buffer of bufreader and checking if it got any bytes
    match reader.fill_buf().await {
        Ok(buf) => {
            if buf.is_empty() {
                tracing::trace!("source_id={} empty object {}/{}", source_id, bucket, key);
                return Ok(Default::default());
            }
        }
        Err(err) => return Err(DownloadError::Failed { err }),
    };

    let mut download_result = match compression {
        Compression::None => read_object_chunked(source_id, reader, tx).await,
        Compression::Gzip => {
            let decoder = GzipDecoder::new(reader);
            read_object_chunked(source_id, decoder, tx).await
        }
    };

    tracing::debug!(
        "source_id={} {}/{} download_result={:?}",
        source_id,
        bucket,
        key,
        download_result,
    );

    if download_result.is_ok() {
        let sent = tx.send(Ok(InternalMessage {
            record: MessagePayload::EOF,
        }));
        if sent.await.is_err() {
            download_result = Err(DownloadError::SendFailed);
        }
    };
    activator.activate().expect("s3 reader activation failed");
    download_result
}

async fn read_object_chunked<R>(
    source_id: &str,
    reader: R,
    tx: &Sender<Result<InternalMessage, S3Error>>,
) -> Result<DownloadMetricUpdate, DownloadError>
where
    R: Unpin + AsyncRead,
{
    let (mut bytes_read, mut chunks) = (0, 0);

    let mut stream = ReaderStream::with_capacity(reader, CHUNK_SIZE);

    while let Some(result) = stream.next().await {
        match result {
            Ok(chunk) => {
                bytes_read += chunk.len();
                chunks += 1;
                if tx
                    .send(Ok(InternalMessage {
                        record: MessagePayload::Data(chunk.to_vec()),
                    }))
                    .await
                    .is_err()
                {
                    return Err(DownloadError::SendFailed);
                }
            }
            Err(err) => return Err(DownloadError::Failed { err }),
        }
    }

    tracing::trace!(
        "source_id={} finished sending object to dataflow chunks={} bytes={}",
        source_id,
        chunks,
        bytes_read
    );
    return Ok(DownloadMetricUpdate {
        bytes: bytes_read.try_into().expect("usize <= u64"),
        messages: chunks,
    });
}

impl SourceReader for S3SourceReader {
    type Key = ();
    type Value = MessagePayload;

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
        let (receiver, shutdowner) = {
            let (dataflow_tx, dataflow_rx) = tokio::sync::mpsc::channel(10_000);
            let (keys_tx, keys_rx) = tokio::sync::mpsc::channel(10_000);
            let (shutdowner, shutdown_rx) = tokio::sync::watch::channel(DataflowStatus::Running);
            let glob = s3_conn.pattern.map(|g| g.compile_matcher());

            tokio::spawn(download_objects_task(
                source_id.to_string(),
                keys_rx,
                dataflow_tx,
                shutdown_rx.clone(),
                s3_conn.aws.clone(),
                consumer_activator,
                s3_conn.compression,
                metrics.clone(),
            ));
            for key_source in s3_conn.key_sources {
                match key_source {
                    S3KeySource::Scan { bucket } => {
                        tracing::debug!(
                            "source_id={} reading s3 bucket={} worker={}",
                            source_id,
                            bucket,
                            worker_id
                        );
                        tokio::spawn(scan_bucket_task(
                            bucket,
                            source_id.to_string(),
                            glob.clone(),
                            s3_conn.aws.clone(),
                            keys_tx.clone(),
                            metrics.clone(),
                        ));
                    }
                    S3KeySource::SqsNotifications { queue } => {
                        tracing::debug!(
                            "source_id={} reading sqs queue={} worker={}",
                            source_id,
                            queue,
                            worker_id
                        );
                        tokio::spawn(read_sqs_task(
                            source_id.to_string(),
                            glob.clone(),
                            queue,
                            s3_conn.aws.clone(),
                            keys_tx.clone(),
                            shutdown_rx.clone(),
                            metrics.clone(),
                        ));
                    }
                }
            }
            (dataflow_rx, shutdowner)
        };

        Ok((
            S3SourceReader {
                source_name,
                id: source_id,
                receiver_stream: receiver,
                dataflow_status: shutdowner,
                offset: S3Offset(0),
            },
            Some(PartitionId::None),
        ))
    }

    fn get_next_message(&mut self) -> Result<NextMessage<Self::Key, Self::Value>, anyhow::Error> {
        match self.receiver_stream.recv().now_or_never() {
            Some(Some(Ok(InternalMessage { record }))) => {
                self.offset += 1;
                Ok(NextMessage::Ready(SourceMessage {
                    partition: PartitionId::None,
                    offset: self.offset.into(),
                    upstream_time_millis: None,
                    key: (),
                    value: record,
                }))
            }
            Some(Some(Err(e))) => match e {
                S3Error::GetObjectError { .. } => {
                    tracing::warn!(
                        "when reading source '{}' ({}): {}",
                        self.source_name,
                        self.id,
                        e
                    );
                    Ok(NextMessage::Pending)
                }
                e @ (S3Error::ListObjectsFailed { .. }
                | S3Error::ClientConstructionFailed(_)
                | S3Error::IoError { .. }) => Err(e.into()),
            },
            None => Ok(NextMessage::Pending),
            Some(None) => Ok(NextMessage::Finished),
        }
    }
}

impl Drop for S3SourceReader {
    fn drop(&mut self) {
        tracing::debug!("source_id={} Dropping S3SourceReader", self.id);
        if self.dataflow_status.send(DataflowStatus::Stopped).is_err() {
            tracing::debug!("source_id={} already shutdown", self.id);
        };
    }
}

// Helper utilities

/// Set the SQS visibility timeout back to zero, allowing the messages to be sent to other clients
async fn release_messages(
    client: &SqsClient,
    message: Option<SqsMessage>,
    messages: impl Iterator<Item = SqsMessage>,
    queue_url: String,
    source_id: &str,
    failed_key: Option<String>,
) {
    if let Err(e) = client
        .change_message_visibility_batch()
        .set_entries(Some(
            message
                .into_iter()
                .chain(messages.into_iter())
                .filter_map(|m| m.receipt_handle)
                .enumerate()
                .map(|(i, receipt_handle)| {
                    tracing::debug!(
                        "source_id={} releasing message unprocessed_key={}",
                        source_id,
                        failed_key.as_deref().unwrap_or("<none>")
                    );
                    ChangeMessageVisibilityBatchRequestEntry::builder()
                        .id(i.to_string())
                        .receipt_handle(receipt_handle)
                        .visibility_timeout(0)
                        .build()
                })
                .collect(),
        ))
        .queue_url(queue_url)
        .send()
        .await
    {
        tracing::warn!("unexpected error releasing SQS messages: {}", e);
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
