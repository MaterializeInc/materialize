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
use std::fmt::{Display, Formatter};
use std::ops::AddAssign;
use std::sync::Arc;
use std::time::Instant;

use async_compression::tokio::bufread::GzipDecoder;
use aws_sdk_s3::error::{GetObjectError, ListObjectsV2Error};
use aws_sdk_s3::types::SdkError;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::model::{ChangeMessageVisibilityBatchRequestEntry, Message as SqsMessage};
use aws_sdk_sqs::Client as SqsClient;
use futures::lock::Mutex;
use futures::{FutureExt, StreamExt, TryStreamExt};
use globset::GlobMatcher;
use timely::scheduling::SyncActivator;
use tokio::io::{AsyncBufReadExt, AsyncRead, BufReader};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{self, Duration};
use tokio_util::io::{ReaderStream, StreamReader};

use mz_dataflow_types::sources::{
    encoding::SourceDataEncoding, AwsConfig, AwsExternalId, Compression, ExternalSourceConnector,
    MzOffset, S3KeySource,
};
use mz_expr::{PartitionId, SourceInstanceId};
use mz_ore::retry::{Retry, RetryReader};
use mz_ore::task;
use mz_repr::MessagePayload;
use tracing::{debug, error, trace, warn, Instrument};

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
    partition: PartitionId,
}
/// Size of data chunks we send to dataflow
const CHUNK_SIZE: usize = 16384;

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

#[derive(Debug, Clone)]
struct KeyInfo {
    bucket: String,
    key: String,
}

struct KeyDisplay<'a, 'b>(&'a str, &'b str);

impl<'a, 'b> Display for KeyDisplay<'a, 'b> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.0, self.1)
    }
}

#[derive(Clone)]
struct SeenObjects {
    /// Map from bucket name -> seen keys
    inner: Arc<Mutex<HashMap<String, BucketInfo>>>,
    metrics: SourceBaseMetrics,
    source_id: Arc<str>,
}

struct BucketInfo {
    keys: HashSet<String>,
    metrics: BucketMetrics,
}

impl SeenObjects {
    fn new(metrics: SourceBaseMetrics, source_id: String) -> SeenObjects {
        SeenObjects {
            inner: Default::default(),
            metrics,
            source_id: Arc::from(source_id),
        }
    }

    /// True if you have successfully taken responsibility for the key
    async fn take_key_ownership(&self, bucket: &str, key: &str) -> bool {
        let mut seen_objects = self.inner.lock().await;
        if let Some(bi) = seen_objects.get_mut(bucket) {
            if bi.keys.contains(key) {
                bi.metrics.objects_duplicate.inc();
                return false;
            }
        } else {
            let bi = BucketInfo {
                keys: HashSet::new(),
                metrics: BucketMetrics::new(&self.metrics, &self.source_id, &bucket),
            };
            seen_objects.insert(bucket.to_string(), bi);
        }
        return true;
    }

    async fn metrics_inc(
        &self,
        bucket: &str,
        new_key: String,
        objects: u64,
        bytes: u64,
        messages: u64,
    ) {
        let mut seen_buckets = self.inner.lock().await;
        let bi = seen_buckets.get_mut(bucket).expect("just inserted");
        bi.metrics.objects_downloaded.inc_by(objects);
        bi.metrics.bytes_downloaded.inc_by(bytes);
        bi.metrics.messages_ingested.inc_by(messages);
        bi.keys.insert(new_key);
    }
}

async fn download_objects_task(
    source_id: String,
    rx: async_channel::Receiver<S3Result<KeyInfo>>,
    tx: Sender<S3Result<InternalMessage>>,
    mut shutdown_rx: tokio::sync::watch::Receiver<DataflowStatus>,
    aws_config: AwsConfig,
    aws_external_id: AwsExternalId,
    activator: Arc<Mutex<SyncActivator>>,
    compression: Compression,
    seen_objects: SeenObjects,
) {
    let config = aws_config.load(aws_external_id).await;
    let client = mz_aws_util::s3::client(&config);

    loop {
        let msg = tokio::select! {
            msg = rx.recv() => {
                if let Ok(msg) = msg {
                    msg
                } else {
                    break;
                }
            }
            status = shutdown_rx.changed() => {
                if status.is_ok() {
                    if let DataflowStatus::Stopped = *shutdown_rx.borrow() {
                        debug!("download_objects received dataflow shutdown message");
                        break;
                    }
                }
                continue;
            }
        };

        match msg {
            Ok(msg) => {
                let (tx, activator, client, msg_ref, sid) =
                    (&tx, &activator, &client, &msg, &source_id);

                if !seen_objects.take_key_ownership(&msg.bucket, &msg.key).await {
                    continue;
                }

                let start = Instant::now();
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
                        debug!(
                            timing=?start.elapsed(),
                            "successfully downloaded {}/{}",
                            msg.bucket, msg.key
                        );
                        seen_objects
                            .metrics_inc(&msg.bucket, msg.key, 1, update.bytes, update.messages)
                            .await;
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
                            drop(rx);
                            break;
                        };
                    }
                    Err(DownloadError::SendFailed) => {
                        drop(rx);
                        break;
                    }
                };
            }
            Err(e) => {
                if tx.send(Err(e)).await.is_err() {
                    drop(rx);
                    break;
                }
            }
        }
    }
    debug!("exiting download objects task");
}

async fn scan_bucket_task(
    bucket: String,
    source_id: String,
    glob: Option<GlobMatcher>,
    aws_config: AwsConfig,
    aws_external_id: AwsExternalId,
    tx: async_channel::Sender<S3Result<KeyInfo>>,
    base_metrics: SourceBaseMetrics,
) {
    let config = aws_config.load(aws_external_id).await;
    let client = mz_aws_util::s3::client(&config);

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
        debug!(
            "downloading single object from s3 bucket={} key={}",
            bucket, key
        );
        if let Err(e) = tx
            .send(Ok(KeyInfo {
                bucket,
                key: key.to_string(),
            }))
            .await
        {
            debug!("Unable to send single key to downloader: {}", e);
        };

        return;
    } else {
        debug!(
            "scanning bucket to find objects to download bucket={}",
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
                                debug!("unable to send keys to downloader: {}", e);
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
                .unwrap_or_else(|e| debug!("Source queue has been shut down: {}", e));

                break;
            }
        }
    }
    debug!("exiting bucket scan task bucket={}", bucket);
}

async fn read_sqs_task(
    source_id: String,
    glob: Option<GlobMatcher>,
    queue: String,
    aws_config: AwsConfig,
    aws_external_id: AwsExternalId,
    tx: async_channel::Sender<S3Result<KeyInfo>>,
    mut shutdown_rx: tokio::sync::watch::Receiver<DataflowStatus>,
    base_metrics: SourceBaseMetrics,
) {
    debug!(
        %source_id, %queue,
        "starting read sqs task",
    );

    let config = aws_config.load(aws_external_id).await;
    let client = mz_aws_util::sqs::client(&config);

    let glob = glob.as_ref();

    // TODO: accept a full url
    let queue_url = match client.get_queue_url().queue_name(&queue).send().await {
        Ok(response) => {
            if let Some(url) = response.queue_url {
                url
            } else {
                error!("Empty queue url response for queue {}", queue);
                return;
            }
        }
        Err(e) => {
            error!("Unable to retrieve queue url for queue {}: {}", queue, e);
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
                        debug!("source_id={} scan_sqs received dataflow shutdown message", source_id);
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
                    error!("failed to read from SQS queue {}: {}", queue, e);
                    break;
                } else {
                    warn!(
                        "unable to read from SQS queue {}: {} ({} retries remaining)",
                        queue, e, allowed_errors
                    );
                }

                time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    debug!("source_id={} exiting sqs reader queue={}", source_id, queue);
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
    tx: &async_channel::Sender<S3Result<KeyInfo>>,
    client: &SqsClient,
    queue_url: &str,
) -> Option<(SqsMessage, String)> {
    if let Some(body) = message.body.as_ref() {
        let event: Result<Event, _> = serde_json::from_str(body);
        match event {
            Ok(event) => {
                if event.records.is_empty() {
                    debug!(
                        "source_id={} sqs event is surprisingly empty {:#?}",
                        source_id, event
                    );
                }

                for record in event.records {
                    trace!(
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
                                debug!(
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
                        trace!("got test event for new queue");
                    }
                    Err(_) => {
                        error!(
                            "[customer-data] Unrecognized message from SQS queue {}: {}",
                            queue_url, body,
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
        warn!(
            "source_id={} Error deleting processed SQS message: {}",
            source_id, e
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
    activator: &Arc<Mutex<SyncActivator>>,
    client: &S3Client,
    bucket: &str,
    key: &str,
    compression: Compression,
    source_id: &str,
) -> Result<DownloadMetricUpdate, DownloadError> {
    let partition = PartitionId::S3 {
        bucket: Arc::from(bucket),
        key: Arc::from(key),
    };
    let retry_reader: RetryReader<_, _, _> = RetryReader::new(|state, offset| async move {
        let range = if offset == 0 {
            None
        } else {
            debug!(
                "Failed to download object: {}/{} attempt={} read={}",
                bucket, key, state.i, offset,
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
                    debug!("object {} has mismatched Content-Encoding: {}", key, s)
                }
                _ => debug!("object {} has unrecognized Content-Encoding: {}", key, s),
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
                trace!("source_id={} empty object {}/{}", source_id, bucket, key);
                return Ok(Default::default());
            }
        }
        Err(err) => return Err(DownloadError::Failed { err }),
    };

    let mut download_result = match compression {
        Compression::None => read_object_chunked(source_id, reader, tx, partition.clone()).await,
        Compression::Gzip => {
            let decoder = GzipDecoder::new(reader);
            read_object_chunked(source_id, decoder, tx, partition.clone()).await
        }
    };

    debug!(
        object=%KeyDisplay(bucket, key),
        ?download_result,
        "completed object download"
    );

    if download_result.is_ok() {
        let sent = tx.send(Ok(InternalMessage {
            record: MessagePayload::EOF,
            partition: partition.clone(),
        }));
        if sent.await.is_err() {
            download_result = Err(DownloadError::SendFailed);
        }
    };
    let act = activator.lock().await;
    act.activate().expect("s3 reader activation failed");
    download_result
}

async fn read_object_chunked<R>(
    source_id: &str,
    reader: R,
    tx: &Sender<Result<InternalMessage, S3Error>>,
    partition_id: PartitionId,
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
                        // ReaderStream return's `None` if the underlying `AsyncRead`
                        // gives out 0 bytes, so the chunk is always !empty.
                        // See https://github.com/tokio-rs/tokio/blob/e8f19e771f501408427f7f9ee6ba4f54b2d4094c/tokio-util/src/io/reader_stream.rs#L102-L108
                        record: MessagePayload::Data(chunk.to_vec()),
                        partition: partition_id.clone(),
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

    trace!(
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
        _worker_count: usize,
        consumer_activator: SyncActivator,
        connector: ExternalSourceConnector,
        aws_external_id: AwsExternalId,
        _restored_offsets: Vec<(PartitionId, Option<MzOffset>)>,
        _encoding: SourceDataEncoding,
        _: Option<Logger>,
        metrics: SourceBaseMetrics,
    ) -> Result<Self, anyhow::Error> {
        let s3_conn = match connector {
            ExternalSourceConnector::S3(s3_conn) => s3_conn,
            _ => {
                panic!("S3 is the only legitimate ExternalSourceConnector for S3SourceReader")
            }
        };

        // a single arbitrary worker is responsible for scanning the bucket it spins up a bunch of
        // tokio tasks, which get data, which then send the data into the workflow which hashes to
        // workers
        let (receiver, shutdowner) = {
            let (dataflow_tx, dataflow_rx) = tokio::sync::mpsc::channel(10_000);
            let (keys_tx, keys_rx) = async_channel::unbounded();
            let (shutdowner, shutdown_rx) = tokio::sync::watch::channel(DataflowStatus::Running);
            let glob = s3_conn.pattern.map(|g| g.compile_matcher());
            let seen_objects = SeenObjects::new(metrics.clone(), source_id.to_string());
            let activator = Arc::from(Mutex::new(consumer_activator));
            let task_count = std::env::var("__MZ_S3_TASKS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .unwrap();
            debug!(%task_count, "spawning concurrent download tasks");

            for task_id in 0..task_count {
                let span = tracing::debug_span!("s3_download", %source_id, %task_id);
                task::spawn(
                    || format!("s3_download:{source_id}:{task_id}"),
                    download_objects_task(
                        source_id.to_string(),
                        keys_rx.clone(),
                        dataflow_tx.clone(),
                        shutdown_rx.clone(),
                        s3_conn.aws.clone(),
                        aws_external_id.clone(),
                        Arc::clone(&activator),
                        s3_conn.compression,
                        seen_objects.clone(),
                    )
                    .instrument(span),
                );
            }
            for key_source in s3_conn.key_sources {
                match key_source {
                    S3KeySource::Scan { bucket } => {
                        debug!(
                            "source_id={} reading s3 bucket={} worker={}",
                            source_id, bucket, worker_id
                        );
                        // TODO(guswynn): see if we can avoid this formatting
                        let task_name = format!("s3_scan:{}:{}", source_id, bucket);
                        task::spawn(
                            || task_name,
                            scan_bucket_task(
                                bucket,
                                source_id.to_string(),
                                glob.clone(),
                                s3_conn.aws.clone(),
                                aws_external_id.clone(),
                                keys_tx.clone(),
                                metrics.clone(),
                            ),
                        );
                    }
                    S3KeySource::SqsNotifications { queue } => {
                        debug!(
                            "source_id={} reading sqs queue={} worker={}",
                            source_id, queue, worker_id
                        );
                        task::spawn(
                            || format!("s3_read_sqs:{}", source_id),
                            read_sqs_task(
                                source_id.to_string(),
                                glob.clone(),
                                queue,
                                s3_conn.aws.clone(),
                                aws_external_id.clone(),
                                keys_tx.clone(),
                                shutdown_rx.clone(),
                                metrics.clone(),
                            ),
                        );
                    }
                }
            }
            (dataflow_rx, shutdowner)
        };

        Ok(S3SourceReader {
            source_name,
            id: source_id,
            receiver_stream: receiver,
            dataflow_status: shutdowner,
            offset: S3Offset(0),
        })
    }

    fn get_next_message(&mut self) -> Result<NextMessage<Self::Key, Self::Value>, anyhow::Error> {
        match self.receiver_stream.recv().now_or_never() {
            Some(Some(Ok(InternalMessage { record, partition }))) => {
                self.offset += 1;
                Ok(NextMessage::Ready(SourceMessage {
                    partition,
                    offset: self.offset.into(),
                    upstream_time_millis: None,
                    key: (),
                    value: record,
                }))
            }
            Some(Some(Err(e))) => match e {
                S3Error::GetObjectError { .. } => {
                    warn!(
                        "when reading source '{}' ({}): {}",
                        self.source_name, self.id, e
                    );
                    Ok(NextMessage::Pending)
                }
                e @ (S3Error::ListObjectsFailed { .. } | S3Error::IoError { .. }) => Err(e.into()),
            },
            None => Ok(NextMessage::Pending),
            Some(None) => Ok(NextMessage::Finished),
        }
    }
}

impl Drop for S3SourceReader {
    fn drop(&mut self) {
        debug!(source_id=%self.id, "Dropping S3SourceReader");
        if self.dataflow_status.send(DataflowStatus::Stopped).is_err() {
            debug!(source_id=%self.id, "already shutdown" );
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
                    debug!(
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
        warn!("unexpected error releasing SQS messages: {}", e);
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
