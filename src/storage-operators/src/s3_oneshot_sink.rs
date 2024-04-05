// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Uploads a consolidated collection to S3

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::str::FromStr;

use aws_types::sdk_config::SdkConfig;
use differential_dataflow::{Collection, Hashable};
use http::Uri;

use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_ore::task::JoinHandleExt;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{S3SinkFormat, S3UploadInfo};
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::{debug, info};

mod parquet;
mod pgcopy;

/// Copy the rows from the input collection to s3.
/// `worker_callback` is used to send the final count of rows uploaded to s3,
/// or an error message if the operator failed. This is per-worker, and
/// these responses are aggregated upstream by the compute client.
/// `sink_id` is used to identify the sink for logging purposes and as a
/// unique prefix for files created by the sink.
///
/// This renders 3 operators used to coordinate the upload:
///   - initialization: confirms the S3 path is empty and writes any sentinel files
///   - upload: uploads data to S3
///   - completion: removes the sentinel file and calls the `worker_callback`
pub fn copy_to<G, F>(
    input_collection: Collection<G, ((Row, u64), ()), Diff>,
    err_collection: Collection<G, ((DataflowError, u64), ()), Diff>,
    up_to: Antichain<G::Timestamp>,
    connection_details: S3UploadInfo,
    connection_context: ConnectionContext,
    aws_connection: AwsConnection,
    sink_id: GlobalId,
    connection_id: GlobalId,
    worker_callback: F,
) where
    G: Scope<Timestamp = Timestamp>,
    F: FnOnce(Result<u64, String>) -> () + 'static,
{
    let scope = input_collection.scope();

    let s3_key_manager = S3KeyManager::new(&sink_id, &connection_details.uri);

    let start_stream = render_initialization_operator(
        scope.clone(),
        connection_context.clone(),
        aws_connection.clone(),
        connection_id,
        sink_id,
        s3_key_manager.clone(),
    );

    let completion_stream = match connection_details.format {
        S3SinkFormat::PgCopy(_) => render_upload_operator::<G, pgcopy::PgCopyUploader>(
            scope.clone(),
            connection_context.clone(),
            aws_connection.clone(),
            connection_id,
            connection_details,
            sink_id,
            input_collection,
            err_collection,
            up_to,
            start_stream,
        ),
        S3SinkFormat::Parquet => render_upload_operator::<G, parquet::ParquetUploader>(
            scope.clone(),
            connection_context.clone(),
            aws_connection.clone(),
            connection_id,
            connection_details,
            sink_id,
            input_collection,
            err_collection,
            up_to,
            start_stream,
        ),
    };

    render_completion_operator(
        scope,
        connection_context,
        aws_connection,
        connection_id,
        sink_id,
        s3_key_manager,
        completion_stream,
        worker_callback,
    );
}

/// Renders the 'initialization' operator, which does work on the leader worker only.
/// The leader worker checks the S3 path for the sink to ensure it's empty
/// (aside from files written by other instances of this sink), and writes an INCOMPLETE
/// sentinel file to indicate to the user that the upload is in-progress.
///
/// The INCOMPLETE sentinel is used to provide a single atomic operation that a user
/// can wire up a notification on, to know when it is safe to start ingesting the
/// data written by this sink to S3. Since the DeleteObject of the INCOMPLETE sentinel
/// will only trigger one S3 notification, even if it's performed by multiple replicas
/// it simplifies the user ergonomics by only having to listen for a single event
/// (a PutObject sentinel would trigger once for each replica).
///
/// Returns a stream with a result object indicating the success or failure of the
/// initialization operation.
fn render_initialization_operator<G>(
    scope: G,
    connection_context: ConnectionContext,
    aws_connection: AwsConnection,
    connection_id: GlobalId,
    sink_id: GlobalId,
    s3_key_manager: S3KeyManager,
) -> Stream<G, Result<(), String>>
where
    G: Scope<Timestamp = Timestamp>,
{
    let worker_id = scope.index();
    let num_workers = scope.peers();
    let leader_id = usize::cast_from((sink_id, "initialization").hashed()) % num_workers;
    let is_leader = worker_id == leader_id;

    let mut builder =
        AsyncOperatorBuilder::new("CopyToS3-initialization".to_string(), scope.clone());

    let (mut start_handle, start_stream) = builder.new_output();

    builder.build(move |caps| async move {
        let [start_cap] = caps.try_into().unwrap();

        if !is_leader {
            return;
        }

        // fallible async block to use the `?` operator for convenience
        let leader_work = async move {
            info!(%sink_id, %worker_id, "s3 leader worker initialization");
            let sdk_config = aws_connection
                .load_sdk_config(&connection_context, connection_id, InTask::Yes)
                .await?;

            let client = mz_aws_util::s3::new_client(&sdk_config);
            let bucket = s3_key_manager.bucket.clone();
            let path_prefix = s3_key_manager.path_prefix().to_string();
            let incomplete_sentinel_key = s3_key_manager.incomplete_sentinel_key();

            // Check that the S3 bucket path is empty before beginning the upload,
            // and upload the INCOMPLETE sentinel file to the S3 path.
            // Since we race against other replicas running the same sink we allow
            // for objects to exist in the path if they were created by this sink
            // (identified by the sink_id prefix).
            // TODO: Add logic to determine if other replicas have already completed
            // a full upload, to avoid writing the incomplete sentinel file again
            // if the upload is already done.
            mz_ore::task::spawn(|| "copytos3:initialization", async move {
                if let Some(files) =
                    mz_aws_util::s3::list_bucket_path(&client, &bucket, &path_prefix).await?
                {
                    let files = files
                        .iter()
                        .filter(|key| !s3_key_manager.is_sink_object(key))
                        .collect::<Vec<_>>();
                    if !files.is_empty() {
                        Err(anyhow::anyhow!(
                            "S3 bucket path is not empty, contains {} objects",
                            files.len()
                        ))?;
                    }
                };

                debug!(%sink_id, %worker_id, "uploading INCOMPLETE sentinel file");
                client
                    .put_object()
                    .bucket(bucket)
                    .key(incomplete_sentinel_key)
                    .send()
                    .await?;

                Ok::<(), anyhow::Error>(())
            })
            .wait_and_assert_finished()
            .await?;
            Ok::<(), anyhow::Error>(())
        };

        let res = leader_work.await.map_err(|e| e.to_string());
        start_handle.give(&start_cap, res).await;
    });

    start_stream
}

/// Renders the 'completion' operator, which expects a `completion_stream`
/// that it reads over a Pipeline edge such that it receives a single
/// completion event per worker. Then forwards this result to the
/// `worker_callback` after any cleanup work (see below).
///
/// On the leader worker, this operator waits to see the empty frontier for
/// the completion_stream and then does some cleanup work before calling
/// the callback.
///
/// This cleanup work removes the INCOMPLETE sentinel file (see description
/// of `render_initialization_operator` for more details).
fn render_completion_operator<G, F>(
    scope: G,
    connection_context: ConnectionContext,
    aws_connection: AwsConnection,
    connection_id: GlobalId,
    sink_id: GlobalId,
    s3_key_manager: S3KeyManager,
    completion_stream: Stream<G, Result<u64, String>>,
    worker_callback: F,
) where
    G: Scope<Timestamp = Timestamp>,
    F: FnOnce(Result<u64, String>) -> () + 'static,
{
    let worker_id = scope.index();
    let num_workers = scope.peers();
    let leader_id = usize::cast_from((sink_id, "completion").hashed()) % num_workers;
    let is_leader = worker_id == leader_id;

    let mut builder = AsyncOperatorBuilder::new("CopyToS3-completion".to_string(), scope.clone());

    let mut completion_input = builder.new_disconnected_input(&completion_stream, Pipeline);

    builder.build(move |_| async move {
        // fallible async block to use the `?` operator for convenience
        let fallible_logic = async move {
            let mut row_count = None;
            while let Some(event) = completion_input.next().await {
                if let AsyncEvent::Data(_ts, data) = event {
                    for result in data {
                        assert!(
                            row_count.is_none(),
                            "unexpectedly received more than 1 event on the completion stream!"
                        );
                        row_count = Some(result.map_err(|e| anyhow!(e))?);
                    }
                }
            }
            let row_count = row_count.expect("did not receive completion event");

            if is_leader {
                debug!(%sink_id, %worker_id, "s3 leader worker completion");
                let sdk_config = aws_connection
                    .load_sdk_config(&connection_context, connection_id, InTask::Yes)
                    .await?;

                let client = mz_aws_util::s3::new_client(&sdk_config);
                let bucket = s3_key_manager.bucket.clone();
                let incomplete_sentinel_key = s3_key_manager.incomplete_sentinel_key();

                // Remove the INCOMPLETE sentinel file to indicate that the upload is complete.
                // This will race against other replicas who are completing the same uploads,
                // such that the first replica to complete its uploads will delete the sentinel
                // and the subsequent replicas shouldn't error if the object is already deleted.
                // TODO: Should we also write a manifest of all the files uploaded?
                mz_ore::task::spawn(|| "copytos3:completion", async move {
                    debug!(%sink_id, %worker_id, "removing INCOMPLETE sentinel file");
                    client
                        .delete_object()
                        .bucket(bucket)
                        .key(incomplete_sentinel_key)
                        .send()
                        .await?;
                    Ok::<(), anyhow::Error>(())
                })
                .wait_and_assert_finished()
                .await?;
            }
            Ok::<u64, anyhow::Error>(row_count)
        };

        worker_callback(fallible_logic.await.map_err(|e| e.to_string()));
    });
}

/// Renders the `upload operator`, which waits on the `start_stream` to ensure
/// initialization is complete and then handles the uploads to S3.
/// Returns a `completion_stream` which contains 1 event per worker of
/// the result of the upload operation, either an error or the number of rows
/// uploaded by the worker.
fn render_upload_operator<G, T>(
    scope: G,
    connection_context: ConnectionContext,
    aws_connection: AwsConnection,
    connection_id: GlobalId,
    connection_details: S3UploadInfo,
    sink_id: GlobalId,
    input_collection: Collection<G, ((Row, u64), ()), Diff>,
    err_collection: Collection<G, ((DataflowError, u64), ()), Diff>,
    up_to: Antichain<G::Timestamp>,
    start_stream: Stream<G, Result<(), String>>,
) -> Stream<G, Result<u64, String>>
where
    G: Scope<Timestamp = Timestamp>,
    T: CopyToS3Uploader,
{
    let worker_id = scope.index();
    let mut builder = AsyncOperatorBuilder::new("CopyToS3-uploader".to_string(), scope.clone());

    let mut input_handle = builder.new_disconnected_input(&input_collection.inner, Pipeline);
    let mut error_handle = builder.new_disconnected_input(&err_collection.inner, Pipeline);

    let (mut completion, completion_stream) = builder.new_output();
    let mut start_handle = builder.new_input_for(&start_stream, Pipeline, &completion);

    builder.build(move |caps| async move {
        let [completion_cap] = caps.try_into().unwrap();

        // Drain all errors from the error stream, exiting if we encounter one
        while let Some(event) = error_handle.next().await {
            match event {
                AsyncEvent::Data(_ts, data) => {
                    for (((error, _), _), ts, _) in data {
                        if !up_to.less_equal(&ts) {
                            completion
                                .give(&completion_cap, Err(error.to_string()))
                                .await;
                            return;
                        }
                    }
                }
                AsyncEvent::Progress(frontier) => {
                    if PartialOrder::less_equal(&up_to, &frontier) {
                        // No error, break from loop and proceed
                        break;
                    }
                }
            }
        }

        // Drain any events in the start stream, which will advance to the empty frontier after
        // initialization is complete, and may contain a result from the leader worker.
        // NOTE: This is being refactored in https://github.com/MaterializeInc/materialize/pull/26489/
        while let Some(event) = start_handle.next().await {
            match event {
                AsyncEvent::Data(cap, data) => {
                    for res in data {
                        if res.is_err() {
                            completion.give(&cap, res.map(|_| 0)).await;
                            return;
                        }
                    }
                }
                AsyncEvent::Progress(_) => {}
            }
        }

        // fallible async block to use the `?` operator for convenience
        let res = async move {
            let sdk_config = aws_connection
                .load_sdk_config(&connection_context, connection_id, InTask::Yes)
                .await?;

            // Map of an uploader per batch.
            let mut s3_uploaders: BTreeMap<u64, T> = BTreeMap::new();
            let mut row_count = 0;
            while let Some(event) = input_handle.next().await {
                match event {
                    AsyncEvent::Data(_ts, data) => {
                        for (((row, batch), ()), ts, diff) in data {
                            if !up_to.less_equal(&ts) {
                                if diff < 0 {
                                    anyhow::bail!(
                                        "Invalid data in source errors, saw retractions ({}) for \
                                        row that does not exist",
                                        diff * -1,
                                    )
                                }
                                row_count += u64::try_from(diff).unwrap();
                                let uploader = match s3_uploaders.entry(batch) {
                                    Entry::Occupied(entry) => entry.into_mut(),
                                    Entry::Vacant(entry) => {
                                        debug!(%sink_id, %worker_id, "handling batch: {}", batch);
                                        entry.insert(T::new(
                                            sdk_config.clone(),
                                            connection_details.clone(),
                                            &sink_id,
                                            batch,
                                        )?)
                                    }
                                };
                                for _ in 0..diff {
                                    uploader.append_row(&row).await?;
                                }
                            }
                        }
                    }
                    AsyncEvent::Progress(frontier) => {
                        if PartialOrder::less_equal(&up_to, &frontier) {
                            for uploader in s3_uploaders.values_mut() {
                                uploader.flush().await?;
                            }
                            // We are done, send the final count.
                            return Ok(row_count);
                        }
                    }
                }
            }
            Ok::<u64, anyhow::Error>(row_count)
        }
        .await;

        completion
            .give(&completion_cap, res.map_err(|e| e.to_string()))
            .await;
    });

    completion_stream
}

/// Helper to manage object keys created by this sink based on the S3 URI provided
/// by the user and the GlobalId that identifies this copy-to-s3 sink.
/// Since there may be multiple compute replicas running their own copy of this sink
/// we need to ensure the S3 keys are consistent such that we can detect when objects
/// were created by an instance of this sink or not.
#[derive(Clone)]
struct S3KeyManager {
    pub bucket: String,
    object_key_prefix: String,
}

impl S3KeyManager {
    pub fn new(sink_id: &GlobalId, s3_uri: &str) -> Self {
        // This url is already validated to be a valid s3 url in sequencer.
        let uri = Uri::from_str(s3_uri).expect("valid s3 url");
        let bucket = uri.host().expect("s3 bucket");
        // TODO: Can an empty path be provided?
        let path = uri.path().trim_start_matches('/').trim_end_matches('/');

        Self {
            bucket: bucket.to_string(),
            object_key_prefix: format!("{}/mz-{}-", path, sink_id),
        }
    }

    /// The S3 key to use for a specific data file, based on the batch
    /// it belongs to and the index within that batch.
    fn data_key(&self, batch: u64, file_index: usize, extension: &str) -> String {
        format!(
            "{}batch-{:04}-{:04}.{}",
            self.object_key_prefix, batch, file_index, extension
        )
    }

    /// The S3 key to use for the incomplete sentinel file
    fn incomplete_sentinel_key(&self) -> String {
        format!("{}INCOMPLETE", self.object_key_prefix)
    }

    /// Whether the given object key belongs to this sink instance
    fn is_sink_object(&self, object_key: &str) -> bool {
        object_key.starts_with(&self.object_key_prefix)
    }

    /// The key prefix based on the URI provided by the user. NOTE this doesn't
    /// contain the additional prefix we include on all keys written by the sink
    /// e.g. `mz-{sink_id}-batch-...`
    /// This is useful when listing objects in the bucket with this prefix to
    /// determine if its clear to upload.
    fn path_prefix(&self) -> &str {
        self.object_key_prefix.rsplit_once('/').expect("exists").0
    }
}

/// This trait is used to abstract over the upload details for different file formats.
/// Each format has its own buffering semantics and upload logic, since some can be
/// written in a streaming fashion row-by-row, whereas others use a columnar-based
/// format that requires buffering a batch of rows before writing to S3.
trait CopyToS3Uploader: Sized {
    fn new(
        sdk_config: SdkConfig,
        connection_details: S3UploadInfo,
        sink_id: &GlobalId,
        batch: u64,
    ) -> Result<Self, anyhow::Error>;
    /// Append a row to the internal buffer, and optionally flush the buffer to S3.
    async fn append_row(&mut self, row: &Row) -> Result<(), anyhow::Error>;
    /// Flush the full remaining internal buffer to S3, and close all open resources.
    /// This will be called when the input stream is finished.
    async fn flush(&mut self) -> Result<(), anyhow::Error>;
}
