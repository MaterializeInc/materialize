// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Uploads a consolidated collection to S3

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::rc::Rc;

use anyhow::anyhow;
use aws_types::sdk_config::SdkConfig;
use differential_dataflow::Hashable;
use differential_dataflow::containers::TimelyStack;
use futures::StreamExt;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::future::InTask;
use mz_ore::task::JoinHandleExt;
use mz_repr::{CatalogItemId, Diff, GlobalId, Row, Timestamp};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::s3_oneshot_sink::S3KeyManager;
use mz_storage_types::sinks::{S3SinkFormat, S3UploadInfo};
use mz_timely_util::builder_async::{
    Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder, PressOnDropButton,
};
use timely::PartialOrder;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::Broadcast;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tracing::debug;

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
///
/// Returns a token that should be held to keep the sink alive.
///
/// The `input_collection` must be a stream of chains, partitioned and exchanged by the row's hash
/// modulo the number of batches.
pub fn copy_to<G, F>(
    input_collection: Stream<G, Vec<TimelyStack<((Row, ()), G::Timestamp, Diff)>>>,
    err_stream: Stream<G, (DataflowError, G::Timestamp, Diff)>,
    up_to: Antichain<G::Timestamp>,
    connection_details: S3UploadInfo,
    connection_context: ConnectionContext,
    aws_connection: AwsConnection,
    sink_id: GlobalId,
    connection_id: CatalogItemId,
    params: CopyToParameters,
    worker_callback: F,
    output_batch_count: u64,
) -> Rc<dyn Any>
where
    G: Scope<Timestamp = Timestamp>,
    F: FnOnce(Result<u64, String>) -> () + 'static,
{
    let scope = input_collection.scope();

    let s3_key_manager = S3KeyManager::new(&sink_id, &connection_details.uri);

    let (start_stream, start_token) =
        render_initialization_operator(scope.clone(), sink_id, up_to.clone(), err_stream);

    let (completion_stream, upload_token) = match connection_details.format {
        S3SinkFormat::PgCopy(_) => render_upload_operator::<G, pgcopy::PgCopyUploader>(
            scope.clone(),
            connection_context.clone(),
            aws_connection.clone(),
            connection_id,
            connection_details,
            sink_id,
            input_collection,
            up_to,
            start_stream,
            params,
            output_batch_count,
        ),
        S3SinkFormat::Parquet => render_upload_operator::<G, parquet::ParquetUploader>(
            scope.clone(),
            connection_context.clone(),
            aws_connection.clone(),
            connection_id,
            connection_details,
            sink_id,
            input_collection,
            up_to,
            start_stream,
            params,
            output_batch_count,
        ),
    };

    let completion_token = render_completion_operator(
        scope,
        connection_context,
        aws_connection,
        connection_id,
        sink_id,
        s3_key_manager,
        completion_stream,
        worker_callback,
    );

    Rc::new(vec![start_token, upload_token, completion_token])
}

/// Renders the 'initialization' operator, which does work on the leader worker only.
///
/// The leader worker receives all errors from the `err_stream` and if it
/// encounters any errors will early exit and broadcast the error on the
/// returned `start_stream`, to avoid any unnecessary work across all workers.
///
/// Returns the `start_stream` with an error received in the `err_stream`, if
/// any, otherwise `Ok(())`.
fn render_initialization_operator<G>(
    scope: G,
    sink_id: GlobalId,
    up_to: Antichain<G::Timestamp>,
    err_stream: Stream<G, (DataflowError, G::Timestamp, Diff)>,
) -> (Stream<G, Result<(), String>>, PressOnDropButton)
where
    G: Scope<Timestamp = Timestamp>,
{
    let worker_id = scope.index();
    let num_workers = scope.peers();
    let leader_id = usize::cast_from((sink_id, "initialization").hashed()) % num_workers;
    let is_leader = worker_id == leader_id;

    let mut builder =
        AsyncOperatorBuilder::new("CopyToS3-initialization".to_string(), scope.clone());

    let (start_handle, start_stream) = builder.new_output::<CapacityContainerBuilder<_>>();

    // Push all errors to the leader worker, so it early exits before doing any initialization work
    // This should be at-most 1 incoming error per-worker due to the filtering of this stream
    // in `CopyToS3OneshotSinkConnection::render_continuous_sink`.
    let mut error_handle = builder.new_input_for(
        &err_stream,
        Exchange::new(move |_| u64::cast_from(leader_id)),
        &start_handle,
    );

    let button = builder.build(move |caps| async move {
        let [start_cap] = caps.try_into().unwrap();

        while let Some(event) = error_handle.next().await {
            match event {
                AsyncEvent::Data(cap, data) => {
                    for (error, ts, _) in data {
                        if !up_to.less_equal(&ts) {
                            start_handle.give(&cap, Err(error.to_string()));
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

        if !is_leader {
            return;
        }

        start_handle.give(&start_cap, Ok(()));
    });

    // Broadcast the result to all workers so that they will all see any error that occurs
    // and exit before doing any unnecessary work
    (start_stream.broadcast(), button.press_on_drop())
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
    connection_id: CatalogItemId,
    sink_id: GlobalId,
    s3_key_manager: S3KeyManager,
    completion_stream: Stream<G, Result<u64, String>>,
    worker_callback: F,
) -> PressOnDropButton
where
    G: Scope<Timestamp = Timestamp>,
    F: FnOnce(Result<u64, String>) -> () + 'static,
{
    let worker_id = scope.index();
    let num_workers = scope.peers();
    let leader_id = usize::cast_from((sink_id, "completion").hashed()) % num_workers;
    let is_leader = worker_id == leader_id;

    let mut builder = AsyncOperatorBuilder::new("CopyToS3-completion".to_string(), scope.clone());

    let mut completion_input = builder.new_disconnected_input(&completion_stream, Pipeline);

    let button = builder.build(move |_| async move {
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

        worker_callback(fallible_logic.await.map_err(|e| e.to_string_with_causes()));
    });

    button.press_on_drop()
}

/// Renders the `upload operator`, which waits on the `start_stream` to ensure
/// initialization is complete and then handles the uploads to S3.
/// Returns a `completion_stream` which contains 1 event per worker of
/// the result of the upload operation, either an error or the number of rows
/// uploaded by the worker.
///
/// The `input_collection` must be a stream of chains, partitioned and exchanged by the row's hash
/// modulo the number of batches.
fn render_upload_operator<G, T>(
    scope: G,
    connection_context: ConnectionContext,
    aws_connection: AwsConnection,
    connection_id: CatalogItemId,
    connection_details: S3UploadInfo,
    sink_id: GlobalId,
    input_collection: Stream<G, Vec<TimelyStack<((Row, ()), G::Timestamp, Diff)>>>,
    up_to: Antichain<G::Timestamp>,
    start_stream: Stream<G, Result<(), String>>,
    params: CopyToParameters,
    output_batch_count: u64,
) -> (Stream<G, Result<u64, String>>, PressOnDropButton)
where
    G: Scope<Timestamp = Timestamp>,
    T: CopyToS3Uploader,
{
    let worker_id = scope.index();
    let mut builder = AsyncOperatorBuilder::new("CopyToS3-uploader".to_string(), scope.clone());

    let mut input_handle = builder.new_disconnected_input(&input_collection, Pipeline);
    let (completion_handle, completion_stream) =
        builder.new_output::<CapacityContainerBuilder<_>>();
    let mut start_handle = builder.new_input_for(&start_stream, Pipeline, &completion_handle);

    let button = builder.build(move |caps| async move {
        let [completion_cap] = caps.try_into().unwrap();

        // Drain any events in the start stream. Once this stream advances to the empty frontier we
        // can proceed with uploading. If we see an error in this stream, forward it to the completion
        // stream and early-exit.
        while let Some(event) = start_handle.next().await {
            match event {
                AsyncEvent::Data(cap, data) => {
                    for res in data {
                        if res.is_err() {
                            completion_handle.give(&cap, res.map(|_| 0));
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

            // As a special case, the 0th worker always forces a file to be
            // created for batch 0, even if it never sees any data for batch 0.
            // This ensures that we always write at least one file to S3, even
            // if the input is empty. See database-issue#8599.
            if worker_id == 0 {
                let mut uploader = T::new(
                    sdk_config.clone(),
                    connection_details.clone(),
                    &sink_id,
                    0,
                    params.clone(),
                )?;
                uploader.force_new_file().await?;
                s3_uploaders.insert(0, uploader);
            }

            let mut row_count = 0;
            while let Some(event) = input_handle.next().await {
                match event {
                    AsyncEvent::Data(_ts, data) => {
                        for ((row, ()), ts, diff) in
                            data.iter().flatten().flat_map(|chunk| chunk.iter())
                        {
                            // We're consuming a batch of data, and the upstream operator has to ensure
                            // that the data is exchanged according to the batch.
                            let batch = row.hashed() % output_batch_count;
                            if !up_to.less_equal(ts) {
                                if diff.is_negative() {
                                    tracing::error!(
                                        %sink_id, %diff, ?row,
                                        "S3 oneshot sink encountered negative multiplicities",
                                    );
                                    anyhow::bail!(
                                        "Invalid data in source, \
                                         saw retractions ({}) for row that does not exist: {:?}",
                                        -*diff,
                                        row,
                                    )
                                }
                                row_count += u64::try_from(diff.into_inner()).unwrap();
                                let uploader = match s3_uploaders.entry(batch) {
                                    Entry::Occupied(entry) => entry.into_mut(),
                                    Entry::Vacant(entry) => {
                                        debug!(%sink_id, %worker_id, "handling batch: {}", batch);
                                        entry.insert(T::new(
                                            sdk_config.clone(),
                                            connection_details.clone(),
                                            &sink_id,
                                            batch,
                                            params.clone(),
                                        )?)
                                    }
                                };
                                for _ in 0..diff.into_inner() {
                                    uploader.append_row(row).await?;
                                }
                            }
                        }
                    }
                    AsyncEvent::Progress(frontier) => {
                        if PartialOrder::less_equal(&up_to, &frontier) {
                            for uploader in s3_uploaders.values_mut() {
                                uploader.finish().await?;
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

        completion_handle.give(&completion_cap, res.map_err(|e| e.to_string_with_causes()));
    });

    (completion_stream, button.press_on_drop())
}

/// dyncfg parameters for the copy_to operator, stored in this struct to avoid
/// requiring storage to depend on the compute crate. See `src/compute-types/src/dyncfgs.rs`
/// for the definition of these parameters.
#[derive(Clone, Debug)]
pub struct CopyToParameters {
    // The ratio (defined as a percentage) of row-group size to max-file-size.
    // See the `parquet` module for more details on how this is used.
    pub parquet_row_group_ratio: usize,
    // The ratio (defined as a percentage) of arrow-builder size to row-group size.
    // See the `parquet` module for more details on how this is used.
    pub arrow_builder_buffer_ratio: usize,
    // The size of each part in the multi-part upload to use when uploading files to S3.
    pub s3_multipart_part_size_bytes: usize,
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
        params: CopyToParameters,
    ) -> Result<Self, anyhow::Error>;
    /// Force the start of a new file, even if no rows have yet been appended or
    /// if the current file has not yet reached the configured `max_file_size`.
    async fn force_new_file(&mut self) -> Result<(), anyhow::Error>;
    /// Append a row to the internal buffer, and optionally flush the buffer to S3.
    async fn append_row(&mut self, row: &Row) -> Result<(), anyhow::Error>;
    /// Flush the full remaining internal buffer to S3, and close all open resources.
    /// This will be called when the input stream is finished.
    async fn finish(&mut self) -> Result<(), anyhow::Error>;
}
