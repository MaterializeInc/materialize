// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Uploads a consolidated collection to S3

use std::collections::BTreeMap;
use std::str::FromStr;

use anyhow::anyhow;
use aws_types::sdk_config::SdkConfig;
use bytesize::ByteSize;
use differential_dataflow::{Collection, Hashable};
use http::Uri;
use mz_aws_util::s3_uploader::{
    CompletedUpload, S3MultiPartUploadError, S3MultiPartUploader, S3MultiPartUploaderConfig,
};
use mz_ore::cast::CastFrom;
use mz_ore::future::InTask;
use mz_ore::task::JoinHandleExt;
use mz_pgcopy::{encode_copy_format, CopyFormatParams};
use mz_repr::{Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::S3UploadInfo;
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::Scope;
use timely::dataflow::Stream;
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::{debug, info};

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

    let completion_stream = render_upload_operator(
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
    );

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
                            "S3 bucket path is not empty, contains: {:?}",
                            files
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

        if is_leader {
            let res = leader_work.await.map_err(|e| e.to_string());
            start_handle.give(&start_cap, res).await;
        };
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
        let leader_cleanup_work = async move {
            info!(%sink_id, %worker_id, "s3 leader worker completion");
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
            Ok::<u64, anyhow::Error>(0)
        };

        let mut res = None;
        while let Some(event) = completion_input.next().await {
            match event {
                AsyncEvent::Data(_ts, data) => {
                    for evt in data {
                        // We only expect to receive one event on this stream
                        assert!(
                            res.is_none(),
                            "unexpectedly received more than 1 event on the completion stream!"
                        );

                        // If we're not the leader or if the completion event is an error,
                        // return immediately to the response callback.
                        if !is_leader || evt.is_err() {
                            worker_callback(evt);
                            return;
                        }
                        // Store this event for the leader worker to use below
                        res = Some(evt);
                    }
                }
                AsyncEvent::Progress(frontier) => {
                    if frontier.is_empty() && is_leader {
                        // Once the leader sees the empty frontier, it knows that all workers
                        // have completed their work and can proceed with cleanup steps
                        assert!(is_leader, "non-leader did not receive a completion event");
                        let cleanup_res = leader_cleanup_work.await;
                        if cleanup_res.is_err() {
                            // something errored while cleaning up
                            worker_callback(cleanup_res.map_err(|e| e.to_string()));
                        } else {
                            match res {
                                Some(res) if res.is_ok() => worker_callback(res),
                                Some(_) => panic!("expected success result for leader"),
                                None => panic!("leader did not receive a completion event"),
                            }
                        }
                        return;
                    }
                }
            }
        }
        panic!("completion stream exited too early");
    });
}

/// Renders the `upload operator`, which waits on the `start_stream` to ensure
/// initialization is complete and then handles the uploads to S3.
/// Returns a `completion_stream` which contains 1 event per worker of
/// the result of the upload operation, either an error or the number of rows
/// uploaded by the worker.
fn render_upload_operator<G>(
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
            let mut s3_uploaders: BTreeMap<u64, CopyToS3Uploader> = BTreeMap::new();
            let mut row_count = 0;
            while let Some(event) = input_handle.next().await {
                match event {
                    AsyncEvent::Data(_ts, data) => {
                        for (((row, batch), ()), ts, diff) in data {
                            if !up_to.less_equal(&ts) {
                                if diff < 0 {
                                    Err(anyhow::anyhow!(
                                        "Invalid data in source errors, saw retractions ({}) for \
                                        row that does not exist",
                                        diff * -1,
                                    ))?;
                                }
                                row_count += u64::try_from(diff).unwrap();
                                let uploader = s3_uploaders.entry(batch).or_insert_with(|| {
                                    debug!(%sink_id, %worker_id, "handling batch: {}", batch);
                                    CopyToS3Uploader::new(
                                        sdk_config.clone(),
                                        connection_details.clone(),
                                        &sink_id,
                                        batch,
                                    )
                                });
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

/// Required state to upload batches to S3
struct CopyToS3Uploader {
    /// The output description.
    desc: RelationDesc,
    /// Params to format the data.
    format: CopyFormatParams<'static>,
    /// The index of the current file within the batch.
    file_index: usize,
    /// Provides the appropriate bucket and object keys to use for uploads
    key_manager: S3KeyManager,
    /// Identifies the batch that files uploaded by this uploader belong to
    batch: u64,
    /// The desired file size. A new file upload will be started
    /// when the size exceeds this amount.
    max_file_size: u64,
    /// The aws sdk config.
    /// This is an option so that we can get an owned value later to move to a
    /// spawned tokio task.
    sdk_config: Option<SdkConfig>,
    /// Multi-part uploader for the current file.
    /// Keeping the uploader in an `Option` to later take owned value.
    current_file_uploader: Option<S3MultiPartUploader>,
    /// Temporary buffer to store the encoded bytes.
    /// Currently at a time this will only store one single encoded row
    /// before getting added to the `current_file_uploader`'s buffer.
    buf: Vec<u8>,
}

impl CopyToS3Uploader {
    fn new(
        sdk_config: SdkConfig,
        connection_details: S3UploadInfo,
        sink_id: &GlobalId,
        batch: u64,
    ) -> CopyToS3Uploader {
        CopyToS3Uploader {
            desc: connection_details.desc,
            sdk_config: Some(sdk_config),
            format: connection_details.format,
            key_manager: S3KeyManager::new(sink_id, &connection_details.uri),
            batch,
            max_file_size: connection_details.max_file_size,
            file_index: 0,
            current_file_uploader: None,
            buf: Vec::new(),
        }
    }

    /// Creates the uploader for the next file and starts the multi part upload.
    async fn start_new_file_upload(&mut self) -> Result<(), anyhow::Error> {
        self.flush().await?;
        assert!(self.current_file_uploader.is_none());

        self.file_index += 1;
        let object_key =
            self.key_manager
                .data_key(self.batch, self.file_index, self.format.file_extension());
        let bucket = self.key_manager.bucket.clone();
        info!("starting upload: bucket {}, key {}", &bucket, &object_key);
        let sdk_config = self
            .sdk_config
            .take()
            .expect("sdk_config should always be present");
        let max_file_size = self.max_file_size;
        // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
        let handle = mz_ore::task::spawn(|| "s3_uploader::try_new", async move {
            let uploader = S3MultiPartUploader::try_new(
                &sdk_config,
                bucket,
                object_key,
                S3MultiPartUploaderConfig {
                    part_size_limit: ByteSize::mib(10).as_u64(),
                    file_size_limit: max_file_size,
                },
            )
            .await;
            (uploader, sdk_config)
        });
        let (uploader, sdk_config) = handle.wait_and_assert_finished().await;
        self.sdk_config = Some(sdk_config);
        self.current_file_uploader = Some(uploader?);
        Ok(())
    }

    /// Finishes any remaining in-progress upload.
    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        if let Some(uploader) = self.current_file_uploader.take() {
            let object_key = self.key_manager.data_key(
                self.batch,
                self.file_index,
                self.format.file_extension(),
            );
            // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
            let handle =
                mz_ore::task::spawn(|| "s3_uploader::finish", async { uploader.finish().await });
            let CompletedUpload {
                part_count,
                total_bytes_uploaded,
            } = handle.wait_and_assert_finished().await?;
            info!(
                "finished upload: bucket {}, key {}, bytes_uploaded {}, parts_uploaded {}",
                &self.key_manager.bucket, object_key, total_bytes_uploaded, part_count
            );
        }
        Ok(())
    }

    async fn upload_buffer(&mut self) -> Result<(), S3MultiPartUploadError> {
        assert!(!self.buf.is_empty());
        assert!(self.current_file_uploader.is_some());

        let mut uploader = self.current_file_uploader.take().unwrap();
        // TODO: Make buf a Bytes so it can be cheaply cloned.
        let buf = std::mem::take(&mut self.buf);
        // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
        let handle = mz_ore::task::spawn(|| "s3_uploader::buffer_chunk", async move {
            let result = uploader.buffer_chunk(&buf).await;
            (uploader, buf, result)
        });
        let (uploader, buf, result) = handle.wait_and_assert_finished().await;
        self.current_file_uploader = Some(uploader);
        self.buf = buf;

        let _ = result?;
        Ok(())
    }

    /// Appends the row to the in-progress upload where it is buffered till it reaches the configured
    /// `part_size_limit` after which the `S3MultiPartUploader` will upload that part. In case it will
    /// exceed the max file size of the ongoing upload, then a new `S3MultiPartUploader` for a new file will
    /// be created and the row data will be appended there.
    async fn append_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        self.buf.clear();
        // encode the row and write to temp buffer.
        encode_copy_format(&self.format, row, self.desc.typ(), &mut self.buf)
            .map_err(|_| anyhow!("error encoding row"))?;

        if self.current_file_uploader.is_none() {
            self.start_new_file_upload().await?;
        }

        match self.upload_buffer().await {
            Ok(_) => Ok(()),
            Err(S3MultiPartUploadError::UploadExceedsMaxFileLimit(_)) => {
                // Start a multi part upload of next file.
                self.start_new_file_upload().await?;
                // Upload data for the new part.
                self.upload_buffer().await?;
                Ok(())
            }
            Err(e) => Err(e),
        }?;

        Ok(())
    }
}

/// On CI, these tests are enabled by adding the scratch-aws-access plugin
/// to the `cargo-test` step in `ci/test/pipeline.template.yml` and setting
/// `MZ_S3_UPLOADER_TEST_S3_BUCKET` in
/// `ci/test/cargo-test/mzcompose.py`.
///
/// For a Materialize developer, to opt in to these tests locally for
/// development, follow the AWS access guide:
///
/// ```text
/// https://www.notion.so/materialize/AWS-access-5fbd9513dcdc4e11a7591e8caa5f63fe
/// ```
///
/// then running `source src/aws-util/src/setup_test_env_mz.sh`. You will also have
/// to run `aws sso login` if you haven't recently.
#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use mz_repr::{ColumnName, ColumnType, Datum, RelationType};
    use uuid::Uuid;

    use super::*;

    fn s3_bucket_path_for_test() -> Option<(String, String)> {
        let bucket = match std::env::var("MZ_S3_UPLOADER_TEST_S3_BUCKET") {
            Ok(bucket) => bucket,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return None;
            }
        };

        let prefix = Uuid::new_v4().to_string();
        let path = format!("cargo_test/{}/file", prefix);
        Some((bucket, path))
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/materialize/issues/18898
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    async fn test_multiple_files() -> Result<(), anyhow::Error> {
        let sdk_config = mz_aws_util::defaults().load().await;
        let (bucket, path) = match s3_bucket_path_for_test() {
            Some(tuple) => tuple,
            None => return Ok(()),
        };
        let sink_id = GlobalId::User(123);
        let batch = 456;
        let typ: RelationType = RelationType::new(vec![ColumnType {
            scalar_type: mz_repr::ScalarType::String,
            nullable: true,
        }]);
        let column_names = vec![ColumnName::from("col1")];
        let desc = RelationDesc::new(typ, column_names.into_iter());
        let mut uploader = CopyToS3Uploader::new(
            sdk_config.clone(),
            S3UploadInfo {
                uri: format!("s3://{}/{}", bucket, path),
                // this is only for testing, users will not be able to set value smaller than 16MB.
                max_file_size: ByteSize::b(6).as_u64(),
                desc,
                format: CopyFormatParams::Csv(Default::default()),
            },
            &sink_id,
            batch,
        );
        let mut row = Row::default();
        // Even though this will exceed max_file_size, it should be successfully uploaded in a single file.
        row.packer().push(Datum::from("1234567"));
        uploader.append_row(&row).await?;

        // Since the max_file_size is 6B, this row will be uploaded to a new file.
        row.packer().push(Datum::Null);
        uploader.append_row(&row).await?;

        row.packer().push(Datum::from("5678"));
        uploader.append_row(&row).await?;

        uploader.flush().await?;

        // Based on the max_file_size, the uploader should have uploaded two
        // files, part-0001.csv and part-0002.csv
        let s3_client = mz_aws_util::s3::new_client(&sdk_config);
        let first_file = s3_client
            .get_object()
            .bucket(bucket.clone())
            .key(format!(
                "{}/mz-{}-batch-{:04}-0001.csv",
                path, sink_id, batch
            ))
            .send()
            .await
            .unwrap();

        let body = first_file.body.collect().await.unwrap().into_bytes();
        let expected_body: &[u8] = b"1234567\n";
        assert_eq!(body, *expected_body);

        let second_file = s3_client
            .get_object()
            .bucket(bucket)
            .key(format!(
                "{}/mz-{}-batch-{:04}-0002.csv",
                path, sink_id, batch
            ))
            .send()
            .await
            .unwrap();

        let body = second_file.body.collect().await.unwrap().into_bytes();
        let expected_body: &[u8] = b"\n5678\n";
        assert_eq!(body, *expected_body);

        Ok(())
    }
}
