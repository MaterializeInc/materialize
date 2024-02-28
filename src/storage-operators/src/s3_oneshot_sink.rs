// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A sink operator that writes to s3.

use std::str::FromStr;

use anyhow::anyhow;
use aws_types::sdk_config::SdkConfig;
use bytesize::ByteSize;
use differential_dataflow::Collection;
use http::Uri;
use mz_aws_util::s3_uploader::{S3MultiPartUploader, S3MultiPartUploaderConfig};
use mz_ore::cast::CastFrom;
use mz_pgcopy::{encode_copy_format, CopyFormatParams};
use mz_repr::{Diff, RelationDesc, Row, Timestamp};
use mz_storage_types::connections::ConnectionContext;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::S3UploadInfo;
use mz_timely_util::builder_async::{Event as AsyncEvent, OperatorBuilder as AsyncOperatorBuilder};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::info;

pub fn copy_to<G, F>(
    input_collection: Collection<G, (Row, ()), Diff>,
    err_collection: Collection<G, (DataflowError, ()), Diff>,
    up_to: Antichain<G::Timestamp>,
    connection_details: S3UploadInfo,
    connection_context: ConnectionContext,
    active_worker: usize,
    callback: F,
) where
    G: Scope<Timestamp = Timestamp>,
    F: Fn(Result<u64, String>) -> () + 'static,
{
    let scope = input_collection.scope();
    let worker_id = scope.index();

    let mut builder = AsyncOperatorBuilder::new("CopyToS3".to_string(), scope);

    let mut input_handle = builder.new_disconnected_input(&input_collection.inner, Pipeline);
    let mut error_handle = builder.new_disconnected_input(&err_collection.inner, Pipeline);

    builder.build(move |_caps| async move {
        if worker_id != active_worker {
            return;
        }

        while let Some(event) = error_handle.next().await {
            match event {
                AsyncEvent::Data(_ts, data) => {
                    if let Some(((error, _), ts, _)) = data.first() {
                        if !up_to.less_equal(ts) {
                            callback(Err(error.to_string()));
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

        let connection_id = connection_details.connection_id;
        let sdk_config = match connection_details
            .aws_connection
            .load_sdk_config(&connection_context, connection_id)
            .await
        {
            Ok(sdk_config) => sdk_config,
            Err(e) => {
                callback(Err(e.to_string()));
                return;
            }
        };

        let mut uploader = CopyToS3Uploader::new(sdk_config, connection_details, "part".into());

        let mut row_count = 0;
        while let Some(event) = input_handle.next().await {
            match event {
                AsyncEvent::Data(_ts, data) => {
                    for ((row, ()), ts, diff) in data {
                        if !up_to.less_equal(&ts) {
                            if diff < 0 {
                                tracing::error!("negative accumulation in copy to s3 input");
                                callback(Err(
                                    "interal error while reading from copy to s3 input".to_string()
                                ));
                                return;
                            }
                            row_count += u64::try_from(diff).unwrap();
                            for _ in 0..diff {
                                match uploader.append_row(&row).await {
                                    Ok(()) => {}
                                    Err(e) => {
                                        callback(Err(e.to_string()));
                                        return;
                                    }
                                }
                            }
                        }
                    }
                }
                AsyncEvent::Progress(frontier) => {
                    if PartialOrder::less_equal(&up_to, &frontier) {
                        match uploader.flush().await {
                            Ok(()) => {
                                // We are done, send the final count.
                                callback(Ok(row_count));
                                return;
                            }
                            Err(e) => {
                                callback(Err(e.to_string()));
                                return;
                            }
                        }
                    }
                }
            }
        }
    });
}

/// Required state to upload batches to S3
struct CopyToS3Uploader {
    /// The output description.
    desc: RelationDesc,
    /// Params to format the data.
    format: CopyFormatParams<'static>,
    /// The index of the current file.
    file_index: usize,
    /// The prefix for the file names.
    file_name_prefix: String,
    /// The s3 bucket.
    bucket: String,
    ///The path prefix where the files should be uploaded to.
    path_prefix: String,
    /// The desired file size. A new file upload will be started
    /// when the size exceeds this amount.
    max_file_size: u64,
    /// The aws sdk config.
    sdk_config: SdkConfig,
    /// Multi-part uploader for the current file.
    /// Keeping the uploader in an `Option` to later take owned value.
    current_file_uploader: Option<S3MultiPartUploader>,
    /// Temporary buffer to store the encoded bytes.
    buf: Vec<u8>,
}

impl CopyToS3Uploader {
    fn new(
        sdk_config: SdkConfig,
        connection_details: S3UploadInfo,
        file_name_prefix: String,
    ) -> CopyToS3Uploader {
        let (bucket, path_prefix) = Self::extract_s3_bucket_path(&connection_details.prefix);
        CopyToS3Uploader {
            desc: connection_details.desc,
            sdk_config,
            format: connection_details.format,
            file_name_prefix,
            bucket,
            path_prefix,
            max_file_size: connection_details.max_file_size,
            file_index: 0,
            current_file_uploader: None,
            buf: Vec::new(),
        }
    }

    /// Creates the uploader for the next file which starts the multi part upload.
    async fn start_new_file(&mut self) -> Result<(), anyhow::Error> {
        self.flush().await?;
        assert!(self.current_file_uploader.is_none());

        self.file_index += 1;
        // TODO: remove hard-coded file extension .csv
        let file_path = format!(
            "{}/{}-{:04}.csv",
            self.path_prefix, self.file_name_prefix, self.file_index
        );

        let bucket = self.bucket.clone();
        info!(
            "starting upload at bucket: {}, file {}",
            &bucket, &file_path
        );
        let uploader = S3MultiPartUploader::try_new(
            &self.sdk_config,
            bucket,
            file_path,
            S3MultiPartUploaderConfig {
                part_size_limit: ByteSize::mib(10).as_u64(),
                file_size_limit: self.max_file_size,
            },
        )
        .await?;
        self.current_file_uploader = Some(uploader);
        Ok(())
    }

    fn extract_s3_bucket_path(prefix: &str) -> (String, String) {
        let uri = Uri::from_str(prefix).expect("valid s3 url");
        let bucket = uri.host().expect("s3 bucket");
        let path = uri.path().trim_start_matches('/').trim_end_matches('/');
        (bucket.to_string(), path.to_string())
    }

    /// Finishes any remaining in-progress upload.
    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        if let Some(uploader) = self.current_file_uploader.take() {
            uploader.finish().await?;
        }
        Ok(())
    }

    /// Appends the row to the in-progress upload or creates a new upload if it
    /// will exceed the max file size.
    async fn append_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        // encode the row and write to temp buffer.
        self.buf.clear();
        encode_copy_format(self.format.clone(), row, self.desc.typ(), &mut self.buf)
            .map_err(|_| anyhow!("error encoding row"))?;
        let buffer_length = self.buf.len();

        if self.current_file_uploader.is_none() {
            self.start_new_file().await?;
        }
        // Ideally it would be nice to get a `&mut uploader` returned from the `start_new_file`,
        // but that runs into borrow checker issues when trying to add the `&self.buf` to the
        // `uploader.add_chunk`.
        let Some(uploader) = self.current_file_uploader.as_mut() else {
            unreachable!("uploader initialized above");
        };
        if u64::cast_from(buffer_length) < uploader.remaining_bytes_limit() {
            // Add to ongoing upload of the current file if still within limit.
            uploader.add_chunk(&self.buf).await?;
        } else {
            // Start a multi part upload of next file.
            self.start_new_file().await?;
            // Upload data for the new part.
            let Some(uploader) = self.current_file_uploader.as_mut() else {
                unreachable!("uploader initialized above");
            };
            uploader.add_chunk(&self.buf).await?;
        }

        Ok(())
    }
}
