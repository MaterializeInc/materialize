// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use aws_types::sdk_config::SdkConfig;
use mz_aws_util::s3_uploader::{
    CompletedUpload, S3MultiPartUploadError, S3MultiPartUploader, S3MultiPartUploaderConfig,
};
use mz_ore::assert_none;
use mz_ore::cast::CastFrom;
use mz_pgcopy::{CopyFormatParams, encode_copy_format, encode_copy_format_header};
use mz_repr::{GlobalId, RelationDesc, Row};
use mz_storage_types::sinks::s3_oneshot_sink::S3KeyManager;
use mz_storage_types::sinks::{S3SinkFormat, S3UploadInfo};
use tracing::info;

use super::{CopyToParameters, CopyToS3Uploader};

/// Required state to upload batches to S3
pub(super) struct PgCopyUploader {
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
    /// Upload parameters.
    params: CopyToParameters,
}

impl CopyToS3Uploader for PgCopyUploader {
    fn new(
        sdk_config: SdkConfig,
        connection_details: S3UploadInfo,
        sink_id: &GlobalId,
        batch: u64,
        params: CopyToParameters,
    ) -> Result<PgCopyUploader, anyhow::Error> {
        match connection_details.format {
            S3SinkFormat::PgCopy(format_params) => Ok(PgCopyUploader {
                desc: connection_details.desc,
                sdk_config: Some(sdk_config),
                format: format_params,
                key_manager: S3KeyManager::new(sink_id, &connection_details.uri),
                batch,
                max_file_size: connection_details.max_file_size,
                file_index: 0,
                current_file_uploader: None,
                params,
            }),
            _ => anyhow::bail!("Expected PgCopy format"),
        }
    }

    /// Finishes any remaining in-progress upload.
    async fn finish(&mut self) -> Result<(), anyhow::Error> {
        if let Some(uploader) = self.current_file_uploader.take() {
            // Moving the aws s3 calls onto tokio tasks instead of using timely runtime.
            let handle =
                mz_ore::task::spawn(|| "s3_uploader::finish", async { uploader.finish().await });
            let CompletedUpload {
                part_count,
                total_bytes_uploaded,
                bucket,
                key,
            } = handle.await?;
            info!(
                "finished upload: bucket {}, key {}, bytes_uploaded {}, parts_uploaded {}",
                bucket, key, total_bytes_uploaded, part_count
            );
        }
        Ok(())
    }

    /// Appends the row to the in-progress upload where it is buffered till it reaches the configured
    /// `part_size_limit` after which the `S3MultiPartUploader` will upload that part. In case it will
    /// exceed the max file size of the ongoing upload, then a new `S3MultiPartUploader` for a new file will
    /// be created and the row data will be appended there.
    async fn append_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        let mut buf: Vec<u8> = vec![];
        // encode the row and write to temp buffer.
        encode_copy_format(&self.format, row, self.desc.typ(), &mut buf)
            .map_err(|_| anyhow!("error encoding row"))?;

        if self.current_file_uploader.is_none() {
            self.start_new_file_upload().await?;
        }
        let mut uploader = self.current_file_uploader.as_mut().expect("known exists");

        match uploader.buffer_chunk(&buf) {
            Ok(_) => Ok(()),
            Err(S3MultiPartUploadError::UploadExceedsMaxFileLimit(_)) => {
                // Start a multi part upload of next file.
                self.start_new_file_upload().await?;
                uploader = self.current_file_uploader.as_mut().expect("known exists");
                uploader.buffer_chunk(&buf)?;
                Ok(())
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn force_new_file(&mut self) -> Result<(), anyhow::Error> {
        self.start_new_file_upload().await
    }
}

impl PgCopyUploader {
    /// Creates the uploader for the next file and starts the multi part upload.
    async fn start_new_file_upload(&mut self) -> Result<(), anyhow::Error> {
        self.finish().await?;
        assert_none!(self.current_file_uploader);

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
        let part_size_limit = u64::cast_from(self.params.s3_multipart_part_size_bytes);
        let handle = mz_ore::task::spawn(|| "s3_uploader::try_new", async move {
            let uploader = S3MultiPartUploader::try_new(
                &sdk_config,
                bucket,
                object_key,
                S3MultiPartUploaderConfig {
                    part_size_limit,
                    file_size_limit: max_file_size,
                },
            )
            .await;
            (uploader, sdk_config)
        });
        let (uploader, sdk_config) = handle.await;
        self.sdk_config = Some(sdk_config);
        let mut uploader = uploader?;
        if self.format.requires_header() {
            let mut buf: Vec<u8> = vec![];
            encode_copy_format_header(&self.format, &self.desc, &mut buf)
                .map_err(|_| anyhow!("error encoding header"))?;
            uploader.buffer_chunk(&buf)?;
        }
        self.current_file_uploader = Some(uploader);
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
    use mz_pgcopy::CopyFormatParams;
    use mz_repr::{ColumnName, Datum, SqlColumnType, SqlRelationType};
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
    #[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/database-issues/issues/5586
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[ignore] // TODO: Reenable against minio so it can run locally
    async fn test_multiple_files() -> Result<(), anyhow::Error> {
        let sdk_config = mz_aws_util::defaults().load().await;
        let (bucket, path) = match s3_bucket_path_for_test() {
            Some(tuple) => tuple,
            None => return Ok(()),
        };
        let sink_id = GlobalId::User(123);
        let batch = 456;
        let typ: SqlRelationType = SqlRelationType::new(vec![SqlColumnType {
            scalar_type: mz_repr::SqlScalarType::String,
            nullable: true,
        }]);
        let column_names = vec![ColumnName::from("col1")];
        let desc = RelationDesc::new(typ, column_names.into_iter());
        let mut uploader = PgCopyUploader::new(
            sdk_config.clone(),
            S3UploadInfo {
                uri: format!("s3://{}/{}", bucket, path),
                // this is only for testing, users will not be able to set value smaller than 16MB.
                max_file_size: ByteSize::b(6).as_u64(),
                desc,
                format: S3SinkFormat::PgCopy(CopyFormatParams::Csv(Default::default())),
            },
            &sink_id,
            batch,
            CopyToParameters {
                s3_multipart_part_size_bytes: 10 * 1024 * 1024,
                arrow_builder_buffer_ratio: 100,
                parquet_row_group_ratio: 100,
            },
        )?;
        let mut row = Row::default();
        // Even though this will exceed max_file_size, it should be successfully uploaded in a single file.
        row.packer().push(Datum::from("1234567"));
        uploader.append_row(&row).await?;

        // Since the max_file_size is 6B, this row will be uploaded to a new file.
        row.packer().push(Datum::Null);
        uploader.append_row(&row).await?;

        row.packer().push(Datum::from("5678"));
        uploader.append_row(&row).await?;

        uploader.finish().await?;

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
