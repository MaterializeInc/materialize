// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_types::sdk_config::SdkConfig;
use mz_arrow_util::builder::ArrowBuilder;
use mz_repr::adt::timestamp::TimeLike;
use mz_repr::{Datum, GlobalId, RelationDesc, Row};
use mz_storage_types::sinks::{S3SinkFormat, S3UploadInfo};

use super::CopyToS3Uploader;

pub(super) struct ParquetUploader {
    /// The output description.
    desc: RelationDesc,
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
    /// The active arrow builder.
    arrow_builder: ArrowBuilder,
}

impl CopyToS3Uploader for ParquetUploader {
    fn new(
        sdk_config: SdkConfig,
        connection_details: S3UploadInfo,
        sink_id: &GlobalId,
        batch: u64,
    ) -> Result<ParquetUploader, anyhow::Error> {
        match connection_details.format {
            S3SinkFormat::Parquet => Ok(ParquetUploader {
                arrow_builder: ArrowBuilder::new(&connection_details.desc, 1024),
                desc: connection_details.desc,
                sdk_config: Some(sdk_config),
                key_manager: S3KeyManager::new(sink_id, &connection_details.uri),
                batch,
                max_file_size: connection_details.max_file_size,
                file_index: 0,
            }),
            _ => anyhow::bail!("Expected Parquet format"),
        }
    }
    async fn append_row(&mut self, row: Row) -> Result<(), anyhow::Error> {
        self.arrow_builder.add_row(row)?;
        if self.arrow_builder.len() >= self.max_file_size {
            self.flush().await?;
        }
    }
    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        anyhow::bail!("Parquet uploading is not yet implemented")
    }
}
