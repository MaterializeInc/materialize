// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_types::sdk_config::SdkConfig;

use mz_repr::{GlobalId, Row};
use mz_storage_types::sinks::S3UploadInfo;

use super::CopyToS3Uploader;

pub(super) struct ParquetUploader {}

impl CopyToS3Uploader for ParquetUploader {
    fn new(
        _sdk_config: SdkConfig,
        _connection_details: S3UploadInfo,
        _sink_id: &GlobalId,
        _batch: u64,
    ) -> Result<ParquetUploader, anyhow::Error> {
        anyhow::bail!("Parquet uploading is not yet implemented")
    }
    async fn append_row(&mut self, _row: &Row) -> Result<(), anyhow::Error> {
        anyhow::bail!("Parquet uploading is not yet implemented")
    }
    async fn flush(&mut self) -> Result<(), anyhow::Error> {
        anyhow::bail!("Parquet uploading is not yet implemented")
    }
}
