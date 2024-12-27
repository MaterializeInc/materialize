// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use aws_types::sdk_config::SdkConfig;
use mz_arrow_util::builder::ArrowBuilder;
use mz_aws_util::s3_uploader::{
    CompletedUpload, S3MultiPartUploader, S3MultiPartUploaderConfig, AWS_S3_MAX_PART_COUNT,
};
use mz_ore::cast::CastFrom;
use mz_ore::future::OreFutureExt;
use mz_repr::{GlobalId, RelationDesc, Row};
use mz_storage_types::sinks::s3_oneshot_sink::S3KeyManager;
use mz_storage_types::sinks::{S3SinkFormat, S3UploadInfo};
use parquet::{
    arrow::arrow_writer::ArrowWriter,
    basic::Compression,
    file::properties::{WriterProperties, WriterVersion},
};
use tracing::{debug, info};

use super::{CopyToParameters, CopyToS3Uploader};

/// Set the default capacity for the array builders inside the ArrowBuilder. This is the
/// number of items each builder can hold before it needs to allocate more memory.
const DEFAULT_ARRAY_BUILDER_ITEM_CAPACITY: usize = 1024;
/// Set the default buffer capacity for the string and binary array builders inside the
/// ArrowBuilder. This is the number of bytes each builder can hold before it needs to allocate
/// more memory.
const DEFAULT_ARRAY_BUILDER_DATA_CAPACITY: usize = 1024;

/// A [`ParquetUploader`] that writes rows to parquet files and uploads them to S3.
///
/// Spawns all S3 operations in tokio tasks to avoid blocking the surrounding timely context.
///
/// ## Buffering
///
/// There are several layers of buffering in this uploader:
///
/// - The uploader will hold a [`ParquetFile`] object after the first row is added. This
///   [`ParquetFile`] holds an [`ArrowBuilder`] and an [`ArrowWriter`].
///
/// - The [`ArrowBuilder`] builds a structure of in-memory `mz_arrow_util::builder::ColBuilder`s from incoming
///   [`mz_repr::Row`]s. Each `mz_arrow_util::builder::ColBuilder` holds a specific [`arrow::array::builder`] type
///   for constructing a column of the given type. The entire [`ArrowBuilder`] is flushed to
///   the [`ParquetFile`]'s [`ArrowWriter`] by converting it into a [`arrow::record_batch::RecordBatch`] once we've
///   given it more than the configured arrow_builder_buffer_bytes.
///
/// - The [`ParquetFile`] holds a [`ArrowWriter`] that buffers until it has enough data to write
///   a parquet 'row group'. The 'row group' size is usually based on the number of rows (in the
///   ArrowWriter), but we also force it to flush based on data-size (see below for more details).
///
/// - When a row group is written out, the active [`ParquetFile`] provides a reference to the row
///   group buffer to its [`S3MultiPartUploader`] which will copy the data to its own buffer.
///   If this upload buffer exceeds the configured part size limit, the [`S3MultiPartUploader`]
///   will upload parts to S3 until the upload buffer is below the limit.
///
/// - When the [`ParquetUploader`] is finished, it will flush the active [`ParquetFile`] which will
///   flush its [`ArrowBuilder`] and any open row groups to the [`S3MultiPartUploader`] and upload
///   the remaining parts to S3.
/// ```text
///       ┌───────────────┐
///       │ mz_repr::Rows │
///       └───────┬───────┘
/// ┌─────────────│───────────────────────────────────────────┐
/// │             │         ParquetFile                       │
/// │ ┌───────────▼─────────────┐                             │
/// │ │       ArrowBuilder      │                             │
/// │ │                         │    ┌──────────────────┐     │
/// │ │     Vec<ArrowColumn>    │    │    ArrowWriter   │     │
/// │ │ ┌─────────┐ ┌─────────┐ │    │                  │     │
/// │ │ │         │ │         │ │    │   ┌──────────┐   │     │
/// │ │ │ColBuildr│ │ColBuildr│ ├────┼──►│  buffer  │   │     │
/// │ │ │         │ │         │ │    │   └─────┬────┘   │     │
/// │ │ └─────────┘ └─────────┘ │    │         │        │     │
/// │ │                         │    │   ┌─────▼────┐   │     │
/// │ └─────────────────────────┘    │   │ row group│   │     │
/// │                                │   └─┬────────┘   │     │
/// │                                │     │            │     │
/// │                                └─────┼────────────┘     │
/// │                               ┌──────┼────────────────┐ │
/// │                               │      │     S3MultiPart│ │
/// │                               │ ┌────▼─────┐ Uploader │ │
/// │                               │ │  buffer  │          │ │
/// │    ┌─────────┐                │ └───┬─────┬┘          │ │
/// │    │ S3 API  │◄───────────────┤     │     │           │ │
/// │    └─────────┘                │ ┌───▼──┐ ┌▼─────┐     │ │
/// │                               │ │ part │ │ part │     │ │
/// │                               │ └──────┘ └──────┘     │ │
/// │                               │                       │ │
/// │                               └───────────────────────┘ │
/// │                                                         │
/// └─────────────────────────────────────────────────────────┘
/// ```
///
/// ## File Size & Buffer Sizes
///
/// We expose a 'MAX FILE SIZE' parameter to the user, but this is difficult to enforce exactly
/// since we don't know the exact size of the data we're writing before a parquet row-group
/// is flushed. This is because the encoded size of the data is different than the in-memory
/// representation and because the data pages within each column in a row-group are compressed.
/// We also don't know the exact size of the parquet metadata that will be written to the file.
///
/// Therefore we don't use the S3MultiPartUploader's hard file size limit since it's difficult
/// to handle those errors after we've already flushed data to the ArrowWriter. Instead we
/// implement a crude check ourselves.
///
/// This check aims to hit the max-size limit but may exceed it by some amount. To ensure
/// that amount is small, we set the max row-group size to a configurable ratio (e.g. 20%)
/// of the max_file_size. This determines how often we'll flush a row-group, but is only an
/// approximation since the actual size of the row-group is not known until it's written.
/// After each row-group is flushed, the size of the file is checked and if it's exceeded
/// max-file-size a new file is started.
///
/// We also set the max ArrowBuilder buffer size to a ratio (e.g. 150%) of the row-group size
/// to avoid the ArrowWriter buffering too much data itself before flushing a row-group. We're
/// aiming for the encoded & compressed size of the ArrowBuilder data to be roughly equal to
/// the row-group size, but this is only an approximation.
///
/// TODO: We may want to consider adding additional limits to the buffer sizes to avoid memory
/// issues if the user sets the max file size to be very large.
pub(super) struct ParquetUploader {
    /// The output description.
    desc: Arc<RelationDesc>,
    /// The index of the next file to upload within the batch.
    next_file_index: usize,
    /// Provides the appropriate bucket and object keys to use for uploads.
    key_manager: S3KeyManager,
    /// Identifies the batch that files uploaded by this uploader belong to.
    batch: u64,
    /// The desired file size. A new file upload will be started
    /// when the size exceeds this amount.
    max_file_size: u64,
    /// The aws sdk config.
    sdk_config: Arc<SdkConfig>,
    row_group_size_bytes: u64,
    arrow_builder_buffer_bytes: u64,
    /// The active parquet file being written to, stored in an option
    /// since it won't be initialized until the builder is first flushed,
    /// and to make it easier to take ownership when calling in spawned
    /// tokio tasks (to avoid doing I/O in the surrounding timely context).
    active_file: Option<ParquetFile>,
    /// Upload and buffer params
    params: CopyToParameters,
}

impl CopyToS3Uploader for ParquetUploader {
    fn new(
        sdk_config: SdkConfig,
        connection_details: S3UploadInfo,
        sink_id: &GlobalId,
        batch: u64,
        params: CopyToParameters,
    ) -> Result<ParquetUploader, anyhow::Error> {
        if params.parquet_row_group_ratio > 100 {
            anyhow::bail!("parquet_row_group_ratio must be <= 100");
        }
        if params.arrow_builder_buffer_ratio < 100 {
            anyhow::bail!("arrow_builder_buffer_ratio must be >= 100");
        }
        let row_group_size_bytes =
            connection_details.max_file_size * u64::cast_from(params.parquet_row_group_ratio) / 100;
        let arrow_builder_buffer_bytes =
            row_group_size_bytes * u64::cast_from(params.arrow_builder_buffer_ratio) / 100;

        match connection_details.format {
            S3SinkFormat::Parquet => Ok(ParquetUploader {
                desc: Arc::new(connection_details.desc),
                sdk_config: Arc::new(sdk_config),
                key_manager: S3KeyManager::new(sink_id, &connection_details.uri),
                batch,
                max_file_size: connection_details.max_file_size,
                next_file_index: 0,
                row_group_size_bytes,
                arrow_builder_buffer_bytes,
                active_file: None,
                params,
            }),
            _ => anyhow::bail!("Expected Parquet format"),
        }
    }

    async fn append_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        let active_file = match self.active_file {
            Some(ref mut file) => file,
            None => self.start_new_file().await?,
        };

        active_file.add_row(row)?;

        // If this file has gone over the max size, start a new one
        if active_file.size_estimate() >= self.max_file_size {
            debug!("file size limit exceeded, starting new file");
            self.start_new_file().await?;
        }

        Ok(())
    }

    async fn finish(&mut self) -> Result<(), anyhow::Error> {
        if let Some(active_file) = self.active_file.take() {
            active_file
                .finish()
                .run_in_task(|| "ParquetFile::finish")
                .await?;
        }
        Ok(())
    }

    async fn force_new_file(&mut self) -> Result<(), anyhow::Error> {
        self.start_new_file().await?;
        Ok(())
    }
}

impl ParquetUploader {
    /// Start a new parquet file for upload. Will finish the current file if one is active.
    async fn start_new_file(&mut self) -> Result<&mut ParquetFile, anyhow::Error> {
        if let Some(active_file) = self.active_file.take() {
            active_file
                .finish()
                .run_in_task(|| "ParquetFile::finish")
                .await?;
        }
        let object_key = self
            .key_manager
            .data_key(self.batch, self.next_file_index, "parquet");
        self.next_file_index += 1;

        let bucket = self.key_manager.bucket.clone();
        info!("starting upload: bucket {}, key {}", bucket, object_key);
        let new_file = ParquetFile::new(
            bucket,
            object_key,
            Arc::clone(&self.desc),
            Arc::clone(&self.sdk_config),
            self.arrow_builder_buffer_bytes,
            self.row_group_size_bytes,
            u64::cast_from(self.params.s3_multipart_part_size_bytes),
        )
        .run_in_task(|| "ParquetFile::new")
        .await?;

        self.active_file = Some(new_file);
        Ok(self.active_file.as_mut().unwrap())
    }
}

/// Helper to tie the lifecycle of the `ArrowBuilder`, `ArrowWriter`, and `S3MultiPartUploader`
/// together for a single parquet file.
struct ParquetFile {
    /// The active arrow builder.
    builder: ArrowBuilder,
    writer: ArrowWriter<Vec<u8>>,
    // TODO: Consider implementing `tokio::io::AsyncWrite` on `S3MultiPartUploader` which would
    // allow us to write directly to the uploader instead of buffering the data in a vec first.
    uploader: S3MultiPartUploader,
    arrow_builder_buffer_bytes: u64,
    row_group_size: u64,
    desc: Arc<RelationDesc>,
}

impl ParquetFile {
    async fn new(
        bucket: String,
        key: String,
        desc: Arc<RelationDesc>,
        sdk_config: Arc<SdkConfig>,
        arrow_builder_buffer_bytes: u64,
        row_group_size: u64,
        part_size_limit: u64,
    ) -> Result<Self, anyhow::Error> {
        let builder = ArrowBuilder::new(
            &desc,
            DEFAULT_ARRAY_BUILDER_ITEM_CAPACITY,
            DEFAULT_ARRAY_BUILDER_DATA_CAPACITY,
        )?;

        let props = WriterProperties::builder()
            // This refers to the number of rows per row-group, which we don't want the writer
            // to enforce since we will flush based on the byte-size of the active row group
            .set_max_row_group_size(usize::MAX)
            // Max compatibility
            .set_writer_version(WriterVersion::PARQUET_1_0)
            .set_compression(Compression::SNAPPY)
            .build();

        // TODO: Consider using an lgalloc buffer here instead of a vec
        let writer = ArrowWriter::try_new(Vec::new(), builder.schema().into(), Some(props))?;
        let uploader = S3MultiPartUploader::try_new(
            sdk_config.as_ref(),
            bucket,
            key,
            S3MultiPartUploaderConfig {
                part_size_limit,
                // We are already enforcing the max size ourselves so we set the max size enforced
                // by the uploader to the max file size it will allow based on the part size limit.
                // This is known to be greater than the `MAX_S3_SINK_FILE_SIZE` enforced during
                // sink creation.
                file_size_limit: part_size_limit
                    .checked_mul(AWS_S3_MAX_PART_COUNT.try_into().expect("known safe"))
                    .expect("known safe"),
            },
        )
        .await?;

        Ok(Self {
            writer,
            uploader,
            arrow_builder_buffer_bytes,
            row_group_size,
            desc,
            builder,
        })
    }

    fn add_row(&mut self, row: &Row) -> Result<(), anyhow::Error> {
        self.builder.add_row(row)?;

        if u64::cast_from(self.builder.row_size_bytes()) > self.arrow_builder_buffer_bytes {
            self.flush_builder()?;
        }

        Ok(())
    }

    /// Flush the current arrow builder, the parquet writer, and the uploader.
    async fn finish(mut self) -> Result<CompletedUpload, anyhow::Error> {
        self.flush_builder()?;
        let buffer = self.writer.into_inner()?;
        self.uploader.buffer_chunk(buffer.as_slice())?;
        let res = self.uploader.finish().await?;
        info!(
            "finished upload: bucket {}, key {}, bytes_uploaded {}, parts_uploaded {}",
            res.bucket, res.key, res.total_bytes_uploaded, res.part_count
        );
        Ok(res)
    }

    /// Flush the current arrow builder to the parquet writer then flushes the writer's buffer to
    /// the uploader which may trigger an upload.
    fn flush_builder(&mut self) -> Result<(), anyhow::Error> {
        let builder = std::mem::replace(
            &mut self.builder,
            ArrowBuilder::new(
                &self.desc,
                DEFAULT_ARRAY_BUILDER_ITEM_CAPACITY,
                DEFAULT_ARRAY_BUILDER_DATA_CAPACITY,
            )?,
        );
        let arrow_batch = builder.to_record_batch()?;

        if arrow_batch.num_rows() == 0 {
            return Ok(());
        }

        let before_groups = self.writer.flushed_row_groups().len();
        self.writer.write(&arrow_batch)?;

        // The writer will flush its buffer to a new parquet row group based on the row count,
        // not the actual size of the data. We flush manually to allow uploading the data in
        // potentially smaller chunks.
        if u64::cast_from(self.writer.in_progress_size()) > self.row_group_size {
            self.writer.flush()?;
        }

        // If the writer has flushed a new row group we can steal its buffer and upload it.
        if self.writer.flushed_row_groups().len() > before_groups {
            let buffer = self.writer.inner_mut();
            self.uploader.buffer_chunk(buffer.as_slice())?;
            // reuse the buffer in the writer
            buffer.clear();
        }
        Ok(())
    }

    /// Returns an approximate size estimate of the file being written.
    fn size_estimate(&self) -> u64 {
        // ArrowWriter.in_progress_size() is just an estimate since it doesn't seem
        // to account for data page compression and metadata that will be written for the next
        // row-group.
        u64::cast_from(self.writer.in_progress_size()) + self.uploader.added_bytes()
    }
}
