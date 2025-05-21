// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apache Parquet encodings and utils for persist data

use std::io::Write;
use std::sync::Arc;

use arrow::row::Row;
use bytes::buf::Reader;
use bytes::{Buf, Bytes};
use differential_dataflow::trace::Description;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_persist_types::Codec64;
use mz_persist_types::parquet::EncodingConfig;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::basic::Encoding;
use parquet::errors::ParquetError;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::file::reader::{ChunkReader, Length};
use timely::progress::{Antichain, Timestamp};
use tracing::warn;

use crate::error::Error;
use crate::generated::persist::ProtoBatchFormat;
use crate::generated::persist::proto_batch_part_inline::FormatMetadata as ProtoFormatMetadata;
use crate::indexed::columnar::arrow::{decode_arrow_batch, encode_arrow_batch};
use crate::indexed::encoding::{
    BlobTraceBatchPart, BlobTraceUpdates, decode_trace_inline_meta, encode_trace_inline_meta,
};
use crate::metrics::{ColumnarMetrics, ParquetColumnMetrics};

const INLINE_METADATA_KEY: &str = "MZ:inline";

/// Information that can be used to locate a row group, and the
/// bloom filter within it.
#[derive(Debug, Clone)]
pub struct RowGroupStats {
    /// The offset of the row group within the file.
    pub offset: i64,
    /// The size of the row group within the file.
    pub size: i64,
    /// The offset and size of the bloom 🌸 filter within the row group.
    pub blooms: Vec<(i64, i32)>,
}

/// A reader for row groups in a parquet file.
/// This is kind of a hack, but it maps logical offsets
/// (from the original file) to physical batches of data.
#[derive(Debug, Clone)]
pub struct RowGroupsReader {
    /// A mapping of the logical offset to the physical bytes.
    pub segments: Vec<((usize, usize), Bytes)>,
    /// all the bytes in the segments
    pub length: usize,
}

impl RowGroupsReader {
    /// Creates a new [`RowGroupsReader`] from the given segments.
    pub fn new(segments: Vec<((usize, usize), Bytes)>) -> Self {
        let length = segments.iter().map(|(_, bytes)| bytes.len()).sum();
        RowGroupsReader { segments, length }
    }

    /// Add a new row group to the reader.
    pub fn push_row_group(&mut self, offset: (usize, usize), bytes: Bytes) {
        self.length += bytes.len();
        self.segments.push((offset, bytes));
    }
}

impl Length for RowGroupsReader {
    fn len(&self) -> u64 {
        self.length as u64
    }
}

impl ChunkReader for RowGroupsReader {
    type T = Reader<Bytes>;
    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        for ((offset_start, offset_end), bytes) in &self.segments {
            if *offset_start <= start.try_into().unwrap()
                && *offset_end >= TryInto::<usize>::try_into(start).unwrap() + length
            {
                let offset = start - *offset_start as u64;
                let length = length as u64;
                return Ok(bytes.slice(offset as usize..(offset + length) as usize));
            }
        }
        Err(ParquetError::EOF(format!(
            "could not find bytes in segments: {start} {length}"
        )))
    }

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        for ((offset_start, offset_end), bytes) in &self.segments {
            if *offset_start <= start.try_into().unwrap()
                && *offset_end >= TryInto::<usize>::try_into(start).unwrap()
            {
                let offset = start - *offset_start as u64;
                return Ok(bytes.slice(offset as usize..*offset_end as usize).reader());
            }
        }
        Err(ParquetError::EOF(format!(
            "could not find bytes in segments: {start}"
        )))
    }
}

/// Encodes a [`BlobTraceBatchPart`] into the Parquet format.
pub fn encode_trace_parquet<W: Write + Send, T: Timestamp + Codec64>(
    w: &mut W,
    batch: &BlobTraceBatchPart<T>,
    metrics: &ColumnarMetrics,
    cfg: &EncodingConfig,
) -> Result<Vec<RowGroupStats>, Error> {
    // Better to error now than write out an invalid batch.
    batch.validate()?;

    let inline_meta = encode_trace_inline_meta(batch);
    encode_parquet_kvtd(w, inline_meta, &batch.updates, metrics, cfg)
}

/// Decodes a BlobTraceBatchPart from the Parquet format.
pub fn decode_trace_parquet<T: Timestamp + Codec64>(
    buf: SegmentedBytes,
    metrics: &ColumnarMetrics,
) -> Result<BlobTraceBatchPart<T>, Error> {
    let metadata = ArrowReaderMetadata::load(&buf, Default::default())?;
    let metadata = metadata
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .as_ref()
        .and_then(|x| x.iter().find(|x| x.key == INLINE_METADATA_KEY));

    let (format, metadata) = decode_trace_inline_meta(metadata.and_then(|x| x.value.as_ref()))?;
    let updates = match format {
        ProtoBatchFormat::Unknown => return Err("unknown format".into()),
        ProtoBatchFormat::ArrowKvtd => {
            return Err("ArrowKVTD format not supported in parquet".into());
        }
        ProtoBatchFormat::ParquetKvtd => decode_parquet_file_kvtd(buf, None, metrics)?,
        ProtoBatchFormat::ParquetStructured => {
            // Even though `format_metadata` is optional, we expect it when
            // our format is ParquetStructured.
            let format_metadata = metadata
                .format_metadata
                .as_ref()
                .ok_or_else(|| "missing field 'format_metadata'".to_string())?;
            decode_parquet_file_kvtd(buf, Some(format_metadata), metrics)?
        }
    };

    let ret = BlobTraceBatchPart {
        desc: metadata.desc.map_or_else(
            || {
                Description::new(
                    Antichain::from_elem(T::minimum()),
                    Antichain::from_elem(T::minimum()),
                    Antichain::from_elem(T::minimum()),
                )
            },
            |x| x.into(),
        ),
        index: metadata.index,
        updates,
    };
    ret.validate()?;
    Ok(ret)
}

/// Encodes [`BlobTraceUpdates`] to Parquet using the [`parquet`] crate.
pub fn encode_parquet_kvtd<W: Write + Send>(
    w: &mut W,
    inline_base64: String,
    updates: &BlobTraceUpdates,
    metrics: &ColumnarMetrics,
    cfg: &EncodingConfig,
) -> Result<Vec<RowGroupStats>, Error> {
    let metadata = KeyValue::new(INLINE_METADATA_KEY.to_string(), inline_base64);

    // Note: most of these settings are the defaults from `arrow2` which we
    // previously used and maintain until we tune with benchmarking.
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(cfg.use_dictionary)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_compression(cfg.compression.into())
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(1024 * 1024)
        .set_max_row_group_size(cfg.max_row_group_size)
        .set_bloom_filter_enabled(cfg.use_bloom_filter)
        .set_key_value_metadata(Some(vec![metadata]))
        .build();

    let batch = encode_arrow_batch(updates);
    let format = match updates {
        BlobTraceUpdates::Row(_) => "k,v,t,d",
        BlobTraceUpdates::Both(_, _) => "k,v,t,d,k_s,v_s",
        BlobTraceUpdates::Structured { .. } => "t,d,k_s,v_s",
    };

    let primary_keys: Vec<usize> = batch
        .schema()
        .fields
        .iter()
        .enumerate()
        .filter_map(|(i, f)| {
            if f.metadata().get("primary_key").is_some() {
                Some(i)
            } else {
                None
            }
        })
        .collect();

    assert!(primary_keys.len() <= 1);

    let mut writer = ArrowWriter::try_new(w, batch.schema(), Some(properties))?;

    writer.write(&batch)?;

    writer.flush()?;
    let mut row_group_stats = vec![];
    let bytes_written = writer.bytes_written();
    let file_metadata = writer.close()?;
    if cfg.use_bloom_filter {
        for row_group in &file_metadata.row_groups {
            let row_group_offset = row_group.file_offset.expect("row group offset");
            let row_group_size = row_group.total_compressed_size.expect("row group size");
            for (i, column) in row_group.columns.iter().enumerate() {
                let mut blooms = vec![];
                if primary_keys.contains(&i) {
                    let bloom = column
                        .meta_data
                        .as_ref()
                        .and_then(|m| Some((m.bloom_filter_offset, m.bloom_filter_length)));
                    if let Some((Some(offset), Some(size))) = bloom {
                        blooms.push((offset, size));
                    }
                }

                row_group_stats.push(RowGroupStats {
                    offset: row_group_offset,
                    size: row_group_size,
                    blooms,
                });
            }
        }
    }

    report_parquet_metrics(metrics, &file_metadata, bytes_written, format);

    Ok(row_group_stats)
}

/// Decodes [`BlobTraceUpdates`] from a reader, using [`arrow`].
pub fn decode_parquet_file_kvtd(
    r: impl parquet::file::reader::ChunkReader + 'static,
    format_metadata: Option<&ProtoFormatMetadata>,
    metrics: &ColumnarMetrics,
) -> Result<BlobTraceUpdates, Error> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(r)?;

    let row_groups = builder.metadata().row_groups();
    let num_rows = usize::try_from(row_groups.iter().map(|rg| rg.num_rows()).sum::<i64>())
        .map_err(|_| Error::String("found negative rows".to_string()))?;
    let builder = builder.with_batch_size(num_rows);

    let schema = Arc::clone(builder.schema());
    let mut reader = builder.build()?;

    match format_metadata {
        None => {
            let mut ret = Vec::new();
            for batch in reader {
                let batch = batch.map_err(|e| Error::String(e.to_string()))?;
                ret.push(batch);
            }
            if ret.len() != 1 {
                warn!("unexpected number of row groups: {}", ret.len());
            }
            let batch = arrow::compute::concat_batches(&schema, &ret)?;
            let updates = decode_arrow_batch(&batch, metrics).map_err(|e| e.to_string())?;
            Ok(updates)
        }
        Some(ProtoFormatMetadata::StructuredMigration(v @ 1..=3)) => {
            let mut batches: Vec<_> = reader
                .into_iter()
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| Error::String(e.to_string()))?;

            batches.iter_mut().for_each(|batch| {
                // Version 1 is a deprecated format so we just ignored the k_s and v_s columns.
                if *v == 1 && batch.num_columns() > 4 {
                    batch.project(&[0, 1, 2, 3]).unwrap();
                }
            });

            let batch = arrow::compute::concat_batches(&schema, &batches)?;

            let updates = decode_arrow_batch(&batch, metrics).map_err(|e| e.to_string())?;
            Ok(updates)
        }
        unknown => Err(format!("unkown ProtoFormatMetadata, {unknown:?}"))?,
    }
}

/// Best effort reporting of metrics from the resulting [`parquet::format::FileMetaData`] returned
/// from the [`ArrowWriter`].
fn report_parquet_metrics(
    metrics: &ColumnarMetrics,
    metadata: &parquet::format::FileMetaData,
    bytes_written: usize,
    format: &'static str,
) {
    metrics
        .parquet()
        .num_row_groups
        .with_label_values(&[format])
        .inc_by(u64::cast_from(metadata.row_groups.len()));
    metrics
        .parquet()
        .encoded_size
        .with_label_values(&[format])
        .inc_by(u64::cast_from(bytes_written));

    let report_column_size = |col_name: &str, metrics: &ParquetColumnMetrics| {
        let (uncomp, comp) = metadata
            .row_groups
            .iter()
            .map(|row_group| row_group.columns.iter())
            .flatten()
            .filter_map(|col_chunk| col_chunk.meta_data.as_ref())
            .filter(|m| m.path_in_schema.first().map(|s| s.as_str()) == Some(col_name))
            .map(|m| (m.total_uncompressed_size, m.total_compressed_size))
            .fold((0, 0), |(tot_u, tot_c), (u, c)| (tot_u + u, tot_c + c));

        let uncomp = uncomp.try_into().unwrap_or(0u64);
        let comp = comp.try_into().unwrap_or(0u64);

        metrics.report_sizes(uncomp, comp);
    };

    report_column_size("k", &metrics.parquet().k_metrics);
    report_column_size("v", &metrics.parquet().v_metrics);
    report_column_size("t", &metrics.parquet().t_metrics);
    report_column_size("d", &metrics.parquet().d_metrics);
    report_column_size("k_s", &metrics.parquet().k_s_metrics);
    report_column_size("v_s", &metrics.parquet().v_s_metrics);
}
