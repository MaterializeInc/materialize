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

use differential_dataflow::trace::Description;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_persist_types::parquet::EncodingConfig;
use mz_persist_types::Codec64;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use timely::progress::{Antichain, Timestamp};
use tracing::warn;

use crate::error::Error;
use crate::gen::persist::proto_batch_part_inline::FormatMetadata as ProtoFormatMetadata;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::columnar::arrow::{decode_arrow_batch, encode_arrow_batch};
use crate::indexed::encoding::{
    decode_trace_inline_meta, encode_trace_inline_meta, BlobTraceBatchPart, BlobTraceUpdates,
};
use crate::metrics::{ColumnarMetrics, ParquetColumnMetrics};

const INLINE_METADATA_KEY: &str = "MZ:inline";

/// Encodes a [`BlobTraceBatchPart`] into the Parquet format.
pub fn encode_trace_parquet<W: Write + Send, T: Timestamp + Codec64>(
    w: &mut W,
    batch: &BlobTraceBatchPart<T>,
    metrics: &ColumnarMetrics,
    cfg: &EncodingConfig,
) -> Result<(), Error> {
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
            return Err("ArrowKVTD format not supported in parquet".into())
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
) -> Result<(), Error> {
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
        .set_max_row_group_size(usize::MAX)
        .set_key_value_metadata(Some(vec![metadata]))
        .build();

    let batch = encode_arrow_batch(updates);
    let format = match updates {
        BlobTraceUpdates::Row(_) => "k,v,t,d",
        BlobTraceUpdates::Both(_, _) => "k,v,t,d,k_s,v_s",
        BlobTraceUpdates::Structured { .. } => "t,d,k_s,v_s",
    };

    let mut writer = ArrowWriter::try_new(w, batch.schema(), Some(properties))?;
    writer.write(&batch)?;

    writer.flush()?;
    let bytes_written = writer.bytes_written();
    let file_metadata = writer.close()?;

    report_parquet_metrics(metrics, &file_metadata, bytes_written, format);

    Ok(())
}

/// Decodes [`BlobTraceUpdates`] from a reader, using [`arrow`].
pub fn decode_parquet_file_kvtd(
    r: impl parquet::file::reader::ChunkReader + 'static,
    format_metadata: Option<&ProtoFormatMetadata>,
    metrics: &ColumnarMetrics,
) -> Result<BlobTraceUpdates, Error> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(r)?;

    // To match arrow2, we default the batch size to the number of rows in the RowGroup.
    let row_groups = builder.metadata().row_groups();
    if row_groups.len() > 1 {
        return Err(Error::String("found more than 1 RowGroup".to_string()));
    }
    let num_rows = usize::try_from(row_groups[0].num_rows())
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
            let batch = ::arrow::compute::concat_batches(&schema, &ret)?;
            let updates = decode_arrow_batch(&batch, metrics).map_err(|e| e.to_string())?;
            Ok(updates)
        }
        Some(ProtoFormatMetadata::StructuredMigration(v @ 1..=3)) => {
            let mut batch = reader
                .next()
                .ok_or_else(|| Error::String("found empty batch".to_string()))??;

            // We enforce an invariant that we have a single RowGroup.
            if reader.next().is_some() {
                return Err(Error::String("found more than one RowGroup".to_string()));
            }

            // Version 1 is a deprecated format so we just ignored the k_s and v_s columns.
            if *v == 1 && batch.num_columns() > 4 {
                batch = batch.project(&[0, 1, 2, 3])?;
            }

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
