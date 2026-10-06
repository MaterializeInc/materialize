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

use arrow::record_batch::{RecordBatch, RecordBatchReader};
use differential_dataflow::trace::Description;
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_persist_types::Codec64;
use mz_persist_types::parquet::EncodingConfig;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder,
};
use parquet::basic::Encoding;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use timely::PartialOrder;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::generated::persist::ProtoBatchFormat;
use crate::generated::persist::proto_batch_part_inline::FormatMetadata as ProtoFormatMetadata;
use crate::indexed::columnar::arrow::{decode_arrow_batch, encode_arrow_batch};
use crate::indexed::encoding::{
    BlobTraceBatchPart, BlobTraceUpdates, decode_trace_inline_meta, encode_trace_inline_meta,
    validate_trace_updates,
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
    let mut reader = BlobTraceBatchPartReader::new(buf, usize::MAX)?;
    let updates = match reader.next_updates(metrics) {
        Some(updates) => updates?,
        None => {
            let empty = RecordBatch::new_empty(reader.reader.schema());
            reader.decode_batch(empty, metrics)?
        }
    };
    // We enforce an invariant that we have a single RowGroup.
    if reader.next_updates(metrics).is_some() {
        return Err(Error::String("found more than one RowGroup".to_string()));
    }
    Ok(BlobTraceBatchPart {
        desc: reader.desc,
        index: reader.index,
        updates,
    })
}

/// Incremental decoder over a parquet-encoded [`BlobTraceBatchPart`].
///
/// Decodes at most `batch_rows` rows per call to [`Self::next_updates`], so
/// only the encoded bytes and one decoded batch are resident at a time. Each
/// batch is validated like [`BlobTraceBatchPart::validate`] validates a whole
/// part.
#[derive(Debug)]
pub struct BlobTraceBatchPartReader<T> {
    desc: Description<T>,
    index: u64,
    num_rows: usize,
    /// Drop the `k_s` and `v_s` columns of the deprecated
    /// `StructuredMigration(1)` format.
    project_v1: bool,
    reader: ParquetRecordBatchReader,
}

impl<T: Timestamp + Codec64> BlobTraceBatchPartReader<T> {
    /// Opens `buf` for decoding in batches of at most `batch_rows` rows.
    ///
    /// A `batch_rows` of 0 is treated as 1.
    pub fn new(buf: SegmentedBytes, batch_rows: usize) -> Result<Self, Error> {
        let metadata = ArrowReaderMetadata::load(&buf, Default::default())?;
        let inline = metadata
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .and_then(|x| x.iter().find(|x| x.key == INLINE_METADATA_KEY));
        let (format, inline) = decode_trace_inline_meta(inline.and_then(|x| x.value.as_ref()))?;
        let project_v1 = match format {
            ProtoBatchFormat::Unknown => return Err("unknown format".into()),
            ProtoBatchFormat::ArrowKvtd => {
                return Err("ArrowKVTD format not supported in parquet".into());
            }
            ProtoBatchFormat::ParquetKvtd => false,
            // Even though `format_metadata` is optional, we expect it when
            // our format is ParquetStructured.
            ProtoBatchFormat::ParquetStructured => match inline.format_metadata {
                None => return Err("missing field 'format_metadata'".into()),
                Some(ProtoFormatMetadata::StructuredMigration(v @ 1..=3)) => v == 1,
                unknown => Err(format!("unkown ProtoFormatMetadata, {unknown:?}"))?,
            },
        };
        let desc = inline.desc.map_or_else(
            || {
                Description::new(
                    Antichain::from_elem(T::minimum()),
                    Antichain::from_elem(T::minimum()),
                    Antichain::from_elem(T::minimum()),
                )
            },
            |x| x.into(),
        );

        // Checked here as well as per batch, since a part without rows yields
        // no batches.
        if PartialOrder::less_equal(desc.upper(), desc.lower()) {
            return Err(format!("invalid desc: {:?}", desc).into());
        }

        let builder = ParquetRecordBatchReaderBuilder::new_with_metadata(buf, metadata);
        let row_groups = builder.metadata().row_groups();
        if row_groups.len() > 1 {
            return Err(Error::String("found more than 1 RowGroup".to_string()));
        }
        let num_rows = match row_groups.first() {
            Some(row_group) => usize::try_from(row_group.num_rows())
                .map_err(|_| Error::String("found negative rows".to_string()))?,
            None => 0,
        };
        let reader = builder
            .with_batch_size(batch_rows.min(num_rows).max(1))
            .build()?;
        Ok(BlobTraceBatchPartReader {
            desc,
            index: inline.index,
            num_rows,
            project_v1,
            reader,
        })
    }

    /// The part's inline description.
    pub fn desc(&self) -> &Description<T> {
        &self.desc
    }

    /// The total number of rows in the part, decoded or not.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Decodes the next batch of rows, or returns `None` once all rows have
    /// been returned.
    pub fn next_updates(
        &mut self,
        metrics: &ColumnarMetrics,
    ) -> Option<Result<BlobTraceUpdates, Error>> {
        let batch = match self.reader.next()? {
            Ok(batch) => batch,
            Err(e) => return Some(Err(Error::String(e.to_string()))),
        };
        Some(self.decode_batch(batch, metrics))
    }

    fn decode_batch(
        &self,
        mut batch: RecordBatch,
        metrics: &ColumnarMetrics,
    ) -> Result<BlobTraceUpdates, Error> {
        // Version 1 is a deprecated format so we just ignored the k_s and v_s columns.
        if self.project_v1 && batch.num_columns() > 4 {
            batch = batch.project(&[0, 1, 2, 3])?;
        }
        let updates = decode_arrow_batch(&batch, metrics).map_err(|e| e.to_string())?;
        validate_trace_updates(&self.desc, &updates)?;
        Ok(updates)
    }
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
        .set_max_row_group_row_count(None)
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

/// Best effort reporting of metrics from the resulting [`parquet::format::FileMetaData`] returned
/// from the [`ArrowWriter`].
fn report_parquet_metrics(
    metrics: &ColumnarMetrics,
    metadata: &ParquetMetaData,
    bytes_written: usize,
    format: &'static str,
) {
    metrics
        .parquet()
        .num_row_groups
        .with_label_values(&[format])
        .inc_by(u64::cast_from(metadata.row_groups().len()));
    metrics
        .parquet()
        .encoded_size
        .with_label_values(&[format])
        .inc_by(u64::cast_from(bytes_written));

    let report_column_size = |col_name: &str, metrics: &ParquetColumnMetrics| {
        let (uncomp, comp) = metadata
            .row_groups()
            .iter()
            .map(|row_group| row_group.columns().iter())
            .flatten()
            .filter(|m| m.column_path().parts().first().map(|s| s.as_str()) == Some(col_name))
            .map(|m| (m.uncompressed_size(), m.compressed_size()))
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
