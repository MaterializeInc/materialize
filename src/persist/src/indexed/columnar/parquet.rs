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

use arrow::record_batch::RecordBatch;
use differential_dataflow::trace::Description;
use mz_dyncfg::Config;
use mz_ore::bytes::SegmentedBytes;
use mz_persist_types::Codec64;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::columnar::arrow::{
    decode_arrow_batch_kvtd, encode_arrow_batch_kvtd, SCHEMA_ARROW_RS_KVTD,
};
use crate::indexed::columnar::ColumnarRecords;
use crate::indexed::encoding::{
    decode_trace_inline_meta, encode_trace_inline_meta, BlobTraceBatchPart,
};
use crate::metrics::ColumnarMetrics;

const INLINE_METADATA_KEY: &str = "MZ:inline";

pub(crate) const USE_PARQUET_DELTA_LENGTH_BYTE_ARRAY: Config<bool> = Config::new(
    "persist_use_parquet_delta_length_byte_array",
    false,
    "'false' by default, when 'true' uses Parquet's `DELTA_LENGTH_BYTE_ARRAY` encoding for the 'k' and 'v' columns."
);

/// Encodes an BlobTraceBatchPart into the Parquet format.
pub fn encode_trace_parquet<W: Write + Send, T: Timestamp + Codec64>(
    w: &mut W,
    batch: &BlobTraceBatchPart<T>,
    metrics: &ColumnarMetrics,
) -> Result<(), Error> {
    // Better to error now than write out an invalid batch.
    batch.validate()?;

    let inline_meta = encode_trace_inline_meta(batch, ProtoBatchFormat::ParquetKvtd);
    encode_parquet_kvtd(w, inline_meta, &batch.updates, metrics)
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
        ProtoBatchFormat::ParquetKvtd => decode_parquet_file_kvtd(buf, metrics)?,
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

/// Encodes [`ColumnarRecords`] to Parquet using the [`parquet`] crate.
pub fn encode_parquet_kvtd<W: Write + Send>(
    w: &mut W,
    inline_base64: String,
    iter: &[ColumnarRecords],
    metrics: &ColumnarMetrics,
) -> Result<(), Error> {
    let metadata = KeyValue::new(INLINE_METADATA_KEY.to_string(), inline_base64);

    // We configure our writer to match the defaults from `arrow2` so our blobs
    // can roundtrip. Eventually we should tune these settings.
    let mut properties_builder = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_compression(Compression::UNCOMPRESSED)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(1024 * 1024)
        .set_max_row_group_size(usize::MAX)
        .set_key_value_metadata(Some(vec![metadata]));

    if USE_PARQUET_DELTA_LENGTH_BYTE_ARRAY.get(&metrics.cfg) {
        properties_builder = properties_builder
            .set_column_encoding(
                ColumnPath::new(vec!["k".to_string()]),
                Encoding::DELTA_LENGTH_BYTE_ARRAY,
            )
            .set_column_encoding(
                ColumnPath::new(vec!["v".to_string()]),
                Encoding::DELTA_LENGTH_BYTE_ARRAY,
            );
    }

    let properties = properties_builder.build();

    let mut writer = ArrowWriter::try_new(w, Arc::clone(&*SCHEMA_ARROW_RS_KVTD), Some(properties))?;

    let batches = iter.into_iter().map(|c| {
        let cols = encode_arrow_batch_kvtd(c);
        RecordBatch::try_new(Arc::clone(&*SCHEMA_ARROW_RS_KVTD), cols)
    });

    for batch in batches {
        writer.write(&batch?)?
    }

    writer.flush()?;
    writer.close()?;

    Ok(())
}

/// Decodes [`ColumnarRecords`] from a reader, using [`arrow`].
pub fn decode_parquet_file_kvtd(
    r: impl parquet::file::reader::ChunkReader + 'static,
    metrics: &ColumnarMetrics,
) -> Result<Vec<ColumnarRecords>, Error> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(r)?;

    // To match arrow2, we default the batch size to the number of rows in the RowGroup.
    let row_groups = builder.metadata().row_groups();
    if row_groups.len() > 1 {
        return Err(Error::String("found more than 1 RowGroup".to_string()));
    }
    let num_rows = usize::try_from(row_groups[0].num_rows())
        .map_err(|_| Error::String("found negative rows".to_string()))?;
    let builder = builder.with_batch_size(num_rows);

    // Make sure we have all of the expected columns.
    if SCHEMA_ARROW_RS_KVTD.fields() != builder.schema().fields() {
        return Err(format!("found invalid schema {:?}", builder.schema()).into());
    }

    let reader = builder.build()?;

    let mut ret = Vec::new();
    for batch in reader {
        let batch = batch.map_err(|e| Error::String(e.to_string()))?;
        ret.push(decode_arrow_batch_kvtd(&batch, metrics)?);
    }
    Ok(ret)
}
