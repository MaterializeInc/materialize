// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apache Parquet encodings and utils for persist data

use std::io::{Read, Seek, Write};
use std::sync::Arc;

use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader};
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, FileWriter, KeyValue, RowGroupIterator, Version, WriteOptions,
};
use differential_dataflow::trace::Description;
use mz_dyncfg::Config;
use mz_ore::bytes::SegmentedBytes;
use mz_persist_types::Codec64;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::columnar::arrow::{
    decode_arrow_batch_kvtd, decode_arrow_batch_kvtd_arrow_rs, encode_arrow_batch_kvtd,
    encode_arrow_batch_kvtd_arrow_rs, SCHEMA_ARROW2_KVTD, SCHEMA_ARROW_RS_KVTD,
};
use crate::indexed::columnar::ColumnarRecords;
use crate::indexed::encoding::{
    decode_trace_inline_meta, encode_trace_inline_meta, BlobTraceBatchPart,
};
use crate::metrics::ColumnarMetrics;

#[derive(Debug, Copy, Clone)]
enum UseArrowRsLibrary {
    Off,
    Read,
    ReadAndWrite,
}

impl UseArrowRsLibrary {
    /// Returns whether or not we should use [`arrow`] for reads.
    fn for_reads(&self) -> bool {
        matches!(
            self,
            UseArrowRsLibrary::Read | UseArrowRsLibrary::ReadAndWrite
        )
    }

    /// Returns whether or not we should use [`arrow`] for writes.
    fn for_writes(&self) -> bool {
        matches!(self, UseArrowRsLibrary::ReadAndWrite)
    }

    /// Parses the provided string, returns [`UseArrowRsLibrary::Off`] on any failure.
    fn from_str(s: &str) -> UseArrowRsLibrary {
        match s.to_lowercase().as_str() {
            "read" => UseArrowRsLibrary::Read,
            "read_and_write" => UseArrowRsLibrary::ReadAndWrite,
            "off" => UseArrowRsLibrary::Off,
            val => {
                // Complain loudly if the value is not what we expect.
                tracing::error!(?val, "unrecognized value for persist_use_arrow_rs_library");
                UseArrowRsLibrary::Off
            }
        }
    }

    /// Returns a string representation for [`UseArrowRsLibrary`].
    const fn to_str(self) -> &'static str {
        match self {
            UseArrowRsLibrary::Off => "off",
            UseArrowRsLibrary::Read => "read",
            UseArrowRsLibrary::ReadAndWrite => "read_and_write",
        }
    }
}

pub(crate) const USE_ARROW_RS_LIBRARY: Config<&str> = Config::new(
    "persist_use_arrow_rs_library",
    UseArrowRsLibrary::Off.to_str(),
    "'off' by default, providing 'read' will use the newer arrow-rs library for reads, 'read_and_write' will use it for both reads and writes.",
);

const INLINE_METADATA_KEY: &str = "MZ:inline";

/// Encodes an BlobTraceBatchPart into the Parquet format.
pub fn encode_trace_parquet<W: Write + Send, T: Timestamp + Codec64>(
    w: &mut W,
    batch: &BlobTraceBatchPart<T>,
    metrics: &ColumnarMetrics,
) -> Result<(), Error> {
    // Better to error now than write out an invalid batch.
    batch.validate()?;
    let inline_meta = encode_trace_inline_meta(batch, ProtoBatchFormat::ParquetKvtd);

    let use_arrow_rs = UseArrowRsLibrary::from_str(&USE_ARROW_RS_LIBRARY.get(&metrics.cfg));
    if use_arrow_rs.for_writes() {
        metrics.arrow_metrics.encode_arrow_rs.inc();
        encode_parquet_kvtd_arrow_rs(w, inline_meta, &batch.updates)
    } else {
        metrics.arrow_metrics.encode_arrow2.inc();
        encode_parquet_kvtd(w, inline_meta, &batch.updates)
    }
}

/// Decodes a BlobTraceBatchPart from the Parquet format.
pub fn decode_trace_parquet<T: Timestamp + Codec64>(
    buf: SegmentedBytes,
    metrics: &ColumnarMetrics,
) -> Result<BlobTraceBatchPart<T>, Error> {
    let use_arrow_rs = UseArrowRsLibrary::from_str(&USE_ARROW_RS_LIBRARY.get(&metrics.cfg));
    let (metadata, updates) = if use_arrow_rs.for_reads() {
        let metadata =
            parquet::arrow::arrow_reader::ArrowReaderMetadata::load(&buf, Default::default())?;
        let metadata = metadata
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .as_ref()
            .and_then(|x| x.iter().find(|x| x.key == INLINE_METADATA_KEY));

        let (format, meta) = decode_trace_inline_meta(metadata.and_then(|x| x.value.as_ref()))?;
        let updates = match format {
            ProtoBatchFormat::Unknown => return Err("unknown format".into()),
            ProtoBatchFormat::ArrowKvtd => {
                return Err("ArrowKVTD format not supported in parquet".into())
            }
            ProtoBatchFormat::ParquetKvtd => decode_parquet_file_kvtd_parquet_rs(buf, metrics)?,
        };
        metrics.arrow_metrics.decode_arrow_rs.inc();

        (meta, updates)
    } else {
        let mut reader = buf.reader();
        let metadata = read_metadata(&mut reader).map_err(|err| err.to_string())?;
        let metadata = metadata
            .key_value_metadata()
            .as_ref()
            .and_then(|x| x.iter().find(|x| x.key == INLINE_METADATA_KEY));

        let (format, meta) = decode_trace_inline_meta(metadata.and_then(|x| x.value.as_ref()))?;
        let updates = match format {
            ProtoBatchFormat::Unknown => return Err("unknown format".into()),
            ProtoBatchFormat::ArrowKvtd => {
                return Err("ArrowKVTD format not supported in parquet".into())
            }
            ProtoBatchFormat::ParquetKvtd => decode_parquet_file_kvtd(&mut reader, metrics)?,
        };
        metrics.arrow_metrics.decode_arrow2.inc();

        (meta, updates)
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

fn encode_parquet_kvtd<W: Write>(
    w: &mut W,
    inline_base64: String,
    iter: &[ColumnarRecords],
) -> Result<(), Error> {
    let iter = iter.into_iter().map(|x| Ok(encode_arrow_batch_kvtd(x)));

    let options = WriteOptions {
        write_statistics: false,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None, // use default limit
    };
    let row_groups = RowGroupIterator::try_new(
        iter,
        &SCHEMA_ARROW2_KVTD,
        options,
        vec![
            vec![Encoding::Plain],
            vec![Encoding::Plain],
            vec![Encoding::Plain],
            vec![Encoding::Plain],
        ],
    )?;

    let metadata = vec![KeyValue {
        key: INLINE_METADATA_KEY.into(),
        value: Some(inline_base64),
    }];
    let mut writer = FileWriter::try_new(w, (**SCHEMA_ARROW2_KVTD).clone(), options)?;
    for group in row_groups {
        writer.write(group?).map_err(|err| err.to_string())?;
    }
    writer.end(Some(metadata)).map_err(|err| err.to_string())?;

    Ok(())
}

/// Encodes [`ColumnarRecords`] to Parquet using the [`parquet`] crate.
pub fn encode_parquet_kvtd_arrow_rs<W: Write + Send>(
    w: &mut W,
    inline_base64: String,
    iter: &[ColumnarRecords],
) -> Result<(), Error> {
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, Encoding};
    use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};

    let metadata =
        parquet::file::metadata::KeyValue::new(INLINE_METADATA_KEY.to_string(), inline_base64);

    // Configure our writer to match arrow2 so the blobs can roundtrip.
    let properties = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_compression(Compression::UNCOMPRESSED)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_data_page_size_limit(1024 * 1024)
        .set_max_row_group_size(usize::MAX)
        .set_key_value_metadata(Some(vec![metadata]))
        .build();

    let mut writer = ArrowWriter::try_new(w, Arc::clone(&*SCHEMA_ARROW_RS_KVTD), Some(properties))?;

    let batches = iter.into_iter().map(|c| {
        let cols = encode_arrow_batch_kvtd_arrow_rs(c);
        RecordBatch::try_new(Arc::clone(&*SCHEMA_ARROW_RS_KVTD), cols)
    });

    for batch in batches {
        writer.write(&batch?)?
    }

    writer.flush()?;
    writer.close()?;

    Ok(())
}

/// Decodes [`ColumnarRecords`] from a reader, using [`arrow2`].
pub fn decode_parquet_file_kvtd<R: Read + Seek>(
    r: &mut R,
    metrics: &ColumnarMetrics,
) -> Result<Vec<ColumnarRecords>, Error> {
    let metadata = read_metadata(r)?;
    let schema = infer_schema(&metadata)?;
    let reader = FileReader::new(r, metadata.row_groups, schema, None, None, None);

    let file_schema = reader.schema().fields.as_slice();
    // We're not trying to accept any sort of user created data, so be strict.
    if file_schema != SCHEMA_ARROW2_KVTD.fields {
        return Err(format!(
            "expected arrow schema {:?} got: {:?}",
            SCHEMA_ARROW2_KVTD.fields, file_schema
        )
        .into());
    }

    let mut ret = Vec::new();
    for batch in reader {
        ret.push(decode_arrow_batch_kvtd(&batch?, metrics)?);
    }
    Ok(ret)
}

/// Decodes [`ColumnarRecords`] from a reader, using [`arrow`].
pub fn decode_parquet_file_kvtd_parquet_rs(
    r: impl parquet::file::reader::ChunkReader + 'static,
    metrics: &ColumnarMetrics,
) -> Result<Vec<ColumnarRecords>, Error> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

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
        ret.push(decode_arrow_batch_kvtd_arrow_rs(&batch, metrics)?);
    }
    Ok(ret)
}
