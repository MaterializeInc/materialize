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

use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader};
use arrow2::io::parquet::write::{
    CompressionOptions, FileWriter, KeyValue, RowGroupIterator, Version, WriteOptions,
};
use differential_dataflow::trace::Description;
use mz_persist_types::Codec64;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::gen::persist::proto_batch_part_inline::FormatMetadata as ProtoFormatMetadata;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::columnar::arrow::{
    decode_arrow_batch_kvtd, decode_arrow_batch_kvtd_ks_vs, encode_arrow_batch_kvtd,
    encode_arrow_batch_kvtd_ks_vs, ENCODINGS_ARROW_KVTD, SCHEMA_ARROW_KVTD,
};
use crate::indexed::encoding::{
    decode_trace_inline_meta, encode_trace_inline_meta, BlobTraceBatchPart, BlobTraceUpdates,
};
use crate::metrics::ColumnarMetrics;

const INLINE_METADATA_KEY: &str = "MZ:inline";

/// Encodes an [`BlobTraceBatchPart`] into the Parquet format.
pub fn encode_trace_parquet<W: Write, T: Timestamp + Codec64>(
    w: &mut W,
    batch: &BlobTraceBatchPart<T>,
) -> Result<(), Error> {
    // Better to error now than write out an invalid batch.
    batch.validate()?;
    encode_parquet_kvtd(w, encode_trace_inline_meta(batch), &batch.updates)
}

/// Decodes a BlobTraceBatchPart from the Parquet format.
pub fn decode_trace_parquet<R: Read + Seek, T: Timestamp + Codec64>(
    r: &mut R,
    metrics: &ColumnarMetrics,
) -> Result<BlobTraceBatchPart<T>, Error> {
    let metadata = read_metadata(r).map_err(|err| err.to_string())?;
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
        ProtoBatchFormat::ParquetKvtd => decode_parquet_file_kvtd(r, None, metrics)?,
        ProtoBatchFormat::ParquetStructured => {
            // Even though `format_metadata` is optional, we expect it when our
            // format is ParquetStructured.
            let format_metadata = meta
                .format_metadata
                .ok_or_else(|| "missing field 'format_metadata'".to_string())?;
            decode_parquet_file_kvtd(r, Some(format_metadata), metrics)?
        }
    };

    let ret = BlobTraceBatchPart {
        desc: meta.desc.map_or_else(
            || {
                Description::new(
                    Antichain::from_elem(T::minimum()),
                    Antichain::from_elem(T::minimum()),
                    Antichain::from_elem(T::minimum()),
                )
            },
            |x| x.into(),
        ),
        index: meta.index,
        updates,
    };
    ret.validate()?;
    Ok(ret)
}

fn encode_parquet_kvtd<W: Write>(
    w: &mut W,
    inline_base64: String,
    updates: &BlobTraceUpdates,
) -> Result<(), Error> {
    let (iter, schema, encodings): (Box<dyn Iterator<Item = _>>, _, _) = match updates {
        BlobTraceUpdates::Row(updates) => {
            let iter = updates.into_iter().map(|x| Ok(encode_arrow_batch_kvtd(x)));
            (
                Box::new(iter),
                (**SCHEMA_ARROW_KVTD).clone(),
                ENCODINGS_ARROW_KVTD.clone(),
            )
        }
        BlobTraceUpdates::Both((codec_updates, structured_updates)) => {
            let (fields, encodings, array) =
                encode_arrow_batch_kvtd_ks_vs(codec_updates, structured_updates);
            let updates = std::iter::once(Ok(array));
            let schema = Schema::from(fields);

            (Box::new(updates), schema, encodings)
        }
    };

    let options = WriteOptions {
        write_statistics: false,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None, // use default limit
    };
    let row_groups = RowGroupIterator::try_new(iter.into_iter(), &schema, options, encodings)?;

    let metadata = vec![KeyValue {
        key: INLINE_METADATA_KEY.into(),
        value: Some(inline_base64),
    }];
    let mut writer = FileWriter::try_new(w, schema, options)?;
    for group in row_groups {
        writer.write(group?).map_err(|err| err.to_string())?;
    }
    writer.end(Some(metadata)).map_err(|err| err.to_string())?;

    Ok(())
}

fn decode_parquet_file_kvtd<R: Read + Seek>(
    r: &mut R,
    format_metadata: Option<ProtoFormatMetadata>,
    metrics: &ColumnarMetrics,
) -> Result<BlobTraceUpdates, Error> {
    let metadata = read_metadata(r)?;
    let schema = infer_schema(&metadata)?;
    let row_group_count = metadata.row_groups.len();

    let mut reader = FileReader::new(r, metadata.row_groups, schema, None, None, None);

    let file_schema = reader.schema().fields.as_slice();
    match format_metadata {
        None => {
            // We're not trying to accept any sort of user created data, so be strict.
            if file_schema != SCHEMA_ARROW_KVTD.fields {
                return Err(format!(
                    "expected arrow schema {:?} got: {:?}",
                    SCHEMA_ARROW_KVTD.fields, file_schema
                )
                .into());
            }

            let mut ret = Vec::new();
            for batch in reader {
                let records = decode_arrow_batch_kvtd(&batch?, metrics, 4)?;
                ret.push(records);
            }
            Ok(BlobTraceUpdates::Row(ret))
        }
        Some(ProtoFormatMetadata::StructuredMigration(1)) => {
            // It's an invariant that we only write a single Row Group.
            if row_group_count != 1 {
                return Err(format!("found {row_group_count} row groups").into());
            }
            if file_schema.len() > 6 {
                return Err(
                    format!("expected at most 6 columns, got {}", file_schema.len()).into(),
                );
            }

            // TODO(parkmycar): Add some more validation here.
            let k_s_column = file_schema.iter().position(|field| field.name == "k_s");
            let v_s_column = file_schema.iter().position(|field| field.name == "v_s");

            let batch = reader
                .next()
                .ok_or_else(|| Error::String("found empty batch".to_string()))?;
            assert!(reader.next().is_none());

            let records = decode_arrow_batch_kvtd_ks_vs(&batch?, metrics, k_s_column, v_s_column)?;
            Ok(BlobTraceUpdates::Both(records))
        }
        unknown => Err(format!("unkown ProtoFormatMetadata, {unknown:?}"))?,
    }
}
