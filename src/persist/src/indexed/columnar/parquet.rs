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

use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::io::parquet::read::{infer_schema, read_metadata, FileReader};
use arrow2::io::parquet::write::{
    CompressionOptions, DynIter, FileWriter, KeyValue, RowGroupIterator, Version, WriteOptions,
};
use differential_dataflow::trace::Description;
use itertools::Itertools;
use mz_persist_types::Codec64;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::columnar::arrow::{decode_arrow_batch_kvtd, decode_arrow_batch_kvtd_columnar};
use crate::indexed::encoding::{
    decode_trace_inline_meta, encode_trace_inline_meta, BlobTraceBatchPart, BlobTraceUpdates,
};
use crate::metrics::ColumnarMetrics;

const INLINE_METADATA_KEY: &str = "MZ:inline";

/// Encodes an BlobTraceBatchPart into the Parquet format.
pub fn encode_trace_parquet<W: Write, T: Timestamp + Codec64>(
    w: &mut W,
    batch: &BlobTraceBatchPart<T>,
) -> Result<(), Error> {
    // Better to error now than write out an invalid batch.
    batch.validate()?;

    let (row_groups, schema, write_options) = match &batch.updates {
        BlobTraceUpdates::Row(updates) => {
            let (mut fields, mut encodings, arrays): (Vec<_>, Vec<_>, Vec<_>) =
                updates.iter().map(|u| u.to_arrow()).multiunzip();

            assert!(fields.iter().all_equal());
            let Some(fields) = fields.pop() else {
                return Ok(());
            };
            assert!(encodings.iter().all_equal());
            let Some(encodings) = encodings.pop() else {
                return Ok(());
            };

            let schema = Schema::from(fields);
            let parquet_write_options = WriteOptions {
                // We don't write statistics because the data isn't structured in a format
                // Parquet can collect any meaningful insights from.
                write_statistics: false,
                version: Version::V2,
                compression: CompressionOptions::Uncompressed,
                data_pagesize_limit: None,
            };

            let chunks = arrays.into_iter().map(Ok);
            let iter =
                RowGroupIterator::try_new(chunks, &schema, parquet_write_options, encodings)?;
            (DynIter::new(iter), schema, parquet_write_options)
        }
        BlobTraceUpdates::Columnar(part) => {
            let (mut fields, mut encodings, arrays): (Vec<_>, Vec<_>, Vec<_>) =
                part.iter().map(|p| p.to_arrow()).multiunzip();

            assert!(fields.iter().all_equal());
            let Some(fields) = fields.pop() else {
                return Ok(());
            };
            assert!(encodings.iter().all_equal());
            let Some(encodings) = encodings.pop() else {
                return Ok(());
            };

            let schema = Schema::from(fields);

            let parquet_write_options = WriteOptions {
                write_statistics: true,
                version: Version::V2,
                compression: CompressionOptions::Uncompressed,
                data_pagesize_limit: None,
            };

            let chunks = arrays.into_iter().map(Ok);
            let iter =
                RowGroupIterator::try_new(chunks, &schema, parquet_write_options, encodings)?;
            (DynIter::new(iter), schema, parquet_write_options)
        }
        BlobTraceUpdates::Both { codec, parquet } => {
            // Row based updates.
            let (mut row_fields, mut row_encodings, row_arrays): (Vec<_>, Vec<_>, Vec<_>) =
                codec.iter().map(|u| u.to_arrow()).multiunzip();

            assert!(row_fields.iter().all_equal());
            let Some(row_fields) = row_fields.pop() else {
                return Ok(());
            };
            assert!(row_encodings.iter().all_equal());
            let Some(row_encodings) = row_encodings.pop() else {
                return Ok(());
            };

            // Columnar based updates.
            let (mut col_fields, mut col_encodings, col_arrays): (Vec<_>, Vec<_>, Vec<_>) =
                parquet.iter().map(|p| p.to_arrow()).multiunzip();

            assert!(col_fields.iter().all_equal());
            let Some(col_fields) = col_fields.pop() else {
                return Ok(());
            };
            assert!(col_encodings.iter().all_equal());
            let Some(col_encodings) = col_encodings.pop() else {
                return Ok(());
            };

            // TODO(parkmcar): We should probably do a stronger check here?
            let row_len: usize = row_arrays.iter().map(|chunk| chunk.len()).sum();
            let col_len: usize = col_arrays.iter().map(|part| part.len()).sum();
            if row_len != col_len {
                return Err(Error::String(
                    "Row and Column based data are different lengths".to_string(),
                ));
            }

            // Extract the key and value columns from the columnar arrays.
            let mut indexes_to_extract = vec![];

            if let Some(key_idx) = col_fields.iter().position(|field| field.name == "k_c") {
                indexes_to_extract.push(key_idx);
            }
            if let Some(val_idx) = col_fields.iter().position(|field| field.name == "v_c") {
                indexes_to_extract.push(val_idx);
            }

            let col_fields: Vec<_> = col_fields
                .into_iter()
                .enumerate()
                .filter_map(|(idx, field)| indexes_to_extract.contains(&idx).then_some(field))
                .collect();
            let col_encodings: Vec<_> = col_encodings
                .into_iter()
                .enumerate()
                .filter_map(|(idx, encoding)| indexes_to_extract.contains(&idx).then_some(encoding))
                .collect();

            let col_arrays: Vec<Vec<_>> = col_arrays
                .into_iter()
                .map(|chunk| {
                    chunk
                        .into_arrays()
                        .into_iter()
                        .enumerate()
                        .filter_map(|(idx, array)| {
                            indexes_to_extract.contains(&idx).then_some(array)
                        })
                        .collect()
                })
                .collect();

            // Splice our fields, encodings, and arrays together.
            let fields: Vec<_> = row_fields.into_iter().chain(col_fields).collect();
            let encodings: Vec<_> = row_encodings.into_iter().chain(col_encodings).collect();

            assert_eq!(row_arrays.len(), col_arrays.len());
            let arrays: Vec<_> = row_arrays
                .into_iter()
                .map(|chunk| chunk.into_arrays())
                .zip(col_arrays.into_iter())
                .map(|(mut row_arrays, col_arrays)| {
                    row_arrays.extend(col_arrays);
                    row_arrays
                })
                .map(Chunk::try_new)
                .collect();

            let schema = Schema::from(fields);

            let parquet_write_options = WriteOptions {
                write_statistics: false,
                version: Version::V2,
                compression: CompressionOptions::Uncompressed,
                data_pagesize_limit: None,
            };

            let iter = RowGroupIterator::try_new(
                arrays.into_iter(),
                &schema,
                parquet_write_options,
                encodings,
            )?;
            (DynIter::new(iter), schema, parquet_write_options)
        }
    };

    let metadata = vec![KeyValue {
        key: INLINE_METADATA_KEY.into(),
        value: Some(encode_trace_inline_meta(
            batch,
            ProtoBatchFormat::ParquetKvtd,
        )),
    }];
    eprintln!("{schema:?}");
    let mut writer = FileWriter::try_new(w, schema, write_options)?;
    for group in row_groups {
        writer.write(group?).map_err(|err| err.to_string())?;
    }
    writer.end(Some(metadata)).map_err(|err| err.to_string())?;

    Ok(())
}

/// Decodes a BlobTraceBatchPart from the Parquet format.
pub fn decode_trace_parquet<R: Read + Seek, T: Timestamp + Codec64>(
    r: &mut R,
    metrics: &ColumnarMetrics,
) -> Result<BlobTraceBatchPart<T>, Error> {
    let metadata = read_metadata(r).map_err(|err| err.to_string())?;

    let mz_metadata = metadata
        .key_value_metadata()
        .as_ref()
        .and_then(|x| x.iter().find(|x| x.key == INLINE_METADATA_KEY));
    let (format, meta) = decode_trace_inline_meta(mz_metadata.and_then(|x| x.value.as_ref()))?;
    match format {
        ProtoBatchFormat::Unknown => return Err("unknown format".into()),
        ProtoBatchFormat::ArrowKvtd => {
            return Err("ArrowKVTD format not supported in parquet".into())
        }
        ProtoBatchFormat::ParquetKvtd => (),
    };

    let schema = infer_schema(&metadata)?;
    let reader = FileReader::new(r, metadata.row_groups, schema.clone(), None, None, None);

    let col_pos = |name: &str| schema.fields.iter().position(|field| field.name == name);

    // Row based encoding.
    let k_idx = col_pos("k");
    let v_idx = col_pos("v");

    // Column based encoding.
    let k_c_idx = col_pos("k_c");
    let v_c_idx = col_pos("v_c");

    let t_idx = col_pos("t").expect("time always exists");
    let d_idx = col_pos("d").expect("diff always exists");

    // Infer which format we have based on what columns exist in the written Parquet.
    let updates = match (k_idx, v_idx, k_c_idx, v_c_idx) {
        // Both.
        (Some(k_idx), Some(v_idx), Some(k_c_idx), None) => {
            let row_column_indexes = [k_idx, v_idx, t_idx, d_idx];
            let col_column_indexes = [k_c_idx, t_idx, d_idx];

            // TODO(parkmcar): Create a Part from the specified column indexes.
            //
            // What makes this tricky is creating a DynStructCol (which is used for the key and val
            // columns) requires a `schema`, but threading that info to decode is tricky and not
            // clearly necessary since the `arrow2::StructArray` should already have the required
            // info.

            let mut updates = Vec::new();
            let mut parts = Vec::new();
            for batch in reader {
                let batch = batch?;
                updates.push(decode_arrow_batch_kvtd(
                    &batch,
                    &row_column_indexes,
                    metrics,
                )?);
                parts.push(Arc::new(decode_arrow_batch_kvtd_columnar(
                    &batch,
                    &col_column_indexes,
                    metrics,
                )?));
            }

            BlobTraceUpdates::Both {
                codec: updates,
                parquet: parts,
            }
        }
        // Only Row.
        (Some(k_idx), Some(v_idx), None, None) => {
            let column_indexes = [k_idx, v_idx, t_idx, d_idx];

            let mut ret = Vec::new();
            for batch in reader {
                ret.push(decode_arrow_batch_kvtd(&batch?, &column_indexes, metrics)?);
            }

            BlobTraceUpdates::Row(ret)
        }
        // Only Column.
        (None, None, Some(_k_c_idx), None) => {
            unreachable!("TODO create a part")
        }
        _ => {
            return Err(format!(
                "Unexpected schema {schema:?}, {k_idx:?}, {v_idx:?}, {k_c_idx:?}, {v_c_idx:?}"
            )
            .into())
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
