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

use arrow2::io::parquet::read::RecordReader;
use arrow2::io::parquet::write::RowGroupIterator;
use arrow2::record_batch::RecordBatch;
use differential_dataflow::trace::Description;
use parquet2::compression::Compression;
use parquet2::encoding::Encoding;
use parquet2::metadata::KeyValue;
use parquet2::write::{write_file, Version, WriteOptions};
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::columnar::arrow::SCHEMA_ARROW_KVTD;
use crate::indexed::columnar::ColumnarRecords;
use crate::indexed::encoding::{
    decode_trace_inline, decode_unsealed_inline, encode_trace_inline_meta,
    encode_unsealed_inline_meta, BlobTraceBatch, BlobUnsealedBatch,
};
use crate::storage::SeqNo;

const INLINE_METADATA_KEY: &'static str = "MZ:meta";

/// Encodes an BlobUnsealedBatch into the Parquet format.
pub fn encode_unsealed_parquet<W: Write>(
    w: &mut W,
    batch: &BlobUnsealedBatch,
) -> Result<(), Error> {
    encode_parquet_kvtd(
        w,
        encode_unsealed_inline_meta(batch, ProtoBatchFormat::ParquetKVTD),
        &batch.updates,
    )
}

/// Encodes an BlobTraceBatch into the Parquet format.
pub fn encode_trace_parquet<W: Write>(w: &mut W, batch: &BlobTraceBatch) -> Result<(), Error> {
    let updates = batch
        .updates
        .iter()
        .map(|((k, v), t, d)| ((k.to_vec(), v.to_vec()), *t, *d))
        .collect();
    encode_parquet_kvtd(
        w,
        encode_trace_inline_meta(batch, ProtoBatchFormat::ParquetKVTD),
        &[updates],
    )
}

/// Decodes a BlobUnsealedBatch from the Parquet format.
pub fn decode_unsealed_parquet<R: Read + Seek>(r: &mut R) -> Result<BlobUnsealedBatch, Error> {
    let reader = RecordReader::try_new(r, None, None, None, None)?;
    let metadata = reader
        .metadata()
        .key_value_metadata()
        .as_ref()
        .and_then(|x| x.iter().find(|x| x.key == INLINE_METADATA_KEY));
    let (format, meta) = decode_unsealed_inline(metadata.and_then(|x| x.value.as_ref()))?;

    let updates = match format {
        ProtoBatchFormat::Unknown => return Err("unknown format".into()),
        ProtoBatchFormat::ArrowKVTD => {
            return Err("ArrowKVTD format not supported in parquet".into())
        }
        ProtoBatchFormat::ParquetKVTD => {
            let mut updates = Vec::new();
            for batch in reader {
                updates.push(ColumnarRecords::try_from(batch?)?);
            }
            updates
        }
    };

    let ret = BlobUnsealedBatch {
        desc: SeqNo(meta.seqno_lower)..SeqNo(meta.seqno_upper),
        updates,
    };
    ret.validate()?;
    Ok(ret)
}

/// Decodes a BlobTraceBatch from the Parquet format.
pub fn decode_trace_parquet<R: Read + Seek>(r: &mut R) -> Result<BlobTraceBatch, Error> {
    let reader = RecordReader::try_new(r, None, None, None, None)?;
    let metadata = reader
        .metadata()
        .key_value_metadata()
        .as_ref()
        .and_then(|x| x.iter().find(|x| x.key == INLINE_METADATA_KEY));
    let (format, meta) = decode_trace_inline(metadata.and_then(|x| x.value.as_ref()))?;

    let updates = match format {
        ProtoBatchFormat::Unknown => return Err("unknown format".into()),
        ProtoBatchFormat::ArrowKVTD => {
            return Err("ArrowKVTD format not supported in parquet".into())
        }
        ProtoBatchFormat::ParquetKVTD => {
            let mut updates = Vec::new();
            for batch in reader {
                for ((k, v), t, d) in ColumnarRecords::try_from(batch?)?.iter() {
                    updates.push(((k.to_vec(), v.to_vec()), t, d));
                }
            }
            updates
        }
    };

    let ret = BlobTraceBatch {
        desc: meta.desc.into_option().map_or_else(
            || {
                Description::new(
                    Antichain::from_elem(u64::minimum()),
                    Antichain::from_elem(u64::minimum()),
                    Antichain::from_elem(u64::minimum()),
                )
            },
            |x| x.into(),
        ),
        updates,
    };
    ret.validate()?;
    Ok(ret)
}

fn encode_parquet_kvtd<W: Write>(
    w: &mut W,
    inline_base64: String,
    batches: &[ColumnarRecords],
) -> Result<(), Error> {
    let iter = batches.iter().map(|x| Ok(RecordBatch::from(x)));

    let schema = SCHEMA_ARROW_KVTD.clone();
    let options = WriteOptions {
        write_statistics: false,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };
    let row_groups = RowGroupIterator::try_new(
        iter,
        &schema,
        options,
        vec![
            Encoding::Plain,
            Encoding::Plain,
            Encoding::Plain,
            Encoding::Plain,
        ],
    )?;

    let parquet_schema = row_groups.parquet_schema().clone();
    let metadata = vec![KeyValue {
        key: INLINE_METADATA_KEY.into(),
        value: Some(inline_base64),
    }];
    write_file(w, row_groups, parquet_schema, options, None, Some(metadata))
        .map_err(|err| err.to_string())?;

    Ok(())
}
