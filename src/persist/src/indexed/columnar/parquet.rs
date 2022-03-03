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
use differential_dataflow::trace::Description;
use parquet2::compression::Compression;
use parquet2::encoding::Encoding;
use parquet2::metadata::KeyValue;
use parquet2::read::read_metadata;
use parquet2::write::{write_file, Version, WriteOptions};
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::columnar::arrow::{
    decode_arrow_batch_kvtd, encode_arrow_batch_kvtd, SCHEMA_ARROW_KVTD,
};
use crate::indexed::columnar::ColumnarRecords;
use crate::indexed::encoding::{
    decode_trace_inline_meta, decode_unsealed_inline_meta, encode_trace_inline_meta,
    encode_unsealed_inline_meta, BlobTraceBatchPart, BlobUnsealedBatch,
};
use crate::storage::SeqNo;

const INLINE_METADATA_KEY: &'static str = "MZ:inline";

/// Encodes an BlobUnsealedBatch into the Parquet format.
pub fn encode_unsealed_parquet<W: Write>(
    w: &mut W,
    batch: &BlobUnsealedBatch,
) -> Result<(), Error> {
    encode_parquet_kvtd(
        w,
        encode_unsealed_inline_meta(batch, ProtoBatchFormat::ParquetKvtd),
        &batch.updates,
    )
}

/// Encodes an BlobTraceBatchPart into the Parquet format.
pub fn encode_trace_parquet<W: Write>(w: &mut W, batch: &BlobTraceBatchPart) -> Result<(), Error> {
    encode_parquet_kvtd(
        w,
        encode_trace_inline_meta(batch, ProtoBatchFormat::ParquetKvtd),
        &batch.updates,
    )
}

/// Decodes a BlobUnsealedBatch from the Parquet format.
pub fn decode_unsealed_parquet<R: Read + Seek>(r: &mut R) -> Result<BlobUnsealedBatch, Error> {
    let metadata = read_metadata(r).map_err(|err| err.to_string())?;
    let metadata = metadata
        .key_value_metadata()
        .as_ref()
        .and_then(|x| x.iter().find(|x| x.key == INLINE_METADATA_KEY));
    let (format, meta) = decode_unsealed_inline_meta(metadata.and_then(|x| x.value.as_ref()))?;

    let updates = match format {
        ProtoBatchFormat::Unknown => return Err("unknown format".into()),
        ProtoBatchFormat::ArrowKvtd => {
            return Err("ArrowKvtd format not supported in parquet".into())
        }
        ProtoBatchFormat::ParquetKvtd => decode_parquet_file_kvtd(r)?,
    };

    let ret = BlobUnsealedBatch {
        desc: SeqNo(meta.seqno_lower)..SeqNo(meta.seqno_upper),
        updates,
    };
    ret.validate()?;
    Ok(ret)
}

/// Decodes a BlobTraceBatchPart from the Parquet format.
pub fn decode_trace_parquet<R: Read + Seek>(r: &mut R) -> Result<BlobTraceBatchPart, Error> {
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
        ProtoBatchFormat::ParquetKvtd => decode_parquet_file_kvtd(r)?,
    };

    let ret = BlobTraceBatchPart {
        desc: meta.desc.map_or_else(
            || {
                Description::new(
                    Antichain::from_elem(u64::minimum()),
                    Antichain::from_elem(u64::minimum()),
                    Antichain::from_elem(u64::minimum()),
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
    iter: &[ColumnarRecords],
) -> Result<(), Error> {
    let iter = iter.into_iter().map(|x| Ok(encode_arrow_batch_kvtd(x)));

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

fn decode_parquet_file_kvtd<R: Read + Seek>(r: &mut R) -> Result<Vec<ColumnarRecords>, Error> {
    let reader = RecordReader::try_new(r, None, None, None, None)?;

    let file_schema = reader.schema().fields().as_slice();
    // We're not trying to accept any sort of user created data, so be strict.
    if file_schema != SCHEMA_ARROW_KVTD.fields() {
        return Err(format!(
            "expected arrow schema {:?} got: {:?}",
            SCHEMA_ARROW_KVTD.fields(),
            file_schema
        )
        .into());
    }

    let mut ret = Vec::new();
    for batch in reader {
        ret.push(decode_arrow_batch_kvtd(&batch?)?);
    }
    Ok(ret)
}
