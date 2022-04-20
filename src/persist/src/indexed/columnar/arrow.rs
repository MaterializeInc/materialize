// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apache Arrow encodings and utils for persist data

use std::collections::BTreeMap;
use std::io::{Read, Seek, Write};
use std::sync::Arc;

use arrow2::array::{Array, BinaryArray, PrimitiveArray};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::ipc::read::{read_file_metadata, FileMetadata, FileReader};
use arrow2::io::ipc::write::{FileWriter, WriteOptions};
use differential_dataflow::trace::Description;
use lazy_static::lazy_static;
use timely::progress::{Antichain, Timestamp};

use crate::error::Error;
use crate::gen::persist::ProtoBatchFormat;
use crate::indexed::columnar::ColumnarRecords;
use crate::indexed::encoding::{
    decode_trace_inline_meta, decode_unsealed_inline_meta, encode_trace_inline_meta,
    encode_unsealed_inline_meta, BlobTraceBatchPart, BlobUnsealedBatch,
};
use crate::location::SeqNo;

lazy_static! {
    /// The Arrow schema we use to encode ((K, V), T, D) tuples.
    pub static ref SCHEMA_ARROW_KVTD: Arc<Schema> = Arc::new(Schema::from(vec![
        Field {
            name: "k".into(),
            data_type: DataType::Binary,
            is_nullable: false,
            metadata: BTreeMap::new(),
        },
        Field {
            name: "v".into(),
            data_type: DataType::Binary,
            is_nullable: false,
            metadata: BTreeMap::new(),
        },
        Field {
            name: "t".into(),
            data_type: DataType::UInt64,
            is_nullable: false,
            metadata: BTreeMap::new(),
        },
        Field {
            name: "d".into(),
            data_type: DataType::Int64,
            is_nullable: false,
            metadata: BTreeMap::new(),
        },
    ]));
}

const INLINE_METADATA_KEY: &'static str = "MZ:inline";

/// Encodes an BlobUnsealedBatch into the Arrow file format.
///
/// NB: This is currently unused, but it's here because we may want to use it
/// for the local cache and so we can easily compare arrow vs parquet.
pub fn encode_unsealed_arrow<W: Write>(w: &mut W, batch: &BlobUnsealedBatch) -> Result<(), Error> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        INLINE_METADATA_KEY.into(),
        encode_unsealed_inline_meta(batch, ProtoBatchFormat::ArrowKvtd),
    );
    let schema = Schema::from(SCHEMA_ARROW_KVTD.fields.clone()).with_metadata(metadata);
    let options = WriteOptions { compression: None };
    let mut writer = FileWriter::try_new(w, &schema, None, options)?;
    for records in batch.updates.iter() {
        writer.write(&encode_arrow_batch_kvtd(records), None)?;
    }
    writer.finish()?;
    Ok(())
}

/// Encodes an BlobTraceBatchPart into the Arrow file format.
///
/// NB: This is currently unused, but it's here because we may want to use it
/// for the local cache and so we can easily compare arrow vs parquet.
pub fn encode_trace_arrow<W: Write>(w: &mut W, batch: &BlobTraceBatchPart) -> Result<(), Error> {
    let mut metadata = BTreeMap::new();
    metadata.insert(
        INLINE_METADATA_KEY.into(),
        encode_trace_inline_meta(batch, ProtoBatchFormat::ArrowKvtd),
    );
    let schema = Schema::from(SCHEMA_ARROW_KVTD.fields.clone()).with_metadata(metadata);
    let options = WriteOptions { compression: None };
    let mut writer = FileWriter::try_new(w, &schema, None, options)?;
    for records in batch.updates.iter() {
        writer.write(&encode_arrow_batch_kvtd(&records), None)?;
    }
    writer.finish()?;
    Ok(())
}

/// Decodes a BlobUnsealedBatch from the Arrow file format.
///
/// NB: This is currently unused, but it's here because we may want to use it
/// for the local cache and so we can easily compare arrow vs parquet.
pub fn decode_unsealed_arrow<R: Read + Seek>(r: &mut R) -> Result<BlobUnsealedBatch, Error> {
    let file_meta = read_file_metadata(r)?;
    let (format, meta) =
        decode_unsealed_inline_meta(file_meta.schema.metadata.get(INLINE_METADATA_KEY))?;

    let updates = match format {
        ProtoBatchFormat::Unknown => return Err("unknown format".into()),
        ProtoBatchFormat::ArrowKvtd => decode_arrow_file_kvtd(r, file_meta)?,
        ProtoBatchFormat::ParquetKvtd => {
            return Err("ParquetKvtd format not supported in arrow".into())
        }
    };

    let ret = BlobUnsealedBatch {
        desc: SeqNo(meta.seqno_lower)..SeqNo(meta.seqno_upper),
        updates,
    };
    ret.validate()?;
    Ok(ret)
}

/// Decodes a BlobTraceBatchPart from the Arrow file format.
///
/// NB: This is currently unused, but it's here because we may want to use it
/// for the local cache and so we can easily compare arrow vs parquet.
pub fn decode_trace_arrow<R: Read + Seek>(r: &mut R) -> Result<BlobTraceBatchPart, Error> {
    let file_meta = read_file_metadata(r)?;
    let (format, meta) =
        decode_trace_inline_meta(file_meta.schema.metadata.get(INLINE_METADATA_KEY))?;

    let updates = match format {
        ProtoBatchFormat::Unknown => return Err("unknown format".into()),
        ProtoBatchFormat::ArrowKvtd => decode_arrow_file_kvtd(r, file_meta)?,
        ProtoBatchFormat::ParquetKvtd => {
            return Err("ParquetKvtd format not supported in arrow".into())
        }
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

fn decode_arrow_file_kvtd<R: Read + Seek>(
    r: &mut R,
    file_meta: FileMetadata,
) -> Result<Vec<ColumnarRecords>, Error> {
    let projection = None;
    let file_reader = FileReader::new(r, file_meta, projection);

    let file_schema = file_reader.schema().fields.as_slice();
    // We're not trying to accept any sort of user created data, so be strict.
    if file_schema != SCHEMA_ARROW_KVTD.fields {
        return Err(format!(
            "expected arrow schema {:?} got: {:?}",
            SCHEMA_ARROW_KVTD.fields, file_schema
        )
        .into());
    }

    let mut ret = Vec::new();
    for chunk in file_reader {
        ret.push(decode_arrow_batch_kvtd(&chunk?)?);
    }
    Ok(ret)
}

/// Converts a ColumnarRecords into an arrow [(K, V, T, D)] Chunk.
pub fn encode_arrow_batch_kvtd(x: &ColumnarRecords) -> Chunk<Arc<dyn Array>> {
    Chunk::try_new(vec![
        Arc::new(BinaryArray::from_data(
            DataType::Binary,
            x.key_offsets.clone(),
            x.key_data.clone(),
            None,
        )) as Arc<dyn Array>,
        Arc::new(BinaryArray::from_data(
            DataType::Binary,
            x.val_offsets.clone(),
            x.val_data.clone(),
            None,
        )),
        Arc::new(PrimitiveArray::from_data(
            DataType::UInt64,
            x.timestamps.clone(),
            None,
        )),
        Arc::new(PrimitiveArray::from_data(
            DataType::Int64,
            x.diffs.clone(),
            None,
        )),
    ])
    .expect("schema matches fields")
}

/// Converts an arrow [(K, V, T, D)] Chunk into a ColumnarRecords.
pub fn decode_arrow_batch_kvtd(x: &Chunk<Arc<dyn Array>>) -> Result<ColumnarRecords, String> {
    let columns = x.columns();
    if columns.len() != 4 {
        return Err(format!("expected 4 fields got {}", columns.len()));
    }
    let key_col = &x.columns()[0];
    let val_col = &x.columns()[1];
    let ts_col = &x.columns()[2];
    let diff_col = &x.columns()[3];

    let key_array = key_col
        .as_any()
        .downcast_ref::<BinaryArray<i32>>()
        .ok_or(format!("column 0 doesn't match schema"))?
        .clone();
    let key_offsets = key_array.offsets().clone();
    let key_data = key_array.values().clone();
    let val_array = val_col
        .as_any()
        .downcast_ref::<BinaryArray<i32>>()
        .ok_or(format!("column 1 doesn't match schema"))?
        .clone();
    let val_offsets = val_array.offsets().clone();
    let val_data = val_array.values().clone();
    let timestamps = ts_col
        .as_any()
        .downcast_ref::<PrimitiveArray<u64>>()
        .ok_or(format!("column 2 doesn't match schema"))?
        .values()
        .clone();
    let diffs = diff_col
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .ok_or(format!("column 3 doesn't match schema"))?
        .values()
        .clone();

    let len = x.len();
    let ret = ColumnarRecords {
        len,
        key_data,
        key_offsets,
        val_data,
        val_offsets,
        timestamps,
        diffs,
    };
    ret.borrow().validate()?;
    Ok(ret)
}
