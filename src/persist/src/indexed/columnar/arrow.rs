// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apache Arrow encodings and utils for persist data

use std::collections::HashMap;
use std::io::{Read, Seek, Write};
use std::sync::Arc;

use arrow2::array::{BinaryArray, PrimitiveArray};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::ipc::read::{read_file_metadata, FileMetadata, FileReader};
use arrow2::io::ipc::write::{FileWriter, WriteOptions};
use arrow2::record_batch::RecordBatch;
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
use crate::storage::SeqNo;

lazy_static! {
    /// The Arrow schema we use to encode ((K, V), T, D) tuples.
    pub static ref SCHEMA_ARROW_KVTD: Arc<Schema> = Arc::new(Schema::new(vec![
        Field {
            name: "k".into(),
            data_type: DataType::Binary,
            nullable: false,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: None,
        },
        Field {
            name: "v".into(),
            data_type: DataType::Binary,
            nullable: false,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: None,
        },
        Field {
            name: "t".into(),
            data_type: DataType::UInt64,
            nullable: false,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: None,
        },
        Field {
            name: "d".into(),
            data_type: DataType::Int64,
            nullable: false,
            dict_id: 0,
            dict_is_ordered: false,
            metadata: None,
        },
    ]));
}

const INLINE_METADATA_KEY: &'static str = "MZ:inline";

/// Encodes an BlobUnsealedBatch into the Arrow file format.
///
/// NB: This is currently unused, but it's here because we may want to use it
/// for the local cache and so we can easily compare arrow vs parquet.
pub fn encode_unsealed_arrow<W: Write>(w: &mut W, batch: &BlobUnsealedBatch) -> Result<(), Error> {
    let mut metadata = HashMap::with_capacity(1);
    metadata.insert(
        INLINE_METADATA_KEY.into(),
        encode_unsealed_inline_meta(batch, ProtoBatchFormat::ArrowKvtd),
    );
    let schema = Schema::new_from(SCHEMA_ARROW_KVTD.fields().clone(), metadata);
    let options = WriteOptions { compression: None };
    let mut writer = FileWriter::try_new(w, &schema, options)?;
    for records in batch.updates.iter() {
        writer.write(&encode_arrow_batch_kvtd(records))?;
    }
    writer.finish()?;
    Ok(())
}

/// Encodes an BlobTraceBatchPart into the Arrow file format.
///
/// NB: This is currently unused, but it's here because we may want to use it
/// for the local cache and so we can easily compare arrow vs parquet.
pub fn encode_trace_arrow<W: Write>(w: &mut W, batch: &BlobTraceBatchPart) -> Result<(), Error> {
    let mut metadata = HashMap::with_capacity(1);
    metadata.insert(
        INLINE_METADATA_KEY.into(),
        encode_trace_inline_meta(batch, ProtoBatchFormat::ArrowKvtd),
    );
    let schema = Schema::new_from(SCHEMA_ARROW_KVTD.fields().clone(), metadata);
    let options = WriteOptions { compression: None };
    let mut writer = FileWriter::try_new(w, &schema, options)?;
    for records in batch.updates.iter() {
        writer.write(&encode_arrow_batch_kvtd(&records))?;
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
        decode_unsealed_inline_meta(file_meta.schema().metadata().get(INLINE_METADATA_KEY))?;

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
        decode_trace_inline_meta(file_meta.schema().metadata().get(INLINE_METADATA_KEY))?;

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

    let file_schema = file_reader.schema().fields().as_slice();
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
    for batch in file_reader {
        ret.push(decode_arrow_batch_kvtd(&batch?)?);
    }
    Ok(ret)
}

/// Converts a ColumnarRecords into an arrow [(K, V, T, D)] RecordBatch.
pub fn encode_arrow_batch_kvtd(x: &ColumnarRecords) -> RecordBatch {
    RecordBatch::try_new(
        SCHEMA_ARROW_KVTD.clone(),
        vec![
            Arc::new(BinaryArray::from_data(
                DataType::Binary,
                x.key_offsets.clone(),
                x.key_data.clone(),
                None,
            )),
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
        ],
    )
    .expect("schema matches fields")
}

/// Converts an arrow [(K, V, T, D)] RecordBatch into a ColumnarRecords.
pub fn decode_arrow_batch_kvtd(x: &RecordBatch) -> Result<ColumnarRecords, String> {
    // We're not trying to accept any sort of user created data, so be strict.
    if x.schema().fields() != SCHEMA_ARROW_KVTD.fields() {
        return Err(format!(
            "expected arrow schema {:?} got: {:?}",
            SCHEMA_ARROW_KVTD.fields(),
            x.schema()
        ));
    }

    let columns = x.columns();
    if columns.len() != 4 {
        return Err(format!("expected 4 fields got {}", columns.len()));
    }
    let key_col = x.column(0);
    let val_col = x.column(1);
    let ts_col = x.column(2);
    let diff_col = x.column(3);

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

    let len = x.num_rows();
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
