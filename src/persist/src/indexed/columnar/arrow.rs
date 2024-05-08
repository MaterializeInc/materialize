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
use std::convert;
use std::sync::Arc;

use arrow::datatypes::{
    DataType as ArrowRsDataType, Field as ArrowRsField, Schema as ArrowRsSchema,
};
use arrow2::array::{Array, BinaryArray, PrimitiveArray};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use mz_dyncfg::Config;
use mz_ore::bytes::MaybeLgBytes;
use mz_ore::lgbytes::{LgBytes, MetricsRegion};
use once_cell::sync::Lazy;

use crate::indexed::columnar::ColumnarRecords;
use crate::metrics::ColumnarMetrics;

/// The Arrow schema we use to encode ((K, V), T, D) tuples.
///
/// Both Time and Diff are presented externally to persist users as a type
/// parameter that implements [mz_persist_types::Codec64]. Our columnar format
/// intentionally stores them both as i64 columns (as opposed to something like
/// a fixed width binary column) because this allows us additional compression
/// options.
///
/// Also note that we intentionally use an i64 over a u64 for Time. Over the
/// range `[0, i64::MAX]`, the bytes are the same and we've talked at various
/// times about changing Time in mz to an i64. Both millis since unix epoch and
/// nanos since unix epoch easily fit into this range (the latter until some
/// time after year 2200). Using a i64 might be a pessimization for a
/// non-realtime mz source with u64 timestamps in the range `(i64::MAX,
/// u64::MAX]`, but realtime sources are overwhelmingly the common case.
pub static SCHEMA_ARROW2_KVTD: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::from(vec![
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
            data_type: DataType::Int64,
            is_nullable: false,
            metadata: BTreeMap::new(),
        },
        Field {
            name: "d".into(),
            data_type: DataType::Int64,
            is_nullable: false,
            metadata: BTreeMap::new(),
        },
    ]))
});

/// Duplicate of [`SCHEMA_ARROW2_KVTD`] but with [`arrow`] types.
pub static SCHEMA_ARROW_RS_KVTD: Lazy<Arc<ArrowRsSchema>> = Lazy::new(|| {
    let schema = ArrowRsSchema::new(vec![
        ArrowRsField::new("k", ArrowRsDataType::Binary, false),
        ArrowRsField::new("v", ArrowRsDataType::Binary, false),
        ArrowRsField::new("t", ArrowRsDataType::Int64, false),
        ArrowRsField::new("d", ArrowRsDataType::Int64, false),
    ]);
    Arc::new(schema)
});

/// Converts a ColumnarRecords into an arrow [(K, V, T, D)] Chunk.
pub fn encode_arrow_batch_kvtd(x: &ColumnarRecords) -> Chunk<Box<dyn Array>> {
    Chunk::try_new(vec![
        convert::identity::<Box<dyn Array>>(Box::new(BinaryArray::new(
            DataType::Binary,
            (*x.key_offsets)
                .as_ref()
                .to_vec()
                .try_into()
                .expect("valid offsets"),
            x.key_data.as_ref().to_vec().into(),
            None,
        ))),
        Box::new(BinaryArray::new(
            DataType::Binary,
            (*x.val_offsets)
                .as_ref()
                .to_vec()
                .try_into()
                .expect("valid offsets"),
            x.val_data.as_ref().to_vec().into(),
            None,
        )),
        Box::new(PrimitiveArray::new(
            DataType::Int64,
            (*x.timestamps).as_ref().to_vec().into(),
            None,
        )),
        Box::new(PrimitiveArray::new(
            DataType::Int64,
            (*x.diffs).as_ref().to_vec().into(),
            None,
        )),
    ])
    .expect("schema matches fields")
}

/// Converts a [`ColumnarRecords`] into `(K, V, T, D)` [`arrow`] columns.
pub fn encode_arrow_batch_kvtd_arrow_rs(x: &ColumnarRecords) -> Vec<arrow::array::ArrayRef> {
    use arrow::array::{BinaryArray, PrimitiveArray};
    use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};

    let key = BinaryArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from((*x.key_offsets).as_ref().to_vec())),
        Buffer::from_vec(x.key_data.as_ref().to_vec()),
        None,
    )
    .expect("valid key array");
    let val = BinaryArray::try_new(
        OffsetBuffer::new(ScalarBuffer::from((*x.val_offsets).as_ref().to_vec())),
        Buffer::from_vec(x.val_data.as_ref().to_vec()),
        None,
    )
    .expect("valid val array");
    let ts = PrimitiveArray::<arrow::datatypes::Int64Type>::try_new(
        (*x.timestamps).as_ref().to_vec().into(),
        None,
    )
    .expect("valid ts array");
    let diff = PrimitiveArray::<arrow::datatypes::Int64Type>::try_new(
        (*x.diffs).as_ref().to_vec().into(),
        None,
    )
    .expect("valid diff array");

    vec![Arc::new(key), Arc::new(val), Arc::new(ts), Arc::new(diff)]
}

pub(crate) const ENABLE_ARROW_LGALLOC_CC_SIZES: Config<bool> = Config::new(
    "persist_enable_arrow_lgalloc_cc_sizes",
    true,
    "An incident flag to disable copying decoded arrow data into lgalloc on cc sized clusters.",
);

pub(crate) const ENABLE_ARROW_LGALLOC_NONCC_SIZES: Config<bool> = Config::new(
    "persist_enable_arrow_lgalloc_noncc_sizes",
    false,
    "A feature flag to enable copying decoded arrow data into lgalloc on non-cc sized clusters.",
);

/// Converts an [`arrow2`] [(K, V, T, D)] Chunk into a ColumnarRecords.
pub fn decode_arrow_batch_kvtd(
    x: &Chunk<Box<dyn Array>>,
    metrics: &ColumnarMetrics,
) -> Result<ColumnarRecords, String> {
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
        .ok_or_else(|| "column 0 doesn't match schema".to_string())?
        .clone();
    let key_offsets = to_region(key_array.offsets().as_slice(), metrics);
    let key_data = to_region(key_array.values().as_slice(), metrics);
    let val_array = val_col
        .as_any()
        .downcast_ref::<BinaryArray<i32>>()
        .ok_or_else(|| "column 1 doesn't match schema".to_string())?
        .clone();
    let val_offsets = to_region(val_array.offsets().as_slice(), metrics);
    let val_data = to_region(val_array.values().as_slice(), metrics);
    let timestamps = ts_col
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .ok_or_else(|| "column 2 doesn't match schema".to_string())?
        .values();
    let timestamps = to_region(timestamps.as_slice(), metrics);
    let diffs = diff_col
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()
        .ok_or_else(|| "column 3 doesn't match schema".to_string())?
        .values();
    let diffs = to_region(diffs.as_slice(), metrics);

    let len = x.len();
    let ret = ColumnarRecords {
        len,
        key_data: MaybeLgBytes::LgBytes(LgBytes::from(key_data)),
        key_offsets,
        val_data: MaybeLgBytes::LgBytes(LgBytes::from(val_data)),
        val_offsets,
        timestamps,
        diffs,
    };
    ret.borrow().validate()?;
    Ok(ret)
}

/// Converts an [`arrow`] [(K, V, T, D)] [`RecordBatch`] into a [`ColumnarRecords`].
///
/// [`RecordBatch`]: `arrow::array::RecordBatch`
pub fn decode_arrow_batch_kvtd_arrow_rs(
    batch: &arrow::array::RecordBatch,
    metrics: &ColumnarMetrics,
) -> Result<ColumnarRecords, String> {
    use arrow::array::{Array as ArrowRsArray, AsArray};

    if batch.columns().len() != 4 {
        return Err(format!(
            "got wrong number of columns! {}",
            batch.columns().len()
        ));
    }

    let columns = batch.columns();

    let key = &columns[0];
    let val = &columns[1];
    let time = &columns[2];
    let diff = &columns[3];

    let key = key
        .as_binary_opt::<i32>()
        .ok_or_else(|| "key column is wrong type".to_string())?;
    let val = val
        .as_binary_opt::<i32>()
        .ok_or_else(|| "val column is wrong type".to_string())?;
    let time = time
        .as_primitive_opt::<arrow::datatypes::Int64Type>()
        .ok_or_else(|| "time column is wrong type".to_string())?;
    let diff = diff
        .as_primitive_opt::<arrow::datatypes::Int64Type>()
        .ok_or_else(|| "diff column is wrong type".to_string())?;

    let key_offsets = to_region(&key.offsets()[..], metrics);
    let key_data = to_region(key.value_data(), metrics);

    let val_offsets = to_region(&val.offsets()[..], metrics);
    let val_data = to_region(val.value_data(), metrics);

    let timestamps = to_region(&time.values()[..], metrics);
    let diffs = to_region(&diff.values()[..], metrics);

    let len = key.len();
    let ret = ColumnarRecords {
        len,
        key_data: MaybeLgBytes::LgBytes(LgBytes::from(key_data)),
        key_offsets,
        val_data: MaybeLgBytes::LgBytes(LgBytes::from(val_data)),
        val_offsets,
        timestamps,
        diffs,
    };
    ret.borrow().validate()?;

    Ok(ret)
}

/// Copies a slice of data into a possibly disk-backed lgalloc region.
fn to_region<T: Copy>(buf: &[T], metrics: &ColumnarMetrics) -> Arc<MetricsRegion<T>> {
    let use_lgbytes_mmap = if metrics.is_cc_active {
        ENABLE_ARROW_LGALLOC_CC_SIZES.get(&metrics.cfg)
    } else {
        ENABLE_ARROW_LGALLOC_NONCC_SIZES.get(&metrics.cfg)
    };
    if use_lgbytes_mmap {
        Arc::new(metrics.lgbytes_arrow.try_mmap_region(buf))
    } else {
        Arc::new(metrics.lgbytes_arrow.heap_region(buf.to_owned()))
    }
}
