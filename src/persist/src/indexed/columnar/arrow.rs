// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apache Arrow encodings and utils for persist data

use std::sync::Arc;

use arrow::array::{Array, AsArray, BinaryArray, PrimitiveArray};
use arrow::buffer::{Buffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema};
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
pub static SCHEMA_ARROW_RS_KVTD: Lazy<Arc<Schema>> = Lazy::new(|| {
    let schema = Schema::new(vec![
        Field::new("k", DataType::Binary, false),
        Field::new("v", DataType::Binary, false),
        Field::new("t", DataType::Int64, false),
        Field::new("d", DataType::Int64, false),
    ]);
    Arc::new(schema)
});

/// Converts a [`ColumnarRecords`] into `(K, V, T, D)` [`arrow`] columns.
pub fn encode_arrow_batch_kvtd(x: &ColumnarRecords) -> Vec<arrow::array::ArrayRef> {
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

/// Converts an [`arrow`] [(K, V, T, D)] [`RecordBatch`] into a [`ColumnarRecords`].
///
/// [`RecordBatch`]: `arrow::array::RecordBatch`
pub fn decode_arrow_batch_kvtd(
    batch: &arrow::array::RecordBatch,
    metrics: &ColumnarMetrics,
) -> Result<ColumnarRecords, String> {
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
