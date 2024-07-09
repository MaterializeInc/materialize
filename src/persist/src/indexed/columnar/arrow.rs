// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Apache Arrow encodings and utils for persist data

use std::ptr::NonNull;
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData, ArrayRef, AsArray, BinaryArray, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, Schema, ToByteSlice};
use mz_dyncfg::Config;
use mz_ore::bytes::MaybeLgBytes;
use mz_ore::iter::IteratorExt;
use mz_ore::lgbytes::{LgBytes, MetricsRegion};
use once_cell::sync::Lazy;

use crate::indexed::columnar::{ColumnarRecords, ColumnarRecordsStructuredExt};
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
    let diff = x.diffs.clone();

    vec![Arc::new(key), Arc::new(val), Arc::new(ts), Arc::new(diff)]
}

/// Converts a [`ColumnarRecords`] and [`ColumnarRecordsStructuredExt`] pair
/// (aka [`BlobTraceUpdates::Both`]) into [`arrow::array::Array`]s with columns
/// [(K, V, T, D, K_S, V_S)].
///
/// [`BlobTraceUpdates::Both`]: crate::indexed::encoding::BlobTraceUpdates::Both
pub fn encode_arrow_batch_kvtd_ks_vs(
    records: &ColumnarRecords,
    structured: &ColumnarRecordsStructuredExt,
) -> (Vec<Arc<Field>>, Vec<Arc<dyn Array>>) {
    let mut fields: Vec<_> = (*SCHEMA_ARROW_RS_KVTD).fields().iter().cloned().collect();
    let mut arrays = encode_arrow_batch_kvtd(records);

    if let Some(key_array) = &structured.key {
        let key_field = Field::new("k_s", key_array.data_type().clone(), false);

        fields.push(Arc::new(key_field));
        arrays.push(Arc::clone(key_array));
    }

    if let Some(val_array) = &structured.val {
        let val_field = Field::new("v_s", val_array.data_type().clone(), false);

        fields.push(Arc::new(val_field));
        arrays.push(Arc::clone(val_array));
    }

    (fields, arrays)
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

fn realloc_data(data: ArrayData, metrics: &ColumnarMetrics) -> ArrayData {
    // NB: we don't have type information for each buffer, so we don't request any particular
    // alignment. Thankfully, lgalloc aligns everything to the page boundary. Arrow validates
    // the alignment of the provided buffers, so if this ever changed, `build` would return
    // an error below.
    let buffers = data
        .buffers()
        .iter()
        .map(|b| realloc_slice(b, metrics))
        .collect();
    let child_data = data
        .child_data()
        .iter()
        .cloned()
        .map(|d| realloc_data(d, metrics))
        .collect();
    let nulls = data.nulls().map(|n| {
        let buffer = realloc_slice(n.buffer(), metrics);
        NullBuffer::new(BooleanBuffer::new(buffer, n.offset(), n.len()))
    });

    data.into_builder()
        .buffers(buffers)
        .child_data(child_data)
        .nulls(nulls)
        .build()
        .expect("reconstructing valid arrow array")
}
fn realloc_any(array: &ArrayRef, metrics: &ColumnarMetrics) -> ArrayRef {
    let data = array.to_data();
    let data = realloc_data(data, metrics);
    make_array(data)
}

fn realloc_slice(slice: &[u8], metrics: &ColumnarMetrics) -> Buffer {
    let region = to_region(slice, metrics);
    let bytes: &[u8] = region.as_ref().to_byte_slice();
    let ptr: NonNull<[u8]> = bytes.into();
    // This is fine: see [[NonNull::as_non_null_ptr]] for an unstable version of this usage.
    let ptr: NonNull<u8> = ptr.cast();
    // SAFETY: `ptr` is valid for `len` bytes, and kept alive as long as `region` lives.
    unsafe { Buffer::from_custom_allocation(ptr, bytes.len(), Arc::new(region)) }
}

/// Converts an [`arrow`] [(K, V, T, D)] [`RecordBatch`] into a [`ColumnarRecords`].
///
/// [`RecordBatch`]: `arrow::array::RecordBatch`
pub fn decode_arrow_batch_kvtd(
    columns: &[Arc<dyn Array>],
    metrics: &ColumnarMetrics,
) -> Result<ColumnarRecords, String> {
    let (key_col, val_col, ts_col, diff_col) = match &columns {
        x @ &[k, v, t, d] => {
            // The columns need to all have the same logical length.
            if !x.iter().map(|col| col.len()).all_equal() {
                return Err(format!(
                    "columns don't all have equal length {k_len}, {v_len}, {t_len}, {d_len}",
                    k_len = k.len(),
                    v_len = v.len(),
                    t_len = t.len(),
                    d_len = d.len()
                ));
            }

            (k, v, t, d)
        }
        _ => return Err(format!("expected 4 columns got {}", columns.len())),
    };

    let key_col = realloc_any(&key_col, metrics);
    let key = key_col
        .as_binary_opt::<i32>()
        .ok_or_else(|| "key column is wrong type".to_string())?;
    let val = val_col
        .as_binary_opt::<i32>()
        .ok_or_else(|| "val column is wrong type".to_string())?;
    let time = ts_col
        .as_primitive_opt::<arrow::datatypes::Int64Type>()
        .ok_or_else(|| "time column is wrong type".to_string())?;
    let diff_col = realloc_any(&diff_col, metrics);
    let diffs = diff_col
        .as_primitive_opt::<arrow::datatypes::Int64Type>()
        .ok_or_else(|| "diff column is wrong type".to_string())?;

    let key_offsets = Arc::new(to_region(&key.offsets()[..], metrics));
    let key_data = Arc::new(to_region(key.value_data(), metrics));

    let val_offsets = Arc::new(to_region(&val.offsets()[..], metrics));
    let val_data = Arc::new(to_region(val.value_data(), metrics));

    let timestamps = Arc::new(to_region(&time.values()[..], metrics));

    let len = key.len();
    let ret = ColumnarRecords {
        len,
        key_data: MaybeLgBytes::LgBytes(LgBytes::from(key_data)),
        key_offsets,
        val_data: MaybeLgBytes::LgBytes(LgBytes::from(val_data)),
        val_offsets,
        timestamps,
        diffs: diffs.clone(),
    };
    ret.borrow().validate()?;

    Ok(ret)
}

/// Converts an arrow [(K, V, T, D)] Chunk into a ColumnarRecords.
pub fn decode_arrow_batch_kvtd_ks_vs(
    cols: &[Arc<dyn Array>],
    maybe_key_col: Option<Arc<dyn Array>>,
    maybe_val_col: Option<Arc<dyn Array>>,
    metrics: &ColumnarMetrics,
) -> Result<(ColumnarRecords, ColumnarRecordsStructuredExt), String> {
    let same_length = cols
        .iter()
        .map(|col| col.as_ref())
        .chain(maybe_key_col.as_deref().into_iter())
        .chain(maybe_val_col.as_deref().into_iter())
        .map(|col| col.len())
        .all_equal();
    if !same_length {
        return Err("not all columns (included structured) have the same length".to_string());
    }

    // We always have (K, V, T, D) columns.
    let primary_records = decode_arrow_batch_kvtd(cols, metrics)?;
    let structured_ext = ColumnarRecordsStructuredExt {
        key: maybe_key_col,
        val: maybe_val_col,
    };

    Ok((primary_records, structured_ext))
}

/// Copies a slice of data into a possibly disk-backed lgalloc region.
fn to_region<T: Copy>(buf: &[T], metrics: &ColumnarMetrics) -> MetricsRegion<T> {
    let use_lgbytes_mmap = if metrics.is_cc_active {
        ENABLE_ARROW_LGALLOC_CC_SIZES.get(&metrics.cfg)
    } else {
        ENABLE_ARROW_LGALLOC_NONCC_SIZES.get(&metrics.cfg)
    };
    if use_lgbytes_mmap {
        metrics.lgbytes_arrow.try_mmap_region(buf)
    } else {
        metrics.lgbytes_arrow.heap_region(buf.to_owned())
    }
}
