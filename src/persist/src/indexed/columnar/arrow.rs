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

use arrow::array::{make_array, Array, ArrayData, ArrayRef, AsArray, RecordBatch};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::{ToByteSlice};
use mz_dyncfg::Config;
use mz_ore::iter::IteratorExt;

use crate::indexed::columnar::{ColumnarRecords, ColumnarRecordsStructuredExt};
use crate::indexed::encoding::BlobTraceUpdates;
use crate::metrics::ColumnarMetrics;

/// Converts a [`ColumnarRecords`] into [`arrow`] columns.
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
pub fn encode_arrow_batch(updates: &BlobTraceUpdates) -> RecordBatch {
    fn array_ref<A: Array + Clone + 'static>(a: &A) -> ArrayRef {
        Arc::new(a.clone())
    }
    // For historical reasons, the codec-encoded columns are placed before T/D,
    // and the structured-encoding columns are placed after.
    let kv = updates
        .records()
        .into_iter()
        .flat_map(|x| [("k", array_ref(&x.key_data)), ("v", array_ref(&x.val_data))]);
    let td = [
        ("t", array_ref(updates.timestamps())),
        ("d", array_ref(updates.diffs())),
    ];
    let ks_vs = updates
        .structured()
        .into_iter()
        .flat_map(|x| [("k_s", Arc::clone(&x.key)), ("v_s", Arc::clone(&x.val))]);

    // We expect all the top-level fields to be fully defined.
    let fields = kv.chain(td).chain(ks_vs).map(|(f, a)| (f, a, false));
    RecordBatch::try_from_iter_with_nullable(fields).expect("valid field definitions")
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

fn realloc_data(data: ArrayData, nullable: bool, metrics: &ColumnarMetrics) -> ArrayData {
    // NB: Arrow generally aligns buffers very coarsely: see arrow::alloc::ALIGNMENT.
    // However, lgalloc aligns buffers even more coarsely - to the page boundary -
    // so we never expect alignment issues in practice. If that changes, build()
    // will return an error below, as it does for all invalid data.
    let buffers = data
        .buffers()
        .iter()
        .map(|b| realloc_buffer(b, metrics))
        .collect();
    let child_data = {
        let field_iter = mz_persist_types::arrow::fields_for_type(data.data_type()).iter();
        let child_iter = data.child_data().iter();
        field_iter
            .zip(child_iter)
            .map(|(f, d)| realloc_data(d.clone(), f.is_nullable(), metrics))
            .collect()
    };
    let nulls = if nullable {
        data.nulls().map(|n| {
            let buffer = realloc_buffer(n.buffer(), metrics);
            NullBuffer::new(BooleanBuffer::new(buffer, n.offset(), n.len()))
        })
    } else {
        if data.nulls().is_some() {
            // This is a workaround for: https://github.com/apache/arrow-rs/issues/6510
            // It should always be safe to drop the null buffer for a non-nullable field, since
            // any nulls cannot possibly represent real data and thus must be masked off at
            // some higher level. We always realloc data we get back from parquet, so this is
            // a convenient and efficient place to do the rewrite.
            // Why does this help? Parquet decoding can generate nulls in non-nullable fields
            // that are only masked by eg. a grandparent, not the direct parent... but some arrow
            // code expects the parent to mask any nulls in its non-nullable children. Dropping
            // the buffer here prevents those validations from failing. (Top-level arrays are always
            // marked nullable, but since they don't have parents that's not a problem either.)
            metrics.parquet.elided_null_buffers.inc();
        }
        None
    };

    // Note that `build` only performs shallow validations, but since we rebuild the array
    // recursively we will have performed the equivalent of `ArrayData::validation_full` on
    // the output.
    data.into_builder()
        .buffers(buffers)
        .child_data(child_data)
        .nulls(nulls)
        .build()
        .expect("reconstructing valid arrow array")
}

/// Re-allocate the backing storage for a specific array using lgalloc, if it's configured.
/// (And hopefully-temporarily work around a parquet decoding issue upstream.)
pub fn realloc_array<A: Array + From<ArrayData>>(array: &A, metrics: &ColumnarMetrics) -> A {
    let data = array.to_data();
    // Top-level arrays are always nullable.
    let data = realloc_data(data, true, metrics);
    A::from(data)
}

/// Re-allocate the backing storage for an array ref using lgalloc, if it's configured.
/// (And hopefully-temporarily work around a parquet decoding issue upstream.)
pub fn realloc_any(array: ArrayRef, metrics: &ColumnarMetrics) -> ArrayRef {
    let data = array.into_data();
    // Top-level arrays are always nullable.
    let data = realloc_data(data, true, metrics);
    make_array(data)
}

fn realloc_buffer(buffer: &Buffer, metrics: &ColumnarMetrics) -> Buffer {
    let use_lgbytes_mmap = if metrics.is_cc_active {
        ENABLE_ARROW_LGALLOC_CC_SIZES.get(&metrics.cfg)
    } else {
        ENABLE_ARROW_LGALLOC_NONCC_SIZES.get(&metrics.cfg)
    };
    let region = if use_lgbytes_mmap {
        metrics
            .lgbytes_arrow
            .try_mmap_region(buffer.as_slice())
            .ok()
    } else {
        None
    };
    let Some(region) = region else {
        return buffer.clone();
    };
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

    let key = key_col
        .as_binary_opt::<i32>()
        .ok_or_else(|| "key column is wrong type".to_string())?;

    let val = val_col
        .as_binary_opt::<i32>()
        .ok_or_else(|| "val column is wrong type".to_string())?;

    let time = ts_col
        .as_primitive_opt::<arrow::datatypes::Int64Type>()
        .ok_or_else(|| "time column is wrong type".to_string())?;

    let diff = diff_col
        .as_primitive_opt::<arrow::datatypes::Int64Type>()
        .ok_or_else(|| "diff column is wrong type".to_string())?;

    let len = key.len();
    let ret = ColumnarRecords {
        len,
        key_data: realloc_array(key, metrics),
        val_data: realloc_array(val, metrics),
        timestamps: realloc_array(time, metrics),
        diffs: realloc_array(diff, metrics),
    };
    ret.validate()?;

    Ok(ret)
}

/// Converts an arrow [(K, V, T, D)] Chunk into a ColumnarRecords.
pub fn decode_arrow_batch_kvtd_ks_vs(
    cols: &[Arc<dyn Array>],
    key_col: Arc<dyn Array>,
    val_col: Arc<dyn Array>,
    metrics: &ColumnarMetrics,
) -> Result<(ColumnarRecords, ColumnarRecordsStructuredExt), String> {
    let same_length = cols
        .iter()
        .map(|col| col.as_ref())
        .chain([&*key_col])
        .chain([&*val_col])
        .map(|col| col.len())
        .all_equal();
    if !same_length {
        return Err("not all columns (included structured) have the same length".to_string());
    }

    // We always have (K, V, T, D) columns.
    let primary_records = decode_arrow_batch_kvtd(cols, metrics)?;
    let structured_ext = ColumnarRecordsStructuredExt {
        key: key_col,
        val: val_col,
    };

    Ok((primary_records, structured_ext))
}
