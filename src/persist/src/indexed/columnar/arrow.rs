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

use arrow2::array::{Array, BinaryArray, PrimitiveArray, StructArray};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::write::Encoding;
use mz_dyncfg::Config;
use mz_ore::bytes::MaybeLgBytes;
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
pub static SCHEMA_ARROW_KVTD: Lazy<Arc<Schema>> = Lazy::new(|| {
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

/// Parquet encodings used for the `[(K, V, T, D)]` format.
pub static ENCODINGS_ARROW_KVTD: Lazy<Vec<Vec<Encoding>>> = Lazy::new(|| {
    vec![
        vec![Encoding::Plain],
        vec![Encoding::Plain],
        vec![Encoding::Plain],
        vec![Encoding::Plain],
    ]
});

/// Converts a [`ColumnarRecords`] (aka [`BlobTraceUpdates::Row`]) into an [`arrow2::chunk::Chunk`]
/// with columns [(K, V, T, D)].
///
/// [`BlobTraceUpdates::Row`]: crate::indexed::encoding::BlobTraceUpdates::Row
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

/// Converts a [`ColumnarRecords`] and [`ColumnarRecordsStructuredExt`] pair
/// (aka [`BlobTraceUpdates::Both`]) into an [`arrow2::chunk::Chunk`] with columns
/// [(K, V, T, D, K_S, V_S)].
///
/// [`BlobTraceUpdates::Both`]: crate::indexed::encoding::BlobTraceUpdates::Both
pub fn encode_arrow_batch_kvtd_ks_vs(
    records: &ColumnarRecords,
    structured: &ColumnarRecordsStructuredExt,
) -> (Vec<Field>, Vec<Vec<Encoding>>, Chunk<Box<dyn Array>>) {
    let codec_chunk = encode_arrow_batch_kvtd(records);
    let mut arrays = codec_chunk.into_arrays();

    let mut fields = SCHEMA_ARROW_KVTD.fields.clone();
    let mut encodings = ENCODINGS_ARROW_KVTD.clone();

    /// Generate a list of encodings for all of the "leaf" arrays.
    ///
    /// TODO(parkmycar): It's probably worth it to use encodings other than "Plan".
    fn generate_encodings(field: &Field, encodings: &mut Vec<Encoding>) {
        match field.data_type() {
            // TODO(parkmycar): We don't use them today, but should also handle Union
            // types here.
            DataType::Struct(fields) => {
                for field in fields {
                    generate_encodings(field, encodings);
                }
            }
            _ => encodings.push(Encoding::Plain),
        }
    }

    if let Some(key_array) = &structured.key {
        let key_field = Field::new("k_s", key_array.data_type().clone(), false);
        let mut key_encodings = Vec::new();
        generate_encodings(&key_field, &mut key_encodings);

        fields.push(key_field);
        encodings.push(key_encodings);
        arrays.push(Box::new(key_array.clone()));
    }

    if let Some(val_array) = &structured.val {
        let val_field = Field::new("v_s", val_array.data_type().clone(), false);
        let mut val_encodings = Vec::new();
        generate_encodings(&val_field, &mut val_encodings);

        fields.push(val_field);
        encodings.push(val_encodings);
        arrays.push(Box::new(val_array.clone()));
    }

    let chunk = Chunk::try_new(arrays).expect("all arrays are the same length");

    (fields, encodings, chunk)
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

/// Converts an arrow [(K, V, T, D)] Chunk into a ColumnarRecords.
pub fn decode_arrow_batch_kvtd(
    x: &Chunk<Box<dyn Array>>,
    metrics: &ColumnarMetrics,
    expected_cols: usize,
) -> Result<ColumnarRecords, String> {
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
    let columns = x.columns();
    if columns.len() != expected_cols {
        return Err(format!(
            "expected {expected_cols} fields got {}",
            columns.len()
        ));
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

/// Converts an arrow [(K, V, T, D)] Chunk into a ColumnarRecords.
pub fn decode_arrow_batch_kvtd_ks_vs(
    x: &Chunk<Box<dyn Array>>,
    metrics: &ColumnarMetrics,
    key_idx: Option<usize>,
    val_idx: Option<usize>,
) -> Result<(ColumnarRecords, ColumnarRecordsStructuredExt), String> {
    // We always expect to have (K, V, T, D), optionally we'll also have K_S and/or V_S.
    let expected_columns = 4 + key_idx.map_or(0, |_| 1) + val_idx.map_or(0, |_| 1);
    let records = decode_arrow_batch_kvtd(x, metrics, expected_columns)?;

    let columns = x.columns();
    if columns.len() != expected_columns {
        return Err(format!(
            "expected {expected_columns} columns, got {}",
            columns.len()
        ));
    }

    let key = if let Some(key_idx) = key_idx {
        let array = columns[key_idx]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| format!("column {key_idx} doesn't match schema"))?
            .clone();
        Some(array)
    } else {
        None
    };

    let val = if let Some(val_idx) = val_idx {
        let array = columns[val_idx]
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| format!("column {val_idx} doesn't match schema"))?
            .clone();
        Some(array)
    } else {
        None
    };

    Ok((records, ColumnarRecordsStructuredExt { key, val }))
}
