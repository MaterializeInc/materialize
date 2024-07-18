// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A columnar representation of one blob's worth of data

use std::sync::Arc;

use arrow::array::{Array, Int64Array, PrimitiveArray, StructArray};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{Field, Int64Type};
use mz_ore::assert_none;
use mz_ore::iter::IteratorExt;

use crate::columnar::sealed::{ColumnMut, ColumnRef};
use crate::columnar::{ColumnEncoder, PartEncoder, Schema, Schema2};
use crate::dyn_col::DynColumnRef;
use crate::dyn_struct::{ColumnsRef, DynStructCfg, DynStructCol, DynStructMut, ValidityRef};
use crate::stats::{ColumnarStats, DynStats, StructStats};
use crate::Codec64;

/// A structured columnar representation of one blob's worth of data.
#[derive(Debug)]
pub struct Part {
    len: usize,
    key: DynStructCol,
    val: DynStructCol,
    ts: ScalarBuffer<i64>,
    diff: ScalarBuffer<i64>,
}

impl Part {
    /// The number of updates contained.
    pub fn len(&self) -> usize {
        debug_assert_eq!(self.validate(), Ok(()));
        self.len
    }

    /// Returns a [ColumnsRef] for the key columns.
    pub fn key_ref(&self) -> ColumnsRef {
        self.key.as_ref()
    }

    /// Returns a [ColumnsRef] for the val columns.
    pub fn val_ref(&self) -> ColumnsRef {
        self.val.as_ref()
    }

    /// Computes a [StructStats] for the key columns.
    pub fn key_stats(&self) -> Result<StructStats, String> {
        let stats = self.key.stats(ValidityRef(None))?;
        Ok(stats.some)
    }

    /// Returns an [`arrow`] array representing the `key` column.
    pub fn to_key_arrow(&self) -> Option<(Field, StructArray)> {
        self.key.to_arrow_struct().map(|array| {
            let field = Field::new("k_s", array.data_type().clone(), false);
            (field, array)
        })
    }

    /// Returns an [`arrow`] array representing the `val` column.
    pub fn to_val_arrow(&self) -> Option<(Field, StructArray)> {
        self.val.to_arrow_struct().map(|array| {
            let field = Field::new("v_s", array.data_type().clone(), false);
            (field, array)
        })
    }

    /// Returns [`arrow`] types representing this [`Part`].
    pub fn to_arrow(&self) -> (Vec<Field>, Vec<Arc<dyn Array>>) {
        let (mut fields, mut arrays) = (Vec::new(), Vec::<Arc<dyn Array>>::new());

        {
            // arrow doesn't allow empty struct arrays. To make a future schema
            // migration for <no columns> <-> <one optional column> easier, we
            // model this as a missing column (rather than something like
            // NullArray). This also matches how we'd do the same for nested
            // structs.
            if let Some((field, key_array)) = self.to_key_arrow() {
                fields.push(field);
                arrays.push(Arc::new(key_array));
            }
        }

        {
            // arrow doesn't allow empty struct arrays. To make a future schema
            // migration for <no columns> <-> <one optional column> easier, we
            // model this as a missing column (rather than something like
            // NullArray). This also matches how we'd do the same for nested
            // structs.
            if let Some((field, val_array)) = self.to_val_arrow() {
                fields.push(field);
                arrays.push(Arc::new(val_array));
            }
        }

        {
            let ts = Int64Array::new(self.ts.clone(), None);
            fields.push(Field::new("t", ts.data_type().clone(), false));
            arrays.push(Arc::new(ts));
        }

        {
            let diff = Int64Array::new(self.diff.clone(), None);
            fields.push(Field::new("d", diff.data_type().clone(), false));
            arrays.push(Arc::new(diff));
        }

        (fields, arrays)
    }

    pub(crate) fn from_arrow<K, KS: Schema<K>, V, VS: Schema<V>>(
        key_schema: &KS,
        val_schema: &VS,
        arrays: &[Arc<dyn Array>],
    ) -> Result<Self, String> {
        let key_schema = key_schema.columns();
        let val_schema = val_schema.columns();

        if !arrays.iter().map(|a| a.len()).all_equal() {
            return Err("arrays do not have equal lengths".to_string());
        }
        let len = arrays
            .get(0)
            .ok_or_else(|| "should have at least 3 arrays, found none")
            .map(|a| a.len())?;
        let mut arrays = arrays.into_iter();
        let key = if key_schema.cols.is_empty() {
            None
        } else {
            let key = arrays
                .next()
                .ok_or_else(|| "missing key column".to_owned())?;
            Some(key)
        };
        let val = if val_schema.cols.is_empty() {
            None
        } else {
            let val = arrays
                .next()
                .ok_or_else(|| "missing val column".to_owned())?;
            Some(val)
        };
        let ts = arrays
            .next()
            .ok_or_else(|| "missing ts column".to_owned())?;
        let diff = arrays
            .next()
            .ok_or_else(|| "missing diff column".to_owned())?;
        if let Some(_) = arrays.next() {
            return Err("too many columns".to_owned());
        }

        let key = match key {
            None => DynStructCol::empty(key_schema),
            Some(key) => DynStructCol::from_arrow(key_schema, &*key)?,
        };

        let val = match val {
            None => DynStructCol::empty(val_schema),
            Some(val) => DynStructCol::from_arrow(val_schema, &*val)?,
        };

        let diff = diff
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .ok_or_else(|| {
                format!(
                    "expected diff to be PrimitiveArray<Int64Type> got {:?}",
                    diff.data_type()
                )
            })?;
        assert_none!(diff.logical_nulls());
        let diff = diff.values().clone();

        let ts = ts
            .as_any()
            .downcast_ref::<PrimitiveArray<Int64Type>>()
            .ok_or_else(|| {
                format!(
                    "expected ts to be PrimitiveArray<Int64Type> got {:?}",
                    ts.data_type()
                )
            })?;
        assert_none!(ts.logical_nulls());
        let ts = ts.values().clone();

        let part = Part {
            len,
            key,
            val,
            ts,
            diff,
        };
        let () = part.validate()?;
        Ok(part)
    }

    fn validate(&self) -> Result<(), String> {
        let () = self.key.validate()?;
        if !self.key.cols.is_empty() && self.len != self.key.len() {
            return Err(format!(
                "key len {} didn't match part len {}",
                self.key.len(),
                self.len
            ));
        }
        let () = self.val.validate()?;
        if !self.val.cols.is_empty() && self.len != self.val.len() {
            return Err(format!(
                "val len {} didn't match part len {}",
                self.val.len(),
                self.len
            ));
        }
        if self.len != self.ts.len() {
            return Err(format!(
                "ts col len {} didn't match part len {}",
                self.ts.len(),
                self.len
            ));
        }
        if self.len != self.diff.len() {
            return Err(format!(
                "diff col len {} didn't match part len {}",
                self.diff.len(),
                self.len
            ));
        }
        // TODO: Also validate the col types match schema.
        Ok(())
    }
}

/// An in-progress columnar constructor for one blob's worth of data.
#[derive(Debug)]
pub struct PartBuilder<K, KS: Schema<K>, V, VS: Schema<V>> {
    /// Configuration for the `key` column.
    key_cfg: DynStructCfg,
    /// Encoder for the `key` column.
    key_encoder: KS::Encoder,
    /// Configuration for the `val` column.
    val_cfg: DynStructCfg,
    /// Encoder for the val column.
    val_encoder: VS::Encoder,

    /// The ts column.
    ts: Codec64Mut,
    /// The diff column.
    diff: Codec64Mut,
}

impl<K, KS: Schema<K>, V, VS: Schema<V>> PartBuilder<K, KS, V, VS> {
    /// Returns a new [`PartBuilder`] that can be used to build a [`Part`].
    pub fn new(key_schema: &KS, val_schema: &VS) -> Result<Self, String> {
        let key = DynStructMut::new(&key_schema.columns());
        let key_cfg = key.cfg().clone();
        let key_encoder = key_schema.encoder(key.as_mut())?;

        let val = DynStructMut::new(&val_schema.columns());
        let val_cfg = val.cfg().clone();
        let val_encoder = val_schema.encoder(val.as_mut())?;

        let ts = Codec64Mut(Vec::new());
        let diff = Codec64Mut(Vec::new());

        let builder = PartBuilder {
            key_cfg,
            key_encoder,
            val_cfg,
            val_encoder,
            ts,
            diff,
        };

        Ok(builder)
    }

    /// Push a new row onto this [`PartBuilder`].
    pub fn push<T: Codec64, D: Codec64>(&mut self, k: &K, v: &V, t: T, d: D) {
        self.key_encoder.encode(k);
        self.val_encoder.encode(v);
        self.ts.push(t);
        self.diff.push(d);
    }

    /// Consumes self returning a [`Part`].
    pub fn finish(self) -> Part {
        let Self {
            key_cfg,
            key_encoder,
            val_cfg,
            val_encoder,
            ts,
            diff,
        } = self;

        let (key_len, key_cols) = key_encoder.finish();
        let (val_len, val_cols) = val_encoder.finish();

        assert!(key_len == val_len);
        assert!(key_len == ts.len());
        assert!(key_len == diff.len());

        let key = DynStructCol {
            len: key_len,
            cfg: key_cfg,
            validity: None,
            cols: key_cols.into_iter().map(DynColumnRef::from).collect(),
        };

        let val = DynStructCol {
            len: val_len,
            cfg: val_cfg,
            validity: None,
            cols: val_cols.into_iter().map(DynColumnRef::from).collect(),
        };

        Part {
            len: key_len,
            key,
            val,
            ts: ScalarBuffer::from(ts.0),
            diff: ScalarBuffer::from(diff.0),
        }
    }
}

/// A structured columnar representation of one blob's worth of data.
pub struct Part2 {
    /// The 'k' values from a Part, generally `SourceData`.
    pub key: Arc<dyn Array>,
    /// Statistics for the `key` values.
    pub key_stats: ColumnarStats,
    /// The 'v' values from a Part, generally `()`.
    pub val: Arc<dyn Array>,
    /// Statistics for the `val` values.
    pub val_stats: ColumnarStats,
    /// The `ts` values from a Part.
    pub time: ScalarBuffer<i64>,
    /// The `diff` values from a Part.
    pub diff: ScalarBuffer<i64>,
}

/// A builder for [`Part2`].
pub struct PartBuilder2<K, KS: Schema2<K>, V, VS: Schema2<V>> {
    key: KS::Encoder,
    val: VS::Encoder,
    time: Vec<i64>,
    diff: Vec<i64>,
}

impl<K, KS: Schema2<K>, V, VS: Schema2<V>> PartBuilder2<K, KS, V, VS> {
    /// Returns a new [`PartBuilder2`].
    pub fn new(key_schema: &KS, val_schema: &VS) -> Self {
        let key = key_schema.encoder().unwrap();
        let val = val_schema.encoder().unwrap();
        let time = Vec::new();
        let diff = Vec::new();

        PartBuilder2 {
            key,
            val,
            time,
            diff,
        }
    }

    /// Pushes a new value onto [`PartBuilder2`].
    pub fn push(&mut self, key: &K, val: &V, time: i64, diff: i64) {
        self.key.append(key);
        self.val.append(val);
        self.time.push(time);
        self.diff.push(diff);
    }

    /// Finishes the builder returning a [`Part2`].
    pub fn finish(self) -> Part2 {
        let PartBuilder2 {
            key,
            val,
            time,
            diff,
        } = self;

        let (key_col, key_stats) = key.finish();
        let (val_col, val_stats) = val.finish();
        let time = ScalarBuffer::from(time);
        let diff = ScalarBuffer::from(diff);

        Part2 {
            key: Arc::new(key_col),
            key_stats: key_stats.into_columnar_stats(),
            val: Arc::new(val_col),
            val_stats: val_stats.into_columnar_stats(),
            time,
            diff,
        }
    }
}

/// Mutable access to a column of a [`Codec64`] implementor.
#[derive(Debug)]
pub struct Codec64Mut(Vec<i64>);

impl Codec64Mut {
    /// Returns the length of the column.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Pushes the given value into this column.
    pub fn push<X: Codec64>(&mut self, val: X) {
        self.0.push(i64::from_le_bytes(Codec64::encode(&val)));
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use super::*;
    use crate::codec_impls::UnitSchema;

    // Make sure that the API structs are Sync + Send, so that they can be used in async tasks.
    // NOTE: This is a compile-time only test. If it compiles, we're good.
    #[allow(unused)]
    fn sync_send() {
        fn is_send_sync<T: Send + Sync>(_: PhantomData<T>) -> bool {
            true
        }

        assert!(is_send_sync::<Part>(PhantomData));
        assert!(is_send_sync::<PartBuilder<(), UnitSchema, (), UnitSchema>>(
            PhantomData
        ));
    }
}
