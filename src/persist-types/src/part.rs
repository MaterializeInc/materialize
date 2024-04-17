// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A columnar representation of one blob's worth of data

use arrow2::array::{Array, PrimitiveArray};
use arrow2::buffer::Buffer;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType as ArrowLogicalType, Field};
use arrow2::io::parquet::write::Encoding;

use crate::columnar::sealed::{ColumnMut, ColumnRef};
use crate::columnar::{PartEncoder, Schema};
use crate::dyn_col::DynColumnRef;
use crate::dyn_struct::{ColumnsRef, DynStructCfg, DynStructCol, DynStructMut, ValidityRef};
use crate::stats::StructStats;
use crate::Codec64;

/// A structured columnar representation of one blob's worth of data.
#[derive(Debug)]
pub struct Part {
    len: usize,
    key: DynStructCol,
    val: DynStructCol,
    ts: Buffer<i64>,
    diff: Buffer<i64>,
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

    pub(crate) fn to_arrow(&self) -> (Vec<Field>, Vec<Vec<Encoding>>, Chunk<Box<dyn Array>>) {
        let (mut fields, mut encodings, mut arrays) =
            (Vec::new(), Vec::new(), Vec::<Box<dyn Array>>::new());

        {
            // arrow2 doesn't allow empty struct arrays. To make a future schema
            // migration for <no columns> <-> <one optional column> easier, we
            // model this as a missing column (rather than something like
            // NullArray). This also matches how we'd do the same for nested
            // structs.
            if let Some((key_array, key_encodings)) = self.key.to_arrow_struct() {
                fields.push(Field::new("k", key_array.data_type().clone(), false));
                encodings.push(key_encodings);
                arrays.push(Box::new(key_array));
            }
        }

        {
            // arrow2 doesn't allow empty struct arrays. To make a future schema
            // migration for <no columns> <-> <one optional column> easier, we
            // model this as a missing column (rather than something like
            // NullArray). This also matches how we'd do the same for nested
            // structs.
            if let Some((val_array, val_encodings)) = self.val.to_arrow_struct() {
                fields.push(Field::new("v", val_array.data_type().clone(), false));
                encodings.push(val_encodings);
                arrays.push(Box::new(val_array));
            }
        }

        {
            let ts = PrimitiveArray::new(ArrowLogicalType::Int64, self.ts.clone(), None);
            fields.push(Field::new("t", ts.data_type().clone(), false));
            encodings.push(vec![Encoding::Plain]);
            arrays.push(Box::new(ts));
        }

        {
            let diff = PrimitiveArray::new(ArrowLogicalType::Int64, self.diff.clone(), None);
            fields.push(Field::new("d", diff.data_type().clone(), false));
            encodings.push(vec![Encoding::Plain]);
            arrays.push(Box::new(diff));
        }

        (fields, encodings, Chunk::new(arrays))
    }

    pub(crate) fn from_arrow<K, KS: Schema<K>, V, VS: Schema<V>>(
        key_schema: &KS,
        val_schema: &VS,
        chunk: Chunk<Box<dyn Array>>,
    ) -> Result<Self, String> {
        let key_schema = key_schema.columns();
        let val_schema = val_schema.columns();

        let len = chunk.len();
        let mut chunk = chunk.arrays().iter();
        let key = if key_schema.cols.is_empty() {
            None
        } else {
            Some(
                chunk
                    .next()
                    .ok_or_else(|| "missing key column".to_owned())?,
            )
        };
        let val = if val_schema.cols.is_empty() {
            None
        } else {
            Some(
                chunk
                    .next()
                    .ok_or_else(|| "missing val column".to_owned())?,
            )
        };
        let ts = chunk.next().ok_or_else(|| "missing ts column".to_owned())?;
        let diff = chunk
            .next()
            .ok_or_else(|| "missing diff column".to_owned())?;
        if let Some(_) = chunk.next() {
            return Err("too many columns".to_owned());
        }

        let key = match key {
            None => DynStructCol::empty(key_schema),
            Some(key) => DynStructCol::from_arrow(key_schema, key)?,
        };

        let val = match val {
            None => DynStructCol::empty(val_schema),
            Some(val) => DynStructCol::from_arrow(val_schema, val)?,
        };

        let diff = diff
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .ok_or_else(|| {
                format!(
                    "expected diff to be PrimitiveArray<i64> got {:?}",
                    diff.data_type()
                )
            })?;
        assert!(diff.validity().is_none());
        let diff = diff.values().clone();

        let ts = ts
            .as_any()
            .downcast_ref::<PrimitiveArray<i64>>()
            .ok_or_else(|| {
                format!(
                    "expected ts to be PrimitiveArray<i64> got {:?}",
                    ts.data_type()
                )
            })?;
        assert!(ts.validity().is_none());
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
            ts: Buffer::from(ts.0),
            diff: Buffer::from(diff.0),
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
