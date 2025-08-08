// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A columnar representation of one blob's worth of data

use std::mem;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, Int64Array};
use arrow::datatypes::ToByteSlice;
use mz_ore::result::ResultExt;

use crate::Codec64;
use crate::arrow::{ArrayIdx, ArrayOrd};
use crate::columnar::{ColumnDecoder, ColumnEncoder, Schema};

/// A structured columnar representation of one blob's worth of data.
#[derive(Debug, Clone)]
pub struct Part {
    /// The 'k' values from a Part, generally `SourceData`.
    pub key: Arc<dyn Array>,
    /// The 'v' values from a Part, generally `()`.
    pub val: Arc<dyn Array>,
    /// The `ts` values from a Part.
    pub time: Int64Array,
    /// The `diff` values from a Part.
    pub diff: Int64Array,
}

impl Part {
    /// The length of each of the arrays in the part.
    pub fn len(&self) -> usize {
        self.key.len()
    }

    /// See [ArrayOrd::goodbytes].
    pub fn goodbytes(&self) -> usize {
        ArrayOrd::new(&self.key).goodbytes()
            + ArrayOrd::new(&self.val).goodbytes()
            + self.time.values().to_byte_slice().len()
            + self.diff.values().to_byte_slice().len()
    }

    fn combine(
        parts: &[Part],
        mut combine_fn: impl FnMut(&[&dyn Array]) -> anyhow::Result<ArrayRef>,
    ) -> anyhow::Result<Self> {
        let mut field_array = Vec::with_capacity(parts.len());
        let mut combine = |get: fn(&Part) -> &dyn Array| {
            field_array.extend(parts.iter().map(get));
            let res = combine_fn(&field_array);
            field_array.clear();
            res
        };

        Ok(Self {
            key: combine(|p| &p.key)?,
            val: combine(|p| &p.val)?,
            time: combine(|p| &p.time)?.as_primitive().clone(),
            diff: combine(|p| &p.diff)?.as_primitive().clone(),
        })
    }

    /// Executes [::arrow::compute::concat] columnwise, or returns `None` if no parts are given.
    pub fn concat(parts: &[Part]) -> anyhow::Result<Option<Self>> {
        match parts.len() {
            0 => return Ok(None),
            1 => return Ok(Some(parts[0].clone())),
            _ => {}
        }
        let combined = Part::combine(parts, |cols| ::arrow::compute::concat(cols).err_into())?;
        Ok(Some(combined))
    }

    /// Executes [::arrow::compute::interleave] columnwise.
    pub fn interleave(parts: &[Part], indices: &[(usize, usize)]) -> anyhow::Result<Self> {
        Part::combine(parts, |cols| {
            ::arrow::compute::interleave(cols, indices).err_into()
        })
    }

    /// Iterate over the contents of this part, decoding as we go.
    pub fn decode_iter<
        'a,
        K: Default + Clone + 'static,
        V: Default + Clone + 'static,
        T: Codec64,
        D: Codec64,
    >(
        &'a self,
        key_schema: &'a impl Schema<K>,
        val_schema: &'a impl Schema<V>,
    ) -> anyhow::Result<impl Iterator<Item = ((K, V), T, D)> + 'a> {
        let key_decoder = key_schema.decoder_any(&*self.key)?;
        let val_decoder = val_schema.decoder_any(&*self.val)?;
        let mut key = K::default();
        let mut val = V::default();
        let iter = (0..self.len()).map(move |i| {
            key_decoder.decode(i, &mut key);
            val_decoder.decode(i, &mut val);
            let time = T::decode(self.time.value(i).to_le_bytes());
            let diff = D::decode(self.diff.value(i).to_le_bytes());
            ((key.clone(), val.clone()), time, diff)
        });
        Ok(iter)
    }

    /// Convert the key/value columns to `ArrayOrd`.
    pub fn as_ord(&self) -> PartOrd {
        PartOrd {
            key: ArrayOrd::new(&*self.key),
            val: ArrayOrd::new(&*self.val),
            time: self.time.clone(),
            diff: self.diff.clone(),
        }
    }
}

/// A part with the key/value arrays downcast to `ArrayOrd` for convenience.
#[derive(Debug, Clone)]
pub struct PartOrd {
    key: ArrayOrd,
    val: ArrayOrd,
    time: Int64Array,
    diff: Int64Array,
}

impl PartOrd {
    /// Iterate over the contents of the part in their un-decoded form.
    pub fn iter(&self) -> impl Iterator<Item = (ArrayIdx<'_>, ArrayIdx<'_>, [u8; 8], [u8; 8])> {
        (0..self.time.len()).map(move |i| {
            let key = self.key.at(i);
            let val = self.val.at(i);
            let time = self.time.value(i).to_le_bytes();
            let diff = self.diff.value(i).to_le_bytes();
            (key, val, time, diff)
        })
    }
}

impl PartialEq for Part {
    fn eq(&self, other: &Self) -> bool {
        let Part {
            key,
            val,
            time,
            diff,
        } = self;
        let Part {
            key: other_key,
            val: other_val,
            time: other_time,
            diff: other_diff,
        } = other;
        key == other_key && val == other_val && time == other_time && diff == other_diff
    }
}

/// A builder for [`Part`].
#[derive(Debug)]
pub struct PartBuilder<K, KS: Schema<K>, V, VS: Schema<V>> {
    key: KS::Encoder,
    val: VS::Encoder,
    time: Codec64Mut,
    diff: Codec64Mut,
}

impl<K, KS: Schema<K>, V, VS: Schema<V>> PartBuilder<K, KS, V, VS> {
    /// Returns a new [`PartBuilder`].
    pub fn new(key_schema: &KS, val_schema: &VS) -> Self {
        let key = key_schema.encoder().unwrap();
        let val = val_schema.encoder().unwrap();
        let time = Codec64Mut(Vec::new());
        let diff = Codec64Mut(Vec::new());

        PartBuilder {
            key,
            val,
            time,
            diff,
        }
    }

    /// Estimate the size of the part this builder will build.
    pub fn goodbytes(&self) -> usize {
        self.key.goodbytes() + self.val.goodbytes() + self.time.goodbytes() + self.diff.goodbytes()
    }

    /// Push a new row onto this [`PartBuilder`].
    pub fn push<T: Codec64, D: Codec64>(&mut self, key: &K, val: &V, t: T, d: D) {
        self.key.append(key);
        self.val.append(val);
        self.time.push(&t);
        self.diff.push(&d);
    }

    /// Finishes the builder returning a [`Part`].
    pub fn finish(self) -> Part {
        let PartBuilder {
            key,
            val,
            time,
            diff,
        } = self;

        let key_col = key.finish();
        let val_col = val.finish();
        let time = Int64Array::from(time.0);
        let diff = Int64Array::from(diff.0);

        Part {
            key: Arc::new(key_col),
            val: Arc::new(val_col),
            time,
            diff,
        }
    }

    /// Finish the builder and replace it with an empty one.
    pub fn finish_and_replace(&mut self, key_schema: &KS, val_schema: &VS) -> Part {
        let builder = mem::replace(self, PartBuilder::new(key_schema, val_schema));
        builder.finish()
    }
}

/// Mutable access to a column of a [`Codec64`] implementor.
#[derive(Debug)]
pub struct Codec64Mut(Vec<i64>);

impl Codec64Mut {
    /// Create a builder, pre-sized to an expected number of elements.
    pub fn with_capacity(capacity: usize) -> Self {
        Codec64Mut(Vec::with_capacity(capacity))
    }

    /// Returns the overall size of the stored data in bytes.
    pub fn goodbytes(&self) -> usize {
        self.0.len() * size_of::<i64>()
    }

    /// Returns the length of the column.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Pushes the given value into this column.
    pub fn push(&mut self, val: &impl Codec64) {
        self.push_raw(val.encode());
    }

    /// Pushes the given encoded value into this column.
    pub fn push_raw(&mut self, val: [u8; 8]) {
        self.0.push(i64::from_le_bytes(val));
    }

    /// Return the allocated array.
    pub fn finish(self) -> Int64Array {
        self.0.into()
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
