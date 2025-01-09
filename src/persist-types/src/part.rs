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

use arrow::array::{Array, Int64Array};

use crate::columnar::{ColumnEncoder, Schema2};
use crate::Codec64;

/// A structured columnar representation of one blob's worth of data.
pub struct Part2 {
    /// The 'k' values from a Part, generally `SourceData`.
    pub key: Arc<dyn Array>,
    /// The 'v' values from a Part, generally `()`.
    pub val: Arc<dyn Array>,
    /// The `ts` values from a Part.
    pub time: Int64Array,
    /// The `diff` values from a Part.
    pub diff: Int64Array,
}

/// A builder for [`Part2`].
#[derive(Debug)]
pub struct PartBuilder2<K, KS: Schema2<K>, V, VS: Schema2<V>> {
    key: KS::Encoder,
    val: VS::Encoder,
    time: Codec64Mut,
    diff: Codec64Mut,
}

impl<K, KS: Schema2<K>, V, VS: Schema2<V>> PartBuilder2<K, KS, V, VS> {
    /// Returns a new [`PartBuilder2`].
    pub fn new(key_schema: &KS, val_schema: &VS) -> Self {
        let key = key_schema.encoder().unwrap();
        let val = val_schema.encoder().unwrap();
        let time = Codec64Mut(Vec::new());
        let diff = Codec64Mut(Vec::new());

        PartBuilder2 {
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

    /// Push a new row onto this [`PartBuilder2`].
    pub fn push<T: Codec64, D: Codec64>(&mut self, key: &K, val: &V, t: T, d: D) {
        self.key.append(key);
        self.val.append(val);
        self.time.push(t);
        self.diff.push(d);
    }

    /// Finishes the builder returning a [`Part2`].
    pub fn finish(self) -> Part2 {
        let PartBuilder2 {
            key,
            val,
            time,
            diff,
        } = self;

        let key_col = key.finish();
        let val_col = val.finish();
        let time = Int64Array::from(time.0);
        let diff = Int64Array::from(diff.0);

        Part2 {
            key: Arc::new(key_col),
            val: Arc::new(val_col),
            time,
            diff,
        }
    }

    /// Finish the builder and replace it with an empty one.
    pub fn finish_and_replace(&mut self, key_schema: &KS, val_schema: &VS) -> Part2 {
        let builder = mem::replace(self, PartBuilder2::new(key_schema, val_schema));
        builder.finish()
    }
}

/// Mutable access to a column of a [`Codec64`] implementor.
#[derive(Debug)]
pub struct Codec64Mut(Vec<i64>);

impl Codec64Mut {
    /// Returns the overall size of the stored data in bytes.
    pub fn goodbytes(&self) -> usize {
        self.0.len() * size_of::<i64>()
    }

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

        assert!(is_send_sync::<Part2>(PhantomData));
        assert!(is_send_sync::<PartBuilder2<(), UnitSchema, (), UnitSchema>>(PhantomData));
    }
}
