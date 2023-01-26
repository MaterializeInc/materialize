// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A columnar representation of one blob's worth of data

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow2::array::{Array, PrimitiveArray, StructArray};
use arrow2::buffer::Buffer;
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType as ArrowLogicalType, Field};
use arrow2::io::parquet::write::Encoding;

use crate::columnar::sealed::ColumnRef;
use crate::columnar::{ColumnFormat, ColumnGet, ColumnPush, Data, DataType, Schema};
use crate::ord::{ColOrd, ColsOrd, ColsOrdKey};

/// A columnar representation of one blob's worth of data.
#[derive(Debug, Default)]
pub struct Part {
    len: usize,
    key: Vec<(String, DynColumnRef)>,
    val: Vec<(String, DynColumnRef)>,
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
    pub fn key_ref<'a>(&'a self) -> ColumnsRef<'a> {
        ColumnsRef {
            cols: self
                .key
                .iter()
                .map(|(name, col)| (name.as_str(), col))
                .collect(),
        }
    }

    /// Returns a [ColumnsRef] for the val columns.
    pub fn val_ref<'a>(&'a self) -> ColumnsRef<'a> {
        ColumnsRef {
            cols: self
                .val
                .iter()
                .map(|(name, col)| (name.as_str(), col))
                .collect(),
        }
    }

    pub(crate) fn to_arrow(&self) -> (Vec<Field>, Vec<Vec<Encoding>>, Chunk<Box<dyn Array>>) {
        let (mut fields, mut encodings, mut arrays) =
            (Vec::new(), Vec::new(), Vec::<Box<dyn Array>>::new());

        {
            let (mut key_fields, mut key_encodings, mut key_arrays) =
                (Vec::new(), Vec::new(), Vec::new());
            for (name, col) in self.key.iter() {
                let (encoding, array) = col.to_arrow();
                key_fields.push(Field::new(name, array.data_type().clone(), col.0.optional));
                key_encodings.push(encoding);
                key_arrays.push(array);
            }
            // arrow2 doesn't allow empty struct arrays. To make a future schema
            // migration for <no columns> <-> <one optional column> easier, we
            // model this as a missing column (rather than something like
            // NullArray). This also matches how we'd do the same for nested
            // structs.
            if !key_arrays.is_empty() {
                let key = StructArray::new(ArrowLogicalType::Struct(key_fields), key_arrays, None);
                fields.push(Field::new("k", key.data_type().clone(), false));
                encodings.push(key_encodings);
                arrays.push(Box::new(key));
            }
        }

        {
            let (mut val_fields, mut val_encodings, mut val_arrays) =
                (Vec::new(), Vec::new(), Vec::new());
            for (name, col) in self.val.iter() {
                let (encoding, array) = col.to_arrow();
                val_fields.push(Field::new(name, array.data_type().clone(), col.0.optional));
                val_encodings.push(encoding);
                val_arrays.push(array);
            }
            // arrow2 doesn't allow empty struct arrays. To make a future schema
            // migration for <no columns> <-> <one optional column> easier, we
            // model this as a missing column (rather than something like
            // NullArray). This also matches how we'd do the same for nested
            // structs.
            if !val_arrays.is_empty() {
                let val = StructArray::new(ArrowLogicalType::Struct(val_fields), val_arrays, None);
                fields.push(Field::new("v", val.data_type().clone(), false));
                encodings.push(val_encodings);
                arrays.push(Box::new(val));
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
        let key = if key_schema.is_empty() {
            None
        } else {
            Some(
                chunk
                    .next()
                    .ok_or_else(|| "missing key column".to_owned())?,
            )
        };
        let val = if val_schema.is_empty() {
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
            None => Vec::new(),
            Some(key) => {
                if let Some(key_array) = key.as_any().downcast_ref::<StructArray>() {
                    assert!(key_array.validity().is_none());
                    assert_eq!(key_schema.len(), key_array.values().len());
                    let mut key = Vec::new();
                    for ((name, typ), array) in key_schema.iter().zip(key_array.values()) {
                        let col = DynColumnRef::from_arrow(typ, array)?;
                        key.push((name.clone(), col));
                    }
                    key
                } else {
                    return Err(format!(
                        "expected key to be Null or Struct array got {:?}",
                        key.data_type()
                    ));
                }
            }
        };

        let val = match val {
            None => Vec::new(),
            Some(val) => {
                if let Some(val_array) = val.as_any().downcast_ref::<StructArray>() {
                    assert!(val_array.validity().is_none());
                    assert_eq!(val_schema.len(), val_array.values().len());
                    let mut val = Vec::new();
                    for ((name, typ), array) in val_schema.iter().zip(val_array.values()) {
                        let col = DynColumnRef::from_arrow(typ, array)?;
                        val.push((name.clone(), col));
                    }
                    val
                } else {
                    return Err(format!(
                        "expected val to be Null or Struct array got {:?}",
                        val.data_type()
                    ));
                }
            }
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

    /// Returns a sorted and consolidated copy of this Part.
    ///
    /// This is a full deep clone of the data. Sort ordering is `(K, V, T, D)`
    /// where each column is ordered according to the parquet ordering
    /// semantics.
    pub fn consolidate(&self) -> Self {
        let key_cols = self
            .key
            .iter()
            .map(|(_, x)| x.to_col_ord())
            .collect::<Vec<_>>();
        let key_cols = ColsOrd::new(&key_cols);
        let val_cols = self
            .val
            .iter()
            .map(|(_, x)| x.to_col_ord())
            .collect::<Vec<_>>();
        let val_cols = ColsOrd::new(&val_cols);
        let ts_col = [ColOrd::I64(&self.ts)];
        let ts_col = ColsOrd::new(&ts_col);
        let mut indexes = (0..self.len())
            .map(|idx| (key_cols.key(idx), val_cols.key(idx), ts_col.key(idx)))
            .collect::<Vec<_>>();
        indexes.sort();
        eprintln!(
            "{:?}",
            indexes
                .iter()
                .map(|(k, v, t)| (k.idx, v.idx, t.idx))
                .collect::<Vec<_>>()
        );
        // WIP this probably wants to be a new method
        let mut sorted = {
            let key = self
                .key
                .iter()
                .map(|(name, col)| (name.to_owned(), DynColumnMut::new_untyped(&col.0)))
                .collect();
            let val = self
                .val
                .iter()
                .map(|(name, col)| (name.to_owned(), DynColumnMut::new_untyped(&col.0)))
                .collect();
            let ts = Vec::new();
            let diff = Vec::new();
            PartBuilder { key, val, ts, diff }
        };
        let mut prev: Option<((ColsOrdKey, ColsOrdKey, ColsOrdKey), i64)> = None;
        for current in indexes {
            if let Some((prev_key, prev_diff)) = prev.as_ref() {
                if prev_key != &current {
                    // WIP figure out how to have just one idx: usize
                    let prev_idx = prev_key.0.idx;
                    assert_eq!(prev_idx, prev_key.1.idx);
                    assert_eq!(prev_idx, prev_key.2.idx);
                    for ((_, src), (_, dst)) in self.key.iter().zip(sorted.key.iter_mut()) {
                        dst.push_from(src, prev_idx).expect("WIP");
                    }
                    for ((_, src), (_, dst)) in self.val.iter().zip(sorted.val.iter_mut()) {
                        dst.push_from(src, prev_idx).expect("WIP");
                    }
                    sorted.ts.push(self.ts[prev_idx]);
                    sorted.diff.push(*prev_diff);
                    let _ = prev.take();
                }
            }
            let current_diff = self.diff[current.2.idx];
            if let Some((_, diff)) = prev.as_mut() {
                *diff = *diff + current_diff;
            } else {
                prev = Some((current, current_diff));
            }
        }
        if let Some((prev_key, prev_diff)) = prev.as_ref() {
            // WIP figure out how to have just one idx: usize
            let prev_idx = prev_key.0.idx;
            assert_eq!(prev_idx, prev_key.1.idx);
            assert_eq!(prev_idx, prev_key.2.idx);
            for ((_, src), (_, dst)) in self.key.iter().zip(sorted.key.iter_mut()) {
                dst.push_from(src, prev_idx).expect("WIP");
            }
            for ((_, src), (_, dst)) in self.val.iter().zip(sorted.val.iter_mut()) {
                dst.push_from(src, prev_idx).expect("WIP");
            }
            sorted.ts.push(self.ts[prev_idx]);
            sorted.diff.push(*prev_diff);
        }
        sorted.finish().expect("WIP")
    }

    fn validate(&self) -> Result<(), String> {
        for (name, col) in self.key.iter() {
            if self.len != col.len() {
                return Err(format!(
                    "key col {} len {} didn't match part len {}",
                    name,
                    col.len(),
                    self.len()
                ));
            }
        }
        for (name, col) in self.val.iter() {
            if self.len != col.len() {
                return Err(format!(
                    "val col {} len {} didn't match part len {}",
                    name,
                    col.len(),
                    self.len()
                ));
            }
        }
        if self.len != self.ts.len() {
            return Err(format!(
                "ts col len {} didn't match part len {}",
                self.ts.len(),
                self.len()
            ));
        }
        if self.len != self.diff.len() {
            return Err(format!(
                "diff col len {} didn't match part len {}",
                self.diff.len(),
                self.len()
            ));
        }
        // TODO: Also validate the col types match schema.
        Ok(())
    }
}

// WIP I couldn't figure out how to reuse the Formatter helpers here, so this
// doesn't e.g. respect the pretty flag
impl Debug for Part {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("[")?;
        for idx in 0..self.len() {
            if idx > 0 {
                f.write_str(",")?;
            }
            f.write_str("((")?;
            for (_, col) in self.key.iter() {
                col.fmt_debug(idx, f)?;
            }
            f.write_str("),(")?;
            for (_, col) in self.val.iter() {
                col.fmt_debug(idx, f)?;
            }
            f.write_str("),")?;
            Debug::fmt(&self.ts[idx], f)?;
            f.write_str(",")?;
            Debug::fmt(&self.diff[idx], f)?;
            f.write_str(")")?;
        }
        f.write_str("]")
    }
}

/// An in-progress columnar constructor for one blob's worth of data.
#[derive(Debug, Default)]
pub struct PartBuilder {
    key: Vec<(String, DynColumnMut)>,
    val: Vec<(String, DynColumnMut)>,
    ts: Vec<i64>,
    diff: Vec<i64>,
}

impl PartBuilder {
    /// Returns a new PartBuilder with the given schema.
    pub fn new<K, KS: Schema<K>, V, VS: Schema<V>>(key_schema: &KS, val_schema: &VS) -> Self {
        let key = key_schema
            .columns()
            .into_iter()
            .map(|(name, data_type)| (name, DynColumnMut::new_untyped(&data_type)))
            .collect();
        let val = val_schema
            .columns()
            .into_iter()
            .map(|(name, data_type)| (name, DynColumnMut::new_untyped(&data_type)))
            .collect();
        let ts = Vec::new();
        let diff = Vec::new();
        PartBuilder { key, val, ts, diff }
    }

    /// Returns a [ColumnsMut] for the key columns.
    pub fn key_mut<'a>(&'a mut self) -> ColumnsMut<'a> {
        ColumnsMut {
            cols: self
                .key
                .iter_mut()
                .map(|(name, col)| (name.as_str(), col))
                .collect(),
        }
    }

    /// Returns a [ColumnsMut] for the val columns.
    pub fn val_mut<'a>(&'a mut self) -> ColumnsMut<'a> {
        ColumnsMut {
            cols: self
                .val
                .iter_mut()
                .map(|(name, col)| (name.as_str(), col))
                .collect(),
        }
    }

    /// Adds a single timestamp and diff.
    ///
    /// TODO: Feels like there's some better way to model this.
    pub fn push_ts_diff(&mut self, ts: i64, diff: i64) {
        self.ts.push(ts);
        self.diff.push(diff);
    }

    /// Completes construction of the [Part].
    pub fn finish(self) -> Result<Part, String> {
        let key = self
            .key
            .into_iter()
            .map(|(name, col)| (name, col.finish_untyped()))
            .collect();
        let val = self
            .val
            .into_iter()
            .map(|(name, col)| (name, col.finish_untyped()))
            .collect();
        let ts = Buffer::from(self.ts);
        let diff = Buffer::from(self.diff);

        let len = diff.len();
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
}

/// Hack to make things work with `Arc<dyn Any>::downcast_ref`.
#[derive(Debug)]
struct DynColumnRef(DataType, Arc<dyn Any + Send + Sync>);

impl DynColumnRef {
    pub fn new<T: Data>(col: T::Col) -> Self {
        DynColumnRef(T::TYPE.clone(), Arc::new(col))
    }

    pub fn downcast<T: Data>(&self) -> Result<&T::Col, String> {
        let col = self
            .1
            .downcast_ref::<T::Col>()
            .ok_or_else(|| format!("expected {} col", std::any::type_name::<T::Col>()))?;
        Ok(col)
    }

    fn expect_downcast<T: Data>(&self) -> &T::Col {
        self.1
            .downcast_ref::<T::Col>()
            .expect("DynColumnRef DataType should have internally consistent")
    }

    pub fn len(&self) -> usize {
        struct LenDataFn<'a>(&'a DynColumnRef);
        impl DataFn<usize> for LenDataFn<'_> {
            fn call<T: Data>(self) -> usize {
                self.0.expect_downcast::<T>().len()
            }
        }
        self.0.data_fn(LenDataFn(self))
    }

    fn to_col_ord<'a>(&'a self) -> ColOrd<'a> {
        match (self.0.optional, self.0.format) {
            (false, ColumnFormat::I64) => ColOrd::I64(self.expect_downcast::<i64>()),
            (false, ColumnFormat::String) => ColOrd::String(self.expect_downcast::<String>()),
            _ => panic!("WIP"),
        }
    }

    fn fmt_debug(&self, idx: usize, fmt: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        struct FmtDebugDataFn<'a, 'b, 'c>(&'a DynColumnRef, usize, &'b mut std::fmt::Formatter<'c>);
        impl DataFn<std::fmt::Result> for FmtDebugDataFn<'_, '_, '_> {
            fn call<T: Data>(self) -> std::fmt::Result {
                let FmtDebugDataFn(col, idx, fmt) = self;
                Debug::fmt(&col.expect_downcast::<T>().get(idx), fmt)
            }
        }
        self.0.data_fn(FmtDebugDataFn(self, idx, fmt))
    }

    #[allow(clippy::borrowed_box)]
    pub(crate) fn from_arrow(data_type: &DataType, array: &Box<dyn Array>) -> Result<Self, String> {
        struct FromArrowDataFn<'a>(&'a Box<dyn Array>);
        impl DataFn<Result<DynColumnRef, String>> for FromArrowDataFn<'_> {
            fn call<T: Data>(self) -> Result<DynColumnRef, String> {
                let typ = T::TYPE.clone();
                let col = T::Col::from_arrow(self.0)?;
                let col: Arc<dyn Any + Send + Sync> = Arc::new(col);
                Ok(DynColumnRef(typ, col))
            }
        }
        let col = data_type.data_fn(FromArrowDataFn(array))?;
        debug_assert_eq!(&col.0, data_type);
        Ok(col)
    }

    pub(crate) fn to_arrow(&self) -> (Encoding, Box<dyn Array>) {
        struct ToArrowDataFn<'a>(&'a DynColumnRef);
        impl DataFn<(Encoding, Box<dyn Array>)> for ToArrowDataFn<'_> {
            fn call<T: Data>(self) -> (Encoding, Box<dyn Array>) {
                self.0.expect_downcast::<T>().to_arrow()
            }
        }
        self.0.data_fn(ToArrowDataFn(self))
    }
}

/// Hack to make things work with `Box<dyn Any>::downcast_mut`.
#[derive(Debug)]
struct DynColumnMut(DataType, Box<dyn Any + Send + Sync>);

impl DynColumnMut {
    pub fn new<T: Data>(col: T::Mut) -> Self {
        DynColumnMut(T::TYPE.clone(), Box::new(col))
    }

    pub fn new_untyped(typ: &DataType) -> Self {
        struct NewUntypedDataFn;
        impl DataFn<DynColumnMut> for NewUntypedDataFn {
            fn call<T: Data>(self) -> DynColumnMut {
                DynColumnMut::new::<T>(T::Mut::default())
            }
        }
        typ.data_fn(NewUntypedDataFn)
    }

    pub fn downcast<T: Data>(&mut self) -> Result<&mut T::Mut, String> {
        let col = self
            .1
            .downcast_mut::<T::Mut>()
            .ok_or_else(|| format!("expected {} col", std::any::type_name::<T::Col>()))?;
        Ok(col)
    }

    pub fn finish<T: Data>(self) -> Result<DynColumnRef, String> {
        let col = self
            .1
            .downcast::<T::Mut>()
            .map_err(|_| format!("expected {} col", std::any::type_name::<T::Col>()))?;
        let col = T::Col::from(*col);
        let col = DynColumnRef::new::<T>(col);
        debug_assert_eq!(self.0, col.0);
        Ok(col)
    }

    fn finish_untyped(self) -> DynColumnRef {
        let typ = self.0.clone();
        struct FinishUntypedDataFn(DynColumnMut);
        impl DataFn<Result<DynColumnRef, String>> for FinishUntypedDataFn {
            fn call<T: Data>(self) -> Result<DynColumnRef, String> {
                self.0.finish::<T>()
            }
        }
        let col = typ
            .data_fn(FinishUntypedDataFn(self))
            .expect("DynColumnMut DataType should have internally consistent");
        assert_eq!(typ, col.0);
        col
    }

    fn push_from(&mut self, src: &DynColumnRef, idx: usize) -> Result<(), String> {
        let typ = self.0.clone();
        struct PushFromDataFn<'a>(&'a mut DynColumnMut, &'a DynColumnRef, usize);
        impl<'a> DataFn<Result<(), String>> for PushFromDataFn<'a> {
            fn call<T: Data>(self) -> Result<(), String> {
                let PushFromDataFn(dst, src, idx) = self;
                let dst = dst.downcast::<T>()?;
                let src = src.downcast::<T>()?;
                dst.push(src.get(idx));
                Ok(())
            }
        }
        typ.data_fn(PushFromDataFn(self, src, idx))
    }
}

/// A set of shared references to named columns.
///
/// This type implements a "builder"-esque pattern to help [Schema::decoder]
/// impls. All columns should be removed via [Self::col] and the [Self::finish]
/// called to verify that all columns have been accounted for.
#[derive(Debug)]
pub struct ColumnsRef<'a> {
    cols: BTreeMap<&'a str, &'a DynColumnRef>,
}

impl<'a> ColumnsRef<'a> {
    /// Removes the named column from the set.
    pub fn col<T: Data>(&mut self, name: &str) -> Result<&'a T::Col, String> {
        let col = self
            .cols
            .remove(name)
            .ok_or_else(|| format!("no col named {}", name))?;
        col.downcast::<T>()
    }

    /// Verifies that all columns in the set have been removed.
    pub fn finish(self) -> Result<(), String> {
        if self.cols.is_empty() {
            Ok(())
        } else {
            let names = self.cols.iter().map(|(x, _)| *x).collect::<Vec<_>>();
            Err(format!("unused cols: {}", names.join(" ")))
        }
    }
}

/// A set of exclusive references to named columns.
///
/// This type implements a "builder"-esque pattern to help [Schema::encoder]
/// impls. All columns should be removed via [Self::col] and the [Self::finish]
/// called to verify that all columns have been accounted for.
#[derive(Debug)]
pub struct ColumnsMut<'a> {
    cols: BTreeMap<&'a str, &'a mut DynColumnMut>,
}

impl<'a> ColumnsMut<'a> {
    /// Removes the named column from the set.
    pub fn col<T: Data>(&mut self, name: &str) -> Result<&'a mut T::Mut, String> {
        let col = self
            .cols
            .remove(name)
            .ok_or_else(|| format!("no col named {}", name))?;
        col.downcast::<T>()
    }

    /// Verifies that all columns in the set have been removed.
    pub fn finish(self) -> Result<(), String> {
        if self.cols.is_empty() {
            Ok(())
        } else {
            let names = self.cols.iter().map(|(x, _)| *x).collect::<Vec<_>>();
            Err(format!("unused cols: {}", names.join(" ")))
        }
    }
}

trait DataFn<R> {
    fn call<T: Data>(self) -> R;
}

impl DataType {
    fn data_fn<R, F: DataFn<R>>(&self, logic: F) -> R {
        match (self.optional, self.format) {
            (false, ColumnFormat::Bool) => logic.call::<bool>(),
            (true, ColumnFormat::Bool) => logic.call::<Option<bool>>(),
            (false, ColumnFormat::U8) => logic.call::<u8>(),
            (true, ColumnFormat::U8) => logic.call::<Option<u8>>(),
            (false, ColumnFormat::U16) => logic.call::<u16>(),
            (true, ColumnFormat::U16) => logic.call::<Option<u16>>(),
            (false, ColumnFormat::U32) => logic.call::<u32>(),
            (true, ColumnFormat::U32) => logic.call::<Option<u32>>(),
            (false, ColumnFormat::U64) => logic.call::<u64>(),
            (true, ColumnFormat::U64) => logic.call::<Option<u64>>(),
            (false, ColumnFormat::I8) => logic.call::<i8>(),
            (true, ColumnFormat::I8) => logic.call::<Option<i8>>(),
            (false, ColumnFormat::I16) => logic.call::<i16>(),
            (true, ColumnFormat::I16) => logic.call::<Option<i16>>(),
            (false, ColumnFormat::I32) => logic.call::<i32>(),
            (true, ColumnFormat::I32) => logic.call::<Option<i32>>(),
            (false, ColumnFormat::I64) => logic.call::<i64>(),
            (true, ColumnFormat::I64) => logic.call::<Option<i64>>(),
            (false, ColumnFormat::F32) => logic.call::<f32>(),
            (true, ColumnFormat::F32) => logic.call::<Option<f32>>(),
            (false, ColumnFormat::F64) => logic.call::<f64>(),
            (true, ColumnFormat::F64) => logic.call::<Option<f64>>(),
            (false, ColumnFormat::Bytes) => logic.call::<Vec<u8>>(),
            (true, ColumnFormat::Bytes) => logic.call::<Option<Vec<u8>>>(),
            (false, ColumnFormat::String) => logic.call::<String>(),
            (true, ColumnFormat::String) => logic.call::<Option<String>>(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use crate::codec_impls::{StringSchema, UnitSchema};
    use crate::columnar::PartEncoder;

    use super::*;

    // Make sure that the API structs are Sync + Send, so that they can be used in async tasks.
    // NOTE: This is a compile-time only test. If it compiles, we're good.
    #[allow(unused)]
    fn sync_send() {
        fn is_send_sync<T: Send + Sync>(_: PhantomData<T>) -> bool {
            true
        }

        assert!(is_send_sync::<Part>(PhantomData));
        assert!(is_send_sync::<PartBuilder>(PhantomData));
    }

    #[test]
    fn part_consolidate() {
        let mut part = PartBuilder::new(&StringSchema, &UnitSchema);
        {
            let mut keys = StringSchema.encoder(part.key_mut()).unwrap();
            keys.encode(&format!("foo"));
            keys.encode(&format!("foo"));
            keys.encode(&format!("foo"));
            keys.encode(&format!("bar"));
            keys.encode(&format!("foo"));
            keys.encode(&format!("baz"));
            keys.encode(&format!("foo"));
        }
        part.push_ts_diff(3, 1);
        part.push_ts_diff(1, 1);
        part.push_ts_diff(2, 1);
        part.push_ts_diff(1, 1);
        part.push_ts_diff(1, 1);
        part.push_ts_diff(1, 1);
        part.push_ts_diff(1, 1);
        let part = part.finish().unwrap();
        let consolidated = part.consolidate();
        eprintln!("{:?}", consolidated);
    }
}
