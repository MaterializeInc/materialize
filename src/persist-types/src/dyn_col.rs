// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Type-erased [crate::columnar::Data::Col] and [crate::columnar::Data::Mut].

use std::any::Any;
use std::sync::Arc;

use arrow2::array::Array;
use arrow2::io::parquet::write::Encoding;

use crate::columnar::sealed::{ColumnMut, ColumnRef};
use crate::columnar::{ColumnCfg, ColumnFormat, ColumnGet, ColumnPush, Data, DataType};
use crate::dyn_struct::{DynStruct, ValidityRef};
use crate::stats::{DynStats, StatsFrom};

/// A type-erased [crate::columnar::Data::Col].
#[derive(Debug)]
pub struct DynColumnRef(DataType, Arc<dyn Any + Send + Sync>);

impl DynColumnRef {
    fn new<T: Data>(col: T::Col) -> Self {
        DynColumnRef(col.cfg().as_type(), Arc::new(col))
    }

    #[cfg(debug_assertions)]
    pub(crate) fn typ(&self) -> &DataType {
        &self.0
    }

    /// Returns a typed, shared reference to the inner value if it is a column
    /// of type `T`, or an Err if it isn't.
    pub fn downcast<T: Data>(&self) -> Result<&T::Col, String> {
        let col = self
            .1
            .downcast_ref::<T::Col>()
            .ok_or_else(|| format!("expected {} col", std::any::type_name::<T::Col>()))?;
        Ok(col)
    }

    /// Returns the number of elements in this column.
    pub fn len(&self) -> usize {
        struct LenDataFn<'a>(&'a DynColumnRef);
        impl DataFn<Result<usize, String>> for LenDataFn<'_> {
            fn call<T: Data>(self, _cfg: &T::Cfg) -> Result<usize, String> {
                self.0.downcast::<T>().map(|x| x.len())
            }
        }
        self.0
            .data_fn(LenDataFn(self))
            .expect("DynColumnRef DataType should be internally consistent")
    }

    /// Computes statistics on this column using the default implementation of
    /// `T::Stats::From`.
    pub fn stats_default(&self, validity: ValidityRef<'_>) -> Box<dyn DynStats> {
        struct StatsDataFn<'a>(&'a DynColumnRef, ValidityRef<'a>);
        impl DataFn<Result<Box<dyn DynStats>, String>> for StatsDataFn<'_> {
            fn call<T: Data>(self, _cfg: &T::Cfg) -> Result<Box<dyn DynStats>, String> {
                let StatsDataFn(col, validity) = self;
                let col = col.downcast::<T>()?;
                Ok(Box::new(T::Stats::stats_from(col, validity)))
            }
        }
        self.0
            .data_fn(StatsDataFn(self, validity))
            .expect("DynColumnRef DataType should be internally consistent")
    }

    #[allow(clippy::borrowed_box)]
    pub(crate) fn from_arrow(data_type: &DataType, array: &Box<dyn Array>) -> Result<Self, String> {
        struct FromArrowDataFn<'a>(&'a Box<dyn Array>);
        impl DataFn<Result<DynColumnRef, String>> for FromArrowDataFn<'_> {
            fn call<T: Data>(self, cfg: &T::Cfg) -> Result<DynColumnRef, String> {
                let typ = cfg.as_type();
                let col = T::Col::from_arrow(cfg, self.0)?;
                let col: Arc<dyn Any + Send + Sync> = Arc::new(col);
                Ok(DynColumnRef(typ, col))
            }
        }
        let col = data_type.data_fn(FromArrowDataFn(array))?;
        #[cfg(debug_assertions)]
        {
            assert_eq!(&col.0, data_type);
        }
        Ok(col)
    }

    pub(crate) fn to_arrow(&self) -> (Encoding, Box<dyn Array>, bool) {
        struct ToArrowDataFn<'a>(&'a DynColumnRef);
        impl DataFn<Result<(Encoding, Box<dyn Array>), String>> for ToArrowDataFn<'_> {
            fn call<T: Data>(self, _cfg: &T::Cfg) -> Result<(Encoding, Box<dyn Array>), String> {
                Ok(self.0.downcast::<T>()?.to_arrow())
            }
        }
        let (encoding, array) = self
            .0
            .data_fn(ToArrowDataFn(self))
            .expect("DynColumnRef DataType should be internally consistent");
        (encoding, array, self.0.optional)
    }
}

/// A type-erased [crate::columnar::Data::Mut].
#[derive(Debug)]
pub struct DynColumnMut(DataType, Box<dyn Any + Send + Sync>);

impl DynColumnMut {
    fn new<T: Data>(col: T::Mut) -> Self {
        DynColumnMut(col.cfg().as_type(), Box::new(col))
    }

    pub(crate) fn new_untyped(typ: &DataType) -> Self {
        struct NewUntypedDataFn;
        impl DataFn<DynColumnMut> for NewUntypedDataFn {
            fn call<T: Data>(self, cfg: &T::Cfg) -> DynColumnMut {
                DynColumnMut::new::<T>(T::Mut::new(cfg))
            }
        }
        typ.data_fn(NewUntypedDataFn)
    }

    #[cfg(debug_assertions)]
    pub(crate) fn typ(&self) -> &DataType {
        &self.0
    }

    /// Returns a typed, exclusive reference to the inner value if it is a
    /// column of type `T`, or an Err if it isn't.
    pub fn downcast<T: Data>(&mut self) -> Result<&mut T::Mut, String> {
        let col = self
            .1
            .downcast_mut::<T::Mut>()
            .ok_or_else(|| format!("expected {} col", std::any::type_name::<T::Col>()))?;
        Ok(col)
    }

    pub(crate) fn push_default(&mut self) {
        struct PushDefaultFn<'a>(&'a mut DynColumnMut);
        impl DataFn<()> for PushDefaultFn<'_> {
            fn call<T: Data>(self, _cfg: &T::Cfg) {
                let col = self
                    .0
                    .downcast::<T>()
                    .expect("DynColumnMut DataType should have internally consistent");
                ColumnPush::<T>::push(col, T::Ref::default());
            }
        }
        self.0.clone().data_fn(PushDefaultFn(self))
    }

    pub(crate) fn push_from(&mut self, src: &DynColumnRef, idx: usize) {
        struct PushFromFn<'a>(&'a DynColumnRef, &'a mut DynColumnMut, usize);
        impl DataFn<()> for PushFromFn<'_> {
            fn call<T: Data>(self, _cfg: &T::Cfg) {
                let PushFromFn(src, dst, idx) = self;
                let dst = dst
                    .downcast::<T>()
                    .expect("DynColumnMut DataType should have internally consistent");
                let src = src
                    .downcast::<T>()
                    .expect("push_from src type should match dst");
                ColumnPush::<T>::push(dst, ColumnGet::<T>::get(src, idx));
            }
        }
        self.0.clone().data_fn(PushFromFn(src, self, idx))
    }

    /// Closes the column to pushes and returns it as a [DynColumnRef].
    pub fn finish<T: Data>(self) -> Result<DynColumnRef, String> {
        let col = self
            .1
            .downcast::<T::Mut>()
            .map_err(|_| format!("expected {} col", std::any::type_name::<T::Col>()))?;
        let col = T::Col::from(*col);
        let col = DynColumnRef::new::<T>(col);
        #[cfg(debug_assertions)]
        {
            assert_eq!(self.0, col.0);
        }
        Ok(col)
    }
}

impl From<DynColumnMut> for DynColumnRef {
    fn from(value: DynColumnMut) -> Self {
        let typ = value.0.clone();
        struct FinishUntypedDataFn(DynColumnMut);
        impl DataFn<Result<DynColumnRef, String>> for FinishUntypedDataFn {
            fn call<T: Data>(self, _cfg: &T::Cfg) -> Result<DynColumnRef, String> {
                self.0.finish::<T>()
            }
        }
        let col = typ
            .data_fn(FinishUntypedDataFn(value))
            .expect("DynColumnMut DataType should have internally consistent");
        #[cfg(debug_assertions)]
        {
            assert_eq!(typ, col.0);
        }
        col
    }
}

trait DataFn<R> {
    fn call<T: Data>(self, cfg: &T::Cfg) -> R;
}

impl DataType {
    fn data_fn<R, F: DataFn<R>>(&self, logic: F) -> R {
        match (self.optional, &self.format) {
            (false, ColumnFormat::Bool) => logic.call::<bool>(&()),
            (true, ColumnFormat::Bool) => logic.call::<Option<bool>>(&()),
            (false, ColumnFormat::U8) => logic.call::<u8>(&()),
            (true, ColumnFormat::U8) => logic.call::<Option<u8>>(&()),
            (false, ColumnFormat::U16) => logic.call::<u16>(&()),
            (true, ColumnFormat::U16) => logic.call::<Option<u16>>(&()),
            (false, ColumnFormat::U32) => logic.call::<u32>(&()),
            (true, ColumnFormat::U32) => logic.call::<Option<u32>>(&()),
            (false, ColumnFormat::U64) => logic.call::<u64>(&()),
            (true, ColumnFormat::U64) => logic.call::<Option<u64>>(&()),
            (false, ColumnFormat::I8) => logic.call::<i8>(&()),
            (true, ColumnFormat::I8) => logic.call::<Option<i8>>(&()),
            (false, ColumnFormat::I16) => logic.call::<i16>(&()),
            (true, ColumnFormat::I16) => logic.call::<Option<i16>>(&()),
            (false, ColumnFormat::I32) => logic.call::<i32>(&()),
            (true, ColumnFormat::I32) => logic.call::<Option<i32>>(&()),
            (false, ColumnFormat::I64) => logic.call::<i64>(&()),
            (true, ColumnFormat::I64) => logic.call::<Option<i64>>(&()),
            (false, ColumnFormat::F32) => logic.call::<f32>(&()),
            (true, ColumnFormat::F32) => logic.call::<Option<f32>>(&()),
            (false, ColumnFormat::F64) => logic.call::<f64>(&()),
            (true, ColumnFormat::F64) => logic.call::<Option<f64>>(&()),
            (false, ColumnFormat::Bytes) => logic.call::<Vec<u8>>(&()),
            (true, ColumnFormat::Bytes) => logic.call::<Option<Vec<u8>>>(&()),
            (false, ColumnFormat::String) => logic.call::<String>(&()),
            (true, ColumnFormat::String) => logic.call::<Option<String>>(&()),
            (false, ColumnFormat::Struct(cfg)) => logic.call::<DynStruct>(cfg),
            (true, ColumnFormat::Struct(cfg)) => logic.call::<Option<DynStruct>>(cfg),
        }
    }
}
