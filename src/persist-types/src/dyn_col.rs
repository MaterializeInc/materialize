// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// A type-erased [crate::columnar::Data::Col].
#[derive(Debug)]
pub struct DynColumnRef(DataType, Arc<dyn Any + Send + Sync>);

impl DynColumnRef {
    fn new<T: Data>(col: T::Col) -> Self {
        DynColumnRef(T::TYPE.clone(), Arc::new(col))
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
            fn call<T: Data>(self) -> Result<usize, String> {
                self.0.downcast::<T>().map(|x| x.len())
            }
        }
        self.0
            .data_fn(LenDataFn(self))
            .expect("DynColumnRef DataType should be internally consistent")
    }

    /// Computes statistics on this column using the default implementation of
    /// `T::Stats::From`.
    ///
    /// See [Self::stats].
    pub fn stats_default(&self) -> Box<dyn DynStats> {
        struct StatsDataFn<'a>(&'a DynColumnRef);
        impl DataFn<Result<Box<dyn DynStats>, String>> for StatsDataFn<'_> {
            fn call<T: Data>(self) -> Result<Box<dyn DynStats>, String> {
                let stats = self.0.stats::<T, _>(|x| T::Stats::from(x))?;
                Ok(Box::new(stats))
            }
        }
        self.0
            .data_fn(StatsDataFn(self))
            .expect("DynColumnRef DataType should be internally consistent")
    }

    /// Computes statistics on this column using the specified function.
    ///
    /// See [Self::stats_default].
    pub fn stats<T: Data, F: FnOnce(&T::Col) -> T::Stats>(
        &self,
        stats_fn: F,
    ) -> Result<T::Stats, String> {
        let col = self.downcast::<T>()?;
        Ok(stats_fn(col))
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
        impl DataFn<Result<(Encoding, Box<dyn Array>), String>> for ToArrowDataFn<'_> {
            fn call<T: Data>(self) -> Result<(Encoding, Box<dyn Array>), String> {
                Ok(self.0.downcast::<T>()?.to_arrow())
            }
        }
        self.0
            .data_fn(ToArrowDataFn(self))
            .expect("DynColumnRef DataType should be internally consistent")
    }
}

/// A type-erased [crate::columnar::Data::Mut].
#[derive(Debug)]
struct DynColumnMut(DataType, Box<dyn Any + Send + Sync>);

impl DynColumnMut {
    fn new<T: Data>(col: T::Mut) -> Self {
        DynColumnMut(T::TYPE.clone(), Box::new(col))
    }

    fn new_untyped(typ: &DataType) -> Self {
        struct NewUntypedDataFn;
        impl DataFn<DynColumnMut> for NewUntypedDataFn {
            fn call<T: Data>(self) -> DynColumnMut {
                DynColumnMut::new::<T>(T::Mut::default())
            }
        }
        typ.data_fn(NewUntypedDataFn)
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
