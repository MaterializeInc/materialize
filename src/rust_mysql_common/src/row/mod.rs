// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::{
    io::ParseBuf,
    misc::unexpected_buf_eof,
    packets::{Column, NullBitmap},
    proto::{Binary, MyDeserialize, Text},
    value::{
        convert::{from_value, from_value_opt, FromValue, FromValueError},
        BinValue, SerializationSide, TextValue, Value, ValueDeserializer,
    },
};
use std::{borrow::Cow, fmt, io, marker::PhantomData, ops::Index, sync::Arc};

pub mod convert;

/// Client side representation of a MySql row.
///
/// It allows you to move column values out of a row with `Row::take` method but note that it
/// makes row incomplete. Calls to `from_row_opt` on incomplete row will return
/// `Error::FromRowError` and also numerical indexing on taken columns will panic.
#[derive(Clone, PartialEq)]
pub struct Row {
    values: Vec<Option<Value>>,
    columns: Arc<[Column]>,
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("Row");
        for (val, column) in self.values.iter().zip(self.columns.iter()) {
            match *val {
                Some(ref val) => {
                    debug.field(column.name_str().as_ref(), val);
                }
                None => {
                    debug.field(column.name_str().as_ref(), &"<taken>");
                }
            }
        }
        debug.finish()
    }
}

/// Creates `Row` from values and columns.
pub fn new_row(values: Vec<Value>, columns: Arc<[Column]>) -> Row {
    assert!(values.len() == columns.len());
    Row {
        values: values.into_iter().map(Some).collect::<Vec<_>>(),
        columns,
    }
}

/// Creates `Row` from values (cells may be missing).
#[doc(hidden)]
pub fn new_row_raw(values: Vec<Option<Value>>, columns: Arc<[Column]>) -> Row {
    assert!(values.len() == columns.len());
    Row { values, columns }
}

impl Row {
    /// Returns length of a row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if the row has a length of 0.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Returns columns of this row.
    pub fn columns_ref(&self) -> &[Column] {
        &self.columns
    }

    /// Returns columns of this row.
    pub fn columns(&self) -> Arc<[Column]> {
        self.columns.clone()
    }

    /// Returns reference to the value of a column with index `index` if it exists and wasn't taken
    /// by `Row::take` method.
    ///
    /// Non panicking version of `row[usize]`.
    pub fn as_ref(&self, index: usize) -> Option<&Value> {
        self.values.get(index).and_then(|x| x.as_ref())
    }

    /// Will copy value at index `index` if it was not taken by `Row::take` earlier,
    /// then will convert it to `T`.
    pub fn get<T, I>(&self, index: I) -> Option<T>
    where
        T: FromValue,
        I: ColumnIndex,
    {
        index.idx(&self.columns).and_then(|idx| {
            self.values
                .get(idx)
                .and_then(|x| x.as_ref())
                .map(|x| from_value::<T>(x.clone()))
        })
    }

    /// Will copy value at index `index` if it was not taken by `Row::take` or `Row::take_opt`
    /// earlier, then will attempt convert it to `T`. Unlike `Row::get`, `Row::get_opt` will
    /// allow you to directly handle errors if the value could not be converted to `T`.
    pub fn get_opt<T, I>(&self, index: I) -> Option<Result<T, FromValueError>>
    where
        T: FromValue,
        I: ColumnIndex,
    {
        index
            .idx(&self.columns)
            .and_then(|idx| self.values.get(idx))
            .and_then(|x| x.as_ref())
            .map(|x| from_value_opt::<T>(x.clone()))
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will converts it to `T`.
    pub fn take<T, I>(&mut self, index: I) -> Option<T>
    where
        T: FromValue,
        I: ColumnIndex,
    {
        index.idx(&self.columns).and_then(|idx| {
            self.values
                .get_mut(idx)
                .and_then(|x| x.take())
                .map(from_value::<T>)
        })
    }

    /// Will take value of a column with index `index` if it exists and wasn't taken earlier then
    /// will attempt to convert it to `T`. Unlike `Row::take`, `Row::take_opt` will allow you to
    /// directly handle errors if the value could not be converted to `T`.
    pub fn take_opt<T, I>(&mut self, index: I) -> Option<Result<T, FromValueError>>
    where
        T: FromValue,
        I: ColumnIndex,
    {
        index
            .idx(&self.columns)
            .and_then(|idx| self.values.get_mut(idx))
            .and_then(|x| x.take())
            .map(from_value_opt::<T>)
    }

    /// Unwraps values of a row.
    ///
    /// # Panics
    ///
    /// Panics if any of columns was taken by `take` method.
    pub fn unwrap(self) -> Vec<Value> {
        self.values
            .into_iter()
            .map(|x| x.expect("Can't unwrap row if some of columns was taken"))
            .collect()
    }

    /// Unwraps values as is (taken cells will be `None`).
    #[doc(hidden)]
    pub fn unwrap_raw(self) -> Vec<Option<Value>> {
        self.values
    }

    #[doc(hidden)]
    pub fn place(&mut self, index: usize, value: Value) {
        self.values[index] = Some(value);
    }
}

impl Index<usize> for Row {
    type Output = Value;

    fn index(&self, index: usize) -> &Value {
        self.values[index].as_ref().unwrap()
    }
}

impl<'a> Index<&'a str> for Row {
    type Output = Value;

    fn index<'r>(&'r self, index: &'a str) -> &'r Value {
        for (i, column) in self.columns.iter().enumerate() {
            if column.name_ref() == index.as_bytes() {
                return self.values[i].as_ref().unwrap();
            }
        }
        panic!("No such column: `{}` in row {:?}", index, self);
    }
}

/// Things that may be used as an index of a row column.
pub trait ColumnIndex {
    fn idx(&self, columns: &[Column]) -> Option<usize>;
}

impl ColumnIndex for usize {
    fn idx(&self, columns: &[Column]) -> Option<usize> {
        if *self >= columns.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl<'a> ColumnIndex for &'a str {
    fn idx(&self, columns: &[Column]) -> Option<usize> {
        for (i, c) in columns.iter().enumerate() {
            if c.name_ref() == self.as_bytes() {
                return Some(i);
            }
        }
        None
    }
}

/// Row deserializer.
///
/// `S` – serialization side (see [`SerializationSide`]);
/// `P` – protocol.
#[derive(Debug, Clone, PartialEq)]
pub struct RowDeserializer<S, P>(Row, PhantomData<(S, P)>);

impl<S, P> RowDeserializer<S, P> {
    pub fn into_inner(self) -> Row {
        self.0
    }
}

impl<S, P> From<RowDeserializer<S, P>> for Row {
    fn from(x: RowDeserializer<S, P>) -> Self {
        x.0
    }
}

impl<'de, T> MyDeserialize<'de> for RowDeserializer<T, Text> {
    const SIZE: Option<usize> = None;
    type Ctx = Arc<[Column]>;

    fn deserialize(columns: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        let mut values = Vec::with_capacity(columns.len());

        for _ in 0..columns.len() {
            values.push(Some(
                ValueDeserializer::<TextValue>::deserialize((), &mut *buf)?.0,
            ))
        }

        Ok(Self(Row { values, columns }, PhantomData))
    }
}

impl<'de, S: SerializationSide> MyDeserialize<'de> for RowDeserializer<S, Binary> {
    const SIZE: Option<usize> = None;
    type Ctx = Arc<[Column]>;

    fn deserialize(columns: Self::Ctx, buf: &mut ParseBuf<'de>) -> io::Result<Self> {
        use Value::*;

        buf.checked_eat_u8().ok_or_else(unexpected_buf_eof)?;

        let bitmap = NullBitmap::<S, Cow<'de, [u8]>>::deserialize(columns.len(), &mut *buf)?;
        let mut values = Vec::with_capacity(columns.len());

        for (i, column) in columns.iter().enumerate() {
            if bitmap.is_null(i) {
                values.push(Some(NULL))
            } else {
                values.push(Some(
                    ValueDeserializer::<BinValue>::deserialize(
                        (column.column_type(), column.flags()),
                        &mut *buf,
                    )?
                    .0,
                ));
            }
        }

        Ok(Self(Row { values, columns }, PhantomData))
    }
}
