// Copyright (c) 2017 Anatoly Ikorsky
//
// Licensed under the Apache License, Version 2.0
// <LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0> or the MIT
// license <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. All files in the project carrying such notice may not be copied,
// modified, or distributed except according to those terms.

use crate::{
    row::Row,
    value::{
        convert::{FromValue, FromValueError},
        Value,
    },
};

use std::{any::type_name, error::Error, fmt};

pub mod frunk;

/// `FromRow` conversion error.
#[derive(Debug, Clone, PartialEq)]
pub struct FromRowError(pub Row);

impl fmt::Display for FromRowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Couldn't convert the row `{:?}` to a desired type",
            self.0
        )
    }
}

impl Error for FromRowError {
    fn description(&self) -> &str {
        "Couldn't convert the row to a desired type"
    }
}

/// Will *panic* if could not convert `row` to `T`.
pub fn from_row<T: FromRow>(row: Row) -> T {
    FromRow::from_row(row)
}

/// Will return `Err(row)` if could not convert `row` to `T`
pub fn from_row_opt<T: FromRow>(row: Row) -> Result<T, FromRowError> {
    FromRow::from_row_opt(row)
}

/// Trait to convert `Row` into a tuple of `FromValue` implementors up to arity 12.
///
/// This trait is convenient way to convert mysql row to a tuple or rust types and relies on
/// `FromValue` trait, i.e. calling `from_row::<(T, U)>(row)` is similar to calling
/// `(T::from_value(column_1), U::from_value(column_2))`.
///
/// Note that conversion will always fail if any of columns was taken using `Row::take` method.
///
/// Conversion of individual columns of a row may fail. In this case `from_row` will panic, and
/// `from_row_opt` will roll back conversion and return original row.
///
/// Concrete types of columns in a row is usually known to a programmer so `from_value` should never
/// panic if types specified correctly. This means that column which holds `NULL` should correspond
/// to a type wrapped in `Option`, `String` is used only with column which hold correct utf8, size
/// and signedness of a numeric type should match to a value stored in a column and so on.
///
/// ```ignore
/// // Consider columns in the row is: Bytes(<some binary data>), NULL and Int(1024)
/// from_row::<(String, u8, u8)>(row) // this will panic because of invalid utf8 in first column.
/// from_row::<(Vec<u8>, u8, u8)>(row) // this will panic because of a NULL in second column.
/// from_row::<(Vec<u8>, Option<u8>, u8)>(row) // this will panic because 1024 does not fit in u8.
/// from_row::<(Vec<u8>)>(row) // this will panic because number of columns != arity of a tuple.
/// from_row::<(Vec<u8>, Option<u8>, u16, Option<u8>)>(row) // same reason of panic as previous.
///
/// from_row::<(Vec<u8>, Option<u8>, u16)>(row) // this'll work and return (vec![..], None, 1024u16)
/// ```
pub trait FromRow {
    fn from_row(row: Row) -> Self
    where
        Self: Sized,
    {
        match Self::from_row_opt(row) {
            Ok(x) => x,
            Err(FromRowError(row)) => panic!(
                "Couldn't convert {:?} to type {}. (see FromRow documentation)",
                row,
                type_name::<Self>(),
            ),
        }
    }

    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized;
}

macro_rules! take_or_place {
    ($row:expr, $index:expr, $t:ident) => (
        match $row.take($index) {
            Some(value) => {
                match $t::get_intermediate(value) {
                    Ok(ir) => ir,
                    Err(FromValueError(value)) => {
                        $row.place($index, value);
                        return Err(FromRowError($row));
                    },
                }
            },
            None => return Err(FromRowError($row)),
        }
    );
    ($row:expr, $index:expr, $t:ident, $( [$idx:expr, $ir:expr] ),*) => (
        match $row.take($index) {
            Some(value) => {
                match $t::get_intermediate(value) {
                    Ok(ir) => ir,
                    Err(FromValueError(value)) => {
                        $($row.place($idx, Into::<Value>::into($ir));)*
                        $row.place($index, value);
                        return Err(FromRowError($row));
                    },
                }
            },
            None => return Err(FromRowError($row)),
        }
    );
}

impl FromRow for Row {
    fn from_row(row: Row) -> Self {
        row
    }

    fn from_row_opt(row: Row) -> Result<Self, FromRowError>
    where
        Self: Sized,
    {
        Ok(row)
    }
}

impl<T> FromRow for T
where
    T: FromValue,
{
    fn from_row_opt(mut row: Row) -> Result<T, FromRowError> {
        if row.len() == 1 {
            Ok(take_or_place!(row, 0, T).into())
        } else {
            Err(FromRowError(row))
        }
    }
}

impl<T1> FromRow for (T1,)
where
    T1: FromValue,
{
    fn from_row_opt(mut row: Row) -> Result<(T1,), FromRowError> {
        if row.len() == 1 {
            Ok((take_or_place!(row, 0, T1).into(),))
        } else {
            Err(FromRowError(row))
        }
    }
}

impl<T1, T2> FromRow for (T1, T2)
where
    T1: FromValue,
    T2: FromValue,
    T1::Intermediate: Into<Value>,
{
    fn from_row_opt(mut row: Row) -> Result<(T1, T2), FromRowError> {
        if row.len() != 2 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        Ok((Into::<T1>::into(ir1), Into::<T2>::into(ir2)))
    }
}

impl<T1, T2, T3> FromRow for (T1, T2, T3)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
{
    fn from_row_opt(mut row: Row) -> Result<(T1, T2, T3), FromRowError> {
        if row.len() != 3 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        Ok((Into::<T1>::into(ir1), Into::<T2>::into(ir2), ir3.into()))
    }
}

impl<T1, T2, T3, T4> FromRow for (T1, T2, T3, T4)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
    T3::Intermediate: Into<Value>,
{
    fn from_row_opt(mut row: Row) -> Result<(T1, T2, T3, T4), FromRowError> {
        if row.len() != 4 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        Ok((
            Into::<T1>::into(ir1),
            Into::<T2>::into(ir2),
            Into::<T3>::into(ir3),
            ir4.into(),
        ))
    }
}

impl<T1, T2, T3, T4, T5> FromRow for (T1, T2, T3, T4, T5)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
    T3::Intermediate: Into<Value>,
    T4::Intermediate: Into<Value>,
{
    fn from_row_opt(mut row: Row) -> Result<(T1, T2, T3, T4, T5), FromRowError> {
        if row.len() != 5 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        Ok((
            Into::<T1>::into(ir1),
            Into::<T2>::into(ir2),
            Into::<T3>::into(ir3),
            Into::<T4>::into(ir4),
            ir5.into(),
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6> FromRow for (T1, T2, T3, T4, T5, T6)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
    T3::Intermediate: Into<Value>,
    T4::Intermediate: Into<Value>,
    T5::Intermediate: Into<Value>,
{
    fn from_row_opt(mut row: Row) -> Result<(T1, T2, T3, T4, T5, T6), FromRowError> {
        if row.len() != 6 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        Ok((
            Into::<T1>::into(ir1),
            Into::<T2>::into(ir2),
            Into::<T3>::into(ir3),
            Into::<T4>::into(ir4),
            Into::<T5>::into(ir5),
            ir6.into(),
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6, T7> FromRow for (T1, T2, T3, T4, T5, T6, T7)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
    T3::Intermediate: Into<Value>,
    T4::Intermediate: Into<Value>,
    T5::Intermediate: Into<Value>,
    T6::Intermediate: Into<Value>,
{
    fn from_row_opt(mut row: Row) -> Result<(T1, T2, T3, T4, T5, T6, T7), FromRowError> {
        if row.len() != 7 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row,
            6,
            T7,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6]
        );
        Ok((
            Into::<T1>::into(ir1),
            Into::<T2>::into(ir2),
            Into::<T3>::into(ir3),
            Into::<T4>::into(ir4),
            Into::<T5>::into(ir5),
            Into::<T6>::into(ir6),
            ir7.into(),
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
    T3::Intermediate: Into<Value>,
    T4::Intermediate: Into<Value>,
    T5::Intermediate: Into<Value>,
    T6::Intermediate: Into<Value>,
    T7::Intermediate: Into<Value>,
{
    fn from_row_opt(mut row: Row) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8), FromRowError> {
        if row.len() != 8 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row,
            6,
            T7,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6]
        );
        let ir8 = take_or_place!(
            row,
            7,
            T8,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7]
        );
        Ok((
            Into::<T1>::into(ir1),
            Into::<T2>::into(ir2),
            Into::<T3>::into(ir3),
            Into::<T4>::into(ir4),
            Into::<T5>::into(ir5),
            Into::<T6>::into(ir6),
            Into::<T7>::into(ir7),
            ir8.into(),
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
    T9: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
    T3::Intermediate: Into<Value>,
    T4::Intermediate: Into<Value>,
    T5::Intermediate: Into<Value>,
    T6::Intermediate: Into<Value>,
    T7::Intermediate: Into<Value>,
    T8::Intermediate: Into<Value>,
{
    fn from_row_opt(mut row: Row) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8, T9), FromRowError> {
        if row.len() != 9 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row,
            6,
            T7,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6]
        );
        let ir8 = take_or_place!(
            row,
            7,
            T8,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7]
        );
        let ir9 = take_or_place!(
            row,
            8,
            T9,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8]
        );
        Ok((
            Into::<T1>::into(ir1),
            Into::<T2>::into(ir2),
            Into::<T3>::into(ir3),
            Into::<T4>::into(ir4),
            Into::<T5>::into(ir5),
            Into::<T6>::into(ir6),
            Into::<T7>::into(ir7),
            Into::<T8>::into(ir8),
            ir9.into(),
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> FromRow for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
    T9: FromValue,
    T10: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
    T3::Intermediate: Into<Value>,
    T4::Intermediate: Into<Value>,
    T5::Intermediate: Into<Value>,
    T6::Intermediate: Into<Value>,
    T7::Intermediate: Into<Value>,
    T8::Intermediate: Into<Value>,
    T9::Intermediate: Into<Value>,
{
    fn from_row_opt(
        mut row: Row,
    ) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10), FromRowError> {
        if row.len() != 10 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row,
            6,
            T7,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6]
        );
        let ir8 = take_or_place!(
            row,
            7,
            T8,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7]
        );
        let ir9 = take_or_place!(
            row,
            8,
            T9,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8]
        );
        let ir10 = take_or_place!(
            row,
            9,
            T10,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8],
            [8, ir9]
        );
        Ok((
            Into::<T1>::into(ir1),
            Into::<T2>::into(ir2),
            Into::<T3>::into(ir3),
            Into::<T4>::into(ir4),
            Into::<T5>::into(ir5),
            Into::<T6>::into(ir6),
            Into::<T7>::into(ir7),
            Into::<T8>::into(ir8),
            Into::<T9>::into(ir9),
            ir10.into(),
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> FromRow
    for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
    T9: FromValue,
    T10: FromValue,
    T11: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
    T3::Intermediate: Into<Value>,
    T4::Intermediate: Into<Value>,
    T5::Intermediate: Into<Value>,
    T6::Intermediate: Into<Value>,
    T7::Intermediate: Into<Value>,
    T8::Intermediate: Into<Value>,
    T9::Intermediate: Into<Value>,
    T10::Intermediate: Into<Value>,
{
    fn from_row_opt(
        mut row: Row,
    ) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11), FromRowError> {
        if row.len() != 11 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row,
            6,
            T7,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6]
        );
        let ir8 = take_or_place!(
            row,
            7,
            T8,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7]
        );
        let ir9 = take_or_place!(
            row,
            8,
            T9,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8]
        );
        let ir10 = take_or_place!(
            row,
            9,
            T10,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8],
            [8, ir9]
        );
        let ir11 = take_or_place!(
            row,
            10,
            T11,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8],
            [8, ir9],
            [9, ir10]
        );
        Ok((
            Into::<T1>::into(ir1),
            Into::<T2>::into(ir2),
            Into::<T3>::into(ir3),
            Into::<T4>::into(ir4),
            Into::<T5>::into(ir5),
            Into::<T6>::into(ir6),
            Into::<T7>::into(ir7),
            Into::<T8>::into(ir8),
            Into::<T9>::into(ir9),
            Into::<T10>::into(ir10),
            ir11.into(),
        ))
    }
}

impl<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> FromRow
    for (T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12)
where
    T1: FromValue,
    T2: FromValue,
    T3: FromValue,
    T4: FromValue,
    T5: FromValue,
    T6: FromValue,
    T7: FromValue,
    T8: FromValue,
    T9: FromValue,
    T10: FromValue,
    T11: FromValue,
    T12: FromValue,
    T1::Intermediate: Into<Value>,
    T2::Intermediate: Into<Value>,
    T3::Intermediate: Into<Value>,
    T4::Intermediate: Into<Value>,
    T5::Intermediate: Into<Value>,
    T6::Intermediate: Into<Value>,
    T7::Intermediate: Into<Value>,
    T8::Intermediate: Into<Value>,
    T9::Intermediate: Into<Value>,
    T10::Intermediate: Into<Value>,
    T11::Intermediate: Into<Value>,
{
    fn from_row_opt(
        mut row: Row,
    ) -> Result<(T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12), FromRowError> {
        if row.len() != 12 {
            return Err(FromRowError(row));
        }
        let ir1 = take_or_place!(row, 0, T1);
        let ir2 = take_or_place!(row, 1, T2, [0, ir1]);
        let ir3 = take_or_place!(row, 2, T3, [0, ir1], [1, ir2]);
        let ir4 = take_or_place!(row, 3, T4, [0, ir1], [1, ir2], [2, ir3]);
        let ir5 = take_or_place!(row, 4, T5, [0, ir1], [1, ir2], [2, ir3], [3, ir4]);
        let ir6 = take_or_place!(row, 5, T6, [0, ir1], [1, ir2], [2, ir3], [3, ir4], [4, ir5]);
        let ir7 = take_or_place!(
            row,
            6,
            T7,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6]
        );
        let ir8 = take_or_place!(
            row,
            7,
            T8,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7]
        );
        let ir9 = take_or_place!(
            row,
            8,
            T9,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8]
        );
        let ir10 = take_or_place!(
            row,
            9,
            T10,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8],
            [8, ir9]
        );
        let ir11 = take_or_place!(
            row,
            10,
            T11,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8],
            [8, ir9],
            [9, ir10]
        );
        let ir12 = take_or_place!(
            row,
            11,
            T12,
            [0, ir1],
            [1, ir2],
            [2, ir3],
            [3, ir4],
            [4, ir5],
            [5, ir6],
            [6, ir7],
            [7, ir8],
            [8, ir9],
            [9, ir10],
            [10, ir11]
        );
        Ok((
            Into::<T1>::into(ir1),
            Into::<T2>::into(ir2),
            Into::<T3>::into(ir3),
            Into::<T4>::into(ir4),
            Into::<T5>::into(ir5),
            Into::<T6>::into(ir6),
            Into::<T7>::into(ir7),
            Into::<T8>::into(ir8),
            Into::<T9>::into(ir9),
            Into::<T10>::into(ir10),
            Into::<T11>::into(ir11),
            ir12.into(),
        ))
    }
}

#[cfg(feature = "nightly")]
#[bench]
fn bench_from_row(bencher: &mut test::Bencher) {
    use std::sync::Arc;

    use crate::{constants::ColumnType, io::WriteMysqlExt, packets::Column, value::Value};

    fn col(name: &str, ty: ColumnType) -> Column {
        let mut payload = b"\x00def".to_vec();
        for _ in 0..5 {
            payload.write_lenenc_str(name.as_bytes()).unwrap();
        }
        payload.extend_from_slice(&b"_\x2d\x00\xff\xff\xff\xff"[..]);
        payload.push(ty as u8);
        payload.extend_from_slice(&b"\x00\x00\x00"[..]);
        Column::read(&payload[..]).unwrap()
    }

    let row = Row {
        values: vec![
            Some(Value::Bytes(b"12.3456789".to_vec())),
            Some(Value::Int(0xF0)),
            Some(Value::Int(0xF000)),
            Some(Value::Int(0xF0000000)),
        ],
        columns: Arc::from(
            vec![
                col("foo", ColumnType::MYSQL_TYPE_STRING),
                col("foo", ColumnType::MYSQL_TYPE_TINY),
                col("foo", ColumnType::MYSQL_TYPE_SHORT),
                col("foo", ColumnType::MYSQL_TYPE_LONG),
            ]
            .into_boxed_slice(),
        ),
    };

    bencher.iter(|| from_row::<(String, u8, u16, u32)>(row.clone()));
}
