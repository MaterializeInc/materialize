// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![allow(missing_docs)]

use std::fmt;

use chrono::{NaiveDate, NaiveDateTime};
use ordered_float::OrderedFloat;
use pretty::{BoxDoc, Doc};
use serde::{Deserialize, Serialize};

use repr::decimal::Significand;
use repr::{ColumnType, Datum, ScalarType};

// TODO(jamii) be careful about overflow in sum/avg
// see https://timely.zulipchat.com/#narrow/stream/186635-engineering/topic/additional.20work/near/163507435

pub fn max_int32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<i32> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_int32())
        .max();
    Datum::from(x)
}

pub fn max_int64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<i64> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_int64())
        .max();
    Datum::from(x)
}

pub fn max_float32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<OrderedFloat<f32>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_ordered_float32())
        .max();
    Datum::from(x)
}

pub fn max_float64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<OrderedFloat<f64>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_ordered_float64())
        .max();
    Datum::from(x)
}

pub fn max_decimal<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<Significand> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_decimal())
        .max();
    Datum::from(x)
}

pub fn max_bool<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<bool> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_bool())
        .max();
    Datum::from(x)
}

pub fn max_string<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<String> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_string())
        .max();
    Datum::from(x)
}

pub fn max_date<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<NaiveDate> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_date())
        .max();
    Datum::from(x)
}

pub fn max_timestamp<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<NaiveDateTime> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_timestamp())
        .max();
    Datum::from(x)
}

pub fn max_null<I>(_datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    Datum::Null
}

pub fn min_int32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<i32> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_int32())
        .min();
    Datum::from(x)
}

pub fn min_int64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<i64> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_int64())
        .min();
    Datum::from(x)
}

pub fn min_float32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<OrderedFloat<f32>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_ordered_float32())
        .min();
    Datum::from(x)
}

pub fn min_float64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<OrderedFloat<f64>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_ordered_float64())
        .min();
    Datum::from(x)
}

pub fn min_decimal<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<Significand> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_decimal())
        .min();
    Datum::from(x)
}

pub fn min_bool<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<bool> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_bool())
        .min();
    Datum::from(x)
}

pub fn min_string<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<String> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_string())
        .min();
    Datum::from(x)
}

pub fn min_date<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<NaiveDate> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_date())
        .min();
    Datum::from(x)
}

pub fn min_timestamp<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<NaiveDateTime> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_timestamp())
        .min();
    Datum::from(x)
}

pub fn min_null<I>(_datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    Datum::Null
}

pub fn sum_int32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x: i32 = datums.map(|d| d.unwrap_int32()).sum();
        Datum::from(x)
    }
}

pub fn sum_int64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x: i64 = datums.map(|d| d.unwrap_int64()).sum();
        Datum::from(x)
    }
}

pub fn sum_float32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x: f32 = datums.map(|d| d.unwrap_float32()).sum();
        Datum::from(x)
    }
}

pub fn sum_float64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x: f64 = datums.map(|d| d.unwrap_float64()).sum();
        Datum::from(x)
    }
}

pub fn sum_decimal<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let sum: Significand = datums.map(|d| d.unwrap_decimal()).sum();
        Datum::from(sum)
    }
}

pub fn sum_null<I>(_datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    Datum::Null
}

pub fn count<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: i64 = datums.into_iter().filter(|d| !d.is_null()).count() as i64;
    Datum::from(x)
}

pub fn count_all<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: i64 = datums.into_iter().count() as i64;
    Datum::from(x)
}

pub fn any<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    datums
        .into_iter()
        .fold(Datum::False, crate::scalar::func::or)
}

pub fn all<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    datums
        .into_iter()
        .fold(Datum::True, crate::scalar::func::and)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum AggregateFunc {
    MaxInt32,
    MaxInt64,
    MaxFloat32,
    MaxFloat64,
    MaxDecimal,
    MaxBool,
    MaxString,
    MaxDate,
    MaxTimestamp,
    MaxNull,
    MinInt32,
    MinInt64,
    MinFloat32,
    MinFloat64,
    MinDecimal,
    MinBool,
    MinString,
    MinDate,
    MinTimestamp,
    MinNull,
    SumInt32,
    SumInt64,
    SumFloat32,
    SumFloat64,
    SumDecimal,
    SumNull,
    Count,
    CountAll, // COUNT(*) counts nulls too
    Any,
    All,
}

impl AggregateFunc {
    pub fn func<I>(self) -> fn(I) -> Datum
    where
        I: IntoIterator<Item = Datum>,
    {
        match self {
            AggregateFunc::MaxInt32 => max_int32,
            AggregateFunc::MaxInt64 => max_int64,
            AggregateFunc::MaxFloat32 => max_float32,
            AggregateFunc::MaxFloat64 => max_float64,
            AggregateFunc::MaxDecimal => max_decimal,
            AggregateFunc::MaxBool => max_bool,
            AggregateFunc::MaxString => max_string,
            AggregateFunc::MaxDate => max_date,
            AggregateFunc::MaxTimestamp => max_timestamp,
            AggregateFunc::MaxNull => max_null,
            AggregateFunc::MinInt32 => min_int32,
            AggregateFunc::MinInt64 => min_int64,
            AggregateFunc::MinFloat32 => min_float32,
            AggregateFunc::MinFloat64 => min_float64,
            AggregateFunc::MinDecimal => min_decimal,
            AggregateFunc::MinBool => min_bool,
            AggregateFunc::MinString => min_string,
            AggregateFunc::MinDate => min_date,
            AggregateFunc::MinTimestamp => min_timestamp,
            AggregateFunc::MinNull => min_null,
            AggregateFunc::SumInt32 => sum_int32,
            AggregateFunc::SumInt64 => sum_int64,
            AggregateFunc::SumFloat32 => sum_float32,
            AggregateFunc::SumFloat64 => sum_float64,
            AggregateFunc::SumDecimal => sum_decimal,
            AggregateFunc::SumNull => sum_null,
            AggregateFunc::Count => count,
            AggregateFunc::CountAll => count_all,
            AggregateFunc::Any => any,
            AggregateFunc::All => all,
        }
    }

    pub fn default(self) -> Datum {
        match self {
            AggregateFunc::Count | AggregateFunc::CountAll => Datum::Int64(0),
            AggregateFunc::Any => Datum::False,
            AggregateFunc::All => Datum::True,
            _ => Datum::Null,
        }
    }

    /// The output column type for the result of an aggregation.
    ///
    /// The output column type also contains nullability information, which
    /// is (without further information) true for aggregations that are not
    /// counts.
    pub fn output_type(self, input_type: ColumnType) -> ColumnType {
        let scalar_type = match self {
            AggregateFunc::Count => ScalarType::Int64,
            AggregateFunc::CountAll => ScalarType::Int64,
            AggregateFunc::Any => ScalarType::Bool,
            AggregateFunc::All => ScalarType::Bool,
            _ => input_type.scalar_type,
        };
        let nullable = match self {
            AggregateFunc::Count | AggregateFunc::CountAll => false,
            // max/min/sum return null on empty sets
            _ => true,
        };
        ColumnType::new(scalar_type).nullable(nullable)
    }
}

impl fmt::Display for AggregateFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AggregateFunc::MaxInt32 => f.write_str("max"),
            AggregateFunc::MaxInt64 => f.write_str("max"),
            AggregateFunc::MaxFloat32 => f.write_str("max"),
            AggregateFunc::MaxFloat64 => f.write_str("max"),
            AggregateFunc::MaxDecimal => f.write_str("max"),
            AggregateFunc::MaxBool => f.write_str("max"),
            AggregateFunc::MaxString => f.write_str("max"),
            AggregateFunc::MaxDate => f.write_str("max"),
            AggregateFunc::MaxTimestamp => f.write_str("max"),
            AggregateFunc::MaxNull => f.write_str("max"),
            AggregateFunc::MinInt32 => f.write_str("min"),
            AggregateFunc::MinInt64 => f.write_str("min"),
            AggregateFunc::MinFloat32 => f.write_str("min"),
            AggregateFunc::MinFloat64 => f.write_str("min"),
            AggregateFunc::MinDecimal => f.write_str("min"),
            AggregateFunc::MinBool => f.write_str("min"),
            AggregateFunc::MinString => f.write_str("min"),
            AggregateFunc::MinDate => f.write_str("min"),
            AggregateFunc::MinTimestamp => f.write_str("min"),
            AggregateFunc::MinNull => f.write_str("min"),
            AggregateFunc::SumInt32 => f.write_str("sum"),
            AggregateFunc::SumInt64 => f.write_str("sum"),
            AggregateFunc::SumFloat32 => f.write_str("sum"),
            AggregateFunc::SumFloat64 => f.write_str("sum"),
            AggregateFunc::SumDecimal => f.write_str("sum"),
            AggregateFunc::SumNull => f.write_str("sum"),
            AggregateFunc::Count => f.write_str("count"),
            AggregateFunc::CountAll => f.write_str("countall"),
            AggregateFunc::Any => f.write_str("any"),
            AggregateFunc::All => f.write_str("all"),
        }
    }
}

impl<'a> From<&'a AggregateFunc> for Doc<'a, BoxDoc<'a, ()>, ()> {
    fn from(f: &'a AggregateFunc) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        Doc::text(f.to_string())
    }
}
