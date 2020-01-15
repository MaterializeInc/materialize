// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![allow(missing_docs)]

use std::fmt;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use repr::decimal::Significand;
use repr::{ColumnType, Datum, RelationType, Row, RowArena, ScalarType};

use crate::EvalEnv;

// TODO(jamii) be careful about overflow in sum/avg
// see https://timely.zulipchat.com/#narrow/stream/186635-engineering/topic/additional.20work/near/163507435

fn max_int32<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<i32> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_int32())
        .max();
    Datum::from(x)
}

fn max_int64<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<i64> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_int64())
        .max();
    Datum::from(x)
}

fn max_float32<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<OrderedFloat<f32>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_ordered_float32())
        .max();
    Datum::from(x)
}

fn max_float64<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<OrderedFloat<f64>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_ordered_float64())
        .max();
    Datum::from(x)
}

fn max_decimal<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<Significand> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_decimal())
        .max();
    Datum::from(x)
}

fn max_bool<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<bool> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_bool())
        .max();
    Datum::from(x)
}

fn max_string<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    match datums
        .into_iter()
        .filter(|d| !d.is_null())
        .max_by(|a, b| a.unwrap_str().cmp(&b.unwrap_str()))
    {
        Some(datum) => datum,
        None => Datum::Null,
    }
}

fn max_date<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<NaiveDate> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_date())
        .max();
    Datum::from(x)
}

fn max_timestamp<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<NaiveDateTime> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_timestamp())
        .max();
    Datum::from(x)
}

fn max_timestamptz<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<DateTime<Utc>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_timestamptz())
        .max();
    Datum::from(x)
}

fn max_null<'a, I>(_datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    Datum::Null
}

fn min_int32<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<i32> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_int32())
        .min();
    Datum::from(x)
}

fn min_int64<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<i64> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_int64())
        .min();
    Datum::from(x)
}

fn min_float32<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<OrderedFloat<f32>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_ordered_float32())
        .min();
    Datum::from(x)
}

fn min_float64<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<OrderedFloat<f64>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_ordered_float64())
        .min();
    Datum::from(x)
}

fn min_decimal<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<Significand> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_decimal())
        .min();
    Datum::from(x)
}

fn min_bool<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<bool> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_bool())
        .min();
    Datum::from(x)
}

fn min_string<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    match datums
        .into_iter()
        .filter(|d| !d.is_null())
        .min_by(|a, b| a.unwrap_str().cmp(&b.unwrap_str()))
    {
        Some(datum) => datum,
        None => Datum::Null,
    }
}

fn min_date<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<NaiveDate> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_date())
        .min();
    Datum::from(x)
}

fn min_timestamp<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<NaiveDateTime> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_timestamp())
        .min();
    Datum::from(x)
}

fn min_timestamptz<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: Option<DateTime<Utc>> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| d.unwrap_timestamptz())
        .min();
    Datum::from(x)
}

fn min_null<'a, I>(_datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    Datum::Null
}

fn sum_int32<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x: i32 = datums.map(|d| d.unwrap_int32()).sum();
        Datum::from(x)
    }
}

fn sum_int64<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x: i64 = datums.map(|d| d.unwrap_int64()).sum();
        Datum::from(x)
    }
}

fn sum_float32<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x: f32 = datums.map(|d| d.unwrap_float32()).sum();
        Datum::from(x)
    }
}

fn sum_float64<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x: f64 = datums.map(|d| d.unwrap_float64()).sum();
        Datum::from(x)
    }
}

fn sum_decimal<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let sum: Significand = datums.map(|d| d.unwrap_decimal()).sum();
        Datum::from(sum)
    }
}

fn sum_null<'a, I>(_datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    Datum::Null
}

fn count<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: i64 = datums.into_iter().filter(|d| !d.is_null()).count() as i64;
    Datum::from(x)
}

fn count_all<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: i64 = datums.into_iter().count() as i64;
    Datum::from(x)
}

fn any<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    datums
        .into_iter()
        .fold(Datum::False, |a, b| crate::scalar::func::or(a, b))
}

fn all<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    datums
        .into_iter()
        .fold(Datum::True, |a, b| crate::scalar::func::and(a, b))
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
    MaxTimestampTz,
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
    MinTimestampTz,
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
    pub fn eval<'a, I>(
        &self,
        datums: I,
        _env: &'a EvalEnv,
        _temp_storage: &'a RowArena,
    ) -> Datum<'a>
    where
        I: IntoIterator<Item = Datum<'a>>,
    {
        match self {
            AggregateFunc::MaxInt32 => max_int32(datums),
            AggregateFunc::MaxInt64 => max_int64(datums),
            AggregateFunc::MaxFloat32 => max_float32(datums),
            AggregateFunc::MaxFloat64 => max_float64(datums),
            AggregateFunc::MaxDecimal => max_decimal(datums),
            AggregateFunc::MaxBool => max_bool(datums),
            AggregateFunc::MaxString => max_string(datums),
            AggregateFunc::MaxDate => max_date(datums),
            AggregateFunc::MaxTimestamp => max_timestamp(datums),
            AggregateFunc::MaxTimestampTz => max_timestamptz(datums),
            AggregateFunc::MaxNull => max_null(datums),
            AggregateFunc::MinInt32 => min_int32(datums),
            AggregateFunc::MinInt64 => min_int64(datums),
            AggregateFunc::MinFloat32 => min_float32(datums),
            AggregateFunc::MinFloat64 => min_float64(datums),
            AggregateFunc::MinDecimal => min_decimal(datums),
            AggregateFunc::MinBool => min_bool(datums),
            AggregateFunc::MinString => min_string(datums),
            AggregateFunc::MinDate => min_date(datums),
            AggregateFunc::MinTimestamp => min_timestamp(datums),
            AggregateFunc::MinTimestampTz => min_timestamptz(datums),
            AggregateFunc::MinNull => min_null(datums),
            AggregateFunc::SumInt32 => sum_int32(datums),
            AggregateFunc::SumInt64 => sum_int64(datums),
            AggregateFunc::SumFloat32 => sum_float32(datums),
            AggregateFunc::SumFloat64 => sum_float64(datums),
            AggregateFunc::SumDecimal => sum_decimal(datums),
            AggregateFunc::SumNull => sum_null(datums),
            AggregateFunc::Count => count(datums),
            AggregateFunc::CountAll => count_all(datums),
            AggregateFunc::Any => any(datums),
            AggregateFunc::All => all(datums),
        }
    }

    pub fn default(&self) -> Datum<'static> {
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
    pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
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

fn jsonb_each<'a>(a: Datum<'a>) -> Vec<Row> {
    match a {
        Datum::Dict(dict) => dict
            .iter()
            .map(|(k, v)| Row::pack(&[Datum::String(k), v]))
            .collect(),
        _ => vec![],
    }
}

fn jsonb_object_keys<'a>(a: Datum<'a>) -> Vec<Row> {
    match a {
        Datum::Dict(dict) => dict
            .iter()
            .map(|(k, _)| Row::pack(&[Datum::String(k)]))
            .collect(),
        _ => vec![],
    }
}

fn jsonb_array_elements<'a>(a: Datum<'a>) -> Vec<Row> {
    match a {
        Datum::List(list) => list.iter().map(|e| Row::pack(&[e])).collect(),
        _ => vec![],
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
            AggregateFunc::MaxTimestampTz => f.write_str("max"),
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
            AggregateFunc::MinTimestampTz => f.write_str("min"),
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum UnaryTableFunc {
    JsonbEach,
    JsonbObjectKeys,
    JsonbArrayElements,
}

impl UnaryTableFunc {
    pub fn eval<'a>(
        &'a self,
        datum: Datum<'a>,
        _env: &'a EvalEnv,
        _temp_storage: &'a RowArena,
    ) -> Vec<Row> {
        match self {
            UnaryTableFunc::JsonbEach => jsonb_each(datum),
            UnaryTableFunc::JsonbObjectKeys => jsonb_object_keys(datum),
            UnaryTableFunc::JsonbArrayElements => jsonb_array_elements(datum),
        }
    }

    pub fn output_type(&self, _input_type: &ColumnType) -> RelationType {
        RelationType::new(match self {
            UnaryTableFunc::JsonbEach => vec![
                ColumnType::new(ScalarType::String),
                ColumnType::new(ScalarType::Jsonb),
            ],
            UnaryTableFunc::JsonbObjectKeys => vec![ColumnType::new(ScalarType::String)],
            UnaryTableFunc::JsonbArrayElements => vec![ColumnType::new(ScalarType::Jsonb)],
        })
    }

    pub fn output_arity(&self) -> usize {
        match self {
            UnaryTableFunc::JsonbEach => 2,
            UnaryTableFunc::JsonbObjectKeys => 1,
            UnaryTableFunc::JsonbArrayElements => 1,
        }
    }
}

impl fmt::Display for UnaryTableFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UnaryTableFunc::JsonbEach => f.write_str("jsonb_each"),
            UnaryTableFunc::JsonbObjectKeys => f.write_str("jsonb_object_keys"),
            UnaryTableFunc::JsonbArrayElements => f.write_str("jsonb_array_elements"),
        }
    }
}
