// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use repr::decimal::Significand;
use repr::Datum;

// TODO(jamii) be careful about overflow in sum/avg
// see https://timely.zulipchat.com/#narrow/stream/186635-engineering/topic/additional.20work/near/163507435

pub fn avg_int32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut sum: i32 = 0;
    let mut len: usize = 0;
    for d in datums.into_iter() {
        if !d.is_null() {
            sum += d.unwrap_int32();
            len += 1;
        }
    }
    if len == 0 {
        Datum::Null
    } else {
        Datum::from(sum / (len as i32))
    }
}

pub fn avg_int64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut sum: i64 = 0;
    let mut len: usize = 0;
    for d in datums.into_iter() {
        if !d.is_null() {
            sum += d.unwrap_int64();
            len += 1;
        }
    }
    if len == 0 {
        Datum::Null
    } else {
        Datum::from(sum / (len as i64))
    }
}

pub fn avg_float32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut sum: f32 = 0.0;
    let mut len: usize = 0;
    for d in datums.into_iter() {
        if !d.is_null() {
            sum += d.unwrap_float32();
            len += 1;
        }
    }
    if len == 0 {
        Datum::Null
    } else {
        Datum::from(sum / (len as f32))
    }
}

pub fn avg_float64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut sum: f64 = 0.0;
    let mut len: usize = 0;
    for d in datums.into_iter() {
        if !d.is_null() {
            sum += d.unwrap_float64();
            len += 1;
        }
    }
    if len == 0 {
        Datum::Null
    } else {
        Datum::from(sum / (len as f64))
    }
}

pub fn avg_decimal<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut sum = Significand::new(0);
    let mut len = Significand::new(0);
    for d in datums.into_iter() {
        if !d.is_null() {
            sum += d.unwrap_decimal();
            len += 1;
        }
    }
    if len == 0 {
        Datum::Null
    } else {
        // TODO(benesch): This should use the same decimal division path as the
        // planner, rather than hardcoding a 6 digit increase in the scale
        // (#212).
        Datum::from(sum * 1_000_000 / len)
    }
}

pub fn avg_null<I>(_datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    Datum::Null
}

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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum AggregateFunc {
    AvgInt32,
    AvgInt64,
    AvgFloat32,
    AvgFloat64,
    AvgDecimal,
    AvgNull,
    MaxInt32,
    MaxInt64,
    MaxFloat32,
    MaxFloat64,
    MaxBool,
    MaxString,
    MaxNull,
    MinInt32,
    MinInt64,
    MinFloat32,
    MinFloat64,
    MinBool,
    MinString,
    MinNull,
    SumInt32,
    SumInt64,
    SumFloat32,
    SumFloat64,
    SumDecimal,
    SumNull,
    Count,
    CountAll, // COUNT(*) counts nulls too
}

impl AggregateFunc {
    pub fn func<I>(self) -> fn(I) -> Datum
    where
        I: IntoIterator<Item = Datum>,
    {
        match self {
            AggregateFunc::AvgInt32 => avg_int32,
            AggregateFunc::AvgInt64 => avg_int64,
            AggregateFunc::AvgFloat32 => avg_float32,
            AggregateFunc::AvgFloat64 => avg_float64,
            AggregateFunc::AvgDecimal => avg_decimal,
            AggregateFunc::AvgNull => avg_null,
            AggregateFunc::MaxInt32 => max_int32,
            AggregateFunc::MaxInt64 => max_int64,
            AggregateFunc::MaxFloat32 => max_float32,
            AggregateFunc::MaxFloat64 => max_float64,
            AggregateFunc::MaxBool => max_bool,
            AggregateFunc::MaxString => max_string,
            AggregateFunc::MaxNull => max_null,
            AggregateFunc::MinInt32 => min_int32,
            AggregateFunc::MinInt64 => min_int64,
            AggregateFunc::MinFloat32 => min_float32,
            AggregateFunc::MinFloat64 => min_float64,
            AggregateFunc::MinBool => min_bool,
            AggregateFunc::MinString => min_string,
            AggregateFunc::MinNull => min_null,
            AggregateFunc::SumInt32 => sum_int32,
            AggregateFunc::SumInt64 => sum_int64,
            AggregateFunc::SumFloat32 => sum_float32,
            AggregateFunc::SumFloat64 => sum_float64,
            AggregateFunc::SumDecimal => sum_decimal,
            AggregateFunc::SumNull => sum_null,
            AggregateFunc::Count => count,
            AggregateFunc::CountAll => count_all,
        }
    }

    pub fn is_nullable(self) -> bool {
        match self {
            AggregateFunc::Count => false,
            // avg/max/min/sum return null on empty sets
            _ => true,
        }
    }

    pub fn default(self) -> Datum {
        match self {
            AggregateFunc::Count | AggregateFunc::CountAll => Datum::Int64(0),
            _ => Datum::Null,
        }
    }
}
