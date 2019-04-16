// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

use crate::repr::Datum;

pub fn and(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_bool() && b.unwrap_bool())
}

pub fn or(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_bool() || b.unwrap_bool())
}

pub fn not(a: Datum) -> Datum {
    Datum::from(!a.unwrap_bool())
}

pub fn add_int32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int32() + b.unwrap_int32())
}

pub fn add_int64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int64() + b.unwrap_int64())
}

pub fn add_float32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float32() + b.unwrap_float32())
}

pub fn add_float64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float64() + b.unwrap_float64())
}

pub fn sub_int32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int32() - b.unwrap_int32())
}

pub fn sub_int64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int64() - b.unwrap_int64())
}

pub fn sub_float32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float32() - b.unwrap_float32())
}

pub fn sub_float64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float64() - b.unwrap_float64())
}

pub fn mul_int32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int32() * b.unwrap_int32())
}

pub fn mul_int64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int64() * b.unwrap_int64())
}

pub fn mul_float32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float32() * b.unwrap_float32())
}

pub fn mul_float64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float64() * b.unwrap_float64())
}

pub fn div_int32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int32() / b.unwrap_int32())
}

pub fn div_int64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int64() / b.unwrap_int64())
}

pub fn div_float32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float32() / b.unwrap_float32())
}

pub fn div_float64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float64() / b.unwrap_float64())
}

pub fn mod_int32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int32() % b.unwrap_int32())
}

pub fn mod_int64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_int64() % b.unwrap_int64())
}

pub fn mod_float32(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float32() % b.unwrap_float32())
}

pub fn mod_float64(a: Datum, b: Datum) -> Datum {
    Datum::from(a.unwrap_float64() % b.unwrap_float64())
}

pub fn neg_int32(a: Datum) -> Datum {
    Datum::from(-a.unwrap_int32())
}

pub fn neg_int64(a: Datum) -> Datum {
    Datum::from(-a.unwrap_int64())
}

pub fn neg_float32(a: Datum) -> Datum {
    Datum::from(-a.unwrap_float32())
}

pub fn neg_float64(a: Datum) -> Datum {
    Datum::from(-a.unwrap_float64())
}

pub fn eq(a: Datum, b: Datum) -> Datum {
    Datum::from(a == b)
}

pub fn lt(a: Datum, b: Datum) -> Datum {
    Datum::from(a < b)
}

pub fn lte(a: Datum, b: Datum) -> Datum {
    Datum::from(a <= b)
}

pub fn gt(a: Datum, b: Datum) -> Datum {
    Datum::from(a > b)
}

pub fn gte(a: Datum, b: Datum) -> Datum {
    Datum::from(a >= b)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BinaryFunc {
    And,
    Or,
    AddInt32,
    AddInt64,
    AddFloat32,
    AddFloat64,
    SubInt32,
    SubInt64,
    SubFloat32,
    SubFloat64,
    MulInt32,
    MulInt64,
    MulFloat32,
    MulFloat64,
    DivInt32,
    DivInt64,
    DivFloat32,
    DivFloat64,
    ModInt32,
    ModInt64,
    ModFloat32,
    ModFloat64,
    Eq,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl BinaryFunc {
    pub fn func(self) -> fn(Datum, Datum) -> Datum {
        match self {
            BinaryFunc::And => and,
            BinaryFunc::Or => or,
            BinaryFunc::AddInt32 => add_int32,
            BinaryFunc::AddInt64 => add_int64,
            BinaryFunc::AddFloat32 => add_float32,
            BinaryFunc::AddFloat64 => add_float64,
            BinaryFunc::SubInt32 => sub_int32,
            BinaryFunc::SubInt64 => sub_int64,
            BinaryFunc::SubFloat32 => sub_float32,
            BinaryFunc::SubFloat64 => sub_float64,
            BinaryFunc::MulInt32 => mul_int32,
            BinaryFunc::MulInt64 => mul_int64,
            BinaryFunc::MulFloat32 => mul_float32,
            BinaryFunc::MulFloat64 => mul_float64,
            BinaryFunc::DivInt32 => div_int32,
            BinaryFunc::DivInt64 => div_int64,
            BinaryFunc::DivFloat32 => div_float32,
            BinaryFunc::DivFloat64 => div_float64,
            BinaryFunc::ModInt32 => mod_int32,
            BinaryFunc::ModInt64 => mod_int64,
            BinaryFunc::ModFloat32 => mod_float32,
            BinaryFunc::ModFloat64 => mod_float64,
            BinaryFunc::Eq => eq,
            BinaryFunc::Lt => lt,
            BinaryFunc::Lte => lte,
            BinaryFunc::Gt => gt,
            BinaryFunc::Gte => gte,
        }
    }
}

pub fn is_null(a: Datum) -> Datum {
    Datum::from(a == Datum::Null)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum UnaryFunc {
    Not,
    IsNull,
    NegInt32,
    NegInt64,
    NegFloat32,
    NegFloat64,
}

impl UnaryFunc {
    pub fn func(self) -> fn(Datum) -> Datum {
        match self {
            UnaryFunc::Not => not,
            UnaryFunc::IsNull => is_null,
            UnaryFunc::NegInt32 => neg_int32,
            UnaryFunc::NegInt64 => neg_int64,
            UnaryFunc::NegFloat32 => neg_float32,
            UnaryFunc::NegFloat64 => neg_float64,
        }
    }
}

// TODO(jamii) be careful about overflow in sum/avg
// see https://timely.zulipchat.com/#narrow/stream/186635-engineering/topic/additional.20work/near/163507435

pub fn avg_int32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut sum: i32 = 0;
    let mut len: usize = 0;
    for d in datums.into_iter() {
        sum += d.unwrap_int32();
        len += 1;
    }
    if len == 0 {
        Datum::Null
    } else {
        Datum::from(f64::from(sum) / len as f64)
    }
}

pub fn avg_int64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut sum: i64 = 0;
    let mut len: usize = 0;
    for d in datums.into_iter() {
        sum += d.unwrap_int64();
        len += 1;
    }
    if len == 0 {
        Datum::Null
    } else {
        // TODO(jamii) check for truncation
        Datum::from(sum as f64 / len as f64)
    }
}

pub fn avg_float32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut sum: f32 = 0.0;
    let mut len: usize = 0;
    for d in datums.into_iter() {
        sum += d.unwrap_float32();
        len += 1;
    }
    if len == 0 {
        Datum::Null
    } else {
        Datum::from(f64::from(sum) / len as f64)
    }
}

pub fn avg_float64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let mut sum: f64 = 0.0;
    let mut len: usize = 0;
    for d in datums.into_iter() {
        sum += d.unwrap_float64();
        len += 1;
    }
    if len == 0 {
        Datum::Null
    } else {
        Datum::from(sum as f64 / len as f64)
    }
}

pub fn max_int32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<i32> = datums.into_iter().map(|d| d.unwrap_int32()).max();
    Datum::from(x)
}

pub fn max_int64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<i64> = datums.into_iter().map(|d| d.unwrap_int64()).max();
    Datum::from(x)
}

pub fn max_float32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<OrderedFloat<f32>> = datums.into_iter().map(|d| d.unwrap_ordered_float32()).max();
    Datum::from(x)
}

pub fn max_float64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<OrderedFloat<f64>> = datums.into_iter().map(|d| d.unwrap_ordered_float64()).max();
    Datum::from(x)
}

pub fn min_int32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<i32> = datums.into_iter().map(|d| d.unwrap_int32()).min();
    Datum::from(x)
}

pub fn min_int64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<i64> = datums.into_iter().map(|d| d.unwrap_int64()).min();
    Datum::from(x)
}

pub fn min_float32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<OrderedFloat<f32>> = datums.into_iter().map(|d| d.unwrap_ordered_float32()).min();
    Datum::from(x)
}

pub fn min_float64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: Option<OrderedFloat<f64>> = datums.into_iter().map(|d| d.unwrap_ordered_float64()).min();
    Datum::from(x)
}

pub fn sum_int32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: i32 = datums.into_iter().map(|d| d.unwrap_int32()).sum();
    Datum::from(x)
}

pub fn sum_int64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: i64 = datums.into_iter().map(|d| d.unwrap_int64()).sum();
    Datum::from(x)
}

pub fn sum_float32<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: f32 = datums.into_iter().map(|d| d.unwrap_float32()).sum();
    Datum::from(x)
}

pub fn sum_float64<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: f64 = datums.into_iter().map(|d| d.unwrap_float64()).sum();
    Datum::from(x)
}

pub fn count<I>(datums: I) -> Datum
where
    I: IntoIterator<Item = Datum>,
{
    let x: i64 = datums.into_iter().count() as i64;
    Datum::from(x)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunc {
    AvgInt32,
    AvgInt64,
    AvgFloat32,
    AvgFloat64,
    MaxInt32,
    MaxInt64,
    MaxFloat32,
    MaxFloat64,
    MinInt32,
    MinInt64,
    MinFloat32,
    MinFloat64,
    SumInt32,
    SumInt64,
    SumFloat32,
    SumFloat64,
    Count,
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
            AggregateFunc::MaxInt32 => max_int32,
            AggregateFunc::MaxInt64 => max_int64,
            AggregateFunc::MaxFloat32 => max_float32,
            AggregateFunc::MaxFloat64 => max_float64,
            AggregateFunc::MinInt32 => min_int32,
            AggregateFunc::MinInt64 => min_int64,
            AggregateFunc::MinFloat32 => min_float32,
            AggregateFunc::MinFloat64 => min_float64,
            AggregateFunc::SumInt32 => sum_int32,
            AggregateFunc::SumInt64 => sum_int64,
            AggregateFunc::SumFloat32 => sum_float32,
            AggregateFunc::SumFloat64 => sum_float64,
            AggregateFunc::Count => count,
        }
    }
}
