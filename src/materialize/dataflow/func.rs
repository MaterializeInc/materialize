// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use failure::bail;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

use crate::repr::Datum;
use crate::repr::ScalarType;

pub fn and(a: Datum, b: Datum) -> Datum {
    match (&a, &b) {
        (Datum::False, _) => Datum::False,
        (_, Datum::False) => Datum::False,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::True, Datum::True) => Datum::True,
        _ => panic!("Cannot compute {:?} AND {:?}", a, b),
    }
}

pub fn or(a: Datum, b: Datum) -> Datum {
    match (&a, &b) {
        (Datum::True, _) => Datum::True,
        (_, Datum::True) => Datum::True,
        (Datum::Null, _) => Datum::Null,
        (_, Datum::Null) => Datum::Null,
        (Datum::False, Datum::False) => Datum::False,
        _ => panic!("Cannot compute {:?} OR {:?}", a, b),
    }
}

pub fn not(a: Datum) -> Datum {
    match &a {
        Datum::False => Datum::True,
        Datum::True => Datum::False,
        Datum::Null => Datum::Null,
        _ => panic!("Cannot compute NOT {:?}", a),
    }
}

pub fn abs_int32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32().abs())
}

pub fn abs_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64().abs())
}

pub fn abs_float32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32().abs())
}

pub fn abs_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64().abs())
}

pub fn cast_int32_to_float32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int32() as f32)
}

pub fn cast_int32_to_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_int32()))
}

pub fn cast_int64_to_int32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): we need to do something better than panicking if the
    // datum doesn't fit in an int32, but what? Poison the whole dataflow?
    // The SQL standard says this an error, but runtime errors are complicated
    // in a streaming setting.
    Datum::from(i32::try_from(a.unwrap_int64()).unwrap())
}

pub fn cast_int64_to_float32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f32)
}

pub fn cast_int64_to_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(a.unwrap_int64() as f64)
}

pub fn cast_float32_to_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float32() as i64)
}

pub fn cast_float32_to_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): is this cast valid?
    Datum::from(f64::from(a.unwrap_float32()))
}

pub fn cast_float64_to_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    // TODO(benesch): this is undefined behavior if the f32 doesn't fit in an
    // i64 (https://github.com/rust-lang/rust/issues/10184).
    Datum::from(a.unwrap_float64() as i64)
}

pub fn add_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() + b.unwrap_int32())
}

pub fn add_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() + b.unwrap_int64())
}

pub fn add_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() + b.unwrap_float32())
}

pub fn add_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() + b.unwrap_float64())
}

pub fn sub_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() - b.unwrap_int32())
}

pub fn sub_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() - b.unwrap_int64())
}

pub fn sub_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() - b.unwrap_float32())
}

pub fn sub_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() - b.unwrap_float64())
}

pub fn mul_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() * b.unwrap_int32())
}

pub fn mul_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() * b.unwrap_int64())
}

pub fn mul_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() * b.unwrap_float32())
}

pub fn mul_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() * b.unwrap_float64())
}

// TODO(jamii) we don't currently have any way of reporting errors from functions, so for now we just adopt sqlite's approach 1/0 = null

pub fn div_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_int32();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int32() / b)
    }
}

pub fn div_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    let b = b.unwrap_int64();
    if b == 0 {
        Datum::Null
    } else {
        Datum::from(a.unwrap_int64() / b)
    }
}

pub fn div_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() / b.unwrap_float32())
}

pub fn div_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() / b.unwrap_float64())
}

pub fn mod_int32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int32() % b.unwrap_int32())
}

pub fn mod_int64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_int64() % b.unwrap_int64())
}

pub fn mod_float32(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float32() % b.unwrap_float32())
}

pub fn mod_float64(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a.unwrap_float64() % b.unwrap_float64())
}

pub fn neg_int32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_int32())
}

pub fn neg_int64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_int64())
}

pub fn neg_float32(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_float32())
}

pub fn neg_float64(a: Datum) -> Datum {
    if a.is_null() {
        return Datum::Null;
    }
    Datum::from(-a.unwrap_float64())
}

pub fn eq(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a == b)
}

pub fn not_eq(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a != b)
}

pub fn lt(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a < b)
}

pub fn lte(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a <= b)
}

pub fn gt(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a > b)
}

pub fn gte(a: Datum, b: Datum) -> Datum {
    if a.is_null() || b.is_null() {
        return Datum::Null;
    }
    Datum::from(a >= b)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
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
    NotEq,
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
            BinaryFunc::NotEq => not_eq,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum UnaryFunc {
    Not,
    IsNull,
    NegInt32,
    NegInt64,
    NegFloat32,
    NegFloat64,
    AbsInt32,
    AbsInt64,
    AbsFloat32,
    AbsFloat64,
    CastInt32ToFloat32,
    CastInt32ToFloat64,
    CastInt64ToInt32,
    CastInt64ToFloat32,
    CastInt64ToFloat64,
    CastFloat32ToInt64,
    CastFloat32ToFloat64,
    CastFloat64ToInt64,
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
            UnaryFunc::AbsInt32 => abs_int32,
            UnaryFunc::AbsInt64 => abs_int64,
            UnaryFunc::AbsFloat32 => abs_float32,
            UnaryFunc::AbsFloat64 => abs_float64,
            UnaryFunc::CastInt32ToFloat32 => cast_int32_to_float32,
            UnaryFunc::CastInt32ToFloat64 => cast_int32_to_float64,
            UnaryFunc::CastInt64ToInt32 => cast_int64_to_int32,
            UnaryFunc::CastInt64ToFloat32 => cast_int64_to_float32,
            UnaryFunc::CastInt64ToFloat64 => cast_int64_to_float64,
            UnaryFunc::CastFloat32ToInt64 => cast_float32_to_int64,
            UnaryFunc::CastFloat32ToFloat64 => cast_float32_to_float64,
            UnaryFunc::CastFloat64ToInt64 => cast_float64_to_int64,
        }
    }
}

pub fn coalesce(datums: Vec<Datum>) -> Datum {
    datums
        .into_iter()
        .find(|d| !d.is_null())
        .unwrap_or(Datum::Null)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum VariadicFunc {
    Coalesce,
}

impl VariadicFunc {
    pub fn func(self) -> fn(Vec<Datum>) -> Datum {
        match self {
            VariadicFunc::Coalesce => coalesce,
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
    SumNull,
    Count,
    CountAll, // COUNT(*) counts nulls too
}

impl AggregateFunc {
    pub fn is_aggregate_func(name: &str) -> bool {
        match name {
            "avg" | "max" | "min" | "sum" | "count" => true,
            _ => false,
        }
    }

    pub fn from_name_and_scalar_type(
        name: &str,
        scalar_type: &ScalarType,
    ) -> Result<(Self, ScalarType), failure::Error> {
        let func = match (name, scalar_type) {
            ("avg", ScalarType::Int32) => AggregateFunc::AvgInt32,
            ("avg", ScalarType::Int64) => AggregateFunc::AvgInt64,
            ("avg", ScalarType::Float32) => AggregateFunc::AvgFloat32,
            ("avg", ScalarType::Float64) => AggregateFunc::AvgFloat64,
            ("avg", ScalarType::Null) => AggregateFunc::AvgNull,
            ("max", ScalarType::Int32) => AggregateFunc::MaxInt32,
            ("max", ScalarType::Int64) => AggregateFunc::MaxInt64,
            ("max", ScalarType::Float32) => AggregateFunc::MaxFloat32,
            ("max", ScalarType::Float64) => AggregateFunc::MaxFloat64,
            ("max", ScalarType::Bool) => AggregateFunc::MaxBool,
            ("max", ScalarType::String) => AggregateFunc::MaxString,
            ("max", ScalarType::Null) => AggregateFunc::MaxNull,
            ("min", ScalarType::Int32) => AggregateFunc::MinInt32,
            ("min", ScalarType::Int64) => AggregateFunc::MinInt64,
            ("min", ScalarType::Float32) => AggregateFunc::MinFloat32,
            ("min", ScalarType::Float64) => AggregateFunc::MinFloat64,
            ("min", ScalarType::Bool) => AggregateFunc::MinBool,
            ("min", ScalarType::String) => AggregateFunc::MinString,
            ("min", ScalarType::Null) => AggregateFunc::MinNull,
            ("sum", ScalarType::Int32) => AggregateFunc::SumInt32,
            ("sum", ScalarType::Int64) => AggregateFunc::SumInt64,
            ("sum", ScalarType::Float32) => AggregateFunc::SumFloat32,
            ("sum", ScalarType::Float64) => AggregateFunc::SumFloat64,
            ("sum", ScalarType::Null) => AggregateFunc::SumNull,
            ("count", _) => AggregateFunc::Count,
            other => bail!("Unimplemented function/type combo: {:?}", other),
        };
        let scalar_type = match name {
            "count" => ScalarType::Int64,
            "avg" | "max" | "min" | "sum" => scalar_type.clone(),
            other => bail!("Unknown aggregate function: {:?}", other),
        };
        Ok((func, scalar_type))
    }

    pub fn func<I>(self) -> fn(I) -> Datum
    where
        I: IntoIterator<Item = Datum>,
    {
        match self {
            AggregateFunc::AvgInt32 => avg_int32,
            AggregateFunc::AvgInt64 => avg_int64,
            AggregateFunc::AvgFloat32 => avg_float32,
            AggregateFunc::AvgFloat64 => avg_float64,
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
