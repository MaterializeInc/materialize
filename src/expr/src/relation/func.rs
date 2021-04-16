// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

use std::fmt;
use std::fs;
use std::iter;
use std::path::PathBuf;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use regex::Regex;
use serde::{Deserialize, Serialize};

use ore::cast::CastFrom;
use repr::adt::decimal::{Significand, MAX_DECIMAL_PRECISION};
use repr::adt::regex::Regex as ReprRegex;
use repr::{CachedRecordIter, ColumnType, Datum, Diff, RelationType, Row, RowArena, ScalarType};

use crate::id::GlobalId;
use crate::scalar::func::jsonb_stringify;

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

fn sum_int32<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x: i64 = datums.map(|d| i64::from(d.unwrap_int32())).sum();
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
        let x: i128 = datums.map(|d| i128::from(d.unwrap_int64())).sum();
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

fn count<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let x: i64 = datums.into_iter().filter(|d| !d.is_null()).count() as i64;
    Datum::from(x)
}

fn any<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    datums
        .into_iter()
        .fold(Datum::False, |state, next| match (state, next) {
            (Datum::True, _) | (_, Datum::True) => Datum::True,
            (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
            _ => Datum::False,
        })
}

fn all<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    datums
        .into_iter()
        .fold(Datum::True, |state, next| match (state, next) {
            (Datum::False, _) | (_, Datum::False) => Datum::False,
            (Datum::Null, _) | (_, Datum::Null) => Datum::Null,
            _ => Datum::True,
        })
}

fn jsonb_agg<'a, I>(datums: I, temp_storage: &'a RowArena) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    temp_storage.make_datum(|packer| {
        packer.push_list(datums.into_iter().filter(|d| !d.is_null()));
    })
}

fn jsonb_object_agg<'a, I>(datums: I, temp_storage: &'a RowArena) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    temp_storage.make_datum(|packer| {
        packer.push_dict(
            datums
                .into_iter()
                .filter_map(|d| {
                    if d.is_null() {
                        return None;
                    }
                    let mut list = d.unwrap_list().iter();
                    let key = list.next().unwrap();
                    let val = list.next().unwrap();
                    if key.is_null() {
                        // TODO(benesch): this should produce an error, but
                        // aggregate functions cannot presently produce errors.
                        None
                    } else {
                        Some((key.unwrap_str(), val))
                    }
                })
                .sorted_by_key(|(k, _v)| *k)
                .dedup_by(|(k1, _v1), (k2, _v2)| k1 == k2),
        );
    })
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
    SumInt32,
    SumInt64,
    SumFloat32,
    SumFloat64,
    SumDecimal,
    Count,
    Any,
    All,
    /// Accumulates JSON-typed `Datum`s into a JSON list.
    ///
    /// WARNING: Unlike the `jsonb_agg` function that is exposed by the SQL
    /// layer, this function filters out `Datum::Null`, for consistency with
    /// the other aggregate functions.
    JsonbAgg,
    /// Zips JSON-typed `Datum`s into a JSON map.
    ///
    /// WARNING: Unlike the `jsonb_object_agg` function that is exposed by the SQL
    /// layer, this function filters out `Datum::Null`, for consistency with
    /// the other aggregate functions.
    JsonbObjectAgg,
    /// Accumulates any number of `Datum::Dummy`s into `Datum::Dummy`.
    ///
    /// Useful for removing an expensive aggregation while maintaining the shape
    /// of a reduce operator.
    Dummy,
}

impl AggregateFunc {
    pub fn eval<'a, I>(&self, datums: I, temp_storage: &'a RowArena) -> Datum<'a>
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
            AggregateFunc::SumInt32 => sum_int32(datums),
            AggregateFunc::SumInt64 => sum_int64(datums),
            AggregateFunc::SumFloat32 => sum_float32(datums),
            AggregateFunc::SumFloat64 => sum_float64(datums),
            AggregateFunc::SumDecimal => sum_decimal(datums),
            AggregateFunc::Count => count(datums),
            AggregateFunc::Any => any(datums),
            AggregateFunc::All => all(datums),
            AggregateFunc::JsonbAgg => jsonb_agg(datums, temp_storage),
            AggregateFunc::JsonbObjectAgg => jsonb_object_agg(datums, temp_storage),
            AggregateFunc::Dummy => Datum::Dummy,
        }
    }

    /// Returns the output of the aggregation function when applied on an empty
    /// input relation.
    pub fn default(&self) -> Datum<'static> {
        match self {
            AggregateFunc::Count => Datum::Int64(0),
            AggregateFunc::Any => Datum::False,
            AggregateFunc::All => Datum::True,
            AggregateFunc::Dummy => Datum::Dummy,
            _ => Datum::Null,
        }
    }

    /// Returns a datum whose inclusion in the aggregation will not change its
    /// result.
    pub fn identity_datum(&self) -> Datum<'static> {
        match self {
            AggregateFunc::Any => Datum::False,
            AggregateFunc::All => Datum::True,
            AggregateFunc::Dummy => Datum::Dummy,
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
            AggregateFunc::Any => ScalarType::Bool,
            AggregateFunc::All => ScalarType::Bool,
            AggregateFunc::JsonbAgg => ScalarType::Jsonb,
            AggregateFunc::JsonbObjectAgg => ScalarType::Jsonb,
            AggregateFunc::SumInt32 => ScalarType::Int64,
            AggregateFunc::SumInt64 => ScalarType::Decimal(MAX_DECIMAL_PRECISION, 0),
            _ => input_type.scalar_type,
        };
        // Count never produces null, and other aggregations only produce
        // null in the presence of null inputs.
        let nullable = match self {
            AggregateFunc::Count => false,
            _ => input_type.nullable,
        };
        scalar_type.nullable(nullable)
    }
}

fn jsonb_each<'a>(a: Datum<'a>, temp_storage: &'a RowArena, stringify: bool) -> Vec<(Row, Diff)> {
    match a {
        Datum::Map(dict) => dict
            .iter()
            .map(move |(k, mut v)| {
                if stringify {
                    v = jsonb_stringify(v, temp_storage);
                }
                (Row::pack_slice(&[Datum::String(k), v]), 1)
            })
            .collect(),
        _ => vec![],
    }
}

fn jsonb_object_keys<'a>(a: Datum<'a>) -> Vec<(Row, Diff)> {
    match a {
        Datum::Map(dict) => dict
            .iter()
            .map(move |(k, _)| (Row::pack_slice(&[Datum::String(k)]), 1))
            .collect(),
        _ => vec![],
    }
}

fn jsonb_array_elements<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
    stringify: bool,
) -> Vec<(Row, Diff)> {
    match a {
        Datum::List(list) => list
            .iter()
            .map(move |mut e| {
                if stringify {
                    e = jsonb_stringify(e, temp_storage);
                }
                (Row::pack_slice(&[e]), 1)
            })
            .collect(),
        _ => vec![],
    }
}

fn regexp_extract(a: Datum, r: &AnalyzedRegex) -> Option<(Row, Diff)> {
    let r = r.inner();
    let a = a.unwrap_str();
    let captures = r.captures(a)?;
    let datums = captures
        .iter()
        .skip(1)
        .map(|m| Datum::from(m.map(|m| m.as_str())));
    Some((Row::pack(datums), 1))
}

fn generate_series_int32(start: Datum, stop: Datum) -> Vec<(Row, Diff)> {
    let start = start.unwrap_int32();
    let stop = stop.unwrap_int32();
    (start..=stop)
        .map(move |i| (Row::pack_slice(&[Datum::Int32(i)]), 1))
        .collect()
}

fn generate_series_int64(start: Datum, stop: Datum) -> Vec<(Row, Diff)> {
    let start = start.unwrap_int64();
    let stop = stop.unwrap_int64();
    (start..=stop)
        .map(move |i| (Row::pack_slice(&[Datum::Int64(i)]), 1))
        .collect()
}

fn unnest_array(a: Datum) -> Vec<(Row, Diff)> {
    a.unwrap_array()
        .elements()
        .iter()
        .map(move |e| (Row::pack_slice(&[e]), 1))
        .collect()
}

fn unnest_list(a: Datum) -> Vec<(Row, Diff)> {
    a.unwrap_list()
        .iter()
        .map(move |e| (Row::pack_slice(&[e]), 1))
        .collect()
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
            AggregateFunc::SumInt32 => f.write_str("sum"),
            AggregateFunc::SumInt64 => f.write_str("sum"),
            AggregateFunc::SumFloat32 => f.write_str("sum"),
            AggregateFunc::SumFloat64 => f.write_str("sum"),
            AggregateFunc::SumDecimal => f.write_str("sum"),
            AggregateFunc::Count => f.write_str("count"),
            AggregateFunc::Any => f.write_str("any"),
            AggregateFunc::All => f.write_str("all"),
            AggregateFunc::JsonbAgg => f.write_str("jsonb_agg"),
            AggregateFunc::JsonbObjectAgg => f.write_str("jsonb_object_agg"),
            AggregateFunc::Dummy => f.write_str("dummy"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct CaptureGroupDesc {
    pub index: u32,
    pub name: Option<String>,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct AnalyzedRegex(ReprRegex, Vec<CaptureGroupDesc>);

impl AnalyzedRegex {
    pub fn new(s: &str) -> Result<Self, regex::Error> {
        let r = regex::Regex::new(s)?;
        let descs: Vec<_> = r
            .capture_names()
            .enumerate()
            // The first capture is the entire matched string.
            // This will often not be useful, so skip it.
            // If people want it they can just surround their
            // entire regex in an explicit capture group.
            .skip(1)
            .map(|(i, name)| CaptureGroupDesc {
                index: i as u32,
                name: name.map(String::from),
                // TODO -- we can do better.
                // https://github.com/MaterializeInc/materialize/issues/1685
                nullable: true,
            })
            .collect();
        Ok(Self(ReprRegex(r), descs))
    }
    pub fn capture_groups_len(&self) -> usize {
        self.1.len()
    }
    pub fn capture_groups_iter(&self) -> impl Iterator<Item = &CaptureGroupDesc> {
        self.1.iter()
    }
    pub fn inner(&self) -> &Regex {
        &(self.0).0
    }
}

pub fn csv_extract(a: Datum, n_cols: usize) -> Vec<(Row, Diff)> {
    let bytes = a.unwrap_str().as_bytes();
    let mut row = Row::default();
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes);
    csv_reader
        .records()
        .filter_map(|res| match res {
            Ok(sr) if sr.len() == n_cols => {
                row.extend(sr.iter().map(|s| Datum::String(s)));
                Some((row.finish_and_reuse(), 1))
            }
            _ => None,
        })
        .collect()
}

pub fn repeat(a: Datum) -> Vec<(Row, Diff)> {
    let n = Diff::cast_from(a.unwrap_int64());
    vec![(Row::default(), n)]
}

// TODO(justin): this should return an error.
pub fn files_for_source(id: GlobalId, persistence_directory: &PathBuf) -> Vec<PathBuf> {
    let source_path = persistence_directory.join(id.to_string());
    match std::fs::read_dir(&source_path) {
        Ok(entries) => entries.map(|e| e.unwrap().path()).collect(),
        Err(_) => {
            // TODO(justin): it would be better to have a real error here, but saying
            // "there are no records" is also acceptable for now.
            return vec![];
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub enum TableFunc {
    JsonbEach {
        stringify: bool,
    },
    JsonbObjectKeys,
    JsonbArrayElements {
        stringify: bool,
    },
    RegexpExtract(AnalyzedRegex),
    CsvExtract(usize),
    GenerateSeriesInt32,
    GenerateSeriesInt64,
    // TODO(justin): should also possibly have GenerateSeriesTimestamp{,Tz}.
    Repeat,
    ReadCachedData {
        source: GlobalId,
        cache_directory: PathBuf,
    },
    UnnestArray {
        el_typ: ScalarType,
    },
    UnnestList {
        el_typ: ScalarType,
    },
}

impl TableFunc {
    pub fn eval<'a>(
        &'a self,
        datums: Vec<Datum<'a>>,
        temp_storage: &'a RowArena,
    ) -> Vec<(Row, Diff)> {
        if self.empty_on_null_input() {
            if datums.iter().any(|d| d.is_null()) {
                return vec![];
            }
        }
        match self {
            TableFunc::JsonbEach { stringify } => jsonb_each(datums[0], temp_storage, *stringify),
            TableFunc::JsonbObjectKeys => jsonb_object_keys(datums[0]),
            TableFunc::JsonbArrayElements { stringify } => {
                jsonb_array_elements(datums[0], temp_storage, *stringify)
            }
            TableFunc::RegexpExtract(a) => regexp_extract(datums[0], a).into_iter().collect(),
            TableFunc::CsvExtract(n_cols) => csv_extract(datums[0], *n_cols).into_iter().collect(),
            TableFunc::GenerateSeriesInt32 => generate_series_int32(datums[0], datums[1]),
            TableFunc::GenerateSeriesInt64 => generate_series_int64(datums[0], datums[1]),
            TableFunc::Repeat => repeat(datums[0]),
            TableFunc::ReadCachedData {
                source,
                cache_directory,
            } => files_for_source(*source, cache_directory)
                .iter()
                .flat_map(|e| {
                    CachedRecordIter::new(fs::read(e).unwrap()).map(move |r| (e.clone(), r))
                })
                .map(|(e, r)| {
                    (
                        Row::pack_slice(&[
                            Datum::String(e.to_str().unwrap()),
                            Datum::Int64(r.offset),
                            Datum::Bytes(&r.key),
                            Datum::Bytes(&r.value),
                        ]),
                        1,
                    )
                })
                .collect::<Vec<(Row, Diff)>>(),
            TableFunc::UnnestArray { .. } => unnest_array(datums[0]),
            TableFunc::UnnestList { .. } => unnest_list(datums[0]),
        }
    }

    pub fn output_type(&self) -> RelationType {
        RelationType::new(match self {
            TableFunc::JsonbEach { stringify: true } => vec![
                ScalarType::String.nullable(false),
                ScalarType::String.nullable(true),
            ],
            TableFunc::JsonbEach { stringify: false } => vec![
                ScalarType::String.nullable(false),
                ScalarType::Jsonb.nullable(false),
            ],
            TableFunc::JsonbObjectKeys => vec![ScalarType::String.nullable(false)],
            TableFunc::JsonbArrayElements { stringify: true } => {
                vec![ScalarType::String.nullable(true)]
            }
            TableFunc::JsonbArrayElements { stringify: false } => {
                vec![ScalarType::Jsonb.nullable(false)]
            }
            TableFunc::RegexpExtract(a) => a
                .capture_groups_iter()
                .map(|cg| ScalarType::String.nullable(cg.nullable))
                .collect(),
            TableFunc::CsvExtract(n_cols) => iter::repeat(ScalarType::String.nullable(false))
                .take(*n_cols)
                .collect(),
            TableFunc::GenerateSeriesInt32 => vec![ScalarType::Int32.nullable(false)],
            TableFunc::GenerateSeriesInt64 => vec![ScalarType::Int64.nullable(false)],
            TableFunc::Repeat => vec![],
            TableFunc::ReadCachedData { .. } => vec![
                ScalarType::String.nullable(true),
                ScalarType::Int64.nullable(false),
                ScalarType::Bytes.nullable(false),
                ScalarType::Bytes.nullable(false),
            ],
            TableFunc::UnnestArray { el_typ } => vec![el_typ.clone().nullable(true)],
            TableFunc::UnnestList { el_typ } => vec![el_typ.clone().nullable(true)],
        })
    }

    pub fn output_arity(&self) -> usize {
        match self {
            TableFunc::JsonbEach { .. } => 2,
            TableFunc::JsonbObjectKeys => 1,
            TableFunc::JsonbArrayElements { .. } => 1,
            TableFunc::RegexpExtract(a) => a.capture_groups_len(),
            TableFunc::CsvExtract(n_cols) => *n_cols,
            TableFunc::GenerateSeriesInt32 => 1,
            TableFunc::GenerateSeriesInt64 => 1,
            TableFunc::Repeat => 0,
            TableFunc::ReadCachedData { .. } => 4,
            TableFunc::UnnestArray { .. } => 1,
            TableFunc::UnnestList { .. } => 1,
        }
    }

    pub fn empty_on_null_input(&self) -> bool {
        // Warning: this returns currently "true" for all TableFuncs.
        // If adding a TableFunc for which this function will return "false",
        // check the places where `empty_on_null_input` is called to ensure,
        // such as NonNullRequirements that the case this function returns
        // false is properly handled.
        match self {
            TableFunc::JsonbEach { .. }
            | TableFunc::JsonbObjectKeys
            | TableFunc::JsonbArrayElements { .. }
            | TableFunc::GenerateSeriesInt32
            | TableFunc::GenerateSeriesInt64
            | TableFunc::RegexpExtract(_)
            | TableFunc::CsvExtract(_)
            | TableFunc::Repeat
            | TableFunc::ReadCachedData { .. }
            | TableFunc::UnnestArray { .. }
            | TableFunc::UnnestList { .. } => true,
        }
    }

    /// True iff the table function preserves the append-only property of its input.
    pub fn preserves_monotonicity(&self) -> bool {
        // Most variants preserve monotonicity, but all variants are enumerated to
        // ensure that added variants at least check that this is the case.
        match self {
            TableFunc::JsonbEach { .. } => true,
            TableFunc::JsonbObjectKeys => true,
            TableFunc::JsonbArrayElements { .. } => true,
            TableFunc::RegexpExtract(_) => true,
            TableFunc::CsvExtract(_) => true,
            TableFunc::GenerateSeriesInt32 => true,
            TableFunc::GenerateSeriesInt64 => true,
            TableFunc::Repeat => false,
            TableFunc::ReadCachedData { .. } => true,
            TableFunc::UnnestArray { .. } => true,
            TableFunc::UnnestList { .. } => true,
        }
    }
}

impl fmt::Display for TableFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableFunc::JsonbEach { .. } => f.write_str("jsonb_each"),
            TableFunc::JsonbObjectKeys => f.write_str("jsonb_object_keys"),
            TableFunc::JsonbArrayElements { .. } => f.write_str("jsonb_array_elements"),
            TableFunc::RegexpExtract(a) => write!(f, "regexp_extract({:?}, _)", a.0),
            TableFunc::CsvExtract(n_cols) => write!(f, "csv_extract({}, _)", n_cols),
            TableFunc::GenerateSeriesInt32 => f.write_str("generate_series"),
            TableFunc::GenerateSeriesInt64 => f.write_str("generate_series"),
            TableFunc::Repeat => f.write_str("repeat_row"),
            TableFunc::ReadCachedData { source, .. } => {
                write!(f, "internal_read_cached_data({})", source)
            }
            TableFunc::UnnestArray { .. } => f.write_str("unnest_array"),
            TableFunc::UnnestList { .. } => f.write_str("unnest_list"),
        }
    }
}
