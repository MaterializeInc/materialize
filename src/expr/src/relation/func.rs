// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

use std::cmp::{max, min};
use std::iter::Sum;
use std::ops::Deref;
use std::str::FromStr;
use std::{fmt, iter};

use chrono::{DateTime, NaiveDateTime, NaiveTime, Utc};
use dec::OrderedDecimal;
use itertools::{Either, Itertools};
use mz_lowertest::MzReflect;
use mz_ore::cast::CastFrom;

use mz_ore::str::separated;
use mz_ore::{soft_assert_eq_no_log, soft_assert_or_log};
use mz_proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::{self, Numeric, NumericMaxScale};
use mz_repr::adt::regex::Regex as ReprRegex;
use mz_repr::adt::timestamp::{CheckedTimestamp, TimestampLike};
use mz_repr::{
    ColumnName, ColumnType, Datum, Diff, RelationType, Row, RowArena, RowPacker, ScalarType,
    SharedRow, datum_size,
};
use num::{CheckedAdd, Integer, Signed, ToPrimitive};
use ordered_float::OrderedFloat;
use proptest::prelude::{Arbitrary, Just};
use proptest::strategy::{BoxedStrategy, Strategy, Union};
use proptest_derive::Arbitrary;
use regex::Regex;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

use crate::EvalError;
use crate::WindowFrameBound::{
    CurrentRow, OffsetFollowing, OffsetPreceding, UnboundedFollowing, UnboundedPreceding,
};
use crate::WindowFrameUnits::{Groups, Range, Rows};
use crate::explain::{HumanizedExpr, HumanizerMode};
use crate::relation::proto_aggregate_func::{
    self, ProtoColumnOrders, ProtoFusedValueWindowFunc, ProtoFusedWindowAggregate,
};
use crate::relation::proto_table_func::{ProtoTabletizedScalar, ProtoWithOrdinality};
use crate::relation::{
    ColumnOrder, ProtoAggregateFunc, ProtoTableFunc, WindowFrame, WindowFrameBound,
    WindowFrameUnits, compare_columns, proto_table_func,
};
use crate::scalar::func::{add_timestamp_months, jsonb_stringify};

include!(concat!(env!("OUT_DIR"), "/mz_expr.relation.func.rs"));

// TODO(jamii) be careful about overflow in sum/avg
// see https://timely.zulipchat.com/#narrow/stream/186635-engineering/topic/additional.20work/near/163507435

fn max_string<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    match datums
        .into_iter()
        .filter(|d| !d.is_null())
        .max_by(|a, b| a.unwrap_str().cmp(b.unwrap_str()))
    {
        Some(datum) => datum,
        None => Datum::Null,
    }
}

fn max_datum<'a, I, DatumType>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
    DatumType: TryFrom<Datum<'a>> + Ord,
    <DatumType as TryFrom<Datum<'a>>>::Error: std::fmt::Debug,
    Datum<'a>: From<Option<DatumType>>,
{
    let x: Option<DatumType> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| DatumType::try_from(d).expect("unexpected type"))
        .max();

    x.into()
}

fn min_datum<'a, I, DatumType>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
    DatumType: TryFrom<Datum<'a>> + Ord,
    <DatumType as TryFrom<Datum<'a>>>::Error: std::fmt::Debug,
    Datum<'a>: From<Option<DatumType>>,
{
    let x: Option<DatumType> = datums
        .into_iter()
        .filter(|d| !d.is_null())
        .map(|d| DatumType::try_from(d).expect("unexpected type"))
        .min();

    x.into()
}

fn min_string<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    match datums
        .into_iter()
        .filter(|d| !d.is_null())
        .min_by(|a, b| a.unwrap_str().cmp(b.unwrap_str()))
    {
        Some(datum) => datum,
        None => Datum::Null,
    }
}

fn sum_datum<'a, I, DatumType, ResultType>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
    DatumType: TryFrom<Datum<'a>>,
    <DatumType as TryFrom<Datum<'a>>>::Error: std::fmt::Debug,
    ResultType: From<DatumType> + Sum + Into<Datum<'a>>,
{
    let mut datums = datums.into_iter().filter(|d| !d.is_null()).peekable();
    if datums.peek().is_none() {
        Datum::Null
    } else {
        let x = datums
            .map(|d| ResultType::from(DatumType::try_from(d).expect("unexpected type")))
            .sum::<ResultType>();
        x.into()
    }
}

fn sum_numeric<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let mut cx = numeric::cx_datum();
    let mut sum = Numeric::zero();
    let mut empty = true;
    for d in datums {
        if !d.is_null() {
            empty = false;
            cx.add(&mut sum, &d.unwrap_numeric().0);
        }
    }
    match empty {
        true => Datum::Null,
        false => Datum::from(sum),
    }
}

// TODO(benesch): remove potentially dangerous usage of `as`.
#[allow(clippy::as_conversions)]
fn count<'a, I>(datums: I) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    // TODO(jkosh44) This should error when the count can't fit inside of an `i64` instead of returning a negative result.
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

fn string_agg<'a, I>(datums: I, temp_storage: &'a RowArena, order_by: &[ColumnOrder]) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    const EMPTY_SEP: &str = "";

    let datums = order_aggregate_datums(datums, order_by);
    let mut sep_value_pairs = datums.into_iter().filter_map(|d| {
        if d.is_null() {
            return None;
        }
        let mut value_sep = d.unwrap_list().iter();
        match (value_sep.next().unwrap(), value_sep.next().unwrap()) {
            (Datum::Null, _) => None,
            (Datum::String(val), Datum::Null) => Some((EMPTY_SEP, val)),
            (Datum::String(val), Datum::String(sep)) => Some((sep, val)),
            _ => unreachable!(),
        }
    });

    let mut s = String::default();
    match sep_value_pairs.next() {
        // First value not prefixed by its separator
        Some((_, value)) => s.push_str(value),
        // If no non-null values sent, return NULL.
        None => return Datum::Null,
    }

    for (sep, value) in sep_value_pairs {
        s.push_str(sep);
        s.push_str(value);
    }

    Datum::String(temp_storage.push_string(s))
}

fn jsonb_agg<'a, I>(datums: I, temp_storage: &'a RowArena, order_by: &[ColumnOrder]) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let datums = order_aggregate_datums(datums, order_by);
    temp_storage.make_datum(|packer| {
        packer.push_list(datums.into_iter().filter(|d| !d.is_null()));
    })
}

fn dict_agg<'a, I>(datums: I, temp_storage: &'a RowArena, order_by: &[ColumnOrder]) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let datums = order_aggregate_datums(datums, order_by);
    temp_storage.make_datum(|packer| {
        let mut datums: Vec<_> = datums
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
            .collect();
        // datums are ordered by any ORDER BY clause now, and we want to preserve
        // the last entry for each key, but we also need to present unique and sorted
        // keys to push_dict. Use sort_by here, which is stable, and so will preserve
        // the ORDER BY order. Then reverse and dedup to retain the last of each
        // key. Reverse again so we're back in push_dict order.
        datums.sort_by_key(|(k, _v)| *k);
        datums.reverse();
        datums.dedup_by_key(|(k, _v)| *k);
        datums.reverse();
        packer.push_dict(datums);
    })
}

/// Assuming datums is a List, sort them by the 2nd through Nth elements
/// corresponding to order_by, then return the 1st element.
///
/// Near the usages of this function, we sometimes want to produce Datums with a shorter lifetime
/// than 'a. We have to actually perform the shortening of the lifetime here, inside this function,
/// because if we were to simply return `impl Iterator<Item = Datum<'a>>`, that wouldn't be
/// covariant in the item type, because opaque types are always invariant. (Contrast this with how
/// we perform the shortening _inside_ this function: the input of the `map` is known to
/// specifically be `std::vec::IntoIter`, which is known to be covariant.)
pub fn order_aggregate_datums<'a: 'b, 'b, I>(
    datums: I,
    order_by: &[ColumnOrder],
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    order_aggregate_datums_with_rank_inner(datums, order_by)
        .into_iter()
        // (`payload` is coerced here to `Datum<'b>` in the argument of the closure)
        .map(|(payload, _order_datums)| payload)
}

/// Assuming datums is a List, sort them by the 2nd through Nth elements
/// corresponding to order_by, then return the 1st element and computed order by expression.
fn order_aggregate_datums_with_rank<'a, I>(
    datums: I,
    order_by: &[ColumnOrder],
) -> impl Iterator<Item = (Datum<'a>, Row)>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    order_aggregate_datums_with_rank_inner(datums, order_by)
        .into_iter()
        .map(|(payload, order_by_datums)| (payload, Row::pack(order_by_datums)))
}

fn order_aggregate_datums_with_rank_inner<'a, I>(
    datums: I,
    order_by: &[ColumnOrder],
) -> Vec<(Datum<'a>, Vec<Datum<'a>>)>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let mut decoded: Vec<(Datum, Vec<Datum>)> = datums
        .into_iter()
        .map(|d| {
            let list = d.unwrap_list();
            let mut list_it = list.iter();
            let payload = list_it.next().unwrap();

            // We decode the order_by Datums here instead of the comparison function, because the
            // comparison function is expected to be called `O(log n)` times on each input row.
            // The only downside is that the decoded data might be bigger, but I think that's fine,
            // because:
            // - if we have a window partition so big that this would create a memory problem, then
            //   the non-incrementalness of window functions will create a serious CPU problem
            //   anyway,
            // - and anyhow various other parts of the window function code already do decoding
            //   upfront.
            let mut order_by_datums = Vec::with_capacity(order_by.len());
            for _ in 0..order_by.len() {
                order_by_datums.push(
                    list_it
                        .next()
                        .expect("must have exactly the same number of Datums as `order_by`"),
                );
            }

            (payload, order_by_datums)
        })
        .collect();

    let mut sort_by =
        |(payload_left, left_order_by_datums): &(Datum, Vec<Datum>),
         (payload_right, right_order_by_datums): &(Datum, Vec<Datum>)| {
            compare_columns(
                order_by,
                left_order_by_datums,
                right_order_by_datums,
                || payload_left.cmp(payload_right),
            )
        };
    // `sort_unstable_by` can be faster and uses less memory than `sort_by`. An unstable sort is
    // enough here, because if two elements are equal in our `compare` function, then the elements
    // are actually binary-equal (because of the `tiebreaker` given to `compare_columns`), so it
    // doesn't matter what order they end up in.
    decoded.sort_unstable_by(&mut sort_by);
    decoded
}

fn array_concat<'a, I>(datums: I, temp_storage: &'a RowArena, order_by: &[ColumnOrder]) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let datums = order_aggregate_datums(datums, order_by);
    let datums: Vec<_> = datums
        .into_iter()
        .map(|d| d.unwrap_array().elements().iter())
        .flatten()
        .collect();
    let dims = ArrayDimension {
        lower_bound: 1,
        length: datums.len(),
    };
    temp_storage.make_datum(|packer| {
        packer.try_push_array(&[dims], datums).unwrap();
    })
}

fn list_concat<'a, I>(datums: I, temp_storage: &'a RowArena, order_by: &[ColumnOrder]) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let datums = order_aggregate_datums(datums, order_by);
    temp_storage.make_datum(|packer| {
        packer.push_list(datums.into_iter().map(|d| d.unwrap_list().iter()).flatten());
    })
}

/// The expected input is in the format of `[((OriginalRow, [EncodedArgs]), OrderByExprs...)]`
/// The output is in the format of `[result_value, original_row]`.
/// See an example at `lag_lead`, where the input-output formats are similar.
fn row_number<'a, I>(
    datums: I,
    callers_temp_storage: &'a RowArena,
    order_by: &[ColumnOrder],
) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    // We want to use our own temp_storage here, to avoid flooding `callers_temp_storage` with a
    // large number of new datums. This is because we don't want to make an assumption about
    // whether the caller creates a new temp_storage between window partitions.
    let temp_storage = RowArena::new();
    let datums = row_number_no_list(datums, &temp_storage, order_by);

    callers_temp_storage.make_datum(|packer| {
        packer.push_list(datums);
    })
}

/// Like `row_number`, but doesn't perform the final wrapping in a list, returning an Iterator
/// instead.
fn row_number_no_list<'a: 'b, 'b, I>(
    datums: I,
    callers_temp_storage: &'b RowArena,
    order_by: &[ColumnOrder],
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let datums = order_aggregate_datums(datums, order_by);

    callers_temp_storage.reserve(datums.size_hint().0);
    datums
        .into_iter()
        .map(|d| d.unwrap_list().iter())
        .flatten()
        .zip(1i64..)
        .map(|(d, i)| {
            callers_temp_storage.make_datum(|packer| {
                packer.push_list_with(|packer| {
                    packer.push(Datum::Int64(i));
                    packer.push(d);
                });
            })
        })
}

/// The expected input is in the format of `[((OriginalRow, [EncodedArgs]), OrderByExprs...)]`
/// The output is in the format of `[result_value, original_row]`.
/// See an example at `lag_lead`, where the input-output formats are similar.
fn rank<'a, I>(datums: I, callers_temp_storage: &'a RowArena, order_by: &[ColumnOrder]) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let temp_storage = RowArena::new();
    let datums = rank_no_list(datums, &temp_storage, order_by);

    callers_temp_storage.make_datum(|packer| {
        packer.push_list(datums);
    })
}

/// Like `rank`, but doesn't perform the final wrapping in a list, returning an Iterator
/// instead.
fn rank_no_list<'a: 'b, 'b, I>(
    datums: I,
    callers_temp_storage: &'b RowArena,
    order_by: &[ColumnOrder],
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    // Keep the row used for ordering around, as it is used to determine the rank
    let datums = order_aggregate_datums_with_rank(datums, order_by);

    let mut datums = datums
        .into_iter()
        .map(|(d0, order_row)| {
            d0.unwrap_list()
                .iter()
                .map(move |d1| (d1, order_row.clone()))
        })
        .flatten();

    callers_temp_storage.reserve(datums.size_hint().0);
    datums
        .next()
        .map_or(vec![], |(first_datum, first_order_row)| {
            // Folding with (last order_by row, last assigned rank, row number, output vec)
            datums.fold((first_order_row, 1, 1, vec![(first_datum, 1)]), |mut acc, (next_datum, next_order_row)| {
                let (ref mut acc_row, ref mut acc_rank, ref mut acc_row_num, ref mut output) = acc;
                *acc_row_num += 1;
                // Identity is based on the order_by expression
                if *acc_row != next_order_row {
                    *acc_rank = *acc_row_num;
                    *acc_row = next_order_row;
                }

                (*output).push((next_datum, *acc_rank));
                acc
            })
        }.3).into_iter().map(|(d, i)| {
        callers_temp_storage.make_datum(|packer| {
            packer.push_list_with(|packer| {
                packer.push(Datum::Int64(i));
                packer.push(d);
            });
        })
    })
}

/// The expected input is in the format of `[((OriginalRow, [EncodedArgs]), OrderByExprs...)]`
/// The output is in the format of `[result_value, original_row]`.
/// See an example at `lag_lead`, where the input-output formats are similar.
fn dense_rank<'a, I>(
    datums: I,
    callers_temp_storage: &'a RowArena,
    order_by: &[ColumnOrder],
) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let temp_storage = RowArena::new();
    let datums = dense_rank_no_list(datums, &temp_storage, order_by);

    callers_temp_storage.make_datum(|packer| {
        packer.push_list(datums);
    })
}

/// Like `dense_rank`, but doesn't perform the final wrapping in a list, returning an Iterator
/// instead.
fn dense_rank_no_list<'a: 'b, 'b, I>(
    datums: I,
    callers_temp_storage: &'b RowArena,
    order_by: &[ColumnOrder],
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    // Keep the row used for ordering around, as it is used to determine the rank
    let datums = order_aggregate_datums_with_rank(datums, order_by);

    let mut datums = datums
        .into_iter()
        .map(|(d0, order_row)| {
            d0.unwrap_list()
                .iter()
                .map(move |d1| (d1, order_row.clone()))
        })
        .flatten();

    callers_temp_storage.reserve(datums.size_hint().0);
    datums
        .next()
        .map_or(vec![], |(first_datum, first_order_row)| {
            // Folding with (last order_by row, last assigned rank, output vec)
            datums.fold((first_order_row, 1, vec![(first_datum, 1)]), |mut acc, (next_datum, next_order_row)| {
                let (ref mut acc_row, ref mut acc_rank, ref mut output) = acc;
                // Identity is based on the order_by expression
                if *acc_row != next_order_row {
                    *acc_rank += 1;
                    *acc_row = next_order_row;
                }

                (*output).push((next_datum, *acc_rank));
                acc
            })
        }.2).into_iter().map(|(d, i)| {
        callers_temp_storage.make_datum(|packer| {
            packer.push_list_with(|packer| {
                packer.push(Datum::Int64(i));
                packer.push(d);
            });
        })
    })
}

/// The expected input is in the format of `[((OriginalRow, EncodedArgs), OrderByExprs...)]`
/// For example,
///
/// lag(x*y, 1, null) over (partition by x+y order by x-y, x/y)
///
/// list of:
/// row(
///   row(
///     row(#0, #1),
///     row((#0 * #1), 1, null)
///   ),
///   (#0 - #1),
///   (#0 / #1)
/// )
///
/// The output is in the format of `[result_value, original_row]`, e.g.
/// list of:
/// row(
///   42,
///   row(7, 8)
/// )
fn lag_lead<'a, I>(
    datums: I,
    callers_temp_storage: &'a RowArena,
    order_by: &[ColumnOrder],
    lag_lead_type: &LagLeadType,
    ignore_nulls: &bool,
) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let temp_storage = RowArena::new();
    let iter = lag_lead_no_list(datums, &temp_storage, order_by, lag_lead_type, ignore_nulls);
    callers_temp_storage.make_datum(|packer| {
        packer.push_list(iter);
    })
}

/// Like `lag_lead`, but doesn't perform the final wrapping in a list, returning an Iterator
/// instead.
fn lag_lead_no_list<'a: 'b, 'b, I>(
    datums: I,
    callers_temp_storage: &'b RowArena,
    order_by: &[ColumnOrder],
    lag_lead_type: &LagLeadType,
    ignore_nulls: &bool,
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    // Sort the datums according to the ORDER BY expressions and return the (OriginalRow, EncodedArgs) record
    let datums = order_aggregate_datums(datums, order_by);

    // Take the (OriginalRow, EncodedArgs) records and unwrap them into separate datums.
    // EncodedArgs = (InputValue, Offset, DefaultValue) for Lag/Lead
    // (`OriginalRow` is kept in a record form, as we don't need to look inside that.)
    let (orig_rows, unwrapped_args): (Vec<_>, Vec<_>) = datums
        .into_iter()
        .map(|d| {
            let mut iter = d.unwrap_list().iter();
            let original_row = iter.next().unwrap();
            let (input_value, offset, default_value) =
                unwrap_lag_lead_encoded_args(iter.next().unwrap());
            (original_row, (input_value, offset, default_value))
        })
        .unzip();

    let result = lag_lead_inner(unwrapped_args, lag_lead_type, ignore_nulls);

    callers_temp_storage.reserve(result.len());
    result
        .into_iter()
        .zip_eq(orig_rows)
        .map(|(result_value, original_row)| {
            callers_temp_storage.make_datum(|packer| {
                packer.push_list_with(|packer| {
                    packer.push(result_value);
                    packer.push(original_row);
                });
            })
        })
}

/// lag/lead's arguments are in a record. This function unwraps this record.
fn unwrap_lag_lead_encoded_args(encoded_args: Datum) -> (Datum, Datum, Datum) {
    let mut encoded_args_iter = encoded_args.unwrap_list().iter();
    let (input_value, offset, default_value) = (
        encoded_args_iter.next().unwrap(),
        encoded_args_iter.next().unwrap(),
        encoded_args_iter.next().unwrap(),
    );
    (input_value, offset, default_value)
}

/// Each element of `args` has the 3 arguments evaluated for a single input row.
/// Returns the results for each input row.
fn lag_lead_inner<'a>(
    args: Vec<(Datum<'a>, Datum<'a>, Datum<'a>)>,
    lag_lead_type: &LagLeadType,
    ignore_nulls: &bool,
) -> Vec<Datum<'a>> {
    if *ignore_nulls {
        lag_lead_inner_ignore_nulls(args, lag_lead_type)
    } else {
        lag_lead_inner_respect_nulls(args, lag_lead_type)
    }
}

fn lag_lead_inner_respect_nulls<'a>(
    args: Vec<(Datum<'a>, Datum<'a>, Datum<'a>)>,
    lag_lead_type: &LagLeadType,
) -> Vec<Datum<'a>> {
    let mut result: Vec<Datum> = Vec::with_capacity(args.len());
    for (idx, (_, offset, default_value)) in args.iter().enumerate() {
        // Null offsets are acceptable, and always return null
        if offset.is_null() {
            result.push(Datum::Null);
            continue;
        }

        let idx = i64::try_from(idx).expect("Array index does not fit in i64");
        let offset = i64::from(offset.unwrap_int32());
        let offset = match lag_lead_type {
            LagLeadType::Lag => -offset,
            LagLeadType::Lead => offset,
        };

        // Get a Datum from `datums`. Return None if index is out of range.
        let datums_get = |i: i64| -> Option<Datum> {
            match u64::try_from(i) {
                Ok(i) => args
                    .get(usize::cast_from(i))
                    .map(|d| Some(d.0)) // succeeded in getting a Datum from the vec
                    .unwrap_or(None), // overindexing
                Err(_) => None, // underindexing (negative index)
            }
        };

        let lagged_value = datums_get(idx + offset).unwrap_or(*default_value);

        result.push(lagged_value);
    }

    result
}

// `i64` indexes get involved in this function because it's convenient to allow negative indexes and
// have `datums_get` fail on them, and thus handle the beginning and end of the input vector
// uniformly, rather than checking underflow separately during index manipulations.
#[allow(clippy::as_conversions)]
fn lag_lead_inner_ignore_nulls<'a>(
    args: Vec<(Datum<'a>, Datum<'a>, Datum<'a>)>,
    lag_lead_type: &LagLeadType,
) -> Vec<Datum<'a>> {
    // We check here once that even the largest index fits in `i64`, and then do silent `as`
    // conversions from `usize` indexes to `i64` indexes throughout this function.
    if i64::try_from(args.len()).is_err() {
        panic!("window partition way too big")
    }
    // Preparation: Make sure we can jump over a run of nulls in constant time, i.e., regardless of
    // how many nulls the run has. The following skip tables will point to the next non-null index.
    let mut skip_nulls_backward = vec![None; args.len()];
    let mut last_non_null: i64 = -1;
    let pairs = args
        .iter()
        .enumerate()
        .zip_eq(skip_nulls_backward.iter_mut());
    for ((i, (d, _, _)), slot) in pairs {
        if d.is_null() {
            *slot = Some(last_non_null);
        } else {
            last_non_null = i as i64;
        }
    }
    let mut skip_nulls_forward = vec![None; args.len()];
    let mut last_non_null: i64 = args.len() as i64;
    let pairs = args
        .iter()
        .enumerate()
        .rev()
        .zip_eq(skip_nulls_forward.iter_mut().rev());
    for ((i, (d, _, _)), slot) in pairs {
        if d.is_null() {
            *slot = Some(last_non_null);
        } else {
            last_non_null = i as i64;
        }
    }

    // The actual computation.
    let mut result: Vec<Datum> = Vec::with_capacity(args.len());
    for (idx, (_, offset, default_value)) in args.iter().enumerate() {
        // Null offsets are acceptable, and always return null
        if offset.is_null() {
            result.push(Datum::Null);
            continue;
        }

        let idx = idx as i64; // checked at the beginning of the function that len() fits
        let offset = i64::cast_from(offset.unwrap_int32());
        let offset = match lag_lead_type {
            LagLeadType::Lag => -offset,
            LagLeadType::Lead => offset,
        };
        let increment = offset.signum();

        // Get a Datum from `datums`. Return None if index is out of range.
        let datums_get = |i: i64| -> Option<Datum> {
            match u64::try_from(i) {
                Ok(i) => args
                    .get(usize::cast_from(i))
                    .map(|d| Some(d.0)) // succeeded in getting a Datum from the vec
                    .unwrap_or(None), // overindexing
                Err(_) => None, // underindexing (negative index)
            }
        };

        let lagged_value = if increment != 0 {
            // We start j from idx, and step j until we have seen an abs(offset) number of non-null
            // values or reach the beginning or end of the partition.
            //
            // If offset is big, then this is slow: Considering the entire function, it's
            // `O(partition_size * offset)`.
            // However, a common use case is an offset of 1, for which this doesn't matter.
            // TODO: For larger offsets, we could have a completely different implementation
            // that starts the inner loop from the index where we found the previous result:
            // https://github.com/MaterializeInc/materialize/pull/29287#discussion_r1738695174
            let mut j = idx;
            for _ in 0..num::abs(offset) {
                j += increment;
                // Jump over a run of nulls
                if datums_get(j).is_some_and(|d| d.is_null()) {
                    let ju = j as usize; // `j >= 0` because of the above `is_some_and`
                    if increment > 0 {
                        j = skip_nulls_forward[ju].expect("checked above that it's null");
                    } else {
                        j = skip_nulls_backward[ju].expect("checked above that it's null");
                    }
                }
                if datums_get(j).is_none() {
                    break;
                }
            }
            match datums_get(j) {
                Some(datum) => datum,
                None => *default_value,
            }
        } else {
            assert_eq!(offset, 0);
            let datum = datums_get(idx).expect("known to exist");
            if !datum.is_null() {
                datum
            } else {
                // I can imagine returning here either `default_value` or `null`.
                // (I'm leaning towards `default_value`.)
                // We used to run into an infinite loop in this case, so panicking is
                // better. Started a SQL Council thread:
                // https://materializeinc.slack.com/archives/C063H5S7NKE/p1724962369706729
                panic!("0 offset in lag/lead IGNORE NULLS");
            }
        };

        result.push(lagged_value);
    }

    result
}

/// The expected input is in the format of [((OriginalRow, InputValue), OrderByExprs...)]
fn first_value<'a, I>(
    datums: I,
    callers_temp_storage: &'a RowArena,
    order_by: &[ColumnOrder],
    window_frame: &WindowFrame,
) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let temp_storage = RowArena::new();
    let iter = first_value_no_list(datums, &temp_storage, order_by, window_frame);
    callers_temp_storage.make_datum(|packer| {
        packer.push_list(iter);
    })
}

/// Like `first_value`, but doesn't perform the final wrapping in a list, returning an Iterator
/// instead.
fn first_value_no_list<'a: 'b, 'b, I>(
    datums: I,
    callers_temp_storage: &'b RowArena,
    order_by: &[ColumnOrder],
    window_frame: &WindowFrame,
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    // Sort the datums according to the ORDER BY expressions and return the (OriginalRow, InputValue) record
    let datums = order_aggregate_datums(datums, order_by);

    // Decode the input (OriginalRow, InputValue) into separate datums
    let (orig_rows, args): (Vec<_>, Vec<_>) = datums
        .into_iter()
        .map(|d| {
            let mut iter = d.unwrap_list().iter();
            let original_row = iter.next().unwrap();
            let arg = iter.next().unwrap();

            (original_row, arg)
        })
        .unzip();

    let results = first_value_inner(args, window_frame);

    callers_temp_storage.reserve(results.len());
    results
        .into_iter()
        .zip_eq(orig_rows)
        .map(|(result_value, original_row)| {
            callers_temp_storage.make_datum(|packer| {
                packer.push_list_with(|packer| {
                    packer.push(result_value);
                    packer.push(original_row);
                });
            })
        })
}

fn first_value_inner<'a>(datums: Vec<Datum<'a>>, window_frame: &WindowFrame) -> Vec<Datum<'a>> {
    let length = datums.len();
    let mut result: Vec<Datum> = Vec::with_capacity(length);
    for (idx, current_datum) in datums.iter().enumerate() {
        let first_value = match &window_frame.start_bound {
            // Always return the current value
            WindowFrameBound::CurrentRow => *current_datum,
            WindowFrameBound::UnboundedPreceding => {
                if let WindowFrameBound::OffsetPreceding(end_offset) = &window_frame.end_bound {
                    let end_offset = usize::cast_from(*end_offset);

                    // If the frame ends before the first row, return null
                    if idx < end_offset {
                        Datum::Null
                    } else {
                        datums[0]
                    }
                } else {
                    datums[0]
                }
            }
            WindowFrameBound::OffsetPreceding(offset) => {
                let start_offset = usize::cast_from(*offset);
                let start_idx = idx.saturating_sub(start_offset);
                if let WindowFrameBound::OffsetPreceding(end_offset) = &window_frame.end_bound {
                    let end_offset = usize::cast_from(*end_offset);

                    // If the frame is empty or ends before the first row, return null
                    if start_offset < end_offset || idx < end_offset {
                        Datum::Null
                    } else {
                        datums[start_idx]
                    }
                } else {
                    datums[start_idx]
                }
            }
            WindowFrameBound::OffsetFollowing(offset) => {
                let start_offset = usize::cast_from(*offset);
                let start_idx = idx.saturating_add(start_offset);
                if let WindowFrameBound::OffsetFollowing(end_offset) = &window_frame.end_bound {
                    // If the frame is empty or starts after the last row, return null
                    if offset > end_offset || start_idx >= length {
                        Datum::Null
                    } else {
                        datums[start_idx]
                    }
                } else {
                    datums
                        .get(start_idx)
                        .map(|d| d.clone())
                        .unwrap_or(Datum::Null)
                }
            }
            // Forbidden during planning
            WindowFrameBound::UnboundedFollowing => unreachable!(),
        };
        result.push(first_value);
    }
    result
}

/// The expected input is in the format of [((OriginalRow, InputValue), OrderByExprs...)]
fn last_value<'a, I>(
    datums: I,
    callers_temp_storage: &'a RowArena,
    order_by: &[ColumnOrder],
    window_frame: &WindowFrame,
) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let temp_storage = RowArena::new();
    let iter = last_value_no_list(datums, &temp_storage, order_by, window_frame);
    callers_temp_storage.make_datum(|packer| {
        packer.push_list(iter);
    })
}

/// Like `last_value`, but doesn't perform the final wrapping in a list, returning an Iterator
/// instead.
fn last_value_no_list<'a: 'b, 'b, I>(
    datums: I,
    callers_temp_storage: &'b RowArena,
    order_by: &[ColumnOrder],
    window_frame: &WindowFrame,
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    // Sort the datums according to the ORDER BY expressions and return the ((OriginalRow, InputValue), OrderByRow) record
    // The OrderByRow is kept around because it is required to compute the peer groups in RANGE mode
    let datums = order_aggregate_datums_with_rank(datums, order_by);

    // Decode the input (OriginalRow, InputValue) into separate datums, while keeping the OrderByRow
    let size_hint = datums.size_hint().0;
    let mut args = Vec::with_capacity(size_hint);
    let mut original_rows = Vec::with_capacity(size_hint);
    let mut order_by_rows = Vec::with_capacity(size_hint);
    for (d, order_by_row) in datums.into_iter() {
        let mut iter = d.unwrap_list().iter();
        let original_row = iter.next().unwrap();
        let arg = iter.next().unwrap();
        order_by_rows.push(order_by_row);
        original_rows.push(original_row);
        args.push(arg);
    }

    let results = last_value_inner(args, &order_by_rows, window_frame);

    callers_temp_storage.reserve(results.len());
    results
        .into_iter()
        .zip_eq(original_rows)
        .map(|(result_value, original_row)| {
            callers_temp_storage.make_datum(|packer| {
                packer.push_list_with(|packer| {
                    packer.push(result_value);
                    packer.push(original_row);
                });
            })
        })
}

fn last_value_inner<'a>(
    args: Vec<Datum<'a>>,
    order_by_rows: &Vec<Row>,
    window_frame: &WindowFrame,
) -> Vec<Datum<'a>> {
    let length = args.len();
    let mut results: Vec<Datum> = Vec::with_capacity(length);
    for (idx, (current_datum, order_by_row)) in args.iter().zip_eq(order_by_rows).enumerate() {
        let last_value = match &window_frame.end_bound {
            WindowFrameBound::CurrentRow => match &window_frame.units {
                // Always return the current value when in ROWS mode
                WindowFrameUnits::Rows => *current_datum,
                WindowFrameUnits::Range => {
                    // When in RANGE mode, return the last value of the peer group
                    // The peer group is the group of rows with the same ORDER BY value
                    // Note: Range is only supported for the default window frame (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                    // which is why it does not appear in the other branches
                    let target_idx = order_by_rows[idx..]
                        .iter()
                        .enumerate()
                        .take_while(|(_, row)| *row == order_by_row)
                        .last()
                        .unwrap()
                        .0
                        + idx;
                    args[target_idx]
                }
                // GROUPS is not supported, and forbidden during planning
                WindowFrameUnits::Groups => unreachable!(),
            },
            WindowFrameBound::UnboundedFollowing => {
                if let WindowFrameBound::OffsetFollowing(start_offset) = &window_frame.start_bound {
                    let start_offset = usize::cast_from(*start_offset);

                    // If the frame starts after the last row of the window, return null
                    if idx + start_offset > length - 1 {
                        Datum::Null
                    } else {
                        args[length - 1]
                    }
                } else {
                    args[length - 1]
                }
            }
            WindowFrameBound::OffsetFollowing(offset) => {
                let end_offset = usize::cast_from(*offset);
                let end_idx = idx.saturating_add(end_offset);
                if let WindowFrameBound::OffsetFollowing(start_offset) = &window_frame.start_bound {
                    let start_offset = usize::cast_from(*start_offset);
                    let start_idx = idx.saturating_add(start_offset);

                    // If the frame is empty or starts after the last row of the window, return null
                    if end_offset < start_offset || start_idx >= length {
                        Datum::Null
                    } else {
                        // Return the last valid element in the window
                        args.get(end_idx).unwrap_or(&args[length - 1]).clone()
                    }
                } else {
                    args.get(end_idx).unwrap_or(&args[length - 1]).clone()
                }
            }
            WindowFrameBound::OffsetPreceding(offset) => {
                let end_offset = usize::cast_from(*offset);
                let end_idx = idx.saturating_sub(end_offset);
                if idx < end_offset {
                    // If the frame ends before the first row, return null
                    Datum::Null
                } else if let WindowFrameBound::OffsetPreceding(start_offset) =
                    &window_frame.start_bound
                {
                    // If the frame is empty, return null
                    if offset > start_offset {
                        Datum::Null
                    } else {
                        args[end_idx]
                    }
                } else {
                    args[end_idx]
                }
            }
            // Forbidden during planning
            WindowFrameBound::UnboundedPreceding => unreachable!(),
        };
        results.push(last_value);
    }
    results
}

/// Executes `FusedValueWindowFunc` on a reduction group.
/// The expected input is in the format of `[((OriginalRow, (Args1, Args2, ...)), OrderByExprs...)]`
/// where `Args1`, `Args2`, are the arguments of each of the fused functions. For functions that
/// have only a single argument (first_value/last_value), these are simple values. For functions
/// that have multiple arguments (lag/lead), these are also records.
fn fused_value_window_func<'a, I>(
    input_datums: I,
    callers_temp_storage: &'a RowArena,
    funcs: &Vec<AggregateFunc>,
    order_by: &Vec<ColumnOrder>,
) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let temp_storage = RowArena::new();
    let iter = fused_value_window_func_no_list(input_datums, &temp_storage, funcs, order_by);
    callers_temp_storage.make_datum(|packer| {
        packer.push_list(iter);
    })
}

/// Like `fused_value_window_func`, but doesn't perform the final wrapping in a list, returning an
/// Iterator instead.
fn fused_value_window_func_no_list<'a: 'b, 'b, I>(
    input_datums: I,
    callers_temp_storage: &'b RowArena,
    funcs: &Vec<AggregateFunc>,
    order_by: &Vec<ColumnOrder>,
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
{
    let has_last_value = funcs
        .iter()
        .any(|f| matches!(f, AggregateFunc::LastValue { .. }));

    let input_datums_with_ranks = order_aggregate_datums_with_rank(input_datums, order_by);

    let size_hint = input_datums_with_ranks.size_hint().0;
    let mut encoded_argsss = vec![Vec::with_capacity(size_hint); funcs.len()];
    let mut original_rows = Vec::with_capacity(size_hint);
    let mut order_by_rows = Vec::with_capacity(size_hint);
    for (d, order_by_row) in input_datums_with_ranks {
        let mut iter = d.unwrap_list().iter();
        let original_row = iter.next().unwrap();
        original_rows.push(original_row);
        let mut argss_iter = iter.next().unwrap().unwrap_list().iter();
        for i in 0..funcs.len() {
            let encoded_args = argss_iter.next().unwrap();
            encoded_argsss[i].push(encoded_args);
        }
        if has_last_value {
            order_by_rows.push(order_by_row);
        }
    }

    let mut results_per_row = vec![Vec::with_capacity(funcs.len()); original_rows.len()];
    for (func, encoded_argss) in funcs.iter().zip_eq(encoded_argsss) {
        let results = match func {
            AggregateFunc::LagLead {
                order_by: inner_order_by,
                lag_lead,
                ignore_nulls,
            } => {
                assert_eq!(order_by, inner_order_by);
                let unwrapped_argss = encoded_argss
                    .into_iter()
                    .map(|encoded_args| unwrap_lag_lead_encoded_args(encoded_args))
                    .collect();
                lag_lead_inner(unwrapped_argss, lag_lead, ignore_nulls)
            }
            AggregateFunc::FirstValue {
                order_by: inner_order_by,
                window_frame,
            } => {
                assert_eq!(order_by, inner_order_by);
                // (No unwrapping to do on the args here, because there is only 1 arg, so it's not
                // wrapped into a record.)
                first_value_inner(encoded_argss, window_frame)
            }
            AggregateFunc::LastValue {
                order_by: inner_order_by,
                window_frame,
            } => {
                assert_eq!(order_by, inner_order_by);
                // (No unwrapping to do on the args here, because there is only 1 arg, so it's not
                // wrapped into a record.)
                last_value_inner(encoded_argss, &order_by_rows, window_frame)
            }
            _ => panic!("unknown window function in FusedValueWindowFunc"),
        };
        for (results, result) in results_per_row.iter_mut().zip_eq(results) {
            results.push(result);
        }
    }

    callers_temp_storage.reserve(2 * original_rows.len());
    results_per_row
        .into_iter()
        .enumerate()
        .map(move |(i, results)| {
            callers_temp_storage.make_datum(|packer| {
                packer.push_list_with(|packer| {
                    packer
                        .push(callers_temp_storage.make_datum(|packer| packer.push_list(results)));
                    packer.push(original_rows[i]);
                });
            })
        })
}

/// `input_datums` is an entire window partition.
/// The expected input is in the format of `[((OriginalRow, InputValue), OrderByExprs...)]`
/// See also in the comment in `window_func_applied_to`.
///
/// `wrapped_aggregate`: e.g., for `sum(...) OVER (...)`, this is the `sum(...)`.
///
/// Note that this `order_by` doesn't have expressions, only `ColumnOrder`s. For an explanation,
/// see the comment on `WindowExprType`.
fn window_aggr<'a, I, A>(
    input_datums: I,
    callers_temp_storage: &'a RowArena,
    wrapped_aggregate: &AggregateFunc,
    order_by: &[ColumnOrder],
    window_frame: &WindowFrame,
) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
    A: OneByOneAggr,
{
    let temp_storage = RowArena::new();
    let iter = window_aggr_no_list::<I, A>(
        input_datums,
        &temp_storage,
        wrapped_aggregate,
        order_by,
        window_frame,
    );
    callers_temp_storage.make_datum(|packer| {
        packer.push_list(iter);
    })
}

/// Like `window_aggr`, but doesn't perform the final wrapping in a list, returning an Iterator
/// instead.
fn window_aggr_no_list<'a: 'b, 'b, I, A>(
    input_datums: I,
    callers_temp_storage: &'b RowArena,
    wrapped_aggregate: &AggregateFunc,
    order_by: &[ColumnOrder],
    window_frame: &WindowFrame,
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
    A: OneByOneAggr,
{
    // Sort the datums according to the ORDER BY expressions and return the ((OriginalRow, InputValue), OrderByRow) record
    // The OrderByRow is kept around because it is required to compute the peer groups in RANGE mode
    let datums = order_aggregate_datums_with_rank(input_datums, order_by);

    // Decode the input (OriginalRow, InputValue) into separate datums, while keeping the OrderByRow
    let size_hint = datums.size_hint().0;
    let mut args: Vec<Datum> = Vec::with_capacity(size_hint);
    let mut original_rows: Vec<Datum> = Vec::with_capacity(size_hint);
    let mut order_by_rows = Vec::with_capacity(size_hint);
    for (d, order_by_row) in datums.into_iter() {
        let mut iter = d.unwrap_list().iter();
        let original_row = iter.next().unwrap();
        let arg = iter.next().unwrap();
        order_by_rows.push(order_by_row);
        original_rows.push(original_row);
        args.push(arg);
    }

    let results = window_aggr_inner::<A>(
        args,
        &order_by_rows,
        wrapped_aggregate,
        order_by,
        window_frame,
        callers_temp_storage,
    );

    callers_temp_storage.reserve(results.len());
    results
        .into_iter()
        .zip_eq(original_rows)
        .map(|(result_value, original_row)| {
            callers_temp_storage.make_datum(|packer| {
                packer.push_list_with(|packer| {
                    packer.push(result_value);
                    packer.push(original_row);
                });
            })
        })
}

fn window_aggr_inner<'a, A>(
    mut args: Vec<Datum<'a>>,
    order_by_rows: &Vec<Row>,
    wrapped_aggregate: &AggregateFunc,
    order_by: &[ColumnOrder],
    window_frame: &WindowFrame,
    temp_storage: &'a RowArena,
) -> Vec<Datum<'a>>
where
    A: OneByOneAggr,
{
    let length = args.len();
    let mut result: Vec<Datum> = Vec::with_capacity(length);

    // In this degenerate case, all results would be `wrapped_aggregate.default()` (usually null).
    // However, this currently can't happen, because
    // - Groups frame mode is currently not supported;
    // - Range frame mode is currently supported only for the default frame, which includes the
    //   current row.
    soft_assert_or_log!(
        !((matches!(window_frame.units, WindowFrameUnits::Groups)
            || matches!(window_frame.units, WindowFrameUnits::Range))
            && !window_frame.includes_current_row()),
        "window frame without current row"
    );

    if (matches!(
        window_frame.start_bound,
        WindowFrameBound::UnboundedPreceding
    ) && matches!(window_frame.end_bound, WindowFrameBound::UnboundedFollowing))
        || (order_by.is_empty()
            && (matches!(window_frame.units, WindowFrameUnits::Groups)
                || matches!(window_frame.units, WindowFrameUnits::Range))
            && window_frame.includes_current_row())
    {
        // Either
        //  - UNBOUNDED frame in both directions, or
        //  - There is no ORDER BY and the frame is such that the current peer group is included.
        //    (The current peer group will be the whole partition if there is no ORDER BY.)
        // We simply need to compute the aggregate once, on the entire partition, and each input
        // row will get this one aggregate value as result.
        let result_value = wrapped_aggregate.eval(args, temp_storage);
        // Every row will get the above aggregate as result.
        for _ in 0..length {
            result.push(result_value);
        }
    } else {
        fn rows_between_unbounded_preceding_and_current_row<'a, A>(
            args: Vec<Datum<'a>>,
            result: &mut Vec<Datum<'a>>,
            mut one_by_one_aggr: A,
            temp_storage: &'a RowArena,
        ) where
            A: OneByOneAggr,
        {
            for current_arg in args.into_iter() {
                one_by_one_aggr.give(&current_arg);
                let result_value = one_by_one_aggr.get_current_aggregate(temp_storage);
                result.push(result_value);
            }
        }

        fn groups_between_unbounded_preceding_and_current_row<'a, A>(
            args: Vec<Datum<'a>>,
            order_by_rows: &Vec<Row>,
            result: &mut Vec<Datum<'a>>,
            mut one_by_one_aggr: A,
            temp_storage: &'a RowArena,
        ) where
            A: OneByOneAggr,
        {
            let mut peer_group_start = 0;
            while peer_group_start < args.len() {
                // Find the boundaries of the current peer group.
                // peer_group_start will point to the first element of the peer group,
                // peer_group_end will point to _just after_ the last element of the peer group.
                let mut peer_group_end = peer_group_start + 1;
                while peer_group_end < args.len()
                    && order_by_rows[peer_group_start] == order_by_rows[peer_group_end]
                {
                    // The peer group goes on while the OrderByRows not differ.
                    peer_group_end += 1;
                }
                // Let's compute the aggregate (which will be the same for all records in this
                // peer group).
                for current_arg in args[peer_group_start..peer_group_end].iter() {
                    one_by_one_aggr.give(current_arg);
                }
                let agg_for_peer_group = one_by_one_aggr.get_current_aggregate(temp_storage);
                // Put the above aggregate into each record in the peer group.
                for _ in args[peer_group_start..peer_group_end].iter() {
                    result.push(agg_for_peer_group);
                }
                // Point to the start of the next peer group.
                peer_group_start = peer_group_end;
            }
        }

        fn rows_between_offset_and_offset<'a>(
            args: Vec<Datum<'a>>,
            result: &mut Vec<Datum<'a>>,
            wrapped_aggregate: &AggregateFunc,
            temp_storage: &'a RowArena,
            offset_start: i64,
            offset_end: i64,
        ) {
            let len = args
                .len()
                .to_i64()
                .expect("window partition's len should fit into i64");
            for i in 0..len {
                let i = i.to_i64().expect("window partition shouldn't be super big");
                // Trim the start of the frame to make it not reach over the start of the window
                // partition.
                let frame_start = max(i + offset_start, 0)
                    .to_usize()
                    .expect("The max made sure it's not negative");
                // Trim the end of the frame to make it not reach over the end of the window
                // partition.
                let frame_end = min(i + offset_end, len - 1).to_usize();
                match frame_end {
                    Some(frame_end) => {
                        if frame_start <= frame_end {
                            // Compute the aggregate on the frame.
                            // TODO:
                            // This implementation is quite slow if the frame is large: we do an
                            // inner loop over the entire frame, and compute the aggregate from
                            // scratch. We could do better:
                            //  - For invertible aggregations we could do a rolling aggregation.
                            //  - There are various tricks for min/max as well, making use of either
                            //    the fixed size of the window, or that we are not retracting
                            //    arbitrary elements but doing queue operations. E.g., see
                            //    http://codercareer.blogspot.com/2012/02/no-33-maximums-in-sliding-windows.html
                            let frame_values = args[frame_start..=frame_end].iter().cloned();
                            let result_value = wrapped_aggregate.eval(frame_values, temp_storage);
                            result.push(result_value);
                        } else {
                            // frame_start > frame_end, so this is an empty frame.
                            let result_value = wrapped_aggregate.default();
                            result.push(result_value);
                        }
                    }
                    None => {
                        // frame_end would be negative, so this is an empty frame.
                        let result_value = wrapped_aggregate.default();
                        result.push(result_value);
                    }
                }
            }
        }

        match (
            &window_frame.units,
            &window_frame.start_bound,
            &window_frame.end_bound,
        ) {
            // Cases where one edge of the frame is CurrentRow.
            // Note that these cases could be merged into the more general cases below where one
            // edge is some offset (with offset = 0), but the CurrentRow cases probably cover 95%
            // of user queries, so let's make this simple and fast.
            (Rows, UnboundedPreceding, CurrentRow) => {
                rows_between_unbounded_preceding_and_current_row::<A>(
                    args,
                    &mut result,
                    A::new(wrapped_aggregate, false),
                    temp_storage,
                );
            }
            (Rows, CurrentRow, UnboundedFollowing) => {
                // Same as above, but reverse.
                args.reverse();
                rows_between_unbounded_preceding_and_current_row::<A>(
                    args,
                    &mut result,
                    A::new(wrapped_aggregate, true),
                    temp_storage,
                );
                result.reverse();
            }
            (Range, UnboundedPreceding, CurrentRow) => {
                // Note that for the default frame, the RANGE frame mode is identical to the GROUPS
                // frame mode.
                groups_between_unbounded_preceding_and_current_row::<A>(
                    args,
                    order_by_rows,
                    &mut result,
                    A::new(wrapped_aggregate, false),
                    temp_storage,
                );
            }
            // The next several cases all call `rows_between_offset_and_offset`. Note that the
            // offset passed to `rows_between_offset_and_offset` should be negated when it's
            // PRECEDING.
            (Rows, OffsetPreceding(start_prec), OffsetPreceding(end_prec)) => {
                let start_prec = start_prec.to_i64().expect(
                    "window frame start OFFSET shouldn't be super big (the planning ensured this)",
                );
                let end_prec = end_prec.to_i64().expect(
                    "window frame end OFFSET shouldn't be super big (the planning ensured this)",
                );
                rows_between_offset_and_offset(
                    args,
                    &mut result,
                    wrapped_aggregate,
                    temp_storage,
                    -start_prec,
                    -end_prec,
                );
            }
            (Rows, OffsetPreceding(start_prec), OffsetFollowing(end_fol)) => {
                let start_prec = start_prec.to_i64().expect(
                    "window frame start OFFSET shouldn't be super big (the planning ensured this)",
                );
                let end_fol = end_fol.to_i64().expect(
                    "window frame end OFFSET shouldn't be super big (the planning ensured this)",
                );
                rows_between_offset_and_offset(
                    args,
                    &mut result,
                    wrapped_aggregate,
                    temp_storage,
                    -start_prec,
                    end_fol,
                );
            }
            (Rows, OffsetFollowing(start_fol), OffsetFollowing(end_fol)) => {
                let start_fol = start_fol.to_i64().expect(
                    "window frame start OFFSET shouldn't be super big (the planning ensured this)",
                );
                let end_fol = end_fol.to_i64().expect(
                    "window frame end OFFSET shouldn't be super big (the planning ensured this)",
                );
                rows_between_offset_and_offset(
                    args,
                    &mut result,
                    wrapped_aggregate,
                    temp_storage,
                    start_fol,
                    end_fol,
                );
            }
            (Rows, OffsetFollowing(_), OffsetPreceding(_)) => {
                unreachable!() // The planning ensured that this nonsensical case can't happen
            }
            (Rows, OffsetPreceding(start_prec), CurrentRow) => {
                let start_prec = start_prec.to_i64().expect(
                    "window frame start OFFSET shouldn't be super big (the planning ensured this)",
                );
                let end_fol = 0;
                rows_between_offset_and_offset(
                    args,
                    &mut result,
                    wrapped_aggregate,
                    temp_storage,
                    -start_prec,
                    end_fol,
                );
            }
            (Rows, CurrentRow, OffsetFollowing(end_fol)) => {
                let start_fol = 0;
                let end_fol = end_fol.to_i64().expect(
                    "window frame end OFFSET shouldn't be super big (the planning ensured this)",
                );
                rows_between_offset_and_offset(
                    args,
                    &mut result,
                    wrapped_aggregate,
                    temp_storage,
                    start_fol,
                    end_fol,
                );
            }
            (Rows, CurrentRow, CurrentRow) => {
                // We could have a more efficient implementation for this, but this is probably
                // super rare. (Might be more common with RANGE or GROUPS frame mode, though!)
                let start_fol = 0;
                let end_fol = 0;
                rows_between_offset_and_offset(
                    args,
                    &mut result,
                    wrapped_aggregate,
                    temp_storage,
                    start_fol,
                    end_fol,
                );
            }
            (Rows, CurrentRow, OffsetPreceding(_))
            | (Rows, UnboundedFollowing, _)
            | (Rows, _, UnboundedPreceding)
            | (Rows, OffsetFollowing(..), CurrentRow) => {
                unreachable!() // The planning ensured that these nonsensical cases can't happen
            }
            (Rows, UnboundedPreceding, UnboundedFollowing) => {
                // This is handled by the complicated if condition near the beginning of this
                // function.
                unreachable!()
            }
            (Rows, UnboundedPreceding, OffsetPreceding(_))
            | (Rows, UnboundedPreceding, OffsetFollowing(_))
            | (Rows, OffsetPreceding(..), UnboundedFollowing)
            | (Rows, OffsetFollowing(..), UnboundedFollowing) => {
                // Unsupported. Bail in the planner.
                // https://github.com/MaterializeInc/database-issues/issues/6720
                unreachable!()
            }
            (Range, _, _) => {
                // Unsupported.
                // The planner doesn't allow Range frame mode for now (except for the default
                // frame), see https://github.com/MaterializeInc/database-issues/issues/6585
                // Note that it would be easy to handle (Range, CurrentRow, UnboundedFollowing):
                // it would be similar to (Rows, CurrentRow, UnboundedFollowing), but would call
                // groups_between_unbounded_preceding_current_row.
                unreachable!()
            }
            (Groups, _, _) => {
                // Unsupported.
                // The planner doesn't allow Groups frame mode for now, see
                // https://github.com/MaterializeInc/database-issues/issues/6588
                unreachable!()
            }
        }
    }

    result
}

/// Computes a bundle of fused window aggregations.
/// The input is similar to `window_aggr`, but `InputValue` is not just a single value, but a record
/// where each component is the input to one of the aggregations.
fn fused_window_aggr<'a, I, A>(
    input_datums: I,
    callers_temp_storage: &'a RowArena,
    wrapped_aggregates: &Vec<AggregateFunc>,
    order_by: &Vec<ColumnOrder>,
    window_frame: &WindowFrame,
) -> Datum<'a>
where
    I: IntoIterator<Item = Datum<'a>>,
    A: OneByOneAggr,
{
    let temp_storage = RowArena::new();
    let iter = fused_window_aggr_no_list::<_, A>(
        input_datums,
        &temp_storage,
        wrapped_aggregates,
        order_by,
        window_frame,
    );
    callers_temp_storage.make_datum(|packer| {
        packer.push_list(iter);
    })
}

/// Like `fused_window_aggr`, but doesn't perform the final wrapping in a list, returning an
/// Iterator instead.
fn fused_window_aggr_no_list<'a: 'b, 'b, I, A>(
    input_datums: I,
    callers_temp_storage: &'b RowArena,
    wrapped_aggregates: &Vec<AggregateFunc>,
    order_by: &Vec<ColumnOrder>,
    window_frame: &WindowFrame,
) -> impl Iterator<Item = Datum<'b>>
where
    I: IntoIterator<Item = Datum<'a>>,
    A: OneByOneAggr,
{
    // Sort the datums according to the ORDER BY expressions and return the ((OriginalRow, InputValue), OrderByRow) record
    // The OrderByRow is kept around because it is required to compute the peer groups in RANGE mode
    let datums = order_aggregate_datums_with_rank(input_datums, order_by);

    let size_hint = datums.size_hint().0;
    let mut argss = vec![Vec::with_capacity(size_hint); wrapped_aggregates.len()];
    let mut original_rows = Vec::with_capacity(size_hint);
    let mut order_by_rows = Vec::with_capacity(size_hint);
    for (d, order_by_row) in datums {
        let mut iter = d.unwrap_list().iter();
        let original_row = iter.next().unwrap();
        original_rows.push(original_row);
        let args_iter = iter.next().unwrap().unwrap_list().iter();
        // Push each argument into the respective list
        for (args, arg) in argss.iter_mut().zip_eq(args_iter) {
            args.push(arg);
        }
        order_by_rows.push(order_by_row);
    }

    let mut results_per_row =
        vec![Vec::with_capacity(wrapped_aggregates.len()); original_rows.len()];
    for (wrapped_aggr, args) in wrapped_aggregates.iter().zip_eq(argss) {
        let results = window_aggr_inner::<A>(
            args,
            &order_by_rows,
            wrapped_aggr,
            order_by,
            window_frame,
            callers_temp_storage,
        );
        for (results, result) in results_per_row.iter_mut().zip_eq(results) {
            results.push(result);
        }
    }

    callers_temp_storage.reserve(2 * original_rows.len());
    results_per_row
        .into_iter()
        .enumerate()
        .map(move |(i, results)| {
            callers_temp_storage.make_datum(|packer| {
                packer.push_list_with(|packer| {
                    packer
                        .push(callers_temp_storage.make_datum(|packer| packer.push_list(results)));
                    packer.push(original_rows[i]);
                });
            })
        })
}

/// An implementation of an aggregation where we can send in the input elements one-by-one, and
/// can also ask the current aggregate at any moment. (This just delegates to other aggregation
/// evaluation approaches.)
pub trait OneByOneAggr {
    /// The `reverse` parameter makes the aggregations process input elements in reverse order.
    /// This has an effect only for non-commutative aggregations, e.g. `list_agg`. These are
    /// currently only some of the Basic aggregations. (Basic aggregations are handled by
    /// `NaiveOneByOneAggr`).
    fn new(agg: &AggregateFunc, reverse: bool) -> Self;
    /// Pushes one input element into the aggregation.
    fn give(&mut self, d: &Datum);
    /// Returns the value of the aggregate computed on the given values so far.
    fn get_current_aggregate<'a>(&self, temp_storage: &'a RowArena) -> Datum<'a>;
}

/// Naive implementation of [OneByOneAggr], suitable for stuff like const folding, but too slow for
/// rendering. This relies only on infrastructure available in `mz-expr`. It simply saves all the
/// given input, and calls the given [AggregateFunc]'s `eval` method when asked about the current
/// aggregate. (For Accumulable and Hierarchical aggregations, the rendering has more efficient
/// implementations, but for Basic aggregations even the rendering uses this naive implementation.)
#[derive(Debug)]
pub struct NaiveOneByOneAggr {
    agg: AggregateFunc,
    input: Vec<Row>,
    reverse: bool,
}

impl OneByOneAggr for NaiveOneByOneAggr {
    fn new(agg: &AggregateFunc, reverse: bool) -> Self {
        NaiveOneByOneAggr {
            agg: agg.clone(),
            input: Vec::new(),
            reverse,
        }
    }

    fn give(&mut self, d: &Datum) {
        let mut row = Row::default();
        row.packer().push(d);
        self.input.push(row);
    }

    fn get_current_aggregate<'a>(&self, temp_storage: &'a RowArena) -> Datum<'a> {
        temp_storage.make_datum(|packer| {
            packer.push(if !self.reverse {
                self.agg
                    .eval(self.input.iter().map(|r| r.unpack_first()), temp_storage)
            } else {
                self.agg.eval(
                    self.input.iter().rev().map(|r| r.unpack_first()),
                    temp_storage,
                )
            });
        })
    }
}

/// Identify whether the given aggregate function is Lag or Lead, since they share
/// implementations.
#[derive(
    Arbitrary, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
pub enum LagLeadType {
    Lag,
    Lead,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect)]
pub enum AggregateFunc {
    MaxNumeric,
    MaxInt16,
    MaxInt32,
    MaxInt64,
    MaxUInt16,
    MaxUInt32,
    MaxUInt64,
    MaxMzTimestamp,
    MaxFloat32,
    MaxFloat64,
    MaxBool,
    MaxString,
    MaxDate,
    MaxTimestamp,
    MaxTimestampTz,
    MaxInterval,
    MaxTime,
    MinNumeric,
    MinInt16,
    MinInt32,
    MinInt64,
    MinUInt16,
    MinUInt32,
    MinUInt64,
    MinMzTimestamp,
    MinFloat32,
    MinFloat64,
    MinBool,
    MinString,
    MinDate,
    MinTimestamp,
    MinTimestampTz,
    MinInterval,
    MinTime,
    SumInt16,
    SumInt32,
    SumInt64,
    SumUInt16,
    SumUInt32,
    SumUInt64,
    SumFloat32,
    SumFloat64,
    SumNumeric,
    Count,
    Any,
    All,
    /// Accumulates `Datum::List`s whose first element is a JSON-typed `Datum`s
    /// into a JSON list. The other elements are columns used by `order_by`.
    ///
    /// WARNING: Unlike the `jsonb_agg` function that is exposed by the SQL
    /// layer, this function filters out `Datum::Null`, for consistency with
    /// the other aggregate functions.
    JsonbAgg {
        order_by: Vec<ColumnOrder>,
    },
    /// Zips `Datum::List`s whose first element is a JSON-typed `Datum`s into a
    /// JSON map. The other elements are columns used by `order_by`.
    ///
    /// WARNING: Unlike the `jsonb_object_agg` function that is exposed by the SQL
    /// layer, this function filters out `Datum::Null`, for consistency with
    /// the other aggregate functions.
    JsonbObjectAgg {
        order_by: Vec<ColumnOrder>,
    },
    /// Zips a `Datum::List` whose first element is a `Datum::List` guaranteed
    /// to be non-empty and whose len % 2 == 0 into a `Datum::Map`. The other
    /// elements are columns used by `order_by`.
    MapAgg {
        order_by: Vec<ColumnOrder>,
        value_type: ScalarType,
    },
    /// Accumulates `Datum::Array`s of `ScalarType::Record` whose first element is a `Datum::Array`
    /// into a single `Datum::Array` (the remaining fields are used by `order_by`).
    ArrayConcat {
        order_by: Vec<ColumnOrder>,
    },
    /// Accumulates `Datum::List`s of `ScalarType::Record` whose first field is a `Datum::List`
    /// into a single `Datum::List` (the remaining fields are used by `order_by`).
    ListConcat {
        order_by: Vec<ColumnOrder>,
    },
    StringAgg {
        order_by: Vec<ColumnOrder>,
    },
    RowNumber {
        order_by: Vec<ColumnOrder>,
    },
    Rank {
        order_by: Vec<ColumnOrder>,
    },
    DenseRank {
        order_by: Vec<ColumnOrder>,
    },
    LagLead {
        order_by: Vec<ColumnOrder>,
        lag_lead: LagLeadType,
        ignore_nulls: bool,
    },
    FirstValue {
        order_by: Vec<ColumnOrder>,
        window_frame: WindowFrame,
    },
    LastValue {
        order_by: Vec<ColumnOrder>,
        window_frame: WindowFrame,
    },
    /// Several value window functions fused into one function, to amortize overheads.
    FusedValueWindowFunc {
        funcs: Vec<AggregateFunc>,
        /// Currently, all the fused functions must have the same `order_by`. (We can later
        /// eliminate this limitation.)
        order_by: Vec<ColumnOrder>,
    },
    WindowAggregate {
        wrapped_aggregate: Box<AggregateFunc>,
        order_by: Vec<ColumnOrder>,
        window_frame: WindowFrame,
    },
    FusedWindowAggregate {
        wrapped_aggregates: Vec<AggregateFunc>,
        order_by: Vec<ColumnOrder>,
        window_frame: WindowFrame,
    },
    /// Accumulates any number of `Datum::Dummy`s into `Datum::Dummy`.
    ///
    /// Useful for removing an expensive aggregation while maintaining the shape
    /// of a reduce operator.
    Dummy,
}

/// An explicit [`Arbitrary`] implementation needed here because of a known
/// `proptest` issue.
///
/// Revert to the derive-macro implementation once the issue[^1] is fixed.
///
/// [^1]: <https://github.com/AltSysrq/proptest/issues/152>
impl Arbitrary for AggregateFunc {
    type Parameters = ();

    type Strategy = Union<BoxedStrategy<Self>>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        use proptest::collection::vec;
        use proptest::prelude::any as proptest_any;
        Union::new(vec![
            Just(AggregateFunc::MaxNumeric).boxed(),
            Just(AggregateFunc::MaxInt16).boxed(),
            Just(AggregateFunc::MaxInt32).boxed(),
            Just(AggregateFunc::MaxInt64).boxed(),
            Just(AggregateFunc::MaxUInt16).boxed(),
            Just(AggregateFunc::MaxUInt32).boxed(),
            Just(AggregateFunc::MaxUInt64).boxed(),
            Just(AggregateFunc::MaxMzTimestamp).boxed(),
            Just(AggregateFunc::MaxFloat32).boxed(),
            Just(AggregateFunc::MaxFloat64).boxed(),
            Just(AggregateFunc::MaxBool).boxed(),
            Just(AggregateFunc::MaxString).boxed(),
            Just(AggregateFunc::MaxTimestamp).boxed(),
            Just(AggregateFunc::MaxDate).boxed(),
            Just(AggregateFunc::MaxTimestampTz).boxed(),
            Just(AggregateFunc::MaxInterval).boxed(),
            Just(AggregateFunc::MaxTime).boxed(),
            Just(AggregateFunc::MinNumeric).boxed(),
            Just(AggregateFunc::MinInt16).boxed(),
            Just(AggregateFunc::MinInt32).boxed(),
            Just(AggregateFunc::MinInt64).boxed(),
            Just(AggregateFunc::MinUInt16).boxed(),
            Just(AggregateFunc::MinUInt32).boxed(),
            Just(AggregateFunc::MinUInt64).boxed(),
            Just(AggregateFunc::MinMzTimestamp).boxed(),
            Just(AggregateFunc::MinFloat32).boxed(),
            Just(AggregateFunc::MinFloat64).boxed(),
            Just(AggregateFunc::MinBool).boxed(),
            Just(AggregateFunc::MinString).boxed(),
            Just(AggregateFunc::MinDate).boxed(),
            Just(AggregateFunc::MinTimestamp).boxed(),
            Just(AggregateFunc::MinTimestampTz).boxed(),
            Just(AggregateFunc::MinInterval).boxed(),
            Just(AggregateFunc::MinTime).boxed(),
            Just(AggregateFunc::SumInt16).boxed(),
            Just(AggregateFunc::SumInt32).boxed(),
            Just(AggregateFunc::SumInt64).boxed(),
            Just(AggregateFunc::SumUInt16).boxed(),
            Just(AggregateFunc::SumUInt32).boxed(),
            Just(AggregateFunc::SumUInt64).boxed(),
            Just(AggregateFunc::SumFloat32).boxed(),
            Just(AggregateFunc::SumFloat64).boxed(),
            Just(AggregateFunc::SumNumeric).boxed(),
            Just(AggregateFunc::Count).boxed(),
            Just(AggregateFunc::Any).boxed(),
            Just(AggregateFunc::All).boxed(),
            vec(proptest_any::<ColumnOrder>(), 1..4)
                .prop_map(|order_by| AggregateFunc::JsonbAgg { order_by })
                .boxed(),
            vec(proptest_any::<ColumnOrder>(), 1..4)
                .prop_map(|order_by| AggregateFunc::JsonbObjectAgg { order_by })
                .boxed(),
            (
                vec(proptest_any::<ColumnOrder>(), 1..4),
                proptest_any::<ScalarType>(),
            )
                .prop_map(|(order_by, value_type)| AggregateFunc::MapAgg {
                    order_by,
                    value_type,
                })
                .boxed(),
            vec(proptest_any::<ColumnOrder>(), 1..4)
                .prop_map(|order_by| AggregateFunc::ArrayConcat { order_by })
                .boxed(),
            vec(proptest_any::<ColumnOrder>(), 1..4)
                .prop_map(|order_by| AggregateFunc::ListConcat { order_by })
                .boxed(),
            vec(proptest_any::<ColumnOrder>(), 1..4)
                .prop_map(|order_by| AggregateFunc::StringAgg { order_by })
                .boxed(),
            vec(proptest_any::<ColumnOrder>(), 1..4)
                .prop_map(|order_by| AggregateFunc::RowNumber { order_by })
                .boxed(),
            vec(proptest_any::<ColumnOrder>(), 1..4)
                .prop_map(|order_by| AggregateFunc::DenseRank { order_by })
                .boxed(),
            (
                vec(proptest_any::<ColumnOrder>(), 1..4),
                proptest_any::<LagLeadType>(),
                proptest_any::<bool>(),
            )
                .prop_map(
                    |(order_by, lag_lead, ignore_nulls)| AggregateFunc::LagLead {
                        order_by,
                        lag_lead,
                        ignore_nulls,
                    },
                )
                .boxed(),
            (
                vec(proptest_any::<ColumnOrder>(), 1..4),
                proptest_any::<WindowFrame>(),
            )
                .prop_map(|(order_by, window_frame)| AggregateFunc::FirstValue {
                    order_by,
                    window_frame,
                })
                .boxed(),
            (
                vec(proptest_any::<ColumnOrder>(), 1..4),
                proptest_any::<WindowFrame>(),
            )
                .prop_map(|(order_by, window_frame)| AggregateFunc::LastValue {
                    order_by,
                    window_frame,
                })
                .boxed(),
            Just(AggregateFunc::Dummy).boxed(),
        ])
    }
}

impl RustType<ProtoColumnOrders> for Vec<ColumnOrder> {
    fn into_proto(&self) -> ProtoColumnOrders {
        ProtoColumnOrders {
            orders: self.into_proto(),
        }
    }

    fn from_proto(proto: ProtoColumnOrders) -> Result<Self, TryFromProtoError> {
        proto.orders.into_rust()
    }
}

impl RustType<ProtoAggregateFunc> for AggregateFunc {
    fn into_proto(&self) -> ProtoAggregateFunc {
        use proto_aggregate_func::Kind;
        ProtoAggregateFunc {
            kind: Some(match self {
                AggregateFunc::MaxNumeric => Kind::MaxNumeric(()),
                AggregateFunc::MaxInt16 => Kind::MaxInt16(()),
                AggregateFunc::MaxInt32 => Kind::MaxInt32(()),
                AggregateFunc::MaxInt64 => Kind::MaxInt64(()),
                AggregateFunc::MaxUInt16 => Kind::MaxUint16(()),
                AggregateFunc::MaxUInt32 => Kind::MaxUint32(()),
                AggregateFunc::MaxUInt64 => Kind::MaxUint64(()),
                AggregateFunc::MaxMzTimestamp => Kind::MaxMzTimestamp(()),
                AggregateFunc::MaxFloat32 => Kind::MaxFloat32(()),
                AggregateFunc::MaxFloat64 => Kind::MaxFloat64(()),
                AggregateFunc::MaxBool => Kind::MaxBool(()),
                AggregateFunc::MaxString => Kind::MaxString(()),
                AggregateFunc::MaxDate => Kind::MaxDate(()),
                AggregateFunc::MaxTimestamp => Kind::MaxTimestamp(()),
                AggregateFunc::MaxTimestampTz => Kind::MaxTimestampTz(()),
                AggregateFunc::MinNumeric => Kind::MinNumeric(()),
                AggregateFunc::MaxInterval => Kind::MaxInterval(()),
                AggregateFunc::MaxTime => Kind::MaxTime(()),
                AggregateFunc::MinInt16 => Kind::MinInt16(()),
                AggregateFunc::MinInt32 => Kind::MinInt32(()),
                AggregateFunc::MinInt64 => Kind::MinInt64(()),
                AggregateFunc::MinUInt16 => Kind::MinUint16(()),
                AggregateFunc::MinUInt32 => Kind::MinUint32(()),
                AggregateFunc::MinUInt64 => Kind::MinUint64(()),
                AggregateFunc::MinMzTimestamp => Kind::MinMzTimestamp(()),
                AggregateFunc::MinFloat32 => Kind::MinFloat32(()),
                AggregateFunc::MinFloat64 => Kind::MinFloat64(()),
                AggregateFunc::MinBool => Kind::MinBool(()),
                AggregateFunc::MinString => Kind::MinString(()),
                AggregateFunc::MinDate => Kind::MinDate(()),
                AggregateFunc::MinTimestamp => Kind::MinTimestamp(()),
                AggregateFunc::MinTimestampTz => Kind::MinTimestampTz(()),
                AggregateFunc::MinInterval => Kind::MinInterval(()),
                AggregateFunc::MinTime => Kind::MinTime(()),
                AggregateFunc::SumInt16 => Kind::SumInt16(()),
                AggregateFunc::SumInt32 => Kind::SumInt32(()),
                AggregateFunc::SumInt64 => Kind::SumInt64(()),
                AggregateFunc::SumUInt16 => Kind::SumUint16(()),
                AggregateFunc::SumUInt32 => Kind::SumUint32(()),
                AggregateFunc::SumUInt64 => Kind::SumUint64(()),
                AggregateFunc::SumFloat32 => Kind::SumFloat32(()),
                AggregateFunc::SumFloat64 => Kind::SumFloat64(()),
                AggregateFunc::SumNumeric => Kind::SumNumeric(()),
                AggregateFunc::Count => Kind::Count(()),
                AggregateFunc::Any => Kind::Any(()),
                AggregateFunc::All => Kind::All(()),
                AggregateFunc::JsonbAgg { order_by } => Kind::JsonbAgg(order_by.into_proto()),
                AggregateFunc::JsonbObjectAgg { order_by } => {
                    Kind::JsonbObjectAgg(order_by.into_proto())
                }
                AggregateFunc::MapAgg {
                    order_by,
                    value_type,
                } => Kind::MapAgg(proto_aggregate_func::ProtoMapAgg {
                    order_by: Some(order_by.into_proto()),
                    value_type: Some(value_type.into_proto()),
                }),
                AggregateFunc::ArrayConcat { order_by } => Kind::ArrayConcat(order_by.into_proto()),
                AggregateFunc::ListConcat { order_by } => Kind::ListConcat(order_by.into_proto()),
                AggregateFunc::StringAgg { order_by } => Kind::StringAgg(order_by.into_proto()),
                AggregateFunc::RowNumber { order_by } => Kind::RowNumber(order_by.into_proto()),
                AggregateFunc::Rank { order_by } => Kind::Rank(order_by.into_proto()),
                AggregateFunc::DenseRank { order_by } => Kind::DenseRank(order_by.into_proto()),
                AggregateFunc::LagLead {
                    order_by,
                    lag_lead,
                    ignore_nulls,
                } => Kind::LagLead(proto_aggregate_func::ProtoLagLead {
                    order_by: Some(order_by.into_proto()),
                    lag_lead: Some(match lag_lead {
                        LagLeadType::Lag => proto_aggregate_func::proto_lag_lead::LagLead::Lag(()),
                        LagLeadType::Lead => {
                            proto_aggregate_func::proto_lag_lead::LagLead::Lead(())
                        }
                    }),
                    ignore_nulls: *ignore_nulls,
                }),
                AggregateFunc::FirstValue {
                    order_by,
                    window_frame,
                } => Kind::FirstValue(proto_aggregate_func::ProtoFramedWindowFunc {
                    order_by: Some(order_by.into_proto()),
                    window_frame: Some(window_frame.into_proto()),
                }),
                AggregateFunc::LastValue {
                    order_by,
                    window_frame,
                } => Kind::LastValue(proto_aggregate_func::ProtoFramedWindowFunc {
                    order_by: Some(order_by.into_proto()),
                    window_frame: Some(window_frame.into_proto()),
                }),
                AggregateFunc::WindowAggregate {
                    wrapped_aggregate,
                    order_by,
                    window_frame,
                } => Kind::WindowAggregate(Box::new(proto_aggregate_func::ProtoWindowAggregate {
                    wrapped_aggregate: Some(wrapped_aggregate.into_proto()),
                    order_by: Some(order_by.into_proto()),
                    window_frame: Some(window_frame.into_proto()),
                })),
                AggregateFunc::FusedValueWindowFunc { funcs, order_by } => {
                    Kind::FusedValueWindowFunc(ProtoFusedValueWindowFunc {
                        funcs: funcs.into_proto(),
                        order_by: Some(order_by.into_proto()),
                    })
                }
                AggregateFunc::FusedWindowAggregate {
                    wrapped_aggregates,
                    order_by,
                    window_frame,
                } => Kind::FusedWindowAggregate(ProtoFusedWindowAggregate {
                    wrapped_aggregates: wrapped_aggregates.into_proto(),
                    order_by: Some(order_by.into_proto()),
                    window_frame: Some(window_frame.into_proto()),
                }),
                AggregateFunc::Dummy => Kind::Dummy(()),
            }),
        }
    }

    fn from_proto(proto: ProtoAggregateFunc) -> Result<Self, TryFromProtoError> {
        use proto_aggregate_func::Kind;
        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoAggregateFunc::kind"))?;
        Ok(match kind {
            Kind::MaxNumeric(()) => AggregateFunc::MaxNumeric,
            Kind::MaxInt16(()) => AggregateFunc::MaxInt16,
            Kind::MaxInt32(()) => AggregateFunc::MaxInt32,
            Kind::MaxInt64(()) => AggregateFunc::MaxInt64,
            Kind::MaxUint16(()) => AggregateFunc::MaxUInt16,
            Kind::MaxUint32(()) => AggregateFunc::MaxUInt32,
            Kind::MaxUint64(()) => AggregateFunc::MaxUInt64,
            Kind::MaxMzTimestamp(()) => AggregateFunc::MaxMzTimestamp,
            Kind::MaxFloat32(()) => AggregateFunc::MaxFloat32,
            Kind::MaxFloat64(()) => AggregateFunc::MaxFloat64,
            Kind::MaxBool(()) => AggregateFunc::MaxBool,
            Kind::MaxString(()) => AggregateFunc::MaxString,
            Kind::MaxDate(()) => AggregateFunc::MaxDate,
            Kind::MaxTimestamp(()) => AggregateFunc::MaxTimestamp,
            Kind::MaxTimestampTz(()) => AggregateFunc::MaxTimestampTz,
            Kind::MaxInterval(()) => AggregateFunc::MaxInterval,
            Kind::MaxTime(()) => AggregateFunc::MaxTime,
            Kind::MinNumeric(()) => AggregateFunc::MinNumeric,
            Kind::MinInt16(()) => AggregateFunc::MinInt16,
            Kind::MinInt32(()) => AggregateFunc::MinInt32,
            Kind::MinInt64(()) => AggregateFunc::MinInt64,
            Kind::MinUint16(()) => AggregateFunc::MinUInt16,
            Kind::MinUint32(()) => AggregateFunc::MinUInt32,
            Kind::MinUint64(()) => AggregateFunc::MinUInt64,
            Kind::MinMzTimestamp(()) => AggregateFunc::MinMzTimestamp,
            Kind::MinFloat32(()) => AggregateFunc::MinFloat32,
            Kind::MinFloat64(()) => AggregateFunc::MinFloat64,
            Kind::MinBool(()) => AggregateFunc::MinBool,
            Kind::MinString(()) => AggregateFunc::MinString,
            Kind::MinDate(()) => AggregateFunc::MinDate,
            Kind::MinTimestamp(()) => AggregateFunc::MinTimestamp,
            Kind::MinTimestampTz(()) => AggregateFunc::MinTimestampTz,
            Kind::MinInterval(()) => AggregateFunc::MinInterval,
            Kind::MinTime(()) => AggregateFunc::MinTime,
            Kind::SumInt16(()) => AggregateFunc::SumInt16,
            Kind::SumInt32(()) => AggregateFunc::SumInt32,
            Kind::SumInt64(()) => AggregateFunc::SumInt64,
            Kind::SumUint16(()) => AggregateFunc::SumUInt16,
            Kind::SumUint32(()) => AggregateFunc::SumUInt32,
            Kind::SumUint64(()) => AggregateFunc::SumUInt64,
            Kind::SumFloat32(()) => AggregateFunc::SumFloat32,
            Kind::SumFloat64(()) => AggregateFunc::SumFloat64,
            Kind::SumNumeric(()) => AggregateFunc::SumNumeric,
            Kind::Count(()) => AggregateFunc::Count,
            Kind::Any(()) => AggregateFunc::Any,
            Kind::All(()) => AggregateFunc::All,
            Kind::JsonbAgg(order_by) => AggregateFunc::JsonbAgg {
                order_by: order_by.into_rust()?,
            },
            Kind::JsonbObjectAgg(order_by) => AggregateFunc::JsonbObjectAgg {
                order_by: order_by.into_rust()?,
            },
            Kind::MapAgg(pma) => AggregateFunc::MapAgg {
                order_by: pma.order_by.into_rust_if_some("ProtoMapAgg::order_by")?,
                value_type: pma
                    .value_type
                    .into_rust_if_some("ProtoMapAgg::value_type")?,
            },
            Kind::ArrayConcat(order_by) => AggregateFunc::ArrayConcat {
                order_by: order_by.into_rust()?,
            },
            Kind::ListConcat(order_by) => AggregateFunc::ListConcat {
                order_by: order_by.into_rust()?,
            },
            Kind::StringAgg(order_by) => AggregateFunc::StringAgg {
                order_by: order_by.into_rust()?,
            },
            Kind::RowNumber(order_by) => AggregateFunc::RowNumber {
                order_by: order_by.into_rust()?,
            },
            Kind::Rank(order_by) => AggregateFunc::Rank {
                order_by: order_by.into_rust()?,
            },
            Kind::DenseRank(order_by) => AggregateFunc::DenseRank {
                order_by: order_by.into_rust()?,
            },
            Kind::LagLead(pll) => AggregateFunc::LagLead {
                order_by: pll.order_by.into_rust_if_some("ProtoLagLead::order_by")?,
                lag_lead: match pll.lag_lead {
                    Some(proto_aggregate_func::proto_lag_lead::LagLead::Lag(())) => {
                        LagLeadType::Lag
                    }
                    Some(proto_aggregate_func::proto_lag_lead::LagLead::Lead(())) => {
                        LagLeadType::Lead
                    }
                    None => {
                        return Err(TryFromProtoError::MissingField(
                            "ProtoLagLead::lag_lead".into(),
                        ));
                    }
                },
                ignore_nulls: pll.ignore_nulls,
            },
            Kind::FirstValue(pfv) => AggregateFunc::FirstValue {
                order_by: pfv
                    .order_by
                    .into_rust_if_some("ProtoFramedWindowFunc::order_by")?,
                window_frame: pfv
                    .window_frame
                    .into_rust_if_some("ProtoFramedWindowFunc::window_frame")?,
            },
            Kind::LastValue(pfv) => AggregateFunc::LastValue {
                order_by: pfv
                    .order_by
                    .into_rust_if_some("ProtoFramedWindowFunc::order_by")?,
                window_frame: pfv
                    .window_frame
                    .into_rust_if_some("ProtoFramedWindowFunc::window_frame")?,
            },
            Kind::WindowAggregate(paf) => AggregateFunc::WindowAggregate {
                wrapped_aggregate: paf
                    .wrapped_aggregate
                    .into_rust_if_some("ProtoWindowAggregate::wrapped_aggregate")?,
                order_by: paf
                    .order_by
                    .into_rust_if_some("ProtoWindowAggregate::order_by")?,
                window_frame: paf
                    .window_frame
                    .into_rust_if_some("ProtoWindowAggregate::window_frame")?,
            },
            Kind::FusedValueWindowFunc(fvwf) => AggregateFunc::FusedValueWindowFunc {
                funcs: fvwf.funcs.into_rust()?,
                order_by: fvwf
                    .order_by
                    .into_rust_if_some("ProtoFusedValueWindowFunc::order_by")?,
            },
            Kind::FusedWindowAggregate(fwa) => AggregateFunc::FusedWindowAggregate {
                wrapped_aggregates: fwa.wrapped_aggregates.into_rust()?,
                order_by: fwa
                    .order_by
                    .into_rust_if_some("ProtoFusedWindowAggregate::order_by")?,
                window_frame: fwa
                    .window_frame
                    .into_rust_if_some("ProtoFusedWindowAggregate::window_frame")?,
            },
            Kind::Dummy(()) => AggregateFunc::Dummy,
        })
    }
}

impl AggregateFunc {
    pub fn eval<'a, I>(&self, datums: I, temp_storage: &'a RowArena) -> Datum<'a>
    where
        I: IntoIterator<Item = Datum<'a>>,
    {
        match self {
            AggregateFunc::MaxNumeric => {
                max_datum::<'a, I, OrderedDecimal<numeric::Numeric>>(datums)
            }
            AggregateFunc::MaxInt16 => max_datum::<'a, I, i16>(datums),
            AggregateFunc::MaxInt32 => max_datum::<'a, I, i32>(datums),
            AggregateFunc::MaxInt64 => max_datum::<'a, I, i64>(datums),
            AggregateFunc::MaxUInt16 => max_datum::<'a, I, u16>(datums),
            AggregateFunc::MaxUInt32 => max_datum::<'a, I, u32>(datums),
            AggregateFunc::MaxUInt64 => max_datum::<'a, I, u64>(datums),
            AggregateFunc::MaxMzTimestamp => max_datum::<'a, I, mz_repr::Timestamp>(datums),
            AggregateFunc::MaxFloat32 => max_datum::<'a, I, OrderedFloat<f32>>(datums),
            AggregateFunc::MaxFloat64 => max_datum::<'a, I, OrderedFloat<f64>>(datums),
            AggregateFunc::MaxBool => max_datum::<'a, I, bool>(datums),
            AggregateFunc::MaxString => max_string(datums),
            AggregateFunc::MaxDate => max_datum::<'a, I, Date>(datums),
            AggregateFunc::MaxTimestamp => {
                max_datum::<'a, I, CheckedTimestamp<NaiveDateTime>>(datums)
            }
            AggregateFunc::MaxTimestampTz => {
                max_datum::<'a, I, CheckedTimestamp<DateTime<Utc>>>(datums)
            }
            AggregateFunc::MaxInterval => max_datum::<'a, I, Interval>(datums),
            AggregateFunc::MaxTime => max_datum::<'a, I, NaiveTime>(datums),
            AggregateFunc::MinNumeric => {
                min_datum::<'a, I, OrderedDecimal<numeric::Numeric>>(datums)
            }
            AggregateFunc::MinInt16 => min_datum::<'a, I, i16>(datums),
            AggregateFunc::MinInt32 => min_datum::<'a, I, i32>(datums),
            AggregateFunc::MinInt64 => min_datum::<'a, I, i64>(datums),
            AggregateFunc::MinUInt16 => min_datum::<'a, I, u16>(datums),
            AggregateFunc::MinUInt32 => min_datum::<'a, I, u32>(datums),
            AggregateFunc::MinUInt64 => min_datum::<'a, I, u64>(datums),
            AggregateFunc::MinMzTimestamp => min_datum::<'a, I, mz_repr::Timestamp>(datums),
            AggregateFunc::MinFloat32 => min_datum::<'a, I, OrderedFloat<f32>>(datums),
            AggregateFunc::MinFloat64 => min_datum::<'a, I, OrderedFloat<f64>>(datums),
            AggregateFunc::MinBool => min_datum::<'a, I, bool>(datums),
            AggregateFunc::MinString => min_string(datums),
            AggregateFunc::MinDate => min_datum::<'a, I, Date>(datums),
            AggregateFunc::MinTimestamp => {
                min_datum::<'a, I, CheckedTimestamp<NaiveDateTime>>(datums)
            }
            AggregateFunc::MinTimestampTz => {
                min_datum::<'a, I, CheckedTimestamp<DateTime<Utc>>>(datums)
            }
            AggregateFunc::MinInterval => min_datum::<'a, I, Interval>(datums),
            AggregateFunc::MinTime => min_datum::<'a, I, NaiveTime>(datums),
            AggregateFunc::SumInt16 => sum_datum::<'a, I, i16, i64>(datums),
            AggregateFunc::SumInt32 => sum_datum::<'a, I, i32, i64>(datums),
            AggregateFunc::SumInt64 => sum_datum::<'a, I, i64, i128>(datums),
            AggregateFunc::SumUInt16 => sum_datum::<'a, I, u16, u64>(datums),
            AggregateFunc::SumUInt32 => sum_datum::<'a, I, u32, u64>(datums),
            AggregateFunc::SumUInt64 => sum_datum::<'a, I, u64, u128>(datums),
            AggregateFunc::SumFloat32 => sum_datum::<'a, I, f32, f32>(datums),
            AggregateFunc::SumFloat64 => sum_datum::<'a, I, f64, f64>(datums),
            AggregateFunc::SumNumeric => sum_numeric(datums),
            AggregateFunc::Count => count(datums),
            AggregateFunc::Any => any(datums),
            AggregateFunc::All => all(datums),
            AggregateFunc::JsonbAgg { order_by } => jsonb_agg(datums, temp_storage, order_by),
            AggregateFunc::MapAgg { order_by, .. } | AggregateFunc::JsonbObjectAgg { order_by } => {
                dict_agg(datums, temp_storage, order_by)
            }
            AggregateFunc::ArrayConcat { order_by } => array_concat(datums, temp_storage, order_by),
            AggregateFunc::ListConcat { order_by } => list_concat(datums, temp_storage, order_by),
            AggregateFunc::StringAgg { order_by } => string_agg(datums, temp_storage, order_by),
            AggregateFunc::RowNumber { order_by } => row_number(datums, temp_storage, order_by),
            AggregateFunc::Rank { order_by } => rank(datums, temp_storage, order_by),
            AggregateFunc::DenseRank { order_by } => dense_rank(datums, temp_storage, order_by),
            AggregateFunc::LagLead {
                order_by,
                lag_lead: lag_lead_type,
                ignore_nulls,
            } => lag_lead(datums, temp_storage, order_by, lag_lead_type, ignore_nulls),
            AggregateFunc::FirstValue {
                order_by,
                window_frame,
            } => first_value(datums, temp_storage, order_by, window_frame),
            AggregateFunc::LastValue {
                order_by,
                window_frame,
            } => last_value(datums, temp_storage, order_by, window_frame),
            AggregateFunc::WindowAggregate {
                wrapped_aggregate,
                order_by,
                window_frame,
            } => window_aggr::<_, NaiveOneByOneAggr>(
                datums,
                temp_storage,
                wrapped_aggregate,
                order_by,
                window_frame,
            ),
            AggregateFunc::FusedValueWindowFunc { funcs, order_by } => {
                fused_value_window_func(datums, temp_storage, funcs, order_by)
            }
            AggregateFunc::FusedWindowAggregate {
                wrapped_aggregates,
                order_by,
                window_frame,
            } => fused_window_aggr::<_, NaiveOneByOneAggr>(
                datums,
                temp_storage,
                wrapped_aggregates,
                order_by,
                window_frame,
            ),
            AggregateFunc::Dummy => Datum::Dummy,
        }
    }

    /// Like `eval`, but it's given a [OneByOneAggr]. If `self` is a `WindowAggregate`, then
    /// the given [OneByOneAggr] will be used to evaluate the wrapped aggregate inside the
    /// `WindowAggregate`. If `self` is not a `WindowAggregate`, then it simply calls `eval`.
    pub fn eval_with_fast_window_agg<'a, I, W>(
        &self,
        datums: I,
        temp_storage: &'a RowArena,
    ) -> Datum<'a>
    where
        I: IntoIterator<Item = Datum<'a>>,
        W: OneByOneAggr,
    {
        match self {
            AggregateFunc::WindowAggregate {
                wrapped_aggregate,
                order_by,
                window_frame,
            } => window_aggr::<_, W>(
                datums,
                temp_storage,
                wrapped_aggregate,
                order_by,
                window_frame,
            ),
            AggregateFunc::FusedWindowAggregate {
                wrapped_aggregates,
                order_by,
                window_frame,
            } => fused_window_aggr::<_, W>(
                datums,
                temp_storage,
                wrapped_aggregates,
                order_by,
                window_frame,
            ),
            _ => self.eval(datums, temp_storage),
        }
    }

    pub fn eval_with_unnest_list<'a, I, W>(
        &self,
        datums: I,
        temp_storage: &'a RowArena,
    ) -> impl Iterator<Item = Datum<'a>>
    where
        I: IntoIterator<Item = Datum<'a>>,
        W: OneByOneAggr,
    {
        // TODO: Use `enum_dispatch` to construct a unified iterator instead of `collect_vec`.
        assert!(self.can_fuse_with_unnest_list());
        match self {
            AggregateFunc::RowNumber { order_by } => {
                row_number_no_list(datums, temp_storage, order_by).collect_vec()
            }
            AggregateFunc::Rank { order_by } => {
                rank_no_list(datums, temp_storage, order_by).collect_vec()
            }
            AggregateFunc::DenseRank { order_by } => {
                dense_rank_no_list(datums, temp_storage, order_by).collect_vec()
            }
            AggregateFunc::LagLead {
                order_by,
                lag_lead: lag_lead_type,
                ignore_nulls,
            } => lag_lead_no_list(datums, temp_storage, order_by, lag_lead_type, ignore_nulls)
                .collect_vec(),
            AggregateFunc::FirstValue {
                order_by,
                window_frame,
            } => first_value_no_list(datums, temp_storage, order_by, window_frame).collect_vec(),
            AggregateFunc::LastValue {
                order_by,
                window_frame,
            } => last_value_no_list(datums, temp_storage, order_by, window_frame).collect_vec(),
            AggregateFunc::FusedValueWindowFunc { funcs, order_by } => {
                fused_value_window_func_no_list(datums, temp_storage, funcs, order_by).collect_vec()
            }
            AggregateFunc::WindowAggregate {
                wrapped_aggregate,
                order_by,
                window_frame,
            } => window_aggr_no_list::<_, W>(
                datums,
                temp_storage,
                wrapped_aggregate,
                order_by,
                window_frame,
            )
            .collect_vec(),
            AggregateFunc::FusedWindowAggregate {
                wrapped_aggregates,
                order_by,
                window_frame,
            } => fused_window_aggr_no_list::<_, W>(
                datums,
                temp_storage,
                wrapped_aggregates,
                order_by,
                window_frame,
            )
            .collect_vec(),
            _ => unreachable!("asserted above that `can_fuse_with_unnest_list`"),
        }
        .into_iter()
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
            AggregateFunc::ArrayConcat { .. } => Datum::empty_array(),
            AggregateFunc::ListConcat { .. } => Datum::empty_list(),
            AggregateFunc::RowNumber { .. }
            | AggregateFunc::Rank { .. }
            | AggregateFunc::DenseRank { .. }
            | AggregateFunc::LagLead { .. }
            | AggregateFunc::FirstValue { .. }
            | AggregateFunc::LastValue { .. }
            | AggregateFunc::WindowAggregate { .. }
            | AggregateFunc::FusedValueWindowFunc { .. }
            | AggregateFunc::FusedWindowAggregate { .. } => Datum::empty_list(),
            AggregateFunc::MaxNumeric
            | AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxUInt16
            | AggregateFunc::MaxUInt32
            | AggregateFunc::MaxUInt64
            | AggregateFunc::MaxMzTimestamp
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxBool
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz
            | AggregateFunc::MaxInterval
            | AggregateFunc::MaxTime
            | AggregateFunc::MinNumeric
            | AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinUInt16
            | AggregateFunc::MinUInt32
            | AggregateFunc::MinUInt64
            | AggregateFunc::MinMzTimestamp
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz
            | AggregateFunc::MinInterval
            | AggregateFunc::MinTime
            | AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64
            | AggregateFunc::SumFloat32
            | AggregateFunc::SumFloat64
            | AggregateFunc::SumNumeric
            | AggregateFunc::Count
            | AggregateFunc::JsonbAgg { .. }
            | AggregateFunc::JsonbObjectAgg { .. }
            | AggregateFunc::MapAgg { .. }
            | AggregateFunc::StringAgg { .. } => Datum::Null,
        }
    }

    pub fn can_fuse_with_unnest_list(&self) -> bool {
        match self {
            AggregateFunc::RowNumber { .. }
            | AggregateFunc::Rank { .. }
            | AggregateFunc::DenseRank { .. }
            | AggregateFunc::LagLead { .. }
            | AggregateFunc::FirstValue { .. }
            | AggregateFunc::LastValue { .. }
            | AggregateFunc::WindowAggregate { .. }
            | AggregateFunc::FusedValueWindowFunc { .. }
            | AggregateFunc::FusedWindowAggregate { .. } => true,
            AggregateFunc::ArrayConcat { .. }
            | AggregateFunc::ListConcat { .. }
            | AggregateFunc::Any
            | AggregateFunc::All
            | AggregateFunc::Dummy
            | AggregateFunc::MaxNumeric
            | AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxUInt16
            | AggregateFunc::MaxUInt32
            | AggregateFunc::MaxUInt64
            | AggregateFunc::MaxMzTimestamp
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxBool
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz
            | AggregateFunc::MaxInterval
            | AggregateFunc::MaxTime
            | AggregateFunc::MinNumeric
            | AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinUInt16
            | AggregateFunc::MinUInt32
            | AggregateFunc::MinUInt64
            | AggregateFunc::MinMzTimestamp
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz
            | AggregateFunc::MinInterval
            | AggregateFunc::MinTime
            | AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64
            | AggregateFunc::SumFloat32
            | AggregateFunc::SumFloat64
            | AggregateFunc::SumNumeric
            | AggregateFunc::Count
            | AggregateFunc::JsonbAgg { .. }
            | AggregateFunc::JsonbObjectAgg { .. }
            | AggregateFunc::MapAgg { .. }
            | AggregateFunc::StringAgg { .. } => false,
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
            AggregateFunc::JsonbAgg { .. } => ScalarType::Jsonb,
            AggregateFunc::JsonbObjectAgg { .. } => ScalarType::Jsonb,
            AggregateFunc::SumInt16 => ScalarType::Int64,
            AggregateFunc::SumInt32 => ScalarType::Int64,
            AggregateFunc::SumInt64 => ScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            },
            AggregateFunc::SumUInt16 => ScalarType::UInt64,
            AggregateFunc::SumUInt32 => ScalarType::UInt64,
            AggregateFunc::SumUInt64 => ScalarType::Numeric {
                max_scale: Some(NumericMaxScale::ZERO),
            },
            AggregateFunc::MapAgg { value_type, .. } => ScalarType::Map {
                value_type: Box::new(value_type.clone()),
                custom_id: None,
            },
            AggregateFunc::ArrayConcat { .. } | AggregateFunc::ListConcat { .. } => {
                match input_type.scalar_type {
                    // The input is wrapped in a Record if there's an ORDER BY, so extract it out.
                    ScalarType::Record { ref fields, .. } => fields[0].1.scalar_type.clone(),
                    _ => unreachable!(),
                }
            }
            AggregateFunc::StringAgg { .. } => ScalarType::String,
            AggregateFunc::RowNumber { .. } => {
                AggregateFunc::output_type_ranking_window_funcs(&input_type, "?row_number?")
            }
            AggregateFunc::Rank { .. } => {
                AggregateFunc::output_type_ranking_window_funcs(&input_type, "?rank?")
            }
            AggregateFunc::DenseRank { .. } => {
                AggregateFunc::output_type_ranking_window_funcs(&input_type, "?dense_rank?")
            }
            AggregateFunc::LagLead { lag_lead: lag_lead_type, .. } => {
                // The input type for Lag is ((OriginalRow, EncodedArgs), OrderByExprs...)
                let fields = input_type.scalar_type.unwrap_record_element_type();
                let original_row_type = fields[0].unwrap_record_element_type()[0]
                    .clone()
                    .nullable(false);
                let output_type_inner = Self::lag_lead_output_type_inner_from_encoded_args(fields[0].unwrap_record_element_type()[1]);
                let column_name = Self::lag_lead_result_column_name(lag_lead_type);

                ScalarType::List {
                    element_type: Box::new(ScalarType::Record {
                        fields: [
                            (column_name, output_type_inner),
                            (ColumnName::from("?orig_row?"), original_row_type),
                        ].into(),
                        custom_id: None,
                    }),
                    custom_id: None,
                }
            }
            AggregateFunc::FirstValue { .. } => {
                // The input type for FirstValue is ((OriginalRow, Arg), OrderByExprs...)
                let fields = input_type.scalar_type.unwrap_record_element_type();
                let original_row_type = fields[0].unwrap_record_element_type()[0]
                    .clone()
                    .nullable(false);
                let value_type = fields[0].unwrap_record_element_type()[1]
                    .clone()
                    .nullable(true); // null when the partition is empty

                ScalarType::List {
                    element_type: Box::new(ScalarType::Record {
                        fields: [
                            (ColumnName::from("?first_value?"), value_type),
                            (ColumnName::from("?orig_row?"), original_row_type),
                        ].into(),
                        custom_id: None,
                    }),
                    custom_id: None,
                }
            }
            AggregateFunc::LastValue { .. } => {
                // The input type for LastValue is ((OriginalRow, Arg), OrderByExprs...)
                let fields = input_type.scalar_type.unwrap_record_element_type();
                let original_row_type = fields[0].unwrap_record_element_type()[0]
                    .clone()
                    .nullable(false);
                let value_type = fields[0].unwrap_record_element_type()[1]
                    .clone()
                    .nullable(true); // null when the partition is empty

                ScalarType::List {
                    element_type: Box::new(ScalarType::Record {
                        fields: [
                            (ColumnName::from("?last_value?"), value_type),
                            (ColumnName::from("?orig_row?"), original_row_type),
                        ].into(),
                        custom_id: None,
                    }),
                    custom_id: None,
                }
            }
            AggregateFunc::WindowAggregate {
                wrapped_aggregate, ..
            } => {
                // The input type for a window aggregate is ((OriginalRow, Arg), OrderByExprs...)
                let fields = input_type.scalar_type.unwrap_record_element_type();
                let original_row_type = fields[0].unwrap_record_element_type()[0]
                    .clone()
                    .nullable(false);
                let arg_type = fields[0].unwrap_record_element_type()[1]
                    .clone()
                    .nullable(true);
                let wrapped_aggr_out_type = wrapped_aggregate.output_type(arg_type);

                ScalarType::List {
                    element_type: Box::new(ScalarType::Record {
                        fields: [
                            (ColumnName::from("?window_agg?"), wrapped_aggr_out_type),
                            (ColumnName::from("?orig_row?"), original_row_type),
                        ].into(),
                        custom_id: None,
                    }),
                    custom_id: None,
                }
            }
            AggregateFunc::FusedWindowAggregate {
                wrapped_aggregates, ..
            } => {
                // The input type for a fused window aggregate is ((OriginalRow, Args), OrderByExprs...)
                // where `Args` is a record.
                let fields = input_type.scalar_type.unwrap_record_element_type();
                let original_row_type = fields[0].unwrap_record_element_type()[0]
                    .clone()
                    .nullable(false);
                let args_type = fields[0].unwrap_record_element_type()[1];
                let arg_types = args_type.unwrap_record_element_type();
                let out_fields = arg_types.iter().zip_eq(wrapped_aggregates).map(|(arg_type, wrapped_agg)| {
                    (
                        ColumnName::from(wrapped_agg.name()),
                        wrapped_agg.output_type((**arg_type).clone().nullable(true)),
                    )
                }).collect_vec();

                ScalarType::List {
                    element_type: Box::new(ScalarType::Record {
                        fields: [
                            (ColumnName::from("?fused_window_agg?"), ScalarType::Record {
                                fields: out_fields.into(),
                                custom_id: None,
                            }.nullable(false)),
                            (ColumnName::from("?orig_row?"), original_row_type),
                        ].into(),
                        custom_id: None,
                    }),
                    custom_id: None,
                }
            }
            AggregateFunc::FusedValueWindowFunc { funcs, order_by: _ } => {
                // The input type is ((OriginalRow, EncodedArgs), OrderByExprs...)
                // where EncodedArgs is a record, where each element is the argument to one of the
                // function calls that got fused. This is a record for lag/lead, and a simple type
                // for first_value/last_value.
                let fields = input_type.scalar_type.unwrap_record_element_type();
                let original_row_type = fields[0].unwrap_record_element_type()[0]
                    .clone()
                    .nullable(false);
                let encoded_args_type = fields[0].unwrap_record_element_type()[1].unwrap_record_element_type();

                ScalarType::List {
                    element_type: Box::new(ScalarType::Record {
                        fields: [
                            (ColumnName::from("?fused_value_window_func?"), ScalarType::Record {
                                fields: encoded_args_type.into_iter().zip_eq(funcs).map(|(arg_type, func)| {
                                    match func {
                                        AggregateFunc::LagLead { lag_lead: lag_lead_type, .. } => {
                                            (
                                                Self::lag_lead_result_column_name(lag_lead_type),
                                                Self::lag_lead_output_type_inner_from_encoded_args(arg_type)
                                            )
                                        },
                                        AggregateFunc::FirstValue { .. } => {
                                            (
                                                ColumnName::from("?first_value?"),
                                                arg_type.clone().nullable(true),
                                            )
                                        }
                                        AggregateFunc::LastValue { .. } => {
                                            (
                                                ColumnName::from("?last_value?"),
                                                arg_type.clone().nullable(true),
                                            )
                                        }
                                        _ => panic!("FusedValueWindowFunc has an unknown function"),
                                    }
                                }).collect(),
                                custom_id: None,
                            }.nullable(false)),
                            (ColumnName::from("?orig_row?"), original_row_type),
                        ].into(),
                        custom_id: None,
                    }),
                    custom_id: None,
                }
            }
            AggregateFunc::Dummy
            | AggregateFunc::MaxNumeric
            | AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxUInt16
            | AggregateFunc::MaxUInt32
            | AggregateFunc::MaxUInt64
            | AggregateFunc::MaxMzTimestamp
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxBool
            // Note AggregateFunc::MaxString, MinString rely on returning input
            // type as output type to support the proper return type for
            // character input.
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz
            | AggregateFunc::MaxInterval
            | AggregateFunc::MaxTime
            | AggregateFunc::MinNumeric
            | AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinUInt16
            | AggregateFunc::MinUInt32
            | AggregateFunc::MinUInt64
            | AggregateFunc::MinMzTimestamp
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz
            | AggregateFunc::MinInterval
            | AggregateFunc::MinTime
            | AggregateFunc::SumFloat32
            | AggregateFunc::SumFloat64
            | AggregateFunc::SumNumeric => input_type.scalar_type.clone(),
        };
        // Count never produces null, and other aggregations only produce
        // null in the presence of null inputs.
        let nullable = match self {
            AggregateFunc::Count => false,
            // Use the nullability of the underlying column being aggregated, not the Records wrapping it
            AggregateFunc::StringAgg { .. } => match input_type.scalar_type {
                // The outer Record wraps the input in the first position, and any ORDER BY expressions afterwards
                ScalarType::Record { fields, .. } => match &fields[0].1.scalar_type {
                    // The inner Record is a (value, separator) tuple
                    ScalarType::Record { fields, .. } => fields[0].1.nullable,
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            },
            _ => input_type.nullable,
        };
        scalar_type.nullable(nullable)
    }

    /// Compute output type for ROW_NUMBER, RANK, DENSE_RANK
    fn output_type_ranking_window_funcs(input_type: &ColumnType, col_name: &str) -> ScalarType {
        match input_type.scalar_type {
            ScalarType::Record { ref fields, .. } => ScalarType::List {
                element_type: Box::new(ScalarType::Record {
                    fields: [
                        (
                            ColumnName::from(col_name),
                            ScalarType::Int64.nullable(false),
                        ),
                        (ColumnName::from("?orig_row?"), {
                            let inner = match &fields[0].1.scalar_type {
                                ScalarType::List { element_type, .. } => element_type.clone(),
                                _ => unreachable!(),
                            };
                            inner.nullable(false)
                        }),
                    ]
                    .into(),
                    custom_id: None,
                }),
                custom_id: None,
            },
            _ => unreachable!(),
        }
    }

    /// Given the `EncodedArgs` part of `((OriginalRow, EncodedArgs), OrderByExprs...)`,
    /// this computes the type of the first field of the output type. (The first field is the
    /// real result, the rest is the original row.)
    fn lag_lead_output_type_inner_from_encoded_args(encoded_args_type: &ScalarType) -> ColumnType {
        // lag/lead have 3 arguments, and the output type is
        // the same as the first of these, but always nullable. (It's null when the
        // lag/lead computation reaches over the bounds of the window partition.)
        encoded_args_type.unwrap_record_element_type()[0]
            .clone()
            .nullable(true)
    }

    fn lag_lead_result_column_name(lag_lead_type: &LagLeadType) -> ColumnName {
        ColumnName::from(match lag_lead_type {
            LagLeadType::Lag => "?lag?",
            LagLeadType::Lead => "?lead?",
        })
    }

    /// Returns true if the non-null constraint on the aggregation can be
    /// converted into a non-null constraint on its parameter expression, ie.
    /// whether the result of the aggregation is null if all the input values
    /// are null.
    pub fn propagates_nonnull_constraint(&self) -> bool {
        match self {
            AggregateFunc::MaxNumeric
            | AggregateFunc::MaxInt16
            | AggregateFunc::MaxInt32
            | AggregateFunc::MaxInt64
            | AggregateFunc::MaxUInt16
            | AggregateFunc::MaxUInt32
            | AggregateFunc::MaxUInt64
            | AggregateFunc::MaxMzTimestamp
            | AggregateFunc::MaxFloat32
            | AggregateFunc::MaxFloat64
            | AggregateFunc::MaxBool
            | AggregateFunc::MaxString
            | AggregateFunc::MaxDate
            | AggregateFunc::MaxTimestamp
            | AggregateFunc::MaxTimestampTz
            | AggregateFunc::MinNumeric
            | AggregateFunc::MinInt16
            | AggregateFunc::MinInt32
            | AggregateFunc::MinInt64
            | AggregateFunc::MinUInt16
            | AggregateFunc::MinUInt32
            | AggregateFunc::MinUInt64
            | AggregateFunc::MinMzTimestamp
            | AggregateFunc::MinFloat32
            | AggregateFunc::MinFloat64
            | AggregateFunc::MinBool
            | AggregateFunc::MinString
            | AggregateFunc::MinDate
            | AggregateFunc::MinTimestamp
            | AggregateFunc::MinTimestampTz
            | AggregateFunc::SumInt16
            | AggregateFunc::SumInt32
            | AggregateFunc::SumInt64
            | AggregateFunc::SumUInt16
            | AggregateFunc::SumUInt32
            | AggregateFunc::SumUInt64
            | AggregateFunc::SumFloat32
            | AggregateFunc::SumFloat64
            | AggregateFunc::SumNumeric
            | AggregateFunc::StringAgg { .. } => true,
            // Count is never null
            AggregateFunc::Count => false,
            _ => false,
        }
    }
}

fn jsonb_each<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
    stringify: bool,
) -> impl Iterator<Item = (Row, Diff)> + 'a {
    // First produce a map, so that a common iterator can be returned.
    let map = match a {
        Datum::Map(dict) => dict,
        _ => mz_repr::DatumMap::empty(),
    };

    map.iter().map(move |(k, mut v)| {
        if stringify {
            v = jsonb_stringify(v, temp_storage);
        }
        (Row::pack_slice(&[Datum::String(k), v]), Diff::ONE)
    })
}

fn jsonb_object_keys<'a>(a: Datum<'a>) -> impl Iterator<Item = (Row, Diff)> + 'a {
    let map = match a {
        Datum::Map(dict) => dict,
        _ => mz_repr::DatumMap::empty(),
    };

    map.iter()
        .map(move |(k, _)| (Row::pack_slice(&[Datum::String(k)]), Diff::ONE))
}

fn jsonb_array_elements<'a>(
    a: Datum<'a>,
    temp_storage: &'a RowArena,
    stringify: bool,
) -> impl Iterator<Item = (Row, Diff)> + 'a {
    let list = match a {
        Datum::List(list) => list,
        _ => mz_repr::DatumList::empty(),
    };
    list.iter().map(move |mut e| {
        if stringify {
            e = jsonb_stringify(e, temp_storage);
        }
        (Row::pack_slice(&[e]), Diff::ONE)
    })
}

fn regexp_extract(a: Datum, r: &AnalyzedRegex) -> Option<(Row, Diff)> {
    let r = r.inner();
    let a = a.unwrap_str();
    let captures = r.captures(a)?;
    let datums = captures
        .iter()
        .skip(1)
        .map(|m| Datum::from(m.map(|m| m.as_str())));
    Some((Row::pack(datums), Diff::ONE))
}

fn regexp_matches<'a, 'r: 'a>(
    exprs: &[Datum<'a>],
) -> Result<impl Iterator<Item = (Row, Diff)> + 'a, EvalError> {
    // There are only two acceptable ways to call this function:
    // 1. regexp_matches(string, regex)
    // 2. regexp_matches(string, regex, flag)
    assert!(exprs.len() == 2 || exprs.len() == 3);
    let a = exprs[0].unwrap_str();
    let r = exprs[1].unwrap_str();

    let (regex, opts) = if exprs.len() == 3 {
        let flag = exprs[2].unwrap_str();
        let opts = AnalyzedRegexOpts::from_str(flag)?;
        (AnalyzedRegex::new(r, opts)?, opts)
    } else {
        let opts = AnalyzedRegexOpts::default();
        (AnalyzedRegex::new(r, opts)?, opts)
    };

    let regex = regex.inner().clone();

    let iter = regex.captures_iter(a).map(move |captures| {
        let matches = captures
            .iter()
            // The first match is the *entire* match, we want the capture groups by themselves.
            .skip(1)
            .map(|m| Datum::from(m.map(|m| m.as_str())))
            .collect::<Vec<_>>();

        let mut binding = SharedRow::get();
        let mut packer = binding.packer();

        let dimension = ArrayDimension {
            lower_bound: 1,
            length: matches.len(),
        };
        packer
            .try_push_array(&[dimension], matches)
            .expect("generated dimensions above");

        (binding.clone(), Diff::ONE)
    });

    // This is slightly unfortunate, but we need to collect the captures into a
    // Vec before we can yield them, because we can't return a iter with a
    // reference to the local `regex` variable.
    // We attempt to minimize the cost of this by using a SmallVec.
    let out = iter.collect::<SmallVec<[_; 3]>>();

    if opts.global {
        Ok(Either::Left(out.into_iter()))
    } else {
        Ok(Either::Right(out.into_iter().take(1)))
    }
}

fn generate_series<N>(
    start: N,
    stop: N,
    step: N,
) -> Result<impl Iterator<Item = (Row, Diff)>, EvalError>
where
    N: Integer + Signed + CheckedAdd + Clone,
    Datum<'static>: From<N>,
{
    if step == N::zero() {
        return Err(EvalError::InvalidParameterValue(
            "step size cannot equal zero".into(),
        ));
    }
    Ok(num::range_step_inclusive(start, stop, step)
        .map(move |i| (Row::pack_slice(&[Datum::from(i)]), Diff::ONE)))
}

/// Like
/// [`num::range_step_inclusive`](https://github.com/rust-num/num-iter/blob/ddb14c1e796d401014c6c7a727de61d8109ad986/src/lib.rs#L279),
/// but for our timestamp types using [`Interval`] for `step`.xwxw
#[derive(Clone)]
pub struct TimestampRangeStepInclusive<T> {
    state: CheckedTimestamp<T>,
    stop: CheckedTimestamp<T>,
    step: Interval,
    rev: bool,
    done: bool,
}

impl<T: TimestampLike> Iterator for TimestampRangeStepInclusive<T> {
    type Item = CheckedTimestamp<T>;

    #[inline]
    fn next(&mut self) -> Option<CheckedTimestamp<T>> {
        if !self.done
            && ((self.rev && self.state >= self.stop) || (!self.rev && self.state <= self.stop))
        {
            let result = self.state.clone();
            match add_timestamp_months(self.state.deref(), self.step.months) {
                Ok(state) => match state.checked_add_signed(self.step.duration_as_chrono()) {
                    Some(v) => match CheckedTimestamp::from_timestamplike(v) {
                        Ok(v) => self.state = v,
                        Err(_) => self.done = true,
                    },
                    None => self.done = true,
                },
                Err(..) => {
                    self.done = true;
                }
            }

            Some(result)
        } else {
            None
        }
    }
}

fn generate_series_ts<T: TimestampLike>(
    start: CheckedTimestamp<T>,
    stop: CheckedTimestamp<T>,
    step: Interval,
    conv: fn(CheckedTimestamp<T>) -> Datum<'static>,
) -> Result<impl Iterator<Item = (Row, Diff)>, EvalError> {
    let normalized_step = step.as_microseconds();
    if normalized_step == 0 {
        return Err(EvalError::InvalidParameterValue(
            "step size cannot equal zero".into(),
        ));
    }
    let rev = normalized_step < 0;

    let trsi = TimestampRangeStepInclusive {
        state: start,
        stop,
        step,
        rev,
        done: false,
    };

    Ok(trsi.map(move |i| (Row::pack_slice(&[conv(i)]), Diff::ONE)))
}

fn generate_subscripts_array(
    a: Datum,
    dim: i32,
) -> Result<Box<dyn Iterator<Item = (Row, Diff)>>, EvalError> {
    if dim <= 0 {
        return Ok(Box::new(iter::empty()));
    }

    match a.unwrap_array().dims().into_iter().nth(
        (dim - 1)
            .try_into()
            .map_err(|_| EvalError::Int32OutOfRange((dim - 1).to_string().into()))?,
    ) {
        Some(requested_dim) => Ok(Box::new(generate_series::<i32>(
            requested_dim.lower_bound.try_into().map_err(|_| {
                EvalError::Int32OutOfRange(requested_dim.lower_bound.to_string().into())
            })?,
            requested_dim
                .length
                .try_into()
                .map_err(|_| EvalError::Int32OutOfRange(requested_dim.length.to_string().into()))?,
            1,
        )?)),
        None => Ok(Box::new(iter::empty())),
    }
}

fn unnest_array<'a>(a: Datum<'a>) -> impl Iterator<Item = (Row, Diff)> + 'a {
    a.unwrap_array()
        .elements()
        .iter()
        .map(move |e| (Row::pack_slice(&[e]), Diff::ONE))
}

fn unnest_list<'a>(a: Datum<'a>) -> impl Iterator<Item = (Row, Diff)> + 'a {
    a.unwrap_list()
        .iter()
        .map(move |e| (Row::pack_slice(&[e]), Diff::ONE))
}

fn unnest_map<'a>(a: Datum<'a>) -> impl Iterator<Item = (Row, Diff)> + 'a {
    a.unwrap_map()
        .iter()
        .map(move |(k, v)| (Row::pack_slice(&[Datum::from(k), v]), Diff::ONE))
}

impl AggregateFunc {
    /// The base function name without the `~[...]` suffix used when rendering
    /// variants that represent a parameterized function family.
    pub fn name(&self) -> &'static str {
        match self {
            Self::MaxNumeric => "max",
            Self::MaxInt16 => "max",
            Self::MaxInt32 => "max",
            Self::MaxInt64 => "max",
            Self::MaxUInt16 => "max",
            Self::MaxUInt32 => "max",
            Self::MaxUInt64 => "max",
            Self::MaxMzTimestamp => "max",
            Self::MaxFloat32 => "max",
            Self::MaxFloat64 => "max",
            Self::MaxBool => "max",
            Self::MaxString => "max",
            Self::MaxDate => "max",
            Self::MaxTimestamp => "max",
            Self::MaxTimestampTz => "max",
            Self::MaxInterval => "max",
            Self::MaxTime => "max",
            Self::MinNumeric => "min",
            Self::MinInt16 => "min",
            Self::MinInt32 => "min",
            Self::MinInt64 => "min",
            Self::MinUInt16 => "min",
            Self::MinUInt32 => "min",
            Self::MinUInt64 => "min",
            Self::MinMzTimestamp => "min",
            Self::MinFloat32 => "min",
            Self::MinFloat64 => "min",
            Self::MinBool => "min",
            Self::MinString => "min",
            Self::MinDate => "min",
            Self::MinTimestamp => "min",
            Self::MinTimestampTz => "min",
            Self::MinInterval => "min",
            Self::MinTime => "min",
            Self::SumInt16 => "sum",
            Self::SumInt32 => "sum",
            Self::SumInt64 => "sum",
            Self::SumUInt16 => "sum",
            Self::SumUInt32 => "sum",
            Self::SumUInt64 => "sum",
            Self::SumFloat32 => "sum",
            Self::SumFloat64 => "sum",
            Self::SumNumeric => "sum",
            Self::Count => "count",
            Self::Any => "any",
            Self::All => "all",
            Self::JsonbAgg { .. } => "jsonb_agg",
            Self::JsonbObjectAgg { .. } => "jsonb_object_agg",
            Self::MapAgg { .. } => "map_agg",
            Self::ArrayConcat { .. } => "array_agg",
            Self::ListConcat { .. } => "list_agg",
            Self::StringAgg { .. } => "string_agg",
            Self::RowNumber { .. } => "row_number",
            Self::Rank { .. } => "rank",
            Self::DenseRank { .. } => "dense_rank",
            Self::LagLead {
                lag_lead: LagLeadType::Lag,
                ..
            } => "lag",
            Self::LagLead {
                lag_lead: LagLeadType::Lead,
                ..
            } => "lead",
            Self::FirstValue { .. } => "first_value",
            Self::LastValue { .. } => "last_value",
            Self::WindowAggregate { .. } => "window_agg",
            Self::FusedValueWindowFunc { .. } => "fused_value_window_func",
            Self::FusedWindowAggregate { .. } => "fused_window_agg",
            Self::Dummy => "dummy",
        }
    }
}

impl<'a, M> fmt::Display for HumanizedExpr<'a, AggregateFunc, M>
where
    M: HumanizerMode,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use AggregateFunc::*;
        let name = self.expr.name();
        match self.expr {
            JsonbAgg { order_by }
            | JsonbObjectAgg { order_by }
            | MapAgg { order_by, .. }
            | ArrayConcat { order_by }
            | ListConcat { order_by }
            | StringAgg { order_by }
            | RowNumber { order_by }
            | Rank { order_by }
            | DenseRank { order_by } => {
                let order_by = order_by.iter().map(|col| self.child(col));
                write!(f, "{}[order_by=[{}]]", name, separated(", ", order_by))
            }
            LagLead {
                lag_lead: _,
                ignore_nulls,
                order_by,
            } => {
                let order_by = order_by.iter().map(|col| self.child(col));
                f.write_str(name)?;
                f.write_str("[")?;
                if *ignore_nulls {
                    f.write_str("ignore_nulls=true, ")?;
                }
                write!(f, "order_by=[{}]", separated(", ", order_by))?;
                f.write_str("]")
            }
            FirstValue {
                order_by,
                window_frame,
            } => {
                let order_by = order_by.iter().map(|col| self.child(col));
                f.write_str(name)?;
                f.write_str("[")?;
                write!(f, "order_by=[{}]", separated(", ", order_by))?;
                if *window_frame != WindowFrame::default() {
                    write!(f, " {}", window_frame)?;
                }
                f.write_str("]")
            }
            LastValue {
                order_by,
                window_frame,
            } => {
                let order_by = order_by.iter().map(|col| self.child(col));
                f.write_str(name)?;
                f.write_str("[")?;
                write!(f, "order_by=[{}]", separated(", ", order_by))?;
                if *window_frame != WindowFrame::default() {
                    write!(f, " {}", window_frame)?;
                }
                f.write_str("]")
            }
            WindowAggregate {
                wrapped_aggregate,
                order_by,
                window_frame,
            } => {
                let order_by = order_by.iter().map(|col| self.child(col));
                let wrapped_aggregate = self.child(wrapped_aggregate.deref());
                f.write_str(name)?;
                f.write_str("[")?;
                write!(f, "{} ", wrapped_aggregate)?;
                write!(f, "order_by=[{}]", separated(", ", order_by))?;
                if *window_frame != WindowFrame::default() {
                    write!(f, " {}", window_frame)?;
                }
                f.write_str("]")
            }
            FusedValueWindowFunc { funcs, order_by } => {
                let order_by = order_by.iter().map(|col| self.child(col));
                let funcs = separated(", ", funcs.iter().map(|func| self.child(func)));
                f.write_str(name)?;
                f.write_str("[")?;
                write!(f, "{} ", funcs)?;
                write!(f, "order_by=[{}]", separated(", ", order_by))?;
                f.write_str("]")
            }
            _ => f.write_str(name),
        }
    }
}

#[derive(
    Arbitrary, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct CaptureGroupDesc {
    pub index: u32,
    pub name: Option<String>,
    pub nullable: bool,
}

impl RustType<ProtoCaptureGroupDesc> for CaptureGroupDesc {
    fn into_proto(&self) -> ProtoCaptureGroupDesc {
        ProtoCaptureGroupDesc {
            index: self.index,
            name: self.name.clone(),
            nullable: self.nullable,
        }
    }

    fn from_proto(proto: ProtoCaptureGroupDesc) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            index: proto.index,
            name: proto.name,
            nullable: proto.nullable,
        })
    }
}

#[derive(
    Arbitrary,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Serialize,
    Deserialize,
    Hash,
    MzReflect,
    Default,
)]
pub struct AnalyzedRegexOpts {
    pub case_insensitive: bool,
    pub global: bool,
}

impl FromStr for AnalyzedRegexOpts {
    type Err = EvalError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut opts = AnalyzedRegexOpts::default();
        for c in s.chars() {
            match c {
                'i' => opts.case_insensitive = true,
                'g' => opts.global = true,
                _ => return Err(EvalError::InvalidRegexFlag(c)),
            }
        }
        Ok(opts)
    }
}

impl RustType<ProtoAnalyzedRegexOpts> for AnalyzedRegexOpts {
    fn into_proto(&self) -> ProtoAnalyzedRegexOpts {
        ProtoAnalyzedRegexOpts {
            case_insensitive: self.case_insensitive,
            global: self.global,
        }
    }

    fn from_proto(proto: ProtoAnalyzedRegexOpts) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            case_insensitive: proto.case_insensitive,
            global: proto.global,
        })
    }
}

#[derive(
    Arbitrary, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect,
)]
pub struct AnalyzedRegex(
    #[proptest(strategy = "mz_repr::adt::regex::any_regex()")] ReprRegex,
    Vec<CaptureGroupDesc>,
    AnalyzedRegexOpts,
);

impl RustType<ProtoAnalyzedRegex> for AnalyzedRegex {
    fn into_proto(&self) -> ProtoAnalyzedRegex {
        ProtoAnalyzedRegex {
            regex: Some(self.0.into_proto()),
            groups: self.1.into_proto(),
            opts: Some(self.2.into_proto()),
        }
    }

    fn from_proto(proto: ProtoAnalyzedRegex) -> Result<Self, TryFromProtoError> {
        Ok(AnalyzedRegex(
            proto.regex.into_rust_if_some("ProtoAnalyzedRegex::regex")?,
            proto.groups.into_rust()?,
            proto.opts.into_rust_if_some("ProtoAnalyzedRegex::opts")?,
        ))
    }
}

impl AnalyzedRegex {
    pub fn new(s: &str, opts: AnalyzedRegexOpts) -> Result<Self, regex::Error> {
        let r = ReprRegex::new(s, opts.case_insensitive)?;
        // TODO(benesch): remove potentially dangerous usage of `as`.
        #[allow(clippy::as_conversions)]
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
                // https://github.com/MaterializeInc/database-issues/issues/612
                nullable: true,
            })
            .collect();
        Ok(Self(r, descs, opts))
    }
    pub fn capture_groups_len(&self) -> usize {
        self.1.len()
    }
    pub fn capture_groups_iter(&self) -> impl Iterator<Item = &CaptureGroupDesc> {
        self.1.iter()
    }
    pub fn inner(&self) -> &Regex {
        &(self.0).regex
    }
    pub fn opts(&self) -> &AnalyzedRegexOpts {
        &self.2
    }
}

pub fn csv_extract(a: Datum<'_>, n_cols: usize) -> impl Iterator<Item = (Row, Diff)> + '_ {
    let bytes = a.unwrap_str().as_bytes();
    let mut row = Row::default();
    let csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes);
    csv_reader.into_records().filter_map(move |res| match res {
        Ok(sr) if sr.len() == n_cols => {
            row.packer().extend(sr.iter().map(Datum::String));
            Some((row.clone(), Diff::ONE))
        }
        _ => None,
    })
}

pub fn repeat(a: Datum) -> Option<(Row, Diff)> {
    let n = a.unwrap_int64();
    if n != 0 {
        Some((Row::default(), n.into()))
    } else {
        None
    }
}

fn wrap<'a>(datums: &'a [Datum<'a>], width: usize) -> impl Iterator<Item = (Row, Diff)> + 'a {
    datums
        .chunks(width)
        .map(|chunk| (Row::pack(chunk), Diff::ONE))
}

fn acl_explode<'a>(
    acl_items: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<impl Iterator<Item = (Row, Diff)> + 'a, EvalError> {
    let acl_items = acl_items.unwrap_array();
    let mut res = Vec::new();
    for acl_item in acl_items.elements().iter() {
        if acl_item.is_null() {
            return Err(EvalError::AclArrayNullElement);
        }
        let acl_item = acl_item.unwrap_acl_item();
        for privilege in acl_item.acl_mode.explode() {
            let row = [
                Datum::UInt32(acl_item.grantor.0),
                Datum::UInt32(acl_item.grantee.0),
                Datum::String(temp_storage.push_string(privilege.to_string())),
                // GRANT OPTION is not implemented, so we hardcode false.
                Datum::False,
            ];
            res.push((Row::pack_slice(&row), Diff::ONE));
        }
    }
    Ok(res.into_iter())
}

fn mz_acl_explode<'a>(
    mz_acl_items: Datum<'a>,
    temp_storage: &'a RowArena,
) -> Result<impl Iterator<Item = (Row, Diff)> + 'a, EvalError> {
    let mz_acl_items = mz_acl_items.unwrap_array();
    let mut res = Vec::new();
    for mz_acl_item in mz_acl_items.elements().iter() {
        if mz_acl_item.is_null() {
            return Err(EvalError::MzAclArrayNullElement);
        }
        let mz_acl_item = mz_acl_item.unwrap_mz_acl_item();
        for privilege in mz_acl_item.acl_mode.explode() {
            let row = [
                Datum::String(temp_storage.push_string(mz_acl_item.grantor.to_string())),
                Datum::String(temp_storage.push_string(mz_acl_item.grantee.to_string())),
                Datum::String(temp_storage.push_string(privilege.to_string())),
                // GRANT OPTION is not implemented, so we hardcode false.
                Datum::False,
            ];
            res.push((Row::pack_slice(&row), Diff::ONE));
        }
    }
    Ok(res.into_iter())
}

/// When adding a new `TableFunc` variant, please consider adding it to
/// `TableFunc::with_ordinality`!
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect)]
pub enum TableFunc {
    AclExplode,
    MzAclExplode,
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
    GenerateSeriesTimestamp,
    GenerateSeriesTimestampTz,
    /// Supplied with an input count,
    ///   1. Adds a column as if a typed subquery result,
    ///   2. Filters the row away if the count is only one,
    ///   3. Errors if the count is not exactly one.
    /// The intent is that this presents as if a subquery result with too many
    /// records contributing. The error column has the same type as the result
    /// should have, but we only produce it if the count exceeds one.
    ///
    /// This logic could nearly be achieved with map, filter, project logic,
    /// but has been challenging to do in a way that respects the vagaries of
    /// SQL and our semantics. If we reveal a constant value in the column we
    /// risk the optimizer pruning the branch; if we reveal that this will not
    /// produce rows we risk the optimizer pruning the branch; if we reveal that
    /// the only possible value is an error we risk the optimizer propagating that
    /// error without guards.
    ///
    /// Before replacing this by an `MirScalarExpr`, quadruple check that it
    /// would not result in misoptimizations due to expression evaluation order
    /// being utterly undefined, and predicate pushdown trimming any fragments
    /// that might produce columns that will not be needed.
    GuardSubquerySize {
        column_type: ScalarType,
    },
    Repeat,
    UnnestArray {
        el_typ: ScalarType,
    },
    UnnestList {
        el_typ: ScalarType,
    },
    UnnestMap {
        value_type: ScalarType,
    },
    /// Given `n` input expressions, wraps them into `n / width` rows, each of
    /// `width` columns.
    ///
    /// This function is not intended to be called directly by end users, but
    /// is useful in the planning of e.g. VALUES clauses.
    Wrap {
        types: Vec<ColumnType>,
        width: usize,
    },
    GenerateSubscriptsArray,
    /// Execute some arbitrary scalar function as a table function.
    TabletizedScalar {
        name: String,
        relation: RelationType,
    },
    RegexpMatches,
    /// Implements the WITH ORDINALITY clause.
    ///
    /// Don't construct `TableFunc::WithOrdinality` manually! Use the `with_ordinality` constructor
    /// function instead, which checks whether the given table function supports `WithOrdinality`.
    #[allow(private_interfaces)]
    WithOrdinality(WithOrdinality),
}

/// Private enum variant of `TableFunc`. Don't construct this directly, but use
/// `TableFunc::with_ordinality` instead.
///
/// Evaluates the inner table function, expands its results into unary (repeating each row as
/// many times as the diff indicates), and appends an integer corresponding to the ordinal
/// position (starting from 1). For example, it numbers the elements of a list when calling
/// `unnest_list`.
///
/// TODO(ggevay): This struct (and its field) is pub only temporarily, until we make
/// `FlatMapElimination` not dive into it.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash, MzReflect)]
pub struct WithOrdinality {
    pub inner: Box<TableFunc>,
}

impl TableFunc {
    /// Adds `WITH ORDINALITY` to a table function if it's allowed on the given table function.
    pub fn with_ordinality(inner: TableFunc) -> Option<TableFunc> {
        match inner {
            TableFunc::AclExplode
            | TableFunc::MzAclExplode
            | TableFunc::JsonbEach { .. }
            | TableFunc::JsonbObjectKeys
            | TableFunc::JsonbArrayElements { .. }
            | TableFunc::RegexpExtract(_)
            | TableFunc::CsvExtract(_)
            | TableFunc::GenerateSeriesInt32
            | TableFunc::GenerateSeriesInt64
            | TableFunc::GenerateSeriesTimestamp
            | TableFunc::GenerateSeriesTimestampTz
            | TableFunc::GuardSubquerySize { .. }
            | TableFunc::Repeat
            | TableFunc::UnnestArray { .. }
            | TableFunc::UnnestList { .. }
            | TableFunc::UnnestMap { .. }
            | TableFunc::Wrap { .. }
            | TableFunc::GenerateSubscriptsArray
            | TableFunc::TabletizedScalar { .. }
            | TableFunc::RegexpMatches => Some(TableFunc::WithOrdinality(WithOrdinality {
                inner: Box::new(inner),
            })),
            // IMPORTANT: Before adding a new table function here, consider negative diffs:
            // `WithOrdinality::eval` will panic if the inner table function emits a negative diff.
            _ => None,
        }
    }
}

/// Manual `Arbitrary`, because proptest-derive is choking on the recursive `WithOrdinality`
/// variant.
impl Arbitrary for TableFunc {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        let leaf = Union::new(vec![
            Just(TableFunc::AclExplode),
            Just(TableFunc::MzAclExplode),
            Just(TableFunc::JsonbObjectKeys),
            Just(TableFunc::GenerateSeriesInt32),
            Just(TableFunc::GenerateSeriesInt64),
            Just(TableFunc::GenerateSeriesTimestamp),
            Just(TableFunc::GenerateSeriesTimestampTz),
            Just(TableFunc::Repeat),
            Just(TableFunc::GenerateSubscriptsArray),
            Just(TableFunc::RegexpMatches),
        ])
        .boxed();

        // recursive WithOrdinality variant
        leaf.prop_recursive(2, 256, 2, |inner| {
            inner.clone().prop_map(|tf| {
                TableFunc::WithOrdinality(WithOrdinality {
                    inner: Box::new(tf),
                })
            })
        })
        .boxed()
    }
}

impl RustType<ProtoTableFunc> for TableFunc {
    fn into_proto(&self) -> ProtoTableFunc {
        use proto_table_func::{Kind, ProtoWrap};

        ProtoTableFunc {
            kind: Some(match self {
                TableFunc::AclExplode => Kind::AclExplode(()),
                TableFunc::MzAclExplode => Kind::MzAclExplode(()),
                TableFunc::JsonbEach { stringify } => Kind::JsonbEach(*stringify),
                TableFunc::JsonbObjectKeys => Kind::JsonbObjectKeys(()),
                TableFunc::JsonbArrayElements { stringify } => Kind::JsonbArrayElements(*stringify),
                TableFunc::RegexpExtract(x) => Kind::RegexpExtract(x.into_proto()),
                TableFunc::CsvExtract(x) => Kind::CsvExtract(x.into_proto()),
                TableFunc::GenerateSeriesInt32 => Kind::GenerateSeriesInt32(()),
                TableFunc::GenerateSeriesInt64 => Kind::GenerateSeriesInt64(()),
                TableFunc::GenerateSeriesTimestamp => Kind::GenerateSeriesTimestamp(()),
                TableFunc::GenerateSeriesTimestampTz => Kind::GenerateSeriesTimestampTz(()),
                TableFunc::GuardSubquerySize { column_type } => {
                    Kind::GuardSubquerySize(column_type.into_proto())
                }
                TableFunc::Repeat => Kind::Repeat(()),
                TableFunc::UnnestArray { el_typ } => Kind::UnnestArray(el_typ.into_proto()),
                TableFunc::UnnestList { el_typ } => Kind::UnnestList(el_typ.into_proto()),
                TableFunc::UnnestMap { value_type } => Kind::UnnestMap(value_type.into_proto()),
                TableFunc::Wrap { types, width } => Kind::Wrap(ProtoWrap {
                    types: types.into_proto(),
                    width: width.into_proto(),
                }),
                TableFunc::GenerateSubscriptsArray => Kind::GenerateSubscriptsArray(()),
                TableFunc::TabletizedScalar { name, relation } => {
                    Kind::TabletizedScalar(ProtoTabletizedScalar {
                        name: name.into_proto(),
                        relation: Some(relation.into_proto()),
                    })
                }
                TableFunc::RegexpMatches => Kind::RegexpMatches(()),
                TableFunc::WithOrdinality(WithOrdinality { inner }) => {
                    Kind::WithOrdinality(Box::new(ProtoWithOrdinality {
                        inner: Some(inner.into_proto()),
                    }))
                }
            }),
        }
    }

    fn from_proto(proto: ProtoTableFunc) -> Result<Self, TryFromProtoError> {
        use proto_table_func::Kind;

        let kind = proto
            .kind
            .ok_or_else(|| TryFromProtoError::missing_field("ProtoTableFunc::Kind"))?;

        Ok(match kind {
            Kind::AclExplode(()) => TableFunc::AclExplode,
            Kind::MzAclExplode(()) => TableFunc::MzAclExplode,
            Kind::JsonbEach(stringify) => TableFunc::JsonbEach { stringify },
            Kind::JsonbObjectKeys(()) => TableFunc::JsonbObjectKeys,
            Kind::JsonbArrayElements(stringify) => TableFunc::JsonbArrayElements { stringify },
            Kind::RegexpExtract(x) => TableFunc::RegexpExtract(x.into_rust()?),
            Kind::CsvExtract(x) => TableFunc::CsvExtract(x.into_rust()?),
            Kind::GenerateSeriesInt32(()) => TableFunc::GenerateSeriesInt32,
            Kind::GenerateSeriesInt64(()) => TableFunc::GenerateSeriesInt64,
            Kind::GenerateSeriesTimestamp(()) => TableFunc::GenerateSeriesTimestamp,
            Kind::GenerateSeriesTimestampTz(()) => TableFunc::GenerateSeriesTimestampTz,
            Kind::GuardSubquerySize(x) => TableFunc::GuardSubquerySize {
                column_type: x.into_rust()?,
            },
            Kind::Repeat(()) => TableFunc::Repeat,
            Kind::UnnestArray(x) => TableFunc::UnnestArray {
                el_typ: x.into_rust()?,
            },
            Kind::UnnestList(x) => TableFunc::UnnestList {
                el_typ: x.into_rust()?,
            },
            Kind::UnnestMap(value_type) => TableFunc::UnnestMap {
                value_type: value_type.into_rust()?,
            },
            Kind::Wrap(x) => TableFunc::Wrap {
                width: x.width.into_rust()?,
                types: x.types.into_rust()?,
            },
            Kind::GenerateSubscriptsArray(()) => TableFunc::GenerateSubscriptsArray,
            Kind::TabletizedScalar(v) => TableFunc::TabletizedScalar {
                name: v.name,
                relation: v
                    .relation
                    .into_rust_if_some("ProtoTabletizedScalar::relation")?,
            },
            Kind::RegexpMatches(_) => TableFunc::RegexpMatches,
            Kind::WithOrdinality(inner) => TableFunc::WithOrdinality(WithOrdinality {
                inner: Box::new(
                    inner
                        .inner
                        .map(|inner| *inner)
                        .into_rust_if_some("ProtoWithOrdinality::inner")?,
                ),
            }),
        })
    }
}

impl TableFunc {
    /// Executes `self` on the given input row (`datums`).
    pub fn eval<'a>(
        &'a self,
        datums: &'a [Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Box<dyn Iterator<Item = (Row, Diff)> + 'a>, EvalError> {
        if self.empty_on_null_input() && datums.iter().any(|d| d.is_null()) {
            return Ok(Box::new(vec![].into_iter()));
        }
        match self {
            TableFunc::AclExplode => Ok(Box::new(acl_explode(datums[0], temp_storage)?)),
            TableFunc::MzAclExplode => Ok(Box::new(mz_acl_explode(datums[0], temp_storage)?)),
            TableFunc::JsonbEach { stringify } => {
                Ok(Box::new(jsonb_each(datums[0], temp_storage, *stringify)))
            }
            TableFunc::JsonbObjectKeys => Ok(Box::new(jsonb_object_keys(datums[0]))),
            TableFunc::JsonbArrayElements { stringify } => Ok(Box::new(jsonb_array_elements(
                datums[0],
                temp_storage,
                *stringify,
            ))),
            TableFunc::RegexpExtract(a) => Ok(Box::new(regexp_extract(datums[0], a).into_iter())),
            TableFunc::CsvExtract(n_cols) => Ok(Box::new(csv_extract(datums[0], *n_cols))),
            TableFunc::GenerateSeriesInt32 => {
                let res = generate_series(
                    datums[0].unwrap_int32(),
                    datums[1].unwrap_int32(),
                    datums[2].unwrap_int32(),
                )?;
                Ok(Box::new(res))
            }
            TableFunc::GenerateSeriesInt64 => {
                let res = generate_series(
                    datums[0].unwrap_int64(),
                    datums[1].unwrap_int64(),
                    datums[2].unwrap_int64(),
                )?;
                Ok(Box::new(res))
            }
            TableFunc::GenerateSeriesTimestamp => {
                fn pass_through<'a>(d: CheckedTimestamp<NaiveDateTime>) -> Datum<'a> {
                    Datum::from(d)
                }
                let res = generate_series_ts(
                    datums[0].unwrap_timestamp(),
                    datums[1].unwrap_timestamp(),
                    datums[2].unwrap_interval(),
                    pass_through,
                )?;
                Ok(Box::new(res))
            }
            TableFunc::GenerateSeriesTimestampTz => {
                fn gen_ts_tz<'a>(d: CheckedTimestamp<DateTime<Utc>>) -> Datum<'a> {
                    Datum::from(d)
                }
                let res = generate_series_ts(
                    datums[0].unwrap_timestamptz(),
                    datums[1].unwrap_timestamptz(),
                    datums[2].unwrap_interval(),
                    gen_ts_tz,
                )?;
                Ok(Box::new(res))
            }
            TableFunc::GenerateSubscriptsArray => {
                generate_subscripts_array(datums[0], datums[1].unwrap_int32())
            }
            TableFunc::GuardSubquerySize { column_type: _ } => {
                // We error if the count is not one,
                // and produce no rows if equal to one.
                let count = datums[0].unwrap_int64();
                if count != 1 {
                    Err(EvalError::MultipleRowsFromSubquery)
                } else {
                    Ok(Box::new([].into_iter()))
                }
            }
            TableFunc::Repeat => Ok(Box::new(repeat(datums[0]).into_iter())),
            TableFunc::UnnestArray { .. } => Ok(Box::new(unnest_array(datums[0]))),
            TableFunc::UnnestList { .. } => Ok(Box::new(unnest_list(datums[0]))),
            TableFunc::UnnestMap { .. } => Ok(Box::new(unnest_map(datums[0]))),
            TableFunc::Wrap { width, .. } => Ok(Box::new(wrap(datums, *width))),
            TableFunc::TabletizedScalar { .. } => {
                let r = Row::pack_slice(datums);
                Ok(Box::new(std::iter::once((r, Diff::ONE))))
            }
            TableFunc::RegexpMatches => Ok(Box::new(regexp_matches(datums)?)),
            TableFunc::WithOrdinality(func_with_ordinality) => {
                func_with_ordinality.eval(datums, temp_storage)
            }
        }
    }

    pub fn output_type(&self) -> RelationType {
        let (column_types, keys) = match self {
            TableFunc::AclExplode => {
                let column_types = vec![
                    ScalarType::Oid.nullable(false),
                    ScalarType::Oid.nullable(false),
                    ScalarType::String.nullable(false),
                    ScalarType::Bool.nullable(false),
                ];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::MzAclExplode => {
                let column_types = vec![
                    ScalarType::String.nullable(false),
                    ScalarType::String.nullable(false),
                    ScalarType::String.nullable(false),
                    ScalarType::Bool.nullable(false),
                ];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::JsonbEach { stringify: true } => {
                let column_types = vec![
                    ScalarType::String.nullable(false),
                    ScalarType::String.nullable(true),
                ];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::JsonbEach { stringify: false } => {
                let column_types = vec![
                    ScalarType::String.nullable(false),
                    ScalarType::Jsonb.nullable(false),
                ];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::JsonbObjectKeys => {
                let column_types = vec![ScalarType::String.nullable(false)];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::JsonbArrayElements { stringify: true } => {
                let column_types = vec![ScalarType::String.nullable(true)];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::JsonbArrayElements { stringify: false } => {
                let column_types = vec![ScalarType::Jsonb.nullable(false)];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::RegexpExtract(a) => {
                let column_types = a
                    .capture_groups_iter()
                    .map(|cg| ScalarType::String.nullable(cg.nullable))
                    .collect();
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::CsvExtract(n_cols) => {
                let column_types = iter::repeat(ScalarType::String.nullable(false))
                    .take(*n_cols)
                    .collect();
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::GenerateSeriesInt32 => {
                let column_types = vec![ScalarType::Int32.nullable(false)];
                let keys = vec![vec![0]];
                (column_types, keys)
            }
            TableFunc::GenerateSeriesInt64 => {
                let column_types = vec![ScalarType::Int64.nullable(false)];
                let keys = vec![vec![0]];
                (column_types, keys)
            }
            TableFunc::GenerateSeriesTimestamp => {
                let column_types = vec![ScalarType::Timestamp { precision: None }.nullable(false)];
                let keys = vec![vec![0]];
                (column_types, keys)
            }
            TableFunc::GenerateSeriesTimestampTz => {
                let column_types =
                    vec![ScalarType::TimestampTz { precision: None }.nullable(false)];
                let keys = vec![vec![0]];
                (column_types, keys)
            }
            TableFunc::GenerateSubscriptsArray => {
                let column_types = vec![ScalarType::Int32.nullable(false)];
                let keys = vec![vec![0]];
                (column_types, keys)
            }
            TableFunc::GuardSubquerySize { column_type } => {
                let column_types = vec![column_type.clone().nullable(false)];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::Repeat => {
                let column_types = vec![];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::UnnestArray { el_typ } => {
                let column_types = vec![el_typ.clone().nullable(true)];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::UnnestList { el_typ } => {
                let column_types = vec![el_typ.clone().nullable(true)];
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::UnnestMap { value_type } => {
                let column_types = vec![
                    ScalarType::String.nullable(false),
                    value_type.clone().nullable(true),
                ];
                let keys = vec![vec![0]];
                (column_types, keys)
            }
            TableFunc::Wrap { types, .. } => {
                let column_types = types.clone();
                let keys = vec![];
                (column_types, keys)
            }
            TableFunc::TabletizedScalar { relation, .. } => {
                return relation.clone();
            }
            TableFunc::RegexpMatches => {
                let column_types =
                    vec![ScalarType::Array(Box::new(ScalarType::String)).nullable(false)];
                let keys = vec![];

                (column_types, keys)
            }
            TableFunc::WithOrdinality(WithOrdinality { inner }) => {
                let mut typ = inner.output_type();
                // Add the ordinality column.
                typ.column_types.push(ScalarType::Int64.nullable(false));
                // The ordinality column is always a key.
                typ.keys.push(vec![typ.column_types.len() - 1]);
                (typ.column_types, typ.keys)
            }
        };

        soft_assert_eq_no_log!(column_types.len(), self.output_arity());

        if !keys.is_empty() {
            RelationType::new(column_types).with_keys(keys)
        } else {
            RelationType::new(column_types)
        }
    }

    pub fn output_arity(&self) -> usize {
        match self {
            TableFunc::AclExplode => 4,
            TableFunc::MzAclExplode => 4,
            TableFunc::JsonbEach { .. } => 2,
            TableFunc::JsonbObjectKeys => 1,
            TableFunc::JsonbArrayElements { .. } => 1,
            TableFunc::RegexpExtract(a) => a.capture_groups_len(),
            TableFunc::CsvExtract(n_cols) => *n_cols,
            TableFunc::GenerateSeriesInt32 => 1,
            TableFunc::GenerateSeriesInt64 => 1,
            TableFunc::GenerateSeriesTimestamp => 1,
            TableFunc::GenerateSeriesTimestampTz => 1,
            TableFunc::GenerateSubscriptsArray => 1,
            TableFunc::GuardSubquerySize { .. } => 1,
            TableFunc::Repeat => 0,
            TableFunc::UnnestArray { .. } => 1,
            TableFunc::UnnestList { .. } => 1,
            TableFunc::UnnestMap { .. } => 2,
            TableFunc::Wrap { width, .. } => *width,
            TableFunc::TabletizedScalar { relation, .. } => relation.column_types.len(),
            TableFunc::RegexpMatches => 1,
            TableFunc::WithOrdinality(WithOrdinality { inner }) => inner.output_arity() + 1,
        }
    }

    pub fn empty_on_null_input(&self) -> bool {
        match self {
            TableFunc::AclExplode
            | TableFunc::MzAclExplode
            | TableFunc::JsonbEach { .. }
            | TableFunc::JsonbObjectKeys
            | TableFunc::JsonbArrayElements { .. }
            | TableFunc::GenerateSeriesInt32
            | TableFunc::GenerateSeriesInt64
            | TableFunc::GenerateSeriesTimestamp
            | TableFunc::GenerateSeriesTimestampTz
            | TableFunc::GenerateSubscriptsArray
            | TableFunc::RegexpExtract(_)
            | TableFunc::CsvExtract(_)
            | TableFunc::Repeat
            | TableFunc::UnnestArray { .. }
            | TableFunc::UnnestList { .. }
            | TableFunc::UnnestMap { .. }
            | TableFunc::RegexpMatches => true,
            TableFunc::GuardSubquerySize { .. } => false,
            TableFunc::Wrap { .. } => false,
            TableFunc::TabletizedScalar { .. } => false,
            TableFunc::WithOrdinality(WithOrdinality { inner }) => inner.empty_on_null_input(),
        }
    }

    /// True iff the table function preserves the append-only property of its input.
    pub fn preserves_monotonicity(&self) -> bool {
        // Most variants preserve monotonicity, but all variants are enumerated to
        // ensure that added variants at least check that this is the case.
        match self {
            TableFunc::AclExplode => false,
            TableFunc::MzAclExplode => false,
            TableFunc::JsonbEach { .. } => true,
            TableFunc::JsonbObjectKeys => true,
            TableFunc::JsonbArrayElements { .. } => true,
            TableFunc::RegexpExtract(_) => true,
            TableFunc::CsvExtract(_) => true,
            TableFunc::GenerateSeriesInt32 => true,
            TableFunc::GenerateSeriesInt64 => true,
            TableFunc::GenerateSeriesTimestamp => true,
            TableFunc::GenerateSeriesTimestampTz => true,
            TableFunc::GenerateSubscriptsArray => true,
            TableFunc::Repeat => false,
            TableFunc::UnnestArray { .. } => true,
            TableFunc::UnnestList { .. } => true,
            TableFunc::UnnestMap { .. } => true,
            TableFunc::Wrap { .. } => true,
            TableFunc::TabletizedScalar { .. } => true,
            TableFunc::RegexpMatches => true,
            TableFunc::GuardSubquerySize { .. } => false,
            TableFunc::WithOrdinality(WithOrdinality { inner }) => inner.preserves_monotonicity(),
        }
    }
}

impl fmt::Display for TableFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            TableFunc::AclExplode => f.write_str("aclexplode"),
            TableFunc::MzAclExplode => f.write_str("mz_aclexplode"),
            TableFunc::JsonbEach { .. } => f.write_str("jsonb_each"),
            TableFunc::JsonbObjectKeys => f.write_str("jsonb_object_keys"),
            TableFunc::JsonbArrayElements { .. } => f.write_str("jsonb_array_elements"),
            TableFunc::RegexpExtract(a) => write!(f, "regexp_extract({:?}, _)", a.0),
            TableFunc::CsvExtract(n_cols) => write!(f, "csv_extract({}, _)", n_cols),
            TableFunc::GenerateSeriesInt32 => f.write_str("generate_series"),
            TableFunc::GenerateSeriesInt64 => f.write_str("generate_series"),
            TableFunc::GenerateSeriesTimestamp => f.write_str("generate_series"),
            TableFunc::GenerateSeriesTimestampTz => f.write_str("generate_series"),
            TableFunc::GenerateSubscriptsArray => f.write_str("generate_subscripts"),
            TableFunc::GuardSubquerySize { .. } => f.write_str("guard_subquery_size"),
            TableFunc::Repeat => f.write_str("repeat_row"),
            TableFunc::UnnestArray { .. } => f.write_str("unnest_array"),
            TableFunc::UnnestList { .. } => f.write_str("unnest_list"),
            TableFunc::UnnestMap { .. } => f.write_str("unnest_map"),
            TableFunc::Wrap { width, .. } => write!(f, "wrap{}", width),
            TableFunc::TabletizedScalar { name, .. } => f.write_str(name),
            TableFunc::RegexpMatches => write!(f, "regexp_matches(_, _, _)"),
            TableFunc::WithOrdinality(WithOrdinality { inner }) => {
                write!(f, "{}[with_ordinality]", inner)
            }
        }
    }
}

impl WithOrdinality {
    /// Executes the `self.inner` table function on the given input row (`datums`), and zips
    /// 1, 2, 3, ... to the result as a new column. We need to expand rows with non-1 diffs into the
    /// corresponding number of rows with unit diffs, because the ordinality column will have
    /// different values for each copy.
    ///
    /// # Panics
    ///
    /// Panics if the `inner` table function emits a negative diff.
    fn eval<'a>(
        &'a self,
        datums: &'a [Datum<'a>],
        temp_storage: &'a RowArena,
    ) -> Result<Box<dyn Iterator<Item = (Row, Diff)> + 'a>, EvalError> {
        let mut next_ordinal: i64 = 1;
        let it = self
            .inner
            .eval(datums, temp_storage)?
            .flat_map(move |(mut row, diff)| {
                let diff = diff.into_inner();
                // WITH ORDINALITY is not well-defined for negative diffs. This is ok, since the
                // only table function that can emit negative diffs is `repeat_row`, which is in
                // `mz_unsafe`, so users can never call it.
                //
                // (We also don't need to worry about negative diffs in FlatMap's input, because
                // the diff of the input of the FlatMap is factored in after we return from here.)
                assert!(diff >= 0);
                // The ordinals that will be associated with this row.
                let mut ordinals = next_ordinal..(next_ordinal + diff);
                next_ordinal += diff;
                // The maximum byte capacity we need for the original row and its ordinal.
                let cap = row.data_len() + datum_size(&Datum::Int64(next_ordinal));
                iter::from_fn(move || {
                    let ordinal = ordinals.next()?;
                    let mut row = if ordinals.is_empty() {
                        // This is the last row, so no need to clone. (Most table functions emit
                        // only 1 diffs, so this completely avoids cloning in most cases.)
                        std::mem::take(&mut row)
                    } else {
                        let mut new_row = Row::with_capacity(cap);
                        new_row.clone_from(&row);
                        new_row
                    };
                    RowPacker::for_existing_row(&mut row).push(Datum::Int64(ordinal));
                    Some((row, Diff::ONE))
                })
            });
        Ok(Box::new(it))
    }
}

#[cfg(test)]
mod tests {
    use super::{AggregateFunc, ProtoAggregateFunc, ProtoTableFunc, TableFunc};
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
       #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn aggregate_func_protobuf_roundtrip(expect in any::<AggregateFunc>() ) {
            let actual = protobuf_roundtrip::<_, ProtoAggregateFunc>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }

    proptest! {
       #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn table_func_protobuf_roundtrip(expect in any::<TableFunc>() ) {
            let actual = protobuf_roundtrip::<_, ProtoTableFunc>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
