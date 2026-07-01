// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: a `Row` survives persist's columnar Arrow encode/decode round
//! trip. `RelationDesc`'s `Schema<Row>` impl encodes rows into Arrow arrays
//! (the on-disk persist format) and decodes them back. A mismatch is silent
//! data corruption. This is coverage-guided, complementing the existing
//! `proptest_non_empty_relation_descs` property test.
//!
//! The generator focuses on the scalar types with non-trivial packed encodings
//! (PackedNumeric, PackedNaiveDateTime, PackedInterval, PackedNaiveTime, ...),
//! plus a few shapes that exercise interactions the bare-scalar columns can't:
//!
//!  * `Char { length }` / `VarChar { max_length }`: these decode through the
//!    same Arrow `StringArray` as plain strings, but carry a length parameter
//!    in the schema. We generate the parameter independently of the stored
//!    string to stress the schema/value split.
//!  * `Numeric { max_scale }` with the special values `NaN`, `Infinity`, and
//!    `-Infinity` (round-tripped via `PackedNumeric`'s BCD bytes), in addition
//!    to ordinary finite decimals, and a randomized `max_scale` in the schema.
//!  * Shallow composites (`List`/`Array`/`Map` of a scalar element and
//!    `Range` of a discrete scalar), placed alongside the packed-scalar columns
//!    so the multi-column encoder builds nested Arrow arrays next to the flat
//!    ones. The composite element is always a simple (non-composite) scalar to
//!    keep nesting shallow. Deeper nesting already has proptest coverage.
//!
//! Leap seconds are excluded (a separate known encoding gap).

#![no_main]

use std::borrow::Borrow;

use chrono::{DateTime, NaiveTime, Utc};
use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_persist_types::columnar::{ColumnDecoder, ColumnEncoder, Schema};
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::char::CharLength;
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::{cx_datum, Numeric, NumericMaxScale};
use mz_repr::adt::range::{Range, RangeBound, RangeLowerBound, RangeUpperBound};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::strconv::parse_numeric;
use mz_repr::{Datum, RelationDesc, Row, SqlColumnType, SqlScalarType, Timestamp};

/// Generate a "simple" scalar type: one that maps to a single flat Arrow array
/// and can serve as a composite element type (no nesting).
fn gen_simple_scalar_type(u: &mut Unstructured) -> arbitrary::Result<SqlScalarType> {
    Ok(match u.int_in_range(0u8..=20)? {
        0 => SqlScalarType::Bool,
        1 => SqlScalarType::Int16,
        2 => SqlScalarType::Int32,
        3 => SqlScalarType::Int64,
        4 => SqlScalarType::UInt16,
        5 => SqlScalarType::UInt32,
        6 => SqlScalarType::UInt64,
        7 => SqlScalarType::PgLegacyChar, // a u8 datum
        8 => SqlScalarType::Float32,
        9 => SqlScalarType::Float64,
        10 => SqlScalarType::String,
        11 => SqlScalarType::Bytes,
        12 => SqlScalarType::Numeric {
            max_scale: gen_max_scale(u)?,
        },
        13 => SqlScalarType::Date,
        14 => SqlScalarType::Time,
        15 => SqlScalarType::Timestamp { precision: None },
        16 => SqlScalarType::TimestampTz { precision: None },
        17 => SqlScalarType::Interval,
        18 => SqlScalarType::MzTimestamp,
        19 => SqlScalarType::Char {
            length: gen_char_length(u)?,
        },
        _ => SqlScalarType::VarChar {
            max_length: gen_varchar_max_length(u)?,
        },
    })
}

/// Generate any column scalar type: a simple scalar, or a shallow composite
/// (`List`/`Array`/`Map` of a simple scalar, or `Range` of a discrete scalar).
fn gen_scalar_type(u: &mut Unstructured) -> arbitrary::Result<SqlScalarType> {
    // Bias toward simple scalars (which carry the packed encodings we mostly
    // care about) but reach the composites a meaningful fraction of the time.
    Ok(match u.int_in_range(0u8..=11)? {
        0..=7 => gen_simple_scalar_type(u)?,
        8 => SqlScalarType::List {
            element_type: Box::new(gen_element_type(u)?),
            custom_id: None,
        },
        9 => SqlScalarType::Array(Box::new(gen_element_type(u)?)),
        10 => SqlScalarType::Map {
            value_type: Box::new(gen_element_type(u)?),
            custom_id: None,
        },
        _ => SqlScalarType::Range {
            element_type: Box::new(gen_range_element_type(u)?),
        },
    })
}

/// Composite element types. Restricted to scalars whose `Datum` is `Copy` (i.e.
/// carries no borrow into the input buffer) so we can collect element datums
/// into a `Vec` and push them without leaking backing storage. This still
/// exercises the nested Arrow encoders for the packed-scalar element types.
fn gen_element_type(u: &mut Unstructured) -> arbitrary::Result<SqlScalarType> {
    Ok(match u.int_in_range(0u8..=11)? {
        0 => SqlScalarType::Bool,
        1 => SqlScalarType::Int16,
        2 => SqlScalarType::Int32,
        3 => SqlScalarType::Int64,
        4 => SqlScalarType::UInt32,
        5 => SqlScalarType::UInt64,
        6 => SqlScalarType::Float64,
        7 => SqlScalarType::Numeric {
            max_scale: gen_max_scale(u)?,
        },
        8 => SqlScalarType::Date,
        9 => SqlScalarType::Time,
        10 => SqlScalarType::Timestamp { precision: None },
        _ => SqlScalarType::Interval,
    })
}

/// Range element types are restricted to the discrete/continuous types that
/// `RangeBound::canonicalize` accepts. We use the two discrete integer types so
/// canonicalization is exercised without dragging in continuous-type quirks.
fn gen_range_element_type(u: &mut Unstructured) -> arbitrary::Result<SqlScalarType> {
    Ok(if bool::arbitrary(u)? {
        SqlScalarType::Int32
    } else {
        SqlScalarType::Int64
    })
}

/// A `max_scale` for `Numeric`: `None` half the time, otherwise an in-range
/// `0..=39` scale to stress the schema-side parameter.
fn gen_max_scale(u: &mut Unstructured) -> arbitrary::Result<Option<NumericMaxScale>> {
    if bool::arbitrary(u)? {
        Ok(None)
    } else {
        let s = u.int_in_range(0i64..=39)?;
        Ok(NumericMaxScale::try_from(s).ok())
    }
}

/// A `Char` length: `None` (the "list element" form) some of the time,
/// otherwise a small positive length.
fn gen_char_length(u: &mut Unstructured) -> arbitrary::Result<Option<CharLength>> {
    if u.ratio(1u8, 4u8)? {
        Ok(None)
    } else {
        let n = u.int_in_range(1i64..=300)?;
        Ok(CharLength::try_from(n).ok())
    }
}

/// A `VarChar` max length: `None` some of the time, otherwise a small positive
/// max length.
fn gen_varchar_max_length(u: &mut Unstructured) -> arbitrary::Result<Option<VarCharMaxLength>> {
    if u.ratio(1u8, 4u8)? {
        Ok(None)
    } else {
        let n = u.int_in_range(1i64..=300)?;
        Ok(VarCharMaxLength::try_from(n).ok())
    }
}

fn gen_naive_ts(
    u: &mut Unstructured,
) -> arbitrary::Result<CheckedTimestamp<chrono::NaiveDateTime>> {
    let secs = u.int_in_range(-8_000_000_000_000i64..=8_000_000_000_000)?;
    let nanos = u.int_in_range(0u32..=999_999_999)?; // < 1s: no leap second
    Ok(DateTime::from_timestamp(secs, nanos)
        .and_then(|d| CheckedTimestamp::from_timestamplike(d.naive_utc()).ok())
        .unwrap_or_else(|| {
            CheckedTimestamp::from_timestamplike(
                DateTime::from_timestamp(0, 0).unwrap().naive_utc(),
            )
            .unwrap()
        }))
}

fn gen_utc_ts(u: &mut Unstructured) -> arbitrary::Result<CheckedTimestamp<DateTime<Utc>>> {
    let secs = u.int_in_range(-8_000_000_000_000i64..=8_000_000_000_000)?;
    let nanos = u.int_in_range(0u32..=999_999_999)?;
    Ok(DateTime::from_timestamp(secs, nanos)
        .and_then(|d| CheckedTimestamp::from_timestamplike(d).ok())
        .unwrap_or_else(|| {
            CheckedTimestamp::from_timestamplike(DateTime::from_timestamp(0, 0).unwrap()).unwrap()
        }))
}

/// Generate a `Numeric` datum value, reaching the special values that an Arrow
/// `PackedNumeric` must round-trip (`NaN`, `±Infinity`) as well as ordinary
/// finite decimals.
///
/// `parse_numeric` deliberately rejects directly-typed `NaN`/`Infinity` (it
/// only allows infinities that arose from overflow), so the special values are
/// constructed straight from the `dec` context instead of via text.
fn gen_numeric(u: &mut Unstructured) -> arbitrary::Result<dec::OrderedDecimal<Numeric>> {
    let n = match u.int_in_range(0u8..=5)? {
        0 => Numeric::nan(),
        1 => Numeric::infinity(),
        2 => {
            let mut cx = cx_datum();
            let mut n = Numeric::infinity();
            cx.neg(&mut n);
            n
        }
        _ => {
            let s = format!("{}.{}", i64::arbitrary(u)?, u32::arbitrary(u)?);
            return Ok(parse_numeric(&s).unwrap_or_else(|_| parse_numeric("0").unwrap()));
        }
    };
    Ok(dec::OrderedDecimal(n))
}

/// Push one datum of the given type (or NULL) into the row. Byte reads use `?`.
/// Out-of-range constructed values fall back to a valid datum so the row always
/// matches the column type.
fn push_datum(
    packer: &mut mz_repr::RowPacker,
    u: &mut Unstructured,
    ty: &SqlScalarType,
    nullable: bool,
) -> arbitrary::Result<()> {
    if nullable && u.ratio(1u8, 8u8)? {
        packer.push(Datum::Null);
        return Ok(());
    }
    match ty {
        SqlScalarType::Bool => packer.push(if bool::arbitrary(u)? {
            Datum::True
        } else {
            Datum::False
        }),
        SqlScalarType::Int16 => packer.push(Datum::Int16(i16::arbitrary(u)?)),
        SqlScalarType::Int32 => packer.push(Datum::Int32(i32::arbitrary(u)?)),
        SqlScalarType::Int64 => packer.push(Datum::Int64(i64::arbitrary(u)?)),
        SqlScalarType::UInt16 => packer.push(Datum::UInt16(u16::arbitrary(u)?)),
        SqlScalarType::UInt32 => packer.push(Datum::UInt32(u32::arbitrary(u)?)),
        SqlScalarType::UInt64 => packer.push(Datum::UInt64(u64::arbitrary(u)?)),
        SqlScalarType::PgLegacyChar => packer.push(Datum::UInt8(u8::arbitrary(u)?)),
        SqlScalarType::Float32 => packer.push(Datum::Float32(f32::arbitrary(u)?.into())),
        SqlScalarType::Float64 => packer.push(Datum::Float64(f64::arbitrary(u)?.into())),
        SqlScalarType::String => packer.push(Datum::String(<&str>::arbitrary(u)?)),
        // Char/VarChar are stored as `Datum::String` and encode through the
        // same Arrow `StringArray`. The schema-side length parameter does not
        // truncate/pad here, so any string round-trips.
        SqlScalarType::Char { .. } | SqlScalarType::VarChar { .. } => {
            packer.push(Datum::String(<&str>::arbitrary(u)?))
        }
        SqlScalarType::Bytes => packer.push(Datum::Bytes(<&[u8]>::arbitrary(u)?)),
        SqlScalarType::Numeric { .. } => {
            packer.push(Datum::Numeric(gen_numeric(u)?));
        }
        SqlScalarType::Date => {
            let d = Date::from_pg_epoch(i32::arbitrary(u)?)
                .unwrap_or_else(|_| Date::from_pg_epoch(0).unwrap());
            packer.push(Datum::Date(d));
        }
        SqlScalarType::Time => {
            let secs = u.int_in_range(0u32..=86_399)?;
            let nanos = u.int_in_range(0u32..=999_999_999)?;
            let t = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).unwrap();
            packer.push(Datum::Time(t));
        }
        SqlScalarType::Timestamp { .. } => packer.push(Datum::Timestamp(gen_naive_ts(u)?)),
        SqlScalarType::TimestampTz { .. } => packer.push(Datum::TimestampTz(gen_utc_ts(u)?)),
        SqlScalarType::Interval => packer.push(Datum::Interval(Interval::new(
            i32::arbitrary(u)?,
            i32::arbitrary(u)?,
            i64::arbitrary(u)?,
        ))),
        SqlScalarType::MzTimestamp => {
            packer.push(Datum::MzTimestamp(Timestamp::from(u64::arbitrary(u)?)))
        }
        // A `List` of `element_type`: a small run of element datums (each of
        // which may itself be NULL since list elements are always nullable).
        SqlScalarType::List { element_type, .. } => {
            let elems = gen_scalar_datums(u, element_type)?;
            packer.push_list(elems.iter().map(Borrow::borrow));
        }
        // A 1-D `Array` of `element_type`. The single dimension's length must
        // match the number of pushed elements. `lower_bound` is arbitrary.
        SqlScalarType::Array(element_type) => {
            let elems = gen_scalar_datums(u, element_type)?;
            let dims = if elems.is_empty() {
                vec![]
            } else {
                vec![ArrayDimension {
                    lower_bound: 1,
                    length: elems.len(),
                }]
            };
            // The element count always matches `dims`, so this can't fail. Fall
            // back to an empty array out of an abundance of caution.
            if packer
                .try_push_array(&dims, elems.iter().map(Borrow::borrow))
                .is_err()
            {
                packer.try_push_array(&[], std::iter::empty::<Datum>()).unwrap();
            }
        }
        // A `Map` of string keys to `value_type` values. Keys must be unique
        // and sorted, which `push_dict` does not enforce, so we dedup/sort here.
        SqlScalarType::Map { value_type, .. } => {
            let n = u.int_in_range(0usize..=6)?;
            let mut entries: Vec<(String, Datum)> = Vec::with_capacity(n);
            for _ in 0..n {
                let k = String::arbitrary(u)?;
                entries.push((k, gen_scalar_datum(u, value_type)?));
            }
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            entries.dedup_by(|a, b| a.0 == b.0);
            packer.push_dict(entries.iter().map(|(k, v)| (k.as_str(), v)));
        }
        // A `Range` over a discrete element type. Bounds are independently
        // finite/infinite and inclusive/exclusive. `push_range` canonicalizes.
        SqlScalarType::Range { element_type } => {
            push_range(packer, u, element_type)?;
        }
        // gen_scalar_type only produces the types above.
        _ => packer.push(Datum::Null),
    }
    Ok(())
}

/// Generate a small vector of owned datums of a simple scalar type, for use as
/// the elements of a list/array. Elements may be NULL.
fn gen_scalar_datums<'a>(
    u: &mut Unstructured,
    ty: &SqlScalarType,
) -> arbitrary::Result<Vec<Datum<'a>>> {
    let n = u.int_in_range(0usize..=6)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if u.ratio(1u8, 8u8)? {
            out.push(Datum::Null);
        } else {
            out.push(gen_scalar_datum(u, ty)?);
        }
    }
    Ok(out)
}

/// Generate a single owned (non-null) datum for a composite element type.
/// Only the `Copy` `Datum` variants (those produced by `gen_element_type` /
/// `gen_range_element_type`) are handled, so the returned `Datum` carries no
/// borrow and the caller can collect it into a `Vec`.
fn gen_scalar_datum<'a>(
    u: &mut Unstructured,
    ty: &SqlScalarType,
) -> arbitrary::Result<Datum<'a>> {
    Ok(match ty {
        SqlScalarType::Bool => {
            if bool::arbitrary(u)? {
                Datum::True
            } else {
                Datum::False
            }
        }
        SqlScalarType::Int16 => Datum::Int16(i16::arbitrary(u)?),
        SqlScalarType::Int32 => Datum::Int32(i32::arbitrary(u)?),
        SqlScalarType::Int64 => Datum::Int64(i64::arbitrary(u)?),
        SqlScalarType::UInt32 => Datum::UInt32(u32::arbitrary(u)?),
        SqlScalarType::UInt64 => Datum::UInt64(u64::arbitrary(u)?),
        SqlScalarType::Float64 => Datum::Float64(f64::arbitrary(u)?.into()),
        SqlScalarType::Numeric { .. } => Datum::Numeric(gen_numeric(u)?),
        SqlScalarType::Date => Datum::Date(
            Date::from_pg_epoch(i32::arbitrary(u)?).unwrap_or_else(|_| Date::from_pg_epoch(0).unwrap()),
        ),
        SqlScalarType::Time => {
            let secs = u.int_in_range(0u32..=86_399)?;
            let nanos = u.int_in_range(0u32..=999_999_999)?;
            Datum::Time(NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos).unwrap())
        }
        SqlScalarType::Timestamp { .. } => Datum::Timestamp(gen_naive_ts(u)?),
        SqlScalarType::TimestampTz { .. } => Datum::TimestampTz(gen_utc_ts(u)?),
        SqlScalarType::Interval => Datum::Interval(Interval::new(
            i32::arbitrary(u)?,
            i32::arbitrary(u)?,
            i64::arbitrary(u)?,
        )),
        SqlScalarType::MzTimestamp => Datum::MzTimestamp(Timestamp::from(u64::arbitrary(u)?)),
        // gen_simple_scalar_type only produces the types above.
        _ => Datum::Null,
    })
}

/// Push a `Range` of the given discrete element type. Each bound is
/// independently infinite (`Datum::Null` => infinite via `RangeBound::new`) or
/// a finite element value, and independently inclusive/exclusive. Empty ranges
/// are reached when `push_range`'s canonicalization collapses the bounds.
fn push_range(
    packer: &mut mz_repr::RowPacker,
    u: &mut Unstructured,
    element_type: &SqlScalarType,
) -> arbitrary::Result<()> {
    // ~1/8 of the time emit the explicitly-empty range.
    if u.ratio(1u8, 8u8)? {
        let _ = packer.push_range(Range { inner: None });
        return Ok(());
    }
    let lower_d = if bool::arbitrary(u)? {
        Datum::Null
    } else {
        gen_scalar_datum(u, element_type)?
    };
    let upper_d = if bool::arbitrary(u)? {
        Datum::Null
    } else {
        gen_scalar_datum(u, element_type)?
    };
    let lower: RangeLowerBound<Datum> = RangeBound::new(lower_d, bool::arbitrary(u)?);
    let upper: RangeUpperBound<Datum> = RangeBound::new(upper_d, bool::arbitrary(u)?);
    // An out-of-order range (lower > upper) is an `InvalidRangeError`. On
    // failure just fall back to the empty range so the column stays valid.
    if packer
        .push_range(Range::new(Some((lower, upper))))
        .is_err()
    {
        let _ = packer.push_range(Range { inner: None });
    }
    Ok(())
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let ncols = u.int_in_range(1usize..=6)?;
    let cols: Vec<SqlColumnType> = (0..ncols)
        .map(|_| {
            Ok(SqlColumnType {
                scalar_type: gen_scalar_type(u)?,
                nullable: bool::arbitrary(u)?,
            })
        })
        .collect::<arbitrary::Result<_>>()?;

    let mut builder = RelationDesc::builder();
    for (i, c) in cols.iter().enumerate() {
        builder = builder.with_column(format!("c{i}"), c.clone());
    }
    let desc = builder.finish();

    let nrows = u.int_in_range(0usize..=10)?;
    let mut rows = Vec::with_capacity(nrows);
    for _ in 0..nrows {
        let mut row = Row::default();
        {
            let mut packer = row.packer();
            for c in &cols {
                push_datum(&mut packer, u, &c.scalar_type, c.nullable)?;
            }
        }
        rows.push(row);
    }

    let Ok(mut encoder) = <RelationDesc as Schema<Row>>::encoder(&desc) else {
        return Ok(());
    };
    for row in &rows {
        encoder.append(row);
    }
    let col = encoder.finish();
    let Ok(decoder) = <RelationDesc as Schema<Row>>::decoder(&desc, col) else {
        return Ok(());
    };
    for (i, orig) in rows.iter().enumerate() {
        let mut out = Row::default();
        decoder.decode(i, &mut out);
        assert_eq!(
            orig, &out,
            "Row changed across Arrow columnar round trip (desc = {desc:?})"
        );
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
