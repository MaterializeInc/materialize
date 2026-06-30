// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `mz_interchange::json::encode_datums_as_json` formats a `Row`'s
//! datums as JSON, the output path of a `FORMAT JSON` Kafka sink. (The JSON
//! *source* decoder is just `Jsonb::from_slice`, already covered by
//! `repr::jsonb_from_slice`.) Each datum is rendered per its column type, which
//! has real per-type logic: floats (a JSON `Number` can't be NaN/Infinity),
//! numerics, intervals, timestamps, dates, bytes. A panic there, or a value
//! that can't be serialized, corrupts/halts a sink, so encoding then serializing
//! must never panic for any well-typed row.
//!
//! Beyond scalars, we generate *composite* column types (`List`, `Map`,
//! `Record`, multi-dimensional `Array`, and `Jsonb`) plus the remaining scalar
//! shapes with their own encode logic (`Uuid`, `Char`/`VarChar` padding,
//! `Range`, `MzAclItem`/`AclItem`). These drive the recursive encode paths that
//! a scalar-only schema never reaches: `encode_array`'s dimension walk, the
//! `Record`/`Map` field iteration (a `zip_eq` against the column type), and the
//! `Jsonb -> serde_json` conversion. A `Row` ultimately comes from a source, so
//! the *values* are arbitrary (NaN/Infinity floats, huge collections, deeply
//! nested records), but they must be *well-typed* against the schema: we
//! generate the type and a structurally matching datum in lockstep, so any panic
//! is in the encoder, not a type/datum mismatch.

#![no_main]

use chrono::{DateTime, NaiveTime, Utc};
use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_interchange::json::encode_datums_as_json;
use mz_repr::adt::char::CharLength;
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::jsonb::JsonbPacker;
use mz_repr::adt::mz_acl_item::{AclItem, MzAclItem};
use mz_repr::adt::range::Range;
use mz_repr::adt::system::Oid;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::adt::varchar::VarCharMaxLength;
use mz_repr::role_id::RoleId;
use mz_repr::strconv::parse_numeric;
use mz_repr::{ColumnName, Datum, Row, RowPacker, SqlColumnType, SqlScalarType, Timestamp};
use uuid::Uuid;

/// A generated column type. Scalars are the leaves. The composite variants nest
/// (with a depth bound) to drive the recursive encode paths. We keep the type
/// structured so that `push_typed` can pack a value that matches it exactly.
enum GType {
    Scalar(SqlScalarType),
    /// SQL `Jsonb`, packed via `JsonbPacker`, rendered by `to_serde_json`.
    Jsonb,
    /// `LIST` of `(nullable element, element type)`.
    List(bool, Box<GType>),
    /// `MAP` of string -> `(nullable value, value type)`.
    Map(bool, Box<GType>),
    /// `RECORD` of `(field nullable, field type)` columns. Encoding `zip_eq`s
    /// the field types against the packed list, so they must line up.
    Record(Vec<(bool, GType)>),
    /// Multi-dimensional `ARRAY` of a scalar element. `dims` are the per-axis
    /// lengths. The total element count is their product.
    Array(Vec<usize>, SqlScalarType),
}

fn gen_naive_ts(
    u: &mut Unstructured,
) -> arbitrary::Result<CheckedTimestamp<chrono::NaiveDateTime>> {
    let secs = u.int_in_range(-8_000_000_000_000i64..=8_000_000_000_000)?;
    let nanos = u.int_in_range(0u32..=999_999_999)?;
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

/// Generate a scalar type (a leaf of the composite tree, and the element type of
/// arrays). Every type here is handled by `push_scalar`.
fn gen_scalar_type(u: &mut Unstructured) -> arbitrary::Result<SqlScalarType> {
    Ok(match u.int_in_range(0u8..=24)? {
        0 => SqlScalarType::Bool,
        1 => SqlScalarType::Int16,
        2 => SqlScalarType::Int32,
        3 => SqlScalarType::Int64,
        4 => SqlScalarType::UInt16,
        5 => SqlScalarType::UInt32,
        6 => SqlScalarType::UInt64,
        7 => SqlScalarType::PgLegacyChar,
        8 => SqlScalarType::Float32,
        9 => SqlScalarType::Float64,
        10 => SqlScalarType::String,
        11 => SqlScalarType::Bytes,
        12 => SqlScalarType::Numeric { max_scale: None },
        13 => SqlScalarType::Date,
        14 => SqlScalarType::Time,
        15 => SqlScalarType::Timestamp { precision: None },
        16 => SqlScalarType::TimestampTz { precision: None },
        17 => SqlScalarType::Interval,
        18 => SqlScalarType::MzTimestamp,
        19 => SqlScalarType::Uuid,
        20 => {
            let length = CharLength::try_from(i64::from(u.int_in_range(1u8..=12)?)).ok();
            SqlScalarType::Char { length }
        }
        21 => {
            let max_length = VarCharMaxLength::try_from(i64::from(u.int_in_range(1u8..=12)?)).ok();
            SqlScalarType::VarChar { max_length }
        }
        // Range over Int32. `unwrap_range().to_string()` is the only logic.
        22 => SqlScalarType::Range {
            element_type: Box::new(SqlScalarType::Int32),
        },
        23 => SqlScalarType::MzAclItem,
        _ => SqlScalarType::AclItem,
    })
}

/// Generate a (possibly composite) column type, bounded by `depth`.
fn gen_type(u: &mut Unstructured, depth: u32) -> arbitrary::Result<GType> {
    if depth == 0 || u.is_empty() {
        return Ok(match u.int_in_range(0u8..=1)? {
            0 => GType::Jsonb,
            _ => GType::Scalar(gen_scalar_type(u)?),
        });
    }
    Ok(match u.int_in_range(0u8..=6)? {
        0 | 1 => GType::Scalar(gen_scalar_type(u)?),
        2 => GType::Jsonb,
        3 => GType::List(bool::arbitrary(u)?, Box::new(gen_type(u, depth - 1)?)),
        4 => GType::Map(bool::arbitrary(u)?, Box::new(gen_type(u, depth - 1)?)),
        5 => {
            let n = u.int_in_range(0u8..=4)?;
            let mut fields = Vec::with_capacity(n.into());
            for _ in 0..n {
                fields.push((bool::arbitrary(u)?, gen_type(u, depth - 1)?));
            }
            GType::Record(fields)
        }
        // Arrays nest only scalars (Materialize arrays don't hold collections),
        // but can have multiple dimensions.
        _ => {
            let ndims = u.int_in_range(1usize..=3)?;
            let mut dims = Vec::with_capacity(ndims);
            for _ in 0..ndims {
                dims.push(u.int_in_range(0usize..=3)?);
            }
            GType::Array(dims, gen_scalar_type(u)?)
        }
    })
}

/// The `SqlScalarType` corresponding to a `GType`, for the column schema.
fn to_scalar_type(ty: &GType) -> SqlScalarType {
    match ty {
        GType::Scalar(s) => s.clone(),
        GType::Jsonb => SqlScalarType::Jsonb,
        GType::List(_, elem) => SqlScalarType::List {
            element_type: Box::new(to_scalar_type(elem)),
            custom_id: None,
        },
        GType::Map(_, value) => SqlScalarType::Map {
            value_type: Box::new(to_scalar_type(value)),
            custom_id: None,
        },
        GType::Record(fields) => SqlScalarType::Record {
            fields: fields
                .iter()
                .enumerate()
                .map(|(i, (nullable, ty))| {
                    (
                        ColumnName::from(format!("f{i}")),
                        SqlColumnType {
                            scalar_type: to_scalar_type(ty),
                            nullable: *nullable,
                        },
                    )
                })
                .collect(),
            custom_id: None,
        },
        GType::Array(_, elem) => SqlScalarType::Array(Box::new(elem.clone())),
    }
}

/// Build a small, valid JSON string of bounded depth for the `Jsonb` path.
fn gen_json_text(u: &mut Unstructured) -> arbitrary::Result<String> {
    Ok(match u.int_in_range(0u8..=7)? {
        0 => "null".into(),
        1 => "true".into(),
        2 => format!("{}", u.arbitrary::<i64>()?),
        3 => format!("{}.25", u.arbitrary::<i32>()?),
        4 => "\"s\"".into(),
        5 => "[1,2,3]".into(),
        6 => "{\"a\":1,\"b\":[true,null]}".into(),
        _ => "[]".into(),
    })
}

/// Push one scalar datum of `ty` (or NULL when allowed). Out-of-range
/// constructed values fall back to a valid datum so the row always matches the
/// column type.
fn push_scalar(
    packer: &mut RowPacker,
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
        // `f{32,64}::arbitrary` produces NaN/Infinity, which a JSON Number cannot
        // represent, exactly the encoding edge we want to exercise.
        SqlScalarType::Float32 => packer.push(Datum::Float32(f32::arbitrary(u)?.into())),
        SqlScalarType::Float64 => packer.push(Datum::Float64(f64::arbitrary(u)?.into())),
        SqlScalarType::String
        | SqlScalarType::VarChar { .. }
        | SqlScalarType::Char { .. } => packer.push(Datum::String(<&str>::arbitrary(u)?)),
        SqlScalarType::Bytes => packer.push(Datum::Bytes(<&[u8]>::arbitrary(u)?)),
        SqlScalarType::Numeric { .. } => {
            let s = format!("{}.{}", i64::arbitrary(u)?, u32::arbitrary(u)?);
            let n = parse_numeric(&s).unwrap_or_else(|_| parse_numeric("0").unwrap());
            packer.push(Datum::Numeric(n));
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
        SqlScalarType::Uuid => packer.push(Datum::Uuid(Uuid::from_u128(u.arbitrary::<u128>()?))),
        // The empty range is always valid and its `to_string` is exercised.
        SqlScalarType::Range { .. } => packer.push(Datum::Range(Range { inner: None })),
        SqlScalarType::MzAclItem => {
            let item = MzAclItem::empty(RoleId::User(u.arbitrary::<u64>()?), RoleId::Public);
            packer.push(Datum::MzAclItem(item));
        }
        SqlScalarType::AclItem => {
            let item = AclItem::empty(Oid(u.arbitrary::<u32>()?), Oid(u.arbitrary::<u32>()?));
            packer.push(Datum::AclItem(item));
        }
        // `gen_scalar_type` never produces other types.
        _ => packer.push(Datum::Null),
    }
    Ok(())
}

/// Push a value of `ty` (or NULL when allowed) into `packer`, recursing into
/// composites so the packed structure matches `to_scalar_type(ty)` exactly.
fn push_typed(
    packer: &mut RowPacker,
    u: &mut Unstructured,
    ty: &GType,
    nullable: bool,
) -> arbitrary::Result<()> {
    match ty {
        GType::Scalar(s) => push_scalar(packer, u, s, nullable)?,
        GType::Jsonb => {
            if nullable && u.ratio(1u8, 8u8)? {
                packer.push(Datum::Null);
            } else {
                let text = gen_json_text(u)?;
                // The text is always valid JSON by construction.
                JsonbPacker::new(packer)
                    .pack_str(&text)
                    .expect("generated valid json");
            }
        }
        GType::List(elem_nullable, elem) => {
            if nullable && u.ratio(1u8, 8u8)? {
                packer.push(Datum::Null);
            } else {
                let n = u.int_in_range(0usize..=4)?;
                packer.push_list_with(|packer| {
                    for _ in 0..n {
                        push_typed(packer, u, elem, *elem_nullable)?;
                    }
                    Ok::<_, arbitrary::Error>(())
                })?;
            }
        }
        GType::Map(value_nullable, value) => {
            if nullable && u.ratio(1u8, 8u8)? {
                packer.push(Datum::Null);
            } else {
                let n = u.int_in_range(0usize..=4)?;
                packer.push_dict_with(|packer| {
                    // Keys must be pushed in ascending order and be unique. The
                    // zero-padded counter is lexically ascending.
                    for i in 0..n {
                        packer.push(Datum::String(&format!("k{i:03}")));
                        push_typed(packer, u, value, *value_nullable)?;
                    }
                    Ok::<_, arbitrary::Error>(())
                })?;
            }
        }
        GType::Record(fields) => {
            if nullable && u.ratio(1u8, 8u8)? {
                packer.push(Datum::Null);
            } else {
                packer.push_list_with(|packer| {
                    for (field_nullable, field_ty) in fields {
                        push_typed(packer, u, field_ty, *field_nullable)?;
                    }
                    Ok::<_, arbitrary::Error>(())
                })?;
            }
        }
        GType::Array(dims, elem) => {
            if nullable && u.ratio(1u8, 8u8)? {
                packer.push(Datum::Null);
                return Ok(());
            }
            // `try_push_array` requires exactly `prod(dims)` elements. A
            // zero-length axis collapses the whole array to empty.
            let nelements: usize = dims.iter().product();
            let array_dims: Vec<mz_repr::adt::array::ArrayDimension> = dims
                .iter()
                .map(|&length| mz_repr::adt::array::ArrayDimension {
                    lower_bound: 1,
                    length,
                })
                .collect();
            // Build the elements into a scratch row first (array elements are
            // always nullable in the encoder).
            let mut scratch = Row::default();
            {
                let mut sp = scratch.packer();
                for _ in 0..nelements {
                    push_scalar(&mut sp, u, elem, true)?;
                }
            }
            let elems: Vec<Datum> = scratch.iter().collect();
            // The dims and element count match by construction, so this can't
            // return an error. If it somehow does, fall back to an empty array.
            if packer.try_push_array(&array_dims, elems).is_err() {
                packer.try_push_array(&[], std::iter::empty::<Datum>()).unwrap();
            }
        }
    }
    Ok(())
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let ncols = u.int_in_range(1usize..=6)?;
    let mut types: Vec<(bool, GType)> = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        types.push((bool::arbitrary(u)?, gen_type(u, 3)?));
    }

    let columns: Vec<(ColumnName, SqlColumnType)> = types
        .iter()
        .enumerate()
        .map(|(i, (nullable, ty))| {
            (
                ColumnName::from(format!("c{i}")),
                SqlColumnType {
                    scalar_type: to_scalar_type(ty),
                    nullable: *nullable,
                },
            )
        })
        .collect();

    let mut row = Row::default();
    {
        let mut packer = row.packer();
        for (nullable, ty) in &types {
            push_typed(&mut packer, u, ty, *nullable)?;
        }
    }

    // Encode the row's datums as JSON (the sink path) and serialize the result.
    // Neither step may panic for any well-typed row.
    let value = encode_datums_as_json(row.iter(), &columns);
    let _ = serde_json::to_vec(&value);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
