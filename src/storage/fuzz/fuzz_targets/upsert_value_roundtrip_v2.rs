// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the v2 upsert operator's value encoding round-trips.
//!
//! Where the v1 upsert state machine consolidates with the XOR/checksum
//! accumulator (see `upsert_consolidate`), the v2 operator
//! (`upsert_continual_feedback_v2`) stores values in a differential
//! `ValRowSpine` as packed `Row` bytes. An [`UpsertValue`] is
//! `Result<Row, Box<UpsertError>>`, so it is folded into a single tagged `Row`
//! by `upsert_value_to_row` (tag `0` + the row's datums for `Ok`, tag `1` + the
//! bincode of the error for `Err`) and recovered from a spine cursor's
//! `DatumSeq` by `datum_seq_to_upsert_value`. A bug in that encode/decode pair
//! silently corrupts the value a v2 upsert source produces, so the round-trip
//! must be the identity for any value (the values come from untrusted source
//! data).
//!
//! We push the encoded `Row` through a real `DatumContainer` (the same byte
//! storage the spine uses) and read it back as a `DatumSeq`, exactly as the
//! operator does, then assert `decode(encode(v)) == v`.
//!
//! Both arms are generated with depth:
//!
//!   * the `Ok` arm draws from the *tricky-encoding* datum space (`Numeric`,
//!     `Date`, `Timestamp`/`TimestampTz`, `Interval`, `Uuid`, `MzTimestamp`,
//!     `Bytes`, long strings, `MzAclItem`, and the nested composites `Array`,
//!     `List`, `Map`, and `Range`), so the tag-prefixed re-pack/extend round-trip
//!     is exercised over every `Row` tag and over multi-byte length encodings,
//!     not just the trivial scalars.
//!   * the `Err` arm covers the whole [`UpsertError`] variant space
//!     (`KeyDecode`, `Value`, `NullKey`) with non-trivial `DecodeError` payloads
//!     and a real decoded key `Row`, so the tag-`1` bincode round-trip is
//!     stressed across all error shapes, not just `NullKey`.

#![no_main]

use differential_dataflow::trace::implementations::BatchContainer;
use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_repr::adt::array::ArrayDimension;
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::adt::numeric::Numeric;
use mz_repr::adt::range::{Range, RangeBound, RangeInner};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::role_id::RoleId;
use mz_repr::{Datum, Row, RowPacker, Timestamp};
use mz_row_spine::DatumContainer;
use mz_storage::fuzz_exports::{UpsertValue, datum_seq_to_upsert_value, upsert_value_to_row};
use mz_storage_types::errors::{
    DecodeError, DecodeErrorKind, UpsertError, UpsertNullKeyError, UpsertValueError,
};
use timely::container::PushInto;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

/// Push a single arbitrary *scalar* (non-composite) datum, drawn from the
/// tricky-encoding space so we vary the `Row` tag and the bincode length.
fn push_scalar(packer: &mut RowPacker, u: &mut Unstructured) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=15)? {
        0 => packer.push(Datum::Null),
        1 => packer.push(Datum::Int32(i32::arbitrary(u)?)),
        2 => packer.push(Datum::Int64(i64::arbitrary(u)?)),
        // `upsert_value_to_row` tags `Ok` values with a leading `UInt8(0)`, so
        // include `UInt8` data datums so a value can't be confused with the tag.
        3 => packer.push(Datum::UInt8(u8::arbitrary(u)?)),
        4 => packer.push(if bool::arbitrary(u)? {
            Datum::True
        } else {
            Datum::False
        }),
        5 => packer.push(Datum::from(Numeric::from(i64::arbitrary(u)?))),
        6 => {
            let days = u.int_in_range(Date::LOW_DAYS..=Date::HIGH_DAYS)?;
            packer.push(Datum::Date(Date::from_pg_epoch(days).unwrap()));
        }
        7 => {
            if let Some(dt) = gen_naive_dt(u)? {
                packer.push(Datum::Timestamp(CheckedTimestamp::from_timestamplike(dt).unwrap()));
            } else {
                packer.push(Datum::Null);
            }
        }
        8 => {
            if let Some(dt) = gen_naive_dt(u)? {
                let utc = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
                packer.push(Datum::TimestampTz(
                    CheckedTimestamp::from_timestamplike(utc).unwrap(),
                ));
            } else {
                packer.push(Datum::Null);
            }
        }
        9 => packer.push(Datum::Interval(Interval::new(
            i32::arbitrary(u)?,
            i32::arbitrary(u)?,
            i64::arbitrary(u)?,
        ))),
        10 => packer.push(Datum::Uuid(uuid::Uuid::from_bytes(
            <[u8; 16]>::arbitrary(u)?,
        ))),
        11 => packer.push(Datum::MzTimestamp(Timestamp::from(u64::arbitrary(u)?))),
        12 => {
            let len = u.int_in_range(0usize..=20)?;
            let bytes = u.bytes(len)?;
            packer.push(Datum::Bytes(bytes));
        }
        13 => packer.push(Datum::MzAclItem(MzAclItem {
            grantee: gen_role_id(u)?,
            grantor: gen_role_id(u)?,
            acl_mode: AclMode::from_bits_truncate(u64::arbitrary(u)?),
        })),
        14 => {
            // A string whose length spills past the 1-byte tiny encoding, so the
            // multi-byte length path is exercised. Mixes ASCII and multi-byte.
            let len = u.int_in_range(0usize..=300)?;
            let mut s = String::with_capacity(len);
            for _ in 0..len {
                s.push(if bool::arbitrary(u)? { 'a' } else { 'é' });
            }
            packer.push(Datum::String(&s));
        }
        _ => {
            let len = u.int_in_range(0usize..=6)?;
            let mut s = String::with_capacity(len);
            for _ in 0..len {
                s.push(if bool::arbitrary(u)? { 'a' } else { 'é' });
            }
            packer.push(Datum::String(&s));
        }
    }
    Ok(())
}

/// An arbitrary in-range `NaiveDateTime`, or `None` if out of the supported range.
fn gen_naive_dt(u: &mut Unstructured) -> arbitrary::Result<Option<NaiveDateTime>> {
    // Stay comfortably inside chrono's representable range.
    let year = u.int_in_range(1i32..=9999)?;
    let ord = u.int_in_range(1u32..=365)?;
    let secs = u.int_in_range(0u32..=86_399)?;
    let nanos = u.int_in_range(0u32..=999_999_999)?;
    let date = NaiveDate::from_yo_opt(year, ord);
    let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos);
    Ok(match (date, time) {
        (Some(d), Some(t)) => Some(NaiveDateTime::new(d, t)),
        _ => None,
    })
}

fn gen_role_id(u: &mut Unstructured) -> arbitrary::Result<RoleId> {
    Ok(match u.int_in_range(0u8..=2)? {
        0 => RoleId::User(u64::arbitrary(u)?),
        1 => RoleId::System(u64::arbitrary(u)?),
        _ => RoleId::Public,
    })
}

/// Push one datum: usually a scalar, occasionally a nested composite
/// (`Array`/`List`/`Map`/`Range`) whose elements are themselves scalars. Falls
/// back to a scalar if a composite can't be constructed for the drawn shape (an
/// invalid range, e.g.), so the caller always ends up with a valid `Row`.
fn push_datum(packer: &mut RowPacker, u: &mut Unstructured) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=9)? {
        // Mostly scalars.
        0..=5 => push_scalar(packer, u),
        // A 1-D Array of scalars.
        6 => {
            let n = u.int_in_range(0usize..=4)?;
            let datums = gen_scalar_vec(u, n)?;
            let dims = [ArrayDimension {
                lower_bound: 1,
                length: datums.len(),
            }];
            packer
                .try_push_array(&dims, datums.iter())
                .expect("single-dimension array is always valid");
            Ok(())
        }
        // A List of scalars.
        7 => {
            let n = u.int_in_range(0usize..=4)?;
            let datums = gen_scalar_vec(u, n)?;
            packer.push_list(datums.iter());
            Ok(())
        }
        // A Map (dict) with sorted, unique string keys.
        8 => {
            let n = u.int_in_range(0usize..=4)?;
            let mut keys: Vec<String> = Vec::with_capacity(n);
            for _ in 0..n {
                let len = u.int_in_range(0usize..=4)?;
                let mut s = String::with_capacity(len);
                for _ in 0..len {
                    s.push(if bool::arbitrary(u)? { 'a' } else { 'b' });
                }
                keys.push(s);
            }
            keys.sort();
            keys.dedup();
            let vals = gen_scalar_vec(u, keys.len())?;
            packer.push_dict(keys.iter().map(String::as_str).zip(vals.iter().copied()));
            Ok(())
        }
        // A Range over Int32 bounds.
        _ => {
            push_range(packer, u)?;
            Ok(())
        }
    }
}

/// Generate a vector of `n` scalar datums by packing them into a scratch row and
/// borrowing them back. (Composite packers need an iterator of `Datum`.)
fn gen_scalar_vec<'a>(
    u: &mut Unstructured,
    n: usize,
) -> arbitrary::Result<Vec<Datum<'static>>> {
    // We only emit `Copy`, `'static`-safe scalar datums here so the returned
    // `Datum`s don't borrow from a scratch buffer. That covers ints, bools,
    // numerics, dates, timestamps, intervals, uuids, and mz-timestamps.
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(match u.int_in_range(0u8..=7)? {
            0 => Datum::Null,
            1 => Datum::Int32(i32::arbitrary(u)?),
            2 => Datum::Int64(i64::arbitrary(u)?),
            3 => Datum::UInt8(u8::arbitrary(u)?),
            4 => Datum::from(Numeric::from(i64::arbitrary(u)?)),
            5 => Datum::Interval(Interval::new(
                i32::arbitrary(u)?,
                i32::arbitrary(u)?,
                i64::arbitrary(u)?,
            )),
            6 => Datum::Uuid(uuid::Uuid::from_bytes(<[u8; 16]>::arbitrary(u)?)),
            _ => Datum::MzTimestamp(Timestamp::from(u64::arbitrary(u)?)),
        });
    }
    Ok(out)
}

/// Push a `Range` over `Int32` bounds (possibly empty or with infinite bounds).
fn push_range(packer: &mut RowPacker, u: &mut Unstructured) -> arbitrary::Result<()> {
    if bool::arbitrary(u)? {
        // Empty range.
        packer
            .push_range(Range { inner: None })
            .expect("empty range is valid");
        return Ok(());
    }
    let lo = i32::arbitrary(u)?;
    let hi = i32::arbitrary(u)?;
    let (lo, hi) = if lo <= hi { (lo, hi) } else { (hi, lo) };
    let lower = RangeBound {
        inclusive: bool::arbitrary(u)?,
        bound: if bool::arbitrary(u)? {
            Some(Datum::Int32(lo))
        } else {
            None
        },
    };
    let upper = RangeBound {
        inclusive: bool::arbitrary(u)?,
        bound: if bool::arbitrary(u)? {
            Some(Datum::Int32(hi))
        } else {
            None
        },
    };
    let range = Range {
        inner: Some(RangeInner { lower, upper }),
    };
    // `push_range` canonicalizes and may reject (e.g. a degenerate empty range).
    // On rejection just push a plain scalar so the row is still valid.
    if packer.push_range(range).is_err() {
        packer.push(Datum::Int32(lo));
    }
    Ok(())
}

/// A small arbitrary `Row` (the untrusted source value), covering the tricky
/// scalar and nested-composite datum space.
fn gen_row(u: &mut Unstructured) -> arbitrary::Result<Row> {
    let n = u.int_in_range(0usize..=5)?;
    let mut row = Row::default();
    let mut packer = row.packer();
    for _ in 0..n {
        push_datum(&mut packer, u)?;
    }
    drop(packer);
    Ok(row)
}

/// An arbitrary `DecodeError` payload for the error-arm variants.
fn gen_decode_error(u: &mut Unstructured) -> arbitrary::Result<DecodeError> {
    let msg = String::arbitrary(u)?;
    let kind = if bool::arbitrary(u)? {
        DecodeErrorKind::Text(msg.into_boxed_str())
    } else {
        DecodeErrorKind::Bytes(msg.into_boxed_str())
    };
    let raw = Vec::<u8>::arbitrary(u)?;
    Ok(DecodeError { kind, raw })
}

fn gen_value(u: &mut Unstructured) -> arbitrary::Result<UpsertValue> {
    if u.ratio(1u8, 4u8)? {
        // Exercise the error arm (tag `1` + bincode of the error) across the
        // whole `UpsertError` variant space.
        let err = match u.int_in_range(0u8..=2)? {
            0 => UpsertError::KeyDecode(gen_decode_error(u)?),
            1 => UpsertError::Value(UpsertValueError {
                inner: gen_decode_error(u)?,
                for_key: gen_row(u)?,
            }),
            _ => UpsertError::NullKey(UpsertNullKeyError),
        };
        Ok(Err(Box::new(err)))
    } else {
        Ok(Ok(gen_row(u)?))
    }
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let value: UpsertValue = gen_value(u)?;

    // Encode, store in the spine's byte container, read back, decode.
    let row = upsert_value_to_row(&value);
    let mut container = DatumContainer::with_capacity(1);
    container.push_into(row);
    let decoded = datum_seq_to_upsert_value(container.index(0));

    assert_eq!(value, decoded, "v2 upsert value encoding did not round-trip");
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
