// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the upsert state machine's value consolidation
//! (`StateValue::merge_update` + `ensure_decoded`). Upsert collapses a key's
//! `(value, diff)` history into its current value using a XOR/length/checksum
//! accumulator (so it never has to keep the unconsolidated history in memory),
//! then `ensure_decoded` `bincode`-decodes the accumulated `value_xor` back into
//! a `Row`. The *values* come from untrusted source data, so this exercises that
//! accumulate-then-decode machinery against arbitrary `Row`s.
//!
//! The *diffs*, by contrast, are Materialize-controlled: a well-formed upsert
//! collection has, per key, zero-or-more canceling `(prev, +1) (prev, -1)` pairs
//! and at most one `(cur, +1)`. We only generate that shape (so `diff_sum` stays
//! in `{0, 1}` and we never trip the intentional "invalid upsert state" invariant
//! that guards against impossible diff sums), shuffle the updates, merge them,
//! and check the consolidated result:
//!
//!   * with a current value, it must decode back to exactly that value (the
//!     canceling pairs must XOR/sum away regardless of order or of colliding
//!     with each other or the current value).
//!   * with no current value, the key must consolidate to absent (a tombstone).
//!
//! A panic in `merge_update`/`ensure_decoded`, or a mismatch, is a finding.
//!
//! The values (`prev`s and the `cur`) are drawn from the tricky-encoding datum
//! space (`Numeric`, `Date`, `Timestamp`/`TimestampTz`, `Interval`, `Uuid`,
//! `MzTimestamp`, `Bytes`, long strings, `MzAclItem`, and the nested composites
//! `Array`/`List`/`Map`/`Range`), and a fraction are `Err(UpsertError)` values
//! (real upsert sources stage errored values alongside `Ok` rows). This stresses
//! the XOR/len/checksum accumulator over bincode payloads of widely varying
//! length, which is exactly what the accumulator's resize/zero-extend logic is
//! sensitive to.

#![no_main]

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
use mz_repr::{Datum, Diff, GlobalId, Row, RowPacker, Timestamp};
use mz_storage::fuzz_exports::{StateValue, UpsertValue, upsert_bincode_opts};
use mz_storage_types::errors::{
    DecodeError, DecodeErrorKind, UpsertError, UpsertNullKeyError, UpsertValueError,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

/// An arbitrary in-range `NaiveDateTime`, or `None` if out of the supported range.
fn gen_naive_dt(u: &mut Unstructured) -> arbitrary::Result<Option<NaiveDateTime>> {
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

/// Push a single arbitrary scalar (non-composite) datum from the tricky space.
fn push_scalar(packer: &mut RowPacker, u: &mut Unstructured) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=14)? {
        0 => packer.push(Datum::Null),
        1 => packer.push(Datum::Int32(i32::arbitrary(u)?)),
        2 => packer.push(Datum::Int64(i64::arbitrary(u)?)),
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
                packer.push(Datum::Timestamp(
                    CheckedTimestamp::from_timestamplike(dt).unwrap(),
                ));
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
        _ => {
            // String length that may spill past the 1-byte tiny encoding.
            let len = u.int_in_range(0usize..=300)?;
            let mut s = String::with_capacity(len);
            for _ in 0..len {
                s.push(if bool::arbitrary(u)? { 'a' } else { 'é' });
            }
            packer.push(Datum::String(&s));
        }
    }
    Ok(())
}

/// A vector of `'static`-safe scalar datums for use as composite elements.
fn gen_scalar_vec(u: &mut Unstructured, n: usize) -> arbitrary::Result<Vec<Datum<'static>>> {
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
    if packer.push_range(range).is_err() {
        packer.push(Datum::Int32(lo));
    }
    Ok(())
}

/// Push one datum: usually a scalar, occasionally a nested composite whose
/// elements are themselves scalars. Always produces a valid `Row`.
fn push_datum(packer: &mut RowPacker, u: &mut Unstructured) -> arbitrary::Result<()> {
    match u.int_in_range(0u8..=9)? {
        0..=5 => push_scalar(packer, u),
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
        7 => {
            let n = u.int_in_range(0usize..=4)?;
            let datums = gen_scalar_vec(u, n)?;
            packer.push_list(datums.iter());
            Ok(())
        }
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
        _ => push_range(packer, u),
    }
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

/// An arbitrary `UpsertValue`: usually an `Ok(Row)`, occasionally an
/// `Err(UpsertError)` spanning the whole variant space (real sources stage
/// errored values alongside good rows).
fn gen_value(u: &mut Unstructured) -> arbitrary::Result<UpsertValue> {
    if u.ratio(1u8, 5u8)? {
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
    let bincode_opts = upsert_bincode_opts();
    let mut bincode_buffer = Vec::new();

    // Build a well-formed history: canceling `(prev, +1) (prev, -1)` pairs plus
    // at most one current `(cur, +1)`.
    let mut updates: Vec<(UpsertValue, Diff)> = Vec::new();
    let n_pairs = u.int_in_range(0usize..=6)?;
    for _ in 0..n_pairs {
        let prev: UpsertValue = gen_value(u)?;
        updates.push((prev.clone(), Diff::ONE));
        updates.push((prev, -Diff::ONE));
    }
    let current: Option<UpsertValue> = if bool::arbitrary(u)? {
        Some(gen_value(u)?)
    } else {
        None
    };
    if let Some(cur) = &current {
        updates.push((cur.clone(), Diff::ONE));
    }

    // Shuffle (Fisher-Yates). Consolidation must be order-independent.
    for i in (1..updates.len()).rev() {
        let j = u.int_in_range(0..=i)?;
        updates.swap(i, j);
    }

    let mut state = StateValue::<(), ()>::default();
    for (value, diff) in updates {
        state.merge_update(value, diff, bincode_opts, &mut bincode_buffer);
    }
    state.ensure_decoded(bincode_opts, GlobalId::User(0), None);
    let decoded = state.into_decoded();

    match (current, decoded.finalized) {
        (Some(cur), Some(got)) => {
            assert_eq!(got, cur, "consolidation decoded the wrong current value")
        }
        (Some(_), None) => {
            panic!("consolidation dropped the current value")
        }
        (None, None) => {}
        (None, other) => {
            panic!("consolidation invented a value from canceling pairs: {other:?}")
        }
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
