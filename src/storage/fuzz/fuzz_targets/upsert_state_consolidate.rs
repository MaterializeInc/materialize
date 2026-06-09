// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the upsert state machine's snapshot consolidation
//! (`UpsertState::consolidate_chunk` + `multi_get`) over the in-memory backend.
//!
//! Where `upsert_consolidate` exercises the XOR/checksum accumulator in
//! isolation for a single key, this drives the whole state machine: a chunk of
//! differential `(key, value, diff)` updates spanning several keys is fed to
//! `consolidate_chunk` (which re-indexes them through `StateValue` and the
//! backend), the snapshot is completed, and `multi_get` reads each key back.
//!
//! The diffs are Materialize-controlled, so per key we generate a *well-formed*
//! history — zero-or-more canceling `(prev, +1) (prev, -1)` pairs plus at most
//! one current `(cur, +1)` — and shuffle the whole multi-key chunk. After
//! consolidation each key must read back exactly its current value (or absent
//! if it had none): the canceling pairs must XOR/sum away across the chunk
//! regardless of order or of colliding with each other. A panic in
//! `consolidate_chunk`/`multi_get`, or a wrong/lost/invented value, is a finding.
//!
//! The values are drawn from the tricky-encoding datum space — `Numeric`,
//! `Date`, `Timestamp`/`TimestampTz`, `Interval`, `Uuid`, `MzTimestamp`,
//! `Bytes`, long strings, `MzAclItem`, and the nested composites
//! `Array`/`List`/`Map`/`Range` — and a fraction are `Err(UpsertError)` values
//! (real upsert sources stage errored values alongside `Ok` rows), so the
//! state-machine consolidation path is exercised over bincode payloads of widely
//! varying length and over both value arms.

#![no_main]

use std::collections::HashMap;
use std::sync::OnceLock;

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
use mz_storage::upsert::types::{upsert_bincode_opts, FuzzUpsertParts, UpsertValueAndSize};
use mz_storage::upsert::{UpsertKey, UpsertValue};
use mz_storage_types::errors::{
    DecodeError, DecodeErrorKind, UpsertError, UpsertNullKeyError, UpsertValueError,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

fn rt() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread runtime")
    })
}

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
/// `Err(UpsertError)` spanning the whole variant space.
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
    // The metrics/statistics plumbing is built once and reused across iterations.
    static PARTS: OnceLock<FuzzUpsertParts> = OnceLock::new();
    let parts = PARTS.get_or_init(FuzzUpsertParts::new);
    let bincode_opts = upsert_bincode_opts();

    let n_keys = u.int_in_range(1usize..=4)?;
    let mut keys: Vec<UpsertKey> = Vec::with_capacity(n_keys);
    let mut expected: HashMap<UpsertKey, Option<UpsertValue>> = HashMap::new();
    let mut chunk: Vec<(UpsertKey, UpsertValue, Diff)> = Vec::new();

    for k in 0..n_keys {
        let key_row = Row::pack_slice(&[Datum::Int32(i32::try_from(k).unwrap())]);
        let key = UpsertKey::from_key(Ok(&key_row));
        keys.push(key);

        let n_pairs = u.int_in_range(0usize..=4)?;
        for _ in 0..n_pairs {
            let prev: UpsertValue = gen_value(u)?;
            chunk.push((key, prev.clone(), Diff::ONE));
            chunk.push((key, prev, -Diff::ONE));
        }
        if bool::arbitrary(u)? {
            let cur = gen_value(u)?;
            chunk.push((key, cur.clone(), Diff::ONE));
            expected.insert(key, Some(cur));
        } else {
            expected.insert(key, None);
        }
    }

    // Shuffle the whole multi-key chunk — consolidation must be order-independent.
    for i in (1..chunk.len()).rev() {
        let j = u.int_in_range(0..=i)?;
        chunk.swap(i, j);
    }

    rt().block_on(async {
        let mut state = parts.state();
        state
            .consolidate_chunk(chunk.into_iter(), true)
            .await
            .expect("consolidate_chunk should not error on well-formed input");

        let mut results = vec![UpsertValueAndSize::default(); keys.len()];
        state
            .multi_get(keys.iter().copied(), results.iter_mut())
            .await
            .expect("multi_get should not error");

        for (key, result) in keys.iter().zip(results) {
            let finalized: Option<UpsertValue> = match result.value {
                None => None,
                Some(mut sv) => {
                    sv.ensure_decoded(bincode_opts, GlobalId::User(0), None);
                    sv.into_decoded().finalized
                }
            };
            match (expected.get(key).unwrap(), finalized) {
                (Some(cur), Some(got)) => {
                    assert_eq!(&got, cur, "key consolidated to the wrong value")
                }
                (Some(_), None) => panic!("consolidation lost a key's value"),
                (None, None) => {}
                (None, other) => {
                    panic!("consolidation invented a value for a canceled key: {other:?}")
                }
            }
        }
    });
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
