// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the upsert operator's *runtime* processing, `drain_staged_input`.
//! That is the per-timestamp `multi_get` -> order-keyed per-command processing ->
//! `multi_put` loop that turns a batch of staged `(key, value, order)` commands
//! at various timestamps into the differential retract/insert output, with
//! last-write-wins resolved by an order key (`from_time`).
//!
//! Where `upsert_consolidate` / `upsert_state_consolidate` only exercise the
//! snapshot-rehydration *consolidation* path, this drives the live runtime loop,
//! which is where the hard-to-reproduce correctness issues live: multiple
//! commands for the same `(timestamp, key)` (only the maximum-order one may
//! survive), deletes, re-inserts of the same value, and a key changing across
//! several timestamps in one drain.
//!
//! Because a single `ToUpper` drain always writes *finalized* values (so a
//! later timestamp's `provisional_order` is `None` and the order-skip never
//! fires across timestamps), the semantics reduce to something a simple,
//! obviously-correct oracle can predict: per `(ts, key)` the maximum-order
//! command wins, and processing those winners in timestamp order per key yields
//! the output (retract the prior value, insert the new one) and the final state.
//! We run the real `drain_staged_input` and assert both its emitted output
//! (consolidated) and its final per-key state match that oracle. A divergence,
//! a dropped/duplicated retraction, a wrong winner, a lost or invented value,
//! is a correctness bug. A panic is an availability bug.
//!
//! To make the interesting cancellation paths fire frequently, each key draws
//! its values from a *tiny per-key pool* (plus deletes): with only a handful of
//! distinct values, re-inserting the same value, and delete-then-reinsert of an
//! identical value, are common, exactly the retract/insert exact-cancellation
//! cases. The values themselves cover the tricky-encoding datum space
//! (`Numeric`, `Date`, `Timestamp`/`TimestampTz`, `Interval`, `Uuid`,
//! `MzTimestamp`, `Bytes`, long strings, `MzAclItem`, nested
//! `Array`/`List`/`Map`/`Range`), and a fraction of pool entries are
//! `Err(UpsertError)` values (real sources stage errored values), which the
//! oracle and output comparison handle directly as `UpsertValue`s.

#![no_main]

use std::collections::BTreeMap;
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
use mz_repr::{Datum, Diff, Row, RowPacker, Timestamp};
use mz_storage::fuzz_exports::{FuzzUpsertParts, UpsertKey, UpsertValue, fuzz_drain_staged_input};
use mz_storage::source::SourceExportCreationConfig;
use mz_storage_types::errors::{
    DecodeError, DecodeErrorKind, UpsertError, UpsertNullKeyError, UpsertValueError,
};

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};

// The metrics plumbing is built once per process. `FuzzUpsertParts` is `Sync`
// so it lives in a `static`. `SourceExportCreationConfig` holds a non-`Sync`
// `SourceStatistics`, so it goes in a `thread_local` (built once, not per
// iteration, because re-registering its Prometheus metrics each call was the
// throughput bottleneck).
static PARTS: OnceLock<FuzzUpsertParts> = OnceLock::new();
thread_local! {
    static CFG: SourceExportCreationConfig = PARTS.get_or_init(FuzzUpsertParts::new).source_config();
}

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
    let n = u.int_in_range(0usize..=4)?;
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

/// Consolidate a differential output into a `(value, ts) -> net diff` map,
/// dropping the entries that cancel out. Keys on the whole `UpsertValue` so both
/// `Ok` rows and `Err` values are compared faithfully.
fn consolidate(updates: &[(UpsertValue, u64, Diff)]) -> BTreeMap<(UpsertValue, u64), Diff> {
    let mut m: BTreeMap<(UpsertValue, u64), Diff> = BTreeMap::new();
    for (value, ts, diff) in updates {
        *m.entry((value.clone(), *ts)).or_insert(Diff::ZERO) += *diff;
    }
    m.retain(|_, d| *d != Diff::ZERO);
    m
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let parts = PARTS.get_or_init(FuzzUpsertParts::new);

    let n_keys = u.int_in_range(1usize..=4)?;
    let n_cmds = u.int_in_range(0usize..=20)?;

    // Build a tiny per-key value pool. Commands draw their value from this small
    // pool (or are deletes), so re-inserting the same value and
    // delete-then-reinsert of an *identical* value are common, exactly the
    // retract/insert exact-cancellation cases that are easy to miss with fresh
    // random values every time.
    let mut pools: Vec<Vec<UpsertValue>> = Vec::with_capacity(n_keys);
    for _ in 0..n_keys {
        let pool_size = u.int_in_range(1usize..=3)?;
        let mut pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            pool.push(gen_value(u)?);
        }
        pools.push(pool);
    }

    // Generate commands `(ts, key_idx, order, value)`. Orders are distinct (the
    // generation index), so the max-order-per-(ts,key) winner is unambiguous,
    // and multiple commands can target the same (ts, key) to exercise dedup.
    let mut commands: Vec<(u64, usize, u64, Option<UpsertValue>)> = Vec::with_capacity(n_cmds);
    for i in 0..n_cmds {
        let ts = u.int_in_range(0u64..=4)?;
        let key_idx = u.int_in_range(0usize..=n_keys - 1)?;
        let value = if u.int_in_range(0u8..=3)? == 0 {
            None // a delete
        } else {
            let pool = &pools[key_idx];
            let pick = u.int_in_range(0usize..=pool.len() - 1)?;
            Some(pool[pick].clone())
        };
        commands.push((ts, key_idx, i as u64, value));
    }

    let keys: Vec<UpsertKey> = (0..n_keys)
        .map(|k| {
            let key_row = Row::pack_slice(&[Datum::Int64(i64::try_from(k).unwrap())]);
            UpsertKey::from_key(Ok(&key_row))
        })
        .collect();

    let drain_to = commands.iter().map(|(ts, ..)| *ts).max().map_or(0, |m| m + 1);

    let hook_commands: Vec<(u64, UpsertKey, u64, Option<UpsertValue>)> = commands
        .iter()
        .map(|(ts, k, o, v)| (*ts, keys[*k], *o, v.clone()))
        .collect();

    let (output, final_state) = CFG.with(|cfg| {
        rt().block_on(fuzz_drain_staged_input(
            parts,
            cfg,
            hook_commands,
            drain_to,
            &keys,
        ))
    });

    // --- Oracle ---------------------------------------------------------------
    // Per (key, ts), the maximum-order command wins.
    let mut winner: BTreeMap<(usize, u64), (u64, Option<UpsertValue>)> = BTreeMap::new();
    for (ts, k, o, v) in &commands {
        let replace = match winner.get(&(*k, *ts)) {
            Some((existing_order, _)) => existing_order < o,
            None => true,
        };
        if replace {
            winner.insert((*k, *ts), (*o, v.clone()));
        }
    }
    // Process winners per key in timestamp order (the BTreeMap iterates
    // key-major, ts-ascending): retract the prior value, insert the new one.
    let mut oracle_output: Vec<(UpsertValue, u64, Diff)> = Vec::new();
    let mut oracle_final: Vec<Option<UpsertValue>> = vec![None; n_keys];
    let mut cur_key: Option<usize> = None;
    let mut prev: Option<UpsertValue> = None;
    for ((k, ts), (_order, value)) in &winner {
        if cur_key != Some(*k) {
            cur_key = Some(*k);
            prev = None; // fresh state: this key has no prior value
        }
        if let Some(p) = &prev {
            oracle_output.push((p.clone(), *ts, Diff::MINUS_ONE));
        }
        if let Some(nv) = value {
            oracle_output.push((nv.clone(), *ts, Diff::ONE));
        }
        prev = value.clone();
        oracle_final[*k] = value.clone();
    }

    // --- Checks ---------------------------------------------------------------
    assert_eq!(
        consolidate(&output),
        consolidate(&oracle_output),
        "drain_staged_input output diverged from the upsert oracle"
    );
    for (i, expected) in oracle_final.iter().enumerate() {
        assert_eq!(
            &final_state[i], expected,
            "final state for key {i} diverged from the upsert oracle"
        );
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
