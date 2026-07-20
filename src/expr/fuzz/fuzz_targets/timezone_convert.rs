// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: the `AT TIME ZONE` conversion math. Parsing an untrusted
//! timezone and applying it to a timestamp does non-trivial offset arithmetic
//! (DST transition lookups, leap-second-aware add/sub, Duration math), all of
//! which must return an `EvalError` rather than panic on any input. We exercise
//! both directions: TIMESTAMP -> TIMESTAMPTZ and TIMESTAMPTZ -> TIMESTAMP.
//!
//! An arbitrary `&str` almost never parses to a `Timezone`, and when it does
//! it's overwhelmingly a trivial `FixedOffset`, so the interesting code (the
//! named-zone DST-transition lookup, ambiguous/nonexistent local times) would
//! barely run. So most of the time we pick a real IANA zone *with DST* (and a
//! few fixed offsets) so the transition math actually executes. A minority arm
//! still feeds an arbitrary string to keep the parser's reject paths covered.

#![no_main]

use chrono::DateTime;
use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{Eval, MirScalarExpr, UnaryFunc, func};
use mz_pgtz::timezone::{Timezone, TimezoneSpec};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, ReprScalarType, RowArena};

/// Real zones whose offsets shift (DST / sub-hour / historical), plus a couple
/// of fixed offsets, the inputs that actually exercise the conversion math.
const ZONES: &[&str] = &[
    "America/New_York",
    "America/Los_Angeles",
    "Europe/London",
    "Europe/Berlin",
    "Europe/Lisbon",
    "Australia/Lord_Howe", // 30-minute DST shift
    "Pacific/Chatham",     // :45 offset with DST
    "Antarctica/Troll",    // 2-hour DST jump
    "Asia/Kolkata",        // :30, no DST
    "America/Sao_Paulo",
    "UTC",
    "+05:30",
    "-08",
];

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    // 3-in-4: a real (mostly DST-bearing) zone, otherwise an arbitrary string.
    let tz = if u.int_in_range(0u8..=3)? != 0 {
        let Ok(tz) = Timezone::parse(u.choose(ZONES)?, TimezoneSpec::Iso) else {
            return Ok(());
        };
        tz
    } else {
        let tz_str = <&str>::arbitrary(u)?;
        let spec = if bool::arbitrary(u)? {
            TimezoneSpec::Iso
        } else {
            TimezoneSpec::Posix
        };
        let Ok(tz) = Timezone::parse(tz_str, spec) else {
            return Ok(());
        };
        tz
    };
    let secs = u.int_in_range(-8_000_000_000_000i64..=8_000_000_000_000)?;
    let nanos = u.int_in_range(0u32..=999_999_999)?;
    let Some(dt) = DateTime::from_timestamp(secs, nanos) else {
        return Ok(());
    };
    let arena = RowArena::new();

    // TIMESTAMP `AT TIME ZONE tz` -> TIMESTAMPTZ.
    if let Ok(ts) = CheckedTimestamp::from_timestamplike(dt.naive_utc()) {
        let expr = MirScalarExpr::literal_ok(Datum::Timestamp(ts), ReprScalarType::Timestamp)
            .call_unary(UnaryFunc::TimezoneTimestamp(func::TimezoneTimestamp(tz)));
        let _ = expr.eval(&[], &arena);
    }

    // TIMESTAMPTZ `AT TIME ZONE tz` -> TIMESTAMP.
    if let Ok(tstz) = CheckedTimestamp::from_timestamplike(dt) {
        let expr = MirScalarExpr::literal_ok(Datum::TimestampTz(tstz), ReprScalarType::TimestampTz)
            .call_unary(UnaryFunc::TimezoneTimestampTz(func::TimezoneTimestampTz(tz)));
        let _ = expr.eval(&[], &arena);
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
