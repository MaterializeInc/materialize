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
//!
//! The result is packed into a `Row` and read back rather than dropped: the
//! leap-second bugs this math is prone to produce an out-of-contract
//! `NaiveTime` that `chrono` hands back without complaint, and only panic once
//! something decodes a `Row` holding it. Evaluating alone would miss them.

#![no_main]

use chrono::DateTime;
use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::{Eval, MirScalarExpr, UnaryFunc, func};
use mz_pgtz::timezone::{Timezone, TimezoneSpec};
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, ReprScalarType, Row, RowArena};

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
    // Sub-minute offset. The whole-minute offsets above can only ever move a
    // leap second to another `:59`, so they never reach the folding branch in
    // `checked_{add,sub}_with_leapsecond`. The named zones do reach it, but
    // only at their pre-standardization LMT offsets.
    "+00:00:01",
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
    // 3-in-4 draws land in 1900-2100, where the DST transitions of the zones
    // above actually live. A uniform draw over the whole representable range
    // (±253,000 years) would hit an ambiguous or nonexistent local time with
    // probability ~5e-8. The wide arm bottoms out at `LOW_DATE` (-4713-12-31,
    // -210_863_606_400): `CheckedTimestamp` rejects everything below that, so
    // drawing down to chrono's own limit instead would leave roughly half of
    // all iterations evaluating nothing at all.
    let mut secs = if u.int_in_range(0u8..=3)? != 0 {
        u.int_in_range(-2_208_988_800i64..=4_102_444_800)?
    } else {
        u.int_in_range(-210_863_606_400i64..=8_000_000_000_000)?
    };
    // chrono encodes a leap second as `nanos >= 1_000_000_000`, and only accepts
    // that encoding when the second-of-minute is 59. That encoding is exactly
    // what the folding in `checked_{add,sub}_with_leapsecond` exists for, so
    // construct it deliberately rather than hoping a uniform `nanos` draw lands
    // there (it never can) or that `secs` happens to be aligned.
    let nanos = if bool::arbitrary(u)? {
        // `DateTime::from_timestamp` derives the second-of-day as
        // `secs.rem_euclid(86_400)`, and `86_400 % 60 == 0`, so aligning `secs`
        // on `rem_euclid(60) == 59` satisfies chrono's precondition.
        secs = secs - secs.rem_euclid(60) + 59;
        u.int_in_range(1_000_000_000u32..=1_999_999_999)?
    } else {
        u.int_in_range(0u32..=999_999_999)?
    };
    let Some(dt) = DateTime::from_timestamp(secs, nanos) else {
        return Ok(());
    };
    let arena = RowArena::new();

    // TIMESTAMP `AT TIME ZONE tz` -> TIMESTAMPTZ.
    if let Ok(ts) = CheckedTimestamp::from_timestamplike(dt.naive_utc()) {
        let expr = MirScalarExpr::literal_ok(Datum::Timestamp(ts), ReprScalarType::Timestamp)
            .call_unary(UnaryFunc::TimezoneTimestamp(func::TimezoneTimestamp(tz)));
        if let Ok(d) = expr.eval(&[], &arena) {
            let _ = Row::pack_slice(&[d]).unpack_first();
        }
    }

    // TIMESTAMPTZ `AT TIME ZONE tz` -> TIMESTAMP.
    if let Ok(tstz) = CheckedTimestamp::from_timestamplike(dt) {
        let expr = MirScalarExpr::literal_ok(Datum::TimestampTz(tstz), ReprScalarType::TimestampTz)
            .call_unary(UnaryFunc::TimezoneTimestampTz(func::TimezoneTimestampTz(
                tz,
            )));
        if let Ok(d) = expr.eval(&[], &arena) {
            let _ = Row::pack_slice(&[d]).unpack_first();
        }
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
