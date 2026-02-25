// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use criterion::{Criterion, criterion_group, criterion_main};
use mz_expr::func::variadic as v;
use mz_expr::{MirScalarExpr, VariadicFunc};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, RowArena};

macro_rules! bench_variadic {
    ($c:expr, $bench_name:ident, $variant:expr, $( $datum:expr ),+ $(,)? ) => {
        $c.bench_function(stringify!($bench_name), |b| {
            let func = VariadicFunc::from($variant);
            let datums: &[Datum] = &[ $( ($datum).into() ),+ ];
            let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
            let arena = RowArena::new();
            b.iter(|| func.eval(datums, &arena, &exprs));
        });
    };
}

// Logic / comparison

fn logic_string_benches(c: &mut Criterion) {
    bench_variadic!(c, coalesce, v::Coalesce, 1i32, 2i32);
    bench_variadic!(c, greatest, v::Greatest, 10i32, 20i32, 5i32);
    bench_variadic!(c, least, v::Least, 10i32, 20i32, 5i32);
    bench_variadic!(c, and, v::And, true, true, true);
    bench_variadic!(c, or, v::Or, true, true, true);
    bench_variadic!(c, error_if_null, v::ErrorIfNull, 42i32, "value was null");

    // String functions

    bench_variadic!(c, concat, v::Concat, "hello", " ", "world");
    bench_variadic!(c, concat_ws, v::ConcatWs, ", ", "one", "two", "three");
    bench_variadic!(c, substr, v::Substr, "hello world", 7i32, 5i32);
    bench_variadic!(c, replace, v::Replace, "hello world", "world", "rust");
    bench_variadic!(c, translate, v::Translate, "hello", "helo", "HELO");
    bench_variadic!(c, split_part, v::SplitPart, "one.two.three", ".", 2i32);
    bench_variadic!(c, pad_leading, v::PadLeading, "hi", 10i32, "*");
    bench_variadic!(c, regexp_match, v::RegexpMatch, "hello world 42", "(\\d+)");

    bench_variadic!(
        c,
        regexp_split_to_array,
        v::RegexpSplitToArray,
        "one1two2three",
        "\\d"
    );

    bench_variadic!(
        c,
        regexp_replace,
        v::RegexpReplace,
        "hello world",
        "wo.ld",
        "rust"
    );

    bench_variadic!(c, string_to_array, v::StringToArray, "one,two,three", ",");
}

// JSON functions

fn json_crypto_benches(c: &mut Criterion) {
    bench_variadic!(c, jsonb_build_array, v::JsonbBuildArray, 1i32, "two", true);

    bench_variadic!(
        c,
        jsonb_build_object,
        v::JsonbBuildObject,
        "key1",
        1i32,
        "key2",
        "value2"
    );

    // HMAC / crypto

    bench_variadic!(c, hmac_string, v::HmacString, "hello", "secret", "sha256");

    bench_variadic!(
        c,
        hmac_bytes,
        v::HmacBytes,
        Datum::Bytes(&[1, 2, 3, 4]),
        Datum::Bytes(&[5, 6, 7, 8]),
        "sha256"
    );
}

// Timestamp / date functions (hand-written due to complex datum construction)

fn timestamp_benches(c: &mut Criterion) {
    c.bench_function("bench_make_timestamp", |b| {
        let func = VariadicFunc::from(v::MakeTimestamp);
        let datums: &[Datum] = &[
            2024i64.into(),
            1i64.into(),
            15i64.into(),
            12i64.into(),
            30i64.into(),
            45.0f64.into(),
        ];
        let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
        let arena = RowArena::new();
        b.iter(|| func.eval(datums, &arena, &exprs));
    });

    c.bench_function("bench_date_bin_timestamp", |b| {
        let func = VariadicFunc::from(v::DateBinTimestamp);
        let interval = Interval::new(0, 0, 3_600_000_000); // 1 hour
        let ts = CheckedTimestamp::from_timestamplike(
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(12, 30, 45)
                .unwrap(),
        )
        .unwrap();
        let origin = CheckedTimestamp::from_timestamplike(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        )
        .unwrap();
        let datums: &[Datum] = &[
            Datum::Interval(interval),
            Datum::Timestamp(ts),
            Datum::Timestamp(origin),
        ];
        let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
        let arena = RowArena::new();
        b.iter(|| func.eval(datums, &arena, &exprs));
    });

    c.bench_function("bench_date_bin_timestamp_tz", |b| {
        let func = VariadicFunc::from(v::DateBinTimestampTz);
        let interval = Interval::new(0, 0, 3_600_000_000); // 1 hour
        let ts = CheckedTimestamp::from_timestamplike(
            Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
        )
        .unwrap();
        let origin = CheckedTimestamp::from_timestamplike(
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
        )
        .unwrap();
        let datums: &[Datum] = &[
            Datum::Interval(interval),
            Datum::TimestampTz(ts),
            Datum::TimestampTz(origin),
        ];
        let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
        let arena = RowArena::new();
        b.iter(|| func.eval(datums, &arena, &exprs));
    });

    c.bench_function("bench_date_diff_timestamp", |b| {
        let func = VariadicFunc::from(v::DateDiffTimestamp);
        let ts1 = CheckedTimestamp::from_timestamplike(
            NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(12, 30, 45)
                .unwrap(),
        )
        .unwrap();
        let ts2 = CheckedTimestamp::from_timestamplike(
            NaiveDate::from_ymd_opt(2024, 6, 20)
                .unwrap()
                .and_hms_opt(8, 15, 30)
                .unwrap(),
        )
        .unwrap();
        let datums: &[Datum] = &["day".into(), Datum::Timestamp(ts1), Datum::Timestamp(ts2)];
        let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
        let arena = RowArena::new();
        b.iter(|| func.eval(datums, &arena, &exprs));
    });

    c.bench_function("bench_date_diff_timestamp_tz", |b| {
        let func = VariadicFunc::from(v::DateDiffTimestampTz);
        let ts1 = CheckedTimestamp::from_timestamplike(
            Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
        )
        .unwrap();
        let ts2 = CheckedTimestamp::from_timestamplike(
            Utc.with_ymd_and_hms(2024, 6, 20, 8, 15, 30).unwrap(),
        )
        .unwrap();
        let datums: &[Datum] = &[
            "day".into(),
            Datum::TimestampTz(ts1),
            Datum::TimestampTz(ts2),
        ];
        let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
        let arena = RowArena::new();
        b.iter(|| func.eval(datums, &arena, &exprs));
    });

    c.bench_function("bench_date_diff_date", |b| {
        let func = VariadicFunc::from(v::DateDiffDate);
        let d1 = Date::from_pg_epoch(8_415).unwrap(); // ~2024-01-15
        let d2 = Date::from_pg_epoch(8_572).unwrap(); // ~2024-06-20
        let datums: &[Datum] = &["day".into(), Datum::Date(d1), Datum::Date(d2)];
        let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
        let arena = RowArena::new();
        b.iter(|| func.eval(datums, &arena, &exprs));
    });

    c.bench_function("bench_date_diff_time", |b| {
        let func = VariadicFunc::from(v::DateDiffTime);
        let t1 = NaiveTime::from_hms_opt(12, 30, 0).unwrap();
        let t2 = NaiveTime::from_hms_opt(18, 45, 30).unwrap();
        let datums: &[Datum] = &["hour".into(), Datum::Time(t1), Datum::Time(t2)];
        let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
        let arena = RowArena::new();
        b.iter(|| func.eval(datums, &arena, &exprs));
    });

    c.bench_function("bench_timezone_time", |b| {
        let func = VariadicFunc::from(v::TimezoneTime);
        let t = NaiveTime::from_hms_opt(12, 30, 0).unwrap();
        let wall = CheckedTimestamp::from_timestamplike(
            Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap(),
        )
        .unwrap();
        let datums: &[Datum] = &["UTC".into(), Datum::Time(t), Datum::TimestampTz(wall)];
        let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
        let arena = RowArena::new();
        b.iter(|| func.eval(datums, &arena, &exprs));
    });
}

// Benchmark registration

criterion_group!(
    benches,
    logic_string_benches,
    json_crypto_benches,
    timestamp_benches
);
criterion_main!(benches);
