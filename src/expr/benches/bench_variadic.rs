// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bencher::{Bencher, benchmark_group, benchmark_main};
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use mz_expr::{MirScalarExpr, VariadicFunc};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, RowArena};

use VariadicFunc::*;

macro_rules! bench_variadic {
    ($bench_name:ident, $variant:expr, $( $datum:expr ),+ $(,)? ) => {
        fn $bench_name(b: &mut Bencher) {
            let func = $variant;
            let datums: &[Datum] = &[ $( ($datum).into() ),+ ];
            let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
            let arena = RowArena::new();
            b.iter(|| func.eval(datums, &arena, &exprs));
        }
    };
}

// Logic / comparison

bench_variadic!(coalesce, Coalesce, 1i32, 2i32);
bench_variadic!(greatest, Greatest, 10i32, 20i32, 5i32);
bench_variadic!(least, Least, 10i32, 20i32, 5i32);
bench_variadic!(and, And, true, true, true);
bench_variadic!(or, Or, true, true, true);
bench_variadic!(error_if_null, ErrorIfNull, 42i32, "value was null");

// String functions

bench_variadic!(concat, Concat, "hello", " ", "world");
bench_variadic!(concat_ws, ConcatWs, ", ", "one", "two", "three");
bench_variadic!(substr, Substr, "hello world", 7i32, 5i32);
bench_variadic!(replace, Replace, "hello world", "world", "rust");
bench_variadic!(translate, Translate, "hello", "helo", "HELO");
bench_variadic!(split_part, SplitPart, "one.two.three", ".", 2i32);
bench_variadic!(pad_leading, PadLeading, "hi", 10i32, "*");
bench_variadic!(regexp_match, RegexpMatch, "hello world 42", "(\\d+)");

bench_variadic!(
    regexp_split_to_array,
    RegexpSplitToArray,
    "one1two2three",
    "\\d"
);

bench_variadic!(
    regexp_replace,
    RegexpReplace,
    "hello world",
    "wo.ld",
    "rust"
);

bench_variadic!(string_to_array, StringToArray, "one,two,three", ",");

// JSON functions

bench_variadic!(jsonb_build_array, JsonbBuildArray, 1i32, "two", true);

bench_variadic!(
    jsonb_build_object,
    JsonbBuildObject,
    "key1",
    1i32,
    "key2",
    "value2"
);

// HMAC / crypto

bench_variadic!(hmac_string, HmacString, "hello", "secret", "sha256");

bench_variadic!(
    hmac_bytes,
    HmacBytes,
    Datum::Bytes(&[1, 2, 3, 4]),
    Datum::Bytes(&[5, 6, 7, 8]),
    "sha256"
);

// Timestamp / date functions (hand-written due to complex datum construction)

fn bench_make_timestamp(b: &mut Bencher) {
    let func = MakeTimestamp;
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
}

fn bench_date_bin_timestamp(b: &mut Bencher) {
    let func = DateBinTimestamp;
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
}

fn bench_date_bin_timestamp_tz(b: &mut Bencher) {
    let func = DateBinTimestampTz;
    let interval = Interval::new(0, 0, 3_600_000_000); // 1 hour
    let ts = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let origin =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap())
            .unwrap();
    let datums: &[Datum] = &[
        Datum::Interval(interval),
        Datum::TimestampTz(ts),
        Datum::TimestampTz(origin),
    ];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_date_diff_timestamp(b: &mut Bencher) {
    let func = DateDiffTimestamp;
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
}

fn bench_date_diff_timestamp_tz(b: &mut Bencher) {
    let func = DateDiffTimestampTz;
    let ts1 = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let ts2 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 6, 20, 8, 15, 30).unwrap())
            .unwrap();
    let datums: &[Datum] = &[
        "day".into(),
        Datum::TimestampTz(ts1),
        Datum::TimestampTz(ts2),
    ];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_date_diff_date(b: &mut Bencher) {
    let func = DateDiffDate;
    let d1 = Date::from_pg_epoch(8_415).unwrap(); // ~2024-01-15
    let d2 = Date::from_pg_epoch(8_572).unwrap(); // ~2024-06-20
    let datums: &[Datum] = &["day".into(), Datum::Date(d1), Datum::Date(d2)];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_date_diff_time(b: &mut Bencher) {
    let func = DateDiffTime;
    let t1 = NaiveTime::from_hms_opt(12, 30, 0).unwrap();
    let t2 = NaiveTime::from_hms_opt(18, 45, 30).unwrap();
    let datums: &[Datum] = &["hour".into(), Datum::Time(t1), Datum::Time(t2)];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

fn bench_timezone_time(b: &mut Bencher) {
    let func = TimezoneTime;
    let t = NaiveTime::from_hms_opt(12, 30, 0).unwrap();
    let wall =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let datums: &[Datum] = &["UTC".into(), Datum::Time(t), Datum::TimestampTz(wall)];
    let exprs: Vec<MirScalarExpr> = (0..datums.len()).map(MirScalarExpr::column).collect();
    let arena = RowArena::new();
    b.iter(|| func.eval(datums, &arena, &exprs));
}

// Benchmark groups

benchmark_group!(
    logic_string_benches,
    coalesce,
    greatest,
    least,
    and,
    or,
    error_if_null,
    concat,
    concat_ws,
    substr,
    replace,
    translate,
    split_part,
    pad_leading,
    regexp_match,
    regexp_split_to_array,
    regexp_replace,
    string_to_array
);

benchmark_group!(
    json_crypto_benches,
    jsonb_build_array,
    jsonb_build_object,
    hmac_string,
    hmac_bytes
);

benchmark_group!(
    timestamp_benches,
    bench_make_timestamp,
    bench_date_bin_timestamp,
    bench_date_bin_timestamp_tz,
    bench_date_diff_timestamp,
    bench_date_diff_timestamp_tz,
    bench_date_diff_date,
    bench_date_diff_time,
    bench_timezone_time
);

benchmark_main!(logic_string_benches, json_crypto_benches, timestamp_benches);
