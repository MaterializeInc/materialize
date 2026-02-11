// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bencher::{Bencher, TestFn, benchmark_group, benchmark_main};
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use mz_expr::{BinaryFunc, MirScalarExpr, func};
use mz_repr::adt::date::Date;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::numeric::Numeric;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, RowArena};
use uuid::Uuid;

macro_rules! bench_binary {
    ($bench_name:ident, $struct_expr:expr,
     $a_datum:expr, $b_datum:expr $(,)? ) => {
        fn $bench_name(b: &mut Bencher) {
            let f = BinaryFunc::from($struct_expr);
            let a = MirScalarExpr::column(0);
            let e = MirScalarExpr::column(1);
            let arena = RowArena::new();
            let datums: &[Datum] = &[($a_datum).into(), ($b_datum).into()];
            b.iter(|| f.eval(datums, &arena, &a, &e));
        }
    };
}

/// Generates a `_group` function that returns individual benchmark entries
/// for each input pair, so they show up as separate lines in benchmark output.
macro_rules! bench_binary_multi {
    ($bench_name:ident, $struct_expr:expr,
     [ $( ($a_datum:expr, $b_datum:expr) ),+ $(,)? ]) => {
        paste::paste! {
            fn [<$bench_name _group>]() -> Vec<bencher::TestDescAndFn> {
                use bencher::{TestDescAndFn, TestDesc};
                use std::borrow::Cow;
                let mut benches = Vec::new();
                $(
                    benches.push(TestDescAndFn {
                        desc: TestDesc {
                            name: Cow::from(concat!(
                                stringify!($bench_name), "(", stringify!($a_datum), ", ", stringify!($b_datum), ")"
                            )),
                            ignore: false,
                        },
                        testfn: TestFn::StaticBenchFn({
                            fn run(b: &mut bencher::Bencher) {
                                let f = BinaryFunc::from($struct_expr);
                                let a = MirScalarExpr::column(0);
                                let e = MirScalarExpr::column(1);
                                let arena = RowArena::new();
                                let datums: &[Datum] = &[($a_datum).into(), ($b_datum).into()];
                                b.iter(|| f.eval(datums, &arena, &a, &e));
                            }
                            run
                        }),
                    });
                )+
                benches
            }
        }
    };
}

// Arithmetic functions

// Add
bench_binary!(add_int16, func::AddInt16, 1i16, 2i16);
bench_binary!(add_int16_error, func::AddInt16, i16::MAX, 1i16);
bench_binary!(add_int32, func::AddInt32, 1i32, 2i32);
bench_binary!(add_int32_error, func::AddInt32, i32::MAX, 1i32);
bench_binary!(add_int64, func::AddInt64, 1i64, 2i64);
bench_binary!(add_int64_error, func::AddInt64, i64::MAX, 1i64);
bench_binary!(add_uint16, func::AddUint16, 1u16, 2u16);
bench_binary!(add_uint16_error, func::AddUint16, u16::MAX, 1u16);
bench_binary!(add_uint32, func::AddUint32, 1u32, 2u32);
bench_binary!(add_uint32_error, func::AddUint32, u32::MAX, 1u32);
bench_binary!(add_uint64, func::AddUint64, 1u64, 2u64);
bench_binary!(add_uint64_error, func::AddUint64, u64::MAX, 1u64);
bench_binary!(add_float32, func::AddFloat32, 1.0f32, 2.0f32);
bench_binary!(add_float32_error, func::AddFloat32, f32::MAX, f32::MAX);
bench_binary!(add_float64, func::AddFloat64, 1.0f64, 2.0f64);
bench_binary!(add_float64_error, func::AddFloat64, f64::MAX, f64::MAX);
bench_binary!(
    add_numeric,
    func::AddNumeric,
    Numeric::from(1),
    Numeric::from(2)
);
bench_binary!(
    add_interval,
    func::AddInterval,
    Interval::new(1, 2, 3_000_000),
    Interval::new(0, 1, 1_000_000)
);

fn add_timestamp_interval(b: &mut Bencher) {
    let f = BinaryFunc::AddTimestampInterval(func::AddTimestampInterval);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Timestamp(ts),
        Datum::Interval(Interval::new(0, 1, 0)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn add_timestamp_tz_interval(b: &mut Bencher) {
    let f = BinaryFunc::AddTimestampTzInterval(func::AddTimestampTzInterval);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::TimestampTz(ts),
        Datum::Interval(Interval::new(0, 1, 0)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn add_date_interval(b: &mut Bencher) {
    let f = BinaryFunc::AddDateInterval(func::AddDateInterval);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Date(Date::from_pg_epoch(0).unwrap()),
        Datum::Interval(Interval::new(1, 0, 0)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn add_date_time(b: &mut Bencher) {
    let f = BinaryFunc::AddDateTime(func::AddDateTime);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Date(Date::from_pg_epoch(0).unwrap()),
        Datum::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn add_time_interval(b: &mut Bencher) {
    let f = BinaryFunc::AddTimeInterval(func::AddTimeInterval);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// Sub
bench_binary!(sub_int16, func::SubInt16, 10i16, 3i16);
bench_binary!(sub_int16_error, func::SubInt16, i16::MIN, 1i16);
bench_binary!(sub_int32, func::SubInt32, 10i32, 3i32);
bench_binary!(sub_int32_error, func::SubInt32, i32::MIN, 1i32);
bench_binary!(sub_int64, func::SubInt64, 10i64, 3i64);
bench_binary!(sub_int64_error, func::SubInt64, i64::MIN, 1i64);
bench_binary!(sub_uint16, func::SubUint16, 10u16, 3u16);
bench_binary!(sub_uint16_error, func::SubUint16, 0u16, 1u16);
bench_binary!(sub_uint32, func::SubUint32, 10u32, 3u32);
bench_binary!(sub_uint32_error, func::SubUint32, 0u32, 1u32);
bench_binary!(sub_uint64, func::SubUint64, 10u64, 3u64);
bench_binary!(sub_uint64_error, func::SubUint64, 0u64, 1u64);
bench_binary!(sub_float32, func::SubFloat32, 10.0f32, 3.0f32);
bench_binary!(sub_float64, func::SubFloat64, 10.0f64, 3.0f64);
bench_binary!(
    sub_numeric,
    func::SubNumeric,
    Numeric::from(10),
    Numeric::from(3)
);
bench_binary!(
    sub_interval,
    func::SubInterval,
    Interval::new(1, 2, 3_000_000),
    Interval::new(0, 1, 1_000_000)
);

fn sub_timestamp(b: &mut Bencher) {
    let f = BinaryFunc::SubTimestamp(func::SubTimestamp);
    let ts1 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let ts2 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[Datum::Timestamp(ts1), Datum::Timestamp(ts2)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn sub_timestamp_tz(b: &mut Bencher) {
    let f = BinaryFunc::SubTimestampTz(func::SubTimestampTz);
    let ts1 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap())
            .unwrap();
    let ts2 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[Datum::TimestampTz(ts1), Datum::TimestampTz(ts2)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn sub_timestamp_interval(b: &mut Bencher) {
    let f = BinaryFunc::SubTimestampInterval(func::SubTimestampInterval);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Timestamp(ts),
        Datum::Interval(Interval::new(0, 1, 0)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn sub_timestamp_tz_interval(b: &mut Bencher) {
    let f = BinaryFunc::SubTimestampTzInterval(func::SubTimestampTzInterval);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::TimestampTz(ts),
        Datum::Interval(Interval::new(0, 1, 0)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

bench_binary!(
    sub_date,
    func::SubDate,
    Date::from_pg_epoch(100).unwrap(),
    Date::from_pg_epoch(0).unwrap()
);

fn sub_date_interval(b: &mut Bencher) {
    let f = BinaryFunc::SubDateInterval(func::SubDateInterval);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Date(Date::from_pg_epoch(100).unwrap()),
        Datum::Interval(Interval::new(1, 0, 0)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn sub_time(b: &mut Bencher) {
    let f = BinaryFunc::SubTime(func::SubTime);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Time(NaiveTime::from_hms_opt(14, 0, 0).unwrap()),
        Datum::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn sub_time_interval(b: &mut Bencher) {
    let f = BinaryFunc::SubTimeInterval(func::SubTimeInterval);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Time(NaiveTime::from_hms_opt(14, 0, 0).unwrap()),
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// Mul
bench_binary!(mul_int16, func::MulInt16, 3i16, 4i16);
bench_binary!(mul_int16_error, func::MulInt16, i16::MAX, 2i16);
bench_binary!(mul_int32, func::MulInt32, 3i32, 4i32);
bench_binary!(mul_int32_error, func::MulInt32, i32::MAX, 2i32);
bench_binary!(mul_int64, func::MulInt64, 3i64, 4i64);
bench_binary!(mul_int64_error, func::MulInt64, i64::MAX, 2i64);
bench_binary!(mul_uint16, func::MulUint16, 3u16, 4u16);
bench_binary!(mul_uint16_error, func::MulUint16, u16::MAX, 2u16);
bench_binary!(mul_uint32, func::MulUint32, 3u32, 4u32);
bench_binary!(mul_uint32_error, func::MulUint32, u32::MAX, 2u32);
bench_binary!(mul_uint64, func::MulUint64, 3u64, 4u64);
bench_binary!(mul_uint64_error, func::MulUint64, u64::MAX, 2u64);
bench_binary!(mul_float32, func::MulFloat32, 3.0f32, 4.0f32);
bench_binary!(mul_float32_error, func::MulFloat32, f32::MAX, 2.0f32);
bench_binary!(mul_float64, func::MulFloat64, 3.0f64, 4.0f64);
bench_binary!(mul_float64_error, func::MulFloat64, f64::MAX, 2.0f64);
bench_binary!(
    mul_numeric,
    func::MulNumeric,
    Numeric::from(3),
    Numeric::from(4)
);
bench_binary!(
    mul_interval,
    func::MulInterval,
    Interval::new(1, 0, 0),
    2.0f64
);

// Div
bench_binary_multi!(
    div_int16,
    func::DivInt16,
    [
        (10i16, 3i16),
        (100i16, 7i16),
        (-50i16, 3i16),
        (i16::MAX, 13i16),
        (1i16, 1i16),
        (0i16, 42i16),
        (32000i16, 127i16),
        (9999i16, 100i16),
    ]
);
bench_binary!(div_int16_divzero, func::DivInt16, 10i16, 0i16);
bench_binary_multi!(
    div_int32,
    func::DivInt32,
    [
        (10i32, 3i32),
        (1000000i32, 7i32),
        (-500i32, 3i32),
        (i32::MAX, 13i32),
        (1i32, 1i32),
        (0i32, 42i32),
        (999999999i32, 127i32),
        (123456789i32, 9876i32),
    ]
);
bench_binary!(div_int32_divzero, func::DivInt32, 10i32, 0i32);
bench_binary_multi!(
    div_int64,
    func::DivInt64,
    [
        (10i64, 3i64),
        (1000000000000i64, 7i64),
        (-500i64, 3i64),
        (i64::MAX, 13i64),
        (1i64, 1i64),
        (0i64, 42i64),
        (999999999999i64, 127i64),
        (123456789012345i64, 9876543i64),
    ]
);
bench_binary!(div_int64_divzero, func::DivInt64, 10i64, 0i64);
bench_binary_multi!(
    div_uint16,
    func::DivUint16,
    [
        (10u16, 3u16),
        (100u16, 7u16),
        (u16::MAX, 13u16),
        (1u16, 1u16),
        (0u16, 42u16),
        (60000u16, 127u16),
    ]
);
bench_binary!(div_uint16_divzero, func::DivUint16, 10u16, 0u16);
bench_binary_multi!(
    div_uint32,
    func::DivUint32,
    [
        (10u32, 3u32),
        (1000000u32, 7u32),
        (u32::MAX, 13u32),
        (1u32, 1u32),
        (0u32, 42u32),
        (999999999u32, 127u32),
    ]
);
bench_binary!(div_uint32_divzero, func::DivUint32, 10u32, 0u32);
bench_binary_multi!(
    div_uint64,
    func::DivUint64,
    [
        (10u64, 3u64),
        (1000000000000u64, 7u64),
        (u64::MAX, 13u64),
        (1u64, 1u64),
        (0u64, 42u64),
        (999999999999u64, 127u64),
    ]
);
bench_binary!(div_uint64_divzero, func::DivUint64, 10u64, 0u64);
bench_binary_multi!(
    div_float32,
    func::DivFloat32,
    [
        (10.0f32, 3.0f32),
        (1.0f32, 7.0f32),
        (1e10f32, 0.001f32),
        (-42.5f32, 3.7f32),
        (0.001f32, 1000.0f32),
        (999.999f32, 1.001f32),
    ]
);
bench_binary!(div_float32_divzero, func::DivFloat32, 10.0f32, 0.0f32);
bench_binary_multi!(
    div_float64,
    func::DivFloat64,
    [
        (10.0f64, 3.0f64),
        (1.0f64, 7.0f64),
        (1e15f64, 0.00001f64),
        (-42.5f64, 3.7f64),
        (0.001f64, 1000.0f64),
        (999.999f64, 1.001f64),
        (1.7976931e100f64, 1.23456789f64),
        (std::f64::consts::PI, std::f64::consts::E),
    ]
);
bench_binary!(div_float64_divzero, func::DivFloat64, 10.0f64, 0.0f64);
bench_binary_multi!(
    div_numeric,
    func::DivNumeric,
    [
        (Numeric::from(10), Numeric::from(3)),
        (Numeric::from(1), Numeric::from(7)),
        (Numeric::from(1000000), Numeric::from(13)),
        (Numeric::from(999999), Numeric::from(42)),
        (Numeric::from(-50), Numeric::from(3)),
        (Numeric::from(1), Numeric::from(1)),
    ]
);
bench_binary!(
    div_numeric_divzero,
    func::DivNumeric,
    Numeric::from(10),
    Numeric::from(0)
);
bench_binary!(
    div_interval,
    func::DivInterval,
    Interval::new(2, 0, 0),
    2.0f64
);
bench_binary!(
    div_interval_divzero,
    func::DivInterval,
    Interval::new(2, 0, 0),
    0.0f64
);

// Mod
bench_binary!(mod_int16, func::ModInt16, 10i16, 3i16);
bench_binary!(mod_int16_divzero, func::ModInt16, 10i16, 0i16);
bench_binary!(mod_int32, func::ModInt32, 10i32, 3i32);
bench_binary!(mod_int32_divzero, func::ModInt32, 10i32, 0i32);
bench_binary!(mod_int64, func::ModInt64, 10i64, 3i64);
bench_binary!(mod_int64_divzero, func::ModInt64, 10i64, 0i64);
bench_binary!(mod_uint16, func::ModUint16, 10u16, 3u16);
bench_binary!(mod_uint32, func::ModUint32, 10u32, 3u32);
bench_binary!(mod_uint64, func::ModUint64, 10u64, 3u64);
bench_binary!(mod_float32, func::ModFloat32, 10.0f32, 3.0f32);
bench_binary!(mod_float64, func::ModFloat64, 10.0f64, 3.0f64);
bench_binary!(
    mod_numeric,
    func::ModNumeric,
    Numeric::from(10),
    Numeric::from(3)
);

// RoundNumeric (binary)
bench_binary!(
    round_numeric,
    func::RoundNumericBinary,
    Numeric::from(314),
    2i32
);

// Age
fn age_timestamp(b: &mut Bencher) {
    let f = BinaryFunc::AgeTimestamp(func::AgeTimestamp);
    let ts1 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 6, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let ts2 = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[Datum::Timestamp(ts1), Datum::Timestamp(ts2)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn age_timestamp_tz(b: &mut Bencher) {
    let f = BinaryFunc::AgeTimestampTz(func::AgeTimestampTz);
    let ts1 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap())
            .unwrap();
    let ts2 =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[Datum::TimestampTz(ts1), Datum::TimestampTz(ts2)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// comparison, and shift functions

// BitAnd
bench_binary!(
    bit_and_int16,
    func::BitAndInt16,
    Datum::Int16(0x0F0F),
    Datum::Int16(0x00FF)
);
bench_binary!(
    bit_and_int32,
    func::BitAndInt32,
    Datum::Int32(0x0F0F0F0F),
    Datum::Int32(0x00FF00FF)
);
bench_binary!(
    bit_and_int64,
    func::BitAndInt64,
    Datum::Int64(0x0F0F),
    Datum::Int64(0x00FF)
);
bench_binary!(
    bit_and_uint16,
    func::BitAndUint16,
    Datum::UInt16(0x0F0F),
    Datum::UInt16(0x00FF)
);
bench_binary!(
    bit_and_uint32,
    func::BitAndUint32,
    Datum::UInt32(0x0F0F),
    Datum::UInt32(0x00FF)
);
bench_binary!(
    bit_and_uint64,
    func::BitAndUint64,
    Datum::UInt64(0x0F0F),
    Datum::UInt64(0x00FF)
);

// BitOr
bench_binary!(
    bit_or_int16,
    func::BitOrInt16,
    Datum::Int16(0x0F00),
    Datum::Int16(0x00F0)
);
bench_binary!(
    bit_or_int32,
    func::BitOrInt32,
    Datum::Int32(0x0F00),
    Datum::Int32(0x00F0)
);
bench_binary!(
    bit_or_int64,
    func::BitOrInt64,
    Datum::Int64(0x0F00),
    Datum::Int64(0x00F0)
);
bench_binary!(
    bit_or_uint16,
    func::BitOrUint16,
    Datum::UInt16(0x0F00),
    Datum::UInt16(0x00F0)
);
bench_binary!(
    bit_or_uint32,
    func::BitOrUint32,
    Datum::UInt32(0x0F00),
    Datum::UInt32(0x00F0)
);
bench_binary!(
    bit_or_uint64,
    func::BitOrUint64,
    Datum::UInt64(0x0F00),
    Datum::UInt64(0x00F0)
);

// BitXor
bench_binary!(
    bit_xor_int16,
    func::BitXorInt16,
    Datum::Int16(0x0FF0),
    Datum::Int16(0x00FF)
);
bench_binary!(
    bit_xor_int32,
    func::BitXorInt32,
    Datum::Int32(0x0FF0),
    Datum::Int32(0x00FF)
);
bench_binary!(
    bit_xor_int64,
    func::BitXorInt64,
    Datum::Int64(0x0FF0),
    Datum::Int64(0x00FF)
);
bench_binary!(
    bit_xor_uint16,
    func::BitXorUint16,
    Datum::UInt16(0x0FF0),
    Datum::UInt16(0x00FF)
);
bench_binary!(
    bit_xor_uint32,
    func::BitXorUint32,
    Datum::UInt32(0x0FF0),
    Datum::UInt32(0x00FF)
);
bench_binary!(
    bit_xor_uint64,
    func::BitXorUint64,
    Datum::UInt64(0x0FF0),
    Datum::UInt64(0x00FF)
);

// BitShiftLeft
bench_binary!(bit_shift_left_int16, func::BitShiftLeftInt16, 1i16, 4i32);
bench_binary!(bit_shift_left_int32, func::BitShiftLeftInt32, 1i32, 4i32);
bench_binary!(bit_shift_left_int64, func::BitShiftLeftInt64, 1i64, 4i32);
bench_binary!(bit_shift_left_uint16, func::BitShiftLeftUint16, 1u16, 4u32);
bench_binary!(bit_shift_left_uint32, func::BitShiftLeftUint32, 1u32, 4u32);
bench_binary!(bit_shift_left_uint64, func::BitShiftLeftUint64, 1u64, 4u32);

// BitShiftRight
bench_binary!(
    bit_shift_right_int16,
    func::BitShiftRightInt16,
    256i16,
    4i32
);
bench_binary!(
    bit_shift_right_int32,
    func::BitShiftRightInt32,
    256i32,
    4i32
);
bench_binary!(
    bit_shift_right_int64,
    func::BitShiftRightInt64,
    256i64,
    4i32
);
bench_binary!(
    bit_shift_right_uint16,
    func::BitShiftRightUint16,
    256u16,
    4u32
);
bench_binary!(
    bit_shift_right_uint32,
    func::BitShiftRightUint32,
    256u32,
    4u32
);
bench_binary!(
    bit_shift_right_uint64,
    func::BitShiftRightUint64,
    256u64,
    4u32
);

// Comparison
bench_binary!(eq, func::Eq, 42i32, 42i32);
bench_binary!(not_eq, func::NotEq, 42i32, 43i32);
bench_binary!(lt, func::Lt, 1i32, 2i32);
bench_binary!(lte, func::Lte, 1i32, 2i32);
bench_binary!(gt, func::Gt, 2i32, 1i32);
bench_binary!(gte, func::Gte, 2i32, 1i32);

// Step 5: Remaining binary functions

// String functions
bench_binary!(text_concat, func::TextConcatBinary, "hello", " world");
bench_binary!(position, func::Position, "hello world", "world");
bench_binary!(left, func::Left, "hello world", 5i32);
bench_binary!(right, func::Right, "hello world", 5i32);
bench_binary!(repeat_string, func::RepeatString, "ab", 3i32);
bench_binary!(trim, func::Trim, "  hello  ", " ");
bench_binary!(trim_leading, func::TrimLeading, "  hello  ", " ");
bench_binary!(trim_trailing, func::TrimTrailing, "  hello  ", " ");
bench_binary!(normalize, func::Normalize, "hello", "NFC");
bench_binary!(starts_with, func::StartsWith, "hello world", "hello");
bench_binary!(
    encoded_bytes_char_length,
    func::EncodedBytesCharLength,
    b"hello",
    "utf-8"
);

// Pattern matching
bench_binary!(like_escape, func::LikeEscape, "100%", "\\");
bench_binary!(
    is_like_match_case_sensitive,
    func::IsLikeMatchCaseSensitive,
    "hello world",
    "%world"
);
bench_binary!(
    is_like_match_case_insensitive,
    func::IsLikeMatchCaseInsensitive,
    "Hello World",
    "%world"
);
bench_binary!(
    is_regexp_match_case_sensitive,
    func::IsRegexpMatchCaseSensitive,
    "hello world",
    "w.rld"
);
bench_binary!(
    is_regexp_match_case_insensitive,
    func::IsRegexpMatchCaseInsensitive,
    "Hello World",
    "w.rld"
);

// Encoding
bench_binary!(convert_from, func::ConvertFrom, b"hello", "utf8");
bench_binary!(encode, func::Encode, b"hello", "base64");
bench_binary!(decode, func::Decode, "aGVsbG8=", "base64");

// Digest
bench_binary!(digest_string, func::DigestString, "hello", "md5");
bench_binary!(digest_bytes, func::DigestBytes, b"hello", "md5");

// Numeric operations
bench_binary_multi!(
    log_numeric,
    func::LogBaseNumeric,
    [
        (Numeric::from(10), Numeric::from(100)),
        (Numeric::from(2), Numeric::from(1024)),
        (Numeric::from(10), Numeric::from(42)),
        (Numeric::from(3), Numeric::from(81)),
        (Numeric::from(7), Numeric::from(999999)),
    ]
);
bench_binary_multi!(
    power,
    func::Power,
    [
        (2.0f64, 10.0f64),
        (2.0f64, 0.5f64),
        (10.0f64, 3.0f64),
        (1.5f64, 7.3f64),
        (0.5f64, 20.0f64),
        (100.0f64, 0.1f64),
        (std::f64::consts::E, 3.0f64),
        (9.0f64, 0.5f64),
    ]
);
bench_binary_multi!(
    power_numeric,
    func::PowerNumeric,
    [
        (Numeric::from(2), Numeric::from(10)),
        (Numeric::from(10), Numeric::from(3)),
        (Numeric::from(3), Numeric::from(7)),
        (Numeric::from(7), Numeric::from(5)),
        (Numeric::from(100), Numeric::from(2)),
    ]
);

// Byte operations
bench_binary!(get_bit, func::GetBit, b"\xff", 3i32);
bench_binary!(get_byte, func::GetByte, b"hello", 0i32);
bench_binary!(
    constant_time_eq_bytes,
    func::ConstantTimeEqBytes,
    b"hello",
    b"hello"
);
bench_binary!(
    constant_time_eq_string,
    func::ConstantTimeEqString,
    "hello",
    "hello"
);

// MzRenderTypmod
bench_binary!(mz_render_typmod, func::MzRenderTypmod, 23u32, -1i32);

// ParseIdent
bench_binary!(parse_ident, func::ParseIdent, "myschema.mytable", true);

// PrettySql
bench_binary!(pretty_sql, func::PrettySql, "SELECT 1", 80i32);

// RegexpReplace
fn regexp_replace(b: &mut Bencher) {
    let regex = mz_repr::adt::regex::Regex::new("world", false).unwrap();
    let f = BinaryFunc::RegexpReplace(func::RegexpReplace { regex, limit: 0 });
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["hello world".into(), "earth".into()];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// UuidGenerateV5
bench_binary!(uuid_generate_v5, func::UuidGenerateV5, Uuid::nil(), "test");

// MzAclItemContainsPrivilege
// Skipped: requires MzAclItem datum construction

// ToChar
fn to_char_timestamp(b: &mut Bencher) {
    let f = BinaryFunc::ToCharTimestamp(func::ToCharTimestampFormat);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[Datum::Timestamp(ts), "YYYY-MM-DD HH24:MI:SS".into()];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn to_char_timestamp_tz(b: &mut Bencher) {
    let f = BinaryFunc::ToCharTimestampTz(func::ToCharTimestampTzFormat);
    let ts = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[Datum::TimestampTz(ts), "YYYY-MM-DD HH24:MI:SS".into()];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// DateBin
fn date_bin_timestamp(b: &mut Bencher) {
    let f = BinaryFunc::DateBinTimestamp(func::DateBinTimestamp);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        Datum::Timestamp(ts),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn date_bin_timestamp_tz(b: &mut Bencher) {
    let f = BinaryFunc::DateBinTimestampTz(func::DateBinTimestampTz);
    let ts = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        Datum::TimestampTz(ts),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// Extract/DatePart
fn extract_interval(b: &mut Bencher) {
    let f = BinaryFunc::ExtractInterval(func::DatePartIntervalNumeric);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["epoch".into(), Interval::new(1, 2, 3_000_000).into()];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn extract_time(b: &mut Bencher) {
    let f = BinaryFunc::ExtractTime(func::DatePartTimeNumeric);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        "hour".into(),
        Datum::Time(NaiveTime::from_hms_opt(12, 30, 45).unwrap()),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn extract_timestamp(b: &mut Bencher) {
    let f = BinaryFunc::ExtractTimestamp(func::DatePartTimestampTimestampNumeric);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["year".into(), Datum::Timestamp(ts)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn extract_timestamp_tz(b: &mut Bencher) {
    let f = BinaryFunc::ExtractTimestampTz(func::DatePartTimestampTimestampTzNumeric);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["year".into(), Datum::TimestampTz(ts)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn extract_date(b: &mut Bencher) {
    let f = BinaryFunc::ExtractDate(func::ExtractDateUnits);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["year".into(), Datum::Date(Date::from_pg_epoch(0).unwrap())];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn date_part_interval(b: &mut Bencher) {
    let f = BinaryFunc::DatePartInterval(func::DatePartIntervalF64);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        "epoch".into(),
        Datum::Interval(Interval::new(1, 2, 3_000_000)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn date_part_time(b: &mut Bencher) {
    let f = BinaryFunc::DatePartTime(func::DatePartTimeF64);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        "hour".into(),
        Datum::Time(NaiveTime::from_hms_opt(12, 30, 45).unwrap()),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn date_part_timestamp(b: &mut Bencher) {
    let f = BinaryFunc::DatePartTimestamp(func::DatePartTimestampTimestampF64);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["year".into(), Datum::Timestamp(ts)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn date_part_timestamp_tz(b: &mut Bencher) {
    let f = BinaryFunc::DatePartTimestampTz(func::DatePartTimestampTimestampTzF64);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["year".into(), Datum::TimestampTz(ts)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// DateTrunc
fn date_trunc_timestamp(b: &mut Bencher) {
    let f = BinaryFunc::DateTruncTimestamp(func::DateTruncUnitsTimestamp);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["hour".into(), Datum::Timestamp(ts)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn date_trunc_timestamp_tz(b: &mut Bencher) {
    let f = BinaryFunc::DateTruncTimestampTz(func::DateTruncUnitsTimestampTz);
    let ts = CheckedTimestamp::from_timestamplike(
        Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["hour".into(), Datum::TimestampTz(ts)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn date_trunc_interval(b: &mut Bencher) {
    let f = BinaryFunc::DateTruncInterval(func::DateTruncInterval);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        "hour".into(),
        Datum::Interval(Interval::new(0, 0, 5_400_000_000)),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// Timezone
fn timezone_timestamp_binary(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneTimestampBinary(func::TimezoneTimestampBinary);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["UTC".into(), Datum::Timestamp(ts)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn timezone_timestamp_tz_binary(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneTimestampTzBinary(func::TimezoneTimestampTzBinary);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["UTC".into(), Datum::TimestampTz(ts)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn timezone_interval_timestamp_binary(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneIntervalTimestampBinary(func::TimezoneIntervalTimestampBinary);
    let ts = CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    )
    .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        Datum::Timestamp(ts),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn timezone_interval_timestamp_tz_binary(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneIntervalTimestampTzBinary(func::TimezoneIntervalTimestampTzBinary);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        Datum::TimestampTz(ts),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn timezone_interval_time_binary(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneIntervalTimeBinary(func::TimezoneIntervalTimeBinary);
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &[
        Datum::Interval(Interval::new(0, 0, 3_600_000_000)),
        Datum::Time(NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
    ];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

fn timezone_offset(b: &mut Bencher) {
    let f = BinaryFunc::TimezoneOffset(func::TimezoneOffset);
    let ts =
        CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap())
            .unwrap();
    let a = MirScalarExpr::column(0);
    let e = MirScalarExpr::column(1);
    let arena = RowArena::new();
    let datums: &[Datum] = &["UTC".into(), Datum::TimestampTz(ts)];
    b.iter(|| f.eval(datums, &arena, &a, &e));
}

// Benchmark registration

benchmark_group!(
    arithmetic_benches,
    // Add
    add_int16,
    add_int16_error,
    add_int32,
    add_int32_error,
    add_int64,
    add_int64_error,
    add_uint16,
    add_uint16_error,
    add_uint32,
    add_uint32_error,
    add_uint64,
    add_uint64_error,
    add_float32,
    add_float32_error,
    add_float64,
    add_float64_error,
    add_numeric,
    add_interval,
    add_timestamp_interval,
    add_timestamp_tz_interval,
    add_date_interval,
    add_date_time,
    add_time_interval,
    // Sub
    sub_int16,
    sub_int16_error,
    sub_int32,
    sub_int32_error,
    sub_int64,
    sub_int64_error,
    sub_uint16,
    sub_uint16_error,
    sub_uint32,
    sub_uint32_error,
    sub_uint64,
    sub_uint64_error,
    sub_float32,
    sub_float64,
    sub_numeric,
    sub_interval,
    sub_timestamp,
    sub_timestamp_tz,
    sub_timestamp_interval,
    sub_timestamp_tz_interval,
    sub_date,
    sub_date_interval,
    sub_time,
    sub_time_interval,
    // Mul
    mul_int16,
    mul_int16_error,
    mul_int32,
    mul_int32_error,
    mul_int64,
    mul_int64_error,
    mul_uint16,
    mul_uint16_error,
    mul_uint32,
    mul_uint32_error,
    mul_uint64,
    mul_uint64_error,
    mul_float32,
    mul_float32_error,
    mul_float64,
    mul_float64_error,
    mul_numeric,
    mul_interval,
    // Div (happy paths are in *_group functions via benchmark_main)
    div_int16_divzero,
    div_int32_divzero,
    div_int64_divzero,
    div_uint16_divzero,
    div_uint32_divzero,
    div_uint64_divzero,
    div_float32_divzero,
    div_float64_divzero,
    div_numeric_divzero,
    div_interval,
    div_interval_divzero,
    // Mod
    mod_int16,
    mod_int16_divzero,
    mod_int32,
    mod_int32_divzero,
    mod_int64,
    mod_int64_divzero,
    mod_uint16,
    mod_uint32,
    mod_uint64,
    mod_float32,
    mod_float64,
    mod_numeric,
    // Round, Age
    round_numeric,
    age_timestamp,
    age_timestamp_tz
);

benchmark_group!(
    bitwise_comparison_benches,
    // BitAnd
    bit_and_int16,
    bit_and_int32,
    bit_and_int64,
    bit_and_uint16,
    bit_and_uint32,
    bit_and_uint64,
    // BitOr
    bit_or_int16,
    bit_or_int32,
    bit_or_int64,
    bit_or_uint16,
    bit_or_uint32,
    bit_or_uint64,
    // BitXor
    bit_xor_int16,
    bit_xor_int32,
    bit_xor_int64,
    bit_xor_uint16,
    bit_xor_uint32,
    bit_xor_uint64,
    // BitShiftLeft
    bit_shift_left_int16,
    bit_shift_left_int32,
    bit_shift_left_int64,
    bit_shift_left_uint16,
    bit_shift_left_uint32,
    bit_shift_left_uint64,
    // BitShiftRight
    bit_shift_right_int16,
    bit_shift_right_int32,
    bit_shift_right_int64,
    bit_shift_right_uint16,
    bit_shift_right_uint32,
    bit_shift_right_uint64,
    // Comparison
    eq,
    not_eq,
    lt,
    lte,
    gt,
    gte
);

benchmark_group!(
    remaining_benches,
    // String
    text_concat,
    position,
    left,
    right,
    repeat_string,
    trim,
    trim_leading,
    trim_trailing,
    normalize,
    starts_with,
    encoded_bytes_char_length,
    // Pattern matching
    like_escape,
    is_like_match_case_sensitive,
    is_like_match_case_insensitive,
    is_regexp_match_case_sensitive,
    is_regexp_match_case_insensitive,
    // Encoding
    convert_from,
    encode,
    decode,
    // Digest
    digest_string,
    digest_bytes,
    // Numeric operations (happy paths are in *_group functions via benchmark_main)
    // Byte operations
    get_bit,
    get_byte,
    constant_time_eq_bytes,
    constant_time_eq_string,
    // Misc
    mz_render_typmod,
    parse_ident,
    pretty_sql,
    regexp_replace,
    uuid_generate_v5,
    // ToChar
    to_char_timestamp,
    to_char_timestamp_tz,
    // DateBin
    date_bin_timestamp,
    date_bin_timestamp_tz,
    // Extract/DatePart
    extract_interval,
    extract_time,
    extract_timestamp,
    extract_timestamp_tz,
    extract_date,
    date_part_interval,
    date_part_time,
    date_part_timestamp,
    date_part_timestamp_tz,
    // DateTrunc
    date_trunc_timestamp,
    date_trunc_timestamp_tz,
    date_trunc_interval,
    // Timezone
    timezone_timestamp_binary,
    timezone_timestamp_tz_binary,
    timezone_interval_timestamp_binary,
    timezone_interval_timestamp_tz_binary,
    timezone_interval_time_binary,
    timezone_offset
);

benchmark_main!(
    arithmetic_benches,
    bitwise_comparison_benches,
    remaining_benches,
    // Multi-input benchmark groups (each input is a separate benchmark entry)
    div_int16_group,
    div_int32_group,
    div_int64_group,
    div_uint16_group,
    div_uint32_group,
    div_uint64_group,
    div_float32_group,
    div_float64_group,
    div_numeric_group,
    log_numeric_group,
    power_group,
    power_numeric_group
);
