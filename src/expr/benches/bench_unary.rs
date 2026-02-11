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
use mz_expr::like_pattern;
use mz_expr::{MirScalarExpr, UnaryFunc, func as f};
use mz_repr::adt::date::Date;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::mz_acl_item::{AclItem, AclMode, MzAclItem};
use mz_repr::adt::numeric::{Numeric, NumericMaxScale};
use mz_repr::adt::regex::Regex;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::role_id::RoleId;
use mz_repr::adt::system::Oid;
use mz_repr::{Datum as D, RowArena, Timestamp};
use ordered_float::OrderedFloat;
use uuid::Uuid;

macro_rules! bench_unary {
    ($bench_name:ident, $struct_expr:expr,
     $a_datum:expr $(,)?) => {
        fn $bench_name(b: &mut Bencher) {
            let f = UnaryFunc::from($struct_expr);
            let a = MirScalarExpr::column(0);
            let arena = RowArena::new();
            let datums: &[D] = &[($a_datum).into()];
            b.iter(|| f.eval(datums, &arena, &a));
        }
    };
}

/// Generates a `_group` function that returns individual benchmark entries
/// for each input, so they show up as separate lines in benchmark output.
macro_rules! bench_unary_multi {
    ($bench_name:ident, $struct_expr:expr,
     [ $( $a_datum:expr ),+ $(,)? ]) => {
        paste::paste! {
            fn [<$bench_name _group>]() -> Vec<bencher::TestDescAndFn> {
                use bencher::{TestDescAndFn, TestFn, TestDesc};
                use std::borrow::Cow;
                let mut benches = Vec::new();
                $(
                    benches.push(TestDescAndFn {
                        desc: TestDesc {
                            name: Cow::from(concat!(
                                stringify!($bench_name), "(", stringify!($a_datum), ")"
                            )),
                            ignore: false,
                        },
                        testfn: TestFn::StaticBenchFn({
                            fn run(b: &mut bencher::Bencher) {
                                let f = UnaryFunc::from($struct_expr);
                                let a = MirScalarExpr::column(0);
                                let arena = RowArena::new();
                                let datums: &[D] = &[($a_datum).into()];
                                b.iter(|| f.eval(datums, &arena, &a));
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

// Unary arithmetic + bitwise functions

// Boolean
bench_unary!(not, f::Not, true);
bench_unary!(is_null, f::IsNull, D::Int32(42));
bench_unary!(is_true, f::IsTrue, true);
bench_unary!(is_false, f::IsFalse, false);

// Neg
bench_unary!(neg_int16, f::NegInt16, 42i16);
bench_unary!(neg_int16_error, f::NegInt16, i16::MIN);
bench_unary!(neg_int32, f::NegInt32, 42i32);
bench_unary!(neg_int32_error, f::NegInt32, i32::MIN);
bench_unary!(neg_int64, f::NegInt64, 42i64);
bench_unary!(neg_int64_error, f::NegInt64, i64::MIN);
bench_unary!(neg_float32, f::NegFloat32, 42.0f32);
bench_unary!(neg_float64, f::NegFloat64, 42.0f64);
bench_unary!(neg_numeric, f::NegNumeric, Numeric::from(42));
bench_unary!(neg_interval, f::NegInterval, Interval::new(1, 2, 3_000_000));

// Abs
bench_unary!(abs_int16, f::AbsInt16, -42i16);
bench_unary!(abs_int32, f::AbsInt32, -42i32);
bench_unary!(abs_int64, f::AbsInt64, -42i64);
bench_unary!(abs_float32, f::AbsFloat32, -42.0f32);
bench_unary!(abs_float64, f::AbsFloat64, -42.0f64);
bench_unary!(abs_numeric, f::AbsNumeric, Numeric::from(-42));

// BitNot
bench_unary!(bit_not_int16, f::BitNotInt16, D::Int16(0x0F0F));
bench_unary!(bit_not_int32, f::BitNotInt32, D::Int32(0x0F0F0F0F));
bench_unary!(bit_not_int64, f::BitNotInt64, D::Int64(0x0F0F0F0F));
bench_unary!(bit_not_uint16, f::BitNotUint16, D::UInt16(0x0F0F));
bench_unary!(bit_not_uint32, f::BitNotUint32, D::UInt32(0x0F0F));
bench_unary!(bit_not_uint64, f::BitNotUint64, D::UInt64(0x0F0F));

// Sqrt/Cbrt (8 diverse inputs each: perfect squares, primes, tiny, huge, fractional)
bench_unary_multi!(
    sqrt_float64,
    f::SqrtFloat64,
    [
        0.0f64,
        0.001f64,
        2.0f64,
        16.0f64,
        17.3f64,
        999.999f64,
        1e12f64,
        1.7976931348623157e100f64,
    ]
);
bench_unary_multi!(
    sqrt_numeric,
    f::SqrtNumeric,
    [
        Numeric::from(2),
        Numeric::from(16),
        Numeric::from(17),
        Numeric::from(9999),
        Numeric::from(1000000),
        Numeric::from(999999999),
    ]
);
bench_unary_multi!(
    cbrt_float64,
    f::CbrtFloat64,
    [
        0.0f64, 0.001f64, 2.0f64, 27.0f64, 30.7f64, 1e9f64, 1.5e100f64, -8.0f64,
    ]
);

// Ceil/Floor/Round/Trunc (diverse fractional values)
bench_unary_multi!(
    ceil_float32,
    f::CeilFloat32,
    [
        0.0f32, 0.1f32, 3.17f32, -2.7f32, 999.999f32, -0.001f32, 1e10f32, 0.5f32,
    ]
);
bench_unary_multi!(
    ceil_float64,
    f::CeilFloat64,
    [
        0.0f64, 0.1f64, 3.17f64, -2.7f64, 999.999f64, -0.001f64, 1e15f64, 0.5f64,
    ]
);
bench_unary_multi!(
    ceil_numeric,
    f::CeilNumeric,
    [
        Numeric::from(0),
        Numeric::from(314),
        Numeric::from(-42),
        Numeric::from(999999),
        Numeric::from(1),
        Numeric::from(-1),
    ]
);
bench_unary_multi!(
    floor_float32,
    f::FloorFloat32,
    [0.0f32, 3.17f32, -2.7f32, 999.999f32, -0.001f32, 0.5f32,]
);
bench_unary_multi!(
    floor_float64,
    f::FloorFloat64,
    [0.0f64, 3.17f64, -2.7f64, 999.999f64, -0.001f64, 0.5f64,]
);
bench_unary_multi!(
    floor_numeric,
    f::FloorNumeric,
    [
        Numeric::from(0),
        Numeric::from(314),
        Numeric::from(-42),
        Numeric::from(999999),
    ]
);
bench_unary_multi!(
    round_float32,
    f::RoundFloat32,
    [0.0f32, 3.17f32, -2.7f32, 0.5f32, 999.999f32, -0.5f32,]
);
bench_unary_multi!(
    round_float64,
    f::RoundFloat64,
    [0.0f64, 3.17f64, -2.7f64, 0.5f64, 999.999f64, -0.5f64,]
);
bench_unary_multi!(
    round_numeric,
    f::RoundNumeric,
    [
        Numeric::from(0),
        Numeric::from(314),
        Numeric::from(-42),
        Numeric::from(999999),
    ]
);
bench_unary_multi!(
    trunc_float32,
    f::TruncFloat32,
    [0.0f32, 3.17f32, -2.7f32, 999.999f32,]
);
bench_unary_multi!(
    trunc_float64,
    f::TruncFloat64,
    [0.0f64, 3.17f64, -2.7f64, 999.999f64,]
);
bench_unary_multi!(
    trunc_numeric,
    f::TruncNumeric,
    [
        Numeric::from(0),
        Numeric::from(314),
        Numeric::from(-42),
        Numeric::from(999999),
    ]
);

// Log/Ln/Exp (diverse magnitudes)
bench_unary_multi!(
    log10,
    f::Log10,
    [
        0.001f64,
        1.0f64,
        10.0f64,
        100.0f64,
        99999.9f64,
        1e15f64,
        0.123456789f64,
        42.42f64,
    ]
);
bench_unary_multi!(
    log10_numeric,
    f::Log10Numeric,
    [
        Numeric::from(1),
        Numeric::from(10),
        Numeric::from(100),
        Numeric::from(99999),
        Numeric::from(42),
    ]
);
bench_unary_multi!(
    ln,
    f::Ln,
    [
        0.001f64,
        1.0f64,
        std::f64::consts::E,
        100.0f64,
        99999.9f64,
        1e15f64,
        0.123456789f64,
        42.42f64,
    ]
);
bench_unary_multi!(
    ln_numeric,
    f::LnNumeric,
    [
        Numeric::from(1),
        Numeric::from(3),
        Numeric::from(100),
        Numeric::from(99999),
        Numeric::from(42),
    ]
);
bench_unary_multi!(
    exp,
    f::Exp,
    [
        0.0f64, 1.0f64, 2.0f64, -1.0f64, 10.0f64, 0.5f64, -5.0f64, 20.0f64,
    ]
);
bench_unary_multi!(
    exp_numeric,
    f::ExpNumeric,
    [
        Numeric::from(0),
        Numeric::from(1),
        Numeric::from(2),
        Numeric::from(10),
        Numeric::from(-1),
    ]
);

// Trig (diverse angles spanning the domain)
bench_unary_multi!(
    cos,
    f::Cos,
    [
        0.0f64,
        0.5f64,
        1.0f64,
        std::f64::consts::PI,
        std::f64::consts::FRAC_PI_2,
        2.5f64,
        -1.0f64,
        100.0f64,
    ]
);
bench_unary_multi!(
    acos,
    f::Acos,
    [
        -1.0f64, -0.5f64, 0.0f64, 0.1f64, 0.5f64, 0.9f64, 0.999f64, 1.0f64,
    ]
);
bench_unary_multi!(
    cosh,
    f::Cosh,
    [
        0.0f64, 0.5f64, 1.0f64, 2.0f64, 5.0f64, -1.0f64, 10.0f64, -5.0f64,
    ]
);
bench_unary_multi!(
    acosh,
    f::Acosh,
    [
        1.0f64, 1.5f64, 2.0f64, 5.0f64, 10.0f64, 100.0f64, 1.001f64, 50.0f64,
    ]
);
bench_unary_multi!(
    sin,
    f::Sin,
    [
        0.0f64,
        0.5f64,
        1.0f64,
        std::f64::consts::PI,
        std::f64::consts::FRAC_PI_2,
        2.5f64,
        -1.0f64,
        100.0f64,
    ]
);
bench_unary_multi!(
    asin,
    f::Asin,
    [
        -1.0f64, -0.5f64, 0.0f64, 0.1f64, 0.5f64, 0.9f64, 0.999f64, 1.0f64,
    ]
);
bench_unary_multi!(
    sinh,
    f::Sinh,
    [
        0.0f64, 0.5f64, 1.0f64, 2.0f64, 5.0f64, -1.0f64, 10.0f64, -5.0f64,
    ]
);
bench_unary_multi!(
    asinh,
    f::Asinh,
    [
        0.0f64, 0.5f64, 1.0f64, 10.0f64, 100.0f64, -1.0f64, -10.0f64, 0.001f64,
    ]
);
bench_unary_multi!(
    tan,
    f::Tan,
    [
        0.0f64, 0.5f64, 1.0f64, 0.785f64, // ~pi/4
        -1.0f64, 1.5f64, // close to pi/2
        3.0f64, 100.0f64,
    ]
);
bench_unary_multi!(
    atan,
    f::Atan,
    [
        0.0f64, 0.5f64, 1.0f64, 10.0f64, 100.0f64, -1.0f64, -100.0f64, 0.001f64,
    ]
);
bench_unary_multi!(
    tanh,
    f::Tanh,
    [
        0.0f64, 0.5f64, 1.0f64, 5.0f64, -1.0f64, -5.0f64, 20.0f64, 0.001f64,
    ]
);
bench_unary_multi!(
    atanh,
    f::Atanh,
    [
        0.0f64, 0.1f64, 0.5f64, 0.9f64, 0.99f64, -0.5f64, -0.9f64, 0.001f64,
    ]
);
bench_unary_multi!(
    cot,
    f::Cot,
    [
        0.1f64,
        0.5f64,
        1.0f64,
        std::f64::consts::FRAC_PI_4,
        2.0f64,
        3.0f64,
        -1.0f64,
        0.01f64,
    ]
);
bench_unary_multi!(
    degrees,
    f::Degrees,
    [
        0.0f64,
        std::f64::consts::PI,
        std::f64::consts::FRAC_PI_2,
        1.0f64,
        -std::f64::consts::PI,
        6.37f64,
        0.001f64,
        100.0f64,
    ]
);
bench_unary_multi!(
    radians,
    f::Radians,
    [
        0.0f64, 45.0f64, 90.0f64, 180.0f64, 360.0f64, -180.0f64, 1.0f64, 0.001f64,
    ]
);

// Unary cast functions (unit struct casts only)

// Bool casts
bench_unary!(cast_bool_to_string, f::CastBoolToString, true);
bench_unary!(
    cast_bool_to_string_nonstandard,
    f::CastBoolToStringNonstandard,
    true,
);
bench_unary!(cast_bool_to_int32, f::CastBoolToInt32, true);
bench_unary!(cast_bool_to_int64, f::CastBoolToInt64, true);

// Int16 casts
bench_unary!(cast_int16_to_float32, f::CastInt16ToFloat32, D::Int16(42));
bench_unary!(cast_int16_to_float64, f::CastInt16ToFloat64, D::Int16(42));
bench_unary!(cast_int16_to_int32, f::CastInt16ToInt32, D::Int16(42));
bench_unary!(cast_int16_to_int64, f::CastInt16ToInt64, D::Int16(42));
bench_unary!(cast_int16_to_uint16, f::CastInt16ToUint16, D::Int16(42));
bench_unary!(cast_int16_to_uint32, f::CastInt16ToUint32, D::Int16(42));
bench_unary!(cast_int16_to_uint64, f::CastInt16ToUint64, D::Int16(42));
bench_unary!(cast_int16_to_string, f::CastInt16ToString, D::Int16(42));

// Int32 casts
bench_unary!(cast_int32_to_bool, f::CastInt32ToBool, D::Int32(1));
bench_unary!(cast_int32_to_float32, f::CastInt32ToFloat32, D::Int32(42));
bench_unary!(cast_int32_to_float64, f::CastInt32ToFloat64, D::Int32(42));
bench_unary!(cast_int32_to_int16, f::CastInt32ToInt16, D::Int32(42));
bench_unary!(cast_int32_to_int64, f::CastInt32ToInt64, D::Int32(42));
bench_unary!(cast_int32_to_uint16, f::CastInt32ToUint16, D::Int32(42));
bench_unary!(cast_int32_to_uint32, f::CastInt32ToUint32, D::Int32(42));
bench_unary!(cast_int32_to_uint64, f::CastInt32ToUint64, D::Int32(42));
bench_unary!(cast_int32_to_string, f::CastInt32ToString, D::Int32(42));
bench_unary!(cast_int32_to_oid, f::CastInt32ToOid, D::Int32(42));

// Int64 casts
bench_unary!(cast_int64_to_int16, f::CastInt64ToInt16, D::Int64(42));
bench_unary!(cast_int64_to_int32, f::CastInt64ToInt32, D::Int64(42));
bench_unary!(cast_int64_to_uint16, f::CastInt64ToUint16, D::Int64(42));
bench_unary!(cast_int64_to_uint32, f::CastInt64ToUint32, D::Int64(42));
bench_unary!(cast_int64_to_uint64, f::CastInt64ToUint64, D::Int64(42));
bench_unary!(cast_int64_to_bool, f::CastInt64ToBool, D::Int64(1));
bench_unary!(cast_int64_to_float32, f::CastInt64ToFloat32, D::Int64(42));
bench_unary!(cast_int64_to_float64, f::CastInt64ToFloat64, D::Int64(42));
bench_unary!(cast_int64_to_string, f::CastInt64ToString, D::Int64(42));
bench_unary!(cast_int64_to_oid, f::CastInt64ToOid, D::Int64(42));

// Uint16 casts
bench_unary!(cast_uint16_to_uint32, f::CastUint16ToUint32, D::UInt16(42));
bench_unary!(cast_uint16_to_uint64, f::CastUint16ToUint64, D::UInt16(42));
bench_unary!(cast_uint16_to_int16, f::CastUint16ToInt16, D::UInt16(42));
bench_unary!(cast_uint16_to_int32, f::CastUint16ToInt32, D::UInt16(42));
bench_unary!(cast_uint16_to_int64, f::CastUint16ToInt64, D::UInt16(42));
bench_unary!(cast_uint16_to_float32, f::CastUint16ToFloat32, 42u16);
bench_unary!(cast_uint16_to_float64, f::CastUint16ToFloat64, 42u16);
bench_unary!(cast_uint16_to_string, f::CastUint16ToString, D::UInt16(42));

// Uint32 casts
bench_unary!(cast_uint32_to_uint16, f::CastUint32ToUint16, D::UInt32(42));
bench_unary!(cast_uint32_to_uint64, f::CastUint32ToUint64, D::UInt32(42));
bench_unary!(cast_uint32_to_int16, f::CastUint32ToInt16, D::UInt32(42));
bench_unary!(cast_uint32_to_int32, f::CastUint32ToInt32, D::UInt32(42));
bench_unary!(cast_uint32_to_int64, f::CastUint32ToInt64, D::UInt32(42));
bench_unary!(cast_uint32_to_float32, f::CastUint32ToFloat32, 42u32);
bench_unary!(cast_uint32_to_float64, f::CastUint32ToFloat64, 42u32);
bench_unary!(cast_uint32_to_string, f::CastUint32ToString, D::UInt32(42));

// Uint64 casts
bench_unary!(cast_uint64_to_uint16, f::CastUint64ToUint16, D::UInt64(42));
bench_unary!(cast_uint64_to_uint32, f::CastUint64ToUint32, D::UInt64(42));
bench_unary!(cast_uint64_to_int16, f::CastUint64ToInt16, D::UInt64(42));
bench_unary!(cast_uint64_to_int32, f::CastUint64ToInt32, D::UInt64(42));
bench_unary!(cast_uint64_to_int64, f::CastUint64ToInt64, D::UInt64(42));
bench_unary!(cast_uint64_to_float32, f::CastUint64ToFloat32, 42u64);
bench_unary!(cast_uint64_to_float64, f::CastUint64ToFloat64, 42u64);
bench_unary!(cast_uint64_to_string, f::CastUint64ToString, D::UInt64(42));

// Float32 casts
bench_unary!(cast_float32_to_int16, f::CastFloat32ToInt16, 42.0f32);
bench_unary!(cast_float32_to_int32, f::CastFloat32ToInt32, 42.0f32);
bench_unary!(cast_float32_to_int64, f::CastFloat32ToInt64, 42.0f32);
bench_unary!(cast_float32_to_uint16, f::CastFloat32ToUint16, 42.0f32);
bench_unary!(cast_float32_to_uint32, f::CastFloat32ToUint32, 42.0f32);
bench_unary!(cast_float32_to_uint64, f::CastFloat32ToUint64, 42.0f32);
bench_unary!(cast_float32_to_float64, f::CastFloat32ToFloat64, 42.0f32);
bench_unary!(cast_float32_to_string, f::CastFloat32ToString, 42.0f32);

// Float64 casts
bench_unary!(cast_float64_to_int16, f::CastFloat64ToInt16, 42.0f64);
bench_unary!(cast_float64_to_int32, f::CastFloat64ToInt32, 42.0f64);
bench_unary!(cast_float64_to_int64, f::CastFloat64ToInt64, 42.0f64);
bench_unary!(cast_float64_to_uint16, f::CastFloat64ToUint16, 42.0f64);
bench_unary!(cast_float64_to_uint32, f::CastFloat64ToUint32, 42.0f64);
bench_unary!(cast_float64_to_uint64, f::CastFloat64ToUint64, 42.0f64);
bench_unary!(cast_float64_to_float32, f::CastFloat64ToFloat32, 42.0f64);
bench_unary!(cast_float64_to_string, f::CastFloat64ToString, 42.0f64);

// Numeric casts
bench_unary!(
    cast_numeric_to_float32,
    f::CastNumericToFloat32,
    Numeric::from(42)
);
bench_unary!(
    cast_numeric_to_float64,
    f::CastNumericToFloat64,
    Numeric::from(42)
);
bench_unary!(
    cast_numeric_to_int16,
    f::CastNumericToInt16,
    Numeric::from(42),
);
bench_unary!(
    cast_numeric_to_int32,
    f::CastNumericToInt32,
    Numeric::from(42),
);
bench_unary!(
    cast_numeric_to_int64,
    f::CastNumericToInt64,
    Numeric::from(42),
);
bench_unary!(
    cast_numeric_to_uint16,
    f::CastNumericToUint16,
    Numeric::from(42),
);
bench_unary!(
    cast_numeric_to_uint32,
    f::CastNumericToUint32,
    Numeric::from(42),
);
bench_unary!(
    cast_numeric_to_uint64,
    f::CastNumericToUint64,
    Numeric::from(42),
);
bench_unary!(
    cast_numeric_to_string,
    f::CastNumericToString,
    Numeric::from(42),
);

// Parameterized casts (XToNumeric)
bench_unary!(cast_int16_to_numeric, f::CastInt16ToNumeric(None), 42i16);
bench_unary!(cast_int32_to_numeric, f::CastInt32ToNumeric(None), 42i32);
bench_unary!(cast_int64_to_numeric, f::CastInt64ToNumeric(None), 42i64);
bench_unary!(cast_uint16_to_numeric, f::CastUint16ToNumeric(None), 42u16);
bench_unary!(cast_uint32_to_numeric, f::CastUint32ToNumeric(None), 42u32);
bench_unary!(cast_uint64_to_numeric, f::CastUint64ToNumeric(None), 42u64);
bench_unary!(
    cast_float32_to_numeric,
    f::CastFloat32ToNumeric(None),
    42f32
);
bench_unary!(
    cast_float64_to_numeric,
    f::CastFloat64ToNumeric(None),
    42f64
);

// String parsing casts
bench_unary!(cast_string_to_bool, f::CastStringToBool, "true");
bench_unary!(cast_string_to_int16, f::CastStringToInt16, "42");
bench_unary!(cast_string_to_int32, f::CastStringToInt32, "42");
bench_unary!(cast_string_to_int64, f::CastStringToInt64, "42");
bench_unary!(cast_string_to_uint16, f::CastStringToUint16, "42");
bench_unary!(cast_string_to_uint32, f::CastStringToUint32, "42");
bench_unary!(cast_string_to_uint64, f::CastStringToUint64, "42");
bench_unary!(cast_string_to_float32, f::CastStringToFloat32, "42.0");
bench_unary!(cast_string_to_float64, f::CastStringToFloat64, "42.0");
bench_unary!(cast_string_to_date, f::CastStringToDate, "2024-01-15");
bench_unary!(cast_string_to_time, f::CastStringToTime, "12:30:45");
bench_unary!(
    cast_string_to_timestamp,
    f::CastStringToTimestamp(None),
    "2024-01-15 12:30:45"
);
bench_unary!(
    cast_string_to_timestamp_tz,
    f::CastStringToTimestampTz(None),
    "2024-01-15 12:30:45+00"
);
bench_unary!(
    cast_string_to_interval,
    f::CastStringToInterval,
    "1 day 2 hours"
);
bench_unary!(
    cast_string_to_uuid,
    f::CastStringToUuid,
    "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
);
bench_unary!(cast_string_to_jsonb, f::CastStringToJsonb, "42");
bench_unary!(cast_string_to_bytes, f::CastStringToBytes, "\\xDEADBEEF");

// Other casts
bench_unary!(cast_oid_to_int32, f::CastOidToInt32, D::UInt32(42));
bench_unary!(cast_oid_to_int64, f::CastOidToInt64, D::UInt32(42));
bench_unary!(cast_oid_to_string, f::CastOidToString, D::UInt32(42));
bench_unary!(cast_uuid_to_string, f::CastUuidToString, Uuid::nil());
bench_unary!(cast_bytes_to_string, f::CastBytesToString, b"hello");
bench_unary!(cast_jsonb_to_string, f::CastJsonbToString, "hello");
bench_unary!(cast_jsonb_to_int16, f::CastJsonbToInt16, 42f64);
bench_unary!(cast_jsonb_to_int32, f::CastJsonbToInt32, 42f64);
bench_unary!(cast_jsonb_to_int64, f::CastJsonbToInt64, 42f64);
bench_unary!(cast_jsonb_to_float32, f::CastJsonbToFloat32, 42f64);
bench_unary!(cast_jsonb_to_float64, f::CastJsonbToFloat64, 42f64);
bench_unary!(cast_jsonb_to_bool, f::CastJsonbToBool, true);

// Remaining unary functions

// String operations
bench_unary!(upper, f::Upper, "hello");
bench_unary!(lower, f::Lower, "HELLO");
bench_unary!(initcap, f::Initcap, "hello world");
bench_unary!(trim_whitespace, f::TrimWhitespace, "  hello  ");
bench_unary!(
    trim_leading_whitespace,
    f::TrimLeadingWhitespace,
    "  hello  "
);
bench_unary!(
    trim_trailing_whitespace,
    f::TrimTrailingWhitespace,
    "  hello  "
);
bench_unary!(ascii, f::Ascii, "hello");
bench_unary!(char_length, f::CharLength, "hello");
bench_unary!(bit_length_bytes, f::BitLengthBytes, b"hello");
bench_unary!(bit_length_string, f::BitLengthString, "hello");
bench_unary!(byte_length_bytes, f::ByteLengthBytes, b"hello");
bench_unary!(byte_length_string, f::ByteLengthString, "hello");
bench_unary!(bit_count_bytes, f::BitCountBytes, b"\xff");
bench_unary!(chr, f::Chr, D::Int32(65));
bench_unary!(reverse, f::Reverse, "hello");
bench_unary!(quote_ident, f::QuoteIdent, "my_table");
bench_unary!(pg_size_pretty, f::PgSizePretty, Numeric::from(1048576));

// Jsonb
bench_unary!(jsonb_typeof, f::JsonbTypeof, D::Float64(OrderedFloat(42.0)));
bench_unary!(jsonb_pretty, f::JsonbPretty, "hello");

// Date/time
bench_unary!(justify_days, f::JustifyDays, Interval::new(0, 35, 0));
bench_unary!(
    justify_hours,
    f::JustifyHours,
    Interval::new(0, 0, 30 * 3_600_000_000)
);
bench_unary!(
    justify_interval,
    f::JustifyInterval,
    Interval::new(0, 35, 30 * 3_600_000_000)
);
bench_unary!(
    cast_date_to_string,
    f::CastDateToString,
    Date::from_pg_epoch(0).unwrap(),
);
bench_unary!(
    cast_time_to_string,
    f::CastTimeToString,
    NaiveTime::from_hms_opt(12, 30, 45).unwrap(),
);
bench_unary!(
    cast_interval_to_string,
    f::CastIntervalToString,
    Interval::new(1, 2, 3_000_000)
);
bench_unary!(
    cast_interval_to_time,
    f::CastIntervalToTime,
    Interval::new(0, 0, 45_000_000_000)
);
bench_unary!(
    cast_time_to_interval,
    f::CastTimeToInterval,
    NaiveTime::from_hms_opt(12, 30, 45).unwrap()
);

// Hash functions
bench_unary!(crc32_bytes, f::Crc32Bytes, b"hello");
bench_unary!(crc32_string, f::Crc32String, "hello");
bench_unary!(kafka_murmur2_bytes, f::KafkaMurmur2Bytes, b"hello");
bench_unary!(kafka_murmur2_string, f::KafkaMurmur2String, "hello");
bench_unary!(seahash_bytes, f::SeahashBytes, b"hello");
bench_unary!(seahash_string, f::SeahashString, "hello");

// --- Helpers for constructing complex datums ---

fn make_ts() -> CheckedTimestamp<chrono::NaiveDateTime> {
    CheckedTimestamp::from_timestamplike(
        NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 30, 45)
            .unwrap(),
    )
    .unwrap()
}

fn make_tstz() -> CheckedTimestamp<chrono::DateTime<Utc>> {
    CheckedTimestamp::from_timestamplike(Utc.with_ymd_and_hms(2024, 1, 15, 12, 30, 45).unwrap())
        .unwrap()
}

// --- Timestamp/Date casts ---

bench_unary!(
    cast_date_to_timestamp,
    f::CastDateToTimestamp(None),
    Date::from_pg_epoch(8_415).unwrap(),
);
bench_unary!(
    cast_date_to_timestamp_tz,
    f::CastDateToTimestampTz(None),
    Date::from_pg_epoch(8_415).unwrap(),
);

fn cast_timestamp_to_date(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampToDate);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_timestamp_to_time(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampToTime);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_timestamp_to_string(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampToString);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_timestamp_to_timestamp_tz(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampToTimestampTz {
        from: None,
        to: None,
    });
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_timestamp_tz_to_date(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampTzToDate);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_timestamp_tz_to_timestamp(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampTzToTimestamp {
        from: None,
        to: None,
    });
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_timestamp_tz_to_string(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampTzToString);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_timestamp_tz_to_time(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampTzToTime);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

bench_unary!(to_timestamp, f::ToTimestamp, 1705312245.0f64);

// --- MzTimestamp casts ---

bench_unary!(
    cast_mz_timestamp_to_string,
    f::CastMzTimestampToString,
    D::MzTimestamp(Timestamp::from(1705312245000u64)),
);
bench_unary!(
    cast_mz_timestamp_to_timestamp,
    f::CastMzTimestampToTimestamp,
    D::MzTimestamp(Timestamp::from(1705312245000u64)),
);
bench_unary!(
    cast_mz_timestamp_to_timestamp_tz,
    f::CastMzTimestampToTimestampTz,
    D::MzTimestamp(Timestamp::from(1705312245000u64)),
);
bench_unary!(
    cast_string_to_mz_timestamp,
    f::CastStringToMzTimestamp,
    "1705312245000",
);
bench_unary!(
    cast_uint64_to_mz_timestamp,
    f::CastUint64ToMzTimestamp,
    D::UInt64(1705312245000),
);
bench_unary!(
    cast_uint32_to_mz_timestamp,
    f::CastUint32ToMzTimestamp,
    D::UInt32(1705312245),
);
bench_unary!(
    cast_int64_to_mz_timestamp,
    f::CastInt64ToMzTimestamp,
    D::Int64(1705312245000),
);
bench_unary!(
    cast_int32_to_mz_timestamp,
    f::CastInt32ToMzTimestamp,
    D::Int32(1705312245),
);
bench_unary!(
    cast_numeric_to_mz_timestamp,
    f::CastNumericToMzTimestamp,
    Numeric::from(1705312245000i64),
);

fn cast_timestamp_to_mz_timestamp(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampToMzTimestamp);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_timestamp_tz_to_mz_timestamp(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastTimestampTzToMzTimestamp);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

bench_unary!(
    cast_date_to_mz_timestamp,
    f::CastDateToMzTimestamp,
    Date::from_pg_epoch(8_415).unwrap(),
);

bench_unary!(
    step_mz_timestamp,
    f::StepMzTimestamp,
    D::MzTimestamp(Timestamp::from(1705312245000u64)),
);

// --- PgLegacyChar / Char / VarChar casts ---

bench_unary!(cast_int32_to_pg_legacy_char, f::CastInt32ToPgLegacyChar, D::Int32(65));
bench_unary!(cast_string_to_pg_legacy_char, f::CastStringToPgLegacyChar, "A");
bench_unary!(cast_string_to_pg_legacy_name, f::CastStringToPgLegacyName, "my_table");
bench_unary!(cast_pg_legacy_char_to_string, f::CastPgLegacyCharToString, D::UInt8(65));
bench_unary!(cast_pg_legacy_char_to_char, f::CastPgLegacyCharToChar, D::UInt8(65));
bench_unary!(cast_pg_legacy_char_to_var_char, f::CastPgLegacyCharToVarChar, D::UInt8(65));
bench_unary!(cast_pg_legacy_char_to_int32, f::CastPgLegacyCharToInt32, D::UInt8(65));

bench_unary!(
    cast_string_to_char,
    f::CastStringToChar {
        length: None,
        fail_on_len: false,
    },
    "hello",
);
bench_unary!(
    cast_string_to_var_char,
    f::CastStringToVarChar {
        length: None,
        fail_on_len: false,
    },
    "hello",
);
bench_unary!(cast_char_to_string, f::CastCharToString, "hello");
bench_unary!(cast_var_char_to_string, f::CastVarCharToString, "hello");
bench_unary!(pad_char, f::PadChar { length: None }, "hello");

// --- OID / Reg* casts ---

bench_unary!(cast_oid_to_reg_class, f::CastOidToRegClass, D::UInt32(42));
bench_unary!(cast_reg_class_to_oid, f::CastRegClassToOid, D::UInt32(42));
bench_unary!(cast_oid_to_reg_proc, f::CastOidToRegProc, D::UInt32(42));
bench_unary!(cast_reg_proc_to_oid, f::CastRegProcToOid, D::UInt32(42));
bench_unary!(cast_oid_to_reg_type, f::CastOidToRegType, D::UInt32(42));
bench_unary!(cast_reg_type_to_oid, f::CastRegTypeToOid, D::UInt32(42));
bench_unary!(cast_string_to_oid, f::CastStringToOid, "42");

// --- Jsonb extras ---

fn jsonb_array_length(b: &mut Bencher) {
    let func = UnaryFunc::from(f::JsonbArrayLength);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let arr = sa.make_datum(|packer| {
        packer.push_list(&[D::Float64(OrderedFloat(1.0)), D::Float64(OrderedFloat(2.0)), D::Float64(OrderedFloat(3.0))]);
    });
    let arena = RowArena::new();
    let datums: &[D] = &[arr];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn jsonb_strip_nulls(b: &mut Bencher) {
    let func = UnaryFunc::from(f::JsonbStripNulls);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let obj = sa.make_datum(|packer| {
        packer.push_dict_with(|packer| {
            packer.push(D::String("a"));
            packer.push(D::Float64(OrderedFloat(1.0)));
            packer.push(D::String("b"));
            packer.push(D::Null);
        });
    });
    let arena = RowArena::new();
    let datums: &[D] = &[obj];
    b.iter(|| func.eval(datums, &arena, &a));
}

bench_unary!(cast_jsonbable_to_jsonb, f::CastJsonbableToJsonb, 42f64);
bench_unary!(
    cast_jsonb_to_numeric,
    f::CastJsonbToNumeric(None),
    42f64,
);

// --- Numeric extras ---

bench_unary!(
    adjust_numeric_scale,
    f::AdjustNumericScale(NumericMaxScale::try_from(2i64).unwrap()),
    Numeric::from(42),
);
bench_unary!(
    cast_string_to_numeric,
    f::CastStringToNumeric(None),
    "42.5",
);

// --- Extract / DatePart / DateTrunc ---

fn extract_interval(b: &mut Bencher) {
    let func = UnaryFunc::from(f::ExtractInterval(DateTimeUnits::Hour));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Interval(Interval::new(1, 2, 3_600_000_000))];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn extract_time(b: &mut Bencher) {
    let func = UnaryFunc::from(f::ExtractTime(DateTimeUnits::Hour));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Time(NaiveTime::from_hms_opt(12, 30, 45).unwrap())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn extract_timestamp(b: &mut Bencher) {
    let func = UnaryFunc::from(f::ExtractTimestamp(DateTimeUnits::Year));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn extract_timestamp_tz(b: &mut Bencher) {
    let func = UnaryFunc::from(f::ExtractTimestampTz(DateTimeUnits::Year));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn extract_date(b: &mut Bencher) {
    let func = UnaryFunc::from(f::ExtractDate(DateTimeUnits::Year));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Date(Date::from_pg_epoch(8_415).unwrap())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn date_part_interval(b: &mut Bencher) {
    let func = UnaryFunc::from(f::DatePartInterval(DateTimeUnits::Hour));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Interval(Interval::new(1, 2, 3_600_000_000))];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn date_part_time(b: &mut Bencher) {
    let func = UnaryFunc::from(f::DatePartTime(DateTimeUnits::Hour));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Time(NaiveTime::from_hms_opt(12, 30, 45).unwrap())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn date_part_timestamp(b: &mut Bencher) {
    let func = UnaryFunc::from(f::DatePartTimestamp(DateTimeUnits::Year));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn date_part_timestamp_tz(b: &mut Bencher) {
    let func = UnaryFunc::from(f::DatePartTimestampTz(DateTimeUnits::Year));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn date_trunc_timestamp(b: &mut Bencher) {
    let func = UnaryFunc::from(f::DateTruncTimestamp(DateTimeUnits::Hour));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn date_trunc_timestamp_tz(b: &mut Bencher) {
    let func = UnaryFunc::from(f::DateTruncTimestampTz(DateTimeUnits::Hour));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn adjust_timestamp_precision(b: &mut Bencher) {
    let func = UnaryFunc::from(f::AdjustTimestampPrecision {
        from: None,
        to: None,
    });
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn adjust_timestamp_tz_precision(b: &mut Bencher) {
    let func = UnaryFunc::from(f::AdjustTimestampTzPrecision {
        from: None,
        to: None,
    });
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

// --- Timezone ---

fn timezone_timestamp(b: &mut Bencher) {
    use mz_pgtz::timezone::Timezone;
    let func = UnaryFunc::from(f::TimezoneTimestamp(Timezone::Tz(chrono_tz::UTC)));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Timestamp(make_ts())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn timezone_timestamp_tz(b: &mut Bencher) {
    use mz_pgtz::timezone::Timezone;
    let func = UnaryFunc::from(f::TimezoneTimestampTz(Timezone::Tz(chrono_tz::UTC)));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::TimestampTz(make_tstz())];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn timezone_time(b: &mut Bencher) {
    use mz_pgtz::timezone::Timezone;
    let func = UnaryFunc::from(f::TimezoneTime {
        tz: Timezone::Tz(chrono_tz::UTC),
        wall_time: NaiveDate::from_ymd_opt(2024, 1, 15)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap(),
    });
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Time(NaiveTime::from_hms_opt(12, 30, 0).unwrap())];
    b.iter(|| func.eval(datums, &arena, &a));
}

// --- Pattern matching ---

fn is_like_match(b: &mut Bencher) {
    let matcher = like_pattern::compile("%world%", false).unwrap();
    let func = UnaryFunc::from(f::IsLikeMatch(matcher));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::String("hello world")];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn is_regexp_match(b: &mut Bencher) {
    let regex = Regex::new("\\d+", false).unwrap();
    let func = UnaryFunc::from(f::IsRegexpMatch(regex));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::String("hello 42 world")];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn regexp_match_unary(b: &mut Bencher) {
    let regex = Regex::new("(\\d+)", false).unwrap();
    let func = UnaryFunc::from(f::RegexpMatch(regex));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::String("hello 42 world")];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn regexp_split_to_array_unary(b: &mut Bencher) {
    let regex = Regex::new("\\d", false).unwrap();
    let func = UnaryFunc::from(f::RegexpSplitToArray(regex));
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::String("one1two2three")];
    b.iter(|| func.eval(datums, &arena, &a));
}

// --- Range functions ---

fn bench_range_empty(b: &mut Bencher) {
    let func = UnaryFunc::from(f::RangeEmpty);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let range = sa.make_datum(|packer| {
        packer
            .push_range(mz_repr::adt::range::Range::new(Some((
                mz_repr::adt::range::RangeBound::new(D::Int32(1), true),
                mz_repr::adt::range::RangeBound::new(D::Int32(10), false),
            ))))
            .unwrap();
    });
    let arena = RowArena::new();
    let datums: &[D] = &[range];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn bench_range_lower_inc(b: &mut Bencher) {
    let func = UnaryFunc::from(f::RangeLowerInc);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let range = sa.make_datum(|packer| {
        packer
            .push_range(mz_repr::adt::range::Range::new(Some((
                mz_repr::adt::range::RangeBound::new(D::Int32(1), true),
                mz_repr::adt::range::RangeBound::new(D::Int32(10), false),
            ))))
            .unwrap();
    });
    let arena = RowArena::new();
    let datums: &[D] = &[range];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn bench_range_upper_inc(b: &mut Bencher) {
    let func = UnaryFunc::from(f::RangeUpperInc);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let range = sa.make_datum(|packer| {
        packer
            .push_range(mz_repr::adt::range::Range::new(Some((
                mz_repr::adt::range::RangeBound::new(D::Int32(1), true),
                mz_repr::adt::range::RangeBound::new(D::Int32(10), false),
            ))))
            .unwrap();
    });
    let arena = RowArena::new();
    let datums: &[D] = &[range];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn bench_range_lower_inf(b: &mut Bencher) {
    let func = UnaryFunc::from(f::RangeLowerInf);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let range = sa.make_datum(|packer| {
        packer
            .push_range(mz_repr::adt::range::Range::new(Some((
                mz_repr::adt::range::RangeBound::new(D::Int32(1), true),
                mz_repr::adt::range::RangeBound::new(D::Int32(10), false),
            ))))
            .unwrap();
    });
    let arena = RowArena::new();
    let datums: &[D] = &[range];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn bench_range_upper_inf(b: &mut Bencher) {
    let func = UnaryFunc::from(f::RangeUpperInf);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let range = sa.make_datum(|packer| {
        packer
            .push_range(mz_repr::adt::range::Range::new(Some((
                mz_repr::adt::range::RangeBound::new(D::Int32(1), true),
                mz_repr::adt::range::RangeBound::new(D::Int32(10), false),
            ))))
            .unwrap();
    });
    let arena = RowArena::new();
    let datums: &[D] = &[range];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn bench_range_lower(b: &mut Bencher) {
    let func = UnaryFunc::from(f::RangeLower);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let range = sa.make_datum(|packer| {
        packer
            .push_range(mz_repr::adt::range::Range::new(Some((
                mz_repr::adt::range::RangeBound::new(D::Int32(1), true),
                mz_repr::adt::range::RangeBound::new(D::Int32(10), false),
            ))))
            .unwrap();
    });
    let arena = RowArena::new();
    let datums: &[D] = &[range];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn bench_range_upper(b: &mut Bencher) {
    let func = UnaryFunc::from(f::RangeUpper);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let range = sa.make_datum(|packer| {
        packer
            .push_range(mz_repr::adt::range::Range::new(Some((
                mz_repr::adt::range::RangeBound::new(D::Int32(1), true),
                mz_repr::adt::range::RangeBound::new(D::Int32(10), false),
            ))))
            .unwrap();
    });
    let arena = RowArena::new();
    let datums: &[D] = &[range];
    b.iter(|| func.eval(datums, &arena, &a));
}

// --- Collection ops ---

fn list_length(b: &mut Bencher) {
    let func = UnaryFunc::from(f::ListLength);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let list = sa.make_datum(|packer| packer.push_list(&[D::Int32(1), D::Int32(2), D::Int32(3)]));
    let arena = RowArena::new();
    let datums: &[D] = &[list];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn map_length(b: &mut Bencher) {
    let func = UnaryFunc::from(f::MapLength);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let map = sa.make_datum(|packer| {
        packer.push_dict_with(|packer| {
            packer.push(D::String("a"));
            packer.push(D::Int32(1));
            packer.push(D::String("b"));
            packer.push(D::Int32(2));
        });
    });
    let arena = RowArena::new();
    let datums: &[D] = &[map];
    b.iter(|| func.eval(datums, &arena, &a));
}

// --- ACL / privileges ---

fn mz_acl_item_grantor(b: &mut Bencher) {
    let func = UnaryFunc::from(f::MzAclItemGrantor);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let item = MzAclItem {
        grantee: RoleId::User(1),
        grantor: RoleId::User(2),
        acl_mode: AclMode::empty(),
    };
    let datums: &[D] = &[D::MzAclItem(item)];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn mz_acl_item_grantee(b: &mut Bencher) {
    let func = UnaryFunc::from(f::MzAclItemGrantee);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let item = MzAclItem {
        grantee: RoleId::User(1),
        grantor: RoleId::User(2),
        acl_mode: AclMode::empty(),
    };
    let datums: &[D] = &[D::MzAclItem(item)];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn mz_acl_item_privileges(b: &mut Bencher) {
    let func = UnaryFunc::from(f::MzAclItemPrivileges);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let item = MzAclItem {
        grantee: RoleId::User(1),
        grantor: RoleId::User(2),
        acl_mode: AclMode::empty(),
    };
    let datums: &[D] = &[D::MzAclItem(item)];
    b.iter(|| func.eval(datums, &arena, &a));
}

bench_unary!(mz_format_privileges, f::MzFormatPrivileges, "r");
bench_unary!(mz_validate_privileges, f::MzValidatePrivileges, "r");
bench_unary!(mz_validate_role_privilege, f::MzValidateRolePrivilege, "USAGE");

fn acl_item_grantor(b: &mut Bencher) {
    let func = UnaryFunc::from(f::AclItemGrantor);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let item = AclItem::empty(Oid(1), Oid(2));
    let datums: &[D] = &[D::AclItem(item)];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn acl_item_grantee(b: &mut Bencher) {
    let func = UnaryFunc::from(f::AclItemGrantee);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let item = AclItem::empty(Oid(1), Oid(2));
    let datums: &[D] = &[D::AclItem(item)];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn acl_item_privileges(b: &mut Bencher) {
    let func = UnaryFunc::from(f::AclItemPrivileges);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let item = AclItem::empty(Oid(1), Oid(2));
    let datums: &[D] = &[D::AclItem(item)];
    b.iter(|| func.eval(datums, &arena, &a));
}

// --- Misc ---

bench_unary!(mz_type_name, f::MzTypeName, D::UInt32(25)); // text oid
bench_unary!(
    try_parse_monotonic_iso8601_timestamp,
    f::TryParseMonotonicIso8601Timestamp,
    "2024-01-15T12:30:45Z",
);

fn pg_column_size(b: &mut Bencher) {
    let func = UnaryFunc::from(f::PgColumnSize);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::Int32(42)];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn mz_row_size(b: &mut Bencher) {
    let func = UnaryFunc::from(f::MzRowSize);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let list = sa.make_datum(|packer| packer.push_list(&[D::Int32(1), D::Int32(2), D::Int32(3)]));
    let arena = RowArena::new();
    let datums: &[D] = &[list];
    b.iter(|| func.eval(datums, &arena, &a));
}

// --- Int2Vector / String casts ---

fn cast_string_to_int2_vector(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastStringToInt2Vector);
    let a = MirScalarExpr::column(0);
    let arena = RowArena::new();
    let datums: &[D] = &[D::String("1 2 3")];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_int2_vector_to_string(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastInt2VectorToString);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let vec = sa.make_datum(|packer| {
        packer.try_push_array(&[mz_repr::adt::array::ArrayDimension { lower_bound: 1, length: 3 }], &[D::Int16(1), D::Int16(2), D::Int16(3)]).unwrap();
    });
    let arena = RowArena::new();
    let datums: &[D] = &[vec];
    b.iter(|| func.eval(datums, &arena, &a));
}

fn cast_int2_vector_to_array(b: &mut Bencher) {
    let func = UnaryFunc::from(f::CastInt2VectorToArray);
    let a = MirScalarExpr::column(0);
    let sa = RowArena::new();
    let vec = sa.make_datum(|packer| {
        packer.try_push_array(&[mz_repr::adt::array::ArrayDimension { lower_bound: 1, length: 3 }], &[D::Int16(1), D::Int16(2), D::Int16(3)]).unwrap();
    });
    let arena = RowArena::new();
    let datums: &[D] = &[vec];
    b.iter(|| func.eval(datums, &arena, &a));
}

// Benchmark registration

benchmark_group!(
    arithmetic_benches,
    // Boolean
    not,
    is_null,
    is_true,
    is_false,
    // Neg
    neg_int16,
    neg_int16_error,
    neg_int32,
    neg_int32_error,
    neg_int64,
    neg_int64_error,
    neg_float32,
    neg_float64,
    neg_numeric,
    neg_interval,
    // Abs
    abs_int16,
    abs_int32,
    abs_int64,
    abs_float32,
    abs_float64,
    abs_numeric,
    // BitNot
    bit_not_int16,
    bit_not_int32,
    bit_not_int64,
    bit_not_uint16,
    bit_not_uint32,
    bit_not_uint64 // Sqrt/Cbrt, Ceil/Floor/Round/Trunc, Log/Ln/Exp, Trig, Degrees/Radians
                   // are in *_group functions via benchmark_main
);

benchmark_group!(
    cast_benches,
    // Bool
    cast_bool_to_string,
    cast_bool_to_string_nonstandard,
    cast_bool_to_int32,
    cast_bool_to_int64,
    // Int16
    cast_int16_to_float32,
    cast_int16_to_float64,
    cast_int16_to_int32,
    cast_int16_to_int64,
    cast_int16_to_uint16,
    cast_int16_to_uint32,
    cast_int16_to_uint64,
    cast_int16_to_string,
    cast_int16_to_numeric,
    // Int32
    cast_int32_to_bool,
    cast_int32_to_float32,
    cast_int32_to_float64,
    cast_int32_to_int16,
    cast_int32_to_int64,
    cast_int32_to_uint16,
    cast_int32_to_uint32,
    cast_int32_to_uint64,
    cast_int32_to_string,
    cast_int32_to_oid,
    cast_int32_to_numeric,
    // Int64
    cast_int64_to_int16,
    cast_int64_to_int32,
    cast_int64_to_uint16,
    cast_int64_to_uint32,
    cast_int64_to_uint64,
    cast_int64_to_bool,
    cast_int64_to_float32,
    cast_int64_to_float64,
    cast_int64_to_string,
    cast_int64_to_oid,
    cast_int64_to_numeric,
    // Uint16
    cast_uint16_to_uint32,
    cast_uint16_to_uint64,
    cast_uint16_to_int16,
    cast_uint16_to_int32,
    cast_uint16_to_int64,
    cast_uint16_to_float32,
    cast_uint16_to_float64,
    cast_uint16_to_string,
    cast_uint16_to_numeric,
    // Uint32
    cast_uint32_to_uint16,
    cast_uint32_to_uint64,
    cast_uint32_to_int16,
    cast_uint32_to_int32,
    cast_uint32_to_int64,
    cast_uint32_to_float32,
    cast_uint32_to_float64,
    cast_uint32_to_string,
    cast_uint32_to_numeric,
    // Uint64
    cast_uint64_to_uint16,
    cast_uint64_to_uint32,
    cast_uint64_to_int16,
    cast_uint64_to_int32,
    cast_uint64_to_int64,
    cast_uint64_to_float32,
    cast_uint64_to_float64,
    cast_uint64_to_string,
    cast_uint64_to_numeric,
    // Float32
    cast_float32_to_int16,
    cast_float32_to_int32,
    cast_float32_to_int64,
    cast_float32_to_uint16,
    cast_float32_to_uint32,
    cast_float32_to_uint64,
    cast_float32_to_float64,
    cast_float32_to_string,
    cast_float32_to_numeric,
    // Float64
    cast_float64_to_int16,
    cast_float64_to_int32,
    cast_float64_to_int64,
    cast_float64_to_uint16,
    cast_float64_to_uint32,
    cast_float64_to_uint64,
    cast_float64_to_float32,
    cast_float64_to_string,
    cast_float64_to_numeric,
    // Numeric
    cast_numeric_to_float32,
    cast_numeric_to_float64,
    cast_numeric_to_int16,
    cast_numeric_to_int32,
    cast_numeric_to_int64,
    cast_numeric_to_uint16,
    cast_numeric_to_uint32,
    cast_numeric_to_uint64,
    cast_numeric_to_string,
    // String parsing
    cast_string_to_bool,
    cast_string_to_int16,
    cast_string_to_int32,
    cast_string_to_int64,
    cast_string_to_uint16,
    cast_string_to_uint32,
    cast_string_to_uint64,
    cast_string_to_float32,
    cast_string_to_float64,
    cast_string_to_date,
    cast_string_to_time,
    cast_string_to_timestamp,
    cast_string_to_timestamp_tz,
    cast_string_to_interval,
    cast_string_to_uuid,
    cast_string_to_jsonb,
    cast_string_to_bytes,
    // Other casts
    cast_oid_to_int32,
    cast_oid_to_int64,
    cast_oid_to_string,
    cast_uuid_to_string,
    cast_bytes_to_string,
    cast_jsonb_to_string,
    cast_jsonb_to_int16,
    cast_jsonb_to_int32,
    cast_jsonb_to_int64,
    cast_jsonb_to_float32,
    cast_jsonb_to_float64,
    cast_jsonb_to_bool,
    // Timestamp/Date casts
    cast_date_to_timestamp,
    cast_date_to_timestamp_tz,
    cast_timestamp_to_date,
    cast_timestamp_to_time,
    cast_timestamp_to_string,
    cast_timestamp_to_timestamp_tz,
    cast_timestamp_tz_to_date,
    cast_timestamp_tz_to_timestamp,
    cast_timestamp_tz_to_string,
    cast_timestamp_tz_to_time,
    to_timestamp,
    // MzTimestamp casts
    cast_mz_timestamp_to_string,
    cast_mz_timestamp_to_timestamp,
    cast_mz_timestamp_to_timestamp_tz,
    cast_string_to_mz_timestamp,
    cast_uint64_to_mz_timestamp,
    cast_uint32_to_mz_timestamp,
    cast_int64_to_mz_timestamp,
    cast_int32_to_mz_timestamp,
    cast_numeric_to_mz_timestamp,
    cast_timestamp_to_mz_timestamp,
    cast_timestamp_tz_to_mz_timestamp,
    cast_date_to_mz_timestamp,
    step_mz_timestamp,
    // PgLegacyChar / Char / VarChar
    cast_int32_to_pg_legacy_char,
    cast_string_to_pg_legacy_char,
    cast_string_to_pg_legacy_name,
    cast_pg_legacy_char_to_string,
    cast_pg_legacy_char_to_char,
    cast_pg_legacy_char_to_var_char,
    cast_pg_legacy_char_to_int32,
    cast_string_to_char,
    cast_string_to_var_char,
    cast_char_to_string,
    cast_var_char_to_string,
    pad_char,
    // OID / Reg*
    cast_oid_to_reg_class,
    cast_reg_class_to_oid,
    cast_oid_to_reg_proc,
    cast_reg_proc_to_oid,
    cast_oid_to_reg_type,
    cast_reg_type_to_oid,
    cast_string_to_oid,
    // Jsonb extras
    jsonb_array_length,
    jsonb_strip_nulls,
    cast_jsonbable_to_jsonb,
    cast_jsonb_to_numeric,
    // Numeric extras
    adjust_numeric_scale,
    cast_string_to_numeric,
    // Int2Vector
    cast_string_to_int2_vector,
    cast_int2_vector_to_string,
    cast_int2_vector_to_array
);

benchmark_group!(
    remaining_benches,
    // String operations
    upper,
    lower,
    initcap,
    trim_whitespace,
    trim_leading_whitespace,
    trim_trailing_whitespace,
    ascii,
    char_length,
    bit_length_bytes,
    bit_length_string,
    byte_length_bytes,
    byte_length_string,
    bit_count_bytes,
    chr,
    reverse,
    quote_ident,
    pg_size_pretty,
    // Jsonb
    jsonb_typeof,
    jsonb_pretty,
    // Date/time
    justify_days,
    justify_hours,
    justify_interval,
    cast_date_to_string,
    cast_time_to_string,
    cast_interval_to_string,
    cast_interval_to_time,
    cast_time_to_interval,
    // Hash functions
    crc32_bytes,
    crc32_string,
    kafka_murmur2_bytes,
    kafka_murmur2_string,
    seahash_bytes,
    seahash_string,
    // Extract / DatePart / DateTrunc
    extract_interval,
    extract_time,
    extract_timestamp,
    extract_timestamp_tz,
    extract_date,
    date_part_interval,
    date_part_time,
    date_part_timestamp,
    date_part_timestamp_tz,
    date_trunc_timestamp,
    date_trunc_timestamp_tz,
    adjust_timestamp_precision,
    adjust_timestamp_tz_precision,
    // Timezone
    timezone_timestamp,
    timezone_timestamp_tz,
    timezone_time,
    // Pattern matching
    is_like_match,
    is_regexp_match,
    regexp_match_unary,
    regexp_split_to_array_unary,
    // Range
    bench_range_empty,
    bench_range_lower_inc,
    bench_range_upper_inc,
    bench_range_lower_inf,
    bench_range_upper_inf,
    bench_range_lower,
    bench_range_upper,
    // Collection ops
    list_length,
    map_length,
    // ACL / privileges
    mz_acl_item_grantor,
    mz_acl_item_grantee,
    mz_acl_item_privileges,
    mz_format_privileges,
    mz_validate_privileges,
    mz_validate_role_privilege,
    acl_item_grantor,
    acl_item_grantee,
    acl_item_privileges,
    // Misc
    mz_type_name,
    try_parse_monotonic_iso8601_timestamp,
    pg_column_size,
    mz_row_size
);

benchmark_main!(
    arithmetic_benches,
    cast_benches,
    remaining_benches,
    // Multi-input benchmark groups (each input is a separate benchmark entry)
    sqrt_float64_group,
    sqrt_numeric_group,
    cbrt_float64_group,
    ceil_float32_group,
    ceil_float64_group,
    ceil_numeric_group,
    floor_float32_group,
    floor_float64_group,
    floor_numeric_group,
    round_float32_group,
    round_float64_group,
    round_numeric_group,
    trunc_float32_group,
    trunc_float64_group,
    trunc_numeric_group,
    log10_group,
    log10_numeric_group,
    ln_group,
    ln_numeric_group,
    exp_group,
    exp_numeric_group,
    cos_group,
    acos_group,
    cosh_group,
    acosh_group,
    sin_group,
    asin_group,
    sinh_group,
    asinh_group,
    tan_group,
    atan_group,
    tanh_group,
    atanh_group,
    cot_group,
    degrees_group,
    radians_group
);
