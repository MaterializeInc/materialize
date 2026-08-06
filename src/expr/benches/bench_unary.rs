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
use mz_expr::like_pattern;
use mz_expr::{MirScalarExpr, UnaryFunc, func as f};
use mz_repr::adt::date::Date;
use mz_repr::adt::datetime::DateTimeUnits;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::mz_acl_item::{AclItem, AclMode, MzAclItem};
use mz_repr::adt::numeric::{Numeric, NumericMaxScale};
use mz_repr::adt::regex::Regex;
use mz_repr::adt::system::Oid;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::role_id::RoleId;
use mz_repr::{Datum as D, RowArena, Timestamp};
use ordered_float::OrderedFloat;
use uuid::Uuid;

macro_rules! bench_unary {
    ($c:expr, $bench_name:ident, $struct_expr:expr,
     $a_datum:expr $(,)?) => {
        $c.bench_function(stringify!($bench_name), |b| {
            let f = UnaryFunc::from($struct_expr);
            let a = MirScalarExpr::column(0);
            let arena = RowArena::new();
            let datums: &[D] = &[($a_datum).into()];
            b.iter(|| f.eval(datums, &arena, &a));
        });
    };
}

/// Generates a criterion benchmark group with individual benchmark entries
/// for each input, so they show up as separate lines in benchmark output.
macro_rules! bench_unary_multi {
    ($c:expr, $bench_name:ident, $struct_expr:expr,
     [ $( $a_datum:expr ),+ $(,)? ]) => {
        {
            let mut group = $c.benchmark_group(stringify!($bench_name));
            $(
                group.bench_function(stringify!($a_datum), |b| {
                    let f = UnaryFunc::from($struct_expr);
                    let a = MirScalarExpr::column(0);
                    let arena = RowArena::new();
                    let datums: &[D] = &[($a_datum).into()];
                    b.iter(|| f.eval(datums, &arena, &a));
                });
            )+
            group.finish();
        }
    };
}

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

// Unary arithmetic + bitwise functions

fn arithmetic_benches(c: &mut Criterion) {
    // Boolean
    bench_unary!(c, not, f::Not, true);
    bench_unary!(c, is_null, f::IsNull, D::Int32(42));
    bench_unary!(c, is_true, f::IsTrue, true);
    bench_unary!(c, is_false, f::IsFalse, false);

    // Neg
    bench_unary!(c, neg_int16, f::NegInt16, 42i16);
    bench_unary!(c, neg_int16_error, f::NegInt16, i16::MIN);
    bench_unary!(c, neg_int32, f::NegInt32, 42i32);
    bench_unary!(c, neg_int32_error, f::NegInt32, i32::MIN);
    bench_unary!(c, neg_int64, f::NegInt64, 42i64);
    bench_unary!(c, neg_int64_error, f::NegInt64, i64::MIN);
    bench_unary!(c, neg_float32, f::NegFloat32, 42.0f32);
    bench_unary!(c, neg_float64, f::NegFloat64, 42.0f64);
    bench_unary!(c, neg_numeric, f::NegNumeric, Numeric::from(42));
    bench_unary!(
        c,
        neg_interval,
        f::NegInterval,
        Interval::new(1, 2, 3_000_000)
    );

    // Abs
    bench_unary!(c, abs_int16, f::AbsInt16, -42i16);
    bench_unary!(c, abs_int32, f::AbsInt32, -42i32);
    bench_unary!(c, abs_int64, f::AbsInt64, -42i64);
    bench_unary!(c, abs_float32, f::AbsFloat32, -42.0f32);
    bench_unary!(c, abs_float64, f::AbsFloat64, -42.0f64);
    bench_unary!(c, abs_numeric, f::AbsNumeric, Numeric::from(-42));

    // BitNot
    bench_unary!(c, bit_not_int16, f::BitNotInt16, D::Int16(0x0F0F));
    bench_unary!(c, bit_not_int32, f::BitNotInt32, D::Int32(0x0F0F0F0F));
    bench_unary!(c, bit_not_int64, f::BitNotInt64, D::Int64(0x0F0F0F0F));
    bench_unary!(c, bit_not_uint16, f::BitNotUint16, D::UInt16(0x0F0F));
    bench_unary!(c, bit_not_uint32, f::BitNotUint32, D::UInt32(0x0F0F));
    bench_unary!(c, bit_not_uint64, f::BitNotUint64, D::UInt64(0x0F0F));

    // Sqrt/Cbrt (8 diverse inputs each: perfect squares, primes, tiny, huge, fractional)
    bench_unary_multi!(
        c,
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
        c,
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
        c,
        cbrt_float64,
        f::CbrtFloat64,
        [
            0.0f64, 0.001f64, 2.0f64, 27.0f64, 30.7f64, 1e9f64, 1.5e100f64, -8.0f64,
        ]
    );

    // Ceil/Floor/Round/Trunc (diverse fractional values)
    bench_unary_multi!(
        c,
        ceil_float32,
        f::CeilFloat32,
        [
            0.0f32, 0.1f32, 3.17f32, -2.7f32, 999.999f32, -0.001f32, 1e10f32, 0.5f32,
        ]
    );
    bench_unary_multi!(
        c,
        ceil_float64,
        f::CeilFloat64,
        [
            0.0f64, 0.1f64, 3.17f64, -2.7f64, 999.999f64, -0.001f64, 1e15f64, 0.5f64,
        ]
    );
    bench_unary_multi!(
        c,
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
        c,
        floor_float32,
        f::FloorFloat32,
        [0.0f32, 3.17f32, -2.7f32, 999.999f32, -0.001f32, 0.5f32,]
    );
    bench_unary_multi!(
        c,
        floor_float64,
        f::FloorFloat64,
        [0.0f64, 3.17f64, -2.7f64, 999.999f64, -0.001f64, 0.5f64,]
    );
    bench_unary_multi!(
        c,
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
        c,
        round_float32,
        f::RoundFloat32,
        [0.0f32, 3.17f32, -2.7f32, 0.5f32, 999.999f32, -0.5f32,]
    );
    bench_unary_multi!(
        c,
        round_float64,
        f::RoundFloat64,
        [0.0f64, 3.17f64, -2.7f64, 0.5f64, 999.999f64, -0.5f64,]
    );
    bench_unary_multi!(
        c,
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
        c,
        trunc_float32,
        f::TruncFloat32,
        [0.0f32, 3.17f32, -2.7f32, 999.999f32,]
    );
    bench_unary_multi!(
        c,
        trunc_float64,
        f::TruncFloat64,
        [0.0f64, 3.17f64, -2.7f64, 999.999f64,]
    );
    bench_unary_multi!(
        c,
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
        c,
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
        c,
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
        c,
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
        c,
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
        c,
        exp,
        f::Exp,
        [
            0.0f64, 1.0f64, 2.0f64, -1.0f64, 10.0f64, 0.5f64, -5.0f64, 20.0f64,
        ]
    );
    bench_unary_multi!(
        c,
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
        c,
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
        c,
        acos,
        f::Acos,
        [
            -1.0f64, -0.5f64, 0.0f64, 0.1f64, 0.5f64, 0.9f64, 0.999f64, 1.0f64,
        ]
    );
    bench_unary_multi!(
        c,
        cosh,
        f::Cosh,
        [
            0.0f64, 0.5f64, 1.0f64, 2.0f64, 5.0f64, -1.0f64, 10.0f64, -5.0f64,
        ]
    );
    bench_unary_multi!(
        c,
        acosh,
        f::Acosh,
        [
            1.0f64, 1.5f64, 2.0f64, 5.0f64, 10.0f64, 100.0f64, 1.001f64, 50.0f64,
        ]
    );
    bench_unary_multi!(
        c,
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
        c,
        asin,
        f::Asin,
        [
            -1.0f64, -0.5f64, 0.0f64, 0.1f64, 0.5f64, 0.9f64, 0.999f64, 1.0f64,
        ]
    );
    bench_unary_multi!(
        c,
        sinh,
        f::Sinh,
        [
            0.0f64, 0.5f64, 1.0f64, 2.0f64, 5.0f64, -1.0f64, 10.0f64, -5.0f64,
        ]
    );
    bench_unary_multi!(
        c,
        asinh,
        f::Asinh,
        [
            0.0f64, 0.5f64, 1.0f64, 10.0f64, 100.0f64, -1.0f64, -10.0f64, 0.001f64,
        ]
    );
    bench_unary_multi!(
        c,
        tan,
        f::Tan,
        [
            0.0f64, 0.5f64, 1.0f64, 0.785f64, // ~pi/4
            -1.0f64, 1.5f64, // close to pi/2
            3.0f64, 100.0f64,
        ]
    );
    bench_unary_multi!(
        c,
        atan,
        f::Atan,
        [
            0.0f64, 0.5f64, 1.0f64, 10.0f64, 100.0f64, -1.0f64, -100.0f64, 0.001f64,
        ]
    );
    bench_unary_multi!(
        c,
        tanh,
        f::Tanh,
        [
            0.0f64, 0.5f64, 1.0f64, 5.0f64, -1.0f64, -5.0f64, 20.0f64, 0.001f64,
        ]
    );
    bench_unary_multi!(
        c,
        atanh,
        f::Atanh,
        [
            0.0f64, 0.1f64, 0.5f64, 0.9f64, 0.99f64, -0.5f64, -0.9f64, 0.001f64,
        ]
    );
    bench_unary_multi!(
        c,
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
        c,
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
        c,
        radians,
        f::Radians,
        [
            0.0f64, 45.0f64, 90.0f64, 180.0f64, 360.0f64, -180.0f64, 1.0f64, 0.001f64,
        ]
    );
}

// Unary cast functions (unit struct casts only)

fn cast_benches(c: &mut Criterion) {
    // Bool casts
    bench_unary!(c, cast_bool_to_string, f::CastBoolToString, true);
    bench_unary!(
        c,
        cast_bool_to_string_nonstandard,
        f::CastBoolToStringNonstandard,
        true,
    );
    bench_unary!(c, cast_bool_to_int32, f::CastBoolToInt32, true);
    bench_unary!(c, cast_bool_to_int64, f::CastBoolToInt64, true);

    // Int16 casts
    bench_unary!(
        c,
        cast_int16_to_float32,
        f::CastInt16ToFloat32,
        D::Int16(42)
    );
    bench_unary!(
        c,
        cast_int16_to_float64,
        f::CastInt16ToFloat64,
        D::Int16(42)
    );
    bench_unary!(c, cast_int16_to_int32, f::CastInt16ToInt32, D::Int16(42));
    bench_unary!(c, cast_int16_to_int64, f::CastInt16ToInt64, D::Int16(42));
    bench_unary!(c, cast_int16_to_uint16, f::CastInt16ToUint16, D::Int16(42));
    bench_unary!(c, cast_int16_to_uint32, f::CastInt16ToUint32, D::Int16(42));
    bench_unary!(c, cast_int16_to_uint64, f::CastInt16ToUint64, D::Int16(42));
    bench_unary!(c, cast_int16_to_string, f::CastInt16ToString, D::Int16(42));

    // Int32 casts
    bench_unary!(c, cast_int32_to_bool, f::CastInt32ToBool, D::Int32(1));
    bench_unary!(
        c,
        cast_int32_to_float32,
        f::CastInt32ToFloat32,
        D::Int32(42)
    );
    bench_unary!(
        c,
        cast_int32_to_float64,
        f::CastInt32ToFloat64,
        D::Int32(42)
    );
    bench_unary!(c, cast_int32_to_int16, f::CastInt32ToInt16, D::Int32(42));
    bench_unary!(c, cast_int32_to_int64, f::CastInt32ToInt64, D::Int32(42));
    bench_unary!(c, cast_int32_to_uint16, f::CastInt32ToUint16, D::Int32(42));
    bench_unary!(c, cast_int32_to_uint32, f::CastInt32ToUint32, D::Int32(42));
    bench_unary!(c, cast_int32_to_uint64, f::CastInt32ToUint64, D::Int32(42));
    bench_unary!(c, cast_int32_to_string, f::CastInt32ToString, D::Int32(42));
    bench_unary!(c, cast_int32_to_oid, f::CastInt32ToOid, D::Int32(42));

    // Int64 casts
    bench_unary!(c, cast_int64_to_int16, f::CastInt64ToInt16, D::Int64(42));
    bench_unary!(c, cast_int64_to_int32, f::CastInt64ToInt32, D::Int64(42));
    bench_unary!(c, cast_int64_to_uint16, f::CastInt64ToUint16, D::Int64(42));
    bench_unary!(c, cast_int64_to_uint32, f::CastInt64ToUint32, D::Int64(42));
    bench_unary!(c, cast_int64_to_uint64, f::CastInt64ToUint64, D::Int64(42));
    bench_unary!(c, cast_int64_to_bool, f::CastInt64ToBool, D::Int64(1));
    bench_unary!(
        c,
        cast_int64_to_float32,
        f::CastInt64ToFloat32,
        D::Int64(42)
    );
    bench_unary!(
        c,
        cast_int64_to_float64,
        f::CastInt64ToFloat64,
        D::Int64(42)
    );
    bench_unary!(c, cast_int64_to_string, f::CastInt64ToString, D::Int64(42));
    bench_unary!(c, cast_int64_to_oid, f::CastInt64ToOid, D::Int64(42));

    // Uint16 casts
    bench_unary!(
        c,
        cast_uint16_to_uint32,
        f::CastUint16ToUint32,
        D::UInt16(42)
    );
    bench_unary!(
        c,
        cast_uint16_to_uint64,
        f::CastUint16ToUint64,
        D::UInt16(42)
    );
    bench_unary!(c, cast_uint16_to_int16, f::CastUint16ToInt16, D::UInt16(42));
    bench_unary!(c, cast_uint16_to_int32, f::CastUint16ToInt32, D::UInt16(42));
    bench_unary!(c, cast_uint16_to_int64, f::CastUint16ToInt64, D::UInt16(42));
    bench_unary!(c, cast_uint16_to_float32, f::CastUint16ToFloat32, 42u16);
    bench_unary!(c, cast_uint16_to_float64, f::CastUint16ToFloat64, 42u16);
    bench_unary!(
        c,
        cast_uint16_to_string,
        f::CastUint16ToString,
        D::UInt16(42)
    );

    // Uint32 casts
    bench_unary!(
        c,
        cast_uint32_to_uint16,
        f::CastUint32ToUint16,
        D::UInt32(42)
    );
    bench_unary!(
        c,
        cast_uint32_to_uint64,
        f::CastUint32ToUint64,
        D::UInt32(42)
    );
    bench_unary!(c, cast_uint32_to_int16, f::CastUint32ToInt16, D::UInt32(42));
    bench_unary!(c, cast_uint32_to_int32, f::CastUint32ToInt32, D::UInt32(42));
    bench_unary!(c, cast_uint32_to_int64, f::CastUint32ToInt64, D::UInt32(42));
    bench_unary!(c, cast_uint32_to_float32, f::CastUint32ToFloat32, 42u32);
    bench_unary!(c, cast_uint32_to_float64, f::CastUint32ToFloat64, 42u32);
    bench_unary!(
        c,
        cast_uint32_to_string,
        f::CastUint32ToString,
        D::UInt32(42)
    );

    // Uint64 casts
    bench_unary!(
        c,
        cast_uint64_to_uint16,
        f::CastUint64ToUint16,
        D::UInt64(42)
    );
    bench_unary!(
        c,
        cast_uint64_to_uint32,
        f::CastUint64ToUint32,
        D::UInt64(42)
    );
    bench_unary!(c, cast_uint64_to_int16, f::CastUint64ToInt16, D::UInt64(42));
    bench_unary!(c, cast_uint64_to_int32, f::CastUint64ToInt32, D::UInt64(42));
    bench_unary!(c, cast_uint64_to_int64, f::CastUint64ToInt64, D::UInt64(42));
    bench_unary!(c, cast_uint64_to_float32, f::CastUint64ToFloat32, 42u64);
    bench_unary!(c, cast_uint64_to_float64, f::CastUint64ToFloat64, 42u64);
    bench_unary!(
        c,
        cast_uint64_to_string,
        f::CastUint64ToString,
        D::UInt64(42)
    );

    // Float32 casts
    bench_unary!(c, cast_float32_to_int16, f::CastFloat32ToInt16, 42.0f32);
    bench_unary!(c, cast_float32_to_int32, f::CastFloat32ToInt32, 42.0f32);
    bench_unary!(c, cast_float32_to_int64, f::CastFloat32ToInt64, 42.0f32);
    bench_unary!(c, cast_float32_to_uint16, f::CastFloat32ToUint16, 42.0f32);
    bench_unary!(c, cast_float32_to_uint32, f::CastFloat32ToUint32, 42.0f32);
    bench_unary!(c, cast_float32_to_uint64, f::CastFloat32ToUint64, 42.0f32);
    bench_unary!(c, cast_float32_to_float64, f::CastFloat32ToFloat64, 42.0f32);
    bench_unary!(c, cast_float32_to_string, f::CastFloat32ToString, 42.0f32);

    // Float64 casts
    bench_unary!(c, cast_float64_to_int16, f::CastFloat64ToInt16, 42.0f64);
    bench_unary!(c, cast_float64_to_int32, f::CastFloat64ToInt32, 42.0f64);
    bench_unary!(c, cast_float64_to_int64, f::CastFloat64ToInt64, 42.0f64);
    bench_unary!(c, cast_float64_to_uint16, f::CastFloat64ToUint16, 42.0f64);
    bench_unary!(c, cast_float64_to_uint32, f::CastFloat64ToUint32, 42.0f64);
    bench_unary!(c, cast_float64_to_uint64, f::CastFloat64ToUint64, 42.0f64);
    bench_unary!(c, cast_float64_to_float32, f::CastFloat64ToFloat32, 42.0f64);
    bench_unary!(c, cast_float64_to_string, f::CastFloat64ToString, 42.0f64);

    // Numeric casts
    bench_unary!(
        c,
        cast_numeric_to_float32,
        f::CastNumericToFloat32,
        Numeric::from(42)
    );
    bench_unary!(
        c,
        cast_numeric_to_float64,
        f::CastNumericToFloat64,
        Numeric::from(42)
    );
    bench_unary!(
        c,
        cast_numeric_to_int16,
        f::CastNumericToInt16,
        Numeric::from(42),
    );
    bench_unary!(
        c,
        cast_numeric_to_int32,
        f::CastNumericToInt32,
        Numeric::from(42),
    );
    bench_unary!(
        c,
        cast_numeric_to_int64,
        f::CastNumericToInt64,
        Numeric::from(42),
    );
    bench_unary!(
        c,
        cast_numeric_to_uint16,
        f::CastNumericToUint16,
        Numeric::from(42),
    );
    bench_unary!(
        c,
        cast_numeric_to_uint32,
        f::CastNumericToUint32,
        Numeric::from(42),
    );
    bench_unary!(
        c,
        cast_numeric_to_uint64,
        f::CastNumericToUint64,
        Numeric::from(42),
    );
    bench_unary!(
        c,
        cast_numeric_to_string,
        f::CastNumericToString,
        Numeric::from(42),
    );

    // Parameterized casts (XToNumeric)
    bench_unary!(c, cast_int16_to_numeric, f::CastInt16ToNumeric(None), 42i16);
    bench_unary!(c, cast_int32_to_numeric, f::CastInt32ToNumeric(None), 42i32);
    bench_unary!(c, cast_int64_to_numeric, f::CastInt64ToNumeric(None), 42i64);
    bench_unary!(
        c,
        cast_uint16_to_numeric,
        f::CastUint16ToNumeric(None),
        42u16
    );
    bench_unary!(
        c,
        cast_uint32_to_numeric,
        f::CastUint32ToNumeric(None),
        42u32
    );
    bench_unary!(
        c,
        cast_uint64_to_numeric,
        f::CastUint64ToNumeric(None),
        42u64
    );
    bench_unary!(
        c,
        cast_float32_to_numeric,
        f::CastFloat32ToNumeric(None),
        42f32
    );
    bench_unary!(
        c,
        cast_float64_to_numeric,
        f::CastFloat64ToNumeric(None),
        42f64
    );

    // String parsing casts
    bench_unary!(c, cast_string_to_bool, f::CastStringToBool, "true");
    bench_unary!(c, cast_string_to_int16, f::CastStringToInt16, "42");
    bench_unary!(c, cast_string_to_int32, f::CastStringToInt32, "42");
    bench_unary!(c, cast_string_to_int64, f::CastStringToInt64, "42");
    bench_unary!(c, cast_string_to_uint16, f::CastStringToUint16, "42");
    bench_unary!(c, cast_string_to_uint32, f::CastStringToUint32, "42");
    bench_unary!(c, cast_string_to_uint64, f::CastStringToUint64, "42");
    bench_unary!(c, cast_string_to_float32, f::CastStringToFloat32, "42.0");
    bench_unary!(c, cast_string_to_float64, f::CastStringToFloat64, "42.0");
    bench_unary!(c, cast_string_to_date, f::CastStringToDate, "2024-01-15");
    bench_unary!(c, cast_string_to_time, f::CastStringToTime, "12:30:45");
    bench_unary!(
        c,
        cast_string_to_timestamp,
        f::CastStringToTimestamp(None),
        "2024-01-15 12:30:45"
    );
    bench_unary!(
        c,
        cast_string_to_timestamp_tz,
        f::CastStringToTimestampTz(None),
        "2024-01-15 12:30:45+00"
    );
    bench_unary!(
        c,
        cast_string_to_interval,
        f::CastStringToInterval,
        "1 day 2 hours"
    );
    bench_unary!(
        c,
        cast_string_to_uuid,
        f::CastStringToUuid,
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    );
    bench_unary!(c, cast_string_to_jsonb, f::CastStringToJsonb, "42");
    bench_unary!(c, cast_string_to_bytes, f::CastStringToBytes, "\\xDEADBEEF");

    // Other casts
    bench_unary!(c, cast_oid_to_int32, f::CastOidToInt32, D::UInt32(42));
    bench_unary!(c, cast_oid_to_int64, f::CastOidToInt64, D::UInt32(42));
    bench_unary!(c, cast_oid_to_string, f::CastOidToString, D::UInt32(42));
    bench_unary!(c, cast_uuid_to_string, f::CastUuidToString, Uuid::nil());
    bench_unary!(c, cast_bytes_to_string, f::CastBytesToString, b"hello");
    bench_unary!(c, cast_jsonb_to_string, f::CastJsonbToString, "hello");
    bench_unary!(c, cast_jsonb_to_int16, f::CastJsonbToInt16, 42f64);
    bench_unary!(c, cast_jsonb_to_int32, f::CastJsonbToInt32, 42f64);
    bench_unary!(c, cast_jsonb_to_int64, f::CastJsonbToInt64, 42f64);
    bench_unary!(c, cast_jsonb_to_float32, f::CastJsonbToFloat32, 42f64);
    bench_unary!(c, cast_jsonb_to_float64, f::CastJsonbToFloat64, 42f64);
    bench_unary!(c, cast_jsonb_to_bool, f::CastJsonbToBool, true);

    // --- Timestamp/Date casts ---

    bench_unary!(
        c,
        cast_date_to_timestamp,
        f::CastDateToTimestamp(None),
        Date::from_pg_epoch(8_415).unwrap(),
    );
    bench_unary!(
        c,
        cast_date_to_timestamp_tz,
        f::CastDateToTimestampTz(None),
        Date::from_pg_epoch(8_415).unwrap(),
    );

    c.bench_function("cast_timestamp_to_date", |b| {
        let func = UnaryFunc::from(f::CastTimestampToDate);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_timestamp_to_time", |b| {
        let func = UnaryFunc::from(f::CastTimestampToTime);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_timestamp_to_string", |b| {
        let func = UnaryFunc::from(f::CastTimestampToString);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_timestamp_to_timestamp_tz", |b| {
        let func = UnaryFunc::from(f::CastTimestampToTimestampTz {
            from: None,
            to: None,
        });
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_timestamp_tz_to_date", |b| {
        let func = UnaryFunc::from(f::CastTimestampTzToDate);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_timestamp_tz_to_timestamp", |b| {
        let func = UnaryFunc::from(f::CastTimestampTzToTimestamp {
            from: None,
            to: None,
        });
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_timestamp_tz_to_string", |b| {
        let func = UnaryFunc::from(f::CastTimestampTzToString);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_timestamp_tz_to_time", |b| {
        let func = UnaryFunc::from(f::CastTimestampTzToTime);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    bench_unary!(c, to_timestamp, f::ToTimestamp, 1705312245.0f64);

    // --- MzTimestamp casts ---

    bench_unary!(
        c,
        cast_mz_timestamp_to_string,
        f::CastMzTimestampToString,
        D::MzTimestamp(Timestamp::from(1705312245000u64)),
    );
    bench_unary!(
        c,
        cast_mz_timestamp_to_timestamp,
        f::CastMzTimestampToTimestamp,
        D::MzTimestamp(Timestamp::from(1705312245000u64)),
    );
    bench_unary!(
        c,
        cast_mz_timestamp_to_timestamp_tz,
        f::CastMzTimestampToTimestampTz,
        D::MzTimestamp(Timestamp::from(1705312245000u64)),
    );
    bench_unary!(
        c,
        cast_string_to_mz_timestamp,
        f::CastStringToMzTimestamp,
        "1705312245000",
    );
    bench_unary!(
        c,
        cast_uint64_to_mz_timestamp,
        f::CastUint64ToMzTimestamp,
        D::UInt64(1705312245000),
    );
    bench_unary!(
        c,
        cast_uint32_to_mz_timestamp,
        f::CastUint32ToMzTimestamp,
        D::UInt32(1705312245),
    );
    bench_unary!(
        c,
        cast_int64_to_mz_timestamp,
        f::CastInt64ToMzTimestamp,
        D::Int64(1705312245000),
    );
    bench_unary!(
        c,
        cast_int32_to_mz_timestamp,
        f::CastInt32ToMzTimestamp,
        D::Int32(1705312245),
    );
    bench_unary!(
        c,
        cast_numeric_to_mz_timestamp,
        f::CastNumericToMzTimestamp,
        Numeric::from(1705312245000i64),
    );

    c.bench_function("cast_timestamp_to_mz_timestamp", |b| {
        let func = UnaryFunc::from(f::CastTimestampToMzTimestamp);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_timestamp_tz_to_mz_timestamp", |b| {
        let func = UnaryFunc::from(f::CastTimestampTzToMzTimestamp);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    bench_unary!(
        c,
        cast_date_to_mz_timestamp,
        f::CastDateToMzTimestamp,
        Date::from_pg_epoch(8_415).unwrap(),
    );

    bench_unary!(
        c,
        step_mz_timestamp,
        f::StepMzTimestamp,
        D::MzTimestamp(Timestamp::from(1705312245000u64)),
    );

    // --- PgLegacyChar / Char / VarChar casts ---

    bench_unary!(
        c,
        cast_int32_to_pg_legacy_char,
        f::CastInt32ToPgLegacyChar,
        D::Int32(65)
    );
    bench_unary!(
        c,
        cast_string_to_pg_legacy_char,
        f::CastStringToPgLegacyChar,
        "A"
    );
    bench_unary!(
        c,
        cast_string_to_pg_legacy_name,
        f::CastStringToPgLegacyName,
        "my_table"
    );
    bench_unary!(
        c,
        cast_pg_legacy_char_to_string,
        f::CastPgLegacyCharToString,
        D::UInt8(65)
    );
    bench_unary!(
        c,
        cast_pg_legacy_char_to_char,
        f::CastPgLegacyCharToChar,
        D::UInt8(65)
    );
    bench_unary!(
        c,
        cast_pg_legacy_char_to_var_char,
        f::CastPgLegacyCharToVarChar,
        D::UInt8(65)
    );
    bench_unary!(
        c,
        cast_pg_legacy_char_to_int32,
        f::CastPgLegacyCharToInt32,
        D::UInt8(65)
    );

    bench_unary!(
        c,
        cast_string_to_char,
        f::CastStringToChar {
            length: None,
            fail_on_len: false,
        },
        "hello",
    );
    bench_unary!(
        c,
        cast_string_to_var_char,
        f::CastStringToVarChar {
            length: None,
            fail_on_len: false,
        },
        "hello",
    );
    bench_unary!(c, cast_char_to_string, f::CastCharToString, "hello");
    bench_unary!(c, cast_var_char_to_string, f::CastVarCharToString, "hello");
    bench_unary!(c, pad_char, f::PadChar { length: None }, "hello");

    // --- OID / Reg* casts ---

    bench_unary!(
        c,
        cast_oid_to_reg_class,
        f::CastOidToRegClass,
        D::UInt32(42)
    );
    bench_unary!(
        c,
        cast_reg_class_to_oid,
        f::CastRegClassToOid,
        D::UInt32(42)
    );
    bench_unary!(c, cast_oid_to_reg_proc, f::CastOidToRegProc, D::UInt32(42));
    bench_unary!(c, cast_reg_proc_to_oid, f::CastRegProcToOid, D::UInt32(42));
    bench_unary!(c, cast_oid_to_reg_type, f::CastOidToRegType, D::UInt32(42));
    bench_unary!(c, cast_reg_type_to_oid, f::CastRegTypeToOid, D::UInt32(42));
    bench_unary!(c, cast_string_to_oid, f::CastStringToOid, "42");

    // --- Jsonb extras ---

    c.bench_function("jsonb_array_length", |b| {
        let func = UnaryFunc::from(f::JsonbArrayLength);
        let a = MirScalarExpr::column(0);
        let sa = RowArena::new();
        let arr = sa.make_datum(|packer| {
            packer.push_list(&[
                D::Float64(OrderedFloat(1.0)),
                D::Float64(OrderedFloat(2.0)),
                D::Float64(OrderedFloat(3.0)),
            ]);
        });
        let arena = RowArena::new();
        let datums: &[D] = &[arr];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("jsonb_strip_nulls", |b| {
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
    });

    bench_unary!(c, cast_jsonbable_to_jsonb, f::CastJsonbableToJsonb, 42f64);
    bench_unary!(c, cast_jsonb_to_numeric, f::CastJsonbToNumeric(None), 42f64,);

    // --- Numeric extras ---

    bench_unary!(
        c,
        adjust_numeric_scale,
        f::AdjustNumericScale(NumericMaxScale::try_from(2i64).unwrap()),
        Numeric::from(42),
    );
    bench_unary!(
        c,
        cast_string_to_numeric,
        f::CastStringToNumeric(None),
        "42.5",
    );

    // --- Int2Vector / String casts ---

    c.bench_function("cast_string_to_int2_vector", |b| {
        let func = UnaryFunc::from(f::CastStringToInt2Vector);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::String("1 2 3")];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_int2_vector_to_string", |b| {
        let func = UnaryFunc::from(f::CastInt2VectorToString);
        let a = MirScalarExpr::column(0);
        let sa = RowArena::new();
        let vec = sa.make_datum(|packer| {
            packer
                .try_push_array(
                    &[mz_repr::adt::array::ArrayDimension {
                        lower_bound: 1,
                        length: 3,
                    }],
                    &[D::Int16(1), D::Int16(2), D::Int16(3)],
                )
                .unwrap();
        });
        let arena = RowArena::new();
        let datums: &[D] = &[vec];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("cast_int2_vector_to_array", |b| {
        let func = UnaryFunc::from(f::CastInt2VectorToArray);
        let a = MirScalarExpr::column(0);
        let sa = RowArena::new();
        let vec = sa.make_datum(|packer| {
            packer
                .try_push_array(
                    &[mz_repr::adt::array::ArrayDimension {
                        lower_bound: 1,
                        length: 3,
                    }],
                    &[D::Int16(1), D::Int16(2), D::Int16(3)],
                )
                .unwrap();
        });
        let arena = RowArena::new();
        let datums: &[D] = &[vec];
        b.iter(|| func.eval(datums, &arena, &a));
    });
}

// Remaining unary functions

fn remaining_benches(c: &mut Criterion) {
    // String operations
    bench_unary!(c, upper, f::Upper, "hello");
    bench_unary!(c, lower, f::Lower, "HELLO");
    bench_unary!(c, initcap, f::Initcap, "hello world");
    bench_unary!(c, trim_whitespace, f::TrimWhitespace, "  hello  ");
    bench_unary!(
        c,
        trim_leading_whitespace,
        f::TrimLeadingWhitespace,
        "  hello  "
    );
    bench_unary!(
        c,
        trim_trailing_whitespace,
        f::TrimTrailingWhitespace,
        "  hello  "
    );
    bench_unary!(c, ascii, f::Ascii, "hello");
    bench_unary!(c, char_length, f::CharLength, "hello");
    bench_unary!(c, bit_length_bytes, f::BitLengthBytes, b"hello");
    bench_unary!(c, bit_length_string, f::BitLengthString, "hello");
    bench_unary!(c, byte_length_bytes, f::ByteLengthBytes, b"hello");
    bench_unary!(c, byte_length_string, f::ByteLengthString, "hello");
    bench_unary!(c, bit_count_bytes, f::BitCountBytes, b"\xff");
    bench_unary!(c, chr, f::Chr, D::Int32(65));
    bench_unary!(c, reverse, f::Reverse, "hello");
    bench_unary!(c, quote_ident, f::QuoteIdent, "my_table");
    bench_unary!(c, pg_size_pretty, f::PgSizePretty, Numeric::from(1048576));

    // Jsonb
    bench_unary!(
        c,
        jsonb_typeof,
        f::JsonbTypeof,
        D::Float64(OrderedFloat(42.0))
    );
    bench_unary!(c, jsonb_pretty, f::JsonbPretty, "hello");

    // Date/time
    bench_unary!(c, justify_days, f::JustifyDays, Interval::new(0, 35, 0));
    bench_unary!(
        c,
        justify_hours,
        f::JustifyHours,
        Interval::new(0, 0, 30 * 3_600_000_000)
    );
    bench_unary!(
        c,
        justify_interval,
        f::JustifyInterval,
        Interval::new(0, 35, 30 * 3_600_000_000)
    );
    bench_unary!(
        c,
        cast_date_to_string,
        f::CastDateToString,
        Date::from_pg_epoch(0).unwrap(),
    );
    bench_unary!(
        c,
        cast_time_to_string,
        f::CastTimeToString,
        NaiveTime::from_hms_opt(12, 30, 45).unwrap(),
    );
    bench_unary!(
        c,
        cast_interval_to_string,
        f::CastIntervalToString,
        Interval::new(1, 2, 3_000_000)
    );
    bench_unary!(
        c,
        cast_interval_to_time,
        f::CastIntervalToTime,
        Interval::new(0, 0, 45_000_000_000)
    );
    bench_unary!(
        c,
        cast_time_to_interval,
        f::CastTimeToInterval,
        NaiveTime::from_hms_opt(12, 30, 45).unwrap()
    );

    // Hash functions
    bench_unary!(c, crc32_bytes, f::Crc32Bytes, b"hello");
    bench_unary!(c, crc32_string, f::Crc32String, "hello");
    bench_unary!(c, kafka_murmur2_bytes, f::KafkaMurmur2Bytes, b"hello");
    bench_unary!(c, kafka_murmur2_string, f::KafkaMurmur2String, "hello");
    bench_unary!(c, seahash_bytes, f::SeahashBytes, b"hello");
    bench_unary!(c, seahash_string, f::SeahashString, "hello");

    // --- Extract / DatePart / DateTrunc ---

    c.bench_function("extract_interval", |b| {
        let func = UnaryFunc::from(f::ExtractInterval(DateTimeUnits::Hour));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Interval(Interval::new(1, 2, 3_600_000_000))];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("extract_time", |b| {
        let func = UnaryFunc::from(f::ExtractTime(DateTimeUnits::Hour));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Time(NaiveTime::from_hms_opt(12, 30, 45).unwrap())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("extract_timestamp", |b| {
        let func = UnaryFunc::from(f::ExtractTimestamp(DateTimeUnits::Year));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("extract_timestamp_tz", |b| {
        let func = UnaryFunc::from(f::ExtractTimestampTz(DateTimeUnits::Year));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("extract_date", |b| {
        let func = UnaryFunc::from(f::ExtractDate(DateTimeUnits::Year));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Date(Date::from_pg_epoch(8_415).unwrap())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("date_part_interval", |b| {
        let func = UnaryFunc::from(f::DatePartInterval(DateTimeUnits::Hour));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Interval(Interval::new(1, 2, 3_600_000_000))];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("date_part_time", |b| {
        let func = UnaryFunc::from(f::DatePartTime(DateTimeUnits::Hour));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Time(NaiveTime::from_hms_opt(12, 30, 45).unwrap())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("date_part_timestamp", |b| {
        let func = UnaryFunc::from(f::DatePartTimestamp(DateTimeUnits::Year));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("date_part_timestamp_tz", |b| {
        let func = UnaryFunc::from(f::DatePartTimestampTz(DateTimeUnits::Year));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("date_trunc_timestamp", |b| {
        let func = UnaryFunc::from(f::DateTruncTimestamp(DateTimeUnits::Hour));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("date_trunc_timestamp_tz", |b| {
        let func = UnaryFunc::from(f::DateTruncTimestampTz(DateTimeUnits::Hour));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("adjust_timestamp_precision", |b| {
        let func = UnaryFunc::from(f::AdjustTimestampPrecision {
            from: None,
            to: None,
        });
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("adjust_timestamp_tz_precision", |b| {
        let func = UnaryFunc::from(f::AdjustTimestampTzPrecision {
            from: None,
            to: None,
        });
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    // --- Timezone ---

    c.bench_function("timezone_timestamp", |b| {
        use mz_pgtz::timezone::Timezone;
        let func = UnaryFunc::from(f::TimezoneTimestamp(Timezone::Tz(chrono_tz::UTC)));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Timestamp(make_ts())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("timezone_timestamp_tz", |b| {
        use mz_pgtz::timezone::Timezone;
        let func = UnaryFunc::from(f::TimezoneTimestampTz(Timezone::Tz(chrono_tz::UTC)));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::TimestampTz(make_tstz())];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("timezone_time", |b| {
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
    });

    // --- Pattern matching ---

    c.bench_function("is_like_match", |b| {
        let matcher = like_pattern::compile("%world%", false).unwrap();
        let func = UnaryFunc::from(f::IsLikeMatch(matcher));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::String("hello world")];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("is_regexp_match", |b| {
        let regex = Regex::new("\\d+", false).unwrap();
        let func = UnaryFunc::from(f::IsRegexpMatch(regex));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::String("hello 42 world")];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("regexp_match_unary", |b| {
        let regex = Regex::new("(\\d+)", false).unwrap();
        let func = UnaryFunc::from(f::RegexpMatch(regex));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::String("hello 42 world")];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("regexp_split_to_array_unary", |b| {
        let regex = Regex::new("\\d", false).unwrap();
        let func = UnaryFunc::from(f::RegexpSplitToArray(regex));
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::String("one1two2three")];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    // --- Range functions ---

    c.bench_function("bench_range_empty", |b| {
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
    });

    c.bench_function("bench_range_lower_inc", |b| {
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
    });

    c.bench_function("bench_range_upper_inc", |b| {
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
    });

    c.bench_function("bench_range_lower_inf", |b| {
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
    });

    c.bench_function("bench_range_upper_inf", |b| {
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
    });

    c.bench_function("bench_range_lower", |b| {
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
    });

    c.bench_function("bench_range_upper", |b| {
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
    });

    // --- Collection ops ---

    c.bench_function("list_length", |b| {
        let func = UnaryFunc::from(f::ListLength);
        let a = MirScalarExpr::column(0);
        let sa = RowArena::new();
        let list =
            sa.make_datum(|packer| packer.push_list(&[D::Int32(1), D::Int32(2), D::Int32(3)]));
        let arena = RowArena::new();
        let datums: &[D] = &[list];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("map_length", |b| {
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
    });

    // --- ACL / privileges ---

    c.bench_function("mz_acl_item_grantor", |b| {
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
    });

    c.bench_function("mz_acl_item_grantee", |b| {
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
    });

    c.bench_function("mz_acl_item_privileges", |b| {
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
    });

    bench_unary!(c, mz_format_privileges, f::MzFormatPrivileges, "r");
    bench_unary!(c, mz_validate_privileges, f::MzValidatePrivileges, "r");
    bench_unary!(
        c,
        mz_validate_role_privilege,
        f::MzValidateRolePrivilege,
        "USAGE"
    );

    c.bench_function("acl_item_grantor", |b| {
        let func = UnaryFunc::from(f::AclItemGrantor);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let item = AclItem::empty(Oid(1), Oid(2));
        let datums: &[D] = &[D::AclItem(item)];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("acl_item_grantee", |b| {
        let func = UnaryFunc::from(f::AclItemGrantee);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let item = AclItem::empty(Oid(1), Oid(2));
        let datums: &[D] = &[D::AclItem(item)];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("acl_item_privileges", |b| {
        let func = UnaryFunc::from(f::AclItemPrivileges);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let item = AclItem::empty(Oid(1), Oid(2));
        let datums: &[D] = &[D::AclItem(item)];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    // --- Misc ---

    bench_unary!(c, mz_type_name, f::MzTypeName, D::UInt32(25)); // text oid
    bench_unary!(
        c,
        try_parse_monotonic_iso8601_timestamp,
        f::TryParseMonotonicIso8601Timestamp,
        "2024-01-15T12:30:45Z",
    );

    c.bench_function("pg_column_size", |b| {
        let func = UnaryFunc::from(f::PgColumnSize);
        let a = MirScalarExpr::column(0);
        let arena = RowArena::new();
        let datums: &[D] = &[D::Int32(42)];
        b.iter(|| func.eval(datums, &arena, &a));
    });

    c.bench_function("mz_row_size", |b| {
        let func = UnaryFunc::from(f::MzRowSize);
        let a = MirScalarExpr::column(0);
        let sa = RowArena::new();
        let list =
            sa.make_datum(|packer| packer.push_list(&[D::Int32(1), D::Int32(2), D::Int32(3)]));
        let arena = RowArena::new();
        let datums: &[D] = &[list];
        b.iter(|| func.eval(datums, &arena, &a));
    });
}

// Benchmark registration

criterion_group!(benches, arithmetic_benches, cast_benches, remaining_benches);
criterion_main!(benches);
