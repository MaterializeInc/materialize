// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests for the accumulable reduce's accumulators: the fixed-point conversion
//! float sums accumulate through, and the packed [`Accums`] encoding, which is
//! held to the same results as a `Vec<Accum>` under the same operations.

use columnation::ColumnStack;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use super::*;

/// The saturating conversion that `float_to_fixed_point` replaces. Used to
/// assert that the new wrapping conversion agrees on the in-range values
/// where the old conversion was already correct.
#[allow(clippy::as_conversions)]
fn saturating_convert(n: f64) -> i128 {
    (n * FLOAT_SCALE) as i128
}

#[mz_ore::test]
fn float_to_fixed_point_matches_saturating_in_range() {
    // For values whose scaled magnitude comfortably fits in an `i128`, the
    // wrapping conversion must produce exactly the same result the previous
    // saturating cast did.
    let cases = [
        0.0,
        -0.0,
        1.0,
        -1.0,
        0.1,
        -0.1,
        0.5,
        -0.5,
        3.25,
        -3.25,
        123456.789,
        -123456.789,
        1e10,
        -1e10,
        1e20,
        -1e20,
        5e30, // large, but scaled magnitude still fits comfortably in i128
        -5e30,
    ];
    for n in cases {
        assert_eq!(
            float_to_fixed_point(n),
            saturating_convert(n),
            "mismatch for n = {n}"
        );
    }
}

#[mz_ore::test]
fn float_to_fixed_point_truncates_toward_zero() {
    // 1.75 * 2^24 = 29360128, exactly representable.
    assert_eq!(float_to_fixed_point(1.75), 29_360_128);
    assert_eq!(float_to_fixed_point(-1.75), -29_360_128);

    // Fractional results truncate toward zero, matching the previous cast.
    let frac = 0.123_456_7_f64;
    assert_eq!(float_to_fixed_point(frac), saturating_convert(frac));
    assert_eq!(float_to_fixed_point(-frac), saturating_convert(-frac));
    assert_eq!(float_to_fixed_point(-frac), -float_to_fixed_point(frac));
}

#[mz_ore::test]
fn float_to_fixed_point_subnormals_round_to_zero() {
    assert_eq!(float_to_fixed_point(0.0), 0);
    assert_eq!(float_to_fixed_point(-0.0), 0);
    assert_eq!(float_to_fixed_point(f64::MIN_POSITIVE / 2.0), 0);
    assert_eq!(float_to_fixed_point(5e-324), 0); // smallest subnormal
}

#[mz_ore::test]
fn float_to_fixed_point_cancels_large_finite_values() {
    // Regression test for database-issues#11265: large finite values that
    // individually overflow the fixed-point domain must still sum to the
    // correct result when their mathematical sum is representable. The
    // previous saturating conversion produced `i128::MAX + i128::MIN == -1`.
    for &n in &[1.1e31_f64, 1e32, 5e33, 1e284] {
        assert_eq!(
            float_to_fixed_point(n).wrapping_add(float_to_fixed_point(-n)),
            0,
            "n = {n} did not cancel with -n"
        );
    }
}

#[mz_ore::test]
fn float_to_fixed_point_sum_via_accumulator() {
    // Exercise the full accumulate-then-finalize path for the reported case.
    let func = AggregateFunc::SumFloat64;
    let mut acc = accumulable_zero(&func);
    acc.plus_equals(&datum_to_accumulator(&func, Datum::from(1.1e31_f64)));
    acc.plus_equals(&datum_to_accumulator(&func, Datum::from(-1.1e31_f64)));
    let datum = finalize_accum(&func, &acc, Diff::from(2_i64));
    assert_eq!(datum, Datum::from(0.0_f64));
}

fn diff(v: i64) -> Diff {
    Diff::from(v)
}

fn count(v: i128) -> AccumCount {
    AccumCount::from(v)
}

fn numeric_agg(s: &str) -> OrderedDecimal<NumericAgg> {
    OrderedDecimal(numeric::cx_agg().parse(s).unwrap())
}

fn numeric_datum(s: &str) -> Datum<'static> {
    Datum::from(numeric::cx_datum().parse(s).unwrap())
}

fn aggr(func: AggregateFunc) -> LirAggregateExpr {
    LirAggregateExpr {
        func,
        expr: LirScalarExpr::column(0),
        distinct: false,
    }
}

/// Every aggregate function `reduction_type` routes to the accumulable
/// reduce, which is exactly the set `Accums` has to encode.
fn accumulable_funcs() -> Vec<AggregateFunc> {
    let funcs = vec![
        AggregateFunc::SumInt16,
        AggregateFunc::SumInt32,
        AggregateFunc::SumInt64,
        AggregateFunc::SumUInt16,
        AggregateFunc::SumUInt32,
        AggregateFunc::SumUInt64,
        AggregateFunc::SumFloat32,
        AggregateFunc::SumFloat64,
        AggregateFunc::SumNumeric,
        AggregateFunc::Count,
        AggregateFunc::Any,
        AggregateFunc::All,
        AggregateFunc::Dummy,
    ];
    for func in &funcs {
        assert_eq!(
            reduction_type(func),
            ReductionType::Accumulable,
            "func={func:?}"
        );
    }
    funcs
}

/// A random input datum for `func`, drawn from the domain
/// `datum_to_accumulator` accepts for it, including `Null` and the special
/// float and numeric values.
///
/// Magnitudes stay small so that no sequence of `plus_equals` and
/// `multiply` can overflow an accumulator. Overflow is not a difference
/// between the two representations (both defer to [`Accum`]), but
/// `Accum::Numeric` panics on it, which would just make the test flaky.
fn random_datum(func: &AggregateFunc, rng: &mut SmallRng) -> Datum<'static> {
    if *func == AggregateFunc::Dummy {
        return Datum::Dummy;
    }
    if rng.random_bool(0.15) {
        return Datum::Null;
    }
    match func {
        AggregateFunc::SumInt16 => Datum::Int16(rng.random_range(-1000..=1000)),
        AggregateFunc::SumInt32 => Datum::Int32(rng.random_range(-1000..=1000)),
        AggregateFunc::SumInt64 => Datum::Int64(rng.random_range(-1000..=1000)),
        AggregateFunc::SumUInt16 => Datum::UInt16(rng.random_range(0..=1000)),
        AggregateFunc::SumUInt32 => Datum::UInt32(rng.random_range(0..=1000)),
        AggregateFunc::SumUInt64 => Datum::UInt64(rng.random_range(0..=1000)),
        AggregateFunc::SumFloat32 => Datum::from(match rng.random_range(0..6) {
            0 => f32::NAN,
            1 => f32::INFINITY,
            2 => f32::NEG_INFINITY,
            _ => rng.random_range(-1000.0..1000.0_f32),
        }),
        AggregateFunc::SumFloat64 => Datum::from(match rng.random_range(0..6) {
            0 => f64::NAN,
            1 => f64::INFINITY,
            2 => f64::NEG_INFINITY,
            _ => rng.random_range(-1000.0..1000.0_f64),
        }),
        AggregateFunc::SumNumeric => match rng.random_range(0..6) {
            0 => numeric_datum("nan"),
            1 => numeric_datum("inf"),
            2 => numeric_datum("-inf"),
            3 => numeric_datum("0.00000001234"),
            _ => Datum::from(Numeric::from(rng.random_range(-1000..=1000))),
        },
        AggregateFunc::Count => Datum::Int32(rng.random_range(-1000..=1000)),
        AggregateFunc::Any | AggregateFunc::All => {
            if rng.random_bool(0.5) {
                Datum::True
            } else {
                Datum::False
            }
        }
        func => panic!("not an accumulable aggregate: {func:?}"),
    }
}

#[mz_ore::test]
fn accums_slot_sizes() {
    // The slot sizes are the point of the packed layout, so pin them: a
    // change here is a change to the arrangement's memory footprint.
    let cases = [
        (
            Accum::Bool {
                trues: diff(1),
                falses: diff(1),
            },
            1 + 16,
        ),
        (
            Accum::SimpleNumber {
                accum: count(1),
                non_nulls: diff(1),
            },
            1 + 24,
        ),
        (
            Accum::Float {
                accum: count(1),
                pos_infs: diff(1),
                neg_infs: diff(1),
                nans: diff(1),
                non_nulls: diff(1),
            },
            1 + 48,
        ),
        (
            Accum::Numeric {
                accum: numeric_agg("1.5"),
                pos_infs: diff(1),
                neg_infs: diff(1),
                nans: diff(1),
                non_nulls: diff(1),
            },
            1 + 95,
        ),
    ];

    for (accum, len) in cases {
        assert_eq!(Accums::pack([accum]).0.len(), len, "accum={accum:?}");
    }

    let total: usize = cases.iter().map(|(_, len)| len).sum();
    let packed = Accums::pack(cases.map(|(accum, _)| accum));
    assert_eq!(packed.0.len(), total);
    assert_eq!(packed.decode().count(), cases.len());
}

#[mz_ore::test]
fn accums_roundtrip() {
    let cases = [
        Accum::Bool {
            trues: diff(0),
            falses: diff(0),
        },
        Accum::Bool {
            trues: diff(i64::MAX),
            falses: diff(i64::MIN),
        },
        Accum::Bool {
            trues: diff(-7),
            falses: diff(9),
        },
        Accum::SimpleNumber {
            accum: count(0),
            non_nulls: diff(0),
        },
        Accum::SimpleNumber {
            accum: count(i128::MAX),
            non_nulls: diff(i64::MAX),
        },
        Accum::SimpleNumber {
            accum: count(i128::MIN),
            non_nulls: diff(i64::MIN),
        },
        Accum::SimpleNumber {
            accum: count(-1),
            non_nulls: diff(-3),
        },
        Accum::Float {
            accum: count(0),
            pos_infs: diff(0),
            neg_infs: diff(0),
            nans: diff(0),
            non_nulls: diff(0),
        },
        Accum::Float {
            accum: count(i128::MAX),
            pos_infs: diff(i64::MAX),
            neg_infs: diff(i64::MIN),
            nans: diff(-5),
            non_nulls: diff(11),
        },
        Accum::Float {
            accum: count(i128::MIN),
            pos_infs: diff(-1),
            neg_infs: diff(1),
            nans: diff(0),
            non_nulls: diff(-1),
        },
        Accum::Numeric {
            accum: OrderedDecimal(NumericAgg::zero()),
            pos_infs: diff(0),
            neg_infs: diff(0),
            nans: diff(0),
            non_nulls: diff(0),
        },
        Accum::Numeric {
            accum: numeric_agg("123456789012345678901234567890123456789012345678901234567890"),
            pos_infs: diff(3),
            neg_infs: diff(-4),
            nans: diff(7),
            non_nulls: diff(-9),
        },
        Accum::Numeric {
            accum: numeric_agg("-9.999999999999999e-100"),
            pos_infs: diff(i64::MIN),
            neg_infs: diff(i64::MAX),
            nans: diff(1),
            non_nulls: diff(1),
        },
        Accum::Numeric {
            accum: numeric_agg("1e100"),
            pos_infs: diff(0),
            neg_infs: diff(0),
            nans: diff(0),
            non_nulls: diff(1),
        },
    ];

    for accum in cases {
        let decoded: Vec<_> = Accums::pack([accum]).decode().collect();
        assert_eq!(decoded, vec![accum], "accum={accum:?}");
    }

    // The same values in one buffer, so slot walking has to find each
    // boundary rather than reading a single slot.
    let decoded: Vec<_> = Accums::pack(cases).decode().collect();
    assert_eq!(decoded, cases.to_vec());
}

#[mz_ore::test]
fn accums_region_roundtrip() {
    // `AccumsRegion` is the only unsafe code in the encoding and every batch
    // passes through it, so pin the copy, read-back, clear cycle.
    let aggrs = [
        aggr(AggregateFunc::SumNumeric),
        aggr(AggregateFunc::Count),
        aggr(AggregateFunc::SumFloat64),
        aggr(AggregateFunc::Any),
    ];
    let zero = <Accums as AccumulableDiff>::zero(&aggrs);
    let layout = zero.layout();
    let mut filled = zero.clone();
    let datums = [
        numeric_datum("1.5"),
        Datum::Int32(1),
        Datum::from(2.5_f64),
        Datum::True,
    ];
    for (idx, (aggr, datum)) in aggrs.iter().zip(datums).enumerate() {
        filled.set(&layout, idx, datum_to_accumulator(&aggr.func, datum));
    }
    assert_ne!(filled, zero);

    let mut stack = ColumnStack::<Accums>::default();
    stack.copy(&zero);
    stack.copy(&filled);
    assert_eq!(stack.len(), 2);
    assert_eq!(stack[0], zero);
    assert_eq!(stack[1], filled);
    assert_eq!(
        stack[1].decode().collect::<Vec<_>>(),
        filled.decode().collect::<Vec<_>>()
    );

    let mut heap_bytes = 0;
    stack.heap_size(|size, _capacity| heap_bytes += size);
    assert!(
        heap_bytes >= zero.0.len() + filled.0.len(),
        "heap_size must account for the packed bytes, got {heap_bytes}"
    );

    stack.clear();
    assert_eq!(stack.len(), 0);
    stack.copy(&filled);
    assert_eq!(stack[0], filled);
}

#[mz_ore::test]
fn accums_empty_is_identity() {
    let aggrs = [aggr(AggregateFunc::SumInt64), aggr(AggregateFunc::Any)];
    let mut filled = <Accums as AccumulableDiff>::zero(&aggrs);
    let layout = filled.layout();
    filled.set(
        &layout,
        0,
        datum_to_accumulator(&AggregateFunc::SumInt64, Datum::Int64(7)),
    );
    filled.set(
        &layout,
        1,
        datum_to_accumulator(&AggregateFunc::Any, Datum::True),
    );

    let mut lhs = Accums::default();
    assert!(lhs.is_zero());
    lhs.plus_equals(&filled);
    assert_eq!(lhs, filled);

    let mut lhs = filled.clone();
    lhs.plus_equals(&Accums::default());
    assert_eq!(lhs, filled);

    assert_eq!(
        Accums::default().multiply(&diff(3)),
        Accums::default(),
        "multiplying the identity yields the identity"
    );
}

#[mz_ore::test]
fn accums_agree_with_vec_accum() {
    let mut rng = SmallRng::seed_from_u64(0);
    let funcs = accumulable_funcs();

    // One layout per function isolates each variant; the wider layouts make
    // slot walking cross variant boundaries.
    let mut layouts: Vec<Vec<AggregateFunc>> =
        funcs.iter().cloned().map(|func| vec![func]).collect();
    layouts.push(funcs.clone());
    layouts.push(vec![
        AggregateFunc::SumNumeric,
        AggregateFunc::Count,
        AggregateFunc::SumFloat64,
        AggregateFunc::Any,
        AggregateFunc::SumInt32,
    ]);

    // Reused across layouts and steps without clearing, so `as_accums` has to
    // discard whatever it finds there.
    let mut scratch = vec![Accum::Bool {
        trues: diff(99),
        falses: diff(99),
    }];

    for layout in layouts {
        let aggrs: Vec<_> = layout.iter().cloned().map(aggr).collect();

        let mut vec_state = <Vec<Accum> as AccumulableDiff>::zero(&aggrs);
        let mut packed_state = <Accums as AccumulableDiff>::zero(&aggrs);
        let vec_layout = vec_state.layout();
        let packed_layout = packed_state.layout();
        assert_eq!(packed_state.as_accums(&mut scratch), &vec_state[..]);

        // The `explode_one` path: overwrite each slot in place.
        for (idx, func) in layout.iter().enumerate() {
            let accum = datum_to_accumulator(func, random_datum(func, &mut rng));
            vec_state.set(&vec_layout, idx, accum);
            packed_state.set(&packed_layout, idx, accum);
        }

        for step in 0..64 {
            if rng.random_bool(0.6) {
                let rhs: Vec<Accum> = layout
                    .iter()
                    .map(|func| datum_to_accumulator(func, random_datum(func, &mut rng)))
                    .collect();
                vec_state.plus_equals(&rhs);
                packed_state.plus_equals(&Accums::pack(rhs));
            } else {
                let factor = diff(rng.random_range(-3..=3));
                vec_state = vec_state.multiply(&factor);
                packed_state = packed_state.multiply(&factor);
            }

            assert_eq!(
                packed_state.as_accums(&mut scratch),
                &vec_state[..],
                "layout={layout:?}, step={step}"
            );
            assert_eq!(
                packed_state.is_zero(),
                vec_state.is_zero(),
                "layout={layout:?}, step={step}"
            );
        }
    }
}
