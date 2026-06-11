// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `Interval` arithmetic. Intervals are built from user input and
//! combined with checked arithmetic that must never panic (overflow returns
//! `None`/`Err`, not a crash) and must respect basic algebra.
//!
//! Oracles (beyond no-panic on every op):
//!  * add is commutative;
//!  * add is associative (`(a+b)+c == a+(b+c)`) whenever both groupings succeed;
//!  * additive inverse: `a + (-a)` is the zero interval (when `checked_neg` and
//!    the add both succeed);
//!  * the sketchy fractional-carry `as`-cast path in `checked_mul`/`checked_op`
//!    is cross-checked against an exact integer reference: for a small whole
//!    factor `k`, `checked_mul(s, k)` must equal `k` repeated `checked_add`s,
//!    and `checked_mul(s, 1.0) == s`. Because that path routes the i64 `micros`
//!    through `f64`, the identity only holds while the fields and their products
//!    stay inside `f64`'s exact-integer range (2^53); the cross-checked operand
//!    `s` is masked into a small range so the comparison is sound (an unbounded
//!    operand legitimately loses precision and is NOT a bug);
//!  * `justify_days` / `justify_hours` / `justify_interval` preserve the
//!    interval's total microseconds (they only redistribute fields using the
//!    fixed 30-day-month / 24-hour-day ratios that `as_microseconds` also uses).

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_repr::adt::interval::Interval;

/// Negate every field with checked arithmetic, mirroring `Interval`'s
/// `CheckedNeg` impl without pulling in the `num-traits` trait. Returns `None`
/// if any field is `MIN` (its negation overflows).
fn checked_neg(a: &Interval) -> Option<Interval> {
    Some(Interval::new(
        a.months.checked_neg()?,
        a.days.checked_neg()?,
        a.micros.checked_neg()?,
    ))
}

/// Add `a` to itself `k` times via `checked_add`, the exact integer reference
/// for `checked_mul(a, k as f64)`. Returns `None` on any intermediate overflow.
fn repeated_add(a: &Interval, k: u32) -> Option<Interval> {
    let mut acc = Interval::new(0, 0, 0);
    for _ in 0..k {
        acc = acc.checked_add(a)?;
    }
    Some(acc)
}

/// Bounds chosen so that, for the small factor `k <= 16`, every field AND its
/// product `field * k` stay (a) exactly representable in `f64` (< 2^53) and
/// (b) inside the destination integer's range, so neither `checked_mul`'s
/// `as`-cast path nor `repeated_add`'s integer path overflows. That makes the
/// two computations a clean, exact reference for each other.
const MONTHS_DAYS_BOUND: i32 = 1 << 26; // *16 = 2^30 < i32::MAX, f64-exact
const MICROS_BOUND: i64 = 1 << 49; // *16 = 2^53, f64-exact, < i64::MAX

fn check(a: Interval, b: Interval, c: Interval, s: Interval, factor: f64, small_k: u8) {
    // --- Addition is commutative; overflow is symmetric. ------------------
    assert_eq!(
        a.checked_add(&b),
        b.checked_add(&a),
        "interval checked_add is not commutative"
    );

    // --- Addition is associative when both groupings succeed. -------------
    let left = a.checked_add(&b).and_then(|ab| ab.checked_add(&c));
    let right = b.checked_add(&c).and_then(|bc| a.checked_add(&bc));
    // Only compare when neither side overflowed; if one overflows the other may
    // legitimately not (intermediate sums differ in magnitude).
    if let (Some(left), Some(right)) = (left, right) {
        assert_eq!(left, right, "interval checked_add is not associative");
    }

    // --- Additive inverse: a + (-a) == 0. ---------------------------------
    if let Some(neg_a) = checked_neg(&a) {
        if let Some(sum) = a.checked_add(&neg_a) {
            assert_eq!(
                sum,
                Interval::new(0, 0, 0),
                "a + (-a) must be the zero interval"
            );
        }
    }

    // --- Multiply through the `as`-cast path, cross-checked exactly. ------
    // `s` is masked so every field and its product with `k` fits exactly in
    // `f64`; otherwise the i64->f64->i64 round-trip in `checked_op` is lossy and
    // the identities below would not hold (that loss is expected, not a bug).
    let s = Interval::new(
        s.months % MONTHS_DAYS_BOUND,
        s.days % MONTHS_DAYS_BOUND,
        s.micros % MICROS_BOUND,
    );

    // Multiply-by-one is the identity on the bounded operand.
    if let Some(prod) = s.checked_mul(1.0) {
        assert_eq!(prod, s, "checked_mul(s, 1.0) must equal s");
    }

    // checked_mul(s, k) == k repeated checked_adds, for small whole k. `k` is
    // small so repeated_add stays cheap and the products stay f64-exact.
    let k = u32::from(small_k % 17); // 0..=16
    let by_mul = s.checked_mul(f64::from(k));
    let by_add = repeated_add(&s, k);
    // Both must agree on success/overflow and, on success, on the value.
    assert_eq!(
        by_mul, by_add,
        "checked_mul(s, {k}) disagrees with {k} repeated checked_adds"
    );

    // --- justify_* preserve total microseconds. ---------------------------
    let total = a.as_microseconds();
    if let Ok(j) = a.justify_days() {
        assert_eq!(
            j.as_microseconds(),
            total,
            "justify_days changed total microseconds"
        );
    }
    if let Ok(j) = a.justify_hours() {
        assert_eq!(
            j.as_microseconds(),
            total,
            "justify_hours changed total microseconds"
        );
    }
    if let Ok(j) = a.justify_interval() {
        assert_eq!(
            j.as_microseconds(),
            total,
            "justify_interval changed total microseconds"
        );
    }

    // --- Remaining ops just must not panic on any input, incl. NaN/inf. ---
    let _ = a.checked_mul(factor);
    let _ = a.checked_div(factor);
    let _ = b.checked_mul(factor);
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let a = Interval::new(u.arbitrary()?, u.arbitrary()?, u.arbitrary()?);
    let b = Interval::new(u.arbitrary()?, u.arbitrary()?, u.arbitrary()?);
    let c = Interval::new(u.arbitrary()?, u.arbitrary()?, u.arbitrary()?);
    let s = Interval::new(u.arbitrary()?, u.arbitrary()?, u.arbitrary()?);
    let factor: f64 = u.arbitrary()?;
    let small_k: u8 = u.arbitrary()?;
    check(a, b, c, s, factor, small_k);
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
