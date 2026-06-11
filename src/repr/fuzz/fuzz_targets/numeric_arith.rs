// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `Numeric` (128-bit decimal) arithmetic through the standard
//! datum context.
//!
//! Operands are generated two ways so the fuzzer reaches values that decimal
//! text rarely produces:
//!  * parsed untrusted decimal text (`parse_numeric`), and
//!  * raw coefficient + exponent pairs (`from_i128` then `scaleb`), which lets
//!    the fuzzer pin a full 39-digit coefficient at an arbitrary scale, hitting
//!    the precision/exponent extremes of `cx_datum` that random text misses.
//!
//! Oracles (beyond no-panic on every op, including `rem`/`div` by zero and
//! `pow`/`ln`/`log10`/`sqrt`/`round`/`rescale` on every value):
//!  * add/mul are commutative;
//!  * add/mul are associative *when the involved ops were exact* (decimal
//!    rounding legitimately breaks associativity, so an `inexact` flag guards
//!    the assertion);
//!  * additive inverse: `a + (-a) == 0`;
//!  * cancellation: `(a + b) - b == a` when exact;
//!  * identities `a + 0 == a`, `a * 1 == a`;
//!  * `sqrt` of a non-negative value is non-negative, and `abs` is
//!    non-negative.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_repr::adt::numeric::{cx_datum, Numeric};
use mz_repr::strconv::parse_numeric;

/// Generate one operand from the fuzzer's byte stream.
///
/// Half the time we parse decimal text (reaching NaN/Inf/scientific notation),
/// the other half we build `coeff * 10^exponent` directly so a wide coefficient
/// can be placed at any scale.
fn gen_operand(u: &mut Unstructured) -> arbitrary::Result<Option<Numeric>> {
    if u.int_in_range(0u8..=1)? == 0 {
        // Parsed text. Borrow the rest of the buffer as UTF-8-ish text. On
        // failure fall back to a short arbitrary string.
        let s: &str = u.arbitrary()?;
        Ok(parse_numeric(s).ok().map(|d| d.0))
    } else {
        let coeff: i128 = u.arbitrary()?;
        // Bound the shift to bracket the datum context's [-39, 38] exponent
        // window without forcing every value outside it.
        let exponent: i16 = u.int_in_range(-60i16..=60)?;
        let mut cx = cx_datum();
        let mut n = cx.from_i128(coeff);
        let shift = Numeric::from(i32::from(exponent));
        // `scaleb` respects the context (clamps/flags rather than panicking).
        cx.scaleb(&mut n, &shift);
        // A finite value that overflowed to infinity isn't an interesting "raw"
        // operand. Drop it so special-value handling has a clear contract.
        if n.is_infinite() {
            Ok(None)
        } else {
            Ok(Some(n))
        }
    }
}

/// Was the operation that just ran in `cx` exact (no rounding, overflow, or
/// invalid operation)? Algebraic identities only hold when exact.
fn op_exact(cx: &dec::Context<Numeric>) -> bool {
    let s = cx.status();
    !s.inexact() && !s.invalid_operation() && !s.overflow()
}

fn check(a: Numeric, b: Numeric) {
    // NaN never equals itself, so equality-based oracles can't be asserted.
    if a.is_nan() || b.is_nan() {
        // Still exercise every operation for panics.
        let mut cx = cx_datum();
        let mut t = a;
        cx.add(&mut t, &b);
        cx.mul(&mut t, &b);
        cx.sub(&mut t, &b);
        cx.div(&mut t, &b);
        cx.rem(&mut t, &b);
        cx.pow(&mut t, &b);
        cx.ln(&mut t);
        cx.log10(&mut t);
        let mut s = a;
        cx.sqrt(&mut s);
        cx.round(&mut t);
        return;
    }

    let mut cx = cx_datum();

    // --- Commutativity -----------------------------------------------------
    let (mut ab, mut ba) = (a, b);
    cx.add(&mut ab, &b);
    cx.add(&mut ba, &a);
    assert_eq!(ab, ba, "numeric add is not commutative");

    let (mut am, mut bm) = (a, b);
    cx.mul(&mut am, &b);
    cx.mul(&mut bm, &a);
    assert_eq!(am, bm, "numeric mul is not commutative");

    // --- Additive inverse: a + (-a) == 0 (exact for all finite a) ----------
    {
        let mut neg_a = a;
        cx.neg(&mut neg_a);
        let mut sum = a;
        cx.clear_status();
        cx.add(&mut sum, &neg_a);
        if op_exact(&cx) {
            assert!(sum.is_zero(), "a + (-a) must be zero, got {}", sum);
        }
    }

    // --- Identities a + 0 == a, a * 1 == a ---------------------------------
    {
        let zero = Numeric::from(0);
        let mut t = a;
        cx.clear_status();
        cx.add(&mut t, &zero);
        if op_exact(&cx) {
            assert_eq!(t, a, "a + 0 must equal a");
        }

        let one = Numeric::from(1);
        let mut t = a;
        cx.clear_status();
        cx.mul(&mut t, &one);
        if op_exact(&cx) {
            assert_eq!(t, a, "a * 1 must equal a");
        }
    }

    // --- Cancellation: (a + b) - b == a, only when both ops were exact -----
    {
        cx.clear_status();
        let mut s = a;
        cx.add(&mut s, &b);
        let sum_exact = op_exact(&cx);
        cx.clear_status();
        cx.sub(&mut s, &b);
        let sub_exact = op_exact(&cx);
        if sum_exact && sub_exact {
            assert_eq!(s, a, "(a + b) - b must equal a when exact");
        }
    }

    // --- Associativity of add: (a+b)+a vs a+(b+a) when exact ---------------
    // Re-using `a` as the third operand keeps the target's input binary while
    // still re-associating across distinct magnitudes.
    {
        cx.clear_status();
        let mut left = a;
        cx.add(&mut left, &b);
        cx.add(&mut left, &a);
        let left_exact = op_exact(&cx);

        cx.clear_status();
        let mut bc = b;
        cx.add(&mut bc, &a);
        let mut right = a;
        cx.add(&mut right, &bc);
        let right_exact = op_exact(&cx);

        if left_exact && right_exact {
            assert_eq!(left, right, "numeric add is not associative (exact)");
        }
    }

    // --- Associativity of mul: (a*b)*a vs a*(b*a) when exact ---------------
    {
        cx.clear_status();
        let mut left = a;
        cx.mul(&mut left, &b);
        cx.mul(&mut left, &a);
        let left_exact = op_exact(&cx);

        cx.clear_status();
        let mut bc = b;
        cx.mul(&mut bc, &a);
        let mut right = a;
        cx.mul(&mut right, &bc);
        let right_exact = op_exact(&cx);

        if left_exact && right_exact {
            assert_eq!(left, right, "numeric mul is not associative (exact)");
        }
    }

    // --- No-panic exercise of the remaining ops, plus sign properties ------
    let mut s = a;
    cx.sub(&mut s, &b);
    let mut q = a;
    cx.div(&mut q, &b); // includes divide-by-zero
    let mut r = a;
    cx.rem(&mut r, &b); // includes rem-by-zero
    let mut p = a;
    cx.pow(&mut p, &b);

    // sqrt of a non-negative finite value is non-negative.
    {
        let mut x = a;
        cx.clear_status();
        cx.sqrt(&mut x);
        if !a.is_negative() && !x.is_nan() && !x.is_infinite() {
            assert!(
                !x.is_negative(),
                "sqrt of non-negative must be non-negative"
            );
        }
    }

    // abs is never negative (modulo NaN, already excluded).
    {
        let mut x = a;
        cx.abs(&mut x);
        if !x.is_nan() {
            assert!(!x.is_negative(), "abs must be non-negative");
        }
    }

    // ln / log10 / round / rescale just must not panic.
    let mut l = a;
    cx.ln(&mut l);
    let mut l = a;
    cx.log10(&mut l);
    let mut rd = a;
    cx.round(&mut rd);
    let mut rsc = a;
    let _ = mz_repr::adt::numeric::rescale(&mut rsc, (b.exponent().unsigned_abs() % 40) as u8);
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let a = gen_operand(&mut u)?;
    let b = gen_operand(&mut u)?;
    if let (Some(a), Some(b)) = (a, b) {
        check(a, b);
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
