// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: `Range` set operations over `int4` bounds. Ranges are built
//! from user input and canonicalized, then unioned/intersected/differenced.
//! None of this may panic, and the results must satisfy set algebra. Bounds are
//! all `int4` so the documented "finite bounds of different types" precondition
//! panic in `canonicalize` stays unreachable.
//!
//! Oracles (beyond no-panic):
//!  * union (when representable as one range) contains both operands; union is
//!    commutative and idempotent (`a ∪ a == a`);
//!  * intersection is contained in both operands; intersection is commutative
//!    and idempotent (`a ∩ a == a`);
//!  * difference algebra: when `a ∖ b` is representable, it is a subset of `a`
//!    and is disjoint from `b` (empty intersection with `b`);
//!  * `contains_elem(x)` agrees with "intersecting the point range `[x,x]` is
//!    non-empty".

#![no_main]

use libfuzzer_sys::arbitrary::{self, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_repr::adt::range::{Range, RangeBound};
use mz_repr::Datum;

// None = infinite bound; Some((inclusive, value)) = finite bound.
type BoundSpec = Option<(bool, i32)>;
// None = empty range; Some((lower, upper)) = a range with those bounds.
type RangeSpec = Option<(BoundSpec, BoundSpec)>;

fn make_range(spec: RangeSpec) -> Range<Datum<'static>> {
    let inner = spec.map(|(lo, hi)| {
        let lower = RangeBound {
            inclusive: matches!(lo, Some((true, _))),
            bound: lo.map(|(_, v)| Datum::Int32(v)),
        };
        let upper = RangeBound {
            inclusive: matches!(hi, Some((true, _))),
            bound: hi.map(|(_, v)| Datum::Int32(v)),
        };
        (lower, upper)
    });
    Range::new(inner)
}

/// Build a canonical operand, or `None` if the spec doesn't canonicalize
/// (misordered bounds, etc.).
fn canonical(spec: RangeSpec) -> Option<Range<Datum<'static>>> {
    let mut r = make_range(spec);
    r.canonicalize().ok()?;
    Some(r)
}

/// Re-anchor a `Range<Datum<'_>>` (e.g. a `difference` result) to `'static`.
/// All bounds here are `int4`, which own no borrowed data, so this is just a
/// lifetime launder.
fn to_static(r: Range<Datum<'_>>) -> Range<Datum<'static>> {
    r.into_bounds(|d| match d {
        Datum::Int32(v) => Datum::Int32(v),
        other => unreachable!("non-int4 bound in int4 range: {other:?}"),
    })
}

fn check(a: Range<Datum<'static>>, b: Range<Datum<'static>>, elem: i32) {
    let _ = a.contains_elem(&elem);

    // --- Union: contains both operands, commutative, idempotent. ----------
    if let Ok(union) = a.union(&b) {
        assert!(
            union.contains_range(&a) && union.contains_range(&b),
            "union must contain both operands"
        );
    }
    assert_eq!(
        a.union(&b),
        b.union(&a),
        "union must be commutative"
    );
    if let Ok(uaa) = a.union(&a) {
        assert_eq!(uaa, a, "union must be idempotent (a ∪ a == a)");
    }

    // --- Intersection: contained in both, commutative, idempotent. --------
    let intersection = a.intersection(&b);
    assert!(
        a.contains_range(&intersection) && b.contains_range(&intersection),
        "intersection must be contained in both operands"
    );
    assert_eq!(
        a.intersection(&b),
        b.intersection(&a),
        "intersection must be commutative"
    );
    assert_eq!(
        a.intersection(&a),
        a,
        "intersection must be idempotent (a ∩ a == a)"
    );

    // --- Difference algebra: a ∖ b ⊆ a and disjoint from b. ---------------
    if let Ok(diff) = a.difference(&b) {
        let diff = to_static(diff);
        assert!(
            a.contains_range(&diff),
            "a ∖ b must be a subset of a"
        );
        assert!(
            diff.intersection(&b).inner.is_none(),
            "a ∖ b must be disjoint from b"
        );
    }

    // --- contains_elem(x) consistency vs the point range [x, x]. ----------
    // `[x, x]` canonicalizes to the singleton {x}; if `x == i32::MAX` the upper
    // bound's `step` overflows and canonicalization fails, in which case we
    // simply skip the comparison.
    if let Some(point) = canonical(Some((Some((true, elem)), Some((true, elem))))) {
        let contains = a.contains_elem(&elem);
        let intersects = a.intersection(&point).inner.is_some();
        assert_eq!(
            contains, intersects,
            "contains_elem({elem}) must agree with [x,x] intersection"
        );
    }
}

fn run(mut u: Unstructured) -> arbitrary::Result<()> {
    let ra: RangeSpec = u.arbitrary()?;
    let rb: RangeSpec = u.arbitrary()?;
    let elem: i32 = u.arbitrary()?;
    if let (Some(a), Some(b)) = (canonical(ra), canonical(rb)) {
        check(a, b, elem);
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let _ = run(Unstructured::new(data));
});
