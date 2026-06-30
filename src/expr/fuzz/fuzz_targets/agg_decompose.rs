// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz target: aggregate functions must obey the decomposition laws that
//! Materialize's *incremental* reduce relies on. The dataflow engine never
//! re-aggregates a group from scratch. It maintains aggregates by combining
//! partial results, so each aggregate must satisfy:
//!
//!  * **Permutation invariance** (every order-insensitive aggregate): the result
//!    must not depend on the order the inputs arrive in. Accumulable maintenance
//!    (sum, count) applies updates in arrival order, so any order-dependence is a
//!    correctness bug.
//!  * **Hierarchical re-aggregation** (min/max/any/all, idempotent aggregates
//!    whose output type equals their input type): `agg(whole)` must equal
//!    `agg([agg(chunk0), agg(chunk1), ...])` over the non-empty chunks of an
//!    arbitrary partition. This is exactly how the bucketed hierarchical reduce
//!    computes min/max, and it must hold even with nulls present in the data.
//!  * **Additive decomposition**: `count(whole)` must equal the sum of the
//!    per-chunk counts. Likewise `sum(whole)` must equal the sum of the
//!    per-chunk sums (this is exactly how accumulable maintenance combines
//!    partial sums across batches). We check the sum law for the *integer* sums
//!    (`SumInt32`/`SumInt64`), where the per-chunk combination is exact.
//!
//! We generate a random multiset of nullable datums in one of several type
//! groups, a random permutation of it, and a random partition into chunks, then
//! check the applicable laws for each aggregate over the chosen group.
//!
//! Groups: `int4`/`int8`/`bool` (exact integers/booleans, plain equality
//! oracle), `text` (lexicographic min/max), plus `float8` and `numeric`. The
//! float/numeric groups exercise the `OrderedFloat`/`OrderedDecimal` ordering
//! used by min/max and feed in the tricky special values (`NaN`, `±Inf`,
//! `-0.0`). We only apply min/max to the float/numeric groups: floating-point
//! and bounded-decimal *sum* is not
//! associative under rounding, so an additive/permutation law over it would be
//! a generator artifact rather than a real product invariant. Datum equality is
//! `OrderedFloat`-based (so `NaN == NaN`), so it is a sound oracle for the
//! ordering aggregates even with NaN present.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::AggregateFunc;
use mz_repr::{Datum, Diff, RowArena};

const MAX_ROWS: usize = 24;
const MAX_CHUNKS: usize = 4;

#[derive(Clone, Copy)]
enum Group {
    Int32,
    Int64,
    Bool,
    Float64,
    Numeric,
    Str,
}

/// A small fixed pool of `'static` strings for the text min/max group. Keeping
/// them `'static` lets text datums flow through the same `Datum<'static>`
/// shuffle/partition machinery as the scalar groups. The set is deliberately
/// tiny so duplicates and ties are common (which is what stresses min/max's
/// hierarchical tie-breaking), and includes the empty string and a prefix pair
/// (`"a"`/`"ab"`) where lexicographic ordering is subtle.
const POOL_STR: &[&str] = &["", "a", "ab", "abc", "b", "Z", "z", "10", "9"];

/// Which decomposition laws apply to an aggregate (permutation invariance always
/// applies and is checked separately).
enum Law {
    /// Idempotent, output type == input type: `agg(whole) == agg(map(agg, parts))`.
    Hierarchical,
    /// `count(whole) == sum(map(count, parts))`.
    AdditiveCount,
    /// `sum(whole) == sum(map(sum, parts))`, with null partials (empty/all-null
    /// chunks) skipped. Only sound for the exact-integer sums.
    AdditiveSum,
}

/// Float values worth probing: ordinary magnitudes plus the IEEE-754 corners
/// that the `OrderedFloat` total order has to canonicalize (NaN as the maximum,
/// the two infinities, and signed zeros, where `-0.0 == 0.0` but distinct bits).
const SPECIAL_F64: &[f64] = &[
    0.0,
    -0.0,
    1.0,
    -1.0,
    f64::INFINITY,
    f64::NEG_INFINITY,
    f64::NAN,
    f64::MIN,
    f64::MAX,
];

fn gen_datum(u: &mut Unstructured, group: Group) -> arbitrary::Result<Datum<'static>> {
    if u.ratio(1u8, 5u8)? {
        return Ok(Datum::Null);
    }
    Ok(match group {
        Group::Int32 => Datum::Int32(i32::arbitrary(u)?),
        Group::Int64 => Datum::Int64(i64::arbitrary(u)?),
        Group::Bool => {
            if bool::arbitrary(u)? {
                Datum::True
            } else {
                Datum::False
            }
        }
        Group::Float64 => {
            // Bias toward the special values so min/max actually has to order
            // NaN/Inf/-0.0 against ordinary numbers. Otherwise a fully random
            // f64 almost never lands on a corner case.
            let f = if u.ratio(1u8, 2u8)? {
                let i = u.int_in_range(0..=SPECIAL_F64.len() - 1)?;
                SPECIAL_F64[i]
            } else {
                f64::arbitrary(u)?
            };
            Datum::from(f)
        }
        Group::Numeric => {
            // Integer-valued numerics keep min/max exact and easy to read. The
            // point of the group is the `OrderedDecimal` comparison path, not
            // fractional precision.
            Datum::from(i128::from(i64::arbitrary(u)?))
        }
        Group::Str => {
            let i = u.int_in_range(0..=POOL_STR.len() - 1)?;
            Datum::String(POOL_STR[i])
        }
    })
}

fn aggregates(group: Group) -> Vec<(AggregateFunc, Law)> {
    match group {
        Group::Int32 => vec![
            (AggregateFunc::MaxInt32, Law::Hierarchical),
            (AggregateFunc::MinInt32, Law::Hierarchical),
            (AggregateFunc::SumInt32, Law::AdditiveSum),
            (AggregateFunc::Count, Law::AdditiveCount),
        ],
        Group::Int64 => vec![
            (AggregateFunc::MaxInt64, Law::Hierarchical),
            (AggregateFunc::MinInt64, Law::Hierarchical),
            (AggregateFunc::SumInt64, Law::AdditiveSum),
            (AggregateFunc::Count, Law::AdditiveCount),
        ],
        Group::Bool => vec![
            (AggregateFunc::MaxBool, Law::Hierarchical),
            (AggregateFunc::MinBool, Law::Hierarchical),
            (AggregateFunc::Any, Law::Hierarchical),
            (AggregateFunc::All, Law::Hierarchical),
            (AggregateFunc::Count, Law::AdditiveCount),
        ],
        // Float/numeric sum is not exactly associative under rounding, so we only
        // assert the ordering laws (min/max are total-order selections and stay
        // exact, NaN included) and the count law.
        Group::Float64 => vec![
            (AggregateFunc::MaxFloat64, Law::Hierarchical),
            (AggregateFunc::MinFloat64, Law::Hierarchical),
            (AggregateFunc::Count, Law::AdditiveCount),
        ],
        Group::Numeric => vec![
            (AggregateFunc::MaxNumeric, Law::Hierarchical),
            (AggregateFunc::MinNumeric, Law::Hierarchical),
            (AggregateFunc::Count, Law::AdditiveCount),
        ],
        // Text min/max select by lexicographic byte order, output type == input
        // type, so the hierarchical re-aggregation law applies.
        Group::Str => vec![
            (AggregateFunc::MaxString, Law::Hierarchical),
            (AggregateFunc::MinString, Law::Hierarchical),
            (AggregateFunc::Count, Law::AdditiveCount),
        ],
    }
}

/// A Fisher-Yates shuffle driven by the fuzz input.
fn shuffle(
    u: &mut Unstructured,
    input: &[Datum<'static>],
) -> arbitrary::Result<Vec<Datum<'static>>> {
    let mut v = input.to_vec();
    for i in (1..v.len()).rev() {
        let j = u.int_in_range(0..=i)?;
        v.swap(i, j);
    }
    Ok(v)
}

/// Randomly assign each input to one of `1..=MAX_CHUNKS` chunks (some may be
/// empty). The chunks' concatenation is a permutation of the input multiset.
fn partition(
    u: &mut Unstructured,
    input: &[Datum<'static>],
) -> arbitrary::Result<Vec<Vec<Datum<'static>>>> {
    let k = u.int_in_range(1usize..=MAX_CHUNKS)?;
    let mut chunks = vec![Vec::new(); k];
    for &d in input {
        let b = u.int_in_range(0..=k - 1)?;
        chunks[b].push(d);
    }
    Ok(chunks)
}

fn as_count(d: Datum) -> i64 {
    match d {
        Datum::Int64(n) => n,
        other => panic!("Count produced a non-int8 datum: {other:?}"),
    }
}

/// Decode an integer-sum result datum to an exact `i128`. `SumInt32` yields an
/// `Int64`, `SumInt64` yields an (integer-valued) `Numeric`. An empty/all-null
/// chunk yields `Null` (returned as `None`). The values are bounded (<= 24
/// inputs of at most i64 magnitude), so every partial sum fits an `i128`
/// exactly and the per-chunk combination below is lossless.
fn as_sum(d: Datum) -> Option<i128> {
    match d {
        Datum::Null => None,
        Datum::Int64(n) => Some(i128::from(n)),
        Datum::Numeric(n) => {
            Some(i128::try_from(n.0).expect("integer-valued sum must fit an i128"))
        }
        other => panic!("integer sum produced an unexpected datum: {other:?}"),
    }
}

fn run(u: &mut Unstructured) -> arbitrary::Result<()> {
    let group = match u.int_in_range(0u8..=5)? {
        0 => Group::Int32,
        1 => Group::Int64,
        2 => Group::Bool,
        3 => Group::Float64,
        4 => Group::Numeric,
        _ => Group::Str,
    };

    let n = u.int_in_range(0usize..=MAX_ROWS)?;
    let mut input = Vec::with_capacity(n);
    for _ in 0..n {
        input.push(gen_datum(u, group)?);
    }
    let permuted = shuffle(u, &input)?;
    let chunks = partition(u, &input)?;

    let arena = RowArena::new();
    for (agg, law) in aggregates(group) {
        // `AggregateFunc::eval` consumes `(datum, multiplicity)` pairs. Each
        // generated row has multiplicity one, so pair every datum with
        // `Diff::ONE`.
        let whole = agg.eval(input.iter().map(|&d| (d, Diff::ONE)), &arena);

        // Permutation invariance: order must never matter.
        let shuffled = agg.eval(permuted.iter().map(|&d| (d, Diff::ONE)), &arena);
        assert_eq!(
            whole, shuffled,
            "{agg:?} is not permutation-invariant\n  input    = {input:?}\n  permuted = {permuted:?}"
        );

        match law {
            Law::Hierarchical => {
                // agg(whole) == agg(map(agg, non-empty chunks)). Empty chunks are
                // skipped: an empty chunk aggregates to null, and for any/all
                // (three-valued) a stray null would corrupt an otherwise false/true
                // result (`false OR null = null`). Min/max absorb null as their
                // identity, but skipping is correct for all of them.
                let partials: Vec<Datum> = chunks
                    .iter()
                    .filter(|c| !c.is_empty())
                    .map(|c| agg.eval(c.iter().map(|&d| (d, Diff::ONE)), &arena))
                    .collect();
                let reaggregated = agg.eval(partials.iter().map(|&d| (d, Diff::ONE)), &arena);
                assert_eq!(
                    whole, reaggregated,
                    "{agg:?} fails hierarchical re-aggregation\n  input  = {input:?}\n  chunks = {chunks:?}\n  partials = {partials:?}"
                );
            }
            Law::AdditiveCount => {
                // count(whole) == sum(map(count, chunks))
                let total: i64 = chunks
                    .iter()
                    .map(|c| as_count(agg.eval(c.iter().map(|&d| (d, Diff::ONE)), &arena)))
                    .sum();
                assert_eq!(
                    as_count(whole),
                    total,
                    "{agg:?} fails additive decomposition\n  input  = {input:?}\n  chunks = {chunks:?}"
                );
            }
            Law::AdditiveSum => {
                // sum(whole) == sum(map(sum, chunks)), combining the non-null
                // per-chunk partials. A chunk that is empty or all-null sums to
                // Null and contributes nothing. If *every* element is null the
                // whole is also Null, so both sides are "no partials" and match.
                let partials: Vec<i128> = chunks
                    .iter()
                    .filter_map(|c| as_sum(agg.eval(c.iter().map(|&d| (d, Diff::ONE)), &arena)))
                    .collect();
                let combined: Option<i128> = if partials.is_empty() {
                    None
                } else {
                    Some(partials.iter().sum())
                };
                assert_eq!(
                    as_sum(whole),
                    combined,
                    "{agg:?} fails additive sum decomposition\n  input  = {input:?}\n  chunks = {chunks:?}"
                );
            }
        }
    }
    Ok(())
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let _ = run(&mut u);
});
