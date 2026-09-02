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
//!    arbitrary partition, and it must hold even with nulls present in the data.
//!    For min/max this is exactly how the bucketed hierarchical reduce works.
//!    `any`/`all` are maintained accumulably in the dataflow instead
//!    (`mz_compute_types::plan::reduce::reduction_type` groups them with the sums
//!    and `Count`, and the accumulator keeps bool counts rather than partial
//!    booleans), but both are idempotent bool monoid homomorphisms, so the same
//!    law holds and pins down `eval`'s three-valued folding.
//!  * **Additive decomposition**: `count(whole)` must equal the sum of the
//!    per-chunk counts. Likewise `sum(whole)` must equal the sum of the
//!    per-chunk sums (this is exactly how accumulable maintenance combines
//!    partial sums across batches). We check the sum law for the *integer* sums
//!    (`SumInt32`/`SumInt64`), where the per-chunk combination is exact.
//!  * **Expansion equivalence** (every aggregate): a row at multiplicity `k` must
//!    aggregate exactly like `k` rows at multiplicity one.
//!
//! We generate a random multiset of nullable datums in one of several type
//! groups, a random permutation of it, and a random partition into chunks, then
//! check the applicable laws for each aggregate over the chosen group.
//!
//! Every generated row carries a random *positive* multiplicity, because
//! `AggregateFunc::eval` consumes `(datum, diff)` pairs and dispatches on the
//! diff three ways: `count` sums the diffs, the signed integer sums scale each
//! value by its diff, and multiplicity-insensitive aggregates
//! (`AggregateFunc::ignores_multiplicity`) drop it. Constant folding is the only
//! caller that ever passes a diff other than one, because
//! `FoldConstants::fold_reduce_constant` consolidates the constant's rows before
//! handing them to `eval` while the dataflow's reduce renderers pass `Diff::ONE`
//! deliberately. So a divergence in that arithmetic is a silent wrong answer
//! reachable only through constant folding, and this is the only coverage of it.
//!
//! The expansion law is what actually pins that arithmetic down, and the
//! permutation and decomposition laws cannot substitute for it. Both of those
//! hold for *any* per-row function summed over the input, so they stay satisfied
//! if `count` counts rows instead of summing diffs, or if the sum drops its
//! `diff` factor: row-counting is additive too. Only comparing against the
//! expanded form distinguishes them. What no law here can detect is a change to
//! `ignores_multiplicity` membership for min/max, since routing those through
//! `expand_counts` instead is genuinely equivalent at positive diffs.
//!
//! Retractions are deliberately out of scope. The signed sums finalize
//! `accum == 0 && non_nulls == 0` to `Null`, which stops being additive once
//! diffs can cancel: `[(0, -1)]` and `[(0, +1)]` each finalize to `0` while
//! their union finalizes to `Null`. Covering retractions needs a law stated over
//! accumulators rather than over finalized datums.
//!
//! Groups: `int4`/`int8`/`bool` (exact integers/booleans, plain equality
//! oracle), `text` (lexicographic min/max), plus `float8` and `numeric`. The
//! float/numeric groups exercise the `OrderedFloat`/`OrderedDecimal` ordering
//! used by min/max. `float8` feeds in the full set of IEEE-754 corners (`NaN`,
//! `±Inf`, `-0.0`); `numeric` feeds the specials a numeric datum can actually
//! hold, which is `NaN` plus equal-value-different-scale pairs (see
//! `SPECIAL_NUMERIC`). We only apply min/max to the float/numeric groups:
//! floating-point and bounded-decimal *sum* is not associative under rounding,
//! so an additive/permutation law over it would be a generator artifact rather
//! than a real product invariant.
//!
//! Datum equality is the oracle throughout, and it is sound for the ordering
//! aggregates because it agrees with the `Ord` that `max_datum`/`min_datum`
//! select by: `Datum`'s derived `PartialEq` delegates to `OrderedFloat` (so
//! `NaN == NaN`) and to `OrderedDecimal`, whose `PartialEq` is defined as
//! `cmp(..) == Equal`. So min/max returning any one of several mutually equal
//! candidates can never trip an assertion here.

#![no_main]

use libfuzzer_sys::arbitrary::{self, Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use mz_expr::AggregateFunc;
use mz_repr::{Datum, Diff, RowArena, strconv};

const MAX_ROWS: usize = 24;
const MAX_CHUNKS: usize = 4;
/// Upper bound on a row's multiplicity. `MAX_ROWS * MAX_DIFF * 2^63` is far
/// inside `i128` and inside numeric's 39 digits of precision, so every partial
/// sum below is exact: `sum_signed_int_counted`'s wrapping arithmetic never
/// wraps, and `SumInt32`'s narrowing to `i64` never truncates.
const MAX_DIFF: u8 = 4;

/// A generated input row: a datum paired with its multiplicity.
type Update = (Datum<'static>, Diff);

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

/// Which decomposition law applies to an aggregate. Permutation invariance and
/// expansion equivalence apply to every aggregate and are checked separately.
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

/// Numeric values worth probing. `Datum::from(i128)` only ever yields canonical,
/// finite, exponent-0 numerics, which makes the integer majority of this group a
/// relabelling of `Group::Int64` through a different comparison function. These
/// add the two datum shapes that actually exercise `OrderedDecimal`'s ordering:
/// `NaN` (whose `Ord` sorts above every finite value), and numerically equal
/// values at different scales, which `OrderedDecimal` reduces before comparing
/// and so calls equal despite differing bit patterns. Both min/max and the
/// equality oracle have to agree on those.
///
/// Every entry must be a value a numeric *datum* can actually hold, otherwise a
/// panic here would be a generator artifact rather than a product bug.
/// `±Infinity` and `-0` are excluded for that reason: `strconv::parse_numeric`
/// rejects a non-overflow infinity outright, `numeric::munge_numeric` folds `-0`
/// to `0` and `-NaN` to `NaN`, and numeric arithmetic returns
/// `EvalError::FloatOverflow` rather than saturating to an infinity. `NaN` is the
/// one special that survives, via `'NaN'::numeric`.
const SPECIAL_NUMERIC: &[&str] = &["NaN", "0", "0.000", "100", "100.00", "1E+2", "-0.5"];

/// A positive multiplicity. See the module doc for why retractions are excluded.
fn gen_diff(u: &mut Unstructured) -> arbitrary::Result<Diff> {
    Ok(Diff::from(i64::from(u.int_in_range(1..=MAX_DIFF)?)))
}

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
            // Bias in the specials at the same ratio as the float group. The
            // majority stays integer-valued, which keeps min/max exact and easy
            // to read, but a fully random i64 never lands on a corner case.
            if u.ratio(1u8, 2u8)? {
                let i = u.int_in_range(0..=SPECIAL_NUMERIC.len() - 1)?;
                Datum::from(
                    strconv::parse_numeric(SPECIAL_NUMERIC[i]).expect("literal numeric parses"),
                )
            } else {
                Datum::from(i128::from(i64::arbitrary(u)?))
            }
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
fn shuffle(u: &mut Unstructured, input: &[Update]) -> arbitrary::Result<Vec<Update>> {
    let mut v = input.to_vec();
    for i in (1..v.len()).rev() {
        let j = u.int_in_range(0..=i)?;
        v.swap(i, j);
    }
    Ok(v)
}

/// Randomly assign each input to one of `1..=MAX_CHUNKS` chunks (some may be
/// empty). The chunks' concatenation is a permutation of the input multiset.
fn partition(u: &mut Unstructured, input: &[Update]) -> arbitrary::Result<Vec<Vec<Update>>> {
    let k = u.int_in_range(1usize..=MAX_CHUNKS)?;
    let mut chunks = vec![Vec::new(); k];
    for &update in input {
        let b = u.int_in_range(0..=k - 1)?;
        chunks[b].push(update);
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
/// chunk yields `Null` (returned as `None`). The values are bounded (see
/// `MAX_DIFF`), so every partial sum fits an `i128` exactly and the per-chunk
/// combination below is lossless.
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
        input.push((gen_datum(u, group)?, gen_diff(u)?));
    }
    let permuted = shuffle(u, &input)?;
    let chunks = partition(u, &input)?;
    // NOTE: expansion is only faithful because every diff is >= 1. At diff 0 the
    // row would vanish here while `ignores_multiplicity` aggregates still see it,
    // so min/max would diverge for reasons that are not a product bug.
    let expanded: Vec<Update> = input
        .iter()
        .flat_map(|&(d, diff)| {
            let copies = usize::try_from(diff.into_inner()).expect("positive diff");
            std::iter::repeat((d, Diff::ONE)).take(copies)
        })
        .collect();

    let arena = RowArena::new();
    for (agg, law) in aggregates(group) {
        let whole = agg.eval(input.iter().copied(), &arena);

        // Permutation invariance: order must never matter.
        let shuffled = agg.eval(permuted.iter().copied(), &arena);
        assert_eq!(
            whole, shuffled,
            "{agg:?} is not permutation-invariant\n  input    = {input:?}\n  permuted = {permuted:?}"
        );

        // Expansion equivalence: a row at multiplicity `k` must aggregate
        // exactly like `k` rows at multiplicity one.
        let unit_diffs = agg.eval(expanded.iter().copied(), &arena);
        assert_eq!(
            whole, unit_diffs,
            "{agg:?} does not treat a diff of k like k unit rows\n  input    = {input:?}\n  expanded = {expanded:?}"
        );

        match law {
            Law::Hierarchical => {
                // agg(whole) == agg(map(agg, non-empty chunks)). Skipping the
                // empty chunks keeps the partials to the results of non-trivial
                // work. It is a no-op for these aggregates: min/max aggregate an
                // empty chunk to `Null` and filter nulls back out on the way in,
                // and any/all fold from their identity (`False`/`True`), so an
                // empty chunk contributes nothing either way.
                let partials: Vec<Datum> = chunks
                    .iter()
                    .filter(|c| !c.is_empty())
                    .map(|c| agg.eval(c.iter().copied(), &arena))
                    .collect();
                // The partials are values, not updates, so each enters the
                // re-aggregation once. Every aggregate under this law ignores
                // multiplicity regardless.
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
                    .map(|c| as_count(agg.eval(c.iter().copied(), &arena)))
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
                    .filter_map(|c| as_sum(agg.eval(c.iter().copied(), &arena)))
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
