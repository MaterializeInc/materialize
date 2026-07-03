// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Scalar equality-saturation over the combined e-graph.
//!
//! The scalar canonicalizer runs its rules through the one CombinedLang
//! machinery. The bounds are copied from the standalone `scalar/egraph.rs`
//! (600 / 1_000 / 100) so the fixpoint reached here is identical to the old
//! engine's, which the differential test gates on. Scalar rules run only here,
//! never in the relational `EGraph::saturate` pass.

// `canonicalize_combined` is the production scalar canonicalizer, reached
// through `scalar::canonicalize_predicates` when `enable_eqsat_scalar_canonicalize`
// is set. The standalone engine in `scalar/egraph.rs` remains only as the parity
// oracle exercised by this module's differential tests.

use mz_expr::MirScalarExpr;
use mz_repr::ReprColumnType;

use crate::eqsat::core::Id;
use crate::eqsat::egraph::view::BaseView;
use crate::eqsat::egraph::{Analyses, CNode, EBindings, EGraph, Index};
use crate::eqsat::rules;
use crate::eqsat::scalar::analysis::{ClassAnalysis, make, merge};
use crate::eqsat::scalar_extract;

/// E-node budget for the saturation loop. Copied from `scalar/egraph.rs` so the
/// scalar fixpoint matches the standalone engine's (parity).
const MAX_ENODES: usize = 600;

/// Per-round match cap per rule. Copied from `scalar/egraph.rs`.
const MATCH_LIMIT: usize = 1_000;

/// Maximum saturation iterations. Copied from `scalar/egraph.rs`.
const MAX_ITERS: usize = 100;

/// Recompute the scalar per-class analysis as a monotone least-fixpoint over the
/// current class layout. The CombinedLang port of `scalar/egraph.rs::recompute_analysis`.
///
/// Operates only on scalar classes (a class holding at least one `CNode::Scalar`).
/// A purely relational class carries no scalar analysis entry, and a scalar
/// node's children are always scalar classes, so restricting to scalar classes
/// keeps `make`'s bottom-up child-lookup invariant intact.
///
/// Seed every scalar class to the merge identity, then repeatedly recompute each
/// class as the merge over its scalar nodes' `make` contributions (reading the
/// current, possibly still-seed, child values) until a pass changes nothing.
/// Pre-seeding makes self-referential classes (from constant folding) sound:
/// `make` always finds its children present, and a self-referential child reads
/// the class's current value rather than being dropped. The lattice is finite
/// and `make`/`merge` are monotone (`could_error` only rises, `literal` only
/// goes None -> Some), so this converges to the conservative upper bound.
///
/// This must run after each `rebuild` (see [`saturate`]): the core's `rebuild`
/// only restores congruence and fires the incremental `on_union` hook, so without
/// this pass a class formed by constant folding could carry an under-approximated
/// `could_error`, the one unsound direction for a guard that blocks error-unsafe
/// rewrites.
fn recompute_analysis(eg: &mut EGraph) {
    let scalar_classes: Vec<Id> = eg
        .class_ids()
        .into_iter()
        .filter(|&id| eg.nodes(id).iter().any(|n| matches!(n, CNode::Scalar(_))))
        .collect();

    eg.data_mut().scalar.analysis.clear();
    for &id in &scalar_classes {
        eg.data_mut().scalar.analysis.insert(
            id,
            ClassAnalysis {
                could_error: false,
                literal: None,
            },
        );
    }
    loop {
        let mut changed = false;
        for &id in &scalar_classes {
            let mut acc = ClassAnalysis {
                could_error: false,
                literal: None,
            };
            for node in eg.nodes(id) {
                let CNode::Scalar(s) = node else { continue };
                let node_a = {
                    let store = &eg.data().scalar.analysis;
                    let find = |c| eg.find(c);
                    make(&s, store, &find)
                };
                acc = merge(acc, node_a);
            }
            // Read the current value into locals so the immutable borrow ends
            // before the mutable `data_mut()` insert below.
            let (cur_err, cur_has_lit) = {
                let cur = eg
                    .data()
                    .scalar
                    .analysis
                    .get(&id)
                    .expect("class seeded above");
                (cur.could_error, cur.literal.is_some())
            };
            // Both fields are monotone, so an inequality is always an increase.
            // Comparing `literal` by presence suffices: equal classes carry the
            // same literal.
            if cur_err != acc.could_error || cur_has_lit != acc.literal.is_some() {
                changed = true;
                eg.data_mut().scalar.analysis.insert(id, acc);
            }
        }
        if !changed {
            break;
        }
    }
}

/// Saturate the scalar rules over `eg`, returning the iteration count.
///
/// The relational `EGraph::saturate` must NOT be reused: it recomputes the
/// relational analyses and reads relational bounds, which would break scalar
/// parity. This loop mirrors the standalone scalar driver instead. Unlike the
/// relational loop it has no per-rule backoff (the old scalar driver had none).
pub(crate) fn saturate(eg: &mut EGraph) -> usize {
    let ruleset = rules::scalar_all();
    let compiled = ruleset.rules();
    let mut iters = 0;
    for _ in 0..MAX_ITERS {
        iters += 1;
        eg.rebuild();
        recompute_analysis(eg);
        if eg.node_count() > MAX_ENODES {
            break;
        }

        // Phase 1 (read-only): collect matches. Scalar rules read neither the
        // relational analyses nor the relational index, so an empty `Analyses`
        // and empty relational `Index` are passed; the scalar index is the live
        // match surface.
        let scalar_index = eg.scalar_index();
        let index = Index::new();
        let analyses = Analyses::default();
        let mut pending: Vec<(usize, EBindings)> = Vec::new();
        {
            let view = BaseView {
                eg,
                index: &index,
                scalar_index: &scalar_index,
                an: &analyses,
            };
            for (qi, rule) in compiled.iter().enumerate() {
                let (matches, _hit) = (rule.find)(&view, &analyses, MATCH_LIMIT + 1);
                for b in matches.into_iter().take(MATCH_LIMIT) {
                    pending.push((qi, b));
                }
            }
        }

        // Phase 2 (mutate): apply and union.
        let mut changed = false;
        for (qi, b) in pending {
            if let Ok(new_id) = (compiled[qi].apply)(eg, &b) {
                if eg.union(new_id, b.root) {
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    iters
}

/// Canonicalize a scalar expression through the combined machinery: lower into a
/// fresh e-graph, saturate the scalar rules, then extract the cheapest form.
///
/// `col_types` is stored for the typed-literal rules to read (unused by the
/// slice-1 rule set, required by the entry's contract for later slices). With an
/// empty scalar rule set this is the identity.
pub(crate) fn canonicalize_combined(
    expr: &MirScalarExpr,
    col_types: &[ReprColumnType],
) -> MirScalarExpr {
    let mut eg = EGraph::new();
    eg.data_mut().scalar.col_types = col_types.to_vec();
    let root = crate::eqsat::scalar::lower::lower_into(&mut eg, expr);
    saturate(&mut eg);
    scalar_extract::raise(&eg, root)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_expr::{MirScalarExpr, UnaryFunc};

    #[mz_ore::test]
    fn canonicalize_combined_is_identity_without_rules() {
        // A single `Not` matches no compiled scalar rule (`not_not` needs a
        // double negation), so this is the identity.
        let e = MirScalarExpr::column(0).call_unary(UnaryFunc::Not(mz_expr::func::Not));
        assert_eq!(canonicalize_combined(&e, &[]), e);
    }

    #[mz_ore::test]
    fn not_not_rewrites_via_combined() {
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let x = MirScalarExpr::column(0);
        let e = not(not(x.clone()));
        assert_eq!(canonicalize_combined(&e, &[]), x);
    }

    // Frozen differential-parity snapshot (SP2b Slice 1): asserts the combined
    // path produces the documented rewrite for the expressions the ported
    // rules cover. `canonicalize_combined` is `pub(crate)`, so this lives
    // in-crate rather than as an external integration test; the committed
    // corpus fixture is read via `include_str!` relative to this file. These
    // snapshots were originally differential (checked against a standalone
    // scalar engine, since deleted once the combined path became production);
    // the frozen `expected` values are that oracle's last-agreed output.
    //
    // Slice 1 ports only `not_not`, so the corpus is restricted to
    // `not(not(...))`-shaped expressions over a bare column: no literals, no
    // type context, nothing that would trigger an unported rule. A failure
    // here is the slice-1 go/no-go trigger, not something to paper over by
    // adjusting the assertion.

    /// Committed corpus fixture (see the file for the format and slice-1 scope).
    const CORPUS: &str = include_str!("../../tests/testdata/eqsat_scalar_corpus");

    #[mz_ore::test]
    fn scalar_parity_not_not() {
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let x = MirScalarExpr::column(0);
        // (input, expected): a double negation collapses, a triple leaves one
        // NOT standing, and a bare column is already the identity.
        let cases = vec![
            (not(not(x.clone())), x.clone()),
            (not(not(not(x.clone()))), not(x.clone())),
            (x.clone(), x.clone()),
        ];
        for (e, expected) in cases {
            let new = canonicalize_combined(&e, &[]);
            assert_eq!(new, expected, "parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice1() {
        // Slice 1 requires at least one double-negation case. Later slices add
        // tie / could_error / type-context coverage assertions here.
        assert!(CORPUS.contains("not(not("), "corpus must exercise not_not");
    }

    // Differential parity harness (SP2b Slice 2): extends slice 1 to the
    // variadic rules (`and_single`, `or_single`, `not_demorgan_and`,
    // `not_demorgan_or`). Same corpus-shaping constraint as slice 1: distinct
    // bare boolean columns only, so none of the old engine's unported rules
    // (const_fold, dedup, flatten_assoc, not_binary_negate, ...) fire and
    // create a divergence unrelated to the rules under test.
    #[mz_ore::test]
    fn scalar_parity_variadic() {
        use mz_expr::{MirScalarExpr, UnaryFunc, VariadicFunc};
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let c = MirScalarExpr::column;

        // (input, expected). The 3-and/4-or De Morgan candidates are frozen
        // UNCHANGED: the tree-size extractor keeps the original form because
        // the de-Morganed alternative (one NOT per operand) always costs more
        // when there is no further not_not/absorb to shrink it back down.
        let not_and3 = not(and(vec![c(0), c(1), c(2)]));
        let not_or4 = not(or(vec![c(0), c(1), c(2), c(3)]));
        let cases = vec![
            (and(vec![c(0)]), c(0)),
            (or(vec![c(0)]), c(0)),
            (not_and3.clone(), not_and3),
            (not_or4.clone(), not_or4),
            (not(and(vec![c(0)])), not(c(0))),
            // Multi-operand de Morgan that OBSERVABLY changes the extracted
            // output: Not(And(Not #0, Not #1)) de-Morgans to
            // Or(Not Not #0, Not Not #1), then not_not collapses to Or(#0, #1)
            // (cost 3), beating the original (cost 6), so extraction picks the
            // pushed form. Uses only ported rules (not_demorgan_and, not_not)
            // over distinct bare columns.
            (not(and(vec![not(c(0)), not(c(1))])), or(vec![c(0), c(1)])),
            // slice-1 shapes still hold under the grown rule set:
            (not(not(c(0))), c(0)),
        ];
        for (e, expected) in cases {
            // Boolean, type-agnostic rules: `&[]` col_types is sufficient (the
            // rules ported here never read a column type).
            let new = canonicalize_combined(&e, &[]);
            assert_eq!(new, expected, "parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice2() {
        assert!(
            CORPUS.contains("and(#0)"),
            "corpus must exercise and_single"
        );
        assert!(CORPUS.contains("or(#0)"), "corpus must exercise or_single");
        assert!(
            CORPUS.contains("not(and(#0, #1, #2))"),
            "corpus must exercise multi-operand de Morgan"
        );
    }

    // Differential parity harness (SP2b Slice 3): extends slices 1-2 to the
    // analysis-gated If rules (`if_true`, `if_false_or_null`, `if_same_branches`).
    // This is the first workout of the `could_error` gate axis, not just the
    // `literal` axis: the positive case proves the gate fires when the condition
    // cannot error, and the negative control proves it blocks when the condition
    // can, with parity holding on both sides because `if_same_branches` is the
    // identical could_error-gated rule in both engines.
    //
    // Same corpus-shaping constraint as slices 1-2: every input is built so the
    // old engine's unported rules (const_fold, if_err_cond, err_prop, ...) have
    // no literal-only or error-literal subterm to seize on. The could_error
    // negative control divides two COLUMNS, not literals: `BinaryFunc::could_error`
    // is a static per-function property independent of operand literalness, so
    // the gate sees `could_error == true` without ever needing to fold `1/0` into
    // an error literal, which would let the old engine's const_fold (unported)
    // collapse the condition to a literal and diverge from the combined engine
    // for reasons unrelated to the could_error gate under test.
    #[mz_ore::test]
    fn scalar_parity_if() {
        use mz_expr::{BinaryFunc, MirScalarExpr, UnaryFunc, VariadicFunc};
        use mz_repr::{Datum, ReprColumnType, ReprScalarType};

        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let c = MirScalarExpr::column;
        let if_expr =
            |cond: MirScalarExpr, then: MirScalarExpr, els: MirScalarExpr| MirScalarExpr::If {
                cond: Box::new(cond),
                then: Box::new(then),
                els: Box::new(els),
            };
        let bool_ct = || ReprScalarType::Bool.nullable(false);
        let int_ct = || ReprScalarType::Int64.nullable(false);

        // Literal-bool/null folds (`literal` analysis axis). Typed bool columns
        // for the branches, and a typed bool literal/null for the condition.
        let true_fold = if_expr(MirScalarExpr::literal_true(), c(0), c(1));
        let false_fold = if_expr(MirScalarExpr::literal_false(), c(0), c(1));
        let null_fold = if_expr(
            MirScalarExpr::literal_null(ReprScalarType::Bool),
            c(0),
            c(1),
        );

        // could_error gate, POSITIVE: #0 is a bare bool column (cannot error),
        // so if_same_branches must collapse identical branches.
        let same_branches_ok = if_expr(c(0), c(1), c(1));

        // could_error gate, NEGATIVE control: the condition divides two columns,
        // which can error (division by zero), so if_same_branches must NOT
        // collapse, in either engine.
        let div_cond = c(0)
            .call_binary(c(1), BinaryFunc::DivInt64(mz_expr::func::DivInt64))
            .call_binary(
                MirScalarExpr::literal_ok(Datum::Int64(0), ReprScalarType::Int64),
                BinaryFunc::Eq(mz_expr::func::Eq),
            );
        let same_branches_errcond = if_expr(div_cond, c(2), c(2));

        // (input, expected, col_types). same_branches_errcond is frozen
        // UNCHANGED: the could_error gate blocks if_same_branches so the
        // negative control must survive intact.
        let cases: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (true_fold, c(0), vec![bool_ct(), bool_ct()]),
            (false_fold, c(1), vec![bool_ct(), bool_ct()]),
            (null_fold, c(1), vec![bool_ct(), bool_ct()]),
            (same_branches_ok, c(1), vec![bool_ct(), bool_ct()]),
            (
                same_branches_errcond.clone(),
                same_branches_errcond,
                vec![int_ct(), int_ct(), int_ct()],
            ),
        ];
        for (e, expected, ct) in cases {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "parity failed for {e:?} with col_types {ct:?}"
            );
        }

        // Regression sampling of slice-1/2 shapes: parity still holds for the
        // not_not/variadic rules under the grown If rule set.
        let not_and3 = not(and(vec![c(0), c(1), c(2)]));
        let regression = vec![
            (not(not(c(0))), c(0)),
            (and(vec![c(0)]), c(0)),
            (not_and3.clone(), not_and3),
        ];
        for (e, expected) in regression {
            let new = canonicalize_combined(&e, &[]);
            assert_eq!(new, expected, "regression parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice3() {
        assert!(
            CORPUS.contains("if(true,"),
            "corpus must exercise the if_true literal-bool fold"
        );
        assert!(
            CORPUS.contains("if(#0, #1, #1)"),
            "corpus must exercise the if_same_branches could_error gate"
        );
    }

    // Differential parity harness (SP2b Slice 4): extends slices 1-3 to
    // const_fold (class-level literal evaluation via `scalar_builtins::const_eval`)
    // and the AND/OR empty identities (`and_empty`, `or_empty`).
    //
    // const_fold is the highest-risk port so far: it is the first ported rule
    // whose RHS runs real `mz_expr` evaluation rather than a declarative
    // template, and the first to touch runtime errors as data. The corpus
    // below walks every axis: non-error folds over Unary/Binary/If, error-as-
    // data (division by zero and integer overflow), a nested fold where one
    // fold's error-literal output becomes another fold's input, a
    // partial-literal call that must NOT fold, the AND/OR empty identities,
    // and the interaction between `and_empty` and the slice-2 `and_single`
    // rule (they must reach a shared fixpoint, not fight).
    //
    // Same corpus-shaping constraint as slices 1-3: every input keeps the old
    // engine's unported rules (and_or_dedup, and_or_short_circuit,
    // flatten_assoc, factor_and_or, absorb_and_or, not_binary_negate,
    // if_err_cond, null/err_prop, isnull_fold, ...) from having anything to
    // seize on, so a mismatch here is a real const_fold/and_empty/or_empty
    // divergence, not a corpus artifact. const_fold itself is ported to both
    // engines, so, unlike slice 3's could_error control, a bare `1 / 0` is
    // safe to use directly: both engines fold it the same way.
    #[mz_ore::test]
    fn scalar_parity_const_eval() {
        use mz_expr::{BinaryFunc, EvalError, MirScalarExpr, UnaryFunc, VariadicFunc};
        use mz_repr::{Datum, ReprColumnType, ReprScalarType};

        let err_lit = |err: EvalError, typ: ReprScalarType, nullable: bool| {
            MirScalarExpr::Literal(
                Err(err),
                ReprColumnType {
                    scalar_type: typ,
                    nullable,
                },
            )
        };
        let c = MirScalarExpr::column;
        let int_lit = |v: i64| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let add64 = || BinaryFunc::AddInt64(mz_expr::func::AddInt64);
        let div64 = || BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let if_expr =
            |cond: MirScalarExpr, then: MirScalarExpr, els: MirScalarExpr| MirScalarExpr::If {
                cond: Box::new(cond),
                then: Box::new(then),
                els: Box::new(els),
            };
        let bool_ct = || ReprScalarType::Bool.nullable(false);
        let int_ct = || ReprScalarType::Int64.nullable(false);

        // All-literal fold, non-error.
        let add_lit = int_lit(1).call_binary(int_lit(2), add64());
        let not_true = not(MirScalarExpr::literal_true());
        let if_lit = if_expr(MirScalarExpr::literal_true(), int_lit(1), int_lit(2));

        // All-literal fold, error-as-data (the negative control): division by
        // zero and integer overflow must fold to the same error literal in
        // both engines.
        let div_by_zero = int_lit(1).call_binary(int_lit(0), div64());
        let overflow = int_lit(i64::MAX).call_binary(int_lit(1), add64());

        // Nested pre-existing error-literal child.
        let nested_err = div_by_zero.clone().call_binary(int_lit(5), add64());

        // Partial-literal: a Column child blocks the fold.
        let partial = c(0).call_binary(int_lit(1), add64());

        // Empty identities.
        let and_empty = and(vec![]);
        let or_empty = or(vec![]);

        // (input, expected, col_types). `partial` is frozen UNCHANGED: the
        // Column child blocks const_fold.
        let cases: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (add_lit, int_lit(3), vec![]),
            (not_true, MirScalarExpr::literal_false(), vec![]),
            (if_lit, int_lit(1), vec![]),
            (
                div_by_zero,
                err_lit(EvalError::DivisionByZero, ReprScalarType::Int64, false),
                vec![],
            ),
            (
                overflow,
                err_lit(
                    EvalError::NumericFieldOverflow,
                    ReprScalarType::Int64,
                    false,
                ),
                vec![],
            ),
            (
                nested_err,
                err_lit(EvalError::DivisionByZero, ReprScalarType::Int64, false),
                vec![],
            ),
            (partial.clone(), partial, vec![int_ct()]),
            (and_empty, MirScalarExpr::literal_true(), vec![]),
            (or_empty, MirScalarExpr::literal_false(), vec![]),
        ];
        for (e, expected, ct) in cases {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "parity failed for {e:?} with col_types {ct:?}"
            );
        }

        // Regression sampling of slice-1/2/3 shapes under the grown rule set,
        // including the and_single/and_empty interaction: And(#0) must still
        // collapse to #0 via and_single, not and_empty.
        let regression: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (not(not(c(0))), c(0), vec![bool_ct()]),
            (and(vec![c(0)]), c(0), vec![bool_ct()]),
            (if_expr(c(0), c(1), c(1)), c(1), vec![bool_ct(), bool_ct()]),
        ];
        for (e, expected, ct) in regression {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "regression parity failed for {e:?}");
        }

        // and_empty and and_single must not fight: lower `And(#0)` directly
        // (bypassing `canonicalize_combined`) so the iteration count is
        // visible, and assert saturation converges well under the 100-round
        // cap rather than merely not timing out. Mirrors the old engine's
        // `test_fold_terminates` (`scalar/rules.rs`).
        let mut eg = EGraph::new();
        eg.data_mut().scalar.col_types = vec![bool_ct()];
        let _root = crate::eqsat::scalar::lower::lower_into(&mut eg, &and(vec![c(0)]));
        let iters = saturate(&mut eg);
        assert!(
            iters <= 10,
            "and_empty/and_single must reach a fixpoint quickly; got {iters} iters"
        );
    }

    #[mz_ore::test]
    fn corpus_covers_const_eval() {
        assert!(
            CORPUS.contains("1 + 2"),
            "corpus must exercise the non-error literal fold"
        );
        assert!(
            CORPUS.contains("1 / 0"),
            "corpus must exercise the division-by-zero error-as-data fold"
        );
        assert!(
            CORPUS.contains("i64::MAX + 1"),
            "corpus must exercise the integer-overflow error-as-data fold"
        );
        assert!(
            CORPUS.contains("(1 / 0) + 5"),
            "corpus must exercise a nested pre-existing error-literal child"
        );
        assert!(
            CORPUS.contains("#0 + 1"),
            "corpus must exercise a partial-literal call that must not fold"
        );
        assert!(
            CORPUS.contains("and()"),
            "corpus must exercise the and_empty identity"
        );
        assert!(
            CORPUS.contains("or()"),
            "corpus must exercise the or_empty identity"
        );
        assert!(
            CORPUS.contains("and(#0)"),
            "corpus must exercise the and_empty/and_single interaction"
        );
    }

    // Differential parity harness (SP2b Slice 5): extends slices 1-4 to the
    // remaining could_error/literal-gated builtins (`if_err_cond`,
    // `null_prop_binary`, `err_prop_binary`, `isnull_fold`) and the
    // metavar-function rule `not_binary_negate`, the last rule ported before
    // the slice-6 variadic-set batch.
    //
    // `not_binary_negate` is the highest-risk axis here: it is the first
    // ported rule whose right-hand side reconstructs a DIFFERENT function
    // symbol read off a bound metavariable (`negate(f)`), not a fixed
    // template. A wrong table entry for even one func would be invisible to a
    // spot check, so the negation axis below enumerates every `BinaryFunc`
    // pair `negate()` returns `Some` for (`Eq`/`NotEq`, `Lt`/`Gte`,
    // `Lte`/`Gt`, confirmed exhaustive against the `#[sqlfunc(negate = ..)]`
    // attributes in `src/expr/src/scalar/func.rs`) crossed with three
    // null-operand shapes.
    //
    // Same corpus-shaping constraint as slices 1-4: every input keeps the old
    // engine's still-unported slice-6 variadic-set rules
    // (`null_prop_variadic`, `err_prop_variadic`, `and_or_dedup`,
    // `and_or_short_circuit`, `and_or_drop_unit`, `flatten_assoc`,
    // `factor_and_or`, `absorb_and_or`) from having anything to seize on: no
    // variadic And/Or of arity > 1 appears anywhere below, so a mismatch here
    // is a real slice-5 divergence, not a corpus artifact.
    #[mz_ore::test]
    fn scalar_parity_slice5() {
        use mz_expr::{BinaryFunc, EvalError, MirScalarExpr, UnaryFunc};
        use mz_repr::ReprScalarType;

        let c = MirScalarExpr::column;
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let int_lit =
            |v: i64| MirScalarExpr::literal_ok(mz_repr::Datum::Int64(v), ReprScalarType::Int64);
        let null_int = || MirScalarExpr::literal_null(ReprScalarType::Int64);
        let null_bool = || MirScalarExpr::literal_null(ReprScalarType::Bool);
        let div64 = || BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        let add64 = || BinaryFunc::AddInt64(mz_expr::func::AddInt64);
        let if_expr =
            |cond: MirScalarExpr, then: MirScalarExpr, els: MirScalarExpr| MirScalarExpr::If {
                cond: Box::new(cond),
                then: Box::new(then),
                els: Box::new(els),
            };
        let int_ct = |nullable: bool| ReprScalarType::Int64.nullable(nullable);
        let err_lit = |err: EvalError, typ: ReprScalarType, nullable: bool| {
            MirScalarExpr::Literal(
                Err(err),
                ReprColumnType {
                    scalar_type: typ,
                    nullable,
                },
            )
        };

        let mut cases: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = Vec::new();

        // --- if_err_cond: a literal-error condition (`1 / 0`, folded by the
        // already-ported const_fold) folds the whole If to that error. The
        // branches carry DISTINCT ReprColumnTypes (#0 non-nullable, #1
        // nullable Int64), so the result exercises the `then.typ.union(els.typ)`
        // merge path rather than a same-type trivial union.
        let err_cond_if = if_expr(int_lit(1).call_binary(int_lit(0), div64()), c(0), c(1));
        cases.push((
            err_cond_if,
            err_lit(EvalError::DivisionByZero, ReprScalarType::Int64, true),
            vec![int_ct(false), int_ct(true)],
        ));

        // --- null_prop_binary: AddInt64(null, #0) with a bare-column (never
        // could_error) other operand folds to null. AddInt64(null, #0 / #1)
        // does NOT fold: the other operand is a division of two columns,
        // which `could_error` intrinsically, regardless of literalness, so
        // the gate blocks the rewrite in both engines (parity on the
        // "no fold" outcome, not just on folds).
        let null_prop_ok = null_int().call_binary(c(0), add64());
        cases.push((null_prop_ok, null_int(), vec![int_ct(false)]));

        let erroring_div = c(0).call_binary(c(1), div64());
        let null_prop_blocked = null_int().call_binary(erroring_div, add64());
        cases.push((
            null_prop_blocked.clone(),
            null_prop_blocked,
            vec![int_ct(false), int_ct(false)],
        ));

        // --- err_prop_binary: (1 / 0) + #0, other operand a bare column,
        // folds to the division's error. (1 / 0) + (#0 / #1) does NOT fold:
        // the other operand can also error, so the gate blocks substituting
        // one error for another that eval might surface first. Its OWN `1 /
        // 0` operand is still independently folded by const_fold though, so
        // the frozen form is `err_lit + (#0 / #1)`, not the raw input.
        let err_expr = int_lit(1).call_binary(int_lit(0), div64());
        let err_prop_ok = err_expr.clone().call_binary(c(0), add64());
        cases.push((
            err_prop_ok,
            err_lit(EvalError::DivisionByZero, ReprScalarType::Int64, false),
            vec![int_ct(false)],
        ));

        let erroring_div2 = c(0).call_binary(c(1), div64());
        let err_prop_blocked = err_expr.call_binary(erroring_div2.clone(), add64());
        cases.push((
            err_prop_blocked,
            err_lit(EvalError::DivisionByZero, ReprScalarType::Int64, false)
                .call_binary(erroring_div2, add64()),
            vec![int_ct(false), int_ct(false)],
        ));

        // --- isnull_fold: IsNull(#0) folds to false when #0 is non-nullable
        // (and error-free, trivially true for a bare column). The nullable
        // control, same shape, must not fold.
        let isnull_expr = c(0).call_unary(UnaryFunc::IsNull(mz_expr::func::IsNull));
        cases.push((
            isnull_expr.clone(),
            MirScalarExpr::literal_false(),
            vec![int_ct(false)],
        ));
        cases.push((isnull_expr.clone(), isnull_expr, vec![int_ct(true)]));

        // --- not_binary_negate (CRUX 2): every BinaryFunc pair negate()
        // returns Some for, crossed with three null-operand shapes, paired
        // with its dual so the expected rewrite target is explicit. Eq/NotEq
        // are propagates_nulls comparisons over ExcludeNull<Datum> inputs (so
        // is Lt/Gte/Lte/Gt), so a literal-null operand also engages
        // null_prop_binary/const_fold in both engines: both null-operand
        // shapes fold to a typed null regardless of which function is under
        // test, not to the dual comparison; that interaction is deliberate,
        // parity must hold whichever rule combination wins the race to the
        // fixpoint.
        let negation_pairs: Vec<(BinaryFunc, BinaryFunc)> = vec![
            (
                BinaryFunc::Eq(mz_expr::func::Eq),
                BinaryFunc::NotEq(mz_expr::func::NotEq),
            ),
            (
                BinaryFunc::NotEq(mz_expr::func::NotEq),
                BinaryFunc::Eq(mz_expr::func::Eq),
            ),
            (
                BinaryFunc::Lt(mz_expr::func::Lt),
                BinaryFunc::Gte(mz_expr::func::Gte),
            ),
            (
                BinaryFunc::Gte(mz_expr::func::Gte),
                BinaryFunc::Lt(mz_expr::func::Lt),
            ),
            (
                BinaryFunc::Lte(mz_expr::func::Lte),
                BinaryFunc::Gt(mz_expr::func::Gt),
            ),
            (
                BinaryFunc::Gt(mz_expr::func::Gt),
                BinaryFunc::Lte(mz_expr::func::Lte),
            ),
        ];
        for (func, dual) in &negation_pairs {
            let both_cols = not(c(0).call_binary(c(1), func.clone()));
            let both_cols_expected = c(0).call_binary(c(1), dual.clone());
            cases.push((
                both_cols,
                both_cols_expected,
                vec![int_ct(false), int_ct(false)],
            ));

            let one_null = not(c(0).call_binary(null_int(), func.clone()));
            cases.push((one_null, null_bool(), vec![int_ct(false)]));

            let both_null = not(null_int().call_binary(null_int(), func.clone()));
            cases.push((both_null, null_bool(), vec![]));
        }

        // Nested double-negation over a negatable comparison: not_not (slice
        // 1) and not_binary_negate must reach the same fixpoint regardless of
        // which fires first (Not(Not(Lt(a,b))) -> Not(Gte(a,b)) -> Lt(a,b),
        // or Not(Not(Lt(a,b))) -> Lt(a,b) directly).
        let nested_not_not = not(not(
            c(0).call_binary(c(1), BinaryFunc::Lt(mz_expr::func::Lt))
        ));
        cases.push((
            nested_not_not,
            c(0).call_binary(c(1), BinaryFunc::Lt(mz_expr::func::Lt)),
            vec![int_ct(false), int_ct(false)],
        ));

        // Not(f(a, <literal error>)): the error operand is itself folded from
        // `1 / 0` by const_fold, then err_prop_binary collapses the
        // comparison to that error before not_binary_negate's choice of
        // partner could matter. Proves the interaction does not panic or
        // union mismatched classes.
        let err_operand = int_lit(1).call_binary(int_lit(0), div64());
        let not_f_err = not(c(0).call_binary(err_operand, BinaryFunc::Eq(mz_expr::func::Eq)));
        cases.push((
            not_f_err,
            err_lit(EvalError::DivisionByZero, ReprScalarType::Bool, false),
            vec![int_ct(false)],
        ));

        // --- Interaction: if_err_cond combined with slice-4 const_fold and
        // slice-3 if_true on the same If. The inner If's literal-true
        // condition resolves via if_true to `1 / 0`, which const_fold then
        // folds to an error literal; that error literal becomes the OUTER
        // If's condition, so if_err_cond fires on the outer If.
        let inner_if = if_expr(
            MirScalarExpr::literal_true(),
            int_lit(1).call_binary(int_lit(0), div64()),
            int_lit(2),
        );
        let if_interaction = if_expr(inner_if, c(0), c(1));
        cases.push((
            if_interaction,
            err_lit(EvalError::DivisionByZero, ReprScalarType::Int64, false),
            vec![int_ct(false), int_ct(false)],
        ));

        // --- Interaction: not_binary_negate under a slice-1 not_not.
        let not_not_negate = not(not(
            c(0).call_binary(c(1), BinaryFunc::Eq(mz_expr::func::Eq))
        ));
        cases.push((
            not_not_negate,
            c(0).call_binary(c(1), BinaryFunc::Eq(mz_expr::func::Eq)),
            vec![int_ct(false), int_ct(false)],
        ));

        for (e, expected, ct) in cases {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "parity failed for {e:?} with col_types {ct:?}"
            );
        }

        // Regression sampling of slices 1-4 under the grown rule set.
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let regression: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (
                not(not(c(0))),
                c(0),
                vec![ReprScalarType::Bool.nullable(false)],
            ),
            (
                and(vec![c(0)]),
                c(0),
                vec![ReprScalarType::Bool.nullable(false)],
            ),
            (
                if_expr(c(0), c(1), c(1)),
                c(1),
                vec![
                    ReprScalarType::Bool.nullable(false),
                    ReprScalarType::Bool.nullable(false),
                ],
            ),
        ];
        for (e, expected, ct) in regression {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "regression parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice5() {
        assert!(
            CORPUS.contains("is null"),
            "corpus must exercise isnull_fold"
        );
        assert!(
            CORPUS.contains("not(#0 = #1)"),
            "corpus must exercise a not_binary_negate negation pair"
        );
        assert!(
            CORPUS.contains("if(1 / 0"),
            "corpus must exercise if_err_cond"
        );
        assert!(
            CORPUS.contains("null + #0"),
            "corpus must exercise null_prop_binary"
        );
        assert!(
            CORPUS.contains("(1 / 0) + #0"),
            "corpus must exercise err_prop_binary"
        );
    }

    // Termination (SP2b Slice 5): `Not(f) -> neg(f)` must reach a fixpoint,
    // not ping-pong between the comparison and its negation. Lowers
    // `Not(Lt(#0, #1))` directly, bypassing `canonicalize_combined`, so the
    // iteration count is visible, mirroring the slice-4
    // and_empty/and_single termination check.
    #[mz_ore::test]
    fn not_binary_negate_terminates() {
        use mz_expr::{BinaryFunc, MirScalarExpr, UnaryFunc};
        use mz_repr::ReprScalarType;

        let c = MirScalarExpr::column;
        let not = |e: MirScalarExpr| e.call_unary(UnaryFunc::Not(mz_expr::func::Not));
        let e = not(c(0).call_binary(c(1), BinaryFunc::Lt(mz_expr::func::Lt)));

        let mut eg = EGraph::new();
        eg.data_mut().scalar.col_types = vec![
            ReprScalarType::Int64.nullable(false),
            ReprScalarType::Int64.nullable(false),
        ];
        let _root = crate::eqsat::scalar::lower::lower_into(&mut eg, &e);
        let iters = saturate(&mut eg);
        assert!(
            iters <= 10,
            "Not(f) -> neg(f) must reach a fixpoint quickly; got {iters} iters"
        );
    }

    // Differential parity harness (SP2b Slice 6a): extends slices 1-5 to the
    // declarative `and_short_circuit`/`or_short_circuit` DSL rules
    // (`scalar.rewrite`), which mirror the imperative `and_or_short_circuit`
    // (`scalar/rules.rs`, line 245): a variadic AND/OR collapses to the
    // connective's zero (`false` for And, `true` for Or) as soon as any
    // operand's literal analysis equals that zero.
    //
    // The rule carries NO could_error guard, by design: `And`/`Or` short-
    // circuit unconditionally in eval (`func/variadic.rs`), returning the
    // zero as soon as it is seen and discarding any error accumulated from
    // an earlier operand. The "E-err envelope" cases below are the
    // correctness check for that design, not a guard to mutation-test: they
    // assert the combined engine's unconditional fold agrees with the old
    // engine on an erroring operand in BOTH operand orders (error-before-zero
    // and zero-before-error), which is exactly the early-return-discards-err
    // semantics the rule relies on.
    //
    // Same corpus-shaping constraint as slices 1-5: every input keeps the old
    // engine's still-unported rules (`null_prop_variadic`, `err_prop_variadic`,
    // `and_or_drop_unit`, `and_or_dedup`, `flatten_assoc`, `factor_and_or`,
    // `absorb_and_or`) from seizing on anything a real slice-6a case wouldn't
    // also trigger. `null_prop_variadic`/`err_prop_variadic` are moot here
    // regardless of shape (`And`/`Or::propagates_nulls()` is `false`, so
    // those rules never match a variadic And/Or node at all). The
    // `and(false, false)` duplicate-operand case is deliberately included
    // despite `and_or_dedup` being unported: dedup would collapse it to
    // `and(false)` in the old engine, which `and_single` then folds to
    // `false`, the same answer `and_short_circuit` reaches directly in the
    // new engine, so the extra old-engine path is a convergent no-op, not a
    // divergence.
    #[mz_ore::test]
    fn scalar_parity_slice6a() {
        use mz_expr::{MirScalarExpr, VariadicFunc};
        use mz_repr::{Datum, ReprScalarType};

        let c = MirScalarExpr::column;
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let not = |e: MirScalarExpr| e.call_unary(mz_expr::UnaryFunc::Not(mz_expr::func::Not));
        let int_lit = |v: i64| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let div64 = || mz_expr::BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        let null_bool = || MirScalarExpr::literal_null(ReprScalarType::Bool);
        let bool_ct = || ReprScalarType::Bool.nullable(false);

        let mut cases: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = Vec::new();
        let f = MirScalarExpr::literal_false;
        let t = MirScalarExpr::literal_true;

        // --- Positions: leading / middle / trailing false operand, all
        // collapse to false. Dual for Or/true.
        cases.push((and(vec![f(), c(0), c(1)]), f(), vec![bool_ct(), bool_ct()]));
        cases.push((and(vec![c(0), f(), c(1)]), f(), vec![bool_ct(), bool_ct()]));
        cases.push((and(vec![c(0), c(1), f()]), f(), vec![bool_ct(), bool_ct()]));
        cases.push((or(vec![t(), c(0), c(1)]), t(), vec![bool_ct(), bool_ct()]));
        cases.push((or(vec![c(0), t(), c(1)]), t(), vec![bool_ct(), bool_ct()]));
        cases.push((or(vec![c(0), c(1), t()]), t(), vec![bool_ct(), bool_ct()]));

        // --- Nulls (3VL): false/true dominates a null operand regardless of
        // position (the zero wins over the unit's absorbing-null behavior).
        cases.push((and(vec![c(0), f(), null_bool()]), f(), vec![bool_ct()]));
        cases.push((or(vec![c(0), t(), null_bool()]), t(), vec![bool_ct()]));

        // --- E-err envelope (the crux): a literal-error operand (`1 / 0`,
        // folded by the already-ported const_fold) must not block the fold,
        // in EITHER operand order.
        let err = || int_lit(1).call_binary(int_lit(0), div64());
        cases.push((and(vec![f(), err()]), f(), vec![]));
        cases.push((and(vec![err(), f()]), f(), vec![]));
        cases.push((or(vec![t(), err()]), t(), vec![]));
        cases.push((or(vec![err(), t()]), t(), vec![]));

        // --- Interactions: single-operand (and_single vs. short_circuit),
        // empty (and_empty; short_circuit does NOT fire, no zero operand),
        // and duplicate-false (and_or_dedup, unported, must not diverge).
        cases.push((and(vec![f()]), f(), vec![]));
        cases.push((and(vec![]), t(), vec![]));
        cases.push((and(vec![f(), f()]), f(), vec![]));

        for (e, expected, ct) in cases {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "parity failed for {e:?} with col_types {ct:?}"
            );
        }

        // Regression sampling of slice-1..5 shapes under the grown rule set.
        let regression: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (not(not(c(0))), c(0), vec![bool_ct()]),
            (and(vec![c(0)]), c(0), vec![bool_ct()]),
            (
                MirScalarExpr::If {
                    cond: Box::new(c(0)),
                    then: Box::new(c(1)),
                    els: Box::new(c(1)),
                },
                c(1),
                vec![bool_ct(), bool_ct()],
            ),
        ];
        for (e, expected, ct) in regression {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "regression parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice6a() {
        assert!(
            CORPUS.contains("and(false,"),
            "corpus must exercise and_short_circuit with a false operand"
        );
        assert!(
            CORPUS.contains("or(true,"),
            "corpus must exercise or_short_circuit with a true operand"
        );
        assert!(
            CORPUS.contains("and(false, 1 / 0)") && CORPUS.contains("and(1 / 0, false)"),
            "corpus must exercise the error-operand short-circuit envelope in both operand orders"
        );
    }

    // Termination (SP2b Slice 6a): `And(.., false, ..) -> false` must reach a
    // fixpoint, not ping-pong with `and_single`/`and_empty`. `false` is a
    // literal, so it is not itself re-foldable by any ported rule. Lowers
    // `And([false, #0])` directly, bypassing `canonicalize_combined`, so the
    // iteration count is visible, mirroring the slice-4/5 termination checks.
    #[mz_ore::test]
    fn and_short_circuit_terminates() {
        use mz_expr::{MirScalarExpr, VariadicFunc};
        use mz_repr::ReprScalarType;

        let c = MirScalarExpr::column;
        let e = MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: vec![MirScalarExpr::literal_false(), c(0)],
        };

        let mut eg = EGraph::new();
        eg.data_mut().scalar.col_types = vec![ReprScalarType::Bool.nullable(false)];
        let _root = crate::eqsat::scalar::lower::lower_into(&mut eg, &e);
        let iters = saturate(&mut eg);
        assert!(
            iters <= 10,
            "And(false, ..) -> false must reach a fixpoint quickly; got {iters} iters"
        );
    }

    // Differential parity harness (SP2b Slice 6c): extends slices 1-6a to the
    // declarative `and_drop_unit`/`or_drop_unit` and `and_dedup`/`or_dedup` DSL
    // rules (`scalar.rewrite`), which mirror the imperative
    // `and_or_drop_unit`/`and_or_dedup` (`scalar/rules.rs`, lines 270 and 181):
    // a variadic AND/OR drops operands equal to the connective's unit (`true`
    // for And, `false` for Or), and collapses operands that share a canonical
    // e-class id to their first occurrence.
    //
    // Both rules are UNCONDITIONAL (no could_error gate), by design: dropping a
    // unit operand or a syntactic duplicate never changes the AND/OR's value or
    // error behavior (the e-graph already canonicalizes children to class ids,
    // so a duplicate is provably the same value including its error behavior).
    // The error cases below are the correctness check for that design: a
    // duplicate error operand dedups to the single error, and a unit operand
    // next to an error drops the unit while the error survives (through
    // and_single, since dropping the sole remaining unit leaves an arity-1 AND).
    //
    // The null cases are the 3VL correctness check for drop_unit specifically:
    // a null operand's `literal` analysis is `Some(None)` (a literal, but not
    // the bool `Some(Some(unit))`), so it must never be dropped, only the true
    // (or false, for Or) operand is.
    //
    // Same corpus-shaping constraint as slices 1-6a: every input keeps the old
    // engine's still-unported rules (`null_prop_variadic`, `err_prop_variadic`,
    // `flatten_assoc`, `factor_and_or`, `absorb_and_or`) from seizing on
    // anything a real slice-6c case wouldn't also trigger. As in slice 6a,
    // `null_prop_variadic`/`err_prop_variadic` are moot regardless of shape
    // (`And`/`Or::propagates_nulls()` is `false`).
    #[mz_ore::test]
    fn scalar_parity_slice6c() {
        use mz_expr::{EvalError, MirScalarExpr, VariadicFunc};
        use mz_repr::{Datum, ReprScalarType};

        let c = MirScalarExpr::column;
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let not = |e: MirScalarExpr| e.call_unary(mz_expr::UnaryFunc::Not(mz_expr::func::Not));
        let int_lit = |v: i64| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let div64 = || mz_expr::BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        let null_bool = || MirScalarExpr::literal_null(ReprScalarType::Bool);
        let bool_ct = || ReprScalarType::Bool.nullable(false);
        let bool_err = || {
            MirScalarExpr::Literal(
                Err(EvalError::DivisionByZero),
                ReprColumnType {
                    scalar_type: ReprScalarType::Bool,
                    nullable: false,
                },
            )
        };

        let mut cases: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = Vec::new();

        // --- drop_unit positions: leading / middle / trailing unit operand.
        cases.push((
            and(vec![MirScalarExpr::literal_true(), c(0), c(1)]),
            and(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            and(vec![c(0), MirScalarExpr::literal_true(), c(1)]),
            and(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            and(vec![c(0), c(1), MirScalarExpr::literal_true()]),
            and(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            or(vec![MirScalarExpr::literal_false(), c(0), c(1)]),
            or(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            or(vec![c(0), MirScalarExpr::literal_false(), c(1)]),
            or(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            or(vec![c(0), c(1), MirScalarExpr::literal_false()]),
            or(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));

        // --- dedup: adjacent, non-adjacent, and more than two copies.
        cases.push((
            and(vec![c(0), c(0), c(1)]),
            and(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            and(vec![c(0), c(1), c(0)]),
            and(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            and(vec![c(0), c(0), c(0), c(1)]),
            and(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            or(vec![c(0), c(0), c(1)]),
            or(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            or(vec![c(0), c(1), c(0)]),
            or(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));
        cases.push((
            or(vec![c(0), c(0), c(0), c(1)]),
            or(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct()],
        ));

        // --- E-err envelope: a literal-error operand dedups with its
        // duplicate to the single error, and survives drop_unit dropping an
        // adjacent unit operand. The error is boolean-typed (`(1/0) = (1/0)`,
        // both sides folded by const_fold, then Eq folds the erroring
        // comparison), NOT the bare Int64 `1 / 0`: and_dedup/and_single
        // eventually expose this operand's OWN class as the AND/OR's value
        // (dedup to arity 1, then and_single/or_single unwraps it), which
        // unions it with the connective's own const_fold result. Both must
        // agree on the literal's `ReprColumnType`, not just its `EvalError`
        // payload, or the shared scalar analysis's merge-conflict assertion
        // fires; an Int64-typed error exposed through a Bool AND/OR would be
        // exactly such a conflict, an artifact of malformed (not boolean-typed)
        // input rather than a real and_dedup/and_drop_unit divergence.
        let err = || {
            let d = int_lit(1).call_binary(int_lit(0), div64());
            d.clone()
                .call_binary(d, mz_expr::BinaryFunc::Eq(mz_expr::func::Eq))
        };
        cases.push((and(vec![err(), err()]), bool_err(), vec![]));
        cases.push((
            and(vec![err(), MirScalarExpr::literal_true()]),
            bool_err(),
            vec![],
        ));
        cases.push((or(vec![err(), err()]), bool_err(), vec![]));
        cases.push((
            or(vec![err(), MirScalarExpr::literal_false()]),
            bool_err(),
            vec![],
        ));

        // --- Nulls (3VL): a null operand is not the unit, so drop_unit must
        // drop only the true/false operand and leave the null in place.
        cases.push((
            and(vec![c(0), MirScalarExpr::literal_true(), null_bool()]),
            and(vec![c(0), null_bool()]),
            vec![bool_ct()],
        ));
        cases.push((
            or(vec![c(0), MirScalarExpr::literal_false(), null_bool()]),
            or(vec![c(0), null_bool()]),
            vec![bool_ct()],
        ));

        // --- Interaction cascades: drop_unit / dedup feeding and_single /
        // and_empty on a later saturation round.
        // And(#0, true) -> And(#0) (drop_unit) -> #0 (and_single).
        cases.push((
            and(vec![c(0), MirScalarExpr::literal_true()]),
            c(0),
            vec![bool_ct()],
        ));
        // And(true) -> And() (drop_unit) -> true (and_empty).
        cases.push((
            and(vec![MirScalarExpr::literal_true()]),
            MirScalarExpr::literal_true(),
            vec![],
        ));
        // And(#0, #0) -> And(#0) (and_dedup) -> #0 (and_single).
        cases.push((and(vec![c(0), c(0)]), c(0), vec![bool_ct()]));

        for (e, expected, ct) in cases {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "parity failed for {e:?} with col_types {ct:?}"
            );
        }

        // Regression sampling of slice-1..6a shapes under the grown rule set.
        let regression: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (not(not(c(0))), c(0), vec![bool_ct()]),
            (and(vec![c(0)]), c(0), vec![bool_ct()]),
            (
                and(vec![MirScalarExpr::literal_false(), c(0), c(1)]),
                MirScalarExpr::literal_false(),
                vec![bool_ct(), bool_ct()],
            ),
            (
                MirScalarExpr::If {
                    cond: Box::new(c(0)),
                    then: Box::new(c(1)),
                    els: Box::new(c(1)),
                },
                c(1),
                vec![bool_ct(), bool_ct()],
            ),
        ];
        for (e, expected, ct) in regression {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "regression parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice6c() {
        assert!(
            CORPUS.contains("and(true,"),
            "corpus must exercise and_drop_unit with a leading true operand"
        );
        assert!(
            CORPUS.contains("or(false,"),
            "corpus must exercise or_drop_unit with a leading false operand"
        );
        assert!(
            CORPUS.contains("and(#0, #0,"),
            "corpus must exercise and_dedup"
        );
        assert!(
            CORPUS.contains("or(#0, #0,"),
            "corpus must exercise or_dedup"
        );
        assert!(
            CORPUS.contains("and(1 / 0 = 1 / 0, 1 / 0 = 1 / 0)"),
            "corpus must exercise and_dedup on a duplicate error operand"
        );
        assert!(
            CORPUS.contains("and(1 / 0 = 1 / 0, true)"),
            "corpus must exercise and_drop_unit preserving an adjacent error operand"
        );
        assert!(
            CORPUS.contains("and(#0, true, null)"),
            "corpus must exercise drop_unit NOT dropping a null operand"
        );
        assert!(
            CORPUS.contains("or(#0, false, null)"),
            "corpus must exercise the dual drop_unit-vs-null case for Or"
        );
    }

    // Termination (SP2b Slice 6c): a term with BOTH a unit literal and a
    // duplicate operand must reach a fixpoint quickly, whichever of
    // and_drop_unit / and_dedup fires first. Each fire strictly shrinks the
    // operand list (the fire-guards `scalar_any_lit_true`/`has_duplicate_id`
    // make a no-op fire impossible), so the two rules cannot ping-pong: they
    // only ever converge toward the single remaining operand. Lowers
    // `And([x, x, true])` directly, bypassing `canonicalize_combined`, so the
    // iteration count is visible, mirroring the slice-4/5/6a termination checks.
    #[mz_ore::test]
    fn drop_unit_dedup_terminates() {
        use mz_expr::{MirScalarExpr, VariadicFunc};
        use mz_repr::ReprScalarType;

        let c = MirScalarExpr::column;
        let e = MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: vec![c(0), c(0), MirScalarExpr::literal_true()],
        };

        let mut eg = EGraph::new();
        eg.data_mut().scalar.col_types = vec![ReprScalarType::Bool.nullable(false)];
        let _root = crate::eqsat::scalar::lower::lower_into(&mut eg, &e);
        let iters = saturate(&mut eg);
        assert!(
            iters <= 10,
            "And(x, x, true) must reach a fixpoint quickly regardless of fire order; got {iters} iters"
        );
    }

    // Differential parity harness (SP2b Slice 6e): extends slices 1-6c to the
    // declarative `absorb_and`/`absorb_or` DSL rules (`scalar.rewrite`), which
    // mirror the imperative `absorb_and_or` (`scalar/rules.rs`, line 601): an
    // outer AND/OR operand whose dual-connective inner set is a proper
    // superset of another operand's inner set is redundant and is dropped,
    // provided every dropped extra has `could_error == false`. The retained
    // subsuming operand is deliberately NOT gated (its own error still
    // surfaces after absorption); only the DROPPED extras are.
    //
    // Same corpus-shaping constraint as slices 1-6c: every input keeps the
    // old engine's still-unported rules (`null_prop_variadic`,
    // `err_prop_variadic`, `flatten_assoc`, `factor_and_or`) from seizing on
    // anything a real slice-6e case wouldn't also trigger.
    //
    // The guard case (the correctness core of this rule) has its own
    // dedicated test, `absorb_guard_blocks_dropped_extra_error`, so the
    // mutation-test target is a single, isolated assertion.
    #[mz_ore::test]
    fn scalar_parity_slice6e() {
        use mz_expr::{MirScalarExpr, VariadicFunc};
        use mz_repr::ReprScalarType;

        let c = MirScalarExpr::column;
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let not = |e: MirScalarExpr| e.call_unary(mz_expr::UnaryFunc::Not(mz_expr::func::Not));
        let bool_ct = || ReprScalarType::Bool.nullable(false);

        let mut cases: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = Vec::new();

        // --- absorb_and: a (#0) inside the inner Or at leading / middle /
        // trailing position. All three absorb to a bare `#0`.
        cases.push((
            and(vec![c(0), or(vec![c(0), c(1), c(2)])]),
            c(0),
            vec![bool_ct(), bool_ct(), bool_ct()],
        ));
        cases.push((
            and(vec![c(0), or(vec![c(1), c(0), c(2)])]),
            c(0),
            vec![bool_ct(), bool_ct(), bool_ct()],
        ));
        cases.push((
            and(vec![c(0), or(vec![c(1), c(2), c(0)])]),
            c(0),
            vec![bool_ct(), bool_ct(), bool_ct()],
        ));

        // --- absorb_or (dual): a inside the inner And at leading / middle /
        // trailing position.
        cases.push((
            or(vec![c(0), and(vec![c(0), c(1), c(2)])]),
            c(0),
            vec![bool_ct(), bool_ct(), bool_ct()],
        ));
        cases.push((
            or(vec![c(0), and(vec![c(1), c(0), c(2)])]),
            c(0),
            vec![bool_ct(), bool_ct(), bool_ct()],
        ));
        cases.push((
            or(vec![c(0), and(vec![c(1), c(2), c(0)])]),
            c(0),
            vec![bool_ct(), bool_ct(), bool_ct()],
        ));

        // --- Extra operands: absorption drops only the subsumed operand; an
        // unrelated outer sibling `c` survives untouched.
        // And(a, Or(a, b), c) -> And(a, c).
        cases.push((
            and(vec![c(0), or(vec![c(0), c(1)]), c(2)]),
            and(vec![c(0), c(2)]),
            vec![bool_ct(), bool_ct(), bool_ct()],
        ));

        // --- General subset, beyond the simple "operand present" framing:
        // neither And operand IS `a`; one's inner-set {a, b} is a proper
        // subset of the other's {a, b, c}. AND(a,b) v AND(a,b,c) -> AND(a,b).
        cases.push((
            or(vec![and(vec![c(0), c(1)]), and(vec![c(0), c(1), c(2)])]),
            and(vec![c(0), c(1)]),
            vec![bool_ct(), bool_ct(), bool_ct()],
        ));

        // --- Nested absorption: the inner And(a, Or(a, b)) absorbs to `a` on
        // one saturation round; the outer Or(a, a) then dedups to `a` too.
        cases.push((
            or(vec![c(0), and(vec![c(0), or(vec![c(0), c(1)])])]),
            c(0),
            vec![bool_ct(), bool_ct()],
        ));

        // --- Interaction: absorb feeding and_drop_unit. The leading `true`
        // drops first (and_drop_unit), exposing And(a, Or(a, b)), which then
        // absorbs to `a` (the remaining arity-1 And then collapses via
        // and_single).
        cases.push((
            and(vec![
                MirScalarExpr::literal_true(),
                c(0),
                or(vec![c(0), c(1)]),
            ]),
            c(0),
            vec![bool_ct(), bool_ct()],
        ));

        for (e, expected, ct) in cases {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "parity failed for {e:?} with col_types {ct:?}"
            );
        }

        // Regression sampling of slice-1..6c shapes under the grown rule set.
        let regression: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (not(not(c(0))), c(0), vec![bool_ct()]),
            (and(vec![c(0)]), c(0), vec![bool_ct()]),
            (
                and(vec![c(0), MirScalarExpr::literal_true(), c(1)]),
                and(vec![c(0), c(1)]),
                vec![bool_ct(), bool_ct()],
            ),
            (and(vec![c(0), c(0)]), c(0), vec![bool_ct()]),
        ];
        for (e, expected, ct) in regression {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "regression parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice6e() {
        assert!(
            CORPUS.contains("and(#0, or(#0, #1, #2))"),
            "corpus must exercise absorb_and with a leading in the inner Or"
        );
        assert!(
            CORPUS.contains("and(#0, or(#1, #0, #2))"),
            "corpus must exercise absorb_and with a in the middle of the inner Or"
        );
        assert!(
            CORPUS.contains("and(#0, or(#1, #2, #0))"),
            "corpus must exercise absorb_and with a trailing in the inner Or"
        );
        assert!(
            CORPUS.contains("or(#0, and(#0, #1, #2))"),
            "corpus must exercise absorb_or (dual)"
        );
        assert!(
            CORPUS.contains("and(#0, or(#0, #1), #2)"),
            "corpus must exercise absorption alongside an unrelated outer operand"
        );
        assert!(
            CORPUS.contains("or(and(#0, #1), and(#0, #1, #2))"),
            "corpus must exercise the general-subset case beyond simple operand presence"
        );
        assert!(
            CORPUS.contains("or(#0, and(#0, or(#0, #1)))"),
            "corpus must exercise nested absorption"
        );
        assert!(
            CORPUS.contains("and(true, #0, or(#0, #1))"),
            "corpus must exercise absorb feeding and_drop_unit"
        );
        assert!(
            CORPUS.contains("and(#0, or(#0, 1 / 0 = 1 / 0))"),
            "corpus must exercise the could_error guard blocking a dropped erroring extra"
        );
    }

    // GUARD (SP2b Slice 6e, the correctness core): the could_error gate on
    // ABSORBED extras (`rest_filters::absorb_drop_index`) must block
    // absorption when a dropped extra could error, even though the outer form
    // is otherwise ripe for the rewrite. This is the single test the
    // mutation-test evidence in the Task-5 report is built on: disabling the
    // gate (forcing `extras_can_error = false` in `absorb_drop_index`) makes
    // this assertion fail, and only this one, proving the gate is
    // load-bearing.
    //
    // `err` is boolean-typed (`(1 / 0) = (1 / 0)`), not a bare Int64 `1 / 0`:
    // the slice-6c type trap (`scalar/analysis.rs::merge`'s
    // conflicting-literal debug_assert, tripped when a variadic collapse
    // exposes an operand's own class directly as the connective's value)
    // applies here too, since a fully-permitted sibling absorption elsewhere
    // in this corpus does expose a bare column's class this way. `a` is
    // nullable so the unsoundness the gate prevents is a live counterexample:
    // at a = null, err = Err, the correct result is Err (null and err = err),
    // but the wrongly-absorbed `a` alone would be null.
    #[mz_ore::test]
    fn absorb_guard_blocks_dropped_extra_error() {
        use mz_expr::{BinaryFunc, EvalError, MirScalarExpr, VariadicFunc};
        use mz_repr::{Datum, ReprScalarType};

        let c = MirScalarExpr::column;
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let int_lit = |v: i64| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let div64 = || BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        let err = || {
            let d = int_lit(1).call_binary(int_lit(0), div64());
            d.clone().call_binary(d, BinaryFunc::Eq(mz_expr::func::Eq))
        };
        let err_lit = || {
            MirScalarExpr::Literal(
                Err(EvalError::DivisionByZero),
                ReprColumnType {
                    scalar_type: ReprScalarType::Bool,
                    nullable: false,
                },
            )
        };
        let ct = vec![ReprScalarType::Bool.nullable(true)];

        let guard_case = and(vec![c(0), or(vec![c(0), err()])]);
        // The guard blocks absorption, so the erroring extra survives in
        // place: only its own class is const-folded to the error literal.
        let expected = and(vec![c(0), or(vec![c(0), err_lit()])]);

        let new = canonicalize_combined(&guard_case, &ct);
        assert_eq!(
            new, expected,
            "guard-case parity failed for {guard_case:?}: combined={new:?} expected={expected:?}"
        );
        assert_ne!(
            new,
            c(0),
            "guard must block absorption of the erroring extra; got the unsound {new:?}"
        );
    }

    // Termination (SP2b Slice 6e): absorption must reach a fixpoint quickly.
    // Each fire strictly shrinks the outer operand list (drops `Q`), and the
    // could_error guard only ever suppresses a fire, never turns a blocked
    // case into a churning one, so there is nothing to ping-pong against.
    // Lowers `And([a, Or(a, b)])` directly, bypassing `canonicalize_combined`,
    // so the iteration count is visible, mirroring the slice-4/5/6a/6c
    // termination checks.
    #[mz_ore::test]
    fn absorb_terminates() {
        use mz_expr::{MirScalarExpr, VariadicFunc};
        use mz_repr::ReprScalarType;

        let c = MirScalarExpr::column;
        let or_ab = MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: vec![c(0), c(1)],
        };
        let e = MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: vec![c(0), or_ab],
        };

        let mut eg = EGraph::new();
        eg.data_mut().scalar.col_types = vec![
            ReprScalarType::Bool.nullable(false),
            ReprScalarType::Bool.nullable(false),
        ];
        let _root = crate::eqsat::scalar::lower::lower_into(&mut eg, &e);
        let iters = saturate(&mut eg);
        assert!(
            iters <= 10,
            "And(a, Or(a, b)) must reach a fixpoint quickly; got {iters} iters"
        );
    }

    // Differential parity harness (SP2b Slice 6b): the variadic null/error
    // propagation rules (`null_prop_variadic`, `err_prop_variadic`). Both gate
    // on `func.propagates_nulls()`, which is FALSE for `And`/`Or`, so the rules
    // never fire on the boolean connectives. Their real domain is a
    // null-propagating variadic like `MakeTimestamp`, so the positive cases use
    // it (mirroring `scalar/rules.rs::test_null_prop_variadic_*`). The And/Or
    // cases are negative controls: neither engine 6b-folds them, they route
    // through short-circuit / drop_unit / single, and parity must still hold.
    //
    // Error operands are kept well-typed for their position. `MakeTimestamp`
    // operands are Int64/Float64, so a bare `1 / 0` (Int64) is a valid year
    // operand. The And/Or controls need a Bool-typed error, built with the
    // `(1 / 0) = (1 / 0)` idiom, for the same type-agreement reason slice 6c's
    // E-err cases require it (a variadic collapse can expose an operand's own
    // class as the connective's value, and the shared scalar analysis's
    // merge-conflict assertion fires on an Int64 error under a Bool connective).
    #[mz_ore::test]
    fn scalar_parity_slice6b() {
        use mz_expr::{BinaryFunc, EvalError, MirScalarExpr, VariadicFunc};
        use mz_repr::{Datum, ReprScalarType};

        let c = MirScalarExpr::column;
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let makets = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::MakeTimestamp(mz_expr::func::variadic::MakeTimestamp),
            exprs: es,
        };
        let int_lit = |v: i64| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let f64_lit = |v: f64| {
            MirScalarExpr::literal_ok(
                Datum::Float64(ordered_float::OrderedFloat(v)),
                ReprScalarType::Float64,
            )
        };
        let null_int = || MirScalarExpr::literal_null(ReprScalarType::Int64);
        let null_f64 = || MirScalarExpr::literal_null(ReprScalarType::Float64);
        let div64 = || BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        // A bare Int64 DivisionByZero error literal: valid for a MakeTimestamp
        // Int64 operand.
        let int_err = || int_lit(1).call_binary(int_lit(0), div64());
        // A nullable Int64 column (`c0`) for the c0-based positions.
        let int_ct = || vec![ReprScalarType::Int64.nullable(true)];

        // --- null_prop fires: a literal null in the leading / middle /
        // trailing operand, with every other operand error-free, folds the
        // whole call to a typed null.
        for (case, ct) in [
            // null year (leading), c0 in the month position.
            (
                makets(vec![
                    null_int(),
                    c(0),
                    int_lit(1),
                    int_lit(0),
                    int_lit(0),
                    f64_lit(0.0),
                ]),
                int_ct(),
            ),
            // null month (middle), c0 in the hour position.
            (
                makets(vec![
                    int_lit(2024),
                    null_int(),
                    int_lit(1),
                    c(0),
                    int_lit(0),
                    f64_lit(0.0),
                ]),
                int_ct(),
            ),
            // null second (trailing, the Float64 operand), c0 in the month
            // position.
            (
                makets(vec![
                    int_lit(2024),
                    c(0),
                    int_lit(1),
                    int_lit(0),
                    int_lit(0),
                    null_f64(),
                ]),
                int_ct(),
            ),
        ] {
            let expected = MirScalarExpr::literal_null(ReprScalarType::Timestamp);
            let new = canonicalize_combined(&case, &ct);
            assert_eq!(new, expected, "null_prop parity failed for {case:?}");
            assert!(
                matches!(
                    new,
                    MirScalarExpr::Literal(Ok(ref row), _) if row.unpack_first() == Datum::Null
                ),
                "null_prop must fold to a typed null literal, got {new:?}"
            );
        }

        // --- err_prop fires: a literal Int64 error (`1 / 0`) in the year
        // operand, with safe others, folds the call to that error literal.
        {
            let case = makets(vec![
                int_err(),
                c(0),
                int_lit(1),
                int_lit(0),
                int_lit(0),
                f64_lit(0.0),
            ]);
            let ct = int_ct();
            let expected = MirScalarExpr::Literal(
                Err(EvalError::DivisionByZero),
                ReprColumnType {
                    scalar_type: ReprScalarType::Timestamp,
                    nullable: false,
                },
            );
            let new = canonicalize_combined(&case, &ct);
            assert_eq!(new, expected, "err_prop parity failed for {case:?}");
            assert!(
                matches!(new, MirScalarExpr::Literal(Err(_), _)),
                "err_prop must fold to an error literal, got {new:?}"
            );
        }

        // --- null-vs-error priority (the envelope crux): both a literal null
        // (year) and a literal error (`1 / 0`, month). null_prop is BLOCKED
        // because the error operand can error, so err_prop wins: the result is
        // the error, NOT null. eval agrees: `eval(makets(null, 1/0, ..))` is
        // Err (an operand error surfaces over the propagated null).
        {
            let case = makets(vec![
                null_int(),
                int_err(),
                int_lit(1),
                int_lit(0),
                int_lit(0),
                f64_lit(0.0),
            ]);
            let ct = int_ct();
            let expected = MirScalarExpr::Literal(
                Err(EvalError::DivisionByZero),
                ReprColumnType {
                    scalar_type: ReprScalarType::Timestamp,
                    nullable: false,
                },
            );
            let new = canonicalize_combined(&case, &ct);
            assert_eq!(new, expected, "null-vs-error parity failed for {case:?}");
            assert!(
                matches!(new, MirScalarExpr::Literal(Err(_), _)),
                "null-vs-error priority must yield the error, not null, got {new:?}"
            );
        }

        // --- Cases where neither 6b rule fires; the call is left intact and
        // both engines agree. Plain parity assertion only.
        let unchanged: Vec<(MirScalarExpr, Vec<ReprColumnType>)> = vec![
            // Blocked: `1 / c0` can error, so null_prop is gated off, and no
            // operand is a LITERAL error, so err_prop cannot fire either.
            (
                makets(vec![
                    null_int(),
                    int_lit(1).call_binary(c(0), div64()),
                    int_lit(1),
                    int_lit(0),
                    int_lit(0),
                    f64_lit(0.0),
                ]),
                int_ct(),
            ),
            // A non-null-propagating variadic (`Coalesce`) with a null and an
            // error operand: `propagates_nulls()` is false, so neither rule
            // considers it. Coalesce is the whole point of not propagating
            // nulls (it returns the first non-null), so it must survive.
            (
                MirScalarExpr::CallVariadic {
                    func: VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce),
                    exprs: vec![null_int(), c(0)],
                },
                int_ct(),
            ),
        ];
        for (case, ct) in unchanged {
            // Neither 6b rule may fire, so the call is frozen UNCHANGED.
            let expected = case.clone();
            let new = canonicalize_combined(&case, &ct);
            assert_eq!(new, expected, "unchanged-case parity failed for {case:?}");
            assert!(
                matches!(new, MirScalarExpr::CallVariadic { .. }),
                "neither 6b rule may fire here; call must survive, got {new:?}"
            );
        }

        // --- And/Or NEGATIVE CONTROLS: both propagate_nulls == false, so 6b
        // never touches them. They fold through short-circuit / drop_unit /
        // single instead, and parity must hold. `err_bool` is the Bool-typed
        // `(1 / 0) = (1 / 0)` error.
        let err_bool = || {
            let d = int_err();
            d.clone().call_binary(d, BinaryFunc::Eq(mz_expr::func::Eq))
        };
        let controls: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (
                and(vec![
                    MirScalarExpr::literal_null(ReprScalarType::Bool),
                    MirScalarExpr::literal_true(),
                ]),
                MirScalarExpr::literal_null(ReprScalarType::Bool),
                vec![],
            ),
            (
                or(vec![MirScalarExpr::literal_false(), err_bool()]),
                MirScalarExpr::Literal(
                    Err(EvalError::DivisionByZero),
                    ReprColumnType {
                        scalar_type: ReprScalarType::Bool,
                        nullable: false,
                    },
                ),
                vec![],
            ),
        ];
        for (case, expected, ct) in controls {
            let new = canonicalize_combined(&case, &ct);
            assert_eq!(
                new, expected,
                "And/Or negative-control parity failed for {case:?}"
            );
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice6b() {
        assert!(
            CORPUS.contains("makets(null, #0, 1, 0, 0, 0)"),
            "corpus must exercise null_prop_variadic firing on a leading null"
        );
        assert!(
            CORPUS.contains("makets(1 / 0, #0, 1, 0, 0, 0)"),
            "corpus must exercise err_prop_variadic firing on a literal error"
        );
        assert!(
            CORPUS.contains("makets(null, 1 / 0, 1, 0, 0, 0)"),
            "corpus must exercise the null-vs-error priority (error wins)"
        );
        assert!(
            CORPUS.contains("makets(null, 1 / #0, 1, 0, 0, 0)"),
            "corpus must exercise the could_error guard blocking null_prop"
        );
        assert!(
            CORPUS.contains("coalesce(null, #0)"),
            "corpus must exercise a non-null-propagating variadic left untouched"
        );
    }

    // GUARD (SP2b Slice 6b, the correctness core): the `other_can_error` gate
    // in `scalar_builtins::null_prop_variadic` must block null propagation when
    // another operand can error, so the error surfaces instead of a wrong null.
    // `makets(null, 1 / 0, ..)` is the live counterexample: eval yields Err
    // (the `1 / 0` operand errors), so the only sound fold is that error.
    // null_prop must stay blocked and err_prop must win.
    //
    // Mutation-test evidence: deleting the `if other_can_error { return None; }`
    // block in `null_prop_variadic` makes THIS test and the slice-6b
    // null-vs-error parity case fail (the combined engine wrongly folds to
    // null), and only those, proving the gate is load-bearing.
    #[mz_ore::test]
    fn null_prop_variadic_guard_blocks_erroring_operand() {
        use mz_expr::{BinaryFunc, EvalError, MirScalarExpr, VariadicFunc};
        use mz_repr::{Datum, ReprScalarType};

        let int_lit = |v: i64| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let f64_lit = |v: f64| {
            MirScalarExpr::literal_ok(
                Datum::Float64(ordered_float::OrderedFloat(v)),
                ReprScalarType::Float64,
            )
        };
        let div64 = || BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        let case = MirScalarExpr::CallVariadic {
            func: VariadicFunc::MakeTimestamp(mz_expr::func::variadic::MakeTimestamp),
            exprs: vec![
                MirScalarExpr::literal_null(ReprScalarType::Int64),
                int_lit(1).call_binary(int_lit(0), div64()),
                int_lit(1),
                int_lit(0),
                int_lit(0),
                f64_lit(0.0),
            ],
        };
        let ct = vec![ReprScalarType::Int64.nullable(true)];
        let expected = MirScalarExpr::Literal(
            Err(EvalError::DivisionByZero),
            ReprColumnType {
                scalar_type: ReprScalarType::Timestamp,
                nullable: false,
            },
        );

        let new = canonicalize_combined(&case, &ct);
        assert_eq!(
            new, expected,
            "guard-case parity failed for {case:?}: combined={new:?} expected={expected:?}"
        );
        assert!(
            matches!(new, MirScalarExpr::Literal(Err(_), _)),
            "guard must keep null_prop blocked so err_prop yields the error; got {new:?}"
        );
    }

    // Termination (SP2b Slice 6b): the null/error propagation rules must reach a
    // fixpoint quickly. Each fires at most once per class (it folds the call to
    // a single literal), and the `other_can_error` / literal-error gates only
    // ever suppress a fire, so there is nothing to ping-pong against. Lowers the
    // exprs directly, bypassing `canonicalize_combined`, so the iteration count
    // is visible, mirroring the slice-6c/6e termination checks.
    #[mz_ore::test]
    fn null_err_prop_variadic_terminates() {
        use mz_expr::{BinaryFunc, MirScalarExpr, VariadicFunc};
        use mz_repr::{Datum, ReprScalarType};

        let int_lit = |v: i64| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let f64_lit = |v: f64| {
            MirScalarExpr::literal_ok(
                Datum::Float64(ordered_float::OrderedFloat(v)),
                ReprScalarType::Float64,
            )
        };
        let div64 = || BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        let makets = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::MakeTimestamp(mz_expr::func::variadic::MakeTimestamp),
            exprs: es,
        };

        // A safe null (null_prop fires) and a null-vs-error mix (err_prop fires,
        // null_prop stays gated). Both must converge.
        let cases = vec![
            makets(vec![
                MirScalarExpr::literal_null(ReprScalarType::Int64),
                MirScalarExpr::column(0),
                int_lit(1),
                int_lit(0),
                int_lit(0),
                f64_lit(0.0),
            ]),
            makets(vec![
                MirScalarExpr::literal_null(ReprScalarType::Int64),
                int_lit(1).call_binary(int_lit(0), div64()),
                int_lit(1),
                int_lit(0),
                int_lit(0),
                f64_lit(0.0),
            ]),
        ];
        for case in cases {
            let mut eg = EGraph::new();
            eg.data_mut().scalar.col_types = vec![ReprScalarType::Int64.nullable(true)];
            let _root = crate::eqsat::scalar::lower::lower_into(&mut eg, &case);
            let iters = saturate(&mut eg);
            assert!(
                iters <= 10,
                "null/err_prop_variadic must reach a fixpoint quickly for {case:?}; got {iters} iters"
            );
        }
    }

    // Differential parity harness (SP2b Slice 6d): the associative-variadic
    // flattening rules (`flatten_{and,or,coalesce,greatest,least}`), one per
    // `is_associative` variadic, porting `scalar/rules.rs::flatten_assoc`. A
    // nested same-func operand is spliced up one level; saturation re-applies
    // for deeper nesting. The rule is UNCONDITIONAL (no `could_error` gate):
    // associativity is order-independent over each func's semilattice, error
    // handling included, so `f(a, f(b, c))` and `f(a, b, c)` evaluate
    // identically for all inputs.
    //
    // CRITICAL new-domain proof: flatten fires on ALL five associative
    // variadics, not just And/Or. The Coalesce/Greatest/Least cases below are
    // the beyond-boolean evidence, and the old-engine oracle flattens them too,
    // so parity must hold there.
    //
    // Same corpus-shaping discipline as slices 1-6e: every case must keep the
    // old engine's one still-unported rule (`factor_and_or`) from seizing on
    // anything a real flatten case wouldn't. `factor_and_or` needs two
    // dual-connective sub-calls (the opposite connective from the outer)
    // sharing a common factor, which none of these have.
    //
    // And/Or error operands are Bool-typed `(1 / 0) = (1 / 0)`, never a bare
    // Int64 `1 / 0`: flatten-then-collapse is exactly the slice-6c type trap.
    // A variadic collapse that exposes an operand's own class as the
    // connective's value trips `scalar/analysis.rs::merge`'s conflicting-literal
    // debug_assert on a bare Int64 under a Bool connective. Coalesce/Greatest/
    // Least operands are well-typed Int64 columns.
    #[mz_ore::test]
    fn scalar_parity_slice6d() {
        use mz_expr::{BinaryFunc, EvalError, MirScalarExpr, VariadicFunc};
        use mz_repr::{Datum, ReprScalarType};

        let c = MirScalarExpr::column;
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let coalesce = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Coalesce(mz_expr::func::variadic::Coalesce),
            exprs: es,
        };
        let greatest = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Greatest(mz_expr::func::variadic::Greatest),
            exprs: es,
        };
        let least = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Least(mz_expr::func::variadic::Least),
            exprs: es,
        };
        let bool_ct = || ReprScalarType::Bool.nullable(false);
        let int_ct = || ReprScalarType::Int64.nullable(false);
        let int_lit = |v: i64| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let div64 = || BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        // Bool-typed error `(1 / 0) = (1 / 0)` (see the header note on the type
        // trap). A bare Int64 `1 / 0` under a Bool connective would panic the
        // shared analysis's merge assertion once a collapse exposes its class.
        let err_bool = || {
            let d = int_lit(1).call_binary(int_lit(0), div64());
            d.clone().call_binary(d, BinaryFunc::Eq(mz_expr::func::Eq))
        };

        // The result of a pure flatten is a same-func variadic with `want_len`
        // operands, none of which is itself a nested same-func call.
        let assert_flat = |got: &MirScalarExpr, want_len: usize| {
            let MirScalarExpr::CallVariadic { func, exprs } = got else {
                panic!("expected a flat CallVariadic, got {got:?}");
            };
            assert_eq!(
                exprs.len(),
                want_len,
                "flat operand count mismatch in {got:?}"
            );
            let outer = std::mem::discriminant(func);
            for e in exprs {
                if let MirScalarExpr::CallVariadic { func: inner, .. } = e {
                    assert_ne!(
                        std::mem::discriminant(inner),
                        outer,
                        "flat result must not contain a nested same-func operand: {got:?}"
                    );
                }
            }
        };

        // --- Pure-flatten And/Or with a nested same-op operand at leading /
        // middle / trailing position. Four distinct bool columns never dedup or
        // collapse, so each lands as a flat 4-ary call.
        let flat_and = and(vec![c(0), c(1), c(2), c(3)]);
        let flat_or = or(vec![c(0), c(1), c(2), c(3)]);
        let flat_bool: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (
                and(vec![and(vec![c(1), c(2)]), c(0), c(3)]),
                flat_and.clone(),
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            (
                and(vec![c(0), and(vec![c(1), c(2)]), c(3)]),
                flat_and.clone(),
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            (
                and(vec![c(0), c(3), and(vec![c(1), c(2)])]),
                flat_and,
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            (
                or(vec![or(vec![c(1), c(2)]), c(0), c(3)]),
                flat_or.clone(),
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            (
                or(vec![c(0), or(vec![c(1), c(2)]), c(3)]),
                flat_or.clone(),
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            (
                or(vec![c(0), c(3), or(vec![c(1), c(2)])]),
                flat_or,
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
        ];
        for (e, expected, ct) in flat_bool {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "parity failed for {e:?} with col_types {ct:?}"
            );
            assert_flat(&new, 4);
        }

        // --- New-domain proof: flatten on Coalesce / Greatest / Least, the
        // three non-boolean associative variadics. Distinct Int64 columns, so
        // the only rewrite is the splice and the result is a flat 3-ary call.
        // Coalesce/Greatest/Least are order-significant, so (unlike And/Or)
        // the flat result keeps the source operand order verbatim.
        let flat_nonbool: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (
                coalesce(vec![c(0), coalesce(vec![c(1), c(2)])]),
                coalesce(vec![c(0), c(1), c(2)]),
                vec![int_ct(), int_ct(), int_ct()],
            ),
            (
                greatest(vec![greatest(vec![c(0), c(1)]), c(2)]),
                greatest(vec![c(0), c(1), c(2)]),
                vec![int_ct(), int_ct(), int_ct()],
            ),
            (
                least(vec![c(0), least(vec![c(1), c(2)])]),
                least(vec![c(0), c(1), c(2)]),
                vec![int_ct(), int_ct(), int_ct()],
            ),
        ];
        for (e, expected, ct) in flat_nonbool {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "parity failed for {e:?} with col_types {ct:?}"
            );
            assert_flat(&new, 3);
        }

        // --- Deep nesting: three nested levels collapse to a flat 4-ary And
        // via saturation re-applying the rule on each newly flat intermediate.
        {
            let e = and(vec![c(0), and(vec![c(1), and(vec![c(2), c(3)])])]);
            let ct = vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()];
            let expected = and(vec![c(0), c(1), c(2), c(3)]);
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "deep-nesting parity failed for {e:?}");
            assert_flat(&new, 4);
        }

        // --- Flatten feeding a cascade: the splice exposes a literal, a
        // duplicate, or an empty inner set that a downstream rule then folds.
        let cascade: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            // Short-circuit: the spliced `false` dominates the flat And.
            (
                and(vec![c(0), and(vec![MirScalarExpr::literal_false(), c(1)])]),
                MirScalarExpr::literal_false(),
                vec![bool_ct(), bool_ct()],
            ),
            // Short-circuit (dual): the spliced `true` dominates the flat Or.
            (
                or(vec![c(0), or(vec![MirScalarExpr::literal_true(), c(1)])]),
                MirScalarExpr::literal_true(),
                vec![bool_ct(), bool_ct()],
            ),
            // drop_unit: the spliced `true` is the And unit and drops out.
            (
                and(vec![c(0), and(vec![MirScalarExpr::literal_true(), c(1)])]),
                and(vec![c(0), c(1)]),
                vec![bool_ct(), bool_ct()],
            ),
            // Cross-boundary dedup: `a` appears both outside and inside; after
            // the splice the duplicate collapses.
            (
                and(vec![c(0), and(vec![c(0), c(1)])]),
                and(vec![c(0), c(1)]),
                vec![bool_ct(), bool_ct()],
            ),
            // Collapse to a single operand via the `and_or_single` self-loop
            // shape: the inner `Or([c0])` collapses into `c0`'s class, the outer
            // `Or([c0, c0])` dedups then collapses to `c0`.
            (or(vec![c(0), or(vec![c(0)])]), c(0), vec![bool_ct()]),
            // Collapse to empty: the spliced-away empty inner And leaves a
            // single-operand And that collapses to `a`.
            (and(vec![c(0), and(vec![])]), c(0), vec![bool_ct()]),
        ];
        for (e, expected, ct) in cascade {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "cascade parity failed for {e:?}");
        }

        // --- Mixed-op negative control: an Or operand inside an And is NOT a
        // same-func nesting, so flatten never fires. The Or survives as a
        // distinct operand (extraction reorders the outer And's operands by
        // `Ord`: the bare Column sorts before the CallVariadic Or).
        {
            let e = and(vec![or(vec![c(0), c(1)]), c(2)]);
            let ct = vec![bool_ct(), bool_ct(), bool_ct()];
            let expected = and(vec![c(2), or(vec![c(0), c(1)])]);
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "mixed-op control parity failed for {e:?}");
            let MirScalarExpr::CallVariadic { func, exprs } = &new else {
                panic!("mixed-op control must stay a CallVariadic And, got {new:?}");
            };
            assert!(
                matches!(func, VariadicFunc::And(_)),
                "mixed-op control outer connective must stay And, got {func:?}"
            );
            assert!(
                exprs.iter().any(|e| matches!(
                    e,
                    MirScalarExpr::CallVariadic { func, .. } if matches!(func, VariadicFunc::Or(_))
                )),
                "mixed-op control must keep its nested Or unflattened, got {new:?}"
            );
        }

        // --- Error operands Bool-typed inside And/Or nesting (the collapse
        // trap). Flatten splices the erroring operand up; the first case keeps
        // it live, the second folds it away under a short-circuit. Parity must
        // hold in both, and neither may panic the shared analysis's merge
        // assertion (which is exactly why the error is Bool-typed).
        let bool_err = || {
            MirScalarExpr::Literal(
                Err(EvalError::DivisionByZero),
                ReprColumnType {
                    scalar_type: ReprScalarType::Bool,
                    nullable: false,
                },
            )
        };
        let errors: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (
                and(vec![c(0), and(vec![err_bool(), c(1)])]),
                and(vec![c(0), c(1), bool_err()]),
                vec![ReprScalarType::Bool.nullable(true), bool_ct()],
            ),
            (
                and(vec![
                    MirScalarExpr::literal_false(),
                    and(vec![err_bool(), c(0)]),
                ]),
                MirScalarExpr::literal_false(),
                vec![bool_ct()],
            ),
        ];
        for (e, expected, ct) in errors {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "error-operand parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice6d() {
        assert!(
            CORPUS.contains("and(#0, and(#1, #2), #3)"),
            "corpus must exercise flatten on And with a nested And operand"
        );
        assert!(
            CORPUS.contains("or(#0, or(#1, #2), #3)"),
            "corpus must exercise flatten on Or with a nested Or operand"
        );
        assert!(
            CORPUS.contains("coalesce(#0, coalesce(#1, #2))"),
            "corpus must exercise flatten on the non-boolean Coalesce"
        );
        assert!(
            CORPUS.contains("greatest(greatest(#0, #1), #2)"),
            "corpus must exercise flatten on the non-boolean Greatest"
        );
        assert!(
            CORPUS.contains("least(#0, least(#1, #2))"),
            "corpus must exercise flatten on the non-boolean Least"
        );
        assert!(
            CORPUS.contains("and(#0, and(#1, and(#2, #3)))"),
            "corpus must exercise deep multi-level flattening"
        );
        assert!(
            CORPUS.contains("and(#0, and(false, #1))"),
            "corpus must exercise flatten feeding a short-circuit"
        );
        assert!(
            CORPUS.contains("and(#0, and(#0, #1))"),
            "corpus must exercise flatten feeding a cross-boundary dedup"
        );
        assert!(
            CORPUS.contains("or(#0, or(#0))"),
            "corpus must exercise the and_or_single self-loop flatten shape"
        );
        assert!(
            CORPUS.contains("and(#0, and(1 / 0 = 1 / 0, #1))"),
            "corpus must exercise flatten preserving a Bool-typed erroring operand"
        );
    }

    // Termination (SP2b Slice 6d): flattening must reach a fixpoint quickly.
    // Each fire strictly folds one level of nesting into the flat operand list,
    // and the two guards keep it finite: the circular-ref skip in
    // `flatten_inner` refuses to splice a same-func node whose canonical
    // children already contain the operand's own class (the `and_or_single`
    // self-loop, where collapsing `f(x)` into `x`'s class leaves an `f`-node
    // pointing back), and `FLATTEN_MAX_OPERANDS` caps the produced vector. So
    // there is no operand explosion to churn against. Lowers each expr directly,
    // bypassing `canonicalize_combined`, so the iteration count is visible,
    // mirroring the slice-6b/6c/6e termination checks.
    #[mz_ore::test]
    fn flatten_terminates() {
        use mz_expr::{MirScalarExpr, VariadicFunc};
        use mz_repr::ReprScalarType;

        let c = MirScalarExpr::column;
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let bool_ct = || ReprScalarType::Bool.nullable(false);

        // (expr, col_types). The self-loop case is the guard's coverage: the
        // inner `Or([c0])` collapses into `c0`'s class via `and_or_single`,
        // after which `c0`'s class holds an `Or`-node whose child is `c0`
        // itself. Without the circular-ref skip, flatten would keep splicing
        // that node back in and grow the operand list without bound.
        let cases: Vec<(MirScalarExpr, Vec<ReprColumnType>)> = vec![
            (
                and(vec![c(0), and(vec![c(1), and(vec![c(2), c(3)])])]),
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            (
                and(vec![c(0), and(vec![c(0), c(1)])]),
                vec![bool_ct(), bool_ct()],
            ),
            (or(vec![c(0), or(vec![c(0)])]), vec![bool_ct()]),
        ];
        for (e, ct) in cases {
            let mut eg = EGraph::new();
            eg.data_mut().scalar.col_types = ct;
            let _root = crate::eqsat::scalar::lower::lower_into(&mut eg, &e);
            let iters = saturate(&mut eg);
            assert!(
                iters <= 10,
                "flatten must reach a fixpoint quickly for {e:?}; got {iters} iters"
            );
        }
    }

    // Differential parity harness (SP2b Slice 6f): dual-connective distributive
    // factoring (`factor_and_or`, the last ported scalar rule). Full-intersection
    // factoring pulls the operands common to every inner And/Or branch out to the
    // dual connective, gated so a factored-away RESIDUAL that could error blocks
    // the rewrite (a common FACTOR that errors is fine, it stays in the result:
    // the CLU-137 property). Each case runs both engines and asserts parity; the
    // parity assertion IS the fork-5 (neutral-portability) proof, so any
    // combined != oracle here is a canonical-form divergence and a slice-7
    // blocker, not something to paper over.
    //
    // Error operands are Bool-typed non-literal predicates (`1 / #c = k`), never a
    // bare Int64 `1 / #c`: a variadic collapse can expose an operand's class as
    // the connective's value, and a bare Int64 error under a Bool connective trips
    // the shared analysis's merge-conflict assertion (the slice-6c type trap).
    #[mz_ore::test]
    fn scalar_parity_slice6f() {
        use mz_expr::{BinaryFunc, MirScalarExpr, VariadicFunc};
        use mz_repr::{Datum, ReprScalarType};

        let c = MirScalarExpr::column;
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let bool_ct = || ReprScalarType::Bool.nullable(false);
        let int_lit = |v: i64| MirScalarExpr::literal_ok(Datum::Int64(v), ReprScalarType::Int64);
        let div64 = || BinaryFunc::DivInt64(mz_expr::func::DivInt64);
        let eq = || BinaryFunc::Eq(mz_expr::func::Eq);

        let is_and = |e: &MirScalarExpr| {
            matches!(
                e,
                MirScalarExpr::CallVariadic { func, .. } if matches!(func, VariadicFunc::And(_))
            )
        };
        let is_or = |e: &MirScalarExpr| {
            matches!(
                e,
                MirScalarExpr::CallVariadic { func, .. } if matches!(func, VariadicFunc::Or(_))
            )
        };

        // --- FIRES: factoring rewrites the outer connective to its dual. The
        // frozen form must show the flip too, proving the rule fired (not
        // merely that the input was already left alone). Or-of-Ands factors
        // to an outer And; And-of-Ors to an outer Or.
        let fires_to_and: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            // Or(And(c0,c1), And(c0,c2)) -> And(c0, Or(c1,c2)).
            (
                or(vec![and(vec![c(0), c(1)]), and(vec![c(0), c(2)])]),
                and(vec![c(0), or(vec![c(1), c(2)])]),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
            // n-ary: three branches sharing c0 -> And(c0, Or(c1,c2,c3)).
            (
                or(vec![
                    and(vec![c(0), c(1)]),
                    and(vec![c(0), c(2)]),
                    and(vec![c(0), c(3)]),
                ]),
                and(vec![c(0), or(vec![c(1), c(2), c(3)])]),
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            // Common factor at a non-leading position inside each inner And.
            // Or(And(c1,c0), And(c2,c0)) -> And(c0, Or(c1,c2)).
            (
                or(vec![and(vec![c(1), c(0)]), and(vec![c(2), c(0)])]),
                and(vec![c(0), or(vec![c(1), c(2)])]),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
        ];
        for (e, expected, ct) in fires_to_and {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "parity failed for {e:?} with col_types {ct:?}"
            );
            assert!(
                is_and(&new),
                "factoring must flip the outer Or to And: {new:?}"
            );
        }

        // Dual: And(Or(c0,c1), Or(c0,c2)) -> Or(c0, And(c1,c2)).
        {
            let e = and(vec![or(vec![c(0), c(1)]), or(vec![c(0), c(2)])]);
            let ct = vec![bool_ct(), bool_ct(), bool_ct()];
            let expected = or(vec![c(0), and(vec![c(1), c(2)])]);
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "dual parity failed for {e:?}");
            assert!(
                is_or(&new),
                "dual factoring must flip the outer And to Or: {new:?}"
            );
        }

        // --- DOES NOT FIRE: factoring leaves the outer connective intact
        // (though extraction may still reorder And/Or operands by `Ord`).
        let no_fire_1 = or(vec![and(vec![c(0), c(1)]), and(vec![c(2), c(3)])]);
        let no_fire_2 = or(vec![
            and(vec![c(0), c(1)]),
            and(vec![c(0), c(2)]),
            and(vec![c(3), c(4)]),
        ]);
        let no_fire: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            // No common factor: disjoint inner sets.
            (
                no_fire_1.clone(),
                no_fire_1,
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            // Partial share: c0 is in two branches but not the third, so the
            // full intersection across every branch is empty.
            (
                no_fire_2.clone(),
                no_fire_2,
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            // Mixed: one branch is a bare column, not an inner And, so there is
            // no uniform inner connective to factor over. The Column operand
            // sorts before the CallVariadic And.
            (
                or(vec![and(vec![c(0), c(1)]), c(2)]),
                or(vec![c(2), and(vec![c(0), c(1)])]),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
        ];
        for (e, expected, ct) in no_fire {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "no-fire parity failed for {e:?} with col_types {ct:?}"
            );
            assert!(
                is_or(&new),
                "factoring must not fire; outer Or must survive: {new:?}"
            );
        }

        // Single branch: Or with one operand collapses via or_single, so factor
        // never applies. Extraction yields the bare inner And.
        {
            let e = or(vec![and(vec![c(0), c(1)])]);
            let ct = vec![bool_ct(), bool_ct()];
            let expected = and(vec![c(0), c(1)]);
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "single-branch parity failed for {e:?}");
        }

        // --- RESIDUAL-ERROR GATE (the correctness core, mirroring
        // `scalar/rules.rs::test_factor_and_or_gate_blocks_erroring_residual`).
        // r = (1 / #1 = 5) errors at #1 == 0. Intersection {c0}, residuals {r}
        // and {c2}. r can error, so factoring must be blocked: the outer Or must
        // stay intact and the result must NOT be the unsound factored form. The
        // witness for why the gate is load-bearing (c0=null, #1=0, c2=true: the
        // input errors, the unsound form would mask it to null) is proven in the
        // standalone-engine test; here we assert the parity and the block.
        {
            let r = int_lit(1)
                .call_binary(c(1), div64())
                .call_binary(int_lit(5), eq());
            let e = or(vec![and(vec![c(0), r.clone()]), and(vec![c(0), c(2)])]);
            let ct = vec![
                ReprScalarType::Bool.nullable(true),
                ReprScalarType::Int64.nullable(true),
                ReprScalarType::Bool.nullable(true),
            ];
            // Blocked: the outer Or survives, its two And branches reordered
            // by `Ord` (And(c0,c2) < And(c0,r): Column sorts before CallBinary).
            let expected = or(vec![and(vec![c(0), c(2)]), and(vec![c(0), r.clone()])]);
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "erroring-residual gate parity failed for {e:?}"
            );
            assert!(
                is_or(&new),
                "blocked factoring must leave the outer Or intact, got {new:?}"
            );
            let unsound = and(vec![c(0), or(vec![r, c(2)])]);
            assert_ne!(
                new, unsound,
                "gate must block factoring an erroring residual; got the unsound form {new:?}"
            );
        }

        // Erroring COMMON FACTOR still fires (the CLU-137 property, mirroring
        // `test_factor_and_or_erroring_common_factor`). g = (1 / #0 = 0) errors at
        // #0 == 0; the residuals c2, c3 are error-free, so the gate permits the
        // rewrite and the erroring factor g is pulled out. Or-of-Ands -> outer And.
        {
            let g = int_lit(1)
                .call_binary(c(0), div64())
                .call_binary(int_lit(0), eq());
            let e = or(vec![and(vec![g.clone(), c(2)]), and(vec![g.clone(), c(3)])]);
            let ct = vec![
                ReprScalarType::Int64.nullable(true),
                ReprScalarType::Int64.nullable(true),
                ReprScalarType::Bool.nullable(true),
                ReprScalarType::Bool.nullable(true),
            ];
            let expected = and(vec![g, or(vec![c(2), c(3)])]);
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(
                new, expected,
                "erroring-common-factor parity failed for {e:?}"
            );
            assert!(
                is_and(&new),
                "erroring common factor must still factor (CLU-137): {new:?}"
            );
        }

        // --- CASCADE: factor feeding, or fed by, the neighbouring rules.
        let cascade: Vec<(MirScalarExpr, MirScalarExpr, Vec<ReprColumnType>)> = vec![
            // Factor feeding flatten: the factored And(c0, Or(c1,c2)) lands
            // inside an outer And and flattens up one level.
            (
                and(vec![
                    c(3),
                    or(vec![and(vec![c(0), c(1)]), and(vec![c(0), c(2)])]),
                ]),
                and(vec![c(0), c(3), or(vec![c(1), c(2)])]),
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            // Fed-by flatten: the single-operand inner Or collapses, exposing a
            // clean Or-of-Ands that then factors.
            (
                or(vec![and(vec![c(0), c(1)]), or(vec![and(vec![c(0), c(2)])])]),
                and(vec![c(0), or(vec![c(1), c(2)])]),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
            // Short-circuit: a `true` operand dominates the outer Or, so factoring
            // is moot and the whole thing folds to `true`.
            (
                or(vec![
                    and(vec![c(0), c(1)]),
                    and(vec![c(0), c(2)]),
                    MirScalarExpr::literal_true(),
                ]),
                MirScalarExpr::literal_true(),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
            // drop_unit feeding factor: the `true` drops from the outer And,
            // exposing And-of-Ors that then factors.
            (
                and(vec![
                    or(vec![c(0), c(1)]),
                    or(vec![c(0), c(2)]),
                    MirScalarExpr::literal_true(),
                ]),
                or(vec![c(0), and(vec![c(1), c(2)])]),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
            // dedup feeding factor: the duplicate branch collapses, leaving a
            // two-branch Or-of-Ands that factors.
            (
                or(vec![
                    and(vec![c(0), c(1)]),
                    and(vec![c(0), c(2)]),
                    and(vec![c(0), c(1)]),
                ]),
                and(vec![c(0), or(vec![c(1), c(2)])]),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
            // Absorb boundary: an empty residual makes factor decline; absorb is
            // the rule that handles it (the inner set {c0,c1} subsumes {c0,c1,c2}
            // -> the superset branch absorbs to c0 AND c1).
            (
                or(vec![and(vec![c(0), c(1)]), and(vec![c(0), c(1), c(2)])]),
                and(vec![c(0), c(1)]),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
            // Empty inner And: and_empty folds And([]) to `true`, which then
            // short-circuits the outer Or to `true`.
            (
                or(vec![and(vec![c(0), c(1)]), and(vec![])]),
                MirScalarExpr::literal_true(),
                vec![bool_ct(), bool_ct()],
            ),
        ];
        for (e, expected, ct) in cascade {
            let new = canonicalize_combined(&e, &ct);
            assert_eq!(new, expected, "cascade parity failed for {e:?}");
        }
    }

    #[mz_ore::test]
    fn corpus_covers_slice6f() {
        assert!(
            CORPUS.contains("or(and(#0, #1), and(#0, #2))"),
            "corpus must exercise factor_and_or firing on Or-of-Ands"
        );
        assert!(
            CORPUS.contains("and(or(#0, #1), or(#0, #2))"),
            "corpus must exercise the dual factor on And-of-Ors"
        );
        assert!(
            CORPUS.contains("or(and(#0, #1), and(#0, #2), and(#0, #3))"),
            "corpus must exercise n-ary factoring across three branches"
        );
        assert!(
            CORPUS.contains("or(and(#1, #0), and(#2, #0))"),
            "corpus must exercise a common factor at a non-leading position"
        );
        assert!(
            CORPUS.contains("or(and(#0, #1), and(#2, #3))"),
            "corpus must exercise the no-common-factor negative control"
        );
        assert!(
            CORPUS.contains("or(and(#0, #1), and(#0, #1, #2))"),
            "corpus must exercise the empty-residual factor/absorb boundary"
        );
        assert!(
            CORPUS.contains("or(and(#0, 1 / #1 = 5), and(#0, #2))"),
            "corpus must exercise the residual-error gate blocking a factor"
        );
        assert!(
            CORPUS.contains("or(and(1 / #0 = 0, #2), and(1 / #0 = 0, #3))"),
            "corpus must exercise an erroring common factor still firing (CLU-137)"
        );
    }

    // Termination (SP2b Slice 6f, fork 4): factoring must reach a fixpoint
    // quickly. Each fire replaces an Or-of-Ands (or And-of-Ors) with the dual
    // form, which is not itself an Or-of-Ands, so factor cannot re-fire on its
    // own output. The only downstream interaction is flatten splicing the
    // factored inner node up one level, which strictly reduces nesting and does
    // not reconstruct a factorable shape, so there is no ping-pong. Lowers each
    // expr directly, bypassing `canonicalize_combined`, so the iteration count is
    // visible, mirroring the slice-6d/6e termination checks. A non-converging
    // case here would be the slice-7 blocker the recon judged absent.
    #[mz_ore::test]
    fn factor_terminates() {
        use mz_expr::{MirScalarExpr, VariadicFunc};
        use mz_repr::ReprScalarType;

        let c = MirScalarExpr::column;
        let and = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::And(mz_expr::func::variadic::And),
            exprs: es,
        };
        let or = |es: Vec<MirScalarExpr>| MirScalarExpr::CallVariadic {
            func: VariadicFunc::Or(mz_expr::func::variadic::Or),
            exprs: es,
        };
        let bool_ct = || ReprScalarType::Bool.nullable(false);

        let cases: Vec<(MirScalarExpr, Vec<ReprColumnType>)> = vec![
            // Single factor, fire-once: the result And(c0, Or(c1,c2)) is not
            // factorable again.
            (
                or(vec![and(vec![c(0), c(1)]), and(vec![c(0), c(2)])]),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
            // Dual.
            (
                and(vec![or(vec![c(0), c(1)]), or(vec![c(0), c(2)])]),
                vec![bool_ct(), bool_ct(), bool_ct()],
            ),
            // n-ary, one fire.
            (
                or(vec![
                    and(vec![c(0), c(1)]),
                    and(vec![c(0), c(2)]),
                    and(vec![c(0), c(3)]),
                ]),
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
            // Factor feeding flatten: exercises the no-ping-pong claim. Factoring
            // produces And(c0, Or(c1,c2)) inside the outer And, flatten splices it
            // up, and neither step reconstructs a factorable shape.
            (
                and(vec![
                    c(3),
                    or(vec![and(vec![c(0), c(1)]), and(vec![c(0), c(2)])]),
                ]),
                vec![bool_ct(), bool_ct(), bool_ct(), bool_ct()],
            ),
        ];
        for (e, ct) in cases {
            let mut eg = EGraph::new();
            eg.data_mut().scalar.col_types = ct;
            let _root = crate::eqsat::scalar::lower::lower_into(&mut eg, &e);
            let iters = saturate(&mut eg);
            assert!(
                iters <= 10,
                "factoring must reach a fixpoint quickly for {e:?}; got {iters} iters"
            );
        }
    }
}
