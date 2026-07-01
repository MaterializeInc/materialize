// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

use mz_expr::{AccessStrategy, Id, MirRelationExpr, MirScalarExpr};
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};
use mz_transform::eqsat::optimize;

/// Build a `Get { Global(Transient(id)) }` source with `arity` non-nullable Int64 columns.
fn src(id: u64, arity: usize) -> MirRelationExpr {
    MirRelationExpr::Get {
        id: Id::Global(GlobalId::Transient(id)),
        typ: ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        ),
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
}

/// Build a `Get { Global(Transient(id)) }` source with `arity` columns.
///
/// `nullable` controls whether the column types are nullable or not. A global
/// Get is used (rather than `Constant`) so the real cost model treats the input
/// as a non-trivial relation; `FoldConstants` cannot collapse it.
fn src_with_nullability(id: u64, arity: usize, nullable: bool) -> MirRelationExpr {
    MirRelationExpr::Get {
        id: Id::Global(GlobalId::Transient(id)),
        typ: ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(nullable))
                .collect(),
        ),
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
}

#[mz_ore::test]
fn pushdown_rules_are_active() {
    // The three column-remapping pushdown rules are enabled now that scalars are
    // real `MirScalarExpr`s and remapping is `MirScalarExpr::permute`.
    let rs = mz_transform::eqsat::default_ruleset();
    for expected in [
        "push_filter_into_join_first",
        "push_filter_into_join_second",
        "push_filter_past_project",
    ] {
        assert!(
            rs.rule_names().contains(&expected),
            "rule {expected} must be active",
        );
    }
}

/// Collect the column support of every `Filter` predicate in `expr`.
fn filter_supports(expr: &MirRelationExpr) -> Vec<std::collections::BTreeSet<usize>> {
    use mz_expr::Columns;
    let mut out = Vec::new();
    expr.visit_pre(|e| {
        if let MirRelationExpr::Filter { predicates, .. } = e {
            for p in predicates {
                out.push(p.support());
            }
        }
    });
    out
}

#[mz_ore::test]
fn push_filter_past_project_remaps_columns() {
    // filter([is_null(#0), is_null(#1)], project([3, 1], r)) over a 4-column
    // nullable r. The predicates read projected positions 0 and 1, which are r's
    // columns 3 and 1. Pushing the filter below the projection remaps position 0
    // to column 3 (and 1 to 1). The extracted plan is the pushed form: a Project
    // wins the cost tie over a Filter at the root (it sorts smaller), so the
    // remapped predicate over column 3 is observable.
    let r = src_with_nullability(1, 4, true);
    let plan = r.project(vec![3, 1]).filter(vec![
        MirScalarExpr::column(0).call_is_null(),
        MirScalarExpr::column(1).call_is_null(),
    ]);
    let out = optimize(plan);
    assert_eq!(out.arity(), 2, "arity preserved");
    // A filter predicate now reads r's column 3, which is only possible if the
    // position-0 predicate was remapped through the projection.
    let supports = filter_supports(&out);
    assert!(
        supports.iter().any(|s| s.contains(&3)),
        "expected a remapped filter predicate over column 3, got {supports:?} in {out:?}"
    );
}

#[mz_ore::test]
fn push_filter_into_join_first_terminates() {
    // filter(is_null(#0), join(a, b)) where the predicate reads only the first
    // input's column 0. The rule pushes the filter onto `a` (no reindexing).
    // Under the cardinality-free cost model the move is cost-neutral and the
    // un-pushed form sorts smaller, so this guards termination and arity: the
    // rule must fire in saturation without diverging.
    let a = src_with_nullability(1, 2, true);
    let b = src(2, 2);
    let plan = MirRelationExpr::join(vec![a, b], vec![])
        .filter(vec![MirScalarExpr::column(0).call_is_null()]);
    let out = optimize(plan);
    assert_eq!(out.arity(), 4, "arity preserved");
    // The predicate still reads column 0 (unchanged by the no-reindex rule).
    assert!(
        filter_supports(&out).iter().any(|s| s.contains(&0)),
        "predicate over column 0 must survive, got {out:?}"
    );
}

#[mz_ore::test]
fn push_filter_into_join_second_terminates() {
    // filter(is_null(#2), join(a, b)) where the predicate reads only the second
    // input (column 2 in the join layout, b's local column 0). The rule shifts
    // the predicate down by arity(a) = 2 when pushing onto b. Cost-neutral, so
    // this guards termination and arity rather than which form is extracted (the
    // shift itself is unit-tested in matcher::tests::shift_rewrites_columns).
    let a = src(1, 2);
    let b = src_with_nullability(2, 2, true);
    let plan = MirRelationExpr::join(vec![a, b], vec![])
        .filter(vec![MirScalarExpr::column(2).call_is_null()]);
    let out = optimize(plan);
    assert_eq!(out.arity(), 4, "arity preserved");
}

fn base(arity: usize) -> MirRelationExpr {
    let typ = ReprRelationType::new(
        (0..arity)
            .map(|_| ReprScalarType::Int64.nullable(false))
            .collect(),
    );
    MirRelationExpr::constant(vec![], typ)
}

#[mz_ore::test]
fn merges_nested_filters() {
    // filter(p, filter(q, r)) should fuse to a single filter of two predicates.
    // Predicates must be boolean-typed: coalesce_mfp calls Filter::action which
    // calls canonicalize_predicates, asserting boolean types. Use is_null on
    // nullable columns so the predicates are not folded to false at lower time.
    let r = src_with_nullability(5, 2, true)
        .filter(vec![MirScalarExpr::column(1).call_is_null()])
        .filter(vec![MirScalarExpr::column(0).call_is_null()]);
    let out = optimize(r);
    let mut filters = 0usize;
    out.visit_pre(|e| {
        if matches!(e, MirRelationExpr::Filter { .. }) {
            filters += 1;
        }
    });
    assert_eq!(filters, 1, "nested filters should fuse");
}

#[mz_ore::test]
fn arity_is_preserved() {
    let r = base(3).map(vec![MirScalarExpr::column(0)]);
    let before = r.arity();
    assert_eq!(optimize(r).arity(), before);
}

#[mz_ore::test]
fn filter_false_optimizes_to_empty() {
    // `empty_false_filter` rewrites `Filter[false] r => Empty(r)`. The engine
    // encodes Empty as `Constant { card: 0, arity }`. Raise must not panic and
    // must produce a relation of the original arity.
    let r = base(2).filter(vec![MirScalarExpr::literal_false()]);
    let out = optimize(r);
    assert_eq!(out.arity(), 2);
}

#[mz_ore::test]
fn union_cancel_optimizes() {
    // `union_cancel` rewrites `Union(a, Negate a) => Empty(a)`. Extraction
    // may pick the Empty; raise must not panic and arity must be preserved.
    let b = base(2);
    let r = MirRelationExpr::Union {
        base: Box::new(b.clone()),
        inputs: vec![b.negate()],
    };
    let out = optimize(r);
    assert_eq!(out.arity(), 2);
}

/// Returns true if `expr` is a 0-row Constant (i.e., the Empty representation
/// produced by `empty_false_filter`).
fn is_empty_constant(expr: &MirRelationExpr) -> bool {
    match expr {
        MirRelationExpr::Constant { rows, .. } => rows.as_ref().map_or(false, |v| v.is_empty()),
        _ => false,
    }
}

#[mz_ore::test]
fn is_null_on_not_null_folds_to_empty() {
    // IS NULL(#0) on a NOT NULL column is provably false via column types.
    // `intern_scalar_in_context` should fold the clone to `false`, setting
    // `lit = Some(false)`, so `empty_false_filter` fires and the filter
    // is replaced by the empty constant.
    let r = src_with_nullability(1, 2, false).filter(vec![MirScalarExpr::column(0).call_is_null()]);
    let out = optimize(r);
    assert_eq!(out.arity(), 2, "arity must be preserved");
    assert!(
        is_empty_constant(&out),
        "IS NULL on NOT NULL column should optimize to empty, got {out:?}"
    );
}

#[mz_ore::test]
fn is_null_on_nullable_stays() {
    // IS NULL(#0) on a nullable column cannot be folded: the predicate may be
    // true or false at runtime. The optimizer must NOT produce an empty constant.
    let r = src_with_nullability(2, 2, true).filter(vec![MirScalarExpr::column(0).call_is_null()]);
    let out = optimize(r);
    assert_eq!(out.arity(), 2, "arity must be preserved");
    assert!(
        !is_empty_constant(&out),
        "IS NULL on nullable column must not be folded to empty, got {out:?}"
    );
}

#[mz_ore::test]
fn case5_filter_over_union_filter() {
    // This is case 5 from compare_real - exercises lit=false on a Filter over Union.
    let a = src_with_nullability(1, 2, false).filter(vec![MirScalarExpr::column(0).call_is_null()]);
    let b = src_with_nullability(2, 2, false).filter(vec![MirScalarExpr::column(1).call_is_null()]);
    let r = MirRelationExpr::Union {
        base: Box::new(a),
        inputs: vec![b],
    }
    .filter(vec![MirScalarExpr::column(0).call_is_null()]);
    let out = optimize(r);
    assert_eq!(out.arity(), 2, "arity must be preserved");
}

#[mz_ore::test]
fn column_ref_lit_is_unaffected() {
    // A non-literal filter predicate does not become a literal false after
    // reduce; `lit` must stay None so existing filter behaviour is unchanged.
    // Use is_null(#0) on a nullable column: not reducible to false, boolean-typed
    // (required for coalesce_mfp's Filter::action / canonicalize_predicates call).
    let r = src_with_nullability(3, 2, true).filter(vec![MirScalarExpr::column(0).call_is_null()]);
    // This should not panic and the filter must not be treated as false.
    let out = optimize(r);
    assert_eq!(out.arity(), 2, "arity must be preserved");
    assert!(
        !is_empty_constant(&out),
        "non-literal filter predicate must not be treated as false, got {out:?}"
    );
}

#[mz_ore::test]
fn threshold_over_empty_filter_collapses() {
    // filter(false, r).threshold() should optimize to an empty constant:
    //   1. empty_false_filter fires: Filter[false] r => Empty(r)
    //   2. threshold_empty fires:    Threshold(Empty) => Empty
    let r = src_with_nullability(10, 2, false)
        .filter(vec![MirScalarExpr::literal_false()])
        .threshold();
    let out = optimize(r);
    assert_eq!(out.arity(), 2, "arity must be preserved");
    assert!(
        is_empty_constant(&out),
        "threshold(filter(false, r)) should optimize to empty constant, got {out:?}"
    );
}

#[mz_ore::test]
fn union_drops_empty_branch() {
    // Union(filter(false, src0), src1) should optimize so the empty branch is gone.
    //   1. empty_false_filter fires on the filter branch: Filter[false] r => Empty(r)
    //   2. union_drop_empty_left fires: Union(Empty, src1) => src1
    let empty_branch =
        src_with_nullability(11, 2, false).filter(vec![MirScalarExpr::literal_false()]);
    let good_branch = src_with_nullability(12, 2, false);
    let r = MirRelationExpr::Union {
        base: Box::new(empty_branch),
        inputs: vec![good_branch],
    };
    let out = optimize(r);
    assert_eq!(out.arity(), 2, "arity must be preserved");
    assert!(
        !is_empty_constant(&out),
        "union with one empty branch should not produce empty constant, got {out:?}"
    );
    // The result should not contain a false-predicate filter.
    let mut has_false_filter = false;
    out.visit_pre(|e| {
        if let MirRelationExpr::Filter { predicates, .. } = e {
            if predicates
                .iter()
                .any(|p| *p == MirScalarExpr::literal_false())
            {
                has_false_filter = true;
            }
        }
    });
    assert!(
        !has_false_filter,
        "optimized union should not contain filter(false), got {out:?}"
    );
}

#[mz_ore::test]
fn union_cancel_then_empty_collapses() {
    // Union(a, Negate(a)) where `a` is an opaque Global Get: after leaf dedup,
    // both `a` references share one e-class, so `union_cancel` can fire and
    // produce Empty. The arity must be preserved.
    let a = src(1, 2);
    let r = MirRelationExpr::Union {
        base: Box::new(a.clone()),
        inputs: vec![a.negate()],
    };
    let out = optimize(r);
    assert_eq!(out.arity(), 2, "arity must be preserved after union_cancel");
    assert!(
        is_empty_constant(&out),
        "Union(a, Negate(a)) for opaque leaf should collapse to empty constant, got {out:?}"
    );
}

/// Count the `MirRelationExpr` nodes of a given variant.
fn count_nodes(expr: &MirRelationExpr, pred: impl Fn(&MirRelationExpr) -> bool) -> usize {
    let mut n = 0;
    expr.visit_pre(|e| {
        if pred(e) {
            n += 1;
        }
    });
    n
}

#[mz_ore::test]
fn reduce_lowers_structurally_and_round_trips() {
    // reduce(group [#0], no aggregates) over a plain source. The source has no
    // known key, so reduce_elision cannot fire and the Reduce must survive,
    // proving it lowers structurally (rather than bailing to an opaque leaf).
    let r = src(1, 2).reduce(vec![0], vec![], None);
    let out = optimize(r);
    assert_eq!(out.arity(), 1, "reduce over group [#0] has arity 1");
    assert_eq!(
        count_nodes(&out, |e| matches!(e, MirRelationExpr::Reduce { .. })),
        1,
        "the Reduce must survive, got {out:?}"
    );
}

#[mz_ore::test]
fn reduce_elision_fires_on_keyed_input() {
    // Nested reduce on the same key: the inner reduce establishes {0} as a unique
    // key, so the outer reduce(group [#0]) is redundant and reduce_elision
    // rewrites it to a projection. Exactly one Reduce (the inner) must remain.
    let r = src(1, 2)
        .reduce(vec![0], vec![], None)
        .reduce(vec![0], vec![], None);
    let out = optimize(r);
    assert_eq!(out.arity(), 1, "arity preserved");
    assert_eq!(
        count_nodes(&out, |e| matches!(e, MirRelationExpr::Reduce { .. })),
        1,
        "the outer reduce must be elided, leaving one Reduce, got {out:?}"
    );
}

#[mz_ore::test]
fn topk_lowers_structurally_and_round_trips() {
    // A TopK with no rule to rewrite it must survive optimization as a TopK
    // (structural passthrough), preserving its arity.
    let r = src(1, 3).top_k(vec![0], vec![], None, 0, None);
    let out = optimize(r);
    assert_eq!(out.arity(), 3, "TopK preserves input arity");
    assert_eq!(
        count_nodes(&out, |e| matches!(e, MirRelationExpr::TopK { .. })),
        1,
        "the TopK must survive, got {out:?}"
    );
}

#[mz_ore::test]
fn topk_input_optimizes_inside_the_envelope() {
    // topk(filter(true, r)): the always-true filter inside the TopK's input is
    // dropped (drop_true_filter), which is only possible because TopK lowers
    // structurally and exposes its input. A bailed TopK would hide the filter.
    // The TopK itself has no rule over a non-empty input, so it survives.
    let r = src_with_nullability(1, 2, false)
        .filter(vec![MirScalarExpr::literal_true()])
        .top_k(vec![0], vec![], None, 0, None);
    let out = optimize(r);
    assert_eq!(out.arity(), 2, "arity preserved");
    assert_eq!(
        count_nodes(&out, |e| matches!(e, MirRelationExpr::TopK { .. })),
        1,
        "the TopK envelope must survive, got {out:?}"
    );
    assert_eq!(
        count_nodes(&out, |e| matches!(e, MirRelationExpr::Filter { .. })),
        0,
        "the always-true filter inside the TopK must be dropped, got {out:?}"
    );
}

#[mz_ore::test]
fn topk_over_empty_collapses() {
    // topk(filter(false, r)): the inner filter folds to empty, then topk_empty
    // propagates emptiness through the TopK, collapsing the whole plan to an
    // empty constant (matching what the real optimizer does).
    let r = src_with_nullability(1, 2, false)
        .filter(vec![MirScalarExpr::literal_false()])
        .top_k(vec![0], vec![], None, 0, None);
    let out = optimize(r);
    assert_eq!(out.arity(), 2, "arity preserved");
    assert!(
        is_empty_constant(&out),
        "topk over an empty input must collapse to an empty constant, got {out:?}"
    );
}

#[mz_ore::test]
fn union_cancel_under_filter_and_map_terminates() {
    // Regression: Union(a, Negate(a)) wrapped in Filter+Map must not hang.
    // Before the merge_filters `not_rel_empty(r)` guard, the union_cancel +
    // empty_false_filter combination created a self-referential e-class that
    // made merge_filters grow predicate lists without bound.
    let a = src(1, 2);
    let r = MirRelationExpr::Union {
        base: Box::new(a.clone()),
        inputs: vec![a.negate()],
    }
    .filter(vec![MirScalarExpr::column(0).call_is_null()])
    .map(vec![MirScalarExpr::column(1)]);
    let out = optimize(r);
    assert_eq!(out.arity(), 3, "arity must be preserved");
}

#[mz_ore::test]
fn non_recursive_let_stays_shared_after_union() {
    use mz_expr::LocalId;

    // A non-recursive `Let l = Filter[#0 is null](src) in Union[Get l, Get l,
    // Get l]`. Step 2 unions the binding definition into the body's e-graph to
    // un-trap its facts onto the `Get l` references. The reassembled plan must
    // stay a correct, sharing `Let`: arity preserved, the binding referenced (not
    // inlined into three copies of the Filter). We assert there is at most one
    // Filter in the output (the binding's single definition), proving the body
    // did not inline the multi-use binding.
    let lid = LocalId::new(1);
    let typ = ReprRelationType::new(
        (0..2)
            .map(|_| ReprScalarType::Int64.nullable(true))
            .collect(),
    );
    let value =
        src_with_nullability(1, 2, true).filter(vec![MirScalarExpr::column(0).call_is_null()]);
    let get_l = || MirRelationExpr::Get {
        id: Id::Local(lid.clone()),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    };
    let body = MirRelationExpr::Union {
        base: Box::new(get_l()),
        inputs: vec![get_l(), get_l()],
    };
    let plan = MirRelationExpr::Let {
        id: lid,
        value: Box::new(value),
        body: Box::new(body),
    };
    let expected_arity = plan.arity();

    let out = optimize(plan);
    assert_eq!(out.arity(), expected_arity, "arity must be preserved");
    // The binding's single Filter must not be duplicated into the three union
    // branches: the optimizer must keep the binding shared rather than inline it.
    let filters = count_nodes(&out, |e| matches!(e, MirRelationExpr::Filter { .. }));
    assert!(
        filters <= 1,
        "the multi-use binding must stay shared (<=1 Filter), not inlined; got {filters} in {out:?}"
    );
}

#[mz_ore::test]
fn impossible_filter_across_let_collapses_via_union() {
    use mz_expr::{BinaryFunc, LocalId};
    use mz_repr::Datum;
    use mz_transform::eqsat::optimize_without_let_union;

    // `Let l0 = Filter[#0 = 5](src) in Union[Filter[#0 = 6](Get l0), Get l0, Get
    // l0]`. The binding pins column 0 to 5; one body use then filters #0 = 6,
    // which is unsatisfiable given #0 = 5. The fact #0 = 5 lives on the binding's
    // definition, trapped behind the multi-use `Get l0`: no MIR post-pass crosses
    // the Let boundary, and `NormalizeLets` will not inline a multi-use binding.
    // Only the eqsat Let-union carries the fact onto the `Get l0` class, after
    // which the body filter's scope is `unsatisfiable` and the colored
    // empty-fold (`colored_derive` marks the class empty; extraction emits an
    // empty Constant) replaces that branch. The union demonstrably crosses the
    // Let here; the `Equivalences`-derived emptiness is detected once the fact
    // reaches the body.
    //
    // This test guards the step-2 Let-union fact propagation in isolation: with
    // the union ON the eqsat optimizer reaches the collapse, with it OFF the fact
    // stays trapped. NOTE: the full production `logical_optimizer` reaches the
    // same collapse independently, via its own Let-aware `Equivalences` analysis
    // that propagates the binding's value facts to each `Get` use site. So on
    // this shape the eqsat win is dormant relative to production; the value of
    // the union is the e-graph-native mechanism (facts on the shared class during
    // saturation), not a plan production could not otherwise reach.
    fn col_eq(col: usize, val: i64) -> MirScalarExpr {
        MirScalarExpr::column(col).call_binary(
            MirScalarExpr::literal_ok(Datum::Int64(val), ReprScalarType::Int64),
            BinaryFunc::Eq(mz_expr::func::Eq),
        )
    }

    let typ = ReprRelationType::new(
        (0..2)
            .map(|_| ReprScalarType::Int64.nullable(true))
            .collect(),
    );
    let build = || {
        let lid = LocalId::new(1);
        let value = src_with_nullability(1, 2, true).filter(vec![col_eq(0, 5)]);
        let get_l = || MirRelationExpr::Get {
            id: Id::Local(lid.clone()),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        // Multi-use body so the post-pass `NormalizeLets` cannot inline the
        // binding (which would otherwise reach the fact without the union).
        let body = MirRelationExpr::Union {
            base: Box::new(get_l().filter(vec![col_eq(0, 6)])),
            inputs: vec![get_l(), get_l()],
        };
        MirRelationExpr::Let {
            id: lid,
            value: Box::new(value),
            body: Box::new(body),
        }
    };

    // Counts Filter predicates of the form `#0 = 6` (the impossible body filter).
    let count_impossible = |e: &MirRelationExpr| {
        count_nodes(e, |n| {
            matches!(n, MirRelationExpr::Filter { predicates, .. }
                if predicates.iter().any(|p| *p == col_eq(0, 6)))
        })
    };

    // With the union: the body filter is gone, replaced by an empty Constant.
    let with_union = optimize(build());
    assert_eq!(with_union.arity(), 2, "arity must be preserved");
    assert_eq!(
        count_impossible(&with_union),
        0,
        "the union must un-trap #0 = 5 onto Get l0 so the impossible #0 = 6 filter collapses; got {with_union:?}"
    );
    assert!(
        count_nodes(&with_union, is_empty_constant) >= 1,
        "the impossible branch must collapse to an empty Constant; got {with_union:?}"
    );

    // Control: without the union, the fact stays trapped behind the multi-use
    // `Get l0`, so the impossible filter survives. This proves the win above
    // requires the Let-union, not some unrelated post-pass.
    let without_union = optimize_without_let_union(build());
    assert_eq!(without_union.arity(), 2, "arity must be preserved");
    assert!(
        count_impossible(&without_union) >= 1,
        "without the union the impossible filter must survive (fact trapped behind Get l0); got {without_union:?}"
    );
}

#[mz_ore::test]
fn recursive_cte_with_inner_cse_does_not_panic() {
    use mz_expr::LocalId;
    use mz_transform::eqsat::optimize_logical;

    // Regression for the CSE fresh-id collision with opaque-leaf LocalIds.
    //
    // `lower` bails `LetRec` into a `Rel::Opaque` carrying the verbatim MIR,
    // which holds a `LocalId`. eqsat's CSE allocates fresh `Let` ids above the
    // max id it can see; before the fix it scanned only `Rel` Let/LocalGet ids
    // and missed the opaque leaf's LocalId, so a fresh CSE id could equal the
    // recursive binding's id, shadowing it. Downstream `Demand::action` asserts
    // a `LetRec` has no shadowed bindings and panics.
    //
    // We build a `LetRec` (bailed to opaque) sitting under a Union with a
    // separate CSE opportunity (a Filter referenced twice), which forces a
    // fresh-id allocation. The whole pipeline (lower -> CSE -> raise ->
    // demand_pushdown) must complete without panicking and preserve arity.
    let lid = LocalId::new(1);
    // Arity 4 so the recursive (opaque) branch matches the self-join branch of
    // the Union below.
    let typ = ReprRelationType::new(
        (0..4)
            .map(|_| ReprScalarType::Int64.nullable(false))
            .collect(),
    );
    let rec = MirRelationExpr::LetRec {
        ids: vec![lid.clone()],
        values: vec![MirRelationExpr::Get {
            id: Id::Local(lid.clone()),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        }],
        limits: vec![None],
        body: Box::new(MirRelationExpr::Get {
            id: Id::Local(lid),
            typ,
            access_strategy: AccessStrategy::UnknownOrLocal,
        }),
    };

    // A CSE opportunity: a filtered source self-joined, then unioned with the
    // recursive (opaque) fragment.
    let filtered = src(2, 2).filter(vec![MirScalarExpr::column(0).call_is_null()]);
    let joined = MirRelationExpr::join_scalars(vec![filtered.clone(), filtered], vec![]);
    let plan = MirRelationExpr::Union {
        base: Box::new(joined),
        inputs: vec![rec],
    };
    let expected_arity = plan.arity();

    // Before the fix this panics in `Demand::action` with
    // "assertion failed: expected None found Some(...)".
    let out = optimize_logical(plan, false);
    assert_eq!(out.arity(), expected_arity, "arity must be preserved");
}
