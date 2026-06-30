// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Tests for the WMR-lift experiment: carrying the iteration version through the
//! e-graph node so congruence keeps versioned recursive references distinct, and
//! the relaxed cross-binding CSE that hoists a shared subterm into a new `LetRec`
//! binding placed at a version-valid order slot.

use std::collections::BTreeMap;

use mz_expr::{AccessStrategy, BinaryFunc, Id, LocalId, MirRelationExpr, MirScalarExpr, func};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, GlobalId, ReprRelationType, ReprScalarType};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::eqsat::cse::eliminate_common_subexpressions;
use mz_transform::eqsat::egraph::EGraph;
use mz_transform::eqsat::ir::{EScalar, RecVersion, Rel};
use mz_transform::eqsat::raise;
use mz_transform::typecheck::{Typecheck, empty_typechecking_context};
use mz_transform::{Transform, TransformCtx};

/// Part A: two `LocalGet`s that differ only in their `version` tag must intern
/// to distinct e-classes, while same-version (and `None`-version) gets share
/// one. This is the congruence-separation mechanism the whole experiment rests
/// on: the e-graph interns `ENode`, so the version must ride on `ENode` for
/// `Prev(j)` and `Cur(j)` to denote different classes.
#[mz_ore::test]
fn versioned_localgets_get_distinct_eclasses() {
    let mk = |version| Rel::LocalGet {
        id: 0,
        arity: 1,
        get: None,
        version,
    };
    let mut g = EGraph::default();
    let prev = g.add_rel(&mk(Some(RecVersion::Prev)));
    let cur = g.add_rel(&mk(Some(RecVersion::Cur)));
    let prev2 = g.add_rel(&mk(Some(RecVersion::Prev)));
    let none1 = g.add_rel(&mk(None));
    let none2 = g.add_rel(&mk(None));
    assert_ne!(prev, cur, "different versions must be different e-classes");
    assert_eq!(prev, prev2, "same version must hash-cons to one e-class");
    assert_eq!(none1, none2, "flag-off (None) gets still share one e-class");
}

/// A single-column `Int64` relation type.
fn typ1() -> ReprRelationType {
    ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)])
}

/// `Get(Id::Local(local))` over a single `Int64` column.
fn local_get(local: u64) -> MirRelationExpr {
    MirRelationExpr::Get {
        id: Id::Local(LocalId::new(local)),
        typ: typ1(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
}

/// The shared subterm `Filter(#0 > 0, Map(#0 + 1, Get(b0)))`, projected back to
/// one column so the binding type stays `Int64`-arity-1. Identical in every
/// binding that builds it over the same `b0`, so CSE can share it.
fn shared_subterm(b0: u64) -> MirRelationExpr {
    let plus = MirScalarExpr::column(0).call_binary(
        MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
        BinaryFunc::AddInt64(func::AddInt64),
    );
    let gt = MirScalarExpr::column(0).call_binary(
        MirScalarExpr::literal_ok(Datum::Int64(0), ReprScalarType::Int64),
        BinaryFunc::Gt(func::Gt),
    );
    local_get(b0)
        .map(vec![plus])
        .filter(vec![gt])
        .project(vec![0])
}

/// A `LetRec` whose two recursive bindings `b1`, `b2` both compute the same
/// `shared_subterm(b0)` and add a distinguishing wrapper, over a common base
/// `b0`. The body unions `b1` and `b2`. This is the cross-binding-sharing shape
/// the lift targets: `Filter(Map(Get(b0)))` is identical in `b1` and `b2`.
fn shared_letrec() -> MirRelationExpr {
    // b0 = 0, b1 = 1, b2 = 2. b0 is ordered before b1 and b2, so the `Get(b0)`
    // inside the shared subterm tags as `Cur` in both bindings.
    let b0 = 0u64;
    let b1 = 1u64;
    let b2 = 2u64;

    // Base case: a self-referential recursive seed for b0. Using `Get(b0)`
    // closes the cycle without an Opaque constant (which would not lower).
    let v0 = local_get(b0);
    // b1 negates the shared subterm; b2 thresholds it. The wrappers differ so
    // the bindings are distinct relations, but the inner subterm is identical.
    let v1 = shared_subterm(b0).negate();
    let v2 = shared_subterm(b0).threshold();

    let body = local_get(b1).union(local_get(b2));

    MirRelationExpr::LetRec {
        ids: vec![LocalId::new(b0), LocalId::new(b1), LocalId::new(b2)],
        values: vec![v0, v1, v2],
        limits: vec![None, None, None],
        body: Box::new(body),
    }
}

/// Count `LetRec` bindings in the outermost `LetRec` of `expr` (0 if none).
fn letrec_binding_count(expr: &MirRelationExpr) -> usize {
    let mut count = 0;
    expr.visit_pre(|e| {
        if let MirRelationExpr::LetRec { ids, .. } = e {
            count = count.max(ids.len());
        }
    });
    count
}

/// Count how many of `expr`'s `LetRec` binding values reference the local id
/// `target` via a `Get(Local(target))`. A hoisted binding is shared when two or
/// more sibling binding values read it.
fn binding_values_referencing(expr: &MirRelationExpr, target: u64) -> usize {
    let mut count = 0;
    expr.visit_pre(|e| {
        if let MirRelationExpr::LetRec { ids, values, .. } = e {
            for (i, value) in values.iter().enumerate() {
                // Skip the binding that *is* `target` (a self-reference does not
                // count as a sharing consumer).
                if u64::from(&ids[i]) == target {
                    continue;
                }
                let mut reads = false;
                value.visit_pre(|n| {
                    if let MirRelationExpr::Get {
                        id: Id::Local(l), ..
                    } = n
                    {
                        if u64::from(l) == target {
                            reads = true;
                        }
                    }
                });
                if reads {
                    count += 1;
                }
            }
        }
    });
    count
}

/// True iff some binding id that did NOT exist in the original `{b0, b1, b2}`
/// set is referenced by two or more sibling binding values: a freshly hoisted,
/// genuinely shared binding.
fn has_shared_hoisted_binding(expr: &MirRelationExpr) -> bool {
    let mut found = false;
    expr.visit_pre(|e| {
        if let MirRelationExpr::LetRec { ids, .. } = e {
            for id in ids {
                let id = u64::from(id);
                if id > 2 && binding_values_referencing(expr, id) >= 2 {
                    found = true;
                }
            }
        }
    });
    found
}

/// Part B: with the lift on, the subterm shared across `b1` and `b2` is hoisted
/// into one new `LetRec` binding, growing the binding count, the hoisted binding
/// is its own value, and the raised plan survives a strict `Typecheck` (the
/// version-aware order placement keeps the plan well-formed). End-to-end result
/// equivalence is deferred to a SQL-level differential test (Task 7): the
/// `mz-transform` unit harness has no differential runtime.
#[mz_ore::test]
fn cross_binding_subexpression_is_shared() {
    let input = shared_letrec();
    let before = letrec_binding_count(&input);

    let optimized = mz_transform::eqsat::optimize_with_wmr_lift(input.clone());

    assert!(
        letrec_binding_count(&optimized) > before,
        "a shared binding should have been introduced; before={before}, plan={optimized:?}"
    );
    assert!(
        has_shared_hoisted_binding(&optimized),
        "a freshly hoisted binding should be read by two or more siblings; plan={optimized:?}"
    );

    // The optimized plan must survive a strict Typecheck: the hoisted binding's
    // order slot realizes its versions, so the LetRec is well-formed.
    let ctx = empty_typechecking_context();
    let features = OptimizerFeatures::default();
    let mut df = DataflowMetainfo::default();
    let mut transform_ctx = TransformCtx::local(&features, &ctx, &mut df, None, None);
    let mut checked = optimized;
    Typecheck::new(std::sync::Arc::clone(&ctx))
        .transform(&mut checked, &mut transform_ctx)
        .expect("optimized plan must pass strict Typecheck");
}

/// A `Rel::Opaque` wrapping a global `Get` with one `Int64` column. Used as a
/// stand-in for an external relation that is not part of the LetRec.
fn opaque_global(gid: u64) -> Rel {
    let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
    Rel::Opaque(Box::new(MirRelationExpr::Get {
        id: Id::Global(GlobalId::Transient(gid)),
        typ,
        access_strategy: AccessStrategy::UnknownOrLocal,
    }))
}

/// The loop-invariant `Rel` subterm: `Filter(#0 > 0, Opaque(GlobalGet(g)))`.
/// It contains no `LocalGet { version: Some(_) }`, so its value does not change
/// across iterations of an enclosing LetRec.
fn rel_invariant_subterm(g: u64) -> Rel {
    let pred = EScalar::plain(MirScalarExpr::column(0).call_binary(
        MirScalarExpr::literal_ok(Datum::Int64(0), ReprScalarType::Int64),
        BinaryFunc::Gt(func::Gt),
    ));
    Rel::Filter {
        predicates: vec![pred],
        input: Box::new(opaque_global(g)),
    }
}

/// A `Rel::LocalGet` with a version tag, representing a reference to a LetRec
/// binding from within the loop.
///
/// The `get` field carries the original `MirRelationExpr::Get { Local }` node
/// (matching what `lower` produces from a real `MirRelationExpr::LetRec`), so
/// `raise` can reconstruct the node verbatim without consulting `scope`.
fn versioned_local(id: usize, version: RecVersion) -> Rel {
    Rel::LocalGet {
        id,
        arity: 1,
        get: Some(Box::new(local_get(u64::try_from(id).expect("id fits u64")))),
        version: Some(version),
    }
}

/// Build a `Rel::LetRec` where the loop-invariant subterm `InvSub` is embedded
/// inside a shared versioned subterm `s_v`, so the outer CSE misses it after
/// Task 5 hoists `s_v`.
///
/// Shape (ids = [b0=10, b1=11, b2=12], fresh above any CSE placeholders):
/// - `b0 = LocalGet(b0, Prev)` — self-recursive seed.
/// - `b1 = Threshold(s_v)` — wraps the shared versioned subterm.
/// - `b2 = Negate(s_v)` — wraps the same `s_v`.
///
/// `s_v = Union(LocalGet(b0, Cur), InvSub)` where `InvSub = Filter(#0>0, Opaque(g))`.
///
/// `s_v` appears as a direct child of both `b1` and `b2`, so in-loop CSE counts
/// it twice. Without Task 6, Task 5 hoists `s_v` as a new LetRec binding, after
/// which `InvSub` appears only once (inside the new binding) and the outer CSE
/// misses it. With Task 6, `InvSub` is caught first and emitted as an outer `Let`.
fn rel_invariant_letrec() -> Rel {
    let b0 = 10usize;
    let b1 = 11usize;
    let b2 = 12usize;
    let g = 42u64;

    let inv_sub = rel_invariant_subterm(g);

    // s_v = Union(LocalGet(b0, Cur), InvSub): the shared versioned subterm.
    // LocalGet(b0, Cur) is the recursive reference to b0 (Cur because b0 < b1, b2).
    let s_v = Rel::Union {
        base: Box::new(versioned_local(b0, RecVersion::Cur)),
        inputs: vec![inv_sub],
    };

    // b0 = LocalGet(b0, Prev): self-recursive.
    let v0 = versioned_local(b0, RecVersion::Prev);
    // b1 = Threshold(s_v).
    let v1 = Rel::Threshold {
        input: Box::new(s_v.clone()),
    };
    // b2 = Negate(s_v).
    let v2 = Rel::Negate {
        input: Box::new(s_v),
    };

    let body = Rel::Union {
        base: Box::new(versioned_local(b1, RecVersion::Final)),
        inputs: vec![versioned_local(b2, RecVersion::Final)],
    };

    Rel::LetRec {
        bindings: vec![(b0, v0), (b1, v1), (b2, v2)],
        limits: vec![None, None, None],
        body: Box::new(body),
    }
}

/// True iff the topmost sequence of `Rel::Let` nodes (before the first non-`Let`
/// node) wraps a `Rel::LetRec`. This is the structural signature of a
/// loop-invariant hoist: the invariant value is bound once outside the loop.
fn rel_has_let_wrapping_letrec(rel: &Rel) -> bool {
    match rel {
        Rel::Let { body, .. } => {
            matches!(body.as_ref(), Rel::LetRec { .. }) || rel_has_let_wrapping_letrec(body)
        }
        _ => false,
    }
}

/// Part C: with the lift on, a loop-invariant subterm embedded inside a shared
/// versioned subterm must be hoisted to an ordinary `Rel::Let` wrapping the
/// whole `Rel::LetRec`, so it is computed once rather than every iteration.
///
/// This test bypasses the full eqsat pipeline and tests `eliminate_common_subexpressions`
/// directly on a hand-constructed `Rel::LetRec` with version tags. The input is
/// shaped so that after Task 5 hoists the versioned shared subterm `s_v`, the
/// invariant child `InvSub` remains only once in the tree and the outer CSE
/// misses it. Task 6 must catch `InvSub` before Task 5 reduces its count.
///
/// Structural assertion: the output is a `Rel::Let` (or a chain of them) whose
/// innermost non-`Let` node is a `Rel::LetRec`. The raised plan must also pass
/// a strict `Typecheck` so a future raise regression on the invariant path is caught.
#[mz_ore::test]
fn loop_invariant_subterm_hoisted_out() {
    let input = rel_invariant_letrec();
    let result = eliminate_common_subexpressions(&input, &BTreeMap::new(), true);

    assert!(
        rel_has_let_wrapping_letrec(&result),
        "loop-invariant subterm must be bound in a Rel::Let outside the LetRec;\n\
         result={result:?}"
    );

    // Raise the result to MIR and verify it passes a strict Typecheck, catching
    // any raise regression on the invariant hoisting path.
    let ctx = empty_typechecking_context();
    let features = OptimizerFeatures::default();
    let mut df = DataflowMetainfo::default();
    let mut transform_ctx = TransformCtx::local(&features, &ctx, &mut df, None, None);
    let mut checked = raise::raise(
        &result,
        false,
        &BTreeMap::new(),
        raise::NativeJoinFlags::none(),
    );
    Typecheck::new(std::sync::Arc::clone(&ctx))
        .transform(&mut checked, &mut transform_ctx)
        .expect("raised plan must pass strict Typecheck");
}

// --------------------------------------------------------------------------
// Part 2: arrangement-count measurement.
//
// The lift's payoff is that a subterm built in two recursive bindings becomes a
// single shared binding, so the dataflow materializes its arrangements once
// rather than once per binding. Two counts make this visible:
//
// * The reuse-aware count is exactly what the `ArrangementCount` objective
//   compares (`Cost::arrangements`). The cost model deduplicates structurally
//   identical arrangements across the whole plan, so it already credits sharing:
//   the lift never raises this count, and on a fixture whose duplicate copies are
//   structurally identical it is unchanged. This is the `<=` guarantee the
//   objective relies on.
// * The physical count charges each arrangement-bearing operator once per
//   occurrence (no cross-tree dedup), matching the number of arrangements the
//   dataflow builds before CSE collapses shared operators. The lift strictly
//   reduces this on a fixture where a versioned, arrangement-bearing subterm is
//   shared across two bindings.
// --------------------------------------------------------------------------

/// A named `Rel` fixture and whether its shared subterm carries an arrangement.
struct WmrFixture {
    name: &'static str,
    rel: Rel,
}

/// Lower the Task 5 MIR fixture to a `Rel` with the given lift setting. Lowering
/// is the same on-path entry the optimizer uses, so the versioned references
/// match what the live pass produces.
fn lowered_shared_letrec(enable_wmr_lift: bool) -> Rel {
    mz_transform::eqsat::lower::lower_with(&shared_letrec(), enable_wmr_lift)
}

/// A `Rel::Join` over `Opaque(global gid)` and the recursive `LocalGet(b0, Cur)`,
/// equi-joined on column 0 of each. A join arranges both inputs, so this subterm
/// carries arrangements the cost model charges.
fn shared_recursive_join(gid: u64, b0: usize) -> Rel {
    Rel::Join {
        inputs: vec![opaque_global(gid), versioned_local(b0, RecVersion::Cur)],
        equivalences: vec![vec![
            EScalar::plain(MirScalarExpr::column(0)),
            EScalar::plain(MirScalarExpr::column(1)),
        ]],
    }
}

/// A `Rel::LetRec` whose two recursive bindings `b1`, `b2` both build the
/// identical `shared_recursive_join` over the common recursive binding `b0`,
/// mirroring the SQL differential's two bindings that both join `edges` against
/// the recursive `r`. The join reads a versioned recursive leaf, so the lift
/// hoists it into one new recursive binding rather than leaving a copy in each.
fn join_sharing_letrec() -> Rel {
    let b0 = 20usize;
    let b1 = 21usize;
    let b2 = 22usize;
    let gid = 77u64;

    let v0 = versioned_local(b0, RecVersion::Prev);
    let v1 = Rel::Threshold {
        input: Box::new(shared_recursive_join(gid, b0)),
    };
    let v2 = Rel::Negate {
        input: Box::new(shared_recursive_join(gid, b0)),
    };
    let body = Rel::Union {
        base: Box::new(versioned_local(b1, RecVersion::Final)),
        inputs: vec![versioned_local(b2, RecVersion::Final)],
    };
    Rel::LetRec {
        bindings: vec![(b0, v0), (b1, v1), (b2, v2)],
        limits: vec![None, None, None],
        body: Box::new(body),
    }
}

/// The fixtures reused from Tasks 5 and 6, plus the join-bearing fixture that
/// exercises the arrangement axis. `enable_wmr_lift` selects the lowering of the
/// Task 5 MIR fixture; the two hand-built `Rel` fixtures are independent of it.
fn wmr_fixtures(enable_wmr_lift: bool) -> Vec<WmrFixture> {
    vec![
        WmrFixture {
            name: "shared_letrec (Task 5)",
            rel: lowered_shared_letrec(enable_wmr_lift),
        },
        WmrFixture {
            name: "rel_invariant_letrec (Task 6)",
            rel: rel_invariant_letrec(),
        },
        WmrFixture {
            name: "join_sharing_letrec",
            rel: join_sharing_letrec(),
        },
    ]
}

/// The reuse-aware arrangement count the `ArrangementCount` objective compares:
/// run the lift's cross-binding CSE (gated on `enable_wmr_lift`) over `rel`, then
/// score the result with the same `CostModel` the objective consumes. This is the
/// `Cost::arrangements` field, which deduplicates structurally identical
/// arrangements across the plan.
fn arrangement_count_with(rel: &Rel, enable_wmr_lift: bool) -> usize {
    use mz_transform::eqsat::cost::CostModel;
    let hoisted = eliminate_common_subexpressions(rel, &BTreeMap::new(), enable_wmr_lift);
    CostModel::new().cost(&hoisted).arrangements
}

/// The physical arrangement count: charge one arrangement per arrangement-bearing
/// operator occurrence, with NO cross-tree deduplication. This matches the number
/// of arrangements the dataflow builds for separate operators, so sharing a
/// subterm (one operator instead of two) lowers it. The charged operators mirror
/// the cost model's `collect_memory`: a binary `Join`/`WcoJoin` arranges each
/// input, an `ArrangeBy`/`ArrangeByMany` arranges its input per key, and a
/// `Reduce`/`TopK` arranges its input.
fn physical_arrangement_count(rel: &Rel) -> usize {
    fn walk(rel: &Rel, count: &mut usize) {
        match rel {
            Rel::Join { inputs, .. } | Rel::WcoJoin { inputs, .. } => {
                *count += inputs.len();
            }
            Rel::ArrangeBy { .. } | Rel::Reduce { .. } | Rel::TopK { .. } => {
                *count += 1;
            }
            Rel::ArrangeByMany { keys, .. } => {
                *count += keys.len();
            }
            _ => {}
        }
        for c in rel.children() {
            walk(c, count);
        }
    }
    let mut count = 0;
    walk(rel, &mut count);
    count
}

/// Part D: the reuse-aware arrangement count (the `ArrangementCount` objective's
/// metric) never increases under the lift. The cost model already credits
/// sharing, so on these fixtures it is unchanged, satisfying the `<=` contract
/// the objective relies on.
#[mz_ore::test]
fn wmr_lift_does_not_increase_arrangement_count() {
    for fixture in wmr_fixtures(true) {
        let with_lift = arrangement_count_with(&fixture.rel, true);
        let without = arrangement_count_with(&fixture.rel, false);
        eprintln!(
            "{}: reuse-aware arrangements with-lift={with_lift} without={without}",
            fixture.name
        );
        assert!(
            with_lift <= without,
            "{}: lift must not increase the reuse-aware arrangement count \
             (with-lift={with_lift}, without={without})",
            fixture.name
        );
    }
}

/// Part D: the lift strictly reduces the PHYSICAL arrangement count on a fixture
/// whose shared subterm carries arrangements. Without the lift the
/// arrangement-bearing join sits inlined in two bindings, so the dataflow builds
/// its input arrangements twice; with the lift the join becomes one shared
/// binding and the arrangements are built once. This is the resource win the
/// reuse-aware metric predicts but does not itself display.
#[mz_ore::test]
fn wmr_lift_reduces_physical_arrangement_count() {
    let fixture = join_sharing_letrec();
    let with_lift = eliminate_common_subexpressions(&fixture, &BTreeMap::new(), true);
    let without = eliminate_common_subexpressions(&fixture, &BTreeMap::new(), false);

    let phys_with = physical_arrangement_count(&with_lift);
    let phys_without = physical_arrangement_count(&without);
    eprintln!(
        "join_sharing_letrec: physical arrangements with-lift={phys_with} without={phys_without}"
    );

    assert!(
        phys_with < phys_without,
        "lift must strictly reduce the physical arrangement count \
         (with-lift={phys_with}, without={phys_without}); the shared join \
         should be materialized once, not once per binding"
    );

    // The reuse-aware metric stays equal: the cost model already deduplicates the
    // two structurally identical joins, so it credits the sharing whether or not
    // the lift physically hoists it.
    let reuse_with = arrangement_count_with(&fixture, true);
    let reuse_without = arrangement_count_with(&fixture, false);
    eprintln!(
        "join_sharing_letrec: reuse-aware arrangements with-lift={reuse_with} without={reuse_without}"
    );
    assert_eq!(
        reuse_with, reuse_without,
        "reuse-aware count is dedup-based, so it is unchanged by physical sharing"
    );
}
