// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Arrangement-count invariant harness for the eqsat optimizer.
//!
//! ## Invariant
//!
//! For every corpus entry, the arrangement count of the eqsat-extracted plan
//! must not exceed the arrangement count of the un-saturated input plan.
//!
//! ## What `arrangements` measures
//!
//! `Cost::arrangements` counts the number of distinct persistent arrangements
//! charged by `collect_memory`, i.e. the size of the `seen` set. Each join
//! input arranged by its join key adds one `ArrId::JoinInput` entry; each
//! `ArrangeBy`/`Reduce`/`TopK` node adds one `ArrId::Node` entry. Binary-join
//! intermediate results are pushed only into the `memory` degree vector, NOT
//! into `seen`, so they do not raise the arrangement count.
//!
//! For the triangle join in particular:
//!
//! * `WcoJoin`: three `ArrId::JoinInput` entries (one per input) => `arrangements == 3`.
//! * Binary `Join`: three `ArrId::JoinInput` entries (the intermediates go into
//!   `memory` only) => `arrangements == 3`.
//!
//! Both join forms produce the same arrangement count for the triangle.
//! WcoJoin dominates on the `memory` degree axis (leading term 1.0 vs 2.0 for
//! the binary intermediate), and on the `time` axis (AGM 1.5 vs 2.0), but not
//! on `arrangements`, which is equal.
//!
//! ## What Proxy A catches
//!
//! Proxy A detects regressions where the extractor introduces SPURIOUS
//! arrangements: extra `ArrangeBy`/`Reduce`/`TopK` nodes or additional join
//! inputs versus the input plan. It does NOT detect a binary-vs-WcoJoin form
//! change (that difference lives on the `memory` degree axis, not the
//! `arrangements` count). The distinction between WcoJoin and binary join is
//! exercised by the `triangle_wcojoin_dominates` and related unit tests in
//! `cost.rs` and by the SLT goldens (end-to-end).
//!
//! ## Reference choice (documented proxy)
//!
//! The natural reference is the full production transform pipeline with eqsat
//! disabled. Invoking `Optimizer::physical_optimizer` from a unit test requires
//! a catalog/index context (a real `IndexOracle`) that the unit-test
//! infrastructure does not provide. The logical pipeline (`logical_optimizer`)
//! is reachable in-crate (see `wcoj_decision.rs`), but it does not run
//! `JoinImplementation`, so every join has zero `ArrangeBy` nodes in its
//! output: the arrangement-count comparison would be trivially satisfied but
//! vacuous for all join-heavy corpus entries.
//!
//! We therefore operate entirely at the `Rel` level (the e-graph's internal
//! representation) where the cost model is defined. This avoids the
//! physical-lowering asymmetry: `optimize_with_availability` with
//! `commit_wcoj=true` emits explicit `ArrangeBy` wrappers for differential
//! joins, so re-lowering the raised `MirRelationExpr` back to `Rel` charges
//! those arrangements again on top of the join's implicit ones, inflating the
//! cost in a way the input baseline (an un-committed join) never incurs.
//!
//! We use the following documented proxy:
//!
//! 1. **Proxy A (input baseline)**: lower the input `MirRelationExpr` to `Rel`
//!    (via `lower::lower`), saturate it with the e-graph optimizer, and compare
//!    the extracted plan's arrangement count against the un-saturated input's
//!    arrangement count:
//!    `cost(extracted_rel).arrangements <= cost(input_rel).arrangements`.
//!    Both sides are measured on the same `Rel`-level cost model, so the
//!    comparison is symmetric. A buggy rule that forces unnecessary binarization
//!    or wraps inputs in extra `ArrangeBy` nodes would fail this test.
//!
//! 2. **Proxy B (objective comparison)**: assert that the `ArrangementCount`
//!    objective never extracts a plan with more arrangements than the legacy
//!    `PeakDegree` objective on the same saturated e-graph. This proves the
//!    default objective does not regress on the primary metric it was designed
//!    to minimize.
//!
//!    On the current six-entry corpus, `ArrangementCount` and `PeakDegree`
//!    extract plans with equal arrangement counts for every entry (the join
//!    forms they prefer differ on the `memory` degree axis but not on the count
//!    of distinct persistent arrangements). Proxy B therefore holds as an
//!    equality and acts as a non-regression guard: it would fire if a bug in
//!    the `ArrangementCount` comparator caused it to extract a plan with more
//!    arrangements than `PeakDegree` would. The distinct value of the
//!    arrangement-count objective (preferring arrangements that can be shared,
//!    which would diverge from `PeakDegree`) is exercised by the
//!    `arrangement_count_dedups_and_credits_oracle` unit test in `cost.rs` and
//!    by the SLT goldens end-to-end.
//!
//! Both proxies are non-tautological: a buggy extractor that inflates
//! arrangements (e.g. always choosing the binarized form of a chain join) would
//! fail Proxy A. A regression in the `ArrangementCount` comparator causing it
//! to select a plan with more arrangements than `PeakDegree` would fail Proxy B.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use mz_expr::{
    AccessStrategy, AggregateExpr, AggregateFunc, BinaryFunc, Id, LetRecLimit, LocalId,
    MirRelationExpr, MirScalarExpr, TableFunc,
};
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};

use crate::eqsat::cost::CostModel;
use crate::eqsat::default_ruleset;
use crate::eqsat::engine::Optimizer;
use crate::eqsat::lower::lower;
use crate::eqsat::objective::{ArrangementCount, PeakDegree};
use crate::eqsat::raise::{NativeJoinFlags, raise};

/// Build a source relation with `arity` Int64 non-nullable columns and a
/// unique transient global id `id`.
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

/// A corpus entry: a name, the input MIR, and the index availability map.
struct CorpusEntry {
    name: &'static str,
    mir: MirRelationExpr,
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
}

/// Build an availability map with a single index key for a global transient id.
fn avail(id: u64, key_cols: &[usize]) -> BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> {
    let key: Vec<MirScalarExpr> = key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
    let mut m = BTreeMap::new();
    m.insert(GlobalId::Transient(id), vec![key]);
    m
}

/// Triangle join R(a,b) ⋈ S(b,c) ⋈ T(c,a): a cyclic join.
/// Column layout: R=#0,#1  S=#2,#3  T=#4,#5
/// Equivalences:  a:#0=#4  b:#1=#2  c:#3=#5
fn triangle() -> MirRelationExpr {
    let r = src(101, 2);
    let s = src(102, 2);
    let t = src(103, 2);
    MirRelationExpr::join_scalars(
        vec![r, s, t],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(5)],
        ],
    )
}

/// Triangle join with all three inputs indexed on their join keys.
fn triangle_indexed() -> (MirRelationExpr, BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>) {
    let mir = triangle();
    // R's join key: local cols {0,1} (a and b). S's: {0,1}. T's: {0,1}.
    let mut available = avail(101, &[0, 1]);
    available.extend(avail(102, &[0, 1]));
    available.extend(avail(103, &[0, 1]));
    (mir, available)
}

/// Two-way join R(a,b) ⋈ S(b,c) on R.b = S.b.
/// Column layout: R=#0,#1  S=#2,#3
/// Equivalence:   b:#1=#2
fn two_way() -> MirRelationExpr {
    let r = src(111, 2);
    let s = src(112, 2);
    MirRelationExpr::join_scalars(
        vec![r, s],
        vec![vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]],
    )
}

/// Two-way join with a reusable index on R's join key (local col 1, i.e. b).
fn two_way_with_index() -> (MirRelationExpr, BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>) {
    let mir = two_way();
    let available = avail(111, &[1]);
    (mir, available)
}

/// Acyclic 3-way chain join R(a,b) ⋈ S(b,c) ⋈ T(c,d).
/// Column layout: R=#0,#1  S=#2,#3  T=#4,#5
/// Equivalences:  b:#1=#2  c:#3=#4
fn chain() -> MirRelationExpr {
    let r = src(121, 2);
    let s = src(122, 2);
    let t = src(123, 2);
    MirRelationExpr::join_scalars(
        vec![r, s, t],
        vec![
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
        ],
    )
}

/// Reduce-over-join: a group-by count over the two-way join.
/// Groups on the join key column (#1) and counts.
fn reduce_over_join() -> MirRelationExpr {
    let join = two_way();
    MirRelationExpr::Reduce {
        input: Box::new(join),
        group_key: vec![MirScalarExpr::column(1)],
        aggregates: vec![AggregateExpr {
            func: AggregateFunc::Count,
            expr: MirScalarExpr::literal_true(),
            distinct: false,
        }],
        monotonic: false,
        expected_group_size: None,
    }
}

/// Build the full validation corpus.
fn corpus() -> Vec<CorpusEntry> {
    let (triangle_idx, avail_tri) = triangle_indexed();
    let (two_way_idx, avail_two) = two_way_with_index();
    vec![
        CorpusEntry {
            name: "triangle_no_indexes",
            mir: triangle(),
            available: BTreeMap::new(),
        },
        CorpusEntry {
            name: "triangle_all_indexes",
            mir: triangle_idx,
            available: avail_tri,
        },
        CorpusEntry {
            name: "two_way_no_index",
            mir: two_way(),
            available: BTreeMap::new(),
        },
        CorpusEntry {
            name: "two_way_with_index",
            mir: two_way_idx,
            available: avail_two,
        },
        CorpusEntry {
            name: "chain_join",
            mir: chain(),
            available: BTreeMap::new(),
        },
        CorpusEntry {
            name: "reduce_over_join",
            mir: reduce_over_join(),
            available: BTreeMap::new(),
        },
    ]
}

/// For each corpus entry, assert that the eqsat-extracted plan at the `Rel`
/// level has no more arrangements than the un-saturated input plan.
///
/// Both plans are measured by the same `CostModel` on the `Rel`
/// representation, avoiding the physical-lowering asymmetry (see module doc).
/// The input `Rel` is measured before saturation; the extracted `Rel` is the
/// result of the greedy `ArrangementCount` extractor after saturation.
#[mz_ore::test]
fn eqsat_never_increases_arrangement_count() {
    for entry in corpus() {
        let model = CostModel::with_available(entry.available.clone());
        let input_rel = lower(&entry.mir);

        // Reference: arrangement count of the un-saturated input plan.
        let ref_cost = model.cost(&input_rel);

        // Eqsat-extracted plan: saturate and extract with ArrangementCount.
        let outcome = Optimizer::new(default_ruleset(), model.clone())
            .with_objective(Arc::new(ArrangementCount))
            .optimize(input_rel.clone());
        let opt_cost = model.cost(&outcome.plan);

        assert!(
            opt_cost.arrangements <= ref_cost.arrangements,
            "{}: eqsat extracted {} arrangements > input {} arrangements\n\
             eqsat plan cost:  {:?}\n  input plan cost:  {:?}\n\
             eqsat plan:  {:#?}",
            entry.name,
            opt_cost.arrangements,
            ref_cost.arrangements,
            opt_cost,
            ref_cost,
            outcome.plan,
        );
    }
}

/// For each corpus entry, assert that the `ArrangementCount` objective extracts
/// a plan with no more arrangements than the legacy `PeakDegree` objective on
/// the same saturated e-graph.
///
/// On the current corpus the two objectives agree on arrangement count for
/// every entry (they differ on the `memory` degree axis, not on the count of
/// distinct persistent arrangements). The assertion therefore holds as an
/// equality and functions as a non-regression guard: a bug in the
/// `ArrangementCount` comparator that caused it to select a plan with more
/// arrangements than `PeakDegree` would break this test.
///
/// Both objectives are exercised on the same input `Rel` by constructing two
/// optimizers that differ only in their objective.
#[mz_ore::test]
fn arrangement_count_no_worse_than_peak_degree() {
    for entry in corpus() {
        let model = CostModel::with_available(entry.available.clone());
        let input_rel = lower(&entry.mir);

        let opt_arr = Optimizer::new(default_ruleset(), model.clone())
            .with_objective(Arc::new(ArrangementCount))
            .optimize(input_rel.clone());
        let opt_peak = Optimizer::new(default_ruleset(), model.clone())
            .with_objective(Arc::new(PeakDegree))
            .optimize(input_rel.clone());

        let cost_arr = model.cost(&opt_arr.plan);
        let cost_peak = model.cost(&opt_peak.plan);

        assert!(
            cost_arr.arrangements <= cost_peak.arrangements,
            "{}: ArrangementCount extracted {} arrangements > PeakDegree {} arrangements\n\
             ArrangementCount cost: {:?}\n  PeakDegree cost: {:?}",
            entry.name,
            cost_arr.arrangements,
            cost_peak.arrangements,
            cost_arr,
            cost_peak,
        );
    }
}

/// Build a simple LetRec with one binding: `LetRec { id=0, value, limits=[None], body }`.
fn simple_letrec(value: MirRelationExpr, body: MirRelationExpr) -> MirRelationExpr {
    MirRelationExpr::LetRec {
        ids: vec![LocalId::new(0)],
        values: vec![value],
        limits: vec![None],
        body: Box::new(body),
    }
}

/// Build a `Get { Id::Local(LocalId::new(0)) }` back-edge of the given arity.
fn local_get(arity: usize) -> MirRelationExpr {
    let typ = ReprRelationType::new(
        (0..arity)
            .map(|_| ReprScalarType::Int64.nullable(false))
            .collect(),
    );
    MirRelationExpr::Get {
        id: Id::Local(LocalId::new(0)),
        typ,
        access_strategy: AccessStrategy::UnknownOrLocal,
    }
}

/// Walk `expr` and collect every `Id::Local` referenced by a `Get` that is
/// not bound by any enclosing `LetRec` or `Let`. An empty result means no
/// dangling back-edges.
fn dangling_locals(expr: &MirRelationExpr) -> Vec<LocalId> {
    fn walk(expr: &MirRelationExpr, bound: &mut Vec<LocalId>, dangling: &mut Vec<LocalId>) {
        match expr {
            MirRelationExpr::Get {
                id: Id::Local(lid), ..
            } => {
                if !bound.contains(lid) && !dangling.contains(lid) {
                    dangling.push(*lid);
                }
            }
            MirRelationExpr::Let { id, value, body } => {
                walk(value, bound, dangling);
                bound.push(*id);
                walk(body, bound, dangling);
                bound.retain(|x| x != id);
            }
            MirRelationExpr::LetRec {
                ids, values, body, ..
            } => {
                let prev_len = bound.len();
                bound.extend_from_slice(ids);
                for v in values {
                    walk(v, bound, dangling);
                }
                walk(body, bound, dangling);
                bound.truncate(prev_len);
            }
            other => {
                for child in other.children() {
                    walk(child, bound, dangling);
                }
            }
        }
    }
    let mut bound = Vec::new();
    let mut dangling = Vec::new();
    walk(expr, &mut bound, &mut dangling);
    dangling
}

/// Run the full end-to-end eqsat pipeline on `mir`: lower -> optimize ->
/// raise. Returns the raised plan.
fn e2e_optimize(mir: MirRelationExpr) -> MirRelationExpr {
    let model = CostModel::new();
    let input_rel = lower(&mir);
    let outcome = Optimizer::new(default_ruleset(), model)
        .with_objective(Arc::new(ArrangementCount))
        .optimize(input_rel);
    raise(
        &outcome.plan,
        false,
        &BTreeMap::new(),
        NativeJoinFlags::none(),
    )
}

/// End-to-end test: eqsat optimizes a fragment inside a LetRec body.
///
/// The body contains `negate(negate(src))`, which `negate_negate` rewrites to
/// `src`. We assert: (a) the result is still a LetRec; (b) the body no longer
/// contains a double negation; (c) the single binding and its `None` limit are
/// unchanged; (d) no dangling local references.
#[mz_ore::test]
fn letrec_body_fragment_is_optimized() {
    // Binding: src(1, 1). Not self-referential; we just need a non-trivial value.
    // Body: negate(negate(src(2, 1))). The negate_negate rule fires here.
    let value = src(1, 1);
    let body = src(2, 1).negate().negate();
    let mir = simple_letrec(value, body);

    let optimized = e2e_optimize(mir);

    // (a) Must still be a LetRec.
    let (ids, values, limits, opt_body) = match &optimized {
        MirRelationExpr::LetRec {
            ids,
            values,
            limits,
            body,
        } => (ids, values, limits, body.as_ref()),
        other => panic!("expected LetRec after optimization, got: {other:?}"),
    };

    // (b) Body must not contain any Negate (the double negation was eliminated).
    let has_negate = {
        let mut found = false;
        opt_body.visit_pre(|e| {
            if matches!(e, MirRelationExpr::Negate { .. }) {
                found = true;
            }
        });
        found
    };
    assert!(
        !has_negate,
        "body still contains Negate after negate_negate should have fired; body: {opt_body:?}"
    );

    // (c) One binding, id=0, limit=None.
    assert_eq!(ids.len(), 1, "expected one binding, got {}", ids.len());
    assert_eq!(
        ids[0],
        LocalId::new(0),
        "binding id must be 0, got {:?}",
        ids[0]
    );
    assert_eq!(limits, &vec![None], "limit must be None, got {:?}", limits);

    // The binding value's content is intentionally not asserted: the optimizer
    // may freely rewrite the non-recursive binding value, and any dangling
    // reference it could introduce is caught by the dangling-local check below.
    let _ = values;

    // (d) No dangling locals.
    let dangling = dangling_locals(&optimized);
    assert!(
        dangling.is_empty(),
        "optimized plan has dangling local references: {dangling:?}\n  plan: {optimized:?}"
    );
}

/// End-to-end test: a unary operator over a LetRec is pushed into the body.
///
/// `normalize_push_into_scopes` rewrites `Negate(LetRec ...)` into
/// `LetRec { ..., body = Negate(body) }`. The result must still be a LetRec
/// with recursion intact, matching limits, and no dangling locals.
#[mz_ore::test]
fn letrec_under_unary_operator() {
    // A minimal self-referential LetRec: value = local_get(0) union src(1,1),
    // body = local_get(0). This exercises the recursive back-edge path.
    let rec = local_get(1);
    let value = MirRelationExpr::Union {
        base: Box::new(rec.clone()),
        inputs: vec![src(1, 1)],
    };
    let letrec = simple_letrec(value, rec);
    // Wrap with Negate, a unary operator.
    let mir = letrec.negate();

    let optimized = e2e_optimize(mir);

    // Must still be a LetRec (the Negate was pushed in, not dropped).
    let (ids, limits, opt_body) = match &optimized {
        MirRelationExpr::LetRec {
            ids, limits, body, ..
        } => (ids, limits, body.as_ref()),
        other => panic!("expected LetRec after optimization, got: {other:?}"),
    };

    // One binding, id=0, limit=None.
    assert_eq!(ids.len(), 1);
    assert_eq!(ids[0], LocalId::new(0));
    assert_eq!(limits, &vec![None]);

    // The pushed-in Negate must be the outermost node of the body:
    // `normalize_push_into_scopes` rewrites `Negate(LetRec x = v in b)` to
    // `LetRec x = v in Negate(b)`, so the body's root is the Negate. Pinning the
    // position (not just "a Negate exists somewhere") keeps the test from passing
    // vacuously if a future rule moved the Negate elsewhere.
    assert!(
        matches!(opt_body, MirRelationExpr::Negate { .. }),
        "body root should be the pushed-in Negate; body: {opt_body:?}"
    );

    // No dangling locals.
    let dangling = dangling_locals(&optimized);
    assert!(
        dangling.is_empty(),
        "optimized plan has dangling local references: {dangling:?}\n  plan: {optimized:?}"
    );
}

/// End-to-end test: a LetRec appearing as one input of a Union exercises
/// `optimize_around_scopes` (hoist, optimize the surrounding fragment, splice
/// back). The result must still contain a LetRec with recursion intact,
/// matching limits, and no dangling locals.
#[mz_ore::test]
fn letrec_as_union_input() {
    // LetRec: value = local_get(0) union src(1,1), body = local_get(0).
    let rec = local_get(1);
    let value = MirRelationExpr::Union {
        base: Box::new(rec.clone()),
        inputs: vec![src(1, 1)],
    };
    let letrec = simple_letrec(value, rec);

    // Union(src(2,1), LetRec ...): the LetRec is an input to a binary Union.
    let mir = MirRelationExpr::Union {
        base: Box::new(src(2, 1)),
        inputs: vec![letrec],
    };

    let optimized = e2e_optimize(mir);

    // The result must still contain a LetRec somewhere (the recursion survived),
    // and that LetRec must still carry the recursive back-edge `Get(Local(0))`.
    // Checking the back-edge explicitly (not just "a LetRec exists") guards
    // against the recursion being silently erased: a LetRec whose self-reference
    // was dropped would be emitted as a plain `Let`, but asserting the back-edge
    // directly keeps the test honest if the assembler ever changes.
    let mut found_letrec = false;
    optimized.visit_pre(|e| {
        if let MirRelationExpr::LetRec {
            ids,
            values,
            limits,
            body,
        } = e
        {
            found_letrec = true;
            assert_eq!(ids.len(), 1, "expected one binding in inner LetRec");
            assert_eq!(ids[0], LocalId::new(0));
            assert_eq!(limits, &vec![None]);

            let mut has_back_edge = false;
            let mut check = |sub: &MirRelationExpr| {
                sub.visit_pre(|n| {
                    if matches!(
                        n,
                        MirRelationExpr::Get {
                            id: Id::Local(l),
                            ..
                        } if *l == LocalId::new(0)
                    ) {
                        has_back_edge = true;
                    }
                });
            };
            for v in values {
                check(v);
            }
            check(body);
            assert!(
                has_back_edge,
                "LetRec lost its recursive back-edge Get(Local(0)); node: {e:?}"
            );
        }
    });
    assert!(
        found_letrec,
        "expected a LetRec in the optimized plan; got: {optimized:?}"
    );

    // No dangling locals.
    let dangling = dangling_locals(&optimized);
    assert!(
        dangling.is_empty(),
        "optimized plan has dangling local references: {dangling:?}\n  plan: {optimized:?}"
    );
}

/// End-to-end test: a mutually-recursive LetRec with two bindings survives
/// optimization with binding order and per-binding limits preserved.
///
/// Bindings: id=0 references id=1, id=1 references id=0 (mutual recursion).
/// limits=[Some(5 iters, no error), None]. After optimization the result must
/// be a LetRec with ids [0, 1] in order, limits unchanged, both back-edges
/// present, and no dangling locals.
#[mz_ore::test]
fn letrec_mutual_two_bindings() {
    // Build the shared type: one Int64 column.
    let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);

    // value0 = Get(Local(1)): references the second binding.
    let value0 = MirRelationExpr::Get {
        id: Id::Local(LocalId::new(1)),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    };

    // value1 = Get(Local(0)): references the first binding.
    let value1 = MirRelationExpr::Get {
        id: Id::Local(LocalId::new(0)),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    };

    // body = Get(Local(0)): returns the first binding.
    let body = MirRelationExpr::Get {
        id: Id::Local(LocalId::new(0)),
        typ: typ.clone(),
        access_strategy: AccessStrategy::UnknownOrLocal,
    };

    let limit0 = Some(LetRecLimit {
        max_iters: std::num::NonZeroU64::new(5).unwrap(),
        return_at_limit: false,
    });
    let limits_input = vec![limit0.clone(), None];

    let mir = MirRelationExpr::LetRec {
        ids: vec![LocalId::new(0), LocalId::new(1)],
        values: vec![value0, value1],
        limits: limits_input.clone(),
        body: Box::new(body),
    };

    let optimized = e2e_optimize(mir);

    // Must be a LetRec with ids [0, 1] in order and limits unchanged.
    let MirRelationExpr::LetRec {
        ids,
        values,
        limits,
        body,
    } = &optimized
    else {
        panic!("expected LetRec after optimization, got: {optimized:?}");
    };

    assert_eq!(
        ids,
        &vec![LocalId::new(0), LocalId::new(1)],
        "binding ids must be [0, 1] in order"
    );
    assert_eq!(
        limits, &limits_input,
        "per-binding limits must be preserved"
    );

    // Both back-edges must still be present somewhere in values or body.
    let mut has_get0 = false;
    let mut has_get1 = false;
    let mut check = |sub: &MirRelationExpr| {
        sub.visit_pre(|n| match n {
            MirRelationExpr::Get {
                id: Id::Local(l), ..
            } if *l == LocalId::new(0) => has_get0 = true,
            MirRelationExpr::Get {
                id: Id::Local(l), ..
            } if *l == LocalId::new(1) => has_get1 = true,
            _ => {}
        });
    };
    for v in values {
        check(v);
    }
    check(body);
    assert!(
        has_get0,
        "back-edge Get(Local(0)) missing from optimized plan"
    );
    assert!(
        has_get1,
        "back-edge Get(Local(1)) missing from optimized plan"
    );

    // No dangling locals.
    let dangling = dangling_locals(&optimized);
    assert!(
        dangling.is_empty(),
        "optimized plan has dangling local references: {dangling:?}\n  plan: {optimized:?}"
    );
}

/// End-to-end test: a filter over a FlatMap is pushed below the FlatMap when
/// the predicate reads only the input's columns.
///
/// `Filter[col(0) = 5](FlatMap(GenerateSeriesInt64, [col(0), col(1), lit 1], src(1, 2)))`:
/// the predicate `#0 = 5` references only input column 0 (input arity 2), so
/// `push_filter_past_flatmap` must sink it below the FlatMap. The raised plan
/// must be `FlatMap { input: Filter { input: Get }, .. }`, not
/// `Filter { input: FlatMap { .. } }`.
#[mz_ore::test]
fn push_filter_past_flatmap_fires() {
    use mz_repr::{Datum, ReprScalarType};
    let input = src(1, 2);
    let flatmap = MirRelationExpr::FlatMap {
        input: Box::new(input),
        func: TableFunc::GenerateSeriesInt64,
        exprs: vec![
            MirScalarExpr::column(0),
            MirScalarExpr::column(1),
            MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
        ],
    };
    // Predicate reads only input column 0 (index < input arity 2).
    let pred = MirScalarExpr::column(0).call_binary(
        MirScalarExpr::literal_ok(Datum::Int64(5), ReprScalarType::Int64),
        BinaryFunc::Eq(mz_expr::func::Eq),
    );
    let mir = flatmap.filter(vec![pred]);

    let optimized = e2e_optimize(mir);

    // After pushing, the root must be a FlatMap whose input subtree contains the Filter.
    // There must be no Filter directly wrapping the outermost FlatMap.
    let root_is_flatmap = matches!(&optimized, MirRelationExpr::FlatMap { .. });
    assert!(
        root_is_flatmap,
        "push_filter_past_flatmap did not fire: expected FlatMap at root, got: {optimized:#?}"
    );
    // The FlatMap's input must be or contain a Filter over the Get.
    if let MirRelationExpr::FlatMap { input, .. } = &optimized {
        let has_filter_in_input = {
            let mut found = false;
            input.visit_pre(|e| {
                if matches!(e, MirRelationExpr::Filter { .. }) {
                    found = true;
                }
            });
            found
        };
        assert!(
            has_filter_in_input,
            "filter was not pushed into FlatMap's input subtree; input: {input:#?}"
        );
    }
}

/// End-to-end test: a filter over a FlatMap is NOT pushed when the predicate
/// reads a func-output column (index >= input arity).
///
/// `Filter[col(2) = 5](FlatMap(GenerateSeriesInt64, ...))`: column 2 is the
/// first func-output column (input arity is 2), so `uses_only_input` must
/// block the push. The raised plan must keep the Filter above the FlatMap.
#[mz_ore::test]
fn push_filter_past_flatmap_blocked_on_func_column() {
    use mz_repr::{Datum, ReprScalarType};
    let input = src(1, 2);
    let flatmap = MirRelationExpr::FlatMap {
        input: Box::new(input),
        func: TableFunc::GenerateSeriesInt64,
        exprs: vec![
            MirScalarExpr::column(0),
            MirScalarExpr::column(1),
            MirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
        ],
    };
    // Predicate reads func-output column 2 (index >= input arity 2): must not push.
    let pred = MirScalarExpr::column(2).call_binary(
        MirScalarExpr::literal_ok(Datum::Int64(5), ReprScalarType::Int64),
        BinaryFunc::Eq(mz_expr::func::Eq),
    );
    let mir = flatmap.filter(vec![pred]);

    let optimized = e2e_optimize(mir);

    // The filter must remain above the FlatMap (root is Filter wrapping FlatMap).
    let root_is_filter_over_flatmap = match &optimized {
        MirRelationExpr::Filter { input, .. } => {
            matches!(input.as_ref(), MirRelationExpr::FlatMap { .. })
        }
        _ => false,
    };
    assert!(
        root_is_filter_over_flatmap,
        "push_filter_past_flatmap incorrectly pushed a func-column predicate; \
         expected Filter(FlatMap), got: {optimized:#?}"
    );
}

/// Tracked (non-gating) microbenchmark: saturation + extraction wall time for
/// each corpus entry. Prints timing for tracking; does not gate on performance.
/// Run with `bin/cargo-test -p mz-transform -- --ignored timing_microbenchmark -- --nocapture` to see output.
///
/// Perf is deferred per spec; this records a baseline for future comparison.
#[mz_ore::test]
#[ignore]
fn timing_microbenchmark() {
    let n_iters = 50;

    println!("\n=== Eqsat saturation+extraction microbenchmark ({n_iters} iterations) ===");
    for entry in corpus() {
        let model = CostModel::with_available(entry.available.clone());
        let input_rel = lower(&entry.mir);
        let t0 = Instant::now();
        for _ in 0..n_iters {
            let _ = Optimizer::new(default_ruleset(), model.clone())
                .with_objective(Arc::new(ArrangementCount))
                .optimize(input_rel.clone());
        }
        let elapsed = t0.elapsed();
        let mean_us = elapsed.as_micros() / u128::try_from(n_iters).expect("n_iters fits u128");
        println!(
            "  {:30} mean {:6} µs  ({} iters, {:.2}ms total)",
            entry.name,
            mean_us,
            n_iters,
            elapsed.as_secs_f64() * 1000.0,
        );
    }
    println!();
}
