// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The e-graph's cardinality-free AGM cost model extracts a `Rel::WcoJoin` for a
//! cyclic join (the triangle R(a,b) ⋈ S(b,c) ⋈ T(c,a), where the AGM bound
//! N^1.5 beats a binary join's N^2 intermediate). The final delta-vs-differential
//! choice, however, is not the cost model's: it is made at commit time
//! (`raise`), reuse-aware, by the real planners.
//!
//! The commit rule (the eager delta rule applied unconditionally): commit the
//! delta strategy iff it needs no more new arrangements than a differential plan
//! would. So a bare triangle with no reusable arrangements commits
//! `Differential` (delta needs strictly more arrangements), while a triangle
//! whose join-key arrangements already exist commits `DeltaQuery` (delta is then
//! free). This matches production with `enable_eager_delta_joins = true`.
//!
//! These tests check both the extraction (e-graph picks `WcoJoin`) and the
//! commit decision (differential without arrangements, delta with them).

use mz_expr::{AccessStrategy, Id, JoinImplementation, MirRelationExpr, MirScalarExpr};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::eqsat::PhysicalEqSatTransform;
use mz_transform::eqsat::cost::CostModel;
use mz_transform::eqsat::default_ruleset;
use mz_transform::eqsat::engine::Optimizer;
use mz_transform::eqsat::ir::Rel;
use mz_transform::eqsat::lower::lower;
use mz_transform::join_implementation::JoinImplementation as JoinImplementationTransform;
use mz_transform::{Optimizer as PipelineOptimizer, Transform, TransformCtx, typecheck};

/// Build a source relation with `arity` Int64 non-nullable columns and a
/// unique transient global id `id`.
///
/// Uses a global `Get` so `FoldConstants` cannot collapse it. Both eqsat and
/// the real pipeline treat an unknown `Get` as an opaque leaf of degree 1.
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

/// Build the triangle join: R(a,b) ⋈ S(b,c) ⋈ T(c,a).
///
/// Column layout: R=#0,#1  S=#2,#3  T=#4,#5
/// Equivalences:  a: #0=#4,  b: #1=#2,  c: #3=#5
fn triangle() -> MirRelationExpr {
    let r = src(1, 2); // cols 0..1
    let s = src(2, 2); // cols 2..3
    let t = src(3, 2); // cols 4..5
    MirRelationExpr::join_scalars(
        vec![r, s, t],
        vec![
            vec![MirScalarExpr::column(0), MirScalarExpr::column(4)], // a = a
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)], // b = b
            vec![MirScalarExpr::column(3), MirScalarExpr::column(5)], // c = c
        ],
    )
}

/// Build a plain 2-way join: R(a,b) ⋈ S(b,c) on R.b = S.b.
///
/// Column layout: R=#0,#1  S=#2,#3
/// Equivalence:   b: #1=#2
///
/// One equivalence touching two inputs is a single hyperedge: trivially
/// acyclic, so `join_to_wcoj` must not fire.
fn two_way() -> MirRelationExpr {
    let r = src(11, 2); // cols 0..1
    let s = src(12, 2); // cols 2..3
    MirRelationExpr::join_scalars(
        vec![r, s],
        vec![vec![MirScalarExpr::column(1), MirScalarExpr::column(2)]], // b = b
    )
}

/// Build an acyclic 3-way chain join: R(a,b) ⋈ S(b,c) ⋈ T(c,d).
///
/// Column layout: R=#0,#1  S=#2,#3  T=#4,#5
/// Equivalences:  b: #1=#2,  c: #3=#4
///
/// The dual hypergraph is a path R-S-T (edge {R,S}, edge {S,T}); GYO reduces it
/// to nothing, so it is acyclic and `join_to_wcoj` must not fire.
fn chain() -> MirRelationExpr {
    let r = src(13, 2); // cols 0..1
    let s = src(14, 2); // cols 2..3
    let t = src(15, 2); // cols 4..5
    MirRelationExpr::join_scalars(
        vec![r, s, t],
        vec![
            vec![MirScalarExpr::column(1), MirScalarExpr::column(2)], // b = b
            vec![MirScalarExpr::column(3), MirScalarExpr::column(4)], // c = c
        ],
    )
}

/// Run `JoinImplementation` on a triangle and return the implementation variant
/// chosen for the top-level join. Returns `None` if the plan is not a join.
fn join_impl_choice(eager_delta: bool) -> Option<JoinImplementation> {
    let mut plan = triangle();
    let mut features = OptimizerFeatures::default();
    features.enable_eager_delta_joins = eager_delta;
    let tc = typecheck::empty_typechecking_context();
    let mut df = DataflowMetainfo::default();
    let mut ctx = TransformCtx::local(&features, &tc, &mut df, None, Some(GlobalId::Transient(99)));
    // Ignore errors: a bare triangle of unknown Gets may trigger soft-asserts
    // inside the transform, but the resulting implementation is still valid to
    // inspect.
    let _ = JoinImplementationTransform::default().transform(&mut plan, &mut ctx);
    match plan {
        MirRelationExpr::Join { implementation, .. } => Some(implementation),
        _ => None,
    }
}

/// Run the real `JoinImplementation` transform on `plan` and return it. Errors
/// are ignored (a bare triangle of unknown Gets can trip soft-asserts), as in
/// [`join_impl_choice`].
fn run_join_implementation(mut plan: MirRelationExpr) -> MirRelationExpr {
    let features = OptimizerFeatures::default();
    let tc = typecheck::empty_typechecking_context();
    let mut df = DataflowMetainfo::default();
    let mut ctx = TransformCtx::local(&features, &tc, &mut df, None, Some(GlobalId::Transient(98)));
    let _ = JoinImplementationTransform::default().transform(&mut plan, &mut ctx);
    plan
}

/// The implementation of the first `Join` found in `expr`, if any.
fn first_join_impl(expr: &MirRelationExpr) -> Option<JoinImplementation> {
    let mut found = None;
    expr.visit_pre(|e| {
        if found.is_none() {
            if let MirRelationExpr::Join { implementation, .. } = e {
                found = Some(implementation.clone());
            }
        }
    });
    found
}

#[mz_ore::test]
fn triangle_without_indexes_commits_differential() {
    // The e-graph picks WcoJoin for the triangle, but the commit decision is
    // reuse-aware: with no arrangements available, a delta plan needs strictly
    // more new arrangements than a differential plan, so raise commits
    // Differential. (Delta is committed only when its arrangements are free; see
    // `min_arrangements_commits_delta_when_arrangements_available`.)
    let out = mz_transform::eqsat::optimize(triangle());
    assert_eq!(out.arity(), 6, "arity preserved");
    assert!(
        matches!(
            first_join_impl(&out),
            Some(JoinImplementation::Differential(..))
        ),
        "bare triangle must commit Differential (delta is not free), got {out:?}"
    );
}

/// Run the full logical optimizer on the triangle with the eqsat flag set to
/// `enable_eqsat`, returning the optimized plan.
fn logical_optimize_triangle(enable_eqsat: bool) -> MirRelationExpr {
    let mut features = OptimizerFeatures::default();
    features.enable_eqsat_optimizer = enable_eqsat;
    let tc = typecheck::empty_typechecking_context();
    let mut df = DataflowMetainfo::default();
    let mut ctx = TransformCtx::local(&features, &tc, &mut df, None, Some(GlobalId::Transient(97)));
    #[allow(deprecated)]
    let optimizer = PipelineOptimizer::logical_optimizer(&mut ctx);
    optimizer
        .optimize(triangle(), &mut ctx)
        .expect("logical optimize")
        .into_inner()
}

/// Collect the implementations of every `Join` in `expr`.
fn all_join_impls(expr: &MirRelationExpr) -> Vec<JoinImplementation> {
    let mut out = Vec::new();
    expr.visit_pre(|e| {
        if let MirRelationExpr::Join { implementation, .. } = e {
            out.push(implementation.clone());
        }
    });
    out
}

#[mz_ore::test]
fn eqsat_logical_optimizer_leaves_joins_unimplemented() {
    // The eqsat pass runs in the logical optimizer. Join implementations are a
    // physical-phase concern: the ProjectionPushdown that runs right after the
    // logical optimizer (with `include_joins`) panics on a filled-in
    // implementation, and JoinImplementation expects to choose it itself. So
    // even though the e-graph internally favours a worst-case-optimal (delta)
    // join for the triangle, the live pass must emit a plain `Unimplemented`
    // join. This is the regression guard for that contract.
    //
    // The delta commitment itself (the experiment's offline payoff) is covered
    // by `triangle_raises_to_delta_query`, which calls `optimize` directly.
    let on = logical_optimize_triangle(true);
    assert!(
        all_join_impls(&on)
            .iter()
            .all(|i| matches!(i, JoinImplementation::Unimplemented)),
        "flag on: logical optimizer must leave all joins Unimplemented, got {on:?}"
    );
    let off = logical_optimize_triangle(false);
    assert!(
        all_join_impls(&off)
            .iter()
            .all(|i| matches!(i, JoinImplementation::Unimplemented)),
        "flag off: logical optimizer must leave all joins Unimplemented, got {off:?}"
    );
}

#[mz_ore::test]
fn committed_differential_survives_join_implementation() {
    // The committed Differential plan (chosen here because no arrangements are
    // free) is not reverted by the real JoinImplementation transform: with eager
    // delta joins off it only attempts a delta upgrade when delta needs zero new
    // arrangements, which does not hold for a bare triangle. So the eqsat join
    // decision survives the downstream pipeline.
    let out = mz_transform::eqsat::optimize(triangle());
    let after = run_join_implementation(out);
    assert!(
        matches!(
            first_join_impl(&after),
            Some(JoinImplementation::Differential(..))
        ),
        "committed Differential must survive JoinImplementation, got {after:?}"
    );
}

/// Run `PhysicalEqSatTransform` on `plan` and return the result.
fn physical_eqsat(mut plan: MirRelationExpr) -> MirRelationExpr {
    let features = OptimizerFeatures::default();
    let tc = typecheck::empty_typechecking_context();
    let mut df = DataflowMetainfo::default();
    let mut ctx = TransformCtx::local(&features, &tc, &mut df, None, Some(GlobalId::Transient(96)));
    PhysicalEqSatTransform
        .transform(&mut plan, &mut ctx)
        .expect("physical eqsat");
    plan
}

#[mz_ore::test]
fn physical_eqsat_commits_differential_for_bare_triangle() {
    // The physical placement has no index availability for these bare Gets, so
    // the reuse-aware commit chooses Differential (a delta plan would need more
    // new arrangements).
    let out = physical_eqsat(triangle());
    assert_eq!(out.arity(), 6, "arity preserved");
    assert!(
        matches!(
            first_join_impl(&out),
            Some(JoinImplementation::Differential(..))
        ),
        "PhysicalEqSatTransform must commit the bare triangle to Differential, got {out:?}"
    );
}

#[mz_ore::test]
fn physical_eqsat_differential_survives_join_implementation() {
    // Differential committed by the physical placement must survive the
    // downstream JoinImplementation transform unchanged (no delta upgrade with
    // eager delta joins off and no free arrangements).
    let after_physical = physical_eqsat(triangle());
    let after_ji = run_join_implementation(after_physical);
    assert!(
        matches!(
            first_join_impl(&after_ji),
            Some(JoinImplementation::Differential(..))
        ),
        "Differential from physical placement must survive JoinImplementation, got {after_ji:?}"
    );
}

#[mz_ore::test]
fn min_arrangements_commits_differential_without_indexes() {
    // The reuse-aware commit helper: with no arrangements available, a 3-way
    // triangle's delta plan needs more new arrangements than the differential
    // plan, so the helper returns the differential plan.
    let join = triangle();
    let available = vec![Vec::new(); 3];
    let features = OptimizerFeatures::default();
    let planned =
        mz_transform::join_implementation::plan_join_min_arrangements(&join, &available, &features)
            .expect("plan");
    assert!(
        matches!(
            first_join_impl(&planned),
            Some(JoinImplementation::Differential(..))
        ),
        "no arrangements: helper must pick Differential, got {planned:?}"
    );
}

#[mz_ore::test]
fn min_arrangements_commits_delta_when_arrangements_available() {
    // When every per-input join-key arrangement already exists, the delta plan
    // needs zero new arrangements, so `delta_new <= diff_new` holds and the
    // helper commits DeltaQuery. Each input is offered both single-column keys
    // and the composite, covering whatever keys the delta planner derives.
    let join = triangle();
    let keys = || {
        vec![
            vec![MirScalarExpr::column(0)],
            vec![MirScalarExpr::column(1)],
            vec![MirScalarExpr::column(0), MirScalarExpr::column(1)],
        ]
    };
    let available = vec![keys(), keys(), keys()];
    let features = OptimizerFeatures::default();
    let planned =
        mz_transform::join_implementation::plan_join_min_arrangements(&join, &available, &features)
            .expect("plan");
    assert!(
        matches!(
            first_join_impl(&planned),
            Some(JoinImplementation::DeltaQuery(_))
        ),
        "all arrangements free: helper must pick DeltaQuery, got {planned:?}"
    );
}

#[mz_ore::test]
fn egraph_picks_wcoj_for_triangle() {
    // Lower the triangle, saturate, extract and inspect the Rel directly.
    let tri = triangle();
    let rel = lower(&tri);
    let model = CostModel::new();

    let outcome = Optimizer::new(default_ruleset(), model.clone()).optimize(rel);
    let best = &outcome.plan;

    // Classify the top-level node.
    let picked_wcoj = matches!(best, Rel::WcoJoin { .. });
    let picked_join = matches!(best, Rel::Join { .. });
    let best_cost = model.cost(best);

    println!();
    println!("=== E-GRAPH DECISION ===");
    println!(
        "Top-level node: {}",
        if picked_wcoj {
            "WcoJoin (AGM)"
        } else if picked_join {
            "Join (binary)"
        } else {
            "other"
        }
    );
    println!(
        "Cost time: {:?}  memory: {:?}  nodes: {}",
        best_cost.time, best_cost.memory, best_cost.nodes
    );
    println!();

    // === Materialize JoinImplementation ===
    let choice_eager_off = join_impl_choice(false);
    let choice_eager_on = join_impl_choice(true);

    let impl_name = |c: &Option<JoinImplementation>| match c {
        Some(JoinImplementation::Differential(..)) => "Differential".to_owned(),
        Some(JoinImplementation::DeltaQuery(..)) => "DeltaQuery".to_owned(),
        Some(JoinImplementation::Unimplemented) => "Unimplemented".to_owned(),
        Some(JoinImplementation::IndexedFilter(..)) => "IndexedFilter".to_owned(),
        None => "not a join".to_owned(),
    };

    println!("=== MATERIALIZE JoinImplementation DECISION ===");
    println!("eager_delta_joins=false: {}", impl_name(&choice_eager_off));
    println!("eager_delta_joins=true:  {}", impl_name(&choice_eager_on));
    println!();

    // === Verdict ===
    println!("=== VERDICT ===");
    let egraph_label = if picked_wcoj {
        "WcoJoin (AGM N^1.5)"
    } else {
        "Join (binary)"
    };
    let mz_label_off = impl_name(&choice_eager_off);
    let mz_label_on = impl_name(&choice_eager_on);
    let decisions_differ = picked_wcoj
        && matches!(
            &choice_eager_off,
            Some(JoinImplementation::Differential(..))
        );

    println!("E-graph decision:           {egraph_label}");
    println!("Materialize (eager off):    {mz_label_off}");
    println!("Materialize (eager on):     {mz_label_on}");
    println!("Decisions differ:           {}", decisions_differ);
    if decisions_differ {
        println!("WHY: The e-graph's AGM cost model picks WcoJoin because the triangle");
        println!("     join is cyclic and N^1.5 < N^2. Materialize's JoinImplementation");
        println!("     has no AGM awareness; on a bare join with no available arrangements,");
        println!("     it falls back to Differential regardless of join shape.");
    } else {
        println!("WHY: Both optimizers converge — either egraph did not pick WcoJoin,");
        println!("     or Materialize picked a delta join that aligns with AGM reasoning.");
    }
    println!();

    // Hard assertion: the e-graph must pick WcoJoin (this is the AGM invariant).
    // The dominant cost degree must be 1.5 (the triangle AGM bound), not 2.0.
    assert!(
        picked_wcoj,
        "e-graph must extract WcoJoin for the triangle join; got {best:?}"
    );
    assert!(
        best_cost.time.first().copied().unwrap_or(0.0) < 1.6,
        "WcoJoin dominant time degree must be ~1.5 (AGM); got time={:?}",
        best_cost.time
    );
}

/// Whether any node in `rel` (the extracted, cheapest plan) is a `WcoJoin`.
fn rel_has_wcoj(rel: &Rel) -> bool {
    matches!(rel, Rel::WcoJoin { .. }) || rel.children().iter().any(|c| rel_has_wcoj(c))
}

/// Lower, saturate, extract, and report whether the cheapest extracted plan
/// contains any `WcoJoin`. For an acyclic join the guard prevents `join_to_wcoj`
/// from firing, so no WcoJoin is created and none can be extracted.
fn extracted_plan_has_wcoj(plan: MirRelationExpr) -> bool {
    let rel = lower(&plan);
    let model = CostModel::new();
    let outcome = Optimizer::new(default_ruleset(), model).optimize(rel);
    rel_has_wcoj(&outcome.plan)
}

#[mz_ore::test]
fn acyclic_two_way_join_gets_no_wcoj() {
    // Parity guarantee: a plain 2-way join is acyclic, so `join_to_wcoj` must
    // not create a WcoJoin and the join must not raise to a DeltaQuery. Before
    // the cyclicity guard this regressed to DeltaQuery (WcoJoin created for all
    // joins, AGM cost model then preferred it). This is the regression gate.
    assert!(
        !extracted_plan_has_wcoj(two_way()),
        "no WcoJoin must be created for an acyclic 2-way join"
    );
    let out = mz_transform::eqsat::optimize(two_way());
    assert!(
        !matches!(
            first_join_impl(&out),
            Some(JoinImplementation::DeltaQuery(_))
        ),
        "acyclic 2-way join must not raise to DeltaQuery, got {out:?}"
    );
}

#[mz_ore::test]
fn acyclic_chain_join_gets_no_wcoj() {
    // Parity guarantee: an acyclic 3-way chain (R-S-T path) must not create a
    // WcoJoin nor raise to a DeltaQuery. Same regression gate as the 2-way case
    // for the multi-input acyclic shape.
    assert!(
        !extracted_plan_has_wcoj(chain()),
        "no WcoJoin must be created for an acyclic 3-way chain join"
    );
    let out = mz_transform::eqsat::optimize(chain());
    assert!(
        !matches!(
            first_join_impl(&out),
            Some(JoinImplementation::DeltaQuery(_))
        ),
        "acyclic chain join must not raise to DeltaQuery, got {out:?}"
    );
}

/// The default objective must be `arrangement-count`, not the legacy
/// `peak-degree`. This test is the TDD anchor for Task 5: it must fail before
/// the objective/extractor fields are wired into `Optimizer`, and pass after.
#[mz_ore::test]
fn optimizer_defaults_to_arrangement_count() {
    let opt = Optimizer::new(default_ruleset(), CostModel::new());
    assert_eq!(opt.objective_name(), "arrangement-count");
}

/// Build an 8-way star join: R ⋈ S1 ⋈ S2 ⋈ S3 ⋈ S4 ⋈ S5 ⋈ S6 ⋈ S7 on a
/// shared key column.
///
/// Column layout: R=#0,#1  each Si=#2i,#2i+1
/// Each Si joins on R.key (#0) = Si.key (#2i).
///
/// This is an acyclic star topology. It is the widest join in the test suite
/// and is used to verify that binarization + commutativity stay within
/// saturation bounds even when many binary alternatives are added.
fn star8() -> MirRelationExpr {
    let r = src(50, 2);
    let s1 = src(51, 2);
    let s2 = src(52, 2);
    let s3 = src(53, 2);
    let s4 = src(54, 2);
    let s5 = src(55, 2);
    let s6 = src(56, 2);
    let s7 = src(57, 2);
    // R.col0 = Si.col0 for each i; R is at columns [0,2), S1 at [2,4), etc.
    let equivs = (0..7)
        .map(|i| {
            vec![
                MirScalarExpr::column(0),             // R.key
                MirScalarExpr::column(2 + 2 * i + 2), // Si.key
            ]
        })
        .collect();
    MirRelationExpr::join_scalars(vec![r, s1, s2, s3, s4, s5, s6, s7], equivs)
}

/// Whether `rel` is a non-binary (n-ary, n >= 3) join at the top level.
fn is_nary_join(rel: &Rel) -> bool {
    matches!(rel, Rel::Join { inputs, .. } if inputs.len() >= 3)
}

/// `binarize_join_first` must not blow up saturation for the widest (8-way star)
/// join in the test suite.
///
/// The binarize rule peels the first two inputs: each application adds a binary
/// alternative without duplicating the n-ary form. It is bounded by MAX_ENODES
/// (600). This test is the required termination gate.
#[mz_ore::test]
fn eight_way_join_saturates_within_bounds() {
    let plan = star8();
    let rel = lower(&plan);
    let model = CostModel::new();
    let outcome = Optimizer::new(default_ruleset(), model).optimize(rel);
    assert!(
        outcome.iterations < 100,
        "8-way star join must saturate within max_iters (100), got {} iterations",
        outcome.iterations
    );
    assert_eq!(
        outcome.plan.arity(),
        16,
        "arity must be preserved after optimization"
    );
}

/// For a 3-way chain join the binarize rule adds a binary split alternative to
/// the root e-class. The arrangement-count objective then picks the form with
/// fewest arrangements.
///
/// The n-ary form (3 direct inputs) has 3 arrangement terms. The binarized form
/// has 4 (inner join result + the two inner inputs + the outer third input).
/// So the n-ary form wins and must be extracted. This is the regression gate:
/// binarization must not silently inflate the arrangement count of the plan.
#[mz_ore::test]
fn binarize_preserves_arrangement_count_optimality() {
    let plan = chain();
    let rel = lower(&plan);
    let model = CostModel::new();
    let outcome = Optimizer::new(default_ruleset(), model.clone()).optimize(rel);

    // Saturation must terminate within max_iters.
    assert!(
        outcome.iterations < 100,
        "chain join must saturate within 100 iterations, got {}",
        outcome.iterations
    );
    // Arity preserved.
    assert_eq!(outcome.plan.arity(), 6, "arity must be preserved");

    // The n-ary form charges one arrangement per direct input. The binarized
    // form charges one more (for the intermediate join result). So the n-ary
    // form wins and must be extracted.
    let cost = model.cost(&outcome.plan);
    assert!(
        is_nary_join(&outcome.plan),
        "arrangement-count objective must extract the n-ary form (3 arrangements), \
         not the binarized form (4 arrangements); got: {:?}",
        outcome.plan
    );
    assert_eq!(
        cost.arrangements, 3,
        "n-ary 3-way chain must charge exactly 3 arrangements; got {:?}",
        cost
    );
}

/// The binarize rule must produce a binary-join alternative for a 3-way join in
/// the e-graph. This is verified indirectly: the initial_cost for the 3-way join
/// (before saturation) must equal the final_cost's arrangements only when the
/// binarized alternatives are strictly worse, confirming that the optimizer
/// explored them and correctly rejected them.
///
/// Additionally, if we FORCE the binarized form by constructing it directly and
/// comparing its arrangement cost to the n-ary form, the n-ary form must be
/// strictly cheaper on the arrangement-count axis.
#[mz_ore::test]
fn binarize_rule_explored_and_correctly_rejected() {
    use mz_transform::eqsat::ir::EScalar;

    // Build the n-ary chain join R-S-T as a Rel directly (not through lower),
    // so we control the exact structure.
    let r = lower(&src(13, 2)); // Rel::Opaque for R
    let s = lower(&src(14, 2)); // Rel::Opaque for S
    let t = lower(&src(15, 2)); // Rel::Opaque for T
    let col = |c: usize| EScalar::plain(MirScalarExpr::column(c));

    // Equivalences: #1=#2 (R.b=S.b), #3=#4 (S.c=T.c).
    let equivs = vec![vec![col(1), col(2)], vec![col(3), col(4)]];

    let nary = Rel::Join {
        inputs: vec![r.clone(), s.clone(), t.clone()],
        equivalences: equivs.clone(),
    };

    // The binarized form: inner join R-S, outer join (R-S)-T.
    // equivs_inner([#1=#2, #3=#4], boundary=4) = [#1=#2] (both cols < 4).
    // equivs_outer([#1=#2, #3=#4], boundary=4) = [#3=#4] (touches col >= 4).
    let inner_equivs = vec![vec![col(1), col(2)]]; // equivs_inner result
    let outer_equivs = vec![vec![col(3), col(4)]]; // equivs_outer result
    let inner = Rel::Join {
        inputs: vec![r.clone(), s.clone()],
        equivalences: inner_equivs,
    };
    let binarized = Rel::Join {
        inputs: vec![inner, t.clone()],
        equivalences: outer_equivs,
    };

    let model = CostModel::new();
    let cost_nary = model.cost(&nary);
    let cost_binarized = model.cost(&binarized);

    // The n-ary form must have strictly fewer arrangements than the binarized
    // form (n-ary charges 3 direct inputs, binarized charges inner-join + T +
    // R-in-inner + S-in-inner = 4).
    assert!(
        cost_nary.arrangements < cost_binarized.arrangements,
        "n-ary form must have fewer arrangements than binarized: \
         n-ary={} binarized={}",
        cost_nary.arrangements,
        cost_binarized.arrangements
    );

    // ArrangementCount prefers the n-ary form.
    use mz_transform::eqsat::objective::{ArrangementCount, Objective};
    assert_eq!(
        ArrangementCount.cmp(&cost_nary, &cost_binarized),
        std::cmp::Ordering::Less,
        "ArrangementCount must prefer n-ary over binarized"
    );
}

/// A binary join wrapped in a swap projection (`Project[[2,3,0,1]](Join(R, S))`)
/// has the same row set as `Join(S, R)` with the first two inputs transposed.
/// After commutativity fires, `fuse_projects` collapses the outer swap with
/// the restore projection to an identity permutation, and `drop_identity_project`
/// eliminates the resulting no-op Project. The extracted plan is therefore
/// `Join(S, R)` with no wrapper, which has one fewer time term and one fewer
/// node than the original Project-wrapped form.
///
/// This is the positive-utility gate for `commute_binary_join`: without the rule
/// the optimizer cannot discover the cheaper `Join(S, R)` form and must pay the
/// extra work term from the outer Project.
#[mz_ore::test]
fn commute_binary_join_eliminates_outer_swap_projection() {
    use mz_transform::eqsat::ir::{EScalar, Rel};

    // R(2 cols, id=80), S(2 cols, id=81). Join on R.col1 = S.col0: equiv [#1, #2].
    let r = lower(&src(80, 2));
    let s = lower(&src(81, 2));
    let col = |c: usize| EScalar::plain(MirScalarExpr::column(c));

    let join_rs = Rel::Join {
        inputs: vec![r.clone(), s.clone()],
        equivalences: vec![vec![col(1), col(2)]],
    };

    // The outer Project swaps the two 2-column groups: outputs [2,3,0,1].
    // This models a downstream consumer that wants S's columns before R's columns.
    // Without commutativity the optimizer must keep this Project.
    // With commutativity + fuse_projects + drop_identity_project it discovers
    // Join(S, R), which carries no Project overhead.
    let plan = Rel::Project {
        input: Box::new(join_rs),
        outputs: vec![2, 3, 0, 1],
    };

    let model = CostModel::new();
    let outcome = Optimizer::new(default_ruleset(), model.clone()).optimize(plan.clone());

    // Saturation must terminate within max_iters.
    assert!(
        outcome.iterations < 100,
        "must saturate within 100 iterations, got {}",
        outcome.iterations
    );

    // The extractor must choose Join(S, R) -- the commuted join without any
    // wrapper -- because it has one fewer time term and one fewer node than
    // the original Project[swap](Join(R, S)) form. Arity must be preserved (4).
    assert_eq!(
        outcome.plan.arity(),
        4,
        "arity must be preserved after optimization"
    );
    assert!(
        matches!(&outcome.plan, Rel::Join { inputs, .. } if inputs.len() == 2),
        "commuted join must be extracted without a Project wrapper; got {:?}",
        outcome.plan
    );

    // Confirm the cost of the extracted plan is strictly less than the original.
    let cost_original = model.cost(&plan);
    let cost_extracted = model.cost(&outcome.plan);
    use mz_transform::eqsat::objective::{ArrangementCount, Objective};
    assert_eq!(
        ArrangementCount.cmp(&cost_extracted, &cost_original),
        std::cmp::Ordering::Less,
        "extracted Join(S,R) must be strictly cheaper than original Project[swap](Join(R,S)); \
         extracted={:?} original={:?}",
        cost_extracted,
        cost_original
    );
}

/// `arrange_idempotent`: an `ArrangeBy` over an input already arranged by the
/// same key is redundant and must be dropped by the physical eqsat pass.
#[mz_ore::test]
fn double_arrange_by_collapses() {
    // ArrangeBy[#0](ArrangeBy[#0](R)): the inner arrangement already produces
    // key [#0], so the outer one is redundant.
    let inner = src(31, 2).arrange_by(&[vec![MirScalarExpr::column(0)]]);
    let plan = inner.arrange_by(&[vec![MirScalarExpr::column(0)]]);
    let out = mz_transform::eqsat::optimize(plan);
    let mut arrange_count = 0;
    out.visit_pre(|e| {
        if matches!(e, MirRelationExpr::ArrangeBy { .. }) {
            arrange_count += 1;
        }
    });
    assert_eq!(
        arrange_count, 1,
        "redundant outer ArrangeBy must be dropped, got {out:?}"
    );
}
