// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Differential harness: compare the eqsat optimizer against the real
//! `logical_optimizer` on a corpus of ~20 diverse MIR plans.
//!
//! CAVEAT: The eqsat cost model treats unsupported subtrees
//! (`Reduce`/`TopK`/`ArrangeBy`) and physical annotations as opaque leaves,
//! so comparisons reflect the supported-relational envelope only, not physical
//! execution cost.  We compare against `logical_optimizer` (not physical) to
//! avoid arrangement/join-implementation noise.
//!
//! A clean "no wins found" result is a valid, valuable outcome.

use mz_expr::{AccessStrategy, Id, MirRelationExpr, MirScalarExpr};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::eqsat::cost::{Cost, CostModel};
use mz_transform::eqsat::lower::lower;
use mz_transform::{Optimizer, TransformCtx, typecheck};

/// Build a source relation with `arity` Int64 non-nullable columns and a
/// unique transient global id `id`.
///
/// Uses a global `Get` rather than `constant(vec![], ...)` so that the real
/// optimizer's `FoldConstants` pass cannot collapse it to a zero-degree empty
/// node.  Both eqsat and the real pipeline treat an unknown `Get` as an opaque
/// leaf of degree 1, so the comparison is fair.
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

/// Build a boolean predicate `IS NULL(col)` on column `i`.
/// This is a valid boolean predicate accepted by the typecheck pass.
fn pred(i: usize) -> MirScalarExpr {
    MirScalarExpr::column(i).call_is_null()
}

/// Compute the eqsat abstract cost of a MirRelationExpr.
fn cost_of(e: &MirRelationExpr) -> Cost {
    let rel = lower(e);
    CostModel::new().cost(&rel)
}

/// Run the real logical optimizer on `input`, returning the optimized plan.
///
/// Returns `Err` with a description if the optimizer fails (e.g., a transform
/// rejects a bare `Get` that lacks a catalog entry).  Callers skip the case
/// rather than panicking.
fn real_optimize(input: MirRelationExpr) -> Result<MirRelationExpr, String> {
    let features = OptimizerFeatures::default();
    let tc = typecheck::empty_typechecking_context();
    let mut df = DataflowMetainfo::default();
    // Pass None as global_id so the typecheck pass does not try to register the
    // whole plan under a transient ID that also appears as a leaf Get inside
    // join cases — that would cause a spurious type mismatch.
    let mut ctx = TransformCtx::local(&features, &tc, &mut df, None, None);
    #[allow(deprecated)]
    let opt = Optimizer::logical_optimizer(&mut ctx);
    // Rebuild ctx because logical_optimizer borrows it mutably to build
    // typecheck passes; after the call it is done and we can use it again.
    let mut df2 = DataflowMetainfo::default();
    let mut ctx2 = TransformCtx::local(&features, &tc, &mut df2, None, None);
    opt.optimize(input, &mut ctx2)
        .map(|o| o.0)
        .map_err(|e| format!("{e}"))
}

/// Format a Cost for table display.
fn fmt_cost(c: &Cost) -> String {
    let time: Vec<String> = c.time.iter().map(|d| format!("{:.2}", d)).collect();
    let mem: Vec<String> = c.memory.iter().map(|d| format!("{:.2}", d)).collect();
    format!("t=[{}] m=[{}] n={}", time.join(","), mem.join(","), c.nodes)
}

#[mz_ore::test]
#[allow(deprecated)]
fn compare_egraph_vs_real() {
    // Each entry is (label, plan).
    let cases: Vec<(&str, MirRelationExpr)> = vec![
        // 1. Single filter — both should produce same.
        ("filter_single", src(1, 2).filter(vec![pred(0)])),
        // 2. Nested filters in one order.
        (
            "nested_filter_outer_0_inner_1",
            src(1, 2).filter(vec![pred(1)]).filter(vec![pred(0)]),
        ),
        // 3. Nested filters in the other order.
        (
            "nested_filter_outer_1_inner_0",
            src(1, 2).filter(vec![pred(0)]).filter(vec![pred(1)]),
        ),
        // 4. Filter-false: eqsat may or may not eliminate; real may not either
        //    without constant evaluation on opaque Gets.
        (
            "filter_false",
            src(1, 2).filter(vec![MirScalarExpr::literal_false()]),
        ),
        // 5. Filter over union with per-branch filters.
        ("filter_over_union_with_branch_filters", {
            let a = src(1, 2).filter(vec![pred(0)]);
            let b = src(2, 2).filter(vec![pred(1)]);
            MirRelationExpr::Union {
                base: Box::new(a),
                inputs: vec![b],
            }
            .filter(vec![pred(0)])
        }),
        // 6. Filter over union over per-branch filters, wrapped in map+project
        //    (stresses phase-ordering between map/project pushdown and filter fusion).
        ("filter_union_branch_filters_map_project", {
            let a = src(1, 2).filter(vec![pred(0)]);
            let b = src(2, 2).filter(vec![pred(1)]);
            let u = MirRelationExpr::Union {
                base: Box::new(a),
                inputs: vec![b],
            };
            u.filter(vec![pred(0)])
                .map(vec![MirScalarExpr::column(1)])
                .project(vec![0, 1, 2])
        }),
        // 7. Double negation: Negate(Negate(src)).
        ("double_negation", src(1, 2).negate().negate()),
        // 8. Double negation buried under a filter.
        (
            "double_negation_under_filter",
            src(1, 2).negate().negate().filter(vec![pred(0)]),
        ),
        // 9. Union(a, Negate(a)) — cancellation candidate.
        ("union_cancel", {
            let a = src(1, 2);
            MirRelationExpr::Union {
                base: Box::new(a.clone()),
                inputs: vec![a.negate()],
            }
        }),
        // 10. Union(a, Negate(a)) nested under a filter+map.
        ("union_cancel_under_filter_map", {
            let a = src(1, 2);
            MirRelationExpr::Union {
                base: Box::new(a.clone()),
                inputs: vec![a.negate()],
            }
            .filter(vec![pred(0)])
            .map(vec![MirScalarExpr::column(1)])
        }),
        // 11. Threshold(Threshold(src)).
        ("double_threshold", src(1, 2).threshold().threshold()),
        // 12. Threshold over Union of a and Negate(a) — eqsat may simplify via
        //     union_cancel then threshold elision.
        ("threshold_over_union_cancel", {
            let a = src(1, 2);
            MirRelationExpr::Union {
                base: Box::new(a.clone()),
                inputs: vec![a.negate()],
            }
            .threshold()
        }),
        // 13. Threshold over a union of two filtered inputs.
        ("threshold_over_union_filtered_inputs", {
            let a = src(1, 2).filter(vec![pred(0)]);
            let b = src(2, 2).filter(vec![pred(1)]);
            MirRelationExpr::Union {
                base: Box::new(a),
                inputs: vec![b],
            }
            .threshold()
        }),
        // 14. Triangle join: R(a,b) ⋈ S(b,c) ⋈ T(c,a).
        //     Eqsat should prefer WcoJoin (N^1.5) over binary join (N^2).
        ("triangle_join", {
            let r = src(1, 2); // cols 0..1
            let s = src(2, 2); // cols 2..3
            let t = src(3, 2); // cols 4..5
            // a: #0=#4, b: #1=#2, c: #3=#5
            MirRelationExpr::join_scalars(
                vec![r, s, t],
                vec![
                    vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
                    vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
                    vec![MirScalarExpr::column(3), MirScalarExpr::column(5)],
                ],
            )
        }),
        // 15. 4-cycle join: R(a,b) ⋈ S(b,c) ⋈ T(c,d) ⋈ U(d,a).
        ("four_cycle_join", {
            let r = src(1, 2); // cols 0..1
            let s = src(2, 2); // cols 2..3
            let t = src(3, 2); // cols 4..5
            let u = src(4, 2); // cols 6..7
            // a: #0=#6, b: #1=#2, c: #3=#4, d: #5=#7
            MirRelationExpr::join_scalars(
                vec![r, s, t, u],
                vec![
                    vec![MirScalarExpr::column(0), MirScalarExpr::column(6)],
                    vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
                    vec![MirScalarExpr::column(3), MirScalarExpr::column(4)],
                    vec![MirScalarExpr::column(5), MirScalarExpr::column(7)],
                ],
            )
        }),
        // 16. Triangle join under a filter (filter pushdown interaction).
        ("triangle_join_filtered", {
            let r = src(1, 2);
            let s = src(2, 2);
            let t = src(3, 2);
            MirRelationExpr::join_scalars(
                vec![r, s, t],
                vec![
                    vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
                    vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
                    vec![MirScalarExpr::column(3), MirScalarExpr::column(5)],
                ],
            )
            .filter(vec![pred(0)])
        }),
        // 17. Binary join with Reduce in one branch (bail behavior test).
        ("join_with_reduce_branch", {
            // Reduce on src(1,2) over group key [col(0)] with no aggregates.
            let reduced = src(1, 2).reduce(vec![0], vec![], None);
            let plain = src(2, 2);
            // a: #0=#2 (reduced has arity 1 from the group key)
            MirRelationExpr::join_scalars(
                vec![reduced, plain],
                vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(1)]],
            )
        }),
        // 18. TopK above a filter (bail behavior test).
        ("topk_over_filter", {
            src(1, 2).filter(vec![pred(0)]).top_k(
                vec![0],
                vec![],
                Some(MirScalarExpr::literal_ok(
                    mz_repr::Datum::Int64(10),
                    ReprScalarType::Int64,
                )),
                0,
                None,
            )
        }),
        // 19. Project-then-filter vs filter-then-project (phase ordering).
        (
            "project_then_filter",
            src(1, 3).project(vec![0, 2]).filter(vec![pred(0)]),
        ),
        // 20. Filter-then-project (complement of case 19).
        (
            "filter_then_project",
            src(1, 3).filter(vec![pred(0)]).project(vec![0, 2]),
        ),
    ];

    // Header
    println!();
    println!(
        "{:<40} | {:<35} | {:<35} | {:<35} | verdict",
        "label", "c_in", "c_real", "c_eq"
    );
    println!("{}", "-".repeat(165));

    let mut wins = 0usize;
    let mut losses = 0usize;
    let mut ties = 0usize;
    let mut skips = 0usize;
    let mut win_details: Vec<(String, MirRelationExpr, MirRelationExpr, MirRelationExpr)> =
        Vec::new();

    for (label, input) in cases {
        let c_in = cost_of(&input);
        let real = match real_optimize(input.clone()) {
            Ok(r) => r,
            Err(e) => {
                skips += 1;
                println!(
                    "{:<40} | {:<35} | {:<35} | {:<35} | SKIP (real_optimize error: {})",
                    label,
                    fmt_cost(&c_in),
                    "—",
                    "—",
                    e,
                );
                continue;
            }
        };
        let c_real = cost_of(&real);
        let eq = mz_transform::eqsat::optimize(input.clone());
        let c_eq = cost_of(&eq);

        let verdict = match c_eq.cmp(&c_real) {
            std::cmp::Ordering::Less => {
                wins += 1;
                win_details.push((label.to_owned(), input.clone(), real.clone(), eq.clone()));
                "WIN (eqsat cheaper)"
            }
            std::cmp::Ordering::Greater => {
                losses += 1;
                "LOSS (real cheaper)"
            }
            std::cmp::Ordering::Equal => {
                ties += 1;
                "TIE"
            }
        };

        println!(
            "{:<40} | {:<35} | {:<35} | {:<35} | {}",
            label,
            fmt_cost(&c_in),
            fmt_cost(&c_real),
            fmt_cost(&c_eq),
            verdict,
        );
    }

    println!("{}", "-".repeat(165));
    println!();
    println!(
        "SUMMARY: {} wins / {} losses / {} ties / {} skips",
        wins, losses, ties, skips
    );
    println!();

    // Print full plans for every WIN so the divergence can be inspected.
    for (label, input, real, eq) in &win_details {
        println!("=== WIN: {} ===", label);
        println!("--- INPUT ---");
        println!("{:#?}", input);
        println!("--- REAL OPTIMIZER ---");
        println!("{:#?}", real);
        println!("--- EQSAT ---");
        println!("{:#?}", eq);
        println!();
    }
}
