// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! A self-contained wall-clock benchmark for the eqsat logical optimizer.
//!
//! Builds a fixed corpus of relational plans that exercise the rewrite rules
//! (filter/map fusion, union distribution, union-cancel, joins) and times
//! `optimize_logical` over many iterations. The same file compiles on the
//! interpreter branch and the codegen branch, so running it on both compares
//! the matcher implementations end to end.
//!
//! ```text
//! cargo run --release -p mz-transform --example eqsat_bench [ITERS]
//! ```

use std::hint::black_box;
use std::time::Instant;

use mz_expr::{AccessStrategy, Id, MirRelationExpr, MirScalarExpr};
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};

/// A source relation with `arity` non-nullable Int64 columns.
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

fn pred(i: usize) -> MirScalarExpr {
    MirScalarExpr::column(i).call_is_null()
}

fn union(a: MirRelationExpr, b: MirRelationExpr) -> MirRelationExpr {
    MirRelationExpr::Union {
        base: Box::new(a),
        inputs: vec![b],
    }
}

/// The benchmark corpus: each plan exercises a different cluster of rules.
fn corpus() -> Vec<(&'static str, MirRelationExpr)> {
    vec![
        // Filter fusion: a deep chain that fires merge_filters repeatedly.
        (
            "filter_chain",
            src(1, 4)
                .filter(vec![pred(0)])
                .filter(vec![pred(1)])
                .filter(vec![pred(2)])
                .filter(vec![pred(3)])
                .filter(vec![pred(0)]),
        ),
        // Map fusion: fuse_maps + map_columns_to_projection.
        (
            "map_chain",
            src(2, 3)
                .map(vec![MirScalarExpr::column(0)])
                .map(vec![MirScalarExpr::column(1)])
                .map(vec![MirScalarExpr::column(2)]),
        ),
        // Filter distributed over a union, then merged in each branch.
        (
            "filter_over_union",
            union(
                src(3, 2).filter(vec![pred(0)]),
                src(4, 2).filter(vec![pred(1)]),
            )
            .filter(vec![pred(0)])
            .filter(vec![pred(1)]),
        ),
        // union_cancel: a + (-a) collapses, then empty propagates.
        (
            "union_cancel",
            union(src(5, 2), src(5, 2).negate())
                .filter(vec![pred(0)])
                .threshold(),
        ),
        // Negate distribution over a nested union.
        (
            "negate_union",
            union(src(6, 2), union(src(7, 2), src(8, 2)))
                .negate()
                .negate(),
        ),
        // Triangle join (cyclic): exercises the join/wcoj machinery.
        (
            "triangle_join",
            MirRelationExpr::join_scalars(
                vec![src(9, 2), src(10, 2), src(11, 2)],
                vec![
                    vec![MirScalarExpr::column(0), MirScalarExpr::column(4)],
                    vec![MirScalarExpr::column(1), MirScalarExpr::column(2)],
                    vec![MirScalarExpr::column(3), MirScalarExpr::column(5)],
                ],
            )
            .filter(vec![pred(0)]),
        ),
        // A larger mixed plan: filtered union of two filtered joins.
        (
            "mixed",
            union(
                MirRelationExpr::join_scalars(
                    vec![src(12, 2), src(13, 2)],
                    vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(2)]],
                )
                .filter(vec![pred(0)])
                .map(vec![MirScalarExpr::column(1)]),
                MirRelationExpr::join_scalars(
                    vec![src(14, 2), src(15, 2)],
                    vec![vec![MirScalarExpr::column(1), MirScalarExpr::column(3)]],
                )
                .filter(vec![pred(1)])
                .map(vec![MirScalarExpr::column(0)]),
            )
            .filter(vec![pred(0)]),
        ),
    ]
}

fn main() {
    let iters: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);

    let corpus = corpus();

    // Warm up (and force any one-time work).
    for (_, plan) in &corpus {
        black_box(mz_transform::eqsat::optimize_logical(plan.clone(), false));
    }

    println!("plan                 iters     total        per-op");
    let mut grand = std::time::Duration::ZERO;
    for (name, plan) in &corpus {
        let t = Instant::now();
        for _ in 0..iters {
            black_box(mz_transform::eqsat::optimize_logical(
                black_box(plan.clone()),
                false,
            ));
        }
        let e = t.elapsed();
        grand += e;
        let n = u32::try_from(iters).expect("iters fits u32");
        println!("{name:<18} {iters:>6}  {:>9.3?}  {:>9.3?}", e, e / n);
    }
    let total_ops = iters * corpus.len();
    let n = u32::try_from(total_ops).expect("op count fits u32");
    println!(
        "TOTAL                {total_ops:>6}  {grand:>9.3?}  {:>9.3?}",
        grand / n
    );
}
