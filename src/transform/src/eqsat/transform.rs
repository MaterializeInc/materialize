// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `Transform` wrappers over the eqsat optimizer, registered in the logical and
//! physical optimizers behind per-phase feature flags.

use mz_expr::MirRelationExpr;
use mz_expr::MirScalarExpr;
use mz_repr::GlobalId;
use std::collections::BTreeMap;

use crate::{Transform, TransformCtx, TransformError};

/// Maximum input plan size (node count) the pass will attempt. Equality
/// saturation explores a combinatorial space; on large plans the e-graph blows
/// up (tens of seconds per object observed on builtin indexes). Skipping large
/// plans keeps catalog bootstrap and optimization within their time budgets. A
/// skipped plan is a sound no-op. The cap is deliberately generous so the vast
/// majority of user and builtin plans still run through the pass.
const MAX_PLAN_SIZE: usize = 200;

// Subsume-or-include audit result: `Demand` and `ProjectionPushdown` remain
// production passes in the logical and physical optimizer pipelines (include-for-now).
// The eqsat pass runs its own demand-narrowing internally (via `raise::demand_pushdown`),
// but the production passes that run before and after eqsat in `lib.rs` are not
// removed: column-liveness analysis requires type context the e-graph does not carry,
// and their placement relative to JoinImplementation is load-bearing. Remove only when
// eqsat subsumes them end-to-end (i.e., when a type-aware saturation pass covers demand).

/// Runs the equality-saturation pass as a `Transform`.
#[derive(Debug)]
pub struct EqSatTransform;

impl Transform for EqSatTransform {
    fn name(&self) -> &'static str {
        "EqSatTransform"
    }

    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // Skip plans above the size cap: saturation cost is superlinear in plan
        // size and a large plan can take tens of seconds. A no-op is sound.
        let plan_size = relation.size();
        if plan_size > MAX_PLAN_SIZE {
            return Ok(());
        }
        // Hard equivalence guard at the live boundary: the pass is equivalence
        // and type preserving, so it must never change arity or any column's
        // scalar type. Optimize a clone, and adopt it only if both match (see
        // `adopt_if_type_preserving`). On any mismatch, leave the input
        // untouched (a no-op is always sound) and log loudly.
        let input_arity = relation.arity();
        // The pass runs in the logical optimizer, so its output must carry only
        // `Unimplemented` joins: the immediately following ProjectionPushdown
        // (run with `include_joins`) panics on a filled-in implementation, and
        // physical join planning expects to choose the implementation itself.
        // `optimize_logical` therefore emits worst-case-optimal joins as plain
        // `Unimplemented` joins rather than committing them to `DeltaQuery`. The
        // delta commitment (the experiment's offline payoff) is exercised by the
        // direct `optimize` callers in tests, not here.
        let optimized =
            crate::eqsat::optimize_logical(relation.clone(), ctx.features.enable_eqsat_wmr_lift);
        adopt_if_type_preserving(relation, optimized, input_arity, "eqsat optimize");
        Ok(())
    }
}

/// Adopt `optimized` into `relation` only if it preserves the input's arity and
/// per-column scalar types. On any mismatch, leave `relation` untouched (a no-op
/// is always sound) and log loudly.
///
/// The pass is equivalence and type preserving, so its output must agree with
/// the input on arity and on each column's representation scalar type. It may
/// legitimately change nullability (a `Filter` strengthens columns to non-null,
/// a `Union` takes the least upper bound), so nullability is not compared. This
/// guard is the live boundary's last line of defense: a synthesized `Empty`
/// whose column types could not be derived at synthesis time falls back to a
/// placeholder type, and this check rejects such a plan rather than emitting a
/// wrong-typed one. `soft_panic_or_log!` panics in debug/test builds to surface
/// the bug and logs in release.
fn adopt_if_type_preserving(
    relation: &mut MirRelationExpr,
    optimized: MirRelationExpr,
    input_arity: usize,
    what: &str,
) {
    let output_arity = optimized.arity();
    if output_arity != input_arity {
        mz_ore::soft_panic_or_log!(
            "{what} changed arity ({} -> {}); leaving the plan unchanged",
            input_arity,
            output_arity,
        );
        return;
    }
    let input_types = relation.typ().column_types;
    let output_types = optimized.typ().column_types;
    let scalar_types_match = input_types
        .iter()
        .zip(output_types.iter())
        .all(|(a, b)| a.scalar_type == b.scalar_type);
    if scalar_types_match {
        *relation = optimized;
    } else {
        mz_ore::soft_panic_or_log!(
            "{what} changed column types ({:?} -> {:?}); leaving the plan unchanged",
            input_types,
            output_types,
        );
    }
}

/// Runs the equality-saturation pass in the physical optimizer, committing the
/// WcoJoin-to-DeltaQuery decision.
///
/// Placement contract: runs after `fixpoint_physical_01` and before
/// `LiteralConstraints`/`JoinImplementation`. At that point joins are still
/// `Unimplemented` (the ProjectionPushdown inside `fixpoint_physical_01` that
/// panics on filled-in implementations has already run). The committed
/// `DeltaQuery` survives `JoinImplementation` because that transform only
/// replans `Unimplemented` and `Differential` joins.
#[derive(Debug)]
pub struct PhysicalEqSatTransform;

impl Transform for PhysicalEqSatTransform {
    fn name(&self) -> &'static str {
        "PhysicalEqSatTransform"
    }

    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        // Same size cap as the logical pass: saturation cost is superlinear and
        // a large plan can take tens of seconds.
        let plan_size = relation.size();
        if plan_size > MAX_PLAN_SIZE {
            return Ok(());
        }
        // Hard equivalence guard: optimize a clone, adopt only if arity and
        // column scalar types are preserved (see `adopt_if_type_preserving`). On
        // any mismatch, leave the input untouched and log loudly.
        let input_arity = relation.arity();
        // Build index availability from ctx.indexes so the cost model does not
        // charge the arrangement-build memory term for join inputs that are
        // already arranged by an available index.  The physical pass has access
        // to the real index oracle; the logical pass uses empty availability.
        let available = build_availability(relation, ctx.indexes);
        // Find literal filters the production detector can turn into index
        // lookups, so the e-graph can weigh that choice during extraction. The
        // detector needs the oracle and imperative MFP analysis, so it runs once
        // here rather than as a saturation rule (see `collect_indexed_filter_seeds`).
        let seeds = collect_indexed_filter_seeds(relation, ctx);
        // Unlike the logical pass, this calls optimize_with_availability
        // (commit_wcoj=true) so the e-graph's WcoJoin choice is lowered to a
        // live DeltaQuery with an index-aware cost model.
        let use_ilp = ctx.features.enable_eqsat_ilp_extraction;
        let use_delta = ctx.features.enable_eqsat_delta_join_cost;
        let optimized = crate::eqsat::optimize_with_availability(
            relation.clone(),
            available,
            seeds,
            use_ilp,
            use_delta,
            ctx.features.enable_eqsat_native_join_commit,
        );
        adopt_if_type_preserving(relation, optimized, input_arity, "eqsat physical optimize");
        Ok(())
    }
}

/// Build an index-availability map from the oracle for all global `Get`s
/// reachable in `relation`.
///
/// Walks `relation` to collect every `GlobalId` referenced by a global `Get`,
/// then queries `oracle.indexes_on` for each to gather available index keys.
/// The result is passed to the cost model so indexed join inputs are not
/// charged the arrangement-build memory term.
fn build_availability(
    relation: &MirRelationExpr,
    oracle: &dyn crate::IndexOracle,
) -> BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> {
    use mz_expr::Id;
    let mut gids: std::collections::BTreeSet<GlobalId> = std::collections::BTreeSet::new();
    relation.visit_pre(|e| {
        if let MirRelationExpr::Get {
            id: Id::Global(gid),
            ..
        } = e
        {
            gids.insert(*gid);
        }
    });
    let mut available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> = BTreeMap::new();
    for gid in gids {
        let keys: Vec<Vec<MirScalarExpr>> = oracle
            .indexes_on(gid)
            .map(|(_idx_id, key)| key.to_vec())
            .collect();
        if !keys.is_empty() {
            available.insert(gid, keys);
        }
    }
    available
}

/// Find literal filters over a global `Get` that the production
/// `LiteralConstraints` transform can realize as an index lookup, returning a
/// seed per such filter for the e-graph to consider.
///
/// The detection logic (`LiteralConstraints::detect_literal_constraints`, reached
/// via `LiteralConstraints::action`) needs the index oracle and imperative MFP
/// analysis, neither of which fits a declarative saturation rule. So we run the
/// production transform on a clone of each candidate's bare `Filter(Get)`: if it
/// produces an `IndexedFilter` join, we capture that realization as the seed's
/// committed form, and lower the same bare filter to recover the exact
/// predicates the e-graph will hold (lowering is the same path the optimizer
/// uses, so the predicates match byte for byte).
///
/// Scope: a literal filter over a global `Get`, possibly through one or more
/// `Project`s. A Project-wrapped filter (`Filter(Project(Get))`) is reduced to
/// the bare `Filter(Get)` that `push_filter_past_project` exposes during
/// saturation, with its predicates permuted down through the projections, so the
/// seed targets that exposed node. A filter wrapped in a `Map` is left to the
/// standalone `LiteralConstraints` pass that still runs downstream.
fn collect_indexed_filter_seeds(
    relation: &MirRelationExpr,
    ctx: &mut TransformCtx,
) -> Vec<crate::eqsat::egraph::IndexedFilterSeed> {
    use std::collections::BTreeSet;

    use crate::eqsat::egraph::IndexedFilterSeed;
    use crate::eqsat::ir::Rel;
    use crate::literal_constraints::LiteralConstraints;

    // Map each non-recursive `Let` binding to its definition, so a literal filter
    // over a local `Get` (the shared-CSE shape: `Let l = Project(Get) in
    // Filter(Get l)`) can be resolved back to the global `Get`. `LetRec` bindings
    // are recursive and deliberately excluded: resolving through a recursion
    // back-edge is unsound (see the engine's `optimize_scope`).
    let mut let_defs: BTreeMap<mz_expr::LocalId, MirRelationExpr> = BTreeMap::new();
    relation.visit_pre(|e| {
        if let MirRelationExpr::Let { id, value, .. } = e {
            let_defs.insert(*id, (**value).clone());
        }
    });

    let mut seeds: Vec<IndexedFilterSeed> = Vec::new();
    // Collect candidate (gid, bare filter) pairs first; the detector needs `&mut
    // ctx`, which cannot be captured by the shared-borrow `visit_pre` closure.
    // Deduplicate by the bare filter: distinct wrappers reducing to the same bare
    // `Filter(Get)` hash-cons to a single e-class, so one seed covers them all,
    // and running the (heavy) detector once per distinct bare filter avoids
    // redundant work on repeated filters (e.g. a self-join or a re-used CTE).
    let mut candidates: Vec<(GlobalId, MirRelationExpr)> = Vec::new();
    let mut seen: BTreeSet<MirRelationExpr> = BTreeSet::new();
    relation.visit_pre(|e| {
        if let Some((gid, bare)) = bare_filter_over_global_get(e, &let_defs) {
            if seen.insert(bare.clone()) {
                candidates.push((gid, bare));
            }
        }
    });
    for (gid, bare) in candidates {
        // Run the production detector on a clone of the bare filter. A failure is
        // a sound no-op (the plan is simply not seeded). Call `action` directly,
        // not the `actually_perform_transform` wrapper, so the per-subtree probe
        // does not emit an optimizer trace.
        let mut committed = bare.clone();
        if LiteralConstraints.action(&mut committed, ctx).is_err() {
            continue;
        }
        if !contains_indexed_filter(&committed) {
            // No usable index for this filter; nothing to seed.
            continue;
        }
        // Lower the bare filter to obtain the predicates exactly as the e-graph
        // holds them once `push_filter_past_project` has exposed it. Only a
        // top-level `Rel::Filter` is in scope.
        if let Rel::Filter { predicates, .. } = crate::eqsat::lower::lower(&bare) {
            seeds.push(IndexedFilterSeed {
                get_id: gid,
                predicates,
                committed,
            });
        }
    }
    seeds
}

/// If `expr` is a literal filter over a global `Get`, possibly through one or
/// more `Project`s and non-recursive `Let` bindings (resolved via `let_defs`),
/// return the global id and the equivalent bare `Filter(Get)`, with the
/// predicates permuted down through the projections to the underlying `Get`
/// columns.
///
/// The bare form is exactly what `push_filter_past_project` exposes during
/// saturation, so running the detector on it yields a committed realization of
/// the `Get`'s arity, and lowering it yields predicates that match the e-graph's
/// pushed `Filter(Get)` node.
fn bare_filter_over_global_get<'a>(
    expr: &'a MirRelationExpr,
    let_defs: &'a BTreeMap<mz_expr::LocalId, MirRelationExpr>,
) -> Option<(GlobalId, MirRelationExpr)> {
    use mz_expr::{Columns, Id};

    let MirRelationExpr::Filter { input, predicates } = expr else {
        return None;
    };
    let mut predicates = predicates.clone();
    let mut node = input.as_ref();
    // Walk down through projections and non-recursive `Let` references to the
    // underlying `Get`. A projection permutes each predicate from its output
    // columns to its input columns (the `predicate.permute(outputs)` push in
    // `PredicatePushdown` / the `push_filter_past_project` rule); a local `Get`
    // is followed to its binding definition. The depth bound is a defensive
    // backstop; a non-recursive `Let` chain is acyclic and terminates anyway.
    for _ in 0..64 {
        match node {
            MirRelationExpr::Project { input, outputs } => {
                for p in &mut predicates {
                    p.permute(outputs);
                }
                node = input.as_ref();
            }
            MirRelationExpr::Get {
                id: Id::Local(lid), ..
            } => {
                node = let_defs.get(lid)?;
            }
            MirRelationExpr::Get {
                id: Id::Global(gid),
                ..
            } => {
                let bare = MirRelationExpr::Filter {
                    predicates,
                    input: Box::new(node.clone()),
                };
                return Some((*gid, bare));
            }
            _ => return None,
        }
    }
    None
}

/// Whether `expr` contains a join committed to an `IndexedFilter` implementation.
fn contains_indexed_filter(expr: &MirRelationExpr) -> bool {
    use mz_expr::JoinImplementation;

    let mut found = false;
    expr.visit_pre(|n| {
        if let MirRelationExpr::Join { implementation, .. } = n {
            if matches!(implementation, JoinImplementation::IndexedFilter(..)) {
                found = true;
            }
        }
    });
    found
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_expr::{AccessStrategy, BinaryFunc, Id, MirRelationExpr, MirScalarExpr};
    use mz_repr::{Datum, GlobalId, ReprRelationType, ReprScalarType};

    use crate::eqsat::egraph::IndexedFilterSeed;
    use crate::eqsat::ir::Rel;
    use crate::eqsat::lower::lower;
    use crate::eqsat::optimize_with_availability;

    use super::bare_filter_over_global_get;

    /// The global the literal filter sits on; indexed on column 0.
    const R_ID: u64 = 201;
    /// A recognizable global standing in for the committed `IndexedFilter`
    /// realization, so we can detect that the seed was extracted.
    const MARKER_ID: u64 = 999;

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

    /// `#col = 5`, a literal point predicate over `col`.
    fn lit_pred_on(col: usize) -> MirScalarExpr {
        MirScalarExpr::column(col).call_binary(
            MirScalarExpr::literal_ok(Datum::Int64(5), ReprScalarType::Int64),
            BinaryFunc::Eq(mz_expr::func::Eq),
        )
    }

    /// `#0 = 5`, a literal point predicate over column 0.
    fn lit_pred() -> MirScalarExpr {
        lit_pred_on(0)
    }

    /// `#0 = val`, a literal point predicate over column 0.
    fn lit_pred_val(val: i64) -> MirScalarExpr {
        MirScalarExpr::column(0).call_binary(
            MirScalarExpr::literal_ok(Datum::Int64(val), ReprScalarType::Int64),
            BinaryFunc::Eq(mz_expr::func::Eq),
        )
    }

    /// A seed for `Filter[#0=val](Get R)` (arity 2) whose committed realization
    /// is a recognizable `Get` of `marker_id`.
    fn marker_seed_val(val: i64, marker_id: u64) -> IndexedFilterSeed {
        let bare = MirRelationExpr::Filter {
            predicates: vec![lit_pred_val(val)],
            input: Box::new(src(R_ID, 2)),
        };
        let Rel::Filter { predicates, .. } = lower(&bare) else {
            panic!("lowering Filter(Get) must yield Rel::Filter");
        };
        IndexedFilterSeed {
            get_id: GlobalId::Transient(R_ID),
            predicates,
            committed: src(marker_id, 2),
        }
    }

    /// Whether `expr` contains a `Get` of the given marker id.
    fn contains_marker_id(expr: &MirRelationExpr, marker_id: u64) -> bool {
        let mut found = false;
        expr.visit_pre(|n| {
            if let MirRelationExpr::Get {
                id: Id::Global(GlobalId::Transient(id)),
                ..
            } = n
            {
                if *id == marker_id {
                    found = true;
                }
            }
        });
        found
    }

    /// A hand-built seed for `Filter[pred](Get R)` of arity `arity`, with a
    /// recognizable committed realization (a `Get` of `MARKER_ID`) standing in
    /// for the production `IndexedFilter`. The predicates are lowered from the
    /// bare `Filter(Get)`, exactly as the e-graph holds them once
    /// `push_filter_past_project` has fired, so the unioning matcher can locate
    /// the e-class.
    fn marker_seed_for(pred: MirScalarExpr, arity: usize) -> IndexedFilterSeed {
        let bare = MirRelationExpr::Filter {
            predicates: vec![pred],
            input: Box::new(src(R_ID, arity)),
        };
        let Rel::Filter { predicates, .. } = lower(&bare) else {
            panic!("lowering Filter(Get) must yield Rel::Filter");
        };
        IndexedFilterSeed {
            get_id: GlobalId::Transient(R_ID),
            predicates,
            committed: src(MARKER_ID, arity),
        }
    }

    fn marker_seed() -> IndexedFilterSeed {
        marker_seed_for(lit_pred(), 2)
    }

    /// Whether `expr` contains the marker `Get` that stands in for the committed
    /// `IndexedFilter` realization (i.e. extraction chose the seeded lookup).
    fn contains_marker(expr: &MirRelationExpr) -> bool {
        let mut found = false;
        expr.visit_pre(|n| {
            if let MirRelationExpr::Get {
                id: Id::Global(GlobalId::Transient(id)),
                ..
            } = n
            {
                if *id == MARKER_ID {
                    found = true;
                }
            }
        });
        found
    }

    /// Index on column 0 of `R`.
    fn avail() -> BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> {
        let mut m = BTreeMap::new();
        m.insert(
            GlobalId::Transient(R_ID),
            vec![vec![MirScalarExpr::column(0)]],
        );
        m
    }

    #[mz_ore::test]
    fn seed_attaches_to_bare_filter_over_get() {
        // Baseline: the input is exactly `Filter(Get)`, the form the seeder
        // targets. The seed attaches and extraction emits the committed lookup.
        let input = MirRelationExpr::Filter {
            predicates: vec![lit_pred()],
            input: Box::new(src(R_ID, 2)),
        };
        let out = optimize_with_availability(input, avail(), vec![marker_seed()], false, false, false);
        assert!(
            contains_marker(&out),
            "seed must attach to bare Filter(Get): {out:?}"
        );
    }

    #[mz_ore::test]
    fn seed_attaches_through_project() {
        // The repro. The literal filter sits over a Project over the Get, the
        // form production actually feeds (`Filter(Project(Get))`). The one-shot
        // pre-saturation seeder only matches a Filter directly over the Get, so
        // the index lookup never enters the e-graph and extraction falls back to
        // a full scan. Saturation's `push_filter_past_project` does expose a
        // `Filter(Get)` node later, but seeding already ran, so the seed is never
        // attached. This currently FAILS and pins the bug.
        let input = MirRelationExpr::Filter {
            predicates: vec![lit_pred()],
            input: Box::new(MirRelationExpr::Project {
                outputs: vec![0],
                input: Box::new(src(R_ID, 2)),
            }),
        };
        let out = optimize_with_availability(input, avail(), vec![marker_seed()], false, false, false);
        assert!(
            contains_marker(&out),
            "seed must attach through Project(Get): {out:?}"
        );
    }

    #[mz_ore::test]
    fn seed_attaches_through_reordering_project() {
        // A non-identity projection: `Project[2,0]` over a 3-column `R` puts
        // `R#0` at output column 1. The filter reads output #1, so the bare
        // equivalent is `Filter[#0=5](Get R)`. `push_filter_past_project` remaps
        // the predicate back through the projection to `#0`, which must equal the
        // seed's predicate lowered from that same bare filter. This guards the
        // predicate match against non-identity projections.
        let input = MirRelationExpr::Filter {
            predicates: vec![lit_pred_on(1)],
            input: Box::new(MirRelationExpr::Project {
                outputs: vec![2, 0],
                input: Box::new(src(R_ID, 3)),
            }),
        };
        let out = optimize_with_availability(
            input,
            avail(),
            vec![marker_seed_for(lit_pred(), 3)],
            false,
            false,
            false,
        );
        assert!(
            contains_marker(&out),
            "seed must attach through a reordering Project(Get): {out:?}"
        );
    }

    #[mz_ore::test]
    fn seed_attaches_through_let_shared_project() {
        // The relation_cse regression shape. A shared `Project(Get)` is bound to
        // a `Let`, and the literal filter sits over the local `Get l0` in the
        // body. The body fragment is optimized by `optimize_body_with_let_union`,
        // which unions the binding definition into the local-get class, so
        // `push_filter_past_project` exposes the bare `Filter(Get global)`. The
        // seed must attach there too, otherwise the lookup degrades to a full
        // scan with the filter hoisted on top of the shared scan.
        use mz_expr::LocalId;

        let l0 = LocalId::new(0);
        let local_get = MirRelationExpr::Get {
            id: Id::Local(l0),
            typ: ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let input = MirRelationExpr::Let {
            id: l0,
            value: Box::new(MirRelationExpr::Project {
                outputs: vec![0],
                input: Box::new(src(R_ID, 2)),
            }),
            body: Box::new(MirRelationExpr::Filter {
                predicates: vec![lit_pred()],
                input: Box::new(local_get),
            }),
        };
        let out = optimize_with_availability(input, avail(), vec![marker_seed()], false, false, false);
        assert!(
            contains_marker(&out),
            "seed must attach through a Let-bound shared Project(Get): {out:?}"
        );
    }

    #[mz_ore::test]
    fn flat_shared_project_two_values_both_lookup() {
        // The exact `relation_cse` line-824 shape, as eqsat actually receives it
        // (flat, no Let): a CrossJoin of four `Filter[#0=v](Project[#0](Get R))`
        // branches over two distinct literals, sharing `Project[#0](Get R)`. Both
        // distinct lookups must be extracted, under BOTH extractors (production
        // defaults to ILP, so a greedy-only fix would not green the SLT).
        let branch = |val: i64| MirRelationExpr::Filter {
            predicates: vec![lit_pred_val(val)],
            input: Box::new(MirRelationExpr::Project {
                outputs: vec![0],
                input: Box::new(src(R_ID, 2)),
            }),
        };
        // Two arity-1 branches and two arity-0 (projected-away) branches, exactly
        // as the planner emits for `s1.f1 = 1 AND s2.f1 = 2`.
        let inputs = vec![
            branch(1).project(vec![]),
            branch(1),
            branch(2).project(vec![]),
            branch(2),
        ];
        let plan = MirRelationExpr::join_scalars(inputs, vec![]);
        let seeds = vec![marker_seed_val(1, 999), marker_seed_val(2, 998)];

        for use_ilp in [false, true] {
            let out =
                optimize_with_availability(plan.clone(), avail(), seeds.clone(), use_ilp, false, false);
            assert!(
                contains_marker_id(&out, 999),
                "value=1 lookup must be extracted (use_ilp={use_ilp}): {out:?}"
            );
            assert!(
                contains_marker_id(&out, 998),
                "value=2 lookup must be extracted (use_ilp={use_ilp}): {out:?}"
            );
        }
    }

    #[mz_ore::test]
    fn bare_filter_reduces_bare_filter_over_get() {
        // A bare `Filter(Get)` is returned unchanged.
        let bare = MirRelationExpr::Filter {
            predicates: vec![lit_pred()],
            input: Box::new(src(R_ID, 2)),
        };
        let (gid, got) =
            bare_filter_over_global_get(&bare, &no_lets()).expect("bare filter is a candidate");
        assert_eq!(gid, GlobalId::Transient(R_ID));
        assert_eq!(got, bare);
    }

    #[mz_ore::test]
    fn bare_filter_peels_reordering_project() {
        // `Filter[#1=5](Project[2,0](Get R3))` reduces to `Filter[#0=5](Get R3)`:
        // output column 1 maps to underlying column `outputs[1] = 0`.
        let input = MirRelationExpr::Filter {
            predicates: vec![lit_pred_on(1)],
            input: Box::new(MirRelationExpr::Project {
                outputs: vec![2, 0],
                input: Box::new(src(R_ID, 3)),
            }),
        };
        let (gid, got) =
            bare_filter_over_global_get(&input, &no_lets()).expect("project filter is a candidate");
        assert_eq!(gid, GlobalId::Transient(R_ID));
        assert_eq!(
            got,
            MirRelationExpr::Filter {
                predicates: vec![lit_pred_on(0)],
                input: Box::new(src(R_ID, 3)),
            }
        );
    }

    #[mz_ore::test]
    fn bare_filter_peels_let_bound_project() {
        // `Filter[#0=5](Get l0)` with `l0 = Project[0](Get R3)` resolves through
        // the binding to the bare `Filter[#0=5](Get R3)`.
        use mz_expr::LocalId;

        let l0 = LocalId::new(0);
        let mut lets = no_lets();
        lets.insert(
            l0,
            MirRelationExpr::Project {
                outputs: vec![0],
                input: Box::new(src(R_ID, 3)),
            },
        );
        let local_get = MirRelationExpr::Get {
            id: Id::Local(l0),
            typ: ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let input = MirRelationExpr::Filter {
            predicates: vec![lit_pred()],
            input: Box::new(local_get),
        };
        let (gid, got) =
            bare_filter_over_global_get(&input, &lets).expect("let-bound filter is a candidate");
        assert_eq!(gid, GlobalId::Transient(R_ID));
        assert_eq!(
            got,
            MirRelationExpr::Filter {
                predicates: vec![lit_pred()],
                input: Box::new(src(R_ID, 3)),
            }
        );
    }

    #[mz_ore::test]
    fn bare_filter_rejects_non_get() {
        // A filter over a constant (no global Get) is not a candidate.
        let input = MirRelationExpr::Filter {
            predicates: vec![lit_pred()],
            input: Box::new(MirRelationExpr::constant(
                vec![],
                ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false); 2]),
            )),
        };
        assert!(bare_filter_over_global_get(&input, &no_lets()).is_none());
    }

    /// An empty `Let`-definition map, for helper tests with no bindings.
    fn no_lets() -> BTreeMap<mz_expr::LocalId, MirRelationExpr> {
        BTreeMap::new()
    }
}
