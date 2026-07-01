// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Equality-saturation rewrites over a subset of MIR relational expressions.
//! See `docs/superpowers/specs/2026-06-19-mir-egraph-saturation-pass-design.md`.

// This module ports a self-contained prototype; the repo-wide bans on std hash
// collections and `Iterator::zip` (determinism conventions elsewhere) do not
// apply to the ported engine. The ported modules also use numeric `as` casts
// pervasively; rewriting them would obscure the prototype's intent. The crate's
// `missing_docs`/`missing_debug_implementations` lints likewise do not fit the
// ported code, so they are relaxed for this module subtree.
#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::as_conversions,
    missing_docs,
    missing_debug_implementations
)]

pub mod analysis;
mod colored;
pub(crate) mod colored_derive;
mod core;
pub mod cost;
pub mod cse;
pub mod dsl;
pub mod egraph;
pub mod engine;
pub mod extract;
pub mod ir;
pub(crate) mod join_commit;
pub(crate) mod join_spelling;
pub mod lean;
pub mod lower;
pub mod matcher;
pub mod objective;
pub mod raise;
pub mod rules;
pub mod scalar;
pub(crate) mod scalar_extract;
pub mod transform;

#[cfg(test)]
mod validation;

pub use transform::{EqSatTransform, PhysicalEqSatTransform};

use mz_expr::{MirRelationExpr, MirScalarExpr};
use mz_repr::GlobalId;
use std::collections::BTreeMap;

/// The built-in rule set, compiled from `eqsat/rules/relational.rewrite` at
/// build time (see [`rules`]).
pub fn default_ruleset() -> rules::CompiledRuleSet {
    rules::all()
}

/// The built-in rules as AST literals, for the Lean emitter (`gen-lean`).
pub fn rules_ast() -> dsl::RuleSet {
    rules::rules_ast()
}

/// Optimize `expr` by equality saturation over the supported relational subset,
/// bailing per-subtree on unsupported variants. Functionally equivalent output.
///
/// Commits each cyclic join to the delta or the differential strategy, whichever
/// needs fewer new arrangements (reuse-aware), via the real planners. The output
/// therefore carries filled-in join implementations and physical-phase structure
/// (arranged inputs, lifted MFPs), so it is only valid after `JoinImplementation`.
/// The live logical-phase transform uses [`optimize_logical`] instead.
pub fn optimize(expr: MirRelationExpr) -> MirRelationExpr {
    // The offline path is the maximal one: it keeps the FULL rule set (all
    // phases) so tests and compare_real exercise every rule, physical ones
    // included.
    optimize_inner(
        expr,
        true,
        BTreeMap::new(),
        default_ruleset(),
        true,
        Vec::new(),
        false,
        false,
        false,
        raise::NativeJoinFlags::none(),
    )
}

/// Like [`optimize`], but with the non-recursive `Let`-definition union turned
/// off. This is the control half of the Let-crossing-win test: a fact proven on
/// a binding definition stays trapped behind the `Get`, so a body rewrite that
/// needs that fact cannot fire. Not for production use.
#[doc(hidden)]
pub fn optimize_without_let_union(expr: MirRelationExpr) -> MirRelationExpr {
    optimize_inner(
        expr,
        true,
        BTreeMap::new(),
        default_ruleset(),
        false,
        Vec::new(),
        false,
        false,
        false,
        raise::NativeJoinFlags::none(),
    )
}

/// Like [`optimize`], but seeds the cost model with arrangement/index
/// availability so the WcoJoin-vs-binary-join decision is index-aware.
///
/// `available` maps each global relation id to the list of index keys
/// available on it (as reported by the `IndexOracle`).  Join inputs that are
/// already arranged by their join key are not charged the arrangement-build
/// memory term, making WcoJoin correctly cheaper when those arrangements exist.
///
/// `seeds` carries indexed-filter decisions the production detector found on
/// the input plan; they are equated into each fragment's e-graph before
/// saturation so cost-based extraction can choose the index lookup.
///
/// `use_ilp` selects the [`extract::IlpExtractor`] instead of the default
/// greedy extractor when `true`. Default is `false` (greedy).
///
/// This is the entry point used by [`PhysicalEqSatTransform`].
///
/// [`PhysicalEqSatTransform`]: transform::PhysicalEqSatTransform
pub fn optimize_with_availability(
    expr: MirRelationExpr,
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    seeds: Vec<egraph::IndexedFilterSeed>,
    use_ilp: bool,
    use_delta: bool,
    flags: crate::eqsat::raise::NativeJoinFlags,
) -> MirRelationExpr {
    // Live physical pass: the cost model sees arrangement / index availability,
    // so arrangement-sensitive rules (`phase physical`) may fire here.
    let rules = default_ruleset().for_phase(dsl::Phase::Physical);
    optimize_inner(
        expr, true, available, rules, true, seeds, use_ilp, false, use_delta, flags,
    )
}

/// Like [`optimize`], but emits worst-case-optimal joins as plain
/// `Unimplemented` joins rather than committing them to `DeltaQuery`. The output
/// carries only logical-phase structure (no arranged inputs, no filled
/// implementations), so it is valid where the logical optimizer runs.
pub fn optimize_logical(expr: MirRelationExpr, enable_wmr_lift: bool) -> MirRelationExpr {
    // Live logical pass: the cost model cannot see arrangement availability, so
    // arrangement-sensitive rules (`phase physical`) are filtered out here.
    let rules = default_ruleset().for_phase(dsl::Phase::Logical);
    optimize_inner(
        expr,
        false,
        BTreeMap::new(),
        rules,
        true,
        Vec::new(),
        false,
        enable_wmr_lift,
        false,
        raise::NativeJoinFlags::none(),
    )
}

/// Like [`optimize`], but with the WMR-lift on: `LetRec` references are lowered
/// with iteration-version tags, and CSE may hoist a subterm shared across
/// recursive bindings into a new binding placed at a version-valid order slot.
///
/// This is the offline entry point that exercises the lift end to end (the live
/// transform gates it on `enable_eqsat_wmr_lift`). Not for production use.
#[doc(hidden)]
pub fn optimize_with_wmr_lift(expr: MirRelationExpr) -> MirRelationExpr {
    optimize_inner(
        expr,
        true,
        BTreeMap::new(),
        default_ruleset(),
        true,
        Vec::new(),
        false,
        true,
        false,
        raise::NativeJoinFlags::none(),
    )
}

fn optimize_inner(
    expr: MirRelationExpr,
    commit_wcoj: bool,
    available: BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    rules: rules::CompiledRuleSet,
    union_let_defs: bool,
    seeds: Vec<egraph::IndexedFilterSeed>,
    use_ilp: bool,
    enable_wmr_lift: bool,
    use_delta: bool,
    flags: crate::eqsat::raise::NativeJoinFlags,
) -> MirRelationExpr {
    let rel = lower::lower_with(&expr, enable_wmr_lift);
    // The raise step also consults `available` to make the delta-vs-differential
    // commit decision reuse-aware, so keep a copy before the cost model takes it.
    let available_for_raise = available.clone();
    let model = if available.is_empty() {
        cost::CostModel::new()
    } else {
        cost::CostModel::with_available(available)
    };
    let optimizer = engine::Optimizer::new(rules, model).with_seeds(seeds);
    let optimizer = if union_let_defs {
        optimizer
    } else {
        optimizer.without_let_union()
    };
    let optimizer = if use_ilp {
        optimizer.with_extractor(std::sync::Arc::new(extract::IlpExtractor))
    } else {
        optimizer
    };
    let optimizer = optimizer.with_delta_join_cost(use_delta);
    let best = optimizer.optimize(rel).plan;
    // Hoist e-classes referenced more than once into Let bindings, turning the
    // extracted tree back into a DAG with sharing. This subsumes RelationCSE:
    // shared compound subterms are bound once and referenced by LocalGet.
    let best = crate::eqsat::cse::eliminate_common_subexpressions(
        &best,
        &available_for_raise,
        enable_wmr_lift,
    );
    // The equivalence-preserving arity guard lives at the live transform
    // boundary (`EqSatTransform`), which adopts this output only if its arity
    // matches the input. Direct test callers assert arity themselves.
    let mut raised = raise::raise(&best, commit_wcoj, &available_for_raise, flags);
    // Coalesce each maximal Map/Filter/Project run into canonical form. The
    // e-graph fusion rules subsume Map-Map and Filter-Filter fusion (via
    // fuse_maps and merge_filters), but two gaps remain unsubsumed: scalar
    // inlining through Map/Project boundaries (substituting a Project's
    // columns into the scalars of a Map above it) and predicate simplification
    // (e.g., null-check filters that a column's non-null type makes vacuous and
    // can be dropped). Both require the MFP
    // extraction machinery in CanonicalizeMfp, which needs type information
    // unavailable during saturation. Include-for-now pending a type-aware
    // saturation layer or a typed extraction phase.
    raise::coalesce_mfp(&mut raised);
    // Acquire column-liveness by reusing the production Demand and
    // ProjectionPushdown passes over the raised plan. The e-graph search does
    // not reason about demand, so folding these in here lets the pipeline drop
    // its standalone Demand/ProjectionPushdown runs once eqsat moves earlier.
    raise::demand_pushdown(&mut raised, commit_wcoj);
    // Tidy the projections that demand pushdown introduces, mirroring the
    // production pipeline which always re-runs CanonicalizeMfp after
    // ProjectionPushdown. Without this, demand pushdown can leave redundant
    // nested Project nodes that the downstream pipeline would otherwise clean
    // up (which is why the optimized-plan SLT gate did not surface them).
    raise::coalesce_mfp(&mut raised);
    // We deliberately do NOT re-run the production `fixpoint_logical_02`
    // (SemijoinIdempotence, ReductionPushdown, ReduceElision, ReduceReduction,
    // LiteralLifting, RelationCSE, FuseAndCollapse) here. `logical_optimizer`
    // already runs that fixpoint BEFORE this pass, and the e-graph treats
    // Reduce/Join nodes as opaque leaves it never disturbs, so a second run is
    // redundant: it leaves the plan unchanged apart from fusing the occasional
    // redundant `Project` that `demand_pushdown` introduces above an opaque
    // Reduce, which the downstream `ProjectionPushdown`/physical passes clean up
    // regardless. The one effect we must preserve, `ReduceReduction`'s split of
    // a mixed-reduction `Reduce` (else `ReducePlan::create_from` panics on
    // lowering), runs unconditionally inside `raise` via `split_mixed_reductions`
    // in both phases, independent of this fixpoint.
    raised
}
