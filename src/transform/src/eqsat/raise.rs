// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Translate a `Rel` back into a real `MirRelationExpr`, reading scalars
//! directly off their `MirScalarExpr` payloads and re-emitting bailed leaves
//! verbatim.
//!
//! This is the structural inverse of [`crate::eqsat::lower::lower`]. The
//! round-trip is semantics-preserving, scalar-canonicalizing, and
//! MFP-canonicalizing rather than byte-identical: lower reduces every scalar
//! payload, and the post-raise `coalesce_mfp` pass coalesces each maximal
//! Map/Filter/Project run into canonical Map-then-Filter-then-Project form
//! (reusing the production `CanonicalizeMfp` machinery). Together,
//! `raise(lower(x))` returns `x` with scalars in `MirScalarExpr::reduce`
//! canonical form and with MFP runs in `MapFilterProject` canonical form.

use std::collections::BTreeMap;

use mz_expr::visit::VisitChildren;
use mz_expr::{
    AccessStrategy, Columns, Id, LocalId, MapFilterProject, MirRelationExpr, MirScalarExpr,
};
use mz_ore::cast::CastFrom;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, ReprRelationType, ReprScalarType};

use crate::canonicalize_mfp::CanonicalizeMfp;
use crate::dataflow::DataflowMetainfo;
use crate::demand::Demand;
use crate::eqsat::ir::{EScalar, RecVersion, Rel};
use crate::movement::ProjectionPushdown;
use crate::reduce_reduction::ReduceReduction;
use crate::typecheck::empty_typechecking_context;
use crate::{Transform, TransformCtx};

/// Flags controlling the physical join-commit path. Replaces the former lone
/// `native_join_commit` bool so Part A (`prioritize_arranged`) and Part B
/// (`eager_delta`) thread without growing the positional arg list.
#[derive(Clone, Copy, Debug)]
pub struct NativeJoinFlags {
    /// Commit acyclic joins to a cost-model Differential/DeltaQuery so
    /// `JoinImplementation` no-ops on them.
    pub commit: bool,
    /// Select the V2 `JoinInputCharacteristics` layout (`enable_join_prioritize_arranged`).
    pub prioritize_arranged: bool,
    /// Allow delta when `delta_new <= diff_new` (not just `== 0`); reads
    /// `enable_eager_delta_joins`.
    pub eager_delta: bool,
}

impl NativeJoinFlags {
    /// All-off: no native commit. Used by logical/offline entry points and tests.
    pub fn none() -> Self {
        NativeJoinFlags {
            commit: false,
            prioritize_arranged: false,
            eager_delta: false,
        }
    }
}

/// Raise `rel` to a `MirRelationExpr`. Inverse of [`crate::eqsat::lower::lower`].
///
/// Scalars are read directly off their `MirScalarExpr` payloads. `Rel::Opaque`
/// leaves re-emit their stored subtree verbatim. Local `Get`s return the
/// original node carried at lower time, preserving their exact type.
/// CSE-introduced `LocalGet { get: None }` nodes are raised to
/// `MirRelationExpr::Get { Id::Local, .. }` using the type of the bound value,
/// which is threaded via `scope` in `raise_inner`.
///
/// `Rel::LetRec` raises to `MirRelationExpr::LetRec` by raising each binding
/// value and the body in order, converting the `usize` ids back to `LocalId`.
/// The recursive back-edges inside binding values are sealed `LocalGet {
/// get: Some(..) }` leaves and raise verbatim, so no scope dependency arises
/// across mutual-recursion bindings.
///
/// When `commit_wcoj` is set, a [`Rel::WcoJoin`] is planned reuse-aware via the
/// real planners: it commits the delta strategy only when delta needs no more
/// new arrangements than a differential plan would, otherwise the differential
/// plan (physical-phase output). When it is clear, the same node is raised as a
/// plain `Unimplemented` join, the only form valid in the logical optimizer.
///
/// `raise`'s output never contains a mixed-reduction `Reduce` (one whose
/// aggregates span more than one `ReductionType`): such a Reduce is split
/// internally via `ReduceReduction` before returning, so the result lowers
/// without the `ReducePlan::create_from` panic regardless of caller or phase.
/// The split runs unconditionally here because no post-pass splits afterward.
pub fn raise(
    rel: &Rel,
    commit_wcoj: bool,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    flags: NativeJoinFlags,
) -> MirRelationExpr {
    let mut scope = BTreeMap::new();
    let mut raised = raise_inner(rel, commit_wcoj, available, flags, &mut scope);
    // Make the raised plan independently safe to lower. `ReducePlan::create_from`
    // panics on a single `Reduce` mixing reduction types, and only this split
    // breaks it apart. It is logical-safe in any phase and a no-op when there is
    // nothing to split, so it runs in both phases. No post-pass splits afterward,
    // so this is the sole splitter on the eqsat path.
    split_mixed_reductions(&mut raised);
    raised
}

/// Inner recursive raise, carrying `scope`: a map from CSE-bound local ids to
/// the `ReprRelationType` of their bound value. Populated when entering a
/// `Rel::Let` arm and consumed by `Rel::LocalGet { get: None }` arms.
fn raise_inner(
    rel: &Rel,
    commit_wcoj: bool,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    flags: NativeJoinFlags,
    scope: &mut BTreeMap<usize, ReprRelationType>,
) -> MirRelationExpr {
    let raise = |r: &Rel, scope: &mut BTreeMap<usize, ReprRelationType>| {
        raise_inner(r, commit_wcoj, available, flags, scope)
    };
    match rel {
        Rel::Opaque(m) => (**m).clone(),
        Rel::LocalGet {
            get,
            id,
            version,
            arity: _,
        } => {
            // Version is informational only on the way out: binding order (set by
            // the LetRec arm) realizes the version, so every versioned get raises
            // to a plain Get(Local id). A None get with a Final/outer version
            // would be a placeholder we cannot type; that must not occur.
            debug_assert!(
                get.is_some() || *version != Some(RecVersion::Final),
                "Final-version placeholder without an original node (id {id})"
            );
            // The exact original MirRelationExpr::Get{Local} node was carried at
            // lower time; return it verbatim so types are preserved.
            // A `None` means a CSE-introduced placeholder: emit a local Get with
            // the type of the bound value, threaded through `scope`.
            match get {
                Some(g) => (**g).clone(),
                None => {
                    let typ = scope.get(id).unwrap_or_else(|| {
                        panic!("raise of a placeholder LocalGet (id {id}) without an original node and no scope entry")
                    });
                    MirRelationExpr::Get {
                        id: Id::Local(LocalId::new(u64::cast_from(*id))),
                        typ: typ.clone(),
                        access_strategy: AccessStrategy::UnknownOrLocal,
                    }
                }
            }
        }
        Rel::Get { name, .. } => {
            unreachable!("lowering never emits Rel::Get (test-only base); got {name:?}")
        }
        Rel::Project { input, outputs } => raise(input, scope).project(outputs.clone()),
        Rel::Map { input, scalars } => raise(input, scope).map(resolve(scalars)),
        Rel::Filter { input, predicates } => raise(input, scope).filter(resolve(predicates)),
        Rel::Join {
            inputs,
            equivalences,
        } => {
            // `join_scalars` intentionally drops constant-singleton (arity-0,
            // 1-row) inputs that act as join identities, so a round-trip is not
            // byte-identical for such inputs but is arity- and
            // semantics-preserving.
            let raised_inputs: Vec<MirRelationExpr> =
                inputs.iter().map(|r| raise(r, scope)).collect();
            // The join with the equivalences exactly as extraction spelled them.
            // This is the value returned on every non-commit path, so it is
            // byte-identical to before for flag-off and all fallbacks.
            let join = MirRelationExpr::join_scalars(
                raised_inputs.clone(),
                equivalences.iter().map(|class| resolve(class)).collect(),
            );
            // Physical phase only: commit acyclic joins to a cost-model-chosen
            // Differential/DeltaQuery so JoinImplementation no-ops on them. On any
            // failure, fall back to the bare Unimplemented join and let
            // JoinImplementation handle it.
            if commit_wcoj && flags.commit {
                commit_join(inputs, &raised_inputs, equivalences, available, flags).unwrap_or(join)
            } else {
                join
            }
        }
        Rel::Negate { input } => raise(input, scope).negate(),
        Rel::Threshold { input } => raise(input, scope).threshold(),
        Rel::Union { base, inputs } => {
            // Use the enum directly rather than the .union() builder to
            // preserve the exact n-ary structure without flattening.
            MirRelationExpr::Union {
                base: Box::new(raise(base, scope)),
                inputs: inputs.iter().map(|r| raise(r, scope)).collect(),
            }
        }
        Rel::Let { id, value, body } => {
            // Raise the bound value, compute its type, insert into scope for the
            // body, then remove after (scope is lexically scoped by the binding).
            let mir_value = raise(value, scope);
            let typ = mir_value.typ();
            scope.insert(*id, typ);
            let mir_body = raise(body, scope);
            scope.remove(id);
            MirRelationExpr::Let {
                id: LocalId::new(u64::cast_from(*id)),
                value: Box::new(mir_value),
                body: Box::new(mir_body),
            }
        }
        Rel::Constant {
            arity, col_types, ..
        } => {
            // Saturation rules (`empty_false_filter`, `union_cancel`) synthesize
            // `Empty(r)` nodes that the engine encodes as `Constant { card: 0,
            // arity }`. Extraction can pick such a node as cheapest; raise it to
            // an empty relation. When the synthesizing rule captured the real
            // column types of the replaced relation, use them so the emitted
            // empty carries the correct type and survives the final strict
            // typecheck. Without captured types (engine unit tests build these
            // directly), fall back to an arity-only placeholder.
            let typ = match col_types {
                Some(col_types) => ReprRelationType::new(col_types.clone()),
                None => repr_type_of_arity(*arity),
            };
            MirRelationExpr::constant(vec![], typ)
        }
        Rel::Reduce {
            input,
            group_key,
            aggregates,
            monotonic,
            expected_group_size,
        } => MirRelationExpr::Reduce {
            input: Box::new(raise(input, scope)),
            group_key: group_key.iter().map(|s| s.expr.clone()).collect(),
            aggregates: aggregates.clone(),
            monotonic: *monotonic,
            expected_group_size: *expected_group_size,
        },
        Rel::TopK { input, shape } => MirRelationExpr::TopK {
            input: Box::new(raise(input, scope)),
            group_key: shape.group_key.clone(),
            order_key: shape.order_key.clone(),
            limit: shape.limit.clone(),
            offset: shape.offset,
            monotonic: shape.monotonic,
            expected_group_size: shape.expected_group_size,
        },
        // One `Rel::ArrangeBy` carries a single key list; MIR keeps a list of
        // keys, so re-wrap it as a one-element list of keys.
        Rel::ArrangeBy { input, key } => MirRelationExpr::ArrangeBy {
            input: Box::new(raise(input, scope)),
            keys: vec![key.iter().map(|s| s.expr.clone()).collect()],
        },
        // `Rel::ArrangeByMany` holds the full key list directly; flatten each
        // inner key list into MIR scalars and pass all of them as `keys`.
        Rel::ArrangeByMany { input, keys } => MirRelationExpr::ArrangeBy {
            input: Box::new(raise(input, scope)),
            keys: keys
                .iter()
                .map(|key| key.iter().map(|s| s.expr.clone()).collect())
                .collect(),
        },
        Rel::IndexedFilter {
            input,
            predicates,
            committed,
        } => {
            // An indexed filter is the e-graph's physical decision to evaluate a
            // literal-constraint `Filter(Get)` as an index lookup. In the physical
            // phase, commit it by emitting `committed`, the production
            // `LiteralConstraints` realization captured at seed time (a semi-join
            // `Join { implementation: IndexedFilter(..) }` that survives
            // JoinImplementation unchanged). The `input` child exists only for
            // cost and analysis. `committed` already embeds the get.
            if commit_wcoj {
                (**committed).clone()
            } else {
                // Logical phase: emit the equivalent plain filter. The node is
                // only ever seeded in the physical pass, so this arm is for
                // totality and is not reached on the production path.
                raise(input, scope).filter(resolve(predicates))
            }
        }
        Rel::WcoJoin {
            inputs,
            equivalences,
        } => {
            // The WcoJoin variant is the e-graph's cost-model decision that this
            // cyclic join is a worst-case-optimal candidate. The final
            // delta-vs-differential choice is made here, reuse-aware: commit the
            // delta strategy only when it needs no more new arrangements than a
            // differential plan would. This mirrors production's eager delta rule
            // and avoids rebuilding arrangements that an existing index already
            // provides. JoinImplementation only (re)plans Unimplemented and
            // Differential joins, so a committed DeltaQuery survives downstream.
            let raised_inputs: Vec<MirRelationExpr> =
                inputs.iter().map(|r| raise(r, scope)).collect();
            let join = MirRelationExpr::join_scalars(
                raised_inputs.clone(),
                equivalences.iter().map(|class| resolve(class)).collect(),
            );
            if !commit_wcoj {
                // Logical-phase output: leave the join `Unimplemented`. Committing
                // an implementation here produces physical-phase structure
                // (arranged inputs, filled implementation) that is invalid before
                // JoinImplementation.
                return join;
            }
            // Build per-input arrangement availability so the planners credit
            // existing indexes. If planning fails (the join folded to a non-join,
            // or the graph is not connected), fall back to the plain join and let
            // JoinImplementation choose.
            let per_input = per_input_available(&raised_inputs, available);
            let features = OptimizerFeatures::default();
            match crate::join_implementation::plan_join_min_arrangements(
                &join, &per_input, &features,
            ) {
                Ok(planned) => planned,
                Err(_) => join,
            }
        }
        // FlatMap: raise the input, then reconstruct the MIR node with the
        // original func and the scalar payloads read directly off the EScalars.
        Rel::FlatMap { input, func, exprs } => MirRelationExpr::FlatMap {
            input: Box::new(raise(input, scope)),
            func: func.clone(),
            exprs: exprs.iter().map(|s| s.expr.clone()).collect(),
        },
        Rel::LetRec {
            bindings,
            limits,
            body,
        } => {
            // Raise each binding value and the body carrying scope. The
            // recursive back-edges lower to sealed LocalGet { get: Some(..) }
            // leaves and raise verbatim without touching scope, so no circular
            // dependency on scope arises across mutual-recursion bindings.
            let ids: Vec<LocalId> = bindings
                .iter()
                .map(|(id, _)| LocalId::new(u64::cast_from(*id)))
                .collect();
            // Raise binding values in order, inserting each binding's type into
            // `scope` after raising it. A CSE-introduced reference
            // (`LocalGet { get: None }`) to a hoisted binding resolves its type
            // from `scope`, so the hoisted binding must be raised before the
            // bindings that read it. The version-aware placement in CSE
            // guarantees that order (a hoisted binding sits before its
            // consumers). Sealed recursive back-edges carry `get: Some` and
            // raise verbatim, so they never consult `scope`.
            let mut values = Vec::with_capacity(bindings.len());
            for (id, v) in bindings {
                let mir_value = raise(v, scope);
                scope.insert(*id, mir_value.typ());
                values.push(mir_value);
            }
            let mir_body = raise(body, scope);
            for (id, _) in bindings {
                scope.remove(id);
            }
            MirRelationExpr::LetRec {
                ids,
                values,
                limits: limits.clone(),
                body: Box::new(mir_body),
            }
        }
    }
}

/// Commit an acyclic join to a cost-model-chosen Differential or DeltaQuery so
/// `JoinImplementation` no-ops on it. Returns the committed join, or `None` to
/// fall back to the bare `Unimplemented` join (a constant-singleton input, an
/// unkeyed cross-join step, or a delta/differential planning failure).
///
/// `inputs` are the `Rel` join inputs (for the cost-model order search),
/// `raised_inputs` their already-raised MIR forms (for arrangement
/// installation), `equivalences` the join's equivalence classes, `available` the
/// index availability map, and `flags` the native-commit flags. Takes the
/// already-raised inputs so it needs no scope, callable on any single join.
fn commit_join(
    inputs: &[Rel],
    raised_inputs: &[MirRelationExpr],
    equivalences: &[Vec<EScalar>],
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    flags: NativeJoinFlags,
) -> Option<MirRelationExpr> {
    // Don't commit if any raised input is a constant singleton:
    // join_scalars drops them, creating an index mismatch with the
    // order computed from the original (pre-drop) Rel inputs.
    if raised_inputs.iter().any(|i| i.is_constant_singleton()) {
        return None;
    }
    // Canonicalize the equivalences with the production canonicalizer
    // before planning. This is what makes every other optimizer path
    // build-profile-stable: each class collapses to one canonical
    // representative (lowest-complexity / lowest column), and that
    // representative is substituted into expressions across classes,
    // deterministically. Without it, the eqsat-extracted spelling (e.g.
    // `#2` vs `#0` inside a null-pad CASE) can differ by build profile and
    // flip the chosen join order, making goldens flaky. We commit a
    // Differential, for which the canonical spelling is still fully keyed
    // (no cross), so canonicalizing here is safe. Done only on the commit
    // path, so JoinImplementation still sees the original spelling on every
    // fallback (it canonicalizes Unimplemented joins itself).
    let mut canon_equivs: Vec<Vec<MirScalarExpr>> =
        equivalences.iter().map(|class| resolve(class)).collect();
    let input_types: Vec<_> = raised_inputs.iter().map(|i| i.typ()).collect();
    mz_expr::canonicalize::canonicalize_equivalences(
        &mut canon_equivs,
        input_types.iter().map(|t| &t.column_types),
    );
    // Compute the order over the canonical equivalences (converted back to
    // the cost model's `EScalar` IR) so the order matches the committed
    // spelling and is build-profile-stable.
    let canon_escalars: Vec<Vec<crate::eqsat::ir::EScalar>> = canon_equivs
        .iter()
        .map(|class| {
            class
                .iter()
                .cloned()
                .map(crate::eqsat::ir::EScalar::plain)
                .collect()
        })
        .collect();
    let model = if available.is_empty() {
        crate::eqsat::cost::CostModel::new()
    } else {
        crate::eqsat::cost::CostModel::with_available(available.clone())
    };
    let order = model.binary_join_order(inputs, &canon_escalars)?;
    // Abort if any non-start step has no key columns: that step would
    // be a cross join in the Differential plan, which is no improvement
    // over what JoinImplementation already produces, and the empty
    // lookup key can trigger a `find_bound_expr` panic in LIR lowering
    // for joins whose equivalences have complex (non-column) members.
    // frontier_key_cols correctly returns an empty set when the chosen
    // order lacks a frontier connection at that step.
    if order.steps[1..].iter().any(|s| s.key_cols.is_empty()) {
        return None;
    }
    let per_input = per_input_available(raised_inputs, available);
    // Delta only helps for 3+ inputs: it avoids the intermediate
    // arrangements a differential chain builds (k-2 of them). A 1-input
    // "join" is a filter whose delta plan is unrenderable (empty
    // per-driver path panics source_keys derivation in
    // delta_join.rs:98-110); a 2-input join has no intermediate
    // arrangement to save and delta would add a second path for nothing.
    // Match JoinImplementation, which forces differential for
    // num_inputs <= 2 (join_implementation.rs:408-429).
    if inputs.len() > 2 {
        // Try a delta commit first: it is preferred when it needs no new
        // arrangements (strict) or no more than differential (eager).
        // Viability is delta_join_order returning Some (every step
        // keyed); a disconnected join has no delta plan and stays
        // differential.
        if let Some(paths) = model.delta_join_order(inputs, &canon_escalars) {
            let delta_new = crate::eqsat::join_commit::delta_new_arrangements(&paths, &per_input);
            let commit_delta = if flags.eager_delta {
                delta_new <= differential_new_arrangements(&order, &per_input)
            } else {
                delta_new == 0
            };
            if commit_delta {
                let canon_join =
                    MirRelationExpr::join_scalars(raised_inputs.to_vec(), canon_equivs.clone());
                if let Some(j) = crate::eqsat::join_commit::commit_delta_query(
                    canon_join,
                    paths,
                    &per_input,
                    raised_inputs,
                    flags.prioritize_arranged,
                ) {
                    return Some(j);
                }
            }
        }
    }
    // Fall through to the SP-B1 differential commit. The committed join
    // carries the canonical equivalences, matching the order computed
    // above.
    let canon_join = MirRelationExpr::join_scalars(raised_inputs.to_vec(), canon_equivs);
    crate::eqsat::join_commit::commit_differential(
        canon_join,
        order,
        &per_input,
        raised_inputs,
        flags.prioritize_arranged,
    )
}

/// New arrangements a differential plan needs, mirroring `differential::plan`
/// (join_implementation.rs:898): `inputs.len().saturating_sub(2) + (start
/// arrangement is new ? 1 : 0)`.
///
/// `available` is the per-input arrangement availability. The start's new
/// arrangement counts iff its aligned start key is not already available, but at
/// decision time we approximate with the start key the cost model produced (the
/// eager comparison is heuristic in JI too).
fn differential_new_arrangements(
    order: &crate::eqsat::cost::JoinOrder,
    available: &[Vec<Vec<MirScalarExpr>>],
) -> usize {
    let n = order.steps.len();
    let start = &order.steps[0];
    let start_key: Vec<MirScalarExpr> = start
        .key_cols
        .iter()
        .map(|&c| MirScalarExpr::column(c))
        .collect();
    let start_new = if available[start.input]
        .iter()
        .any(|k| k.as_slice() == start_key.as_slice())
    {
        0
    } else {
        1
    };
    n.saturating_sub(2) + start_new
}

/// Build per-input arrangement availability for a join's raised inputs.
///
/// For each input, collect the arrangement keys already materialized on it: the
/// index keys of a global `Get` (from `available`), explicit `ArrangeBy` keys, a
/// `Reduce`'s group-key prefix, and an `IndexedFilter`'s index keys.
///
/// Leading `Map`/`Filter`/`Project` wrappers are extracted as one `MapFilterProject`
/// (via [`MapFilterProject::extract_non_errors_from_expr`], the exact same extraction
/// `join_implementation::implement_arrangements` performs when it lifts the MFP to
/// reuse an index). The result's keys are reported in the input's PROJECTED column
/// space, so a base index whose key columns survive a pure column-selecting
/// `Project` is credited at those columns' projected positions. `implement_arrangements`
/// then lifts the projection above the arrangement and permutes the needed key back
/// down to the base. A base key is dropped when any of its columns does not survive
/// the projection, matching the case where the lift could not reproduce it.
///
/// The result feeds the delta and differential planners so neither charges an
/// arrangement that an existing index already provides.
fn per_input_available(
    inputs: &[MirRelationExpr],
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
) -> Vec<Vec<Vec<MirScalarExpr>>> {
    inputs
        .iter()
        .map(|input| {
            // Extract the leading error-free Map/Filter/Project run, reaching the
            // non-MFP base under it. `project` maps each projected (input-space)
            // column to the base column it selects.
            let (mfp, node) = MapFilterProject::extract_non_errors_from_expr(input);
            let (_, _, project) = mfp.as_map_filter_project();
            // Invert `project`: base column -> a projected position that selects
            // it. When a column is selected more than once, the first position is
            // used; a duplicated key column then simply fails to match a wider
            // needed key, which is conservative (a fresh arrangement is built).
            let mut base_to_projected: BTreeMap<usize, usize> = BTreeMap::new();
            for (projected_pos, &base_col) in project.iter().enumerate() {
                base_to_projected.entry(base_col).or_insert(projected_pos);
            }
            // Lift a base-space key into the projected column space, or drop it if
            // any referenced column does not survive the projection.
            let lift = |key: &[MirScalarExpr]| -> Option<Vec<MirScalarExpr>> {
                key.iter()
                    .map(|e| {
                        if e.support()
                            .iter()
                            .all(|c| base_to_projected.contains_key(c))
                        {
                            let mut e = e.clone();
                            e.permute_map(&base_to_projected);
                            Some(e)
                        } else {
                            None
                        }
                    })
                    .collect()
            };
            let mut keys: Vec<Vec<MirScalarExpr>> = Vec::new();
            match node {
                MirRelationExpr::Get {
                    id: Id::Global(gid),
                    ..
                } => {
                    if let Some(idx) = available.get(gid) {
                        keys.extend(idx.iter().filter_map(|k| lift(k)));
                    }
                }
                MirRelationExpr::ArrangeBy {
                    keys: arr_keys,
                    input,
                } => {
                    keys.extend(arr_keys.iter().filter_map(|k| lift(k)));
                    if let MirRelationExpr::Get {
                        id: Id::Global(gid),
                        ..
                    } = &**input
                    {
                        if let Some(idx) = available.get(gid) {
                            keys.extend(idx.iter().filter_map(|k| lift(k)));
                        }
                    }
                }
                MirRelationExpr::Reduce { group_key, .. } => {
                    let group_key: Vec<MirScalarExpr> =
                        (0..group_key.len()).map(MirScalarExpr::column).collect();
                    if let Some(k) = lift(&group_key) {
                        keys.push(k);
                    }
                }
                MirRelationExpr::Join {
                    implementation: mz_expr::JoinImplementation::IndexedFilter(gid, ..),
                    ..
                } => {
                    if let Some(idx) = available.get(gid) {
                        keys.extend(idx.iter().filter_map(|k| lift(k)));
                    }
                }
                _ => {}
            }
            keys.sort();
            keys.dedup();
            keys
        })
        .collect()
}

/// Coalesce each maximal Map/Filter/Project run in `expr` into canonical
/// Map-then-Filter-then-Project form, bottom up.
///
/// Reuses `MapFilterProject::extract_non_errors_from_expr_mut`,
/// `MapFilterProject::optimize`, and `CanonicalizeMfp::rebuild_mfp` (which
/// includes `fusion::filter::Filter::action` for predicate canonicalization).
/// This produces output identical to what the production `CanonicalizeMfp`
/// transform emits, so eqsat fully subsumes that transform.
/// Runs only on Map/Filter/Project nodes, so it never touches Join
/// implementations or disturbs the logical-phase joins-Unimplemented contract.
pub(crate) fn coalesce_mfp(expr: &mut MirRelationExpr) {
    // Guard: the eqsat pass can produce Map-then-Project sequences where the
    // Project references a column beyond the base arity (e.g. a Map's own
    // output column folded back via `map_columns_to_projection`). Such chains
    // are invalid MIR, and `extract_non_errors_from_expr_mut` panics on them.
    // Skip coalescing for chains that fail this check; the downstream pipeline
    // (CanonicalizeMfp) will handle them after type information is available.
    if !mfp_chain_valid(expr) {
        // Still recurse into children of the base so inner valid chains are
        // coalesced. Walk past the M/F/P prefix to find the non-MFP base and
        // recurse into its children directly.
        coalesce_mfp_children_of_base(expr);
        return;
    }
    // Extract the maximal error-free MFP run at the root of `expr`, stripping
    // all Map/Filter/Project layers down to the non-MFP base. This matches the
    // approach used in `CanonicalizeMfp::action`: extract first, then recurse
    // into the base's children, then rebuild. Extracting before recursing avoids
    // visiting intermediate M/F/P nodes as if they were independent roots (which
    // would cause double-processing and mismatched column arities).
    let mut mfp = MapFilterProject::extract_non_errors_from_expr_mut(expr);
    mfp.optimize();
    // Recurse into the children of the base (non-MFP node now in `expr`).
    // `VisitChildren::visit_mut_children` visits direct children only, so each
    // child recursion independently coalesces its own MFP run.
    expr.visit_mut_children(coalesce_mfp);
    // Rebuild the optimized MFP on top of the now-coalesced base using the
    // production canonicalizer, which also runs Filter::action (predicate
    // canonicalization: sort, dedup, split conjuncts, reduce).
    // Pass `false` for the eqsat scalar canonicalizer: predicates raised here
    // already come from the saturated e-graph, so re-running the scalar
    // canonicalizer would be redundant.
    CanonicalizeMfp::rebuild_mfp(mfp, expr, false);
}

/// Demand-narrow the raised plan by reusing the production `Demand` and
/// `ProjectionPushdown` passes, seeded at the root with full demand.
///
/// The e-graph does not reason about column liveness during search, so this
/// post-extraction pass acquires it the same way `coalesce_mfp` acquires MFP
/// canonicalization: by running the real production transforms over the
/// equivalent raised tree. The reused passes union demand across all uses of a
/// shared `Let` binding, which the bottom-up e-class analyses cannot express.
///
/// `commit_wcoj` selects the phase. In the logical phase all joins are
/// `Unimplemented`, so `Demand` (which introduces the `Project(#0,#0)` join
/// column-duplication trick) and full `ProjectionPushdown` are both safe. In the
/// physical phase joins are filled (`DeltaQuery`); `ProjectionPushdown` must skip
/// joins, and `Demand` is omitted because it manipulates join equivalences and
/// would corrupt a committed delta plan. The physical phase is gated off and
/// unvalidated by the SLT differential gate, so it stays conservative.
///
/// Applies the narrowing on a clone and adopts it only on success, so a
/// (practically impossible, given the bounded plan size) recursion-limit error
/// leaves the input untouched rather than half-transformed.
pub(crate) fn demand_pushdown(expr: &mut MirRelationExpr, commit_wcoj: bool) {
    let mut work = expr.clone();
    let arity = work.arity();
    if !commit_wcoj {
        if Demand::default()
            .action(&mut work, (0..arity).collect(), &mut BTreeMap::new())
            .is_err()
        {
            return;
        }
    }
    let pp = if commit_wcoj {
        ProjectionPushdown::skip_joins()
    } else {
        ProjectionPushdown::default()
    };
    if pp
        .action(&mut work, &(0..arity).collect(), &mut BTreeMap::new())
        .is_err()
    {
        return;
    }
    *expr = work;
}

/// Split every mixed-reduction `Reduce` in `expr` into a join of single-type
/// reduces by running the production `ReduceReduction` transform over the whole
/// tree.
///
/// `ReducePlan::create_from` (lowering) panics on a single `Reduce` mixing
/// reduction types (e.g. Accumulable `sum` with Hierarchical `min`), and only
/// `ReduceReduction` splits it. Splitting is semantics-preserving and a no-op on
/// already-split reduces, so it runs unconditionally in both phases. This makes
/// `raise`'s output safe to lower regardless of caller or phase, independent of
/// the phase-gated `logical_fixpoint_02`.
///
/// The local `TransformCtx` uses default features and empty oracles, matching
/// the other reuse post-passes which run their production transforms without a
/// threaded-through context. The result is adopted only if it preserves arity,
/// mirroring the `logical_fixpoint_02` arity guard.
fn split_mixed_reductions(expr: &mut MirRelationExpr) {
    let features = OptimizerFeatures::default();
    let typecheck_ctx = empty_typechecking_context();
    let mut df_meta = DataflowMetainfo::default();
    let mut ctx = TransformCtx::local(&features, &typecheck_ctx, &mut df_meta, None, None);
    let mut work = expr.clone();
    let arity = work.arity();
    if ReduceReduction.transform(&mut work, &mut ctx).is_err() {
        return;
    }
    if work.arity() == arity {
        *expr = work;
    }
}

/// Returns true iff the Map/Filter/Project chain rooted at `expr` has
/// consistent column arities (each Project's output indices are within bounds
/// for the arity that the chain produces up to that point). An invalid chain
/// indicates the eqsat pass produced a plan with out-of-scope column references,
/// which `extract_non_errors_from_expr_mut` cannot handle without panicking.
fn mfp_chain_valid(expr: &MirRelationExpr) -> bool {
    // Walk the chain recursively, returning the output arity or None on OOB.
    fn check(expr: &MirRelationExpr) -> Option<usize> {
        match expr {
            MirRelationExpr::Map { input, scalars }
                if scalars.iter().all(|s| !s.is_literal_err()) =>
            {
                let inner_arity = check(input)?;
                Some(inner_arity + scalars.len())
            }
            MirRelationExpr::Filter { input, predicates }
                if predicates.iter().all(|p| !p.is_literal_err()) =>
            {
                check(input)
            }
            MirRelationExpr::Project { input, outputs } => {
                let inner_arity = check(input)?;
                // All output indices must be within the arity produced by the
                // inner chain (which includes any mapped columns from Map nodes
                // below).
                if outputs.iter().all(|&o| o < inner_arity) {
                    Some(outputs.len())
                } else {
                    None
                }
            }
            x => Some(x.arity()),
        }
    }
    check(expr).is_some()
}

/// Walk past the Map/Filter/Project prefix of `expr` and recurse
/// `coalesce_mfp` into the children of the non-MFP base node.
/// Used when the chain is invalid and we skip extraction but still want to
/// coalesce inner sub-trees.
fn coalesce_mfp_children_of_base(expr: &mut MirRelationExpr) {
    match expr {
        MirRelationExpr::Map { input, scalars } if scalars.iter().all(|s| !s.is_literal_err()) => {
            coalesce_mfp_children_of_base(input);
        }
        MirRelationExpr::Filter { input, predicates }
            if predicates.iter().all(|p| !p.is_literal_err()) =>
        {
            coalesce_mfp_children_of_base(input);
        }
        MirRelationExpr::Project { input, .. } => {
            coalesce_mfp_children_of_base(input);
        }
        base => {
            base.visit_mut_children(coalesce_mfp);
        }
    }
}

/// A placeholder relation type of the given arity for a synthesized empty
/// constant (produced by `empty_false_filter` / `union_cancel`). The pass is
/// offline; the surrounding optimizer recomputes column types when this is
/// ever wired live. Column types are nullable to admit an empty collection.
fn repr_type_of_arity(arity: usize) -> ReprRelationType {
    ReprRelationType::new(
        (0..arity)
            .map(|_| ReprScalarType::Int64.nullable(true))
            .collect(),
    )
}

/// Read a slice of [`EScalar`]s back to their `MirScalarExpr`s.
fn resolve(scalars: &[EScalar]) -> Vec<MirScalarExpr> {
    scalars.iter().map(|s| s.expr.clone()).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_expr::{AccessStrategy, Id, LocalId, MirRelationExpr, MirScalarExpr, func};
    use mz_repr::{ReprRelationType, ReprScalarType};

    use crate::eqsat::lower::{lower, lower_with};

    use super::{coalesce_mfp, raise};

    fn base(arity: usize) -> MirRelationExpr {
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        MirRelationExpr::constant(vec![], typ)
    }

    /// Lower then raise and assert structural identity.
    fn roundtrip(r: MirRelationExpr) {
        let rel = lower(&r);
        // These round-trips never involve a WcoJoin (lowering never emits one),
        // so `commit_wcoj` is irrelevant here.
        let back = raise(&rel, true, &BTreeMap::new(), super::NativeJoinFlags::none());
        assert_eq!(back, r, "round-trip changed the plan");
    }

    #[mz_ore::test]
    fn roundtrip_filter_over_constant() {
        // Filter wraps a bailed Constant leaf; the round-trip must recover the
        // original Constant verbatim from the interner. The predicate must be
        // boolean-typed so that coalesce_mfp can call Filter::action without
        // triggering the boolean-type assertion in canonicalize_predicates. Use
        // a column-equality predicate (#0 = #1) which is boolean and stable
        // under canonicalize_predicates (not reducible to a constant).
        let pred = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), func::Eq);
        let r = base(2).filter(vec![pred]);
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_map_over_constant() {
        let r = base(2).map(vec![MirScalarExpr::column(1)]);
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_binary_union() {
        // Two independent bases unioned: both are bailed leaves; Union is
        // structural and must survive the trip.
        let a = base(2);
        let b = base(2);
        // Construct Union directly to avoid the flattening done by .union().
        let r = MirRelationExpr::Union {
            base: Box::new(a),
            inputs: vec![b],
        };
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_join_of_two_bases() {
        // Join of two bailed inputs with one equivalence class.
        let a = base(2);
        let b = base(2);
        // join_scalars may filter constant-singleton inputs, so use non-empty
        // constants by choosing a base that is not a constant-singleton. The
        // base() helper produces an empty constant, which join_scalars retains
        // because it has zero rows (non-singleton).
        let r = MirRelationExpr::join_scalars(
            vec![a, b],
            vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(2)]],
        );
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_single_key_arrange_by() {
        // A single-key ArrangeBy lowers to Rel::ArrangeBy and must raise back to
        // the identical MIR ArrangeBy (one key list, bare column key unchanged
        // by the lower-time reduce).
        let r = base(2).arrange_by(&[vec![MirScalarExpr::column(0)]]);
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_multi_key_arrange_by() {
        // A two-key ArrangeBy lowers to Rel::ArrangeByMany and must raise back to
        // the identical MIR ArrangeBy (same two key lists, column expressions
        // unchanged by the lower-time reduce).
        use crate::eqsat::ir::Rel;
        let r = base(3).arrange_by(&[
            vec![MirScalarExpr::column(0)],
            vec![MirScalarExpr::column(1)],
        ]);
        // Verify lower produces ArrangeByMany, not an Opaque bail.
        assert!(
            matches!(lower(&r), Rel::ArrangeByMany { .. }),
            "two-key ArrangeBy must lower to Rel::ArrangeByMany"
        );
        roundtrip(r);
    }

    #[mz_ore::test]
    fn indexed_filter_commits_to_its_realization_in_the_physical_phase() {
        use crate::eqsat::ir::{EScalar, Rel};

        // An indexed filter carries the equivalent filter predicates plus a
        // committed physical realization. Lowering never produces it (only the
        // physical pass seeds it), so build it by hand.
        let input = lower(&base(2));
        let pred = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), func::Eq);
        // `committed` stands in for the production IndexedFilter join. A distinct,
        // recognizable plan so we can assert it is emitted verbatim.
        let committed = base(2).negate();
        let node = Rel::IndexedFilter {
            input: Box::new(input),
            predicates: vec![EScalar::plain(pred.clone())],
            committed: Box::new(committed.clone()),
        };

        // Physical phase: emit the committed realization verbatim.
        assert_eq!(
            raise(
                &node,
                true,
                &BTreeMap::new(),
                super::NativeJoinFlags::none()
            ),
            committed,
            "physical raise must emit the committed realization"
        );
        // Logical phase: emit the equivalent plain filter over the raised input.
        assert_eq!(
            raise(
                &node,
                false,
                &BTreeMap::new(),
                super::NativeJoinFlags::none()
            ),
            base(2).filter(vec![pred]),
            "logical raise must emit the equivalent Filter(input)"
        );
    }

    #[mz_ore::test]
    fn roundtrip_let_binding() {
        // Let binding where the body references the bound id via a local Get.
        // This exercises record_local_get / resolve_local_get.
        let local = LocalId::new(0);
        let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let get_local = MirRelationExpr::Get {
            id: Id::Local(local),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let r = MirRelationExpr::Let {
            id: local,
            value: Box::new(base(1)),
            body: Box::new(get_local),
        };
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_unsupported_is_identity() {
        // An unsupported plan (entire tree is one opaque leaf) must round-trip
        // identically: the whole subtree is stored and recovered verbatim.
        let inner = base(2);
        let r = inner.arrange_by(&[vec![MirScalarExpr::column(0)]]);
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_flatmap() {
        // A FlatMap over a base relation must lower to Rel::FlatMap and raise back
        // to the identical MIR FlatMap (func + exprs preserved, exprs unchanged by
        // the lower-time reduce on column references).
        use crate::eqsat::ir::Rel;
        use crate::eqsat::lower::lower;
        use mz_expr::TableFunc;
        use mz_repr::ReprScalarType;
        let r = base(2).flat_map(
            TableFunc::GenerateSeriesInt64,
            vec![
                MirScalarExpr::column(0),
                MirScalarExpr::column(1),
                MirScalarExpr::literal_ok(mz_repr::Datum::Int64(1), ReprScalarType::Int64),
            ],
        );
        // Verify lower produces the real Rel::FlatMap, not Rel::Opaque.
        assert!(
            matches!(lower(&r), Rel::FlatMap { .. }),
            "FlatMap must lower to Rel::FlatMap, not Rel::Opaque"
        );
        roundtrip(r);
    }

    #[mz_ore::test]
    fn raise_cse_let_with_placeholder_local_get() {
        // Simulate a CSE-produced tree: Let { id=1, value=base(2), body=LocalGet { id=1, get=None } }.
        // The `get: None` form is what CSE produces; raise must emit a local Get
        // using the bound value's type rather than panicking.
        use crate::eqsat::ir::Rel;

        let value_rel = Rel::Constant {
            card: 0,
            arity: 2,
            col_types: None,
        };
        let cse_let = Rel::Let {
            id: 1,
            value: Box::new(value_rel),
            body: Box::new(Rel::LocalGet {
                id: 1,
                arity: 2,
                get: None,
                version: None,
            }),
        };
        // Must not panic; raise threads the bound value's type into scope.
        let raised = raise(
            &cse_let,
            false,
            &BTreeMap::new(),
            super::NativeJoinFlags::none(),
        );
        match &raised {
            MirRelationExpr::Let { id, value, body } => {
                assert_eq!(u64::from(id), 1);
                let expected_typ = value.typ();
                match body.as_ref() {
                    MirRelationExpr::Get {
                        id: Id::Local(lid),
                        typ,
                        access_strategy: AccessStrategy::UnknownOrLocal,
                    } => {
                        assert_eq!(u64::from(lid), 1);
                        assert_eq!(typ, &expected_typ);
                    }
                    other => panic!("expected local Get body, got {other:?}"),
                }
            }
            other => panic!("expected Let, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn coalesce_fuses_nested_filter_map_filter() {
        // Build a Rel that represents filter(p1, map(s, filter(p2, base))).
        // This is a non-canonical MFP run: two Filters with a Map between them.
        // After raise + coalesce_mfp, the result must be canonical:
        //   at most one Map, one Filter, one Project per contiguous run,
        //   in Map-then-Filter-then-Project order.
        use crate::eqsat::ir::{EScalar, Rel};

        let base_rel = Rel::Constant {
            card: 0,
            arity: 2,
            col_types: None,
        };
        // filter(p2 = is_null(#0), base) -- boolean predicate, non-trivial
        let p2 = EScalar::plain(MirScalarExpr::column(0).call_is_null());
        let after_inner_filter = Rel::Filter {
            input: Box::new(base_rel),
            predicates: vec![p2],
        };
        // map(s = #1, filter(p2, base))  -- appends column #2 = #1
        let s = EScalar::plain(MirScalarExpr::column(1));
        let after_map = Rel::Map {
            input: Box::new(after_inner_filter),
            scalars: vec![s],
        };
        // filter(p1 = is_null(#1), map(s, filter(p2, base))) -- second boolean pred
        let p1 = EScalar::plain(MirScalarExpr::column(1).call_is_null());
        let rel = Rel::Filter {
            input: Box::new(after_map),
            predicates: vec![p1],
        };

        // Raise the non-canonical tree then coalesce it.
        let mut result = raise(
            &rel,
            false,
            &BTreeMap::new(),
            super::NativeJoinFlags::none(),
        );
        coalesce_mfp(&mut result);

        // Walk the result tree and count contiguous M/F/P layers.
        // Canonical order is: Map? then Filter? then Project? over a non-MFP base.
        // We verify that the filter and map are not interleaved (i.e., no Filter
        // directly below a Map below another Filter).
        fn is_filter(e: &MirRelationExpr) -> bool {
            matches!(e, MirRelationExpr::Filter { .. })
        }
        fn is_map(e: &MirRelationExpr) -> bool {
            matches!(e, MirRelationExpr::Map { .. })
        }

        // The outermost node must NOT be a Filter sitting on top of a Map
        // sitting on top of a Filter — that is the non-canonical pattern.
        // After coalescing the two filters fuse, so at most one Filter survives.
        let mut filter_count = 0usize;
        let mut map_count = 0usize;
        let mut e = &result;
        loop {
            if is_filter(e) {
                filter_count += 1;
                match e {
                    MirRelationExpr::Filter { input, .. } => e = input,
                    _ => unreachable!(),
                }
            } else if is_map(e) {
                map_count += 1;
                match e {
                    MirRelationExpr::Map { input, .. } => e = input,
                    _ => unreachable!(),
                }
            } else {
                break;
            }
        }
        // The two filters must have been fused into one: at most one Filter and
        // one Map in the contiguous top-level MFP run.
        assert!(
            filter_count <= 1,
            "expected at most 1 Filter in MFP run after coalescing, got {filter_count}"
        );
        assert!(
            map_count <= 1,
            "expected at most 1 Map in MFP run after coalescing, got {map_count}"
        );
    }

    /// A non-`Int64` branch that collapses to `Empty` via `union_cancel` must
    /// raise an empty relation carrying the branch's REAL column type, not the
    /// `Int64?` arity-only placeholder, and the result must survive a strict
    /// Typecheck. Regression for the soundness hole where a synthesized `Empty`
    /// lost column types and a non-`Int64` collapsed branch emitted a
    /// wrong-typed plan that typechecked anyway.
    #[mz_ore::test]
    fn empty_from_union_cancel_keeps_real_column_type() {
        use mz_expr::{AccessStrategy, Id};
        use mz_repr::GlobalId;

        use crate::eqsat::optimize;
        use crate::typecheck::{Typecheck, empty_typechecking_context};
        use crate::{Transform, TransformCtx};
        use mz_repr::optimize::OptimizerFeatures;

        // A global relation with a single non-nullable `text` column. A global
        // Get bails to an opaque leaf carrying this typed node, so the
        // structural type derivation can read the real column type off it.
        let col = ReprScalarType::String.nullable(false);
        let typ = ReprRelationType::new(vec![col.clone()]);
        let get = MirRelationExpr::Get {
            id: Id::Global(GlobalId::Transient(1)),
            typ,
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        // Union(g, Negate(g)) cancels to Empty(g) via `union_cancel`. The Empty
        // must carry g's `text` column type.
        let plan = MirRelationExpr::Union {
            base: Box::new(get.clone()),
            inputs: vec![get.negate()],
        };

        let optimized = optimize(plan);

        // Find the synthesized empty constant and assert its column type is the
        // real `text`, not the `Int64?` placeholder.
        let mut found_empty = false;
        optimized.visit_pre(|e| {
            if let MirRelationExpr::Constant {
                rows: Ok(rows),
                typ,
            } = e
            {
                if rows.is_empty() {
                    found_empty = true;
                    assert_eq!(
                        typ.column_types,
                        vec![col.clone()],
                        "synthesized Empty must carry the real text column type, got {:?}",
                        typ.column_types
                    );
                }
            }
        });
        assert!(
            found_empty,
            "expected union_cancel to collapse the plan to an empty constant; got {optimized:?}"
        );

        // The optimized plan must survive a strict Typecheck.
        let ctx = empty_typechecking_context();
        let features = OptimizerFeatures::default();
        let mut df_meta = crate::dataflow::DataflowMetainfo::default();
        let mut transform_ctx = TransformCtx::local(&features, &ctx, &mut df_meta, None, None);
        let mut checked = optimized;
        Typecheck::new(std::sync::Arc::clone(&ctx))
            .transform(&mut checked, &mut transform_ctx)
            .expect("optimized plan must pass strict Typecheck");
    }

    #[mz_ore::test]
    fn roundtrip_letrec() {
        // A LetRec must lower to Rel::LetRec (not Rel::Opaque) and raise back to
        // an identical MIR LetRec, with the recursive Get(Local) back-edge, the
        // binding order, and the per-binding limits preserved.
        use crate::eqsat::ir::Rel;

        let lid = LocalId::new(0);
        let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let get_rec = MirRelationExpr::Get {
            id: Id::Local(lid),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        // Use the recursive Get as both the value and the body. The recursive
        // back-edge lowers to a sealed LocalGet leaf so no e-graph cycle forms.
        let value = get_rec.clone();
        let r = MirRelationExpr::LetRec {
            ids: vec![lid],
            values: vec![value],
            limits: vec![None],
            body: Box::new(get_rec),
        };
        assert!(
            matches!(lower(&r), Rel::LetRec { .. }),
            "LetRec must lower to Rel::LetRec, not Opaque"
        );
        roundtrip(r);
    }

    #[mz_ore::test]
    fn roundtrip_letrec_versioned() {
        // The same LetRec as roundtrip_letrec, but lowered with the lift on.
        // Lowering tags versions; raise must erase them and reproduce identical MIR.
        let lid = LocalId::new(0);
        let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
        let get_rec = MirRelationExpr::Get {
            id: Id::Local(lid),
            typ: typ.clone(),
            access_strategy: AccessStrategy::UnknownOrLocal,
        };
        let value = get_rec.clone();
        let r = MirRelationExpr::LetRec {
            ids: vec![lid],
            values: vec![value],
            limits: vec![None],
            body: Box::new(get_rec),
        };
        let lowered = lower_with(&r, true);
        let raised = raise(
            &lowered,
            false,
            &BTreeMap::new(),
            super::NativeJoinFlags::none(),
        );
        assert_eq!(raised, r, "versioned round trip must be identity");
    }

    /// `raise` must never return a `Reduce` whose aggregates mix reduction
    /// types: `ReducePlan::create_from` (lowering) panics on such a Reduce, and
    /// only the internal `ReduceReduction` split breaks it apart. The split must
    /// run in BOTH phases, since the physical phase (`commit_wcoj = true`) skips
    /// `logical_fixpoint_02`, which would otherwise be the only splitter. Build a
    /// single `Reduce` mixing an Accumulable `sum` with a Hierarchical `min`,
    /// lower it, then assert `raise` splits it for both `commit_wcoj` values.
    #[mz_ore::test]
    fn raise_splits_mixed_reduction_reduce_in_both_phases() {
        use mz_compute_types::plan::reduce::{ReductionType, reduction_type};
        use mz_expr::{AggregateExpr, AggregateFunc};

        // The two functions are genuinely different reduction types, otherwise
        // there would be nothing to split.
        assert_eq!(
            reduction_type(&AggregateFunc::SumInt64),
            ReductionType::Accumulable
        );
        assert_eq!(
            reduction_type(&AggregateFunc::MinInt64),
            ReductionType::Hierarchical
        );

        // Returns true iff `expr` contains a Reduce mixing reduction types.
        fn has_mixed_reduction_reduce(expr: &MirRelationExpr) -> bool {
            let mut found = false;
            expr.visit_pre(|e| {
                if let MirRelationExpr::Reduce { aggregates, .. } = e {
                    let mut types = aggregates.iter().map(|a| reduction_type(&a.func));
                    if let Some(first) = types.next() {
                        if types.any(|t| t != first) {
                            found = true;
                        }
                    }
                }
            });
            found
        }

        let sum = AggregateExpr {
            func: AggregateFunc::SumInt64,
            expr: MirScalarExpr::column(1),
            distinct: false,
        };
        let min = AggregateExpr {
            func: AggregateFunc::MinInt64,
            expr: MirScalarExpr::column(1),
            distinct: false,
        };
        let mixed = MirRelationExpr::Reduce {
            input: Box::new(base(2)),
            group_key: vec![MirScalarExpr::column(0)],
            aggregates: vec![sum, min],
            monotonic: false,
            expected_group_size: None,
        };
        assert!(
            has_mixed_reduction_reduce(&mixed),
            "test input must contain a mixed-reduction Reduce"
        );

        let rel = lower(&mixed);

        // Both phases must split the mixed Reduce. The physical phase
        // (`commit_wcoj = true`) is the gap that `logical_fixpoint_02` leaves,
        // so this is the load-bearing case for the fix.
        for commit_wcoj in [false, true] {
            let raised = raise(
                &rel,
                commit_wcoj,
                &BTreeMap::new(),
                super::NativeJoinFlags::none(),
            );
            assert!(
                !has_mixed_reduction_reduce(&raised),
                "raise (commit_wcoj={commit_wcoj}) must split the mixed-reduction Reduce; got {raised:?}"
            );
            // Arity is preserved across the split: group key plus two aggregates.
            assert_eq!(
                raised.arity(),
                3,
                "raise (commit_wcoj={commit_wcoj}) must preserve arity; got {raised:?}"
            );
        }
    }

    /// Build a global `Get` for a transient id with the given arity, all columns
    /// non-nullable `Int64`.
    fn global_get(id: u64, arity: usize) -> MirRelationExpr {
        use mz_repr::GlobalId;
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        MirRelationExpr::Get {
            id: Id::Global(GlobalId::Transient(id)),
            typ,
            access_strategy: AccessStrategy::UnknownOrLocal,
        }
    }

    /// A single-index availability map keyed by column references.
    fn avail(id: u64, key_cols: &[usize]) -> BTreeMap<mz_repr::GlobalId, Vec<Vec<MirScalarExpr>>> {
        use mz_repr::GlobalId;
        let key: Vec<MirScalarExpr> = key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
        let mut m = BTreeMap::new();
        m.insert(GlobalId::Transient(id), vec![key]);
        m
    }

    #[mz_ore::test]
    fn per_input_available_sees_through_projection() {
        // `Project([5,1,0])(Filter(Get wa))` over an index on base col 0. The
        // projection is a pure column selection, so the base index is reusable:
        // its key col 0 survives at output position 2. `per_input_available` must
        // credit the index in the PROJECTED column space, i.e. `[#2]`.
        use super::per_input_available;
        let pred = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), func::Eq);
        let input = global_get(1, 6).filter(vec![pred]).project(vec![5, 1, 0]);
        let available = avail(1, &[0]);
        let per_input = per_input_available(&[input], &available);
        assert_eq!(
            per_input,
            vec![vec![vec![MirScalarExpr::column(2)]]],
            "index on base col 0 must be credited at its projected position #2"
        );
    }

    #[mz_ore::test]
    fn per_input_available_projection_dropping_index_col_credits_nothing() {
        // The projection drops base col 0 (the index key), so the base index no
        // longer survives into the projected space and must not be credited.
        use super::per_input_available;
        let pred = MirScalarExpr::column(0).call_binary(MirScalarExpr::column(1), func::Eq);
        let input = global_get(1, 6).filter(vec![pred]).project(vec![1, 5]);
        let available = avail(1, &[0]);
        let per_input = per_input_available(&[input], &available);
        assert_eq!(
            per_input,
            vec![Vec::<Vec<MirScalarExpr>>::new()],
            "dropping the index col must credit no arrangement; got {per_input:?}"
        );
    }
}
