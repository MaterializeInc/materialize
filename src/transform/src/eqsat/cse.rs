// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Extraction-time common-subexpression elimination.
//!
//! The e-graph already *subsumes* CSE: equal subterms are hash-consed into a
//! single e-class, so sharing is implicit while the optimizer runs. Extracting
//! a plan back to a tree (see [`crate::eqsat::egraph::EGraph::extract`]) loses that
//! sharing; this pass re-introduces it, binding every subexpression that occurs
//! more than once with a [`Rel::Let`] and replacing its occurrences with
//! [`Rel::LocalGet`]. This is the relational analogue of `transform/src/cse`
//! (ANF + `NormalizeLets`), reduced to its essence: binding shared structure.
//!
//! `LetRec` (mutually-recursive bindings, the substrate for Materialize's
//! `WITH MUTUALLY RECURSIVE`) is *not* modeled here — equality saturation over
//! a finite e-graph does not represent fixpoints, so recursive bindings would
//! need first-class support in the IR, cost model, and Lean semantics. See
//! `COVERAGE.md`.

use std::collections::BTreeMap;

use mz_expr::{Id, MirRelationExpr, MirScalarExpr};
use mz_ore::cast::CastFrom;
use mz_repr::GlobalId;

use crate::eqsat::ir::{RecVersion, Rel};

/// Bind every subexpression that occurs more than once in `rel` with a `Let`,
/// turning a tree back into a DAG-with-sharing.
///
/// `available` maps each global id to its maintained index keys. A subexpression
/// that reads an index-backed global `Get` (under only column-preserving MFP
/// wrappers) is left inlined rather than hoisted into a shared `Let`: hoisting it
/// freezes a narrowed projection in front of the `Get`, which defeats the
/// downstream `JoinImplementation` reuse of the maintained full-width arrangement
/// (the inlined form lets each use recover that reuse). Pass an empty map to
/// disable this guard (the logical phase, which has no index availability).
///
/// `enable_wmr_lift` turns on cross-`LetRec`-binding sharing. A subterm shared
/// across two or more recursive binding values is hoisted: if it contains at
/// least one versioned recursive leaf it becomes a new `LetRec` binding at a
/// version-valid order slot; if it is loop-invariant (no versioned leaf) it
/// becomes an ordinary `Let` wrapping the whole `LetRec`, computed once before
/// the loop starts (see [`hoist_shared_letrec_subterms`]). With the flag off
/// neither kind hoists and the output is identical to the pre-WMR pass.
pub fn eliminate_common_subexpressions(
    rel: &Rel,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
    enable_wmr_lift: bool,
) -> Rel {
    // First, share subterms across the bindings of each `LetRec` (flag-gated).
    // This rewrites each `LetRec` in place: versioned shared subterms become new
    // LetRec bindings (Task 5); loop-invariant shared subterms become outer `Let`
    // bindings wrapping the LetRec (Task 6). The ordinary outer-`Let` CSE below
    // then runs over the result, picking up any remaining shared closed subterms.
    let rel = if enable_wmr_lift {
        hoist_letrec_subterms_everywhere(rel)
    } else {
        rel.clone()
    };
    let rel = &rel;
    // 1. Count occurrences of every distinct subtree.
    let mut counts: BTreeMap<Rel, usize> = BTreeMap::new();
    count(rel, &mut counts);

    // 2. Pick the shared, non-trivial subtrees. Order by size ascending so a
    //    shared subtree is bound *after* (inside) any larger one is impossible;
    //    rather, smaller ones are bound first (outermost) and are therefore in
    //    scope for the larger values that reference them.
    let mut shared: Vec<Rel> = counts
        .into_iter()
        .filter(|(r, n)| *n >= 2 && worth_binding(r, available))
        .map(|(r, _)| r)
        .collect();
    shared.sort_by(|a, b| a.node_count().cmp(&b.node_count()).then_with(|| a.cmp(b)));
    if shared.is_empty() {
        return rel.clone();
    }

    // 3. Assign a local id to each shared subtree. Ids start above the maximum
    //    id already present in the tree so CSE-introduced ids never clash with
    //    lowered Let/LocalGet ids (real LocalId numbers).
    let max_existing = max_local_id(rel);
    let id_base = max_existing.saturating_add(1);
    let ids: BTreeMap<Rel, usize> = shared
        .iter()
        .enumerate()
        .map(|(i, r)| (r.clone(), id_base + i))
        .collect();

    // 4. The body, with every shared subtree replaced by its `LocalGet`.
    let mut result = subst(rel, &ids);

    // 5. Wrap in `Let` bindings, largest (innermost) first so the fold leaves
    //    the smallest binding outermost — keeping every value's references in
    //    scope.
    for r in shared.iter().rev() {
        let id = ids[r];
        let value = Box::new(subst_children(r, &ids));
        result = Rel::Let {
            id,
            value,
            body: Box::new(result),
        };
    }
    result
}

/// A subtree is worth binding if it is compound (sharing a `Get`/`Constant`/
/// `Opaque`/`LocalGet` saves nothing) AND closed (contains no references to
/// lower'd local ids via `LocalGet { get: Some }` or nested `Let` bindings) AND
/// it is not just an index-backed global `Get` read (see `reads_index_backed_get`).
/// Open subtrees reference locals from an outer scope and cannot be hoisted
/// outside that scope without breaking the scoping invariant.
fn worth_binding(rel: &Rel, available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>) -> bool {
    !matches!(
        rel,
        Rel::Get { .. } | Rel::Constant { .. } | Rel::Opaque(_) | Rel::LocalGet { .. }
    ) && is_closed(rel)
        && !reads_index_backed_get(rel, available)
}

/// True iff `rel`, after stripping leading column-preserving `Project`/`Filter`/
/// `Map` wrappers, is an `Opaque` global `Get` that a maintained index covers.
/// Such a subtree is left inlined: hoisting it into a `Let` freezes the narrowing
/// projection that a prior `ProjectionPushdown` placed in front of the `Get`,
/// which prevents `JoinImplementation` from reusing the maintained full-width
/// arrangement (it can still recover that reuse from the inlined per-use form).
fn reads_index_backed_get(
    rel: &Rel,
    available: &BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>>,
) -> bool {
    let mut node = rel;
    loop {
        match node {
            Rel::Project { input, .. } | Rel::Filter { input, .. } | Rel::Map { input, .. } => {
                node = input
            }
            Rel::Opaque(m) => {
                return matches!(
                    &**m,
                    MirRelationExpr::Get { id: Id::Global(gid), .. }
                        if available.get(gid).is_some_and(|keys| !keys.is_empty())
                );
            }
            _ => return false,
        }
    }
}

/// Returns true iff `rel` contains no reference to an enclosing scope and no
/// nested `Rel::Let`. Such subtrees depend on an outer scope and cannot be
/// hoisted above it.
///
/// A `LocalGet { get: Some }` references an outer `Let`. A versioned reference
/// (`version: Some(_)`) is bound by an enclosing `LetRec` (the WMR-lift pass
/// inserts these), so it likewise cannot leave that scope. Only an unversioned
/// `get: None` placeholder (a CSE-introduced ordinary local) is closed.
fn is_closed(rel: &Rel) -> bool {
    match rel {
        Rel::LocalGet { get: Some(_), .. } => false,
        Rel::LocalGet {
            version: Some(_), ..
        } => false,
        Rel::LocalGet { get: None, .. } => true,
        Rel::Let { .. } => false,
        _ => rel.children().iter().all(|c| is_closed(c)),
    }
}

/// Walk `rel` and return the maximum `LocalId` reachable anywhere in the plan,
/// or 0 if none are present. Used to pick a fresh id base for CSE bindings.
///
/// This must account for *every* `LocalId` in scope. Two sources require
/// explicit handling. First, a first-class [`Rel::LetRec`] node carries binding
/// ids that may have no corresponding `LocalGet` reference (e.g. an unreferenced
/// base case), so the `LocalGet` arm alone would miss them. Second, those buried
/// in the verbatim `MirRelationExpr` of a [`Rel::Opaque`] leaf have no `Rel`
/// children, so a plain `Rel`-only walk never sees their ids. If a
/// CSE-introduced id were to collide with either, it would shadow an existing
/// binding and a later `Demand`/`NormalizeLets` pass that asserts no shadowing
/// (see `transform/src/demand.rs`) would panic on recursive CTEs.
fn max_local_id(rel: &Rel) -> usize {
    fn walk(rel: &Rel, max: &mut usize) {
        match rel {
            Rel::Let { id, value, body } => {
                if *id > *max {
                    *max = *id;
                }
                walk(value, max);
                walk(body, max);
            }
            Rel::LetRec { bindings, body, .. } => {
                // Account for the binding ids themselves. A binding whose id is
                // the maximum in the plan may have no `LocalGet` reference (e.g.
                // an unreferenced base case), so the `LocalGet` arm alone would
                // miss it and CSE could allocate a colliding id.
                for (id, value) in bindings {
                    if *id > *max {
                        *max = *id;
                    }
                    walk(value, max);
                }
                walk(body, max);
            }
            Rel::LocalGet { id, .. } => {
                if *id > *max {
                    *max = *id;
                }
            }
            Rel::Opaque(mir) => {
                let m = max_mir_local_id(mir);
                if m > *max {
                    *max = m;
                }
            }
            _ => {
                for c in rel.children() {
                    walk(c, max);
                }
            }
        }
    }
    let mut max = 0;
    walk(rel, &mut max);
    max
}

/// The maximum `LocalId` (as `usize`) appearing anywhere in `mir`, or 0 if none.
///
/// Covers both binding sites (`Let`/`LetRec` ids) and references
/// (`Get { Id::Local }`); the two coincide in well-formed MIR, but we take the
/// max over both so an unreferenced binding still counts.
pub(crate) fn max_mir_local_id(mir: &MirRelationExpr) -> usize {
    let mut max = 0;
    mir.visit_pre(|e| {
        let mut note = |local: &mz_expr::LocalId| {
            let id = usize::cast_from(u64::from(local));
            if id > max {
                max = id;
            }
        };
        match e {
            MirRelationExpr::Get {
                id: Id::Local(local),
                ..
            } => note(local),
            MirRelationExpr::Let { id, .. } => note(id),
            MirRelationExpr::LetRec { ids, .. } => {
                for id in ids {
                    note(id);
                }
            }
            _ => {}
        }
    });
    max
}

/// Recursively apply [`hoist_shared_letrec_subterms`] to every `LetRec` in
/// `rel`, bottom up, so a nested `LetRec` is processed before the one that
/// encloses it.
fn hoist_letrec_subterms_everywhere(rel: &Rel) -> Rel {
    // Rebuild children first (bottom up), then act on this node if it is a
    // LetRec.
    let rebuilt = {
        let new: Vec<Rel> = rel
            .children()
            .into_iter()
            .map(hoist_letrec_subterms_everywhere)
            .collect();
        rel.with_children(new)
    };
    match rebuilt {
        Rel::LetRec {
            bindings,
            limits,
            body,
        } => hoist_shared_letrec_subterms(bindings, limits, *body),
        other => other,
    }
}

/// True iff `rel` contains no versioned recursive leaf, so its value does not
/// change across loop iterations and it may be computed once outside the LetRec.
///
/// A subterm is loop-invariant when no `Rel::LocalGet { version: Some(_), .. }`
/// appears anywhere in its tree. Such a subterm reads only global (opaque) leaves
/// or CSE placeholders and is unaffected by the iteration variable.
fn is_loop_invariant(rel: &Rel) -> bool {
    match rel {
        Rel::LocalGet {
            version: Some(_), ..
        } => false,
        _ => rel.children().iter().all(|c| is_loop_invariant(c)),
    }
}

/// Hoist subterms shared across the values of a single `LetRec`'s bindings.
///
/// Two kinds of candidates are considered:
///
/// 1. **Versioned** (`reads_a_versioned_leaf`): contain at least one recursive
///    leaf. Hoisted into new `LetRec` bindings at a version-valid order slot.
/// 2. **Loop-invariant** (`is_loop_invariant`): contain NO versioned leaf. Their
///    value never changes across iterations, so they are hoisted once as ordinary
///    `Rel::Let` bindings wrapping the entire `LetRec`.
///
/// Both kinds require the candidate to be compound, `is_version_closed`, and to
/// appear two or more times among the binding values (repeats within a single
/// binding value count). Invariant candidates are
/// processed first (ascending size order mixes them with versioned ones, but the
/// size-first order ensures a smaller invariant child is bound before a larger
/// versioned parent that contains it).
///
/// Placement for versioned hoists (conservative-correct): all original bindings
/// keep their relative order. A hoisted binding `s` is inserted just after the
/// maximum position among the `Cur(j)` sources it reads (`lower_bound`), which
/// is before every consumer. `Prev(m)` reads impose `m` at or after `s`,
/// satisfied because consumers sit at or after every `Prev` target. The interval
/// is non-empty because the original order already satisfies every constraint; an
/// empty interval signals a real bug, so we panic rather than silently skip.
fn hoist_shared_letrec_subterms(
    mut bindings: Vec<(usize, Rel)>,
    mut limits: Vec<Option<mz_expr::LetRecLimit>>,
    body: Rel,
) -> Rel {
    // The ids bound by this LetRec, used to classify versioned leaves.
    let own_ids: std::collections::BTreeSet<usize> = bindings.iter().map(|(id, _)| *id).collect();

    // Count each compound, version-closed subterm across all binding values.
    // A subterm rooted at a binding value itself is excluded (sharing a whole
    // binding gains nothing and would create a trivial alias).
    //
    // The loop counts all version-closed candidates, including loop-invariant
    // ones (those with no versioned leaf). Invariant candidates are separated
    // from versioned ones in the filter below.
    let mut counts: BTreeMap<Rel, usize> = BTreeMap::new();
    for (_, value) in &bindings {
        for c in value.children() {
            count_letrec_candidates(c, &own_ids, &mut counts);
        }
    }

    // Shared candidates in ascending size order. The combined sort ensures a
    // smaller invariant child is bound before a larger versioned parent that
    // contains it: after hoisting the invariant child, the versioned parent now
    // holds a `LocalGet` placeholder, so its structure changes and it may or
    // may not still appear enough times in the consumers.
    let mut shared: Vec<Rel> = counts
        .into_iter()
        .filter(|(r, n)| {
            *n >= 2
                && is_version_closed(r, &own_ids)
                && (reads_a_versioned_leaf(r) || is_loop_invariant(r))
        })
        .map(|(r, _)| r)
        .collect();
    shared.sort_by(|a, b| a.node_count().cmp(&b.node_count()).then_with(|| a.cmp(b)));
    if shared.is_empty() {
        return Rel::LetRec {
            bindings,
            limits,
            body: Box::new(body),
        };
    }

    // Fresh ids, above every id already in the bindings, body, or buried in
    // opaque leaves, so a hoisted id never shadows an existing binding.
    let mut next_id = {
        let mut max = body
            .children()
            .iter()
            .map(|c| max_local_id(c))
            .max()
            .unwrap_or(0)
            .max(max_local_id(&body));
        for (id, value) in &bindings {
            max = max.max(*id).max(max_local_id(value));
        }
        max.saturating_add(1)
    };

    // Loop-invariant subterms to emit as outer `Let` bindings. Collected in
    // processing order (ascending size); they will wrap the LetRec after the loop.
    let mut invariant_lets: Vec<(usize, Rel)> = Vec::new();
    // The LetRec body, with all invariant substitutions applied progressively.
    let mut cur_body = body;

    for subterm in shared {
        let new_id = next_id;
        next_id += 1;

        if is_loop_invariant(&subterm) {
            // Loop-invariant: hoist above the LetRec as an ordinary Let. Replace
            // the subterm in every binding value with an unversioned CSE placeholder
            // (version: None, get: None), which raise threads through scope.
            let reference = Rel::LocalGet {
                id: new_id,
                arity: subterm.arity(),
                get: None,
                version: None,
            };
            for (_, value) in bindings.iter_mut() {
                *value = replace_subterm(value, &subterm, &reference);
            }
            // Replace in the body too. The body normally reads Final-versioned
            // locals, but may contain invariant subterms if the original plan
            // happened to share structure between binding values and the body.
            cur_body = replace_subterm(&cur_body, &subterm, &reference);
            // Record the invariant binding. The subterm's value is what is bound
            // in the outer Let; the reference (LocalGet placeholder) takes its place
            // inside the LetRec.
            invariant_lets.push((new_id, subterm));
        } else {
            // Versioned: insert as a new LetRec binding at the correct slot.
            let cur_sources = cur_source_positions(&subterm, &own_ids, &bindings);
            let prev_sources = prev_source_ids(&subterm, &own_ids);
            let lower_bound = cur_sources.iter().copied().max();

            let consumer_positions: Vec<usize> = bindings
                .iter()
                .enumerate()
                .filter(|(_, (_, v))| contains_subterm(v, &subterm))
                .map(|(i, _)| i)
                .collect();
            let upper_bound = consumer_positions.iter().copied().min();

            let slot = match lower_bound {
                Some(p) => p + 1,
                None => 0,
            };

            if let Some(first_consumer) = upper_bound {
                assert!(
                    slot <= first_consumer,
                    "hoisted binding has no valid order slot: Cur sources at {:?} \
                     force slot {slot}, but first consumer is at {first_consumer}; \
                     Prev targets {:?}",
                    cur_sources,
                    prev_sources,
                );
            }

            let reference = Rel::LocalGet {
                id: new_id,
                arity: subterm.arity(),
                get: None,
                version: Some(RecVersion::Cur),
            };
            for (_, value) in bindings.iter_mut() {
                *value = replace_subterm(value, &subterm, &reference);
            }

            bindings.insert(slot, (new_id, subterm));
            limits.insert(slot, None);
        }
    }

    let letrec = Rel::LetRec {
        bindings,
        limits,
        body: Box::new(cur_body),
    };

    // Wrap the LetRec with invariant `Let` bindings. Processing was in ascending
    // size order, so `invariant_lets` is smallest-first. Reversing puts the
    // largest invariant binding innermost (closest to the LetRec), consistent
    // with the outer CSE's binding order.
    //
    // NOTE: because all invariant bindings are closed (no outer-Let refs, no
    // versioned leaves), they do not reference each other and the nesting order
    // does not affect scope correctness.
    invariant_lets
        .into_iter()
        .rev()
        .fold(letrec, |inner, (id, value)| Rel::Let {
            id,
            value: Box::new(value),
            body: Box::new(inner),
        })
}

/// Count every compound, version-closed subtree of `rel` (including `rel`),
/// keyed by structure. Leaves and ineligible subterms are skipped, but the walk
/// still descends into their children.
///
/// Counts both versioned subterms (`reads_a_versioned_leaf`) and loop-invariant
/// ones (`is_loop_invariant`), since both are candidates for hoisting. Versioned
/// subterms become new LetRec bindings (Task 5); invariant ones become outer
/// `Let` bindings (Task 6).
fn count_letrec_candidates(
    rel: &Rel,
    own_ids: &std::collections::BTreeSet<usize>,
    counts: &mut BTreeMap<Rel, usize>,
) {
    if is_compound(rel)
        && is_version_closed(rel, own_ids)
        && (reads_a_versioned_leaf(rel) || is_loop_invariant(rel))
    {
        *counts.entry(rel.clone()).or_insert(0) += 1;
    }
    for c in rel.children() {
        count_letrec_candidates(c, own_ids, counts);
    }
}

/// A node worth sharing: not a bare leaf.
fn is_compound(rel: &Rel) -> bool {
    !matches!(
        rel,
        Rel::Get { .. } | Rel::Constant { .. } | Rel::Opaque(_) | Rel::LocalGet { .. }
    )
}

/// True iff `rel` may be hoisted out of a binding value into its own binding of
/// the same `LetRec`: no nested `Let`/`LetRec`, and every `LocalGet` is either a
/// versioned recursive leaf of this `LetRec` (`version: Some(_)`) or carries no
/// outer-scope reference (`get: None`). An ordinary outer-`Let` reference
/// (`get: Some, version: None`) binds in a scope the hoisted binding cannot see,
/// so it disqualifies the subterm.
fn is_version_closed(rel: &Rel, own_ids: &std::collections::BTreeSet<usize>) -> bool {
    match rel {
        Rel::Let { .. } | Rel::LetRec { .. } => false,
        Rel::LocalGet {
            version: Some(_),
            id,
            ..
        } => own_ids.contains(id),
        Rel::LocalGet {
            get: Some(_),
            version: None,
            ..
        } => false,
        Rel::LocalGet { .. } => true,
        _ => rel.children().iter().all(|c| is_version_closed(c, own_ids)),
    }
}

/// True iff `rel` reads at least one versioned recursive leaf. Without one, the
/// subterm is an ordinary closed subtree the outer-`Let` CSE already handles, so
/// the LetRec pass leaves it alone.
fn reads_a_versioned_leaf(rel: &Rel) -> bool {
    match rel {
        Rel::LocalGet {
            version: Some(_), ..
        } => true,
        _ => rel.children().iter().any(|c| reads_a_versioned_leaf(c)),
    }
}

/// The binding positions of the `Cur(j)` leaves `rel` reads (j in this LetRec).
fn cur_source_positions(
    rel: &Rel,
    own_ids: &std::collections::BTreeSet<usize>,
    bindings: &[(usize, Rel)],
) -> Vec<usize> {
    let mut ids = std::collections::BTreeSet::new();
    collect_versioned_leaves(rel, RecVersion::Cur, own_ids, &mut ids);
    bindings
        .iter()
        .enumerate()
        .filter(|(_, (id, _))| ids.contains(id))
        .map(|(i, _)| i)
        .collect()
}

/// The binding ids of the `Prev(m)` leaves `rel` reads (m in this LetRec).
fn prev_source_ids(rel: &Rel, own_ids: &std::collections::BTreeSet<usize>) -> Vec<usize> {
    let mut ids = std::collections::BTreeSet::new();
    collect_versioned_leaves(rel, RecVersion::Prev, own_ids, &mut ids);
    ids.into_iter().collect()
}

/// Collect the ids of `LocalGet` leaves with version `want` whose id is in
/// `own_ids`.
fn collect_versioned_leaves(
    rel: &Rel,
    want: RecVersion,
    own_ids: &std::collections::BTreeSet<usize>,
    out: &mut std::collections::BTreeSet<usize>,
) {
    if let Rel::LocalGet {
        id,
        version: Some(v),
        ..
    } = rel
    {
        if *v == want && own_ids.contains(id) {
            out.insert(*id);
        }
    }
    for c in rel.children() {
        collect_versioned_leaves(c, want, own_ids, out);
    }
}

/// True iff `needle` occurs anywhere in `rel` (structural equality).
fn contains_subterm(rel: &Rel, needle: &Rel) -> bool {
    rel == needle || rel.children().iter().any(|c| contains_subterm(c, needle))
}

/// Replace every occurrence of `needle` in `rel` with `with`.
fn replace_subterm(rel: &Rel, needle: &Rel, with: &Rel) -> Rel {
    if rel == needle {
        return with.clone();
    }
    let new: Vec<Rel> = rel
        .children()
        .into_iter()
        .map(|c| replace_subterm(c, needle, with))
        .collect();
    rel.with_children(new)
}

fn count(rel: &Rel, counts: &mut BTreeMap<Rel, usize>) {
    *counts.entry(rel.clone()).or_insert(0) += 1;
    for c in rel.children() {
        count(c, counts);
    }
}

/// Replace `rel` itself with a `LocalGet` if it is shared, else recurse.
fn subst(rel: &Rel, ids: &BTreeMap<Rel, usize>) -> Rel {
    if let Some(&id) = ids.get(rel) {
        return Rel::LocalGet {
            id,
            arity: rel.arity(),
            get: None,
            version: None,
        };
    }
    subst_children(rel, ids)
}

/// Substitute within `rel`'s children, keeping `rel`'s own operator.
fn subst_children(rel: &Rel, ids: &BTreeMap<Rel, usize>) -> Rel {
    let new: Vec<Rel> = rel.children().into_iter().map(|c| subst(c, ids)).collect();
    rel.with_children(new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::eqsat::ir::EScalar;
    use mz_expr::{AccessStrategy, MirScalarExpr};
    use mz_repr::{ReprRelationType, ReprScalarType};

    fn get(name: &str, arity: usize) -> Rel {
        Rel::Get {
            name: name.into(),
            arity,
        }
    }

    /// A `Rel::Opaque` wrapping a global `Get` for a transient id, as `lower`
    /// emits for `MirRelationExpr::Get { Id::Global }`.
    fn global_opaque(id: u64, arity: usize) -> Rel {
        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        Rel::Opaque(Box::new(MirRelationExpr::Get {
            id: Id::Global(GlobalId::Transient(id)),
            typ,
            access_strategy: AccessStrategy::UnknownOrLocal,
        }))
    }

    /// An availability map with a single index key for a global relation.
    fn avail_one(id: u64, key_cols: &[usize]) -> BTreeMap<GlobalId, Vec<Vec<MirScalarExpr>>> {
        let key: Vec<MirScalarExpr> = key_cols.iter().map(|&c| MirScalarExpr::column(c)).collect();
        let mut m = BTreeMap::new();
        m.insert(GlobalId::Transient(id), vec![key]);
        m
    }

    /// Whether any `Let` in `rel` binds a value that, after stripping MFP
    /// wrappers, reads the transient global `Get` `gid`.
    fn binds_read_of(rel: &Rel, gid: u64) -> bool {
        fn reaches(rel: &Rel, gid: u64) -> bool {
            match rel {
                Rel::Project { input, .. } | Rel::Filter { input, .. } | Rel::Map { input, .. } => {
                    reaches(input, gid)
                }
                Rel::Opaque(m) => matches!(
                    &**m,
                    MirRelationExpr::Get { id: Id::Global(GlobalId::Transient(g)), .. } if *g == gid
                ),
                _ => false,
            }
        }
        if let Rel::Let { value, .. } = rel {
            if reaches(value, gid) {
                return true;
            }
        }
        rel.children().iter().any(|c| binds_read_of(c, gid))
    }

    #[mz_ore::test]
    fn shares_a_repeated_subexpression() {
        // A self-join of `Filter(R)`: the filtered relation appears twice.
        let filtered = Rel::Filter {
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
            input: Box::new(get("R", 2)),
        };
        let plan = Rel::Join {
            inputs: vec![filtered.clone(), filtered.clone()],
            equivalences: vec![],
        };
        let out = eliminate_common_subexpressions(&plan, &BTreeMap::new(), false);

        // The result is a single Let binding the shared Filter, with two
        // LocalGets in the join.
        match &out {
            Rel::Let { id, value, body } => {
                assert_eq!(**value, filtered);
                match &**body {
                    Rel::Join { inputs, .. } => {
                        for i in inputs {
                            assert!(matches!(i, Rel::LocalGet { id: gid, .. } if gid == id));
                        }
                    }
                    other => panic!("expected Join body, got {other}"),
                }
            }
            other => panic!("expected a Let, got {other}"),
        }
        // Arity is preserved.
        assert_eq!(out.arity(), plan.arity());
    }

    #[mz_ore::test]
    fn shares_a_repeated_arrange_by() {
        // Two joins of the same `ArrangeBy(R, [#0])`: the arrangement appears
        // twice and must be bound once (DD lesson L4, arrangement sharing falls
        // out of CSE for free now that ArrangeBy is a first-class node).
        let arranged = Rel::ArrangeBy {
            input: Box::new(get("R", 2)),
            key: vec![EScalar::plain(MirScalarExpr::column(0))],
        };
        let plan = Rel::Join {
            inputs: vec![arranged.clone(), arranged.clone()],
            equivalences: vec![],
        };
        let out = eliminate_common_subexpressions(&plan, &BTreeMap::new(), false);
        match &out {
            Rel::Let { id, value, body } => {
                assert_eq!(**value, arranged);
                match &**body {
                    Rel::Join { inputs, .. } => {
                        for i in inputs {
                            assert!(matches!(i, Rel::LocalGet { id: gid, .. } if gid == id));
                        }
                    }
                    other => panic!("expected Join body, got {other}"),
                }
            }
            other => panic!("expected a Let, got {other}"),
        }
        assert_eq!(out.arity(), plan.arity());
    }

    #[mz_ore::test]
    fn does_not_hoist_index_backed_get_read() {
        // A narrowed read of an index-backed global Get, shared across two joins.
        // Hoisting it into a Let freezes the narrowing projection in front of the
        // Get and defeats JoinImplementation's reuse of the maintained full-width
        // arrangement, so the read must stay inlined when an index covers it.
        let narrowed = Rel::Project {
            outputs: vec![0, 2],
            input: Box::new(Rel::Filter {
                predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
                input: Box::new(global_opaque(1, 3)),
            }),
        };
        let plan = Rel::Union {
            base: Box::new(Rel::Join {
                inputs: vec![narrowed.clone(), get("S", 2)],
                equivalences: vec![],
            }),
            inputs: vec![Rel::Join {
                inputs: vec![narrowed.clone(), get("T", 2)],
                equivalences: vec![],
            }],
        };

        // With an available index on the Get, no Let binds a read of it.
        let covered = eliminate_common_subexpressions(&plan, &avail_one(1, &[0]), false);
        assert!(
            !binds_read_of(&covered, 1),
            "index-backed read must stay inlined, got: {covered}"
        );

        // Control: with no index availability, the shared read IS hoisted.
        let uncovered = eliminate_common_subexpressions(&plan, &BTreeMap::new(), false);
        assert!(
            binds_read_of(&uncovered, 1),
            "without an index the shared read should be hoisted, got: {uncovered}"
        );
    }

    #[mz_ore::test]
    fn leaves_unshared_plans_alone() {
        let plan = Rel::Join {
            inputs: vec![get("R", 2), get("S", 2)],
            equivalences: vec![],
        };
        assert_eq!(
            eliminate_common_subexpressions(&plan, &BTreeMap::new(), false),
            plan
        );
    }

    /// Build an opaque leaf carrying a `LetRec` that binds (and references)
    /// `local_id`. This is exactly what `lower` produces for an unsupported
    /// `WITH MUTUALLY RECURSIVE` subtree.
    fn opaque_letrec(local_id: u64, arity: usize) -> Rel {
        use mz_expr::{Id, LocalId, MirRelationExpr};
        use mz_repr::{ReprRelationType, ReprScalarType};

        let typ = ReprRelationType::new(
            (0..arity)
                .map(|_| ReprScalarType::Int64.nullable(false))
                .collect(),
        );
        let lid = LocalId::new(local_id);
        // LetRec x = Get(x) in Get(x): a self-reference closing the cycle. The
        // body and value both Get the bound LocalId, so the id appears as a
        // binding site and as a reference.
        let mir = MirRelationExpr::LetRec {
            ids: vec![lid.clone()],
            values: vec![MirRelationExpr::Get {
                id: Id::Local(lid.clone()),
                typ: typ.clone(),
                access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
            }],
            limits: vec![None],
            body: Box::new(MirRelationExpr::Get {
                id: Id::Local(lid),
                typ,
                access_strategy: mz_expr::AccessStrategy::UnknownOrLocal,
            }),
        };
        Rel::Opaque(Box::new(mir))
    }

    /// Collect every `LocalId` (as usize) reachable in `rel`, including those
    /// buried in opaque MIR leaves. Mirrors what a real shadowing check sees.
    fn all_local_ids(rel: &Rel) -> std::collections::BTreeSet<usize> {
        let mut out = std::collections::BTreeSet::new();
        fn walk(rel: &Rel, out: &mut std::collections::BTreeSet<usize>) {
            match rel {
                Rel::Let { id, .. } => {
                    out.insert(*id);
                }
                Rel::LocalGet { id, .. } => {
                    out.insert(*id);
                }
                Rel::Opaque(mir) => {
                    out.insert(max_mir_local_id(mir));
                }
                _ => {}
            }
            for c in rel.children() {
                walk(c, out);
            }
        }
        walk(rel, &mut out);
        out
    }

    /// Regression: `max_local_id` must account for `Rel::LetRec` binding ids,
    /// not just `LocalGet` references. A binding whose id is the maximum in the
    /// plan may have no `LocalGet` reference (unreferenced base case), so only
    /// scanning `LocalGet` nodes would miss it. Before the fix, CSE could
    /// allocate an id equal to the unreferenced binding's id, causing a
    /// shadowing collision downstream.
    #[mz_ore::test]
    fn max_local_id_counts_unreferenced_letrec_binding() {
        // Build a Rel::LetRec: id=5 (unreferenced), id=0 (referenced in body).
        // The only LocalGet in the plan references id=0, so a LocalGet-only scan
        // returns max=0. The fix must return max=5 from the binding ids.
        let body_get = Rel::LocalGet {
            id: 0,
            arity: 1,
            get: None,
            version: None,
        };
        let letrec = Rel::LetRec {
            bindings: vec![
                (0usize, get("src", 1)),
                // id=5: no LocalGet references this anywhere.
                (5usize, get("src2", 1)),
            ],
            limits: vec![None, None],
            body: Box::new(body_get),
        };

        let ceiling = max_local_id(&letrec);
        assert!(
            ceiling >= 5,
            "max_local_id returned {ceiling}, expected >= 5 (unreferenced binding id=5)"
        );

        // Running CSE on a plan that nests this LetRec must not introduce a
        // Let id <= 5, which would collide with the unreferenced binding.
        let filtered = Rel::Filter {
            predicates: vec![EScalar::plain(mz_expr::MirScalarExpr::column(0))],
            input: Box::new(get("R", 1)),
        };
        let plan = Rel::Union {
            base: Box::new(Rel::Join {
                inputs: vec![filtered.clone(), filtered.clone()],
                equivalences: vec![],
            }),
            inputs: vec![letrec],
        };
        let out = eliminate_common_subexpressions(&plan, &BTreeMap::new(), false);
        fn let_ids(rel: &Rel, out: &mut Vec<usize>) {
            if let Rel::Let { id, .. } = rel {
                out.push(*id);
            }
            for c in rel.children() {
                let_ids(c, out);
            }
        }
        let mut ids = Vec::new();
        let_ids(&out, &mut ids);
        for id in &ids {
            assert!(
                *id > 5,
                "CSE Let id {id} collides with LetRec binding id 5: {out}"
            );
        }
    }

    /// Regression: a CSE-introduced `Let` id must not collide with a `LocalId`
    /// hidden inside a `Rel::Opaque` leaf (e.g. a bailed `LetRec`). Before the
    /// fix, `max_local_id` ignored opaque leaves, so the fresh-id base could
    /// equal a binding id inside the opaque MIR, shadowing it. Downstream the
    /// production `Demand` pass asserts no shadowing and panics on recursive
    /// CTEs.
    #[mz_ore::test]
    fn cse_id_does_not_collide_with_opaque_letrec_local() {
        // Pick the opaque LocalId to be exactly what the OLD allocation would
        // hand out as the first "fresh" id. The only non-opaque ids here are
        // none, so the old `max_local_id` returns 0 and id_base would be 1.
        let opaque = opaque_letrec(1, 2);

        // A CSE opportunity: a compound subterm referenced twice, forcing a
        // fresh-id allocation. The opaque leaf rides along in the body.
        let filtered = Rel::Filter {
            predicates: vec![EScalar::plain(MirScalarExpr::column(0))],
            input: Box::new(get("R", 2)),
        };
        let plan = Rel::Union {
            base: Box::new(Rel::Join {
                inputs: vec![filtered.clone(), filtered.clone()],
                equivalences: vec![],
            }),
            inputs: vec![opaque],
        };

        let out = eliminate_common_subexpressions(&plan, &BTreeMap::new(), false);

        // CSE must have fired (a Let exists somewhere).
        fn has_let(rel: &Rel) -> bool {
            matches!(rel, Rel::Let { .. }) || rel.children().iter().any(|c| has_let(c))
        }
        assert!(has_let(&out), "expected CSE to introduce a Let: {out}");

        // No CSE-introduced Let id may equal a LocalId inside the opaque leaf.
        // Collect the new Let ids and assert they are all strictly above the
        // opaque's LocalId (1).
        fn let_ids(rel: &Rel, out: &mut Vec<usize>) {
            if let Rel::Let { id, .. } = rel {
                out.push(*id);
            }
            for c in rel.children() {
                let_ids(c, out);
            }
        }
        let mut ids = Vec::new();
        let_ids(&out, &mut ids);
        assert!(!ids.is_empty());
        for id in &ids {
            assert!(
                *id > 1,
                "CSE Let id {id} collides with opaque LocalId 1: {out}"
            );
        }

        // Arity is preserved and the opaque LocalId is still distinct from
        // every Let id (no shadowing).
        assert_eq!(out.arity(), plan.arity());
        let locals = all_local_ids(&out);
        assert!(locals.contains(&1), "opaque LocalId should survive");
        for id in &ids {
            assert!(locals.contains(id));
        }
    }
}
