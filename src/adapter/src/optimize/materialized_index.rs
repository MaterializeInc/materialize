// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Routing reads of a relation through a materialized index.
//!
//! A materialized index (`CREATE MATERIALIZED INDEX`) is a persist shard that
//! holds a copy of a relation with its key columns first, sorted by them. A
//! one-shot query reads it instead of the relation when the locally optimized
//! plan pins the leading key column, either to a literal or through a join
//! equivalence.
//!
//! The route is chosen before the query's timestamp, because it changes which
//! collections need read holds and how fresh they must be. Peek sequencing calls
//! [`choose_routes`] on a locally optimized plan, picks the timestamp from the
//! routed plan's dependencies, and hands the routes to the peek optimizer, which
//! applies them with [`apply_routes`].

use std::collections::{BTreeMap, BTreeSet};

use mz_compute_types::dataflows::{BuildDesc, LookupImport, MirDataflowDescription};
use mz_controller_types::ClusterId;
use mz_expr::{Columns, Id, LocalId, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr};
use mz_repr::{GlobalId, ReprRelationType};
use mz_sql::plan::MaterializedIndexDesc;

use crate::catalog::CatalogState;

/// A materialized index chosen to serve reads of a relation.
#[derive(Clone, Debug)]
pub struct MaterializedIndexRoute {
    /// The materialized index's collection.
    pub id: GlobalId,
    pub desc: MaterializedIndexDesc,
    /// The materialized index's relation type, in stored column order.
    pub typ: ReprRelationType,
}

/// Routes keyed by the relation they replace.
pub type MaterializedIndexRoutes = BTreeMap<GlobalId, MaterializedIndexRoute>;

/// Materialized indexes on `on`, each with the [`GlobalId`] to read it from.
pub fn materialized_indexes_on(
    state: &CatalogState,
    on: GlobalId,
) -> Vec<(GlobalId, MaterializedIndexDesc)> {
    let Some(entry) = state.try_get_entry_by_global_id(&on) else {
        return Vec::new();
    };
    entry
        .used_by()
        .iter()
        .filter_map(|item_id| {
            let mv = state.get_entry(item_id).materialized_view()?;
            let mi = mv.materialized_index.as_ref()?;
            (mi.on == on).then(|| (mv.global_id_writes(), mi.clone()))
        })
        .collect()
}

/// Whether any of `ids` has a materialized index, i.e. whether
/// [`choose_routes`] can return anything for a plan over them.
pub fn any_materialized_index(
    state: &CatalogState,
    ids: impl IntoIterator<Item = GlobalId>,
) -> bool {
    ids.into_iter()
        .any(|id| !materialized_indexes_on(state, id).is_empty())
}

/// Picks a materialized index for each relation in `expr` whose leading key
/// column is pinned, unless `cluster` has an in-memory index on the relation.
pub fn choose_routes(
    expr: &MirRelationExpr,
    state: &CatalogState,
    cluster: ClusterId,
) -> MaterializedIndexRoutes {
    let mut routes = MaterializedIndexRoutes::new();
    let aliases = let_aliases(expr);
    expr.visit_pre(|e| {
        for (on, key0) in pinned_columns(e, &aliases) {
            if routes.contains_key(&on) || state.get_indexes_on(on, cluster).next().is_some() {
                continue;
            }
            let chosen = materialized_indexes_on(state, on)
                .into_iter()
                .find(|(_, desc)| desc.key[0] == key0);
            if let Some((id, desc)) = chosen {
                let typ = state
                    .get_entry_by_global_id(&id)
                    .relation_desc()
                    .map(|desc| ReprRelationType::from(desc.typ()))
                    .expect("materialized index is a relation");
                routes.insert(on, MaterializedIndexRoute { id, desc, typ });
            }
        }
    });
    routes
}

/// The `(relation, column)` pairs that `expr` pins directly: an equality with a
/// literal in a `Filter` over a `Get`, or a join equivalence that relates a
/// `Get` input's column to another input.
fn pinned_columns(
    expr: &MirRelationExpr,
    aliases: &BTreeMap<LocalId, GlobalId>,
) -> Vec<(GlobalId, usize)> {
    let mut pinned = Vec::new();
    match expr {
        MirRelationExpr::Filter { input, predicates } => {
            if let MirRelationExpr::Get {
                id: Id::Global(on),
                typ,
                ..
            } = &**input
            {
                for column in 0..typ.arity() {
                    let column_expr = MirScalarExpr::column(column);
                    if predicates
                        .iter()
                        .any(|p| p.expr_eq_literal(&column_expr).is_some())
                    {
                        pinned.push((*on, column));
                    }
                }
            }
        }
        MirRelationExpr::Join {
            inputs,
            equivalences,
            ..
        } => {
            let mut offset = 0;
            for input in inputs {
                let arity = input.arity();
                if let Some(on) = resolve(input, aliases) {
                    for column in 0..arity {
                        let column_expr = MirScalarExpr::column(offset + column);
                        let related = equivalences.iter().any(|class| {
                            class.contains(&column_expr)
                                && class.iter().any(|other| {
                                    other
                                        .support()
                                        .iter()
                                        .any(|c| *c < offset || *c >= offset + arity)
                                })
                        });
                        if related {
                            pinned.push((on, column));
                        }
                    }
                }
                offset += arity;
            }
        }
        _ => {}
    }
    pinned
}

/// Replaces each routed `Get` with a read of its materialized index,
/// projected back to the relation's column order.
pub fn apply_routes(expr: &mut MirRelationExpr, routes: &MaterializedIndexRoutes) {
    if routes.is_empty() {
        return;
    }
    expr.visit_pre_mut(|e| {
        let MirRelationExpr::Get {
            id: Id::Global(on),
            typ,
            ..
        } = e
        else {
            return;
        };
        let Some(route) = routes.get(on) else {
            return;
        };
        let arity = typ.arity();
        let mut position = vec![0; arity];
        for (stored, column) in route.desc.stored_columns(arity).into_iter().enumerate() {
            position[column] = stored;
        }
        *e = MirRelationExpr::global_get(route.id, route.typ.clone()).project(position);
    });
}

/// Reads routed materialized indexes as lookups where every read of one is a
/// join input whose leading key columns another input of that join pins.
///
/// For each such index, adds an object holding the keys the probing inputs
/// produce, ahead of the object with the joins, and marks the index's
/// import as a lookup on it. Rendering then fetches only parts whose key ranges
/// may hold a key. That keeps a superset of the rows the joins can match, so the
/// joins stay exact.
///
/// A probe input that is a read with only filters, maps and projections on top
/// is read again for the keys. Anything costlier is hoisted into its own object,
/// read by both the join and the keys, so it is computed once. `ids` supplies
/// transient ids for new objects. A lookup that would need more ids than
/// remain is not planned.
pub fn plan_lookup_join(
    df_desc: &mut MirDataflowDescription,
    routes: &MaterializedIndexRoutes,
    ids: &mut Vec<GlobalId>,
) {
    for route in routes.values() {
        if !df_desc.source_imports.contains_key(&route.id) {
            continue;
        }
        plan_lookup(df_desc, route, ids);
    }
}

/// A join input that reads a materialized index, and the keys another input of
/// the join pins for it.
struct LookupSite {
    /// The probing input, with its columns renumbered from zero.
    probe: MirRelationExpr,
    /// Key expressions over `probe`, one per pinned leading key column.
    keys: Vec<MirScalarExpr>,
}

fn plan_lookup(
    df_desc: &mut MirDataflowDescription,
    route: &MaterializedIndexRoute,
    ids: &mut Vec<GlobalId>,
) {
    let mi = route.id;
    let lookups: BTreeSet<GlobalId> = BTreeSet::from([mi]);
    // Every read of `mi` must be a lookup site, all in one object. A let binding
    // whose value is a read of `mi` stands for it: its uses are the reads.
    let mut site_object = None;
    for (position, object) in df_desc.objects_to_build.iter().enumerate() {
        let aliases = let_aliases(object.plan.as_inner());
        let mut refs = 0;
        object.plan.as_inner().visit_pre(|e| {
            if matches!(e, MirRelationExpr::Get { .. }) && resolve(e, &aliases) == Some(mi) {
                refs += 1;
            }
        });
        let definitions = aliases.values().filter(|id| **id == mi).count();
        let reads = refs - definitions;
        if refs > 0 {
            if site_object.is_some() {
                return;
            }
            site_object = Some((position, reads));
        }
    }
    let Some((position, reads)) = site_object else {
        return;
    };

    // Find the sites, hoisting costly probe inputs into new objects.
    let mut sites = Vec::new();
    let mut hoisted = Vec::new();
    let mut out_of_ids = false;
    let object = &mut df_desc.objects_to_build[position];
    let aliases = let_aliases(object.plan.as_inner());
    object.plan.as_inner_mut().visit_pre_mut(|e| {
        let MirRelationExpr::Join {
            inputs,
            equivalences,
            ..
        } = e
        else {
            return;
        };
        let arities: Vec<usize> = inputs.iter().map(|input| input.arity()).collect();
        let offsets: Vec<usize> = arities
            .iter()
            .scan(0, |offset, arity| {
                let start = *offset;
                *offset += arity;
                Some(start)
            })
            .collect();
        for lookup_input in 0..inputs.len() {
            if resolve(&inputs[lookup_input], &aliases) != Some(mi) {
                continue;
            }
            let Some((probe_input, keys)) = pinned_by_one_input(
                equivalences,
                &arities,
                &offsets,
                lookup_input,
                route.desc.key.len(),
                inputs,
                &lookups,
            ) else {
                continue;
            };
            let probe = strip_arrange(&inputs[probe_input]);
            let probe = if is_cheap(probe) {
                probe.clone()
            } else {
                let Some(id) = ids.pop() else {
                    out_of_ids = true;
                    continue;
                };
                let typ = probe.typ();
                let hoisted_expr = strip_arrange_mut(&mut inputs[probe_input]);
                let expr =
                    std::mem::replace(hoisted_expr, MirRelationExpr::global_get(id, typ.clone()));
                hoisted.push(BuildDesc {
                    id,
                    plan: OptimizedMirRelationExpr::declare_optimized(expr),
                });
                MirRelationExpr::global_get(id, typ)
            };
            sites.push(LookupSite { probe, keys });
        }
    });
    // Hoisting is equivalent whether or not the lookup is planned, so keep it.
    let hoisted_count = hoisted.len();
    for (offset, object) in hoisted.into_iter().enumerate() {
        df_desc.objects_to_build.insert(position + offset, object);
    }
    if out_of_ids || sites.len() != reads {
        return;
    }
    let Some(keys_id) = ids.pop() else {
        return;
    };
    let key_len = sites
        .iter()
        .map(|site| site.keys.len())
        .min()
        .expect("reads > 0");
    let parts: Vec<MirRelationExpr> = sites
        .into_iter()
        .map(|LookupSite { probe, keys }| {
            let arity = probe.arity();
            probe
                .map(keys.into_iter().take(key_len).collect())
                .project((arity..arity + key_len).collect())
        })
        .collect();
    let typ = parts[0].typ();
    // No `Distinct`: the render-side collector deduplicates, and stops consuming
    // once there are too many keys to be worth a lookup. A `Distinct` would cost a
    // full arrangement of the probe keys even then.
    let keys = MirRelationExpr::union_many(parts, typ);
    df_desc.objects_to_build.insert(
        position + hoisted_count,
        BuildDesc {
            id: keys_id,
            plan: OptimizedMirRelationExpr::declare_optimized(keys),
        },
    );
    let import = df_desc.source_imports.get_mut(&mi).expect("checked above");
    import.lookup = Some(LookupImport {
        keys_from: keys_id,
        key_columns: (0..key_len).collect(),
    });
}

/// Finds one input other than `lookup_input` that pins the lookup input's
/// leading key columns through `equivalences`, and returns it with the key
/// expressions renumbered to its own columns. Inputs that read a lookup or a
/// let binding cannot probe, because their rows are not available up front.
fn pinned_by_one_input(
    equivalences: &[Vec<MirScalarExpr>],
    arities: &[usize],
    offsets: &[usize],
    lookup_input: usize,
    key_len: usize,
    inputs: &[MirRelationExpr],
    lookups: &BTreeSet<GlobalId>,
) -> Option<(usize, Vec<MirScalarExpr>)> {
    let mut best: Option<(usize, Vec<MirScalarExpr>)> = None;
    for probe_input in 0..inputs.len() {
        if probe_input == lookup_input || !can_probe(&inputs[probe_input], lookups) {
            continue;
        }
        let columns = offsets[probe_input]..offsets[probe_input] + arities[probe_input];
        let mut keys = Vec::new();
        for key_column in 0..key_len {
            let column = MirScalarExpr::column(offsets[lookup_input] + key_column);
            let pinned = equivalences
                .iter()
                .filter(|class| class.contains(&column))
                .flat_map(|class| class.iter())
                .find(|expr| {
                    let support = expr.support();
                    !support.is_empty() && support.iter().all(|c| columns.contains(c))
                });
            let Some(pinned) = pinned else {
                break;
            };
            let mut pinned = pinned.clone();
            pinned.visit_pre_mut(|e| {
                if let MirScalarExpr::Column(c, _) = e {
                    *c -= columns.start;
                }
            });
            keys.push(pinned);
        }
        if !keys.is_empty()
            && best
                .as_ref()
                .is_none_or(|(_, best)| keys.len() > best.len())
        {
            best = Some((probe_input, keys));
        }
    }
    best
}

fn can_probe(input: &MirRelationExpr, lookups: &BTreeSet<GlobalId>) -> bool {
    let mut ok = true;
    input.visit_pre(|e| {
        if let MirRelationExpr::Get { id, .. } = e {
            ok &= match id {
                Id::Local(_) => false,
                Id::Global(id) => !lookups.contains(id),
            };
        }
    });
    ok
}

/// Whether reading `expr` again costs about as much as reading its input.
fn is_cheap(expr: &MirRelationExpr) -> bool {
    match expr {
        MirRelationExpr::Get { .. } => true,
        MirRelationExpr::Filter { input, .. }
        | MirRelationExpr::Map { input, .. }
        | MirRelationExpr::Project { input, .. } => is_cheap(input),
        _ => false,
    }
}

fn strip_arrange(expr: &MirRelationExpr) -> &MirRelationExpr {
    match expr {
        MirRelationExpr::ArrangeBy { input, .. } => strip_arrange(input),
        other => other,
    }
}

fn strip_arrange_mut(expr: &mut MirRelationExpr) -> &mut MirRelationExpr {
    match expr {
        MirRelationExpr::ArrangeBy { input, .. } => strip_arrange_mut(input),
        other => other,
    }
}

/// Let bindings whose value reads a global collection, by binding.
fn let_aliases(expr: &MirRelationExpr) -> BTreeMap<LocalId, GlobalId> {
    let mut aliases = BTreeMap::new();
    expr.visit_pre(|e| {
        if let MirRelationExpr::Let { id, value, .. } = e {
            if let Some(global) = resolve(value, &aliases) {
                aliases.insert(*id, global);
            }
        }
    });
    aliases
}

/// Like [`global_get`], but also looking through let bindings in `aliases`.
fn resolve(expr: &MirRelationExpr, aliases: &BTreeMap<LocalId, GlobalId>) -> Option<GlobalId> {
    match expr {
        MirRelationExpr::Get {
            id: Id::Local(id), ..
        } => aliases.get(id).copied(),
        MirRelationExpr::ArrangeBy { input, .. } | MirRelationExpr::Filter { input, .. } => {
            resolve(input, aliases)
        }
        other => global_get(other),
    }
}

/// The collection `expr` reads, looking through operators that keep its columns.
fn global_get(expr: &MirRelationExpr) -> Option<GlobalId> {
    match expr {
        MirRelationExpr::Get {
            id: Id::Global(id), ..
        } => Some(*id),
        MirRelationExpr::ArrangeBy { input, .. } | MirRelationExpr::Filter { input, .. } => {
            global_get(input)
        }
        _ => None,
    }
}
