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
use mz_expr::{Columns, Id, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr};
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
    expr.visit_pre(|e| {
        for (on, key0) in pinned_columns(e) {
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
fn pinned_columns(expr: &MirRelationExpr) -> Vec<(GlobalId, usize)> {
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
                if let Some(on) = global_get(input) {
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

/// Reads a routed materialized index as a lookup when an object of the
/// dataflow joins it, at its leading key column, with one other input.
///
/// Adds an object `keys_id` holding the distinct probe-side keys, ahead of the
/// joining object, and marks the materialized index's import as a lookup on it.
/// Rendering then fetches only parts whose key range may hold a probe key. That
/// keeps a superset of the rows the join can match, so the join stays exact.
///
/// NOTE(poc): the probe side is computed twice, once for the keys and once in
/// the join. Applies only when the materialized index is read exactly once in
/// the dataflow, so no other reader sees the restricted import.
pub fn plan_lookup_join(
    df_desc: &mut MirDataflowDescription,
    routes: &MaterializedIndexRoutes,
    keys_id: GlobalId,
) -> bool {
    let lookups: BTreeSet<GlobalId> = routes.values().map(|route| route.id).collect();
    let mut found: Option<(usize, GlobalId, MirRelationExpr)> = None;
    for (position, object) in df_desc.objects_to_build.iter().enumerate() {
        object.plan.as_inner().visit_pre(|e| {
            if found.is_some() {
                return;
            }
            let MirRelationExpr::Join {
                inputs,
                equivalences,
                ..
            } = e
            else {
                return;
            };
            if inputs.len() != 2 {
                return;
            }
            let arities = [inputs[0].arity(), inputs[1].arity()];
            for (lookup_side, probe_side) in [(0, 1), (1, 0)] {
                let Some(mi) = global_get(&inputs[lookup_side]).filter(|id| lookups.contains(id))
                else {
                    continue;
                };
                let offset = |side: usize| if side == 0 { 0 } else { arities[0] };
                let key_column = MirScalarExpr::column(offset(lookup_side));
                let probe_columns = offset(probe_side)..offset(probe_side) + arities[probe_side];
                let probe_key = equivalences
                    .iter()
                    .filter(|class| class.contains(&key_column))
                    .flat_map(|class| class.iter())
                    .find(|expr| {
                        let support = expr.support();
                        !support.is_empty() && support.iter().all(|c| probe_columns.contains(c))
                    });
                let Some(probe_key) = probe_key else {
                    continue;
                };
                let mut probe_key = probe_key.clone();
                probe_key.visit_pre_mut(|e| {
                    if let MirScalarExpr::Column(c, _) = e {
                        *c -= probe_columns.start;
                    }
                });
                let probe = match &inputs[probe_side] {
                    MirRelationExpr::ArrangeBy { input, .. } => (**input).clone(),
                    other => other.clone(),
                };
                let mut has_locals = false;
                probe.visit_pre(|e| {
                    has_locals |= matches!(
                        e,
                        MirRelationExpr::Get {
                            id: Id::Local(_),
                            ..
                        }
                    );
                });
                if has_locals {
                    continue;
                }
                let keys = probe
                    .map_one(probe_key)
                    .project(vec![arities[probe_side]])
                    .distinct();
                found = Some((position, mi, keys));
                return;
            }
        });
    }
    let Some((position, mi, keys)) = found else {
        return false;
    };
    let mut reads = 0;
    for object in &df_desc.objects_to_build {
        object.plan.as_inner().visit_pre(|e| {
            if global_get(e) == Some(mi) && matches!(e, MirRelationExpr::Get { .. }) {
                reads += 1;
            }
        });
    }
    let Some(import) = df_desc.source_imports.get_mut(&mi) else {
        return false;
    };
    if reads != 1 {
        return false;
    }
    import.lookup = Some(LookupImport {
        keys_from: keys_id,
        key_column: 0,
    });
    df_desc.objects_to_build.insert(
        position,
        BuildDesc {
            id: keys_id,
            plan: OptimizedMirRelationExpr::declare_optimized(keys),
        },
    );
    true
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
