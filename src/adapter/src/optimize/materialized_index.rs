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

use std::collections::BTreeMap;

use mz_controller_types::ClusterId;
use mz_expr::{Columns, Id, MirRelationExpr, MirScalarExpr};
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
                if let MirRelationExpr::Get {
                    id: Id::Global(on), ..
                } = input
                {
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
                            pinned.push((*on, column));
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
