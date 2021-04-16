// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Whole-dataflow optimization
//!
//! A dataflow may contain multiple views, each of which may only be
//! optimized locally. However, information like demand and predicate
//! pushdown can be applied across views once we understand the context
//! in which the views will be executed.

use dataflow_types::{DataflowDesc, LinearOperator};
use expr::{Id, LocalId, MirRelationExpr};
use std::collections::{HashMap, HashSet};

/// Optimizes the implementation of each dataflow.
///
/// This method is currently limited in scope to propagating filtering and
/// projection information, though it could certainly generalize beyond.
pub fn optimize_dataflow(dataflow: &mut DataflowDesc) {
    // Inline views that are used in only one other view.
    inline_views(dataflow);

    optimize_dataflow_filters(dataflow);
    // TODO: when the linear operator contract ensures that propagated
    // predicates are always applied, projections and filters can be removed
    // from where they come from. Once projections and filters can be removed,
    // TODO: it would be useful for demand to be optimized after filters
    // that way demand only includes the columns that are still necessary after
    // the filters are applied.
    optimize_dataflow_demand(dataflow);
    monotonic::optimize_dataflow_monotonic(dataflow);
}

/// Inline views used in one other view, and in no exported objects.
fn inline_views(dataflow: &mut DataflowDesc) {
    // We cannot inline anything whose `BuildDesc::id` appears in either the
    // `index_exports` or `sink_exports` of `dataflow`, because we lose our
    // ability to name it.

    // A view can / should be in-lined in another view if it is only used by
    // one subsequent view. If there are two distinct views that have not
    // themselves been merged, then too bad and it doesn't get inlined.

    // Starting from the *last* object to build, walk backwards and inline
    // any view (not index, because who knows wtf that is) that is neither
    // referenced by a `index_exports` nor `sink_exports` nor more than two
    // remaining objects to build.

    for index in (0..dataflow.objects_to_build.len()).rev() {
        // Test that we are not attempting to inline an index.
        // TODO: Figure out what that would mean and why indexes
        // are separate concepts in the first place.
        if dataflow.objects_to_build[index].typ.is_some() {
            // Capture the name used by others to reference this view.
            let global_id = dataflow.objects_to_build[index].id;
            // Determine if any exports directly reference this view.
            let mut occurs_in_export = false;
            for (_gid, sink_desc) in dataflow.sink_exports.iter() {
                if sink_desc.from == global_id {
                    occurs_in_export = true;
                }
            }
            for (_, index_desc, _) in dataflow.index_exports.iter() {
                if index_desc.on_id == global_id {
                    occurs_in_export = true;
                }
            }
            // Count the number of subsequent views that reference this view.
            let mut occurrences_in_later_views = Vec::new();
            for other in (index + 1)..dataflow.objects_to_build.len() {
                if dataflow.objects_to_build[other]
                    .relation_expr
                    .as_ref()
                    .global_uses()
                    .contains(&global_id)
                {
                    occurrences_in_later_views.push(other);
                }
            }
            // Inline if the view is referenced in one view and no exports.
            if !occurs_in_export && occurrences_in_later_views.len() == 1 {
                let other = occurrences_in_later_views[0];
                // We can remove this view and insert it in the later view,
                // but are not able to relocate the later view `other`.

                // When splicing in the `index` view, we need to create disjoint
                // identifiers for the Let's `body` and `value`, as well as a new
                // identifier for the binding itself. Following `UpdateLet`, we
                // go with the binding first, then the value, then the body.
                let update_let = crate::update_let::UpdateLet;
                let mut id_gen = crate::IdGen::default();
                let new_local = LocalId::new(id_gen.allocate_id());
                // Use the same `id_gen` to assign new identifiers to `index`.
                update_let.action(
                    dataflow.objects_to_build[index].relation_expr.as_mut(),
                    &mut HashMap::new(),
                    &mut id_gen,
                );
                // Assign new identifiers to the other relation.
                update_let.action(
                    dataflow.objects_to_build[other].relation_expr.as_mut(),
                    &mut HashMap::new(),
                    &mut id_gen,
                );
                // Install the `new_local` name wherever `global_id` was used.
                dataflow.objects_to_build[other]
                    .relation_expr
                    .as_mut()
                    .visit_mut(&mut |expr| {
                        if let MirRelationExpr::Get { id, .. } = expr {
                            if id == &Id::Global(global_id) {
                                *id = Id::Local(new_local);
                            }
                        }
                    });

                // With identifiers rewritten, we can replace `other` with
                // a `MirRelationExpr::Let` binding, whose value is `index` and
                // whose body is `other`.
                let body = dataflow.objects_to_build[other]
                    .relation_expr
                    .as_mut()
                    .take_dangerous();
                let value = dataflow.objects_to_build[index]
                    .relation_expr
                    .as_mut()
                    .take_dangerous();
                *dataflow.objects_to_build[other].relation_expr.as_mut() = MirRelationExpr::Let {
                    id: new_local,
                    value: Box::new(value),
                    body: Box::new(body),
                };
                dataflow.objects_to_build.remove(index);
            }
        }
    }

    // Re-optimize each dataflow.
    // TODO: We should attempt to minimize the number of re-optimizations, as each
    // may introduce e.g. `ArrangeBy` operators that make sense at that optimization
    // moment, but less sense later on (i.e. cost, but aren't needed). One candidate
    // is to perform *logical* optimizations early (on view definition, when params
    // are instatiated, here) and then *physical* optimization (e.g. join planning)
    // only once (and probably in here).
    // TODO(mcsherry): we should determine indexes from the optimized representation
    // just before we plan to install the dataflow. This would also allow us to not
    // add indexes imperatively to `DataflowDesc`.
    let mut indexes = HashMap::new();
    for (global_id, (desc, _type)) in dataflow.index_imports.iter() {
        indexes
            .entry(desc.on_id)
            .or_insert_with(Vec::new)
            .push((*global_id, desc.keys.clone()));
    }
    let optimizer = crate::Optimizer::default();
    for object in dataflow.objects_to_build.iter_mut() {
        // Re-name bindings to accommodate other analyses, specifically
        // `InlineLet` which probably wants a reworking in any case.
        // Re-run all optimizations on the composite views.
        optimizer
            .transform(object.relation_expr.as_mut(), &indexes)
            .unwrap();
    }
}

/// Pushes demand information from published outputs to dataflow inputs.
fn optimize_dataflow_demand(dataflow: &mut DataflowDesc) {
    let mut demand = HashMap::new();

    // Demand all columns of inputs to sinks.
    for (_id, sink) in dataflow.sink_exports.iter() {
        let input_id = sink.from;
        demand
            .entry(Id::Global(input_id))
            .or_insert_with(HashSet::new)
            .extend(0..dataflow.arity_of(&input_id));
    }

    // Demand all columns of inputs to exported indexes.
    for (_id, desc, _typ) in dataflow.index_exports.iter() {
        let input_id = desc.on_id;
        demand
            .entry(Id::Global(input_id))
            .or_insert_with(HashSet::new)
            .extend(0..dataflow.arity_of(&input_id));
    }

    // Propagate demand information from outputs to inputs.
    for build_desc in dataflow.objects_to_build.iter_mut().rev() {
        let transform = crate::demand::Demand;
        if let Some(columns) = demand.get(&Id::Global(build_desc.id)).clone() {
            transform.action(
                build_desc.relation_expr.as_mut(),
                columns.clone(),
                &mut demand,
            );
        }
    }

    // Push demand information into the SourceDesc.
    for (source_id, (source_desc, _)) in dataflow.source_imports.iter_mut() {
        if let Some(columns) = demand.get(&Id::Global(*source_id)).clone() {
            // Install no-op demand information if none exists.
            if source_desc.operators.is_none() {
                source_desc.operators = Some(LinearOperator {
                    predicates: Vec::new(),
                    projection: (0..source_desc.bare_desc.arity()).collect(),
                })
            }
            // Restrict required columns by those identified as demanded.
            if let Some(operator) = &mut source_desc.operators {
                operator.projection.retain(|col| columns.contains(col));
            }
        }
    }
}

/// Pushes predicate to dataflow inputs.
fn optimize_dataflow_filters(dataflow: &mut DataflowDesc) {
    // Contains id -> predicates map, describing those predicates that
    // can (but need not) be applied to the collection named by `id`.
    let mut predicates = HashMap::<Id, HashSet<expr::MirScalarExpr>>::new();

    // Propagate predicate information from outputs to inputs.
    for build_desc in dataflow.objects_to_build.iter_mut().rev() {
        let transform = crate::predicate_pushdown::PredicatePushdown;
        if let Some(list) = predicates.get(&Id::Global(build_desc.id)).clone() {
            if !list.is_empty() {
                *build_desc.relation_expr.as_mut() = build_desc
                    .relation_expr
                    .as_mut()
                    .take_dangerous()
                    .filter(list.iter().cloned());
            }
        }
        transform.dataflow_transform(build_desc.relation_expr.as_mut(), &mut predicates);
    }

    // Push predicate information into the SourceDesc.
    for (source_id, (source_desc, _)) in dataflow.source_imports.iter_mut() {
        if let Some(list) = predicates.get(&Id::Global(*source_id)).clone() {
            // Install no-op predicate information if none exists.
            if source_desc.operators.is_none() {
                source_desc.operators = Some(LinearOperator {
                    predicates: Vec::new(),
                    projection: (0..source_desc.bare_desc.arity()).collect(),
                })
            }
            // Add any predicates that can be pushed to the source.
            if let Some(operator) = &mut source_desc.operators {
                operator.predicates.extend(list.iter().cloned());
            }
        }
    }
}

/// Analysis to identify monotonic collections, especially TopK inputs.
pub mod monotonic {

    use dataflow_types::{DataflowDesc, SourceConnector, SourceEnvelope};
    use expr::MirRelationExpr;
    use expr::{GlobalId, Id};
    use std::collections::HashSet;

    // Determines if a relation is monotonic, and applies any optimizations along the way.
    fn is_monotonic(expr: &mut MirRelationExpr, sources: &HashSet<GlobalId>) -> bool {
        match expr {
            MirRelationExpr::Get { id, .. } => {
                if let Id::Global(id) = id {
                    sources.contains(id)
                } else {
                    false
                }
            }
            MirRelationExpr::Project { input, .. } => is_monotonic(input, sources),
            MirRelationExpr::Filter { input, predicates } => {
                let is_monotonic = is_monotonic(input, sources);
                // Non-temporal predicates can introduce non-monotonicity, as they
                // can result in the future removal of records.
                // TODO: this could be improved to only restrict if upper bounds
                // are present, as temporal lower bounds only delay introduction.
                is_monotonic && !predicates.iter().any(|p| p.contains_temporal())
            }
            MirRelationExpr::Map { input, .. } => is_monotonic(input, sources),
            MirRelationExpr::TopK {
                input, monotonic, ..
            } => {
                *monotonic = is_monotonic(input, sources);
                false
            }
            MirRelationExpr::Reduce {
                input,
                aggregates,
                monotonic,
                ..
            } => {
                *monotonic = is_monotonic(input, sources);
                // Reduce is monotonic iff its input is and it is a "distinct",
                // with no aggregate values; otherwise it may need to retract.
                *monotonic && aggregates.is_empty()
            }
            MirRelationExpr::Union { base, inputs } => {
                let mut monotonic = is_monotonic(base, sources);
                for input in inputs.iter_mut() {
                    let monotonic_i = is_monotonic(input, sources);
                    monotonic = monotonic && monotonic_i;
                }
                monotonic
            }
            MirRelationExpr::ArrangeBy { input, .. } => is_monotonic(input, sources),
            MirRelationExpr::FlatMap { input, func, .. } => {
                let is_monotonic = is_monotonic(input, sources);
                is_monotonic && func.preserves_monotonicity()
            }
            MirRelationExpr::Join { inputs, .. } => {
                // If all inputs to the join are monotonic then so is the join.
                let mut monotonic = true;
                for input in inputs.iter_mut() {
                    let monotonic_i = is_monotonic(input, sources);
                    monotonic = monotonic && monotonic_i;
                }
                monotonic
            }
            MirRelationExpr::Constant { rows: Ok(rows), .. } => {
                rows.iter().all(|(_, diff)| diff > &0)
            }
            MirRelationExpr::Threshold { input } => is_monotonic(input, sources),
            // Let and Negate remain
            _ => {
                expr.visit1_mut(|e| {
                    is_monotonic(e, sources);
                });
                false
            }
        }
    }

    /// Propagates information about monotonic inputs through views.
    pub fn optimize_dataflow_monotonic(dataflow: &mut DataflowDesc) {
        let mut monotonic = std::collections::HashSet::new();
        for (source_id, (source_desc, _)) in dataflow.source_imports.iter_mut() {
            if let SourceConnector::External {
                envelope: SourceEnvelope::None,
                ..
            } = source_desc.connector
            {
                monotonic.insert(source_id.clone());
            }
        }

        // Propagate predicate information from outputs to inputs.
        for build_desc in dataflow.objects_to_build.iter_mut() {
            is_monotonic(build_desc.relation_expr.as_mut(), &monotonic);
        }
    }
}
