// Copyright Materialize, Inc. and contributors. All rights reserved.
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

use std::collections::{BTreeSet, HashMap, HashSet};

use mz_compute_client::types::dataflows::DataflowDesc;
use mz_expr::visit::Visit;
use mz_expr::{CollectionPlan, Id, LocalId, MapFilterProject, MirRelationExpr};

use crate::{monotonic::MonotonicFlag, IndexOracle, Optimizer, TransformError};

/// Optimizes the implementation of each dataflow.
///
/// Inlines views, performs a full optimization pass including physical
/// planning using the supplied indexes, propagates filtering and projection
/// information to dataflow sources and lifts monotonicity information.
#[tracing::instrument(
    target = "optimizer",
    level = "debug",
    skip_all,
    fields(path.segment ="global")
)]
pub fn optimize_dataflow(
    dataflow: &mut DataflowDesc,
    indexes: &dyn IndexOracle,
) -> Result<(), TransformError> {
    // Inline views that are used in only one other view.
    inline_views(dataflow)?;

    // Logical optimization pass after view inlining
    optimize_dataflow_relations(dataflow, indexes, &Optimizer::logical_optimizer())?;

    optimize_dataflow_filters(dataflow)?;
    // TODO: when the linear operator contract ensures that propagated
    // predicates are always applied, projections and filters can be removed
    // from where they come from. Once projections and filters can be removed,
    // TODO: it would be useful for demand to be optimized after filters
    // that way demand only includes the columns that are still necessary after
    // the filters are applied.
    optimize_dataflow_demand(dataflow)?;

    // A smaller logical optimization pass after projections and filters are
    // pushed down across views.
    optimize_dataflow_relations(dataflow, indexes, &Optimizer::logical_cleanup_pass())?;

    // Physical optimization pass
    optimize_dataflow_relations(dataflow, indexes, &Optimizer::physical_optimizer())?;

    optimize_dataflow_monotonic(dataflow)?;

    optimize_dataflow_index_imports(dataflow, indexes)?;

    mz_repr::explain_new::trace_plan(dataflow);

    Ok(())
}

/// Inline views used in one other view, and in no exported objects.
fn inline_views(dataflow: &mut DataflowDesc) -> Result<(), TransformError> {
    // We cannot inline anything whose `BuildDesc::id` appears in either the
    // `index_exports` or `sink_exports` of `dataflow`, because we lose our
    // ability to name it.

    // A view can / should be in-lined in another view if it is only used by
    // one subsequent view. If there are two distinct views that have not
    // themselves been merged, then too bad and it doesn't get inlined.

    // Starting from the *last* object to build, walk backwards and inline
    // any view that is neither referenced by a `index_exports` nor
    // `sink_exports` nor more than two remaining objects to build.

    for index in (0..dataflow.objects_to_build.len()).rev() {
        // Capture the name used by others to reference this view.
        let global_id = dataflow.objects_to_build[index].id;
        // Determine if any exports directly reference this view.
        let mut occurs_in_export = false;
        for (_gid, sink_desc) in dataflow.sink_exports.iter() {
            if sink_desc.from == global_id {
                occurs_in_export = true;
            }
        }
        for (_, (index_desc, _)) in dataflow.index_exports.iter() {
            if index_desc.on_id == global_id {
                occurs_in_export = true;
            }
        }
        // Count the number of subsequent views that reference this view.
        let mut occurrences_in_later_views = Vec::new();
        for other in (index + 1)..dataflow.objects_to_build.len() {
            if dataflow.objects_to_build[other]
                .plan
                .depends_on()
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
            // identifier for the binding itself. Following `NormalizeLets`, we
            // go with the binding first, then the value, then the body.
            let mut id_gen = crate::IdGen::default();
            let new_local = LocalId::new(id_gen.allocate_id());
            // Use the same `id_gen` to assign new identifiers to `index`.
            crate::normalize_lets::renumber_bindings(
                dataflow.objects_to_build[index].plan.as_inner_mut(),
                &mut id_gen,
            )?;
            // Assign new identifiers to the other relation.
            crate::normalize_lets::renumber_bindings(
                dataflow.objects_to_build[other].plan.as_inner_mut(),
                &mut id_gen,
            )?;
            // Install the `new_local` name wherever `global_id` was used.
            dataflow.objects_to_build[other]
                .plan
                .as_inner_mut()
                .visit_mut_post(&mut |expr| {
                    if let MirRelationExpr::Get { id, .. } = expr {
                        if id == &Id::Global(global_id) {
                            *id = Id::Local(new_local);
                        }
                    }
                })?;

            // With identifiers rewritten, we can replace `other` with
            // a `MirRelationExpr::Let` binding, whose value is `index` and
            // whose body is `other`.
            let body = dataflow.objects_to_build[other]
                .plan
                .as_inner_mut()
                .take_dangerous();
            let value = dataflow.objects_to_build[index]
                .plan
                .as_inner_mut()
                .take_dangerous();
            *dataflow.objects_to_build[other].plan.as_inner_mut() = MirRelationExpr::Let {
                id: new_local,
                value: Box::new(value),
                body: Box::new(body),
            };
            dataflow.objects_to_build.remove(index);
        }
    }

    Ok(())
}

/// Performs either the logical or the physical optimization pass on the
/// dataflow using the supplied set of indexes.
#[tracing::instrument(
    target = "optimizer",
    level = "debug",
    skip_all,
    fields(path.segment = optimizer.name)
)]
fn optimize_dataflow_relations(
    dataflow: &mut DataflowDesc,
    indexes: &dyn IndexOracle,
    optimizer: &Optimizer,
) -> Result<(), TransformError> {
    // Re-optimize each dataflow
    // TODO(mcsherry): we should determine indexes from the optimized representation
    // just before we plan to install the dataflow. This would also allow us to not
    // add indexes imperatively to `DataflowDesc`.
    for object in dataflow.objects_to_build.iter_mut() {
        // Re-run all optimizations on the composite views.
        optimizer.transform(object.plan.as_inner_mut(), indexes)?;
    }

    mz_repr::explain_new::trace_plan(dataflow);

    Ok(())
}

/// Pushes demand information from published outputs to dataflow inputs,
/// projecting away unnecessary columns.
///
/// Dataflows that exist for the sake of generating plan explanations do not
/// have published outputs. In this case, we push demand information from views
/// not depended on by other views to dataflow inputs.
#[tracing::instrument(
    target = "optimizer",
    level = "debug",
    skip_all,
    fields(path.segment ="demand")
)]
fn optimize_dataflow_demand(dataflow: &mut DataflowDesc) -> Result<(), TransformError> {
    // Maps id -> union of known columns demanded from the source/view with the
    // corresponding id.
    let mut demand = HashMap::new();

    if dataflow.index_exports.is_empty() && dataflow.sink_exports.is_empty() {
        // In the absence of any exports, just demand all columns from views
        // that are not depended on by another view, which is currently the last
        // object in `objects_to_build`.

        // A DataflowDesc without exports is currently created in the context of
        // EXPLAIN outputs. This ensures that the output has all the columns of
        // the original explainee.
        if let Some(build_desc) = dataflow.objects_to_build.iter_mut().rev().next() {
            demand
                .entry(Id::Global(build_desc.id))
                .or_insert_with(BTreeSet::new)
                .extend(0..build_desc.plan.as_inner_mut().arity());
        }
    } else {
        // Demand all columns of inputs to sinks.
        for (_id, sink) in dataflow.sink_exports.iter() {
            let input_id = sink.from;
            demand
                .entry(Id::Global(input_id))
                .or_insert_with(BTreeSet::new)
                .extend(0..dataflow.arity_of(&input_id));
        }

        // Demand all columns of inputs to exported indexes.
        for (_id, (desc, _typ)) in dataflow.index_exports.iter() {
            let input_id = desc.on_id;
            demand
                .entry(Id::Global(input_id))
                .or_insert_with(BTreeSet::new)
                .extend(0..dataflow.arity_of(&input_id));
        }
    }

    optimize_dataflow_demand_inner(
        dataflow
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| (Id::Global(build_desc.id), build_desc.plan.as_inner_mut())),
        &mut demand,
    )?;

    mz_repr::explain_new::trace_plan(dataflow);

    Ok(())
}

/// Pushes demand through views in `view_sequence` in order, removing
/// columns not demanded.
///
/// This method is made public for the sake of testing.
/// TODO: make this private once we allow multiple exports per dataflow.
pub fn optimize_dataflow_demand_inner<'a, I>(
    view_sequence: I,
    demand: &mut HashMap<Id, BTreeSet<usize>>,
) -> Result<(), TransformError>
where
    I: Iterator<Item = (Id, &'a mut MirRelationExpr)>,
{
    // Maps id -> The projection that was pushed down on the view with the
    // corresponding id.
    let mut applied_projection = HashMap::new();
    // Collect the mutable references to views after pushing projection down
    // in order to run cleanup actions on them in a second loop.
    let mut view_refs = Vec::new();
    let projection_pushdown = crate::projection_pushdown::ProjectionPushdown;
    for (id, view) in view_sequence {
        if let Some(columns) = demand.get(&id) {
            let projection_pushed_down = columns.iter().map(|c| *c).collect();
            // Push down the projection consisting of the entries of `columns`
            // in increasing order.
            projection_pushdown.action(view, &projection_pushed_down, demand)?;
            let new_type = view.typ();
            applied_projection.insert(id, (projection_pushed_down, new_type));
        }
        view_refs.push(view);
    }

    for view in view_refs {
        // Update `Get` nodes to reflect any columns that have been projected away.
        projection_pushdown.update_projection_around_get(view, &applied_projection)?;
    }

    Ok(())
}

/// Pushes predicate to dataflow inputs.
#[tracing::instrument(
    target = "optimizer",
    level = "debug",
    skip_all,
    fields(path.segment ="filters")
)]
fn optimize_dataflow_filters(dataflow: &mut DataflowDesc) -> Result<(), TransformError> {
    // Contains id -> predicates map, describing those predicates that
    // can (but need not) be applied to the collection named by `id`.
    let mut predicates = HashMap::<Id, HashSet<mz_expr::MirScalarExpr>>::new();

    // Propagate predicate information from outputs to inputs.
    optimize_dataflow_filters_inner(
        dataflow
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| (Id::Global(build_desc.id), build_desc.plan.as_inner_mut())),
        &mut predicates,
    )?;

    // Push predicate information into the SourceDesc.
    for (source_id, (source, _monotonic)) in dataflow.source_imports.iter_mut() {
        if let Some(list) = predicates.remove(&Id::Global(*source_id)) {
            if !list.is_empty() {
                // Canonicalize the order of predicates, for stable plans.
                let mut list = list.into_iter().collect::<Vec<_>>();
                list.sort();
                // Install no-op predicate information if none exists.
                if source.arguments.operators.is_none() {
                    source.arguments.operators = Some(MapFilterProject::new(source.typ.arity()));
                }
                // Add any predicates that can be pushed to the source.
                if let Some(operator) = source.arguments.operators.take() {
                    source.arguments.operators = Some(operator.filter(list));
                    source.arguments.operators.as_mut().map(|x| x.optimize());
                }
            }
        }
    }

    mz_repr::explain_new::trace_plan(dataflow);

    Ok(())
}

/// Pushes filters down through views in `view_sequence` in order.
///
/// This method is made public for the sake of testing.
/// TODO: make this private once we allow multiple exports per dataflow.
pub fn optimize_dataflow_filters_inner<'a, I>(
    view_iter: I,
    predicates: &mut HashMap<Id, HashSet<mz_expr::MirScalarExpr>>,
) -> Result<(), TransformError>
where
    I: Iterator<Item = (Id, &'a mut MirRelationExpr)>,
{
    let transform = crate::predicate_pushdown::PredicatePushdown::default();
    for (id, view) in view_iter {
        if let Some(list) = predicates.get(&id).clone() {
            if !list.is_empty() {
                *view = view.take_dangerous().filter(list.iter().cloned());
            }
        }
        transform.action(view, predicates)?;
    }
    Ok(())
}

/// Propagates information about monotonic inputs through operators.
#[tracing::instrument(
    target = "optimizer",
    level = "debug",
    skip_all,
    fields(path.segment ="monotonic")
)]
pub fn optimize_dataflow_monotonic(dataflow: &mut DataflowDesc) -> Result<(), TransformError> {
    let mut monotonic_ids = HashSet::new();
    for (source_id, (_source, is_monotonic)) in dataflow.source_imports.iter() {
        if *is_monotonic {
            monotonic_ids.insert(source_id.clone());
        }
    }
    for (_index_id, (index_desc, _, is_monotonic)) in dataflow.index_imports.iter() {
        if *is_monotonic {
            monotonic_ids.insert(index_desc.on_id.clone());
        }
    }

    let monotonic_flag = MonotonicFlag::default();

    for build_desc in dataflow.objects_to_build.iter_mut() {
        monotonic_flag.apply(
            build_desc.plan.as_inner_mut(),
            &monotonic_ids,
            &mut HashSet::new(),
        )?;
    }

    Ok(())
}

/// Restricts the indexes imported by `dataflow` to only the ones it needs.
///
/// The input `dataflow` should import all indexes belonging to all views it
/// references.
#[tracing::instrument(
    target = "optimizer",
    level = "debug",
    skip_all,
    fields(path.segment ="index_imports")
)]
fn optimize_dataflow_index_imports(
    dataflow: &mut DataflowDesc,
    indexes: &dyn IndexOracle,
) -> Result<(), TransformError> {
    // Generate (a mapping of views used by exports and objects to build) ->
    // (indexes from that view that have been explicitly chosen to be used)
    let mut indexes_by_view = HashMap::new();
    for sink_desc in dataflow.sink_exports.iter() {
        indexes_by_view
            .entry(sink_desc.1.from)
            .or_insert_with(HashSet::new);
    }
    for (_, (index_desc, _)) in dataflow.index_exports.iter() {
        indexes_by_view
            .entry(index_desc.on_id)
            .or_insert_with(HashSet::new);
    }
    for build_desc in dataflow.objects_to_build.iter_mut() {
        build_desc
            .plan
            .as_inner_mut()
            .visit_post(&mut |e| match e {
                MirRelationExpr::Get {
                    id: Id::Global(id), ..
                } => {
                    indexes_by_view.entry(*id).or_insert_with(HashSet::new);
                }
                MirRelationExpr::ArrangeBy { input, keys } => {
                    if let MirRelationExpr::Get {
                        id: Id::Global(id), ..
                    } = &**input
                    {
                        for key in keys {
                            if indexes.indexes_on(*id).find(|k| k == &key).is_some() {
                                indexes_by_view.get_mut(id).unwrap().insert(key.clone());
                            }
                        }
                    }
                }
                _ => {}
            })?;
    }
    for (id, keys) in indexes_by_view.iter_mut() {
        if keys.is_empty() {
            // If a dataflow uses a view that has indexes, but it has not
            // chosen to use any specific index of that view, pick an arbitrary
            // index to read from.
            if let Some(key) = indexes.indexes_on(*id).next() {
                keys.insert(key.to_owned());
            }
        }
    }
    dataflow.index_imports.retain(|_, (index_desc, _, _)| {
        if let Some(keys) = indexes_by_view.get(&index_desc.on_id) {
            keys.contains(&index_desc.key)
        } else {
            false
        }
    });
    Ok(())
}
