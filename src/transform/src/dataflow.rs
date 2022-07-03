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

use mz_compute_client::DataflowDesc;
use mz_expr::visit::Visit;
use mz_expr::{CollectionPlan, Id, LocalId, MirRelationExpr};
use mz_ore::id_gen::IdGen;
use mz_repr::GlobalId;
use mz_storage::client::transforms::LinearOperator;

use crate::{monotonic::MonotonicFlag, IndexOracle, Optimizer, TransformError};

/// Optimizes the implementation of each dataflow.
///
/// Inlines views, performs a full optimization pass including physical
/// planning using the supplied indexes, propagates filtering and projection
/// information to dataflow sources and lifts monotonicity information.
#[tracing::instrument(target = "optimizer", level = "trace", skip_all)]
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
            // identifier for the binding itself. Following `UpdateLet`, we
            // go with the binding first, then the value, then the body.
            let update_let = crate::update_let::UpdateLet::default();
            let mut id_gen = crate::IdGen::default();
            let new_local = LocalId::new(id_gen.allocate_id());
            // Use the same `id_gen` to assign new identifiers to `index`.
            update_let.action(
                dataflow.objects_to_build[index].plan.as_inner_mut(),
                &mut HashMap::new(),
                &mut id_gen,
            )?;
            // Assign new identifiers to the other relation.
            update_let.action(
                dataflow.objects_to_build[other].plan.as_inner_mut(),
                &mut HashMap::new(),
                &mut id_gen,
            )?;
            // Install the `new_local` name wherever `global_id` was used.
            #[allow(deprecated)]
            dataflow.objects_to_build[other]
                .plan
                .as_inner_mut()
                .visit_mut_post_nolimit(&mut |expr| {
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
        // Re-name bindings to accommodate other analyses, specifically
        // `InlineLet` which probably wants a reworking in any case.
        // Re-run all optimizations on the composite views.
        optimizer.transform(object.plan.as_inner_mut(), indexes)?;
    }

    Ok(())
}

/// Pushes demand information from published outputs to dataflow inputs,
/// projecting away unnecessary columns.
///
/// Dataflows that exist for the sake of generating plan explanations do not
/// have published outputs. In this case, we push demand information from views
/// not depended on by other views to dataflow inputs.
fn optimize_dataflow_demand(dataflow: &mut DataflowDesc) -> Result<(), TransformError> {
    // Maps id -> union of known columns demanded from the source/view with the
    // corresponding id.
    let mut demand = HashMap::new();

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

    optimize_dataflow_demand_inner(
        dataflow
            .objects_to_build
            .iter_mut()
            .rev()
            .map(|build_desc| (Id::Global(build_desc.id), build_desc.plan.as_inner_mut())),
        &mut demand,
    )?;

    // Push demand information into the SourceDesc.
    for (source_id, (source, _monotonic)) in dataflow.source_imports.iter_mut() {
        if let Some(columns) = demand.get(&Id::Global(*source_id)).clone() {
            // Install no-op demand information if none exists.
            if source.arguments.operators.is_none() {
                source.arguments.operators = Some(LinearOperator {
                    predicates: Vec::new(),
                    projection: (0..source.typ.arity()).collect(),
                })
            }
            // Restrict required columns by those identified as demanded.
            if let Some(operator) = &mut source.arguments.operators {
                operator.projection.retain(|col| columns.contains(col));
            }
        }
    }

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
            projection_pushdown.action(view, &projection_pushed_down, demand);
            applied_projection.insert(id, projection_pushed_down);
        } else if id == Id::Global(GlobalId::Explain) {
            // If we just want to explain the plan for a given view, then there
            // will be no upstream demand. Just demand all columns from views
            // that are not depended on by another view.
            let arity = view.arity();
            projection_pushdown.action(view, &(0..arity).collect(), demand);
        }
        view_refs.push(view);
    }

    let typ_update = crate::update_let::UpdateLet::default();
    for view in view_refs {
        // Update column references to views where projections were pushed down.
        projection_pushdown.update_projection_around_get(view, &applied_projection);
        // Types need to be updated after ProjectionPushdown
        // because the width of each view may have changed.
        typ_update.action(view, &mut HashMap::new(), &mut IdGen::default())?;
    }

    Ok(())
}

/// Pushes predicate to dataflow inputs.
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
        if let Some(list) = predicates.get(&Id::Global(*source_id)).clone() {
            // Install no-op predicate information if none exists.
            if source.arguments.operators.is_none() {
                source.arguments.operators = Some(LinearOperator {
                    predicates: Vec::new(),
                    projection: (0..source.typ.arity()).collect(),
                })
            }
            // Add any predicates that can be pushed to the source.
            if let Some(operator) = &mut source.arguments.operators {
                operator.predicates.extend(list.iter().cloned());
                operator.predicates.sort();
            }
        }
    }

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

/// Propagates information about monotonic inputs through views.
pub fn optimize_dataflow_monotonic(dataflow: &mut DataflowDesc) -> Result<(), TransformError> {
    let mut monotonic = std::collections::HashSet::new();
    for (source_id, (_source, is_monotonic)) in dataflow.source_imports.iter_mut() {
        if *is_monotonic {
            monotonic.insert(source_id.clone());
        }
    }

    let monotonic_flag = MonotonicFlag::default();

    // Propagate predicate information from outputs to inputs.
    for build_desc in dataflow.objects_to_build.iter_mut() {
        monotonic_flag.apply(
            build_desc.plan.as_inner_mut(),
            &monotonic,
            &mut HashSet::new(),
        )?;
    }

    Ok(())
}
