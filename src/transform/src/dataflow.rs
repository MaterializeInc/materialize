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

use std::collections::{BTreeMap, BTreeSet};

use mz_compute_client::types::dataflows::{DataflowDesc, IndexImport};
use mz_expr::visit::Visit;
use mz_expr::{
    CollectionPlan, Id, JoinImplementation, LocalId, MapFilterProject, MirRelationExpr,
    MirScalarExpr, RECURSION_LIMIT,
};
use mz_ore::soft_panic_or_log;
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_repr::explain::IndexUsageType;
use mz_repr::GlobalId;

use crate::monotonic::MonotonicFlag;
use crate::{IndexOracle, Optimizer, StatisticsOracle, TransformArgs, TransformError};

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
    stats: &dyn StatisticsOracle,
) -> Result<(), TransformError> {
    let ctx = crate::typecheck::empty_context();

    // Inline views that are used in only one other view.
    inline_views(dataflow)?;

    // Logical optimization pass after view inlining
    optimize_dataflow_relations(
        dataflow,
        indexes,
        stats,
        &Optimizer::logical_optimizer(&ctx),
    )?;

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
    optimize_dataflow_relations(
        dataflow,
        indexes,
        stats,
        &Optimizer::logical_cleanup_pass(&ctx, false),
    )?;

    // Physical optimization pass
    optimize_dataflow_relations(
        dataflow,
        indexes,
        stats,
        &Optimizer::physical_optimizer(&ctx),
    )?;

    optimize_dataflow_monotonic(dataflow)?;

    prune_and_annotate_dataflow_index_imports(dataflow, indexes)?;

    mz_repr::explain::trace_plan(dataflow);

    Ok(())
}

/// Inline views used in one other view, and in no exported objects.
#[tracing::instrument(
    target = "optimizer",
    level = "debug",
    skip_all,
    fields(path.segment = "inline_views")
)]
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

    mz_repr::explain::trace_plan(dataflow);

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
    stats: &dyn StatisticsOracle,
    optimizer: &Optimizer,
) -> Result<(), TransformError> {
    // Re-optimize each dataflow
    // TODO(mcsherry): we should determine indexes from the optimized representation
    // just before we plan to install the dataflow. This would also allow us to not
    // add indexes imperatively to `DataflowDesc`.
    for object in dataflow.objects_to_build.iter_mut() {
        // Re-run all optimizations on the composite views.
        optimizer.transform(
            object.plan.as_inner_mut(),
            TransformArgs::with_id_and_stats(indexes, stats, &object.id),
        )?;
    }

    mz_repr::explain::trace_plan(dataflow);

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
    let mut demand = BTreeMap::new();

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

    mz_repr::explain::trace_plan(dataflow);

    Ok(())
}

/// Pushes demand through views in `view_sequence` in order, removing
/// columns not demanded.
///
/// This method is made public for the sake of testing.
/// TODO: make this private once we allow multiple exports per dataflow.
pub fn optimize_dataflow_demand_inner<'a, I>(
    view_sequence: I,
    demand: &mut BTreeMap<Id, BTreeSet<usize>>,
) -> Result<(), TransformError>
where
    I: Iterator<Item = (Id, &'a mut MirRelationExpr)>,
{
    // Maps id -> The projection that was pushed down on the view with the
    // corresponding id.
    let mut applied_projection = BTreeMap::new();
    // Collect the mutable references to views after pushing projection down
    // in order to run cleanup actions on them in a second loop.
    let mut view_refs = Vec::new();
    let projection_pushdown = crate::movement::ProjectionPushdown;
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
    let mut predicates = BTreeMap::<Id, BTreeSet<mz_expr::MirScalarExpr>>::new();

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

    mz_repr::explain::trace_plan(dataflow);

    Ok(())
}

/// Pushes filters down through views in `view_sequence` in order.
///
/// This method is made public for the sake of testing.
/// TODO: make this private once we allow multiple exports per dataflow.
pub fn optimize_dataflow_filters_inner<'a, I>(
    view_iter: I,
    predicates: &mut BTreeMap<Id, BTreeSet<mz_expr::MirScalarExpr>>,
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
    let mut monotonic_ids = BTreeSet::new();
    for (source_id, (_source, is_monotonic)) in dataflow.source_imports.iter() {
        if *is_monotonic {
            monotonic_ids.insert(source_id.clone());
        }
    }
    for (
        _index_id,
        IndexImport {
            desc: index_desc,
            monotonic,
            ..
        },
    ) in dataflow.index_imports.iter()
    {
        if *monotonic {
            monotonic_ids.insert(index_desc.on_id.clone());
        }
    }

    let monotonic_flag = MonotonicFlag::default();

    for build_desc in dataflow.objects_to_build.iter_mut() {
        monotonic_flag.apply(
            build_desc.plan.as_inner_mut(),
            &monotonic_ids,
            &mut BTreeSet::new(),
        )?;
    }

    mz_repr::explain::trace_plan(dataflow);

    Ok(())
}

/// Restricts the indexes imported by `dataflow` to only the ones it needs.
/// Additionally, it annotates each index import by how the index will be used, i.e., it fills in
/// `IndexImport::usage_types`.
///
/// The input `dataflow` should import all indexes belonging to all views/sources/tables it
/// references.
///
/// The input plans should be normalized with `NormalizeLets`! Otherwise we might find dangling
/// `ArrangeBy`s at the top of unused Let bindings.
///
/// Should be called on a [DataflowDesc] only once.
#[tracing::instrument(
    target = "optimizer",
    level = "debug",
    skip_all,
    fields(path.segment = "index_imports")
)]
fn prune_and_annotate_dataflow_index_imports(
    dataflow: &mut DataflowDesc,
    indexes: &dyn IndexOracle,
) -> Result<(), TransformError> {
    // This will be a mapping of
    // (ids used by exports and objects to build) ->
    // (arrangement keys and usage types on that id that have been explicitly requested by the MIR
    // plans)
    let mut index_reqs_by_id = BTreeMap::new();

    // First, insert `sink_exports` and `index_exports` into `index_reqs_by_id` to account for the
    // (currently only theoretical) possibility that we are using an index that we are also building
    // ourselves.
    for (_id, sink_desc) in dataflow.sink_exports.iter() {
        index_reqs_by_id
            .entry(sink_desc.from)
            .or_insert_with(Vec::new);
    }
    for (_id, (index_desc, _)) in dataflow.index_exports.iter() {
        index_reqs_by_id
            .entry(index_desc.on_id)
            .or_insert_with(Vec::new);
    }

    // Go through the MIR plans of `objects_to_build` and collect which arrangements are requested
    // for which we also have an available index.
    for build_desc in dataflow.objects_to_build.iter_mut() {
        CollectIndexRequests::new(indexes, &mut index_reqs_by_id)
            .collect_index_reqs(build_desc.plan.as_inner_mut())?;
    }

    // Add full index scans, i.e., where an index exists, but either no specific key was requested
    // or the requested key didn't match an existing index in the above code.
    for (id, requests_with_matching_indexes) in index_reqs_by_id.iter_mut() {
        if requests_with_matching_indexes.is_empty() {
            // Pick an arbitrary index to be fully scanned.
            // TODO: There are various edge cases where a better choice would be possible:
            // - Some indexes might be less skewed than others.
            // - Some indexes might have an error, while others don't.
            //   https://github.com/MaterializeInc/materialize/issues/15557
            // - Some indexes might have less extra data in their keys, which won't be used in a
            //   full scan.
            if let Some(key) = indexes.indexes_on(*id).next() {
                requests_with_matching_indexes.push((key.to_owned(), IndexUsageType::FullScan));
            }
        }
    }

    // Annotate index imports by their usage types
    for (
        _index_id,
        IndexImport {
            desc: index_desc,
            typ: _,
            monotonic: _,
            usage_types,
        },
    ) in dataflow.index_imports.iter_mut()
    {
        if let Some(requested_accesses) = index_reqs_by_id.get(&index_desc.on_id) {
            assert!(usage_types.is_none()); // We should be called on a dataflow only once
            *usage_types = Some(Vec::new());
            assert!(!requested_accesses.is_empty()); // See above at the "Add full index scans"
            for (req_key, req_usage_type) in requested_accesses {
                if *req_key == index_desc.key {
                    usage_types.as_mut().unwrap().push(req_usage_type.clone());
                }
            }
        }
    }

    // Prune index imports to only those that are used
    dataflow
        .index_imports
        .retain(|_, index_import| match &index_import.usage_types {
            None => false,
            Some(usage_types) => {
                !usage_types.is_empty()
                // (can be empty here when an other index on this same object is used)
            }
        });

    mz_repr::explain::trace_plan(dataflow);

    Ok(())
}

#[derive(Debug)]
struct CollectIndexRequests<'a> {
    // We were told about these indexes being available.
    indexes_available: &'a dyn IndexOracle,
    // We'll be collecting index requests here.
    index_reqs_by_id: &'a mut BTreeMap<GlobalId, Vec<(Vec<MirScalarExpr>, IndexUsageType)>>,
    // As we recurse down a MirRelationExpr, we'll need to keep track of the context of the
    // current node (see docs on `IndexUsageContext` about what context we keep).
    // Moreover, we need to propagate this context from cte uses to cte definitions.
    // `context_across_lets` will keep track of the contexts that reached each use of a LocalId
    // added together.
    context_across_lets: BTreeMap<LocalId, Vec<IndexUsageContext>>,
    recursion_guard: RecursionGuard,
}

impl<'a> CheckedRecursion for CollectIndexRequests<'a> {
    fn recursion_guard(&self) -> &RecursionGuard {
        &self.recursion_guard
    }
}

impl<'a> CollectIndexRequests<'a> {
    fn new(
        indexes_available: &'a dyn IndexOracle,
        index_reqs_by_id: &'a mut BTreeMap<GlobalId, Vec<(Vec<MirScalarExpr>, IndexUsageType)>>,
    ) -> CollectIndexRequests<'a> {
        CollectIndexRequests {
            indexes_available,
            index_reqs_by_id,
            context_across_lets: BTreeMap::new(),
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }

    pub fn collect_index_reqs(
        &mut self,
        expr: &MirRelationExpr,
    ) -> Result<(), RecursionLimitError> {
        assert!(self.context_across_lets.is_empty());
        self.collect_index_reqs_inner(
            expr,
            &IndexUsageContext::from_usage_type(IndexUsageType::PlanRoot),
        )?;
        assert!(self.context_across_lets.is_empty());
        Ok(())
    }

    fn collect_index_reqs_inner(
        &mut self,
        expr: &MirRelationExpr,
        contexts: &Vec<IndexUsageContext>,
    ) -> Result<(), RecursionLimitError> {
        // See comment on `IndexUsageContext`.
        Ok(match expr {
            MirRelationExpr::Let { id, value, body } => {
                let shadowed_context = self.context_across_lets.insert(id.clone(), Vec::new());
                // No shadowing in MIR
                assert!(shadowed_context.is_none());
                // We go backwards: Recurse on the body and then the value.
                self.collect_index_reqs_inner(body, contexts)?;
                // The above call filled in the entry for `id` in `context_across_lets` (if it
                // was referenced). Anyhow, at least an empty entry should exist, because we started
                // above with inserting it.
                self.collect_index_reqs_inner(
                    value,
                    &self.context_across_lets.get(id).unwrap().clone(),
                )?;
                // Clean up the id from the saved contexts.
                self.context_across_lets.remove(id);
            }
            MirRelationExpr::LetRec {
                ids,
                values,
                limits: _,
                body,
            } => {
                for id in ids {
                    let shadowed_context = self.context_across_lets.insert(id.clone(), Vec::new());
                    assert!(shadowed_context.is_none()); // No shadowing in MIR
                }
                // We go backwards: Recurse on the body first.
                self.collect_index_reqs_inner(body, contexts)?;
                // Reset the contexts of the ids (of the current LetRec), because an arrangement
                // from a value can't be used in the body.
                for id in ids {
                    *self.context_across_lets.get_mut(id).unwrap() = Vec::new();
                }
                // Recurse on the values in reverse order.
                // Note that we do only one pass, i.e., we won't see context through a Get that
                // refers to the previous iteration. But this is ok, because we can't reuse
                // arrangements across iterations anyway.
                for (id, value) in ids.iter().zip(values.iter()).rev() {
                    self.collect_index_reqs_inner(
                        value,
                        &self.context_across_lets.get(id).unwrap().clone(),
                    )?;
                }
                // Clean up the ids from the saved contexts.
                for id in ids {
                    self.context_across_lets.remove(id);
                }
            }
            MirRelationExpr::Get {
                id: Id::Local(local_id),
                ..
            } => {
                // Add the current context to the vector of contexts of `local_id`.
                // (The unwrap is safe, because the Let and LetRec cases start with inserting an
                // empty entry.)
                self.context_across_lets
                    .get_mut(local_id)
                    .unwrap()
                    .extend(contexts.iter().cloned());
                // No recursive call here, because Get has no inputs.
            }
            MirRelationExpr::Join {
                inputs,
                implementation,
                ..
            } => {
                match implementation {
                    JoinImplementation::Differential(..) => {
                        for input in inputs {
                            self.collect_index_reqs_inner(
                                input,
                                &IndexUsageContext::from_usage_type(
                                    IndexUsageType::DifferentialJoin,
                                ),
                            )?;
                        }
                    }
                    JoinImplementation::DeltaQuery(..) => {
                        // For Delta joins, the first input is special, see
                        // https://github.com/MaterializeInc/materialize/issues/6789
                        self.collect_index_reqs_inner(
                            &inputs[0],
                            &IndexUsageContext::from_usage_type(IndexUsageType::DeltaJoin(true)),
                        )?;
                        for input in &inputs[1..] {
                            self.collect_index_reqs_inner(
                                input,
                                &IndexUsageContext::from_usage_type(IndexUsageType::DeltaJoin(
                                    false,
                                )),
                            )?;
                        }
                    }
                    JoinImplementation::IndexedFilter(..) => {
                        for input in inputs {
                            self.collect_index_reqs_inner(
                                input,
                                &IndexUsageContext::from_usage_type(IndexUsageType::Lookup),
                            )?;
                        }
                    }
                    JoinImplementation::Unimplemented => {
                        soft_panic_or_log!(
                            "CollectIndexRequests encountered an Unimplemented join"
                        );
                    }
                }
            }
            MirRelationExpr::ArrangeBy { input, keys } => {
                let mut new_contexts = contexts.clone();
                IndexUsageContext::add_keys(&mut new_contexts, keys);
                self.collect_index_reqs_inner(input, &new_contexts)?;
            }
            MirRelationExpr::Get {
                id: Id::Global(global_id),
                ..
            } => {
                self.index_reqs_by_id
                    .entry(*global_id)
                    .or_insert_with(Vec::new);
                for context in contexts {
                    match &context.requested_keys {
                        None => {
                            // We have some index usage context, but didn't see an `ArrangeBy`.
                            match context.usage_type {
                                IndexUsageType::FullScan => {
                                    // Not possible, because we add these later, only after
                                    // `CollectIndexRequests` has already run.
                                    unreachable!()
                                },
                                // You can find more info on why the following join cases shouldn't
                                // happen in comments of the Join lowering to LIR.
                                IndexUsageType::Lookup => soft_panic_or_log!("CollectIndexRequests encountered an IndexedFilter join without an ArrangeBy"),
                                IndexUsageType::DifferentialJoin => soft_panic_or_log!("CollectIndexRequests encountered a Differential join without an ArrangeBy"),
                                IndexUsageType::DeltaJoin(_) => soft_panic_or_log!("CollectIndexRequests encountered a Delta join without an ArrangeBy"),
                                IndexUsageType::PlanRoot => {
                                    // This is ok: happens when the entire query is a `Get`.
                                },
                                IndexUsageType::Unknown => {
                                    // Not possible, because we create IndexUsageType::Unknown
                                    // only when we see an `ArrangeBy`.
                                    unreachable!()
                                },
                            }
                        }
                        Some(requested_keys) => {
                            for requested_key in requested_keys {
                                if self
                                    .indexes_available
                                    .indexes_on(*global_id)
                                    .find(|available_key| available_key == &requested_key)
                                    .is_some()
                                {
                                    self.index_reqs_by_id
                                        .get_mut(global_id)
                                        .unwrap()
                                        .push((requested_key.clone(), context.usage_type.clone()));
                                }
                            }
                        }
                    }
                }
            }
            _ => {
                // Nothing interesting at this node, empty the context.
                let empty_context = Vec::new();
                // And recurse.
                for child in expr.children() {
                    self.collect_index_reqs_inner(child, &empty_context)?;
                }
            }
        })
    }
}

/// This struct will save info about parent nodes as we are descending down a `MirRelationExpr`.
/// We always start with filling in `usage_type` when we see an operation that uses an arrangement,
/// and then we fill in `requested_keys` when we see an `ArrangeBy`. So, the pattern that we are
/// looking for is
/// ```text
/// <operation that uses an index>
///   ArrangeBy <requested_keys>
///     Get <global_id>
/// ```
/// When we reach a `Get` to a global id, we access this context struct to see if the rest of the
/// pattern is present above the `Get`.
///
/// Note that we usually put this struct in a Vec, because we track context across local let
/// bindings, which means that a node can have multiple parents.
#[derive(Debug, Clone)]
struct IndexUsageContext {
    usage_type: IndexUsageType,
    requested_keys: Option<BTreeSet<Vec<MirScalarExpr>>>,
}

impl IndexUsageContext {
    pub fn from_usage_type(usage_type: IndexUsageType) -> Vec<Self> {
        vec![IndexUsageContext {
            usage_type,
            requested_keys: None,
        }]
    }

    // Add the keys of an ArrangeBy into the context.
    // Soft_panics if haven't already seen something that indicates what the index will be used for.
    pub fn add_keys(contexts: &mut Vec<IndexUsageContext>, keys_to_add: &Vec<Vec<MirScalarExpr>>) {
        if contexts.is_empty() {
            // No join above us, and we are not at the root. Why does this ArrangeBy even exist?
            soft_panic_or_log!("CollectIndexRequests encountered a dangling ArrangeBy");
            // Anyhow, let's create a context with an Unknown index usage, so that we have a place
            // to note down the requested keys below.
            *contexts = IndexUsageContext::from_usage_type(IndexUsageType::Unknown);
        }
        for context in contexts {
            if context.requested_keys.is_none() {
                context.requested_keys = Some(BTreeSet::new());
            }
            context
                .requested_keys
                .as_mut()
                .unwrap()
                .extend(keys_to_add.iter().cloned());
        }
    }
}
