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

use itertools::Itertools;
use mz_compute_types::dataflows::{BuildDesc, DataflowDesc, DataflowDescription, IndexImport};
use mz_expr::{
    AccessStrategy, CollectionPlan, Id, JoinImplementation, LocalId, MapFilterProject,
    MirRelationExpr, MirScalarExpr, RECURSION_LIMIT,
};
use mz_ore::stack::{CheckedRecursion, RecursionGuard, RecursionLimitError};
use mz_ore::{assert_none, soft_assert_eq_or_log, soft_assert_or_log, soft_panic_or_log};
use mz_repr::GlobalId;
use mz_repr::explain::{DeltaJoinIndexUsageType, IndexUsageType, UsedIndexes};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::monotonic::MonotonicFlag;
use crate::notice::CrossJoin;
use crate::notice::RawOptimizerNotice;
use crate::{IndexOracle, Optimizer, TransformCtx, TransformError};

/// Optimizes the implementation of each dataflow.
///
/// Inlines views, performs a full optimization pass including physical
/// planning using the supplied indexes, propagates filtering and projection
/// information to dataflow sources and lifts monotonicity information.
#[mz_ore::instrument(
    target = "optimizer",
    level = "debug",
    fields(path.segment ="global")
)]
pub fn optimize_dataflow(
    dataflow: &mut DataflowDesc,
    transform_ctx: &mut TransformCtx,
    fast_path_optimizer: bool,
) -> Result<(), TransformError> {
    // Inline views that are used in only one other view.
    inline_views(dataflow)?;

    if fast_path_optimizer {
        optimize_dataflow_relations(
            dataflow,
            &Optimizer::fast_path_optimizer(transform_ctx),
            transform_ctx,
        )?;
    } else {
        // Logical optimization pass after view inlining
        optimize_dataflow_relations(
            dataflow,
            #[allow(deprecated)]
            &Optimizer::logical_optimizer(transform_ctx),
            transform_ctx,
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
            &Optimizer::logical_cleanup_pass(transform_ctx, false),
            transform_ctx,
        )?;

        // Physical optimization pass
        optimize_dataflow_relations(
            dataflow,
            &Optimizer::physical_optimizer(transform_ctx),
            transform_ctx,
        )?;

        optimize_dataflow_monotonic(dataflow, transform_ctx)?;
    }

    prune_and_annotate_dataflow_index_imports(
        dataflow,
        transform_ctx.indexes,
        transform_ctx.df_meta,
    )?;

    // Warning: If you want to add a transform call here, consider it very carefully whether it
    // could accidentally invalidate information that we already derived above in
    // `optimize_dataflow_monotonic` or `prune_and_annotate_dataflow_index_imports`.

    mz_repr::explain::trace_plan(dataflow);

    gather_notices(dataflow, transform_ctx.df_meta);

    Ok(())
}

/// Inline views used in one other view, and in no exported objects.
#[mz_ore::instrument(
    target = "optimizer",
    level = "debug",
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
                .visit_pre_mut(|expr| {
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

    mz_repr::explain::trace_plan(dataflow);

    Ok(())
}

/// Performs either the logical or the physical optimization pass on the
/// dataflow using the supplied set of indexes.
#[mz_ore::instrument(
    target = "optimizer",
    level = "debug",
    fields(path.segment = optimizer.name)
)]
fn optimize_dataflow_relations(
    dataflow: &mut DataflowDesc,
    optimizer: &Optimizer,
    ctx: &mut TransformCtx,
) -> Result<(), TransformError> {
    // Re-optimize each dataflow
    for object in dataflow.objects_to_build.iter_mut() {
        // Re-run all optimizations on the composite views.
        ctx.set_global_id(object.id);
        optimizer.transform(object.plan.as_inner_mut(), ctx)?;
        ctx.reset_global_id();
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
#[mz_ore::instrument(
    target = "optimizer",
    level = "debug",
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
    let projection_pushdown = crate::movement::ProjectionPushdown::default();
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
        projection_pushdown.update_projection_around_get(view, &applied_projection);
    }

    Ok(())
}

/// Pushes predicate to dataflow inputs.
#[mz_ore::instrument(
    target = "optimizer",
    level = "debug",
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
    for (source_id, (source, _monotonic, _upper)) in dataflow.source_imports.iter_mut() {
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

/// Propagates information about monotonic inputs through operators,
/// using [`mz_repr::optimize::OptimizerFeatures`] from `ctx` for [`crate::analysis::Analysis`].
#[mz_ore::instrument(
    target = "optimizer",
    level = "debug",
    fields(path.segment ="monotonic")
)]
pub fn optimize_dataflow_monotonic(
    dataflow: &mut DataflowDesc,
    ctx: &mut TransformCtx,
) -> Result<(), TransformError> {
    let mut monotonic_ids = BTreeSet::new();
    for (source_id, (_source, is_monotonic, _upper)) in dataflow.source_imports.iter() {
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
        monotonic_flag.transform(build_desc.plan.as_inner_mut(), ctx, &monotonic_ids)?;
    }

    mz_repr::explain::trace_plan(dataflow);

    Ok(())
}

/// Restricts the indexes imported by `dataflow` to only the ones it needs.
/// It also adds to the `DataflowMetainfo` how each index will be used.
/// It also annotates global `Get`s with whether they will be reads from Persist or an index, plus
/// their index usage types.
///
/// The input `dataflow` should import all indexes belonging to all views/sources/tables it
/// references.
///
/// The input plans should be normalized with `NormalizeLets`! Otherwise, we might find dangling
/// `ArrangeBy`s at the top of unused Let bindings.
#[mz_ore::instrument(
    target = "optimizer",
    level = "debug",
    fields(path.segment = "index_imports")
)]
fn prune_and_annotate_dataflow_index_imports(
    dataflow: &mut DataflowDesc,
    indexes: &dyn IndexOracle,
    dataflow_metainfo: &mut DataflowMetainfo,
) -> Result<(), TransformError> {
    // Preparation.
    // Let's save the unique keys of the sources. This will inform which indexes to choose for full
    // scans. (We can't get this info from `source_imports`, because `source_imports` only has those
    // sources that are not getting an indexed read.)
    let mut source_keys = BTreeMap::new();
    for build_desc in dataflow.objects_to_build.iter() {
        build_desc
            .plan
            .as_inner()
            .visit_pre(|expr: &MirRelationExpr| match expr {
                MirRelationExpr::Get {
                    id: Id::Global(global_id),
                    typ,
                    ..
                } => {
                    source_keys.entry(*global_id).or_insert_with(|| {
                        typ.keys
                            .iter()
                            .map(|key| {
                                key.iter()
                                    // Convert the Vec<usize> key to Vec<MirScalarExpr>, so that
                                    // later we can more easily compare index keys to these keys.
                                    .map(|col_idx| MirScalarExpr::column(*col_idx))
                                    .collect()
                            })
                            .collect()
                    });
                }
                _ => {}
            });
    }

    // This will be a mapping of
    // (ids used by exports and objects to build) ->
    // (arrangement keys and usage types on that id that have been requested)
    let mut index_reqs_by_id = BTreeMap::new();

    // Go through the MIR plans of `objects_to_build` and collect which arrangements are requested
    // for which we also have an available index.
    for build_desc in dataflow.objects_to_build.iter_mut() {
        CollectIndexRequests::new(&source_keys, indexes, &mut index_reqs_by_id)
            .collect_index_reqs(build_desc.plan.as_inner_mut())?;
    }

    // Collect index usages by `sink_exports`.
    // A sink export sometimes wants to directly use an imported index. I know of one case where
    // this happens: The dataflow for a SUBSCRIBE on an indexed view won't have any
    // `objects_to_build`, but will want to directly read from the index and write to a sink.
    for (_sink_id, sink_desc) in dataflow.sink_exports.iter() {
        // First, let's see if there exists an index on the id that the sink wants. If not, there is
        // nothing we can do here.
        if let Some((idx_id, arbitrary_idx_key)) = indexes.indexes_on(sink_desc.from).next() {
            // If yes, then we'll add a request of _some_ index: If we already collected an index
            // request on this id, then use that, otherwise use the above arbitrarily picked index.
            let requested_idxs = index_reqs_by_id
                .entry(sink_desc.from)
                .or_insert_with(Vec::new);
            if let Some((already_req_idx_id, already_req_key, _)) = requested_idxs.get(0) {
                requested_idxs.push((
                    *already_req_idx_id,
                    already_req_key.clone(),
                    IndexUsageType::SinkExport,
                ));
            } else {
                requested_idxs.push((
                    idx_id,
                    arbitrary_idx_key.to_owned(),
                    IndexUsageType::SinkExport,
                ));
            }
        }
    }

    // Collect index usages by `index_exports`.
    for (_id, (index_desc, _)) in dataflow.index_exports.iter() {
        // First, let's see if there exists an index on the id that the exported index is on. If
        // not, there is nothing we can do here.
        if let Some((idx_id, arbitrary_index_key)) = indexes.indexes_on(index_desc.on_id).next() {
            // If yes, then we'll add an index request of some index: If we already collected an
            // index request on this id, then use that, otherwise use the above arbitrarily picked
            // index.
            let requested_idxs = index_reqs_by_id
                .entry(index_desc.on_id)
                .or_insert_with(Vec::new);
            if let Some((already_req_idx_id, already_req_key, _)) = requested_idxs.get(0) {
                requested_idxs.push((
                    *already_req_idx_id,
                    already_req_key.clone(),
                    IndexUsageType::IndexExport,
                ));
            } else {
                // This is surprising: Actually, an index creation dataflow always has a plan in
                // `objects_to_build` that will have a Get of the object that the index is on (see
                // `DataflowDescription::export_index`). Therefore, we should have already requested
                // an index usage when seeing that Get in `CollectIndexRequests`.
                soft_panic_or_log!(
                    "We are seeing an index export on an id that's not mentioned in `objects_to_build`"
                );
                requested_idxs.push((
                    idx_id,
                    arbitrary_index_key.to_owned(),
                    IndexUsageType::IndexExport,
                ));
            }
        }
    }

    // By now, `index_reqs_by_id` has all ids that we think might benefit from having an index on.
    // Moreover, for each of these ids, if any index exists on it, then we should have already
    // picked one. If not, then we have a bug somewhere. In that case, do a soft panic, and add an
    // Unknown usage, picking an arbitrary index.
    for (id, index_reqs) in index_reqs_by_id.iter_mut() {
        if index_reqs.is_empty() {
            // Try to pick an arbitrary index to be fully scanned.
            if let Some((idx_id, key)) = indexes.indexes_on(*id).next() {
                soft_panic_or_log!(
                    "prune_and_annotate_dataflow_index_imports didn't find any index for an id, even though one exists
id: {}, key: {:?}",
                    id,
                    key
                );
                index_reqs.push((idx_id, key.to_owned(), IndexUsageType::Unknown));
            }
        }
    }

    // Adjust FullScans to not introduce a new index dependency if there is also some non-FullScan
    // request on the same id.
    // `full_scan_changes` saves the changes that we do: Each (Get id, index id) entry indicates
    // that if a Get has that id, then any full scan index accesses on it should be changed to use
    // the indicated index id.
    let mut full_scan_changes = BTreeMap::new();
    for (get_id, index_reqs) in index_reqs_by_id.iter_mut() {
        // Let's choose a non-FullScan access (if exists).
        if let Some((picked_idx, picked_idx_key)) = choose_index(
            &source_keys,
            get_id,
            &index_reqs
                .iter()
                .filter_map(|(idx_id, key, usage_type)| match usage_type {
                    IndexUsageType::FullScan => None,
                    _ => Some((*idx_id, key.clone())),
                })
                .collect_vec(),
        ) {
            // Found a non-FullScan access. Modify all FullScans to use the same index as that one.
            for (idx_id, key, usage_type) in index_reqs {
                match usage_type {
                    IndexUsageType::FullScan => {
                        full_scan_changes.insert(get_id, picked_idx);
                        *idx_id = picked_idx;
                        key.clone_from(&picked_idx_key);
                    }
                    _ => {}
                }
            }
        }
    }
    // Apply the above full scan changes to also the Gets.
    for build_desc in dataflow.objects_to_build.iter_mut() {
        build_desc
            .plan
            .as_inner_mut()
            .visit_pre_mut(|expr: &mut MirRelationExpr| {
                match expr {
                    MirRelationExpr::Get {
                        id: Id::Global(global_id),
                        typ: _,
                        access_strategy: persist_or_index,
                    } => {
                        if let Some(new_idx_id) = full_scan_changes.get(global_id) {
                            match persist_or_index {
                                AccessStrategy::UnknownOrLocal => {
                                    // Should have been already filled by `collect_index_reqs`.
                                    unreachable!()
                                }
                                AccessStrategy::Persist => {
                                    // We already know that it's an indexed access.
                                    unreachable!()
                                }
                                AccessStrategy::SameDataflow => {
                                    // We have not added such annotations yet.
                                    unreachable!()
                                }
                                AccessStrategy::Index(accesses) => {
                                    for (idx_id, usage_type) in accesses {
                                        if matches!(usage_type, IndexUsageType::FullScan) {
                                            *idx_id = *new_idx_id;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            });
    }

    // Annotate index imports by their usage types
    dataflow_metainfo.index_usage_types = BTreeMap::new();
    for (
        index_id,
        IndexImport {
            desc: index_desc,
            typ: _,
            monotonic: _,
        },
    ) in dataflow.index_imports.iter_mut()
    {
        // A sanity check that we are not importing an index that we are also exporting.
        assert!(
            !dataflow
                .index_exports
                .iter()
                .map(|(exported_index_id, _)| exported_index_id)
                .any(|exported_index_id| exported_index_id == index_id)
        );

        let mut new_usage_types = Vec::new();
        // Let's see whether something has requested an index on this object that this imported
        // index is on.
        if let Some(index_reqs) = index_reqs_by_id.get(&index_desc.on_id) {
            for (req_idx_id, req_key, req_usage_type) in index_reqs {
                if req_idx_id == index_id {
                    soft_assert_eq_or_log!(*req_key, index_desc.key);
                    new_usage_types.push(req_usage_type.clone());
                }
            }
        }
        if !new_usage_types.is_empty() {
            dataflow_metainfo
                .index_usage_types
                .insert(*index_id, new_usage_types);
        }
    }

    // Prune index imports to only those that are used
    dataflow
        .index_imports
        .retain(|id, _index_import| dataflow_metainfo.index_usage_types.contains_key(id));

    // Determine AccessStrategy::SameDataflow accesses. These were classified as
    // AccessStrategy::Persist inside collect_index_reqs, so now we check these, and if the id is of
    // a collection that we are building ourselves, then we adjust the access strategy.
    let mut objects_to_build_ids = BTreeSet::new();
    for BuildDesc { id, plan: _ } in dataflow.objects_to_build.iter() {
        objects_to_build_ids.insert(id.clone());
    }
    for build_desc in dataflow.objects_to_build.iter_mut() {
        build_desc
            .plan
            .as_inner_mut()
            .visit_pre_mut(|expr: &mut MirRelationExpr| match expr {
                MirRelationExpr::Get {
                    id: Id::Global(global_id),
                    typ: _,
                    access_strategy,
                } => match access_strategy {
                    AccessStrategy::Persist => {
                        if objects_to_build_ids.contains(global_id) {
                            *access_strategy = AccessStrategy::SameDataflow;
                        }
                    }
                    _ => {}
                },
                _ => {}
            });
    }

    // A sanity check that all Get annotations indicate indexes that are present in `index_imports`.
    for build_desc in dataflow.objects_to_build.iter() {
        build_desc
            .plan
            .as_inner()
            .visit_pre(|expr: &MirRelationExpr| match expr {
                MirRelationExpr::Get {
                    id: Id::Global(_),
                    typ: _,
                    access_strategy: AccessStrategy::Index(accesses),
                } => {
                    for (idx_id, _) in accesses {
                        soft_assert_or_log!(
                            dataflow.index_imports.contains_key(idx_id),
                            "Dangling Get index annotation"
                        );
                    }
                }
                _ => {}
            });
    }

    mz_repr::explain::trace_plan(dataflow);

    Ok(())
}

/// Pick an index from a given Vec of index keys.
///
/// Currently, we pick as follows:
///  - If there is an index on a unique key, then we pick that. (It might be better distributed, and
///    is less likely to get dropped than other indexes.)
///  - Otherwise, we pick an arbitrary index.
///
/// TODO: There are various edge cases where a better choice would be possible:
/// - Some indexes might be less skewed than others. (Although, picking a unique key tries to
///   capture this already.)
/// - Some indexes might have an error, while others don't.
///   <https://github.com/MaterializeInc/database-issues/issues/4455>
/// - Some indexes might have more extra data in their keys (because of being on more complicated
///   expressions than just column references), which won't be used in a full scan.
fn choose_index(
    source_keys: &BTreeMap<GlobalId, BTreeSet<Vec<MirScalarExpr>>>,
    id: &GlobalId,
    indexes: &Vec<(GlobalId, Vec<MirScalarExpr>)>,
) -> Option<(GlobalId, Vec<MirScalarExpr>)> {
    match source_keys.get(id) {
        None => indexes.iter().next().cloned(), // pick an arbitrary index
        Some(coll_keys) => match indexes
            .iter()
            .find(|(_idx_id, key)| coll_keys.contains(&*key))
        {
            Some((idx_id, key)) => Some((*idx_id, key.clone())),
            None => indexes.iter().next().cloned(), // pick an arbitrary index
        },
    }
}

#[derive(Debug)]
struct CollectIndexRequests<'a> {
    /// We were told about these unique keys on sources.
    source_keys: &'a BTreeMap<GlobalId, BTreeSet<Vec<MirScalarExpr>>>,
    /// We were told about these indexes being available.
    indexes_available: &'a dyn IndexOracle,
    /// We'll be collecting index requests here.
    index_reqs_by_id:
        &'a mut BTreeMap<GlobalId, Vec<(GlobalId, Vec<MirScalarExpr>, IndexUsageType)>>,
    /// As we recurse down a MirRelationExpr, we'll need to keep track of the context of the
    /// current node (see docs on `IndexUsageContext` about what context we keep).
    /// Moreover, we need to propagate this context from cte uses to cte definitions.
    /// `context_across_lets` will keep track of the contexts that reached each use of a LocalId
    /// added together.
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
        source_keys: &'a BTreeMap<GlobalId, BTreeSet<Vec<MirScalarExpr>>>,
        indexes_available: &'a dyn IndexOracle,
        index_reqs_by_id: &'a mut BTreeMap<
            GlobalId,
            Vec<(GlobalId, Vec<MirScalarExpr>, IndexUsageType)>,
        >,
    ) -> CollectIndexRequests<'a> {
        CollectIndexRequests {
            source_keys,
            indexes_available,
            index_reqs_by_id,
            context_across_lets: BTreeMap::new(),
            recursion_guard: RecursionGuard::with_limit(RECURSION_LIMIT),
        }
    }

    pub fn collect_index_reqs(
        &mut self,
        expr: &mut MirRelationExpr,
    ) -> Result<(), RecursionLimitError> {
        assert!(self.context_across_lets.is_empty());
        self.collect_index_reqs_inner(
            expr,
            &IndexUsageContext::from_usage_type(IndexUsageType::PlanRootNoArrangement),
        )?;
        assert!(self.context_across_lets.is_empty());
        // Sanity check that we don't have any `DeltaJoinIndexUsageType::Unknown` remaining.
        for (_id, index_reqs) in self.index_reqs_by_id.iter() {
            for (_, _, index_usage_type) in index_reqs {
                soft_assert_or_log!(
                    !matches!(
                        index_usage_type,
                        IndexUsageType::DeltaJoin(DeltaJoinIndexUsageType::Unknown)
                    ),
                    "Delta join Unknown index usage remained"
                );
            }
        }
        Ok(())
    }

    fn collect_index_reqs_inner(
        &mut self,
        expr: &mut MirRelationExpr,
        contexts: &Vec<IndexUsageContext>,
    ) -> Result<(), RecursionLimitError> {
        self.checked_recur_mut(|this| {

            // If an index exists on `on_id`, this function picks an index to be fully scanned.
            let pick_index_for_full_scan = |on_id: &GlobalId| {
                // Note that the choice we make here might be modified later at the
                // "Adjust FullScans to not introduce a new index dependency".
                choose_index(
                    this.source_keys,
                    on_id,
                    &this.indexes_available.indexes_on(*on_id).map(
                        |(idx_id, key)| (idx_id, key.iter().cloned().collect_vec())
                    ).collect_vec()
                )
            };

            // See comment on `IndexUsageContext`.
            Ok(match expr {
                MirRelationExpr::Join {
                    inputs,
                    implementation,
                    ..
                } => {
                    match implementation {
                        JoinImplementation::Differential(..) => {
                            for input in inputs {
                                this.collect_index_reqs_inner(
                                    input,
                                    &IndexUsageContext::from_usage_type(
                                        IndexUsageType::DifferentialJoin,
                                    ),
                                )?;
                            }
                        }
                        JoinImplementation::DeltaQuery(..) => {
                            // For Delta joins, the first input is special, see
                            // https://github.com/MaterializeInc/database-issues/issues/2115
                            this.collect_index_reqs_inner(
                                &mut inputs[0],
                                &IndexUsageContext::from_usage_type(IndexUsageType::DeltaJoin(DeltaJoinIndexUsageType::Unknown)),
                            )?;
                            for input in &mut inputs[1..] {
                                this.collect_index_reqs_inner(
                                    input,
                                    &IndexUsageContext::from_usage_type(IndexUsageType::DeltaJoin(
                                        DeltaJoinIndexUsageType::Lookup,
                                    )),
                                )?;
                            }
                        }
                        JoinImplementation::IndexedFilter(_coll_id, idx_id, ..) => {
                            for input in inputs {
                                this.collect_index_reqs_inner(
                                    input,
                                    &IndexUsageContext::from_usage_type(IndexUsageType::Lookup(*idx_id)),
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
                    this.collect_index_reqs_inner(input, &IndexUsageContext::add_keys(contexts, keys))?;
                }
                MirRelationExpr::Get {
                    id: Id::Global(global_id),
                    access_strategy: persist_or_index,
                    ..
                } => {
                    this.index_reqs_by_id
                        .entry(*global_id)
                        .or_insert_with(Vec::new);
                    // If the context is empty, it means we didn't see an operator that would
                    // specifically want to use an index for this Get. However, let's still try to
                    // find an index for a full scan.
                    let mut try_full_scan = contexts.is_empty();
                    let mut index_accesses = Vec::new();
                    for context in contexts {
                        match &context.requested_keys {
                            None => {
                                // We have some index usage context, but didn't see an `ArrangeBy`.
                                try_full_scan = true;
                                match context.usage_type {
                                    IndexUsageType::FullScan | IndexUsageType::SinkExport | IndexUsageType::IndexExport => {
                                        // Not possible, because these don't go through
                                        // IndexUsageContext at all.
                                        unreachable!()
                                    },
                                    // You can find more info on why the following join cases
                                    // shouldn't happen in comments of the Join lowering to LIR.
                                    IndexUsageType::Lookup(_) => soft_panic_or_log!("CollectIndexRequests encountered an IndexedFilter join without an ArrangeBy"),
                                    IndexUsageType::DifferentialJoin => soft_panic_or_log!("CollectIndexRequests encountered a Differential join without an ArrangeBy"),
                                    IndexUsageType::DeltaJoin(_) => soft_panic_or_log!("CollectIndexRequests encountered a Delta join without an ArrangeBy"),
                                    IndexUsageType::PlanRootNoArrangement => {
                                        // This is ok: the entire plan is a `Get`, with not even an
                                        // `ArrangeBy`. Note that if an index exists, the usage will
                                        // be saved as `FullScan` (NOT as `PlanRootNoArrangement`),
                                        // because we are going into the `try_full_scan` if.
                                    },
                                    IndexUsageType::FastPathLimit => {
                                        // These are created much later, not even inside
                                        // `prune_and_annotate_dataflow_index_imports`.
                                        unreachable!()
                                    }
                                    IndexUsageType::DanglingArrangeBy => {
                                        // Not possible, because we create `DanglingArrangeBy`
                                        // only when we see an `ArrangeBy`.
                                        unreachable!()
                                    },
                                    IndexUsageType::Unknown => {
                                        // These are added only after `CollectIndexRequests` has run.
                                        unreachable!()
                                    }
                                }
                            }
                            Some(requested_keys) => {
                                for requested_key in requested_keys {
                                    match this
                                        .indexes_available
                                        .indexes_on(*global_id)
                                        .find(|(available_idx_id, available_key)| {
                                            match context.usage_type {
                                                IndexUsageType::Lookup(req_idx_id) => {
                                                    // `LiteralConstraints` already picked an index
                                                    // by id. Let's use that one.
                                                    assert!(!(available_idx_id == &req_idx_id && available_key != &requested_key));
                                                    available_idx_id == &req_idx_id
                                                },
                                                _ => available_key == &requested_key,
                                            }
                                        })
                                    {
                                        Some((idx_id, key)) => {
                                            this.index_reqs_by_id
                                                .get_mut(global_id)
                                                .unwrap()
                                                .push((idx_id, key.to_owned(), context.usage_type.clone()));
                                            index_accesses.push((idx_id, context.usage_type.clone()));
                                        },
                                        None => {
                                            // If there is a key requested for which we don't have an
                                            // index, then we might still be able to do a full scan of a
                                            // differently keyed index.
                                            try_full_scan = true;
                                        }
                                    }
                                }
                                if requested_keys.is_empty() {
                                    // It's a bit weird if an MIR ArrangeBy is not requesting any
                                    // key, but let's try a full scan in that case anyhow.
                                    try_full_scan = true;
                                }
                            }
                        }
                    }
                    if try_full_scan {
                        // Keep in mind that when having 2 contexts coming from 2 uses of a Let,
                        // this code can't distinguish between the case when there is 1 ArrangeBy at the
                        // top of the Let, or when the 2 uses each have an `ArrangeBy`. In both cases,
                        // we'll add only 1 full scan, which would be wrong in the latter case. However,
                        // the latter case can't currently happen until we do
                        // https://github.com/MaterializeInc/database-issues/issues/6363
                        // Also note that currently we are deduplicating index usage types when
                        // printing index usages in EXPLAIN.
                        if let Some((idx_id, key)) = pick_index_for_full_scan(global_id) {
                            this.index_reqs_by_id
                                .get_mut(global_id)
                                .unwrap()
                                .push((idx_id, key.to_owned(), IndexUsageType::FullScan));
                            index_accesses.push((idx_id, IndexUsageType::FullScan));
                        }
                    }
                    if index_accesses.is_empty() {
                        *persist_or_index = AccessStrategy::Persist;
                    } else {
                        *persist_or_index = AccessStrategy::Index(index_accesses);
                    }
                }
                MirRelationExpr::Get {
                    id: Id::Local(local_id),
                    ..
                } => {
                    // Add the current context to the vector of contexts of `local_id`.
                    // (The unwrap is safe, because the Let and LetRec cases start with inserting an
                    // empty entry.)
                    this.context_across_lets
                        .get_mut(local_id)
                        .unwrap()
                        .extend(contexts.iter().cloned());
                    // No recursive call here, because Get has no inputs.
                }
                MirRelationExpr::Let { id, value, body } => {
                    let shadowed_context = this.context_across_lets.insert(id.clone(), Vec::new());
                    // No shadowing in MIR
                    assert_none!(shadowed_context);
                    // We go backwards: Recurse on the body and then the value.
                    this.collect_index_reqs_inner(body, contexts)?;
                    // The above call filled in the entry for `id` in `context_across_lets` (if it
                    // was referenced). Anyhow, at least an empty entry should exist, because we started
                    // above with inserting it.
                    this.collect_index_reqs_inner(
                        value,
                        &this.context_across_lets[id].clone(),
                    )?;
                    // Clean up the id from the saved contexts.
                    this.context_across_lets.remove(id);
                }
                MirRelationExpr::LetRec {
                    ids,
                    values,
                    limits: _,
                    body,
                } => {
                    for id in ids.iter() {
                        let shadowed_context = this.context_across_lets.insert(id.clone(), Vec::new());
                        assert_none!(shadowed_context); // No shadowing in MIR
                    }
                    // We go backwards: Recurse on the body first.
                    this.collect_index_reqs_inner(body, contexts)?;
                    // Reset the contexts of the ids (of the current LetRec), because an arrangement
                    // from a value can't be used in the body.
                    for id in ids.iter() {
                        *this.context_across_lets.get_mut(id).unwrap() = Vec::new();
                    }
                    // Recurse on the values in reverse order.
                    // Note that we do only one pass, i.e., we won't see context through a Get that
                    // refers to the previous iteration. But this is ok, because we can't reuse
                    // arrangements across iterations anyway.
                    for (id, value) in ids.iter().rev().zip_eq(values.iter_mut().rev()) {
                        this.collect_index_reqs_inner(
                            value,
                            &this.context_across_lets[id].clone(),
                        )?;
                    }
                    // Clean up the ids from the saved contexts.
                    for id in ids {
                        this.context_across_lets.remove(id);
                    }
                }
                _ => {
                    // Nothing interesting at this node, recurse with the empty context (regardless of
                    // what context we got from above).
                    let empty_context = Vec::new();
                    for child in expr.children_mut() {
                        this.collect_index_reqs_inner(child, &empty_context)?;
                    }
                }
            })
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

    // Add the keys of an ArrangeBy into the contexts.
    // Soft_panics if haven't already seen something that indicates what the index will be used for.
    pub fn add_keys(
        old_contexts: &Vec<IndexUsageContext>,
        keys_to_add: &Vec<Vec<MirScalarExpr>>,
    ) -> Vec<IndexUsageContext> {
        let old_contexts = if old_contexts.is_empty() {
            // No join above us, and we are not at the root. Why does this ArrangeBy even exist?
            soft_panic_or_log!("CollectIndexRequests encountered a dangling ArrangeBy");
            // Anyhow, let's create a context with a `DanglingArrangeBy` index usage, so that we
            // have a place to note down the requested keys below.
            IndexUsageContext::from_usage_type(IndexUsageType::DanglingArrangeBy)
        } else {
            old_contexts.clone()
        };
        old_contexts
            .into_iter()
            .flat_map(|old_context| {
                if !matches!(
                    old_context.usage_type,
                    IndexUsageType::DeltaJoin(DeltaJoinIndexUsageType::Unknown)
                ) {
                    // If it's not an unknown delta join usage, then we simply note down the new
                    // keys into `requested_keys`.
                    let mut context = old_context.clone();
                    if context.requested_keys.is_none() {
                        context.requested_keys = Some(BTreeSet::new());
                    }
                    context
                        .requested_keys
                        .as_mut()
                        .unwrap()
                        .extend(keys_to_add.iter().cloned());
                    Some(context).into_iter().chain(None)
                } else {
                    // If it's an unknown delta join usage, then we need to figure out whether this
                    // is a full scan or a lookup.
                    //
                    // `source_key` in `DeltaPathPlan` determines which arrangement we are going to
                    // scan when starting the rendering of a delta path. This is the one for which
                    // we want a `DeltaJoinIndexUsageType::FirstInputFullScan`.
                    //
                    // However, `DeltaPathPlan` is an LIR concept, and here we need to figure out
                    // the `source_key` based on the MIR plan. We do this by doing the same as
                    // `DeltaJoinPlan::create_from`: choose the smallest key (by `Ord`).
                    let source_key = keys_to_add
                        .iter()
                        .min()
                        .expect("ArrangeBy below a delta join has at least one key");
                    let full_scan_context = IndexUsageContext {
                        requested_keys: Some(BTreeSet::from([source_key.clone()])),
                        usage_type: IndexUsageType::DeltaJoin(
                            DeltaJoinIndexUsageType::FirstInputFullScan,
                        ),
                    };
                    let lookup_keys = keys_to_add
                        .into_iter()
                        .filter(|key| *key != source_key)
                        .cloned()
                        .collect_vec();
                    if lookup_keys.is_empty() {
                        Some(full_scan_context).into_iter().chain(None)
                    } else {
                        let lookup_context = IndexUsageContext {
                            requested_keys: Some(lookup_keys.into_iter().collect()),
                            usage_type: IndexUsageType::DeltaJoin(DeltaJoinIndexUsageType::Lookup),
                        };
                        Some(full_scan_context)
                            .into_iter()
                            .chain(Some(lookup_context))
                    }
                }
            })
            .collect()
    }
}

/// Gathers optimization notices by traversing the MIR plan.
/// Note that some notices are collected elsewhere, e.g. inside `Transform`s or sequencing.
#[mz_ore::instrument(
    target = "optimizer",
    level = "debug",
    fields(path.segment = "index_imports")
)]
fn gather_notices(dataflow: &DataflowDesc, dataflow_metainfo: &mut DataflowMetainfo) {
    for build_desc in dataflow.objects_to_build.iter() {
        // Gather `CrossJoin` notices.
        // Some system objects also have cross joins. We don't want them to create noise.
        if !build_desc.id.is_system() {
            build_desc.plan.visit_pre(&mut |expr: &MirRelationExpr| {
                match expr {
                    MirRelationExpr::Join { implementation, .. } => {
                        match implementation {
                            JoinImplementation::Differential(_start, order) => {
                                for (_, key, _) in order {
                                    if key.is_empty() {
                                        dataflow_metainfo.push_optimizer_notice_dedup(CrossJoin)
                                    }
                                }
                            }
                            JoinImplementation::DeltaQuery(orders) => {
                                for order in orders {
                                    for (_, key, _) in order {
                                        if key.is_empty() {
                                            dataflow_metainfo.push_optimizer_notice_dedup(CrossJoin)
                                        }
                                    }
                                }
                            }
                            JoinImplementation::IndexedFilter(..) => {
                                // no cross joins
                            }
                            JoinImplementation::Unimplemented => {}
                        }
                    }
                    _ => {}
                }
            });
        }
    }
}

/// Extra information about the dataflow. This is not going to be shipped, but has to be processed
/// in other ways, e.g., showing notices to the user, or saving meta-information to the catalog.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Arbitrary)]
pub struct DataflowMetainfo<Notice = RawOptimizerNotice> {
    /// Notices that the optimizer wants to show to users.
    /// For pushing a new element, use [`Self::push_optimizer_notice_dedup`].
    pub optimizer_notices: Vec<Notice>,
    /// What kind of operation (full scan, lookup, ...) will access each index. Computed by
    /// `prune_and_annotate_dataflow_index_imports`.
    pub index_usage_types: BTreeMap<GlobalId, Vec<IndexUsageType>>,
}

impl<Notice> Default for DataflowMetainfo<Notice> {
    fn default() -> Self {
        DataflowMetainfo {
            optimizer_notices: Vec::new(),
            index_usage_types: BTreeMap::new(),
        }
    }
}

impl<Notice> DataflowMetainfo<Notice> {
    /// Create a [`UsedIndexes`] instance by resolving each `id` in the
    /// `index_ids` iterator against an entry expected to exist in the
    /// [`DataflowMetainfo::index_usage_types`].
    pub fn used_indexes<T>(&self, df_desc: &DataflowDescription<T>) -> UsedIndexes {
        UsedIndexes::new(
            df_desc
                .index_imports
                .iter()
                .map(|(id, _)| {
                    let entry = self.index_usage_types.get(id).cloned();
                    // If an entry does not exist, mark the usage type for this
                    // index as `Unknown`.
                    //
                    // This should never happen if this method is called after
                    // running `prune_and_annotate_dataflow_index_imports` on
                    // the dataflow (this happens at the end of the
                    // `optimize_dataflow` call).
                    let index_usage_type = entry.unwrap_or_else(|| vec![IndexUsageType::Unknown]);

                    (*id, index_usage_type)
                })
                .collect(),
        )
    }
}

impl DataflowMetainfo<RawOptimizerNotice> {
    /// Pushes a [`RawOptimizerNotice`] into [`Self::optimizer_notices`], but
    /// only if the exact same notice is not already present.
    pub fn push_optimizer_notice_dedup<T>(&mut self, notice: T)
    where
        T: Into<RawOptimizerNotice>,
    {
        let notice = notice.into();
        if !self.optimizer_notices.contains(&notice) {
            self.optimizer_notices.push(notice);
        }
    }
}
