// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Delta join execution dataflow construction.
//!
//! Consult [DeltaJoinPlan] documentation for details.

#![allow(clippy::op_ref)]
use std::collections::HashSet;
use timely::dataflow::Scope;

use dataflow_types::plan::join::delta_join::{DeltaJoinPlan, DeltaPathPlan, DeltaStagePlan};
use dataflow_types::plan::join::JoinClosure;
use dataflow_types::plan::Permutation;
use dataflow_types::DataflowError;

use expr::MirScalarExpr;
use repr::{Diff, Row, RowArena};
use timely::progress::Antichain;

use super::super::context::{ArrangementFlavor, Context};
use crate::operator::CollectionExt;
use crate::render::context::CollectionBundle;
use repr::DatumVec;

impl<G> Context<G, Row, repr::Timestamp>
where
    G: Scope<Timestamp = repr::Timestamp>,
{
    /// Renders `MirRelationExpr:Join` using dogs^3 delta query dataflows.
    ///
    /// The join is followed by the application of `map_filter_project`, whose
    /// implementation will be pushed in to the join pipeline if at all possible.
    pub fn render_delta_join(
        &mut self,
        inputs: Vec<CollectionBundle<G, Row, G::Timestamp>>,
        join_plan: DeltaJoinPlan,
        scope: &mut G,
    ) -> CollectionBundle<G, Row, G::Timestamp> {
        // Collects error streams for the ambient scope.
        let mut scope_errs = Vec::new();

        // Deduplicate the error streams of multiply used arrangements.
        let mut err_dedup = HashSet::new();

        // We create a new region to contain the dataflow paths for the delta join.
        let (oks, errs) = scope.clone().region_named("delta query", |inner| {
            // Our plan is to iterate through each input relation, and attempt
            // to find a plan that maximally uses existing keys (better: uses
            // existing arrangements, to which we have access).
            let mut join_results = Vec::new();

            // First let's prepare the input arrangements we will need.
            // This reduces redundant imports, and simplifies the dataflow structure.
            // As the arrangements are all shared, it should not dramatically improve
            // the efficiency, but the dataflow simplification is worth doing.
            let mut arrangements = std::collections::BTreeMap::new();
            for path_plan in join_plan.path_plans.iter() {
                for stage_plan in path_plan.stage_plans.iter() {
                    let lookup_idx = stage_plan.lookup_relation;
                    let lookup_key = stage_plan.lookup_key.clone();
                    let lookup_permutation = stage_plan.lookup_permutation.clone();
                    arrangements
                        .entry((lookup_idx, lookup_key.clone()))
                        .or_insert_with(|| {
                            match inputs[lookup_idx]
                                .arrangement(&lookup_key)
                                .unwrap_or_else(|| {
                                    panic!(
                                        "Arrangement alarmingly absent!: {}, {:?}",
                                        lookup_idx, lookup_key,
                                    )
                                }) {
                                ArrangementFlavor::Local(oks, errs, _) => {
                                    if err_dedup.insert((lookup_idx, lookup_key)) {
                                        scope_errs.push(errs.as_collection(|k, _v| k.clone()));
                                    }
                                    (lookup_permutation, Ok(oks.enter(inner)))
                                }
                                ArrangementFlavor::Trace(_gid, oks, errs, _) => {
                                    if err_dedup.insert((lookup_idx, lookup_key)) {
                                        scope_errs.push(errs.as_collection(|k, _v| k.clone()));
                                    }
                                    (lookup_permutation, Err(oks.enter(inner)))
                                }
                            }
                        });
                }
            }

            // Collects error streams for the inner scope. Concats before leaving.
            let mut inner_errs = Vec::with_capacity(inputs.len());
            for path_plan in join_plan.path_plans.into_iter() {
                // Deconstruct the stages of the path plan.
                let DeltaPathPlan {
                    source_relation,
                    initial_closure,
                    stage_plans,
                    final_closure,
                } = path_plan;

                // This collection determines changes that result from updates inbound
                // from `inputs[relation]` and reflects all strictly prior updates and
                // concurrent updates from relations prior to `relation`.
                let name = format!("delta path {}", source_relation);
                let path_results = inner.clone().region_named(&name, |region| {
                    // The plan is to move through each relation, starting from `relation` and in the order
                    // indicated in `orders[relation]`. At each moment, we will have the columns from the
                    // subset of relations encountered so far, and we will have applied as much as we can
                    // of the filters in `equivalences` and the logic in `map_filter_project`, based on the
                    // available columns.
                    //
                    // As we go, we will track the physical locations of each intended output column, as well
                    // as the locations of intermediate results from partial application of `map_filter_project`.
                    //
                    // Just before we apply the `lookup` function to perform a join, we will first use our
                    // available information to determine the filtering and logic that we can apply, and
                    // introduce that in to the `lookup` logic to cause it to happen in that operator.

                    // Collects error streams for the region scope. Concats before leaving.
                    let mut region_errs = Vec::with_capacity(inputs.len());

                    use differential_dataflow::AsCollection;
                    use timely::dataflow::operators::Map;

                    // Ensure this input is rendered, and extract its update stream.
                    let update_stream = if let Some((_key, (permutation, val))) = arrangements
                        .iter()
                        .find(|(key, _val)| key.0 == source_relation)
                    {
                        let as_of = self.as_of_frontier.clone();
                        match val {
                            Ok(local) => {
                                let arranged = local.enter(region);
                                let (update_stream, err_stream) = build_update_stream(
                                    arranged,
                                    as_of,
                                    source_relation,
                                    initial_closure,
                                    permutation.clone(),
                                );
                                region_errs.push(err_stream);
                                update_stream
                            }
                            Err(trace) => {
                                let arranged = trace.enter(region);
                                let (update_stream, err_stream) = build_update_stream(
                                    arranged,
                                    as_of,
                                    source_relation,
                                    initial_closure,
                                    permutation.clone(),
                                );
                                region_errs.push(err_stream);
                                update_stream
                            }
                        }
                    } else {
                        // If this branch is reached, it means that the optimizer, specifically the
                        // transform `JoinImplementation`, has made a mistake and the plan may be
                        // suboptimal, but it is still possible to render the plan.
                        let mut update_stream = inputs[source_relation]
                            .as_collection()
                            .0
                            .enter(inner)
                            .enter_region(region);

                        // Apply what `closure` we are able to, and record any errors.
                        if !initial_closure.is_identity() {
                            let (stream, errs) =
                                update_stream.flat_map_fallible("DeltaJoinInitialization", {
                                    let mut row_builder = Row::default();
                                    let mut datums = DatumVec::new();
                                    move |row| {
                                        let temp_storage = RowArena::new();
                                        let mut datums_local = datums.borrow_with(&row);
                                        // TODO(mcsherry): re-use `row` allocation.
                                        initial_closure
                                            .apply(
                                                &mut datums_local,
                                                &temp_storage,
                                                &mut row_builder,
                                            )
                                            .transpose()
                                    }
                                });
                            update_stream = stream;
                            region_errs.push(errs.map(DataflowError::from));
                        }

                        update_stream
                    };

                    // Promote `time` to a datum element.
                    //
                    // The `half_join` operator manipulates as "data" a pair `(data, time)`,
                    // while tracking the initial time `init_time` separately and without
                    // modification. The initial value for both times is the initial time.
                    let mut update_stream = update_stream
                        .inner
                        .map(|(v, t, d)| ((v, t.clone()), t, d))
                        .as_collection();

                    // Repeatedly update `update_stream` to reflect joins with more and more
                    // other relations, in the specified order.
                    for stage_plan in stage_plans.into_iter() {
                        let DeltaStagePlan {
                            lookup_relation,
                            stream_key,
                            stream_thinning,
                            lookup_key,
                            lookup_permutation: _,
                            join_permutation,
                            closure,
                        } = stage_plan;

                        // We require different logic based on the relative order of the two inputs.
                        // If the `source` relation precedes the `lookup` relation, we present all
                        // updates with less or equal `time`, and otherwise we present only updates
                        // with strictly less `time`.
                        //
                        // We need to write the logic twice, as there are two types of arrangement
                        // we might have: either dataflow-local or an imported trace.
                        let (oks, errs) =
                            match arrangements.get(&(lookup_relation, lookup_key)).unwrap() {
                                (_, Ok(local)) => {
                                    if source_relation < lookup_relation {
                                        build_halfjoin(
                                            update_stream,
                                            local.enter_region(region),
                                            stream_key,
                                            stream_thinning,
                                            join_permutation,
                                            |t1, t2| t1.le(t2),
                                            closure,
                                        )
                                    } else {
                                        build_halfjoin(
                                            update_stream,
                                            local.enter_region(region),
                                            stream_key,
                                            stream_thinning,
                                            join_permutation,
                                            |t1, t2| t1.lt(t2),
                                            closure,
                                        )
                                    }
                                }
                                (_, Err(trace)) => {
                                    if source_relation < lookup_relation {
                                        build_halfjoin(
                                            update_stream,
                                            trace.enter_region(region),
                                            stream_key,
                                            stream_thinning,
                                            join_permutation,
                                            |t1, t2| t1.le(t2),
                                            closure,
                                        )
                                    } else {
                                        build_halfjoin(
                                            update_stream,
                                            trace.enter_region(region),
                                            stream_key,
                                            stream_thinning,
                                            join_permutation,
                                            |t1, t2| t1.lt(t2),
                                            closure,
                                        )
                                    }
                                }
                            };
                        update_stream = oks;
                        region_errs.push(errs);
                    }

                    // Delay updates as appropriate.
                    //
                    // The `half_join` operator maintains a time that we now discard (the `_`),
                    // and replace with the `time` that is maintained with the data. The former
                    // exists to pin a consistent total order on updates throughout the process,
                    // while allowing `time` to vary upwards as a result of actions on time.
                    let mut update_stream = update_stream
                        .inner
                        .map(|((row, time), _, diff)| (row, time, diff))
                        .as_collection();

                    // We have completed the join building, but may have work remaining.
                    // For example, we may have expressions not pushed down (e.g. literals)
                    // and projections that could not be applied (e.g. column repetition).
                    if let Some(final_closure) = final_closure {
                        let (updates, errors) =
                            update_stream.flat_map_fallible("DeltaJoinFinalization", {
                                // Reuseable allocation for unpacking.
                                let mut datums = DatumVec::new();
                                let mut row_builder = Row::default();
                                move |row| {
                                    let temp_storage = RowArena::new();
                                    let mut datums_local = datums.borrow_with(&row);
                                    // TODO(mcsherry): re-use `row` allocation.
                                    final_closure
                                        .apply(&mut datums_local, &temp_storage, &mut row_builder)
                                        .map_err(DataflowError::from)
                                        .transpose()
                                }
                            });

                        update_stream = updates;
                        region_errs.push(errors);
                    }

                    inner_errs.push(
                        differential_dataflow::collection::concatenate(region, region_errs).leave(),
                    );
                    update_stream.leave()
                });

                join_results.push(path_results);
            }

            scope_errs
                .push(differential_dataflow::collection::concatenate(inner, inner_errs).leave());

            // Concatenate the results of each delta query as the accumulated results.
            (
                differential_dataflow::collection::concatenate(inner, join_results).leave(),
                differential_dataflow::collection::concatenate(scope, scope_errs),
            )
        });
        CollectionBundle::from_collections(oks, errs)
    }
}

use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;

/// Constructs a `half_join` from supplied arguments.
///
/// This method exists to factor common logic from four code paths that are generic over the type of trace.
/// The `comparison` function should either be `le` or `lt` depending on which relation comes first in the
/// total order on relations (in order to break ties consistently).
///
/// The input and output streams are of pairs `(data, time)` where the `time` component can be greater than
/// the time of the update. This operator may manipulate `time` as part of this pair, but will not manipulate
/// the time of the update. This is crucial for correctness, as the total order on times of updates is used
/// to ensure that any two updates are matched at most once.
fn build_halfjoin<G, Tr, CF>(
    updates: Collection<G, (Row, G::Timestamp)>,
    trace: Arranged<G, Tr>,
    prev_key: Vec<MirScalarExpr>,
    prev_thinning: Vec<usize>,
    permutation: Permutation,
    comparison: CF,
    mut closure: JoinClosure,
) -> (
    Collection<G, (Row, G::Timestamp)>,
    Collection<G, DataflowError>,
)
where
    G: Scope<Timestamp = repr::Timestamp>,
    Tr: TraceReader<Time = G::Timestamp, Key = Row, Val = Row, R = Diff> + Clone + 'static,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    CF: Fn(&G::Timestamp, &G::Timestamp) -> bool + 'static,
{
    closure.permute(&permutation);
    let (updates, errs) = updates.map_fallible("DeltaJoinKeyPreparation", {
        // Reuseable allocation for unpacking.
        let mut datums = DatumVec::new();
        let mut row_packer = Row::default();
        move |(row, time)| {
            let temp_storage = RowArena::new();
            let datums_local = datums.borrow_with(&row);
            row_packer.clear();
            row_packer.try_extend(
                prev_key
                    .iter()
                    .map(|e| e.eval(&datums_local, &temp_storage)),
            )?;
            let row_key = row_packer.finish_and_reuse();
            row_packer.extend(prev_thinning.iter().map(|e| datums_local[*e]));
            let row_value = row_packer.finish_and_reuse();
            // Explicit drop to release borrow on `row` so that it can be returned.
            drop(datums_local);
            Ok((row_key, row_value, time))
        }
    });

    use differential_dataflow::AsCollection;
    use timely::dataflow::operators::OkErr;

    let mut datums = DatumVec::new();
    let mut row_builder = Row::default();

    let (oks, errs2) = dogsdogsdogs::operators::half_join::half_join_internal_unsafe(
        &updates,
        trace,
        |time| time.saturating_sub(1),
        comparison,
        // TODO(mcsherry): investigate/establish trade-offs here; time based had problems,
        // in that we seem to yield too much and do too little work when we do.
        |_timer, count| count > 1_000_000,
        // TODO(mcsherry): consider `RefOrMut` in `half_join` interface to allow re-use.
        move |key, stream_row, lookup_row, initial, time, diff1, diff2| {
            let temp_storage = RowArena::new();
            let mut datums_local = datums.borrow_with_many(&[key, stream_row, lookup_row]);
            let row = closure.apply(&mut datums_local, &temp_storage, &mut row_builder);
            let diff = diff1.clone() * diff2.clone();
            let dout = (row, time.clone());
            Some((dout, initial.clone(), diff))
        },
    )
    .inner
    .ok_err(|(data_time, init_time, diff)| {
        // TODO(mcsherry): consider `ok_err()` for `Collection`.
        match data_time {
            (Ok(data), time) => Ok((data.map(|data| (data, time)), init_time, diff)),
            (Err(err), _time) => Err((DataflowError::from(err), init_time, diff)),
        }
    });

    (
        oks.as_collection().flat_map(|x| x),
        errs.concat(&errs2.as_collection()),
    )
}

/// Builds the beginning of the update stream of a delta path.
///
/// At start-up time only the delta path for the first relation sees updates, since any updates fed to the
/// other delta paths would be discarded anyway due to the tie-breaking logic that avoids double-counting
/// updates happening at the same time on different relations.
fn build_update_stream<G, Tr>(
    trace: Arranged<G, Tr>,
    as_of: Antichain<G::Timestamp>,
    source_relation: usize,
    mut initial_closure: JoinClosure,
    permutation: Permutation,
) -> (Collection<G, Row>, Collection<G, DataflowError>)
where
    G: Scope<Timestamp = repr::Timestamp>,
    Tr: TraceReader<Time = G::Timestamp, Key = Row, Val = Row, R = Diff> + Clone + 'static,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
{
    use crate::operator::StreamExt;
    use differential_dataflow::AsCollection;
    use timely::dataflow::channels::pact::Pipeline;

    let mut row_builder = Row::default();
    initial_closure.permute(&permutation);
    let (ok_stream, err_stream) =
        trace
            .stream
            .unary_fallible(Pipeline, "UpdateStream", move |_, _| {
                let mut datums = DatumVec::new();
                Box::new(move |input, ok_output, err_output| {
                    input.for_each(|time, data| {
                        let mut ok_session = ok_output.session(&time);
                        let mut err_session = err_output.session(&time);
                        for wrapper in data.iter() {
                            let batch = &wrapper;
                            let mut cursor = batch.cursor();
                            while let Some(key) = cursor.get_key(batch) {
                                while let Some(val) = cursor.get_val(batch) {
                                    cursor.map_times(batch, |time, diff| {
                                        // note: only the delta path for the first relation will see
                                        // updates at start-up time
                                        if source_relation == 0 || !as_of.elements().contains(&time)
                                        {
                                            let temp_storage = RowArena::new();
                                            let mut datums_local =
                                                datums.borrow_with_many(&[key, val]);
                                            if !initial_closure.is_identity() {
                                                match initial_closure
                                                    .apply(
                                                        &mut datums_local,
                                                        &temp_storage,
                                                        &mut row_builder,
                                                    )
                                                    .transpose()
                                                {
                                                    Some(Ok(row)) => ok_session.give((
                                                        row,
                                                        time.clone(),
                                                        diff.clone(),
                                                    )),
                                                    Some(Err(err)) => err_session.give((
                                                        err,
                                                        time.clone(),
                                                        diff.clone(),
                                                    )),
                                                    None => {}
                                                }
                                            } else {
                                                let row = {
                                                    row_builder.clear();
                                                    row_builder.extend(&*datums_local);
                                                    row_builder.finish_and_reuse()
                                                };
                                                ok_session.give((row, time.clone(), diff.clone()));
                                            }
                                        }
                                    });
                                    cursor.step_val(batch);
                                }
                                cursor.step_key(batch);
                            }
                        }
                    });
                })
            });

    (
        ok_stream.as_collection(),
        err_stream.as_collection().map(DataflowError::from),
    )
}
