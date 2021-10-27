// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Delta join execution planning and dataflow construction.
//!
//! Delta joins are a join over multiple input relations, implemented by an
//! independent dataflow path for each input. Each path is joined against the
//! other inputs using a "lookup" operator, and the path results are collected
//! and return as the output for the entire dataflow.
//!
//! This implementation strategy allows us to re-use existing arrangements, and
//! not create any new stateful operators.

#![allow(clippy::op_ref)]
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use timely::dataflow::Scope;

use dataflow_types::DataflowError;
use expr::{JoinInputMapper, MapFilterProject, MirScalarExpr};
use repr::{Diff, Row, RowArena};
use timely::progress::Antichain;

use super::super::context::{ArrangementFlavor, Context};
use crate::operator::CollectionExt;
use crate::render::context::CollectionBundle;
use crate::render::datum_vec::DatumVec;
use crate::render::join::{JoinBuildState, JoinClosure};

/// A delta query is implemented by a set of paths, one for each input.
///
/// Each delta query path responds to its input changes by repeated lookups
/// in arrangements for other join inputs. These lookups require specific
/// instructions about which expressions to use as keys. Along the way,
/// various closures are applied to filter and project as early as possible.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaJoinPlan {
    /// The set of path plans.
    ///
    /// Each path identifies its source relation, so the order is only
    /// important for determinism of dataflow construction.
    path_plans: Vec<DeltaPathPlan>,
}

/// A delta query path is implemented by a sequences of stages,
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaPathPlan {
    /// The relation whose updates seed the dataflow path.
    source_relation: usize,
    /// An initial closure to apply before any stages.
    ///
    /// Values of `None` indicate the identity closure.
    initial_closure: Option<JoinClosure>,
    /// A *sequence* of stages to apply one after the other.
    stage_plans: Vec<DeltaStagePlan>,
    /// A concluding closure to apply after the last stage.
    ///
    /// Values of `None` indicate the identity closure.
    final_closure: Option<JoinClosure>,
}

/// A delta query stage performs a stream lookup into an arrangement.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeltaStagePlan {
    /// The relation index into which we will look up.
    lookup_relation: usize,
    /// The key expressions to use for the streamed relation.
    ///
    /// While this starts as a stream of the source relation,
    /// it evolves through multiple lookups and ceases to be
    /// the same thing, hence the different name.
    stream_key: Vec<MirScalarExpr>,
    /// The key expressions to use for the lookup relation.
    lookup_key: Vec<MirScalarExpr>,
    /// The closure to apply to the concatenation of columns
    /// of the stream and lookup relations.
    closure: JoinClosure,
}

impl DeltaJoinPlan {
    /// Create a new join plan from the required arguments.
    pub fn create_from(
        equivalences: &[Vec<MirScalarExpr>],
        join_orders: &[Vec<(usize, Vec<MirScalarExpr>)>],
        input_mapper: JoinInputMapper,
        map_filter_project: &mut MapFilterProject,
    ) -> Self {
        let number_of_inputs = join_orders.len();

        // Create an empty plan, with capacity for the intended number of path plans.
        let mut join_plan = DeltaJoinPlan {
            path_plans: Vec::with_capacity(number_of_inputs),
        };

        let temporal_mfp = map_filter_project.extract_temporal();

        // Each source relation will contribute a path to the join plan.
        for source_relation in 0..number_of_inputs {
            // Construct initial join build state.
            // This state will evolves as we build the join dataflow.
            let mut join_build_state = JoinBuildState::new(
                input_mapper.global_columns(source_relation),
                &equivalences,
                &map_filter_project,
            );

            // Initial action we can take on the source relation before joining.
            let initial_closure = join_build_state.extract_closure();
            let initial_closure = if initial_closure.is_identity() {
                None
            } else {
                Some(initial_closure)
            };

            // Sequence of steps to apply.
            let mut stage_plans = Vec::with_capacity(number_of_inputs - 1);

            // We track the input relations as they are added to the join so we can figure out
            // which expressions have been bound.
            let mut bound_inputs = vec![source_relation];
            // We use the order specified by the implementation.
            let order = &join_orders[source_relation];

            for (lookup_relation, lookup_key) in order.iter() {
                // rebase the intended key to use global column identifiers.
                let lookup_key_rebased = lookup_key
                    .iter()
                    .map(|k| input_mapper.map_expr_to_global(k.clone(), *lookup_relation))
                    .collect::<Vec<_>>();

                // Expressions to use as a key for the stream of incoming updates
                // are determined by locating the elements of `lookup_key` among
                // the existing bound `columns`. If that cannot be done, the plan
                // is irrecoverably defective and we panic.
                // TODO: explicitly validate this before rendering.
                let stream_key = lookup_key_rebased
                    .iter()
                    .map(|expr| {
                        let mut bound_expr = input_mapper
                            .find_bound_expr(expr, &bound_inputs, &join_build_state.equivalences)
                            .expect("Expression in join plan is not bound at time of use");
                        // Rewrite column references to physical locations.
                        bound_expr.permute_map(&join_build_state.column_map);
                        bound_expr
                    })
                    .collect::<Vec<_>>();

                // Introduce new columns and expressions they enable. Form a new closure.
                let closure = join_build_state.add_columns(
                    input_mapper.global_columns(*lookup_relation),
                    &lookup_key_rebased,
                );

                bound_inputs.push(*lookup_relation);
                // record the stage plan as next in the path.
                stage_plans.push(DeltaStagePlan {
                    lookup_relation: *lookup_relation,
                    stream_key,
                    lookup_key: lookup_key.clone(),
                    closure,
                });
            }
            // determine a final closure, and complete the path plan.
            let final_closure = join_build_state.complete();
            let final_closure = if final_closure.is_identity() {
                None
            } else {
                Some(final_closure)
            };

            // Insert the path plan.
            join_plan.path_plans.push(DeltaPathPlan {
                source_relation,
                initial_closure,
                stage_plans,
                final_closure,
            });
        }

        // Now that `map_filter_project` has been captured in the state builder,
        // assign the remaining temporal predicates to it, for the caller's use.
        *map_filter_project = temporal_mfp;

        join_plan
    }
}

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
                                ArrangementFlavor::Local(oks, errs) => {
                                    if err_dedup.insert((lookup_idx, lookup_key)) {
                                        scope_errs.push(errs.as_collection(|k, _v| k.clone()));
                                    }
                                    Ok(oks.enter(inner))
                                }
                                ArrangementFlavor::Trace(_gid, oks, errs) => {
                                    if err_dedup.insert((lookup_idx, lookup_key)) {
                                        scope_errs.push(errs.as_collection(|k, _v| k.clone()));
                                    }
                                    Err(oks.enter(inner))
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
                    let update_stream = if let Some((_key, val)) = arrangements
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
                        if let Some(initial_closure) = initial_closure {
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
                            lookup_key,
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
                                Ok(local) => {
                                    if source_relation < lookup_relation {
                                        build_halfjoin(
                                            update_stream,
                                            local.enter_region(region),
                                            stream_key,
                                            |t1, t2| t1.le(t2),
                                            closure,
                                        )
                                    } else {
                                        build_halfjoin(
                                            update_stream,
                                            local.enter_region(region),
                                            stream_key,
                                            |t1, t2| t1.lt(t2),
                                            closure,
                                        )
                                    }
                                }
                                Err(trace) => {
                                    if source_relation < lookup_relation {
                                        build_halfjoin(
                                            update_stream,
                                            trace.enter_region(region),
                                            stream_key,
                                            |t1, t2| t1.le(t2),
                                            closure,
                                        )
                                    } else {
                                        build_halfjoin(
                                            update_stream,
                                            trace.enter_region(region),
                                            stream_key,
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
    comparison: CF,
    closure: JoinClosure,
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
            // Explicit drop to release borrow on `row` so that it can be returned.
            drop(datums_local);
            Ok((row_key, row, time))
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
        move |_key, stream_row, lookup_row, initial, time, diff1, diff2| {
            let temp_storage = RowArena::new();
            let mut datums_local = datums.borrow();
            datums_local.extend(stream_row.iter());
            datums_local.extend(lookup_row.iter());
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
    initial_closure: Option<JoinClosure>,
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
                            while let Some(_key) = cursor.get_key(batch) {
                                while let Some(val) = cursor.get_val(batch) {
                                    cursor.map_times(batch, |time, diff| {
                                        // note: only the delta path for the first relation will see
                                        // updates at start-up time
                                        if source_relation == 0 || !as_of.elements().contains(&time)
                                        {
                                            if let Some(initial_closure) = &initial_closure {
                                                let temp_storage = RowArena::new();
                                                let mut datums_local = datums.borrow_with(&val);
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
                                                ok_session.give((
                                                    val.clone(),
                                                    time.clone(),
                                                    diff.clone(),
                                                ));
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
