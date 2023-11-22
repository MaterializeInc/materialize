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

use std::collections::{BTreeMap, BTreeSet};

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Collection, ExchangeData, Hashable};
use mz_compute_types::plan::join::delta_join::{DeltaJoinPlan, DeltaPathPlan, DeltaStagePlan};
use mz_compute_types::plan::join::JoinClosure;
use mz_expr::MirScalarExpr;
use mz_repr::fixed_length::{FromRowByTypes, IntoRowByTypes};
use mz_repr::{ColumnType, DatumVec, Diff, Row, RowArena, SharedRow};
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::{CollectionExt, StreamExt};
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Map, OkErr};
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp};

use crate::render::context::{
    ArrangementFlavor, CollectionBundle, Context, ShutdownToken, SpecializedArrangement,
    SpecializedArrangementImport,
};
use crate::render::RenderTimestamp;

impl<G> Context<G>
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
{
    /// Renders `MirRelationExpr:Join` using dogs^3 delta query dataflows.
    ///
    /// The join is followed by the application of `map_filter_project`, whose
    /// implementation will be pushed in to the join pipeline if at all possible.
    pub fn render_delta_join(
        &mut self,
        inputs: Vec<CollectionBundle<G>>,
        join_plan: DeltaJoinPlan,
    ) -> CollectionBundle<G> {
        // We create a new region to contain the dataflow paths for the delta join.
        let (oks, errs) = self.scope.clone().region_named("Join(Delta)", |inner| {
            // Collects error streams for the ambient scope.
            let mut inner_errs = Vec::new();

            // Deduplicate the error streams of multiply used arrangements.
            let mut err_dedup = BTreeSet::new();

            // Our plan is to iterate through each input relation, and attempt
            // to find a plan that maximally uses existing keys (better: uses
            // existing arrangements, to which we have access).
            let mut join_results = Vec::new();

            // First let's prepare the input arrangements we will need.
            // This reduces redundant imports, and simplifies the dataflow structure.
            // As the arrangements are all shared, it should not dramatically improve
            // the efficiency, but the dataflow simplification is worth doing.
            let mut arrangements = BTreeMap::new();
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
                                        inner_errs.push(
                                            errs.enter_region(inner)
                                                .as_collection(|k, _v| k.clone()),
                                        );
                                    }
                                    Ok(oks.enter_region(inner))
                                }
                                ArrangementFlavor::Trace(_gid, oks, errs) => {
                                    if err_dedup.insert((lookup_idx, lookup_key)) {
                                        inner_errs.push(
                                            errs.enter_region(inner)
                                                .as_collection(|k, _v| k.clone()),
                                        );
                                    }
                                    Err(oks.enter_region(inner))
                                }
                            }
                        });
                }
            }

            for path_plan in join_plan.path_plans {
                // Deconstruct the stages of the path plan.
                let DeltaPathPlan {
                    source_relation,
                    initial_closure,
                    stage_plans,
                    final_closure,
                    source_key,
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

                    // Ensure this input is rendered, and extract its update stream.
                    let val = arrangements
                        .get(&(source_relation, source_key))
                        .expect("Arrangement promised by the planner is absent!");
                    let as_of = self.as_of_frontier.clone();
                    let update_stream = match val {
                        Ok(local) => {
                            let arranged = local.enter_region(region);
                            let (update_stream, err_stream) = dispatch_build_update_stream_local(
                                arranged,
                                as_of,
                                source_relation,
                                initial_closure,
                            );
                            region_errs.push(err_stream);
                            update_stream
                        }
                        Err(trace) => {
                            let arranged = trace.enter_region(region);
                            let (update_stream, err_stream) = dispatch_build_update_stream_trace(
                                arranged,
                                as_of,
                                source_relation,
                                initial_closure,
                            );
                            region_errs.push(err_stream);
                            update_stream
                        }
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
                    for stage_plan in stage_plans {
                        let DeltaStagePlan {
                            lookup_relation,
                            stream_key,
                            stream_thinning,
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
                                        dispatch_build_halfjoin_local(
                                            update_stream,
                                            local.enter_region(region),
                                            stream_key,
                                            stream_thinning,
                                            |t1, t2| t1.le(t2),
                                            closure,
                                            self.shutdown_token.clone(),
                                        )
                                    } else {
                                        dispatch_build_halfjoin_local(
                                            update_stream,
                                            local.enter_region(region),
                                            stream_key,
                                            stream_thinning,
                                            |t1, t2| t1.lt(t2),
                                            closure,
                                            self.shutdown_token.clone(),
                                        )
                                    }
                                }
                                Err(trace) => {
                                    if source_relation < lookup_relation {
                                        dispatch_build_halfjoin_trace(
                                            update_stream,
                                            trace.enter_region(region),
                                            stream_key,
                                            stream_thinning,
                                            |t1, t2| t1.le(t2),
                                            closure,
                                            self.shutdown_token.clone(),
                                        )
                                    } else {
                                        dispatch_build_halfjoin_trace(
                                            update_stream,
                                            trace.enter_region(region),
                                            stream_key,
                                            stream_thinning,
                                            |t1, t2| t1.lt(t2),
                                            closure,
                                            self.shutdown_token.clone(),
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
                                move |row| {
                                    let binding = SharedRow::get();
                                    let mut row_builder = binding.borrow_mut();
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
                        differential_dataflow::collection::concatenate(region, region_errs)
                            .leave_region(),
                    );
                    update_stream.leave_region()
                });

                join_results.push(path_results);
            }

            // Concatenate the results of each delta query as the accumulated results.
            (
                differential_dataflow::collection::concatenate(inner, join_results).leave_region(),
                differential_dataflow::collection::concatenate(inner, inner_errs).leave_region(),
            )
        });
        CollectionBundle::from_collections(oks, errs)
    }
}

/// Dispatches half-join construction according to arrangement type specialization.
fn dispatch_build_halfjoin_local<G, CF>(
    updates: Collection<G, (Row, G::Timestamp), Diff>,
    trace: SpecializedArrangement<G>,
    prev_key: Vec<MirScalarExpr>,
    prev_thinning: Vec<usize>,
    comparison: CF,
    closure: JoinClosure,
    shutdown_token: ShutdownToken,
) -> (
    Collection<G, (Row, G::Timestamp), Diff>,
    Collection<G, DataflowError, Diff>,
)
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
    CF: Fn(&G::Timestamp, &G::Timestamp) -> bool + 'static,
{
    match trace {
        SpecializedArrangement::RowUnit(inner) => build_halfjoin(
            updates,
            inner,
            None,
            Some(vec![]),
            prev_key,
            prev_thinning,
            comparison,
            closure,
            shutdown_token,
        ),
        SpecializedArrangement::RowRow(inner) => build_halfjoin(
            updates,
            inner,
            None,
            None,
            prev_key,
            prev_thinning,
            comparison,
            closure,
            shutdown_token,
        ),
    }
}

/// Dispatches half-join construction according to trace type specialization.
fn dispatch_build_halfjoin_trace<G, T, CF>(
    updates: Collection<G, (Row, G::Timestamp), Diff>,
    trace: SpecializedArrangementImport<G, T>,
    prev_key: Vec<MirScalarExpr>,
    prev_thinning: Vec<usize>,
    comparison: CF,
    closure: JoinClosure,
    shutdown_token: ShutdownToken,
) -> (
    Collection<G, (Row, G::Timestamp), Diff>,
    Collection<G, DataflowError, Diff>,
)
where
    G: Scope,
    T: Timestamp + Lattice + Columnation,
    G::Timestamp: Lattice + crate::render::RenderTimestamp + Refines<T> + Columnation,
    CF: Fn(&G::Timestamp, &G::Timestamp) -> bool + 'static,
{
    match trace {
        SpecializedArrangementImport::RowUnit(inner) => build_halfjoin(
            updates,
            inner,
            None,
            Some(vec![]),
            prev_key,
            prev_thinning,
            comparison,
            closure,
            shutdown_token,
        ),
        SpecializedArrangementImport::RowRow(inner) => build_halfjoin(
            updates,
            inner,
            None,
            None,
            prev_key,
            prev_thinning,
            comparison,
            closure,
            shutdown_token,
        ),
    }
}

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
fn build_halfjoin<G, Tr, CF, K, V>(
    updates: Collection<G, (Row, G::Timestamp), Diff>,
    trace: Arranged<G, Tr>,
    trace_key_types: Option<Vec<ColumnType>>,
    trace_val_types: Option<Vec<ColumnType>>,
    prev_key: Vec<MirScalarExpr>,
    prev_thinning: Vec<usize>,
    comparison: CF,
    closure: JoinClosure,
    shutdown_token: ShutdownToken,
) -> (
    Collection<G, (Row, G::Timestamp), Diff>,
    Collection<G, DataflowError, Diff>,
)
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
    Tr: TraceReader<Time = G::Timestamp, Key = K, Val = V, R = Diff> + Clone + 'static,
    K: ExchangeData + FromRowByTypes + Hashable + IntoRowByTypes,
    V: ExchangeData + IntoRowByTypes,
    CF: Fn(&G::Timestamp, &G::Timestamp) -> bool + 'static,
{
    let updates_key_types = trace_key_types.clone();
    let (updates, errs) = updates.map_fallible("DeltaJoinKeyPreparation", {
        // Reuseable allocation for unpacking.
        let mut datums = DatumVec::new();
        let mut key_buf = K::default();
        move |(row, time)| {
            let temp_storage = RowArena::new();
            let datums_local = datums.borrow_with(&row);
            let key = key_buf.try_from_datum_iter(
                prev_key
                    .iter()
                    .map(|e| e.eval(&datums_local, &temp_storage)),
                updates_key_types.as_deref(),
            )?;
            let binding = SharedRow::get();
            let mut row_builder = binding.borrow_mut();
            row_builder
                .packer()
                .extend(prev_thinning.iter().map(|&c| datums_local[c]));
            let row_value = row_builder.clone();

            Ok((key, row_value, time))
        }
    });

    let mut datums = DatumVec::new();

    if closure.could_error() {
        let (oks, errs2) = dogsdogsdogs::operators::half_join::half_join_internal_unsafe(
            &updates,
            trace,
            |time| time.step_back(),
            comparison,
            // TODO(mcsherry): investigate/establish trade-offs here; time based had problems,
            // in that we seem to yield too much and do too little work when we do.
            |_timer, count| count > 1_000_000,
            // TODO(mcsherry): consider `RefOrMut` in `half_join` interface to allow re-use.
            move |key, stream_row, lookup_row, initial, time, diff1, diff2| {
                // Check the shutdown token to avoid doing unnecessary work when the dataflow is
                // shutting down.
                shutdown_token.probe()?;

                let binding = SharedRow::get();
                let mut row_builder = binding.borrow_mut();
                let temp_storage = RowArena::new();

                let key = key.into_datum_iter(trace_key_types.as_deref());
                let stream_row = stream_row.into_datum_iter(None);
                let lookup_row = lookup_row.into_datum_iter(trace_val_types.as_deref());

                let mut datums_local = datums.borrow();
                datums_local.extend(key);
                datums_local.extend(stream_row);
                datums_local.extend(lookup_row);

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
    } else {
        let oks = dogsdogsdogs::operators::half_join::half_join_internal_unsafe(
            &updates,
            trace,
            |time| time.step_back(),
            comparison,
            // TODO(mcsherry): investigate/establish trade-offs here; time based had problems,
            // in that we seem to yield too much and do too little work when we do.
            |_timer, count| count > 1_000_000,
            // TODO(mcsherry): consider `RefOrMut` in `half_join` interface to allow re-use.
            move |key, stream_row, lookup_row, initial, time, diff1, diff2| {
                // Check the shutdown token to avoid doing unnecessary work when the dataflow is
                // shutting down.
                shutdown_token.probe()?;

                let binding = SharedRow::get();
                let mut row_builder = binding.borrow_mut();
                let temp_storage = RowArena::new();

                let key = key.into_datum_iter(trace_key_types.as_deref());
                let stream_row = stream_row.into_datum_iter(None);
                let lookup_row = lookup_row.into_datum_iter(trace_val_types.as_deref());

                let mut datums_local = datums.borrow();
                datums_local.extend(key);
                datums_local.extend(stream_row);
                datums_local.extend(lookup_row);

                let row = closure
                    .apply(&mut datums_local, &temp_storage, &mut row_builder)
                    .expect("Closure claimed to never errer");
                let diff = diff1.clone() * diff2.clone();
                row.map(|r| ((r, time.clone()), initial.clone(), diff))
            },
        );

        (oks, errs)
    }
}

/// Dispatches building of a delta path update stream by to arrangement type specialization.
fn dispatch_build_update_stream_local<G>(
    trace: SpecializedArrangement<G>,
    as_of: Antichain<mz_repr::Timestamp>,
    source_relation: usize,
    initial_closure: JoinClosure,
) -> (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>)
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
{
    match trace {
        SpecializedArrangement::RowUnit(inner) => build_update_stream(
            inner,
            None,
            Some(vec![]),
            as_of,
            source_relation,
            initial_closure,
        ),
        SpecializedArrangement::RowRow(inner) => {
            build_update_stream(inner, None, None, as_of, source_relation, initial_closure)
        }
    }
}

/// Dispatches building of a delta path update stream by to trace type specialization.
fn dispatch_build_update_stream_trace<G, T>(
    trace: SpecializedArrangementImport<G, T>,
    as_of: Antichain<mz_repr::Timestamp>,
    source_relation: usize,
    initial_closure: JoinClosure,
) -> (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>)
where
    G: Scope,
    T: Timestamp + Lattice + Columnation,
    G::Timestamp: Lattice + crate::render::RenderTimestamp + Refines<T> + Columnation,
{
    match trace {
        SpecializedArrangementImport::RowUnit(inner) => build_update_stream(
            inner,
            None,
            Some(vec![]),
            as_of,
            source_relation,
            initial_closure,
        ),
        SpecializedArrangementImport::RowRow(inner) => {
            build_update_stream(inner, None, None, as_of, source_relation, initial_closure)
        }
    }
}

/// Builds the beginning of the update stream of a delta path.
///
/// At start-up time only the delta path for the first relation sees updates, since any updates fed to the
/// other delta paths would be discarded anyway due to the tie-breaking logic that avoids double-counting
/// updates happening at the same time on different relations.
fn build_update_stream<G, Tr, K, V>(
    trace: Arranged<G, Tr>,
    trace_key_types: Option<Vec<ColumnType>>,
    trace_val_types: Option<Vec<ColumnType>>,
    as_of: Antichain<mz_repr::Timestamp>,
    source_relation: usize,
    initial_closure: JoinClosure,
) -> (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>)
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
    Tr: TraceReader<Time = G::Timestamp, Key = K, Val = V, R = Diff> + Clone + 'static,
    K: Columnation + ExchangeData + FromRowByTypes + Hashable + IntoRowByTypes,
    V: Columnation + ExchangeData + IntoRowByTypes,
{
    let mut inner_as_of = Antichain::new();
    for event_time in as_of.elements().iter() {
        inner_as_of.insert(<G::Timestamp>::to_inner(event_time.clone()));
    }

    let (ok_stream, err_stream) =
        trace
            .stream
            .unary_fallible(Pipeline, "UpdateStream", move |_, _| {
                let mut datums = DatumVec::new();
                Box::new(move |input, ok_output, err_output| {
                    input.for_each(|time, data| {
                        let binding = SharedRow::get();
                        let mut row_builder = binding.borrow_mut();
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
                                        if source_relation == 0
                                            || !inner_as_of.elements().contains(time)
                                        {
                                            let temp_storage = RowArena::new();

                                            let key =
                                                key.into_datum_iter(trace_key_types.as_deref());
                                            let val =
                                                val.into_datum_iter(trace_val_types.as_deref());

                                            let mut datums_local = datums.borrow();
                                            datums_local.extend(key);
                                            datums_local.extend(val);

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
                                                    row_builder.packer().extend(&*datums_local);
                                                    row_builder.clone()
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
