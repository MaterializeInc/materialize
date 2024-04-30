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

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::{BatchReader, Cursor, TraceReader};
use differential_dataflow::{AsCollection, Collection, ExchangeData, Hashable};
use mz_compute_types::plan::join::delta_join::{DeltaJoinPlan, DeltaPathPlan, DeltaStagePlan};
use mz_compute_types::plan::join::JoinClosure;
use mz_expr::MirScalarExpr;
use mz_repr::fixed_length::{FromDatumIter, ToDatumIter};
use mz_repr::{DatumVec, Diff, Row, RowArena, SharedRow};
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::{CollectionExt, StreamExt};
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Map, OkErr};
use timely::dataflow::Scope;
use timely::progress::timestamp::Refines;
use timely::progress::Antichain;

use crate::render::context::{
    ArrangementFlavor, CollectionBundle, Context, MzArrangement, MzArrangementImport, ShutdownToken,
};
use crate::render::RenderTimestamp;
use crate::typedefs::{RowRowAgent, RowRowEnter};

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
        &self,
        inputs: Vec<CollectionBundle<G>>,
        join_plan: DeltaJoinPlan,
    ) -> CollectionBundle<G> {
        // We create a new region to contain the dataflow paths for the delta join.
        let (oks, errs) = self.scope.clone().region_named("Join(Delta)", |inner| {
            // Collects error streams for the ambient scope.
            let mut inner_errs = Vec::new();

            // Our plan is to iterate through each input relation, and attempt
            // to find a plan that maximally uses existing keys (better: uses
            // existing arrangements, to which we have access).
            let mut join_results = Vec::new();

            // Bring all collection bundles into the `inner` region.
            // TODO: Prune unused collections and arrangements, for clarity.
            let inputs = inputs
                .into_iter()
                .map(|cb| cb.enter_region(inner))
                .collect::<Vec<_>>();

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

                    let bundles = inputs
                        .iter()
                        .map(|cb| cb.enter_region(region))
                        .collect::<Vec<_>>();

                    // Collects error streams for the region scope. Concats before leaving.
                    let mut region_errs = Vec::with_capacity(inputs.len());

                    // Form the initial stream of updates that will hydrate the delta path.
                    let (update_stream, err_stream) = build_update_stream(
                        &bundles[source_relation],
                        self.as_of_frontier.clone(),
                        Some(source_key),
                        source_relation,
                        initial_closure,
                    );

                    region_errs.push(err_stream);

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
                        let (oks, errs) = {
                            if source_relation < lookup_relation {
                                build_halfjoin(
                                    update_stream,
                                    stream_key,
                                    stream_thinning,
                                    &bundles[lookup_relation],
                                    lookup_key,
                                    |t1, t2| t1.le(t2),
                                    closure,
                                    self.shutdown_token.clone(),
                                )
                            } else {
                                build_halfjoin(
                                    update_stream,
                                    stream_key,
                                    stream_thinning,
                                    &bundles[lookup_relation],
                                    lookup_key,
                                    |t1, t2| t1.lt(t2),
                                    closure,
                                    self.shutdown_token.clone(),
                                )
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
                        let name = "DeltaJoinFinalization";
                        type CB<C> = ConsolidatingContainerBuilder<C>;
                        let (updates, errors) = update_stream
                            .flat_map_fallible::<CB<_>, CB<_>, _, _, _, _>(name, {
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

/// Constructs a halfjoin from an update stream and collection bundle.
fn build_halfjoin<G, CF>(
    updates: Collection<G, (Row, G::Timestamp), Diff>,
    prev_key: Vec<MirScalarExpr>,
    prev_thinning: Vec<usize>,
    bundle: &CollectionBundle<G>,
    lookup_key: Vec<MirScalarExpr>,
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
    match bundle.arrangement(&lookup_key[..]) {
        Some(ArrangementFlavor::Local(MzArrangement::RowRow(inner), errs)) => {
            let (ok, err) = build_halfjoin_trace::<_, RowRowAgent<_, _>, Row, _>(
                updates,
                inner,
                prev_key,
                prev_thinning,
                comparison,
                closure,
                shutdown_token,
            );
            (ok, err.concat(&errs.as_collection(|k, _v| k.clone())))
        }
        Some(ArrangementFlavor::Trace(_, MzArrangementImport::RowRow(inner), errs)) => {
            let (ok, err) = build_halfjoin_trace::<_, RowRowEnter<_, _, _>, Row, _>(
                updates,
                inner,
                prev_key,
                prev_thinning,
                comparison,
                closure,
                shutdown_token,
            );
            (ok, err.concat(&errs.as_collection(|k, _v| k.clone())))
        }
        None => {
            panic!("Missing promised arrangement")
        }
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
fn build_halfjoin_trace<G, Tr, K, CF>(
    updates: Collection<G, (Row, G::Timestamp), Diff>,
    trace: Arranged<G, Tr>,
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
    Tr: TraceReader<Time = G::Timestamp, Diff = Diff> + Clone + 'static,
    K: ExchangeData + Hashable + Default + FromDatumIter + ToDatumIter,
    for<'a> Tr::Key<'a>: IntoOwned<'a, Owned = K>,
    for<'a> Tr::Val<'a>: ToDatumIter,
    CF: Fn(Tr::TimeGat<'_>, &G::Timestamp) -> bool + 'static,
{
    let name = "DeltaJoinKeyPreparation";
    type CB<C> = CapacityContainerBuilder<C>;
    let (updates, errs) = updates.map_fallible::<CB<_>, CB<_>, _, _, _>(name, {
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
        let (oks, errs2) = differential_dogs3::operators::half_join::half_join_internal_unsafe(
            &updates,
            trace,
            |time, antichain| {
                antichain.insert(time.step_back());
            },
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

                let key = key.to_datum_iter();
                let stream_row = stream_row.to_datum_iter();
                let lookup_row = lookup_row.to_datum_iter();

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
        let oks = differential_dogs3::operators::half_join::half_join_internal_unsafe(
            &updates,
            trace,
            |time, antichain| {
                antichain.insert(time.step_back());
            },
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

                let key = key.to_datum_iter();
                let stream_row = stream_row.to_datum_iter();
                let lookup_row = lookup_row.to_datum_iter();

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

/// Builds and initial update stream from a collection bundle.
fn build_update_stream<G>(
    bundle: &CollectionBundle<G>,
    as_of: Antichain<mz_repr::Timestamp>,
    source_key: Option<Vec<MirScalarExpr>>,
    source_relation: usize,
    initial_closure: JoinClosure,
) -> (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>)
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
{
    if let Some(source_key) = source_key {
        match bundle.arrangement(&source_key[..]) {
            Some(ArrangementFlavor::Local(MzArrangement::RowRow(inner), errs)) => {
                let (ok, err) = build_update_stream_trace::<_, RowRowAgent<_, _>>(
                    inner,
                    as_of,
                    source_relation,
                    initial_closure,
                );
                (ok, err.concat(&errs.as_collection(|k, _v| k.clone())))
            }
            Some(ArrangementFlavor::Trace(_, MzArrangementImport::RowRow(inner), errs)) => {
                let (ok, err) = build_update_stream_trace::<_, RowRowEnter<_, _, _>>(
                    inner,
                    as_of,
                    source_relation,
                    initial_closure,
                );
                (ok, err.concat(&errs.as_collection(|k, _v| k.clone())))
            }
            None => {
                panic!("Missing promised arrangement")
            }
        }
    } else {
        // Build an update stream based on a `Collection` input.
        unimplemented!();
    }
}

/// Builds the beginning of the update stream of a delta path.
///
/// At start-up time only the delta path for the first relation sees updates, since any updates fed to the
/// other delta paths would be discarded anyway due to the tie-breaking logic that avoids double-counting
/// updates happening at the same time on different relations.
fn build_update_stream_trace<G, Tr>(
    trace: Arranged<G, Tr>,
    as_of: Antichain<mz_repr::Timestamp>,
    source_relation: usize,
    initial_closure: JoinClosure,
) -> (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>)
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
    for<'a, 'b> &'a G::Timestamp: PartialEq<Tr::TimeGat<'b>>,
    Tr: for<'a> TraceReader<Time = G::Timestamp, Diff = Diff> + Clone + 'static,
    for<'a> Tr::Key<'a>: ToDatumIter,
    for<'a> Tr::Val<'a>: ToDatumIter,
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
                                            || inner_as_of.elements().iter().all(|e| e != time)
                                        {
                                            let time = time.into_owned();
                                            let temp_storage = RowArena::new();

                                            let key = key.to_datum_iter();
                                            let val = val.to_datum_iter();

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
                                                        time,
                                                        diff.into_owned(),
                                                    )),
                                                    Some(Err(err)) => err_session.give((
                                                        err,
                                                        time,
                                                        diff.into_owned(),
                                                    )),
                                                    None => {}
                                                }
                                            } else {
                                                let row = {
                                                    row_builder.packer().extend(&*datums_local);
                                                    row_builder.clone()
                                                };
                                                ok_session.give((row, time, diff.into_owned()));
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
