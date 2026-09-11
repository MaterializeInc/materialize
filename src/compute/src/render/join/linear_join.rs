// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rendering of linear join plans.
//!
//! Consult [LinearJoinPlan] documentation for details.

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::trace::cursor::{BatchCursor, BatchKey, BatchVal};
use differential_dataflow::trace::implementations::merge_batcher::MergeBatcher;
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use differential_dataflow::{AsCollection, Data, VecCollection};
use mz_compute_types::plan::join::JoinClosure;
use mz_compute_types::plan::join::linear_join::{LinearJoinPlan, LinearStagePlan};
use mz_expr::Eval;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{DatumVec, Diff, Row, RowArena, SharedRow};
use mz_row_spine::{RowRowBuilder, RowRowColPagedBuilder, RowRowSpine};
use mz_timely_util::columnar::batcher;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::merge_batcher::ColumnMergeBatcher;
use mz_timely_util::columnar::{
    Col2ValBatcher, Col2ValColBatcher, Col2ValPagedBatcher, columnar_exchange,
};
use mz_timely_util::operator::{CollectionExt, StreamExt};
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::OkErr;

use crate::extensions::arrange::{ArrangementBatcher, MzArrangeCore};
use crate::render::RenderTimestamp;
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context};
use crate::render::errors::DataflowErrorSer;
use crate::typedefs::{RowRowAgent, RowRowEnter};

/// Joins two arranged collections, applying `result` to each matching pair.
///
/// A thin name for differential's `join_core`, which owns the fuel budget it yields on.
fn join_arranged<'s, T, Tr1, Tr2, L, I>(
    arranged1: Arranged<'s, Tr1>,
    arranged2: Arranged<'s, Tr2>,
    result: L,
) -> VecCollection<'s, T, I::Item, Diff>
where
    T: Lattice + timely::progress::Timestamp,
    Tr1: TraceReader<Batch: Navigable, Time = T> + Clone + 'static,
    Tr2: TraceReader<Batch: Navigable, Time = T> + Clone + 'static,
    BatchCursor<Tr1>: Cursor<Time = T, Diff = Diff>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = BatchKey<'a, Tr1>, Time = T, Diff = Diff>,
    L: FnMut(BatchKey<'_, Tr1>, BatchVal<'_, Tr1>, BatchVal<'_, Tr2>) -> I + 'static,
    I: IntoIterator<Item: Data> + 'static,
{
    arranged1.join_core(arranged2, result)
}

/// Different forms the streamed data might take.
enum JoinedFlavor<'scope, T: RenderTimestamp> {
    /// Streamed data as a collection.
    Collection(VecCollection<'scope, T, Row, Diff>),
    /// A dataflow-local arrangement.
    Local(Arranged<'scope, RowRowAgent<T, Diff>>),
    /// An imported arrangement.
    Trace(Arranged<'scope, RowRowEnter<mz_repr::Timestamp, Diff, T>>),
}

impl<'scope, T> Context<'scope, T>
where
    T: Lattice + RenderTimestamp,
{
    pub(crate) fn render_join(
        &self,
        inputs: Vec<CollectionBundle<'scope, T>>,
        linear_plan: LinearJoinPlan,
    ) -> CollectionBundle<'scope, T> {
        self.scope.clone().region_named("Join(Linear)", |inner| {
            self.render_join_inner(inputs, linear_plan, inner)
        })
    }

    fn render_join_inner(
        &self,
        inputs: Vec<CollectionBundle<'scope, T>>,
        linear_plan: LinearJoinPlan,
        inner: Scope<'_, T>,
    ) -> CollectionBundle<'scope, T> {
        // Collect all error streams, and concatenate them at the end.
        let mut errors = Vec::new();

        // Determine which form our maintained spine of updates will initially take.
        // First, just check out the availability of an appropriate arrangement.
        // This will be `None` in the degenerate single-input join case, which ensures
        // that we do not panic if we never go around the `stage_plans` loop.
        let arrangement = linear_plan
            .stage_plans
            .get(0)
            .and_then(|stage| inputs[linear_plan.source_relation].arrangement(&stage.stream_key));
        // We can use an arrangement if it exists and an initial closure does not.
        let mut joined = match (arrangement, linear_plan.initial_closure) {
            (Some(ArrangementFlavor::Local(oks, errs)), None) => {
                errors.push(errs.as_collection(|k, _v| k.clone()).enter_region(inner));
                JoinedFlavor::Local(oks.enter_region(inner))
            }
            (Some(ArrangementFlavor::Trace(_gid, oks, errs)), None) => {
                errors.push(errs.as_collection(|k, _v| k.clone()).enter_region(inner));
                JoinedFlavor::Trace(oks.enter_region(inner))
            }
            (_, initial_closure) => {
                // TODO: extract closure from the first stage in the join plan, should it exist.
                // TODO: apply that closure in `flat_map_ref` rather than calling `.collection`.
                let (joined, errs) = inputs[linear_plan.source_relation]
                    .as_specific_collection(linear_plan.source_key.as_deref(), &self.config_set);
                errors.push(errs.enter_region(inner));
                let mut joined = joined.enter_region(inner);

                // In the current code this should always be `None`, but we have this here should
                // we change that and want to know what we should be doing.
                if let Some(closure) = initial_closure {
                    // If there is no starting arrangement, then we can run filters
                    // directly on the starting collection.
                    // If there is only one input, we are done joining, so run filters
                    let name = "LinearJoinInitialization";
                    type CB<C> = ConsolidatingContainerBuilder<C>;
                    let (j, errs) = joined.flat_map_fallible::<CB<_>, CB<_>, _, _, _, _>(name, {
                        // Reuseable allocation for unpacking.
                        let mut datums = DatumVec::new();
                        move |row| {
                            let mut row_builder = SharedRow::get();
                            let temp_storage = RowArena::new();
                            let mut datums_local = datums.borrow_with(&row);
                            // TODO(mcsherry): re-use `row` allocation.
                            closure
                                .apply(&mut datums_local, &temp_storage, &mut row_builder)
                                .map(|row| row.cloned())
                                .map_err(DataflowErrorSer::from)
                                .transpose()
                        }
                    });
                    joined = j;
                    errors.push(errs);
                }

                JoinedFlavor::Collection(joined)
            }
        };

        // progress through stages, updating partial results and errors.
        for stage_plan in linear_plan.stage_plans.into_iter() {
            // Different variants of `joined` implement this differently,
            // and the logic is centralized there.
            let stream = self.differential_join(
                joined,
                inputs[stage_plan.lookup_relation].enter_region(inner),
                stage_plan,
                &mut errors,
            );
            // Update joined results and capture any errors.
            joined = JoinedFlavor::Collection(stream);
        }

        // We have completed the join building, but may have work remaining.
        // For example, we may have expressions not pushed down (e.g. literals)
        // and projections that could not be applied (e.g. column repetition).
        let bundle = if let JoinedFlavor::Collection(mut joined) = joined {
            if let Some(closure) = linear_plan.final_closure {
                let name = "LinearJoinFinalization";
                type CB<C> = ConsolidatingContainerBuilder<C>;
                let (updates, errs) = joined.flat_map_fallible::<CB<_>, CB<_>, _, _, _, _>(name, {
                    // Reuseable allocation for unpacking.
                    let mut datums = DatumVec::new();
                    move |row| {
                        let mut row_builder = SharedRow::get();
                        let temp_storage = RowArena::new();
                        let mut datums_local = datums.borrow_with(&row);
                        // TODO(mcsherry): re-use `row` allocation.
                        closure
                            .apply(&mut datums_local, &temp_storage, &mut row_builder)
                            .map(|row| row.cloned())
                            .map_err(DataflowErrorSer::from)
                            .transpose()
                    }
                });

                joined = updates;
                errors.push(errs);
            }

            // Return joined results and all produced errors collected together.
            CollectionBundle::from_collections(
                joined,
                differential_dataflow::collection::concatenate(inner, errors),
            )
        } else {
            panic!("Unexpectedly arranged join output");
        };
        bundle.leave_region(self.scope)
    }

    /// Looks up the arrangement for the next input and joins it to the arranged
    /// version of the join of previous inputs.
    fn differential_join<'s>(
        &self,
        mut joined: JoinedFlavor<'s, T>,
        lookup_relation: CollectionBundle<'s, T>,
        LinearStagePlan {
            stream_key,
            stream_thinning,
            lookup_key,
            closure,
            lookup_relation: _,
        }: LinearStagePlan,
        errors: &mut Vec<VecCollection<'s, T, DataflowErrorSer, Diff>>,
    ) -> VecCollection<'s, T, Row, Diff> {
        // If we have only a streamed collection, we must first form an arrangement.
        if let JoinedFlavor::Collection(stream) = joined {
            let name = "LinearJoinKeyPreparation";
            let (keyed, errs) = stream
                .inner
                .unary_fallible::<ColumnBuilder<((Row, Row), T, Diff)>, _, _, _>(
                    Pipeline,
                    name,
                    |_, _| {
                        Box::new(move |input, ok, errs| {
                            let mut temp_storage = RowArena::new();
                            let mut key_buf = Row::default();
                            let mut val_buf = Row::default();
                            let mut datums = DatumVec::new();
                            input.for_each(|time, data| {
                                let mut ok_session = ok.session_with_builder(&time);
                                let mut err_session = errs.session(&time);
                                for (row, time, diff) in data.iter() {
                                    temp_storage.clear();
                                    let datums_local = datums.borrow_with(row);
                                    let datums = stream_key
                                        .iter()
                                        .map(|e| e.eval(&datums_local, &temp_storage));
                                    let result = key_buf.packer().try_extend(datums);
                                    match result {
                                        Ok(()) => {
                                            val_buf.packer().extend(
                                                stream_thinning.iter().map(|e| datums_local[*e]),
                                            );
                                            ok_session.give(((&key_buf, &val_buf), time, diff));
                                        }
                                        Err(e) => {
                                            err_session.give((e.into(), time.clone(), *diff));
                                        }
                                    }
                                }
                            });
                        })
                    },
                );

            errors.push(errs.as_collection());

            let exchange = ExchangeCore::<ColumnBuilder<_>, _>::new_core(
                columnar_exchange::<Row, Row, T, Diff>,
            );
            let arranged = match ArrangementBatcher::from_config(&self.config_set) {
                ArrangementBatcher::ColumnarPaged => keyed
                    .mz_arrange_core::<_, Col2ValPagedBatcher<
                        _,
                        _,
                        _,
                        _,
                        batcher::ColumnChunker<_>,
                        RowRowColPagedBuilder<_, _>,
                    >, RowRowSpine<_, _>>(
                        exchange, "JoinStage", ColumnMergeBatcher::new
                    ),
                ArrangementBatcher::Columnar => keyed.mz_arrange_core::<_, Col2ValColBatcher<
                    _,
                    _,
                    _,
                    _,
                    batcher::ColumnChunker<_>,
                    RowRowColPagedBuilder<_, _>,
                >, RowRowSpine<_, _>>(
                    exchange, "JoinStage", MergeBatcher::new
                ),
                ArrangementBatcher::Columnation => keyed.mz_arrange_core::<_, Col2ValBatcher<
                    _,
                    _,
                    _,
                    _,
                    batcher::Chunker<_>,
                    RowRowBuilder<_, _>,
                >, RowRowSpine<_, _>>(
                    exchange, "JoinStage", MergeBatcher::new
                ),
            };
            joined = JoinedFlavor::Local(arranged);
        }

        // Demultiplex the four different cross products of arrangement types we might have.
        let arrangement = lookup_relation
            .arrangement(&lookup_key[..])
            .expect("Arrangement absent despite explicit construction");

        match joined {
            JoinedFlavor::Collection(_) => {
                unreachable!("JoinedFlavor::VecCollection variant avoided at top of method");
            }
            JoinedFlavor::Local(local) => match arrangement {
                ArrangementFlavor::Local(oks, errs1) => {
                    let (oks, errs2) = self
                        .differential_join_inner::<RowRowAgent<_, _>, RowRowAgent<_, _>>(
                            local, oks, closure,
                        );

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = self
                        .differential_join_inner::<RowRowAgent<_, _>, RowRowEnter<_, _, _>>(
                            local, oks, closure,
                        );

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
            },
            JoinedFlavor::Trace(trace) => match arrangement {
                ArrangementFlavor::Local(oks, errs1) => {
                    let (oks, errs2) = self
                        .differential_join_inner::<RowRowEnter<_, _, _>, RowRowAgent<_, _>>(
                            trace, oks, closure,
                        );

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = self
                        .differential_join_inner::<RowRowEnter<_, _, _>, RowRowEnter<_, _, _>>(
                            trace, oks, closure,
                        );

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
            },
        }
    }

    /// Joins the arrangement for `next_input` to the arranged version of the
    /// join of previous inputs. This is split into its own method to enable
    /// reuse of code with different types of `next_input`.
    ///
    /// The return type includes an optional error collection, which may be
    /// `None` if we can determine that `closure` cannot error.
    fn differential_join_inner<'s, Tr1, Tr2>(
        &self,
        prev_keyed: Arranged<'s, Tr1>,
        next_input: Arranged<'s, Tr2>,
        closure: JoinClosure,
    ) -> (
        VecCollection<'s, T, Row, Diff>,
        Option<VecCollection<'s, T, DataflowErrorSer, Diff>>,
    )
    where
        Tr1: TraceReader<Batch: Navigable, Time = T> + Clone + 'static,
        Tr2: TraceReader<Batch: Navigable, Time = T> + Clone + 'static,
        for<'a> BatchCursor<Tr1>:
            Cursor<Key<'a>: ExtendDatums, Val<'a>: ExtendDatums, Time = T, Diff = Diff>,
        for<'a> BatchCursor<Tr2>:
            Cursor<Key<'a> = BatchKey<'a, Tr1>, Val<'a>: ExtendDatums, Time = T, Diff = Diff>,
    {
        // Reuseable allocation for unpacking.
        let mut datums = DatumVec::new();

        if closure.could_error() {
            let (oks, err) = join_arranged(prev_keyed, next_input, move |key, old, new| {
                let mut row_builder = SharedRow::get();
                let temp_storage = RowArena::new();

                let mut datums_local = datums.borrow();
                key.extend_datums(&temp_storage, &mut datums_local, None);
                old.extend_datums(&temp_storage, &mut datums_local, None);
                new.extend_datums(&temp_storage, &mut datums_local, None);

                closure
                    .apply(&mut datums_local, &temp_storage, &mut row_builder)
                    .map(|row| row.cloned())
                    .map_err(DataflowErrorSer::from)
                    .transpose()
            })
            .inner
            .ok_err(|(x, t, d)| {
                // TODO(mcsherry): consider `ok_err()` for `Collection`.
                match x {
                    Ok(x) => Ok((x, t, d)),
                    Err(x) => Err((x, t, d)),
                }
            });

            (oks.as_collection(), Some(err.as_collection()))
        } else {
            let oks = join_arranged(prev_keyed, next_input, move |key, old, new| {
                let mut row_builder = SharedRow::get();
                let temp_storage = RowArena::new();

                let mut datums_local = datums.borrow();
                key.extend_datums(&temp_storage, &mut datums_local, None);
                old.extend_datums(&temp_storage, &mut datums_local, None);
                new.extend_datums(&temp_storage, &mut datums_local, None);

                closure
                    .apply(&mut datums_local, &temp_storage, &mut row_builder)
                    .expect("Closure claimed to never error")
                    .cloned()
            });

            (oks, None)
        }
    }
}
