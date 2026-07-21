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

use std::time::{Duration, Instant};

use columnar::{Columnar, Index};
use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::trace::cursor::{BatchCursor, BatchKey, BatchVal};
use differential_dataflow::trace::{Cursor, Navigable, TraceReader};
use differential_dataflow::{AsCollection, Data, VecCollection};
use mz_compute_types::dyncfgs::{
    ENABLE_COLUMN_PAGED_BATCHER, ENABLE_MZ_JOIN_CORE, LINEAR_JOIN_YIELDING,
};
use mz_compute_types::plan::join::JoinClosure;
use mz_compute_types::plan::join::linear_join::{LinearJoinPlan, LinearStagePlan};
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_dyncfg::ConfigSet;
use mz_expr::Eval;
use mz_repr::fixed_length::ExtendDatums;
use mz_repr::{DatumVec, Diff, Row, RowArena, SharedRow};
use mz_timely_util::columnar::batcher;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::{Col2ValBatcher, Col2ValPagedBatcher, columnar_exchange};
use mz_timely_util::operator::{CollectionExt, StreamExt};
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::OkErr;

use crate::extensions::arrange::MzArrangeCore;
use crate::render::RenderTimestamp;
use crate::render::columnar::CollectionEdge;
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context};
use crate::render::errors::DataflowErrorSer;
use crate::render::join::mz_join_core::mz_join_core;
use crate::typedefs::{RowRowAgent, RowRowEnter};
use mz_row_spine::{RowRowBuilder, RowRowColPagedBuilder, RowRowSpine};

/// Available linear join implementations.
///
/// See the `mz_join_core` module docs for our rationale for providing two join implementations.
#[derive(Clone, Copy)]
enum LinearJoinImpl {
    Materialize,
    DifferentialDataflow,
}

/// Specification of how linear joins are to be executed.
///
/// Note that currently `yielding` only affects the `Materialize` join implementation, as the DD
/// join doesn't allow configuring its yielding behavior. Merging [#390] would fix this.
///
/// [#390]: https://github.com/TimelyDataflow/differential-dataflow/pull/390
#[derive(Clone, Copy)]
pub struct LinearJoinSpec {
    implementation: LinearJoinImpl,
    yielding: YieldSpec,
}

impl Default for LinearJoinSpec {
    fn default() -> Self {
        Self {
            implementation: LinearJoinImpl::Materialize,
            yielding: Default::default(),
        }
    }
}

impl LinearJoinSpec {
    /// Create a `LinearJoinSpec` based on the given config.
    pub fn from_config(config: &ConfigSet) -> Self {
        let implementation = if ENABLE_MZ_JOIN_CORE.get(config) {
            LinearJoinImpl::Materialize
        } else {
            LinearJoinImpl::DifferentialDataflow
        };

        let yielding_raw = LINEAR_JOIN_YIELDING.get(config);
        let yielding = YieldSpec::try_from_str(&yielding_raw).unwrap_or_else(|| {
            tracing::error!("invalid LINEAR_JOIN_YIELDING config: {yielding_raw}");
            YieldSpec::default()
        });

        Self {
            implementation,
            yielding,
        }
    }

    /// Render a join operator according to this specification.
    fn render<'s, T, Tr1, Tr2, L, I>(
        &self,
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
        use LinearJoinImpl::*;

        match (
            self.implementation,
            self.yielding.after_work,
            self.yielding.after_time,
        ) {
            (DifferentialDataflow, _, _) => arranged1.join_core(arranged2, result),
            (Materialize, Some(work_limit), Some(time_limit)) => {
                let yield_fn =
                    move |start: Instant, work| work >= work_limit || start.elapsed() >= time_limit;
                mz_join_core(arranged1, arranged2, result, yield_fn).as_collection()
            }
            (Materialize, Some(work_limit), None) => {
                let yield_fn = move |_start, work| work >= work_limit;
                mz_join_core(arranged1, arranged2, result, yield_fn).as_collection()
            }
            (Materialize, None, Some(time_limit)) => {
                let yield_fn = move |start: Instant, _work| start.elapsed() >= time_limit;
                mz_join_core(arranged1, arranged2, result, yield_fn).as_collection()
            }
            (Materialize, None, None) => {
                let yield_fn = |_start, _work| false;
                mz_join_core(arranged1, arranged2, result, yield_fn).as_collection()
            }
        }
    }
}

/// Specification of a dataflow operator's yielding behavior.
#[derive(Clone, Copy)]
struct YieldSpec {
    /// Yield after the given amount of work was performed.
    after_work: Option<usize>,
    /// Yield after the given amount of time has elapsed.
    after_time: Option<Duration>,
}

impl Default for YieldSpec {
    fn default() -> Self {
        Self {
            after_work: Some(1_000_000),
            after_time: Some(Duration::from_millis(100)),
        }
    }
}

impl YieldSpec {
    fn try_from_str(s: &str) -> Option<Self> {
        let mut after_work = None;
        let mut after_time = None;

        let options = s.split(',').map(|o| o.trim());
        for option in options {
            let mut iter = option.split(':').map(|p| p.trim());
            match std::array::from_fn(|_| iter.next()) {
                [Some("work"), Some(amount), None] => {
                    let amount = amount.parse().ok()?;
                    after_work = Some(amount);
                }
                [Some("time"), Some(millis), None] => {
                    let millis = millis.parse().ok()?;
                    let duration = Duration::from_millis(millis);
                    after_time = Some(duration);
                }
                _ => return None,
            }
        }

        Some(Self {
            after_work,
            after_time,
        })
    }
}

/// Different forms the streamed data might take.
enum JoinedFlavor<'scope, T: RenderTimestamp> {
    /// Streamed data as a collection edge. `differential_join` forms its
    /// arrangement key off the edge, so a columnar source flows in without a
    /// `ColumnarToVec` decode.
    Collection(CollectionEdge<'scope, T>),
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
                let (joined, errs) = match linear_plan.source_key.as_deref() {
                    // No source key: consume the input edge directly rather than
                    // decoding it to `Vec`. `differential_join` forms the source
                    // arrangement key off this edge below (C1 borrowed-push
                    // pattern), so a columnar source has no `ColumnarToVec` hop.
                    None => inputs[linear_plan.source_relation]
                        .collection
                        .clone()
                        .expect("The unarranged collection doesn't exist."),
                    // With a source key the input is materialized from an existing
                    // arrangement, which `as_specific_collection` presents as a
                    // columnar edge. `differential_join` forms the source key off
                    // this edge below, so the columnar source has no `ColumnarToVec`
                    // hop.
                    Some(key) => inputs[linear_plan.source_relation]
                        .as_specific_collection(Some(key), &self.config_set),
                };
                errors.push(errs.enter_region(inner));
                let joined = joined.enter_region(inner);

                // In the current code this should always be `None`, but we have this here should
                // we change that and want to know what we should be doing.
                if let Some(closure) = initial_closure {
                    // If there is no starting arrangement, then we can run filters
                    // directly on the starting collection.
                    // If there is only one input, we are done joining, so run filters.
                    // `into_vec` is the identity on the `Vec` arm, so this is
                    // unchanged for `Vec` sources; a columnar source decodes here,
                    // but this branch is never taken in current lowering.
                    let name = "LinearJoinInitialization";
                    type CB<C> = ConsolidatingContainerBuilder<C>;
                    let (j, errs) = joined
                        .into_vec()
                        .flat_map_fallible::<CB<_>, CB<_>, _, _, _, _>(name, {
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
                    errors.push(errs);
                    JoinedFlavor::Collection(CollectionEdge::Vec(j))
                } else {
                    JoinedFlavor::Collection(joined)
                }
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
            // Update joined results and capture any errors. Stage output is a
            // `Vec` collection.
            joined = JoinedFlavor::Collection(CollectionEdge::Vec(stream));
        }

        // We have completed the join building, but may have work remaining.
        // For example, we may have expressions not pushed down (e.g. literals)
        // and projections that could not be applied (e.g. column repetition).
        let bundle = if let JoinedFlavor::Collection(joined) = joined {
            // The join output is a `Vec` collection, so `into_vec` is the identity
            // on the `Vec` arm.
            let mut joined = joined.into_vec();
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
            let (arranged, errs) = arrange_join_input(
                stream,
                stream_key,
                stream_thinning,
                ENABLE_COLUMN_PAGED_BATCHER.get(&self.config_set),
            );
            errors.push(errs);
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
            let (oks, err) = self
                .linear_join_spec
                .render(prev_keyed, next_input, move |key, old, new| {
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
            let oks = self
                .linear_join_spec
                .render(prev_keyed, next_input, move |key, old, new| {
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

/// Forms the source arrangement for a streamed join input off a collection edge.
///
/// Both arms build the same `((key, value), t, d)` columnar updates and push the
/// key and value borrowed into a `ColumnBuilder`, which the `Col2Val` batcher
/// consumes. This is the zero-allocation pattern: no owned `Row` per record on
/// the ok path. The arms differ only in how records are read, the `Vec` arm from
/// the owned container and the columnar arm from the borrowed column, and in the
/// error path, which owns time and diff.
fn arrange_join_input<'s, T>(
    edge: CollectionEdge<'s, T>,
    stream_key: Vec<LirScalarExpr>,
    stream_thinning: Vec<usize>,
    use_paged_path: bool,
) -> (
    Arranged<'s, RowRowAgent<T, Diff>>,
    VecCollection<'s, T, DataflowErrorSer, Diff>,
)
where
    T: Lattice + RenderTimestamp,
{
    let name = "LinearJoinKeyPreparation";
    let (keyed, errs) = match edge {
        CollectionEdge::Vec(stream) => stream
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
                                match key_buf.packer().try_extend(datums) {
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
            ),
        CollectionEdge::Columnar(stream) => stream
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
                            // Rows are read from the borrowed column; the key and
                            // value are pushed borrowed. Time and diff are owned
                            // only on the error path.
                            for (row, time, diff) in data.borrow().into_index_iter() {
                                temp_storage.clear();
                                let datums_local = datums.borrow_with(row);
                                let datums = stream_key
                                    .iter()
                                    .map(|e| e.eval(&datums_local, &temp_storage));
                                match key_buf.packer().try_extend(datums) {
                                    Ok(()) => {
                                        val_buf.packer().extend(
                                            stream_thinning.iter().map(|e| datums_local[*e]),
                                        );
                                        ok_session.give(((&key_buf, &val_buf), time, diff));
                                    }
                                    Err(e) => {
                                        err_session.give((
                                            e.into(),
                                            Columnar::into_owned(time),
                                            Columnar::into_owned(diff),
                                        ));
                                    }
                                }
                            }
                        });
                    })
                },
            ),
    };

    let exchange =
        ExchangeCore::<ColumnBuilder<_>, _>::new_core(columnar_exchange::<Row, Row, T, Diff>);
    let arranged = if use_paged_path {
        keyed.mz_arrange_core::<
            _,
            batcher::ColumnChunker<_>,
            Col2ValPagedBatcher<_, _, _, _>,
            RowRowColPagedBuilder<_, _>,
            RowRowSpine<_, _>,
        >(exchange, "JoinStage")
    } else {
        keyed.mz_arrange_core::<
            _,
            batcher::Chunker<_>,
            Col2ValBatcher<_, _, _, _>,
            RowRowBuilder<_, _>,
            RowRowSpine<_, _>,
        >(exchange, "JoinStage")
    };
    (arranged, errs.as_collection())
}

#[cfg(test)]
mod tests {
    use differential_dataflow::input::Input;
    use mz_expr::EvalError;
    use mz_repr::{Datum, ReprScalarType, Timestamp};
    use timely::dataflow::operators::Capture;
    use timely::dataflow::operators::capture::{Event, Extract};

    use super::*;
    use crate::render::columnar::vec_to_columnar;

    type KeyedUpdate = ((Row, Row), Timestamp, Diff);
    type ErrUpdate = (DataflowErrorSer, Timestamp, Diff);
    type Captured<D> = std::sync::mpsc::Receiver<Event<Timestamp, Vec<D>>>;

    fn extract_sorted(captured: Captured<KeyedUpdate>) -> Vec<KeyedUpdate> {
        let mut updates: Vec<_> = captured
            .extract()
            .into_iter()
            .flat_map(|(_, data)| data)
            .collect();
        updates.sort();
        updates
    }

    // `DataflowErrorSer` is not `Ord`, so project the error to its debug string
    // for a stable ordering. The time and diff ride along, so this verifies the
    // columnar arm's `into_owned` on the error path reconstructs the same
    // `(time, diff)` as the `Vec` arm.
    fn extract_err(captured: Captured<ErrUpdate>) -> Vec<(String, Timestamp, Diff)> {
        let mut updates: Vec<_> = captured
            .extract()
            .into_iter()
            .flat_map(|(_, data)| data)
            .map(|(e, t, d)| (format!("{e:?}"), t, d))
            .collect();
        updates.sort();
        updates
    }

    /// Input rows tagged with distinct timestamps and mixed-sign diffs. The two
    /// `-1` records retract at a `(row, time)` with no matching insertion, so they
    /// survive the `InputSession`'s pre-send consolidation and exercise a negative
    /// diff on the ok path (pushed borrowed, not owned).
    fn test_input() -> Vec<(Row, u64, Diff)> {
        vec![
            (
                Row::pack_slice(&[Datum::Int32(1), Datum::String("a")]),
                0,
                Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(2), Datum::String("b")]),
                1,
                Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(1), Datum::String("c")]),
                2,
                Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(3), Datum::Null]),
                2,
                Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(2), Datum::String("b")]),
                2,
                -Diff::ONE,
            ),
            (
                Row::pack_slice(&[Datum::Int32(4), Datum::String("d")]),
                1,
                -Diff::ONE,
            ),
        ]
    }

    /// Runs `arrange_join_input` against the same input fed once as a `Vec` edge
    /// and once as a columnar edge, keying by `key` with column 1 as the value.
    /// Returns the sorted ok updates (read back from each arrangement) and the
    /// sorted err updates of each arm.
    #[allow(clippy::type_complexity)]
    fn run_both_arms(
        input: Vec<(Row, u64, Diff)>,
        key: Vec<LirScalarExpr>,
    ) -> (
        Vec<KeyedUpdate>,
        Vec<KeyedUpdate>,
        Vec<(String, Timestamp, Diff)>,
        Vec<(String, Timestamp, Diff)>,
    ) {
        let (ok_vec, ok_col, err_vec, err_col) = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut handle, collection) = scope.new_collection();
                let mut ok_caps = Vec::new();
                let mut err_caps = Vec::new();
                for edge in [
                    CollectionEdge::Vec(collection.clone()),
                    CollectionEdge::Columnar(vec_to_columnar(collection)),
                ] {
                    let (arranged, errs) = arrange_join_input(edge, key.clone(), vec![1], false);
                    let keyed = arranged.as_collection(|k, v| (k.to_row(), v.to_row()));
                    ok_caps.push(keyed.inner.capture());
                    err_caps.push(errs.inner.capture());
                }
                let err_col = err_caps.pop().unwrap();
                let err_vec = err_caps.pop().unwrap();
                let ok_col = ok_caps.pop().unwrap();
                let ok_vec = ok_caps.pop().unwrap();
                for (row, time, diff) in input {
                    handle.update_at(row, Timestamp::from(time), diff);
                }
                handle.advance_to(Timestamp::from(3_u64));
                handle.flush();
                (ok_vec, ok_col, err_vec, err_col)
            })
        });
        (
            extract_sorted(ok_vec),
            extract_sorted(ok_col),
            extract_err(err_vec),
            extract_err(err_col),
        )
    }

    /// The columnar arm of `arrange_join_input` forms the same keyed arrangement
    /// as the `Vec` arm, across several distinct timestamps and a retraction.
    ///
    /// This proves the columnar key-forming is correct and that the columnar arm
    /// ran (the input is fed through `vec_to_columnar`). It does not prove the
    /// absence of a silent `ColumnarToVec` decode on the ok path: such a decode
    /// would yield identical contents. No-decode holds by code inspection, the
    /// columnar arm reads via `into_index_iter` and pushes the key and value
    /// borrowed into the `ColumnBuilder`, never calling `into_vec`.
    ///
    /// The stream key here is infallible column projection, so `try_extend` never
    /// fails and the ok path pushes the diff borrowed. The retraction records
    /// exercise a negative diff on that borrowed ok path. `Columnar::into_owned`
    /// runs only on the error path, which `arrange_join_input_arms_agree_on_error_path`
    /// covers.
    #[mz_ore::test]
    fn arrange_join_input_arms_agree() {
        let (ok_vec, ok_col, err_vec, err_col) =
            run_both_arms(test_input(), vec![LirScalarExpr::column(0)]);
        assert!(!ok_vec.is_empty());
        assert_eq!(ok_vec, ok_col);
        assert!(err_vec.is_empty() && err_col.is_empty());
        // A retraction survives into the arrangement, so the ok path handled a
        // negative (borrowed) diff.
        assert!(ok_vec.iter().any(|(_, _, d)| *d < Diff::ZERO));
        // Key is column 0, value is column 1 (the thinning), so both are single
        // datums.
        for ((key, value), _t, _d) in &ok_vec {
            assert_eq!(key.iter().count(), 1);
            assert_eq!(value.iter().count(), 1);
        }
    }

    /// A key expression that always errors drives every record onto the
    /// `try_extend` Err branch, exercising the columnar arm's `Columnar::into_owned`
    /// reconstruction of each error's `(time, diff)`. Both arms must agree on the
    /// errors, and the ok output must be empty on both.
    #[mz_ore::test]
    fn arrange_join_input_arms_agree_on_error_path() {
        let key = vec![LirScalarExpr::literal(
            Err(EvalError::DivisionByZero),
            ReprScalarType::Int32,
        )];
        let (ok_vec, ok_col, err_vec, err_col) = run_both_arms(test_input(), key);
        assert!(ok_vec.is_empty() && ok_col.is_empty());
        assert!(!err_vec.is_empty());
        assert_eq!(err_vec, err_col);
    }
}
