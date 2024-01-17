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

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{AsCollection, Collection, Data};
use mz_compute_types::dataflows::YieldSpec;
use mz_compute_types::plan::join::linear_join::{LinearJoinPlan, LinearStagePlan};
use mz_compute_types::plan::join::JoinClosure;
use mz_repr::fixed_length::IntoRowByTypes;
use mz_repr::{ColumnType, DatumVec, Diff, Row, RowArena, SharedRow};
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;
use timely::container::columnation::Columnation;
use timely::dataflow::operators::OkErr;
use timely::dataflow::Scope;
use timely::progress::timestamp::{Refines, Timestamp};

use crate::extensions::arrange::MzArrange;
use crate::render::context::{
    ArrangementFlavor, CollectionBundle, Context, ShutdownToken, SpecializedArrangement,
    SpecializedArrangementImport,
};
use crate::render::join::mz_join_core::mz_join_core;
use crate::row_spine::RowRowSpine;
use crate::typedefs::{RowAgent, RowEnter, RowRowAgent, RowRowEnter};

/// Available linear join implementations.
///
/// See the `mz_join_core` module docs for our rationale for providing two join implementations.
#[derive(Clone, Copy)]
pub enum LinearJoinImpl {
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
    pub implementation: LinearJoinImpl,
    pub yielding: YieldSpec,
}

impl Default for LinearJoinSpec {
    fn default() -> Self {
        Self {
            implementation: LinearJoinImpl::Materialize,
            yielding: YieldSpec {
                after_work: Some(1_000_000),
                after_time: Some(Duration::from_millis(100)),
            },
        }
    }
}

impl LinearJoinSpec {
    /// Render a join operator according to this specification.
    fn render<G, Tr1, Tr2, L, I>(
        &self,
        arranged1: &Arranged<G, Tr1>,
        arranged2: &Arranged<G, Tr2>,
        shutdown_token: ShutdownToken,
        result: L,
    ) -> Collection<G, I::Item, Diff>
    where
        G: Scope,
        G::Timestamp: Lattice,
        Tr1: TraceReader<Time = G::Timestamp, Diff = Diff> + Clone + 'static,
        Tr2: for<'a> TraceReader<Key<'a> = Tr1::Key<'a>, Time = G::Timestamp, Diff = Diff>
            + Clone
            + 'static,
        L: FnMut(Tr1::Key<'_>, Tr1::Val<'_>, Tr2::Val<'_>) -> I + 'static,
        I: IntoIterator,
        I::Item: Data,
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
                mz_join_core(arranged1, arranged2, shutdown_token, result, yield_fn)
            }
            (Materialize, Some(work_limit), None) => {
                let yield_fn = move |_start, work| work >= work_limit;
                mz_join_core(arranged1, arranged2, shutdown_token, result, yield_fn)
            }
            (Materialize, None, Some(time_limit)) => {
                let yield_fn = move |start: Instant, _work| start.elapsed() >= time_limit;
                mz_join_core(arranged1, arranged2, shutdown_token, result, yield_fn)
            }
            (Materialize, None, None) => {
                let yield_fn = |_start, _work| false;
                mz_join_core(arranged1, arranged2, shutdown_token, result, yield_fn)
            }
        }
    }
}

/// Different forms the streamed data might take.
enum JoinedFlavor<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T> + Columnation,
    T: Timestamp + Lattice + Columnation,
{
    /// Streamed data as a collection.
    Collection(Collection<G, Row, Diff>),
    /// A dataflow-local arrangement.
    Local(SpecializedArrangement<G>),
    /// An imported arrangement.
    Trace(SpecializedArrangementImport<G, T>),
}

impl<G, T> Context<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T> + Columnation,
    T: Timestamp + Lattice + Columnation,
{
    pub(crate) fn render_join(
        &mut self,
        inputs: Vec<CollectionBundle<G, T>>,
        linear_plan: LinearJoinPlan,
    ) -> CollectionBundle<G, T> {
        self.scope.clone().region_named("Join(Linear)", |inner| {
            // Collect all error streams, and concatenate them at the end.
            let mut errors = Vec::new();

            // Determine which form our maintained spine of updates will initially take.
            // First, just check out the availability of an appropriate arrangement.
            // This will be `None` in the degenerate single-input join case, which ensures
            // that we do not panic if we never go around the `stage_plans` loop.
            let arrangement = linear_plan.stage_plans.get(0).and_then(|stage| {
                inputs[linear_plan.source_relation].arrangement(&stage.stream_key)
            });
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
                        .as_specific_collection(linear_plan.source_key.as_deref());
                    errors.push(errs.enter_region(inner));
                    let mut joined = joined.enter_region(inner);

                    // In the current code this should always be `None`, but we have this here should
                    // we change that and want to know what we should be doing.
                    if let Some(closure) = initial_closure {
                        // If there is no starting arrangement, then we can run filters
                        // directly on the starting collection.
                        // If there is only one input, we are done joining, so run filters
                        let (j, errs) = joined.flat_map_fallible("LinearJoinInitialization", {
                            // Reuseable allocation for unpacking.
                            let mut datums = DatumVec::new();
                            move |row| {
                                let binding = SharedRow::get();
                                let mut row_builder = binding.borrow_mut();
                                let temp_storage = RowArena::new();
                                let mut datums_local = datums.borrow_with(&row);
                                // TODO(mcsherry): re-use `row` allocation.
                                closure
                                    .apply(&mut datums_local, &temp_storage, &mut row_builder)
                                    .map_err(DataflowError::from)
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
            if let JoinedFlavor::Collection(mut joined) = joined {
                if let Some(closure) = linear_plan.final_closure {
                    let (updates, errs) = joined.flat_map_fallible("LinearJoinFinalization", {
                        // Reuseable allocation for unpacking.
                        let mut datums = DatumVec::new();
                        move |row| {
                            let binding = SharedRow::get();
                            let mut row_builder = binding.borrow_mut();
                            let temp_storage = RowArena::new();
                            let mut datums_local = datums.borrow_with(&row);
                            // TODO(mcsherry): re-use `row` allocation.
                            closure
                                .apply(&mut datums_local, &temp_storage, &mut row_builder)
                                .map_err(DataflowError::from)
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
            }
            .leave_region()
        })
    }

    /// Looks up the arrangement for the next input and joins it to the arranged
    /// version of the join of previous inputs.
    fn differential_join<S>(
        &self,
        mut joined: JoinedFlavor<S, T>,
        lookup_relation: CollectionBundle<S, T>,
        LinearStagePlan {
            stream_key,
            stream_thinning,
            lookup_key,
            closure,
            lookup_relation: _,
        }: LinearStagePlan,
        errors: &mut Vec<Collection<S, DataflowError, Diff>>,
    ) -> Collection<S, Row, Diff>
    where
        S: Scope<Timestamp = G::Timestamp>,
    {
        // If we have only a streamed collection, we must first form an arrangement.
        if let JoinedFlavor::Collection(stream) = joined {
            let (keyed, errs) = stream.map_fallible("LinearJoinKeyPreparation", {
                // Reuseable allocation for unpacking.
                let mut datums = DatumVec::new();
                move |row| {
                    let binding = SharedRow::get();
                    let mut row_builder = binding.borrow_mut();
                    let temp_storage = RowArena::new();
                    let datums_local = datums.borrow_with(&row);
                    row_builder.packer().try_extend(
                        stream_key
                            .iter()
                            .map(|e| e.eval(&datums_local, &temp_storage)),
                    )?;
                    let key = row_builder.clone();
                    row_builder
                        .packer()
                        .extend(stream_thinning.iter().map(|e| datums_local[*e]));
                    let value = row_builder.clone();
                    Ok((key, value))
                }
            });

            errors.push(errs);

            // TODO(vmarcos): We should implement further arrangement specialization here (#22104).
            // By knowing how types propagate through joins we could specialize intermediate
            // arrangements as well, either in values or eventually in keys.
            let arranged = keyed.mz_arrange::<RowRowSpine<_, _>>("JoinStage");
            joined = JoinedFlavor::Local(SpecializedArrangement::RowRow(arranged));
        }

        // Demultiplex the four different cross products of arrangement types we might have.
        let arrangement = lookup_relation
            .arrangement(&lookup_key[..])
            .expect("Arrangement absent despite explicit construction");

        use SpecializedArrangement as A;
        use SpecializedArrangementImport as I;

        match joined {
            JoinedFlavor::Collection(_) => {
                unreachable!("JoinedFlavor::Collection variant avoided at top of method");
            }
            JoinedFlavor::Local(local) => match arrangement {
                ArrangementFlavor::Local(oks, errs1) => {
                    let (oks, errs2) = match (local, oks) {
                        (A::RowUnit(prev_keyed), A::RowUnit(next_input)) => self
                            .differential_join_inner::<_, RowAgent<_, _>, RowAgent<_, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                Some(vec![]),
                                Some(vec![]),
                                closure,
                            ),
                        (A::RowUnit(prev_keyed), A::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowAgent<_, _>, RowRowAgent<_, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                Some(vec![]),
                                None,
                                closure,
                            ),
                        (A::RowRow(prev_keyed), A::RowUnit(next_input)) => self
                            .differential_join_inner::<_, RowRowAgent<_, _>, RowAgent<_, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                None,
                                Some(vec![]),
                                closure,
                            ),
                        (A::RowRow(prev_keyed), A::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowRowAgent<_, _>, RowRowAgent<_, _>>(
                                prev_keyed, next_input, None, None, None, closure,
                            ),
                    };

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = match (local, oks) {
                        (A::RowUnit(prev_keyed), I::RowUnit(next_input)) => self
                            .differential_join_inner::<_, RowAgent<_, _>, RowEnter<_, _, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                Some(vec![]),
                                Some(vec![]),
                                closure,
                            ),
                        (A::RowUnit(prev_keyed), I::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowAgent<_, _>, RowRowEnter<_, _, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                Some(vec![]),
                                None,
                                closure,
                            ),
                        (A::RowRow(prev_keyed), I::RowUnit(next_input)) => self
                            .differential_join_inner::<_, RowRowAgent<_, _>, RowEnter<_, _, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                None,
                                Some(vec![]),
                                closure,
                            ),
                        (A::RowRow(prev_keyed), I::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowRowAgent<_, _>, RowRowEnter<_, _, _>>(
                                prev_keyed, next_input, None, None, None, closure,
                            ),
                    };

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
            },
            JoinedFlavor::Trace(trace) => match arrangement {
                ArrangementFlavor::Local(oks, errs1) => {
                    let (oks, errs2) = match (trace, oks) {
                        (I::RowUnit(prev_keyed), A::RowUnit(next_input)) => self
                            .differential_join_inner::<_, RowEnter<_, _, _>, RowAgent<_, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                Some(vec![]),
                                Some(vec![]),
                                closure,
                            ),
                        (I::RowUnit(prev_keyed), A::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowEnter<_, _, _>, RowRowAgent<_, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                Some(vec![]),
                                None,
                                closure,
                            ),
                        (I::RowRow(prev_keyed), A::RowUnit(next_input)) => self
                            .differential_join_inner::<_, RowRowEnter<_, _, _>, RowAgent<_, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                None,
                                Some(vec![]),
                                closure,
                            ),
                        (I::RowRow(prev_keyed), A::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowRowEnter<_, _, _>, RowRowAgent<_, _>>(
                                prev_keyed, next_input, None, None, None, closure,
                            ),
                    };

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = match (trace, oks) {
                        (I::RowUnit(prev_keyed), I::RowUnit(next_input)) => self
                            .differential_join_inner::<_, RowEnter<_, _, _>, RowEnter<_, _, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                Some(vec![]),
                                Some(vec![]),
                                closure,
                            ),
                        (I::RowUnit(prev_keyed), I::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowEnter<_, _, _>, RowRowEnter<_, _, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                Some(vec![]),
                                None,
                                closure,
                            ),
                        (I::RowRow(prev_keyed), I::RowUnit(next_input)) => self
                            .differential_join_inner::<_, RowRowEnter<_, _, _>, RowEnter<_, _, _>>(
                                prev_keyed,
                                next_input,
                                None,
                                None,
                                Some(vec![]),
                                closure,
                            ),
                        (I::RowRow(prev_keyed), I::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowRowEnter<_, _, _>, RowRowEnter<_, _, _>>(
                                prev_keyed, next_input, None, None, None, closure,
                            ),
                    };

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
    fn differential_join_inner<S, Tr1, Tr2>(
        &self,
        prev_keyed: Arranged<S, Tr1>,
        next_input: Arranged<S, Tr2>,
        key_types: Option<Vec<ColumnType>>,
        prev_types: Option<Vec<ColumnType>>,
        next_types: Option<Vec<ColumnType>>,
        closure: JoinClosure,
    ) -> (
        Collection<S, Row, Diff>,
        Option<Collection<S, DataflowError, Diff>>,
    )
    where
        S: Scope<Timestamp = G::Timestamp>,
        Tr1: TraceReader<Time = G::Timestamp, Diff = Diff> + Clone + 'static,
        Tr2: for<'a> TraceReader<Key<'a> = Tr1::Key<'a>, Time = G::Timestamp, Diff = Diff>
            + Clone
            + 'static,
        for<'a> Tr1::Key<'a>: IntoRowByTypes,
        for<'a> Tr1::Val<'a>: IntoRowByTypes,
        for<'a> Tr2::Val<'a>: IntoRowByTypes,
    {
        // Reuseable allocation for unpacking.
        let mut datums = DatumVec::new();

        if closure.could_error() {
            let (oks, err) = self
                .linear_join_spec
                .render(
                    &prev_keyed,
                    &next_input,
                    self.shutdown_token.clone(),
                    move |key, old, new| {
                        let binding = SharedRow::get();
                        let mut row_builder = binding.borrow_mut();
                        let temp_storage = RowArena::new();

                        let key = key.into_datum_iter(key_types.as_deref());
                        let old = old.into_datum_iter(prev_types.as_deref());
                        let new = new.into_datum_iter(next_types.as_deref());

                        let mut datums_local = datums.borrow();
                        datums_local.extend(key);
                        datums_local.extend(old);
                        datums_local.extend(new);

                        closure
                            .apply(&mut datums_local, &temp_storage, &mut row_builder)
                            .map_err(DataflowError::from)
                            .transpose()
                    },
                )
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
            let oks = self.linear_join_spec.render(
                &prev_keyed,
                &next_input,
                self.shutdown_token.clone(),
                move |key, old, new| {
                    let binding = SharedRow::get();
                    let mut row_builder = binding.borrow_mut();
                    let temp_storage = RowArena::new();

                    let key = key.into_datum_iter(key_types.as_deref());
                    let old = old.into_datum_iter(prev_types.as_deref());
                    let new = new.into_datum_iter(next_types.as_deref());

                    let mut datums_local = datums.borrow();
                    datums_local.extend(key);
                    datums_local.extend(old);
                    datums_local.extend(new);

                    closure
                        .apply(&mut datums_local, &temp_storage, &mut row_builder)
                        .expect("Closure claimed to never error")
                },
            );

            (oks, None)
        }
    }
}
