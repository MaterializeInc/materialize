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

use columnar::Columnar;
use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{AsCollection, Collection, Data};
use mz_compute_types::dyncfgs::{ENABLE_MZ_JOIN_CORE, LINEAR_JOIN_YIELDING};
use mz_compute_types::plan::join::linear_join::{LinearJoinPlan, LinearStagePlan};
use mz_compute_types::plan::join::JoinClosure;
use mz_dyncfg::ConfigSet;
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::{DatumVec, Diff, Row, RowArena, SharedRow};
use mz_storage_types::errors::DataflowError;
use mz_timely_util::containers::{columnar_exchange, Col2ValBatcher, ColumnBuilder};
use mz_timely_util::operator::{CollectionExt, StreamExt};
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::OkErr;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::timestamp::{Refines, Timestamp};

use crate::extensions::arrange::MzArrangeCore;
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context, ShutdownToken};
use crate::render::join::mz_join_core::mz_join_core;
use crate::render::RenderTimestamp;
use crate::row_spine::{RowRowBuilder, RowRowSpine};
use crate::typedefs::{RowRowAgent, RowRowEnter};

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
        let implementation = match ENABLE_MZ_JOIN_CORE.get(config) {
            true => LinearJoinImpl::Materialize,
            false => LinearJoinImpl::DifferentialDataflow,
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
                mz_join_core(arranged1, arranged2, shutdown_token, result, yield_fn).as_collection()
            }
            (Materialize, Some(work_limit), None) => {
                let yield_fn = move |_start, work| work >= work_limit;
                mz_join_core(arranged1, arranged2, shutdown_token, result, yield_fn).as_collection()
            }
            (Materialize, None, Some(time_limit)) => {
                let yield_fn = move |start: Instant, _work| start.elapsed() >= time_limit;
                mz_join_core(arranged1, arranged2, shutdown_token, result, yield_fn).as_collection()
            }
            (Materialize, None, None) => {
                let yield_fn = |_start, _work| false;
                mz_join_core(arranged1, arranged2, shutdown_token, result, yield_fn).as_collection()
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
enum JoinedFlavor<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T> + Columnation,
    T: Timestamp + Lattice + Columnation,
{
    /// Streamed data as a collection.
    Collection(Collection<G, Row, Diff>),
    /// A dataflow-local arrangement.
    Local(Arranged<G, RowRowAgent<G::Timestamp, Diff>>),
    /// An imported arrangement.
    Trace(Arranged<G, RowRowEnter<T, Diff, G::Timestamp>>),
}

impl<G, T> Context<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T> + RenderTimestamp,
    <G::Timestamp as Columnar>::Container: Clone + Send,
    for<'a> <G::Timestamp as Columnar>::Ref<'a>: Ord + Copy,
    T: Timestamp + Lattice + Columnation,
{
    pub(crate) fn render_join(
        &self,
        inputs: Vec<CollectionBundle<G, T>>,
        linear_plan: LinearJoinPlan,
    ) -> CollectionBundle<G, T> {
        self.scope.clone().region_named("Join(Linear)", |inner| {
            self.render_join_inner(inputs, linear_plan, inner)
        })
    }

    fn render_join_inner(
        &self,
        inputs: Vec<CollectionBundle<G, T>>,
        linear_plan: LinearJoinPlan,
        inner: &mut Child<G, <G as ScopeParent>::Timestamp>,
    ) -> CollectionBundle<G, T> {
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
        let bundle = if let JoinedFlavor::Collection(mut joined) = joined {
            if let Some(closure) = linear_plan.final_closure {
                let name = "LinearJoinFinalization";
                type CB<C> = ConsolidatingContainerBuilder<C>;
                let (updates, errs) = joined.flat_map_fallible::<CB<_>, CB<_>, _, _, _, _>(name, {
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
        };
        bundle.leave_region()
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
            let name = "LinearJoinKeyPreparation";
            let (keyed, errs) = stream
                .inner
                .unary_fallible::<ColumnBuilder<((Row, Row), S::Timestamp, Diff)>, _, _, _>(
                    Pipeline,
                    name,
                    |_, _| {
                        Box::new(move |input, ok, errs| {
                            let temp_storage = RowArena::new();
                            let mut key_buf = Row::default();
                            let mut val_buf = Row::default();
                            let mut datums = DatumVec::new();
                            while let Some((time, data)) = input.next() {
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
                            }
                        })
                    },
                );

            errors.push(errs.as_collection());

            let arranged = keyed
                .mz_arrange_core::<_, Col2ValBatcher<_, _,_, _>, RowRowBuilder<_, _>, RowRowSpine<_, _>>(
                    ExchangeCore::<ColumnBuilder<_>, _>::new_core(columnar_exchange::<Row, Row, S::Timestamp, Diff>),"JoinStage"
                );
            joined = JoinedFlavor::Local(arranged);
        }

        // Demultiplex the four different cross products of arrangement types we might have.
        let arrangement = lookup_relation
            .arrangement(&lookup_key[..])
            .expect("Arrangement absent despite explicit construction");

        match joined {
            JoinedFlavor::Collection(_) => {
                unreachable!("JoinedFlavor::Collection variant avoided at top of method");
            }
            JoinedFlavor::Local(local) => match arrangement {
                ArrangementFlavor::Local(oks, errs1) => {
                    let (oks, errs2) = self
                        .differential_join_inner::<_, RowRowAgent<_, _>, RowRowAgent<_, _>>(
                            local, oks, closure,
                        );

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = self
                        .differential_join_inner::<_, RowRowAgent<_, _>, RowRowEnter<_, _, _>>(
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
                        .differential_join_inner::<_, RowRowEnter<_, _, _>, RowRowAgent<_, _>>(
                            trace, oks, closure,
                        );

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = self
                        .differential_join_inner::<_, RowRowEnter<_, _, _>, RowRowEnter<_, _, _>>(
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
    fn differential_join_inner<S, Tr1, Tr2>(
        &self,
        prev_keyed: Arranged<S, Tr1>,
        next_input: Arranged<S, Tr2>,
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
        for<'a> Tr1::Key<'a>: ToDatumIter,
        for<'a> Tr1::Val<'a>: ToDatumIter,
        for<'a> Tr2::Val<'a>: ToDatumIter,
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

                        let mut datums_local = datums.borrow();
                        datums_local.extend(key.to_datum_iter());
                        datums_local.extend(old.to_datum_iter());
                        datums_local.extend(new.to_datum_iter());

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

                    let mut datums_local = datums.borrow();
                    datums_local.extend(key.to_datum_iter());
                    datums_local.extend(old.to_datum_iter());
                    datums_local.extend(new.to_datum_iter());

                    closure
                        .apply(&mut datums_local, &temp_storage, &mut row_builder)
                        .expect("Closure claimed to never error")
                },
            );

            (oks, None)
        }
    }
}
