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

use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::{AsCollection, Collection, Data};
use mz_compute_types::dyncfgs::{ENABLE_MZ_JOIN_CORE, LINEAR_JOIN_YIELDING};
use mz_compute_types::plan::join::linear_join::{LinearJoinPlan, LinearStagePlan};
use mz_compute_types::plan::join::JoinClosure;
use mz_dyncfg::ConfigSet;
use mz_expr::SafeMfpPlan;
use mz_repr::fixed_length::ToDatumIter;
use mz_repr::{DatumVec, Diff, Row, RowArena, SharedRow};
use mz_storage_types::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;
use timely::container::columnation::Columnation;
use timely::container::CapacityContainerBuilder;
use timely::dataflow::operators::OkErr;
use timely::dataflow::scopes::Child;
use timely::dataflow::{Scope, ScopeParent};
use timely::progress::timestamp::{Refines, Timestamp};

use crate::extensions::arrange::MzArrange;
use crate::render::context::{
    ArrangementFlavor, CollectionBundle, Context, MzArrangement, MzArrangementImport, ShutdownToken,
};
use crate::render::join::mz_join_core::mz_join_core;
use crate::row_spine::RowRowSpine;
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
            let parts: Vec<_> = option.split(':').map(|p| p.trim()).collect();
            match &parts[..] {
                ["work", amount] => {
                    let amount = amount.parse().ok()?;
                    after_work = Some(amount);
                }
                ["time", millis] => {
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
#[derive(Clone)]
enum JoinedFlavor<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T> + Columnation,
    T: Timestamp + Lattice + Columnation,
{
    /// Streamed data as a collection.
    Collection(Collection<G, Row, Diff>),
    /// A dataflow-local arrangement.
    Local(MzArrangement<G>),
    /// An imported arrangement.
    Trace(MzArrangementImport<G, T>),
}

impl<G, T> Context<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T> + Columnation,
    T: Timestamp + Lattice + Columnation,
{
    /// Accepts input to render as a list of lists, where each outer list element
    /// is a join argument, each of which is a list of terms to union together as
    /// an input to the join. Each term comes with an optional `SafeMfpPlan` that
    /// will be applied to the term before it is joined, and a boolean indicating
    /// whethere the results should be negated.
    ///
    /// Each optional plan is applied to rows laid out in `[key, old, new]` order,
    /// and their column references should reflect this. This means that the plan
    /// for the first input should only reference `key` and `old` columns, and the
    /// plans for subsequent inputs should only reference `key` and `new` columns.
    pub(crate) fn render_join(
        &mut self,
        inputs: Vec<Vec<(CollectionBundle<G, T>, Option<SafeMfpPlan>, bool)>>,
        linear_plan: LinearJoinPlan,
    ) -> CollectionBundle<G, T> {
        self.scope.clone().region_named("Join(Linear)", |inner| {
            self.render_join_inner(inputs, linear_plan, inner)
        })
    }

    fn render_join_inner(
        &mut self,
        inputs: Vec<Vec<(CollectionBundle<G, T>, Option<SafeMfpPlan>, bool)>>,
        linear_plan: LinearJoinPlan,
        inner: &mut Child<G, <G as ScopeParent>::Timestamp>,
    ) -> CollectionBundle<G, T> {
        // Collect all error streams, and concatenate them at the end.
        let mut errors = Vec::new();

        // Determine which form our maintained spine of updates will initially take.
        // First, just check out the availability of an appropriate arrangement.
        // This will be `None` in the degenerate single-input join case, which ensures
        // that we do not panic if we never go around the `stage_plans` loop.
        let mut joined = Vec::new();

        for (input, mfp, negate) in inputs[linear_plan.source_relation].iter() {
            let arrangement = linear_plan
                .stage_plans
                .get(0)
                .and_then(|stage| input.arrangement(&stage.stream_key));
            // We can use an arrangement if it exists and an initial closure does not.
            let start = match (arrangement, linear_plan.initial_closure.clone()) {
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
                    let (joined, errs) =
                        input.as_specific_collection(linear_plan.source_key.as_deref());
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
                        let (j, errs) =
                            joined.flat_map_fallible::<CB<_>, CB<_>, _, _, _, _>(name, {
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

            joined.push((start, mfp.clone(), *negate));
        }

        // progress through stages, updating partial results and errors.
        for stage_plan in linear_plan.stage_plans.into_iter() {
            // Different variants of `joined` implement this differently,
            // and the logic is centralized there.
            let mut streams = Vec::new();
            for (prev, prev_mfp, prev_neg) in joined {
                for (next, next_mfp, next_neg) in inputs[stage_plan.lookup_relation].iter() {
                    let mut stream = self.differential_join(
                        prev.clone(),
                        prev_mfp.clone(),
                        next.enter_region(inner),
                        next_mfp.clone(),
                        stage_plan.clone(),
                        &mut errors,
                    );
                    if prev_neg != *next_neg {
                        stream = stream.negate();
                    }
                    streams.push(stream);
                }
            }
            // Update joined results and capture any errors.
            if streams.len() == 1 {
                joined = vec![(
                    JoinedFlavor::Collection(streams.pop().unwrap()),
                    None,
                    false,
                )];
            } else {
                joined = vec![(
                    JoinedFlavor::Collection(streams.pop().unwrap().concatenate(streams)),
                    None,
                    false,
                )];
            }
        }

        // We have completed the join building, but may have work remaining.
        // For example, we may have expressions not pushed down (e.g. literals)
        // and projections that could not be applied (e.g. column repetition).
        let bundle = if let (JoinedFlavor::Collection(mut joined), None, false) =
            joined.pop().unwrap()
        {
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
        joined_mfp: Option<SafeMfpPlan>,
        lookup_relation: CollectionBundle<S, T>,
        lookup_mfp: Option<SafeMfpPlan>,
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
            type CB<C> = CapacityContainerBuilder<C>;
            let (keyed, errs) = stream.map_fallible::<CB<_>, CB<_>, _, _, _>(name, {
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
            joined = JoinedFlavor::Local(MzArrangement::RowRow(arranged));
        }

        // Demultiplex the four different cross products of arrangement types we might have.
        let arrangement = lookup_relation
            .arrangement(&lookup_key[..])
            .expect("Arrangement absent despite explicit construction");

        use MzArrangement as A;
        use MzArrangementImport as I;

        match joined {
            JoinedFlavor::Collection(_) => {
                unreachable!("JoinedFlavor::Collection variant avoided at top of method");
            }
            JoinedFlavor::Local(local) => match arrangement {
                ArrangementFlavor::Local(oks, errs1) => {
                    let (oks, errs2) = match (local, oks) {
                        (A::RowRow(prev_keyed), A::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowRowAgent<_, _>, RowRowAgent<_, _>>(
                                prev_keyed, joined_mfp, next_input, lookup_mfp, closure,
                            ),
                    };

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = match (local, oks) {
                        (A::RowRow(prev_keyed), I::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowRowAgent<_, _>, RowRowEnter<_, _, _>>(
                                prev_keyed, joined_mfp, next_input, lookup_mfp, closure,
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
                        (I::RowRow(prev_keyed), A::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowRowEnter<_, _, _>, RowRowAgent<_, _>>(
                                prev_keyed, joined_mfp, next_input, lookup_mfp, closure,
                            ),
                    };

                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.extend(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = match (trace, oks) {
                        (I::RowRow(prev_keyed), I::RowRow(next_input)) => self
                            .differential_join_inner::<_, RowRowEnter<_, _, _>, RowRowEnter<_, _, _>>(
                                prev_keyed, joined_mfp, next_input, lookup_mfp, closure,
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
        prev_mfp: Option<SafeMfpPlan>,
        next_input: Arranged<S, Tr2>,
        next_mfp: Option<SafeMfpPlan>,
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

        let mut could_error = closure.could_error();
        if let Some(mfp) = prev_mfp.as_ref() {
            could_error |= mfp.could_error();
        }
        if let Some(mfp) = next_mfp.as_ref() {
            could_error |= mfp.could_error();
        }

        if could_error {
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

                        let key = key.to_datum_iter();
                        let old = old.to_datum_iter();
                        let new = new.to_datum_iter();

                        let mut datums_local = datums.borrow();
                        datums_local.extend(key);
                        datums_local.extend(old);
                        if let Some(mfp) = prev_mfp.as_ref() {
                            let start = datums_local.len();
                            let result = mfp.evaluate_inner(&mut datums_local, &temp_storage);
                            let added = datums_local.len();
                            // We need to apply mfp.projection, which `evaluate_inner` does not do.
                            // We assume `mfp.projection[..mfp.input_arity]` to be `0 .. mfp.input_arity`.
                            // Any columns added by `mfp.expressions` must be dropped.
                            for col in mfp.projection[mfp.input_arity..].iter() {
                                let datum = datums_local[*col].clone();
                                datums_local.push(datum);
                            }
                            // Drain columns added my `mfp.expressions`.
                            datums_local.drain(start..added);

                            match result {
                                Ok(false) => {
                                    return None;
                                }
                                Err(err) => {
                                    return Some(Err(DataflowError::from(err)));
                                }
                                _ => {}
                            }
                        }
                        datums_local.extend(new);
                        if let Some(mfp) = next_mfp.as_ref() {
                            let start = datums_local.len();
                            let result = mfp.evaluate_inner(&mut datums_local, &temp_storage);
                            let added = datums_local.len();
                            // We need to apply mfp.projection, which `evaluate_inner` does not do.
                            // We assume `mfp.projection[..mfp.input_arity]` to be `0 .. mfp.input_arity`.
                            // Any columns added by `mfp.expressions` must be dropped.
                            for col in mfp.projection[mfp.input_arity..].iter() {
                                let datum = datums_local[*col].clone();
                                datums_local.push(datum);
                            }
                            // Drain columns added my `mfp.expressions`.
                            datums_local.drain(start..added);

                            match result {
                                Ok(false) => {
                                    return None;
                                }
                                Err(err) => {
                                    return Some(Err(DataflowError::from(err)));
                                }
                                _ => {}
                            }
                        }

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

                    let key = key.to_datum_iter();
                    let old = old.to_datum_iter();
                    let new = new.to_datum_iter();

                    let mut datums_local = datums.borrow();
                    datums_local.extend(key);
                    datums_local.extend(old);
                    if let Some(mfp) = prev_mfp.as_ref() {
                        let start = datums_local.len();
                        let result = mfp.evaluate_inner(&mut datums_local, &temp_storage);
                        let added = datums_local.len();
                        // We need to apply mfp.projection, which `evaluate_inner` does not do.
                        // We assume `mfp.projection[..mfp.input_arity]` to be `0 .. mfp.input_arity`.
                        // Any columns added by `mfp.expressions` must be dropped.
                        for col in mfp.projection[mfp.input_arity..].iter() {
                            let datum = datums_local[*col].clone();
                            datums_local.push(datum);
                        }
                        // Drain columns added my `mfp.expressions`.
                        datums_local.drain(start..added);

                        match result {
                            Ok(false) => {
                                return None;
                            }
                            Err(err) => {
                                panic!("unexpected error: {err}")
                            }
                            _ => {}
                        }
                    }
                    datums_local.extend(new);
                    if let Some(mfp) = next_mfp.as_ref() {
                        let start = datums_local.len();
                        let result = mfp.evaluate_inner(&mut datums_local, &temp_storage);
                        let added = datums_local.len();
                        // We need to apply mfp.projection, which `evaluate_inner` does not do.
                        // We assume `mfp.projection[..mfp.input_arity]` to be `0 .. mfp.input_arity`.
                        // Any columns added by `mfp.expressions` must be dropped.
                        for col in mfp.projection[mfp.input_arity..].iter() {
                            let datum = datums_local[*col].clone();
                            datums_local.push(datum);
                        }
                        // Drain columns added my `mfp.expressions`.
                        datums_local.drain(start..added);

                        match result {
                            Ok(false) => {
                                return None;
                            }
                            Err(err) => {
                                panic!("unexpected error: {err}")
                            }
                            _ => {}
                        }
                    }
                    closure
                        .apply(&mut datums_local, &temp_storage, &mut row_builder)
                        .expect("Closure claimed to never error")
                },
            );

            (oks, None)
        }
    }
}
