// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;

use columnar::{Columnar, Index};
use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use mz_compute_types::dyncfgs::COMPUTE_FLAT_MAP_FUEL;
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_expr::TableFunc;
use mz_expr::{Eval, MfpPlan};
use mz_repr::{DatumVec, RowArena, SharedRow};
use mz_repr::{Diff, Row, RowRef, Timestamp};
use mz_timely_util::columnar::Column;
use mz_timely_util::operator::StreamExt;
use timely::Container;
use timely::container::DrainContainer;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::generic::Session;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;

use crate::render::RenderTimestamp;
use crate::render::columnar::CollectionEdge;
use crate::render::context::{CollectionBundle, Context};
use crate::render::errors::DataflowErrorSer;

impl<'scope, T: crate::render::RenderTimestamp> Context<'scope, T> {
    /// Applies a `TableFunc` to every row, followed by an `mfp`.
    pub fn render_flat_map(
        &self,
        input_key: Option<Vec<LirScalarExpr>>,
        input: CollectionBundle<'scope, T>,
        exprs: Vec<LirScalarExpr>,
        func: TableFunc,
        mfp_plan: MfpPlan<LirScalarExpr>,
    ) -> CollectionBundle<'scope, T> {
        let until = self.until.clone();
        let scope = input.scope();

        // Budget to limit the number of rows processed in a single invocation.
        //
        // The current implementation can only yield between input batches, but not from within
        // a batch. A `generate_series` can still cause unavailability if it generates many rows.
        let budget = COMPUTE_FLAT_MAP_FUEL.get(&self.config_set);

        // The unarranged path (no key) reads the input `CollectionEdge` directly,
        // so the columnar arm never decodes rows at the input. The keyed path
        // reads an existing arrangement, already columnar internally, and is
        // presented as a `Vec` edge here.
        let (edge, err_collection) = match input_key.as_deref() {
            None => input
                .collection
                .clone()
                .expect("The unarranged collection doesn't exist."),
            Some(key) => {
                let (oks, errs) = input.as_specific_collection(Some(key), &self.config_set);
                (CollectionEdge::Vec(oks), errs)
            }
        };

        let (oks, errs) = match edge {
            CollectionEdge::Vec(c) => {
                flat_map_stage(c.inner, scope, exprs, func, mfp_plan, until, budget)
            }
            CollectionEdge::Columnar(c) => {
                flat_map_stage(c.inner, scope, exprs, func, mfp_plan, until, budget)
            }
        };

        use differential_dataflow::AsCollection;
        let ok_collection = oks.as_collection();
        let new_err_collection = errs.as_collection();
        let err_collection = err_collection.concat(new_err_collection);
        CollectionBundle::from_collections(ok_collection, err_collection)
    }
}

/// Output ok-session container builder for [`flat_map_stage`].
type FlatMapOk<T> = ConsolidatingContainerBuilder<Vec<(Row, T, Diff)>>;
/// Output err-session container builder for [`flat_map_stage`].
type FlatMapErr<T> = ConsolidatingContainerBuilder<Vec<(DataflowErrorSer, T, Diff)>>;

/// Yields the `(row, time, diff)` records of one queued FlatMap input batch.
///
/// The two edge arms differ only in how records are read from a queued batch:
/// the `Vec` arm drains owned rows, the `Column` arm iterates the borrowed
/// column and never materializes an owned [`Row`]. Everything else in the
/// FlatMap operator (the fuel queue, budget, and re-activation) is shared
/// through [`flat_map_stage`].
trait FlatMapBatch<T> {
    /// Calls `logic` once per record, presenting the row as a borrowed
    /// [`RowRef`] and the time and diff by reference.
    fn for_each_record(&mut self, logic: impl FnMut(&RowRef, &T, &Diff));
}

impl<T: RenderTimestamp> FlatMapBatch<T> for Vec<(Row, T, Diff)> {
    fn for_each_record(&mut self, mut logic: impl FnMut(&RowRef, &T, &Diff)) {
        for (row, time, diff) in self.drain(..) {
            logic(&row, &time, &diff);
        }
    }
}

impl<T: RenderTimestamp> FlatMapBatch<T> for Column<(Row, T, Diff)> {
    fn for_each_record(&mut self, mut logic: impl FnMut(&RowRef, &T, &Diff)) {
        // Rows are read from the borrowed column, never materialized as owned
        // `Row`s. Times and diffs are owned only to hand `logic` a reference.
        for (row, t, d) in self.borrow().into_index_iter() {
            logic(row, &Columnar::into_owned(t), &Columnar::into_owned(d));
        }
    }
}

/// The fueled FlatMap operator, generic over the input edge arm.
///
/// This is the sole owner of the fuel machinery, so the `Vec` and `Column`
/// arms cannot drift apart. Incoming batches are queued; each activation
/// processes queued batches and drains each record's table-function expansion
/// through the mfp, decrementing a per-activation `budget`. When the budget is
/// exhausted the operator re-activates itself and stops, deferring the rest of
/// the queue to a later activation. This bounds the work a single
/// `generate_series` can do before yielding the worker.
fn flat_map_stage<'scope, T, C>(
    stream: Stream<'scope, T, C>,
    scope: Scope<'scope, T>,
    exprs: Vec<LirScalarExpr>,
    func: TableFunc,
    mfp_plan: MfpPlan<LirScalarExpr>,
    until: Antichain<Timestamp>,
    budget: usize,
) -> (
    Stream<'scope, T, Vec<(Row, T, Diff)>>,
    Stream<'scope, T, Vec<(DataflowErrorSer, T, Diff)>>,
)
where
    T: RenderTimestamp,
    C: Container + DrainContainer + Clone + Default + FlatMapBatch<T> + 'static,
{
    stream.unary_fallible::<FlatMapOk<T>, FlatMapErr<T>, _, _>(
        Pipeline,
        "FlatMapStage",
        move |_, info| {
            let activator = scope.activator_for(info.address);
            let mut queue = VecDeque::new();
            Box::new(move |input, ok_output, err_output| {
                let mut datums = DatumVec::new();
                let mut datums_mfp = DatumVec::new();

                // Buffer for extensions to `input_row`.
                let mut table_func_output = Vec::new();

                let mut budget = budget;

                input.for_each(|cap, data| {
                    queue.push_back((cap.retain(0), cap.retain(1), std::mem::take(data)))
                });

                while let Some((ok_cap, err_cap, mut data)) = queue.pop_front() {
                    let mut ok_session = ok_output.session_with_builder(&ok_cap);
                    let mut err_session = err_output.session_with_builder(&err_cap);

                    data.for_each_record(|input_row, time, diff| {
                        process_flat_map_row(
                            input_row,
                            time,
                            diff,
                            &exprs,
                            &func,
                            &mfp_plan,
                            &until,
                            &mut datums,
                            &mut datums_mfp,
                            &mut table_func_output,
                            &mut ok_session,
                            &mut err_session,
                            &mut budget,
                        );
                    });
                    if budget == 0 {
                        activator.activate();
                        break;
                    }
                }
            })
        },
    )
}

/// Expands one input record's table function and drains it through the mfp.
///
/// Evaluates `exprs` to the table-function arguments, then `func`, chunking the
/// expansion so [`drain_through_mfp`] amortizes the input-row decode. Argument
/// or function evaluation errors emit to the err session and return early. The
/// output budget is decremented inside [`drain_through_mfp`].
fn process_flat_map_row<T>(
    input_row: &RowRef,
    time: &T,
    diff: &Diff,
    exprs: &[LirScalarExpr],
    func: &TableFunc,
    mfp_plan: &MfpPlan<LirScalarExpr>,
    until: &Antichain<Timestamp>,
    datums: &mut DatumVec,
    datums_mfp: &mut DatumVec,
    table_func_output: &mut Vec<(Row, Diff)>,
    ok_session: &mut Session<'_, '_, T, FlatMapOk<T>, Capability<T>>,
    err_session: &mut Session<'_, '_, T, FlatMapErr<T>, Capability<T>>,
    budget: &mut usize,
) where
    T: RenderTimestamp,
{
    let temp_storage = RowArena::new();

    // Unpack datums for expression evaluation.
    let datums_local = datums.borrow_with(input_row);
    let args = exprs
        .iter()
        .map(|e| e.eval(&datums_local, &temp_storage))
        .collect::<Result<Vec<_>, _>>();
    let args = match args {
        Ok(args) => args,
        Err(e) => {
            err_session.give((e.into(), time.clone(), *diff));
            return;
        }
    };
    let mut extensions = match func.eval(&args, &temp_storage) {
        Ok(exts) => exts.fuse(),
        Err(e) => {
            err_session.give((e.into(), time.clone(), *diff));
            return;
        }
    };

    // Draw additional columns out of the table func evaluation.
    while let Some((extension, output_diff)) = extensions.next() {
        table_func_output.push((extension, output_diff));
        table_func_output.extend((&mut extensions).take(1023));
        // We could consolidate `table_func_output`, but it seems unlikely to be productive.
        drain_through_mfp(
            input_row,
            time,
            diff,
            datums_mfp,
            table_func_output,
            mfp_plan,
            until,
            ok_session,
            err_session,
            budget,
        );
        table_func_output.clear();
    }
}

/// Drains a list of extensions to `input_row` through a supplied `MfpPlan` and into output buffers.
///
/// The method decodes `input_row`, and should be amortized across non-trivial `extensions`.
fn drain_through_mfp<T>(
    input_row: &RowRef,
    input_time: &T,
    input_diff: &Diff,
    datum_vec: &mut DatumVec,
    extensions: &[(Row, Diff)],
    mfp_plan: &MfpPlan<LirScalarExpr>,
    until: &Antichain<Timestamp>,
    ok_output: &mut Session<'_, '_, T, FlatMapOk<T>, Capability<T>>,
    err_output: &mut Session<'_, '_, T, FlatMapErr<T>, Capability<T>>,
    budget: &mut usize,
) where
    T: RenderTimestamp,
{
    let temp_storage = RowArena::new();
    let mut row_builder = SharedRow::get();

    // This is not cheap, and is meant to be amortized across many `extensions`.
    let mut datums_local = datum_vec.borrow_with(input_row);
    let datums_len = datums_local.len();

    let event_time = input_time.event_time().clone();

    for (cols, diff) in extensions.iter() {
        // Arrange `datums_local` to reflect the intended output pre-mfp.
        datums_local.truncate(datums_len);
        datums_local.extend(cols.iter());

        let results = mfp_plan.evaluate(
            &mut datums_local,
            &temp_storage,
            event_time,
            *diff * *input_diff,
            |time| !until.less_equal(time),
            &mut row_builder,
        );

        for result in results {
            *budget = budget.saturating_sub(1);
            match result {
                Ok((row, event_time, diff)) => {
                    // Copy the whole time, and re-populate event time.
                    let mut time = input_time.clone();
                    *time.event_time_mut() = event_time;
                    ok_output.give((row, time, diff));
                }
                Err((err, event_time, diff)) => {
                    // Copy the whole time, and re-populate event time.
                    let mut time = input_time.clone();
                    *time.event_time_mut() = event_time;
                    err_output.give((err, time, diff));
                }
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::input::Input;
    use mz_expr::MapFilterProject;
    use mz_repr::{Datum, ReprScalarType};
    use timely::dataflow::operators::Inspect;
    use timely::dataflow::operators::capture::{Capture, Extract};

    use super::*;
    use crate::render::columnar::vec_to_columnar;

    // `generate_series(1, stop, 1)` reading `stop` from column 1 of the input
    // row, with an identity mfp over the (start, stop, generated) triple.
    fn flat_map_args() -> (Vec<LirScalarExpr>, TableFunc, MfpPlan<LirScalarExpr>) {
        let exprs = vec![
            LirScalarExpr::column(0),
            LirScalarExpr::column(1),
            LirScalarExpr::literal_ok(Datum::Int64(1), ReprScalarType::Int64),
        ];
        let func = TableFunc::GenerateSeriesInt64;
        let mfp = MapFilterProject::<LirScalarExpr>::new(3)
            .into_plan()
            .expect("identity mfp");
        (exprs, func, mfp)
    }

    fn input_row(stop: i64) -> Row {
        Row::pack_slice(&[Datum::Int64(1), Datum::Int64(stop)])
    }

    /// Runs one input row at each of `batches` distinct timestamps through
    /// `flat_map_stage` with the given fuel `budget`, driving the worker one
    /// step at a time. Returns how many steps were needed to produce all
    /// output and the total output-record count.
    fn run_fueled(batches: u64, stop: i64, budget: usize) -> (usize, usize) {
        let expected = usize::try_from(batches).unwrap() * usize::try_from(stop).unwrap();
        timely::execute_directly(move |worker| {
            let collected = Rc::new(RefCell::new(0usize));
            let sink = Rc::clone(&collected);
            let mut input = worker.dataflow::<Timestamp, _, _>(|scope| {
                let (input, collection) = scope.new_collection();
                let (exprs, func, mfp) = flat_map_args();
                let stream = collection.inner;
                let scope = stream.scope();
                let (oks, _errs) =
                    flat_map_stage(stream, scope, exprs, func, mfp, Antichain::new(), budget);
                oks.inspect(move |_| *sink.borrow_mut() += 1);
                input
            });

            // Feed one row per timestamp as a separate batch. Distinct times
            // keep the per-batch output from consolidating away.
            for i in 0..batches {
                input.advance_to(Timestamp::from(i));
                input.update(input_row(stop), Diff::ONE);
                input.flush();
            }
            input.advance_to(Timestamp::from(batches));
            input.flush();

            let mut steps = 0;
            while *collected.borrow() < expected {
                worker.step();
                steps += 1;
                assert!(steps < 10_000, "flat map did not converge");
            }
            (steps, *collected.borrow())
        })
    }

    #[mz_ore::test]
    fn flat_map_fuel_bounds_per_activation() {
        // A fuel budget of one forces the operator to yield after each queued
        // batch and re-activate, so several batches take several activations.
        // An unbounded budget drains the whole queue in a single activation.
        // This is the availability guard the fuel machinery exists for.
        let (fueled_steps, fueled_count) = run_fueled(4, 3, 1);
        let (unfueled_steps, unfueled_count) = run_fueled(4, 3, usize::MAX);
        assert_eq!(fueled_count, 12);
        assert_eq!(unfueled_count, 12);
        assert_eq!(
            unfueled_steps, 1,
            "unbounded budget drains in one activation"
        );
        assert!(
            fueled_steps > unfueled_steps,
            "fuel budget must spread work across activations: fueled={fueled_steps} unfueled={unfueled_steps}"
        );
    }

    #[mz_ore::test]
    fn flat_map_arms_agree() {
        // The `Vec` and `Column` input arms must produce identical `(row, time,
        // diff)` output. Multiple timestamps and a retraction exercise time
        // handling and negative diffs. The columnar arm's `Columnar::into_owned`
        // for time and diff runs on every ok record here (the input decode has
        // no fallible/try_extend path, so the standing fallible-key rule does
        // not apply), and this comparison proves it decodes correctly.
        let (vec_captured, col_captured) = timely::execute_directly(move |worker| {
            worker.dataflow::<Timestamp, _, _>(|scope| {
                let (mut input, collection) = scope.new_collection();
                let mut captures = Vec::new();
                for columnar in [false, true] {
                    let (exprs, func, mfp) = flat_map_args();
                    let edge = if columnar {
                        CollectionEdge::Columnar(vec_to_columnar(collection.clone()))
                    } else {
                        CollectionEdge::Vec(collection.clone())
                    };
                    let (oks, _errs) = match edge {
                        CollectionEdge::Vec(c) => {
                            let stream = c.inner;
                            let scope = stream.scope();
                            flat_map_stage(
                                stream,
                                scope,
                                exprs,
                                func,
                                mfp,
                                Antichain::new(),
                                usize::MAX,
                            )
                        }
                        CollectionEdge::Columnar(c) => {
                            let stream = c.inner;
                            let scope = stream.scope();
                            flat_map_stage(
                                stream,
                                scope,
                                exprs,
                                func,
                                mfp,
                                Antichain::new(),
                                usize::MAX,
                            )
                        }
                    };
                    captures.push(oks.capture());
                }
                let col = captures.pop().unwrap();
                let vec = captures.pop().unwrap();
                // t=0: generate_series(1, 2); t=1: generate_series(1, 3);
                // t=2: retract the t=0 row.
                input.advance_to(Timestamp::from(0_u64));
                input.update(input_row(2), Diff::ONE);
                input.advance_to(Timestamp::from(1_u64));
                input.update(input_row(3), Diff::ONE);
                input.advance_to(Timestamp::from(2_u64));
                input.update(input_row(2), -Diff::ONE);
                input.advance_to(Timestamp::from(3_u64));
                input.flush();
                (vec, col)
            })
        });

        let extract_sorted = |captured: std::sync::mpsc::Receiver<_>| {
            let mut updates: Vec<(Row, Timestamp, Diff)> = captured
                .extract()
                .into_iter()
                .flat_map(|(_, data)| data)
                .collect();
            updates.sort();
            updates
        };
        let vec_updates = extract_sorted(vec_captured);
        assert!(!vec_updates.is_empty());
        assert!(
            vec_updates.iter().any(|(_, _, d)| *d < Diff::ZERO),
            "the retraction must survive as a negative diff"
        );
        assert_eq!(vec_updates, extract_sorted(col_captured));
    }
}
