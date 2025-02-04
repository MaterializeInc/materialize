// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use columnar::Columnar;
use differential_dataflow::consolidation::ConsolidatingContainerBuilder;
use mz_expr::MfpPlan;
use mz_expr::{MapFilterProject, MirScalarExpr, TableFunc};
use mz_repr::{DatumVec, RowArena, SharedRow};
use mz_repr::{Diff, Row, Timestamp};
use mz_timely_util::operator::StreamExt;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::pushers::buffer::Session;
use timely::dataflow::channels::pushers::{Counter, Tee};
use timely::dataflow::Scope;
use timely::progress::Antichain;

use crate::render::context::{CollectionBundle, Context};
use crate::render::DataflowError;

impl<G> Context<G>
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
    <G::Timestamp as Columnar>::Container: Clone + Send,
{
    /// Applies a `TableFunc` to every row, followed by an `mfp`.
    pub fn render_flat_map(
        &self,
        input: CollectionBundle<G>,
        func: TableFunc,
        exprs: Vec<MirScalarExpr>,
        mfp: MapFilterProject,
        input_key: Option<Vec<MirScalarExpr>>,
    ) -> CollectionBundle<G> {
        let until = self.until.clone();
        let mfp_plan = mfp.into_plan().expect("MapFilterProject planning failed");
        let (ok_collection, err_collection) =
            input.as_specific_collection(input_key.as_deref(), &self.config_set);
        let stream = ok_collection.inner;
        let (oks, errs) = stream.unary_fallible(Pipeline, "FlatMapStage", move |_, _| {
            Box::new(move |input, ok_output, err_output| {
                let mut datums = DatumVec::new();
                let mut datums_mfp = DatumVec::new();

                // Buffer for extensions to `input_row`.
                let mut table_func_output = Vec::new();

                input.for_each(|cap, data| {
                    let mut ok_session = ok_output.session_with_builder(&cap);
                    let mut err_session = err_output.session_with_builder(&cap);

                    'input: for (input_row, time, diff) in data.drain(..) {
                        let temp_storage = RowArena::new();

                        // Unpack datums for expression evaluation.
                        let datums_local = datums.borrow_with(&input_row);
                        let args = exprs
                            .iter()
                            .map(|e| e.eval(&datums_local, &temp_storage))
                            .collect::<Result<Vec<_>, _>>();
                        let args = match args {
                            Ok(args) => args,
                            Err(e) => {
                                err_session.give((e.into(), time, diff));
                                continue 'input;
                            }
                        };
                        let mut extensions = match func.eval(&args, &temp_storage) {
                            Ok(exts) => exts.fuse(),
                            Err(e) => {
                                err_session.give((e.into(), time, diff));
                                continue 'input;
                            }
                        };

                        // Draw additional columns out of the table func evaluation.
                        while let Some((extension, output_diff)) = extensions.next() {
                            table_func_output.push((extension, output_diff));
                            table_func_output.extend((&mut extensions).take(1023));
                            // We could consolidate `table_func_output`, but it seems unlikely to be productive.
                            drain_through_mfp(
                                &input_row,
                                &time,
                                &diff,
                                &mut datums_mfp,
                                &table_func_output,
                                &mfp_plan,
                                &until,
                                &mut ok_session,
                                &mut err_session,
                            );
                            table_func_output.clear();
                        }
                    }
                })
            })
        });

        use differential_dataflow::AsCollection;
        let ok_collection = oks.as_collection();
        let new_err_collection = errs.as_collection();
        let err_collection = err_collection.concat(&new_err_collection);
        CollectionBundle::from_collections(ok_collection, err_collection)
    }
}

/// Drains a list of extensions to `input_row` through a supplied `MfpPlan` and into output buffers.
///
/// The method decodes `input_row`, and should be amortized across non-trivial `extensions`.
fn drain_through_mfp<T>(
    input_row: &Row,
    input_time: &T,
    input_diff: &Diff,
    datum_vec: &mut DatumVec,
    extensions: &[(Row, Diff)],
    mfp_plan: &MfpPlan,
    until: &Antichain<Timestamp>,
    ok_output: &mut Session<
        T,
        ConsolidatingContainerBuilder<Vec<(Row, T, Diff)>>,
        Counter<T, Vec<(Row, T, Diff)>, Tee<T, Vec<(Row, T, Diff)>>>,
    >,
    err_output: &mut Session<
        T,
        ConsolidatingContainerBuilder<Vec<(DataflowError, T, Diff)>>,
        Counter<T, Vec<(DataflowError, T, Diff)>, Tee<T, Vec<(DataflowError, T, Diff)>>>,
    >,
) where
    T: crate::render::RenderTimestamp,
    <T as Columnar>::Container: Clone + Send,
{
    let temp_storage = RowArena::new();
    let binding = SharedRow::get();
    let mut row_builder = binding.borrow_mut();

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
            diff * *input_diff,
            |time| !until.less_equal(time),
            &mut row_builder,
        );

        for result in results {
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
