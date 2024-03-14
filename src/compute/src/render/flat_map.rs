// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr::{MapFilterProject, MirScalarExpr, TableFunc};
use mz_repr::{DatumVec, RowArena, SharedRow};
use mz_timely_util::operator::StreamExt;
use timely::dataflow::Scope;

use crate::render::context::{CollectionBundle, Context};

impl<G> Context<G>
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
{
    /// Renders `relation_expr` followed by `map_filter_project` if provided.
    pub fn render_flat_map(
        &mut self,
        input: CollectionBundle<G>,
        func: TableFunc,
        exprs: Vec<MirScalarExpr>,
        mfp: MapFilterProject,
        input_key: Option<Vec<MirScalarExpr>>,
    ) -> CollectionBundle<G> {
        let until = self.until.clone();
        let mfp_plan = mfp.into_plan().expect("MapFilterProject planning failed");
        let (ok_collection, err_collection) = input.as_specific_collection(input_key.as_deref());
        let (oks, errs) = ok_collection.inner.flat_map_fallible("FlatMapStage", {
            let mut datums = DatumVec::new();
            let mut datums_mfp = DatumVec::new();

            move |(input_row, mut time, diff)| {
                let mut table_func_output = Vec::new();

                // Into which we accumulate result update triples.
                let mut oks_output = Vec::new();
                let mut err_output = Vec::new();

                let temp_storage = RowArena::new();

                // Unpack datums for expression evaluation.
                let datums_local = datums.borrow_with(&input_row);
                let exprs = exprs
                    .iter()
                    .map(|e| e.eval(&datums_local, &temp_storage))
                    .collect::<Result<Vec<_>, _>>();
                let exprs = match exprs {
                    Ok(exprs) => exprs,
                    Err(e) => return vec![(Err((e.into(), time, diff)))],
                };
                let output_rows = match func.eval(&exprs, &temp_storage) {
                    Ok(exprs) => exprs,
                    Err(e) => return vec![(Err((e.into(), time, diff)))],
                };

                // Draw rows out of the table func evaluation.
                // Consolidate them as we acculumate them, and when we are "full" apply
                // the MFP and record and perhaps consolidate the results.
                for (output_row, r) in output_rows {
                    table_func_output.push((output_row, r));
                    if table_func_output.len() == table_func_output.capacity() {
                        // Consolidate the collection of things we will append to `input_row`.
                        differential_dataflow::consolidation::consolidate(&mut table_func_output);
                        if table_func_output.len() > table_func_output.capacity() / 2 {
                            drain_through_mfp(
                                &input_row,
                                &mut time,
                                &diff,
                                &mut datums_mfp,
                                &table_func_output,
                                &mfp_plan,
                                &until,
                                &mut oks_output,
                                &mut err_output,
                            );

                            // Double the capacity of the table func output buffer.
                            table_func_output.clear();
                            table_func_output.reserve(2 * table_func_output.capacity());
                        }
                    }
                }

                // Drain any straggler extensions.
                if !table_func_output.is_empty() {
                    drain_through_mfp(
                        &input_row,
                        &mut time,
                        &diff,
                        &mut datums_mfp,
                        &table_func_output,
                        &mfp_plan,
                        &until,
                        &mut oks_output,
                        &mut err_output,
                    );
                }
                // About to be dropped, but here to remind us in case we ever re-use it.
                table_func_output.clear();

                let oks = oks_output.into_iter().map(Ok);
                let err = err_output.into_iter().map(Err);
                oks.chain(err).collect::<Vec<_>>()
            }
        });

        use differential_dataflow::AsCollection;
        let ok_collection = oks.as_collection();
        let new_err_collection = errs.as_collection();
        let err_collection = err_collection.concat(&new_err_collection);
        CollectionBundle::from_collections(ok_collection, err_collection)
    }
}

use crate::render::DataflowError;
use mz_expr::MfpPlan;
use mz_repr::{Diff, Row, Timestamp};
use timely::progress::Antichain;

/// Drains a list of extensions to `input_row` through a supplied `MfpPlan` and into output buffers.
fn drain_through_mfp<T>(
    input_row: &Row,
    input_time: &mut T,
    input_diff: &Diff,
    datum_vec: &mut DatumVec,
    extensions: &[(Row, Diff)],
    mfp_plan: &MfpPlan,
    until: &Antichain<Timestamp>,
    oks_output: &mut Vec<(Row, T, Diff)>,
    err_output: &mut Vec<(DataflowError, T, Diff)>,
) where
    T: crate::render::RenderTimestamp,
{
    let temp_storage = RowArena::new();
    let binding = SharedRow::get();
    let mut row_builder = binding.borrow_mut();

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
                    *time.event_time() = event_time;
                    oks_output.push((row, time, diff));
                    if oks_output.len() == oks_output.capacity() {
                        differential_dataflow::consolidation::consolidate_updates(oks_output);
                        if oks_output.len() > oks_output.capacity() / 2 {
                            oks_output.reserve(2 * oks_output.capacity() - oks_output.len());
                        }
                    }
                }
                Err((err, event_time, diff)) => {
                    // Copy the whole time, and re-populate event time.
                    let mut time = input_time.clone();
                    *time.event_time() = event_time;
                    err_output.push((err, time, diff));
                    if err_output.len() == err_output.capacity() {
                        differential_dataflow::consolidation::consolidate_updates(err_output);
                        if err_output.len() > err_output.capacity() / 2 {
                            err_output.reserve(2 * err_output.capacity() - err_output.len());
                        }
                    }
                }
            }
        }
    }
}
