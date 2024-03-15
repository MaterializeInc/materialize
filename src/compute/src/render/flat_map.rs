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
                let temp_storage = RowArena::new();

                // Buffer for extensions to `input_row`.
                let mut table_func_output = Vec::new();
                // Buffer for outputs of MFP on extended `input_row`.
                let mut output_buffer = Vec::new();

                // Unpack datums for expression evaluation.
                let datums_local = datums.borrow_with(&input_row);
                let args = exprs
                    .iter()
                    .map(|e| e.eval(&datums_local, &temp_storage))
                    .collect::<Result<Vec<_>, _>>();
                let args = match args {
                    Ok(args) => args,
                    Err(e) => return vec![(Err((e.into(), time, diff)))],
                };
                let mut extensions = match func.eval(&args, &temp_storage) {
                    Ok(exts) => exts,
                    Err(e) => return vec![(Err((e.into(), time, diff)))],
                }
                .fuse();

                // Draw additional columns out of the table func evaluation.
                while let Some((extension, output_diff)) = extensions.next() {
                    table_func_output.push((extension, output_diff));
                    table_func_output.extend((&mut extensions).take(1023));
                    // We could consolidate `table_func_output`, but it seems unlikely to be productive.
                    drain_through_mfp(
                        &input_row,
                        &mut time,
                        &diff,
                        &mut datums_mfp,
                        &table_func_output,
                        &mfp_plan,
                        &until,
                        &mut output_buffer,
                    );
                    table_func_output.clear();
                }

                output_buffer
                    .into_iter()
                    .map(|(data, time, diff)| match data {
                        Ok(row) => Ok((row, time, diff)),
                        Err(err) => Err((err, time, diff)),
                    })
                    .collect::<Vec<_>>()
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
///
/// The method decodes `input_row`, and should be amortized across non-trivial `extensions`.
fn drain_through_mfp<T>(
    input_row: &Row,
    input_time: &mut T,
    input_diff: &Diff,
    datum_vec: &mut DatumVec,
    extensions: &[(Row, Diff)],
    mfp_plan: &MfpPlan,
    until: &Antichain<Timestamp>,
    output: &mut Vec<(Result<Row, DataflowError>, T, Diff)>,
) where
    T: crate::render::RenderTimestamp,
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
            let update = match result {
                Ok((row, event_time, diff)) => {
                    // Copy the whole time, and re-populate event time.
                    let mut time = input_time.clone();
                    *time.event_time() = event_time;
                    (Ok(row), time, diff)
                }
                Err((err, event_time, diff)) => {
                    // Copy the whole time, and re-populate event time.
                    let mut time = input_time.clone();
                    *time.event_time() = event_time;
                    (Err(err), time, diff)
                }
            };
            output.push(update);
            if output.len() == output.capacity() {
                differential_dataflow::consolidation::consolidate_updates(output);
                if output.len() > output.capacity() / 2 {
                    output.reserve(2 * output.capacity() - output.len());
                }
            }
        }
    }
}
