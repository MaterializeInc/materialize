// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use timely::dataflow::Scope;

use mz_expr::{MapFilterProject, MirScalarExpr, TableFunc};
use mz_repr::{Row, RowArena};

use crate::operator::StreamExt;
use crate::render::context::CollectionBundle;
use crate::render::context::Context;
use mz_repr::DatumVec;

impl<G> Context<G, Row, mz_repr::Timestamp>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    /// Renders `relation_expr` followed by `map_filter_project` if provided.
    pub fn render_flat_map(
        &mut self,
        input: CollectionBundle<G, Row, G::Timestamp>,
        func: TableFunc,
        exprs: Vec<MirScalarExpr>,
        mfp: MapFilterProject,
        input_key: Option<Vec<MirScalarExpr>>,
    ) -> CollectionBundle<G, Row, G::Timestamp> {
        let mfp_plan = mfp.into_plan().expect("MapFilterProject planning failed");
        let (ok_collection, err_collection) = input.as_specific_collection(input_key.as_deref());
        let (oks, errs) = ok_collection.inner.flat_map_fallible("FlatMapStage", {
            let mut datums = DatumVec::new();
            let mut row_builder = Row::default();
            move |(input_row, time, diff)| {
                let temp_storage = RowArena::new();
                // Unpack datums and capture its length (to rewind MFP eval).
                let mut datums_local = datums.borrow_with(&input_row);
                let datums_len = datums_local.len();
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
                // Declare borrows outside the closure so that appropriately lifetimed
                // borrows are moved in and used by `mfp.evaluate`.
                let temp_storage = &temp_storage;
                let mfp_plan = &mfp_plan;
                let output_rows_vec: Vec<_> = output_rows.collect();
                let row_builder = &mut row_builder;
                output_rows_vec
                    .iter()
                    .flat_map(move |(output_row, r)| {
                        // Remove any additional columns added in prior evaluation.
                        datums_local.truncate(datums_len);
                        // Extend datums with additional columns, replace some with dummy values.
                        datums_local.extend(output_row.iter());
                        mfp_plan
                            .evaluate(
                                &mut datums_local,
                                temp_storage,
                                time,
                                diff * *r,
                                row_builder,
                            )
                            .collect::<Vec<_>>()
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
