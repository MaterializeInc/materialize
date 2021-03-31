// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use dataflow_types::*;
use expr::MirRelationExpr;
use repr::{Datum, Row, RowArena};

use crate::operator::CollectionExt;
use crate::render::context::Context;
use crate::render::datum_vec::DatumVec;

impl<G, T> Context<G, MirRelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Renders `relation_expr` followed by `map_filter_project` if provided.
    pub fn render_flat_map(
        &mut self,
        relation_expr: &MirRelationExpr,
        map_filter_project: Option<expr::MapFilterProject>,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>) {
        if let MirRelationExpr::FlatMap {
            input,
            func,
            exprs,
            demand,
        } = relation_expr
        {
            let func = func.clone();
            let exprs = exprs.clone();

            // Determine for each output column if it should be replaced by a
            // small default value. This information comes from the "demand"
            // analysis, and is meant to allow us to avoid reproducing the
            // input in each output, if at all possible.
            let types = relation_expr.typ();
            let arity = types.column_types.len();
            let replace = (0..arity)
                .map(|col| !demand.as_ref().map(|d| d.contains(&col)).unwrap_or(true))
                .collect::<Vec<_>>();

            let (ok_collection, err_collection) = self.collection(input).unwrap();
            let (ok_collection, new_err_collection) =
                ok_collection.explode_fallible({
                    let mut datums = DatumVec::new();
                    let mut row_packer = repr::Row::default();
                    move |input_row| {
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
                            Err(e) => return vec![(Err(e.into()), 1)],
                        };
                        let output_rows = func.eval(exprs, &temp_storage);
                        // Blank out entries in `datum` here, for simplicity later on.
                        for index in 0..datums_len {
                            if replace[index] {
                                datums_local[index] = Datum::Dummy;
                            }
                        }
                        // Declare borrows outside the closure so that appropriately lifetimed
                        // borrows are moved in and used by `mfp.evaluate`.
                        let map_filter_project = &map_filter_project;
                        let row_packer = &mut row_packer;
                        let temp_storage = &temp_storage;
                        let replace = &replace;
                        output_rows
                            .iter()
                            .filter_map(move |(output_row, r)| {
                                if let Some(mfp) = map_filter_project {
                                    // Remove any additional columns added in prior evaluation.
                                    datums_local.truncate(datums_len);
                                    // Extend datums with additional columns, replace some with dummy values.
                                    datums_local.extend(output_row.iter());
                                    for index in datums_len..datums_local.len() {
                                        if replace[index] {
                                            datums_local[index] = Datum::Dummy;
                                        }
                                    }
                                    mfp.evaluate(&mut datums_local, temp_storage)
                                        .transpose()
                                        .map(|x| (x.map_err(DataflowError::from), *r))
                                } else {
                                    Some((
                                        Ok::<_, DataflowError>({
                                            row_packer.extend(
                                                datums_local
                                                    .iter()
                                                    .cloned()
                                                    .chain(output_row.iter())
                                                    .zip(replace.iter())
                                                    .map(|(datum, replace)| {
                                                        if *replace {
                                                            Datum::Dummy
                                                        } else {
                                                            datum
                                                        }
                                                    }),
                                            );
                                            row_packer.finish_and_reuse()
                                        }),
                                        *r,
                                    ))
                                }
                            })
                            .collect::<Vec<_>>()
                        // The collection avoids the lifetime issues of the `datums` borrow,
                        // which allows us to avoid multiple unpackings of `input_row`. We
                        // could avoid this allocation with a custom iterator that understands
                        // the borrowing, but it probably isn't the leading order issue here.
                    }
                });
            let err_collection = err_collection.concat(&new_err_collection);
            (ok_collection, err_collection)
        } else {
            panic!("Non-FlatMap expression provided to `render_flat_map`");
        }
    }
}
