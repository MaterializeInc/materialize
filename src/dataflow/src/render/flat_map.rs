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
use expr::RelationExpr;
use repr::{Datum, Row, RowArena};

use crate::operator::CollectionExt;
use crate::render::context::Context;

impl<G, T> Context<G, RelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Renders `relation_expr` followed by `map_filter_project` if provided.
    pub fn render_flat_map(
        &mut self,
        relation_expr: &RelationExpr,
        map_filter_project: Option<expr::MapFilterProject>,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>) {
        if let RelationExpr::FlatMap {
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
                    let mut row_packer = repr::RowPacker::new();
                    move |input_row| {
                        // Unpack datums and capture its length (to rewind MFP eval).
                        let mut datums = input_row.unpack();
                        let datums_len = datums.len();
                        // Determine which columns will be replaced with Dummy values.
                        let replace = replace.clone();
                        let temp_storage = RowArena::new();
                        let exprs = exprs
                            .iter()
                            .map(|e| e.eval(&datums, &temp_storage))
                            .collect::<Result<Vec<_>, _>>();
                        let exprs = match exprs {
                            Ok(exprs) => exprs,
                            Err(e) => return vec![(Err(e.into()), 1)],
                        };
                        let output_rows = func.eval(exprs, &temp_storage);
                        // Blank out entries in `datum` here, for simplicity later on.
                        for index in 0..datums_len {
                            if replace[index] {
                                datums[index] = Datum::Dummy;
                            }
                        }
                        // Declare borrows outside the closure so that appropriately lifetimed
                        // borrows are moved in and used by `mfp.evaluate`.
                        let map_filter_project = &map_filter_project;
                        let row_packer = &mut row_packer;
                        let temp_storage = &temp_storage;
                        output_rows
                            .iter()
                            .filter_map(move |(output_row, r)| {
                                if let Some(mfp) = &map_filter_project {
                                    // Remove any additional columns added in prior evaluation.
                                    datums.truncate(datums_len);
                                    // Extend datums with additional columns, replace some with dummy values.
                                    datums.extend(output_row.iter());
                                    for index in datums_len..datums.len() {
                                        if replace[index] {
                                            datums[index] = Datum::Dummy;
                                        }
                                    }
                                    mfp.evaluate(&mut datums, temp_storage, row_packer)
                                        .transpose()
                                        .map(|x| (x.map_err(DataflowError::from), *r))
                                } else {
                                    Some((
                                        Ok::<_, DataflowError>(
                                            row_packer.pack(
                                                datums
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
                                            ),
                                        ),
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
