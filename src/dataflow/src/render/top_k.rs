// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use expr::{Diff, RelationExpr};
use repr::Row;

use crate::render::context::Context;

impl<G, T> Context<G, RelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub fn render_topk(&mut self, relation_expr: &RelationExpr) {
        if let RelationExpr::TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
        } = relation_expr
        {
            use differential_dataflow::operators::reduce::Reduce;

            // self.ensure_rendered(input, scope, worker_index);
            let (ok_input, err_input) = self.collection(input).unwrap();

            // To provide a robust incremental orderby-limit experience, we want to avoid grouping
            // *all* records (or even large groups) and then applying the ordering and limit. Instead,
            // a more robust approach forms groups of bounded size (here, 16) and applies the offset
            // and limit to each, and then increases the sizes of the groups.

            // Builds a "stage", which uses a finer grouping than is required to reduce the volume of
            // updates, and to reduce the amount of work on the critical path for updates. The cost is
            // a larger number of arrangements when this optimization does nothing beneficial.
            fn build_topk_stage<G>(
                collection: Collection<G, ((Row, u64), Row), Diff>,
                order_key: &[expr::ColumnOrder],
                modulus: u64,
                offset: usize,
                limit: Option<usize>,
            ) -> Collection<G, ((Row, u64), Row), Diff>
            where
                G: Scope,
                G::Timestamp: Lattice,
            {
                let order_clone = order_key.to_vec();

                collection
                    .map(move |((key, hash), row)| ((key, hash % modulus), row))
                    .reduce_named("TopK", {
                        move |_key, source, target| {
                            target.extend(source.iter().map(|&(row, diff)| (row.clone(), diff)));
                            let must_shrink = offset > 0
                                || limit
                                    .map(|l| {
                                        target.iter().map(|(_, d)| *d).sum::<isize>() as usize > l
                                    })
                                    .unwrap_or(false);
                            if must_shrink {
                                if !order_clone.is_empty() {
                                    //todo: use arrangements or otherwise make the sort more performant?
                                    let sort_by = |left: &(Row, isize), right: &(Row, isize)| {
                                        expr::compare_columns(
                                            &order_clone,
                                            &left.0.unpack(),
                                            &right.0.unpack(),
                                            || left.cmp(right),
                                        )
                                    };
                                    target.sort_by(sort_by);
                                }

                                let mut skipped = 0; // Number of records offset so far
                                let mut output = 0; // Number of produced output records.
                                let mut cursor = 0; // Position of current input record.

                                //skip forward until an offset number of records is reached
                                while cursor < target.len() {
                                    if skipped + (target[cursor].1 as usize) > offset {
                                        break;
                                    }
                                    skipped += target[cursor].1 as usize;
                                    cursor += 1;
                                }
                                let skip_cursor = cursor;
                                if cursor < target.len() {
                                    if skipped < offset {
                                        //if offset only skips some members of a group of identical
                                        //records, return the rest
                                        target[skip_cursor].1 -= (offset - skipped) as isize;
                                    }
                                    //apply limit
                                    if let Some(limit) = limit {
                                        while output < limit && cursor < target.len() {
                                            let to_emit = std::cmp::min(
                                                limit - output,
                                                target[cursor].1 as usize,
                                            );
                                            target[cursor].1 = to_emit as isize;
                                            output += to_emit;
                                            cursor += 1;
                                        }
                                        target.truncate(cursor);
                                    }
                                }
                                target.drain(..skip_cursor);
                            }
                        }
                    })
            }

            let group_clone = group_key.to_vec();
            let mut collection = ok_input.map({
                let mut row_packer = repr::RowPacker::new();
                move |row| {
                    let row_hash = row.hashed();
                    let datums = row.unpack();
                    let group_row = row_packer.pack(group_clone.iter().map(|i| datums[*i]));
                    ((group_row, row_hash), row)
                }
            });
            // This sequence of numbers defines the shifts that happen to the 64 bit hash
            // of the record, and has the properties that 1. there are not too many of them,
            // and 2. each has a modest difference to the next.
            //
            // These two properties mean that there should be no reductions on groups that
            // are substantially larger than `offset + limit` (the largest factor should be
            // bounded by two raised to the difference between subsequent numbers);
            if let Some(limit) = limit {
                for log_modulus in
                    [60, 56, 52, 48, 44, 40, 36, 32, 28, 24, 20, 16, 12, 8, 4u64].iter()
                {
                    // here we do not apply `offset`, but instead restrict ourself with a limit
                    // that includes the offset. We cannot apply `offset` until we perform the
                    // final, complete reduction.
                    collection = build_topk_stage(
                        collection,
                        order_key,
                        1u64 << log_modulus,
                        0,
                        Some(*offset + *limit),
                    );
                }
            }

            // We do a final step, both to make sure that we complete the reduction, and to correctly
            // apply `offset` to the final group, as we have not yet been applying it to the partially
            // formed groups.
            let result = build_topk_stage(collection, order_key, 1u64, *offset, *limit)
                .map(|((_key, _hash), row)| row);
            self.collections
                .insert(relation_expr.clone(), (result, err_input));
        }
    }
}
