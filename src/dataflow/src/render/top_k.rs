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

use expr::{Diff, RelationExpr};
use repr::Row;

use crate::render::context::Context;

// The implementation requires integer timestamps to be able to delay feedback for monotonic inputs.
impl<G> Context<G, RelationExpr, Row, dataflow_types::Timestamp>
where
    G: Scope<Timestamp = dataflow_types::Timestamp>,
{
    pub fn render_topk(&mut self, relation_expr: &RelationExpr) {
        if let RelationExpr::TopK {
            input,
            group_key,
            order_key,
            limit,
            offset,
            monotonic,
        } = relation_expr
        {
            let arity = input.arity();
            let (ok_input, err_input) = self.collection(input).unwrap();

            // For monotonic inputs, we are able to retract inputs not produced as outputs.
            if *monotonic {
                use differential_dataflow::operators::iterate::Variable;
                use differential_dataflow::operators::Consolidate;
                let delay = std::time::Duration::from_nanos(10_000_000_000);
                let retractions = Variable::new(&mut ok_input.scope(), delay.as_millis() as u64);
                let thinned = ok_input.concat(&retractions.negate());
                let result = build_topk(thinned, group_key, order_key, *offset, *limit, arity);
                retractions.set(&ok_input.concat(&result.negate()).consolidate());
                self.collections
                    .insert(relation_expr.clone(), (result, err_input));
            } else {
                let result = build_topk(ok_input, group_key, order_key, *offset, *limit, arity);
                self.collections
                    .insert(relation_expr.clone(), (result, err_input));
            }

            /// Constructs a TopK dataflow subgraph.
            fn build_topk<G>(
                collection: Collection<G, Row, Diff>,
                group_key: &[usize],
                order_key: &[expr::ColumnOrder],
                offset: usize,
                limit: Option<usize>,
                arity: usize,
            ) -> Collection<G, Row, Diff>
            where
                G: Scope,
                G::Timestamp: Lattice,
            {
                let group_clone = group_key.to_vec();
                let mut collection = collection.map({
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
                            Some(offset + limit),
                            arity,
                        );
                    }
                }

                // We do a final step, both to make sure that we complete the reduction, and to correctly
                // apply `offset` to the final group, as we have not yet been applying it to the partially
                // formed groups.
                build_topk_stage(collection, order_key, 1u64, offset, limit, arity)
                    .map(|((_key, _hash), row)| row)
            }

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
                arity: usize,
            ) -> Collection<G, ((Row, u64), Row), Diff>
            where
                G: Scope,
                G::Timestamp: Lattice,
            {
                use differential_dataflow::operators::Reduce;
                let order_clone = order_key.to_vec();

                collection
                    .map(move |((key, hash), row)| ((key, hash % modulus), row))
                    .reduce_named("TopK", {
                        move |_key, source, target: &mut Vec<(Row, isize)>| {
                            // Determine if we must actually shrink the result set.
                            let must_shrink = offset > 0
                                || limit
                                    .map(|l| {
                                        source.iter().map(|(_, d)| *d).sum::<isize>() as usize > l
                                    })
                                    .unwrap_or(false);
                            if must_shrink {
                                // local copies that may count down to zero.
                                let mut offset = offset;
                                let mut limit = limit;

                                // The order in which we should produce rows.
                                let mut indexes = (0..source.len()).collect::<Vec<_>>();
                                if !order_clone.is_empty() {
                                    // We decode the datums once, into a common buffer for efficiency.
                                    // Each row should contain `arity` columns; we should check that.
                                    let mut buffer = Vec::with_capacity(arity * source.len());
                                    for (index, row) in source.iter().enumerate() {
                                        buffer.extend(row.0.iter());
                                        assert_eq!(buffer.len(), arity * (index + 1));
                                    }
                                    let width = buffer.len() / source.len();

                                    //todo: use arrangements or otherwise make the sort more performant?
                                    indexes.sort_by(|left, right| {
                                        let left = &buffer[left * width..][..width];
                                        let right = &buffer[right * width..][..width];
                                        expr::compare_columns(&order_clone, left, right, || {
                                            left.cmp(right)
                                        })
                                    });
                                }

                                // We now need to lay out the data in order of `buffer`, but respecting
                                // the `offset` and `limit` constraints.
                                for index in indexes.into_iter() {
                                    let (row, mut diff) = source[index];
                                    if diff > 0 {
                                        // If we are still skipping early records ...
                                        if offset > 0 {
                                            let to_skip = std::cmp::min(offset, diff as usize);
                                            offset -= to_skip;
                                            diff -= to_skip as isize;
                                        }
                                        // We should produce at most `limit` records.
                                        if let Some(limit) = &mut limit {
                                            diff = std::cmp::min(diff, *limit as isize);
                                            *limit -= diff as usize;
                                        }
                                        // Output the indicated number of rows.
                                        if diff > 0 {
                                            target.push((row.clone(), diff));
                                        }
                                    }
                                }
                            } else {
                                for (row, diff) in source.iter() {
                                    target.push(((*row).clone(), diff.clone()));
                                }
                            }
                        }
                    })
            }
        }
    }
}
