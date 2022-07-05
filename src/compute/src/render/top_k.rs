// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! TopK execution logic.
//!
//! Consult [TopKPlan] documentation for details.

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use timely::dataflow::Scope;

use mz_compute_client::plan::top_k::{
    BasicTopKPlan, MonotonicTop1Plan, MonotonicTopKPlan, TopKPlan,
};
use mz_repr::{Diff, Row};

use crate::render::context::CollectionBundle;
use crate::render::context::Context;

// The implementation requires integer timestamps to be able to delay feedback for monotonic inputs.
impl<G> Context<G, Row>
where
    G: Scope,
    G::Timestamp: crate::render::RenderTimestamp,
{
    pub(crate) fn render_topk(
        &mut self,
        input: CollectionBundle<G, Row>,
        top_k_plan: TopKPlan,
    ) -> CollectionBundle<G, Row> {
        let (ok_input, err_input) = input.as_specific_collection(None);

        // We create a new region to compartmentalize the topk logic.
        let ok_result = ok_input.scope().region_named("TopK", |inner| {
            let ok_input = ok_input.enter(inner);
            let ok_result = match top_k_plan {
                TopKPlan::MonotonicTop1(MonotonicTop1Plan {
                    group_key,
                    order_key,
                }) => render_top1_monotonic(ok_input, group_key, order_key),
                TopKPlan::MonotonicTopK(MonotonicTopKPlan {
                    order_key,
                    group_key,
                    arity,
                    limit,
                }) => {
                    // For monotonic inputs, we are able to retract inputs that can no longer be produced
                    // as outputs. Any inputs beyond `offset + limit` will never again be produced as
                    // outputs, and can be removed. The simplest form of this is when `offset == 0` and
                    // these removable records are those in the input not produced in the output.
                    // TODO: consider broadening this optimization to `offset > 0` by first filtering
                    // down to `offset = 0` and `limit = offset + limit`, followed by a finishing act
                    // of `offset` and `limit`, discarding only the records not produced in the intermediate
                    // stage.
                    use differential_dataflow::operators::iterate::Variable;
                    let delay = std::time::Duration::from_nanos(10_000_000_000);
                    let retractions = Variable::new(
                        &mut ok_input.scope(),
                        <G::Timestamp as crate::render::RenderTimestamp>::system_delay(
                            delay.as_millis() as u64,
                        ),
                    );
                    let thinned = ok_input.concat(&retractions.negate());
                    let result = build_topk(thinned, group_key, order_key, 0, limit, arity);
                    retractions.set(&ok_input.concat(&result.negate()));
                    result
                }
                TopKPlan::Basic(BasicTopKPlan {
                    group_key,
                    order_key,
                    offset,
                    limit,
                    arity,
                }) => build_topk(ok_input, group_key, order_key, offset, limit, arity),
            };
            // Extract the results from the region.
            ok_result.leave_region()
        });

        return CollectionBundle::from_collections(ok_result, err_input);

        /// Constructs a TopK dataflow subgraph.
        fn build_topk<G>(
            collection: Collection<G, Row, Diff>,
            group_key: Vec<usize>,
            order_key: Vec<mz_expr::ColumnOrder>,
            offset: usize,
            limit: Option<usize>,
            arity: usize,
        ) -> Collection<G, Row, Diff>
        where
            G: Scope,
            G::Timestamp: Lattice,
        {
            let mut datum_vec = mz_repr::DatumVec::new();
            let mut collection = collection.map({
                move |row| {
                    let row_hash = row.hashed();
                    let group_row = {
                        let datums = datum_vec.borrow_with(&row);
                        let iterator = group_key.iter().map(|i| datums[*i]);
                        let total_size = mz_repr::datums_size(iterator.clone());
                        let mut group_row = Row::with_capacity(total_size);
                        group_row.packer().extend(iterator);
                        group_row
                    };
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
                        order_key.clone(),
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
            order_key: Vec<mz_expr::ColumnOrder>,
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

            let input = collection.map(move |((key, hash), row)| ((key, hash % modulus), row));
            // We only want to arrange parts of the input that are not part of the actual output
            // such that `input.concat(&negated_output.negate())` yields the correct TopK
            let negated_output = input.reduce_named("TopK", {
                move |_key, source, target: &mut Vec<(Row, Diff)>| {
                    // Determine if we must actually shrink the result set.
                    let must_shrink = offset > 0
                        || limit
                            .map(|l| source.iter().map(|(_, d)| *d).sum::<Diff>() as usize > l)
                            .unwrap_or(false);
                    if must_shrink {
                        // First go ahead and emit all records
                        for (row, diff) in source.iter() {
                            target.push(((*row).clone(), diff.clone()));
                        }
                        // local copies that may count down to zero.
                        let mut offset = offset;
                        let mut limit = limit;

                        // The order in which we should produce rows.
                        let mut indexes = (0..source.len()).collect::<Vec<_>>();
                        if !order_key.is_empty() {
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
                                mz_expr::compare_columns(&order_key, left, right, || {
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
                                    let to_skip =
                                        std::cmp::min(offset, usize::try_from(diff).unwrap());
                                    offset -= to_skip;
                                    diff -= Diff::try_from(to_skip).unwrap();
                                }
                                // We should produce at most `limit` records.
                                if let Some(limit) = &mut limit {
                                    diff = std::cmp::min(diff, Diff::try_from(*limit).unwrap());
                                    *limit -= diff as usize;
                                }
                                // Output the indicated number of rows.
                                if diff > 0 {
                                    // Emit retractions for the elements actually part of
                                    // the set of TopK elements.
                                    target.push((row.clone(), -diff));
                                }
                            }
                        }
                    }
                }
            });

            negated_output.negate().concat(&input).consolidate()
        }

        fn render_top1_monotonic<G>(
            collection: Collection<G, Row, Diff>,
            group_key: Vec<usize>,
            order_key: Vec<mz_expr::ColumnOrder>,
        ) -> Collection<G, Row, Diff>
        where
            G: Scope,
            G::Timestamp: Lattice,
        {
            // We can place our rows directly into the diff field, and only keep the relevant one
            // corresponding to evaluating our aggregate, instead of having to do a hierarchical
            // reduction.
            use timely::dataflow::operators::Map;

            let collection = collection.map({
                let mut datum_vec = mz_repr::DatumVec::new();
                move |row| {
                    let group_key = {
                        let datums = datum_vec.borrow_with(&row);
                        let iterator = group_key.iter().map(|i| datums[*i]);
                        let total_size = mz_repr::datums_size(iterator.clone());
                        let mut group_key = Row::with_capacity(total_size);
                        group_key.packer().extend(iterator);
                        group_key
                    };
                    (group_key, row)
                }
            });

            // We arrange the inputs ourself to force it into a leaner structure because we know we
            // won't care about values.
            //
            // TODO: Could we use explode here? We'd lose the diff>0 assert and we'd have to impl Mul
            // for the monoid, unclear if it's worth it.
            let partial: Collection<G, Row, monoids::Top1Monoid> = collection
                .consolidate()
                .inner
                .map(move |((group_key, row), time, diff)| {
                    assert!(diff > 0);
                    // NB: Top1 can throw out the diff since we've asserted that it's > 0. A more
                    // general TopK monoid would have to account for diff.
                    (
                        group_key,
                        time,
                        monoids::Top1Monoid {
                            row,
                            order_key: order_key.clone(),
                        },
                    )
                })
                .as_collection();
            let result = partial
                .arrange_by_self()
                .reduce_abelian::<_, OrdValSpine<_, _, _, _>>("Top1Monotonic", {
                    move |_key, input, output| {
                        let accum = &input[0].1;
                        output.push((accum.row.clone(), 1));
                    }
                });
            // TODO(#7331): Here we discard the arranged output.
            result.as_collection(|_k, v| v.clone())
        }
    }
}

/// Monoids for in-place compaction of monotonic streams.
pub mod monoids {
    use std::cmp::Ordering;

    use differential_dataflow::difference::Semigroup;
    use serde::{Deserialize, Serialize};

    use mz_expr::ColumnOrder;
    use mz_repr::Row;

    /// A monoid containing a row and an ordering.
    #[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize, Hash)]
    pub struct Top1Monoid {
        pub row: Row,
        pub order_key: Vec<ColumnOrder>,
    }

    impl Ord for Top1Monoid {
        fn cmp(&self, other: &Self) -> Ordering {
            debug_assert_eq!(self.order_key, other.order_key);

            // It might be nice to cache this row decoding like the non-monotonic codepath, but we'd
            // have to store the decoded Datums in the same struct as the Row, which gets tricky.
            let left: Vec<_> = self.row.unpack();
            let right: Vec<_> = other.row.unpack();
            mz_expr::compare_columns(&self.order_key, &left, &right, || left.cmp(&right))
        }
    }
    impl PartialOrd for Top1Monoid {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            Some(self.cmp(other))
        }
    }

    impl Semigroup for Top1Monoid {
        fn plus_equals(&mut self, rhs: &Self) {
            // It's unclear what the semantics are of a TopK without an ordering, but match the
            // non-monotonic impl's behavior of not doing any decoding work.
            if self.order_key.is_empty() {
                return;
            }

            let cmp = (&*self).cmp(rhs);
            // NB: Reminder that TopK returns the _minimum_ K items.
            if cmp == Ordering::Greater {
                self.clone_from(&rhs);
            }
        }

        fn is_zero(&self) -> bool {
            false
        }
    }
}
