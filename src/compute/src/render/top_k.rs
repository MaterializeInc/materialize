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

use std::collections::HashMap;

use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::ArrangeBySelf;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
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
                    let mut datum_vec = mz_repr::DatumVec::new();
                    let collection = ok_input.map(move |row| {
                        let group_row = {
                            let datums = datum_vec.borrow_with(&row);
                            let iterator = group_key.iter().map(|i| datums[*i]);
                            let total_size = mz_repr::datums_size(iterator.clone());
                            let mut group_row = Row::with_capacity(total_size);
                            group_row.packer().extend(iterator);
                            group_row
                        };
                        (group_row, row)
                    });

                    // For monotonic inputs, we are able to thin the input relation in two stages:
                    // 1. First, we can do an intra-timestamp thinning which has the advantage of
                    //    being computed in a streaming fashion, even for the initial snapshot.
                    // 2. Then, we can do inter-timestamp thinning by feeding back negations for
                    //    any records that have been invalidated.
                    let collection = if let Some(limit) = limit {
                        render_intra_ts_thinning(collection, order_key.clone(), limit)
                    } else {
                        collection
                    };

                    let collection =
                        collection.map(|(group_row, row)| ((group_row, row.hashed()), row));

                    // For monotonic inputs, we are able to retract inputs that can no longer be produced
                    // as outputs. Any inputs beyond `offset + limit` will never again be produced as
                    // outputs, and can be removed. The simplest form of this is when `offset == 0` and
                    // these removable records are those in the input not produced in the output.
                    // TODO: consider broadening this optimization to `offset > 0` by first filtering
                    // down to `offset = 0` and `limit = offset + limit`, followed by a finishing act
                    // of `offset` and `limit`, discarding only the records not produced in the intermediate
                    // stage.
                    use differential_dataflow::operators::iterate::Variable;
                    let delay = std::time::Duration::from_secs(10);
                    let retractions = Variable::new(
                        &mut ok_input.scope(),
                        <G::Timestamp as crate::render::RenderTimestamp>::system_delay(
                            delay.try_into().expect("must fit"),
                        ),
                    );
                    let thinned = collection.concat(&retractions.negate());

                    // As an additional optimization, we can skip creating the full topk hierachy
                    // here since we now have an upper bound on the number records due to the
                    // intra-ts thinning. The maximum number of records per timestamp is
                    // (num_workers * limit), which we expect to be a small number and so we render
                    // a single topk stage.
                    let result = build_topk_stage(thinned, order_key, 1u64, 0, limit, arity);
                    retractions.set(&collection.concat(&result.negate()));

                    result.map(|((_key, _hash), row)| row)
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
            // TODO(#16549): Use explicit arrangement
            let negated_output = input.reduce_named("TopK", {
                move |_key, source, target: &mut Vec<(Row, Diff)>| {
                    // Determine if we must actually shrink the result set.
                    // TODO(benesch): avoid dangerous `as` conversion.
                    #[allow(clippy::as_conversions)]
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
                            // Note: source was originally ordered by the u8 array representation
                            // of rows, but left.cmp(right) uses Datum::cmp.
                            mz_expr::compare_columns(&order_key, left, right, || left.cmp(right))
                        });

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
                                // TODO(benesch): avoid dangerous `as` conversion.
                                #[allow(clippy::as_conversions)]
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

            negated_output
                .negate()
                .concat(&input)
                // TODO(#16549): Use explicit arrangement
                .consolidate()
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
                // TODO(#16549): Use explicit arrangement
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
                // TODO(#16549): Use explicit arrangement
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

        fn render_intra_ts_thinning<G>(
            collection: Collection<G, (Row, Row), Diff>,
            order_key: Vec<mz_expr::ColumnOrder>,
            limit: usize,
        ) -> Collection<G, (Row, Row), Diff>
        where
            G: Scope,
            G::Timestamp: Lattice,
        {
            let mut aggregates = HashMap::new();
            let mut vector = Vec::new();
            collection
                .inner
                .unary_notify(
                    Pipeline,
                    "TopKIntraTimeThinning",
                    [],
                    move |input, output, notificator| {
                        while let Some((time, data)) = input.next() {
                            data.swap(&mut vector);
                            let agg_time = aggregates
                                .entry(time.time().clone())
                                .or_insert_with(HashMap::new);
                            for ((grp_row, row), record_time, diff) in vector.drain(..) {
                                let monoid = monoids::Top1Monoid {
                                    row,
                                    order_key: order_key.clone(),
                                };
                                let topk = agg_time.entry((grp_row, record_time)).or_insert_with(
                                    move || {
                                        topk_agg::TopKBatch::new(
                                            limit.try_into().expect("must fit"),
                                        )
                                    },
                                );
                                topk.update(monoid, diff);
                            }
                            notificator.notify_at(time.retain());
                        }

                        notificator.for_each(|time, _, _| {
                            if let Some(aggs) = aggregates.remove(time.time()) {
                                let mut session = output.session(&time);
                                for ((grp_row, record_time), topk) in aggs {
                                    session.give_iterator(topk.into_iter().map(|(monoid, diff)| {
                                        ((grp_row.clone(), monoid.row), record_time.clone(), diff)
                                    }))
                                }
                            }
                        });
                    },
                )
                .as_collection()
        }
    }
}

/// Types for in-place intra-ts aggregation of monotonic streams.
pub mod topk_agg {
    use differential_dataflow::consolidation;
    use smallvec::SmallVec;

    // TODO: This struct looks a lot like ChangeBatch and indeed its code is a modified version of
    // that. It would be nice to find a way to reuse some or all of the code from there.
    //
    // Additionally, because we're calling into DD's consolidate method we are forced to work with
    // the `Ord` trait which for the usage we do above means that we need to clone the `order_key`
    // for each record. It would be nice to also remove the need for cloning that piece of data
    pub struct TopKBatch<T> {
        updates: SmallVec<[(T, i64); 16]>,
        clean: usize,
        limit: i64,
    }

    impl<T: Ord> TopKBatch<T> {
        pub fn new(limit: i64) -> Self {
            Self {
                updates: SmallVec::new(),
                clean: 0,
                limit,
            }
        }

        /// Adds a new update, for `item` with `value`.
        ///
        /// This could be optimized to perform compaction when the number of "dirty" elements exceeds
        /// half the length of the list, which would keep the total footprint within reasonable bounds
        /// even under an arbitrary number of updates. This has a cost, and it isn't clear whether it
        /// is worth paying without some experimentation.
        #[inline]
        pub fn update(&mut self, item: T, value: i64) {
            self.updates.push((item, value));
            self.maintain_bounds();
        }

        /// Compact the internal representation.
        ///
        /// This method sort `self.updates` and consolidates elements with equal item, discarding
        /// any whose accumulation is zero. It is optimized to only do this if the number of dirty
        /// elements is non-zero.
        #[inline]
        pub fn compact(&mut self) {
            if self.clean < self.updates.len() && self.updates.len() > 1 {
                let len = consolidation::consolidate_slice(&mut self.updates);
                self.updates.truncate(len);

                // We can now retain only the first K records and throw away everything else
                let mut limit = self.limit;
                self.updates.retain(|x| {
                    if limit > 0 {
                        limit -= x.1;
                        true
                    } else {
                        false
                    }
                });
                // By the end of the loop above `limit` will be less than or equal to zero. The
                // case where it goes negative is when the last record we retained had more copies
                // than necessary. For this reason we need to do one final adjustment of the diff
                // field of the last record so that the total sum of the diffs in the batch is K.
                if let Some(item) = self.updates.last_mut() {
                    // We are subtracting the limit *negated*, therefore we are subtracting a value
                    // that is *greater* than or equal to zero, which represents the excess.
                    item.1 -= -limit;
                }
            }
            self.clean = self.updates.len();
        }

        /// Maintain the bounds of pending (non-compacted) updates versus clean (compacted) data.
        /// This function tries to minimize work by only compacting if enough work has accumulated.
        fn maintain_bounds(&mut self) {
            // if we have more than 32 elements and at least half of them are not clean, compact
            if self.updates.len() > 32 && self.updates.len() >> 1 >= self.clean {
                self.compact()
            }
        }
    }

    impl<T: Ord> IntoIterator for TopKBatch<T> {
        type Item = (T, i64);
        type IntoIter = smallvec::IntoIter<[(T, i64); 16]>;

        fn into_iter(mut self) -> Self::IntoIter {
            self.compact();
            self.updates.into_iter()
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
            let cmp = (*self).cmp(rhs);
            // NB: Reminder that TopK returns the _minimum_ K items.
            if cmp == Ordering::Greater {
                self.clone_from(rhs);
            }
        }

        fn is_zero(&self) -> bool {
            false
        }
    }
}
