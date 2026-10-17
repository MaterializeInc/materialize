// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utilities and stream extensions for temporal bucketing.

use std::hash::Hash;

use columnar::{Columnar, Index, Len, Push};
use differential_dataflow::Hashable;
use differential_dataflow::lattice::Lattice;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::columnar_exchange_data;
use mz_timely_util::temporal::bucket::TimeMajorBucket;
use mz_timely_util::temporal::{BucketChain, BucketRange, BucketTimestamp};
use timely::Accountable;
use timely::ExchangeData;
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::channels::pact::{Exchange, ExchangeCore};
use timely::dataflow::operators::Operator;
use timely::dataflow::{Stream, StreamVec};
use timely::order::TotalOrder;
use timely::progress::{Antichain, PathSummary, Timestamp};

use crate::typedefs::MzData;

/// Sort outstanding updates into a [`BucketChain`], and reveal data not in advance of the input
/// frontier. Retains a capability at the last input frontier to retain the right to produce data
/// at times between the last input frontier and the current input frontier.
pub trait TemporalBucketing<'scope, T: Timestamp>: Sized {
    /// Construct a new stream that stores updates into a [`BucketChain`] and reveals data
    /// not in advance of the frontier. Data that is within `threshold` distance of the input
    /// frontier or the `as_of` is passed through without being stored in the chain.
    ///
    /// The output container matches the input's, so a caller keeps whichever
    /// representation it had.
    fn bucket(self, as_of: Antichain<T>, threshold: T::Summary) -> Self;
}

/// Implementation for streams in scopes where timestamps define a total order.
impl<'scope, T, D> TemporalBucketing<'scope, T> for Stream<'scope, T, Column<(D, T, mz_repr::Diff)>>
where
    T: Timestamp + Default + ExchangeData + MzData + BucketTimestamp + TotalOrder + Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    D: ExchangeData + MzData + Ord + Clone + std::fmt::Debug + Hashable,
    for<'a> columnar::Ref<'a, D>: Copy + Ord + Hash,
    for<'a> columnar::Ref<'a, mz_repr::Diff>: Ord,
    for<'a> <(D, T, mz_repr::Diff) as Columnar>::Container:
        Push<columnar::Ref<'a, (D, T, mz_repr::Diff)>>,
{
    fn bucket(self, as_of: Antichain<T>, threshold: T::Summary) -> Self {
        let scope = self.scope();
        let logger = scope
            .worker()
            .logger_for("differential/arrange")
            .map(Into::into);

        type CB<D, T> = CapacityContainerBuilder<Column<(D, T, mz_repr::Diff)>>;

        let pact = ExchangeCore::<ColumnBuilder<_>, _>::new_core(
            columnar_exchange_data::<D, T, mz_repr::Diff>,
        );
        self.unary_frontier::<CB<D, T>, _, _, _>(pact, "Temporal delay", |cap, info| {
            let mut chain = BucketChain::new(TimeMajorBucket::new(logger, info.global_id));
            let activator = scope.activator_for(info.address);

            // Cap tracking the lower bound of potentially outstanding data.
            let mut cap = Some(cap);

            // Holds one bucket's worth of updates on the way into the chain.
            // Reused across activations for its allocation.
            let mut buffer: Column<(D, T, mz_repr::Diff)> = Default::default();
            // Reused input permutation, ordered by time.
            let mut permutation: Vec<usize> = Vec::new();
            // Reused so reading a record's time does not allocate: an iterative `T`
            // owns a `PointStamp`'s allocation.
            let mut time_buf = T::minimum();

            move |(input, frontier), output| {
                // The upper frontier is the join of the input frontier and the `as_of` frontier,
                // with the `threshold` summary applied to it.
                let mut upper = Antichain::new();
                for time1 in &frontier.frontier() {
                    for time2 in as_of.elements() {
                        // TODO: Use `join_assign` if we ever use a timestamp with allocations.
                        if let Some(time) = threshold.results_in(&time1.join(time2)) {
                            upper.insert(time);
                        }
                    }
                }

                input.for_each_time(|time, data| {
                    let mut session = output.session_with_builder(&time);
                    for data in data {
                        let borrowed = data.borrow();

                        // Pass through data about to be revealed, and retain the
                        // index of everything the chain has to hold. Only the
                        // retained records need ordering, and in steady state the
                        // pass-through share is the larger one.
                        permutation.clear();
                        for index in 0..borrowed.len() {
                            let update = borrowed.get(index);
                            time_buf.copy_from(update.1);
                            if upper.less_equal(&time_buf) {
                                permutation.push(index);
                            } else {
                                session.give(update);
                            }
                        }

                        // Order the retained records by time so each bucket's
                        // records land contiguously below. Sorting indices keeps
                        // the records in place.
                        permutation.sort_unstable_by_key(|index| borrowed.get(*index).1);

                        // The range `buffer`'s contents belong to, `None` while empty.
                        let mut buffered_range = None;
                        for index in permutation.drain(..) {
                            let update = borrowed.get(index);
                            time_buf.copy_from(update.1);

                            // Ship the buffer whenever the bucket changes, which
                            // the time order makes a single transition per bucket.
                            let contained = match &buffered_range {
                                Some(range) => BucketRange::contains(range, &time_buf),
                                None => false,
                            };
                            if !contained {
                                if let Some(range) = buffered_range.take() {
                                    let bucket = chain.find_mut(&range.start).expect("Must exist");
                                    bucket.push_container(&mut buffer);
                                }
                                buffered_range =
                                    Some(chain.range_of(&time_buf).expect("Must exist"));
                            }
                            buffer.push_into(update);
                        }

                        // Handle leftover data in the buffer.
                        if let Some(range) = buffered_range.take() {
                            let bucket = chain.find_mut(&range.start).expect("Must exist");
                            bucket.push_container(&mut buffer);
                        }
                    }
                });

                // Check for data that is ready to be revealed.
                let peeled = chain.peel(upper.borrow());
                if let Some(cap) = cap.as_ref() {
                    let mut session = output.session_with_builder(cap);
                    for mut column in peeled.into_iter().flat_map(|x| x.done()) {
                        session.give_container(&mut column);
                    }
                } else {
                    // If we don't have a cap, we should not have any data to reveal.
                    assert!(
                        peeled
                            .into_iter()
                            .flat_map(|x| x.done())
                            .all(|column| column.record_count() == 0),
                        "Unexpected data revealed without a cap."
                    );
                }

                // Downgrade the cap to the current input frontier.
                if frontier.is_empty() || upper.is_empty() {
                    cap = None;
                } else if let Some(cap) = cap.as_mut() {
                    // TODO: This assumes that the time is total ordered.
                    cap.downgrade(&upper[0]);
                }

                // Maintain the bucket chain by restoring it with fuel.
                let mut fuel = 1_000_000;
                chain.restore(&mut fuel);
                if fuel <= 0 {
                    // If we run out of fuel, we activate the operator to continue processing.
                    activator.activate();
                }
            }
        })
    }
}

/// Implementation for `Vec` streams in scopes where timestamps define a total order.
///
/// A caller whose consumer wants owned records keeps a `Vec`-native operator, because
/// staging the whole stream through a column would copy every pass-through record and
/// allocate it again on the way out. Only records that enter the chain are encoded, which
/// they were anyway: the chain's batcher is columnar. The reduce key-value path is the one
/// such caller, and this implementation goes away once its consumer reads columns.
impl<'scope, T, D> TemporalBucketing<'scope, T> for StreamVec<'scope, T, (D, T, mz_repr::Diff)>
where
    T: Timestamp + Default + ExchangeData + MzData + BucketTimestamp + TotalOrder + Lattice,
    D: ExchangeData + MzData + Ord + Clone + std::fmt::Debug + Hashable,
    for<'a> <(D, T, mz_repr::Diff) as Columnar>::Container: Push<&'a (D, T, mz_repr::Diff)>,
{
    fn bucket(self, as_of: Antichain<T>, threshold: T::Summary) -> Self {
        let scope = self.scope();
        let logger = scope
            .worker()
            .logger_for("differential/arrange")
            .map(Into::into);

        let pact = Exchange::new(|(d, _, _): &(D, T, mz_repr::Diff)| d.hashed().into());
        self.unary_frontier::<CapacityContainerBuilder<Vec<(D, T, mz_repr::Diff)>>, _, _, _>(
            pact,
            "Temporal delay",
            |cap, info| {
                let mut chain = BucketChain::new(TimeMajorBucket::new(logger, info.global_id));
                let activator = scope.activator_for(info.address);

                // Cap tracking the lower bound of potentially outstanding data.
                let mut cap = Some(cap);

                // Staging column for the records of one bucket. The chain's batcher is
                // columnar, so a stored record is encoded either way.
                let mut buffer: Column<(D, T, mz_repr::Diff)> = Default::default();

                move |(input, frontier), output| {
                    // The upper frontier is the join of the input frontier and the `as_of`
                    // frontier, with the `threshold` summary applied to it.
                    let mut upper = Antichain::new();
                    for time1 in &frontier.frontier() {
                        for time2 in as_of.elements() {
                            // TODO: Use `join_assign` if we ever use a timestamp with allocations.
                            if let Some(time) = threshold.results_in(&time1.join(time2)) {
                                upper.insert(time);
                            }
                        }
                    }

                    input.for_each_time(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for data in data {
                            // Skip data that is about to be revealed.
                            let pass_through =
                                data.extract_if(.., |(_, t, _)| !upper.less_equal(t));
                            session.give_iterator(pass_through);

                            // Sort data by time, then drain it into a buffer that contains data
                            // for a single bucket. We scan the data for ranges of time that fall
                            // into the same bucket so we can push batches of data at once.
                            data.sort_unstable_by(|(_, t, _), (_, t2, _)| t.cmp(t2));

                            let mut drain = data.drain(..);
                            if let Some(update) = drain.next() {
                                let mut range = chain.range_of(&update.1).expect("Must exist");
                                buffer.push_into(&update);
                                for update in drain {
                                    // If we have a range, check if the time is not within it.
                                    if !range.contains(&update.1) {
                                        // If the time is outside the range, push the current
                                        // buffer to the chain and reset the range.
                                        if !buffer.is_empty() {
                                            let bucket =
                                                chain.find_mut(&range.start).expect("Must exist");
                                            bucket.push_container(&mut buffer);
                                        }
                                        range = chain.range_of(&update.1).expect("Must exist");
                                    }
                                    buffer.push_into(&update);
                                }

                                // Handle leftover data in the buffer.
                                if !buffer.is_empty() {
                                    let bucket = chain.find_mut(&range.start).expect("Must exist");
                                    bucket.push_container(&mut buffer);
                                }
                            }
                        }
                    });

                    // Check for data that is ready to be revealed.
                    let peeled = chain.peel(upper.borrow());
                    if let Some(cap) = cap.as_ref() {
                        let mut session = output.session_with_builder(cap);
                        for column in peeled.into_iter().flat_map(|x| x.done()) {
                            session.give_iterator(
                                column
                                    .borrow()
                                    .into_index_iter()
                                    .map(<(D, T, mz_repr::Diff)>::into_owned),
                            );
                        }
                    } else {
                        // If we don't have a cap, we should not have any data to reveal.
                        assert!(
                            peeled
                                .into_iter()
                                .flat_map(|x| x.done())
                                .all(|column| column.record_count() == 0),
                            "Unexpected data revealed without a cap."
                        );
                    }

                    // Downgrade the cap to the current input frontier.
                    if frontier.is_empty() || upper.is_empty() {
                        cap = None;
                    } else if let Some(cap) = cap.as_mut() {
                        // TODO: This assumes that the time is total ordered.
                        cap.downgrade(&upper[0]);
                    }

                    // Maintain the bucket chain by restoring it with fuel.
                    let mut fuel = 1_000_000;
                    chain.restore(&mut fuel);
                    if fuel <= 0 {
                        // If we run out of fuel, we activate the operator to continue processing.
                        activator.activate();
                    }
                }
            },
        )
    }
}
