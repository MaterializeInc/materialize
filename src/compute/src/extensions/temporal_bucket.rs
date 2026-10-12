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
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Batcher;
use mz_timely_util::columnar::Column;
use mz_timely_util::columnar::batcher::ColumnChunker;
use mz_timely_util::columnar::builder::ColumnBuilder;
use mz_timely_util::columnar::chunk::{AccountedChunkBatcher, ColumnChunk, ComputeSpill};
use mz_timely_util::columnar::columnar_exchange_data;
use mz_timely_util::temporal::{Bucket, BucketChain, BucketRange, BucketTimestamp};
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
            let mut chain = BucketChain::new(MergeBatcherWrapper::new(logger, info.global_id));
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
                let mut chain = BucketChain::new(MergeBatcherWrapper::new(logger, info.global_id));
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

/// A wrapper around [`AccountedChunkBatcher`] that implements the bucketing API.
///
/// This is the same merge batcher the arrange sites' chunked arm uses, so the
/// bucket chain and those arrangements share one merge-batcher implementation.
/// The choice is unconditional here: the bucket chain does not consult the
/// arrange batcher selector.
///
/// The batcher consumes sorted, consolidated [`ColumnChunk`] input, so this
/// wrapper carries a [`ColumnChunker`] that sorts and consolidates the input
/// columns into the [`Column`]s it wraps as chunks.
///
/// Updates are stored time-major, as `((time, data), time, diff)`, so each
/// chunk covers a narrow range of times. Splitting at a time then loads only
/// the chunks whose time bounds straddle it, and moves every other chunk
/// whole, spilled or not. With data-major chunks every chunk spans the
/// bucket's times, and each split loads and rewrites the whole bucket. The
/// leading copy of the time orders the chunks, and the trailing copy is the
/// batcher's time, which `seal` partitions by.
struct MergeBatcherWrapper<D, T, R>
where
    D: MzData,
    T: MzData + Ord + Default + Timestamp + Lattice,
    R: MzData + Semigroup + Default + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
{
    logger: Option<differential_dataflow::logging::Logger>,
    operator_id: usize,
    /// Time-major copy of the updates on their way into the chunker.
    staging: Column<((T, D), T, R)>,
    chunker: ColumnChunker<((T, D), T, R)>,
    inner: AccountedChunkBatcher<(T, D), T, R, ComputeSpill>,
}

impl<D, T, R> MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Clone,
    T: MzData + Ord + Clone + Default + Timestamp + Lattice,
    R: MzData + Semigroup + Default + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: Push<&'a R>,
{
    /// Construct a new `MergeBatcherWrapper` with the given logger and operator ID.
    fn new(logger: Option<differential_dataflow::logging::Logger>, operator_id: usize) -> Self {
        Self {
            logger: logger.clone(),
            operator_id,
            staging: Column::default(),
            chunker: ColumnChunker::default(),
            inner: AccountedChunkBatcher::new(logger, operator_id),
        }
    }

    /// Consolidate `buffer` through the chunker and feed any complete chunks to
    /// the batcher. Leaves `buffer` empty, retaining its allocation.
    fn push_container(&mut self, buffer: &mut Column<(D, T, R)>) {
        use columnar::Container as _;
        use timely::container::{ContainerBuilder as _, PushInto as _};
        if buffer.is_empty() {
            return;
        }
        // `staging` is only ever cleared, which keeps it typed.
        let Column::Typed(staging) = &mut self.staging else {
            unreachable!("staging column is typed");
        };
        let (data, time, diff) = buffer.borrow();
        staging.0.0.extend_from_self(time, 0..time.len());
        staging.0.1.extend_from_self(data, 0..data.len());
        staging.1.extend_from_self(time, 0..time.len());
        staging.2.extend_from_self(diff, 0..diff.len());
        buffer.clear();
        self.chunker.push_into(&mut self.staging);
        self.staging.clear();
        while let Some(chunk) = self.chunker.extract() {
            self.inner
                .push_into(ColumnChunk::from_body(std::mem::take(chunk)));
        }
    }

    /// Flush any partial chunk still held by the chunker into the batcher.
    fn flush(&mut self) {
        use timely::container::{ContainerBuilder as _, PushInto as _};
        while let Some(chunk) = self.chunker.finish() {
            self.inner
                .push_into(ColumnChunk::from_body(std::mem::take(chunk)));
        }
    }

    /// Reveal the contents of the merge batcher, as one column per chunk.
    ///
    /// A spilled chunk loads when the iterator reaches it.
    fn done(mut self) -> impl Iterator<Item = Column<(D, T, R)>> {
        use columnar::Container as _;
        self.flush();
        let (chain, _description) = self.inner.seal(Antichain::new());
        chain.into_iter().map(|chunk| {
            let body = chunk.into_body();
            let ((_, data), time, diff) = body.borrow();
            let mut column = <(D, T, R) as Columnar>::Container::default();
            column.0.extend_from_self(data, 0..data.len());
            column.1.extend_from_self(time, 0..time.len());
            column.2.extend_from_self(diff, 0..diff.len());
            Column::Typed(column)
        })
    }
}

impl<D, T, R> Bucket for MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Clone,
    T: MzData + Ord + Clone + Default + Lattice + BucketTimestamp,
    R: MzData + Semigroup + Default + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: Push<&'a R>,
{
    type Timestamp = T;

    fn split(mut self, timestamp: &Self::Timestamp, fuel: &mut i64) -> (Self, Self) {
        use timely::container::PushInto as _;
        self.flush();
        let upper = Antichain::from_elem(timestamp.clone());
        let mut lower = Self::new(self.logger.clone(), self.operator_id);
        // Sealing at `timestamp` ships exactly the updates strictly less than it,
        // as sorted, consolidated chunks; feed them to the lower batcher whole,
        // so spilled bodies move without being loaded. The `fuel` charge covers
        // the sealing work those records paid for.
        let (chain, _description) = self.inner.seal(upper);
        for chunk in chain {
            *fuel = fuel.saturating_sub(chunk.record_count());
            lower.inner.push_into(chunk);
        }
        (lower, self)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_ore::pool::Pool;
    use mz_repr::{Diff, Timestamp};
    use mz_timely_util::columnar::chunk::set_spill_override;

    use super::*;

    type Wrapper = MergeBatcherWrapper<u64, Timestamp, Diff>;
    type Update = (u64, Timestamp, Diff);

    /// Updates with colliding `(data, time)` pairs and retractions, spread
    /// over times `[1_000, 101_000)`.
    fn updates(round: u64) -> Vec<Update> {
        (0..20_000u64)
            .map(|i| {
                let x = (i + round * 20_000).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                let data = (x >> 8) % 5_000;
                let time = Timestamp::from(1_000 + (x >> 24) % 100_000);
                let diff = if x & (1 << 40) == 0 {
                    Diff::ONE
                } else {
                    Diff::MINUS_ONE
                };
                (data, time, diff)
            })
            .collect()
    }

    /// Push `updates` into the buckets of `chain` that hold their times, the
    /// way the operator does.
    fn insert(chain: &mut BucketChain<Wrapper>, mut updates: Vec<Update>) {
        updates.sort_by_key(|(_, time, _)| *time);
        let mut buffer = Column::default();
        let mut range: Option<BucketRange<Timestamp>> = None;
        for update in &updates {
            if !range
                .as_ref()
                .is_some_and(|range| range.contains(&update.1))
            {
                if let Some(range) = range.take() {
                    let bucket = chain.find_mut(&range.start).expect("must exist");
                    bucket.push_container(&mut buffer);
                }
                range = Some(chain.range_of(&update.1).expect("must exist"));
            }
            buffer.push_into(update);
        }
        if let Some(range) = range {
            let bucket = chain.find_mut(&range.start).expect("must exist");
            bucket.push_container(&mut buffer);
        }
    }

    fn accumulate(
        map: &mut BTreeMap<(u64, Timestamp), Diff>,
        updates: impl Iterator<Item = Update>,
    ) {
        for (data, time, diff) in updates {
            *map.entry((data, time)).or_default() += diff;
        }
        map.retain(|_, diff| !diff.is_zero());
    }

    /// Peels at a sequence of frontiers, with updates arriving between peels
    /// for times in buckets that earlier peels split, and compares each
    /// peel's output with the reference accumulation of the times it covers.
    fn peel_matches_reference() {
        let mut chain = BucketChain::new(Wrapper::new(None, 0));
        let mut reference = BTreeMap::new();
        let mut lower = Timestamp::MIN;
        for round in 0..6u64 {
            let fresh: Vec<_> = updates(round)
                .into_iter()
                .filter(|(_, time, _)| lower <= *time)
                .collect();
            accumulate(&mut reference, fresh.iter().copied());
            insert(&mut chain, fresh);

            let upper = (round < 5).then(|| Timestamp::from(1_000 + (round + 1) * 17_000));
            let frontier = Antichain::from_iter(upper);
            let mut revealed = BTreeMap::new();
            for column in chain
                .peel(frontier.borrow())
                .into_iter()
                .flat_map(|x| x.done())
            {
                accumulate(
                    &mut revealed,
                    column.borrow().into_index_iter().map(Update::into_owned),
                );
            }
            let mut expected = BTreeMap::new();
            reference.retain(|(data, time), diff| {
                if frontier.less_equal(time) {
                    true
                } else {
                    expected.insert((*data, *time), *diff);
                    false
                }
            });
            assert!(!expected.is_empty(), "round {round} reveals nothing");
            assert_eq!(revealed, expected, "round {round}");

            let mut fuel = 0;
            while fuel <= 0 {
                fuel = 1_000_000;
                chain.restore(&mut fuel);
            }
            lower = upper.unwrap_or(Timestamp::MAX);
        }
        assert!(reference.is_empty());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn peel_matches_reference_resident() {
        peel_matches_reference();
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)]
    fn peel_matches_reference_spilled() {
        let pool = Pool::new().expect("pool creation");
        set_spill_override(Some(pool.clone()));
        peel_matches_reference();
        set_spill_override(None);
        assert!(pool.stats().inserts > 0, "nothing spilled");
    }
}
