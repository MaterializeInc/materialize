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
use mz_timely_util::columnar::columnar_exchange_data;
use mz_timely_util::columnar::merge_batcher::ColumnMergeBatcher;
use mz_timely_util::temporal::{Bucket, BucketChain, BucketRange, BucketTimestamp};
use timely::Accountable;
use timely::container::{CapacityContainerBuilder, PushInto};
use timely::dataflow::channels::pact::{ExchangeCore, Pipeline};
use timely::dataflow::operators::Operator;
use timely::dataflow::{Stream, StreamVec};
use timely::order::TotalOrder;
use timely::progress::{Antichain, PathSummary, Timestamp};
use timely::{ExchangeData, PartialOrder};

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
///
/// Columnar throughout. The chain's batcher already holds [`Column`] chunks, so
/// the reveal path moves containers, and the input side addresses records by
/// index through a time-ordered permutation rather than moving them. No owned
/// record is materialized on either path.
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
                            if upper.less_equal(&T::into_owned(update.1)) {
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
                            let update_time = T::into_owned(update.1);

                            // Ship the buffer whenever the bucket changes, which
                            // the time order makes a single transition per bucket.
                            let contained = match &buffered_range {
                                Some(range) => BucketRange::contains(range, &update_time),
                                None => false,
                            };
                            if !contained {
                                if let Some(range) = buffered_range.take() {
                                    let bucket = chain.find_mut(&range.start).expect("Must exist");
                                    bucket.push_container(&mut buffer);
                                }
                                buffered_range =
                                    Some(chain.range_of(&update_time).expect("Must exist"));
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
                    // The chain hands back `Column` chunks already in the output's
                    // shape, so each one moves as a container.
                    for mut chunk in peeled.into_iter().flat_map(|x| x.done()) {
                        session.give_container(&mut chunk);
                    }
                } else {
                    // If we don't have a cap, we should not have any data to reveal.
                    assert!(
                        peeled
                            .into_iter()
                            .flat_map(|x| x.done())
                            .all(|chunk| chunk.record_count() == 0),
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
/// Kept alongside the columnar implementation for consumers that re-encode what
/// they read, where a `Vec` is the cheaper intermediate. The reduce key-value
/// path is the one such caller: its bucketed output feeds an arrangement.
impl<'scope, T, D> TemporalBucketing<'scope, T> for StreamVec<'scope, T, (D, T, mz_repr::Diff)>
where
    T: Timestamp + Default + ExchangeData + MzData + BucketTimestamp + TotalOrder + Lattice,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    D: ExchangeData + MzData + Ord + Clone + std::fmt::Debug + Hashable,
    for<'a> columnar::Ref<'a, D>: Copy + Ord + Hash,
    for<'a> columnar::Ref<'a, mz_repr::Diff>: Ord,
    for<'a> <(D, T, mz_repr::Diff) as Columnar>::Container:
        Push<&'a (D, T, mz_repr::Diff)> + Push<columnar::Ref<'a, (D, T, mz_repr::Diff)>>,
{
    fn bucket(self, as_of: Antichain<T>, threshold: T::Summary) -> Self {
        // Stage the `Vec` updates into a column and run the columnar operator, so
        // there is one bucketing implementation rather than two. The staging copy
        // is the price of a `Vec` caller, paid here rather than at the call site.
        let staged = self.unary::<ColumnBuilder<(D, T, mz_repr::Diff)>, _, _, _>(
            Pipeline,
            "BucketStage",
            |_cap, _info| {
                move |input, output| {
                    input.for_each(|time, data| {
                        let mut session = output.session_with_builder(&time);
                        for update in data.drain(..) {
                            session.give(&update);
                        }
                    });
                }
            },
        );

        staged
            .bucket(as_of, threshold)
            .unary::<CapacityContainerBuilder<Vec<(D, T, mz_repr::Diff)>>, _, _, _>(
                Pipeline,
                "BucketUnstage",
                |_cap, _info| {
                    move |input, output| {
                        input.for_each(|time, data| {
                            let mut session = output.session_with_builder(&time);
                            session.give_iterator(
                                data.borrow()
                                    .into_index_iter()
                                    .map(<(D, T, mz_repr::Diff)>::into_owned),
                            );
                        });
                    }
                },
            )
    }
}

/// A wrapper around [`ColumnMergeBatcher`] that implements the bucketing API.
///
/// This is the same columnar-native merge batcher (`Col2ValPagedBatcher`) the
/// default arrangement uses, so the bucket chain and arrangements share a single
/// merge-batcher implementation. The batcher consumes pre-chunked, consolidated
/// [`Column`] input, so this wrapper carries a [`ColumnChunker`] that sorts and
/// consolidates the input columns into the chunks the batcher consumes.
struct MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Clone,
    T: MzData + Ord + PartialOrder + Clone,
    R: MzData + Semigroup + Default,
{
    logger: Option<differential_dataflow::logging::Logger>,
    operator_id: usize,
    chunker: ColumnChunker<(D, T, R)>,
    inner: ColumnMergeBatcher<D, T, R>,
}

impl<D, T, R> MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Clone + 'static,
    T: MzData + Ord + PartialOrder + Clone + Default + Timestamp,
    R: MzData + Semigroup + Default + 'static + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: Push<&'a R>,
    for<'a> <(D, T, R) as Columnar>::Container: Push<&'a (D, T, R)>,
{
    /// Construct a new `MergeBatcherWrapper` with the given logger and operator ID.
    fn new(logger: Option<differential_dataflow::logging::Logger>, operator_id: usize) -> Self {
        Self {
            logger: logger.clone(),
            operator_id,
            chunker: ColumnChunker::default(),
            inner: ColumnMergeBatcher::new(logger, operator_id),
        }
    }

    /// Consolidate `buffer` through the chunker and feed any complete chunks to
    /// the batcher. Leaves `buffer` empty, retaining its allocation.
    fn push_container(&mut self, buffer: &mut Column<(D, T, R)>) {
        use timely::container::{ContainerBuilder as _, PushInto as _};
        if buffer.is_empty() {
            return;
        }
        self.chunker.push_into(buffer);
        buffer.clear();
        while let Some(chunk) = self.chunker.extract() {
            self.inner.push_into(std::mem::take(chunk));
        }
    }

    /// Flush any partial chunk still held by the chunker into the batcher.
    fn flush(&mut self) {
        use timely::container::ContainerBuilder as _;
        while let Some(chunk) = self.chunker.finish() {
            self.inner.push_into(std::mem::take(chunk));
        }
    }

    /// Reveal the contents of the merge batcher, returning a vector of `Column` chunks.
    fn done(mut self) -> Vec<Column<(D, T, R)>> {
        self.flush();
        let (chain, _description) = self.inner.seal(Antichain::new());
        chain
    }
}

impl<D, T, R> Bucket for MergeBatcherWrapper<D, T, R>
where
    D: MzData + Ord + Clone + 'static,
    T: MzData + Ord + PartialOrder + Clone + Default + 'static + BucketTimestamp,
    R: MzData + Semigroup + Default + 'static + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> columnar::Ref<'a, R>: Ord,
    for<'a> <D as Columnar>::Container: Push<columnar::Ref<'a, D>>,
    for<'a> <T as Columnar>::Container: Push<columnar::Ref<'a, T>>,
    for<'a> <R as Columnar>::Container: Push<&'a R>,
    for<'a> <(D, T, R) as Columnar>::Container: Push<&'a (D, T, R)>,
{
    type Timestamp = T;

    fn split(mut self, timestamp: &Self::Timestamp, fuel: &mut i64) -> (Self, Self) {
        // Re-chunks the sealed chunks into the lower batcher rather than splitting
        // the batcher's chains in place. Each chunk moves as a container, so this
        // visits no record.
        //
        // TODO: Split the batcher's chains directly without re-chunking.
        self.flush();
        let upper = Antichain::from_elem(timestamp.clone());
        let mut lower = Self::new(self.logger.clone(), self.operator_id);
        let (chain, _description) = self.inner.seal(upper);
        for mut chunk in chain {
            *fuel = fuel.saturating_sub(chunk.record_count());
            lower.push_container(&mut chunk);
        }
        (lower, self)
    }
}
