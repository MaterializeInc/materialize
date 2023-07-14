// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Operators that transform collections that evolve with some timestamp `FromTime` into a
//! collections that evolve with some other timestamp `IntoTime.
//!
//! Reclocking happens in two separate phases, implemented by [ReclockOperator] and
//! [ReclockFollower] respectively.
//!
/// For the first phase, the `ReclockOperator` observes the progress of a stream that is
/// timestamped with some source time `FromTime` and generates bindings that describe how the
/// collection should evolve in target time `IntoTime`.
///
/// For the second phase, the `ReclockFollower` observes both the data and the progress of a
/// collection as it evolves in the `FromTime` domain and reclocks it into a collection that
/// evolves in `IntoTime` according to the reclock decisions that have been taken by the
/// `ReclockOperator`.
use std::cell::RefCell;
use std::fmt::Display;
use std::rc::Rc;

use differential_dataflow::consolidation;
use differential_dataflow::difference::Abelian;
use differential_dataflow::lattice::Lattice;
use futures::{FutureExt, StreamExt};
use mz_persist_client::error::UpperMismatch;
use mz_repr::Diff;
use mz_storage_client::util::remap_handle::RemapHandle;
use mz_timely_util::antichain::AntichainExt;
use timely::order::{PartialOrder, TotalOrder};
use timely::progress::frontier::{Antichain, AntichainRef, MutableAntichain};
use timely::progress::Timestamp;

pub mod compat;

/// A "follower" for the ReclockOperator, that maintains a trace based on the results of reclocking
/// and data from the source. It provides the `reclock` method, which produces messages with their
/// associated timestamps.
///
/// Shareable with `.share()`
pub struct ReclockFollower<FromTime: Timestamp, IntoTime: Timestamp + Lattice + Display> {
    /// The `since` maintained by the local handle. This may be beyond the shared `since`
    since: Antichain<IntoTime>,
    pub inner: Rc<RefCell<ReclockFollowerInner<FromTime, IntoTime>>>,
}

#[derive(Debug)]
pub struct ReclockFollowerInner<FromTime: Timestamp, IntoTime: Timestamp + Lattice + Display> {
    /// A dTVC trace of the remap collection containing all updates at `t: since <= t < upper`.
    // NOTE(petrosagg): Once we write this as a timely operator this should just be an arranged
    // trace of the remap collection
    remap_trace: Vec<(FromTime, IntoTime, Diff)>,
    /// Since frontier of the partial remap trace
    since: MutableAntichain<IntoTime>,
    /// Upper frontier of the partial remap trace
    upper: Antichain<IntoTime>,
    /// The upper frontier in terms of `FromTime`. Any attempt to reclock messages beyond this
    /// frontier will result in an error.
    source_upper: MutableAntichain<FromTime>,
}

impl<FromTime, IntoTime> ReclockFollower<FromTime, IntoTime>
where
    FromTime: Timestamp,
    IntoTime: Timestamp + Lattice + Display,
{
    /// Constructs a new [ReclockFollower]
    pub fn new(as_of: Antichain<IntoTime>) -> Self {
        let mut since = MutableAntichain::new();
        since.update_iter(as_of.iter().map(|t| (t.clone(), 1)));

        Self {
            since: as_of,
            inner: Rc::new(RefCell::new(ReclockFollowerInner {
                remap_trace: Vec::new(),
                since,
                upper: Antichain::from_elem(IntoTime::minimum()),
                source_upper: MutableAntichain::new(),
            })),
        }
    }

    pub fn source_upper(&self) -> Antichain<FromTime> {
        self.inner.borrow().source_upper.frontier().to_owned()
    }

    pub fn initialized(&self) -> bool {
        let inner = self.inner.borrow();
        PartialOrder::less_than(&inner.since.frontier(), &inner.upper.borrow())
    }

    /// Pushes a new trace batch into this [`ReclockFollower`].
    pub fn push_trace_batch(&mut self, mut batch: ReclockBatch<FromTime, IntoTime>) {
        let mut inner = self.inner.borrow_mut();
        // Ensure we only add consolidated batches to our trace
        consolidation::consolidate_updates(&mut batch.updates);
        inner.remap_trace.extend(batch.updates.iter().cloned());
        inner.source_upper.update_iter(
            batch
                .updates
                .into_iter()
                .map(|(src_ts, _ts, diff)| (src_ts, diff)),
        );
        inner.upper = batch.upper;
    }

    /// Reclocks a batch of messages timestamped with `FromTime` and returns an iterator of
    /// messages timestamped with `IntoTime`.
    ///
    /// Each item of the resulting iterator will be associated with either the time it should be
    /// reclocked to or an error indicating that a reclocking decision could not be taken with the
    /// data that we have at hand.
    ///
    /// This method is most efficient when the to be reclocked iterator presents data in contiguous
    /// runs with the same `FromTime`.
    pub fn reclock<'a, M: 'a>(
        &'a self,
        batch: impl IntoIterator<Item = (M, FromTime)> + 'a,
    ) -> impl Iterator<Item = (M, Result<IntoTime, ReclockError<FromTime>>)> + 'a
    where
        IntoTime: TotalOrder,
    {
        let mut memo: Option<(FromTime, Result<IntoTime, ReclockError<FromTime>>)> = None;
        batch.into_iter().map(move |(msg, src_ts)| {
            let result = match &memo {
                Some((prev_src_ts, result)) if prev_src_ts == &src_ts => result.clone(),
                _ => {
                    let result = self.reclock_time_total(&src_ts);
                    memo.insert((src_ts, result)).1.clone()
                }
            };
            (msg, result)
        })
    }

    /// Reclocks a single `FromTime` timestamp into the `IntoTime` time domain.
    pub fn reclock_time(
        &self,
        src_ts: &FromTime,
    ) -> Result<Antichain<IntoTime>, ReclockError<FromTime>> {
        if !self.initialized() {
            return Err(ReclockError::Uninitialized);
        }
        let inner = self.inner.borrow();
        if inner.source_upper.less_equal(src_ts) {
            return Err(ReclockError::BeyondUpper(src_ts.clone()));
        }

        // In order to understand the logic of the following section let's first consider an
        // example of trying to reclock the FromTime D from this partial ordering:
        //
        //     ,--B----D
        //    /              ,-------F----.
        //   A              /              \
        //    `---C--------E---------G------H
        //
        // ..into a target time domain where the remap collection varies according to this IntoTime
        // partial ordering:
        //
        //   *----*--.---------------*------*
        //   t0   t1  \              t2     t3
        //             `--------------------*
        //                                  t4
        // ..and the FromTime frontiers at times t0, t1, t2, t3, t4 accumulate to:
        //
        // t0: Antichain{A}
        // t1: Antichain{B, C}
        // t2: Antichain{F, G}
        // t3: Antichain{H}
        // t4: Antichain{H}
        //
        // In the example above the correct answer is {t2, t4}, because this is the smallest
        // antichain of IntoTime times such that the remap collection accumulates at each one of
        // them to a FromTime frontier `f` such that D is not beyond `f`.
        //
        // We need to compute the answer by iterating over the consolidated remap trace which will
        // present to us one diff at a time. We know that by construction at any given IntoTime
        // time the remap collection accumulates to a well formed antichain. That is, it contains
        // exactly one copy of mutually incomparable FromTime elements.
        //
        // We also know that `src_ts` is beyond the since frontier, therefore there exist witness
        // timestamps `from_ts` that are less than or equal to `src_ts` and occur at IntoTime times
        // with a positive diff. We also know that `src_ts` is not beyond the upper frontier,
        // therefore all the positive diffs of the witness times must be retracted at subsequent
        // IntoTime times.
        //
        // This cycle may be repeated an arbitrary amount of times until the final retraction. The
        // IntoTime times at which the final retraction happens are the times that `src_ts` should
        // be reclocked to.
        //
        // Therefore, if we filter the remap trace for witness timestamps and construct a
        // MutableAntichain of the IntoTime times the witnesses occur at with a negated diff we'll
        // end up computing a frontier of all the IntoTime times such that the remap collection
        // accumulates to a frontier `f` such that `src_ts` is not beyond `f`, since the witness
        // has been retracted.
        //
        // For our example above the remap trace would look like this:
        //
        // (A, t0, +1)
        //
        // (A, t1, -1)
        // (B, t1, +1)
        // (C, t1, +1)
        //
        // (B, t2, -1)
        // (C, t2, -1)
        // (F, t2, +1)
        // (G, t2, +1)
        //
        // (F, t3, -1)
        // (G, t3, -1)
        // (H, t3, +1)
        //
        // (B, t4, -1)
        // (C, t4, -1)
        // (H, t4, +1)
        //
        // We are interested in reclocking the FromTime D so if we filter the trace for witnesses
        // (i.e triplets such that `from_ts` is less than or equal to D) we are left with:
        //
        // (A, t0, +1)
        // (A, t1, -1)
        // (B, t1, +1)
        // (B, t2, -1)
        // (B, t4, -1)
        //
        // Keeping the IntoTime component and negating the diffs we have the following collection:
        //
        // (t0, -1)
        // (t1, +1)
        // (t1, -1)
        // (t2, +1)
        // (t4, +1)
        //
        // Processing this through a MutableAntichain will give as the desired frontier {t2, t4}
        // since the diffs for t1 cancel out and t0 has a negative diff.
        //
        // While IntoTime is a partially ordered time and in the example above the answer was two
        // separate times, we force that there is actually only one such time by requiring the
        // ticker stream to provide a single timestamp per tick and advance its upper on each tick.
        // This is just limitation of having the API function signatures from the original reclock
        // implementation require a single IntoTime result. We should ideally lift that and make
        // the reclock operators fully general.
        let mut into_times = MutableAntichain::new();

        let mut minimum = IntoTime::minimum();
        minimum.advance_by(inner.since.frontier());
        into_times.update_iter([(minimum, 1)]);

        into_times.update_iter(
            inner
                .remap_trace
                .iter()
                .filter(|(from_ts, _, _)| PartialOrder::less_equal(from_ts, src_ts))
                .map(|(_, into_ts, diff)| (into_ts.clone(), diff.negate())),
        );
        Ok(into_times.frontier().to_owned())
    }

    /// Reclocks a single `FromTime` timestamp into a totally ordered `IntoTime` time domain.
    pub fn reclock_time_total(&self, src_ts: &FromTime) -> Result<IntoTime, ReclockError<FromTime>>
    where
        IntoTime: TotalOrder,
    {
        Ok(self
            .reclock_time(src_ts)?
            .into_option()
            .expect("reclock_time produced the empty antichain"))
    }

    /// Reclocks a `FromTime` frontier into a `IntoTime` frontier.
    ///
    /// The conversion has the property that all messages that are beyond the provided `FromTime`
    /// frontier will be relocked at times that will be beyond the returned `IntoTime` frontier.
    /// This can be used to drive a `IntoTime` capability forward when the caller knows that a
    /// `FromTime` frontier has advanced.
    ///
    /// The method returns an error if the `FromTime` frontier is not beyond the since frontier.
    /// The error will contain the offending `FromTime`.
    pub fn reclock_frontier(
        &self,
        source_frontier: AntichainRef<'_, FromTime>,
    ) -> Result<Antichain<IntoTime>, ReclockError<FromTime>> {
        let mut dest_frontier = self.inner.borrow().upper.clone();

        for src_ts in source_frontier.iter() {
            match self.reclock_time(src_ts) {
                Ok(dest_ts) => {
                    dest_frontier.extend(dest_ts);
                }
                Err(ReclockError::BeyondUpper(_)) => {}
                Err(err @ ReclockError::Uninitialized) => return Err(err),
            }
        }

        Ok(dest_frontier)
    }

    /// Reclocks an `IntoTime` frontier into a `FromTime` frontier.

    /// The conversion has the property that all messages that would be reclocked to times beyond
    /// the provided `IntoTime` frontier will be beyond the returned `FromTime` frontier. This can
    /// be used to compute a safe starting point to resume producing an `IntoTime` collection at a
    /// particular frontier.
    pub fn source_upper_at_frontier<'a>(
        &self,
        frontier: AntichainRef<'a, IntoTime>,
    ) -> Result<Antichain<FromTime>, ReclockError<AntichainRef<'a, IntoTime>>> {
        let inner = self.inner.borrow();
        if PartialOrder::less_equal(&frontier, &inner.since.frontier()) {
            return Ok(Antichain::from_elem(FromTime::minimum()));
        }
        if !PartialOrder::less_than(&frontier, &inner.upper.borrow()) {
            if PartialOrder::less_equal(&frontier, &inner.upper.borrow()) {
                return Ok(inner.source_upper.frontier().to_owned());
            } else if frontier.is_empty() {
                return Ok(Antichain::new());
            } else {
                return Err(ReclockError::BeyondUpper(frontier));
            }
        }
        let mut source_upper = MutableAntichain::new();

        source_upper.update_iter(inner.remap_trace.iter().filter_map(|(src_ts, ts, diff)| {
            if frontier
                .iter()
                .any(|dest_ts| PartialOrder::less_than(ts, dest_ts))
            {
                Some((src_ts.clone(), *diff))
            } else {
                None
            }
        }));
        Ok(source_upper.frontier().to_owned())
    }

    /// Compacts the trace held by this reclock follower to the specified frontier.
    ///
    /// Reclocking has the property that it commutes with compaction. What this means is that
    /// reclocking a collection and then compacting the result to some frontier F will produce
    /// exactly the same result with first compacting the remap trace to frontier F and then
    /// reclocking the collection.
    pub fn compact(&mut self, new_since: Antichain<IntoTime>) {
        // Ignore compaction requests while we initialize
        if !self.initialized() {
            return;
        }
        let inner = &mut *self.inner.borrow_mut();
        if !PartialOrder::less_equal(&self.since, &new_since) {
            panic!(
                "ReclockFollower: new_since={} is not beyond self.since={}. inner.since={}",
                new_since.pretty(),
                self.since.pretty(),
                inner.since.pretty(),
            );
        }
        inner.since.update_iter(
            self.since
                .iter()
                .map(|t| (t.clone(), -1))
                .chain(new_since.iter().map(|t| (t.clone(), 1))),
        );
        self.since = new_since;

        // Compact the remap trace according to the computed frontier
        for (_src_ts, ts, _diff) in inner.remap_trace.iter_mut() {
            ts.advance_by(inner.since.frontier());
        }
        // And then consolidate
        consolidation::consolidate_updates(&mut inner.remap_trace);
    }

    pub fn since(&self) -> AntichainRef<'_, IntoTime> {
        self.since.borrow()
    }

    pub fn share(&self) -> Self {
        self.inner
            .borrow_mut()
            .since
            .update_iter(self.since.iter().map(|t| (t.clone(), 1)));
        Self {
            since: self.since.clone(),
            inner: Rc::clone(&self.inner),
        }
    }
}

impl<FromTime: Timestamp, IntoTime: Timestamp + Lattice + Display> Drop
    for ReclockFollower<FromTime, IntoTime>
{
    fn drop(&mut self) {
        // Release read hold
        self.compact(Antichain::new());
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReclockError<T> {
    Uninitialized,
    BeyondUpper(T),
}

/// The `ReclockOperator` is responsible for observing progress in the `FromTime` domain and
/// consume messages from a ticker of progress in the `IntoTime` domain. When the source frontier
/// advances and the ticker ticks the `ReclockOperator` will generate the data that describe this
/// correspondence and write them out to its provided remap handle. The output generated by the
/// reclock operator can be thought of as `Collection<G, FromTime>` where `G::Timestamp` is
/// `IntoTime`.
///
/// The `ReclockOperator` will always maintain the invariant that for any time `IntoTime` the remap
/// collection accumulates into an Antichain where each `FromTime` timestamp has frequency `1`. In
/// other words the remap collection describes a well formed `Antichain<FromTime>` as it is
/// marching forwards.
#[derive(Debug)]
pub struct ReclockOperator<
    FromTime: Timestamp,
    IntoTime: Timestamp + Lattice,
    Handle: RemapHandle<FromTime = FromTime, IntoTime = IntoTime>,
    Clock,
> {
    /// Upper frontier of the partial remap trace
    upper: Antichain<IntoTime>,
    /// The upper frontier in terms of `FromTime`. Any attempt to reclock messages beyond this
    /// frontier will lead to minting new bindings.
    source_upper: MutableAntichain<FromTime>,

    /// A handle allowing this operator to publish updates to and read back from the remap collection
    remap_handle: Handle,
    /// A stream of IntoTime values and upper frontiers, used to drive minting bindings
    /// In the future this will be a timely input to the reclock operator
    clock_stream: Clock,
}

#[derive(Clone, Debug)]
pub struct ReclockBatch<FromTime, IntoTime> {
    pub updates: Vec<(FromTime, IntoTime, Diff)>,
    pub upper: Antichain<IntoTime>,
}

impl<FromTime, IntoTime, Handle, Clock> ReclockOperator<FromTime, IntoTime, Handle, Clock>
where
    FromTime: Timestamp,
    IntoTime: Timestamp + Lattice,
    Handle: RemapHandle<FromTime = FromTime, IntoTime = IntoTime>,
    Clock: futures::Stream<Item = (IntoTime, Antichain<IntoTime>)> + Unpin,
{
    /// Construct a new [ReclockOperator] from the given collection metadata
    pub async fn new(
        remap_handle: Handle,
        clock_stream: Clock,
    ) -> (Self, ReclockBatch<FromTime, IntoTime>) {
        let upper = remap_handle.upper().clone();

        let mut operator = Self {
            upper: Antichain::from_elem(IntoTime::minimum()),
            source_upper: MutableAntichain::new(),
            remap_handle,
            clock_stream,
        };

        // Initialize or load the initial state that might exist in the shard
        let trace_batch = if upper.elements() == [IntoTime::minimum()] {
            let (_, upper) = operator.clock_stream.next().await.expect("end of time");
            let batch = vec![(FromTime::minimum(), IntoTime::minimum(), 1)];
            match operator.append_batch(batch, upper.clone()).await {
                Ok(trace_batch) => trace_batch,
                Err(UpperMismatch { current, .. }) => operator.sync(current.borrow()).await,
            }
        } else {
            operator.sync(upper.borrow()).await
        };

        (operator, trace_batch)
    }

    /// Advances the upper of the reclock operator if appropriate
    pub async fn advance(&mut self) -> ReclockBatch<FromTime, IntoTime> {
        // It's fine to call now_or_never here because next() is cancel safe
        match self.clock_stream.next().now_or_never() {
            Some(tick) => {
                let (_, upper) = tick.expect("end of time");
                match self.append_batch(vec![], upper.clone()).await {
                    Ok(trace_batch) => trace_batch,
                    Err(UpperMismatch { current, .. }) => self.sync(current.borrow()).await,
                }
            }
            None => ReclockBatch {
                updates: vec![],
                upper: self.upper.clone(),
            },
        }
    }

    /// Syncs the state of this operator to match that of the persist shard until the provided
    /// frontier
    async fn sync(
        &mut self,
        target_upper: AntichainRef<'_, IntoTime>,
    ) -> ReclockBatch<FromTime, IntoTime> {
        let mut updates: Vec<(FromTime, IntoTime, Diff)> = Vec::new();

        // Tail the remap collection until we reach the target upper frontier. Note that, in the
        // common case, we are also the writer, so we are waiting to read-back what we wrote
        while PartialOrder::less_than(&self.upper.borrow(), &target_upper) {
            let (mut batch, upper) = self
                .remap_handle
                .next()
                .await
                .expect("requested data after empty antichain");
            self.upper = upper;
            updates.append(&mut batch);
        }

        self.source_upper.update_iter(
            updates
                .iter()
                .map(|(src_ts, _dest_ts, diff)| (src_ts.clone(), *diff)),
        );

        ReclockBatch {
            updates,
            upper: self.upper.clone(),
        }
    }

    pub async fn mint(
        &mut self,
        new_source_upper: AntichainRef<'_, FromTime>,
    ) -> ReclockBatch<FromTime, IntoTime> {
        // The updates to the remap trace that occured during minting.
        let mut batch = ReclockBatch {
            updates: vec![],
            upper: self.upper.clone(),
        };

        while PartialOrder::less_than(&self.source_upper.frontier(), &new_source_upper) {
            let (ts, mut upper) = self
                .clock_stream
                .by_ref()
                .skip_while(|(_ts, upper)| {
                    std::future::ready(PartialOrder::less_equal(
                        &upper.borrow(),
                        &self.upper.borrow(),
                    ))
                })
                .next()
                .await
                .expect("clock stream ended without reaching the empty frontier");

            // If source is closed, close remap shard as well.
            if new_source_upper.is_empty() {
                upper = Antichain::new();
            }

            let mut updates = vec![];
            for src_ts in self.source_upper.frontier().iter().cloned() {
                updates.push((src_ts, ts.clone(), -1));
            }
            for src_ts in new_source_upper.iter().cloned() {
                updates.push((src_ts, ts.clone(), 1));
            }
            consolidation::consolidate_updates(&mut updates);

            let new_batch = match self.append_batch(updates, upper).await {
                Ok(trace_batch) => trace_batch,
                Err(UpperMismatch { current, .. }) => self.sync(current.borrow()).await,
            };
            batch.updates.extend(new_batch.updates);
            batch.upper = new_batch.upper;
        }

        batch
    }

    /// Appends the provided updates to the remap collection at the next available minting
    /// IntoTime and updates this operator's in-memory state accordingly.
    ///
    /// If an attempt to mint bindings fails due to another process having raced and appended
    /// bindings concurrently then the current global upper will be returned as an error. This is
    /// the frontier that this operator must be synced to for a future append attempt to have any
    /// chance of success.
    async fn append_batch(
        &mut self,
        updates: Vec<(FromTime, IntoTime, Diff)>,
        new_upper: Antichain<IntoTime>,
    ) -> Result<ReclockBatch<FromTime, IntoTime>, UpperMismatch<IntoTime>> {
        match self
            .remap_handle
            .compare_and_append(updates, self.upper.clone(), new_upper.clone())
            .await
        {
            // We have successfully produced data in the remap collection so let's read back what
            // we wrote to update our local state
            Ok(()) => Ok(self.sync(new_upper.borrow()).await),
            Err(mismatch) => Err(mismatch),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::Stream;
    use itertools::Itertools;
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_persist_client::{PersistLocation, ShardId};
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_repr::{GlobalId, RelationDesc, ScalarType, Timestamp};
    use mz_storage_client::controller::CollectionMetadata;
    use mz_storage_client::types::sources::{MzOffset, SourceData};
    use mz_storage_client::util::remap_handle::RemapHandle;
    use mz_timely_util::order::Partitioned;
    use once_cell::sync::Lazy;
    use timely::progress::Timestamp as _;

    use super::*;

    // 15 minutes
    static PERSIST_READER_LEASE_TIMEOUT_MS: Duration = Duration::from_secs(60 * 15);

    static PERSIST_CACHE: Lazy<Arc<PersistClientCache>> = Lazy::new(|| {
        let mut persistcfg = PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
        persistcfg.reader_lease_duration = PERSIST_READER_LEASE_TIMEOUT_MS;
        Arc::new(PersistClientCache::new(
            persistcfg,
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        ))
    });

    static PROGRESS_DESC: Lazy<RelationDesc> = Lazy::new(|| {
        RelationDesc::empty()
            .with_column(
                "partition",
                ScalarType::Range {
                    element_type: Box::new(ScalarType::Int32),
                }
                .nullable(false),
            )
            .with_column("offset", ScalarType::UInt64.nullable(true))
    });

    async fn make_test_operator(
        shard: ShardId,
        as_of: Antichain<Timestamp>,
    ) -> (
        ReclockOperator<
            Partitioned<i32, MzOffset>,
            Timestamp,
            impl RemapHandle<FromTime = Partitioned<i32, MzOffset>, IntoTime = Timestamp>,
            impl Stream<Item = (Timestamp, Antichain<Timestamp>)>,
        >,
        ReclockFollower<Partitioned<i32, MzOffset>, Timestamp>,
    ) {
        let metadata = CollectionMetadata {
            persist_location: PersistLocation {
                blob_uri: "mem://".to_owned(),
                consensus_uri: "mem://".to_owned(),
            },
            remap_shard: Some(shard),
            data_shard: ShardId::new(),
            status_shard: None,
            relation_desc: RelationDesc::empty(),
        };

        let clock_stream = futures::stream::iter((0..).map(|seconds| {
            let ts = Timestamp::from(seconds * 1000);
            let upper = Antichain::from_elem(ts.step_forward());
            (ts, upper)
        }));

        let write_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())));

        let remap_handle = crate::source::reclock::compat::PersistHandle::new(
            Arc::clone(&*PERSIST_CACHE),
            metadata,
            as_of.clone(),
            write_frontier,
            GlobalId::Explain,
            "unittest",
            0,
            1,
            PROGRESS_DESC.clone(),
        )
        .await
        .unwrap();

        let (operator, initial_batch) = ReclockOperator::new(remap_handle, clock_stream).await;

        let mut follower = ReclockFollower::new(as_of);

        // Push any updates that might already exist in the persist shard to the follower.
        follower.push_trace_batch(initial_batch);

        (operator, follower)
    }

    /// Generates a `Partitioned<i32, MzOffset>` antichain where all the provided
    /// partitions are at the specified offset and the gaps in between are filled with range
    /// timestamps at offset zero.
    fn partitioned_frontier<I>(items: I) -> Antichain<Partitioned<i32, MzOffset>>
    where
        I: IntoIterator<Item = (i32, MzOffset)>,
    {
        let mut frontier = Antichain::new();
        let mut prev = None;
        for (pid, offset) in items {
            assert!(prev.as_ref() < Some(&pid));
            let gap = Partitioned::with_range(prev.clone(), Some(pid.clone()), MzOffset::from(0));
            frontier.extend([gap, Partitioned::with_partition(pid.clone(), offset)]);
            prev = Some(pid);
        }
        frontier.insert(Partitioned::with_range(prev, None, MzOffset::from(0)));
        frontier
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    async fn test_basic_usage() {
        let (mut operator, mut follower) =
            make_test_operator(ShardId::new(), Antichain::from_elem(0.into())).await;

        // Reclock offsets 1 and 3 to timestamp 1000
        let batch = vec![
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(4))]);
        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);

        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(
            reclocked_msgs,
            &[(1, 1000.into()), (1, 1000.into()), (3, 1000.into())]
        );

        // This will return the antichain containing 1000 because that's where future messages will
        // offset 1 will be reclocked to
        let query = partitioned_frontier([(0, MzOffset::from(1))]);
        assert_eq!(
            Ok(Antichain::from_elem(1000.into())),
            follower.reclock_frontier(query.borrow())
        );

        // Reclock more messages for offsets 3 to the same timestamp
        let batch = vec![
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
        ];
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(3, 1000.into()), (3, 1000.into())]);

        // We're done with offset 3. Now reclocking the source upper will result to the overall
        // target upper (1001) because any new bindings will be minted beyond that timestamp.
        let query = partitioned_frontier([(0, MzOffset::from(4))]);

        assert_eq!(
            Ok(Antichain::from_elem(1001.into())),
            follower.reclock_frontier(query.borrow())
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    async fn test_reclock_frontier() {
        let persist_location = PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        };

        let remap_shard = ShardId::new();

        let persist_client = PERSIST_CACHE
            .open(persist_location)
            .await
            .expect("error creating persist client");

        let mut remap_read_handle = persist_client
            .open_leased_reader::<SourceData, (), Timestamp, Diff>(
                remap_shard,
                "test_since_hold",
                Arc::new(PROGRESS_DESC.clone()),
                Arc::new(UnitSchema),
            )
            .await
            .expect("error opening persist shard");

        let (mut operator, mut follower) =
            make_test_operator(remap_shard, Antichain::from_elem(0.into())).await;

        let query = Antichain::from_elem(Partitioned::minimum());
        // This is the initial source frontier so we should get the initial ts upper
        assert_eq!(
            Ok(Antichain::from_elem(1.into())),
            follower.reclock_frontier(query.borrow())
        );

        // Mint a couple of bindings for multiple partitions
        follower.push_trace_batch(
            operator
                .mint(partitioned_frontier([(1, MzOffset::from(10))]).borrow())
                .await,
        );

        follower.push_trace_batch(
            operator
                .mint(
                    partitioned_frontier([(1, MzOffset::from(10)), (2, MzOffset::from(10))])
                        .borrow(),
                )
                .await,
        );

        let mut remap_trace = BTreeSet::new();
        remap_trace.extend(follower.inner.borrow().remap_trace.clone());
        assert_eq!(
            remap_trace,
            BTreeSet::from_iter([
                // Initial state
                (
                    Partitioned::with_range(None, None, MzOffset::from(0)),
                    0.into(),
                    1
                ),
                // updates from first mint
                (
                    Partitioned::with_range(None, Some(1), MzOffset::from(0)),
                    1000.into(),
                    1
                ),
                (
                    Partitioned::with_range(None, None, MzOffset::from(0)),
                    1000.into(),
                    -1
                ),
                (
                    Partitioned::with_range(Some(1), None, MzOffset::from(0)),
                    1000.into(),
                    1
                ),
                (
                    Partitioned::with_partition(1, MzOffset::from(10)),
                    1000.into(),
                    1
                ),
                // updates from second mint
                (
                    Partitioned::with_range(Some(1), Some(2), MzOffset::from(0)),
                    2000.into(),
                    1
                ),
                (
                    Partitioned::with_range(Some(1), None, MzOffset::from(0)),
                    2000.into(),
                    -1
                ),
                (
                    Partitioned::with_range(Some(2), None, MzOffset::from(0)),
                    2000.into(),
                    1
                ),
                (
                    Partitioned::with_partition(2, MzOffset::from(10)),
                    2000.into(),
                    1
                ),
            ])
        );

        // The initial frontier should now map to the minimum between the two partitions
        let query = Antichain::from_elem(Partitioned::minimum());
        assert_eq!(
            Ok(Antichain::from_elem(1000.into())),
            follower.reclock_frontier(query.borrow())
        );

        // Map a frontier that advances only one of the partitions
        let query = partitioned_frontier([(1, MzOffset::from(9))]);
        assert_eq!(
            Ok(Antichain::from_elem(1000.into())),
            follower.reclock_frontier(query.borrow())
        );
        let query = partitioned_frontier([(1, MzOffset::from(10))]);
        assert_eq!(
            Ok(Antichain::from_elem(2000.into())),
            follower.reclock_frontier(query.borrow())
        );
        // A frontier that is the upper of both partitions should map to the timestamp upper
        let query = partitioned_frontier([(1, MzOffset::from(10)), (2, MzOffset::from(10))]);
        assert_eq!(
            Ok(Antichain::from_elem(2001.into())),
            follower.reclock_frontier(query.borrow())
        );

        // Advance the operator and confirm that we get to the next timestamp
        follower.push_trace_batch(operator.advance().await);
        let query = partitioned_frontier([(1, MzOffset::from(10)), (2, MzOffset::from(10))]);
        assert_eq!(
            Ok(Antichain::from_elem(3001.into())),
            follower.reclock_frontier(query.borrow())
        );

        // Compact but not enough to change the bindings
        remap_read_handle
            .downgrade_since(&Antichain::from_elem(900.into()))
            .await;
        follower.compact(Antichain::from_elem(900.into()));
        let query = partitioned_frontier([(1, MzOffset::from(9))]);
        assert_eq!(
            Ok(Antichain::from_elem(1000.into())),
            follower.reclock_frontier(query.borrow())
        );

        // Compact enough to compact bindings
        remap_read_handle
            .downgrade_since(&Antichain::from_elem(1500.into()))
            .await;
        follower.compact(Antichain::from_elem(1500.into()));
        let query = partitioned_frontier([(1, MzOffset::from(9))]);
        // Now reclocking the same offset maps to the compacted binding, which is the same result
        // as if we had reclocked offset 9 with the uncompacted bindings and then compacted that.
        assert_eq!(
            Ok(Antichain::from_elem(1500.into())),
            follower.reclock_frontier(query.borrow())
        );
        let query = partitioned_frontier([(1, MzOffset::from(10))]);
        assert_eq!(
            Ok(Antichain::from_elem(2000.into())),
            follower.reclock_frontier(query.borrow())
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    async fn test_reclock() {
        let (mut operator, mut follower) =
            make_test_operator(ShardId::new(), Antichain::from_elem(0.into())).await;

        // Reclock offsets 1 and 2 to timestamp 1000
        let batch = vec![
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (2, Partitioned::with_partition(0, MzOffset::from(2))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(3))]);

        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(1, 1000.into()), (2, 1000.into())]);

        // Reclock offsets 3 and 4 to timestamp 2000
        let batch = vec![
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
            (4, Partitioned::with_partition(0, MzOffset::from(4))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(5))]);

        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(3, 2000.into()), (4, 2000.into())]);

        // Reclock the same offsets again
        let batch = vec![
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (2, Partitioned::with_partition(0, MzOffset::from(2))),
        ];

        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(1, 1000.into()), (2, 1000.into())]);

        // Reclock a batch with offsets that spans multiple bindings
        let batch = vec![
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (2, Partitioned::with_partition(0, MzOffset::from(2))),
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
            (4, Partitioned::with_partition(0, MzOffset::from(4))),
        ];
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(
            reclocked_msgs,
            &[
                (1, 1000.into()),
                (2, 1000.into()),
                (3, 2000.into()),
                (4, 2000.into()),
            ]
        );

        // Reclock a batch that contains multiple messages having the same offset
        let batch = vec![
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
        ];
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(
            reclocked_msgs,
            &[
                (1, 1000.into()),
                (1, 1000.into()),
                (3, 2000.into()),
                (3, 2000.into()),
            ]
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    async fn test_reclock_gh16318() {
        let (mut operator, mut follower) =
            make_test_operator(ShardId::new(), Antichain::from_elem(0.into())).await;

        // First mint bindings for 0 at timestamp 1000
        let source_upper = partitioned_frontier([(0, MzOffset::from(50))]);
        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);

        // Then only for 1 at timestamp 2000
        let source_upper = partitioned_frontier([(0, MzOffset::from(50)), (1, MzOffset::from(50))]);
        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);

        // Then again only for 0 at timestamp 3000
        let source_upper =
            partitioned_frontier([(0, MzOffset::from(100)), (1, MzOffset::from(50))]);
        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);

        // Reclockng (0, 50) must ignore the updates on the FromTime frontier that happened at
        // timestamp 2000 since those are completely unrelated
        let batch = vec![(50, Partitioned::with_partition(0, MzOffset::from(50)))];
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(50, 3000.into())]);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    async fn test_compaction() {
        let persist_location = PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        };

        let remap_shard = ShardId::new();

        let persist_client = PERSIST_CACHE
            .open(persist_location)
            .await
            .expect("error creating persist client");

        let mut remap_read_handle = persist_client
            .open_leased_reader::<SourceData, (), Timestamp, Diff>(
                remap_shard,
                "test_since_hold",
                Arc::new(PROGRESS_DESC.clone()),
                Arc::new(UnitSchema),
            )
            .await
            .expect("error opening persist shard");

        let (mut operator, mut follower) =
            make_test_operator(remap_shard, Antichain::from_elem(0.into())).await;

        // Reclock offsets 1 and 2 to timestamp 1000
        let batch = vec![
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (2, Partitioned::with_partition(0, MzOffset::from(2))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(3))]);

        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(1, 1000.into()), (2, 1000.into())]);

        // Reclock offsets 3 and 4 to timestamp 2000
        let batch = vec![
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
            (4, Partitioned::with_partition(0, MzOffset::from(4))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(5))]);

        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(3, 2000.into()), (4, 2000.into())]);

        // Compact enough so that offsets >= 3 remain uncompacted
        remap_read_handle
            .downgrade_since(&Antichain::from_elem(1000.into()))
            .await;
        follower.compact(Antichain::from_elem(1000.into()));

        // Reclock offsets 3 and 4 again to see we get the uncompacted result
        let batch = vec![
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
            (4, Partitioned::with_partition(0, MzOffset::from(4))),
        ];

        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(3, 2000.into()), (4, 2000.into())]);

        // Attempting to reclock offset 2 should return compacted bindings
        let src_ts = Partitioned::with_partition(0, MzOffset::from(2));
        let batch = vec![(2, src_ts.clone())];

        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(2, 1000.into())]);

        // Starting a new operator with an `as_of` is the same as having compacted
        let (_operator, follower) =
            make_test_operator(remap_shard, Antichain::from_elem(1000.into())).await;

        // Reclocking offsets 3 and 4 should succeed
        let batch = vec![
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
            (4, Partitioned::with_partition(0, MzOffset::from(4))),
        ];

        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(3, 2000.into()), (4, 2000.into())]);

        // But attempting to reclock offset 2 should return an error
        let batch = vec![(2, Partitioned::with_partition(0, MzOffset::from(2)))];

        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(2, 1000.into())]);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    async fn test_sharing() {
        let (mut operator, mut follower) =
            make_test_operator(ShardId::new(), Antichain::from_elem(0.into())).await;

        // Install a since hold
        let shared_follower = follower.share();

        // First mint bindings for partition 0 offset 1 at timestamp 1000
        let source_upper = partitioned_frontier([(0, MzOffset::from(1))]);
        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);

        // Advance the since frontier on one of the handles at a timestamp that is less than 1000
        // to leave the previously minted binding intact. Since we have an active since hold
        // through `shared_follower` nothing in the trace is actually compacted.
        follower.compact(Antichain::from_elem(500.into()));

        // This will release since hold of {0} through `shared_follower` and the overall since
        // frontier will become {500} which must now actually compact the in-memory trace.
        drop(shared_follower);

        // Verify that we reclock partition 0 offset 0 correctly
        let batch = vec![(0, Partitioned::with_partition(0, MzOffset::from(0)))];
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(0, 1000.into())]);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    async fn test_concurrency() {
        // Create two operators pointing to the same shard
        let shared_shard = ShardId::new();
        let (mut op_a, mut follower_a) =
            make_test_operator(shared_shard, Antichain::from_elem(0.into())).await;
        let (mut op_b, mut follower_b) =
            make_test_operator(shared_shard, Antichain::from_elem(0.into())).await;

        // Reclock a batch from one of the operators
        // Reclock offsets 1 and 2 to timestamp 1000 from operator A
        let batch = vec![
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (2, Partitioned::with_partition(0, MzOffset::from(2))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(3))]);

        follower_a.push_trace_batch(op_a.mint(source_upper.borrow()).await);
        let reclocked_msgs = follower_a
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(1, 1000.into()), (2, 1000.into())]);

        follower_a.compact(Antichain::from_elem(1000.into()));

        // Advance the time by a lot
        op_b.clock_stream.by_ref().take(10).count().await;

        // Reclock a batch that includes messages from the bindings already minted
        let batch = vec![
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (2, Partitioned::with_partition(0, MzOffset::from(2))),
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
            (4, Partitioned::with_partition(0, MzOffset::from(4))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(5))]);
        // This operator should attempt to mint in one go, fail, re-sync, and retry only for the
        // bindings that still need minting
        follower_b.push_trace_batch(op_b.mint(source_upper.borrow()).await);
        let reclocked_msgs = follower_b
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(
            reclocked_msgs,
            &[
                (1, 1000.into()),
                (2, 1000.into()),
                (3, 11000.into()),
                (4, 11000.into())
            ]
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    async fn test_inversion() {
        let persist_location = PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        };

        let remap_shard = ShardId::new();

        let persist_client = PERSIST_CACHE
            .open(persist_location)
            .await
            .expect("error creating persist client");

        let mut remap_read_handle = persist_client
            .open_leased_reader::<SourceData, (), Timestamp, Diff>(
                remap_shard,
                "test_since_hold",
                Arc::new(PROGRESS_DESC.clone()),
                Arc::new(UnitSchema),
            )
            .await
            .expect("error opening persist shard");

        let (mut operator, mut follower) =
            make_test_operator(remap_shard, Antichain::from_elem(0.into())).await;

        // SETUP
        // Reclock offsets 1 and 2 to timestamp 1000
        let batch = vec![
            (1, Partitioned::with_partition(0, MzOffset::from(1))),
            (2, Partitioned::with_partition(0, MzOffset::from(2))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(3))]);

        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(1, 1000.into()), (2, 1000.into())]);
        // Reclock offsets 3 and 4 to timestamp 2000
        let batch = vec![
            (3, Partitioned::with_partition(0, MzOffset::from(3))),
            (4, Partitioned::with_partition(0, MzOffset::from(4))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(5))]);

        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(3, 2000.into()), (4, 2000.into())]);
        // Reclock offsets 5 and 6 to timestamp 3000
        let batch = vec![
            (5, Partitioned::with_partition(0, MzOffset::from(5))),
            (6, Partitioned::with_partition(0, MzOffset::from(6))),
        ];
        let source_upper = partitioned_frontier([(0, MzOffset::from(7))]);

        follower.push_trace_batch(operator.mint(source_upper.borrow()).await);
        let reclocked_msgs = follower
            .reclock(batch)
            .map(|(m, ts)| (m, ts.unwrap()))
            .collect_vec();
        assert_eq!(reclocked_msgs, &[(5, 3000.into()), (6, 3000.into())]);

        // END SETUP
        //

        // If we source_upper_at_frontier at the current `upper`, we should get the offset
        // upper (strictly greater!!) back!
        assert_eq!(
            follower
                .source_upper_at_frontier(Antichain::from_elem(3001.into()).borrow())
                .unwrap(),
            partitioned_frontier([(0, MzOffset::from(7))])
        );
        // Check out "upper strictly greater is correct
        assert_eq!(
            follower
                .source_upper_at_frontier(Antichain::from_elem(3000.into()).borrow())
                .unwrap(),
            // Note this is the UPPER offset for the previous part of
            // the trace.
            partitioned_frontier([(0, MzOffset::from(5))])
        );
        // random time in the middle of 2 pieces of the trace
        assert_eq!(
            follower
                .source_upper_at_frontier(Antichain::from_elem(2500.into()).borrow())
                .unwrap(),
            // Note this is the UPPER offset for the previous part of
            // the trace.
            partitioned_frontier([(0, MzOffset::from(5))])
        );

        // Check startup edge-case (the since is still 0 here) doesn't panic.
        assert_eq!(
            follower
                .source_upper_at_frontier(Antichain::from_elem(Timestamp::minimum()).borrow())
                .unwrap(),
            Antichain::from_elem(Partitioned::minimum())
        );

        // Similarly, for an earlier part of the trace,
        // we get the upper for that section of the trace
        assert_eq!(
            follower
                .source_upper_at_frontier(Antichain::from_elem(2001.into()).borrow())
                .unwrap(),
            partitioned_frontier([(0, MzOffset::from(5))])
        );
        // upper logic, as before
        assert_eq!(
            follower
                .source_upper_at_frontier(Antichain::from_elem(2000.into()).borrow())
                .unwrap(),
            partitioned_frontier([(0, MzOffset::from(3))])
        );

        // After compaction it should still work
        remap_read_handle
            .downgrade_since(&Antichain::from_elem(1000.into()))
            .await;
        follower.compact(Antichain::from_elem(1000.into()));
        assert_eq!(
            follower
                .source_upper_at_frontier(Antichain::from_elem(2001.into()).borrow())
                .unwrap(),
            partitioned_frontier([(0, MzOffset::from(5))])
        );
        // compact as close as we can
        remap_read_handle
            .downgrade_since(&Antichain::from_elem(2000.into()))
            .await;
        follower.compact(Antichain::from_elem(2000.into()));
        assert_eq!(
            follower
                .source_upper_at_frontier(Antichain::from_elem(2001.into()).borrow())
                .unwrap(),
            partitioned_frontier([(0, MzOffset::from(5))])
        );

        // If we compact too far, we get an error. Note we compact
        // to the previous UPPER we were checking.
        remap_read_handle
            .downgrade_since(&Antichain::from_elem(2001.into()))
            .await;
        follower.compact(Antichain::from_elem(2001.into()));

        assert_eq!(
            follower.source_upper_at_frontier(Antichain::from_elem(2001.into()).borrow()),
            Ok(Antichain::from_elem(Partitioned::minimum()))
        );
    }

    // Regression test for
    // https://github.com/MaterializeInc/materialize/issues/14740.
    #[mz_ore::test(tokio::test(start_paused = true))]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    async fn test_since_hold() {
        let binding_shard = ShardId::new();

        let (mut operator, _follower) =
            make_test_operator(binding_shard, Antichain::from_elem(0.into())).await;

        // We do multiple rounds of minting. This will downgrade the since of
        // the internal listen. If we didn't make sure to also heartbeat the
        // internal handle that holds back the overall remap since the checks
        // below would fail.
        //
        // We do two rounds and advance the time by half the lease timeout in
        // between so that the "listen handle" will not timeout but the internal
        // handle used for holding back the since will timeout.

        tokio::time::advance(PERSIST_READER_LEASE_TIMEOUT_MS / 2 + Duration::from_millis(1)).await;
        let source_upper = partitioned_frontier([(0, MzOffset::from(3))]);
        let _ = operator.mint(source_upper.borrow()).await;

        tokio::time::advance(PERSIST_READER_LEASE_TIMEOUT_MS / 2 + Duration::from_millis(1)).await;
        let source_upper = partitioned_frontier([(0, MzOffset::from(5))]);
        let _ = operator.mint(source_upper.borrow()).await;

        // Allow time for background maintenance work, which does lease
        // expiration. 1 ms is enough here, we just need to yield to allow the
        // background task to be "scheduled".
        tokio::time::sleep(Duration::from_millis(1)).await;

        // Starting a new operator with an `as_of` of `0`, to verify that
        // holding back the `since` of the remap shard works as expected.
        let (_operator, _follower) =
            make_test_operator(binding_shard, Antichain::from_elem(0.into())).await;

        // Also manually assert the since of the remap shard.
        let persist_location = PersistLocation {
            blob_uri: "mem://".to_owned(),
            consensus_uri: "mem://".to_owned(),
        };

        let persist_client = PERSIST_CACHE
            .open(persist_location)
            .await
            .expect("error creating persist client");

        let read_handle = persist_client
            .open_leased_reader::<SourceData, (), Timestamp, Diff>(
                binding_shard,
                "test_since_hold",
                Arc::new(PROGRESS_DESC.clone()),
                Arc::new(UnitSchema),
            )
            .await
            .expect("error opening persist shard");

        assert_eq!(
            Antichain::from_elem(0.into()),
            read_handle.since().to_owned()
        );
    }
}
