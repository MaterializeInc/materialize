// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timestamper using persistent collection
use std::borrow::Borrow;
use std::cell::{Ref, RefCell};
use std::collections::hash_map::{self, HashMap};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use differential_dataflow::consolidation;
use differential_dataflow::lattice::Lattice as _;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp as _;
use timely::PartialOrder;
use tokio::sync::Mutex;

use mz_expr::PartitionId;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::{Listen, ListenEvent, ReadHandle};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::Timestamp;
use tracing::trace;

use crate::controller::CollectionMetadata;
use crate::types::sources::MzOffset;

/// A "follower" for the ReclockOperator, that maintains
/// a trace based on the results of reclocking and data from
/// the source. It provides the `reclock` method, which
/// produces messages with their associated timestamps.
///
/// Shareable with `.share()`
pub struct ReclockFollower {
    inner: Rc<RefCell<ReclockFollowerInner>>,
}

struct ReclockFollowerInner {
    /// A dTVC trace of the remap collection containing all consolidated updates at
    /// `t` such that `since <= t < upper` indexed by partition and sorted by time.
    remap_trace: HashMap<PartitionId, Vec<(Timestamp, MzOffset)>>,
    /// Since frontier of the partial remap trace
    since: Antichain<Timestamp>,
    /// Upper frontier of the partial remap trace
    upper: Antichain<Timestamp>,
    /// The upper frontier in terms of `SourceTime`. Any attempt to reclock messages beyond this
    /// frontier will lead to minting new bindings.
    source_upper: HashMap<PartitionId, MzOffset>,
}

impl ReclockFollower {
    /// Construct a new [ReclockOperator] from the given collection metadata
    pub fn new(as_of: Antichain<Timestamp>) -> Self {
        Self {
            inner: Rc::new(RefCell::new(ReclockFollowerInner {
                remap_trace: HashMap::new(),
                since: as_of,
                upper: Antichain::from_elem(Timestamp::minimum()),
                source_upper: HashMap::new(),
            })),
        }
    }

    pub fn source_upper(&self) -> Ref<HashMap<PartitionId, MzOffset>> {
        // `borrow` overlaps with `std::borrow::Borrow` so we have to do this
        Ref::map(RefCell::borrow(&self.inner), |inner| &inner.source_upper)
    }

    /// Pushes new trace updates into this [`ReclockFollower`].
    pub fn push_trace_updates(
        &self,
        updates: impl IntoIterator<Item = (PartitionId, Vec<(Timestamp, MzOffset)>)>,
    ) {
        let mut inner = self.inner.borrow_mut();
        for (pid, updates) in updates {
            for (ts, diff) in updates {
                let bindings = inner.remap_trace.entry(pid.clone()).or_default();
                bindings.push((ts, diff));

                *inner.source_upper.entry(pid.clone()).or_default() += diff;
            }
        }
    }

    /// Updates the upper based on information received from
    /// [`ReclockOperator`].
    pub fn push_upper_update(&self, upper: Antichain<Timestamp>) {
        self.inner.borrow_mut().upper = upper;
    }

    /// Reclocks a batch of messages timestamped with `SourceTime` and returns an iterator of
    /// messages timestamped with `DestTime`.
    ///
    /// The returned iterator will drain the provided batch as it being consumed. It is not
    /// guaranteed that the provided batch will be cleared if the iterator is dropped in a
    /// partially consumed state.
    ///
    /// The method returns an error if any of the messages is timestamped at a `SourceTime` that is
    /// not beyond the since frontier. The error will contain the offending `SourceTime`.
    ///
    /// This method returns `None` if we don't yet have enough bindings to cover
    /// this batch, that is if the frontier in `SourceTime` is not yet advanced
    /// far enough.
    pub fn reclock<'a, M>(
        &'a self,
        batch: &'a mut HashMap<PartitionId, Vec<(M, MzOffset)>>,
    ) -> Result<Option<ReclockIter<'a, M>>, (PartitionId, MzOffset)> {
        let inner = RefCell::borrow(&self.inner);

        let mut batch_upper = HashMap::with_capacity(batch.len());
        for (pid, messages) in batch.iter_mut() {
            messages.sort_unstable_by(|a, b| a.1.cmp(&b.1));
            if let Some((_msg, offset)) = messages.first() {
                let part_since = inner.partition_since(pid);
                if !(part_since <= *offset) {
                    return Err((pid.clone(), *offset));
                }
            }
            if let Some((_msg, offset)) = messages.last() {
                batch_upper.insert(pid, *offset + 1);
            }
        }

        // Ensure we have enough bindings
        for (pid, offset) in batch_upper {
            let bindings_upper = inner.source_upper.get(&pid);
            if let Some(bindings_upper) = bindings_upper {
                if &offset > bindings_upper {
                    trace!("offset {} >= bindings_upper {}", offset, bindings_upper);
                    return Ok(None);
                }
            } else {
                return Ok(None);
            }
        }

        Ok(Some(ReclockIter {
            reclock: inner,
            messages: batch.iter_mut(),
        }))
    }

    /// Reclocks a `SourceTime` frontier into a `DestTime` frontier.
    ///
    /// The conversion has the property that all messages that are beyond the provided `SourceTime`
    /// frontier will be relocked at times that will be beyond the returned `DestTime` frontier.
    /// This can be used to drive a `DestTime` capability forward when the caller knows that a
    /// `SourceTime` frontier has advanced.
    ///
    /// The method returns an error if the `SourceTime` frontier is not beyond the since frontier.
    /// The error will contain the offending `SourceTime`.
    pub fn reclock_frontier(
        &self,
        source_frontier: &HashMap<PartitionId, MzOffset>,
    ) -> Result<Antichain<Timestamp>, (PartitionId, MzOffset)> {
        let inner = RefCell::borrow(&self.inner);
        // The upper is the greatest frontier that we can ever return
        let mut dest_frontier = inner.upper.clone();

        let mut partitions = HashSet::new();
        partitions.extend(inner.source_upper.keys());
        partitions.extend(source_frontier.keys());
        // To refine it we have to go through all the partitions we know about and:
        for pid in partitions {
            let offset = source_frontier.get(pid).copied().unwrap_or_default();
            // Ensure that the offsets are beyond the source since frontier
            if !(inner.partition_since(pid) <= offset) {
                return Err((pid.clone(), offset));
            }
            // If a binding exists whose upper is greater than `offset` then all messages that are
            // beyond `offset` will be reclocked at a time that is beyond that binding's time.
            let binding = inner
                .partition_bindings(pid)
                .find(|(_, upper)| offset < *upper);
            if let Some((ts, _)) = binding {
                // Adding to the frontier will "pull" it backwards if this timestamp is less than
                // the its current value.
                dest_frontier.insert(ts);
            }
            // If no such binding exists then the offset in question needs bindings to be minted
            // that will certainly be beyond the upper. Therefore the upper fits the property that
            // this method promises and since `dest_frontier` was initialized with it we have
            // nothing to do.
        }

        Ok(dest_frontier)
    }

    /// Compacts the internal state
    #[allow(dead_code)]
    pub fn compact(&self, new_since: Antichain<Timestamp>) {
        self.inner.borrow_mut().compact(new_since)
    }

    /// Create a shallow copy of this struct that shares the underlying trace.
    #[allow(dead_code)]
    pub fn share(&self) -> Self {
        Self {
            inner: Rc::clone(&self.inner),
        }
    }
}

impl ReclockFollowerInner {
    pub fn compact(&mut self, new_since: Antichain<Timestamp>) {
        assert!(PartialOrder::less_equal(&self.since, &new_since));
        for bindings in self.remap_trace.values_mut() {
            // Compact the remap trace according to the computed frontier
            for (timestamp, _) in bindings.iter_mut() {
                timestamp.advance_by(new_since.borrow());
            }
            // And then consolidate
            consolidation::consolidate(bindings);
        }
        self.since = new_since;
    }

    /// Returns an iterator of timestamp bindings for a given partition
    fn partition_bindings(&self, pid: &PartitionId) -> PartitionBindings {
        let bindings = match self.remap_trace.get(pid) {
            Some(bindings) => (*bindings).iter(),
            None => (&[]).iter(),
        };
        PartitionBindings {
            offset: MzOffset::default(),
            bindings,
        }
    }
    /// Returns the since frontier for a given partition
    fn partition_since(&self, pid: &PartitionId) -> MzOffset {
        if self.since.elements() == [Timestamp::minimum()] {
            // If we never compacted in the DestTime domain then the SourceTime domain isn't
            // compated either. Therefore the since frontier is zero
            MzOffset::default()
        } else {
            // If we have compacted there are two posibilities. Either the since frontier is at
            // exactly the time of the first binding (and will continue to do so since times are
            // advanced during compaction), or it is behind it.
            let (first_ts, first_offset) = self
                .remap_trace
                .get(pid)
                .and_then(|b| b.first())
                .copied()
                .unwrap_or_default();

            if self.since.less_than(&first_ts) {
                // If it is behind then the first binding will cover all offsets starting at zero,
                // so the since frontier of the partition is also zero.
                MzOffset::default()
            } else {
                // Otherwise the since frontier is the offset of the first binding
                first_offset
            }
        }
    }
}

/// The reclock operator reclocks a stream that is timestamped with some timestamp `SourceTime`
/// into another time domain that is timestamped with some timestamp `DestTime`.
///
/// Currently the `SourceTime` is hardcoded to `(PartitionId, MzOffset)`
/// and `DestTime` is hardcoded to `mz_repr::Timestamp`
pub struct ReclockOperator {
    /// A dTVC trace of the remap collection containing all consolidated updates at
    /// `t` such that `since <= t < upper` indexed by partition and sorted by time.
    remap_trace: HashMap<PartitionId, Vec<(Timestamp, MzOffset)>>,
    /// Since frontier of the partial remap trace
    since: Antichain<Timestamp>,
    /// Upper frontier of the partial remap trace
    upper: Antichain<Timestamp>,
    /// The upper frontier in terms of `SourceTime`. Any attempt to reclock messages beyond this
    /// frontier will lead to minting new bindings.
    source_upper: HashMap<PartitionId, MzOffset>,

    /// Write handle of the remap persist shard
    write_handle: WriteHandle<(), PartitionId, Timestamp, MzOffset>,
    /// Read handle of the remap persist shard
    ///
    /// NB: Until #13534 is addressed, this intentionally holds back the since
    /// of the remap shard indefinitely.
    read_handle: ReadHandle<(), PartitionId, Timestamp, MzOffset>,
    /// A listener to tail the remap shard for new updates
    listener: Listen<(), PartitionId, Timestamp, MzOffset>,
    /// The function that should be used to get the current time when minting new bindings
    now: NowFn,
    /// Values of current time will be rounded to be multiples of this duration in milliseconds
    update_interval_ms: u64,
}

impl ReclockOperator {
    /// Construct a new [ReclockOperator] from the given collection metadata
    pub async fn new(
        persist_clients: Arc<Mutex<PersistClientCache>>,
        metadata: CollectionMetadata,
        now: NowFn,
        update_interval: Duration,
        as_of: Antichain<Timestamp>,
    ) -> anyhow::Result<Self> {
        let mut persist_clients = persist_clients.lock().await;
        let persist_client = persist_clients
            .open(metadata.persist_location)
            .await
            .context("error creating persist client")?;
        drop(persist_clients);

        let (write_handle, read_handle) = persist_client
            .open(metadata.remap_shard)
            .await
            .context("error opening persist shard")?;

        let (since, upper) = (read_handle.since(), write_handle.upper().clone());

        assert!(
            PartialOrder::less_equal(since, &as_of),
            "invalid as_of: as_of({as_of:?}) < since({since:?})"
        );

        assert!(
            as_of.elements() == [Timestamp::minimum()] || PartialOrder::less_than(&as_of, &upper),
            "invalid as_of: upper({upper:?}) <= as_of({as_of:?})",
        );

        let listener = read_handle
            .clone()
            .await
            .listen(as_of.clone())
            .await
            .expect("since <= as_of asserted");

        let mut operator = Self {
            remap_trace: HashMap::new(),
            since: as_of.clone(),
            upper: Antichain::from_elem(Timestamp::minimum()),
            source_upper: HashMap::new(),
            write_handle,
            read_handle,
            listener,
            now,
            update_interval_ms: update_interval
                .as_millis()
                .try_into()
                .expect("huge duration"),
        };

        // Load the initial state that might exist in the shard
        operator.sync(&upper).await;

        Ok(operator)
    }

    /// Reclocks a `SourceTime` frontier into a `DestTime` frontier.
    ///
    /// The conversion has the property that all messages that are beyond the provided `SourceTime`
    /// frontier will be relocked at times that will be beyond the returned `DestTime` frontier.
    /// This can be used to drive a `DestTime` capability forward when the caller knows that a
    /// `SourceTime` frontier has advanced.
    ///
    /// The method returns an error if the `SourceTime` frontier is not beyond the since frontier.
    /// The error will contain the offending `SourceTime`.
    pub fn reclock_frontier(
        &self,
        source_frontier: &HashMap<PartitionId, MzOffset>,
    ) -> Result<Antichain<Timestamp>, (PartitionId, MzOffset)> {
        // The upper is the greatest frontier that we can ever return
        let mut dest_frontier = self.upper.clone();

        let mut partitions = HashSet::new();
        partitions.extend(self.source_upper.keys());
        partitions.extend(source_frontier.keys());
        // To refine it we have to go through all the partitions we know about and:
        for pid in partitions {
            let offset = source_frontier.get(pid).copied().unwrap_or_default();
            // Ensure that the offsets are beyond the source since frontier
            if !(self.partition_since(pid) <= offset) {
                return Err((pid.clone(), offset));
            }
            // If a binding exists whose upper is greater than `offset` then all messages that are
            // beyond `offset` will be reclocked at a time that is beyond that binding's time.
            let binding = self
                .partition_bindings(pid)
                .find(|(_, upper)| offset < *upper);
            if let Some((ts, _)) = binding {
                // Adding to the frontier will "pull" it backwards if this timestamp is less than
                // the its current value.
                dest_frontier.insert(ts);
            }
            // If no such binding exists then the offset in question needs bindings to be minted
            // that will certainly be beyond the upper. Therefore the upper fits the property that
            // this method promises and since `dest_frontier` was initialized with it we have
            // nothing to do.
        }

        Ok(dest_frontier)
    }

    /// Compacts the internal state
    #[allow(dead_code)]
    pub async fn compact(&mut self, new_since: Antichain<Timestamp>) {
        assert!(PartialOrder::less_equal(&self.since, &new_since));
        for bindings in self.remap_trace.values_mut() {
            // Compact the remap trace according to the computed frontier
            for (timestamp, _) in bindings.iter_mut() {
                timestamp.advance_by(new_since.borrow());
            }
            // And then consolidate
            consolidation::consolidate(bindings);
        }
        self.since = new_since;
        self.read_handle.maybe_downgrade_since(&self.since).await;
    }

    /// Advances the upper of the reclock operator if appropriate
    pub async fn advance(&mut self) {
        if self.next_mint_timestamp().is_ok() {
            let empty: Vec<(PartitionId, MzOffset)> = Vec::new();
            while let Err(Upper(actual_upper)) = self.append(&empty).await {
                self.sync(&actual_upper).await;
            }
        }
    }

    /// Calculates the source upper frontier at a particular timestamp
    pub fn source_upper_at(&self, target: Timestamp) -> HashMap<PartitionId, MzOffset> {
        let mut source_upper = HashMap::new();
        for pid in self.remap_trace.keys() {
            let binding = self
                .partition_bindings(pid)
                .take_while(|(ts, _)| ts <= &target)
                .last();
            if let Some((_, part_upper)) = binding {
                source_upper.insert(pid.clone(), part_upper);
            }
        }
        source_upper
    }

    /// Syncs the state of this operator to match that of the persist shard until the provided
    /// frontier
    async fn sync(
        &mut self,
        target_upper: &Antichain<Timestamp>,
    ) -> Vec<(PartitionId, Vec<(Timestamp, MzOffset)>)> {
        let mut pending_batch = vec![];

        let mut trace_updates: HashMap<PartitionId, Vec<(Timestamp, MzOffset)>> = HashMap::new();

        // If this is the first sync and the collection is non-empty load the initial snapshot
        let first_sync = self.upper.elements() == [Timestamp::minimum()];
        if first_sync && PartialOrder::less_than(&self.upper, target_upper) {
            for ((_, pid), ts, diff) in self
                .read_handle
                .snapshot_and_fetch(self.since.clone())
                .await
                .expect("local since is not beyond read handle's since")
            {
                let pid = pid.expect("failed to decode partition");
                pending_batch.push((pid, ts, diff));
            }
        }

        // Tail the listen stream until we reach the target upper frontier. Note that, in the
        // common case, we are also the writer, so we are waiting to read-back what we wrote
        while PartialOrder::less_than(&self.upper, target_upper) {
            for event in self.listener.next().await {
                match event {
                    ListenEvent::Progress(new_upper) => {
                        consolidation::consolidate_updates(&mut pending_batch);
                        for (pid, ts, diff) in pending_batch.drain(..) {
                            let bindings = self.remap_trace.entry(pid.clone()).or_default();
                            bindings.push((ts, diff));

                            // Record all updates for returning.
                            let update_bindings = trace_updates.entry(pid.clone()).or_default();
                            update_bindings.push((ts, diff));

                            *self.source_upper.entry(pid.clone()).or_default() += diff;
                        }
                        self.upper = new_upper;
                    }
                    ListenEvent::Updates(updates) => {
                        for ((_, pid), ts, diff) in updates {
                            let pid = pid.expect("failed to decode partition");
                            pending_batch.push((pid, ts, diff));
                        }
                    }
                }
            }
        }

        trace_updates.into_iter().collect()
    }

    /// Returns the current contents of the remap trace. Suitable for
    /// bootstrapping a `ReclockListener`.
    pub fn remap_trace(&self) -> HashMap<PartitionId, Vec<(Timestamp, MzOffset)>> {
        self.remap_trace.clone()
    }

    /// Ensures that the persist shard backing this reclock operator contains bindings that cover
    /// the provided source frontier by minting bindings where appropriate.
    ///
    /// When this function returns the local dTVC view of the remap collection will contain
    /// definite timestamp bindings that can be used to reclock messages at offsets that are not
    /// beyond the provided frontier.
    pub async fn mint<P: Borrow<PartitionId>>(
        &mut self,
        source_frontier: &HashMap<P, MzOffset>,
    ) -> HashMap<PartitionId, Vec<(Timestamp, MzOffset)>> {
        // Any updates to the remap trace that occured during minting.
        let mut trace_updates: HashMap<PartitionId, Vec<(Timestamp, MzOffset)>> = HashMap::new();

        loop {
            let mut updates = vec![];
            for (pid, upper) in source_frontier {
                let pid = pid.borrow();
                let part_upper = self.source_upper.get(pid).copied().unwrap_or_default();

                if let Some(diff) = upper.checked_sub(part_upper) {
                    if diff > MzOffset::from(0) {
                        updates.push((pid.clone(), diff));
                    }
                }
            }

            // There are no updates to append, so we're done
            if updates.is_empty() {
                break;
            }

            match self.append(&updates).await {
                Ok(new_updates) => {
                    for (pid, update) in new_updates {
                        let bindings = trace_updates.entry(pid.clone()).or_default();
                        bindings.extend(update);
                    }
                    break;
                }
                Err(Upper(actual_upper)) => {
                    let new_updates = self.sync(&actual_upper).await;
                    for (pid, update) in new_updates {
                        let bindings = trace_updates.entry(pid.clone()).or_default();
                        bindings.extend(update);
                    }
                }
            }
        }

        trace_updates
    }

    /// Appends the provided updates to the remap collection at the next available minting
    /// timestamp and updates this operator's in-memory state accordingly.
    ///
    /// If an attempt to mint bindings fails due to another process having raced and appended
    /// bindings concurrently then the current global upper will be returned as an error. This is
    /// the frontier that this operator must be synced to for a future append attempt to have any
    /// chance of success.
    async fn append<P>(
        &mut self,
        updates: &[(P, MzOffset)],
    ) -> Result<Vec<(PartitionId, Vec<(Timestamp, MzOffset)>)>, Upper<Timestamp>>
    where
        P: Borrow<PartitionId>,
    {
        let next_ts = loop {
            match self.next_mint_timestamp() {
                Ok(ts) => break ts,
                Err(sleep_duration) => tokio::time::sleep(sleep_duration).await,
            }
        };
        let new_upper = Antichain::from_elem(next_ts.step_forward());
        loop {
            let upper = self.upper.clone();
            let new_upper = new_upper.clone();
            let updates = updates
                .iter()
                .map(|(pid, diff)| (((), pid.borrow()), next_ts, diff));
            match self
                .write_handle
                .compare_and_append(updates, upper, new_upper)
                .await
            {
                Ok(Ok(Ok(()))) => break,
                Ok(Ok(Err(actual_upper))) => return Err(actual_upper),
                Ok(Err(invalid_use)) => panic!("compare_and_append failed: {invalid_use}"),
                // An external error means that the operation might have suceeded or failed but we
                // don't know. In either case it is safe to retry because:
                // * If it succeeded, then on retry we'll get an `Upper(_)` error as if some other
                //   process raced us (but we actually raced ourselves). Since the operator is
                //   built to handle concurrent instances of itself this safe to do and will
                //   correctly re-sync its state. Once it resyncs we'll re-enter `mint` and notice
                //   that there are no updates to add (because we just added them and don't know
                //   it!) and the reclock operation will proceed normally.
                // * If it failed, then we'll succeed on retry and proceed normally.
                Err(external_err) => {
                    tracing::debug!("compare_and_append failed: {external_err}");
                    continue;
                }
            }
        }
        // At this point we have successfully produced data in the reclock shard so we need to
        // sync to update our local view as well
        Ok(self.sync(&new_upper).await)
    }

    /// Produces a new timestamp suitable for minting bindings or the amount of time that the
    /// caller needs to wait for one to become available. On success, the returned timestamp is
    /// guaranteed to be beyond the current `upper` frontier and a multiple of `update_interval_ms`
    pub fn next_mint_timestamp(&self) -> Result<Timestamp, Duration> {
        let now = (self.now)();
        let mut new_ts = now - now % self.update_interval_ms;
        if (now % self.update_interval_ms) != 0 {
            new_ts += self.update_interval_ms;
        }
        let new_ts: Timestamp = new_ts.try_into().expect("must fit");
        let upper_ts = self.upper.as_option().expect("no more timestamps to mint");
        if upper_ts <= &new_ts {
            Ok(new_ts)
        } else {
            let upper: u64 = upper_ts.into();
            Err(Duration::from_millis(upper - now))
        }
    }

    /// Returns an iterator of timestamp bindings for a given partition
    fn partition_bindings(&self, pid: &PartitionId) -> PartitionBindings {
        let bindings = match self.remap_trace.get(pid) {
            Some(bindings) => (*bindings).iter(),
            None => (&[]).iter(),
        };
        PartitionBindings {
            offset: MzOffset::default(),
            bindings,
        }
    }

    /// Returns the since frontier for a given partition
    fn partition_since(&self, pid: &PartitionId) -> MzOffset {
        if self.since.elements() == [Timestamp::minimum()] {
            // If we never compacted in the DestTime domain then the SourceTime domain isn't
            // compated either. Therefore the since frontier is zero
            MzOffset::default()
        } else {
            // If we have compacted there are two posibilities. Either the since frontier is at
            // exactly the time of the first binding (and will continue to do so since times are
            // advanced during compaction), or it is behind it.
            let (first_ts, first_offset) = self
                .remap_trace
                .get(pid)
                .and_then(|b| b.first())
                .copied()
                .unwrap_or_default();

            if self.since.less_than(&first_ts) {
                // If it is behind then the first binding will cover all offsets starting at zero,
                // so the since frontier of the partition is also zero.
                MzOffset::default()
            } else {
                // Otherwise the since frontier is the offset of the first binding
                first_offset
            }
        }
    }
}

/// The Iterator returned by [ReclockOperator::partition_bindings]
struct PartitionBindings<'a> {
    offset: MzOffset,
    bindings: std::slice::Iter<'a, (Timestamp, MzOffset)>,
}

impl Iterator for PartitionBindings<'_> {
    type Item = (Timestamp, MzOffset);
    fn next(&mut self) -> Option<Self::Item> {
        let &(ts, diff) = self.bindings.next()?;
        self.offset += diff;
        Some((ts, self.offset))
    }
}

/// The Iterator returned by [ReclockFollower::reclock]
pub struct ReclockIter<'a, M> {
    reclock: Ref<'a, ReclockFollowerInner>,
    messages: hash_map::IterMut<'a, PartitionId, Vec<(M, MzOffset)>>,
}

impl<'a, M> ReclockIter<'a, M> {
    pub fn for_each<F>(mut self, mut f: F)
    where
        F: FnMut(M, Timestamp),
    {
        for (partition, messages) in &mut self.messages {
            let mut partition_bindings = self.reclock.partition_bindings(partition).peekable();

            for (message, offset) in messages.drain(..) {
                // Skip bindings whose source offset upper doesn't cover this message's offset
                while !(offset < partition_bindings.peek().expect("not enough bindings").1) {
                    partition_bindings.next();
                }
                let (ts, _) = partition_bindings.peek().expect("not enough bindings");
                f(message, *ts)
            }
        }
    }

    #[cfg(test)]
    pub fn consume_all(self) -> Vec<(M, Timestamp)> {
        let mut vec = Vec::new();
        self.for_each(|m, ts| {
            vec.push((m, ts));
        });
        vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::now::SYSTEM_TIME;
    use once_cell::sync::Lazy;

    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::{PersistConfig, PersistLocation, ShardId};

    static PERSIST_CACHE: Lazy<Arc<Mutex<PersistClientCache>>> = Lazy::new(|| {
        Arc::new(Mutex::new(PersistClientCache::new(
            PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
        )))
    });

    async fn make_test_operator(
        shard: ShardId,
        as_of: Antichain<Timestamp>,
    ) -> (ReclockOperator, ReclockFollower) {
        let start = tokio::time::Instant::now();
        let now_fn = NowFn::from(move || start.elapsed().as_millis().try_into().unwrap());

        let metadata = CollectionMetadata {
            persist_location: PersistLocation {
                blob_uri: "mem://".to_owned(),
                consensus_uri: "mem://".to_owned(),
            },
            remap_shard: shard,
            data_shard: ShardId::new(),
            status_shard: None,
        };

        let operator = ReclockOperator::new(
            Arc::clone(&*PERSIST_CACHE),
            metadata,
            now_fn.clone(),
            Duration::from_secs(1),
            as_of.clone(),
        )
        .await
        .unwrap();

        let follower = ReclockFollower::new(as_of);

        // Push any updates that might already exist in the persist shard to the
        // follower.
        follower.push_trace_updates(operator.remap_trace().into_iter());

        (operator, follower)
    }

    async fn mint_and_follow(
        operator: &mut ReclockOperator,
        follower: &mut ReclockFollower,
        source_upper: &mut HashMap<PartitionId, MzOffset>,
    ) {
        let trace_updates = operator.mint(source_upper).await;
        let reclock_upper = operator
            .reclock_frontier(&source_upper)
            .expect("wrong source upper");
        follower.push_trace_updates(trace_updates.into_iter());
        follower.push_upper_update(reclock_upper);
    }

    #[tokio::test(start_paused = true)]
    async fn test_basic_usage() {
        const PART_ID: PartitionId = PartitionId::None;
        let (mut operator, mut follower) =
            make_test_operator(ShardId::new(), Antichain::from_elem(0.into())).await;
        let mut source_upper = HashMap::new();

        tokio::time::advance(Duration::from_secs(1)).await;

        let mut batch = HashMap::new();

        // Reclock offsets 1 and 3 to timestamp 1000
        batch.insert(
            PART_ID,
            vec![
                (1, MzOffset::from(1)),
                (1, MzOffset::from(1)),
                (3, MzOffset::from(3)),
            ],
        );
        source_upper.insert(PART_ID, MzOffset::from(4));
        mint_and_follow(&mut operator, &mut follower, &mut source_upper).await;

        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(
            reclocked_msgs,
            &[(1, 1000.into()), (1, 1000.into()), (3, 1000.into())]
        );
        assert!(batch[&PART_ID].is_empty());

        // This will return the antichain containing 1000 because that's where future messages will
        // offset 1 will be reclocked to
        let query = HashMap::from_iter([(PART_ID, MzOffset::from(1))]);
        assert_eq!(
            Ok(Antichain::from_elem(1000.into())),
            follower.reclock_frontier(&query)
        );

        // Reclock more messages for offsets 3 to the same timestamp
        batch.insert(
            PART_ID,
            vec![(3, MzOffset::from(3)), (3, MzOffset::from(3))],
        );
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(reclocked_msgs, &[(3, 1000.into()), (3, 1000.into())]);
        assert!(batch[&PART_ID].is_empty());

        // We're done with offset 3. Now the reclocking the source upper will result to the overall
        // target upper (1001) because any new bindings will be minted beyond that timestamp.
        let query = HashMap::from_iter([(PART_ID, MzOffset::from(4))]);

        assert_eq!(
            Ok(Antichain::from_elem(1001.into())),
            follower.reclock_frontier(&query)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_next_mint_timestamp() {
        let (mut operator, _follower) =
            make_test_operator(ShardId::new(), Antichain::from_elem(0.into())).await;

        // Test ceiling of timestamps works as expected
        assert_eq!(operator.next_mint_timestamp(), Ok(0.into()));

        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(operator.next_mint_timestamp(), Ok(1000.into()));

        tokio::time::advance(Duration::from_millis(999)).await;
        assert_eq!(operator.next_mint_timestamp(), Ok(1000.into()));

        tokio::time::advance(Duration::from_millis(125)).await;
        assert_eq!(operator.next_mint_timestamp(), Ok(2000.into()));

        // Advance the upper frontier to 2001
        operator.advance().await;

        // Test calculation of sleep time works as expected
        let sleep_duration = operator.next_mint_timestamp().unwrap_err();
        assert_eq!(sleep_duration, Duration::from_millis(2001 - 1125));

        // Test that if we wait the indicated amount we indeed manage to get a timestamp
        tokio::time::advance(sleep_duration).await;
        assert_eq!(operator.next_mint_timestamp(), Ok(3000.into()));
    }

    #[tokio::test(start_paused = true)]
    async fn test_reclock_frontier() {
        const PART1: PartitionId = PartitionId::Kafka(1);
        const PART2: PartitionId = PartitionId::Kafka(2);

        let (mut operator, _follower) =
            make_test_operator(ShardId::new(), Antichain::from_elem(0.into())).await;

        let query = HashMap::new();
        // This is the initial source frontier so we should get the initial ts upper
        assert_eq!(
            Ok(Antichain::from_elem(0.into())),
            operator.reclock_frontier(&query)
        );

        tokio::time::advance(Duration::from_secs(1)).await;

        // Mint a couple of bindings for multiple partitions
        operator
            .mint(&HashMap::from_iter([(PART1, MzOffset::from(10))]))
            .await;
        operator
            .mint(&HashMap::from_iter([(PART2, MzOffset::from(10))]))
            .await;
        assert_eq!(
            operator.remap_trace[&PART1],
            &[(1000.into(), MzOffset::from(10))]
        );
        assert_eq!(
            operator.remap_trace[&PART2],
            &[(2000.into(), MzOffset::from(10))]
        );

        // The initial frontier should now map to the minimum between the two partitions
        let query = HashMap::new();
        assert_eq!(
            Ok(Antichain::from_elem(1000.into())),
            operator.reclock_frontier(&query)
        );

        // Map a frontier that advances only one of the partitions
        let query = HashMap::from_iter([(PART1, MzOffset::from(9))]);
        assert_eq!(
            Ok(Antichain::from_elem(1000.into())),
            operator.reclock_frontier(&query)
        );
        let query = HashMap::from_iter([(PART1, MzOffset::from(10))]);
        assert_eq!(
            Ok(Antichain::from_elem(2000.into())),
            operator.reclock_frontier(&query)
        );
        // A frontier that is the upper of both partitions should map to the timestamp upper
        let query = HashMap::from_iter([(PART1, MzOffset::from(10)), (PART2, MzOffset::from(10))]);
        assert_eq!(
            Ok(Antichain::from_elem(2001.into())),
            operator.reclock_frontier(&query)
        );

        // Advance the operator and confirm that we get to the next timestamp
        tokio::time::advance(Duration::from_secs(1)).await;
        operator.advance().await;
        let query = HashMap::from_iter([(PART1, MzOffset::from(10)), (PART2, MzOffset::from(10))]);
        assert_eq!(
            Ok(Antichain::from_elem(3001.into())),
            operator.reclock_frontier(&query)
        );

        // Compact but not enough to change the bindings
        operator.compact(Antichain::from_elem(900.into())).await;
        let query = HashMap::from_iter([(PART1, MzOffset::from(9))]);
        assert_eq!(
            Ok(Antichain::from_elem(1000.into())),
            operator.reclock_frontier(&query)
        );

        // Compact enough to compact bindings
        operator.compact(Antichain::from_elem(1500.into())).await;
        let query = HashMap::from_iter([(PART1, MzOffset::from(9))]);
        assert_eq!(
            Err((PART1, MzOffset::from(9))),
            operator.reclock_frontier(&query)
        );
        let query = HashMap::from_iter([(PART1, MzOffset::from(10))]);
        assert_eq!(
            Ok(Antichain::from_elem(2000.into())),
            operator.reclock_frontier(&query)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_reclock() {
        const PART_ID: PartitionId = PartitionId::None;

        let (mut operator, mut follower) =
            make_test_operator(ShardId::new(), Antichain::from_elem(0.into())).await;

        let mut batch = HashMap::new();
        let mut source_upper = HashMap::new();

        // Reclock offsets 1 and 2 to timestamp 0
        batch.insert(
            PART_ID,
            vec![(1, MzOffset::from(1)), (2, MzOffset::from(2))],
        );
        source_upper.insert(PART_ID, MzOffset::from(3));

        mint_and_follow(&mut operator, &mut follower, &mut source_upper).await;
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(reclocked_msgs, &[(1, 0.into()), (2, 0.into())]);
        assert!(batch[&PART_ID].is_empty());

        // Reclock offsets 3 and 4 to timestamp 1000
        batch.insert(
            PART_ID,
            vec![(3, MzOffset::from(3)), (4, MzOffset::from(4))],
        );
        source_upper.insert(PART_ID, MzOffset::from(5));

        mint_and_follow(&mut operator, &mut follower, &mut source_upper).await;
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(reclocked_msgs, &[(3, 1000.into()), (4, 1000.into())]);
        assert!(batch[&PART_ID].is_empty());

        // Reclock the same offsets again
        batch.insert(
            PART_ID,
            vec![(1, MzOffset::from(1)), (2, MzOffset::from(2))],
        );
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(reclocked_msgs, &[(1, 0.into()), (2, 0.into())]);
        assert!(batch[&PART_ID].is_empty());

        // Reclock a batch with offsets that spans multiple bindings
        batch.insert(
            PART_ID,
            vec![
                (1, MzOffset::from(1)),
                (2, MzOffset::from(2)),
                (3, MzOffset::from(3)),
                (4, MzOffset::from(4)),
            ],
        );
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(
            reclocked_msgs,
            &[
                (1, 0.into()),
                (2, 0.into()),
                (3, 1000.into()),
                (4, 1000.into())
            ]
        );
        assert!(batch[&PART_ID].is_empty());

        // Reclock a batch that contains multiple messages having the same offset
        batch.insert(
            PART_ID,
            vec![
                (1, MzOffset::from(1)),
                (1, MzOffset::from(1)),
                (3, MzOffset::from(3)),
                (3, MzOffset::from(3)),
            ],
        );
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(
            reclocked_msgs,
            &[
                (1, 0.into()),
                (1, 0.into()),
                (3, 1000.into()),
                (3, 1000.into())
            ]
        );
        assert!(batch[&PART_ID].is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn test_compaction() {
        let binding_shard = ShardId::new();

        const PART_ID: PartitionId = PartitionId::None;
        let (mut operator, mut follower) =
            make_test_operator(binding_shard, Antichain::from_elem(0.into())).await;

        let mut batch = HashMap::new();
        let mut source_upper = HashMap::new();

        tokio::time::advance(Duration::from_secs(1)).await;

        // Reclock offsets 1 and 2 to timestamp 1000
        batch.insert(
            PART_ID,
            vec![(1, MzOffset::from(1)), (2, MzOffset::from(2))],
        );
        source_upper.insert(PART_ID, MzOffset::from(3));

        mint_and_follow(&mut operator, &mut follower, &mut source_upper).await;
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(reclocked_msgs, &[(1, 1000.into()), (2, 1000.into())]);
        assert!(batch[&PART_ID].is_empty());

        // Reclock offsets 3 and 4 to timestamp 2000
        batch.insert(
            PART_ID,
            vec![(3, MzOffset::from(3)), (4, MzOffset::from(4))],
        );
        source_upper.insert(PART_ID, MzOffset::from(5));

        mint_and_follow(&mut operator, &mut follower, &mut source_upper).await;
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(reclocked_msgs, &[(3, 2000.into()), (4, 2000.into())]);
        assert!(batch[&PART_ID].is_empty());

        // Compact enough so that we can correctly timestamp only offsets >= 3
        operator.compact(Antichain::from_elem(1000.into())).await;
        follower.compact(Antichain::from_elem(1000.into()));

        // Reclock offsets 3 and 4 again to see we haven't lost the ability
        batch.insert(
            PART_ID,
            vec![(3, MzOffset::from(3)), (4, MzOffset::from(4))],
        );
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(reclocked_msgs, &[(3, 2000.into()), (4, 2000.into())]);
        assert!(batch[&PART_ID].is_empty());

        // Attempting to reclock offset 2 should return an error
        batch.insert(PART_ID, vec![(2, MzOffset::from(2))]);
        assert_eq!(
            follower.reclock(&mut batch).and(Ok(())),
            Err((PART_ID, 2.into()))
        );

        // Starting a new operator with an `as_of` is the same as having compacted
        let (_operator, follower) =
            make_test_operator(binding_shard, Antichain::from_elem(1000.into())).await;

        // Reclocking offsets 3 and 4 should succeed
        batch.insert(
            PART_ID,
            vec![(3, MzOffset::from(3)), (4, MzOffset::from(4))],
        );
        let reclocked_msgs = follower
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(reclocked_msgs, &[(3, 2000.into()), (4, 2000.into())]);
        assert!(batch[&PART_ID].is_empty());

        // But attempting to reclock offset 2 should return an error
        batch.insert(PART_ID, vec![(2, MzOffset::from(2))]);
        assert_eq!(
            follower.reclock(&mut batch).and(Ok(())),
            Err((PART_ID, 2.into()))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_concurrency() {
        const PART_ID: PartitionId = PartitionId::None;

        // Create two operators pointing to the same shard
        let shared_shard = ShardId::new();
        let (mut op_a, mut follower_a) =
            make_test_operator(shared_shard, Antichain::from_elem(0.into())).await;
        let (mut op_b, mut follower_b) =
            make_test_operator(shared_shard, Antichain::from_elem(0.into())).await;

        // Reclock a batch from one of the operators
        let mut batch = HashMap::new();
        let mut source_upper = HashMap::new();

        tokio::time::advance(Duration::from_secs(1)).await;

        // Reclock offsets 1 and 2 to timestamp 1000 from operator A
        batch.insert(
            PART_ID,
            vec![(1, MzOffset::from(1)), (2, MzOffset::from(2))],
        );
        source_upper.insert(PART_ID, MzOffset::from(3));

        mint_and_follow(&mut op_a, &mut follower_a, &mut source_upper).await;
        let reclocked_msgs = follower_a
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(reclocked_msgs, &[(1, 1000.into()), (2, 1000.into())]);
        assert!(batch[&PART_ID].is_empty());

        // Also compact operator A. Since operator B has its own read handle it shouldn't affect it
        op_a.compact(Antichain::from_elem(1000.into())).await;
        follower_a.compact(Antichain::from_elem(1000.into()));

        // Advance the time by a lot
        tokio::time::advance(Duration::from_secs(10)).await;

        // Reclock a batch that includes messages from the bindings already minted
        batch.insert(
            PART_ID,
            vec![
                (1, MzOffset::from(1)),
                (2, MzOffset::from(2)),
                (3, MzOffset::from(3)),
                (4, MzOffset::from(4)),
            ],
        );
        source_upper.insert(PART_ID, MzOffset::from(5));
        // This operator should attempt to mint in one go, fail, re-sync, and retry only for the
        // bindings that still need minting
        mint_and_follow(&mut op_b, &mut follower_b, &mut source_upper).await;
        let reclocked_msgs = follower_b
            .reclock(&mut batch)
            .expect("beyond source frontier")
            .expect("we should have all required bindings")
            .consume_all();
        assert_eq!(
            reclocked_msgs,
            &[
                (1, 1000.into()),
                (2, 1000.into()),
                (3, 11000.into()),
                (4, 11000.into())
            ]
        );
        assert!(batch[&PART_ID].is_empty());
    }
}
