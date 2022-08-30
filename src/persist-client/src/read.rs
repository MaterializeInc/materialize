// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Read capabilities and handles

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use futures::Stream;
use mz_ore::task::RuntimeExt;
use serde::{Deserialize, Serialize};
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;
use tokio::runtime::Handle;
use tracing::{debug_span, instrument, warn, Instrument};
use uuid::Uuid;

use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::{Codec, Codec64};

use crate::fetch::{
    fetch_leased_part, BatchFetcher, LeasedBatchPart, SerdeLeasedBatchPartMetadata,
};
use crate::internal::machine::Machine;
use crate::internal::metrics::Metrics;
use crate::internal::state::{HollowBatch, Since};
use crate::{GarbageCollector, PersistConfig};

/// An opaque identifier for a reader of a persist durable TVC (aka shard).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ReaderId(pub(crate) [u8; 16]);

impl std::fmt::Display for ReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "r{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for ReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReaderId({})", Uuid::from_bytes(self.0))
    }
}

impl ReaderId {
    pub(crate) fn new() -> Self {
        ReaderId(*Uuid::new_v4().as_bytes())
    }
}

/// Capable of generating a snapshot of all data at `as_of`, followed by a
/// listen of all updates.
///
/// For more details, see [`ReadHandle::snapshot`] and [`Listen`].
#[derive(Debug)]
pub struct Subscribe<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
{
    snapshot: Option<(Vec<LeasedBatchPart<T>>, Antichain<T>)>,
    listen: Listen<K, V, T, D>,
}

impl<K, V, T, D> Subscribe<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    fn new(
        snapshot_parts: Vec<LeasedBatchPart<T>>,
        snapshot_as_of: Antichain<T>,
        listen: Listen<K, V, T, D>,
    ) -> Self {
        assert_eq!(snapshot_as_of, listen.as_of);
        Subscribe {
            snapshot: Some((snapshot_parts, snapshot_as_of)),
            listen,
        }
    }

    /// Returns a `HollowBatch` enriched with the proper metadata.
    ///
    /// First returns snapshot parts, until they're exhausted, at which point
    /// begins returning listen parts.
    ///
    /// The returned `Antichain` represents the subscription progress as it will
    /// be _after_ the returned parts are fetched.
    #[instrument(level = "debug", skip_all, fields(shard = %self.listen.handle.machine.shard_id()))]
    pub async fn next(&mut self) -> (Vec<LeasedBatchPart<T>>, Antichain<T>) {
        // This is odd, but we move our handle into a `Listen`.
        self.listen.handle.maybe_heartbeat_reader().await;

        match self.snapshot.take() {
            Some(x) => x,
            None => self.listen.next_parts().await,
        }
    }

    /// Returns the given [`LeasedBatchPart`], releasing its lease.
    pub fn return_leased_part(&mut self, leased_part: LeasedBatchPart<T>) {
        self.listen.handle.process_returned_leased_part(leased_part)
    }
}

/// Data and progress events of a shard subscription.
///
/// TODO: Unify this with [timely::dataflow::operators::to_stream::Event] or
/// [timely::dataflow::operators::capture::event::Event].
#[derive(Debug, PartialEq)]
pub enum ListenEvent<K, V, T, D> {
    /// Progress of the shard.
    Progress(Antichain<T>),
    /// Data of the shard.
    Updates(Vec<((Result<K, String>, Result<V, String>), T, D)>),
}

/// An ongoing subscription of updates to a shard.
#[derive(Debug)]
pub struct Listen<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
{
    handle: ReadHandle<K, V, T, D>,

    as_of: Antichain<T>,
    since: Antichain<T>,
    frontier: Antichain<T>,
}

impl<K, V, T, D> Listen<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    async fn new(mut handle: ReadHandle<K, V, T, D>, as_of: Antichain<T>) -> Self {
        let since = as_of.clone();
        // This listen only needs to distinguish things after its frontier
        // (initially as_of although the frontier is inclusive and the as_of
        // isn't). Be a good citizen and downgrade early.
        handle.downgrade_since(&since).await;

        Listen {
            handle,
            since,
            frontier: as_of.clone(),
            as_of,
        }
    }

    /// Convert listener into futures::Stream
    pub fn into_stream(mut self) -> impl Stream<Item = ListenEvent<K, V, T, D>> {
        async_stream::stream!({
            loop {
                for msg in self.next().await {
                    yield msg;
                }
            }
        })
    }

    /// An exclusive upper bound on the progress of this Listen.
    pub fn frontier(&self) -> &Antichain<T> {
        &self.frontier
    }

    /// Attempt to pull out the next values of this subscription.
    ///
    /// The returned [`LeasedBatchPart`] is appropriate to use with
    /// `crate::fetch::fetch_leased_part`.
    ///
    /// The returned `Antichain` represents the subscription progress as it will
    /// be _after_ the returned parts are fetched.
    pub async fn next_parts(&mut self) -> (Vec<LeasedBatchPart<T>>, Antichain<T>) {
        // We might also want to call maybe_heartbeat_reader in the
        // `next_listen_batch` loop so that we can heartbeat even when batches
        // aren't incoming. At the moment, the ownership would be weird and we
        // should always have batches incoming regularly, so maybe it isn't
        // actually necessary.
        let batch = self.handle.machine.next_listen_batch(&self.frontier).await;

        // A lot of things across mz have to line up to hold the following
        // invariant and violations only show up as subtle correctness errors,
        // so explictly validate it here. Better to panic and roll back a
        // release than be incorrect (also potentially corrupting a sink).
        //
        // Note that the since check is intentionally less_than, not less_equal.
        // If a batch's since is X, that means we can no longer distinguish X
        // (beyond self.frontier) from X-1 (not beyond self.frontier) to keep
        // former and filter out the latter.
        assert!(
            PartialOrder::less_than(batch.desc.since(), &self.frontier)
                // Special case when the frontier == the as_of (i.e. the first
                // time this is called on a new Listen). Because as_of is
                // _exclusive_, we don't need to be able to distinguish X from
                // X-1.
                || (self.frontier == self.as_of
                    && PartialOrder::less_equal(batch.desc.since(), &self.frontier)),
            "Listen on {} received a batch {:?} advanced past the listen frontier {:?}",
            self.handle.machine.shard_id(),
            batch.desc,
            self.frontier
        );

        let new_frontier = batch.desc.upper().clone();

        // We will have a new frontier, so this is an opportunity to downgrade our
        // since capability. Go through `maybe_heartbeat` so we can rate limit
        // this along with our heartbeats.
        //
        // HACK! Everything would be simpler if we could downgrade since to the
        // new frontier, but we can't. The next call needs to be able to
        // distinguish between the times T at the frontier (to emit updates with
        // these times) and T-1 (to filter them). Advancing the since to
        // frontier would erase the ability to distinguish between them. Ideally
        // we'd use what is conceptually "batch.upper - 1" (the greatest
        // elements that are still strictly less than batch.upper, which will be
        // the new value of self.frontier after this call returns), but the
        // trait bounds on T don't give us a way to compute that directly.
        // Instead, we sniff out any elements in self.frontier (the upper of the
        // batch the last time we called this) that are strictly less_than the
        // batch upper to compute a new since. For totally ordered times
        // (currently always the case in mz) self.frontier will always have a
        // single element and it will be less_than upper, but the following
        // logic is (hopefully) correct for partially order times as well. We
        // could also abuse the fact that every time we actually emit is
        // guaranteed by definition to be less_than upper to be a bit more
        // prompt, but this would involve a lot more temporary antichains and
        // it's unclear if that's worth it.
        for x in self.frontier.elements().iter() {
            let less_than_upper = batch.desc.upper().elements().iter().any(|u| x.less_than(u));
            if less_than_upper {
                self.since.join_assign(&Antichain::from_elem(x.clone()));
            }
        }
        self.handle.maybe_downgrade_since(&self.since).await;

        let metadata = SerdeLeasedBatchPartMetadata::Listen {
            as_of: self.as_of.iter().map(T::encode).collect(),
            lower: self.frontier.iter().map(T::encode).collect(),
        };
        let parts = self.handle.lease_batch_parts(batch, metadata).collect();

        // NB: Keep this after we use self.frontier to join_assign self.since
        // and also after we construct metadata.
        self.frontier = new_frontier;

        (parts, self.frontier.clone())
    }

    /// Attempt to pull out the next values of this subscription.
    ///
    /// The updates received in [ListenEvent::Updates] should be assumed to be in arbitrary order
    /// and not necessarily consolidated. However, the timestamp of each individual update will be
    /// greater than or equal to the last received [ListenEvent::Progress] frontier (or this
    /// [Listen]'s initial `as_of` frontier if no progress event has been emitted yet) and less
    /// than the next [ListenEvent::Progress] frontier.
    ///
    /// If you have a use for consolidated listen output, given that snapshots can't be
    /// consolidated, come talk to us!
    #[instrument(level = "debug", name = "listen::next", skip_all, fields(shard = %self.handle.machine.shard_id()))]
    pub async fn next(&mut self) -> Vec<ListenEvent<K, V, T, D>> {
        let (parts, progress) = self.next_parts().await;
        let mut ret = Vec::with_capacity(parts.len() + 1);
        for part in parts {
            let (part, updates) = fetch_leased_part(
                part,
                self.handle.blob.as_ref(),
                &self.handle.metrics,
                Some(&self.handle.reader_id),
            )
            .await;
            self.handle.process_returned_leased_part(part);
            if !updates.is_empty() {
                ret.push(ListenEvent::Updates(updates));
            }
        }
        ret.push(ListenEvent::Progress(progress));
        ret
    }

    /// Politely expires this listen, releasing its lease.
    ///
    /// There is a best-effort impl in Drop to expire a listen that wasn't
    /// explictly expired with this method. When possible, explicit expiry is
    /// still preferred because the Drop one is best effort and is dependant on
    /// a tokio [Handle] being available in the TLC at the time of drop (which
    /// is a bit subtle). Also, explicit expiry allows for control over when it
    /// happens.
    pub async fn expire(self) {
        self.handle.expire().await
    }

    /// Test helper to read from the listener until the given frontier is
    /// reached. Because compaction can arbitrarily combine batches, we only
    /// return the final progress info.
    #[cfg(test)]
    #[track_caller]
    pub async fn read_until(
        &mut self,
        ts: &T,
    ) -> (
        Vec<((Result<K, String>, Result<V, String>), T, D)>,
        Antichain<T>,
    ) {
        let mut updates = Vec::new();
        let mut frontier = Antichain::from_elem(T::minimum());
        while self.frontier.less_than(ts) {
            for event in self.next().await {
                match event {
                    ListenEvent::Updates(mut x) => updates.append(&mut x),
                    ListenEvent::Progress(x) => frontier = x,
                }
            }
        }
        // Unlike most tests, intentionally don't consolidate updates here
        // because Listen replays them at the original fidelity.
        (updates, frontier)
    }
}

/// A "capability" granting the ability to read the state of some shard at times
/// greater or equal to `self.since()`.
///
/// Production users should call [Self::expire] before dropping a ReadHandle so
/// that it can expire its leases. If/when rust gets AsyncDrop, this will be
/// done automatically.
///
/// All async methods on ReadHandle retry for as long as they are able, but the
/// returned [std::future::Future]s implement "cancel on drop" semantics. This
/// means that callers can add a timeout using [tokio::time::timeout] or
/// [tokio::time::timeout_at].
///
/// ```rust,no_run
/// # let mut read: mz_persist_client::read::ReadHandle<String, String, u64, i64> = unimplemented!();
/// # let timeout: std::time::Duration = unimplemented!();
/// # let new_since: timely::progress::Antichain<u64> = unimplemented!();
/// # async {
/// tokio::time::timeout(timeout, read.downgrade_since(&new_since)).await
/// # };
/// ```
#[derive(Debug)]
pub struct ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in the auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) reader_id: ReaderId,
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) gc: GarbageCollector<K, V, T, D>,
    pub(crate) blob: Arc<dyn Blob + Send + Sync>,

    pub(crate) since: Antichain<T>,
    pub(crate) last_heartbeat: Instant,
    pub(crate) explicitly_expired: bool,

    pub(crate) leased_seqnos: BTreeMap<SeqNo, usize>,
}

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + Lattice + Codec64,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// This handle's `since` frontier.
    ///
    /// This will always be greater or equal to the shard-global `since`.
    pub fn since(&self) -> &Antichain<T> {
        &self.since
    }

    /// Forwards the since frontier of this handle, giving up the ability to
    /// read at times not greater or equal to `new_since`.
    ///
    /// This may trigger (asynchronous) compaction and consolidation in the
    /// system. A `new_since` of the empty antichain "finishes" this shard,
    /// promising that no more data will ever be read by this handle.
    ///
    /// This also acts as a heartbeat for the reader lease (including if called
    /// with `new_since` equal to something like `self.since()` or the minimum
    /// timestamp, making the call a no-op).
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn downgrade_since(&mut self, new_since: &Antichain<T>) {
        // Guaranteed to be the smallest/oldest outstanding lease on a `SeqNo`.
        let outstanding_seqno = self.leased_seqnos.keys().next().cloned();

        let (_seqno, current_reader_since, maintenance) = self
            .machine
            .downgrade_since(
                &self.reader_id,
                outstanding_seqno,
                new_since,
                (self.cfg.now)(),
            )
            .await;
        self.since = current_reader_since.0;
        // A heartbeat is just any downgrade_since traffic, so update the
        // internal rate limiter here to play nicely with `maybe_heartbeat`.
        self.last_heartbeat = Instant::now();
        maintenance.start_performing(&self.machine, &self.gc);
    }

    /// Returns an ongoing subscription of updates to a shard.
    ///
    /// The stream includes all data at times greater than `as_of`. Combined
    /// with [Self::snapshot] it will produce exactly correct results: the
    /// snapshot is the TVCs contents at `as_of` and all subsequent updates
    /// occur at exactly their indicated time. The recipient should only
    /// downgrade their read capability when they are certain they have all data
    /// through the frontier they would downgrade to.
    ///
    /// This takes ownership of the ReadHandle so the Listen can use it to
    /// [Self::downgrade_since] as it progresses. If you need to keep this
    /// handle, then [Self::clone] it before calling listen.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn listen(self, as_of: Antichain<T>) -> Result<Listen<K, V, T, D>, Since<T>> {
        let () = self.machine.verify_listen(&as_of).await?;
        Ok(Listen::new(self, as_of).await)
    }

    /// Returns a [`BatchFetcher`], which does not hold since or seqno
    /// capabilities.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn batch_fetcher(self) -> BatchFetcher<K, V, T, D> {
        BatchFetcher::new(self).await
    }

    /// Returns all of the contents of the shard TVC at `as_of` broken up into
    /// [`LeasedBatchPart`]es. These parts can be "turned in" via
    /// `crate::fetch::fetch_batch_part` to receive the data they contain.
    ///
    /// This command returns the contents of this shard as of `as_of` once they
    /// are known. This may "block" (in an async-friendly way) if `as_of` is
    /// greater or equal to the current `upper` of the shard. The recipient
    /// should only downgrade their read capability when they are certain they
    /// have all data through the frontier they would downgrade to.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    #[instrument(level = "trace", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn snapshot(
        &mut self,
        as_of: Antichain<T>,
    ) -> Result<Vec<LeasedBatchPart<T>>, Since<T>> {
        let batches = self.machine.snapshot(&as_of).await?;

        let metadata = SerdeLeasedBatchPartMetadata::Snapshot {
            as_of: as_of.iter().map(T::encode).collect(),
        };
        let mut leased_parts = Vec::new();
        for batch in batches {
            // Flatten the HollowBatch into one LeasedBatchPart per key. Each key
            // corresponds to a "part" or s3 object. This allows persist_source
            // to distribute work by parts (smallish, more even size) instead of
            // batches (arbitrarily large).
            leased_parts.extend(self.lease_batch_parts(batch, metadata.clone()));
        }
        Ok(leased_parts)
    }

    /// Generates a [Self::snapshot], and fetches all of the batches
    /// it contains.
    pub async fn snapshot_and_fetch(
        &mut self,
        as_of: Antichain<T>,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, Since<T>> {
        let snap = self.snapshot(as_of).await?;

        let mut contents = Vec::new();
        for batch in snap {
            let (batch, mut r) = fetch_leased_part(
                batch,
                self.blob.as_ref(),
                &self.metrics,
                Some(&self.reader_id),
            )
            .await;
            self.process_returned_leased_part(batch);
            contents.append(&mut r);
        }
        Ok(contents)
    }

    /// Returns a snapshot of all of a shard's data using `as_of`, followed by
    /// listening to any future updates.
    ///
    /// For more details on this operation's semantics, see [Self::snapshot] and
    /// [Self::listen].
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn subscribe(
        mut self,
        as_of: Antichain<T>,
    ) -> Result<Subscribe<K, V, T, D>, Since<T>> {
        let snapshot_parts = self.snapshot(as_of.clone()).await?;
        let listen = self.listen(as_of.clone()).await?;
        Ok(Subscribe::new(snapshot_parts, as_of, listen))
    }

    fn lease_batch_parts(
        &mut self,
        batch: HollowBatch<T>,
        metadata: SerdeLeasedBatchPartMetadata,
    ) -> impl Iterator<Item = LeasedBatchPart<T>> + '_ {
        batch.keys.into_iter().map(move |key| LeasedBatchPart {
            shard_id: self.machine.shard_id(),
            reader_id: self.reader_id.clone(),
            metadata: metadata.clone(),
            desc: batch.desc.clone(),
            key,
            leased_seqno: Some(self.lease_seqno()),
        })
    }

    /// Tracks that the `ReadHandle`'s machine's current `SeqNo` is being
    /// "leased out" to a `LeasedBatchPart`, and cannot be garbage
    /// collected until its lease has been returned.
    fn lease_seqno(&mut self) -> SeqNo {
        let seqno = self.machine.seqno();

        *self.leased_seqnos.entry(seqno).or_insert(0) += 1;

        seqno
    }

    /// Processes that a part issued from `self` has been consumed, and `self`
    /// can now process any internal bookkeeping.
    ///
    /// # Panics
    /// - If `self` does not have record of issuing the [`LeasedBatchPart`], e.g.
    ///   it originated from from another `ReadHandle`.
    pub(crate) fn process_returned_leased_part(&mut self, mut leased_part: LeasedBatchPart<T>) {
        if let Some(lease) = leased_part.return_lease(&self.reader_id) {
            // Tracks that a `SeqNo` lease has been returned and can be dropped. Once
            // a `SeqNo` has no more outstanding leases, it can be removed, and
            // `Self::downgrade_since` no longer needs to prevent it from being
            // garbage collected.
            let remaining_leases = self
                .leased_seqnos
                .get_mut(&lease)
                .expect("leased SeqNo returned, but lease not issued from this ReadHandle");

            *remaining_leases -= 1;

            if remaining_leases == &0 {
                self.leased_seqnos.remove(&lease);
            }
        }
    }

    /// Returns an independent [ReadHandle] with a new [ReaderId] but the same
    /// `since`.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn clone(&self) -> Self {
        let new_reader_id = ReaderId::new();
        let mut machine = self.machine.clone();
        let read_cap = machine.clone_reader(&new_reader_id, (self.cfg.now)()).await;
        let new_reader = ReadHandle {
            cfg: self.cfg.clone(),
            metrics: Arc::clone(&self.metrics),
            reader_id: new_reader_id,
            machine,
            gc: self.gc.clone(),
            blob: Arc::clone(&self.blob),
            since: read_cap.since,
            last_heartbeat: Instant::now(),
            explicitly_expired: false,
            leased_seqnos: BTreeMap::new(),
        };
        new_reader
    }

    /// A rate-limited version of [Self::downgrade_since].
    ///
    /// This is an internally rate limited helper, designed to allow users to
    /// call it as frequently as they like. Call this [Self::downgrade_since],
    /// or Self::maybe_heartbeat_reader on some interval that is "frequent"
    /// compared to PersistConfig::FAKE_READ_LEASE_DURATION.
    ///
    /// This is communicating actual progress information, so is given
    /// preferential treatment compared to Self::maybe_heartbeat_reader.
    pub async fn maybe_downgrade_since(&mut self, new_since: &Antichain<T>) {
        // NB: min_elapsed is intentionally smaller than the one in
        // maybe_heartbeat_reader (this is the preferential treatment mentioned
        // above).
        let min_elapsed = PersistConfig::FAKE_READ_LEASE_DURATION / 4;
        let elapsed_since_last_heartbeat = self.last_heartbeat.elapsed();
        if elapsed_since_last_heartbeat >= min_elapsed {
            self.downgrade_since(&new_since).await;
        }
    }

    /// Heartbeats the read lease if necessary.
    ///
    /// This is an internally rate limited helper, designed to allow users to
    /// call it as frequently as they like. Call this [Self::downgrade_since],
    /// or [Self::maybe_downgrade_since] on some interval that is "frequent"
    /// compared to PersistConfig::FAKE_READ_LEASE_DURATION.
    async fn maybe_heartbeat_reader(&mut self) {
        let min_elapsed = PersistConfig::FAKE_READ_LEASE_DURATION / 2;
        let elapsed_since_last_heartbeat = self.last_heartbeat.elapsed();
        if elapsed_since_last_heartbeat >= min_elapsed {
            let (_, maintenance) = self
                .machine
                .heartbeat_reader(&self.reader_id, (self.cfg.now)())
                .await;
            self.last_heartbeat = Instant::now();
            maintenance.start_performing(&self.machine, &self.gc);
        }
    }

    /// Politely expires this reader, releasing its lease.
    ///
    /// There is a best-effort impl in Drop to expire a reader that wasn't
    /// explictly expired with this method. When possible, explicit expiry is
    /// still preferred because the Drop one is best effort and is dependant on
    /// a tokio [Handle] being available in the TLC at the time of drop (which
    /// is a bit subtle). Also, explicit expiry allows for control over when it
    /// happens.
    #[instrument(level = "debug", skip_all, fields(shard = %self.machine.shard_id()))]
    pub async fn expire(mut self) {
        self.machine.expire_reader(&self.reader_id).await;
        self.explicitly_expired = true;
    }

    /// Test helper for a [Self::listen] call that is expected to succeed.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_listen(self, as_of: T) -> Listen<K, V, T, D> {
        self.listen(Antichain::from_elem(as_of))
            .await
            .expect("cannot serve requested as_of")
    }
}

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Debug + Codec + Ord,
    V: Debug + Codec + Ord,
    T: Timestamp + Lattice + Codec64 + Ord,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// Test helper to generate a [Self::snapshot] call that is expected to
    /// succeed, process its batches, and then return its data sorted.
    #[cfg(test)]
    #[track_caller]
    pub async fn expect_snapshot_and_fetch(
        &mut self,
        as_of: T,
    ) -> Vec<((Result<K, String>, Result<V, String>), T, D)> {
        let mut ret = self
            .snapshot_and_fetch(Antichain::from_elem(as_of))
            .await
            .expect("cannot serve requested as_of");

        ret.sort();
        ret
    }
}

impl<K, V, T, D> Drop for ReadHandle<K, V, T, D>
where
    T: Timestamp + Lattice + Codec64,
    // These are only here so we can use them in this auto-expiring `Drop` impl.
    K: Debug + Codec,
    V: Debug + Codec,
    D: Semigroup + Codec64 + Send + Sync,
{
    fn drop(&mut self) {
        if self.explicitly_expired {
            return;
        }
        let handle = match Handle::try_current() {
            Ok(x) => x,
            Err(_) => {
                warn!("ReadHandle {} dropped without being explicitly expired, falling back to lease timeout", self.reader_id);
                return;
            }
        };
        let mut machine = self.machine.clone();
        let reader_id = self.reader_id.clone();
        // Spawn a best-effort task to expire this read handle. It's fine if
        // this doesn't run to completion, we'd just have to wait out the lease
        // before the shard-global since is unblocked.
        //
        // Intentionally create the span outside the task to set the parent.
        let expire_span = debug_span!("drop::expire");
        let _ = handle.spawn_named(
            || format!("ReadHandle::expire ({})", self.reader_id),
            async move {
                machine.expire_reader(&reader_id).await;
            }
            .instrument(expire_span),
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::new_test_client;

    // Verifies the semantics of `SeqNo` leases + checks dropping `LeasedBatchPart` semantics.
    #[tokio::test]
    async fn seqno_leases() {
        mz_ore::test::init_logging();
        let mut data = vec![];
        for i in 0..20 {
            data.push(((i.to_string(), i.to_string()), i, 1))
        }

        let (mut write, read) = new_test_client()
            .await
            .expect_open::<String, String, u64, i64>(crate::ShardId::new())
            .await;

        // Seed with some values
        let mut offset = 0;
        let mut width = 2;

        for i in offset..offset + width {
            write
                .expect_compare_and_append(&data[i..i + 1], i as u64, i as u64 + 1)
                .await;
        }
        offset += width;

        // Create machinery for subscribe + fetch

        let fetcher = read.clone().await.batch_fetcher().await;

        let mut subscribe = read
            .subscribe(timely::progress::Antichain::from_elem(1))
            .await
            .expect("cannot serve requested as_of");

        // Determine sequence number at outset.
        let original_seqno_since = subscribe.listen.handle.machine.seqno_since();

        let mut parts = vec![];

        width = 4;
        // Collect parts while continuing to write values
        for i in offset..offset + width {
            let (mut new_parts, _) = subscribe.next().await;
            parts.append(&mut new_parts);
            // Here and elsewhere we "cheat" and immediately downgrade the since
            // to demonstrate the effects of SeqNo leases immediately.
            subscribe
                .listen
                .handle
                .downgrade_since(&subscribe.listen.since)
                .await;

            write
                .expect_compare_and_append(&data[i..i + 1], i as u64, i as u64 + 1)
                .await;

            // SeqNo is not downgraded
            assert_eq!(
                subscribe.listen.handle.machine.seqno_since(),
                original_seqno_since
            );
        }

        offset += width;

        let mut seqno_since = subscribe.listen.handle.machine.seqno_since();

        // We're starting out with the original, non-downgraded SeqNo
        assert_eq!(seqno_since, original_seqno_since);

        // We have to handle the parts we generate during the next loop to
        // ensure they don't panic.
        let mut subsequent_parts = vec![];

        // Ensure monotonicity of seqnos we're processing, otherwise the
        // invariant we're testing (returning the last part of a seqno will
        // downgrade its since) will not hold.
        let mut this_seqno = None;

        // Repeat the same process as above, more or less, while fetching + returning parts
        for (mut i, part) in parts.into_iter().enumerate() {
            let part_seqno = part.leased_seqno.unwrap();
            let last_seqno = this_seqno.replace(part_seqno);
            assert!(last_seqno.is_none() || this_seqno >= last_seqno);

            let (part, _) = fetcher.fetch_leased_part(part).await;

            // Emulating drop
            subscribe.return_leased_part(part);

            // Simulates an exchange
            let (parts, _) = subscribe.next().await;
            for part in parts {
                subsequent_parts.push(part.into_exchangeable_part());
            }

            subscribe
                .listen
                .handle
                .downgrade_since(&subscribe.listen.since)
                .await;

            // Write more new values
            i += offset;
            write
                .expect_compare_and_append(&data[i..i + 1], i as u64, i as u64 + 1)
                .await;

            // We should expect the SeqNo to be downgraded if this part's SeqNo
            // is no longer leased to any other parts, either.
            let expect_downgrade = subscribe
                .listen
                .handle
                .leased_seqnos
                .get(&part_seqno)
                .is_none();

            let new_seqno_since = subscribe.listen.handle.machine.seqno_since();
            if expect_downgrade {
                assert!(new_seqno_since > seqno_since);
            } else {
                assert_eq!(new_seqno_since, seqno_since);
            }
            seqno_since = new_seqno_since;
        }

        // SeqNo since was downgraded
        assert!(seqno_since > original_seqno_since);

        // Return any outstanding parts, to prevent a panic!
        for part in subsequent_parts {
            subscribe.return_leased_part(part.into());
        }

        drop(subscribe);
    }
}

// WIP the way skip_consensus_fetch_optimization works is pretty fundamentally
// incompatible with maintaining a lease
#[cfg(test)]
#[cfg(WIP)]
mod tests {
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_persist::location::Consensus;
    use mz_persist::mem::{MemBlob, MemBlobConfig, MemConsensus};
    use mz_persist::unreliable::{UnreliableConsensus, UnreliableHandle};
    use timely::ExchangeData;

    use crate::internal::metrics::Metrics;
    use crate::tests::all_ok;
    use crate::{PersistClient, PersistConfig};

    use super::*;

    // Verifies performance optimizations where a Listener doesn't fetch the
    // latest Consensus state if the one it currently has can serve the next
    // request.
    #[tokio::test]
    async fn skip_consensus_fetch_optimization() {
        mz_ore::test::init_logging();
        let data = vec![
            (("0".to_owned(), "zero".to_owned()), 0, 1),
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let blob = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let consensus = Arc::new(MemConsensus::default());
        let unreliable = UnreliableHandle::default();
        unreliable.totally_available();
        let consensus = Arc::new(UnreliableConsensus::new(consensus, unreliable.clone()))
            as Arc<dyn Consensus + Send + Sync>;
        let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));
        let (mut write, mut read) = PersistClient::new(
            PersistConfig::new(SYSTEM_TIME.clone()),
            blob,
            consensus,
            metrics,
        )
        .await
        .expect("client construction failed")
        .expect_open::<String, String, u64, i64>(ShardId::new())
        .await;

        write.expect_compare_and_append(&data[0..1], 0, 1).await;
        write.expect_compare_and_append(&data[1..2], 1, 2).await;
        write.expect_compare_and_append(&data[2..3], 2, 3).await;

        let mut snapshot = read.expect_snapshot_and_fetch(2).await;
        let mut listen = read.expect_listen(0).await;

        // Manually advance the listener's machine so that it has the latest
        // state by fetching the first events from next. This is awkward but
        // only necessary because we're about to do some weird things with
        // unreliable.
        let listen_actual = listen.next().await;
        let expected_events = vec![ListenEvent::Progress(Antichain::from_elem(1))];
        assert_eq!(listen_actual, expected_events);

        // At this point, the snapshot and listen's state should have all the
        // writes. Test this by making consensus completely unavailable.
        unreliable.totally_unavailable();
        assert_eq!(snapshot, all_ok(&data, 2));
        assert_eq!(
            listen.read_until(&3).await,
            (all_ok(&data[1..], 1), Antichain::from_elem(3))
        );
    }
}
