// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Read capabilities and handles

use async_stream::stream;
use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures::Stream;
use futures_util::{StreamExt, stream};
use mz_dyncfg::Config;
use mz_ore::halt;
use mz_ore::instrument;
use mz_ore::now::EpochMillis;
use mz_ore::task::{AbortOnDropHandle, JoinHandle, RuntimeExt};
use mz_persist::location::{Blob, SeqNo};
use mz_persist_types::columnar::{ColumnDecoder, Schema};
use mz_persist_types::{Codec, Codec64};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp};
use tokio::runtime::Handle;
use tracing::{Instrument, debug_span, warn};
use uuid::Uuid;

use crate::batch::BLOB_TARGET_SIZE;
use crate::cfg::{COMPACTION_MEMORY_BOUND_BYTES, RetryParameters};
use crate::fetch::FetchConfig;
use crate::fetch::{FetchBatchFilter, FetchedPart, Lease, LeasedBatchPart, fetch_leased_part};
use crate::internal::encoding::Schemas;
use crate::internal::machine::{ExpireFn, Machine};
use crate::internal::metrics::{Metrics, ReadMetrics, ShardMetrics};
use crate::internal::state::{BatchPart, HollowBatch};
use crate::internal::watch::StateWatch;
use crate::iter::{Consolidator, StructuredSort};
use crate::schema::SchemaCache;
use crate::stats::{SnapshotPartStats, SnapshotPartsStats, SnapshotStats};
use crate::{GarbageCollector, PersistConfig, ShardId, parse_id};

pub use crate::internal::encoding::LazyPartStats;
pub use crate::internal::state::Since;

/// An opaque identifier for a reader of a persist durable TVC (aka shard).
#[derive(Arbitrary, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LeasedReaderId(pub(crate) [u8; 16]);

impl std::fmt::Display for LeasedReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "r{}", Uuid::from_bytes(self.0))
    }
}

impl std::fmt::Debug for LeasedReaderId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LeasedReaderId({})", Uuid::from_bytes(self.0))
    }
}

impl std::str::FromStr for LeasedReaderId {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_id('r', "LeasedReaderId", s).map(LeasedReaderId)
    }
}

impl From<LeasedReaderId> for String {
    fn from(reader_id: LeasedReaderId) -> Self {
        reader_id.to_string()
    }
}

impl TryFrom<String> for LeasedReaderId {
    type Error = String;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl LeasedReaderId {
    pub(crate) fn new() -> Self {
        LeasedReaderId(*Uuid::new_v4().as_bytes())
    }
}

/// Capable of generating a snapshot of all data at `as_of`, followed by a
/// listen of all updates.
///
/// For more details, see [`ReadHandle::snapshot`] and [`Listen`].
#[derive(Debug)]
pub struct Subscribe<K: Codec, V: Codec, T, D> {
    snapshot: Option<Vec<LeasedBatchPart<T>>>,
    listen: Listen<K, V, T, D>,
}

impl<K, V, T, D> Subscribe<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    fn new(snapshot_parts: Vec<LeasedBatchPart<T>>, listen: Listen<K, V, T, D>) -> Self {
        Subscribe {
            snapshot: Some(snapshot_parts),
            listen,
        }
    }

    /// Returns a `LeasedBatchPart` enriched with the proper metadata.
    ///
    /// First returns snapshot parts, until they're exhausted, at which point
    /// begins returning listen parts.
    ///
    /// The returned `Antichain` represents the subscription progress as it will
    /// be _after_ the returned parts are fetched.
    #[instrument(level = "debug", fields(shard = %self.listen.handle.machine.shard_id()))]
    pub async fn next(
        &mut self,
        // If Some, an override for the default listen sleep retry parameters.
        listen_retry: Option<RetryParameters>,
    ) -> Vec<ListenEvent<T, LeasedBatchPart<T>>> {
        match self.snapshot.take() {
            Some(parts) => vec![ListenEvent::Updates(parts)],
            None => {
                let (parts, upper) = self.listen.next(listen_retry).await;
                vec![ListenEvent::Updates(parts), ListenEvent::Progress(upper)]
            }
        }
    }
}

impl<K, V, T, D> Subscribe<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// Equivalent to `next`, but rather than returning a [`LeasedBatchPart`],
    /// fetches and returns the data from within it.
    #[instrument(level = "debug", fields(shard = %self.listen.handle.machine.shard_id()))]
    pub async fn fetch_next(
        &mut self,
    ) -> Vec<ListenEvent<T, ((Result<K, String>, Result<V, String>), T, D)>> {
        let events = self.next(None).await;
        let new_len = events
            .iter()
            .map(|event| match event {
                ListenEvent::Updates(parts) => parts.len(),
                ListenEvent::Progress(_) => 1,
            })
            .sum();
        let mut ret = Vec::with_capacity(new_len);
        for event in events {
            match event {
                ListenEvent::Updates(parts) => {
                    for part in parts {
                        let fetched_part = self.listen.fetch_batch_part(part).await;
                        let updates = fetched_part.collect::<Vec<_>>();
                        if !updates.is_empty() {
                            ret.push(ListenEvent::Updates(updates));
                        }
                    }
                }
                ListenEvent::Progress(progress) => ret.push(ListenEvent::Progress(progress)),
            }
        }
        ret
    }

    /// Fetches the contents of `part` and returns its lease.
    pub async fn fetch_batch_part(&mut self, part: LeasedBatchPart<T>) -> FetchedPart<K, V, T, D> {
        self.listen.fetch_batch_part(part).await
    }
}

impl<K, V, T, D> Subscribe<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// Politely expires this subscribe, releasing its lease.
    ///
    /// There is a best-effort impl in Drop for [`ReadHandle`] to expire the
    /// [`ReadHandle`] held by the subscribe that wasn't explicitly expired
    /// with this method. When possible, explicit expiry is still preferred
    /// because the Drop one is best effort and is dependant on a tokio
    /// [Handle] being available in the TLC at the time of drop (which is a bit
    /// subtle). Also, explicit expiry allows for control over when it happens.
    pub async fn expire(mut self) {
        let _ = self.snapshot.take(); // Drop all leased parts.
        self.listen.expire().await;
    }
}

/// Data and progress events of a shard subscription.
///
/// TODO: Unify this with [timely::dataflow::operators::capture::event::Event].
#[derive(Debug, PartialEq)]
pub enum ListenEvent<T, D> {
    /// Progress of the shard.
    Progress(Antichain<T>),
    /// Data of the shard.
    Updates(Vec<D>),
}

/// An ongoing subscription of updates to a shard.
#[derive(Debug)]
pub struct Listen<K: Codec, V: Codec, T, D> {
    handle: ReadHandle<K, V, T, D>,
    watch: StateWatch<K, V, T, D>,

    as_of: Antichain<T>,
    since: Antichain<T>,
    frontier: Antichain<T>,
}

impl<K, V, T, D> Listen<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    async fn new(mut handle: ReadHandle<K, V, T, D>, as_of: Antichain<T>) -> Self {
        let since = as_of.clone();
        // This listen only needs to distinguish things after its frontier
        // (initially as_of although the frontier is inclusive and the as_of
        // isn't). Be a good citizen and downgrade early.
        handle.downgrade_since(&since).await;

        let watch = handle.machine.applier.watch();
        Listen {
            handle,
            watch,
            since,
            frontier: as_of.clone(),
            as_of,
        }
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
    pub async fn next(
        &mut self,
        // If Some, an override for the default listen sleep retry parameters.
        retry: Option<RetryParameters>,
    ) -> (Vec<LeasedBatchPart<T>>, Antichain<T>) {
        let batch = self
            .handle
            .machine
            .next_listen_batch(
                &self.frontier,
                &mut self.watch,
                Some(&self.handle.reader_id),
                retry,
            )
            .await;

        // A lot of things across mz have to line up to hold the following
        // invariant and violations only show up as subtle correctness errors,
        // so explicitly validate it here. Better to panic and roll back a
        // release than be incorrect (also potentially corrupting a sink).
        //
        // Note that the since check is intentionally less_than, not less_equal.
        // If a batch's since is X, that means we can no longer distinguish X
        // (beyond self.frontier) from X-1 (not beyond self.frontier) to keep
        // former and filter out the latter.
        let acceptable_desc = PartialOrder::less_than(batch.desc.since(), &self.frontier)
            // Special case when the frontier == the as_of (i.e. the first
            // time this is called on a new Listen). Because as_of is
            // _exclusive_, we don't need to be able to distinguish X from
            // X-1.
            || (self.frontier == self.as_of
            && PartialOrder::less_equal(batch.desc.since(), &self.frontier));
        if !acceptable_desc {
            let lease_state = self
                .handle
                .machine
                .applier
                .reader_lease(self.handle.reader_id.clone());
            if let Some(lease) = lease_state {
                panic!(
                    "Listen on {} received a batch {:?} advanced past the listen frontier {:?}, but the lease has not expired: {:?}",
                    self.handle.machine.shard_id(),
                    batch.desc,
                    self.frontier,
                    lease
                )
            } else {
                // Ideally we'd percolate this error up, so callers could eg. restart a dataflow
                // instead of restarting a process...
                halt!(
                    "Listen on {} received a batch {:?} advanced past the listen frontier {:?} after the reader has expired. \
                     This can happen in exceptional cases: a machine goes to sleep or is running out of memory or CPU, for example.",
                    self.handle.machine.shard_id(),
                    batch.desc,
                    self.frontier
                )
            }
        }

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
        // single element and it will be less_than upper. We
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

        // IMPORTANT! Make sure this `lease_batch_parts` stays before the
        // `maybe_downgrade_since` call. Otherwise, we might give up our
        // capability on the batch's SeqNo before we lease it, which could lead
        // to blobs that it references being GC'd.
        let filter = FetchBatchFilter::Listen {
            as_of: self.as_of.clone(),
            lower: self.frontier.clone(),
        };
        let parts = self.handle.lease_batch_parts(batch, filter).collect().await;

        self.handle.maybe_downgrade_since(&self.since).await;

        // NB: Keep this after we use self.frontier to join_assign self.since
        // and also after we construct metadata.
        self.frontier = new_frontier;

        (parts, self.frontier.clone())
    }
}

impl<K, V, T, D> Listen<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
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
    #[instrument(level = "debug", name = "listen::next", fields(shard = %self.handle.machine.shard_id()))]
    pub async fn fetch_next(
        &mut self,
    ) -> Vec<ListenEvent<T, ((Result<K, String>, Result<V, String>), T, D)>> {
        let (parts, progress) = self.next(None).await;
        let mut ret = Vec::with_capacity(parts.len() + 1);
        for part in parts {
            let fetched_part = self.fetch_batch_part(part).await;
            let updates = fetched_part.collect::<Vec<_>>();
            if !updates.is_empty() {
                ret.push(ListenEvent::Updates(updates));
            }
        }
        ret.push(ListenEvent::Progress(progress));
        ret
    }

    /// Convert listener into futures::Stream
    pub fn into_stream(
        mut self,
    ) -> impl Stream<Item = ListenEvent<T, ((Result<K, String>, Result<V, String>), T, D)>> {
        async_stream::stream!({
            loop {
                for msg in self.fetch_next().await {
                    yield msg;
                }
            }
        })
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
            for event in self.fetch_next().await {
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

impl<K, V, T, D> Listen<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// Fetches the contents of `part` and returns its lease.
    ///
    /// This is broken out into its own function to provide a trivial means for
    /// [`Subscribe`], which contains a [`Listen`], to fetch batches.
    async fn fetch_batch_part(&mut self, part: LeasedBatchPart<T>) -> FetchedPart<K, V, T, D> {
        let fetched_part = fetch_leased_part(
            &self.handle.cfg,
            &part,
            self.handle.blob.as_ref(),
            Arc::clone(&self.handle.metrics),
            &self.handle.metrics.read.listen,
            &self.handle.machine.applier.shard_metrics,
            &self.handle.reader_id,
            self.handle.read_schemas.clone(),
            &mut self.handle.schema_cache,
        )
        .await;
        fetched_part
    }

    /// Politely expires this listen, releasing its lease.
    ///
    /// There is a best-effort impl in Drop for [`ReadHandle`] to expire the
    /// [`ReadHandle`] held by the listen that wasn't explicitly expired with
    /// this method. When possible, explicit expiry is still preferred because
    /// the Drop one is best effort and is dependant on a tokio [Handle] being
    /// available in the TLC at the time of drop (which is a bit subtle). Also,
    /// explicit expiry allows for control over when it happens.
    pub async fn expire(self) {
        self.handle.expire().await
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
pub struct ReadHandle<K: Codec, V: Codec, T, D> {
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) machine: Machine<K, V, T, D>,
    pub(crate) gc: GarbageCollector<K, V, T, D>,
    pub(crate) blob: Arc<dyn Blob>,
    pub(crate) reader_id: LeasedReaderId,
    pub(crate) read_schemas: Schemas<K, V>,
    pub(crate) schema_cache: SchemaCache<K, V, T, D>,

    since: Antichain<T>,
    pub(crate) last_heartbeat: EpochMillis,
    pub(crate) leased_seqnos: BTreeMap<SeqNo, Lease>,
    pub(crate) unexpired_state: Option<UnexpiredReadHandleState>,
}

/// Length of time after a reader's last operation after which the reader may be
/// expired.
pub(crate) const READER_LEASE_DURATION: Config<Duration> = Config::new(
    "persist_reader_lease_duration",
    Duration::from_secs(60 * 15),
    "The time after which we'll clean up stale read leases",
);

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Debug + Codec,
    V: Debug + Codec,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    pub(crate) async fn new(
        cfg: PersistConfig,
        metrics: Arc<Metrics>,
        machine: Machine<K, V, T, D>,
        gc: GarbageCollector<K, V, T, D>,
        blob: Arc<dyn Blob>,
        reader_id: LeasedReaderId,
        read_schemas: Schemas<K, V>,
        since: Antichain<T>,
        last_heartbeat: EpochMillis,
    ) -> Self {
        let schema_cache = machine.applier.schema_cache();
        let expire_fn = Self::expire_fn(machine.clone(), gc.clone(), reader_id.clone());
        ReadHandle {
            cfg,
            metrics: Arc::clone(&metrics),
            machine: machine.clone(),
            gc: gc.clone(),
            blob,
            reader_id: reader_id.clone(),
            read_schemas,
            schema_cache,
            since,
            last_heartbeat,
            leased_seqnos: BTreeMap::new(),
            unexpired_state: Some(UnexpiredReadHandleState {
                expire_fn,
                _heartbeat_tasks: machine
                    .start_reader_heartbeat_tasks(reader_id, gc)
                    .await
                    .into_iter()
                    .map(JoinHandle::abort_on_drop)
                    .collect(),
            }),
        }
    }

    /// This handle's shard id.
    pub fn shard_id(&self) -> ShardId {
        self.machine.shard_id()
    }

    /// This handle's `since` frontier.
    ///
    /// This will always be greater or equal to the shard-global `since`.
    pub fn since(&self) -> &Antichain<T> {
        &self.since
    }

    fn outstanding_seqno(&mut self) -> Option<SeqNo> {
        while let Some(first) = self.leased_seqnos.first_entry() {
            if first.get().count() <= 1 {
                first.remove();
            } else {
                return Some(*first.key());
            }
        }
        None
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
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn downgrade_since(&mut self, new_since: &Antichain<T>) {
        // Guaranteed to be the smallest/oldest outstanding lease on a `SeqNo`.
        let outstanding_seqno = self.outstanding_seqno();

        let heartbeat_ts = (self.cfg.now)();
        let (_seqno, current_reader_since, maintenance) = self
            .machine
            .downgrade_since(&self.reader_id, outstanding_seqno, new_since, heartbeat_ts)
            .await;

        // Debugging for database-issues#4590.
        if let Some(outstanding_seqno) = outstanding_seqno {
            let seqnos_held = _seqno.0.saturating_sub(outstanding_seqno.0);
            // We get just over 1 seqno-per-second on average for a shard in
            // prod, so this is about an hour.
            const SEQNOS_HELD_THRESHOLD: u64 = 60 * 60;
            if seqnos_held >= SEQNOS_HELD_THRESHOLD {
                tracing::info!(
                    "{} reader {} holding an unexpected number of seqnos {} vs {}: {:?}. bt: {:?}",
                    self.machine.shard_id(),
                    self.reader_id,
                    outstanding_seqno,
                    _seqno,
                    self.leased_seqnos.keys().take(10).collect::<Vec<_>>(),
                    // The Debug impl of backtrace is less aesthetic, but will put the trace
                    // on a single line and play more nicely with our Honeycomb quota
                    Backtrace::force_capture(),
                );
            }
        }

        self.since = current_reader_since.0;
        // A heartbeat is just any downgrade_since traffic, so update the
        // internal rate limiter here to play nicely with `maybe_heartbeat`.
        self.last_heartbeat = heartbeat_ts;
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
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn listen(self, as_of: Antichain<T>) -> Result<Listen<K, V, T, D>, Since<T>> {
        let () = self.machine.verify_listen(&as_of)?;
        Ok(Listen::new(self, as_of).await)
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
    #[instrument(level = "trace", fields(shard = %self.machine.shard_id()))]
    pub async fn snapshot(
        &mut self,
        as_of: Antichain<T>,
    ) -> Result<Vec<LeasedBatchPart<T>>, Since<T>> {
        let batches = self.machine.snapshot(&as_of).await?;

        if !PartialOrder::less_equal(self.since(), &as_of) {
            return Err(Since(self.since().clone()));
        }

        let filter = FetchBatchFilter::Snapshot { as_of };
        let mut leased_parts = Vec::new();
        for batch in batches {
            // Flatten the HollowBatch into one LeasedBatchPart per key. Each key
            // corresponds to a "part" or s3 object. This allows persist_source
            // to distribute work by parts (smallish, more even size) instead of
            // batches (arbitrarily large).
            leased_parts.extend(
                self.lease_batch_parts(batch, filter.clone())
                    .collect::<Vec<_>>()
                    .await,
            );
        }
        Ok(leased_parts)
    }

    /// Returns a snapshot of all of a shard's data using `as_of`, followed by
    /// listening to any future updates.
    ///
    /// For more details on this operation's semantics, see [Self::snapshot] and
    /// [Self::listen].
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn subscribe(
        mut self,
        as_of: Antichain<T>,
    ) -> Result<Subscribe<K, V, T, D>, Since<T>> {
        let snapshot_parts = self.snapshot(as_of.clone()).await?;
        let listen = self.listen(as_of.clone()).await?;
        Ok(Subscribe::new(snapshot_parts, listen))
    }

    fn lease_batch_part(
        &mut self,
        desc: Description<T>,
        part: BatchPart<T>,
        filter: FetchBatchFilter<T>,
    ) -> LeasedBatchPart<T> {
        LeasedBatchPart {
            metrics: Arc::clone(&self.metrics),
            shard_id: self.machine.shard_id(),
            filter,
            desc,
            part,
            lease: self.lease_seqno(),
            filter_pushdown_audit: false,
        }
    }

    fn lease_batch_parts(
        &mut self,
        batch: HollowBatch<T>,
        filter: FetchBatchFilter<T>,
    ) -> impl Stream<Item = LeasedBatchPart<T>> + '_ {
        stream! {
            let blob = Arc::clone(&self.blob);
            let metrics = Arc::clone(&self.metrics);
            let desc = batch.desc.clone();
            for await part in batch.part_stream(self.shard_id(), &*blob, &*metrics) {
                yield self.lease_batch_part(desc.clone(), part.expect("leased part").into_owned(), filter.clone())
            }
        }
    }

    /// Tracks that the `ReadHandle`'s machine's current `SeqNo` is being
    /// "leased out" to a `LeasedBatchPart`, and cannot be garbage
    /// collected until its lease has been returned.
    fn lease_seqno(&mut self) -> Lease {
        let seqno = self.machine.seqno();
        let lease = self
            .leased_seqnos
            .entry(seqno)
            .or_insert_with(|| Lease::new(seqno));
        lease.clone()
    }

    /// Returns an independent [ReadHandle] with a new [LeasedReaderId] but the
    /// same `since`.
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn clone(&self, purpose: &str) -> Self {
        let new_reader_id = LeasedReaderId::new();
        let machine = self.machine.clone();
        let gc = self.gc.clone();
        let heartbeat_ts = (self.cfg.now)();
        let (reader_state, maintenance) = machine
            .register_leased_reader(
                &new_reader_id,
                purpose,
                READER_LEASE_DURATION.get(&self.cfg),
                heartbeat_ts,
                false,
            )
            .await;
        maintenance.start_performing(&machine, &gc);
        // The point of clone is that you're guaranteed to have the same (or
        // greater) since capability, verify that.
        // TODO: better if it's the same since capability exactly.
        assert!(PartialOrder::less_equal(&reader_state.since, &self.since));
        let new_reader = ReadHandle::new(
            self.cfg.clone(),
            Arc::clone(&self.metrics),
            machine,
            gc,
            Arc::clone(&self.blob),
            new_reader_id,
            self.read_schemas.clone(),
            reader_state.since,
            heartbeat_ts,
        )
        .await;
        new_reader
    }

    /// A rate-limited version of [Self::downgrade_since].
    ///
    /// This is an internally rate limited helper, designed to allow users to
    /// call it as frequently as they like. Call this or [Self::downgrade_since],
    /// on some interval that is "frequent" compared to the read lease duration.
    pub async fn maybe_downgrade_since(&mut self, new_since: &Antichain<T>) {
        let min_elapsed = READER_LEASE_DURATION.get(&self.cfg) / 4;
        let elapsed_since_last_heartbeat =
            Duration::from_millis((self.cfg.now)().saturating_sub(self.last_heartbeat));
        if elapsed_since_last_heartbeat >= min_elapsed {
            self.downgrade_since(new_since).await;
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
    #[instrument(level = "debug", fields(shard = %self.machine.shard_id()))]
    pub async fn expire(mut self) {
        // We drop the unexpired state before expiring the reader to ensure the
        // heartbeat tasks can never observe the expired state. This doesn't
        // matter for correctness, but avoids confusing log output if the
        // heartbeat task were to discover that its lease has been expired.
        let Some(unexpired_state) = self.unexpired_state.take() else {
            return;
        };
        unexpired_state.expire_fn.0().await;
    }

    fn expire_fn(
        machine: Machine<K, V, T, D>,
        gc: GarbageCollector<K, V, T, D>,
        reader_id: LeasedReaderId,
    ) -> ExpireFn {
        ExpireFn(Box::new(move || {
            Box::pin(async move {
                let (_, maintenance) = machine.expire_leased_reader(&reader_id).await;
                maintenance.start_performing(&machine, &gc);
            })
        }))
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

/// State for a read handle that has not been explicitly expired.
#[derive(Debug)]
pub(crate) struct UnexpiredReadHandleState {
    expire_fn: ExpireFn,
    pub(crate) _heartbeat_tasks: Vec<AbortOnDropHandle<()>>,
}

/// An incremental cursor through a particular shard, returned from [ReadHandle::snapshot_cursor].
///
/// To read an entire dataset, the
/// client should call `next` until it returns `None`, which signals all data has been returned...
/// but it's also free to abandon the instance at any time if it eg. only needs a few entries.
#[derive(Debug)]
pub struct Cursor<K: Codec, V: Codec, T: Timestamp + Codec64, D: Codec64, L = Lease> {
    consolidator: Consolidator<T, D, StructuredSort<K, V, T, D>>,
    max_len: usize,
    max_bytes: usize,
    _lease: L,
    read_schemas: Schemas<K, V>,
}

impl<K: Codec, V: Codec, T: Timestamp + Codec64, D: Codec64, L> Cursor<K, V, T, D, L> {
    /// Extracts and returns the lease from the cursor. Allowing the caller to
    /// do any necessary cleanup associated with the lease.
    pub fn into_lease(self: Self) -> L {
        self._lease
    }
}

impl<K, V, T, D, L> Cursor<K, V, T, D, L>
where
    K: Debug + Codec + Ord,
    V: Debug + Codec + Ord,
    T: Timestamp + Lattice + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    /// Grab the next batch of consolidated data.
    pub async fn next(
        &mut self,
    ) -> Option<impl Iterator<Item = ((Result<K, String>, Result<V, String>), T, D)> + '_> {
        let Self {
            consolidator,
            max_len,
            max_bytes,
            _lease,
            read_schemas: _,
        } = self;

        let part = consolidator
            .next_chunk(*max_len, *max_bytes)
            .await
            .expect("fetching a leased part")?;
        let key_decoder = self
            .read_schemas
            .key
            .decoder_any(part.key.as_ref())
            .expect("ok");
        let val_decoder = self
            .read_schemas
            .val
            .decoder_any(part.val.as_ref())
            .expect("ok");
        let iter = (0..part.len()).map(move |i| {
            let mut k = K::default();
            let mut v = V::default();
            key_decoder.decode(i, &mut k);
            val_decoder.decode(i, &mut v);
            let t = T::decode(part.time.value(i).to_le_bytes());
            let d = D::decode(part.diff.value(i).to_le_bytes());
            ((Ok(k), Ok(v)), t, d)
        });

        Some(iter)
    }
}

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Debug + Codec + Ord,
    V: Debug + Codec + Ord,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
{
    /// Generates a [Self::snapshot], and fetches all of the batches it
    /// contains.
    ///
    /// The output is consolidated. Furthermore, to keep memory usage down when
    /// reading a snapshot that consolidates well, this consolidates as it goes.
    ///
    /// Potential future improvements (if necessary):
    /// - Accept something like a `F: Fn(K,V) -> (K,V)` argument, which looks
    ///   like an MFP you might be pushing down. Reason being that if you are
    ///   projecting or transforming in a way that allows further consolidation,
    ///   amazing.
    /// - Reuse any code we write to streaming-merge consolidate in
    ///   persist_source here.
    pub async fn snapshot_and_fetch(
        &mut self,
        as_of: Antichain<T>,
    ) -> Result<Vec<((Result<K, String>, Result<V, String>), T, D)>, Since<T>> {
        let mut cursor = self.snapshot_cursor(as_of, |_| true).await?;
        let mut contents = Vec::new();
        while let Some(iter) = cursor.next().await {
            contents.extend(iter);
        }

        // We don't currently guarantee that encoding is one-to-one, so we still need to
        // consolidate the decoded outputs. However, let's report if this isn't a noop.
        let old_len = contents.len();
        consolidate_updates(&mut contents);
        if old_len != contents.len() {
            // TODO(bkirwi): do we need more / finer-grained metrics for this?
            self.machine
                .applier
                .shard_metrics
                .unconsolidated_snapshot
                .inc();
        }

        Ok(contents)
    }

    /// Generates a [Self::snapshot], and fetches all of the batches it
    /// contains.
    ///
    /// To keep memory usage down when reading a snapshot that consolidates well, this consolidates
    /// as it goes. However, note that only the serialized data is consolidated: the deserialized
    /// data will only be consolidated if your K/V codecs are one-to-one.
    pub async fn snapshot_cursor(
        &mut self,
        as_of: Antichain<T>,
        should_fetch_part: impl for<'a> Fn(Option<&'a LazyPartStats>) -> bool,
    ) -> Result<Cursor<K, V, T, D>, Since<T>> {
        let batches = self.machine.snapshot(&as_of).await?;
        let lease = self.lease_seqno();

        Self::read_batches_consolidated(
            &self.cfg,
            Arc::clone(&self.metrics),
            Arc::clone(&self.machine.applier.shard_metrics),
            self.metrics.read.snapshot.clone(),
            Arc::clone(&self.blob),
            self.shard_id(),
            as_of,
            self.read_schemas.clone(),
            &batches,
            lease,
            should_fetch_part,
            COMPACTION_MEMORY_BOUND_BYTES.get(&self.cfg),
        )
    }

    pub(crate) fn read_batches_consolidated<L>(
        persist_cfg: &PersistConfig,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        read_metrics: ReadMetrics,
        blob: Arc<dyn Blob>,
        shard_id: ShardId,
        as_of: Antichain<T>,
        schemas: Schemas<K, V>,
        batches: &[HollowBatch<T>],
        lease: L,
        should_fetch_part: impl for<'a> Fn(Option<&'a LazyPartStats>) -> bool,
        memory_budget_bytes: usize,
    ) -> Result<Cursor<K, V, T, D, L>, Since<T>> {
        let context = format!("{}[as_of={:?}]", shard_id, as_of.elements());
        let filter = FetchBatchFilter::Snapshot {
            as_of: as_of.clone(),
        };

        let mut consolidator = Consolidator::new(
            context,
            FetchConfig::from_persist_config(persist_cfg),
            shard_id,
            StructuredSort::new(schemas.clone()),
            blob,
            metrics,
            shard_metrics,
            read_metrics,
            filter,
            memory_budget_bytes,
        );
        for batch in batches {
            for (meta, run) in batch.runs() {
                consolidator.enqueue_run(
                    &batch.desc,
                    meta,
                    run.into_iter()
                        .filter(|p| should_fetch_part(p.stats()))
                        .cloned(),
                );
            }
        }
        // This default may end up consolidating more records than previously
        // for cases like fast-path peeks, where only the first few entries are used.
        // If this is a noticeable performance impact, thread the max-len in from the caller.
        let max_len = persist_cfg.compaction_yield_after_n_updates;
        let max_bytes = BLOB_TARGET_SIZE.get(persist_cfg).max(1);

        Ok(Cursor {
            consolidator,
            max_len,
            max_bytes,
            _lease: lease,
            read_schemas: schemas,
        })
    }

    /// Returns aggregate statistics about the contents of the shard TVC at the
    /// given frontier.
    ///
    /// This command returns the contents of this shard as of `as_of` once they
    /// are known. This may "block" (in an async-friendly way) if `as_of` is
    /// greater or equal to the current `upper` of the shard. If `None` is given
    /// for `as_of`, then the latest stats known by this process are used.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    pub fn snapshot_stats(
        &self,
        as_of: Option<Antichain<T>>,
    ) -> impl Future<Output = Result<SnapshotStats, Since<T>>> + Send + 'static {
        let machine = self.machine.clone();
        async move {
            let batches = match as_of {
                Some(as_of) => machine.snapshot(&as_of).await?,
                None => machine.applier.all_batches(),
            };
            let num_updates = batches.iter().map(|b| b.len).sum();
            Ok(SnapshotStats {
                shard_id: machine.shard_id(),
                num_updates,
            })
        }
    }

    /// Returns aggregate statistics about the contents of the shard TVC at the
    /// given frontier.
    ///
    /// This command returns the contents of this shard as of `as_of` once they
    /// are known. This may "block" (in an async-friendly way) if `as_of` is
    /// greater or equal to the current `upper` of the shard.
    ///
    /// The `Since` error indicates that the requested `as_of` cannot be served
    /// (the caller has out of date information) and includes the smallest
    /// `as_of` that would have been accepted.
    pub async fn snapshot_parts_stats(
        &self,
        as_of: Antichain<T>,
    ) -> Result<SnapshotPartsStats, Since<T>> {
        let batches = self.machine.snapshot(&as_of).await?;
        let parts = stream::iter(&batches)
            .flat_map(|b| b.part_stream(self.shard_id(), &*self.blob, &*self.metrics))
            .map(|p| {
                let p = p.expect("live batch");
                SnapshotPartStats {
                    encoded_size_bytes: p.encoded_size_bytes(),
                    stats: p.stats().cloned(),
                }
            })
            .collect()
            .await;
        Ok(SnapshotPartsStats {
            metrics: Arc::clone(&self.machine.applier.metrics),
            shard_id: self.machine.shard_id(),
            parts,
        })
    }
}

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Debug + Codec + Ord,
    V: Debug + Codec + Ord,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Codec64 + Send + Sync,
{
    /// Generates a [Self::snapshot], and streams out all of the updates
    /// it contains in bounded memory.
    ///
    /// The output is not consolidated.
    pub async fn snapshot_and_stream(
        &mut self,
        as_of: Antichain<T>,
    ) -> Result<
        impl Stream<Item = ((Result<K, String>, Result<V, String>), T, D)> + use<K, V, T, D>,
        Since<T>,
    > {
        let snap = self.snapshot(as_of).await?;

        let blob = Arc::clone(&self.blob);
        let metrics = Arc::clone(&self.metrics);
        let snapshot_metrics = self.metrics.read.snapshot.clone();
        let shard_metrics = Arc::clone(&self.machine.applier.shard_metrics);
        let reader_id = self.reader_id.clone();
        let schemas = self.read_schemas.clone();
        let mut schema_cache = self.schema_cache.clone();
        let persist_cfg = self.cfg.clone();
        let stream = async_stream::stream! {
            for part in snap {
                let mut fetched_part = fetch_leased_part(
                    &persist_cfg,
                    &part,
                    blob.as_ref(),
                    Arc::clone(&metrics),
                    &snapshot_metrics,
                    &shard_metrics,
                    &reader_id,
                    schemas.clone(),
                    &mut schema_cache,
                )
                .await;

                while let Some(next) = fetched_part.next() {
                    yield next;
                }
            }
        };

        Ok(stream)
    }
}

impl<K, V, T, D> ReadHandle<K, V, T, D>
where
    K: Debug + Codec + Ord,
    V: Debug + Codec + Ord,
    T: Timestamp + TotalOrder + Lattice + Codec64 + Sync,
    D: Semigroup + Ord + Codec64 + Send + Sync,
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

impl<K: Codec, V: Codec, T, D> Drop for ReadHandle<K, V, T, D> {
    fn drop(&mut self) {
        // We drop the unexpired state before expiring the reader to ensure the
        // heartbeat tasks can never observe the expired state. This doesn't
        // matter for correctness, but avoids confusing log output if the
        // heartbeat task were to discover that its lease has been expired.
        let Some(unexpired_state) = self.unexpired_state.take() else {
            return;
        };

        let handle = match Handle::try_current() {
            Ok(x) => x,
            Err(_) => {
                warn!(
                    "ReadHandle {} dropped without being explicitly expired, falling back to lease timeout",
                    self.reader_id
                );
                return;
            }
        };
        // Spawn a best-effort task to expire this read handle. It's fine if
        // this doesn't run to completion, we'd just have to wait out the lease
        // before the shard-global since is unblocked.
        //
        // Intentionally create the span outside the task to set the parent.
        let expire_span = debug_span!("drop::expire");
        handle.spawn_named(
            || format!("ReadHandle::expire ({})", self.reader_id),
            unexpired_state.expire_fn.0().instrument(expire_span),
        );
    }
}

#[cfg(test)]
mod tests {
    use std::pin;
    use std::str::FromStr;

    use mz_dyncfg::ConfigUpdates;
    use mz_ore::cast::CastFrom;
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist::mem::{MemBlob, MemBlobConfig, MemConsensus};
    use mz_persist::unreliable::{UnreliableConsensus, UnreliableHandle};
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use tokio_stream::StreamExt;

    use crate::async_runtime::IsolatedRuntime;
    use crate::batch::BLOB_TARGET_SIZE;
    use crate::cache::StateCache;
    use crate::internal::metrics::Metrics;
    use crate::rpc::NoopPubSubSender;
    use crate::tests::{all_ok, new_test_client};
    use crate::{Diagnostics, PersistClient, PersistConfig, ShardId};

    use super::*;

    // Verifies `Subscribe` can be dropped while holding snapshot batches.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn drop_unused_subscribe(dyncfgs: ConfigUpdates) {
        let data = [
            (("0".to_owned(), "zero".to_owned()), 0, 1),
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let (mut write, read) = new_test_client(&dyncfgs)
            .await
            .expect_open::<String, String, u64, i64>(crate::ShardId::new())
            .await;

        write.expect_compare_and_append(&data[0..1], 0, 1).await;
        write.expect_compare_and_append(&data[1..2], 1, 2).await;
        write.expect_compare_and_append(&data[2..3], 2, 3).await;

        let subscribe = read
            .subscribe(timely::progress::Antichain::from_elem(2))
            .await
            .unwrap();
        assert!(
            !subscribe.snapshot.as_ref().unwrap().is_empty(),
            "snapshot must have batches for test to be meaningful"
        );
        drop(subscribe);
    }

    // Verifies that we streaming-consolidate away identical key-values in the same batch.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn streaming_consolidate(dyncfgs: ConfigUpdates) {
        let data = &[
            // Identical records should sum together...
            (("k".to_owned(), "v".to_owned()), 0, 1),
            (("k".to_owned(), "v".to_owned()), 1, 1),
            (("k".to_owned(), "v".to_owned()), 2, 1),
            // ...and when they cancel out entirely they should be omitted.
            (("k2".to_owned(), "v".to_owned()), 0, 1),
            (("k2".to_owned(), "v".to_owned()), 1, -1),
        ];

        let (mut write, read) = {
            let client = new_test_client(&dyncfgs).await;
            client.cfg.set_config(&BLOB_TARGET_SIZE, 1000); // So our batch stays together!
            client
                .expect_open::<String, String, u64, i64>(crate::ShardId::new())
                .await
        };

        write.expect_compare_and_append(data, 0, 5).await;

        let mut snapshot = read
            .subscribe(timely::progress::Antichain::from_elem(4))
            .await
            .unwrap();

        let mut updates = vec![];
        'outer: loop {
            for event in snapshot.fetch_next().await {
                match event {
                    ListenEvent::Progress(t) => {
                        if !t.less_than(&4) {
                            break 'outer;
                        }
                    }
                    ListenEvent::Updates(data) => {
                        updates.extend(data);
                    }
                }
            }
        }
        assert_eq!(
            updates,
            &[((Ok("k".to_owned()), Ok("v".to_owned())), 4u64, 3i64)],
        )
    }

    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn snapshot_and_stream(dyncfgs: ConfigUpdates) {
        let data = &mut [
            (("k1".to_owned(), "v1".to_owned()), 0, 1),
            (("k2".to_owned(), "v2".to_owned()), 1, 1),
            (("k3".to_owned(), "v3".to_owned()), 2, 1),
            (("k4".to_owned(), "v4".to_owned()), 2, 1),
            (("k5".to_owned(), "v5".to_owned()), 3, 1),
        ];

        let (mut write, mut read) = {
            let client = new_test_client(&dyncfgs).await;
            client.cfg.set_config(&BLOB_TARGET_SIZE, 0); // split batches across multiple parts
            client
                .expect_open::<String, String, u64, i64>(crate::ShardId::new())
                .await
        };

        write.expect_compare_and_append(&data[0..2], 0, 2).await;
        write.expect_compare_and_append(&data[2..4], 2, 3).await;
        write.expect_compare_and_append(&data[4..], 3, 4).await;

        let as_of = Antichain::from_elem(3);
        let mut snapshot = pin::pin!(read.snapshot_and_stream(as_of.clone()).await.unwrap());

        let mut snapshot_rows = vec![];
        while let Some(((k, v), t, d)) = snapshot.next().await {
            snapshot_rows.push(((k.expect("valid key"), v.expect("valid key")), t, d));
        }

        for ((_k, _v), t, _d) in data.as_mut_slice() {
            t.advance_by(as_of.borrow());
        }

        assert_eq!(data.as_slice(), snapshot_rows.as_slice());
    }

    // Verifies the semantics of `SeqNo` leases + checks dropping `LeasedBatchPart` semantics.
    #[mz_persist_proc::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // https://github.com/MaterializeInc/database-issues/issues/5964
    async fn seqno_leases(dyncfgs: ConfigUpdates) {
        let mut data = vec![];
        for i in 0..20 {
            data.push(((i.to_string(), i.to_string()), i, 1))
        }

        let shard_id = ShardId::new();

        let client = new_test_client(&dyncfgs).await;
        let (mut write, read) = client
            .expect_open::<String, String, u64, i64>(shard_id)
            .await;

        // Seed with some values
        let mut offset = 0;
        let mut width = 2;

        for i in offset..offset + width {
            write
                .expect_compare_and_append(
                    &data[i..i + 1],
                    u64::cast_from(i),
                    u64::cast_from(i) + 1,
                )
                .await;
        }
        offset += width;

        // Create machinery for subscribe + fetch
        let mut fetcher = client
            .create_batch_fetcher::<String, String, u64, i64>(
                shard_id,
                Default::default(),
                Default::default(),
                false,
                Diagnostics::for_tests(),
            )
            .await
            .unwrap();

        let mut subscribe = read
            .subscribe(timely::progress::Antichain::from_elem(1))
            .await
            .expect("cannot serve requested as_of");

        // Determine sequence number at outset.
        let original_seqno_since = subscribe.listen.handle.machine.applier.seqno_since();

        let mut parts = vec![];

        width = 4;
        // Collect parts while continuing to write values
        for i in offset..offset + width {
            for event in subscribe.next(None).await {
                if let ListenEvent::Updates(mut new_parts) = event {
                    parts.append(&mut new_parts);
                    // Here and elsewhere we "cheat" and immediately downgrade the since
                    // to demonstrate the effects of SeqNo leases immediately.
                    subscribe
                        .listen
                        .handle
                        .downgrade_since(&subscribe.listen.since)
                        .await;
                }
            }

            write
                .expect_compare_and_append(
                    &data[i..i + 1],
                    u64::cast_from(i),
                    u64::cast_from(i) + 1,
                )
                .await;

            // SeqNo is not downgraded
            assert_eq!(
                subscribe.listen.handle.machine.applier.seqno_since(),
                original_seqno_since
            );
        }

        offset += width;

        let mut seqno_since = subscribe.listen.handle.machine.applier.seqno_since();

        // We're starting out with the original, non-downgraded SeqNo
        assert_eq!(seqno_since, original_seqno_since);

        // We have to handle the parts we generate during the next loop to
        // ensure they don't panic.
        let mut subsequent_parts = vec![];

        // Ensure monotonicity of seqnos we're processing, otherwise the
        // invariant we're testing (returning the last part of a seqno will
        // downgrade its since) will not hold.
        let mut this_seqno = SeqNo::minimum();

        // Repeat the same process as above, more or less, while fetching + returning parts
        for (mut i, part) in parts.into_iter().enumerate() {
            let part_seqno = part.lease.seqno();
            let last_seqno = this_seqno;
            this_seqno = part_seqno;
            assert!(this_seqno >= last_seqno);

            let (part, lease) = part.into_exchangeable_part();
            let _ = fetcher.fetch_leased_part(part).await;
            drop(lease);

            // Simulates an exchange
            for event in subscribe.next(None).await {
                if let ListenEvent::Updates(parts) = event {
                    for part in parts {
                        let (_, lease) = part.into_exchangeable_part();
                        subsequent_parts.push(lease);
                    }
                }
            }

            subscribe
                .listen
                .handle
                .downgrade_since(&subscribe.listen.since)
                .await;

            // Write more new values
            i += offset;
            write
                .expect_compare_and_append(
                    &data[i..i + 1],
                    u64::cast_from(i),
                    u64::cast_from(i) + 1,
                )
                .await;

            // We should expect the SeqNo to be downgraded if this part's SeqNo
            // is no longer leased to any other parts, either.
            let expect_downgrade = subscribe.listen.handle.outstanding_seqno() > Some(part_seqno);

            let new_seqno_since = subscribe.listen.handle.machine.applier.seqno_since();
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
        drop(subsequent_parts);
        drop(subscribe);
    }

    #[mz_ore::test]
    fn reader_id_human_readable_serde() {
        #[derive(Debug, Serialize, Deserialize)]
        struct Container {
            reader_id: LeasedReaderId,
        }

        // roundtrip through json
        let id =
            LeasedReaderId::from_str("r00000000-1234-5678-0000-000000000000").expect("valid id");
        assert_eq!(
            id,
            serde_json::from_value(serde_json::to_value(id.clone()).expect("serializable"))
                .expect("deserializable")
        );

        // deserialize a serialized string directly
        assert_eq!(
            id,
            serde_json::from_str("\"r00000000-1234-5678-0000-000000000000\"")
                .expect("deserializable")
        );

        // roundtrip id through a container type
        let json = json!({ "reader_id": id });
        assert_eq!(
            "{\"reader_id\":\"r00000000-1234-5678-0000-000000000000\"}",
            &json.to_string()
        );
        let container: Container = serde_json::from_value(json).expect("deserializable");
        assert_eq!(container.reader_id, id);
    }

    // Verifies performance optimizations where a Listener doesn't fetch the
    // latest Consensus state if the one it currently has can serve the next
    // request.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // too slow
    async fn skip_consensus_fetch_optimization() {
        let data = vec![
            (("0".to_owned(), "zero".to_owned()), 0, 1),
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
        ];

        let cfg = PersistConfig::new_for_tests();
        let blob = Arc::new(MemBlob::open(MemBlobConfig::default()));
        let consensus = Arc::new(MemConsensus::default());
        let unreliable = UnreliableHandle::default();
        unreliable.totally_available();
        let consensus = Arc::new(UnreliableConsensus::new(consensus, unreliable.clone()));
        let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
        let pubsub_sender = Arc::new(NoopPubSubSender);
        let (mut write, mut read) = PersistClient::new(
            cfg,
            blob,
            consensus,
            metrics,
            Arc::new(IsolatedRuntime::default()),
            Arc::new(StateCache::new_no_metrics()),
            pubsub_sender,
        )
        .expect("client construction failed")
        .expect_open::<String, String, u64, i64>(ShardId::new())
        .await;

        write.expect_compare_and_append(&data[0..1], 0, 1).await;
        write.expect_compare_and_append(&data[1..2], 1, 2).await;
        write.expect_compare_and_append(&data[2..3], 2, 3).await;

        let snapshot = read.expect_snapshot_and_fetch(2).await;
        let mut listen = read.expect_listen(0).await;

        // Manually advance the listener's machine so that it has the latest
        // state by fetching the first events from next. This is awkward but
        // only necessary because we're about to do some weird things with
        // unreliable.
        let listen_actual = listen.fetch_next().await;
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
