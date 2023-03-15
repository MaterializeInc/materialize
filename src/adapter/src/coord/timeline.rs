// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A mechanism to ensure that a sequence of writes and reads proceed correctly through timestamps.

use std::cmp;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::future::Future;
use std::thread;
use std::time::Duration;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use once_cell::sync::Lazy;
use timely::progress::Timestamp as TimelyTimestamp;
use tracing::error;

use mz_compute_client::controller::ComputeInstanceId;
use mz_expr::{CollectionPlan, OptimizedMirRelationExpr};
use mz_ore::collections::CollectionExt;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_ore::vec::VecExt;
use mz_repr::{GlobalId, Timestamp, TimestampManipulation};
use mz_sql::names::{ResolvedDatabaseSpecifier, SchemaSpecifier};
use mz_storage_client::types::sources::Timeline;

use crate::catalog::CatalogItem;
use crate::client::ConnectionId;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::read_policy::ReadHolds;
use crate::coord::{timeline, Coordinator};
use crate::util::ResultExt;
use crate::AdapterError;

/// An enum describing the timeline context of a query.
#[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum TimelineContext {
    /// Can only ever belong to a single specific timeline. The answer will depend on a timestamp
    /// chosen from that specific timeline.
    TimelineDependent(Timeline),
    /// Can belong to any timeline. The answer will depend on a timestamp chosen from some
    /// timeline.
    TimestampDependent,
    /// The answer does not depend on a chosen timestamp.
    TimestampIndependent,
}

impl TimelineContext {
    /// Whether or not the context contains a timeline.
    pub fn contains_timeline(&self) -> bool {
        self.timeline().is_some()
    }

    /// The timeline belonging to this context, if one exists.
    pub fn timeline(&self) -> Option<&Timeline> {
        match self {
            Self::TimelineDependent(timeline) => Some(timeline),
            Self::TimestampIndependent | Self::TimestampDependent => None,
        }
    }
}

/// Timestamps used by writes in an Append command.
#[derive(Debug)]
pub struct WriteTimestamp<T = mz_repr::Timestamp> {
    /// Timestamp that the write will take place on.
    pub(crate) timestamp: T,
    /// Timestamp to advance the appended table to.
    pub(crate) advance_to: T,
}

/// Global state for a single timeline.
///
/// For each timeline we maintain a timestamp oracle, which is responsible for
/// providing read (and sometimes write) timestamps, and a set of read holds which
/// guarantee that those read timestamps are valid.
pub(crate) struct TimelineState<T> {
    pub(crate) oracle: DurableTimestampOracle<T>,
    pub(crate) read_holds: ReadHolds<T>,
}

impl<T: fmt::Debug> fmt::Debug for TimelineState<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimelineState")
            .field("read_holds", &self.read_holds)
            .finish()
    }
}

/// A timeline can perform reads and writes. Reads happen at the read timestamp
/// and writes happen at the write timestamp. After the write has completed, but before a response
/// is sent, the read timestamp must be updated to a value greater than or equal to `self.write_ts`.
struct TimestampOracleState<T> {
    read_ts: T,
    write_ts: T,
}

/// A type that provides write and read timestamps, reads observe exactly their preceding writes..
///
/// Specifically, all read timestamps will be greater or equal to all previously reported completed
/// write timestamps, and strictly less than all subsequently emitted write timestamps.
struct TimestampOracle<T> {
    state: TimestampOracleState<T>,
    next: Box<dyn Fn() -> T>,
}

impl<T: TimestampManipulation> TimestampOracle<T> {
    /// Create a new timeline, starting at the indicated time. `next` generates
    /// new timestamps when invoked. The timestamps have no requirements, and can
    /// retreat from previous invocations.
    pub fn new<F>(initially: T, next: F) -> Self
    where
        F: Fn() -> T + 'static,
    {
        Self {
            state: TimestampOracleState {
                read_ts: initially.clone(),
                write_ts: initially,
            },
            next: Box::new(next),
        }
    }

    /// Acquire a new timestamp for writing.
    ///
    /// This timestamp will be strictly greater than all prior values of
    /// `self.read_ts()` and `self.write_ts()`.
    fn write_ts(&mut self) -> WriteTimestamp<T> {
        let mut next = (self.next)();
        if next.less_equal(&self.state.write_ts) {
            next = self.state.write_ts.step_forward();
        }
        assert!(self.state.read_ts.less_than(&next));
        assert!(self.state.write_ts.less_than(&next));
        self.state.write_ts = next.clone();
        assert!(self.state.read_ts.less_equal(&self.state.write_ts));
        let advance_to = next.step_forward();
        WriteTimestamp {
            timestamp: next,
            advance_to,
        }
    }

    /// Peek the current write timestamp.
    fn peek_write_ts(&self) -> T {
        self.state.write_ts.clone()
    }

    /// Acquire a new timestamp for reading.
    ///
    /// This timestamp will be greater or equal to all prior values of `self.apply_write(write_ts)`,
    /// and strictly less than all subsequent values of `self.write_ts()`.
    fn read_ts(&self) -> T {
        self.state.read_ts.clone()
    }

    /// Mark a write at `write_ts` completed.
    ///
    /// All subsequent values of `self.read_ts()` will be greater or equal to `write_ts`.
    fn apply_write(&mut self, write_ts: T) {
        if self.state.read_ts.less_than(&write_ts) {
            self.state.read_ts = write_ts;

            if self.state.write_ts.less_than(&self.state.read_ts) {
                self.state.write_ts = self.state.read_ts.clone();
            }
        }
        assert!(self.state.read_ts.less_equal(&self.state.write_ts));
    }
}

/// Interval used to persist durable timestamps. See [`DurableTimestampOracle`] for more
/// details.
pub static TIMESTAMP_PERSIST_INTERVAL: Lazy<mz_repr::Timestamp> = Lazy::new(|| {
    Duration::from_secs(5)
        .as_millis()
        .try_into()
        .expect("5 seconds can fit into `Timestamp`")
});

/// The Coordinator tries to prevent the persisted timestamp from exceeding
/// a value [`TIMESTAMP_INTERVAL_UPPER_BOUND`] times [`TIMESTAMP_PERSIST_INTERVAL`]
/// larger than the current system time.
pub const TIMESTAMP_INTERVAL_UPPER_BOUND: u64 = 2;

/// Convenience function for calculating the current upper bound that we want to
/// prevent the global timestamp from exceeding.
fn upper_bound(now: &Timestamp) -> Timestamp {
    now.saturating_add(
        TIMESTAMP_PERSIST_INTERVAL.saturating_mul(Timestamp::from(TIMESTAMP_INTERVAL_UPPER_BOUND)),
    )
}

/// Returns the current system time while protecting against backwards time
/// jumps.
///
/// The caller is responsible for providing the previously recorded system time
/// via the `previous_now` parameter.
///
/// If `previous_now` is more than `TIMESTAMP_INTERVAL_UPPER_BOUND *
/// TIMESTAMP_PERSIST_INTERVAL` milliseconds ahead of the current system time
/// (i.e., due to a backwards time jump), this function will block until the
/// system time advances.
///
/// The returned time is guaranteed to be greater than or equal to
/// `previous_now`.
pub fn monotonic_now(now: NowFn, previous_now: Timestamp) -> Timestamp {
    let mut now_ts = now();
    let monotonic_now = cmp::max(previous_now, now_ts.into());
    let mut upper_bound = timeline::upper_bound(&mz_repr::Timestamp::from(now_ts));
    while monotonic_now > upper_bound {
        // Cap retry time to 1s. In cases where the system clock has retreated
        // by some large amount of time, this prevents against then waiting for
        // that large amount of time in case the system clock then advances back
        // to near what it was.
        let remaining_ms = cmp::min(monotonic_now.saturating_sub(upper_bound), 1_000.into());
        error!(
            "Coordinator tried to start with initial timestamp of \
            {monotonic_now}, which is more than \
            {TIMESTAMP_INTERVAL_UPPER_BOUND} intervals of size {} larger than \
            now, {now_ts}. Sleeping for {remaining_ms} ms.",
            *TIMESTAMP_PERSIST_INTERVAL
        );
        thread::sleep(Duration::from_millis(remaining_ms.into()));
        now_ts = now();
        upper_bound = timeline::upper_bound(&mz_repr::Timestamp::from(now_ts));
    }
    monotonic_now
}

/// A type that wraps a [`TimestampOracle`] and provides durable timestamps. This allows us to
/// recover a timestamp that is larger than all previous timestamps on restart. The protocol
/// is based on timestamp recovery from Percolator <https://research.google/pubs/pub36726/>. We
/// "pre-allocate" a group of timestamps at once, and only durably store the largest of those
/// timestamps. All timestamps within that interval can be served directly from memory, without
/// going to disk. On restart, we re-initialize the current timestamp to a value one larger
/// than the persisted timestamp.
///
/// See [`TimestampOracle`] for more details on the properties of the timestamps.
pub struct DurableTimestampOracle<T> {
    timestamp_oracle: TimestampOracle<T>,
    durable_timestamp: T,
    persist_interval: T,
}

impl<T: TimestampManipulation> DurableTimestampOracle<T> {
    /// Create a new durable timeline, starting at the indicated time. Timestamps will be
    /// allocated in groups of size `persist_interval`. Also returns the new timestamp that
    /// needs to be persisted to disk.
    ///
    /// See [`TimestampOracle::new`] for more details.
    pub(crate) async fn new<F, Fut>(
        initially: T,
        next: F,
        persist_interval: T,
        persist_fn: impl FnOnce(T) -> Fut,
    ) -> Self
    where
        F: Fn() -> T + 'static,
        Fut: Future<Output = Result<(), crate::catalog::Error>>,
    {
        let mut oracle = Self {
            timestamp_oracle: TimestampOracle::new(initially.clone(), next),
            durable_timestamp: initially.clone(),
            persist_interval,
        };
        oracle
            .maybe_allocate_new_timestamps(&initially, persist_fn)
            .await;
        oracle
    }

    /// Acquire a new timestamp for writing. Optionally returns a timestamp that needs to be
    /// persisted to disk.
    ///
    /// See [`TimestampOracle::write_ts`] for more details.
    async fn write_ts<Fut>(&mut self, persist_fn: impl FnOnce(T) -> Fut) -> WriteTimestamp<T>
    where
        Fut: Future<Output = Result<(), crate::catalog::Error>>,
    {
        let ts = self.timestamp_oracle.write_ts();
        self.maybe_allocate_new_timestamps(&ts.timestamp, persist_fn)
            .await;
        ts
    }

    /// Peek current write timestamp.
    fn peek_write_ts(&self) -> T {
        self.timestamp_oracle.peek_write_ts()
    }

    /// Acquire a new timestamp for reading. Optionally returns a timestamp that needs to be
    /// persisted to disk.
    ///
    /// See [`TimestampOracle::read_ts`] for more details.
    pub fn read_ts(&self) -> T {
        let ts = self.timestamp_oracle.read_ts();
        assert!(
            ts.less_than(&self.durable_timestamp),
            "read_ts should not advance the global timestamp"
        );
        ts
    }

    /// Mark a write at `write_ts` completed.
    ///
    /// See [`TimestampOracle::apply_write`] for more details.
    pub async fn apply_write<Fut>(&mut self, lower_bound: T, persist_fn: impl FnOnce(T) -> Fut)
    where
        Fut: Future<Output = Result<(), crate::catalog::Error>>,
    {
        self.timestamp_oracle.apply_write(lower_bound.clone());
        self.maybe_allocate_new_timestamps(&lower_bound, persist_fn)
            .await;
    }

    /// Checks to see if we can serve the timestamp from memory, or if we need to durably store
    /// a new timestamp.
    ///
    /// If `ts` is less than the persisted timestamp then we can serve `ts` from memory,
    /// otherwise we need to durably store some timestamp greater than `ts`.
    async fn maybe_allocate_new_timestamps<Fut>(
        &mut self,
        ts: &T,
        persist_fn: impl FnOnce(T) -> Fut,
    ) where
        Fut: Future<Output = Result<(), crate::catalog::Error>>,
    {
        if self.durable_timestamp.less_equal(ts)
            // Since the timestamp is at its max value, we know that no other Coord can
            // allocate a higher value.
            && self.durable_timestamp.less_than(&T::maximum())
        {
            self.durable_timestamp = ts.step_forward_by(&self.persist_interval);
            persist_fn(self.durable_timestamp.clone())
                .await
                .unwrap_or_terminate("can't persist timestamp");
        }
    }
}

impl Coordinator {
    pub(crate) fn now(&self) -> EpochMillis {
        (self.catalog.config().now)()
    }

    pub(crate) fn now_datetime(&self) -> DateTime<Utc> {
        to_datetime(self.now())
    }

    pub(crate) fn get_timestamp_oracle_mut(
        &mut self,
        timeline: &Timeline,
    ) -> &mut DurableTimestampOracle<Timestamp> {
        &mut self
            .global_timelines
            .get_mut(timeline)
            .expect("all timelines have a timestamp oracle")
            .oracle
    }

    pub(crate) fn get_timestamp_oracle(
        &self,
        timeline: &Timeline,
    ) -> &DurableTimestampOracle<Timestamp> {
        &self
            .global_timelines
            .get(timeline)
            .expect("all timelines have a timestamp oracle")
            .oracle
    }

    /// Returns a reference to the timestamp oracle used for reads and writes
    /// from/to a local input.
    fn get_local_timestamp_oracle(&self) -> &DurableTimestampOracle<Timestamp> {
        self.get_timestamp_oracle(&Timeline::EpochMilliseconds)
    }

    /// Assign a timestamp for a read from a local input. Reads following writes
    /// must be at a time >= the write's timestamp; we choose "equal to" for
    /// simplicity's sake and to open as few new timestamps as possible.
    pub(crate) fn get_local_read_ts(&self) -> Timestamp {
        self.get_local_timestamp_oracle().read_ts()
    }

    /// Assign a timestamp for a write to a local input and increase the local ts.
    /// Writes following reads must ensure that they are assigned a strictly larger
    /// timestamp to ensure they are not visible to any real-time earlier reads.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn get_local_write_ts(&mut self) -> WriteTimestamp {
        self.global_timelines
            .get_mut(&Timeline::EpochMilliseconds)
            .expect("no realtime timeline")
            .oracle
            .write_ts(|ts| {
                self.catalog
                    .persist_timestamp(&Timeline::EpochMilliseconds, ts)
            })
            .await
    }

    /// Peek the current timestamp used for operations on local inputs. Used to determine how much
    /// to block group commits by.
    pub(crate) fn peek_local_write_ts(&self) -> Timestamp {
        self.get_local_timestamp_oracle().peek_write_ts()
    }

    /// Peek the current timestamp used for operations on local inputs. Used to determine how much
    /// to block group commits by.
    pub(crate) async fn apply_local_write(&mut self, timestamp: Timestamp) {
        let now = self.now().into();
        if timestamp > timeline::upper_bound(&now) {
            error!(
                "Setting local read timestamp to {timestamp}, which is more than \
                {TIMESTAMP_INTERVAL_UPPER_BOUND} intervals of size {} larger than now, {now}",
                *TIMESTAMP_PERSIST_INTERVAL
            );
        }
        self.global_timelines
            .get_mut(&Timeline::EpochMilliseconds)
            .expect("no realtime timeline")
            .oracle
            .apply_write(timestamp, |ts| {
                self.catalog
                    .persist_timestamp(&Timeline::EpochMilliseconds, ts)
            })
            .await;
    }

    /// Ensures that a global timeline state exists for `timeline`.
    pub(crate) async fn ensure_timeline_state<'a>(
        &'a mut self,
        timeline: &'a Timeline,
    ) -> &mut TimelineState<Timestamp> {
        Self::ensure_timeline_state_with_initial_time(
            timeline,
            Timestamp::minimum(),
            self.catalog.config().now.clone(),
            |ts| self.catalog.persist_timestamp(timeline, ts),
            &mut self.global_timelines,
        )
        .await
    }

    /// Ensures that a global timeline state exists for `timeline`, with an initial time
    /// of `initially`.
    pub(crate) async fn ensure_timeline_state_with_initial_time<'a, Fut>(
        timeline: &'a Timeline,
        initially: Timestamp,
        now: NowFn,
        persist_fn: impl FnOnce(Timestamp) -> Fut,
        global_timelines: &'a mut BTreeMap<Timeline, TimelineState<Timestamp>>,
    ) -> &'a mut TimelineState<Timestamp>
    where
        Fut: Future<Output = Result<(), crate::catalog::Error>>,
    {
        if !global_timelines.contains_key(timeline) {
            let oracle = if timeline == &Timeline::EpochMilliseconds {
                DurableTimestampOracle::new(
                    initially,
                    move || (now)().into(),
                    *TIMESTAMP_PERSIST_INTERVAL,
                    persist_fn,
                )
                .await
            } else {
                DurableTimestampOracle::new(
                    initially,
                    Timestamp::minimum,
                    *TIMESTAMP_PERSIST_INTERVAL,
                    persist_fn,
                )
                .await
            };
            global_timelines.insert(
                timeline.clone(),
                TimelineState {
                    oracle,
                    read_holds: ReadHolds::new(),
                },
            );
        }
        global_timelines.get_mut(timeline).expect("inserted above")
    }

    pub(crate) fn associate_with_timelines(
        &self,
        storage_ids: impl IntoIterator<Item = GlobalId> + Clone,
        compute_ids: impl IntoIterator<Item = (ComputeInstanceId, GlobalId)> + Clone,
        clusters: impl IntoIterator<Item = ComputeInstanceId> + Clone,
    ) -> BTreeMap<Timeline, (bool, CollectionIdBundle)> {
        let mut compute: BTreeMap<_, BTreeSet<_>> = BTreeMap::new();

        // Collect all compute_ids.
        for (instance_id, id) in compute_ids {
            compute.entry(instance_id).or_default().insert(id);
        }

        // Collect all GlobalIds associated with a compute instance ID.
        let cluster_set: BTreeSet<_> = clusters.into_iter().collect();
        for (_timeline, TimelineState { read_holds, .. }) in &self.global_timelines {
            let holds = read_holds
                .compute_ids()
                .filter(|(instance_id, _id)| cluster_set.contains(instance_id));
            for (instance_id, ids) in holds {
                let ids = ids.map(|(_antichain, id)| id);
                if let Some(set) = compute.get_mut(instance_id) {
                    set.extend(ids);
                } else {
                    compute.insert(*instance_id, ids.copied().collect());
                }
            }
        }

        // Create a bundle which we can check timelines against.
        let bundle = CollectionIdBundle {
            storage_ids: storage_ids.into_iter().collect(),
            compute_ids: compute,
        };

        self.partition_ids_by_timeline_context(&bundle)
            .filter_map(|(context, bundle)| {
                let TimelineContext::TimelineDependent(timeline) = context else {
                    return None;
                };
                let TimelineState { read_holds, .. } = self
                    .global_timelines
                    .get(&timeline)
                    .expect("all timeslines have a timestamp oracle");

                let empty = read_holds.id_bundle().difference(&bundle).is_empty();

                Some((timeline, (empty, bundle)))
            })
            .collect()
    }

    pub(crate) fn remove_resources_associate_with_timeline<I>(&mut self, associations: I)
    where
        I: IntoIterator<Item = (Timeline, (bool, CollectionIdBundle))>,
    {
        for (timeline, (empty, bundle)) in associations {
            let TimelineState { read_holds, .. } = self
                .global_timelines
                .get_mut(&timeline)
                .expect("all timeslines have a timestamp oracle");

            // Remove all of the underlying resources.
            for id in bundle.storage_ids {
                read_holds.remove_storage_id(&id);
            }
            for (compute_id, ids) in bundle.compute_ids {
                for id in ids {
                    read_holds.remove_compute_id(&compute_id, &id);
                }
            }

            // If we thought this Timeline should now be empty, let's assert that it is.
            assert_eq!(empty, read_holds.is_empty());

            // Finally, remove the Timeline if it's empty.
            if empty {
                self.global_timelines.remove(&timeline);
            }
        }
    }

    pub(crate) fn remove_compute_ids_from_timeline<I>(&mut self, ids: I) -> Vec<Timeline>
    where
        I: IntoIterator<Item = (ComputeInstanceId, GlobalId)>,
    {
        let mut empty_timelines = BTreeSet::new();
        for (compute_instance, id) in ids {
            for (timeline, TimelineState { read_holds, .. }) in &mut self.global_timelines {
                read_holds.remove_compute_id(&compute_instance, &id);
                if read_holds.is_empty() {
                    empty_timelines.insert(timeline.clone());
                }
            }
        }
        empty_timelines.into_iter().collect()
    }

    pub(crate) fn ids_in_timeline(&self, timeline: &Timeline) -> CollectionIdBundle {
        let mut id_bundle = CollectionIdBundle::default();
        for entry in self.catalog.entries() {
            if let TimelineContext::TimelineDependent(entry_timeline) =
                self.get_timeline_context(entry.id())
            {
                if timeline == &entry_timeline {
                    match entry.item() {
                        CatalogItem::Table(_)
                        | CatalogItem::Source(_)
                        | CatalogItem::MaterializedView(_) => {
                            id_bundle.storage_ids.insert(entry.id());
                        }
                        CatalogItem::Index(index) => {
                            id_bundle
                                .compute_ids
                                .entry(index.cluster_id)
                                .or_default()
                                .insert(entry.id());
                        }
                        CatalogItem::View(_)
                        | CatalogItem::Sink(_)
                        | CatalogItem::Type(_)
                        | CatalogItem::Func(_)
                        | CatalogItem::Secret(_)
                        | CatalogItem::Connection(_)
                        | CatalogItem::Log(_) => {}
                    }
                }
            }
        }
        id_bundle
    }

    /// Return an error if the ids are from incompatible timeline contexts. This should
    /// be used to prevent users from doing things that are either meaningless
    /// (joining data from timelines that have similar numbers with different
    /// meanings like two separate debezium topics) or will never complete (joining
    /// cdcv2 and realtime data).
    pub(crate) fn validate_timeline_context<I>(
        &self,
        ids: I,
    ) -> Result<TimelineContext, AdapterError>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let mut timeline_contexts: Vec<_> = self.get_timeline_contexts(ids).into_iter().collect();
        // If there's more than one timeline, we will not produce meaningful
        // data to a user. Take, for example, some realtime source and a debezium
        // consistency topic source. The realtime source uses something close to now
        // for its timestamps. The debezium source starts at 1 and increments per
        // transaction. We don't want to choose some timestamp that is valid for both
        // of these because the debezium source will never get to the same value as the
        // realtime source's "milliseconds since Unix epoch" value. And even if it did,
        // it's not meaningful to join just because those two numbers happen to be the
        // same now.
        //
        // Another example: assume two separate debezium consistency topics. Both
        // start counting at 1 and thus have similarish numbers that probably overlap
        // a lot. However it's still not meaningful to join those two at a specific
        // transaction counter number because those counters are unrelated to the
        // other.
        let timelines: Vec<_> = timeline_contexts
            .drain_filter_swapping(|timeline_context| timeline_context.contains_timeline())
            .collect();

        // A single or group of objects may contain multiple compatible timeline
        // contexts. For example `SELECT *, 1, mz_now() FROM t` will contain all
        // types of contexts. We choose the strongest context level to return back.
        if timelines.len() > 1 {
            Err(AdapterError::Unsupported(
                "multiple timelines within one dataflow",
            ))
        } else if timelines.len() == 1 {
            Ok(timelines.into_element())
        } else if timeline_contexts
            .iter()
            .contains(&TimelineContext::TimestampDependent)
        {
            Ok(TimelineContext::TimestampDependent)
        } else {
            Ok(TimelineContext::TimestampIndependent)
        }
    }

    /// Return the timeline context belonging to a GlobalId, if one exists.
    pub(crate) fn get_timeline_context(&self, id: GlobalId) -> TimelineContext {
        self.validate_timeline_context(vec![id])
            .expect("impossible for a single object to belong to incompatible timeline contexts")
    }

    /// Return the timeline contexts belonging to a list of GlobalIds, if any exist.
    fn get_timeline_contexts<I>(&self, ids: I) -> BTreeSet<TimelineContext>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let mut timelines: BTreeMap<GlobalId, TimelineContext> = BTreeMap::new();
        let mut contains_temporal = false;

        // Recurse through IDs to find all sources and tables, adding new ones to
        // the set until we reach the bottom.
        let mut ids: Vec<_> = ids.into_iter().collect();
        // Helper function for both materialized views and views.
        fn visit_view_expr(
            id: GlobalId,
            optimized_expr: &OptimizedMirRelationExpr,
            ids: &mut Vec<GlobalId>,
            timelines: &mut BTreeMap<GlobalId, TimelineContext>,
            contains_temporal: &mut bool,
        ) {
            if !*contains_temporal && optimized_expr.contains_temporal() {
                *contains_temporal = true;
            }
            let depends_on = optimized_expr.depends_on();
            if depends_on.is_empty() {
                if *contains_temporal {
                    timelines.insert(id, TimelineContext::TimestampDependent);
                } else {
                    timelines.insert(id, TimelineContext::TimestampIndependent);
                }
            } else {
                ids.extend(depends_on);
            }
        }
        while let Some(id) = ids.pop() {
            // Protect against possible infinite recursion. Not sure if it's possible, but
            // a cheap prevention for the future.
            if timelines.contains_key(&id) {
                continue;
            }
            if let Some(entry) = self.catalog.try_get_entry(&id) {
                match entry.item() {
                    CatalogItem::Source(source) => {
                        timelines.insert(
                            id,
                            TimelineContext::TimelineDependent(source.timeline.clone()),
                        );
                    }
                    CatalogItem::Index(index) => {
                        ids.push(index.on);
                    }
                    CatalogItem::View(view) => {
                        visit_view_expr(
                            id,
                            &view.optimized_expr,
                            &mut ids,
                            &mut timelines,
                            &mut contains_temporal,
                        );
                    }
                    CatalogItem::MaterializedView(mview) => {
                        visit_view_expr(
                            id,
                            &mview.optimized_expr,
                            &mut ids,
                            &mut timelines,
                            &mut contains_temporal,
                        );
                    }
                    CatalogItem::Table(table) => {
                        timelines.insert(id, TimelineContext::TimelineDependent(table.timeline()));
                    }
                    CatalogItem::Log(_) => {
                        timelines.insert(
                            id,
                            TimelineContext::TimelineDependent(Timeline::EpochMilliseconds),
                        );
                    }
                    CatalogItem::Sink(_)
                    | CatalogItem::Type(_)
                    | CatalogItem::Func(_)
                    | CatalogItem::Secret(_)
                    | CatalogItem::Connection(_) => {}
                }
            }
        }

        timelines
            .into_iter()
            .map(|(_, timeline)| timeline)
            .collect()
    }

    /// Returns an iterator that partitions an id bundle by the timeline context that each id belongs to.
    pub fn partition_ids_by_timeline_context(
        &self,
        id_bundle: &CollectionIdBundle,
    ) -> impl Iterator<Item = (TimelineContext, CollectionIdBundle)> {
        let mut res: BTreeMap<TimelineContext, CollectionIdBundle> = BTreeMap::new();

        for id in &id_bundle.storage_ids {
            let timeline_context = self.get_timeline_context(*id);
            res.entry(timeline_context)
                .or_default()
                .storage_ids
                .insert(*id);
        }

        for (compute_instance, ids) in &id_bundle.compute_ids {
            for id in ids {
                let timeline_context = self.get_timeline_context(*id);
                res.entry(timeline_context)
                    .or_default()
                    .compute_ids
                    .entry(*compute_instance)
                    .or_default()
                    .insert(*id);
            }
        }

        res.into_iter()
    }

    /// Return the set of ids in a timedomain and verify timeline correctness.
    ///
    /// When a user starts a transaction, we need to prevent compaction of anything
    /// they might read from. We use a heuristic of "anything in the same database
    /// schemas with the same timeline as whatever the first query is".
    pub(crate) fn timedomain_for<'a, I>(
        &self,
        uses_ids: I,
        timeline_context: &TimelineContext,
        conn_id: ConnectionId,
        compute_instance: ComputeInstanceId,
    ) -> Result<CollectionIdBundle, AdapterError>
    where
        I: IntoIterator<Item = &'a GlobalId>,
    {
        // Gather all the used schemas.
        let mut schemas = BTreeSet::new();
        for id in uses_ids {
            let entry = self.catalog.get_entry(id);
            let name = entry.name();
            schemas.insert((&name.qualifiers.database_spec, &name.qualifiers.schema_spec));
        }

        // If any of the system schemas is specified, add the rest of the
        // system schemas.
        let system_schemas = [
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog.get_mz_catalog_schema_id().clone()),
            ),
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog.get_pg_catalog_schema_id().clone()),
            ),
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog.get_information_schema_id().clone()),
            ),
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog.get_mz_internal_schema_id().clone()),
            ),
        ];
        if system_schemas.iter().any(|s| schemas.contains(s)) {
            schemas.extend(system_schemas);
        }

        // Gather the IDs of all items in all used schemas.
        let mut item_ids: BTreeSet<GlobalId> = BTreeSet::new();
        for (db, schema) in schemas {
            let schema = self.catalog.get_schema(db, schema, conn_id);
            item_ids.extend(schema.items.values());
        }

        // Gather the dependencies of those items.
        let mut id_bundle: CollectionIdBundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(item_ids.iter());

        // Filter out ids from different timelines.
        for ids in [
            &mut id_bundle.storage_ids,
            &mut id_bundle.compute_ids.entry(compute_instance).or_default(),
        ] {
            ids.retain(|&id| {
                let id_timeline_context = self
                    .validate_timeline_context(vec![id])
                    .expect("single id should never fail");
                match (&id_timeline_context, &timeline_context) {
                    // If this id doesn't have a timeline, we can keep it.
                    (
                        TimelineContext::TimestampIndependent | TimelineContext::TimestampDependent,
                        _,
                    ) => true,
                    // If there's no source timeline, we have the option to opt into a timeline,
                    // so optimistically choose epoch ms. This is useful when the first query in a
                    // transaction is on a static view.
                    (
                        TimelineContext::TimelineDependent(id_timeline),
                        TimelineContext::TimestampIndependent | TimelineContext::TimestampDependent,
                    ) => id_timeline == &Timeline::EpochMilliseconds,
                    // Otherwise check if timelines are the same.
                    (
                        TimelineContext::TimelineDependent(id_timeline),
                        TimelineContext::TimelineDependent(source_timeline),
                    ) => id_timeline == source_timeline,
                }
            });
        }

        Ok(id_bundle)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn advance_timelines(&mut self) {
        let global_timelines = std::mem::take(&mut self.global_timelines);
        for (
            timeline,
            TimelineState {
                mut oracle,
                mut read_holds,
            },
        ) in global_timelines
        {
            let now = if timeline == Timeline::EpochMilliseconds {
                oracle.read_ts()
            } else {
                // For non realtime sources, we define now as the largest timestamp, not in
                // advance of any object's upper. This is the largest timestamp that is closed
                // to writes.
                let id_bundle = self.ids_in_timeline(&timeline);
                self.largest_not_in_advance_of_upper(&self.least_valid_write(&id_bundle))
            };
            oracle
                .apply_write(now, |ts| self.catalog.persist_timestamp(&timeline, ts))
                .await;
            let read_ts = oracle.read_ts();
            if read_holds.times().any(|time| time.less_than(&read_ts)) {
                read_holds = self.update_read_hold(read_holds, read_ts);
            }
            self.global_timelines
                .insert(timeline, TimelineState { oracle, read_holds });
        }
    }
}
