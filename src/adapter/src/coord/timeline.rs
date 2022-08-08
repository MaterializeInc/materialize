// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A mechanism to ensure that a sequence of writes and reads proceed correctly through timestamps.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::time::Duration;

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use timely::progress::Timestamp as TimelyTimestamp;

use mz_compute_client::controller::ComputeInstanceId;
use mz_expr::CollectionPlan;
use mz_ore::now::{to_datetime, EpochMillis};
use mz_repr::{GlobalId, Timestamp};
use mz_sql::names::{ResolvedDatabaseSpecifier, SchemaSpecifier};
use mz_stash::Append;
use mz_storage::types::sources::Timeline;

use crate::catalog::CatalogItem;
use crate::client::ConnectionId;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::read_policy::ReadHolds;
use crate::coord::CoordTimestamp;
use crate::coord::Coordinator;
use crate::AdapterError;

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

impl<T: CoordTimestamp> TimestampOracle<T> {
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
    fn read_ts(&mut self) -> T {
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
    Duration::from_secs(15)
        .as_millis()
        .try_into()
        .expect("15 seconds can fit into `Timestamp`")
});

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

impl<T: CoordTimestamp> DurableTimestampOracle<T> {
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
    pub fn read_ts(&mut self) -> T {
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
                .expect("can't persist timestamp");
        }
    }
}

impl<S: Append + 'static> Coordinator<S> {
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

    /// Returns a reference to the timestamp oracle used for reads and writes
    /// from/to a local input.
    fn get_local_timestamp_oracle(&self) -> &DurableTimestampOracle<Timestamp> {
        &self
            .global_timelines
            .get(&Timeline::EpochMilliseconds)
            .expect("no realtime timeline")
            .oracle
    }

    /// Returns a mutable reference to the timestamp oracle used for reads and writes
    /// from/to a local input.
    pub(crate) fn get_local_timestamp_oracle_mut(
        &mut self,
    ) -> &mut DurableTimestampOracle<Timestamp> {
        self.get_timestamp_oracle_mut(&Timeline::EpochMilliseconds)
    }

    /// Assign a timestamp for a read from a local input. Reads following writes
    /// must be at a time >= the write's timestamp; we choose "equal to" for
    /// simplicity's sake and to open as few new timestamps as possible.
    pub(crate) fn get_local_read_ts(&mut self) -> Timestamp {
        self.get_local_timestamp_oracle_mut().read_ts()
    }

    /// Assign a timestamp for a write to a local input and increase the local ts.
    /// Writes following reads must ensure that they are assigned a strictly larger
    /// timestamp to ensure they are not visible to any real-time earlier reads.
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
    pub(crate) fn peek_local_write_ts(&mut self) -> Timestamp {
        self.get_local_timestamp_oracle().peek_write_ts()
    }

    /// Peek the current timestamp used for operations on local inputs. Used to determine how much
    /// to block group commits by.
    pub(crate) async fn apply_local_write(&mut self, timestamp: Timestamp) {
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
    pub(crate) async fn ensure_timeline_state(
        &mut self,
        timeline: Timeline,
    ) -> &mut TimelineState<Timestamp> {
        if !self.global_timelines.contains_key(&timeline) {
            self.global_timelines.insert(
                timeline.clone(),
                TimelineState {
                    oracle: DurableTimestampOracle::new(
                        Timestamp::minimum(),
                        Timestamp::minimum,
                        *TIMESTAMP_PERSIST_INTERVAL,
                        |ts| self.catalog.persist_timestamp(&timeline, ts),
                    )
                    .await,
                    read_holds: ReadHolds::new(Timestamp::minimum()),
                },
            );
        }
        self.global_timelines
            .get_mut(&timeline)
            .expect("inserted above")
    }

    pub(crate) fn remove_storage_ids_from_timeline<I>(&mut self, ids: I) -> Vec<Timeline>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let mut empty_timelines = Vec::new();
        for id in ids {
            if let Some(timeline) = self.get_timeline(id) {
                let TimelineState { read_holds, .. } = self
                    .global_timelines
                    .get_mut(&timeline)
                    .expect("all timelines have a timestamp oracle");
                read_holds.id_bundle.storage_ids.remove(&id);
                if read_holds.id_bundle.is_empty() {
                    self.global_timelines.remove(&timeline);
                    empty_timelines.push(timeline);
                }
            }
        }
        empty_timelines
    }

    pub(crate) fn remove_compute_ids_from_timeline<I>(&mut self, ids: I) -> Vec<Timeline>
    where
        I: IntoIterator<Item = (ComputeInstanceId, GlobalId)>,
    {
        let mut empty_timelines = Vec::new();
        for (compute_instance, id) in ids {
            if let Some(timeline) = self.get_timeline(id) {
                let TimelineState { read_holds, .. } = self
                    .global_timelines
                    .get_mut(&timeline)
                    .expect("all timelines have a timestamp oracle");
                if let Some(ids) = read_holds.id_bundle.compute_ids.get_mut(&compute_instance) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        read_holds.id_bundle.compute_ids.remove(&compute_instance);
                    }
                    if read_holds.id_bundle.is_empty() {
                        self.global_timelines.remove(&timeline);
                        empty_timelines.push(timeline);
                    }
                }
            }
        }
        empty_timelines
    }

    pub(crate) fn remove_compute_instance_from_timeline(
        &mut self,
        compute_instance: ComputeInstanceId,
    ) -> Vec<Timeline> {
        let mut empty_timelines = Vec::new();
        for (timeline, TimelineState { read_holds, .. }) in &mut self.global_timelines {
            read_holds.id_bundle.compute_ids.remove(&compute_instance);
            if read_holds.id_bundle.is_empty() {
                empty_timelines.push(timeline.clone());
            }
        }

        for timeline in &empty_timelines {
            self.global_timelines.remove(timeline);
        }

        empty_timelines
    }

    pub(crate) fn ids_in_timeline(&self, timeline: &Timeline) -> CollectionIdBundle {
        let mut id_bundle = CollectionIdBundle::default();
        for entry in self.catalog.entries() {
            if let Some(entry_timeline) = self.get_timeline(entry.id()) {
                if timeline == &entry_timeline {
                    match entry.item() {
                        CatalogItem::Table(_)
                        | CatalogItem::Source(_)
                        | CatalogItem::MaterializedView(_)
                        | CatalogItem::StorageCollection(_) => {
                            id_bundle.storage_ids.insert(entry.id());
                        }
                        CatalogItem::Index(index) => {
                            id_bundle
                                .compute_ids
                                .entry(index.compute_instance)
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

    /// Return an error if the ids are from incompatible timelines. This should
    /// be used to prevent users from doing things that are either meaningless
    /// (joining data from timelines that have similar numbers with different
    /// meanings like two separate debezium topics) or will never complete (joining
    /// cdcv2 and realtime data).
    pub(crate) fn validate_timeline<I>(&self, ids: I) -> Result<Option<Timeline>, AdapterError>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let timelines = self.get_timelines(ids);
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
        if timelines.len() > 1 {
            return Err(AdapterError::Unsupported(
                "multiple timelines within one dataflow",
            ));
        }
        Ok(timelines.into_iter().next())
    }

    /// Return the timeline belonging to a GlobalId, if one exists.
    pub(crate) fn get_timeline(&self, id: GlobalId) -> Option<Timeline> {
        let timelines = self.get_timelines(vec![id]);
        assert!(
            timelines.len() <= 1,
            "impossible for a single object to belong to two timelines"
        );
        timelines.into_iter().next()
    }

    /// Return the timelines belonging to a list of GlobalIds, if any exist.
    fn get_timelines<I>(&self, ids: I) -> HashSet<Timeline>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let mut timelines: HashMap<GlobalId, Timeline> = HashMap::new();

        // Recurse through IDs to find all sources and tables, adding new ones to
        // the set until we reach the bottom. Static views will end up with an empty
        // timelines.
        let mut ids: Vec<_> = ids.into_iter().collect();
        while let Some(id) = ids.pop() {
            // Protect against possible infinite recursion. Not sure if it's possible, but
            // a cheap prevention for the future.
            if timelines.contains_key(&id) {
                continue;
            }
            if let Some(entry) = self.catalog.try_get_entry(&id) {
                match entry.item() {
                    CatalogItem::Source(source) => {
                        timelines.insert(id, source.timeline.clone());
                    }
                    CatalogItem::Index(index) => {
                        ids.push(index.on);
                    }
                    CatalogItem::View(view) => {
                        ids.extend(view.optimized_expr.depends_on());
                    }
                    CatalogItem::MaterializedView(mview) => {
                        ids.extend(mview.optimized_expr.depends_on());
                    }
                    CatalogItem::Table(table) => {
                        timelines.insert(id, table.timeline());
                    }
                    CatalogItem::Log(_) => {
                        timelines.insert(id, Timeline::EpochMilliseconds);
                    }
                    CatalogItem::StorageCollection(_) => {
                        timelines.insert(id, Timeline::EpochMilliseconds);
                    }
                    _ => {}
                }
            }
        }

        timelines
            .into_iter()
            .map(|(_, timeline)| timeline)
            .collect()
    }

    /// Return the set of ids in a timedomain and verify timeline correctness.
    ///
    /// When a user starts a transaction, we need to prevent compaction of anything
    /// they might read from. We use a heuristic of "anything in the same database
    /// schemas with the same timeline as whatever the first query is".
    pub(crate) fn timedomain_for<'a, I>(
        &self,
        uses_ids: I,
        timeline: &Option<Timeline>,
        conn_id: ConnectionId,
        compute_instance: ComputeInstanceId,
    ) -> Result<CollectionIdBundle, AdapterError>
    where
        I: IntoIterator<Item = &'a GlobalId>,
    {
        // Gather all the used schemas.
        let mut schemas = HashSet::new();
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
        ];
        if system_schemas.iter().any(|s| schemas.contains(s)) {
            schemas.extend(system_schemas);
        }

        // Gather the IDs of all items in all used schemas.
        let mut item_ids: HashSet<GlobalId> = HashSet::new();
        for (db, schema) in schemas {
            let schema = self.catalog.get_schema(&db, &schema, conn_id);
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
                let id_timeline = self
                    .validate_timeline(vec![id])
                    .expect("single id should never fail");
                match (&id_timeline, &timeline) {
                    // If this id doesn't have a timeline, we can keep it.
                    (None, _) => true,
                    // If there's no source timeline, we have the option to opt into a timeline,
                    // so optimistically choose epoch ms. This is useful when the first query in a
                    // transaction is on a static view.
                    (Some(id_timeline), None) => id_timeline == &Timeline::EpochMilliseconds,
                    // Otherwise check if timelines are the same.
                    (Some(id_timeline), Some(source_timeline)) => id_timeline == source_timeline,
                }
            });
        }

        Ok(id_bundle)
    }
}
