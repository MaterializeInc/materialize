// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A mechanism to ensure that a sequence of writes and reads proceed correctly through timestamps.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use mz_adapter_types::connection::ConnectionId;
use mz_compute_types::ComputeInstanceId;
use mz_expr::{CollectionPlan, OptimizedMirRelationExpr};
use mz_ore::collections::CollectionExt;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_ore::vec::VecExt;
use mz_repr::{GlobalId, Timestamp};
use mz_sql::names::{ResolvedDatabaseSpecifier, SchemaSpecifier};
use mz_storage_types::sources::Timeline;
use timely::progress::Timestamp as TimelyTimestamp;
use tracing::{debug, error};

use crate::catalog::CatalogItem;
use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::read_policy::ReadHolds;
use crate::coord::timestamp_oracle::catalog_oracle::{
    CatalogTimestampOracle, CatalogTimestampPersistence, TimestampPersistence,
    TIMESTAMP_INTERVAL_UPPER_BOUND, TIMESTAMP_PERSIST_INTERVAL,
};
use crate::coord::timestamp_oracle::{self, TimestampOracle};
use crate::coord::timestamp_selection::TimestampProvider;
use crate::coord::Coordinator;
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
    pub(crate) oracle: Box<dyn TimestampOracle<T>>,
    pub(crate) read_holds: ReadHolds<T>,
}

impl<T: fmt::Debug> fmt::Debug for TimelineState<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimelineState")
            .field("read_holds", &self.read_holds)
            .finish()
    }
}

impl Coordinator {
    pub(crate) fn now(&self) -> EpochMillis {
        (self.catalog().config().now)()
    }

    pub(crate) fn now_datetime(&self) -> DateTime<Utc> {
        to_datetime(self.now())
    }

    pub(crate) fn get_timestamp_oracle(
        &self,
        timeline: &Timeline,
    ) -> &dyn TimestampOracle<Timestamp> {
        self.global_timelines
            .get(timeline)
            .expect("all timelines have a timestamp oracle")
            .oracle
            .as_ref()
    }

    /// Returns a reference to the timestamp oracle used for reads and writes
    /// from/to a local input.
    fn get_local_timestamp_oracle(&self) -> &dyn TimestampOracle<Timestamp> {
        self.get_timestamp_oracle(&Timeline::EpochMilliseconds)
    }

    /// Assign a timestamp for a read from a local input. Reads following writes
    /// must be at a time >= the write's timestamp; we choose "equal to" for
    /// simplicity's sake and to open as few new timestamps as possible.
    pub(crate) async fn get_local_read_ts(&self) -> Timestamp {
        self.get_local_timestamp_oracle().read_ts().await
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
            .write_ts()
            .await
    }

    /// Peek the current timestamp used for operations on local inputs. Used to determine how much
    /// to block group commits by.
    pub(crate) async fn peek_local_write_ts(&self) -> Timestamp {
        self.get_local_timestamp_oracle().peek_write_ts().await
    }

    /// Peek the current timestamp used for operations on local inputs. Used to determine how much
    /// to block group commits by.
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn apply_local_write(&mut self, timestamp: Timestamp) {
        let now = self.now().into();

        if timestamp > timestamp_oracle::catalog_oracle::upper_bound(&now) {
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
            .apply_write(timestamp)
            .await;
    }

    /// Ensures that a global timeline state exists for `timeline`.
    pub(crate) async fn ensure_timeline_state<'a>(
        &'a mut self,
        timeline: &'a Timeline,
    ) -> &mut TimelineState<Timestamp> {
        let catalog = Arc::clone(&self.catalog);
        let timestamp_persistence = CatalogTimestampPersistence::new(timeline.clone(), catalog);

        Self::ensure_timeline_state_with_initial_time(
            timeline,
            Timestamp::minimum(),
            self.catalog().config().now.clone(),
            timestamp_persistence,
            &mut self.global_timelines,
        )
        .await
    }

    /// Ensures that a global timeline state exists for `timeline`, with an initial time
    /// of `initially`.
    pub(crate) async fn ensure_timeline_state_with_initial_time<'a, P>(
        timeline: &'a Timeline,
        initially: Timestamp,
        now: NowFn,
        timestamp_persistence: P,
        global_timelines: &'a mut BTreeMap<Timeline, TimelineState<Timestamp>>,
    ) -> &'a mut TimelineState<Timestamp>
    where
        P: TimestampPersistence<mz_repr::Timestamp> + 'static,
    {
        if !global_timelines.contains_key(timeline) {
            let now_fn = if timeline == &Timeline::EpochMilliseconds {
                now
            } else {
                // Timelines that are not `EpochMilliseconds` don't have an
                // "external" clock that wants to drive forward timestamps in
                // addition to the rule that write timestamps must be strictly
                // monotonically increasing.
                //
                // Passing in a clock that always yields the minimum takes the
                // clock out of the equation and makes timestamps advance only
                // by the rule about strict monotonicity mentioned above.
                NowFn::from(|| Timestamp::minimum().into())
            };

            let oracle: Box<dyn TimestampOracle<mz_repr::Timestamp>> = Box::new(
                CatalogTimestampOracle::new(
                    initially,
                    now_fn,
                    *TIMESTAMP_PERSIST_INTERVAL,
                    timestamp_persistence,
                )
                .await,
            );

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

    /// Groups together storage and compute resources into a [`CollectionIdBundle`]
    pub(crate) fn build_collection_id_bundle(
        &self,
        storage_ids: impl IntoIterator<Item = GlobalId>,
        compute_ids: impl IntoIterator<Item = (ComputeInstanceId, GlobalId)>,
        clusters: impl IntoIterator<Item = ComputeInstanceId>,
    ) -> CollectionIdBundle {
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
                compute.entry(*instance_id).or_default().extend(ids);
            }
        }

        CollectionIdBundle {
            storage_ids: storage_ids.into_iter().collect(),
            compute_ids: compute,
        }
    }

    /// Given a [`Timeline`] and a [`CollectionIdBundle`], removes all of the "storage ids"
    /// and "compute ids" in the bundle, from the timeline.
    pub(crate) fn remove_resources_associated_with_timeline(
        &mut self,
        timeline: Timeline,
        ids: CollectionIdBundle,
    ) -> bool {
        let TimelineState { read_holds, .. } = self
            .global_timelines
            .get_mut(&timeline)
            .expect("all timeslines have a timestamp oracle");

        // Remove all of the underlying resources.
        for id in ids.storage_ids {
            read_holds.remove_storage_id(&id);
        }
        for (compute_id, ids) in ids.compute_ids {
            for id in ids {
                read_holds.remove_compute_id(&compute_id, &id);
            }
        }
        let became_empty = read_holds.is_empty();

        became_empty
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
        for entry in self.catalog().entries() {
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
                // If the definition contains a temporal function, the timeline must
                // be timestamp dependent.
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
            if let Some(entry) = self.catalog().try_get_entry(&id) {
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
        conn_id: &ConnectionId,
        compute_instance: ComputeInstanceId,
    ) -> Result<CollectionIdBundle, AdapterError>
    where
        I: IntoIterator<Item = &'a GlobalId>,
    {
        // Gather all the used schemas.
        let mut schemas = BTreeSet::new();
        for id in uses_ids {
            let entry = self.catalog().get_entry(id);
            let name = entry.name();
            schemas.insert((&name.qualifiers.database_spec, &name.qualifiers.schema_spec));
        }

        let pg_catalog_schema = (
            &ResolvedDatabaseSpecifier::Ambient,
            &SchemaSpecifier::Id(self.catalog().get_pg_catalog_schema_id().clone()),
        );
        let system_schemas = [
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog().get_mz_catalog_schema_id().clone()),
            ),
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog().get_mz_internal_schema_id().clone()),
            ),
            pg_catalog_schema.clone(),
            (
                &ResolvedDatabaseSpecifier::Ambient,
                &SchemaSpecifier::Id(self.catalog().get_information_schema_id().clone()),
            ),
        ];

        if system_schemas.iter().any(|s| schemas.contains(s)) {
            // If any of the system schemas is specified, add the rest of the
            // system schemas.
            schemas.extend(system_schemas);
        } else if !schemas.is_empty() {
            // Always include the pg_catalog schema, if schemas is non-empty. The pg_catalog schemas is
            // sometimes used by applications in followup queries.
            schemas.insert(pg_catalog_schema);
        }

        // Gather the IDs of all items in all used schemas.
        let mut item_ids: BTreeSet<GlobalId> = BTreeSet::new();
        for (db, schema) in schemas {
            let schema = self.catalog().get_schema(db, schema, conn_id);
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
            // Timeline::EpochMilliseconds is advanced in group commits and doesn't need to be
            // manually advanced here.
            if timeline != Timeline::EpochMilliseconds {
                // For non realtime sources, we define now as the largest timestamp, not in
                // advance of any object's upper. This is the largest timestamp that is closed
                // to writes.
                let id_bundle = self.ids_in_timeline(&timeline);

                // Advance the timeline if-and-only-if there are objects in it.
                // Otherwise we'd advance to the empty frontier, meaning we
                // close it off for ever.
                if !id_bundle.is_empty() {
                    let least_valid_write = self.least_valid_write(&id_bundle);
                    let now = Self::largest_not_in_advance_of_upper(&least_valid_write);
                    oracle.apply_write(now).await;
                    debug!(
                        least_valid_write = ?least_valid_write,
                        oracle_read_ts = ?oracle.read_ts().await,
                        "advanced {:?} to {}",
                        timeline,
                        now,
                    );
                }
            };
            let read_ts = oracle.read_ts().await;
            if read_holds.times().any(|time| time.less_than(&read_ts)) {
                read_holds = self.update_read_hold(read_holds, read_ts);
            }
            self.global_timelines
                .insert(timeline, TimelineState { oracle, read_holds });
        }
    }
}
