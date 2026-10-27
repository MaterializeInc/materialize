// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Publication of original storage health transitions by their replica.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use mz_catalog::catalog::Catalog;
use mz_controller_types::ReplicaId;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage::server::StatusEvent;
use mz_storage_client::client::TimestamplessUpdate;
use mz_storage_client::collection_writer::monotonic_append;
use mz_storage_client::controller::IntrospectionType;
use tokio::sync::mpsc;

/// Queues transitions independently of query connections. Replica execution
/// teardown can discard unpublished history.
pub(super) struct History {
    tx: mpsc::UnboundedSender<Publication>,
    _task: AbortOnDropHandle<()>,
}

enum Publication {
    Transition { event: StatusEvent, current: bool },
    RetireExecution(u64),
    CompletedSink { id: GlobalId, present: bool },
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum CurrentOwner {
    Execution(u64),
    // Catalog-following observes terminal output without starting an execution.
    CompletedSinks,
}

impl History {
    pub fn new(
        catalog: &Catalog,
        persist: PersistClient,
        replica: ReplicaId,
        deployment: u64,
        incarnation: u64,
    ) -> anyhow::Result<Self> {
        let mut bindings = BTreeMap::new();
        for (kind, builtin) in [
            (
                IntrospectionType::SourceStatusHistory,
                &*mz_catalog::builtin::MZ_SOURCE_STATUS_HISTORY,
            ),
            (
                IntrospectionType::SinkStatusHistory,
                &*mz_catalog::builtin::MZ_SINK_STATUS_HISTORY,
            ),
            (
                IntrospectionType::StorageStatuses,
                &*mz_catalog::builtin::MZ_STORAGE_STATUSES_RAW,
            ),
        ] {
            let item = catalog.resolve_builtin_storage_collection(builtin);
            let id = catalog.get_entry(&item).latest_global_id();
            let shard = catalog
                .state()
                .storage_metadata()
                .get_collection_shard(id)
                .context("status history requires committed shard metadata")?;
            bindings.insert(kind, (id, shard, builtin.desc.clone()));
        }
        let now = catalog.config().now.clone();
        Ok(Self::spawn(
            bindings,
            persist,
            replica,
            deployment,
            incarnation,
            now,
        ))
    }

    fn spawn(
        mut bindings: BTreeMap<IntrospectionType, (GlobalId, ShardId, RelationDesc)>,
        persist: PersistClient,
        replica: ReplicaId,
        deployment: u64,
        incarnation: u64,
        now: mz_ore::now::NowFn,
    ) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<Publication>();
        let task = mz_ore::task::spawn(|| "replica-status-history", async move {
            let mut writers = BTreeMap::new();
            let mut events = Vec::new();
            let mut current_rows = BTreeMap::<CurrentOwner, BTreeMap<GlobalId, Row>>::new();
            while rx.recv_many(&mut events, 1024).await != 0 {
                let mut batches: BTreeMap<_, Vec<TimestamplessUpdate>> = BTreeMap::new();
                for publication in events.drain(..) {
                    match publication {
                        Publication::Transition { event, current } => {
                            let mut update = event.update;
                            let id = update.id;
                            update.replica_id = Some(replica);
                            let row: Row = update.into();
                            let row = Row::pack(row.iter().chain([Datum::UInt64(deployment)]));
                            batches
                                .entry(event.history)
                                .or_default()
                                .push(TimestamplessUpdate {
                                    row: row.clone(),
                                    diff: Diff::ONE,
                                });
                            if current {
                                let row = Row::pack(row.iter().chain([Datum::UInt64(incarnation)]));
                                let execution = event
                                    .execution
                                    .expect("current health has an admitted execution");
                                let updates = batches
                                    .entry(IntrospectionType::StorageStatuses)
                                    .or_default();
                                if let Some(previous) = current_rows
                                    .entry(CurrentOwner::Execution(execution))
                                    .or_default()
                                    .insert(id, row.clone())
                                {
                                    updates.push(TimestamplessUpdate {
                                        row: previous,
                                        diff: -Diff::ONE,
                                    });
                                }
                                updates.push(TimestamplessUpdate {
                                    row,
                                    diff: Diff::ONE,
                                });
                            }
                        }
                        Publication::RetireExecution(execution) => {
                            for row in current_rows
                                .remove(&CurrentOwner::Execution(execution))
                                .into_iter()
                                .flat_map(BTreeMap::into_values)
                            {
                                batches
                                    .entry(IntrospectionType::StorageStatuses)
                                    .or_default()
                                    .push(TimestamplessUpdate {
                                        row,
                                        diff: -Diff::ONE,
                                    });
                            }
                        }
                        Publication::CompletedSink { id, present } => {
                            let rows = current_rows
                                .entry(CurrentOwner::CompletedSinks)
                                .or_default();
                            let update = if present {
                                if rows.contains_key(&id) {
                                    continue;
                                }
                                let row = Row::pack([
                                    Datum::Null,
                                    Datum::String(&id.to_string()),
                                    Datum::String("running"),
                                    Datum::Null,
                                    Datum::Null,
                                    Datum::String(&replica.to_string()),
                                    Datum::UInt64(deployment),
                                    Datum::UInt64(incarnation),
                                ]);
                                rows.insert(id, row.clone());
                                Some(TimestamplessUpdate {
                                    row,
                                    diff: Diff::ONE,
                                })
                            } else {
                                rows.remove(&id).map(|row| TimestamplessUpdate {
                                    row,
                                    diff: -Diff::ONE,
                                })
                            };
                            if let Some(update) = update {
                                batches
                                    .entry(IntrospectionType::StorageStatuses)
                                    .or_default()
                                    .push(update);
                            }
                        }
                    }
                }
                for (kind, updates) in batches {
                    if let std::collections::btree_map::Entry::Vacant(entry) = writers.entry(kind) {
                        let (id, shard, desc) =
                            bindings.remove(&kind).expect("storage status publication");
                        let writer = persist
                            .open_writer(
                                shard,
                                Arc::new(desc),
                                Arc::new(UnitSchema),
                                Diagnostics {
                                    shard_name: id.to_string(),
                                    handle_purpose: "replica status history".into(),
                                },
                            )
                            .await
                            .expect("status history schema matches catalog");
                        entry.insert(writer);
                    }
                    monotonic_append(
                        writers.get_mut(&kind).expect("opened history writer"),
                        updates,
                        Timestamp::from(now()),
                    )
                    .await;
                }
            }
        })
        .abort_on_drop();
        Self { tx, _task: task }
    }

    /// The runtime owner retires all of an attempt's health before installing a
    /// replacement. Worker-local drops cannot retract another worker's successor.
    pub fn retire_execution(&self, execution: u64) {
        self.tx
            .send(Publication::RetireExecution(execution))
            .expect("status publisher is alive");
    }

    pub fn record(&self, event: StatusEvent, current: bool) {
        self.tx
            .send(Publication::Transition { event, current })
            .expect("replica status history writer is alive");
    }

    /// Publishes only current health for a desired sink whose Persist output upper
    /// is empty. A recovery requirement or compaction permission alone is not proof.
    /// The catalog-following owner retracts this observation when placement or the
    /// definition changes. Neither operation manufactures a historical transition.
    pub fn completed_sink(&self, id: GlobalId, present: bool) {
        self.tx
            .send(Publication::CompletedSink { id, present })
            .expect("replica status history writer is alive");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_persist_client::read::ListenEvent;
    use mz_storage_client::client::{Status, StatusUpdate};
    use mz_storage_client::healthcheck::{
        MZ_SINK_STATUS_HISTORY_DESC, MZ_SOURCE_STATUS_HISTORY_DESC, MZ_STORAGE_STATUSES_RAW_DESC,
    };
    use mz_storage_types::sources::SourceData;
    use timely::progress::Antichain;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn publishes_original_source_and_sink_transitions_without_observers() {
        let persist = PersistClient::new_for_tests().await;
        let replica = ReplicaId::User(9);
        let destinations = [
            (
                IntrospectionType::SourceStatusHistory,
                MZ_SOURCE_STATUS_HISTORY_DESC.clone(),
            ),
            (
                IntrospectionType::SinkStatusHistory,
                MZ_SINK_STATUS_HISTORY_DESC.clone(),
            ),
        ];
        let mut bindings = BTreeMap::new();
        let mut readers = Vec::new();
        for (index, (kind, desc)) in destinations.into_iter().enumerate() {
            let id = GlobalId::User(u64::try_from(index).unwrap() + 1);
            let shard = ShardId::new();
            let reader = persist
                .open_leased_reader::<SourceData, (), Timestamp, i64>(
                    shard,
                    Arc::new(desc.clone()),
                    Arc::new(UnitSchema),
                    Diagnostics::for_tests(),
                    false,
                )
                .await
                .unwrap();
            bindings.insert(kind, (id, shard, desc));
            readers.push((kind, id, reader));
        }
        let history = History::spawn(
            bindings,
            persist,
            replica,
            7,
            42,
            mz_ore::now::NowFn::from(|| 100),
        );
        let occurred_at = mz_ore::now::to_datetime(12);
        for (kind, id, _) in &readers {
            for status in [Status::Starting, Status::Running] {
                history.record(
                    StatusEvent {
                        owner: *id,
                        execution: Some(3),
                        history: *kind,
                        update: StatusUpdate::new(*id, occurred_at, status),
                    },
                    false,
                );
            }
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            for (_, id, reader) in readers {
                let mut subscription = reader
                    .subscribe(Antichain::from_elem(Timestamp::MIN))
                    .await
                    .unwrap();
                let mut statuses = Vec::new();
                while statuses.len() < 2 {
                    for event in subscription.fetch_next().await {
                        if let ListenEvent::Updates(updates) = event {
                            for ((data, ()), _, diff) in updates {
                                assert_eq!(diff, 1);
                                let row = data.0.unwrap();
                                let values = row.unpack();
                                assert_eq!(
                                    values[0],
                                    Datum::TimestampTz(occurred_at.try_into().unwrap())
                                );
                                assert_eq!(values[1].unwrap_str(), id.to_string());
                                assert_eq!(values[5].unwrap_str(), replica.to_string());
                                assert_eq!(values[6], Datum::UInt64(7));
                                statuses.push(values[2].unwrap_str().to_owned());
                            }
                        }
                    }
                }
                statuses.sort();
                assert_eq!(statuses, ["running", "starting"]);
            }
        })
        .await
        .expect("source and sink history must publish without query observers");
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn current_health_replaces_rows_and_ignores_late_predecessor_events() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let persist = PersistClient::new_for_tests().await;
            let mut bindings = BTreeMap::new();
            let mut readers = BTreeMap::new();
            for (kind, desc) in [
                (
                    IntrospectionType::SinkStatusHistory,
                    MZ_SINK_STATUS_HISTORY_DESC.clone(),
                ),
                (
                    IntrospectionType::StorageStatuses,
                    MZ_STORAGE_STATUSES_RAW_DESC.clone(),
                ),
            ] {
                let shard = ShardId::new();
                let reader = persist
                    .open_leased_reader::<SourceData, (), Timestamp, i64>(
                        shard,
                        Arc::new(desc.clone()),
                        Arc::new(UnitSchema),
                        Diagnostics::for_tests(),
                        false,
                    )
                    .await
                    .unwrap();
                bindings.insert(kind, (GlobalId::System(1), shard, desc));
                readers.insert(kind, reader);
            }
            let publisher = History::spawn(
                bindings,
                persist,
                ReplicaId::User(9),
                7,
                42,
                mz_ore::now::NowFn::from(|| 100),
            );
            let event = |id, execution, at, status| StatusEvent {
                owner: id,
                execution: Some(execution),
                history: IntrospectionType::SinkStatusHistory,
                update: StatusUpdate::new(id, mz_ore::now::to_datetime(at), status),
            };
            let target = GlobalId::User(1);
            let witness = GlobalId::User(2);
            publisher.record(event(target, 1, 10, Status::Starting), true);
            publisher.retire_execution(1);
            publisher.record(event(target, 2, 20, Status::Running), true);
            // A predecessor can have a later wall-clock timestamp. Its original
            // transition belongs in history, but cannot replace current health.
            publisher.record(event(target, 1, 30, Status::Starting), false);
            publisher.retire_execution(1);
            let completed = GlobalId::User(3);
            publisher.record(event(completed, 4, 5, Status::Running), true);
            publisher.retire_execution(4);
            publisher.completed_sink(completed, true);
            publisher.completed_sink(completed, true);
            publisher.retire_execution(4);
            publisher.record(event(witness, 3, 40, Status::Starting), true);

            let mut current = readers
                .remove(&IntrospectionType::StorageStatuses)
                .unwrap()
                .subscribe(Antichain::from_elem(Timestamp::MIN))
                .await
                .unwrap();
            let mut rows = BTreeMap::<Row, i64>::new();
            for witness_status in ["starting", "running"] {
                // The witness travels through the same publication queue. Seeing
                // it establishes completion of the preceding changes without a
                // timer or a test-only acknowledgement path.
                loop {
                    for event in current.fetch_next().await {
                        if let ListenEvent::Updates(updates) = event {
                            for ((data, ()), _, diff) in updates {
                                *rows.entry(data.0.unwrap()).or_default() += diff;
                            }
                        }
                    }
                    rows.retain(|_, diff| *diff != 0);
                    if rows.keys().any(|row| {
                        let values = row.unpack();
                        values[1].unwrap_str() == witness.to_string()
                            && values[2].unwrap_str() == witness_status
                    }) {
                        break;
                    }
                }
                assert!(rows.values().all(|diff| *diff == 1));
                let target_rows: Vec<_> = rows
                    .keys()
                    .filter(|row| row.unpack()[1].unwrap_str() == target.to_string())
                    .collect();
                if witness_status == "starting" {
                    let completed_rows: Vec<_> = rows
                        .keys()
                        .filter(|row| row.unpack()[1].unwrap_str() == completed.to_string())
                        .collect();
                    assert_eq!(completed_rows.len(), 1);
                    let values = completed_rows[0].unpack();
                    assert_eq!(values[0], Datum::Null);
                    assert_eq!(values[2].unwrap_str(), "running");
                    assert_eq!(values[6], Datum::UInt64(7));
                    assert_eq!(values[7], Datum::UInt64(42));
                    assert_eq!(target_rows.len(), 1);
                    let values = target_rows[0].unpack();
                    assert_eq!(values[2].unwrap_str(), "running");
                    assert_eq!(values[7], Datum::UInt64(42));
                    assert_eq!(
                        values[0],
                        Datum::TimestampTz(mz_ore::now::to_datetime(20).try_into().unwrap())
                    );
                    publisher.retire_execution(2);
                    publisher.completed_sink(completed, false);
                    publisher.record(event(witness, 3, 50, Status::Running), true);
                } else {
                    assert!(target_rows.is_empty());
                    assert_eq!(rows.len(), 1, "retirement must preserve other objects");
                }
            }
            let mut history = readers
                .remove(&IntrospectionType::SinkStatusHistory)
                .unwrap()
                .subscribe(Antichain::from_elem(Timestamp::MIN))
                .await
                .unwrap();
            let mut historical_rows = Vec::new();
            while historical_rows.len() < 6 {
                for event in history.fetch_next().await {
                    if let ListenEvent::Updates(updates) = event {
                        for ((data, ()), _, diff) in updates {
                            assert_eq!(diff, 1);
                            historical_rows.push(data.0.unwrap());
                        }
                    }
                }
            }
            assert_eq!(historical_rows.len(), 6);
            assert!(historical_rows.iter().any(|row| {
                let values = row.unpack();
                values[1].unwrap_str() == target.to_string()
                    && values[0]
                        == Datum::TimestampTz(mz_ore::now::to_datetime(30).try_into().unwrap())
            }));
        })
        .await
        .expect("current health and original history must publish");
    }
}
