// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Replica-owned compute frontier observations, independent of query connections.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use anyhow::Context;
use mz_catalog::catalog::Catalog;
use mz_compute_client::protocol::response::FrontiersResponse;
use mz_controller_types::ReplicaId;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, Diff, GlobalId, RelationDesc, Row, Timestamp};
use mz_storage_client::client::TimestamplessUpdate;
use mz_storage_client::collection_writer::monotonic_append;
use tokio::sync::mpsc;

/// One incarnation publishes its aggregated compute observations independently
/// of query connections. Catalog ownership filters superseded incarnations.
pub(super) struct Frontiers {
    tx: mpsc::UnboundedSender<(GlobalId, Option<Row>)>,
    replica: ReplicaId,
    deployment: u64,
    incarnation: u64,
    _task: AbortOnDropHandle<()>,
}

impl Frontiers {
    pub fn new(
        catalog: &Catalog,
        persist: PersistClient,
        replica: ReplicaId,
        deployment: u64,
        incarnation: u64,
    ) -> anyhow::Result<Self> {
        let builtin = &*mz_catalog::builtin::MZ_REPLICA_FRONTIER_OBSERVATIONS;
        let item = catalog.resolve_builtin_storage_collection(builtin);
        let id = catalog.get_entry(&item).latest_global_id();
        let shard = catalog
            .state()
            .storage_metadata()
            .get_collection_shard(id)
            .context("frontier observations require committed storage metadata")?;
        let desc = builtin.desc.clone();
        let now = catalog.config().now.clone();
        Ok(Self::spawn(
            persist,
            (id, shard, desc),
            now,
            replica,
            deployment,
            incarnation,
        ))
    }

    fn spawn(
        persist: PersistClient,
        (id, shard, desc): (GlobalId, ShardId, RelationDesc),
        now: mz_ore::now::NowFn,
        replica: ReplicaId,
        deployment: u64,
        incarnation: u64,
    ) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<(GlobalId, Option<Row>)>();
        let task = mz_ore::task::spawn(|| "replica-frontier-observations", async move {
            let mut writer = persist
                .open_writer(
                    shard,
                    Arc::new(desc),
                    Arc::new(UnitSchema),
                    Diagnostics {
                        shard_name: id.to_string(),
                        handle_purpose: "replica frontier observations".into(),
                    },
                )
                .await
                .expect("frontier observation schema matches catalog");
            let mut current = BTreeMap::new();
            // Retirement is final within this incarnation. MV replacement and
            // metric-plan replacement allocate distinct execution GlobalIds, and
            // plan revision changes do not retire installed executions. A late
            // predecessor report must never restore its row.
            let mut retired = BTreeSet::new();
            let mut events = Vec::new();
            while rx.recv_many(&mut events, 1024).await != 0 {
                let mut updates = Vec::new();
                for (id, row) in events.drain(..) {
                    if row.is_none() {
                        retired.insert(id);
                    } else if retired.contains(&id) {
                        continue;
                    }
                    if current.get(&id) == row.as_ref() {
                        continue;
                    }
                    if let Some(previous) = current.remove(&id) {
                        updates.push(TimestamplessUpdate {
                            row: previous,
                            diff: -Diff::ONE,
                        });
                    }
                    if let Some(row) = row {
                        current.insert(id, row.clone());
                        updates.push(TimestamplessUpdate {
                            row,
                            diff: Diff::ONE,
                        });
                    }
                }
                if !updates.is_empty() {
                    monotonic_append(&mut writer, updates, Timestamp::from(now())).await;
                }
            }
            writer.expire().await;
        })
        .abort_on_drop();
        Self {
            tx,
            replica,
            deployment,
            incarnation,
            _task: task,
        }
    }

    /// Called after partition aggregation, only for a live installed collection.
    /// The execution owner orders updates and retirement on this same queue.
    pub fn record(&self, id: GlobalId, progress: &FrontiersResponse) {
        let id_string = id.to_string();
        let replica_string = self.replica.to_string();
        let row = Row::pack_slice(&[
            Datum::String(&id_string),
            Datum::String(&replica_string),
            Datum::UInt64(self.deployment),
            Datum::UInt64(self.incarnation),
            Datum::from(progress.read_frontier.is_some()),
            progress
                .read_frontier
                .as_ref()
                .and_then(|f| f.as_option())
                .map(|t| Datum::MzTimestamp(*t))
                .unwrap_or(Datum::Null),
            Datum::from(progress.write_frontier.is_some()),
            progress
                .write_frontier
                .as_ref()
                .and_then(|f| f.as_option())
                .map(|t| Datum::MzTimestamp(*t))
                .unwrap_or(Datum::Null),
        ]);
        self.tx
            .send((id, Some(row)))
            .expect("frontier publisher remains alive");
    }

    pub fn retire(&self, id: GlobalId) {
        self.tx
            .send((id, None))
            .expect("frontier publisher remains alive");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_storage_types::sources::SourceData;
    use timely::progress::Antichain;

    #[mz_ore::test(tokio::test)]
    async fn same_deployment_publishers_preserve_peers_and_retire_late_reports() {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let persist = PersistClient::new_for_tests().await;
            let shard = ShardId::new();
            let desc = mz_catalog::builtin::MZ_REPLICA_FRONTIER_OBSERVATIONS
                .desc
                .clone();
            let mut reader = persist
                .open_leased_reader::<SourceData, (), Timestamp, i64>(
                    shard,
                    Arc::new(desc.clone()),
                    Arc::new(UnitSchema),
                    Diagnostics::for_tests(),
                    false,
                )
                .await
                .expect("frontier reader");
            let mut writer = persist
                .open_writer::<SourceData, (), Timestamp, i64>(
                    shard,
                    Arc::new(desc.clone()),
                    Arc::new(UnitSchema),
                    Diagnostics::for_tests(),
                )
                .await
                .expect("frontier probe");
            let make = |replica, incarnation| {
                Frontiers::spawn(
                    persist.clone(),
                    (GlobalId::System(1), shard, desc.clone()),
                    mz_ore::now::SYSTEM_TIME.clone(),
                    replica,
                    7,
                    incarnation,
                )
            };
            let a = make(ReplicaId::User(1), 10);
            let b = make(ReplicaId::User(2), 11);
            let id = GlobalId::User(1);
            let other = GlobalId::User(2);
            let first = FrontiersResponse {
                read_frontier: Some(Antichain::from_elem(Timestamp::from(10))),
                write_frontier: Some(Antichain::from_elem(Timestamp::from(20))),
                ..Default::default()
            };
            a.record(id, &first);
            b.record(id, &first);
            a.retire(id);
            // A report queued after retirement cannot resurrect this execution.
            a.record(id, &first);
            let completed = FrontiersResponse {
                read_frontier: Some(Antichain::from_elem(Timestamp::from(15))),
                write_frontier: Some(Antichain::new()),
                ..Default::default()
            };
            b.record(id, &completed);
            b.record(other, &FrontiersResponse::default());
            // Closing the real production queues drains all submitted reports.
            let Frontiers { tx, _task, .. } = a;
            drop(tx);
            _task.await;
            let Frontiers { tx, _task, .. } = b;
            drop(tx);
            _task.await;
            let upper = *writer
                .fetch_recent_upper()
                .await
                .as_option()
                .expect("open shard");
            let snapshot = reader
                .snapshot_and_fetch(Antichain::from_elem(
                    upper.step_back().expect("published rows"),
                ))
                .await
                .expect("frontier snapshot");
            let mut rows = snapshot
                .into_iter()
                .map(|((data, ()), _, diff)| (data.0.expect("valid row"), diff))
                .collect::<Vec<_>>();
            differential_dataflow::consolidation::consolidate(&mut rows);
            assert_eq!(rows.len(), 2);
            for (row, diff) in rows {
                assert_eq!(diff, 1);
                let fields = row.iter().collect::<Vec<_>>();
                assert_eq!(fields[1], Datum::String("u2"));
                assert_eq!(fields[2], Datum::UInt64(7));
                assert_eq!(fields[3], Datum::UInt64(11));
                if fields[0] == Datum::String("u1") {
                    assert_eq!(fields[4], Datum::True);
                    assert_eq!(fields[5], Datum::MzTimestamp(Timestamp::from(15)));
                    assert_eq!(fields[6], Datum::True);
                    assert_eq!(fields[7], Datum::Null);
                } else {
                    assert_eq!(fields[0], Datum::String("u2"));
                    assert_eq!(fields[4], Datum::False);
                    assert_eq!(fields[6], Datum::False);
                }
            }
            reader.expire().await;
            writer.expire().await;
        })
        .await
        .expect("frontier publishers complete");
    }
}
