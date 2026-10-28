// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Publication of this deployment's observed replica process transitions.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use mz_catalog::catalog::Catalog;
use mz_controller::clusters::ReplicaServiceName;
use mz_orchestrator::{ServiceEvent, ServiceStatus};
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_storage_client::client::TimestamplessUpdate;
use mz_storage_client::collection_writer::monotonic_append;
use tokio::sync::mpsc;

pub(super) struct ReplicaHistory {
    pub task: AbortOnDropHandle<()>,
    tx: mpsc::UnboundedSender<TimestamplessUpdate>,
    transitions: Transitions,
}

impl ReplicaHistory {
    pub fn new(catalog: &Catalog, persist: PersistClient, generation: u64) -> anyhow::Result<Self> {
        let builtin = &*mz_catalog::builtin::MZ_CLUSTER_REPLICA_STATUS_HISTORY;
        let item = catalog.resolve_builtin_storage_collection(builtin);
        let id = catalog.get_entry(&item).latest_global_id();
        let shard = catalog
            .state()
            .storage_metadata()
            .get_collection_shard(id)
            .context("replica status history requires committed shard metadata")?;
        let desc = builtin.desc.clone();
        let now = catalog.config().now.clone();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let task = mz_ore::task::spawn(|| "controller-replica-status", async move {
            let mut writer = persist
                .open_writer(
                    shard,
                    Arc::new(desc),
                    Arc::new(UnitSchema),
                    Diagnostics {
                        shard_name: id.to_string(),
                        handle_purpose: "controller replica status".into(),
                    },
                )
                .await
                .expect("replica status schema matches catalog");
            let mut updates = Vec::new();
            while rx.recv_many(&mut updates, 1024).await != 0 {
                monotonic_append(
                    &mut writer,
                    std::mem::take(&mut updates),
                    Timestamp::from(now()),
                )
                .await;
            }
        })
        .abort_on_drop();
        Ok(Self {
            task,
            tx,
            transitions: Transitions {
                generation,
                desired: BTreeMap::new(),
                previous: BTreeMap::new(),
            },
        })
    }

    pub fn set_desired(&mut self, desired: BTreeMap<String, u64>) {
        self.transitions.set_desired(desired);
    }

    /// Queue an original observation without blocking process reconciliation.
    /// Controller exit may discard unpublished history, as with replica health.
    pub fn record(&mut self, event: &ServiceEvent) {
        if let Some(row) = self.transitions.record(event) {
            self.tx
                .send(TimestamplessUpdate {
                    row,
                    diff: Diff::ONE,
                })
                .expect("replica status publisher is alive");
        }
    }
}

struct Transitions {
    generation: u64,
    desired: BTreeMap<String, u64>,
    previous: BTreeMap<(String, u64), ServiceStatus>,
}

impl Transitions {
    fn set_desired(&mut self, desired: BTreeMap<String, u64>) {
        self.previous.retain(|(service, process), _| {
            desired.get(service).is_some_and(|count| process < count)
        });
        self.desired = desired;
    }

    fn record(&mut self, event: &ServiceEvent) -> Option<Row> {
        let processes = self.desired.get(&event.service_id)?;
        let name: ReplicaServiceName = event.service_id.parse().ok()?;
        if name.generation != self.generation || event.process_id >= *processes {
            return None;
        }
        let key = (event.service_id.clone(), event.process_id);
        if self.previous.get(&key) == Some(&event.status) {
            return None;
        }
        self.previous.insert(key, event.status);
        let reason = match event.status {
            ServiceStatus::Online => None,
            ServiceStatus::Offline(reason) => reason.map(|reason| reason.to_string()),
        };
        // The generation qualifies history, including observations during prewarming.
        // The public current-status view selects the active deployment. Retention
        // remains with the existing collection manager, not this publisher.
        Some(Row::pack_slice(&[
            Datum::String(&name.replica_id.to_string()),
            Datum::UInt64(event.process_id),
            Datum::String(event.status.as_kebab_case_str()),
            Datum::from(reason.as_deref()),
            Datum::TimestampTz(event.time.try_into().expect("must fit")),
            Datum::UInt64(self.generation),
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn deployment_scoped_transitions_and_replay() {
        let service = ReplicaServiceName {
            cluster_id: mz_controller_types::ClusterId::User(1),
            replica_id: mz_controller_types::ReplicaId::User(1),
            generation: 7,
        }
        .to_string();
        let other = ReplicaServiceName {
            cluster_id: mz_controller_types::ClusterId::User(1),
            replica_id: mz_controller_types::ReplicaId::User(1),
            generation: 8,
        }
        .to_string();
        let mut transitions = Transitions {
            generation: 7,
            desired: BTreeMap::new(),
            previous: BTreeMap::new(),
        };
        let mut event = ServiceEvent {
            service_id: service.clone(),
            process_id: 0,
            status: ServiceStatus::Online,
            restart_count: 0,
            time: chrono::Utc::now(),
        };
        // A watch can precede desired inventory. Replaying it after binding must publish.
        assert!(transitions.record(&event).is_none());
        transitions.set_desired(BTreeMap::from([(service.clone(), 1), (other.clone(), 1)]));
        let row = transitions.record(&event).expect("initial status");
        assert_eq!(row.iter().last(), Some(Datum::UInt64(7)));
        event.time += chrono::Duration::seconds(1);
        assert!(transitions.record(&event).is_none());
        event.restart_count += 1;
        assert!(transitions.record(&event).is_none());
        event.status = ServiceStatus::Offline(Some(mz_orchestrator::OfflineReason::OomKilled));
        let offline = transitions.record(&event).expect("intervening failure");
        assert_eq!(offline.iter().nth(3), Some(Datum::String("oom-killed")));
        event.status = ServiceStatus::Online;
        assert!(transitions.record(&event).is_some());
        // Reconciliation replay must not replace or duplicate the initial flaps.
        assert!(transitions.record(&event).is_none());
        event.service_id = other;
        assert!(transitions.record(&event).is_none());
        event.service_id = service.clone();
        event.process_id = 1;
        assert!(transitions.record(&event).is_none());
        event.process_id = 0;
        transitions.set_desired(BTreeMap::new());
        assert!(transitions.record(&event).is_none());
        transitions.set_desired(BTreeMap::from([(service, 1)]));
        assert!(transitions.record(&event).is_some());
    }
}
