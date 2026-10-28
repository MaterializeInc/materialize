// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Resource observations from the deployment's process owner, independent of SQL.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use mz_catalog::catalog::Catalog;
use mz_controller::clusters::{ReplicaServiceName, spawn_service_metrics_task};
use mz_orchestrator::{NamespacedOrchestrator, ServiceProcessMetrics};
use mz_ore::cast::CastFrom;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, Diff, Row, Timestamp};
use mz_storage_client::client::TimestamplessUpdate;
use mz_storage_client::collection_writer::monotonic_append;
use tokio::sync::{mpsc, watch};

pub(super) struct ReplicaMetrics {
    pub task: AbortOnDropHandle<()>,
    desired: watch::Sender<BTreeMap<String, u64>>,
}

impl ReplicaMetrics {
    pub fn new(
        catalog: &Catalog,
        persist: PersistClient,
        generation: u64,
        orchestrator: Arc<dyn NamespacedOrchestrator>,
    ) -> anyhow::Result<Self> {
        let builtin = &*mz_catalog::builtin::MZ_CLUSTER_REPLICA_METRICS_HISTORY;
        let item = catalog.resolve_builtin_storage_collection(builtin);
        let id = catalog.get_entry(&item).latest_global_id();
        let shard = catalog
            .state()
            .storage_metadata()
            .get_collection_shard(id)
            .context("replica metrics history requires committed shard metadata")?;
        let desc = builtin.desc.clone();
        let now = catalog.config().now.clone();
        let (desired, mut changes) = watch::channel(BTreeMap::<String, u64>::new());
        let task = mz_ore::task::spawn(|| "controller-replica-metrics", async move {
            let mut writer = persist.open_writer(
                shard, Arc::new(desc), Arc::new(UnitSchema),
                Diagnostics { shard_name: id.to_string(), handle_purpose: "controller replica metrics".into() },
            ).await.expect("replica metrics schema matches catalog");
            let (tx, mut rx) = mpsc::unbounded_channel();
            let mut samplers = BTreeMap::new();
            let mut inventory = BTreeMap::new();
            loop {
                tokio::select! {
                    changed = changes.changed() => {
                        if changed.is_err() { break; }
                        inventory = changes.borrow_and_update().clone();
                        samplers.retain(|service, _| inventory.contains_key(service));
                        for service in inventory.keys() {
                            let Ok(name) = service.parse::<ReplicaServiceName>() else { continue; };
                            if name.generation != generation { continue; }
                            samplers.entry(service.clone()).or_insert_with(|| {
                                let tx = tx.clone();
                                let observed_now = now.clone();
                                let service = service.clone();
                                spawn_service_metrics_task(Arc::clone(&orchestrator), service.clone(), move |metrics| {
                                    let _ = tx.send((service.clone(), metrics, observed_now()));
                                })
                            });
                        }
                    }
                    Some((service, metrics, observed_at)) = rx.recv() => {
                        let Some(processes) = inventory.get(&service) else { continue; };
                        let Ok(name) = service.parse::<ReplicaServiceName>() else { continue; };
                        if name.generation != generation { continue; }
                        let timestamp = Timestamp::from(now());
                        let rows = metrics_rows(&name.replica_id.to_string(), &metrics, *processes, generation, observed_at);
                        // Do not cancel submitted appends when inventory changes. These
                        // are historical observations, not authority over the replica.
                        monotonic_append(&mut writer, rows, timestamp).await;
                    }
                }
            }
        }).abort_on_drop();
        Ok(Self { task, desired })
    }

    pub fn set_desired(&self, desired: BTreeMap<String, u64>) {
        self.desired.send_if_modified(|current| {
            if *current == desired {
                false
            } else {
                *current = desired;
                true
            }
        });
    }
}

fn metrics_rows(
    replica_id: &str,
    metrics: &[ServiceProcessMetrics],
    processes: u64,
    generation: u64,
    now: u64,
) -> Vec<TimestamplessUpdate> {
    let occurred_at = mz_ore::now::to_datetime(now).try_into().expect("must fit");
    metrics
        .iter()
        .enumerate()
        .take_while(|(process, _)| u64::cast_from(*process) < processes)
        .map(|(process, metrics)| TimestamplessUpdate {
            row: Row::pack_slice(&[
                Datum::String(replica_id),
                Datum::UInt64(u64::cast_from(process)),
                metrics.cpu_nano_cores.into(),
                metrics.memory_bytes.into(),
                metrics.disk_bytes.into(),
                Datum::TimestampTz(occurred_at),
                metrics.heap_bytes.into(),
                metrics.heap_limit.into(),
                metrics.swap_bytes.into(),
                Datum::UInt64(generation),
            ]),
            diff: Diff::ONE,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    #[test]
    fn observations_preserve_unknown_values_and_deployment_identity() {
        let samples = [
            ServiceProcessMetrics::default(),
            ServiceProcessMetrics {
                cpu_nano_cores: Some(123),
                memory_bytes: Some(0),
                disk_bytes: Some(456),
                heap_bytes: Some(789),
                heap_limit: Some(1024),
                swap_bytes: Some(0),
            },
            ServiceProcessMetrics::default(),
        ];
        let rows = metrics_rows("u42", &samples, 2, 17, 1000);
        assert_eq!(rows.len(), 2);
        let timestamp = mz_ore::now::to_datetime(1000).try_into().unwrap();
        assert_eq!(
            rows[0].row,
            Row::pack_slice(&[
                Datum::String("u42"),
                Datum::UInt64(0),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::TimestampTz(timestamp),
                Datum::Null,
                Datum::Null,
                Datum::Null,
                Datum::UInt64(17),
            ])
        );
        assert_eq!(
            rows[1].row,
            Row::pack_slice(&[
                Datum::String("u42"),
                Datum::UInt64(1),
                Datum::UInt64(123),
                Datum::UInt64(0),
                Datum::UInt64(456),
                Datum::TimestampTz(timestamp),
                Datum::UInt64(789),
                Datum::UInt64(1024),
                Datum::UInt64(0),
                Datum::UInt64(17),
            ])
        );
        let desc = &*mz_storage_client::healthcheck::REPLICA_METRICS_HISTORY_DESC;
        for row in rows {
            assert_eq!(row.diff, Diff::ONE);
            assert_eq!(row.row.iter().count(), desc.arity());
            for (datum, (_, ty)) in row.row.iter().zip_eq(desc.iter()) {
                assert!(datum.is_instance_of_sql(ty));
            }
        }
        assert!(metrics_rows("u42", &samples, 0, 17, 1000).is_empty());
    }
}
