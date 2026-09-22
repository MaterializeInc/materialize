// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Sink reconstruction from committed definitions and protected logical inputs.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use mz_catalog::catalog::Catalog;
use mz_catalog::memory::objects::CatalogItem;
use mz_catalog::read_protection::{
    CLIENT_PROTECTION_HEARTBEAT_INTERVAL, CLIENT_PROTECTION_UNCHANGED_GRACE,
};
use mz_controller_types::ClusterId;
use mz_repr::{GlobalId, Timestamp};
use mz_storage_client::client::{RunSinkCommand, StorageCommand};
use mz_storage_types::sinks::{StorageSinkConnection, StorageSinkDesc};
use timely::PartialOrder;
use timely::progress::Antichain;

use super::{ReplicaEffects, ReplicaEnactment, storage_metadata};

pub(super) struct Sink {
    // Canonical definition uses a minimum as_of. Execution frontiers may advance
    // without changing the definition or requiring another installation.
    pub definition: StorageSinkDesc<()>,
    pub execution: u64,
    pub restart: bool,
}

impl ReplicaEnactment {
    /// A fresh catalog observation belongs to this batch of requests, not to the
    /// earlier Run admission. Replies never authorize a different attempt.
    pub async fn admit_kafka_preopens(
        &mut self,
        catalog: &mut Catalog,
        effects: &mut ReplicaEffects,
        cluster: ClusterId,
        build: &str,
    ) -> anyhow::Result<()> {
        let Some(io) = &mut self.io.storage else {
            return Ok(());
        };
        let requests = std::mem::take(&mut io.preopens);
        if requests.is_empty() {
            return Ok(());
        }
        let refreshed: anyhow::Result<()> = async {
            let (_, updates) = self.io.wait(catalog.sync_to_current_updates()).await?;
            super::super::super::absorb_updates(effects, catalog, cluster, build, updates);
            self.ensure_live(catalog)?;
            self.publish(catalog, effects, cluster, build, true, None)
                .await?;
            self.ensure_recent_protection()
        }
        .await;
        self.apply_storage_progress();
        let desired = if refreshed.is_ok() {
            self.desired_sinks(catalog, cluster)
        } else {
            BTreeMap::new()
        };
        for (request, execution, id) in requests {
            let allowed = refreshed.is_ok()
                && self.storage_state.sinks.get(&id).is_some_and(|sink| {
                    !sink.restart
                        && sink.execution == execution
                        && desired.get(&id) == Some(&sink.definition)
                });
            let max_age = allowed
                .then(|| {
                    (CLIENT_PROTECTION_UNCHANGED_GRACE - CLIENT_PROTECTION_HEARTBEAT_INTERVAL)
                        .checked_sub(self.published_at.elapsed())
                })
                .flatten();
            self.io
                .storage
                .as_mut()
                .expect("storage endpoint")
                .endpoint
                .reply_kafka_pre_open(request, execution, id, max_age);
        }
        refreshed
    }

    fn kafka_eligible(&self, catalog: &Catalog, cluster: ClusterId) -> bool {
        let replicas: BTreeSet<_> = catalog
            .get_cluster(cluster)
            .replicas()
            .map(|replica| replica.replica_id)
            .collect();
        // Membership comes from the committed catalog. The immutable tag is not
        // a fence, and a heartbeat age alone does not reclaim an incarnation.
        catalog
            .state()
            .client_incarnations()
            .iter()
            .filter(|(_, value)| value.replica_id.is_some_and(|id| replicas.contains(&id)))
            .map(|(incarnation, _)| *incarnation)
            .min()
            == Some(self.protection.incarnation())
    }

    pub(super) fn desired_sinks(
        &self,
        catalog: &Catalog,
        cluster: ClusterId,
    ) -> BTreeMap<GlobalId, StorageSinkDesc<()>> {
        let kafka_eligible = self.kafka_eligible(catalog, cluster);
        catalog
            .entries()
            .filter_map(|entry| {
                let CatalogItem::Sink(sink) = entry.item() else {
                    return None;
                };
                if sink.cluster_id != cluster {
                    return None;
                }
                let (definition, _) = catalog
                    .state()
                    .storage_sink_description(sink, Antichain::from_elem(Timestamp::MIN));
                if matches!(definition.connection, StorageSinkConnection::Kafka(_))
                    && !kafka_eligible
                {
                    return None;
                }
                Some((entry.latest_global_id(), definition))
            })
            .collect()
    }

    /// Installs current sink definitions under the replica's shared protection.
    /// Health requests reconstruct from durable output progress, never from a
    /// cached execution description or an adapter's observation of that progress.
    pub async fn install_sinks(
        &mut self,
        catalog: &mut Catalog,
        effects: &mut ReplicaEffects,
        cluster: ClusterId,
        build: &str,
        metadata: &storage_metadata::Resolution,
    ) -> anyhow::Result<bool> {
        if self.io.storage.is_none() {
            return Ok(false);
        }
        self.ensure_live(catalog)?;
        self.apply_storage_progress();
        let desired = self.desired_sinks(catalog, cluster);
        let dropped: Vec<_> = self
            .storage_state
            .sinks
            .keys()
            .filter(|id| !desired.contains_key(id))
            .copied()
            .collect();
        for id in dropped {
            let old = self
                .storage_state
                .sinks
                .remove(&id)
                .expect("installed sink");
            self.storage_state.starting.remove(&old.execution);
            self.io
                .storage
                .as_mut()
                .expect("storage endpoint")
                .endpoint
                .send(StorageCommand::AllowCompaction(id, Antichain::new()));
        }
        let mut pending = false;
        for (id, definition) in desired {
            if self
                .storage_state
                .sinks
                .get(&id)
                .is_some_and(|sink| !sink.restart && sink.definition == definition)
            {
                continue;
            }
            let (Some(from_metadata), Some(to_metadata), Some(upper)) = (
                metadata.metadata.get(&definition.from),
                metadata.metadata.get(&id),
                metadata.uppers.get(&id),
            ) else {
                pending = true;
                continue;
            };
            let requirement = catalog
                .state()
                .maintained_read_requirements()
                .get(&id)
                .with_context(|| format!("sink {id} has no recovery requirement"))?;
            let Some(frontier) = requirement.frontier else {
                // A terminal durable output has no remaining input to replay.
                continue;
            };
            let inputs = BTreeSet::from([definition.from]);
            let holds = self
                .acquire(
                    catalog,
                    effects,
                    cluster,
                    build,
                    &inputs,
                    &BTreeSet::new(),
                    &BTreeMap::from([(definition.from, frontier)]),
                )
                .await?;
            // Grant publication can consume concurrent DDL. Do not combine its
            // current authority with a superseded definition or shard mapping.
            let current = catalog.try_get_entry_by_global_id(&id).and_then(|entry| {
                let CatalogItem::Sink(sink) = entry.item() else {
                    return None;
                };
                (sink.cluster_id == cluster).then(|| {
                    catalog
                        .state()
                        .storage_sink_description(sink, Antichain::from_elem(Timestamp::MIN))
                        .0
                })
            });
            if current.as_ref() != Some(&definition)
                || (matches!(definition.connection, StorageSinkConnection::Kafka(_))
                    && !self.kafka_eligible(catalog, cluster))
                || catalog
                    .state()
                    .storage_metadata()
                    .collection_metadata
                    .get(&id)
                    != Some(&to_metadata.data_shard)
                || catalog
                    .state()
                    .storage_metadata()
                    .collection_metadata
                    .get(&definition.from)
                    != Some(&from_metadata.data_shard)
            {
                pending = true;
                continue;
            }
            self.ensure_live(catalog)?;
            self.ensure_recent_protection()?;
            let as_of = Antichain::from_elem(frontier);
            let with_snapshot = definition.with_snapshot && !PartialOrder::less_than(&as_of, upper);
            let description = StorageSinkDesc {
                from: definition.from,
                from_desc: definition.from_desc.clone(),
                connection: definition.connection.clone(),
                envelope: definition.envelope,
                with_snapshot,
                version: definition.version,
                as_of,
                from_storage_metadata: from_metadata.clone(),
                to_storage_metadata: to_metadata.clone(),
                commit_interval: definition.commit_interval,
            };
            let execution = self
                .io
                .storage
                .as_mut()
                .expect("storage endpoint")
                .endpoint
                .send(StorageCommand::RunSink(Box::new(RunSinkCommand {
                    id,
                    description,
                })));
            if let Some(old) = self.storage_state.sinks.insert(
                id,
                Sink {
                    definition,
                    execution,
                    restart: false,
                },
            ) {
                self.storage_state.starting.remove(&old.execution);
            }
            self.storage_state.reads.insert(execution, holds);
            self.storage_state.starting.insert(execution);
        }
        Ok(pending || !self.storage_state.starting.is_empty())
    }
}
