// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Storage execution on the replica's shared catalog and protection incarnation.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context;
use differential_dataflow::lattice::Lattice;
use mz_catalog::catalog::Catalog;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_repr::{GlobalId, Timestamp};
use mz_storage::server::{ReplicaStorage, ReplicaStorageResponse};
use mz_storage_client::client::{RunIngestionCommand, StorageCommand, StorageResponse};
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::read_holds::ReadHold;
use mz_storage_types::sources::envelope::SourceEnvelope;
use mz_storage_types::sources::{IngestionDescription, SourceConnection, SourceExport};
use timely::progress::Antichain;

use super::{ReplicaEffects, ReplicaEnactment, storage_metadata};

mod sinks;

pub(super) struct StorageIo {
    pub endpoint: ReplicaStorage,
    pub inputs: BTreeMap<(u64, GlobalId), Antichain<Timestamp>>,
    pub restarts: BTreeSet<(u64, GlobalId)>,
    pub started: BTreeSet<u64>,
    preopens: Vec<(uuid::Uuid, u64, GlobalId)>,
    initialized: bool,
}

impl StorageIo {
    pub fn new(endpoint: ReplicaStorage) -> Self {
        Self {
            endpoint,
            inputs: BTreeMap::new(),
            restarts: BTreeSet::new(),
            started: BTreeSet::new(),
            preopens: Vec::new(),
            initialized: false,
        }
    }

    pub fn configure(&mut self, parameters: StorageParameters) {
        self.endpoint
            .send(StorageCommand::UpdateConfiguration(Box::new(parameters)));
        if !self.initialized {
            // The incarnation has committed under the replica's deployment
            // generation. Each Run still requires its own protected admission.
            self.endpoint.send(StorageCommand::AllowWrites);
            self.endpoint.send(StorageCommand::InitializationComplete);
            self.initialized = true;
        }
    }

    pub fn absorb(&mut self, response: ReplicaStorageResponse) {
        match response {
            ReplicaStorageResponse::KafkaPreOpen {
                request,
                execution,
                id,
            } => {
                self.preopens.push((request, execution, id));
            }
            ReplicaStorageResponse::ExecutionStarted { execution } => {
                self.started.insert(execution);
            }
            ReplicaStorageResponse::ExecutionInput {
                execution,
                input,
                frontier,
            } => {
                self.inputs.insert((execution, input), frontier);
            }
            ReplicaStorageResponse::RestartRequested { execution, id } => {
                self.restarts.insert((execution, id));
            }
            // Durable progress is sampled from Persist for recovery/publication.
            // Neither a shared write upper nor a bookkeeping DROP completes reads.
            ReplicaStorageResponse::Response(
                StorageResponse::FrontierUpper(_, _) | StorageResponse::DroppedId(_),
            ) => {}
            ReplicaStorageResponse::Response(response) => {
                tracing::debug!(?response, "replica storage response");
            }
        }
    }
}

struct Ingestion {
    definition: IngestionDescription<()>,
    execution: u64,
    restart: bool,
}

#[derive(Default)]
pub(super) struct StorageState {
    ingestions: BTreeMap<GlobalId, Ingestion>,
    sinks: BTreeMap<GlobalId, sinks::Sink>,
    // Includes retired attempts until their actual readers finish.
    reads: BTreeMap<u64, BTreeMap<GlobalId, ReadHold>>,
    starting: BTreeSet<u64>,
}

impl StorageState {
    fn apply_progress(&mut self, io: &mut StorageIo) {
        for execution in std::mem::take(&mut io.started) {
            self.starting.remove(&execution);
        }
        for ((execution, input), frontier) in std::mem::take(&mut io.inputs) {
            let Some(reads) = self.reads.get_mut(&execution) else {
                continue;
            };
            let Some(hold) = reads.get_mut(&input) else {
                continue;
            };
            hold.try_downgrade(hold.since().join(&frontier))
                .expect("monotone input progress");
            if frontier.is_empty() {
                reads.remove(&input);
            }
        }
        self.reads.retain(|_, reads| !reads.is_empty());
        for (execution, id) in std::mem::take(&mut io.restarts) {
            if let Some(ingestion) = self.ingestions.get_mut(&id)
                && ingestion.execution == execution
            {
                ingestion.restart = true;
                self.starting.remove(&execution);
            }
            if let Some(sink) = self.sinks.get_mut(&id)
                && sink.execution == execution
            {
                sink.restart = true;
                self.starting.remove(&execution);
            }
        }
    }
}

fn ingestions(
    catalog: &Catalog,
    cluster: ClusterId,
    replica: ReplicaId,
) -> BTreeMap<GlobalId, IngestionDescription<()>> {
    let single_replica = catalog
        .get_cluster(cluster)
        .replicas()
        .map(|r| r.replica_id)
        .min();
    catalog
        .entries()
        .filter_map(|entry| {
            let description = catalog.state().ingestion_description(entry.id())?;
            (description.instance_id == cluster
                && (!description.desc.connection.prefers_single_replica()
                    || single_replica == Some(replica)))
            .then_some((entry.latest_global_id(), description))
        })
        .collect()
}

impl ReplicaEnactment {
    pub fn storage_wanted(
        &self,
        catalog: &Catalog,
        cluster: ClusterId,
        replica: ReplicaId,
    ) -> BTreeSet<GlobalId> {
        if self.io.storage.is_none() {
            return BTreeSet::new();
        }
        ingestions(catalog, cluster, replica)
            .values()
            .flat_map(|i| i.collection_ids())
            .chain(
                self.desired_sinks(catalog, cluster)
                    .into_iter()
                    .flat_map(|(id, sink)| [id, sink.from]),
            )
            .collect()
    }

    pub fn apply_storage_progress(&mut self) {
        if let Some(io) = &mut self.io.storage {
            self.storage_state.apply_progress(io);
        }
    }

    /// Returns whether any source still awaits metadata, admission, or global
    /// reader installation. All read grants share the replica's single publisher.
    pub async fn install_sources(
        &mut self,
        catalog: &mut Catalog,
        effects: &mut ReplicaEffects,
        cluster: ClusterId,
        replica: ReplicaId,
        build: &str,
        metadata: &storage_metadata::Resolution,
    ) -> anyhow::Result<bool> {
        if self.io.storage.is_none() {
            return Ok(false);
        }
        self.ensure_live(catalog)?;
        self.apply_storage_progress();
        let desired = ingestions(catalog, cluster, replica);
        let dropped: Vec<_> = self
            .storage_state
            .ingestions
            .keys()
            .filter(|id| !desired.contains_key(id))
            .copied()
            .collect();
        for id in dropped {
            let old = self
                .storage_state
                .ingestions
                .remove(&id)
                .expect("installed ingestion");
            self.storage_state.starting.remove(&old.execution);
            let io = self.io.storage.as_mut().expect("storage endpoint");
            for output in old
                .definition
                .collection_ids()
                .chain(std::iter::once(id))
                .collect::<BTreeSet<_>>()
            {
                io.endpoint
                    .send(StorageCommand::AllowCompaction(output, Antichain::new()));
            }
        }
        let mut pending = false;
        for (id, definition) in desired {
            if self
                .storage_state
                .ingestions
                .get(&id)
                .is_some_and(|i| !i.restart && i.definition == definition)
            {
                continue;
            }
            if definition
                .collection_ids()
                .any(|id| !metadata.metadata.contains_key(&id))
            {
                pending = true;
                continue;
            }
            let inputs: BTreeSet<_> = std::iter::once(definition.remap_collection_id)
                .chain(definition.source_exports.iter().filter_map(|(id, export)| {
                    matches!(export.data_config.envelope, SourceEnvelope::Upsert(_)).then_some(*id)
                }))
                .collect();
            let requested = inputs
                .iter()
                .map(|input| {
                    let floor = catalog
                        .state()
                        .collection_compaction_bounds()
                        .get(input)
                        .and_then(|f| f.as_option())
                        .copied()
                        .with_context(|| {
                            format!("storage input {input} has no readable permission")
                        })?;
                    Ok((*input, floor))
                })
                .collect::<anyhow::Result<BTreeMap<_, _>>>()?;
            let holds = self
                .acquire(
                    catalog,
                    effects,
                    cluster,
                    build,
                    &inputs,
                    &BTreeSet::new(),
                    &requested,
                )
                .await?;
            self.ensure_live(catalog)?;
            let current = catalog
                .try_get_entry_by_global_id(&id)
                .and_then(|entry| catalog.state().ingestion_description(entry.id()));
            if current.as_ref() != Some(&definition)
                || (definition.desc.connection.prefers_single_replica()
                    && catalog
                        .get_cluster(cluster)
                        .replicas()
                        .map(|r| r.replica_id)
                        .min()
                        != Some(replica))
                || definition.collection_ids().any(|output| {
                    catalog
                        .state()
                        .storage_metadata()
                        .collection_metadata
                        .get(&output)
                        != Some(&metadata.metadata[&output].data_shard)
                })
            {
                pending = true;
                continue;
            }
            let description = IngestionDescription {
                desc: definition.desc.clone(),
                instance_id: cluster,
                remap_collection_id: definition.remap_collection_id,
                remap_metadata: metadata.metadata[&definition.remap_collection_id].clone(),
                source_exports: definition
                    .source_exports
                    .iter()
                    .map(|(export_id, export)| {
                        (
                            *export_id,
                            SourceExport {
                                storage_metadata: metadata.metadata[export_id].clone(),
                                details: export.details.clone(),
                                data_config: export.data_config.clone(),
                            },
                        )
                    })
                    .collect(),
            };
            self.ensure_recent_protection()?;
            let io = self.io.storage.as_mut().expect("storage endpoint");
            if let Some(old) = self.storage_state.ingestions.get(&id) {
                self.storage_state.starting.remove(&old.execution);
                let outputs: BTreeSet<_> = definition.collection_ids().collect();
                for removed in old
                    .definition
                    .collection_ids()
                    .filter(|id| !outputs.contains(id))
                {
                    io.endpoint
                        .send(StorageCommand::AllowCompaction(removed, Antichain::new()));
                }
            }
            let execution = io.endpoint.send(StorageCommand::RunIngestion(Box::new(
                RunIngestionCommand {
                    id,
                    description,
                    remap_compaction_bound: catalog
                        .state()
                        .collection_compaction_bounds()
                        .get(&definition.remap_collection_id)
                        .cloned(),
                },
            )));
            self.storage_state.reads.insert(execution, holds);
            self.storage_state.starting.insert(execution);
            self.storage_state.ingestions.insert(
                id,
                Ingestion {
                    definition,
                    execution,
                    restart: false,
                },
            );
        }
        Ok(pending || !self.storage_state.starting.is_empty())
    }
}
