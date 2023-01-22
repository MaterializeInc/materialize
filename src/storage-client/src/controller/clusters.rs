// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provisioning and assignment of storage clusters.
//!
//! A storage cluster is a group of `clusterd` processes. It may host any number
//! of storage objects, where a storage object is either an ingestion (i.e., a
//! source) or a sink.
//!
//! The [`StorageClusters`] type manages provisioning of storage clusters and
//! assignment of storage objects to those clusters. The default policy is to
//! create a new storage cluster for each storage object, but storage objects
//! may override this policy by specifying the address of an existing storage
//! cluster. This policy is subject to change in the future.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use serde::{Deserialize, Serialize};
use timely::progress::Timestamp;
use tokio::sync::Mutex;

use mz_build_info::BuildInfo;
use mz_orchestrator::{CpuLimit, MemoryLimit};
use mz_orchestrator::{NamespacedOrchestrator, ServiceConfig, ServicePort};
use mz_ore::collections::CollectionExt;
use mz_ore::halt;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::Codec64;
use mz_proto::RustType;

use crate::client::{ProtoStorageCommand, ProtoStorageResponse, StorageCommand, StorageResponse};
use crate::controller::rehydration::RehydratingStorageClient;
use crate::types::instances::StorageInstanceId;
use crate::types::parameters::StorageParameters;

/// The network address of a storage cluster.
pub type StorageClusterAddr = String;

/// Resource allocations for a storage cluster.
///
/// Has some overlap with mz_compute_client::controller::ComputeReplicaAllocation,
/// but keeping it separate for now due slightly different semantics.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageClusterResourceAllocation {
    /// The memory limit for each process in the replica.
    pub memory_limit: Option<MemoryLimit>,
    /// The CPU limit for each process in the replica.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of worker threads in the replica.
    pub workers: NonZeroUsize,
}

/// Size or address of a storage instance
///
/// This represents how resources for a storage instance are going to be
/// provisioned.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageClusterConfig {
    /// Remote unmanaged storage
    Remote {
        /// The network address of the clusterd process.
        addr: String,
    },
    /// A remote but managed replica
    Managed {
        /// The resource allocation for the replica.
        allocation: StorageClusterResourceAllocation,
        /// SQL size parameter used for allocation
        size: String,
    },
}

impl StorageClusterConfig {
    /// Returns the size specified by the storage cluster configuration, if
    /// the storage cluster is a managed storage cluster.
    pub fn size(&self) -> Option<&str> {
        match self {
            StorageClusterConfig::Remote { .. } => None,
            StorageClusterConfig::Managed { size, .. } => Some(size),
        }
    }
}

/// Describes a storage cluster.
#[derive(Debug, Clone)]
pub struct StorageClusterDesc {
    /// The ID of the storage cluster.
    pub id: StorageInstanceId,
    /// The configuration of the storage cluster.
    pub config: StorageClusterConfig,
}

/// Configuration for [`StorageClusters`].
#[derive(Debug, Clone)]
pub struct StorageClustersConfig {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// An orchestrator to start and stop storage clusters.
    pub orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The clusterd image to use when starting new storage clusters.
    pub clusterd_image: String,
    /// The init container image to use for clusterd.
    pub init_container_image: Option<String>,
}

/// Manages provisioning of storage clusters and assignment of storage objects
/// to those clusters.
///
/// See the [module documentation](self) for details.
#[derive(Debug)]
pub struct StorageClusters<T> {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// An orchestrator to start and stop storage clusters.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The clusterd image to use when starting new storage clusters.
    clusterd_image: String,
    /// The init container image to use for clusterd.
    init_container_image: Option<String>,
    /// Clients for all known storage clusters.
    clients: HashMap<StorageInstanceId, RehydratingStorageClient<T>>,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
    /// Storage configuration to apply to newly provisioned clusters.
    config: StorageParameters,
    /// A handle to Persist
    persist: Arc<Mutex<PersistClientCache>>,
}

impl<T> StorageClusters<T>
where
    T: Timestamp + Lattice + Codec64,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    /// Constructs a new [`StorageClusters`] from its configuration.
    pub fn new(
        config: StorageClustersConfig,
        persist: Arc<Mutex<PersistClientCache>>,
    ) -> StorageClusters<T> {
        StorageClusters {
            build_info: config.build_info,
            orchestrator: config.orchestrator,
            clusterd_image: config.clusterd_image,
            init_container_image: config.init_container_image,
            clients: HashMap::new(),
            initialized: false,
            config: Default::default(),
            persist,
        }
    }

    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before
    /// implementing prior commands, and so it is important for a user to invoke
    /// this method as soon as it is comfortable. This method can be invoked
    /// immediately, at the potential expense of performance.
    pub fn initialization_complete(&mut self) {
        self.initialized = true;
        for client in self.clients() {
            client.send(StorageCommand::InitializationComplete);
        }
    }

    /// Update the configuration of all clusters (existing and future).
    pub fn update_configuration(&mut self, config_params: StorageParameters) {
        for client in self.clients() {
            client.send(StorageCommand::UpdateConfiguration(config_params.clone()));
        }

        self.config.update(config_params);
    }

    /// Creates a new storage instance.
    pub fn create_instance(&mut self, id: StorageInstanceId) {
        let mut client = RehydratingStorageClient::new(self.build_info, Arc::clone(&self.persist));
        if self.initialized {
            client.send(StorageCommand::InitializationComplete);
        }
        client.send(StorageCommand::UpdateConfiguration(self.config.clone()));
        let old_client = self.clients.insert(id, client);
        assert!(old_client.is_none(), "storage instance {id} already exists");
    }

    /// Drops a storage instance.
    pub fn drop_instance(&mut self, id: StorageInstanceId) {
        let client = self.clients.remove(&id);
        assert!(client.is_some(), "storage instance {id} does not exist");
    }

    /// Provisions a storage replica for the storage instance with the specified
    /// ID.
    ///
    /// If the storage replica is managed, this will ensure that the backing
    /// orchestrator allocates resources, either by creating or updating the
    /// existing service. For 'remote' clusterd instances, the user is required
    /// to independently make sure that any resources exist -- if the
    /// orchestrator had provisioned a service for this cluster in the past, it
    /// will be dropped.
    pub async fn ensure_replica(
        &mut self,
        id: StorageInstanceId,
        cluster_config: StorageClusterConfig,
    ) -> Result<(), anyhow::Error>
    where
        StorageCommand<T>: RustType<ProtoStorageCommand>,
        StorageResponse<T>: RustType<ProtoStorageResponse>,
    {
        let addr = match cluster_config {
            StorageClusterConfig::Remote { addr } => {
                self.drop_replica(id).await?;
                addr
            }
            StorageClusterConfig::Managed { allocation, .. } => {
                let service = self
                    .orchestrator
                    .ensure_service(
                        &id.to_string(),
                        ServiceConfig {
                            image: self.clusterd_image.clone(),
                            init_container_image: self.init_container_image.clone(),
                            args: &|assigned| {
                                vec![
                                    format!("--storage-workers={}", allocation.workers),
                                    format!(
                                        "--storage-controller-listen-addr={}",
                                        assigned["storagectl"]
                                    ),
                                    format!(
                                        "--compute-controller-listen-addr={}",
                                        assigned["computectl"]
                                    ),
                                    format!(
                                        "--internal-http-listen-addr={}",
                                        assigned["internal-http"]
                                    ),
                                    format!("--opentelemetry-resource=storage_id={}", id),
                                ]
                            },
                            ports: vec![
                                ServicePort {
                                    name: "storagectl".into(),
                                    port_hint: 2100,
                                },
                                ServicePort {
                                    name: "computectl".into(),
                                    port_hint: 2101,
                                },
                                ServicePort {
                                    name: "internal-http".into(),
                                    port_hint: 6878,
                                },
                            ],
                            cpu_limit: allocation.cpu_limit,
                            memory_limit: allocation.memory_limit,
                            scale: NonZeroUsize::new(1).unwrap(),
                            labels: BTreeMap::from_iter([(
                                "size".to_string(),
                                allocation.workers.to_string(),
                            )]),
                            availability_zone: None,
                            // TODO: Decide on an A-A policy for storage clusters
                            anti_affinity: None,
                        },
                    )
                    .await?;
                service.addresses("storagectl").into_element()
            }
        };
        self.client(id).unwrap().connect(addr);
        Ok(())
    }

    /// Deprovisions the storage replica for the specified ID.
    pub async fn drop_replica(&self, id: StorageInstanceId) -> Result<(), anyhow::Error> {
        self.orchestrator.drop_service(&id.to_string()).await
    }

    /// Retrives the client for the storage cluster for the given ID, if it
    /// exists.
    pub fn client(&mut self, id: StorageInstanceId) -> Option<&mut RehydratingStorageClient<T>> {
        self.clients.get_mut(&id)
    }

    /// Returns an iterator over clients for all known storage clusters.
    pub fn clients(&mut self) -> impl Iterator<Item = &mut RehydratingStorageClient<T>> {
        self.clients.values_mut()
    }

    /// Removes orphaned storage clusters from the orchestrator.
    ///
    /// A storage cluster is considered orphaned if it is present in the orchestrator but not in the
    /// controller and the storage cluster id is less than `next_id`. We assume that the controller knows
    /// all storage clusters with ids [0..next_id).
    pub async fn remove_orphans(&self, next_id: StorageInstanceId) -> Result<(), anyhow::Error> {
        // Parse GlobalId and return contained user id, if any
        fn user_id(id: &String) -> Option<u64> {
            StorageInstanceId::from_str(id)
                .ok()
                .and_then(|id| match id {
                    StorageInstanceId::User(x) => Some(x),
                    _ => None,
                })
        }

        tracing::debug!("Removing storage clusterd orphans. next_id = {}", next_id);

        let next_id = match next_id {
            StorageInstanceId::User(x) => x,
            _ => anyhow::bail!("Expected GlobalId::User, got {}", next_id),
        };

        let service_ids: HashSet<_> = self
            .orchestrator
            .list_services()
            .await?
            .iter()
            .filter_map(user_id)
            .collect();

        let keep: HashSet<_> = {
            self.clients
                .keys()
                .filter_map(|x| match x {
                    StorageInstanceId::User(x) => Some(x),
                    _ => None,
                })
                .cloned()
                .collect()
        };

        for id in service_ids {
            if id >= next_id {
                // Found a storage cluster in kubernetes with a higher id than what we are aware of. This
                // must have been created by an environmentd with a higher epoch number.
                halt!(
                    "Found storage cluster id ({}) in orchestrator >= than next_id ({})",
                    id,
                    next_id
                );
            }
            if !keep.contains(&id) {
                tracing::warn!("Removing storage cluster orphan {}", id);
                self.drop_replica(StorageInstanceId::User(id)).await?;
            }
        }

        Ok(())
    }
}
