// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provisioning and assignment of storage hosts.
//!
//! A storage host is a single `storaged` process. It may host any number of
//! storage objects, where a storage object is either an ingestion (i.e., a
//! source) or a sink.
//!
//! The [`StorageHosts`] type manages provisioning of storage hosts and
//! assignment of storage objects to those hosts. The default policy is to
//! create a new storage host for each storage object, but storage objects
//! may override this policy by specifying the address of an existing storage
//! host. This policy is subject to change in the future.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use timely::progress::Timestamp;
use tokio::sync::Mutex;

use mz_build_info::BuildInfo;
use mz_orchestrator::{NamespacedOrchestrator, ServiceConfig, ServicePort, ServiceProcessMetrics};
use mz_ore::collections::CollectionExt;
use mz_ore::halt;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::Codec64;
use mz_proto::RustType;
use mz_repr::GlobalId;

use crate::client::{ProtoStorageCommand, ProtoStorageResponse, StorageCommand, StorageResponse};
use crate::controller::rehydration::RehydratingStorageClient;
use crate::types::hosts::{StorageHostConfig, StorageHostResourceAllocation};

/// The network address of a storage host.
pub type StorageHostAddr = String;

/// Configuration for [`StorageHosts`].
pub struct StorageHostsConfig {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// An orchestrator to start and stop storage hosts.
    pub orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The storaged image to use when starting new storage hosts.
    pub storaged_image: String,
    /// The init container image to use for storaged.
    pub init_container_image: Option<String>,
}

/// Manages provisioning of storage hosts and assignment of storage objects
/// to those hosts.
///
/// See the [module documentation](self) for details.
#[derive(Debug)]
pub struct StorageHosts<T> {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// An orchestrator to start and stop storage hosts.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The storaged image to use when starting new storage hosts.
    storaged_image: String,
    /// The init container image to use for storaged.
    init_container_image: Option<String>,
    /// The known storage hosts, identified by network address.
    hosts: HashMap<StorageHostAddr, StorageHost<T>>,
    /// The assignment of storage objects to storage hosts.
    objects: Arc<std::sync::Mutex<HashMap<GlobalId, StorageHostAddr>>>,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
    /// A handle to Persist
    persist: Arc<Mutex<PersistClientCache>>,
}

/// Metadata about a single storage host, effectively used for reference-counting
/// the storage client.
#[derive(Debug)]
struct StorageHost<T> {
    /// The client to the storage host.
    client: RehydratingStorageClient<T>,
    /// The IDs of the storage objects installed on the storage host.
    objects: HashSet<GlobalId>,
}

impl<T> StorageHosts<T>
where
    T: Timestamp + Lattice + Codec64,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    /// Constructs a new [`StorageHosts`] from its configuration.
    pub fn new(
        config: StorageHostsConfig,
        persist: Arc<Mutex<PersistClientCache>>,
    ) -> StorageHosts<T> {
        StorageHosts {
            build_info: config.build_info,
            orchestrator: config.orchestrator,
            storaged_image: config.storaged_image,
            init_container_image: config.init_container_image,
            objects: Arc::new(std::sync::Mutex::new(HashMap::new())),
            hosts: HashMap::new(),
            initialized: false,
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

    /// Creates a [`MetricsFetcher`] that can be used to repeatedly fetch
    /// metrics for all known storage objects.
    pub fn metrics_fetcher(&self) -> MetricsFetcher {
        MetricsFetcher::new(Arc::clone(&self.orchestrator), Arc::clone(&self.objects))
    }

    /// Provisions a storage host for the storage object with the specified ID.
    /// If the storage host is managed, this will ensure that the backing orchestrator
    /// allocates resources, either by creating or updating the existing service.
    /// (For 'remote' storaged instances, the user is required to independently make
    /// sure that any resources exist -- if the orchestrator had provisioned a service
    /// for this host in the past, it will be dropped.)
    ///
    /// At present, the policy for storage host assignment creates a new storage
    /// host for each storage object. This policy is subject to change.
    ///
    /// Returns a client to the provisioned host. The client may be
    /// retrieved in the future via the [`client`](StorageHosts::client)
    /// method.
    pub async fn provision(
        &mut self,
        id: GlobalId,
        host_config: StorageHostConfig,
    ) -> Result<&mut RehydratingStorageClient<T>, anyhow::Error>
    where
        StorageCommand<T>: RustType<ProtoStorageCommand>,
        StorageResponse<T>: RustType<ProtoStorageResponse>,
    {
        let host_addr = match host_config {
            StorageHostConfig::Remote { addr } => {
                self.drop_storage_host(id).await?;
                addr
            }
            StorageHostConfig::Managed { allocation, .. } => {
                self.ensure_storage_host(id, allocation).await?
            }
        };

        if let Some(previous_address) = {
            let mut objects = self.objects.lock().expect("lock poisoned");
            objects.insert(id, host_addr.clone())
        } {
            if previous_address != host_addr {
                self.remove_id_from_host(id, host_addr.clone());
            }
        };

        let host = self.hosts.entry(host_addr.clone()).or_insert_with(|| {
            let mut client = RehydratingStorageClient::new(
                host_addr,
                self.build_info,
                Arc::clone(&self.persist),
            );
            if self.initialized {
                client.send(StorageCommand::InitializationComplete);
            }
            StorageHost {
                client,
                objects: HashSet::with_capacity(1),
            }
        });

        host.objects.insert(id);

        Ok(&mut host.client)
    }

    /// Deprovisions the storage host for the storage object with the specified
    /// ID: ensures we're not orchestrating any resources for this id, and cleans
    /// up any internal state.
    pub async fn deprovision(&mut self, id: GlobalId) -> Result<(), anyhow::Error> {
        self.drop_storage_host(id).await?;

        let host_addr = { self.objects.lock().expect("lock poisoned").remove(&id) };

        if let Some(host_addr) = host_addr {
            self.remove_id_from_host(id, host_addr);
        }

        Ok(())
    }

    /// If a id no longer maps to a particular host_addr, this removes the id from the host's
    /// set -- and, if the set is empty, shuts down the client.
    fn remove_id_from_host(&mut self, id: GlobalId, host_addr: StorageHostAddr) {
        if let Entry::Occupied(mut entry) = self.hosts.entry(host_addr) {
            let objects = &mut entry.get_mut().objects;
            objects.remove(&id);
            if objects.is_empty() {
                entry.remove();
            }
        }
    }

    /// Retrives the client for the storage host for the given ID, if the
    /// ID is currently provisioned.
    pub fn client(&mut self, id: GlobalId) -> Option<&mut RehydratingStorageClient<T>> {
        let objects = self.objects.lock().expect("lock poisoned");
        let host_addr = objects.get(&id)?;

        match self.hosts.get_mut(host_addr) {
            None => panic!(
                "StorageHosts internally inconsistent: \
                 ingestion {id} referenced missing storage host {host_addr:?}"
            ),
            Some(host) => Some(&mut host.client),
        }
    }

    /// Returns an iterator over clients for all known storage hosts.
    pub fn clients(&mut self) -> impl Iterator<Item = &mut RehydratingStorageClient<T>> {
        self.hosts.values_mut().map(|h| &mut h.client)
    }

    /// Starts a orchestrated storage host for the specified ID.
    async fn ensure_storage_host(
        &self,
        id: GlobalId,
        allocation: StorageHostResourceAllocation,
    ) -> Result<StorageHostAddr, anyhow::Error> {
        let storage_service = self
            .orchestrator
            .ensure_service(
                &id.to_string(),
                ServiceConfig {
                    image: self.storaged_image.clone(),
                    init_container_image: self.init_container_image.clone(),
                    args: &|assigned| {
                        vec![
                            format!("--workers={}", allocation.workers),
                            format!("--controller-listen-addr={}", assigned["controller"]),
                            format!("--internal-http-listen-addr={}", assigned["internal-http"]),
                            format!("--opentelemetry-resource=storage_id={}", id),
                        ]
                    },
                    ports: vec![
                        ServicePort {
                            name: "controller".into(),
                            port_hint: 2100,
                        },
                        ServicePort {
                            name: "internal-http".into(),
                            port_hint: 6878,
                        },
                    ],
                    cpu_limit: allocation.cpu_limit,
                    memory_limit: allocation.memory_limit,
                    scale: NonZeroUsize::new(1).unwrap(),
                    labels: HashMap::from_iter([(
                        "size".to_string(),
                        allocation.workers.to_string(),
                    )]),
                    availability_zone: None,
                    // TODO: Decide on an A-A policy for storage hosts
                    anti_affinity: None,
                },
            )
            .await?;
        Ok(storage_service.addresses("controller").into_element())
    }

    /// Drops an orchestrated storage host for the specified ID.
    async fn drop_storage_host(&self, id: GlobalId) -> Result<(), anyhow::Error> {
        self.orchestrator.drop_service(&id.to_string()).await
    }

    /// Removes orphaned storageds from the orchestrator.
    ///
    /// A storaged is considered orphaned if it is present in the orchestrator but not in the
    /// controller and the storaged id is less than `next_id`. We assume that the controller knows
    /// all storageds with ids [0..next_id).
    pub async fn remove_orphans(&self, next_id: GlobalId) -> Result<(), anyhow::Error> {
        // Parse GlobalId and return contained user id, if any
        fn user_id(id: &String) -> Option<u64> {
            GlobalId::from_str(id).ok().and_then(|id| match id {
                GlobalId::User(x) => Some(x),
                _ => None,
            })
        }

        tracing::debug!("Removing storaged orphans. next_id = {}", next_id);

        let next_id = match next_id {
            GlobalId::User(x) => x,
            _ => anyhow::bail!("Expected GlobalId::User, got {}", next_id),
        };

        let service_ids: HashSet<_> = self
            .orchestrator
            .list_services()
            .await?
            .iter()
            .filter_map(user_id)
            .collect();

        let catalog_ids: HashSet<_> = {
            let objects = self.objects.lock().expect("lock poisoned");

            objects
                .keys()
                .filter_map(|x| match x {
                    GlobalId::User(x) => Some(x),
                    _ => None,
                })
                .cloned()
                .collect()
        };

        for id in service_ids {
            if id >= next_id {
                // Found a storaged in kubernetes with a higher id than what we are aware of. This
                // must have been created by an environmentd with a higher epoch number.
                halt!(
                    "Found storaged id ({}) in orchestrator >= than next_id ({})",
                    id,
                    next_id
                );
            }
            if !catalog_ids.contains(&id) && id < next_id {
                let gid = GlobalId::User(id);
                tracing::warn!("Removing storaged orphan {}", gid);
                self.orchestrator.drop_service(&gid.to_string()).await?;
            }
        }

        Ok(())
    }
}

/// A helper that shares state with a [`StorageHosts`] and allows repeatedly
/// fetching metrics for all known storage hosts.
pub struct MetricsFetcher {
    /// The orchestrator that we use to fetch metrics.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The shared assignment of storage objects to storage hosts. This is
    /// shared with a [`StorageHosts`].
    objects: Arc<std::sync::Mutex<HashMap<GlobalId, StorageHostAddr>>>,
}

impl MetricsFetcher {
    /// Creates a new [`MetricsFetcher`].
    fn new(
        orchestrator: Arc<dyn NamespacedOrchestrator>,
        objects: Arc<std::sync::Mutex<HashMap<GlobalId, StorageHostAddr>>>,
    ) -> Self {
        Self {
            orchestrator,
            objects,
        }
    }

    /// Fetches and returns metrics for all known storage hosts.
    pub async fn fetch_metrics(
        &mut self,
    ) -> Vec<(
        GlobalId,
        String,
        Result<Vec<ServiceProcessMetrics>, anyhow::Error>,
    )> {
        let service_ids = {
            self.objects
                .lock()
                .expect("lock poisoned")
                .iter()
                .map(|(id, host_addr)| (id.clone(), host_addr.clone()))
                .collect::<Vec<_>>()
        };
        tracing::trace!("fetch_metrics: service_ids: {:?}", service_ids);

        let mut metrics = Vec::new();

        // TODO(aljoscha): Fetching all this sequentially will be slow when we
        // have many sources. We should allow fetching metrics for multiple
        // services on the Orchestrator and then use it here.
        for (service_id, host_addr) in service_ids {
            let service_metrics = self
                .orchestrator
                .fetch_service_metrics(&service_id.to_string())
                .await;
            metrics.push((service_id, host_addr, service_metrics));
        }

        metrics
    }
}
