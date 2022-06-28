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
use std::sync::Arc;

use timely::progress::Timestamp;
use tracing::info;

use mz_orchestrator::{NamespacedOrchestrator, ServiceConfig, ServicePort};
use mz_ore::collections::CollectionExt;
use mz_proto::RustType;
use mz_repr::GlobalId;

use crate::client::controller::rehydration::RehydratingStorageClient;
use crate::client::{
    ProtoStorageCommand, ProtoStorageResponse, StorageCommand, StorageGrpcClient, StorageResponse,
};

/// The network address of a storage host.
pub type StorageHostAddr = String;

/// Configuration for [`StorageHosts`].
pub struct StorageHostsConfig {
    /// An orchestrator to start and stop storage hosts.
    pub orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The storaged image to use when starting new storage hosts.
    pub storaged_image: String,
}

/// Manages provisioning of storage hosts and assignment of storage objects
/// to those hosts.
///
/// See the [module documentation](self) for details.
#[derive(Debug)]
pub struct StorageHosts<T> {
    /// An orchestrator to start and stop storage hosts.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The storaged image to use when starting new storage hosts.
    storaged_image: String,
    /// The known storage hosts, identified by network address.
    hosts: HashMap<StorageHostAddr, StorageHost<T>>,
    /// The assignment of storage objects to storage hosts.
    objects: HashMap<GlobalId, StorageHostAddr>,
}

/// Metadata about a single storage host.
#[derive(Debug)]
struct StorageHost<T> {
    /// The client to the storage host.
    client: RehydratingStorageClient<T>,
    /// The IDs of the storage objects installed on the storage host.
    objects: HashSet<GlobalId>,
    /// Whether the storage host is orchestrated.
    orchestrated: bool,
}

impl<T> StorageHosts<T> {
    /// Constructs a new [`StorageHosts`] from its configuration.
    pub fn new(config: StorageHostsConfig) -> StorageHosts<T> {
        StorageHosts {
            orchestrator: config.orchestrator,
            storaged_image: config.storaged_image,
            objects: HashMap::new(),
            hosts: HashMap::new(),
        }
    }

    /// Provisions a storage host for the storage object with the specified ID.
    ///
    /// If `remote_addr` is `Some`, then the specified storage host is used.
    /// Otherwise, a storage host is assigned automatically.
    ///
    /// At present, the policy for storage host assignment creates a new storage
    /// host for each storage object. This policy is subject to change.
    ///
    /// Returns a client to the provisioned host. The client may be
    /// retrieved in the future via the [`client`](StorageHosts::client)
    /// method.
    ///
    /// # Panics
    ///
    /// Panics if `id` is already provisioned.
    pub async fn provision(
        &mut self,
        id: GlobalId,
        host_addr: Option<StorageHostAddr>,
    ) -> Result<&mut RehydratingStorageClient<T>, anyhow::Error>
    where
        T: Timestamp,
        StorageCommand<T>: RustType<ProtoStorageCommand>,
        StorageResponse<T>: RustType<ProtoStorageResponse>,
    {
        let (host_addr, orchestrated) = match host_addr {
            Some(host_addr) => (host_addr, false),
            None => (self.start_storage_host(id).await?, true),
        };
        let existed = self.objects.insert(id, host_addr.clone());
        assert!(
            existed.is_none(),
            "StorageHosts::provision called for already-provisioned ID {id}"
        );
        info!("assigned storage object {id} to storage host {host_addr}");
        match self.hosts.entry(host_addr.clone()) {
            Entry::Vacant(entry) => {
                let client =
                    RehydratingStorageClient::new(StorageGrpcClient::new(host_addr.clone()));
                let host = entry.insert(StorageHost {
                    client,
                    objects: HashSet::from_iter([id]),
                    orchestrated,
                });
                Ok(&mut host.client)
            }
            Entry::Occupied(entry) => {
                let host = entry.into_mut();
                let inserted = host.objects.insert(id);
                assert!(
                    inserted,
                    "StorageHosts internally inconsistent: ID {id} partially tracked"
                );
                Ok(&mut host.client)
            }
        }
    }

    /// Deprovisions the storage host for the storage object with the specified
    /// ID.
    ///
    /// # Panics
    ///
    /// Panics if the provided `id` has not been provisioned.
    pub async fn deprovision(&mut self, id: GlobalId) -> Result<(), anyhow::Error> {
        let host_addr = self.objects.remove(&id).unwrap_or_else(|| {
            panic!("StorageHosts::deprovision called with unprovisioned ID {id}")
        });
        match self.hosts.entry(host_addr.clone()) {
            Entry::Vacant(_) => panic!(
                "StorageHosts internally inconsistent: \
                 ingestion {id} referenced missing storage host {host_addr:?}"
            ),
            Entry::Occupied(mut entry) => {
                let host = entry.get_mut();
                let removed = host.objects.remove(&id);
                assert!(
                    removed,
                    "StorageHosts internally inconsistent: ingestion {id} backreference missing"
                );
                if host.objects.is_empty() {
                    let orchestrated = host.orchestrated;
                    entry.remove_entry();
                    if orchestrated {
                        self.stop_storage_host(id).await?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Retrives the client for the storage host for the given ID, if the
    /// ID is currently provisioned.
    pub fn client(&mut self, id: GlobalId) -> Option<&mut RehydratingStorageClient<T>> {
        let host_addr = self.objects.get(&id)?;
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
    async fn start_storage_host(&self, id: GlobalId) -> Result<StorageHostAddr, anyhow::Error> {
        let storage_service = self
            .orchestrator
            .ensure_service(
                &id.to_string(),
                ServiceConfig {
                    image: self.storaged_image.clone(),
                    args: &|assigned| {
                        vec![
                            format!("--workers=1"),
                            format!(
                                "--listen-addr={}:{}",
                                assigned.listen_host, assigned.ports["controller"]
                            ),
                            format!(
                                "--internal-http-listen-addr={}:{}",
                                assigned.listen_host, assigned.ports["internal-http"]
                            ),
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
                            port_hint: 6877,
                        },
                    ],
                    // TODO: limits?
                    cpu_limit: None,
                    memory_limit: None,
                    scale: NonZeroUsize::new(1).unwrap(),
                    labels: HashMap::new(),
                    availability_zone: None,
                },
            )
            .await?;
        Ok(storage_service.addresses("controller").into_element())
    }

    /// Stops an orchestrated storage host for the specified ID.
    async fn stop_storage_host(&self, id: GlobalId) -> Result<(), anyhow::Error> {
        self.orchestrator.drop_service(&id.to_string()).await
    }
}
