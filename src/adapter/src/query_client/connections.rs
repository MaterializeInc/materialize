// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Catalog-derived query connections. This module never provisions replicas or
//! acquires ownership of maintained compute state.

use std::collections::BTreeMap;
use std::num::NonZero;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use mz_build_info::BuildInfo;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_types::ComputeInstanceId;
use mz_controller::clusters::{ReplicaLocation, ReplicaServiceName};
use mz_controller_types::ReplicaId;
use mz_orchestrator::{NamespacedOrchestrator, ServicePort};
use mz_ore::cast::CastFrom;
use mz_ore::task::AbortOnDropHandle;
use mz_service::transport::{Client, NoopMetrics};
use tokio::sync::watch;

use super::compute::ReplicaQueryClient;
use crate::AdapterError;
use crate::catalog::Catalog;

type ReplicaKey = (ComputeInstanceId, ReplicaId);

/// Immutable deployment inputs, not controller state. `orchestrator` must already
/// be restricted to the `cluster` namespace used for replica provisioning.
#[derive(Debug)]
pub(crate) struct QueryReplicaConnectionsConfig {
    pub orchestrator: Arc<dyn NamespacedOrchestrator>,
    pub deploy_generation: u64,
    pub build_info: &'static BuildInfo,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Endpoint {
    Unmanaged(Vec<String>),
    Managed {
        service: String,
        scale: NonZero<u16>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Settings {
    connect_timeout: Duration,
    keepalive_timeout: Duration,
    max_result_size: usize,
}

struct ReplicaState {
    settings: Settings,
    client: Option<ReplicaQueryClient>,
}

impl std::fmt::Debug for ReplicaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicaState")
            .field("settings", &self.settings)
            .field("connected", &self.client.is_some())
            .finish()
    }
}

#[derive(Debug)]
struct Replica {
    endpoint: Endpoint,
    state: Arc<Mutex<ReplicaState>>,
    settings_changed: watch::Sender<()>,
    _task: AbortOnDropHandle<()>,
}

/// A desired replica remains in the pool while it is connecting or reconnecting.
/// Tasks hold only weak references to their slots. Removing a slot or dropping
/// the pool aborts its task, including an incomplete transport handshake.
#[derive(Debug)]
pub(crate) struct QueryReplicaConnections {
    config: Arc<QueryReplicaConnectionsConfig>,
    replicas: Mutex<BTreeMap<ReplicaKey, Replica>>,
    changed: watch::Sender<()>,
}

impl QueryReplicaConnections {
    /// Coalesced topology and connection changes, without retaining a connection.
    pub(crate) fn changes(&self) -> watch::Receiver<()> {
        self.changed.subscribe()
    }

    pub(crate) fn new(config: QueryReplicaConnectionsConfig) -> Self {
        Self {
            config: Arc::new(config),
            replicas: Mutex::new(BTreeMap::new()),
            changed: watch::channel(()).0,
        }
    }

    /// Reconcile a current committed catalog snapshot without waiting for any
    /// replica. Call in catalog-application order, including system config changes.
    pub(crate) fn sync_catalog(self: &Arc<Self>, catalog: &Catalog) {
        let vars = catalog.system_config();
        let settings = Settings {
            connect_timeout: vars.grpc_connect_timeout(),
            keepalive_timeout: vars.grpc_client_http2_keep_alive_timeout(),
            max_result_size: usize::cast_from(vars.max_result_size()),
        };
        let desired = catalog
            .clusters()
            .flat_map(|cluster| {
                cluster.replicas().map(move |replica| {
                    let key = (cluster.id, replica.replica_id);
                    let endpoint = match &replica.config.location {
                        ReplicaLocation::Unmanaged(location) => {
                            Endpoint::Unmanaged(location.computectl_addrs.clone())
                        }
                        ReplicaLocation::Managed(location) => Endpoint::Managed {
                            service: ReplicaServiceName {
                                cluster_id: cluster.id,
                                replica_id: replica.replica_id,
                                generation: self.config.deploy_generation,
                            }
                            .to_string(),
                            scale: location.allocation.scale,
                        },
                    };
                    (key, endpoint)
                })
            })
            .collect();
        self.reconcile(desired, settings);
    }

    fn reconcile(&self, desired: BTreeMap<ReplicaKey, Endpoint>, settings: Settings) {
        let mut replicas = self.replicas.lock().expect("query replicas mutex poisoned");
        replicas.retain(|key, replica| desired.get(key) == Some(&replica.endpoint));
        for (key, endpoint) in desired {
            let replica = replicas.entry(key).or_insert_with(|| {
                let config = Arc::clone(&self.config);
                let changed = self.changed.clone();
                let state = Arc::new(Mutex::new(ReplicaState {
                    settings,
                    client: None,
                }));
                let (settings_changed, rx) = watch::channel(());
                let task = mz_ore::task::spawn(
                    || format!("query-replica-{}-{}", key.0, key.1),
                    run(
                        Arc::downgrade(&state),
                        key,
                        endpoint.clone(),
                        config,
                        changed,
                        rx,
                    ),
                )
                .abort_on_drop();
                Replica {
                    endpoint,
                    state,
                    settings_changed,
                    _task: task,
                }
            });
            let mut state = replica.state.lock().expect("query replica mutex poisoned");
            if state.settings != settings {
                state.settings = settings;
                replica.settings_changed.send_replace(());
            }
        }
        self.changed.send_replace(());
    }

    /// Return ready connections only. An empty result says nothing about desired
    /// replicas. Planning may use their wire frontier caches, never as read holds.
    pub(crate) fn ready_clients(
        &self,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
    ) -> Vec<ReplicaQueryClient> {
        self.ready_snapshot(cluster, target).1
    }

    /// Wait for at least one matching replica, not every replica in the cluster.
    /// Absence is an error, but a desired replica awaiting QueryReady is not.
    /// Dropping this future cancels the wait without affecting connections.
    pub(crate) async fn clients(
        &self,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
    ) -> Result<Vec<ReplicaQueryClient>, AdapterError> {
        let mut changed = self.changed.subscribe();
        loop {
            let (desired, clients) = self.ready_snapshot(cluster, target);
            if desired == 0 {
                return Err(AdapterError::Unstructured(anyhow::anyhow!(
                    "no desired query replica for cluster {cluster}, target {target:?}"
                )));
            }
            if !clients.is_empty() {
                return Ok(clients);
            }
            changed.changed().await.expect("pool owns change sender");
        }
    }

    pub(crate) fn ready_snapshot(
        &self,
        cluster: ComputeInstanceId,
        target: Option<ReplicaId>,
    ) -> (usize, Vec<ReplicaQueryClient>) {
        let replicas = self.replicas.lock().expect("query replicas mutex poisoned");
        let mut desired = 0;
        let mut clients = Vec::new();
        for ((id, replica_id), replica) in replicas.iter() {
            if *id != cluster || target.is_some_and(|target| target != *replica_id) {
                continue;
            }
            desired += 1;
            let mut state = replica.state.lock().expect("query replica mutex poisoned");
            if let Some(client) = &state.client {
                if client.is_connected() {
                    clients.push(client.clone());
                } else {
                    state.client = None;
                    self.changed.send_replace(());
                }
            }
        }
        (desired, clients)
    }
}

async fn run(
    replica: Weak<Mutex<ReplicaState>>,
    key: ReplicaKey,
    endpoint: Endpoint,
    config: Arc<QueryReplicaConnectionsConfig>,
    changed: watch::Sender<()>,
    mut settings_changed: watch::Receiver<()>,
) {
    let mut backoff = Duration::from_millis(100);
    loop {
        let Some(settings) = replica.upgrade().map(|replica| {
            replica
                .lock()
                .expect("query replica mutex poisoned")
                .settings
        }) else {
            return;
        };
        let result = async {
            let addresses = match &endpoint {
                Endpoint::Unmanaged(addresses) => addresses.clone(),
                Endpoint::Managed { service, scale } => config.orchestrator.service_addresses(
                    service,
                    *scale,
                    &ServicePort {
                        name: "computectl".into(),
                        // Production computectl port, shared with lifecycle provisioning.
                        port_hint: 2101,
                    },
                )?,
            };
            anyhow::ensure!(!addresses.is_empty(), "replica has no computectl addresses");
            // Native client metrics are controller-owned ReplicaMetrics, with
            // lifecycle command accounting. Do not share that state. CTP server
            // metrics still account for this connection's traffic.
            let transport = Client::<ComputeCommand, ComputeResponse>::connect_partitioned(
                addresses,
                config.build_info.semver_version(),
                settings.connect_timeout,
                settings.keepalive_timeout,
                NoopMetrics,
            )
            .await?;
            let client =
                ReplicaQueryClient::connect(Box::new(transport), settings.max_result_size).await?;
            let mut frontiers = client.frontier_changes();
            let mut applied_limit = settings.max_result_size;
            loop {
                // Never retain a strong slot reference across an await. Publication
                // and catalog updates use the same lock, so an old handshake cannot
                // publish a connection with an obsolete result ceiling.
                let Some(limit) = replica.upgrade().map(|replica| {
                    let mut state = replica.lock().expect("query replica mutex poisoned");
                    let limit = state.settings.max_result_size;
                    if limit == applied_limit && state.client.is_none() {
                        state.client = Some(client.clone());
                        changed.send_replace(());
                    }
                    limit
                }) else {
                    return Ok::<(), anyhow::Error>(());
                };
                if limit != applied_limit {
                    client.set_max_result_size(limit).await?;
                    applied_limit = limit;
                    continue;
                }
                client.frontiers()?;
                tokio::select! {
                    result = frontiers.changed() => { result?; }
                    result = settings_changed.changed() => { result?; }
                }
            }
        }
        .await;
        let Some(slot) = replica.upgrade() else {
            return;
        };
        slot.lock().expect("query replica mutex poisoned").client = None;
        changed.send_replace(());
        drop(slot);
        if let Err(error) = result {
            tracing::warn!(cluster = %key.0, replica = %key.1, ?backoff,
                "query replica connection failed: {error:#}");
        }
        // Reconnect only the transport. Failed query work is never replayed.
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(1));
    }
}
