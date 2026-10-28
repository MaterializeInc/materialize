// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use mz_catalog::catalog::{Catalog, CatalogError, Op};
use mz_catalog::durable::{AdapterEndpoint, AdapterRuntime};
use mz_orchestrator::{ServiceEvent, ServiceStatus};
use mz_server_core::listeners::{AllowedRoles, AuthenticatorKind};

use super::provisioning::{AdapterAllocation, AdapterProvisioningConfig};
use super::{AdapterInventoryEntry, ReplicaInventoryEntry};

/// Last observed SQL readiness, not a promise about a future connection.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Readiness {
    pub ready: Option<bool>,
    pub error: Option<String>,
    /// Unix milliseconds of the last change, not a periodic heartbeat.
    pub changed_at: i64,
    #[serde(skip)]
    target: Option<ProbeTarget>,
}

impl Readiness {
    fn from_runtime(runtime: &AdapterRuntime) -> Option<Self> {
        Some(Self {
            ready: runtime.ready,
            error: runtime.readiness_error.clone(),
            changed_at: runtime.readiness_changed_at?,
            target: None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProbeTarget {
    url: String,
    user: &'static str,
    process_revision: Option<u64>,
}

pub(super) struct ProbeResult {
    service: String,
    target: ProbeTarget,
    ready: Option<bool>,
    error: Option<String>,
}

pub(super) struct Observations {
    pub(super) adapters: BTreeMap<String, AdapterInventoryEntry>,
    pub(super) replicas: BTreeMap<String, ReplicaInventoryEntry>,
    processes: BTreeMap<(String, u64), ServiceEvent>,
    process_revisions: BTreeMap<(String, u64), u64>,
    readiness: BTreeMap<String, Readiness>,
    pending: BTreeSet<String>,
    pub(super) probes: FuturesUnordered<BoxFuture<'static, ProbeResult>>,
    client: reqwest::Client,
    permits: Arc<tokio::sync::Semaphore>,
}

impl Observations {
    pub(super) fn record(&mut self, event: Result<ServiceEvent, anyhow::Error>) {
        match event {
            Ok(event) => {
                let key = (event.service_id.clone(), event.process_id);
                let changed = self.processes.get(&key).is_none_or(|previous| {
                    previous.status != event.status || previous.restart_count != event.restart_count
                });
                if changed {
                    // Restart counts can reset and timestamps can repeat. Count
                    // observed transitions rather than treating those values as an
                    // incarnation. Kubernetes also replays unchanged status with
                    // fresh probe times, which must not invalidate readiness.
                    // This revision fences only this owner's outstanding futures.
                    let revision = self.process_revisions.entry(key.clone()).or_default();
                    *revision = revision
                        .checked_add(1)
                        .expect("process observation revision exhausted");
                }
                if self.adapters.contains_key(&event.service_id)
                    && self.processes.get(&key).is_some_and(|previous| {
                        previous.status != event.status
                            || previous.restart_count != event.restart_count
                    })
                {
                    // A readiness sample from the previous process is not a
                    // sample of its replacement. Unknown is not an offline claim.
                    self.readiness.insert(
                        event.service_id.clone(),
                        Readiness {
                            ready: None,
                            error: Some("awaiting readiness observation".into()),
                            changed_at: event.time.timestamp_millis(),
                            target: None,
                        },
                    );
                }
                self.processes.insert(key, event);
            }
            Err(error) => tracing::warn!(%error, "controller process observation failed"),
        }
    }

    fn processes(&self, service: &str) -> Vec<ServiceEvent> {
        self.processes
            .iter()
            .filter(|((id, _), _)| id == service)
            .map(|(_, event)| event.clone())
            .collect()
    }

    pub(super) fn adapters(
        &self,
        catalog: &Catalog,
        launch: &AdapterProvisioningConfig,
        sizes: &BTreeMap<String, AdapterAllocation>,
    ) -> Vec<AdapterInventoryEntry> {
        let mut desired = super::desired_adapters(catalog, launch, sizes);
        for entry in &mut desired {
            if let Some(observed) = self.adapters.get(&entry.service) {
                entry.endpoints = observed.endpoints.clone();
                if entry.provisioning_error.is_none() {
                    entry.provisioning_error = observed.provisioning_error.clone();
                }
            }
            entry.observed_processes = self.processes(&entry.service);
            entry.readiness = self.readiness.get(&entry.service).cloned().or_else(|| {
                catalog
                    .state()
                    .get_adapter_runtime(entry.id, entry.generation)
                    .and_then(Readiness::from_runtime)
            });
        }
        desired
    }

    pub(super) fn replicas(
        &self,
        catalog: &Catalog,
        generation: u64,
    ) -> Vec<ReplicaInventoryEntry> {
        let mut desired = super::desired_replicas(catalog, generation);
        for entry in &mut desired {
            if let Some(observed) = self.replicas.get(&entry.service) {
                entry.endpoints = observed.endpoints.clone();
                entry.provisioning_error = observed.provisioning_error.clone();
            }
            entry.observed_processes = self.processes(&entry.service);
        }
        desired
    }
}

impl Observations {
    pub(super) fn new() -> anyhow::Result<Self> {
        Ok(Self {
            adapters: BTreeMap::new(),
            replicas: BTreeMap::new(),
            processes: BTreeMap::new(),
            process_revisions: BTreeMap::new(),
            readiness: BTreeMap::new(),
            pending: BTreeSet::new(),
            probes: FuturesUnordered::new(),
            client: reqwest::Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
            permits: Arc::new(tokio::sync::Semaphore::new(16)),
        })
    }

    pub(super) fn retain(&mut self, desired: &BTreeSet<String>) {
        self.processes
            .retain(|(service, _), _| desired.contains(service));
        self.readiness
            .retain(|service, _| desired.contains(service));
        self.process_revisions
            .retain(|(service, _), _| desired.contains(service));
    }

    fn target(
        &self,
        entry: &AdapterInventoryEntry,
        launch: &AdapterProvisioningConfig,
    ) -> Option<ProbeTarget> {
        // Probe existing unauthenticated health routes. Do not change listener
        // policy, acquire credentials, follow redirects or bypass TLS verification.
        launch
            .listeners
            .http
            .iter()
            .filter(|(_, config)| {
                config.authenticator_kind == AuthenticatorKind::None
                    && config.routes.metrics.is_enabled()
            })
            .filter_map(|(name, config)| {
                let port = launch.http_port_name(name)?;
                let address = entry.endpoints.get(&port)?.first()?;
                let user = match config.routes.metrics.allowed_roles()? {
                    AllowedRoles::Internal => "mz_system",
                    AllowedRoles::Normal | AllowedRoles::NormalAndInternal => "materialize",
                };
                let scheme = if config.enable_tls { "https" } else { "http" };
                let process_revision = self
                    .process_revisions
                    .get(&(entry.service.clone(), 0))
                    .copied();
                Some((
                    config.enable_tls,
                    ProbeTarget {
                        url: format!("{scheme}://{address}/api/readyz"),
                        user,
                        process_revision,
                    },
                ))
            })
            .min_by_key(|(tls, _)| *tls)
            .map(|(_, target)| target)
    }

    pub(super) fn seed_readiness(&mut self, catalog: &Catalog) {
        for entry in self.adapters.values() {
            if !self.readiness.contains_key(&entry.service) {
                if let Some(readiness) = catalog
                    .state()
                    .get_adapter_runtime(entry.id, entry.generation)
                    .and_then(Readiness::from_runtime)
                {
                    self.readiness.insert(entry.service.clone(), readiness);
                }
            }
        }
    }

    pub(super) fn start_probes(&mut self, launch: &AdapterProvisioningConfig) {
        for entry in self.adapters.values().cloned().collect::<Vec<_>>() {
            let Some(target) = self.target(&entry, launch) else {
                self.set_readiness(
                    &entry.service,
                    None,
                    Some("no observable readiness endpoint".into()),
                    None,
                );
                continue;
            };
            if self
                .readiness
                .get(&entry.service)
                .and_then(|value| value.target.as_ref())
                != Some(&target)
            {
                // Do not attach an earlier sample to newly assigned endpoints or
                // to a process this owner has not yet probed.
                self.set_readiness(
                    &entry.service,
                    None,
                    Some("awaiting readiness observation".into()),
                    Some(target.clone()),
                );
            }
            if self.pending.contains(&entry.service) {
                continue;
            }
            self.pending.insert(entry.service.clone());
            let client = self.client.clone();
            let permits = Arc::clone(&self.permits);
            // One queued request per declaration. Semaphore fairness prevents a
            // large or unreachable fleet from monopolizing sockets or the owner loop.
            self.probes.push(
                async move {
                    let _permit = permits
                        .acquire_owned()
                        .await
                        .expect("probe semaphore stays open");
                    let response = client
                        .get(&target.url)
                        .basic_auth(target.user, None::<&str>)
                        .send()
                        .await;
                    let (ready, error) = match response {
                        Ok(response) if response.status() == reqwest::StatusCode::OK => {
                            (Some(true), None)
                        }
                        Ok(response)
                            if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE =>
                        {
                            (Some(false), None)
                        }
                        Ok(_) => (None, Some("readiness endpoint unavailable".into())),
                        Err(_) => (None, Some("readiness probe failed".into())),
                    };
                    ProbeResult {
                        service: entry.service,
                        target,
                        ready,
                        error,
                    }
                }
                .boxed(),
            );
        }
    }

    pub(super) fn complete_probe(
        &mut self,
        result: ProbeResult,
        launch: &AdapterProvisioningConfig,
    ) {
        self.pending.remove(&result.service);
        let Some(entry) = self.adapters.get(&result.service).cloned() else {
            return;
        };
        // A late response from an old endpoint or observed process incarnation
        // must not claim that its replacement is ready.
        if self.target(&entry, launch).as_ref() != Some(&result.target) {
            return;
        }
        self.set_readiness(
            &entry.service,
            result.ready,
            result.error,
            Some(result.target),
        );
    }

    fn set_readiness(
        &mut self,
        service: &str,
        ready: Option<bool>,
        error: Option<String>,
        target: Option<ProbeTarget>,
    ) {
        let previous = self.readiness.get(service);
        let changed_at = match previous {
            Some(previous) if previous.ready == ready && previous.error == error => {
                previous.changed_at
            }
            _ => chrono::Utc::now().timestamp_millis(),
        };
        self.readiness.insert(
            service.to_owned(),
            Readiness {
                ready,
                error,
                changed_at,
                target,
            },
        );
    }

    pub(super) async fn publish(
        &self,
        catalog: &mut Catalog,
        launch: &AdapterProvisioningConfig,
        sizes: &BTreeMap<String, AdapterAllocation>,
    ) -> Result<(), CatalogError> {
        let mut ops = Vec::new();
        for entry in self.adapters(catalog, launch, sizes) {
            let previous = catalog
                .state()
                .get_adapter_runtime(entry.id, entry.generation);
            let runtime = adapter_runtime(&entry, launch, previous);
            if Some(&runtime) != previous {
                ops.push(Op::UpdateAdapterRuntime { runtime });
            }
        }
        // Even an empty catalog transaction advances its upper and wakes peers.
        if !ops.is_empty() {
            catalog
                .transact(None, None, mz_repr::Timestamp::MIN, None, ops)
                .await?;
        }
        Ok(())
    }
}

fn adapter_runtime(
    entry: &AdapterInventoryEntry,
    launch: &AdapterProvisioningConfig,
    previous: Option<&AdapterRuntime>,
) -> AdapterRuntime {
    let mut runtime = previous.cloned().unwrap_or_else(|| AdapterRuntime {
        adapter_id: entry.id,
        deployment_generation: entry.generation,
        endpoints: Vec::new(),
        status: None,
        reason: None,
        restart_count: None,
        status_changed_at: None,
        ready: None,
        readiness_error: None,
        readiness_changed_at: None,
        provisioning_error: None,
    });
    runtime.endpoints.clear();
    for (name, port, kind) in launch
        .listeners
        .sql
        .keys()
        .map(|name| (name, launch.sql_port_name(name), "sql"))
        .chain(
            launch
                .listeners
                .http
                .keys()
                .map(|name| (name, launch.http_port_name(name), "http")),
        )
    {
        if let Some(addresses) = port.and_then(|port| entry.endpoints.get(&port)) {
            for address in addresses {
                runtime.endpoints.push(AdapterEndpoint {
                    listener: name.clone(),
                    kind: kind.into(),
                    address: address.clone(),
                });
            }
        }
    }
    runtime.provisioning_error = entry.provisioning_error.clone();
    // Adapters have exactly one process. Absence of an event is not a transition.
    if let Some(event) = entry.observed_processes.first() {
        let status = Some(event.status.as_kebab_case_str().to_owned());
        let reason = match event.status {
            ServiceStatus::Offline(reason) => reason.map(|reason| reason.to_string()),
            ServiceStatus::Online => None,
        };
        if runtime.status != status || runtime.reason != reason {
            runtime.status_changed_at = Some(event.time.timestamp_millis());
        }
        runtime.status = status;
        runtime.reason = reason;
        runtime.restart_count = Some(event.restart_count);
    }
    if let Some(readiness) = &entry.readiness {
        runtime.ready = readiness.ready;
        runtime.readiness_error = readiness.error.clone();
        runtime.readiness_changed_at = Some(readiness.changed_at);
    }
    runtime
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (AdapterInventoryEntry, AdapterProvisioningConfig) {
        let launch = AdapterProvisioningConfig {
            image: "adapterd".into(),
            init_container_image: None,
            args: Vec::new(),
            listeners: serde_json::from_str(include_str!(
                "../../../materialized/ci/listener_configs/v26_32_0/no_auth.json"
            ))
            .unwrap(),
            generation: 4,
        };
        let entry = AdapterInventoryEntry {
            id: "a7".parse().unwrap(),
            name: "peer".into(),
            size: "1".into(),
            generation: 4,
            service: "adapter-a7-gen-4".into(),
            resources: None,
            endpoints: BTreeMap::from([("sql-0".into(), vec!["127.0.0.1:1234".into()])]),
            observed_processes: Vec::new(),
            readiness: None,
            provisioning_error: None,
        };
        (entry, launch)
    }

    #[mz_ore::test(tokio::test)]
    async fn readiness_results_follow_the_observed_target() {
        let (mut entry, launch) = fixture();
        let port = launch.http_port_name("internal").unwrap();
        entry
            .endpoints
            .insert(port.clone(), vec!["127.0.0.1:1235".into()]);
        let service = entry.service.clone();
        let event = |status, time| {
            Ok(ServiceEvent {
                service_id: service.clone(),
                process_id: 0,
                status,
                restart_count: 0,
                time: chrono::DateTime::from_timestamp_millis(time).unwrap(),
            })
        };
        let result = |target| ProbeResult {
            service: service.clone(),
            target,
            ready: Some(true),
            error: None,
        };
        let mut observations = Observations::new().unwrap();
        observations.adapters.insert(service.clone(), entry.clone());
        observations.record(event(ServiceStatus::Online, 100));
        let original = observations.target(&entry, &launch).unwrap();
        observations.complete_probe(result(original.clone()), &launch);
        assert_eq!(observations.readiness[&service].ready, Some(true));

        // Both timestamp and restart count can repeat across observed transitions.
        observations.record(event(ServiceStatus::Offline(None), 100));
        observations.record(event(ServiceStatus::Online, 100));
        observations.complete_probe(result(original), &launch);
        assert_eq!(observations.readiness[&service].ready, None);
        let current = observations.target(&entry, &launch).unwrap();
        observations.complete_probe(result(current.clone()), &launch);
        observations.readiness.get_mut(&service).unwrap().changed_at = 123;

        // Kubernetes can re-list the same state with a fresh last-probe time.
        observations.record(event(ServiceStatus::Online, 200));
        observations.complete_probe(result(current.clone()), &launch);
        assert_eq!(observations.readiness[&service].ready, Some(true));
        assert_eq!(observations.readiness[&service].changed_at, 123);

        entry.endpoints.insert(port, vec!["127.0.0.1:1236".into()]);
        observations.adapters.insert(service.clone(), entry.clone());
        observations.start_probes(&launch);
        observations.complete_probe(result(current), &launch);
        assert_eq!(observations.readiness[&service].ready, None);
        let replacement = observations.target(&entry, &launch).unwrap();
        observations.complete_probe(result(replacement.clone()), &launch);
        assert_eq!(observations.readiness[&service].ready, Some(true));

        observations.adapters.clear();
        observations.retain(&BTreeSet::new());
        observations.complete_probe(result(replacement), &launch);
        assert!(!observations.readiness.contains_key(&service));
    }

    #[mz_ore::test]
    fn endpoints_do_not_claim_readiness() {
        let (entry, launch) = fixture();
        let runtime = adapter_runtime(&entry, &launch, None);
        assert_eq!(
            runtime.endpoints,
            vec![AdapterEndpoint {
                listener: "external".into(),
                kind: "sql".into(),
                address: "127.0.0.1:1234".into(),
            }]
        );
        assert_eq!(runtime.adapter_id, entry.id);
        assert_eq!(runtime.deployment_generation, 4);
        assert_eq!(runtime.status, None);
        assert_eq!(runtime.ready, None);
        assert_eq!(runtime.status_changed_at, None);
        assert_eq!(runtime.readiness_changed_at, None);
    }

    #[mz_ore::test]
    fn replay_and_restart_counts_preserve_transition_time() {
        let (mut entry, launch) = fixture();
        entry.observed_processes.push(ServiceEvent {
            service_id: entry.service.clone(),
            process_id: 0,
            status: ServiceStatus::Online,
            restart_count: 0,
            time: chrono::DateTime::from_timestamp_millis(100).unwrap(),
        });
        entry.readiness = Some(Readiness {
            ready: Some(true),
            error: None,
            changed_at: 200,
            target: None,
        });
        let initial = adapter_runtime(&entry, &launch, None);
        assert_eq!(adapter_runtime(&entry, &launch, Some(&initial)), initial);
        entry.observed_processes[0].restart_count = 1;
        entry.observed_processes[0].time = chrono::DateTime::from_timestamp_millis(300).unwrap();
        let restarted = adapter_runtime(&entry, &launch, Some(&initial));
        assert_eq!(restarted.restart_count, Some(1));
        assert_eq!(restarted.status_changed_at, Some(100));
        entry.observed_processes[0].status =
            ServiceStatus::Offline(Some(mz_orchestrator::OfflineReason::Initializing));
        let offline = adapter_runtime(&entry, &launch, Some(&restarted));
        assert_eq!(offline.status.as_deref(), Some("offline"));
        assert_eq!(offline.reason.as_deref(), Some("initializing"));
        assert_eq!(offline.status_changed_at, Some(300));
        entry.observed_processes.clear();
        entry.readiness = None;
        assert_eq!(adapter_runtime(&entry, &launch, Some(&offline)), offline);
    }
}
