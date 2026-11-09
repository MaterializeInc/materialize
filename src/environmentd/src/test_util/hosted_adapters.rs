// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fixture-owned adapters behind the controller's existing orchestrator boundary.
//!
//! The fixture owns the actual adapter server and must drop it before its lease.
//! Deleting a service revokes its lease and endpoints, but cannot kill the hosting
//! process. Administrative process-death semantics belong in real subprocess
//! fixtures. This backend does not supervise processes, interpret launch arguments,
//! or reconfigure test hooks. Readiness still comes from probing actual listeners.
//! Services must have scale one and retain their port names until dropped. Other
//! launch and scheduling settings are left to the fixture, not applied here.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZero;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, ensure};
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use futures::channel::mpsc;
use futures::stream::BoxStream;
use mz_orchestrator::scheduling_config::ServiceSchedulingConfig;
use mz_orchestrator::{
    NamespacedOrchestrator, Orchestrator, Service, ServiceConfig, ServiceEvent, ServicePort,
    ServiceProcessMetrics, ServiceStatus,
};
use tokio::sync::Notify;

#[derive(Debug)]
pub(super) struct HostedOrchestrator {
    fallback: Arc<dyn Orchestrator>,
    adapters: Arc<HostedAdapters>,
}

impl HostedOrchestrator {
    pub(super) fn new(fallback: Arc<dyn Orchestrator>, adapters: Arc<HostedAdapters>) -> Self {
        Self { fallback, adapters }
    }
}

impl Orchestrator for HostedOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        if namespace == "adapter" {
            self.adapters.clone()
        } else {
            self.fallback.namespace(namespace)
        }
    }
}

#[derive(Debug, Default)]
pub(super) struct HostedAdapters {
    state: Arc<Mutex<State>>,
    registered: Notify,
}

#[derive(Debug, Default)]
struct State {
    services: BTreeMap<String, DesiredService>,
    watchers: Vec<mpsc::UnboundedSender<ServiceEvent>>,
}

impl State {
    fn emit(&mut self, event: ServiceEvent) {
        self.watchers
            .retain(|watcher| watcher.unbounded_send(event.clone()).is_ok());
    }
}

#[derive(Debug)]
struct DesiredService {
    identity: Arc<()>,
    ports: BTreeSet<String>,
    claim: Option<Arc<()>>,
    addresses: Option<BTreeMap<String, String>>,
    event: ServiceEvent,
}

impl HostedAdapters {
    pub(super) fn new() -> Self {
        Self::default()
    }

    /// Waits for controller registration, then exclusively claims the service.
    /// The caller must also wait for controller exit to detect startup failure.
    /// Cancellation before a successful return leaves no claim behind.
    pub(super) async fn claim(self: &Arc<Self>, id: &str) -> anyhow::Result<HostedLease> {
        loop {
            let registered = self.registered.notified();
            tokio::pin!(registered);
            // Subscribe before checking state so registration cannot be missed.
            registered.as_mut().enable();
            {
                let mut state = self.state.lock().expect("hosted adapters lock poisoned");
                if let Some(service) = state.services.get_mut(id) {
                    ensure!(
                        service.claim.is_none(),
                        "hosted adapter {id} already claimed"
                    );
                    let claim = Arc::new(());
                    service.claim = Some(Arc::clone(&claim));
                    return Ok(HostedLease {
                        adapters: Arc::clone(self),
                        id: id.to_owned(),
                        claim,
                    });
                }
            }
            registered.await;
        }
    }
}

/// Exclusive ownership of one fixture-hosted adapter incarnation.
#[derive(Debug)]
pub(super) struct HostedLease {
    adapters: Arc<HostedAdapters>,
    id: String,
    claim: Arc<()>,
}

impl HostedLease {
    /// Publishes actual bound listeners once, with exactly the configured ports.
    /// A dropped service revokes this lease even if its ID is later recreated.
    pub(super) fn attach(&self, addresses: BTreeMap<String, String>) -> anyhow::Result<()> {
        let mut state = self
            .adapters
            .state
            .lock()
            .expect("hosted adapters lock poisoned");
        let service = state
            .services
            .get_mut(&self.id)
            .filter(|service| self.owns(service))
            .ok_or_else(|| anyhow!("hosted adapter {} lease revoked", self.id))?;
        ensure!(
            service.ports.iter().eq(addresses.keys()),
            "hosted adapter {} requires exactly ports {:?}",
            self.id,
            service.ports
        );
        ensure!(
            service.addresses.is_none(),
            "hosted adapter already attached"
        );
        service.addresses = Some(addresses);
        service.event = event(&self.id, ServiceStatus::Online);
        let event = service.event.clone();
        state.emit(event);
        Ok(())
    }

    fn owns(&self, service: &DesiredService) -> bool {
        service
            .claim
            .as_ref()
            .is_some_and(|claim| Arc::ptr_eq(claim, &self.claim))
    }
}

impl Drop for HostedLease {
    fn drop(&mut self) {
        let mut state = self
            .adapters
            .state
            .lock()
            .expect("hosted adapters lock poisoned");
        if let Some(service) = state.services.get_mut(&self.id).filter(|s| self.owns(s)) {
            service.claim = None;
            service.addresses = None;
            service.event = event(&self.id, ServiceStatus::Offline(None));
            let event = service.event.clone();
            state.emit(event);
        }
    }
}

#[derive(Debug)]
struct HostedService {
    state: Arc<Mutex<State>>,
    id: String,
    identity: Arc<()>,
    ports: BTreeSet<String>,
}

impl Service for HostedService {
    fn addresses(&self, port: &str) -> Vec<String> {
        assert!(
            self.ports.contains(port),
            "unknown hosted adapter port {port}"
        );
        let state = self.state.lock().expect("hosted adapters lock poisoned");
        state
            .services
            .get(&self.id)
            .filter(|service| Arc::ptr_eq(&service.identity, &self.identity))
            .and_then(|service| service.addresses.as_ref()?.get(port))
            .cloned()
            .into_iter()
            .collect()
    }
}

fn event(id: &str, status: ServiceStatus) -> ServiceEvent {
    ServiceEvent {
        service_id: id.to_owned(),
        process_id: 0,
        status,
        restart_count: 0,
        time: Utc::now(),
    }
}

#[async_trait]
impl NamespacedOrchestrator for HostedAdapters {
    fn service_addresses(
        &self,
        id: &str,
        scale: NonZero<u16>,
        port: &ServicePort,
    ) -> anyhow::Result<Vec<String>> {
        ensure!(scale.get() == 1, "hosted adapters require scale 1");
        let state = self.state.lock().expect("hosted adapters lock poisoned");
        let Some(service) = state.services.get(id) else {
            return Ok(Vec::new());
        };
        ensure!(
            service.ports.contains(&port.name),
            "unknown hosted adapter port"
        );
        Ok(service
            .addresses
            .as_ref()
            .and_then(|addresses| addresses.get(&port.name))
            .cloned()
            .into_iter()
            .collect())
    }

    fn ensure_service(&self, id: &str, config: ServiceConfig) -> anyhow::Result<Box<dyn Service>> {
        ensure!(config.scale.get() == 1, "hosted adapters require scale 1");
        let ports: BTreeSet<_> = config.ports.iter().map(|port| port.name.clone()).collect();
        ensure!(
            ports.len() == config.ports.len(),
            "duplicate hosted adapter ports"
        );
        let mut state = self.state.lock().expect("hosted adapters lock poisoned");
        if let Some(service) = state.services.get(id) {
            // Only the fixture can change its listeners. Never pretend that argv
            // or resource changes reconfigure the in-process server or its hooks.
            ensure!(
                service.ports == ports,
                "drop hosted adapter before changing ports"
            );
        } else {
            let event = event(id, ServiceStatus::Offline(None));
            state.services.insert(
                id.to_owned(),
                DesiredService {
                    identity: Arc::new(()),
                    ports: ports.clone(),
                    claim: None,
                    addresses: None,
                    event: event.clone(),
                },
            );
            state.emit(event);
            self.registered.notify_waiters();
        }
        Ok(Box::new(HostedService {
            state: Arc::clone(&self.state),
            id: id.to_owned(),
            identity: Arc::clone(&state.services[id].identity),
            ports,
        }))
    }

    fn drop_service(&self, id: &str) -> anyhow::Result<()> {
        let mut state = self.state.lock().expect("hosted adapters lock poisoned");
        if state.services.remove(id).is_some() {
            state.emit(event(id, ServiceStatus::Offline(None)));
        }
        Ok(())
    }

    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        let state = self.state.lock().expect("hosted adapters lock poisoned");
        Ok(state.services.keys().cloned().collect())
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn watch_services(&self) -> BoxStream<'static, anyhow::Result<ServiceEvent>> {
        let (sender, receiver) = mpsc::unbounded();
        let mut state = self.state.lock().expect("hosted adapters lock poisoned");
        // Snapshot and subscription share the mutation lock. Queue every event,
        // including transitions before the caller first polls the stream.
        for service in state.services.values() {
            sender
                .unbounded_send(service.event.clone())
                .expect("new watch receiver is open");
        }
        state.watchers.retain(|watcher| !watcher.is_closed());
        state.watchers.push(sender);
        receiver.map(Ok).boxed()
    }

    async fn fetch_service_metrics(&self, id: &str) -> anyhow::Result<Vec<ServiceProcessMetrics>> {
        let state = self.state.lock().expect("hosted adapters lock poisoned");
        ensure!(
            state.services.contains_key(id),
            "unknown hosted adapter {id}"
        );
        // Per-adapter resource usage is unavailable inside the shared process.
        Ok(vec![ServiceProcessMetrics::default()])
    }

    fn update_scheduling_config(&self, _config: ServiceSchedulingConfig) {}
}

#[cfg(test)]
mod tests {
    use futures::FutureExt;

    use super::*;

    fn config() -> ServiceConfig {
        ServiceConfig {
            inherit_kubernetes_owner_references: false,
            process_restart_on_image_change: false,
            app_name: "adapter-test".into(),
            image: "unused".into(),
            init_container_image: None,
            args: Box::new(|_| panic!("hosted adapters must not evaluate argv")),
            ports: ["query", "health"]
                .map(|name| ServicePort {
                    name: name.into(),
                    port_hint: 0,
                    tcp_listen_ip: Some(std::net::Ipv4Addr::LOCALHOST.into()),
                })
                .to_vec(),
            memory_limit: None,
            memory_request: None,
            cpu_limit: None,
            cpu_request: None,
            scale: NonZero::new(1).unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            availability_zones: None,
            other_replicas_selector: Vec::new(),
            replicas_selector: Vec::new(),
            disk_limit: None,
            node_selector: BTreeMap::new(),
        }
    }

    fn addresses() -> BTreeMap<String, String> {
        BTreeMap::from([
            ("query".into(), "127.0.0.1:1234".into()),
            ("health".into(), "127.0.0.1:5678".into()),
        ])
    }

    fn expect_event(
        watch: &mut BoxStream<'static, anyhow::Result<ServiceEvent>>,
        status: ServiceStatus,
    ) {
        let event = watch.next().now_or_never().unwrap().unwrap().unwrap();
        assert_eq!(event.service_id, "adapter");
        assert_eq!(event.process_id, 0);
        assert_eq!(event.status, status);
        assert_eq!(event.restart_count, 0);
    }

    #[mz_ore::test(tokio::test)]
    async fn claim_transfer_and_revocation() {
        let adapters = Arc::new(HostedAdapters::new());
        let claim = adapters.claim("adapter");
        let competing = adapters.claim("adapter");
        tokio::pin!(claim);
        tokio::pin!(competing);
        assert!(futures::poll!(claim.as_mut()).is_pending());
        assert!(futures::poll!(competing.as_mut()).is_pending());
        let original = adapters.ensure_service("adapter", config()).unwrap();
        let (first, second) = futures::join!(claim, competing);
        let lease = match (first, second) {
            (Ok(lease), Err(_)) | (Err(_), Ok(lease)) => lease,
            results => panic!("expected exactly one successful claim: {results:?}"),
        };
        assert!(adapters.claim("adapter").await.is_err());
        lease.attach(addresses()).unwrap();
        drop(lease);
        assert!(original.addresses("query").is_empty());

        let stale = adapters.claim("adapter").await.unwrap();
        stale.attach(addresses()).unwrap();
        adapters.drop_service("adapter").unwrap();
        assert!(stale.attach(addresses()).is_err());
        let replacement = adapters.ensure_service("adapter", config()).unwrap();
        let current = adapters.claim("adapter").await.unwrap();
        assert!(stale.attach(addresses()).is_err());
        assert!(replacement.addresses("query").is_empty());
        current.attach(addresses()).unwrap();
        assert!(stale.attach(addresses()).is_err());
        let mut watch = adapters.watch_services();
        expect_event(&mut watch, ServiceStatus::Online);
        drop(stale);
        assert!(watch.next().now_or_never().is_none());
        assert!(adapters.claim("adapter").await.is_err());
        assert!(original.addresses("query").is_empty());
        assert_eq!(
            replacement.addresses("query"),
            vec![addresses()["query"].clone()]
        );
        drop(current);
        expect_event(&mut watch, ServiceStatus::Offline(None));
        assert!(replacement.addresses("query").is_empty());
        assert!(adapters.claim("adapter").await.is_ok());
    }

    #[mz_ore::test(tokio::test)]
    async fn addresses_and_lifecycle() {
        let adapters = Arc::new(HostedAdapters::new());
        let mut watch = adapters.watch_services();
        let port = config().ports[0].clone();
        let scale = NonZero::new(1).unwrap();
        assert!(
            adapters
                .service_addresses("adapter", scale, &port)
                .unwrap()
                .is_empty()
        );
        let service = adapters.ensure_service("adapter", config()).unwrap();
        assert!(service.addresses("query").is_empty());
        assert!(
            adapters
                .service_addresses("adapter", scale, &port)
                .unwrap()
                .is_empty()
        );
        expect_event(&mut watch, ServiceStatus::Offline(None));
        let lease = adapters.claim("adapter").await.unwrap();
        let mut missing = addresses();
        missing.remove("health");
        assert!(lease.attach(missing).is_err());
        let mut extra = addresses();
        extra.insert("extra".into(), "127.0.0.1:9999".into());
        assert!(lease.attach(extra).is_err());
        let mut wrong = addresses();
        let address = wrong.remove("health").unwrap();
        wrong.insert("wrong".into(), address);
        assert!(lease.attach(wrong).is_err());
        assert!(service.addresses("query").is_empty());
        assert!(watch.next().now_or_never().is_none());
        lease.attach(addresses()).unwrap();
        expect_event(&mut watch, ServiceStatus::Online);
        assert!(lease.attach(addresses()).is_err());
        for port in config().ports {
            assert_eq!(
                service.addresses(&port.name),
                vec![addresses()[&port.name].clone()]
            );
            assert_eq!(
                adapters.service_addresses("adapter", scale, &port).unwrap(),
                service.addresses(&port.name)
            );
        }
        let repeated = adapters.ensure_service("adapter", config()).unwrap();
        assert_eq!(repeated.addresses("query"), service.addresses("query"));
        assert!(watch.next().now_or_never().is_none());
        assert_eq!(adapters.list_services().await.unwrap(), vec!["adapter"]);
        assert_eq!(
            adapters.fetch_service_metrics("adapter").await.unwrap(),
            vec![ServiceProcessMetrics::default()]
        );
        adapters.flush().await.unwrap();
        let mut invalid = config();
        invalid.scale = NonZero::new(2).unwrap();
        assert!(adapters.ensure_service("other", invalid).is_err());
        let mut changed = config();
        changed.ports.pop();
        assert!(adapters.ensure_service("adapter", changed).is_err());
        adapters.drop_service("adapter").unwrap();
        expect_event(&mut watch, ServiceStatus::Offline(None));
        assert!(service.addresses("query").is_empty());
        assert!(adapters.list_services().await.unwrap().is_empty());
        assert!(adapters.fetch_service_metrics("adapter").await.is_err());
        drop(lease);
        adapters.drop_service("adapter").unwrap();
        assert!(watch.next().now_or_never().is_none());
    }

    #[mz_ore::test(tokio::test)]
    async fn watch_snapshot_and_unpolled_transitions() {
        let adapters = Arc::new(HostedAdapters::new());
        adapters.ensure_service("adapter", config()).unwrap();
        let mut before = adapters.watch_services();
        let lease = adapters.claim("adapter").await.unwrap();
        lease.attach(addresses()).unwrap();
        let mut during = adapters.watch_services();
        drop(lease);
        let replacement = adapters.claim("adapter").await.unwrap();
        replacement.attach(addresses()).unwrap();
        // Neither stream was polled while these transitions happened.
        expect_event(&mut before, ServiceStatus::Offline(None));
        for watch in [&mut before, &mut during] {
            expect_event(watch, ServiceStatus::Online);
            expect_event(watch, ServiceStatus::Offline(None));
            expect_event(watch, ServiceStatus::Online);
            assert!(watch.next().now_or_never().is_none());
        }
    }
}
