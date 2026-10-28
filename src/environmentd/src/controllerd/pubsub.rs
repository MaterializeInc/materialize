// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Environment-scoped relay provisioning from static launch configuration.

use std::collections::BTreeMap;
use std::num::NonZero;
use std::sync::Arc;

use futures::stream::BoxStream;
use mz_orchestrator::{
    CpuLimit, DiskLimit, MemoryLimit, NamespacedOrchestrator, ServiceConfig, ServiceEvent,
    ServicePort,
};
use serde::Serialize;

const SERVICE: &str = "relay";
const GRPC: &str = "pubsub";
const HTTP: &str = "internal-http";

pub struct PubsubConfig {
    pub image: String,
    pub grpc_port_hint: u16,
    pub http_port_hint: u16,
    pub cpu_limit: CpuLimit,
    pub memory_limit: MemoryLimit,
}

impl PubsubConfig {
    fn service_config(&self) -> ServiceConfig {
        ServiceConfig {
            app_name: "pubsubd".into(),
            inherit_kubernetes_owner_references: false,
            image: self.image.clone(),
            init_container_image: None,
            args: Box::new(|assigned| {
                vec![
                    format!("--listen-addr={}", assigned.listen_addrs[GRPC]),
                    format!(
                        "--internal-http-listen-addr={}",
                        assigned.listen_addrs[HTTP]
                    ),
                ]
            }),
            ports: vec![
                ServicePort {
                    name: GRPC.into(),
                    port_hint: self.grpc_port_hint,
                    requires_tcp: true,
                },
                ServicePort {
                    name: HTTP.into(),
                    port_hint: self.http_port_hint,
                    requires_tcp: true,
                },
            ],
            memory_limit: Some(self.memory_limit),
            memory_request: None,
            cpu_limit: Some(self.cpu_limit),
            cpu_request: None,
            scale: NonZero::new(1).expect("one relay"),
            labels: BTreeMap::from([("type".into(), "pubsub".into())]),
            annotations: BTreeMap::new(),
            availability_zones: None,
            other_replicas_selector: Vec::new(),
            replicas_selector: Vec::new(),
            disk_limit: Some(DiskLimit::ZERO),
            node_selector: BTreeMap::new(),
        }
    }
}

#[derive(Serialize)]
pub struct PubsubInventory {
    service: &'static str,
    cpu_limit: CpuLimit,
    memory_limit: MemoryLimit,
    endpoints: BTreeMap<String, Vec<String>>,
    observed_processes: BTreeMap<u64, ServiceEvent>,
    provisioning_error: Option<&'static str>,
}

/// Only the statically selected provisioner owns this value. There is no
/// deployment retirement cleanup: the shared relay outlives deployments.
pub struct ManagedPubsub {
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    config: PubsubConfig,
    inventory: PubsubInventory,
    url: String,
}

impl ManagedPubsub {
    /// Assigns an endpoint before clients are constructed, without waiting for
    /// relay readiness. Persist clients retain their durable fallback.
    pub fn new(
        orchestrator: Arc<dyn NamespacedOrchestrator>,
        config: PubsubConfig,
    ) -> anyhow::Result<Self> {
        let service = orchestrator.ensure_service(SERVICE, config.service_config())?;
        let grpc = service.addresses(GRPC);
        anyhow::ensure!(grpc.len() == 1, "expected one shared relay endpoint");
        let url = format!("http://{}", grpc[0]);
        let inventory = PubsubInventory {
            service: SERVICE,
            cpu_limit: config.cpu_limit,
            memory_limit: config.memory_limit,
            endpoints: BTreeMap::from([
                (GRPC.into(), grpc),
                (HTTP.into(), service.addresses(HTTP)),
            ]),
            observed_processes: BTreeMap::new(),
            provisioning_error: None,
        };
        Ok(Self {
            orchestrator,
            config,
            inventory,
            url,
        })
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    pub(super) fn inventory(&self) -> &PubsubInventory {
        &self.inventory
    }

    pub(super) fn watch(&self) -> BoxStream<'static, Result<ServiceEvent, anyhow::Error>> {
        self.orchestrator.watch_services()
    }

    pub(super) fn record(&mut self, event: Result<ServiceEvent, anyhow::Error>) {
        match event {
            Ok(event) if event.service_id == SERVICE => {
                self.inventory
                    .observed_processes
                    .insert(event.process_id, event);
            }
            Ok(_) => (),
            Err(error) => tracing::warn!(%error, "pubsub observation failed"),
        }
    }

    pub(super) fn reconcile(&mut self) {
        let result = self
            .orchestrator
            .ensure_service(SERVICE, self.config.service_config())
            .and_then(|service| {
                anyhow::ensure!(
                    service.addresses(GRPC) == self.inventory.endpoints[GRPC],
                    "shared relay endpoint changed after client construction"
                );
                Ok(())
            });
        self.inventory.provisioning_error = match result {
            Ok(()) => None,
            Err(error) => {
                tracing::warn!(%error, "pubsub provisioning failed");
                Some("relay provisioning failed")
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_orchestrator::ServiceAssignments;

    #[mz_ore::test]
    fn relay_uses_assigned_ports_and_environment_lifetime() {
        let config = PubsubConfig {
            image: "pubsubd".into(),
            grpc_port_hint: 6879,
            http_port_hint: 6881,
            cpu_limit: CpuLimit::from_millicpus(1000),
            memory_limit: MemoryLimit("512 MiB".parse().unwrap()),
        }
        .service_config();
        assert!(!config.inherit_kubernetes_owner_references);
        assert_eq!(config.scale.get(), 1);
        assert!(config.ports.iter().all(|port| port.requires_tcp));
        assert!(config.ports.iter().any(|port| port.name == "internal-http"));
        assert!(!config.labels.contains_key("generation"));
        let listen_addrs = BTreeMap::from([
            (GRPC.into(), "127.0.0.1:17000".into()),
            (HTTP.into(), "127.0.0.1:17001".into()),
        ]);
        let args = (config.args)(ServiceAssignments {
            listen_addrs: &listen_addrs,
            peer_addrs: &[],
        });
        assert_eq!(
            args,
            vec![
                "--listen-addr=127.0.0.1:17000",
                "--internal-http-listen-addr=127.0.0.1:17001"
            ]
        );
    }
}
