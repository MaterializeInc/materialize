// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Backend-independent launch configuration for declared adapter realizations.

use std::collections::BTreeMap;
use std::num::NonZero;

use anyhow::ensure;
use mz_orchestrator::{CpuLimit, DiskLimit, MemoryLimit, ServiceConfig, ServicePort};
use mz_repr::AdapterId;
use mz_server_core::listeners::VersionedListenersConfig;
use mz_server_core::listeners::v26_32_0::ListenersConfig;
use serde::{Deserialize, Serialize};

/// Resource allocation selected by a declaration's requested size.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdapterAllocation {
    pub cpu_limit: CpuLimit,
    pub memory_limit: MemoryLimit,
}

/// Backend identity is independent of the adapter's mutable display name.
pub fn service_name(adapter_id: AdapterId, generation: u64) -> String {
    format!("adapter-{adapter_id}-gen-{generation}")
}

/// Resolved launch inputs supplied by the deployment launcher. Arguments can
/// contain credentials and must not appear in the controller's inventory export.
pub struct AdapterProvisioningConfig {
    pub image: String,
    pub init_container_image: Option<String>,
    /// Common serving arguments, excluding identity and listener configuration.
    pub args: Vec<String>,
    /// Authentication, routes and port hints. The backend assigns final addresses.
    pub listeners: ListenersConfig,
    pub generation: u64,
}

impl AdapterProvisioningConfig {
    /// Backend port name for a configured SQL listener.
    pub fn sql_port_name(&self, name: &str) -> Option<String> {
        self.listeners
            .sql
            .keys()
            .position(|key| key == name)
            .map(|i| format!("sql-{i}"))
    }

    /// Backend port name for a configured HTTP listener.
    pub fn http_port_name(&self, name: &str) -> Option<String> {
        self.listeners
            .http
            .keys()
            .position(|key| key == name)
            .map(|i| format!("http-{i}"))
    }

    /// Builds one process from committed intent. Does not provision it or imply
    /// readiness. The caller must resolve and validate the requested size first.
    pub fn service_config(
        &self,
        adapter_id: AdapterId,
        name: &str,
        size: &str,
        allocation: &AdapterAllocation,
    ) -> anyhow::Result<ServiceConfig> {
        for arg in &self.args {
            let key = arg.split('=').next().expect("split has one element");
            ensure!(
                !matches!(
                    key,
                    "--adapter-id"
                        | "--deploy-generation"
                        | "--process"
                        | "--listeners-config-path"
                        | "--listeners-config-json"
                ),
                "adapter launch arguments must not override {key}"
            );
        }
        ensure!(
            self.listeners.sql.len() + self.listeners.http.len() <= usize::from(u16::MAX),
            "too many adapter listeners"
        );
        // Listener names are user supplied. Bounded ordinal names satisfy backend
        // port-name constraints without collisions or changes to listener policy.
        let ports = self
            .listeners
            .sql
            .iter()
            .enumerate()
            .map(|(i, (_, config))| ServicePort {
                name: format!("sql-{i}"),
                port_hint: config.addr.port(),
                requires_tcp: true,
            })
            .chain(
                self.listeners
                    .http
                    .iter()
                    .enumerate()
                    .map(|(i, (_, config))| ServicePort {
                        name: format!("http-{i}"),
                        port_hint: config.addr.port(),
                        requires_tcp: true,
                    }),
            )
            .collect();
        let args = self.args.clone();
        let listeners = self.listeners.clone();
        let generation = self.generation;
        Ok(ServiceConfig {
            inherit_kubernetes_owner_references: true,
            app_name: "adapterd".into(),
            image: self.image.clone(),
            init_container_image: self.init_container_image.clone(),
            args: Box::new(move |assigned| {
                let mut listeners = listeners.clone();
                for (i, (_, listener)) in listeners.sql.iter_mut().enumerate() {
                    listener.addr = assigned.listen_addrs[&format!("sql-{i}")]
                        .parse()
                        .expect("adapter SQL ports require TCP addresses");
                }
                for (i, (_, listener)) in listeners.http.iter_mut().enumerate() {
                    listener.addr = assigned.listen_addrs[&format!("http-{i}")]
                        .parse()
                        .expect("adapter HTTP ports require TCP addresses");
                }
                let mut args = args.clone();
                args.extend([
                    format!("--adapter-id={adapter_id}"),
                    format!("--deploy-generation={generation}"),
                    "--persist-pubsub-external".into(),
                    format!(
                        "--listeners-config-json={}",
                        serde_json::to_string(&VersionedListenersConfig::V2(listeners))
                            .expect("listener configuration serializes")
                    ),
                ]);
                args
            }),
            ports,
            memory_limit: Some(allocation.memory_limit),
            memory_request: None,
            cpu_limit: Some(allocation.cpu_limit),
            cpu_request: None,
            scale: NonZero::new(1).expect("one adapter process"),
            labels: BTreeMap::from([
                ("type".into(), "adapter".into()),
                ("adapter-id".into(), adapter_id.to_string()),
                ("generation".into(), generation.to_string()),
                ("size".into(), size.replace('=', "-").replace(',', "_")),
            ]),
            annotations: BTreeMap::from([("adapter-name".into(), name.into())]),
            availability_zones: None,
            other_replicas_selector: Vec::new(),
            replicas_selector: Vec::new(),
            disk_limit: Some(DiskLimit::ZERO),
            node_selector: BTreeMap::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_orchestrator::ServiceAssignments;

    #[mz_ore::test]
    fn adapter_launch_preserves_listener_policy_and_stable_identity() {
        let mut listeners: ListenersConfig = serde_json::from_str(include_str!(
            "../../../materialized/ci/listener_configs/v26_32_0/no_auth.json"
        ))
        .expect("listener fixture parses");
        let external = listeners
            .http
            .remove("external")
            .expect("external HTTP listener");
        listeners
            .http
            .insert("external-http-with-a-long-name".into(), external);
        let launch = AdapterProvisioningConfig {
            image: "adapterd".into(),
            init_container_image: None,
            args: vec!["--orchestrator=process".into()],
            listeners: listeners.clone(),
            generation: 7,
        };
        let allocation = AdapterAllocation {
            cpu_limit: CpuLimit::from_millicpus(1000),
            memory_limit: serde_json::from_str("\"1 GiB\"").expect("memory limit parses"),
        };
        let original = launch
            .service_config(AdapterId(3), "before", "small", &allocation)
            .expect("adapter specification");
        let renamed = launch
            .service_config(AdapterId(3), "after", "small", &allocation)
            .expect("renamed specification");
        let addresses: BTreeMap<_, _> = original
            .ports
            .iter()
            .enumerate()
            .map(|(i, port)| {
                assert!(port.requires_tcp);
                assert!(port.name.len() <= 15);
                (port.name.clone(), format!("127.0.0.1:{}", 18000 + i))
            })
            .collect();
        let assignments = || ServiceAssignments {
            listen_addrs: &addresses,
            peer_addrs: &[],
        };
        let args = (original.args)(assignments());
        assert_eq!(
            args,
            (renamed.args)(assignments()),
            "rename must not alter process arguments"
        );
        assert_eq!(original.labels, renamed.labels);
        assert_eq!(original.cpu_limit, Some(allocation.cpu_limit));
        assert_eq!(original.memory_limit, Some(allocation.memory_limit));
        assert_eq!(original.scale.get(), 1);
        assert_eq!(original.disk_limit, Some(DiskLimit::ZERO));
        assert!(args.iter().any(|arg| arg == "--adapter-id=a3"));
        assert!(args.iter().any(|arg| arg == "--deploy-generation=7"));
        assert!(args.iter().any(|arg| arg == "--persist-pubsub-external"));
        let json = args
            .iter()
            .find_map(|arg| arg.strip_prefix("--listeners-config-json="))
            .expect("inline listener configuration");
        let assigned: ListenersConfig =
            serde_json::from_str(json).expect("assigned listeners parse");
        let mut expected = listeners;
        for (name, listener) in &mut expected.sql {
            listener.addr = addresses
                [&launch.sql_port_name(name).expect("configured SQL listener")]
                .parse()
                .expect("TCP address");
        }
        for (name, listener) in &mut expected.http {
            listener.addr = addresses[&launch
                .http_port_name(name)
                .expect("configured HTTP listener")]
                .parse()
                .expect("TCP address");
        }
        assert_eq!(
            serde_json::to_value(assigned).expect("JSON"),
            serde_json::to_value(expected).expect("JSON")
        );
    }
}
