// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use futures::stream::BoxStream;
use futures::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;

use mz_orchestrator::{
    LabelSelectionLogic, LabelSelector, NamespacedOrchestrator, Service, ServiceConfig,
    ServiceEvent, ServicePort,
};

use crate::command::ReplicaId;

use super::{ComputeInstanceEvent, ComputeInstanceId, ComputeReplicaAllocation};

pub(super) struct ComputeOrchestrator {
    inner: Arc<dyn NamespacedOrchestrator>,
    computed_image: String,
}

impl ComputeOrchestrator {
    pub(super) fn new(inner: Arc<dyn NamespacedOrchestrator>, computed_image: String) -> Self {
        Self {
            inner,
            computed_image,
        }
    }

    pub(super) async fn ensure_replica(
        &self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
        allocation: ComputeReplicaAllocation,
        availability_zone: String,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let service_name = generate_replica_service_name(instance_id, replica_id);

        let service = self
            .inner
            .ensure_service(
                &service_name,
                ServiceConfig {
                    image: self.computed_image.clone(),
                    args: &|assigned| {
                        let mut compute_opts = vec![
                            format!(
                                "--controller-listen-addr={}:{}",
                                assigned.listen_host, assigned.ports["controller"]
                            ),
                            format!(
                                "--internal-http-listen-addr={}:{}",
                                assigned.listen_host, assigned.ports["internal-http"]
                            ),
                            format!("--opentelemetry-resource=instance_id={}", instance_id),
                            format!("--opentelemetry-resource=replica_id={}", replica_id),
                        ];
                        if let Some(index) = assigned.index {
                            compute_opts
                                .push(format!("--opentelemetry-resource=replica_index={}", index));
                        }
                        compute_opts
                    },
                    ports: vec![
                        ServicePort {
                            name: "controller".into(),
                            port_hint: 2100,
                        },
                        ServicePort {
                            name: "compute".into(),
                            port_hint: 2102,
                        },
                        ServicePort {
                            name: "internal-http".into(),
                            port_hint: 6878,
                        },
                    ],
                    cpu_limit: allocation.cpu_limit,
                    memory_limit: allocation.memory_limit,
                    scale: allocation.scale,
                    labels: HashMap::from([
                        ("replica-id".into(), replica_id.to_string()),
                        ("cluster-id".into(), instance_id.to_string()),
                        ("type".into(), "cluster".into()),
                    ]),
                    availability_zone: Some(availability_zone),
                    // This constrains the orchestrator (for those orchestrators that support
                    // anti-affinity, today just k8s) to never schedule pods for different replicas
                    // of the same cluster on the same node. Pods from the _same_ replica are fine;
                    // pods from different clusters are also fine.
                    //
                    // The point is that if pods of two replicas are on the same node, that node
                    // going down would kill both replicas, and so the replication factor of the
                    // cluster in question is illusory.
                    anti_affinity: Some(vec![
                        LabelSelector {
                            label_name: "cluster-id".to_string(),
                            logic: LabelSelectionLogic::Eq {
                                value: instance_id.to_string(),
                            },
                        },
                        LabelSelector {
                            label_name: "replica-id".into(),
                            logic: LabelSelectionLogic::NotEq {
                                value: replica_id.to_string(),
                            },
                        },
                    ]),
                },
            )
            .await?;

        Ok(service)
    }

    pub(super) async fn drop_replica(
        &self,
        instance_id: ComputeInstanceId,
        replica_id: ReplicaId,
    ) -> Result<(), anyhow::Error> {
        let service_name = generate_replica_service_name(instance_id, replica_id);
        self.inner.drop_service(&service_name).await
    }

    pub(super) fn watch_services(&self) -> BoxStream<'static, ComputeInstanceEvent> {
        fn translate_event(event: ServiceEvent) -> Result<ComputeInstanceEvent, anyhow::Error> {
            let (instance_id, replica_id) = parse_replica_service_name(&event.service_id)?;
            Ok(ComputeInstanceEvent {
                instance_id,
                replica_id,
                process_id: event.process_id,
                status: event.status,
                time: event.time,
            })
        }

        let stream = self
            .inner
            .watch_services()
            .map(|event| event.and_then(translate_event))
            .filter_map(|event| async {
                match event {
                    Ok(event) => Some(event),
                    Err(error) => {
                        tracing::error!("service watch error: {error}");
                        None
                    }
                }
            });

        Box::pin(stream)
    }
}

/// Deterministically generates replica names based on inputs.
fn generate_replica_service_name(instance_id: ComputeInstanceId, replica_id: ReplicaId) -> String {
    format!("cluster-{instance_id}-replica-{replica_id}")
}

/// Parse a name generated by `generate_replica_service_name`, to extract the
/// replica's compute instance ID and replica ID values.
fn parse_replica_service_name(
    service_name: &str,
) -> Result<(ComputeInstanceId, ReplicaId), anyhow::Error> {
    static SERVICE_NAME_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?-u)^cluster-(\d+)-replica-(\d+)$").unwrap());

    let caps = SERVICE_NAME_RE
        .captures(service_name)
        .ok_or_else(|| anyhow!("invalid service name: {service_name}"))?;

    let instance_id = caps.get(1).unwrap().as_str().parse().unwrap();
    let replica_id = caps.get(2).unwrap().as_str().parse().unwrap();
    Ok((instance_id, replica_id))
}
