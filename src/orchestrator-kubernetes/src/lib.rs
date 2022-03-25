// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;

use anyhow::bail;
use async_trait::async_trait;
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, PodSpec, PodTemplateSpec, ResourceRequirements,
    Service as K8sService, ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::{Api, DeleteParams, ListParams, ObjectMeta, Patch, PatchParams};
use kube::client::Client;
use kube::config::{Config, KubeConfigOptions};
use kube::error::Error;
use maplit::btreemap;

use mz_orchestrator::{NamespacedOrchestrator, Orchestrator, Service, ServiceConfig};

const FIELD_MANAGER: &str = "materialized";

/// Configures a [`KubernetesOrchestrator`].
#[derive(Debug, Clone)]
pub struct KubernetesOrchestratorConfig {
    /// The name of a Kubernetes context to use, if the Kubernetes configuration
    /// is loaded from the local kubeconfig.
    pub context: String,
}

/// An orchestrator backed by Kubernetes.
#[derive(Clone)]
pub struct KubernetesOrchestrator {
    client: Client,
    kubernetes_namespace: String,
}

impl fmt::Debug for KubernetesOrchestrator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("KubernetesOrchestrator").finish()
    }
}

impl KubernetesOrchestrator {
    /// Creates a new Kubernetes orchestrator from the provided configuration.
    pub async fn new(
        config: KubernetesOrchestratorConfig,
    ) -> Result<KubernetesOrchestrator, anyhow::Error> {
        let kubeconfig_options = KubeConfigOptions {
            context: Some(config.context),
            ..Default::default()
        };
        let config = match Config::from_kubeconfig(&kubeconfig_options).await {
            Ok(config) => config,
            Err(kubeconfig_err) => match Config::from_cluster_env() {
                Ok(config) => config,
                Err(in_cluster_err) => {
                    bail!("failed to infer config: in-cluster: ({in_cluster_err}), kubeconfig: ({kubeconfig_err})");
                }
            },
        };
        let kubernetes_namespace = config.default_namespace.clone();
        let client = Client::try_from(config)?;
        Ok(KubernetesOrchestrator {
            client,
            kubernetes_namespace,
        })
    }
}

impl Orchestrator for KubernetesOrchestrator {
    fn namespace(&self, namespace: &str) -> Box<dyn NamespacedOrchestrator> {
        Box::new(NamespacedKubernetesOrchestrator {
            service_api: Api::default_namespaced(self.client.clone()),
            stateful_set_api: Api::default_namespaced(self.client.clone()),
            kubernetes_namespace: self.kubernetes_namespace.clone(),
            namespace: namespace.into(),
        })
    }
}

#[derive(Clone)]
struct NamespacedKubernetesOrchestrator {
    service_api: Api<K8sService>,
    stateful_set_api: Api<StatefulSet>,
    kubernetes_namespace: String,
    namespace: String,
}

impl fmt::Debug for NamespacedKubernetesOrchestrator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("NamespacedKubernetesOrchestrator")
            .field("namespace", &self.namespace)
            .finish()
    }
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedKubernetesOrchestrator {
    async fn ensure_service(
        &mut self,
        id: &str,
        ServiceConfig {
            image,
            args,
            ports,
            memory_limit,
            cpu_limit,
            processes,
        }: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let name = format!("{}-{id}", self.namespace);
        let labels = btreemap! {
            "service".into() => id.into(),
        };
        let mut limits = BTreeMap::new();
        if let Some(memory_limit) = memory_limit {
            limits.insert(
                "memory".into(),
                Quantity(memory_limit.as_bytes().to_string()),
            );
        }
        if let Some(cpu_limit) = cpu_limit {
            limits.insert(
                "cpu".into(),
                Quantity(format!("{}m", cpu_limit.as_millicpus())),
            );
        }
        let service = K8sService {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                ports: Some(
                    ports
                        .iter()
                        .map(|port| ServicePort {
                            port: *port,
                            name: Some(format!("port-{port}")),
                            ..Default::default()
                        })
                        .collect(),
                ),
                cluster_ip: None,
                selector: Some(labels.clone()),
                ..Default::default()
            }),
            status: None,
        };
        let stateful_set = StatefulSet {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                ..Default::default()
            },
            spec: Some(StatefulSetSpec {
                selector: LabelSelector {
                    match_labels: Some(labels.clone()),
                    ..Default::default()
                },
                service_name: name.clone(),
                replicas: Some(processes.try_into()?),
                template: PodTemplateSpec {
                    metadata: Some(ObjectMeta {
                        labels: Some(labels.clone()),
                        ..Default::default()
                    }),
                    spec: Some(PodSpec {
                        containers: vec![Container {
                            name: "default".into(),
                            image: Some(image),
                            args: Some(args),
                            ports: Some(
                                ports
                                    .iter()
                                    .map(|port| ContainerPort {
                                        container_port: *port,
                                        ..Default::default()
                                    })
                                    .collect(),
                            ),
                            resources: Some(ResourceRequirements {
                                limits: Some(limits),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                ..Default::default()
            }),
            status: None,
        };
        self.service_api
            .patch(
                &name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(service),
            )
            .await?;
        self.stateful_set_api
            .patch(
                &name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(stateful_set),
            )
            .await?;
        let hosts = (0..processes)
            .map(|i| {
                format!(
                    "{name}-{i}.{name}.{}.svc.cluster.local",
                    self.kubernetes_namespace
                )
            })
            .collect();
        Ok(Box::new(KubernetesService { hosts }))
    }

    /// Drops the identified service, if it exists.
    async fn drop_service(&mut self, id: &str) -> Result<(), anyhow::Error> {
        let name = format!("{}-{id}", self.namespace);
        let res = self
            .stateful_set_api
            .delete(&name, &DeleteParams::default())
            .await;
        match res {
            Ok(_) => Ok(()),
            Err(Error::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Lists the identifiers of all known services.
    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        let stateful_sets = self.stateful_set_api.list(&ListParams::default()).await?;
        let name_prefix = format!("{}-", self.namespace);
        Ok(stateful_sets
            .into_iter()
            .filter_map(|ss| {
                ss.metadata
                    .name
                    .unwrap()
                    .strip_prefix(&name_prefix)
                    .map(Into::into)
            })
            .collect())
    }
}

#[derive(Debug, Clone)]
struct KubernetesService {
    hosts: Vec<String>,
}

impl Service for KubernetesService {
    fn hosts(&self) -> Vec<String> {
        self.hosts.clone()
    }
}
