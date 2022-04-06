// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::fmt;

use anyhow::bail;
use async_trait::async_trait;
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, Pod, PodSpec, PodTemplateSpec, ResourceRequirements,
    Service as K8sService, ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector;
use kube::api::{Api, DeleteParams, ListParams, ObjectMeta, Patch, PatchParams};
use kube::client::Client;
use kube::config::{Config, KubeConfigOptions};
use kube::error::Error;
use kube::ResourceExt;
use sha2::{Digest, Sha256};

use mz_orchestrator::{NamespacedOrchestrator, Orchestrator, Service, ServiceConfig};

const FIELD_MANAGER: &str = "materialized";

/// Configures a [`KubernetesOrchestrator`].
#[derive(Debug, Clone)]
pub struct KubernetesOrchestratorConfig {
    /// The name of a Kubernetes context to use, if the Kubernetes configuration
    /// is loaded from the local kubeconfig.
    pub context: String,
    /// Labels to install on every service created by the orchestrator.
    pub service_labels: HashMap<String, String>,
}

/// An orchestrator backed by Kubernetes.
#[derive(Clone)]
pub struct KubernetesOrchestrator {
    client: Client,
    kubernetes_namespace: String,
    service_labels: HashMap<String, String>,
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
        let kubeconfig = match Config::from_kubeconfig(&kubeconfig_options).await {
            Ok(config) => config,
            Err(kubeconfig_err) => match Config::from_cluster_env() {
                Ok(config) => config,
                Err(in_cluster_err) => {
                    bail!("failed to infer config: in-cluster: ({in_cluster_err}), kubeconfig: ({kubeconfig_err})");
                }
            },
        };
        let kubernetes_namespace = kubeconfig.default_namespace.clone();
        let client = Client::try_from(kubeconfig)?;
        Ok(KubernetesOrchestrator {
            client,
            kubernetes_namespace,
            service_labels: config.service_labels,
        })
    }
}

impl Orchestrator for KubernetesOrchestrator {
    fn namespace(&self, namespace: &str) -> Box<dyn NamespacedOrchestrator> {
        Box::new(NamespacedKubernetesOrchestrator {
            service_api: Api::default_namespaced(self.client.clone()),
            stateful_set_api: Api::default_namespaced(self.client.clone()),
            pod_api: Api::default_namespaced(self.client.clone()),
            kubernetes_namespace: self.kubernetes_namespace.clone(),
            namespace: namespace.into(),
            service_labels: self.service_labels.clone(),
        })
    }
}

#[derive(Clone)]
struct NamespacedKubernetesOrchestrator {
    service_api: Api<K8sService>,
    stateful_set_api: Api<StatefulSet>,
    pod_api: Api<Pod>,
    kubernetes_namespace: String,
    namespace: String,
    service_labels: HashMap<String, String>,
}

impl fmt::Debug for NamespacedKubernetesOrchestrator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("NamespacedKubernetesOrchestrator")
            .field("kubernetes_namespace", &self.kubernetes_namespace)
            .field("namespace", &self.namespace)
            .field("service_labels", &self.service_labels)
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
            labels: labels_in,
        }: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let name = format!("{}-{id}", self.namespace);
        let mut labels = BTreeMap::new();
        for (key, value) in labels_in {
            labels.insert(
                format!("{}.materialized.materialize.cloud/{}", self.namespace, key),
                value,
            );
        }
        for port in &ports {
            labels.insert(
                format!("materialized.materialize.cloud/port-{}", port.name),
                "true".into(),
            );
        }
        labels.insert(
            "materialized.materialize.cloud/namespace".into(),
            self.namespace.clone(),
        );
        labels.insert(
            "materialized.materialize.cloud/service-id".into(),
            id.into(),
        );
        for (key, value) in &self.service_labels {
            labels.insert(key.clone(), value.clone());
        }
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
                            port: port.port,
                            name: Some(port.name.clone()),
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

        let mut pod_template_spec = PodTemplateSpec {
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
                                container_port: port.port,
                                name: Some(port.name.clone()),
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
        };
        let pod_template_json = serde_json::to_string(&pod_template_spec).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(pod_template_json);
        let pod_template_hash = format!("{:x}", hasher.finalize());
        let pod_template_hash_annotation = "materialized.materialize.cloud/pod-template-hash";
        pod_template_spec
            .metadata
            .as_mut()
            .unwrap()
            .annotations
            .as_mut()
            .unwrap()
            .insert(
                pod_template_hash_annotation.to_owned(),
                pod_template_hash.clone(),
            );

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
                template: pod_template_spec,
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
        for pod_id in 0..processes {
            let pod_name = format!("{}-{}", &name, pod_id);
            let pod = self.pod_api.get(&pod_name).await?;
            if pod.annotations().get(pod_template_hash_annotation) != Some(&pod_template_hash) {
                match self
                    .pod_api
                    .delete(&pod_name, &DeleteParams::default())
                    .await
                {
                    Ok(_) => {}
                    // object already doesn't exist
                    Err(kube::Error::Api(e)) if e.code == 404 => {}
                    Err(e) => Err(e)?,
                }
            }
        }
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
