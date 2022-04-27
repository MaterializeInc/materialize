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
use std::sync::Arc;

use anyhow::bail;
use async_trait::async_trait;
use clap::ArgEnum;
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, Pod, PodSpec, PodTemplateSpec, ResourceRequirements,
    SecretVolumeSource, Service as K8sService, ServicePort, ServiceSpec, Volume, VolumeMount,
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
use mz_secrets_kubernetes::SECRET_NAME;

const FIELD_MANAGER: &str = "materialized";

/// Configures a [`KubernetesOrchestrator`].
#[derive(Debug, Clone)]
pub struct KubernetesOrchestratorConfig {
    /// The name of a Kubernetes context to use, if the Kubernetes configuration
    /// is loaded from the local kubeconfig.
    pub context: String,
    /// Labels to install on every service created by the orchestrator.
    pub service_labels: HashMap<String, String>,
    /// The image pull policy to set for services created by the orchestrator.
    pub image_pull_policy: KubernetesImagePullPolicy,
}

#[derive(ArgEnum, Debug, Clone, Copy)]
pub enum KubernetesImagePullPolicy {
    Always,
    IfNotPresent,
    Never,
}

impl fmt::Display for KubernetesImagePullPolicy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KubernetesImagePullPolicy::Always => f.write_str("Always"),
            KubernetesImagePullPolicy::IfNotPresent => f.write_str("IfNotPresent"),
            KubernetesImagePullPolicy::Never => f.write_str("Never"),
        }
    }
}

/// An orchestrator backed by Kubernetes.
pub struct KubernetesOrchestrator {
    client: Client,
    kubernetes_namespace: String,
    service_labels: HashMap<String, String>,
    image_pull_policy: KubernetesImagePullPolicy,
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
            image_pull_policy: config.image_pull_policy,
        })
    }
}

impl Orchestrator for KubernetesOrchestrator {
    fn listen_host(&self) -> &str {
        "0.0.0.0"
    }
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        Arc::new(NamespacedKubernetesOrchestrator {
            service_api: Api::default_namespaced(self.client.clone()),
            stateful_set_api: Api::default_namespaced(self.client.clone()),
            pod_api: Api::default_namespaced(self.client.clone()),
            kubernetes_namespace: self.kubernetes_namespace.clone(),
            namespace: namespace.into(),
            service_labels: self.service_labels.clone(),
            image_pull_policy: self.image_pull_policy,
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
    image_pull_policy: KubernetesImagePullPolicy,
}

impl fmt::Debug for NamespacedKubernetesOrchestrator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("NamespacedKubernetesOrchestrator")
            .field("kubernetes_namespace", &self.kubernetes_namespace)
            .field("namespace", &self.namespace)
            .field("service_labels", &self.service_labels)
            .field("image_pull_policy", &self.image_pull_policy)
            .finish()
    }
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedKubernetesOrchestrator {
    async fn ensure_service(
        &self,
        id: &str,
        ServiceConfig {
            image,
            args,
            ports: ports_in,
            memory_limit,
            cpu_limit,
            scale,
            labels: labels_in,
            availability_zone,
        }: ServiceConfig<'_>,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let name = format!("{}-{id}", self.namespace);
        let mut labels = BTreeMap::new();
        for (key, value) in labels_in {
            labels.insert(
                format!("{}.materialized.materialize.cloud/{}", self.namespace, key),
                value,
            );
        }
        for port in &ports_in {
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
                Quantity(memory_limit.0.as_u64().to_string()),
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
                    ports_in
                        .iter()
                        .map(|port| ServicePort {
                            port: port.port_hint.into(),
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

        let volume_name = "secrets-mount".to_string();

        let secrets_volume = Volume {
            name: volume_name.clone(),
            secret: Some(SecretVolumeSource {
                secret_name: Some(SECRET_NAME.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let hosts = (0..scale.get())
            .map(|i| {
                format!(
                    "{name}-{i}.{name}.{}.svc.cluster.local",
                    self.kubernetes_namespace
                )
            })
            .collect::<Vec<_>>();

        let ports = ports_in
            .iter()
            .map(|p| (p.name.clone(), p.port_hint))
            .collect::<HashMap<_, _>>();
        let node_selector = availability_zone
            .map(|az| BTreeMap::from([("materialize.cloud/availability-zone".to_string(), az)]));
        let hosts_ports = hosts
            .iter()
            .map(|host| (host.clone(), ports.clone()))
            .collect::<Vec<_>>();
        let mut pod_template_spec = PodTemplateSpec {
            metadata: Some(ObjectMeta {
                labels: Some(labels.clone()),
                annotations: Some(BTreeMap::new()), // Do not delete, we insert into it below.
                ..Default::default()
            }),
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "default".into(),
                    image: Some(image),
                    args: Some(args(&hosts_ports, &ports, None)),
                    image_pull_policy: Some(self.image_pull_policy.to_string()),
                    ports: Some(
                        ports_in
                            .iter()
                            .map(|port| ContainerPort {
                                container_port: port.port_hint.into(),
                                name: Some(port.name.clone()),
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    resources: Some(ResourceRequirements {
                        limits: Some(limits),
                        ..Default::default()
                    }),
                    volume_mounts: Some(vec![VolumeMount {
                        mount_path: "/secrets".to_string(),
                        name: volume_name.clone(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                volumes: Some(vec![secrets_volume]),
                node_selector,
                ..Default::default()
            }),
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
                replicas: Some(scale.get().try_into()?),
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
        // Explicitly delete any pods in the stateful set that don't match the
        // template. In theory, Kubernetes would do this automatically, but
        // in practice we have observed that it does not.
        // See: https://github.com/kubernetes/kubernetes/issues/67250
        for pod_id in 0..scale.get() {
            let pod_name = format!("{}-{}", &name, pod_id);
            let pod = match self.pod_api.get(&pod_name).await {
                Ok(pod) => pod,
                // Pod already doesn't exist.
                Err(kube::Error::Api(e)) if e.code == 404 => continue,
                Err(e) => return Err(e.into()),
            };
            if pod.annotations().get(pod_template_hash_annotation) != Some(&pod_template_hash) {
                match self
                    .pod_api
                    .delete(&pod_name, &DeleteParams::default())
                    .await
                {
                    Ok(_) => (),
                    // Pod got deleted while we were looking at it.
                    Err(kube::Error::Api(e)) if e.code == 404 => (),
                    Err(e) => return Err(e.into()),
                }
            }
        }
        Ok(Box::new(KubernetesService { hosts, ports }))
    }

    /// Drops the identified service, if it exists.
    async fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
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
    ports: HashMap<String, u16>,
}

impl Service for KubernetesService {
    fn addresses(&self, port: &str) -> Vec<String> {
        let port = self.ports[port];
        self.hosts
            .iter()
            .map(|host| format!("{host}:{port}"))
            .collect()
    }
}
