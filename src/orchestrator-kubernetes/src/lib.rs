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
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use clap::ArgEnum;
use futures::stream::{BoxStream, StreamExt};
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    Affinity, Container, ContainerPort, Pod, PodAffinityTerm, PodAntiAffinity, PodSpec,
    PodTemplateSpec, ResourceRequirements, Secret, Service as K8sService, ServicePort, ServiceSpec,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};
use kube::api::{Api, DeleteParams, ListParams, ObjectMeta, Patch, PatchParams};
use kube::client::Client;
use kube::error::Error;
use kube::runtime::{watcher, WatchStreamExt};
use kube::ResourceExt;
use sha2::{Digest, Sha256};

use mz_orchestrator::LabelSelector as MzLabelSelector;
use mz_orchestrator::{
    LabelSelectionLogic, NamespacedOrchestrator, Orchestrator, Service, ServiceAssignments,
    ServiceConfig, ServiceEvent, ServiceStatus,
};

pub mod secrets;
pub mod util;

const FIELD_MANAGER: &str = "environmentd";

/// Configures a [`KubernetesOrchestrator`].
#[derive(Debug, Clone)]
pub struct KubernetesOrchestratorConfig {
    /// The name of a Kubernetes context to use, if the Kubernetes configuration
    /// is loaded from the local kubeconfig.
    pub context: String,
    /// Labels to install on every service created by the orchestrator.
    pub service_labels: HashMap<String, String>,
    /// Node selector to install on every service created by the orchestrator.
    pub service_node_selector: HashMap<String, String>,
    /// The service account that each service should run as, if any.
    pub service_account: Option<String>,
    /// The image pull policy to set for services created by the orchestrator.
    pub image_pull_policy: KubernetesImagePullPolicy,
}

/// Specifies whether Kubernetes should pull Docker images when creating pods.
#[derive(ArgEnum, Debug, Clone, Copy)]
pub enum KubernetesImagePullPolicy {
    /// Always pull the Docker image from the registry.
    Always,
    /// Pull the Docker image only if the image is not present.
    IfNotPresent,
    /// Never pull the Docker image.
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
    config: KubernetesOrchestratorConfig,
    secret_api: Api<Secret>,
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
        let (client, kubernetes_namespace) = util::create_client(config.context.clone()).await?;
        Ok(KubernetesOrchestrator {
            client: client.clone(),
            kubernetes_namespace,
            config,
            secret_api: Api::default_namespaced(client),
        })
    }
}

impl Orchestrator for KubernetesOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        Arc::new(NamespacedKubernetesOrchestrator {
            service_api: Api::default_namespaced(self.client.clone()),
            stateful_set_api: Api::default_namespaced(self.client.clone()),
            pod_api: Api::default_namespaced(self.client.clone()),
            kubernetes_namespace: self.kubernetes_namespace.clone(),
            namespace: namespace.into(),
            config: self.config.clone(),
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
    config: KubernetesOrchestratorConfig,
}

impl fmt::Debug for NamespacedKubernetesOrchestrator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("NamespacedKubernetesOrchestrator")
            .field("kubernetes_namespace", &self.kubernetes_namespace)
            .field("namespace", &self.namespace)
            .field("config", &self.config)
            .finish()
    }
}

fn label_selector_to_k8s(
    MzLabelSelector { label_name, logic }: MzLabelSelector,
    prefix: &str,
) -> Result<LabelSelectorRequirement, anyhow::Error> {
    let (operator, values) = match logic {
        LabelSelectionLogic::Eq { value } => Ok(("In", vec![value])),
        LabelSelectionLogic::NotEq { value } => Ok(("NotIn", vec![value])),
        LabelSelectionLogic::Exists => Ok(("Exists", vec![])),
        LabelSelectionLogic::NotExists => Ok(("DoesNotExist", vec![])),
        LabelSelectionLogic::InSet { values } => {
            if values.is_empty() {
                Err(anyhow!(
                    "Invalid selector logic for {label_name}: empty `in` set"
                ))
            } else {
                Ok(("In", values))
            }
        }
        LabelSelectionLogic::NotInSet { values } => {
            if values.is_empty() {
                Err(anyhow!(
                    "Invalid selector logic for {label_name}: empty `notin` set"
                ))
            } else {
                Ok(("NotIn", values))
            }
        }
    }?;
    let lsr = LabelSelectorRequirement {
        key: format!("{prefix}/{label_name}"),
        operator: operator.to_string(),
        values: Some(values),
    };
    Ok(lsr)
}

impl NamespacedKubernetesOrchestrator {
    /// Return a `ListParams` instance that limits results to the namespace
    /// assigned to this orchestrator.
    fn list_params(&self) -> ListParams {
        let ns_selector = format!(
            "environmentd.materialize.cloud/namespace={}",
            self.namespace
        );
        ListParams::default().labels(&ns_selector)
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
            anti_affinity,
        }: ServiceConfig<'_>,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let name = format!("{}-{id}", self.namespace);
        let mut labels = BTreeMap::new();
        for (key, value) in labels_in {
            labels.insert(
                format!("{}.environmentd.materialize.cloud/{}", self.namespace, key),
                value,
            );
        }
        for port in &ports_in {
            labels.insert(
                format!("environmentd.materialize.cloud/port-{}", port.name),
                "true".into(),
            );
        }
        labels.insert(
            "environmentd.materialize.cloud/namespace".into(),
            self.namespace.clone(),
        );
        labels.insert(
            "environmentd.materialize.cloud/service-id".into(),
            id.into(),
        );
        for (key, value) in &self.config.service_labels {
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
        let peers = hosts
            .iter()
            .map(|host| (host.clone(), ports.clone()))
            .collect::<Vec<_>>();

        let mut node_selector: BTreeMap<String, String> = self
            .config
            .service_node_selector
            .clone()
            .into_iter()
            .collect();
        if let Some(availability_zone) = availability_zone {
            node_selector.insert(
                "materialize.cloud/availability-zone".to_string(),
                availability_zone,
            );
        }
        let mut args = args(&ServiceAssignments {
            listen_host: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            ports: &ports,
            index: None,
            peers: &peers,
        });
        args.push("--secrets-reader=kubernetes".into());
        args.push(format!(
            "--secrets-reader-kubernetes-context={}",
            self.config.context
        ));

        let anti_affinity = anti_affinity
            .map(|label_selectors| -> Result<_, anyhow::Error> {
                let label_selector_requirements = label_selectors
                    .into_iter()
                    .map(|ls| label_selector_to_k8s(ls, &self.namespace))
                    .collect::<Result<Vec<_>, _>>()?;
                let ls = LabelSelector {
                    match_expressions: Some(label_selector_requirements),
                    ..Default::default()
                };
                let pat = PodAffinityTerm {
                    label_selector: Some(ls),
                    topology_key: "kubernetes.io/hostname".to_string(),
                    ..Default::default()
                };
                Ok(PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(vec![pat]),
                    ..Default::default()
                })
            })
            .transpose()?;
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
                    args: Some(args),
                    image_pull_policy: Some(self.config.image_pull_policy.to_string()),
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
                    ..Default::default()
                }],
                node_selector: Some(node_selector),
                service_account: self.config.service_account.clone(),
                affinity: Some(Affinity {
                    pod_anti_affinity: anti_affinity,
                    ..Default::default()
                }),
                ..Default::default()
            }),
        };
        let pod_template_json = serde_json::to_string(&pod_template_spec).unwrap();
        let mut hasher = Sha256::new();
        hasher.update(pod_template_json);
        let pod_template_hash = format!("{:x}", hasher.finalize());
        let pod_template_hash_annotation = "environmentd.materialize.cloud/pod-template-hash";
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
                pod_management_policy: Some("Parallel".to_string()),
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
        let stateful_sets = self.stateful_set_api.list(&self.list_params()).await?;
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

    fn watch_services(&self) -> BoxStream<'static, Result<ServiceEvent, anyhow::Error>> {
        fn into_service_event(pod: Pod) -> Result<ServiceEvent, anyhow::Error> {
            let process_id = pod.name_any().split('-').last().unwrap().parse()?;
            let service_id_label = "environmentd.materialize.cloud/service-id";
            let service_id = pod
                .labels()
                .get(service_id_label)
                .ok_or_else(|| anyhow!("missing label: {service_id_label}"))?
                .clone();

            let (pod_ready, last_probe_time) = pod
                .status
                .and_then(|status| status.conditions)
                .and_then(|conditions| conditions.into_iter().find(|c| c.type_ == "Ready"))
                .map(|c| (c.status == "True", c.last_probe_time))
                .unwrap_or((false, None));

            let status = if pod_ready {
                ServiceStatus::Ready
            } else {
                ServiceStatus::NotReady
            };
            let time = if let Some(time) = last_probe_time {
                time.0
            } else {
                Utc::now()
            };

            Ok(ServiceEvent {
                service_id,
                process_id,
                status,
                time,
            })
        }

        let stream = watcher(self.pod_api.clone(), self.list_params())
            .touched_objects()
            .map(|object| object.map_err(Into::into).and_then(into_service_event));
        Box::pin(stream)
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
