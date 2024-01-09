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
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::Utc;
use clap::ArgEnum;
use cloud_resource_controller::KubernetesResourceReader;
use futures::stream::{BoxStream, StreamExt};
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    Affinity, Container, ContainerPort, ContainerState, EnvVar, EnvVarSource,
    EphemeralVolumeSource, NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm,
    ObjectFieldSelector, ObjectReference, PersistentVolumeClaim, PersistentVolumeClaimSpec,
    PersistentVolumeClaimTemplate, Pod, PodAffinity, PodAffinityTerm, PodAntiAffinity,
    PodSecurityContext, PodSpec, PodTemplateSpec, PreferredSchedulingTerm, ResourceRequirements,
    Secret, Service as K8sService, ServicePort, ServiceSpec, Toleration, TopologySpreadConstraint,
    Volume, VolumeMount, WeightedPodAffinityTerm,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, LabelSelectorRequirement};
use kube::api::{Api, DeleteParams, ObjectMeta, Patch, PatchParams};
use kube::client::Client;
use kube::error::Error;
use kube::runtime::{watcher, WatchStreamExt};
use kube::ResourceExt;
use maplit::btreemap;
use mz_cloud_resources::crd::vpc_endpoint::v1::VpcEndpoint;
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_orchestrator::{
    scheduling_config::*, DiskLimit, LabelSelectionLogic, LabelSelector as MzLabelSelector,
    NamespacedOrchestrator, NotReadyReason, Orchestrator, Service, ServiceConfig, ServiceEvent,
    ServiceProcessMetrics, ServiceStatus,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tracing::warn;

pub mod cloud_resource_controller;
pub mod secrets;
pub mod util;

const FIELD_MANAGER: &str = "environmentd";
const NODE_FAILURE_THRESHOLD_SECONDS: i64 = 30;

/// Configures a [`KubernetesOrchestrator`].
#[derive(Debug, Clone)]
pub struct KubernetesOrchestratorConfig {
    /// The name of a Kubernetes context to use, if the Kubernetes configuration
    /// is loaded from the local kubeconfig.
    pub context: String,
    /// The name of a non-default Kubernetes scheduler to use, if any.
    pub scheduler_name: Option<String>,
    /// Labels to install on every service created by the orchestrator.
    pub service_labels: BTreeMap<String, String>,
    /// Node selector to install on every service created by the orchestrator.
    pub service_node_selector: BTreeMap<String, String>,
    /// The service account that each service should run as, if any.
    pub service_account: Option<String>,
    /// The image pull policy to set for services created by the orchestrator.
    pub image_pull_policy: KubernetesImagePullPolicy,
    /// An AWS external ID prefix to use when making AWS operations on behalf
    /// of the environment.
    pub aws_external_id_prefix: Option<AwsExternalIdPrefix>,
    /// Whether to use code coverage mode or not. Always false for production.
    pub coverage: bool,
    /// The Kubernetes StorageClass to use for the ephemeral volume attached to
    /// services that request disk.
    ///
    /// If unspecified, the orchestrator will refuse to create services that
    /// request disk.
    pub ephemeral_volume_storage_class: Option<String>,
    /// The optional fs group for service's pods' `securityContext`.
    pub service_fs_group: Option<i64>,
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
    vpc_endpoint_api: Api<VpcEndpoint>,
    namespaces: Mutex<BTreeMap<String, Arc<dyn NamespacedOrchestrator>>>,
    resource_reader: Arc<KubernetesResourceReader>,
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
        let resource_reader =
            Arc::new(KubernetesResourceReader::new(config.context.clone()).await?);
        Ok(KubernetesOrchestrator {
            client: client.clone(),
            kubernetes_namespace,
            config,
            secret_api: Api::default_namespaced(client.clone()),
            vpc_endpoint_api: Api::default_namespaced(client),
            namespaces: Mutex::new(BTreeMap::new()),
            resource_reader,
        })
    }
}

impl Orchestrator for KubernetesOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        let mut namespaces = self.namespaces.lock().expect("lock poisoned");
        Arc::clone(namespaces.entry(namespace.into()).or_insert_with(|| {
            Arc::new(NamespacedKubernetesOrchestrator {
                metrics_api: Api::default_namespaced(self.client.clone()),
                custom_metrics_api: Api::default_namespaced(self.client.clone()),
                service_api: Api::default_namespaced(self.client.clone()),
                stateful_set_api: Api::default_namespaced(self.client.clone()),
                pod_api: Api::default_namespaced(self.client.clone()),
                kubernetes_namespace: self.kubernetes_namespace.clone(),
                namespace: namespace.into(),
                config: self.config.clone(),
                // TODO(guswynn): make this configurable.
                scheduling_config: Default::default(),
                service_infos: std::sync::Mutex::new(BTreeMap::new()),
            })
        }))
    }
}

#[derive(Clone, Copy)]
struct ServiceInfo {
    scale: u16,
    disk: bool,
    disk_limit: Option<DiskLimit>,
}

struct NamespacedKubernetesOrchestrator {
    metrics_api: Api<PodMetrics>,
    custom_metrics_api: Api<MetricValueList>,
    service_api: Api<K8sService>,
    stateful_set_api: Api<StatefulSet>,
    pod_api: Api<Pod>,
    kubernetes_namespace: String,
    namespace: String,
    config: KubernetesOrchestratorConfig,
    scheduling_config: std::sync::RwLock<ServiceSchedulingConfig>,
    service_infos: std::sync::Mutex<BTreeMap<String, ServiceInfo>>,
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

#[derive(Deserialize, Clone, Debug)]
pub struct PodMetricsContainer {
    pub name: String,
    pub usage: PodMetricsContainerUsage,
}

#[derive(Deserialize, Clone, Debug)]
pub struct PodMetricsContainerUsage {
    pub cpu: Quantity,
    pub memory: Quantity,
}

#[derive(Deserialize, Clone, Debug)]
pub struct PodMetrics {
    pub metadata: ObjectMeta,
    pub timestamp: String,
    pub window: String,
    pub containers: Vec<PodMetricsContainer>,
}

impl k8s_openapi::Resource for PodMetrics {
    const GROUP: &'static str = "metrics.k8s.io";
    const KIND: &'static str = "PodMetrics";
    const VERSION: &'static str = "v1beta1";
    const API_VERSION: &'static str = "metrics.k8s.io/v1beta1";
    const URL_PATH_SEGMENT: &'static str = "pods";

    type Scope = k8s_openapi::NamespaceResourceScope;
}

impl k8s_openapi::Metadata for PodMetrics {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &Self::Ty {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut Self::Ty {
        &mut self.metadata
    }
}

// Note that these types are very weird. We are `get`-ing a
// `List` object, and lying about it having an `ObjectMeta`
// (it deserializes as empty, but we don't need it). The custom
// metrics API is designed this way, which is very non-standard.
// A discussion in the `kube` channel in the `tokio` discord
// confirmed that this layout + using `get_subresource` is the
// best way to handle this.

#[derive(Deserialize, Clone, Debug)]
pub struct MetricIdentifier {
    #[serde(rename = "metricName")]
    pub name: String,
    // We skip `selector` for now, as we don't use it
}

#[derive(Deserialize, Clone, Debug)]
pub struct MetricValue {
    #[serde(rename = "describedObject")]
    pub described_object: ObjectReference,
    #[serde(flatten)]
    pub metric_identifier: MetricIdentifier,
    pub timestamp: String,
    pub value: Quantity,
    // We skip `windowSeconds`, as we don't need it
}

#[derive(Deserialize, Clone, Debug)]
pub struct MetricValueList {
    pub metadata: ObjectMeta,
    pub items: Vec<MetricValue>,
}

impl k8s_openapi::Resource for MetricValueList {
    const GROUP: &'static str = "custom.metrics.k8s.io";
    const KIND: &'static str = "MetricValueList";
    const VERSION: &'static str = "v1beta1";
    const API_VERSION: &'static str = "custom.metrics.k8s.io/v1beta1";
    const URL_PATH_SEGMENT: &'static str = "persistentvolumeclaims";

    type Scope = k8s_openapi::NamespaceResourceScope;
}

impl k8s_openapi::Metadata for MetricValueList {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &Self::Ty {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut Self::Ty {
        &mut self.metadata
    }
}

impl NamespacedKubernetesOrchestrator {
    /// Return a `wather::Config` instance that limits results to the namespace
    /// assigned to this orchestrator.
    fn watch_pod_params(&self) -> watcher::Config {
        let ns_selector = format!(
            "environmentd.materialize.cloud/namespace={}",
            self.namespace
        );
        watcher::Config::default().labels(&ns_selector)
    }
    /// Convert a higher-level label key to the actual one we
    /// will give to Kubernetes
    fn make_label_key(&self, key: &str) -> String {
        format!("{}.environmentd.materialize.cloud/{}", self.namespace, key)
    }
    fn label_selector_to_k8s(
        &self,
        MzLabelSelector { label_name, logic }: MzLabelSelector,
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
            key: self.make_label_key(&label_name),
            operator: operator.to_string(),
            values: Some(values),
        };
        Ok(lsr)
    }
}

#[derive(Debug)]
struct ScaledQuantity {
    integral_part: u64,
    exponent: i8,
    base10: bool,
}

impl ScaledQuantity {
    pub fn try_to_integer(&self, scale: i8, base10: bool) -> Option<u64> {
        if base10 != self.base10 {
            return None;
        }
        let exponent = self.exponent - scale;
        let mut result = self.integral_part;
        let base = if self.base10 { 10 } else { 2 };
        if exponent < 0 {
            for _ in exponent..0 {
                result /= base;
            }
        } else {
            for _ in 0..exponent {
                result = result.checked_mul(2)?;
            }
        }
        Some(result)
    }
}

// Parse a k8s `Quantity` object
// into a numeric value.
//
// This is intended to support collecting CPU and Memory data.
// Thus, there are a few that things Kubernetes attempts to do, that we don't,
// because I've never observed metrics-server specifically sending them:
// (1) Handle negative numbers (because it's not useful for that use-case)
// (2) Handle non-integers (because I have never observed them being actually sent)
// (3) Handle scientific notation (e.g. 1.23e2)
fn parse_k8s_quantity(s: &str) -> Result<ScaledQuantity, anyhow::Error> {
    const DEC_SUFFIXES: &[(&str, i8)] = &[
        ("n", -9),
        ("u", -6),
        ("m", -3),
        ("", 0),
        ("k", 3), // yep, intentionally lowercase.
        ("M", 6),
        ("G", 9),
        ("T", 12),
        ("P", 15),
        ("E", 18),
    ];
    const BIN_SUFFIXES: &[(&str, i8)] = &[
        ("", 0),
        ("Ki", 10),
        ("Mi", 20),
        ("Gi", 30),
        ("Ti", 40),
        ("Pi", 50),
        ("Ei", 60),
    ];

    let (positive, s) = match s.chars().next() {
        Some('+') => (true, &s[1..]),
        Some('-') => (false, &s[1..]),
        _ => (true, s),
    };

    if !positive {
        anyhow::bail!("Negative numbers not supported")
    }

    fn is_suffix_char(ch: char) -> bool {
        "numkMGTPEKi".contains(ch)
    }
    let (num, suffix) = match s.find(is_suffix_char) {
        None => (s, ""),
        Some(idx) => s.split_at(idx),
    };
    let num: u64 = num.parse()?;
    let (exponent, base10) = if let Some((_, exponent)) =
        DEC_SUFFIXES.iter().find(|(target, _)| suffix == *target)
    {
        (exponent, true)
    } else if let Some((_, exponent)) = BIN_SUFFIXES.iter().find(|(target, _)| suffix == *target) {
        (exponent, false)
    } else {
        anyhow::bail!("Unrecognized suffix: {suffix}");
    };
    Ok(ScaledQuantity {
        integral_part: num,
        exponent: *exponent,
        base10,
    })
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedKubernetesOrchestrator {
    async fn fetch_service_metrics(
        &self,
        id: &str,
    ) -> Result<Vec<ServiceProcessMetrics>, anyhow::Error> {
        let info = if let Some(info) = self.service_infos.lock().expect("poisoned lock").get(id) {
            *info
        } else {
            // This should have been set in `ensure_service`.
            tracing::error!("Failed to get info for {id}");
            anyhow::bail!("Failed to get info for {id}");
        };
        /// Get metrics for a particular service and process, converting them into a sane (i.e., numeric) format.
        ///
        /// Note that we want to keep going even if a lookup fails for whatever reason,
        /// so this function is infallible. If we fail to get cpu or memory for a particular pod,
        /// we just log a warning and install `None` in the returned struct.
        async fn get_metrics(
            self_: &NamespacedKubernetesOrchestrator,
            id: &str,
            i: usize,
            disk: bool,
            disk_limit: Option<DiskLimit>,
        ) -> ServiceProcessMetrics {
            let name = format!("{}-{id}-{i}", self_.namespace);

            let disk_usage_fut = async {
                if disk {
                    Some(
                        self_
                            .custom_metrics_api
                            .get_subresource(
                                "kubelet_volume_stats_used_bytes",
                                // The CSI provider we use sets up persistentvolumeclaim's
                                // with `{pod name}-scratch` as the name. It also provides
                                // the metrics, and does so under the `persistentvolumeclaims`
                                // resource type, instead of `pods`.
                                &format!("{name}-scratch"),
                            )
                            .await,
                    )
                } else {
                    None
                }
            };
            let disk_capacity_fut = async {
                if disk {
                    Some(
                        self_
                            .custom_metrics_api
                            .get_subresource(
                                "kubelet_volume_stats_capacity_bytes",
                                &format!("{name}-scratch"),
                            )
                            .await,
                    )
                } else {
                    None
                }
            };
            let (metrics, disk_usage, disk_capacity) = match futures::future::join3(
                self_.metrics_api.get(&name),
                disk_usage_fut,
                disk_capacity_fut,
            )
            .await
            {
                (Ok(metrics), disk_usage, disk_capacity) => {
                    // TODO(guswynn): don't tolerate errors here, when we are more
                    // comfortable with `prometheus-adapter` in production
                    // (And we run it in ci).

                    let disk_usage = match disk_usage {
                        Some(Ok(disk_usage)) => Some(disk_usage),
                        Some(Err(e)) if !matches!(&e, Error::Api(e) if e.code == 404) => {
                            warn!(
                                "Failed to fetch `kubelet_volume_stats_used_bytes` for {name}: {e}"
                            );
                            None
                        }
                        _ => None,
                    };

                    let disk_capacity = match disk_capacity {
                        Some(Ok(disk_capacity)) => Some(disk_capacity),
                        Some(Err(e)) if !matches!(&e, Error::Api(e) if e.code == 404) => {
                            warn!("Failed to fetch `kubelet_volume_stats_capacity_bytes` for {name}: {e}");
                            None
                        }
                        _ => None,
                    };

                    (metrics, disk_usage, disk_capacity)
                }
                (Err(e), _, _) => {
                    warn!("Failed to get metrics for {name}: {e}");
                    return ServiceProcessMetrics::default();
                }
            };
            let Some(PodMetricsContainer {
                usage:
                    PodMetricsContainerUsage {
                        cpu: Quantity(cpu_str),
                        memory: Quantity(mem_str),
                    },
                ..
            }) = metrics.containers.get(0)
            else {
                warn!("metrics result contained no containers for {name}");
                return ServiceProcessMetrics::default();
            };

            let cpu = match parse_k8s_quantity(cpu_str) {
                Ok(q) => match q.try_to_integer(-9, true) {
                    Some(i) => Some(i),
                    None => {
                        tracing::error!("CPU value {q:? }out of range");
                        None
                    }
                },
                Err(e) => {
                    tracing::error!("Failed to parse CPU value {cpu_str}: {e}");
                    None
                }
            };
            let memory = match parse_k8s_quantity(mem_str) {
                Ok(q) => match q.try_to_integer(0, false) {
                    Some(i) => Some(i),
                    None => {
                        tracing::error!("Memory value {q:?} out of range");
                        None
                    }
                },
                Err(e) => {
                    tracing::error!("Failed to parse memory value {mem_str}: {e}");
                    None
                }
            };

            let disk_usage = match (disk_usage, disk_capacity) {
                (Some(disk_usage_metrics), Some(disk_capacity_metrics)) => {
                    if !disk_usage_metrics.items.is_empty()
                        && !disk_capacity_metrics.items.is_empty()
                    {
                        let disk_usage_str = &*disk_usage_metrics.items[0].value.0;
                        let disk_usage = match parse_k8s_quantity(disk_usage_str) {
                            Ok(q) => match q.try_to_integer(0, true) {
                                Some(i) => Some(i),
                                None => {
                                    tracing::error!("Disk usage value {q:?} out of range");
                                    None
                                }
                            },
                            Err(e) => {
                                tracing::error!(
                                    "Failed to parse disk usage value {disk_usage_str}: {e}"
                                );
                                None
                            }
                        };
                        let disk_capacity_str = &*disk_capacity_metrics.items[0].value.0;
                        let disk_capacity = match parse_k8s_quantity(disk_capacity_str) {
                            Ok(q) => match q.try_to_integer(0, true) {
                                Some(i) => Some(i),
                                None => {
                                    tracing::error!("Disk capacity value {q:?} out of range");
                                    None
                                }
                            },
                            Err(e) => {
                                tracing::error!(
                                    "Failed to parse disk capacity value {disk_capacity_str}: {e}"
                                );
                                None
                            }
                        };

                        // We only populate a `disk_usage` if we have all 5 of of:
                        // - a disk limit (so it must be an actual managed cluster with a real limit)
                        // - a reported disk capacity
                        // - a reported disk usage
                        // - disk capacity is less than disk limit.
                        // - There are no overflows
                        //
                        //
                        // The `disk_usage` is augmented with the difference between
                        // the limit and the `capacity`, so users don't erroneously think they
                        // have more real usable capacity than they actually do. The difference
                        // comes from LVM and filesystem overheads.
                        match (disk_usage, disk_capacity, disk_limit) {
                            (
                                Some(disk_usage),
                                Some(disk_capacity),
                                Some(DiskLimit(disk_limit)),
                            ) => {
                                if disk_limit.0 >= disk_capacity {
                                    if let Some(disk_usage) =
                                        (disk_limit.0 - disk_capacity).checked_add(disk_usage)
                                    {
                                        Some(disk_usage)
                                    } else {
                                        tracing::error!(
                                            "disk usage  {} + correction {} overflowed?",
                                            disk_usage,
                                            disk_limit.0 - disk_capacity
                                        );
                                        None
                                    }
                                } else {
                                    // `fetch_service_metrics` gets the `disk_limit` from the
                                    // `service_infos`, which can be _more_ up-to-date than the
                                    // actual replica when it is altered, so we warn instead of
                                    // error. This is the same as the warning above about not
                                    // finding the containers above, which could occur when the
                                    // `scale` is being altered.
                                    tracing::warn!(
                                        "disk capacity {} is larger than the disk limit {} ?",
                                        disk_capacity,
                                        disk_limit.0
                                    );
                                    None
                                }
                            }
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            };

            ServiceProcessMetrics {
                cpu_nano_cores: cpu,
                memory_bytes: memory,
                disk_usage_bytes: disk_usage,
            }
        }
        let ret = futures::future::join_all(
            (0..info.scale).map(|i| get_metrics(self, id, i.into(), info.disk, info.disk_limit)),
        );

        Ok(ret.await)
    }

    async fn ensure_service(
        &self,
        id: &str,
        ServiceConfig {
            image,
            init_container_image,
            args,
            ports: ports_in,
            memory_limit,
            cpu_limit,
            scale,
            labels: labels_in,
            availability_zones,
            other_replicas_selector,
            replicas_selector,
            disk,
            disk_limit,
            node_selector,
        }: ServiceConfig<'_>,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        // This is extremely cheap to clone, so just look into the lock once.
        let scheduling_config: ServiceSchedulingConfig =
            self.scheduling_config.read().expect("poisoned").clone();
        let disk = scheduling_config.always_use_disk || disk;

        let name = format!("{}-{id}", self.namespace);
        // The match labels should be the minimal set of labels that uniquely
        // identify the pods in the stateful set. Changing these after the
        // `StatefulSet` is created is not permitted by Kubernetes, and we're
        // not yet smart enough to handle deleting and recreating the
        // `StatefulSet`.
        let match_labels = btreemap! {
            "environmentd.materialize.cloud/namespace".into() => self.namespace.clone(),
            "environmentd.materialize.cloud/service-id".into() => id.into(),
        };
        let mut labels = match_labels.clone();
        for (key, value) in labels_in {
            labels.insert(self.make_label_key(&key), value);
        }

        labels.insert(self.make_label_key("scale"), scale.to_string());

        for port in &ports_in {
            labels.insert(
                format!("environmentd.materialize.cloud/port-{}", port.name),
                "true".into(),
            );
        }
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
                selector: Some(match_labels.clone()),
                ..Default::default()
            }),
            status: None,
        };

        let hosts = (0..scale)
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
            .collect::<BTreeMap<_, _>>();
        let listen_addrs = ports_in
            .iter()
            .map(|p| (p.name.clone(), format!("0.0.0.0:{}", p.port_hint)))
            .collect::<BTreeMap<_, _>>();
        let mut args = args(&listen_addrs);

        // This constrains the orchestrator (for those orchestrators that support
        // anti-affinity, today just k8s) to never schedule pods for different replicas
        // of the same cluster on the same node. Pods from the _same_ replica are fine;
        // pods from different clusters are also fine.
        //
        // The point is that if pods of two replicas are on the same node, that node
        // going down would kill both replicas, and so the replication factor of the
        // cluster in question is illusory.
        let anti_affinity = Some({
            let label_selector_requirements = other_replicas_selector
                .clone()
                .into_iter()
                .map(|ls| self.label_selector_to_k8s(ls))
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

            if !scheduling_config.soften_replication_anti_affinity {
                PodAntiAffinity {
                    required_during_scheduling_ignored_during_execution: Some(vec![pat]),
                    ..Default::default()
                }
            } else {
                PodAntiAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![
                        WeightedPodAffinityTerm {
                            weight: scheduling_config.soften_replication_anti_affinity_weight,
                            pod_affinity_term: pat,
                        },
                    ]),
                    ..Default::default()
                }
            }
        });

        let pod_affinity = if let Some(weight) = scheduling_config.multi_pod_az_affinity_weight {
            // `match_labels` sufficiently selects pods in the same replica.
            let ls = LabelSelector {
                match_labels: Some(match_labels.clone()),
                ..Default::default()
            };
            let pat = PodAffinityTerm {
                label_selector: Some(ls),
                topology_key: "topology.kubernetes.io/zone".to_string(),
                ..Default::default()
            };

            Some(PodAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        weight,
                        pod_affinity_term: pat,
                    },
                ]),
                ..Default::default()
            })
        } else {
            None
        };

        let topology_spread = if scheduling_config.topology_spread.enabled {
            let config = &scheduling_config.topology_spread;

            if !config.ignore_non_singular_scale || scale <= 1 {
                let label_selector_requirements = (if config.ignore_non_singular_scale {
                    let mut replicas_selector_ignoring_scale = replicas_selector.clone();

                    replicas_selector_ignoring_scale.push(mz_orchestrator::LabelSelector {
                        label_name: "scale".into(),
                        logic: mz_orchestrator::LabelSelectionLogic::Eq {
                            value: "1".to_string(),
                        },
                    });

                    replicas_selector_ignoring_scale
                } else {
                    replicas_selector
                })
                .into_iter()
                .map(|ls| self.label_selector_to_k8s(ls))
                .collect::<Result<Vec<_>, _>>()?;
                let ls = LabelSelector {
                    match_expressions: Some(label_selector_requirements),
                    ..Default::default()
                };

                let constraint = TopologySpreadConstraint {
                    label_selector: Some(ls),
                    min_domains: None,
                    max_skew: config.max_skew,
                    topology_key: "topology.kubernetes.io/zone".to_string(),
                    when_unsatisfiable: if config.soft {
                        "ScheduleAnyway".to_string()
                    } else {
                        "DoNotSchedule".to_string()
                    },
                    // TODO(guswynn): restore these once they are supported.
                    // Consider node affinities when calculating topology spread. This is the
                    // default: <https://docs.rs/k8s-openapi/latest/k8s_openapi/api/core/v1/struct.TopologySpreadConstraint.html#structfield.node_affinity_policy>,
                    // made explicit.
                    // node_affinity_policy: Some("Honor".to_string()),
                    // Do not consider node taints when calculating topology spread. This is the
                    // default: <https://docs.rs/k8s-openapi/latest/k8s_openapi/api/core/v1/struct.TopologySpreadConstraint.html#structfield.node_taints_policy>,
                    // made explicit.
                    // node_taints_policy: Some("Ignore".to_string()),
                    match_label_keys: None,
                    // Once the above are restorted, we should't have `..Default::default()` here because the specifics of these fields are
                    // subtle enough where we want compilation failures when we upgrade
                    ..Default::default()
                };
                Some(vec![constraint])
            } else {
                None
            }
        } else {
            None
        };

        let pod_annotations = btreemap! {
            // Prevent the cluster-autoscaler (or karpenter) from evicting these pods in attempts to scale down
            // and terminate nodes.
            // This will cost us more money, but should give us better uptime.
            // This does not prevent all evictions by Kubernetes, only the ones initiated by the
            // cluster-autoscaler (or karpenter). Notably, eviction of pods for resource overuse is still enabled.
            "cluster-autoscaler.kubernetes.io/safe-to-evict".to_owned() => "false".to_string(),
            "karpenter.sh/do-not-evict".to_owned() => "true".to_string(),

            // It's called do-not-disrupt in newer versions of karpenter, so adding for forward/backward compatibility
            "karpenter.sh/do-not-disrupt".to_owned() => "true".to_string(),
        };

        let default_node_selector = [("materialize.cloud/disk".to_string(), disk.to_string())];

        let node_selector: BTreeMap<String, String> = default_node_selector
            .into_iter()
            .chain(self.config.service_node_selector.clone())
            .chain(node_selector)
            .collect();

        let node_affinity = if let Some(availability_zones) = availability_zones {
            let selector = NodeSelectorTerm {
                match_expressions: Some(vec![NodeSelectorRequirement {
                    key: "materialize.cloud/availability-zone".to_string(),
                    operator: "In".to_string(),
                    values: Some(availability_zones),
                }]),
                match_fields: None,
            };

            if scheduling_config.soften_az_affinity {
                Some(NodeAffinity {
                    preferred_during_scheduling_ignored_during_execution: Some(vec![
                        PreferredSchedulingTerm {
                            preference: selector,
                            weight: scheduling_config.soften_az_affinity_weight,
                        },
                    ]),
                    required_during_scheduling_ignored_during_execution: None,
                })
            } else {
                Some(NodeAffinity {
                    preferred_during_scheduling_ignored_during_execution: None,
                    required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                        node_selector_terms: vec![selector],
                    }),
                })
            }
        } else {
            None
        };

        let container_name = image
            .splitn(2, '/')
            .skip(1)
            .next()
            .and_then(|name_version| name_version.splitn(2, ':').next())
            .context("`image` is not ORG/NAME:VERSION")?
            .to_string();

        let init_containers = init_container_image.map(|image| {
            vec![Container {
                name: "init".to_string(),
                image: Some(image),
                image_pull_policy: Some(self.config.image_pull_policy.to_string()),
                resources: Some(ResourceRequirements {
                    claims: None,
                    // Set both limits and requests to the same values, to ensure a
                    // `Guaranteed` QoS class for the pod.
                    limits: Some(limits.clone()),
                    requests: Some(limits.clone()),
                }),
                env: Some(vec![
                    EnvVar {
                        name: "MZ_NAMESPACE".to_string(),
                        value_from: Some(EnvVarSource {
                            field_ref: Some(ObjectFieldSelector {
                                field_path: "metadata.namespace".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    EnvVar {
                        name: "MZ_POD_NAME".to_string(),
                        value_from: Some(EnvVarSource {
                            field_ref: Some(ObjectFieldSelector {
                                field_path: "metadata.name".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                    EnvVar {
                        name: "MZ_NODE_NAME".to_string(),
                        value_from: Some(EnvVarSource {
                            field_ref: Some(ObjectFieldSelector {
                                field_path: "spec.nodeName".to_string(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }]
        });

        let env = if self.config.coverage {
            Some(vec![EnvVar {
                name: "LLVM_PROFILE_FILE".to_string(),
                value: Some(format!("/coverage/{}-%p-%9m%c.profraw", self.namespace)),
                ..Default::default()
            }])
        } else {
            None
        };

        let mut volume_mounts = vec![];

        if self.config.coverage {
            volume_mounts.push(VolumeMount {
                name: "coverage".to_string(),
                mount_path: "/coverage".to_string(),
                ..Default::default()
            })
        }

        let volumes = match (disk, &self.config.ephemeral_volume_storage_class) {
            (true, Some(ephemeral_volume_storage_class)) => {
                volume_mounts.push(VolumeMount {
                    name: "scratch".to_string(),
                    mount_path: "/scratch".to_string(),
                    ..Default::default()
                });
                args.push("--scratch-directory=/scratch".into());

                Some(vec![Volume {
                    name: "scratch".to_string(),
                    ephemeral: Some(EphemeralVolumeSource {
                        volume_claim_template: Some(PersistentVolumeClaimTemplate {
                            spec: PersistentVolumeClaimSpec {
                                access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                                storage_class_name: Some(
                                    ephemeral_volume_storage_class.to_string(),
                                ),
                                resources: Some(ResourceRequirements {
                                    requests: Some(BTreeMap::from([(
                                        "storage".to_string(),
                                        Quantity(
                                            disk_limit
                                                .unwrap_or(DiskLimit::ARBITRARY)
                                                .0
                                                .as_u64()
                                                .to_string(),
                                        ),
                                    )])),
                                    ..Default::default()
                                }),
                                ..Default::default()
                            },
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }])
            }
            (true, None) => {
                return Err(anyhow!(
                    "service requested disk but no ephemeral volume storage class was configured"
                ));
            }
            (false, _) => None,
        };

        let volume_claim_templates = if self.config.coverage {
            Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("coverage".to_string()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(ResourceRequirements {
                        requests: Some(BTreeMap::from([(
                            "storage".to_string(),
                            Quantity("10Gi".to_string()),
                        )])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }])
        } else {
            None
        };

        let security_context = if let Some(fs_group) = self.config.service_fs_group {
            Some(PodSecurityContext {
                fs_group: Some(fs_group),
                ..Default::default()
            })
        } else {
            None
        };

        let tolerations = Some(vec![
            // When the node becomes `NotReady` it indicates there is a problem
            // with the node. By default Kubernetes waits 300s (5 minutes)
            // before descheduling the pod, but we tune this to 30s for faster
            // recovery in the case of node failure.
            Toleration {
                effect: Some("NoExecute".into()),
                key: Some("node.kubernetes.io/not-ready".into()),
                operator: Some("Exists".into()),
                toleration_seconds: Some(NODE_FAILURE_THRESHOLD_SECONDS),
                value: None,
            },
            Toleration {
                effect: Some("NoExecute".into()),
                key: Some("node.kubernetes.io/unreachable".into()),
                operator: Some("Exists".into()),
                toleration_seconds: Some(NODE_FAILURE_THRESHOLD_SECONDS),
                value: None,
            },
        ]);

        let mut pod_template_spec = PodTemplateSpec {
            metadata: Some(ObjectMeta {
                labels: Some(labels.clone()),
                annotations: Some(pod_annotations), // Do not delete, we insert into it below.
                ..Default::default()
            }),
            spec: Some(PodSpec {
                init_containers,
                containers: vec![Container {
                    name: container_name,
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
                        claims: None,
                        // Set both limits and requests to the same values, to ensure a
                        // `Guaranteed` QoS class for the pod.
                        limits: Some(limits.clone()),
                        requests: Some(limits),
                    }),
                    volume_mounts: if !volume_mounts.is_empty() {
                        Some(volume_mounts)
                    } else {
                        None
                    },
                    env,
                    ..Default::default()
                }],
                volumes,
                security_context,
                node_selector: Some(node_selector),
                scheduler_name: self.config.scheduler_name.clone(),
                service_account: self.config.service_account.clone(),
                affinity: Some(Affinity {
                    pod_anti_affinity: anti_affinity,
                    pod_affinity,
                    node_affinity,
                    ..Default::default()
                }),
                topology_spread_constraints: topology_spread,
                tolerations,
                // Setting a 0s termination grace period has the side effect of
                // automatically starting a new pod when the previous pod is
                // currently terminating. This enables recovery from a node
                // failure with no manual intervention. Without this setting,
                // the StatefulSet controller will refuse to start a new pod
                // until the failed node is manually removed from the Kubernetes
                // cluster.
                //
                // The Kubernetes documentation strongly advises against this
                // setting, as StatefulSets attempt to provide "at most once"
                // semantics [0]--that is, the guarantee that for a given pod in
                // a StatefulSet there is *at most* one pod with that identity
                // running in the cluster.
                //
                // Materialize services, however, are carefully designed to
                // *not* rely on this guarantee. In fact, we do not believe that
                // correct distributed systems can meaningfully rely on
                // Kubernetes's guarantee--network packets from a pod can be
                // arbitrarily delayed, long past that pod's termination.
                //
                // [0]: https://kubernetes.io/docs/tasks/run-application/force-delete-stateful-set-pod/#statefulset-considerations
                termination_grace_period_seconds: Some(0),
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
                    match_labels: Some(match_labels),
                    ..Default::default()
                },
                service_name: name.clone(),
                replicas: Some(scale.into()),
                template: pod_template_spec,
                pod_management_policy: Some("Parallel".to_string()),
                volume_claim_templates,
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
        for pod_id in 0..scale {
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
        self.service_infos.lock().expect("poisoned lock").insert(
            id.to_string(),
            ServiceInfo {
                scale,
                disk,
                disk_limit,
            },
        );
        Ok(Box::new(KubernetesService { hosts, ports }))
    }

    /// Drops the identified service, if it exists.
    async fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        fail::fail_point!("kubernetes_drop_service", |_| Err(anyhow!("failpoint")));
        self.service_infos.lock().expect("poisoned lock").remove(id);
        let name = format!("{}-{id}", self.namespace);
        let res = self
            .stateful_set_api
            .delete(&name, &DeleteParams::default())
            .await;
        match res {
            Ok(_) => (),
            Err(Error::Api(e)) if e.code == 404 => (),
            Err(e) => return Err(e.into()),
        }

        let res = self
            .service_api
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
        let stateful_sets = self.stateful_set_api.list(&Default::default()).await?;
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

            fn is_state_oom(state: &ContainerState) -> bool {
                state
                    .terminated
                    .as_ref()
                    // 137 is the exit code corresponding to OOM in Kubernetes.
                    // It'd be a bit clearer to compare the reason to "OOMKilled",
                    // but this doesn't work in Kind for some reason, preventing us from
                    // writing automated tests.
                    .map(|terminated| terminated.exit_code == 137)
                    .unwrap_or(false)
            }
            let oomed = pod
                .status
                .as_ref()
                .and_then(|status| status.container_statuses.as_ref())
                .map(|container_statuses| {
                    container_statuses.iter().any(|cs| {
                        // We check whether the current _or_ the last state
                        // is an OOM kill. The reason for this is that after a kill,
                        // the state toggles from "Terminated" to "Waiting" very quickly,
                        // at which point the OOM error appears int he last state,
                        // not the current one.
                        //
                        // This "oomed" value is ignored later on if the pod is ready,
                        // so there is no risk that we will go directly from "Terminated"
                        // to "Running" and incorrectly report that we are currently
                        // oom-killed.
                        cs.last_state.as_ref().map(is_state_oom).unwrap_or(false)
                            || cs.state.as_ref().map(is_state_oom).unwrap_or(false)
                    })
                })
                .unwrap_or(false);

            let (pod_ready, last_probe_time) = pod
                .status
                .and_then(|status| status.conditions)
                .and_then(|conditions| conditions.into_iter().find(|c| c.type_ == "Ready"))
                .map(|c| (c.status == "True", c.last_probe_time))
                .unwrap_or((false, None));

            let status = if pod_ready {
                ServiceStatus::Ready
            } else {
                ServiceStatus::NotReady(oomed.then_some(NotReadyReason::OomKilled))
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

        let stream = watcher(self.pod_api.clone(), self.watch_pod_params())
            .touched_objects()
            .filter_map(|object| async move {
                match object {
                    Ok(pod) => Some(into_service_event(pod)),
                    Err(error) => {
                        // We assume that errors returned by Kubernetes are usually transient, so we
                        // just log a warning and ignore them otherwise.
                        tracing::warn!("service watch error: {error}");
                        None
                    }
                }
            });
        Box::pin(stream)
    }

    fn update_scheduling_config(&self, config: ServiceSchedulingConfig) {
        *self.scheduling_config.write().expect("poisoned") = config;
    }
}

#[derive(Debug, Clone)]
struct KubernetesService {
    hosts: Vec<String>,
    ports: BTreeMap<String, u16>,
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
