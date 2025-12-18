// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZero;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{env, fmt};

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use chrono::Utc;
use clap::ValueEnum;
use cloud_resource_controller::KubernetesResourceReader;
use futures::TryFutureExt;
use futures::stream::{BoxStream, StreamExt};
use k8s_openapi::DeepMerge;
use k8s_openapi::api::apps::v1::{StatefulSet, StatefulSetSpec, StatefulSetUpdateStrategy};
use k8s_openapi::api::core::v1::{
    Affinity, Capabilities, Container, ContainerPort, EnvVar, EnvVarSource, EphemeralVolumeSource,
    NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, ObjectFieldSelector,
    ObjectReference, PersistentVolumeClaim, PersistentVolumeClaimSpec,
    PersistentVolumeClaimTemplate, Pod, PodAffinity, PodAffinityTerm, PodAntiAffinity,
    PodSecurityContext, PodSpec, PodTemplateSpec, PreferredSchedulingTerm, ResourceRequirements,
    SeccompProfile, Secret, SecurityContext, Service as K8sService, ServicePort, ServiceSpec,
    Toleration, TopologySpreadConstraint, Volume, VolumeMount, VolumeResourceRequirements,
    WeightedPodAffinityTerm,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
    LabelSelector, LabelSelectorRequirement, OwnerReference,
};
use kube::ResourceExt;
use kube::api::{Api, DeleteParams, ObjectMeta, PartialObjectMetaExt, Patch, PatchParams};
use kube::client::Client;
use kube::error::Error as K8sError;
use kube::runtime::{WatchStreamExt, watcher};
use maplit::btreemap;
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_cloud_resources::crd::vpc_endpoint::v1::VpcEndpoint;
use mz_orchestrator::{
    DiskLimit, LabelSelectionLogic, LabelSelector as MzLabelSelector, NamespacedOrchestrator,
    OfflineReason, Orchestrator, Service, ServiceAssignments, ServiceConfig, ServiceEvent,
    ServiceProcessMetrics, ServiceStatus, scheduling_config::*,
};
use mz_ore::cast::CastInto;
use mz_ore::retry::Retry;
use mz_ore::task::AbortOnDropHandle;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

pub mod cloud_resource_controller;
pub mod secrets;
pub mod util;

const FIELD_MANAGER: &str = "environmentd";
const NODE_FAILURE_THRESHOLD_SECONDS: i64 = 30;

const POD_TEMPLATE_HASH_ANNOTATION: &str = "environmentd.materialize.cloud/pod-template-hash";

/// Configures a [`KubernetesOrchestrator`].
#[derive(Debug, Clone)]
pub struct KubernetesOrchestratorConfig {
    /// The name of a Kubernetes context to use, if the Kubernetes configuration
    /// is loaded from the local kubeconfig.
    pub context: String,
    /// The name of a non-default Kubernetes scheduler to use, if any.
    pub scheduler_name: Option<String>,
    /// Annotations to install on every service created by the orchestrator.
    pub service_annotations: BTreeMap<String, String>,
    /// Labels to install on every service created by the orchestrator.
    pub service_labels: BTreeMap<String, String>,
    /// Node selector to install on every service created by the orchestrator.
    pub service_node_selector: BTreeMap<String, String>,
    /// Affinity to install on every service created by the orchestrator.
    pub service_affinity: Option<String>,
    /// Tolerations to install on every service created by the orchestrator.
    pub service_tolerations: Option<String>,
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
    /// The prefix to prepend to all object names
    pub name_prefix: Option<String>,
    /// Whether we should attempt to collect metrics from kubernetes
    pub collect_pod_metrics: bool,
    /// Whether to annotate pods for prometheus service discovery.
    pub enable_prometheus_scrape_annotations: bool,
}

impl KubernetesOrchestratorConfig {
    pub fn name_prefix(&self) -> String {
        self.name_prefix.clone().unwrap_or_default()
    }
}

/// Specifies whether Kubernetes should pull Docker images when creating pods.
#[derive(ValueEnum, Debug, Clone, Copy)]
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

impl KubernetesImagePullPolicy {
    pub fn as_kebab_case_str(&self) -> &'static str {
        match self {
            Self::Always => "always",
            Self::IfNotPresent => "if-not-present",
            Self::Never => "never",
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
            let (command_tx, command_rx) = mpsc::unbounded_channel();
            let worker = OrchestratorWorker {
                metrics_api: Api::default_namespaced(self.client.clone()),
                service_api: Api::default_namespaced(self.client.clone()),
                stateful_set_api: Api::default_namespaced(self.client.clone()),
                pod_api: Api::default_namespaced(self.client.clone()),
                owner_references: vec![],
                command_rx,
                name_prefix: self.config.name_prefix.clone().unwrap_or_default(),
                collect_pod_metrics: self.config.collect_pod_metrics,
            }
            .spawn(format!("kubernetes-orchestrator-worker:{namespace}"));

            Arc::new(NamespacedKubernetesOrchestrator {
                pod_api: Api::default_namespaced(self.client.clone()),
                kubernetes_namespace: self.kubernetes_namespace.clone(),
                namespace: namespace.into(),
                config: self.config.clone(),
                // TODO(guswynn): make this configurable.
                scheduling_config: Default::default(),
                service_infos: std::sync::Mutex::new(BTreeMap::new()),
                command_tx,
                _worker: worker,
            })
        }))
    }
}

#[derive(Clone, Copy)]
struct ServiceInfo {
    scale: NonZero<u16>,
}

struct NamespacedKubernetesOrchestrator {
    pod_api: Api<Pod>,
    kubernetes_namespace: String,
    namespace: String,
    config: KubernetesOrchestratorConfig,
    scheduling_config: std::sync::RwLock<ServiceSchedulingConfig>,
    service_infos: std::sync::Mutex<BTreeMap<String, ServiceInfo>>,
    command_tx: mpsc::UnboundedSender<WorkerCommand>,
    _worker: AbortOnDropHandle<()>,
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

/// Commands sent from a [`NamespacedKubernetesOrchestrator`] to its
/// [`OrchestratorWorker`].
///
/// Commands for which the caller expects a result include a `result_tx` on which the
/// [`OrchestratorWorker`] will deliver the result.
enum WorkerCommand {
    EnsureService {
        desc: ServiceDescription,
    },
    DropService {
        name: String,
    },
    ListServices {
        namespace: String,
        result_tx: oneshot::Sender<Vec<String>>,
    },
    FetchServiceMetrics {
        name: String,
        info: ServiceInfo,
        result_tx: oneshot::Sender<Vec<ServiceProcessMetrics>>,
    },
}

/// A description of a service to be created by an [`OrchestratorWorker`].
#[derive(Debug, Clone)]
struct ServiceDescription {
    name: String,
    scale: NonZero<u16>,
    service: K8sService,
    stateful_set: StatefulSet,
    pod_template_hash: String,
}

/// A task executing blocking work for a [`NamespacedKubernetesOrchestrator`] in the background.
///
/// This type exists to enable making [`NamespacedKubernetesOrchestrator::ensure_service`] and
/// [`NamespacedKubernetesOrchestrator::drop_service`] non-blocking, allowing invocation of these
/// methods in latency-sensitive contexts.
///
/// Note that, apart from `ensure_service` and `drop_service`, this worker also handles blocking
/// orchestrator calls that query service state (such as `list_services`). These need to be
/// sequenced through the worker loop to ensure they linearize as expected. For example, we want to
/// ensure that a `list_services` result contains exactly those services that were previously
/// created with `ensure_service` and not yet dropped with `drop_service`.
struct OrchestratorWorker {
    metrics_api: Api<PodMetrics>,
    service_api: Api<K8sService>,
    stateful_set_api: Api<StatefulSet>,
    pod_api: Api<Pod>,
    owner_references: Vec<OwnerReference>,
    command_rx: mpsc::UnboundedReceiver<WorkerCommand>,
    name_prefix: String,
    collect_pod_metrics: bool,
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

impl NamespacedKubernetesOrchestrator {
    fn service_name(&self, id: &str) -> String {
        format!(
            "{}{}-{id}",
            self.config.name_prefix.as_deref().unwrap_or(""),
            self.namespace
        )
    }

    /// Return a `watcher::Config` instance that limits results to the namespace
    /// assigned to this orchestrator.
    fn watch_pod_params(&self) -> watcher::Config {
        let ns_selector = format!(
            "environmentd.materialize.cloud/namespace={}",
            self.namespace
        );
        // This watcher timeout must be shorter than the client read timeout.
        watcher::Config::default().timeout(59).labels(&ns_selector)
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

    fn send_command(&self, cmd: WorkerCommand) {
        self.command_tx.send(cmd).expect("worker task not dropped");
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
                result = result.checked_mul(base)?;
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

        let (result_tx, result_rx) = oneshot::channel();
        self.send_command(WorkerCommand::FetchServiceMetrics {
            name: self.service_name(id),
            info,
            result_tx,
        });

        let metrics = result_rx.await.expect("worker task not dropped");
        Ok(metrics)
    }

    fn ensure_service(
        &self,
        id: &str,
        ServiceConfig {
            image,
            init_container_image,
            args,
            ports: ports_in,
            memory_limit,
            memory_request,
            cpu_limit,
            scale,
            labels: labels_in,
            annotations: annotations_in,
            availability_zones,
            other_replicas_selector,
            replicas_selector,
            disk_limit,
            node_selector,
        }: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        // This is extremely cheap to clone, so just look into the lock once.
        let scheduling_config: ServiceSchedulingConfig =
            self.scheduling_config.read().expect("poisoned").clone();

        // Enable disk if the size does not disable it.
        let disk = disk_limit != Some(DiskLimit::ZERO);

        let name = self.service_name(id);
        // The match labels should be the minimal set of labels that uniquely
        // identify the pods in the stateful set. Changing these after the
        // `StatefulSet` is created is not permitted by Kubernetes, and we're
        // not yet smart enough to handle deleting and recreating the
        // `StatefulSet`.
        let mut match_labels = btreemap! {
            "environmentd.materialize.cloud/namespace".into() => self.namespace.clone(),
            "environmentd.materialize.cloud/service-id".into() => id.into(),
        };
        for (key, value) in &self.config.service_labels {
            match_labels.insert(key.clone(), value.clone());
        }

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
        let mut limits = BTreeMap::new();
        let mut requests = BTreeMap::new();
        if let Some(memory_limit) = memory_limit {
            limits.insert(
                "memory".into(),
                Quantity(memory_limit.0.as_u64().to_string()),
            );
            requests.insert(
                "memory".into(),
                Quantity(memory_limit.0.as_u64().to_string()),
            );
        }
        if let Some(memory_request) = memory_request {
            requests.insert(
                "memory".into(),
                Quantity(memory_request.0.as_u64().to_string()),
            );
        }
        if let Some(cpu_limit) = cpu_limit {
            limits.insert(
                "cpu".into(),
                Quantity(format!("{}m", cpu_limit.as_millicpus())),
            );
            requests.insert(
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
                cluster_ip: Some("None".to_string()),
                selector: Some(match_labels.clone()),
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
            .collect::<BTreeMap<_, _>>();

        let mut listen_addrs = BTreeMap::new();
        let mut peer_addrs = vec![BTreeMap::new(); hosts.len()];
        for (name, port) in &ports {
            listen_addrs.insert(name.clone(), format!("0.0.0.0:{port}"));
            for (i, host) in hosts.iter().enumerate() {
                peer_addrs[i].insert(name.clone(), format!("{host}:{port}"));
            }
        }
        let mut args = args(ServiceAssignments {
            listen_addrs: &listen_addrs,
            peer_addrs: &peer_addrs,
        });

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

            if !config.ignore_non_singular_scale || scale.get() == 1 {
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
                    min_domains: config.min_domains,
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

        let mut pod_annotations = btreemap! {
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
        for (key, value) in annotations_in {
            // We want to use the same prefix as our labels keys
            pod_annotations.insert(self.make_label_key(&key), value);
        }
        if self.config.enable_prometheus_scrape_annotations {
            if let Some(internal_http_port) = ports_in
                .iter()
                .find(|port| port.name == "internal-http")
                .map(|port| port.port_hint.to_string())
            {
                // Enable prometheus scrape discovery
                pod_annotations.insert("prometheus.io/scrape".to_owned(), "true".to_string());
                pod_annotations.insert("prometheus.io/port".to_owned(), internal_http_port);
                pod_annotations.insert("prometheus.io/path".to_owned(), "/metrics".to_string());
                pod_annotations.insert("prometheus.io/scheme".to_owned(), "http".to_string());
            }
        }
        for (key, value) in &self.config.service_annotations {
            pod_annotations.insert(key.clone(), value.clone());
        }

        let default_node_selector = if disk {
            vec![("materialize.cloud/disk".to_string(), disk.to_string())]
        } else {
            // if the cluster doesn't require disk, we can omit the selector
            // allowing it to be scheduled onto nodes with and without the
            // selector
            vec![]
        };

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

        let mut affinity = Affinity {
            pod_anti_affinity: anti_affinity,
            pod_affinity,
            node_affinity,
            ..Default::default()
        };
        if let Some(service_affinity) = &self.config.service_affinity {
            affinity.merge_from(serde_json::from_str(service_affinity)?);
        }

        let container_name = image
            .rsplit_once('/')
            .and_then(|(_, name_version)| name_version.rsplit_once(':'))
            .context("`image` is not ORG/NAME:VERSION")?
            .0
            .to_string();

        let container_security_context = if scheduling_config.security_context_enabled {
            Some(SecurityContext {
                privileged: Some(false),
                run_as_non_root: Some(true),
                allow_privilege_escalation: Some(false),
                seccomp_profile: Some(SeccompProfile {
                    type_: "RuntimeDefault".to_string(),
                    ..Default::default()
                }),
                capabilities: Some(Capabilities {
                    drop: Some(vec!["ALL".to_string()]),
                    ..Default::default()
                }),
                ..Default::default()
            })
        } else {
            None
        };

        let init_containers = init_container_image.map(|image| {
            vec![Container {
                name: "init".to_string(),
                image: Some(image),
                image_pull_policy: Some(self.config.image_pull_policy.to_string()),
                resources: Some(ResourceRequirements {
                    claims: None,
                    limits: Some(limits.clone()),
                    requests: Some(requests.clone()),
                }),
                security_context: container_security_context.clone(),
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
                                resources: Some(VolumeResourceRequirements {
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

        if let Some(name_prefix) = &self.config.name_prefix {
            args.push(format!("--secrets-reader-name-prefix={}", name_prefix));
        }

        let volume_claim_templates = if self.config.coverage {
            Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta {
                    name: Some("coverage".to_string()),
                    ..Default::default()
                },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".to_string()]),
                    resources: Some(VolumeResourceRequirements {
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
                run_as_user: Some(fs_group),
                run_as_group: Some(fs_group),
                ..Default::default()
            })
        } else {
            None
        };

        let mut tolerations = vec![
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
        ];
        if let Some(service_tolerations) = &self.config.service_tolerations {
            tolerations.extend(serde_json::from_str::<Vec<_>>(service_tolerations)?);
        }
        let tolerations = Some(tolerations);

        let mut pod_template_spec = PodTemplateSpec {
            metadata: Some(ObjectMeta {
                labels: Some(labels.clone()),
                // Only set `annotations` _after_ we have computed the pod template hash, to
                // avoid that annotation changes cause pod replacements.
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
                    security_context: container_security_context.clone(),
                    resources: Some(ResourceRequirements {
                        claims: None,
                        limits: Some(limits),
                        requests: Some(requests),
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
                affinity: Some(affinity),
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
        pod_annotations.insert(
            POD_TEMPLATE_HASH_ANNOTATION.to_owned(),
            pod_template_hash.clone(),
        );

        pod_template_spec.metadata.as_mut().unwrap().annotations = Some(pod_annotations);

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
                service_name: Some(name.clone()),
                replicas: Some(scale.cast_into()),
                template: pod_template_spec,
                update_strategy: Some(StatefulSetUpdateStrategy {
                    type_: Some("OnDelete".to_owned()),
                    ..Default::default()
                }),
                pod_management_policy: Some("Parallel".to_string()),
                volume_claim_templates,
                ..Default::default()
            }),
            status: None,
        };

        self.send_command(WorkerCommand::EnsureService {
            desc: ServiceDescription {
                name,
                scale,
                service,
                stateful_set,
                pod_template_hash,
            },
        });

        self.service_infos
            .lock()
            .expect("poisoned lock")
            .insert(id.to_string(), ServiceInfo { scale });

        Ok(Box::new(KubernetesService { hosts, ports }))
    }

    /// Drops the identified service, if it exists.
    fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        fail::fail_point!("kubernetes_drop_service", |_| Err(anyhow!("failpoint")));
        self.service_infos.lock().expect("poisoned lock").remove(id);

        self.send_command(WorkerCommand::DropService {
            name: self.service_name(id),
        });

        Ok(())
    }

    /// Lists the identifiers of all known services.
    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        let (result_tx, result_rx) = oneshot::channel();
        self.send_command(WorkerCommand::ListServices {
            namespace: self.namespace.clone(),
            result_tx,
        });

        let list = result_rx.await.expect("worker task not dropped");
        Ok(list)
    }

    fn watch_services(&self) -> BoxStream<'static, Result<ServiceEvent, anyhow::Error>> {
        fn into_service_event(pod: Pod) -> Result<ServiceEvent, anyhow::Error> {
            let process_id = pod.name_any().split('-').next_back().unwrap().parse()?;
            let service_id_label = "environmentd.materialize.cloud/service-id";
            let service_id = pod
                .labels()
                .get(service_id_label)
                .ok_or_else(|| anyhow!("missing label: {service_id_label}"))?
                .clone();

            let oomed = pod
                .status
                .as_ref()
                .and_then(|status| status.container_statuses.as_ref())
                .map(|container_statuses| {
                    container_statuses.iter().any(|cs| {
                        // The container might have already transitioned from "terminated" to
                        // "waiting"/"running" state, in which case we need to check its previous
                        // state to find out why it terminated.
                        let current_state = cs.state.as_ref().and_then(|s| s.terminated.as_ref());
                        let last_state = cs.last_state.as_ref().and_then(|s| s.terminated.as_ref());
                        let termination_state = current_state.or(last_state);

                        // The interesting exit codes are:
                        //  * 135 (SIGBUS): occurs when lgalloc runs out of disk
                        //  * 137 (SIGKILL): occurs when the OOM killer terminates the container
                        //  * 167: occurs when the lgalloc or memory limiter terminates the process
                        // We treat the all of these as OOM conditions since swap and lgalloc use
                        // disk only for spilling memory.
                        let exit_code = termination_state.map(|s| s.exit_code);
                        exit_code.is_some_and(|e| [135, 137, 167].contains(&e))
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
                ServiceStatus::Online
            } else {
                ServiceStatus::Offline(oomed.then_some(OfflineReason::OomKilled))
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

impl OrchestratorWorker {
    fn spawn(self, name: String) -> AbortOnDropHandle<()> {
        mz_ore::task::spawn(|| name, self.run()).abort_on_drop()
    }

    async fn run(mut self) {
        {
            info!("initializing Kubernetes orchestrator worker");
            let start = Instant::now();

            // Fetch the owner reference for our own pod (usually a
            // StatefulSet), so that we can propagate it to the services we
            // create.
            let hostname = env::var("HOSTNAME").unwrap_or_else(|_| panic!("HOSTNAME environment variable missing or invalid; required for Kubernetes orchestrator"));
            let orchestrator_pod = Retry::default()
                .clamp_backoff(Duration::from_secs(10))
                .retry_async(|_| self.pod_api.get(&hostname))
                .await
                .expect("always retries on error");
            self.owner_references
                .extend(orchestrator_pod.owner_references().into_iter().cloned());

            info!(
                "Kubernetes orchestrator worker initialized in {:?}",
                start.elapsed()
            );
        }

        while let Some(cmd) = self.command_rx.recv().await {
            self.handle_command(cmd).await;
        }
    }

    /// Handle a worker command.
    ///
    /// If handling the command fails, it is automatically retried. All command handlers return
    /// [`K8sError`], so we can reasonably assume that a failure is caused by issues communicating
    /// with the K8S server and that retrying resolves them eventually.
    async fn handle_command(&self, cmd: WorkerCommand) {
        async fn retry<F, U, R>(f: F, cmd_type: &str) -> R
        where
            F: Fn() -> U,
            U: Future<Output = Result<R, K8sError>>,
        {
            Retry::default()
                .clamp_backoff(Duration::from_secs(10))
                .retry_async(|_| {
                    f().map_err(
                        |error| tracing::error!(%cmd_type, "orchestrator call failed: {error}"),
                    )
                })
                .await
                .expect("always retries on error")
        }

        use WorkerCommand::*;
        match cmd {
            EnsureService { desc } => {
                retry(|| self.ensure_service(desc.clone()), "EnsureService").await
            }
            DropService { name } => retry(|| self.drop_service(&name), "DropService").await,
            ListServices {
                namespace,
                result_tx,
            } => {
                let result = retry(|| self.list_services(&namespace), "ListServices").await;
                let _ = result_tx.send(result);
            }
            FetchServiceMetrics {
                name,
                info,
                result_tx,
            } => {
                let result = self.fetch_service_metrics(&name, &info).await;
                let _ = result_tx.send(result);
            }
        }
    }

    async fn fetch_service_metrics(
        &self,
        name: &str,
        info: &ServiceInfo,
    ) -> Vec<ServiceProcessMetrics> {
        if !self.collect_pod_metrics {
            return (0..info.scale.get())
                .map(|_| ServiceProcessMetrics::default())
                .collect();
        }

        /// Usage metrics reported by clusterd processes.
        #[derive(Deserialize)]
        pub(crate) struct ClusterdUsage {
            disk_bytes: Option<u64>,
            memory_bytes: Option<u64>,
            swap_bytes: Option<u64>,
            heap_limit: Option<u64>,
        }

        /// Get metrics for a particular service and process, converting them into a sane (i.e., numeric) format.
        ///
        /// Note that we want to keep going even if a lookup fails for whatever reason,
        /// so this function is infallible. If we fail to get cpu or memory for a particular pod,
        /// we just log a warning and install `None` in the returned struct.
        async fn get_metrics(
            self_: &OrchestratorWorker,
            service_name: &str,
            i: usize,
        ) -> ServiceProcessMetrics {
            let name = format!("{service_name}-{i}");

            let clusterd_usage_fut = get_clusterd_usage(self_, service_name, i);
            let (metrics, clusterd_usage) =
                match futures::future::join(self_.metrics_api.get(&name), clusterd_usage_fut).await
                {
                    (Ok(metrics), Ok(clusterd_usage)) => (metrics, Some(clusterd_usage)),
                    (Ok(metrics), Err(e)) => {
                        warn!("Failed to fetch clusterd usage for {name}: {e}");
                        (metrics, None)
                    }
                    (Err(e), _) => {
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

            let mut process_metrics = ServiceProcessMetrics::default();

            match parse_k8s_quantity(cpu_str) {
                Ok(q) => match q.try_to_integer(-9, true) {
                    Some(nano_cores) => process_metrics.cpu_nano_cores = Some(nano_cores),
                    None => error!("CPU value {q:?} out of range"),
                },
                Err(e) => error!("failed to parse CPU value {cpu_str}: {e}"),
            }
            match parse_k8s_quantity(mem_str) {
                Ok(q) => match q.try_to_integer(0, false) {
                    Some(mem) => process_metrics.memory_bytes = Some(mem),
                    None => error!("memory value {q:?} out of range"),
                },
                Err(e) => error!("failed to parse memory value {mem_str}: {e}"),
            }

            if let Some(usage) = clusterd_usage {
                // clusterd may report disk usage as either `disk_bytes`, or `swap_bytes`, or both.
                //
                // For now the Console expects the swap size to be reported in `disk_bytes`.
                // Once the Console has been ported to use `heap_bytes`/`heap_limit`, we can
                // simplify things by setting `process_metrics.disk_bytes = usage.disk_bytes`.
                process_metrics.disk_bytes = match (usage.disk_bytes, usage.swap_bytes) {
                    (Some(disk), Some(swap)) => Some(disk + swap),
                    (disk, swap) => disk.or(swap),
                };

                // clusterd may report heap usage as `memory_bytes` and optionally `swap_bytes`.
                // If no `memory_bytes` is reported, we can't know the heap usage.
                process_metrics.heap_bytes = match (usage.memory_bytes, usage.swap_bytes) {
                    (Some(memory), Some(swap)) => Some(memory + swap),
                    (Some(memory), None) => Some(memory),
                    (None, _) => None,
                };

                process_metrics.heap_limit = usage.heap_limit;
            }

            process_metrics
        }

        /// Get the current usage metrics exposed by a clusterd process.
        ///
        /// Usage metrics are collected by connecting to a metrics endpoint exposed by the process.
        /// The endpoint is assumed to be reachable at the 'internal-http' under the HTTP path
        /// `/api/usage-metrics`.
        async fn get_clusterd_usage(
            self_: &OrchestratorWorker,
            service_name: &str,
            i: usize,
        ) -> anyhow::Result<ClusterdUsage> {
            let service = self_
                .service_api
                .get(service_name)
                .await
                .with_context(|| format!("failed to get service {service_name}"))?;
            let namespace = service
                .metadata
                .namespace
                .context("missing service namespace")?;
            let internal_http_port = service
                .spec
                .and_then(|spec| spec.ports)
                .and_then(|ports| {
                    ports
                        .into_iter()
                        .find(|p| p.name == Some("internal-http".into()))
                })
                .map(|p| p.port);
            let Some(port) = internal_http_port else {
                bail!("internal-http port missing in service spec");
            };
            let metrics_url = format!(
                "http://{service_name}-{i}.{service_name}.{namespace}.svc.cluster.local:{port}\
                 /api/usage-metrics"
            );

            let http_client = reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .context("error building HTTP client")?;
            let resp = http_client.get(metrics_url).send().await?;
            let usage = resp.json().await?;

            Ok(usage)
        }

        let ret = futures::future::join_all(
            (0..info.scale.cast_into()).map(|i| get_metrics(self, name, i)),
        );

        ret.await
    }

    async fn ensure_service(&self, mut desc: ServiceDescription) -> Result<(), K8sError> {
        // We inject our own pod's owner references into the Kubernetes objects
        // created for the service so that if the
        // Deployment/StatefulSet/whatever that owns the pod running the
        // orchestrator gets deleted, so do all services spawned by this
        // orchestrator.
        desc.service
            .metadata
            .owner_references
            .get_or_insert(vec![])
            .extend(self.owner_references.iter().cloned());
        desc.stateful_set
            .metadata
            .owner_references
            .get_or_insert(vec![])
            .extend(self.owner_references.iter().cloned());

        let ss_spec = desc.stateful_set.spec.as_ref().unwrap();
        let pod_metadata = ss_spec.template.metadata.as_ref().unwrap();
        let pod_annotations = pod_metadata.annotations.clone();

        self.service_api
            .patch(
                &desc.name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(desc.service),
            )
            .await?;
        self.stateful_set_api
            .patch(
                &desc.name,
                &PatchParams::apply(FIELD_MANAGER).force(),
                &Patch::Apply(desc.stateful_set),
            )
            .await?;

        // We manage pod recreation manually, using the OnDelete StatefulSet update strategy, for
        // two reasons:
        //  * Kubernetes doesn't always automatically replace StatefulSet pods when their specs
        //    change, see https://github.com/kubernetes/kubernetes#67250.
        //  * Kubernetes replaces StatefulSet pods when their annotations change, which is not
        //    something we want as it could cause unavailability.
        //
        // Our pod recreation policy is simple: If a pod's template hash changed, delete it, and
        // let the StatefulSet controller recreate it. Otherwise, patch the existing pod's
        // annotations to line up with the ones in the spec.
        for pod_id in 0..desc.scale.get() {
            let pod_name = format!("{}-{pod_id}", desc.name);
            let pod = match self.pod_api.get(&pod_name).await {
                Ok(pod) => pod,
                // Pod already doesn't exist.
                Err(kube::Error::Api(e)) if e.code == 404 => continue,
                Err(e) => return Err(e),
            };

            let result = if pod.annotations().get(POD_TEMPLATE_HASH_ANNOTATION)
                != Some(&desc.pod_template_hash)
            {
                self.pod_api
                    .delete(&pod_name, &DeleteParams::default())
                    .await
                    .map(|_| ())
            } else {
                let metadata = ObjectMeta {
                    annotations: pod_annotations.clone(),
                    ..Default::default()
                }
                .into_request_partial::<Pod>();
                self.pod_api
                    .patch_metadata(
                        &pod_name,
                        &PatchParams::apply(FIELD_MANAGER).force(),
                        &Patch::Apply(&metadata),
                    )
                    .await
                    .map(|_| ())
            };

            match result {
                Ok(()) => (),
                // Pod was deleted concurrently.
                Err(kube::Error::Api(e)) if e.code == 404 => continue,
                Err(e) => return Err(e),
            }
        }

        Ok(())
    }

    async fn drop_service(&self, name: &str) -> Result<(), K8sError> {
        let res = self
            .stateful_set_api
            .delete(name, &DeleteParams::default())
            .await;
        match res {
            Ok(_) => (),
            Err(K8sError::Api(e)) if e.code == 404 => (),
            Err(e) => return Err(e),
        }

        let res = self
            .service_api
            .delete(name, &DeleteParams::default())
            .await;
        match res {
            Ok(_) => Ok(()),
            Err(K8sError::Api(e)) if e.code == 404 => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn list_services(&self, namespace: &str) -> Result<Vec<String>, K8sError> {
        let stateful_sets = self.stateful_set_api.list(&Default::default()).await?;
        let name_prefix = format!("{}{namespace}-", self.name_prefix);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn k8s_quantity_base10_large() {
        let cases = &[
            ("42", 42),
            ("42k", 42000),
            ("42M", 42000000),
            ("42G", 42000000000),
            ("42T", 42000000000000),
            ("42P", 42000000000000000),
        ];

        for (input, expected) in cases {
            let quantity = parse_k8s_quantity(input).unwrap();
            let number = quantity.try_to_integer(0, true).unwrap();
            assert_eq!(number, *expected, "input={input}, quantity={quantity:?}");
        }
    }

    #[mz_ore::test]
    fn k8s_quantity_base10_small() {
        let cases = &[("42n", 42), ("42u", 42000), ("42m", 42000000)];

        for (input, expected) in cases {
            let quantity = parse_k8s_quantity(input).unwrap();
            let number = quantity.try_to_integer(-9, true).unwrap();
            assert_eq!(number, *expected, "input={input}, quantity={quantity:?}");
        }
    }

    #[mz_ore::test]
    fn k8s_quantity_base2() {
        let cases = &[
            ("42Ki", 42 << 10),
            ("42Mi", 42 << 20),
            ("42Gi", 42 << 30),
            ("42Ti", 42 << 40),
            ("42Pi", 42 << 50),
        ];

        for (input, expected) in cases {
            let quantity = parse_k8s_quantity(input).unwrap();
            let number = quantity.try_to_integer(0, false).unwrap();
            assert_eq!(number, *expected, "input={input}, quantity={quantity:?}");
        }
    }
}
