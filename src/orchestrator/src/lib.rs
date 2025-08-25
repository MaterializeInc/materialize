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
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use bytesize::ByteSize;
use chrono::{DateTime, Utc};
use derivative::Derivative;
use futures_core::stream::BoxStream;
use mz_ore::cast::CastFrom;
use serde::de::Unexpected;
use serde::{Deserialize, Deserializer, Serialize};

/// An orchestrator manages services.
///
/// A service is a set of one or more processes running the same image. See
/// [`ServiceConfig`] for details.
///
/// All services live within a namespace. A namespace allows multiple users to
/// share an orchestrator without conflicting: each user can only create,
/// delete, and list the services within their namespace. Namespaces are not
/// isolated at the network level, however: services in one namespace can
/// communicate with services in another namespace with no restrictions.
///
/// Services **must** be tolerant of running as part of a distributed system. In
/// particular, services **must** be prepared for the possibility that there are
/// two live processes with the same identity. This can happen, for example,
/// when the machine hosting a process *appears* to fail, from the perspective
/// of the orchestrator, and so the orchestrator restarts the process on another
/// machine, but in fact the original machine is still alive, just on the
/// opposite side of a network partition. Be sure to design any communication
/// with other services (e.g., an external database) to correctly handle
/// competing communication from another incarnation of the service.
///
/// The intent is that you can implement `Orchestrator` with pods in Kubernetes,
/// containers in Docker, or processes on your local machine.
pub trait Orchestrator: fmt::Debug + Send + Sync {
    /// Enter a namespace in the orchestrator.
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator>;
}

/// An orchestrator restricted to a single namespace.
#[async_trait]
pub trait NamespacedOrchestrator: fmt::Debug + Send + Sync {
    /// Ensures that a service with the given configuration is running.
    ///
    /// If a service with the same ID already exists, its configuration is
    /// updated to match `config`. This may or may not involve restarting the
    /// service, depending on whether the existing service matches `config`.
    fn ensure_service(
        &self,
        id: &str,
        config: ServiceConfig,
    ) -> Result<Box<dyn Service>, anyhow::Error>;

    /// Drops the identified service, if it exists.
    fn drop_service(&self, id: &str) -> Result<(), anyhow::Error>;

    /// Lists the identifiers of all known services.
    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error>;

    /// Watch for status changes of all known services.
    fn watch_services(&self) -> BoxStream<'static, Result<ServiceEvent, anyhow::Error>>;

    /// Gets resource usage metrics for all processes associated with a service.
    ///
    /// Returns `Err` if the entire process failed. Returns `Ok(v)` otherwise,
    /// with one element in `v` for each process of the service,
    /// even in not all metrics could be collected for all processes.
    /// In such a case, the corresponding fields of `ServiceProcessMetrics` will be `None`.
    async fn fetch_service_metrics(
        &self,
        id: &str,
    ) -> Result<Vec<ServiceProcessMetrics>, anyhow::Error>;

    fn update_scheduling_config(&self, config: scheduling_config::ServiceSchedulingConfig);
}

/// An event describing a status change of an orchestrated service.
#[derive(Debug, Clone, Serialize)]
pub struct ServiceEvent {
    pub service_id: String,
    pub process_id: u64,
    pub status: ServiceStatus,
    pub time: DateTime<Utc>,
}

/// Why the service is not ready, if known
#[derive(Debug, Clone, Copy, Serialize, Eq, PartialEq)]
pub enum OfflineReason {
    OomKilled,
    Initializing,
}

impl fmt::Display for OfflineReason {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            OfflineReason::OomKilled => f.write_str("oom-killed"),
            OfflineReason::Initializing => f.write_str("initializing"),
        }
    }
}

/// Describes the status of an orchestrated service.
#[derive(Debug, Clone, Copy, Serialize, Eq, PartialEq)]
pub enum ServiceStatus {
    /// Service is ready to accept requests.
    Online,
    /// Service is not ready to accept requests.
    /// The inner element is `None` if the reason
    /// is unknown
    Offline(Option<OfflineReason>),
}

impl ServiceStatus {
    /// Returns the service status as a kebab-case string.
    pub fn as_kebab_case_str(&self) -> &'static str {
        match self {
            ServiceStatus::Online => "online",
            ServiceStatus::Offline(_) => "offline",
        }
    }
}

/// Describes a running service managed by an `Orchestrator`.
pub trait Service: fmt::Debug + Send + Sync {
    /// Given the name of a port, returns the addresses for each of the
    /// service's processes, in order.
    ///
    /// Panics if `port` does not name a valid port.
    fn addresses(&self, port: &str) -> Vec<String>;
}

#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize, Eq, PartialEq)]
pub struct ServiceProcessMetrics {
    pub cpu_nano_cores: Option<u64>,
    pub memory_bytes: Option<u64>,
    pub disk_usage_bytes: Option<u64>,
}

/// A simple language for describing assertions about a label's existence and value.
///
/// Used by [`LabelSelector`].
#[derive(Clone, Debug)]
pub enum LabelSelectionLogic {
    /// The label exists and its value equals the given value.
    /// Equivalent to `InSet { values: vec![value] }`
    Eq { value: String },
    /// Either the label does not exist, or it exists
    /// but its value does not equal the given value.
    /// Equivalent to `NotInSet { values: vec![value] }`
    NotEq { value: String },
    /// The label exists.
    Exists,
    /// The label does not exist.
    NotExists,
    /// The label exists and its value is one of the given values.
    InSet { values: Vec<String> },
    /// Either the label does not exist, or it exists
    /// but its value is not one of the given values.
    NotInSet { values: Vec<String> },
}

/// A simple language for describing whether a label
/// exists and whether the value corresponding to it is in some set.
/// Intended to correspond to the capabilities offered by Kubernetes label selectors,
/// but without directly exposing Kubernetes API code to consumers of this module.
#[derive(Clone, Debug)]
pub struct LabelSelector {
    /// The name of the label
    pub label_name: String,
    /// An assertion about the existence and value of a label
    /// named `label_name`
    pub logic: LabelSelectionLogic,
}

/// Describes the desired state of a service.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct ServiceConfig {
    /// An opaque identifier for the executable or container image to run.
    ///
    /// Often names a container on Docker Hub or a path on the local machine.
    pub image: String,
    /// For the Kubernetes orchestrator, this is an init container to
    /// configure for the pod running the service.
    pub init_container_image: Option<String>,
    /// A function that generates the arguments for each process of the service
    /// given the assigned listen addresses for each named port.
    #[derivative(Debug = "ignore")]
    pub args: Box<dyn Fn(ServiceAssignments) -> Vec<String> + Send + Sync>,
    /// Ports to expose.
    pub ports: Vec<ServicePort>,
    /// An optional limit on the memory that the service can use.
    pub memory_limit: Option<MemoryLimit>,
    /// An optional request on the memory that the service can use. If unspecified,
    /// use the same value as `memory_limit`.
    pub memory_request: Option<MemoryLimit>,
    /// An optional limit on the CPU that the service can use.
    pub cpu_limit: Option<CpuLimit>,
    /// The number of copies of this service to run.
    pub scale: u16,
    /// Arbitrary key–value pairs to attach to the service in the orchestrator
    /// backend.
    ///
    /// The orchestrator backend may apply a prefix to the key if appropriate.
    pub labels: BTreeMap<String, String>,
    /// Arbitrary key–value pairs to attach to the service as annotations in the
    /// orchestrator backend.
    ///
    /// The orchestrator backend may apply a prefix to the key if appropriate.
    pub annotations: BTreeMap<String, String>,
    /// The availability zones the service can be run in. If no availability
    /// zones are specified, the orchestrator is free to choose one.
    pub availability_zones: Option<Vec<String>>,
    /// A set of label selectors selecting all _other_ services that are replicas of this one.
    ///
    /// This may be used to implement anti-affinity. If _all_ such selectors
    /// match for a given service, this service should not be co-scheduled on
    /// a machine with that service.
    ///
    /// The orchestrator backend may or may not actually implement anti-affinity functionality.
    pub other_replicas_selector: Vec<LabelSelector>,
    /// A set of label selectors selecting all services that are replicas of this one,
    /// including itself.
    ///
    /// This may be used to implement placement spread.
    ///
    /// The orchestrator backend may or may not actually implement placement spread functionality.
    pub replicas_selector: Vec<LabelSelector>,

    /// Whether scratch disk space should be allocated for the service.
    pub disk: bool,
    /// The maximum amount of scratch disk space that the service is allowed to consume.
    pub disk_limit: Option<DiskLimit>,
    /// Node selector for this service.
    pub node_selector: BTreeMap<String, String>,
}

/// A named port associated with a service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServicePort {
    /// A descriptive name for the port.
    ///
    /// Note that not all orchestrator backends make use of port names.
    pub name: String,
    /// The desired port number.
    ///
    /// Not all orchestrator backends will make use of the hint.
    pub port_hint: u16,
}

/// Assignments that the orchestrator has made for a process in a service.
#[derive(Clone, Debug)]
pub struct ServiceAssignments<'a> {
    /// For each specified [`ServicePort`] name, a listen address.
    pub listen_addrs: &'a BTreeMap<String, String>,
    /// The listen addresses of each peer in the service.
    ///
    /// The order of peers is significant. Each peer is uniquely identified by its position in the
    /// list.
    pub peer_addrs: &'a [BTreeMap<String, String>],
}

impl ServiceAssignments<'_> {
    /// Return the peer addresses for the specified [`ServicePort`] name.
    pub fn peer_addresses(&self, name: &str) -> Vec<String> {
        self.peer_addrs.iter().map(|a| a[name].clone()).collect()
    }
}

/// Describes a limit on memory.
#[derive(Copy, Clone, Debug, PartialOrd, Eq, Ord, PartialEq)]
pub struct MemoryLimit(pub ByteSize);

impl MemoryLimit {
    pub const MAX: Self = Self(ByteSize(u64::MAX));
}

impl<'de> Deserialize<'de> for MemoryLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        <String as Deserialize>::deserialize(deserializer)
            .and_then(|s| {
                ByteSize::from_str(&s).map_err(|_e| {
                    use serde::de::Error;
                    D::Error::invalid_value(serde::de::Unexpected::Str(&s), &"valid size in bytes")
                })
            })
            .map(MemoryLimit)
    }
}

impl Serialize for MemoryLimit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <String as Serialize>::serialize(&self.0.to_string(), serializer)
    }
}

/// Describes a limit on CPU resources.
#[derive(Debug, Copy, Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct CpuLimit {
    millicpus: usize,
}

impl CpuLimit {
    pub const MAX: Self = Self::from_millicpus(usize::MAX / 1_000_000);

    /// Constructs a new CPU limit from a number of millicpus.
    pub const fn from_millicpus(millicpus: usize) -> CpuLimit {
        CpuLimit { millicpus }
    }

    /// Returns the CPU limit in millicpus.
    pub fn as_millicpus(&self) -> usize {
        self.millicpus
    }

    /// Returns the CPU limit in nanocpus.
    pub fn as_nanocpus(&self) -> u64 {
        // The largest possible value of a u64 is
        // 18_446_744_073_709_551_615,
        // so we won't overflow this
        // unless we have an instance with
        // ~18.45 billion cores.
        //
        // Such an instance seems unrealistic,
        // at least until we raise another few rounds
        // of funding ...

        u64::cast_from(self.millicpus)
            .checked_mul(1_000_000)
            .expect("Nano-CPUs must be representable")
    }
}

impl<'de> Deserialize<'de> for CpuLimit {
    // TODO(benesch): remove this once this function no longer makes use of
    // potentially dangerous `as` conversions.
    #[allow(clippy::as_conversions)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Note -- we just round off any precision beyond 0.001 here.
        let float = f64::deserialize(deserializer)?;
        let millicpus = (float * 1000.).round();
        if millicpus < 0. || millicpus > (std::usize::MAX as f64) {
            use serde::de::Error;
            Err(D::Error::invalid_value(
                Unexpected::Float(float),
                &"a float representing a plausible number of CPUs",
            ))
        } else {
            Ok(Self {
                millicpus: millicpus as usize,
            })
        }
    }
}

impl Serialize for CpuLimit {
    // TODO(benesch): remove this once this function no longer makes use of
    // potentially dangerous `as` conversions.
    #[allow(clippy::as_conversions)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <f64 as Serialize>::serialize(&(self.millicpus as f64 / 1000.0), serializer)
    }
}

/// Describes a limit on disk usage.
#[derive(Copy, Clone, Debug, PartialOrd, Eq, Ord, PartialEq)]
pub struct DiskLimit(pub ByteSize);

impl DiskLimit {
    pub const ZERO: Self = Self(ByteSize(0));
    pub const MAX: Self = Self(ByteSize(u64::MAX));
    pub const ARBITRARY: Self = Self(ByteSize::gib(1));
}

impl<'de> Deserialize<'de> for DiskLimit {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        <String as Deserialize>::deserialize(deserializer)
            .and_then(|s| {
                ByteSize::from_str(&s).map_err(|_e| {
                    use serde::de::Error;
                    D::Error::invalid_value(serde::de::Unexpected::Str(&s), &"valid size in bytes")
                })
            })
            .map(DiskLimit)
    }
}

impl Serialize for DiskLimit {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        <String as Serialize>::serialize(&self.0.to_string(), serializer)
    }
}

/// Configuration for how services are scheduled. These may be ignored by orchestrator
/// implementations.
pub mod scheduling_config {
    #[derive(Debug, Clone)]
    pub struct ServiceTopologySpreadConfig {
        /// If `true`, enable spread for replicated services.
        ///
        /// Defaults to `true`.
        pub enabled: bool,
        /// If `true`, ignore services with `scale` > 1 when expressing
        /// spread constraints.
        ///
        /// Default to `true`.
        pub ignore_non_singular_scale: bool,
        /// The `maxSkew` for spread constraints.
        /// See
        /// <https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/>
        /// for more details.
        ///
        /// Defaults to `1`.
        pub max_skew: i32,
        /// The `minDomains` for spread constraints.
        /// See
        /// <https://kubernetes.io/docs/concepts/scheduling-eviction/topology-spread-constraints/>
        /// for more details.
        ///
        /// Defaults to None.
        pub min_domains: Option<i32>,
        /// If `true`, make the spread constraints into a preference.
        ///
        /// Defaults to `false`.
        pub soft: bool,
    }

    #[derive(Debug, Clone)]
    pub struct ServiceSchedulingConfig {
        /// If `Some`, add a affinity preference with the given
        /// weight for services that horizontally scale.
        ///
        /// Defaults to `Some(100)`.
        pub multi_pod_az_affinity_weight: Option<i32>,
        /// If `true`, make the node-scope anti-affinity between
        /// replicated services a preference over a constraint.
        ///
        /// Defaults to `false`.
        pub soften_replication_anti_affinity: bool,
        /// The weight for `soften_replication_anti_affinity.
        ///
        /// Defaults to `100`.
        pub soften_replication_anti_affinity_weight: i32,
        /// Configuration for `TopologySpreadConstraint`'s
        pub topology_spread: ServiceTopologySpreadConfig,
        /// If `true`, make the az-scope node affinity soft.
        ///
        /// Defaults to `false`.
        pub soften_az_affinity: bool,
        /// The weight for `soften_replication_anti_affinity.
        ///
        /// Defaults to `100`.
        pub soften_az_affinity_weight: i32,
        // Whether to enable security context for the service.
        pub security_context_enabled: bool,
    }

    pub const DEFAULT_POD_AZ_AFFINITY_WEIGHT: Option<i32> = Some(100);
    pub const DEFAULT_SOFTEN_REPLICATION_ANTI_AFFINITY: bool = false;
    pub const DEFAULT_SOFTEN_REPLICATION_ANTI_AFFINITY_WEIGHT: i32 = 100;

    pub const DEFAULT_TOPOLOGY_SPREAD_ENABLED: bool = true;
    pub const DEFAULT_TOPOLOGY_SPREAD_IGNORE_NON_SINGULAR_SCALE: bool = true;
    pub const DEFAULT_TOPOLOGY_SPREAD_MAX_SKEW: i32 = 1;
    pub const DEFAULT_TOPOLOGY_SPREAD_MIN_DOMAIN: Option<i32> = None;
    pub const DEFAULT_TOPOLOGY_SPREAD_SOFT: bool = false;

    pub const DEFAULT_SOFTEN_AZ_AFFINITY: bool = false;
    pub const DEFAULT_SOFTEN_AZ_AFFINITY_WEIGHT: i32 = 100;
    pub const DEFAULT_SECURITY_CONTEXT_ENABLED: bool = true;

    impl Default for ServiceSchedulingConfig {
        fn default() -> Self {
            ServiceSchedulingConfig {
                multi_pod_az_affinity_weight: DEFAULT_POD_AZ_AFFINITY_WEIGHT,
                soften_replication_anti_affinity: DEFAULT_SOFTEN_REPLICATION_ANTI_AFFINITY,
                soften_replication_anti_affinity_weight:
                    DEFAULT_SOFTEN_REPLICATION_ANTI_AFFINITY_WEIGHT,
                topology_spread: ServiceTopologySpreadConfig {
                    enabled: DEFAULT_TOPOLOGY_SPREAD_ENABLED,
                    ignore_non_singular_scale: DEFAULT_TOPOLOGY_SPREAD_IGNORE_NON_SINGULAR_SCALE,
                    max_skew: DEFAULT_TOPOLOGY_SPREAD_MAX_SKEW,
                    min_domains: DEFAULT_TOPOLOGY_SPREAD_MIN_DOMAIN,
                    soft: DEFAULT_TOPOLOGY_SPREAD_SOFT,
                },
                soften_az_affinity: DEFAULT_SOFTEN_AZ_AFFINITY,
                soften_az_affinity_weight: DEFAULT_SOFTEN_AZ_AFFINITY_WEIGHT,
                security_context_enabled: DEFAULT_SECURITY_CONTEXT_ENABLED,
            }
        }
    }
}
