// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cluster and replica configuration types.

use std::collections::BTreeMap;
use std::num::NonZero;

use mz_compute_types::config::{ComputeReplicaConfig, ComputeReplicaLogging};
use mz_orchestrator::{CpuLimit, DiskLimit, MemoryLimit};
use mz_ore::cast::CastInto;
use mz_repr::adt::numeric::Numeric;
use serde::{Deserialize, Serialize};

/// The status of a cluster.
pub type ClusterStatus = mz_orchestrator::ServiceStatus;

/// Couples replica activation with withdrawal of controller installation authority.
/// Both the provisioner and clusterd must use the same build-level ownership gate.
pub const REPLICA_OWNED_COMPUTE: bool = false;

/// Configures a cluster replica.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ReplicaConfig {
    /// The location of the replica.
    pub location: ReplicaLocation,
    /// Configuration for the compute half of the replica.
    pub compute: ComputeReplicaConfig,
}

/// Configures the resource allocation for a cluster replica.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReplicaAllocation {
    /// The memory limit for each process in the replica.
    pub memory_limit: Option<MemoryLimit>,
    /// The CPU limit for each process in the replica.
    pub cpu_limit: Option<CpuLimit>,
    /// The CPU limit for each process in the replica.
    pub cpu_request: Option<CpuLimit>,
    /// The disk limit for each process in the replica.
    pub disk_limit: Option<DiskLimit>,
    /// The number of processes in the replica.
    pub scale: NonZero<u16>,
    /// The number of worker threads in the replica.
    pub workers: NonZero<usize>,
    /// The number of credits per hour that the replica consumes.
    #[serde(deserialize_with = "mz_repr::adt::numeric::str_serde::deserialize")]
    pub credits_per_hour: Numeric,
    /// Whether each process has exclusive access to its CPU cores.
    #[serde(default)]
    pub cpu_exclusive: bool,
    /// Whether this size represents a modern "cc" size rather than a legacy
    /// T-shirt size.
    #[serde(default = "default_true")]
    pub is_cc: bool,
    /// The size *family* this size belongs to, e.g. the size `D.1-xsmall`
    /// belongs to family `D` and the legacy t-shirt sizes belong to family
    /// `legacy`. The family is the coarse axis and is *not* a prefix of the size
    /// name in general. Used as the
    /// `replica_size_family` attribute when evaluating replica-local scoped
    /// feature flags (see the scoped feature flags design). When unset, the
    /// family falls back to a value derived from [`Self::is_cc`] via
    /// [`ReplicaAllocation::family`].
    #[serde(default)]
    pub family: Option<String>,
    /// Whether instances of this type use swap as the spill-to-disk mechanism.
    #[serde(default)]
    pub swap_enabled: bool,
    /// Whether instances of this type can be created.
    #[serde(default)]
    pub disabled: bool,
    /// Additional node selectors.
    #[serde(default)]
    pub selectors: BTreeMap<String, String>,
}

impl ReplicaAllocation {
    /// The name of the size family this allocation belongs to, used as the
    /// `replica_size_family` attribute when evaluating replica-local scoped
    /// feature flags.
    ///
    /// Falls back to a value derived from [`Self::is_cc`] when [`Self::family`]
    /// is unset: `"cc"` for modern sizes and `"legacy"` for the legacy t-shirt
    /// sizes. This keeps the legacy family targetable even before every size
    /// gains an explicit `family` in the size configuration.
    pub fn family(&self) -> &str {
        match &self.family {
            Some(family) => family.as_str(),
            None if self.is_cc => "cc",
            None => "legacy",
        }
    }
}

fn default_true() -> bool {
    true
}

#[mz_ore::test]
// We test this particularly because we deserialize values from strings.
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
fn test_replica_allocation_deserialization() {
    use bytesize::ByteSize;
    use mz_ore::{assert_err, assert_ok};

    let data = r#"
        {
            "cpu_limit": 1.0,
            "memory_limit": "10GiB",
            "disk_limit": "100MiB",
            "scale": 16,
            "workers": 1,
            "credits_per_hour": "16",
            "swap_enabled": true,
            "selectors": {
                "key1": "value1",
                "key2": "value2"
            }
        }"#;

    let replica_allocation: ReplicaAllocation = serde_json::from_str(data)
        .expect("deserialization from JSON succeeds for ReplicaAllocation");

    assert_eq!(
        replica_allocation,
        ReplicaAllocation {
            credits_per_hour: 16.into(),
            disk_limit: Some(DiskLimit(ByteSize::mib(100))),
            disabled: false,
            memory_limit: Some(MemoryLimit(ByteSize::gib(10))),
            cpu_limit: Some(CpuLimit::from_millicpus(1000)),
            cpu_request: None,
            cpu_exclusive: false,
            is_cc: true,
            family: None,
            swap_enabled: true,
            scale: NonZero::new(16).unwrap(),
            workers: NonZero::new(1).unwrap(),
            selectors: BTreeMap::from([
                ("key1".to_string(), "value1".to_string()),
                ("key2".to_string(), "value2".to_string())
            ]),
        }
    );

    let data = r#"
        {
            "cpu_limit": 0,
            "memory_limit": "0GiB",
            "disk_limit": "0MiB",
            "scale": 1,
            "workers": 1,
            "credits_per_hour": "0",
            "cpu_exclusive": true,
            "disabled": true
        }"#;

    let replica_allocation: ReplicaAllocation = serde_json::from_str(data)
        .expect("deserialization from JSON succeeds for ReplicaAllocation");

    assert_eq!(
        replica_allocation,
        ReplicaAllocation {
            credits_per_hour: 0.into(),
            disk_limit: Some(DiskLimit(ByteSize::mib(0))),
            disabled: true,
            memory_limit: Some(MemoryLimit(ByteSize::gib(0))),
            cpu_limit: Some(CpuLimit::from_millicpus(0)),
            cpu_request: None,
            cpu_exclusive: true,
            is_cc: true,
            family: None,
            swap_enabled: false,
            scale: NonZero::new(1).unwrap(),
            workers: NonZero::new(1).unwrap(),
            selectors: Default::default(),
        }
    );

    // `scale` and `workers` must be non-zero.
    let data = r#"{"scale": 0, "workers": 1, "credits_per_hour": "0"}"#;
    assert_err!(serde_json::from_str::<ReplicaAllocation>(data));
    let data = r#"{"scale": 1, "workers": 0, "credits_per_hour": "0"}"#;
    assert_err!(serde_json::from_str::<ReplicaAllocation>(data));
    let data = r#"{"scale": 1, "workers": 1, "credits_per_hour": "0"}"#;
    assert_ok!(serde_json::from_str::<ReplicaAllocation>(data));
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decContextDefault` on OS `linux`
fn test_replica_allocation_family() {
    let parse = |json: &str| -> ReplicaAllocation {
        serde_json::from_str(json).expect("deserialization from JSON succeeds")
    };

    // An explicit `family` is used verbatim.
    assert_eq!(
        parse(r#"{"scale": 1, "workers": 1, "credits_per_hour": "0", "family": "D"}"#).family(),
        "D"
    );
    // Without an explicit `family`, modern (`is_cc`) sizes fall back to "cc".
    // `is_cc` defaults to true.
    assert_eq!(
        parse(r#"{"scale": 1, "workers": 1, "credits_per_hour": "0"}"#).family(),
        "cc"
    );
    // Without an explicit `family`, legacy (non-`is_cc`) sizes fall back to
    // "legacy".
    assert_eq!(
        parse(r#"{"scale": 1, "workers": 1, "credits_per_hour": "0", "is_cc": false}"#).family(),
        "legacy"
    );
    // An explicit family wins even for a legacy size.
    assert_eq!(
        parse(
            r#"{"scale": 1, "workers": 1, "credits_per_hour": "0", "is_cc": false, "family": "legacy-special"}"#
        )
        .family(),
        "legacy-special"
    );
}

/// Configures the location of a cluster replica.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub enum ReplicaLocation {
    /// An unmanaged replica.
    Unmanaged(UnmanagedReplicaLocation),
    /// A managed replica.
    Managed(ManagedReplicaLocation),
}

impl ReplicaLocation {
    /// Returns the number of processes specified by this replica location.
    pub fn num_processes(&self) -> usize {
        match self {
            ReplicaLocation::Unmanaged(UnmanagedReplicaLocation {
                computectl_addrs, ..
            }) => computectl_addrs.len(),
            ReplicaLocation::Managed(ManagedReplicaLocation { allocation, .. }) => {
                allocation.scale.cast_into()
            }
        }
    }

    pub fn billed_as(&self) -> Option<&str> {
        match self {
            ReplicaLocation::Managed(ManagedReplicaLocation { billed_as, .. }) => {
                billed_as.as_deref()
            }
            ReplicaLocation::Unmanaged(_) => None,
        }
    }

    pub fn internal(&self) -> bool {
        match self {
            ReplicaLocation::Managed(ManagedReplicaLocation { internal, .. }) => *internal,
            ReplicaLocation::Unmanaged(_) => false,
        }
    }

    /// Returns the number of workers specified by this replica location.
    ///
    /// `None` for unmanaged replicas, whose worker count we don't know.
    pub fn workers(&self) -> Option<usize> {
        match self {
            ReplicaLocation::Managed(ManagedReplicaLocation { allocation, .. }) => {
                Some(allocation.workers.get() * self.num_processes())
            }
            ReplicaLocation::Unmanaged(_) => None,
        }
    }

    /// Whether the replica is durably marked `pending`.
    ///
    /// Vestigial: no path creates one anymore. A crash on a version that still
    /// staged reconfigurations through overlap replicas could have left one
    /// behind, and the catalog-open migration reaps those.
    pub fn pending(&self) -> bool {
        match self {
            ReplicaLocation::Managed(ManagedReplicaLocation { pending, .. }) => *pending,
            ReplicaLocation::Unmanaged(_) => false,
        }
    }
}

/// The "role" of a cluster, which is currently used to determine the
/// severity of alerts for problems with its replicas.
#[derive(Debug, Clone)]
pub enum ClusterRole {
    /// The existence and proper functioning of the cluster's replicas is
    /// business-critical for Materialize.
    SystemCritical,
    /// Assuming no bugs, the cluster's replicas should always exist and function
    /// properly. If it doesn't, however, that is less urgent than
    /// would be the case for a `SystemCritical` replica.
    System,
    /// The cluster is controlled by the user, and might go down for
    /// reasons outside our control (e.g., OOMs).
    User,
}

/// The location of an unmanaged replica.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnmanagedReplicaLocation {
    /// The network addresses of the storagectl endpoints for each process in
    /// the replica.
    pub storagectl_addrs: Vec<String>,
    /// The network addresses of the computectl endpoints for each process in
    /// the replica.
    pub computectl_addrs: Vec<String>,
}

/// The location of a managed replica.
#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ManagedReplicaLocation {
    /// The resource allocation for the replica.
    pub allocation: ReplicaAllocation,
    /// SQL size parameter used for allocation
    pub size: String,
    /// If `true`, Materialize support owns this replica.
    pub internal: bool,
    /// Optional SQL size parameter used for billing.
    pub billed_as: Option<String>,
    /// The availability zones the replica may be placed in; empty means
    /// unconstrained.
    ///
    /// For a replica of a managed cluster this is the cluster's
    /// `AVAILABILITY ZONES` pool; for a replica of an unmanaged cluster it is
    /// the single user-pinned `AVAILABILITY ZONE`, as a zero- or one-element
    /// list.
    ///
    /// Not serialized: this is re-derived from the cluster config at
    /// concretization, not read back from a durable record.
    #[serde(skip)]
    pub availability_zones: Vec<String>,
    /// See [`ReplicaLocation::pending`].
    pub pending: bool,
}

impl ManagedReplicaLocation {
    /// Return the size which should be used to determine billing-related information.
    pub fn size_for_billing(&self) -> &str {
        self.billed_as.as_deref().unwrap_or(&self.size)
    }
}

/// Configures logging for a cluster replica.
pub type ReplicaLogging = ComputeReplicaLogging;
