// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZero;

use anyhow::bail;
use bytesize::ByteSize;
use ipnet::IpNet;
use mz_adapter_types::bootstrap_builtin_cluster_config::BootstrapBuiltinClusterConfig;
use mz_auth::password::Password;
use mz_build_info::BuildInfo;
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_controller::clusters::ReplicaAllocation;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_license_keys::ValidatedLicenseKey;
use mz_orchestrator::MemoryLimit;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::PersistClient;
use mz_repr::CatalogItemId;
use mz_repr::adt::numeric::Numeric;
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_sql::catalog::EnvironmentId;
use serde::{Deserialize, Serialize};

use crate::durable::{CatalogError, DurableCatalogState};

const GIB: u64 = 1024 * 1024 * 1024;

/// Scoped (per-cluster and per-replica) system-parameter overrides, keyed by
/// object id. Each value is the raw (unparsed) string for a parameter whose
/// scoped value differs from the environment-wide value. An absent entry means
/// no override. Empty maps mean no scoped overrides at all.
///
/// This is the in-memory mirror of the durable `cluster_system_configurations`
/// and `replica_system_configurations` catalog collections.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopedParameters {
    /// Cluster-coherent overrides, keyed by cluster id.
    pub cluster: BTreeMap<ClusterId, BTreeMap<String, String>>,
    /// Replica-local overrides, keyed by replica id.
    pub replica: BTreeMap<ReplicaId, BTreeMap<String, String>>,
}

/// The set of objects a [`ScopedParameters`] update was evaluated for, used to
/// bound which durable override rows the update may prune.
///
/// The update is authoritative only for objects in this set. The durable apply
/// removes a row only when its owning object is in scope and the update no
/// longer carries that override, so an object created after the update's
/// evaluation snapshot, and the override it folded into its own create
/// transaction, is not wiped by a concurrent full-state reconcile.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ScopedParametersScope {
    /// Cluster ids whose rows the update may prune.
    pub clusters: BTreeSet<ClusterId>,
    /// Replica ids whose rows the update may prune.
    pub replicas: BTreeSet<ReplicaId>,
}

impl ScopedParameters {
    /// Returns `true` if there are no cluster or replica overrides.
    pub fn is_empty(&self) -> bool {
        self.cluster.is_empty() && self.replica.is_empty()
    }

    /// Returns a copy of `self` with `other`'s entries merged in, replacing any
    /// existing entry for the same object. Expresses no removals.
    pub fn merge(&self, other: &ScopedParameters) -> ScopedParameters {
        let mut merged = self.clone();
        merged
            .cluster
            .extend(other.cluster.iter().map(|(id, v)| (*id, v.clone())));
        merged
            .replica
            .extend(other.replica.iter().map(|(id, v)| (*id, v.clone())));
        merged
    }
}

/// Configures a catalog.
#[derive(Debug)]
pub struct Config<'a> {
    /// The connection to the catalog storage.
    pub storage: Box<dyn DurableCatalogState>,
    /// The registry that catalog uses to report metrics.
    pub metrics_registry: &'a MetricsRegistry,
    pub state: StateConfig,
}

#[derive(Debug)]
pub struct StateConfig {
    /// Whether to enable unsafe mode.
    pub unsafe_mode: bool,
    /// Whether the build is a local dev build.
    pub all_features: bool,
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// A persistent ID associated with the environment.
    pub environment_id: EnvironmentId,
    /// Whether to start Materialize in read-only mode.
    pub read_only: bool,
    /// Function to generate wall clock now; can be mocked.
    pub now: mz_ore::now::NowFn,
    /// Linearizable timestamp of when this environment booted.
    pub boot_ts: mz_repr::Timestamp,
    /// Whether or not to skip catalog migrations.
    pub skip_migrations: bool,
    /// Map of strings to corresponding compute replica sizes.
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    /// Builtin system cluster config.
    pub builtin_system_cluster_config: BootstrapBuiltinClusterConfig,
    /// Builtin catalog server cluster config.
    pub builtin_catalog_server_cluster_config: BootstrapBuiltinClusterConfig,
    /// Builtin probe cluster config.
    pub builtin_probe_cluster_config: BootstrapBuiltinClusterConfig,
    /// Builtin support cluster config.
    pub builtin_support_cluster_config: BootstrapBuiltinClusterConfig,
    /// Builtin analytics cluster config.
    pub builtin_analytics_cluster_config: BootstrapBuiltinClusterConfig,
    /// Dynamic defaults for system parameters.
    pub system_parameter_defaults: BTreeMap<String, String>,
    /// An optional map of system parameters pulled from a remote frontend.
    /// A `None` value indicates that the initial sync was skipped.
    pub remote_system_parameters: Option<BTreeMap<String, String>>,
    /// Valid availability zones for replicas.
    pub availability_zones: Vec<String>,
    /// IP Addresses which will be used for egress.
    pub egress_addresses: Vec<IpNet>,
    /// Context for generating an AWS Principal.
    pub aws_principal_context: Option<AwsPrincipalContext>,
    /// Supported AWS PrivateLink availability zone ids.
    pub aws_privatelink_availability_zones: Option<BTreeSet<String>>,
    /// Host name or URL for connecting to the HTTP server of this instance.
    pub http_host_name: Option<String>,
    /// Context for source and sink connections.
    pub connection_context: mz_storage_types::connections::ConnectionContext,
    pub builtin_item_migration_config: BuiltinItemMigrationConfig,
    pub persist_client: PersistClient,
    /// Overrides the current value of the [`mz_adapter_types::dyncfgs::ENABLE_EXPRESSION_CACHE`]
    /// feature flag.
    pub enable_expression_cache_override: Option<bool>,
    /// Helm chart version
    pub helm_chart_version: Option<String>,
    pub external_login_password_mz_system: Option<Password>,
    pub license_key: ValidatedLicenseKey,
}

/// Non-runtime configuration required to reconstruct the catalog in a replica.
///
/// This is operator-provided configuration, not an end-user input. Runtime
/// handles and credentials are supplied locally and are never serialized here.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaCatalogConfig {
    pub unsafe_mode: bool,
    pub all_features: bool,
    #[serde(serialize_with = "serialize_replica_sizes")]
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    pub builtin_system_cluster_config: BootstrapBuiltinClusterConfig,
    pub builtin_catalog_server_cluster_config: BootstrapBuiltinClusterConfig,
    pub builtin_probe_cluster_config: BootstrapBuiltinClusterConfig,
    pub builtin_support_cluster_config: BootstrapBuiltinClusterConfig,
    pub builtin_analytics_cluster_config: BootstrapBuiltinClusterConfig,
    pub system_parameter_defaults: BTreeMap<String, String>,
    pub availability_zones: Vec<String>,
    pub egress_addresses: Vec<IpNet>,
    pub aws_principal_context: Option<AwsPrincipalContext>,
    pub aws_privatelink_availability_zones: Option<BTreeSet<String>>,
    pub http_host_name: Option<String>,
    pub helm_chart_version: Option<String>,
    pub license_key: ValidatedLicenseKey,
}

// ReplicaAllocation accepts string credits on input but its generic serializer
// emits Numeric's internal representation. Only this JSON transport needs the
// input representation, so leave existing diagnostic serialization unchanged.
fn serialize_replica_sizes<S>(
    sizes: &ClusterReplicaSizeMap,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::{Error, SerializeMap};

    let mut map = serializer.serialize_map(Some(sizes.0.len()))?;
    for (name, allocation) in &sizes.0 {
        let mut value = serde_json::to_value(allocation).map_err(S::Error::custom)?;
        value["credits_per_hour"] =
            serde_json::Value::String(allocation.credits_per_hour.to_string());
        map.serialize_entry(name, &value)?;
    }
    map.end()
}

impl ReplicaCatalogConfig {
    /// Copies reconstruction inputs without runtime handles or credentials.
    ///
    /// The caller must supply effective system parameter defaults in `state`.
    pub fn from_state(state: &StateConfig) -> Self {
        Self {
            unsafe_mode: state.unsafe_mode,
            all_features: state.all_features,
            cluster_replica_sizes: state.cluster_replica_sizes.clone(),
            builtin_system_cluster_config: state.builtin_system_cluster_config.clone(),
            builtin_catalog_server_cluster_config: state
                .builtin_catalog_server_cluster_config
                .clone(),
            builtin_probe_cluster_config: state.builtin_probe_cluster_config.clone(),
            builtin_support_cluster_config: state.builtin_support_cluster_config.clone(),
            builtin_analytics_cluster_config: state.builtin_analytics_cluster_config.clone(),
            system_parameter_defaults: state.system_parameter_defaults.clone(),
            availability_zones: state.availability_zones.clone(),
            egress_addresses: state.egress_addresses.clone(),
            aws_principal_context: state.aws_principal_context.clone(),
            aws_privatelink_availability_zones: state.aws_privatelink_availability_zones.clone(),
            http_host_name: state.http_host_name.clone(),
            helm_chart_version: state.helm_chart_version.clone(),
            license_key: state.license_key.clone(),
        }
    }

    /// Creates a read-only catalog configuration without migrations or remote
    /// parameter synchronization. Native reconstruction sets the boot timestamp
    /// from the catalog upper before loading the state.
    pub fn into_state(
        self,
        build_info: &'static BuildInfo,
        environment_id: EnvironmentId,
        connection_context: mz_storage_types::connections::ConnectionContext,
        persist_client: PersistClient,
    ) -> StateConfig {
        StateConfig {
            unsafe_mode: self.unsafe_mode,
            all_features: self.all_features,
            cluster_replica_sizes: self.cluster_replica_sizes,
            builtin_system_cluster_config: self.builtin_system_cluster_config,
            builtin_catalog_server_cluster_config: self.builtin_catalog_server_cluster_config,
            builtin_probe_cluster_config: self.builtin_probe_cluster_config,
            builtin_support_cluster_config: self.builtin_support_cluster_config,
            builtin_analytics_cluster_config: self.builtin_analytics_cluster_config,
            system_parameter_defaults: self.system_parameter_defaults,
            availability_zones: self.availability_zones,
            egress_addresses: self.egress_addresses,
            aws_principal_context: self.aws_principal_context,
            aws_privatelink_availability_zones: self.aws_privatelink_availability_zones,
            http_host_name: self.http_host_name,
            helm_chart_version: self.helm_chart_version,
            license_key: self.license_key,
            build_info,
            environment_id,
            read_only: true,
            now: mz_ore::now::SYSTEM_TIME.clone(),
            boot_ts: mz_repr::Timestamp::MIN,
            skip_migrations: true,
            remote_system_parameters: None,
            connection_context,
            builtin_item_migration_config: BuiltinItemMigrationConfig {
                persist_client: persist_client.clone(),
                read_only: true,
                force_migration: None,
            },
            persist_client,
            enable_expression_cache_override: Some(false),
            external_login_password_mz_system: None,
        }
    }
}

#[derive(Debug)]
pub struct BuiltinItemMigrationConfig {
    pub persist_client: PersistClient,
    pub read_only: bool,
    pub force_migration: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterReplicaSizeMap(pub BTreeMap<String, ReplicaAllocation>);

impl ClusterReplicaSizeMap {
    pub fn parse_from_str(s: &str, credit_consumption_from_memory: bool) -> anyhow::Result<Self> {
        let mut cluster_replica_sizes: BTreeMap<String, ReplicaAllocation> =
            serde_json::from_str(s)?;
        if credit_consumption_from_memory {
            for (name, replica) in cluster_replica_sizes.iter_mut() {
                let Some(memory_limit) = replica.memory_limit else {
                    bail!("No memory limit found in cluster definition for {name}");
                };
                let total_memory = memory_limit.0 * replica.scale.get();
                replica.credits_per_hour = Numeric::from(total_memory.0) / Numeric::from(GIB);
            }
        }
        Ok(Self(cluster_replica_sizes))
    }

    /// Iterate all enabled (not disabled) replica allocations, with their name.
    pub fn enabled_allocations(&self) -> impl Iterator<Item = (&String, &ReplicaAllocation)> {
        self.0.iter().filter(|(_, a)| !a.disabled)
    }

    /// Get a replica allocation by size name. Returns a reference to the allocation, or an
    /// error if the size is unknown.
    pub fn get_allocation_by_name(&self, name: &str) -> Result<&ReplicaAllocation, CatalogError> {
        self.0.get(name).ok_or_else(|| {
            CatalogError::Catalog(SqlCatalogError::UnknownClusterReplicaSize(name.into()))
        })
    }

    /// Used for testing and local purposes. This default value should not be used in production.
    ///
    /// Credits per hour are calculated as being equal to scale. This is not necessarily how the
    /// value is computed in production.
    pub fn for_tests() -> Self {
        // {
        //     "scale=1,workers=1": {"scale": 1, "workers": 1},
        //     "scale=1,workers=2": {"scale": 1, "workers": 2},
        //     "scale=1,workers=4": {"scale": 1, "workers": 4},
        //     /// ...
        //     "scale=1,workers=32": {"scale": 1, "workers": 32}
        //     /// Testing with multiple processes on a single machine
        //     "scale=2,workers=4": {"scale": 2, "workers": 4},
        //     /// Used in mzcompose tests
        //     "scale=2,workers=2": {"scale": 2, "workers": 2},
        //     ...
        //     "scale=16,workers=16": {"scale": 16, "workers": 16},
        //     /// Used in the shared_fate cloudtest tests
        //     "scale=2,workers=1": {"scale": 2, "workers": 1},
        //     ...
        //     "scale=16,workers=1": {"scale": 16, "workers": 1},
        //     /// Used in the cloudtest tests that force OOMs
        //     "scale=1,workers=1,mem=2GiB": { "memory_limit": 2GiB },
        //     ...
        //     "scale=1,workers=1,mem=16": { "memory_limit": 16GiB },
        // }
        let mut inner = (0..=5)
            .flat_map(|i| {
                let workers = 1 << i;
                [
                    (format!("scale=1,workers={workers}"), None),
                    (format!("scale=1,workers={workers},mem=4GiB"), Some(4)),
                    (format!("scale=1,workers={workers},mem=8GiB"), Some(8)),
                    (format!("scale=1,workers={workers},mem=16GiB"), Some(16)),
                    (format!("scale=1,workers={workers},mem=32GiB"), Some(32)),
                ]
                .map(|(name, memory_limit)| {
                    (
                        name,
                        ReplicaAllocation {
                            memory_limit: memory_limit.map(|gib| MemoryLimit(ByteSize::gib(gib))),
                            cpu_limit: None,
                            cpu_request: None,
                            disk_limit: None,
                            scale: NonZero::new(1).expect("not zero"),
                            workers: NonZero::new(workers).expect("not zero"),
                            credits_per_hour: 1.into(),
                            cpu_exclusive: false,
                            is_cc: false,
                            family: None,
                            swap_enabled: false,
                            disabled: false,
                            selectors: BTreeMap::default(),
                        },
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();

        for i in 1..=5 {
            let scale = 1 << i;
            inner.insert(
                format!("scale={scale},workers=1"),
                ReplicaAllocation {
                    memory_limit: None,
                    cpu_limit: None,
                    cpu_request: None,
                    disk_limit: None,
                    scale: NonZero::new(scale).expect("not zero"),
                    workers: NonZero::new(1).expect("not zero"),
                    credits_per_hour: scale.into(),
                    cpu_exclusive: false,
                    is_cc: false,
                    family: None,
                    swap_enabled: false,
                    disabled: false,
                    selectors: BTreeMap::default(),
                },
            );

            inner.insert(
                format!("scale={scale},workers={scale}"),
                ReplicaAllocation {
                    memory_limit: None,
                    cpu_limit: None,
                    cpu_request: None,
                    disk_limit: None,
                    scale: NonZero::new(scale).expect("not zero"),
                    workers: NonZero::new(scale.into()).expect("not zero"),
                    credits_per_hour: scale.into(),
                    cpu_exclusive: false,
                    is_cc: false,
                    family: None,
                    swap_enabled: false,
                    disabled: false,
                    selectors: BTreeMap::default(),
                },
            );

            inner.insert(
                format!("scale=1,workers=8,mem={scale}GiB"),
                ReplicaAllocation {
                    memory_limit: Some(MemoryLimit(ByteSize(u64::cast_from(scale) * (1 << 30)))),
                    cpu_limit: None,
                    cpu_request: None,
                    disk_limit: None,
                    scale: NonZero::new(1).expect("not zero"),
                    workers: NonZero::new(8).expect("not zero"),
                    credits_per_hour: 1.into(),
                    cpu_exclusive: false,
                    is_cc: false,
                    family: None,
                    swap_enabled: false,
                    disabled: false,
                    selectors: BTreeMap::default(),
                },
            );
        }

        inner.insert(
            "scale=2,workers=4".to_string(),
            ReplicaAllocation {
                memory_limit: None,
                cpu_limit: None,
                cpu_request: None,
                disk_limit: None,
                scale: NonZero::new(2).expect("not zero"),
                workers: NonZero::new(4).expect("not zero"),
                credits_per_hour: 2.into(),
                cpu_exclusive: false,
                is_cc: false,
                family: None,
                swap_enabled: false,
                disabled: false,
                selectors: BTreeMap::default(),
            },
        );

        inner.insert(
            "free".to_string(),
            ReplicaAllocation {
                memory_limit: None,
                cpu_limit: None,
                cpu_request: None,
                disk_limit: None,
                scale: NonZero::new(1).expect("not zero"),
                workers: NonZero::new(1).expect("not zero"),
                credits_per_hour: 0.into(),
                cpu_exclusive: false,
                is_cc: true,
                family: None,
                swap_enabled: false,
                disabled: true,
                selectors: BTreeMap::default(),
            },
        );

        Self(inner)
    }
}

/// Context used to generate an AWS Principal.
///
/// In the case of AWS PrivateLink connections, Materialize will connect to the
/// VPC endpoint as the AWS Principal generated via this context.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsPrincipalContext {
    pub aws_account_id: String,
    pub aws_external_id_prefix: AwsExternalIdPrefix,
}

impl AwsPrincipalContext {
    // NOTE: the `mz_catalog.mz_aws_privatelink_connections` builtin materialized
    // view reconstructs this ARN format in SQL from the mz_aws_account_id() and
    // mz_aws_external_id_prefix() functions (see MZ_AWS_PRIVATELINK_CONNECTIONS
    // in src/catalog/src/builtin/mz_catalog.rs). Keep the two in sync.
    pub fn to_principal_string(&self, aws_external_id_suffix: CatalogItemId) -> String {
        format!(
            "arn:aws:iam::{}:role/mz_{}_{}",
            self.aws_account_id, self.aws_external_id_prefix, aws_external_id_suffix
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // can't call foreign function `decContextDefault`
    fn cluster_replica_size_credits_from_memory() {
        let s = r#"{
            "test": {
                "memory_limit": "1000MiB",
                "scale": 2,
                "workers": 10,
                "credits_per_hour": "0"
            }
        }"#;
        let map = ClusterReplicaSizeMap::parse_from_str(s, true).unwrap();

        let alloc = map.get_allocation_by_name("test").unwrap();
        let expected = Numeric::from(2000) / Numeric::from(1024);
        assert_eq!(alloc.credits_per_hour, expected);
    }
}

#[cfg(test)]
mod scoped_parameters_tests {
    use std::collections::BTreeMap;

    use super::{ClusterId, ReplicaId, ScopedParameters};

    fn cfg(name: &str, value: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(name.to_string(), value.to_string())])
    }

    #[mz_ore::test]
    fn test_scoped_parameters_is_empty() {
        assert!(ScopedParameters::default().is_empty());

        let mut params = ScopedParameters::default();
        params.cluster.insert(ClusterId::User(1), cfg("f", "true"));
        assert!(!params.is_empty());

        let mut params = ScopedParameters::default();
        params.replica.insert(ReplicaId::User(1), cfg("f", "true"));
        assert!(!params.is_empty());
    }

    #[mz_ore::test]
    fn test_scoped_parameters_merge() {
        let mut base = ScopedParameters::default();
        base.cluster.insert(ClusterId::User(1), cfg("f", "old"));
        base.cluster.insert(ClusterId::User(2), cfg("f", "keep"));
        base.replica.insert(ReplicaId::User(1), cfg("g", "old"));

        let mut incoming = ScopedParameters::default();
        // Overrides the existing entry for the same object...
        incoming.cluster.insert(ClusterId::User(1), cfg("f", "new"));
        // ...and adds a new object, leaving others untouched.
        incoming.replica.insert(ReplicaId::User(2), cfg("g", "new"));

        let merged = base.merge(&incoming);

        // Replaced.
        assert_eq!(merged.cluster[&ClusterId::User(1)], cfg("f", "new"));
        // Untouched object retained (merge does not express removals).
        assert_eq!(merged.cluster[&ClusterId::User(2)], cfg("f", "keep"));
        // Pre-existing replica retained, new replica added.
        assert_eq!(merged.replica[&ReplicaId::User(1)], cfg("g", "old"));
        assert_eq!(merged.replica[&ReplicaId::User(2)], cfg("g", "new"));

        // The original is unchanged (merge returns a copy).
        assert_eq!(base.cluster[&ClusterId::User(1)], cfg("f", "old"));
    }
}
