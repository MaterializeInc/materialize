// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::bail;
use bytesize::ByteSize;
use ipnet::IpNet;
use mz_adapter_types::bootstrap_builtin_cluster_config::BootstrapBuiltinClusterConfig;
use mz_auth::password::Password;
use mz_build_info::BuildInfo;
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_controller::clusters::ReplicaAllocation;
use mz_license_keys::ValidatedLicenseKey;
use mz_orchestrator::MemoryLimit;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::PersistClient;
use mz_repr::CatalogItemId;
use mz_repr::adt::numeric::Numeric;
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_sql::catalog::EnvironmentId;
use serde::Serialize;

use crate::durable::{CatalogError, DurableCatalogState};

const GIB: u64 = 1024 * 1024 * 1024;

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

#[derive(Debug)]
pub struct BuiltinItemMigrationConfig {
    pub persist_client: PersistClient,
    pub read_only: bool,
    pub force_migration: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
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
                replica.credits_per_hour = Numeric::from(
                    (memory_limit.0 * replica.scale * u64::try_from(replica.workers)?).0,
                ) / Numeric::from(1 * GIB);
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
                let workers: u8 = 1 << i;
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
                            disk_limit: None,
                            scale: 1,
                            workers: workers.into(),
                            credits_per_hour: 1.into(),
                            cpu_exclusive: false,
                            is_cc: false,
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
                    disk_limit: None,
                    scale,
                    workers: 1,
                    credits_per_hour: scale.into(),
                    cpu_exclusive: false,
                    is_cc: false,
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
                    disk_limit: None,
                    scale,
                    workers: scale.into(),
                    credits_per_hour: scale.into(),
                    cpu_exclusive: false,
                    is_cc: false,
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
                    disk_limit: None,
                    scale: 1,
                    workers: 8,
                    credits_per_hour: 1.into(),
                    cpu_exclusive: false,
                    is_cc: false,
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
                disk_limit: None,
                scale: 2,
                workers: 4,
                credits_per_hour: 2.into(),
                cpu_exclusive: false,
                is_cc: false,
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
                disk_limit: None,
                scale: 0,
                workers: 0,
                credits_per_hour: 0.into(),
                cpu_exclusive: false,
                is_cc: true,
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
#[derive(Debug, Clone, Serialize)]
pub struct AwsPrincipalContext {
    pub aws_account_id: String,
    pub aws_external_id_prefix: AwsExternalIdPrefix,
}

impl AwsPrincipalContext {
    pub fn to_principal_string(&self, aws_external_id_suffix: CatalogItemId) -> String {
        format!(
            "arn:aws:iam::{}:role/mz_{}_{}",
            self.aws_account_id, self.aws_external_id_prefix, aws_external_id_suffix
        )
    }
}
