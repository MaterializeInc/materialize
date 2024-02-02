// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Duration;

use bytesize::ByteSize;
use mz_build_info::BuildInfo;
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_controller::clusters::ReplicaAllocation;
use mz_orchestrator::MemoryLimit;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::GlobalId;
use mz_sql::catalog::EnvironmentId;
use mz_sql::session::vars::{ConnectionCounter, OwnedVarInput};
use serde::{Deserialize, Serialize};

use crate::durable::DurableCatalogState;

/// Configures a catalog.
#[derive(Debug)]
pub struct Config<'a> {
    /// The connection to the catalog storage.
    pub storage: Box<dyn DurableCatalogState>,
    /// The registry that catalog uses to report metrics.
    pub metrics_registry: &'a MetricsRegistry,
    /// How long to retain storage usage records
    pub storage_usage_retention_period: Option<Duration>,
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
    /// Function to generate wall clock now; can be mocked.
    pub now: mz_ore::now::NowFn,
    /// Whether or not to skip catalog migrations.
    pub skip_migrations: bool,
    /// Map of strings to corresponding compute replica sizes.
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    /// Builtin cluster replica size.
    pub builtin_cluster_replica_size: String,
    /// Dynamic defaults for system parameters.
    pub system_parameter_defaults: BTreeMap<String, String>,
    /// A optional map of system parameters pulled from a remote frontend.
    /// A `None` value indicates that the initial sync was skipped.
    pub remote_system_parameters: Option<BTreeMap<String, OwnedVarInput>>,
    /// Valid availability zones for replicas.
    pub availability_zones: Vec<String>,
    /// IP Addresses which will be used for egress.
    pub egress_ips: Vec<Ipv4Addr>,
    /// Context for generating an AWS Principal.
    pub aws_principal_context: Option<AwsPrincipalContext>,
    /// Supported AWS PrivateLink availability zone ids.
    pub aws_privatelink_availability_zones: Option<BTreeSet<String>>,
    /// Host name or URL for connecting to the HTTP server of this instance.
    pub http_host_name: Option<String>,
    /// Context for source and sink connections.
    pub connection_context: mz_storage_types::connections::ConnectionContext,
    /// Global connection limit and count
    pub active_connection_count: Arc<std::sync::Mutex<ConnectionCounter>>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusterReplicaSizeMap(pub BTreeMap<String, ReplicaAllocation>);

impl ClusterReplicaSizeMap {
    /// Iterate all enabled (not disabled) replica allocations, with their name.
    pub fn enabled_allocations(&self) -> impl Iterator<Item = (&String, &ReplicaAllocation)> {
        self.0.iter().filter(|(_, a)| !a.disabled)
    }
}

impl Default for ClusterReplicaSizeMap {
    // Used for testing and local purposes. This default value should not be used in production.
    //
    // Credits per hour are calculated as being equal to scale. This is not necessarily how the
    // value is computed in production.
    fn default() -> Self {
        // {
        //     "1": {"scale": 1, "workers": 1},
        //     "2": {"scale": 1, "workers": 2},
        //     "4": {"scale": 1, "workers": 4},
        //     /// ...
        //     "32": {"scale": 1, "workers": 32}
        //     /// Testing with multiple processes on a single machine
        //     "2-4": {"scale": 2, "workers": 4},
        //     /// Used in mzcompose tests
        //     "2-2": {"scale": 2, "workers": 2},
        //     ...
        //     "16-16": {"scale": 16, "workers": 16},
        //     /// Used in the shared_fate cloudtest tests
        //     "2-1": {"scale": 2, "workers": 1},
        //     ...
        //     "16-1": {"scale": 16, "workers": 1},
        //     /// Used in the cloudtest tests that force OOMs
        //     "mem-2": { "memory_limit": 2Gb },
        //     ...
        //     "mem-16": { "memory_limit": 16Gb },
        // }
        let mut inner = (0..=5)
            .flat_map(|i| {
                let workers: u8 = 1 << i;
                [
                    (workers.to_string(), None),
                    (format!("{workers}-4G"), Some(4)),
                    (format!("{workers}-8G"), Some(8)),
                    (format!("{workers}-16G"), Some(16)),
                    (format!("{workers}-32G"), Some(32)),
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
                format!("{scale}-1"),
                ReplicaAllocation {
                    memory_limit: None,
                    cpu_limit: None,
                    disk_limit: None,
                    scale,
                    workers: 1,
                    credits_per_hour: scale.into(),
                    cpu_exclusive: false,
                    disabled: false,
                    selectors: BTreeMap::default(),
                },
            );

            inner.insert(
                format!("{scale}-{scale}"),
                ReplicaAllocation {
                    memory_limit: None,
                    cpu_limit: None,
                    disk_limit: None,
                    scale,
                    workers: scale.into(),
                    credits_per_hour: scale.into(),
                    cpu_exclusive: false,
                    disabled: false,
                    selectors: BTreeMap::default(),
                },
            );

            inner.insert(
                format!("mem-{scale}"),
                ReplicaAllocation {
                    memory_limit: Some(MemoryLimit(ByteSize(u64::cast_from(scale) * (1 << 30)))),
                    cpu_limit: None,
                    disk_limit: None,
                    scale: 1,
                    workers: 8,
                    credits_per_hour: 1.into(),
                    cpu_exclusive: false,
                    disabled: false,
                    selectors: BTreeMap::default(),
                },
            );
        }

        inner.insert(
            "2-4".to_string(),
            ReplicaAllocation {
                memory_limit: None,
                cpu_limit: None,
                disk_limit: None,
                scale: 2,
                workers: 4,
                credits_per_hour: 2.into(),
                cpu_exclusive: false,
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
                disabled: true,
                selectors: BTreeMap::default(),
            },
        );

        for size in ["1cc", "1C"] {
            inner.insert(
                size.to_string(),
                ReplicaAllocation {
                    memory_limit: None,
                    cpu_limit: None,
                    disk_limit: None,
                    scale: 1,
                    workers: 1,
                    credits_per_hour: 1.into(),
                    cpu_exclusive: false,
                    disabled: false,
                    selectors: BTreeMap::default(),
                },
            );
        }

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
    pub fn to_principal_string(&self, aws_external_id_suffix: GlobalId) -> String {
        format!(
            "arn:aws:iam::{}:role/mz_{}_{}",
            self.aws_account_id, self.aws_external_id_prefix, aws_external_id_suffix
        )
    }
}
