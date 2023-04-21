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
use serde::Deserialize;

use mz_build_info::BuildInfo;
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_controller::clusters::ReplicaAllocation;
use mz_orchestrator::MemoryLimit;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_repr::GlobalId;
use mz_secrets::SecretsReader;
use mz_sql::catalog::EnvironmentId;

use crate::catalog::storage;
use crate::config::SystemParameterFrontend;

/// Configures a catalog.
#[derive(Debug)]
pub struct Config<'a> {
    /// The connection to the stash.
    pub storage: storage::Connection,
    /// Whether to enable unsafe mode.
    pub unsafe_mode: bool,
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// A persistent ID associated with the environment.
    pub environment_id: EnvironmentId,
    /// Function to generate wall clock now; can be mocked.
    pub now: mz_ore::now::NowFn,
    /// Whether or not to skip catalog migrations.
    pub skip_migrations: bool,
    /// The registry that catalog uses to report metrics.
    pub metrics_registry: &'a MetricsRegistry,
    /// Map of strings to corresponding compute replica sizes.
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    /// Default storage cluster size. Must be a key from cluster_replica_sizes.
    pub default_storage_cluster_size: Option<String>,
    /// Values to set for system parameters, if those system parameters have not
    /// already been set by the system user.
    pub bootstrap_system_parameters: BTreeMap<String, String>,
    /// Valid availability zones for replicas.
    pub availability_zones: Vec<String>,
    /// A handle to a secrets manager that can only read secrets.
    pub secrets_reader: Arc<dyn SecretsReader>,
    /// IP Addresses which will be used for egress.
    pub egress_ips: Vec<Ipv4Addr>,
    /// Context for generating an AWS Principal.
    pub aws_principal_context: Option<AwsPrincipalContext>,
    /// Supported AWS PrivateLink availability zone ids.
    pub aws_privatelink_availability_zones: Option<BTreeSet<String>>,
    /// A optional frontend used to pull system parameters for initial sync in
    /// Catalog::open. A `None` value indicates that the initial sync should be
    /// skipped.
    pub system_parameter_frontend: Option<Arc<SystemParameterFrontend>>,
    /// How long to retain storage usage records
    pub storage_usage_retention_period: Option<Duration>,
    /// Needed only for migrating PG source column metadata. If `None`, will
    /// skip any migrations that require it, which will likely cause tests to
    /// fail.
    ///
    /// TODO(migration): delete in version v.51 (released in v0.49 + 1
    /// additional release)
    pub connection_context: Option<mz_storage_client::types::connections::ConnectionContext>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterReplicaSizeMap(pub BTreeMap<String, ReplicaAllocation>);

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
            .map(|i| {
                let workers: u8 = 1 << i;
                (
                    workers.to_string(),
                    ReplicaAllocation {
                        memory_limit: None,
                        cpu_limit: None,
                        scale: 1,
                        workers: workers.into(),
                        credits_per_hour: 1.into(),
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();

        for i in 1..=5 {
            let scale = 1 << i;
            inner.insert(
                format!("{scale}-1"),
                ReplicaAllocation {
                    memory_limit: None,
                    cpu_limit: None,
                    scale,
                    workers: 1,
                    credits_per_hour: scale.into(),
                },
            );

            inner.insert(
                format!("{scale}-{scale}"),
                ReplicaAllocation {
                    memory_limit: None,
                    cpu_limit: None,
                    scale,
                    workers: scale.into(),
                    credits_per_hour: scale.into(),
                },
            );

            inner.insert(
                format!("mem-{scale}"),
                ReplicaAllocation {
                    memory_limit: Some(MemoryLimit(ByteSize(u64::cast_from(scale) * (1 << 30)))),
                    cpu_limit: None,
                    scale: 1,
                    workers: 8,
                },
            );
        }

        inner.insert(
            "2-4".to_string(),
            ReplicaAllocation {
                memory_limit: None,
                cpu_limit: None,
                scale: 2,
                workers: 4,
                credits_per_hour: 2.into(),
            },
        );
        Self(inner)
    }
}

/// Context used to generate an AWS Principal.
///
/// In the case of AWS PrivateLink connections, Materialize will connect to the
/// VPC endpoint as the AWS Principal generated via this context.
#[derive(Debug, Clone)]
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
