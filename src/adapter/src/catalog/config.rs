// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::net::Ipv4Addr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use serde::Deserialize;

use mz_build_info::BuildInfo;
use mz_compute_client::controller::ComputeReplicaAllocation;
use mz_ore::metrics::MetricsRegistry;
use mz_secrets::SecretsReader;
use mz_storage_client::types::hosts::StorageHostResourceAllocation;

use crate::catalog::storage;

/// Configures a catalog.
#[derive(Debug)]
pub struct Config<'a, S> {
    /// The connection to the stash.
    pub storage: storage::Connection<S>,
    /// Whether to enable unsafe mode.
    pub unsafe_mode: bool,
    /// Whether to enable persisted introspection sources.
    pub persisted_introspection: bool,
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// A persistent ID associated with the environment.
    pub environment_id: String,
    /// Function to generate wall clock now; can be mocked.
    pub now: mz_ore::now::NowFn,
    /// Whether or not to skip catalog migrations.
    pub skip_migrations: bool,
    /// The registry that catalog uses to report metrics.
    pub metrics_registry: &'a MetricsRegistry,
    /// Map of strings to corresponding compute replica sizes.
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    /// Map of strings to corresponding storage host sizes.
    pub storage_host_sizes: StorageHostSizeMap,
    /// Default storage host size, should be a key from storage_host_sizes.
    pub default_storage_host_size: Option<String>,
    /// Values to set for system parameters, if those system parameters have not
    /// already been set by the system user.
    pub bootstrap_system_parameters: BTreeMap<String, String>,
    /// Valid availability zones for replicas.
    pub availability_zones: Vec<String>,
    /// A handle to a secrets manager that can only read secrets.
    pub secrets_reader: Arc<dyn SecretsReader>,
    /// IP Addresses which will be used for egress.
    pub egress_ips: Vec<Ipv4Addr>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterReplicaSizeMap(pub HashMap<String, ComputeReplicaAllocation>);

impl Default for ClusterReplicaSizeMap {
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
        // }
        let mut inner = (0..=5)
            .map(|i| {
                let workers = 1 << i;
                (
                    workers.to_string(),
                    ComputeReplicaAllocation {
                        memory_limit: None,
                        cpu_limit: None,
                        scale: NonZeroUsize::new(1).unwrap(),
                        workers: NonZeroUsize::new(workers).unwrap(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();

        for i in 1..=5 {
            let scale = 1 << i;
            inner.insert(
                format!("{scale}-1"),
                ComputeReplicaAllocation {
                    memory_limit: None,
                    cpu_limit: None,
                    scale: NonZeroUsize::new(scale).unwrap(),
                    workers: NonZeroUsize::new(1).unwrap(),
                },
            );

            inner.insert(
                format!("{scale}-{scale}"),
                ComputeReplicaAllocation {
                    memory_limit: None,
                    cpu_limit: None,
                    scale: NonZeroUsize::new(scale).unwrap(),
                    workers: NonZeroUsize::new(scale).unwrap(),
                },
            );
        }

        inner.insert(
            "2-4".to_string(),
            ComputeReplicaAllocation {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(4).unwrap(),
            },
        );
        Self(inner)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageHostSizeMap(pub HashMap<String, StorageHostResourceAllocation>);

impl Default for StorageHostSizeMap {
    fn default() -> Self {
        Self(
            (0..=5)
                .map(|i| {
                    let workers = 1 << i;
                    (
                        workers.to_string(),
                        StorageHostResourceAllocation {
                            memory_limit: None,
                            cpu_limit: None,
                            workers: NonZeroUsize::new(workers).unwrap(),
                        },
                    )
                })
                .collect::<HashMap<_, _>>(),
        )
    }
}
