// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::num::NonZeroUsize;

use serde::Deserialize;

use mz_build_info::BuildInfo;
use mz_controller::ComputeInstanceReplicaAllocation;
use mz_ore::metrics::MetricsRegistry;

use crate::catalog::storage;

/// Configures a catalog.
#[derive(Debug)]
pub struct Config<'a, S> {
    /// The connection to the stash.
    pub storage: storage::Connection<S>,
    /// Whether to enable unsafe mode.
    pub unsafe_mode: bool,
    /// Information about this build of Materialize.
    pub build_info: &'static BuildInfo,
    /// Function to generate wall clock now; can be mocked.
    pub now: mz_ore::now::NowFn,
    /// Whether or not to skip catalog migrations.
    pub skip_migrations: bool,
    /// The registry that catalog uses to report metrics.
    pub metrics_registry: &'a MetricsRegistry,
    /// Map of strings to corresponding compute replica sizes.
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    /// Valid availability zones for replicas.
    pub availability_zones: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClusterReplicaSizeMap(pub HashMap<String, ComputeInstanceReplicaAllocation>);

impl Default for ClusterReplicaSizeMap {
    fn default() -> Self {
        // {
        //     "1": {"scale": 1, "workers": 1},
        //     "2": {"scale": 1, "workers": 2},
        //     "4": {"scale": 1, "workers": 4},
        //     /// ...
        //     "32": {"scale": 1, "workers": 32}
        //     /// Testing with multiple processes on a single machine is a novelty, so
        //     /// we don't bother providing many options.
        //     "2-1": {"scale": 2, "workers": 1},
        //     "2-2": {"scale": 2, "workers": 2},
        //     "2-4": {"scale": 2, "workers": 4},
        // }
        let mut inner = (0..=5)
            .map(|i| {
                let workers = 1 << i;
                (
                    workers.to_string(),
                    ComputeInstanceReplicaAllocation {
                        memory_limit: None,
                        cpu_limit: None,
                        scale: NonZeroUsize::new(1).unwrap(),
                        workers: NonZeroUsize::new(workers).unwrap(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        inner.insert(
            "2-1".to_string(),
            ComputeInstanceReplicaAllocation {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(1).unwrap(),
            },
        );
        inner.insert(
            "2-2".to_string(),
            ComputeInstanceReplicaAllocation {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(2).unwrap(),
            },
        );
        inner.insert(
            "2-4".to_string(),
            ComputeInstanceReplicaAllocation {
                memory_limit: None,
                cpu_limit: None,
                scale: NonZeroUsize::new(2).unwrap(),
                workers: NonZeroUsize::new(4).unwrap(),
            },
        );
        Self(inner)
    }
}
