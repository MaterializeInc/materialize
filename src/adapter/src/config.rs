// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use mz_build_info::BuildInfo;
use mz_cluster_client::ReplicaId;
use mz_controller_types::ClusterId;
use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, UIntGauge};
#[cfg(feature = "telemetry")]
use mz_ore::now::NowFn;
use mz_sql::catalog::EnvironmentId;
use prometheus::IntCounter;

mod backend;
mod frontend;
mod params;
mod sync;

pub use backend::SystemParameterBackend;
pub use frontend::{
    ClusterEvalContext, ClusterScopeContext, ReplicaEvalContext, ReplicaScopeContext,
    SystemParameterFrontend,
};
pub use params::{ModifiedParameter, SynchronizedParameters};
pub use sync::system_parameter_sync;

/// Scoped (per-cluster and per-replica) system-parameter overrides, keyed by
/// object id. Each value is the raw (unparsed) string for a parameter whose
/// scoped value differs from the environment-wide value; an absent entry means
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

/// A factory for [SystemParameterFrontend] instances.
#[derive(Clone, Debug)]
pub struct SystemParameterSyncConfig {
    /// The environment ID that should identify connected clients.
    env_id: EnvironmentId,
    /// Build info for the environment running this.
    build_info: &'static BuildInfo,
    /// Parameter sync metrics.
    metrics: Metrics,
    ///  /// A map from parameter names to LaunchDarkly feature keys
    /// to use when populating the [SynchronizedParameters]
    /// instance in [SystemParameterFrontend::pull].
    key_map: BTreeMap<String, String>,
    /// Configuration for the parameter backend that we're syncing with.
    backend_config: SystemParameterSyncClientConfig,
}

#[derive(Clone, Debug)]
pub enum SystemParameterSyncClientConfig {
    File {
        // Path to a JSON config file that contains system parameters.
        path: PathBuf,
    },
    #[cfg(feature = "telemetry")]
    LaunchDarkly {
        /// The LaunchDarkly SDK key
        sdk_key: String,
        /// Function to return the current time.
        now_fn: NowFn,
    },
}

impl SystemParameterSyncClientConfig {
    fn is_launch_darkly(&self) -> bool {
        match &self {
            #[cfg(feature = "telemetry")]
            Self::LaunchDarkly { .. } => true,
            Self::File { .. } => false,
        }
    }
}

impl SystemParameterSyncConfig {
    /// Construct a new [SystemParameterFrontend] instance.
    pub fn new(
        env_id: EnvironmentId,
        build_info: &'static BuildInfo,
        registry: &MetricsRegistry,
        key_map: BTreeMap<String, String>,
        backend_config: SystemParameterSyncClientConfig,
    ) -> Self {
        Self {
            env_id,
            build_info,
            metrics: Metrics::register_into(registry),
            key_map,
            backend_config,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Metrics {
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    pub last_cse_time_seconds: UIntGauge,
    #[cfg_attr(not(feature = "telemetry"), allow(dead_code))]
    pub last_sse_time_seconds: UIntGauge,
    pub params_changed: IntCounter,
}

impl Metrics {
    pub(super) fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            last_cse_time_seconds: registry.register(metric!(
                name: "mz_parameter_frontend_last_cse_time_seconds",
                help: "The last known time when the LaunchDarkly client sent an event to the LaunchDarkly server (as unix timestamp).",
            )),
            last_sse_time_seconds: registry.register(metric!(
                name: "mz_parameter_frontend_last_sse_time_seconds",
                help: "The last known time when the LaunchDarkly client received an event from the LaunchDarkly server (as unix timestamp).",
            )),
            params_changed: registry.register(metric!(
                name: "mz_parameter_frontend_params_changed",
                help: "The number of parameter changes pulled from the LaunchDarkly frontend.",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use mz_cluster_client::ReplicaId;
    use mz_controller_types::ClusterId;

    use super::ScopedParameters;

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
