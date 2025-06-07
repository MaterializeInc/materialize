// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::path::PathBuf;

use mz_build_info::BuildInfo;
use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, UIntGauge};
use mz_ore::now::NowFn;
use mz_sql::catalog::EnvironmentId;
use prometheus::IntCounter;

mod backend;
mod frontend;
mod params;
mod sync;

pub use backend::SystemParameterBackend;
pub use frontend::SystemParameterFrontend;
pub use params::{ModifiedParameter, SynchronizedParameters};
pub use sync::system_parameter_sync;

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
            Self::LaunchDarkly { .. } => true,
            _ => false,
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
    pub last_cse_time_seconds: UIntGauge,
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
