// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_build_info::BuildInfo;
use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
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
    /// The SDK key.
    ld_sdk_key: String,
    /// A map from parameter names to LaunchDarkly feature keys
    /// to use when populating the [SynchronizedParameters]
    /// instance in [SystemParameterFrontend::pull].
    ld_key_map: BTreeMap<String, String>,
}

impl SystemParameterSyncConfig {
    /// Construct a new [SystemParameterFrontend] instance.
    pub fn new(
        env_id: EnvironmentId,
        build_info: &'static BuildInfo,
        registry: &MetricsRegistry,
        ld_sdk_key: String,
        ld_key_map: BTreeMap<String, String>,
    ) -> Self {
        Self {
            env_id,
            build_info,
            metrics: Metrics::register_into(registry),
            ld_sdk_key,
            ld_key_map,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct Metrics {
    pub params_changed: IntCounter,
}

impl Metrics {
    pub(super) fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            params_changed: registry.register(metric!(
                name: "mz_parameter_frontend_params_changed",
                help: "The number of parameter changes pulled from the LaunchDarkly frontend.",
            )),
        }
    }
}
