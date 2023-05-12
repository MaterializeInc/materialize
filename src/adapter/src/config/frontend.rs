// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use derivative::Derivative;
use launchdarkly_server_sdk as ld;
use mz_ore::{
    metric,
    metrics::{MetricsRegistry, UIntGauge},
    now::NowFn,
};
use mz_sql::catalog::{CloudProvider, EnvironmentId};
use prometheus::IntCounter;
use tokio::time;

use super::SynchronizedParameters;

/// A frontend client for pulling [SynchronizedParameters] from LaunchDarkly.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct SystemParameterFrontend {
    /// An SDK client to mediate interactions with the LaunchDarkly client.
    #[derivative(Debug = "ignore")]
    ld_client: ld::Client,
    /// The context to use when quering LaunchDarkly using the SDK.
    /// This scopes down queries to a specific key.
    ld_ctx: ld::Context,
    /// A map from parameter names to LaunchDarkly feature keys
    /// to use when populating the the [SynchronizedParameters]
    /// instance in [SystemParameterFrontend::pull].
    ld_key_map: BTreeMap<String, String>,
    /// Frontend metrics.
    ld_metrics: Metrics,
    /// Function to return the current time.
    now_fn: NowFn,
}

impl SystemParameterFrontend {
    /// Construct a new [SystemParameterFrontend] instance.
    pub fn new(
        env_id: EnvironmentId,
        registry: &MetricsRegistry,
        ld_sdk_key: &str,
        ld_key_map: BTreeMap<String, String>,
        now_fn: NowFn,
    ) -> Result<Self, anyhow::Error> {
        let ld_metrics = Metrics::register_into(registry);
        let ld_config = ld::ConfigBuilder::new(ld_sdk_key)
            .event_processor(ld::EventProcessorBuilder::new().on_success({
                let last_known_time_seconds = ld_metrics.last_known_time_seconds.clone();
                let last_cse_time_seconds = ld_metrics.last_cse_time_seconds.clone();
                Arc::new(move |result| {
                    if let Ok(ts) = u64::try_from(result.time_from_server / 1000) {
                        last_known_time_seconds.set(ts);
                        last_cse_time_seconds.set(ts);
                    } else {
                        tracing::warn!("Cannot convert time_from_server / 1000 from u128 to u64");
                    }
                })
            }))
            .build();
        let ld_client = ld::Client::build(ld_config)?;
        let ld_ctx = if env_id.cloud_provider() != &CloudProvider::Local {
            ld::ContextBuilder::new(env_id.to_string())
                .kind("environment")
                .set_string("cloud_provider", env_id.cloud_provider().to_string())
                .set_string("cloud_provider_region", env_id.cloud_provider_region())
                .set_string("organization_id", env_id.organization_id().to_string())
                .set_string("ordinal", env_id.ordinal().to_string())
                .build()
                .map_err(|e| anyhow::anyhow!(e))?
        } else {
            // If cloud_provider is 'local', use an anonymous user with a custom
            // email and set the organization_id to `uuid::Uuid::nil()`, as
            // otherwise we will create a lot of additional contexts (which are
            // the billable entity for LaunchDarkly).
            ld::ContextBuilder::new("anonymous-dev@materialize.com")
                .anonymous(true) // exclude this user from the dashboard
                .kind("environment")
                .set_string("cloud_provider", env_id.cloud_provider().to_string())
                .set_string("cloud_provider_region", env_id.cloud_provider_region())
                .set_string("organization_id", uuid::Uuid::nil().to_string())
                .set_string("ordinal", env_id.ordinal().to_string())
                .build()
                .map_err(|e| anyhow::anyhow!(e))?
        };

        Ok(Self {
            ld_client,
            ld_ctx,
            ld_key_map,
            ld_metrics,
            now_fn,
        })
    }

    /// Ensure the backing [ld::Client] is initialized.
    ///
    /// The [ld::Client::initialized_async] call will be attempted in a loop
    /// with an exponential backoff with power `2s` and max duration `60s`.
    pub async fn ensure_initialized(&self) {
        tracing::info!("waiting for SystemParameterFrontend to initialize");

        // Start and initialize LD client for the frontend. The callback passed
        // will export the last time when an SSE event from the LD server was
        // received in a Prometheus metric.
        self.ld_client.start_with_default_executor_and_callback({
            let last_sse_time_seconds = self.ld_metrics.last_sse_time_seconds.clone();
            let now_fn = self.now_fn.clone();
            Arc::new(move |_ev| {
                let ts = now_fn() / 1000;
                last_sse_time_seconds.set(ts);
            })
        });

        let max_backoff = Duration::from_secs(60);
        let mut backoff = Duration::from_secs(5);
        while !self.ld_client.initialized_async().await {
            tracing::warn!("SystemParameterFrontend failed to initialize");
            time::sleep(backoff).await;
            backoff = (backoff * 2).min(max_backoff);
        }

        tracing::info!("successfully initialized SystemParameterFrontend");
    }

    /// Pull the current values for all [SynchronizedParameters] from the
    /// [SystemParameterFrontend] and return `true` iff at least one parameter
    /// value was modified.
    pub fn pull(&self, params: &mut SynchronizedParameters) -> bool {
        let mut changed = false;

        for param_name in params.synchronized().into_iter() {
            let flag_name = self
                .ld_key_map
                .get(param_name)
                .map(|flag_name| flag_name.as_str())
                .unwrap_or(param_name);

            let flag_var =
                self.ld_client
                    .variation(&self.ld_ctx, flag_name, params.get(param_name));

            let flag_str = match flag_var {
                ld::FlagValue::Bool(v) => v.to_string(),
                ld::FlagValue::Str(v) => v,
                ld::FlagValue::Number(v) => v.to_string(),
                ld::FlagValue::Json(v) => v.to_string(),
            };

            let change = params.modify(param_name, flag_str.as_str());
            self.ld_metrics.params_changed.inc_by(u64::from(change));
            changed |= change;
        }

        changed
    }
}

#[derive(Debug, Clone)]
struct Metrics {
    // TODO: remove this in favor of last_cse_time_seconds.
    pub last_known_time_seconds: UIntGauge,
    pub last_cse_time_seconds: UIntGauge,
    pub last_sse_time_seconds: UIntGauge,
    pub params_changed: IntCounter,
}

impl Metrics {
    fn register_into(registry: &MetricsRegistry) -> Self {
        Self {
            // TODO: remove this in favor of last_cse_time_seconds.
            last_known_time_seconds: registry.register(metric!(
                name: "mz_parameter_frontend_last_known_time_seconds",
                help: "The last known time of the LaunchDarkly frontend (as unix timestamp).",
            )),
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
