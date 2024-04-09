// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use launchdarkly_server_sdk as ld;
use mz_build_info::BuildInfo;
use mz_ore::now::NowFn;
use mz_sql::catalog::{CloudProvider, EnvironmentId};
use tokio::time;

use crate::config::{Metrics, SynchronizedParameters, SystemParameterSyncConfig};

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
    /// to use when populating the [SynchronizedParameters]
    /// instance in [SystemParameterFrontend::pull].
    ld_key_map: BTreeMap<String, String>,
    /// Frontend metrics.
    ld_metrics: Metrics,
    /// Function to return the current time.
    now_fn: NowFn,
}

impl SystemParameterFrontend {
    /// Create a new [SystemParameterFrontend] initialize.
    ///
    /// This will create and initialize an [ld::Client] instance. The
    /// [ld::Client::initialized_async] call will be attempted in a loop with an
    /// exponential backoff with power `2s` and max duration `60s`.
    pub async fn from(sync_config: &SystemParameterSyncConfig) -> Result<Self, anyhow::Error> {
        Ok(Self {
            ld_client: ld_client(sync_config).await?,
            ld_ctx: ld_ctx(&sync_config.env_id, sync_config.build_info)?,
            ld_key_map: sync_config.ld_key_map.clone(),
            ld_metrics: sync_config.metrics.clone(),
            now_fn: sync_config.now_fn.clone(),
        })
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

impl Drop for SystemParameterFrontend {
    fn drop(&mut self) {
        tracing::info!("closing LaunchDarkly client");
        self.ld_client.close();
        tracing::info!("closed LaunchDarkly client");
    }
}

fn ld_config(sync_config: &SystemParameterSyncConfig) -> ld::Config {
    ld::ConfigBuilder::new(&sync_config.ld_sdk_key)
        .event_processor(ld::EventProcessorBuilder::new().on_success({
            let last_cse_time_seconds = sync_config.metrics.last_cse_time_seconds.clone();
            Arc::new(move |result| {
                if let Ok(ts) = u64::try_from(result.time_from_server / 1000) {
                    last_cse_time_seconds.set(ts);
                } else {
                    tracing::warn!("Cannot convert time_from_server / 1000 from u128 to u64");
                }
            })
        }))
        .build()
}

async fn ld_client(sync_config: &SystemParameterSyncConfig) -> Result<ld::Client, anyhow::Error> {
    let ld_client = ld::Client::build(ld_config(sync_config))?;

    tracing::info!("waiting for SystemParameterFrontend to initialize");

    // Start and initialize LD client for the frontend. The callback passed
    // will export the last time when an SSE event from the LD server was
    // received in a Prometheus metric.
    ld_client.start_with_default_executor_and_callback({
        let last_sse_time_seconds = sync_config.metrics.last_sse_time_seconds.clone();
        let now_fn = sync_config.now_fn.clone();
        Arc::new(move |_ev| {
            let ts = now_fn() / 1000;
            last_sse_time_seconds.set(ts);
        })
    });

    let max_backoff = Duration::from_secs(60);
    let mut backoff = Duration::from_secs(5);
    while !ld_client.initialized_async().await {
        tracing::warn!("SystemParameterFrontend failed to initialize");
        time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }

    tracing::info!("successfully initialized SystemParameterFrontend");

    Ok(ld_client)
}

fn ld_ctx(
    env_id: &EnvironmentId,
    build_info: &'static BuildInfo,
) -> Result<ld::Context, anyhow::Error> {
    // Register multiple contexts for this client.
    //
    // Unfortunately, it seems that the order in which conflicting targeting
    // rules are applied depends on the definition order of feature flag
    // variations rather than on the order in which context are registered with
    // the multi-context builder.
    let mut ctx_builder = ld::MultiContextBuilder::new();

    if env_id.cloud_provider() != &CloudProvider::Local {
        ctx_builder.add_context(
            ld::ContextBuilder::new(env_id.to_string())
                .kind("environment")
                .set_string("cloud_provider", env_id.cloud_provider().to_string())
                .set_string("cloud_provider_region", env_id.cloud_provider_region())
                .set_string("organization_id", env_id.organization_id().to_string())
                .set_string("ordinal", env_id.ordinal().to_string())
                .build()
                .map_err(|e| anyhow::anyhow!(e))?,
        );
        ctx_builder.add_context(
            ld::ContextBuilder::new(env_id.organization_id().to_string())
                .kind("organization")
                .build()
                .map_err(|e| anyhow::anyhow!(e))?,
        );
    } else {
        // If cloud_provider is 'local', use anonymous `environment` and
        // `organization` contexts with fixed keys, as otherwise we will create
        // a lot of additional contexts (which are the billable entity for
        // LaunchDarkly).
        ctx_builder.add_context(
            ld::ContextBuilder::new("anonymous-dev@materialize.com")
                .anonymous(true) // exclude this user from the dashboard
                .kind("environment")
                .set_string("cloud_provider", env_id.cloud_provider().to_string())
                .set_string("cloud_provider_region", env_id.cloud_provider_region())
                .set_string("organization_id", uuid::Uuid::nil().to_string())
                .set_string("ordinal", env_id.ordinal().to_string())
                .build()
                .map_err(|e| anyhow::anyhow!(e))?,
        );
        ctx_builder.add_context(
            ld::ContextBuilder::new(uuid::Uuid::nil().to_string())
                .anonymous(true) // exclude this user from the dashboard
                .kind("organization")
                .build()
                .map_err(|e| anyhow::anyhow!(e))?,
        );
    };

    ctx_builder.add_context(
        ld::ContextBuilder::new(build_info.sha)
            .kind("build")
            .set_string("semver_version", build_info.semver_version().to_string())
            .build()
            .map_err(|e| anyhow::anyhow!(e))?,
    );

    ctx_builder.build().map_err(|e| anyhow::anyhow!(e))
}
