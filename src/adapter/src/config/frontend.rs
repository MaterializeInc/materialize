// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use hyper_tls::HttpsConnector;
use launchdarkly_server_sdk as ld;
use mz_build_info::BuildInfo;
use mz_cloud_provider::CloudProvider;
use mz_ore::now::NowFn;
use mz_sql::catalog::EnvironmentId;
use serde_json::Value as JsonValue;
use tokio::time;
use tracing::warn;

use crate::config::{
    Metrics, SynchronizedParameters, SystemParameterSyncClientConfig, SystemParameterSyncConfig,
};

/// A frontend client for pulling [SynchronizedParameters] from LaunchDarkly.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct SystemParameterFrontend {
    /// An SDK client to mediate interactions with the LaunchDarkly and json config file clients.
    client: SystemParameterFrontendClient,
    /// A map from parameter names to LaunchDarkly feature keys
    /// to use when populating the [SynchronizedParameters]
    /// instance in [SystemParameterFrontend::pull].
    key_map: BTreeMap<String, String>,
    /// Frontend metrics.
    metrics: Metrics,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub enum SystemParameterFrontendClient {
    File {
        path: PathBuf,
    },
    LaunchDarkly {
        /// An SDK client to mediate interactions with the LaunchDarkly client.
        #[derivative(Debug = "ignore")]
        client: ld::Client,
        /// The context to use when querying LaunchDarkly using the SDK.
        /// This scopes down queries to a specific key.
        ctx: ld::Context,
    },
}

impl SystemParameterFrontendClient {}

impl SystemParameterFrontend {
    /// Create a new [SystemParameterFrontend] initialize.
    ///
    /// This will create and initialize an [ld::Client] instance. The
    /// [ld::Client::initialized_async] call will be attempted in a loop with an
    /// exponential backoff with power `2s` and max duration `60s`.
    pub async fn from(sync_config: &SystemParameterSyncConfig) -> Result<Self, anyhow::Error> {
        match &sync_config.backend_config {
            super::SystemParameterSyncClientConfig::File { path } => Ok(Self {
                client: SystemParameterFrontendClient::File { path: path.clone() },
                key_map: sync_config.key_map.clone(),
                metrics: sync_config.metrics.clone(),
            }),
            SystemParameterSyncClientConfig::LaunchDarkly { sdk_key, now_fn } => Ok(Self {
                client: SystemParameterFrontendClient::LaunchDarkly {
                    client: ld_client(sdk_key, &sync_config.metrics, now_fn).await?,
                    ctx: ld_ctx(&sync_config.env_id, sync_config.build_info)?,
                },
                metrics: sync_config.metrics.clone(),
                key_map: sync_config.key_map.clone(),
            }),
        }
    }

    /// Pull the current values for all [SynchronizedParameters] from the
    /// [SystemParameterFrontend] and return `true` iff at least one parameter
    /// value was modified.
    pub fn pull(&self, params: &mut SynchronizedParameters) -> bool {
        let mut changed = false;
        for param_name in params.synchronized().into_iter() {
            let flag_name = self
                .key_map
                .get(param_name)
                .map(|flag_name| flag_name.as_str())
                .unwrap_or(param_name);

            let flag_str = match self.client {
                SystemParameterFrontendClient::LaunchDarkly {
                    ref client,
                    ref ctx,
                } => {
                    let flag_var = client.variation(ctx, flag_name, params.get(param_name));
                    match flag_var {
                        ld::FlagValue::Bool(v) => v.to_string(),
                        ld::FlagValue::Str(v) => v,
                        ld::FlagValue::Number(v) => v.to_string(),
                        ld::FlagValue::Json(v) => v.to_string(),
                    }
                }
                SystemParameterFrontendClient::File { ref path } => {
                    let file_contents = fs::read_to_string(path)
                        .inspect_err(|e| warn!("Could not open system paraemter sync file {}", e))
                        .unwrap_or_default();
                    let values: BTreeMap<String, JsonValue> = serde_json::from_str(&file_contents)
                        .inspect_err(|e| warn!("Could not open system paraemter sync file {:?}", e))
                        .unwrap_or_default();
                    values
                        .get(flag_name)
                        .and_then(|o| match o {
                            serde_json::Value::String(v) => Some(v.to_string()),
                            serde_json::Value::Number(v) => Some(v.to_string()),
                            serde_json::Value::Bool(v) => Some(v.to_string()),
                            serde_json::Value::Object(_) => Some(o.to_string()),
                            serde_json::Value::Array(_) => Some(o.to_string()),
                            serde_json::Value::Null => None,
                        })
                        .unwrap_or_else(|| params.get(param_name))
                }
            };

            let old = params.get(param_name);
            let change = params.modify(param_name, flag_str.as_str());
            if change {
                tracing::debug!(
                    %param_name, %old, new = %flag_str,
                    "updating system param",
                );
            }
            self.metrics.params_changed.inc_by(u64::from(change));
            changed |= change;
        }

        changed
    }
}

fn ld_config(api_key: &str, metrics: &Metrics) -> ld::Config {
    ld::ConfigBuilder::new(api_key)
        .event_processor(
            ld::EventProcessorBuilder::new()
                .https_connector(HttpsConnector::new())
                .on_success({
                    let last_cse_time_seconds = metrics.last_cse_time_seconds.clone();
                    Arc::new(move |result| {
                        if let Ok(ts) = u64::try_from(result.time_from_server / 1000) {
                            last_cse_time_seconds.set(ts);
                        } else {
                            tracing::warn!(
                                "Cannot convert time_from_server / 1000 from u128 to u64"
                            );
                        }
                    })
                }),
        )
        .data_source(ld::StreamingDataSourceBuilder::new().https_connector(HttpsConnector::new()))
        .build()
        .expect("valid config")
}

async fn ld_client(
    api_key: &str,
    metrics: &Metrics,
    now_fn: &NowFn,
) -> Result<ld::Client, anyhow::Error> {
    let ld_client = ld::Client::build(ld_config(api_key, metrics))?;
    tracing::info!("waiting for SystemParameterFrontend to initialize");
    // Start and initialize LD client for the frontend. The callback passed
    // will export the last time when an SSE event from the LD server was
    // received in a Prometheus metric.
    ld_client.start_with_default_executor_and_callback({
        let last_sse_time_seconds = metrics.last_sse_time_seconds.clone();
        let now_fn = now_fn.clone();
        Arc::new(move |_ev| {
            let ts = now_fn() / 1000;
            last_sse_time_seconds.set(ts);
        })
    });

    let max_backoff = Duration::from_secs(60);
    let mut backoff = Duration::from_secs(5);
    let timeout = Duration::from_secs(10);

    // TODO(materialize#32030): fix retry logic
    loop {
        match ld_client.wait_for_initialization(timeout).await {
            Some(true) => break,
            Some(false) => tracing::warn!("SystemParameterFrontend failed to initialize"),
            None => tracing::warn!("SystemParameterFrontend initialization timed out"),
        }

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
