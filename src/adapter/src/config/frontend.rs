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
use mz_cluster_client::ReplicaId;
use mz_controller_types::ClusterId;
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
    /// The environment ID, used to build scoped (`cluster` / `replica`)
    /// evaluation contexts.
    env_id: EnvironmentId,
    /// Build info, used to build scoped evaluation contexts.
    build_info: &'static BuildInfo,
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
                env_id: sync_config.env_id.clone(),
                build_info: sync_config.build_info,
                metrics: sync_config.metrics.clone(),
            }),
            SystemParameterSyncClientConfig::LaunchDarkly { sdk_key, now_fn } => Ok(Self {
                client: SystemParameterFrontendClient::LaunchDarkly {
                    client: ld_client(sdk_key, &sync_config.metrics, now_fn).await?,
                    // The environment-wide context carries no cluster/replica
                    // scope. Scoped evaluation passes a `cluster` or `replica`
                    // context per pass via [`ld_ctx`].
                    ctx: ld_ctx(&sync_config.env_id, sync_config.build_info, None, None)?,
                },
                env_id: sync_config.env_id.clone(),
                build_info: sync_config.build_info,
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

    /// Evaluates the replica-local scoped parameters for each given replica and
    /// returns, per cluster and replica, the parameter values that differ from
    /// the environment-wide value held in `params`.
    ///
    /// Only the LaunchDarkly client performs scoped evaluation. The file
    /// client returns an empty map (replicas fall back to the environment-wide value).
    /// The returned map is sparse: replicas (and clusters) with no overriding
    /// value are omitted.
    pub fn pull_replica_overrides(
        &self,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        replicas: &[ReplicaEvalContext],
    ) -> BTreeMap<ReplicaId, BTreeMap<String, String>> {
        let mut out: BTreeMap<ReplicaId, BTreeMap<String, String>> = BTreeMap::new();

        let SystemParameterFrontendClient::LaunchDarkly { client, .. } = &self.client else {
            // The file client has no notion of scoped evaluation.
            return out;
        };

        if param_names.is_empty() {
            return out;
        }

        for replica in replicas {
            let ctx = match ld_ctx(
                &self.env_id,
                self.build_info,
                Some(&replica.cluster),
                Some(&replica.replica),
            ) {
                Ok(ctx) => ctx,
                Err(e) => {
                    warn!(
                        replica_id = %replica.replica.id,
                        "could not build scoped LD context: {e}"
                    );
                    continue;
                }
            };

            let overrides = self.evaluate_scoped_overrides(client, &ctx, params, param_names);
            if !overrides.is_empty() {
                out.insert(replica.replica_id, overrides);
            }
        }

        out
    }

    /// Evaluates the cluster-coherent scoped parameters for each given cluster
    /// and returns, per cluster, the parameter values that differ from the
    /// environment-wide value held in `params`. Evaluated replica-free (the
    /// `cluster` context kind), so the value cannot vary by replica.
    ///
    /// Only the LaunchDarkly client performs scoped evaluation. The file
    /// client returns an empty map. The returned map is sparse.
    pub fn pull_cluster_overrides(
        &self,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        clusters: &[ClusterEvalContext],
    ) -> BTreeMap<ClusterId, BTreeMap<String, String>> {
        let mut out: BTreeMap<ClusterId, BTreeMap<String, String>> = BTreeMap::new();

        let SystemParameterFrontendClient::LaunchDarkly { client, .. } = &self.client else {
            // The file client has no notion of scoped evaluation.
            return out;
        };

        if param_names.is_empty() {
            return out;
        }

        for cluster in clusters {
            let ctx = match ld_ctx(&self.env_id, self.build_info, Some(&cluster.cluster), None) {
                Ok(ctx) => ctx,
                Err(e) => {
                    warn!(
                        cluster_id = %cluster.cluster.id,
                        "could not build scoped LD context: {e}"
                    );
                    continue;
                }
            };

            let overrides = self.evaluate_scoped_overrides(client, &ctx, params, param_names);
            if !overrides.is_empty() {
                out.insert(cluster.cluster_id, overrides);
            }
        }

        out
    }

    /// Evaluates each of `param_names` against `ctx`, returning only the values
    /// that differ from the environment-wide value held in `params`. Shared by
    /// the cluster and replica passes, so the returned map is sparse.
    ///
    /// We record on the differs-from-env test, not the `variation_detail`
    /// reason. The inline comment at the recording decision explains why.
    fn evaluate_scoped_overrides(
        &self,
        client: &ld::Client,
        ctx: &ld::Context,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
    ) -> BTreeMap<String, String> {
        let mut overrides = BTreeMap::new();
        for &param_name in param_names {
            let flag_name = self
                .key_map
                .get(param_name)
                .map(|flag_name| flag_name.as_str())
                .unwrap_or(param_name);

            let base = params.get(param_name);
            // Evaluate with `base` as the default, so a silent LD (flag absent,
            // off, error, failed prerequisite) resolves back to the env-wide
            // value and is dropped by the difference test below.
            let flag_var = client.variation(ctx, flag_name, base.clone());
            let value = match flag_var {
                ld::FlagValue::Bool(v) => v.to_string(),
                ld::FlagValue::Str(v) => v,
                ld::FlagValue::Number(v) => v.to_string(),
                ld::FlagValue::Json(v) => v.to_string(),
            };

            // Record iff the scoped evaluation *differs* from the env-wide value.
            // The `variation_detail` reason is the wrong signal: it cannot say
            // which context kind's clause matched (an env-level rule and a
            // cluster-specific rule both report `RuleMatch`), and `Fallthrough`
            // serves the env-wide value to every object. Comparing against the
            // env-wide baseline is the only signal that means "this scope context
            // changed the answer", which is what must beat a manual `FEATURES`
            // pin and what keeps the durable collections sparse. See the scoped
            // feature flags design, §Resolution.
            //
            // Compare in the parameter's canonical encoding. `base` is the
            // var-formatted env-wide value (a `bool` is `"on"`/`"off"`), whereas
            // the raw LaunchDarkly value spells a boolean `"true"`/`"false"`, so a
            // direct string compare would treat every boolean flag as differing,
            // even on `Fallthrough`. We still *store* the raw `value` (downstream
            // consumers parse `"true"`/`"false"`). Only the decision is canonical.
            let differs = match params.canonicalize(param_name, &value) {
                Some(canonical) => canonical != base,
                // LaunchDarkly served a value that does not parse for this
                // parameter's type (e.g. a malformed boolean like `"maybe"`).
                // Never record it: storing an unparseable value would poison
                // resolution. The optimizer's `bool` decode, for one, panics on
                // every plan for a cluster-coherent override it cannot parse.
                // Treat it as "no scoped opinion" and fall back to the env-wide
                // value.
                None => false,
            };
            if differs {
                overrides.insert(param_name.to_string(), value);
            }
        }
        overrides
    }
}

/// The identity of a single live replica, used to evaluate replica-local scoped
/// parameters in [`SystemParameterFrontend::pull_replica_overrides`].
#[derive(Clone, Debug)]
pub struct ReplicaEvalContext {
    /// The owning cluster's id.
    pub cluster_id: ClusterId,
    /// The replica's id.
    pub replica_id: ReplicaId,
    /// The owning cluster's scope context (for the replica-free, cluster pass).
    pub cluster: ClusterScopeContext,
    /// The replica's scope context.
    pub replica: ReplicaScopeContext,
}

impl ReplicaEvalContext {
    /// Builds the eval context for a replica owned by `cluster`. The owning
    /// cluster's scope context supplies both the cluster-free pass and the
    /// replica's cluster attributes (id, name, builtin flag), so the caller
    /// passes it once rather than restating those fields.
    pub(crate) fn for_replica(
        cluster_id: ClusterId,
        cluster: ClusterScopeContext,
        replica_id: ReplicaId,
        name: String,
        size: String,
        size_family: String,
    ) -> Self {
        let replica = ReplicaScopeContext {
            id: replica_id.to_string(),
            name,
            is_builtin: cluster.is_builtin,
            size,
            size_family,
            cluster_id: cluster.id.clone(),
            cluster_name: cluster.name.clone(),
        };
        ReplicaEvalContext {
            cluster_id,
            replica_id,
            cluster,
            replica,
        }
    }
}

/// The identity of a single live cluster, used to evaluate cluster-coherent
/// scoped parameters in [`SystemParameterFrontend::pull_cluster_overrides`].
#[derive(Clone, Debug)]
pub struct ClusterEvalContext {
    /// The cluster's id.
    pub cluster_id: ClusterId,
    /// The cluster's scope context (replica-free).
    pub cluster: ClusterScopeContext,
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

/// Identity of a cluster, used to build a `cluster` context kind for
/// cluster-coherent scoped feature flags.
///
/// Exposes both `id` and `name`: an LD rule that targets `cluster_id` is an
/// incarnation pin that dies on drop/recreate (ids are never reused), while a
/// rule targeting `cluster_name` / `is_builtin` is a durable role predicate
/// that re-applies to any matching cluster. See the scoped feature flags
/// design.
#[derive(Clone, Debug)]
pub struct ClusterScopeContext {
    /// The cluster's catalog id, e.g. `s2` or `u1`.
    pub id: String,
    /// The cluster's name, e.g. `mz_catalog_server`.
    pub name: String,
    /// Whether the cluster is a builtin (system) cluster.
    pub is_builtin: bool,
}

/// Identity of a replica, used to build a `replica` context kind for
/// replica-local scoped feature flags.
///
/// Carries the owning cluster's identity as attributes so that replica-local
/// flags can be cluster-targeted without a second evaluation, and the replica
/// size and size *family* so flags can be keyed by size family (e.g. legacy
/// sizes keep `lgalloc`). See the scoped feature flags design.
#[derive(Clone, Debug)]
pub struct ReplicaScopeContext {
    /// The replica's catalog id.
    pub id: String,
    /// The replica's name.
    pub name: String,
    /// Whether the replica belongs to a builtin (system) cluster.
    pub is_builtin: bool,
    /// The replica's full size name, e.g. `D.1-xsmall` or a legacy t-shirt size
    /// like `xsmall`. This is the fine-grained targeting axis. The coarse axis
    /// is [`Self::size_family`]. The two are distinct: `D.1-xsmall` is a size,
    /// `D` is its family.
    pub size: String,
    /// The replica's size family, e.g. `D` or `legacy`. The coarse targeting
    /// axis, derived from the size map rather than the size name (see
    /// [`Self::size`]).
    pub size_family: String,
    /// The owning cluster's catalog id.
    pub cluster_id: String,
    /// The owning cluster's name.
    pub cluster_name: String,
}

/// Builds a single `cluster` context kind from a [`ClusterScopeContext`].
///
/// Deliberately replica-free: cluster-coherent flags must resolve identically
/// across a cluster's replicas, so no replica/size attributes appear here.
fn cluster_context(cluster: &ClusterScopeContext) -> Result<ld::Context, anyhow::Error> {
    ld::ContextBuilder::new(cluster.id.as_str())
        .anonymous(true) // keep the LD dashboard Contexts list clean
        .kind("cluster")
        .set_string("cluster_id", cluster.id.clone())
        .set_string("cluster_name", cluster.name.clone())
        .set_string("is_builtin", cluster.is_builtin.to_string())
        .build()
        .map_err(|e| anyhow::anyhow!(e))
}

/// Builds a single `replica` context kind from a [`ReplicaScopeContext`].
///
/// Includes the owning cluster's identity so a rule can combine both axes,
/// e.g. "size family `D` *and* cluster `foo`".
fn replica_context(replica: &ReplicaScopeContext) -> Result<ld::Context, anyhow::Error> {
    ld::ContextBuilder::new(replica.id.as_str())
        .anonymous(true) // keep the LD dashboard Contexts list clean
        .kind("replica")
        .set_string("replica_id", replica.id.clone())
        .set_string("replica_name", replica.name.clone())
        .set_string("is_builtin", replica.is_builtin.to_string())
        .set_string("replica_size", replica.size.clone())
        .set_string("replica_size_family", replica.size_family.clone())
        .set_string("cluster_id", replica.cluster_id.clone())
        .set_string("cluster_name", replica.cluster_name.clone())
        .build()
        .map_err(|e| anyhow::anyhow!(e))
}

/// Builds a multi-context for evaluating scoped feature flags.
///
/// Composes the base contexts (`environment` + `organization` + `build`) with:
/// - a `cluster` context for cluster-coherent (replica-free) resolution, and/or
/// - a `replica` context for replica-local resolution.
///
/// The environment-wide pass passes `None` for both. This is the single entry
/// point the sync loop uses to evaluate each scoped pass.
fn ld_ctx(
    env_id: &EnvironmentId,
    build_info: &'static BuildInfo,
    cluster: Option<&ClusterScopeContext>,
    replica: Option<&ReplicaScopeContext>,
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

    // Cluster-coherent resolution evaluates with a `cluster` context (no
    // replica attributes). Replica-local resolution additionally carries a
    // `replica` context. The environment-wide pass carries neither.
    if let Some(cluster) = cluster {
        ctx_builder.add_context(cluster_context(cluster)?);
    }
    if let Some(replica) = replica {
        ctx_builder.add_context(replica_context(replica)?);
    }

    ctx_builder.build().map_err(|e| anyhow::anyhow!(e))
}

#[cfg(test)]
mod tests {
    use mz_build_info::DUMMY_BUILD_INFO;

    use super::*;

    fn env_id() -> EnvironmentId {
        EnvironmentId::for_tests()
    }

    #[mz_ore::test]
    fn builds_cluster_scoped_context() {
        // Cluster-coherent resolution evaluates with a replica-free `cluster`
        // context.
        let cluster = ClusterScopeContext {
            id: "s2".into(),
            name: "mz_catalog_server".into(),
            is_builtin: true,
        };
        ld_ctx(&env_id(), &DUMMY_BUILD_INFO, Some(&cluster), None)
            .expect("cluster-scoped context builds");
    }

    #[mz_ore::test]
    fn builds_replica_scoped_context() {
        // Replica-local resolution carries both a `cluster` and a `replica`
        // context so a rule can combine size family and cluster.
        let cluster = ClusterScopeContext {
            id: "u1".into(),
            name: "quickstart".into(),
            is_builtin: false,
        };
        let replica = ReplicaScopeContext {
            id: "u1-replica-1".into(),
            name: "r1".into(),
            is_builtin: false,
            size: "D.1-xsmall".into(),
            size_family: "D".into(),
            cluster_id: "u1".into(),
            cluster_name: "quickstart".into(),
        };
        ld_ctx(&env_id(), &DUMMY_BUILD_INFO, Some(&cluster), Some(&replica))
            .expect("replica-scoped context builds");
    }

    #[mz_ore::test]
    fn environment_wide_context_is_unscoped() {
        ld_ctx(&env_id(), &DUMMY_BUILD_INFO, None, None).expect("environment-wide context builds");
    }
}
