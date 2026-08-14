// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use derivative::Derivative;
use futures::TryStreamExt;
use launchdarkly_sdk_transport::{ByteStream, HttpTransport, ResponseFuture};
use launchdarkly_server_sdk as ld;
use mz_build_info::BuildInfo;
use mz_cloud_provider::CloudProvider;
use mz_cluster_client::ReplicaId;
use mz_controller_types::ClusterId;
use mz_dyncfg::ParameterScope;
use mz_ore::metrics::UIntGauge;
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
    /// The config-sync file as of the last [`SystemParameterFrontend::pull`], or
    /// `None` before the first one. Never populated for the LaunchDarkly client.
    config_file: Mutex<Option<CachedConfigFile>>,
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

/// Reserved top-level key of the config-sync file holding the cluster-coherent
/// overrides, keyed by cluster name.
const CLUSTERS_SECTION: &str = "clusters";
/// Reserved top-level key of the config-sync file holding the replica-local
/// overrides, keyed by cluster name then replica name.
const REPLICAS_SECTION: &str = "replicas";

/// The parsed contents of the config-sync file.
///
/// The file is a JSON object whose keys are parameter names, except for the two
/// reserved section keys [`CLUSTERS_SECTION`] and [`REPLICAS_SECTION`]. A file
/// carrying neither reserved key is therefore a flat, wholly environment-wide
/// parameter map.
///
/// No synced system parameter may be named `clusters` or `replicas`, or the
/// reserved section would shadow it.
/// `test_no_synced_parameter_shadows_a_reserved_section` enforces that.
#[derive(Debug, Default, PartialEq)]
struct ConfigFile {
    /// Environment-wide values, keyed by the parameter's external name.
    environment: BTreeMap<String, JsonValue>,
    /// Cluster-coherent overrides, keyed by cluster name then external name.
    clusters: BTreeMap<String, BTreeMap<String, JsonValue>>,
    /// Replica-local overrides, keyed by cluster name, then replica name, then
    /// external name.
    ///
    /// Nested rather than keyed by a composite `"cluster.replica"` string
    /// because cluster and replica names are SQL identifiers that may themselves
    /// contain a `.`, which would make a composite key ambiguous.
    replicas: BTreeMap<String, BTreeMap<String, BTreeMap<String, JsonValue>>>,
}

impl ConfigFile {
    /// Parses the config-sync file's contents, or `None` if the document is not a
    /// JSON object.
    ///
    /// Individual sections are parsed leniently: a section, object entry, or
    /// value of the wrong shape is dropped with a warning rather than failing the
    /// parse, so one bad scoped entry cannot strand the rest of the file.
    ///
    /// A `None` return is "no information about any parameter", which callers must
    /// keep distinct from a valid but empty document. An empty document is a
    /// complete desired state of "no scoped overrides", which the reconcile
    /// applies by durably pruning every override. See
    /// [`SystemParameterFrontend::has_scoped_desired_state`].
    fn parse(contents: &str) -> Option<Self> {
        let values: BTreeMap<String, JsonValue> = match serde_json::from_str(contents) {
            Ok(values) => values,
            Err(e) => {
                warn!("could not parse system parameter sync file: {e}");
                return None;
            }
        };

        let mut file = Self::default();
        for (key, value) in values {
            match key.as_str() {
                CLUSTERS_SECTION => {
                    file.clusters = as_object(FilePosition::Section(CLUSTERS_SECTION), value)
                        .into_iter()
                        .map(|(cluster, params)| {
                            let params = as_object(FilePosition::Cluster(&cluster), params);
                            (cluster, params)
                        })
                        .collect();
                }
                REPLICAS_SECTION => {
                    file.replicas = as_object(FilePosition::Section(REPLICAS_SECTION), value)
                        .into_iter()
                        .map(|(cluster, replicas)| {
                            let replicas =
                                as_object(FilePosition::ClusterReplicas(&cluster), replicas)
                                    .into_iter()
                                    .map(|(replica, params)| {
                                        let params = as_object(
                                            FilePosition::Replica {
                                                cluster: &cluster,
                                                replica: &replica,
                                            },
                                            params,
                                        );
                                        (replica, params)
                                    })
                                    .collect();
                            (cluster, replicas)
                        })
                        .collect();
                }
                _ => {
                    file.environment.insert(key, value);
                }
            }
        }

        Some(file)
    }
}

/// The frontend's cached view of the config-sync file.
///
/// Caching keeps the file off the coordinator loop: create-time scoped resolution
/// runs the scoped passes inline on the loop that serializes all DDL and query
/// sequencing, where a synchronous read has no business. That path is
/// best-effort and re-reconciled by the next sync tick, so a value up to one tick
/// old is fine there.
#[derive(Debug)]
struct CachedConfigFile {
    /// The contents the cache was built from, or `None` if that read failed.
    ///
    /// Compared against the next read so that re-parsing, and every warning the
    /// file provokes, happen once per change rather than once per tick.
    contents: Option<String>,
    /// The parse of [`Self::contents`], or `None` if the read failed or the
    /// document was not a JSON object.
    file: Option<Arc<ConfigFile>>,
}

/// A position in the config-sync file, naming it in a warning about a value of
/// the wrong shape there.
enum FilePosition<'a> {
    /// A reserved top-level section.
    Section(&'a str),
    /// One cluster's entry in the `clusters` section.
    Cluster(&'a str),
    /// One cluster's entry in the `replicas` section, holding its replicas.
    ClusterReplicas(&'a str),
    /// One replica's entry within a cluster's entry in the `replicas` section.
    Replica { cluster: &'a str, replica: &'a str },
}

impl fmt::Display for FilePosition<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilePosition::Section(name) => write!(f, "the {name} section"),
            FilePosition::Cluster(cluster) => write!(f, "cluster {cluster:?}"),
            FilePosition::ClusterReplicas(cluster) => {
                write!(f, "the {REPLICAS_SECTION} entry for cluster {cluster:?}")
            }
            FilePosition::Replica { cluster, replica } => {
                write!(f, "replica {cluster:?}.{replica:?}")
            }
        }
    }
}

/// Interprets `value` as a JSON object, or warns and yields an empty map.
fn as_object(position: FilePosition<'_>, value: JsonValue) -> BTreeMap<String, JsonValue> {
    match value {
        JsonValue::Object(map) => map.into_iter().collect(),
        other => {
            warn!(
                "ignoring {position} in system parameter sync file: expected a JSON object, found {}",
                json_type_name(&other)
            );
            BTreeMap::new()
        }
    }
}

fn json_type_name(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(_) => "boolean",
        JsonValue::Number(_) => "number",
        JsonValue::String(_) => "string",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

/// Renders a JSON value as the raw parameter string the backend parses.
///
/// `null` yields `None`, meaning the file expresses no opinion for this
/// parameter, so its current value stands.
fn json_param_value(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(v) => Some(v.clone()),
        JsonValue::Number(v) => Some(v.to_string()),
        JsonValue::Bool(v) => Some(v.to_string()),
        JsonValue::Object(_) | JsonValue::Array(_) => Some(value.to_string()),
        JsonValue::Null => None,
    }
}

/// The verdict on a value a scoped source served for a parameter.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScopedValue {
    /// Parses, and differs from the environment-wide value, so it is recorded as
    /// an override.
    Override,
    /// Parses, but matches the environment-wide value, so there is no override
    /// to record.
    MatchesEnvironment,
    /// Does not parse for the parameter's type, so it is dropped.
    Unparseable,
}

/// Classifies `value` as the scoped value of `param_name` against the
/// environment-wide `base`, the var-formatted value held in `params`.
///
/// Recording is keyed on *differing* from the environment-wide value. For
/// LaunchDarkly the `variation_detail` reason is the wrong signal: it cannot say
/// which context kind's clause matched (an env-level rule and a cluster-specific
/// rule both report `RuleMatch`), and `Fallthrough` serves the env-wide value to
/// every object. Comparing against the env-wide baseline is the only signal that
/// means "this scope changed the answer", which is what must beat a manual
/// `FEATURES` pin and what keeps the durable collections sparse. See the scoped
/// feature flags design, §Resolution.
///
/// The comparison runs in the parameter's canonical encoding. `base` is
/// var-formatted (a `bool` is `"on"`/`"off"`), whereas a raw source value spells
/// a boolean `"true"`/`"false"`, so a direct string compare would treat every
/// boolean parameter as differing, even when the source served the env-wide
/// value. Callers still *store* the raw value, since downstream consumers parse
/// `"true"`/`"false"`. Only the decision is canonical.
///
/// [`ScopedValue::Unparseable`] must never be recorded: a stored unparseable
/// value would poison resolution. The optimizer's `bool` decode, for one, panics
/// on every plan for a cluster-coherent override it cannot parse. It means "no
/// scoped opinion", falling back to the environment-wide value.
fn classify_scoped_value(
    params: &SynchronizedParameters,
    param_name: &str,
    base: &str,
    value: &str,
) -> ScopedValue {
    match params.canonicalize(param_name, value) {
        Some(canonical) if canonical != base => ScopedValue::Override,
        Some(_) => ScopedValue::MatchesEnvironment,
        None => ScopedValue::Unparseable,
    }
}

impl SystemParameterFrontend {
    /// Create a new [SystemParameterFrontend] initialize.
    ///
    /// This will create and initialize an [ld::Client] instance. The
    /// [ld::Client::wait_for_initialization] call will be attempted in a loop with an
    /// exponential backoff with power `2s` and max duration `60s`.
    pub async fn from(sync_config: &SystemParameterSyncConfig) -> Result<Self, anyhow::Error> {
        match &sync_config.backend_config {
            super::SystemParameterSyncClientConfig::File { path } => Ok(Self {
                client: SystemParameterFrontendClient::File { path: path.clone() },
                key_map: sync_config.key_map.clone(),
                env_id: sync_config.env_id.clone(),
                build_info: sync_config.build_info,
                metrics: sync_config.metrics.clone(),
                config_file: Mutex::new(None),
            }),
            SystemParameterSyncClientConfig::LaunchDarkly {
                sdk_key,
                base_uri,
                now_fn,
            } => Ok(Self {
                client: SystemParameterFrontendClient::LaunchDarkly {
                    client: ld_client(sdk_key, base_uri.as_deref(), &sync_config.metrics, now_fn)
                        .await?,
                    // The environment-wide context carries no cluster/replica
                    // scope. Scoped evaluation passes a `cluster` or `replica`
                    // context per pass via [`ld_ctx`].
                    ctx: ld_ctx(&sync_config.env_id, sync_config.build_info, None, None)?,
                },
                env_id: sync_config.env_id.clone(),
                build_info: sync_config.build_info,
                metrics: sync_config.metrics.clone(),
                key_map: sync_config.key_map.clone(),
                config_file: Mutex::new(None),
            }),
        }
    }

    /// Pull the current values for all [SynchronizedParameters] from the
    /// [SystemParameterFrontend] and return `true` iff at least one parameter
    /// value was modified.
    pub fn pull(&self, params: &mut SynchronizedParameters) -> bool {
        // The file is read exactly once per tick, here, and the scoped passes read
        // the cache this refreshes. Reading it per parameter would let a rewrite
        // land mid-loop and be observed as a torn read, and reading it in the
        // scoped passes would put a synchronous read on the coordinator loop,
        // which resolves a new object's overrides at create time.
        let file = match &self.client {
            SystemParameterFrontendClient::File { path } => {
                self.refresh_config_file(path, fs::read_to_string(path), params)
            }
            SystemParameterFrontendClient::LaunchDarkly { .. } => None,
        };

        let mut changed = false;
        for param_name in params.synchronized().into_iter() {
            let flag_name = self.external_name(param_name);

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
                // A parameter the file does not mention, and every parameter when
                // the file could not be read or parsed, keeps its current value.
                SystemParameterFrontendClient::File { .. } => file
                    .as_ref()
                    .and_then(|file| file.environment.get(flag_name))
                    .and_then(json_param_value)
                    .unwrap_or_else(|| params.get(param_name)),
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

    /// The name this parameter is keyed by in the backing source: its key-map
    /// entry when it has one (a LaunchDarkly flag key), otherwise the parameter
    /// name itself.
    fn external_name<'a>(&'a self, param_name: &'a str) -> &'a str {
        self.key_map
            .get(param_name)
            .map_or(param_name, String::as_str)
    }

    /// Refreshes the cached config-sync file from `read`, the outcome of reading
    /// it at `path`, and returns the new parse.
    ///
    /// Everything the file is diagnosed for, both the shape warnings the parse
    /// emits and the scoped-section diagnostics, is reported here and only when
    /// the contents changed. A standing mistake in the file would otherwise be
    /// logged on every tick, and the sync loop ticks once a second.
    fn refresh_config_file(
        &self,
        path: &Path,
        read: io::Result<String>,
        params: &SynchronizedParameters,
    ) -> Option<Arc<ConfigFile>> {
        let mut cache = self
            .config_file
            .lock()
            .expect("config file cache lock poisoned");

        if let Some(cached) = &*cache {
            if cached.contents.as_deref() == read.as_deref().ok() {
                return cached.file.clone();
            }
        }

        let contents = match read {
            Ok(contents) => Some(contents),
            Err(e) => {
                warn!(
                    "could not read system parameter sync file {}: {e}",
                    path.display()
                );
                None
            }
        };
        let file = contents
            .as_deref()
            .and_then(ConfigFile::parse)
            .map(Arc::new);
        if let Some(file) = &file {
            for diagnostic in self.scoped_section_diagnostics(file, params) {
                warn!("{diagnostic}");
            }
        }
        *cache = Some(CachedConfigFile {
            contents,
            file: file.clone(),
        });

        file
    }

    /// The config-sync file as of the last [`Self::pull`], or `None` if no read
    /// of it has succeeded.
    fn cached_config_file(&self) -> Option<Arc<ConfigFile>> {
        self.config_file
            .lock()
            .expect("config file cache lock poisoned")
            .as_ref()
            .and_then(|cached| cached.file.clone())
    }

    /// Whether the frontend knows the desired state of the scoped parameters.
    ///
    /// `false` when the config-sync file has never been read and parsed
    /// successfully, which includes a file that is missing (the ConfigMap volume
    /// is mounted optional, so it can disappear), unreadable, or not a JSON
    /// object.
    ///
    /// The scoped desired state is complete: the reconcile prunes every override
    /// absent from it. A caller must therefore skip the reconcile while this is
    /// `false` rather than treat "no information" as "no overrides", which would
    /// durably drop every scoped override on a typo and restore it once the file
    /// is fixed. Always `true` for LaunchDarkly, whose evaluation falls back to
    /// the environment-wide value when it has nothing to say.
    pub fn has_scoped_desired_state(&self) -> bool {
        match &self.client {
            SystemParameterFrontendClient::LaunchDarkly { .. } => true,
            SystemParameterFrontendClient::File { .. } => self.cached_config_file().is_some(),
        }
    }

    /// The problems with `file`'s scoped sections that an operator can act on: a
    /// key that is not a parameter scopable at that position, and a value that
    /// does not parse for its parameter's type. Resolution drops both silently,
    /// and nothing surfaces a parameter's scope from SQL, so without this an
    /// operator has nothing to debug against.
    ///
    /// Returned rather than logged so that [`Self::refresh_config_file`] can log
    /// them only when the file changes.
    fn scoped_section_diagnostics(
        &self,
        file: &ConfigFile,
        params: &SynchronizedParameters,
    ) -> Vec<String> {
        let mut diagnostics = Vec::new();

        let cluster_params = self.scopable_params(params, ParameterScope::Cluster);
        for (cluster, section) in &file.clusters {
            section_diagnostics(
                section,
                params,
                &cluster_params,
                "cluster-scoped",
                FilePosition::Cluster(cluster),
                &mut diagnostics,
            );
        }

        let replica_params = self.scopable_params(params, ParameterScope::Replica);
        for (cluster, replicas) in &file.replicas {
            for (replica, section) in replicas {
                section_diagnostics(
                    section,
                    params,
                    &replica_params,
                    "replica-scoped",
                    FilePosition::Replica { cluster, replica },
                    &mut diagnostics,
                );
            }
        }

        diagnostics
    }

    /// The synced parameters that declare `scope`, keyed by the name a config-sync
    /// file section spells them with.
    fn scopable_params<'a>(
        &'a self,
        params: &SynchronizedParameters,
        scope: ParameterScope,
    ) -> BTreeMap<&'a str, &'static str> {
        params
            .synchronized_with_scope(scope)
            .into_iter()
            .map(|param_name| (self.external_name(param_name), param_name))
            .collect()
    }

    /// Evaluates the replica-local scoped parameters for each given replica and
    /// returns, per replica, the parameter values that differ from the
    /// environment-wide value held in `params`.
    ///
    /// The returned map is sparse: replicas with no overriding value are
    /// omitted. Replicas absent from `replicas` are never evaluated, so a name
    /// the config-sync file mentions that is not live is ignored.
    pub fn pull_replica_overrides(
        &self,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        replicas: &[ReplicaEvalContext],
    ) -> BTreeMap<ReplicaId, BTreeMap<String, String>> {
        let mut out: BTreeMap<ReplicaId, BTreeMap<String, String>> = BTreeMap::new();

        if param_names.is_empty() {
            return out;
        }

        let client = match &self.client {
            SystemParameterFrontendClient::LaunchDarkly { client, .. } => client,
            // Resolved from the cache `pull` refreshes, so this does no I/O: the
            // create path calls it on the coordinator loop. An empty result here
            // means "no overrides", so a caller reconciling the full desired state
            // must first check `has_scoped_desired_state`.
            SystemParameterFrontendClient::File { .. } => {
                let Some(file) = self.cached_config_file() else {
                    return out;
                };
                return self.file_replica_overrides(&file, params, param_names, replicas);
            }
        };

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
    /// environment-wide value held in `params`. Resolved replica-free, so the
    /// value cannot vary by replica.
    ///
    /// The returned map is sparse: clusters with no overriding value are
    /// omitted. Clusters absent from `clusters` are never evaluated, so a name
    /// the config-sync file mentions that is not live is ignored.
    pub fn pull_cluster_overrides(
        &self,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        clusters: &[ClusterEvalContext],
    ) -> BTreeMap<ClusterId, BTreeMap<String, String>> {
        let mut out: BTreeMap<ClusterId, BTreeMap<String, String>> = BTreeMap::new();

        if param_names.is_empty() {
            return out;
        }

        let client = match &self.client {
            SystemParameterFrontendClient::LaunchDarkly { client, .. } => client,
            // See the file arm of `pull_replica_overrides`.
            SystemParameterFrontendClient::File { .. } => {
                let Some(file) = self.cached_config_file() else {
                    return out;
                };
                return self.file_cluster_overrides(&file, params, param_names, clusters);
            }
        };

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
    /// See [`classify_scoped_value`] for why recording keys on the
    /// differs-from-environment test rather than the `variation_detail` reason.
    fn evaluate_scoped_overrides(
        &self,
        client: &ld::Client,
        ctx: &ld::Context,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
    ) -> BTreeMap<String, String> {
        let mut overrides = BTreeMap::new();
        for &param_name in param_names {
            let base = params.get(param_name);
            // Evaluate with `base` as the default, so a silent LD (flag absent,
            // off, error, failed prerequisite) resolves back to the env-wide
            // value and is dropped by the difference test below.
            let flag_var = client.variation(ctx, self.external_name(param_name), base.clone());
            let value = match flag_var {
                ld::FlagValue::Bool(v) => v.to_string(),
                ld::FlagValue::Str(v) => v,
                ld::FlagValue::Number(v) => v.to_string(),
                ld::FlagValue::Json(v) => v.to_string(),
            };

            // An unparseable value is dropped silently: LaunchDarkly targeting
            // is not authored per environment, so a warning here would repeat
            // every tick for something the environment's operator cannot fix.
            if classify_scoped_value(params, param_name, &base, &value) == ScopedValue::Override {
                overrides.insert(param_name.to_string(), value);
            }
        }
        overrides
    }

    /// Resolves the cluster-coherent overrides `file` declares for each of
    /// `clusters`, matching a cluster to its file section by name.
    ///
    /// The live clusters drive the lookup, so a section naming a cluster that
    /// does not exist is simply never consulted.
    fn file_cluster_overrides(
        &self,
        file: &ConfigFile,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        clusters: &[ClusterEvalContext],
    ) -> BTreeMap<ClusterId, BTreeMap<String, String>> {
        let mut out = BTreeMap::new();
        for cluster in clusters {
            let name = &cluster.cluster.name;
            let Some(section) = file.clusters.get(name) else {
                continue;
            };
            let overrides = self.file_section_overrides(section, params, param_names);
            if !overrides.is_empty() {
                out.insert(cluster.cluster_id, overrides);
            }
        }
        out
    }

    /// Resolves the replica-local overrides `file` declares for each of
    /// `replicas`, matching a replica to its file section by owning cluster name
    /// then replica name.
    ///
    /// The live replicas drive the lookup, so a section naming a cluster or
    /// replica that does not exist is simply never consulted.
    fn file_replica_overrides(
        &self,
        file: &ConfigFile,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        replicas: &[ReplicaEvalContext],
    ) -> BTreeMap<ReplicaId, BTreeMap<String, String>> {
        let mut out = BTreeMap::new();
        for replica in replicas {
            let (cluster_name, replica_name) =
                (&replica.replica.cluster_name, &replica.replica.name);
            let Some(section) = file
                .replicas
                .get(cluster_name)
                .and_then(|cluster| cluster.get(replica_name))
            else {
                continue;
            };
            let overrides = self.file_section_overrides(section, params, param_names);
            if !overrides.is_empty() {
                out.insert(replica.replica_id, overrides);
            }
        }
        out
    }

    /// Resolves one object's config-sync file section into its scoped overrides,
    /// applying the same parseability and differs-from-environment rules as the
    /// LaunchDarkly path.
    ///
    /// A parameter the section omits carries no scoped opinion, so it is absent
    /// from the result and resolves to the environment-wide value. Silent, as this
    /// runs on every tick and for every create: the operator-facing diagnostics
    /// are [`Self::scoped_section_diagnostics`], reported once per change to the
    /// file.
    fn file_section_overrides(
        &self,
        section: &BTreeMap<String, JsonValue>,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
    ) -> BTreeMap<String, String> {
        let mut overrides = BTreeMap::new();
        for &param_name in param_names {
            let Some(value) = section
                .get(self.external_name(param_name))
                .and_then(json_param_value)
            else {
                continue;
            };

            let base = params.get(param_name);
            if classify_scoped_value(params, param_name, &base, &value) == ScopedValue::Override {
                overrides.insert(param_name.to_string(), value);
            }
        }
        overrides
    }
}

/// Appends the diagnostics for one object's config-sync file section to
/// `diagnostics`.
///
/// `scopable` holds the parameters that accept a value at this position, keyed by
/// the name the file spells them with, and `scope` names that scope for the
/// message. A key outside `scopable` is a parameter of another scope, or a
/// misspelling: either way resolution never consults it.
fn section_diagnostics(
    section: &BTreeMap<String, JsonValue>,
    params: &SynchronizedParameters,
    scopable: &BTreeMap<&str, &'static str>,
    scope: &str,
    position: FilePosition<'_>,
    diagnostics: &mut Vec<String>,
) {
    for (name, value) in section {
        let Some(&param_name) = scopable.get(name.as_str()) else {
            diagnostics.push(format!(
                "ignoring {name} for {position} in the system parameter sync file: \
                 not a {scope} system parameter"
            ));
            continue;
        };
        // `null` expresses no opinion rather than a value, so there is nothing to
        // parse.
        let Some(value) = json_param_value(value) else {
            continue;
        };
        let base = params.get(param_name);
        if classify_scoped_value(params, param_name, &base, &value) == ScopedValue::Unparseable {
            diagnostics.push(format!(
                "ignoring unparseable value {value:?} for system parameter {param_name} \
                 on {position} in the system parameter sync file"
            ));
        }
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

/// The identity of a single live cluster, used to evaluate cluster-coherent
/// scoped parameters in [`SystemParameterFrontend::pull_cluster_overrides`].
#[derive(Clone, Debug)]
pub struct ClusterEvalContext {
    /// The cluster's id.
    pub cluster_id: ClusterId,
    /// The cluster's scope context (replica-free).
    pub cluster: ClusterScopeContext,
}

/// An [`HttpTransport`] wrapper that records timestamps on successful HTTP
/// responses. Used to populate Prometheus metrics that track LaunchDarkly
/// connectivity health.
///
/// Two instances are created — one for the event processor (CSE metric, tracks
/// outbound event sends) and one for the streaming data source (SSE metric,
/// tracks inbound SSE events).
#[derive(Clone)]
struct MetricsTransport<T> {
    inner: T,
    last_success_gauge: UIntGauge,
    now_fn: NowFn,
}

impl<T: HttpTransport> HttpTransport for MetricsTransport<T> {
    fn request(&self, request: http::Request<Option<Bytes>>) -> ResponseFuture {
        let inner_fut = self.inner.request(request);
        let gauge = self.last_success_gauge.clone();
        let now_fn = self.now_fn.clone();
        Box::pin(async move {
            let resp = inner_fut.await?;
            if resp.status().is_success() {
                gauge.set(now_fn() / 1000);
                let (parts, body) = resp.into_parts();
                let wrapped: ByteStream = Box::pin(body.inspect_ok(move |_| {
                    gauge.set(now_fn() / 1000);
                }));
                Ok(http::Response::from_parts(parts, wrapped))
            } else {
                Ok(resp)
            }
        })
    }
}

fn ld_config(
    api_key: &str,
    base_uri: Option<&str>,
    metrics: &Metrics,
    now_fn: &NowFn,
) -> ld::Config {
    // How long a body read on the streaming connection may stay idle before
    // the transport surfaces a timeout error to the data source. This is the
    // error class of incident-984 (a silently-dead connection). Overridable
    // via a hidden env var so tests can trigger the timeout path in seconds
    // instead of minutes (see test/launchdarkly-reconnect).
    //
    // The default must stay above LaunchDarkly's streaming heartbeat interval
    // (roughly 3 minutes per LD's documentation), or a healthy idle stream
    // would trip the timeout and reconnect spuriously. Benign now that
    // reconnects work, but wasteful. The same constant lives in
    // `mz-dyncfg-launchdarkly`.
    let read_timeout = match std::env::var("MZ_LAUNCHDARKLY_READ_TIMEOUT") {
        Ok(v) => humantime::parse_duration(&v).unwrap_or_else(|e| {
            // Don't silently fall back: a typo here (e.g. `5sec`) would
            // otherwise present as an unexplained timeout far downstream.
            tracing::error!(
                "ignoring unparseable MZ_LAUNCHDARKLY_READ_TIMEOUT {v:?}: {e}; \
                 falling back to default"
            );
            Duration::from_secs(300)
        }),
        Err(_) => Duration::from_secs(300),
    };

    // NOTE: `HyperTransport` auto-detects the `HTTP_PROXY`/`HTTPS_PROXY`/
    // `NO_PROXY` env vars and routes through a configured proxy. No exposure
    // today (our cloud pods set no proxy vars, self-managed never builds an LD
    // client), but worth knowing if proxy vars ever appear on a pod.
    let transport = launchdarkly_sdk_transport::HyperTransport::builder()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(read_timeout)
        .build_https()
        .expect("failed to create HTTPS transport");

    let cse_transport = MetricsTransport {
        inner: transport.clone(),
        last_success_gauge: metrics.last_cse_time_seconds.clone(),
        now_fn: now_fn.clone(),
    };
    let data_source_transport = MetricsTransport {
        inner: transport,
        last_success_gauge: metrics.last_sse_time_seconds.clone(),
        now_fn: now_fn.clone(),
    };

    let mut event_processor = ld::EventProcessorBuilder::new();
    event_processor.transport(cse_transport);

    let mut data_source = ld::StreamingDataSourceBuilder::new();
    data_source.transport(data_source_transport);

    let mut config = ld::ConfigBuilder::new(api_key)
        .event_processor(&event_processor)
        .data_source(&data_source);
    if let Some(base_uri) = base_uri {
        let mut endpoints = ld::ServiceEndpointsBuilder::new();
        endpoints.relay_proxy(base_uri);
        config = config.service_endpoints(&endpoints);
    }
    config.build().expect("valid config")
}

async fn ld_client(
    api_key: &str,
    base_uri: Option<&str>,
    metrics: &Metrics,
    now_fn: &NowFn,
) -> Result<ld::Client, anyhow::Error> {
    let ld_client = ld::Client::build(ld_config(api_key, base_uri, metrics, now_fn))?;
    tracing::info!("waiting for SystemParameterFrontend to initialize");
    ld_client.start_with_default_executor();

    let max_backoff = Duration::from_secs(60);
    let mut backoff = Duration::from_secs(5);
    let timeout = Duration::from_secs(10);

    // TODO(materialize#38055): fix retry logic
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
    use std::sync::atomic::{AtomicU64, Ordering};

    use futures::StreamExt;
    use launchdarkly_sdk_transport::{ByteStream, TransportError};
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::metrics::MetricsRegistry;

    use super::*;

    fn env_id() -> EnvironmentId {
        EnvironmentId::for_tests()
    }

    /// A cluster-coherent `bool` parameter whose environment-wide default is
    /// `off`.
    const CLUSTER_PARAM: &str = "enable_eager_delta_joins";
    /// A replica-local `bool` parameter whose environment-wide default is `on`.
    const REPLICA_PARAM: &str = "enable_lgalloc";

    /// A file-backed frontend. The tests below drive it from contents they pass
    /// in, so its path is never read.
    fn file_frontend() -> SystemParameterFrontend {
        SystemParameterFrontend {
            client: SystemParameterFrontendClient::File {
                path: CONFIG_PATH.into(),
            },
            key_map: BTreeMap::new(),
            env_id: env_id(),
            build_info: &DUMMY_BUILD_INFO,
            metrics: Metrics::register_into(&MetricsRegistry::new()),
            config_file: Mutex::new(None),
        }
    }

    /// The path a [`file_frontend`] reports in its warnings. Never opened.
    const CONFIG_PATH: &str = "/nonexistent/system-params.json";

    /// Parses a document the test knows to be a JSON object.
    fn parse(contents: &str) -> ConfigFile {
        ConfigFile::parse(contents).expect("document is a JSON object")
    }

    fn cluster_ctx(id: u64, name: &str) -> ClusterEvalContext {
        ClusterEvalContext {
            cluster_id: ClusterId::User(id),
            cluster: ClusterScopeContext {
                id: format!("u{id}"),
                name: name.into(),
                is_builtin: false,
            },
        }
    }

    fn replica_ctx(
        cluster_id: u64,
        cluster_name: &str,
        replica_id: u64,
        replica_name: &str,
    ) -> ReplicaEvalContext {
        ReplicaEvalContext {
            cluster_id: ClusterId::User(cluster_id),
            replica_id: ReplicaId::User(replica_id),
            cluster: ClusterScopeContext {
                id: format!("u{cluster_id}"),
                name: cluster_name.into(),
                is_builtin: false,
            },
            replica: ReplicaScopeContext {
                id: format!("u{replica_id}"),
                name: replica_name.into(),
                is_builtin: false,
                size: "D.1-xsmall".into(),
                size_family: "D".into(),
                cluster_id: format!("u{cluster_id}"),
                cluster_name: cluster_name.into(),
            },
        }
    }

    fn overrides(param_name: &str, value: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(param_name.to_string(), value.to_string())])
    }

    /// A file with no reserved section is a plain environment-wide parameter
    /// map, the flat form a config map without scoped sections takes.
    #[mz_ore::test]
    fn test_parse_flat_file_is_environment_wide() {
        let file = parse(
            r#"{
                "max_connections": 1000,
                "allowed_cluster_replica_sizes": "'25cc', '50cc'",
                "enable_lgalloc": false
            }"#,
        );

        assert_eq!(
            file.environment.keys().collect::<Vec<_>>(),
            vec![
                "allowed_cluster_replica_sizes",
                "enable_lgalloc",
                "max_connections"
            ]
        );
        assert!(file.clusters.is_empty());
        assert!(file.replicas.is_empty());

        // Every JSON scalar renders to the raw string the backend parses.
        assert_eq!(
            json_param_value(&file.environment["max_connections"]).as_deref(),
            Some("1000")
        );
        assert_eq!(
            json_param_value(&file.environment["enable_lgalloc"]).as_deref(),
            Some("false")
        );
        // An explicit `null` expresses no opinion, leaving the value alone.
        let null = parse(r#"{"max_connections": null}"#);
        assert_eq!(json_param_value(&null.environment["max_connections"]), None);
    }

    #[mz_ore::test]
    fn test_parse_scoped_sections() {
        let file = parse(
            r#"{
                "enable_lgalloc": false,
                "clusters": {"prod": {"enable_eager_delta_joins": true}},
                "replicas": {"prod": {"r1": {"enable_lgalloc": true}}}
            }"#,
        );

        // The reserved keys are sections, every other key is environment-wide.
        assert_eq!(
            file.environment.keys().collect::<Vec<_>>(),
            vec!["enable_lgalloc"]
        );
        assert_eq!(
            file.clusters["prod"][CLUSTER_PARAM],
            JsonValue::Bool(true),
            "cluster section parsed"
        );
        assert_eq!(
            file.replicas["prod"]["r1"][REPLICA_PARAM],
            JsonValue::Bool(true),
            "replica section parsed, nested cluster then replica"
        );
    }

    /// One malformed section, or one malformed entry inside a section, must not
    /// strand the rest of the file.
    #[mz_ore::test]
    fn test_parse_ignores_malformed_sections() {
        let file = parse(
            r#"{
                "max_connections": 1000,
                "clusters": 7,
                "replicas": {"prod": {"r1": "not-an-object"}}
            }"#,
        );

        assert_eq!(
            file.environment.keys().collect::<Vec<_>>(),
            vec!["max_connections"]
        );
        assert!(file.clusters.is_empty(), "non-object section dropped");
        assert!(
            file.replicas["prod"]["r1"].is_empty(),
            "non-object entry dropped, its position retained"
        );
    }

    /// A document that is not a JSON object at all carries no information, which
    /// is distinct from an empty document. See
    /// `test_read_failure_keeps_scoped_overrides`.
    #[mz_ore::test]
    fn test_parse_rejects_non_object_document() {
        assert_eq!(ConfigFile::parse("[]"), None);
        assert_eq!(ConfigFile::parse("not json"), None);
        assert_eq!(ConfigFile::parse("{}"), Some(ConfigFile::default()));
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_cluster_section_applied() {
        let params = SynchronizedParameters::default();
        let file = parse(r#"{"clusters": {"prod": {"enable_eager_delta_joins": true}}}"#);

        let out = file_frontend().file_cluster_overrides(
            &file,
            &params,
            &[CLUSTER_PARAM],
            &[cluster_ctx(1, "prod"), cluster_ctx(2, "staging")],
        );

        // Sparse: only the named cluster gets a row.
        assert_eq!(
            out,
            BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "true"))])
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_replica_section_applied() {
        let params = SynchronizedParameters::default();
        let file = parse(r#"{"replicas": {"prod": {"r1": {"enable_lgalloc": false}}}}"#);

        let out = file_frontend().file_replica_overrides(
            &file,
            &params,
            &[REPLICA_PARAM],
            &[
                replica_ctx(1, "prod", 1, "r1"),
                // Same cluster, different replica name.
                replica_ctx(1, "prod", 2, "r2"),
                // Same replica name in a different cluster: a replica name is
                // unique only within its cluster, so the nesting must keep the
                // two apart.
                replica_ctx(2, "staging", 3, "r1"),
            ],
        );

        assert_eq!(
            out,
            BTreeMap::from([(ReplicaId::User(1), overrides(REPLICA_PARAM, "false"))])
        );
    }

    /// A section naming an object that is not live is ignored, not an error: the
    /// live objects drive the lookup, so the file is never a second source of
    /// truth for what exists.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_unknown_object_names_ignored() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let file = parse(
            r#"{
                "clusters": {"gone": {"enable_eager_delta_joins": true}},
                "replicas": {"gone": {"r1": {"enable_lgalloc": false}}}
            }"#,
        );

        assert!(
            frontend
                .file_cluster_overrides(&file, &params, &[CLUSTER_PARAM], &[cluster_ctx(1, "prod")])
                .is_empty()
        );
        assert!(
            frontend
                .file_replica_overrides(
                    &file,
                    &params,
                    &[REPLICA_PARAM],
                    &[replica_ctx(1, "prod", 1, "r1")]
                )
                .is_empty()
        );
    }

    /// An unparseable scoped value is dropped rather than rejected or stored.
    /// Storing it would poison resolution: the optimizer's `bool` decode panics
    /// on every plan for a cluster-coherent override it cannot parse.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_unparseable_value_dropped() {
        let params = SynchronizedParameters::default();
        let file = parse(
            r#"{"clusters": {"prod": {
                "enable_eager_delta_joins": "maybe"
            }}}"#,
        );

        assert!(
            file_frontend()
                .file_cluster_overrides(&file, &params, &[CLUSTER_PARAM], &[cluster_ctx(1, "prod")])
                .is_empty()
        );
    }

    /// A scoped value that agrees with the environment-wide value records no
    /// override, keeping the durable collections sparse. The comparison is in
    /// the parameter's canonical encoding, so the file's `false` matches the
    /// var-formatted `off`.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_value_matching_environment_dropped() {
        let params = SynchronizedParameters::default();
        assert_eq!(params.get(CLUSTER_PARAM), "off");
        let file = parse(r#"{"clusters": {"prod": {"enable_eager_delta_joins": false}}}"#);

        assert!(
            file_frontend()
                .file_cluster_overrides(&file, &params, &[CLUSTER_PARAM], &[cluster_ctx(1, "prod")])
                .is_empty()
        );
    }

    /// A whole-document failure, an unreadable file or a document that is not a
    /// JSON object, must express "no information", not "no overrides": the scoped
    /// desired state is complete, so treating a failure as an empty state would
    /// durably prune every scoped override and restore it once the file is fixed.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_read_failure_keeps_scoped_overrides() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let clusters = [cluster_ctx(1, "prod")];
        let scoped_file = r#"{"clusters": {"prod": {"enable_eager_delta_joins": true}}}"#;

        // A readable file establishes the override.
        frontend.refresh_config_file(Path::new(CONFIG_PATH), Ok(scoped_file.to_string()), &params);
        assert!(frontend.has_scoped_desired_state());
        assert_eq!(
            frontend.pull_cluster_overrides(&params, &[CLUSTER_PARAM], &clusters),
            BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "true"))])
        );

        for failure in [
            Err(io::Error::from(io::ErrorKind::NotFound)),
            Ok("}not json{".to_string()),
        ] {
            frontend.refresh_config_file(Path::new(CONFIG_PATH), failure, &params);
            // The reconcile is skipped wholesale on this signal, which is what
            // leaves the existing overrides in place. The empty resolution below
            // would prune them if it were reconciled.
            assert!(!frontend.has_scoped_desired_state());
            assert!(
                frontend
                    .pull_cluster_overrides(&params, &[CLUSTER_PARAM], &clusters)
                    .is_empty()
            );

            // A readable file again resolves as before.
            frontend.refresh_config_file(
                Path::new(CONFIG_PATH),
                Ok(scoped_file.to_string()),
                &params,
            );
            assert!(frontend.has_scoped_desired_state());
        }

        // An empty document, on the other hand, is a complete desired state of
        // "no overrides", so it does prune.
        frontend.refresh_config_file(Path::new(CONFIG_PATH), Ok("{}".to_string()), &params);
        assert!(frontend.has_scoped_desired_state());
        assert!(
            frontend
                .pull_cluster_overrides(&params, &[CLUSTER_PARAM], &clusters)
                .is_empty()
        );
    }

    /// Unchanged contents reuse the cached parse, so the scoped passes do no I/O
    /// and nothing the file is diagnosed for is reported twice.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_config_file_cached_until_it_changes() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let read = |contents: &str| {
            frontend
                .refresh_config_file(Path::new(CONFIG_PATH), Ok(contents.to_string()), &params)
                .expect("document is a JSON object")
        };

        let first = read(r#"{"clusters": {"prod": {"enable_eager_delta_joins": true}}}"#);
        let again = read(r#"{"clusters": {"prod": {"enable_eager_delta_joins": true}}}"#);
        assert!(Arc::ptr_eq(&first, &again), "unchanged file was re-parsed");

        let changed = read(r#"{"clusters": {"prod": {"enable_eager_delta_joins": false}}}"#);
        assert!(
            !Arc::ptr_eq(&first, &changed),
            "changed file was not parsed"
        );
    }

    /// A section key that is not a parameter scopable there is dropped by
    /// resolution, so it is diagnosed instead. Nothing surfaces a parameter's
    /// scope from SQL, which leaves an operator nothing else to debug against.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_diagnoses_unscopable_section_keys() {
        let params = SynchronizedParameters::default();
        let file = parse(&format!(
            r#"{{
                "clusters": {{"prod": {{
                    "{REPLICA_PARAM}": false,
                    "enabel_eager_delta_joins": true,
                    "{CLUSTER_PARAM}": "maybe"
                }}}},
                "replicas": {{"prod": {{"r1": {{"{CLUSTER_PARAM}": true}}}}}}
            }}"#
        ));

        assert_eq!(
            file_frontend().scoped_section_diagnostics(&file, &params),
            // Ordered by section, then by key within a section.
            vec![
                // A misspelled parameter name.
                "ignoring enabel_eager_delta_joins for cluster \"prod\" in the system \
                 parameter sync file: not a cluster-scoped system parameter"
                    .to_string(),
                // A value that does not parse for the parameter's type.
                format!(
                    "ignoring unparseable value \"maybe\" for system parameter {CLUSTER_PARAM} \
                     on cluster \"prod\" in the system parameter sync file"
                ),
                // A replica-scoped parameter in a `clusters` section.
                format!(
                    "ignoring {REPLICA_PARAM} for cluster \"prod\" in the system parameter \
                     sync file: not a cluster-scoped system parameter"
                ),
                // A cluster-scoped parameter in a `replicas` section.
                format!(
                    "ignoring {CLUSTER_PARAM} for replica \"prod\".\"r1\" in the system \
                     parameter sync file: not a replica-scoped system parameter"
                ),
            ]
        );
    }

    /// The reserved section names shadow any synced parameter of the same name,
    /// so no such parameter may exist. Renaming the parameter is the fix.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_no_synced_parameter_shadows_a_reserved_section() {
        let params = SynchronizedParameters::default();
        for section in [CLUSTERS_SECTION, REPLICAS_SECTION] {
            assert!(
                !params.is_synchronized(section),
                "synced system parameter {section:?} is shadowed by the config-sync \
                 file section of the same name; rename the parameter"
            );
        }
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

    /// A fake transport that simulates a long-lived SSE streaming connection:
    /// returns 200 OK immediately, then delivers multiple SSE events as body
    /// chunks (exactly how LaunchDarkly's streaming data source works).
    #[derive(Clone)]
    struct FakeSseTransport;

    impl HttpTransport for FakeSseTransport {
        fn request(&self, _request: http::Request<Option<Bytes>>) -> ResponseFuture {
            let body: ByteStream = Box::pin(futures::stream::iter(vec![
                Ok(Bytes::from("event: put\ndata: {\"flags\":{}}\n\n")),
                Ok(Bytes::from("event: patch\ndata: {\"key\":\"flag1\"}\n\n")),
                Ok(Bytes::from("event: patch\ndata: {\"key\":\"flag2\"}\n\n")),
            ]));
            Box::pin(async move {
                http::Response::builder()
                    .status(200)
                    .body(body)
                    .map_err(|e| TransportError::new(std::io::Error::other(e)))
            })
        }
    }

    /// A fake transport that returns an error, simulating a failed connection.
    #[derive(Clone)]
    struct FailingTransport;

    impl HttpTransport for FailingTransport {
        fn request(&self, _request: http::Request<Option<Bytes>>) -> ResponseFuture {
            Box::pin(async move {
                Err(TransportError::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "connection refused",
                )))
            })
        }
    }

    /// A fake transport that returns 200 OK, delivers one event, then errors
    /// mid-stream with a timeout: the non-Eof stream error a dropped long-lived
    /// SSE connection surfaces.
    #[derive(Clone)]
    struct MidStreamFailureTransport;

    impl HttpTransport for MidStreamFailureTransport {
        fn request(&self, _request: http::Request<Option<Bytes>>) -> ResponseFuture {
            let body: ByteStream = Box::pin(futures::stream::iter(vec![
                Ok(Bytes::from("event: put\ndata: {\"flags\":{}}\n\n")),
                Err(TransportError::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "body timed out",
                ))),
            ]));
            Box::pin(async move {
                http::Response::builder()
                    .status(200)
                    .body(body)
                    .map_err(|e| TransportError::new(std::io::Error::other(e)))
            })
        }
    }

    fn test_gauge(registry: &MetricsRegistry, name: &str) -> UIntGauge {
        registry.register(mz_ore::metric!(
            name: name,
            help: "test gauge",
        ))
    }

    /// Verifies that MetricsTransport updates the gauge on each body chunk,
    /// not just on the initial HTTP 200 response head. This matters for
    /// long-lived streaming connections where SSE events arrive as body chunks.
    #[mz_ore::test(tokio::test)]
    async fn test_metric_updated_on_body_chunks() -> Result<(), anyhow::Error> {
        let time = Arc::new(AtomicU64::new(1_000_000));
        let time_clone = Arc::clone(&time);
        let now_fn = NowFn::from(move || time_clone.load(Ordering::SeqCst));

        let registry = MetricsRegistry::new();
        let gauge = test_gauge(&registry, "test_sse_gauge");

        let transport = MetricsTransport {
            inner: FakeSseTransport,
            last_success_gauge: gauge.clone(),
            now_fn,
        };

        assert_eq!(gauge.get(), 0);

        let request = http::Request::builder()
            .uri("https://stream.launchdarkly.com/all")
            .body(None)?;
        let response = transport.request(request).await?;

        assert_eq!(gauge.get(), 1000);

        time.store(2_800_000, Ordering::SeqCst);

        let mut body = response.into_body();
        let mut event_count = 0;
        while let Some(Ok(_chunk)) = body.next().await {
            event_count += 1;
        }
        assert_eq!(event_count, 3);

        assert_eq!(gauge.get(), 2800);
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn test_cse_metric_updates_correctly_per_request() -> Result<(), anyhow::Error> {
        let time = Arc::new(AtomicU64::new(1_000_000));
        let time_clone = Arc::clone(&time);
        let now_fn = NowFn::from(move || time_clone.load(Ordering::SeqCst));

        let registry = MetricsRegistry::new();
        let gauge = test_gauge(&registry, "test_cse_gauge");

        let transport = MetricsTransport {
            inner: FakeSseTransport,
            last_success_gauge: gauge.clone(),
            now_fn,
        };

        let req = || -> Result<http::Request<Option<Bytes>>, http::Error> {
            http::Request::builder()
                .uri("https://events.launchdarkly.com/bulk")
                .body(None)
        };

        let _ = transport.request(req()?).await?;
        assert_eq!(gauge.get(), 1000);

        time.store(2_000_000, Ordering::SeqCst);
        let _ = transport.request(req()?).await?;
        assert_eq!(gauge.get(), 2000);

        time.store(3_000_000, Ordering::SeqCst);
        let _ = transport.request(req()?).await?;
        assert_eq!(gauge.get(), 3000);
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    async fn test_metric_not_updated_on_failed_request() -> Result<(), anyhow::Error> {
        let now_fn = NowFn::from(|| 5_000_000u64);

        let registry = MetricsRegistry::new();
        let gauge = test_gauge(&registry, "test_fail_gauge");

        let transport = MetricsTransport {
            inner: FailingTransport,
            last_success_gauge: gauge.clone(),
            now_fn,
        };

        let request = http::Request::builder()
            .uri("https://stream.launchdarkly.com/all")
            .body(None)?;
        let result = transport.request(request).await;
        assert!(result.is_err());
        assert_eq!(gauge.get(), 0, "gauge must not update on transport error");
        Ok(())
    }

    /// Verifies that when an SSE connection returns 200 OK and then dies
    /// mid-stream, `last_sse_time_seconds` advances only for the events that
    /// arrived and then freezes — the frozen timestamp is what lets the
    /// staleness alert detect a stuck data source.
    #[mz_ore::test(tokio::test)]
    async fn test_metric_frozen_on_midstream_error() -> Result<(), anyhow::Error> {
        let time = Arc::new(AtomicU64::new(1_000_000));
        let time_clone = Arc::clone(&time);
        let now_fn = NowFn::from(move || time_clone.load(Ordering::SeqCst));

        let registry = MetricsRegistry::new();
        let gauge = test_gauge(&registry, "test_midstream_gauge");

        let transport = MetricsTransport {
            inner: MidStreamFailureTransport,
            last_success_gauge: gauge.clone(),
            now_fn,
        };

        // The 200 OK response head updates the gauge.
        let request = http::Request::builder()
            .uri("https://stream.launchdarkly.com/all")
            .body(None)?;
        let response = transport.request(request).await?;
        assert_eq!(gauge.get(), 1000);

        // The first event arrives and advances the gauge.
        time.store(2_000_000, Ordering::SeqCst);
        let mut body = response.into_body();
        assert!(matches!(body.next().await, Some(Ok(_))));
        assert_eq!(gauge.get(), 2000);

        // The stream then errors mid-flight. Time has moved forward, but the
        // gauge must stay frozen at the last successful event.
        time.store(9_000_000, Ordering::SeqCst);
        assert!(matches!(body.next().await, Some(Err(_))));
        assert_eq!(
            gauge.get(),
            2000,
            "gauge must freeze on mid-stream error so the staleness alert can fire"
        );
        Ok(())
    }
}
