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
use regex::Regex;
use serde::Deserialize;
use serde::de::{DeserializeOwned, IgnoredAny};
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
        client: Arc<ld::Client>,
        /// The context to use when querying LaunchDarkly using the SDK.
        /// This scopes down queries to a specific key.
        ctx: ld::Context,
    },
}

impl SystemParameterFrontendClient {}

impl Drop for SystemParameterFrontendClient {
    fn drop(&mut self) {
        let client = match self {
            SystemParameterFrontendClient::LaunchDarkly { client, ctx: _ } => client,
            SystemParameterFrontendClient::File { path: _ } => return,
        };
        // Dropping an `ld::Client` without `close` disconnects its event
        // dispatcher thread, which logs an error and discards the analytics
        // events it has not sent yet. `close` blocks until the final flush
        // completes, so it must not run on an async worker thread.
        let client = Arc::clone(client);
        if tokio::runtime::Handle::try_current().is_ok() {
            mz_ore::task::spawn_blocking(
                || "SystemParameterFrontend close",
                move || client.close(),
            );
        } else {
            client.close();
        }
    }
}

/// The top-level key whose presence selects [`ConfigFileV2`] over
/// [`LegacyConfigFile`].
const VERSION_KEY: &str = "version";

/// The config-sync file in the format every consumer reads, which a document
/// opts into with `"version": 2`.
///
/// Deserialization is all-or-nothing: a structural defect anywhere fails the
/// whole document. What depends on this binary's parameter set rather than on
/// the document's structure stays a per-parameter diagnostic, see
/// [`SystemParameterFrontend::scoped_rule_diagnostics`].
#[derive(Debug, Default, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ConfigFileV2 {
    /// Checked by [`Self::parse`] ahead of the rest of the document.
    #[serde(rename = "version")]
    _version: IgnoredAny,
    /// Environment-wide values, keyed by the parameter's external name.
    #[serde(default)]
    global_parameters: BTreeMap<String, ParamValue>,
    /// The rules in file order. The first rule that matches an object decides
    /// each parameter it supplies.
    #[serde(default)]
    rules: Vec<Rule>,
}

/// The config-sync file in the format without a `version` key: a flat map of
/// environment-wide values, keyed by the parameter's external name.
#[derive(Deserialize)]
#[serde(transparent)]
struct LegacyConfigFile(BTreeMap<String, ParamValue>);

impl From<LegacyConfigFile> for ConfigFileV2 {
    fn from(legacy: LegacyConfigFile) -> Self {
        Self {
            _version: IgnoredAny,
            global_parameters: legacy.0,
            rules: Vec::new(),
        }
    }
}

impl ConfigFileV2 {
    /// Parses the config-sync file's top-level object in either format, or
    /// describes the first defect.
    ///
    /// An error is "no information about any parameter", which callers must keep
    /// distinct from a valid but empty document.
    fn parse(document: serde_json::Map<String, JsonValue>) -> Result<Self, String> {
        // The format is chosen by the key's presence. Trying version 2 and
        // falling back to the flat format would read a defective version 2
        // document as a valid flat one, which prunes every scoped override.
        match document.get(VERSION_KEY) {
            // Checked ahead of the rest, so a document of another version is
            // reported as that rather than as its first unknown key.
            Some(version) if version.as_u64() != Some(2) => Err(format!(
                "{VERSION_KEY}: unsupported version {version}, expected 2"
            )),
            Some(_) => deserialize_tracked(JsonValue::Object(document)),
            None => {
                deserialize_tracked::<LegacyConfigFile>(JsonValue::Object(document)).map(Self::from)
            }
        }
    }
}

/// Deserializes `value`, prefixing an error with the path to the element it is
/// about, such as `rules[0].clauses[1].op`.
fn deserialize_tracked<T: DeserializeOwned>(value: JsonValue) -> Result<T, String> {
    serde_path_to_error::deserialize(value).map_err(|e| {
        // An error about the document as a whole names its own position.
        if e.path().iter().next().is_none() {
            e.inner().to_string()
        } else {
            format!("{}: {}", e.path(), e.inner())
        }
    })
}

/// A parameter value as the raw string the backend parses, or `None` for a JSON
/// `null`, which expresses no opinion, so the parameter's current value stands.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(from = "JsonValue")]
struct ParamValue(Option<String>);

impl From<JsonValue> for ParamValue {
    fn from(value: JsonValue) -> Self {
        Self(match value {
            JsonValue::String(v) => Some(v),
            JsonValue::Number(v) => Some(v.to_string()),
            JsonValue::Bool(v) => Some(v.to_string()),
            JsonValue::Object(_) | JsonValue::Array(_) => Some(value.to_string()),
            JsonValue::Null => None,
        })
    }
}

/// The kind of object a [`Rule`] matches, named after the LaunchDarkly context
/// kind (see [`cluster_context`] and [`replica_context`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ContextKind {
    Cluster,
    Replica,
}

/// An attribute of a cluster or replica that a [`Clause`] matches on, the same
/// set the LaunchDarkly `cluster` and `replica` contexts carry (see
/// [`cluster_context`] and [`replica_context`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScopeAttribute {
    ClusterId,
    ClusterName,
    /// Whether the object is, or belongs to, a builtin (system) cluster.
    IsBuiltin,
    ReplicaId,
    ReplicaName,
    ReplicaSize,
    ReplicaSizeFamily,
}

impl ScopeAttribute {
    /// Whether an object of `kind` carries this attribute.
    fn in_context(&self, kind: ContextKind) -> bool {
        match self {
            Self::ClusterId | Self::ClusterName | Self::IsBuiltin => true,
            Self::ReplicaId | Self::ReplicaName | Self::ReplicaSize | Self::ReplicaSizeFamily => {
                kind == ContextKind::Replica
            }
        }
    }
}

/// The attributes a cluster is matched against, mirroring [`cluster_context`].
/// Replica-free, so a cluster-coherent parameter resolves identically across
/// the cluster's replicas.
fn cluster_attributes(cluster: &ClusterScopeContext) -> BTreeMap<ScopeAttribute, String> {
    BTreeMap::from([
        (ScopeAttribute::ClusterId, cluster.id.clone()),
        (ScopeAttribute::ClusterName, cluster.name.clone()),
        (ScopeAttribute::IsBuiltin, cluster.is_builtin.to_string()),
    ])
}

/// The attributes a replica is matched against, mirroring [`replica_context`].
fn replica_attributes(replica: &ReplicaScopeContext) -> BTreeMap<ScopeAttribute, String> {
    BTreeMap::from([
        (ScopeAttribute::ClusterId, replica.cluster_id.clone()),
        (ScopeAttribute::ClusterName, replica.cluster_name.clone()),
        (ScopeAttribute::IsBuiltin, replica.is_builtin.to_string()),
        (ScopeAttribute::ReplicaId, replica.id.clone()),
        (ScopeAttribute::ReplicaName, replica.name.clone()),
        (ScopeAttribute::ReplicaSize, replica.size.clone()),
        (
            ScopeAttribute::ReplicaSizeFamily,
            replica.size_family.clone(),
        ),
    ])
}

/// One element of the `rules` array: the parameters to apply to the objects of
/// one context kind that its clauses match.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(try_from = "RawRule")]
struct Rule {
    context_kind: ContextKind,
    /// The clauses, ANDed. An empty list constrains nothing, so it matches every
    /// object of [`Self::context_kind`]. Every clause names an attribute that
    /// objects of that kind carry.
    clauses: Vec<Clause>,
    /// The values this rule supplies, keyed by the parameter's external name.
    parameters: BTreeMap<String, ParamValue>,
}

/// A [`Rule`] as written, before its clauses are checked against its context
/// kind.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawRule {
    context_kind: ContextKind,
    clauses: Vec<Clause>,
    parameters: BTreeMap<String, ParamValue>,
}

impl TryFrom<RawRule> for Rule {
    type Error = String;

    fn try_from(raw: RawRule) -> Result<Self, String> {
        for (index, clause) in raw.clauses.iter().enumerate() {
            if clause
                .context_kind
                .is_some_and(|kind| kind != raw.context_kind)
            {
                return Err(format!(
                    "clauses[{index}].contextKind differs from the rule's contextKind"
                ));
            }
            // A clause on an attribute the context kind lacks could only ever be
            // false. Refusing it keeps a cluster rule replica-free by
            // construction rather than by `cluster_attributes` staying so.
            if !clause.attribute.in_context(raw.context_kind) {
                return Err(format!(
                    "clauses[{index}].attribute is not carried by objects of the rule's \
                     contextKind"
                ));
            }
        }
        Ok(Self {
            context_kind: raw.context_kind,
            clauses: raw.clauses,
            parameters: raw.parameters,
        })
    }
}

impl Rule {
    /// Whether the rule selects an object of `kind` carrying `attributes`. A rule
    /// of another context kind selects nothing.
    fn matches(&self, kind: ContextKind, attributes: &BTreeMap<ScopeAttribute, String>) -> bool {
        self.context_kind == kind && self.clauses.iter().all(|clause| clause.matches(attributes))
    }
}

/// One clause of a [`Rule`]: an operator applied to one attribute's value.
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "RawClause")]
struct Clause {
    /// The attribute whose value the operator is applied to.
    attribute: ScopeAttribute,
    op: ClauseOp,
    /// Whether to invert the clause, after the OR across the operator's operands
    /// as in LaunchDarkly.
    negate: bool,
    /// The context kind the clause names, checked against its rule's.
    context_kind: Option<ContextKind>,
}

/// A [`Clause`] as written, in LaunchDarkly's clause shape.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawClause {
    attribute: ScopeAttribute,
    op: Operator,
    values: Vec<ClauseValue>,
    /// Absent means false. More lenient than the SDK, which requires the key,
    /// because this file is hand-authored.
    #[serde(default)]
    negate: bool,
    /// Optional, unlike in LaunchDarkly, as the rule already names the context
    /// kind.
    #[serde(default)]
    context_kind: Option<ContextKind>,
    /// The key LaunchDarkly's REST API stamps on a clause, accepted so that a
    /// clause copied from the API parses as written.
    #[serde(default, rename = "_id")]
    _id: IgnoredAny,
}

impl TryFrom<RawClause> for Clause {
    type Error = String;

    fn try_from(raw: RawClause) -> Result<Self, String> {
        let values = raw.values.into_iter().map(|value| value.0);
        let op = match raw.op {
            Operator::In => ClauseOp::In(values.collect()),
            Operator::StartsWith => ClauseOp::StartsWith(values.collect()),
            Operator::EndsWith => ClauseOp::EndsWith(values.collect()),
            Operator::Contains => ClauseOp::Contains(values.collect()),
            Operator::Matches => ClauseOp::Matches(
                values
                    .enumerate()
                    .map(|(index, pattern)| {
                        Regex::new(&pattern).map(Pattern).map_err(|e| {
                            // Collapse the multi-line error so the warning is one
                            // line.
                            let error = e
                                .to_string()
                                .split_whitespace()
                                .collect::<Vec<_>>()
                                .join(" ");
                            format!("values[{index}] is the invalid pattern {pattern:?}: {error}")
                        })
                    })
                    .collect::<Result<_, _>>()?,
            ),
        };
        Ok(Self {
            attribute: raw.attribute,
            op,
            negate: raw.negate,
            context_kind: raw.context_kind,
        })
    }
}

/// One entry of a clause's `values`, a JSON string, number or boolean rendered
/// to a string as scope attributes are, so `true` and `"true"` are the same
/// value.
#[derive(Deserialize)]
#[serde(try_from = "JsonValue")]
struct ClauseValue(String);

impl TryFrom<JsonValue> for ClauseValue {
    type Error = String;

    fn try_from(value: JsonValue) -> Result<Self, String> {
        match value {
            JsonValue::String(v) => Ok(Self(v)),
            JsonValue::Number(v) => Ok(Self(v.to_string())),
            JsonValue::Bool(v) => Ok(Self(v.to_string())),
            other @ (JsonValue::Object(_) | JsonValue::Array(_) | JsonValue::Null) => Err(format!(
                "expected a string, number or boolean, found {other}"
            )),
        }
    }
}

/// A clause's `op`, the string operators of LaunchDarkly's vocabulary.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum Operator {
    In,
    StartsWith,
    EndsWith,
    Contains,
    Matches,
}

/// A [`Clause`]'s operator with the operands it is applied against, ORed.
#[derive(Debug, PartialEq, Eq)]
enum ClauseOp {
    In(Vec<String>),
    StartsWith(Vec<String>),
    EndsWith(Vec<String>),
    Contains(Vec<String>),
    Matches(Vec<Pattern>),
}

/// A compiled `matches` pattern, equal to another compiled from the same source.
#[derive(Debug)]
struct Pattern(Regex);

impl PartialEq for Pattern {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Eq for Pattern {}

impl Clause {
    /// Whether the clause holds for an object carrying `attributes`.
    fn matches(&self, attributes: &BTreeMap<ScopeAttribute, String>) -> bool {
        let Some(value) = attributes.get(&self.attribute) else {
            // Unreachable, as `Rule` refuses an attribute outside its context
            // kind. `false` regardless of `negate`, as in the SDK.
            return false;
        };
        self.op.holds(value) != self.negate
    }
}

impl ClauseOp {
    /// Whether the operator holds for `value`, before [`Clause::negate`].
    fn holds(&self, value: &str) -> bool {
        match self {
            Self::In(allowed) => allowed.iter().any(|allowed| allowed == value),
            Self::StartsWith(prefixes) => prefixes.iter().any(|prefix| value.starts_with(prefix)),
            Self::EndsWith(suffixes) => suffixes.iter().any(|suffix| value.ends_with(suffix)),
            Self::Contains(needles) => needles.iter().any(|needle| value.contains(needle)),
            // Unanchored, as LaunchDarkly's `matches`, which uses the same crate.
            Self::Matches(patterns) => patterns.iter().any(|pattern| pattern.0.is_match(value)),
        }
    }
}

/// The frontend's cached view of the config-sync file.
#[derive(Debug)]
struct CachedConfigFile {
    /// The contents the cache was built from, or `None` if that read failed.
    /// The next read is compared against it, so the file is re-parsed, and
    /// diagnosed, only when it changes.
    contents: Option<String>,
    /// The parse of [`Self::contents`], or `None` if the read failed or the
    /// document did not parse. The environment-wide pass reads this.
    current: Option<Arc<ConfigFileV2>>,
    /// The newest valid parse whose environment-wide section is committed to
    /// the catalog. The scoped passes read this. It survives a failed read.
    published: Option<Arc<ConfigFileV2>>,
}

/// Whether `value`, a scoped source's raw value for `param_name`, is an
/// override: it parses for the parameter's type and differs from the
/// environment-wide value held in `params`.
///
/// An unparseable value is no override, as recording it would panic the
/// optimizer's `bool` decode for a cluster-coherent override. The comparison is
/// in the parameter's canonical encoding, since a raw boolean is `"true"` where
/// the environment-wide value is `"on"`. Callers store the raw value.
fn is_override(params: &SynchronizedParameters, param_name: &str, value: &str) -> bool {
    params
        .canonicalize(param_name, value)
        .is_some_and(|canonical| canonical != params.get(param_name))
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
        // Read the file once per tick rather than once per parameter, so a
        // rewrite landing mid-loop cannot be observed as a torn read. The scoped
        // passes read only the cache, as the create path runs them on the
        // coordinator loop.
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
                    .and_then(|file| file.global_parameters.get(flag_name))
                    .and_then(|value| value.0.clone())
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
    /// it at `path`, and returns its parse.
    ///
    /// Unchanged contents return the cached parse without re-parsing, so a bad
    /// file is warned about once per change rather than on every tick. The
    /// scoped passes see the new parse only after [`Self::publish_config_file`].
    fn refresh_config_file(
        &self,
        path: &Path,
        read: io::Result<String>,
        params: &SynchronizedParameters,
    ) -> Option<Arc<ConfigFileV2>> {
        let mut cache = self
            .config_file
            .lock()
            .expect("config file cache lock poisoned");

        if let Some(cached) = &*cache {
            if cached.contents.as_deref() == read.as_deref().ok() {
                return cached.current.clone();
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
        let file = contents.as_deref().and_then(|contents| {
            let parsed = serde_json::from_str(contents)
                .map_err(|e| e.to_string())
                .and_then(ConfigFileV2::parse);
            match parsed {
                Ok(file) => Some(Arc::new(file)),
                Err(e) => {
                    warn!("could not parse system parameter sync file: {e}");
                    None
                }
            }
        });
        if let Some(file) = &file {
            for diagnostic in self.scoped_rule_diagnostics(file, params) {
                warn!("{diagnostic}");
            }
        }
        let published = cache.as_ref().and_then(|cached| cached.published.clone());
        *cache = Some(CachedConfigFile {
            contents,
            current: file.clone(),
            // Advanced only by `publish_config_file`.
            published,
        });

        file
    }

    /// The newest valid config-sync file whose environment-wide section is
    /// committed to the catalog, or `None` if no read has parsed yet.
    fn published_config_file(&self) -> Option<Arc<ConfigFileV2>> {
        self.config_file
            .lock()
            .expect("config file cache lock poisoned")
            .as_ref()
            .and_then(|cached| cached.published.clone())
    }

    /// Promotes the current read to the published parse. A no-op when the
    /// current read failed or did not parse.
    ///
    /// Call this after pushing the current read's environment-wide values and
    /// before the scoped reconcile. A rule is recorded only where it differs from
    /// the environment-wide value, so it must be resolved against its own file's
    /// values.
    pub fn publish_config_file(&self) {
        let mut cache = self
            .config_file
            .lock()
            .expect("config file cache lock poisoned");
        if let Some(cached) = &mut *cache {
            if cached.current.is_some() {
                cached.published = cached.current.clone();
            }
        }
    }

    /// Whether the frontend knows the desired state of the scoped parameters.
    ///
    /// `false` when the most recent read of the config-sync file failed or did
    /// not parse. The scoped reconcile prunes every override absent from the
    /// desired state, so callers must skip it while this is `false`. Always
    /// `true` for LaunchDarkly.
    pub fn has_scoped_desired_state(&self) -> bool {
        match &self.client {
            SystemParameterFrontendClient::LaunchDarkly { .. } => true,
            SystemParameterFrontendClient::File { .. } => self
                .config_file
                .lock()
                .expect("config file cache lock poisoned")
                .as_ref()
                .is_some_and(|cached| cached.current.is_some()),
        }
    }

    /// The rule parameters that resolution drops silently and that an operator
    /// can act on. They depend on this binary's parameter set, which is why they
    /// are diagnosed per parameter rather than failing the document.
    fn scoped_rule_diagnostics(
        &self,
        file: &ConfigFileV2,
        params: &SynchronizedParameters,
    ) -> Vec<String> {
        let mut diagnostics = Vec::new();

        let scopable = self.scopable_params(params);
        for (index, rule) in file.rules.iter().enumerate() {
            let position = format!("rules[{index}]");

            for (name, value) in &rule.parameters {
                let Some(&(param_name, scope)) = scopable.get(name.as_str()) else {
                    diagnostics.push(format!(
                        "ignoring {name} for {position} in the system parameter sync file: \
                         not a cluster-scoped or replica-scoped system parameter"
                    ));
                    continue;
                };
                // A cluster-scoped parameter resolves only through the cluster
                // pass, which never consults a replica rule.
                if scope == ParameterScope::Cluster && rule.context_kind == ContextKind::Replica {
                    diagnostics.push(format!(
                        "ignoring {param_name} for {position} in the system parameter sync file: \
                         {param_name} is cluster-scoped, so it cannot be supplied through a \
                         rule of contextKind \"replica\""
                    ));
                    continue;
                }
                // `null` expresses no opinion rather than a value, so there is
                // nothing to parse.
                let Some(value) = &value.0 else {
                    continue;
                };
                if params.canonicalize(param_name, value).is_none() {
                    diagnostics.push(format!(
                        "ignoring unparseable value {value:?} for system parameter {param_name} \
                         on {position} in the system parameter sync file"
                    ));
                }
            }
        }

        diagnostics
    }

    /// The synced parameters that declare a scope, keyed by the name a rule spells
    /// them with and carrying the scope they declare.
    fn scopable_params<'a>(
        &'a self,
        params: &SynchronizedParameters,
    ) -> BTreeMap<&'a str, (&'static str, ParameterScope)> {
        let mut scopable = BTreeMap::new();
        for scope in [ParameterScope::Cluster, ParameterScope::Replica] {
            for param_name in params.synchronized_with_scope(scope) {
                scopable.insert(self.external_name(param_name), (param_name, scope));
            }
        }
        scopable
    }

    /// Evaluates the replica-local scoped parameters for each given replica and
    /// returns, per replica, the parameter values that differ from the
    /// environment-wide value held in `params`.
    ///
    /// The returned map is sparse: replicas with no overriding value are
    /// omitted. Replicas absent from `replicas` are never evaluated, so a
    /// config-sync file rule that matches nothing live has no effect.
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
            // An empty result means "no overrides", so a caller reconciling the
            // full desired state must first check `has_scoped_desired_state`.
            SystemParameterFrontendClient::File { .. } => {
                let Some(file) = self.published_config_file() else {
                    return out;
                };
                let replicas = replicas
                    .iter()
                    .map(|replica| (replica.replica_id, replica_attributes(&replica.replica)));
                return self.file_overrides(
                    &file,
                    params,
                    param_names,
                    ContextKind::Replica,
                    replicas,
                );
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
    /// omitted. Clusters absent from `clusters` are never evaluated, so a
    /// config-sync file rule that matches nothing live has no effect.
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
                let Some(file) = self.published_config_file() else {
                    return out;
                };
                let clusters = clusters
                    .iter()
                    .map(|cluster| (cluster.cluster_id, cluster_attributes(&cluster.cluster)));
                return self.file_overrides(
                    &file,
                    params,
                    param_names,
                    ContextKind::Cluster,
                    clusters,
                );
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

            // Record on differing from the environment-wide value. The
            // `variation_detail` reason cannot say which context kind's clause
            // matched, and `Fallthrough` serves every object the same value. An
            // unparseable value is dropped silently, as nothing in the
            // environment can fix it.
            if is_override(params, param_name, &value) {
                overrides.insert(param_name.to_string(), value);
            }
        }
        overrides
    }

    /// Resolves the scoped overrides `file`'s rules declare for each of
    /// `objects`, the id and scope attributes of each object of context `kind`.
    /// Every parameter in `param_names` must declare the scope of `kind`.
    ///
    /// The first matching rule with a non-null value for a parameter decides it.
    /// A decided value that matches the environment-wide value or does not parse
    /// yields no override. Silent, see [`Self::scoped_rule_diagnostics`].
    fn file_overrides<Id: Ord>(
        &self,
        file: &ConfigFileV2,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        kind: ContextKind,
        objects: impl Iterator<Item = (Id, BTreeMap<ScopeAttribute, String>)>,
    ) -> BTreeMap<Id, BTreeMap<String, String>> {
        let requested: BTreeMap<&str, &'static str> = param_names
            .iter()
            .map(|&param_name| (self.external_name(param_name), param_name))
            .collect();

        let mut out = BTreeMap::new();
        for (id, attributes) in objects {
            let mut decided: BTreeMap<&'static str, &str> = BTreeMap::new();
            for rule in file
                .rules
                .iter()
                .filter(|rule| rule.matches(kind, &attributes))
            {
                for (name, value) in &rule.parameters {
                    let Some(&param_name) = requested.get(name.as_str()) else {
                        continue;
                    };
                    // First match wins before the value is judged, so a malformed
                    // value does not fall through to a later rule.
                    if decided.contains_key(param_name) {
                        continue;
                    }
                    // `null` expresses no opinion, leaving the parameter to a later
                    // rule.
                    let Some(value) = &value.0 else {
                        continue;
                    };
                    decided.insert(param_name, value);
                }
            }

            let overrides: BTreeMap<String, String> = decided
                .into_iter()
                .filter(|(param_name, value)| is_override(params, param_name, value))
                .map(|(param_name, value)| (param_name.to_string(), value.to_string()))
                .collect();
            if !overrides.is_empty() {
                out.insert(id, overrides);
            }
        }
        out
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
    let transport = mz_dyncfg_launchdarkly::https_transport(Duration::from_secs(10), read_timeout)
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
) -> Result<Arc<ld::Client>, anyhow::Error> {
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

    Ok(Arc::new(ld_client))
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
    // NOTE: The order these are added in does not affect evaluation, as the SDK
    // looks each context kind up by name (`Context::as_kind`). Within a flag,
    // targets, then rules, then the fallthrough apply, each in array order with
    // the first match winning.
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
mod tests;
