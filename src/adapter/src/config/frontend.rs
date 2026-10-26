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
#[serde(try_from = "RawConfigFileV2")]
struct ConfigFileV2 {
    /// Environment-wide values, keyed by the parameter's external name.
    global_parameters: BTreeMap<String, JsonValue>,
    /// The predicates rules select objects with, keyed by segment name.
    segments: BTreeMap<String, Segment>,
    /// The rules in file order. The first rule whose segment matches an object
    /// decides each parameter it supplies. Every rule names a key of
    /// [`Self::segments`].
    rules: Vec<Rule>,
}

/// A [`ConfigFileV2`] as written, before its rules are checked against its
/// segments.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfigFileV2 {
    #[serde(rename = "version")]
    _version: FormatVersion,
    #[serde(default)]
    global_parameters: BTreeMap<String, JsonValue>,
    #[serde(default)]
    segments: BTreeMap<String, Segment>,
    #[serde(default)]
    rules: Vec<Rule>,
}

impl TryFrom<RawConfigFileV2> for ConfigFileV2 {
    type Error = String;

    fn try_from(raw: RawConfigFileV2) -> Result<Self, String> {
        for (index, rule) in raw.rules.iter().enumerate() {
            if !raw.segments.contains_key(&rule.segment) {
                return Err(format!(
                    "rules[{index}] names the segment {:?}, which segments does not define",
                    rule.segment
                ));
            }
        }
        Ok(Self {
            global_parameters: raw.global_parameters,
            segments: raw.segments,
            rules: raw.rules,
        })
    }
}

/// The `version` of a [`ConfigFileV2`], which must be 2.
#[derive(Deserialize)]
#[serde(try_from = "u64")]
struct FormatVersion;

impl TryFrom<u64> for FormatVersion {
    type Error = String;

    fn try_from(version: u64) -> Result<Self, String> {
        match version {
            2 => Ok(Self),
            other => Err(format!("unsupported version {other}, expected 2")),
        }
    }
}

/// The config-sync file in the format without a `version` key: a flat map of
/// environment-wide values, keyed by the parameter's external name.
#[derive(Deserialize)]
#[serde(transparent)]
struct LegacyConfigFile(BTreeMap<String, JsonValue>);

impl From<LegacyConfigFile> for ConfigFileV2 {
    fn from(legacy: LegacyConfigFile) -> Self {
        Self {
            global_parameters: legacy.0,
            segments: BTreeMap::new(),
            rules: Vec::new(),
        }
    }
}

impl ConfigFileV2 {
    /// Parses the config-sync file's contents in either format, or describes
    /// the first defect.
    ///
    /// An error is "no information about any parameter", which callers must keep
    /// distinct from a valid but empty document.
    fn parse(contents: &str) -> Result<Self, String> {
        let document: serde_json::Map<String, JsonValue> =
            serde_json::from_str(contents).map_err(|e| e.to_string())?;
        // The format is chosen by the key's presence. Trying version 2 and
        // falling back to the flat format would read a defective version 2
        // document as a valid flat one, which prunes every scoped override.
        if let Some(version) = document.get(VERSION_KEY) {
            // Checked ahead of the rest, so a document of another version is
            // reported as that rather than as its first unknown key.
            let _: FormatVersion =
                FormatVersion::deserialize(version).map_err(|e| format!("{VERSION_KEY}: {e}"))?;
            deserialize_tracked(JsonValue::Object(document))
        } else {
            deserialize_tracked::<LegacyConfigFile>(JsonValue::Object(document)).map(Self::from)
        }
    }

    /// Each rule with its index in [`Self::rules`] and the segment it names.
    fn resolved_rules(&self) -> impl Iterator<Item = (usize, &Rule, &Segment)> {
        self.rules.iter().enumerate().map(move |(index, rule)| {
            let segment = self
                .segments
                .get(&rule.segment)
                .expect("deserialization rejects a rule naming an undefined segment");
            (index, rule, segment)
        })
    }
}

/// Deserializes `value`, prefixing an error with the path to the element it is
/// about, such as `segments.prod.clauses[1].op`.
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

/// The kind of object a [`Segment`]'s clauses match, named after the
/// LaunchDarkly context kind (see [`cluster_context`] and [`replica_context`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ContextKind {
    Cluster,
    Replica,
}

impl ContextKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Cluster => "cluster",
            Self::Replica => "replica",
        }
    }
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
    fn as_str(&self) -> &'static str {
        match self {
            Self::ClusterId => "cluster_id",
            Self::ClusterName => "cluster_name",
            Self::IsBuiltin => "is_builtin",
            Self::ReplicaId => "replica_id",
            Self::ReplicaName => "replica_name",
            Self::ReplicaSize => "replica_size",
            Self::ReplicaSizeFamily => "replica_size_family",
        }
    }

    /// Whether the attribute can distinguish two replicas of one cluster.
    fn is_replica_attribute(&self) -> bool {
        match self {
            Self::ClusterId | Self::ClusterName | Self::IsBuiltin => false,
            Self::ReplicaId | Self::ReplicaName | Self::ReplicaSize | Self::ReplicaSizeFamily => {
                true
            }
        }
    }

    /// Whether an object of `kind` carries this attribute.
    fn in_context(&self, kind: ContextKind) -> bool {
        match kind {
            ContextKind::Cluster => !self.is_replica_attribute(),
            ContextKind::Replica => true,
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

/// A named predicate selecting the clusters or replicas a rule applies to: a
/// context kind and LaunchDarkly-shaped clauses, ANDed.
#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "RawSegment")]
struct Segment {
    context_kind: ContextKind,
    /// The clauses, ANDed. An empty list constrains nothing, so it matches every
    /// object of [`Self::context_kind`]. Every clause names an attribute that
    /// objects of that kind carry.
    clauses: Vec<Clause>,
}

/// A [`Segment`] as written, before its clauses are checked against its
/// context kind.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RawSegment {
    context_kind: ContextKind,
    clauses: Vec<Clause>,
}

impl TryFrom<RawSegment> for Segment {
    type Error = String;

    fn try_from(raw: RawSegment) -> Result<Self, String> {
        // A clause on an attribute the context kind lacks could only ever be
        // false. Refusing it is what makes the cluster-coherence guard
        // structural rather than a property of `cluster_attributes` staying
        // replica-free.
        for (index, clause) in raw.clauses.iter().enumerate() {
            if !clause.attribute.in_context(raw.context_kind) {
                return Err(format!(
                    "clauses[{index}] names the attribute {:?}, which a {:?} does not carry",
                    clause.attribute.as_str(),
                    raw.context_kind.as_str()
                ));
            }
        }
        Ok(Self {
            context_kind: raw.context_kind,
            clauses: raw.clauses,
        })
    }
}

/// One clause of a [`Segment`]: an operator applied to one attribute's value
/// against a list of values.
#[derive(Debug, Deserialize)]
#[serde(try_from = "RawClause")]
struct Clause {
    /// The attribute whose value the operator is applied to.
    attribute: ScopeAttribute,
    op: Operator,
    /// The values the operator is applied against, ORed.
    values: Vec<String>,
    /// The compiled [`Operator::Matches`] patterns, one per entry of
    /// [`Self::values`], and empty for every other operator.
    patterns: Vec<Regex>,
    /// Whether to invert the clause, after the OR across [`Self::values`] as in
    /// LaunchDarkly.
    negate: bool,
}

/// A [`Clause`] as written, in LaunchDarkly's clause shape.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawClause {
    attribute: ScopeAttribute,
    op: Operator,
    values: Vec<ClauseValue>,
    /// Absent means false. More lenient than the SDK, which requires the key,
    /// because this file is hand-authored.
    #[serde(default)]
    negate: bool,
    /// The key LaunchDarkly's REST API stamps on a clause, accepted so that a
    /// clause copied from the API parses as written.
    #[serde(default)]
    _id: IgnoredAny,
}

impl TryFrom<RawClause> for Clause {
    type Error = String;

    fn try_from(raw: RawClause) -> Result<Self, String> {
        let values: Vec<String> = raw.values.into_iter().map(|value| value.0).collect();
        let mut patterns = Vec::new();
        if raw.op == Operator::Matches {
            for (index, pattern) in values.iter().enumerate() {
                let compiled = Regex::new(pattern).map_err(|e| {
                    // Collapse the multi-line error so the warning is one line.
                    let error = e
                        .to_string()
                        .split_whitespace()
                        .collect::<Vec<_>>()
                        .join(" ");
                    format!(
                        "values[{index}] is the invalid {:?} pattern {pattern:?}: {error}",
                        Operator::Matches.as_str()
                    )
                })?;
                patterns.push(compiled);
            }
        }
        Ok(Self {
            attribute: raw.attribute,
            op: raw.op,
            values,
            patterns,
            negate: raw.negate,
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

/// A [`Clause`] operator, the string operators of LaunchDarkly's vocabulary.
///
/// NOTE: The SDK's own `Op` is `pub(crate)`, so this mirrors it and
/// `test_operator_vocabulary_matches_launchdarkly` keeps the two aligned. The
/// compiler does not.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "String")]
enum Operator {
    In,
    StartsWith,
    EndsWith,
    Contains,
    Matches,
}

impl Operator {
    /// Every supported operator. The vocabulary
    /// `test_operator_vocabulary_matches_launchdarkly` checks against.
    const ALL: [Self; 5] = [
        Self::In,
        Self::StartsWith,
        Self::EndsWith,
        Self::Contains,
        Self::Matches,
    ];

    /// The operator of this name, or `None` if it is not supported.
    fn parse(op: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|candidate| candidate.as_str() == op)
    }

    fn as_str(&self) -> &'static str {
        match self {
            Self::In => "in",
            Self::StartsWith => "startsWith",
            Self::EndsWith => "endsWith",
            Self::Contains => "contains",
            Self::Matches => "matches",
        }
    }
}

impl TryFrom<String> for Operator {
    type Error = String;

    fn try_from(op: String) -> Result<Self, String> {
        if let Some(supported) = Self::parse(&op) {
            return Ok(supported);
        }
        Err(match unsupported_operator(&op) {
            Some(reason) => {
                format!("the LaunchDarkly operator {op:?} is not supported: it {reason}")
            }
            None => format!(
                "{op:?} is not an operator, expected one of {}",
                Self::ALL.map(|supported| supported.as_str()).join(", ")
            ),
        })
    }
}

/// Why the LaunchDarkly operator `op` is refused, or `None` if it is not a
/// LaunchDarkly operator at all.
fn unsupported_operator(op: &str) -> Option<&'static str> {
    match op {
        "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" => {
            Some("compares numbers, and every cluster and replica attribute is a string")
        }
        "before" | "after" => {
            Some("compares dates, and every cluster and replica attribute is a string")
        }
        "semVerEqual" | "semVerGreaterThan" | "semVerLessThan" => {
            Some("compares semantic versions, and every cluster and replica attribute is a string")
        }
        // A clause-level segment reference would be a second way to name a
        // segment, the `rules` array already being the first.
        "segmentMatch" => Some(
            "references another segment, which this file expresses through the segment a rule \
             names",
        ),
        _ => None,
    }
}

/// Compares the patterns by the source they were compiled from, a [`Regex`] being
/// uncomparable itself. Two clauses equal under this decide the same objects.
impl PartialEq for Clause {
    fn eq(&self, other: &Self) -> bool {
        self.attribute == other.attribute
            && self.op == other.op
            && self.values == other.values
            && self.negate == other.negate
            && self.patterns.len() == other.patterns.len()
            && std::iter::zip(&self.patterns, &other.patterns)
                .all(|(a, b)| a.as_str() == b.as_str())
    }
}

impl Eq for Clause {}

impl Clause {
    /// Whether the clause holds for an object carrying `attributes`.
    fn matches(&self, attributes: &BTreeMap<ScopeAttribute, String>) -> bool {
        let Some(value) = attributes.get(&self.attribute) else {
            // Unreachable, as `Segment` refuses an attribute outside its
            // context kind. `false` regardless of `negate`, as in the SDK.
            return false;
        };
        self.holds(value) != self.negate
    }

    /// Whether the operator holds for `value`, before [`Self::negate`].
    fn holds(&self, value: &str) -> bool {
        match self.op {
            Operator::In => self.values.iter().any(|allowed| allowed == value),
            Operator::StartsWith => self.values.iter().any(|prefix| value.starts_with(prefix)),
            Operator::EndsWith => self.values.iter().any(|suffix| value.ends_with(suffix)),
            Operator::Contains => self.values.iter().any(|needle| value.contains(needle)),
            // Unanchored, as LaunchDarkly's `matches`, which uses the same crate.
            Operator::Matches => self.patterns.iter().any(|pattern| pattern.is_match(value)),
        }
    }
}

impl Segment {
    /// Whether the segment selects an object of `kind` carrying `attributes`. A
    /// segment of another context kind selects nothing.
    fn matches(&self, kind: ContextKind, attributes: &BTreeMap<ScopeAttribute, String>) -> bool {
        self.context_kind == kind && self.clauses.iter().all(|clause| clause.matches(attributes))
    }
}

/// One element of the `rules` array: the parameters to apply to the objects a
/// segment matches.
#[derive(Debug, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Rule {
    /// The name of the [`Segment`] selecting the objects this rule applies to.
    segment: String,
    /// The values this rule supplies, keyed by the parameter's external name.
    parameters: BTreeMap<String, JsonValue>,
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
    /// Does not parse for the parameter's type, so it is dropped. Recording it
    /// would panic the optimizer's `bool` decode for a cluster-coherent override.
    Unparseable,
}

/// Classifies `value` as the scoped value of `param_name` against the
/// environment-wide `base`, the var-formatted value held in `params`.
///
/// The comparison is in the parameter's canonical encoding, since a raw boolean
/// is `"true"` where `base` is `"on"`. Callers store the raw value.
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
        let file = contents
            .as_deref()
            .and_then(|contents| match ConfigFileV2::parse(contents) {
                Ok(file) => Some(Arc::new(file)),
                Err(e) => {
                    warn!("could not parse system parameter sync file: {e}");
                    None
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
        for (index, rule, segment) in file.resolved_rules() {
            let position = format!("rules[{index}] (segment {:?})", rule.segment);

            for (name, value) in &rule.parameters {
                let Some(&(param_name, scope)) = scopable.get(name.as_str()) else {
                    diagnostics.push(format!(
                        "ignoring {name} for {position} in the system parameter sync file: \
                         not a cluster-scoped or replica-scoped system parameter"
                    ));
                    continue;
                };
                // The coherence guard, see [`Self::file_rule_overrides`].
                if scope == ParameterScope::Cluster && segment.context_kind == ContextKind::Replica
                {
                    diagnostics.push(format!(
                        "ignoring {param_name} for {position} in the system parameter sync file: \
                         {param_name} is cluster-scoped, so it cannot be supplied through a \
                         segment of context kind {:?}",
                        ContextKind::Replica.as_str()
                    ));
                    continue;
                }
                // `null` expresses no opinion rather than a value, so there is
                // nothing to parse.
                let Some(value) = json_param_value(value) else {
                    continue;
                };
                let base = params.get(param_name);
                if classify_scoped_value(params, param_name, &base, &value)
                    == ScopedValue::Unparseable
                {
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
    /// config-sync file segment that matches nothing live has no effect.
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
    /// omitted. Clusters absent from `clusters` are never evaluated, so a
    /// config-sync file segment that matches nothing live has no effect.
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
            if classify_scoped_value(params, param_name, &base, &value) == ScopedValue::Override {
                overrides.insert(param_name.to_string(), value);
            }
        }
        overrides
    }

    /// Resolves the cluster-coherent overrides `file`'s rules declare for each of
    /// `clusters`.
    fn file_cluster_overrides(
        &self,
        file: &ConfigFileV2,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        clusters: &[ClusterEvalContext],
    ) -> BTreeMap<ClusterId, BTreeMap<String, String>> {
        let mut out = BTreeMap::new();
        for cluster in clusters {
            let overrides = self.file_rule_overrides(
                file,
                params,
                param_names,
                ParameterScope::Cluster,
                ContextKind::Cluster,
                &cluster_attributes(&cluster.cluster),
            );
            if !overrides.is_empty() {
                out.insert(cluster.cluster_id, overrides);
            }
        }
        out
    }

    /// Resolves the replica-local overrides `file`'s rules declare for each of
    /// `replicas`.
    fn file_replica_overrides(
        &self,
        file: &ConfigFileV2,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        replicas: &[ReplicaEvalContext],
    ) -> BTreeMap<ReplicaId, BTreeMap<String, String>> {
        let mut out = BTreeMap::new();
        for replica in replicas {
            let overrides = self.file_rule_overrides(
                file,
                params,
                param_names,
                ParameterScope::Replica,
                ContextKind::Replica,
                &replica_attributes(&replica.replica),
            );
            if !overrides.is_empty() {
                out.insert(replica.replica_id, overrides);
            }
        }
        out
    }

    /// Resolves one object's scoped overrides from `file`'s rules, given the
    /// object's context `kind` and scope `attributes`, and the `scope` that every
    /// parameter in `param_names` declares.
    ///
    /// The first matching rule with a non-null value for a parameter decides it.
    /// A decided value that matches the environment-wide value or does not parse
    /// yields no override. Silent, see [`Self::scoped_rule_diagnostics`].
    fn file_rule_overrides(
        &self,
        file: &ConfigFileV2,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        scope: ParameterScope,
        kind: ContextKind,
        attributes: &BTreeMap<ScopeAttribute, String>,
    ) -> BTreeMap<String, String> {
        let requested: BTreeMap<&str, &'static str> = param_names
            .iter()
            .map(|&param_name| (self.external_name(param_name), param_name))
            .collect();

        let mut decided: BTreeMap<&'static str, String> = BTreeMap::new();
        for (_, rule, segment) in file.resolved_rules() {
            if !segment.matches(kind, attributes) {
                continue;
            }
            // The coherence guard: a cluster-scoped parameter comes only from a
            // `cluster` segment. The match above already ensures this when `kind`
            // is `Cluster`, and this keeps it from resting on the callers.
            if scope == ParameterScope::Cluster && segment.context_kind != ContextKind::Cluster {
                continue;
            }

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
                let Some(value) = json_param_value(value) else {
                    continue;
                };
                decided.insert(param_name, value);
            }
        }

        let mut overrides = BTreeMap::new();
        for (param_name, value) in decided {
            let base = params.get(param_name);
            if classify_scoped_value(params, param_name, &base, &value) == ScopedValue::Override {
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
    /// A second cluster-coherent `bool` parameter, also `off` environment-wide,
    /// for the tests that need two parameters to observe rule ordering.
    const CLUSTER_PARAM_2: &str = "enable_join_prioritize_arranged";
    /// A replica-local `bool` parameter whose environment-wide default is `on`.
    const REPLICA_PARAM: &str = "enable_lgalloc";

    /// A file-backed frontend reading `path`.
    fn file_frontend_at(path: &Path) -> SystemParameterFrontend {
        SystemParameterFrontend {
            client: SystemParameterFrontendClient::File { path: path.into() },
            key_map: BTreeMap::new(),
            env_id: env_id(),
            build_info: &DUMMY_BUILD_INFO,
            metrics: Metrics::register_into(&MetricsRegistry::new()),
            config_file: Mutex::new(None),
        }
    }

    /// A file-backed frontend for tests that drive it from contents they pass
    /// in, so its path is never read.
    fn file_frontend() -> SystemParameterFrontend {
        file_frontend_at(Path::new(CONFIG_PATH))
    }

    /// The path a [`file_frontend`] reports in its warnings. Never opened.
    const CONFIG_PATH: &str = "/nonexistent/system-params.json";

    /// Parses a document the test knows to be valid.
    fn parse(contents: &str) -> ConfigFileV2 {
        ConfigFileV2::parse(contents).unwrap_or_else(|e| panic!("document does not parse: {e}"))
    }

    /// The error a document the test knows to be invalid is rejected with.
    fn parse_error(contents: &str) -> String {
        ConfigFileV2::parse(contents).expect_err("document parses")
    }

    /// One tick of the sync loop's handling of the file: read it, then publish
    /// that read once its environment-wide values would have been pushed. The
    /// scoped passes see nothing until the publish, so a test that skips it is
    /// testing the window rather than the steady state.
    fn sync_tick(
        frontend: &SystemParameterFrontend,
        params: &SynchronizedParameters,
        read: io::Result<String>,
    ) {
        frontend.refresh_config_file(Path::new(CONFIG_PATH), read, params);
        frontend.publish_config_file();
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

    /// A replica of a legacy size family, the coarse targeting axis a segment
    /// matching on `replica_size_family` selects.
    fn legacy_replica_ctx(
        cluster_id: u64,
        cluster_name: &str,
        replica_id: u64,
        replica_name: &str,
    ) -> ReplicaEvalContext {
        let mut ctx = replica_ctx(cluster_id, cluster_name, replica_id, replica_name);
        ctx.replica.size = "xsmall".into();
        ctx.replica.size_family = "legacy".into();
        ctx
    }

    fn overrides(param_name: &str, value: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(param_name.to_string(), value.to_string())])
    }

    /// The clause a parse assertion expects.
    fn clause(attribute: ScopeAttribute, op: Operator, values: &[&str], negate: bool) -> Clause {
        Clause {
            attribute,
            op,
            values: values.iter().map(|value| value.to_string()).collect(),
            patterns: match op {
                Operator::Matches => values
                    .iter()
                    .map(|value| Regex::new(value).expect("test pattern compiles"))
                    .collect(),
                _ => Vec::new(),
            },
            negate,
        }
    }

    /// A document whose single rule gives the `analytics` cluster `value` for
    /// [`CLUSTER_PARAM`].
    fn scoped_file(value: bool) -> String {
        format!(
            r#"{{
                "version": 2,
                "segments": {{"analytics": {{
                    "contextKind": "cluster",
                    "clauses": [
                        {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                    ]
                }}}},
                "rules": [
                    {{"segment": "analytics", "parameters": {{"{CLUSTER_PARAM}": {value}}}}}
                ]
            }}"#
        )
    }

    /// A document whose `cluster` segment `s` holds `clause` as its only clause,
    /// with one rule naming it.
    fn single_clause_file(clause: &str) -> String {
        format!(
            r#"{{
                "version": 2,
                "segments": {{"s": {{"contextKind": "cluster", "clauses": [{clause}]}}}},
                "rules": [{{"segment": "s", "parameters": {{"{CLUSTER_PARAM}": true}}}}]
            }}"#
        )
    }

    #[mz_ore::test]
    fn test_parse_legacy_document() {
        let file = parse(
            r#"{
                "max_connections": 1000,
                "allowed_cluster_replica_sizes": "'25cc', '50cc'",
                "enable_lgalloc": false
            }"#,
        );

        assert_eq!(
            file,
            ConfigFileV2::from(LegacyConfigFile(BTreeMap::from([
                ("max_connections".to_string(), JsonValue::from(1000)),
                (
                    "allowed_cluster_replica_sizes".to_string(),
                    JsonValue::from("'25cc', '50cc'")
                ),
                ("enable_lgalloc".to_string(), JsonValue::Bool(false)),
            ])))
        );
        assert!(file.segments.is_empty());
        assert!(file.rules.is_empty());

        // Every JSON scalar renders to the raw string the backend parses.
        assert_eq!(
            json_param_value(&file.global_parameters["max_connections"]).as_deref(),
            Some("1000")
        );
        assert_eq!(
            json_param_value(&file.global_parameters["enable_lgalloc"]).as_deref(),
            Some("false")
        );
        assert_eq!(
            json_param_value(&file.global_parameters["allowed_cluster_replica_sizes"]).as_deref(),
            Some("'25cc', '50cc'")
        );
        // An explicit `null` expresses no opinion, leaving the value alone.
        let null = parse(r#"{"max_connections": null}"#);
        assert_eq!(
            json_param_value(&null.global_parameters["max_connections"]),
            None
        );

        // Without `version` no key is reserved, so these are parameter names like
        // any other, and the document scopes nothing.
        let flat = parse(r#"{"segments": {}, "rules": []}"#);
        assert_eq!(
            flat.global_parameters.keys().collect::<Vec<_>>(),
            vec!["rules", "segments"]
        );
        assert!(flat.segments.is_empty());
        assert!(flat.rules.is_empty());
    }

    #[mz_ore::test]
    fn test_parse_v2_document() {
        let file = parse(
            r#"{
                "version": 2,
                "global_parameters": {"enable_lgalloc": false},
                "segments": {
                    "analytics": {
                        "contextKind": "cluster",
                        "clauses": [{
                            "attribute": "cluster_name",
                            "op": "in",
                            "values": ["analytics", "analytics_2"]
                        }]
                    },
                    "legacy-replicas": {
                        "contextKind": "replica",
                        "clauses": [
                            {
                                "attribute": "replica_size_family",
                                "op": "in",
                                "values": ["legacy"]
                            },
                            {"attribute": "is_builtin", "op": "in", "values": [false]},
                            {
                                "attribute": "replica_name",
                                "op": "matches",
                                "values": ["^scratch-"],
                                "negate": true,
                                "_id": "carried over from the LaunchDarkly API"
                            }
                        ]
                    }
                },
                "rules": [
                    {"segment": "analytics", "parameters": {"enable_eager_delta_joins": true}},
                    {"segment": "legacy-replicas", "parameters": {"enable_lgalloc": true}}
                ]
            }"#,
        );

        assert_eq!(
            file.global_parameters,
            BTreeMap::from([("enable_lgalloc".to_string(), JsonValue::Bool(false))])
        );

        assert_eq!(
            file.segments["analytics"],
            Segment {
                context_kind: ContextKind::Cluster,
                clauses: vec![clause(
                    ScopeAttribute::ClusterName,
                    Operator::In,
                    &["analytics", "analytics_2"],
                    false
                )],
            }
        );

        // Clauses keep their array order, `negate` defaults to false, a boolean
        // value may be written as a JSON boolean or as its string, and the
        // LaunchDarkly REST API's `_id` is ignored rather than rejected.
        assert_eq!(
            file.segments["legacy-replicas"],
            Segment {
                context_kind: ContextKind::Replica,
                clauses: vec![
                    clause(
                        ScopeAttribute::ReplicaSizeFamily,
                        Operator::In,
                        &["legacy"],
                        false
                    ),
                    clause(ScopeAttribute::IsBuiltin, Operator::In, &["false"], false),
                    clause(
                        ScopeAttribute::ReplicaName,
                        Operator::Matches,
                        &["^scratch-"],
                        true
                    ),
                ],
            }
        );

        // Rules keep the document order that decides which of them wins.
        assert_eq!(
            file.rules
                .iter()
                .map(|rule| rule.segment.as_str())
                .collect::<Vec<_>>(),
            vec!["analytics", "legacy-replicas"]
        );
        assert_eq!(
            file.rules[0].parameters[CLUSTER_PARAM],
            JsonValue::Bool(true)
        );
    }

    /// The `version` key alone selects the format, so a defective version 2
    /// document is rejected rather than read as a flat parameter map.
    #[mz_ore::test]
    fn test_parse_selects_format_by_version_key() {
        assert_eq!(parse(r#"{"version": 2}"#), ConfigFileV2::default());

        // A flat key is not an environment-wide value in version 2.
        let flat_key = parse_error(r#"{"version": 2, "max_connections": 1000}"#);
        assert!(
            flat_key.contains("unknown field `max_connections`"),
            "{flat_key}"
        );

        // Read as a flat map, this would apply `max_connections` and scope
        // nothing, pruning every existing override.
        let broken = parse_error(
            r#"{"version": 2, "global_parameters": {"max_connections": 1000}, "segments": 7}"#,
        );
        assert!(broken.starts_with("segments: invalid type"), "{broken}");

        // Another version is reported as such, ahead of its unknown keys.
        assert_eq!(
            parse_error(r#"{"version": 3, "anything": true}"#),
            "version: unsupported version 3, expected 2"
        );
        let string_version = parse_error(r#"{"version": "2"}"#);
        assert!(
            string_version.starts_with("version: invalid type"),
            "{string_version}"
        );
    }

    /// A document that is not a JSON object carries no information, which is
    /// distinct from an empty document.
    #[mz_ore::test]
    fn test_parse_rejects_non_object_document() {
        assert!(ConfigFileV2::parse("[]").is_err());
        assert!(ConfigFileV2::parse("not json").is_err());
        assert_eq!(parse("{}"), ConfigFileV2::default());
    }

    /// Every structural defect fails the whole document, and the error names the
    /// path to it. Each case is a document with one defect, the path the error
    /// starts with, and a fragment of the message.
    #[mz_ore::test]
    fn test_parse_rejects_structural_defects() {
        let segment = r#""s": {"contextKind": "cluster", "clauses": []}"#;
        let rule = |extra: &str| {
            format!(
                r#"{{"version": 2, "segments": {{{segment}}}, "rules": [{{
                    "segment": "s", "parameters": {{}}{extra}
                }}]}}"#
            )
        };
        let segment_with = |body: &str| format!(r#"{{"version": 2, "segments": {{"s": {body}}}}}"#);
        let cluster_name = r#""attribute": "cluster_name", "op": "in", "values": ["prod"]"#;

        let cases = [
            // Sections of the wrong type.
            (
                r#"{"version": 2, "segments": 7}"#.to_string(),
                "segments",
                "invalid type",
            ),
            (
                r#"{"version": 2, "rules": {}}"#.to_string(),
                "rules",
                "invalid type",
            ),
            (
                r#"{"version": 2, "global_parameters": []}"#.to_string(),
                "global_parameters",
                "invalid type",
            ),
            // Rules.
            (
                r#"{"version": 2, "rules": ["not-a-rule"]}"#.to_string(),
                "rules[0]",
                "invalid type",
            ),
            (
                rule(r#", "priority": 1"#),
                "rules[0]",
                "unknown field `priority`",
            ),
            (
                format!(
                    r#"{{"version": 2, "segments": {{{segment}}}, "rules": [{{"segment": "s"}}]}}"#
                ),
                "rules[0]",
                "missing field `parameters`",
            ),
            (
                format!(
                    r#"{{"version": 2, "segments": {{{segment}}}, "rules": [
                        {{"segment": "s", "parameters": {{}}}},
                        {{"segment": "nope", "parameters": {{}}}}
                    ]}}"#
                ),
                "rules[1] names the segment \"nope\"",
                "which segments does not define",
            ),
            // Segments.
            (
                segment_with(r#"{"clauses": []}"#),
                "segments.s",
                "missing field `contextKind`",
            ),
            (
                segment_with(r#"{"contextKind": "environment", "clauses": []}"#),
                "segments.s.contextKind",
                "unknown variant `environment`",
            ),
            (
                segment_with(r#"{"contextKind": "cluster"}"#),
                "segments.s",
                "missing field `clauses`",
            ),
            (
                segment_with(r#"{"contextKind": "cluster", "clauses": {}}"#),
                "segments.s.clauses",
                "invalid type",
            ),
            (
                segment_with(r#"{"contextKind": "cluster", "clauses": [], "description": "x"}"#),
                "segments.s",
                "unknown field `description`",
            ),
            (
                single_clause_file(&format!(
                    r#"{{{cluster_name}}}, {{
                        "attribute": "replica_size_family", "op": "in", "values": ["legacy"]
                    }}"#
                )),
                "segments.s: clauses[1] names the attribute \"replica_size_family\"",
                "which a \"cluster\" does not carry",
            ),
            // Clauses.
            (
                single_clause_file("7"),
                "segments.s.clauses[0]",
                "invalid type",
            ),
            (
                single_clause_file(r#"{"op": "in", "values": []}"#),
                "segments.s.clauses[0]",
                "missing field `attribute`",
            ),
            (
                single_clause_file(r#"{"attribute": "cluster_nmae", "op": "in", "values": []}"#),
                "segments.s.clauses[0].attribute",
                "unknown variant `cluster_nmae`",
            ),
            (
                single_clause_file(r#"{"attribute": 7, "op": "in", "values": []}"#),
                "segments.s.clauses[0].attribute",
                "invalid type",
            ),
            (
                single_clause_file(r#"{"attribute": "cluster_name", "values": []}"#),
                "segments.s.clauses[0]",
                "missing field `op`",
            ),
            (
                single_clause_file(r#"{"attribute": "cluster_name", "op": "in"}"#),
                "segments.s.clauses[0]",
                "missing field `values`",
            ),
            (
                single_clause_file(
                    r#"{"attribute": "cluster_name", "op": "in", "values": "prod"}"#,
                ),
                "segments.s.clauses[0].values",
                "invalid type",
            ),
            (
                single_clause_file(
                    r#"{"attribute": "cluster_name", "op": "in", "values": ["prod", ["prod"]]}"#,
                ),
                "segments.s.clauses[0].values[1]",
                "expected a string, number or boolean",
            ),
            (
                single_clause_file(&format!(r#"{{{cluster_name}, "negate": "yes"}}"#)),
                "segments.s.clauses[0].negate",
                "invalid type",
            ),
            (
                single_clause_file(&format!(r#"{{{cluster_name}, "nope": 1}}"#)),
                "segments.s.clauses[0]",
                "unknown field `nope`",
            ),
            // The segment declares the context kind for all of its clauses.
            (
                single_clause_file(&format!(r#"{{{cluster_name}, "contextKind": "cluster"}}"#)),
                "segments.s.clauses[0]",
                "unknown field `contextKind`",
            ),
            (
                single_clause_file(
                    r#"{"attribute": "cluster_name", "op": "matches", "values": ["^prod-", "^prod-["]}"#,
                ),
                "segments.s.clauses[0]: values[1] is the invalid \"matches\" pattern \"^prod-[\": ",
                "",
            ),
        ];

        for (document, path, fragment) in cases {
            let error = parse_error(&document);
            assert!(
                error.starts_with(path) && error.contains(fragment),
                "expected an error at {path:?} containing {fragment:?}, got {error:?} for {document}"
            );
        }
    }

    /// The ten LaunchDarkly operators this format refuses are recognised and
    /// refused with their own reason, rather than lumped in with a typo. An
    /// operator that is not LaunchDarkly's at all is refused as unknown.
    #[mz_ore::test]
    fn test_unsupported_operators_rejected() {
        let rejected = |op: &str| {
            parse_error(&single_clause_file(&format!(
                r#"{{"attribute": "cluster_name", "op": "{op}", "values": ["1"]}}"#
            )))
        };
        let unsupported = |op: &str, reason: &str| {
            format!(
                "segments.s.clauses[0].op: the LaunchDarkly operator {op:?} is not supported: it \
                 {reason}"
            )
        };

        for op in [
            "lessThan",
            "lessThanOrEqual",
            "greaterThan",
            "greaterThanOrEqual",
        ] {
            assert_eq!(
                rejected(op),
                unsupported(
                    op,
                    "compares numbers, and every cluster and replica attribute is a string"
                )
            );
        }
        for op in ["before", "after"] {
            assert_eq!(
                rejected(op),
                unsupported(
                    op,
                    "compares dates, and every cluster and replica attribute is a string"
                )
            );
        }
        for op in ["semVerEqual", "semVerGreaterThan", "semVerLessThan"] {
            assert_eq!(
                rejected(op),
                unsupported(
                    op,
                    "compares semantic versions, and every cluster and replica attribute is a \
                     string"
                )
            );
        }
        assert_eq!(
            rejected("segmentMatch"),
            unsupported(
                "segmentMatch",
                "references another segment, which this file expresses through the segment a \
                 rule names"
            )
        );

        // Not a LaunchDarkly operator at all, including the casing a hand-author
        // is most likely to reach for.
        for op in ["starts_with", "IN"] {
            assert_eq!(
                rejected(op),
                format!(
                    "segments.s.clauses[0].op: {op:?} is not an operator, expected one of in, \
                     startsWith, endsWith, contains, matches"
                )
            );
        }
    }

    /// Every LaunchDarkly operator is either supported or refused with a reason.
    /// An operator added to the SDK does not fail this.
    #[mz_ore::test]
    fn test_operator_vocabulary_matches_launchdarkly() {
        let launchdarkly = [
            "in",
            "startsWith",
            "endsWith",
            "contains",
            "matches",
            "lessThan",
            "lessThanOrEqual",
            "greaterThan",
            "greaterThanOrEqual",
            "before",
            "after",
            "segmentMatch",
            "semVerEqual",
            "semVerGreaterThan",
            "semVerLessThan",
        ];
        let supported = Operator::ALL.map(|op| op.as_str());

        for op in launchdarkly {
            assert_eq!(
                Operator::parse(op).is_some(),
                unsupported_operator(op).is_none(),
                "{op:?} must be either supported or refused with a reason, not both or neither"
            );
        }
        for op in supported {
            assert!(launchdarkly.contains(&op), "{op:?} is not a LD operator");
            // The name a diagnostic prints round-trips through the parse.
            assert_eq!(Operator::parse(op).map(|op| op.as_str()), Some(op));
        }
        assert_eq!(supported.len(), 5);
        assert_eq!(launchdarkly.len(), 15);
        // Nothing outside the vocabulary is silently accepted as refusable.
        assert_eq!(unsupported_operator("starts_with"), None);
        assert_eq!(Operator::parse("starts_with"), None);
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_cluster_rule_applied() {
        let params = SynchronizedParameters::default();
        let file = parse(&scoped_file(true));

        let out = file_frontend().file_cluster_overrides(
            &file,
            &params,
            &[CLUSTER_PARAM],
            &[cluster_ctx(1, "analytics"), cluster_ctx(2, "staging")],
        );

        // Sparse: only the cluster the segment matches gets a row.
        assert_eq!(
            out,
            BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "true"))])
        );
    }

    /// The clauses of one segment are ANDed, and a replica may be matched on its
    /// own attributes as well as its owning cluster's.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_replica_rule_applied() {
        let params = SynchronizedParameters::default();
        let file = parse(&format!(
            r#"{{
                "version": 2,
                "segments": {{"legacy-in-prod": {{
                    "contextKind": "replica",
                    "clauses": [
                        {{
                            "attribute": "replica_size_family",
                            "op": "in",
                            "values": ["legacy"]
                        }},
                        {{"attribute": "cluster_name", "op": "in", "values": ["prod"]}}
                    ]
                }}}},
                "rules": [
                    {{"segment": "legacy-in-prod", "parameters": {{"{REPLICA_PARAM}": false}}}}
                ]
            }}"#
        ));

        let out = file_frontend().file_replica_overrides(
            &file,
            &params,
            &[REPLICA_PARAM],
            &[
                legacy_replica_ctx(1, "prod", 1, "r1"),
                // Right cluster, wrong size family.
                replica_ctx(1, "prod", 2, "r2"),
                // Right size family, wrong cluster.
                legacy_replica_ctx(2, "staging", 3, "r1"),
            ],
        );

        assert_eq!(
            out,
            BTreeMap::from([(ReplicaId::User(1), overrides(REPLICA_PARAM, "false"))])
        );
    }

    /// A replica-local parameter may be supplied through a `replica` segment whose
    /// clauses name cluster attributes alone, which targets every replica of that
    /// cluster.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_replica_rule_targeted_by_cluster() {
        let params = SynchronizedParameters::default();
        let file = parse(&format!(
            r#"{{
                "version": 2,
                "segments": {{"prod": {{
                    "contextKind": "replica",
                    "clauses": [
                        {{"attribute": "cluster_name", "op": "in", "values": ["prod"]}}
                    ]
                }}}},
                "rules": [{{"segment": "prod", "parameters": {{"{REPLICA_PARAM}": false}}}}]
            }}"#
        ));

        let out = file_frontend().file_replica_overrides(
            &file,
            &params,
            &[REPLICA_PARAM],
            &[
                replica_ctx(1, "prod", 1, "r1"),
                replica_ctx(1, "prod", 2, "r2"),
                replica_ctx(2, "staging", 3, "r1"),
            ],
        );

        assert_eq!(
            out,
            BTreeMap::from([
                (ReplicaId::User(1), overrides(REPLICA_PARAM, "false")),
                (ReplicaId::User(2), overrides(REPLICA_PARAM, "false")),
            ])
        );
    }

    /// The first rule whose segment matches decides a parameter, and it decides it
    /// before the value is judged, so a value agreeing with the environment-wide
    /// one still shadows a later rule. A rule that does not mention a parameter
    /// leaves it to a later one, and a segment with no clauses matches every
    /// object of its context kind.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_first_matching_rule_wins() {
        let params = SynchronizedParameters::default();
        // Both parameters are off environment-wide, so `true` is an override for
        // either. Asserted so that a default flip fails here rather than quietly
        // weakening the test.
        assert_eq!(params.get(CLUSTER_PARAM), "off");
        assert_eq!(params.get(CLUSTER_PARAM_2), "off");

        let file = parse(&format!(
            r#"{{
                "version": 2,
                "segments": {{
                    "analytics": {{
                        "contextKind": "cluster",
                        "clauses": [
                            {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                        ]
                    }},
                    "every-cluster": {{"contextKind": "cluster", "clauses": []}}
                }},
                "rules": [
                    {{"segment": "analytics", "parameters": {{"{CLUSTER_PARAM}": false}}}},
                    {{"segment": "every-cluster", "parameters": {{
                        "{CLUSTER_PARAM}": true,
                        "{CLUSTER_PARAM_2}": true
                    }}}}
                ]
            }}"#
        ));

        let out = file_frontend().file_cluster_overrides(
            &file,
            &params,
            &[CLUSTER_PARAM, CLUSTER_PARAM_2],
            &[cluster_ctx(1, "analytics"), cluster_ctx(2, "staging")],
        );

        assert_eq!(
            out,
            BTreeMap::from([
                // The first rule pinned `CLUSTER_PARAM` to the environment-wide
                // value, so the catch-all rule does not raise it, but it does
                // still decide the parameter the first rule left alone.
                (ClusterId::User(1), overrides(CLUSTER_PARAM_2, "true")),
                (
                    ClusterId::User(2),
                    BTreeMap::from([
                        (CLUSTER_PARAM.to_string(), "true".to_string()),
                        (CLUSTER_PARAM_2.to_string(), "true".to_string()),
                    ])
                ),
            ])
        );
    }

    /// Each of the five supported operators, over the one attribute every context
    /// kind carries. Values within a clause are ORed.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_clause_operators() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();

        // Every rule supplies the same parameter, and each segment is tested on
        // its own file, so a hit is unambiguous.
        let hits = |op: &str, values: &str, name: &str| {
            let file = parse(&single_clause_file(&format!(
                r#"{{"attribute": "cluster_name", "op": "{op}", "values": {values}}}"#
            )));
            !frontend
                .file_cluster_overrides(&file, &params, &[CLUSTER_PARAM], &[cluster_ctx(1, name)])
                .is_empty()
        };

        assert!(hits("in", r#"["prod"]"#, "prod"));
        assert!(!hits("in", r#"["prod"]"#, "prod-1"));
        // Values are ORed.
        assert!(hits("in", r#"["staging", "prod"]"#, "prod"));

        assert!(hits("startsWith", r#"["prod-"]"#, "prod-ingest"));
        assert!(!hits("startsWith", r#"["prod-"]"#, "staging-prod-ingest"));

        assert!(hits("endsWith", r#"["-prod"]"#, "ingest-prod"));
        assert!(!hits("endsWith", r#"["-prod"]"#, "prod-ingest"));

        assert!(hits("contains", r#"["prod"]"#, "staging-prod-1"));
        assert!(!hits("contains", r#"["prod"]"#, "staging-1"));

        // Unanchored, as the `regex` crate and so LaunchDarkly's `matches` are.
        assert!(hits("matches", r#"["prod"]"#, "staging-prod-1"));
        assert!(hits("matches", r#"["^prod-"]"#, "prod-ingest"));
        assert!(!hits("matches", r#"["^prod-"]"#, "staging-prod-ingest"));
        assert!(hits("matches", r#"["^prod$"]"#, "prod"));
        assert!(!hits("matches", r#"["^prod$"]"#, "prod-1"));
        // Patterns within a clause are ORed too.
        assert!(hits("matches", r#"["^prod-", "^stage-"]"#, "stage-1"));

        // An empty value list satisfies no operator.
        assert!(!hits("in", "[]", "prod"));
        assert!(!hits("matches", "[]", "prod"));
    }

    /// `negate` inverts the clause *after* the OR across its values, as it does in
    /// LaunchDarkly, so a negated `in` means "none of these" rather than "not this
    /// one".
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_clause_negate() {
        let params = SynchronizedParameters::default();
        let file = parse(&single_clause_file(
            r#"{
                "attribute": "cluster_name",
                "op": "in",
                "values": ["scratch", "sandbox"],
                "negate": true
            }"#,
        ));

        let out = file_frontend().file_cluster_overrides(
            &file,
            &params,
            &[CLUSTER_PARAM],
            &[
                cluster_ctx(1, "prod"),
                // Both listed values are excluded, which is what makes this "none
                // of these" rather than "not the first one".
                cluster_ctx(2, "scratch"),
                cluster_ctx(3, "sandbox"),
            ],
        );

        assert_eq!(
            out,
            BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "true"))])
        );
    }

    /// A cluster-coherent parameter may not be supplied through a `replica`
    /// segment: honouring that would let the parameter resolve differently across
    /// one cluster's replicas. The parameter is dropped from that rule, leaving it
    /// to a later one, while a replica-local parameter in the same rule is
    /// unaffected.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_cluster_coherence_guard() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let file = parse(&format!(
            r#"{{
                "version": 2,
                "segments": {{
                    "legacy-replicas": {{
                        "contextKind": "replica",
                        "clauses": [{{
                            "attribute": "replica_size_family",
                            "op": "in",
                            "values": ["legacy"]
                        }}]
                    }},
                    "analytics": {{
                        "contextKind": "cluster",
                        "clauses": [
                            {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                        ]
                    }}
                }},
                "rules": [
                    {{"segment": "legacy-replicas", "parameters": {{
                        "{CLUSTER_PARAM}": true,
                        "{REPLICA_PARAM}": false
                    }}}},
                    {{"segment": "analytics", "parameters": {{"{CLUSTER_PARAM}": true}}}}
                ]
            }}"#
        ));

        assert_eq!(
            frontend.file_cluster_overrides(
                &file,
                &params,
                &[CLUSTER_PARAM],
                &[cluster_ctx(1, "analytics")]
            ),
            BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "true"))])
        );
        assert_eq!(
            frontend.file_replica_overrides(
                &file,
                &params,
                &[REPLICA_PARAM],
                &[legacy_replica_ctx(1, "analytics", 1, "r1")]
            ),
            BTreeMap::from([(ReplicaId::User(1), overrides(REPLICA_PARAM, "false"))])
        );
        assert_eq!(
            frontend.scoped_rule_diagnostics(&file, &params),
            vec![format!(
                "ignoring {CLUSTER_PARAM} for rules[0] (segment \"legacy-replicas\") in the system \
                 parameter sync file: {CLUSTER_PARAM} is cluster-scoped, so it cannot be supplied \
                 through a segment of context kind \"replica\""
            )]
        );
    }

    /// A pattern does not change which rule decides a parameter: the first rule
    /// whose segment matches still wins. Two patterns of which one is the narrower
    /// is the shape that makes this the natural way to write an exception.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_pattern_obeys_first_matching_rule() {
        let params = SynchronizedParameters::default();
        let file = parse(&format!(
            r#"{{
                "version": 2,
                "segments": {{
                    "prod-canary": {{
                        "contextKind": "cluster",
                        "clauses": [{{
                            "attribute": "cluster_name",
                            "op": "matches",
                            "values": ["^prod-canary"]
                        }}]
                    }},
                    "prod": {{
                        "contextKind": "cluster",
                        "clauses": [{{
                            "attribute": "cluster_name",
                            "op": "matches",
                            "values": ["^prod-"]
                        }}]
                    }}
                }},
                "rules": [
                    {{"segment": "prod-canary", "parameters": {{
                        "{CLUSTER_PARAM}": false,
                        "{CLUSTER_PARAM_2}": true
                    }}}},
                    {{"segment": "prod", "parameters": {{"{CLUSTER_PARAM}": true}}}}
                ]
            }}"#
        ));

        let out = file_frontend().file_cluster_overrides(
            &file,
            &params,
            &[CLUSTER_PARAM, CLUSTER_PARAM_2],
            &[cluster_ctx(1, "prod-canary-1"), cluster_ctx(2, "prod-main")],
        );

        assert_eq!(
            out,
            BTreeMap::from([
                // Both patterns match the canary, and the narrower rule comes
                // first, so it holds `CLUSTER_PARAM` at the environment-wide
                // value against the broader rule below. It still decides the
                // parameter the broader rule leaves alone.
                (ClusterId::User(1), overrides(CLUSTER_PARAM_2, "true")),
                (ClusterId::User(2), overrides(CLUSTER_PARAM, "true")),
            ])
        );
    }

    /// A segment that matches nothing live is ignored, not an error: the live
    /// objects drive the resolution, so the file is never a second source of truth
    /// for what exists.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_segment_matching_nothing_live_ignored() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let file = parse(&format!(
            r#"{{
                "version": 2,
                "segments": {{
                    "gone-cluster": {{
                        "contextKind": "cluster",
                        "clauses": [
                            {{"attribute": "cluster_name", "op": "in", "values": ["gone"]}}
                        ]
                    }},
                    "gone-replicas": {{
                        "contextKind": "replica",
                        "clauses": [
                            {{"attribute": "cluster_name", "op": "in", "values": ["gone"]}}
                        ]
                    }}
                }},
                "rules": [
                    {{"segment": "gone-cluster", "parameters": {{"{CLUSTER_PARAM}": true}}}},
                    {{"segment": "gone-replicas", "parameters": {{"{REPLICA_PARAM}": false}}}}
                ]
            }}"#
        ));

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
        let file = parse(&format!(
            r#"{{
                "version": 2,
                "segments": {{"analytics": {{
                    "contextKind": "cluster",
                    "clauses": [
                        {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                    ]
                }}}},
                "rules": [{{"segment": "analytics", "parameters": {{
                    "{CLUSTER_PARAM}": "maybe"
                }}}}]
            }}"#
        ));

        assert!(
            file_frontend()
                .file_cluster_overrides(
                    &file,
                    &params,
                    &[CLUSTER_PARAM],
                    &[cluster_ctx(1, "analytics")]
                )
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
        let file = parse(&scoped_file(false));

        assert!(
            file_frontend()
                .file_cluster_overrides(
                    &file,
                    &params,
                    &[CLUSTER_PARAM],
                    &[cluster_ctx(1, "analytics")]
                )
                .is_empty()
        );
    }

    /// An unreadable or unparseable file, including a structurally defective
    /// version 2 document, skips the reconcile, while create-time evaluation keeps
    /// resolving against the last valid parse.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_read_failure_keeps_scoped_overrides() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let clusters = [cluster_ctx(1, "analytics")];
        let expected = BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "true"))]);

        // A readable file establishes the override.
        sync_tick(&frontend, &params, Ok(scoped_file(true)));
        assert!(frontend.has_scoped_desired_state());
        assert_eq!(
            frontend.pull_cluster_overrides(&params, &[CLUSTER_PARAM], &clusters),
            expected
        );

        for failure in [
            Err(io::Error::from(io::ErrorKind::NotFound)),
            Ok("}not json{".to_string()),
            Ok(scoped_file(true).replace("cluster_name", "cluster_nmae")),
        ] {
            sync_tick(&frontend, &params, failure);
            // The reconcile is skipped wholesale on this signal, which is what
            // leaves the existing durable overrides in place.
            assert!(!frontend.has_scoped_desired_state());
            // The create path, however, still resolves the last valid rules, so a
            // replica created now is not stranded at the environment-wide value.
            assert_eq!(
                frontend.pull_cluster_overrides(&params, &[CLUSTER_PARAM], &clusters),
                expected,
                "a failed read dropped the last valid parse"
            );

            // A readable file again resolves as before.
            sync_tick(&frontend, &params, Ok(scoped_file(true)));
            assert!(frontend.has_scoped_desired_state());
        }

        // An empty document, on the other hand, is a complete desired state of
        // "no overrides", so it does replace the last valid parse and prune.
        sync_tick(&frontend, &params, Ok("{}".to_string()));
        assert!(frontend.has_scoped_desired_state());
        assert!(
            frontend
                .pull_cluster_overrides(&params, &[CLUSTER_PARAM], &clusters)
                .is_empty()
        );
    }

    /// A file's rules reach the scoped passes only once that file's
    /// environment-wide section is published, so a create between the read and
    /// the push resolves against the old file and its baseline.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_rules_are_published_with_their_baseline() {
        let frontend = file_frontend();
        let clusters = [cluster_ctx(1, "analytics")];

        // The baseline before and after the new file's environment-wide section is
        // pushed. The parameter is `off` environment-wide by default.
        let before = SynchronizedParameters::default();
        let mut after = SynchronizedParameters::default();
        assert!(after.modify(CLUSTER_PARAM, "on"));
        assert_eq!(before.get(CLUSTER_PARAM), "off");
        assert_eq!(after.get(CLUSTER_PARAM), "on");

        // The file in force: the rule turns the parameter on for `analytics`
        // while it is off everywhere else.
        sync_tick(&frontend, &before, Ok(scoped_file(true)));
        let established = BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "true"))]);
        assert_eq!(
            frontend.pull_cluster_overrides(&before, &[CLUSTER_PARAM], &clusters),
            established
        );

        // The file is rewritten to turn the parameter on environment-wide and
        // hold `analytics` back at `false`. Read, but not yet pushed.
        let flipped = format!(
            r#"{{
                "version": 2,
                "global_parameters": {{"{CLUSTER_PARAM}": true}},
                "segments": {{"analytics": {{
                    "contextKind": "cluster",
                    "clauses": [
                        {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                    ]
                }}}},
                "rules": [
                    {{"segment": "analytics", "parameters": {{"{CLUSTER_PARAM}": false}}}}
                ]
            }}"#
        );
        frontend.refresh_config_file(Path::new(CONFIG_PATH), Ok(flipped), &before);

        // Still the old file's answer. Resolving the new rules here would compare
        // `false` against the not-yet-pushed `off`, call it "matches the
        // environment", and record nothing. The push would then take
        // `analytics` to `on`, the one value its rule forbids.
        assert_eq!(
            frontend.pull_cluster_overrides(&before, &[CLUSTER_PARAM], &clusters),
            established,
            "a read was published to the scoped passes ahead of its baseline"
        );

        // Once the push has landed, the new rules resolve against the baseline
        // they were written for.
        frontend.publish_config_file();
        assert_eq!(
            frontend.pull_cluster_overrides(&after, &[CLUSTER_PARAM], &clusters),
            BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "false"))])
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_config_file_cached_until_it_changes() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let read = |contents: &str| {
            frontend
                .refresh_config_file(Path::new(CONFIG_PATH), Ok(contents.to_string()), &params)
                .expect("document parses")
        };

        let first = read(&scoped_file(true));
        let again = read(&scoped_file(true));
        assert!(Arc::ptr_eq(&first, &again), "unchanged file was re-parsed");

        let changed = read(&scoped_file(false));
        assert!(
            !Arc::ptr_eq(&first, &changed),
            "changed file was not parsed"
        );
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_unreadable_file_has_no_scoped_desired_state() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let refresh = |read: io::Result<String>| {
            frontend.refresh_config_file(Path::new(CONFIG_PATH), read, &params);
        };

        refresh(Ok(r#"{"max_connections": 1000}"#.to_string()));
        assert!(frontend.has_scoped_desired_state());

        for failure in [
            Err(io::Error::from(io::ErrorKind::NotFound)),
            Ok("}not json{".to_string()),
            Ok("[]".to_string()),
            Ok(r#"{"version": 2, "segments": 7}"#.to_string()),
        ] {
            refresh(failure);
            assert!(!frontend.has_scoped_desired_state());

            refresh(Ok(r#"{"max_connections": 1000}"#.to_string()));
            assert!(frontend.has_scoped_desired_state());
        }

        // An empty document is a complete desired state of "no overrides".
        refresh(Ok("{}".to_string()));
        assert!(frontend.has_scoped_desired_state());
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_pull_from_file() {
        let path = std::env::temp_dir().join(format!(
            "mz-adapter-test-pull-from-file-{}.json",
            std::process::id()
        ));
        let frontend = file_frontend_at(&path);
        let mut params = SynchronizedParameters::default();
        let max_connections = params.get("max_connections");
        assert_ne!(max_connections, "1000");

        fs::write(
            &path,
            r#"{"max_connections": 1000, "enable_lgalloc": false, "max_tables": null}"#,
        )
        .expect("can write the test file");
        assert!(frontend.pull(&mut params));
        assert_eq!(params.get("max_connections"), "1000");
        assert_eq!(params.get("enable_lgalloc"), "off");
        assert_eq!(
            params.get("max_tables"),
            SynchronizedParameters::default().get("max_tables"),
            "null changed the value"
        );

        // A version 2 document supplies the same values from `global_parameters`.
        fs::write(
            &path,
            r#"{"version": 2, "global_parameters": {"max_connections": 2000}}"#,
        )
        .expect("can write the test file");
        assert!(frontend.pull(&mut params));
        assert_eq!(params.get("max_connections"), "2000");

        // Unparseable, structurally defective or missing, the file leaves every
        // current value alone.
        for contents in [
            "}not json{",
            r#"{"version": 2, "global_parameters": {"max_connections": 5}, "segments": 7}"#,
        ] {
            fs::write(&path, contents).expect("can write the test file");
            assert!(!frontend.pull(&mut params));
            assert!(!frontend.has_scoped_desired_state());
        }
        fs::remove_file(&path).expect("can remove the test file");
        assert!(!frontend.pull(&mut params));
        assert!(!frontend.has_scoped_desired_state());
        assert_eq!(params.get("max_connections"), "2000");
        assert_eq!(params.get("enable_lgalloc"), "off");
    }

    /// The mistakes in a rule that depend on this binary's parameter set are
    /// diagnosed rather than failing the document. Resolution drops each of them,
    /// and nothing surfaces a parameter's scope from SQL, which leaves an operator
    /// nothing else to debug against.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_diagnoses_rule_mistakes() {
        let params = SynchronizedParameters::default();
        let file = parse(&format!(
            r#"{{
                "version": 2,
                "segments": {{"analytics": {{
                    "contextKind": "cluster",
                    "clauses": [
                        {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                    ]
                }}}},
                "rules": [
                    {{"segment": "analytics", "parameters": {{}}}},
                    {{"segment": "analytics", "parameters": {{
                        "max_connections": 100,
                        "enabel_eager_delta_joins": true,
                        "{CLUSTER_PARAM}": "maybe"
                    }}}}
                ]
            }}"#
        ));

        assert_eq!(
            file_frontend().scoped_rule_diagnostics(&file, &params),
            // Ordered by rule, then by parameter name within a rule.
            vec![
                // A misspelled parameter name.
                "ignoring enabel_eager_delta_joins for rules[1] (segment \"analytics\") in the \
                 system parameter sync file: not a cluster-scoped or replica-scoped system \
                 parameter"
                    .to_string(),
                // A value that does not parse for the parameter's type.
                format!(
                    "ignoring unparseable value \"maybe\" for system parameter {CLUSTER_PARAM} \
                     on rules[1] (segment \"analytics\") in the system parameter sync file"
                ),
                // A parameter that carries no scope at all, so it can only be set
                // environment-wide.
                "ignoring max_connections for rules[1] (segment \"analytics\") in the system \
                 parameter sync file: not a cluster-scoped or replica-scoped system parameter"
                    .to_string(),
            ]
        );
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
