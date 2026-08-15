// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
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
use regex::Regex;
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

/// Reserved top-level key of the config-sync file holding the segment
/// definitions, keyed by segment name.
const SEGMENTS_SECTION: &str = "segments";
/// Reserved top-level key of the config-sync file holding the ordered rules.
const RULES_SECTION: &str = "rules";
/// Key of a `rules` element naming the segment whose objects it applies to.
const RULE_SEGMENT: &str = "segment";
/// Key of a `rules` element holding the parameters it supplies.
const RULE_PARAMETERS: &str = "parameters";
/// Key of a [`Segment`] predicate entry listing the values the attribute may
/// equal. LaunchDarkly's `in` operator.
const IN_OPERATOR: &str = "in";
/// Key of a [`Segment`] predicate entry listing the regular expressions the
/// attribute's value may match. LaunchDarkly's `matches` operator.
const MATCHES_OPERATOR: &str = "matches";

/// The parsed contents of the config-sync file.
///
/// The file is a JSON object whose keys are parameter names, except for the two
/// reserved section keys [`SEGMENTS_SECTION`] and [`RULES_SECTION`]. A file
/// carrying neither reserved key is therefore a flat, wholly environment-wide
/// parameter map.
///
/// No synced system parameter may be named `segments` or `rules`, or the
/// reserved section would shadow it.
/// `test_no_synced_parameter_shadows_a_reserved_section` enforces that.
#[derive(Debug, Default, PartialEq)]
struct ConfigFile {
    /// Environment-wide values, keyed by the parameter's external name.
    environment: BTreeMap<String, JsonValue>,
    /// The predicates rules select objects with, keyed by segment name.
    segments: BTreeMap<String, Segment>,
    /// The rules, in the document order the file lists them in. The first rule
    /// whose segment matches an object decides each parameter it supplies, so the
    /// order is load-bearing and an array is the only shape that carries it: a
    /// JSON object's key order is lost on parse.
    rules: Vec<Rule>,
}

impl ConfigFile {
    /// Parses the config-sync file's contents, or `None` if the document is not a
    /// JSON object.
    ///
    /// Individual sections are parsed leniently: a section, segment, rule, or
    /// value of the wrong shape is dropped with a warning rather than failing the
    /// parse, so one bad entry cannot strand the rest of the file.
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
                SEGMENTS_SECTION => {
                    file.segments = as_object(FilePosition::Section(SEGMENTS_SECTION), value)
                        .into_iter()
                        .filter_map(|(name, predicate)| {
                            let segment = Segment::parse(FilePosition::Segment(&name), predicate)?;
                            Some((name, segment))
                        })
                        .collect();
                }
                RULES_SECTION => {
                    file.rules = as_array(FilePosition::Section(RULES_SECTION), value)
                        .into_iter()
                        .enumerate()
                        .filter_map(|(index, rule)| Rule::parse(index + 1, rule))
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

/// An attribute of a cluster or replica that a [`Segment`] predicate matches on.
///
/// The vocabulary is closed, and is the same one the LaunchDarkly `cluster` and
/// `replica` context kinds carry (see [`cluster_context`] and
/// [`replica_context`]), so that a segment expresses what a LaunchDarkly rule
/// expresses and the file can stand in for LaunchDarkly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
    /// The attribute of this name, or `None` if the name is outside the
    /// vocabulary.
    fn parse(name: &str) -> Option<Self> {
        match name {
            "cluster_id" => Some(Self::ClusterId),
            "cluster_name" => Some(Self::ClusterName),
            "is_builtin" => Some(Self::IsBuiltin),
            "replica_id" => Some(Self::ReplicaId),
            "replica_name" => Some(Self::ReplicaName),
            "replica_size" => Some(Self::ReplicaSize),
            "replica_size_family" => Some(Self::ReplicaSizeFamily),
            _ => None,
        }
    }

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
    ///
    /// A [`ParameterScope::Cluster`] parameter may not be supplied through a
    /// segment matching on one. See
    /// [`SystemParameterFrontend::file_rule_overrides`].
    fn is_replica_attribute(&self) -> bool {
        match self {
            Self::ClusterId | Self::ClusterName | Self::IsBuiltin => false,
            Self::ReplicaId | Self::ReplicaName | Self::ReplicaSize | Self::ReplicaSizeFamily => {
                true
            }
        }
    }
}

/// The attributes a cluster is matched against, mirroring [`cluster_context`].
///
/// Deliberately replica-free: a cluster-coherent parameter must resolve
/// identically across the cluster's replicas.
fn cluster_attributes(cluster: &ClusterScopeContext) -> BTreeMap<ScopeAttribute, String> {
    BTreeMap::from([
        (ScopeAttribute::ClusterId, cluster.id.clone()),
        (ScopeAttribute::ClusterName, cluster.name.clone()),
        (ScopeAttribute::IsBuiltin, cluster.is_builtin.to_string()),
    ])
}

/// The attributes a replica is matched against, mirroring [`replica_context`].
///
/// Carries the owning cluster's attributes too, so a replica-local parameter can
/// be targeted by cluster alone.
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

/// A named predicate over a cluster's or replica's scope attributes.
///
/// Attributes are ANDed and what one attribute allows is ORed. That covers
/// LaunchDarkly's `in` and `matches` operators and nothing more: there is no
/// negation operator.
#[derive(Debug, Default, PartialEq, Eq)]
struct Segment {
    /// What each attribute the predicate constrains allows. An empty predicate
    /// constrains nothing, so it matches every object.
    attributes: BTreeMap<ScopeAttribute, AttributePredicate>,
    /// The predicate entries this binary cannot evaluate, keyed by the attribute
    /// name the file spells and reported by
    /// [`SystemParameterFrontend::scoped_rule_diagnostics`].
    ///
    /// A segment with any such entry matches nothing, so the rules naming it
    /// never apply. Fail-safe on purpose: dropping the entry instead would leave
    /// the surviving ANDed attributes matching a *wider* set of objects than the
    /// operator wrote, and a predicate whose every entry was dropped would match
    /// everything.
    rejected: BTreeMap<String, RejectedAttribute>,
}

/// What one attribute of a [`Segment`] predicate allows.
///
/// The two operators are ORed, as are the entries within either, so an
/// attribute matches when its value equals one of [`Self::values`] or is
/// matched by one of [`Self::patterns`]. An attribute allowing neither matches
/// nothing.
#[derive(Debug, Default)]
struct AttributePredicate {
    /// The values the attribute may equal, the `in` operator.
    values: BTreeSet<String>,
    /// The patterns the attribute's value may match, the `matches` operator.
    ///
    /// Compiled here, when the file is parsed, because evaluation runs per
    /// object per sync tick and per object creation, while a parse happens only
    /// when the file changes.
    patterns: Vec<Regex>,
}

impl AttributePredicate {
    /// Whether the attribute's `value` is allowed.
    fn matches(&self, value: &str) -> bool {
        self.values.contains(value) || self.patterns.iter().any(|pattern| pattern.is_match(value))
    }
}

/// Compares the patterns by the source they were compiled from, a [`Regex`]
/// being uncomparable itself. Two predicates equal under this decide the same
/// values.
impl PartialEq for AttributePredicate {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
            && self.patterns.len() == other.patterns.len()
            && std::iter::zip(&self.patterns, &other.patterns)
                .all(|(a, b)| a.as_str() == b.as_str())
    }
}

impl Eq for AttributePredicate {}

/// Why a [`Segment`] predicate entry cannot be evaluated.
#[derive(Debug, PartialEq, Eq)]
enum RejectedAttribute {
    /// The name is outside the [`ScopeAttribute`] vocabulary.
    UnknownAttribute,
    /// The entry is neither an array of JSON scalars nor an operator object of
    /// those, so it cannot be read as a predicate at all. Only those shapes are
    /// accepted, so that a value spelled as some other shape, for instance an
    /// operator this binary predates, is rejected rather than misread.
    UnsupportedValues,
    /// The operator object carries a key that is not an operator this binary
    /// knows, for the same reason an unknown attribute name is rejected: it
    /// expresses a constraint that cannot be honoured.
    UnknownOperator(String),
    /// A `matches` entry is not a valid regular expression, so the constraint it
    /// expresses is unknown.
    InvalidPattern { pattern: String, error: String },
}

impl fmt::Display for RejectedAttribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RejectedAttribute::UnknownAttribute => {
                write!(f, "is not a cluster or replica attribute")
            }
            RejectedAttribute::UnsupportedValues => {
                write!(
                    f,
                    "expects an array of strings, numbers or booleans, or an object of an \
                     {IN_OPERATOR:?} array and a {MATCHES_OPERATOR:?} array of patterns"
                )
            }
            RejectedAttribute::UnknownOperator(operator) => {
                write!(
                    f,
                    "names {operator:?}, which is not one of the {IN_OPERATOR:?} and \
                     {MATCHES_OPERATOR:?} operators"
                )
            }
            RejectedAttribute::InvalidPattern { pattern, error } => {
                write!(
                    f,
                    "has the invalid {MATCHES_OPERATOR:?} pattern {pattern:?}: {error}"
                )
            }
        }
    }
}

impl Segment {
    /// Parses one entry of the `segments` section, or `None` if its value is not
    /// a JSON object.
    ///
    /// A predicate entry that cannot be evaluated is kept as a
    /// [`RejectedAttribute`] rather than dropped, so that the segment matches
    /// nothing and the diagnostics can name it.
    fn parse(position: FilePosition<'_>, value: JsonValue) -> Option<Self> {
        let predicate = match value {
            JsonValue::Object(predicate) => predicate,
            other => {
                warn!(
                    "ignoring {position} in system parameter sync file: expected a JSON object, found {}",
                    json_type_name(&other)
                );
                return None;
            }
        };

        let mut segment = Self::default();
        for (name, value) in predicate {
            let Some(attribute) = ScopeAttribute::parse(&name) else {
                segment
                    .rejected
                    .insert(name, RejectedAttribute::UnknownAttribute);
                continue;
            };
            match attribute_predicate(&value) {
                Ok(predicate) => {
                    segment.attributes.insert(attribute, predicate);
                }
                Err(rejected) => {
                    segment.rejected.insert(name, rejected);
                }
            }
        }
        Some(segment)
    }

    /// Whether the predicate holds for an object carrying `attributes`.
    ///
    /// An attribute the object does not carry never matches, so a predicate
    /// constraining a replica attribute holds for no cluster: a cluster's
    /// attributes are replica-free.
    fn matches(&self, attributes: &BTreeMap<ScopeAttribute, String>) -> bool {
        self.rejected.is_empty()
            && self.attributes.iter().all(|(attribute, allowed)| {
                attributes
                    .get(attribute)
                    .is_some_and(|value| allowed.matches(value))
            })
    }

    /// A replica attribute the predicate constrains, if any.
    fn replica_attribute(&self) -> Option<ScopeAttribute> {
        self.attributes
            .keys()
            .copied()
            .find(|attribute| attribute.is_replica_attribute())
    }
}

/// One element of the `rules` array: the parameters to apply to the objects a
/// segment matches.
#[derive(Debug, PartialEq)]
struct Rule {
    /// The rule's 1-based position in the `rules` array, named in diagnostics.
    /// Recorded rather than derived from the parsed order so that a malformed
    /// element, which is dropped, does not renumber the rules after it.
    ordinal: usize,
    /// The name of the [`Segment`] selecting the objects this rule applies to.
    segment: String,
    /// The values this rule supplies, keyed by the parameter's external name.
    parameters: BTreeMap<String, JsonValue>,
}

impl Rule {
    /// Parses the `ordinal`th element of the `rules` array, or `None` if it is
    /// not an object carrying a segment name and a parameter object.
    fn parse(ordinal: usize, value: JsonValue) -> Option<Self> {
        let position = FilePosition::Rule {
            ordinal,
            segment: None,
        };
        let mut rule = match value {
            JsonValue::Object(rule) => rule,
            other => {
                warn!(
                    "ignoring {position} in system parameter sync file: expected a JSON object, found {}",
                    json_type_name(&other)
                );
                return None;
            }
        };

        let segment = match rule.remove(RULE_SEGMENT) {
            Some(JsonValue::String(segment)) => segment,
            other => {
                warn!(
                    "ignoring {position} in system parameter sync file: expected a {RULE_SEGMENT:?} \
                     name, found {}",
                    other.as_ref().map_or("nothing", json_type_name)
                );
                return None;
            }
        };
        let parameters = match rule.remove(RULE_PARAMETERS) {
            Some(JsonValue::Object(parameters)) => parameters.into_iter().collect(),
            other => {
                warn!(
                    "ignoring {position} in system parameter sync file: expected a \
                     {RULE_PARAMETERS:?} object, found {}",
                    other.as_ref().map_or("nothing", json_type_name)
                );
                return None;
            }
        };

        Some(Self {
            ordinal,
            segment,
            parameters,
        })
    }

    /// The rule's position, for a diagnostic about it.
    fn position(&self) -> FilePosition<'_> {
        FilePosition::Rule {
            ordinal: self.ordinal,
            segment: Some(&self.segment),
        }
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

/// A position in the config-sync file, naming it in a diagnostic about what is
/// written there.
enum FilePosition<'a> {
    /// A reserved top-level section.
    Section(&'a str),
    /// One entry of the `segments` section.
    Segment(&'a str),
    /// One element of the `rules` array, by its 1-based position and, once
    /// parsed, the segment it names.
    Rule {
        ordinal: usize,
        segment: Option<&'a str>,
    },
}

impl fmt::Display for FilePosition<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilePosition::Section(name) => write!(f, "the {name} section"),
            FilePosition::Segment(name) => write!(f, "segment {name:?}"),
            FilePosition::Rule {
                ordinal,
                segment: None,
            } => write!(f, "rule {ordinal}"),
            FilePosition::Rule {
                ordinal,
                segment: Some(segment),
            } => write!(f, "rule {ordinal} (segment {segment:?})"),
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

/// Interprets `value` as a JSON array, or warns and yields an empty vector.
fn as_array(position: FilePosition<'_>, value: JsonValue) -> Vec<JsonValue> {
    match value {
        JsonValue::Array(values) => values,
        other => {
            warn!(
                "ignoring {position} in system parameter sync file: expected a JSON array, found {}",
                json_type_name(&other)
            );
            Vec::new()
        }
    }
}

/// The predicate a [`Segment`] entry expresses, or why it cannot be evaluated.
///
/// The entry is written either as a bare array of the values the attribute may
/// equal, or as an object of the [`IN_OPERATOR`] and [`MATCHES_OPERATOR`]
/// operators, each optional. A bare array is the object carrying only `in`.
///
/// An attribute with more than one problem is reported for the first one found,
/// which is enough to make its segment match nothing.
fn attribute_predicate(value: &JsonValue) -> Result<AttributePredicate, RejectedAttribute> {
    let operators: BTreeMap<&str, &JsonValue> = match value {
        JsonValue::Array(_) => BTreeMap::from([(IN_OPERATOR, value)]),
        JsonValue::Object(operators) => operators
            .iter()
            .map(|(operator, value)| (operator.as_str(), value))
            .collect(),
        _ => return Err(RejectedAttribute::UnsupportedValues),
    };

    let mut predicate = AttributePredicate::default();
    for (operator, value) in operators {
        match operator {
            IN_OPERATOR => predicate.values = predicate_values(value)?,
            MATCHES_OPERATOR => predicate.patterns = predicate_patterns(value)?,
            operator => {
                return Err(RejectedAttribute::UnknownOperator(operator.to_string()));
            }
        }
    }
    Ok(predicate)
}

/// The values an `in` operator allows, or a rejection if `value` is not an array
/// of JSON scalars.
///
/// Rendered to strings because that is how scope attributes are spelled, so a
/// boolean attribute may be written either as `true` or as `"true"`.
fn predicate_values(value: &JsonValue) -> Result<BTreeSet<String>, RejectedAttribute> {
    let JsonValue::Array(values) = value else {
        return Err(RejectedAttribute::UnsupportedValues);
    };
    values
        .iter()
        .map(scalar_string)
        .collect::<Option<_>>()
        .ok_or(RejectedAttribute::UnsupportedValues)
}

/// The patterns a `matches` operator allows, or a rejection if `value` is not an
/// array of strings or one of them is not a valid regular expression.
///
/// A pattern is unanchored, as it is in LaunchDarkly and in the RE2 syntax
/// LaunchDarkly's `matches` uses, so it matches anywhere in the attribute's
/// value. `^` and `$` are how a whole-value match is asked for.
///
/// Only strings are accepted: a number or boolean spelled here is a value rather
/// than a pattern, so accepting it would quietly read a typo as a regex.
fn predicate_patterns(value: &JsonValue) -> Result<Vec<Regex>, RejectedAttribute> {
    let JsonValue::Array(values) = value else {
        return Err(RejectedAttribute::UnsupportedValues);
    };
    values
        .iter()
        .map(|value| {
            let JsonValue::String(pattern) = value else {
                return Err(RejectedAttribute::UnsupportedValues);
            };
            Regex::new(pattern).map_err(|e| RejectedAttribute::InvalidPattern {
                pattern: pattern.clone(),
                // The regex crate renders a parse error as a multi-line block
                // that points at the offending character. Collapsed so that the
                // warning this ends up in stays one log line.
                error: e
                    .to_string()
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" "),
            })
        })
        .collect()
}

/// Renders a JSON scalar as the string a scope attribute is compared against, or
/// `None` for a composite value or `null`.
fn scalar_string(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::String(v) => Some(v.clone()),
        JsonValue::Number(v) => Some(v.to_string()),
        JsonValue::Bool(v) => Some(v.to_string()),
        JsonValue::Object(_) | JsonValue::Array(_) | JsonValue::Null => None,
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
            for diagnostic in self.scoped_rule_diagnostics(file, params) {
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

    /// The problems with `file`'s segments and rules that an operator can act on:
    /// a predicate entry that cannot be evaluated, a rule naming a segment that
    /// does not exist, a parameter that is not scopable at all, a cluster-scoped
    /// parameter supplied through a replica-discriminating segment, and a value
    /// that does not parse for its parameter's type. Resolution drops each of
    /// these silently, and nothing surfaces a parameter's scope from SQL, so
    /// without this an operator has nothing to debug against.
    ///
    /// Returned rather than logged so that [`Self::refresh_config_file`] can log
    /// them only when the file changes.
    fn scoped_rule_diagnostics(
        &self,
        file: &ConfigFile,
        params: &SynchronizedParameters,
    ) -> Vec<String> {
        let mut diagnostics = Vec::new();

        for (name, segment) in &file.segments {
            for (attribute, rejected) in &segment.rejected {
                diagnostics.push(format!(
                    "{} in the system parameter sync file matches no cluster or replica: \
                     attribute {attribute:?} {rejected}",
                    FilePosition::Segment(name)
                ));
            }
        }

        let scopable = self.scopable_params(params);
        for rule in &file.rules {
            let Some(segment) = file.segments.get(&rule.segment) else {
                diagnostics.push(format!(
                    "ignoring rule {} in the system parameter sync file: no segment named {:?}",
                    rule.ordinal, rule.segment
                ));
                continue;
            };
            let position = rule.position();
            let replica_attribute = segment.replica_attribute();

            for (name, value) in &rule.parameters {
                let Some(&(param_name, scope)) = scopable.get(name.as_str()) else {
                    diagnostics.push(format!(
                        "ignoring {name} for {position} in the system parameter sync file: \
                         not a cluster-scoped or replica-scoped system parameter"
                    ));
                    continue;
                };
                // The coherence guard, see [`Self::file_rule_overrides`].
                if scope == ParameterScope::Cluster {
                    if let Some(attribute) = replica_attribute {
                        diagnostics.push(format!(
                            "ignoring {param_name} for {position} in the system parameter sync \
                             file: {param_name} is cluster-scoped, so it cannot be supplied \
                             through a segment matching on the replica attribute {}",
                            attribute.as_str()
                        ));
                        continue;
                    }
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

    /// Resolves the cluster-coherent overrides `file`'s rules declare for each of
    /// `clusters`.
    ///
    /// The live clusters drive the resolution, so a segment that matches nothing
    /// live simply never applies.
    fn file_cluster_overrides(
        &self,
        file: &ConfigFile,
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
    ///
    /// The live replicas drive the resolution, so a segment that matches nothing
    /// live simply never applies.
    fn file_replica_overrides(
        &self,
        file: &ConfigFile,
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
                &replica_attributes(&replica.replica),
            );
            if !overrides.is_empty() {
                out.insert(replica.replica_id, overrides);
            }
        }
        out
    }

    /// Resolves one object's scoped overrides from `file`'s rules, given the
    /// object's scope `attributes` and the `scope` that every parameter in
    /// `param_names` declares.
    ///
    /// The first rule whose segment matches the object and that mentions a
    /// parameter decides that parameter. A parameter no matching rule mentions
    /// carries no scoped opinion, so it is absent from the result and resolves to
    /// the environment-wide value, as does one whose deciding value matches the
    /// environment-wide value or does not parse. The parseability and
    /// differs-from-environment rules are the LaunchDarkly path's.
    ///
    /// Silent, as this runs on every tick and for every create: the
    /// operator-facing diagnostics are [`Self::scoped_rule_diagnostics`], reported
    /// once per change to the file.
    fn file_rule_overrides(
        &self,
        file: &ConfigFile,
        params: &SynchronizedParameters,
        param_names: &[&'static str],
        scope: ParameterScope,
        attributes: &BTreeMap<ScopeAttribute, String>,
    ) -> BTreeMap<String, String> {
        let requested: BTreeMap<&str, &'static str> = param_names
            .iter()
            .map(|&param_name| (self.external_name(param_name), param_name))
            .collect();

        let mut decided: BTreeMap<&'static str, String> = BTreeMap::new();
        for rule in &file.rules {
            let Some(segment) = file.segments.get(&rule.segment) else {
                continue;
            };
            if !segment.matches(attributes) {
                continue;
            }
            // The coherence guard. A cluster-coherent parameter must resolve
            // identically across a cluster's replicas, which a segment matching on
            // a replica attribute cannot promise, so such a rule supplies no
            // cluster-scoped parameter. The match above already fails for such a
            // segment, since a cluster carries no replica attributes, but the
            // guard is explicit so that the invariant does not rest on
            // `cluster_attributes` staying replica-free. Its operator-facing half
            // is the matching diagnostic.
            if scope == ParameterScope::Cluster && segment.replica_attribute().is_some() {
                continue;
            }

            for (name, value) in &rule.parameters {
                let Some(&param_name) = requested.get(name.as_str()) else {
                    continue;
                };
                // First match wins, and it wins before the value is judged:
                // whether an override lands must not depend on a fallthrough that
                // only a malformed value could trigger.
                if decided.contains_key(param_name) {
                    continue;
                }
                // `null` expresses no opinion rather than a value, exactly as at
                // the top level, so it leaves the parameter to a later rule.
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
    /// A second cluster-coherent `bool` parameter, also `off` environment-wide,
    /// for the tests that need two parameters to observe rule ordering.
    const CLUSTER_PARAM_2: &str = "enable_join_prioritize_arranged";
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

    /// The predicate an exact-value entry parses to.
    fn exact<const N: usize>(values: [&str; N]) -> AttributePredicate {
        AttributePredicate {
            values: values.into_iter().map(str::to_string).collect(),
            patterns: Vec::new(),
        }
    }

    /// A document whose single rule gives the `analytics` cluster `value` for
    /// [`CLUSTER_PARAM`].
    fn scoped_file(value: bool) -> String {
        format!(
            r#"{{
                "segments": {{"analytics": {{"cluster_name": ["analytics"]}}}},
                "rules": [
                    {{"segment": "analytics", "parameters": {{"{CLUSTER_PARAM}": {value}}}}}
                ]
            }}"#
        )
    }

    /// A file with no reserved section is a plain environment-wide parameter
    /// map, the flat form a config map without segments and rules takes.
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
        assert!(file.segments.is_empty());
        assert!(file.rules.is_empty());

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
    fn test_parse_segments_and_rules() {
        let file = parse(
            r#"{
                "enable_lgalloc": false,
                "segments": {
                    "analytics": {"cluster_name": ["analytics", "analytics_2"]},
                    "legacy-replicas": {"replica_size_family": ["legacy"], "is_builtin": [false]}
                },
                "rules": [
                    {"segment": "analytics", "parameters": {"enable_eager_delta_joins": true}},
                    {"segment": "legacy-replicas", "parameters": {"enable_lgalloc": true}}
                ]
            }"#,
        );

        // The reserved keys are sections, every other key is environment-wide.
        assert_eq!(
            file.environment.keys().collect::<Vec<_>>(),
            vec!["enable_lgalloc"]
        );

        // A boolean attribute may be written as a JSON boolean or as its string:
        // both render to the string the attribute is compared against.
        assert_eq!(
            file.segments["analytics"].attributes,
            BTreeMap::from([(
                ScopeAttribute::ClusterName,
                exact(["analytics", "analytics_2"])
            )])
        );
        assert_eq!(
            file.segments["legacy-replicas"].attributes,
            BTreeMap::from([
                (ScopeAttribute::IsBuiltin, exact(["false"])),
                (ScopeAttribute::ReplicaSizeFamily, exact(["legacy"])),
            ])
        );

        // Rules keep the document order that decides which of them wins.
        assert_eq!(
            file.rules
                .iter()
                .map(|rule| (rule.ordinal, rule.segment.as_str()))
                .collect::<Vec<_>>(),
            vec![(1, "analytics"), (2, "legacy-replicas")]
        );
        assert_eq!(
            file.rules[0].parameters[CLUSTER_PARAM],
            JsonValue::Bool(true)
        );
    }

    /// One malformed segment or rule must not strand the rest of the file, and
    /// dropping a rule must not renumber the rules after it.
    #[mz_ore::test]
    fn test_parse_ignores_malformed_segments_and_rules() {
        let file = parse(
            r#"{
                "max_connections": 1000,
                "segments": {"broken": 7, "prod": {"cluster_name": ["prod"]}},
                "rules": [
                    "not-a-rule",
                    {"parameters": {"enable_lgalloc": true}},
                    {"segment": "prod", "parameters": {"enable_lgalloc": true}}
                ]
            }"#,
        );

        assert_eq!(
            file.environment.keys().collect::<Vec<_>>(),
            vec!["max_connections"]
        );
        assert_eq!(
            file.segments.keys().collect::<Vec<_>>(),
            vec!["prod"],
            "non-object segment dropped"
        );
        assert_eq!(file.rules.len(), 1);
        assert_eq!(
            file.rules[0].ordinal, 3,
            "surviving rule keeps its position in the file"
        );

        // A section of the wrong shape drops that section only.
        let sections = parse(r#"{"max_connections": 1000, "segments": 7, "rules": {}}"#);
        assert!(sections.segments.is_empty());
        assert!(sections.rules.is_empty());
        assert_eq!(
            sections.environment.keys().collect::<Vec<_>>(),
            vec!["max_connections"]
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

    /// The attributes of one segment are ANDed, and a replica may be matched on
    /// its own attributes as well as its owning cluster's.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_replica_rule_applied() {
        let params = SynchronizedParameters::default();
        let file = parse(&format!(
            r#"{{
                "segments": {{"legacy-in-prod": {{
                    "replica_size_family": ["legacy"],
                    "cluster_name": ["prod"]
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

    /// A replica-local parameter may be supplied through a segment matching on
    /// cluster attributes alone, which targets every replica of that cluster.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_file_replica_rule_targeted_by_cluster() {
        let params = SynchronizedParameters::default();
        let file = parse(&format!(
            r#"{{
                "segments": {{"prod": {{"cluster_name": ["prod"]}}}},
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
    /// leaves it to a later one, and a segment with an empty predicate matches
    /// every object.
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
                "segments": {{
                    "analytics": {{"cluster_name": ["analytics"]}},
                    "everything": {{}}
                }},
                "rules": [
                    {{"segment": "analytics", "parameters": {{"{CLUSTER_PARAM}": false}}}},
                    {{"segment": "everything", "parameters": {{
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

    /// A cluster-coherent parameter may not be supplied through a segment that
    /// matches on a replica attribute: honouring that would let the parameter
    /// resolve differently across one cluster's replicas. The parameter is
    /// dropped from that rule, leaving it to a later one, while a replica-local
    /// parameter in the same rule is unaffected.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_cluster_coherence_guard() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let file = parse(&format!(
            r#"{{
                "segments": {{
                    "legacy-replicas": {{"replica_size_family": ["legacy"]}},
                    "analytics": {{"cluster_name": ["analytics"]}}
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
                "ignoring {CLUSTER_PARAM} for rule 1 (segment \"legacy-replicas\") in the system \
                 parameter sync file: {CLUSTER_PARAM} is cluster-scoped, so it cannot be supplied \
                 through a segment matching on the replica attribute replica_size_family"
            )]
        );
    }

    /// A predicate entry this binary cannot evaluate makes its segment match
    /// nothing. Dropping the entry instead would widen the segment, in the limit
    /// to every cluster and replica.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_uninterpretable_predicate_matches_nothing() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let file = parse(&format!(
            r#"{{
                "segments": {{
                    "typo": {{"cluster_nmae": ["analytics"]}},
                    "unlisted": {{"cluster_name": "analytics"}},
                    "unknown-operator": {{"cluster_name": {{"starts_with": ["analytics"]}}}}
                }},
                "rules": [
                    {{"segment": "typo", "parameters": {{"{CLUSTER_PARAM}": true}}}},
                    {{"segment": "unlisted", "parameters": {{"{CLUSTER_PARAM}": true}}}},
                    {{"segment": "unknown-operator", "parameters": {{"{CLUSTER_PARAM}": true}}}}
                ]
            }}"#
        ));

        assert_eq!(
            file.segments["typo"].rejected,
            BTreeMap::from([(
                "cluster_nmae".to_string(),
                RejectedAttribute::UnknownAttribute
            )])
        );
        assert_eq!(
            file.segments["unlisted"].rejected,
            BTreeMap::from([(
                "cluster_name".to_string(),
                RejectedAttribute::UnsupportedValues
            )])
        );
        assert_eq!(
            file.segments["unknown-operator"].rejected,
            BTreeMap::from([(
                "cluster_name".to_string(),
                RejectedAttribute::UnknownOperator("starts_with".to_string())
            )])
        );
        assert!(
            frontend
                .file_cluster_overrides(
                    &file,
                    &params,
                    &[CLUSTER_PARAM],
                    &[cluster_ctx(1, "analytics")]
                )
                .is_empty()
        );
        assert_eq!(
            frontend.scoped_rule_diagnostics(&file, &params),
            vec![
                "segment \"typo\" in the system parameter sync file matches no cluster or \
                 replica: attribute \"cluster_nmae\" is not a cluster or replica attribute"
                    .to_string(),
                "segment \"unknown-operator\" in the system parameter sync file matches no \
                 cluster or replica: attribute \"cluster_name\" names \"starts_with\", which is \
                 not one of the \"in\" and \"matches\" operators"
                    .to_string(),
                "segment \"unlisted\" in the system parameter sync file matches no cluster or \
                 replica: attribute \"cluster_name\" expects an array of strings, numbers or \
                 booleans, or an object of an \"in\" array and a \"matches\" array of patterns"
                    .to_string(),
            ]
        );
    }

    /// A `matches` pattern selects by name, which an exact list cannot do for a
    /// cluster or replica that does not exist yet. The predicate object's two
    /// operators are ORed, so a value hit by either is allowed.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_pattern_predicate() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let file = parse(&format!(
            r#"{{
                "segments": {{
                    "prod": {{"cluster_name": {{"matches": ["^prod-"]}}}},
                    "prod-or-analytics": {{"cluster_name": {{
                        "in": ["analytics"],
                        "matches": ["^prod-"]
                    }}}}
                }},
                "rules": [
                    {{"segment": "prod", "parameters": {{"{CLUSTER_PARAM}": true}}}},
                    {{"segment": "prod-or-analytics", "parameters": {{
                        "{CLUSTER_PARAM_2}": true
                    }}}}
                ]
            }}"#
        ));

        // A bare array is the `in` operator alone, so it parses to what the
        // object form spells.
        assert_eq!(
            parse(r#"{"segments": {"s": {"cluster_name": ["analytics"]}}}"#).segments["s"]
                .attributes[&ScopeAttribute::ClusterName],
            parse(r#"{"segments": {"s": {"cluster_name": {"in": ["analytics"]}}}}"#).segments["s"]
                .attributes[&ScopeAttribute::ClusterName]
        );

        let out = frontend.file_cluster_overrides(
            &file,
            &params,
            &[CLUSTER_PARAM, CLUSTER_PARAM_2],
            &[
                // The pattern is unanchored on the right, so a prefix matches.
                cluster_ctx(1, "prod-ingest"),
                // Only the `in` side hits.
                cluster_ctx(2, "analytics"),
                // The leading `^` is what keeps this out of both segments.
                cluster_ctx(3, "staging-prod-ingest"),
            ],
        );

        assert_eq!(
            out,
            BTreeMap::from([
                (
                    ClusterId::User(1),
                    BTreeMap::from([
                        (CLUSTER_PARAM.to_string(), "true".to_string()),
                        (CLUSTER_PARAM_2.to_string(), "true".to_string()),
                    ])
                ),
                (ClusterId::User(2), overrides(CLUSTER_PARAM_2, "true")),
            ])
        );
    }

    /// A pattern does not change which rule decides a parameter: the first rule
    /// whose segment matches still wins, whichever operator made it match. Two
    /// patterns of which one is the narrower is the shape that makes this the
    /// natural way to write an exception.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_pattern_obeys_first_matching_rule() {
        let params = SynchronizedParameters::default();
        let file = parse(&format!(
            r#"{{
                "segments": {{
                    "prod-canary": {{"cluster_name": {{"matches": ["^prod-canary"]}}}},
                    "prod": {{"cluster_name": {{"matches": ["^prod-"]}}}}
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

    /// An invalid pattern makes its segment match nothing, exactly as an unknown
    /// attribute name does. Dropping it instead would widen the segment to every
    /// object the surviving entries allow.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_invalid_pattern_matches_nothing() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let file = parse(&format!(
            r#"{{
                "segments": {{"prod": {{
                    "cluster_name": {{"matches": ["^prod-["]}},
                    "is_builtin": [false]
                }}}},
                "rules": [{{"segment": "prod", "parameters": {{"{CLUSTER_PARAM}": true}}}}]
            }}"#
        ));

        assert!(
            frontend
                .file_cluster_overrides(
                    &file,
                    &params,
                    &[CLUSTER_PARAM],
                    &[cluster_ctx(1, "prod-ingest"), cluster_ctx(2, "analytics")]
                )
                .is_empty()
        );

        // The regex crate's error text is its own, so only the framing is pinned.
        let diagnostics = frontend.scoped_rule_diagnostics(&file, &params);
        assert_eq!(diagnostics.len(), 1, "{diagnostics:?}");
        assert!(
            diagnostics[0].starts_with(
                "segment \"prod\" in the system parameter sync file matches no cluster or \
                 replica: attribute \"cluster_name\" has the invalid \"matches\" pattern \
                 \"^prod-[\": "
            ),
            "{}",
            diagnostics[0]
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
                "segments": {{"gone": {{"cluster_name": ["gone"]}}}},
                "rules": [{{"segment": "gone", "parameters": {{
                    "{CLUSTER_PARAM}": true,
                    "{REPLICA_PARAM}": false
                }}}}]
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
                "segments": {{"analytics": {{"cluster_name": ["analytics"]}}}},
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

    /// A whole-document failure, an unreadable file or a document that is not a
    /// JSON object, must express "no information", not "no overrides": the scoped
    /// desired state is complete, so treating a failure as an empty state would
    /// durably prune every scoped override and restore it once the file is fixed.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_read_failure_keeps_scoped_overrides() {
        let params = SynchronizedParameters::default();
        let frontend = file_frontend();
        let clusters = [cluster_ctx(1, "analytics")];

        // A readable file establishes the override.
        frontend.refresh_config_file(Path::new(CONFIG_PATH), Ok(scoped_file(true)), &params);
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
            frontend.refresh_config_file(Path::new(CONFIG_PATH), Ok(scoped_file(true)), &params);
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
        let read = |contents: String| {
            frontend
                .refresh_config_file(Path::new(CONFIG_PATH), Ok(contents), &params)
                .expect("document is a JSON object")
        };

        let first = read(scoped_file(true));
        let again = read(scoped_file(true));
        assert!(Arc::ptr_eq(&first, &again), "unchanged file was re-parsed");

        let changed = read(scoped_file(false));
        assert!(
            !Arc::ptr_eq(&first, &changed),
            "changed file was not parsed"
        );
    }

    /// The mistakes an operator can realistically make in a rule are diagnosed
    /// rather than hard-failed. Resolution drops each of them, and nothing
    /// surfaces a parameter's scope from SQL, which leaves an operator nothing
    /// else to debug against.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_diagnoses_rule_mistakes() {
        let params = SynchronizedParameters::default();
        let file = parse(&format!(
            r#"{{
                "segments": {{"analytics": {{"cluster_name": ["analytics"]}}}},
                "rules": [
                    {{"segment": "analytics", "parameters": {{
                        "max_connections": 100,
                        "enabel_eager_delta_joins": true,
                        "{CLUSTER_PARAM}": "maybe"
                    }}}},
                    {{"segment": "analytics_2", "parameters": {{"{CLUSTER_PARAM}": true}}}}
                ]
            }}"#
        ));

        assert_eq!(
            file_frontend().scoped_rule_diagnostics(&file, &params),
            // Ordered by rule, then by parameter name within a rule.
            vec![
                // A misspelled parameter name.
                "ignoring enabel_eager_delta_joins for rule 1 (segment \"analytics\") in the \
                 system parameter sync file: not a cluster-scoped or replica-scoped system \
                 parameter"
                    .to_string(),
                // A value that does not parse for the parameter's type.
                format!(
                    "ignoring unparseable value \"maybe\" for system parameter {CLUSTER_PARAM} \
                     on rule 1 (segment \"analytics\") in the system parameter sync file"
                ),
                // A parameter that carries no scope at all, so it can only be set
                // environment-wide.
                "ignoring max_connections for rule 1 (segment \"analytics\") in the system \
                 parameter sync file: not a cluster-scoped or replica-scoped system parameter"
                    .to_string(),
                // A rule naming a segment the file does not define.
                "ignoring rule 2 in the system parameter sync file: no segment named \
                 \"analytics_2\""
                    .to_string(),
            ]
        );
    }

    /// The reserved section names shadow any synced parameter of the same name,
    /// so no such parameter may exist. Renaming the parameter is the fix.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
    fn test_no_synced_parameter_shadows_a_reserved_section() {
        let params = SynchronizedParameters::default();
        for section in [SEGMENTS_SECTION, RULES_SECTION] {
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
