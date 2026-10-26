// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

/// The top-level object of a document the test knows to be one.
fn object(contents: &str) -> serde_json::Map<String, JsonValue> {
    serde_json::from_str(contents).unwrap_or_else(|e| panic!("not a JSON object: {e}"))
}

/// Parses a document the test knows to be valid.
fn parse(contents: &str) -> ConfigFileV2 {
    ConfigFileV2::parse(object(contents)).unwrap_or_else(|e| panic!("document does not parse: {e}"))
}

/// The error a document the test knows to be invalid is rejected with.
fn parse_error(contents: &str) -> String {
    ConfigFileV2::parse(object(contents)).expect_err("document parses")
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
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_refresh_rejects_non_object_document() {
    let params = SynchronizedParameters::default();
    let frontend = file_frontend();
    let refresh = |contents: &str| {
        frontend.refresh_config_file(Path::new(CONFIG_PATH), Ok(contents.to_string()), &params)
    };

    assert_eq!(refresh("[]"), None);
    assert_eq!(refresh("not json"), None);
    assert_eq!(refresh("{}").as_deref(), Some(&ConfigFileV2::default()));
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
            single_clause_file(r#"{"attribute": "cluster_name", "op": "in", "values": "prod"}"#),
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
