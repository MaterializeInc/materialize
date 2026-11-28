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
    file_frontend_with_now(path, NowFn::from(|| 0))
}

/// A file-backed frontend reading `path`, whose clock is `now_fn`.
fn file_frontend_with_now(path: &Path, now_fn: NowFn) -> SystemParameterFrontend {
    SystemParameterFrontend {
        client: SystemParameterFrontendClient::File {
            path: path.into(),
            now_fn,
        },
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

/// The release a version 2 document must name for [`DUMMY_BUILD_INFO`], which
/// every test frontend runs as.
const RELEASE: &str = "v0.0.0+dummy";

/// A release other than [`RELEASE`].
const OTHER_RELEASE: &str = "v26.46.1";

/// The top-level object of a document the test knows to be one.
fn object(contents: &str) -> serde_json::Map<String, JsonValue> {
    serde_json::from_str(contents).unwrap_or_else(|e| panic!("not a JSON object: {e}"))
}

/// Parses a document the test knows to be valid.
fn parse(contents: &str) -> ConfigFileV2 {
    ConfigFileV2::parse(object(contents), RELEASE)
        .unwrap_or_else(|e| panic!("document does not parse: {e:?}"))
}

/// The defect a document the test knows to be malformed is rejected with.
fn parse_error(contents: &str) -> String {
    match ConfigFileV2::parse(object(contents), RELEASE) {
        Err(ParseError::Invalid(e)) => e,
        other => panic!("document is not rejected as malformed: {other:?}"),
    }
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

/// A replica of a legacy size family, the coarse targeting axis a rule
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

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

fn param_value(value: &str) -> ParamValue {
    ParamValue(Some(value.to_string()))
}

/// Resolves `file`'s rules for `clusters`, as the file arm of
/// [`SystemParameterFrontend::pull_cluster_overrides`] does.
fn cluster_overrides(
    file: &ConfigFileV2,
    params: &SynchronizedParameters,
    param_names: &[&'static str],
    clusters: &[ClusterEvalContext],
) -> BTreeMap<ClusterId, BTreeMap<String, String>> {
    let clusters = clusters
        .iter()
        .map(|cluster| (cluster.cluster_id, cluster_attributes(&cluster.cluster)));
    file_frontend().file_overrides(file, params, param_names, ContextKind::Cluster, clusters)
}

/// Resolves `file`'s rules for `replicas`, as the file arm of
/// [`SystemParameterFrontend::pull_replica_overrides`] does.
fn replica_overrides(
    file: &ConfigFileV2,
    params: &SynchronizedParameters,
    param_names: &[&'static str],
    replicas: &[ReplicaEvalContext],
) -> BTreeMap<ReplicaId, BTreeMap<String, String>> {
    let replicas = replicas
        .iter()
        .map(|replica| (replica.replica_id, replica_attributes(&replica.replica)));
    file_frontend().file_overrides(file, params, param_names, ContextKind::Replica, replicas)
}

/// A document whose single rule gives the `analytics` cluster `value` for
/// [`CLUSTER_PARAM`].
fn scoped_file(value: bool) -> String {
    format!(
        r#"{{
            "version": 2,
            "release": "v0.0.0+dummy",
            "rules": [{{
                "contextKind": "cluster",
                "clauses": [
                    {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                ],
                "parameters": {{"{CLUSTER_PARAM}": {value}}}
            }}]
        }}"#
    )
}

/// A document whose single `cluster` rule holds `clause` as its only clause.
fn single_clause_file(clause: &str) -> String {
    format!(
        r#"{{
            "version": 2,
            "release": "v0.0.0+dummy",
            "rules": [{{
                "contextKind": "cluster",
                "clauses": [{clause}],
                "parameters": {{"{CLUSTER_PARAM}": true}}
            }}]
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

    // Every JSON scalar renders to the raw string the backend parses.
    assert_eq!(
        file,
        ConfigFileV2 {
            global_parameters: BTreeMap::from([
                ("max_connections".to_string(), param_value("1000")),
                (
                    "allowed_cluster_replica_sizes".to_string(),
                    param_value("'25cc', '50cc'")
                ),
                ("enable_lgalloc".to_string(), param_value("false")),
            ]),
            ..Default::default()
        }
    );
    // An explicit `null` expresses no opinion, leaving the value alone.
    let null = parse(r#"{"max_connections": null}"#);
    assert_eq!(null.global_parameters["max_connections"], ParamValue(None));

    // Without `version` no key is reserved, so these are parameter names like
    // any other, and the document scopes nothing.
    let flat = parse(r#"{"globalParameters": {}, "rules": []}"#);
    assert_eq!(
        flat.global_parameters.keys().collect::<Vec<_>>(),
        vec!["globalParameters", "rules"]
    );
    assert!(flat.rules.is_empty());
}

#[mz_ore::test]
fn test_parse_v2_document() {
    let file = parse(
        r#"{
            "version": 2,
            "release": "v0.0.0+dummy",
            "globalParameters": {"enable_lgalloc": false},
            "rules": [
                {
                    "contextKind": "cluster",
                    "clauses": [{
                        "attribute": "cluster_name",
                        "op": "in",
                        "values": ["analytics", "analytics_2"]
                    }],
                    "parameters": {"enable_eager_delta_joins": true}
                },
                {
                    "contextKind": "replica",
                    "clauses": [
                        {
                            "contextKind": "replica",
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
                    ],
                    "parameters": {"enable_lgalloc": true}
                }
            ]
        }"#,
    );

    assert_eq!(
        file.global_parameters,
        BTreeMap::from([("enable_lgalloc".to_string(), param_value("false"))])
    );

    // Rules keep the document order that decides which of them wins, and so do
    // their clauses. `negate` defaults to false, a clause may repeat its rule's
    // context kind, a boolean value may be written as a JSON boolean or as its
    // string, and the LaunchDarkly REST API's `_id` is ignored rather than
    // rejected.
    assert_eq!(
        file.rules,
        vec![
            Rule {
                context_kind: ContextKind::Cluster,
                clauses: vec![Clause {
                    attribute: ScopeAttribute::ClusterName,
                    op: ClauseOp::In(strings(&["analytics", "analytics_2"])),
                    negate: false,
                    context_kind: None,
                }],
                parameters: BTreeMap::from([(CLUSTER_PARAM.to_string(), param_value("true"))]),
            },
            Rule {
                context_kind: ContextKind::Replica,
                clauses: vec![
                    Clause {
                        attribute: ScopeAttribute::ReplicaSizeFamily,
                        op: ClauseOp::In(strings(&["legacy"])),
                        negate: false,
                        context_kind: Some(ContextKind::Replica),
                    },
                    Clause {
                        attribute: ScopeAttribute::IsBuiltin,
                        op: ClauseOp::In(strings(&["false"])),
                        negate: false,
                        context_kind: None,
                    },
                    Clause {
                        attribute: ScopeAttribute::ReplicaName,
                        op: ClauseOp::Matches(vec![Pattern(
                            Regex::new("^scratch-").expect("test pattern compiles")
                        )]),
                        negate: true,
                        context_kind: None,
                    },
                ],
                parameters: BTreeMap::from([(REPLICA_PARAM.to_string(), param_value("true"))]),
            },
        ]
    );
}

/// The `version` key alone selects the format, so a defective version 2
/// document is rejected rather than read as a flat parameter map.
#[mz_ore::test]
fn test_parse_selects_format_by_version_key() {
    assert_eq!(
        parse(r#"{"version": 2, "release": "v0.0.0+dummy"}"#),
        ConfigFileV2::default()
    );

    // A flat key is not an environment-wide value in version 2.
    let flat_key =
        parse_error(r#"{"version": 2, "release": "v0.0.0+dummy", "max_connections": 1000}"#);
    assert!(
        flat_key.contains("unknown field `max_connections`"),
        "{flat_key}"
    );

    // Read as a flat map, this would apply `max_connections` and scope
    // nothing, pruning every existing override.
    let broken = parse_error(
        r#"{"version": 2, "release": "v0.0.0+dummy", "globalParameters": {"max_connections": 1000}, "rules": 7}"#,
    );
    assert!(broken.starts_with("rules: invalid type"), "{broken}");

    // Another version is reported as such, ahead of its unknown keys.
    assert_eq!(
        parse_error(r#"{"version": 3, "anything": true}"#),
        "version: unsupported version 3, expected 2"
    );
    assert_eq!(
        parse_error(r#"{"version": "2"}"#),
        r#"version: unsupported version "2", expected 2"#
    );
}

#[mz_ore::test]
fn test_running_release_is_mz_version_without_sha() {
    assert_eq!(running_release(&DUMMY_BUILD_INFO), RELEASE);
    let mz_version = DUMMY_BUILD_INFO.human_version(None);
    assert_eq!(mz_version.split_whitespace().next(), Some(RELEASE));
}

/// A version 2 document names the release it targets, and one for another
/// release is reported as that, ahead of any defect its schema has here.
#[mz_ore::test]
fn test_parse_checks_release() {
    let parse_release = |contents: &str| ConfigFileV2::parse(object(contents), RELEASE);
    let mismatch = Err(ParseError::ReleaseMismatch {
        file: OTHER_RELEASE.to_string(),
        running: RELEASE.to_string(),
    });

    assert_eq!(
        parse_release(r#"{"version": 2, "release": "v0.0.0+dummy"}"#),
        Ok(ConfigFileV2::default())
    );
    assert_eq!(
        parse_release(r#"{"version": 2, "release": "v26.46.1"}"#),
        mismatch
    );
    // Compared exactly, so neither `mz_version()` in full nor the version
    // without its `v` matches.
    for release in [r#""v0.0.0+dummy (000000000)""#, r#""0.0.0+dummy""#] {
        assert!(
            matches!(
                parse_release(&format!(r#"{{"version": 2, "release": {release}}}"#)),
                Err(ParseError::ReleaseMismatch { .. })
            ),
            "{release} matched"
        );
    }

    // Defects another release's schema would have here, such as a parameter
    // section this build does not know or a type it changed, are not reported.
    assert_eq!(
        parse_release(r#"{"version": 2, "release": "v26.46.1", "segments": {}, "rules": 7}"#),
        mismatch
    );
    // An unsupported version is reported ahead of the release.
    assert_eq!(
        parse_error(r#"{"version": 3, "release": "v26.46.1"}"#),
        "version: unsupported version 3, expected 2"
    );

    // The release is mandatory in version 2, and a string.
    assert_eq!(
        parse_error(r#"{"version": 2, "globalParameters": {}}"#),
        r#"release: missing, expected the release the document targets, such as "v0.0.0+dummy""#
    );
    assert_eq!(
        parse_error(r#"{"version": 2, "release": 26}"#),
        "release: expected a string, found 26"
    );

    // The flat format names no release, so `release` is a parameter name there.
    let flat = parse(r#"{"release": "v26.46.1"}"#);
    assert_eq!(
        flat.global_parameters.keys().collect::<Vec<_>>(),
        vec!["release"]
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
    let rule_with =
        |body: &str| format!(r#"{{"version": 2, "release": "v0.0.0+dummy", "rules": [{body}]}}"#);
    let cluster_name = r#""attribute": "cluster_name", "op": "in", "values": ["prod"]"#;

    let cases = [
        // Sections of the wrong type.
        (
            r#"{"version": 2, "release": "v0.0.0+dummy", "rules": {}}"#.to_string(),
            "rules",
            "invalid type",
        ),
        (
            r#"{"version": 2, "release": "v0.0.0+dummy", "globalParameters": []}"#.to_string(),
            "globalParameters",
            "invalid type",
        ),
        // Keys are camelCase, and segments are written inline in their rule.
        (
            r#"{"version": 2, "release": "v0.0.0+dummy", "global_parameters": {}}"#.to_string(),
            "",
            "unknown field `global_parameters`",
        ),
        (
            r#"{"version": 2, "release": "v0.0.0+dummy", "segments": {}}"#.to_string(),
            "",
            "unknown field `segments`",
        ),
        // Rules.
        (
            r#"{"version": 2, "release": "v0.0.0+dummy", "rules": ["not-a-rule"]}"#.to_string(),
            "rules[0]",
            "invalid type",
        ),
        (
            rule_with(
                r#"{"contextKind": "cluster", "clauses": [], "parameters": {}, "priority": 1}"#,
            ),
            "rules[0]",
            "unknown field `priority`",
        ),
        (
            rule_with(
                r#"{"contextKind": "cluster", "clauses": [], "segment": "s", "parameters": {}}"#,
            ),
            "rules[0]",
            "unknown field `segment`",
        ),
        (
            rule_with(r#"{"contextKind": "cluster", "clauses": []}"#),
            "rules[0]",
            "missing field `parameters`",
        ),
        (
            rule_with(r#"{"clauses": [], "parameters": {}}"#),
            "rules[0]",
            "missing field `contextKind`",
        ),
        (
            rule_with(r#"{"contextKind": "environment", "clauses": [], "parameters": {}}"#),
            "rules[0].contextKind",
            "unknown variant `environment`",
        ),
        (
            rule_with(r#"{"contextKind": "cluster", "parameters": {}}"#),
            "rules[0]",
            "missing field `clauses`",
        ),
        (
            rule_with(r#"{"contextKind": "cluster", "clauses": {}, "parameters": {}}"#),
            "rules[0].clauses",
            "invalid type",
        ),
        (
            single_clause_file(&format!(
                r#"{{{cluster_name}}}, {{
                    "attribute": "replica_size_family", "op": "in", "values": ["legacy"]
                }}"#
            )),
            "rules[0]: clauses[1].attribute is not carried by objects of the rule's contextKind",
            "",
        ),
        // A clause may repeat its rule's context kind, but not contradict it.
        (
            single_clause_file(&format!(r#"{{{cluster_name}, "contextKind": "replica"}}"#)),
            "rules[0]: clauses[0].contextKind differs from the rule's contextKind",
            "",
        ),
        (
            single_clause_file(&format!(r#"{{{cluster_name}, "contextKind": "user"}}"#)),
            "rules[0].clauses[0].contextKind",
            "unknown variant `user`",
        ),
        // Clauses.
        (
            single_clause_file("7"),
            "rules[0].clauses[0]",
            "invalid type",
        ),
        (
            single_clause_file(r#"{"op": "in", "values": []}"#),
            "rules[0].clauses[0]",
            "missing field `attribute`",
        ),
        (
            single_clause_file(r#"{"attribute": "cluster_nmae", "op": "in", "values": []}"#),
            "rules[0].clauses[0].attribute",
            "unknown variant `cluster_nmae`",
        ),
        (
            single_clause_file(r#"{"attribute": 7, "op": "in", "values": []}"#),
            "rules[0].clauses[0].attribute",
            "invalid type",
        ),
        (
            single_clause_file(r#"{"attribute": "cluster_name", "values": []}"#),
            "rules[0].clauses[0]",
            "missing field `op`",
        ),
        (
            single_clause_file(r#"{"attribute": "cluster_name", "op": "in"}"#),
            "rules[0].clauses[0]",
            "missing field `values`",
        ),
        (
            single_clause_file(r#"{"attribute": "cluster_name", "op": "in", "values": "prod"}"#),
            "rules[0].clauses[0].values",
            "invalid type",
        ),
        (
            single_clause_file(
                r#"{"attribute": "cluster_name", "op": "in", "values": ["prod", ["prod"]]}"#,
            ),
            "rules[0].clauses[0].values[1]",
            "expected a string, number or boolean",
        ),
        (
            single_clause_file(&format!(r#"{{{cluster_name}, "negate": "yes"}}"#)),
            "rules[0].clauses[0].negate",
            "invalid type",
        ),
        (
            single_clause_file(&format!(r#"{{{cluster_name}, "nope": 1}}"#)),
            "rules[0].clauses[0]",
            "unknown field `nope`",
        ),
        // Operators: LaunchDarkly's non-string ones, and a casing a hand-author
        // is likely to reach for.
        (
            single_clause_file(
                r#"{"attribute": "cluster_name", "op": "lessThan", "values": ["1"]}"#,
            ),
            "rules[0].clauses[0].op",
            "unknown variant `lessThan`",
        ),
        (
            single_clause_file(
                r#"{"attribute": "cluster_name", "op": "segmentMatch", "values": ["s"]}"#,
            ),
            "rules[0].clauses[0].op",
            "unknown variant `segmentMatch`",
        ),
        (
            single_clause_file(
                r#"{"attribute": "cluster_name", "op": "starts_with", "values": []}"#,
            ),
            "rules[0].clauses[0].op",
            "unknown variant `starts_with`",
        ),
        (
            single_clause_file(
                r#"{"attribute": "cluster_name", "op": "matches", "values": ["^prod-", "^prod-["]}"#,
            ),
            "rules[0].clauses[0]: values[1] is the invalid pattern \"^prod-[\": ",
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

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_file_cluster_rule_applied() {
    let params = SynchronizedParameters::default();
    let file = parse(&scoped_file(true));

    let out = cluster_overrides(
        &file,
        &params,
        &[CLUSTER_PARAM],
        &[cluster_ctx(1, "analytics"), cluster_ctx(2, "staging")],
    );

    // Sparse: only the cluster the rule matches gets a row.
    assert_eq!(
        out,
        BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "true"))])
    );
}

/// The clauses of one rule are ANDed, and a replica may be matched on its own
/// attributes as well as its owning cluster's.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_file_replica_rule_applied() {
    let params = SynchronizedParameters::default();
    let file = parse(&format!(
        r#"{{
            "version": 2,
            "release": "v0.0.0+dummy",
            "rules": [{{
                "contextKind": "replica",
                "clauses": [
                    {{
                        "attribute": "replica_size_family",
                        "op": "in",
                        "values": ["legacy"]
                    }},
                    {{"attribute": "cluster_name", "op": "in", "values": ["prod"]}}
                ],
                "parameters": {{"{REPLICA_PARAM}": false}}
            }}]
        }}"#
    ));

    let out = replica_overrides(
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

/// A replica-local parameter may be supplied through a `replica` rule whose
/// clauses name cluster attributes alone, which targets every replica of that
/// cluster.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_file_replica_rule_targeted_by_cluster() {
    let params = SynchronizedParameters::default();
    let file = parse(&format!(
        r#"{{
            "version": 2,
            "release": "v0.0.0+dummy",
            "rules": [{{
                "contextKind": "replica",
                "clauses": [
                    {{"attribute": "cluster_name", "op": "in", "values": ["prod"]}}
                ],
                "parameters": {{"{REPLICA_PARAM}": false}}
            }}]
        }}"#
    ));

    let out = replica_overrides(
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

/// The first matching rule decides a parameter, and it decides it before the
/// value is judged, so a value agreeing with the environment-wide one still
/// shadows a later rule. A rule that does not mention a parameter leaves it to
/// a later one, and a rule with no clauses matches every object of its context
/// kind.
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
            "release": "v0.0.0+dummy",
            "rules": [
                {{
                    "contextKind": "cluster",
                    "clauses": [
                        {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                    ],
                    "parameters": {{"{CLUSTER_PARAM}": false}}
                }},
                {{
                    "contextKind": "cluster",
                    "clauses": [],
                    "parameters": {{"{CLUSTER_PARAM}": true, "{CLUSTER_PARAM_2}": true}}
                }}
            ]
        }}"#
    ));

    let out = cluster_overrides(
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

    // Every rule supplies the same parameter, and each clause is tested on its
    // own file, so a hit is unambiguous.
    let hits = |op: &str, values: &str, name: &str| {
        let file = parse(&single_clause_file(&format!(
            r#"{{"attribute": "cluster_name", "op": "{op}", "values": {values}}}"#
        )));
        !cluster_overrides(&file, &params, &[CLUSTER_PARAM], &[cluster_ctx(1, name)]).is_empty()
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

    let out = cluster_overrides(
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

/// A cluster-coherent parameter may not be supplied through a `replica` rule:
/// honouring that would let the parameter resolve differently across one
/// cluster's replicas. The cluster pass never consults a `replica` rule, so the
/// parameter is left to a later rule, while a replica-local parameter in the
/// same rule is unaffected.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_cluster_coherence_guard() {
    let params = SynchronizedParameters::default();
    let file = parse(&format!(
        r#"{{
            "version": 2,
            "release": "v0.0.0+dummy",
            "rules": [
                {{
                    "contextKind": "replica",
                    "clauses": [{{
                        "attribute": "replica_size_family",
                        "op": "in",
                        "values": ["legacy"]
                    }}],
                    "parameters": {{"{CLUSTER_PARAM}": true, "{REPLICA_PARAM}": false}}
                }},
                {{
                    "contextKind": "cluster",
                    "clauses": [
                        {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                    ],
                    "parameters": {{"{CLUSTER_PARAM}": true}}
                }}
            ]
        }}"#
    ));

    assert_eq!(
        cluster_overrides(
            &file,
            &params,
            &[CLUSTER_PARAM],
            &[cluster_ctx(1, "analytics")]
        ),
        BTreeMap::from([(ClusterId::User(1), overrides(CLUSTER_PARAM, "true"))])
    );
    assert_eq!(
        replica_overrides(
            &file,
            &params,
            &[REPLICA_PARAM],
            &[legacy_replica_ctx(1, "analytics", 1, "r1")]
        ),
        BTreeMap::from([(ReplicaId::User(1), overrides(REPLICA_PARAM, "false"))])
    );
    assert_eq!(
        file_frontend().scoped_rule_diagnostics(&file, &params),
        vec![format!(
            "ignoring {CLUSTER_PARAM} for rules[0] in the system parameter sync file: \
             {CLUSTER_PARAM} is cluster-scoped, so it cannot be supplied through a rule of \
             contextKind \"replica\""
        )]
    );
}

/// A pattern does not change which rule decides a parameter: the first rule
/// that matches still wins. Two patterns of which one is the narrower is the
/// shape that makes this the natural way to write an exception.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_pattern_obeys_first_matching_rule() {
    let params = SynchronizedParameters::default();
    let file = parse(&format!(
        r#"{{
            "version": 2,
            "release": "v0.0.0+dummy",
            "rules": [
                {{
                    "contextKind": "cluster",
                    "clauses": [{{
                        "attribute": "cluster_name",
                        "op": "matches",
                        "values": ["^prod-canary"]
                    }}],
                    "parameters": {{"{CLUSTER_PARAM}": false, "{CLUSTER_PARAM_2}": true}}
                }},
                {{
                    "contextKind": "cluster",
                    "clauses": [{{
                        "attribute": "cluster_name",
                        "op": "matches",
                        "values": ["^prod-"]
                    }}],
                    "parameters": {{"{CLUSTER_PARAM}": true}}
                }}
            ]
        }}"#
    ));

    let out = cluster_overrides(
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

/// A rule that matches nothing live is ignored, not an error: the live objects
/// drive the resolution, so the file is never a second source of truth for what
/// exists.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_file_rule_matching_nothing_live_ignored() {
    let params = SynchronizedParameters::default();
    let file = parse(&format!(
        r#"{{
            "version": 2,
            "release": "v0.0.0+dummy",
            "rules": [
                {{
                    "contextKind": "cluster",
                    "clauses": [
                        {{"attribute": "cluster_name", "op": "in", "values": ["gone"]}}
                    ],
                    "parameters": {{"{CLUSTER_PARAM}": true}}
                }},
                {{
                    "contextKind": "replica",
                    "clauses": [
                        {{"attribute": "cluster_name", "op": "in", "values": ["gone"]}}
                    ],
                    "parameters": {{"{REPLICA_PARAM}": false}}
                }}
            ]
        }}"#
    ));

    assert!(
        cluster_overrides(&file, &params, &[CLUSTER_PARAM], &[cluster_ctx(1, "prod")]).is_empty()
    );
    assert!(
        replica_overrides(
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
            "release": "v0.0.0+dummy",
            "rules": [{{
                "contextKind": "cluster",
                "clauses": [
                    {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                ],
                "parameters": {{"{CLUSTER_PARAM}": "maybe"}}
            }}]
        }}"#
    ));

    assert!(
        cluster_overrides(
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
        cluster_overrides(
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
        Ok(scoped_file(false).replace(RELEASE, OTHER_RELEASE)),
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
            "release": "v0.0.0+dummy",
            "globalParameters": {{"{CLUSTER_PARAM}": true}},
            "rules": [{{
                "contextKind": "cluster",
                "clauses": [
                    {{"attribute": "cluster_name", "op": "in", "values": ["analytics"]}}
                ],
                "parameters": {{"{CLUSTER_PARAM}": false}}
            }}]
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
        Ok(r#"{"version": 2, "release": "v0.0.0+dummy", "rules": 7}"#.to_string()),
        Ok(r#"{"version": 2, "release": "v26.46.1"}"#.to_string()),
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

    // A version 2 document supplies the same values from `globalParameters`.
    fs::write(
        &path,
        r#"{"version": 2, "release": "v0.0.0+dummy", "globalParameters": {"max_connections": 2000}}"#,
    )
    .expect("can write the test file");
    assert!(frontend.pull(&mut params));
    assert_eq!(params.get("max_connections"), "2000");

    // Unparseable, structurally defective, for another release or missing, the
    // file leaves every current value alone.
    for contents in [
        "}not json{",
        r#"{"version": 2, "release": "v0.0.0+dummy", "globalParameters": {"max_connections": 5}, "rules": 7}"#,
        r#"{"version": 2, "release": "v26.46.1", "globalParameters": {"max_connections": 5}}"#,
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
            "release": "v0.0.0+dummy",
            "rules": [
                {{"contextKind": "cluster", "clauses": [], "parameters": {{}}}},
                {{
                    "contextKind": "cluster",
                    "clauses": [],
                    "parameters": {{
                        "max_connections": 100,
                        "enabel_eager_delta_joins": true,
                        "{CLUSTER_PARAM}": "maybe"
                    }}
                }}
            ]
        }}"#
    ));

    assert_eq!(
        file_frontend().scoped_rule_diagnostics(&file, &params),
        // Ordered by rule, then by parameter name within a rule.
        vec![
            // A misspelled parameter name.
            "ignoring enabel_eager_delta_joins for rules[1] in the system parameter sync \
             file: not a cluster-scoped or replica-scoped system parameter"
                .to_string(),
            // A value that does not parse for the parameter's type.
            format!(
                "ignoring unparseable value \"maybe\" for system parameter {CLUSTER_PARAM} \
                 on rules[1] in the system parameter sync file"
            ),
            // A parameter that carries no scope at all, so it can only be set
            // environment-wide.
            "ignoring max_connections for rules[1] in the system parameter sync file: not a \
             cluster-scoped or replica-scoped system parameter"
                .to_string(),
        ]
    );
}

/// A file that exists but does not apply is a defect, which fails the first
/// boot. A file that cannot be read is not.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_config_file_defect() {
    let params = SynchronizedParameters::default();
    let frontend = file_frontend();
    let defect = |read: io::Result<String>| {
        sync_tick(&frontend, &params, read);
        frontend.config_file_defect()
    };

    assert_eq!(
        frontend.config_file_defect(),
        None,
        "defect before any read"
    );
    assert_eq!(defect(Ok(scoped_file(true))), None);
    assert_eq!(defect(Err(io::Error::from(io::ErrorKind::NotFound))), None);
    assert_eq!(
        defect(Ok(scoped_file(true).replace(RELEASE, OTHER_RELEASE))).as_deref(),
        Some(
            "system parameter sync file /nonexistent/system-params.json targets release \
             v26.46.1, running v0.0.0+dummy"
        )
    );
    assert_eq!(
        defect(Ok(
            r#"{"version": 2, "release": "v0.0.0+dummy", "rules": 7}"#.to_string()
        ))
        .as_deref(),
        Some(
            "system parameter sync file /nonexistent/system-params.json does not parse: \
             rules: invalid type: number, expected a sequence"
        )
    );
    assert_eq!(defect(Ok("{}".to_string())), None);
}

/// The status gauge is one-hot over the latest read's outcome, and the applied
/// time advances on every read that parses, changed or not.
#[mz_ore::test]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `decNumberFromInt32` on OS `linux`
fn test_file_status_metrics() {
    let params = SynchronizedParameters::default();
    let time = Arc::new(AtomicU64::new(0));
    let time_clone = Arc::clone(&time);
    let frontend = file_frontend_with_now(
        Path::new(CONFIG_PATH),
        NowFn::from(move || time_clone.load(Ordering::SeqCst)),
    );
    let status = || {
        ConfigFileStatus::ALL
            .into_iter()
            .filter(|candidate| {
                let gauge = frontend
                    .metrics
                    .file_status
                    .with_label_values(&[candidate.label()])
                    .get();
                assert!(gauge == 0 || gauge == 1, "{candidate:?} is {gauge}");
                gauge == 1
            })
            .collect::<Vec<_>>()
    };
    let last_applied = || frontend.metrics.file_last_applied_time_seconds.get();
    let tick = |at_seconds: u64, read: io::Result<String>| {
        time.store(at_seconds * 1000, Ordering::SeqCst);
        sync_tick(&frontend, &params, read);
    };

    tick(10, Ok(scoped_file(true)));
    assert_eq!(status(), vec![ConfigFileStatus::Ok]);
    assert_eq!(last_applied(), 10);
    tick(20, Ok(scoped_file(true)));
    assert_eq!(last_applied(), 20, "an unchanged file is applied again");

    for (failure, expected) in [
        (
            Err(io::Error::from(io::ErrorKind::NotFound)),
            ConfigFileStatus::Unreadable,
        ),
        (Ok("}not json{".to_string()), ConfigFileStatus::Invalid),
        (
            Ok(scoped_file(true).replace(RELEASE, OTHER_RELEASE)),
            ConfigFileStatus::ReleaseMismatch,
        ),
    ] {
        tick(30, failure);
        assert_eq!(status(), vec![expected]);
        assert_eq!(last_applied(), 20, "{expected:?} counted as applied");
    }

    tick(40, Ok(scoped_file(true)));
    assert_eq!(status(), vec![ConfigFileStatus::Ok]);
    assert_eq!(last_applied(), 40);
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
