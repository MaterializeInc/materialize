// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr::{MapFilterProject, MfpPlan};
use mz_repr::{Datum, ReprScalarType, Row};

use super::*;
use crate::dataflows::LirDataflowDescription;
use crate::plan::scalar::LirScalarExpr;
use crate::plan::{AvailableCollections, GetPlan, LirId};

fn lir_id(n: u64) -> LirId {
    serde_json::from_value(serde_json::json!(n)).expect("LirId is a u64")
}

fn node(n: u64, node: LirRelationNode) -> LirRelationExpr {
    LirRelationExpr {
        lir_id: lir_id(n),
        node,
    }
}

fn get(n: u64, id: GlobalId) -> LirRelationExpr {
    node(
        n,
        LirRelationNode::Get {
            id: Id::Global(id),
            keys: AvailableCollections::new_raw(),
            plan: GetPlan::PassArrangements,
        },
    )
}

fn sample_envelope() -> LirPlanEnvelope {
    let constant = node(
        1,
        LirRelationNode::Constant {
            rows: Ok(vec![(
                StableRow(Row::pack_slice(&[Datum::Int64(7), Datum::Null])),
                mz_repr::Timestamp::from(0u64),
                mz_repr::Diff::from(1i64),
            )]),
        },
    );
    let mfp = MapFilterProject::<LirScalarExpr>::new(1).map([LirScalarExpr::literal_ok(
        Datum::String("x"),
        ReprScalarType::String,
    )]);
    let mfp = node(
        3,
        LirRelationNode::Mfp {
            input: Box::new(get(2, GlobalId::Transient(1))),
            mfp: MfpPlan::create_from(mfp).expect("no temporal filters"),
            input_key_val: None,
        },
    );
    LirPlanEnvelope {
        lir_version: LIR_VERSION,
        imports: vec![
            EnvelopeImport {
                id: GlobalId::Transient(1),
                source: ImportSource::Name("materialize.public.t".into()),
                persist: true,
                indexes: vec![],
            },
            EnvelopeImport {
                id: GlobalId::Transient(2),
                source: ImportSource::Item {
                    item_id: CatalogItemId::User(5),
                    version: serde_json::from_value(serde_json::json!(0)).expect("u64"),
                },
                persist: false,
                indexes: vec![ImportSource::Name("materialize.public.t_idx".into())],
            },
        ],
        objects: vec![
            EnvelopeObject {
                id: GlobalId::Transient(3),
                plan: constant,
            },
            EnvelopeObject {
                id: GlobalId::Transient(4),
                plan: mfp,
            },
        ],
        output: GlobalId::Transient(4),
    }
}

#[mz_ore::test]
fn envelope_roundtrip() {
    let envelope = sample_envelope();
    let json = envelope_to_json(&envelope);
    assert!(json.contains("\"$row\""), "{json}");
    assert_eq!(parse_envelope(&json), Ok(envelope));
}

#[mz_ore::test]
fn envelope_rejects_version() {
    let mut envelope = sample_envelope();
    envelope.lir_version = 2;
    let json = envelope_to_json(&envelope);
    assert_eq!(
        parse_envelope(&json),
        Err(EnvelopeError::Version {
            found: 2,
            expected: 1
        })
    );
}

#[mz_ore::test]
fn envelope_malformed_row() {
    let json = envelope_to_json(&sample_envelope()).replace("\"Int64\"", "\"Bogus\"");
    assert!(matches!(parse_envelope(&json), Err(EnvelopeError::Row(_))));
}

#[mz_ore::test]
fn envelope_unknown_function_names_it() {
    let call = r#"{"lir_id": 1, "node": {"Mfp": {
        "input": {"lir_id": 2, "node": {"Constant": {"rows": {"Ok": []}}}},
        "mfp": {"mfp": {"mfp": {"expressions": [
            {"CallUnary": {"func": "NoSuchFunc",
                           "expr": {"Column": [0, null]}}}
        ], "predicates": [], "projection": [0], "input_arity": 1}},
        "lower_bounds": [], "upper_bounds": []},
        "input_key_val": null}}}"#;
    let json = format!(
        r#"{{"lir_version": 1, "imports": [], "output": {{"Transient": 1}},
            "objects": [{{"id": {{"Transient": 1}}, "plan": {call}}}]}}"#
    );
    let Err(EnvelopeError::Json(msg)) = parse_envelope(&json) else {
        panic!("expected a Json error");
    };
    assert!(msg.contains("NoSuchFunc"), "{msg}");
}

/// An envelope whose single plan is `nodes` nested `Negate`s over a constant.
///
/// Its JSON depth is 8 + 3 * `nodes`: the envelope, the objects array, and the object take
/// 3, each `Negate` takes 3, and the constant takes 5.
fn nested_negate_json(nodes: usize) -> String {
    let mut plan = String::from(r#"{"lir_id": 1, "node": {"Constant": {"rows": {"Ok": []}}}}"#);
    for _ in 0..nodes {
        plan = format!(r#"{{"lir_id": 1, "node": {{"Negate": {{"input": {plan}}}}}}}"#);
    }
    format!(
        r#"{{"lir_version": 1, "imports": [], "output": {{"Transient": 1}},
            "objects": [{{"id": {{"Transient": 1}}, "plan": {plan}}}]}}"#
    )
}

#[mz_ore::test]
fn envelope_depth_above_cap_is_rejected() {
    // 5000 nodes are 15008 levels deep.
    let Err(EnvelopeError::Json(msg)) = parse_envelope(&nested_negate_json(5000)) else {
        panic!("expected a Json error");
    };
    assert!(msg.contains(&MAX_JSON_DEPTH.to_string()), "{msg}");
    // One level above the cap.
    let nodes = (MAX_JSON_DEPTH - 8) / 3 + 1;
    assert!(8 + 3 * nodes > MAX_JSON_DEPTH);
    assert!(matches!(
        parse_envelope(&nested_negate_json(nodes)),
        Err(EnvelopeError::Json(_))
    ));
    // Megabytes of opening brackets are rejected by the pre-scan, not parsed.
    let hostile = "[".repeat(4 * 1024 * 1024);
    assert!(matches!(
        parse_envelope(&hostile),
        Err(EnvelopeError::Json(_))
    ));
}

#[mz_ore::test]
fn envelope_depth_just_under_cap_parses_and_drops() {
    let nodes = (MAX_JSON_DEPTH - 8) / 3;
    assert!(8 + 3 * nodes <= MAX_JSON_DEPTH);
    // Runs on the ordinary test thread stack, so the drop is covered too.
    let envelope = parse_envelope(&nested_negate_json(nodes)).expect("under the cap");
    assert_eq!(envelope.objects.len(), 1);
    drop(envelope);
}

#[mz_ore::test]
fn envelope_depth_ignores_brackets_in_strings() {
    let many = "[".repeat(2 * MAX_JSON_DEPTH);
    assert!(exceeds_max_depth(&many));
    assert!(!exceeds_max_depth(&format!(r#"["{many}"]"#)));
    // An escaped quote does not end the string.
    assert!(!exceeds_max_depth(&format!(r#"["\"{many}"]"#)));
}

#[mz_ore::test]
fn substitute_global_ids_rewrites_gets() {
    let (a, b, c) = (
        GlobalId::Transient(1),
        GlobalId::Transient(2),
        GlobalId::Transient(3),
    );
    let mut plan = node(
        3,
        LirRelationNode::Union {
            inputs: vec![
                get(1, a),
                node(
                    2,
                    LirRelationNode::Negate {
                        input: Box::new(get(4, b)),
                    },
                ),
            ],
            consolidate_output: false,
            temporal_bucketing_strategies: vec![],
        },
    );
    let map = BTreeMap::from([(a, c)]);
    substitute_global_ids(&mut plan, &map);

    let expected = node(
        3,
        LirRelationNode::Union {
            inputs: vec![
                get(1, c),
                node(
                    2,
                    LirRelationNode::Negate {
                        input: Box::new(get(4, b)),
                    },
                ),
            ],
            consolidate_output: false,
            temporal_bucketing_strategies: vec![],
        },
    );
    assert_eq!(plan, expected);
}

/// The predicate the source import of [`extract_dataflow`] pushes down.
fn pushdown_predicate() -> mz_expr::MirScalarExpr {
    mz_expr::MirScalarExpr::column(1).call_is_null().not()
}

/// A dataflow that reads `u1` from persist with pushed-down operators and `u2` through its
/// index `u3`, and builds `u10` from both and `u11` from `u10` and a constant.
fn extract_dataflow() -> LirDataflowDescription {
    use mz_expr::MirScalarExpr;
    use mz_repr::{SqlRelationType, SqlScalarType, Timestamp};
    use timely::progress::Antichain;

    use crate::dataflows::{BuildDesc, IndexDesc, IndexImport, SourceImport};
    use crate::sources::{SourceInstanceArguments, SourceInstanceDesc};

    let typ = SqlRelationType::new(vec![
        SqlScalarType::Int64.nullable(false),
        SqlScalarType::Int64.nullable(true),
    ]);
    let mut df = LirDataflowDescription::new("extract".to_string());
    df.source_imports.insert(
        GlobalId::User(1),
        SourceImport {
            desc: SourceInstanceDesc {
                arguments: SourceInstanceArguments {
                    operators: Some(
                        MapFilterProject::new(2)
                            .filter([pushdown_predicate()])
                            .project([1]),
                    ),
                },
                storage_metadata: (),
                typ: typ.clone(),
            },
            monotonic: false,
            with_snapshot: true,
            upper: Antichain::from_elem(Timestamp::MIN),
        },
    );
    df.index_imports.insert(
        GlobalId::User(3),
        IndexImport {
            desc: IndexDesc {
                on_id: GlobalId::User(2),
                key: vec![MirScalarExpr::column(0)],
            },
            typ: mz_repr::ReprRelationType::from(&typ),
            monotonic: false,
            with_snapshot: true,
        },
    );
    let interval = node(
        6,
        LirRelationNode::Constant {
            rows: Ok(vec![(
                StableRow(Row::pack_slice(&[Datum::Interval(Default::default())])),
                mz_repr::Timestamp::from(0u64),
                mz_repr::Diff::from(1i64),
            )]),
        },
    );
    df.objects_to_build.push(BuildDesc {
        id: GlobalId::User(10),
        plan: node(
            3,
            LirRelationNode::Union {
                inputs: vec![get(1, GlobalId::User(1)), get(2, GlobalId::User(2))],
                consolidate_output: false,
                temporal_bucketing_strategies: vec![],
            },
        ),
    });
    df.objects_to_build.push(BuildDesc {
        id: GlobalId::User(11),
        plan: node(
            7,
            LirRelationNode::Union {
                inputs: vec![get(4, GlobalId::User(10)), interval],
                consolidate_output: false,
                temporal_bucketing_strategies: vec![],
            },
        ),
    });
    df
}

fn name_of(id: GlobalId) -> String {
    id.to_string()
}

#[mz_ore::test]
fn extract_then_parse_roundtrip() {
    let df = extract_dataflow();
    let envelope = extract_envelope(&df, GlobalId::User(11), &name_of).expect("extracts");

    let (t1, t2, t3, t4, t5) = (
        GlobalId::Transient(1),
        GlobalId::Transient(2),
        GlobalId::Transient(3),
        GlobalId::Transient(4),
        GlobalId::Transient(5),
    );
    let predicate = LirScalarExpr::try_from(&pushdown_predicate()).expect("materializable");
    let pushdown = MfpPlan::create_from(
        MapFilterProject::<LirScalarExpr>::new(2)
            .filter([predicate])
            .project([1]),
    )
    .expect("no temporal filters");
    let mut output_plan = df.objects_to_build[1].plan.clone();
    substitute_global_ids(
        &mut output_plan,
        &BTreeMap::from([(GlobalId::User(10), t4)]),
    );
    let expected = LirPlanEnvelope {
        lir_version: LIR_VERSION,
        imports: vec![
            EnvelopeImport {
                id: t1,
                source: ImportSource::Name("u1".into()),
                persist: true,
                indexes: vec![],
            },
            EnvelopeImport {
                id: t2,
                source: ImportSource::Name("u2".into()),
                persist: false,
                indexes: vec![ImportSource::Name("u3".into())],
            },
        ],
        objects: vec![
            // The pushdown takes fresh LIR ids above the dataflow's largest, 7.
            EnvelopeObject {
                id: t3,
                plan: node(
                    9,
                    LirRelationNode::Mfp {
                        input: Box::new(get(8, t1)),
                        mfp: pushdown,
                        input_key_val: None,
                    },
                ),
            },
            EnvelopeObject {
                id: t4,
                plan: node(
                    3,
                    LirRelationNode::Union {
                        inputs: vec![get(1, t3), get(2, t2)],
                        consolidate_output: false,
                        temporal_bucketing_strategies: vec![],
                    },
                ),
            },
            EnvelopeObject {
                id: t5,
                plan: output_plan,
            },
        ],
        output: t5,
    };
    assert_eq!(envelope, expected);

    // Rows without a readable form, like the interval, survive in the default form.
    let json = envelope_to_json(&envelope);
    assert_eq!(parse_envelope(&json), Ok(envelope));

    assert_eq!(check_extract_round_trip(&df), Ok(()));
}

#[mz_ore::test]
fn extract_rejects_output_that_is_not_an_object() {
    let df = extract_dataflow();
    let err = extract_envelope(&df, GlobalId::User(1), &name_of).unwrap_err();
    assert!(err.contains("u1"), "{err}");
}

#[mz_ore::test]
fn extract_rejects_pushdown_on_indexed_relation() {
    let mut df = extract_dataflow();
    let import = df.index_imports.get_mut(&GlobalId::User(3)).expect("index");
    import.desc.on_id = GlobalId::User(1);
    let err = extract_envelope(&df, GlobalId::User(11), &name_of).unwrap_err();
    assert!(err.contains("u1"), "{err}");
}

#[mz_ore::test]
fn extract_skips_identity_pushdown() {
    let mut df = extract_dataflow();
    let import = df
        .source_imports
        .get_mut(&GlobalId::User(1))
        .expect("source");
    import.desc.arguments.operators = Some(MapFilterProject::new(2));
    let envelope = extract_envelope(&df, GlobalId::User(11), &name_of).expect("extracts");
    assert_eq!(envelope.objects.len(), 2);
    assert_eq!(check_extract_round_trip(&df), Ok(()));
}

#[mz_ore::test]
fn round_trip_reports_lost_index_import() {
    // A name that does not resolve back to the import it came from is a mismatch.
    let df = extract_dataflow();
    let mut envelope = extract_envelope(&df, GlobalId::User(11), &name_of).expect("extracts");
    envelope.imports[1].indexes = vec![ImportSource::Name("u4".into())];
    let err = restore_and_compare(&df, GlobalId::User(11), &envelope).unwrap_err();
    assert!(err.contains("index imports"), "{err}");
}

#[mz_ore::test]
fn round_trip_rejects_import_without_access() {
    let df = extract_dataflow();
    let mut envelope = extract_envelope(&df, GlobalId::User(11), &name_of).expect("extracts");
    envelope.imports[1].indexes.clear();
    let err = restore_and_compare(&df, GlobalId::User(11), &envelope).unwrap_err();
    assert!(err.contains("neither persist nor an index"), "{err}");
}

#[mz_ore::test]
fn round_trip_reports_skipped_pushdown_redirect() {
    // Reading the raw import instead of the pushdown object drops the pushed-down filter, yet
    // both placeholders map back to `u1`.
    let df = extract_dataflow();
    let mut envelope = extract_envelope(&df, GlobalId::User(11), &name_of).expect("extracts");
    let (raw, pushdown) = (GlobalId::Transient(1), GlobalId::Transient(3));
    substitute_global_ids(
        &mut envelope.objects[1].plan,
        &BTreeMap::from([(pushdown, raw)]),
    );
    let err = restore_and_compare(&df, GlobalId::User(11), &envelope).unwrap_err();
    assert!(err.contains("raw import"), "{err}");
}

#[mz_ore::test]
fn extract_temporal_pushdown_has_bounds() {
    use mz_expr::{MirScalarExpr, UnmaterializableFunc, func};

    let mz_now = || MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow);
    let mut df = extract_dataflow();
    let import = df
        .source_imports
        .get_mut(&GlobalId::User(1))
        .expect("source");
    import.desc.arguments.operators = Some(MapFilterProject::new(2).filter([
        mz_now().call_binary(MirScalarExpr::column(0), func::Gte),
        mz_now().call_binary(MirScalarExpr::column(1), func::Lt),
    ]));
    let envelope = extract_envelope(&df, GlobalId::User(11), &name_of).expect("extracts");

    let LirRelationNode::Mfp { mfp, .. } = &envelope.objects[0].plan.node else {
        panic!("expected a pushdown Mfp, got {:?}", envelope.objects[0]);
    };
    let (safe, lower, upper) = mfp.clone().into_parts();
    // `mz_now() >= #0` bounds from below by `#0`, `mz_now() < #1` from above by `#1`.
    assert_eq!(lower, vec![LirScalarExpr::column(0)]);
    assert_eq!(upper, vec![LirScalarExpr::column(1)]);
    assert!(safe.into_mfp().predicates.is_empty());
    assert_eq!(check_extract_round_trip(&df), Ok(()));
}
