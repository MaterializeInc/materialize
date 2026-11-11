// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::Cell;
use std::time::Duration;

use mz_expr::{MapFilterProject, UnmaterializableFunc, func};
use mz_repr::refresh_schedule::RefreshEvery;
use mz_repr::{Datum, ReprScalarType, SqlScalarType, Timestamp};

use super::*;
use crate::plan::scalar::mfp_plan_mir_to_lir;
use crate::plan::{ArrangementStrategy, AvailableCollections, GetPlan, LirId};
use crate::sinks::SubscribeSinkConnection;

const SOURCE: GlobalId = GlobalId::User(1);
const IMPORTED_INDEX: GlobalId = GlobalId::User(2);
const ON: GlobalId = GlobalId::User(3);
const INDEX: GlobalId = GlobalId::User(4);
const MV_SINK: GlobalId = GlobalId::User(5);
const VIEW: GlobalId = GlobalId::Transient(1);

///////////////////////////////////////////////////////////////////////////
// FIXTURES
///////////////////////////////////////////////////////////////////////////

fn repr_typ() -> ReprRelationType {
    ReprRelationType::new(vec![ReprScalarType::Int64.nullable(true)])
}

fn sql_typ() -> SqlRelationType {
    SqlRelationType::new(vec![SqlScalarType::Int64.nullable(true)])
}

fn key() -> Vec<MirScalarExpr> {
    vec![MirScalarExpr::column(0)]
}

fn lir_key() -> Vec<LirScalarExpr> {
    key()
        .iter()
        .map(|e| LirScalarExpr::try_from(e).unwrap())
        .collect()
}

fn identity_mfp() -> MfpPlan<LirScalarExpr> {
    MfpPlan::create_from(MapFilterProject::<LirScalarExpr>::new(1)).unwrap()
}

fn get(lir_id: u64, id: GlobalId) -> LirRelationExpr {
    LirRelationExpr {
        lir_id: LirId(lir_id),
        node: LirRelationNode::Get {
            id: Id::Global(id),
            keys: AvailableCollections::new_raw(),
            plan: GetPlan::Collection(identity_mfp()),
        },
    }
}

///////////////////////////////////////////////////////////////////////////
// TESTS
///////////////////////////////////////////////////////////////////////////

/// A dataflow reading `SOURCE` (with a pushed-down filter, including a
/// temporal bound) and `IMPORTED_INDEX`, building `VIEW`, with no exports.
fn imports_only() -> LirDataflowDescription {
    let mz_now = MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow);
    let bound = MirScalarExpr::literal_ok(
        Datum::MzTimestamp(Timestamp::from(5u64)),
        ReprScalarType::MzTimestamp,
    );
    let mfp = MapFilterProject::new(1).filter([
        MirScalarExpr::column(0).call_is_null().not(),
        mz_now.call_binary(bound, func::Gte),
    ]);
    let mfp = mfp_plan_mir_to_lir(MfpPlan::create_from(mfp).expect("temporal bound is plannable"));
    let mut df = DataflowDescription::new("test".to_string());
    df.source_imports.insert(
        SOURCE,
        SourceImport {
            desc: SourceInstanceDesc {
                arguments: SourceInstanceArguments {
                    operators: Some(mfp),
                },
                storage_metadata: (),
                typ: sql_typ(),
            },
            monotonic: true,
            with_snapshot: true,
            upper: Antichain::new(),
        },
    );
    df.index_imports.insert(
        IMPORTED_INDEX,
        IndexImport {
            desc: IndexDesc {
                on_id: ON,
                key: key(),
            },
            typ: repr_typ(),
            monotonic: false,
            with_snapshot: true,
        },
    );
    df.objects_to_build.push(BuildDesc {
        id: VIEW,
        plan: get(1, SOURCE),
    });
    df
}

fn mv_desc() -> RelationDesc {
    RelationDesc::new(sql_typ(), ["c"])
}

fn schedule() -> RefreshSchedule {
    RefreshSchedule {
        everies: vec![RefreshEvery {
            interval: Duration::from_secs(60),
            aligned_to: Timestamp::from(0u64),
        }],
        ats: vec![Timestamp::from(10u64)],
    }
}

/// `mv_desc()` with column 0 asserted non-null, which is what both the
/// catalog and the optimizer hold for a view with that assertion.
fn asserted_mv_desc() -> RelationDesc {
    RelationDesc::new(
        SqlRelationType::new(vec![SqlScalarType::Int64.nullable(false)]),
        mv_desc().iter_names().cloned(),
    )
}

fn mv_info() -> MaterializedViewInfo {
    MaterializedViewInfo {
        desc: asserted_mv_desc(),
        non_null_assertions: vec![0],
        refresh_schedule: Some(schedule()),
    }
}

/// The sink the materialized view optimizer emits for `mv_info()`.
fn mv_sink(from: GlobalId) -> ComputeSinkDesc {
    let asserted = asserted_mv_desc();
    ComputeSinkDesc {
        from,
        from_desc: asserted.clone(),
        connection: ComputeSinkConnection::MaterializedView(MaterializedViewSinkConnection {
            value_desc: asserted,
            storage_metadata: (),
        }),
        with_snapshot: true,
        up_to: Antichain::new(),
        non_null_assertions: vec![0],
        refresh_schedule: Some(schedule()),
        from_arrangement: Some((lir_key(), identity_mfp())),
    }
}

/// What the catalog says about `IMPORTED_INDEX` when it matches the
/// fixtures.
fn imported_index_info() -> IndexInfo {
    IndexInfo {
        on: ON,
        keys: key(),
        typ: repr_typ(),
    }
}

/// Transient ids are handed out from `next_transient` upward. The
/// fixtures number their builds from 1, so a context starting at 1
/// reproduces them.
struct Ctx {
    collections: BTreeMap<GlobalId, SqlRelationType>,
    indexes: BTreeMap<GlobalId, IndexInfo>,
    mv: Option<MaterializedViewInfo>,
    next_transient: Cell<u64>,
}

impl Ctx {
    /// A context whose catalog matches the imports of `imports_only()`
    /// and holds the given exported index and materialized view.
    fn new(index: Option<IndexInfo>, mv: Option<MaterializedViewInfo>) -> Self {
        let mut indexes = BTreeMap::from([(IMPORTED_INDEX, imported_index_info())]);
        if let Some(index) = index {
            indexes.insert(INDEX, index);
        }
        Ctx {
            collections: BTreeMap::from([(SOURCE, sql_typ())]),
            indexes,
            mv,
            next_transient: Cell::new(1),
        }
    }
}

impl InstantiationContext for Ctx {
    fn collection_type(&self, id: GlobalId) -> Option<SqlRelationType> {
        self.collections.get(&id).cloned()
    }
    fn index(&self, id: GlobalId) -> Option<IndexInfo> {
        self.indexes.get(&id).cloned()
    }
    fn materialized_view(&self, _id: GlobalId) -> Option<MaterializedViewInfo> {
        self.mv.clone()
    }
    fn metric_sink(&self, _id: GlobalId) -> Option<MetricSinkInfo> {
        None
    }
    fn allocate_transient_id(&self) -> GlobalId {
        let id = self.next_transient.get();
        self.next_transient.set(id + 1);
        GlobalId::Transient(id)
    }
}

fn instantiate(df: &LirDataflowDescription, ctx: &Ctx) -> LirDataflowDescription {
    PinnedDataflow::try_from(df.clone())
        .expect("pinnable")
        .instantiate(ctx, df.debug_name.clone())
        .expect("instantiates")
}

#[mz_ore::test]
fn rejects_one_shot_sinks() {
    let mut df = imports_only();
    df.sink_exports.insert(
        MV_SINK,
        ComputeSinkDesc {
            from: VIEW,
            from_desc: mv_desc(),
            connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection {
                output: Vec::new(),
            }),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: Vec::new(),
            refresh_schedule: None,
            from_arrangement: None,
        },
    );
    assert_eq!(
        PinnedDataflow::try_from(df),
        Err(PinError::UnpinnableSink(MV_SINK))
    );
}

#[mz_ore::test]
fn rejects_transient_index_exports() {
    let mut df = imports_only();
    let peek_index = GlobalId::Transient(9);
    df.index_exports.insert(
        peek_index,
        (
            IndexDesc {
                on_id: VIEW,
                key: key(),
            },
            repr_typ(),
        ),
    );
    assert_eq!(
        PinnedDataflow::try_from(df),
        Err(PinError::TransientIndexExport(peek_index))
    );
}

#[mz_ore::test]
#[should_panic(expected = "pinning a dataflow with no exports")]
fn rejects_dataflows_without_exports() {
    let _ = PinnedDataflow::try_from(imports_only());
}

fn materialized_view_dataflow() -> LirDataflowDescription {
    let mut df = imports_only();
    df.sink_exports.insert(MV_SINK, mv_sink(VIEW));
    df.refresh_schedule = Some(schedule());
    df
}

#[mz_ore::test]
fn materialized_view_round_trips() {
    let df = materialized_view_dataflow();

    let pinned = PinnedDataflow::try_from(df.clone()).unwrap();
    let (_, lower, upper) = pinned.source_imports[&SOURCE]
        .operators
        .clone()
        .expect("pushed-down operators")
        .into_parts();
    assert_eq!(
        (lower.len(), upper.len()),
        (1, 0),
        "temporal bound is pinned as a bound"
    );
    let ctx = Ctx::new(None, Some(mv_info()));
    let rebuilt = pinned.instantiate(&ctx, "test".to_string()).unwrap();
    assert_eq!(rebuilt, df);
}

#[mz_ore::test]
fn rejects_builtin_collections() {
    fn move_key<V>(map: &mut BTreeMap<GlobalId, V>, from: GlobalId, to: GlobalId) {
        let value = map.remove(&from).expect("fixture has the id");
        map.insert(to, value);
    }
    let builtin = GlobalId::System(1);

    let mut df = materialized_view_dataflow();
    move_key(&mut df.source_imports, SOURCE, builtin);
    assert_eq!(
        PinnedDataflow::try_from(df),
        Err(PinError::BuiltinCollection(builtin)),
        "a builtin source import"
    );

    let mut df = materialized_view_dataflow();
    move_key(&mut df.index_imports, IMPORTED_INDEX, builtin);
    assert_eq!(
        PinnedDataflow::try_from(df),
        Err(PinError::BuiltinCollection(builtin)),
        "a builtin index import"
    );

    let mut df = materialized_view_dataflow();
    move_key(&mut df.sink_exports, MV_SINK, builtin);
    assert_eq!(
        PinnedDataflow::try_from(df),
        Err(PinError::BuiltinCollection(builtin)),
        "a builtin materialized view"
    );

    let mut df = index_dataflow();
    move_key(&mut df.index_exports, INDEX, builtin);
    assert_eq!(
        PinnedDataflow::try_from(df),
        Err(PinError::BuiltinCollection(builtin)),
        "a builtin index"
    );

    let introspection = GlobalId::IntrospectionSourceIndex(1);
    let mut df = materialized_view_dataflow();
    move_key(&mut df.index_imports, IMPORTED_INDEX, introspection);
    assert_eq!(
        PinnedDataflow::try_from(df),
        Err(PinError::BuiltinCollection(introspection)),
        "an introspection index import"
    );
}

#[mz_ore::test]
fn materialized_view_key_drift_is_detected() {
    let ctx = Ctx::new(
        None,
        Some(MaterializedViewInfo {
            desc: RelationDesc::new(asserted_mv_desc().into_typ().with_key(vec![0]), ["c"]),
            ..mv_info()
        }),
    );
    let pinned = PinnedDataflow::try_from(materialized_view_dataflow()).unwrap();
    let detail = export_drift_detail(pinned.instantiate(&ctx, "test".to_string()), MV_SINK);
    assert!(detail.contains("pinned type"), "{detail}");
}

#[mz_ore::test]
fn materialized_view_column_type_drift_is_detected() {
    let pinned = PinnedDataflow::try_from(materialized_view_dataflow()).unwrap();
    let ctx = Ctx::new(
        None,
        Some(MaterializedViewInfo {
            desc: RelationDesc::new(
                SqlRelationType::new(vec![SqlScalarType::String.nullable(true)]),
                ["c"],
            ),
            ..mv_info()
        }),
    );
    let detail = export_drift_detail(pinned.instantiate(&ctx, "test".to_string()), MV_SINK);
    assert!(detail.contains("column 0"), "{detail}");
}

#[mz_ore::test]
fn pinned_dataflow_round_trips_through_json() {
    let pinned = PinnedDataflow::try_from(materialized_view_dataflow()).unwrap();
    let json = serde_json::to_string(&pinned).expect("serializes as JSON");
    assert!(json.contains("\"u1\":"), "map keys are id strings: {json}");
    let parsed: PinnedDataflow = serde_json::from_str(&json).expect("parses back");
    assert_eq!(parsed, pinned);
}

#[mz_ore::test]
fn instantiate_reallocates_transient_build_ids() {
    let mut df = materialized_view_dataflow();
    // A second build reading the first, and the sink reading the second,
    // so both a `Get` and a sink `from` must follow the renaming.
    let inner = GlobalId::Transient(2);
    df.objects_to_build.push(BuildDesc {
        id: inner,
        plan: get(2, VIEW),
    });
    df.sink_exports.insert(MV_SINK, mv_sink(inner));

    let ctx = Ctx::new(None, Some(mv_info()));
    ctx.next_transient.set(7);
    let rebuilt = instantiate(&df, &ctx);

    let ids: Vec<_> = rebuilt.objects_to_build.iter().map(|o| o.id).collect();
    assert_eq!(ids, [GlobalId::Transient(7), GlobalId::Transient(8)]);
    assert!(
        matches!(
            &rebuilt.objects_to_build[1].plan.node,
            LirRelationNode::Get {
                id: Id::Global(GlobalId::Transient(7)),
                ..
            }
        ),
        "the Get follows the renamed build"
    );
    assert_eq!(rebuilt.sink_exports[&MV_SINK].from, GlobalId::Transient(8));
    assert_eq!(
        rebuilt.source_imports.keys().copied().collect::<Vec<_>>(),
        [SOURCE]
    );
}

fn index_dataflow() -> LirDataflowDescription {
    let mut df = imports_only();
    df.objects_to_build.push(BuildDesc {
        id: ON,
        plan: get(2, SOURCE),
    });
    df.objects_to_build.push(BuildDesc {
        id: INDEX,
        plan: LirRelationExpr {
            lir_id: LirId(3),
            node: LirRelationNode::ArrangeBy {
                input_key: None,
                input: Box::new(get(4, ON)),
                input_mfp: identity_mfp(),
                forms: AvailableCollections::new_arranged(vec![(lir_key(), vec![0], vec![])]),
                strategy: ArrangementStrategy::Direct,
            },
        },
    });
    df.index_exports.insert(
        INDEX,
        (
            IndexDesc {
                on_id: ON,
                key: key(),
            },
            repr_typ(),
        ),
    );
    df
}

#[mz_ore::test]
fn index_round_trips() {
    let df = index_dataflow();
    let pinned = PinnedDataflow::try_from(df.clone()).unwrap();
    assert_eq!(pinned.index_exports, BTreeMap::from([(INDEX, ON)]));
    let ctx = Ctx::new(
        Some(IndexInfo {
            on: ON,
            keys: key(),
            typ: repr_typ(),
        }),
        None,
    );
    let rebuilt = pinned.instantiate(&ctx, "test".to_string()).unwrap();
    assert_eq!(rebuilt, df);
}

/// The shape lowering produces when `ON` already arranges by the key: the
/// index's build is a bare `Get` of `ON` carrying that arrangement.
fn index_dataflow_with_elided_arrange_by() -> LirDataflowDescription {
    let mut df = index_dataflow();
    let build = df
        .objects_to_build
        .iter_mut()
        .find(|object| object.id == INDEX)
        .unwrap();
    build.plan = LirRelationExpr {
        lir_id: LirId(3),
        node: LirRelationNode::Get {
            id: Id::Global(ON),
            keys: AvailableCollections::new_arranged(vec![(lir_key(), vec![0], vec![])]),
            plan: GetPlan::PassArrangements,
        },
    };
    df
}

#[mz_ore::test]
fn index_with_elided_arrange_by_round_trips() {
    let df = index_dataflow_with_elided_arrange_by();
    let ctx = Ctx::new(
        Some(IndexInfo {
            on: ON,
            keys: key(),
            typ: repr_typ(),
        }),
        None,
    );
    assert_eq!(instantiate(&df, &ctx), df);
}

#[mz_ore::test]
fn index_key_drift_is_detected() {
    let ctx = || {
        Ctx::new(
            Some(IndexInfo {
                on: ON,
                keys: vec![MirScalarExpr::column(0).call_is_null()],
                typ: repr_typ(),
            }),
            None,
        )
    };
    for df in [index_dataflow(), index_dataflow_with_elided_arrange_by()] {
        let pinned = PinnedDataflow::try_from(df).unwrap();
        assert!(matches!(
            pinned.instantiate(&ctx(), "test".to_string()),
            Err(InstantiateError::ExportDrift { id: INDEX, .. })
        ));
    }
}

#[mz_ore::test]
fn index_built_from_an_unverifiable_shape_is_drift() {
    let mut df = index_dataflow();
    let build = df
        .objects_to_build
        .iter_mut()
        .find(|object| object.id == INDEX)
        .unwrap();
    build.plan = LirRelationExpr {
        lir_id: LirId(3),
        node: LirRelationNode::Negate {
            input: Box::new(get(4, ON)),
        },
    };
    let pinned = PinnedDataflow::try_from(df).unwrap();
    let ctx = Ctx::new(
        Some(IndexInfo {
            on: ON,
            keys: key(),
            typ: repr_typ(),
        }),
        None,
    );
    assert!(matches!(
        pinned.instantiate(&ctx, "test".to_string()),
        Err(InstantiateError::ExportDrift { id: INDEX, .. })
    ));
}

#[mz_ore::test]
fn missing_catalog_item_is_detected() {
    let pinned = PinnedDataflow::try_from(index_dataflow()).unwrap();
    let ctx = Ctx::new(None, None);
    assert_eq!(
        pinned.instantiate(&ctx, "test".to_string()),
        Err(InstantiateError::MissingCatalogItem {
            id: INDEX,
            kind: "index"
        })
    );
}

/// Unwraps an `ExportDrift` on `id` and returns what disagreed.
fn export_drift_detail(
    result: Result<LirDataflowDescription, InstantiateError>,
    id: GlobalId,
) -> String {
    match result {
        Err(InstantiateError::ExportDrift {
            id: drifted,
            detail,
        }) if drifted == id => detail,
        other => panic!("expected export drift on {id}, got {other:?}"),
    }
}

#[mz_ore::test]
fn index_on_drift_is_detected() {
    let pinned = PinnedDataflow::try_from(index_dataflow()).unwrap();
    let ctx = Ctx::new(
        Some(IndexInfo {
            on: SOURCE,
            keys: key(),
            typ: repr_typ(),
        }),
        None,
    );
    let detail = export_drift_detail(pinned.instantiate(&ctx, "test".to_string()), INDEX);
    assert!(detail.contains("pinned on"), "{detail}");
}

#[mz_ore::test]
fn index_export_without_a_build_is_drift() {
    let mut df = index_dataflow();
    df.objects_to_build.retain(|object| object.id != INDEX);
    let pinned = PinnedDataflow::try_from(df).unwrap();
    let ctx = Ctx::new(
        Some(IndexInfo {
            on: ON,
            keys: key(),
            typ: repr_typ(),
        }),
        None,
    );
    let detail = export_drift_detail(pinned.instantiate(&ctx, "test".to_string()), INDEX);
    assert!(detail.contains("builds no object"), "{detail}");
}

#[mz_ore::test]
fn sink_reading_an_unbuilt_object_is_drift() {
    let mut df = materialized_view_dataflow();
    df.sink_exports
        .insert(MV_SINK, mv_sink(GlobalId::Transient(42)));
    let pinned = PinnedDataflow::try_from(df).unwrap();
    let ctx = Ctx::new(None, Some(mv_info()));
    let detail = export_drift_detail(pinned.instantiate(&ctx, "test".to_string()), MV_SINK);
    assert!(detail.contains("builds no object"), "{detail}");
}

#[mz_ore::test]
fn missing_materialized_view_is_detected() {
    let pinned = PinnedDataflow::try_from(materialized_view_dataflow()).unwrap();
    let ctx = Ctx::new(None, None);
    assert_eq!(
        pinned.instantiate(&ctx, "test".to_string()),
        Err(InstantiateError::MissingCatalogItem {
            id: MV_SINK,
            kind: "materialized view"
        })
    );
}

#[mz_ore::test]
fn missing_metric_sink_is_detected() {
    let mut df = imports_only();
    df.sink_exports.insert(
        MV_SINK,
        ComputeSinkDesc {
            from: VIEW,
            from_desc: mv_desc(),
            connection: ComputeSinkConnection::MetricSink(MetricSinkConnection {
                label: "u5".to_string(),
            }),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: Vec::new(),
            refresh_schedule: None,
            from_arrangement: None,
        },
    );
    let pinned = PinnedDataflow::try_from(df).unwrap();
    let ctx = Ctx::new(None, None);
    assert_eq!(
        pinned.instantiate(&ctx, "test".to_string()),
        Err(InstantiateError::MissingCatalogItem {
            id: MV_SINK,
            kind: "metric sink"
        })
    );
}

#[mz_ore::test]
fn missing_source_import_is_detected() {
    let pinned = PinnedDataflow::try_from(materialized_view_dataflow()).unwrap();
    let mut ctx = Ctx::new(None, Some(mv_info()));
    ctx.collections.remove(&SOURCE);
    assert_eq!(
        pinned.instantiate(&ctx, "test".to_string()),
        Err(InstantiateError::MissingCatalogItem {
            id: SOURCE,
            kind: "collection"
        })
    );
}

#[mz_ore::test]
fn missing_index_import_is_detected() {
    let pinned = PinnedDataflow::try_from(materialized_view_dataflow()).unwrap();
    let mut ctx = Ctx::new(None, Some(mv_info()));
    ctx.indexes.remove(&IMPORTED_INDEX);
    assert_eq!(
        pinned.instantiate(&ctx, "test".to_string()),
        Err(InstantiateError::MissingCatalogItem {
            id: IMPORTED_INDEX,
            kind: "index"
        })
    );
}

#[mz_ore::test]
fn source_import_type_drift_is_detected() {
    let pinned = PinnedDataflow::try_from(materialized_view_dataflow()).unwrap();
    let mut ctx = Ctx::new(None, Some(mv_info()));
    ctx.collections.insert(
        SOURCE,
        SqlRelationType::new(vec![SqlScalarType::String.nullable(true)]),
    );
    assert!(matches!(
        pinned.instantiate(&ctx, "test".to_string()),
        Err(InstantiateError::ImportDrift { id: SOURCE, .. })
    ));
}

#[mz_ore::test]
fn index_import_drift_is_detected() {
    let pinned = PinnedDataflow::try_from(materialized_view_dataflow()).unwrap();
    let altered = [
        IndexInfo {
            on: SOURCE,
            ..imported_index_info()
        },
        IndexInfo {
            keys: vec![MirScalarExpr::column(0).call_is_null()],
            ..imported_index_info()
        },
        IndexInfo {
            typ: ReprRelationType::new(vec![ReprScalarType::String.nullable(true)]),
            ..imported_index_info()
        },
    ];
    for info in altered {
        let mut ctx = Ctx::new(None, Some(mv_info()));
        ctx.indexes.insert(IMPORTED_INDEX, info);
        assert!(matches!(
            pinned.clone().instantiate(&ctx, "test".to_string()),
            Err(InstantiateError::ImportDrift {
                id: IMPORTED_INDEX,
                ..
            })
        ));
    }
}
