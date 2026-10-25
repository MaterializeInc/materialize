// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_compute_types::dataflows::{BuildDesc, IndexDesc, IndexImport};
use mz_compute_types::plan::scalar::LirScalarExpr;
use mz_expr::{
    AccessStrategy, Id, MirScalarExpr, OptimizedMirRelationExpr, UnmaterializableFunc, func,
};
use mz_repr::explain::IndexUsageType;
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{ReprRelationType, ReprScalarType, SqlRelationType};

use super::*;

const TABLE: GlobalId = GlobalId::User(1);
const FIRST_INDEX: GlobalId = GlobalId::User(2);
const SECOND_INDEX: GlobalId = GlobalId::User(3);
const VIEW: GlobalId = GlobalId::Transient(1);
const SINK: GlobalId = GlobalId::Transient(2);

/// Three non-nullable `int8` columns.
fn typ() -> ReprRelationType {
    ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false); 3])
}

fn get_table() -> MirRelationExpr {
    MirRelationExpr::Get {
        id: Id::Global(TABLE),
        typ: typ(),
        access_strategy: AccessStrategy::Index(vec![(FIRST_INDEX, IndexUsageType::FullScan)]),
    }
}

/// A batched subscribe dataflow that builds `VIEW` from `plan` and reads `TABLE` through one
/// index import per entry of `keys`, with ids `FIRST_INDEX` and `SECOND_INDEX`.
fn dataflow(plan: MirRelationExpr, keys: &[Vec<usize>]) -> MirDataflowDescription {
    let mut df = MirDataflowDescription::new("test".to_string());
    let ids = [FIRST_INDEX, SECOND_INDEX];
    for (i, key) in keys.iter().enumerate() {
        df.index_imports.insert(
            ids[i],
            IndexImport {
                desc: IndexDesc {
                    on_id: TABLE,
                    key: key.iter().copied().map(MirScalarExpr::column).collect(),
                },
                typ: typ(),
                monotonic: false,
                with_snapshot: true,
            },
        );
    }
    let view_desc = RelationDesc::new(
        SqlRelationType::from_repr(&plan.typ()),
        (0..plan.arity()).map(|c| format!("v{c}")),
    );
    df.objects_to_build.push(BuildDesc {
        id: VIEW,
        plan: OptimizedMirRelationExpr::declare_optimized(plan),
    });
    df.export_sink(
        SINK,
        ComputeSinkDesc {
            from: VIEW,
            from_desc: view_desc.clone(),
            connection: ComputeSinkConnection::BatchedSubscribe(BatchedSubscribeSinkConnection {
                stash_id: Uuid::nil(),
                with_snapshot: true,
                output_desc: view_desc,
                mfp: None,
            }),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: vec![],
            refresh_schedule: None,
            from_arrangement: None,
        },
    );
    df.set_as_of(Antichain::from_elem(Timestamp::MIN));
    df
}

/// Applies the rewrite and lowers the dataflow, returning the lowered sink.
fn rewrite_and_lower(mut df: MirDataflowDescription) -> ComputeSinkDesc {
    apply_batched_index_rewrite(&mut df);
    let lowered = LirRelationExpr::finalize_dataflow(df, &OptimizerFeatures::default(), None)
        .expect("lowers");
    lowered.sink_exports[&SINK].clone()
}

fn arrangement_key(sink: &ComputeSinkDesc) -> Option<Vec<LirScalarExpr>> {
    sink.from_arrangement.as_ref().map(|(key, _)| key.clone())
}

fn connection_mfp(sink: &ComputeSinkDesc) -> Option<SafeMfpPlan> {
    let ComputeSinkConnection::BatchedSubscribe(connection) = &sink.connection else {
        panic!("not a batched subscribe: {:?}", sink.connection);
    };
    connection.mfp.clone()
}

#[mz_ore::test]
fn a_filter_over_an_index_takes_the_arranged_path() {
    let predicate = MirScalarExpr::column(1).call_binary(MirScalarExpr::column(2), func::Lt);
    let plan = get_table().filter([predicate.clone()]);
    let output_desc = {
        let df = dataflow(plan.clone(), &[vec![0]]);
        df.sink_exports[&SINK].from_desc.clone()
    };

    let sink = rewrite_and_lower(dataflow(plan, &[vec![0]]));

    assert_eq!(sink.from, TABLE, "the sink reads the indexed collection");
    assert_eq!(sink.from_desc.arity(), 3);
    assert_eq!(
        arrangement_key(&sink),
        Some(vec![LirScalarExpr::column(0)]),
        "the sink reads the index arrangement",
    );
    let expected = MapFilterProject::new(3)
        .filter([predicate])
        .into_plan()
        .and_then(|plan| plan.into_nontemporal().map_err(|e| format!("{e:?}")))
        .expect("non-temporal");
    assert_eq!(connection_mfp(&sink), Some(expected));
    let ComputeSinkConnection::BatchedSubscribe(connection) = &sink.connection else {
        unreachable!("checked by `connection_mfp`");
    };
    assert_eq!(
        connection.output_desc, output_desc,
        "the output stays the subscribed relation's"
    );
}

#[mz_ore::test]
fn an_identity_over_an_index_needs_no_mfp() {
    let mut df = dataflow(get_table(), &[vec![0]]);
    apply_batched_index_rewrite(&mut df);
    assert!(
        df.objects_to_build.is_empty(),
        "the view is no longer built"
    );

    let sink = rewrite_and_lower(df);

    assert_eq!(sink.from, TABLE);
    assert_eq!(arrangement_key(&sink), Some(vec![LirScalarExpr::column(0)]));
    assert_eq!(connection_mfp(&sink), None);
}

#[mz_ore::test]
fn a_projection_dropping_a_key_column_takes_the_general_path() {
    let df = dataflow(get_table().project(vec![1, 2]), &[vec![0]]);
    assert_eq!(batched_index_rewrite(&df, VIEW), None);

    let sink = rewrite_and_lower(df);

    assert_eq!(sink.from, VIEW, "the sink reads the view");
    assert_eq!(arrangement_key(&sink), None);
    assert_eq!(connection_mfp(&sink), None);
}

#[mz_ore::test]
fn a_computed_key_column_takes_the_general_path() {
    // The output's first column is `#0 + #0`, not the key column `#0` itself.
    let plan = get_table()
        .map(vec![
            MirScalarExpr::column(0).call_binary(MirScalarExpr::column(0), func::AddInt64),
        ])
        .project(vec![3, 1, 2]);
    let df = dataflow(plan, &[vec![0]]);

    assert_eq!(batched_index_rewrite(&df, VIEW), None);
}

#[mz_ore::test]
fn a_temporal_filter_takes_the_general_path() {
    let mz_now = MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow);
    let bound = MirScalarExpr::column(1).call_unary(func::CastInt64ToMzTimestamp);
    let plan = get_table().filter([mz_now.call_binary(bound, func::Lte)]);
    let df = dataflow(plan, &[vec![0]]);
    assert_eq!(batched_index_rewrite(&df, VIEW), None);

    let sink = rewrite_and_lower(df);

    assert_eq!(sink.from, VIEW, "the sink reads the view");
    assert_eq!(arrangement_key(&sink), None);
}

#[mz_ore::test]
fn the_rewrite_picks_the_index_lowering_picks() {
    // Both indexes pass the key check. Lowering reads the first in id order.
    let sink = rewrite_and_lower(dataflow(get_table(), &[vec![1], vec![0]]));

    assert_eq!(sink.from, TABLE);
    assert_eq!(arrangement_key(&sink), Some(vec![LirScalarExpr::column(1)]));
}

#[mz_ore::test]
fn only_the_index_lowering_picks_is_checked() {
    // The first index's key column `#1` is projected away. The second index's key `#0` is kept,
    // but lowering would not read that index.
    let df = dataflow(get_table().project(vec![0, 2]), &[vec![1], vec![0]]);

    assert_eq!(batched_index_rewrite(&df, VIEW), None);
}

#[mz_ore::test]
fn a_view_read_elsewhere_stays_built() {
    let mut df = dataflow(get_table(), &[vec![0]]);
    let mut other = df.sink_exports[&SINK].clone();
    other.connection = ComputeSinkConnection::Subscribe(SubscribeSinkConnection { output: vec![] });
    df.export_sink(GlobalId::Transient(3), other);

    apply_batched_index_rewrite(&mut df);

    assert_eq!(df.sink_exports[&SINK].from, TABLE);
    assert_eq!(df.sink_exports[&GlobalId::Transient(3)].from, VIEW);
    assert_eq!(
        df.objects_to_build.iter().map(|b| b.id).collect::<Vec<_>>(),
        vec![VIEW]
    );
}
