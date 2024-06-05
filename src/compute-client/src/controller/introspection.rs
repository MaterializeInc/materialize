// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use mz_compute_types::dataflows::{BuildDesc, DataflowDescription, IndexDesc, IndexImport};
use mz_compute_types::plan::reduce::{AccumulablePlan, KeyValPlan, ReducePlan};
use mz_compute_types::plan::{AvailableCollections, GetPlan, Plan};
use mz_compute_types::sinks::{ComputeSinkConnection, ComputeSinkDesc, SubscribeSinkConnection};
use mz_expr::{
    permutation_for_arrangement, AggregateExpr, AggregateFunc, Id, MapFilterProject, MirScalarExpr,
};
use mz_ore::collections::CollectionExt;
use mz_ore::id_gen::IdGen;
use mz_repr::global_id::TransientIdGen;
use mz_repr::{GlobalId, RelationDesc, ScalarType};

use crate::controller::ComputeControllerTimestamp;
use crate::logging::{ComputeLog, LogVariant};

pub(super) fn build_subscribe_dataflows<T>(
    id_gen: Arc<TransientIdGen>,
    log_indexes: &BTreeMap<LogVariant, GlobalId>,
) -> Vec<(GlobalId, DataflowDescription<Plan<T>, (), T>)>
where
    T: ComputeControllerTimestamp,
{
    let variant = LogVariant::Compute(ComputeLog::ErrorCount);

    let index_id = log_indexes[&variant];
    let sink_id = id_gen.allocate_id();
    let view_id = id_gen.allocate_id();

    let index_key: Vec<_> = variant
        .index_by()
        .into_iter()
        .map(MirScalarExpr::Column)
        .collect();
    let index_typ = variant.desc().typ().clone();
    let (permutation, thinning) = permutation_for_arrangement(&index_key, 3);
    let group_key = [MirScalarExpr::Column(0)];
    let aggregates = vec![AggregateExpr {
        func: AggregateFunc::SumInt64,
        expr: MirScalarExpr::Column(1),
        distinct: false,
    }];

    let mut lir_id_gen = IdGen::default();

    let dataflow = DataflowDescription {
        source_imports: Default::default(),
        index_imports: [(
            index_id,
            IndexImport {
                desc: IndexDesc {
                    on_id: index_id,
                    key: index_key.clone(),
                },
                typ: index_typ.clone(),
                monotonic: false,
            },
        )]
        .into(),
        objects_to_build: vec![BuildDesc {
            id: view_id,
            plan: Plan::Reduce {
                input: Box::new(Plan::Get {
                    id: Id::Global(index_id),
                    keys: AvailableCollections::new_arranged(
                        vec![(index_key.clone(), permutation, thinning)],
                        Some(index_typ.column_types),
                    ),
                    plan: GetPlan::Arrangement(
                        index_key.clone(),
                        None,
                        MapFilterProject::new(3).project([0, 2]),
                    ),
                    lir_id: lir_id_gen.allocate_id(),
                }),
                key_val_plan: KeyValPlan::new(2, &group_key, &aggregates, None),
                plan: ReducePlan::Accumulable(AccumulablePlan {
                    full_aggrs: aggregates.clone(),
                    simple_aggrs: vec![(0, 0, aggregates.into_element())],
                    distinct_aggrs: Default::default(),
                }),
                input_key: None,
                mfp_after: MapFilterProject::new(2),
                lir_id: lir_id_gen.allocate_id(),
            },
        }],
        index_exports: Default::default(),
        sink_exports: [(
            sink_id,
            ComputeSinkDesc {
                from: view_id,
                from_desc: RelationDesc::empty()
                    .with_column("export_id", ScalarType::String.nullable(false))
                    .with_column("count", ScalarType::Int64.nullable(false))
                    .with_key(vec![0]),
                connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection {}),
                with_snapshot: true,
                up_to: Default::default(),
                non_null_assertions: Default::default(),
                refresh_schedule: None,
            },
        )]
        .into(),
        as_of: None,
        until: Default::default(),
        initial_storage_as_of: None,
        refresh_schedule: None,
        debug_name: format!("introspection-subscribe-error-count-{sink_id}"),
    };

    vec![(sink_id, dataflow)]
}
