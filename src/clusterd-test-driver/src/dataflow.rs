// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Assembly of compute [`DataflowDescription`]s for the headless test driver.
//!
//! The single entry point [`index_dataflow`] builds a dataflow that imports a
//! persist-backed storage collection and arranges it into an index, ready to be
//! shipped as [`ComputeCommand::CreateDataflow`].
//!
//! [`ComputeCommand::CreateDataflow`]: mz_compute_client::protocol::command::ComputeCommand::CreateDataflow

use std::collections::BTreeMap;

use mz_compute_types::dataflows::{BuildDesc, DataflowDescription, IndexDesc, SourceImport};
use mz_compute_types::plan::Plan;
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_compute_types::sources::SourceInstanceDesc;
use mz_expr::MirScalarExpr;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, RelationDesc, ReprRelationType, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use timely::progress::Antichain;

/// Build a single-index dataflow over a persist shard.
///
/// The returned [`DataflowDescription`] imports the storage collection backed by
/// `shard` as `source_id`, then builds and exports an index `index_id` that
/// arranges that collection by `key_cols`.
///
/// # Construction strategy
///
/// We deliberately do *not* hand-roll the [`RenderPlan`]: the [`LirId`]s used to
/// stitch nodes together have no public constructor, and the [`LetFreePlan`]
/// invariants (notably a valid `topological_order`) are easy to get wrong. Instead
/// we mirror exactly what the real compute controller does:
///
///  1. Build a MIR-level [`DataflowDescription<OptimizedMirRelationExpr, ()>`] using
///     the same [`import_source`] / [`export_index`] helpers the optimizer uses.
///  2. Lower it to LIR via [`Plan::finalize_dataflow`], yielding
///     [`DataflowDescription<Plan, ()>`].
///  3. Augment it into [`DataflowDescription<RenderPlan, CollectionMetadata>`] by
///     converting each object's [`Plan`] via [`RenderPlan::try_from`] and attaching
///     the storage [`CollectionMetadata`] to the source import — the same step
///     performed in `compute-client`'s `Instance::create_dataflow`.
///
/// This guarantees the emitted plan is structurally identical to one produced by a
/// live `environmentd`, at the cost of running the (cheap, deterministic) lowering
/// in-process.
///
/// [`LirId`]: mz_compute_types::plan::LirId
/// [`LetFreePlan`]: mz_compute_types::plan::render_plan::LetFreePlan
/// [`import_source`]: DataflowDescription::import_source
/// [`export_index`]: DataflowDescription::export_index
/// [`DataflowDescription<OptimizedMirRelationExpr, ()>`]: DataflowDescription
/// [`DataflowDescription<Plan, ()>`]: DataflowDescription
/// [`DataflowDescription<RenderPlan, CollectionMetadata>`]: DataflowDescription
pub fn index_dataflow(
    source_id: GlobalId,
    index_id: GlobalId,
    shard: ShardId,
    location: PersistLocation,
    desc: RelationDesc,
    key_cols: Vec<usize>,
    as_of: Timestamp,
) -> DataflowDescription<RenderPlan, CollectionMetadata> {
    // The arrangement key as MIR scalar expressions over the source columns.
    let key: Vec<MirScalarExpr> = key_cols.into_iter().map(MirScalarExpr::column).collect();

    // Both relation-type flavors are derived from `desc`:
    //  * `SqlRelationType` (via `RelationDesc::typ`) flows into the source import.
    //  * `ReprRelationType` (via `ReprRelationType::from(&SqlRelationType)`) is what
    //    the index export and the MIR `global_get` expect.
    let sql_typ = desc.typ().clone();
    let repr_typ = ReprRelationType::from(desc.typ());

    // (1) MIR-level dataflow: import the source, export the index over it.
    let mut mir: DataflowDescription<mz_expr::OptimizedMirRelationExpr, ()> =
        DataflowDescription::new("headless-index".to_string());
    // `monotonic: false` matches the verified-structure requirement.
    mir.import_source(source_id, sql_typ, false);
    mir.export_index(
        index_id,
        IndexDesc {
            on_id: source_id,
            key,
        },
        repr_typ,
    );
    mir.as_of = Some(Antichain::from_elem(as_of));
    mir.until = Antichain::new();

    // (2) Lower MIR -> LIR. This is deterministic and does not require any external
    // state; failure here indicates a malformed input plan, which is a programmer
    // error in this driver.
    let features = OptimizerFeatures::default();
    let lowered: DataflowDescription<Plan, ()> =
        Plan::finalize_dataflow(mir, &features).expect("lowering index dataflow");

    // (3) Augment into the `<RenderPlan, CollectionMetadata>` form sent to clusterd.
    augment(lowered, source_id, shard, location, desc)
}

/// Convert a lowered `DataflowDescription<Plan, ()>` into the
/// `<RenderPlan, CollectionMetadata>` form expected by the compute protocol.
///
/// Mirrors `compute-client`'s `Instance::create_dataflow`: each object's [`Plan`]
/// is flattened into a [`RenderPlan`], and every source import is augmented with the
/// storage [`CollectionMetadata`] needed by the compute instance to read it.
fn augment(
    lowered: DataflowDescription<Plan, ()>,
    source_id: GlobalId,
    shard: ShardId,
    location: PersistLocation,
    desc: RelationDesc,
) -> DataflowDescription<RenderPlan, CollectionMetadata> {
    let metadata = CollectionMetadata {
        persist_location: location,
        data_shard: shard,
        relation_desc: desc,
        txns_shard: None,
    };

    // Attach the storage metadata to each source import. In a live controller the
    // `upper` is the storage collection's real write frontier; here the shard holds a
    // single batch written at timestamp 0, so its upper is `1`.
    let mut source_imports = BTreeMap::new();
    for (id, import) in lowered.source_imports {
        debug_assert_eq!(id, source_id, "only the single source import is expected");
        let desc = SourceInstanceDesc {
            storage_metadata: metadata.clone(),
            arguments: import.desc.arguments,
            typ: import.desc.typ,
        };
        source_imports.insert(
            id,
            SourceImport {
                desc,
                monotonic: import.monotonic,
                with_snapshot: import.with_snapshot,
                upper: Antichain::from_elem(Timestamp::from(0).step_forward()),
            },
        );
    }

    let objects_to_build = lowered
        .objects_to_build
        .into_iter()
        .map(|object| BuildDesc {
            id: object.id,
            plan: RenderPlan::try_from(object.plan).expect("valid lowered plan"),
        })
        .collect();

    DataflowDescription {
        source_imports,
        objects_to_build,
        // The remaining fields carry over unchanged from the lowered dataflow.
        index_imports: lowered.index_imports,
        index_exports: lowered.index_exports,
        sink_exports: BTreeMap::new(),
        as_of: lowered.as_of,
        until: lowered.until,
        initial_storage_as_of: lowered.initial_storage_as_of,
        refresh_schedule: lowered.refresh_schedule,
        debug_name: lowered.debug_name,
        time_dependence: lowered.time_dependence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use mz_compute_types::plan::GetPlan;
    use mz_compute_types::plan::render_plan::Expr;
    use mz_expr::Id;

    /// Assert the assembled dataflow matches the verified structure: a single
    /// source import, a single object building `Get(source) -> ArrangeBy(key)`,
    /// and a single index export over the source.
    #[mz_ore::test]
    fn index_dataflow_structure() {
        let desc = crate::data::sample_desc();
        let loc = PersistLocation {
            blob_uri: "mem://".parse().unwrap(),
            consensus_uri: "mem://".parse().unwrap(),
        };
        let df = index_dataflow(
            GlobalId::User(1000),
            GlobalId::User(1001),
            ShardId::new(),
            loc,
            desc,
            vec![0],
            Timestamp::from(0),
        );
        // Structural assertions mirroring the spec.
        assert_eq!(df.source_imports.len(), 1);
        assert_eq!(df.objects_to_build.len(), 1);
        assert_eq!(df.index_exports.len(), 1);
        assert!(df.sink_exports.is_empty());
        assert!(df.index_imports.is_empty());
        assert_eq!(df.as_of, Some(Antichain::from_elem(Timestamp::from(0))));
        assert_eq!(df.debug_name, "headless-index");

        let (sid, si) = df.source_imports.iter().next().unwrap();
        assert_eq!(*sid, GlobalId::User(1000));
        assert!(si.with_snapshot);
        assert!(!si.monotonic);
        assert_eq!(si.upper, Antichain::from_elem(Timestamp::from(1)));
        assert!(si.desc.arguments.operators.is_none());

        let (iid, (idesc, _typ)) = df.index_exports.iter().next().unwrap();
        assert_eq!(*iid, GlobalId::User(1001));
        assert_eq!(idesc.on_id, GlobalId::User(1000));
        assert_eq!(idesc.key, vec![MirScalarExpr::column(0)]);

        // The built object is `Get(source) -> ArrangeBy(key)`. Destructure the
        // `RenderPlan` and verify the root arranges, keyed by `Column(0)`, over a
        // `Get` of the source collection.
        let plan = &df.objects_to_build[0].plan;
        assert!(plan.binds.is_empty());
        let (nodes, root, _order) = plan.body.clone().destruct();
        let root_node = &nodes[&root];
        let Expr::ArrangeBy {
            input,
            forms,
            strategy,
            ..
        } = &root_node.expr
        else {
            panic!("expected root ArrangeBy, got {:?}", root_node.expr);
        };
        assert_eq!(forms.arranged.len(), 1);
        assert_eq!(forms.arranged[0].0, vec![MirScalarExpr::column(0)]);
        assert_eq!(
            *strategy,
            mz_compute_types::plan::ArrangementStrategy::Direct
        );
        let input_node = &nodes[input];
        let Expr::Get { id, plan, .. } = &input_node.expr else {
            panic!("expected ArrangeBy input Get, got {:?}", input_node.expr);
        };
        assert_eq!(*id, Id::Global(GlobalId::User(1000)));
        assert!(matches!(plan, GetPlan::PassArrangements));
    }
}
