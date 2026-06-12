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
//! [`DataflowBuilder`] is the generic boundary between tests and the dataflow
//! assembly mechanism. A test describes its dataflow in terms of persist imports,
//! MIR objects to compute, and index exports; the builder owns the parts that are
//! hard and reusable — the MIR-to-LIR lowering, the [`RenderPlan`] conversion, the
//! [`CollectionMetadata`] attachment, and the `SqlRelationType`-versus-
//! `ReprRelationType` bookkeeping — and produces a
//! `DataflowDescription<RenderPlan, CollectionMetadata>` ready to ship as
//! [`ComputeCommand::CreateDataflow`].
//!
//! [`index_dataflow`] is thin sugar over the builder for the common single-index
//! shape.
//!
//! [`ComputeCommand::CreateDataflow`]: mz_compute_client::protocol::command::ComputeCommand::CreateDataflow

use std::collections::BTreeMap;

use mz_compute_types::dataflows::{BuildDesc, DataflowDescription, IndexDesc, SourceImport};
use mz_compute_types::plan::Plan;
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_compute_types::sources::SourceInstanceDesc;
use mz_expr::{MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr};
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, RelationDesc, ReprRelationType, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use timely::progress::Antichain;

/// A persist-backed storage collection to import into a dataflow.
///
/// `upper` is the exclusive upper bound of the shard's written data (the next
/// timestamp after the last written one): for data written at a single timestamp
/// `t`, pass `t + 1`; for data spread across `0..n_ts`, pass `n_ts`. The compute
/// instance uses it to know when the source's data is fully available.
#[derive(Clone, Debug)]
pub struct PersistSource {
    /// The data shard backing the collection.
    pub shard: ShardId,
    /// The persist location (blob + consensus) the shard lives in.
    pub location: PersistLocation,
    /// The relation schema of the collection.
    pub desc: RelationDesc,
    /// The exclusive upper bound of the shard's written data.
    pub upper: Timestamp,
}

/// A handle to an imported collection or built object, used to reference it when
/// constructing MIR for further objects.
#[derive(Clone, Debug)]
pub struct Input {
    id: GlobalId,
    typ: ReprRelationType,
}

impl Input {
    /// The id this input is bound to in the dataflow.
    pub fn id(&self) -> GlobalId {
        self.id
    }

    /// A MIR `Get` of this input, carrying its relation type, for use as a leaf
    /// when building a computation over it.
    pub fn get(&self) -> MirRelationExpr {
        MirRelationExpr::global_get(self.id, self.typ.clone())
    }
}

/// Builds a compute dataflow from generic parts, hiding the lowering and persist
/// wiring mechanism.
///
/// # Contract
///
/// The caller supplies MIR; the builder lowers it *faithfully* and attaches the
/// persist wiring. Optimization — fusion, predicate pushdown, and notably
/// join-implementation selection — is the caller's responsibility, not the
/// builder's. This keeps the dependency surface minimal (no `mz-transform`) and
/// matches the original behavior of lowering hand-built minimal MIR without
/// optimizing. A `Join` whose `implementation` is left `Unimplemented` is rejected
/// by the LIR lowering; supporting joins later means adding an opt-in `optimize`
/// step, paid for only by tests that need it.
///
/// # Construction strategy
///
/// The builder deliberately does *not* hand-roll the [`RenderPlan`]: the [`LirId`]s
/// used to stitch nodes together have no public constructor, and the [`LetFreePlan`]
/// invariants (notably a valid `topological_order`) are easy to get wrong. Instead
/// it mirrors exactly what the real compute controller does:
///
///  1. Accumulate a MIR-level [`DataflowDescription<OptimizedMirRelationExpr, ()>`]
///     using the same [`import_source`] / [`insert_plan`] / [`export_index`] helpers
///     the optimizer uses.
///  2. Lower it to LIR via [`Plan::finalize_dataflow`], yielding
///     [`DataflowDescription<Plan, ()>`].
///  3. Augment it into [`DataflowDescription<RenderPlan, CollectionMetadata>`] by
///     converting each object's [`Plan`] via [`RenderPlan::try_from`] and attaching
///     the storage [`CollectionMetadata`] to each source import — the same step
///     performed in `compute-client`'s `Instance::create_dataflow`.
///
/// This guarantees the emitted plan is structurally identical to one produced by a
/// live `environmentd`, at the cost of running the (cheap, deterministic) lowering
/// in-process.
///
/// [`LirId`]: mz_compute_types::plan::LirId
/// [`LetFreePlan`]: mz_compute_types::plan::render_plan::LetFreePlan
/// [`import_source`]: DataflowDescription::import_source
/// [`insert_plan`]: DataflowDescription::insert_plan
/// [`export_index`]: DataflowDescription::export_index
/// [`DataflowDescription<OptimizedMirRelationExpr, ()>`]: DataflowDescription
/// [`DataflowDescription<Plan, ()>`]: DataflowDescription
/// [`DataflowDescription<RenderPlan, CollectionMetadata>`]: DataflowDescription
pub struct DataflowBuilder {
    /// The MIR-level description being accumulated.
    mir: DataflowDescription<OptimizedMirRelationExpr, ()>,
    /// Persist metadata per imported source id, consumed by the augment step.
    sources: BTreeMap<GlobalId, PersistSource>,
    /// Relation type per referenceable id (imports and built objects), so
    /// `export_index` can derive the `on_type` instead of taking it as an argument.
    types: BTreeMap<GlobalId, ReprRelationType>,
}

impl DataflowBuilder {
    /// Start an empty builder. `name` becomes the dataflow's debug name.
    pub fn new(name: impl Into<String>) -> Self {
        DataflowBuilder {
            mir: DataflowDescription::new(name.into()),
            sources: BTreeMap::new(),
            types: BTreeMap::new(),
        }
    }

    /// Import a persist-backed storage collection as `id`.
    ///
    /// Registers the source on the MIR description and records the persist metadata
    /// for the augment step. Returns an [`Input`] handle whose [`Input::get`] yields
    /// a correctly typed `Get` node, so callers never construct a [`ReprRelationType`]
    /// by hand.
    pub fn import_persist(&mut self, id: GlobalId, source: PersistSource) -> Input {
        // `import_source` takes the `SqlRelationType`; the `Get`/export path wants the
        // `ReprRelationType`. Both are derived from the single `desc`.
        let sql_typ = source.desc.typ().clone();
        let repr_typ = ReprRelationType::from(source.desc.typ());
        // `monotonic: false` matches the verified-structure requirement.
        self.mir.import_source(id, sql_typ, false);
        self.sources.insert(id, source);
        self.types.insert(id, repr_typ.clone());
        Input { id, typ: repr_typ }
    }

    /// Insert a MIR object to compute, bound to `id`.
    ///
    /// `expr` is wrapped via [`OptimizedMirRelationExpr::declare_optimized`]; the
    /// caller is responsible for any optimization (see the type-level contract). The
    /// object's relation type is recorded so a later [`Self::export_index`] over `id`
    /// can derive its `on_type`.
    pub fn build(&mut self, id: GlobalId, expr: MirRelationExpr) -> &mut Self {
        self.types.insert(id, expr.typ());
        self.mir
            .insert_plan(id, OptimizedMirRelationExpr::declare_optimized(expr));
        self
    }

    /// Export an index `index_id` arranging `on_id` by `key_cols`.
    ///
    /// `on_id` may be an imported source or a built object; either way the lowering
    /// synthesizes the `ArrangeBy`. The `on_type` is derived from the referenced id,
    /// which must have been imported or built first.
    pub fn export_index(
        &mut self,
        index_id: GlobalId,
        on_id: GlobalId,
        key_cols: Vec<usize>,
    ) -> &mut Self {
        let on_type = self
            .types
            .get(&on_id)
            .unwrap_or_else(|| panic!("export_index on unknown id {on_id}"))
            .clone();
        let key: Vec<MirScalarExpr> = key_cols.into_iter().map(MirScalarExpr::column).collect();
        self.mir
            .export_index(index_id, IndexDesc { on_id, key }, on_type);
        self
    }

    /// Set the dataflow's `as_of` (the read frontier hydration starts from).
    pub fn as_of(&mut self, t: Timestamp) -> &mut Self {
        self.mir.as_of = Some(Antichain::from_elem(t));
        self
    }

    /// Set the dataflow's `until` (the exclusive upper bound past which output is
    /// dropped). Defaults to the empty antichain (no bound).
    pub fn until(&mut self, t: Timestamp) -> &mut Self {
        self.mir.until = Antichain::from_elem(t);
        self
    }

    /// Lower the accumulated MIR and attach persist wiring, producing the
    /// `DataflowDescription` the compute protocol expects.
    pub fn finish(self) -> DataflowDescription<RenderPlan, CollectionMetadata> {
        // Lower MIR -> LIR. Deterministic and self-contained; a failure here means a
        // malformed input plan, which is a programmer error in this driver.
        let features = OptimizerFeatures::default();
        let lowered: DataflowDescription<Plan, ()> =
            Plan::finalize_dataflow(self.mir, &features).expect("lowering dataflow");
        augment(lowered, &self.sources)
    }
}

/// Build a single-index dataflow over a persist shard.
///
/// Thin sugar over [`DataflowBuilder`] for the common shape: import the collection
/// backed by `shard` as `source_id`, set `as_of`, and export an index `index_id`
/// arranging the collection by `key_cols`.
///
/// `shard_upper` is the exclusive upper bound of the shard's written data; see
/// [`PersistSource::upper`].
pub fn index_dataflow(
    source_id: GlobalId,
    index_id: GlobalId,
    shard: ShardId,
    location: PersistLocation,
    desc: RelationDesc,
    key_cols: Vec<usize>,
    as_of: Timestamp,
    shard_upper: Timestamp,
) -> DataflowDescription<RenderPlan, CollectionMetadata> {
    let mut builder = DataflowBuilder::new("headless-index");
    builder.import_persist(
        source_id,
        PersistSource {
            shard,
            location,
            desc,
            upper: shard_upper,
        },
    );
    builder.as_of(as_of);
    builder.export_index(index_id, source_id, key_cols);
    builder.finish()
}

/// Convert a lowered `DataflowDescription<Plan, ()>` into the
/// `<RenderPlan, CollectionMetadata>` form expected by the compute protocol.
///
/// Mirrors `compute-client`'s `Instance::create_dataflow`: each object's [`Plan`]
/// is flattened into a [`RenderPlan`], and every source import is augmented with the
/// storage [`CollectionMetadata`] needed by the compute instance to read it. The
/// per-id [`PersistSource`] supplies the metadata and the exclusive `upper` telling
/// the compute instance up to which timestamp the shard's data is available.
fn augment(
    lowered: DataflowDescription<Plan, ()>,
    sources: &BTreeMap<GlobalId, PersistSource>,
) -> DataflowDescription<RenderPlan, CollectionMetadata> {
    // Attach the storage metadata to each source import, looked up by id. In a live
    // controller the `upper` is the storage collection's real write frontier; the
    // caller provides it via `PersistSource::upper` to reflect the written data.
    let mut source_imports = BTreeMap::new();
    for (id, import) in lowered.source_imports {
        let source = sources
            .get(&id)
            .unwrap_or_else(|| panic!("no persist metadata registered for source {id}"));
        let metadata = CollectionMetadata {
            persist_location: source.location.clone(),
            data_shard: source.shard,
            relation_desc: source.desc.clone(),
            txns_shard: None,
        };
        let desc = SourceInstanceDesc {
            storage_metadata: metadata,
            arguments: import.desc.arguments,
            typ: import.desc.typ,
        };
        source_imports.insert(
            id,
            SourceImport {
                desc,
                monotonic: import.monotonic,
                with_snapshot: import.with_snapshot,
                upper: Antichain::from_elem(source.upper),
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
            Timestamp::from(1),
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

    /// Exercise the general `build` path: import a source, compute a `Project` over
    /// it, and export an index on the computed object. The computation and the
    /// arrange must lower to two distinct objects, and the index export must
    /// reference the built object rather than the source.
    #[mz_ore::test]
    fn build_computed_object_lowers() {
        let desc = crate::data::sample_desc();
        let loc = PersistLocation {
            blob_uri: "mem://".parse().unwrap(),
            consensus_uri: "mem://".parse().unwrap(),
        };
        let (source_id, comp_id, index_id) = (
            GlobalId::User(1000),
            GlobalId::User(1001),
            GlobalId::User(1002),
        );

        let mut builder = DataflowBuilder::new("headless-build");
        let src = builder.import_persist(
            source_id,
            PersistSource {
                shard: ShardId::new(),
                location: loc,
                desc,
                upper: Timestamp::from(1),
            },
        );
        // Project away the payload column, keeping only `id` (column 0).
        builder.build(comp_id, src.get().project(vec![0]));
        builder.as_of(Timestamp::from(0));
        builder.export_index(index_id, comp_id, vec![0]);
        let df = builder.finish();

        // One source import; the index export references the computed object.
        assert_eq!(df.source_imports.len(), 1);
        assert!(df.source_imports.contains_key(&source_id));
        let (iid, (idesc, _typ)) = df.index_exports.iter().next().unwrap();
        assert_eq!(*iid, index_id);
        assert_eq!(idesc.on_id, comp_id);

        // The computation and the arrange lower to two distinct build objects.
        assert_eq!(df.objects_to_build.len(), 2);
        let ids: Vec<_> = df.objects_to_build.iter().map(|o| o.id).collect();
        assert!(ids.contains(&comp_id));
        assert!(ids.contains(&index_id));
    }
}
