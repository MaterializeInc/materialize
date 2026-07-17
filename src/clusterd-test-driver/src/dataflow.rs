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

use mz_compute_types::dataflows::{
    BuildDesc, DataflowDescription, IndexDesc, IndexImport, SourceImport,
};
use mz_compute_types::plan::LirRelationExpr;
use mz_compute_types::plan::render_plan::RenderPlan;
use mz_compute_types::sinks::{
    ComputeSinkConnection, ComputeSinkDesc, MaterializedViewSinkConnection, SubscribeSinkConnection,
};
use mz_compute_types::sources::SourceInstanceDesc;
use mz_expr::{
    AggregateExpr, AggregateFunc, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr,
};
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{GlobalId, RelationDesc, ReprRelationType, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::typecheck::empty_typechecking_context;
use mz_transform::{EmptyStatisticsOracle, IndexOracle, TransformCtx, optimize_dataflow};
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

/// A persist-backed target shard for a materialized-view sink to write to.
#[derive(Clone, Debug)]
pub struct PersistSink {
    /// The data shard the sink writes its output to.
    pub shard: ShardId,
    /// The persist location (blob + consensus) the shard lives in.
    pub location: PersistLocation,
}

/// An [`IndexOracle`] over a dataflow's own `index_imports`, exposing exactly the
/// arrangements this dataflow may read.
///
/// The real `environmentd` optimizer is handed a catalog-backed oracle that knows
/// every index on the cluster; the test driver has no catalog, but a dataflow's
/// `index_imports` already name exactly the arrangements available to it, so they
/// are the correct — and only — index information to expose. Without this, the
/// optimizer would not recognize an imported index and would re-plan a `Get` over
/// the indexed collection as a (non-existent) persist read.
#[derive(Debug)]
struct ImportedIndexOracle {
    /// `on_id` -> the `(index_id, key)` arrangements imported on it.
    by_on_id: BTreeMap<GlobalId, Vec<(GlobalId, Vec<MirScalarExpr>)>>,
}

impl ImportedIndexOracle {
    /// Build the oracle from a dataflow's `index_imports`, grouping by arranged id.
    fn new(index_imports: &BTreeMap<GlobalId, IndexImport>) -> Self {
        let mut by_on_id: BTreeMap<GlobalId, Vec<(GlobalId, Vec<MirScalarExpr>)>> = BTreeMap::new();
        for (index_id, import) in index_imports {
            by_on_id
                .entry(import.desc.on_id)
                .or_default()
                .push((*index_id, import.desc.key.clone()));
        }
        ImportedIndexOracle { by_on_id }
    }
}

impl IndexOracle for ImportedIndexOracle {
    fn indexes_on(
        &self,
        id: GlobalId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &[MirScalarExpr])> + '_> {
        match self.by_on_id.get(&id) {
            Some(indexes) => Box::new(indexes.iter().map(|(id, key)| (*id, key.as_slice()))),
            None => Box::new(std::iter::empty()),
        }
    }
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
/// By default the caller supplies MIR and the builder lowers it *faithfully*,
/// attaching the persist wiring without optimizing — so a hand-built minimal plan
/// lowers exactly as written. Optimization — fusion, predicate pushdown, and notably
/// join-implementation selection — is opt-in via [`Self::optimize`], paid for only
/// by callers that need it. A `Join` whose `implementation` is left `Unimplemented`
/// is rejected by the LIR lowering, so a plan containing one requires `optimize`,
/// which runs [`mz_transform::optimize_dataflow`] to fill the implementation first.
/// When optimizing, the builder hands the optimizer an index oracle built from its
/// own `index_imports` (`ImportedIndexOracle`), so imported arrangements are
/// recognized — the same index information `environmentd`'s catalog oracle would
/// supply for these imports.
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
///  2. Lower it to LIR via [`LirRelationExpr::finalize_dataflow`], yielding
///     [`DataflowDescription<LirRelationExpr, ()>`].
///  3. Augment it into [`DataflowDescription<RenderPlan, CollectionMetadata>`] by
///     converting each object's [`LirRelationExpr`] via [`RenderPlan::try_from`] and attaching
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
    /// Target storage metadata per materialized-view sink id, consumed by the
    /// augment step to fill the sink connection's `storage_metadata`.
    sinks: BTreeMap<GlobalId, CollectionMetadata>,
    /// Relation type per referenceable id (imports and built objects), so
    /// `export_index` can derive the `on_type` instead of taking it as an argument.
    types: BTreeMap<GlobalId, ReprRelationType>,
    /// Whether `finish` runs the MIR dataflow optimizer before lowering. Off by
    /// default (faithful lowering of the caller's MIR); see [`Self::optimize`].
    optimize: bool,
}

impl DataflowBuilder {
    /// Start an empty builder. `name` becomes the dataflow's debug name.
    pub fn new(name: impl Into<String>) -> Self {
        DataflowBuilder {
            mir: DataflowDescription::new(name.into()),
            sources: BTreeMap::new(),
            sinks: BTreeMap::new(),
            types: BTreeMap::new(),
            optimize: false,
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

    /// Import a previously-exported index, making the collection it arranges
    /// (`on_id`) available to this dataflow as an in-memory arrangement.
    ///
    /// Unlike [`Self::import_persist`], this imports no storage collection: the
    /// arrangement is served from the replica's existing, hydrated index, so the
    /// dataflow needs no [`CollectionMetadata`] and the augment step leaves the
    /// index import untouched. The MIR-to-LIR lowering registers the imported
    /// arrangement under `Get(on_id)` automatically, so a faithful (unoptimized)
    /// `Get(on_id)` picks it up. Returns an [`Input`] referencing `on_id` — the
    /// id a computation `Get`s, not the index id itself.
    pub fn import_index(
        &mut self,
        index_id: GlobalId,
        on_id: GlobalId,
        key_cols: Vec<usize>,
        on_type: ReprRelationType,
        monotonic: bool,
    ) -> Input {
        let key: Vec<MirScalarExpr> = key_cols.into_iter().map(MirScalarExpr::column).collect();
        self.mir.import_index(
            index_id,
            IndexDesc { on_id, key },
            on_type.clone(),
            monotonic,
        );
        self.types.insert(on_id, on_type.clone());
        Input {
            id: on_id,
            typ: on_type,
        }
    }

    /// A typed `Get` of an already-imported or built id, for callers that
    /// assemble MIR by id rather than threading [`Input`] handles — notably the
    /// JSON MIR translator. Errors if `id` was never imported or built, so a bad
    /// reference surfaces cleanly instead of constructing an ill-typed `Get`.
    pub fn get(&self, id: GlobalId) -> anyhow::Result<MirRelationExpr> {
        let typ = self
            .types
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("get of unknown id {id}; import or build it first"))?
            .clone();
        Ok(MirRelationExpr::global_get(id, typ))
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

    /// Export a materialized-view persist sink `sink_id` writing the collection
    /// `from_id` to a target persist shard (a materialized view).
    ///
    /// `value_desc` is the output relation schema; it must match `from_id`'s type
    /// (validated by the caller). The target shard is identified by `target`, whose
    /// `CollectionMetadata` the augment step splices into the sink connection — the
    /// compute persist sink opens it as `SourceData/()/Timestamp/StorageDiff`, the
    /// same codec a storage collection uses, so the shard reads back like any other.
    ///
    /// `up_to` is always the empty antichain: the persist sink does not implement
    /// `UP TO` (it panics during rendering otherwise), and the real optimizer
    /// likewise leaves a materialized view's `up_to` empty — it is a subscribe-only
    /// concept.
    pub fn export_materialized_view(
        &mut self,
        sink_id: GlobalId,
        from_id: GlobalId,
        value_desc: RelationDesc,
        target: PersistSink,
    ) -> &mut Self {
        let metadata = CollectionMetadata {
            persist_location: target.location,
            data_shard: target.shard,
            relation_desc: value_desc.clone(),
            txns_shard: None,
        };
        self.sinks.insert(sink_id, metadata);
        // The MIR-level description carries the unit storage metadata; the augment
        // step replaces it with the `CollectionMetadata` recorded above.
        let desc = ComputeSinkDesc {
            from: from_id,
            from_desc: value_desc.clone(),
            connection: ComputeSinkConnection::MaterializedView(MaterializedViewSinkConnection {
                value_desc,
                storage_metadata: (),
            }),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: vec![],
            refresh_schedule: None,
            from_key: None,
        };
        self.mir.export_sink(sink_id, desc);
        self
    }

    /// Export a subscribe sink `sink_id` streaming changes of the collection
    /// `from_id` back as `ComputeResponse::SubscribeResponse` batches.
    ///
    /// Unlike a materialized view, a subscribe writes no shard, so it needs no
    /// storage metadata. `value_desc` is the output schema (must match `from_id`'s
    /// type); `up_to` is the exclusive upper at which the subscribe completes. The
    /// empty `output` ordering leaves intra-timestamp order unconstrained — the
    /// driver consolidates and sorts the updates for a deterministic golden.
    pub fn export_subscribe(
        &mut self,
        sink_id: GlobalId,
        from_id: GlobalId,
        value_desc: RelationDesc,
        up_to: Antichain<Timestamp>,
    ) -> &mut Self {
        let desc = ComputeSinkDesc {
            from: from_id,
            from_desc: value_desc,
            connection: ComputeSinkConnection::Subscribe(SubscribeSinkConnection {
                output: vec![],
            }),
            with_snapshot: true,
            up_to,
            non_null_assertions: vec![],
            refresh_schedule: None,
            from_key: None,
        };
        self.mir.export_sink(sink_id, desc);
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

    /// Run the MIR dataflow optimizer in [`Self::finish`] before lowering.
    ///
    /// Off by default: the builder otherwise lowers the caller's MIR faithfully (the
    /// contract above). Enable it for plans that don't lower from raw MIR — notably a
    /// `Join`, whose `implementation` defaults to `Unimplemented` and is rejected by
    /// the LIR lowering until [`mz_transform::optimize_dataflow`]'s `JoinImplementation`
    /// fills it in — or to reproduce the plan `environmentd` would ship for a logical
    /// expression rather than the literal one written.
    pub fn optimize(&mut self) -> &mut Self {
        self.optimize = true;
        self
    }

    /// Lower the accumulated MIR and attach persist wiring, producing the
    /// `DataflowDescription` the compute protocol expects.
    ///
    /// Returns an error rather than panicking on a malformed plan (e.g. a key
    /// column out of range, or an unbalanced object graph), so a caller driving
    /// this from external input — notably the script reader — can surface a clean
    /// error instead of crashing the process.
    pub fn finish(mut self) -> anyhow::Result<DataflowDescription<RenderPlan, CollectionMetadata>> {
        let features = OptimizerFeatures::default();
        // Optionally run the MIR dataflow optimizer first (e.g. to fill a `Join`'s
        // implementation). The index oracle is built from this dataflow's own
        // `index_imports`, so the optimizer recognizes imported arrangements and
        // plans `Get`s over them as arrangement reads (not persist reads); the
        // statistics oracle is empty — no catalog stats — so join planning falls
        // back to a differential join, which lowers.
        if self.optimize {
            let indexes = ImportedIndexOracle::new(&self.mir.index_imports);
            let typecheck_ctx = empty_typechecking_context();
            let mut df_meta = DataflowMetainfo::default();
            let mut ctx = TransformCtx::global(
                &indexes,
                &EmptyStatisticsOracle,
                &features,
                &typecheck_ctx,
                &mut df_meta,
                None,
            );
            optimize_dataflow(&mut self.mir, &mut ctx, false)
                .map_err(|e| anyhow::anyhow!("optimizing dataflow failed: {e}"))?;
        }
        // Lower MIR -> LIR. Deterministic and self-contained.
        let lowered: DataflowDescription<LirRelationExpr, ()> =
            LirRelationExpr::finalize_dataflow(self.mir, &features, None)
                .map_err(|e| anyhow::anyhow!("lowering dataflow failed: {e}"))?;
        augment(lowered, &self.sources, &self.sinks)
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
) -> anyhow::Result<DataflowDescription<RenderPlan, CollectionMetadata>> {
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

/// Build a dataflow that counts the rows of an existing index and exports the
/// count as a new, peekable index.
///
/// Imports index `index_id` (arranging `on_id`, schema `on_type`, key `key_cols`),
/// computes `Reduce` with a single `count(*)` aggregate and an empty group key over
/// `Get(on_id)`, and exports `out_index_id` arranging the one-column count by `[0]`.
/// This is the compute-side realization of a row-count assertion: the count runs
/// through a real reduce operator rather than being tallied in the driver.
///
/// The result collection has one `bigint` column. Over an empty input the reduce
/// emits no rows (SQL's default-zero is added higher up), so a peek of the output
/// yields `[]`, which callers read as a count of `0`.
pub fn count_over_index(
    index_id: GlobalId,
    on_id: GlobalId,
    on_type: ReprRelationType,
    key_cols: Vec<usize>,
    reduce_id: GlobalId,
    out_index_id: GlobalId,
    as_of: Timestamp,
) -> anyhow::Result<DataflowDescription<RenderPlan, CollectionMetadata>> {
    let mut builder = DataflowBuilder::new("headless-count");
    // `monotonic: false` keeps the import faithful to a general (non-append-only)
    // index; the count reduce does not require monotonicity.
    let input = builder.import_index(index_id, on_id, key_cols, on_type, false);
    // `count(*)`: count over a non-null literal, so every row contributes.
    let count = AggregateExpr {
        func: AggregateFunc::Count,
        expr: MirScalarExpr::literal_true(),
        distinct: false,
    };
    let reduce = MirRelationExpr::Reduce {
        input: Box::new(input.get()),
        group_key: vec![],
        aggregates: vec![count],
        monotonic: false,
        expected_group_size: None,
    };
    builder.build(reduce_id, reduce);
    builder.as_of(as_of);
    // The reduce output is a single column; arrange it by that column so the
    // exported index is peekable.
    builder.export_index(out_index_id, reduce_id, vec![0]);
    builder.finish()
}

/// Convert a lowered `DataflowDescription<Plan, ()>` into the
/// `<RenderPlan, CollectionMetadata>` form expected by the compute protocol.
///
/// Mirrors `compute-client`'s `Instance::create_dataflow`: each object's [`LirRelationExpr`]
/// is flattened into a [`RenderPlan`], and every source import is augmented with the
/// storage [`CollectionMetadata`] needed by the compute instance to read it. The
/// per-id [`PersistSource`] supplies the metadata and the exclusive `upper` telling
/// the compute instance up to which timestamp the shard's data is available.
fn augment(
    lowered: DataflowDescription<LirRelationExpr, ()>,
    sources: &BTreeMap<GlobalId, PersistSource>,
    sinks: &BTreeMap<GlobalId, CollectionMetadata>,
) -> anyhow::Result<DataflowDescription<RenderPlan, CollectionMetadata>> {
    // Attach the storage metadata to each source import, looked up by id. In a live
    // controller the `upper` is the storage collection's real write frontier; the
    // caller provides it via `PersistSource::upper` to reflect the written data.
    let mut source_imports = BTreeMap::new();
    for (id, import) in lowered.source_imports {
        let source = sources
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("no persist metadata registered for source {id}"))?;
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
        .map(|object| {
            // `RenderPlan::try_from` fails (with `()`) on a structurally invalid
            // lowered plan; surface it as an error rather than panicking.
            let plan = RenderPlan::try_from(object.plan)
                .map_err(|()| anyhow::anyhow!("RenderPlan conversion failed for {}", object.id))?;
            Ok::<_, anyhow::Error>(BuildDesc {
                id: object.id,
                plan,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // Splice the storage metadata into each sink export, mirroring how
    // `compute-client`'s `Instance::create_dataflow` fills the materialized-view
    // sink's `storage_metadata` from the storage controller. A subscribe carries no
    // metadata; copy-to is not built by this driver.
    let mut sink_exports = BTreeMap::new();
    for (id, sink) in lowered.sink_exports {
        let connection = match sink.connection {
            ComputeSinkConnection::MaterializedView(conn) => {
                let metadata = sinks.get(&id).ok_or_else(|| {
                    anyhow::anyhow!("no target metadata registered for materialized-view sink {id}")
                })?;
                ComputeSinkConnection::MaterializedView(MaterializedViewSinkConnection {
                    value_desc: conn.value_desc,
                    storage_metadata: metadata.clone(),
                })
            }
            ComputeSinkConnection::Subscribe(conn) => ComputeSinkConnection::Subscribe(conn),
            ComputeSinkConnection::CopyToS3Oneshot(_) => {
                anyhow::bail!("copy-to-s3 sink {id} is not implemented")
            }
        };
        sink_exports.insert(
            id,
            ComputeSinkDesc {
                from: sink.from,
                from_desc: sink.from_desc,
                connection,
                with_snapshot: sink.with_snapshot,
                up_to: sink.up_to,
                non_null_assertions: sink.non_null_assertions,
                refresh_schedule: sink.refresh_schedule,
                from_key: sink.from_key,
            },
        );
    }

    Ok(DataflowDescription {
        source_imports,
        objects_to_build,
        // The remaining fields carry over unchanged from the lowered dataflow.
        index_imports: lowered.index_imports,
        index_exports: lowered.index_exports,
        sink_exports,
        as_of: lowered.as_of,
        until: lowered.until,
        initial_storage_as_of: lowered.initial_storage_as_of,
        refresh_schedule: lowered.refresh_schedule,
        debug_name: lowered.debug_name,
        time_dependence: lowered.time_dependence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use mz_compute_types::plan::GetPlan;
    use mz_compute_types::plan::render_plan::Expr;
    use mz_compute_types::plan::scalar::LirScalarExpr;
    use mz_expr::Id;

    /// Assert the assembled dataflow matches the verified structure: a single
    /// source import, a single object building `Get(source) -> ArrangeBy(key)`,
    /// and a single index export over the source.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
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
        )
        .unwrap();
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
        assert_eq!(forms.arranged[0].0, vec![LirScalarExpr::column(0)]);
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
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
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
        let df = builder.finish().unwrap();

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

    /// A `Join` does not lower from raw MIR — its `implementation` defaults to
    /// `Unimplemented` and the LIR lowering rejects it — but `optimize()` runs the
    /// MIR optimizer first, which fills the implementation, so the same dataflow
    /// then lowers. This is exactly what the `optimize` flag buys.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn join_lowers_only_with_optimize() {
        let loc = PersistLocation {
            blob_uri: "mem://".parse().unwrap(),
            consensus_uri: "mem://".parse().unwrap(),
        };
        // Build a two-source equi-join (`#0 = #2` across the concatenated columns)
        // and export an index over it. `optimize` selects whether the MIR optimizer
        // runs in `finish`.
        let assemble = |optimize: bool| {
            let mut builder = DataflowBuilder::new("headless-join-test");
            let left = builder.import_persist(
                GlobalId::User(1000),
                PersistSource {
                    shard: ShardId::new(),
                    location: loc.clone(),
                    desc: crate::data::sample_desc(),
                    upper: Timestamp::from(1),
                },
            );
            let right = builder.import_persist(
                GlobalId::User(1001),
                PersistSource {
                    shard: ShardId::new(),
                    location: loc.clone(),
                    desc: crate::data::sample_desc(),
                    upper: Timestamp::from(1),
                },
            );
            let join = MirRelationExpr::join_scalars(
                vec![left.get(), right.get()],
                vec![vec![MirScalarExpr::column(0), MirScalarExpr::column(2)]],
            );
            builder.build(GlobalId::User(2000), join);
            if optimize {
                builder.optimize();
            }
            builder.as_of(Timestamp::from(0));
            builder.export_index(GlobalId::User(2001), GlobalId::User(2000), vec![0]);
            builder.finish()
        };

        // Without the optimizer the `Unimplemented` join is rejected by the lowering.
        assert!(assemble(false).is_err());
        // With it, the optimizer fills the join implementation and the dataflow lowers.
        assert!(assemble(true).is_ok());
    }

    /// A single dataflow can export both an index and a materialized view over the
    /// same built object (binding). Both exports reference that object; the index
    /// arranges it and the MV sink writes it to a target shard.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn index_and_mv_same_binding() {
        let desc = crate::data::sample_desc();
        let loc = PersistLocation {
            blob_uri: "mem://".parse().unwrap(),
            consensus_uri: "mem://".parse().unwrap(),
        };
        let (source_id, view_id, index_id, sink_id) = (
            GlobalId::User(1000),
            GlobalId::User(1001),
            GlobalId::User(1002),
            GlobalId::User(1003),
        );

        let mut builder = DataflowBuilder::new("headless-index-and-mv");
        let src = builder.import_persist(
            source_id,
            PersistSource {
                shard: ShardId::new(),
                location: loc.clone(),
                desc: desc.clone(),
                upper: Timestamp::from(1),
            },
        );
        // A view over the source is the shared binding both exports reference.
        builder.build(
            view_id,
            src.get().filter(vec![MirScalarExpr::literal_true()]),
        );
        builder.as_of(Timestamp::from(0));
        builder.export_index(index_id, view_id, vec![0]);
        builder.export_materialized_view(
            sink_id,
            view_id,
            desc,
            PersistSink {
                shard: ShardId::new(),
                location: loc,
            },
        );
        let df = builder.finish().unwrap();

        // Both exports are present and reference the same view binding.
        assert_eq!(df.index_exports.len(), 1);
        assert_eq!(df.sink_exports.len(), 1);
        let (_iid, (idesc, _typ)) = df.index_exports.iter().next().unwrap();
        assert_eq!(idesc.on_id, view_id);
        let (sid, sink) = df.sink_exports.iter().next().unwrap();
        assert_eq!(*sid, sink_id);
        assert_eq!(sink.from, view_id);
        // The MV sink carries the target shard's storage metadata after augment.
        assert!(matches!(
            sink.connection,
            ComputeSinkConnection::MaterializedView(_)
        ));
    }

    /// With `optimize` on, the optimizer is handed an index oracle built from the
    /// dataflow's `index_imports`, so a `Get` over an imported (but not persisted)
    /// collection is recognized as an arrangement read. Were the oracle empty, the
    /// optimizer would re-plan that `Get` as a persist read of a collection that has
    /// no source import, and `finish` would fail — so success here, with one index
    /// import and no source imports, is the proof the index information reached the
    /// optimizer.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn optimize_uses_imported_index() {
        let desc = crate::data::sample_desc();
        let on_type = ReprRelationType::from(desc.typ());
        let (index_id, on_id, view_id, out_index_id) = (
            GlobalId::User(1001),
            GlobalId::User(1000),
            GlobalId::User(2000),
            GlobalId::User(2001),
        );

        let mut builder = DataflowBuilder::new("headless-optimize-imported-index");
        let input = builder.import_index(index_id, on_id, vec![0], on_type, false);
        // A view over the imported arrangement; with `optimize` the optimizer must
        // recognize the import to plan the `Get` as an arrangement read.
        builder.build(view_id, input.get().project(vec![0]));
        builder.optimize();
        builder.as_of(Timestamp::from(0));
        builder.export_index(out_index_id, view_id, vec![0]);
        let df = builder.finish().unwrap();

        // The collection is read from the imported arrangement, not from persist:
        // exactly one index import, no source imports.
        assert_eq!(df.index_imports.len(), 1);
        assert!(df.source_imports.is_empty());
        let (iid, import) = df.index_imports.iter().next().unwrap();
        assert_eq!(*iid, index_id);
        assert_eq!(import.desc.on_id, on_id);
    }

    /// A count-over-index dataflow imports the index (no storage source), builds
    /// the reduce and its arrange as two objects, and exports the count index.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `rust_psm_stack_pointer` on OS `linux`
    fn count_over_index_structure() {
        let desc = crate::data::sample_desc();
        let on_type = ReprRelationType::from(desc.typ());
        let df = count_over_index(
            GlobalId::User(1001), // existing index to import
            GlobalId::User(1000), // collection it arranges
            on_type,
            vec![0],              // its key
            GlobalId::User(2000), // reduce build object
            GlobalId::User(2001), // exported count index
            Timestamp::from(0),
        )
        .unwrap();

        // Imports the arrangement, not a storage collection.
        assert_eq!(df.index_imports.len(), 1);
        assert!(df.source_imports.is_empty());
        let (iid, import) = df.index_imports.iter().next().unwrap();
        assert_eq!(*iid, GlobalId::User(1001));
        assert_eq!(import.desc.on_id, GlobalId::User(1000));
        assert_eq!(import.desc.key, vec![MirScalarExpr::column(0)]);

        // Reduce + arrange lower to two build objects; the count index exports.
        assert_eq!(df.objects_to_build.len(), 2);
        assert_eq!(df.index_exports.len(), 1);
        let (eid, (edesc, _typ)) = df.index_exports.iter().next().unwrap();
        assert_eq!(*eid, GlobalId::User(2001));
        assert_eq!(edesc.on_id, GlobalId::User(2000));
        assert_eq!(edesc.key, vec![MirScalarExpr::column(0)]);
    }
}
