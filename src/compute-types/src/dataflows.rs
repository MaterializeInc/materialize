// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for describing dataflows.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use mz_expr::{CollectionPlan, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr};
use mz_ore::soft_assert_or_log;
use mz_proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::{GlobalId, RelationType};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::time_dependence::TimeDependence;
use proptest::prelude::{Arbitrary, any};
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;

use crate::dataflows::proto_dataflow_description::{
    ProtoIndexExport, ProtoIndexImport, ProtoSinkExport, ProtoSourceImport,
};
use crate::plan::Plan;
use crate::plan::render_plan::RenderPlan;
use crate::sinks::{ComputeSinkConnection, ComputeSinkDesc};
use crate::sources::{SourceInstanceArguments, SourceInstanceDesc};

include!(concat!(env!("OUT_DIR"), "/mz_compute_types.dataflows.rs"));

/// A description of a dataflow to construct and results to surface.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DataflowDescription<P, S: 'static = (), T = mz_repr::Timestamp> {
    /// Sources instantiations made available to the dataflow pair with monotonicity information.
    pub source_imports: BTreeMap<GlobalId, (SourceInstanceDesc<S>, bool, Antichain<T>)>,
    /// Indexes made available to the dataflow.
    /// (id of index, import)
    pub index_imports: BTreeMap<GlobalId, IndexImport>,
    /// Views and indexes to be built and stored in the local context.
    /// Objects must be built in the specific order, as there may be
    /// dependencies of later objects on prior identifiers.
    pub objects_to_build: Vec<BuildDesc<P>>,
    /// Indexes to be made available to be shared with other dataflows
    /// (id of new index, description of index, relationtype of base source/view/table)
    pub index_exports: BTreeMap<GlobalId, (IndexDesc, RelationType)>,
    /// sinks to be created
    /// (id of new sink, description of sink)
    pub sink_exports: BTreeMap<GlobalId, ComputeSinkDesc<S, T>>,
    /// An optional frontier to which inputs should be advanced.
    ///
    /// If this is set, it should override the default setting determined by
    /// the upper bound of `since` frontiers contributing to the dataflow.
    /// It is an error for this to be set to a frontier not beyond that default.
    pub as_of: Option<Antichain<T>>,
    /// Frontier beyond which the dataflow should not execute.
    /// Specifically, updates at times greater or equal to this frontier are suppressed.
    /// This is often set to `as_of + 1` to enable "batch" computations.
    /// Note that frontier advancements might still happen to times that are after the `until`,
    /// only data is suppressed. (This is consistent with how frontier advancements can also
    /// happen before the `as_of`.)
    pub until: Antichain<T>,
    /// The initial as_of when the collection is first created. Filled only for materialized views.
    /// Note that this doesn't change upon restarts.
    pub initial_storage_as_of: Option<Antichain<T>>,
    /// The schedule of REFRESH materialized views.
    pub refresh_schedule: Option<RefreshSchedule>,
    /// Human-readable name
    pub debug_name: String,
    /// Description of how the dataflow's progress relates to wall-clock time. None for unknown.
    pub time_dependence: Option<TimeDependence>,
}

impl<P, S> DataflowDescription<P, S, mz_repr::Timestamp> {
    /// Tests if the dataflow refers to a single timestamp, namely
    /// that `as_of` has a single coordinate and that the `until`
    /// value corresponds to the `as_of` value plus one, or `as_of`
    /// is the maximum timestamp and is thus single.
    pub fn is_single_time(&self) -> bool {
        // TODO: this would be much easier to check if `until` was a strict lower bound,
        // and we would be testing that `until == as_of`.

        let until = &self.until;

        // IF `as_of` is not set at all this can't be a single time dataflow.
        let Some(as_of) = self.as_of.as_ref() else {
            return false;
        };
        // Ensure that as_of <= until.
        soft_assert_or_log!(
            timely::PartialOrder::less_equal(as_of, until),
            "expected empty `as_of ≤ until`, got `{as_of:?} ≰ {until:?}`",
        );
        // IF `as_of` is not a single timestamp this can't be a single time dataflow.
        let Some(as_of) = as_of.as_option() else {
            return false;
        };
        // Ensure that `as_of = MAX` implies `until.is_empty()`.
        soft_assert_or_log!(
            as_of != &mz_repr::Timestamp::MAX || until.is_empty(),
            "expected `until = {{}}` due to `as_of = MAX`, got `until = {until:?}`",
        );
        // Note that the `(as_of = MAX, until = {})` case also returns `true`
        // here (as expected) since we are going to compare two `None` values.
        as_of.try_step_forward().as_ref() == until.as_option()
    }
}

impl<T> DataflowDescription<Plan<T>, (), mz_repr::Timestamp> {
    /// Check invariants expected to be true about `DataflowDescription`s.
    pub fn check_invariants(&self) -> Result<(), String> {
        let mut plans: Vec<_> = self.objects_to_build.iter().map(|o| &o.plan).collect();
        let mut lir_ids = BTreeSet::new();

        while let Some(plan) = plans.pop() {
            let lir_id = plan.lir_id;
            if !lir_ids.insert(lir_id) {
                return Err(format!(
                    "duplicate `LirId` in `DataflowDescription`: {lir_id}"
                ));
            }
            plans.extend(plan.node.children());
        }

        Ok(())
    }
}

impl<T> DataflowDescription<OptimizedMirRelationExpr, (), T> {
    /// Creates a new dataflow description with a human-readable name.
    pub fn new(name: String) -> Self {
        Self {
            source_imports: Default::default(),
            index_imports: Default::default(),
            objects_to_build: Vec::new(),
            index_exports: Default::default(),
            sink_exports: Default::default(),
            as_of: Default::default(),
            until: Antichain::new(),
            initial_storage_as_of: None,
            refresh_schedule: None,
            debug_name: name,
            time_dependence: None,
        }
    }

    /// Imports a previously exported index.
    ///
    /// This method makes available an index previously exported as `id`, identified
    /// to the query by `description` (which names the view the index arranges, and
    /// the keys by which it is arranged).
    pub fn import_index(
        &mut self,
        id: GlobalId,
        desc: IndexDesc,
        typ: RelationType,
        monotonic: bool,
    ) {
        self.index_imports.insert(
            id,
            IndexImport {
                desc,
                typ,
                monotonic,
            },
        );
    }

    /// Imports a source and makes it available as `id`.
    pub fn import_source(&mut self, id: GlobalId, typ: RelationType, monotonic: bool) {
        // Import the source with no linear operators applied to it.
        // They may be populated by whole-dataflow optimization.
        self.source_imports.insert(
            id,
            (
                SourceInstanceDesc {
                    storage_metadata: (),
                    arguments: SourceInstanceArguments { operators: None },
                    typ,
                },
                monotonic,
                Antichain::new(),
            ),
        );
    }

    /// Binds to `id` the relation expression `plan`.
    pub fn insert_plan(&mut self, id: GlobalId, plan: OptimizedMirRelationExpr) {
        self.objects_to_build.push(BuildDesc { id, plan });
    }

    /// Exports as `id` an index described by `description`.
    ///
    /// Future uses of `import_index` in other dataflow descriptions may use `id`,
    /// as long as this dataflow has not been terminated in the meantime.
    pub fn export_index(&mut self, id: GlobalId, description: IndexDesc, on_type: RelationType) {
        // We first create a "view" named `id` that ensures that the
        // data are correctly arranged and available for export.
        self.insert_plan(
            id,
            OptimizedMirRelationExpr::declare_optimized(MirRelationExpr::ArrangeBy {
                input: Box::new(MirRelationExpr::global_get(
                    description.on_id,
                    on_type.clone(),
                )),
                keys: vec![description.key.clone()],
            }),
        );
        self.index_exports.insert(id, (description, on_type));
    }

    /// Exports as `id` a sink described by `description`.
    pub fn export_sink(&mut self, id: GlobalId, description: ComputeSinkDesc<(), T>) {
        self.sink_exports.insert(id, description);
    }

    /// Returns true iff `id` is already imported.
    pub fn is_imported(&self, id: &GlobalId) -> bool {
        self.objects_to_build.iter().any(|bd| &bd.id == id)
            || self.index_imports.keys().any(|i| i == id)
            || self.source_imports.keys().any(|i| i == id)
    }

    /// The number of columns associated with an identifier in the dataflow.
    pub fn arity_of(&self, id: &GlobalId) -> usize {
        for (source_id, (source, _monotonic, _upper)) in self.source_imports.iter() {
            if source_id == id {
                return source.typ.arity();
            }
        }
        for IndexImport { desc, typ, .. } in self.index_imports.values() {
            if &desc.on_id == id {
                return typ.arity();
            }
        }
        for desc in self.objects_to_build.iter() {
            if &desc.id == id {
                return desc.plan.arity();
            }
        }
        panic!("GlobalId {} not found in DataflowDesc", id);
    }

    /// Calls r and s on any sub-members of those types in self. Halts at the first error return.
    pub fn visit_children<R, S, E>(&mut self, r: R, s: S) -> Result<(), E>
    where
        R: Fn(&mut OptimizedMirRelationExpr) -> Result<(), E>,
        S: Fn(&mut MirScalarExpr) -> Result<(), E>,
    {
        for BuildDesc { plan, .. } in &mut self.objects_to_build {
            r(plan)?;
        }
        for (source_instance_desc, _, _upper) in self.source_imports.values_mut() {
            let Some(mfp) = source_instance_desc.arguments.operators.as_mut() else {
                continue;
            };
            for expr in mfp.expressions.iter_mut() {
                s(expr)?;
            }
            for (_, expr) in mfp.predicates.iter_mut() {
                s(expr)?;
            }
        }
        Ok(())
    }
}

impl<P, S, T> DataflowDescription<P, S, T> {
    /// Sets the `as_of` frontier to the supplied argument.
    ///
    /// This method allows the dataflow to indicate a frontier up through
    /// which all times should be advanced. This can be done for at least
    /// two reasons: 1. correctness and 2. performance.
    ///
    /// Correctness may require an `as_of` to ensure that historical detail
    /// is consolidated at representative times that do not present specific
    /// detail that is not specifically correct. For example, updates may be
    /// compacted to times that are no longer the source times, but instead
    /// some byproduct of when compaction was executed; we should not present
    /// those specific times as meaningfully different from other equivalent
    /// times.
    ///
    /// Performance may benefit from an aggressive `as_of` as it reduces the
    /// number of distinct moments at which collections vary. Differential
    /// dataflow will refresh its outputs at each time its inputs change and
    /// to moderate that we can minimize the volume of distinct input times
    /// as much as possible.
    ///
    /// Generally, one should consider setting `as_of` at least to the `since`
    /// frontiers of contributing data sources and as aggressively as the
    /// computation permits.
    pub fn set_as_of(&mut self, as_of: Antichain<T>) {
        self.as_of = Some(as_of);
    }

    /// Records the initial `as_of` of the storage collection associated with a materialized view.
    pub fn set_initial_as_of(&mut self, initial_as_of: Antichain<T>) {
        self.initial_storage_as_of = Some(initial_as_of);
    }

    /// Identifiers of imported objects (indexes and sources).
    pub fn import_ids(&self) -> impl Iterator<Item = GlobalId> + Clone + '_ {
        self.imported_index_ids().chain(self.imported_source_ids())
    }

    /// Identifiers of imported indexes.
    pub fn imported_index_ids(&self) -> impl Iterator<Item = GlobalId> + Clone + '_ {
        self.index_imports.keys().copied()
    }

    /// Identifiers of imported sources.
    pub fn imported_source_ids(&self) -> impl Iterator<Item = GlobalId> + Clone + '_ {
        self.source_imports.keys().copied()
    }

    /// Identifiers of exported objects (indexes and sinks).
    pub fn export_ids(&self) -> impl Iterator<Item = GlobalId> + Clone + '_ {
        self.exported_index_ids().chain(self.exported_sink_ids())
    }

    /// Identifiers of exported indexes.
    pub fn exported_index_ids(&self) -> impl Iterator<Item = GlobalId> + Clone + '_ {
        self.index_exports.keys().copied()
    }

    /// Identifiers of exported sinks.
    pub fn exported_sink_ids(&self) -> impl Iterator<Item = GlobalId> + Clone + '_ {
        self.sink_exports.keys().copied()
    }

    /// Identifiers of exported persist sinks.
    pub fn persist_sink_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.sink_exports
            .iter()
            .filter_map(|(id, desc)| match desc.connection {
                ComputeSinkConnection::MaterializedView(_) => Some(*id),
                ComputeSinkConnection::ContinualTask(_) => Some(*id),
                _ => None,
            })
    }

    /// Identifiers of exported subscribe sinks.
    pub fn subscribe_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.sink_exports
            .iter()
            .filter_map(|(id, desc)| match desc.connection {
                ComputeSinkConnection::Subscribe(_) => Some(*id),
                _ => None,
            })
    }

    /// Identifiers of exported continual tasks.
    pub fn continual_task_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.sink_exports
            .iter()
            .filter_map(|(id, desc)| match desc.connection {
                ComputeSinkConnection::ContinualTask(_) => Some(*id),
                _ => None,
            })
    }

    /// Identifiers of exported copy to sinks.
    pub fn copy_to_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.sink_exports
            .iter()
            .filter_map(|(id, desc)| match desc.connection {
                ComputeSinkConnection::CopyToS3Oneshot(_) => Some(*id),
                _ => None,
            })
    }

    /// Produce a `Display`able value containing the import IDs of this dataflow.
    pub fn display_import_ids(&self) -> impl fmt::Display + '_ {
        use mz_ore::str::{bracketed, separated};
        bracketed("[", "]", separated(", ", self.import_ids()))
    }

    /// Produce a `Display`able value containing the export IDs of this dataflow.
    pub fn display_export_ids(&self) -> impl fmt::Display + '_ {
        use mz_ore::str::{bracketed, separated};
        bracketed("[", "]", separated(", ", self.export_ids()))
    }

    /// Whether this dataflow installs transient collections.
    pub fn is_transient(&self) -> bool {
        self.export_ids().all(|id| id.is_transient())
    }

    /// Returns the description of the object to build with the specified
    /// identifier.
    ///
    /// # Panics
    ///
    /// Panics if `id` is not present in `objects_to_build` exactly once.
    pub fn build_desc(&self, id: GlobalId) -> &BuildDesc<P> {
        let mut builds = self.objects_to_build.iter().filter(|build| build.id == id);
        let build = builds
            .next()
            .unwrap_or_else(|| panic!("object to build id {id} unexpectedly missing"));
        assert!(builds.next().is_none());
        build
    }
}

impl<P, S, T> DataflowDescription<P, S, T>
where
    P: CollectionPlan,
{
    /// Computes the set of identifiers upon which the specified collection
    /// identifier depends.
    ///
    /// `collection_id` must specify a valid object in `objects_to_build`.
    ///
    /// This method includes identifiers for e.g. intermediate views, and should be filtered
    /// if one only wants sources and indexes.
    ///
    /// This method is safe for mutually recursive view definitions.
    pub fn depends_on(&self, collection_id: GlobalId) -> BTreeSet<GlobalId> {
        let mut out = BTreeSet::new();
        self.depends_on_into(collection_id, &mut out);
        out
    }

    /// Like `depends_on`, but appends to an existing `BTreeSet`.
    pub fn depends_on_into(&self, collection_id: GlobalId, out: &mut BTreeSet<GlobalId>) {
        out.insert(collection_id);
        if self.source_imports.contains_key(&collection_id) {
            // The collection is provided by an imported source. Report the
            // dependency on the source.
            out.insert(collection_id);
            return;
        }

        // NOTE(benesch): we're not smart enough here to know *which* index
        // for the collection will be used, if one exists, so we have to report
        // the dependency on all of them.
        let mut found_index = false;
        for (index_id, IndexImport { desc, .. }) in &self.index_imports {
            if desc.on_id == collection_id {
                // The collection is provided by an imported index. Report the
                // dependency on the index.
                out.insert(*index_id);
                found_index = true;
            }
        }
        if found_index {
            return;
        }

        // The collection is not provided by a source or imported index.
        // It must be a collection whose plan we have handy. Recurse.
        let build = self.build_desc(collection_id);
        for id in build.plan.depends_on() {
            if !out.contains(&id) {
                self.depends_on_into(id, out)
            }
        }
    }

    /// Computes the set of imports upon which the specified collection depends.
    ///
    /// This method behaves like `depends_on` but filters out internal dependencies that are not
    /// included in the dataflow imports.
    pub fn depends_on_imports(&self, collection_id: GlobalId) -> BTreeSet<GlobalId> {
        let is_import = |id: &GlobalId| {
            self.source_imports.contains_key(id) || self.index_imports.contains_key(id)
        };

        let deps = self.depends_on(collection_id);
        deps.into_iter().filter(is_import).collect()
    }
}

impl<S, T> DataflowDescription<RenderPlan, S, T>
where
    S: Clone + PartialEq,
    T: Clone + timely::PartialOrder,
{
    /// Determine if a dataflow description is compatible with this dataflow description.
    ///
    /// Compatible dataflows have structurally equal exports, imports, and objects to build. The
    /// `as_of` of the receiver has to be less equal the `other` `as_of`.
    ///
    /// Note that this method performs normalization as part of the structural equality checking,
    /// which involves cloning both `self` and `other`. It is therefore relatively expensive and
    /// should only be used on cold code paths.
    ///
    // TODO: The semantics of this function are only useful for command reconciliation at the moment.
    pub fn compatible_with(&self, other: &Self) -> bool {
        let old = self.as_comparable();
        let new = other.as_comparable();

        let equality = old.index_exports == new.index_exports
            && old.sink_exports == new.sink_exports
            && old.objects_to_build == new.objects_to_build
            && old.index_imports == new.index_imports
            && old.source_imports == new.source_imports
            && old.time_dependence == new.time_dependence;

        let partial = if let (Some(old_as_of), Some(new_as_of)) = (&old.as_of, &new.as_of) {
            timely::PartialOrder::less_equal(old_as_of, new_as_of)
        } else {
            false
        };

        equality && partial
    }

    /// Returns a `DataflowDescription` that has the same structure as `self` and can be
    /// structurally compared to other `DataflowDescription`s.
    ///
    /// The function normalizes several properties. It replaces transient `GlobalId`s
    /// that are only used internally (i.e. not imported nor exported) with consecutive IDs
    /// starting from `t1`. It replaces the source import's `upper` by a dummy value.
    fn as_comparable(&self) -> Self {
        let external_ids: BTreeSet<_> = self.import_ids().chain(self.export_ids()).collect();

        let mut id_counter = 0;
        let mut replacements = BTreeMap::new();

        let mut maybe_replace = |id: GlobalId| {
            if id.is_transient() && !external_ids.contains(&id) {
                *replacements.entry(id).or_insert_with(|| {
                    id_counter += 1;
                    GlobalId::Transient(id_counter)
                })
            } else {
                id
            }
        };

        let mut source_imports = self.source_imports.clone();
        for (_source, _monotonic, upper) in source_imports.values_mut() {
            *upper = Antichain::new();
        }

        let mut objects_to_build = self.objects_to_build.clone();
        for object in &mut objects_to_build {
            object.id = maybe_replace(object.id);
            object.plan.replace_ids(&mut maybe_replace);
        }

        let mut index_exports = self.index_exports.clone();
        for (desc, _typ) in index_exports.values_mut() {
            desc.on_id = maybe_replace(desc.on_id);
        }

        let mut sink_exports = self.sink_exports.clone();
        for desc in sink_exports.values_mut() {
            desc.from = maybe_replace(desc.from);
        }

        DataflowDescription {
            source_imports,
            index_imports: self.index_imports.clone(),
            objects_to_build,
            index_exports,
            sink_exports,
            as_of: self.as_of.clone(),
            until: self.until.clone(),
            initial_storage_as_of: self.initial_storage_as_of.clone(),
            refresh_schedule: self.refresh_schedule.clone(),
            debug_name: self.debug_name.clone(),
            time_dependence: self.time_dependence.clone(),
        }
    }
}

impl RustType<ProtoDataflowDescription> for DataflowDescription<RenderPlan, CollectionMetadata> {
    fn into_proto(&self) -> ProtoDataflowDescription {
        ProtoDataflowDescription {
            source_imports: self.source_imports.into_proto(),
            index_imports: self.index_imports.into_proto(),
            objects_to_build: self.objects_to_build.into_proto(),
            index_exports: self.index_exports.into_proto(),
            sink_exports: self.sink_exports.into_proto(),
            as_of: self.as_of.into_proto(),
            until: Some(self.until.into_proto()),
            initial_storage_as_of: self.initial_storage_as_of.into_proto(),
            refresh_schedule: self.refresh_schedule.into_proto(),
            debug_name: self.debug_name.clone(),
            time_dependence: self.time_dependence.into_proto(),
        }
    }

    fn from_proto(proto: ProtoDataflowDescription) -> Result<Self, TryFromProtoError> {
        Ok(DataflowDescription {
            source_imports: proto.source_imports.into_rust()?,
            index_imports: proto.index_imports.into_rust()?,
            objects_to_build: proto.objects_to_build.into_rust()?,
            index_exports: proto.index_exports.into_rust()?,
            sink_exports: proto.sink_exports.into_rust()?,
            as_of: proto.as_of.map(|x| x.into_rust()).transpose()?,
            until: proto
                .until
                .map(|x| x.into_rust())
                .transpose()?
                .unwrap_or_else(Antichain::new),
            initial_storage_as_of: proto
                .initial_storage_as_of
                .map(|x| x.into_rust())
                .transpose()?,
            refresh_schedule: proto.refresh_schedule.into_rust()?,
            debug_name: proto.debug_name,
            time_dependence: proto.time_dependence.into_rust()?,
        })
    }
}

impl
    ProtoMapEntry<
        GlobalId,
        (
            SourceInstanceDesc<CollectionMetadata>,
            bool,
            Antichain<mz_repr::Timestamp>,
        ),
    > for ProtoSourceImport
{
    fn from_rust<'a>(
        entry: (
            &'a GlobalId,
            &'a (
                SourceInstanceDesc<CollectionMetadata>,
                bool,
                Antichain<mz_repr::Timestamp>,
            ),
        ),
    ) -> Self {
        ProtoSourceImport {
            id: Some(entry.0.into_proto()),
            source_instance_desc: Some((entry.1).0.into_proto()),
            monotonic: (entry.1).1.into_proto(),
            upper: Some((entry.1).2.into_proto()),
        }
    }

    fn into_rust(
        self,
    ) -> Result<
        (
            GlobalId,
            (
                SourceInstanceDesc<CollectionMetadata>,
                bool,
                Antichain<mz_repr::Timestamp>,
            ),
        ),
        TryFromProtoError,
    > {
        Ok((
            self.id.into_rust_if_some("ProtoSourceImport::id")?,
            (
                self.source_instance_desc
                    .into_rust_if_some("ProtoSourceImport::source_instance_desc")?,
                self.monotonic.into_rust()?,
                self.upper.into_rust_if_some("ProtoSourceImport::upper")?,
            ),
        ))
    }
}

impl ProtoMapEntry<GlobalId, IndexImport> for ProtoIndexImport {
    fn from_rust<'a>(
        (
            id,
            IndexImport {
                desc,
                typ,
                monotonic,
            },
        ): (&'a GlobalId, &'a IndexImport),
    ) -> Self {
        ProtoIndexImport {
            id: Some(id.into_proto()),
            index_desc: Some(desc.into_proto()),
            typ: Some(typ.into_proto()),
            monotonic: monotonic.into_proto(),
        }
    }

    fn into_rust(self) -> Result<(GlobalId, IndexImport), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoIndex::id")?,
            IndexImport {
                desc: self
                    .index_desc
                    .into_rust_if_some("ProtoIndexImport::index_desc")?,
                typ: self.typ.into_rust_if_some("ProtoIndexImport::typ")?,
                monotonic: self.monotonic.into_rust()?,
            },
        ))
    }
}

impl ProtoMapEntry<GlobalId, (IndexDesc, RelationType)> for ProtoIndexExport {
    fn from_rust<'a>(
        (id, (index_desc, typ)): (&'a GlobalId, &'a (IndexDesc, RelationType)),
    ) -> Self {
        ProtoIndexExport {
            id: Some(id.into_proto()),
            index_desc: Some(index_desc.into_proto()),
            typ: Some(typ.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(GlobalId, (IndexDesc, RelationType)), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoIndexExport::id")?,
            (
                self.index_desc
                    .into_rust_if_some("ProtoIndexExport::index_desc")?,
                self.typ.into_rust_if_some("ProtoIndexExport::typ")?,
            ),
        ))
    }
}

impl ProtoMapEntry<GlobalId, ComputeSinkDesc<CollectionMetadata>> for ProtoSinkExport {
    fn from_rust<'a>(
        (id, sink_desc): (&'a GlobalId, &'a ComputeSinkDesc<CollectionMetadata>),
    ) -> Self {
        ProtoSinkExport {
            id: Some(id.into_proto()),
            sink_desc: Some(sink_desc.into_proto()),
        }
    }

    fn into_rust(
        self,
    ) -> Result<(GlobalId, ComputeSinkDesc<CollectionMetadata>), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoSinkExport::id")?,
            self.sink_desc
                .into_rust_if_some("ProtoSinkExport::sink_desc")?,
        ))
    }
}

impl Arbitrary for DataflowDescription<RenderPlan, CollectionMetadata, mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any_dataflow_description(any_source_import_collection_metadata()).boxed()
    }
}

impl Arbitrary for DataflowDescription<OptimizedMirRelationExpr, (), mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any_dataflow_description(any_source_import()).boxed()
    }
}

impl Arbitrary for DataflowDescription<Plan, (), mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any_dataflow_description(any_source_import()).boxed()
    }
}

fn any_dataflow_description<P, S, T>(
    any_source_import: impl Strategy<Value = (GlobalId, (SourceInstanceDesc<S>, bool, Antichain<T>))>,
) -> impl Strategy<Value = DataflowDescription<P, S, T>>
where
    P: Arbitrary,
    S: 'static + Arbitrary,
    T: Arbitrary + timely::PartialOrder,
    ComputeSinkDesc<S, T>: Arbitrary,
{
    // `prop_map` is only implemented for tuples of 12 elements or less, so we need to use nested
    // tuples.
    (
        (
            proptest::collection::vec(any_source_import, 1..3),
            proptest::collection::vec(any_dataflow_index_import(), 1..3),
            proptest::collection::vec(any::<BuildDesc<P>>(), 1..3),
            proptest::collection::vec(any_dataflow_index_export(), 1..3),
            proptest::collection::vec(any::<(GlobalId, ComputeSinkDesc<S, T>)>(), 1..3),
            any::<bool>(),
            proptest::collection::vec(any::<T>(), 1..5),
            any::<bool>(),
            proptest::collection::vec(any::<T>(), 1..5),
            any::<bool>(),
            any::<RefreshSchedule>(),
            proptest::string::string_regex(".*").unwrap(),
        ),
        any::<Option<TimeDependence>>(),
    )
        .prop_map(
            |(
                (
                    source_imports,
                    index_imports,
                    objects_to_build,
                    index_exports,
                    sink_descs,
                    as_of_some,
                    as_of,
                    initial_storage_as_of_some,
                    initial_as_of,
                    refresh_schedule_some,
                    refresh_schedule,
                    debug_name,
                ),
                time_dependence,
            )| DataflowDescription {
                source_imports: BTreeMap::from_iter(source_imports),
                index_imports: BTreeMap::from_iter(index_imports),
                objects_to_build,
                index_exports: BTreeMap::from_iter(index_exports),
                sink_exports: BTreeMap::from_iter(sink_descs),
                as_of: if as_of_some {
                    Some(Antichain::from(as_of))
                } else {
                    None
                },
                until: Antichain::new(),
                initial_storage_as_of: if initial_storage_as_of_some {
                    Some(Antichain::from(initial_as_of))
                } else {
                    None
                },
                refresh_schedule: if refresh_schedule_some {
                    Some(refresh_schedule)
                } else {
                    None
                },
                debug_name,
                time_dependence,
            },
        )
}

fn any_source_import_collection_metadata() -> impl Strategy<
    Value = (
        GlobalId,
        (
            SourceInstanceDesc<CollectionMetadata>,
            bool,
            Antichain<mz_repr::Timestamp>,
        ),
    ),
> {
    (
        any::<GlobalId>(),
        any::<(SourceInstanceDesc<CollectionMetadata>, bool)>().prop_map(
            |(source_instance_desc, monotonic)| (source_instance_desc, monotonic, Antichain::new()),
        ),
    )
}

fn any_source_import() -> impl Strategy<
    Value = (
        GlobalId,
        (SourceInstanceDesc<()>, bool, Antichain<mz_repr::Timestamp>),
    ),
> {
    (any::<GlobalId>(), any::<(SourceInstanceDesc<()>, bool)>()).prop_map(
        |(id, (source_instance_desc, monotonic))| {
            (id, (source_instance_desc, monotonic, Antichain::new()))
        },
    )
}

proptest::prop_compose! {
    fn any_dataflow_index_import()(
        id in any::<GlobalId>(),
        desc in any::<IndexDesc>(),
        typ in any::<RelationType>(),
        monotonic in any::<bool>(),
    ) -> (GlobalId, IndexImport) {
        (id, IndexImport {desc, typ, monotonic})
    }
}

proptest::prop_compose! {
    fn any_dataflow_index_export()(
        id in any::<GlobalId>(),
        index in any::<IndexDesc>(),
        typ in any::<RelationType>(),
    ) -> (GlobalId, (IndexDesc, RelationType)) {
        (id, (index, typ))
    }
}

/// A commonly used name for dataflows contain MIR expressions.
pub type DataflowDesc = DataflowDescription<OptimizedMirRelationExpr, ()>;

/// An index storing processed updates so they can be queried
/// or reused in other computations
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct IndexDesc {
    /// Identity of the collection the index is on.
    pub on_id: GlobalId,
    /// Expressions to be arranged, in order of decreasing primacy.
    #[proptest(strategy = "proptest::collection::vec(any::<MirScalarExpr>(), 1..3)")]
    pub key: Vec<MirScalarExpr>,
}

impl RustType<ProtoIndexDesc> for IndexDesc {
    fn into_proto(&self) -> ProtoIndexDesc {
        ProtoIndexDesc {
            on_id: Some(self.on_id.into_proto()),
            key: self.key.into_proto(),
        }
    }

    fn from_proto(proto: ProtoIndexDesc) -> Result<Self, TryFromProtoError> {
        Ok(IndexDesc {
            on_id: proto.on_id.into_rust_if_some("ProtoIndexDesc::on_id")?,
            key: proto.key.into_rust()?,
        })
    }
}

/// Information about an imported index, and how it will be used by the dataflow.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Arbitrary)]
pub struct IndexImport {
    /// Description of index.
    pub desc: IndexDesc,
    /// Schema and keys of the object the index is on.
    pub typ: RelationType,
    /// Whether the index will supply monotonic data.
    pub monotonic: bool,
}

/// An association of a global identifier to an expression.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildDesc<P> {
    /// TODO(database-issues#7533): Add documentation.
    pub id: GlobalId,
    /// TODO(database-issues#7533): Add documentation.
    pub plan: P,
}

impl RustType<ProtoBuildDesc> for BuildDesc<RenderPlan> {
    fn into_proto(&self) -> ProtoBuildDesc {
        ProtoBuildDesc {
            id: Some(self.id.into_proto()),
            plan: Some(self.plan.into_proto()),
        }
    }

    fn from_proto(x: ProtoBuildDesc) -> Result<Self, TryFromProtoError> {
        Ok(BuildDesc {
            id: x.id.into_rust_if_some("ProtoBuildDesc::id")?,
            plan: x.plan.into_rust_if_some("ProtoBuildDesc::plan")?,
        })
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::assert_ok;
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::ProptestConfig;
    use proptest::proptest;

    use crate::dataflows::DataflowDescription;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]


        #[mz_ore::test]
        fn dataflow_description_protobuf_roundtrip(expect in any::<DataflowDescription<RenderPlan, CollectionMetadata, mz_repr::Timestamp>>()) {
            let actual = protobuf_roundtrip::<_, ProtoDataflowDescription>(&expect);
            assert_ok!(actual);
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
