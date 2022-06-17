// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The types for the dataflow crate.
//!
//! These are extracted into their own crate so that crates that only depend
//! on the interface of the dataflow crate, and not its implementation, can
//! avoid the dependency, as the dataflow crate is very slow to compile.

use std::collections::{BTreeMap, BTreeSet};
use std::convert::TryFrom;
use std::num::NonZeroUsize;

use proptest::prelude::{any, Arbitrary};
use proptest::prop_oneof;
use proptest::strategy::{BoxedStrategy, Just, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;

use mz_expr::{CollectionPlan, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr};
use mz_repr::proto::any_uuid;
use mz_repr::proto::{IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::{Diff, GlobalId, RelationType, Row};

use crate::client::controller::storage::CollectionMetadata;
use crate::types::sinks::SinkDesc;
use crate::types::sources::SourceDesc;
use crate::Plan;

use proto_dataflow_description::*;

pub mod connections;
pub mod sinks;
pub mod sources;

include!(concat!(env!("OUT_DIR"), "/mz_dataflow_types.types.rs"));

/// The response from a `Peek`.
///
/// Note that each `Peek` expects to generate exactly one `PeekResponse`, i.e.
/// we expect a 1:1 contract between `Peek` and `PeekResponse`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum PeekResponse {
    Rows(Vec<(Row, NonZeroUsize)>),
    Error(String),
    Canceled,
}

impl PeekResponse {
    pub fn unwrap_rows(self) -> Vec<(Row, NonZeroUsize)> {
        match self {
            PeekResponse::Rows(rows) => rows,
            PeekResponse::Error(_) | PeekResponse::Canceled => {
                panic!("PeekResponse::unwrap_rows called on {:?}", self)
            }
        }
    }
}

impl RustType<ProtoPeekResponse> for PeekResponse {
    fn into_proto(&self) -> ProtoPeekResponse {
        use proto_peek_response::Kind::*;
        use proto_peek_response::*;
        ProtoPeekResponse {
            kind: Some(match self {
                PeekResponse::Rows(rows) => Rows(ProtoRows {
                    rows: rows
                        .iter()
                        .map(|(r, d)| ProtoRow {
                            row: Some(r.into_proto()),
                            diff: d.into_proto(),
                        })
                        .collect(),
                }),
                PeekResponse::Error(err) => proto_peek_response::Kind::Error(err.clone()),
                PeekResponse::Canceled => Canceled(()),
            }),
        }
    }

    fn from_proto(proto: ProtoPeekResponse) -> Result<Self, TryFromProtoError> {
        use proto_peek_response::Kind::*;
        match proto.kind {
            Some(Rows(rows)) => Ok(PeekResponse::Rows(
                rows.rows
                    .into_iter()
                    .map(|row| {
                        Ok((
                            row.row.into_rust_if_some("ProtoRow::row")?,
                            NonZeroUsize::from_proto(row.diff)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, TryFromProtoError>>()?,
            )),
            Some(proto_peek_response::Kind::Error(err)) => Ok(PeekResponse::Error(err)),
            Some(Canceled(())) => Ok(PeekResponse::Canceled),
            None => Err(TryFromProtoError::missing_field("ProtoPeekResponse::kind")),
        }
    }
}

impl Arbitrary for PeekResponse {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            proptest::collection::vec(
                (
                    any::<Row>(),
                    (1..usize::MAX).prop_map(|u| NonZeroUsize::try_from(u).unwrap())
                ),
                1..11
            )
            .prop_map(PeekResponse::Rows),
            ".*".prop_map(PeekResponse::Error),
            Just(PeekResponse::Canceled),
        ]
        .boxed()
    }
}

/// Various responses that can be communicated about the progress of a TAIL command.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum TailResponse<T = mz_repr::Timestamp> {
    /// A batch of updates over a non-empty interval of time.
    Batch(TailBatch<T>),
    /// The TAIL dataflow was dropped, leaving updates from this frontier onward unspecified.
    DroppedAt(Antichain<T>),
}

impl RustType<ProtoTailResponse> for TailResponse<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoTailResponse {
        use proto_tail_response::Kind::*;
        ProtoTailResponse {
            kind: Some(match self {
                TailResponse::Batch(tail_batch) => Batch(tail_batch.into_proto()),
                TailResponse::DroppedAt(antichain) => DroppedAt(antichain.into()),
            }),
        }
    }

    fn from_proto(proto: ProtoTailResponse) -> Result<Self, TryFromProtoError> {
        use proto_tail_response::Kind::*;
        match proto.kind {
            Some(Batch(tail_batch)) => Ok(TailResponse::Batch(tail_batch.into_rust()?)),
            Some(DroppedAt(antichain)) => Ok(TailResponse::DroppedAt(antichain.into())),
            None => Err(TryFromProtoError::missing_field("ProtoTailResponse::kind")),
        }
    }
}

impl Arbitrary for TailResponse<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<TailBatch<mz_repr::Timestamp>>().prop_map(TailResponse::Batch),
            proptest::collection::vec(any::<u64>(), 1..4)
                .prop_map(|antichain| TailResponse::DroppedAt(Antichain::from(antichain)))
        ]
        .boxed()
    }
}

/// A batch of updates for the interval `[lower, upper)`.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TailBatch<T> {
    /// The lower frontier of the batch of updates.
    pub lower: Antichain<T>,
    /// The upper frontier of the batch of updates.
    pub upper: Antichain<T>,
    /// All updates greater than `lower` and not greater than `upper`.
    pub updates: Vec<(T, Row, Diff)>,
}

impl RustType<ProtoTailBatch> for TailBatch<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoTailBatch {
        use proto_tail_batch::ProtoUpdate;
        ProtoTailBatch {
            lower: Some((&self.lower).into()),
            upper: Some((&self.upper).into()),
            updates: self
                .updates
                .iter()
                .map(|(t, r, d)| ProtoUpdate {
                    timestamp: *t,
                    row: Some(r.into_proto()),
                    diff: *d,
                })
                .collect(),
        }
    }

    fn from_proto(proto: ProtoTailBatch) -> Result<Self, TryFromProtoError> {
        Ok(TailBatch {
            lower: proto
                .lower
                .map(Into::into)
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoTailUpdate::lower"))?,
            upper: proto
                .upper
                .map(Into::into)
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoTailUpdate::upper"))?,
            updates: proto
                .updates
                .into_iter()
                .map(|update| {
                    Ok((
                        update.timestamp,
                        update.row.into_rust_if_some("ProtoUpdate::row")?,
                        update.diff,
                    ))
                })
                .collect::<Result<Vec<_>, TryFromProtoError>>()?,
        })
    }
}

impl Arbitrary for TailBatch<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            proptest::collection::vec(any::<u64>(), 1..4),
            proptest::collection::vec(any::<u64>(), 1..4),
            proptest::collection::vec(
                (any::<mz_repr::Timestamp>(), any::<Row>(), any::<Diff>()),
                1..4,
            ),
        )
            .prop_map(|(lower, upper, updates)| TailBatch {
                lower: Antichain::from(lower),
                upper: Antichain::from(upper),
                updates,
            })
            .boxed()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
/// A batch of updates to be fed to a local input
pub struct Update<T = mz_repr::Timestamp> {
    pub row: Row,
    pub timestamp: T,
    pub diff: Diff,
}

/// A commonly used name for dataflows contain MIR expressions.
pub type DataflowDesc = DataflowDescription<OptimizedMirRelationExpr, ()>;

/// An association of a global identifier to an expression.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildDesc<P> {
    pub id: GlobalId,
    pub plan: P,
}

impl RustType<ProtoBuildDesc> for BuildDesc<crate::plan::Plan> {
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

/// A description of an instantiation of a source.
///
/// This includes a description of the source, but additionally any
/// context-dependent options like the ability to apply filtering and
/// projection to the records as they emerge.
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceDesc<M> {
    /// A description of the source to construct.
    //TODO(petrosagg): COMPUTE doesn't need the full description of the source, only the metadata to
    // read a storage collection. Remove this field
    pub description: crate::types::sources::SourceDesc,
    /// Arguments for this instantiation of the source.
    pub arguments: SourceInstanceArguments,
    /// Additional metadata used by storage instances to render this source instance and by the
    /// storage client of a compute instance to read it.
    pub storage_metadata: M,
}

impl Arbitrary for SourceInstanceDesc<CollectionMetadata> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<SourceDesc>(),
            any::<SourceInstanceArguments>(),
            any::<CollectionMetadata>(),
        )
            .prop_map(
                |(description, arguments, storage_metadata)| SourceInstanceDesc {
                    description,
                    arguments,
                    storage_metadata,
                },
            )
            .boxed()
    }
}

impl RustType<ProtoSourceInstanceDesc> for SourceInstanceDesc<CollectionMetadata> {
    fn into_proto(&self) -> ProtoSourceInstanceDesc {
        ProtoSourceInstanceDesc {
            description: Some(self.description.into_proto()),
            arguments: Some(self.arguments.into_proto()),
            storage_metadata: Some(self.storage_metadata.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceInstanceDesc) -> Result<Self, TryFromProtoError> {
        Ok(SourceInstanceDesc {
            description: proto
                .description
                .into_rust_if_some("ProtoSourceInstanceDesc::description")?,
            arguments: proto
                .arguments
                .into_rust_if_some("ProtoSourceInstanceDesc::arguments")?,
            storage_metadata: proto
                .storage_metadata
                .into_rust_if_some("ProtoSourceInstanceDesc::storage_metadata")?,
        })
    }
}

/// Per-source construction arguments.
#[derive(Arbitrary, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceArguments {
    /// Optional linear operators that can be applied record-by-record.
    pub operators: Option<crate::LinearOperator>,
}

impl RustType<ProtoSourceInstanceArguments> for SourceInstanceArguments {
    fn into_proto(&self) -> ProtoSourceInstanceArguments {
        ProtoSourceInstanceArguments {
            operators: self.operators.into_proto(),
        }
    }

    fn from_proto(proto: ProtoSourceInstanceArguments) -> Result<Self, TryFromProtoError> {
        Ok(SourceInstanceArguments {
            operators: proto.operators.into_rust()?,
        })
    }
}

/// Type alias for source subscriptions, (dataflow_id, source_id).
pub type SourceInstanceId = (uuid::Uuid, mz_repr::GlobalId);

/// A description of a dataflow to construct and results to surface.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct DataflowDescription<P, S = (), T = mz_repr::Timestamp> {
    /// Sources instantiations made available to the dataflow.
    pub source_imports: BTreeMap<GlobalId, SourceInstanceDesc<S>>,
    /// Indexes made available to the dataflow.
    pub index_imports: BTreeMap<GlobalId, (IndexDesc, RelationType)>,
    /// Views and indexes to be built and stored in the local context.
    /// Objects must be built in the specific order, as there may be
    /// dependencies of later objects on prior identifiers.
    pub objects_to_build: Vec<BuildDesc<P>>,
    /// Indexes to be made available to be shared with other dataflows
    /// (id of new index, description of index, relationtype of base source/view)
    pub index_exports: BTreeMap<GlobalId, (IndexDesc, RelationType)>,
    /// sinks to be created
    /// (id of new sink, description of sink)
    pub sink_exports: BTreeMap<GlobalId, crate::types::sinks::SinkDesc<T>>,
    /// An optional frontier to which inputs should be advanced.
    ///
    /// If this is set, it should override the default setting determined by
    /// the upper bound of `since` frontiers contributing to the dataflow.
    /// It is an error for this to be set to a frontier not beyond that default.
    pub as_of: Option<Antichain<T>>,
    /// Human readable name
    pub debug_name: String,
    /// Unique ID of the dataflow
    pub id: uuid::Uuid,
}

fn any_source_import() -> impl Strategy<Value = (GlobalId, SourceInstanceDesc<CollectionMetadata>)>
{
    (
        any::<GlobalId>(),
        any::<SourceInstanceDesc<CollectionMetadata>>(),
    )
}

proptest::prop_compose! {
    fn any_dataflow_index()(
        id in any::<GlobalId>(),
        index in any::<IndexDesc>(),
        typ in any::<RelationType>()
    ) -> (GlobalId, (IndexDesc, RelationType)) {
        (id, (index, typ))
    }
}

proptest::prop_compose! {
    fn any_dataflow_description()(
        source_imports in proptest::collection::vec(any_source_import(), 1..3),
        index_imports in proptest::collection::vec(any_dataflow_index(), 1..3),
        objects_to_build in proptest::collection::vec(any::<BuildDesc<Plan>>(), 1..3),
        index_exports in proptest::collection::vec(any_dataflow_index(), 1..3),
        sink_descs in proptest::collection::vec(any::<(GlobalId, SinkDesc<mz_repr::Timestamp>)>(), 1..3),
        as_of_some in any::<bool>(),
        as_of in proptest::collection::vec(any::<u64>(), 1..5),
        debug_name in ".*",
        id in any_uuid(),
    ) -> DataflowDescription<Plan, CollectionMetadata, mz_repr::Timestamp> {
        DataflowDescription {
            source_imports: BTreeMap::from_iter(source_imports.into_iter()),
            index_imports: BTreeMap::from_iter(index_imports.into_iter()),
            objects_to_build,
            index_exports: BTreeMap::from_iter(index_exports.into_iter()),
            sink_exports: BTreeMap::from_iter(
                sink_descs.into_iter(),
            ),
            as_of: if as_of_some {
                Some(Antichain::from(as_of))
            } else {
                None
            },
            debug_name,
            id,
        }
    }
}

impl Arbitrary for DataflowDescription<Plan, CollectionMetadata, mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any_dataflow_description().boxed()
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
            debug_name: name,
            id: uuid::Uuid::new_v4(),
        }
    }

    /// Imports a previously exported index.
    ///
    /// This method makes available an index previously exported as `id`, identified
    /// to the query by `description` (which names the view the index arranges, and
    /// the keys by which it is arranged).
    pub fn import_index(&mut self, id: GlobalId, description: IndexDesc, typ: RelationType) {
        self.index_imports.insert(id, (description, typ));
    }

    /// Imports a source and makes it available as `id`.
    pub fn import_source(&mut self, id: GlobalId, description: SourceDesc) {
        // Import the source with no linear operators applied to it.
        // They may be populated by whole-dataflow optimization.
        self.source_imports.insert(
            id,
            SourceInstanceDesc {
                description,
                storage_metadata: (),
                arguments: SourceInstanceArguments { operators: None },
            },
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
    pub fn export_sink(&mut self, id: GlobalId, description: SinkDesc<T>) {
        self.sink_exports.insert(id, description);
    }

    /// Returns true iff `id` is already imported.
    pub fn is_imported(&self, id: &GlobalId) -> bool {
        self.objects_to_build.iter().any(|bd| &bd.id == id)
            || self.source_imports.keys().any(|i| i == id)
    }

    /// Assigns the `as_of` frontier to the supplied argument.
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

    /// The number of columns associated with an identifier in the dataflow.
    pub fn arity_of(&self, id: &GlobalId) -> usize {
        for (source_id, source) in self.source_imports.iter() {
            if source_id == id {
                return source.description.desc.arity();
            }
        }
        for (desc, typ) in self.index_imports.values() {
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
}

impl<P, S, T> DataflowDescription<P, S, T>
where
    P: CollectionPlan,
{
    /// Identifiers of exported objects (indexes and sinks).
    pub fn export_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.index_exports
            .keys()
            .chain(self.sink_exports.keys())
            .cloned()
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

    /// Computes the set of identifiers upon which the specified collection
    /// identifier depends.
    ///
    /// `id` must specify a valid object in `objects_to_build`.
    pub fn depends_on(&self, collection_id: GlobalId) -> BTreeSet<GlobalId> {
        let mut out = BTreeSet::new();
        self.depends_on_into(collection_id, &mut out);
        out
    }

    /// Like `depends_on`, but appends to an existing `BTreeSet`.
    pub fn depends_on_into(&self, collection_id: GlobalId, out: &mut BTreeSet<GlobalId>) {
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
        for (index_id, (desc, _typ)) in &self.index_imports {
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
            self.depends_on_into(id, out)
        }
    }

    /// Determine a unique id for this dataflow based on the indexes it exports.
    // TODO: The semantics of this function are only useful for command reconciliation at the moment.
    pub fn global_id(&self) -> Option<GlobalId> {
        let mut exports = self.export_ids();
        let id = exports.next()?;
        if exports.all(|other_id| other_id == id) {
            Some(id)
        } else {
            None
        }
    }
}

impl<P: PartialEq, S: PartialEq, T: timely::PartialOrder> DataflowDescription<P, S, T> {
    /// Determine if a dataflow description is compatible with this dataflow description.
    ///
    /// Compatible dataflows have equal exports, imports, and objects to build. The `as_of` of
    /// the receiver has to be less equal the `other` `as_of`.
    ///
    // TODO: The semantics of this function are only useful for command reconciliation at the moment.
    pub fn compatible_with(&self, other: &Self) -> bool {
        let equality = self.index_exports == other.index_exports
            && self.sink_exports == other.sink_exports
            && self.objects_to_build == other.objects_to_build
            && self.index_imports == other.index_imports
            && self.source_imports == other.source_imports;
        let partial = if let (Some(as_of), Some(other_as_of)) = (&self.as_of, &other.as_of) {
            timely::PartialOrder::less_equal(as_of, other_as_of)
        } else {
            false
        };
        equality && partial
    }
}

impl RustType<ProtoDataflowDescription>
    for DataflowDescription<crate::plan::Plan, CollectionMetadata>
{
    fn into_proto(&self) -> ProtoDataflowDescription {
        ProtoDataflowDescription {
            source_imports: self.source_imports.into_proto(),
            index_imports: self.index_imports.into_proto(),
            objects_to_build: self.objects_to_build.into_proto(),
            index_exports: self.index_exports.into_proto(),
            sink_exports: self.sink_exports.into_proto(),
            as_of: self.as_of.as_ref().map(Into::into),
            debug_name: self.debug_name.clone(),
            id: Some(self.id.into_proto()),
        }
    }

    fn from_proto(proto: ProtoDataflowDescription) -> Result<Self, TryFromProtoError> {
        Ok(DataflowDescription {
            source_imports: proto.source_imports.into_rust()?,
            index_imports: proto.index_imports.into_rust()?,
            objects_to_build: proto.objects_to_build.into_rust()?,
            index_exports: proto.index_exports.into_rust()?,
            sink_exports: proto.sink_exports.into_rust()?,
            as_of: proto.as_of.map(Into::into),
            debug_name: proto.debug_name,
            id: proto.id.into_rust_if_some("ProtoDataflowDescription::id")?,
        })
    }
}

impl ProtoMapEntry<GlobalId, SourceInstanceDesc<CollectionMetadata>> for ProtoSourceImport {
    fn from_rust<'a>(entry: (&'a GlobalId, &'a SourceInstanceDesc<CollectionMetadata>)) -> Self {
        ProtoSourceImport {
            id: Some(entry.0.into_proto()),
            source_instance_desc: Some(entry.1.into_proto()),
        }
    }

    fn into_rust(
        self,
    ) -> Result<(GlobalId, SourceInstanceDesc<CollectionMetadata>), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoSourceImport::id")?,
            self.source_instance_desc
                .into_rust_if_some("ProtoSourceImport::source_instance_desc")?,
        ))
    }
}

impl ProtoMapEntry<GlobalId, (IndexDesc, RelationType)> for ProtoIndex {
    fn from_rust<'a>(
        (id, (index_desc, typ)): (&'a GlobalId, &'a (IndexDesc, RelationType)),
    ) -> Self {
        ProtoIndex {
            id: Some(id.into_proto()),
            index_desc: Some(index_desc.into_proto()),
            typ: Some(typ.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(GlobalId, (IndexDesc, RelationType)), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoIndex::id")?,
            (
                self.index_desc
                    .into_rust_if_some("ProtoIndex::index_desc")?,
                self.typ.into_rust_if_some("ProtoIndex::typ")?,
            ),
        ))
    }
}

impl ProtoMapEntry<GlobalId, SinkDesc> for ProtoSinkExport {
    fn from_rust<'a>((id, sink_desc): (&'a GlobalId, &'a SinkDesc)) -> Self {
        ProtoSinkExport {
            id: Some(id.into_proto()),
            sink_desc: Some(sink_desc.into_proto()),
        }
    }

    fn into_rust(self) -> Result<(GlobalId, SinkDesc), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoSinkExport::id")?,
            self.sink_desc
                .into_rust_if_some("ProtoSinkExport::sink_desc")?,
        ))
    }
}

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

// TODO: change contract to ensure that the operator is always applied to
// streams of rows
/// In-place restrictions that can be made to rows.
///
/// These fields indicate *optional* information that may applied to
/// streams of rows. Any row that does not satisfy all predicates may
/// be discarded, and any column not listed in the projection may be
/// replaced by a default value.
///
/// The intended order of operations is that the predicates are first
/// applied, and columns not in projection can then be overwritten with
/// default values. This allows the projection to avoid capturing columns
/// used by the predicates but not otherwise required.
#[derive(Arbitrary, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
pub struct LinearOperator {
    /// Rows that do not pass all predicates may be discarded.
    #[proptest(strategy = "proptest::collection::vec(any::<MirScalarExpr>(), 0..2)")]
    pub predicates: Vec<MirScalarExpr>,
    /// Columns not present in `projection` may be replaced with
    /// default values.
    pub projection: Vec<usize>,
}

impl RustType<ProtoLinearOperator> for LinearOperator {
    fn into_proto(&self) -> ProtoLinearOperator {
        ProtoLinearOperator {
            predicates: self.predicates.into_proto(),
            projection: self.projection.into_proto(),
        }
    }

    fn from_proto(proto: ProtoLinearOperator) -> Result<Self, TryFromProtoError> {
        Ok(LinearOperator {
            predicates: proto.predicates.into_rust()?,
            projection: proto.projection.into_rust()?,
        })
    }
}

impl LinearOperator {
    /// Reports whether this linear operator is trivial when applied to an
    /// input of the specified arity.
    pub fn is_trivial(&self, arity: usize) -> bool {
        self.predicates.is_empty() && self.projection.iter().copied().eq(0..arity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_repr::proto::protobuf_roundtrip;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn dataflow_description_protobuf_roundtrip(expect in any::<DataflowDescription<Plan, CollectionMetadata, mz_repr::Timestamp>>()) {
            let actual = protobuf_roundtrip::<_, ProtoDataflowDescription>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
