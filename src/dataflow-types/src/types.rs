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
use std::num::NonZeroUsize;

use proptest::prelude::{any, Arbitrary};
use proptest::prop_oneof;
use proptest::strategy::{BoxedStrategy, Just, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;

use mz_expr::{CollectionPlan, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr};
use mz_repr::proto::{any_uuid, FromProtoIfSome, ProtoRepr, TryFromProtoError, TryIntoIfSome};
use mz_repr::{Diff, GlobalId, RelationType, Row};

use crate::client::controller::storage::CollectionMetadata;
use crate::types::sinks::SinkDesc;
use crate::types::sources::SourceDesc;
use crate::Plan;

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

impl From<&PeekResponse> for ProtoPeekResponse {
    fn from(x: &PeekResponse) -> Self {
        use proto_peek_response::Kind::*;
        use proto_peek_response::*;
        ProtoPeekResponse {
            kind: Some(match x {
                PeekResponse::Rows(rows) => Rows(ProtoRows {
                    rows: rows
                        .iter()
                        .map(|(r, d)| ProtoRow {
                            row: Some(r.into()),
                            diff: d.into_proto(),
                        })
                        .collect(),
                }),
                PeekResponse::Error(err) => proto_peek_response::Kind::Error(err.clone()),
                PeekResponse::Canceled => Canceled(()),
            }),
        }
    }
}

impl TryFrom<ProtoPeekResponse> for PeekResponse {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoPeekResponse) -> Result<Self, TryFromProtoError> {
        use proto_peek_response::Kind::*;
        match x.kind {
            Some(Rows(rows)) => Ok(PeekResponse::Rows(
                rows.rows
                    .into_iter()
                    .map(|row| {
                        Ok((
                            row.row.try_into_if_some("ProtoRow::row")?,
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

impl From<&TailResponse<mz_repr::Timestamp>> for ProtoTailResponse {
    fn from(x: &TailResponse<mz_repr::Timestamp>) -> Self {
        use proto_tail_response::Kind::*;
        ProtoTailResponse {
            kind: Some(match x {
                TailResponse::Batch(tail_batch) => Batch(tail_batch.into()),
                TailResponse::DroppedAt(antichain) => DroppedAt(antichain.into()),
            }),
        }
    }
}

impl TryFrom<ProtoTailResponse> for TailResponse<mz_repr::Timestamp> {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoTailResponse) -> Result<Self, Self::Error> {
        use proto_tail_response::Kind::*;
        match x.kind {
            Some(Batch(tail_batch)) => Ok(TailResponse::Batch(tail_batch.try_into()?)),
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

impl From<&TailBatch<mz_repr::Timestamp>> for ProtoTailBatch {
    fn from(x: &TailBatch<mz_repr::Timestamp>) -> Self {
        use proto_tail_batch::ProtoUpdate;
        ProtoTailBatch {
            lower: Some((&x.lower).into()),
            upper: Some((&x.upper).into()),
            updates: x
                .updates
                .iter()
                .map(|(t, r, d)| ProtoUpdate {
                    timestamp: *t,
                    row: Some(r.into()),
                    diff: *d,
                })
                .collect(),
        }
    }
}

impl TryFrom<ProtoTailBatch> for TailBatch<mz_repr::Timestamp> {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoTailBatch) -> Result<Self, Self::Error> {
        Ok(TailBatch {
            lower: x
                .lower
                .map(Into::into)
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoTailUpdate::lower"))?,
            upper: x
                .upper
                .map(Into::into)
                .ok_or_else(|| TryFromProtoError::missing_field("ProtoTailUpdate::upper"))?,
            updates: x
                .updates
                .into_iter()
                .map(|update| {
                    Ok((
                        update.timestamp,
                        update.row.try_into_if_some("ProtoUpdate::row")?,
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

impl From<&BuildDesc<crate::plan::Plan>> for ProtoBuildDesc {
    fn from(x: &BuildDesc<crate::plan::Plan>) -> Self {
        ProtoBuildDesc {
            id: Some((&x.id).into()),
            plan: Some((&x.plan).into()),
        }
    }
}

impl TryFrom<ProtoBuildDesc> for BuildDesc<crate::plan::Plan> {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoBuildDesc) -> Result<Self, Self::Error> {
        Ok(BuildDesc {
            id: x.id.try_into_if_some("ProtoBuildDesc::id")?,
            plan: x.plan.try_into_if_some("ProtoBuildDesc::plan")?,
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
            any::<SourceInstanceArguments>(),
            any::<CollectionMetadata>(),
        )
            .prop_map(|(arguments, storage_metadata)| SourceInstanceDesc {
                description: crate::sources::any_source_desc_stub(),
                arguments,
                storage_metadata,
            })
            .boxed()
    }
}

impl From<&SourceInstanceDesc<CollectionMetadata>> for ProtoSourceInstanceDesc {
    fn from(x: &SourceInstanceDesc<CollectionMetadata>) -> Self {
        ProtoSourceInstanceDesc {
            description: serde_json::to_string(&x.description).unwrap(),
            arguments: Some((&x.arguments).into()),
            storage_metadata: Some((&x.storage_metadata).into()),
        }
    }
}

impl TryFrom<ProtoSourceInstanceDesc> for SourceInstanceDesc<CollectionMetadata> {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoSourceInstanceDesc) -> Result<Self, Self::Error> {
        Ok(SourceInstanceDesc {
            description: serde_json::from_str(&x.description).map_err(TryFromProtoError::from)?,
            arguments: x
                .arguments
                .try_into_if_some("ProtoSourceInstanceDesc::arguments")?,
            storage_metadata: x
                .storage_metadata
                .try_into_if_some("ProtoSourceInstanceDesc::storage_metadata")?,
        })
    }
}

/// Per-source construction arguments.
#[derive(Arbitrary, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceArguments {
    /// Optional linear operators that can be applied record-by-record.
    pub operators: Option<crate::LinearOperator>,
}

impl From<&SourceInstanceArguments> for ProtoSourceInstanceArguments {
    fn from(x: &SourceInstanceArguments) -> Self {
        ProtoSourceInstanceArguments {
            operators: x.operators.as_ref().map(Into::into),
        }
    }
}

impl TryFrom<ProtoSourceInstanceArguments> for SourceInstanceArguments {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoSourceInstanceArguments) -> Result<Self, Self::Error> {
        Ok(SourceInstanceArguments {
            operators: x.operators.map(TryInto::try_into).transpose()?,
        })
    }
}

/// Type alias for source subscriptions, (dataflow_id, source_id).
pub type SourceInstanceId = (uuid::Uuid, mz_repr::GlobalId);

/// A formed request for source instantiation.
#[derive(Debug, Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SourceInstanceRequest<T = mz_repr::Timestamp> {
    /// The source's own identifier.
    pub source_id: mz_repr::GlobalId,
    /// A dataflow identifier that should be unique across dataflows.
    pub dataflow_id: uuid::Uuid,
    /// Arguments to the source instantiation.
    pub arguments: SourceInstanceArguments,
    /// Frontier beyond which updates must be correct.
    pub as_of: Antichain<T>,
}

impl<T> SourceInstanceRequest<T> {
    /// Source identifier uniquely identifying this instantiation.
    pub fn unique_id(&self) -> SourceInstanceId {
        (self.dataflow_id, self.source_id)
    }
}

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
        sink_ids in proptest::collection::vec(any::<GlobalId>(), 1..3),
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
                sink_ids
                    .into_iter()
                    .map(|id| (id, crate::sinks::any_sink_desc_stub())),
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
    ///
    /// The `requesting_view` argument is currently necessary to correctly track the
    /// dependencies of views on indexes.
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

    /// Exports as `id` an index on `on_id`.
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
        // TODO: This could be implemented without heap allocation.
        let mut exports = self.export_ids().collect::<Vec<_>>();
        exports.sort_unstable();
        exports.dedup();
        if exports.len() == 1 {
            return exports.pop();
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

impl From<&DataflowDescription<crate::plan::Plan, CollectionMetadata>>
    for ProtoDataflowDescription
{
    fn from(x: &DataflowDescription<crate::plan::Plan, CollectionMetadata>) -> Self {
        use proto_dataflow_description::*;
        ProtoDataflowDescription {
            source_imports: x
                .source_imports
                .iter()
                .map(|(id, source_instance_desc)| ProtoSourceImport {
                    id: Some(id.into()),
                    source_instance_desc: Some(source_instance_desc.into()),
                })
                .collect(),
            index_imports: x
                .index_imports
                .iter()
                .map(|(id, (index_desc, typ))| ProtoIndex {
                    id: Some(id.into()),
                    index_desc: Some(index_desc.into()),
                    typ: Some(typ.into()),
                })
                .collect(),
            objects_to_build: x.objects_to_build.iter().map(Into::into).collect(),
            index_exports: x
                .index_exports
                .iter()
                .map(|(id, (index_desc, typ))| ProtoIndex {
                    id: Some(id.into()),
                    index_desc: Some(index_desc.into()),
                    typ: Some(typ.into()),
                })
                .collect(),
            sink_exports: x
                .sink_exports
                .iter()
                .map(|(id, sink_desc)| ProtoSinkExport {
                    id: Some(id.into()),
                    sink_desc: serde_json::to_string(sink_desc).unwrap(),
                })
                .collect(),
            as_of: x.as_of.as_ref().map(Into::into),
            debug_name: x.debug_name.clone(),
            id: Some(x.id.into_proto()),
        }
    }
}

impl TryFrom<ProtoDataflowDescription>
    for DataflowDescription<crate::plan::Plan, CollectionMetadata>
{
    type Error = TryFromProtoError;

    fn try_from(x: ProtoDataflowDescription) -> Result<Self, Self::Error> {
        Ok(DataflowDescription {
            source_imports: x
                .source_imports
                .into_iter()
                .map(|inner| {
                    Ok((
                        inner.id.try_into_if_some("ProtoSourceImport::id")?,
                        inner
                            .source_instance_desc
                            .try_into_if_some("ProtoSourceImport::source_instance_desc")?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, Self::Error>>()?,
            index_imports: x
                .index_imports
                .into_iter()
                .map(|inner| {
                    Ok((
                        inner.id.try_into_if_some("ProtoIndex::id")?,
                        (
                            inner
                                .index_desc
                                .try_into_if_some("ProtoIndex::index_desc")?,
                            inner.typ.try_into_if_some("ProtoIndex::typ")?,
                        ),
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, Self::Error>>()?,
            objects_to_build: x
                .objects_to_build
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            index_exports: x
                .index_exports
                .into_iter()
                .map(|inner| {
                    Ok((
                        inner.id.try_into_if_some("ProtoIndex::id")?,
                        (
                            inner
                                .index_desc
                                .try_into_if_some("ProtoIndex::index_desc")?,
                            inner.typ.try_into_if_some("ProtoIndex::typ")?,
                        ),
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, Self::Error>>()?,
            sink_exports: x
                .sink_exports
                .into_iter()
                .map(|inner| {
                    Ok((
                        inner.id.try_into_if_some("ProtoSinkExport::id")?,
                        serde_json::from_str(&inner.sink_desc).map_err(TryFromProtoError::from)?,
                    ))
                })
                .collect::<Result<BTreeMap<_, _>, Self::Error>>()?,
            as_of: x.as_of.map(Into::into),
            debug_name: x.debug_name,
            id: x.id.from_proto_if_some("ProtoDataflowDescription::id")?,
        })
    }
}

/// Types and traits related to the introduction of changing collections into `dataflow`.
pub mod sources {
    use std::collections::{BTreeMap, HashMap};
    use std::ops::{Add, AddAssign, Deref, DerefMut, Sub};
    use std::time::Duration;

    use anyhow::{anyhow, bail};
    use bytes::BufMut;
    use chrono::NaiveDateTime;
    use differential_dataflow::lattice::Lattice;
    use globset::Glob;
    use http::Uri;
    use mz_persist_client::read::ReadHandle;
    use mz_persist_client::{PersistLocation, ShardId};
    use mz_persist_types::Codec64;
    use prost::Message;
    use serde::{Deserialize, Serialize};
    use timely::progress::Timestamp;
    use uuid::Uuid;

    use mz_kafka_util::KafkaAddrs;
    use mz_persist_types::Codec;
    use mz_repr::proto::TryFromProtoError;
    use mz_repr::{ColumnType, GlobalId, RelationDesc, RelationType, Row, ScalarType};

    use crate::postgres_source::PostgresSourceDetails;
    use crate::DataflowError;

    include!(concat!(
        env!("OUT_DIR"),
        "/mz_dataflow_types.types.sources.rs"
    ));

    // Types and traits related to the *decoding* of data for sources.
    pub mod encoding {
        use anyhow::Context;
        use serde::{Deserialize, Serialize};

        use mz_interchange::{avro, protobuf};
        use mz_repr::{ColumnType, RelationDesc, ScalarType};

        /// A description of how to interpret data from various sources
        ///
        /// Almost all sources only present values as part of their records, but Kafka allows a key to be
        /// associated with each record, which has a possibly independent encoding.
        #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
        pub enum SourceDataEncoding {
            Single(DataEncoding),
            KeyValue {
                key: DataEncoding,
                value: DataEncoding,
            },
        }

        /// A description of how each row should be decoded, from a string of bytes to a sequence of
        /// Differential updates.
        #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
        pub enum DataEncoding {
            Avro(AvroEncoding),
            Protobuf(ProtobufEncoding),
            Csv(CsvEncoding),
            Regex(RegexEncoding),
            Postgres,
            Bytes,
            Text,
            RowCodec(RelationDesc),
        }

        impl SourceDataEncoding {
            pub fn key_ref(&self) -> Option<&DataEncoding> {
                match self {
                    SourceDataEncoding::Single(_) => None,
                    SourceDataEncoding::KeyValue { key, .. } => Some(key),
                }
            }

            /// Return either the Single encoding if this was a `SourceDataEncoding::Single`, else return the value encoding
            pub fn value(self) -> DataEncoding {
                match self {
                    SourceDataEncoding::Single(encoding) => encoding,
                    SourceDataEncoding::KeyValue { value, .. } => value,
                }
            }

            pub fn value_ref(&self) -> &DataEncoding {
                match self {
                    SourceDataEncoding::Single(encoding) => encoding,
                    SourceDataEncoding::KeyValue { value, .. } => value,
                }
            }

            pub fn desc(&self) -> Result<(Option<RelationDesc>, RelationDesc), anyhow::Error> {
                Ok(match self {
                    SourceDataEncoding::Single(value) => (None, value.desc()?),
                    SourceDataEncoding::KeyValue { key, value } => {
                        (Some(key.desc()?), value.desc()?)
                    }
                })
            }
        }

        pub fn included_column_desc(included_columns: Vec<(&str, ColumnType)>) -> RelationDesc {
            let mut desc = RelationDesc::empty();
            for (name, ty) in included_columns {
                desc = desc.with_column(name, ty);
            }
            desc
        }

        impl DataEncoding {
            /// Computes the [`RelationDesc`] for the relation specified by this
            /// data encoding and envelope.
            ///
            /// If a key desc is provided it will be prepended to the returned desc
            fn desc(&self) -> Result<RelationDesc, anyhow::Error> {
                // Add columns for the data, based on the encoding format.
                Ok(match self {
                    DataEncoding::Bytes => {
                        RelationDesc::empty().with_column("data", ScalarType::Bytes.nullable(false))
                    }
                    DataEncoding::Avro(AvroEncoding { schema, .. }) => {
                        let parsed_schema =
                            avro::parse_schema(schema).context("validating avro schema")?;
                        avro::schema_to_relationdesc(parsed_schema)
                            .context("validating avro schema")?
                    }
                    DataEncoding::Protobuf(ProtobufEncoding {
                        descriptors,
                        message_name,
                        confluent_wire_format: _,
                    }) => protobuf::DecodedDescriptors::from_bytes(
                        descriptors,
                        message_name.to_owned(),
                    )?
                    .columns()
                    .iter()
                    .fold(RelationDesc::empty(), |desc, (name, ty)| {
                        desc.with_column(name, ty.clone())
                    }),
                    DataEncoding::Regex(RegexEncoding { regex }) => regex
                        .capture_names()
                        .enumerate()
                        // The first capture is the entire matched string. This will
                        // often not be useful, so skip it. If people want it they can
                        // just surround their entire regex in an explicit capture
                        // group.
                        .skip(1)
                        .fold(RelationDesc::empty(), |desc, (i, name)| {
                            let name = match name {
                                None => format!("column{}", i),
                                Some(name) => name.to_owned(),
                            };
                            let ty = ScalarType::String.nullable(true);
                            desc.with_column(name, ty)
                        }),
                    DataEncoding::Csv(CsvEncoding { columns, .. }) => match columns {
                        ColumnSpec::Count(n) => {
                            (1..=*n).into_iter().fold(RelationDesc::empty(), |desc, i| {
                                desc.with_column(
                                    format!("column{}", i),
                                    ScalarType::String.nullable(false),
                                )
                            })
                        }
                        ColumnSpec::Header { names } => names
                            .iter()
                            .map(|s| &**s)
                            .fold(RelationDesc::empty(), |desc, name| {
                                desc.with_column(name, ScalarType::String.nullable(false))
                            }),
                    },
                    DataEncoding::Text => RelationDesc::empty()
                        .with_column("text", ScalarType::String.nullable(false)),
                    DataEncoding::Postgres => RelationDesc::empty()
                        .with_column("oid", ScalarType::Int32.nullable(false))
                        .with_column(
                            "row_data",
                            ScalarType::List {
                                element_type: Box::new(ScalarType::String),
                                custom_id: None,
                            }
                            .nullable(false),
                        ),
                    DataEncoding::RowCodec(desc) => desc.clone(),
                })
            }

            pub fn op_name(&self) -> &'static str {
                match self {
                    DataEncoding::Bytes => "Bytes",
                    DataEncoding::Avro(_) => "Avro",
                    DataEncoding::Protobuf(_) => "Protobuf",
                    DataEncoding::Regex { .. } => "Regex",
                    DataEncoding::Csv(_) => "Csv",
                    DataEncoding::Text => "Text",
                    DataEncoding::Postgres => "Postgres",
                    DataEncoding::RowCodec(_) => "RowCodec",
                }
            }
        }

        /// Encoding in Avro format.
        #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
        pub struct AvroEncoding {
            pub schema: String,
            pub schema_registry_config: Option<mz_ccsr::ClientConfig>,
            pub confluent_wire_format: bool,
        }

        /// Encoding in Protobuf format.
        #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
        pub struct ProtobufEncoding {
            pub descriptors: Vec<u8>,
            pub message_name: String,
            pub confluent_wire_format: bool,
        }

        /// Arguments necessary to define how to decode from CSV format
        #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
        pub struct CsvEncoding {
            pub columns: ColumnSpec,
            pub delimiter: u8,
        }

        /// Determines the RelationDesc and decoding of CSV objects
        #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
        pub enum ColumnSpec {
            /// The first row is not a header row, and all columns get default names like `columnN`.
            Count(usize),
            /// The first row is a header row and therefore does become data
            ///
            /// Each of the values in `names` becomes the default name of a column in the dataflow.
            Header { names: Vec<String> },
        }

        impl ColumnSpec {
            /// The number of columns described by the column spec.
            pub fn arity(&self) -> usize {
                match self {
                    ColumnSpec::Count(n) => *n,
                    ColumnSpec::Header { names } => names.len(),
                }
            }

            pub fn into_header_names(self) -> Option<Vec<String>> {
                match self {
                    ColumnSpec::Count(_) => None,
                    ColumnSpec::Header { names } => Some(names),
                }
            }
        }

        #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
        pub struct RegexEncoding {
            pub regex: mz_repr::adt::regex::Regex,
        }
    }

    /// Universal language for describing message positions in Materialize, in a source independent
    /// way. Individual sources like Kafka or File sources should explicitly implement their own offset
    /// type that converts to/From MzOffsets. A 0-MzOffset denotes an empty stream.
    #[derive(
        Copy, Clone, Default, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize,
    )]
    pub struct MzOffset {
        pub offset: i64,
    }

    impl std::fmt::Display for MzOffset {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.offset)
        }
    }

    impl Add<i64> for MzOffset {
        type Output = MzOffset;

        fn add(self, x: i64) -> MzOffset {
            MzOffset {
                offset: self.offset + x,
            }
        }
    }

    impl Add<Self> for MzOffset {
        type Output = Self;

        fn add(self, x: Self) -> Self {
            MzOffset {
                offset: self.offset + x.offset,
            }
        }
    }

    impl AddAssign<i64> for MzOffset {
        fn add_assign(&mut self, x: i64) {
            self.offset += x;
        }
    }

    impl AddAssign<Self> for MzOffset {
        fn add_assign(&mut self, x: Self) {
            self.offset += x.offset;
        }
    }

    // Output is a diff but not itself an MzOffset
    impl Sub<Self> for MzOffset {
        type Output = i64;

        fn sub(self, other: Self) -> i64 {
            self.offset - other.offset
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    pub struct KafkaOffset {
        pub offset: i64,
    }

    /// Convert from KafkaOffset to MzOffset (1-indexed)
    impl From<KafkaOffset> for MzOffset {
        fn from(kafka_offset: KafkaOffset) -> Self {
            MzOffset {
                offset: kafka_offset.offset + 1,
            }
        }
    }

    /// Convert from MzOffset to Kafka::Offset as long as
    /// the offset is not negative
    impl Into<KafkaOffset> for MzOffset {
        fn into(self) -> KafkaOffset {
            KafkaOffset {
                offset: self.offset - 1,
            }
        }
    }

    /// Which piece of metadata a column corresponds to
    #[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum IncludedColumnSource {
        /// The materialize-specific notion of "position"
        ///
        /// This is legacy, and should be removed when default metadata is no longer included
        DefaultPosition,
        Partition,
        Offset,
        Timestamp,
        Topic,
        Headers,
    }

    /// Whether and how to include the decoded key of a stream in dataflows
    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum KeyEnvelope {
        /// Never include the key in the output row
        None,
        /// For composite key encodings, pull the fields from the encoding into columns.
        Flattened,
        /// Upsert is identical to Flattened but differs for non-avro sources, for which key names are overwritten.
        LegacyUpsert,
        /// Always use the given name for the key.
        ///
        /// * For a single-field key, this means that the column will get the given name.
        /// * For a multi-column key, the columns will get packed into a [`ScalarType::Record`], and
        ///   that Record will get the given name.
        Named(String),
    }

    /// A column that was created via an `INCLUDE` expression
    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct IncludedColumnPos {
        pub name: String,
        pub pos: usize,
    }

    /// The meaning of the timestamp number produced by data sources. This type
    /// is not concerned with the source of the timestamp (like if the data came
    /// from a Debezium consistency topic or a CDCv2 stream), instead only what the
    /// timestamp number means.
    ///
    /// Some variants here have attached data used to differentiate incomparable
    /// instantiations. These attached data types should be expanded in the future
    /// if we need to tell apart more kinds of sources.
    #[derive(Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Serialize, Deserialize, Hash)]
    pub enum Timeline {
        /// EpochMilliseconds means the timestamp is the number of milliseconds since
        /// the Unix epoch.
        EpochMilliseconds,
        /// External means the timestamp comes from an external data source and we
        /// don't know what the number means. The attached String is the source's name,
        /// which will result in different sources being incomparable.
        External(String),
        /// User means the user has manually specified a timeline. The attached
        /// String is specified by the user, allowing them to decide sources that are
        /// joinable.
        User(String),
    }

    /// `SourceEnvelope`s, describe how to turn a stream of messages from `SourceConnector`s,
    /// and turn them into a _differential stream_, that is, a stream of (data, time, diff)
    /// triples.
    ///
    /// Some sources (namely postgres and pubnub) skip any explicit envelope handling, effectively
    /// asserting that `SourceEnvelope` is `None` with `KeyEnvelope::None`.
    // TODO(guswynn): update this ^ when SimpleSource is gone.
    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub enum SourceEnvelope {
        /// The most trivial version is `None`, which typically produces triples where the diff
        /// is ALWAYS `1`
        ///
        /// If the `KeyEnvelope` is present,
        /// include the key columns as an output column of the source with the given properties.
        // TODO(guswynn): update `None` docs to describe `-1` diff case, when support for it is
        // added for postgres sources.
        None(KeyEnvelope),
        /// `Debezium` avoids holding onto previously seen values by trusting the required
        /// `before` and `after` value fields coming from the upstream source.
        Debezium(DebeziumEnvelope),
        /// `Upsert` holds onto previously seen values and produces `1` or `-1` diffs depending on
        /// whether or not the required _key_ outputed by the source has been seen before. This also
        /// supports a `Debezium` mode.
        Upsert(UpsertEnvelope),
        /// `CdcV2` requires sources output messages in a strict form that requires a upstream-provided
        /// timeline.
        CdcV2,
        /// An envelope for sources that directly read differential Rows. This is internal and
        /// cannot be requested via SQL.
        DifferentialRow,
    }

    /// `UnplannedSourceEnvelope` is a `SourceEnvelope` missing some information. This information
    /// is obtained in `UnplannedSourceEnvelope::desc`, where
    /// `UnplannedSourceEnvelope::into_source_envelope`
    /// creates a full `SourceEnvelope`
    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub enum UnplannedSourceEnvelope {
        None(KeyEnvelope),
        Debezium(DebeziumEnvelope),
        Upsert(UpsertStyle),
        CdcV2,
        /// An envelope for sources that directly read differential Rows. This is internal and
        /// cannot be requested via SQL.
        DifferentialRow,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub struct UpsertEnvelope {
        /// What style of Upsert we are using
        pub style: UpsertStyle,
        /// The indices of the keys in the full value row, used
        /// to deduplicate data in `upsert_core`
        pub key_indices: Vec<usize>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub enum UpsertStyle {
        /// `ENVELOPE UPSERT`, where the key shape depends on the independent
        /// `KeyEnvelope`
        Default(KeyEnvelope),
        /// `ENVELOPE DEBEZIUM UPSERT`
        Debezium { after_idx: usize },
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub struct DebeziumEnvelope {
        /// The column index containing the `before` row
        pub before_idx: usize,
        /// The column index containing the `after` row
        pub after_idx: usize,
        pub mode: DebeziumMode,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub struct DebeziumTransactionMetadata {
        pub tx_metadata_global_id: GlobalId,
        pub tx_status_idx: usize,
        pub tx_transaction_id_idx: usize,
        pub tx_data_collections_idx: usize,
        pub tx_data_collections_data_collection_idx: usize,
        pub tx_data_collections_event_count_idx: usize,
        pub tx_data_collection_name: String,
        pub data_transaction_id_idx: usize,
    }

    /// Ordered means we can trust Debezium high water marks
    ///
    /// In standard operation, Debezium should always emit messages in position order, but
    /// messages may be duplicated.
    ///
    /// For example, this is a legal stream of Debezium event positions:
    ///
    /// ```text
    /// 1 2 3 2
    /// ```
    ///
    /// Note that `2` appears twice, but the *first* time it appeared it appeared in order.
    /// Any position below the highest-ever seen position is guaranteed to be a duplicate,
    /// and can be ignored.
    ///
    /// Now consider this stream:
    ///
    /// ```text
    /// 1 3 2
    /// ```
    ///
    /// In this case, `2` is sent *out* of order, and if it is ignored we will miss important
    /// state.
    ///
    /// It is possible for users to do things with multiple databases and multiple Debezium
    /// instances pointing at the same Kafka topic that mean that the Debezium guarantees do
    /// not hold, in which case we are required to track individual messages, instead of just
    /// the highest-ever-seen message.
    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub enum DebeziumMode {
        /// Do not perform any deduplication
        None,
        /// We can trust high water mark
        Ordered(DebeziumDedupProjection),
        /// We need to store some piece of state for every message
        Full(DebeziumDedupProjection),
        FullInRange {
            projection: DebeziumDedupProjection,
            pad_start: Option<NaiveDateTime>,
            start: NaiveDateTime,
            end: NaiveDateTime,
        },
    }

    impl DebeziumMode {
        pub fn tx_metadata(&self) -> Option<&DebeziumTransactionMetadata> {
            match self {
                DebeziumMode::Ordered(DebeziumDedupProjection { tx_metadata, .. })
                | DebeziumMode::Full(DebeziumDedupProjection { tx_metadata, .. })
                | DebeziumMode::FullInRange {
                    projection: DebeziumDedupProjection { tx_metadata, .. },
                    ..
                } => tx_metadata.as_ref(),
                DebeziumMode::None => None,
            }
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub struct DebeziumDedupProjection {
        /// The column index containing the debezium source metadata
        pub source_idx: usize,
        /// The record index of the `source.snapshot` field
        pub snapshot_idx: usize,
        /// The upstream database specific fields
        pub source_projection: DebeziumSourceProjection,
        /// The column index containing the debezium transaction metadata
        pub transaction_idx: usize,
        /// The record index of the `transaction.total_order` field
        pub total_order_idx: usize,
        pub tx_metadata: Option<DebeziumTransactionMetadata>,
    }

    /// Debezium generates records that contain metadata about the upstream database. The structure of
    /// this metadata depends on the type of connector used. This struct records the relevant indices
    /// in the record, calculated during planning, so that the dataflow operator can unpack the
    /// structure and extract the relevant information.
    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub enum DebeziumSourceProjection {
        MySql {
            file: usize,
            pos: usize,
            row: usize,
        },
        Postgres {
            sequence: Option<usize>,
            lsn: usize,
        },
        SqlServer {
            change_lsn: usize,
            event_serial_no: usize,
        },
    }

    /// Computes the indices of the value's relation description that appear in the key.
    ///
    /// Returns an error if it detects a common columns between the two relations that has the same
    /// name but a different type, if a key column is missing from the value, and if the key relation
    /// has a column with no name.
    fn match_key_indices(
        key_desc: &RelationDesc,
        value_desc: &RelationDesc,
    ) -> anyhow::Result<Vec<usize>> {
        let mut indices = Vec::new();
        for (name, key_type) in key_desc.iter() {
            let (index, value_type) = value_desc
                .get_by_name(name)
                .ok_or_else(|| anyhow!("Value schema missing primary key column: {}", name))?;

            if key_type == value_type {
                indices.push(index);
            } else {
                bail!(
                    "key and value column types do not match: key {:?} vs. value {:?}",
                    key_type,
                    value_type
                );
            }
        }
        Ok(indices)
    }

    impl UnplannedSourceEnvelope {
        /// Transforms an `UnplannedSourceEnvelope` into a `SourceEnvelope`
        ///
        /// Panics if the input envelope is `UnplannedSourceEnvelope::Upsert` and
        /// key is not passed as `Some`
        fn into_source_envelope(self, key: Option<Vec<usize>>) -> SourceEnvelope {
            match self {
                UnplannedSourceEnvelope::Upsert(upsert_style) => {
                    SourceEnvelope::Upsert(UpsertEnvelope {
                        style: upsert_style,
                        key_indices: key.expect("into_source_envelope to be passed correct parameters for UnplannedSourceEnvelope::Upsert"),
                    })
                },
                UnplannedSourceEnvelope::Debezium(inner) => {
                    SourceEnvelope::Debezium(inner)
                }
                UnplannedSourceEnvelope::None(inner) => SourceEnvelope::None(inner),
                UnplannedSourceEnvelope::CdcV2 => SourceEnvelope::CdcV2,
                UnplannedSourceEnvelope::DifferentialRow => SourceEnvelope::DifferentialRow,
            }
        }

        /// Computes the output relation of this envelope when applied on top of the decoded key and
        /// value relation desc
        pub fn desc(
            self,
            key_desc: Option<RelationDesc>,
            value_desc: RelationDesc,
            metadata_desc: RelationDesc,
        ) -> anyhow::Result<(SourceEnvelope, RelationDesc)> {
            Ok(match &self {
                UnplannedSourceEnvelope::None(key_envelope)
                | UnplannedSourceEnvelope::Upsert(UpsertStyle::Default(key_envelope)) => {
                    let key_desc = match key_desc {
                        Some(desc) => desc,
                        None => {
                            return Ok((
                                self.into_source_envelope(None),
                                value_desc.concat(metadata_desc),
                            ))
                        }
                    };

                    let (keyed, key) = match key_envelope {
                        KeyEnvelope::None => (value_desc, None),
                        KeyEnvelope::Flattened => {
                            // Add the key columns as a key.
                            let key_indices: Vec<usize> = (0..key_desc.arity()).collect();
                            let key_desc = key_desc.with_key(key_indices.clone());
                            (key_desc.concat(value_desc), Some(key_indices))
                        }
                        KeyEnvelope::LegacyUpsert => {
                            let key_indices: Vec<usize> = (0..key_desc.arity()).collect();
                            let key_desc = key_desc.with_key(key_indices.clone());
                            let names = (0..key_desc.arity()).map(|i| format!("key{}", i));
                            // Rename key columns to "keyN"
                            (
                                key_desc.with_names(names).concat(value_desc),
                                Some(key_indices),
                            )
                        }
                        KeyEnvelope::Named(key_name) => {
                            let key_desc = {
                                // if the key has multiple objects, nest them as a record inside of a single name
                                if key_desc.arity() > 1 {
                                    let key_type = key_desc.typ();
                                    let key_as_record = RelationType::new(vec![ColumnType {
                                        nullable: false,
                                        scalar_type: ScalarType::Record {
                                            fields: key_desc
                                                .iter_names()
                                                .zip(key_type.column_types.iter())
                                                .map(|(name, ty)| (name.clone(), ty.clone()))
                                                .collect(),
                                            custom_id: None,
                                        },
                                    }]);

                                    RelationDesc::new(key_as_record, [key_name.to_string()])
                                } else {
                                    key_desc.with_names([key_name.to_string()])
                                }
                            };
                            // In all cases the first column is the key
                            (key_desc.with_key(vec![0]).concat(value_desc), Some(vec![0]))
                        }
                    };
                    (self.into_source_envelope(key), keyed.concat(metadata_desc))
                }
                UnplannedSourceEnvelope::Debezium(DebeziumEnvelope { after_idx, .. })
                | UnplannedSourceEnvelope::Upsert(UpsertStyle::Debezium { after_idx }) => {
                    match &value_desc.typ().column_types[*after_idx].scalar_type {
                        ScalarType::Record { fields, .. } => {
                            let mut desc = RelationDesc::from_names_and_types(fields.clone());
                            let key = key_desc.map(|k| match_key_indices(&k, &desc)).transpose()?;
                            if let Some(key) = key.clone() {
                                desc = desc.with_key(key);
                            }

                            let desc = match self {
                                UnplannedSourceEnvelope::Upsert(_) => desc.concat(metadata_desc),
                                _ => desc,
                            };

                            (self.into_source_envelope(key), desc)
                        }
                        ty => bail!(
                            "Incorrect type for Debezium value, expected Record, got {:?}",
                            ty
                        ),
                    }
                }
                UnplannedSourceEnvelope::CdcV2 => {
                    // the correct types

                    // CdcV2 row data are in a record in a record in a list
                    match &value_desc.typ().column_types[0].scalar_type {
                        ScalarType::List { element_type, .. } => match &**element_type {
                            ScalarType::Record { fields, .. } => {
                                // TODO maybe check this by name
                                match &fields[0].1.scalar_type {
                                    ScalarType::Record { fields, .. } => (
                                        self.into_source_envelope(None),
                                        RelationDesc::from_names_and_types(fields.clone()),
                                    ),
                                    ty => {
                                        bail!("Unexpected type for MATERIALIZE envelope: {:?}", ty)
                                    }
                                }
                            }
                            ty => bail!("Unexpected type for MATERIALIZE envelope: {:?}", ty),
                        },
                        ty => bail!("Unexpected type for MATERIALIZE envelope: {:?}", ty),
                    }
                }
                UnplannedSourceEnvelope::DifferentialRow => (
                    self.into_source_envelope(None),
                    value_desc.concat(metadata_desc),
                ),
            })
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct KafkaSourceConnector {
        pub addrs: KafkaAddrs,
        pub topic: String,
        // Represents options specified by user when creating the source, e.g.
        // security settings.
        pub config_options: BTreeMap<String, String>,
        // Map from partition -> starting offset
        pub start_offsets: HashMap<i32, i64>,
        pub group_id_prefix: Option<String>,
        pub cluster_id: Uuid,
        /// If present, include the timestamp as an output column of the source with the given name
        pub include_timestamp: Option<IncludedColumnPos>,
        /// If present, include the partition as an output column of the source with the given name.
        pub include_partition: Option<IncludedColumnPos>,
        /// If present, include the topic as an output column of the source with the given name.
        pub include_topic: Option<IncludedColumnPos>,
        /// If present, include the offset as an output column of the source with the given name.
        pub include_offset: Option<IncludedColumnPos>,
        pub include_headers: Option<IncludedColumnPos>,
    }

    /// Legacy logic included something like an offset into almost data streams
    ///
    /// Eventually we will require `INCLUDE <metadata>` for everything.
    pub fn provide_default_metadata(
        envelope: &UnplannedSourceEnvelope,
        encoding: &encoding::DataEncoding,
    ) -> bool {
        let is_avro = matches!(encoding, encoding::DataEncoding::Avro(_));
        let is_stateless_dbz = match envelope {
            UnplannedSourceEnvelope::Debezium(_) => true,
            _ => false,
        };

        !is_avro && !is_stateless_dbz
    }

    #[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
    pub enum ConnectorInner {
        Kafka {
            broker: KafkaAddrs,
            config_options: BTreeMap<String, String>,
        },
        CSR {
            registry: String,
            with_options: BTreeMap<String, String>,
        },
    }

    impl ConnectorInner {
        pub fn uri(&self) -> String {
            match self {
                ConnectorInner::Kafka { broker, .. } => broker.to_string(),
                ConnectorInner::CSR { registry, .. } => registry.to_owned(),
            }
        }

        pub fn options(&self) -> BTreeMap<String, String> {
            match self {
                ConnectorInner::Kafka { config_options, .. } => config_options.clone(),
                ConnectorInner::CSR { with_options, .. } => with_options.clone(),
            }
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum Compression {
        Gzip,
        None,
    }

    /// A source of updates for a relational collection.
    ///
    /// A source contains enough information to instantiate a stream of changes,
    /// as well as related metadata about the columns, their types, and properties
    /// of the collection.
    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub struct SourceDesc {
        pub connector: SourceConnector,
        pub desc: RelationDesc,
    }

    /// A stub for generating arbitrary [SourceDesc].
    /// Currently only produces the simplest instance of one.
    pub(super) fn any_source_desc_stub() -> SourceDesc {
        SourceDesc {
            connector: SourceConnector::Local {
                timeline: Timeline::EpochMilliseconds,
            },
            desc: RelationDesc::empty(),
        }
    }

    /// A `SourceConnector` describes how data is produced for a source, be
    /// it from a local table, or some upstream service. It is the first
    /// step of _rendering_ of a source, and describes only how to produce
    /// a stream of messages associated with MzOffset's.
    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub enum SourceConnector {
        External {
            connector: ExternalSourceConnector,
            encoding: encoding::SourceDataEncoding,
            envelope: SourceEnvelope,
            metadata_columns: Vec<IncludedColumnSource>,
            ts_frequency: Duration,
            timeline: Timeline,
        },

        /// A local "source" is fed by a local input handle.
        Local { timeline: Timeline },
    }

    impl SourceConnector {
        /// Returns `true` if this connector yields input data (including
        /// timestamps) that is stable across restarts. This is important for
        /// exactly-once Sinks that need to ensure that the same data is written,
        /// even when failures/restarts happen.
        pub fn yields_stable_input(&self) -> bool {
            if let SourceConnector::External { connector, .. } = self {
                // Conservatively, set all Kafka/File sources as having stable inputs because
                // we know they will be read in a known, repeatable offset order (modulo compaction for some Kafka sources).
                match connector {
                    // TODO(guswynn): does postgres count here as well?
                    ExternalSourceConnector::Kafka(_) => true,
                    // Currently, the Kinesis connector assigns "offsets" by counting the message in the order it was received
                    // and this order is not replayable across different reads of the same Kinesis stream.
                    ExternalSourceConnector::Kinesis(_) => false,
                    _ => false,
                }
            } else {
                false
            }
        }

        pub fn name(&self) -> &'static str {
            match self {
                SourceConnector::External { connector, .. } => connector.name(),
                SourceConnector::Local { .. } => "local",
            }
        }

        pub fn timeline(&self) -> Timeline {
            match self {
                SourceConnector::External { timeline, .. } => timeline.clone(),
                SourceConnector::Local { timeline, .. } => timeline.clone(),
            }
        }

        pub fn requires_single_materialization(&self) -> bool {
            if let SourceConnector::External { connector, .. } = self {
                connector.requires_single_materialization()
            } else {
                false
            }
        }

        /// Returns a `ReadHandle` that can be used to read from the persist shard that a rendered
        /// version of this connector would write to or if the input data is available as a persist
        /// shard. Returns `None` if this type of connector doesn't write to persist.
        pub async fn get_read_handle<T: Timestamp + Lattice + Codec64>(
            &self,
        ) -> Result<Option<ReadHandle<Row, Row, T, mz_repr::Diff>>, anyhow::Error> {
            let result = match self {
                SourceConnector::External {
                    connector: ExternalSourceConnector::Persist(persist_connector),
                    ..
                } => {
                    let location = PersistLocation {
                        blob_uri: persist_connector.blob_uri.clone(),
                        consensus_uri: persist_connector.consensus_uri.clone(),
                    };

                    let persist_client = location.open().await?;

                    let (_write, read) = persist_client
                        .open::<Row, Row, T, mz_repr::Diff>(persist_connector.shard_id)
                        .await?;

                    Some(read)
                }
                SourceConnector::External { .. } => None,
                SourceConnector::Local { .. } => None,
            };

            Ok(result)
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum ExternalSourceConnector {
        Kafka(KafkaSourceConnector),
        Kinesis(KinesisSourceConnector),
        S3(S3SourceConnector),
        Postgres(PostgresSourceConnector),
        PubNub(PubNubSourceConnector),
        Persist(PersistSourceConnector),
    }

    impl ExternalSourceConnector {
        /// Returns the name and type of each additional metadata column that
        /// Materialize will automatically append to the source's inherent columns.
        ///
        /// Presently, each source type exposes precisely one metadata column that
        /// corresponds to some source-specific record counter. For example, file
        /// sources use a line number, while Kafka sources use a topic offset.
        ///
        /// The columns declared here must be kept in sync with the actual source
        /// implementations that produce these columns.
        pub fn metadata_columns(&self, include_defaults: bool) -> Vec<(&str, ColumnType)> {
            let mut columns = Vec::new();
            let default_col = |name| (name, ScalarType::Int64.nullable(false));
            match self {
                Self::Kafka(KafkaSourceConnector {
                    include_partition: part,
                    include_timestamp: time,
                    include_topic: topic,
                    include_offset: offset,
                    include_headers: headers,
                    ..
                }) => {
                    let mut items = BTreeMap::new();
                    // put the offset at the end if necessary
                    if include_defaults && offset.is_none() {
                        items.insert(4, default_col("mz_offset"));
                    }

                    for (include, ty) in [
                        (offset, ScalarType::Int64),
                        (part, ScalarType::Int32),
                        (time, ScalarType::Timestamp),
                        (topic, ScalarType::String),
                        (
                            headers,
                            ScalarType::List {
                                element_type: Box::new(ScalarType::Record {
                                    fields: vec![
                                        (
                                            "key".into(),
                                            ColumnType {
                                                nullable: false,
                                                scalar_type: ScalarType::String,
                                            },
                                        ),
                                        (
                                            "value".into(),
                                            ColumnType {
                                                nullable: false,
                                                scalar_type: ScalarType::Bytes,
                                            },
                                        ),
                                    ],
                                    custom_id: None,
                                }),
                                custom_id: None,
                            },
                        ),
                    ] {
                        if let Some(include) = include {
                            items.insert(include.pos + 1, (&include.name, ty.nullable(false)));
                        }
                    }

                    items.into_values().collect()
                }
                Self::Kinesis(_) => {
                    if include_defaults {
                        columns.push(default_col("mz_offset"))
                    };
                    columns
                }
                // TODO: should we include object key and possibly object-internal offset here?
                Self::S3(_) => {
                    if include_defaults {
                        columns.push(default_col("mz_record"))
                    };
                    columns
                }
                Self::Postgres(_) => vec![],
                Self::PubNub(_) => vec![],
                Self::Persist(_) => vec![],
            }
        }

        // TODO(bwm): get rid of this when we no longer have the notion of default metadata
        pub fn default_metadata_column_name(&self) -> Option<&str> {
            match self {
                ExternalSourceConnector::Kafka(_) => Some("mz_offset"),
                ExternalSourceConnector::Kinesis(_) => Some("mz_offset"),
                ExternalSourceConnector::S3(_) => Some("mz_record"),
                ExternalSourceConnector::Postgres(_) => None,
                ExternalSourceConnector::PubNub(_) => None,
                ExternalSourceConnector::Persist(_) => None,
            }
        }

        pub fn metadata_column_types(&self, include_defaults: bool) -> Vec<IncludedColumnSource> {
            match self {
                ExternalSourceConnector::Kafka(KafkaSourceConnector {
                    include_partition: part,
                    include_timestamp: time,
                    include_topic: topic,
                    include_offset: offset,
                    include_headers: headers,
                    ..
                }) => {
                    // create a sorted list of column types based on the order they were declared in sql
                    // TODO: should key be included in the sorted list? Breaking change, and it's
                    // already special (it commonly multiple columns embedded in it).
                    let mut items = BTreeMap::new();
                    if include_defaults && offset.is_none() {
                        items.insert(4, IncludedColumnSource::DefaultPosition);
                    }
                    for (include, ty) in [
                        (offset, IncludedColumnSource::Offset),
                        (part, IncludedColumnSource::Partition),
                        (time, IncludedColumnSource::Timestamp),
                        (topic, IncludedColumnSource::Topic),
                        (headers, IncludedColumnSource::Headers),
                    ] {
                        if let Some(include) = include {
                            items.insert(include.pos, ty);
                        }
                    }

                    items.into_values().collect()
                }

                ExternalSourceConnector::Kinesis(_) | ExternalSourceConnector::S3(_) => {
                    if include_defaults {
                        vec![IncludedColumnSource::DefaultPosition]
                    } else {
                        Vec::new()
                    }
                }
                ExternalSourceConnector::Postgres(_)
                | ExternalSourceConnector::PubNub(_)
                | ExternalSourceConnector::Persist(_) => Vec::new(),
            }
        }

        /// Returns the name of the external source connector.
        pub fn name(&self) -> &'static str {
            match self {
                ExternalSourceConnector::Kafka(_) => "kafka",
                ExternalSourceConnector::Kinesis(_) => "kinesis",
                ExternalSourceConnector::S3(_) => "s3",
                ExternalSourceConnector::Postgres(_) => "postgres",
                ExternalSourceConnector::PubNub(_) => "pubnub",
                ExternalSourceConnector::Persist(_) => "persist",
            }
        }

        /// Optionally returns the name of the upstream resource this source corresponds to.
        /// (Currently only implemented for Kafka and Kinesis, to match old-style behavior
        ///  TODO: decide whether we want file paths and other upstream names to show up in metrics too.
        pub fn upstream_name(&self) -> Option<&str> {
            match self {
                ExternalSourceConnector::Kafka(KafkaSourceConnector { topic, .. }) => {
                    Some(topic.as_str())
                }
                ExternalSourceConnector::Kinesis(KinesisSourceConnector {
                    stream_name, ..
                }) => Some(stream_name.as_str()),
                ExternalSourceConnector::S3(_) => None,
                ExternalSourceConnector::Postgres(_) => None,
                ExternalSourceConnector::PubNub(_) => None,
                ExternalSourceConnector::Persist(_) => None,
            }
        }

        pub fn requires_single_materialization(&self) -> bool {
            match self {
                ExternalSourceConnector::S3(c) => c.requires_single_materialization(),
                ExternalSourceConnector::Postgres(_) => true,

                ExternalSourceConnector::Kafka(_)
                | ExternalSourceConnector::Kinesis(_)
                | ExternalSourceConnector::PubNub(_)
                | ExternalSourceConnector::Persist(_) => false,
            }
        }
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct KinesisSourceConnector {
        pub stream_name: String,
        pub aws: AwsConfig,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PostgresSourceConnector {
        pub conn: String,
        pub publication: String,
        pub slot_name: String,
        pub details: PostgresSourceDetails,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PubNubSourceConnector {
        pub subscribe_key: String,
        pub channel: String,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PersistSourceConnector {
        pub consensus_uri: String,
        pub blob_uri: String,
        pub shard_id: ShardId,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct S3SourceConnector {
        pub key_sources: Vec<S3KeySource>,
        pub pattern: Option<Glob>,
        pub aws: AwsConfig,
        pub compression: Compression,
    }

    impl S3SourceConnector {
        fn requires_single_materialization(&self) -> bool {
            // SQS Notifications are not durable, multiple sources depending on them will get
            // non-intersecting subsets of objects to read
            self.key_sources
                .iter()
                .any(|s| matches!(s, S3KeySource::SqsNotifications { .. }))
        }
    }

    /// A Source of Object Key names, the argument of the `DISCOVER OBJECTS` clause
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub enum S3KeySource {
        /// Scan the S3 Bucket to discover keys to download
        Scan { bucket: String },
        /// Load object keys based on the contents of an S3 Notifications channel
        ///
        /// S3 notifications channels can be configured to go to SQS, which is the
        /// only target we currently support.
        SqsNotifications { queue: String },
    }

    /// A wrapper for [`Uri`] that implements [`Serialize`] and `Deserialize`.
    #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
    pub struct SerdeUri(#[serde(with = "http_serde::uri")] pub Uri);

    /// AWS configuration overrides for a source or sink.
    ///
    /// This is a distinct type from any of the configuration types built into the
    /// AWS SDK so that we can implement `Serialize` and `Deserialize`.
    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct AwsConfig {
        /// AWS Credentials, or where to find them
        pub credentials: AwsCredentials,
        /// The AWS region to use.
        ///
        /// Uses the default region (looking at env vars, config files, etc) if not provided.
        pub region: Option<String>,
        /// The AWS role to assume.
        pub role: Option<AwsAssumeRole>,
        /// The custom AWS endpoint to use, if any.
        pub endpoint: Option<SerdeUri>,
    }

    /// AWS credentials for a source or sink.
    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum AwsCredentials {
        /// Look for credentials using the [default credentials chain][credchain]
        ///
        /// [credchain]: aws_config::default_provider::credentials::DefaultCredentialsChain
        Default,
        /// Load credentials using the given named profile
        Profile { profile_name: String },
        /// Use the enclosed static credentials
        Static {
            access_key_id: String,
            secret_access_key: String,
            session_token: Option<String>,
        },
    }

    /// A role for Materialize to assume when performing AWS API calls.
    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct AwsAssumeRole {
        /// The Amazon Resource Name of the role to assume.
        pub arn: String,
    }

    /// An external ID to use for all AWS AssumeRole operations.
    ///
    /// Note that it is critical for security that this ID can **not** be provided by users running
    /// in Materialize Cloud, it must be provided by Materialize. Currently this guarantee is
    /// satisfied by only making this accessible from the CLI, which users do not have access to.
    ///
    /// <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum AwsExternalId {
        NotProvided,
        ISwearThisCameFromACliArgOrEnvVariable(String),
    }

    impl Default for AwsExternalId {
        fn default() -> Self {
            AwsExternalId::NotProvided
        }
    }

    impl AwsExternalId {
        fn get(&self) -> Option<&str> {
            match self {
                AwsExternalId::NotProvided => None,
                AwsExternalId::ISwearThisCameFromACliArgOrEnvVariable(v) => Some(v),
            }
        }
    }

    impl AwsConfig {
        /// Loads the AWS SDK configuration object from the environment, then
        /// applies the overrides from this object.
        pub async fn load(&self, external_id: AwsExternalId) -> aws_types::SdkConfig {
            use aws_config::default_provider::credentials::DefaultCredentialsChain;
            use aws_config::default_provider::region::DefaultRegionChain;
            use aws_config::sts::AssumeRoleProvider;
            use aws_smithy_http::endpoint::Endpoint;
            use aws_types::credentials::SharedCredentialsProvider;
            use aws_types::region::Region;

            let region = match &self.region {
                Some(region) => Some(Region::new(region.clone())),
                _ => {
                    let mut rc = DefaultRegionChain::builder();
                    if let AwsCredentials::Profile { profile_name } = &self.credentials {
                        rc = rc.profile_name(profile_name);
                    }
                    rc.build().region().await
                }
            };

            let mut cred_provider = match &self.credentials {
                AwsCredentials::Default => SharedCredentialsProvider::new(
                    DefaultCredentialsChain::builder()
                        .region(region.clone())
                        .build()
                        .await,
                ),
                AwsCredentials::Profile { profile_name } => SharedCredentialsProvider::new(
                    DefaultCredentialsChain::builder()
                        .profile_name(profile_name)
                        .region(region.clone())
                        .build()
                        .await,
                ),
                AwsCredentials::Static {
                    access_key_id,
                    secret_access_key,
                    session_token,
                } => SharedCredentialsProvider::new(aws_types::Credentials::from_keys(
                    access_key_id,
                    secret_access_key,
                    session_token.clone(),
                )),
            };

            if let Some(AwsAssumeRole { arn }) = &self.role {
                let mut role = AssumeRoleProvider::builder(arn).session_name("materialized");
                // This affects which region to perform STS on, not where
                // anything else happens.
                if let Some(region) = &region {
                    role = role.region(region.clone());
                }
                if let Some(external_id) = external_id.get() {
                    role = role.external_id(external_id);
                }
                cred_provider = SharedCredentialsProvider::new(role.build(cred_provider));
            }

            let mut loader = aws_config::from_env()
                .region(region)
                .credentials_provider(cred_provider);
            if let Some(endpoint) = &self.endpoint {
                loader = loader.endpoint_resolver(Endpoint::immutable(endpoint.0.clone()));
            }
            loader.load().await
        }
    }

    #[derive(Debug, Clone)]
    pub struct SourceData(pub Result<Row, DataflowError>);

    impl Deref for SourceData {
        type Target = Result<Row, DataflowError>;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl DerefMut for SourceData {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.0
        }
    }

    impl From<&SourceData> for ProtoSourceData {
        fn from(x: &SourceData) -> Self {
            use proto_source_data::Kind;
            ProtoSourceData {
                kind: Some(match &**x {
                    Ok(row) => Kind::Ok(row.into()),
                    Err(err) => Kind::Err(err.into()),
                }),
            }
        }
    }

    impl TryFrom<ProtoSourceData> for SourceData {
        type Error = TryFromProtoError;

        fn try_from(data: ProtoSourceData) -> Result<Self, Self::Error> {
            use proto_source_data::Kind;
            match data.kind {
                Some(kind) => match kind {
                    Kind::Ok(row) => Ok(SourceData(Ok(row.try_into()?))),
                    Kind::Err(err) => Ok(SourceData(Err(err.try_into()?))),
                },
                None => Result::Err(TryFromProtoError::missing_field("ProtoSourceData::kind")),
            }
        }
    }

    impl Codec for SourceData {
        fn codec_name() -> String {
            "protobuf[SourceData]".into()
        }

        fn encode<B: BufMut>(&self, buf: &mut B) {
            ProtoSourceData::from(self)
                .encode(buf)
                .expect("no required fields means no initialization errors");
        }

        fn decode(buf: &[u8]) -> Result<Self, String> {
            let proto = ProtoSourceData::decode(buf).map_err(|err| err.to_string())?;
            Self::try_from(proto).map_err(|err| err.to_string())
        }
    }
}

/// Types and traits related to reporting changing collections out of `dataflow`.
pub mod sinks {

    use std::collections::BTreeMap;
    use std::time::Duration;

    use serde::{Deserialize, Serialize};
    use timely::progress::frontier::Antichain;
    use url::Url;

    use mz_kafka_util::KafkaAddrs;
    use mz_persist_client::ShardId;
    use mz_repr::{GlobalId, RelationDesc};

    /// A sink for updates to a relational collection.
    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub struct SinkDesc<T = mz_repr::Timestamp> {
        pub from: GlobalId,
        pub from_desc: RelationDesc,
        pub connector: SinkConnector,
        pub envelope: Option<SinkEnvelope>,
        pub as_of: SinkAsOf<T>,
    }

    /// A stub for generating arbitrary [SinkDesc].
    /// Currently only produces the simplest instance of one.
    pub(super) fn any_sink_desc_stub() -> SinkDesc<mz_repr::Timestamp> {
        SinkDesc {
            from: GlobalId::Explain,
            from_desc: RelationDesc::empty(),
            connector: SinkConnector::Tail(TailSinkConnector {}),
            envelope: None,
            as_of: SinkAsOf {
                frontier: Antichain::new(),
                strict: false,
            },
        }
    }

    #[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum SinkEnvelope {
        Debezium,
        Upsert,
        /// An envelope for sinks that directly write differential Rows. This is internal and
        /// cannot be requested via SQL.
        DifferentialRow,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct SinkAsOf<T = mz_repr::Timestamp> {
        pub frontier: Antichain<T>,
        pub strict: bool,
    }

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub enum SinkConnector {
        Kafka(KafkaSinkConnector),
        Tail(TailSinkConnector),
        Persist(PersistSinkConnector),
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct KafkaSinkConsistencyConnector {
        pub topic: String,
        pub schema_id: i32,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct KafkaSinkConnector {
        pub addrs: KafkaAddrs,
        pub topic: String,
        pub topic_prefix: String,
        pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
        pub relation_key_indices: Option<Vec<usize>>,
        pub value_desc: RelationDesc,
        pub published_schema_info: Option<PublishedSchemaInfo>,
        pub consistency: Option<KafkaSinkConsistencyConnector>,
        pub exactly_once: bool,
        // Source dependencies for exactly-once sinks.
        pub transitive_source_dependencies: Vec<GlobalId>,
        // Maximum number of records the sink will attempt to send each time it is
        // invoked
        pub fuel: usize,
        pub config_options: BTreeMap<String, String>,
    }

    /// TODO(JLDLaughlin): Documentation.
    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PublishedSchemaInfo {
        pub key_schema_id: Option<i32>,
        pub value_schema_id: i32,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PersistSinkConnector {
        pub value_desc: RelationDesc,
        pub shard_id: ShardId,
        pub consensus_uri: String,
        pub blob_uri: String,
    }

    impl SinkConnector {
        /// Returns the name of the sink connector.
        pub fn name(&self) -> &'static str {
            match self {
                SinkConnector::Kafka(_) => "kafka",
                SinkConnector::Tail(_) => "tail",
                SinkConnector::Persist(_) => "persist",
            }
        }

        /// Returns `true` if this sink requires sources to block timestamp binding
        /// compaction until all sinks that depend on a given source have finished
        /// writing out that timestamp.
        ///
        /// To achieve that, each sink will hold a `AntichainToken` for all of
        /// the sources it depends on, and will advance all of its source
        /// dependencies' compaction frontiers as it completes writes.
        ///
        /// Sinks that do need to hold back compaction need to insert an
        /// [`Antichain`] into `StorageState::sink_write_frontiers` that they update
        /// in order to advance the frontier that holds back upstream compaction
        /// of timestamp bindings.
        ///
        /// See also [`transitive_source_dependencies`](SinkConnector::transitive_source_dependencies).
        pub fn requires_source_compaction_holdback(&self) -> bool {
            match self {
                SinkConnector::Kafka(k) => k.exactly_once,
                SinkConnector::Tail(_) => false,
                SinkConnector::Persist(_) => false,
            }
        }

        /// Returns the [`GlobalIds`](GlobalId) of the transitive sources of this
        /// sink.
        pub fn transitive_source_dependencies(&self) -> &[GlobalId] {
            match self {
                SinkConnector::Kafka(k) => &k.transitive_source_dependencies,
                SinkConnector::Tail(_) => &[],
                SinkConnector::Persist(_) => &[],
            }
        }
    }

    #[derive(Default, Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
    pub struct TailSinkConnector {}

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum SinkConnectorBuilder {
        Kafka(KafkaSinkConnectorBuilder),
        Persist(PersistSinkConnectorBuilder),
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct PersistSinkConnectorBuilder {
        pub consensus_uri: String,
        pub blob_uri: String,
        pub shard_id: ShardId,
        pub value_desc: RelationDesc,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub struct KafkaSinkConnectorBuilder {
        pub broker_addrs: KafkaAddrs,
        pub format: KafkaSinkFormat,
        /// A natural key of the sinked relation (view or source).
        pub relation_key_indices: Option<Vec<usize>>,
        /// The user-specified key for the sink.
        pub key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
        pub value_desc: RelationDesc,
        pub topic_prefix: String,
        pub consistency_topic_prefix: Option<String>,
        pub consistency_format: Option<KafkaSinkFormat>,
        pub topic_suffix_nonce: String,
        pub partition_count: i32,
        pub replication_factor: i32,
        pub fuel: usize,
        pub config_options: BTreeMap<String, String>,
        // Forces the sink to always write to the same topic across restarts instead
        // of picking a new topic each time.
        pub reuse_topic: bool,
        // Source dependencies for exactly-once sinks.
        pub transitive_source_dependencies: Vec<GlobalId>,
        pub retention: KafkaSinkConnectorRetention,
    }

    #[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
    pub struct KafkaSinkConnectorRetention {
        pub duration: Option<Option<Duration>>,
        pub bytes: Option<i64>,
    }

    #[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
    pub enum KafkaSinkFormat {
        Avro {
            schema_registry_url: Url,
            key_schema: Option<String>,
            value_schema: String,
            ccsr_config: mz_ccsr::ClientConfig,
        },
        Json,
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

impl From<&IndexDesc> for ProtoIndexDesc {
    fn from(x: &IndexDesc) -> Self {
        ProtoIndexDesc {
            on_id: Some((&x.on_id).into()),
            key: x.key.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<ProtoIndexDesc> for IndexDesc {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoIndexDesc) -> Result<Self, Self::Error> {
        Ok(IndexDesc {
            on_id: x.on_id.try_into_if_some("ProtoIndexDesc::on_id")?,
            key: x
                .key
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
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

impl From<&LinearOperator> for ProtoLinearOperator {
    fn from(x: &LinearOperator) -> Self {
        ProtoLinearOperator {
            predicates: x.predicates.iter().map(Into::into).collect(),
            projection: x.projection.iter().map(|x| x.into_proto()).collect(),
        }
    }
}

impl TryFrom<ProtoLinearOperator> for LinearOperator {
    type Error = TryFromProtoError;

    fn try_from(x: ProtoLinearOperator) -> Result<Self, Self::Error> {
        Ok(LinearOperator {
            predicates: x
                .predicates
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
            projection: x
                .projection
                .into_iter()
                .map(usize::from_proto)
                .collect::<Result<Vec<_>, _>>()?,
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
