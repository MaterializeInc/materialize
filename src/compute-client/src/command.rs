// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Tonic generates code that calls clone on an Arc. Allow this here.
// TODO: Remove this once tonic does not produce this code anymore.
#![allow(clippy::clone_on_ref_ptr)]

//! Compute layer commands.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroI64;

use proptest::prelude::{any, Arbitrary};
use proptest::prop_oneof;
use proptest::strategy::{BoxedStrategy, Strategy};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use uuid::Uuid;

use mz_expr::{
    CollectionPlan, MirRelationExpr, MirScalarExpr, OptimizedMirRelationExpr, RowSetFinishing,
};
use mz_ore::tracing::OpenTelemetryContext;
use mz_proto::{any_uuid, IntoRustIfSome, ProtoMapEntry, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, RelationType, Row};
use mz_storage_client::client::ProtoAllowCompaction;
use mz_storage_client::controller::CollectionMetadata;

use crate::command::proto_dataflow_description::{
    ProtoIndexExport, ProtoIndexImport, ProtoSinkExport, ProtoSourceImport,
};
use crate::logging::LoggingConfig;
use crate::plan::Plan;
use crate::sinks::{ComputeSinkConnection, ComputeSinkDesc};

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.command.rs"));

/// Commands related to the computation and maintenance of views.
///
/// A replica can consist of multiple computed processes. Upon startup, a computed will listen for
/// a connection from environmentd. The first command sent to computed must be a CreateTimely
/// command, which will build the timely runtime.
///
/// CreateTimely is the only command that is sent to every process of the replica by environmentd.
/// The other commands are sent only to the first process, which in turn will disseminate the
/// command to other timely workers using the timely communication fabric.
///
/// After a timely runtime has been built with CreateTimely, a sequence of commands that have to be
/// handled in the timely runtime can be sent: First a CreateInstance must be sent which activates
/// logging sources. After this, any combination of CreateDataflows, AllowCompaction, Peek,
/// UpdateMaxResultSize and CancelPeeks can be sent.
///
/// Within this sequence, exactly one InitializationComplete has to be sent. Commands sent before
/// InitializationComplete are buffered and are compacted. For example a Peek followed by a
/// CancelPeek will become a no-op if sent before InitializationComplete. After
/// InitializationComplete, the computed is considered rehydrated and will immediately act upon the
/// commands. If a new cluster is created, InitializationComplete will follow immediately after
/// CreateInstance. If a replica is added to a cluster or environmentd restarts and rehydrates a
/// computed, a potentially long command sequence will be sent before InitializationComplete.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComputeCommand<T = mz_repr::Timestamp> {
    /// Create the timely runtime according to the supplied CommunicationConfig. Must be the first
    /// command sent to a computed. This is the only command that is broadcasted by
    /// ActiveReplication to all computed processes within a replica.
    CreateTimely {
        comm_config: CommunicationConfig,
        epoch: ComputeStartupEpoch,
    },

    /// Setup and logging sources within a running timely instance. Must be the second command
    /// after CreateTimely.
    CreateInstance(InstanceConfig),

    /// Indicates that the controller has sent all commands reflecting its
    /// initial state.
    InitializationComplete,

    /// Create a sequence of dataflows.
    ///
    /// Each of the dataflows must contain `as_of` members that are valid
    /// for each of the referenced arrangements, meaning `AllowCompaction`
    /// should be held back to those values until the command.
    /// Subsequent commands may arbitrarily compact the arrangements;
    /// the dataflow runners are responsible for ensuring that they can
    /// correctly maintain the dataflows.
    CreateDataflows(Vec<DataflowDescription<crate::plan::Plan<T>, CollectionMetadata, T>>),

    /// Enable compaction in compute-managed collections.
    ///
    /// Each entry in the vector names a collection and provides a frontier after which
    /// accumulations must be correct. The workers gain the liberty of compacting
    /// the corresponding maintained traces up through that frontier.
    AllowCompaction(Vec<(GlobalId, Antichain<T>)>),

    /// Peek at an arrangement.
    Peek(Peek<T>),

    /// Cancel the peeks associated with the given `uuids`.
    CancelPeeks {
        /// The identifiers of the peek requests to cancel.
        uuids: BTreeSet<Uuid>,
    },
    UpdateMaxResultSize(u32),
}

impl RustType<ProtoComputeCommand> for ComputeCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoComputeCommand {
        use proto_compute_command::Kind::*;
        use proto_compute_command::*;
        ProtoComputeCommand {
            kind: Some(match self {
                ComputeCommand::CreateInstance(config) => CreateInstance(config.into_proto()),
                ComputeCommand::InitializationComplete => InitializationComplete(()),
                ComputeCommand::CreateDataflows(dataflows) => {
                    CreateDataflows(ProtoCreateDataflows {
                        dataflows: dataflows.into_proto(),
                    })
                }
                ComputeCommand::AllowCompaction(collections) => {
                    AllowCompaction(ProtoAllowCompaction {
                        collections: collections.into_proto(),
                    })
                }
                ComputeCommand::Peek(peek) => Peek(peek.into_proto()),
                ComputeCommand::CancelPeeks { uuids } => CancelPeeks(ProtoCancelPeeks {
                    uuids: uuids.into_proto(),
                }),
                ComputeCommand::UpdateMaxResultSize(max_result_size) => {
                    UpdateMaxResultSize(max_result_size.into_proto())
                }
                ComputeCommand::CreateTimely {
                    comm_config,
                    epoch: ComputeStartupEpoch { envd, replica },
                } => CreateTimely(ProtoCreateTimely {
                    comm_config: Some(comm_config.into_proto()),
                    epoch: Some(ProtoComputeStartupEpoch {
                        envd: envd.get().into_proto(),
                        replica: replica.into_proto(),
                    }),
                }),
            }),
        }
    }

    fn from_proto(proto: ProtoComputeCommand) -> Result<Self, TryFromProtoError> {
        use proto_compute_command::Kind::*;
        use proto_compute_command::*;
        match proto.kind {
            Some(CreateInstance(config)) => Ok(ComputeCommand::CreateInstance(config.into_rust()?)),
            Some(InitializationComplete(())) => Ok(ComputeCommand::InitializationComplete),
            Some(CreateDataflows(ProtoCreateDataflows { dataflows })) => {
                Ok(ComputeCommand::CreateDataflows(dataflows.into_rust()?))
            }
            Some(AllowCompaction(ProtoAllowCompaction { collections })) => {
                Ok(ComputeCommand::AllowCompaction(collections.into_rust()?))
            }
            Some(Peek(peek)) => Ok(ComputeCommand::Peek(peek.into_rust()?)),
            Some(CancelPeeks(ProtoCancelPeeks { uuids })) => Ok(ComputeCommand::CancelPeeks {
                uuids: uuids.into_rust()?,
            }),
            Some(UpdateMaxResultSize(ProtoUpdateMaxResultSize { max_result_size })) => {
                Ok(ComputeCommand::UpdateMaxResultSize(max_result_size))
            }
            Some(CreateTimely(ProtoCreateTimely { comm_config, epoch })) => {
                let comm_config = comm_config.ok_or_else(|| {
                    TryFromProtoError::missing_field("ProtoCreateTimely::comm_config")
                })?;
                let epoch = epoch
                    .ok_or_else(|| TryFromProtoError::missing_field("ProtoCreateTimely::epoch"))?;
                Ok(ComputeCommand::CreateTimely {
                    comm_config: comm_config.into_rust()?,
                    epoch: epoch.into_rust()?,
                })
            }
            None => Err(TryFromProtoError::missing_field(
                "ProtoComputeCommand::kind",
            )),
        }
    }
}

impl Arbitrary for ComputeCommand<mz_repr::Timestamp> {
    type Strategy = BoxedStrategy<Self>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            any::<InstanceConfig>().prop_map(ComputeCommand::CreateInstance),
            proptest::collection::vec(
                any::<DataflowDescription<crate::plan::Plan, CollectionMetadata, mz_repr::Timestamp>>(),
                1..4
            )
            .prop_map(ComputeCommand::CreateDataflows),
            proptest::collection::vec(
                (
                    any::<GlobalId>(),
                    proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4)
                ),
                1..4
            )
            .prop_map(|collections| ComputeCommand::AllowCompaction(
                collections
                    .into_iter()
                    .map(|(id, frontier_vec)| { (id, Antichain::from(frontier_vec)) })
                    .collect()
            )),
            any::<Peek>().prop_map(ComputeCommand::Peek),
            proptest::collection::vec(any_uuid(), 1..6).prop_map(|uuids| {
                ComputeCommand::CancelPeeks {
                    uuids: BTreeSet::from_iter(uuids.into_iter()),
                }
            })
        ]
        .boxed()
    }
}

/// A value generated by environmentd and passed to the computed processes
/// to help them disambiguate different `CreateTimely` commands.
///
/// The semantics of this value are not important, except that they
/// must be totally ordered, and any value (for a given replica) must
/// be greater than any that were generated before (for that replica).
/// This is the reason for having two
/// components (one from the stash that increases on every environmentd restart,
/// another in-memory and local to the current incarnation of environmentd)
#[derive(PartialEq, Eq, Debug, Copy, Clone, Serialize, Deserialize)]
pub struct ComputeStartupEpoch {
    envd: NonZeroI64,
    replica: u64,
}

impl RustType<ProtoComputeStartupEpoch> for ComputeStartupEpoch {
    fn into_proto(&self) -> ProtoComputeStartupEpoch {
        let Self { envd, replica } = self;
        ProtoComputeStartupEpoch {
            envd: envd.get(),
            replica: *replica,
        }
    }

    fn from_proto(proto: ProtoComputeStartupEpoch) -> Result<Self, TryFromProtoError> {
        let ProtoComputeStartupEpoch { envd, replica } = proto;
        Ok(Self {
            envd: envd.try_into().unwrap(),
            replica,
        })
    }
}

impl ComputeStartupEpoch {
    pub fn new(envd: NonZeroI64, replica: u64) -> Self {
        Self { envd, replica }
    }

    /// Serialize for transfer over the network
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut ret = [0; 16];
        let mut p = &mut ret[..];
        use std::io::Write;
        p.write_all(&self.envd.get().to_be_bytes()[..]).unwrap();
        p.write_all(&self.replica.to_be_bytes()[..]).unwrap();
        ret
    }

    /// Inverse of `to_bytes`
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        let envd = i64::from_be_bytes((&bytes[0..8]).try_into().unwrap());
        let replica = u64::from_be_bytes((&bytes[8..16]).try_into().unwrap());
        Self {
            envd: envd.try_into().unwrap(),
            replica,
        }
    }
}

impl std::fmt::Display for ComputeStartupEpoch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { envd, replica } = self;
        write!(f, "({envd}, {replica})")
    }
}

impl PartialOrd for ComputeStartupEpoch {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ComputeStartupEpoch {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let Self { envd, replica } = self;
        let Self {
            envd: other_envd,
            replica: other_replica,
        } = other;
        (envd, replica).cmp(&(other_envd, other_replica))
    }
}

/// An abstraction allowing us to name different replicas.
pub type ReplicaId = u64;

/// Identifier of a process within a replica.
pub type ProcessId = u64;

#[derive(Arbitrary, Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Configuration sent to new compute instances.
pub struct InstanceConfig {
    /// Configuration of logging sources.
    pub logging: LoggingConfig,
    /// Max size in bytes of any result.
    pub max_result_size: u32,
}

/// Configuration of the cluster we will spin up
#[derive(Arbitrary, Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct CommunicationConfig {
    /// Number of per-process worker threads
    pub workers: usize,
    /// Identity of this process
    pub process: usize,
    /// Addresses of all processes
    pub addresses: Vec<String>,
}

impl RustType<ProtoInstanceConfig> for InstanceConfig {
    fn into_proto(&self) -> ProtoInstanceConfig {
        ProtoInstanceConfig {
            logging: Some(self.logging.into_proto()),
            max_result_size: self.max_result_size,
        }
    }

    fn from_proto(proto: ProtoInstanceConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            logging: proto
                .logging
                .into_rust_if_some("ProtoInstanceConfig::logging")?,
            max_result_size: proto.max_result_size,
        })
    }
}

impl RustType<ProtoCommunicationConfig> for CommunicationConfig {
    fn into_proto(&self) -> ProtoCommunicationConfig {
        ProtoCommunicationConfig {
            workers: self.workers.into_proto(),
            addresses: self.addresses.into_proto(),
            process: self.process.into_proto(),
        }
    }

    fn from_proto(proto: ProtoCommunicationConfig) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            process: proto.process.into_rust()?,
            workers: proto.workers.into_rust()?,
            addresses: proto.addresses.into_rust()?,
        })
    }
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
#[derive(Arbitrary, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceDesc<M> {
    /// Arguments for this instantiation of the source.
    pub arguments: SourceInstanceArguments,
    /// Additional metadata used by the storage client of a compute instance to read it.
    pub storage_metadata: M,
    /// The relation type of this source
    pub typ: RelationType,
}

impl RustType<ProtoSourceInstanceDesc> for SourceInstanceDesc<CollectionMetadata> {
    fn into_proto(&self) -> ProtoSourceInstanceDesc {
        ProtoSourceInstanceDesc {
            arguments: Some(self.arguments.into_proto()),
            storage_metadata: Some(self.storage_metadata.into_proto()),
            typ: Some(self.typ.into_proto()),
        }
    }

    fn from_proto(proto: ProtoSourceInstanceDesc) -> Result<Self, TryFromProtoError> {
        Ok(SourceInstanceDesc {
            arguments: proto
                .arguments
                .into_rust_if_some("ProtoSourceInstanceDesc::arguments")?,
            storage_metadata: proto
                .storage_metadata
                .into_rust_if_some("ProtoSourceInstanceDesc::storage_metadata")?,
            typ: proto
                .typ
                .into_rust_if_some("ProtoSourceInstanceDesc::typ")?,
        })
    }
}

/// Per-source construction arguments.
#[derive(Arbitrary, Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct SourceInstanceArguments {
    /// Linear operators to be applied record-by-record.
    pub operators: Option<mz_expr::MapFilterProject>,
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
    /// Sources instantiations made available to the dataflow pair with monotonicity information.
    pub source_imports: BTreeMap<GlobalId, (SourceInstanceDesc<S>, bool)>,
    /// Indexes made available to the dataflow.
    /// (id of new index, description of index, relationtype of base source/view, monotonic)
    pub index_imports: BTreeMap<GlobalId, (IndexDesc, RelationType, bool)>,
    /// Views and indexes to be built and stored in the local context.
    /// Objects must be built in the specific order, as there may be
    /// dependencies of later objects on prior identifiers.
    pub objects_to_build: Vec<BuildDesc<P>>,
    /// Indexes to be made available to be shared with other dataflows
    /// (id of new index, description of index, relationtype of base source/view)
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
    pub until: Antichain<T>,
    /// Human readable name
    pub debug_name: String,
}

fn any_source_import(
) -> impl Strategy<Value = (GlobalId, (SourceInstanceDesc<CollectionMetadata>, bool))> {
    (
        any::<GlobalId>(),
        any::<(SourceInstanceDesc<CollectionMetadata>, bool)>(),
    )
}

proptest::prop_compose! {
    fn any_dataflow_index_import()(
        id in any::<GlobalId>(),
        index in any::<IndexDesc>(),
        typ in any::<RelationType>(),
        monotonic in any::<bool>(),
    ) -> (GlobalId, (IndexDesc, RelationType, bool)) {
        (id, (index, typ, monotonic))
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

proptest::prop_compose! {
    fn any_dataflow_description()(
        source_imports in proptest::collection::vec(any_source_import(), 1..3),
        index_imports in proptest::collection::vec(any_dataflow_index_import(), 1..3),
        objects_to_build in proptest::collection::vec(any::<BuildDesc<Plan>>(), 1..3),
        index_exports in proptest::collection::vec(any_dataflow_index_export(), 1..3),
        sink_descs in proptest::collection::vec(
            any::<(GlobalId, ComputeSinkDesc<CollectionMetadata, mz_repr::Timestamp>)>(),
            1..3,
        ),
        as_of_some in any::<bool>(),
        as_of in proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..5),
        debug_name in ".*",
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
            until: Antichain::new(),
            debug_name,
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
            until: Antichain::new(),
            debug_name: name,
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
        description: IndexDesc,
        typ: RelationType,
        monotonic: bool,
    ) {
        self.index_imports.insert(id, (description, typ, monotonic));
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
        for (source_id, (source, _monotonic)) in self.source_imports.iter() {
            if source_id == id {
                return source.typ.arity();
            }
        }
        for (desc, typ, _monotonic) in self.index_imports.values() {
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

    /// Identifiers of exported subscribe sinks.
    pub fn subscribe_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.sink_exports
            .iter()
            .filter_map(|(id, desc)| match desc.connection {
                ComputeSinkConnection::Subscribe(_) => Some(*id),
                _ => None,
            })
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
        for (index_id, (desc, _typ, _monotonic)) in &self.index_imports {
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
            as_of: self.as_of.into_proto(),
            until: Some(self.until.into_proto()),
            debug_name: self.debug_name.clone(),
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
            debug_name: proto.debug_name,
        })
    }
}

impl ProtoMapEntry<GlobalId, (SourceInstanceDesc<CollectionMetadata>, bool)> for ProtoSourceImport {
    fn from_rust<'a>(
        entry: (
            &'a GlobalId,
            &'a (SourceInstanceDesc<CollectionMetadata>, bool),
        ),
    ) -> Self {
        ProtoSourceImport {
            id: Some(entry.0.into_proto()),
            source_instance_desc: Some(entry.1 .0.into_proto()),
            monotonic: entry.1 .1.into_proto(),
        }
    }

    fn into_rust(
        self,
    ) -> Result<(GlobalId, (SourceInstanceDesc<CollectionMetadata>, bool)), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoSourceImport::id")?,
            (
                self.source_instance_desc
                    .into_rust_if_some("ProtoSourceImport::source_instance_desc")?,
                self.monotonic.into_rust()?,
            ),
        ))
    }
}

impl ProtoMapEntry<GlobalId, (IndexDesc, RelationType, bool)> for ProtoIndexImport {
    fn from_rust<'a>(
        (id, (index_desc, typ, monotonic)): (&'a GlobalId, &'a (IndexDesc, RelationType, bool)),
    ) -> Self {
        ProtoIndexImport {
            id: Some(id.into_proto()),
            index_desc: Some(index_desc.into_proto()),
            typ: Some(typ.into_proto()),
            monotonic: monotonic.into_proto(),
        }
    }

    fn into_rust(self) -> Result<(GlobalId, (IndexDesc, RelationType, bool)), TryFromProtoError> {
        Ok((
            self.id.into_rust_if_some("ProtoIndex::id")?,
            (
                self.index_desc
                    .into_rust_if_some("ProtoIndexImport::index_desc")?,
                self.typ.into_rust_if_some("ProtoIndexImport::typ")?,
                self.monotonic.into_rust()?,
            ),
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

/// Peek at an arrangement.
///
/// This request elicits data from the worker, by naming an
/// arrangement and some actions to apply to the results before
/// returning them.
///
/// The `timestamp` member must be valid for the arrangement that
/// is referenced by `id`. This means that `AllowCompaction` for
/// this arrangement should not pass `timestamp` before this command.
/// Subsequent commands may arbitrarily compact the arrangements;
/// the dataflow runners are responsible for ensuring that they can
/// correctly answer the `Peek`.
#[derive(Arbitrary, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Peek<T = mz_repr::Timestamp> {
    /// The identifier of the arrangement.
    pub id: GlobalId,
    /// If `Some`, then look up only the given keys from the arrangement (instead of a full scan).
    /// The vector is never empty.
    #[proptest(strategy = "proptest::option::of(proptest::collection::vec(any::<Row>(), 1..5))")]
    pub literal_constraints: Option<Vec<Row>>,
    /// The identifier of this peek request.
    ///
    /// Used in responses and cancellation requests.
    #[proptest(strategy = "any_uuid()")]
    pub uuid: Uuid,
    /// The logical timestamp at which the arrangement is queried.
    pub timestamp: T,
    /// Actions to apply to the result set before returning them.
    pub finishing: RowSetFinishing,
    /// Linear operation to apply in-line on each result.
    pub map_filter_project: mz_expr::SafeMfpPlan,
    /// An `OpenTelemetryContext` to forward trace information along
    /// to the compute worker to allow associating traces between
    /// the compute controller and the compute worker.
    #[proptest(strategy = "empty_otel_ctx()")]
    pub otel_ctx: OpenTelemetryContext,
}

impl RustType<ProtoPeek> for Peek {
    fn into_proto(&self) -> ProtoPeek {
        ProtoPeek {
            id: Some(self.id.into_proto()),
            key: match &self.literal_constraints {
                // In the Some case, the vector is never empty, so it's safe to encode None as an
                // empty vector, and Some(vector) as just the vector.
                Some(vec) => {
                    assert!(!vec.is_empty());
                    vec.into_proto()
                }
                None => Vec::<Row>::new().into_proto(),
            },
            uuid: Some(self.uuid.into_proto()),
            timestamp: self.timestamp.into(),
            finishing: Some(self.finishing.into_proto()),
            map_filter_project: Some(self.map_filter_project.into_proto()),
            otel_ctx: self.otel_ctx.clone().into(),
        }
    }

    fn from_proto(x: ProtoPeek) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            id: x.id.into_rust_if_some("ProtoPeek::id")?,
            literal_constraints: {
                let vec: Vec<Row> = x.key.into_rust()?;
                if vec.is_empty() {
                    None
                } else {
                    Some(vec)
                }
            },
            uuid: x.uuid.into_rust_if_some("ProtoPeek::uuid")?,
            timestamp: x.timestamp.into(),
            finishing: x.finishing.into_rust_if_some("ProtoPeek::finishing")?,
            map_filter_project: x
                .map_filter_project
                .into_rust_if_some("ProtoPeek::map_filter_project")?,
            otel_ctx: x.otel_ctx.into(),
        })
    }
}

impl RustType<ProtoUpdateMaxResultSize> for u32 {
    fn into_proto(&self) -> ProtoUpdateMaxResultSize {
        ProtoUpdateMaxResultSize {
            max_result_size: *self,
        }
    }

    fn from_proto(proto: ProtoUpdateMaxResultSize) -> Result<Self, TryFromProtoError> {
        Ok(proto.max_result_size)
    }
}

fn empty_otel_ctx() -> impl Strategy<Value = OpenTelemetryContext> {
    (0..1).prop_map(|_| OpenTelemetryContext::empty())
}

#[derive(Debug)]
pub struct ComputeCommandHistory<T = mz_repr::Timestamp> {
    /// The number of commands at the last time we compacted the history.
    reduced_count: usize,
    /// The sequence of commands that should be applied.
    ///
    /// This list may not be "compact" in that there can be commands that could be optimized
    /// or removed given the context of other commands, for example compaction commands that
    /// can be unified, or dataflows that can be dropped due to allowed compaction.
    commands: Vec<ComputeCommand<T>>,
    /// The number of dataflows in the compute command history.
    dataflow_count: usize,
}

impl<T: timely::progress::Timestamp> ComputeCommandHistory<T> {
    /// Add a command to the history.
    ///
    /// This action will reduce the history every time it doubles while retaining the
    /// provided peeks.
    pub fn push<V>(
        &mut self,
        command: ComputeCommand<T>,
        peeks: &std::collections::HashMap<uuid::Uuid, V>,
    ) {
        if let ComputeCommand::CreateDataflows(dataflows) = &command {
            self.dataflow_count += dataflows.len();
        }
        self.commands.push(command);
        if self.commands.len() > 2 * self.reduced_count {
            self.retain_peeks(peeks);
            self.reduce();
        }
    }

    /// Obtains the length of this compute command history in number of commands.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Obtains the number of dataflows in this compute command history.
    pub fn dataflow_count(&self) -> usize {
        self.dataflow_count
    }

    /// Reduces `self.history` to a minimal form.
    ///
    /// This action not only simplifies the issued history, but importantly reduces the instructions
    /// to only reference inputs from times that are still certain to be valid. Commands that allow
    /// compaction of a collection also remove certainty that the inputs will be available for times
    /// not greater or equal to that compaction frontier.
    pub fn reduce(&mut self) {
        // First determine what the final compacted frontiers will be for each collection.
        // These will determine for each collection whether the command that creates it is required,
        // and if required what `as_of` frontier should be used for its updated command.
        let mut final_frontiers = std::collections::BTreeMap::new();
        let mut live_dataflows = Vec::new();
        let mut live_peeks = Vec::new();
        let mut live_cancels = std::collections::BTreeSet::new();

        let mut create_inst_command = None;
        let mut create_timely_command = None;
        let mut update_max_result_size_command = None;

        let mut initialization_complete = false;

        for command in self.commands.drain(..) {
            match command {
                create_timely @ ComputeCommand::CreateTimely { .. } => {
                    assert!(create_timely_command.is_none());
                    create_timely_command = Some(create_timely);
                }
                // We should be able to handle the Create* commands, should this client need to be restartable.
                create_inst @ ComputeCommand::CreateInstance(_) => {
                    assert!(create_inst_command.is_none());
                    create_inst_command = Some(create_inst);
                }
                ComputeCommand::InitializationComplete => {
                    initialization_complete = true;
                }
                ComputeCommand::CreateDataflows(dataflows) => {
                    live_dataflows.extend(dataflows);
                }
                ComputeCommand::AllowCompaction(frontiers) => {
                    for (id, frontier) in frontiers {
                        final_frontiers.insert(id, frontier.clone());
                    }
                }
                ComputeCommand::Peek(peek) => {
                    live_peeks.push(peek);
                }
                ComputeCommand::CancelPeeks { uuids } => {
                    live_cancels.extend(uuids);
                }
                update @ ComputeCommand::UpdateMaxResultSize(_) => {
                    update_max_result_size_command = Some(update);
                }
            }
        }

        // Determine the required antichains to support live peeks;
        let mut live_peek_frontiers = std::collections::BTreeMap::new();
        for Peek { id, timestamp, .. } in live_peeks.iter() {
            // Introduce `time` as a constraint on the `as_of` frontier of `id`.
            live_peek_frontiers
                .entry(id)
                .or_insert_with(Antichain::new)
                .insert(timestamp.clone());
        }

        // Update dataflow `as_of` frontiers, constrained by live peeks and allowed compaction.
        // One possible frontier is the empty frontier, indicating that the dataflow can be removed.
        for dataflow in live_dataflows.iter_mut() {
            let mut as_of = Antichain::new();
            for id in dataflow.export_ids() {
                // If compaction has been allowed use that; otherwise use the initial `as_of`.
                if let Some(frontier) = final_frontiers.get(&id) {
                    as_of.extend(frontier.clone());
                } else {
                    as_of.extend(dataflow.as_of.clone().unwrap());
                }
                // If we have requirements from peeks, apply them to hold `as_of` back.
                if let Some(frontier) = live_peek_frontiers.get(&id) {
                    as_of.extend(frontier.clone());
                }
            }

            // Remove compaction for any collection that brought us to `as_of`.
            for id in dataflow.export_ids() {
                if let Some(frontier) = final_frontiers.get(&id) {
                    if frontier == &as_of {
                        final_frontiers.remove(&id);
                    }
                }
            }

            dataflow.as_of = Some(as_of);
        }

        // Discard dataflows whose outputs have all been allowed to compact away.
        live_dataflows.retain(|dataflow| dataflow.as_of != Some(Antichain::new()));

        // Record the volume of post-compaction commands.
        let mut command_count = 1;
        command_count += live_dataflows.len();
        command_count += final_frontiers.len();
        command_count += live_peeks.len();
        command_count += live_cancels.len();
        if update_max_result_size_command.is_some() {
            command_count += 1;
        }

        // Reconstitute the commands as a compact history.
        if let Some(create_timely_command) = create_timely_command {
            self.commands.push(create_timely_command);
        }
        if let Some(create_inst_command) = create_inst_command {
            self.commands.push(create_inst_command);
        }
        self.dataflow_count = live_dataflows.len();
        if !live_dataflows.is_empty() {
            self.commands
                .push(ComputeCommand::CreateDataflows(live_dataflows));
        }
        self.commands
            .extend(live_peeks.into_iter().map(ComputeCommand::Peek));
        if !live_cancels.is_empty() {
            self.commands.push(ComputeCommand::CancelPeeks {
                uuids: live_cancels,
            });
        }
        // Allow compaction only after emmitting peek commands.
        if !final_frontiers.is_empty() {
            self.commands.push(ComputeCommand::AllowCompaction(
                final_frontiers.into_iter().collect(),
            ));
        }
        if initialization_complete {
            self.commands.push(ComputeCommand::InitializationComplete);
        }
        if let Some(update_max_result_size_command) = update_max_result_size_command {
            self.commands.push(update_max_result_size_command)
        }

        self.reduced_count = command_count;
    }

    /// Retain only those peeks present in `peeks` and discard the rest.
    pub fn retain_peeks<V>(&mut self, peeks: &std::collections::HashMap<uuid::Uuid, V>) {
        for command in self.commands.iter_mut() {
            if let ComputeCommand::CancelPeeks { uuids } = command {
                uuids.retain(|uuid| peeks.contains_key(uuid));
            }
        }
        self.commands.retain(|command| match command {
            ComputeCommand::Peek(peek) => peeks.contains_key(&peek.uuid),
            ComputeCommand::CancelPeeks { uuids } => !uuids.is_empty(),
            _ => true,
        });
    }

    /// Iterate through the contained commands.
    pub fn iter(&self) -> impl Iterator<Item = &ComputeCommand<T>> {
        self.commands.iter()
    }
}

impl<T> Default for ComputeCommandHistory<T> {
    fn default() -> Self {
        Self {
            reduced_count: 0,
            commands: Vec::new(),
            dataflow_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::ProptestConfig;
    use proptest::proptest;

    use mz_proto::protobuf_roundtrip;

    use super::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn peek_protobuf_roundtrip(expect in any::<Peek>() ) {
            let actual = protobuf_roundtrip::<_, ProtoPeek>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        // TODO: Unignore after fixing #14543.
        #[test]
        #[ignore]
        fn compute_command_protobuf_roundtrip(expect in any::<ComputeCommand<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoComputeCommand>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }

        // TODO: Unignore after fixing #14543.
        #[test]
        #[ignore]
        fn dataflow_description_protobuf_roundtrip(expect in any::<DataflowDescription<Plan, CollectionMetadata, mz_repr::Timestamp>>()) {
            let actual = protobuf_roundtrip::<_, ProtoDataflowDescription>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
