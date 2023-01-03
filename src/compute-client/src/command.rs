// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute layer commands.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroI64;

use proptest::prelude::{any, Arbitrary};
use proptest::strategy::{BoxedStrategy, Strategy, Union};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use timely::progress::frontier::Antichain;
use uuid::Uuid;

use mz_expr::RowSetFinishing;
use mz_ore::tracing::OpenTelemetryContext;
use mz_proto::{any_uuid, IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use mz_repr::{GlobalId, Row};
use mz_storage_client::client::ProtoAllowCompaction;
use mz_storage_client::controller::CollectionMetadata;

use crate::logging::LoggingConfig;
use crate::types::dataflows::DataflowDescription;

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.command.rs"));

/// Commands related to the computation and maintenance of views.
///
/// A replica can consist of multiple clusterd processes. Upon startup, a clusterd will listen for
/// a connection from environmentd. The first command sent to clusterd must be a CreateTimely
/// command, which will build the timely runtime.
///
/// CreateTimely is the only command that is sent to every process of the replica by environmentd.
/// The other commands are sent only to the first process, which in turn will disseminate the
/// command to other timely workers using the timely communication fabric.
///
/// After a timely runtime has been built with CreateTimely, a sequence of commands that have to be
/// handled in the timely runtime can be sent: First a CreateInstance must be sent which activates
/// logging sources. After this, any combination of UpdateConfiguration, CreateDataflows,
/// AllowCompaction, Peek, and CancelPeeks can be sent.
///
/// Within this sequence, exactly one InitializationComplete has to be sent. Commands sent before
/// InitializationComplete are buffered and are compacted. For example a Peek followed by a
/// CancelPeek will become a no-op if sent before InitializationComplete. After
/// InitializationComplete, the clusterd is considered rehydrated and will immediately act upon the
/// commands. If a new cluster is created, InitializationComplete will follow immediately after
/// CreateInstance. If a replica is added to a cluster or environmentd restarts and rehydrates a
/// clusterd, a potentially long command sequence will be sent before InitializationComplete.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ComputeCommand<T = mz_repr::Timestamp> {
    /// Create the timely runtime according to the supplied CommunicationConfig. Must be the first
    /// command sent to a clusterd. This is the only command that is broadcasted by
    /// ActiveReplication to all clusterd processes within a replica.
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

    /// Update compute instance configuration.
    UpdateConfiguration(BTreeSet<ComputeParameter>),

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
}

impl RustType<ProtoComputeCommand> for ComputeCommand<mz_repr::Timestamp> {
    fn into_proto(&self) -> ProtoComputeCommand {
        use proto_compute_command::Kind::*;
        use proto_compute_command::*;
        ProtoComputeCommand {
            kind: Some(match self {
                ComputeCommand::CreateInstance(config) => CreateInstance(config.into_proto()),
                ComputeCommand::InitializationComplete => InitializationComplete(()),
                ComputeCommand::UpdateConfiguration(params) => {
                    UpdateConfiguration(ProtoUpdateConfiguration {
                        params: params.into_proto(),
                    })
                }
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
            Some(UpdateConfiguration(ProtoUpdateConfiguration { params })) => {
                Ok(ComputeCommand::UpdateConfiguration(params.into_rust()?))
            }
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
    type Strategy = Union<BoxedStrategy<Self>>;
    type Parameters = ();

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        Union::new(vec![
                any::<InstanceConfig>()
                    .prop_map(ComputeCommand::CreateInstance)
                    .boxed(),
                proptest::collection::btree_set(any::<ComputeParameter>(), 1..4)
                    .prop_map(ComputeCommand::UpdateConfiguration)
                    .boxed(),
                proptest::collection::vec(
                    any::<
                        DataflowDescription<
                            crate::plan::Plan,
                            CollectionMetadata,
                            mz_repr::Timestamp,
                        >,
                    >(),
                    1..4,
                )
                .prop_map(ComputeCommand::CreateDataflows)
                .boxed(),
                proptest::collection::vec(
                    (
                        any::<GlobalId>(),
                        proptest::collection::vec(any::<mz_repr::Timestamp>(), 1..4),
                    ),
                    1..4,
                )
                .prop_map(|collections| {
                    ComputeCommand::AllowCompaction(
                        collections
                            .into_iter()
                            .map(|(id, frontier_vec)| (id, Antichain::from(frontier_vec)))
                            .collect(),
                    )
                })
                .boxed(),
                any::<Peek>().prop_map(ComputeCommand::Peek).boxed(),
                proptest::collection::vec(any_uuid(), 1..6)
                    .prop_map(|uuids| ComputeCommand::CancelPeeks {
                        uuids: BTreeSet::from_iter(uuids.into_iter()),
                    })
                    .boxed(),
            ])
    }
}

/// A value generated by environmentd and passed to the clusterd processes
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

/// Compute instance configuration parameters.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Arbitrary)]
pub enum ComputeParameter {
    /// The maximum allowed size in bytes for results of peeks and subscribes.
    ///
    /// Peeks and subscribes that would return results larger than this maximum return error
    /// responses instead.
    MaxResultSize(u32),
}

impl RustType<ProtoComputeParameter> for ComputeParameter {
    fn into_proto(&self) -> ProtoComputeParameter {
        use proto_compute_parameter::*;

        ProtoComputeParameter {
            kind: Some(match self {
                ComputeParameter::MaxResultSize(size) => Kind::MaxResultSize(*size),
            }),
        }
    }

    fn from_proto(proto: ProtoComputeParameter) -> Result<Self, TryFromProtoError> {
        use proto_compute_parameter::*;

        match proto.kind {
            Some(Kind::MaxResultSize(size)) => Ok(ComputeParameter::MaxResultSize(size)),
            None => Err(TryFromProtoError::missing_field(
                "ProtoComputeParameter::kind",
            )),
        }
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
        let mut final_frontiers = BTreeMap::new();
        let mut live_dataflows = Vec::new();
        let mut live_peeks = Vec::new();
        let mut live_cancels = BTreeSet::new();

        let mut create_inst_command = None;
        let mut create_timely_command = None;

        // Collect only the final configuration.
        // Note that this is only correct as long as all config parameters apply globally. If we
        // ever introduce parameters that only affect subsequent commands, we will have to
        // reconsider this approach.
        let mut final_configuration = BTreeSet::new();

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
                ComputeCommand::UpdateConfiguration(params) => {
                    final_configuration.extend(params);
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
        if !final_configuration.is_empty() {
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
        if !final_configuration.is_empty() {
            self.commands
                .push(ComputeCommand::UpdateConfiguration(final_configuration));
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

        #[test]
        fn compute_command_protobuf_roundtrip(expect in any::<ComputeCommand<mz_repr::Timestamp>>() ) {
            let actual = protobuf_roundtrip::<_, ProtoComputeCommand>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}
