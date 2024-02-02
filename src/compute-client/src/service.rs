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

//! Compute layer client and server.

use std::collections::BTreeMap;
use std::iter;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use bytesize::ByteSize;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_repr::{Diff, GlobalId, Row};
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use mz_service::grpc::{GrpcClient, GrpcServer, ProtoServiceTypes, ResponseStream};
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::PartialOrder;
use tonic::{Request, Status, Streaming};
use uuid::Uuid;

use crate::metrics::ReplicaMetrics;
use crate::protocol::command::{ComputeCommand, ProtoComputeCommand};
use crate::protocol::response::{
    ComputeResponse, CopyToResponse, PeekResponse, ProtoComputeResponse, SubscribeBatch,
    SubscribeResponse,
};
use crate::service::proto_compute_server::ProtoCompute;

include!(concat!(env!("OUT_DIR"), "/mz_compute_client.service.rs"));

/// A client to a compute server.
pub trait ComputeClient<T = mz_repr::Timestamp>:
    GenericClient<ComputeCommand<T>, ComputeResponse<T>>
{
}

impl<C, T> ComputeClient<T> for C where C: GenericClient<ComputeCommand<T>, ComputeResponse<T>> {}

#[async_trait]
impl<T: Send> GenericClient<ComputeCommand<T>, ComputeResponse<T>> for Box<dyn ComputeClient<T>> {
    async fn send(&mut self, cmd: ComputeCommand<T>) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }
    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, anyhow::Error> {
        (**self).recv().await
    }
}

/// TODO(#25239): Add documentation.
#[derive(Debug, Clone)]
pub enum ComputeProtoServiceTypes {}

impl ProtoServiceTypes for ComputeProtoServiceTypes {
    type PC = ProtoComputeCommand;
    type PR = ProtoComputeResponse;
    type STATS = ReplicaMetrics;
    const URL: &'static str = "/mz_compute_client.service.ProtoCompute/CommandResponseStream";
}

/// TODO(#25239): Add documentation.
pub type ComputeGrpcClient = GrpcClient<ComputeProtoServiceTypes>;

#[async_trait]
impl<F, G> ProtoCompute for GrpcServer<F>
where
    F: Fn() -> G + Send + Sync + 'static,
    G: ComputeClient + 'static,
{
    type CommandResponseStreamStream = ResponseStream<ProtoComputeResponse>;

    async fn command_response_stream(
        &self,
        request: Request<Streaming<ProtoComputeCommand>>,
    ) -> Result<tonic::Response<Self::CommandResponseStreamStream>, Status> {
        self.forward_bidi_stream(request).await
    }
}

/// Maintained state for partitioned compute clients.
///
/// This helper type unifies the responses of multiple partitioned workers in order to present as a
/// single worker:
///
///   * It emits `FrontierUpper` responses reporting the minimum/meet of frontiers reported by the
///     individual workers.
///   * It emits `PeekResponse`s and `SubscribeResponse`s reporting the union of the responses
///     received from the workers.
///
/// In the compute communication stack, this client is instantiated several times:
///
///   * One instance on the controller side, dispatching between cluster processes.
///   * One instance in each cluster process, dispatching between timely worker threads.
///
/// Note that because compute commands, except `CreateTimely` and `UpdateConfiguration`, are only
/// sent to the first process, the cluster-side instances of `PartitionedComputeState` are not
/// guaranteed to see all compute commands. Or more specifically: The instance running inside
/// process 0 sees all commands, whereas the instances running inside the other processes only see
/// `CreateTimely` and `UpdateConfiguration`. The `PartitionedComputeState` implementation must be
/// able to cope with this limited visiblity. It does so by performing most of its state management
/// based on observed compute responses rather than commands.
#[derive(Debug)]
pub struct PartitionedComputeState<T> {
    /// Number of partitions the state machine represents.
    parts: usize,
    /// The maximum result size this state machine can return.
    ///
    /// This is updated upon receiving [`ComputeCommand::UpdateConfiguration`]s.
    max_result_size: u64,
    /// Upper frontiers for indexes and sinks, both collected as a `MutableAntichain` across all
    /// partitions and individually listed for each partition.
    ///
    /// Frontier tracking for a collection is initialized when the first `FrontierUpper` response
    /// for that collection is received. Frontier tracking is ceased when all shards have reported
    /// advancement to the empty frontier.
    ///
    /// The compute protocol requires that shards always emit a `FrontierUpper` response reporting
    /// the empty frontier when a collection is dropped. It further requires that no further
    /// `FrontierUpper` responses are emitted for a collection after the empty frontier was
    /// reported. These properties ensure that a) we always cease frontier tracking for collections
    /// that have been dropped and b) frontier tracking for a collection is not re-initialized
    /// after it was ceased.
    uppers: BTreeMap<GlobalId, (MutableAntichain<T>, Vec<Antichain<T>>)>,
    /// Pending responses for a peek; returnable once all are available.
    ///
    /// Tracking of responses for a peek is initialized when the first `PeekResponse` for that peek
    /// is received. Once all shards have provided a `PeekResponse`, a unified peek response is
    /// emitted and the peek tracking state is dropped again.
    ///
    /// The compute protocol requires that exactly one response is emitted for each peek. This
    /// property ensures that a) we can eventually drop the tracking state maintained for a peek
    /// and b) we won't re-initialize tracking for a peek we have already served.
    peek_responses: BTreeMap<Uuid, BTreeMap<usize, PeekResponse>>,
    /// Pending responses for a copy to; returnable once all are available.
    ///
    /// Tracking of responses for a COPY TO is initialized when the first `CopyResponse` for that command
    /// is received. Once all shards have provided a `CopyResponse`, a unified copy response is
    /// emitted and the copy_to tracking state is dropped again.
    ///
    /// The compute protocol requires that exactly one response is emitted for each COPY TO command. This
    /// property ensures that a) we can eventually drop the tracking state maintained for a copy
    /// and b) we won't re-initialize tracking for a copy we have already served.
    copy_to_responses: BTreeMap<GlobalId, BTreeMap<usize, CopyToResponse>>,
    /// Tracks in-progress `SUBSCRIBE`s, and the stashed rows we are holding back until their
    /// timestamps are complete.
    ///
    /// The updates may be `Err` if any of the batches have reported an error, in which case the
    /// subscribe is permanently borked.
    ///
    /// Tracking of a subscribe is initialized when the first `SubscribeResponse` for that
    /// subscribe is received. Once all shards have emitted an "end-of-subscribe" response the
    /// subscribe tracking state is dropped again.
    ///
    /// The compute protocol requires that for a subscribe that shuts down an end-of-subscribe
    /// response is emitted:
    ///
    ///   * Either a `Batch` response reporting advancement to the empty frontier...
    ///   * ... or a `DroppedAt` response reporting that the subscribe was dropped before
    ///     completing.
    ///
    /// The compute protocol further requires that no further `SubscribeResponse`s are emitted for
    /// a subscribe after an end-of-subscribe was reported.
    ///
    /// These two properties ensure that a) once a subscribe has shut down, we can eventually drop
    /// the tracking state maintained for it and b) we won't re-initialize tracking for a subscribe
    /// we have already dropped.
    pending_subscribes: BTreeMap<GlobalId, PendingSubscribe<T>>,
}

impl<T> Partitionable<ComputeCommand<T>, ComputeResponse<T>>
    for (ComputeCommand<T>, ComputeResponse<T>)
where
    T: timely::progress::Timestamp + Lattice,
{
    type PartitionedState = PartitionedComputeState<T>;

    fn new(parts: usize) -> PartitionedComputeState<T> {
        PartitionedComputeState {
            parts,
            max_result_size: u64::MAX,
            uppers: BTreeMap::new(),
            peek_responses: BTreeMap::new(),
            pending_subscribes: BTreeMap::new(),
            copy_to_responses: BTreeMap::new(),
        }
    }
}

impl<T> PartitionedComputeState<T>
where
    T: timely::progress::Timestamp,
{
    fn reset(&mut self) {
        let PartitionedComputeState {
            parts: _,
            max_result_size: _,
            uppers,
            peek_responses,
            pending_subscribes,
            copy_to_responses,
        } = self;
        uppers.clear();
        peek_responses.clear();
        pending_subscribes.clear();
        copy_to_responses.clear();
    }

    /// Observes commands that move past, and prepares state for responses.
    pub fn observe_command(&mut self, command: &ComputeCommand<T>) {
        match command {
            ComputeCommand::CreateTimely { .. } => self.reset(),
            ComputeCommand::UpdateConfiguration(config) => {
                if let Some(max_result_size) = config.max_result_size {
                    self.max_result_size = max_result_size;
                }
            }
            _ => {
                // We are not guaranteed to observe other compute commands. We
                // must therefore not add any logic here that relies on doing so.
            }
        }
    }

    fn start_frontier_tracking(&mut self, id: GlobalId) {
        let mut frontier = MutableAntichain::new();
        // TODO(benesch): fix this dangerous use of `as`.
        #[allow(clippy::as_conversions)]
        frontier.update_iter(iter::once((T::minimum(), self.parts as i64)));
        let part_frontiers = vec![Antichain::from_elem(T::minimum()); self.parts];
        let previous = self.uppers.insert(id, (frontier, part_frontiers));
        assert!(
            previous.is_none(),
            "starting frontier tracking for already present identifier {id}"
        );
    }

    fn cease_frontier_tracking(&mut self, id: GlobalId) {
        let previous = self.uppers.remove(&id);
        assert!(
            previous.is_some(),
            "ceasing frontier tracking for absent identifier {id}",
        );
    }
}

impl<T> PartitionedState<ComputeCommand<T>, ComputeResponse<T>> for PartitionedComputeState<T>
where
    T: timely::progress::Timestamp + Lattice,
{
    fn split_command(&mut self, command: ComputeCommand<T>) -> Vec<Option<ComputeCommand<T>>> {
        self.observe_command(&command);

        // As specified by the compute protocol:
        //  * Forward `CreateTimely` and `UpdateConfiguration` commands to all shards.
        //  * Forward all other commands to the first shard only.
        match command {
            ComputeCommand::CreateTimely { config, epoch } => {
                let timely_cmds = config.split_command(self.parts);

                timely_cmds
                    .into_iter()
                    .map(|config| Some(ComputeCommand::CreateTimely { config, epoch }))
                    .collect()
            }
            command @ ComputeCommand::UpdateConfiguration(_) => {
                vec![Some(command); self.parts]
            }
            command => {
                let mut r = vec![None; self.parts];
                r[0] = Some(command);
                r
            }
        }
    }

    fn absorb_response(
        &mut self,
        shard_id: usize,
        message: ComputeResponse<T>,
    ) -> Option<Result<ComputeResponse<T>, anyhow::Error>> {
        match message {
            ComputeResponse::FrontierUpper {
                id,
                upper: new_shard_upper,
            } => {
                // Initialize frontier tracking state for this collection, if necessary.
                if !self.uppers.contains_key(&id) {
                    self.start_frontier_tracking(id);
                }

                let (frontier, shard_frontiers) = self.uppers.get_mut(&id).unwrap();

                let old_upper = frontier.frontier().to_owned();
                let shard_upper = &mut shard_frontiers[shard_id];
                frontier.update_iter(shard_upper.iter().map(|t| (t.clone(), -1)));
                shard_upper.join_assign(&new_shard_upper);
                frontier.update_iter(shard_upper.iter().map(|t| (t.clone(), 1)));

                let new_upper = frontier.frontier();

                let result = if PartialOrder::less_than(&old_upper.borrow(), &new_upper) {
                    Some(Ok(ComputeResponse::FrontierUpper {
                        id,
                        upper: new_upper.to_owned(),
                    }))
                } else {
                    None
                };

                if new_upper.is_empty() {
                    // All shards have reported advancement to the empty frontier, so we do not
                    // expect further updates for this collection.
                    self.cease_frontier_tracking(id);
                }

                result
            }
            ComputeResponse::PeekResponse(uuid, response, otel_ctx) => {
                // Incorporate new peek responses; awaiting all responses.
                let entry = self
                    .peek_responses
                    .entry(uuid)
                    .or_insert_with(Default::default);
                let novel = entry.insert(shard_id, response);
                assert!(novel.is_none(), "Duplicate peek response");
                // We may be ready to respond.
                if entry.len() == self.parts {
                    let mut response = PeekResponse::Rows(Vec::new());
                    for (_part, r) in std::mem::take(entry).into_iter() {
                        response = match (response, r) {
                            (_, PeekResponse::Canceled) => PeekResponse::Canceled,
                            (PeekResponse::Canceled, _) => PeekResponse::Canceled,
                            (_, PeekResponse::Error(e)) => PeekResponse::Error(e),
                            (PeekResponse::Error(e), _) => PeekResponse::Error(e),
                            (PeekResponse::Rows(mut rows), PeekResponse::Rows(r)) => {
                                rows.extend(r.into_iter());

                                let total_size: u64 = rows
                                    .iter()
                                    // Note: if the type of count changes in the future to be a
                                    // signed integer, then we'll need to consolidate rows before
                                    // taking this summation.
                                    .map(|(row, count): &(Row, NonZeroUsize)| {
                                        let size = row
                                            .byte_len()
                                            .saturating_add(std::mem::size_of_val(count));
                                        u64::cast_from(size)
                                    })
                                    .sum();

                                if total_size > self.max_result_size {
                                    // Note: We match on this specific error message in tests
                                    // so it's important that nothing else returns the same
                                    // string.
                                    let err = format!(
                                        "total result exceeds max size of {}",
                                        ByteSize::b(self.max_result_size)
                                    );
                                    PeekResponse::Error(err)
                                } else {
                                    PeekResponse::Rows(rows)
                                }
                            }
                        };
                    }
                    self.peek_responses.remove(&uuid);
                    // We take the otel_ctx from the last peek, but they should all be the same
                    Some(Ok(ComputeResponse::PeekResponse(uuid, response, otel_ctx)))
                } else {
                    None
                }
            }
            ComputeResponse::SubscribeResponse(id, response) => {
                // Initialize tracking for this subscribe, if necessary.
                let entry = self
                    .pending_subscribes
                    .entry(id)
                    .or_insert_with(|| PendingSubscribe::new(self.parts));

                let emit_response = match response {
                    SubscribeResponse::Batch(batch) => {
                        let frontiers = &mut entry.frontiers;
                        let old_frontier = frontiers.frontier().to_owned();
                        frontiers.update_iter(batch.lower.into_iter().map(|t| (t, -1)));
                        frontiers.update_iter(batch.upper.into_iter().map(|t| (t, 1)));
                        let new_frontier = frontiers.frontier().to_owned();

                        match (&mut entry.stashed_updates, batch.updates) {
                            (Err(_), _) => {
                                // Subscribe is borked; nothing to do.
                                // TODO: Consider refreshing error?
                            }
                            (_, Err(text)) => {
                                entry.stashed_updates = Err(text);
                            }
                            (Ok(stashed_updates), Ok(updates)) => {
                                stashed_updates.extend(updates);
                            }
                        }

                        // If the frontier has advanced, it is time to announce subscribe progress.
                        // Unless we have already announced that the subscribe has been dropped, in
                        // which case we must keep quiet.
                        if old_frontier != new_frontier && !entry.dropped {
                            let updates = match &mut entry.stashed_updates {
                                Ok(stashed_updates) => {
                                    // The compute protocol requires us to only send out
                                    // consolidated batches.
                                    consolidate_updates(stashed_updates);

                                    let mut ship = Vec::new();
                                    let mut keep = Vec::new();
                                    for (time, data, diff) in stashed_updates.drain(..) {
                                        if new_frontier.less_equal(&time) {
                                            keep.push((time, data, diff));
                                        } else {
                                            ship.push((time, data, diff));
                                        }
                                    }
                                    entry.stashed_updates = Ok(keep);
                                    Ok(ship)
                                }
                                Err(text) => Err(text.clone()),
                            };
                            Some(Ok(ComputeResponse::SubscribeResponse(
                                id,
                                SubscribeResponse::Batch(SubscribeBatch {
                                    lower: old_frontier,
                                    upper: new_frontier,
                                    updates,
                                }),
                            )))
                        } else {
                            None
                        }
                    }
                    SubscribeResponse::DroppedAt(frontier) => {
                        entry
                            .frontiers
                            .update_iter(frontier.iter().map(|t| (t.clone(), -1)));

                        if entry.dropped {
                            None
                        } else {
                            entry.dropped = true;
                            Some(Ok(ComputeResponse::SubscribeResponse(
                                id,
                                SubscribeResponse::DroppedAt(frontier),
                            )))
                        }
                    }
                };

                if entry.frontiers.frontier().is_empty() {
                    // All shards have reported advancement to the empty frontier or dropping, so
                    // we do not expect further updates for this subscribe.
                    self.pending_subscribes.remove(&id);
                }

                emit_response
            }
            ComputeResponse::CopyToResponse(id, response) => {
                // Incorporate new copy to responses; awaiting all responses.
                let entry = self
                    .copy_to_responses
                    .entry(id)
                    .or_insert_with(Default::default);
                let novel = entry.insert(shard_id, response);
                assert!(novel.is_none(), "Duplicate copy to response");
                // We may be ready to respond.
                if entry.len() == self.parts {
                    let mut response = CopyToResponse::RowCount(0);
                    for (_part, r) in std::mem::take(entry).into_iter() {
                        response = match (response, r) {
                            // It's important that we receive all the `Dropped` messages as well
                            // so that the `copy_to_responses` state can be cleared.
                            (_, CopyToResponse::Dropped) => CopyToResponse::Dropped,
                            (CopyToResponse::Dropped, _) => CopyToResponse::Dropped,
                            (_, CopyToResponse::Error(e)) => CopyToResponse::Error(e),
                            (CopyToResponse::Error(e), _) => CopyToResponse::Error(e),
                            (CopyToResponse::RowCount(r1), CopyToResponse::RowCount(r2)) => {
                                CopyToResponse::RowCount(r1 + r2)
                            }
                        };
                    }
                    self.copy_to_responses.remove(&id);
                    Some(Ok(ComputeResponse::CopyToResponse(id, response)))
                } else {
                    None
                }
            }
            response @ ComputeResponse::Status(_) => {
                // Pass through status responses.
                Some(Ok(response))
            }
        }
    }
}

#[derive(Debug)]
struct PendingSubscribe<T> {
    /// The subscribe frontiers of the partitioned shards.
    frontiers: MutableAntichain<T>,
    /// The updates we are holding back until their timestamps are complete.
    stashed_updates: Result<Vec<(T, Row, Diff)>, String>,
    /// Whether we have already emitted a `DroppedAt` response for this subscribe.
    ///
    /// This field is used to ensure we emit such a response only once.
    dropped: bool,
}

impl<T: timely::progress::Timestamp> PendingSubscribe<T> {
    fn new(parts: usize) -> Self {
        let mut frontiers = MutableAntichain::new();
        // TODO(benesch): fix this dangerous use of `as`.
        #[allow(clippy::as_conversions)]
        frontiers.update_iter([(T::minimum(), parts as i64)]);

        Self {
            frontiers,
            stashed_updates: Ok(Vec::new()),
            dropped: false,
        }
    }
}
