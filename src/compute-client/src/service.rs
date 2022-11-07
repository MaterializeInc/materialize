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

use std::collections::HashMap;
use std::iter;

use async_trait::async_trait;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::lattice::Lattice;
use timely::progress::frontier::{Antichain, MutableAntichain};
use timely::PartialOrder;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

use mz_repr::{Diff, GlobalId, Row};
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use mz_service::grpc::{BidiProtoClient, ClientTransport, GrpcClient, GrpcServer, ResponseStream};

use crate::command::{CommunicationConfig, ComputeCommand, ProtoComputeCommand};
use crate::response::{
    ComputeResponse, PeekResponse, ProtoComputeResponse, SubscribeBatch, SubscribeResponse,
};
use crate::service::proto_compute_client::ProtoComputeClient;
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

pub type ComputeGrpcClient = GrpcClient<ProtoComputeClient<ClientTransport>>;

#[async_trait]
impl BidiProtoClient for ProtoComputeClient<ClientTransport> {
    type PC = ProtoComputeCommand;
    type PR = ProtoComputeResponse;

    fn new(inner: ClientTransport) -> Self {
        ProtoComputeClient::new(inner)
    }

    async fn establish_bidi_stream(
        &mut self,
        rx: UnboundedReceiverStream<Self::PC>,
    ) -> Result<Response<Streaming<Self::PR>>, Status> {
        self.command_response_stream(rx).await
    }
}

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
/// This helper type unifies the responses of multiple partitioned
/// workers in order to present as a single worker.
#[derive(Debug)]
pub struct PartitionedComputeState<T> {
    /// Number of partitions the state machine represents.
    parts: usize,
    /// Upper frontiers for indexes and sinks, both unioned across all partitions and from each
    /// individual partition.
    uppers: HashMap<GlobalId, (MutableAntichain<T>, Vec<Antichain<T>>)>,
    /// Pending responses for a peek; returnable once all are available.
    peek_responses: HashMap<Uuid, HashMap<usize, PeekResponse>>,
    /// Tracks in-progress `SUBSCRIBE`s, and the stashed rows we are holding
    /// back until their timestamps are complete.
    ///
    /// The updates may be `Err` if any of the batches have reported an error, in which case the
    /// subscribe is permanently borked.
    pending_subscribes:
        HashMap<GlobalId, Option<(MutableAntichain<T>, Result<Vec<(T, Row, Diff)>, String>)>>,
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
            uppers: HashMap::new(),
            peek_responses: HashMap::new(),
            pending_subscribes: HashMap::new(),
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
            uppers,
            peek_responses,
            pending_subscribes,
        } = self;
        uppers.clear();
        peek_responses.clear();
        pending_subscribes.clear();
    }

    /// Observes commands that move past, and prepares state for responses.
    pub fn observe_command(&mut self, command: &ComputeCommand<T>) {
        if let ComputeCommand::CreateTimely { .. } = command {
            self.reset();
        } else {
            // Note that we are not guaranteed to observe other compute commands than
            // `CreateTimely`. The `Partitioned` compute client is used by `computed` processes,
            // and in a multi-process replica only the first process receives all compute commands.
            // We should therefore not add any logic here that relies on observing commands other
            // than `CreateTimely`.
        }
    }

    fn start_frontier_tracking(&mut self, id: GlobalId) {
        let mut frontier = MutableAntichain::new();
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
        match command {
            ComputeCommand::CreateTimely { comm_config, epoch } => (0..self.parts)
                .into_iter()
                .map(|part| {
                    Some(ComputeCommand::CreateTimely {
                        comm_config: CommunicationConfig {
                            process: part,
                            ..comm_config.clone()
                        },
                        epoch,
                    })
                })
                .collect(),
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
            ComputeResponse::FrontierUppers(list) => {
                let mut new_uppers = Vec::new();

                for (id, new_shard_upper) in list {
                    // Initialize frontier tracking state for this collection, if necessary.
                    if !self.uppers.contains_key(&id) {
                        self.start_frontier_tracking(id);
                    }

                    let (frontier, shard_frontiers) = self.uppers.get_mut(&id).unwrap();

                    let old_upper = frontier.frontier().to_owned();
                    let shard_upper = &mut shard_frontiers[shard_id];
                    frontier.update_iter(shard_upper.iter().map(|t| (t.clone(), -1)));
                    frontier.update_iter(new_shard_upper.iter().map(|t| (t.clone(), 1)));
                    shard_upper.join_assign(&new_shard_upper);

                    let new_upper = frontier.frontier();
                    if PartialOrder::less_than(&old_upper.borrow(), &new_upper) {
                        new_uppers.push((id, new_upper.to_owned()));
                    }

                    if new_upper.is_empty() {
                        // All shards have reported advancement to the empty frontier, so we do not
                        // expect further updates for this collection.
                        self.cease_frontier_tracking(id);
                    }
                }

                if new_uppers.is_empty() {
                    None
                } else {
                    Some(Ok(ComputeResponse::FrontierUppers(new_uppers)))
                }
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
                                PeekResponse::Rows(rows)
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
                let maybe_entry = self.pending_subscribes.entry(id).or_insert_with(|| {
                    let mut frontier = MutableAntichain::new();
                    frontier.update_iter(std::iter::once((T::minimum(), self.parts as i64)));
                    Some((frontier, Ok(Vec::new())))
                });

                let entry = match maybe_entry {
                    None => {
                        // This subscribe has been dropped;
                        // we should permanently block
                        // any messages from it
                        return None;
                    }
                    Some(entry) => entry,
                };

                match response {
                    SubscribeResponse::Batch(SubscribeBatch {
                        lower,
                        upper,
                        mut updates,
                    }) => {
                        let old_frontier = entry.0.frontier().to_owned();
                        entry.0.update_iter(lower.iter().map(|t| (t.clone(), -1)));
                        entry.0.update_iter(upper.iter().map(|t| (t.clone(), 1)));
                        let new_frontier = entry.0.frontier().to_owned();
                        match (&mut entry.1, &mut updates) {
                            (Err(_), _) => {
                                // Subscribe is borked; nothing to do.
                                // TODO: Consider refreshing error?
                            }
                            (_, Err(text)) => {
                                entry.1 = Err(text.clone());
                            }
                            (Ok(stashed_updates), Ok(updates)) => {
                                stashed_updates.append(updates);
                            }
                        }

                        // If the frontier has advanced, it is time to announce a thing.
                        if old_frontier != new_frontier {
                            let updates = match &mut entry.1 {
                                Ok(stashed_updates) => {
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
                                    entry.1 = Ok(keep);
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
                        *maybe_entry = None;
                        Some(Ok(ComputeResponse::SubscribeResponse(
                            id,
                            SubscribeResponse::DroppedAt(frontier),
                        )))
                    }
                }
            }
        }
    }
}
