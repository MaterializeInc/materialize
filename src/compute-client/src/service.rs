// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute layer client and server.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::mem;

use async_trait::async_trait;
use bytesize::ByteSize;
use differential_dataflow::lattice::Lattice;
use mz_expr::row::RowCollection;
use mz_ore::cast::{CastFrom, CastInto};
use mz_ore::soft_panic_or_log;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::{GlobalId, Timestamp, UpdateCollection};
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use timely::PartialOrder;
use timely::progress::frontier::{Antichain, MutableAntichain};
use uuid::Uuid;

use crate::protocol::command::ComputeCommand;
use crate::protocol::response::{
    BatchedSubscribeBatch, BatchedSubscribeResponse, ComputeResponse, CopyToResponse,
    FrontiersResponse, PeekError, PeekResponse, Share, StashedPeekResponse, SubscribeBatch,
    SubscribeResponse,
};

/// A client to a compute server.
pub trait ComputeClient: GenericClient<ComputeCommand, ComputeResponse> {}

impl<C> ComputeClient for C where C: GenericClient<ComputeCommand, ComputeResponse> {}

#[async_trait]
impl GenericClient<ComputeCommand, ComputeResponse> for Box<dyn ComputeClient> {
    async fn send(&mut self, cmd: ComputeCommand) -> Result<(), anyhow::Error> {
        (**self).send(cmd).await
    }

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> Result<Option<ComputeResponse>, anyhow::Error> {
        // `GenericClient::recv` is required to be cancel safe.
        (**self).recv().await
    }
}

/// Maintained state for partitioned compute clients.
///
/// This helper type unifies the responses of multiple partitioned workers in order to present as a
/// single worker:
///
///   * It emits `Frontiers` responses reporting the minimum/meet of frontiers reported by the
///     individual workers.
///   * It emits `PeekResponse`s and `SubscribeResponse`s reporting the union of the responses
///     received from the workers.
///
/// In the compute communication stack, this client is instantiated several times:
///
///   * One instance on the controller side, dispatching between cluster processes.
///   * One instance in each cluster process, dispatching between timely worker threads.
///
/// Note that because compute commands, except `Hello` and `UpdateConfiguration`, are only
/// sent to the first process, the cluster-side instances of `PartitionedComputeState` are not
/// guaranteed to see all compute commands. Or more specifically: The instance running inside
/// process 0 sees all commands, whereas the instances running inside the other processes only see
/// `Hello` and `UpdateConfiguration`. The `PartitionedComputeState` implementation must be
/// able to cope with this limited visibility. It does so by performing most of its state management
/// based on observed compute responses rather than commands.
#[derive(Debug)]
pub struct PartitionedComputeState {
    /// Number of partitions the state machine represents.
    parts: usize,
    /// The maximum result size this state machine can return.
    ///
    /// This is updated upon receiving [`ComputeCommand::UpdateConfiguration`]s.
    max_result_size: u64,
    /// Tracked frontiers for indexes and sinks.
    ///
    /// Frontier tracking for a collection is initialized when the first `Frontiers` response
    /// for that collection is received. Frontier tracking is ceased when all shards have reported
    /// advancement to the empty frontier for all frontier kinds.
    ///
    /// The compute protocol requires that shards always emit `Frontiers` responses reporting empty
    /// frontiers for all frontier kinds when a collection is dropped. It further requires that no
    /// further `Frontier` responses are emitted for a collection after the empty frontiers were
    /// reported. These properties ensure that a) we always cease frontier tracking for collections
    /// that have been dropped and b) frontier tracking for a collection is not re-initialized
    /// after it was ceased.
    frontiers: BTreeMap<GlobalId, TrackedFrontiers>,
    /// For each in-progress peek the response data received so far, and the set of shards that
    /// provided responses already.
    ///
    /// Tracking of responses for a peek is initialized when the first `PeekResponse` for that peek
    /// is received. Once all shards have provided a `PeekResponse`, a unified peek response is
    /// emitted and the peek tracking state is dropped again.
    ///
    /// The compute protocol requires that exactly one response is emitted for each peek. This
    /// property ensures that a) we can eventually drop the tracking state maintained for a peek
    /// and b) we won't re-initialize tracking for a peek we have already served.
    peek_responses: BTreeMap<Uuid, PendingPeek>,
    /// For each in-progress copy-to the response data received so far, and the set of shards that
    /// provided responses already.
    ///
    /// Tracking of responses for a COPY TO is initialized when the first `CopyResponse` for that command
    /// is received. Once all shards have provided a `CopyResponse`, a unified copy response is
    /// emitted and the copy_to tracking state is dropped again.
    ///
    /// The compute protocol requires that exactly one response is emitted for each COPY TO command. This
    /// property ensures that a) we can eventually drop the tracking state maintained for a copy
    /// and b) we won't re-initialize tracking for a copy we have already served.
    copy_to_responses: BTreeMap<GlobalId, (CopyToResponse, BTreeSet<usize>)>,
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
    pending_subscribes: BTreeMap<GlobalId, PendingSubscribe>,
    /// Tracks in-progress batched subscribes, and the descriptions not every shard has reported
    /// yet.
    ///
    /// Tracking of a batched subscribe is initialized when the first `BatchedSubscribeResponse`
    /// for it is received. Once every shard has ended its output, with a `Batch` reporting the
    /// empty upper or with a `DroppedAt`, and the merged end was emitted, the tracking state is
    /// dropped again. The compute protocol forbids responses after a shard's end, so tracking is
    /// not re-initialized for a subscribe that has shut down.
    pending_batched_subscribes: BTreeMap<GlobalId, PendingBatchedSubscribe>,
}

impl Partitionable<ComputeCommand, ComputeResponse> for (ComputeCommand, ComputeResponse) {
    type PartitionedState = PartitionedComputeState;

    fn new(parts: usize) -> PartitionedComputeState {
        PartitionedComputeState {
            parts,
            max_result_size: u64::MAX,
            frontiers: BTreeMap::new(),
            peek_responses: BTreeMap::new(),
            pending_subscribes: BTreeMap::new(),
            pending_batched_subscribes: BTreeMap::new(),
            copy_to_responses: BTreeMap::new(),
        }
    }
}

impl PartitionedComputeState {
    /// Observes commands that move past.
    pub fn observe_command(&mut self, command: &ComputeCommand) {
        match command {
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

    /// Absorb a [`ComputeResponse::Frontiers`].
    fn absorb_frontiers(
        &mut self,
        shard_id: usize,
        collection_id: GlobalId,
        frontiers: FrontiersResponse,
    ) -> Option<ComputeResponse> {
        let tracked = self
            .frontiers
            .entry(collection_id)
            .or_insert_with(|| TrackedFrontiers::new(self.parts));

        let write_frontier = frontiers
            .write_frontier
            .and_then(|f| tracked.update_write_frontier(shard_id, &f));
        let input_frontier = frontiers
            .input_frontier
            .and_then(|f| tracked.update_input_frontier(shard_id, &f));
        let output_frontier = frontiers
            .output_frontier
            .and_then(|f| tracked.update_output_frontier(shard_id, &f));

        let frontiers = FrontiersResponse {
            write_frontier,
            input_frontier,
            output_frontier,
        };
        let result = frontiers
            .has_updates()
            .then_some(ComputeResponse::Frontiers(collection_id, frontiers));

        if tracked.all_empty() {
            // All shards have reported advancement to the empty frontier, so we do not
            // expect further updates for this collection.
            self.frontiers.remove(&collection_id);
        }

        result
    }

    /// Absorb a [`ComputeResponse::PeekResponse`].
    fn absorb_peek_response(
        &mut self,
        shard_id: usize,
        uuid: Uuid,
        response: PeekResponse,
        otel_ctx: OpenTelemetryContext,
    ) -> Option<ComputeResponse> {
        let pending = self
            .peek_responses
            .entry(uuid)
            .or_insert_with(PendingPeek::new);
        pending.absorb(shard_id, response, self.max_result_size);

        if pending.ready_shards.len() == self.parts {
            let response = self.peek_responses.remove(&uuid).unwrap().response;
            Some(ComputeResponse::PeekResponse(uuid, response, otel_ctx))
        } else {
            None
        }
    }

    /// Absorb a [`ComputeResponse::CopyToResponse`].
    fn absorb_copy_to_response(
        &mut self,
        shard_id: usize,
        copyto_id: GlobalId,
        response: CopyToResponse,
    ) -> Option<ComputeResponse> {
        use CopyToResponse::*;

        let (merged, ready_shards) = self
            .copy_to_responses
            .entry(copyto_id)
            .or_insert((CopyToResponse::RowCount(0), BTreeSet::new()));

        let first = ready_shards.insert(shard_id);
        assert!(first, "duplicate copy-to response");

        let resp1 = mem::replace(merged, Dropped);
        *merged = match (resp1, response) {
            (Dropped, _) | (_, Dropped) => Dropped,
            (Error(e), _) | (_, Error(e)) => Error(e),
            (RowCount(r1), RowCount(r2)) => RowCount(r1 + r2),
        };

        if ready_shards.len() == self.parts {
            let (response, _) = self.copy_to_responses.remove(&copyto_id).unwrap();
            Some(ComputeResponse::CopyToResponse(copyto_id, response))
        } else {
            None
        }
    }

    /// Absorb a [`ComputeResponse::SubscribeResponse`].
    fn absorb_batched_subscribe_response(
        &mut self,
        shard_id: usize,
        subscribe_id: GlobalId,
        response: BatchedSubscribeResponse,
    ) -> Option<ComputeResponse> {
        let tracked = self
            .pending_batched_subscribes
            .entry(subscribe_id)
            .or_insert_with(|| PendingBatchedSubscribe::new(self.parts));
        let emitted = tracked.absorb(shard_id, response, self.max_result_size);
        if tracked.is_done() {
            self.pending_batched_subscribes.remove(&subscribe_id);
        }
        emitted.map(|response| {
            ComputeResponse::BatchedSubscribeResponse(subscribe_id, Box::new(response))
        })
    }

    fn absorb_subscribe_response(
        &mut self,
        subscribe_id: GlobalId,
        response: SubscribeResponse,
    ) -> Option<ComputeResponse> {
        let tracked = self
            .pending_subscribes
            .entry(subscribe_id)
            .or_insert_with(|| PendingSubscribe::new(self.parts));

        let emit_response = match response {
            SubscribeResponse::Batch(batch) => {
                let frontiers = &mut tracked.frontiers;
                let old_frontier = frontiers.frontier().to_owned();
                frontiers.update_iter(batch.lower.into_iter().map(|t| (t, -1)));
                frontiers.update_iter(batch.upper.into_iter().map(|t| (t, 1)));
                let new_frontier = frontiers.frontier().to_owned();

                tracked.stash(batch.updates, self.max_result_size);

                // If the frontier has advanced, it is time to announce subscribe progress. Unless
                // we have already announced that the subscribe has been dropped, in which case we
                // must keep quiet.
                if old_frontier != new_frontier && !tracked.dropped {
                    let updates = match &mut tracked.stashed_updates {
                        Ok(stashed_updates) => {
                            // Split each collection along the frontier, passing the prefix along.
                            let mut ship = vec![];
                            let mut keep = vec![];
                            for collection in stashed_updates.drain(..) {
                                let partition_point = collection
                                    .times()
                                    .partition_point(|t| !new_frontier.less_equal(t));
                                let (ship_coll, keep_coll) = collection.split_at(partition_point);
                                if ship_coll.len() > 0 {
                                    ship.push(ship_coll);
                                }
                                if keep_coll.len() > 0 {
                                    keep.push(keep_coll);
                                }
                            }
                            tracked.stashed_result_size = keep.iter().map(|c| c.byte_len()).sum();
                            tracked.stashed_updates = Ok(keep);
                            Ok(ship)
                        }
                        Err(text) => Err(text.clone()),
                    };
                    Some(ComputeResponse::SubscribeResponse(
                        subscribe_id,
                        SubscribeResponse::Batch(SubscribeBatch {
                            lower: old_frontier,
                            upper: new_frontier,
                            updates,
                        }),
                    ))
                } else {
                    None
                }
            }
            SubscribeResponse::DroppedAt(frontier) => {
                tracked
                    .frontiers
                    .update_iter(frontier.iter().map(|t| (t.clone(), -1)));

                if tracked.dropped {
                    None
                } else {
                    tracked.dropped = true;
                    Some(ComputeResponse::SubscribeResponse(
                        subscribe_id,
                        SubscribeResponse::DroppedAt(frontier),
                    ))
                }
            }
        };

        if tracked.frontiers.frontier().is_empty() {
            // All shards have reported advancement to the empty frontier or dropping, so
            // we do not expect further updates for this subscribe.
            self.pending_subscribes.remove(&subscribe_id);
        }

        emit_response
    }
}

impl PartitionedState<ComputeCommand, ComputeResponse> for PartitionedComputeState {
    fn split_command(&mut self, command: ComputeCommand) -> Vec<Option<ComputeCommand>> {
        self.observe_command(&command);

        // As specified by the compute protocol:
        //  * Forward `Hello` and `UpdateConfiguration` commands to all shards.
        //  * Forward all other commands to the first shard only.
        match command {
            command @ ComputeCommand::Hello { .. }
            | command @ ComputeCommand::UpdateConfiguration(_) => {
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
        message: ComputeResponse,
    ) -> Option<Result<ComputeResponse, anyhow::Error>> {
        let response = match message {
            ComputeResponse::Frontiers(id, frontiers) => {
                self.absorb_frontiers(shard_id, id, frontiers)
            }
            ComputeResponse::PeekResponse(uuid, response, otel_ctx) => {
                self.absorb_peek_response(shard_id, uuid, response, otel_ctx)
            }
            ComputeResponse::SubscribeResponse(id, response) => {
                self.absorb_subscribe_response(id, response)
            }
            ComputeResponse::CopyToResponse(id, response) => {
                self.absorb_copy_to_response(shard_id, id, response)
            }
            ComputeResponse::BatchedSubscribeResponse(id, response) => {
                self.absorb_batched_subscribe_response(shard_id, id, *response)
            }
            response @ ComputeResponse::Status(_) => {
                // Pass through status responses.
                Some(response)
            }
        };

        response.map(Ok)
    }
}

/// Tracked frontiers for an index or a sink collection.
///
/// Each frontier is maintained both as a `MutableAntichain` across all partitions and individually
/// for each partition.
#[derive(Debug)]
struct TrackedFrontiers {
    /// The tracked write frontier.
    write_frontier: (MutableAntichain<Timestamp>, Vec<Antichain<Timestamp>>),
    /// The tracked input frontier.
    input_frontier: (MutableAntichain<Timestamp>, Vec<Antichain<Timestamp>>),
    /// The tracked output frontier.
    output_frontier: (MutableAntichain<Timestamp>, Vec<Antichain<Timestamp>>),
}

impl TrackedFrontiers {
    /// Initializes frontier tracking state for a new collection.
    fn new(parts: usize) -> Self {
        // TODO(benesch): fix this dangerous use of `as`.
        #[allow(clippy::as_conversions)]
        let parts_diff = parts as i64;

        let mut frontier = MutableAntichain::new();
        frontier.update_iter([(Timestamp::MIN, parts_diff)]);
        let part_frontiers = vec![Antichain::from_elem(Timestamp::MIN); parts];
        let frontier_entry = (frontier, part_frontiers);

        Self {
            write_frontier: frontier_entry.clone(),
            input_frontier: frontier_entry.clone(),
            output_frontier: frontier_entry,
        }
    }

    /// Returns whether all tracked frontiers have advanced to the empty frontier.
    fn all_empty(&self) -> bool {
        self.write_frontier.0.frontier().is_empty()
            && self.input_frontier.0.frontier().is_empty()
            && self.output_frontier.0.frontier().is_empty()
    }

    /// Updates write frontier tracking with a new shard frontier.
    ///
    /// If this causes the global write frontier to advance, the advanced frontier is returned.
    fn update_write_frontier(
        &mut self,
        shard_id: usize,
        new_shard_frontier: &Antichain<Timestamp>,
    ) -> Option<Antichain<Timestamp>> {
        Self::update_frontier(&mut self.write_frontier, shard_id, new_shard_frontier)
    }

    /// Updates input frontier tracking with a new shard frontier.
    ///
    /// If this causes the global input frontier to advance, the advanced frontier is returned.
    fn update_input_frontier(
        &mut self,
        shard_id: usize,
        new_shard_frontier: &Antichain<Timestamp>,
    ) -> Option<Antichain<Timestamp>> {
        Self::update_frontier(&mut self.input_frontier, shard_id, new_shard_frontier)
    }

    /// Updates output frontier tracking with a new shard frontier.
    ///
    /// If this causes the global output frontier to advance, the advanced frontier is returned.
    fn update_output_frontier(
        &mut self,
        shard_id: usize,
        new_shard_frontier: &Antichain<Timestamp>,
    ) -> Option<Antichain<Timestamp>> {
        Self::update_frontier(&mut self.output_frontier, shard_id, new_shard_frontier)
    }

    /// Updates the provided frontier entry with a new shard frontier.
    fn update_frontier(
        entry: &mut (MutableAntichain<Timestamp>, Vec<Antichain<Timestamp>>),
        shard_id: usize,
        new_shard_frontier: &Antichain<Timestamp>,
    ) -> Option<Antichain<Timestamp>> {
        let (frontier, shard_frontiers) = entry;

        let old_frontier = frontier.frontier().to_owned();
        let shard_frontier = &mut shard_frontiers[shard_id];
        frontier.update_iter(shard_frontier.iter().map(|t| (t.clone(), -1)));
        shard_frontier.join_assign(new_shard_frontier);
        frontier.update_iter(shard_frontier.iter().map(|t| (t.clone(), 1)));

        let new_frontier = frontier.frontier();

        if PartialOrder::less_than(&old_frontier.borrow(), &new_frontier) {
            Some(new_frontier.to_owned())
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct PendingSubscribe {
    /// The subscribe frontiers of the partitioned shards.
    frontiers: MutableAntichain<Timestamp>,
    /// The updates we are holding back until their timestamps are complete.
    stashed_updates: Result<Vec<UpdateCollection>, String>,
    /// The row size of stashed updates, for `max_result_size` checking.
    stashed_result_size: usize,
    /// Whether we have already emitted a `DroppedAt` response for this subscribe.
    ///
    /// This field is used to ensure we emit such a response only once.
    dropped: bool,
}

impl PendingSubscribe {
    fn new(parts: usize) -> Self {
        let mut frontiers = MutableAntichain::new();
        // TODO(benesch): fix this dangerous use of `as`.
        #[allow(clippy::as_conversions)]
        frontiers.update_iter([(Timestamp::MIN, parts as i64)]);

        Self {
            frontiers,
            stashed_updates: Ok(Vec::new()),
            stashed_result_size: 0,
            dropped: false,
        }
    }

    /// Stash a new batch of updates.
    ///
    /// This also implements the short-circuit behavior of error responses, and performs
    /// `max_result_size` checking.
    fn stash(&mut self, new_updates: Result<Vec<UpdateCollection>, String>, max_result_size: u64) {
        match (&mut self.stashed_updates, new_updates) {
            (Err(_), _) => {
                // Subscribe is borked; nothing to do.
                // TODO: Consider refreshing error?
            }
            (_, Err(text)) => {
                self.stashed_updates = Err(text);
            }
            (Ok(stashed), Ok(new)) => {
                let new_size: usize = new.iter().map(|coll| coll.byte_len()).sum();
                self.stashed_result_size += new_size;

                if self.stashed_result_size > max_result_size.cast_into() {
                    self.stashed_updates = Err(format!(
                        "total result exceeds max size of {}",
                        ByteSize::b(max_result_size)
                    ));
                } else {
                    stashed.extend(new);
                }
            }
        }
    }
}

/// Assembles the output of a batched subscribe from the responses of every part.
///
/// Part `p`'s k-th batch reports the k-th description. A description is emitted once every part
/// has reported it, with the shares of all parts concatenated and the first error kept.
/// Descriptions are emitted in order. Each input adds to one description and only the oldest
/// description can be emitted, so each input emits at most one response.
#[derive(Debug)]
struct PendingBatchedSubscribe {
    /// The number of parts.
    parts: usize,
    /// The descriptions that have not been emitted, oldest first.
    descriptions: VecDeque<PendingDescription>,
    /// For each part, the index into `descriptions` of the next description it reports.
    next: Vec<usize>,
    /// The parts that have ended their output.
    ended: BTreeSet<usize>,
    /// Whether a part ended its output with `DroppedAt`.
    dropped: bool,
    /// The inline bytes of the shares in `descriptions`, for `max_result_size` checking.
    inline_bytes: usize,
    /// The orphans that parts forwarded with their `DroppedAt`.
    orphans: Vec<Share>,
}

/// A description that not every part has reported yet.
#[derive(Debug)]
struct PendingDescription {
    /// The lower frontier, as reported by the first part.
    lower: Antichain<Timestamp>,
    /// The upper frontier, as reported by the first part.
    upper: Antichain<Timestamp>,
    /// The shares of the parts that have reported the description.
    shares: Vec<Share>,
    /// The first error reported for the description.
    error: Option<String>,
    /// The number of parts that have reported the description.
    reported: usize,
}

impl PendingBatchedSubscribe {
    fn new(parts: usize) -> Self {
        Self {
            parts,
            descriptions: VecDeque::new(),
            next: vec![0; parts],
            ended: BTreeSet::new(),
            dropped: false,
            inline_bytes: 0,
            orphans: Vec::new(),
        }
    }

    /// Absorbs `response` from `part` and returns the merged response it completes, if any.
    ///
    /// Once every part has ended and a part dropped, the merge emits one `DroppedAt` whose orphans
    /// are the shares of every description not emitted, together with the orphans the parts
    /// forwarded.
    fn absorb(
        &mut self,
        part: usize,
        response: BatchedSubscribeResponse,
        max_result_size: u64,
    ) -> Option<BatchedSubscribeResponse> {
        if self.ended.contains(&part) {
            soft_panic_or_log!("batched subscribe response from part {part} after its end");
            return None;
        }

        let reported_frontier = match response {
            BatchedSubscribeResponse::Batch(batch) => {
                let upper = batch.upper.clone();
                self.absorb_batch(part, batch);
                upper
            }
            BatchedSubscribeResponse::DroppedAt { frontier, orphans } => {
                self.ended.insert(part);
                self.dropped = true;
                self.orphans.extend(orphans);
                frontier
            }
        };

        if self.ended.len() == self.parts {
            // Completion is the oldest description, reported by every part with an empty upper.
            // Anything else left over can only follow a drop or a disagreement on descriptions,
            // and its shares are orphaned. Emitting the oldest description as well would emit
            // two responses for one input.
            let completes = !self.dropped
                && self.descriptions.len() == 1
                && self.descriptions[0].reported == self.parts;
            if !completes {
                return Some(self.drop_pending(reported_frontier));
            }
        }

        self.emit_oldest(max_result_size)
    }

    fn absorb_batch(&mut self, part: usize, batch: BatchedSubscribeBatch) {
        let index = self.next[part];
        self.next[part] += 1;
        if index == self.descriptions.len() {
            self.descriptions.push_back(PendingDescription {
                lower: batch.lower.clone(),
                upper: batch.upper.clone(),
                shares: Vec::new(),
                error: None,
                reported: 0,
            });
        }

        let description = &mut self.descriptions[index];
        if description.lower != batch.lower || description.upper != batch.upper {
            description
                .error
                .get_or_insert_with(|| "replica processes disagree on batch descriptions".into());
        }
        if description.error.is_none() {
            description.error = batch.error;
        }
        self.inline_bytes += batch
            .shares
            .iter()
            .map(Share::inline_byte_len)
            .sum::<usize>();
        description.shares.extend(batch.shares);
        description.reported += 1;

        if batch.upper.is_empty() {
            self.ended.insert(part);
        }
    }

    /// Emits the oldest description if every part has reported it.
    fn emit_oldest(&mut self, max_result_size: u64) -> Option<BatchedSubscribeResponse> {
        if self.descriptions.front()?.reported < self.parts {
            return None;
        }
        let mut description = self.descriptions.pop_front().expect("checked above");
        for next in &mut self.next {
            *next -= 1;
        }

        // The limit covers every held share, including those of descriptions not yet complete.
        if description.error.is_none() && u64::cast_from(self.inline_bytes) > max_result_size {
            description.error = Some(format!(
                "total result exceeds max size of {}",
                ByteSize::b(max_result_size)
            ));
        }
        self.inline_bytes -= description
            .shares
            .iter()
            .map(Share::inline_byte_len)
            .sum::<usize>();

        Some(BatchedSubscribeResponse::Batch(BatchedSubscribeBatch {
            lower: description.lower,
            upper: description.upper,
            shares: description.shares,
            error: description.error,
        }))
    }

    /// Orphans every description not emitted and returns the merged `DroppedAt`.
    ///
    /// The frontier is the lower of the oldest description not emitted, which is the upper of the
    /// last one emitted. Without such a description, `reported_frontier` is the frontier of the
    /// part that ended last.
    fn drop_pending(
        &mut self,
        reported_frontier: Antichain<Timestamp>,
    ) -> BatchedSubscribeResponse {
        let frontier = match self.descriptions.front() {
            Some(description) => description.lower.clone(),
            None => reported_frontier,
        };
        let mut orphans: Vec<_> = self
            .descriptions
            .drain(..)
            .flat_map(|description| description.shares)
            .collect();
        orphans.append(&mut self.orphans);
        self.inline_bytes = 0;
        BatchedSubscribeResponse::DroppedAt { frontier, orphans }
    }

    /// Whether every part has ended and the merged end was emitted.
    fn is_done(&self) -> bool {
        self.ended.len() == self.parts && self.descriptions.is_empty()
    }
}

/// Accumulates the per-worker responses to one peek into the single response the controller
/// hands upwards.
#[derive(Debug)]
struct PendingPeek {
    /// The responses merged so far.
    response: PeekResponse,
    /// Inline result bytes seen so far, across all shards.
    ///
    /// Tracked separately from `response` because a worker's rows are dropped as soon as any
    /// worker reports an error. Without this the aggregate size check would depend on the order
    /// the responses happen to arrive in.
    inline_byte_len: usize,
    /// The shards that have provided responses.
    ready_shards: BTreeSet<usize>,
}

impl PendingPeek {
    fn new() -> Self {
        Self {
            response: PeekResponse::Rows(vec![RowCollection::default()]),
            inline_byte_len: 0,
            ready_shards: BTreeSet::new(),
        }
    }

    fn absorb(&mut self, shard_id: usize, response: PeekResponse, max_result_size: u64) {
        let first = self.ready_shards.insert(shard_id);
        assert!(first, "duplicate peek response");

        self.inline_byte_len = self
            .inline_byte_len
            .saturating_add(response.inline_byte_len());
        let current = mem::replace(&mut self.response, PeekResponse::Canceled);
        self.response = merge_peek_responses(current, response);

        // Merging eagerly is what keeps the controller's memory bounded, so the size check has to
        // happen on every response rather than once at the end.
        if self.inline_byte_len > max_result_size.cast_into() {
            let error = PeekError::ResultExceedsMaxSize {
                max_result_size: max_result_size.cast_into(),
            };
            let current = mem::replace(&mut self.response, PeekResponse::Canceled);
            self.response = merge_peek_responses(current, PeekResponse::Error(error));
        }
    }
}

/// Merge two [`PeekResponse`]s.
fn merge_peek_responses(resp1: PeekResponse, resp2: PeekResponse) -> PeekResponse {
    use PeekResponse::*;

    // Cancelations and errors short-circuit. Cancelations take precedence over errors.
    //
    // TODO: every stashed response this merge drops leaks its batches in blob storage. That is a
    // stashed response overridden by a cancelation or an error here, both responses when their
    // shard ids or relation descs differ below, the stashed batches merged so far when
    // `PendingPeek::absorb` merges in `ResultExceedsMaxSize`, and the partial responses
    // `peek_responses` still holds when the partitioned state goes away. Deleting them needs a
    // persist client, which the merge does not have.
    let (resp1, resp2) = match (resp1, resp2) {
        (Canceled, _) | (_, Canceled) => return Canceled,
        (Error(e1), Error(e2)) => return Error(merge_peek_errors(e1, e2)),
        (Error(e), _) | (_, Error(e)) => return Error(e),
        resps => resps,
    };

    match (resp1, resp2) {
        (Rows(mut rows1), Rows(rows2)) => {
            rows1.extend(rows2);
            Rows(rows1)
        }
        (Rows(rows), Stashed(mut stashed)) | (Stashed(mut stashed), Rows(rows)) => {
            stashed.inline_rows.extend(rows);
            Stashed(stashed)
        }
        (Stashed(stashed1), Stashed(stashed2)) => {
            // Deconstruct so we don't miss adding new fields. We need to be careful about
            // merging everything!
            let StashedPeekResponse {
                num_rows_batches: num_rows_batches1,
                encoded_size_bytes: encoded_size_bytes1,
                relation_desc: relation_desc1,
                shard_id: shard_id1,
                batches: mut batches1,
                inline_rows: mut inline_rows1,
            } = *stashed1;
            let StashedPeekResponse {
                num_rows_batches: num_rows_batches2,
                encoded_size_bytes: encoded_size_bytes2,
                relation_desc: relation_desc2,
                shard_id: shard_id2,
                batches: mut batches2,
                inline_rows: inline_rows2,
            } = *stashed2;

            if shard_id1 != shard_id2 {
                soft_panic_or_log!(
                    "shard IDs of stashed responses do not match: \
                             {shard_id1} != {shard_id2}"
                );
                return Error(PeekError::unstructured("internal error"));
            }
            if relation_desc1 != relation_desc2 {
                soft_panic_or_log!(
                    "relation descs of stashed responses do not match: \
                             {relation_desc1:?} != {relation_desc2:?}"
                );
                return Error(PeekError::unstructured("internal error"));
            }

            batches1.append(&mut batches2);
            inline_rows1.extend(inline_rows2);

            Stashed(Box::new(StashedPeekResponse {
                num_rows_batches: num_rows_batches1 + num_rows_batches2,
                encoded_size_bytes: encoded_size_bytes1 + encoded_size_bytes2,
                relation_desc: relation_desc1,
                shard_id: shard_id1,
                batches: batches1,
                inline_rows: inline_rows1,
            }))
        }
        _ => unreachable!("handled above"),
    }
}

/// Merge two [`PeekError`]s into the one we report.
///
/// A row-iteration-limit error only wins against another one, and then the smaller limit wins so
/// the choice does not depend on which worker answered first. Any other error outranks it: a
/// query that both overran the limit and failed for a real reason should report the real reason.
fn merge_peek_errors(error1: PeekError, error2: PeekError) -> PeekError {
    match (error1, error2) {
        (
            PeekError::RowIterationLimitExceeded { limit: limit1 },
            PeekError::RowIterationLimitExceeded { limit: limit2 },
        ) => PeekError::RowIterationLimitExceeded {
            limit: limit1.min(limit2),
        },
        (PeekError::RowIterationLimitExceeded { .. }, error)
        | (error, PeekError::RowIterationLimitExceeded { .. }) => error,
        (error, _) => error,
    }
}

#[cfg(test)]
mod tests;
