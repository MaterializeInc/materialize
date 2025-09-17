// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Tonic generates code that violates clippy lints.
// TODO: Remove this once tonic does not produce this code anymore.
#![allow(clippy::as_conversions, clippy::clone_on_ref_ptr)]

//! Compute layer client and server.

use std::collections::{BTreeMap, BTreeSet};
use std::mem;

use async_trait::async_trait;
use bytesize::ByteSize;
use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::lattice::Lattice;
use mz_expr::row::RowCollection;
use mz_ore::cast::CastInto;
use mz_ore::soft_panic_or_log;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::GlobalId;
use mz_service::client::{GenericClient, Partitionable, PartitionedState};
use timely::PartialOrder;
use timely::progress::frontier::{Antichain, MutableAntichain};
use uuid::Uuid;

use crate::controller::ComputeControllerTimestamp;
use crate::protocol::command::ComputeCommand;
use crate::protocol::response::{
    ComputeResponse, CopyToResponse, FrontiersResponse, PeekResponse, StashedPeekResponse,
    StashedSubscribeBatch, SubscribeBatch, SubscribeBatchContents, SubscribeResponse,
};

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

    /// # Cancel safety
    ///
    /// This method is cancel safe. If `recv` is used as the event in a [`tokio::select!`]
    /// statement and some other branch completes first, it is guaranteed that no messages were
    /// received by this client.
    async fn recv(&mut self) -> Result<Option<ComputeResponse<T>>, anyhow::Error> {
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
pub struct PartitionedComputeState<T> {
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
    frontiers: BTreeMap<GlobalId, TrackedFrontiers<T>>,
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
    peek_responses: BTreeMap<Uuid, (PeekResponse, BTreeSet<usize>)>,
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
    pending_subscribes: BTreeMap<GlobalId, PendingSubscribe<T>>,
}

impl<T> Partitionable<ComputeCommand<T>, ComputeResponse<T>>
    for (ComputeCommand<T>, ComputeResponse<T>)
where
    T: ComputeControllerTimestamp,
{
    type PartitionedState = PartitionedComputeState<T>;

    fn new(parts: usize) -> PartitionedComputeState<T> {
        PartitionedComputeState {
            parts,
            max_result_size: u64::MAX,
            frontiers: BTreeMap::new(),
            peek_responses: BTreeMap::new(),
            pending_subscribes: BTreeMap::new(),
            copy_to_responses: BTreeMap::new(),
        }
    }
}

impl<T> PartitionedComputeState<T>
where
    T: ComputeControllerTimestamp,
{
    /// Observes commands that move past.
    pub fn observe_command(&mut self, command: &ComputeCommand<T>) {
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
        frontiers: FrontiersResponse<T>,
    ) -> Option<ComputeResponse<T>> {
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
    ) -> Option<ComputeResponse<T>> {
        let (merged, ready_shards) = self.peek_responses.entry(uuid).or_insert((
            PeekResponse::Rows(RowCollection::default()),
            BTreeSet::new(),
        ));

        let first = ready_shards.insert(shard_id);
        assert!(first, "duplicate peek response");

        let resp1 = mem::replace(merged, PeekResponse::Canceled);
        *merged = merge_peek_responses(resp1, response, self.max_result_size);

        if ready_shards.len() == self.parts {
            let (response, _) = self.peek_responses.remove(&uuid).unwrap();
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
    ) -> Option<ComputeResponse<T>> {
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
    fn absorb_subscribe_response(
        &mut self,
        subscribe_id: GlobalId,
        response: SubscribeResponse<T>,
    ) -> Option<ComputeResponse<T>> {
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
                    let stashed = std::mem::replace(
                        &mut tracked.stashed_updates,
                        SubscribeBatchContents::Updates(Vec::new()),
                    );
                    let updates = match stashed {
                        SubscribeBatchContents::Updates(mut stashed_updates) => {
                            // The compute protocol requires us to only send out consolidated
                            // batches.
                            consolidate_updates(&mut stashed_updates);

                            let mut ship = Vec::new();
                            let mut keep = Vec::new();
                            for (time, data, diff) in stashed_updates.drain(..) {
                                if new_frontier.less_equal(&time) {
                                    keep.push((time, data, diff));
                                } else {
                                    ship.push((time, data, diff));
                                }
                            }
                            tracked.stashed_updates = SubscribeBatchContents::Updates(keep);
                            SubscribeBatchContents::Updates(ship)
                        }
                        SubscribeBatchContents::Stashed(stashed) => {
                            tracing::info!(?stashed.num_rows_batches, "yielding stashed batches");
                            SubscribeBatchContents::Stashed(stashed)
                        }
                        SubscribeBatchContents::Error(text) => {
                            SubscribeBatchContents::Error(text.clone())
                        }
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

impl<T> PartitionedState<ComputeCommand<T>, ComputeResponse<T>> for PartitionedComputeState<T>
where
    T: ComputeControllerTimestamp,
{
    fn split_command(&mut self, command: ComputeCommand<T>) -> Vec<Option<ComputeCommand<T>>> {
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
        message: ComputeResponse<T>,
    ) -> Option<Result<ComputeResponse<T>, anyhow::Error>> {
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
struct TrackedFrontiers<T> {
    /// The tracked write frontier.
    write_frontier: (MutableAntichain<T>, Vec<Antichain<T>>),
    /// The tracked input frontier.
    input_frontier: (MutableAntichain<T>, Vec<Antichain<T>>),
    /// The tracked output frontier.
    output_frontier: (MutableAntichain<T>, Vec<Antichain<T>>),
}

impl<T> TrackedFrontiers<T>
where
    T: timely::progress::Timestamp + Lattice,
{
    /// Initializes frontier tracking state for a new collection.
    fn new(parts: usize) -> Self {
        // TODO(benesch): fix this dangerous use of `as`.
        #[allow(clippy::as_conversions)]
        let parts_diff = parts as i64;

        let mut frontier = MutableAntichain::new();
        frontier.update_iter([(T::minimum(), parts_diff)]);
        let part_frontiers = vec![Antichain::from_elem(T::minimum()); parts];
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
        new_shard_frontier: &Antichain<T>,
    ) -> Option<Antichain<T>> {
        Self::update_frontier(&mut self.write_frontier, shard_id, new_shard_frontier)
    }

    /// Updates input frontier tracking with a new shard frontier.
    ///
    /// If this causes the global input frontier to advance, the advanced frontier is returned.
    fn update_input_frontier(
        &mut self,
        shard_id: usize,
        new_shard_frontier: &Antichain<T>,
    ) -> Option<Antichain<T>> {
        Self::update_frontier(&mut self.input_frontier, shard_id, new_shard_frontier)
    }

    /// Updates output frontier tracking with a new shard frontier.
    ///
    /// If this causes the global output frontier to advance, the advanced frontier is returned.
    fn update_output_frontier(
        &mut self,
        shard_id: usize,
        new_shard_frontier: &Antichain<T>,
    ) -> Option<Antichain<T>> {
        Self::update_frontier(&mut self.output_frontier, shard_id, new_shard_frontier)
    }

    /// Updates the provided frontier entry with a new shard frontier.
    fn update_frontier(
        entry: &mut (MutableAntichain<T>, Vec<Antichain<T>>),
        shard_id: usize,
        new_shard_frontier: &Antichain<T>,
    ) -> Option<Antichain<T>> {
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
struct PendingSubscribe<T> {
    /// The subscribe frontiers of the partitioned shards.
    frontiers: MutableAntichain<T>,
    /// The updates we are holding back until their timestamps are complete.
    stashed_updates: SubscribeBatchContents<T>,
    /// The row size of stashed updates, for `max_result_size` checking.
    stashed_result_size: usize,
    /// Whether we have already emitted a `DroppedAt` response for this subscribe.
    ///
    /// This field is used to ensure we emit such a response only once.
    dropped: bool,
}

impl<T: ComputeControllerTimestamp> PendingSubscribe<T> {
    fn new(parts: usize) -> Self {
        let mut frontiers = MutableAntichain::new();
        // TODO(benesch): fix this dangerous use of `as`.
        #[allow(clippy::as_conversions)]
        frontiers.update_iter([(T::minimum(), parts as i64)]);

        Self {
            frontiers,
            stashed_updates: SubscribeBatchContents::Updates(Vec::new()),
            stashed_result_size: 0,
            dropped: false,
        }
    }

    /// Stash a new batch of updates.
    ///
    /// This also implements the short-circuit behavior of error responses, and performs
    /// `max_result_size` checking.
    fn stash(&mut self, new_updates: SubscribeBatchContents<T>, max_result_size: u64) {
        match (&mut self.stashed_updates, &new_updates) {
            (SubscribeBatchContents::Error(_), _) => {
                // Subscribe is borked; nothing to do.
                // TODO: Consider refreshing error?
            }
            (_, SubscribeBatchContents::Error(text)) => {
                self.stashed_updates = SubscribeBatchContents::Error(text.clone());
            }
            (SubscribeBatchContents::Updates(stashed), SubscribeBatchContents::Updates(new)) => {
                let new_size: usize = new.iter().map(|(_, row, _)| row.byte_len()).sum();
                self.stashed_result_size += new_size;

                if self.stashed_result_size > max_result_size.cast_into() {
                    self.stashed_updates = SubscribeBatchContents::Error(format!(
                        "total result exceeds max size of {}",
                        ByteSize::b(max_result_size)
                    ));
                } else {
                    stashed.extend(new.iter().cloned());
                }
            }
            (SubscribeBatchContents::Updates(_), SubscribeBatchContents::Stashed(_))
            | (SubscribeBatchContents::Stashed(_), SubscribeBatchContents::Updates(_))
            | (SubscribeBatchContents::Stashed(_), SubscribeBatchContents::Stashed(_)) => {
                // The merge function expects concrete mz_repr::Timestamp types
                // Since T is ComputeControllerTimestamp, we can safely cast
                let old_updates = mem::replace(
                    &mut self.stashed_updates,
                    SubscribeBatchContents::Updates(Vec::new()),
                );
                let old_concrete = cast_subscribe_batch_contents_to_concrete(old_updates);
                let new_concrete = cast_subscribe_batch_contents_to_concrete(new_updates);
                let merged_concrete =
                    merge_subscribe_batch_contents(old_concrete, new_concrete, max_result_size);
                self.stashed_updates = cast_subscribe_batch_contents_from_concrete(merged_concrete);
            }
        }
    }
}

/// Merge two [`PeekResponse`]s.
fn merge_peek_responses(
    resp1: PeekResponse,
    resp2: PeekResponse,
    max_result_size: u64,
) -> PeekResponse {
    use PeekResponse::*;

    // Cancelations and errors short-circuit. Cancelations take precedence over errors.
    let (resp1, resp2) = match (resp1, resp2) {
        (Canceled, _) | (_, Canceled) => return Canceled,
        (Error(e), _) | (_, Error(e)) => return Error(e),
        resps => resps,
    };

    let total_byte_len = resp1.inline_byte_len() + resp2.inline_byte_len();
    if total_byte_len > max_result_size.cast_into() {
        // Note: We match on this specific error message in tests so it's important that
        // nothing else returns the same string.
        let err = format!(
            "total result exceeds max size of {}",
            ByteSize::b(max_result_size)
        );
        return Error(err);
    }

    match (resp1, resp2) {
        (Rows(mut rows1), Rows(rows2)) => {
            rows1.merge(&rows2);
            Rows(rows1)
        }
        (Rows(rows), Stashed(mut stashed)) | (Stashed(mut stashed), Rows(rows)) => {
            stashed.inline_rows.merge(&rows);
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
                return Error("internal error".into());
            }
            if relation_desc1 != relation_desc2 {
                soft_panic_or_log!(
                    "relation descs of stashed responses do not match: \
                             {relation_desc1:?} != {relation_desc2:?}"
                );
                return Error("internal error".into());
            }

            batches1.append(&mut batches2);
            inline_rows1.merge(&inline_rows2);

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

/// Cast SubscribeBatchContents from generic T to concrete Timestamp.
/// In practice T is always mz_repr::Timestamp for ComputeControllerTimestamp.
fn cast_subscribe_batch_contents_to_concrete<T>(
    contents: SubscribeBatchContents<T>,
) -> SubscribeBatchContents<mz_repr::Timestamp>
where
    T: ComputeControllerTimestamp,
{
    // Safety: T is always mz_repr::Timestamp in practice for ComputeControllerTimestamp
    unsafe { std::mem::transmute(contents) }
}

/// Cast SubscribeBatchContents from concrete Timestamp to generic T.
/// In practice T is always mz_repr::Timestamp for ComputeControllerTimestamp.
fn cast_subscribe_batch_contents_from_concrete<T>(
    contents: SubscribeBatchContents<mz_repr::Timestamp>,
) -> SubscribeBatchContents<T>
where
    T: ComputeControllerTimestamp,
{
    // Safety: T is always mz_repr::Timestamp in practice for ComputeControllerTimestamp
    unsafe { std::mem::transmute(contents) }
}

/// Merge two [`SubscribeBatchContents`] with concrete Timestamp.
fn merge_subscribe_batch_contents(
    contents1: SubscribeBatchContents<mz_repr::Timestamp>,
    contents2: SubscribeBatchContents<mz_repr::Timestamp>,
    max_result_size: u64,
) -> SubscribeBatchContents<mz_repr::Timestamp> {
    use SubscribeBatchContents::*;

    // Errors short-circuit.
    let (contents1, contents2) = match (contents1, contents2) {
        (Error(e), _) | (_, Error(e)) => return Error(e),
        (contents1, contents2) => (contents1, contents2),
    };

    // Calculate total size and check against max_result_size
    let total_size = match (&contents1, &contents2) {
        (Updates(updates1), Updates(updates2)) => {
            updates1
                .iter()
                .map(|(_, row, _)| row.byte_len())
                .sum::<usize>()
                + updates2
                    .iter()
                    .map(|(_, row, _)| row.byte_len())
                    .sum::<usize>()
        }
        (Updates(updates), Stashed(stashed)) | (Stashed(stashed), Updates(updates)) => {
            updates
                .iter()
                .map(|(_, row, _)| row.byte_len())
                .sum::<usize>()
                + stashed.size_bytes()
        }
        (Stashed(stashed1), Stashed(stashed2)) => stashed1.size_bytes() + stashed2.size_bytes(),
        (Error(_), _) | (_, Error(_)) => 0, // Errors don't contribute to size
    };

    if total_size > max_result_size.cast_into() {
        return Error(format!(
            "result exceeds max size of {}",
            ByteSize::b(max_result_size)
        ));
    }

    match (contents1, contents2) {
        (Updates(mut updates1), Updates(updates2)) => {
            updates1.extend(updates2);
            Updates(updates1)
        }
        (Updates(updates), Stashed(mut stashed)) | (Stashed(mut stashed), Updates(updates)) => {
            stashed.inline_updates.extend(updates);
            Stashed(stashed)
        }
        (Stashed(stashed1), Stashed(stashed2)) => {
            // Deconstruct so we don't miss adding new fields. We need to be careful about
            // merging everything!
            let StashedSubscribeBatch {
                num_rows_batches: num_rows_batches1,
                encoded_size_bytes: encoded_size_bytes1,
                relation_desc: relation_desc1,
                shard_id: shard_id1,
                batches: mut batches1,
                inline_updates: mut inline_updates1,
            } = *stashed1;
            let StashedSubscribeBatch {
                num_rows_batches: num_rows_batches2,
                encoded_size_bytes: encoded_size_bytes2,
                relation_desc: relation_desc2,
                shard_id: shard_id2,
                batches: mut batches2,
                inline_updates: inline_updates2,
            } = *stashed2;

            if shard_id1 != shard_id2 {
                soft_panic_or_log!(
                    "shard IDs of stashed subscribe responses do not match: \
                     {shard_id1} != {shard_id2}"
                );
                return Error("internal error".into());
            }
            if relation_desc1 != relation_desc2 {
                soft_panic_or_log!(
                    "relation descs of stashed subscribe responses do not match: \
                     {relation_desc1:?} != {relation_desc2:?}"
                );
                return Error("internal error".into());
            }

            batches1.append(&mut batches2);
            inline_updates1.extend(inline_updates2);

            Stashed(Box::new(StashedSubscribeBatch {
                num_rows_batches: num_rows_batches1 + num_rows_batches2,
                encoded_size_bytes: encoded_size_bytes1 + encoded_size_bytes2,
                relation_desc: relation_desc1,
                shard_id: shard_id1,
                batches: batches1,
                inline_updates: inline_updates1,
            }))
        }
        _ => unreachable!("handled above"),
    }
}
