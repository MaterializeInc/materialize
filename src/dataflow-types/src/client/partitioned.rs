// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Clients whose implementation is partitioned across a set of subclients
//! (e.g. timely workers).

use std::collections::HashMap;
use std::iter;

use timely::progress::frontier::MutableAntichain;
use tracing::debug;
use uuid::Uuid;

use mz_repr::{Diff, GlobalId, Row};
use mz_service::client::{Partitionable, PartitionedState};

use crate::client::{
    ComputeCommand, ComputeResponse, PeekResponse, StorageCommand, StorageResponse,
};
use crate::{DataflowDescription, TailResponse};

/// Maintained state for partitioned storage clients.
///
/// This helper type unifies the responses of multiple partitioned
/// workers in order to present as a single worker.
#[derive(Debug)]
pub struct PartitionedStorageState<T> {
    /// Number of partitions the state machine represents.
    parts: usize,
    /// Upper frontiers for sources.
    uppers: HashMap<GlobalId, MutableAntichain<T>>,
}

impl<T> Partitionable<StorageCommand<T>, StorageResponse<T>>
    for (StorageCommand<T>, StorageResponse<T>)
where
    T: timely::progress::Timestamp,
{
    type PartitionedState = PartitionedStorageState<T>;

    fn new(parts: usize) -> PartitionedStorageState<T> {
        PartitionedStorageState {
            parts,
            uppers: HashMap::new(),
        }
    }
}

impl<T> PartitionedStorageState<T>
where
    T: timely::progress::Timestamp,
{
    fn observe_command(&mut self, command: &StorageCommand<T>) {
        match command {
            StorageCommand::IngestSources(ingestions) => {
                for ingestion in ingestions {
                    let mut frontier = MutableAntichain::new();
                    frontier.update_iter(iter::once((T::minimum(), self.parts as i64)));
                    let previous = self.uppers.insert(ingestion.id, frontier);
                    assert!(previous.is_none(), "Protocol error: starting frontier tracking for already present identifier {:?} due to command {:?}", ingestion.id, command);
                }
            }
            _ => {
                // Other commands have no known impact on frontier tracking.
            }
        }
    }
}

impl<T> PartitionedState<StorageCommand<T>, StorageResponse<T>> for PartitionedStorageState<T>
where
    T: timely::progress::Timestamp,
{
    fn split_command(&mut self, command: StorageCommand<T>) -> Vec<StorageCommand<T>> {
        self.observe_command(&command);

        vec![command; self.parts]
    }

    fn absorb_response(
        &mut self,
        _shard_id: usize,
        response: StorageResponse<T>,
    ) -> Option<Result<StorageResponse<T>, anyhow::Error>> {
        match response {
            // Avoid multiple retractions of minimum time, to present as updates from one worker.
            StorageResponse::FrontierUppers(mut list) => {
                for (id, changes) in list.iter_mut() {
                    if let Some(frontier) = self.uppers.get_mut(id) {
                        let iter = frontier.update_iter(changes.drain());
                        changes.extend(iter);
                    } else {
                        changes.clear();
                    }
                }
                // The following block implements a `list.retain()` of non-empty change batches.
                // This is more verbose than `list.retain()` because that method cannot mutate
                // its argument, and `is_empty()` may need to do this (as it is lazily compacted).
                let mut cursor = 0;
                while let Some((_id, changes)) = list.get_mut(cursor) {
                    if changes.is_empty() {
                        list.swap_remove(cursor);
                    } else {
                        cursor += 1;
                    }
                }

                if list.is_empty() {
                    None
                } else {
                    Some(Ok(StorageResponse::FrontierUppers(list)))
                }
            }
            // TODO(guswynn): is this the correct implementation?
            StorageResponse::LinearizedTimestamps(feedback) => {
                Some(Ok(StorageResponse::LinearizedTimestamps(feedback)))
            }
        }
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
    /// Upper frontiers for indexes and sinks.
    uppers: HashMap<GlobalId, MutableAntichain<T>>,
    /// Pending responses for a peek; returnable once all are available.
    peek_responses: HashMap<Uuid, HashMap<usize, PeekResponse>>,
    /// Tracks in-progress `TAIL`s, and the stashed rows we are holding
    /// back until their timestamps are complete.
    pending_tails: HashMap<GlobalId, Option<(MutableAntichain<T>, Vec<(T, Row, Diff)>)>>,
}

impl<T> Partitionable<ComputeCommand<T>, ComputeResponse<T>>
    for (ComputeCommand<T>, ComputeResponse<T>)
where
    T: timely::progress::Timestamp + Copy,
{
    type PartitionedState = PartitionedComputeState<T>;

    fn new(parts: usize) -> PartitionedComputeState<T> {
        PartitionedComputeState {
            parts,
            uppers: HashMap::new(),
            peek_responses: HashMap::new(),
            pending_tails: HashMap::new(),
        }
    }
}

impl<T> PartitionedComputeState<T>
where
    T: timely::progress::Timestamp + Copy,
{
    fn reset(&mut self) {
        let PartitionedComputeState {
            parts: _,
            uppers,
            peek_responses,
            pending_tails,
        } = self;
        uppers.clear();
        peek_responses.clear();
        pending_tails.clear();
    }

    /// Observes commands that move past, and prepares state for responses.
    ///
    /// In particular, this method installs and removes upper frontier maintenance.
    pub fn observe_command(&mut self, command: &ComputeCommand<T>) {
        match command {
            ComputeCommand::CreateInstance(_) | ComputeCommand::DropInstance => {
                self.reset();
            }
            _ => (),
        }

        // Temporary storage for identifiers to add to and remove from frontier tracking.
        let mut start = Vec::new();
        let mut cease = Vec::new();
        command.frontier_tracking(&mut start, &mut cease);
        // Apply the determined effects of the command to `self.uppers`.
        for id in start.into_iter() {
            let mut frontier = timely::progress::frontier::MutableAntichain::new();
            frontier.update_iter(Some((T::minimum(), self.parts as i64)));
            let previous = self.uppers.insert(id, frontier);
            assert!(previous.is_none(), "Protocol error: starting frontier tracking for already present identifier {:?} due to command {:?}", id, command);
        }
        for id in cease.into_iter() {
            let previous = self.uppers.remove(&id);
            if previous.is_none() {
                debug!("Protocol error: ceasing frontier tracking for absent identifier {:?} due to command {:?}", id, command);
            }
        }
    }
}

impl<T> PartitionedState<ComputeCommand<T>, ComputeResponse<T>> for PartitionedComputeState<T>
where
    T: timely::progress::Timestamp + Copy,
{
    fn split_command(&mut self, command: ComputeCommand<T>) -> Vec<ComputeCommand<T>> {
        self.observe_command(&command);

        match command {
            ComputeCommand::CreateDataflows(dataflows) => {
                let mut dataflows_parts = vec![Vec::new(); self.parts];

                for dataflow in dataflows {
                    // A list of descriptions of objects for each part to build.
                    let mut builds_parts = vec![Vec::new(); self.parts];
                    // Partition each build description among `parts`.
                    for build_desc in dataflow.objects_to_build {
                        let build_part = build_desc.plan.partition_among(self.parts);
                        for (plan, objects_to_build) in
                            build_part.into_iter().zip(builds_parts.iter_mut())
                        {
                            objects_to_build.push(crate::BuildDesc {
                                id: build_desc.id,
                                plan,
                            });
                        }
                    }
                    // Each list of build descriptions results in a dataflow description.
                    for (dataflows_part, objects_to_build) in
                        dataflows_parts.iter_mut().zip(builds_parts)
                    {
                        dataflows_part.push(DataflowDescription {
                            source_imports: dataflow.source_imports.clone(),
                            index_imports: dataflow.index_imports.clone(),
                            objects_to_build,
                            index_exports: dataflow.index_exports.clone(),
                            sink_exports: dataflow.sink_exports.clone(),
                            as_of: dataflow.as_of.clone(),
                            debug_name: dataflow.debug_name.clone(),
                            id: dataflow.id,
                        });
                    }
                }
                dataflows_parts
                    .into_iter()
                    .map(ComputeCommand::CreateDataflows)
                    .collect()
            }
            command => vec![command; self.parts],
        }
    }

    fn absorb_response(
        &mut self,
        shard_id: usize,
        message: ComputeResponse<T>,
    ) -> Option<Result<ComputeResponse<T>, anyhow::Error>> {
        match message {
            ComputeResponse::FrontierUppers(mut list) => {
                for (id, changes) in list.iter_mut() {
                    if let Some(frontier) = self.uppers.get_mut(id) {
                        let iter = frontier.update_iter(changes.drain());
                        changes.extend(iter);
                    } else {
                        changes.clear();
                    }
                }

                // The following block implements a `list.retain()` of non-empty change batches.
                // This is more verbose than `list.retain()` because that method cannot mutate
                // its argument, and `is_empty()` may need to do this (as it is lazily compacted).
                let mut cursor = 0;
                while let Some((_id, changes)) = list.get_mut(cursor) {
                    if changes.is_empty() {
                        list.swap_remove(cursor);
                    } else {
                        cursor += 1;
                    }
                }

                if list.is_empty() {
                    None
                } else {
                    Some(Ok(ComputeResponse::FrontierUppers(list)))
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
            ComputeResponse::TailResponse(id, response) => {
                let maybe_entry = self.pending_tails.entry(id).or_insert_with(|| {
                    let mut frontier = MutableAntichain::new();
                    frontier.update_iter(std::iter::once((T::minimum(), self.parts as i64)));
                    Some((frontier, Vec::new()))
                });

                let entry = match maybe_entry {
                    None => {
                        // This tail has been dropped;
                        // we should permanently block
                        // any messages from it
                        return None;
                    }
                    Some(entry) => entry,
                };

                use crate::TailBatch;
                use differential_dataflow::consolidation::consolidate_updates;
                match response {
                    TailResponse::Batch(TailBatch {
                        lower,
                        upper,
                        mut updates,
                    }) => {
                        let old_frontier = entry.0.frontier().to_owned();
                        entry.0.update_iter(lower.iter().map(|t| (t.clone(), -1)));
                        entry.0.update_iter(upper.iter().map(|t| (t.clone(), 1)));
                        entry.1.append(&mut updates);
                        let new_frontier = entry.0.frontier().to_owned();
                        if old_frontier != new_frontier {
                            consolidate_updates(&mut entry.1);
                            let mut ship = Vec::new();
                            let mut keep = Vec::new();
                            for (time, data, diff) in entry.1.drain(..) {
                                if new_frontier.less_equal(&time) {
                                    keep.push((time, data, diff));
                                } else {
                                    ship.push((time, data, diff));
                                }
                            }
                            entry.1 = keep;
                            Some(Ok(ComputeResponse::TailResponse(
                                id,
                                TailResponse::Batch(TailBatch {
                                    lower: old_frontier,
                                    upper: new_frontier,
                                    updates: ship,
                                }),
                            )))
                        } else {
                            None
                        }
                    }
                    TailResponse::DroppedAt(frontier) => {
                        *maybe_entry = None;
                        Some(Ok(ComputeResponse::TailResponse(
                            id,
                            TailResponse::DroppedAt(frontier),
                        )))
                    }
                }
            }
        }
    }
}
