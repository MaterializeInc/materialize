// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Types for cluster-internal control messages that can be broadcast to all
//! workers from individual operators/workers.

use std::time::Instant;

use serde::{Deserialize, Serialize};
use timely::communication::Allocate;
use timely::progress::Antichain;
use timely::synchronization::Sequencer;

use mz_repr::GlobalId;
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::sinks::{MetadataFilled, StorageSinkDesc};
use mz_storage_client::types::sources::IngestionDescription;

use crate::storage_state::Worker;

/// Internal commands that can be sent by individual operators/workers that will
/// be broadcast to all workers. The worker main loop will receive those and act
/// on them.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum InternalStorageCommand {
    /// Suspend and restart the dataflow identified by the `GlobalId`.
    SuspendAndRestart(GlobalId),
    /// Render an ingestion dataflow at the given resumption frontier.
    CreateIngestionDataflow {
        /// ID of the ingestion/sourve.
        id: GlobalId,
        /// The description of the ingestion/source.
        ingestion_description: IngestionDescription<CollectionMetadata>,
        /// The frontier at which we should (re-)start ingestion.
        resumption_frontier: Antichain<mz_repr::Timestamp>,
    },
    /// Render a sink dataflow.
    CreateSinkDataflow(
        GlobalId,
        StorageSinkDesc<MetadataFilled, mz_repr::Timestamp>,
    ),
    /// Drop all state and operators for a dataflow.
    DropDataflow(GlobalId),
}

/// Allows broadcasting [`internal commands`](InternalStorageCommand) to all
/// workers.
pub trait InternalCommandSender {
    /// Broadcasts the given command to all workers.
    fn broadcast(&mut self, internal_cmd: InternalStorageCommand);
}

impl InternalCommandSender for Sequencer<InternalStorageCommand> {
    fn broadcast(&mut self, internal_cmd: InternalStorageCommand) {
        self.push(internal_cmd);
    }
}

impl<'w, A: Allocate> Worker<'w, A> {
    pub(crate) fn setup_command_sequencer(&mut self) -> Sequencer<InternalStorageCommand> {
        // TODO(aljoscha): Use something based on `mz_ore::NowFn`?
        Sequencer::new(self.timely_worker, Instant::now())
    }
}
