// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Types for cluster-internal control messages that can be broadcast to all
//! workers from individual operators/workers.

use std::collections::BTreeMap;
use std::time::Instant;

use mz_repr::{GlobalId, Row};
use mz_rocksdb::config::SharedWriteBufferManager;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::parameters::StorageParameters;
use mz_storage_types::sinks::{MetadataFilled, StorageSinkDesc};
use mz_storage_types::sources::IngestionDescription;
use serde::{Deserialize, Serialize};
use timely::communication::Allocate;
use timely::progress::Antichain;
use timely::synchronization::Sequencer;
use timely::worker::Worker as TimelyWorker;

use crate::statistics::{SinkStatisticsRecord, SourceStatisticsRecord};

/// _Dynamic_ storage instance configuration parameters that are used during dataflow rendering.
/// Changes to these parameters are applied to `StorageWorker`s in a consistent order
/// with source and sink creation.
#[derive(Debug)]
pub struct DataflowParameters {
    /// Configuration/tuning for RocksDB. This also contains
    /// some shared objects, which is why its separate.
    pub upsert_rocksdb_tuning_config: mz_rocksdb::RocksDBConfig,
}

impl DataflowParameters {
    /// Creates a new instance of `DataflowParameters` with given shared rocksdb write buffer manager
    /// and the cluster memory limit
    pub fn new(
        shared_rocksdb_write_buffer_manager: SharedWriteBufferManager,
        cluster_memory_limit: Option<usize>,
    ) -> Self {
        Self {
            upsert_rocksdb_tuning_config: mz_rocksdb::RocksDBConfig::new(
                shared_rocksdb_write_buffer_manager,
                cluster_memory_limit,
            ),
        }
    }
    /// Update the `DataflowParameters` with new configuration.
    pub fn update(&mut self, storage_parameters: StorageParameters) {
        self.upsert_rocksdb_tuning_config
            .apply(storage_parameters.upsert_rocksdb_tuning_config.clone());
    }
}

/// Internal commands that can be sent by individual operators/workers that will
/// be broadcast to all workers. The worker main loop will receive those and act
/// on them.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum InternalStorageCommand {
    /// Suspend and restart the dataflow identified by the `GlobalId`.
    SuspendAndRestart {
        /// The id of the dataflow that should be restarted.
        id: GlobalId,
        /// The reason for the restart request.
        reason: String,
    },
    /// Render an ingestion dataflow at the given resumption frontier.
    CreateIngestionDataflow {
        /// ID of the ingestion/sourve.
        id: GlobalId,
        /// The description of the ingestion/source.
        ingestion_description: IngestionDescription<CollectionMetadata>,
        /// The frontier beyond which ingested updates should be uncompacted. Inputs to the
        /// ingestion are guaranteed to be readable at this frontier.
        as_of: Antichain<mz_repr::Timestamp>,
        /// A frontier in the Materialize time domain with the property that all updates not beyond
        /// it have already been durably ingested.
        resume_uppers: BTreeMap<GlobalId, Antichain<mz_repr::Timestamp>>,
        /// A frontier in the source time domain with the property that all updates not beyond it
        /// have already been durably ingested.
        source_resume_uppers: BTreeMap<GlobalId, Vec<Row>>,
    },
    /// Render a sink dataflow.
    RunSinkDataflow(
        GlobalId,
        StorageSinkDesc<MetadataFilled, mz_repr::Timestamp>,
    ),
    /// Drop all state and operators for a dataflow. This is a vec because some
    /// dataflows have their state spread over multiple IDs (i.e. sources that
    /// spawn subsources); this means that actions taken in response to this
    /// command should be permissive about missing state.
    DropDataflow(Vec<GlobalId>),

    /// Update the configuration for rendering dataflows.
    UpdateConfiguration {
        /// The new configuration parameters.
        storage_parameters: StorageParameters,
    },
    /// For moving statistics updates to worker 0.
    StatisticsUpdate {
        /// Local statistics, with their epochs.
        sources: Vec<(usize, SourceStatisticsRecord)>,
        /// Local statistics, with their epochs.
        sinks: Vec<(usize, SinkStatisticsRecord)>,
    },
}

/// Allows broadcasting [`internal commands`](InternalStorageCommand) to all
/// workers.
pub trait InternalCommandSender {
    /// Broadcasts the given command to all workers.
    fn broadcast(&mut self, internal_cmd: InternalStorageCommand);

    /// Returns the next available command, if any. This returns `None` when
    /// there are currently no commands but there might be commands again in the
    /// future.
    fn next(&mut self) -> Option<InternalStorageCommand>;
}

impl InternalCommandSender for Sequencer<InternalStorageCommand> {
    fn broadcast(&mut self, internal_cmd: InternalStorageCommand) {
        self.push(internal_cmd);
    }

    fn next(&mut self) -> Option<InternalStorageCommand> {
        Iterator::next(self)
    }
}

pub(crate) fn setup_command_sequencer<'w, A: Allocate>(
    timely_worker: &'w mut TimelyWorker<A>,
) -> Sequencer<InternalStorageCommand> {
    // TODO(aljoscha): Use something based on `mz_ore::NowFn`?
    Sequencer::new(timely_worker, Instant::now())
}
