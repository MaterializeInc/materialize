// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Types for cluster-internal control messages that can be broadcast to all
//! workers from individual operators/workers.

use std::collections::BTreeMap;
use std::time::Instant;

use mz_repr::{GlobalId, Row};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::sinks::{MetadataFilled, StorageSinkDesc};
use mz_storage_client::types::sources::IngestionDescription;
use serde::{Deserialize, Serialize};
use timely::communication::Allocate;
use timely::progress::Antichain;
use timely::synchronization::Sequencer;
use timely::worker::Worker as TimelyWorker;

/// Storage instance configuration parameters that are used during dataflow rendering.
/// Changes to these parameters are applied to `StorageWorker`s in a consistent order
/// with source and sink creation.
#[derive(Debug, Default)]
pub struct DataflowParameters {
    /// Configured PG replication timeouts,
    pub pg_replication_timeouts: mz_postgres_util::ReplicationTimeouts,
    /// Configuration/tuning for RocksDB
    pub upsert_rocksdb_tuning_config: mz_rocksdb::RocksDBConfig,
    /// A set of parameters to configure auto spill to disk behaviour for a `DISK`
    /// enabled upsert source
    pub auto_spill_config: mz_storage_client::types::parameters::UpsertAutoSpillConfig,
    /// A loose boundary on the number of inflight bytes used by parts in `persist_source`,
    /// used in `UPSERT/DEBEZIUM` sources.
    pub storage_dataflow_max_inflight_bytes: Option<usize>,
}

impl DataflowParameters {
    /// Update the `DataflowParameters` with new configuration.
    pub fn update(
        &mut self,
        pg_replication_timeouts: mz_postgres_util::ReplicationTimeouts,
        rocksdb_params: mz_rocksdb::RocksDBTuningParameters,
        auto_spill_config: mz_storage_client::types::parameters::UpsertAutoSpillConfig,
        storage_dataflow_max_inflight_bytes: Option<usize>,
    ) {
        self.pg_replication_timeouts = pg_replication_timeouts;
        self.upsert_rocksdb_tuning_config.apply(rocksdb_params);
        self.auto_spill_config = auto_spill_config;
        self.storage_dataflow_max_inflight_bytes = storage_dataflow_max_inflight_bytes;
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
    CreateSinkDataflow(
        GlobalId,
        StorageSinkDesc<MetadataFilled, mz_repr::Timestamp>,
    ),
    /// Drop all state and operators for a dataflow.
    DropDataflow(GlobalId),

    /// Update the configuration for rendering dataflows.
    UpdateConfiguration {
        /// Postgres timeout configuration.
        pg: mz_postgres_util::ReplicationTimeouts,
        /// RocksDB configuration.
        rocksdb: mz_rocksdb::RocksDBTuningParameters,
        /// Backpressure configuration.
        storage_dataflow_max_inflight_bytes: Option<usize>,
        /// Upsert autospill configuration.
        auto_spill_config: mz_storage_client::types::parameters::UpsertAutoSpillConfig,
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
