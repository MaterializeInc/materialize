// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Unified worker implementation for compute and storage.
//!
//! This module provides a worker that runs both compute and storage dataflows
//! within a single Timely runtime, coordinating their execution through a
//! unified event loop.
//!
//! # Architecture
//!
//! The unified worker merges the event loops from `mz_compute::server` and
//! `mz_storage::storage_state::Worker`. Key components:
//!
//! - Single `step_or_park` loop coordinating both domains
//! - Compute command distribution via Exchange-based broadcast
//! - Storage internal command sequencing for consistent dataflow rendering
//! - Shared metrics and tracing infrastructure
//!
//! # Remaining Work
//!
//! - Unified introspection/logging infrastructure
//! - Wire up to be usable from clusterd via a configuration flag

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{TryRecvError, select};
use mz_cluster::client::ClusterSpec;
use mz_compute::command_channel;
use mz_compute::compute_state::{ActiveComputeState, ComputeState};
use mz_compute::metrics::WorkerMetrics as ComputeWorkerMetrics;
use mz_compute::server::ResponseSender as ComputeResponseSender;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_storage::internal_control::{self, DataflowParameters, InternalStorageCommand};
use mz_storage::statistics::AggregatedStatistics;
use mz_storage::storage_state::StorageState;
use mz_storage::storage_state::async_storage_worker::{
    AsyncStorageWorker, AsyncStorageWorkerResponse,
};
use mz_storage_client::client::{StorageCommand, StorageResponse};
use mz_storage_types::configuration::StorageConfiguration;
use mz_txn_wal::operator::TxnsContext;
use timely::communication::Allocate;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::{mpsc, watch};
use tracing::{info, trace, warn};
use uuid::Uuid;

use crate::unified_config::{ClusterCommand, ClusterResponse, UnifiedConfig};

// Type aliases for clarity
type StorageCommandReceiver = crossbeam_channel::Receiver<StorageCommand>;
type StorageResponseSender = mpsc::UnboundedSender<StorageResponse>;

/// Error type returned on connection nonce changes.
struct NonceChange(Uuid);

/// Endpoint used by workers to receive compute commands via the command channel dataflow.
struct ComputeCommandReceiver {
    inner: command_channel::Receiver,
    worker_id: usize,
    nonce: Option<Uuid>,
    stashed_command: Option<ComputeCommand>,
}

impl ComputeCommandReceiver {
    fn new(inner: command_channel::Receiver, worker_id: usize) -> Self {
        Self {
            inner,
            worker_id,
            nonce: None,
            stashed_command: None,
        }
    }

    fn try_recv(&mut self) -> Result<Option<ComputeCommand>, NonceChange> {
        if let Some(command) = self.stashed_command.take() {
            return Ok(Some(command));
        }
        let Some((command, nonce)) = self.inner.try_recv() else {
            return Ok(None);
        };

        trace!(worker = self.worker_id, %nonce, ?command, "received compute command");

        if Some(nonce) == self.nonce {
            Ok(Some(command))
        } else {
            self.nonce = Some(nonce);
            self.stashed_command = Some(command);
            Err(NonceChange(nonce))
        }
    }

    fn is_empty(&self) -> bool {
        self.stashed_command.is_none() && self.inner.is_empty()
    }
}

/// State maintained for each unified worker thread.
pub struct UnifiedWorker<'w, A: Allocate> {
    /// The underlying Timely worker.
    timely_worker: &'w mut TimelyWorker<A>,

    // Compute domain
    compute_command_rx: ComputeCommandReceiver,
    compute_response_tx: ComputeResponseSender,
    compute_state: Option<ComputeState>,
    compute_metrics: ComputeWorkerMetrics,

    // Storage domain
    storage_command_rx: StorageCommandReceiver,
    storage_response_tx: StorageResponseSender,
    storage_state: StorageState,

    // Shared resources
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    compute_context: mz_compute::server::ComputeInstanceContext,
}

impl ClusterSpec for UnifiedConfig {
    type Command = ClusterCommand;
    type Response = ClusterResponse;

    fn run_worker<A: Allocate + 'static>(
        &self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            Uuid,
            crossbeam_channel::Receiver<ClusterCommand>,
            mpsc::UnboundedSender<ClusterResponse>,
        )>,
    ) {
        let worker_id = timely_worker.index();

        // Set core affinity if configured
        if self.compute_instance_context.worker_core_affinity {
            set_core_affinity(worker_id);
        }

        // Create compute command channel (broadcasts from worker 0 to all workers)
        let (compute_cmd_tx, compute_cmd_rx) = command_channel::render(timely_worker);
        let (compute_resp_tx, compute_resp_rx) = crossbeam_channel::unbounded();

        // Create storage internal command sequencer (must be created once per worker)
        let (internal_cmd_tx, internal_cmd_rx) =
            internal_control::setup_command_sequencer(timely_worker);

        // Create storage configuration
        let storage_configuration =
            StorageConfiguration::new(self.connection_context.clone(), mz_dyncfgs::all_dyncfgs());

        // Storage read-only state
        let (read_only_tx, read_only_rx) = watch::channel(true);

        // Create async worker for storage (handles persist operations asynchronously)
        let async_worker =
            AsyncStorageWorker::new(thread::current(), Arc::clone(&self.persist_clients));

        let cluster_memory_limit = self.storage_instance_context.cluster_memory_limit;

        // Create storage state
        let storage_state = StorageState {
            source_uppers: BTreeMap::new(),
            source_tokens: BTreeMap::new(),
            metrics: self.storage_metrics.clone(),
            reported_frontiers: BTreeMap::new(),
            ingestions: BTreeMap::new(),
            exports: BTreeMap::new(),
            oneshot_ingestions: BTreeMap::new(),
            now: self.now.clone(),
            timely_worker_index: timely_worker.index(),
            timely_worker_peers: timely_worker.peers(),
            instance_context: self.storage_instance_context.clone(),
            persist_clients: Arc::clone(&self.persist_clients),
            txns_ctx: self.txns_ctx.clone(),
            sink_tokens: BTreeMap::new(),
            sink_write_frontiers: BTreeMap::new(),
            dropped_ids: Vec::new(),
            aggregated_statistics: AggregatedStatistics::new(
                timely_worker.index(),
                timely_worker.peers(),
            ),
            shared_status_updates: Default::default(),
            latest_status_updates: Default::default(),
            initial_status_reported: Default::default(),
            internal_cmd_tx,
            internal_cmd_rx,
            read_only_tx,
            read_only_rx,
            async_worker,
            storage_configuration,
            dataflow_parameters: DataflowParameters::new(
                self.shared_rocksdb_write_buffer_manager.clone(),
                cluster_memory_limit,
            ),
            tracing_handle: Arc::clone(&self.tracing_handle),
            server_maintenance_interval: Duration::ZERO,
        };

        // Create storage command/response channels (will be connected by adapter)
        let (storage_cmd_tx, storage_cmd_rx) = crossbeam_channel::unbounded();
        let (storage_resp_tx, storage_resp_rx) = mpsc::unbounded_channel();

        // Spawn channel adapter to demultiplex unified commands to compute and storage
        spawn_unified_channel_adapter(
            client_rx,
            compute_cmd_tx,
            compute_resp_rx,
            storage_cmd_tx,
            storage_resp_rx,
            worker_id,
        );

        let compute_metrics = self.compute_metrics.for_worker(worker_id);

        UnifiedWorker {
            timely_worker,
            compute_command_rx: ComputeCommandReceiver::new(compute_cmd_rx, worker_id),
            compute_response_tx: ComputeResponseSender::new(compute_resp_tx, worker_id),
            compute_state: None,
            compute_metrics,
            storage_command_rx: storage_cmd_rx,
            storage_response_tx: storage_resp_tx,
            storage_state,
            persist_clients: Arc::clone(&self.persist_clients),
            txns_ctx: self.txns_ctx.clone(),
            tracing_handle: Arc::clone(&self.tracing_handle),
            compute_context: self.compute_instance_context.clone(),
        }
        .run()
    }
}

impl<'w, A: Allocate + 'static> UnifiedWorker<'w, A> {
    /// Runs the unified worker.
    pub fn run(&mut self) {
        // Wait for first compute command to get initial nonce
        let NonceChange(nonce) = self
            .recv_compute_command()
            .expect_err("change to first nonce");
        self.set_compute_nonce(nonce);

        loop {
            let Err(NonceChange(nonce)) = self.run_client();
            self.set_compute_nonce(nonce);
        }
    }

    fn set_compute_nonce(&mut self, nonce: Uuid) {
        self.compute_response_tx.set_nonce(nonce);
    }

    /// Handles commands for a client connection, returns when the compute nonce changes.
    fn run_client(&mut self) -> Result<Infallible, NonceChange> {
        // Reconcile compute state
        self.reconcile_compute()?;

        // Last maintenance times
        let mut last_compute_maintenance = Instant::now();
        let mut last_storage_stats_time = Instant::now();
        let mut last_storage_maintenance = Instant::now();

        loop {
            // Get maintenance intervals
            let compute_maintenance_interval = self
                .compute_state
                .as_ref()
                .map_or(Duration::ZERO, |state| state.server_maintenance_interval);
            let storage_maintenance_interval = self.storage_state.server_maintenance_interval;
            let storage_stats_interval = self
                .storage_state
                .storage_configuration
                .parameters
                .statistics_collection_interval;

            let now = Instant::now();

            // Determine sleep duration (minimum of all intervals)
            let mut sleep_duration: Option<Duration> = None;

            // Check compute maintenance
            if now >= last_compute_maintenance + compute_maintenance_interval {
                last_compute_maintenance = now;
                if let Some(mut compute_state) = self.activate_compute() {
                    compute_state.compute_state.traces.maintenance();
                    compute_state.report_frontiers();
                    compute_state.report_metrics();
                    compute_state.check_expiration();
                }
                self.compute_metrics.record_shared_row_metrics();
            } else {
                let next = last_compute_maintenance + compute_maintenance_interval;
                let remaining = next.saturating_duration_since(now);
                sleep_duration =
                    Some(sleep_duration.map_or(remaining, |d: Duration| d.min(remaining)));
            }

            // Check storage maintenance
            if now >= last_storage_maintenance + storage_maintenance_interval {
                last_storage_maintenance = now;
                self.report_storage_frontier_progress();
            } else {
                let next = last_storage_maintenance + storage_maintenance_interval;
                let remaining = next.saturating_duration_since(now);
                sleep_duration =
                    Some(sleep_duration.map_or(remaining, |d: Duration| d.min(remaining)));
            }

            // Determine if we can park
            let can_park = self.compute_command_rx.is_empty()
                && self.storage_command_rx.is_empty()
                && self.storage_state.async_worker.is_empty();

            // Step the timely worker
            let timer = self
                .compute_metrics
                .timely_step_duration_seconds
                .start_timer();
            if can_park {
                // Include storage stats interval in sleep calculation
                let stats_remaining =
                    storage_stats_interval.saturating_sub(last_storage_stats_time.elapsed());
                let park_duration =
                    sleep_duration.map_or(stats_remaining, |d: Duration| d.min(stats_remaining));
                self.timely_worker.step_or_park(Some(park_duration));
            } else {
                self.timely_worker.step();
            }
            timer.observe_duration();

            // Handle compute commands
            self.handle_pending_compute_commands()?;

            // Process compute peeks/subscribes
            if let Some(mut compute_state) = self.activate_compute() {
                compute_state.process_peeks();
                compute_state.process_subscribes();
                compute_state.process_copy_tos();
            }

            // Handle storage dropped IDs
            for id in std::mem::take(&mut self.storage_state.dropped_ids) {
                self.send_storage_response(StorageResponse::DroppedId(id));
            }

            // Report storage status updates
            self.report_storage_status_updates();

            // Report storage statistics if interval elapsed
            if last_storage_stats_time.elapsed() >= storage_stats_interval {
                self.report_storage_statistics();
                last_storage_stats_time = Instant::now();
            }

            // Handle storage commands
            loop {
                match self.storage_command_rx.try_recv() {
                    Ok(cmd) => self.storage_state.handle_storage_command(cmd),
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        // Storage channel disconnected - this is unexpected in unified mode
                        warn!("storage command channel disconnected unexpectedly");
                        break;
                    }
                }
            }

            // Handle storage async worker responses
            while let Ok(response) = self.storage_state.async_worker.try_recv() {
                self.handle_storage_async_worker_response(response);
            }

            // Handle storage internal commands (renders dataflows as needed)
            while let Some(command) = self.storage_state.internal_cmd_rx.try_recv() {
                self.handle_internal_storage_command(command);
            }
        }
    }

    fn handle_pending_compute_commands(&mut self) -> Result<(), NonceChange> {
        while let Some(cmd) = self.compute_command_rx.try_recv()? {
            self.handle_compute_command(cmd);
        }
        Ok(())
    }

    fn handle_compute_command(&mut self, cmd: ComputeCommand) {
        if let ComputeCommand::CreateInstance(_) = &cmd {
            self.compute_state = Some(ComputeState::new(
                Arc::clone(&self.persist_clients),
                self.txns_ctx.clone(),
                self.compute_metrics.clone(),
                Arc::clone(&self.tracing_handle),
                self.compute_context.clone(),
            ));
        }
        self.activate_compute().unwrap().handle_compute_command(cmd);
    }

    fn activate_compute(&mut self) -> Option<ActiveComputeState<'_, A>> {
        if let Some(compute_state) = &mut self.compute_state {
            Some(ActiveComputeState {
                timely_worker: &mut *self.timely_worker,
                compute_state,
                response_tx: &mut self.compute_response_tx,
            })
        } else {
            None
        }
    }

    /// Receive the next compute command, stepping Timely while waiting.
    fn recv_compute_command(&mut self) -> Result<ComputeCommand, NonceChange> {
        loop {
            if let Some(cmd) = self.compute_command_rx.try_recv()? {
                return Ok(cmd);
            }

            let start = Instant::now();
            self.timely_worker.step_or_park(None);
            self.compute_metrics
                .timely_step_duration_seconds
                .observe(start.elapsed().as_secs_f64());
        }
    }

    /// Reconcile compute state after a nonce change.
    fn reconcile_compute(&mut self) -> Result<(), NonceChange> {
        // Drain commands until InitializationComplete
        let mut new_commands = Vec::new();
        loop {
            match self.recv_compute_command()? {
                ComputeCommand::InitializationComplete => break,
                command => new_commands.push(command),
            }
        }

        // Apply reconciliation logic
        // For now, just apply all new commands
        // TODO: Implement full reconciliation logic from compute
        for cmd in new_commands {
            self.handle_compute_command(cmd);
        }

        Ok(())
    }

    // Storage helper methods

    fn send_storage_response(&self, response: StorageResponse) {
        let _ = self.storage_response_tx.send(response);
    }

    fn report_storage_frontier_progress(&mut self) {
        // Report frontier progress for all storage collections
        for (id, frontier) in &self.storage_state.reported_frontiers {
            self.send_storage_response(StorageResponse::FrontierUpper(*id, frontier.clone()));
        }
    }

    fn report_storage_status_updates(&mut self) {
        let updates = std::mem::take(&mut *self.storage_state.shared_status_updates.borrow_mut());
        for update in updates {
            self.send_storage_response(StorageResponse::StatusUpdate(update));
        }
    }

    fn report_storage_statistics(&mut self) {
        // First, emit local statistics to the internal command channel for aggregation
        let (local_sources, local_sinks) = self.storage_state.aggregated_statistics.emit_local();
        if !local_sources.is_empty() || !local_sinks.is_empty() {
            self.storage_state
                .internal_cmd_tx
                .send(InternalStorageCommand::StatisticsUpdate {
                    sources: local_sources,
                    sinks: local_sinks,
                });
        }

        // Then, get the aggregated snapshot and send to the controller
        let (source_stats, sink_stats) = self.storage_state.aggregated_statistics.snapshot();
        if !source_stats.is_empty() || !sink_stats.is_empty() {
            self.send_storage_response(StorageResponse::StatisticsUpdates(
                source_stats,
                sink_stats,
            ));
        }
    }

    fn handle_storage_async_worker_response(
        &self,
        response: AsyncStorageWorkerResponse<mz_repr::Timestamp>,
    ) {
        assert_eq!(
            self.timely_worker.index(),
            0,
            "only worker #0 handles async responses"
        );

        match response {
            AsyncStorageWorkerResponse::IngestionFrontiersUpdated {
                id,
                ingestion_description,
                as_of,
                resume_uppers,
                source_resume_uppers,
            } => {
                self.storage_state.internal_cmd_tx.send(
                    InternalStorageCommand::CreateIngestionDataflow {
                        id,
                        ingestion_description,
                        as_of,
                        resume_uppers,
                        source_resume_uppers,
                    },
                );
            }
            AsyncStorageWorkerResponse::ExportFrontiersUpdated { id, description } => {
                self.storage_state
                    .internal_cmd_tx
                    .send(InternalStorageCommand::RunSinkDataflow(id, description));
            }
            AsyncStorageWorkerResponse::DropDataflow(id) => {
                self.storage_state
                    .internal_cmd_tx
                    .send(InternalStorageCommand::DropDataflow(vec![id]));
            }
        }
    }

    fn handle_internal_storage_command(&mut self, command: InternalStorageCommand) {
        // Delegate to the storage crate's free function which handles all
        // internal storage commands, including dataflow rendering.
        mz_storage::handle_internal_storage_command(
            self.timely_worker,
            &mut self.storage_state,
            command,
        );
    }
}

/// Spawn the channel adapter that demultiplexes unified commands.
fn spawn_unified_channel_adapter(
    client_rx: crossbeam_channel::Receiver<(
        Uuid,
        crossbeam_channel::Receiver<ClusterCommand>,
        mpsc::UnboundedSender<ClusterResponse>,
    )>,
    compute_cmd_tx: command_channel::Sender,
    compute_resp_rx: crossbeam_channel::Receiver<(ComputeResponse, Uuid)>,
    storage_cmd_tx: crossbeam_channel::Sender<StorageCommand>,
    storage_resp_rx: mpsc::UnboundedReceiver<StorageResponse>,
    worker_id: usize,
) {
    thread::Builder::new()
        .name(format!("uca-{worker_id}")) // unified channel adapter
        .spawn(move || {
            let mut stashed_compute_responses = BTreeMap::<Uuid, Vec<ComputeResponse>>::new();
            let mut storage_resp_rx = storage_resp_rx;

            while let Ok((nonce, command_rx, response_tx)) = client_rx.recv() {
                // Send stashed compute responses for this nonce
                if let Some(resps) = stashed_compute_responses.remove(&nonce) {
                    for resp in resps {
                        let _ = response_tx.send(ClusterResponse::Compute(resp));
                    }
                }

                // Serve this connection
                let mut connection_active = true;
                while connection_active {
                    select! {
                        recv(command_rx) -> msg => match msg {
                            Ok(ClusterCommand::Compute(cmd)) => {
                                compute_cmd_tx.send((cmd, nonce));
                            }
                            Ok(ClusterCommand::Storage(cmd)) => {
                                let _ = storage_cmd_tx.send(cmd);
                            }
                            Err(_) => {
                                connection_active = false;
                            }
                        },
                        recv(compute_resp_rx) -> msg => {
                            let (resp, resp_nonce) = msg.expect("worker connected");
                            if resp_nonce == nonce {
                                // Clear stash on matching response
                                stashed_compute_responses.clear();
                                let _ = response_tx.send(ClusterResponse::Compute(resp));
                            } else {
                                // Stash responses with unknown nonce
                                stashed_compute_responses
                                    .entry(resp_nonce)
                                    .or_default()
                                    .push(resp);
                            }
                        },
                    }

                    // Check for storage responses (non-blocking)
                    while let Ok(resp) = storage_resp_rx.try_recv() {
                        let _ = response_tx.send(ClusterResponse::Storage(resp));
                    }
                }
            }
        })
        .expect("failed to spawn unified channel adapter");
}

/// Set the current thread's core affinity.
#[cfg(not(target_os = "macos"))]
fn set_core_affinity(worker_id: usize) {
    use tracing::error;

    let Some(mut core_ids) = core_affinity::get_core_ids() else {
        error!(worker_id, "unable to get core IDs for setting affinity");
        return;
    };

    core_ids.sort_unstable_by_key(|i| i.id);
    let idx = worker_id % core_ids.len();
    let core_id = core_ids[idx];

    if core_affinity::set_for_current(core_id) {
        info!(
            worker_id,
            core_id = core_id.id,
            "set core affinity for worker"
        );
    } else {
        error!(
            worker_id,
            core_id = core_id.id,
            "failed to set core affinity for worker"
        )
    }
}

#[cfg(target_os = "macos")]
fn set_core_affinity(_worker_id: usize) {
    info!("setting core affinity is not supported on macOS");
}
