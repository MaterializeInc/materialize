// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An interactive dataflow server.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::fmt::Debug;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::Error;
use crossbeam_channel::SendError;
use mz_cluster::client::{ClusterClient, ClusterSpec};
use mz_cluster_client::client::TimelyConfig;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::history::ComputeCommandHistory;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::ComputeClient;
use mz_ore::halt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use timely::communication::Allocate;
use timely::progress::Antichain;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;
use tracing::{info, trace, warn};
use uuid::Uuid;

use crate::command_channel;
use crate::compute_state::{ActiveComputeState, ComputeState, ReportedFrontier};
use crate::metrics::{ComputeMetrics, WorkerMetrics};

/// Caller-provided configuration for compute.
#[derive(Clone, Debug)]
pub struct ComputeInstanceContext {
    /// A directory that can be used for scratch work.
    pub scratch_directory: Option<PathBuf>,
    /// Whether to set core affinity for Timely workers.
    pub worker_core_affinity: bool,
    /// Context required to connect to an external sink from compute,
    /// like the `CopyToS3OneshotSink` compute sink.
    pub connection_context: ConnectionContext,
}

/// Configures the server with compute-specific metrics.
#[derive(Debug, Clone)]
struct Config {
    /// `persist` client cache.
    pub persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    pub txns_ctx: TxnsContext,
    /// A process-global handle to tracing configuration.
    pub tracing_handle: Arc<TracingHandle>,
    /// Metrics exposed by compute replicas.
    pub metrics: ComputeMetrics,
    /// Other configuration for compute.
    pub context: ComputeInstanceContext,
}

/// Initiates a timely dataflow computation, processing compute commands.
pub async fn serve(
    timely_config: Option<TimelyConfig>,
    metrics_registry: &MetricsRegistry,
    persist_clients: Arc<PersistClientCache>,
    txns_ctx: TxnsContext,
    tracing_handle: Arc<TracingHandle>,
    context: ComputeInstanceContext,
) -> Result<impl Fn() -> Box<dyn ComputeClient> + use<>, Error> {
    let config = Config {
        persist_clients,
        txns_ctx,
        tracing_handle,
        metrics: ComputeMetrics::register_with(metrics_registry),
        context,
    };
    let tokio_executor = tokio::runtime::Handle::current();

    let timely_container = if let Some(timely_config) = timely_config {
        let timely = config
            .build_cluster(timely_config, tokio_executor.clone())
            .await?;
        Some(timely)
    } else {
        None
    };
    let timely_container = Arc::new(tokio::sync::Mutex::new(timely_container));

    let client_builder = move || {
        let client = ClusterClient::new(
            Arc::clone(&timely_container),
            tokio_executor.clone(),
            config.clone(),
        );
        let client: Box<dyn ComputeClient> = Box::new(client);
        client
    };

    Ok(client_builder)
}

/// Error type returned on connection nonce changes.
///
/// A nonce change informs workers that subsequent commands come a from a new client connection
/// and therefore require reconciliation.
struct NonceChange(Uuid);

/// Endpoint used by workers to receive compute commands.
///
/// Observes nonce changes in the command stream and converts them into receive errors.
struct CommandReceiver {
    /// The channel supplying commands.
    inner: command_channel::Receiver,
    /// The ID of the Timely worker.
    worker_id: usize,
    /// The nonce identifying the current cluster protocol incarnation.
    nonce: Option<Uuid>,
    /// A stash to enable peeking the next command, used in `try_recv`.
    stashed_command: Option<ComputeCommand>,
}

impl CommandReceiver {
    fn new(inner: command_channel::Receiver, worker_id: usize) -> Self {
        Self {
            inner,
            worker_id,
            nonce: None,
            stashed_command: None,
        }
    }

    /// Receive the next pending command, if any.
    ///
    /// If the next command has a different nonce, this method instead returns an `Err`
    /// containing the new nonce.
    fn try_recv(&mut self) -> Result<Option<ComputeCommand>, NonceChange> {
        if let Some(command) = self.stashed_command.take() {
            return Ok(Some(command));
        }
        let Some((command, nonce)) = self.inner.try_recv() else {
            return Ok(None);
        };

        trace!(worker = self.worker_id, %nonce, ?command, "received command");

        if Some(nonce) == self.nonce {
            Ok(Some(command))
        } else {
            self.nonce = Some(nonce);
            self.stashed_command = Some(command);
            Err(NonceChange(nonce))
        }
    }
}

/// Endpoint used by workers to send sending compute responses.
///
/// Tags responses with the current nonce, allowing receivers to filter out responses intended for
/// previous client connections.
pub(crate) struct ResponseSender {
    /// The channel consuming responses.
    inner: crossbeam_channel::Sender<(ComputeResponse, Uuid)>,
    /// The ID of the Timely worker.
    worker_id: usize,
    /// The nonce identifying the current cluster protocol incarnation.
    nonce: Option<Uuid>,
}

impl ResponseSender {
    fn new(inner: crossbeam_channel::Sender<(ComputeResponse, Uuid)>, worker_id: usize) -> Self {
        Self {
            inner,
            worker_id,
            nonce: None,
        }
    }

    /// Set the cluster protocol nonce.
    fn set_nonce(&mut self, nonce: Uuid) {
        self.nonce = Some(nonce);
    }

    /// Send a compute response.
    pub fn send(&self, response: ComputeResponse) -> Result<(), SendError<ComputeResponse>> {
        let nonce = self.nonce.expect("nonce must be initialized");

        trace!(worker = self.worker_id, %nonce, ?response, "sending response");
        self.inner
            .send((response, nonce))
            .map_err(|SendError((resp, _))| SendError(resp))
    }
}

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
struct Worker<'w, A: Allocate> {
    /// The underlying Timely worker.
    timely_worker: &'w mut TimelyWorker<A>,
    /// The channel over which commands are received.
    command_rx: CommandReceiver,
    /// The channel over which responses are sent.
    response_tx: ResponseSender,
    compute_state: Option<ComputeState>,
    /// Compute metrics.
    metrics: WorkerMetrics,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers
    persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    txns_ctx: TxnsContext,
    /// A process-global handle to tracing configuration.
    tracing_handle: Arc<TracingHandle>,
    context: ComputeInstanceContext,
}

impl ClusterSpec for Config {
    type Command = ComputeCommand;
    type Response = ComputeResponse;

    fn run_worker<A: Allocate + 'static>(
        &self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            Uuid,
            crossbeam_channel::Receiver<ComputeCommand>,
            mpsc::UnboundedSender<ComputeResponse>,
        )>,
    ) {
        if self.context.worker_core_affinity {
            set_core_affinity(timely_worker.index());
        }

        let worker_id = timely_worker.index();
        let metrics = self.metrics.for_worker(worker_id);

        // Create the command channel that broadcasts commands from worker 0 to other workers. We
        // reuse this channel between client connections, to avoid bugs where different workers end
        // up creating incompatible sides of the channel dataflow after reconnects.
        // See database-issues#8964.
        let (cmd_tx, cmd_rx) = command_channel::render(timely_worker);
        let (resp_tx, resp_rx) = crossbeam_channel::unbounded();

        spawn_channel_adapter(client_rx, cmd_tx, resp_rx, worker_id);

        Worker {
            timely_worker,
            command_rx: CommandReceiver::new(cmd_rx, worker_id),
            response_tx: ResponseSender::new(resp_tx, worker_id),
            metrics,
            context: self.context.clone(),
            persist_clients: Arc::clone(&self.persist_clients),
            txns_ctx: self.txns_ctx.clone(),
            compute_state: None,
            tracing_handle: Arc::clone(&self.tracing_handle),
        }
        .run()
    }
}

/// Set the current thread's core affinity, based on the given `worker_id`.
#[cfg(not(target_os = "macos"))]
fn set_core_affinity(worker_id: usize) {
    use tracing::error;

    let Some(mut core_ids) = core_affinity::get_core_ids() else {
        error!(worker_id, "unable to get core IDs for setting affinity");
        return;
    };

    // The `get_core_ids` docs don't say anything about a guaranteed order of the returned Vec,
    // so sort it just to be safe.
    core_ids.sort_unstable_by_key(|i| i.id);

    // On multi-process replicas `worker_id` might be greater than the number of available cores.
    // However, we assume that we always have at least as many cores as there are local workers.
    // Violating this assumption is safe but might lead to degraded performance due to skew in core
    // utilization.
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

/// Set the current thread's core affinity, based on the given `worker_id`.
#[cfg(target_os = "macos")]
fn set_core_affinity(_worker_id: usize) {
    // Setting core affinity is known to not work on Apple Silicon:
    // https://github.com/Elzair/core_affinity_rs/issues/22
    info!("setting core affinity is not supported on macOS");
}

impl<'w, A: Allocate + 'static> Worker<'w, A> {
    /// Runs a compute worker.
    pub fn run(&mut self) {
        // The command receiver is initialized without an nonce, so receiving the first command
        // always triggers a nonce change.
        let NonceChange(nonce) = self.recv_command().expect_err("change to first nonce");
        self.set_nonce(nonce);

        loop {
            let Err(NonceChange(nonce)) = self.run_client();
            self.set_nonce(nonce);
        }
    }

    fn set_nonce(&mut self, nonce: Uuid) {
        self.response_tx.set_nonce(nonce);
    }

    /// Handles commands for a client connection, returns when the nonce changes.
    fn run_client(&mut self) -> Result<Infallible, NonceChange> {
        self.reconcile()?;

        // The last time we did periodic maintenance.
        let mut last_maintenance = Instant::now();

        // Commence normal operation.
        loop {
            // Get the maintenance interval, default to zero if we don't have a compute state.
            let maintenance_interval = self
                .compute_state
                .as_ref()
                .map_or(Duration::ZERO, |state| state.server_maintenance_interval);

            let now = Instant::now();
            // Determine if we need to perform maintenance, which is true if `maintenance_interval`
            // time has passed since the last maintenance.
            let sleep_duration;
            if now >= last_maintenance + maintenance_interval {
                last_maintenance = now;
                sleep_duration = None;

                // Report frontier information back the coordinator.
                if let Some(mut compute_state) = self.activate_compute() {
                    compute_state.compute_state.traces.maintenance();
                    // Report operator hydration before frontiers, as reporting frontiers may
                    // affect hydration reporting.
                    compute_state.report_operator_hydration();
                    compute_state.report_frontiers();
                    compute_state.report_dropped_collections();
                    compute_state.report_metrics();
                    compute_state.check_expiration();
                }

                self.metrics.record_shared_row_metrics();
            } else {
                // We didn't perform maintenance, sleep until the next maintenance interval.
                let next_maintenance = last_maintenance + maintenance_interval;
                sleep_duration = Some(next_maintenance.saturating_duration_since(now))
            };

            // Step the timely worker, recording the time taken.
            let timer = self.metrics.timely_step_duration_seconds.start_timer();
            self.timely_worker.step_or_park(sleep_duration);
            timer.observe_duration();

            self.handle_pending_commands()?;

            if let Some(mut compute_state) = self.activate_compute() {
                compute_state.process_peeks();
                compute_state.process_subscribes();
                compute_state.process_copy_tos();
            }
        }
    }

    fn handle_pending_commands(&mut self) -> Result<(), NonceChange> {
        while let Some(cmd) = self.command_rx.try_recv()? {
            self.handle_command(cmd);
        }
        Ok(())
    }

    fn handle_command(&mut self, cmd: ComputeCommand) {
        match &cmd {
            ComputeCommand::CreateInstance(_) => {
                self.compute_state = Some(ComputeState::new(
                    Arc::clone(&self.persist_clients),
                    self.txns_ctx.clone(),
                    self.metrics.clone(),
                    Arc::clone(&self.tracing_handle),
                    self.context.clone(),
                ));
            }
            _ => (),
        }
        self.activate_compute().unwrap().handle_compute_command(cmd);
    }

    fn activate_compute(&mut self) -> Option<ActiveComputeState<'_, A>> {
        if let Some(compute_state) = &mut self.compute_state {
            Some(ActiveComputeState {
                timely_worker: &mut *self.timely_worker,
                compute_state,
                response_tx: &mut self.response_tx,
            })
        } else {
            None
        }
    }

    /// Receive the next compute command.
    ///
    /// This method blocks if no command is currently available, but takes care to step the Timely
    /// worker while doing so.
    fn recv_command(&mut self) -> Result<ComputeCommand, NonceChange> {
        loop {
            if let Some(cmd) = self.command_rx.try_recv()? {
                return Ok(cmd);
            }

            let start = Instant::now();
            self.timely_worker.step_or_park(None);
            self.metrics
                .timely_step_duration_seconds
                .observe(start.elapsed().as_secs_f64());
        }
    }

    /// Extract commands until `InitializationComplete`, and make the worker reflect those commands.
    ///
    /// This method is meant to be a function of the commands received thus far (as recorded in the
    /// compute state command history) and the new commands from `command_rx`. It should not be a
    /// function of other characteristics, like whether the worker has managed to respond to a peek
    /// or not. Some effort goes in to narrowing our view to only the existing commands we can be sure
    /// are live at all other workers.
    ///
    /// The methodology here is to drain `command_rx` until an `InitializationComplete`, at which point
    /// the prior commands are "reconciled" in. Reconciliation takes each goal dataflow and looks for an
    /// existing "compatible" dataflow (per `compatible()`) it can repurpose, with some additional tests
    /// to be sure that we can cut over from one to the other (no additional compaction, no tails/sinks).
    /// With any connections established, old orphaned dataflows are allow to compact away, and any new
    /// dataflows are created from scratch. "Kept" dataflows are allowed to compact up to any new `as_of`.
    ///
    /// Some additional tidying happens, cleaning up pending peeks, reported frontiers, and creating a new
    /// subscribe response buffer. We will need to be vigilant with future modifications to `ComputeState` to
    /// line up changes there with clean resets here.
    fn reconcile(&mut self) -> Result<(), NonceChange> {
        // To initialize the connection, we want to drain all commands until we receive a
        // `ComputeCommand::InitializationComplete` command to form a target command state.
        let mut new_commands = Vec::new();
        loop {
            match self.recv_command()? {
                ComputeCommand::InitializationComplete => break,
                command => new_commands.push(command),
            }
        }

        // Commands we will need to apply before entering normal service.
        // These commands may include dropping existing dataflows, compacting existing dataflows,
        // and creating new dataflows, in addition to standard peek and compaction commands.
        // The result should be the same as if dropping all dataflows and running `new_commands`.
        let mut todo_commands = Vec::new();
        // We only have a compute history if we are in an initialized state
        // (i.e. after a `CreateInstance`).
        // If this is not the case, just copy `new_commands` into `todo_commands`.
        if let Some(compute_state) = &mut self.compute_state {
            // Reduce the installed commands.
            // Importantly, act as if all peeks may have been retired (as we cannot know otherwise).
            compute_state.command_history.discard_peeks();
            compute_state.command_history.reduce();

            // At this point, we need to sort out which of the *certainly installed* dataflows are
            // suitable replacements for the requested dataflows. A dataflow is "certainly installed"
            // as of a frontier if its compaction allows it to go no further. We ignore peeks for this
            // reasoning, as we cannot be certain that peeks still exist at any other worker.

            // Having reduced our installed command history retaining no peeks (above), we should be able
            // to use track down installed dataflows we can use as surrogates for requested dataflows (which
            // have retained all of their peeks, creating a more demanding `as_of` requirement).
            // NB: installed dataflows may still be allowed to further compact, and we should double check
            // this before being too confident. It should be rare without peeks, but could happen with e.g.
            // multiple outputs of a dataflow.

            // The values with which a prior `CreateInstance` was called, if it was.
            let mut old_instance_config = None;
            // Index dataflows by `export_ids().collect()`, as this is a precondition for their compatibility.
            let mut old_dataflows = BTreeMap::default();
            // Maintain allowed compaction, in case installed identifiers may have been allowed to compact.
            let mut old_frontiers = BTreeMap::default();
            for command in compute_state.command_history.iter() {
                match command {
                    ComputeCommand::CreateInstance(config) => {
                        old_instance_config = Some(config);
                    }
                    ComputeCommand::CreateDataflow(dataflow) => {
                        let export_ids = dataflow.export_ids().collect::<BTreeSet<_>>();
                        old_dataflows.insert(export_ids, dataflow);
                    }
                    ComputeCommand::AllowCompaction { id, frontier } => {
                        old_frontiers.insert(id, frontier);
                    }
                    _ => {
                        // Nothing to do in these cases.
                    }
                }
            }

            // Compaction commands that can be applied to existing dataflows.
            let mut old_compaction = BTreeMap::default();
            // Exported identifiers from dataflows we retain.
            let mut retain_ids = BTreeSet::default();

            // Traverse new commands, sorting out what remediation we can do.
            for command in new_commands.iter() {
                match command {
                    ComputeCommand::CreateDataflow(dataflow) => {
                        // Attempt to find an existing match for the dataflow.
                        let as_of = dataflow.as_of.as_ref().unwrap();
                        let export_ids = dataflow.export_ids().collect::<BTreeSet<_>>();

                        if let Some(old_dataflow) = old_dataflows.get(&export_ids) {
                            let compatible = old_dataflow.compatible_with(dataflow);
                            let uncompacted = !export_ids
                                .iter()
                                .flat_map(|id| old_frontiers.get(id))
                                .any(|frontier| {
                                    !timely::PartialOrder::less_equal(
                                        *frontier,
                                        dataflow.as_of.as_ref().unwrap(),
                                    )
                                });

                            // We cannot reconcile subscriptions at the moment, because the
                            // response buffer is shared, and to a first approximation must be
                            // completely reformed.
                            let subscribe_free = dataflow.subscribe_ids().next().is_none();

                            // If we have replaced any dependency of this dataflow, we need to
                            // replace this dataflow, to make it use the replacement.
                            let dependencies_retained = dataflow
                                .imported_index_ids()
                                .all(|id| retain_ids.contains(&id));

                            if compatible && uncompacted && subscribe_free && dependencies_retained
                            {
                                // Match found; remove the match from the deletion queue,
                                // and compact its outputs to the dataflow's `as_of`.
                                old_dataflows.remove(&export_ids);
                                for id in export_ids.iter() {
                                    old_compaction.insert(*id, as_of.clone());
                                }
                                retain_ids.extend(export_ids);
                            } else {
                                warn!(
                                    ?export_ids,
                                    ?compatible,
                                    ?uncompacted,
                                    ?subscribe_free,
                                    ?dependencies_retained,
                                    old_as_of = ?old_dataflow.as_of,
                                    new_as_of = ?as_of,
                                    "dataflow reconciliation failed",
                                );

                                // Dump the full dataflow plans if they are incompatible, to
                                // simplify debugging hard-to-reproduce reconciliation failures.
                                if !compatible {
                                    warn!(
                                        old = ?old_dataflow,
                                        new = ?dataflow,
                                        "incompatible dataflows in reconciliation",
                                    );
                                }

                                todo_commands
                                    .push(ComputeCommand::CreateDataflow(dataflow.clone()));
                            }

                            compute_state.metrics.record_dataflow_reconciliation(
                                compatible,
                                uncompacted,
                                subscribe_free,
                                dependencies_retained,
                            );
                        } else {
                            todo_commands.push(ComputeCommand::CreateDataflow(dataflow.clone()));
                        }
                    }
                    ComputeCommand::CreateInstance(config) => {
                        // Cluster creation should not be performed again!
                        if old_instance_config.map_or(false, |old| !old.compatible_with(config)) {
                            halt!(
                                "new instance configuration not compatible with existing instance configuration:\n{:?}\nvs\n{:?}",
                                config,
                                old_instance_config,
                            );
                        }
                    }
                    // All other commands we apply as requested.
                    command => {
                        todo_commands.push(command.clone());
                    }
                }
            }

            // Issue compaction commands first to reclaim resources.
            for (_, dataflow) in old_dataflows.iter() {
                for id in dataflow.export_ids() {
                    // We want to drop anything that has not yet been dropped,
                    // and nothing that has already been dropped.
                    if old_frontiers.get(&id) != Some(&&Antichain::new()) {
                        old_compaction.insert(id, Antichain::new());
                    }
                }
            }
            for (&id, frontier) in &old_compaction {
                let frontier = frontier.clone();
                todo_commands.insert(0, ComputeCommand::AllowCompaction { id, frontier });
            }

            // Clean up worker-local state.
            //
            // Various aspects of `ComputeState` need to be either uninstalled, or return to a blank slate.
            // All dropped dataflows should clean up after themselves, as we plan to install new dataflows
            // re-using the same identifiers.
            // All re-used dataflows should roll back any believed communicated information (e.g. frontiers)
            // so that they recommunicate that information as if from scratch.

            // Remove all pending peeks.
            for (_, peek) in std::mem::take(&mut compute_state.pending_peeks) {
                // Log dropping the peek request.
                if let Some(logger) = compute_state.compute_logger.as_mut() {
                    logger.log(&peek.as_log_event(false));
                }
            }

            // Clear the list of dropped collections.
            // We intended to report their dropping, but the controller does not expect to hear
            // about them anymore.
            compute_state.dropped_collections = Default::default();

            for (&id, collection) in compute_state.collections.iter_mut() {
                // Adjust reported frontiers:
                //  * For dataflows we continue to use, reset to ensure we report something not
                //    before the new `as_of` next.
                //  * For dataflows we drop, set to the empty frontier, to ensure we don't report
                //    anything for them.
                let retained = retain_ids.contains(&id);
                let compaction = old_compaction.remove(&id);
                let new_reported_frontier = match (retained, compaction) {
                    (true, Some(new_as_of)) => ReportedFrontier::NotReported { lower: new_as_of },
                    (true, None) => {
                        unreachable!("retained dataflows are compacted to the new as_of")
                    }
                    (false, Some(new_frontier)) => {
                        assert!(new_frontier.is_empty());
                        ReportedFrontier::Reported(new_frontier)
                    }
                    (false, None) => {
                        // Logging dataflows are implicitly retained and don't have a new as_of.
                        // Reset them to the minimal frontier.
                        ReportedFrontier::new()
                    }
                };

                collection.reset_reported_frontiers(new_reported_frontier);

                // Sink tokens should be retained for retained dataflows, and dropped for dropped
                // dataflows.
                //
                // Dropping the tokens of active subscribes makes them place `DroppedAt` responses
                // into the subscribe response buffer. We drop that buffer in the next step, which
                // ensures that we don't send out `DroppedAt` responses for subscribes dropped
                // during reconciliation.
                if !retained {
                    collection.sink_token = None;
                }
            }

            // We must drop the subscribe response buffer as it is global across all subscribes.
            // If it were broken out by `GlobalId` then we could drop only those of dataflows we drop.
            compute_state.subscribe_response_buffer = Rc::new(RefCell::new(Vec::new()));

            // The controller expects the logging collections to be readable from the minimum time
            // initially. We cannot recreate the logging arrangements without restarting the
            // instance, but we can pad the compacted times with empty data. Doing so is sound
            // because logging collections from different replica incarnations are considered
            // distinct TVCs, so the controller doesn't expect any historical consistency from
            // these collections when it reconnects to a replica.
            //
            // TODO(database-issues#8152): Consider resolving this with controller-side reconciliation instead.
            if let Some(config) = old_instance_config {
                for id in config.logging.index_logs.values() {
                    let trace = compute_state
                        .traces
                        .remove(id)
                        .expect("logging trace exists");
                    let padded = trace.into_padded();
                    compute_state.traces.set(*id, padded);
                }
            }
        } else {
            todo_commands.clone_from(&new_commands);
        }

        // Execute the commands to bring us to `new_commands`.
        for command in todo_commands.into_iter() {
            self.handle_command(command);
        }

        // Overwrite `self.command_history` to reflect `new_commands`.
        // It is possible that there still isn't a compute state yet.
        if let Some(compute_state) = &mut self.compute_state {
            let mut command_history = ComputeCommandHistory::new(self.metrics.for_history());
            for command in new_commands.iter() {
                command_history.push(command.clone());
            }
            compute_state.command_history = command_history;
        }
        Ok(())
    }
}

/// Spawn a thread to bridge between [`ClusterClient`] and [`Worker`] channels.
///
/// The [`Worker`] expects a pair of persistent channels, with punctuation marking reconnects,
/// while the [`ClusterClient`] provides a new pair of channels on each reconnect.
fn spawn_channel_adapter(
    client_rx: crossbeam_channel::Receiver<(
        Uuid,
        crossbeam_channel::Receiver<ComputeCommand>,
        mpsc::UnboundedSender<ComputeResponse>,
    )>,
    command_tx: command_channel::Sender,
    response_rx: crossbeam_channel::Receiver<(ComputeResponse, Uuid)>,
    worker_id: usize,
) {
    thread::Builder::new()
        // "cca" stands for "compute channel adapter". We need to shorten that because Linux has a
        // 15-character limit for thread names.
        .name(format!("cca-{worker_id}"))
        .spawn(move || {
            // To make workers aware of the individual client connections, we tag forwarded
            // commands with the client nonce. Additionally, we use the nonce to filter out
            // responses with a different nonce, which are intended for different client
            // connections.
            //
            // It's possible that we receive responses with nonces from the past but also from the
            // future: Worker 0 might have received a new nonce before us and broadcasted it to our
            // Timely cluster. When we receive a response with a future nonce, we need to wait with
            // forwarding it until we have received the same nonce from a client connection.
            //
            // Nonces are not ordered so we don't know whether a response nonce is from the past or
            // the future. We thus assume that every response with an unknown nonce might be from
            // the future and stash them all. Every time we reconnect, we immediately send all
            // stashed responses with a matching nonce. Every time we receive a new response with a
            // nonce that matches our current one, we can discard the entire response stash as we
            // know that all stashed responses must be from the past.
            let mut stashed_responses = BTreeMap::<Uuid, Vec<ComputeResponse>>::new();

            while let Ok((nonce, command_rx, response_tx)) = client_rx.recv() {
                // Send stashed responses for this client.
                if let Some(resps) = stashed_responses.remove(&nonce) {
                    for resp in resps {
                        let _ = response_tx.send(resp);
                    }
                }

                // Wait for a new response while forwarding received commands.
                let serve_rx_channels = || loop {
                    crossbeam_channel::select! {
                        recv(command_rx) -> msg => match msg {
                            Ok(cmd) => command_tx.send((cmd, nonce)),
                            Err(_) => return Err(()),
                        },
                        recv(response_rx) -> msg => {
                            return Ok(msg.expect("worker connected"));
                        }
                    }
                };

                // Serve this connection until we see any of the channels disconnect.
                loop {
                    let Ok((resp, resp_nonce)) = serve_rx_channels() else {
                        break;
                    };

                    if resp_nonce == nonce {
                        // Response for the current connection; forward it.
                        stashed_responses.clear();
                        if response_tx.send(resp).is_err() {
                            break;
                        }
                    } else {
                        // Response for a past or future connection; stash it.
                        let stash = stashed_responses.entry(resp_nonce).or_default();
                        stash.push(resp);
                    }
                }
            }
        })
        .unwrap();
}
