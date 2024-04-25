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
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Error;
use crossbeam_channel::{RecvError, TryRecvError};
use mz_cluster::server::TimelyContainerRef;
use mz_compute_client::protocol::command::ComputeCommand;
use mz_compute_client::protocol::history::ComputeCommandHistory;
use mz_compute_client::protocol::response::ComputeResponse;
use mz_compute_client::service::ComputeClient;
use mz_compute_types::dataflows::{BuildDesc, DataflowDescription};
use mz_ore::cast::CastFrom;
use mz_ore::halt;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use timely::communication::Allocate;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::Operator;
use timely::progress::{Antichain, Timestamp};
use timely::scheduling::{Scheduler, SyncActivator};
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::{info, trace, warn};

use crate::compute_state::{ActiveComputeState, ComputeState, ReportedFrontier};
use crate::metrics::ComputeMetrics;

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
pub struct Config {
    /// Metrics exposed by compute replicas.
    // TODO(guswynn): cluster-unification: ensure these stats
    // also work for storage when merging.
    pub metrics: ComputeMetrics,
    /// Other configuration for compute.
    pub context: ComputeInstanceContext,
}

/// Initiates a timely dataflow computation, processing compute commands.
pub fn serve(
    config: mz_cluster::server::ClusterConfig,
    context: ComputeInstanceContext,
) -> Result<
    (
        TimelyContainerRef<ComputeCommand, ComputeResponse, SyncActivator>,
        impl Fn() -> Box<dyn ComputeClient>,
    ),
    Error,
> {
    let metrics = ComputeMetrics::register_with(&config.metrics_registry);
    let compute_config = Config { metrics, context };

    let (timely_container, client_builder) = mz_cluster::server::serve::<
        Config,
        ComputeCommand,
        ComputeResponse,
    >(config, compute_config)?;
    let client_builder = {
        move || {
            let client: Box<dyn ComputeClient> = client_builder();
            client
        }
    };

    Ok((timely_container, client_builder))
}

type ActivatorSender = crossbeam_channel::Sender<SyncActivator>;

/// Endpoint used by workers to receive compute commands.
struct CommandReceiver {
    inner: crossbeam_channel::Receiver<ComputeCommand>,
    worker_id: usize,
}

impl CommandReceiver {
    fn new(inner: crossbeam_channel::Receiver<ComputeCommand>, worker_id: usize) -> Self {
        Self { inner, worker_id }
    }

    fn try_recv(&self) -> Result<ComputeCommand, TryRecvError> {
        self.inner.try_recv().map(|cmd| {
            trace!(worker = ?self.worker_id, command = ?cmd, "received command");
            cmd
        })
    }
}

/// Endpoint used by workers to send sending compute responses.
pub(crate) struct ResponseSender {
    inner: mpsc::UnboundedSender<ComputeResponse>,
    worker_id: usize,
}

impl ResponseSender {
    fn new(inner: mpsc::UnboundedSender<ComputeResponse>, worker_id: usize) -> Self {
        Self { inner, worker_id }
    }

    pub fn send(&self, response: ComputeResponse) -> Result<(), SendError<ComputeResponse>> {
        trace!(worker = ?self.worker_id, response = ?response, "sending response");
        self.inner.send(response)
    }
}

struct CommandReceiverQueue {
    queue: Rc<RefCell<VecDeque<Result<ComputeCommand, TryRecvError>>>>,
}

impl CommandReceiverQueue {
    fn try_recv(&mut self) -> Result<ComputeCommand, TryRecvError> {
        match self.queue.borrow_mut().pop_front() {
            Some(Ok(cmd)) => Ok(cmd),
            Some(Err(e)) => Err(e),
            None => Err(TryRecvError::Empty),
        }
    }

    /// Block until a command is available.
    /// This method takes the worker as an argument such that it can step timely while no result
    /// is available.
    fn recv<A: Allocate>(&mut self, worker: &mut Worker<A>) -> Result<ComputeCommand, RecvError> {
        while self.is_empty() {
            let start = Instant::now();
            worker.timely_worker.step_or_park(None);
            worker
                .metrics
                .timely_step_duration_seconds
                .observe(start.elapsed().as_secs_f64());
        }
        match self.try_recv() {
            Ok(cmd) => Ok(cmd),
            Err(TryRecvError::Disconnected) => Err(RecvError),
            Err(TryRecvError::Empty) => unreachable!("checked above"),
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.borrow().is_empty()
    }
}

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
struct Worker<'w, A: Allocate> {
    /// The underlying Timely worker.
    timely_worker: &'w mut TimelyWorker<A>,
    /// The channel over which communication handles for newly connected clients
    /// are delivered.
    client_rx: crossbeam_channel::Receiver<(
        crossbeam_channel::Receiver<ComputeCommand>,
        mpsc::UnboundedSender<ComputeResponse>,
        ActivatorSender,
    )>,
    compute_state: Option<ComputeState>,
    /// Compute metrics.
    metrics: ComputeMetrics,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers
    persist_clients: Arc<PersistClientCache>,
    /// Context necessary for rendering txn-wal operators.
    txns_ctx: TxnsContext,
    /// A process-global handle to tracing configuration.
    tracing_handle: Arc<TracingHandle>,
    context: ComputeInstanceContext,
}

impl mz_cluster::types::AsRunnableWorker<ComputeCommand, ComputeResponse> for Config {
    type Activatable = SyncActivator;
    fn build_and_run<A: Allocate + 'static>(
        config: Self,
        timely_worker: &mut TimelyWorker<A>,
        client_rx: crossbeam_channel::Receiver<(
            crossbeam_channel::Receiver<ComputeCommand>,
            tokio::sync::mpsc::UnboundedSender<ComputeResponse>,
            ActivatorSender,
        )>,
        persist_clients: Arc<PersistClientCache>,
        txns_ctx: TxnsContext,
        tracing_handle: Arc<TracingHandle>,
    ) {
        if config.context.worker_core_affinity {
            set_core_affinity(timely_worker.index());
        }

        Worker {
            timely_worker,
            client_rx,
            metrics: config.metrics,
            context: config.context,
            persist_clients,
            txns_ctx,
            compute_state: None,
            tracing_handle,
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
    /// Waits for client connections and runs them to completion.
    pub fn run(&mut self) {
        let mut shutdown = false;
        while !shutdown {
            match self.client_rx.recv() {
                Ok((rx, tx, activator_tx)) => {
                    self.setup_channel_and_run_client(rx, tx, activator_tx)
                }
                Err(_) => shutdown = true,
            }
        }
    }

    fn split_command<T: Timestamp>(
        command: ComputeCommand<T>,
        parts: usize,
    ) -> Vec<ComputeCommand<T>> {
        match command {
            ComputeCommand::CreateDataflow(dataflow) => {
                // A list of descriptions of objects for each part to build.
                let mut builds_parts = vec![Vec::new(); parts];
                // Partition each build description among `parts`.
                for build_desc in dataflow.objects_to_build {
                    let build_part = build_desc.plan.partition_among(parts);
                    for (plan, objects_to_build) in
                        build_part.into_iter().zip(builds_parts.iter_mut())
                    {
                        objects_to_build.push(BuildDesc {
                            id: build_desc.id,
                            plan,
                        });
                    }
                }

                // Each list of build descriptions results in a dataflow description.
                builds_parts
                    .into_iter()
                    .map(|objects_to_build| DataflowDescription {
                        source_imports: dataflow.source_imports.clone(),
                        index_imports: dataflow.index_imports.clone(),
                        objects_to_build,
                        index_exports: dataflow.index_exports.clone(),
                        sink_exports: dataflow.sink_exports.clone(),
                        as_of: dataflow.as_of.clone(),
                        until: dataflow.until.clone(),
                        debug_name: dataflow.debug_name.clone(),
                        initial_storage_as_of: dataflow.initial_storage_as_of.clone(),
                        refresh_schedule: dataflow.refresh_schedule.clone(),
                    })
                    .map(ComputeCommand::CreateDataflow)
                    .collect()
            }
            command => vec![command; parts],
        }
    }

    fn setup_channel_and_run_client(
        &mut self,
        command_rx: crossbeam_channel::Receiver<ComputeCommand>,
        response_tx: mpsc::UnboundedSender<ComputeResponse>,
        activator_tx: ActivatorSender,
    ) {
        let cmd_queue = Rc::new(RefCell::new(
            VecDeque::<Result<ComputeCommand, TryRecvError>>::new(),
        ));
        let peers = self.timely_worker.peers();
        let worker_id = self.timely_worker.index();

        let command_rx = CommandReceiver::new(command_rx, worker_id);
        let response_tx = ResponseSender::new(response_tx, worker_id);

        self.timely_worker.dataflow::<u64, _, _>({
            let cmd_queue = Rc::clone(&cmd_queue);

            move |scope| {
                let mut container = Default::default();
                source(scope, "CmdSource", |capability, info| {
                    // Send activator for this operator back
                    let activator = scope.sync_activator_for(&info.address[..]);
                    activator_tx.send(activator).expect("activator_tx working");

                    //Hold onto capbility until we receive a disconnected error
                    let mut cap_opt = Some(capability);
                    // Drop capability if we are not the leader, as our queue will
                    // be empty and we will never use nor importantly downgrade it.
                    if worker_id != 0 {
                        cap_opt = None;
                    }

                    move |output| {
                        let mut disconnected = false;
                        if let Some(cap) = cap_opt.as_mut() {
                            let time = cap.time().clone();
                            let mut session = output.session(&cap);

                            loop {
                                match command_rx.try_recv() {
                                    Ok(cmd) => {
                                        // Commands must never be accepted from another worker. This
                                        // implementation does not guarantee an ordering of events
                                        // sent to different workers.
                                        assert_eq!(worker_id, 0);
                                        session.give_iterator(
                                            Self::split_command(cmd, peers).into_iter().enumerate(),
                                        );
                                    }
                                    Err(TryRecvError::Disconnected) => {
                                        disconnected = true;
                                        break;
                                    }
                                    Err(TryRecvError::Empty) => {
                                        break;
                                    }
                                };
                            }
                            cap.downgrade(&(time + 1));
                        } else {
                            // Non-leader workers will still receive `UpdateConfiguration` commands
                            // and we must drain those to not leak memory.
                            if let Ok(cmd) = command_rx.try_recv() {
                                assert_ne!(worker_id, 0);
                                assert!(matches!(cmd, ComputeCommand::UpdateConfiguration(_)));
                            }
                        }

                        if disconnected {
                            cap_opt = None;
                        }
                    }
                })
                .sink(
                    Exchange::new(|(idx, _)| u64::cast_from(*idx)),
                    "CmdReceiver",
                    move |input| {
                        let mut queue = cmd_queue.borrow_mut();
                        if input.frontier().is_empty() {
                            queue.push_back(Err(TryRecvError::Disconnected))
                        }
                        while let Some((_, data)) = input.next() {
                            data.swap(&mut container);
                            for (_, cmd) in container.drain(..) {
                                queue.push_back(Ok(cmd));
                            }
                        }
                    },
                );
            }
        });

        self.run_client(
            CommandReceiverQueue {
                queue: Rc::clone(&cmd_queue),
            },
            response_tx,
        )
    }

    /// Draws commands from a single client until disconnected.
    fn run_client(
        &mut self,
        mut command_rx: CommandReceiverQueue,
        mut response_tx: ResponseSender,
    ) {
        if let Err(_) = self.reconcile(&mut command_rx, &mut response_tx) {
            return;
        }

        // Commence normal operation.
        let mut shutdown = false;
        while !shutdown {
            // Enable trace compaction.
            if let Some(compute_state) = &mut self.compute_state {
                compute_state.traces.maintenance();
            }

            let start = Instant::now();
            self.timely_worker.step_or_park(None);
            self.metrics
                .timely_step_duration_seconds
                .observe(start.elapsed().as_secs_f64());

            // Report frontier information back the coordinator.
            if let Some(mut compute_state) = self.activate_compute(&mut response_tx) {
                compute_state.report_frontiers();
                compute_state.report_dropped_collections();
                compute_state.report_operator_hydration();
            }

            // Handle any received commands.
            let mut cmds = vec![];
            let mut empty = false;
            while !empty {
                match command_rx.try_recv() {
                    Ok(cmd) => cmds.push(cmd),
                    Err(TryRecvError::Empty) => empty = true,
                    Err(TryRecvError::Disconnected) => {
                        empty = true;
                        shutdown = true;
                    }
                }
            }
            for cmd in cmds {
                self.handle_command(&mut response_tx, cmd);
            }

            if let Some(mut compute_state) = self.activate_compute(&mut response_tx) {
                compute_state.process_peeks();
                compute_state.process_subscribes();
                compute_state.process_copy_tos();
            }

            self.metrics
                .record_shared_row_metrics(self.timely_worker.index());
        }
    }

    fn handle_command(&mut self, response_tx: &mut ResponseSender, cmd: ComputeCommand) {
        match &cmd {
            ComputeCommand::CreateInstance(_) => {
                self.compute_state = Some(ComputeState::new(
                    self.timely_worker.index(),
                    Arc::clone(&self.persist_clients),
                    self.txns_ctx.clone(),
                    self.metrics.clone(),
                    Arc::clone(&self.tracing_handle),
                    self.context.clone(),
                ));
            }
            _ => (),
        }
        self.activate_compute(response_tx)
            .unwrap()
            .handle_compute_command(cmd);
    }

    fn activate_compute<'a>(
        &'a mut self,
        response_tx: &'a mut ResponseSender,
    ) -> Option<ActiveComputeState<'a, A>> {
        if let Some(compute_state) = &mut self.compute_state {
            Some(ActiveComputeState {
                timely_worker: &mut *self.timely_worker,
                compute_state,
                response_tx,
            })
        } else {
            None
        }
    }

    /// Extract commands until `InitializationComplete`, and make the worker reflect those commands.
    /// If the worker can not be made to reflect the commands, exit the process.
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
    fn reconcile(
        &mut self,
        command_rx: &mut CommandReceiverQueue,
        response_tx: &mut ResponseSender,
    ) -> Result<(), RecvError> {
        let worker_id = self.timely_worker.index();

        // To initialize the connection, we want to drain all commands until we receive a
        // `ComputeCommand::InitializationComplete` command to form a target command state.
        let mut new_commands = Vec::new();
        loop {
            match command_rx.recv(self)? {
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
            // have retained all of their peeks, creating a more demainding `as_of` requirement).
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
                            // We cannot reconcile subscriptions at the moment, because the response buffer is shared,
                            // and to a first approximation must be completely reformed.
                            let subscribe_free = dataflow
                                .sink_exports
                                .iter()
                                .all(|(_id, sink)| !sink.connection.is_subscribe());
                            if compatible && uncompacted && subscribe_free {
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
                                    old_as_of = ?old_dataflow.as_of,
                                    new_as_of = ?as_of,
                                    "dataflow reconciliation failed",
                                );
                                todo_commands
                                    .push(ComputeCommand::CreateDataflow(dataflow.clone()));
                            }

                            compute_state.metrics.record_dataflow_reconciliation(
                                worker_id,
                                compatible,
                                uncompacted,
                                subscribe_free,
                            );
                        } else {
                            todo_commands.push(ComputeCommand::CreateDataflow(dataflow.clone()));
                        }
                    }
                    ComputeCommand::CreateInstance(config) => {
                        // Cluster creation should not be performed again!
                        if Some(config) != old_instance_config {
                            halt!(
                                "new instance configuration does not match existing instance configuration:\n{:?}\nvs\n{:?}",
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
                    logger.log(peek.as_log_event(false));
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
            // TODO(#27730): Consider resolving this with controller-side reconciliation instead.
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
            self.handle_command(response_tx, command);
        }

        // Overwrite `self.command_history` to reflect `new_commands`.
        // It is possible that there still isn't a compute state yet.
        if let Some(compute_state) = &mut self.compute_state {
            let mut command_history =
                ComputeCommandHistory::new(self.metrics.for_history(worker_id));
            for command in new_commands.iter() {
                command_history.push(command.clone());
            }
            compute_state.command_history = command_history;
        }
        Ok(())
    }
}
