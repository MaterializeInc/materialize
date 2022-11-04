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
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::fmt::{Debug, Formatter};
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Error};
use async_trait::async_trait;
use crossbeam_channel::{RecvError, TryRecvError};
use futures::future;
use timely::communication::initialize::WorkerGuards;
use timely::communication::Allocate;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::source;
use timely::dataflow::operators::Operator;
use timely::execute::execute_from;
use timely::progress::Timestamp;
use timely::scheduling::{Scheduler, SyncActivator};
use timely::worker::Worker as TimelyWorker;
use timely::WorkerConfig;
use tokio::sync::mpsc;

use mz_build_info::BuildInfo;
use mz_compute_client::command::CommunicationConfig;
use mz_compute_client::command::{
    BuildDesc, ComputeCommand, ComputeCommandHistory, DataflowDescription,
};
use mz_compute_client::response::ComputeResponse;
use mz_compute_client::service::ComputeClient;
use mz_ore::halt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::PersistConfig;
use mz_service::client::{GenericClient, Partitioned};
use mz_service::local::LocalClient;
use tracing::info;

use crate::communication::initialize_networking;
use crate::compute_state::ActiveComputeState;
use crate::compute_state::ComputeState;
use crate::{TraceManager, TraceMetrics};

/// Configures a dataflow server.
#[derive(Debug)]
pub struct Config {
    /// Build information.
    pub build_info: &'static BuildInfo,
    /// Function to get wall time now.
    pub now: NowFn,
    /// Metrics registry through which dataflow metrics will be reported.
    pub metrics_registry: MetricsRegistry,
}

/// A client managing access to the local portion of a Timely cluster
struct ClusterClient<C> {
    /// The actual client to talk to the cluster
    inner: Option<C>,
    /// The running timely instance
    timely_container: TimelyContainerRef,
    /// The dataflow trace metrics.
    trace_metrics: TraceMetrics,
    /// Handle to the persist infrastructure.
    persist_clients: Arc<tokio::sync::Mutex<PersistClientCache>>,
    /// The handle to the Tokio runtime.
    tokio_handle: tokio::runtime::Handle,
}

/// Metadata about timely workers in this process.
pub struct TimelyContainer {
    /// The current communication config in use
    comm_config: CommunicationConfig,
    /// Channels over which to send endpoints for wiring up a new Client
    client_txs: Vec<
        crossbeam_channel::Sender<(
            crossbeam_channel::Receiver<ComputeCommand>,
            mpsc::UnboundedSender<ComputeResponse>,
            crossbeam_channel::Sender<SyncActivator>,
        )>,
    >,
    /// Thread guards that keep worker threads alive
    worker_guards: WorkerGuards<()>,
}

/// Threadsafe reference to an optional TimelyContainer
pub type TimelyContainerRef = Arc<Mutex<Option<TimelyContainer>>>;

/// Initiates a timely dataflow computation, processing compute commands.
pub fn serve(
    config: Config,
) -> Result<(TimelyContainerRef, impl Fn() -> Box<dyn ComputeClient>), Error> {
    // Various metrics related things.
    let trace_metrics = TraceMetrics::register_with(&config.metrics_registry);

    let persist_clients = PersistClientCache::new(
        PersistConfig::new(config.build_info, config.now.clone()),
        &config.metrics_registry,
    );
    let persist_clients = Arc::new(tokio::sync::Mutex::new(persist_clients));
    let tokio_executor = tokio::runtime::Handle::current();
    let timely_container = Arc::new(Mutex::new(None));
    let client_builder = {
        let timely_container = Arc::clone(&timely_container);
        move || {
            let client = ClusterClient::new(
                Arc::clone(&timely_container),
                trace_metrics.clone(),
                Arc::clone(&persist_clients),
                tokio_executor.clone(),
            );
            Box::new(client) as Box<dyn ComputeClient>
        }
    };

    Ok((timely_container, client_builder))
}

impl ClusterClient<PartitionedClient> {
    fn new(
        timely_container: TimelyContainerRef,
        trace_metrics: TraceMetrics,
        persist_clients: Arc<tokio::sync::Mutex<PersistClientCache>>,
        tokio_handle: tokio::runtime::Handle,
    ) -> Self {
        Self {
            timely_container,
            inner: None,
            trace_metrics,
            persist_clients,
            tokio_handle,
        }
    }

    async fn build_timely(
        &mut self,
        comm_config: CommunicationConfig,
    ) -> Result<TimelyContainer, Error> {
        info!("Building timely container with config {comm_config:?}");
        let (client_txs, client_rxs): (Vec<_>, Vec<_>) = (0..comm_config.workers)
            .map(|_| crossbeam_channel::unbounded())
            .unzip();
        let client_rxs: Mutex<Vec<_>> = Mutex::new(client_rxs.into_iter().map(Some).collect());

        let (builders, other) = initialize_networking(&comm_config)
            .await
            .map_err(|e| anyhow!("{e}"))?;

        let workers = comm_config.workers;
        let trace_metrics = self.trace_metrics.clone();
        let persist_clients = Arc::clone(&self.persist_clients);
        let tokio_executor = self.tokio_handle.clone();
        let worker_guards = execute_from(
            builders,
            other,
            WorkerConfig::default(),
            move |timely_worker| {
                let timely_worker_index = timely_worker.index();
                let _tokio_guard = tokio_executor.enter();
                let client_rx = client_rxs.lock().unwrap()[timely_worker_index % workers]
                    .take()
                    .unwrap();
                let _trace_metrics = trace_metrics.clone();
                let persist_clients = Arc::clone(&persist_clients);
                Worker {
                    timely_worker,
                    client_rx,
                    compute_state: None,
                    trace_metrics: trace_metrics.clone(),
                    persist_clients,
                }
                .run()
            },
        )
        .map_err(|e| anyhow!("{e}"))?;

        Ok(TimelyContainer {
            comm_config,
            client_txs,
            worker_guards,
        })
    }

    async fn build(&mut self, comm_config: CommunicationConfig) -> Result<(), Error> {
        let workers = comm_config.workers;

        // Check if we can reuse the existing timely instance.
        // We currently do not support reinstantiating timely, we simply panic
        // if another communication config is requested. This
        // code must panic before dropping the worker guards contained in timely_container.
        // As we don't terminate timely workers, the thread join would hang forever, possibly
        // creating a fair share of confusion in the orchestrator.

        let timely = self.timely_container.lock().unwrap().take();
        let timely = match timely {
            Some(existing) => {
                if comm_config != existing.comm_config {
                    halt!(
                        "new timely configuration does not match existing timely configuration:\n{:?}\nvs\n{:?}",
                        comm_config,
                        existing.comm_config,
                    );
                }
                info!("Timely already initialized; re-using.");
                existing
            }
            None => self.build_timely(comm_config).await?,
        };

        let (command_txs, command_rxs): (Vec<_>, Vec<_>) =
            (0..workers).map(|_| crossbeam_channel::unbounded()).unzip();
        let (response_txs, response_rxs): (Vec<_>, Vec<_>) =
            (0..workers).map(|_| mpsc::unbounded_channel()).unzip();
        let activators = timely
            .client_txs
            .iter()
            .zip(command_rxs)
            .zip(response_txs)
            .map(|((client_tx, cmd_rx), resp_tx)| {
                let (activator_tx, activator_rx) = crossbeam_channel::unbounded();
                client_tx
                    .send((cmd_rx, resp_tx, activator_tx))
                    .expect("worker should not drop first");

                activator_rx.recv().unwrap()
            })
            .collect();

        self.inner = Some(LocalClient::new_partitioned(
            response_rxs,
            command_txs,
            activators,
        ));

        *self.timely_container.lock().unwrap() = Some(timely);
        Ok(())
    }
}

impl<C: Debug> Debug for ClusterClient<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterClient")
            .field("trace_metrics", &self.trace_metrics)
            .field("persist_clients", &self.persist_clients)
            .field("inner", &self.inner)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl GenericClient<ComputeCommand, ComputeResponse> for ClusterClient<PartitionedClient> {
    async fn send(&mut self, cmd: ComputeCommand) -> Result<(), Error> {
        // Changing this debug statement requires changing the replica-isolation test
        tracing::debug!("ClusterClient send={:?}", &cmd);
        match cmd {
            ComputeCommand::CreateTimely(comm_config) => self.build(comm_config).await,
            ComputeCommand::DropInstance => {
                self.inner.as_mut().expect("intialized").send(cmd).await?;
                self.inner = None;
                let _ = self
                    .timely_container
                    .lock()
                    .unwrap()
                    .take()
                    .expect("Running instance") // Maybe better to send an error back in this case?
                    .worker_guards
                    .join();
                Ok(())
            }
            _ => self.inner.as_mut().expect("intialized").send(cmd).await,
        }
    }

    async fn recv(&mut self) -> Result<Option<ComputeResponse>, Error> {
        if let Some(client) = self.inner.as_mut() {
            client.recv().await
        } else {
            future::pending().await
        }
    }
}

type CommandReceiver = crossbeam_channel::Receiver<ComputeCommand>;
type ResponseSender = mpsc::UnboundedSender<ComputeResponse>;
type ActivatorSender = crossbeam_channel::Sender<SyncActivator>;

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
            worker.timely_worker.step_or_park(None);
        }
        match self
            .queue
            .borrow_mut()
            .pop_front()
            .expect("Must contain element")
        {
            Ok(cmd) => Ok(cmd),
            Err(TryRecvError::Disconnected) => Err(RecvError),
            Err(TryRecvError::Empty) => panic!("Must not be empty"),
        }
    }

    fn is_empty(&self) -> bool {
        self.queue.borrow().is_empty()
    }
}
type PartitionedClient = Partitioned<
    LocalClient<ComputeCommand, ComputeResponse, SyncActivator>,
    ComputeCommand,
    ComputeResponse,
>;

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
struct Worker<'w, A: Allocate> {
    /// The underlying Timely worker.
    timely_worker: &'w mut TimelyWorker<A>,
    /// The channel over which communication handles for newly connected clients
    /// are delivered.
    client_rx: crossbeam_channel::Receiver<(CommandReceiver, ResponseSender, ActivatorSender)>,
    compute_state: Option<ComputeState>,
    /// Trace metrics.
    trace_metrics: TraceMetrics,
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    /// This is intentionally shared between workers
    persist_clients: Arc<tokio::sync::Mutex<PersistClientCache>>,
}

impl<'w, A: Allocate> Worker<'w, A> {
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
            ComputeCommand::CreateDataflows(dataflows) => {
                let mut dataflows_parts = vec![Vec::new(); parts];

                for dataflow in dataflows {
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
                            until: dataflow.until.clone(),
                            debug_name: dataflow.debug_name.clone(),
                        });
                    }
                }
                dataflows_parts
                    .into_iter()
                    .map(ComputeCommand::CreateDataflows)
                    .collect()
            }
            command => vec![command; parts],
        }
    }

    fn setup_channel_and_run_client(
        &mut self,
        command_rx: CommandReceiver,
        response_tx: ResponseSender,
        activator_tx: ActivatorSender,
    ) {
        let cmd_queue = Rc::new(RefCell::new(
            VecDeque::<Result<ComputeCommand, TryRecvError>>::new(),
        ));
        let peers = self.timely_worker.peers();
        let idx = self.timely_worker.index();

        {
            let cmd_queue = Rc::clone(&cmd_queue);

            self.timely_worker.dataflow::<u64, _, _>(move |scope| {
                source(scope, "CmdSource", |capability, info| {
                    // Send activator for this operator back
                    let activator = scope.sync_activator_for(&info.address[..]);
                    activator_tx.send(activator).expect("activator_tx working");

                    //Hold onto capbility until we receive a disconnected error
                    let mut cap_opt = Some(capability);
                    // Drop capability if we are not the leader, as our queue will
                    // be empty and we will never use nor importantly downgrade it.
                    if idx != 0 {
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
                                        // Commands must never be sent to another worker. This
                                        // implementation does not guarantee an ordering of events
                                        // sent to different workers.
                                        assert_eq!(idx, 0);
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
                        }

                        if disconnected {
                            cap_opt = None;
                        }
                    }
                })
                .unary_frontier::<Vec<()>, _, _, _>(
                    Exchange::new(|(idx, _)| *idx as u64),
                    "CmdReceiver",
                    |_, _| {
                        let mut container = Default::default();
                        move |input, _| {
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
                        }
                    },
                );
            });
        }

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

            self.timely_worker.step_or_park(None);

            // Report frontier information back the coordinator.
            if let Some(mut compute_state) = self.activate_compute(&mut response_tx) {
                compute_state.report_compute_frontiers();
                compute_state.report_dropped_collections();
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
            }
        }
    }

    #[allow(dead_code)]
    fn shut_down(&mut self, response_tx: &mut ResponseSender) {
        if let Some(mut compute_state) = self.activate_compute(response_tx) {
            compute_state.compute_state.traces.del_all_traces();
            compute_state.shutdown_logging();
        }
    }

    fn handle_command(&mut self, response_tx: &mut ResponseSender, cmd: ComputeCommand) {
        let mut should_drop_compute = false;
        match &cmd {
            ComputeCommand::CreateInstance(config) => {
                self.compute_state = Some(ComputeState {
                    traces: TraceManager::new(
                        self.trace_metrics.clone(),
                        self.timely_worker.index(),
                    ),
                    sink_tokens: HashMap::new(),
                    subscribe_response_buffer: std::rc::Rc::new(
                        std::cell::RefCell::new(Vec::new()),
                    ),
                    sink_write_frontiers: HashMap::new(),
                    pending_peeks: HashMap::new(),
                    reported_frontiers: HashMap::new(),
                    dropped_collections: Vec::new(),
                    compute_logger: None,
                    persist_clients: Arc::clone(&self.persist_clients),
                    command_history: ComputeCommandHistory::default(),
                    max_result_size: config.max_result_size,
                });
            }
            ComputeCommand::DropInstance => {
                should_drop_compute = true;
            }
            _ => (),
        }
        self.activate_compute(response_tx)
            .unwrap()
            .handle_compute_command(cmd);
        if should_drop_compute {
            self.compute_state = None;
        }
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
        // Exported identifiers from dataflows we retain.
        let mut retain_ids = BTreeSet::default();

        // We only have a compute history if we are in an initialized state
        // e.g. before a `CreateInstance` or after a `DropInstance`).
        // If this is not the case, just copy `new_commands` into `todo_commands`.
        if let Some(compute_state) = &mut self.compute_state {
            // Reduce the installed commands.
            // Importantly, act as if all peeks may have been retired (as we cannot know otherwise).
            compute_state
                .command_history
                .retain_peeks(&HashMap::<_, ()>::default());
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

            // The configuration with which a prior `CreateInstance` was called, if it was.
            let mut old_config = None;
            // Index dataflows by `export_ids().collect()`, as this is a precondition for their compatibility.
            let mut old_dataflows = BTreeMap::default();
            // Maintain allowed compaction, in case installed identifiers may have been allowed to compact.
            let mut old_frontiers = BTreeMap::default();
            for command in compute_state.command_history.iter() {
                match command {
                    ComputeCommand::CreateInstance(config) => {
                        old_config = Some(config);
                    }
                    ComputeCommand::CreateDataflows(dataflows) => {
                        for dataflow in dataflows.iter() {
                            let export_ids = dataflow.export_ids().collect::<BTreeSet<_>>();
                            old_dataflows.insert(export_ids, dataflow);
                        }
                    }
                    ComputeCommand::AllowCompaction(frontiers) => {
                        for (id, frontier) in frontiers.iter() {
                            old_frontiers.insert(id, frontier);
                        }
                    }
                    _ => {
                        // Nothing to do in these cases.
                    }
                }
            }

            // Compaction commands that can be applied to existing dataflows.
            let mut old_compaction = BTreeMap::default();

            // Traverse new commands, sorting out what remediation we can do.
            for command in new_commands.iter() {
                match command {
                    ComputeCommand::CreateDataflows(dataflows) => {
                        // Track dataflow we must build anew.
                        let mut new_dataflows = Vec::new();

                        // Attempt to find an existing match for each dataflow.
                        for dataflow in dataflows.iter() {
                            let export_ids = dataflow.export_ids().collect::<BTreeSet<_>>();
                            if let Some(old_dataflow) = old_dataflows.get(&export_ids) {
                                let compatible = dataflow.compatible_with(old_dataflow);
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
                                        old_compaction.insert(*id, dataflow.as_of.clone().unwrap());
                                    }
                                    retain_ids.extend(export_ids);
                                } else {
                                    new_dataflows.push(dataflow.clone());
                                }
                            } else {
                                new_dataflows.push(dataflow.clone());
                            }
                        }

                        if !new_dataflows.is_empty() {
                            todo_commands.push(ComputeCommand::CreateDataflows(new_dataflows));
                        }
                    }
                    ComputeCommand::CreateInstance(new_config) => {
                        // Cluster creation should not be performed again!
                        if Some(new_config) != old_config {
                            halt!(
                                "new instance configuration does not match existing instance configuration:\n{:?}\nvs\n{:?}",
                                new_config,
                                old_config,
                            );
                        }

                        // Ensure we retain the logging sink dataflows.
                        for (id, _) in new_config.logging.sink_logs.values() {
                            retain_ids.insert(*id);
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
                    if old_frontiers.get(&id) != Some(&&timely::progress::Antichain::default()) {
                        old_compaction.insert(id, timely::progress::Antichain::default());
                    }
                }
            }
            if !old_compaction.is_empty() {
                todo_commands.insert(
                    0,
                    ComputeCommand::AllowCompaction(old_compaction.into_iter().collect::<Vec<_>>()),
                );
            }

            // Clean up worker-local state.
            //
            // Various aspects of `ComputeState` need to be either uninstalled, or return to a blank slate.
            // All dropped dataflows should clean up after themselves, as we plan to install new dataflows
            // re-using the same identifiers.
            // All re-used dataflows should roll back any believed communicated information (e.g. frontiers)
            // so that they recommunicate that information as if from scratch.

            // Remove all pending peeks.
            compute_state.pending_peeks.clear();
            // We compact away removed frontiers, and so only need to reset ids we continue to use.
            for (_, frontier) in compute_state.reported_frontiers.iter_mut() {
                *frontier = timely::progress::Antichain::from_elem(<_>::minimum());
            }
            // Sink tokens should be retained for retained dataflows, and dropped for dropped dataflows.
            compute_state
                .sink_tokens
                .retain(|id, _| retain_ids.contains(id));
            // We must drop the subscribe response buffer as it is global across all subscribes.
            // If it were broken out by `GlobalId` then we could drop only those of dataflows we drop.
            compute_state.subscribe_response_buffer =
                std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
        } else {
            todo_commands = new_commands.clone();
        }

        // Execute the commands to bring us to `new_commands`.
        for command in todo_commands.into_iter() {
            self.handle_command(response_tx, command);
        }

        // Overwrite `self.command_history` to reflect `new_commands`.
        // It is possible that there still isn't a compute state yet.
        if let Some(compute_state) = &mut self.compute_state {
            let mut command_history = ComputeCommandHistory::default();
            for command in new_commands.iter() {
                command_history.push(command.clone(), &compute_state.pending_peeks);
            }
            compute_state.command_history = command_history;
        }
        Ok(())
    }
}
