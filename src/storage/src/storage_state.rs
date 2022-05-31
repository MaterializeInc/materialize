// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for storage timely instances.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crossbeam_channel::TryRecvError;
use timely::communication::Allocate;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;

use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_dataflow_types::client::{StorageCommand, StorageResponse};
use mz_dataflow_types::sources::{ExternalSourceConnector, SourceConnector, SourceDesc};
use mz_dataflow_types::ConnectorContext;
use mz_ore::now::NowFn;
use mz_persist_client::{write::WriteHandle, PersistLocation};
use mz_repr::{GlobalId, Row, Timestamp};

use crate::decode::metrics::DecodeMetrics;
use crate::source::metrics::SourceBaseMetrics;

/// How frequently each dataflow worker sends timestamp binding updates
/// back to the coordinator.
const TS_BINDING_FEEDBACK_INTERVAL: Duration = Duration::from_millis(1_000);

/// State maintained for each worker thread.
///
/// Much of this state can be viewed as local variables for the worker thread,
/// holding state that persists across function calls.
pub struct Worker<'w, A: Allocate> {
    /// The underlying Timely worker.
    pub timely_worker: &'w mut TimelyWorker<A>,
    /// The channel from which commands are drawn.
    pub command_rx: crossbeam_channel::Receiver<StorageCommand>,
    /// The channel over which storage responses are reported.
    pub response_tx: mpsc::UnboundedSender<StorageResponse>,
    /// The state associated with collection ingress and egress.
    pub storage_state: StorageState,
}

/// Worker-local state related to the ingress or egress of collections of data.
pub struct StorageState {
    /// Source descriptions that have been created and not yet dropped.
    ///
    /// For the moment we retain all source descriptions, even those that have been
    /// dropped, as this is used to check for rebinding of previous identifiers.
    /// Once we have a better mechanism to avoid that, for example that identifiers
    /// must strictly increase, we can clean up descriptions when sources are dropped.
    pub source_descriptions: HashMap<GlobalId, SourceDesc<CollectionMetadata>>,
    /// The highest observed upper frontier for collection.
    ///
    /// This is shared among all source instances, so that they can jointly advance the
    /// frontier even as other instances are created and dropped. Ideally, the Storage
    /// module would eventually provide one source of truth on this rather than multiple,
    /// and we should aim for that but are not there yet.
    pub source_uppers: HashMap<GlobalId, Rc<RefCell<Antichain<mz_repr::Timestamp>>>>,
    /// Persist handles for all sources that deal with persist collections/shards.
    pub persist_handles:
        HashMap<GlobalId, WriteHandle<Row, Row, mz_repr::Timestamp, mz_repr::Diff>>,
    /// Persist shard ids for the reclocking collection of a source.
    pub collection_metadata: HashMap<GlobalId, CollectionMetadata>,
    /// Handles to created sources, keyed by ID
    pub source_tokens: HashMap<GlobalId, Rc<dyn Any>>,
    /// Decoding metrics reported by all dataflows.
    pub decode_metrics: DecodeMetrics,
    /// Tracks the conditional write frontiers we have reported.
    pub reported_frontiers: HashMap<GlobalId, Antichain<Timestamp>>,
    /// Tracks the last time we sent binding durability info over `response_tx`.
    pub last_bindings_feedback: Instant,
    /// Undocumented
    pub now: NowFn,
    /// Metrics for the source-specific side of dataflows.
    pub source_metrics: SourceBaseMetrics,
    /// Index of the associated timely dataflow worker.
    pub timely_worker_index: usize,
    /// Peers in the associated timely dataflow worker.
    pub timely_worker_peers: usize,
    /// Configuration for source and sink connectors.
    pub connector_context: ConnectorContext,
}

impl<'w, A: Allocate> Worker<'w, A> {
    /// Draws from `dataflow_command_receiver` until shutdown.
    pub fn run(&mut self) {
        let mut shutdown = false;
        while !shutdown {
            // Ask Timely to execute a unit of work. If Timely decides there's
            // nothing to do, it will park the thread. We rely on another thread
            // unparking us when there's new work to be done, e.g., when sending
            // a command or when new Kafka messages have arrived.
            self.timely_worker.step_or_park(None);

            self.report_frontier_progress();

            // Handle any received commands.
            let mut cmds = vec![];
            let mut empty = false;
            while !empty {
                match self.command_rx.try_recv() {
                    Ok(cmd) => cmds.push(cmd),
                    Err(TryRecvError::Empty) => empty = true,
                    Err(TryRecvError::Disconnected) => {
                        empty = true;
                        shutdown = true;
                    }
                }
            }
            for cmd in cmds {
                self.handle_storage_command(cmd);
            }
        }
    }

    /// Entry point for applying a storage command.
    pub fn handle_storage_command(&mut self, cmd: StorageCommand) {
        match cmd {
            StorageCommand::CreateSources(sources) => {
                for source in sources {
                    match &source.desc.connector {
                        SourceConnector::Local { .. } => {
                            // TODO(benesch): fix the types here so that we can
                            // enforce this statically.
                            unreachable!("local sources are handled entirely by controller");
                        }
                        SourceConnector::External {
                            connector: ExternalSourceConnector::Persist(persist_connector),
                            ..
                        } => {
                            let location = PersistLocation {
                                blob_uri: persist_connector.blob_uri.clone(),
                                consensus_uri: persist_connector.consensus_uri.clone(),
                            };

                            // TODO: Make these parts async aware?
                            let persist_client =
                                futures_executor::block_on(location.open()).unwrap();

                            let write = futures_executor::block_on(
                                persist_client
                                    .open_writer::<Row, Row, mz_repr::Timestamp, mz_repr::Diff>(
                                        persist_connector.shard_id,
                                    ),
                            )
                            .unwrap();

                            self.storage_state.persist_handles.insert(source.id, write);
                        }
                        SourceConnector::External { .. } => {
                            // Initialize shared frontier tracking.
                            self.storage_state.source_uppers.insert(
                                source.id,
                                Rc::new(RefCell::new(Antichain::from_elem(
                                    mz_repr::Timestamp::minimum(),
                                ))),
                            );

                            crate::render::build_storage_dataflow(
                                &mut self.timely_worker,
                                &mut self.storage_state,
                                &source.id.to_string(),
                                source.clone(),
                            );
                        }
                    }

                    self.storage_state
                        .source_descriptions
                        .insert(source.id, source.desc);

                    use timely::progress::Timestamp;
                    self.storage_state.reported_frontiers.insert(
                        source.id,
                        Antichain::from_elem(mz_repr::Timestamp::minimum()),
                    );

                    self.storage_state
                        .collection_metadata
                        .insert(source.id, source.storage_metadata);
                }
            }
            StorageCommand::AllowCompaction(list) => {
                for (id, frontier) in list {
                    if frontier.is_empty() {
                        // Indicates that we may drop `id`, as there are no more valid times to read.
                        // Clean up per-source state.
                        self.storage_state.source_descriptions.remove(&id);
                        self.storage_state.source_uppers.remove(&id);
                        self.storage_state.reported_frontiers.remove(&id);
                        self.storage_state.source_tokens.remove(&id);
                        self.storage_state.persist_handles.remove(&id);
                    }
                }
            }
        }
    }

    /// Emit information about write frontier progress, along with information that should
    /// be made durable for this to be the case.
    ///
    /// The write frontier progress is "conditional" in that it is not until the information is made
    /// durable that the data are emitted to downstream workers, and indeed they should not rely on
    /// the completeness of what they hear until the information is made durable.
    ///
    /// Specifically, this sends information about new timestamp bindings created by dataflow workers,
    /// with the understanding if that if made durable (and ack'd back to the workers) the source will
    /// in fact progress with this write frontier.
    pub fn report_frontier_progress(&mut self) {
        // Do nothing if dataflow workers can't send feedback or if not enough time has elapsed since
        // the last time we reported timestamp bindings.
        if self.storage_state.last_bindings_feedback.elapsed() < TS_BINDING_FEEDBACK_INTERVAL {
            return;
        }
        let mut changes = Vec::new();

        // Check if any observed frontier should advance the reported frontiers.
        for (id, frontier) in self.storage_state.source_uppers.iter() {
            let reported_frontier = self
                .storage_state
                .reported_frontiers
                .get_mut(&id)
                .expect("Reported frontier missing!");

            let observed_frontier = frontier.borrow();

            // Only do a thing if it *advances* the frontier, not just *changes* the frontier.
            // This is protection against `frontier` lagging behind what we have conditionally reported.
            if <_ as PartialOrder>::less_than(reported_frontier, &observed_frontier) {
                let mut change_batch = ChangeBatch::new();
                for time in reported_frontier.elements().iter() {
                    change_batch.update(time.clone(), -1);
                }
                for time in observed_frontier.elements().iter() {
                    change_batch.update(time.clone(), 1);
                }
                if !change_batch.is_empty() {
                    changes.push((*id, change_batch));
                }
                reported_frontier.clone_from(&observed_frontier);
            }
        }

        for (id, write_handle) in self.storage_state.persist_handles.iter_mut() {
            let reported_frontier = self
                .storage_state
                .reported_frontiers
                .get_mut(&id)
                .expect("Reported frontier missing!");

            // TODO: Make these parts of the code async?
            let observed_frontier = futures_executor::block_on(write_handle.fetch_recent_upper());

            // Only do a thing if it *advances* the frontier, not just *changes* the frontier.
            // This is protection against `frontier` lagging behind what we have conditionally reported.
            if <_ as PartialOrder>::less_than(reported_frontier, &observed_frontier) {
                let mut change_batch = ChangeBatch::new();
                for time in reported_frontier.elements().iter() {
                    change_batch.update(time.clone(), -1);
                }
                for time in observed_frontier.elements().iter() {
                    change_batch.update(time.clone(), 1);
                }
                if !change_batch.is_empty() {
                    changes.push((*id, change_batch));
                }
                reported_frontier.clone_from(&observed_frontier);
            }
        }

        if !changes.is_empty() {
            self.send_storage_response(StorageResponse::FrontierUppers(changes));
        }
        self.storage_state.last_bindings_feedback = Instant::now();
    }

    /// Send a response to the coordinator.
    fn send_storage_response(&self, response: StorageResponse) {
        // Ignore send errors because the coordinator is free to ignore our
        // responses. This happens during shutdown.
        let _ = self.response_tx.send(response);
    }
}
