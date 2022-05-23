// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for storage timely instances.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Weak;
use std::time::{Duration, Instant};

use differential_dataflow::lattice::Lattice;
use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::PersistLocation;
use timely::communication::Allocate;
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::ActivateCapability;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;

use mz_dataflow_types::client::{RenderSourcesCommand, StorageCommand, StorageResponse};
use mz_dataflow_types::sources::{ExternalSourceConnector, SourceConnector};
use mz_dataflow_types::ConnectorContext;
use mz_ore::now::NowFn;
use mz_repr::{Diff, GlobalId, Row, Timestamp};

use crate::boundary::StorageCapture;
use crate::decode::metrics::DecodeMetrics;
use crate::source::metrics::SourceBaseMetrics;
use crate::source::SourceToken;

/// How frequently each dataflow worker sends timestamp binding updates
/// back to the coordinator.
const TS_BINDING_FEEDBACK_INTERVAL: Duration = Duration::from_millis(1_000);

/// Worker-local state related to the ingress or egress of collections of data.
pub struct StorageState {
    /// State about each table, keyed by table ID.
    pub table_state: HashMap<GlobalId, TableState<mz_repr::Timestamp>>,
    /// Source descriptions that have been created and not yet dropped.
    ///
    /// For the moment we retain all source descriptions, even those that have been
    /// dropped, as this is used to check for rebinding of previous identifiers.
    /// Once we have a better mechanism to avoid that, for example that identifiers
    /// must strictly increase, we can clean up descriptions when sources are dropped.
    pub source_descriptions: HashMap<GlobalId, mz_dataflow_types::sources::SourceDesc>,
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
    /// Handles to external sources, keyed by ID.
    // TODO(guswynn): determine if this field is needed
    pub ts_source_mapping: HashMap<GlobalId, Vec<Weak<Option<SourceToken>>>>,
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

/// State about a single table.
pub struct TableState<T> {
    /// The since frontier for the table.
    pub since: Antichain<T>,
    /// The upper frontier for the table.
    pub upper: T,
    /// The data in the table.
    pub data: Vec<(Row, T, Diff)>,
    /// The size of `data` after the last consolidation.
    pub last_consolidated_size: usize,
    /// Handles to the live local inputs for the table.
    pub(crate) inputs: Vec<LocalInput>,
}

/// A wrapper around [StorageState] with a live timely worker and response channel.
pub struct ActiveStorageState<'a, A: Allocate, B: StorageCapture> {
    /// The underlying Timely worker.
    pub timely_worker: &'a mut TimelyWorker<A>,
    /// The storage state itself.
    pub storage_state: &'a mut StorageState,
    /// The channel over which frontier information is reported.
    pub response_tx: &'a mut mpsc::UnboundedSender<StorageResponse>,
    /// The boundary with the Compute layer.
    pub boundary: &'a mut B,
}

impl<'a, A: Allocate, B: StorageCapture> ActiveStorageState<'a, A, B> {
    /// Entry point for applying a storage command.
    pub fn handle_storage_command(&mut self, cmd: StorageCommand) {
        match cmd {
            StorageCommand::CreateSources(sources) => {
                for source in sources {
                    match &source.desc.connector {
                        SourceConnector::Local { .. } | SourceConnector::Log => {
                            self.storage_state.table_state.insert(
                                source.id,
                                TableState {
                                    since: Antichain::from_elem(Timestamp::minimum()),
                                    upper: Timestamp::minimum(),
                                    data: vec![],
                                    last_consolidated_size: 0,
                                    inputs: vec![],
                                },
                            );

                            // Initialize shared frontier tracking.
                            self.storage_state.source_uppers.insert(
                                source.id,
                                Rc::new(RefCell::new(Antichain::from_elem(
                                    mz_repr::Timestamp::minimum(),
                                ))),
                            );
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
                            // Nothing to do at the moment, but in the future
                            // prepare source ingestion.

                            // Initialize shared frontier tracking.
                            self.storage_state.source_uppers.insert(
                                source.id,
                                Rc::new(RefCell::new(Antichain::from_elem(
                                    mz_repr::Timestamp::minimum(),
                                ))),
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
            StorageCommand::RenderSources(sources) => self.build_storage_dataflow(sources),
            StorageCommand::AllowCompaction(list) => {
                for (id, frontier) in list {
                    if frontier.is_empty() {
                        // Indicates that we may drop `id`, as there are no more valid times to read.

                        // Drop table-related state.
                        self.storage_state.table_state.remove(&id);
                        // Clean up per-source state.
                        self.storage_state.source_descriptions.remove(&id);
                        self.storage_state.source_uppers.remove(&id);
                        self.storage_state.reported_frontiers.remove(&id);
                        self.storage_state.ts_source_mapping.remove(&id);
                        self.storage_state.persist_handles.remove(&id);
                    } else if let Some(table_state) = self.storage_state.table_state.get_mut(&id) {
                        table_state.since = frontier.clone();
                    }
                }
            }

            StorageCommand::Append(appends) => {
                for (id, updates, upper) in appends {
                    let table_state = match self.storage_state.table_state.get_mut(&id) {
                        Some(table_state) => table_state,
                        None => panic!(
                            "table state {} missing for insert at worker {}",
                            id,
                            self.timely_worker.index()
                        ),
                    };

                    // Add the new updates to all existing renders of the table.
                    for input in &mut table_state.inputs {
                        if let Some(capability) = input.capability.upgrade() {
                            let mut capability = capability.borrow_mut();
                            let mut session = input.handle.session(capability.clone());
                            for update in &updates {
                                assert!(update.timestamp >= *capability.time());
                                session.give((update.row.clone(), update.timestamp, update.diff));
                            }
                            capability.downgrade(&upper);
                        }
                    }

                    assert!(upper >= table_state.upper);
                    table_state.upper = upper;
                    // Announce the table updates as durably recorded. This is not correct,
                    // but it also hasn't been correct afaict.
                    // TODO(petrosagg): correct this once STORAGE owns table durability.
                    let mut borrow = self.storage_state.source_uppers[&id].borrow_mut();
                    let mut joined_frontier = Antichain::new();
                    for time1 in borrow.iter() {
                        joined_frontier.insert(time1.join(&upper));
                    }
                    *borrow = joined_frontier;

                    // Discard entries that are no longer active.
                    table_state
                        .inputs
                        .retain(|input| input.capability.upgrade().is_some());

                    // Stash the data for use by future renders of the table.
                    for update in updates {
                        table_state
                            .data
                            .push((update.row, update.timestamp, update.diff));
                    }

                    // Consolidate the data in the table if it's doubled in size
                    // since the last consolidation.
                    if table_state.data.len() > table_state.last_consolidated_size * 2 {
                        for (_data, time, _diff) in &mut table_state.data {
                            time.advance_by(table_state.since.borrow());
                        }
                        differential_dataflow::consolidation::consolidate_updates(
                            &mut table_state.data,
                        );
                        table_state.last_consolidated_size = table_state.data.len();
                    }
                }
            }
        }
    }

    fn build_storage_dataflow(&mut self, dataflows: Vec<RenderSourcesCommand<Timestamp>>) {
        for RenderSourcesCommand {
            debug_name,
            dataflow_id,
            as_of,
            source_imports,
        } in dataflows
        {
            crate::render::build_storage_dataflow(
                self.timely_worker,
                &mut self.storage_state,
                &debug_name,
                as_of,
                source_imports,
                dataflow_id,
                self.boundary,
            );
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

pub(crate) struct LocalInput {
    pub(crate) handle: UnorderedHandle<Timestamp, (Row, Timestamp, Diff)>,
    /// A weak reference to the capability, in case all uses are dropped.
    pub capability: std::rc::Weak<RefCell<ActivateCapability<Timestamp>>>,
}
