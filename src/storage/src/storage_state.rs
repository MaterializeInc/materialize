// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker-local state for storage timely instances.

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use mz_persist_client::write::WriteHandle;
use mz_persist_client::PersistLocation;
use timely::communication::Allocate;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;
use tracing::{debug, trace};

use mz_dataflow_types::client::{
    CreateSourceCommand, StorageCommand, StorageResponse, TimestampBindingFeedback,
};
use mz_dataflow_types::sources::AwsExternalId;
use mz_dataflow_types::sources::{ExternalSourceConnector, SourceConnector, SourceDesc};
use mz_expr::PartitionId;
use mz_ore::now::NowFn;
use mz_repr::{Diff, GlobalId, Row, Timestamp};

use crate::decode::metrics::DecodeMetrics;
use crate::source::metrics::SourceBaseMetrics;
use crate::source::timestamp::TimestampBindingRc;
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
    pub source_descriptions: HashMap<GlobalId, SourceDesc>,
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
    /// Handles to created sources, keyed by ID
    pub source_tokens: HashMap<GlobalId, Arc<dyn Any + Send + Sync>>,
    // TODO(guswynn): determine if this field is needed
    /// Handles to external sources, keyed by ID.
    pub ts_source_mapping: HashMap<GlobalId, Vec<Weak<Option<SourceToken>>>>,
    /// Timestamp data updates for each source.
    pub ts_histories: HashMap<GlobalId, TimestampBindingRc>,
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
    /// An external ID to use for all AWS AssumeRole operations.
    pub aws_external_id: AwsExternalId,
    /// Index of the associated timely dataflow worker.
    pub timely_worker_index: usize,
    /// Peers in the associated timely dataflow worker.
    pub timely_worker_peers: usize,
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
}

/// A wrapper around [StorageState] with a live timely worker and response channel.
pub struct ActiveStorageState<'a, A: Allocate> {
    /// The underlying Timely worker.
    pub timely_worker: &'a mut TimelyWorker<A>,
    /// The storage state itself.
    pub storage_state: &'a mut StorageState,
    /// The channel over which frontier information is reported.
    pub response_tx: &'a mut mpsc::UnboundedSender<StorageResponse>,
}

impl<'a, A: Allocate> ActiveStorageState<'a, A> {
    /// Sets up the timestamp binding machinery if needed for this source
    fn setup_timestamp_binding_state(&mut self, source: &CreateSourceCommand<Timestamp>) {
        let ts_history = if let SourceConnector::External {
            connector,
            ts_frequency,
            ..
        } = &source.desc.connector
        {
            let rt_default = TimestampBindingRc::new(
                ts_frequency.as_millis().try_into().unwrap(),
                self.storage_state.now.clone(),
            );
            match connector {
                ExternalSourceConnector::Kinesis(_) | ExternalSourceConnector::S3(_) => {
                    rt_default.add_partition(PartitionId::None, None);
                    Some(rt_default)
                }
                ExternalSourceConnector::PubNub(_) => {
                    rt_default.add_partition(PartitionId::None, None);
                    Some(rt_default)
                }
                ExternalSourceConnector::Kafka(_) => Some(rt_default),
                ExternalSourceConnector::Postgres(_) | ExternalSourceConnector::Persist(_) => None,
            }
        } else {
            debug!(
                "Timestamping not supported for local or persist sources {}. Ignoring",
                source.id
            );
            None
        };

        // Add any timestamp bindings that we were already aware of on restart.
        if let Some(ts_history) = ts_history {
            for (pid, timestamp, offset) in source.ts_bindings.iter().cloned() {
                if crate::source::responsible_for(
                    &source.id,
                    self.timely_worker.index(),
                    self.timely_worker.peers(),
                    &pid,
                ) {
                    trace!(
                        "Adding partition/binding on worker {}: ({}, {}, {})",
                        self.timely_worker.index(),
                        pid,
                        timestamp,
                        offset
                    );
                    ts_history.add_partition(pid.clone(), None);
                    ts_history.add_binding(pid, timestamp, offset);
                } else {
                    trace!(
                        "NOT adding partition/binding on worker {}: ({}, {}, {})",
                        self.timely_worker.index(),
                        pid,
                        timestamp,
                        offset
                    );
                }
            }

            self.storage_state
                .ts_histories
                .insert(source.id, ts_history);
        } else {
            assert!(source.ts_bindings.is_empty());
        }
    }

    /// Entry point for applying a storage command.
    pub fn handle_storage_command(&mut self, cmd: StorageCommand) {
        match cmd {
            StorageCommand::CreateSources(sources) => {
                for source in sources {
                    match &source.desc.connector {
                        SourceConnector::Local { .. } => {
                            self.storage_state.table_state.insert(
                                source.id,
                                TableState {
                                    since: Antichain::from_elem(Timestamp::minimum()),
                                    upper: Timestamp::minimum(),
                                    data: vec![],
                                    last_consolidated_size: 0,
                                },
                            );

                            self.setup_timestamp_binding_state(&source);

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

                            let (write, _read) = futures_executor::block_on(
                                persist_client.open::<Row, Row, mz_repr::Timestamp, mz_repr::Diff>(
                                    persist_connector.shard_id,
                                ),
                            )
                            .unwrap();

                            self.storage_state.persist_handles.insert(source.id, write);
                        }
                        SourceConnector::External { .. } => {
                            self.setup_timestamp_binding_state(&source);

                            // Initialize shared frontier tracking.
                            self.storage_state.source_uppers.insert(
                                source.id,
                                Rc::new(RefCell::new(Antichain::from_elem(
                                    mz_repr::Timestamp::minimum(),
                                ))),
                            );

                            crate::render::build_storage_dataflow(
                                self.timely_worker,
                                self.storage_state,
                                "foobar", //TODO(petrosagg): fix this
                                Antichain::from_elem(0),
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
                }
            }
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
                        self.storage_state.ts_histories.remove(&id);
                        self.storage_state.source_tokens.remove(&id);
                        self.storage_state.ts_source_mapping.remove(&id);
                        self.storage_state.persist_handles.remove(&id);
                    } else {
                        if let Some(ts_history) = self.storage_state.ts_histories.get_mut(&id) {
                            ts_history.set_compaction_frontier(frontier.borrow());
                        }

                        if let Some(table_state) = self.storage_state.table_state.get_mut(&id) {
                            table_state.since = frontier.clone();
                        }
                    }
                }
            }

            StorageCommand::DurabilityFrontierUpdates(list) => {
                for (id, frontier) in list {
                    if let Some(ts_history) = self.storage_state.ts_histories.get_mut(&id) {
                        ts_history.set_durability_frontier(frontier.borrow());
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
    pub fn report_conditional_frontier_progress(&mut self) {
        // Do nothing if dataflow workers can't send feedback or if not enough time has elapsed since
        // the last time we reported timestamp bindings.
        if self.storage_state.last_bindings_feedback.elapsed() < TS_BINDING_FEEDBACK_INTERVAL {
            return;
        }
        let mut changes = Vec::new();
        let mut bindings = Vec::new();
        let mut new_frontier = Antichain::new();

        // Need to go through all sources that are generating timestamp bindings, and extract their upper frontiers.
        // If that frontier is different than the durability frontier we've previously reported then we also need to
        // get the new bindings we've produced and send them to the coordinator.
        for (id, history) in self.storage_state.ts_histories.iter() {
            // Read the upper frontier and compare to what we've reported.
            history.read_upper(&mut new_frontier);
            let prev_frontier = self
                .storage_state
                .reported_frontiers
                .get_mut(&id)
                .expect("Frontier missing!");

            // This is not an error in the case that the input has advanced without producing bindings, as in the
            // case of a non-TAIL file, which will close its output and report itself as complete.
            if <_ as PartialOrder>::less_than(prev_frontier, &new_frontier) {
                let mut change_batch = ChangeBatch::new();
                for time in prev_frontier.elements().iter() {
                    change_batch.update(time.clone(), -1);
                }
                for time in new_frontier.elements().iter() {
                    change_batch.update(time.clone(), 1);
                }
                change_batch.compact();
                if !change_batch.is_empty() {
                    changes.push((*id, change_batch));
                }
                // Add all timestamp bindings we know about between the old and new frontier.
                bindings.push((
                    *id,
                    history.get_bindings_in_range(prev_frontier.borrow(), new_frontier.borrow()),
                ));
                prev_frontier.clone_from(&new_frontier);
            }
        }

        // Check if any observed frontier should advance the reported frontiers.
        // We don't expect to see this for sources with timestamp histories, as their durability requests should run
        // ahead of stream progress. However, for other sources we may see something here.
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

        if !changes.is_empty() || !bindings.is_empty() {
            self.send_storage_response(StorageResponse::TimestampBindings(
                TimestampBindingFeedback { changes, bindings },
            ));
        }
        self.storage_state.last_bindings_feedback = Instant::now();
    }
    /// Instruct all real-time sources managed by the worker to close their current
    /// timestamp and move to the next wall clock time.
    ///
    /// Needs to be called periodically (ideally once per "timestamp_frequency" in order
    /// for real time sources to make progress.
    pub fn update_rt_timestamps(&self) {
        for (_, history) in self.storage_state.ts_histories.iter() {
            history.update_timestamp();
        }
    }

    /// Send a response to the coordinator.
    fn send_storage_response(&self, response: StorageResponse) {
        // Ignore send errors because the coordinator is free to ignore our
        // responses. This happens during shutdown.
        let _ = self.response_tx.send(response);
    }
}
