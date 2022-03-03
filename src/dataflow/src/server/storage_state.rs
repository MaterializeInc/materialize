// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::{Rc, Weak};
use std::time::{Duration, Instant};

use timely::communication::Allocate;
use timely::order::PartialOrder;
use timely::progress::frontier::Antichain;
use timely::progress::ChangeBatch;
use timely::worker::Worker as TimelyWorker;
use tokio::sync::mpsc;
use tracing::{debug, trace};

use mz_dataflow_types::client::{
    CreateSourceCommand, Response, StorageCommand, StorageResponse, TimestampBindingFeedback,
};
use mz_dataflow_types::sources::AwsExternalId;
use mz_dataflow_types::sources::{ExternalSourceConnector, SourceConnector};
use mz_dataflow_types::SourceInstanceDesc;
use mz_expr::{GlobalId, PartitionId};
use mz_ore::now::NowFn;
use mz_persist::client::RuntimeClient;
use mz_repr::Timestamp;

use crate::metrics::Metrics;
use crate::render::sources::PersistedSourceManager;
use crate::server::boundary::StorageCapture;
use crate::server::LocalInput;
use crate::source::metrics::SourceBaseMetrics;
use crate::source::timestamp::TimestampBindingRc;
use crate::source::SourceToken;

/// How frequently each dataflow worker sends timestamp binding updates
/// back to the coordinator.
const TS_BINDING_FEEDBACK_INTERVAL: Duration = Duration::from_millis(1_000);

/// Worker-local state related to the ingress or egress of collections of data.
pub struct StorageState {
    /// Handles to local inputs, keyed by ID.
    pub local_inputs: HashMap<GlobalId, LocalInput>,
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
    /// Handles to external sources, keyed by ID.
    pub ts_source_mapping: HashMap<GlobalId, Vec<Weak<Option<SourceToken>>>>,
    /// Timestamp data updates for each source.
    pub ts_histories: HashMap<GlobalId, TimestampBindingRc>,
    /// Handles that allow setting the compaction frontier for a persisted source. There can only
    /// ever be one running (rendered) source of a persisted source, and if there is one, this map
    /// will contain a handle to it.
    pub persisted_sources: PersistedSourceManager,
    /// Metrics reported by all dataflows.
    pub unspecified_metrics: Metrics,
    /// Handle to the persistence runtime. None if disabled.
    pub persist: Option<RuntimeClient>,
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

/// A wrapper around [StorageState] with a live timely worker and response channel.
pub struct ActiveStorageState<'a, A: Allocate, B: StorageCapture> {
    /// The underlying Timely worker.
    pub timely_worker: &'a mut TimelyWorker<A>,
    /// The storage state itself.
    pub storage_state: &'a mut StorageState,
    /// The channel over which frontier information is reported.
    pub response_tx: &'a mut mpsc::UnboundedSender<Response>,
    /// The boundary with the Compute layer.
    pub boundary: &'a mut B,
}

impl<'a, A: Allocate, B: StorageCapture> ActiveStorageState<'a, A, B> {
    /// Sets up the timestamp binding machinery if needed for this source
    fn setup_timestamp_binding_state(&mut self, source: &CreateSourceCommand<u64>) {
        let source_timestamp_data = if let SourceConnector::External {
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
                ExternalSourceConnector::AvroOcf(_)
                | ExternalSourceConnector::File(_)
                | ExternalSourceConnector::Kinesis(_)
                | ExternalSourceConnector::S3(_) => {
                    rt_default.add_partition(PartitionId::None, None);
                    Some(rt_default)
                }
                ExternalSourceConnector::Kafka(_) => Some(rt_default),
                ExternalSourceConnector::Postgres(_) | ExternalSourceConnector::PubNub(_) => None,
            }
        } else {
            debug!(
                "Timestamping not supported for local sources {}. Ignoring",
                source.id
            );
            None
        };

        // Add any timestamp bindings that we were already aware of on restart.
        if let Some(data) = source_timestamp_data {
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
                    data.add_partition(pid.clone(), None);
                    data.add_binding(pid, timestamp, offset);
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

            self.storage_state.ts_histories.insert(source.id, data);
        } else {
            assert!(source.ts_bindings.is_empty());
        }
    }

    /// Entry point for applying a storage command.
    pub(crate) fn handle_storage_command(&mut self, cmd: StorageCommand) {
        match cmd {
            StorageCommand::CreateSources(sources) => {
                // Nothing to do at the moment, but in the future prepare source ingestion.
                for source in sources {
                    self.setup_timestamp_binding_state(&source);

                    self.storage_state
                        .source_descriptions
                        .insert(source.id, source.desc);

                    // Initialize shared frontier tracking.
                    use timely::progress::Timestamp;
                    self.storage_state.source_uppers.insert(
                        source.id,
                        Rc::new(RefCell::new(Antichain::from_elem(
                            mz_repr::Timestamp::minimum(),
                        ))),
                    );

                    self.storage_state.reported_frontiers.insert(
                        source.id,
                        Antichain::from_elem(mz_repr::Timestamp::minimum()),
                    );
                }
            }
            StorageCommand::RenderSources(sources) => self.build_storage_dataflow(sources),
            StorageCommand::AllowCompaction(list) => {
                for (id, frontier) in list {
                    if frontier.is_empty() {
                        // Indicates that we may drop `id`, as there are no more valid times to read.

                        // Drop table-related state.
                        self.storage_state.local_inputs.remove(&id);
                        // Clean up potentially left over persisted source state.
                        self.storage_state.persisted_sources.del_source(&id);
                        // Clean up per-source state.
                        self.storage_state.source_descriptions.remove(&id);
                        self.storage_state.source_uppers.remove(&id);
                        self.storage_state.reported_frontiers.remove(&id);
                        self.storage_state.ts_histories.remove(&id);
                        self.storage_state.ts_source_mapping.remove(&id);
                    } else {
                        if let Some(ts_history) = self.storage_state.ts_histories.get_mut(&id) {
                            ts_history.set_compaction_frontier(frontier.borrow());
                        }

                        self.storage_state
                            .persisted_sources
                            .allow_compaction(id, frontier);
                    }
                }
            }

            StorageCommand::AdvanceAllLocalInputs { advance_to } => {
                for (_, local_input) in self.storage_state.local_inputs.iter_mut() {
                    local_input.capability.downgrade(&advance_to);
                }
            }

            StorageCommand::Insert { id, updates } => {
                let input = match self.storage_state.local_inputs.get_mut(&id) {
                    Some(input) => input,
                    None => panic!(
                        "local input {} missing for insert at worker {}",
                        id,
                        self.timely_worker.index()
                    ),
                };
                let mut session = input.handle.session(input.capability.clone());
                for update in updates {
                    assert!(update.timestamp >= *input.capability.time());
                    session.give((update.row, update.timestamp, update.diff));
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

    fn build_storage_dataflow(
        &mut self,
        dataflows: Vec<(
            String,
            GlobalId,
            Option<Antichain<Timestamp>>,
            BTreeMap<GlobalId, SourceInstanceDesc>,
        )>,
    ) {
        for (debug_name, dataflow_id, as_of, source_imports) in dataflows {
            for (source_id, instance) in source_imports.iter() {
                assert_eq!(
                    self.storage_state.source_descriptions[source_id],
                    instance.description
                );
            }
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
                bindings.extend(
                    history
                        .get_bindings_in_range(prev_frontier.borrow(), new_frontier.borrow())
                        .into_iter()
                        .map(|(pid, ts, offset)| (*id, pid, ts, offset)),
                );
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
        let _ = self.response_tx.send(Response::Storage(response));
    }
}
