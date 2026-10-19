// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Sync Timely operator implementation of the MV sink.
//!
//! This module provides an alternative implementation of `persist_sink` that uses sync Timely
//! operators communicating with Tokio tasks via channels, instead of async Timely operators.
//! Gated behind the `ENABLE_SYNC_MV_SINK` dyncfg.
//!
//! See the [main module](super::materialized_view) for the operator graph and design docs.
//!
//! ### Channel ordering requirements
//!
//! Each operator splits state across a Timely thread (which observes inputs and frontiers and does
//! the CPU work) and a Tokio task (which owns persist I/O state). They communicate via `mpsc`
//! command channels, which preserve send order on a single sender. Each operator instance
//! constructs its own `(tx, rx)` pair inside `render` and never clones the sender, so there is
//! exactly one producer per channel, and its sends are totally ordered. Different worker instances
//! of the same operator never share a channel, so cross-worker ordering is not a concern. The
//! correctness of the operators relies on a few ordering invariants between the messages sent
//! within a single Timely activation:
//!
//!  * **`mint`**: the `persist_watch` Tokio task is the sole producer of persist-frontier updates
//!    and emits them in monotonically increasing order, terminated by the empty frontier. The
//!    Timely closure drains the receiver each activation; processing in receive order is therefore
//!    sufficient. No cross-channel ordering is needed because `mint` only has the one channel.
//!
//!  * **`write`**: the Timely closure owns the correction buffers and builds the parts of a
//!    batch itself, so the Tokio task only hands parts to persist. A batch write is a `Start`,
//!    its `Part`s, and a `Finish`, and the closure sends the next `Start` only after the task
//!    answered the previous `Finish`. The task therefore never interleaves the parts of two
//!    batches.
//!
//!  * **`append`**: per-activation, the Timely closure forwards messages in the order
//!    `Description` → `Batch` → `BatchesFrontier`. The first two carry the data the task needs
//!    to absorb; `BatchesFrontier` is the trigger that allows `maybe_append_batches` to fire.
//!    Sending `BatchesFrontier` *after* its corresponding `Batch` messages ensures the task does
//!    not append a batch description before all batches contributing to it have been absorbed.
//!    If the order were reversed, `maybe_append_batches` could fire on an incomplete `batches`
//!    set and miss writes.

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{Hashable, VecCollection};
use mz_compute_types::dyncfgs::{
    MV_SINK_ADVANCE_PERSIST_FRONTIERS, MV_SINK_SHARED_BATCH_MAX_BYTES,
};
use mz_dyncfg::{ConfigSet, ConfigValHandle};
use mz_ore::cast::CastFrom;
use mz_persist_client::batch::{Batch, ProtoBatch};
use mz_persist_client::write::WriteHandle;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use timely::PartialOrder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;
use tokio::sync::{mpsc, watch};
use tracing::trace;

use crate::compute_state::ComputeState;
use crate::render::StartSignal;
use crate::render::errors::DataflowErrorSer;
use crate::sink::correction::{ChannelLogging, Correction, CorrectionLogger};
use crate::sink::materialized_view::{
    BatchDescription, BatchesStream, DescsStream, DesiredStreams, OkErr, PersistApi,
    PersistStreams, SharedSinkFrontier, advance, operator_name, persist_source,
};
use crate::sink::shared_batch::SharedBatchWriter;

/// Renders an MV sink writing the given desired collection into the `target` persist collection.
///
/// This is the sync Timely operator implementation, using Tokio tasks for I/O.
pub(super) fn persist_sink<'s>(
    sink_id: GlobalId,
    target: &mz_storage_types::controller::CollectionMetadata,
    ok_collection: VecCollection<'s, Timestamp, Row, Diff>,
    err_collection: VecCollection<'s, Timestamp, DataflowErrorSer, Diff>,
    as_of: Antichain<Timestamp>,
    compute_state: &mut ComputeState,
    start_signal: StartSignal,
    read_only_rx: watch::Receiver<bool>,
) -> Rc<dyn Any> {
    let scope = ok_collection.scope();
    let desired = OkErr::new(ok_collection.inner, err_collection.inner);

    // Read back the persist shard.
    let (persist, persist_token) =
        persist_source(scope, sink_id, target.clone(), compute_state, start_signal);

    let persist_api = PersistApi {
        persist_clients: Arc::clone(&compute_state.persist_clients),
        persist_batches: compute_state.persist_batches.clone(),
        collection: target.clone(),
        shard_name: sink_id.to_string(),
        purpose: format!("MV sink {sink_id}"),
    };

    let (desired, descs, sink_frontier) = mint::render(
        sink_id,
        persist_api.clone(),
        as_of.clone(),
        read_only_rx.clone(),
        desired,
    );

    // Broadcast batch descriptions to all workers, regardless of whether or not they are
    // responsible for the append, to give them a chance to clean up any outdated state they
    // might still hold.
    let descs = descs.broadcast();

    let batches = write::render(
        sink_id,
        persist_api.clone(),
        as_of,
        desired,
        persist,
        descs.clone(),
        read_only_rx,
        Rc::clone(&compute_state.worker_config),
        &compute_state.metrics,
    );

    append::render(sink_id, persist_api, descs, batches);

    // Report sink frontier updates to the `ComputeState`.
    let collection = compute_state.expect_collection_mut(sink_id);
    collection.sink_write_frontier = Some(sink_frontier);

    Rc::new(persist_token)
}

/// Implementation of the `mint` operator.
mod mint {
    use super::*;
    use timely::progress::frontier::AntichainRef;

    /// Render the `mint` operator.
    ///
    /// The parameters passed in are:
    ///  * `sink_id`: The `GlobalId` of the sink export.
    ///  * `persist_api`: An object providing access to the output persist shard.
    ///  * `as_of`: The first time for which the sink may produce output.
    ///  * `read_only_rx`: A receiver that reports the sink is in read-only mode.
    ///  * `desired`: The ok/err streams that should be sinked to persist.
    pub fn render<'s>(
        sink_id: GlobalId,
        persist_api: PersistApi,
        as_of: Antichain<Timestamp>,
        mut read_only_rx: watch::Receiver<bool>,
        desired: DesiredStreams<'s>,
    ) -> (DesiredStreams<'s>, DescsStream<'s>, SharedSinkFrontier) {
        let scope = desired.ok.scope();
        let worker_id = scope.index();
        let worker_count = scope.peers();

        // Determine the active worker for the mint operator.
        let active_worker_id = usize::cast_from(sink_id.hashed()) % scope.peers();

        let sink_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::MIN)));
        let shared_frontier = Rc::clone(&sink_frontier);

        let name = operator_name(sink_id, "mint");
        let mut builder = OperatorBuilderRc::new(name, scope.clone());
        let info = builder.operator_info();

        // Create outputs (before inputs, so no input connections yet).
        let (ok_output, ok_stream) = builder.new_output();
        let (err_output, err_stream) = builder.new_output();
        let (desc_output, desc_stream) = builder.new_output();

        let mut ok_output = OutputBuilder::from(ok_output);
        let mut err_output = OutputBuilder::from(err_output);
        let mut desc_output = OutputBuilder::from(desc_output);

        // desired_ok -> output 0 (ok passthrough)
        let mut desired_ok_input = builder.new_input_connection(
            desired.ok,
            Pipeline,
            [(0, Antichain::from_elem(Default::default()))],
        );
        // desired_err -> output 1 (err passthrough)
        let mut desired_err_input = builder.new_input_connection(
            desired.err,
            Pipeline,
            [(1, Antichain::from_elem(Default::default()))],
        );

        // Set up background tasks and state for the active worker only.
        let mut task_handles = Vec::new();
        let read_only = *read_only_rx.borrow_and_update();
        let mut state = None;
        if worker_id == active_worker_id {
            // Spawn a Tokio task to watch the persist shard's upper frontier.
            //
            // We collect the persist frontier from a write handle directly, rather than
            // inspecting the `persist` stream, because the latter has two annoying glitches:
            //  (a) It starts at the shard's read frontier, not its write frontier.
            //  (b) It can lag behind if there are spikes in ingested data.
            //
            // The decoupling from the `persist` stream is load-bearing: that stream can fall
            // arbitrarily behind the shard upper during snapshot replay or write spikes. Using
            // it would delay both (1) the controller-visible sink frontier (`shared_frontier`),
            // which previously caused a CrossJoin feature-bench regression where the controller
            // held a finished MV dataflow open waiting for the empty frontier, and (2) the
            // `state.persist_frontier` that gates batch-description minting, stalling mint until
            // the read-back stream catches up. The goal isn't tick-granular descriptions per
            // se — it's avoiding the stream-induced stall. See 5eab5ff896 for the original
            // regression and rationale.
            //
            // The task sends the empty frontier as its final message before exiting. The
            // operator drops `persist_rx` once it receives the empty frontier.
            let (persist_tx, persist_rx) = mpsc::unbounded_channel();
            let sync_activator = scope.worker().sync_activator_for(info.address.to_vec());
            let handle = mz_ore::task::spawn(
                || operator_name(sink_id, "mint::persist_watch"),
                async move {
                    let mut writer = persist_api.open_writer().await;
                    let mut frontier = Antichain::from_elem(Timestamp::MIN);
                    loop {
                        writer.wait_for_upper_past(&frontier).await;
                        frontier = writer.upper().clone();
                        if persist_tx.send(frontier.clone()).is_err() {
                            return;
                        }
                        if sync_activator.activate().is_err() {
                            return;
                        }
                        if frontier.is_empty() {
                            return;
                        }
                    }
                },
            );
            task_handles.push(handle.abort_on_drop());

            // Spawn a Tokio task to wake the operator when read-only mode changes.
            if read_only {
                let sync_activator = scope.worker().sync_activator_for(info.address.to_vec());
                let mut rx = read_only_rx.clone();
                let handle = mz_ore::task::spawn(
                    || format!("mv_sink({sink_id})::mint::read_only_watch"),
                    async move {
                        let _ = rx.changed().await;
                        let _ = sync_activator.activate();
                    },
                );
                task_handles.push(handle.abort_on_drop());
            }

            state = Some(State::new(
                sink_id,
                worker_count,
                as_of,
                read_only,
                persist_rx,
            ));
        }

        builder.build(move |capabilities| {
            // Passing through the `desired` streams only requires data capabilities, so we can
            // immediately drop their initial capabilities here.
            let [_, _, desc_cap]: [_; 3] =
                capabilities.try_into().expect("one capability per output");

            let mut cap_set = if state.is_some() {
                Some(CapabilitySet::from_elem(desc_cap))
            } else {
                drop(desc_cap);
                shared_frontier.borrow_mut().clear();
                None
            };

            move |frontiers| {
                // Keep task handles alive so they are aborted when the operator is dropped.
                let _ = &task_handles;

                // Pass through desired data.
                let mut ok_out = ok_output.activate();
                desired_ok_input.for_each(|cap, data| {
                    ok_out.session(&cap).give_container(data);
                });
                let mut err_out = err_output.activate();
                desired_err_input.for_each(|cap, data| {
                    err_out.session(&cap).give_container(data);
                });

                let Some(state) = &mut state else {
                    // Non-active worker: just pass through data.
                    return;
                };
                let cap_set = cap_set.as_mut().unwrap();

                // Track desired frontiers.
                state.advance_desired_ok_frontier(frontiers[0].frontier());
                state.advance_desired_err_frontier(frontiers[1].frontier());

                state.drain_persist_rx(&shared_frontier);

                // Check read-only mode.
                if state.read_only && read_only_rx.has_changed().unwrap_or(false) {
                    if !*read_only_rx.borrow_and_update() {
                        state.allow_writes();
                    }
                }

                // Try to mint a batch description.
                let mut desc_out = desc_output.activate();
                if let Some(desc) = state.maybe_mint_batch_description() {
                    let lower_ts = *desc.lower.as_option().expect("not empty");
                    let cap = cap_set.delayed(&lower_ts);
                    desc_out.session(&cap).give(desc);

                    // We only emit strictly increasing `lower`s, so we can let our output frontier
                    // advance beyond the current `lower`.
                    cap_set.downgrade([lower_ts.step_forward()]);
                } else {
                    // The next emitted `lower` will be at least the `persist` frontier, so we can
                    // advance our output frontier as far.
                    let _ = cap_set.try_downgrade(state.persist_frontier.iter());
                }
            }
        });

        let desired_output_streams = OkErr::new(ok_stream, err_stream);
        (desired_output_streams, desc_stream, sink_frontier)
    }

    /// State maintained by the `mint` operator.
    struct State {
        sink_id: GlobalId,
        /// The number of workers in the Timely cluster.
        worker_count: usize,
        /// The frontiers of the `desired` inputs.
        desired_frontiers: OkErr<Antichain<Timestamp>, Antichain<Timestamp>>,
        /// The frontier of the target persist shard.
        persist_frontier: Antichain<Timestamp>,
        /// Receiver for persist frontier updates from the Tokio persist_watch task.
        ///
        /// Dropped once the empty frontier is received (the task's shutdown signal).
        persist_rx: Option<mpsc::UnboundedReceiver<Antichain<Timestamp>>>,
        /// The append worker for the next batch description, chosen in round-robin fashion.
        next_append_worker: usize,
        /// The last `lower` we have emitted in a batch description, if any. Whenever the
        /// `persist_frontier` moves beyond this frontier, we need to mint a new description.
        last_lower: Option<Antichain<Timestamp>>,
        /// Whether we are operating in read-only mode.
        ///
        /// In read-only mode, minting of batch descriptions is disabled.
        read_only: bool,
    }

    impl State {
        fn new(
            sink_id: GlobalId,
            worker_count: usize,
            as_of: Antichain<Timestamp>,
            read_only: bool,
            persist_rx: mpsc::UnboundedReceiver<Antichain<Timestamp>>,
        ) -> Self {
            // Initializing `persist_frontier` to the `as_of` ensures that the first minted batch
            // description will have a `lower` of `as_of` or beyond, and thus that we don't spend
            // effort writing out snapshots of data that is already in the shard.
            let persist_frontier = as_of;

            Self {
                sink_id,
                worker_count,
                desired_frontiers: OkErr::new_frontiers(),
                persist_frontier,
                persist_rx: Some(persist_rx),
                next_append_worker: 0,
                last_lower: None,
                read_only,
            }
        }

        fn trace<S: AsRef<str>>(&self, message: S) {
            let message = message.as_ref();
            trace!(
                sink_id = %self.sink_id,
                desired_frontier = ?self.desired_frontiers.frontier().elements(),
                persist_frontier = ?self.persist_frontier.elements(),
                last_lower = ?self.last_lower,
                message,
            );
        }

        fn advance_desired_ok_frontier(&mut self, frontier: AntichainRef<Timestamp>) {
            if advance(&mut self.desired_frontiers.ok, frontier) {
                self.trace("advanced `desired` ok frontier");
            }
        }

        fn advance_desired_err_frontier(&mut self, frontier: AntichainRef<Timestamp>) {
            if advance(&mut self.desired_frontiers.err, frontier) {
                self.trace("advanced `desired` err frontier");
            }
        }

        fn advance_persist_frontier(&mut self, frontier: AntichainRef<Timestamp>) {
            if advance(&mut self.persist_frontier, frontier) {
                self.trace("advanced `persist` frontier");
            }
        }

        /// Drain persist frontier updates from the Tokio task.
        ///
        /// Frontiers from the `persist_watch` task are monotonically increasing, so only the
        /// most recent one matters. We drain all queued messages and apply just the latest,
        /// avoiding redundant `advance_persist_frontier`/`trace!` calls when several updates
        /// arrived between activations.
        ///
        /// The task sends the empty frontier as its final message before exiting. Once
        /// received, we drop the receiver.
        fn drain_persist_rx(&mut self, shared_frontier: &RefCell<Antichain<Timestamp>>) {
            let Some(mut rx) = self.persist_rx.take() else {
                return;
            };
            let mut latest: Option<Antichain<Timestamp>> = None;
            let mut closed = false;
            loop {
                match rx.try_recv() {
                    Ok(frontier) => {
                        let done = frontier.is_empty();
                        latest = Some(frontier);
                        if done {
                            closed = true;
                            break;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => break,
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        panic!("mint persist_watch task unexpectedly gone");
                    }
                }
            }
            if let Some(frontier) = latest {
                shared_frontier.borrow_mut().clone_from(&frontier);
                self.advance_persist_frontier(frontier.borrow());
            }
            if !closed {
                self.persist_rx = Some(rx);
            }
        }

        fn allow_writes(&mut self) {
            if self.read_only {
                self.read_only = false;
                self.trace("switched to write mode");
            }
        }

        fn maybe_mint_batch_description(&mut self) -> Option<BatchDescription> {
            let desired_frontier = self.desired_frontiers.frontier();
            let persist_frontier = &self.persist_frontier;

            // We only mint new batch descriptions when:
            //  1. We are _not_ in read-only mode.
            //  2. The `desired` frontier is ahead of the `persist` frontier.
            //  3. The `persist` frontier advanced since we last emitted a batch description.
            let desired_ahead = PartialOrder::less_than(persist_frontier, desired_frontier);
            let persist_advanced = self.last_lower.as_ref().map_or(true, |lower| {
                PartialOrder::less_than(lower, persist_frontier)
            });

            if self.read_only || !desired_ahead || !persist_advanced {
                return None;
            }

            let lower = persist_frontier.clone();
            let upper = desired_frontier.clone();
            let append_worker = self.next_append_worker;
            let desc = BatchDescription::new(lower, upper, append_worker);

            self.next_append_worker = (append_worker + 1) % self.worker_count;
            self.last_lower = Some(desc.lower.clone());

            self.trace(format!("minted batch description: {desc:?}"));
            Some(desc)
        }
    }
}

/// Implementation of the `write` operator.
mod write {
    use super::*;

    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll, Wake, Waker};
    use std::time::Instant;

    use mz_ore::cast::CastFrom;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::part::{Part, PartBuilder};
    use mz_repr::RelationDesc;
    use mz_timely_util::activator::ArcActivator;
    use prometheus::IntCounter;
    use prometheus::core::{AtomicF64, GenericCounter};
    use timely::scheduling::SyncActivator;

    use crate::metrics::WorkerMetrics;

    /// Commands sent from the Timely operator to the Tokio write task. A batch write is a `Start`
    /// followed by its parts and a `Finish`.
    enum WriteCommand {
        /// Start writing the batch with the given description.
        Start(BatchDescription),
        /// Add a part of updates to the current batch.
        Part(Part),
        /// All parts of the current batch have been sent.
        Finish,
    }

    /// The correction buffers owned by the `write` operator.
    type Corrections = OkErr<Correction<Row>, Correction<DataflowErrorSer>>;

    /// A response from the Tokio write task back to the Timely operator.
    struct WriteResponse {
        /// The written batches, empty if this worker has none to deliver for the interval.
        batches: Vec<ProtoBatch>,
    }

    /// The size at which the operator cuts the updates of a batch into a part for the write task.
    ///
    /// Persist concatenates parts up to its blob target size before uploading them, so this only
    /// bounds the updates waiting between the operator and the write task, at
    /// [`PARTS_IN_FLIGHT`] parts per worker.
    const PART_BYTES: usize = 4 << 20;
    /// How many parts the operator hands to the write task before blocking on it.
    const PARTS_IN_FLIGHT: usize = 2;

    /// Render the `write` operator.
    ///
    /// The parameters passed in are:
    ///  * `sink_id`: The `GlobalId` of the sink export.
    ///  * `persist_api`: An object providing access to the output persist shard.
    ///  * `as_of`: The first time for which the sink may produce output.
    ///  * `desired`: The ok/err streams that should be sinked to persist.
    ///  * `persist`: The ok/err streams read back from the output persist shard.
    ///  * `descs`: The stream of batch descriptions produced by the `mint` operator.
    pub fn render<'s>(
        sink_id: GlobalId,
        persist_api: PersistApi,
        as_of: Antichain<Timestamp>,
        desired: DesiredStreams<'s>,
        persist: PersistStreams<'s>,
        descs: DescsStream<'s>,
        mut read_only_rx: watch::Receiver<bool>,
        worker_config: Rc<ConfigSet>,
        compute_metrics: &WorkerMetrics,
    ) -> BatchesStream<'s> {
        let scope = desired.ok.scope();
        let worker_id = scope.index();

        let name = operator_name(sink_id, "write");
        let mut builder = OperatorBuilderRc::new(name, scope.clone());
        let info = builder.operator_info();

        // `Correction` reports its size through a channel, which `CorrectionLogger` drains into
        // the Timely loggers.
        let mut channel_logging = None;
        let mut correction_logger = None;
        if let (Some(compute_logger), Some(differential_logger)) = (
            scope.worker().logger_for("materialize/compute"),
            scope.worker().logger_for("differential/arrange"),
        ) {
            let operator_info = builder.operator_info();
            let (tx, rx) = mpsc::unbounded_channel();
            channel_logging = Some(ChannelLogging::new(tx));
            correction_logger = Some(CorrectionLogger::new(
                compute_logger,
                differential_logger.into(),
                operator_info.global_id,
                operator_info.address.to_vec(),
                rx,
            ));
        }

        // It is important that we exchange the `desired` and `persist` data the same way, so
        // updates that cancel each other out end up on the same worker.
        let exchange_ok = |(d, _, _): &(Row, Timestamp, Diff)| d.hashed();
        let exchange_err = |(d, _, _): &(DataflowErrorSer, Timestamp, Diff)| d.hashed();

        // Data inputs are created before the output, so they are not connected to it.
        let mut desired_ok_input = builder.new_input(desired.ok, Exchange::new(exchange_ok));
        let mut desired_err_input = builder.new_input(desired.err, Exchange::new(exchange_err));
        let mut persist_ok_input = builder.new_input(persist.ok, Exchange::new(exchange_ok));
        let mut persist_err_input = builder.new_input(persist.err, Exchange::new(exchange_err));
        let mut descs_input = builder.new_input(descs, Pipeline);

        // Only descs (input 4) is connected to the batches output.
        let (batches_output, batches_output_stream) =
            builder.new_output_connection([(4, Antichain::from_elem(Default::default()))]);
        let mut batches_output = OutputBuilder::from(batches_output);

        // Obtain SinkMetrics synchronously from the persist client cache, rather than through
        // a WriteHandle, to avoid async I/O on the Timely thread.
        let sink_metrics = persist_api.persist_clients.metrics().sink.clone();
        let worker_metrics = sink_metrics.for_worker(worker_id);
        let corrections: Corrections = OkErr::new(
            Correction::new(
                sink_metrics.clone(),
                worker_metrics.clone(),
                channel_logging.clone(),
                &worker_config,
            ),
            Correction::new(
                sink_metrics,
                worker_metrics,
                channel_logging,
                &worker_config,
            ),
        );

        let advance_persist_frontiers_at_startup =
            MV_SINK_ADVANCE_PERSIST_FRONTIERS.get(&worker_config);
        let key_schema = Rc::new(persist_api.collection.relation_desc.clone());
        let write_metrics = WriteMetrics {
            wait_seconds: compute_metrics.mv_sink_write_wait_seconds_total.clone(),
            part_bytes: compute_metrics.mv_sink_write_part_bytes_total.clone(),
        };
        let shared_batch_max_bytes = MV_SINK_SHARED_BATCH_MAX_BYTES.handle(&worker_config);

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<WriteCommand>(PARTS_IN_FLIGHT);
        // Batch writes take turns on the one sender, see `Write`.
        let cmd_tx = Rc::new(cmd_tx);
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel::<WriteResponse>();

        // Spawn the Tokio task that hands the operator's parts to persist.
        let (activator, activation_ack) = ArcActivator::new(scope, &info);
        let write_task_handle = mz_ore::task::spawn(
            || operator_name(sink_id, "write::batch_writer"),
            async move {
                let batch_writer = BatchWriter::new(&persist_api, shared_batch_max_bytes).await;
                let mut desc = None;
                let mut write = None;
                while let Some(cmd) = cmd_rx.recv().await {
                    match cmd {
                        WriteCommand::Start(d) => desc = Some(d),
                        WriteCommand::Part(part) => {
                            let desc = desc.as_ref().expect("`Part` follows `Start`");
                            // Start the write only once there are updates, since
                            // `maybe_start_batch` requests a write for every ready interval,
                            // including idle ones. A worker that never joins a shared batch
                            // cannot be the last holder responsible for delivering it.
                            let write =
                                write.get_or_insert_with(|| batch_writer.shared.start_parts(desc));
                            write.push_part(part).await;
                        }
                        WriteCommand::Finish => {
                            desc = None;
                            let batches = match write.take() {
                                Some(write) => write.finish().await,
                                None => Vec::new(),
                            };
                            batch_writer.respond(&resp_tx, batches).await;
                            // Activate the operator to emit the batches. ArcActivator suppresses
                            // redundant activations, so this is cheap.
                            activator.activate();
                        }
                    }
                }
            },
        )
        .abort_on_drop();

        // Wakes the operator when the write task takes a part, so a pending batch write resumes.
        let waker = Waker::from(Arc::new(ActivateOnWake(
            scope.worker().sync_activator_for(info.address.to_vec()),
        )));

        // In read-only mode, wake the operator when writes are allowed so the forced
        // consolidation stops re-arming and the batch-write path takes over promptly. Without
        // this the operator only learns of the transition on its next input activation, which an
        // idle sink may not get for a while.
        let read_only_watch_handle = (*read_only_rx.borrow()).then(|| {
            let sync_activator = scope.worker().sync_activator_for(info.address.to_vec());
            let mut rx = read_only_rx.clone();
            mz_ore::task::spawn(
                || operator_name(sink_id, "write::read_only_watch"),
                async move {
                    let _ = rx.changed().await;
                    let _ = sync_activator.activate();
                },
            )
            .abort_on_drop()
        });

        // A sync operator, not the async bridge. The bridge queues input updates verbatim until its
        // future polls them. This operator consolidates them into the corrections as they arrive.
        // It only needs async to yield while the write task holds the parts it can take, which
        // `write_batch` covers.
        builder.build(move |capabilities| {
            // We will use the data capabilities from the `descs` input to produce output, so no
            // need to hold onto the initial capabilities.
            drop(capabilities);

            let read_only = *read_only_rx.borrow_and_update();
            let mut state = State::new(
                sink_id,
                worker_id,
                corrections,
                as_of,
                advance_persist_frontiers_at_startup,
                read_only,
            );

            // On drop, the logger drains remaining events and retracts all logged state.
            let mut correction_logger = correction_logger;

            move |frontiers| {
                // Keep task handles alive so they are aborted when the operator is dropped.
                let _ = &write_task_handle;
                let _ = &read_only_watch_handle;

                // Acknowledge activation so the Tokio task can activate us again.
                activation_ack.ack();

                // `write_batch` is async because `cmd_tx` is bounded, so a send can wait for the
                // write task. While the channel is full, the future is pending and the operator
                // returns, so other Timely operators on this worker run. The write task taking a
                // command wakes `waker`, which activates the operator to poll the future again.
                let mut cx = Context::from_waker(&waker);
                state.poll_write(&mut cx);

                descs_input.for_each(|cap, data| {
                    let cap = cap.retain(0);
                    for desc in data.drain(..) {
                        state.absorb_batch_description(desc, cap.clone());
                    }
                });

                // NOTE: While a batch write holds the corrections, the data inputs stay unread
                // and their frontiers unobserved. Timely holds the updates until the write returns
                // the corrections. The frontiers are read afresh then, so no batch starts before
                // its updates are in the corrections. A batch starts only once `desired` passed
                // its `upper`. The updates that wait meanwhile arrived during the write, and are
                // not the snapshot.
                if let Some(corrections) = state.write.corrections_mut() {
                    // `desired` updates enter the corrections as positive contributions and
                    // `persist` updates as negative ones, so the buffers hold `desired - persist`,
                    // the updates that bring the shard in line with `desired`.
                    //
                    // NOTE: Inserting on this worker keeps it busy until the buffers absorb the
                    // updates, which holds back the operators feeding it. Under memory pressure,
                    // this limits the input to the rate at which the buffers page out. Handing the
                    // updates to another thread lets them pile up unbounded in front of the
                    // buffers.
                    desired_ok_input.for_each(|_cap, data| corrections.ok.insert(data));
                    desired_err_input.for_each(|_cap, data| corrections.err.insert(data));
                    persist_ok_input.for_each(|_cap, data| corrections.ok.insert_negated(data));
                    persist_err_input.for_each(|_cap, data| corrections.err.insert_negated(data));

                    state.advance_desired_frontiers(
                        frontiers[0].frontier(),
                        frontiers[1].frontier(),
                    );
                    state.advance_persist_frontiers(
                        frontiers[2].frontier(),
                        frontiers[3].frontier(),
                    );
                    state.maybe_force_consolidation();
                }

                // Track read-only mode so the forced consolidation stops re-arming once writes
                // are allowed and the batch-write path takes over driving consolidation.
                if state.read_only && read_only_rx.has_changed().unwrap_or(false) {
                    if !*read_only_rx.borrow_and_update() {
                        state.set_read_only(false);
                    }
                }

                loop {
                    match resp_rx.try_recv() {
                        Ok(resp) => {
                            let (desc, cap) = state
                                .complete_write()
                                .expect("the write task only answers a finished batch write");
                            if resp.batches.is_empty() {
                                state.trace("skipping empty batch");
                            } else {
                                state.trace(format!("wrote {} batches", resp.batches.len()));
                                let mut out = batches_output.activate();
                                let mut session = out.session(&cap);
                                for batch in resp.batches {
                                    session.give((desc.clone(), batch));
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            panic!("write task unexpectedly gone");
                        }
                    }
                }

                state.maybe_start_batch(&mut cx, |corrections, desc| {
                    Box::pin(write_batch(
                        corrections,
                        desc.clone(),
                        Rc::clone(&key_schema),
                        PART_BYTES,
                        Rc::clone(&cmd_tx),
                        write_metrics.clone(),
                    ))
                });

                // Drain last, so the events of this activation's correction work reach the log
                // without waiting for another activation.
                if let Some(logger) = &mut correction_logger {
                    logger.apply_events();
                }
            }
        });

        batches_output_stream
    }

    /// Counters a batch write reports to.
    #[derive(Clone)]
    struct WriteMetrics {
        wait_seconds: GenericCounter<AtomicF64>,
        part_bytes: IntCounter,
    }

    /// Activates an operator when woken.
    struct ActivateOnWake(SyncActivator);

    impl Wake for ActivateOnWake {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            // The worker may have shut down by the time the write task wakes us.
            let _ = self.0.activate();
        }
    }

    /// Writes the corrections before `desc.upper` as one batch and returns them.
    ///
    /// Sends the write task a `Start`, the updates cut into parts of `part_bytes`, and a
    /// `Finish`. The future is pending while `cmd_tx` is full, and the write task receiving a
    /// command wakes it.
    async fn write_batch(
        mut corrections: Corrections,
        desc: BatchDescription,
        key_schema: Rc<RelationDesc>,
        part_bytes: usize,
        cmd_tx: Rc<mpsc::Sender<WriteCommand>>,
        metrics: WriteMetrics,
    ) -> Corrections {
        let send = async |cmd| {
            let start = Instant::now();
            cmd_tx
                .send(cmd)
                .await
                .expect("write task unexpectedly gone");
            metrics.wait_seconds.inc_by(start.elapsed().as_secs_f64());
        };

        let upper = &desc.upper;
        corrections.ok.consolidate_before(upper);
        corrections.err.consolidate_before(upper);

        send(WriteCommand::Start(desc.clone())).await;
        let oks = corrections
            .ok
            .consolidated_updates_before(upper)
            .map(|(d, t, r)| (SourceData(Ok(d)), t, r));
        let errs = corrections
            .err
            .consolidated_updates_before(upper)
            .map(|(d, t, r)| (SourceData(Err(d.deserialize())), t, r));
        let mut part = PartBuilder::new(&*key_schema, &UnitSchema);
        for (data, t, r) in oks.chain(errs) {
            part.push(&data, &(), t, r.into_inner());
            if part.goodbytes() >= part_bytes {
                metrics.part_bytes.inc_by(u64::cast_from(part.goodbytes()));
                let full = part.finish_and_replace(&*key_schema, &UnitSchema);
                send(WriteCommand::Part(full)).await;
            }
        }
        if part.len() > 0 {
            metrics.part_bytes.inc_by(u64::cast_from(part.goodbytes()));
            send(WriteCommand::Part(part.finish())).await;
        }
        send(WriteCommand::Finish).await;
        corrections
    }

    /// The batch-writing context owned by the Tokio write task.
    ///
    /// The `writer` cleans up a finished batch when the response channel has already gone away.
    struct BatchWriter {
        writer: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        shared: SharedBatchWriter,
    }

    impl BatchWriter {
        async fn new(
            persist_api: &PersistApi,
            shared_batch_max_bytes: ConfigValHandle<usize>,
        ) -> Self {
            let writer = persist_api.open_writer().await;
            let shared = SharedBatchWriter::new(persist_api, &writer, shared_batch_max_bytes).await;
            Self { writer, shared }
        }

        /// Sends the written `batches` to the operator, deleting them if it is gone.
        async fn respond(
            &self,
            resp_tx: &mpsc::UnboundedSender<WriteResponse>,
            batches: Vec<ProtoBatch>,
        ) {
            if let Err(err) = resp_tx.send(WriteResponse { batches }) {
                for proto_batch in err.0.batches {
                    let batch = self.writer.batch_from_transmittable_batch(proto_batch);
                    batch.delete().await;
                }
            }
        }
    }

    /// A batch write in progress, which returns the corrections once the write task has taken all
    /// of its parts.
    type BatchWriteFuture = Pin<Box<dyn Future<Output = Corrections>>>;

    /// A batch write whose parts the write task has taken.
    struct WrittenBatch {
        corrections: Corrections,
        desc: BatchDescription,
        /// The capability to emit the batch's response with.
        cap: Capability<Timestamp>,
    }

    /// The batch write of the `write` operator.
    enum Write {
        /// No batch write is in flight.
        Idle(Corrections),
        /// A batch write holds the corrections while it waits for the write task to take its
        /// parts.
        Pending(Pin<Box<dyn Future<Output = WrittenBatch>>>),
        /// The batch write returned the corrections and waits for the write task's response.
        AwaitingResponse(WrittenBatch),
    }

    impl Write {
        /// Returns the corrections, unless a pending batch write holds them.
        fn corrections_mut(&mut self) -> Option<&mut Corrections> {
            match self {
                Write::Idle(corrections)
                | Write::AwaitingResponse(WrittenBatch { corrections, .. }) => Some(corrections),
                Write::Pending(_) => None,
            }
        }

        /// Moves the state out, leaving a pending write that never completes in its place.
        fn take(&mut self) -> Self {
            // `pending()` is zero-sized, so boxing it does not allocate.
            std::mem::replace(self, Write::Pending(Box::pin(std::future::pending())))
        }
    }

    /// State maintained by the `write` operator.
    struct State {
        sink_id: GlobalId,
        worker_id: usize,
        /// The frontiers of the `desired` inputs.
        desired_frontiers: OkErr<Antichain<Timestamp>, Antichain<Timestamp>>,
        /// The frontiers of the `persist` inputs.
        ///
        /// Note that this is _not_ the same as the write frontier of the output persist shard! It
        /// usually is, but during snapshot processing, these frontiers will start at the shard's
        /// read frontier, so they can be beyond its write frontier. This is important as it means
        /// we must not discard batch descriptions based on these persist frontiers: A batch
        /// description might still be valid even if its `lower` is before the persist frontiers we
        /// observe.
        persist_frontiers: OkErr<Antichain<Timestamp>, Antichain<Timestamp>>,
        /// The current valid batch description and associated output capability, if any.
        batch_description: Option<(BatchDescription, Capability<Timestamp>)>,
        /// A request to force a consolidation of corrections once both `desired_frontiers` and
        /// `persist_frontiers` become greater than the given frontier.
        ///
        /// Normally we force a consolidation whenever we write a batch, but there are periods
        /// (like read-only mode) when that doesn't happen, and we need to manually force
        /// consolidation instead. In read-only mode this is re-armed after each firing so it
        /// keeps sweeping forward as the frontiers advance (see `maybe_force_consolidation`).
        force_consolidation_after: Option<Antichain<Timestamp>>,
        /// Whether the sink is in read-only mode. While read-only, the forced consolidation
        /// re-arms after each firing (see `maybe_force_consolidation`).
        read_only: bool,
        /// The corrections, and the batch write that holds them, if any.
        write: Write,
    }

    impl State {
        fn new(
            sink_id: GlobalId,
            worker_id: usize,
            corrections: Corrections,
            as_of: Antichain<Timestamp>,
            advance_persist_frontiers_at_startup: bool,
            read_only: bool,
        ) -> Self {
            // Force a consolidation of corrections after the snapshot updates have been fully
            // processed, to ensure we get rid of those as quickly as possible.
            let force_consolidation_after = Some(as_of.clone());

            let mut state = Self {
                sink_id,
                worker_id,
                desired_frontiers: OkErr::new_frontiers(),
                persist_frontiers: OkErr::new_frontiers(),
                batch_description: None,
                force_consolidation_after,
                read_only,
                write: Write::Idle(corrections),
            };

            // Immediately advance the persist frontier tracking to the `as_of`.
            // This is important to ensure the persist sink doesn't get stuck if the output shard's
            // initial frontier is less than the `as_of`. The `mint` operator first emits a batch
            // description with `lower = as_of`, and the `write` operator only emits a batch when
            // its observed persist frontier is >= the batch description's `lower`, which (assuming
            // no other writers) would be never if we didn't advance the observed persist frontier
            // to the `as_of`.
            //
            // NOTE: This also advances the since of the corrections to the `as_of`. Without that,
            // snapshot updates stay at times below the `as_of`, and the first batch, with
            // `lower = as_of`, trips persist's `UpdateNotBeyondLower`.
            if advance_persist_frontiers_at_startup {
                state.advance_persist_frontiers(as_of.borrow(), as_of.borrow());
            }

            state
        }

        fn trace<S: AsRef<str>>(&self, message: S) {
            let message = message.as_ref();
            trace!(
                sink_id = %self.sink_id,
                worker = %self.worker_id,
                desired_frontier = ?self.desired_frontiers.frontier().elements(),
                persist_frontier = ?self.persist_frontiers.frontier().elements(),
                batch_description = ?self.batch_description.as_ref().map(|(d, _)| d),
                message,
            );
        }

        fn advance_desired_frontiers(
            &mut self,
            ok: AntichainRef<Timestamp>,
            err: AntichainRef<Timestamp>,
        ) {
            if advance(&mut self.desired_frontiers.ok, ok) {
                self.trace("advanced `desired` ok frontier");
            }
            if advance(&mut self.desired_frontiers.err, err) {
                self.trace("advanced `desired` err frontier");
            }
        }

        /// Advances the `persist` frontiers, and the since of the corrections to them.
        ///
        /// Does nothing while a batch write holds the corrections.
        fn advance_persist_frontiers(
            &mut self,
            ok: AntichainRef<Timestamp>,
            err: AntichainRef<Timestamp>,
        ) {
            let Some(corrections) = self.write.corrections_mut() else {
                return;
            };
            let ok_advanced = advance(&mut self.persist_frontiers.ok, ok);
            let err_advanced = advance(&mut self.persist_frontiers.err, err);
            // The since is the `persist` frontier, and a batch starts only once its `lower` is at
            // or before that frontier. Rounding update times up to the since therefore lifts
            // every update to at least the `lower` of the next batch to start, as persist
            // requires.
            if ok_advanced || err_advanced {
                let since = self.persist_frontiers.frontier().to_owned();
                corrections.ok.advance_since(since.clone());
                corrections.err.advance_since(since);
            }
            if ok_advanced {
                self.trace("advanced `persist` ok frontier");
            }
            if err_advanced {
                self.trace("advanced `persist` err frontier");
            }
        }

        /// Consolidate the corrections if a forced consolidation is due.
        ///
        /// Does nothing while a batch write holds the corrections.
        fn maybe_force_consolidation(&mut self) {
            let Some(request) = &self.force_consolidation_after else {
                return;
            };
            let Some(corrections) = self.write.corrections_mut() else {
                return;
            };

            let desired_frontier = self.desired_frontiers.frontier();
            let persist_frontier = self.persist_frontiers.frontier();
            if PartialOrder::less_than(request, desired_frontier)
                && PartialOrder::less_than(request, persist_frontier)
            {
                // In read-only mode the sink mints no batches, so no batch write sweeps
                // `consolidate_before(upper)` forward. A single forced consolidation only covers
                // the band below the `since` at the moment it fires. That leaves the forward
                // region uncancelled for the whole read-only window (CLU-131). Re-arming at the
                // current persist frontier keeps the forced consolidation sweeping forward as
                // `desired` and `persist` advance. Once writes are allowed, batch writes take
                // over and the request disarms.
                self.force_consolidation_after = if self.read_only {
                    Some(persist_frontier.to_owned())
                } else {
                    None
                };
                corrections.ok.consolidate_at_since();
                corrections.err.consolidate_at_since();
                self.trace("forced correction consolidation");
            }
        }

        /// Update read-only mode. Once writes are allowed, a still-armed forced consolidation
        /// fires one final time and then disarms.
        fn set_read_only(&mut self, read_only: bool) {
            self.read_only = read_only;
        }

        fn absorb_batch_description(&mut self, desc: BatchDescription, cap: Capability<Timestamp>) {
            // Enforce monotonicity: drop descriptions whose `lower` regresses below the one we
            // already hold. The `mint` operator only emits strictly increasing `lower`s
            // (invariant 1), so a regression means this description is outdated. We cannot use
            // `persist_frontiers` for the same check, because during snapshot processing those
            // frontiers can be ahead of the shard's write frontier and a still-valid description
            // may have a `lower` below them.
            if let Some((prev, _)) = &self.batch_description {
                if PartialOrder::less_than(&desc.lower, &prev.lower) {
                    self.trace(format!("skipping outdated batch description: {desc:?}"));
                    return;
                }
            }

            self.batch_description = Some((desc, cap));
            self.trace("set batch description");
        }

        /// Polls the batch write, if one is pending.
        fn poll_write(&mut self, cx: &mut Context<'_>) {
            if let Write::Pending(future) = &mut self.write {
                if let Poll::Ready(written) = future.as_mut().poll(cx) {
                    self.write = Write::AwaitingResponse(written);
                }
            }
        }

        /// Starts a batch write for the current batch description if no batch write is in flight
        /// and a batch can be written for the description.
        ///
        /// `write_batch` returns the future that writes the corrections for the description.
        fn maybe_start_batch(
            &mut self,
            cx: &mut Context<'_>,
            write_batch: impl FnOnce(Corrections, &BatchDescription) -> BatchWriteFuture,
        ) {
            let corrections = match self.write.take() {
                Write::Idle(corrections) => corrections,
                in_flight => {
                    self.write = in_flight;
                    return;
                }
            };

            // We can write a new batch if we have seen all `persist` updates before `lower` and
            // all `desired` updates before `upper`.
            let persist_frontier = self.persist_frontiers.frontier();
            let desired_frontier = self.desired_frontiers.frontier();
            let ready = self.batch_description.take_if(|(desc, _cap)| {
                PartialOrder::less_equal(&desc.lower, persist_frontier)
                    && PartialOrder::less_equal(&desc.upper, desired_frontier)
            });
            let Some((desc, cap)) = ready else {
                self.write = Write::Idle(corrections);
                return;
            };

            self.trace(format!("write batch description: {desc:?}"));
            let future = write_batch(corrections, &desc);
            self.write = Write::Pending(Box::pin(async move {
                let corrections = future.await;
                WrittenBatch {
                    corrections,
                    desc,
                    cap,
                }
            }));
            self.poll_write(cx);
        }

        /// Completes the batch write the write task answered, returning the description and
        /// capability of its batch.
        ///
        /// Returns `None` unless a batch write awaits a response.
        fn complete_write(&mut self) -> Option<(BatchDescription, Capability<Timestamp>)> {
            match self.write.take() {
                Write::AwaitingResponse(WrittenBatch {
                    corrections,
                    desc,
                    cap,
                }) => {
                    self.write = Write::Idle(corrections);
                    Some((desc, cap))
                }
                other => {
                    self.write = other;
                    None
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use mz_ore::metrics::MetricsRegistry;
        use mz_persist_client::cfg::PersistConfig;
        use mz_persist_client::metrics::Metrics;
        use mz_repr::{Datum, SqlScalarType};

        use super::*;

        fn corrections() -> Corrections {
            let registry = MetricsRegistry::new();
            let metrics = Metrics::new(&PersistConfig::new_for_tests(), &registry);
            let sink_metrics = metrics.sink.clone();
            let worker_metrics = sink_metrics.for_worker(0);
            let config = mz_dyncfgs::all_dyncfgs();
            OkErr::new(
                Correction::new(sink_metrics.clone(), worker_metrics.clone(), None, &config),
                Correction::new(sink_metrics, worker_metrics, None, &config),
            )
        }

        #[mz_ore::test]
        fn write_batch_yields_while_the_write_task_is_full() {
            let mut corrections = corrections();
            let mut updates = (0..3)
                .map(|i| {
                    (
                        Row::pack_slice(&[Datum::Int64(i)]),
                        Timestamp::from(0),
                        Diff::from(1),
                    )
                })
                .collect();
            corrections.ok.insert(&mut updates);
            let desc = BatchDescription::new(
                Antichain::from_elem(Timestamp::from(0)),
                Antichain::from_elem(Timestamp::from(1)),
                0,
            );
            let key_schema = Rc::new(
                RelationDesc::builder()
                    .with_column("a", SqlScalarType::Int64.nullable(false))
                    .finish(),
            );
            let metrics = WriteMetrics {
                wait_seconds: GenericCounter::new("wait", "wait").expect("valid metric"),
                part_bytes: IntCounter::new("bytes", "bytes").expect("valid metric"),
            };
            // A channel of one, and a part per update.
            let (tx, mut rx) = mpsc::channel(1);
            let mut write = Box::pin(write_batch(
                corrections,
                desc,
                key_schema,
                1,
                Rc::new(tx),
                metrics,
            ));

            let mut cx = Context::from_waker(Waker::noop());
            let mut commands = Vec::new();
            let mut pending = 0;
            let corrections = loop {
                match write.as_mut().poll(&mut cx) {
                    Poll::Ready(corrections) => break corrections,
                    Poll::Pending => {
                        pending += 1;
                        let cmd = rx
                            .try_recv()
                            .expect("pending only while the channel is full");
                        commands.push(cmd);
                    }
                }
            };
            while let Ok(cmd) = rx.try_recv() {
                commands.push(cmd);
            }

            assert!(pending > 0, "a full channel makes the write pending");
            let shape: Vec<_> = commands
                .iter()
                .map(|cmd| match cmd {
                    WriteCommand::Start(_) => "start",
                    WriteCommand::Part(_) => "part",
                    WriteCommand::Finish => "finish",
                })
                .collect();
            assert_eq!(shape, ["start", "part", "part", "part", "finish"]);
            let upper = Antichain::from_elem(Timestamp::from(1));
            assert_eq!(
                corrections.ok.consolidated_updates_before(&upper).count(),
                3,
                "the write hands the corrections back unchanged"
            );
        }
    }
}

/// Implementation of the `append` operator.
mod append {
    use super::*;

    /// Commands sent from the Timely operator to the Tokio append task.
    enum AppendCommand {
        /// A new batch description has been received.
        Description(BatchDescription),
        /// A written batch has been received.
        Batch(ProtoBatch),
        /// The batches frontier has advanced.
        BatchesFrontier(Antichain<Timestamp>),
    }

    /// Render the `append` operator.
    ///
    /// The parameters passed in are:
    ///  * `sink_id`: The `GlobalId` of the sink export.
    ///  * `persist_api`: An object providing access to the output persist shard.
    ///  * `descs`: The stream of batch descriptions produced by the `mint` operator.
    ///  * `batches`: The stream of written batches produced by the `write` operator.
    pub fn render<'s>(
        sink_id: GlobalId,
        persist_api: PersistApi,
        descs: DescsStream<'s>,
        batches: BatchesStream<'s>,
    ) {
        let scope = descs.scope();
        let worker_id = scope.index();

        let name = operator_name(sink_id, "append");
        let mut builder = OperatorBuilderRc::new(name, scope.clone());
        let mut descs_input = builder.new_input(descs, Pipeline);
        let batch_exchange =
            Exchange::new(|(desc, _): &(BatchDescription, _)| u64::cast_from(desc.append_worker));
        let mut batches_input = builder.new_input(batches, batch_exchange);

        // Channel for commands to the Tokio append task.
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<AppendCommand>();

        // Spawn Tokio task that owns the append state machine.
        let append_task_handle =
            mz_ore::task::spawn(|| operator_name(sink_id, "append"), async move {
                let writer = persist_api.open_writer().await;
                let mut state = State::new(sink_id, worker_id, writer);

                while let Some(cmd) = cmd_rx.recv().await {
                    match cmd {
                        AppendCommand::Description(desc) => {
                            state.absorb_batch_description(desc).await;
                            state.maybe_append_batches().await;
                        }
                        AppendCommand::Batch(batch) => {
                            state.absorb_batch(batch).await;
                        }
                        AppendCommand::BatchesFrontier(frontier) => {
                            state.advance_batches_frontier(frontier.borrow());
                            state.maybe_append_batches().await;
                        }
                    }
                }
            })
            .abort_on_drop();

        builder.build(move |_capabilities| {
            let mut prev_batches_frontier = Antichain::from_elem(Timestamp::MIN);

            move |frontiers| {
                // Keep task handle alive so it is aborted when the operator is dropped.
                let _ = &append_task_handle;

                // Forward batch descriptions to the Tokio task.
                descs_input.for_each(|_cap, data| {
                    for desc in data.drain(..) {
                        cmd_tx
                            .send(AppendCommand::Description(desc))
                            .expect("append task unexpectedly gone");
                    }
                });

                // Forward batches to the Tokio task.
                batches_input.for_each(|_cap, data| {
                    for (_desc, batch) in data.drain(..) {
                        // The batch description is only used for routing and we ignore it
                        // here since we already get one from `descs_input`.
                        cmd_tx
                            .send(AppendCommand::Batch(batch))
                            .expect("append task unexpectedly gone");
                    }
                });

                // Forward batches frontier advancements *after* the per-activation
                // `Description`/`Batch` sends above. The Tokio task drains commands FIFO and only
                // calls `maybe_append_batches` on `Description`/`BatchesFrontier`; if a frontier
                // advance arrived before its batches, the task could append an incomplete set.
                // See module-level docs for the full ordering invariant.
                let new_batches_frontier = frontiers[1].frontier();
                if PartialOrder::less_than(&prev_batches_frontier.borrow(), &new_batches_frontier) {
                    prev_batches_frontier.clear();
                    prev_batches_frontier.extend(new_batches_frontier.iter().cloned());
                    cmd_tx
                        .send(AppendCommand::BatchesFrontier(
                            new_batches_frontier.to_owned(),
                        ))
                        .expect("append task unexpectedly gone");
                }
            }
        });
    }

    /// State maintained by the `append` Tokio task.
    struct State {
        sink_id: GlobalId,
        worker_id: usize,
        persist_writer: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        /// The current input frontier of `batches`.
        batches_frontier: Antichain<Timestamp>,
        /// The greatest observed `lower` from both `descs` and `batches`.
        lower: Antichain<Timestamp>,
        /// The batch description for `lower`, if any.
        batch_description: Option<BatchDescription>,
        /// Batches received for `lower`.
        batches: Vec<Batch<SourceData, (), Timestamp, StorageDiff>>,
    }

    impl State {
        fn new(
            sink_id: GlobalId,
            worker_id: usize,
            persist_writer: WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        ) -> Self {
            Self {
                sink_id,
                worker_id,
                persist_writer,
                batches_frontier: Antichain::from_elem(Timestamp::MIN),
                lower: Antichain::from_elem(Timestamp::MIN),
                batch_description: None,
                batches: Default::default(),
            }
        }

        fn trace<S: AsRef<str>>(&self, message: S) {
            let message = message.as_ref();
            trace!(
                sink_id = %self.sink_id,
                worker = %self.worker_id,
                batches_frontier = ?self.batches_frontier.elements(),
                lower = ?self.lower.elements(),
                batch_description = ?self.batch_description,
                message,
            );
        }

        fn advance_batches_frontier(&mut self, frontier: AntichainRef<Timestamp>) {
            if advance(&mut self.batches_frontier, frontier) {
                self.trace("advanced `batches` frontier");
            }
        }

        /// Advance the current `lower`.
        ///
        /// Discards all currently stashed batches and batch descriptions, assuming that they are
        /// now invalid.
        async fn advance_lower(&mut self, frontier: Antichain<Timestamp>) {
            assert!(PartialOrder::less_than(&self.lower, &frontier));

            self.lower = frontier;
            self.batch_description = None;

            // Remove stashed batches, cleaning up those we didn't append.
            for batch in self.batches.drain(..) {
                batch.delete().await;
            }

            self.trace("advanced `lower`");
        }

        /// Absorb the given batch description into the state, provided it is not outdated.
        async fn absorb_batch_description(&mut self, desc: BatchDescription) {
            if PartialOrder::less_than(&self.lower, &desc.lower) {
                self.advance_lower(desc.lower.clone()).await;
            } else if &self.lower != &desc.lower {
                self.trace(format!("skipping outdated batch description: {desc:?}"));
                return;
            }

            if desc.append_worker == self.worker_id {
                self.batch_description = Some(desc);
                self.trace("set batch description");
            }
        }

        /// Absorb the given batch into the state, provided it is not outdated.
        async fn absorb_batch(&mut self, batch: ProtoBatch) {
            let batch = self.persist_writer.batch_from_transmittable_batch(batch);
            if PartialOrder::less_than(&self.lower, batch.lower()) {
                self.advance_lower(batch.lower().clone()).await;
            } else if &self.lower != batch.lower() {
                self.trace(format!(
                    "skipping outdated batch: ({:?}, {:?})",
                    batch.lower().elements(),
                    batch.upper().elements(),
                ));

                // Ensure the batch's data gets properly cleaned up before dropping it.
                batch.delete().await;
                return;
            }

            self.batches.push(batch);
            self.trace("absorbed a batch");
        }

        async fn maybe_append_batches(&mut self) {
            let batches_complete = PartialOrder::less_than(&self.lower, &self.batches_frontier);
            if !batches_complete {
                return;
            }

            let Some(desc) = self.batch_description.take() else {
                return;
            };

            let new_lower = match self.append_batches(desc).await {
                Ok(shard_upper) => {
                    self.trace("appended a batch");
                    shard_upper
                }
                Err(shard_upper) => {
                    // Failing the append is expected in the presence of concurrent replicas. There
                    // is nothing special to do here: The self-correcting feedback mechanism
                    // ensures that we observe the concurrent changes, compute their consequences,
                    // and append them at a future time.
                    self.trace(format!(
                        "append failed due to `lower` mismatch: {:?}",
                        shard_upper.elements(),
                    ));
                    shard_upper
                }
            };

            self.advance_lower(new_lower).await;
        }

        /// Append the current `batches` to the output shard.
        ///
        /// Returns whether the append was successful or not, and the current shard upper in either
        /// case.
        ///
        /// This method advances the shard upper to the batch `lower` if necessary. This is the
        /// mechanism that brings the shard upper to the sink as-of when appending the initial
        /// batch.
        ///
        /// An alternative mechanism for bringing the shard upper to the sink as-of would be making
        /// a single append at operator startup. The reason we are doing it here instead is that it
        /// simplifies the implementation of read-only mode. In read-only mode we have to defer any
        /// persist writes, including the initial upper bump. Having only a single place that
        /// performs writes makes it easy to ensure we are doing that correctly.
        async fn append_batches(
            &mut self,
            desc: BatchDescription,
        ) -> Result<Antichain<Timestamp>, Antichain<Timestamp>> {
            let (lower, upper) = (desc.lower, desc.upper);
            let mut to_append: Vec<_> = self.batches.iter_mut().collect();

            loop {
                let result = self
                    .persist_writer
                    .compare_and_append_batch(&mut to_append, lower.clone(), upper.clone(), true)
                    .await
                    .expect("valid usage");

                match result {
                    Ok(()) => return Ok(upper),
                    Err(mismatch) if PartialOrder::less_than(&mismatch.current, &lower) => {
                        advance_shard_upper(&mut self.persist_writer, lower.clone()).await;

                        // At this point the shard's since and upper are likely the same, a state
                        // that is likely to hit edge-cases in logic reasoning about frontiers.
                        fail::fail_point!("mv_advanced_upper");
                    }
                    Err(mismatch) => return Err(mismatch.current),
                }
            }
        }
    }

    /// Advance the frontier of the given writer's shard to at least the given `upper`.
    async fn advance_shard_upper(
        persist_writer: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        upper: Antichain<Timestamp>,
    ) {
        let empty_updates: &[((SourceData, ()), Timestamp, StorageDiff)] = &[];
        let lower = Antichain::from_elem(Timestamp::MIN);
        persist_writer
            .append(empty_updates, lower, upper)
            .await
            .expect("valid usage")
            .expect("should always succeed");
    }
}
