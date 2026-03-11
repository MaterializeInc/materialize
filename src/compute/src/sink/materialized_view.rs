// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A dataflow sink that writes input records to a persist shard.
//!
//! This implementation is both parallel and self-correcting.
//!
//!  * parallel: Multiple workers can participate in writing updates for the same times, letting
//!    sink throughput scale with the number of workers allocated to the replica.
//!  * self-correcting: The sink continually compares the contents of the persist shard with the
//!    contents of the input collection and writes down the difference. If the persist shard ends
//!    up with undesired contents for any reason, this is corrected the next time the sink manages
//!    to append to the shard.
//!
//! ### Operators
//!
//! The persist sink consists of a graph of operators.
//!
//!    desired                    persist <---------------.
//!       |                          |                    |
//!       |                          |                    |
//!       |---------------------.    |                    |
//!       |                     |    |                    |
//!       |                     |    |                    |
//!       v                     v    v                    |
//!   +--------+              +--------+              +--------+
//!   |  mint  | --descs-.--> | write  | --batches--> | append |
//!   +--------+          \   +--------+          .-> +--------+
//!                        \_____________________/
//!
//!  * `mint` mints batch descriptions, i.e., `(lower, upper)` bounds of batches that should be
//!    written. The persist API requires that all workers write batches with the same bounds, so
//!    they can be appended as a single logical batch. To ensure this, the `mint` operator only
//!    runs on a single worker that broadcasts minted descriptions to all workers. Batch bounds are
//!    picked based on the frontiers of the `desired` stream and the output persist shard.
//!  * `write` stages batch data in persist, based on the batch descriptions received from the
//!    `mint` operator, but without appending it to the persist shard. This is a multi-worker
//!    operator, with each worker writing batches of the data that arrives at its local inputs. To
//!    do so it reads from the `desired` and `persist` streams and produces the difference between
//!    them to write back out, ensuring that the final contents of the persist shard match
//!    `desired`.
//!  * `append` appends the batches minted by `mint` and written by `write` to the persist shard.
//!    This is a multi-worker operator, where workers are responsible for different subsets of
//!    batch descriptions. If a worker is responsible for a given batch description, it waits for
//!    all workers to stage their batches for that batch description, then appends all the batches
//!    together as a single logical batch.
//!
//! Note that while the above graph suggests that `mint` and `write` both receive copies of the
//! `desired` stream, the actual implementation passes that stream through `mint` and lets `write`
//! read the passed-through stream, to avoid cloning data.
//!
//! Also note that the `append` operator's implementation would perhaps be more natural as a
//! single-worker implementation. The purpose of sharing the work between all workers is to avoid a
//! work imbalance where one worker is overloaded (doing both appends and the consequent persist
//! maintenance work) while others are comparatively idle.
//!
//! The persist sink is written to be robust to the presence of other conflicting instances (e.g.
//! from other replicas) writing to the same persist shard. Each of the three operators needs to be
//! able to handle conflicting writes that unexpectedly change the contents of the output persist
//! shard.
//!
//! ### Frontiers
//!
//! The `desired` frontier tracks the progress of the upstream dataflow, but may be rounded up to
//! the next refresh time for dataflows that follow a refresh schedule other than "on commit".
//!
//! The `persist` frontier tracks the `upper` frontier of the target persist shard, with one
//! exception: When the `persist_source` that reads back the shard is rendered, it will start
//! reading at its `since` frontier. So if the shard's `since` is initially greater than its
//! `upper`, the `persist` frontier too will be in advance of the shard `upper`, until the `upper`
//! has caught up. To avoid getting confused by this edge case, the `mint` operator does not use
//! the `persist` stream to observe the shard frontier but keeps its own `WriteHandle` instead.
//!
//! The `descs` frontier communicates which `lower` bounds may still be emitted in batch
//! descriptions. All future batch descriptions will have a `lower` that is greater or equal to the
//! current `descs` frontier.
//!
//! The `batches` frontier communicates for which `lower` bounds batches may still be written. All
//! batches for descriptions with `lower`s less than the current `batches` frontier have already
//! been written.
//!
//! ### Invariants
//!
//! The implementation upholds several invariants that can be relied upon to simplify the
//! implementation:
//!
//!  1. `lower`s in minted batch descriptions are unique and strictly increasing. That is, the
//!     `mint` operator will never mint the same `lower` twice and a minted `lower` is always
//!     greater than any previously minted ones.
//!  2. `upper`s in minted batch descriptions are monotonically increasing.
//!  3. From (1) follows that there is always at most one "valid" batch description in flight in
//!     the operator graph. "Valid" here means that the described batch can be appended to the
//!     persist shard.
//!
//! The main simplification these invariants allow is that operators only need to keep track of the
//! most recent batch description and/or `lower`. Previous batch descriptions are not valid
//! anymore, so there is no reason to hold any state or perform any work in support of them.
//!
//! ### Read-only Mode
//!
//! The persist sink can optionally be initialized in read-only mode. In this mode it is passive
//! and avoids any writes to persist. Activating the `read_only_rx` transitions the sink into write
//! mode, where it commences normal operation.
//!
//! Read-only mode is implemented by the `mint` operator. To disable writes, the `mint` operator
//! simply avoids minting any batch descriptions. Since both the `write` and the `append` operator
//! require batch descriptions to write/append batches, this suppresses any persist communication.
//! At the same time, the `write` operator still observes changes to the `desired` and `persist`
//! collections, allowing it to keep its correction buffer up-to-date.

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{AsCollection, Hashable, VecCollection};
use mz_compute_types::sinks::{ComputeSinkDesc, MaterializedViewSinkConnection};
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_persist_client::batch::{Batch, ProtoBatch};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::operators::shard_source::{ErrorHandler, SnapshotMode};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::PressOnDropButton;
use mz_timely_util::probe::{Handle, ProbeNotify};
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder as OperatorBuilderRc;
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::{Capability, CapabilitySet, probe};
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Antichain;
use timely::progress::frontier::AntichainRef;
use tokio::sync::{mpsc, watch};
use tracing::trace;

use crate::compute_state::ComputeState;
use crate::render::StartSignal;
use crate::render::sinks::SinkRender;
use crate::sink::correction::{ChannelLogging, Correction, CorrectionLogger};
use crate::sink::refresh::apply_refresh;

impl<G> SinkRender<G> for MaterializedViewSinkConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        start_signal: StartSignal,
        mut ok_collection: VecCollection<G, Row, Diff>,
        mut err_collection: VecCollection<G, DataflowError, Diff>,
        _ct_times: Option<VecCollection<G, (), Diff>>,
        output_probe: &Handle<Timestamp>,
    ) -> Option<Rc<dyn Any>> {
        // Attach probes reporting the compute frontier.
        // The `apply_refresh` operator can round up frontiers, making it impossible to accurately
        // track the progress of the computation, so we need to attach probes before it.
        let probe = probe::Handle::default();
        ok_collection = ok_collection
            .probe_with(&probe)
            .inner
            .probe_notify_with(vec![output_probe.clone()])
            .as_collection();
        let collection_state = compute_state.expect_collection_mut(sink_id);
        collection_state.compute_probe = Some(probe);

        // If a `RefreshSchedule` was specified, round up timestamps.
        if let Some(refresh_schedule) = &sink.refresh_schedule {
            ok_collection = apply_refresh(ok_collection, refresh_schedule.clone());
            err_collection = apply_refresh(err_collection, refresh_schedule.clone());
        }

        if sink.up_to != Antichain::default() {
            unimplemented!(
                "UP TO is not supported for persist sinks yet, and shouldn't have been accepted during parsing/planning"
            )
        }

        let read_only_rx = collection_state.read_only_rx.clone();

        let token = persist_sink(
            sink_id,
            &self.storage_metadata,
            ok_collection,
            err_collection,
            as_of,
            compute_state,
            start_signal,
            read_only_rx,
        );
        Some(token)
    }
}

/// Type of the `desired` stream, split into `Ok` and `Err` streams.
type DesiredStreams<S> =
    OkErr<StreamVec<S, (Row, Timestamp, Diff)>, StreamVec<S, (DataflowError, Timestamp, Diff)>>;

/// Type of the `persist` stream, split into `Ok` and `Err` streams.
type PersistStreams<S> =
    OkErr<StreamVec<S, (Row, Timestamp, Diff)>, StreamVec<S, (DataflowError, Timestamp, Diff)>>;

/// Type of the `descs` stream.
type DescsStream<S> = StreamVec<S, BatchDescription>;

/// Type of the `batches` stream.
type BatchesStream<S> = StreamVec<S, (BatchDescription, ProtoBatch)>;

/// Type of the shared sink write frontier.
type SharedSinkFrontier = Rc<RefCell<Antichain<Timestamp>>>;

/// Renders an MV sink writing the given desired collection into the `target` persist collection.
pub(super) fn persist_sink<S>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    ok_collection: VecCollection<S, Row, Diff>,
    err_collection: VecCollection<S, DataflowError, Diff>,
    as_of: Antichain<Timestamp>,
    compute_state: &mut ComputeState,
    start_signal: StartSignal,
    read_only_rx: watch::Receiver<bool>,
) -> Rc<dyn Any>
where
    S: Scope<Timestamp = Timestamp>,
{
    let mut scope = ok_collection.scope();
    let desired = OkErr::new(ok_collection.inner, err_collection.inner);

    // Read back the persist shard.
    let (persist, persist_token) = persist_source(
        &mut scope,
        sink_id,
        target.clone(),
        compute_state,
        start_signal,
    );

    let persist_api = PersistApi {
        persist_clients: Arc::clone(&compute_state.persist_clients),
        collection: target.clone(),
        shard_name: sink_id.to_string(),
        purpose: format!("MV sink {sink_id}"),
    };

    let (desired, descs, sink_frontier) = mint::render(
        sink_id,
        persist_api.clone(),
        as_of.clone(),
        read_only_rx,
        desired,
    );

    let descs = descs.broadcast();

    let batches = write::render(
        sink_id,
        persist_api.clone(),
        as_of,
        desired,
        persist,
        descs.clone(),
        Rc::clone(&compute_state.worker_config),
    );

    append::render(sink_id, persist_api, descs, batches);

    // Report sink frontier updates to the `ComputeState`.
    let collection = compute_state.expect_collection_mut(sink_id);
    collection.sink_write_frontier = Some(sink_frontier);

    Rc::new(persist_token)
}

/// Generic wrapper around ok/err pairs (e.g. streams, frontiers), to simplify code dealing with
/// such pairs.
struct OkErr<O, E> {
    ok: O,
    err: E,
}

impl<O, E> OkErr<O, E> {
    fn new(ok: O, err: E) -> Self {
        Self { ok, err }
    }
}

impl OkErr<Antichain<Timestamp>, Antichain<Timestamp>> {
    fn new_frontiers() -> Self {
        Self {
            ok: Antichain::from_elem(Timestamp::MIN),
            err: Antichain::from_elem(Timestamp::MIN),
        }
    }

    /// Return the overall frontier, i.e., the minimum of `ok` and `err`.
    fn frontier(&self) -> &Antichain<Timestamp> {
        if PartialOrder::less_equal(&self.ok, &self.err) {
            &self.ok
        } else {
            &self.err
        }
    }
}

/// Advance the given `frontier` to `new`, if the latter one is greater.
///
/// Returns whether `frontier` was advanced.
fn advance(frontier: &mut Antichain<Timestamp>, new: AntichainRef<'_, Timestamp>) -> bool {
    if PartialOrder::less_than(&frontier.borrow(), &new) {
        frontier.clear();
        frontier.extend(new.iter().cloned());
        true
    } else {
        false
    }
}

/// A persist API specialized to a single collection.
#[derive(Clone)]
struct PersistApi {
    persist_clients: Arc<PersistClientCache>,
    collection: CollectionMetadata,
    shard_name: String,
    purpose: String,
}

impl PersistApi {
    async fn open_client(&self) -> PersistClient {
        self.persist_clients
            .open(self.collection.persist_location.clone())
            .await
            .unwrap_or_else(|error| panic!("error opening persist client: {error}"))
    }

    async fn open_writer(&self) -> WriteHandle<SourceData, (), Timestamp, StorageDiff> {
        self.open_client()
            .await
            .open_writer(
                self.collection.data_shard,
                Arc::new(self.collection.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: self.shard_name.clone(),
                    handle_purpose: self.purpose.clone(),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("error opening persist writer: {error}"))
    }
}

/// Instantiate a persist source reading back the `target` collection.
fn persist_source<S>(
    scope: &mut S,
    sink_id: GlobalId,
    target: CollectionMetadata,
    compute_state: &ComputeState,
    start_signal: StartSignal,
) -> (PersistStreams<S>, Vec<PressOnDropButton>)
where
    S: Scope<Timestamp = Timestamp>,
{
    // There is no guarantee that the sink as-of is beyond the persist shard's since. If it isn't,
    // instantiating a `persist_source` with it would panic. So instead we leave it to
    // `persist_source` to select an appropriate as-of. We only care about times beyond the current
    // shard upper anyway.
    //
    // TODO(teskje): Ideally we would select the as-of as `join(sink_as_of, since, upper)`, to
    // allow `persist_source` to omit as much historical detail as possible. However, we don't know
    // the shard frontiers and we cannot get them here as that requires an `async` context. We
    // should consider extending the `persist_source` API to allow as-of selection based on the
    // shard's current frontiers.
    let as_of = None;

    let until = Antichain::new();
    let map_filter_project = None;

    let (ok_stream, err_stream, token) = mz_storage_operators::persist_source::persist_source(
        scope,
        sink_id,
        Arc::clone(&compute_state.persist_clients),
        &compute_state.txns_ctx,
        target,
        None,
        as_of,
        SnapshotMode::Include,
        until,
        map_filter_project,
        compute_state.dataflow_max_inflight_bytes(),
        start_signal,
        ErrorHandler::Halt("compute persist sink"),
    );

    let streams = OkErr::new(ok_stream, err_stream);
    (streams, token)
}

/// A description for a batch of updates to be written.
///
/// Batch descriptions are produced by the `mint` operator and consumed by the `write` and `append`
/// operators, where they inform which batches should be written or appended, respectively.
///
/// Each batch description also contains the index of its "append worker", i.e. the worker that is
/// responsible for appending the written batches to the output shard.
#[derive(Clone, Serialize, Deserialize)]
struct BatchDescription {
    lower: Antichain<Timestamp>,
    upper: Antichain<Timestamp>,
    append_worker: usize,
}

impl BatchDescription {
    fn new(lower: Antichain<Timestamp>, upper: Antichain<Timestamp>, append_worker: usize) -> Self {
        assert!(PartialOrder::less_than(&lower, &upper));
        Self {
            lower,
            upper,
            append_worker,
        }
    }
}

impl std::fmt::Debug for BatchDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({:?}, {:?})@{}",
            self.lower.elements(),
            self.upper.elements(),
            self.append_worker,
        )
    }
}

/// Construct a name for the given sub-operator.
fn operator_name(sink_id: GlobalId, sub_operator: &str) -> String {
    format!("mv_sink({sink_id})::{sub_operator}")
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
    ///  * `read_only_tx`: A receiver that reports the sink is in read-only mode.
    ///  * `desired`: The ok/err streams that should be sinked to persist.
    pub fn render<S>(
        sink_id: GlobalId,
        persist_api: PersistApi,
        as_of: Antichain<Timestamp>,
        mut read_only_rx: watch::Receiver<bool>,
        desired: DesiredStreams<S>,
    ) -> (DesiredStreams<S>, DescsStream<S>, SharedSinkFrontier)
    where
        S: Scope<Timestamp = Timestamp>,
    {
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
            // The task sends the empty frontier as its final message before exiting. The
            // operator drops `persist_rx` once it receives the empty frontier.
            let (persist_tx, persist_rx) = mpsc::unbounded_channel();
            let sync_activator = scope.sync_activator_for(info.address.to_vec());
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
                let sync_activator = scope.sync_activator_for(info.address.to_vec());
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
            // work needlessly writing batches at previous times.
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
                last_lower = ?self.last_lower.as_ref().map(|f| f.elements()),
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
        /// The task sends the empty frontier as its final message before exiting. Once
        /// received, we drop the receiver.
        fn drain_persist_rx(&mut self, shared_frontier: &RefCell<Antichain<Timestamp>>) {
            let Some(mut rx) = self.persist_rx.take() else {
                return;
            };
            loop {
                match rx.try_recv() {
                    Ok(frontier) => {
                        shared_frontier.borrow_mut().clone_from(&frontier);
                        let done = frontier.is_empty();
                        self.advance_persist_frontier(frontier.borrow());
                        if done {
                            return;
                        }
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        self.persist_rx = Some(rx);
                        return;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        panic!("mint persist_watch task unexpectedly gone");
                    }
                }
            }
        }

        fn allow_writes(&mut self) {
            if self.read_only {
                self.read_only = false;
                self.trace("disabled read-only mode");
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

    use mz_timely_util::activator::ArcActivator;

    /// Commands sent from the Timely operator to the Tokio write task.
    enum WriteCommand {
        /// Forward desired updates to the corrections buffer.
        Desired(Result<Vec<(Row, Timestamp, Diff)>, Vec<(DataflowError, Timestamp, Diff)>>),
        /// Forward persist updates (negated) to the corrections buffer.
        Persist(Result<Vec<(Row, Timestamp, Diff)>, Vec<(DataflowError, Timestamp, Diff)>>),
        /// The persist frontier advanced. Used for `advance_since` and consolidation.
        PersistFrontier(Antichain<Timestamp>),
        /// Force a consolidation of the corrections buffer.
        ForceConsolidation,
        /// Write a batch with the given description. The task drains corrections and writes
        /// them to persist.
        WriteBatch(BatchDescription),
    }

    /// A response from the Tokio write task back to the Timely operator.
    struct WriteResponse {
        /// The written batch, or `None` if the corrections buffer had no updates.
        batch: Option<ProtoBatch>,
    }

    /// Render the `write` operator.
    ///
    /// The parameters passed in are:
    ///  * `sink_id`: The `GlobalId` of the sink export.
    ///  * `persist_api`: An object providing access to the output persist shard.
    ///  * `as_of`: The first time for which the sink may produce output.
    ///  * `desired`: The ok/err streams that should be sinked to persist.
    ///  * `persist`: The ok/err streams read back from the output persist shard.
    ///  * `descs`: The stream of batch descriptions produced by the `mint` operator (must be broadcast).
    pub fn render<S>(
        sink_id: GlobalId,
        persist_api: PersistApi,
        as_of: Antichain<Timestamp>,
        desired: DesiredStreams<S>,
        persist: PersistStreams<S>,
        descs: DescsStream<S>,
        worker_config: Rc<ConfigSet>,
    ) -> BatchesStream<S>
    where
        S: Scope<Timestamp = Timestamp>,
    {
        let scope = desired.ok.scope();
        let worker_id = scope.index();

        let name = operator_name(sink_id, "write");
        let mut builder = OperatorBuilderRc::new(name, scope.clone());
        let info = builder.operator_info();

        // Set up correction buffer logging. CorrectionLogger is not Send (uses timely
        // loggers), so the Tokio task uses ChannelLogging to send events back to the
        // Timely thread for application by the CorrectionLogger.
        let mut channel_logging = None;
        let mut correction_logger = None;
        if let (Some(compute_logger), Some(differential_logger)) = (
            scope.logger_for("materialize/compute"),
            scope.logger_for("differential/arrange"),
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
        let exchange_err = |(d, _, _): &(DataflowError, Timestamp, Diff)| d.hashed();

        // Disconnected inputs: no connection to any output.
        let mut desired_ok_input = builder.new_input(desired.ok, Exchange::new(exchange_ok));
        let mut desired_err_input = builder.new_input(desired.err, Exchange::new(exchange_err));
        let mut persist_ok_input = builder.new_input(persist.ok, Exchange::new(exchange_ok));
        let mut persist_err_input = builder.new_input(persist.err, Exchange::new(exchange_err));

        // descs -> output 0 (batches)
        let mut descs_input = builder.new_input(descs, Pipeline);

        let (batches_output, batches_output_stream) =
            builder.new_output_connection([(4, Antichain::from_elem(Default::default()))]);
        let mut batches_output = OutputBuilder::from(batches_output);

        // Obtain SinkMetrics synchronously from the persist client cache.
        let sink_metrics = persist_api.persist_clients.metrics().sink.clone();

        // Construct corrections on the Timely thread (reads ConfigSet), then move to the
        // Tokio task. The ChannelLogging sends events back to the Timely thread.
        let worker_metrics = sink_metrics.for_worker(worker_id);
        let mut corrections: OkErr<Correction<Row>, Correction<DataflowError>> = OkErr::new(
            Correction::new(
                sink_metrics.clone(),
                worker_metrics.clone(),
                channel_logging.clone(),
                &worker_config,
            ),
            Correction::new(
                sink_metrics.clone(),
                worker_metrics,
                channel_logging,
                &worker_config,
            ),
        );

        // Channels for commands and responses.
        let (cmd_tx, mut cmd_rx) = mpsc::unbounded_channel::<WriteCommand>();
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel::<WriteResponse>();

        // Spawn Tokio task that owns the WriteHandle and corrections buffer.
        let (activator, activation_ack) = ArcActivator::new(&scope, &info);
        let write_task_handle = {
            mz_ore::task::spawn(
                || operator_name(sink_id, "write::batch_writer"),
                async move {
                    let mut writer = persist_api.open_writer().await;

                    while let Some(cmd) = cmd_rx.recv().await {
                        apply_command(&mut corrections, &mut writer, cmd, &resp_tx).await;
                        // Activate the operator to drain logging events and process batch responses.
                        // ArcActivator suppresses redundant activations, so this is cheap.
                        activator.activate();
                    }
                },
            )
            .abort_on_drop()
        };

        builder.build(move |capabilities| {
            // We will use the data capabilities from the `descs` input to produce output, so no
            // need to hold onto the initial capabilities.
            drop(capabilities);

            let mut state = State::new(sink_id, worker_id, as_of);

            // Whether a batch write is currently in flight in the Tokio task.
            let mut batch_in_flight: Option<(BatchDescription, Capability<Timestamp>)> = None;

            // CorrectionLogger lives on the Timely thread and drains events from
            // the channel each activation. On drop, it drains remaining events and
            // retracts all logged state.
            let mut correction_logger = correction_logger;

            move |frontiers| {
                // Keep task handle alive so it is aborted when the operator is dropped.
                let _ = &write_task_handle;

                // Acknowledge activation so the Tokio task can activate us again.
                activation_ack.ack();

                // Drain logging events from the Tokio task's ChannelLogging.
                if let Some(logger) = &mut correction_logger {
                    logger.apply_events();
                }
                // Drain all inputs and forward data to the Tokio task.
                desired_ok_input.for_each(|_cap, data| {
                    cmd_tx
                        .send(WriteCommand::Desired(Ok(std::mem::take(data))))
                        .expect("write task unexpectedly gone");
                });
                desired_err_input.for_each(|_cap, data| {
                    cmd_tx
                        .send(WriteCommand::Desired(Err(std::mem::take(data))))
                        .expect("write task unexpectedly gone");
                });
                persist_ok_input.for_each(|_cap, data| {
                    cmd_tx
                        .send(WriteCommand::Persist(Ok(std::mem::take(data))))
                        .expect("write task unexpectedly gone");
                });
                persist_err_input.for_each(|_cap, data| {
                    cmd_tx
                        .send(WriteCommand::Persist(Err(std::mem::take(data))))
                        .expect("write task unexpectedly gone");
                });

                // Accept batch descriptions.
                descs_input.for_each(|cap, data| {
                    let cap = cap.retain(0);
                    for desc in data.drain(..) {
                        state.absorb_batch_description(desc, cap.clone());
                    }
                });

                // Track frontiers. Send persist frontier to Tokio task for advance_since.
                state.advance_desired_ok_frontier(frontiers[0].frontier());
                state.advance_desired_err_frontier(frontiers[1].frontier());
                if state.advance_persist_ok_frontier(frontiers[2].frontier())
                    | state.advance_persist_err_frontier(frontiers[3].frontier())
                {
                    cmd_tx
                        .send(WriteCommand::PersistFrontier(
                            state.persist_frontiers.frontier().to_owned(),
                        ))
                        .expect("write task unexpectedly gone");

                    // Check if we need to force consolidation.
                    if state.should_force_consolidation() {
                        cmd_tx
                            .send(WriteCommand::ForceConsolidation)
                            .expect("write task unexpectedly gone");
                    }
                }
                // Desired frontier advances can also trigger consolidation.
                if state.should_force_consolidation() {
                    cmd_tx
                        .send(WriteCommand::ForceConsolidation)
                        .expect("write task unexpectedly gone");
                }

                // Try to receive batch results from the Tokio task.
                loop {
                    match resp_rx.try_recv() {
                        Ok(resp) => {
                            if let Some((desc, cap)) = batch_in_flight.take() {
                                if let Some(batch) = resp.batch {
                                    let mut out = batches_output.activate();
                                    out.session(&cap).give((desc, batch));
                                    state.trace("wrote a batch");
                                } else {
                                    state.trace("skipping empty batch");
                                }
                            }
                        }
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            panic!("write task unexpectedly gone");
                        }
                    }
                }

                // If no batch in flight, try to write a new batch.
                if batch_in_flight.is_none() {
                    if let Some((desc, cap)) = state.maybe_start_batch(&cmd_tx) {
                        batch_in_flight = Some((desc, cap));
                    }
                }
            }
        });

        batches_output_stream
    }

    /// Apply a single command to the task state.
    async fn apply_command(
        corrections: &mut OkErr<Correction<Row>, Correction<DataflowError>>,
        writer: &mut WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        cmd: WriteCommand,
        resp_tx: &mpsc::UnboundedSender<WriteResponse>,
    ) {
        match cmd {
            WriteCommand::Desired(Ok(mut updates)) => {
                corrections.ok.insert(&mut updates);
            }
            WriteCommand::Desired(Err(mut updates)) => {
                corrections.err.insert(&mut updates);
            }
            WriteCommand::Persist(Ok(mut updates)) => {
                corrections.ok.insert_negated(&mut updates);
            }
            WriteCommand::Persist(Err(mut updates)) => {
                corrections.err.insert_negated(&mut updates);
            }
            WriteCommand::PersistFrontier(frontier) => {
                corrections.ok.advance_since(frontier.clone());
                corrections.err.advance_since(frontier);
            }
            WriteCommand::ForceConsolidation => {
                corrections.ok.consolidate_at_since();
                corrections.err.consolidate_at_since();
            }
            WriteCommand::WriteBatch(desc) => {
                // Chain ok and err correction iterators directly, avoiding an
                // intermediate Vec allocation.
                let oks = corrections
                    .ok
                    .updates_before(&desc.upper)
                    .map(|(d, t, r)| ((SourceData(Ok(d)), ()), t, r.into_inner()));
                let errs = corrections
                    .err
                    .updates_before(&desc.upper)
                    .map(|(d, t, r)| ((SourceData(Err(d)), ()), t, r.into_inner()));
                let mut updates = oks.chain(errs).peekable();

                if updates.peek().is_none() {
                    // No corrections to write.
                    let _ = resp_tx.send(WriteResponse { batch: None });
                    return;
                }

                let batch = writer
                    .batch(updates, desc.lower, desc.upper)
                    .await
                    .expect("valid usage");
                let proto_batch = batch.into_transmittable_batch();
                if let Err(err) = resp_tx.send(WriteResponse {
                    batch: Some(proto_batch),
                }) {
                    let batch =
                        writer.batch_from_transmittable_batch(err.0.batch.expect("just sent"));
                    batch.delete().await;
                }
            }
        }
    }

    /// State maintained by the `write` operator on the Timely thread.
    ///
    /// The corrections buffer and batch writing logic live on the Tokio task. This state
    /// only tracks what's needed for frontier management and batch description handling.
    struct State {
        sink_id: GlobalId,
        worker_id: usize,
        /// The frontiers of the `desired` inputs.
        desired_frontiers: OkErr<Antichain<Timestamp>, Antichain<Timestamp>>,
        /// The frontiers of the `persist` inputs.
        persist_frontiers: OkErr<Antichain<Timestamp>, Antichain<Timestamp>>,
        /// The current valid batch description and associated output capability, if any.
        batch_description: Option<(BatchDescription, Capability<Timestamp>)>,
        /// A request to force a consolidation of corrections once both `desired_frontiers` and
        /// `persist_frontiers` become greater than the given frontier.
        force_consolidation_after: Option<Antichain<Timestamp>>,
    }

    impl State {
        fn new(sink_id: GlobalId, worker_id: usize, as_of: Antichain<Timestamp>) -> Self {
            // Force a consolidation of corrections after the snapshot updates have been fully
            // processed, to ensure we get rid of those as quickly as possible.
            let force_consolidation_after = Some(as_of);

            Self {
                sink_id,
                worker_id,
                desired_frontiers: OkErr::new_frontiers(),
                persist_frontiers: OkErr::new_frontiers(),
                batch_description: None,
                force_consolidation_after,
            }
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

        /// Returns true if the persist frontier advanced.
        fn advance_persist_ok_frontier(&mut self, frontier: AntichainRef<Timestamp>) -> bool {
            if advance(&mut self.persist_frontiers.ok, frontier) {
                self.trace("advanced `persist` ok frontier");
                true
            } else {
                false
            }
        }

        /// Returns true if the persist frontier advanced.
        fn advance_persist_err_frontier(&mut self, frontier: AntichainRef<Timestamp>) -> bool {
            if advance(&mut self.persist_frontiers.err, frontier) {
                self.trace("advanced `persist` err frontier");
                true
            } else {
                false
            }
        }

        /// Check if a forced consolidation should be triggered.
        ///
        /// Returns true (once) when both desired and persist frontiers have advanced past the
        /// consolidation request frontier.
        fn should_force_consolidation(&mut self) -> bool {
            let Some(request) = &self.force_consolidation_after else {
                return false;
            };

            let desired_frontier = self.desired_frontiers.frontier();
            let persist_frontier = self.persist_frontiers.frontier();
            if PartialOrder::less_than(request, desired_frontier)
                && PartialOrder::less_than(request, persist_frontier)
            {
                self.trace("requesting correction consolidation");
                self.force_consolidation_after = None;
                true
            } else {
                false
            }
        }

        fn absorb_batch_description(&mut self, desc: BatchDescription, cap: Capability<Timestamp>) {
            // The incoming batch description is outdated if we already have a batch description
            // with a greater `lower`.
            //
            // Note that we cannot assume a description is outdated based on the comparison of its
            // `lower` with the `persist_frontier`. The persist frontier observed by the `write`
            // operator is initialized with the shard's read frontier, so it can be greater than
            // the shard's write frontier.
            if let Some((prev, _)) = &self.batch_description {
                if PartialOrder::less_than(&desc.lower, &prev.lower) {
                    self.trace(format!("skipping outdated batch description: {desc:?}"));
                    return;
                }
            }

            self.batch_description = Some((desc, cap));
            self.trace("set batch description");
        }

        /// Check if a batch can be written and send a write command to the Tokio task if so.
        ///
        /// Returns `(desc, cap)` if a batch write was initiated.
        fn maybe_start_batch(
            &mut self,
            cmd_tx: &mpsc::UnboundedSender<WriteCommand>,
        ) -> Option<(BatchDescription, Capability<Timestamp>)> {
            let (desc, _cap) = self.batch_description.as_ref()?;

            // We can write a new batch if we have seen all `persist` updates before `lower` and
            // all `desired` updates up to `upper`.
            let persist_complete =
                PartialOrder::less_equal(&desc.lower, self.persist_frontiers.frontier());
            let desired_complete =
                PartialOrder::less_equal(&desc.upper, self.desired_frontiers.frontier());
            if !persist_complete || !desired_complete {
                return None;
            }

            self.trace("write batch description");
            let (desc, cap) = self.batch_description.take()?;
            cmd_tx
                .send(WriteCommand::WriteBatch(desc.clone()))
                .expect("write task unexpectedly gone");
            Some((desc, cap))
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
    ///  * `descs`: The stream of batch descriptions produced by the `mint` operator (must be broadcast).
    ///  * `batches`: The stream of written batches produced by the `write` operator.
    pub fn render<S>(
        sink_id: GlobalId,
        persist_api: PersistApi,
        descs: DescsStream<S>,
        batches: BatchesStream<S>,
    ) where
        S: Scope<Timestamp = Timestamp>,
    {
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

                // Forward batches frontier advancements.
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
