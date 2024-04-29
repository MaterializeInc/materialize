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
//!  desired                      persist <---------------.
//!     |                            |                    |
//!     |             .--------------|                    |
//!     |----------- / ---------.    |                    |
//!     |    .______/           |    |                    |
//!     |    |                  |    |                    |
//!     v    v                  v    v                    |
//!   +--------+              +--------+              +--------+
//!   |  mint  | --descs-.--> | write  | --batches--> | append |
//!   +--------+          \   +--------+          .-> +--------+
//!                        \_____________________/
//!
//!  * `mint` mints batch descriptions, i.e., `(lower, upper)` bounds of batches that should be
//!    written. The persist API requires that all workers write batches with the same bounds, so
//!    they can be appended as a single logical batch. To ensure this, the `mint` operator only
//!    runs on a single worker that broadcasts minted descriptions to all workers. Batch bounds are
//!    picked based on the frontiers of the `desired` and `persist` stream, which `mint` receives
//!    as inputs.
//!  * `write` stages batch data in persist, based on the batch descriptions received from the
//!    `mint` operator, but without appending it to the persist shard. This is a multi-worker
//!    operator, with each worker writing batches of the data that arrives at its local inputs. To
//!    do so it reads from the `desired` and `persist` streams and produces the difference between
//!    them to write back out, ensuring that the final contents of the persist shard match
//!    `desired`.
//!  * `append` appends the batches minted by `mint` and written by `write` to the persist shard.
//!    This is again a single-worker operator. It waits for all workers to stage their batches for
//!    a given batch description, then appends all the batches together as a single logical batch.
//!
//! Note that while the above graph suggests that `mint` and `write` both receive copies of the
//! `desired` and `persist` streams, the actual implementation passes both streams through `mint`
//! and lets `write` read the passed-through streams, to avoid cloning data.
//!
//! The persist sink is written to be robust to the presence of other conflicting instances (e.g.
//! from other replicas) writing to the same persist shard. Each of the three operators needs to be
//! able to handle conflicting writes that unexpectedly change the contents of `persist`.
//!
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
//! has caught up.
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

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{Collection, Hashable};
use mz_ore::cast::CastFrom;
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::batch::{Batch, BatchBuilder};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::metrics::SinkMetrics;
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::{Event, OperatorBuilder};
use serde::{Deserialize, Serialize};
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::{Broadcast, Capability, CapabilitySet, InspectCore};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::PartialOrder;
use tracing::trace;

use crate::compute_state::ComputeState;
use crate::render::StartSignal;
use crate::sink::correction::Correction;

type DesiredStreams<S> =
    OkErr<Stream<S, (Row, Timestamp, Diff)>, Stream<S, (DataflowError, Timestamp, Diff)>>;
type PersistStreams<S> =
    OkErr<Stream<S, (Row, Timestamp, Diff)>, Stream<S, (DataflowError, Timestamp, Diff)>>;
type DescsStream<S> = Stream<S, BatchDescription>;
type BatchesStream<S> = Stream<S, BatchOrData>;

/// Renders a persist sink writing the given desired collection into the `target` persist
/// collection.
pub(super) fn persist_sink<S>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    ok_collection: Collection<S, Row, Diff>,
    err_collection: Collection<S, DataflowError, Diff>,
    as_of: Antichain<Timestamp>,
    compute_state: &mut ComputeState,
    start_signal: StartSignal,
) -> Rc<dyn Any>
where
    S: Scope<Timestamp = Timestamp>,
{
    let mut scope = ok_collection.scope();
    let desired = OkErr::new(ok_collection.inner, err_collection.inner);

    // Read back the persist shard.
    let (mut persist, persist_token) = persist_source(
        &mut scope,
        sink_id,
        target.clone(),
        compute_state,
        start_signal,
    );

    // Report sink frontier updates to the `ComputeState`.
    let sink_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::MIN)));
    let collection = compute_state.expect_collection_mut(sink_id);
    collection.sink_write_frontier = Some(Rc::clone(&sink_frontier));

    persist.ok = persist.ok.inspect_container(move |event| {
        if let Err(frontier) = event {
            let mut borrow = sink_frontier.borrow_mut();
            borrow.clear();
            borrow.extend(frontier.iter().copied());
        }
    });

    // Determine the active worker for single-worker operators.
    let active_worker_id = usize::cast_from(sink_id.hashed()) % scope.peers();

    let operator_name = |name| format!("persist_sink({sink_id})::{name}");
    let persist_api = |purpose| PersistApi {
        persist_clients: Arc::clone(&compute_state.persist_clients),
        collection: target.clone(),
        shard_name: sink_id.to_string(),
        purpose,
    };

    let name = operator_name("mint");
    let (desired, persist, descs, mint_token) = mint::render(
        name.clone(),
        persist_api(name),
        as_of,
        active_worker_id,
        &desired,
        &persist,
    );

    let name = operator_name("write");
    let (batches, write_token) =
        write::render(name.clone(), persist_api(name), &desired, &persist, &descs);

    let name = operator_name("append");
    let append_token = append::render(
        name.clone(),
        persist_api(name),
        active_worker_id,
        &descs,
        &batches,
    );

    Rc::new((persist_token, mint_token, write_token, append_token))
}

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

    fn frontier(&self) -> &Antichain<Timestamp> {
        if PartialOrder::less_equal(&self.ok, &self.err) {
            &self.ok
        } else {
            &self.err
        }
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
    fn config(&self) -> PersistConfig {
        self.persist_clients.cfg().clone()
    }

    async fn open_client(&self) -> PersistClient {
        self.persist_clients
            .open(self.collection.persist_location.clone())
            .await
            .unwrap_or_else(|error| panic!("error opening persist client: {error}"))
    }

    async fn open_handles(
        &self,
    ) -> (
        WriteHandle<SourceData, (), Timestamp, Diff>,
        ReadHandle<SourceData, (), Timestamp, Diff>,
    ) {
        self.open_client()
            .await
            .open(
                self.collection.data_shard,
                Arc::new(self.collection.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: self.shard_name.clone(),
                    handle_purpose: self.purpose.clone(),
                },
                false,
            )
            .await
            .unwrap_or_else(|error| panic!("error opening persist handles: {error}"))
    }

    async fn open_writer(&self) -> WriteHandle<SourceData, (), Timestamp, Diff> {
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

    async fn open_metrics(&self) -> SinkMetrics {
        let client = self.open_client().await;
        client.metrics().sink.clone()
    }
}

/// Instantiate a persist source reading back the `target` collection.
fn persist_source<S>(
    scope: &mut S,
    sink_id: GlobalId,
    target: CollectionMetadata,
    compute_state: &mut ComputeState,
    start_signal: StartSignal,
) -> (PersistStreams<S>, Box<dyn Any>)
where
    S: Scope<Timestamp = Timestamp>,
{
    // There is no guarantee that the sink as-of is beyond the persist shard's since. If it isn't,
    // instantiating a `persist_source` with it would panic. So instead we leave it to
    // `persist_source` to select an appropriate as-of. We only care about times beyond the current
    // shard upper anyway.
    //
    // TODO(teskje): Ideally we would select the as-of as `join(sink_as_of, since, upper)`, to
    // allow `persist_source` to omit as much historical detail as possible, and to simplify the
    // implementation of the `mint` operator. However, we don't know the shard frontiers and we
    // cannot get them here as that requires an `async` context. We should consider extending the
    // `persist_source` API to allow as-of selection based on the shard's current frontiers.
    let as_of = None;

    let until = Antichain::new();
    let map_filter_project = None;

    let (ok_stream, err_stream, token) = mz_storage_operators::persist_source::persist_source(
        scope,
        sink_id,
        Arc::clone(&compute_state.persist_clients),
        &compute_state.txns_ctx,
        &compute_state.worker_config,
        target,
        as_of,
        SnapshotMode::Include,
        until,
        map_filter_project,
        compute_state.dataflow_max_inflight_bytes(),
        start_signal,
        |error| panic!("compute_persist_sink: {error}"),
    );

    let streams = OkErr::new(ok_stream, err_stream);
    let token = Box::new(token);
    (streams, token)
}

#[derive(Clone, Serialize, Deserialize)]
struct BatchDescription {
    lower: Antichain<Timestamp>,
    upper: Antichain<Timestamp>,
}

impl BatchDescription {
    fn new(lower: Antichain<Timestamp>, upper: Antichain<Timestamp>) -> Self {
        debug_assert!(PartialOrder::less_than(&lower, &upper));
        Self { lower, upper }
    }
}

impl std::fmt::Debug for BatchDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({:?}, {:?})",
            self.lower.elements(),
            self.upper.elements()
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
enum BatchOrData {
    Batch(ProtoBatch),
    Data {
        lower: Antichain<Timestamp>,
        upper: Antichain<Timestamp>,
        contents: Vec<(Result<Row, DataflowError>, Timestamp, Diff)>,
    },
}

mod mint {
    use timely::container::CapacityContainerBuilder;

    use super::*;

    pub fn render<S>(
        name: String,
        persist_api: PersistApi,
        as_of: Antichain<Timestamp>,
        active_worker_id: usize,
        desired: &DesiredStreams<S>,
        persist: &PersistStreams<S>,
    ) -> (
        DesiredStreams<S>,
        PersistStreams<S>,
        DescsStream<S>,
        Box<dyn Any>,
    )
    where
        S: Scope<Timestamp = Timestamp>,
    {
        let scope = desired.ok.scope();
        let worker_id = scope.index();

        let mut op = OperatorBuilder::new(name, scope);

        let mut new_ok_err_outputs = || {
            let (ok_output, ok_stream) = op.new_output::<CapacityContainerBuilder<_>>();
            let (err_output, err_stream) = op.new_output::<CapacityContainerBuilder<_>>();
            (
                OkErr::new(ok_output, err_output),
                OkErr::new(ok_stream, err_stream),
            )
        };

        let (mut desired_outputs, desired_output_streams) = new_ok_err_outputs();
        let (mut persist_outputs, persist_output_streams) = new_ok_err_outputs();
        let (mut desc_output, desc_output_stream) = op.new_output();

        let mut desired_inputs = OkErr {
            ok: op.new_input_for(&desired.ok, Pipeline, &desired_outputs.ok),
            err: op.new_input_for(&desired.err, Pipeline, &desired_outputs.err),
        };
        let mut persist_inputs = OkErr {
            ok: op.new_input_for(&persist.ok, Pipeline, &persist_outputs.ok),
            err: op.new_input_for(&persist.err, Pipeline, &persist_outputs.err),
        };

        let button = op.build(move |capabilities| async move {
            // Passing through `desired` and `persist` data only requires data capabilities, so we
            // can immediately drop their initial capabilities here.
            let [_, _, _, _, desc_cap]: [_; 5] =
                capabilities.try_into().expect("one capability per output");

            // Non-active workers just pass the `desired` and `persist` data through.
            if worker_id != active_worker_id {
                drop(desc_cap);

                loop {
                    tokio::select! {
                        Some(event) = desired_inputs.ok.next() => {
                            if let Event::Data(cap, mut data) = event {
                                desired_outputs.ok.give_container(&cap, &mut data).await;
                            }
                        }
                        Some(event) = desired_inputs.err.next() => {
                            if let Event::Data(cap, mut data) = event {
                                desired_outputs.err.give_container(&cap, &mut data).await;
                            }
                        }
                        Some(event) = persist_inputs.ok.next() => {
                            if let Event::Data(cap, mut data) = event {
                                persist_outputs.ok.give_container(&cap, &mut data).await;
                            }
                        }
                        Some(event) = persist_inputs.err.next() => {
                            if let Event::Data(cap, mut data) = event {
                                persist_outputs.err.give_container(&cap, &mut data).await;
                            }
                        }
                        // All inputs are exhausted, so we can shut down.
                        else => return,
                    }
                }
            }

            let mut cap_set = CapabilitySet::from_elem(desc_cap);
            let mut state = State::new();

            state.prepare_target_shard(persist_api, as_of).await;

            loop {
                // Read from the inputs, pass through all data to the respective outputs, and keep
                // track of the input frontiers. When a frontier advances we might have to mint a
                // new batch description.
                let maybe_desc = tokio::select! {
                    Some(event) = desired_inputs.ok.next() => {
                        match event {
                            Event::Data(cap, mut data) => {
                                desired_outputs.ok.give_container(&cap, &mut data).await;
                                None
                            }
                            Event::Progress(frontier) => {
                                state.advance_desired_ok_frontier(frontier);
                                state.maybe_mint_batch_description()
                            }
                        }
                    }
                    Some(event) = desired_inputs.err.next() => {
                        match event {
                            Event::Data(cap, mut data) => {
                                desired_outputs.err.give_container(&cap, &mut data).await;
                                None
                            }
                            Event::Progress(frontier) => {
                                state.advance_desired_err_frontier(frontier);
                                state.maybe_mint_batch_description()
                            }
                        }
                    }
                    Some(event) = persist_inputs.ok.next() => {
                        match event {
                            Event::Data(cap, mut data) => {
                                persist_outputs.ok.give_container(&cap, &mut data).await;
                                None
                            }
                            Event::Progress(frontier) => {
                                state.advance_persist_ok_frontier(frontier);
                                state.maybe_mint_batch_description()
                            }
                        }
                    }
                    Some(event) = persist_inputs.err.next() => {
                        match event {
                            Event::Data(cap, mut data) => {
                                persist_outputs.err.give_container(&cap, &mut data).await;
                                None
                            }
                            Event::Progress(frontier) => {
                                state.advance_persist_err_frontier(frontier);
                                state.maybe_mint_batch_description()
                            }
                        }
                    }
                    // All inputs are exhausted, so we can shut down.
                    else => return,
                };

                if let Some(desc) = maybe_desc {
                    let lower_ts = *desc.lower.as_option().expect("not empty");
                    let cap = cap_set.delayed(&lower_ts);
                    desc_output.give(&cap, desc).await;

                    // We only emit strictly increasing `lower`s, so we can let our output frontier
                    // advance beyond the current `lower`.
                    cap_set.downgrade([lower_ts.step_forward()]);
                } else {
                    // The next emitted `lower` will be at least the `persist` frontier, so we can
                    // advance our output frontier as far.
                    let _ = cap_set.try_downgrade(state.persist_frontiers.frontier().iter());
                }
            }
        });

        let token = Box::new(button.press_on_drop());

        (
            desired_output_streams,
            persist_output_streams,
            desc_output_stream,
            token,
        )
    }

    struct State {
        /// The frontiers of the `desired` inputs.
        desired_frontiers: OkErr<Antichain<Timestamp>, Antichain<Timestamp>>,
        /// The frontiers of the `persist` inputs.
        persist_frontiers: OkErr<Antichain<Timestamp>, Antichain<Timestamp>>,
        /// The last `lower` we have emitted in a batch description, if any. Whenever the
        /// `persist_frontier` moves beyond this frontier, we need to mint a new description.
        last_lower: Option<Antichain<Timestamp>>,

        initial_read_hold: Option<ReadHandle<SourceData, (), Timestamp, Diff>>,
    }

    impl State {
        fn new() -> Self {
            Self {
                desired_frontiers: OkErr::new_frontiers(),
                persist_frontiers: OkErr::new_frontiers(),
                last_lower: None,
                initial_read_hold: None,
            }
        }

        fn trace<S: AsRef<str>>(&self, message: S) {
            let message = message.as_ref();
            trace!(
                desired_frontier = ?self.desired_frontiers.frontier().elements(),
                persist_frontier = ?self.persist_frontiers.frontier().elements(),
                last_lower = ?self.last_lower.as_ref().map(|f| f.elements()),
                message,
            );
        }

        async fn prepare_target_shard(
            &mut self,
            persist_api: PersistApi,
            as_of: Antichain<Timestamp>,
        ) {
            let (mut writer, mut reader) = persist_api.open_handles().await;

            let mut upper = as_of;
            upper.join_assign(reader.since());

            advance_shard_upper(&mut writer, upper.clone()).await;

            writer.expire().await;

            reader.downgrade_since(&upper).await;
            self.initial_read_hold = Some(reader);

            self.persist_frontiers.ok.clone_from(&upper);
            self.persist_frontiers.err = upper;
        }

        fn advance_desired_ok_frontier(&mut self, frontier: Antichain<Timestamp>) {
            if PartialOrder::less_than(&self.desired_frontiers.ok, &frontier) {
                self.desired_frontiers.ok = frontier;
                self.trace("advanced `desired` ok frontier");
            }
        }

        fn advance_desired_err_frontier(&mut self, frontier: Antichain<Timestamp>) {
            if PartialOrder::less_than(&self.desired_frontiers.err, &frontier) {
                self.desired_frontiers.err = frontier;
                self.trace("advanced `desired` err frontier");
            }
        }

        fn advance_persist_ok_frontier(&mut self, frontier: Antichain<Timestamp>) {
            if PartialOrder::less_than(&self.persist_frontiers.ok, &frontier) {
                self.persist_frontiers.ok = frontier;
                self.apply_persist_frontier_advancement();
                self.trace("advanced `persist` ok frontier");
            }
        }

        fn advance_persist_err_frontier(&mut self, frontier: Antichain<Timestamp>) {
            if PartialOrder::less_than(&self.persist_frontiers.err, &frontier) {
                self.persist_frontiers.err = frontier;
                self.apply_persist_frontier_advancement();
                self.trace("advanced `persist` err frontier");
            }
        }

        /// Apply the effects of a previous `persist` frontier advancement.
        fn apply_persist_frontier_advancement(&mut self) {
            let frontier = self.persist_frontiers.frontier();

            // `persist` frontier advancements beyond `initial_read_hold` are driven by the
            // `persist_source`, so we can know when the `persist_source` has installed its own
            // read hold and we can drop our initial one.
            if let Some(hold) = &self.initial_read_hold {
                if PartialOrder::less_than(hold.since(), frontier) {
                    self.initial_read_hold = None;
                }
            }
        }

        fn maybe_mint_batch_description(&mut self) -> Option<BatchDescription> {
            let desired_frontier = self.desired_frontiers.frontier();
            let persist_frontier = self.persist_frontiers.frontier();

            // We only mint new batch descriptions when:
            //  1. The `desired` frontier is ahead of the `persist` frontier.
            //  2. The `persist` frontier advanced since we last emitted a batch description.
            let desired_ahead = PartialOrder::less_than(persist_frontier, desired_frontier);
            let persist_advanced = self.last_lower.as_ref().map_or(true, |lower| {
                PartialOrder::less_than(lower, persist_frontier)
            });

            if !desired_ahead || !persist_advanced {
                return None;
            }

            let lower = persist_frontier.clone();
            let upper = desired_frontier.clone();
            let desc = BatchDescription::new(lower, upper);

            self.last_lower = Some(desc.lower.clone());

            self.trace(format!("minted batch description: {desc:?}"));
            Some(desc)
        }
    }

    async fn advance_shard_upper(
        persist_writer: &mut WriteHandle<SourceData, (), Timestamp, Diff>,
        upper: Antichain<Timestamp>,
    ) {
        let empty_updates: &[((SourceData, ()), Timestamp, Diff)] = &[];
        let lower = Antichain::from_elem(Timestamp::MIN);
        persist_writer
            .append(empty_updates, lower, upper)
            .await
            .expect("valid usage")
            .expect("should always succeed");
    }
}

mod write {
    use super::*;

    pub fn render<S>(
        name: String,
        persist_api: PersistApi,
        desired: &DesiredStreams<S>,
        persist: &PersistStreams<S>,
        descs: &Stream<S, BatchDescription>,
    ) -> (BatchesStream<S>, Box<dyn Any>)
    where
        S: Scope<Timestamp = Timestamp>,
    {
        let scope = desired.ok.scope();
        let worker_id = scope.index();

        let mut op = OperatorBuilder::new(name, scope);

        let (mut batches_output, batches_output_stream) = op.new_output();

        // It is important that we exchange the `desired` and `persist` data the same way, so
        // updates that cancel each other out end up on the same worker.
        let exchange_ok = |(d, _, _): &(Row, Timestamp, Diff)| d.hashed();
        let exchange_err = |(d, _, _): &(DataflowError, Timestamp, Diff)| d.hashed();

        let mut desired_inputs = OkErr::new(
            op.new_disconnected_input(&desired.ok, Exchange::new(exchange_ok)),
            op.new_disconnected_input(&desired.err, Exchange::new(exchange_err)),
        );
        let mut persist_inputs = OkErr::new(
            op.new_disconnected_input(&persist.ok, Exchange::new(exchange_ok)),
            op.new_disconnected_input(&persist.err, Exchange::new(exchange_err)),
        );
        let mut descs_input = op.new_input_for(&descs.broadcast(), Pipeline, &batches_output);

        let button = op.build(move |capabilities| async move {
            // We will use the data capabilities from the `descs` input to produce output, so no
            // need to hold onto the initial capabilities.
            drop(capabilities);

            let persist_writer = persist_api.open_writer().await;
            let sink_metrics = persist_api.open_metrics().await;
            let mut state = State::new(
                worker_id,
                persist_api.config(),
                persist_writer,
                sink_metrics,
            );

            loop {
                // Read from the inputs, extract `desired` updates as positive contributions to
                // `correction` and `persist` updates as negative contributions. If either the
                // `desired` or `persist` frontier advances, or if we receive a new batch description,
                // we might have to write a new batch.
                let maybe_batch = tokio::select! {
                    Some(event) = desired_inputs.ok.next() => {
                        match event {
                            Event::Data(_cap, data) => {
                                state.corrections.ok.insert(data);
                                None
                            }
                            Event::Progress(frontier) => {
                                state.advance_desired_ok_frontier(frontier);
                                state.maybe_write_batch().await
                            }
                        }
                    }
                    Some(event) = desired_inputs.err.next() => {
                        match event {
                            Event::Data(_cap, data) => {
                                state.corrections.err.insert(data);
                                None
                            }
                            Event::Progress(frontier) => {
                                state.advance_desired_err_frontier(frontier);
                                state.maybe_write_batch().await
                            }
                        }
                    }
                    Some(event) = persist_inputs.ok.next() => {
                        match event {
                            Event::Data(_cap, data) => {
                                state.corrections.ok.insert_negated(data);
                                None
                            }
                            Event::Progress(frontier) => {
                                state.advance_persist_ok_frontier(frontier);
                                state.maybe_write_batch().await
                            }
                        }
                    }
                    Some(event) = persist_inputs.err.next() => {
                        match event {
                            Event::Data(_cap, data) => {
                                state.corrections.err.insert_negated(data);
                                None
                            }
                            Event::Progress(frontier) => {
                                state.advance_persist_err_frontier(frontier);
                                state.maybe_write_batch().await
                            }
                        }
                    }
                    Some(event) = descs_input.next() => {
                        match event {
                            Event::Data(cap, data) => {
                                for desc in data {
                                    state.absorb_batch_description(desc, cap.clone());
                                }
                                state.maybe_write_batch().await
                            }
                            Event::Progress(_frontier) => None,
                        }
                    }
                    // All inputs are exhausted, so we can shut down.
                    else => return,
                };

                if let Some((batch, cap)) = maybe_batch {
                    batches_output.give(&cap, batch).await;
                }
            }
        });

        let token = Box::new(button.press_on_drop());

        (batches_output_stream, token)
    }

    struct State {
        worker_id: usize,
        persist_config: PersistConfig,
        persist_writer: WriteHandle<SourceData, (), Timestamp, Diff>,
        /// Contains `desired - persist`, reflecting the updates we would like to commit to
        /// `persist` in order to "correct" it to track `desired`. This collection is only modified
        /// by updates received from either the `desired` or `persist` inputs.
        corrections: OkErr<Correction<Row>, Correction<DataflowError>>,
        /// The frontiers of the `desired` inputs.
        desired_frontiers: OkErr<Antichain<Timestamp>, Antichain<Timestamp>>,
        /// The frontiers of the `persist` inputs.
        persist_frontiers: OkErr<Antichain<Timestamp>, Antichain<Timestamp>>,
        /// The current valid batch description and associated output capability, if any.
        batch_description: Option<(BatchDescription, Capability<Timestamp>)>,
    }

    impl State {
        fn new(
            worker_id: usize,
            persist_config: PersistConfig,
            persist_writer: WriteHandle<SourceData, (), Timestamp, Diff>,
            metrics: SinkMetrics,
        ) -> Self {
            let worker_metrics = metrics.for_worker(worker_id);

            Self {
                worker_id,
                persist_config,
                persist_writer,
                corrections: OkErr::new(
                    Correction::new(metrics.clone(), worker_metrics.clone()),
                    Correction::new(metrics, worker_metrics),
                ),
                desired_frontiers: OkErr::new_frontiers(),
                persist_frontiers: OkErr::new_frontiers(),
                batch_description: None,
            }
        }

        fn trace<S: AsRef<str>>(&self, message: S) {
            let message = message.as_ref();
            trace!(
                worker = %self.worker_id,
                desired_frontier = ?self.desired_frontiers.frontier().elements(),
                persist_frontier = ?self.persist_frontiers.frontier().elements(),
                batch_description = ?self.batch_description.as_ref().map(|(d, _)| d),
                message,
            );
        }

        fn advance_desired_ok_frontier(&mut self, frontier: Antichain<Timestamp>) {
            if PartialOrder::less_than(&self.desired_frontiers.ok, &frontier) {
                self.desired_frontiers.ok = frontier;
                self.trace("advanced `desired` ok frontier");
            }
        }

        fn advance_desired_err_frontier(&mut self, frontier: Antichain<Timestamp>) {
            if PartialOrder::less_than(&self.desired_frontiers.err, &frontier) {
                self.desired_frontiers.err = frontier;
                self.trace("advanced `desired` err frontier");
            }
        }

        fn advance_persist_ok_frontier(&mut self, frontier: Antichain<Timestamp>) {
            if PartialOrder::less_than(&self.persist_frontiers.ok, &frontier) {
                self.persist_frontiers.ok = frontier;
                self.apply_persist_frontier_advancement();
                self.trace("advanced `persist` ok frontier");
            }
        }

        fn advance_persist_err_frontier(&mut self, frontier: Antichain<Timestamp>) {
            if PartialOrder::less_than(&self.persist_frontiers.err, &frontier) {
                self.persist_frontiers.err = frontier;
                self.apply_persist_frontier_advancement();
                self.trace("advanced `persist` err frontier");
            }
        }

        /// Apply the effects of a previous `persist` frontier advancement.
        fn apply_persist_frontier_advancement(&mut self) {
            let frontier = self.persist_frontiers.frontier();

            // We will only emit times at or after the `persist` frontier, so now is a good time to
            // advance the times of stashed updates.
            self.corrections.ok.set_lower(frontier.clone());
            self.corrections.err.set_lower(frontier.clone());

            // If the `persist` frontier is beyond the `lower` of the current batch description, we
            // won't be able to append the batch, so the batch description is not valid anymore.
            if let Some((desc, _)) = &self.batch_description {
                if PartialOrder::less_than(&desc.lower, frontier) {
                    self.batch_description = None;
                }
            }
        }

        fn absorb_batch_description(&mut self, desc: BatchDescription, cap: Capability<Timestamp>) {
            if PartialOrder::less_than(&desc.lower, self.persist_frontiers.frontier()) {
                self.trace(format!("skipping outdated batch description: {desc:?}"));
                return;
            }

            self.batch_description = Some((desc, cap));
            self.trace("set batch description");
        }

        async fn maybe_write_batch(&mut self) -> Option<(BatchOrData, Capability<Timestamp>)> {
            let (desc, _cap) = self.batch_description.as_ref()?;

            // We can write a new batch if we have seen all `persist` updates before `lower` and
            // all `desired` updates up to `upper`.
            let persist_complete = desc.lower == *self.persist_frontiers.frontier();
            let desired_complete =
                PartialOrder::less_equal(&desc.upper, self.desired_frontiers.frontier());
            if !persist_complete || !desired_complete {
                return None;
            }

            let (desc, cap) = self.batch_description.take()?;

            debug_assert_eq!(desc.lower, *self.corrections.ok.lower());
            debug_assert_eq!(desc.lower, *self.corrections.err.lower());

            let batch_ok_updates = self.corrections.ok.updates_before(&desc.upper);
            let batch_err_updates = self.corrections.err.updates_before(&desc.upper);
            let update_count = batch_ok_updates.len() + batch_err_updates.len();

            // Don't write empty batches.
            if update_count == 0 {
                drop((batch_ok_updates, batch_err_updates));
                self.trace("skipping empty batch");
                return None;
            }

            // Small batch optimization: Instead of writing small batches and appending them
            // separately, we will send their data to the `append` operator and let it do the write
            // and append in one operation.
            let minimum_batch_updates = self.persist_config.sink_minimum_batch_updates();
            let batch = if update_count <= minimum_batch_updates {
                let oks = batch_ok_updates.map(|(d, t, r)| (Ok(d), t, r));
                let errs = batch_err_updates.map(|(d, t, r)| (Err(d), t, r));
                let contents = oks.chain(errs).collect();
                BatchOrData::Data {
                    lower: desc.lower,
                    upper: desc.upper,
                    contents,
                }
            } else {
                let oks = batch_ok_updates.map(|(d, t, r)| ((SourceData(Ok(d)), ()), t, r));
                let errs = batch_err_updates.map(|(d, t, r)| ((SourceData(Err(d)), ()), t, r));
                let batch = self
                    .persist_writer
                    .batch(oks.chain(errs), desc.lower, desc.upper)
                    .await
                    .expect("valid usage");
                BatchOrData::Batch(batch.into_transmittable_batch())
            };

            self.trace(format!("wrote batch with {update_count} updates"));
            Some((batch, cap))
        }
    }
}

mod append {
    use super::*;

    pub fn render<S>(
        name: String,
        persist_api: PersistApi,
        active_worker_id: usize,
        descs: &DescsStream<S>,
        batches: &BatchesStream<S>,
    ) -> Box<dyn Any>
    where
        S: Scope<Timestamp = Timestamp>,
    {
        let scope = descs.scope();
        let worker_id = scope.index();

        let mut op = OperatorBuilder::new(name, scope);

        let mut descs_input = op.new_disconnected_input(descs, Pipeline);
        let mut batches_input = op.new_disconnected_input(
            batches,
            Exchange::new(move |_| u64::cast_from(active_worker_id)),
        );

        let button = op.build(move |_capabilities| async move {
            if worker_id != active_worker_id {
                return;
            }

            let persist_writer = persist_api.open_writer().await;
            let mut state = State::new(persist_writer);

            loop {
                // Read from the inputs, absorb batch descriptions and batches. If the `batches`
                // frontier advances, or if we receive a new batch description, we might have to
                // append a new batch.
                tokio::select! {
                    Some(event) = descs_input.next() => {
                        if let Event::Data(_cap, data) = event {
                            for desc in data {
                                state.absorb_batch_description(desc);
                                state.maybe_append_batch().await;
                            }
                        }
                    }
                    Some(event) = batches_input.next() => {
                        match event {
                            Event::Data(_cap, data) => {
                                for batch in data {
                                    state.absorb_batch(batch).await;
                                }
                            }
                            Event::Progress(frontier) => {
                                state.advance_batches_frontier(frontier);
                                state.maybe_append_batch().await;
                            }
                        }
                    }
                    // All inputs are exhausted, so we can shut down.
                    else => return,
                }
            }
        });

        let token = Box::new(button.press_on_drop());
        token
    }

    struct State {
        persist_writer: WriteHandle<SourceData, (), Timestamp, Diff>,
        /// The current input frontier of `batches`.
        batches_frontier: Antichain<Timestamp>,
        /// The greatest observed `lower` from both `descs` and `batches`.
        lower: Antichain<Timestamp>,
        /// The batch description for `lower`, if any.
        batch_description: Option<BatchDescription>,
        /// Written batches received for `lower`.
        written_batches: Vec<Batch<SourceData, (), Timestamp, Diff>>,
        /// Unwritten data received for `lower`.
        unwritten_data: Option<BatchBuilder<SourceData, (), Timestamp, Diff>>,
    }

    impl State {
        fn new(persist_writer: WriteHandle<SourceData, (), Timestamp, Diff>) -> Self {
            Self {
                persist_writer,
                batches_frontier: Antichain::from_elem(Timestamp::MIN),
                lower: Antichain::from_elem(Timestamp::MIN),
                batch_description: None,
                written_batches: Default::default(),
                unwritten_data: None,
            }
        }

        fn trace<S: AsRef<str>>(&self, message: S) {
            let message = message.as_ref();
            trace!(
                batches_frontier = ?self.batches_frontier.elements(),
                lower = ?self.lower.elements(),
                batch_description = ?self.batch_description,
                message,
            );
        }

        fn advance_batches_frontier(&mut self, frontier: Antichain<Timestamp>) {
            if PartialOrder::less_than(&self.batches_frontier, &frontier) {
                self.batches_frontier = frontier;
                self.trace("advanced `batches` frontier");
            }
        }

        fn advance_lower(&mut self, frontier: Antichain<Timestamp>) {
            debug_assert!(PartialOrder::less_than(&self.lower, &frontier));

            self.lower = frontier;
            self.batch_description = None;
            self.written_batches.clear();
            self.unwritten_data = None;

            self.trace("advanced `lower`");
        }

        fn absorb_batch_description(&mut self, desc: BatchDescription) {
            if PartialOrder::less_than(&self.lower, &desc.lower) {
                self.advance_lower(desc.lower.clone());
            } else if &self.lower != &desc.lower {
                self.trace(format!("skipping outdated batch description: {desc:?}"));
                return;
            }

            self.batch_description = Some(desc);
            self.trace("set batch description");
        }

        async fn absorb_batch(&mut self, batch: BatchOrData) {
            match batch {
                BatchOrData::Batch(batch) => {
                    let batch = self.persist_writer.batch_from_transmittable_batch(batch);
                    if PartialOrder::less_than(&self.lower, batch.lower()) {
                        self.advance_lower(batch.lower().clone());
                    } else if &self.lower != batch.lower() {
                        self.trace(format!(
                            "skipping outdated batch: ({:?}, {:?})",
                            batch.lower().elements(),
                            batch.upper().elements(),
                        ));
                        return;
                    }
                    self.written_batches.push(batch);
                }
                BatchOrData::Data {
                    lower,
                    upper,
                    contents,
                } => {
                    if PartialOrder::less_than(&self.lower, &lower) {
                        self.advance_lower(lower.clone());
                    } else if self.lower != lower {
                        self.trace(format!(
                            "skipping outdated batch: ({:?}, {:?})",
                            lower.elements(),
                            upper.elements(),
                        ));
                        return;
                    }
                    let builder = self
                        .unwritten_data
                        .get_or_insert_with(|| self.persist_writer.builder(lower));
                    for (data, time, diff) in contents {
                        builder
                            .add(&SourceData(data), &(), &time, &diff)
                            .await
                            .expect("valid usage");
                    }
                }
            }

            self.trace("absorbed a batch");
        }

        async fn maybe_append_batch(&mut self) {
            let batches_complete = PartialOrder::less_than(&self.lower, &self.batches_frontier);
            if !batches_complete {
                return;
            }

            let Some(desc) = self.batch_description.take() else {
                return;
            };

            if let Some(builder) = self.unwritten_data.take() {
                let batch = builder
                    .finish(desc.upper.clone())
                    .await
                    .expect("valid usage");
                self.written_batches.push(batch);
            }

            let mut to_append: Vec<_> = self.written_batches.iter_mut().collect();
            let result = self
                .persist_writer
                .compare_and_append_batch(
                    &mut to_append[..],
                    desc.lower.clone(),
                    desc.upper.clone(),
                )
                .await
                .expect("valid usage");

            let new_lower = match result {
                Ok(()) => {
                    self.trace("appended a batch");
                    desc.upper
                }
                Err(mismatch) => {
                    let shard_upper = mismatch.current;
                    self.trace(format!(
                        "append failed due to `lower` mismatch: {:?}",
                        shard_upper.elements(),
                    ));
                    // Clean up batches we didn't manage to append.
                    for batch in self.written_batches.drain(..) {
                        batch.delete().await;
                    }
                    shard_upper
                }
            };

            self.advance_lower(new_lower);
        }
    }
}
