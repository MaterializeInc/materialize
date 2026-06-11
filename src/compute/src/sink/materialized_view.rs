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

use differential_dataflow::{AsCollection, VecCollection};
use mz_compute_types::sinks::{ComputeSinkDesc, MaterializedViewSinkConnection};
use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::operators::shard_source::{ErrorHandler, SnapshotMode};
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::SourceData;
use mz_timely_util::builder_async::PressOnDropButton;
use mz_timely_util::probe::{Handle, ProbeNotify};
use serde::{Deserialize, Serialize};
use timely::PartialOrder;
use timely::dataflow::operators::probe;
use timely::dataflow::{Scope, StreamVec};
use timely::progress::Antichain;

use crate::compute_state::ComputeState;
use crate::render::StartSignal;
use crate::render::errors::DataflowErrorSer;
use crate::render::sinks::SinkRender;
use crate::sink::materialized_view_v2;
use crate::sink::refresh::apply_refresh;

impl<'scope> SinkRender<'scope> for MaterializedViewSinkConnection<CollectionMetadata> {
    fn render_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        start_signal: StartSignal,
        mut ok_collection: VecCollection<'scope, Timestamp, Row, Diff>,
        mut err_collection: VecCollection<'scope, Timestamp, DataflowErrorSer, Diff>,
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

        let token = materialized_view_v2::persist_sink(
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
pub(super) type DesiredStreams<'s> = OkErr<
    StreamVec<'s, Timestamp, (Row, Timestamp, Diff)>,
    StreamVec<'s, Timestamp, (DataflowErrorSer, Timestamp, Diff)>,
>;

/// Type of the `persist` stream, split into `Ok` and `Err` streams.
pub(super) type PersistStreams<'s> = OkErr<
    StreamVec<'s, Timestamp, (Row, Timestamp, Diff)>,
    StreamVec<'s, Timestamp, (DataflowErrorSer, Timestamp, Diff)>,
>;

/// Type of the `descs` stream.
pub(super) type DescsStream<'s> = StreamVec<'s, Timestamp, BatchDescription>;

/// Type of the `batches` stream.
pub(super) type BatchesStream<'s> = StreamVec<'s, Timestamp, (BatchDescription, ProtoBatch)>;

/// Type of the shared sink write frontier.
pub(super) type SharedSinkFrontier = Rc<RefCell<Antichain<Timestamp>>>;

/// Generic wrapper around ok/err pairs (e.g. streams, frontiers), to simplify code dealing with
/// such pairs.
pub(super) struct OkErr<O, E> {
    pub(super) ok: O,
    pub(super) err: E,
}

impl<O, E> OkErr<O, E> {
    pub(super) fn new(ok: O, err: E) -> Self {
        Self { ok, err }
    }
}

impl OkErr<Antichain<Timestamp>, Antichain<Timestamp>> {
    pub(super) fn new_frontiers() -> Self {
        Self {
            ok: Antichain::from_elem(Timestamp::MIN),
            err: Antichain::from_elem(Timestamp::MIN),
        }
    }

    /// Return the overall frontier, i.e., the minimum of `ok` and `err`.
    pub(super) fn frontier(&self) -> &Antichain<Timestamp> {
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
pub(super) fn advance(
    frontier: &mut Antichain<Timestamp>,
    new: timely::progress::frontier::AntichainRef<'_, Timestamp>,
) -> bool {
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
pub(super) struct PersistApi {
    pub(super) persist_clients: Arc<PersistClientCache>,
    pub(super) collection: CollectionMetadata,
    pub(super) shard_name: String,
    pub(super) purpose: String,
}

impl PersistApi {
    pub(super) async fn open_client(&self) -> PersistClient {
        self.persist_clients
            .open(self.collection.persist_location.clone())
            .await
            .unwrap_or_else(|error| panic!("error opening persist client: {error}"))
    }

    pub(super) async fn open_writer(&self) -> WriteHandle<SourceData, (), Timestamp, StorageDiff> {
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
pub(super) fn persist_source<'s>(
    scope: Scope<'s, Timestamp>,
    sink_id: GlobalId,
    target: CollectionMetadata,
    compute_state: &ComputeState,
    start_signal: StartSignal,
) -> (PersistStreams<'s>, Vec<PressOnDropButton>) {
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

    let (ok_stream, err_stream, token) =
        mz_storage_operators::persist_source::persist_source::<DataflowErrorSer>(
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
            start_signal.into_send_future(),
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
pub(super) struct BatchDescription {
    pub(super) lower: Antichain<Timestamp>,
    pub(super) upper: Antichain<Timestamp>,
    pub(super) append_worker: usize,
}

impl BatchDescription {
    pub(super) fn new(
        lower: Antichain<Timestamp>,
        upper: Antichain<Timestamp>,
        append_worker: usize,
    ) -> Self {
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
pub(super) fn operator_name(sink_id: GlobalId, sub_operator: &str) -> String {
    format!("mv_sink({sink_id})::{sub_operator}")
}
