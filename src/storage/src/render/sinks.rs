// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to the creation of dataflow sinks.

use std::sync::Arc;
use std::time::{Duration, Instant};

use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::implementations::ord_neu::{
    OrdValBatcher, OrdValSpine, RcOrdValBuilder,
};
use differential_dataflow::{AsCollection, Hashable, VecCollection};
use mz_persist_client::operators::shard_source::SnapshotMode;
use mz_repr::{Datum, Diff, GlobalId, Row, Timestamp};
use mz_storage_operators::persist_source;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{StorageSinkConnection, StorageSinkDesc};
use mz_timely_util::builder_async::PressOnDropButton;
use timely::dataflow::operators::Leave;
use timely::dataflow::{Scope, StreamVec};
use tracing::warn;

use crate::healthcheck::HealthStatusMessage;
use crate::storage_state::StorageState;

/// The concrete trace type produced internally when arranging a sink's input.
/// The sink never sees this directly — only the batches flowing through it —
/// but it's the anchor for the batch type in [`SinkBatchStream`].
pub(crate) type SinkTrace = TraceAgent<OrdValSpine<Option<Row>, Row, Timestamp, Diff>>;

/// Stream of arrangement batches handed to [`SinkRender::render_sink`].
///
/// This is `Arranged::stream` with the trace reader dropped: sinks only need
/// batch-level access (no random-access reads via a cursor), so we don't keep
/// a `TraceAgent` alive. Dropping the reader lets the spine's compaction
/// frontiers advance to the empty antichain, so the arrange operator can
/// aggressively compact / release batch state as updates flow through.
pub(crate) type SinkBatchStream<'scope> =
    StreamVec<'scope, Timestamp, <SinkTrace as TraceReader>::Batch>;

/// _Renders_ complete _differential_ collections
/// that represent the sink and its errors as requested
/// by the original `CREATE SINK` statement.
pub(crate) fn render_sink<'scope>(
    scope: Scope<'scope, ()>,
    storage_state: &mut StorageState,
    sink_id: GlobalId,
    sink: &StorageSinkDesc<CollectionMetadata, mz_repr::Timestamp>,
) -> (
    StreamVec<'scope, (), HealthStatusMessage>,
    Vec<PressOnDropButton>,
) {
    let snapshot_mode = if sink.with_snapshot {
        SnapshotMode::Include
    } else {
        SnapshotMode::Exclude
    };

    let error_handler = storage_state.error_handler("storage_sink", sink_id);

    let name = format!("{sink_id}-sinks");
    let outer_scope = scope.clone();

    scope.scoped(&name, |scope| {
        let mut tokens = vec![];
        let sink_render = get_sink_render_for(&sink.connection);

        let (ok_collection, err_collection, persist_tokens) = persist_source::persist_source(
            scope,
            sink.from,
            Arc::clone(&storage_state.persist_clients),
            &storage_state.txns_ctx,
            sink.from_storage_metadata.clone(),
            None,
            Some(sink.as_of.clone()),
            snapshot_mode,
            timely::progress::Antichain::new(),
            None,
            None,
            async {},
            error_handler,
        );
        tokens.extend(persist_tokens);

        let batches = arrange_sink_input(&*sink_render, ok_collection.as_collection());
        let key_is_synthetic = sink_render.get_key_indices().is_none()
            && sink_render.get_relation_key_indices().is_none();

        let (health, sink_tokens) = sink_render.render_sink(
            storage_state,
            sink,
            sink_id,
            batches,
            key_is_synthetic,
            err_collection.as_collection(),
        );
        tokens.extend(sink_tokens);
        (health.leave(outer_scope), tokens)
    })
}

/// Extract the sink's key column(s) from each row, arrange the resulting
/// `(Option<Row>, Row)` collection by key, and return just the stream of
/// batches — dropping the trace reader.
///
/// Prefers the user-specified sink key, falling back to any natural key of the
/// underlying relation. When neither exists, a synthetic per-row hash is used
/// purely to distribute work across workers — in that case the sink should
/// treat the key as absent (`key_is_synthetic`).
///
/// Partial-moving `arranged.stream` lets the surrounding `Arranged` (and the
/// `TraceAgent` it holds) drop, releasing the spine's compaction holds so the
/// arrange operator can compact batch state as it's emitted.
fn arrange_sink_input<'scope>(
    sink_render: &dyn SinkRender<'scope>,
    collection: VecCollection<'scope, Timestamp, Row, Diff>,
) -> SinkBatchStream<'scope> {
    let key_indices = sink_render
        .get_key_indices()
        .or_else(|| sink_render.get_relation_key_indices())
        .map(|k| k.to_vec());

    let keyed = match key_indices {
        None => collection.map(|row| (Some(Row::pack(Some(Datum::UInt64(row.hashed())))), row)),
        Some(key_indices) => {
            let mut datum_vec = mz_repr::DatumVec::new();
            collection.map(move |row| {
                // TODO[perf] (btv) - is there a way to avoid unpacking and
                // repacking every row and cloning the datums? Does it matter?
                let key = {
                    let datums = datum_vec.borrow_with(&row);
                    Row::pack(key_indices.iter().map(|&idx| datums[idx].clone()))
                };
                (Some(key), row)
            })
        }
    };

    // Allow access to `arrange_named` because we cannot access Mz's wrapper
    // from here. TODO(database-issues#5046): Revisit with cluster unification.
    #[allow(clippy::disallowed_methods)]
    let Arranged {stream, trace: _} = keyed.arrange_named::<OrdValBatcher<_, _, _, _>, RcOrdValBuilder<_, _, _, _>, OrdValSpine<_, _, _, _>>("Arrange Sink");
    stream
}

/// Rate-limited detector for primary-key uniqueness violations as a sink's
/// cursor walk observes `(key, timestamp)` groups.
///
/// Call [`PkViolationWarner::observe`] once per emitted `DiffPair`. When the
/// current `(key, timestamp)` group changes — or when input batches finish —
/// call [`PkViolationWarner::flush`] so the accumulated count is evaluated.
///
/// Keys are identified by their `Hashable::hashed()` value rather than held
/// by value, so the hot observe path does no `Row` clones. A hash collision
/// can mask a PK violation but this is a purely diagnostic check, so the
/// trade-off is acceptable.
pub(crate) struct PkViolationWarner {
    sink_id: GlobalId,
    from_id: GlobalId,
    last_warning: Instant,
    current: Option<(u64, Timestamp)>,
    count: usize,
}

impl PkViolationWarner {
    pub fn new(sink_id: GlobalId, from_id: GlobalId) -> Self {
        Self {
            sink_id,
            from_id,
            last_warning: Instant::now(),
            current: None,
            count: 0,
        }
    }

    /// Record that a `DiffPair` was observed at `(key, time)`. If this starts
    /// a new group, the previous group's count is flushed (and warned about
    /// if the count was > 1).
    pub fn observe(&mut self, key: &Option<Row>, time: Timestamp) {
        // `None` keys hash to a distinct sentinel from any `Row::hashed()`;
        // the exact constant doesn't matter for correctness (it just needs
        // to be stable).
        let hash = key.as_ref().map(|k| k.hashed()).unwrap_or(u64::MAX);
        let same = self.current == Some((hash, time));
        if !same {
            self.flush();
            self.current = Some((hash, time));
        }
        self.count += 1;
    }

    /// Flush the pending `(key, timestamp)` group count. Emits a
    /// rate-limited warning if more than one `DiffPair` was observed.
    pub fn flush(&mut self) {
        if self.count > 1 {
            let now = Instant::now();
            if now.duration_since(self.last_warning) >= Duration::from_secs(10) {
                self.last_warning = now;
                warn!(
                    sink_id = ?self.sink_id,
                    from_id = ?self.from_id,
                    "primary key error: expected at most one update per key and timestamp; \
                        this can happen when the configured sink key is not a primary key of \
                        the sinked relation"
                );
            }
        }
        self.current = None;
        self.count = 0;
    }
}

/// A type that can be rendered as a dataflow sink.
pub(crate) trait SinkRender<'scope> {
    /// Gets the indexes of the columns that form the key that the user
    /// specified when creating the sink, if any.
    fn get_key_indices(&self) -> Option<&[usize]>;

    /// Gets the indexes of the columns that form a key of the sink's underlying
    /// relation, if such a key exists.
    fn get_relation_key_indices(&self) -> Option<&[usize]>;

    /// Renders the sink's dataflow.
    ///
    /// The sink receives a stream of arrangement batches keyed on `Option<Row>`.
    /// The sink is responsible for walking each batch (typically via
    /// [`mz_interchange::envelopes::for_each_diff_pair`]) and handling any
    /// envelope-specific diff-pair construction. When `key_is_synthetic` is
    /// true the arrangement's key is a per-row hash used only for worker
    /// distribution — the sink should treat the key as absent when producing
    /// output.
    fn render_sink(
        &self,
        storage_state: &mut StorageState,
        sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
        sink_id: GlobalId,
        batches: SinkBatchStream<'scope>,
        key_is_synthetic: bool,
        err_collection: VecCollection<'scope, Timestamp, DataflowError, Diff>,
    ) -> (
        StreamVec<'scope, Timestamp, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    );
}

fn get_sink_render_for<'scope>(connection: &StorageSinkConnection) -> Box<dyn SinkRender<'scope>> {
    match connection {
        StorageSinkConnection::Kafka(connection) => Box::new(connection.clone()),
        StorageSinkConnection::Iceberg(connection) => Box::new(connection.clone()),
    }
}
