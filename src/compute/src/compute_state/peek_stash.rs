// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! For eligible peeks, we send the result back via the peek stash (aka persist
//! blob), instead of inline in `ComputeResponse`.

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::{PeekResponse, StashedPeekResponse};
use mz_expr::row::RowCollection;
use mz_ore::cast::CastFrom;
use mz_ore::future::OreFutureExt;
use mz_ore::task::RuntimeExt;
use mz_persist_client::Schemas;
use mz_persist_client::batch::{Batch, BatchBuilder};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{RelationDesc, Timestamp};
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tracing::warn;
use uuid::Uuid;

use crate::compute_state::peek_scan::RowBatch;

/// Whether a [`StashUpload`] has room for more rows.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum UploadDemand {
    /// The upload wants more rows.
    Wants,
    /// The upload holds every row the peek's finishing can use. Rows pushed after this are
    /// discarded, because they sit past the offset plus limit the finishing asks for and no answer
    /// built from this upload can contain them.
    Satisfied,
}

/// A peek's answer on its way to the peek stash, written to persist a batch of rows at a time.
///
/// The upload owns the IO the stash needs, so a walk that feeds it performs none: a driver that can
/// await pushes the rows the walk produced and finishes the upload, and the walk itself neither
/// opens a client nor writes a byte. That split is what keeps a walk drivable from a timely worker
/// and from an async task alike.
///
/// The rows an upload is given are the rows it writes, in the order it is given them, up to the
/// point where the peek's finishing has all it can use. An upload that does not reach a reader
/// deletes the parts a finished batch can reach, whether it is handed to [`StashUpload::discard`],
/// only dropped, or stopped part-way through finishing.
///
/// What a finished batch reaches is not always everything that was written. Persist merges runs
/// once a builder holds more than `peek_response_stash_batch_max_runs` of them, and a merge writes
/// a fresh output and drops the inputs it read without entering them into shard state, so nothing
/// on either side can address them again. Parts flush at `persist_blob_target_size`, so an upload
/// small enough never to merge deletes all of what it wrote and a large one deletes the output of
/// its last merge. A reader deleting a response it has finished with reaches exactly the same
/// parts, so this is a property of the builder rather than of abandonment.
///
/// The guarantee also ends where the finished batch does. Once [`StashUpload::finish`] hands back
/// a response, the parts belong to the response rather than to any upload, and a caller that drops
/// one leaves them behind. A replica that dies mid-upload leaves them behind too, and
/// [`StashUpload::abandon`] says so.
///
/// Nothing here bounds how large a stashed answer grows where the finishing has no limit to reach.
/// `max_result_size` bounds the prefix a single scan retains between batches and never the sum
/// across them, so an upload writes as many rows as the walk feeding it produces. What keeps a
/// reader from having to hold all of them at once is the reader's own budget, the dyncfgs
/// `peek_stash_read_batch_size_bytes` and `peek_stash_read_memory_budget_bytes`, and nothing on
/// this side.
///
/// Every write reports its failure rather than raising it. The unit that fails is the peek, whose
/// driver answers it with the error, so a rejected write costs one query its answer instead of
/// costing the walk that produced it its task. Cleanup holds itself to the same rule by catching
/// what persist raises, for the reason [`StashUpload::abandon`] gives.
pub(super) struct StashUpload {
    /// The description the stashed response reports, and the schema the batch is written under.
    relation_desc: RelationDesc,
    /// The shard the batch belongs to, derived from the peek's uuid so that a reader holding the
    /// response can find it.
    shard_id: ShardId,
    /// The parts persist has taken so far. Taken by whichever of [`StashUpload::finish`] and
    /// [`StashUpload::abandon`] gets there first, and a `None` says the parts are accounted for
    /// and nothing is left to delete.
    batch_builder: Option<BatchBuilder<SourceData, (), Timestamp, i64>>,
    /// The upper the batch is finished at, one step beyond the timestamp every row is written at.
    upper: Antichain<Timestamp>,
    /// The number of rows the peek's finishing can use, its offset plus its limit, or `None` for a
    /// finishing that can use every row the peek produces.
    max_rows: Option<usize>,
    /// Rows written so far, counting a row with a diff of `n` as `n` rows, which is how the
    /// finishing counts them.
    num_rows: u64,
    /// The runtime an abandoned upload's deletion is spawned on, held rather than taken from the
    /// ambient context because [`StashUpload::abandon`] runs where there may be none.
    runtime: Handle,
}

/// What a [`StashUpload`] whose builder is already gone would be: an upload past the one call that
/// consumes it, which no caller holds.
const BUILDER_TAKEN: &str = "an upload holds its builder until it is finished or abandoned";

impl StashUpload {
    /// Opens an upload for the peek `peek_uuid` identifies.
    ///
    /// Fails when the stash location does not open, in which case nothing has been written.
    pub(super) async fn open(
        persist_clients: &PersistClientCache,
        persist_location: PersistLocation,
        batch_max_runs: usize,
        peek_uuid: Uuid,
        relation_desc: RelationDesc,
        max_rows: Option<usize>,
    ) -> Result<Self, String> {
        let client = persist_clients
            .open(persist_location)
            .await
            .map_err(|e| e.to_string())?;

        let shard_id = format!("s{}", peek_uuid);
        let shard_id = ShardId::try_from(shard_id).expect("can parse");
        let write_schemas: Schemas<SourceData, ()> = Schemas {
            id: None,
            key: Arc::new(relation_desc.clone()),
            val: Arc::new(UnitSchema),
        };

        let result_ts = Timestamp::default();
        let lower = Antichain::from_elem(result_ts);
        let upper = Antichain::from_elem(result_ts.step_forward());

        // We have to use SourceData, which is a wrapper around a Result<Row,
        // DataflowError>, because the bare columnar Row encoder doesn't support
        // encoding rows with zero columns.
        //
        // TODO: We _could_ work around the above by teaching the bare columnar
        // Row encoder about zero-column rows.
        let batch_builder = client
            .batch_builder::<SourceData, (), Timestamp, i64>(
                shard_id,
                write_schemas,
                lower,
                Some(batch_max_runs),
            )
            .await;

        Ok(Self {
            relation_desc,
            shard_id,
            batch_builder: Some(batch_builder),
            upper,
            max_rows,
            num_rows: 0,
            runtime: Handle::current(),
        })
    }

    /// Writes `rows` to the stash and reports whether the upload wants more.
    ///
    /// An upload that reaches what the finishing can use stops there, part-way through `rows` if
    /// that is where it lands, and discards the rest of them. A driver holding a walk that is still
    /// producing rows learns from the [`UploadDemand::Satisfied`] this returns that it may stop the
    /// walk rather than finish it.
    ///
    /// Fails where persist rejects the write, which leaves the upload unusable and the rows it
    /// holds unanswerable.
    pub(super) async fn push(&mut self, rows: RowBatch) -> Result<UploadDemand, String> {
        for (row, diff) in rows {
            if self.demand() == UploadDemand::Satisfied {
                break;
            }

            self.num_rows += u64::from(NonZeroU64::try_from(diff).expect("diff fits into u64"));
            let diff: i64 = diff.into();

            self.batch_builder
                .as_mut()
                .expect(BUILDER_TAKEN)
                .add(&SourceData(Ok(row)), &(), &Timestamp::default(), &diff)
                .await
                .map_err(|err| err.to_string())?;
        }

        Ok(self.demand())
    }

    /// Whether the upload still wants rows.
    pub(super) fn demand(&self) -> UploadDemand {
        match self.max_rows {
            Some(max_rows) if self.num_rows >= u64::cast_from(max_rows) => UploadDemand::Satisfied,
            _ => UploadDemand::Wants,
        }
    }

    /// Finishes the batch and builds the response that names it.
    ///
    /// `inline_rows` are rows of the same answer that never reached the stash, which the response
    /// carries beside the batch rather than paying a write for. They are outside the stashed row
    /// count, which describes the batch alone, and outside the ordering the stash imposes, which is
    /// sound because a peek reaches the stash only with an empty `order_by`.
    ///
    /// Fails where persist rejects the batch, in which case nothing that was written is readable.
    /// A rejection takes the parts with it, because persist keeps the builder it was asked to
    /// finish and hands back no handle to what it holds. Only a batch whose bounds do not admit
    /// its own updates is refused, which this upload's fixed lower, upper and timestamp cannot
    /// produce, so the case is stated rather than expected.
    pub(super) async fn finish(mut self, inline_rows: RowBatch) -> Result<PeekResponse, String> {
        let delivered = self.finish_batch().await?;

        // Built before the batch leaves its guard, so that an unwind here still deletes what was
        // written rather than leaving it for nobody.
        let inline_rows = inline_rows
            .into_iter()
            .map(|(row, copies)| {
                let copies = NonZeroUsize::try_from(copies).expect("fits into usize");
                (row, copies)
            })
            .collect();

        let batch = delivered.take();

        let stashed_response = StashedPeekResponse {
            num_rows_batches: self.num_rows,
            encoded_size_bytes: batch.encoded_size_bytes(),
            relation_desc: self.relation_desc.clone(),
            shard_id: self.shard_id,
            batches: vec![batch.into_transmittable_batch()],
            inline_rows: vec![RowCollection::new(inline_rows, &[])],
        };
        Ok(PeekResponse::Stashed(Box::new(stashed_response)))
    }

    /// Finishes the batch as work of its own, and leaves the upload holding no parts.
    ///
    /// Flushing the buffered part and the part uploads still in flight is the longest await an
    /// upload makes, so it is where a cancellation is most likely to land, and the builder is
    /// already out of the upload by then. Handing the builder to a task the cancellation does not
    /// reach is what keeps that window from stranding everything written so far: whoever ends up
    /// holding the batch nobody will read deletes it.
    async fn finish_batch(&mut self) -> Result<DeliveredBatch, String> {
        let batch_builder = self.batch_builder.take().expect(BUILDER_TAKEN);
        let upper = self.upper.clone();
        let shard_id = self.shard_id;
        let runtime = self.runtime.clone();

        let (tx, rx) = oneshot::channel();
        let _handle =
            self.runtime
                .spawn_named(|| format!("peek_stash::finish({shard_id})"), async move {
                    let delivered = batch_builder
                        .finish(upper)
                        .await
                        .map(|batch| DeliveredBatch::new(batch, runtime, shard_id))
                        .map_err(|err| err.to_string());
                    // A send that finds no receiver hands the delivery straight back, and dropping it
                    // here deletes the batch. An error nobody is left to read is simply dropped.
                    let _undelivered = tx.send(delivered);
                });

        // The task is detached rather than held, so it outlives this await and the only way the
        // channel closes without a delivery is a runtime that is going away.
        rx.await
            .map_err(|_| "peek stash lost the task finishing its batch".to_string())?
    }

    /// Deletes the parts this upload has already written, consuming it.
    ///
    /// A driver whose peek will be answered with something other than these rows calls this to say
    /// so. The deletion is scheduled rather than performed here, so this returns before blob
    /// storage has caught up and reports nothing about how it went.
    pub(super) fn discard(mut self) {
        self.abandon();
    }

    /// Schedules the deletion of whatever parts the upload still holds, and leaves it holding
    /// none.
    ///
    /// Persist hands back a handle to the parts it has taken only by finishing the batch, so the
    /// rows still buffered are written out before the delete removes them again.
    ///
    /// That write is not small. A builder flushes only once it holds `persist_blob_target_size`
    /// bytes, 128 MiB by default, so abandoning an upload can put that much into blob storage for
    /// no purpose beyond earning the handle that deletes it, and hold it in memory meanwhile. The
    /// walk's permit is released when the walk returns rather than when this finishes, so nothing
    /// bounds how many abandoned uploads carry a part at once.
    ///
    /// TODO(PER-70): persist could offer a builder teardown that surrenders the parts already
    /// written without flushing the buffered one, which would make this cost a delete and nothing
    /// else, and would remove the reason the finish below has to be caught.
    fn abandon(&mut self) {
        let Some(batch_builder) = self.batch_builder.take() else {
            return;
        };

        let upper = self.upper.clone();
        let shard_id = self.shard_id;

        // Scheduled rather than awaited because the caller that matters most cannot await at all.
        // Cancelling a peek aborts the walk driving it, and an aborted task is dropped rather than
        // polled again, so the deletion reaches blob storage only as work that does not belong to
        // that task. Entering the runtime explicitly is what lets this run from a `Drop`, which is
        // not guaranteed to run inside a runtime context.
        //
        // NOTE: this reaches only an upload that a live replica gives up on. A replica that dies
        // mid-upload, or one whose runtime is already shutting down, leaves the parts in blob
        // storage, and reclaiming those needs a reader-side sweep or persist's own garbage
        // collection.
        let _handle =
            self.runtime
                .spawn_named(|| format!("peek_stash::discard({shard_id})"), async move {
                    // A builder whose write was in flight when its walk was aborted holds a part
                    // that persist has already marked as being waited on, and finishing it panics
                    // rather than returning, which PER-70 tracks. The panic has to be caught here:
                    // this replica installs a handler that aborts the process for any panic outside
                    // a catch, so reclaiming one query's blob storage would otherwise cost the
                    // whole replica. Failing to reclaim is what this path already tolerates
                    // elsewhere, which makes the leak the right outcome and the abort the wrong one.
                    let finished = std::panic::AssertUnwindSafe(batch_builder.finish(upper))
                        .ore_catch_unwind()
                        .await;
                    match finished {
                        Ok(Ok(batch)) => batch.delete().await,
                        Ok(Err(error)) => {
                            warn!(%shard_id, %error, "peek stash cannot delete an abandoned batch")
                        }
                        Err(_panic) => {
                            warn!(
                                %shard_id,
                                "peek stash cannot reclaim an abandoned batch interrupted mid-write"
                            )
                        }
                    }
                });
    }
}

impl Drop for StashUpload {
    /// Deletes the parts of an upload that ends without [`StashUpload::finish`] or
    /// [`StashUpload::discard`], which is the only cleanup a walk aborted mid-upload gets.
    fn drop(&mut self) {
        self.abandon();
    }
}

/// A finished batch on its way to the response that will name it, deleted from blob storage if it
/// never arrives.
///
/// A batch nobody takes out of a delivery is a batch no reader will ever be told how to find, so
/// dropping one deletes it. That covers both ways a delivery goes unclaimed, the send that finds
/// no receiver and the receiver dropped between the send and the take, which is what persist's own
/// `Drop for Batch` does not: it logs the blob keys it is leaving behind and leaves them.
struct DeliveredBatch {
    /// Taken by [`DeliveredBatch::take`], and a `None` says the batch has an owner that will
    /// answer for it.
    batch: Option<Batch<SourceData, (), Timestamp, i64>>,
    runtime: Handle,
    shard_id: ShardId,
}

impl DeliveredBatch {
    fn new(
        batch: Batch<SourceData, (), Timestamp, i64>,
        runtime: Handle,
        shard_id: ShardId,
    ) -> Self {
        Self {
            batch: Some(batch),
            runtime,
            shard_id,
        }
    }

    /// Claims the batch, consuming the delivery.
    ///
    /// The caller owes persist what the delivery owed it: a batch it drops without transmitting or
    /// deleting is a batch whose blobs stay behind.
    fn take(mut self) -> Batch<SourceData, (), Timestamp, i64> {
        self.batch
            .take()
            .expect("a delivery holds its batch until it is claimed")
    }
}

impl Drop for DeliveredBatch {
    fn drop(&mut self) {
        let Some(batch) = self.batch.take() else {
            return;
        };

        let shard_id = self.shard_id;
        let _handle = self
            .runtime
            .spawn_named(|| format!("peek_stash::delete({shard_id})"), async move {
                batch.delete().await
            });
    }
}

/// Where a peek's rows go when they may not be answered with inline, and what opening the upload
/// that writes them takes.
///
/// A driver holds a target rather than an open upload, because a walk that never crosses the stash
/// threshold must neither open a shard nor write a byte, and most walks never do. A driver is given
/// one exactly when the scan it drives was opened stash-eligible, so a walk with no target is a
/// walk whose scan offers no batch.
pub(super) struct StashTarget {
    persist_clients: Arc<PersistClientCache>,
    persist_location: PersistLocation,
    /// The peek's uuid, which the shard the batch belongs to is derived from.
    peek_uuid: Uuid,
    /// The description the rows are written under, and the one the response reports.
    relation_desc: RelationDesc,
    /// The number of rows the peek's finishing can use, its offset plus its limit, or `None` for a
    /// finishing that can use every row the peek produces.
    max_rows: Option<usize>,
}

impl StashTarget {
    /// The stash `peek`'s rows go to, at `persist_location`.
    pub(super) fn new(
        peek: &Peek,
        persist_clients: Arc<PersistClientCache>,
        persist_location: PersistLocation,
    ) -> Self {
        Self {
            persist_clients,
            persist_location,
            peek_uuid: peek.uuid,
            relation_desc: peek.result_desc.clone(),
            max_rows: peek.finishing.num_rows_needed(),
        }
    }

    /// Opens the upload, whose batch builder holds at most `batch_max_runs` runs.
    pub(super) async fn open(&self, batch_max_runs: usize) -> Result<StashUpload, String> {
        StashUpload::open(
            &self.persist_clients,
            self.persist_location.clone(),
            batch_max_runs,
            self.peek_uuid,
            self.relation_desc.clone(),
            self.max_rows,
        )
        .await
    }
}

/// Tests of the incremental stash upload, over the persist location a replica would write to.
///
/// [`tests::stashed_rows`] is shared with the drivers that feed an upload, which read a response
/// back the same way.
#[cfg(test)]
pub(crate) mod tests {
    use std::num::NonZeroI64;
    use std::time::Duration;

    use mz_compute_types::dyncfgs::{
        PEEK_RESPONSE_STASH_BATCH_MAX_RUNS, PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES,
    };
    use mz_dyncfg::{ConfigUpdates, ConfigVal};
    use mz_ore::cast::CastLossy;
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_repr::{Datum, Row, SqlScalarType};

    use super::*;

    /// How many turns a test gives blob storage to catch up before it declares a deletion lost.
    ///
    /// Deleting the parts of an abandoned upload is scheduled rather than awaited, and the write
    /// that earns the handle to delete runs partly on persist's isolated runtime, so a test can
    /// only wait for it. Bounded by turns rather than by a deadline, so a deletion that never
    /// happens fails the test rather than hanging the suite.
    const BLOB_TURNS: usize = 1_000;

    /// A run limit above what any upload here produces, so that its builder never merges runs.
    ///
    /// A merge writes parts of its own and leaves the parts it merged from behind, both of which
    /// are persist's business rather than the upload's. Raising the limit past the runs a test
    /// produces is what makes the parts it counts the parts the upload is answerable for.
    const NO_RUN_MERGING: usize = 64;

    /// A persist client cache whose blob traffic a test can count.
    ///
    /// Configured so that every row an upload takes becomes a part in blob storage rather than
    /// bytes inlined into shard state, which is what makes "the parts an upload wrote" something a
    /// test can observe at all. The counts are read off the registry because persist keeps its own
    /// metric handles private.
    pub(crate) struct CountedBlob {
        registry: MetricsRegistry,
        clients: Arc<PersistClientCache>,
    }

    impl CountedBlob {
        pub(crate) fn new() -> Self {
            let cfg = PersistConfig::new_for_tests();

            let mut updates = ConfigUpdates::default();
            for (name, val) in [
                // A part per row, so a walk that has pushed anything has put a key in blob
                // storage.
                ("persist_blob_target_size", ConfigVal::Usize(0)),
                // Without this persist keeps a small part in shard state, where no blob counter
                // sees it and no delete has anything to remove.
                (
                    "persist_inline_writes_single_max_bytes",
                    ConfigVal::Usize(0),
                ),
                // A part per row means a builder that stalls waiting for its outstanding writes,
                // and a task aborted inside that stall leaves persist's `Pending` in the
                // `Blocking` state it panics on when the cleanup later finishes the batch. Raising
                // the bound past the parts a test produces keeps the stall out of the way, so what
                // an abort test measures is this module's cleanup rather than that hazard. See the
                // finding recorded for this layer.
                (
                    "persist_batch_builder_max_outstanding_parts",
                    ConfigVal::Usize(8_192),
                ),
            ] {
                assert!(
                    cfg.entry(name).is_some(),
                    "persist no longer has a config named {name}"
                );
                updates.add_dynamic(name, val);
            }
            updates.apply(&cfg);

            let registry = MetricsRegistry::new();
            let clients = Arc::new(PersistClientCache::new(cfg, &registry, |_, _| {
                PubSubClientConnection::noop()
            }));
            Self { registry, clients }
        }

        pub(crate) fn clients(&self) -> &Arc<PersistClientCache> {
            &self.clients
        }

        /// Blob keys written.
        pub(crate) fn written(&self) -> u64 {
            self.succeeded("blob_set")
        }

        /// Blob keys deleted.
        pub(crate) fn deleted(&self) -> u64 {
            self.succeeded("blob_delete")
        }

        /// Deletes that found nothing to delete, which is how a delete of a key that was never
        /// written shows up.
        pub(crate) fn deletes_of_nothing(&self) -> u64 {
            self.counter("mz_persist_external_blob_delete_noop_count", &[])
        }

        /// Waits until every key written has been deleted, which is what an upload that reaches no
        /// reader owes.
        pub(crate) async fn wait_until_nothing_is_left(&self, what: &str) {
            for _ in 0..BLOB_TURNS {
                let written = self.written();
                if written > 0 && self.deleted() == written {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            panic!(
                "{what} left {} of {} blob keys behind after {BLOB_TURNS} turns",
                self.written() - self.deleted(),
                self.written(),
            );
        }

        /// Waits until at least one part has reached blob storage.
        pub(crate) async fn wait_until_something_is_written(&self, what: &str) {
            for _ in 0..BLOB_TURNS {
                if self.written() > 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            panic!("{what} wrote no blob key within {BLOB_TURNS} turns");
        }

        fn succeeded(&self, op: &str) -> u64 {
            self.counter("mz_persist_external_succeeded_count", &[("op", op)])
        }

        /// Sums the counter series named `name` whose labels include all of `labels`, reading zero
        /// for a series that has not been incremented yet.
        fn counter(&self, name: &str, labels: &[(&str, &str)]) -> u64 {
            let Some(family) = self
                .registry
                .gather()
                .into_iter()
                .find(|m| m.name() == name)
            else {
                return 0;
            };
            family
                .get_metric()
                .iter()
                .filter(|metric| {
                    labels.iter().all(|(name, value)| {
                        metric
                            .get_label()
                            .iter()
                            .any(|label| label.name() == *name && label.value() == *value)
                    })
                })
                .map(|metric| u64::cast_lossy(metric.get_counter().value()))
                .sum()
        }
    }

    /// The description of the single-column result every peek here asks for.
    fn result_desc() -> RelationDesc {
        RelationDesc::builder()
            .with_column("value", SqlScalarType::UInt64.nullable(false))
            .finish()
    }

    /// A batch of `values`, each carrying `diff` copies of itself.
    fn batch(values: impl IntoIterator<Item = u64>, diff: i64) -> RowBatch {
        let diff = NonZeroI64::new(diff).expect("a row carries a non-zero diff");
        values
            .into_iter()
            .map(|value| (Row::pack_slice(&[Datum::UInt64(value)]), diff))
            .collect()
    }

    /// An upload of a peek that can use `max_rows` rows, opened against `clients`.
    async fn open_upload(clients: &PersistClientCache, max_rows: Option<usize>) -> StashUpload {
        open_upload_with_runs(
            clients,
            max_rows,
            *PEEK_RESPONSE_STASH_BATCH_MAX_RUNS.default(),
        )
        .await
    }

    /// An upload whose batch builder holds at most `batch_max_runs` runs.
    ///
    /// A builder that has more runs than that merges them, which writes parts of its own and
    /// leaves the parts it merged from behind. A test counting what an upload wrote raises the
    /// limit above the runs it produces, so that the parts it counts are the parts the upload owns.
    async fn open_upload_with_runs(
        clients: &PersistClientCache,
        max_rows: Option<usize>,
        batch_max_runs: usize,
    ) -> StashUpload {
        StashUpload::open(
            clients,
            PersistLocation::new_in_mem(),
            batch_max_runs,
            Uuid::new_v4(),
            result_desc(),
            max_rows,
        )
        .await
        .expect("the in-memory location opens")
    }

    /// Writes `rows` to `upload`, which persist accepts for every batch built here.
    async fn push(upload: &mut StashUpload, rows: RowBatch) -> UploadDemand {
        upload.push(rows).await.expect("persist takes the rows")
    }

    /// Finishes `upload` beside `inline_rows`, which persist accepts for every batch built here.
    async fn finish(upload: StashUpload, inline_rows: RowBatch) -> PeekResponse {
        upload
            .finish(inline_rows)
            .await
            .expect("persist finishes the batch")
    }

    /// The values a finished upload holds, in ascending order, each repeated as often as the diff
    /// it was written with.
    ///
    /// A stashed response names a persist batch rather than carrying rows, so what the upload wrote
    /// is only visible from the batch. Persist consolidates a batch rather than preserving the
    /// order it was written in, so the values are sorted here and compared as the multiset they
    /// are.
    async fn stashed_values(clients: &PersistClientCache, response: PeekResponse) -> Vec<u64> {
        let PeekResponse::Stashed(stashed) = response else {
            panic!("an upload finishes into a stashed response, not {response:?}");
        };

        stashed_rows(clients, *stashed)
            .await
            .into_iter()
            .map(|row| row.unpack_first().unwrap_uint64())
            .collect()
    }

    /// The rows the batches of `stashed` hold, in [`Row`] order, each repeated as often as the diff
    /// it was written with.
    ///
    /// A stashed response names a persist batch rather than carrying rows, so what an upload wrote
    /// is only visible from the batch. Read as the coordinator reads one, deletions included, and
    /// sorted because persist consolidates a batch rather than preserving the order it was written
    /// in. Rows the response carries in `inline_rows` are not here: they never reached a batch.
    pub(crate) async fn stashed_rows(
        clients: &PersistClientCache,
        stashed: StashedPeekResponse,
    ) -> Vec<Row> {
        // Opened out of the cache that opened the upload, because two `PersistLocation`s naming the
        // same in-memory URI reach the same blob only through one cache.
        let mut client = clients
            .open(PersistLocation::new_in_mem())
            .await
            .expect("the in-memory location opens");

        let shard_id = stashed.shard_id;
        let batches = stashed
            .batches
            .into_iter()
            .map(|batch| client.batch_from_transmittable_batch(&shard_id, batch))
            .collect();
        let read_schemas: Schemas<SourceData, ()> = Schemas {
            id: None,
            key: Arc::new(stashed.relation_desc),
            val: Arc::new(UnitSchema),
        };
        let mut cursor = client
            .read_batches_consolidated::<_, _, _, i64>(
                shard_id,
                Antichain::from_elem(Timestamp::default()),
                read_schemas,
                batches,
                |_stats| true,
                *PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES.default(),
            )
            .await
            .expect("the batch is readable at the timestamp it was written at");

        let mut rows = Vec::new();
        while let Some(updates) = cursor.next().await {
            for ((key, _val), _time, diff) in updates {
                let row = key.0.expect("the peek stash holds no errors");
                let copies = usize::try_from(diff).expect("a stashed row carries a positive diff");
                rows.extend(std::iter::repeat_n(row, copies));
            }
        }
        rows.sort();

        // Deleted as the coordinator deletes them once it has read them. A batch dropped without
        // this leaves its blob keys behind and says so in a warning.
        for batch in cursor.into_lease() {
            batch.delete().await;
        }
        rows
    }

    /// An upload with no limit to reach holds every row pushed into it, over as many pushes as the
    /// driver made.
    #[mz_ore::test(tokio::test)]
    async fn an_upload_holds_the_rows_pushed_into_it() {
        let clients = PersistClientCache::new_no_metrics();
        let mut upload = open_upload(&clients, None).await;

        assert_eq!(push(&mut upload, batch(0..3, 1)).await, UploadDemand::Wants);
        assert_eq!(push(&mut upload, batch(3..5, 1)).await, UploadDemand::Wants);

        let response = finish(upload, RowBatch::new()).await;
        let PeekResponse::Stashed(stashed) = &response else {
            panic!("an upload finishes into a stashed response, not {response:?}");
        };
        assert_eq!(
            stashed.num_rows_batches, 5,
            "the response counts the rows the upload wrote"
        );
        assert_eq!(
            stashed_values(&clients, response).await,
            vec![0, 1, 2, 3, 4]
        );
    }

    /// An upload counts a row against the limit as often as its diff says the answer holds it,
    /// rather than once.
    #[mz_ore::test(tokio::test)]
    async fn an_upload_counts_a_row_as_often_as_its_diff() {
        let clients = PersistClientCache::new_no_metrics();
        let mut upload = open_upload(&clients, Some(4)).await;

        assert_eq!(
            push(&mut upload, batch(0..2, 3)).await,
            UploadDemand::Satisfied,
            "six copies of two rows is past a limit of four"
        );
        assert_eq!(
            stashed_values(&clients, finish(upload, RowBatch::new()).await).await,
            vec![0, 0, 0, 1, 1, 1],
            "the row that crossed the limit is written whole"
        );
    }

    /// An upload that has all the rows the finishing can use stops there, part-way through the push
    /// that took it past the limit, and discards what is pushed after.
    #[mz_ore::test(tokio::test)]
    async fn an_upload_stops_at_the_rows_the_finishing_can_use() {
        let clients = PersistClientCache::new_no_metrics();
        let mut upload = open_upload(&clients, Some(2)).await;

        assert_eq!(
            upload.demand(),
            UploadDemand::Wants,
            "an upload that has written nothing wants rows"
        );
        assert_eq!(
            push(&mut upload, batch(0..4, 1)).await,
            UploadDemand::Satisfied
        );
        assert_eq!(
            push(&mut upload, batch(4..8, 1)).await,
            UploadDemand::Satisfied,
            "a satisfied upload stays satisfied"
        );

        assert_eq!(
            stashed_values(&clients, finish(upload, RowBatch::new()).await).await,
            vec![0, 1],
            "the rows past the limit are discarded, in the push that crossed it and after it"
        );
    }

    /// Rows a walk still held when it ended travel with the response rather than through the
    /// batch, so a reader sees them beside the stashed rows and the stashed row count counts only
    /// what the batch holds.
    #[mz_ore::test(tokio::test)]
    async fn rows_that_never_reached_the_stash_travel_inline() {
        let clients = PersistClientCache::new_no_metrics();
        let mut upload = open_upload(&clients, None).await;

        assert_eq!(push(&mut upload, batch(0..3, 1)).await, UploadDemand::Wants);

        let response = finish(upload, batch(3..5, 1)).await;
        let PeekResponse::Stashed(stashed) = &response else {
            panic!("an upload finishes into a stashed response, not {response:?}");
        };
        assert_eq!(
            stashed.num_rows_batches, 3,
            "the stashed row count describes the batch alone"
        );
        let expected: Vec<(Row, NonZeroUsize)> = batch(3..5, 1)
            .into_iter()
            .map(|(row, copies)| {
                (
                    row,
                    NonZeroUsize::try_from(copies).expect("fits into usize"),
                )
            })
            .collect();
        assert_eq!(
            stashed.inline_rows,
            vec![RowCollection::new(expected, &[])],
            "the rows the walk still held travel with the response"
        );
        assert_eq!(
            stashed_values(&clients, response).await,
            vec![0, 1, 2],
            "the rows carried inline are not written to the batch as well"
        );
    }

    /// An upload built by hand, so that a test can hold the bounds [`StashUpload::open`] never
    /// produces.
    ///
    /// Opening fixes the lower, the upper and the timestamp every row is written at, and that
    /// combination is one persist always accepts. The rejection arms exist all the same, because
    /// the alternative to reporting a rejection is a panic in a promoted walk, which lands in the
    /// dead-task arm and takes the worker down with it in a test build.
    async fn upload_with_bounds(
        clients: &PersistClientCache,
        lower: Timestamp,
        upper: Timestamp,
    ) -> StashUpload {
        let client = clients
            .open(PersistLocation::new_in_mem())
            .await
            .expect("the in-memory location opens");
        let shard_id = ShardId::try_from(format!("s{}", Uuid::new_v4())).expect("can parse");
        let relation_desc = result_desc();
        let write_schemas: Schemas<SourceData, ()> = Schemas {
            id: None,
            key: Arc::new(relation_desc.clone()),
            val: Arc::new(UnitSchema),
        };
        let batch_builder = client
            .batch_builder::<SourceData, (), Timestamp, i64>(
                shard_id,
                write_schemas,
                Antichain::from_elem(lower),
                Some(NO_RUN_MERGING),
            )
            .await;

        StashUpload {
            relation_desc,
            shard_id,
            batch_builder: Some(batch_builder),
            upper: Antichain::from_elem(upper),
            max_rows: None,
            num_rows: 0,
            runtime: Handle::current(),
        }
    }

    /// An upload a driver gives up on deletes the parts it had already written.
    ///
    /// This is the path a walk takes when the peek is answered with something other than these
    /// rows, an error from the walk itself or a cancellation the walk observed.
    #[mz_ore::test(tokio::test)]
    async fn a_discarded_upload_deletes_the_parts_it_wrote() {
        let blob = CountedBlob::new();
        let mut upload = open_upload_with_runs(blob.clients(), None, NO_RUN_MERGING).await;

        assert_eq!(push(&mut upload, batch(0..4, 1)).await, UploadDemand::Wants);
        blob.wait_until_something_is_written("an upload holding four rows")
            .await;

        upload.discard();

        blob.wait_until_nothing_is_left("a discarded upload").await;
        assert_eq!(
            blob.deletes_of_nothing(),
            0,
            "the deletes must name the keys the upload wrote"
        );
    }

    /// An upload that is only dropped deletes the parts it had already written, which is the whole
    /// cleanup a walk aborted mid-upload gets: an aborted task is dropped rather than polled
    /// again, so nothing it would have called runs.
    #[mz_ore::test(tokio::test)]
    async fn a_dropped_upload_deletes_the_parts_it_wrote() {
        let blob = CountedBlob::new();
        let mut upload = open_upload_with_runs(blob.clients(), None, NO_RUN_MERGING).await;

        assert_eq!(push(&mut upload, batch(0..4, 1)).await, UploadDemand::Wants);
        blob.wait_until_something_is_written("an upload holding four rows")
            .await;

        drop(upload);

        blob.wait_until_nothing_is_left("a dropped upload").await;
        assert_eq!(
            blob.deletes_of_nothing(),
            0,
            "the deletes must name the keys the upload wrote"
        );
    }

    /// A finished batch that never reaches the response naming it is deleted.
    ///
    /// This is the window between persist handing the batch over and the response taking it: a
    /// cancellation that lands there leaves a batch no reader will ever be told how to find, and
    /// persist's own `Drop for Batch` logs the keys and leaves them.
    #[mz_ore::test(tokio::test)]
    async fn a_finished_batch_that_reaches_no_response_is_deleted() {
        let blob = CountedBlob::new();
        let mut upload = open_upload_with_runs(blob.clients(), None, NO_RUN_MERGING).await;

        assert_eq!(push(&mut upload, batch(0..4, 1)).await, UploadDemand::Wants);
        let delivered = upload
            .finish_batch()
            .await
            .expect("persist finishes the batch");
        assert!(
            blob.written() > 0,
            "a finished batch has written the parts it holds"
        );
        assert_eq!(
            blob.deleted(),
            0,
            "a batch still on its way to a response is not deleted"
        );

        drop(delivered);

        blob.wait_until_nothing_is_left("an unclaimed delivery")
            .await;
        assert_eq!(
            blob.deletes_of_nothing(),
            0,
            "the deletes must name the keys the batch wrote"
        );
    }

    /// An upload that finishes into a response leaves its parts alone, because the response is
    /// what a reader finds them by.
    ///
    /// The other tests here would all pass against an upload that deleted everything it wrote
    /// unconditionally, which is the shape of cleanup that costs every stashed peek its answer.
    #[mz_ore::test(tokio::test)]
    async fn a_finished_upload_leaves_its_parts_for_the_response() {
        let blob = CountedBlob::new();
        let mut upload = open_upload_with_runs(blob.clients(), None, NO_RUN_MERGING).await;

        assert_eq!(push(&mut upload, batch(0..4, 1)).await, UploadDemand::Wants);
        let response = finish(upload, RowBatch::new()).await;

        // Every chance for a deletion that should not happen to happen.
        for _ in 0..BLOB_TURNS {
            tokio::task::yield_now().await;
        }
        assert!(
            blob.written() > 0,
            "a finished upload has written the parts it holds"
        );
        assert_eq!(
            blob.deleted(),
            0,
            "the parts of a finished upload belong to the response that names them"
        );
        assert_eq!(
            stashed_values(blob.clients(), response).await,
            vec![0, 1, 2, 3],
            "the response must still name a readable batch"
        );
    }

    /// A write persist rejects is reported rather than raised.
    ///
    /// The unit that fails is the peek, whose driver answers it with the error. A panic here would
    /// instead end the task driving a promoted walk, and the worker reads a dead task as a defect.
    #[mz_ore::test(tokio::test)]
    async fn a_rejected_write_is_reported_rather_than_raised() {
        let clients = PersistClientCache::new_no_metrics();
        // A lower past the timestamp the upload writes its rows at, which is the one thing
        // `BatchBuilder::add` refuses.
        let mut upload = upload_with_bounds(
            &clients,
            Timestamp::default().step_forward(),
            Timestamp::default().step_forward().step_forward(),
        )
        .await;

        let rejection = upload.push(batch(0..2, 1)).await;

        assert!(
            rejection
                .as_ref()
                .is_err_and(|error| error.contains("not beyond batch lower")),
            "persist must reject the write and the upload must report it: {rejection:?}",
        );
    }

    /// A batch persist rejects is reported rather than raised.
    ///
    /// A rejected finish leaves the parts behind, which [`StashUpload::finish`] states: persist
    /// keeps the builder it was asked to finish and hands back no handle to what it holds.
    #[mz_ore::test(tokio::test)]
    async fn a_rejected_batch_is_reported_rather_than_raised() {
        let clients = PersistClientCache::new_no_metrics();
        // An upper the rows the upload holds are not below, which is the one thing
        // `BatchBuilder::finish` refuses.
        let mut upload =
            upload_with_bounds(&clients, Timestamp::default(), Timestamp::default()).await;

        assert_eq!(push(&mut upload, batch(0..2, 1)).await, UploadDemand::Wants);
        let rejection = upload.finish(RowBatch::new()).await;

        assert!(
            rejection
                .as_ref()
                .is_err_and(|error| error.contains("beyond the expected batch upper")),
            "persist must reject the batch and the upload must report it: {rejection:?}",
        );
    }
}
