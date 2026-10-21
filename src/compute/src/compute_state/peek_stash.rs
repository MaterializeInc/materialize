// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! For eligible peeks, we send the result back via the peek stash (aka persist
//! blob), instead of inline in `ComputeResponse`.

use std::num::NonZeroU64;
use std::sync::Arc;

use mz_compute_client::protocol::command::Peek;
use mz_compute_client::protocol::response::{PeekResponse, StashedPeekResponse};
use mz_compute_client::stash::OwnedStash;
use mz_ore::task::RuntimeExt;
use mz_persist::location::ExternalError;
use mz_persist_client::Schemas;
use mz_persist_client::batch::{Added, Batch, BatchBuilder};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::error::InvalidUsage;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{Diff, RelationDesc, Row, Timestamp};
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tracing::warn;
use uuid::Uuid;

use crate::compute_state::peek_scan::RowBatch;

/// A failure that leaves an upload unable to answer the peek whose rows it holds.
///
/// The variant names the step that refused, and the driver reports it as the peek's error.
#[derive(Debug, thiserror::Error)]
pub(super) enum StashError {
    /// The stash location did not open, so nothing was written.
    #[error("peek stash could not open its persist location: {0}")]
    OpenLocation(#[source] ExternalError),
    /// Persist refused a row the upload handed it.
    #[error("peek stash could not write a row: {0}")]
    WriteRow(#[source] InvalidUsage<Timestamp>),
    /// Persist refused to finish the batch, which takes with it every part already written.
    #[error("peek stash could not finish its batch: {0}")]
    FinishBatch(#[source] InvalidUsage<Timestamp>),
    /// The task finishing the batch ended without delivering one, which only a runtime that is
    /// going away can cause.
    #[error("peek stash lost the task finishing its batch")]
    LostFinishTask,
}

/// A peek's answer on its way to the peek stash, written to persist a batch of rows at a time.
///
/// The upload owns the IO the stash needs, so a walk that feeds it performs none: a driver that can
/// await pushes the rows the walk produced and finishes the upload, and the walk itself neither
/// opens a client nor writes a byte. That split keeps a walk drivable from a timely worker and from
/// an async task alike.
///
/// The rows an upload is given are the rows it writes, in the order it is given them. An upload
/// that does not reach a reader deletes what it can, whether it is dropped by a driver that will
/// answer with something else or stopped part-way through finishing.
/// [`StashUpload::abandon`] bounds what that reaches and what it costs.
pub(super) struct StashUpload {
    /// The description the stashed response reports, and the schema the batch is written under.
    relation_desc: RelationDesc,
    /// The shard the batch belongs to. A peek derives it from its uuid with [`stash_shard_id`] so
    /// that a reader holding the response can find it.
    shard_id: ShardId,
    /// The parts persist has taken so far. Taken by whichever of [`StashUpload::finish`] and
    /// [`StashUpload::abandon`] gets there first, and a `None` says the parts are accounted for
    /// and nothing is left to delete.
    batch_builder: Option<BatchBuilder<SourceData, (), Timestamp, i64>>,
    /// The upper the batch is finished at.
    upper: Antichain<Timestamp>,
    /// Whether any update has been written, which tells [`StashUpload::finish_stashed`] there is
    /// no batch to hand back.
    wrote_updates: bool,
    /// Rows written so far, counting a row with a diff of `n` as `n` rows, which is how the
    /// finishing counts them.
    num_rows: u64,
    /// Whether persist has taken a part off this builder and into blob storage. Until it has,
    /// everything the upload holds is in memory, so an abandoned upload drops its builder instead
    /// of paying a write and a delete to reclaim nothing.
    wrote_parts: bool,
    /// The runtime an abandoned upload's deletion is spawned on, held rather than taken from the
    /// ambient context because [`StashUpload::abandon`] runs where there may be none: a `Drop`
    /// carries no runtime context of its own, and `Handle::current` panics without one.
    runtime: Handle,
}

/// The `expect` message where the builder is taken: it is present until `finish` or `abandon`
/// takes it, and neither runs twice.
const BUILDER_TAKEN: &str = "an upload holds its builder until it is finished or abandoned";

/// The shard a peek's stashed batch belongs to.
pub(crate) fn stash_shard_id(uuid: Uuid) -> ShardId {
    let shard_id = format!("s{}", uuid);
    ShardId::try_from(shard_id).expect("can parse")
}

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
    ) -> Result<Self, StashError> {
        let result_ts = Timestamp::default();
        Self::open_range(
            persist_clients,
            persist_location,
            batch_max_runs,
            stash_shard_id(peek_uuid),
            relation_desc,
            Antichain::from_elem(result_ts),
            Antichain::from_elem(result_ts.step_forward()),
        )
        .await
    }

    /// Opens an upload that writes to `shard_id` a batch over `[lower, upper)`.
    ///
    /// Updates may carry any time in the range and any nonzero diff. The bounds are enforced by
    /// persist: [`StashUpload::push_update`] refuses a time before `lower` and
    /// [`StashUpload::finish_stashed`] refuses a batch holding a time at or beyond `upper`.
    ///
    /// Fails when the stash location does not open, in which case nothing has been written.
    pub(crate) async fn open_range(
        persist_clients: &PersistClientCache,
        persist_location: PersistLocation,
        batch_max_runs: usize,
        shard_id: ShardId,
        relation_desc: RelationDesc,
        lower: Antichain<Timestamp>,
        upper: Antichain<Timestamp>,
    ) -> Result<Self, StashError> {
        let client = persist_clients
            .open(persist_location)
            .await
            .map_err(StashError::OpenLocation)?;

        let write_schemas: Schemas<SourceData, ()> = Schemas {
            id: None,
            key: Arc::new(relation_desc.clone()),
            val: Arc::new(UnitSchema),
        };

        // We have to use SourceData, which is a wrapper around a Result<Row,
        // DataflowError>, because the bare columnar Row encoder doesn't support
        // encoding rows with zero columns.
        //
        // TODO: We _could_ work around the above by teaching the bare columnar
        // Row encoder about zero-column rows.
        let batch_builder = client.batch_builder::<SourceData, (), Timestamp, i64>(
            shard_id,
            "peek_stash",
            write_schemas,
            lower,
            Some(batch_max_runs),
        );

        Ok(Self {
            relation_desc,
            shard_id,
            batch_builder: Some(batch_builder),
            upper,
            wrote_updates: false,
            num_rows: 0,
            wrote_parts: false,
            runtime: Handle::current(),
        })
    }

    /// Writes one update to the stash.
    ///
    /// Fails where persist rejects the write, which leaves the upload unusable.
    #[cfg_attr(not(test), expect(dead_code, reason = "no caller outside tests"))]
    pub(crate) async fn push_update(
        &mut self,
        row: &Row,
        time: Timestamp,
        diff: Diff,
    ) -> Result<(), StashError> {
        self.write(&SourceData(Ok(row.clone())), time, diff.into_inner())
            .await
    }

    async fn write(
        &mut self,
        data: &SourceData,
        time: Timestamp,
        diff: i64,
    ) -> Result<(), StashError> {
        let batch_builder = self.batch_builder.as_mut().expect(BUILDER_TAKEN);
        let added = batch_builder
            .add(data, &(), &time, &diff)
            .await
            .map_err(StashError::WriteRow)?;
        self.wrote_updates = true;
        self.wrote_parts |= matches!(added, Added::RecordAndParts);
        Ok(())
    }

    /// Writes `rows` to the stash.
    ///
    /// Every row given is written. Stopping where the peek's finishing has all it can use is the
    /// scan's to decide, since it is the scan that holds the finishing and produces the rows.
    ///
    /// Fails where persist rejects the write, which leaves the upload unusable and the rows it
    /// holds unanswerable.
    pub(super) async fn push(&mut self, rows: RowBatch) -> Result<(), StashError> {
        for (row, diff) in rows {
            self.num_rows += u64::from(NonZeroU64::try_from(diff).expect("diff fits into u64"));
            self.write(&SourceData(Ok(row)), Timestamp::default(), diff.into())
                .await?;
        }

        Ok(())
    }

    /// Finishes the batch and builds the response that names it.
    ///
    /// Every row of the answer is in the batch. The response's `inline_rows` are left for the
    /// controller, which merges in the rows of workers whose share never reached the stash.
    ///
    /// Fails where persist rejects the batch, which takes the parts with it: persist keeps the
    /// builder and hands back no handle to what it holds. Only a batch whose bounds do not admit
    /// its own updates is refused, which an upload opened by [`StashUpload::open`] cannot
    /// produce.
    pub(super) async fn finish(mut self) -> Result<PeekResponse, StashError> {
        let delivered = self.finish_batch().await?;
        let batch = delivered.take();

        let stashed_response = StashedPeekResponse {
            num_rows_batches: self.num_rows,
            encoded_size_bytes: batch.encoded_size_bytes(),
            relation_desc: self.relation_desc.clone(),
            shard_id: self.shard_id,
            batches: vec![batch.into_transmittable_batch()],
            inline_rows: Vec::new(),
        };
        Ok(PeekResponse::Stashed(Box::new(stashed_response)))
    }

    /// Finishes the batch and hands it to the caller, or returns `None` when no update was pushed.
    ///
    /// The caller owns the batch and must transmit or delete it, since a batch dropped without
    /// either leaves its blobs behind.
    ///
    /// Fails where persist rejects the batch, which takes the parts with it, as for
    /// [`StashUpload::finish`]. That includes a batch holding a time at or beyond the upper.
    #[cfg_attr(not(test), expect(dead_code, reason = "no caller outside tests"))]
    pub(crate) async fn finish_stashed(
        mut self,
    ) -> Result<Option<Batch<SourceData, (), Timestamp, i64>>, StashError> {
        if !self.wrote_updates {
            // Nothing was written, so the builder holds no part and dropping it reclaims all.
            drop(self.batch_builder.take());
            return Ok(None);
        }
        Ok(Some(self.finish_batch().await?.take()))
    }

    /// Finishes the batch as work of its own, and leaves the upload holding no parts.
    ///
    /// Flushing the buffered part and the uploads still in flight is the longest await an upload
    /// makes, so a cancellation most likely lands there, with the builder already out of the
    /// upload. A task the cancellation cannot reach holds it instead, and whoever ends up holding a
    /// batch nobody will read deletes it.
    async fn finish_batch(&mut self) -> Result<DeliveredBatch, StashError> {
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
                        .map_err(StashError::FinishBatch);
                    // A send that finds no receiver hands the delivery straight back, and dropping it
                    // here deletes the batch. An error nobody is left to read is simply dropped.
                    let _undelivered = tx.send(delivered);
                });

        // The task is detached rather than held, so it outlives this await and the only way the
        // channel closes without a delivery is a runtime that is going away.
        rx.await.map_err(|_| StashError::LostFinishTask)?
    }

    /// Schedules the deletion of whatever parts the upload still holds, and leaves it holding
    /// none.
    ///
    /// Persist hands back a deletable handle only by finishing the batch, so the buffered rows are
    /// written out first. That write reaches `persist_blob_target_size`, 128 MiB by default, and
    /// nothing bounds how many abandoned uploads carry one at once, because the walk releases its
    /// permit before this finishes.
    ///
    /// The handle does not reach parts a run merge already dropped from shard state. A reader
    /// deleting a response it has finished with reaches the same set, so that bounds the builder
    /// rather than abandonment.
    ///
    /// TODO: a builder teardown that surrendered the written parts without flushing the buffered
    /// one would make this cost a delete and nothing else.
    fn abandon(&mut self) {
        let Some(batch_builder) = self.batch_builder.take() else {
            return;
        };

        // An upload persist never took a part off holds its rows in memory alone, so its builder
        // goes with it. Finishing here would upload the buffered part just to delete it again,
        // and a part reaches blob storage only once the buffer passes
        // `persist_blob_target_size`, which is far above the stash threshold that opened this
        // upload. So this is the case nearly every abandoned upload is in.
        if !self.wrote_parts {
            drop(batch_builder);
            return;
        }

        let upper = self.upper.clone();
        let shard_id = self.shard_id;

        // Scheduled, not awaited: the caller that matters most cannot await at all. Cancelling a
        // peek aborts the walk driving it, and an aborted task is dropped rather than polled again,
        // so the deletion reaches blob storage only as work outside that task. Entering the runtime
        // explicitly lets this run from a `Drop`, which has no guaranteed runtime context.
        //
        // NOTE: this reaches only an upload a live replica gives up on. A replica that dies
        // mid-upload, or one whose runtime is shutting down, leaves the parts behind for a
        // reader-side sweep or persist's garbage collection.
        let _handle =
            self.runtime
                .spawn_named(|| format!("peek_stash::discard({shard_id})"), async move {
                    match batch_builder.finish(upper).await {
                        Ok(batch) => batch.delete().await,
                        Err(error) => {
                            warn!(%shard_id, %error, "peek stash cannot delete an abandoned batch")
                        }
                    }
                });
    }
}

impl Drop for StashUpload {
    /// Deletes the parts of an upload that ends without [`StashUpload::finish`], which is the
    /// only cleanup a walk aborted mid-upload gets.
    fn drop(&mut self) {
        self.abandon();
    }
}

/// A finished batch on its way to the response that will name it, deleted from blob storage if it
/// never arrives.
///
/// A batch nobody takes out of a delivery is one no reader will be told how to find, so dropping it
/// deletes it. That covers both ways a delivery goes unclaimed: a send that finds no receiver, and
/// a receiver dropped between the send and the take. Persist's own `Drop for Batch` covers neither,
/// logging the blob keys and leaving them.
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
    /// The caller takes over the obligation: a batch dropped without being transmitted or deleted
    /// leaves its blobs behind.
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
/// A driver holds a target, not an open upload: a walk that never crosses the stash threshold opens
/// no shard and writes no byte, and most walks never do. A driver gets one exactly when its scan was
/// opened stash-eligible, so a walk with no target has a scan that offers no batch.
#[derive(Clone)]
pub(super) struct StashTarget {
    persist_clients: Arc<PersistClientCache>,
    persist_location: PersistLocation,
    /// The peek's uuid, which the shard the batch belongs to is derived from.
    peek_uuid: Uuid,
    /// The description the rows are written under, and the one the response reports.
    relation_desc: RelationDesc,
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
        }
    }

    /// Opens the upload, whose batch builder holds at most `batch_max_runs` runs.
    pub(super) async fn open(&self, batch_max_runs: usize) -> Result<StashUpload, StashError> {
        StashUpload::open(
            &self.persist_clients,
            self.persist_location.clone(),
            batch_max_runs,
            self.peek_uuid,
            self.relation_desc.clone(),
        )
        .await
    }

    /// Deletes the batches `response` names if it is stashed, and drops anything else.
    ///
    /// Returns at once. The deletion runs as a task of its own, so it completes even when the
    /// caller's task is aborted right after, which is how a cancelled peek's walk ends.
    pub(super) fn discard(&self, response: PeekResponse) {
        let PeekResponse::Stashed(stashed) = response else {
            return;
        };
        let StashedPeekResponse {
            shard_id, batches, ..
        } = *stashed;
        let persist_clients = Arc::clone(&self.persist_clients);
        let persist_location = self.persist_location.clone();
        let _handle = mz_ore::task::spawn(
            || format!("peek_stash::discard_response({shard_id})"),
            async move {
                match persist_clients.open(persist_location).await {
                    Ok(client) => drop(OwnedStash::from_proto(&client, shard_id, batches)),
                    Err(error) => {
                        warn!(%shard_id, %error, "peek stash cannot delete a discarded response")
                    }
                }
            },
        );
    }
}

/// Tests of the incremental stash upload, over the persist location a replica would write to.
///
/// [`tests::stashed_rows`] is shared with the drivers that feed an upload, which read a response
/// back the same way.
#[cfg(test)]
pub(crate) mod tests;
