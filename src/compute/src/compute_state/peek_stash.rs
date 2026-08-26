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
use mz_persist_client::Schemas;
use mz_persist_client::batch::BatchBuilder;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{RelationDesc, Timestamp};
use mz_storage_types::sources::SourceData;
use timely::progress::Antichain;
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
/// point where the peek's finishing has all it can use. An upload dropped without
/// [`StashUpload::finish`] leaves the parts it has already written to blob storage behind.
///
/// Every write reports its failure rather than raising it. The unit that fails is the peek, whose
/// driver answers it with the error, so a rejected write costs one query its answer instead of
/// costing the walk that produced it its task.
pub(super) struct StashUpload {
    /// The description the stashed response reports, and the schema the batch is written under.
    relation_desc: RelationDesc,
    /// The shard the batch belongs to, derived from the peek's uuid so that a reader holding the
    /// response can find it.
    shard_id: ShardId,
    batch_builder: BatchBuilder<SourceData, (), Timestamp, i64>,
    /// The upper the batch is finished at, one step beyond the timestamp every row is written at.
    upper: Antichain<Timestamp>,
    /// The number of rows the peek's finishing can use, its offset plus its limit, or `None` for a
    /// finishing that can use every row the peek produces.
    max_rows: Option<usize>,
    /// Rows written so far, counting a row with a diff of `n` as `n` rows, which is how the
    /// finishing counts them.
    num_rows: u64,
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
            batch_builder,
            upper,
            max_rows,
            num_rows: 0,
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
    pub(super) async fn finish(self, inline_rows: RowBatch) -> Result<PeekResponse, String> {
        let batch = self
            .batch_builder
            .finish(self.upper)
            .await
            .map_err(|err| err.to_string())?;

        let inline_rows = inline_rows
            .into_iter()
            .map(|(row, copies)| {
                let copies = NonZeroUsize::try_from(copies).expect("fits into usize");
                (row, copies)
            })
            .collect();

        let stashed_response = StashedPeekResponse {
            num_rows_batches: self.num_rows,
            encoded_size_bytes: batch.encoded_size_bytes(),
            relation_desc: self.relation_desc,
            shard_id: self.shard_id,
            batches: vec![batch.into_transmittable_batch()],
            inline_rows: vec![RowCollection::new(inline_rows, &[])],
        };
        Ok(PeekResponse::Stashed(Box::new(stashed_response)))
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

    use mz_compute_types::dyncfgs::{
        PEEK_RESPONSE_STASH_BATCH_MAX_RUNS, PEEK_RESPONSE_STASH_READ_MEMORY_BUDGET_BYTES,
    };
    use mz_repr::{Datum, Row, SqlScalarType};

    use super::*;

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
        StashUpload::open(
            clients,
            PersistLocation::new_in_mem(),
            *PEEK_RESPONSE_STASH_BATCH_MAX_RUNS.default(),
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
}
