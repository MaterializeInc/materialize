// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tests of the upload that writes a peek's answer to the peek response stash.

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
/// Nothing an upload writes is inlined into shard state, where no blob counter would see it,
/// which is what makes "the parts an upload wrote" something a test can observe at all. How
/// soon a part gets there is the constructors' difference. The counts are read off the registry
/// because persist keeps its own metric handles private.
pub(crate) struct CountedBlob {
    registry: MetricsRegistry,
    clients: Arc<PersistClientCache>,
}

impl CountedBlob {
    /// A cache in which every row an upload takes becomes a part in blob storage.
    pub(crate) fn new() -> Self {
        Self::with_part_size(Some(0))
    }

    /// A cache that leaves persist's part size where production has it, so the rows an upload
    /// takes stay buffered in its builder and reach blob storage only if a batch is finished.
    pub(crate) fn with_buffered_parts() -> Self {
        Self::with_part_size(None)
    }

    fn with_part_size(blob_target_size: Option<usize>) -> Self {
        let cfg = PersistConfig::new_for_tests();

        let mut updates = ConfigUpdates::default();
        let part_size = blob_target_size
            .map(|size| ("persist_blob_target_size", ConfigVal::Usize(size)))
            .into_iter();
        for (name, val) in part_size.chain([
            // Without this persist keeps a small part in shard state, where no blob counter
            // sees it and no delete has anything to remove.
            (
                "persist_inline_writes_single_max_bytes",
                ConfigVal::Usize(0),
            ),
            // A part per row would otherwise hold the builder in its outstanding-write stall,
            // so raise the bound past the parts a test produces.
            (
                "persist_batch_builder_max_outstanding_parts",
                ConfigVal::Usize(8_192),
            ),
        ]) {
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

/// An upload opened against `clients`.
async fn open_upload(clients: &PersistClientCache) -> StashUpload {
    open_upload_with_runs(clients, *PEEK_RESPONSE_STASH_BATCH_MAX_RUNS.default()).await
}

/// An upload whose batch builder holds at most `batch_max_runs` runs.
///
/// A builder that has more runs than that merges them, which writes parts of its own and
/// leaves the parts it merged from behind. A test counting what an upload wrote raises the
/// limit above the runs it produces, so that the parts it counts are the parts the upload owns.
async fn open_upload_with_runs(clients: &PersistClientCache, batch_max_runs: usize) -> StashUpload {
    StashUpload::open(
        clients,
        PersistLocation::new_in_mem(),
        batch_max_runs,
        Uuid::new_v4(),
        result_desc(),
    )
    .await
    .expect("the in-memory location opens")
}

/// Writes `rows` to `upload`, which persist accepts for every batch built here.
async fn push(upload: &mut StashUpload, rows: RowBatch) {
    upload.push(rows).await.expect("persist takes the rows")
}

/// Finishes `upload`, which persist accepts for every batch built here.
async fn finish(upload: StashUpload) -> PeekResponse {
    upload.finish().await.expect("persist finishes the batch")
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
    let mut upload = open_upload(&clients).await;

    push(&mut upload, batch(0..3, 1)).await;
    push(&mut upload, batch(3..5, 1)).await;

    let response = finish(upload).await;
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

/// An upload writes every row it is given, whole, however often the answer holds it. Where the
/// finishing's limit lands is the scan's to decide, not the upload's.
#[mz_ore::test(tokio::test)]
async fn an_upload_writes_every_row_it_is_given() {
    let clients = PersistClientCache::new_no_metrics();
    let mut upload = open_upload(&clients).await;

    push(&mut upload, batch(0..2, 3)).await;

    assert_eq!(
        stashed_values(&clients, finish(upload).await).await,
        vec![0, 0, 0, 1, 1, 1],
    );
}

/// An upload built by hand, so that a test can hold the bounds [`StashUpload::open`] never
/// produces.
///
/// Opening fixes the lower, the upper and the timestamp every row is written at, and that
/// combination is one persist always accepts. The rejection arms exist all the same, because
/// the alternative to reporting a rejection is a panic in an offloaded walk, which lands in the
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
        num_rows: 0,
        wrote_parts: false,
        runtime: Handle::current(),
    }
}

/// An upload that is dropped deletes the parts it had already written.
///
/// Dropping is the whole of the cleanup, both for a driver that will answer the peek with
/// something other than these rows and for a walk aborted mid-upload: an aborted task is
/// dropped rather than polled again, so nothing it would have called runs.
#[mz_ore::test(tokio::test)]
async fn a_dropped_upload_deletes_the_parts_it_wrote() {
    let blob = CountedBlob::new();
    let mut upload = open_upload_with_runs(blob.clients(), NO_RUN_MERGING).await;

    push(&mut upload, batch(0..4, 1)).await;
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

/// An upload persist has taken no part off costs no blob traffic to give up.
///
/// This is the case nearly every abandoned upload is in, because a part reaches blob storage
/// only once the builder's buffer passes `persist_blob_target_size`, far above the stash
/// threshold that opens an upload at all. Writing the buffered part out just to delete it again
/// would make giving up cost two round trips per abandoned peek.
#[mz_ore::test(tokio::test)]
async fn a_dropped_upload_holding_no_part_writes_none() {
    let blob = CountedBlob::with_buffered_parts();
    let mut upload = open_upload_with_runs(blob.clients(), NO_RUN_MERGING).await;

    push(&mut upload, batch(0..4, 1)).await;
    assert_eq!(blob.written(), 0, "four rows stay in the builder");

    drop(upload);

    // Given the same turns an abandonment that does write gets, so a regression that finishes
    // the batch here fails rather than racing.
    for _ in 0..BLOB_TURNS {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    assert_eq!(
        blob.written(),
        0,
        "giving up an upload holding no part must write none"
    );
    assert_eq!(blob.deleted(), 0, "and must delete nothing");
}

/// A finished batch that never reaches the response naming it is deleted.
///
/// This is the window between persist handing the batch over and the response taking it: a
/// cancellation that lands there leaves a batch no reader will ever be told how to find, and
/// persist's own `Drop for Batch` logs the keys and leaves them.
#[mz_ore::test(tokio::test)]
async fn a_finished_batch_that_reaches_no_response_is_deleted() {
    let blob = CountedBlob::new();
    let mut upload = open_upload_with_runs(blob.clients(), NO_RUN_MERGING).await;

    push(&mut upload, batch(0..4, 1)).await;
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
    let mut upload = open_upload_with_runs(blob.clients(), NO_RUN_MERGING).await;

    push(&mut upload, batch(0..4, 1)).await;
    let response = finish(upload).await;

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
/// instead end the task driving an offloaded walk, and the worker reads a dead task as a defect.
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
        matches!(&rejection, Err(StashError::WriteRow(error)) if error.to_string().contains("not beyond batch lower")),
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
    let mut upload = upload_with_bounds(&clients, Timestamp::default(), Timestamp::default()).await;

    push(&mut upload, batch(0..2, 1)).await;
    let rejection = upload.finish().await;

    assert!(
        matches!(&rejection, Err(StashError::FinishBatch(error)) if error.to_string().contains("beyond the expected batch upper")),
        "persist must reject the batch and the upload must report it: {rejection:?}",
    );
}
