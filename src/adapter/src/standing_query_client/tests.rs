// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::consolidation::consolidate_updates;
use mz_catalog::memory::objects::StandingQuery;
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, RelationDesc, SqlScalarType};

use super::*;

fn param_desc() -> RelationDesc {
    StandingQuery::build_param_collection_desc(&[("cid".into(), SqlScalarType::Int32)])
}

/// The param row of request `request_id` with parameter `cid`, written at `write_ts`.
fn param_row(request_id: u64, cid: i32, write_ts: Timestamp) -> Row {
    Row::pack_slice(&[
        Datum::UInt64(request_id),
        Datum::Int32(cid),
        Datum::MzTimestamp(write_ts),
    ])
}

/// A standing query client over a fresh in-memory param shard.
async fn client_with_shard(
    initial_target: Option<Timestamp>,
) -> (
    StandingQueryExecuteClient,
    PersistClient,
    ShardId,
    mpsc::UnboundedReceiver<StandingQueryFlush>,
    watch::Sender<Option<Timestamp>>,
) {
    let persist = PersistClient::new_for_tests().await;
    let shard_id = ShardId::new();
    let (client, flush_rx, advance_upper_tx) =
        client_on_shard(&persist, shard_id, initial_target).await;
    (client, persist, shard_id, flush_rx, advance_upper_tx)
}

/// A standing query client over the param shard `shard_id`.
async fn client_on_shard(
    persist: &PersistClient,
    shard_id: ShardId,
    initial_target: Option<Timestamp>,
) -> (
    StandingQueryExecuteClient,
    mpsc::UnboundedReceiver<StandingQueryFlush>,
    watch::Sender<Option<Timestamp>>,
) {
    let write_handle = persist
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            shard_id,
            Arc::new(param_desc()),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
        )
        .await
        .expect("valid usage");
    let (flush_tx, flush_rx) = mpsc::unbounded_channel();
    let (advance_upper_tx, advance_upper_rx) = watch::channel(initial_target);
    let client = StandingQueryExecuteClient::new(
        CatalogItemId::User(1),
        GlobalId::User(1),
        persist.clone(),
        param_desc(),
        write_handle,
        flush_tx,
        advance_upper_rx,
    );
    (client, flush_rx, advance_upper_tx)
}

/// Starts an execution with parameter `cid` and leaves it waiting for results.
fn execute(client: &StandingQueryExecuteClient, cid: i32) {
    let client = client.clone();
    mz_ore::task::spawn(|| "standing-query-test-execute", async move {
        let params = [(Row::pack_slice(&[Datum::Int32(cid)]), SqlScalarType::Int32)];
        let _ = client.execute(&params, None).await;
    });
}

async fn upper(persist: &PersistClient, shard_id: ShardId) -> Timestamp {
    let mut write_handle = persist
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            shard_id,
            Arc::new(param_desc()),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
        )
        .await
        .expect("valid usage");
    write_handle
        .fetch_recent_upper()
        .await
        .as_option()
        .copied()
        .expect("shard not closed")
}

/// Waits until the upper of `shard_id` reaches `target`, and panics after a minute.
async fn wait_for_upper(persist: &PersistClient, shard_id: ShardId, target: Timestamp) {
    let wait = async {
        while upper(persist, shard_id).await < target {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(60), wait)
        .await
        .unwrap_or_else(|_| panic!("the upper of {shard_id} did not reach {target}"));
}

/// The param rows the shard holds at `as_of`, consolidated.
async fn live_rows(persist: &PersistClient, shard_id: ShardId, as_of: Timestamp) -> Vec<Row> {
    let mut read_handle = persist
        .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
            shard_id,
            Arc::new(param_desc()),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
            false,
        )
        .await
        .expect("valid usage");
    let mut updates: Vec<_> = read_handle
        .snapshot_and_fetch(Antichain::from_elem(as_of))
        .await
        .expect("as_of not compacted")
        .into_iter()
        .map(|((data, ()), ts, diff)| (data.0.expect("no errors"), ts, diff))
        .collect();
    consolidate_updates(&mut updates);
    updates
        .into_iter()
        .map(|(row, _ts, diff)| {
            assert_eq!(diff, 1, "param rows have multiplicity one");
            row
        })
        .collect()
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn a_write_consumes_one_timestamp_and_carries_it() {
    let (client, persist, shard_id, mut flush_rx, _advance_upper_tx) =
        client_with_shard(Some(Timestamp::new(10))).await;

    execute(&client, 42);
    let flush = flush_rx.recv().await.expect("batcher running");
    let write_ts = flush.write_ts;

    // Nothing else writes the shard, so its upper shows what the append
    // covered.
    assert_eq!(upper(&persist, shard_id).await, write_ts.step_forward());
    assert_eq!(
        live_rows(&persist, shard_id, write_ts).await,
        vec![param_row(flush.request_ids[0], 42, write_ts)],
        "the param row carries its write timestamp",
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn a_write_retracts_the_previous_writes_rows() {
    let (client, persist, shard_id, mut flush_rx, advance_upper_tx) =
        client_with_shard(Some(Timestamp::new(10))).await;

    execute(&client, 42);
    let first = flush_rx.recv().await.expect("batcher running");
    execute(&client, 43);
    let second = flush_rx.recv().await.expect("batcher running");
    assert_eq!(
        second.write_ts,
        first.write_ts.step_forward(),
        "nothing else advanced the upper between the writes",
    );

    let second_row = param_row(second.request_ids[0], 43, second.write_ts);
    assert_eq!(
        live_rows(&persist, shard_id, second.write_ts).await,
        vec![second_row],
        "the second write retracted the first write's row at its own timestamp",
    );

    // An idle batcher retracts the last write's rows when it advances the upper.
    let target = Timestamp::new(100);
    advance_upper_tx
        .send(Some(target))
        .expect("batcher running");
    wait_for_upper(&persist, shard_id, target).await;
    assert_eq!(
        live_rows(&persist, shard_id, target.step_back().expect("not minimum")).await,
        Vec::<Row>::new(),
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn a_new_batcher_retracts_leftovers_in_its_first_append() {
    let (client, persist, shard_id, mut flush_rx, advance_upper_tx) =
        client_with_shard(Some(Timestamp::new(10))).await;
    execute(&client, 42);
    let flush = flush_rx.recv().await.expect("batcher running");
    let leftover = param_row(flush.request_ids[0], 42, flush.write_ts);
    // Stop the batcher before it retracts the row, as a crash would.
    drop(advance_upper_tx);
    drop(client);
    let crash_upper = upper(&persist, shard_id).await;
    assert_eq!(
        live_rows(
            &persist,
            shard_id,
            crash_upper.step_back().expect("written")
        )
        .await,
        vec![leftover],
    );

    let target = Timestamp::new(100);
    let (_client, _flush_rx, _advance_upper_tx) =
        client_on_shard(&persist, shard_id, Some(target)).await;
    // The first append the new batcher makes advances the upper to `target`.
    wait_for_upper(&persist, shard_id, target).await;
    assert_eq!(
        live_rows(&persist, shard_id, target.step_back().expect("not minimum")).await,
        Vec::<Row>::new(),
        "the new batcher's first append retracted the leftover",
    );
}

/// Another process, such as a previous leader that has not been terminated
/// yet, appends to the shard between two of this batcher's writes.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn a_write_moves_past_a_concurrent_append() {
    let (client, persist, shard_id, mut flush_rx, _advance_upper_tx) =
        client_with_shard(Some(Timestamp::new(10))).await;
    execute(&client, 42);
    let first = flush_rx.recv().await.expect("batcher running");

    let other_upper = upper(&persist, shard_id).await;
    let mut other = persist
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            shard_id,
            Arc::new(param_desc()),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
        )
        .await
        .expect("valid usage");
    other
        .compare_and_append(
            std::iter::empty::<((SourceData, ()), Timestamp, StorageDiff)>(),
            Antichain::from_elem(other_upper),
            Antichain::from_elem(other_upper.step_forward()),
        )
        .await
        .expect("valid usage")
        .expect("upper matches");

    execute(&client, 43);
    let second = tokio::time::timeout(Duration::from_secs(60), flush_rx.recv())
        .await
        .expect("the batcher survives the upper mismatch")
        .expect("batcher running");
    assert_eq!(second.write_ts, other_upper.step_forward());
    assert_eq!(
        live_rows(&persist, shard_id, second.write_ts).await,
        vec![param_row(second.request_ids[0], 43, second.write_ts)],
        "the retried write carries its actual timestamp and retracts the first write's row",
    );
    assert_ne!(first.write_ts, second.write_ts);
}

/// A previous leader that is still running when this process starts, and
/// retracts its own rows before this batcher becomes writable.
#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn leftovers_retracted_meanwhile_are_not_retracted_again() {
    let (client, persist, shard_id, mut flush_rx, advance_upper_tx) =
        client_with_shard(Some(Timestamp::new(10))).await;
    execute(&client, 42);
    let flush = flush_rx.recv().await.expect("batcher running");
    let leftover = param_row(flush.request_ids[0], 42, flush.write_ts);
    drop(advance_upper_tx);
    drop(client);

    // A read-only batcher starts, and reads the upper while the row is live.
    let (_client, _flush_rx, new_advance_upper_tx) =
        client_on_shard(&persist, shard_id, None).await;

    // The previous leader retracts its row.
    let leader_upper = upper(&persist, shard_id).await;
    let mut leader = persist
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            shard_id,
            Arc::new(param_desc()),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
        )
        .await
        .expect("valid usage");
    leader
        .compare_and_append(
            [((SourceData(Ok(leftover)), ()), leader_upper, -1)],
            Antichain::from_elem(leader_upper),
            Antichain::from_elem(leader_upper.step_forward()),
        )
        .await
        .expect("valid usage")
        .expect("upper matches");

    let target = Timestamp::new(100);
    new_advance_upper_tx
        .send(Some(target))
        .expect("batcher running");
    wait_for_upper(&persist, shard_id, target).await;
    // `live_rows` also asserts that no row has a negative multiplicity.
    assert_eq!(
        live_rows(&persist, shard_id, target.step_back().expect("not minimum")).await,
        Vec::<Row>::new(),
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
async fn batcher_without_upper_target_does_not_write() {
    let (client, persist, shard_id, mut flush_rx, advance_upper_tx) = client_with_shard(None).await;
    let initial_upper = upper(&persist, shard_id).await;

    execute(&client, 42);
    let flush = tokio::time::timeout(Duration::from_millis(200), flush_rx.recv()).await;
    assert!(flush.is_err(), "a read-only batcher wrote {flush:?}");
    assert_eq!(upper(&persist, shard_id).await, initial_upper);

    advance_upper_tx
        .send(Some(Timestamp::new(10)))
        .expect("batcher running");
    let flush = flush_rx.recv().await.expect("batcher running");
    assert!(
        flush.write_ts >= Timestamp::new(10),
        "the parked request is written once the batcher has a target"
    );
}
