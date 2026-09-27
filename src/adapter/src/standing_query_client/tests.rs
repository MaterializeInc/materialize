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
use mz_persist_client::{Diagnostics, PersistClient, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, RelationDesc, SqlScalarType};

use super::*;

fn param_desc() -> RelationDesc {
    RelationDesc::builder()
        .with_column("request_id", SqlScalarType::UInt64.nullable(false))
        .with_column("cid", SqlScalarType::Int32.nullable(true))
        .finish()
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
        write_handle,
        flush_tx,
        advance_upper_rx,
    );
    (client, persist, shard_id, flush_rx, advance_upper_tx)
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

/// The param rows live at `as_of`, consolidated.
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
async fn each_write_retracts_its_rows_in_the_same_append() {
    let (client, persist, shard_id, mut flush_rx, _advance_upper_tx) =
        client_with_shard(Some(Timestamp::new(10))).await;

    execute(&client, 42);
    let flush = flush_rx.recv().await.expect("batcher running");
    let write_ts = flush.write_ts;

    // Nothing else writes the shard, so its upper shows what the append
    // covered.
    assert_eq!(
        upper(&persist, shard_id).await,
        write_ts.step_forward().step_forward()
    );
    assert_eq!(
        live_rows(&persist, shard_id, write_ts).await,
        vec![Row::pack_slice(&[
            Datum::UInt64(flush.request_ids[0]),
            Datum::Int32(42)
        ])],
        "the param row is live at its write timestamp",
    );
    assert_eq!(
        live_rows(&persist, shard_id, write_ts.step_forward()).await,
        Vec::<Row>::new(),
        "the append that inserted the param row also retracted it",
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
