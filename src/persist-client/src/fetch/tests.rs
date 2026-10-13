// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin;

use futures::StreamExt;
use mz_dyncfg::ConfigUpdates;
use timely::progress::Antichain;

use crate::batch::{
    BLOB_TARGET_SIZE, INLINE_WRITES_SINGLE_MAX_BYTES, INLINE_WRITES_TOTAL_MAX_BYTES,
};
use crate::fetch::PART_DECODE_BATCH_ROWS;
use crate::internal::state::BatchPart;
use crate::read::ReadHandle;
use crate::tests::new_test_client;
use crate::{Diagnostics, PersistClient, ShardId};

type Update = ((String, String), u64, i64);

fn update(kv: &str, t: u64, d: i64) -> Update {
    ((kv.to_owned(), kv.to_owned()), t, d)
}

/// Ten rows for one part. Read as of a time at or past 1, with batches of 3
/// rows, rows 2 and 3 (`c`) consolidate across a batch boundary, and rows 5
/// and 6 (`e`) cancel across one.
fn data() -> Vec<Update> {
    vec![
        update("a", 1, 1),
        update("b", 1, 1),
        update("c", 0, 1),
        update("c", 1, 1),
        update("d", 1, 1),
        update("e", 0, 1),
        update("e", 1, -1),
        update("f", 1, 1),
        update("g", 1, 1),
        update("h", 1, 1),
    ]
}

/// [`data`] read as of `t`, with all updates advanced to `t`.
fn expected(t: u64) -> Vec<Update> {
    vec![
        update("a", t, 1),
        update("b", t, 1),
        update("c", t, 2),
        update("d", t, 1),
        update("f", t, 1),
        update("g", t, 1),
        update("h", t, 1),
    ]
}

/// A test client that writes each batch below as a single hollow part.
async fn single_part_client(dyncfgs: &ConfigUpdates) -> PersistClient {
    let client = new_test_client(dyncfgs).await;
    client.cfg.set_config(&INLINE_WRITES_SINGLE_MAX_BYTES, 0);
    client.cfg.set_config(&INLINE_WRITES_TOTAL_MAX_BYTES, 0);
    // The test client splits parts at a tiny target size.
    client.cfg.set_config(&BLOB_TARGET_SIZE, 1 << 20);
    client
}

/// Reads the shard's single hollow part as of `as_of`, with and without
/// batched decoding, through both `snapshot_and_stream` and the
/// `BatchFetcher`/`FetchedBlob` path used by `persist_source`.
async fn assert_batched_reads(
    client: &PersistClient,
    shard_id: ShardId,
    read: &mut ReadHandle<String, String, u64, i64>,
    as_of: u64,
    expected: &[Update],
) {
    let as_of = Antichain::from_elem(as_of);
    let mut fetcher = client
        .create_batch_fetcher::<String, String, u64, i64>(
            shard_id,
            Default::default(),
            Default::default(),
            false,
            Diagnostics::for_tests(),
        )
        .await
        .expect("valid usage");

    for batch_rows in [
        None,
        Some(0),
        Some(1),
        Some(3),
        Some(4),
        Some(10),
        Some(16384),
    ] {
        client.cfg.set_config(&PART_DECODE_BATCH_ROWS, batch_rows);

        let mut stream = pin::pin!(read.snapshot_and_stream(as_of.clone()).await.unwrap());
        let mut actual = vec![];
        while let Some(update) = stream.next().await {
            actual.push(update);
        }
        assert_eq!(
            actual, expected,
            "snapshot_and_stream, batch_rows={batch_rows:?}"
        );

        let parts = read.snapshot(as_of.clone()).await.expect("as_of available");
        assert_eq!(parts.len(), 1, "expected a single part");
        let mut actual = vec![];
        for part in parts {
            assert!(
                matches!(part.part, BatchPart::Hollow(_)),
                "expected a hollow part"
            );
            let (part, _lease) = part.into_exchangeable_part();
            let blob = fetcher
                .fetch_leased_part(part)
                .await
                .expect("valid usage")
                .expect("blob present");
            actual.extend(blob.parse().part);
        }
        assert_eq!(
            actual, expected,
            "FetchedBlob::parse, batch_rows={batch_rows:?}"
        );
    }
}

#[mz_persist_proc::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
async fn batched_part_decode(dyncfgs: ConfigUpdates) {
    let client = single_part_client(&dyncfgs).await;
    let shard_id = ShardId::new();
    let (mut write, mut read) = client
        .expect_open::<String, String, u64, i64>(shard_id)
        .await;
    write.expect_compare_and_append(&data(), 0, 2).await;

    assert_batched_reads(&client, shard_id, &mut read, 1, &expected(1)).await;
}

/// A part whose registered description differs from its inline one, so
/// every batch is truncated and has its timestamps rewritten.
#[mz_persist_proc::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
async fn batched_part_decode_ts_rewrite(dyncfgs: ConfigUpdates) {
    let client = single_part_client(&dyncfgs).await;
    let shard_id = ShardId::new();
    let (mut write, mut read) = client
        .expect_open::<String, String, u64, i64>(shard_id)
        .await;

    let mut builder = write.builder(Antichain::from_elem(0));
    for ((k, v), t, d) in data() {
        builder.add(&k, &v, &t, &d).await.expect("valid usage");
    }
    let batch = builder
        .finish(Antichain::from_elem(2))
        .await
        .expect("valid usage");
    let batch = batch.into_transmittable_batch();
    let mut batch = write.batch_from_transmittable_batch(batch);
    batch
        .rewrite_ts(&Antichain::from_elem(2), Antichain::from_elem(3))
        .expect("valid rewrite");
    write
        .expect_compare_and_append_batch(&mut [&mut batch], 0, 3)
        .await;

    assert_batched_reads(&client, shard_id, &mut read, 2, &expected(2)).await;
}
