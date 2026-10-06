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

use crate::Diagnostics;
use crate::ShardId;
use crate::batch::{
    BLOB_TARGET_SIZE, INLINE_WRITES_SINGLE_MAX_BYTES, INLINE_WRITES_TOTAL_MAX_BYTES,
};
use crate::fetch::PART_DECODE_BATCH_ROWS;
use crate::tests::new_test_client;

type Update = ((String, String), u64, i64);

fn update(kv: &str, t: u64, d: i64) -> Update {
    ((kv.to_owned(), kv.to_owned()), t, d)
}

/// Reads a single hollow part, with and without batched decoding, through
/// both `snapshot_and_stream` and the `BatchFetcher`/`FetchedBlob` path used by
/// `persist_source`.
#[mz_persist_proc::test(tokio::test)]
#[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
async fn batched_part_decode(dyncfgs: ConfigUpdates) {
    let client = new_test_client(&dyncfgs).await;
    client.cfg.set_config(&INLINE_WRITES_SINGLE_MAX_BYTES, 0);
    client.cfg.set_config(&INLINE_WRITES_TOTAL_MAX_BYTES, 0);
    // The test client splits parts at a tiny target size. Keep all rows in
    // one part.
    client.cfg.set_config(&BLOB_TARGET_SIZE, 1 << 20);
    let shard_id = ShardId::new();
    let (mut write, mut read) = client
        .expect_open::<String, String, u64, i64>(shard_id)
        .await;

    // Ten rows in one part. Reading as of 1 advances time 0 to 1, so with
    // batches of 3 rows, rows 2 and 3 (`c`) consolidate across a batch
    // boundary, and rows 5 and 6 (`e`) cancel across one.
    let data = [
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
    ];
    write.expect_compare_and_append(&data, 0, 2).await;
    let expected = vec![
        update("a", 1, 1),
        update("b", 1, 1),
        update("c", 1, 2),
        update("d", 1, 1),
        update("f", 1, 1),
        update("g", 1, 1),
        update("h", 1, 1),
    ];
    let as_of = Antichain::from_elem(1u64);

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

    for batch_rows in [0, 1, 3, 4, 10, 16384] {
        client.cfg.set_config(&PART_DECODE_BATCH_ROWS, batch_rows);

        let mut stream = pin::pin!(read.snapshot_and_stream(as_of.clone()).await.unwrap());
        let mut actual = vec![];
        while let Some(update) = stream.next().await {
            actual.push(update);
        }
        assert_eq!(
            actual, expected,
            "snapshot_and_stream, batch_rows={batch_rows}"
        );

        let parts = read.snapshot(as_of.clone()).await.expect("as_of available");
        assert_eq!(parts.len(), 1, "expected a single part");
        let mut actual = vec![];
        for part in parts {
            assert!(
                matches!(part.part, crate::internal::state::BatchPart::Hollow(_)),
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
            "FetchedBlob::parse, batch_rows={batch_rows}"
        );
    }
}
