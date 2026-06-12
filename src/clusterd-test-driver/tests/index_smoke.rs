// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! End-to-end smoke test: import a small persist shard and arrange it into an
//! index, then peek the result.
//!
//! This test only runs when `CLUSTERD_COMPUTE_ADDR` is set (see
//! [`mz_clusterd_test_driver::target::e2e_enabled`]); otherwise it returns early so
//! that it compiles and passes by skipping. It is exercised against real infra by
//! the mzcompose stack.

use std::net::SocketAddr;
use std::time::Duration;

use mz_clusterd_test_driver::data::{sample_desc, sample_rows, write_rows_single_ts};
use mz_clusterd_test_driver::dataflow::index_dataflow;
use mz_clusterd_test_driver::driver::Driver;
use mz_clusterd_test_driver::persist_host::PersistHost;
use mz_clusterd_test_driver::target;
use mz_persist_types::{PersistLocation, ShardId};
use mz_repr::{GlobalId, Timestamp};

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_over_small_shard() {
    if !target::e2e_enabled() {
        return;
    }
    let consensus = std::env::var("COCKROACH_URL").expect("COCKROACH_URL for e2e");
    let blob = std::env::var("PERSIST_BLOB_URL").unwrap_or_else(|_| {
        let dir = tempfile::tempdir().unwrap();
        let p = format!("file://{}", dir.path().display());
        std::mem::forget(dir); // e2e only; OK to leak for the test process lifetime
        p
    });
    let loc = PersistLocation {
        blob_uri: blob.parse().unwrap(),
        consensus_uri: consensus.parse().unwrap(),
    };
    let bind: SocketAddr = std::env::var("DRIVER_PUBSUB_BIND")
        .unwrap_or_else(|_| "0.0.0.0:6879".to_string())
        .parse()
        .unwrap();
    let host = PersistHost::start_on(bind, loc.clone())
        .await
        .expect("host");
    let client = host.client().await.expect("client");

    let shard = ShardId::new();
    let desc = sample_desc();
    let rows = sample_rows(10_000, 16);
    write_rows_single_ts(&client, shard, &desc, &rows, Timestamp::from(0))
        .await
        .expect("write");

    let driver = Driver::connect(host, &target::compute_addr())
        .await
        .expect("connect");

    let source_id = GlobalId::User(1000);
    let index_id = GlobalId::User(1001);
    let df = index_dataflow(
        source_id,
        index_id,
        shard,
        loc.clone(),
        desc.clone(),
        vec![0],
        Timestamp::from(0),
        Timestamp::from(1),
    )
    .expect("build index dataflow");
    driver.submit_dataflow(df).expect("submit");
    driver.schedule(index_id).expect("schedule");

    driver
        .expect_frontier(index_id, Timestamp::from(1), Duration::from_secs(60))
        .await
        .expect("frontier");
    let n = driver
        .peek_count(index_id, desc, Timestamp::from(0))
        .await
        .expect("peek");
    assert_eq!(n, 10_000);
}
