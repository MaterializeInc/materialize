// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared storage-usage publication with two independent adapter executions.

#![recursion_limit = "256"]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use mz_environmentd::test_util::TestHarness;
use mz_postgres_util::{batch_execute, query, query_one, sql};
use mz_repr::AdapterId;
use tokio_postgres::Client;

type UsageRow = (String, String, String, i64, String);

async fn rows(client: &Client, since: DateTime<Utc>) -> BTreeSet<UsageRow> {
    let rows = query(
        client,
        sql!(
            "SELECT id::text, shard_id, collection_timestamp::text,
                floor(extract(epoch FROM collection_timestamp))::bigint, size_bytes::text
         FROM mz_internal.mz_storage_usage_by_shard
         WHERE collection_timestamp >= $1"
        ),
        &[&since],
    )
    .await
    .expect("read shared usage rows");
    let mut result = BTreeSet::new();
    let mut intervals = BTreeMap::new();
    let mut batches = BTreeMap::new();
    let mut shards = BTreeSet::new();
    for row in rows {
        let row: UsageRow = (row.get(0), row.get(1), row.get(2), row.get(3), row.get(4));
        if let Some(prior) = intervals.insert(row.3, row.0.clone()) {
            assert_eq!(
                prior, row.0,
                "multiple batches claimed the same finish-time interval"
            );
        }
        if let Some(prior) = batches.insert(row.0.clone(), row.2.clone()) {
            assert_eq!(prior, row.2, "batch has multiple measurement timestamps");
        }
        assert!(
            shards.insert((row.0.clone(), row.1.clone())),
            "duplicate shard row in batch"
        );
        assert!(result.insert(row));
    }
    result
}

async fn observe_progress(
    client: &Client,
    since: DateTime<Utc>,
    shard: &str,
) -> BTreeSet<UsageRow> {
    loop {
        let observed = rows(client, since).await;
        let batches: BTreeSet<_> = observed
            .iter()
            .filter(|row| row.1 == shard && row.4.parse::<u64>().expect("size") > 0)
            .map(|row| row.0.clone())
            .collect();
        if batches.len() >= 3 {
            return observed;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn now(client: &Client) -> DateTime<Utc> {
    query_one(client, sql!("SELECT now()"), &[])
        .await
        .expect("cutoff")
        .get(0)
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn storage_usage_claims() {
    tokio::time::timeout(Duration::from_secs(180), run())
        .await
        .expect("storage usage claim regression exceeded 180s");
}

async fn run() {
    let harness =
        TestHarness::default().with_storage_usage_collection_interval(Duration::from_secs(1));
    let server = harness.clone().start().await;
    let deployment = server.deployment();
    let client = server.connect().internal().await.expect("first adapter");
    for statement in [
        sql!("CREATE TABLE usage_input (a int)"),
        sql!("INSERT INTO usage_input VALUES (1), (2), (3)"),
        sql!("CREATE ADAPTER usage_peer (SIZE '1')"),
    ] {
        batch_execute(&client, statement)
            .await
            .expect("create input and peer");
    }
    let shard: String = query_one(
        &client,
        sql!(
            "SELECT shard_id FROM mz_internal.mz_storage_shards s
         JOIN mz_catalog.mz_objects o ON o.id = s.object_id WHERE o.name = 'usage_input'"
        ),
        &[],
    )
    .await
    .expect("input shard")
    .get(0);
    let id: String = query_one(
        &client,
        sql!("SELECT id FROM mz_catalog.mz_adapters WHERE name = 'usage_peer'"),
        &[],
    )
    .await
    .expect("peer ID")
    .get(0);
    let peer_harness = harness.with_adapter_id(id.parse::<AdapterId>().expect("AdapterId"));
    let peer = peer_harness.clone().start_in(Arc::clone(&deployment)).await;
    let peer_client = peer.connect().internal().await.expect("peer connection");
    let since = now(&client).await;
    let observed = observe_progress(&client, since, &shard).await;
    assert!(
        observed.is_subset(&rows(&peer_client, since).await),
        "peer must see published batches"
    );

    // Restart one adapter without resetting replicas or the shared Persist cache.
    // New intervals must keep progressing, including after losing claim races.
    drop(peer_client);
    drop(peer);
    let peer = peer_harness.start_in(deployment).await;
    let peer_client = peer.connect().internal().await.expect("restarted peer");
    let after_restart = now(&peer_client).await;
    observe_progress(&peer_client, after_restart, &shard).await;
    rows(&client, since).await;
    let contents: i64 = query_one(
        &peer_client,
        sql!("SELECT sum(a)::bigint FROM usage_input"),
        &[],
    )
    .await
    .expect("input contents")
    .get(0);
    assert_eq!(contents, 6);
}
