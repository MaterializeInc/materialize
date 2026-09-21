// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_persist_client::critical::Opaque;

use super::*;

#[mz_ore::test(tokio::test)]
async fn rate_limited_target_survives_a_stale_snapshot() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::NowFn;
    use mz_persist_client::PersistLocation;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;

    let clock = Arc::new(AtomicU64::new(1_000_000));
    let mut config = PersistConfig::new_for_tests();
    config.now = NowFn::from({
        let clock = Arc::clone(&clock);
        move || clock.load(Ordering::SeqCst)
    });
    config.critical_downgrade_interval = Duration::from_secs(1);
    let cache = PersistClientCache::new(config, &MetricsRegistry::new(), |_, _| {
        PubSubClientConnection::noop()
    });
    let persist = cache.open(PersistLocation::new_in_mem()).await.unwrap();
    let shard = ShardId::new();
    let id = GlobalId::User(1);
    let wanted = BTreeSet::from([id]);
    let mut metadata = StorageMetadata::default();
    metadata.collection_metadata.insert(id, shard);
    metadata.compaction_bounds.insert(id, frontier(5));
    let mut compaction = Compaction::default();
    assert_eq!(
        compaction
            .reconcile(&persist, &metadata, &wanted)
            .await
            .unwrap(),
        0
    );
    metadata.compaction_bounds.insert(id, frontier(20));
    assert_eq!(
        compaction
            .reconcile(&persist, &metadata, &wanted)
            .await
            .unwrap(),
        1
    );
    assert_eq!(observer(&persist, shard).await.since(), &frontier(5));
    metadata.compaction_bounds.insert(id, frontier(10));
    clock.fetch_add(1_000, Ordering::SeqCst);
    assert_eq!(
        compaction
            .reconcile(&persist, &metadata, &wanted)
            .await
            .unwrap(),
        0
    );
    assert_eq!(observer(&persist, shard).await.since(), &frontier(20));
}

fn frontier(time: u64) -> Antichain<Timestamp> {
    Antichain::from_elem(Timestamp::from(time))
}

async fn observer(persist: &PersistClient, shard: ShardId) -> CriticalSinceHandle {
    read_protection::open_critical_handle(
        persist,
        shard,
        Diagnostics {
            shard_name: shard.to_string(),
            handle_purpose: "compaction test".into(),
        },
    )
    .await
    .unwrap()
}

#[mz_ore::test(tokio::test)]
async fn all_aliases_constrain_only_wanted_shards() {
    let persist = PersistClient::new_for_tests().await;
    let shard = ShardId::new();
    let unrelated = ShardId::new();
    let local = GlobalId::User(1);
    let retained = GlobalId::User(2);
    let remote = GlobalId::User(3);
    let mut metadata = StorageMetadata::default();
    metadata
        .collection_metadata
        .extend([(local, shard), (retained, shard), (remote, unrelated)]);
    metadata.retained_collections.insert(retained);
    metadata.compaction_bounds.extend([
        (local, frontier(20)),
        (retained, frontier(5)),
        (remote, frontier(30)),
    ]);
    let wanted = BTreeSet::from([local]);
    let mut compaction = Compaction::default();
    assert_eq!(
        compaction
            .reconcile(&persist, &metadata, &wanted)
            .await
            .unwrap(),
        0
    );
    assert_eq!(observer(&persist, shard).await.since(), &frontier(5));
    assert_eq!(
        compaction.handles.keys().copied().collect::<Vec<_>>(),
        vec![shard]
    );
    assert_eq!(observer(&persist, unrelated).await.since(), &frontier(0));

    // Neither absence of permission nor local interest authorizes retirement.
    metadata.compaction_bounds.remove(&retained);
    metadata.compaction_bounds.insert(local, Antichain::new());
    assert_eq!(
        compaction
            .reconcile(&persist, &metadata, &wanted)
            .await
            .unwrap(),
        1
    );
    assert_eq!(observer(&persist, shard).await.since(), &frontier(5));
    compaction
        .reconcile(&persist, &metadata, &BTreeSet::new())
        .await
        .unwrap();
    assert!(compaction.handles.is_empty());
    assert_eq!(observer(&persist, shard).await.since(), &frontier(5));
}

#[mz_ore::test(tokio::test)]
async fn shared_opaque_contention_and_stale_permission() {
    let persist = PersistClient::new_for_tests().await;
    let shard = ShardId::new();
    let mut first = observer(&persist, shard).await;
    let mut peer = observer(&persist, shard).await;
    let initial = peer.opaque().clone();
    let other = Opaque::encode(&42_i64);
    peer.compare_and_downgrade_since(&initial, (&other, &frontier(10)))
        .await
        .unwrap();

    // A conflicting opaque is refreshed, not taken over or treated as a fence.
    assert!(matches!(
        read_protection::downgrade_since(&mut first, &frontier(20)).await,
        Some(Err(_))
    ));
    assert_eq!(first.opaque(), &other);
    assert_eq!(first.since(), &frontier(10));
    assert!(
        matches!(read_protection::downgrade_since(&mut first, &frontier(5)).await, Some(Ok(since)) if since == frontier(10))
    );
    // Empty permission can finish despite the rate limit from the failed CAS.
    assert!(
        matches!(read_protection::downgrade_since(&mut first, &Antichain::new()).await, Some(Ok(since)) if since.is_empty())
    );
    assert_eq!(observer(&persist, shard).await.opaque(), &other);
    assert!(observer(&persist, shard).await.since().is_empty());
}

#[test]
fn missing_permission_is_not_empty_permission() {
    let local = GlobalId::User(1);
    let alias = GlobalId::User(2);
    let shard = ShardId::new();
    let wanted = BTreeSet::from([local]);
    let mut metadata = StorageMetadata::default();
    metadata
        .collection_metadata
        .extend([(local, shard), (alias, shard)]);
    metadata.compaction_bounds.insert(local, Antichain::new());
    assert_eq!(
        shard_bounds(&metadata, &wanted),
        BTreeMap::from([(shard, None)])
    );
    metadata.compaction_bounds.insert(alias, Antichain::new());
    assert_eq!(
        shard_bounds(&metadata, &wanted),
        BTreeMap::from([(shard, Some(Antichain::new()))])
    );
    metadata.compaction_bounds.remove(&local);
    assert_eq!(
        shard_bounds(&metadata, &wanted),
        BTreeMap::from([(shard, None)])
    );
}
