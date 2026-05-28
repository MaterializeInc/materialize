// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Periodic cache refresh task.
//!
//! Defends against silent writes from other writers (a second `environmentd`
//! during failover, clusterds still on the direct-CRDB code path during
//! rollout, admin tooling). For every shard with at least one active
//! subscriber, re-reads the head from the backing store at a configured
//! interval and monotonic-merges the result into the cache.

use std::sync::Arc;
use std::time::Duration;

use mz_ore::task::JoinHandle;
use mz_persist::location::Consensus;
use tracing::warn;

use crate::cache::ShardCache;
use crate::subscribe::SubscriberRegistry;

/// Spawn the refresh task. Returns a `JoinHandle` so the caller can abort it
/// on shutdown.
pub fn spawn_refresh(
    consensus: Arc<dyn Consensus + Send + Sync>,
    cache: Arc<ShardCache>,
    registry: Arc<SubscriberRegistry>,
    interval: Duration,
) -> JoinHandle<()> {
    mz_ore::task::spawn(|| "persist_committer::refresh", async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            let subscribed = cache.shards_with_subscribers();
            for shard in subscribed {
                match consensus.head(&shard).await {
                    Ok(Some(v)) => {
                        let prev = cache.get(&shard).map(|p| p.seqno);
                        let new_seqno = v.seqno;
                        cache.insert(&shard, v.clone());
                        if Some(new_seqno) != prev {
                            registry.publish(&shard, v);
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        warn!(shard, error = %e, "refresh head failed");
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mz_persist::location::{SeqNo, VersionedData};
    use mz_persist::mem::MemConsensus;

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn refresh_picks_up_underlying_writes() {
        let underlying: Arc<dyn Consensus + Send + Sync> = Arc::new(MemConsensus::default());
        let cache = Arc::new(ShardCache::new(100));
        let registry = Arc::new(SubscriberRegistry::new());
        let token = cache.subscribe("s1");
        // Cache thinks shard is empty; underlying has seqno=0 already.
        let _ = underlying
            .compare_and_set(
                "s1",
                VersionedData {
                    seqno: SeqNo(0),
                    data: Bytes::from(vec![0xAA]),
                },
            )
            .await
            .unwrap();
        let _ = underlying
            .compare_and_set(
                "s1",
                VersionedData {
                    seqno: SeqNo(1),
                    data: Bytes::from(vec![0xBB]),
                },
            )
            .await
            .unwrap();

        let _handle = spawn_refresh(
            underlying,
            Arc::clone(&cache),
            registry,
            Duration::from_secs(1),
        );

        tokio::time::advance(Duration::from_millis(1500)).await;
        for _ in 0..50 {
            tokio::task::yield_now().await;
            if cache.get("s1").is_some() {
                break;
            }
        }
        let head = cache.get("s1").expect("refresh must populate cache");
        assert_eq!(head.seqno, SeqNo(1));
        drop(token);
    }
}
