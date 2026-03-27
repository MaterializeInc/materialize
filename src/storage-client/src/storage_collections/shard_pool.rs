// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A pool of pre-opened persist shards for reducing DDL latency.
//!
//! During `open_data_handles`, each collection requires `upgrade_version` and
//! `open_critical_since` calls that hit CRDB. This module pre-performs those
//! operations (with epoch fencing) in the background so they can be skipped
//! at DDL time.
//!
//! The write handle still needs `RelationDesc` (the table schema), so it
//! cannot be pre-opened and stays on the critical path.

use std::collections::VecDeque;
use std::num::NonZeroI64;
use std::sync::{Arc, Mutex};

use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::{Opaque, SinceHandle};
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::Codec64;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::dyncfgs::{
    SHARD_POOL_ENABLED, SHARD_POOL_REPLENISH_INTERVAL, SHARD_POOL_TARGET_SIZE,
};
use mz_storage_types::sources::SourceData;
use mz_storage_types::StorageDiff;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::{debug, info, warn};

use crate::controller::PersistEpoch;
use crate::storage_collections::metrics::ShardPoolMetrics;

/// A pre-opened shard with its critical since handle already epoch-fenced.
#[derive(Debug)]
pub struct PreOpenedShard<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    /// The shard ID that was pre-allocated.
    pub shard_id: ShardId,
    /// The critical since handle, already epoch-fenced with `envd_epoch`.
    pub since_handle: SinceHandle<SourceData, (), T, StorageDiff>,
}

/// A thread-safe pool of pre-opened shards.
#[derive(Debug)]
pub struct ShardPool<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    inner: Mutex<VecDeque<PreOpenedShard<T>>>,
    metrics: ShardPoolMetrics,
}

impl<T> ShardPool<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    /// Creates a new pool with the given metrics.
    pub fn new(metrics: ShardPoolMetrics) -> Self {
        ShardPool {
            inner: Mutex::new(VecDeque::new()),
            metrics,
        }
    }

    /// Takes a pre-opened shard from the pool, if one is available.
    /// Updates hit/miss metrics accordingly.
    pub fn take(&self) -> Option<PreOpenedShard<T>> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        let result = inner.pop_front();
        if result.is_some() {
            self.metrics.hits.inc();
            self.metrics
                .pool_size
                .set(u64::cast_from(inner.len()));
        } else {
            self.metrics.misses.inc();
        }
        result
    }

    /// Returns a pre-opened shard to the pool.
    pub fn put(&self, shard: PreOpenedShard<T>) {
        let mut inner = self.inner.lock().expect("lock poisoned");
        inner.push_back(shard);
        self.metrics
            .pool_size
            .set(u64::cast_from(inner.len()));
    }

    /// Returns the current number of shards in the pool.
    pub fn len(&self) -> usize {
        self.inner.lock().expect("lock poisoned").len()
    }
}

/// Pre-opens a single shard by generating a new `ShardId`, calling
/// `upgrade_version`, and then `open_critical_since` with epoch fencing.
///
/// This mirrors the logic in `open_data_handles` (upgrade_version) and
/// `open_critical_handle` (epoch fencing CAS loop).
pub async fn pre_open_shard<T>(
    persist_client: &PersistClient,
    envd_epoch: NonZeroI64,
) -> Result<PreOpenedShard<T>, anyhow::Error>
where
    T: TimelyTimestamp + Lattice + TotalOrder + Codec64 + Sync,
{
    let shard_id = ShardId::new();

    let diagnostics = Diagnostics {
        shard_name: format!("pre-opened:{shard_id}"),
        handle_purpose: "shard pool pre-open".to_owned(),
    };

    // Step 1: upgrade_version (same as open_data_handles)
    persist_client
        .upgrade_version::<SourceData, (), T, StorageDiff>(shard_id, diagnostics.clone())
        .await
        .map_err(|e| anyhow::anyhow!("upgrade_version failed: {e:?}"))?;

    // Step 2: open_critical_since with epoch fencing CAS loop
    // (same as open_critical_handle)
    let mut handle: SinceHandle<SourceData, (), T, StorageDiff> = persist_client
        .open_critical_since(
            shard_id,
            PersistClient::CONTROLLER_CRITICAL_SINCE,
            Opaque::encode(&PersistEpoch::default()),
            diagnostics,
        )
        .await
        .map_err(|e| anyhow::anyhow!("open_critical_since failed: {e:?}"))?;

    let since = Antichain::from_elem(T::minimum());

    loop {
        let current_epoch: PersistEpoch = handle.opaque().decode();
        let unchecked_success = current_epoch.0.map(|e| e <= envd_epoch).unwrap_or(true);

        if unchecked_success {
            let checked_success = handle
                .compare_and_downgrade_since(
                    &Opaque::encode(&current_epoch),
                    (
                        &Opaque::encode(&PersistEpoch::from(envd_epoch)),
                        &since,
                    ),
                )
                .await
                .is_ok();
            if checked_success {
                break;
            }
        } else {
            mz_ore::halt!("shard pool: fenced by envd @ {current_epoch:?}. ours = {envd_epoch}");
        }
    }

    Ok(PreOpenedShard {
        shard_id,
        since_handle: handle,
    })
}

/// Configuration for the shard pool replenishment background task.
pub struct ShardPoolReplenishConfig<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    pub envd_epoch: NonZeroI64,
    pub config: Arc<Mutex<StorageConfiguration>>,
    pub persist_location: PersistLocation,
    pub persist: Arc<PersistClientCache>,
    pub pool: Arc<ShardPool<T>>,
}

/// Background task that keeps the shard pool filled to the target size.
///
/// Checks the pool size at `SHARD_POOL_REPLENISH_INTERVAL` and pre-opens
/// shards as needed. Note: the pool size check and subsequent fills are
/// not atomic; concurrent `take()` calls may cause the pool to briefly
/// exceed the target. This is benign: excess shards are used by subsequent
/// DDLs or GC'd on restart via `pre_allocated_shards` catalog tracking.
pub async fn shard_pool_replenish_task<T>(
    ShardPoolReplenishConfig {
        envd_epoch,
        config,
        persist_location,
        persist,
        pool,
    }: ShardPoolReplenishConfig<T>,
) where
    T: TimelyTimestamp + Lattice + TotalOrder + Codec64 + Sync,
{
    loop {
        let (enabled, target_size, replenish_interval) = {
            let config = config.lock().expect("lock poisoned");
            let config_set = config.config_set();
            (
                SHARD_POOL_ENABLED.get(config_set),
                SHARD_POOL_TARGET_SIZE.get(config_set),
                SHARD_POOL_REPLENISH_INTERVAL.get(config_set),
            )
        };

        tokio::time::sleep(replenish_interval).await;

        if !enabled {
            continue;
        }

        let current_size = pool.len();
        if current_size >= target_size {
            continue;
        }

        let deficit = target_size - current_size;
        debug!(
            current_size,
            target_size, deficit, "replenishing shard pool"
        );

        let persist_client = match persist.open(persist_location.clone()).await {
            Ok(client) => client,
            Err(e) => {
                warn!("shard pool: failed to open persist client: {e}");
                continue;
            }
        };

        for _ in 0..deficit {
            match pre_open_shard::<T>(&persist_client, envd_epoch).await {
                Ok(shard) => {
                    debug!(shard_id = %shard.shard_id, "pre-opened shard added to pool");
                    pool.put(shard);
                }
                Err(e) => {
                    warn!("shard pool: failed to pre-open shard: {e}");
                    break;
                }
            }
        }
    }
}

/// Spawns the shard pool replenishment task and returns its handle.
///
/// The task is only spawned when not in read-only mode.
pub fn spawn_shard_pool_task<T>(
    envd_epoch: NonZeroI64,
    config: Arc<Mutex<StorageConfiguration>>,
    persist_location: PersistLocation,
    persist: Arc<PersistClientCache>,
    pool: Arc<ShardPool<T>>,
    read_only: bool,
) -> Option<Arc<AbortOnDropHandle<()>>>
where
    T: TimelyTimestamp + Lattice + TotalOrder + Codec64 + Sync + 'static,
{
    if read_only {
        info!("disabling shard pool in read-only mode");
        return None;
    }

    let task = mz_ore::task::spawn(
        || "storage_collections::shard_pool_replenish_task",
        shard_pool_replenish_task(ShardPoolReplenishConfig {
            envd_epoch,
            config,
            persist_location,
            persist,
            pool,
        }),
    );

    Some(Arc::new(task.abort_on_drop()))
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroI64;
    use std::str::FromStr;
    use std::sync::Arc;

    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::now::SYSTEM_TIME;
    use mz_ore::url::SensitiveUrl;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_persist_client::PersistLocation;

    use super::*;
    use crate::storage_collections::metrics::ShardPoolMetrics;

    fn test_metrics() -> ShardPoolMetrics {
        ShardPoolMetrics::register_into(&MetricsRegistry::new())
    }

    fn test_persist_location() -> PersistLocation {
        PersistLocation {
            blob_uri: SensitiveUrl::from_str("mem://").expect("valid url"),
            consensus_uri: SensitiveUrl::from_str("mem://").expect("valid url"),
        }
    }

    async fn test_persist_client() -> (Arc<PersistClientCache>, mz_persist_client::PersistClient) {
        let cache = Arc::new(PersistClientCache::new(
            PersistConfig::new_default_configs(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        ));
        let client = cache
            .open(test_persist_location())
            .await
            .expect("open persist");
        (cache, client)
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_empty_pool_take_returns_none() {
        let pool = ShardPool::<mz_repr::Timestamp>::new(test_metrics());
        assert!(pool.take().is_none());
        // Miss metric should be incremented.
        assert_eq!(pool.metrics.misses.get(), 1.0);
        assert_eq!(pool.metrics.hits.get(), 0.0);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_put_and_take() {
        let (_cache, client) = test_persist_client().await;
        let epoch = NonZeroI64::new(1).unwrap();
        let shard = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard");
        let expected_id = shard.shard_id;

        let pool = ShardPool::new(test_metrics());
        pool.put(shard);
        assert_eq!(pool.len(), 1);

        let taken = pool.take().expect("should have a shard");
        assert_eq!(taken.shard_id, expected_id);
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.metrics.hits.get(), 1.0);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_fifo_order() {
        let (_cache, client) = test_persist_client().await;
        let epoch = NonZeroI64::new(1).unwrap();

        let shard_a = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard a");
        let shard_b = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard b");
        let id_a = shard_a.shard_id;
        let id_b = shard_b.shard_id;

        let pool = ShardPool::new(test_metrics());
        pool.put(shard_a);
        pool.put(shard_b);

        let first = pool.take().unwrap();
        assert_eq!(first.shard_id, id_a);
        let second = pool.take().unwrap();
        assert_eq!(second.shard_id, id_b);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_len() {
        let (_cache, client) = test_persist_client().await;
        let epoch = NonZeroI64::new(1).unwrap();

        let pool = ShardPool::<mz_repr::Timestamp>::new(test_metrics());
        assert_eq!(pool.len(), 0);

        let shard = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard");
        pool.put(shard);
        assert_eq!(pool.len(), 1);

        let shard2 = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard");
        pool.put(shard2);
        assert_eq!(pool.len(), 2);

        pool.take();
        assert_eq!(pool.len(), 1);

        pool.take();
        assert_eq!(pool.len(), 0);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_metrics_hit_miss() {
        let (_cache, client) = test_persist_client().await;
        let epoch = NonZeroI64::new(1).unwrap();
        let pool = ShardPool::<mz_repr::Timestamp>::new(test_metrics());

        // Two misses.
        pool.take();
        pool.take();
        assert_eq!(pool.metrics.misses.get(), 2.0);
        assert_eq!(pool.metrics.hits.get(), 0.0);

        // One hit.
        let shard = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard");
        pool.put(shard);
        pool.take();
        assert_eq!(pool.metrics.hits.get(), 1.0);
        assert_eq!(pool.metrics.misses.get(), 2.0);
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_pool_size_metric() {
        let (_cache, client) = test_persist_client().await;
        let epoch = NonZeroI64::new(1).unwrap();
        let pool = ShardPool::<mz_repr::Timestamp>::new(test_metrics());

        assert_eq!(pool.metrics.pool_size.get(), 0);

        let shard = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard");
        pool.put(shard);
        assert_eq!(pool.metrics.pool_size.get(), 1);

        let shard2 = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard");
        pool.put(shard2);
        assert_eq!(pool.metrics.pool_size.get(), 2);

        pool.take();
        assert_eq!(pool.metrics.pool_size.get(), 1);

        pool.take();
        assert_eq!(pool.metrics.pool_size.get(), 0);
    }
}
