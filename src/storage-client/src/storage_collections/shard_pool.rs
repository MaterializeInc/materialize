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
//!
//! ## Crash Recovery
//!
//! Pre-opened shard IDs are tracked in a `pre_allocated_shards` catalog
//! collection (same pattern as `unfinalized_shards`). On restart,
//! `initialize_state` moves any unclaimed pre-allocated shards to
//! `unfinalized_shards` for GC by `finalize_shards_task`. This prevents
//! shard leaks when shards are pre-opened but the process crashes before
//! they are claimed by a DDL operation.

use std::collections::{BTreeSet, VecDeque};
use std::num::NonZeroI64;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;

use differential_dataflow::lattice::Lattice;
use mz_ore::cast::CastFrom;
use mz_ore::task::AbortOnDropHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::critical::{Opaque, SinceHandle};
use mz_persist_client::{Diagnostics, PersistClient, PersistLocation, ShardId};
use mz_persist_types::Codec64;
use mz_storage_types::StorageDiff;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::dyncfgs::{
    SHARD_POOL_ENABLED, SHARD_POOL_REPLENISH_INTERVAL, SHARD_POOL_TARGET_SIZE,
};
use mz_storage_types::sources::SourceData;
use timely::order::TotalOrder;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use tracing::{debug, info, warn};

use crate::controller::PersistEpoch;
use crate::storage_collections::metrics::ShardPoolMetrics;

/// A pre-opened shard with its critical since handle already epoch-fenced.
#[derive(Debug)]
pub(crate) struct PreOpenedShard<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    /// The shard ID that was pre-allocated.
    pub(crate) shard_id: ShardId,
    /// The critical since handle, already epoch-fenced with `envd_epoch`.
    pub(crate) since_handle: SinceHandle<SourceData, (), T, StorageDiff>,
}

/// A thread-safe pool of pre-opened shards.
#[derive(Debug)]
pub(crate) struct ShardPool<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    inner: Mutex<VecDeque<PreOpenedShard<T>>>,
    /// Shard IDs that have been put into the pool but not yet persisted
    /// to the catalog. Drained during `prepare_state` to batch catalog writes.
    pending_catalog_inserts: Mutex<BTreeSet<ShardId>>,
    metrics: ShardPoolMetrics,
}

impl<T> ShardPool<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    /// Creates a new pool with the given metrics.
    pub(crate) fn new(metrics: ShardPoolMetrics) -> Self {
        ShardPool {
            inner: Mutex::new(VecDeque::new()),
            pending_catalog_inserts: Mutex::new(BTreeSet::new()),
            metrics,
        }
    }

    /// Takes a pre-opened shard from the pool, if one is available.
    /// Updates hit/miss metrics accordingly.
    pub(crate) fn take(&self) -> Option<PreOpenedShard<T>> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        let result = inner.pop_front();
        if result.is_some() {
            self.metrics.hits.inc();
            self.metrics.pool_size.set(u64::cast_from(inner.len()));
        } else {
            self.metrics.misses.inc();
        }
        result
    }

    /// Returns a pre-opened shard to the pool.
    ///
    /// The two mutex acquisitions are intentionally separate: `pending_catalog_inserts`
    /// is locked first so the shard ID is visible to `drain_pending_inserts` promptly,
    /// then `inner` is locked to make the shard available for `take`. The brief window
    /// where the ID appears in `pending_catalog_inserts` but not in `inner` is harmless:
    /// `prepare_state` would persist the ID to the catalog and then fail to claim it
    /// immediately (miss), leaving it in the catalog for the next DDL.
    pub(crate) fn put(&self, shard: PreOpenedShard<T>) {
        self.pending_catalog_inserts
            .lock()
            .expect("lock poisoned")
            .insert(shard.shard_id);
        let mut inner = self.inner.lock().expect("lock poisoned");
        inner.push_back(shard);
        self.metrics.pool_size.set(u64::cast_from(inner.len()));
    }

    /// Drains shard IDs that have been added to the pool but not yet persisted
    /// to the catalog. Called during `prepare_state` to batch catalog writes.
    ///
    /// # Atomicity
    ///
    /// The caller must ensure the returned shard IDs are persisted within the
    /// same catalog transaction that claims or drops them. If the catalog
    /// transaction fails, the in-memory pool state becomes stale, but that
    /// is acceptable because a failed catalog transaction will halt the
    /// process, and on restart `initialize_state` will move any unclaimed
    /// pre-allocated shards to `unfinalized_shards` for GC.
    pub(crate) fn drain_pending_inserts(&self) -> BTreeSet<ShardId> {
        std::mem::take(&mut *self.pending_catalog_inserts.lock().expect("lock poisoned"))
    }

    /// Returns the current number of shards in the pool.
    pub(crate) fn len(&self) -> usize {
        self.inner.lock().expect("lock poisoned").len()
    }
}

/// Pre-opens a single shard by generating a new `ShardId`, calling
/// `upgrade_version`, and then `open_critical_since` with epoch fencing.
///
/// This mirrors the logic in `open_data_handles` (upgrade_version) and
/// `open_critical_handle` (epoch fencing CAS loop).
pub(crate) async fn pre_open_shard<T>(
    persist_client: &PersistClient,
    envd_epoch: NonZeroI64,
) -> Result<PreOpenedShard<T>, anyhow::Error>
where
    T: TimelyTimestamp + Lattice + TotalOrder + Codec64 + Sync,
{
    use anyhow::Context;

    let shard_id = ShardId::new();

    let diagnostics = Diagnostics {
        shard_name: format!("pre-opened:{shard_id}"),
        handle_purpose: "shard pool pre-open".to_owned(),
    };

    // Step 1: upgrade_version (same as open_data_handles)
    persist_client
        .upgrade_version::<SourceData, (), T, StorageDiff>(shard_id, diagnostics.clone())
        .await
        .context("upgrade_version failed")?;

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
        .context("open_critical_since failed")?;

    let since = Antichain::from_elem(T::minimum());

    loop {
        let current_epoch: PersistEpoch = handle.opaque().decode();
        let unchecked_success = current_epoch.0.map(|e| e <= envd_epoch).unwrap_or(true);

        if unchecked_success {
            let checked_success = handle
                .compare_and_downgrade_since(
                    &Opaque::encode(&current_epoch),
                    (&Opaque::encode(&PersistEpoch::from(envd_epoch)), &since),
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
pub(crate) struct ShardPoolReplenishConfig<T>
where
    T: TimelyTimestamp + Lattice + Codec64,
{
    pub(crate) envd_epoch: NonZeroI64,
    pub(crate) config: Arc<Mutex<StorageConfiguration>>,
    pub(crate) persist_location: PersistLocation,
    pub(crate) persist: Arc<PersistClientCache>,
    pub(crate) pool: Arc<ShardPool<T>>,
    /// Fires when `initialize_state` has completed its GC pass. The pool task
    /// must not write shards to `pending_catalog_inserts` until this fires,
    /// otherwise `initialize_state` could GC current-epoch pool shards.
    pub(crate) init_notify: Arc<Notify>,
}

/// Background task that keeps the shard pool filled to the target size.
///
/// Checks the pool size at `SHARD_POOL_REPLENISH_INTERVAL` and pre-opens
/// shards as needed. Note: the pool size check and subsequent fills are
/// not atomic; concurrent `take()` calls may cause the pool to briefly
/// exceed the target. This is benign: excess shards are used by subsequent
/// DDLs or GC'd on restart via `pre_allocated_shards` catalog tracking.
pub(crate) async fn shard_pool_replenish_task<T>(
    ShardPoolReplenishConfig {
        envd_epoch,
        config,
        persist_location,
        persist,
        pool,
        init_notify,
    }: ShardPoolReplenishConfig<T>,
) where
    T: TimelyTimestamp + Lattice + TotalOrder + Codec64 + Sync,
{
    // Wait until `initialize_state` has completed its GC pass before allowing
    // shards to be written to `pending_catalog_inserts`. If we wrote catalog
    // entries before GC ran, `initialize_state` would see them as previous-epoch
    // leftovers and finalize them while they're still in use.
    init_notify.notified().await;

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

        // Sleep first so that we don't hammer persist on startup before any DDL
        // has arrived. The first fill happens after one full interval, which is
        // acceptable: a pool miss falls back to opening a fresh shard on the
        // critical path (same cost as today, no regression).
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
pub(crate) fn spawn_shard_pool_task<T>(
    envd_epoch: NonZeroI64,
    config: Arc<Mutex<StorageConfiguration>>,
    persist_location: PersistLocation,
    persist: Arc<PersistClientCache>,
    pool: Arc<ShardPool<T>>,
    init_notify: Arc<Notify>,
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
            init_notify,
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
    use mz_persist_client::PersistLocation;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;

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

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_drain_pending_inserts() {
        let (_cache, client) = test_persist_client().await;
        let epoch = NonZeroI64::new(1).unwrap();
        let pool = ShardPool::<mz_repr::Timestamp>::new(test_metrics());

        // Empty pool has no pending inserts.
        assert!(pool.drain_pending_inserts().is_empty());

        // Put two shards, both should appear in pending inserts.
        let shard_a = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard");
        let shard_b = pre_open_shard::<mz_repr::Timestamp>(&client, epoch)
            .await
            .expect("pre_open_shard");
        let id_a = shard_a.shard_id;
        let id_b = shard_b.shard_id;
        pool.put(shard_a);
        pool.put(shard_b);

        let pending = pool.drain_pending_inserts();
        assert_eq!(pending.len(), 2);
        assert!(pending.contains(&id_a));
        assert!(pending.contains(&id_b));

        // Drain is idempotent; second call returns empty.
        assert!(pool.drain_pending_inserts().is_empty());

        // Pool still has the shards.
        assert_eq!(pool.len(), 2);
    }

    /// Verifies that the replenishment task is gated on `init_notify` and does
    /// not write shards to `pending_catalog_inserts` before it fires. This is
    /// the regression test for the race where `initialize_state`'s GC pass
    /// could finalize current-epoch pool shards if the task ran concurrently.
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)]
    async fn test_init_notify_gates_pool_replenishment() {
        use std::time::Duration;

        use mz_dyncfg::{ConfigSet, ConfigUpdates};
        use mz_secrets::InMemorySecretsController;
        use mz_storage_types::configuration::StorageConfiguration;
        use mz_storage_types::connections::ConnectionContext;
        use mz_storage_types::dyncfgs::{
            SHARD_POOL_REPLENISH_INTERVAL, SHARD_POOL_TARGET_SIZE, all_dyncfgs,
        };

        let (cache, _client) = test_persist_client().await;
        let epoch = NonZeroI64::new(1).unwrap();
        let pool = Arc::new(ShardPool::<mz_repr::Timestamp>::new(test_metrics()));
        let init_notify = Arc::new(tokio::sync::Notify::new());

        let connection_context =
            ConnectionContext::for_tests(Arc::new(InMemorySecretsController::new()));
        let config_set = all_dyncfgs(ConfigSet::default());

        // Short replenish interval so the task would fill the pool quickly if it
        // were not blocked on init_notify.
        let mut updates = ConfigUpdates::default();
        updates.add(&SHARD_POOL_REPLENISH_INTERVAL, Duration::from_millis(10));
        updates.add(&SHARD_POOL_TARGET_SIZE, 2_usize);
        updates.apply(&config_set);

        let config = Arc::new(std::sync::Mutex::new(StorageConfiguration::new(
            connection_context,
            config_set,
        )));

        let _task_handle = spawn_shard_pool_task(
            epoch,
            Arc::clone(&config),
            test_persist_location(),
            Arc::clone(&cache),
            Arc::clone(&pool),
            Arc::clone(&init_notify),
            false,
        );

        // Wait much longer than the replenish interval. If the task were not
        // gated on init_notify it would have filled the pool by now.
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(pool.len(), 0, "pool must be empty before init_notify fires");
        assert!(
            pool.drain_pending_inserts().is_empty(),
            "no catalog inserts before init_notify fires"
        );

        // Signal that initialize_state has completed its GC pass.
        init_notify.notify_waiters();

        // The task wakes up, sleeps one interval (~10ms), then fills the pool.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if pool.len() > 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "pool did not fill within 5s after init_notify fired"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let pending = pool.drain_pending_inserts();
        assert!(
            !pending.is_empty(),
            "shards must be queued for catalog insertion after pool fills"
        );
    }
}
