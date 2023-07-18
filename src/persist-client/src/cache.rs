// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A cache of [PersistClient]s indexed by [PersistLocation]s.

use std::any::Any;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::future::Future;
use std::sync::{Arc, RwLock, TryLockError, Weak};
use std::time::{Duration, Instant};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::cfg::{BlobConfig, ConsensusConfig};
use mz_persist::location::{
    Blob, Consensus, ExternalError, VersionedData, BLOB_GET_LIVENESS_KEY,
    CONSENSUS_HEAD_LIVENESS_KEY,
};
use mz_persist_types::{Codec, Codec64};
use timely::progress::Timestamp;
use tokio::sync::{Mutex, OnceCell};
use tokio::task::JoinHandle;
use tracing::{debug, instrument};

use crate::async_runtime::IsolatedRuntime;
use crate::error::{CodecConcreteType, CodecMismatch};
use crate::internal::machine::retry_external;
use crate::internal::metrics::{LockMetrics, Metrics, MetricsBlob, MetricsConsensus, ShardMetrics};
use crate::internal::state::TypedState;
use crate::internal::watch::StateWatchNotifier;
use crate::rpc::{PubSubClientConnection, PubSubSender, ShardSubscriptionToken};
use crate::{Diagnostics, PersistClient, PersistConfig, PersistLocation, ShardId};

/// A cache of [PersistClient]s indexed by [PersistLocation]s.
///
/// There should be at most one of these per process. All production
/// PersistClients should be created through this cache.
///
/// This is because, in production, persist is heavily limited by the number of
/// server-side Postgres/Aurora connections. This cache allows PersistClients to
/// share, for example, these Postgres connections.
#[derive(Debug)]
pub struct PersistClientCache {
    pub(crate) cfg: PersistConfig,
    pub(crate) metrics: Arc<Metrics>,
    blob_by_uri: Mutex<BTreeMap<String, (RttLatencyTask, Arc<dyn Blob + Send + Sync>)>>,
    consensus_by_uri: Mutex<BTreeMap<String, (RttLatencyTask, Arc<dyn Consensus + Send + Sync>)>>,
    isolated_runtime: Arc<IsolatedRuntime>,
    pub(crate) state_cache: Arc<StateCache>,
    pubsub_sender: Arc<dyn PubSubSender>,
    _pubsub_receiver_task: JoinHandle<()>,
}

#[derive(Debug)]
struct RttLatencyTask(JoinHandle<()>);

impl Drop for RttLatencyTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl PersistClientCache {
    /// Returns a new [PersistClientCache].
    pub fn new<F>(cfg: PersistConfig, registry: &MetricsRegistry, pubsub: F) -> Self
    where
        F: FnOnce(&PersistConfig, Arc<Metrics>) -> PubSubClientConnection,
    {
        let metrics = Arc::new(Metrics::new(&cfg, registry));
        let pubsub_client = pubsub(&cfg, Arc::clone(&metrics));

        let state_cache = Arc::new(StateCache::new(
            &cfg,
            Arc::clone(&metrics),
            Arc::clone(&pubsub_client.sender),
        ));
        let _pubsub_receiver_task = crate::rpc::subscribe_state_cache_to_pubsub(
            Arc::clone(&state_cache),
            pubsub_client.receiver,
        );

        PersistClientCache {
            cfg,
            metrics,
            blob_by_uri: Mutex::new(BTreeMap::new()),
            consensus_by_uri: Mutex::new(BTreeMap::new()),
            isolated_runtime: Arc::new(IsolatedRuntime::new()),
            state_cache,
            pubsub_sender: pubsub_client.sender,
            _pubsub_receiver_task,
        }
    }

    /// A test helper that returns a [PersistClientCache] disconnected from
    /// metrics.
    #[cfg(test)]
    pub fn new_no_metrics() -> Self {
        Self::new(
            PersistConfig::new_for_tests(),
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        )
    }

    /// Returns the [PersistConfig] being used by this cache.
    pub fn cfg(&self) -> &PersistConfig {
        &self.cfg
    }

    /// Returns a new [PersistClient] for interfacing with persist shards made
    /// durable to the given [PersistLocation].
    ///
    /// The same `location` may be used concurrently from multiple processes.
    #[instrument(level = "debug", skip_all)]
    pub async fn open(&self, location: PersistLocation) -> Result<PersistClient, ExternalError> {
        let blob = self.open_blob(location.blob_uri).await?;
        let consensus = self.open_consensus(location.consensus_uri).await?;
        PersistClient::new(
            self.cfg.clone(),
            blob,
            consensus,
            Arc::clone(&self.metrics),
            Arc::clone(&self.isolated_runtime),
            Arc::clone(&self.state_cache),
            Arc::clone(&self.pubsub_sender),
        )
    }

    // No sense in measuring rtt latencies more often than this.
    const PROMETHEUS_SCRAPE_INTERVAL: Duration = Duration::from_secs(60);

    async fn open_consensus(
        &self,
        consensus_uri: String,
    ) -> Result<Arc<dyn Consensus + Send + Sync>, ExternalError> {
        let mut consensus_by_uri = self.consensus_by_uri.lock().await;
        let consensus = match consensus_by_uri.entry(consensus_uri) {
            Entry::Occupied(x) => Arc::clone(&x.get().1),
            Entry::Vacant(x) => {
                // Intentionally hold the lock, so we don't double connect under
                // concurrency.
                let consensus = ConsensusConfig::try_from(
                    x.key(),
                    Box::new(self.cfg.clone()),
                    self.metrics.postgres_consensus.clone(),
                )?;
                let consensus =
                    retry_external(&self.metrics.retries.external.consensus_open, || {
                        consensus.clone().open()
                    })
                    .await;
                let consensus =
                    Arc::new(MetricsConsensus::new(consensus, Arc::clone(&self.metrics)));
                let task = consensus_rtt_latency_task(
                    Arc::clone(&consensus),
                    Arc::clone(&self.metrics),
                    Self::PROMETHEUS_SCRAPE_INTERVAL,
                )
                .await;
                Arc::clone(&x.insert((RttLatencyTask(task), consensus)).1)
            }
        };
        Ok(consensus)
    }

    async fn open_blob(
        &self,
        blob_uri: String,
    ) -> Result<Arc<dyn Blob + Send + Sync>, ExternalError> {
        let mut blob_by_uri = self.blob_by_uri.lock().await;
        let blob = match blob_by_uri.entry(blob_uri) {
            Entry::Occupied(x) => Arc::clone(&x.get().1),
            Entry::Vacant(x) => {
                // Intentionally hold the lock, so we don't double connect under
                // concurrency.
                let blob = BlobConfig::try_from(
                    x.key(),
                    Box::new(self.cfg.clone()),
                    self.metrics.s3_blob.clone(),
                )
                .await?;
                let blob = retry_external(&self.metrics.retries.external.blob_open, || {
                    blob.clone().open()
                })
                .await;
                let blob = Arc::new(MetricsBlob::new(blob, Arc::clone(&self.metrics)));
                let task = blob_rtt_latency_task(
                    Arc::clone(&blob),
                    Arc::clone(&self.metrics),
                    Self::PROMETHEUS_SCRAPE_INTERVAL,
                )
                .await;
                Arc::clone(&x.insert((RttLatencyTask(task), blob)).1)
            }
        };
        Ok(blob)
    }
}

/// Starts a task to periodically measure the persist-observed latency to
/// consensus.
///
/// This is a task, rather than something like looking at the latencies of prod
/// traffic, so that we minimize any issues around Futures not being polled
/// promptly (as can and does happen with the Timely-polled Futures).
///
/// The caller is responsible for shutdown via [JoinHandle::abort].
///
/// No matter whether we wrap MetricsConsensus before or after we start up the
/// rtt latency task, there's the possibility for it being confusing at some
/// point. Err on the side of more data (including the latency measurements) to
/// start.
async fn blob_rtt_latency_task(
    blob: Arc<MetricsBlob>,
    metrics: Arc<Metrics>,
    measurement_interval: Duration,
) -> JoinHandle<()> {
    mz_ore::task::spawn(|| "persist::blob_rtt_latency", async move {
        // Use the tokio Instant for next_measurement because the reclock tests
        // mess with the tokio sleep clock.
        let mut next_measurement = tokio::time::Instant::now();
        loop {
            tokio::time::sleep_until(next_measurement).await;
            let start = Instant::now();
            match blob.get(BLOB_GET_LIVENESS_KEY).await {
                Ok(_) => {
                    metrics.blob.rtt_latency.set(start.elapsed().as_secs_f64());
                }
                Err(_) => {
                    // Don't spam retries if this returns an error. We're
                    // guaranteed by the method signature that we've already got
                    // metrics coverage of these, so we'll count the errors.
                }
            }
            next_measurement = tokio::time::Instant::now() + measurement_interval;
        }
    })
}

/// Starts a task to periodically measure the persist-observed latency to
/// consensus.
///
/// This is a task, rather than something like looking at the latencies of prod
/// traffic, so that we minimize any issues around Futures not being polled
/// promptly (as can and does happen with the Timely-polled Futures).
///
/// The caller is responsible for shutdown via [JoinHandle::abort].
///
/// No matter whether we wrap MetricsConsensus before or after we start up the
/// rtt latency task, there's the possibility for it being confusing at some
/// point. Err on the side of more data (including the latency measurements) to
/// start.
async fn consensus_rtt_latency_task(
    consensus: Arc<MetricsConsensus>,
    metrics: Arc<Metrics>,
    measurement_interval: Duration,
) -> JoinHandle<()> {
    mz_ore::task::spawn(|| "persist::blob_rtt_latency", async move {
        // Use the tokio Instant for next_measurement because the reclock tests
        // mess with the tokio sleep clock.
        let mut next_measurement = tokio::time::Instant::now();
        loop {
            tokio::time::sleep_until(next_measurement).await;
            let start = Instant::now();
            match consensus.head(CONSENSUS_HEAD_LIVENESS_KEY).await {
                Ok(_) => {
                    metrics
                        .consensus
                        .rtt_latency
                        .set(start.elapsed().as_secs_f64());
                }
                Err(_) => {
                    // Don't spam retries if this returns an error. We're
                    // guaranteed by the method signature that we've already got
                    // metrics coverage of these, so we'll count the errors.
                }
            }
            next_measurement = tokio::time::Instant::now() + measurement_interval;
        }
    })
}

pub(crate) trait DynState: Debug + Send + Sync {
    fn codecs(&self) -> (String, String, String, String, Option<CodecConcreteType>);
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn push_diff(&self, diff: VersionedData);
}

impl<K, V, T, D> DynState for LockingTypedState<K, V, T, D>
where
    K: Codec,
    V: Codec,
    T: Timestamp + Lattice + Codec64,
    D: Codec64,
{
    fn codecs(&self) -> (String, String, String, String, Option<CodecConcreteType>) {
        (
            K::codec_name(),
            V::codec_name(),
            T::codec_name(),
            D::codec_name(),
            Some(CodecConcreteType(std::any::type_name::<(K, V, T, D)>())),
        )
    }

    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn push_diff(&self, diff: VersionedData) {
        self.write_lock(&self.metrics.locks.applier_write, |state| {
            let seqno_before = state.seqno;
            state.apply_encoded_diffs(&self.cfg, &self.metrics, std::iter::once(&diff));
            let seqno_after = state.seqno;
            assert!(seqno_after >= seqno_before);

            if seqno_before != seqno_after {
                debug!(
                    "applied pushed diff {}. seqno {} -> {}.",
                    state.shard_id, seqno_before, state.seqno
                );
                self.shard_metrics.pubsub_push_diff_applied.inc();
            } else {
                debug!(
                    "failed to apply pushed diff {}. seqno {} vs diff {}",
                    state.shard_id, seqno_before, diff.seqno
                );
                if diff.seqno <= seqno_before {
                    self.shard_metrics.pubsub_push_diff_not_applied_stale.inc();
                } else {
                    self.shard_metrics
                        .pubsub_push_diff_not_applied_out_of_order
                        .inc();
                }
            }
        })
    }
}

/// A cache of `TypedState`, shared between all machines for that shard.
///
/// This is shared between all machines that come out of the same
/// [PersistClientCache], but in production there is one of those per process,
/// so in practice, we have one copy of state per shard per process.
///
/// The mutex contention between commands is not an issue, because if two
/// command for the same shard are executing concurrently, only one can win
/// anyway, the other will retry. With the mutex, we even get to avoid the retry
/// if the racing commands are on the same process.
#[derive(Debug)]
pub struct StateCache {
    cfg: Arc<PersistConfig>,
    pub(crate) metrics: Arc<Metrics>,
    states: Arc<std::sync::Mutex<BTreeMap<ShardId, Arc<OnceCell<Weak<dyn DynState>>>>>>,
    pubsub_sender: Arc<dyn PubSubSender>,
}

#[derive(Debug)]
enum StateCacheInit {
    Init(Arc<dyn DynState>),
    NeedInit(Arc<OnceCell<Weak<dyn DynState>>>),
}

impl StateCache {
    /// Returns a new StateCache.
    pub fn new(
        cfg: &PersistConfig,
        metrics: Arc<Metrics>,
        pubsub_sender: Arc<dyn PubSubSender>,
    ) -> Self {
        StateCache {
            cfg: Arc::new(cfg.clone()),
            metrics,
            states: Default::default(),
            pubsub_sender,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_no_metrics() -> Self {
        Self::new(
            &PersistConfig::new_for_tests(),
            Arc::new(Metrics::new(
                &PersistConfig::new_for_tests(),
                &MetricsRegistry::new(),
            )),
            Arc::new(crate::rpc::NoopPubSubSender),
        )
    }

    pub(crate) async fn get<K, V, T, D, F, InitFn>(
        &self,
        shard_id: ShardId,
        mut init_fn: InitFn,
        diagnostics: &Diagnostics,
    ) -> Result<Arc<LockingTypedState<K, V, T, D>>, Box<CodecMismatch>>
    where
        K: Debug + Codec,
        V: Debug + Codec,
        T: Timestamp + Lattice + Codec64,
        D: Semigroup + Codec64,
        F: Future<Output = Result<TypedState<K, V, T, D>, Box<CodecMismatch>>>,
        InitFn: FnMut() -> F,
    {
        loop {
            let init = {
                let mut states = self.states.lock().expect("lock poisoned");
                let state = states.entry(shard_id).or_default();
                match state.get() {
                    Some(once_val) => match once_val.upgrade() {
                        Some(x) => StateCacheInit::Init(x),
                        None => {
                            // If the Weak has lost the ability to upgrade,
                            // we've dropped the State and it's gone. Clear the
                            // OnceCell and init a new one.
                            *state = Arc::new(OnceCell::new());
                            StateCacheInit::NeedInit(Arc::clone(state))
                        }
                    },
                    None => StateCacheInit::NeedInit(Arc::clone(state)),
                }
            };

            let state = match init {
                StateCacheInit::Init(x) => x,
                StateCacheInit::NeedInit(init_once) => {
                    let mut did_init: Option<Arc<LockingTypedState<K, V, T, D>>> = None;
                    let state = init_once
                        .get_or_try_init::<Box<CodecMismatch>, _, _>(|| async {
                            let init_res = init_fn().await;
                            let state = Arc::new(LockingTypedState::new(
                                shard_id,
                                init_res?,
                                Arc::clone(&self.metrics),
                                Arc::clone(&self.cfg),
                                Arc::clone(&self.pubsub_sender).subscribe(&shard_id),
                                diagnostics,
                            ));
                            let ret = Arc::downgrade(&state);
                            did_init = Some(state);
                            let ret: Weak<dyn DynState> = ret;
                            Ok(ret)
                        })
                        .await?;
                    if let Some(x) = did_init {
                        // We actually did the init work, don't bother casting back
                        // the type erased and weak version. Additionally, inform
                        // any listeners of this new state.
                        return Ok(x);
                    }
                    let Some(state) = state.upgrade() else {
                        // Race condition. Between when we first checked the
                        // OnceCell and the `get_or_try_init` call, (1) the
                        // initialization finished, (2) the other user dropped
                        // the strong ref, and (3) the Arc noticed it was down
                        // to only weak refs and dropped the value. Nothing we
                        // can do except try again.
                        continue;
                    };
                    state
                }
            };

            match Arc::clone(&state)
                .as_any()
                .downcast::<LockingTypedState<K, V, T, D>>()
            {
                Ok(x) => return Ok(x),
                Err(_) => {
                    return Err(Box::new(CodecMismatch {
                        requested: (
                            K::codec_name(),
                            V::codec_name(),
                            T::codec_name(),
                            D::codec_name(),
                            Some(CodecConcreteType(std::any::type_name::<(K, V, T, D)>())),
                        ),
                        actual: state.codecs(),
                    }))
                }
            }
        }
    }

    pub(crate) fn get_state_weak(&self, shard_id: &ShardId) -> Option<Weak<dyn DynState>> {
        self.states
            .lock()
            .expect("lock")
            .get(shard_id)
            .and_then(|x| x.get())
            .map(Weak::clone)
    }

    #[cfg(test)]
    fn get_cached(&self, shard_id: &ShardId) -> Option<Arc<dyn DynState>> {
        self.states
            .lock()
            .expect("lock")
            .get(shard_id)
            .and_then(|x| x.get())
            .and_then(|x| x.upgrade())
    }

    #[cfg(test)]
    fn initialized_count(&self) -> usize {
        self.states
            .lock()
            .expect("lock")
            .values()
            .filter(|x| x.initialized())
            .count()
    }

    #[cfg(test)]
    fn strong_count(&self) -> usize {
        self.states
            .lock()
            .expect("lock")
            .values()
            .filter(|x| x.get().map_or(false, |x| x.upgrade().is_some()))
            .count()
    }
}

/// A locked decorator for TypedState that abstracts out the specific lock implementation used.
/// Guards the private lock with public accessor fns to make locking scopes more explicit and
/// simpler to reason about.
pub(crate) struct LockingTypedState<K, V, T, D> {
    shard_id: ShardId,
    state: RwLock<TypedState<K, V, T, D>>,
    notifier: StateWatchNotifier,
    cfg: Arc<PersistConfig>,
    metrics: Arc<Metrics>,
    shard_metrics: Arc<ShardMetrics>,
    _subscription_token: Arc<ShardSubscriptionToken>,
}

impl<K, V, T: Debug, D> Debug for LockingTypedState<K, V, T, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let LockingTypedState {
            shard_id,
            state,
            notifier,
            cfg: _cfg,
            metrics: _metrics,
            shard_metrics: _shard_metrics,
            _subscription_token,
        } = self;
        f.debug_struct("LockingTypedState")
            .field("shard_id", shard_id)
            .field("state", state)
            .field("notifier", notifier)
            .finish()
    }
}

impl<K, V, T, D> LockingTypedState<K, V, T, D> {
    fn new(
        shard_id: ShardId,
        initial_state: TypedState<K, V, T, D>,
        metrics: Arc<Metrics>,
        cfg: Arc<PersistConfig>,
        subscription_token: Arc<ShardSubscriptionToken>,
        diagnostics: &Diagnostics,
    ) -> Self {
        Self {
            shard_id,
            notifier: StateWatchNotifier::new(Arc::clone(&metrics)),
            state: RwLock::new(initial_state),
            cfg: Arc::clone(&cfg),
            shard_metrics: metrics.shards.shard(&shard_id, &diagnostics.shard_name),
            metrics,
            _subscription_token: subscription_token,
        }
    }

    pub(crate) fn shard_id(&self) -> &ShardId {
        &self.shard_id
    }

    pub(crate) fn read_lock<R, F: FnMut(&TypedState<K, V, T, D>) -> R>(
        &self,
        metrics: &LockMetrics,
        mut f: F,
    ) -> R {
        metrics.acquire_count.inc();
        let state = match self.state.try_read() {
            Ok(x) => x,
            Err(TryLockError::WouldBlock) => {
                metrics.blocking_acquire_count.inc();
                let start = Instant::now();
                let state = self.state.read().expect("lock poisoned");
                metrics
                    .blocking_seconds
                    .inc_by(start.elapsed().as_secs_f64());
                state
            }
            Err(TryLockError::Poisoned(err)) => panic!("state read lock poisoned: {}", err),
        };
        f(&state)
    }

    pub(crate) fn write_lock<R, F: FnOnce(&mut TypedState<K, V, T, D>) -> R>(
        &self,
        metrics: &LockMetrics,
        f: F,
    ) -> R {
        metrics.acquire_count.inc();
        let mut state = match self.state.try_write() {
            Ok(x) => x,
            Err(TryLockError::WouldBlock) => {
                metrics.blocking_acquire_count.inc();
                let start = Instant::now();
                let state = self.state.write().expect("lock poisoned");
                metrics
                    .blocking_seconds
                    .inc_by(start.elapsed().as_secs_f64());
                state
            }
            Err(TryLockError::Poisoned(err)) => panic!("state read lock poisoned: {}", err),
        };
        let seqno_before = state.seqno;
        let ret = f(&mut state);
        let seqno_after = state.seqno;
        debug_assert!(seqno_after >= seqno_before);
        if seqno_after > seqno_before {
            self.notifier.notify(seqno_after);
        }
        // For now, make sure to notify while under lock. It's possible to move
        // this out of the lock window, see [StateWatchNotifier::notify].
        drop(state);
        ret
    }

    pub(crate) fn notifier(&self) -> &StateWatchNotifier {
        &self.notifier
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;
    use std::sync::atomic::{AtomicBool, Ordering};

    use futures::stream::{FuturesUnordered, StreamExt};
    use mz_build_info::DUMMY_BUILD_INFO;
    use mz_ore::now::SYSTEM_TIME;
    use mz_ore::task::spawn;

    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn client_cache() {
        let cache = PersistClientCache::new(
            PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        );
        assert_eq!(cache.blob_by_uri.lock().await.len(), 0);
        assert_eq!(cache.consensus_by_uri.lock().await.len(), 0);

        // Opening a location on an empty cache saves the results.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://blob_zero".to_owned(),
                consensus_uri: "mem://consensus_zero".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.lock().await.len(), 1);
        assert_eq!(cache.consensus_by_uri.lock().await.len(), 1);

        // Opening a location with an already opened consensus reuses it, even
        // if the blob is different.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://blob_one".to_owned(),
                consensus_uri: "mem://consensus_zero".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.lock().await.len(), 2);
        assert_eq!(cache.consensus_by_uri.lock().await.len(), 1);

        // Ditto the other way.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://blob_one".to_owned(),
                consensus_uri: "mem://consensus_one".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.lock().await.len(), 2);
        assert_eq!(cache.consensus_by_uri.lock().await.len(), 2);

        // Query params and path matter, so we get new instances.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://blob_one?foo".to_owned(),
                consensus_uri: "mem://consensus_one/bar".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.lock().await.len(), 3);
        assert_eq!(cache.consensus_by_uri.lock().await.len(), 3);

        // User info and port also matter, so we get new instances.
        let _ = cache
            .open(PersistLocation {
                blob_uri: "mem://user@blob_one".to_owned(),
                consensus_uri: "mem://@consensus_one:123".to_owned(),
            })
            .await
            .expect("failed to open location");
        assert_eq!(cache.blob_by_uri.lock().await.len(), 4);
        assert_eq!(cache.consensus_by_uri.lock().await.len(), 4);
    }

    #[mz_ore::test(tokio::test)]
    async fn state_cache() {
        mz_ore::test::init_logging();
        fn new_state<K, V, T, D>(shard_id: ShardId) -> TypedState<K, V, T, D>
        where
            K: Codec,
            V: Codec,
            T: Timestamp + Lattice + Codec64,
            D: Codec64,
        {
            TypedState::new(
                DUMMY_BUILD_INFO.semver_version(),
                shard_id,
                "host".into(),
                0,
            )
        }
        fn assert_same<K, V, T, D>(
            state1: &LockingTypedState<K, V, T, D>,
            state2: &LockingTypedState<K, V, T, D>,
        ) {
            let pointer1 = format!("{:p}", state1.state.read().expect("lock").deref());
            let pointer2 = format!("{:p}", state2.state.read().expect("lock").deref());
            assert_eq!(pointer1, pointer2);
        }

        let s1 = ShardId::new();
        let states = Arc::new(StateCache::new_no_metrics());

        // The cache starts empty.
        assert_eq!(states.states.lock().expect("lock").len(), 0);

        // Panic'ing during init_fn .
        let s = Arc::clone(&states);
        let res = spawn(|| "test", async move {
            s.get::<(), (), u64, i64, _, _>(
                s1,
                || async { panic!("boom") },
                &Diagnostics::for_tests(),
            )
            .await
        })
        .await;
        assert!(res.is_err());
        assert_eq!(states.initialized_count(), 0);

        // Returning an error from init_fn doesn't initialize an entry in the cache.
        let res = states
            .get::<(), (), u64, i64, _, _>(
                s1,
                || async {
                    Err(Box::new(CodecMismatch {
                        requested: ("".into(), "".into(), "".into(), "".into(), None),
                        actual: ("".into(), "".into(), "".into(), "".into(), None),
                    }))
                },
                &Diagnostics::for_tests(),
            )
            .await;
        assert!(res.is_err());
        assert_eq!(states.initialized_count(), 0);

        // Initialize one shard.
        let did_work = Arc::new(AtomicBool::new(false));
        let s1_state1 = states
            .get::<(), (), u64, i64, _, _>(
                s1,
                || {
                    let did_work = Arc::clone(&did_work);
                    async move {
                        did_work.store(true, Ordering::SeqCst);
                        Ok(new_state(s1))
                    }
                },
                &Diagnostics::for_tests(),
            )
            .await
            .expect("should successfully initialize");
        assert_eq!(did_work.load(Ordering::SeqCst), true);
        assert_eq!(states.initialized_count(), 1);
        assert_eq!(states.strong_count(), 1);

        // Trying to initialize it again does no work and returns the same state.
        let did_work = Arc::new(AtomicBool::new(false));
        let s1_state2 = states
            .get::<(), (), u64, i64, _, _>(
                s1,
                || {
                    let did_work = Arc::clone(&did_work);
                    async move {
                        did_work.store(true, Ordering::SeqCst);
                        did_work.store(true, Ordering::SeqCst);
                        Ok(new_state(s1))
                    }
                },
                &Diagnostics::for_tests(),
            )
            .await
            .expect("should successfully initialize");
        assert_eq!(did_work.load(Ordering::SeqCst), false);
        assert_eq!(states.initialized_count(), 1);
        assert_eq!(states.strong_count(), 1);
        assert_same(&s1_state1, &s1_state2);

        // Trying to initialize with different types doesn't work.
        let did_work = Arc::new(AtomicBool::new(false));
        let res = states
            .get::<String, (), u64, i64, _, _>(
                s1,
                || {
                    let did_work = Arc::clone(&did_work);
                    async move {
                        did_work.store(true, Ordering::SeqCst);
                        Ok(new_state(s1))
                    }
                },
                &Diagnostics::for_tests(),
            )
            .await;
        assert_eq!(did_work.load(Ordering::SeqCst), false);
        assert_eq!(
            format!("{}", res.expect_err("types shouldn't match")),
            "requested codecs (\"String\", \"()\", \"u64\", \"i64\", Some(CodecConcreteType(\"(alloc::string::String, (), u64, i64)\"))) did not match ones in durable storage (\"()\", \"()\", \"u64\", \"i64\", Some(CodecConcreteType(\"((), (), u64, i64)\")))"
        );
        assert_eq!(states.initialized_count(), 1);
        assert_eq!(states.strong_count(), 1);

        // We can add a shard of a different type.
        let s2 = ShardId::new();
        let s2_state1 = states
            .get::<String, (), u64, i64, _, _>(
                s2,
                || async { Ok(new_state(s2)) },
                &Diagnostics::for_tests(),
            )
            .await
            .expect("should successfully initialize");
        assert_eq!(states.initialized_count(), 2);
        assert_eq!(states.strong_count(), 2);
        let s2_state2 = states
            .get::<String, (), u64, i64, _, _>(
                s2,
                || async { Ok(new_state(s2)) },
                &Diagnostics::for_tests(),
            )
            .await
            .expect("should successfully initialize");
        assert_same(&s2_state1, &s2_state2);

        // The cache holds weak references to State so we reclaim memory if the
        // shards stops being used.
        drop(s1_state1);
        assert_eq!(states.strong_count(), 2);
        drop(s1_state2);
        assert_eq!(states.strong_count(), 1);
        assert_eq!(states.initialized_count(), 2);
        assert!(states.get_cached(&s1).is_none());

        // But we can re-init that shard if necessary.
        let s1_state1 = states
            .get::<(), (), u64, i64, _, _>(
                s1,
                || async { Ok(new_state(s1)) },
                &Diagnostics::for_tests(),
            )
            .await
            .expect("should successfully initialize");
        assert_eq!(states.initialized_count(), 2);
        assert_eq!(states.strong_count(), 2);
        drop(s1_state1);
        assert_eq!(states.strong_count(), 1);
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // too slow
    async fn state_cache_concurrency() {
        mz_ore::test::init_logging();

        const COUNT: usize = 1000;
        let id = ShardId::new();
        let cache = StateCache::new_no_metrics();
        let diagnostics = Diagnostics::for_tests();

        let mut futures = (0..COUNT)
            .map(|_| {
                cache.get::<(), (), u64, i64, _, _>(
                    id,
                    || async {
                        Ok(TypedState::new(
                            DUMMY_BUILD_INFO.semver_version(),
                            id,
                            "host".into(),
                            0,
                        ))
                    },
                    &diagnostics,
                )
            })
            .collect::<FuturesUnordered<_>>();

        for _ in 0..COUNT {
            let _ = futures.next().await.unwrap();
        }
    }
}
