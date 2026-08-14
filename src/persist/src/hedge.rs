// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A [Blob] decorator that hedges slow `get` requests.
//!
//! Established connections to the blob store occasionally die in ways that
//! surface only after multiple seconds (a TCP reset after a hang, or a black
//! hole), well before any client timeout fires. A `get` riding such a
//! connection stalls everything downstream of it, while other connections on
//! the same process serve the same store normally. The mitigation, endorsed by
//! the major object stores for idempotent reads, is a hedged request: if the
//! first `get` has not completed within a short delay, race a second one on a
//! connection the first cannot have poisoned, and take whichever succeeds
//! first.
//!
//! Only `get` is hedged. All other [Blob] methods are forwarded to the
//! primary handle untouched: writes, deletes, and restores have side
//! effects, and lists are not latency-critical enough to justify racing a
//! streaming interface. Extending hedging to any of them is forbidden.
//!
//! The hedge handle must not share a connection pool (or DNS state) with the
//! primary, otherwise the hedge can be assigned the very connection that is
//! dying. See [crate::cfg::open_hedge_sibling] for how that isolation is
//! constructed per backend.
//!
//! Hedging operates within a single `retry_external` attempt, before any
//! failure surfaces. The retry ladder, which is what recovers this failure
//! class when hedging is off (at the cost of the full hang), stays untouched
//! as the backstop.
//!
//! NOTE: enabling hedging largely suppresses the old fingerprints of the
//! dead-connection class (client timeout counters, the SDK's
//! connection-poisoning log lines), because the hung request is cancelled
//! before they trigger. The `hedges_won` counter is the replacement signal.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::{Either, select};
use mz_dyncfg::{Config, ConfigSet};
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastLossy;
use mz_ore::task::AbortOnDropHandle;
use tracing::{debug, warn};

use crate::location::{BLOB_GET_LIVENESS_KEY, Blob, BlobMetadata, ExternalError};
use crate::metrics::BlobHedgeMetrics;

pub(crate) const BLOB_HEDGED_GET_ENABLED: Config<bool> = Config::new(
    "persist_blob_hedged_get_enabled",
    false,
    "Whether to hedge slow blob gets with a second request on a separate \
    connection pool (Materialize).",
);

pub(crate) const BLOB_HEDGED_GET_DELAY: Config<Duration> = Config::new(
    "persist_blob_hedged_get_delay",
    Duration::from_secs(2),
    "How long a blob get may be in flight before a hedge request is fired \
    (Materialize).",
);

pub(crate) const BLOB_HEDGED_GET_MAX_CONCURRENT: Config<usize> = Config::new(
    "persist_blob_hedged_get_max_concurrent",
    // Bounds the extra in-flight bytes (which the fetch memory semaphore
    // cannot see) to about two batch parts. The warmer holds this many
    // sockets open, so every admitted hedge can be served warm.
    2,
    "Maximum concurrent hedge requests per blob handle, bounding the memory \
    held by raced gets and the number of warm sockets (Materialize).",
);

pub(crate) const BLOB_HEDGED_GET_BUDGET_RATIO: Config<f64> = Config::new(
    "persist_blob_hedged_get_budget_ratio",
    0.01,
    "Long-run bound on hedge requests as a fraction of blob gets \
    (Materialize).",
);

// NOTE: the warmer only runs while hedging is enabled, so `enabled` stops
// its traffic too. Setting this knob to 0 additionally stops the warmer
// while keeping hedging on, which is why it must be changeable at runtime.
pub(crate) const BLOB_HEDGED_GET_WARM_INTERVAL: Config<Duration> = Config::new(
    "persist_blob_hedged_get_warm_interval",
    Duration::from_secs(20),
    "How often to issue liveness gets that keep the hedge connection pool \
    warm, 0 disables warming without disabling hedging (Materialize).",
);

/// The cost of one hedge in bucket tokens. Micro-token granularity keeps
/// small `budget_ratio` values (down to 1e-6) from rounding to "never
/// refill".
const HEDGE_COST_MICRO_TOKENS: u64 = 1_000_000;

/// Token-bucket capacity: 32 hedges.
///
/// The bucket's shape, not an operational lever: the tuning lever is
/// `persist_blob_hedged_get_budget_ratio` and the kill switch is
/// `persist_blob_hedged_get_enabled`. NOTE: because the bucket starts full,
/// `budget_ratio = 0` still permits ~32 banked hedges before draining. It is
/// not an instant stop, `enabled` is.
const BUDGET_BURST_MICRO_TOKENS: u64 = 32 * HEDGE_COST_MICRO_TOKENS;

/// Why a hedge was not fired for a get that exceeded the delay.
enum HedgeRefused {
    Concurrency,
    Budget,
}

/// Bounds hedge amplification with two independent guards: the concurrency
/// cap bounds memory held by raced gets, the token bucket bounds long-run
/// request-rate/egress amplification (e.g. a store-wide brownout making
/// every get slow, or large gets that legitimately exceed the delay, must
/// not settle into hedging every request).
#[derive(Debug)]
struct HedgeBudget {
    concurrent: AtomicUsize,
    micro_tokens: AtomicU64,
}

impl HedgeBudget {
    fn new() -> Self {
        HedgeBudget {
            concurrent: AtomicUsize::new(0),
            micro_tokens: AtomicU64::new(BUDGET_BURST_MICRO_TOKENS),
        }
    }

    /// Attempts to acquire both guards. The returned guard releases the
    /// concurrency slot on drop. Spent tokens come back only via
    /// [HedgeBudget::replenish].
    fn try_acquire(&self, max_concurrent: usize) -> Result<HedgeGuard<'_>, HedgeRefused> {
        let got_slot = self
            .concurrent
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |c| {
                (c < max_concurrent).then_some(c + 1)
            })
            .is_ok();
        if !got_slot {
            return Err(HedgeRefused::Concurrency);
        }
        let took_token = self
            .micro_tokens
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |t| {
                t.checked_sub(HEDGE_COST_MICRO_TOKENS)
            })
            .is_ok();
        if !took_token {
            self.concurrent.fetch_sub(1, Ordering::SeqCst);
            return Err(HedgeRefused::Budget);
        }
        Ok(HedgeGuard(self))
    }

    /// Adds `ratio` tokens, called once per completed get (hedged or not),
    /// so under sustained slowness hedging settles at `ratio` of traffic.
    fn replenish(&self, ratio: f64) {
        let add = u64::cast_lossy(ratio.clamp(0.0, 1.0) * f64::cast_lossy(HEDGE_COST_MICRO_TOKENS));
        if add == 0 {
            return;
        }
        let _ = self
            .micro_tokens
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |t| {
                Some((t + add).min(BUDGET_BURST_MICRO_TOKENS))
            });
    }
}

struct HedgeGuard<'a>(&'a HedgeBudget);

impl Drop for HedgeGuard<'_> {
    fn drop(&mut self) {
        self.0.concurrent.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The sibling handle a [HedgedBlob] runs hedge requests on, produced by
/// [crate::cfg::open_hedge_sibling].
#[derive(Debug)]
pub enum HedgeSibling {
    /// A handle onto the same durable store with fully separate connection
    /// state, kept warm by the wrapper.
    Isolated(Arc<dyn Blob>),
    /// The backend has no connection state to isolate (or a second open
    /// would observe a different store): hedge on the primary instance
    /// itself, with nothing to warm.
    SharedWithPrimary,
    /// Opening the sibling failed: hedging is unavailable for this process
    /// lifetime.
    Unavailable,
}

/// A [Blob] decorator that hedges slow `get` requests, per the module docs.
#[derive(Debug)]
pub struct HedgedBlob {
    primary: Arc<dyn Blob>,
    /// The handle hedge requests run on. `None` = hedging unavailable,
    /// visible as `hedges_skipped{reason="unavailable"}`.
    hedge: Option<Arc<dyn Blob>>,
    cfg: Arc<ConfigSet>,
    metrics: BlobHedgeMetrics,
    budget: HedgeBudget,
    _warmer: Option<AbortOnDropHandle<()>>,
}

/// Keeps the sibling's connection pool warm with periodic concurrent
/// liveness gets, while hedging is enabled. A cold hedge can stall up to
/// the connect timeout, which during correlated connection events is
/// exactly when it must not. While hedging is disabled the warmer idles
/// and the sibling sees no traffic at all, so a freshly enabled flag can
/// find a cold pool for up to one warm interval plus a handshake. Hedges
/// in that window are merely no better than no hedge, never worse.
fn spawn_warmer(
    hedge: Arc<dyn Blob>,
    cfg: Arc<ConfigSet>,
    metrics: BlobHedgeMetrics,
) -> AbortOnDropHandle<()> {
    mz_ore::task::spawn(|| "persist::blob_hedge_warmer", async move {
        loop {
            let interval = BLOB_HEDGED_GET_WARM_INTERVAL.get(&cfg);
            if !BLOB_HEDGED_GET_ENABLED.get(&cfg) || interval == Duration::ZERO {
                // Nothing to keep warm. Re-check at the configured cadence
                // (or its default while warming is set to 0), so a dyncfg
                // flip takes effect without a restart.
                let recheck = if interval == Duration::ZERO {
                    *BLOB_HEDGED_GET_WARM_INTERVAL.default()
                } else {
                    interval
                };
                tokio::time::sleep(recheck).await;
                continue;
            }
            // Ping first, sleep second, so the pool is warm from process
            // start. As many concurrent pings as hedges can run at once
            // (HTTP/1.1 allows one in-flight request per connection, so N
            // concurrent pings force N warm sockets).
            let start = Instant::now();
            let sockets = BLOB_HEDGED_GET_MAX_CONCURRENT.get(&cfg);
            let pings = (0..sockets).map(|_| hedge.get(BLOB_GET_LIVENESS_KEY));
            let pings = futures_util::future::join_all(pings);
            // Bound the cycle: an unbounded hung ping would block warming
            // past hyper's pool idle eviction, going cold exactly during the
            // correlated events warming exists for. The timeout also drops
            // the hung request, which closes its dying socket.
            match tokio::time::timeout(interval, pings).await {
                Ok(results) if results.iter().all(|r| r.is_ok()) => {
                    metrics.rtt_latency.set(start.elapsed().as_secs_f64());
                }
                Ok(_) | Err(_) => {
                    // A failing or hung warm path means hedges cannot be
                    // trusted to be fast. Surface it, and do not update the
                    // gauge: a fast failure must not report as a fast
                    // healthy path.
                    metrics.warm_errors.inc();
                }
            }
            tokio::time::sleep(interval).await;
        }
    })
    .abort_on_drop()
}

impl HedgedBlob {
    /// Returns a new [HedgedBlob].
    ///
    /// Must be called from within a tokio runtime: it spawns the sibling
    /// warming task.
    pub fn new(
        primary: Arc<dyn Blob>,
        sibling: HedgeSibling,
        cfg: Arc<ConfigSet>,
        metrics: BlobHedgeMetrics,
    ) -> HedgedBlob {
        let (hedge, warmer) = match sibling {
            HedgeSibling::Isolated(h) => {
                let warmer = spawn_warmer(Arc::clone(&h), Arc::clone(&cfg), metrics.clone());
                (Some(h), Some(warmer))
            }
            HedgeSibling::SharedWithPrimary => (Some(Arc::clone(&primary)), None),
            HedgeSibling::Unavailable => (None, None),
        };
        metrics.armed.set(i64::from(hedge.is_some()));
        HedgedBlob {
            primary,
            hedge,
            cfg,
            metrics,
            budget: HedgeBudget::new(),
            _warmer: warmer,
        }
    }

    /// Returns the sibling handle and a budget guard, or `None` (having
    /// already recorded why) if this get must not hedge.
    fn admit(&self) -> Option<(&Arc<dyn Blob>, HedgeGuard<'_>)> {
        let Some(hedge_blob) = &self.hedge else {
            self.metrics.skipped_unavailable.inc();
            return None;
        };
        match self
            .budget
            .try_acquire(BLOB_HEDGED_GET_MAX_CONCURRENT.get(&self.cfg))
        {
            Ok(guard) => Some((hedge_blob, guard)),
            Err(HedgeRefused::Concurrency) => {
                self.metrics.skipped_concurrency.inc();
                None
            }
            Err(HedgeRefused::Budget) => {
                self.metrics.skipped_budget.inc();
                None
            }
        }
    }

    fn record_win(&self, key: &str, start: Instant) {
        self.metrics.won.inc();
        self.metrics
            .won_seconds
            .observe(start.elapsed().as_secs_f64());
        debug!(%key, elapsed = ?start.elapsed(), "blob get won by hedge request");
    }

    async fn get_hedged(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let start = Instant::now();
        let delay = BLOB_HEDGED_GET_DELAY.get(&self.cfg);
        let mut primary = std::pin::pin!(self.primary.get(key));
        // NOTE: both races below rely on `select` polling its first argument
        // first: a primary that is ready exactly at the delay boundary wins
        // without firing a hedge, and a primary that is ready simultaneously
        // with the hedge is never miscredited as a hedge win. tokio::select!
        // does NOT have this property unless marked `biased`.
        if let Either::Left((res, _sleep)) =
            select(primary.as_mut(), std::pin::pin!(tokio::time::sleep(delay))).await
        {
            // The fast path: the primary's result verbatim, success or
            // error. A fast error stays on the ordinary retry-ladder path,
            // since hedging targets hangs, not failures.
            return res;
        }
        let Some((hedge_blob, guard)) = self.admit() else {
            return primary.await;
        };
        self.metrics.fired.inc();
        let mut hedge = std::pin::pin!(hedge_blob.get(key));
        // First success wins. The losing future is dropped, which cancels
        // the request in flight (there is no task boundary between here and
        // the backend). An error on one leg does not end the race: the slow
        // leg is expected to be a hung request, and a fast-failing hedge
        // must not convert a get that was about to succeed into an error.
        match select(primary.as_mut(), hedge.as_mut()).await {
            Either::Left((Ok(res), _hedge)) => Ok(res),
            Either::Right((Ok(res), _primary)) => {
                self.record_win(key, start);
                Ok(res)
            }
            Either::Left((Err(primary_err), hedge)) => {
                // The primary failed after the hedge fired. If the hedge is
                // healthy it wins within about one round trip, so wait a
                // bounded extra window for it. Without the bound, a slow
                // hedge would hold the get far past the point where
                // returning the error to the retry ladder (which recovers
                // this failure class reliably) is the better move.
                match tokio::time::timeout(delay, hedge).await {
                    Ok(Ok(res)) => {
                        self.record_win(key, start);
                        Ok(res)
                    }
                    Ok(Err(hedge_err)) => {
                        self.metrics.errors.inc();
                        warn!(%key, %hedge_err, "hedged blob get: both requests failed");
                        // Callers see the same error object they would have
                        // seen without hedging. Do not attach the hedge
                        // error as context: ExternalError::is_timeout
                        // matches on the error string, so appending text can
                        // change how the error is classified.
                        Err(primary_err)
                    }
                    Err(_elapsed) => {
                        warn!(%key, "hedged blob get: primary failed, hedge still pending");
                        Err(primary_err)
                    }
                }
            }
            Either::Right((Err(hedge_err), primary)) => {
                self.metrics.errors.inc();
                warn!(%key, %hedge_err, "hedge request failed, awaiting primary");
                // The hedge leg is gone, so the concurrency slot no longer
                // bounds any in-flight memory. Release it rather than
                // pinning it for the primary's remaining hang, which could
                // starve other gets of their hedges during exactly the
                // events hedging exists for.
                drop(guard);
                primary.await
            }
        }
    }
}

#[async_trait]
impl Blob for HedgedBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        if !BLOB_HEDGED_GET_ENABLED.get(&self.cfg) {
            return self.primary.get(key).await;
        }
        let res = self.get_hedged(key).await;
        self.budget
            .replenish(BLOB_HEDGED_GET_BUDGET_RATIO.get(&self.cfg));
        res
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.primary.list_keys_and_metadata(key_prefix, f).await
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        self.primary.set(key, value).await
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        self.primary.delete(key).await
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        self.primary.restore(key).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use anyhow::anyhow;
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::metrics::MetricsRegistry;

    use crate::location::tests::blob_impl_test;
    use crate::mem::MemMultiRegistry;

    use super::*;

    /// A test [Blob] whose `get` sleeps a fixed delay and then returns a
    /// fixed outcome, counting calls.
    #[derive(Debug)]
    struct TestBlob {
        delay: Duration,
        outcome: Result<Option<&'static str>, &'static str>,
        gets: AtomicUsize,
    }

    impl TestBlob {
        fn new(
            delay: Duration,
            outcome: Result<Option<&'static str>, &'static str>,
        ) -> Arc<TestBlob> {
            Arc::new(TestBlob {
                delay,
                outcome,
                gets: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl Blob for TestBlob {
        async fn get(&self, _key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            match self.outcome {
                Ok(x) => Ok(x.map(|x| SegmentedBytes::from(Bytes::from(x)))),
                Err(msg) => Err(ExternalError::from(anyhow!(msg))),
            }
        }

        async fn list_keys_and_metadata(
            &self,
            _key_prefix: &str,
            _f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
        ) -> Result<(), ExternalError> {
            unreachable!("test blob only supports get")
        }

        async fn set(&self, _key: &str, _value: Bytes) -> Result<(), ExternalError> {
            unreachable!("test blob only supports get")
        }

        async fn delete(&self, _key: &str) -> Result<Option<usize>, ExternalError> {
            unreachable!("test blob only supports get")
        }

        async fn restore(&self, _key: &str) -> Result<(), ExternalError> {
            unreachable!("test blob only supports get")
        }
    }

    fn test_cfg(customize: impl FnOnce(&mut ConfigUpdates)) -> Arc<ConfigSet> {
        let cfg = crate::cfg::all_dyn_configs(ConfigSet::default());
        let mut updates = ConfigUpdates::default();
        updates.add(&BLOB_HEDGED_GET_ENABLED, true);
        customize(&mut updates);
        updates.apply(&cfg);
        Arc::new(cfg)
    }

    fn metrics() -> BlobHedgeMetrics {
        BlobHedgeMetrics::new(&MetricsRegistry::new())
    }

    fn hedged(primary: &Arc<TestBlob>, hedge: &Arc<TestBlob>, cfg: Arc<ConfigSet>) -> HedgedBlob {
        let primary: Arc<dyn Blob> = Arc::<TestBlob>::clone(primary);
        let hedge: Arc<dyn Blob> = Arc::<TestBlob>::clone(hedge);
        HedgedBlob::new(primary, HedgeSibling::Isolated(hedge), cfg, metrics())
    }

    const SECS: fn(u64) -> Duration = Duration::from_secs;

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn fast_primary_no_hedge() {
        let primary = TestBlob::new(SECS(0), Ok(Some("x")));
        let hedge = TestBlob::new(SECS(0), Ok(Some("x")));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        assert!(blob.get("k").await.unwrap().is_some());
        assert_eq!(hedge.gets.load(Ordering::SeqCst), 0);
        assert_eq!(blob.metrics.fired.get(), 0);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn hedge_wins_and_cancels_primary() {
        let primary = TestBlob::new(SECS(3600), Ok(Some("slow")));
        let hedge = TestBlob::new(SECS(0), Ok(Some("fast")));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        let start = tokio::time::Instant::now();
        let res = blob.get("k").await.unwrap().expect("some");
        // The hedge's value won, and it won at exactly the hedge delay, not
        // at the primary's 3600s: the primary was cancelled while pending.
        assert_eq!(res.into_contiguous(), b"fast".to_vec());
        assert_eq!(start.elapsed(), SECS(2));
        assert_eq!(blob.metrics.fired.get(), 1);
        assert_eq!(blob.metrics.won.get(), 1);
        assert_eq!(blob.metrics.won_seconds.get_sample_count(), 1);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn primary_wins_after_hedge_fired() {
        let primary = TestBlob::new(SECS(3), Ok(Some("primary")));
        let hedge = TestBlob::new(SECS(3600), Ok(Some("hedge")));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        let res = blob.get("k").await.unwrap().expect("some");
        assert_eq!(res.into_contiguous(), b"primary".to_vec());
        assert_eq!(blob.metrics.fired.get(), 1);
        assert_eq!(blob.metrics.won.get(), 0);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn hedge_error_does_not_fail_get() {
        let primary = TestBlob::new(SECS(5), Ok(Some("primary")));
        let hedge = TestBlob::new(SECS(0), Err("hedge boom"));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        // First success wins, not first completion: the hedge fails fast at
        // the 2s mark but the primary's later success is returned.
        let res = blob.get("k").await.unwrap().expect("some");
        assert_eq!(res.into_contiguous(), b"primary".to_vec());
        assert_eq!(blob.metrics.errors.get(), 1);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn primary_error_then_hedge_success() {
        let primary = TestBlob::new(SECS(3), Err("primary boom"));
        let hedge = TestBlob::new(SECS(2), Ok(Some("hedge")));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        let res = blob.get("k").await.unwrap().expect("some");
        assert_eq!(res.into_contiguous(), b"hedge".to_vec());
        assert_eq!(blob.metrics.won.get(), 1);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn primary_error_then_hedge_error() {
        // Both legs fail with the primary failing first: the hedge's error
        // within the grace window is counted, the primary's error returned.
        let primary = TestBlob::new(SECS(3), Err("primary boom"));
        let hedge = TestBlob::new(Duration::from_millis(1500), Err("hedge boom"));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        let err = blob.get("k").await.unwrap_err();
        assert!(err.to_string().contains("primary boom"), "{}", err);
        assert!(!err.to_string().contains("hedge boom"), "{}", err);
        assert_eq!(blob.metrics.errors.get(), 1);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn primary_error_hedge_timeout() {
        // The primary fails after the hedge fired, and the hedge is slow:
        // the get returns the primary's error after a bounded extra wait
        // instead of holding on the hedge indefinitely.
        let primary = TestBlob::new(SECS(3), Err("primary boom"));
        let hedge = TestBlob::new(SECS(3600), Ok(Some("hedge")));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        let start = tokio::time::Instant::now();
        let err = blob.get("k").await.unwrap_err();
        assert!(err.to_string().contains("primary boom"), "{}", err);
        // Primary error at 3s plus the delay-sized grace window.
        assert_eq!(start.elapsed(), SECS(5));
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn dropped_get_releases_concurrency_slot() {
        // Dropping a hedged get mid-race must release the concurrency slot,
        // else abandoned gets would permanently disable hedging.
        let primary = TestBlob::new(SECS(3600), Ok(Some("slow")));
        let hedge = TestBlob::new(SECS(3600), Ok(Some("slow")));
        let cfg = test_cfg(|u| u.add(&BLOB_HEDGED_GET_MAX_CONCURRENT, 1));
        let blob = hedged(&primary, &hedge, cfg);
        for expected_fired in [1, 2] {
            let res = tokio::time::timeout(SECS(10), blob.get("k")).await;
            assert!(res.is_err(), "get should still be pending at timeout");
            assert_eq!(blob.metrics.fired.get(), expected_fired);
        }
        assert_eq!(blob.metrics.skipped_concurrency.get(), 0);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn hedge_error_then_primary_error() {
        // Both legs fail with the hedge failing first: the get falls back to
        // awaiting the primary and returns the primary's error.
        let primary = TestBlob::new(SECS(3), Err("primary boom"));
        let hedge = TestBlob::new(SECS(0), Err("hedge boom"));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        let err = blob.get("k").await.unwrap_err();
        assert!(err.to_string().contains("primary boom"), "{}", err);
        assert!(!err.to_string().contains("hedge boom"), "{}", err);
        assert!(!err.is_timeout());
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn fast_primary_error_passthrough() {
        let primary = TestBlob::new(SECS(0), Err("fast fail"));
        let hedge = TestBlob::new(SECS(0), Ok(Some("hedge")));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        assert!(blob.get("k").await.is_err());
        assert_eq!(hedge.gets.load(Ordering::SeqCst), 0);
        assert_eq!(blob.metrics.fired.get(), 0);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn ok_none_wins() {
        let primary = TestBlob::new(SECS(3600), Ok(None));
        let hedge = TestBlob::new(SECS(0), Ok(None));
        let blob = hedged(&primary, &hedge, test_cfg(|_| {}));
        assert!(blob.get("k").await.unwrap().is_none());
        assert_eq!(blob.metrics.won.get(), 1);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn disabled_passthrough() {
        let primary = TestBlob::new(SECS(0), Ok(Some("x")));
        let hedge = TestBlob::new(SECS(0), Ok(Some("x")));
        let cfg = test_cfg(|u| u.add(&BLOB_HEDGED_GET_ENABLED, false));
        let blob = hedged(&primary, &hedge, cfg);
        assert!(blob.get("k").await.unwrap().is_some());
        assert_eq!(hedge.gets.load(Ordering::SeqCst), 0);
        assert_eq!(blob.metrics.fired.get(), 0);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn unavailable_sibling() {
        let primary = TestBlob::new(SECS(3), Ok(Some("x")));
        let primary_blob: Arc<dyn Blob> = Arc::<TestBlob>::clone(&primary);
        let blob = HedgedBlob::new(
            primary_blob,
            HedgeSibling::Unavailable,
            test_cfg(|_| {}),
            metrics(),
        );
        assert_eq!(blob.metrics.armed.get(), 0);
        assert!(blob.get("k").await.unwrap().is_some());
        assert_eq!(blob.metrics.skipped_unavailable.get(), 1);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn budget_exhausts_and_refills() {
        let primary = TestBlob::new(SECS(10), Ok(Some("slow")));
        let hedge = TestBlob::new(SECS(0), Ok(Some("fast")));
        // No refill, so the bucket only ever drains.
        let cfg = test_cfg(|u| u.add(&BLOB_HEDGED_GET_BUDGET_RATIO, 0.0));
        let blob = hedged(&primary, &hedge, Arc::clone(&cfg));
        for _ in 0..32 {
            assert!(blob.get("k").await.unwrap().is_some());
        }
        assert_eq!(blob.metrics.fired.get(), 32);
        assert!(blob.get("k").await.unwrap().is_some());
        assert_eq!(blob.metrics.fired.get(), 32);
        assert_eq!(blob.metrics.skipped_budget.get(), 1);
        // Turn refill up to one token per completed get. The next get still
        // finds an empty bucket (refill lands at completion), the one after
        // hedges again.
        let mut updates = ConfigUpdates::default();
        updates.add(&BLOB_HEDGED_GET_BUDGET_RATIO, 1.0);
        updates.apply(&cfg);
        assert!(blob.get("k").await.unwrap().is_some());
        assert_eq!(blob.metrics.skipped_budget.get(), 2);
        assert!(blob.get("k").await.unwrap().is_some());
        assert_eq!(blob.metrics.fired.get(), 33);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn concurrency_cap() {
        let primary = TestBlob::new(SECS(10), Ok(Some("slow")));
        let hedge = TestBlob::new(SECS(5), Ok(Some("fast")));
        let cfg = test_cfg(|u| u.add(&BLOB_HEDGED_GET_MAX_CONCURRENT, 1));
        let blob = hedged(&primary, &hedge, cfg);
        let (a, b) = tokio::join!(blob.get("k1"), blob.get("k2"));
        assert!(a.is_ok() && b.is_ok());
        assert_eq!(blob.metrics.fired.get(), 1);
        assert_eq!(blob.metrics.skipped_concurrency.get(), 1);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn warmer_pings_isolated_sibling() {
        let primary = TestBlob::new(SECS(0), Ok(None));
        let hedge = TestBlob::new(SECS(0), Ok(None));
        let cfg = test_cfg(|_| {});
        let sockets = BLOB_HEDGED_GET_MAX_CONCURRENT.get(&cfg);
        let blob = hedged(&primary, &hedge, cfg);
        // The warmer pings immediately at start, then every 20s, holding as
        // many sockets as hedges can run at once.
        tokio::time::sleep(SECS(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(hedge.gets.load(Ordering::SeqCst), sockets);
        tokio::time::sleep(SECS(20)).await;
        tokio::task::yield_now().await;
        assert_eq!(hedge.gets.load(Ordering::SeqCst), 2 * sockets);
        drop(blob);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn warmer_gated_on_enabled() {
        let primary = TestBlob::new(SECS(0), Ok(None));
        let hedge = TestBlob::new(SECS(0), Ok(None));
        let cfg = test_cfg(|u| u.add(&BLOB_HEDGED_GET_ENABLED, false));
        let blob = hedged(&primary, &hedge, Arc::clone(&cfg));
        // Disabled: the warmer idles, the sibling sees no traffic.
        tokio::time::sleep(SECS(120)).await;
        tokio::task::yield_now().await;
        assert_eq!(hedge.gets.load(Ordering::SeqCst), 0);
        // Enabling at runtime starts warming within one warm interval.
        let mut updates = ConfigUpdates::default();
        updates.add(&BLOB_HEDGED_GET_ENABLED, true);
        updates.apply(&cfg);
        tokio::time::sleep(*BLOB_HEDGED_GET_WARM_INTERVAL.default() + SECS(1)).await;
        tokio::task::yield_now().await;
        assert!(hedge.gets.load(Ordering::SeqCst) > 0);
        drop(blob);
    }

    #[mz_ore::test(tokio::test(start_paused = true))]
    async fn shared_sibling_gets_no_warmer() {
        let primary = TestBlob::new(SECS(0), Ok(None));
        let primary_blob: Arc<dyn Blob> = Arc::<TestBlob>::clone(&primary);
        let blob = HedgedBlob::new(
            primary_blob,
            HedgeSibling::SharedWithPrimary,
            test_cfg(|_| {}),
            metrics(),
        );
        assert!(blob._warmer.is_none());
        assert_eq!(blob.metrics.armed.get(), 1);
        tokio::time::sleep(SECS(60)).await;
        assert_eq!(primary.gets.load(Ordering::SeqCst), 0);
    }

    /// A test [Blob] that delays gets so the hedge (delay 0) fires and wins
    /// on every get in the conformance run below. Non-get methods pass
    /// through undelayed.
    #[derive(Debug)]
    struct SlowGetBlob(Arc<dyn Blob>);

    #[async_trait]
    impl Blob for SlowGetBlob {
        async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
            tokio::time::sleep(Duration::from_millis(2)).await;
            self.0.get(key).await
        }

        async fn list_keys_and_metadata(
            &self,
            key_prefix: &str,
            f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
        ) -> Result<(), ExternalError> {
            self.0.list_keys_and_metadata(key_prefix, f).await
        }

        async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
            self.0.set(key, value).await
        }

        async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
            self.0.delete(key).await
        }

        async fn restore(&self, key: &str) -> Result<(), ExternalError> {
            self.0.restore(key).await
        }
    }

    /// Runs the full [Blob] conformance suite with a hedge racing on every
    /// single get: the primary's gets are artificially delayed while the
    /// hedge reads the same underlying store undelayed, so the hedge fires
    /// and wins throughout.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn hedged_blob_conformance() {
        let registry = Arc::new(tokio::sync::Mutex::new(MemMultiRegistry::new(false)));
        let cfg = test_cfg(|u| {
            u.add(&BLOB_HEDGED_GET_DELAY, Duration::ZERO);
            u.add(&BLOB_HEDGED_GET_BUDGET_RATIO, 1.0);
        });
        let metrics = metrics();
        let metrics_check = metrics.clone();
        blob_impl_test(move |path| {
            let path = path.to_owned();
            let registry = Arc::clone(&registry);
            let cfg = Arc::clone(&cfg);
            let metrics = metrics.clone();
            async move {
                let store: Arc<dyn Blob> = Arc::new(registry.lock().await.blob(&path));
                let primary: Arc<dyn Blob> = Arc::new(SlowGetBlob(Arc::clone(&store)));
                Ok(HedgedBlob::new(
                    primary,
                    HedgeSibling::Isolated(store),
                    cfg,
                    metrics,
                ))
            }
        })
        .await
        .expect("conformance");
        assert!(metrics_check.fired.get() > 0, "no hedge ever fired");
        assert!(metrics_check.won.get() > 0, "no hedge ever won");
    }
}
