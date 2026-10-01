// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A timestamp oracle that wraps a `TimestampOracle` and batches calls
//! to it.

use std::num::NonZeroUsize;
use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::phase::PhaseGuard;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{oneshot, watch};

use crate::metrics::Metrics;
use crate::{TimestampOracle, WriteTimestamp};

/// A batching [`TimestampOracle`] backed by a [`TimestampOracle`]
///
/// This will only batch calls to `read_ts` because the rest of the system
/// already naturally does batching of write-related calls via the group commit
/// mechanism. Write-related calls are passed straight through to the backing
/// oracle.
///
/// For `read_ts` calls, we have to be careful to never cache results from the
/// backing oracle: for the timestamp to be linearized we can never return a
/// result as of an earlier moment, but batching them up is correct because this
/// can only make it so that we return later timestamps. Those later timestamps
/// still fall within the duration of the `read_ts` call and so are linearized.
pub struct BatchingTimestampOracle<T> {
    inner: Arc<dyn TimestampOracle<T> + Send + Sync>,
    command_tx: UnboundedSender<Command<T>>,
    qps: crate::metrics::QpsPhases,
}

/// A command on the internal batching command stream.
enum Command<T> {
    ReadTs(oneshot::Sender<(T, PhaseGuard)>, PhaseGuard),
}

impl<T> std::fmt::Debug for BatchingTimestampOracle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchingTimestampOracle").finish()
    }
}

impl<T> BatchingTimestampOracle<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Creates a batching oracle with one backing-read batch in flight.
    pub fn new(metrics: Arc<Metrics>, oracle: Arc<dyn TimestampOracle<T> + Send + Sync>) -> Self {
        Self::new_with_read_concurrency(metrics, oracle, watch::channel(NonZeroUsize::MIN).1)
    }

    /// Batches fresh reads with a dynamically resolved backing-read concurrency limit.
    ///
    /// Reducing the limit lets existing reads finish before admitting another
    /// batch. Closing the configuration channel retains its last limit.
    pub fn new_with_read_concurrency(
        metrics: Arc<Metrics>,
        oracle: Arc<dyn TimestampOracle<T> + Send + Sync>,
        mut read_concurrency: watch::Receiver<NonZeroUsize>,
    ) -> Self {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_oracle = Arc::clone(&oracle);
        let qps = metrics.qps.clone();

        mz_ore::task::spawn(|| "BatchingTimestampOracle Worker Task", async move {
            let read_ts_metrics = &metrics.batching.read_ts;
            let mut reads = FuturesUnordered::new();
            let mut closed = false;
            let mut config_closed = false;
            while !closed || !reads.is_empty() {
                let limit = *read_concurrency.borrow_and_update();
                let can_admit = !closed && reads.len() < limit.get();
                tokio::select! {
                    // Poll fresh backing reads before admitting more requests.
                    // Ready responses must not starve under continuous arrivals.
                    biased;
                    changed = read_concurrency.changed(), if !config_closed => {
                        config_closed = changed.is_err();
                    }
                    _ = reads.next(), if !reads.is_empty() => {}
                    cmd = command_rx.recv(), if can_admit => {
                        let Some(cmd) = cmd else {
                            closed = true;
                            continue;
                        };
                        let mut pending_cmds = vec![cmd];
                        // A finite queue snapshot prevents arrivals from keeping
                        // the drain open indefinitely and delaying the backing read.
                        let queued = command_rx.len();
                        for _ in 0..queued {
                            if let Ok(cmd) = command_rx.try_recv() {
                                pending_cmds.push(cmd);
                            }
                        }
                        read_ts_metrics.ops_count.inc_by(u64::cast_from(pending_cmds.len()));
                        read_ts_metrics.batches_count.inc();

                        let oracle = Arc::clone(&task_oracle);
                        let phases = metrics.qps.clone();
                        reads.push(async move {
                            // Membership is closed before this fresh observation.
                            // Overlapping batches can complete out of order, so
                            // each result belongs only to its own waiting requests.
                            for Command::ReadTs(_, queued) in &mut pending_cmds {
                                std::mem::take(queued).finish();
                            }
                            let ts = phases.backing_read.time(oracle.read_ts()).await;
                            for Command::ReadTs(response_tx, _) in pending_cmds {
                                let response = (ts.clone(), phases.response_resume.start());
                                let _ = response_tx.send(response);
                            }
                        });
                    }
                }
            }

            tracing::debug!("shutting down BatchingTimestampOracle task");
        });

        Self {
            inner: oracle,
            command_tx,
            qps,
        }
    }
}

/// Waits forever, for a `read_ts` whose worker task is no longer there to
/// answer it.
///
/// The worker task owns both ends of the internal channels, and while a caller
/// holds a sender the only thing that ends it is the Tokio runtime dropping it
/// during shutdown: a panic in the task would abort the process instead
/// (`mz_ore::panic::install_enhanced_handler`). There is no timestamp that
/// could be invented here without breaking linearizability, so the caller waits
/// and shutdown drops its task at this await point.
async fn worker_task_gone<T>() -> T {
    tracing::debug!("BatchingTimestampOracle worker task is gone, parking read_ts");
    std::future::pending().await
}

#[async_trait]
impl<T> TimestampOracle<T> for BatchingTimestampOracle<T>
where
    T: Send + Sync,
{
    async fn write_ts(&self) -> WriteTimestamp<T> {
        self.inner.write_ts().await
    }

    async fn peek_write_ts(&self) -> T {
        self.inner.peek_write_ts().await
    }

    async fn read_ts(&self) -> T {
        let (tx, rx) = oneshot::channel();
        self.qps
            .read_total
            .time(async {
                if self
                    .command_tx
                    .send(Command::ReadTs(tx, self.qps.queue.start()))
                    .is_err()
                {
                    return worker_task_gone().await;
                }
                match rx.await {
                    Ok((ts, resume)) => {
                        resume.finish();
                        ts
                    }
                    Err(_) => worker_task_gone().await,
                }
            })
            .await
    }

    async fn apply_write(&self, write_ts: T) {
        self.inner.apply_write(write_ts).await
    }
}

#[cfg(test)]
mod tests {

    use mz_ore::metrics::MetricsRegistry;
    use mz_repr::Timestamp;
    use tracing::info;

    use crate::postgres_oracle::{PostgresTimestampOracle, PostgresTimestampOracleConfig};

    use super::*;

    fn fixed_read_concurrency(limit: usize) -> watch::Receiver<NonZeroUsize> {
        watch::channel(NonZeroUsize::new(limit).expect("positive limit")).1
    }

    /// An oracle that answers nothing, for tests that only exercise the
    /// batching wrapper's own plumbing.
    #[derive(Debug)]
    struct PendingOracle;

    #[async_trait]
    impl TimestampOracle<Timestamp> for PendingOracle {
        async fn write_ts(&self) -> WriteTimestamp<Timestamp> {
            std::future::pending().await
        }

        async fn peek_write_ts(&self) -> Timestamp {
            std::future::pending().await
        }

        async fn read_ts(&self) -> Timestamp {
            std::future::pending().await
        }

        async fn apply_write(&self, _write_ts: Timestamp) {
            std::future::pending().await
        }
    }

    /// Runtime shutdown drops the worker task while callers still hold the
    /// oracle, so `read_ts` must wait rather than panic on the closed channel.
    /// Dropping the runtime the worker was spawned on, while keeping the oracle
    /// alive on another one, reproduces that state deterministically.
    #[mz_ore::test]
    fn test_read_ts_waits_when_worker_task_is_gone() {
        let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));

        let worker_runtime = tokio::runtime::Runtime::new().expect("can build runtime");
        let oracle = worker_runtime
            .block_on(async { BatchingTimestampOracle::new(metrics, Arc::new(PendingOracle)) });
        drop(worker_runtime);

        let caller_runtime = tokio::runtime::Runtime::new().expect("can build runtime");
        caller_runtime.block_on(async {
            let read_ts = std::pin::pin!(oracle.read_ts());
            assert!(futures::poll!(read_ts).is_pending());
        });
    }

    #[derive(Debug)]
    struct ControlledOracle {
        calls: tokio::sync::mpsc::UnboundedSender<oneshot::Sender<u64>>,
    }

    #[mz_ore::test(tokio::test)]
    async fn pipelined_reads_preserve_membership_and_complete_out_of_order() {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let (calls_tx, mut calls_rx) = tokio::sync::mpsc::unbounded_channel();
            let oracle = BatchingTimestampOracle::new_with_read_concurrency(
                Arc::new(Metrics::new(&MetricsRegistry::new())),
                Arc::new(ControlledOracle { calls: calls_tx }),
                fixed_read_concurrency(2),
            );

            let first = oracle.read_ts();
            tokio::pin!(first);
            assert!(futures::poll!(&mut first).is_pending());
            let backing_first = calls_rx.recv().await.expect("first backing read");

            // This request arrives after the first backing observation starts.
            let second = oracle.read_ts();
            tokio::pin!(second);
            assert!(futures::poll!(&mut second).is_pending());
            let backing_second = calls_rx
                .recv()
                .await
                .expect("second starts while first pending");
            backing_second.send(20).expect("second still in flight");
            assert_eq!(second.await, 20);
            assert!(futures::poll!(&mut first).is_pending());

            let third = oracle.read_ts();
            tokio::pin!(third);
            assert!(futures::poll!(&mut third).is_pending());
            let backing_third = calls_rx
                .recv()
                .await
                .expect("third needs a fresh observation");
            backing_third.send(30).expect("third still in flight");
            assert_eq!(third.await, 30);

            // The older overlapping call owns its older observation. It must
            // not steal another batch's timestamp or delay that batch's reply.
            backing_first.send(10).expect("first still in flight");
            assert_eq!(first.await, 10);

            let fourth = oracle.read_ts();
            tokio::pin!(fourth);
            assert!(futures::poll!(&mut fourth).is_pending());
            let backing_fourth = calls_rx.recv().await.expect("no cached reply");
            backing_fourth.send(40).expect("fourth still in flight");
            assert_eq!(fourth.await, 40);
        })
        .await
        .expect("pipeline made progress");
    }

    #[mz_ore::test(tokio::test)]
    async fn pipelined_reads_bound_concurrency_and_survive_cancellation() {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let (calls_tx, mut calls_rx) = tokio::sync::mpsc::unbounded_channel();
            let oracle = BatchingTimestampOracle::new_with_read_concurrency(
                Arc::new(Metrics::new(&MetricsRegistry::new())),
                Arc::new(ControlledOracle { calls: calls_tx }),
                fixed_read_concurrency(2),
            );
            let mut first = Box::pin(oracle.read_ts());
            assert!(futures::poll!(&mut first).is_pending());
            let backing_first = calls_rx.recv().await.expect("first backing read");
            let mut second = Box::pin(oracle.read_ts());
            assert!(futures::poll!(&mut second).is_pending());
            let backing_second = calls_rx.recv().await.expect("second backing read");

            let third = oracle.read_ts();
            tokio::pin!(third);
            assert!(futures::poll!(&mut third).is_pending());
            tokio::task::yield_now().await;
            assert!(
                futures::poll!(std::pin::pin!(calls_rx.recv())).is_pending(),
                "no third backing read at capacity"
            );
            drop(first);
            tokio::task::yield_now().await;
            assert!(
                futures::poll!(std::pin::pin!(calls_rx.recv())).is_pending(),
                "cancellation does not release a backing slot"
            );

            backing_second.send(20).expect("second still in flight");
            assert_eq!(second.await, 20);
            let backing_third = calls_rx
                .recv()
                .await
                .expect("third starts after slot completes");
            backing_third.send(30).expect("third still in flight");
            assert_eq!(third.await, 30);
            backing_first
                .send(10)
                .expect("cancelled caller does not cancel backing read");

            let fourth = oracle.read_ts();
            tokio::pin!(fourth);
            assert!(futures::poll!(&mut fourth).is_pending());
            let backing_fourth = calls_rx
                .recv()
                .await
                .expect("worker survives cancelled reply");
            backing_fourth.send(40).expect("fourth still in flight");
            assert_eq!(fourth.await, 40);
        })
        .await
        .expect("pipeline made progress");
    }

    #[mz_ore::test(tokio::test)]
    async fn read_concurrency_reduction_drains_existing_batches() {
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let (calls_tx, mut calls_rx) = tokio::sync::mpsc::unbounded_channel();
            let (limit, updates) = watch::channel(NonZeroUsize::new(2).expect("positive limit"));
            let oracle = BatchingTimestampOracle::new_with_read_concurrency(
                Arc::new(Metrics::new(&MetricsRegistry::new())),
                Arc::new(ControlledOracle { calls: calls_tx }),
                updates,
            );
            let first = oracle.read_ts();
            tokio::pin!(first);
            assert!(futures::poll!(&mut first).is_pending());
            let backing_first = calls_rx.recv().await.expect("first backing read");
            let second = oracle.read_ts();
            tokio::pin!(second);
            assert!(futures::poll!(&mut second).is_pending());
            let backing_second = calls_rx.recv().await.expect("second backing read");

            limit.send_replace(NonZeroUsize::MIN);
            let third = oracle.read_ts();
            tokio::pin!(third);
            assert!(futures::poll!(&mut third).is_pending());
            backing_second
                .send(20)
                .expect("in-flight read is not cancelled");
            assert_eq!(second.await, 20);
            tokio::task::yield_now().await;
            assert!(futures::poll!(std::pin::pin!(calls_rx.recv())).is_pending());
            backing_first.send(10).expect("first still in flight");
            assert_eq!(first.await, 10);
            let backing_third = calls_rx.recv().await.expect("new batch after both drain");

            limit.send_replace(NonZeroUsize::new(2).expect("positive limit"));
            let fourth = oracle.read_ts();
            tokio::pin!(fourth);
            assert!(futures::poll!(&mut fourth).is_pending());
            let backing_fourth = calls_rx
                .recv()
                .await
                .expect("higher limit admits a second batch");
            backing_fourth.send(40).expect("fourth still in flight");
            assert_eq!(fourth.await, 40);
            backing_third.send(30).expect("third still in flight");
            assert_eq!(third.await, 30);
        })
        .await
        .expect("limit changes made progress");
    }

    #[mz_ore::test]
    fn shutdown_drops_two_in_flight_batches_without_inventing_timestamps() {
        let worker_runtime = tokio::runtime::Runtime::new().expect("worker runtime");
        let (calls_tx, mut calls_rx) = tokio::sync::mpsc::unbounded_channel();
        let oracle = worker_runtime.block_on(async {
            BatchingTimestampOracle::new_with_read_concurrency(
                Arc::new(Metrics::new(&MetricsRegistry::new())),
                Arc::new(ControlledOracle { calls: calls_tx }),
                fixed_read_concurrency(2),
            )
        });
        let mut first = Box::pin(oracle.read_ts());
        let mut second = Box::pin(oracle.read_ts());
        let (backing_first, backing_second) = worker_runtime.block_on(async {
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                assert!(futures::poll!(&mut first).is_pending());
                let backing_first = calls_rx.recv().await.expect("first backing read");
                assert!(futures::poll!(&mut second).is_pending());
                let backing_second = calls_rx.recv().await.expect("second backing read");
                (backing_first, backing_second)
            })
            .await
            .expect("both batches started")
        });
        drop(worker_runtime);
        assert!(backing_first.send(10).is_err());
        assert!(backing_second.send(20).is_err());
        let caller_runtime = tokio::runtime::Runtime::new().expect("caller runtime");
        caller_runtime.block_on(async {
            assert!(futures::poll!(&mut first).is_pending());
            assert!(futures::poll!(&mut second).is_pending());
        });
    }

    #[async_trait]
    impl TimestampOracle<u64> for ControlledOracle {
        async fn read_ts(&self) -> u64 {
            let (tx, rx) = oneshot::channel();
            self.calls.send(tx).expect("test receiver alive");
            rx.await.expect("test supplies timestamp")
        }
        async fn write_ts(&self) -> WriteTimestamp<u64> {
            panic!("test only reads")
        }
        async fn peek_write_ts(&self) -> u64 {
            panic!("test only reads")
        }
        async fn apply_write(&self, _: u64) {
            panic!("test only reads")
        }
    }

    // A late arrival must not join a read that is already in progress. Exercise
    // all diagnostic modes without a metadata store or global env mutation.
    #[mz_ore::test(tokio::test)]
    async fn qps_diagnostics_preserve_batch_boundaries_and_cancellation() {
        use mz_ore::metrics::phase::Mode;
        for mode in [Mode::Off, Mode::Wall, Mode::Poll] {
            for limit in [1, 2] {
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    let (calls_tx, mut calls_rx) = tokio::sync::mpsc::unbounded_channel();
                    let mut metrics = Metrics::new(&MetricsRegistry::new());
                    let registry = MetricsRegistry::new();
                    metrics.qps = crate::metrics::QpsPhases::new(&registry, mode);
                    let oracle = BatchingTimestampOracle::new_with_read_concurrency(
                        Arc::new(metrics),
                        Arc::new(ControlledOracle { calls: calls_tx }),
                        fixed_read_concurrency(limit),
                    );

                    let first = oracle.read_ts();
                    tokio::pin!(first);
                    assert!(futures::poll!(&mut first).is_pending());
                    let backing_first = calls_rx.recv().await.expect("first batch");

                    // The backing read is already running before the next request.
                    let second = oracle.read_ts();
                    tokio::pin!(second);
                    assert!(futures::poll!(&mut second).is_pending());
                    backing_first.send(10).expect("worker waiting");
                    assert_eq!(first.await, 10);
                    let backing_second = calls_rx.recv().await.expect("new batch");
                    backing_second.send(20).expect("worker waiting");
                    assert_eq!(second.await, 20);

                    // Losing a caller must not kill the worker or cache its result.
                    let mut cancelled = Box::pin(oracle.read_ts());
                    assert!(futures::poll!(&mut cancelled).is_pending());
                    let backing_cancelled = calls_rx.recv().await.expect("cancelled batch");
                    drop(cancelled);
                    backing_cancelled.send(30).expect("worker still waiting");

                    let fourth = oracle.read_ts();
                    tokio::pin!(fourth);
                    assert!(futures::poll!(&mut fourth).is_pending());
                    let backing_fourth = calls_rx.recv().await.expect("fresh batch after cancel");
                    backing_fourth.send(40).expect("worker still alive");
                    assert_eq!(fourth.await, 40);

                    let gathered = registry.gather();
                    let active = gathered
                        .iter()
                        .find(|family| family.name() == "mz_ts_oracle_qps_phase_active");
                    if mode == Mode::Off {
                        assert!(active.is_none());
                    } else {
                        let active = active.expect("enabled phase gauges");
                        for metric in active.get_metric() {
                            assert_eq!(metric.get_gauge().value(), 0.0);
                        }
                        let durations = gathered
                            .iter()
                            .find(|family| family.name() == "mz_ts_oracle_qps_phase_seconds")
                            .expect("enabled histograms");
                        let sample_count = |phase, outcome, kind| {
                            durations
                                .get_metric()
                                .iter()
                                .find(|metric| {
                                    [("phase", phase), ("outcome", outcome), ("kind", kind)]
                                        .iter()
                                        .all(|(name, value)| {
                                            metric.get_label().iter().any(|label| {
                                                label.name() == *name && label.value() == *value
                                            })
                                        })
                                })
                                .expect("bound metric")
                                .get_histogram()
                                .sample_count()
                        };
                        assert_eq!(sample_count("read_total", "returned", "wall"), 3);
                        assert_eq!(sample_count("read_total", "dropped", "wall"), 1);
                        assert_eq!(sample_count("batch_backing_read", "returned", "wall"), 4);
                        assert_eq!(sample_count("response_resume", "returned", "wall"), 3);
                        assert_eq!(sample_count("response_resume", "dropped", "wall"), 1);
                    }
                })
                .await
                .expect("test did not make progress");
            }
        }
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn pipelined_read_only_oracle_observes_another_oracles_applies()
    -> Result<(), anyhow::Error> {
        let Some(config) = PostgresTimestampOracleConfig::new_for_test() else {
            info!("metadata backend not configured: skipping external publication test");
            return Ok(());
        };
        let timeline = uuid::Uuid::new_v4().to_string();
        let now = mz_ore::now::NowFn::from(|| 0u64);
        let writer = PostgresTimestampOracle::open(
            config.clone(),
            timeline.clone(),
            Timestamp::MIN,
            now.clone(),
            false,
        )
        .await;
        let reader =
            PostgresTimestampOracle::open(config, timeline, Timestamp::MIN, now, true).await;
        let reader = BatchingTimestampOracle::new_with_read_concurrency(
            Arc::new(Metrics::new(&MetricsRegistry::new())),
            Arc::new(reader),
            fixed_read_concurrency(2),
        );
        assert_eq!(reader.read_ts().await, Timestamp::MIN);
        writer.apply_write(Timestamp::from(50u64)).await;
        assert_eq!(reader.read_ts().await, Timestamp::from(50u64));
        let write = writer.write_ts().await;
        assert!(write.timestamp > Timestamp::from(50u64));
        assert_eq!(reader.read_ts().await, Timestamp::from(50u64));
        writer.apply_write(write.timestamp).await;
        assert_eq!(reader.read_ts().await, write.timestamp);
        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_batching_timestamp_oracle() -> Result<(), anyhow::Error> {
        let config = match PostgresTimestampOracleConfig::new_for_test() {
            Some(config) => config,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    PostgresTimestampOracleConfig::EXTERNAL_TESTS_POSTGRES_URL
                );
                return Ok(());
            }
        };
        let metrics = Arc::new(Metrics::new(&MetricsRegistry::new()));

        for limit in [1, 2] {
            crate::tests::timestamp_oracle_impl_test(|timeline, now_fn, initial_ts| {
                // We use the postgres oracle as the backing oracle.
                let pg_oracle = PostgresTimestampOracle::open(
                    config.clone(),
                    timeline,
                    initial_ts,
                    now_fn,
                    false, /* read-only */
                );

                async {
                    let arced_pg_oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                        Arc::new(pg_oracle.await);

                    let batching_oracle = BatchingTimestampOracle::new_with_read_concurrency(
                        Arc::clone(&metrics),
                        arced_pg_oracle,
                        fixed_read_concurrency(limit),
                    );

                    let arced_oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                        Arc::new(batching_oracle);

                    arced_oracle
                }
            })
            .await?;
        }

        Ok(())
    }
}
