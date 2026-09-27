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

use std::sync::Arc;

use async_trait::async_trait;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::phase::PhaseGuard;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::oneshot;

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
    /// Crates a [`BatchingTimestampOracle`] that uses the given inner oracle.
    pub fn new(metrics: Arc<Metrics>, oracle: Arc<dyn TimestampOracle<T> + Send + Sync>) -> Self {
        let (command_tx, mut command_rx) = tokio::sync::mpsc::unbounded_channel();

        let task_oracle = Arc::clone(&oracle);
        let qps = metrics.qps.clone();

        mz_ore::task::spawn(|| "BatchingTimestampOracle Worker Task", async move {
            let read_ts_metrics = &metrics.batching.read_ts;

            // See comment on `BatchingTimestampOracle` for why this batching is
            // correct.
            while let Some(cmd) = command_rx.recv().await {
                let mut pending_cmds = vec![cmd];
                while let Ok(cmd) = command_rx.try_recv() {
                    pending_cmds.push(cmd);
                }

                read_ts_metrics
                    .ops_count
                    .inc_by(u64::cast_from(pending_cmds.len()));
                read_ts_metrics.batches_count.inc();

                // End the queue interval only after the batch is closed. Never
                // add a later arrival to an already-running backing read.
                for Command::ReadTs(_, queued) in &mut pending_cmds {
                    std::mem::take(queued).finish();
                }
                let ts = metrics.qps.backing_read.time(task_oracle.read_ts()).await;
                for Command::ReadTs(response_tx, _) in pending_cmds {
                    // It's okay if the receiver drops, just means
                    // they're not interested anymore.
                    let _ = response_tx.send((ts.clone(), metrics.qps.response_resume.start()));
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
                self.command_tx.send(Command::ReadTs(tx, self.qps.queue.start())).expect(
            "worker task cannot stop while we still have senders for the command/request channel",
        );

                let (ts, resume) = rx.await.expect(
                    "worker task cannot stop while there are outstanding commands/requests",
                );
                resume.finish();
                ts
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

    #[derive(Debug)]
    struct ControlledOracle {
        calls: tokio::sync::mpsc::UnboundedSender<oneshot::Sender<u64>>,
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
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                let (calls_tx, mut calls_rx) = tokio::sync::mpsc::unbounded_channel();
                let mut metrics = Metrics::new(&MetricsRegistry::new());
                let registry = MetricsRegistry::new();
                metrics.qps = crate::metrics::QpsPhases::new(&registry, mode);
                let oracle = BatchingTimestampOracle::new(
                    Arc::new(metrics),
                    Arc::new(ControlledOracle { calls: calls_tx }),
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

                let batching_oracle =
                    BatchingTimestampOracle::new(Arc::clone(&metrics), arced_pg_oracle);

                let arced_oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                    Arc::new(batching_oracle);

                arced_oracle
            }
        })
        .await?;

        Ok(())
    }
}
