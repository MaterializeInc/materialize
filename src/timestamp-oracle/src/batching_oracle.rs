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
}

/// A command on the internal batching command stream.
enum Command<T> {
    ReadTs(oneshot::Sender<T>),
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

                let ts = task_oracle.read_ts().await;
                for cmd in pending_cmds {
                    match cmd {
                        Command::ReadTs(response_tx) => {
                            // It's okay if the receiver drops, just means
                            // they're not interested anymore.
                            let _ = response_tx.send(ts.clone());
                        }
                    }
                }
            }

            tracing::debug!("shutting down BatchingTimestampOracle task");
        });

        Self {
            inner: oracle,
            command_tx,
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

        if self.command_tx.send(Command::ReadTs(tx)).is_err() {
            return worker_task_gone().await;
        }

        match rx.await {
            Ok(ts) => ts,
            Err(_) => worker_task_gone().await,
        }
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
