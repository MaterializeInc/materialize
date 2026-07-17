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
///
/// Batches are processed by `READ_TS_PIPELINE_DEPTH` workers, so a new batch
/// can be gathered and its backing read issued while previous batches are
/// still in flight. This pipelining is correct for the same reason batching
/// is: each worker drains its batch _before_ issuing the backing `read_ts`, so
/// the returned timestamp is determined during every waiter's `read_ts` call.
/// Distinct callers whose calls overlap may observe timestamps in either
/// completion order, which linearizability allows for concurrent operations.
/// Calls that do not overlap stay ordered: a call that starts after another
/// completed is served by a backing read issued after the earlier one
/// finished, and the backing oracle's read timestamp never regresses.
pub struct BatchingTimestampOracle<T> {
    inner: Arc<dyn TimestampOracle<T> + Send + Sync>,
    command_tx: UnboundedSender<Command<T>>,
}

/// The number of concurrent read batches that may be in flight against the
/// backing oracle.
///
/// A depth of 1 gives strictly serialized batches: while a batch is in flight,
/// waiters queue up for the next batch, adding up to a full backing-oracle
/// round trip of latency. A depth of 2 lets the next batch be issued
/// immediately, cutting that queueing delay, in exchange for up to twice the
/// query rate against the backing store when reads are saturated.
const READ_TS_PIPELINE_DEPTH: usize = 2;

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
        let (command_tx, command_rx) = tokio::sync::mpsc::unbounded_channel();
        let command_rx = Arc::new(tokio::sync::Mutex::new(command_rx));

        for worker_id in 0..READ_TS_PIPELINE_DEPTH {
            let task_oracle = Arc::clone(&oracle);
            let metrics = Arc::clone(&metrics);
            let command_rx = Arc::clone(&command_rx);

            mz_ore::task::spawn(
                move || format!("BatchingTimestampOracle Worker Task {}", worker_id),
                async move {
                    let read_ts_metrics = &metrics.batching.read_ts;

                    // See comment on `BatchingTimestampOracle` for why this
                    // batching and pipelining is correct.
                    loop {
                        // NOTE: The receiver lock must be released before the
                        // backing read_ts call below, so that another worker
                        // can gather the next batch while this one's read is
                        // in flight. A worker parked on `recv` holds the lock,
                        // which is fine: exactly one worker gathers commands
                        // at a time, the others park on the lock.
                        let pending_cmds = {
                            let mut command_rx = command_rx.lock().await;
                            match command_rx.recv().await {
                                Some(cmd) => {
                                    let mut pending_cmds = vec![cmd];
                                    while let Ok(cmd) = command_rx.try_recv() {
                                        pending_cmds.push(cmd);
                                    }
                                    pending_cmds
                                }
                                None => break,
                            }
                        };

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
                },
            );
        }

        Self {
            inner: oracle,
            command_tx,
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

        self.command_tx.send(Command::ReadTs(tx)).expect(
            "worker task cannot stop while we still have senders for the command/request channel",
        );

        rx.await
            .expect("worker task cannot stop while there are outstanding commands/requests")
    }

    async fn apply_write(&self, write_ts: T) -> T {
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
