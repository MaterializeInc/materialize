// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A timestamp oracle backed by SQLite for persistence/durability and where
//! all oracle operations are self-sufficiently linearized, without requiring
//! any external precautions/machinery.

use std::sync::Arc;

use async_trait::async_trait;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::task;
use mz_repr::Timestamp;
use rusqlite::params;
use tokio::sync::Mutex;
use tracing::{debug, info};

use crate::WriteTimestamp;
use crate::metrics::Metrics;
use crate::postgres_oracle::retry_fallible;
use crate::{GenericNowFn, TimestampOracle};

// We store timestamps as TEXT (decimal string) to support the full u64 range.
// SQLite's INTEGER type is i64 which cannot hold values above i64::MAX.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS timestamp_oracle (
    timeline TEXT NOT NULL,
    read_ts TEXT NOT NULL,
    write_ts TEXT NOT NULL,
    PRIMARY KEY (timeline)
)
";

/// Configuration to connect to a SQLite-backed implementation of
/// [`TimestampOracle`].
#[derive(Clone, Debug)]
pub struct SqliteTimestampOracleConfig {
    path: String,
    metrics: Arc<Metrics>,
}

impl SqliteTimestampOracleConfig {
    /// Returns a new [`SqliteTimestampOracleConfig`] for the given path.
    pub fn new(path: String, metrics_registry: &MetricsRegistry) -> Self {
        SqliteTimestampOracleConfig {
            path,
            metrics: Arc::new(Metrics::new(metrics_registry)),
        }
    }

    /// Extract the path from a `sqlite://` URL.
    pub fn from_url(
        url: &mz_ore::url::SensitiveUrl,
        metrics_registry: &MetricsRegistry,
    ) -> Result<Self, anyhow::Error> {
        // SensitiveUrl derefs to url::Url.
        let path = if let Some(host) = url.host_str() {
            format!("{}{}", host, url.path())
        } else {
            url.path().to_string()
        };
        if path.is_empty() {
            anyhow::bail!("missing path in sqlite URL: {}", url.as_str());
        }
        Ok(SqliteTimestampOracleConfig {
            path,
            metrics: Arc::new(Metrics::new(metrics_registry)),
        })
    }

    /// Returns a new [`SqliteTimestampOracleConfig`] for use in unit tests.
    #[cfg(test)]
    pub fn new_for_test() -> Result<Self, anyhow::Error> {
        let dir = tempfile::tempdir()?;
        #[allow(deprecated)]
        let path = dir.into_path().join("oracle_test.db");
        Ok(SqliteTimestampOracleConfig {
            path: path.to_string_lossy().into_owned(),
            metrics: Arc::new(Metrics::new(&MetricsRegistry::new())),
        })
    }

    /// Returns the metrics associated with this config.
    pub(crate) fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

/// A [`TimestampOracle`] backed by SQLite.
#[derive(Debug)]
pub struct SqliteTimestampOracle<N>
where
    N: GenericNowFn<Timestamp>,
{
    timeline: String,
    next: N,
    conn: Arc<Mutex<rusqlite::Connection>>,
    metrics: Arc<Metrics>,
    read_only: bool,
}

/// Convert a u64 timestamp to its decimal string representation for storage.
fn ts_to_text(ts: u64) -> String {
    ts.to_string()
}

/// Parse a decimal string timestamp back to u64.
fn text_to_ts(s: &str) -> u64 {
    s.parse::<u64>()
        .expect("stored timestamp should be a valid u64")
}

impl<N> SqliteTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    /// Open a SQLite [`TimestampOracle`] instance with `config`, for the
    /// timeline named `timeline`. `next` generates new timestamps when invoked.
    /// Timestamps that are returned are made durable and will never retract.
    pub async fn open(
        config: SqliteTimestampOracleConfig,
        timeline: String,
        initially: Timestamp,
        next: N,
        read_only: bool,
    ) -> Self {
        info!(config = ?config, "opening SqliteTimestampOracle");

        let fallible = || async {
            let metrics = Arc::clone(&config.metrics);
            let path = config.path.clone();

            let conn = task::spawn_blocking(
                || "timestamp_oracle::sqlite::open",
                move || -> Result<_, anyhow::Error> {
                    let conn = rusqlite::Connection::open(&path)?;
                    conn.execute_batch("PRAGMA journal_mode = WAL;")?;
                    conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
                    conn.execute_batch("PRAGMA synchronous = OFF;")?;
                    conn.execute_batch("PRAGMA journal_size_limit = 0;")?;
                    conn.execute_batch("PRAGMA mmap_size = 67108864;")?;
                    conn.execute_batch("PRAGMA cache_size = -8000;")?;
                    conn.execute_batch("PRAGMA temp_store = MEMORY;")?;
                    conn.execute_batch(SCHEMA)?;
                    Ok(conn)
                },
            )
            .await?;

            let conn = Arc::new(Mutex::new(conn));

            let oracle = SqliteTimestampOracle {
                timeline: timeline.clone(),
                next: next.clone(),
                conn: Arc::clone(&conn),
                metrics,
                read_only,
            };

            // Create a row for our timeline, if it doesn't exist.
            let tl = timeline.clone();
            let initially_text = ts_to_text(initially.into());
            let c = Arc::clone(&conn);
            task::spawn_blocking(
                || "timestamp_oracle::sqlite::open_init_timeline",
                move || -> Result<_, anyhow::Error> {
                    let conn = c.blocking_lock();
                    conn.execute(
                        "INSERT OR IGNORE INTO timestamp_oracle (timeline, read_ts, write_ts)
                             VALUES (?1, ?2, ?3)",
                        params![tl, initially_text, initially_text],
                    )?;
                    Ok(())
                },
            )
            .await?;

            // Forward timestamps to what we're given from outside.
            if !read_only {
                TimestampOracle::apply_write(&oracle, initially).await;
            }

            Ok(oracle)
        };

        let metrics = &config.metrics.retries.open;
        retry_fallible(metrics, fallible).await
    }

    /// Returns a `Vec` of all known timelines along with their current greatest
    /// timestamp (max of read_ts and write_ts).
    pub async fn get_all_timelines(
        config: SqliteTimestampOracleConfig,
    ) -> Result<Vec<(String, Timestamp)>, anyhow::Error> {
        let fallible = || async {
            let path = config.path.clone();
            let result = task::spawn_blocking(
                || "timestamp_oracle::sqlite::get_all_timelines",
                move || -> Result<_, anyhow::Error> {
                    let conn = rusqlite::Connection::open(&path)?;
                    conn.execute_batch("PRAGMA journal_mode = WAL;")?;
                    conn.execute_batch("PRAGMA busy_timeout = 5000;")?;
                    conn.execute_batch("PRAGMA synchronous = OFF;")?;
                    conn.execute_batch("PRAGMA journal_size_limit = 0;")?;
                    conn.execute_batch("PRAGMA mmap_size = 67108864;")?;
                    conn.execute_batch("PRAGMA cache_size = -8000;")?;
                    conn.execute_batch("PRAGMA temp_store = MEMORY;")?;

                    // Check if the table exists.
                    let exists: bool = conn.query_row(
                        "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name='timestamp_oracle')",
                        [],
                        |row| row.get(0),
                    )?;
                    if !exists {
                        return Ok(Vec::new());
                    }

                    let mut stmt =
                        conn.prepare("SELECT timeline, read_ts, write_ts FROM timestamp_oracle")?;
                    let rows = stmt.query_map([], |row| {
                        let timeline: String = row.get(0)?;
                        let read_ts_text: String = row.get(1)?;
                        let write_ts_text: String = row.get(2)?;
                        Ok((timeline, read_ts_text, write_ts_text))
                    })?;

                    let mut result = Vec::new();
                    for row in rows {
                        let (timeline, read_ts_text, write_ts_text) = row?;
                        let read_ts = text_to_ts(&read_ts_text);
                        let write_ts = text_to_ts(&write_ts_text);
                        let ts = std::cmp::max(read_ts, write_ts);
                        result.push((timeline, Timestamp::from(ts)));
                    }
                    Ok(result)
                },
            )
            .await?;

            Ok(result)
        };

        let metrics = &config.metrics.retries.get_all_timelines;
        let result = retry_fallible(metrics, fallible).await;
        Ok(result)
    }

    async fn fallible_write_ts(&self) -> Result<WriteTimestamp<Timestamp>, anyhow::Error> {
        if self.read_only {
            panic!("attempting write_ts in read-only mode");
        }

        let proposed_next_ts: u64 = self.next.now().into();
        let conn = Arc::clone(&self.conn);
        let timeline = self.timeline.clone();

        let write_ts = task::spawn_blocking(
            || "timestamp_oracle::sqlite::write_ts",
            move || -> Result<u64, anyhow::Error> {
                let conn = conn.blocking_lock();
                // Read current write_ts, compute new value, and update.
                // We do this in application code since SQLite stores TEXT, not numeric.
                let current_text: String = conn.query_row(
                    "SELECT write_ts FROM timestamp_oracle WHERE timeline = ?1",
                    params![timeline],
                    |row| row.get(0),
                )?;
                let current = text_to_ts(&current_text);
                let new_write_ts = std::cmp::max(current + 1, proposed_next_ts);
                let new_text = ts_to_text(new_write_ts);
                conn.execute(
                    "UPDATE timestamp_oracle SET write_ts = ?2 WHERE timeline = ?1",
                    params![timeline, new_text],
                )?;
                Ok(new_write_ts)
            },
        )
        .await?;

        let write_ts = Timestamp::from(write_ts);

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            proposed_next_ts = ?proposed_next_ts,
            "returning from write_ts()"
        );

        let advance_to = write_ts.step_forward();

        Ok(WriteTimestamp {
            timestamp: write_ts,
            advance_to,
        })
    }

    async fn fallible_peek_write_ts(&self) -> Result<Timestamp, anyhow::Error> {
        let conn = Arc::clone(&self.conn);
        let timeline = self.timeline.clone();

        let write_ts = task::spawn_blocking(
            || "timestamp_oracle::sqlite::peek_write_ts",
            move || -> Result<u64, anyhow::Error> {
                let conn = conn.blocking_lock();
                let write_ts_text: String = conn.query_row(
                    "SELECT write_ts FROM timestamp_oracle WHERE timeline = ?1",
                    params![timeline],
                    |row| row.get(0),
                )?;
                Ok(text_to_ts(&write_ts_text))
            },
        )
        .await?;

        let write_ts = Timestamp::from(write_ts);

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            "returning from peek_write_ts()"
        );

        Ok(write_ts)
    }

    async fn fallible_read_ts(&self) -> Result<Timestamp, anyhow::Error> {
        let conn = Arc::clone(&self.conn);
        let timeline = self.timeline.clone();

        let read_ts = task::spawn_blocking(
            || "timestamp_oracle::sqlite::read_ts",
            move || -> Result<u64, anyhow::Error> {
                let conn = conn.blocking_lock();
                let read_ts_text: String = conn.query_row(
                    "SELECT read_ts FROM timestamp_oracle WHERE timeline = ?1",
                    params![timeline],
                    |row| row.get(0),
                )?;
                Ok(text_to_ts(&read_ts_text))
            },
        )
        .await?;

        let read_ts = Timestamp::from(read_ts);

        debug!(
            timeline = ?self.timeline,
            read_ts = ?read_ts,
            "returning from read_ts()"
        );

        Ok(read_ts)
    }

    async fn fallible_apply_write(&self, write_ts: Timestamp) -> Result<(), anyhow::Error> {
        if self.read_only {
            panic!("attempting apply_write in read-only mode");
        }

        let conn = Arc::clone(&self.conn);
        let timeline = self.timeline.clone();
        let write_ts_u64: u64 = write_ts.into();

        task::spawn_blocking(
            || "timestamp_oracle::sqlite::apply_write",
            move || -> Result<(), anyhow::Error> {
                let conn = conn.blocking_lock();
                // Read current values, compute max, and update.
                let (current_read_text, current_write_text): (String, String) = conn.query_row(
                    "SELECT read_ts, write_ts FROM timestamp_oracle WHERE timeline = ?1",
                    params![timeline],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                let current_read = text_to_ts(&current_read_text);
                let current_write = text_to_ts(&current_write_text);
                let new_read = std::cmp::max(current_read, write_ts_u64);
                let new_write = std::cmp::max(current_write, write_ts_u64);
                conn.execute(
                    "UPDATE timestamp_oracle SET read_ts = ?2, write_ts = ?3 WHERE timeline = ?1",
                    params![timeline, ts_to_text(new_read), ts_to_text(new_write)],
                )?;
                Ok(())
            },
        )
        .await?;

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            "returning from apply_write()"
        );

        Ok(())
    }
}

#[async_trait]
impl<N> TimestampOracle<Timestamp> for SqliteTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    async fn write_ts(&self) -> WriteTimestamp<Timestamp> {
        let metrics = &self.metrics.retries.write_ts;

        retry_fallible(metrics, || {
            self.metrics
                .oracle
                .write_ts
                .run_op(|| self.fallible_write_ts())
        })
        .await
    }

    async fn peek_write_ts(&self) -> Timestamp {
        let metrics = &self.metrics.retries.peek_write_ts;

        retry_fallible(metrics, || {
            self.metrics
                .oracle
                .peek_write_ts
                .run_op(|| self.fallible_peek_write_ts())
        })
        .await
    }

    async fn read_ts(&self) -> Timestamp {
        let metrics = &self.metrics.retries.read_ts;

        retry_fallible(metrics, || {
            self.metrics
                .oracle
                .read_ts
                .run_op(|| self.fallible_read_ts())
        })
        .await
    }

    async fn apply_write(&self, write_ts: Timestamp) {
        let metrics = &self.metrics.retries.apply_write;

        retry_fallible(metrics, || {
            self.metrics
                .oracle
                .apply_write
                .run_op(|| self.fallible_apply_write(write_ts.clone()))
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test(tokio::test)]
    async fn test_sqlite_timestamp_oracle() -> Result<(), anyhow::Error> {
        let config = SqliteTimestampOracleConfig::new_for_test()?;

        crate::tests::timestamp_oracle_impl_test(|timeline, now_fn, initial_ts| {
            let oracle = SqliteTimestampOracle::open(
                config.clone(),
                timeline,
                initial_ts,
                now_fn,
                false, /* read-only */
            );

            async {
                let arced_oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                    Arc::new(oracle.await);

                arced_oracle
            }
        })
        .await?;

        Ok(())
    }
}
