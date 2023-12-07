// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A timestamp oracle backed by "Postgres" for persistence/durability and where
//! all oracle operations are self-sufficiently linearized, without requiring
//! any external precautions/machinery.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use deadpool_postgres::{Object, PoolError};
use dec::Decimal;
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_pgrepr::Numeric;
use mz_postgres_client::{PostgresClient, PostgresClientConfig, PostgresClientKnobs};
use mz_repr::Timestamp;
use tracing::{debug, info};

use crate::coord::timeline::WriteTimestamp;
use crate::coord::timestamp_oracle::metrics::{Metrics, RetryMetrics};
use crate::coord::timestamp_oracle::retry::Retry;
use crate::coord::timestamp_oracle::{GenericNowFn, ShareableTimestampOracle, TimestampOracle};

// The timestamp columns are a `DECIMAL` that is big enough to hold
// `18446744073709551615`, the maximum value of `u64` which is our underlying
// timestamp type.
//
// These `sql_stats_automatic_collection_enabled` are for the cost-based
// optimizer but all the queries against this table are single-table and very
// carefully tuned to hit the primary index, so the cost-based optimizer doesn't
// really get us anything. OTOH, the background jobs that crdb creates to
// collect these stats fill up the jobs table (slowing down all sorts of
// things).
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS timestamp_oracle (
    timeline text NOT NULL,
    read_ts DECIMAL(20,0) NOT NULL,
    write_ts DECIMAL(20,0) NOT NULL,
    PRIMARY KEY(timeline)
) WITH (sql_stats_automatic_collection_enabled = false);
";

/// A [`TimestampOracle`] backed by "Postgres".
#[derive(Debug)]
pub struct PostgresTimestampOracle<N>
where
    N: GenericNowFn<Timestamp>,
{
    timeline: String,
    next: N,
    postgres_client: Arc<PostgresClient>,
    metrics: Arc<Metrics>,
}

/// Configuration to connect to a Postgres-backed implementation of
/// [`TimestampOracle`].
#[derive(Clone, Debug)]
pub struct PostgresTimestampOracleConfig {
    url: String,
    metrics: Arc<Metrics>,

    /// The maximum size of the connection pool to Postgres/CRDB.
    pub connection_pool_max_size: usize,

    /// The maximum time to wait when attempting to obtain a connection from the pool.
    pub connection_pool_max_wait: Option<Duration>,

    /// The minimum TTL of a connection to Postgres/CRDB before it is
    /// proactively terminated. Connections are routinely culled to balance load
    /// against the downstream database.
    pub connection_pool_ttl: Duration,

    /// The minimum time between TTLing connections to Postgres/CRDB. This delay
    /// is used to stagger reconnections to avoid stampedes and high tail
    /// latencies. This value should be much less than `connection_pool_ttl` so
    /// that reconnections are biased towards terminating the oldest connections
    /// first. A value of `connection_pool_ttl / connection_pool_max_size` is
    /// likely a good place to start so that all connections are rotated when
    /// the pool is fully used.
    pub connection_pool_ttl_stagger: Duration,

    /// The duration to wait for a Postgres/CRDB connection to be made before
    /// retrying.
    pub connect_timeout: Duration,

    /// The TCP user timeout for a Postgres/CRDB connection. Specifies the
    /// amount of time that transmitted data may remain unacknowledged before
    /// the TCP connection is forcibly closed.
    pub tcp_user_timout: Duration,
}

impl From<PostgresTimestampOracleConfig> for PostgresClientConfig {
    fn from(config: PostgresTimestampOracleConfig) -> Self {
        let metrics = config.metrics.postgres_client.clone();
        PostgresClientConfig::new(config.url.clone(), Arc::new(config), metrics)
    }
}

impl PostgresTimestampOracleConfig {
    pub(crate) const EXTERNAL_TESTS_POSTGRES_URL: &'static str = "COCKROACH_URL";

    /// Returns a new instance of [`PostgresTimestampOracleConfig`] with default tuning.
    pub fn new(url: &str, metrics: Arc<Metrics>) -> Self {
        PostgresTimestampOracleConfig {
            url: url.to_string(),
            metrics,
            // TODO(aljoscha): These defaults are taken as is from the persist
            // Consensus tuning. Once we're sufficiently advanced we might want
            // to a) make some of these configurable dynamically, as they are in
            // persist, and b) pick defaults that are different from the persist
            // defaults.
            connection_pool_max_size: 50,
            connection_pool_max_wait: Some(Duration::from_secs(60)),
            connection_pool_ttl: Duration::from_secs(300),
            connection_pool_ttl_stagger: Duration::from_secs(6),
            connect_timeout: Duration::from_secs(5),
            tcp_user_timout: Duration::from_secs(30),
        }
    }

    /// Returns a new [`PostgresTimestampOracleConfig`] for use in unit tests.
    ///
    /// By default, postgres oracle tests are no-ops so that `cargo test` works
    /// on new environments without any configuration. To activate the tests for
    /// [`PostgresTimestampOracle`] set the `COCKROACH_URL` environment variable
    /// with a valid connection url [1].
    ///
    /// [1]: https://docs.rs/tokio-postgres/latest/tokio_postgres/config/struct.Config.html#url
    pub fn new_for_test() -> Option<Self> {
        let url = match std::env::var(Self::EXTERNAL_TESTS_POSTGRES_URL) {
            Ok(url) => url,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return None;
            }
        };

        let config = PostgresTimestampOracleConfig {
            url: url.to_string(),
            metrics: Arc::new(Metrics::new(&MetricsRegistry::new())),
            connection_pool_max_size: 2,
            connection_pool_max_wait: None,
            connection_pool_ttl: Duration::MAX,
            connection_pool_ttl_stagger: Duration::MAX,
            connect_timeout: Duration::MAX,
            tcp_user_timout: Duration::ZERO,
        };

        Some(config)
    }
}

impl PostgresClientKnobs for PostgresTimestampOracleConfig {
    fn connection_pool_max_size(&self) -> usize {
        self.connection_pool_max_size
    }

    fn connection_pool_max_wait(&self) -> Option<Duration> {
        self.connection_pool_max_wait.clone()
    }

    fn connection_pool_ttl(&self) -> Duration {
        self.connection_pool_ttl.clone()
    }

    fn connection_pool_ttl_stagger(&self) -> Duration {
        self.connection_pool_ttl_stagger.clone()
    }

    fn connect_timeout(&self) -> Duration {
        self.connect_timeout.clone()
    }

    fn tcp_user_timeout(&self) -> Duration {
        self.tcp_user_timout.clone()
    }
}

impl<N> PostgresTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    /// Open a Postgres [`TimestampOracle`] instance with `config`, for the
    /// timeline named `timeline`. `next` generates new timestamps when invoked.
    /// Timestamps that are returned are made durable and will never retract.
    pub(crate) async fn open(
        config: PostgresTimestampOracleConfig,
        timeline: String,
        initially: Timestamp,
        next: N,
    ) -> Self {
        info!(url = ?config.url, "opening PostgresTimestampOracle");

        let fallible = || async {
            let metrics = Arc::clone(&config.metrics);

            let postgres_client = PostgresClient::open(config.clone().into())?;

            let client = postgres_client.get_connection().await?;

            // The `timestamp_oracle` table creates and deletes rows at a high
            // frequency, generating many tombstoned rows. If Cockroach's GC
            // interval is set high (the default is 25h) and these tombstones
            // accumulate, scanning over the table will take increasingly and
            // prohibitively long.
            //
            // See: https://github.com/MaterializeInc/materialize/issues/13975
            // See: https://www.cockroachlabs.com/docs/stable/configure-zone.html#variables
            client
                .batch_execute(&format!(
                    "{} {}",
                    SCHEMA,
                    "ALTER TABLE timestamp_oracle CONFIGURE ZONE USING gc.ttlseconds = 600;",
                ))
                .await?;

            let oracle = PostgresTimestampOracle {
                timeline: timeline.clone(),
                next: next.clone(),
                postgres_client: Arc::new(postgres_client),
                metrics,
            };

            // Create a row for our timeline, if it doesn't exist. The
            // `apply_write` call below expects the row to be present. If we
            // didn't have this here we would always need CHECK EXISTS calls or
            // something in `apply_write`, making it more complicated, so we
            // only do it once here, on initialization.
            let q = r#"
                    INSERT INTO timestamp_oracle (timeline, read_ts, write_ts)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (timeline) DO NOTHING;
                "#;
            let statement = client.prepare_cached(q).await?;

            let initially_coerced = Self::ts_to_decimal(initially);
            let _ = client
                .execute(
                    &statement,
                    &[&oracle.timeline, &initially_coerced, &initially_coerced],
                )
                .await?;

            // Forward timestamps to what we're given from outside. Remember,
            // the above query will only create the row at the initial timestamp
            // if it didn't exist before.
            ShareableTimestampOracle::apply_write(&oracle, initially).await;

            Result::<_, anyhow::Error>::Ok(oracle)
        };

        let metrics = &config.metrics.retries.open;

        let oracle = retry_fallible(metrics, fallible).await;

        oracle
    }

    async fn get_connection(&self) -> Result<Object, PoolError> {
        self.postgres_client.get_connection().await
    }

    /// Returns a `Vec` of all known timelines along with their current greatest
    /// timestamp (max of read_ts and write_ts).
    ///
    /// For use when initializing another [`TimestampOracle`] implementation
    /// from another oracle's state.
    ///
    /// NOTE: This method can have linearizability violations, but it's hard to
    /// get around those. They will only occur during bootstrap, and only
    /// if/when we migrate between different oracle implementations. Once we
    /// migrated from the catalog-backed oracle to the postgres/crdb-backed
    /// oracle we can remove this code and it's callsite.
    pub(crate) async fn get_all_timelines(
        config: PostgresTimestampOracleConfig,
    ) -> Result<Vec<(String, Timestamp)>, anyhow::Error> {
        let fallible = || async {
            let postgres_client = PostgresClient::open(config.clone().into())?;

            let mut client = postgres_client.get_connection().await?;

            let txn = client.transaction().await?;

            // Using `table_schema = CURRENT_SCHEMA` makes sure we only include
            // tables that are queryable by us. Otherwise this check might
            // return true but then the query below fails with a confusing
            // "table does not exist" error.
            let q = r#"
            SELECT EXISTS (SELECT * FROM information_schema.tables WHERE table_name = 'timestamp_oracle' AND table_schema = CURRENT_SCHEMA);
        "#;
            let statement = txn.prepare(q).await?;
            let exists_row = txn.query_one(&statement, &[]).await?;
            let exists: bool = exists_row.try_get("exists").expect("missing exists column");
            if !exists {
                return Ok(Vec::new());
            }

            let q = r#"
            SELECT timeline, GREATEST(read_ts, write_ts) as ts FROM timestamp_oracle;
        "#;
            let statement = txn.prepare(q).await?;
            let rows = txn.query(&statement, &[]).await?;

            txn.commit().await?;

            let result = rows
                .into_iter()
                .map(|row| {
                    let timeline: String =
                        row.try_get("timeline").expect("missing timeline column");
                    let ts: Numeric = row.try_get("ts").expect("missing ts column");
                    let ts = Self::decimal_to_ts(ts);

                    (timeline, ts)
                })
                .collect::<Vec<_>>();

            Ok(result)
        };

        let metrics = &config.metrics.retries.get_all_timelines;

        let result = retry_fallible(metrics, fallible).await;

        Ok(result)
    }

    #[tracing::instrument(name = "oracle::write_ts", level = "debug", skip_all)]
    async fn fallible_write_ts(&self) -> Result<WriteTimestamp<Timestamp>, anyhow::Error> {
        let proposed_next_ts = self.next.now();
        let proposed_next_ts = Self::ts_to_decimal(proposed_next_ts);

        let q = r#"
            UPDATE timestamp_oracle SET write_ts = GREATEST(write_ts+1, $2)
                WHERE timeline = $1
            RETURNING write_ts;
        "#;
        let client = self.get_connection().await?;
        let statement = client.prepare_cached(q).await?;
        let result = client
            .query_one(&statement, &[&self.timeline, &proposed_next_ts])
            .await?;

        let write_ts: Numeric = result.try_get("write_ts").expect("missing column write_ts");
        let write_ts = Self::decimal_to_ts(write_ts);

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            proposed_next_ts = ?proposed_next_ts,
            "returning from write_ts()");

        let advance_to = write_ts.step_forward();

        Ok(WriteTimestamp {
            timestamp: write_ts,
            advance_to,
        })
    }

    #[tracing::instrument(name = "oracle::peek_write_ts", level = "debug", skip_all)]
    async fn fallible_peek_write_ts(&self) -> Result<Timestamp, anyhow::Error> {
        let q = r#"
            SELECT write_ts FROM timestamp_oracle
                WHERE timeline = $1;
        "#;
        let client = self.get_connection().await?;
        let statement = client.prepare_cached(q).await?;
        let result = client.query_one(&statement, &[&self.timeline]).await?;

        let write_ts: Numeric = result.try_get("write_ts").expect("missing column write_ts");
        let write_ts = Self::decimal_to_ts(write_ts);

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            "returning from peek_write_ts()");

        Ok(write_ts)
    }

    #[tracing::instrument(name = "oracle::read_ts", level = "debug", skip_all)]
    async fn fallible_read_ts(&self) -> Result<Timestamp, anyhow::Error> {
        let q = r#"
            SELECT read_ts FROM timestamp_oracle
                WHERE timeline = $1;
        "#;
        let client = self.get_connection().await?;
        let statement = client.prepare_cached(q).await?;
        let result = client.query_one(&statement, &[&self.timeline]).await?;

        let read_ts: Numeric = result.try_get("read_ts").expect("missing column read_ts");
        let read_ts = Self::decimal_to_ts(read_ts);

        debug!(
            timeline = ?self.timeline,
            read_ts = ?read_ts,
            "returning from read_ts()");

        Ok(read_ts)
    }

    #[tracing::instrument(name = "oracle::apply_write", level = "debug", skip_all)]
    async fn fallible_apply_write(&self, write_ts: Timestamp) -> Result<(), anyhow::Error> {
        let q = r#"
            UPDATE timestamp_oracle SET write_ts = GREATEST(write_ts, $2), read_ts = GREATEST(read_ts, $2)
                WHERE timeline = $1;
        "#;
        let client = self.get_connection().await?;
        let statement = client.prepare_cached(q).await?;
        let write_ts = Self::ts_to_decimal(write_ts);

        let _ = client
            .execute(&statement, &[&self.timeline, &write_ts])
            .await?;

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            "returning from apply_write()");

        Ok(())
    }

    // We could add `From` impls for these but for now keep the code local to
    // the oracle.
    fn ts_to_decimal(ts: Timestamp) -> Numeric {
        let decimal = Decimal::from(ts);
        Numeric::from(decimal)
    }

    // We could add `From` impls for these but for now keep the code local to
    // the oracle.
    fn decimal_to_ts(ts: Numeric) -> Timestamp {
        ts.0 .0.try_into().expect("we only use u64 timestamps")
    }
}

// A wrapper around the `fallible_` methods that adds operation metrics and
// retries.
//
// NOTE: This implementation is tied to [`mz_repr::Timestamp`]. We could change
// that, and also make the types we store in the backing "Postgres" table
// generic. But in practice we only use oracles for [`mz_repr::Timestamp`] so
// don't do that extra work for now.
#[async_trait]
impl<N> ShareableTimestampOracle<Timestamp> for PostgresTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    async fn write_ts(&self) -> WriteTimestamp<Timestamp> {
        let metrics = &self.metrics.retries.write_ts;

        let res = retry_fallible(metrics, || {
            self.metrics
                .oracle
                .write_ts
                .run_op(|| self.fallible_write_ts())
        })
        .await;

        res
    }

    async fn peek_write_ts(&self) -> Timestamp {
        let metrics = &self.metrics.retries.peek_write_ts;

        let res = retry_fallible(metrics, || {
            self.metrics
                .oracle
                .peek_write_ts
                .run_op(|| self.fallible_peek_write_ts())
        })
        .await;

        res
    }

    async fn read_ts(&self) -> Timestamp {
        let metrics = &self.metrics.retries.read_ts;

        let res = retry_fallible(metrics, || {
            self.metrics
                .oracle
                .read_ts
                .run_op(|| self.fallible_read_ts())
        })
        .await;

        res
    }

    async fn apply_write(&self, write_ts: Timestamp) {
        let metrics = &self.metrics.retries.apply_write;

        let res = retry_fallible(metrics, || {
            self.metrics
                .oracle
                .apply_write
                .run_op(|| self.fallible_apply_write(write_ts.clone()))
        })
        .await;

        res
    }
}

pub const INFO_MIN_ATTEMPTS: usize = 3;

pub async fn retry_fallible<R, F, WorkFn>(metrics: &RetryMetrics, mut work_fn: WorkFn) -> R
where
    F: std::future::Future<Output = Result<R, anyhow::Error>>,
    WorkFn: FnMut() -> F,
{
    let mut retry = metrics.stream(Retry::oracle_defaults(SystemTime::now()).into_retry_stream());
    loop {
        match work_fn().await {
            Ok(x) => {
                if retry.attempt() > 0 {
                    debug!(
                        "external operation {} succeeded after failing at least once",
                        metrics.name,
                    );
                }
                return x;
            }
            Err(err)
                if err
                    .to_string()
                    .contains("\"timestamp_oracle\" does not exist") =>
            {
                // TODO(aljoscha): Re-consider this before enabling the oracle
                // in production.
                // It's a policy question whether this actually _is_ an
                // unrecoverable error: an operator could go in an re-create the
                // `timestamp_oracle` table once the problem is noticed.
                // However, our tests currently assume that materialize will
                // fail unrecoverably in cases where we do a rug-pull/remove
                // expected tables from CRDB.
                panic!(
                    "external operation {} failed unrecoverably, someone removed our database/schema/table: {}",
                    metrics.name,
                    err.display_with_causes()
                );
            }
            Err(err) => {
                if retry.attempt() >= INFO_MIN_ATTEMPTS {
                    info!(
                        "external operation {} failed, retrying in {:?}: {}",
                        metrics.name,
                        retry.next_sleep(),
                        err.display_with_causes()
                    );
                } else {
                    debug!(
                        "external operation {} failed, retrying in {:?}: {}",
                        metrics.name,
                        retry.next_sleep(),
                        err.display_with_causes()
                    );
                }
                retry = retry.sleep().await;
            }
        }
    }
}

#[async_trait(?Send)]
impl<N> TimestampOracle<Timestamp> for PostgresTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    #[tracing::instrument(name = "oracle::write_ts", level = "debug", skip_all)]
    async fn write_ts(&mut self) -> WriteTimestamp<Timestamp> {
        ShareableTimestampOracle::write_ts(self).await
    }

    #[tracing::instrument(name = "oracle::peek_write_ts", level = "debug", skip_all)]
    async fn peek_write_ts(&self) -> Timestamp {
        ShareableTimestampOracle::peek_write_ts(self).await
    }

    #[tracing::instrument(name = "oracle::read_ts", level = "debug", skip_all)]
    async fn read_ts(&self) -> Timestamp {
        ShareableTimestampOracle::read_ts(self).await
    }

    #[tracing::instrument(name = "oracle::apply_write", level = "debug", skip_all)]
    async fn apply_write(&mut self, write_ts: Timestamp) {
        ShareableTimestampOracle::apply_write(self, write_ts).await
    }

    fn get_shared(&self) -> Option<Arc<dyn ShareableTimestampOracle<Timestamp> + Send + Sync>> {
        let shallow_clone = Self {
            timeline: self.timeline.clone(),
            next: self.next.clone(),
            postgres_client: Arc::clone(&self.postgres_client),
            metrics: Arc::clone(&self.metrics),
        };

        Some(Arc::new(shallow_clone))
    }
}

#[cfg(test)]
mod tests {

    use crate::coord::timestamp_oracle;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_postgres_timestamp_oracle() -> Result<(), anyhow::Error> {
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

        timestamp_oracle::tests::timestamp_oracle_impl_test(|timeline, now_fn, initial_ts| {
            let oracle =
                PostgresTimestampOracle::open(config.clone(), timeline, initial_ts, now_fn);

            oracle
        })
        .await?;

        Ok(())
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn test_shareable_postgres_timestamp_oracle() -> Result<(), anyhow::Error> {
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

        timestamp_oracle::tests::shareable_timestamp_oracle_impl_test(
            |timeline, now_fn, initial_ts| {
                let oracle =
                    PostgresTimestampOracle::open(config.clone(), timeline, initial_ts, now_fn);

                async {
                    oracle
                        .await
                        .get_shared()
                        .expect("postgres oracle is shareable")
                }
            },
        )
        .await?;

        Ok(())
    }
}
