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
use std::time::Duration;

use async_trait::async_trait;
use deadpool_postgres::{Object, PoolError};
use mz_postgres_client::{PostgresClient, PostgresClientConfig, PostgresClientKnobs};
use mz_repr::Timestamp;
use tracing::{debug, info};

use crate::coord::timeline::WriteTimestamp;
use crate::coord::timestamp_oracle::metrics::Metrics;
use crate::coord::timestamp_oracle::{GenericNowFn, TimestampOracle};

// These `sql_stats_automatic_collection_enabled` are for the cost-based
// optimizer but all the queries against this table are single-table and very
// carefully tuned to hit the primary index, so the cost-based optimizer doesn't
// really get us anything. OTOH, the background jobs that crdb creates to
// collect these stats fill up the jobs table (slowing down all sorts of
// things).
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS timestamp_oracle (
    timeline text NOT NULL,
    read_ts bigint NOT NULL,
    write_ts bigint NOT NULL,
    PRIMARY KEY(timeline)
) WITH (sql_stats_automatic_collection_enabled = false);
";

/// A [`TimestampOracle`] backed by "Postgres".
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
    N: GenericNowFn<Timestamp> + 'static,
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

        let fallible = async move {
            let metrics = Arc::clone(&config.metrics);

            let postgres_client = PostgresClient::open(config.into())?;

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

            let mut oracle = PostgresTimestampOracle {
                timeline,
                next,
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
            let statement = client
                .prepare_cached(q)
                .await
                .expect("todo: add internal retries");
            let initially_coerced = Self::coerce_timestamp(initially, "initial_ts");
            let _ = client
                .execute(
                    &statement,
                    &[&oracle.timeline, &initially_coerced, &initially_coerced],
                )
                .await?;

            // Forward timestamps to what we're given from outside. Remember,
            // the above query will only create the row at the initial timestamp
            // if it didn't exist before.
            oracle.apply_write(initially).await;

            Result::<_, anyhow::Error>::Ok(oracle)
        };

        let oracle = fallible.await.expect("todo: add internal retries");

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
        let postgres_client = PostgresClient::open(config.into())?;

        let mut client = postgres_client.get_connection().await?;

        let txn = client.transaction().await?;

        let q = r#"
            SELECT EXISTS (SELECT * FROM information_schema.tables where table_name = 'timestamp_oracle');
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
        let rows = txn
            .query(&statement, &[])
            .await
            .expect("todo: add internal retries");

        txn.commit().await?;

        let result = rows
            .into_iter()
            .map(|row| {
                let timeline: String = row.try_get("timeline").expect("missing timeline column");
                let ts: i64 = row.try_get("ts").expect("missing ts column");
                let ts = Timestamp::try_from(ts).expect("we don't use negative timestamps");

                (timeline, ts)
            })
            .collect::<Vec<_>>();

        Ok(result)
    }

    #[tracing::instrument(name = "oracle::write_ts", level = "debug", skip_all)]
    async fn fallible_write_ts(&self) -> Result<WriteTimestamp<Timestamp>, anyhow::Error> {
        let proposed_next_ts = self.next.now();
        let proposed_next_ts = Self::coerce_timestamp(proposed_next_ts, "proposed_next_ts");

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

        let write_ts: i64 = result.try_get("write_ts").expect("missing column write_ts");
        let write_ts = Timestamp::try_from(write_ts).expect("we don't use negative timestamps");

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

        let write_ts: i64 = result.try_get("write_ts").expect("missing column write_ts");
        let write_ts = Timestamp::try_from(write_ts).expect("we don't use negative timestamps");

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

        let read_ts: i64 = result.try_get("read_ts").expect("missing column read_ts");
        let read_ts = Timestamp::try_from(read_ts).expect("we don't use negative timestamps");

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
        let write_ts = Self::coerce_timestamp(write_ts, "write_ts");

        let _ = client
            .execute(&statement, &[&self.timeline, &write_ts])
            .await?;

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            "returning from apply_write()");

        Ok(())
    }

    fn coerce_timestamp(ts: Timestamp, display_name: &str) -> i64 {
        match i64::try_from(ts) {
            Ok(ts) => ts,
            Err(err) => panic!("{} {} does not fit into i64: {}", display_name, ts, err),
        }
    }
}

// A wrapper around the `fallible_` methods that adds operation metrics and
// (TODO: future work) retries.
//
// NOTE: This implementation is tied to [`mz_repr::Timestamp`]. We could change
// that, and also make the types we store in the backing "Postgres" table
// generic. But in practice we only use oracles for [`mz_repr::Timestamp`] so
// don't do that extra work for now.
#[async_trait(?Send)]
impl<N> TimestampOracle<Timestamp> for PostgresTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + 'static,
{
    async fn write_ts(&mut self) -> WriteTimestamp<Timestamp> {
        self.metrics
            .oracle
            .write_ts
            .run_op(|| self.fallible_write_ts())
            .await
            .expect("todo: add retries")
    }

    async fn peek_write_ts(&self) -> Timestamp {
        self.metrics
            .oracle
            .peek_write_ts
            .run_op(|| self.fallible_peek_write_ts())
            .await
            .expect("todo: add retries")
    }

    async fn read_ts(&self) -> Timestamp {
        self.metrics
            .oracle
            .read_ts
            .run_op(|| self.fallible_read_ts())
            .await
            .expect("todo: add retries")
    }

    async fn apply_write(&mut self, write_ts: Timestamp) {
        self.metrics
            .oracle
            .apply_write
            .run_op(|| self.fallible_apply_write(write_ts))
            .await
            .expect("todo: add retries")
    }
}
