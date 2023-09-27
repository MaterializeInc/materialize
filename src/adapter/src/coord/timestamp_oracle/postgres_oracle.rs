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
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_postgres_client::metrics::PostgresClientMetrics;
use mz_postgres_client::{PostgresClient, PostgresClientConfig, PostgresClientKnobs};
use mz_repr::Timestamp;
use tracing::{debug, info};

use crate::coord::timeline::WriteTimestamp;
use crate::coord::timestamp_oracle::TimestampOracle;

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
}

/// A [`NowFn`] that is generic over the timestamp.
///
/// The oracle operations work in terms of [`mz_repr::Timestamp`] and we could
/// work around it by bridging between the two in the oracle implementation
/// itself. This wrapper type makes that slightly easier, though.
pub trait GenericNowFn<T>: Clone + Send + Sync {
    fn now(&self) -> T;
}

impl GenericNowFn<mz_repr::Timestamp> for NowFn {
    fn now(&self) -> mz_repr::Timestamp {
        (self)().into()
    }
}

/// Configuration to connect to a Postgres-backed implementation of
/// [TimestampOracle].
#[derive(Clone, Debug)]
pub struct PostgresTimestampOracleConfig {
    url: String,
    metrics_registry: MetricsRegistry,

    /// The maximum size of the connection pool to Postgres/CRDB.
    pub connection_pool_max_size: usize,
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
        let metrics = PostgresClientMetrics::new(&config.metrics_registry, "mz_postgres_ts_oracle");
        PostgresClientConfig::new(config.url.clone(), Arc::new(config), metrics)
    }
}

impl PostgresTimestampOracleConfig {
    /// Returns a new instance of [`PostgresTimestampOracleConfig`] with default tuning.
    pub fn new(url: &str, metrics_registry: MetricsRegistry) -> Self {
        PostgresTimestampOracleConfig {
            url: url.to_string(),
            metrics_registry,
            // NOTE: These defaults are taken as is from the persist Consensus
            // tuning. Once we're sufficiently advanced we might want to a) make
            // some of these configurable dynamicall, as they are in persist,
            // and b) pick defaults that are different from the persist
            // defaults.
            connection_pool_max_size: 50,
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
    /// Open a Postgres [TimestampOracle] instance with `config`, for the
    /// timeline named `timeline`. `next` generates new timestamps when invoked.
    /// Timestamps that are returned are made durable and will never retract.
    pub(crate) async fn open(
        config: PostgresTimestampOracleConfig,
        timeline: String,
        initially: Timestamp,
        next: N,
    ) -> Result<Self, anyhow::Error> {
        info!(url = ?config.url, "opening PostgresTimestampOracle");

        let postgres_client = PostgresClient::open(config.into())?;

        let client = postgres_client.get_connection().await?;

        // TODO! Do we need this?
        // The `consensus` table creates and deletes rows at a high frequency, generating many
        // tombstoned rows. If Cockroach's GC interval is set high (the default is 25h) and
        // these tombstones accumulate, scanning over the table will take increasingly and
        // prohibitively long.
        //
        // See: https://github.com/MaterializeInc/materialize/issues/13975
        // See: https://www.cockroachlabs.com/docs/stable/configure-zone.html#variables
        client
            .batch_execute(&format!(
                "{} {}",
                SCHEMA, "ALTER TABLE timestamp_oracle CONFIGURE ZONE USING gc.ttlseconds = 600;",
            ))
            .await?;

        let mut oracle = PostgresTimestampOracle {
            timeline,
            next,
            postgres_client: Arc::new(postgres_client),
        };

        let q = r#"
            INSERT INTO timestamp_oracle (timeline, read_ts, write_ts)
                VALUES ($1, $2, $3)
                ON CONFLICT (timeline) DO NOTHING;
        "#;
        let statement = client.prepare_cached(q).await.expect("todo");
        let _ = client
            .execute(&statement, &[&oracle.timeline, &0i64, &0i64])
            .await?;

        oracle.apply_write(initially).await;

        Ok(oracle)
    }

    async fn get_connection(&self) -> Result<Object, PoolError> {
        self.postgres_client.get_connection().await
    }
}

// NOTE: This implementation is tied to [`mz_repr::Timestamp`]. We could change
// that, and also make the types we store in the backing "Postgres" table
// generic. But in practice we only use oracles for [`mz_repr::Timestamp`] so
// don't do that extra work for now.
#[async_trait(?Send)]
impl<N> TimestampOracle<Timestamp> for PostgresTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + 'static,
{
    #[tracing::instrument(name = "oracle::write_ts", level = "debug", skip_all)]
    async fn write_ts(&mut self) -> WriteTimestamp<Timestamp> {
        let proposed_next_ts = self.next.now();
        let proposed_next_ts = i64::try_from(proposed_next_ts).expect("todo");

        let q = r#"
            UPDATE timestamp_oracle SET write_ts = GREATEST(write_ts+1, $2)
                WHERE timeline = $1
            RETURNING write_ts;
        "#;
        let client = self.get_connection().await.expect("todo");
        let statement = client.prepare_cached(q).await.expect("todo");
        let rows = client
            .query(&statement, &[&self.timeline, &proposed_next_ts])
            .await
            .expect("todo");

        assert!(rows.len() == 1);

        let result = rows.first().unwrap();

        let write_ts: i64 = result.try_get("write_ts").expect("todo");
        let write_ts = Timestamp::try_from(write_ts).expect("todo");

        debug!(write_ts = ?write_ts, "write_ts");

        let advance_to = write_ts.step_forward();

        WriteTimestamp {
            timestamp: write_ts,
            advance_to,
        }
    }

    #[tracing::instrument(name = "oracle::peek_write_ts", level = "debug", skip_all)]
    async fn peek_write_ts(&self) -> Timestamp {
        let q = r#"
            SELECT write_ts FROM timestamp_oracle
                WHERE timeline = $1;
        "#;
        let client = self.get_connection().await.expect("todo");
        let statement = client.prepare_cached(q).await.expect("todo");
        let rows = client
            .query(&statement, &[&self.timeline])
            .await
            .expect("todo");

        assert!(rows.len() == 1);

        let result = rows.first().unwrap();

        let write_ts: i64 = result.try_get("write_ts").expect("todo");
        let write_ts = Timestamp::try_from(write_ts).expect("todo");

        debug!(write_ts = ?write_ts, "peek_write_ts");

        write_ts
    }

    #[tracing::instrument(name = "oracle::read_ts", level = "debug", skip_all)]
    async fn read_ts(&self) -> Timestamp {
        let q = r#"
            SELECT read_ts FROM timestamp_oracle
                WHERE timeline = $1;
        "#;
        let client = self.get_connection().await.expect("todo");
        let statement = client.prepare_cached(q).await.expect("todo");
        let rows = client
            .query(&statement, &[&self.timeline])
            .await
            .expect("todo");

        assert!(rows.len() == 1);

        let result = rows.first().unwrap();

        let read_ts: i64 = result.try_get("read_ts").expect("todo");
        let read_ts = Timestamp::try_from(read_ts).expect("todo");

        debug!(read_ts = ?read_ts, "read_ts");

        read_ts
    }

    #[tracing::instrument(name = "oracle::apply_write", level = "debug", skip_all)]
    async fn apply_write(&mut self, write_ts: Timestamp) {
        let q = r#"
            UPDATE timestamp_oracle SET write_ts = GREATEST(write_ts, $2), read_ts = GREATEST(read_ts, $2)
                WHERE timeline = $1;
        "#;
        let client = self.get_connection().await.expect("todo");
        let statement = client.prepare_cached(q).await.expect("todo");
        let write_ts = i64::try_from(write_ts).expect("todo");

        debug!(new_write_ts = ?write_ts, "apply_write");

        let _ = client
            .execute(&statement, &[&self.timeline, &write_ts])
            .await
            .expect("todo");
    }
}
