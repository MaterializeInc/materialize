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

use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use deadpool_postgres::tokio_postgres::Config;
use deadpool_postgres::tokio_postgres::error::SqlState;
use deadpool_postgres::{Object, PoolError};
use dec::Decimal;
use mz_adapter_types::timestamp_oracle::{
    DEFAULT_PG_TIMESTAMP_ORACLE_CONNECT_TIMEOUT, DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_SIZE,
    DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_WAIT, DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL,
    DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL_STAGGER, DEFAULT_PG_TIMESTAMP_ORACLE_TCP_USER_TIMEOUT,
};
use mz_ore::error::ErrorExt;
use mz_ore::instrument;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::url::SensitiveUrl;
use mz_pgrepr::Numeric;
use mz_postgres_client::{PostgresClient, PostgresClientConfig, PostgresClientKnobs};
use mz_repr::Timestamp;
use postgres_protocol::escape::escape_identifier;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::WriteTimestamp;
use crate::metrics::{Metrics, RetryMetrics};
use crate::retry::Retry;
use crate::{GenericNowFn, TimestampOracle};

// The timestamp columns are a `DECIMAL` that is big enough to hold
// `18446744073709551615`, the maximum value of `u64` which is our underlying
// timestamp type.
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS timestamp_oracle (
    timeline text NOT NULL,
    read_ts DECIMAL(20,0) NOT NULL,
    write_ts DECIMAL(20,0) NOT NULL,
    PRIMARY KEY(timeline)
)
";

// These `sql_stats_automatic_collection_enabled` are for the cost-based
// optimizer but all the queries against this table are single-table and very
// carefully tuned to hit the primary index, so the cost-based optimizer doesn't
// really get us anything. OTOH, the background jobs that crdb creates to
// collect these stats fill up the jobs table (slowing down all sorts of
// things).
const CRDB_SCHEMA_OPTIONS: &str = "WITH (sql_stats_automatic_collection_enabled = false)";
// The `timestamp_oracle` table creates and deletes rows at a high
// frequency, generating many tombstoned rows. If Cockroach's GC
// interval is set high (the default is 25h) and these tombstones
// accumulate, scanning over the table will take increasingly and
// prohibitively long.
//
// See: https://github.com/MaterializeInc/database-issues/issues/4001
// See: https://www.cockroachlabs.com/docs/stable/configure-zone.html#variables
const CRDB_CONFIGURE_ZONE: &str =
    "ALTER TABLE timestamp_oracle CONFIGURE ZONE USING gc.ttlseconds = 600;";

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
    /// A read-only timestamp oracle is NOT allowed to do operations that change
    /// the backing Postgres/CRDB state.
    read_only: bool,
}

/// Configuration to connect to a Postgres-backed implementation of
/// [`TimestampOracle`].
#[derive(Clone, Debug)]
pub struct PostgresTimestampOracleConfig {
    url: SensitiveUrl,
    pub metrics: Arc<Metrics>,

    /// Configurations that can be dynamically updated.
    pub dynamic: Arc<DynamicConfig>,
}

impl From<PostgresTimestampOracleConfig> for PostgresClientConfig {
    fn from(config: PostgresTimestampOracleConfig) -> Self {
        let metrics = config.metrics.postgres_client.clone();
        PostgresClientConfig::new(config.url.clone(), Arc::new(config), metrics)
    }
}

impl PostgresTimestampOracleConfig {
    pub(crate) const EXTERNAL_TESTS_POSTGRES_URL: &'static str = "METADATA_BACKEND_URL";

    /// Returns a new instance of [`PostgresTimestampOracleConfig`] with default tuning.
    pub fn new(url: &SensitiveUrl, metrics_registry: &MetricsRegistry) -> Self {
        let metrics = Arc::new(Metrics::new(metrics_registry));

        let dynamic = DynamicConfig::default();

        PostgresTimestampOracleConfig {
            url: url.clone(),
            metrics,
            dynamic: Arc::new(dynamic),
        }
    }

    /// Returns a new [`PostgresTimestampOracleConfig`] for use in unit tests.
    ///
    /// By default, postgres oracle tests are no-ops so that `cargo test` works
    /// on new environments without any configuration. To activate the tests for
    /// [`PostgresTimestampOracle`] set the `METADATA_BACKEND_URL` environment variable
    /// with a valid connection url [1].
    ///
    /// [1]: https://docs.rs/tokio-postgres/latest/tokio_postgres/config/struct.Config.html#url
    pub fn new_for_test() -> Option<Self> {
        let url = match std::env::var(Self::EXTERNAL_TESTS_POSTGRES_URL) {
            Ok(url) => SensitiveUrl::from_str(&url).expect("invalid Postgres URL"),
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return None;
            }
        };

        let dynamic = DynamicConfig::default();

        let config = PostgresTimestampOracleConfig {
            url,
            metrics: Arc::new(Metrics::new(&MetricsRegistry::new())),
            dynamic: Arc::new(dynamic),
        };

        Some(config)
    }
}

/// Part of [`PostgresTimestampOracleConfig`] that can be dynamically updated.
///
/// The timestamp oracle is expected to react to each of these such that
/// updating the value returned by the function takes effect (i.e. no caching
/// it). This should happen "as promptly as reasonably possible" where that's
/// defined by the tradeoffs of complexity vs promptness.
///
/// These are hooked up to LaunchDarkly. Specifically, LaunchDarkly configs are
/// serialized into a [`PostgresTimestampOracleParameters`]. In environmentd,
/// these are applied directly via [`PostgresTimestampOracleParameters::apply`]
/// to the [`PostgresTimestampOracleConfig`].
#[derive(Debug)]
pub struct DynamicConfig {
    /// The maximum size of the connection pool to Postgres/CRDB.
    pg_connection_pool_max_size: AtomicUsize,

    /// The maximum time to wait when attempting to obtain a connection from the pool.
    pg_connection_pool_max_wait: RwLock<Option<Duration>>,

    /// The minimum TTL of a connection to Postgres/CRDB before it is
    /// proactively terminated. Connections are routinely culled to balance load
    /// against the downstream database.
    pg_connection_pool_ttl: RwLock<Duration>,

    /// The minimum time between TTLing connections to Postgres/CRDB. This delay
    /// is used to stagger reconnections to avoid stampedes and high tail
    /// latencies. This value should be much less than `connection_pool_ttl` so
    /// that reconnections are biased towards terminating the oldest connections
    /// first. A value of `connection_pool_ttl / connection_pool_max_size` is
    /// likely a good place to start so that all connections are rotated when
    /// the pool is fully used.
    pg_connection_pool_ttl_stagger: RwLock<Duration>,

    /// The duration to wait for a Postgres/CRDB connection to be made before
    /// retrying.
    pg_connection_pool_connect_timeout: RwLock<Duration>,

    /// The TCP user timeout for a Postgres/CRDB connection. Specifies the
    /// amount of time that transmitted data may remain unacknowledged before
    /// the TCP connection is forcibly closed.
    pg_connection_pool_tcp_user_timeout: RwLock<Duration>,
}

impl Default for DynamicConfig {
    fn default() -> Self {
        Self {
            // TODO(aljoscha): These defaults are taken as is from the persist
            // Consensus tuning. Once we're sufficiently advanced we might want
            // to pick defaults that are different from the persist defaults.
            pg_connection_pool_max_size: AtomicUsize::new(
                DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_SIZE,
            ),
            pg_connection_pool_max_wait: RwLock::new(Some(
                DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_MAX_WAIT,
            )),
            pg_connection_pool_ttl: RwLock::new(DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL),
            pg_connection_pool_ttl_stagger: RwLock::new(
                DEFAULT_PG_TIMESTAMP_ORACLE_CONNPOOL_TTL_STAGGER,
            ),
            pg_connection_pool_connect_timeout: RwLock::new(
                DEFAULT_PG_TIMESTAMP_ORACLE_CONNECT_TIMEOUT,
            ),
            pg_connection_pool_tcp_user_timeout: RwLock::new(
                DEFAULT_PG_TIMESTAMP_ORACLE_TCP_USER_TIMEOUT,
            ),
        }
    }
}

impl DynamicConfig {
    // TODO: Decide if we can relax these.
    const LOAD_ORDERING: Ordering = Ordering::SeqCst;
    const STORE_ORDERING: Ordering = Ordering::SeqCst;

    fn connection_pool_max_size(&self) -> usize {
        self.pg_connection_pool_max_size.load(Self::LOAD_ORDERING)
    }

    fn connection_pool_max_wait(&self) -> Option<Duration> {
        *self
            .pg_connection_pool_max_wait
            .read()
            .expect("lock poisoned")
    }

    fn connection_pool_ttl(&self) -> Duration {
        *self.pg_connection_pool_ttl.read().expect("lock poisoned")
    }

    fn connection_pool_ttl_stagger(&self) -> Duration {
        *self
            .pg_connection_pool_ttl_stagger
            .read()
            .expect("lock poisoned")
    }

    fn connect_timeout(&self) -> Duration {
        *self
            .pg_connection_pool_connect_timeout
            .read()
            .expect("lock poisoned")
    }

    fn tcp_user_timeout(&self) -> Duration {
        *self
            .pg_connection_pool_tcp_user_timeout
            .read()
            .expect("lock poisoned")
    }
}

impl PostgresClientKnobs for PostgresTimestampOracleConfig {
    fn connection_pool_max_size(&self) -> usize {
        self.dynamic.connection_pool_max_size()
    }

    fn connection_pool_max_wait(&self) -> Option<Duration> {
        self.dynamic.connection_pool_max_wait()
    }

    fn connection_pool_ttl(&self) -> Duration {
        self.dynamic.connection_pool_ttl()
    }

    fn connection_pool_ttl_stagger(&self) -> Duration {
        self.dynamic.connection_pool_ttl_stagger()
    }

    fn connect_timeout(&self) -> Duration {
        self.dynamic.connect_timeout()
    }

    fn tcp_user_timeout(&self) -> Duration {
        self.dynamic.tcp_user_timeout()
    }
}

/// Updates to values in [`PostgresTimestampOracleConfig`].
///
/// These reflect updates made to LaunchDarkly.
///
/// Parameters can be set (`Some`) or unset (`None`). Unset parameters should be
/// interpreted to mean "use the previous value".
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresTimestampOracleParameters {
    /// Configures `DynamicConfig::pg_connection_pool_max_size`.
    pub pg_connection_pool_max_size: Option<usize>,
    /// Configures `DynamicConfig::pg_connection_pool_max_wait`.
    ///
    /// NOTE: Yes, this is an `Option<Option<...>>`. Fields in this struct are
    /// all `Option` to signal the presence or absence of a system var
    /// configuration. A `Some(None)` value in this field signals the desire to
    /// have no `max_wait`. This is different from `None`, which signals that
    /// there is no actively set system var, although this case is not currently
    /// possible with how the code around this works.
    pub pg_connection_pool_max_wait: Option<Option<Duration>>,
    /// Configures `DynamicConfig::pg_connection_pool_ttl`.
    pub pg_connection_pool_ttl: Option<Duration>,
    /// Configures `DynamicConfig::pg_connection_pool_ttl_stagger`.
    pub pg_connection_pool_ttl_stagger: Option<Duration>,
    /// Configures `DynamicConfig::pg_connection_pool_connect_timeout`.
    pub pg_connection_pool_connect_timeout: Option<Duration>,
    /// Configures `DynamicConfig::pg_connection_pool_tcp_user_timeout`.
    pub pg_connection_pool_tcp_user_timeout: Option<Duration>,
}

impl PostgresTimestampOracleParameters {
    /// Update the parameter values with the set ones from `other`.
    pub fn update(&mut self, other: PostgresTimestampOracleParameters) {
        // Deconstruct self and other so we get a compile failure if new fields
        // are added.
        let Self {
            pg_connection_pool_max_size: self_pg_connection_pool_max_size,
            pg_connection_pool_max_wait: self_pg_connection_pool_max_wait,
            pg_connection_pool_ttl: self_pg_connection_pool_ttl,
            pg_connection_pool_ttl_stagger: self_pg_connection_pool_ttl_stagger,
            pg_connection_pool_connect_timeout: self_pg_connection_pool_connect_timeout,
            pg_connection_pool_tcp_user_timeout: self_pg_connection_pool_tcp_user_timeout,
        } = self;
        let Self {
            pg_connection_pool_max_size: other_pg_connection_pool_max_size,
            pg_connection_pool_max_wait: other_pg_connection_pool_max_wait,
            pg_connection_pool_ttl: other_pg_connection_pool_ttl,
            pg_connection_pool_ttl_stagger: other_pg_connection_pool_ttl_stagger,
            pg_connection_pool_connect_timeout: other_pg_connection_pool_connect_timeout,
            pg_connection_pool_tcp_user_timeout: other_pg_connection_pool_tcp_user_timeout,
        } = other;
        if let Some(v) = other_pg_connection_pool_max_size {
            *self_pg_connection_pool_max_size = Some(v);
        }
        if let Some(v) = other_pg_connection_pool_max_wait {
            *self_pg_connection_pool_max_wait = Some(v);
        }
        if let Some(v) = other_pg_connection_pool_ttl {
            *self_pg_connection_pool_ttl = Some(v);
        }
        if let Some(v) = other_pg_connection_pool_ttl_stagger {
            *self_pg_connection_pool_ttl_stagger = Some(v);
        }
        if let Some(v) = other_pg_connection_pool_connect_timeout {
            *self_pg_connection_pool_connect_timeout = Some(v);
        }
        if let Some(v) = other_pg_connection_pool_tcp_user_timeout {
            *self_pg_connection_pool_tcp_user_timeout = Some(v);
        }
    }

    /// Applies the parameter values to the given in-memory config object.
    ///
    /// Note that these overrides are not all applied atomically: i.e. it's
    /// possible for the timestamp oracle/postgres client to race with this and
    /// see some but not all of the parameters applied.
    pub fn apply(&self, cfg: &PostgresTimestampOracleConfig) {
        info!(params = ?self, "Applying configuration update!");

        // Deconstruct self so we get a compile failure if new fields are added.
        let Self {
            pg_connection_pool_max_size,
            pg_connection_pool_max_wait,
            pg_connection_pool_ttl,
            pg_connection_pool_ttl_stagger,
            pg_connection_pool_connect_timeout,
            pg_connection_pool_tcp_user_timeout,
        } = self;
        if let Some(pg_connection_pool_max_size) = pg_connection_pool_max_size {
            cfg.dynamic
                .pg_connection_pool_max_size
                .store(*pg_connection_pool_max_size, DynamicConfig::STORE_ORDERING);
        }
        if let Some(pg_connection_pool_max_wait) = pg_connection_pool_max_wait {
            let mut max_wait = cfg
                .dynamic
                .pg_connection_pool_max_wait
                .write()
                .expect("lock poisoned");
            *max_wait = *pg_connection_pool_max_wait;
        }
        if let Some(pg_connection_pool_ttl) = pg_connection_pool_ttl {
            let mut ttl = cfg
                .dynamic
                .pg_connection_pool_ttl
                .write()
                .expect("lock poisoned");
            *ttl = *pg_connection_pool_ttl;
        }
        if let Some(pg_connection_pool_ttl_stagger) = pg_connection_pool_ttl_stagger {
            let mut ttl_stagger = cfg
                .dynamic
                .pg_connection_pool_ttl_stagger
                .write()
                .expect("lock poisoned");
            *ttl_stagger = *pg_connection_pool_ttl_stagger;
        }
        if let Some(pg_connection_pool_connect_timeout) = pg_connection_pool_connect_timeout {
            let mut timeout = cfg
                .dynamic
                .pg_connection_pool_connect_timeout
                .write()
                .expect("lock poisoned");
            *timeout = *pg_connection_pool_connect_timeout;
        }
        if let Some(pg_connection_pool_tcp_user_timeout) = pg_connection_pool_tcp_user_timeout {
            let mut timeout = cfg
                .dynamic
                .pg_connection_pool_tcp_user_timeout
                .write()
                .expect("lock poisoned");
            *timeout = *pg_connection_pool_tcp_user_timeout;
        }
    }
}

impl<N> PostgresTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    /// Open a Postgres [`TimestampOracle`] instance with `config`, for the
    /// timeline named `timeline`. `next` generates new timestamps when invoked.
    /// Timestamps that are returned are made durable and will never retract.
    pub async fn open(
        config: PostgresTimestampOracleConfig,
        timeline: String,
        initially: Timestamp,
        next: N,
        read_only: bool,
    ) -> Self {
        info!(config = ?config, "opening PostgresTimestampOracle");

        let fallible = || async {
            let metrics = Arc::clone(&config.metrics);

            // don't need to unredact here because we're just pulling out the username
            let pg_config: Config = config.url.to_string().parse()?;
            let role = pg_config.get_user().unwrap();
            let create_schema = format!(
                "CREATE SCHEMA IF NOT EXISTS tsoracle AUTHORIZATION {}",
                escape_identifier(role),
            );

            let postgres_client = PostgresClient::open(config.clone().into())?;

            let client = postgres_client.get_connection().await?;

            let crdb_mode = match client
                .batch_execute(&format!(
                    "{}; {}{}; {}",
                    create_schema, SCHEMA, CRDB_SCHEMA_OPTIONS, CRDB_CONFIGURE_ZONE,
                ))
                .await
            {
                Ok(()) => true,
                Err(e)
                    if e.code() == Some(&SqlState::INVALID_PARAMETER_VALUE)
                        || e.code() == Some(&SqlState::SYNTAX_ERROR) =>
                {
                    info!(
                        "unable to initiate timestamp_oracle with CRDB params, this is expected and OK when running against Postgres: {:?}",
                        e
                    );
                    false
                }
                Err(e) => return Err(e.into()),
            };

            if !crdb_mode {
                client
                    .batch_execute(&format!("{}; {};", create_schema, SCHEMA))
                    .await?;
            }

            let oracle = PostgresTimestampOracle {
                timeline: timeline.clone(),
                next: next.clone(),
                postgres_client: Arc::new(postgres_client),
                metrics,
                read_only,
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
            if !read_only {
                TimestampOracle::apply_write(&oracle, initially).await;
            }

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
    pub async fn get_all_timelines(
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

    #[mz_ore::instrument(name = "oracle::write_ts")]
    async fn fallible_write_ts(&self) -> Result<WriteTimestamp<Timestamp>, anyhow::Error> {
        if self.read_only {
            panic!("attempting write_ts in read-only mode");
        }

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

    #[mz_ore::instrument(name = "oracle::peek_write_ts")]
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

    #[mz_ore::instrument(name = "oracle::read_ts")]
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

    #[mz_ore::instrument(name = "oracle::apply_write")]
    async fn fallible_apply_write(&self, write_ts: Timestamp) -> Result<(), anyhow::Error> {
        if self.read_only {
            panic!("attempting apply_write in read-only mode");
        }

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
        ts.0.0.try_into().expect("we only use u64 timestamps")
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
impl<N> TimestampOracle<Timestamp> for PostgresTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    #[instrument]
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

    #[instrument]
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

    #[instrument]
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

    #[instrument]
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
                    info!(
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

#[cfg(test)]
mod tests {
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

        crate::tests::timestamp_oracle_impl_test(|timeline, now_fn, initial_ts| {
            let oracle = PostgresTimestampOracle::open(
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
