// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by Postgres.

use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::PoolError;
use deadpool_postgres::tokio_postgres::Config;
use deadpool_postgres::tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use futures_util::StreamExt;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::{CastFrom, CastLossy};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::url::SensitiveUrl;
use mz_postgres_client::metrics::PostgresClientMetrics;
use mz_postgres_client::{
    Client, Connection, IsolationLevel, PostgresClient, PostgresClientConfig, PostgresClientKnobs,
};
use postgres_protocol::escape::escape_identifier;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Row, Statement};
use tracing::{info, warn};

use crate::error::Error;
use crate::location::{CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData};

/// Flag to run PostgreSQL consensus connections under READ COMMITTED isolation instead of SERIALIZABLE.
///
/// The query family used on vanilla Postgres is designed to be linearizable under READ COMMITTED
/// isolation, and therefore is also linearizable under SERIALIZABLE, so this flag can be flipped
/// freely. The flag only exists to make the upgrade path from older versions safe since multiple
/// query versions co-exist during a 0dt upgrade. See the note on the default value for more
/// information.
///
/// This flag should stay off on CockroachDB. The system will refuse to issue consensus queries if
/// the flag is enabled on CockroachDB.
pub const PG_CONSENSUS_READ_COMMITTED: mz_dyncfg::Config<bool> = mz_dyncfg::Config::new(
    "persist_pg_consensus_read_committed",
    // NOTE: The default value of this flag *MUST* remain *false* for as long as there are users
    // running Materialize versions <=v26.32. Environments running one of these versions will be
    // issuing consensus queries that are only correct in SERIALIZABLE isolation. Therefore this
    // flag must default to off so that during the 0dt deployment that takes an environment from
    // <=v26.32 to >=v26.33 the consensus queries of both families linearize together.
    false,
    "Run consensus connections under READ COMMITTED isolation instead of SERIALIZABLE when targetting
    PostgreSQL backends. This flag must be off when targetting CockroachDB.",
);

/// Maximum number of shared connections that single-statement consensus operations run on.
/// 0 disables sharing entirely (each operation checks out an exclusive pooled connection).
///
/// The shared set scales with demand: an operation prefers an idle shared connection (behaving
/// exactly like an exclusive checkout), grows the set while all connections are busy and the set
/// is below the cap, and only once the cap is reached pipelines onto the least-loaded connection
/// instead of queueing. tokio-postgres pipelines concurrent requests issued on the same
/// connection at the wire protocol level, so one connection can carry many in-flight operations.
/// Each operation is its own implicit transaction, so interleaving them on a shared session is
/// indistinguishable from running them on separate sessions. The multi-statement shard
/// initialization path always uses an exclusive pooled connection, since an explicit transaction
/// must not interleave.
///
/// The effective cap is clamped below the pool's max size, reserving capacity for the exclusive
/// paths so they cannot be starved by the shared set.
///
/// The default applies to CockroachDB as well, deliberately: with the network round trips real
/// deployments have, sharing raises throughput and lowers median latency there too (throughput
/// up to 1.7x and p50 down 1.3-1.7x at 2ms RTT in the committed benchmark). The one measured
/// regression is sub-millisecond RTT combined with thousands of concurrently writing shards,
/// where deeply pipelined commit bursts serve worse than pool queueing. Setting the flag to 0
/// restores the exclusive-checkout behavior and frees the shared connections.
pub const PG_CONSENSUS_PIPELINE_CONNECTIONS: mz_dyncfg::Config<usize> = mz_dyncfg::Config::new(
    "persist_pg_consensus_pipeline_connections",
    50,
    "Maximum number of shared connections that consensus operations run on, pipelining once all \
    are busy, instead of checking out an exclusive pooled connection per operation. 0 disables \
    sharing.",
);

/// Maximum number of operations in flight on one shared connection before new operations wait
/// for a slot instead of stacking deeper. 0 disables the limit.
///
/// Unbounded pipelines degrade at extreme concurrency: every queued operation waits behind the
/// entire queue in front of it on its connection, and past roughly a thousand in-flight
/// operations per connection both throughput and tail latency suffer. The default is high
/// enough that the limit only engages once total in-flight operations exceed the cap times the
/// shared set size (tens of thousands of concurrently writing shards at the default pool
/// size), where the benchmark shows bounding depth recovers throughput that unbounded
/// pipelines lose. The wait is signaled by operation completions, without polling.
pub const PG_CONSENSUS_PIPELINE_DEPTH: mz_dyncfg::Config<usize> = mz_dyncfg::Config::new(
    "persist_pg_consensus_pipeline_depth",
    1024,
    "Maximum operations in flight per shared consensus connection before new operations wait \
    for a slot. 0 disables the limit.",
);

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS consensus (
    shard text NOT NULL,
    sequence_number bigint NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY(shard, sequence_number)
)
";

// These `sql_stats_automatic_collection_enabled` are for the cost-based
// optimizer but all the queries against this table are single-table and very
// carefully tuned to hit the primary index, so the cost-based optimizer doesn't
// really get us anything. OTOH, the background jobs that crdb creates to
// collect these stats fill up the jobs table (slowing down all sorts of
// things).
const CRDB_SCHEMA_OPTIONS: &str = "WITH (sql_stats_automatic_collection_enabled = false)";
// The `consensus` table creates and deletes rows at a high frequency, generating many
// tombstoned rows. If Cockroach's GC interval is set high (the default is 25h) and
// these tombstones accumulate, scanning over the table will take increasingly and
// prohibitively long.
//
// See: https://github.com/MaterializeInc/database-issues/issues/4001
// See: https://www.cockroachlabs.com/docs/stable/configure-zone.html#variables
const CRDB_CONFIGURE_ZONE: &str = "ALTER TABLE consensus CONFIGURE ZONE USING gc.ttlseconds = 600";

/// NOTE: `mz-persist` intentionally does not depend on `mz-postgres-util`.
/// These helpers are the only direct driver-call boundary in this module.
async fn pg_batch_execute(client: &Client, query: &str) -> Result<(), tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.batch_execute(query).await
}

async fn pg_query_prepared(
    client: &Client,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.query(statement, params).await
}

async fn pg_query_opt_prepared(
    client: &Client,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<Row>, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.query_opt(statement, params).await
}

async fn pg_execute_prepared(
    client: &Client,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.execute(statement, params).await
}

async fn pg_txn_execute_prepared(
    txn: &deadpool_postgres::Transaction<'_>,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    txn.execute(statement, params).await
}

async fn pg_txn_query_one_prepared(
    txn: &deadpool_postgres::Transaction<'_>,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Row, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    txn.query_one(statement, params).await
}

impl ToSql for SeqNo {
    fn to_sql(
        &self,
        ty: &Type,
        w: &mut bytes::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        // We can only represent sequence numbers in the range [0, i64::MAX].
        let value = i64::try_from(self.0)?;
        <i64 as ToSql>::to_sql(&value, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <i64 as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<'a> FromSql<'a> for SeqNo {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<SeqNo, Box<dyn std::error::Error + Sync + Send>> {
        let sequence_number = <i64 as FromSql>::from_sql(ty, raw)?;

        // Sanity check that the sequence number we received falls in the
        // [0, i64::MAX] range.
        let sequence_number = u64::try_from(sequence_number)?;
        Ok(SeqNo(sequence_number))
    }

    fn accepts(ty: &Type) -> bool {
        <i64 as FromSql>::accepts(ty)
    }
}

/// Configuration to connect to a Postgres backed implementation of [Consensus].
#[derive(Clone, Debug)]
pub struct PostgresConsensusConfig {
    url: SensitiveUrl,
    knobs: Arc<dyn PostgresClientKnobs>,
    metrics: PostgresClientMetrics,
    dyncfg: Arc<ConfigSet>,
}

impl PostgresConsensusConfig {
    const EXTERNAL_TESTS_POSTGRES_URL: &'static str =
        "MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL";

    /// Returns a new [PostgresConsensusConfig] for use in production.
    pub fn new(
        url: &SensitiveUrl,
        knobs: Box<dyn PostgresClientKnobs>,
        metrics: PostgresClientMetrics,
        dyncfg: Arc<ConfigSet>,
    ) -> Result<Self, Error> {
        Ok(PostgresConsensusConfig {
            url: url.clone(),
            knobs: Arc::from(knobs),
            metrics,
            dyncfg,
        })
    }

    /// Returns a new [PostgresConsensusConfig] for use in unit tests.
    ///
    /// By default, persist tests that use external storage (like Postgres) are
    /// no-ops so that `cargo test` works on new environments without any
    /// configuration. To activate the tests for [PostgresConsensus] set the
    /// `MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL` environment variable
    /// with a valid connection url [1].
    ///
    /// [1]: https://docs.rs/tokio-postgres/latest/tokio_postgres/config/struct.Config.html#url
    pub fn new_for_test() -> Result<Option<Self>, Error> {
        let url = match std::env::var(Self::EXTERNAL_TESTS_POSTGRES_URL) {
            Ok(url) => SensitiveUrl::from_str(&url).map_err(|e| e.to_string())?,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        struct TestConsensusKnobs;
        impl std::fmt::Debug for TestConsensusKnobs {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TestConsensusKnobs").finish_non_exhaustive()
            }
        }
        impl PostgresClientKnobs for TestConsensusKnobs {
            fn connection_pool_max_size(&self) -> usize {
                2
            }

            fn connection_pool_max_wait(&self) -> Option<Duration> {
                Some(Duration::from_secs(1))
            }

            fn connection_pool_ttl(&self) -> Duration {
                Duration::MAX
            }
            fn connection_pool_ttl_stagger(&self) -> Duration {
                Duration::MAX
            }
            fn connect_timeout(&self) -> Duration {
                Duration::MAX
            }
            fn tcp_user_timeout(&self) -> Duration {
                Duration::ZERO
            }

            fn keepalives_idle(&self) -> Duration {
                Duration::from_secs(10)
            }

            fn keepalives_interval(&self) -> Duration {
                Duration::from_secs(5)
            }

            fn keepalives_retries(&self) -> u32 {
                5
            }

            fn statement_timeout(&self) -> Duration {
                Duration::ZERO
            }
        }

        let dyncfg = crate::cfg::all_dyn_configs(ConfigSet::default());
        let config = PostgresConsensusConfig::new(
            &url,
            Box::new(TestConsensusKnobs),
            PostgresClientMetrics::new(&MetricsRegistry::new(), "mz_persist"),
            Arc::new(dyncfg),
        )?;
        Ok(Some(config))
    }
}

/// What flavor of Postgres are we connected to for consensus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PostgresMode {
    /// CockroachDB, used in our cloud offering.
    CockroachDB,
    /// Vanilla Postgres, the default for our self-hosted offering.
    Postgres,
}

/// Implementation of [Consensus] over a Postgres database.
pub struct PostgresConsensus {
    postgres_client: PostgresClient,
    mode: PostgresMode,
    dyncfg: Arc<ConfigSet>,
    knobs: Arc<dyn PostgresClientKnobs>,
    pipeline: SharedConns,
    metrics: PostgresClientMetrics,
}

/// A demand-scaled set of connections shared by concurrent single-statement operations, used
/// when [PG_CONSENSUS_PIPELINE_CONNECTIONS] is non-zero.
///
/// Entries hold checked-out pool objects, so shared connections count against the pool's max
/// size and inherit its TLS, isolation, and timeout setup. An entry an operation is using is kept
/// alive by the operation's own Arc clone, which means `Arc::strong_count - 1` is exactly the
/// number of in-flight operations on that connection and is used for least-loaded dispatch.
/// Dead or expired entries are shed at acquisition time; dropping the last Arc returns the
/// object to the pool, where the pool's own recycling applies. The operation that was in flight
/// when a connection died surfaces an error to the caller, which retries at the persist layer.
#[derive(Default)]
struct SharedConns {
    conns: Mutex<Vec<SharedConn>>,
    /// Signaled once per completed shared operation, waking one operation waiting out the
    /// per-connection depth limit.
    released: tokio::sync::Notify,
}

struct SharedConn {
    conn: Arc<Connection>,
    created_at: Instant,
}

enum SharedAcquire {
    /// A connection with no other operation in flight, or the least-loaded one at cap.
    Conn(Arc<Connection>),
    /// All connections are busy and the set is below cap: the caller should check out a pool
    /// connection and offer it via [SharedConns::insert].
    Grow,
    /// The set is at cap and every connection is at the depth limit: the caller must wait for
    /// [SharedConns::released] and retry.
    Full,
}

impl SharedConns {
    fn acquire(&self, cap: usize, depth: usize, ttl: Duration) -> SharedAcquire {
        let mut conns = self.conns.lock().expect("lock poisoned");
        // Shed dead and expired entries. In-flight operations keep their entry alive through
        // their own Arc; once the last clone drops, the object returns to the pool.
        conns.retain(|c| !c.conn.is_closed() && c.created_at.elapsed() < ttl);
        // Shed entries past the cap, so a lowered cap converges immediately instead of the
        // excess staying in least-loaded dispatch until the TTL expires it. In-flight
        // operations on a shed entry finish undisturbed through their own Arc.
        conns.truncate(cap);
        // Prefer the first idle connection, keeping low-index connections hot and letting the
        // tail expire out after bursts.
        if let Some(c) = conns.iter().find(|c| Arc::strong_count(&c.conn) == 1) {
            return SharedAcquire::Conn(Arc::clone(&c.conn));
        }
        if conns.len() < cap {
            return SharedAcquire::Grow;
        }
        let least_loaded = conns
            .iter()
            .min_by_key(|c| Arc::strong_count(&c.conn))
            .expect("cap > 0 implies non-empty set");
        if depth > 0 && Arc::strong_count(&least_loaded.conn) - 1 >= depth {
            return SharedAcquire::Full;
        }
        SharedAcquire::Conn(Arc::clone(&least_loaded.conn))
    }

    /// Adds a connection to the shared set, unless a concurrent grower already filled it to
    /// `cap`. Either way the caller keeps using the connection it passed in for its own
    /// operation; an un-inserted surplus connection returns to the pool when the operation
    /// drops it.
    fn insert(&self, conn: &Arc<Connection>, cap: usize) {
        let mut conns = self.conns.lock().expect("lock poisoned");
        if conns.len() < cap {
            conns.push(SharedConn {
                conn: Arc::clone(conn),
                created_at: Instant::now(),
            });
        }
    }

    /// Drops every entry, returning each connection to the pool once its in-flight operations
    /// finish. Called when sharing is disabled, so the flag actually frees the pool capacity
    /// the shared set was occupying.
    ///
    /// NOTE: an operation that read a non-zero cap may insert a connection after a concurrent
    /// zero-cap operation cleared the set. Such a straggler lingers only until the next
    /// zero-cap operation clears again, so this needs no synchronization.
    fn clear(&self) {
        self.conns.lock().expect("lock poisoned").clear();
    }

    fn len(&self) -> usize {
        self.conns.lock().expect("lock poisoned").len()
    }
}

/// A connection to run a single-statement consensus operation on: either an exclusively held
/// pooled connection or a shared (possibly pipelined) one.
enum OpConn<'a> {
    Pooled(Connection),
    Shared(SharedLease<'a>),
}

/// A shared connection held for the duration of one operation. Dropping it signals
/// [SharedConns::released], admitting one operation waiting out the depth limit.
struct SharedLease<'a> {
    conn: Arc<Connection>,
    set: &'a SharedConns,
}

impl Drop for SharedLease<'_> {
    fn drop(&mut self) {
        // With no waiter this stores a single permit, which at worst causes one spurious
        // wakeup and re-check later.
        self.set.released.notify_one();
    }
}

impl std::ops::Deref for OpConn<'_> {
    type Target = Client;

    fn deref(&self) -> &Client {
        match self {
            OpConn::Pooled(conn) => conn,
            OpConn::Shared(lease) => &lease.conn,
        }
    }
}

impl std::fmt::Debug for PostgresConsensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresConsensus").finish_non_exhaustive()
    }
}

impl PostgresConsensus {
    /// Open a Postgres [Consensus] instance with `config`, for the collection
    /// named `shard`.
    pub async fn open(config: PostgresConsensusConfig) -> Result<Self, ExternalError> {
        // don't need to unredact here because we just want to pull out the username
        let pg_config: Config = config.url.to_string().parse()?;
        let role = pg_config.get_user().expect("failed to get PostgreSQL user");
        let create_schema = format!(
            "CREATE SCHEMA IF NOT EXISTS consensus AUTHORIZATION {}",
            escape_identifier(role),
        );

        let dyncfg = Arc::clone(&config.dyncfg);
        let knobs = Arc::clone(&config.knobs);

        // The resolver runs per connection, so a flag change takes effect as the pool cycles
        // connections. It unconditionally follows the flag: it does not know the backend. The
        // backend-specific safety check lives in `get_connection`, which asserts that CockroachDB
        // connections are SERIALIZABLE. The connection carries the level it was created with, so
        // that assertion is exact rather than a guess about what the resolver returned.
        let metrics = config.metrics.clone();
        let client_config = PostgresClientConfig::new(config.url, config.knobs, config.metrics)
            .with_isolation(Arc::new(move || {
                if PG_CONSENSUS_READ_COMMITTED.get(&dyncfg) {
                    IsolationLevel::ReadCommitted
                } else {
                    IsolationLevel::Serializable
                }
            }));
        let postgres_client = PostgresClient::open(client_config)?;

        let client = postgres_client.get_connection().await?;

        let mode = match pg_batch_execute(
            &client,
            &format!(
                "{}; {}{}; {};",
                create_schema, SCHEMA, CRDB_SCHEMA_OPTIONS, CRDB_CONFIGURE_ZONE,
            ),
        )
        .await
        {
            Ok(()) => PostgresMode::CockroachDB,
            Err(e) if e.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) => {
                warn!(
                    "unable to ALTER TABLE consensus, this is expected and OK when connecting with a read-only user"
                );
                PostgresMode::CockroachDB
            }
            // Vanilla Postgres doesn't support the Cockroach zone configuration
            // that we attempted, so we use that to determine what mode we're in.
            Err(e)
                if e.code() == Some(&SqlState::INVALID_PARAMETER_VALUE)
                    || e.code() == Some(&SqlState::SYNTAX_ERROR) =>
            {
                info!(
                    "unable to initiate consensus with CRDB params, this is expected and OK when running against Postgres: {:?}",
                    e
                );
                PostgresMode::Postgres
            }
            Err(e) => return Err(e.into()),
        };

        match mode {
            PostgresMode::CockroachDB => {}
            PostgresMode::Postgres => {
                pg_batch_execute(&client, &format!("{}; {};", create_schema, SCHEMA)).await?;
            }
        }

        Ok(PostgresConsensus {
            postgres_client,
            mode,
            dyncfg: Arc::clone(&config.dyncfg),
            knobs,
            pipeline: SharedConns::default(),
            metrics,
        })
    }

    /// Drops and recreates the `consensus` table in Postgres
    ///
    /// ONLY FOR TESTING
    pub async fn drop_and_recreate(&self) -> Result<(), ExternalError> {
        // this could be a TRUNCATE if we're confident the db won't reuse any state
        let client = self.get_connection().await?;
        pg_batch_execute(&client, "DROP TABLE consensus").await?;
        let crdb_mode = match pg_batch_execute(
            &client,
            &format!("{}{}; {}", SCHEMA, CRDB_SCHEMA_OPTIONS, CRDB_CONFIGURE_ZONE,),
        )
        .await
        {
            Ok(()) => true,
            Err(e) if e.code() == Some(&SqlState::INSUFFICIENT_PRIVILEGE) => {
                warn!(
                    "unable to ALTER TABLE consensus, this is expected and OK when connecting with a read-only user"
                );
                true
            }
            Err(e)
                if e.code() == Some(&SqlState::INVALID_PARAMETER_VALUE)
                    || e.code() == Some(&SqlState::SYNTAX_ERROR) =>
            {
                info!(
                    "unable to initiate consensus with CRDB params, this is expected and OK when running against Postgres: {:?}",
                    e
                );
                false
            }
            Err(e) => return Err(e.into()),
        };

        if !crdb_mode {
            pg_batch_execute(&client, SCHEMA).await?;
        }
        Ok(())
    }

    async fn get_connection(&self) -> Result<Connection, PoolError> {
        let conn = self.postgres_client.get_connection().await?;
        // On CockroachDB we always run the lockless `CRDB_*` queries, which are only linearizable
        // under SERIALIZABLE. The connection records the level it was created with, so we assert on
        // that exact level rather than trusting the isolation flag to be off. The `POSTGRES_*`
        // queries used on vanilla Postgres are correct under any isolation, so that backend needs
        // no check.
        if self.mode == PostgresMode::CockroachDB {
            assert_eq!(
                conn.isolation_level(),
                IsolationLevel::Serializable,
                "consensus on CockroachDB requires SERIALIZABLE isolation, refusing to run \
                 CRDB_* queries under {:?}",
                conn.isolation_level(),
            );
        }
        Ok(conn)
    }

    /// Returns the connection to run a single-statement operation on: a shared connection when
    /// [PG_CONSENSUS_PIPELINE_CONNECTIONS] is non-zero, otherwise an exclusive pooled connection.
    /// Multi-statement (explicit transaction) paths must use [Self::get_connection] instead, an
    /// explicit transaction must not interleave with other operations on a shared session.
    async fn op_connection(&self) -> Result<OpConn<'_>, PoolError> {
        // Clamp below the pool's actual size so the exclusive paths (shard init, schema setup)
        // always have at least one pool slot the shared set cannot occupy. This must use the
        // pool's built size, not the `connection_pool_max_size` knob: the knob is a dyncfg that a
        // system-parameter sync can raise after the pool was built (the pool only picks up a new
        // size across a restart). Clamping against the raised knob would let the shared set try to
        // occupy more slots than the pool has, seizing every connection and starving the exclusive
        // paths.
        let cap = PG_CONSENSUS_PIPELINE_CONNECTIONS.get(&self.dyncfg).min(
            self.postgres_client
                .connection_pool_max_size()
                .saturating_sub(1),
        );
        if cap == 0 {
            // Sharing is disabled: drop the shared set, otherwise its entries would stay
            // checked out of the pool until process restart and the flag would degrade the
            // exclusive path to whatever slots the set left over, instead of restoring the
            // pre-sharing behavior it exists to roll back to.
            self.pipeline.clear();
            self.metrics.connpool_shared_size.set(0);
            return Ok(OpConn::Pooled(self.get_connection().await?));
        }
        let depth = PG_CONSENSUS_PIPELINE_DEPTH.get(&self.dyncfg);
        let ttl = self.knobs.connection_pool_ttl();
        let conn = loop {
            // Creating the future before the acquire check makes the wait race-free: a
            // completion between the check and the await stores a permit that the first poll
            // consumes.
            let released = self.pipeline.released.notified();
            match self.pipeline.acquire(cap, depth, ttl) {
                SharedAcquire::Conn(conn) => break conn,
                SharedAcquire::Grow => {
                    let conn = Arc::new(self.get_connection().await?);
                    self.pipeline.insert(&conn, cap);
                    break conn;
                }
                SharedAcquire::Full => released.await,
            }
        };
        // strong_count - 1 discounts the set's own entry, leaving the in-flight operations
        // including this one. A surplus connection that lost the insert race has no set entry,
        // so the max(1) keeps its (sole) operation counted.
        self.metrics
            .connpool_shared_inflight
            .observe(f64::cast_lossy((Arc::strong_count(&conn) - 1).max(1)));
        self.metrics
            .connpool_shared_size
            .set(u64::cast_from(self.pipeline.len()));
        Ok(OpConn::Shared(SharedLease {
            conn,
            set: &self.pipeline,
        }))
    }
}

#[async_trait]
impl Consensus for PostgresConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let q = "SELECT DISTINCT shard FROM consensus";

        Box::pin(try_stream! {
            // NB: it's important that we hang on to this client for the lifetime of the stream,
            // to avoid returning it to the pool prematurely.
            //
            // This must be an exclusive connection, not a shared one: tokio-postgres delivers
            // responses in request order and stops reading the socket while a consumer lags,
            // so a lazily drained stream would head-of-line block every operation pipelined
            // behind it. list_keys is rare (admin tooling) and not latency-sensitive.
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(q).await?;
            let params: &[String] = &[];
            let mut rows = Box::pin(client.query_raw(&statement, params).await?);
            while let Some(row) = rows.next().await {
                let shard: String = row?.try_get("shard")?;
                yield shard;
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let q = "SELECT sequence_number, data FROM consensus
             WHERE shard = $1 ORDER BY sequence_number DESC LIMIT 1";
        let row = {
            let client = self.op_connection().await?;
            let statement = client.prepare_cached(q).await?;
            pg_query_opt_prepared(&client, &statement, &[&key]).await?
        };
        let row = match row {
            None => return Ok(None),
            Some(row) => row,
        };

        let seqno: SeqNo = row.try_get("sequence_number")?;

        let data: Vec<u8> = row.try_get("data")?;
        Ok(Some(VersionedData {
            seqno,
            data: Bytes::from(data),
        }))
    }

    async fn compare_and_set(
        &self,
        key: &str,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        let expected = new.seqno.previous();

        let result = match expected {
            Some(expected) => {
                /// This query has been written to execute within a single
                /// network round-trip. The insert performance has been tuned
                /// against CockroachDB, ensuring it goes through the fast-path
                /// 1-phase commit of CRDB. Any changes to this query should
                /// confirm an EXPLAIN ANALYZE (VERBOSE) query plan contains
                /// `auto commit`
                static CRDB_CAS_QUERY: &str = "
                    INSERT INTO consensus (shard, sequence_number, data)
                    SELECT $1, $2, $3
                    WHERE (SELECT sequence_number FROM consensus
                        WHERE shard = $1
                        ORDER BY sequence_number DESC LIMIT 1) = $4;
                ";

                // ## Correctness argument
                //
                // The Postgres tuned queries are designed to be correct under READ COMMITTED
                // isolation. In that mode each operation sees its own snapshot of the database and
                // special care is needed to ensure that the observable behavior is linearizable.
                //
                // The whole argument rests on one invariant: the live sequence numbers `>= 0` form
                // a contiguous range with no gaps, whose maximum is the head. Appends only ever
                // extend the head by one and truncation only ever removes a prefix, preserving
                // contiguity. (The `-1` sentinel written at init sits below all appends and never
                // participates in any test here, so we ignore it.) The cases below rely on the
                // following equivalence:
                //
                //        `seqno` is the head iff `seqno` is present and `seqno+1` is absent.
                //
                // A client performs CaS operations at a seqno one above the seqno it has already
                // observed, unless it is initializing the shard for the first time, in which case it
                // does a plain insert. The two scenarios are analyzed separately:
                //
                // 1. CaS for `expected_seqno+1` issued after `expected_seqno` was observed
                //
                // Because `expected_seqno` was observed, the consensus table must have contained a
                // row with it at some point, the `expected_row`.
                //
                // The first operation of the CaS query is to find `expected_row` and lock it with
                // a `FOR KEY SHARE` lock. The `expected_row` may or may not still exist, depending on
                // how outdated the client is. If `expected_seqno` is the current head the row is
                // guaranteed to exist, since truncation never deletes the head. This gives two cases:
                //
                // 1.1. `expected_row` exists
                //
                // The INSERT is the linearization point of this CaS, and it commits a row iff
                // `expected_seqno` is the head at the instant the INSERT runs. The held lock keeps
                // `expected_seqno` present at that instant (see 1.1.3). By contiguity it is then
                // the head iff `expected_seqno+1` is absent. This is exactly what the PRIMARY KEY
                // tests as the INSERT tries to write `expected_seqno+1` (`$2`). So the INSERT
                // writes one row and the call returns Committed (advancing the head to
                // `expected_seqno+1`, the range still contiguous) precisely when `expected_seqno`
                // was the head; otherwise the PK raises `unique_violation` and the call returns
                // ExpectationMismatch with the table unchanged. The remaining obligation is that
                // operations interleaving between the lock and the INSERT cannot break this. There
                // are three cases:
                //
                // 1.1.1 Another CaS has taken its own `expected_row_2` lock but not inserted yet.
                //
                // `FOR KEY SHARE` is shared, so the two locks neither block each other nor wait:
                // locks never serialize appenders, the PRIMARY KEY does. Having only locked, the
                // other CaS has not changed the table, so it does not affect what this INSERT
                // observes. If it is racing for the same head (`expected_row_2 == expected_row`) both
                // appenders attempt to INSERT the same `expected_seqno+1`, and the PK admits exactly
                // one. Whichever INSERT commits first is linearized first and becomes the head; the
                // other then finds `expected_seqno+1` present and is rejected — case 1.1.2 from its
                // side.
                //
                // 1.1.2 Another CaS has performed its `expected_seqno+1` insertion.
                //
                // By contiguity an append always targets head+1, so the only insertion that can
                // affect this one is `expected_seqno+1` itself: nothing above it can be added while
                // `expected_seqno+1` is still absent. If such an insert commits first, then
                // `expected_seqno` is no longer the head, and this INSERT now finds `expected_seqno+1`
                // present and is rejected by the PK → ExpectationMismatch. That is correct: this CaS
                // is linearized after the other appender, and at that point it must fail.
                //
                // 1.1.3 A truncation has happened.
                //
                // A truncation deletes a prefix `[0, cut)` and never the head. Its DELETE takes a
                // `FOR UPDATE`-strength row lock on each row it removes, and that conflicts with the
                // `FOR KEY SHARE` held on `expected_seqno`. So no committed truncation can have
                // removed `expected_seqno` while that lock is held: any truncation that commits in
                // this window has `cut <= expected_seqno` and deletes only rows strictly below
                // `expected_seqno`. That leaves `expected_seqno` present and the presence/absence of
                // `expected_seqno+1` untouched, so it does not change the INSERT's outcome, and
                // removing a low prefix keeps the range contiguous. This is also why, once locked,
                // `expected_row` is still present at INSERT time, closing the "lock found a row, then
                // it was GC'd before the INSERT" hole.
                //
                // 1.2. `expected_row` does not exist
                //
                // The CTE is empty, the INSERT touches zero rows, and the call returns
                // ExpectationMismatch without modifying the table. This is correct: `expected_seqno`
                // was observed, so it was present once, and the only way a present seqno later
                // becomes absent is truncation (inserts never delete). A truncation that removed
                // `expected_seqno` advanced the head strictly above it (it removes a prefix and keeps
                // the head), so `expected_seqno` is not the head and the CaS must fail. This operation
                // linearizes at its locking SELECT, where `expected_seqno` is already gone.
                //
                // 2. CaS that initializes the shard, issued with `expected` = None
                //
                // The init path (see the `None` arm) inserts a `-1` sentinel and the first row at
                // seqno 0, then commits only if `max(sequence_number)` is 0. It commits (head
                // becomes 0) exactly when the shard was empty: a shard that still holds seqno 0
                // fails the insert's PK, and a shard with a head above 0 either fails the PK (if
                // seqno 0 is present) or, if seqno 0 was truncated away, inserts into the gap and
                // is caught by the `max > 0` check and rolled back. This is correct because an
                // initialized shard always retains a live row (truncation never removes the head).
                // Concurrent first-time inits serialize on the seqno-0 PK lock, so exactly one wins.
                static POSTGRES_CAS_QUERY: &str = "
                WITH expected_row AS (
                    SELECT sequence_number FROM consensus
                    WHERE shard = $1 AND sequence_number = $4
                    FOR KEY SHARE
                )
                INSERT INTO consensus (shard, sequence_number, data)
                SELECT $1, $2, $3
                FROM expected_row;
                ";

                let q = match self.mode {
                    PostgresMode::CockroachDB => CRDB_CAS_QUERY,
                    PostgresMode::Postgres => POSTGRES_CAS_QUERY,
                };
                let client = self.op_connection().await?;
                let statement = client.prepare_cached(q).await?;
                pg_execute_prepared(
                    &client,
                    &statement,
                    &[&key, &new.seqno, &new.data.as_ref(), &expected],
                )
                .await
            }
            None => {
                match self.mode {
                    PostgresMode::Postgres => {
                        // SUBTLE: This query is designed to be linearizable with respect to:
                        // * the other POSTGRES queries (cas/truncate) present in this file under
                        //   READ COMMITTED isolation
                        // * the CRDB and POSTGRES queries existed in Materialize versions <=v26.30
                        //   that run under SERIALIZABLE isolation
                        // * the POSTGRES queries that were introduced in
                        //   d6dff42fd69d1a87edc4ae99e7ea1364201830dc, exist in versions v26.31 and
                        //   v26.32, and run under READ COMMITTED isolation
                        //
                        // # Correctness with respect to current version
                        //
                        // The POSTGRES queries in the current version are designed to be
                        // linearizable under READ COMMITTED, therefore the correctness argument is
                        // identical under SERIALIZABLE which is a strictly stronger isolation
                        // level.
                        //
                        // ## Concurrent initialization
                        //
                        // The first query to insert seqno 0 into the shard will take the primary
                        // key lock for that row. Any concurrent clients that attempt to also
                        // insert seqno 0 will have their INSERT statement block on the PK lock
                        // until the first query commits, forcing them to seriale after the winning
                        // query commits at which point they will receive a PK violation error and
                        // only the first client will win.
                        //
                        // ## Stale initlization
                        //
                        // If stale client attempts to insert seqno 0 in an
                        // already initialized shard then:
                        // * if seqno 0 has not been truncated the insert fails with a PK violation
                        // * if seqno 0 has been truncated the insert succeeds but the subsequent
                        // read of the max seqno will return the current head and the tx will rollback.
                        //
                        // Note that the sentinel seqno of -1 does not participate and is not
                        // needed for this correctness argument. It only exists to cover the
                        // co-existence with v26.31 queries (see below).
                        //
                        // # Correctness with respect to v26.30 (CRDB/POSTGRES family,
                        //   SERIALIZABLE) and v26.31/v26.32 (CRDB family, SERIALIZABLE)
                        //
                        // This query linearizes correctly with queries from v26.30 only in
                        // SERIALIZABLE mode. This is why the `persist_pg_consensus_read_committed`
                        // must default to off (see note in flag definition) so that during the
                        // co-existence period of 0dt upgrades both systems participate in the
                        // SERIALIZABLE conflict resolution.
                        //
                        // The correctness argument is that in the absence of concurrent mutations
                        // (i.e in SERIALIZABLE) this query only succeeds if the shard is
                        // uninitialized. Therefore only one client will succeed.
                        //
                        // # Correctness with respect to v26.31/v26.32 (POSTGRES family, READ COMMITTED)
                        //
                        // Versions v26.31 and v26.32 run in READ COMMITTED isolation and
                        // initialize a shard by blindly inserting seqno -1 and seqno 0 into the
                        // shard. By never truncating seqno -1 it ensures that stale initialiations
                        // hit PK violations on seqno -1 even though seqno 0 has been removed.
                        //
                        // For this reason this version must also insert the sentinel seqno -1 in
                        // as part of its initialization and avoid truncating it during truncation.
                        // Without this treatment a stale shard initialization from a version
                        // v26.31 client against a shard that had been initialized, written to, and
                        // truncated by a v26.33 client would succeed, and the shard state would be
                        // corrupted since the seqnos would not be contiguous anymore.
                        //
                        // When versions v26.31/v26.32 have become old enough that we believe no
                        // one will run them again we can remove the sentinel seqno handling from
                        // the initialization and truncation queries to simplify them.
                        static POSTGRES_INIT_INSERT: &str =
                            "INSERT INTO consensus (shard, sequence_number, data)
                        VALUES ($1, -1, ''), ($1, $2, $3)";
                        static POSTGRES_INIT_MAX: &str =
                            "SELECT max(sequence_number) FROM consensus WHERE shard = $1";
                        let mut client = self.get_connection().await?;
                        let txn = client.transaction().await?;
                        let insert = txn.prepare_cached(POSTGRES_INIT_INSERT).await?;
                        match pg_txn_execute_prepared(
                            &txn,
                            &insert,
                            &[&key, &new.seqno, &new.data.as_ref()],
                        )
                        .await
                        {
                            Ok(_) => {
                                let max_stmt = txn.prepare_cached(POSTGRES_INIT_MAX).await?;
                                let row =
                                    pg_txn_query_one_prepared(&txn, &max_stmt, &[&key]).await?;
                                let max: SeqNo = row.try_get(0)?;
                                if max == SeqNo::minimum() {
                                    txn.commit().await?;
                                    Ok(1)
                                } else {
                                    txn.rollback().await?;
                                    Ok(0)
                                }
                            }
                            // The insert failed (e.g. `unique_violation` because seqno 0 already
                            // exists). Roll back and let the caller map the error below.
                            Err(e) => {
                                let _ = txn.rollback().await;
                                Err(e)
                            }
                        }
                    }
                    PostgresMode::CockroachDB => {
                        static CRDB_INIT_QUERY: &str =
                            "INSERT INTO consensus SELECT $1, $2, $3 WHERE
                        NOT EXISTS (
                            SELECT * FROM consensus WHERE shard = $1
                        )";
                        let client = self.op_connection().await?;
                        let statement = client.prepare_cached(CRDB_INIT_QUERY).await?;
                        pg_execute_prepared(
                            &client,
                            &statement,
                            &[&key, &new.seqno, &new.data.as_ref()],
                        )
                        .await
                    }
                }
            }
        };

        match result {
            Ok(n) if n >= 1 => Ok(CaSResult::Committed),
            Ok(_) => Ok(CaSResult::ExpectationMismatch),
            Err(e) if e.code() == Some(&SqlState::UNIQUE_VIOLATION) => {
                Ok(CaSResult::ExpectationMismatch)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let q = "SELECT sequence_number, data FROM consensus
             WHERE shard = $1 AND sequence_number >= $2
             ORDER BY sequence_number ASC LIMIT $3";
        let Ok(limit) = i64::try_from(limit) else {
            return Err(ExternalError::from(anyhow!(
                "limit must be [0, i64::MAX]. was: {:?}",
                limit
            )));
        };
        let rows = {
            let client = self.op_connection().await?;
            let statement = client.prepare_cached(q).await?;
            pg_query_prepared(&client, &statement, &[&key, &from, &limit]).await?
        };
        let mut results = Vec::with_capacity(rows.len());

        for row in rows {
            let seqno: SeqNo = row.try_get("sequence_number")?;
            let data: Vec<u8> = row.try_get("data")?;
            results.push(VersionedData {
                seqno,
                data: Bytes::from(data),
            });
        }
        Ok(results)
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        // The `sequence_number >= 0` clause preserves the seqno `-1` sentinel that the
        // initialization from v26.31/v26.32 clients writes. The sentinel is a truncation-proof
        // "already initialized" marker (relied on by v26.31/v26.32 and preserved for it during a
        // rolling deploy). The clause is a no-op for shards that have no sentinel, since all of
        // their seqnos are already >= 0.
        static TRUNCATE_QUERY: &str = "
        DELETE FROM consensus
        WHERE shard = $1 AND sequence_number >= 0 AND sequence_number < $2 AND
        EXISTS (
            SELECT * FROM consensus WHERE shard = $1 AND sequence_number >= $2
        )
        ";

        let result = {
            let client = self.op_connection().await?;
            let statement = client.prepare_cached(TRUNCATE_QUERY).await?;
            pg_execute_prepared(&client, &statement, &[&key, &seqno]).await?
        };
        if result == 0 {
            // We weren't able to successfully truncate any rows inspect head to
            // determine whether the request was valid and there were no records in
            // the provided range, or the request was invalid because it would have
            // also deleted head.

            // It's safe to call head in a subsequent transaction rather than doing
            // so directly in the same transaction because, once a given (seqno, data)
            // pair exists for our shard, we enforce the invariants that
            // 1. Our shard will always have _some_ data mapped to it.
            // 2. All operations that modify the (seqno, data) can only increase
            //    the sequence number.
            let current = self.head(key).await?;
            if current.map_or(true, |data| data.seqno < seqno) {
                return Err(ExternalError::from(anyhow!(
                    "upper bound too high for truncate: {:?}",
                    seqno
                )));
            }
        }

        Ok(Some(usize::cast_from(result)))
    }
}

#[cfg(test)]
mod tests {
    use mz_dyncfg::ConfigUpdates;
    use mz_ore::assert_err;
    use tracing::info;
    use uuid::Uuid;

    use crate::location::tests::consensus_impl_test;

    use super::*;

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn postgres_consensus() -> Result<(), ExternalError> {
        let config = match PostgresConsensusConfig::new_for_test()? {
            Some(config) => config,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    PostgresConsensusConfig::EXTERNAL_TESTS_POSTGRES_URL
                );
                return Ok(());
            }
        };

        consensus_impl_test(|| PostgresConsensus::open(config.clone())).await?;

        // On a Postgres backend the default run above already exercises the tuned queries under
        // SERIALIZABLE. Re-run the contract with READ COMMITTED to also cover that isolation (on a
        // CockroachDB backend the flag is a no-op and this just re-runs the default queries).
        // `consensus_impl_test` asserts on `list_keys`, so it needs a clean table: the previous run
        // leaves shards behind, so drop and recreate the table before running it again.
        let read_committed_config =
            PostgresConsensusConfig::new_for_test()?.expect("postgres url was set above");
        {
            let mut updates = ConfigUpdates::default();
            updates.add(&PG_CONSENSUS_READ_COMMITTED, true);
            // The default-config run above already covers the shared-connection path (the flag
            // defaults on). Pin this run to the exclusive-checkout pool for coverage of that
            // path.
            updates.add(&PG_CONSENSUS_PIPELINE_CONNECTIONS, 0);
            updates.apply(&read_committed_config.dyncfg);
        }
        PostgresConsensus::open(read_committed_config.clone())
            .await?
            .drop_and_recreate()
            .await?;
        consensus_impl_test(|| PostgresConsensus::open(read_committed_config.clone())).await?;

        // Re-run the contract with READ COMMITTED plus operations on shared connections, which
        // must be observably identical to exclusive pooled connections. (The test knobs cap the
        // pool at 2, so the effective shared cap is 1 and every operation pipelines onto a
        // single connection, the most extreme sharing.)
        let pipelined_config =
            PostgresConsensusConfig::new_for_test()?.expect("postgres url was set above");
        {
            let mut updates = ConfigUpdates::default();
            updates.add(&PG_CONSENSUS_READ_COMMITTED, true);
            updates.add(&PG_CONSENSUS_PIPELINE_CONNECTIONS, 2);
            updates.apply(&pipelined_config.dyncfg);
        }
        PostgresConsensus::open(pipelined_config.clone())
            .await?
            .drop_and_recreate()
            .await?;
        consensus_impl_test(|| PostgresConsensus::open(pipelined_config.clone())).await?;

        // Re-run the contract with the per-connection depth limit at its most extreme (cap 1,
        // depth 1: every operation on the single shared connection makes concurrent ones wait),
        // which must also be observably identical.
        let depth_config =
            PostgresConsensusConfig::new_for_test()?.expect("postgres url was set above");
        {
            let mut updates = ConfigUpdates::default();
            updates.add(&PG_CONSENSUS_READ_COMMITTED, true);
            updates.add(&PG_CONSENSUS_PIPELINE_CONNECTIONS, 2);
            updates.add(&PG_CONSENSUS_PIPELINE_DEPTH, 1);
            updates.apply(&depth_config.dyncfg);
        }
        PostgresConsensus::open(depth_config.clone())
            .await?
            .drop_and_recreate()
            .await?;
        consensus_impl_test(|| PostgresConsensus::open(depth_config.clone())).await?;

        // The flag is a kill switch: flipping it to 0 at runtime must drain the shared set,
        // returning its connections to the pool, rather than stranding them checked out until
        // the process restarts.
        {
            let consensus = PostgresConsensus::open(pipelined_config.clone()).await?;
            let key = Uuid::new_v4().to_string();
            // A single-statement operation populates the shared set.
            assert_eq!(consensus.head(&key).await, Ok(None));
            assert_eq!(
                consensus
                    .pipeline
                    .conns
                    .lock()
                    .expect("lock poisoned")
                    .len(),
                1
            );
            let mut updates = ConfigUpdates::default();
            updates.add(&PG_CONSENSUS_PIPELINE_CONNECTIONS, 0);
            updates.apply(&pipelined_config.dyncfg);
            assert_eq!(consensus.head(&key).await, Ok(None));
            assert_eq!(
                consensus
                    .pipeline
                    .conns
                    .lock()
                    .expect("lock poisoned")
                    .len(),
                0
            );
        }

        // and now verify the implementation-specific `drop_and_recreate` works as intended
        let consensus = PostgresConsensus::open(config.clone()).await?;
        let key = Uuid::new_v4().to_string();
        let state = VersionedData {
            seqno: SeqNo(0),
            data: Bytes::from("abc"),
        };

        assert_eq!(
            consensus.compare_and_set(&key, state.clone()).await,
            Ok(CaSResult::Committed),
        );

        assert_eq!(consensus.head(&key).await, Ok(Some(state.clone())));

        consensus.drop_and_recreate().await?;

        assert_eq!(consensus.head(&key).await, Ok(None));

        // This should be a separate postgres_consensus_blocking test, but nextest makes it
        // difficult since we can't specify that both tests touch the consensus table and thus
        // interfere with each other.
        let config = match PostgresConsensusConfig::new_for_test()? {
            Some(config) => config,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    PostgresConsensusConfig::EXTERNAL_TESTS_POSTGRES_URL
                );
                return Ok(());
            }
        };

        let consensus: PostgresConsensus = PostgresConsensus::open(config.clone()).await?;
        // Max size in test is 2... let's saturate the pool.
        let _conn1 = consensus.get_connection().await?;
        let _conn2 = consensus.get_connection().await?;

        // And finally, we should see the next connect time out.
        let conn3 = consensus.get_connection().await;

        assert_err!(conn3);

        Ok(())
    }
}
