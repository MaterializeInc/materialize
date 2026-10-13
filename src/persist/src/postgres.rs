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
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::PoolError;
use deadpool_postgres::tokio_postgres::Config;
use deadpool_postgres::tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use futures_util::StreamExt;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::url::SensitiveUrl;
use mz_postgres_client::metrics::PostgresClientMetrics;
use mz_postgres_client::{
    Connection, IsolationLevel, PostgresClient, PostgresClientConfig, PostgresClientKnobs,
};
use postgres_protocol::escape::escape_identifier;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Row, Statement};
use tracing::{info, warn};

use crate::error::Error;
use crate::location::{CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData};

/// Flag to run PostgreSQL consensus connections under READ COMMITTED isolation instead of SERIALIZABLE.
///
/// The query family used on vanilla Postgres is linearizable under READ COMMITTED isolation and
/// under SERIALIZABLE isolation, and remains linearizable when some connections run under each,
/// so this flag can be flipped freely once every process runs this query family. Every operation
/// of the family is a single statement, and a single-statement transaction takes its snapshot at
/// the same instant under either isolation level, so the only difference between the levels is
/// that SERIALIZABLE may additionally abort a statement with a serialization failure, which the
/// caller retries. The flag only exists to make the upgrade path from older versions safe since
/// multiple query versions co-exist during a 0dt upgrade. See the note on the default value for
/// more information.
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
    mz_dyncfg::ParameterScope::Environment,
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
async fn pg_batch_execute(client: &Connection, query: &str) -> Result<(), tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.batch_execute(query).await
}

async fn pg_query_prepared(
    client: &Connection,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.query(statement, params).await
}

async fn pg_query_opt_prepared(
    client: &Connection,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<Row>, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.query_opt(statement, params).await
}

async fn pg_execute_prepared(
    client: &Connection,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<u64, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.execute(statement, params).await
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

        let dyncfg = ConfigSet::default().add(&PG_CONSENSUS_READ_COMMITTED);
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

        // The resolver runs per connection, so a flag change takes effect as the pool cycles
        // connections. It unconditionally follows the flag: it does not know the backend. The
        // backend-specific safety check lives in `get_connection`, which asserts that CockroachDB
        // connections are SERIALIZABLE. The connection carries the level it was created with, so
        // that assertion is exact rather than a guess about what the resolver returned.
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
}

#[async_trait]
impl Consensus for PostgresConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let q = "SELECT DISTINCT shard FROM consensus";

        Box::pin(try_stream! {
            // NB: it's important that we hang on to this client for the lifetime of the stream,
            // to avoid returning it to the pool prematurely.
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
            let client = self.get_connection().await?;
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
                // The init path (see the `None` arm) is a single statement that inserts the `-1`
                // marker and seqno 0, guarded by `NOT EXISTS` over the shard.
                //
                // Against a shard this CaS could be appending to, the existence guard is false and
                // the init affects no rows. Against an empty shard it inserts both rows
                // atomically, and there is no append to interleave with.
                //
                // Concurrent first-time inits block on the uncommitted `-1` marker and the loser
                // is rejected by the PK, so exactly one wins.
                //
                // Truncation does not remove the marker, so a delayed init cannot re-use seqno 0
                // if a gap is left by truncation.
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
                let client = self.get_connection().await?;
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
                        // * the other POSTGRES queries (cas/truncate) present in this file, under
                        //   READ COMMITTED isolation, under SERIALIZABLE isolation, and under a
                        //   mixture of the two
                        // * the CRDB and POSTGRES queries that existed in Materialize versions
                        //   <=v26.30 and run under SERIALIZABLE isolation
                        // * the POSTGRES queries that were introduced in
                        //   d6dff42fd69d1a87edc4ae99e7ea1364201830dc, exist in versions v26.31 and
                        //   v26.32, and run under READ COMMITTED isolation
                        //
                        // The statement inserts two rows, a marker at seqno -1 with no data and
                        // the first record at seqno 0, guarded by `NOT EXISTS` over the shard.
                        // The guard is evaluated against the statement's snapshot. The primary key
                        // check of the INSERT runs against the live index. Truncation never
                        // deletes the marker (see `truncate`), so a shard created by this query
                        // keeps its marker for as long as it exists.
                        //
                        // # Correctness with respect to the current version
                        //
                        // The specification is that the first write succeeds iff the shard is
                        // empty. There are three cases at the instant the INSERT runs:
                        //
                        // * The guard saw the shard non-empty. Non-emptiness is monotone (no
                        //   truncation deletes the head), so the shard is still non-empty and the
                        //   INSERT correctly affects zero rows.
                        // * The guard saw the shard empty and the shard is still empty. Both rows
                        //   are inserted and the shard is created. Concurrent first writers block
                        //   on the uncommitted marker row and then fail on the primary key, so
                        //   exactly one of them commits.
                        // * The guard saw the shard empty but the shard was created in between.
                        //   Every creator running this query family writes the marker, no
                        //   truncation removes it, and this INSERT targets the marker's key, so it
                        //   fails on the primary key regardless of what happened to seqno 0. This
                        //   is why the marker is load-bearing: without it a delayed first writer
                        //   could re-insert seqno 0 into a shard whose seqno 0 had been truncated.
                        //
                        // None of this depends on the isolation level. The guard is stale in the
                        // same way under READ COMMITTED and under SERIALIZABLE (a single-statement
                        // transaction takes its snapshot when the statement starts in both), and
                        // the marker collision closes the window in both. In particular the
                        // argument does not rely on SSI, which matters while the
                        // `persist_pg_consensus_read_committed` flag propagates and some
                        // connections run under each level: SSI only tracks conflicts among
                        // SERIALIZABLE transactions and would not see a READ COMMITTED creator.
                        //
                        // # Correctness with respect to <=v26.30 (SERIALIZABLE only)
                        //
                        // The <=v26.30 first write is `INSERT ... WHERE NOT EXISTS` without a
                        // marker, and the <=v26.30 truncate deletes the marker. Mixed with those
                        // queries this query is correct only when every transaction in the system
                        // is SERIALIZABLE, which is why `persist_pg_consensus_read_committed` must
                        // default to off (see the note on the flag). Under SERIALIZABLE every
                        // interleaving is equivalent to a serial one, and in a serial execution
                        // this statement inserts iff the shard is empty, which agrees with the old
                        // guard. The guard is also a read, so SSI tracks the statement; a blind
                        // insert of the two rows would be serializable even when it resurrects a
                        // truncated seqno 0 on a marker-less shard, and so would not be safe here.
                        //
                        // # Correctness with respect to v26.31/v26.32 (READ COMMITTED)
                        //
                        // Those versions initialize a shard by blindly inserting seqno -1 and
                        // seqno 0 and rely on colliding with the marker of any existing shard.
                        // This query writes the same marker and `truncate` preserves it, so a stale
                        // v26.31 first write against a shard created here fails on the marker, and
                        // a stale first write from here against a shard created by v26.31 fails
                        // the same way.
                        static POSTGRES_INIT_QUERY: &str = "
                        INSERT INTO consensus (shard, sequence_number, data)
                        SELECT v.shard, v.sequence_number, v.data
                        FROM (VALUES ($1::text, -1::bigint, ''::bytea), ($1::text, $2::bigint, $3::bytea))
                            AS v (shard, sequence_number, data)
                        WHERE NOT EXISTS (SELECT * FROM consensus WHERE shard = $1)
                        ";
                        let client = self.get_connection().await?;
                        let statement = client.prepare_cached(POSTGRES_INIT_QUERY).await?;
                        pg_execute_prepared(
                            &client,
                            &statement,
                            &[&key, &new.seqno, &new.data.as_ref()],
                        )
                        .await
                    }
                    PostgresMode::CockroachDB => {
                        static CRDB_INIT_QUERY: &str =
                            "INSERT INTO consensus SELECT $1, $2, $3 WHERE
                        NOT EXISTS (
                            SELECT * FROM consensus WHERE shard = $1
                        )";
                        let client = self.get_connection().await?;
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
            let client = self.get_connection().await?;
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
        // The `sequence_number >= 0` clause preserves the seqno `-1` marker that the first write
        // inserts. The marker is a truncation-proof "already initialized" row that a delayed first
        // write collides with (see `compare_and_set`), so it must outlive every truncation. The
        // clause is a no-op for shards that have no marker, since all of their seqnos are already
        // >= 0.
        static TRUNCATE_QUERY: &str = "
        DELETE FROM consensus
        WHERE shard = $1 AND sequence_number >= 0 AND sequence_number < $2 AND
        EXISTS (
            SELECT * FROM consensus WHERE shard = $1 AND sequence_number >= $2
        )
        ";

        let result = {
            let client = self.get_connection().await?;
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
            updates.apply(&read_committed_config.dyncfg);
        }
        PostgresConsensus::open(read_committed_config.clone())
            .await?
            .drop_and_recreate()
            .await?;
        consensus_impl_test(|| PostgresConsensus::open(read_committed_config.clone())).await?;

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
