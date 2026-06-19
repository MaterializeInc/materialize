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
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::tokio_postgres::Config;
use deadpool_postgres::tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use deadpool_postgres::{Object, PoolError};
use futures_util::StreamExt;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::url::SensitiveUrl;
use mz_postgres_client::metrics::PostgresClientMetrics;
use mz_postgres_client::{
    PostgresClient, PostgresClientConfig, PostgresClientKnobs, TransactionIsolationLevel,
};
use postgres_protocol::escape::escape_identifier;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Row, Statement};
use tracing::{info, warn};

use crate::error::Error;
use crate::location::{CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData};

/// Flag to use concensus queries that are tuned for vanilla Postgres.
pub const USE_POSTGRES_TUNED_QUERIES: mz_dyncfg::Config<bool> = mz_dyncfg::Config::new(
    "persist_use_postgres_tuned_queries",
    false,
    "Use a set of queries for consensus that have specifically been tuned against
    Postgres to ensure we acquire a minimal number of locks.",
);

/// Whether the Postgres consensus connections use SERIALIZABLE transaction isolation.
///
/// `true` (the default) preserves historical behavior. Setting it `false` switches the consensus
/// connections to READ COMMITTED, which eliminates the SSI predicate-lock contention that can
/// collapse the consensus store under load. The consensus operations are single-statement and
/// arbitrated by the `(shard, sequence_number)` PRIMARY KEY, so READ COMMITTED is correctness-
/// preserving (analyzed and linearizability-tested for incident 1036).
///
/// Only affects **vanilla Postgres** consensus; CockroachDB connections always use SERIALIZABLE
/// regardless of this flag (the downgrade is applied only after the backend is detected as
/// Postgres).
pub const CONSENSUS_SERIALIZABLE_ISOLATION: mz_dyncfg::Config<bool> = mz_dyncfg::Config::new(
    "persist_consensus_serializable_isolation",
    true,
    "If true, use SERIALIZABLE isolation for Postgres consensus connections; if false, use \
     READ COMMITTED. Postgres only — CockroachDB always uses SERIALIZABLE.",
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
async fn pg_batch_execute(client: &Object, query: &str) -> Result<(), tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.batch_execute(query).await
}

async fn pg_query_prepared(
    client: &Object,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.query(statement, params).await
}

async fn pg_query_opt_prepared(
    client: &Object,
    statement: &Statement,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Option<Row>, tokio_postgres::Error> {
    #[allow(clippy::disallowed_methods)]
    client.query_opt(statement, params).await
}

async fn pg_execute_prepared(
    client: &Object,
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

impl From<PostgresConsensusConfig> for PostgresClientConfig {
    fn from(config: PostgresConsensusConfig) -> Self {
        PostgresClientConfig::new(config.url, config.knobs, config.metrics)
    }
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
        }

        let dyncfg = ConfigSet::default().add(&USE_POSTGRES_TUNED_QUERIES);
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
    dyncfg: Arc<ConfigSet>,
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

        // The backend (Postgres vs CockroachDB) is only known after the probe below, but the
        // connection pool — and its per-connection isolation `SET` — is built now. Share a cell
        // that the resolver reads: until the mode is known (the probe connections), and for
        // CockroachDB always, connections use SERIALIZABLE. Only once detected as Postgres does the
        // `persist_consensus_serializable_isolation=false` flag downgrade new connections to READ
        // COMMITTED. (Early probe/setup connections stay SERIALIZABLE until the pool recycles them.)
        let mode_cell: Arc<OnceLock<PostgresMode>> = Arc::new(OnceLock::new());
        let client_config = {
            let dyncfg = Arc::clone(&dyncfg);
            let mode_cell = Arc::clone(&mode_cell);
            PostgresClientConfig::from(config).with_isolation_level(Arc::new(move || {
                match mode_cell.get() {
                    Some(PostgresMode::Postgres)
                        if !CONSENSUS_SERIALIZABLE_ISOLATION.get(&dyncfg) =>
                    {
                        TransactionIsolationLevel::ReadCommitted
                    }
                    _ => TransactionIsolationLevel::Serializable,
                }
            }))
        };
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

        if mode != PostgresMode::CockroachDB {
            pg_batch_execute(&client, &format!("{}; {};", create_schema, SCHEMA)).await?;
        }

        // Publish the detected mode so the pool's isolation resolver can (for Postgres only) honor
        // the READ COMMITTED flag on subsequently-created connections.
        let _ = mode_cell.set(mode);

        Ok(PostgresConsensus {
            postgres_client,
            dyncfg,
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

    async fn get_connection(&self) -> Result<Object, PoolError> {
        self.postgres_client.get_connection().await
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

        let result = if let Some(expected) = expected {
            /// This query has been written to execute within a single
            /// network round-trip. The insert performance has been tuned
            /// against CockroachDB, ensuring it goes through the fast-path
            /// 1-phase commit of CRDB. Any changes to this query should
            /// confirm an EXPLAIN ANALYZE (VERBOSE) query plan contains
            /// `auto commit`
            ///
            /// `ON CONFLICT DO NOTHING`: under READ COMMITTED two racing
            /// appenders with the same `expected` can both pass the WHERE
            /// guard (each sees the other's uncommitted insert as invisible);
            /// the PRIMARY KEY (shard, sequence_number) then admits exactly
            /// one. Without this clause the loser surfaces a `23505`
            /// unique_violation (mapped to Indeterminate); with it the loser
            /// inserts 0 rows, which we report as `ExpectationMismatch` —
            /// matching the MemConsensus reference semantics. Harmless under
            /// SERIALIZABLE (SSI aborts the loser with 40001 first). (CRDB
            /// note: confirm this still hits the 1PC fast path before shipping
            /// to a CRDB deployment; CRDB stays SERIALIZABLE regardless.)
            static CRDB_CAS_QUERY: &str = "
                INSERT INTO consensus (shard, sequence_number, data)
                SELECT $1, $2, $3
                WHERE (SELECT sequence_number FROM consensus
                       WHERE shard = $1
                       ORDER BY sequence_number DESC LIMIT 1) = $4
                ON CONFLICT DO NOTHING;
            ";

            /// This query has been written to ensure we only get row level
            /// locks on the `(shard, seq_no)` we're trying to update. The insert
            /// performance has been tuned against Postgres 15 to ensure it
            /// minimizes possible serialization conflicts.
            ///
            /// See `CRDB_CAS_QUERY` for why `ON CONFLICT DO NOTHING` is needed
            /// under READ COMMITTED: the `FOR UPDATE` lock is taken on the
            /// current head row, which does not prevent two appenders from both
            /// trying to insert the same next sequence number.
            static POSTGRES_CAS_QUERY: &str = "
            WITH last_seq AS (
                SELECT sequence_number FROM consensus
                WHERE shard = $1
                ORDER BY sequence_number DESC
                LIMIT 1
                FOR UPDATE
            )
            INSERT INTO consensus (shard, sequence_number, data)
            SELECT $1, $2, $3
            FROM last_seq
            WHERE last_seq.sequence_number = $4
            ON CONFLICT DO NOTHING;
            ";

            let q = if USE_POSTGRES_TUNED_QUERIES.get(&self.dyncfg)
                && self.mode == PostgresMode::Postgres
            {
                POSTGRES_CAS_QUERY
            } else {
                CRDB_CAS_QUERY
            };
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(q).await?;
            pg_execute_prepared(
                &client,
                &statement,
                &[&key, &new.seqno, &new.data.as_ref(), &expected],
            )
            .await?
        } else {
            // Insert the new row as long as no other row exists for the same shard.
            let q = "INSERT INTO consensus SELECT $1, $2, $3 WHERE
                     NOT EXISTS (
                         SELECT * FROM consensus WHERE shard = $1
                     )
                     ON CONFLICT DO NOTHING";
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(q).await?;
            pg_execute_prepared(&client, &statement, &[&key, &new.seqno, &new.data.as_ref()])
                .await?
        };

        if result == 1 {
            Ok(CaSResult::Committed)
        } else {
            Ok(CaSResult::ExpectationMismatch)
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
        static CRDB_TRUNCATE_QUERY: &str = "
        DELETE FROM consensus
        WHERE shard = $1 AND sequence_number < $2 AND
        EXISTS (
            SELECT * FROM consensus WHERE shard = $1 AND sequence_number >= $2
        )
        ";

        /// This query has been specifically tuned to ensure we get the minimal
        /// number of __row__ locks possible, and that it doesn't conflict with
        /// concurrently running compare and swap operations that are trying to
        /// evolve the shard.
        ///
        /// It's performance has been benchmarked against Postgres 15.
        ///
        /// Note: The `ORDER BY` in the newer_exists CTE exists so we obtain a
        /// row lock on the lowest possible sequence number. This ensures
        /// minimal conflict between concurrently running truncate and append
        /// operations.
        static POSTGRES_TRUNCATE_QUERY: &str = "
        WITH newer_exists AS (
            SELECT * FROM consensus
            WHERE shard = $1
                AND sequence_number >= $2
            ORDER BY sequence_number ASC
            LIMIT 1
            FOR UPDATE
        ),
        to_lock AS (
            SELECT ctid FROM consensus
            WHERE shard = $1
            AND sequence_number < $2
            AND EXISTS (SELECT * FROM newer_exists)
            ORDER BY sequence_number DESC
            FOR UPDATE
        )
        DELETE FROM consensus
        USING to_lock
        WHERE consensus.ctid = to_lock.ctid;
        ";

        let q = if USE_POSTGRES_TUNED_QUERIES.get(&self.dyncfg)
            && self.mode == PostgresMode::Postgres
        {
            POSTGRES_TRUNCATE_QUERY
        } else {
            CRDB_TRUNCATE_QUERY
        };
        let result = {
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(q).await?;
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
    use std::collections::BTreeMap;
    use std::sync::Mutex;
    use std::time::Instant;

    use mz_ore::assert_err;
    use tracing::info;
    use uuid::Uuid;

    use crate::location::SCAN_ALL;
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

    // ----------------------------------------------------------------------
    // Linearizability verification for the consensus log under a configurable
    // Postgres isolation level (incident-1036: can we run READ COMMITTED?).
    //
    // The consensus log is linearizable iff: (1) each seqno is committed by at
    // most one compare_and_set ("CaS safety" — no two writers both win the same
    // slot); (2) the committed chain is gapless 0..=H; (3) a scan reflects
    // exactly that chain with each slot holding the *winning* writer's data (no
    // lost update / torn write). These must hold under SERIALIZABLE (trivially)
    // AND under READ COMMITTED (the thing we're verifying). The negative-control
    // test below proves this checker actually detects a non-linearizable store.
    // ----------------------------------------------------------------------

    /// A 16-byte payload uniquely identifying (worker, attempt), so a slot's
    /// stored bytes reveal exactly which CaS call wrote it.
    fn payload(worker: u64, counter: u64) -> Bytes {
        let mut v = Vec::with_capacity(16);
        v.extend_from_slice(&worker.to_be_bytes());
        v.extend_from_slice(&counter.to_be_bytes());
        Bytes::from(v)
    }

    /// Spawn `workers` tasks that race to advance one shard's seqno chain by
    /// `head` + `compare_and_set` for `duration`. Returns every (seqno, data)
    /// pair that *some* worker was told it `Committed` (including the initial
    /// seqno-0 append), the count of Determinate (40001) and Indeterminate
    /// errors observed, and the final scan of the chain.
    async fn run_cas_race(
        consensus: Arc<dyn Consensus>,
        shard: String,
        workers: u64,
        duration: Duration,
    ) -> (Vec<(u64, Bytes)>, u64, u64, Vec<VersionedData>) {
        let init = VersionedData {
            seqno: SeqNo(0),
            data: payload(u64::MAX, 0),
        };
        assert_eq!(
            consensus.compare_and_set(&shard, init.clone()).await.unwrap(),
            CaSResult::Committed,
            "initial append should commit"
        );

        let deadline = Instant::now() + duration;
        let mut handles = Vec::new();
        for w in 0..workers {
            let consensus = Arc::clone(&consensus);
            let shard = shard.clone();
            handles.push(mz_ore::task::spawn(|| format!("cas-race-{w}"), async move {
                let mut claims: Vec<(u64, Bytes)> = Vec::new();
                let (mut det, mut indet) = (0u64, 0u64);
                let mut counter = 0u64;
                while Instant::now() < deadline {
                    let head = match consensus.head(&shard).await {
                        Ok(Some(v)) => v.seqno,
                        Ok(None) => panic!("shard {shard} unexpectedly empty mid-race"),
                        Err(_) => continue, // transient read failure; re-read
                    };
                    counter += 1;
                    let data = payload(w, counter);
                    let new = VersionedData {
                        seqno: SeqNo(head.0 + 1),
                        data: data.clone(),
                    };
                    match consensus.compare_and_set(&shard, new).await {
                        Ok(CaSResult::Committed) => claims.push((head.0 + 1, data)),
                        Ok(CaSResult::ExpectationMismatch) => {}
                        Err(ExternalError::Determinate(_)) => det += 1,
                        Err(ExternalError::Indeterminate(_)) => indet += 1,
                    }
                }
                (claims, det, indet)
            }));
        }

        let mut all_claims = vec![(0u64, init.data.clone())];
        let (mut det, mut indet) = (0u64, 0u64);
        for h in handles {
            let (claims, d, i) = h.await;
            all_claims.extend(claims);
            det += d;
            indet += i;
        }
        let scan = consensus.scan(&shard, SeqNo(0), SCAN_ALL).await.unwrap();
        (all_claims, det, indet, scan)
    }

    /// Check the three linearizability invariants. Returns the final head seqno
    /// on success, or a description of the first violation found.
    fn check_linearizable(
        claims: &[(u64, Bytes)],
        scan: &[VersionedData],
    ) -> Result<u64, String> {
        // (1) CaS safety: no seqno claimed Committed by two CaS calls.
        let mut by_seqno: BTreeMap<u64, Bytes> = BTreeMap::new();
        for (seqno, data) in claims {
            if let Some(prev) = by_seqno.insert(*seqno, data.clone()) {
                return Err(format!(
                    "CaS SAFETY VIOLATED: seqno {seqno} was Committed by two compare_and_set \
                     calls (data {prev:?} and {data:?}) — both writers won the same slot"
                ));
            }
        }
        // (2) Gapless chain 0..=max.
        let max = *by_seqno
            .keys()
            .next_back()
            .ok_or_else(|| "no committed appends at all".to_string())?;
        for s in 0..=max {
            if !by_seqno.contains_key(&s) {
                return Err(format!("GAP: seqno {s} missing from committed chain (max={max})"));
            }
        }
        // (3) Scan reflects exactly the committed chain, each slot = winner's data.
        if scan.len() as u64 != max + 1 {
            return Err(format!(
                "scan returned {} rows, expected {} (chain 0..={max})",
                scan.len(),
                max + 1
            ));
        }
        for (i, vd) in scan.iter().enumerate() {
            let expected = i as u64;
            if vd.seqno.0 != expected {
                return Err(format!(
                    "scan not contiguous: position {i} holds seqno {}",
                    vd.seqno.0
                ));
            }
            let winner = by_seqno.get(&expected).expect("checked gapless above");
            if &vd.data != winner {
                return Err(format!(
                    "LOST UPDATE at seqno {expected}: stored {:?} but the winning CaS wrote {winner:?}",
                    vd.data
                ));
            }
        }
        Ok(max)
    }

    fn linearizability_test_config(
        pool_max_size: usize,
        serializable: bool,
    ) -> Option<PostgresConsensusConfig> {
        let url = match std::env::var(PostgresConsensusConfig::EXTERNAL_TESTS_POSTGRES_URL) {
            Ok(url) => SensitiveUrl::from_str(&url).expect("valid url"),
            Err(_) => return None,
        };
        #[derive(Debug)]
        struct Knobs(usize);
        impl PostgresClientKnobs for Knobs {
            fn connection_pool_max_size(&self) -> usize {
                self.0
            }
            fn connection_pool_max_wait(&self) -> Option<Duration> {
                Some(Duration::from_secs(30))
            }
            fn connection_pool_ttl(&self) -> Duration {
                Duration::MAX
            }
            fn connection_pool_ttl_stagger(&self) -> Duration {
                Duration::MAX
            }
            fn connect_timeout(&self) -> Duration {
                Duration::from_secs(10)
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
        }
        let dyncfg = ConfigSet::default()
            .add(&USE_POSTGRES_TUNED_QUERIES)
            .add(&CONSENSUS_SERIALIZABLE_ISOLATION);
        let mut updates = mz_dyncfg::ConfigUpdates::default();
        updates.add(&CONSENSUS_SERIALIZABLE_ISOLATION, serializable);
        updates.apply(&dyncfg);
        Some(
            PostgresConsensusConfig::new(
                &url,
                Box::new(Knobs(pool_max_size)),
                PostgresClientMetrics::new(&MetricsRegistry::new(), "mz_persist"),
                Arc::new(dyncfg),
            )
            .expect("config"),
        )
    }

    /// Directly confirm the `persist_consensus_serializable_isolation` flag drives the actual
    /// session isolation of the consensus connections (via `SHOW transaction_isolation`), in both
    /// directions. This validates the shippable flag end-to-end through the real pool + hook.
    #[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 4))]
    #[cfg_attr(miri, ignore)]
    async fn postgres_consensus_isolation_flag() -> Result<(), ExternalError> {
        for (serializable, expected) in [(true, "serializable"), (false, "read committed")] {
            let config = match linearizability_test_config(12, serializable) {
                Some(c) => c,
                None => {
                    info!(
                        "{} env not set: skipping",
                        PostgresConsensusConfig::EXTERNAL_TESTS_POSTGRES_URL
                    );
                    return Ok(());
                }
            };
            let consensus = PostgresConsensus::open(config).await?;
            // Force several fresh connections (more than the 1-2 probe connections open() makes
            // before the mode is published) and read each session's isolation directly.
            let mut conns = Vec::new();
            for _ in 0..8 {
                conns.push(consensus.get_connection().await?);
            }
            let mut seen = std::collections::BTreeSet::new();
            for c in &conns {
                let row = c.query_one("SHOW transaction_isolation", &[]).await?;
                seen.insert(row.get::<_, String>(0));
            }
            info!("flag serializable={serializable}: observed session isolations {seen:?}");
            assert!(
                seen.contains(expected),
                "flag serializable={serializable} should produce {expected:?} sessions; saw {seen:?}"
            );
            if serializable {
                // SERIALIZABLE must be the ONLY isolation seen (no accidental downgrade anywhere).
                assert_eq!(
                    seen,
                    std::collections::BTreeSet::from(["serializable".to_string()]),
                    "serializable flag must yield only serializable sessions; saw {seen:?}"
                );
            }
        }
        Ok(())
    }

    /// Hammer a single shard with many concurrent compare_and_set racers against real Postgres and
    /// assert the consensus log stayed linearizable under BOTH isolation levels (driven by the
    /// `persist_consensus_serializable_isolation` flag). The point of the exercise is that READ
    /// COMMITTED (flag=false) is just as linearizable as SERIALIZABLE, and additionally produces
    /// zero serialization failures / indeterminate errors.
    ///
    /// Filter to JUST this test when running (e.g.
    /// `cargo test -p mz-persist postgres_consensus_linearizability`): the sibling
    /// `postgres_consensus` test calls `drop_and_recreate`, which would nuke the shared `consensus`
    /// table mid-race.
    #[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
    #[cfg_attr(miri, ignore)]
    async fn postgres_consensus_linearizability() -> Result<(), ExternalError> {
        const WORKERS: u64 = 16;
        let mut ser_determinate: Option<u64> = None;
        for serializable in [true, false] {
            let config = match linearizability_test_config(usize::cast_from(WORKERS) + 4, serializable) {
                Some(config) => config,
                None => {
                    info!(
                        "{} env not set: skipping",
                        PostgresConsensusConfig::EXTERNAL_TESTS_POSTGRES_URL
                    );
                    return Ok(());
                }
            };
            let label = if serializable { "SERIALIZABLE" } else { "READ COMMITTED" };
            info!("running consensus linearizability race under isolation = {label}");

            let consensus: Arc<dyn Consensus> = Arc::new(PostgresConsensus::open(config).await?);
            let shard = Uuid::new_v4().to_string();

            let (claims, determinate, indeterminate, scan) =
                run_cas_race(Arc::clone(&consensus), shard, WORKERS, Duration::from_secs(5)).await;

            let head = check_linearizable(&claims, &scan).unwrap_or_else(|violation| {
                panic!("consensus NOT linearizable under {label}: {violation}");
            });

            // Sanity: the race must have actually exercised contention, otherwise a trivially-serial
            // run could pass vacuously.
            let committed = claims.len() as u64;
            assert!(
                committed > WORKERS * 5,
                "race too short to be meaningful under {label}: only {committed} appends"
            );
            info!(
                "linearizable under {label}: head={head} appends={committed} \
                 determinate(40001)={determinate} indeterminate={indeterminate}"
            );

            // With `ON CONFLICT DO NOTHING`, a losing racer resolves to ExpectationMismatch under
            // either isolation level, so indeterminate (e.g. a leaked 23505) must be zero always.
            assert_eq!(
                indeterminate, 0,
                "ON CONFLICT must resolve races to ExpectationMismatch, not Indeterminate errors \
                 (isolation {label})"
            );

            if serializable {
                ser_determinate = Some(determinate);
            } else {
                // READ COMMITTED itself never raises 40001. The only 40001s here come from the one
                // SERIALIZABLE probe connection open() creates before the backend mode is known,
                // which lingers in the pool under the test's infinite TTL (production's 300s TTL
                // recycles it). So RC must nearly *eliminate* the SSI serialization-failure storm
                // relative to SERIALIZABLE, not merely reduce it.
                let ser = ser_determinate.expect("serializable iteration ran first").max(1);
                assert!(
                    determinate * 4 < ser,
                    "READ COMMITTED should nearly eliminate 40001 serialization failures, but saw \
                     RC={determinate} vs SERIALIZABLE={ser}"
                );
            }
        }

        Ok(())
    }

    /// Append-at-top + truncate-at-bottom + concurrent scans. Because truncate
    /// only removes a contiguous prefix (< some seqno below head) and appends
    /// only add above head, every committed snapshot of a shard is a contiguous
    /// seqno range [lo, hi]. So EVERY scan must return strictly-+1-contiguous
    /// seqnos; a hole would betray a READ COMMITTED visibility anomaly across
    /// the truncate/append/scan interleaving. Also asserts head is monotonic.
    #[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
    #[cfg_attr(miri, ignore)]
    async fn postgres_consensus_concurrent_truncate() -> Result<(), ExternalError> {
        const APPENDERS: u64 = 12;
        // Exercise the interesting case: READ COMMITTED (flag=false).
        let config = match linearizability_test_config(usize::cast_from(APPENDERS) + 6, false) {
            Some(config) => config,
            None => {
                info!(
                    "{} env not set: skipping",
                    PostgresConsensusConfig::EXTERNAL_TESTS_POSTGRES_URL
                );
                return Ok(());
            }
        };
        let isolation = "READ COMMITTED";
        info!("running concurrent truncate test under isolation = {isolation}");
        let consensus: Arc<dyn Consensus> = Arc::new(PostgresConsensus::open(config).await?);
        let shard = Uuid::new_v4().to_string();
        consensus
            .compare_and_set(&shard, VersionedData { seqno: SeqNo(0), data: payload(0, 0) })
            .await?;

        let deadline = Instant::now() + Duration::from_secs(5);
        let mut handles = Vec::new();

        // Appenders.
        for w in 1..=APPENDERS {
            let (consensus, shard) = (Arc::clone(&consensus), shard.clone());
            handles.push(mz_ore::task::spawn(|| format!("appender-{w}"), async move {
                let mut counter = 0u64;
                while Instant::now() < deadline {
                    let Ok(Some(head)) = consensus.head(&shard).await else { continue };
                    counter += 1;
                    let new = VersionedData { seqno: SeqNo(head.seqno.0 + 1), data: payload(w, counter) };
                    let _ = consensus.compare_and_set(&shard, new).await;
                }
                0u64
            }));
        }
        // Truncator: keep ~50 versions behind head.
        {
            let (consensus, shard) = (Arc::clone(&consensus), shard.clone());
            handles.push(mz_ore::task::spawn(|| "truncator", async move {
                while Instant::now() < deadline {
                    if let Ok(Some(head)) = consensus.head(&shard).await {
                        if head.seqno.0 > 50 {
                            let _ = consensus.truncate(&shard, SeqNo(head.seqno.0 - 50)).await;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
                0u64
            }));
        }
        // Scanners: assert every scan is a contiguous seqno range, and head() is monotonic.
        for s in 0..2u64 {
            let (consensus, shard) = (Arc::clone(&consensus), shard.clone());
            handles.push(mz_ore::task::spawn(|| format!("scanner-{s}"), async move {
                let mut last_head = 0u64;
                let mut scans = 0u64;
                while Instant::now() < deadline {
                    if let Ok(Some(h)) = consensus.head(&shard).await {
                        assert!(h.seqno.0 >= last_head, "head regressed {} -> {}", last_head, h.seqno.0);
                        last_head = h.seqno.0;
                    }
                    let rows = consensus.scan(&shard, SeqNo(0), SCAN_ALL).await.expect("scan");
                    for win in rows.windows(2) {
                        assert_eq!(
                            win[1].seqno.0,
                            win[0].seqno.0 + 1,
                            "scan returned NON-CONTIGUOUS seqnos {} then {} under {} — \
                             truncate/append/scan visibility anomaly",
                            win[0].seqno.0,
                            win[1].seqno.0,
                            isolation,
                        );
                    }
                    scans += 1;
                }
                scans
            }));
        }

        let mut total_scans = 0u64;
        for h in handles {
            total_scans += h.await;
        }
        // Final state must still be a clean contiguous suffix.
        let rows = consensus.scan(&shard, SeqNo(0), SCAN_ALL).await?;
        for win in rows.windows(2) {
            assert_eq!(win[1].seqno.0, win[0].seqno.0 + 1, "final scan not contiguous");
        }
        info!(
            "concurrent truncate OK under {isolation}: final head={:?}, {} live versions, {total_scans} contiguity-checked scans",
            rows.last().map(|v| v.seqno.0),
            rows.len(),
        );
        Ok(())
    }

    // --- Negative control -------------------------------------------------

    /// A deliberately NON-linearizable consensus: read-modify-write with no
    /// atomic arbitration (no PK, no expected check) and a yield in the race
    /// window, so concurrent appenders double-claim the same seqno. Exists only
    /// to prove `check_linearizable` actually catches a broken store.
    #[derive(Debug, Default)]
    struct RacyConsensus {
        data: Mutex<BTreeMap<String, BTreeMap<u64, Bytes>>>,
    }

    #[async_trait]
    impl Consensus for RacyConsensus {
        fn list_keys(&self) -> ResultStream<'_, String> {
            let keys: Vec<_> = self
                .data
                .lock()
                .expect("lock")
                .keys()
                .cloned()
                .map(Ok)
                .collect();
            Box::pin(futures_util::stream::iter(keys))
        }

        async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
            let g = self.data.lock().expect("lock");
            Ok(g.get(key).and_then(|m| m.iter().next_back()).map(|(s, d)| {
                VersionedData {
                    seqno: SeqNo(*s),
                    data: d.clone(),
                }
            }))
        }

        async fn compare_and_set(
            &self,
            key: &str,
            new: VersionedData,
        ) -> Result<CaSResult, ExternalError> {
            // BROKEN ON PURPOSE: read head, release the lock, yield (opening the
            // race window), then blindly insert with no expected-seqno check.
            let _ = self.head(key).await?;
            tokio::task::yield_now().await;
            let mut g = self.data.lock().expect("lock");
            g.entry(key.to_string())
                .or_default()
                .insert(new.seqno.0, new.data);
            Ok(CaSResult::Committed)
        }

        async fn scan(
            &self,
            key: &str,
            from: SeqNo,
            limit: usize,
        ) -> Result<Vec<VersionedData>, ExternalError> {
            let g = self.data.lock().expect("lock");
            let out = g
                .get(key)
                .into_iter()
                .flat_map(|m| m.range(from.0..))
                .take(limit)
                .map(|(s, d)| VersionedData {
                    seqno: SeqNo(*s),
                    data: d.clone(),
                })
                .collect();
            Ok(out)
        }

        async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
            let mut g = self.data.lock().expect("lock");
            let mut n = 0;
            if let Some(m) = g.get_mut(key) {
                let keep: BTreeMap<u64, Bytes> = m.split_off(&seqno.0);
                n = m.len();
                *m = keep;
            }
            Ok(Some(n))
        }
    }

    /// Proves the linearizability checker has teeth: the same race harness run
    /// against the deliberately-broken `RacyConsensus` MUST be caught.
    #[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 8))]
    #[cfg_attr(miri, ignore)]
    async fn linearizability_checker_catches_violations() {
        let consensus: Arc<dyn Consensus> = Arc::new(RacyConsensus::default());
        let (claims, _, _, scan) = run_cas_race(
            consensus,
            "racy".to_string(),
            16,
            Duration::from_secs(2),
        )
        .await;
        let result = check_linearizable(&claims, &scan);
        assert!(
            result.is_err(),
            "negative control FAILED: checker did not catch the non-linearizable RacyConsensus \
             (got head={result:?}) — the linearizability assertions are not trustworthy"
        );
        info!("negative control OK: checker caught the violation: {}", result.unwrap_err());
    }
}
