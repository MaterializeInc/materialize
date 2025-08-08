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
use deadpool_postgres::tokio_postgres::Config;
use deadpool_postgres::tokio_postgres::types::{FromSql, IsNull, ToSql, Type, to_sql_checked};
use deadpool_postgres::{Object, PoolError};
use futures_util::StreamExt;
use mz_dyncfg::ConfigSet;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::url::SensitiveUrl;
use mz_postgres_client::metrics::PostgresClientMetrics;
use mz_postgres_client::{PostgresClient, PostgresClientConfig, PostgresClientKnobs};
use postgres_protocol::escape::escape_identifier;
use tokio_postgres::error::SqlState;
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
        let role = pg_config.get_user().unwrap();
        let create_schema = format!(
            "CREATE SCHEMA IF NOT EXISTS consensus AUTHORIZATION {}",
            escape_identifier(role),
        );

        let dyncfg = Arc::clone(&config.dyncfg);
        let postgres_client = PostgresClient::open(config.into())?;

        let client = postgres_client.get_connection().await?;

        let mode = match client
            .batch_execute(&format!(
                "{}; {}{}; {};",
                create_schema, SCHEMA, CRDB_SCHEMA_OPTIONS, CRDB_CONFIGURE_ZONE,
            ))
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
            client
                .batch_execute(&format!("{}; {};", create_schema, SCHEMA))
                .await?;
        }

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
        client.execute("DROP TABLE consensus", &[]).await?;
        let crdb_mode = match client
            .batch_execute(&format!(
                "{}{}; {}",
                SCHEMA, CRDB_SCHEMA_OPTIONS, CRDB_CONFIGURE_ZONE,
            ))
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
            client.execute(SCHEMA, &[]).await?;
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
            client.query_opt(&statement, &[&key]).await?
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
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(Error::from(
                        format!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, expected)).into());
            }
        }

        let result = if let Some(expected) = expected {
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

            /// This query has been written to ensure we only get row level
            /// locks on the `(shard, seq_no)` we're trying to update. The insert
            /// performance has been tuned against Postgres 15 to ensure it
            /// minimizes possible serialization conflicts.
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
            WHERE last_seq.sequence_number = $4;
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
            client
                .execute(
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
            client
                .execute(&statement, &[&key, &new.seqno, &new.data.as_ref()])
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
            client.query(&statement, &[&key, &from, &limit]).await?
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

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
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
            client.execute(&statement, &[&key, &seqno]).await?
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

        Ok(usize::cast_from(result))
    }
}

#[cfg(test)]
mod tests {
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

        // and now verify the implementation-specific `drop_and_recreate` works as intended
        let consensus = PostgresConsensus::open(config.clone()).await?;
        let key = Uuid::new_v4().to_string();
        let state = VersionedData {
            seqno: SeqNo(5),
            data: Bytes::from("abc"),
        };

        assert_eq!(
            consensus.compare_and_set(&key, None, state.clone()).await,
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
