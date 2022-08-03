// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by Postgres.

use anyhow::{anyhow, bail};
use async_trait::async_trait;
use bytes::Bytes;
use deadpool_postgres::tokio_postgres::config::SslMode;
use deadpool_postgres::tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use deadpool_postgres::tokio_postgres::Config;
use deadpool_postgres::{
    Hook, HookError, HookErrorCause, ManagerConfig, Object, PoolError, RecyclingMethod,
};
use deadpool_postgres::{Manager, Pool};
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use postgres_openssl::MakeTlsConnector;
use std::time::Instant;
use tracing::debug;

use crate::error::Error;
use crate::location::{Consensus, ExternalError, SeqNo, VersionedData};
use crate::metrics::PostgresConsensusMetrics;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS consensus (
    shard text NOT NULL,
    sequence_number bigint NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY(shard, sequence_number)
);
";

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
    url: String,
    connection_pool_max_size: usize,
    metrics: PostgresConsensusMetrics,
}

impl PostgresConsensusConfig {
    const EXTERNAL_TESTS_POSTGRES_URL: &'static str =
        "MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL";

    /// Returns a new [PostgresConsensusConfig] for use in production.
    pub async fn new(
        url: &str,
        connection_pool_max_size: usize,
        metrics: PostgresConsensusMetrics,
    ) -> Result<Self, Error> {
        Ok(PostgresConsensusConfig {
            url: url.to_string(),
            connection_pool_max_size,
            metrics,
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
    pub async fn new_for_test() -> Result<Option<Self>, Error> {
        let url = match std::env::var(Self::EXTERNAL_TESTS_POSTGRES_URL) {
            Ok(url) => url,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        let config = PostgresConsensusConfig::new(
            &url,
            2,
            PostgresConsensusMetrics::new(&MetricsRegistry::new()),
        )
        .await?;
        Ok(Some(config))
    }
}

/// Implementation of [Consensus] over a Postgres database.
pub struct PostgresConsensus {
    pool: Pool,
    metrics: PostgresConsensusMetrics,
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
        let pg_config: Config = config.url.parse()?;
        let tls = make_tls(&pg_config)?;

        let manager = Manager::from_config(
            pg_config,
            tls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );

        let pool = Pool::builder(manager)
            .max_size(config.connection_pool_max_size)
            .post_create(Hook::async_fn(|client, _| {
                Box::pin(async move {
                    debug!("opened new consensus postgres connection");
                    client.batch_execute(
                        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                    ).await.map_err(|e| HookError::Abort(HookErrorCause::Backend(e)))
                })
            }))
            .build()
            .expect("postgres connection pool built with incorrect parameters");

        let mut client = pool.get().await?;

        let tx = client.transaction().await?;
        let version: String = tx.query_one("SELECT version()", &[]).await?.get(0);
        // Only get the advisory lock on Postgres (but not Cockroach which doesn't
        // support this function and (we suspect) doesn't have this bug anyway). Use
        // this construction (not cockroach instead of yes postgres) to avoid
        // accidentally not executing in Postgres.
        let is_cockroach = version.starts_with("CockroachDB");
        if !is_cockroach {
            // Obtain an advisory lock before attempting to create the schema. This is
            // necessary to work around concurrency bugs in `CREATE TABLE IF NOT EXISTS`
            // in PostgreSQL.
            //
            // See: https://github.com/MaterializeInc/materialize/issues/12560
            // See: https://www.postgresql.org/message-id/CA%2BTgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg%40mail.gmail.com
            // See: https://stackoverflow.com/a/29908840
            //
            // The lock ID was randomly generated.
            tx.batch_execute("SELECT pg_advisory_xact_lock(135664303235462630);")
                .await?;
        }

        tx.batch_execute(SCHEMA).await?;

        if is_cockroach {
            // The `consensus` table creates and deletes rows at a high frequency, generating many
            // tombstoned rows. If Cockroach's GC interval is set high (the default is 25h) and
            // these tombstones accumulate, scanning over the table will take increasingly and
            // prohibitively long.
            //
            // See: https://github.com/MaterializeInc/materialize/issues/13975
            // See: https://www.cockroachlabs.com/docs/stable/configure-zone.html#variables
            tx.batch_execute("ALTER TABLE consensus CONFIGURE ZONE USING gc.ttlseconds = 600;")
                .await?;
        }

        tx.commit().await?;
        Ok(PostgresConsensus {
            pool,
            metrics: config.metrics,
        })
    }

    /// Drops and recreates the `consensus` table in Postgres
    ///
    /// ONLY FOR TESTING
    pub async fn drop_and_recreate(&self) -> Result<(), ExternalError> {
        // this could be a TRUNCATE if we're confident the db won't reuse any state
        let client = self.get_connection().await?;
        client.execute("DROP TABLE consensus", &[]).await?;
        client.execute(SCHEMA, &[]).await?;
        Ok(())
    }

    async fn get_connection(&self) -> Result<Object, PoolError> {
        let start = Instant::now();
        let res = self.pool.get().await;
        self.metrics
            .connpool_acquire_seconds
            .inc_by(start.elapsed().as_secs_f64());
        self.metrics.connpool_acquires.inc();
        // note that getting the pool size here requires briefly locking the pool
        self.metrics
            .connpool_size
            .set(u64::cast_from(self.pool.status().size));
        res
    }
}

// This function is copied from mz-postgres-util because of a cyclic dependency
// difficulty that we don't want to deal with now.
// TODO: Untangle that and remove this copy.
fn make_tls(config: &Config) -> Result<MakeTlsConnector, anyhow::Error> {
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    // The mode dictates whether we verify peer certs and hostnames. By default, Postgres is
    // pretty relaxed and recommends SslMode::VerifyCa or SslMode::VerifyFull for security.
    //
    // For more details, check out Table 33.1. SSL Mode Descriptions in
    // https://postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION.
    let (verify_mode, verify_hostname) = match config.get_ssl_mode() {
        SslMode::Disable | SslMode::Prefer => (SslVerifyMode::NONE, false),
        SslMode::Require => match config.get_ssl_root_cert() {
            // If a root CA file exists, the behavior of sslmode=require will be the same as
            // that of verify-ca, meaning the server certificate is validated against the CA.
            //
            // For more details, check out the note about backwards compatibility in
            // https://postgresql.org/docs/current/libpq-ssl.html#LIBQ-SSL-CERTIFICATES.
            Some(_) => (SslVerifyMode::PEER, false),
            None => (SslVerifyMode::NONE, false),
        },
        SslMode::VerifyCa => (SslVerifyMode::PEER, false),
        SslMode::VerifyFull => (SslVerifyMode::PEER, true),
        _ => panic!("unexpected sslmode {:?}", config.get_ssl_mode()),
    };

    // Configure peer verification
    builder.set_verify(verify_mode);

    // Configure certificates
    match (config.get_ssl_cert(), config.get_ssl_key()) {
        (Some(ssl_cert), Some(ssl_key)) => {
            builder.set_certificate(&*X509::from_pem(ssl_cert)?)?;
            builder.set_private_key(&*PKey::private_key_from_pem(ssl_key)?)?;
        }
        (None, Some(_)) => bail!("must provide both sslcert and sslkey, but only provided sslkey"),
        (Some(_), None) => bail!("must provide both sslcert and sslkey, but only provided sslcert"),
        _ => {}
    }
    if let Some(ssl_root_cert) = config.get_ssl_root_cert() {
        builder
            .cert_store_mut()
            .add_cert(X509::from_pem(ssl_root_cert)?)?;
    }

    let mut tls_connector = MakeTlsConnector::new(builder.build());

    // Configure hostname verification
    match (verify_mode, verify_hostname) {
        (SslVerifyMode::PEER, false) => tls_connector.set_callback(|connect, _| {
            connect.set_verify_hostname(false);
            Ok(())
        }),
        _ => {}
    }

    Ok(tls_connector)
}

#[async_trait]
impl Consensus for PostgresConsensus {
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
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(Error::from(
                        format!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, expected)).into());
            }
        }

        let result = if let Some(expected) = expected {
            // Only insert the new row if:
            // - sequence number expected is already present
            // - expected corresponds to the most recent sequence number
            //   i.e. there is no other sequence number > expected already
            //   present.
            //
            // This query has also been written to execute within a single
            // network round-trip (instead of a slightly simpler implementation
            // that would call `BEGIN` and have multiple `SELECT` queries).
            let q = "INSERT INTO consensus SELECT $1, $2, $3 WHERE
                     EXISTS (
                        SELECT * FROM consensus WHERE shard = $1 AND sequence_number = $4
                     )
                     AND NOT EXISTS (
                         SELECT * FROM consensus WHERE shard = $1 AND sequence_number > $4
                     )
                     ON CONFLICT DO NOTHING";
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
            Ok(Ok(()))
        } else {
            // It's safe to call head in a subsequent transaction rather than doing
            // so directly in the same transaction because, once a given (seqno, data)
            // pair exists for our shard, we enforce the invariants that
            // 1. Our shard will always have _some_ data mapped to it.
            // 2. All operations that modify the (seqno, data) can only increase
            //    the sequence number.
            let current = self.head(key).await?;
            Ok(Err(current))
        }
    }

    async fn scan(&self, key: &str, from: SeqNo) -> Result<Vec<VersionedData>, ExternalError> {
        let q = "SELECT sequence_number, data FROM consensus
             WHERE shard = $1 AND sequence_number >= $2
             ORDER BY sequence_number";
        let rows = {
            let client = self.get_connection().await?;
            let statement = client.prepare_cached(q).await?;
            client.query(&statement, &[&key, &from]).await?
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

        if results.is_empty() {
            Err(ExternalError::from(anyhow!(
                "sequence number lower bound too high for scan: {:?}",
                from
            )))
        } else {
            Ok(results)
        }
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<usize, ExternalError> {
        let q = "DELETE FROM consensus
                WHERE shard = $1 AND sequence_number < $2 AND
                EXISTS(
                    SELECT * FROM consensus WHERE shard = $1 AND sequence_number >= $2
                )";

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
    use crate::location::tests::consensus_impl_test;
    use tracing::info;
    use uuid::Uuid;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn postgres_consensus() -> Result<(), ExternalError> {
        let config = match PostgresConsensusConfig::new_for_test().await? {
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
            Ok(Ok(()))
        );

        assert_eq!(consensus.head(&key).await, Ok(Some(state.clone())));

        consensus.drop_and_recreate().await?;

        assert_eq!(consensus.head(&key).await, Ok(None));

        Ok(())
    }
}
