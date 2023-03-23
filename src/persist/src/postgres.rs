// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by Postgres.

use crate::cfg::ConsensusKnobs;
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
use mz_ore::now::SYSTEM_TIME;
use openssl::pkey::PKey;
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use openssl::x509::X509;
use postgres_openssl::MakeTlsConnector;
use std::fmt::Formatter;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::error::Error;
use crate::location::{CaSResult, Consensus, ExternalError, SeqNo, VersionedData};
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
    knobs: Arc<dyn ConsensusKnobs>,
    metrics: PostgresConsensusMetrics,
}

impl PostgresConsensusConfig {
    const EXTERNAL_TESTS_POSTGRES_URL: &'static str =
        "MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL";

    /// Returns a new [PostgresConsensusConfig] for use in production.
    pub fn new(
        url: &str,
        knobs: Box<dyn ConsensusKnobs>,
        metrics: PostgresConsensusMetrics,
    ) -> Result<Self, Error> {
        Ok(PostgresConsensusConfig {
            url: url.to_string(),
            knobs: Arc::from(knobs),
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
    pub fn new_for_test() -> Result<Option<Self>, Error> {
        let url = match std::env::var(Self::EXTERNAL_TESTS_POSTGRES_URL) {
            Ok(url) => url,
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
        impl ConsensusKnobs for TestConsensusKnobs {
            fn connection_pool_max_size(&self) -> usize {
                2
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

            fn cas_batch_interval(&self) -> Duration {
                Duration::from_millis(25)
            }

            fn cas_max_batch_size(&self) -> usize {
                0
            }
        }

        let config = PostgresConsensusConfig::new(
            &url,
            Box::new(TestConsensusKnobs),
            PostgresConsensusMetrics::new(&MetricsRegistry::new()),
        )?;
        Ok(Some(config))
    }
}

/// Implementation of [Consensus] over a Postgres database.
#[derive(Clone)]
pub struct PostgresConsensus {
    pool: Pool,
    config: PostgresConsensusConfig,
    metrics: PostgresConsensusMetrics,
    buf: Arc<Mutex<Vec<CaSEntry>>>,
    last_flush_timestamp: Arc<AtomicU64>,
}

struct CaSEntry {
    key: String,
    expected: SeqNo,
    new: VersionedData,
    tx: tokio::sync::oneshot::Sender<bool>,
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
        let mut pg_config: Config = config.url.parse()?;
        pg_config.connect_timeout(config.knobs.connect_timeout());
        let tls = make_tls(&pg_config)?;

        let manager = Manager::from_config(
            pg_config,
            tls,
            ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
            },
        );

        let last_ttl_connection = AtomicU64::new(0);
        let connections_created = config.metrics.connpool_connections_created.clone();
        let ttl_reconnections = config.metrics.connpool_ttl_reconnections.clone();
        let knobs = Arc::clone(&config.knobs);
        let pool = Pool::builder(manager)
            .max_size(config.knobs.connection_pool_max_size())
            .post_create(Hook::async_fn(move |client, _| {
                connections_created.inc();
                Box::pin(async move {
                    debug!("opened new consensus postgres connection");
                    client.batch_execute(
                        "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE",
                    ).await.map_err(|e| HookError::Abort(HookErrorCause::Backend(e)))
                })
            }))
            .pre_recycle(Hook::sync_fn(move |_client, conn_metrics| {
                // proactively TTL connections to rebalance load to Postgres/CRDB. this helps
                // fix skew when downstream DB operations (e.g. CRDB rolling restart) result
                // in uneven load to each node, and works to reduce the # of connections
                // maintained by the pool after bursty workloads.

                // add a bias towards TTLing older connections first
                if conn_metrics.age() < knobs.connection_pool_ttl() {
                    return Ok(());
                }

                let last_ttl = last_ttl_connection.load(Ordering::SeqCst);
                let now = (SYSTEM_TIME)();
                let elapsed_since_last_ttl = Duration::from_millis(now.saturating_sub(last_ttl));

                // stagger out reconnections to avoid stampeding the DB
                if elapsed_since_last_ttl > knobs.connection_pool_ttl_stagger()
                    && last_ttl_connection
                        .compare_exchange_weak(last_ttl, now, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                {
                    ttl_reconnections.inc();
                    return Err(HookError::Continue(Some(HookErrorCause::Message(
                        "connection has been TTLed".to_string(),
                    ))));
                }

                Ok(())
            }))
            .build()
            .expect("postgres connection pool built with incorrect parameters");

        let client = pool.get().await?;

        // The `consensus` table creates and deletes rows at a high frequency, generating many
        // tombstoned rows. If Cockroach's GC interval is set high (the default is 25h) and
        // these tombstones accumulate, scanning over the table will take increasingly and
        // prohibitively long.
        //
        // See: https://github.com/MaterializeInc/materialize/issues/13975
        // See: https://www.cockroachlabs.com/docs/stable/configure-zone.html#variables
        client
            .batch_execute(&format!(
                "{}; {}",
                SCHEMA, "ALTER TABLE consensus CONFIGURE ZONE USING gc.ttlseconds = 600;"
            ))
            .await?;

        let pg_consensus = PostgresConsensus {
            pool,
            metrics: config.metrics.clone(),
            config,
            buf: Arc::new(Mutex::new(Vec::new())),
            last_flush_timestamp: Arc::new((SYSTEM_TIME)().into()),
        };
        pg_consensus.start_task();

        Ok(pg_consensus)
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
        if let Err(PoolError::Backend(err)) = &res {
            debug!("error establishing connection: {}", err);
            self.metrics.connpool_connection_errors.inc();
        }
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

impl PostgresConsensus {
    fn start_task(&self) {
        let s = self.clone();
        let _ = mz_ore::task::spawn(|| format!("postgres batcher"), async move {
            let mut interval = tokio::time::interval(s.config.knobs.cas_batch_interval());
            loop {
                interval.tick().await;
                s.execute_batch_cas().await;
                interval = tokio::time::interval(s.config.knobs.cas_batch_interval());
            }
        });
    }

    async fn execute_batch_cas(&self) {
        let batch = {
            let mut buf = self.buf.lock().expect("abc");
            self.last_flush_timestamp
                .store((SYSTEM_TIME)().into(), Ordering::SeqCst);
            std::mem::take(&mut *buf)
        };

        if batch.is_empty() {
            return;
        }

        info!("Batch size: {}", batch.len());

        let mut param_count = 1;
        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();

        let mut cas_batch_values = vec![];
        let mut seqno_queries = vec![];
        let mut slices = vec![];
        for cas in &batch {
            slices.push(cas.new.data.as_ref());
        }
        let slices = slices;

        for (i, cas) in batch.iter().enumerate() {
            cas_batch_values.push(format!(
                "(${}::text, ${}::bigint, ${}::bigint, ${}::bytea)",
                param_count,
                param_count + 1,
                param_count + 2,
                param_count + 3
            ));
            seqno_queries.push(format!("(SELECT shard, sequence_number FROM consensus.consensus WHERE shard = ${param_count} ORDER BY sequence_number DESC LIMIT 1)"));
            params.push(&cas.key);
            params.push(&cas.expected);
            params.push(&cas.new.seqno);
            params.push(&slices[i]);
            param_count += 4;
        }

        let q = format!(
            r#"
            WITH cas_batch (shard, expected_sequence_number, new_sequence_number, data) AS (
               VALUES {}
            ),
            shard_seqnos (shard, sequence_number) AS (
              {}
            )
            INSERT INTO consensus.consensus (shard, sequence_number, data)
            SELECT cas_batch.shard, cas_batch.new_sequence_number, cas_batch.data
            FROM cas_batch, shard_seqnos
            WHERE cas_batch.shard = shard_seqnos.shard AND cas_batch.expected_sequence_number = shard_seqnos.sequence_number
            ON CONFLICT (shard, sequence_number) DO NOTHING
            RETURNING shard, sequence_number;
        "#,
            cas_batch_values.join(","),
            seqno_queries.join(" UNION "),
        );

        info!("Query: {}, params: {:?}", q, params);

        let client = self.get_connection().await.expect("conn");
        let statement = client.prepare_cached(&q).await.expect("statement");
        let rows = client
            .query(&statement, params.as_slice())
            .await
            .expect("rows");
        let mut successful_cas = vec![];
        for row in rows {
            let shard: String = row.try_get("shard").expect("shahd");
            let seqno: SeqNo = row.try_get("sequence_number").expect("seqno");
            successful_cas.push((shard, seqno));
        }

        for entry in batch {
            if successful_cas.contains(&(entry.key.clone(), entry.new.seqno.clone())) {
                let _ = entry.tx.send(true);
            } else {
                let _ = entry.tx.send(false);
            }
        }
    }

    async fn batch_compare_and_set(
        &self,
        key: String,
        expected: SeqNo,
        new: VersionedData,
        tx: tokio::sync::oneshot::Sender<bool>,
    ) {
        let should_execute = {
            let mut buf = self.buf.lock().expect("abc");

            for existing_entry in buf.iter_mut() {
                if &existing_entry.key == &key {
                    // our CaS is greater than a pending request, we can replace the
                    // pending request because it must be out-of-date at this point
                    if &existing_entry.expected < &expected {
                        let new = CaSEntry {
                            key,
                            expected,
                            new,
                            tx,
                        };
                        let old = std::mem::replace(existing_entry, new);
                        let _ = old.tx.send(false);
                    } else {
                        // otherwise, someone beat us to add a pending CaS request, send
                        // an early-return to our caller without needing to ping CRDB
                        let _ = tx.send(false);
                    }
                    return;
                }
            }

            buf.push(CaSEntry {
                key,
                expected,
                new,
                tx,
            });

            buf.len() >= self.config.knobs.cas_max_batch_size()
        };

        if should_execute {
            self.execute_batch_cas().await;
        }
    }
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
    ) -> Result<CaSResult, ExternalError> {
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(Error::from(
                        format!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, expected)).into());
            }
        }

        let result = if let Some(expected) = expected {
            // This query has been written to execute within a single
            // network round-trip. The insert performance has been tuned
            // against CockroachDB, ensuring it goes through the fast-path
            // 1-phase commit of CRDB. Any changes to this query should
            // confirm an EXPLAIN ANALYZE (VERBOSE) query plan contains
            // `auto commit`
            if self.config.knobs.cas_max_batch_size() == 0 {
                let q = r#"
                    INSERT INTO consensus (shard, sequence_number, data)
                    SELECT $1, $2, $3
                    WHERE (SELECT sequence_number FROM consensus
                           WHERE shard = $1
                           ORDER BY sequence_number DESC LIMIT 1) = $4;
                "#;
                let client = self.get_connection().await?;
                let statement = client.prepare_cached(q).await?;
                client
                    .execute(
                        &statement,
                        &[&key, &new.seqno, &new.data.as_ref(), &expected],
                    )
                    .await?
            } else {
                let (oneshot_tx, oneshot_rx) = tokio::sync::oneshot::channel();
                self.batch_compare_and_set(key.to_string(), expected, new, oneshot_tx)
                    .await;

                match oneshot_rx.await {
                    Ok(res) => {
                        if res {
                            1
                        } else {
                            0
                        }
                    }
                    Err(err) => {
                        warn!("for some reason the rx channel errored: {:?}", err);
                        0
                    }
                }
            }
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
            info!("({}, {:?}) passed CaS", key, expected);
            Ok(CaSResult::Committed)
        } else {
            info!("({}, {:?}) failed CaS", key, expected);
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
                    "limit must be [0, i64::MAX]. was: {:?}", limit
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
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
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

        Ok(())
    }
}
