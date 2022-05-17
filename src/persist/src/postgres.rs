// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by Postgres.

use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use tokio_postgres::{Client as PostgresClient, IsolationLevel, NoTls, Transaction};

use mz_ore::task;

use crate::error::Error;
use crate::location::{Consensus, ExternalError, SeqNo, VersionedData};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS tail (
    shard text NOT NULL,
    sequence_number bigint NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY(shard, sequence_number)
);

CREATE TABLE IF NOT EXISTS head (
    shard text NOT NULL,
    sequence_number bigint NOT NULL,
    data bytea NOT NULL,
    PRIMARY KEY(shard)
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
}

impl PostgresConsensusConfig {
    const EXTERNAL_TESTS_POSTGRES_URL: &'static str =
        "MZ_PERSIST_EXTERNAL_STORAGE_TEST_POSTGRES_URL";

    /// Returns a new [PostgresConsensusConfig] for use in production.
    pub async fn new(url: &str) -> Result<Self, Error> {
        Ok(PostgresConsensusConfig {
            url: url.to_string(),
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

        let config = PostgresConsensusConfig::new(&url).await?;
        Ok(Some(config))
    }
}

/// Implementation of [Consensus] over a Postgres database.
#[derive(Debug)]
pub struct PostgresConsensus {
    client: Arc<Mutex<PostgresClient>>,
    _handle: JoinHandle<()>,
}

impl PostgresConsensus {
    /// Open a Postgres [Consensus] instance with `config`, for the collection
    /// named `shard`.
    pub async fn open(config: PostgresConsensusConfig) -> Result<Self, ExternalError> {
        // TODO: reconsider opening with NoTLS. Perhaps we actually want to,
        // especially given the fact that its not entirely known what data
        // will actually be stored in Consensus.
        let (mut client, conn) = tokio_postgres::connect(&config.url, NoTls).await?;
        let handle = task::spawn(|| "pg_consensus_client", async move {
            if let Err(e) = conn.await {
                tracing::error!("connection error: {}", e);
                return;
            }
        });

        let tx = client.transaction().await?;
        tx.batch_execute(SCHEMA).await?;
        tx.commit().await?;
        Ok(PostgresConsensus {
            client: Arc::new(Mutex::new(client)),
            _handle: handle,
        })
    }

    async fn tx<'a>(
        &self,
        client: &'a mut PostgresClient,
    ) -> Result<Transaction<'a>, ExternalError> {
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::Serializable)
            .start()
            .await?;
        Ok(tx)
    }

    async fn head_tx(
        &self,
        tx: &Transaction<'_>,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError> {
        let q = "SELECT sequence_number, data FROM head WHERE shard = $1";

        let row = match tx.query_opt(q, &[&key]).await? {
            Some(row) => row,
            None => return Ok(None),
        };

        let seqno: SeqNo = row.try_get("sequence_number")?;
        let data: Vec<u8> = row.try_get("data")?;
        Ok(Some(VersionedData { seqno, data }))
    }
}

#[async_trait]
impl Consensus for PostgresConsensus {
    async fn head(
        &self,
        _deadline: Instant,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError> {
        // TODO: properly use the deadline argument.
        let mut client = self.client.lock().await;
        let tx = self.tx(&mut client).await?;
        let ret = self.head_tx(&tx, key).await?;
        tx.commit().await?;
        Ok(ret)
    }

    async fn compare_and_set(
        &self,
        _deadline: Instant,
        key: &str,
        expected_seqno: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        // TODO: properly use the deadline argument.
        if let Some(expected) = expected_seqno {
            if new.seqno <= expected {
                return Err(Error::from(
                        format!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, expected)).into());
            }
        }
        let mut client = self.client.lock().await;
        let tx = self.tx(&mut client).await?;

        let q = "DELETE FROM head WHERE shard = $1 RETURNING sequence_number, data";
        let current_head = match tx.query_opt(q, &[&key]).await? {
            Some(row) => {
                let head = VersionedData {
                    seqno: row.get(0),
                    data: row.get(1),
                };
                Some(head)
            }
            None => None,
        };

        let current_seqno = current_head.as_ref().map(|h| h.seqno);
        if current_seqno != expected_seqno {
            // The DELETE will be rolled back since the transaction won't commit
            return Ok(Err(current_head));
        }

        if let Some(head) = current_head {
            let q = "INSERT INTO tail VALUES ($1, $2, $3)";
            tx.execute(q, &[&key, &head.seqno, &head.data]).await?;
        }

        let q = "INSERT INTO head VALUES ($1, $2, $3)";
        tx.execute(q, &[&key, &new.seqno, &new.data]).await?;

        tx.commit().await?;

        Ok(Ok(()))
    }

    async fn scan(
        &self,
        _deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let mut client = self.client.lock().await;
        let tx = self.tx(&mut client).await?;
        // TODO: properly use the deadline argument.
        let q = "SELECT sequence_number, data FROM tail
                 WHERE shard = $1 AND sequence_number >= $2
                 ORDER BY sequence_number";
        let rows = tx.query(q, &[&key, &from]).await?;
        let mut results = vec![];
        for row in rows {
            let seqno: SeqNo = row.get("sequence_number");
            let data: Vec<u8> = row.get("data");
            results.push(VersionedData { seqno, data });
        }
        results.extend(self.head_tx(&tx, key).await?);

        if results.is_empty() {
            Err(ExternalError::from(anyhow!(
                "sequence number lower bound too high for scan: {:?}",
                from
            )))
        } else {
            tx.commit().await?;
            Ok(results)
        }
    }

    async fn truncate(
        &self,
        deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError> {
        let q = "DELETE FROM consensus
                WHERE shard = $1 AND sequence_number < $2 AND
                EXISTS(
                    SELECT * FROM consensus WHERE shard = $1 AND sequence_number >= $2
                )";

        let result = {
            let client = self.client.lock().await;
            client.execute(&*q, &[&key, &seqno]).await?
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
            let current = self.head(deadline, key).await?;
            if current.map_or(true, |data| data.seqno < seqno) {
                return Err(ExternalError::from(anyhow!(
                    "upper bound too high for truncate: {:?}",
                    seqno
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::location::tests::consensus_impl_test;
    use tracing::info;

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

        consensus_impl_test(|| futures_executor::block_on(PostgresConsensus::open(config.clone())))
            .await
    }
}
