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
-- Obtain an advisory lock before attempting to create the schema. This is
-- necessary to work around concurrency bugs in `CREATE TABLE IF NOT EXISTS`
-- in PostgreSQL.
--
-- See: https://github.com/MaterializeInc/materialize/issues/12560
-- See: https://www.postgresql.org/message-id/CA%2BTgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg%40mail.gmail.com
-- See: https://stackoverflow.com/a/29908840
--
-- The lock ID was randomly generated.
SELECT pg_advisory_xact_lock(135664303235462630);

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
        let q = "SELECT sequence_number, data FROM consensus WHERE shard = $1 ORDER BY sequence_number DESC LIMIT 1";

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
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        // TODO: properly use the deadline argument.

        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(Error::from(
                        format!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, expected)).into());
            }
        }

        let mut client = self.client.lock().await;
        let tx = self.tx(&mut client).await?;
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

            tx.execute(&*q, &[&key, &new.seqno, &new.data, &expected])
                .await?
        } else {
            // Insert the new row as long as no other row exists for the same shard.
            let q = "INSERT INTO consensus SELECT $1, $2, $3 WHERE
                     NOT EXISTS (
                         SELECT * FROM consensus WHERE shard = $1
                     )
                     ON CONFLICT DO NOTHING";
            tx.execute(&*q, &[&key, &new.seqno, &new.data]).await?
        };

        let ret = if result == 1 {
            Ok(())
        } else {
            let current = self.head_tx(&tx, key).await?;
            Err(current)
        };

        tx.commit().await?;
        Ok(ret)
    }

    async fn scan(
        &self,
        _deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        // TODO: properly use the deadline argument.

        let q = "SELECT sequence_number, data FROM consensus
             WHERE shard = $1 AND sequence_number >= $2
             ORDER BY sequence_number";
        let mut client = self.client.lock().await;
        let tx = self.tx(&mut client).await?;
        // Intentianally wait to actually use the data until after the transaction
        // has successfully committed to ensure the reads were valid. See [1]
        // for more details.
        //
        // [1]: https://www.postgresql.org/docs/current/transaction-iso.html#XACT-SERIALIZABLE
        let rows = tx.query(&*q, &[&key, &from]).await?;
        tx.commit().await?;
        let mut results = vec![];

        for row in rows {
            let seqno: SeqNo = row.try_get("sequence_number")?;
            let data: Vec<u8> = row.try_get("data")?;
            results.push(VersionedData { seqno, data });
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

    async fn truncate(
        &self,
        _deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError> {
        let q = "DELETE FROM consensus
                WHERE shard = $1 AND sequence_number < $2 AND
                EXISTS(
                    SELECT * FROM consensus WHERE shard = $1 AND sequence_number >= $2
                )";

        let mut client = self.client.lock().await;
        let tx = self.tx(&mut client).await?;
        let result = tx.execute(&*q, &[&key, &seqno]).await?;
        let ret = if result == 0 {
            // We weren't able to successfully truncate any rows inspect head to
            // determine whether the request was valid and there were no records in
            // the provided range, or the request was invalid because it would have
            // also deleted head.
            let current = self.head_tx(&tx, key).await?;
            // Intentionally don't early exit here and instead wait until after
            // we have committed the transaction to ensure that our reads were
            // valid. See [1] for more details.
            //
            // [1]: https://www.postgresql.org/docs/current/transaction-iso.html#XACT-SERIALIZABLE
            if current.map_or(true, |data| data.seqno < seqno) {
                Err(ExternalError::from(anyhow!(
                    "upper bound too high for truncate: {:?}",
                    seqno
                )))
            } else {
                Ok(())
            }
        } else {
            Ok(())
        };

        tx.commit().await?;

        ret
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

        consensus_impl_test(|| PostgresConsensus::open(config.clone())).await
    }
}
