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
use tokio::task::JoinHandle;
use tokio_postgres::types::{to_sql_checked, FromSql, IsNull, ToSql, Type};
use tokio_postgres::{Client as PostgresClient, NoTls};

use mz_ore::task;

use crate::error::Error;
use crate::location::{Consensus, ExternalError, SeqNo, VersionedData};

const SCHEMA: &str = "
SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;

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
CREATE OR REPLACE FUNCTION compare_and_set(
    target_shard text,
    expected_seqno bigint,
    new_seqno bigint,
    new_data bytea,
    OUT success boolean,
    OUT actual_seqno bigint,
    OUT actual_data bytea
) AS $$
DECLARE
   head_seqno bigint;
   head_data bytea;
BEGIN
    SELECT sequence_number, data
    FROM consensus
    WHERE shard = target_shard
    ORDER BY sequence_number DESC LIMIT 1
    INTO head_seqno, head_data;

    IF head_seqno IS NOT DISTINCT FROM expected_seqno THEN
        INSERT INTO consensus VALUES (target_shard, new_seqno, new_data);
        success := true;
    ELSE
        success := false;
        actual_seqno := head_seqno;
        actual_data := head_data;
    END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION truncate(
    target_shard text,
    truncate_seqno bigint,
    OUT success boolean
) AS $$
DECLARE
   head_seqno bigint;
BEGIN
    SELECT sequence_number
    FROM consensus
    WHERE shard = target_shard
    ORDER BY sequence_number DESC LIMIT 1
    INTO head_seqno;

    IF truncate_seqno > head_seqno THEN
        success := false;
    ELSE
        DELETE FROM consensus WHERE shard = target_shard AND sequence_number < truncate_seqno;
        success := true;
    END IF;
END;
$$ LANGUAGE plpgsql;

SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;
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
    client: Arc<PostgresClient>,
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
            client: Arc::new(client),
            _handle: handle,
        })
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
        let q = "SELECT sequence_number, data FROM consensus WHERE shard = $1 ORDER BY sequence_number DESC LIMIT 1";

        let row = match self.client.query_opt(q, &[&key]).await? {
            Some(row) => row,
            None => return Ok(None),
        };

        let seqno: SeqNo = row.try_get("sequence_number")?;
        let data: Vec<u8> = row.try_get("data")?;
        Ok(Some(VersionedData { seqno, data }))
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

        let q = "SELECT * FROM compare_and_set($1, $2, $3, $4);";
        let row = self
            .client
            .query_one(q, &[&key, &expected, &new.seqno, &new.data])
            .await?;

        let success: bool = row.try_get("success")?;
        let ret = if success {
            Ok(())
        } else {
            let seqno: Option<SeqNo> = row.try_get("actual_seqno")?;
            let data: Option<Vec<u8>> = row.try_get("actual_data")?;
            match (seqno, data) {
                (Some(seqno), Some(data)) => Err(Some(VersionedData { seqno, data })),
                (None, None) => Err(None),
                _ => panic!("invalid state"),
            }
        };
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
        let rows = self.client.query(q, &[&key, &from]).await?;
        let mut results = vec![];

        for row in rows {
            let seqno: SeqNo = row.try_get("sequence_number")?;
            let data: Vec<u8> = row.try_get("data")?;
            results.push(VersionedData { seqno, data });
        }

        if results.is_empty() {
            Err(ExternalError::from(anyhow!(
                "sequence number lower bound too high for scan: {from:?}"
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
        let q = "SELECT * FROM truncate($1, $2);";
        let row = self.client.query_one(q, &[&key, &seqno]).await?;

        let success: bool = row.try_get("success")?;
        let ret = if success {
            Ok(())
        } else {
            Err(ExternalError::from(anyhow!(
                "upper bound too high for truncate: {:?}",
                seqno
            )))
        };

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
