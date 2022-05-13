// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by sqlite.

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::anyhow;
use async_trait::async_trait;
use rusqlite::types::{FromSql, FromSqlError, ToSql, ToSqlOutput, Value, ValueRef};
use rusqlite::{named_params, params, Connection, Error as SqliteError, OptionalExtension};
use std::sync::Mutex;

use crate::error::Error as PersistError;
use crate::location::{Consensus, ExternalError, SeqNo, VersionedData};

const APPLICATION_ID: i32 = 0x0678_ef32; // chosen randomly

const SCHEMA: &str = "
CREATE TABLE consensus (
    shard text NOT NULL,
    sequence_number bigint NOT NULL,
    data blob NOT NULL,
    PRIMARY KEY(shard, sequence_number)
);";

impl ToSql for SeqNo {
    fn to_sql(&self) -> Result<ToSqlOutput<'_>, SqliteError> {
        // We can only represent sequence numbers in the range [0, i64::MAX].
        let value = match i64::try_from(self.0) {
            Ok(value) => value,
            Err(e) => return Err(SqliteError::ToSqlConversionFailure(Box::new(e))),
        };
        Ok(ToSqlOutput::Owned(Value::Integer(value)))
    }
}

impl FromSql for SeqNo {
    fn column_result(value: ValueRef<'_>) -> Result<Self, FromSqlError> {
        let sequence_number = <i64 as FromSql>::column_result(value)?;

        // Sanity check that the sequence number we received from sqlite falls
        // in the [0, i64::MAX] range.
        let sequence_number = match u64::try_from(sequence_number) {
            Ok(seqno) => seqno,
            Err(e) => return Err(FromSqlError::Other(Box::new(e))),
        };

        Ok(SeqNo(sequence_number))
    }
}

/// Implementation of [Consensus] over a sqlite database.
#[derive(Debug)]
pub struct SqliteConsensus {
    // Intentionally a std::sync::Mutex instead of a tokio one because we don't
    // hold it across await points (ha! there are no await points with
    // rusqlite).
    conn: Arc<Mutex<Connection>>,
}

impl SqliteConsensus {
    /// Open a sqlite-backed [Consensus] instance at `path`.
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self, ExternalError> {
        let path = path.as_ref().to_path_buf();
        mz_ore::task::spawn_blocking(
            || "sqlite::open",
            move || {
                let mut conn = Connection::open(path)?;
                let tx = conn.transaction()?;
                const Q: &str = "PRAGMA application_id";
                let app_id: i32 = tx.query_row(Q, params![], |row| row.get(0))?;
                if app_id == 0 {
                    let q = format!(
                        "PRAGMA application_id = {}; PRAGMA user_version = 1; {}",
                        APPLICATION_ID, SCHEMA
                    );
                    tx.execute_batch(&q)?;
                } else if app_id != APPLICATION_ID {
                    return Err(ExternalError::from(anyhow!(
                        "invalid application id: {}",
                        app_id
                    )));
                }
                tx.commit()?;
                Ok(SqliteConsensus {
                    conn: Arc::new(Mutex::new(conn)),
                })
            },
        )
        .await?
    }
}

#[async_trait]
impl Consensus for SqliteConsensus {
    async fn head(
        &self,
        _deadline: Instant,
        key: &str,
    ) -> Result<Option<VersionedData>, ExternalError> {
        let shard = key.to_owned();
        let conn = Arc::clone(&self.conn);
        mz_ore::task::spawn_blocking(
            || "sqlite::head",
            move || {
                let conn = conn.lock().map_err(PersistError::from)?;
                const Q: &str = "
SELECT sequence_number, data FROM consensus
WHERE shard = $shard ORDER BY sequence_number DESC LIMIT 1";
                let mut stmt = conn.prepare(Q)?;
                stmt.query_row(named_params! {"$shard": shard}, |row| {
                    let seqno = row.get("sequence_number")?;
                    let data: Vec<_> = row.get("data")?;
                    Ok(VersionedData { seqno, data })
                })
                .optional()
                .map_err(|e| e.into())
            },
        )
        .await?
    }

    async fn compare_and_set(
        &self,
        deadline: Instant,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<Result<(), Option<VersionedData>>, ExternalError> {
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(ExternalError::from(
                        anyhow!("new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                                 new.seqno, expected)));
            }
        }

        let shard = key.to_owned();
        let conn = Arc::clone(&self.conn);
        let result = mz_ore::task::spawn_blocking(
            || "sqlite::compare_and_set",
            move || -> Result<usize, ExternalError> {
                let conn = conn.lock().map_err(PersistError::from)?;
                let result = if let Some(expected) = expected {
                    // Only insert the new row if:
                    // - sequence number expected is already present
                    // - expected corresponds to the most recent sequence number
                    //   i.e. there is no other sequence number > expected already
                    //   present.
                    const Q: &str = "
INSERT INTO consensus SELECT $shard, $sequence_number, $data WHERE
EXISTS (
   SELECT * FROM consensus WHERE shard = $shard AND sequence_number = $expected
)
AND NOT EXISTS (
    SELECT * FROM consensus WHERE shard = $shard AND sequence_number > $expected
)
ON CONFLICT DO NOTHING";
                    let mut stmt = conn.prepare_cached(Q)?;
                    stmt.execute(named_params! {
                        "$shard": shard,
                        "$sequence_number": new.seqno,
                        "$data": new.data,
                        "$expected": expected,
                    })?
                } else {
                    // Insert the new row as long as no other row exists for the same shard.
                    const Q: &str = "
INSERT INTO consensus SELECT $shard, $sequence_number, $data WHERE
NOT EXISTS (
    SELECT * FROM consensus WHERE shard = $shard
)
ON CONFLICT DO NOTHING";
                    let mut stmt = conn.prepare_cached(Q)?;
                    stmt.execute(named_params! {
                        "$shard": shard,
                        "$sequence_number": new.seqno,
                        "$data": new.data,
                    })?
                };
                Ok(result)
            },
        )
        .await??;

        if result == 1 {
            Ok(Ok(()))
        } else {
            // It's safe to call head in a subsequent transaction rather than doing
            // so directly in the same transaction because, once a given (seqno, data)
            // pair exists for our shard, we enforce the invariants that
            // 1. Our shard will always have _some_ data mapped to it.
            // 2. All operations that modify the (seqno, data) can only increase
            //    the sequence number.
            let current = self.head(deadline, key).await?;
            Ok(Err(current))
        }
    }

    async fn scan(
        &self,
        _deadline: Instant,
        key: &str,
        from: SeqNo,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let shard = key.to_owned();
        let conn = Arc::clone(&self.conn);
        let rows = mz_ore::task::spawn_blocking(
            || "sqlite::scan",
            move || -> Result<Vec<VersionedData>, ExternalError> {
                let conn = conn.lock().map_err(PersistError::from)?;
                const Q: &str = "
SELECT sequence_number, data FROM consensus
WHERE shard = $shard AND sequence_number >= $from
ORDER BY sequence_number";
                let mut stmt = conn.prepare_cached(Q)?;
                let params = named_params! {"$shard": shard, "$from": from};
                let rows = stmt.query_map(params, |row| {
                    let seqno = row.get("sequence_number")?;
                    let data: Vec<_> = row.get("data")?;
                    Ok(VersionedData { seqno, data })
                })?;

                let rows = rows.collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            },
        )
        .await??;

        if rows.is_empty() {
            Err(ExternalError::from(anyhow!(
                "sequence number lower bound too high for scan: {:?}",
                from
            )))
        } else {
            Ok(rows)
        }
    }

    async fn truncate(
        &self,
        deadline: Instant,
        key: &str,
        seqno: SeqNo,
    ) -> Result<(), ExternalError> {
        let shard = key.to_owned();
        let conn = Arc::clone(&self.conn);
        let result = mz_ore::task::spawn_blocking(
            || "sqlite::truncate",
            move || -> Result<usize, ExternalError> {
                let conn = conn.lock().map_err(PersistError::from)?;
                const Q: &str = "
DELETE FROM consensus
WHERE shard = $shard AND sequence_number < $sequence_number AND
EXISTS(
    SELECT * FROM consensus WHERE shard = $shard AND sequence_number >= $sequence_number
)";
                let mut stmt = conn.prepare_cached(Q)?;
                let result =
                    stmt.execute(named_params! {"$shard": shard, "$sequence_number": seqno})?;
                Ok(result)
            },
        )
        .await??;

        if result == 0 {
            // We weren't able to successfully truncate any rows. Inspect head to
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

    use super::*;

    #[tokio::test]
    async fn sqlite_consensus() -> Result<(), ExternalError> {
        let temp_dir = tempfile::tempdir()?;
        consensus_impl_test(|| async {
            SqliteConsensus::open(temp_dir.path().join("sqlite_consensus")).await
        })
        .await
    }
}
