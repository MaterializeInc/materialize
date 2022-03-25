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
use rusqlite::{named_params, params, Connection, OptionalExtension};
use tokio::sync::Mutex;

use crate::error::Error;
use crate::location::{Consensus, ExternalError, SeqNo, VersionedData};

const APPLICATION_ID: i32 = 0x0678_ef32; // chosen randomly

const SCHEMA: &str = "
CREATE TABLE consensus (
    shard text NOT NULL,
    sequence_number integer NOT NULL,
    data blob NOT NULL,
    PRIMARY KEY(shard, sequence_number)
);
";

/// Implementation of [Consensus] over a sqlite database.
#[derive(Debug)]
pub struct SqliteConsensus {
    conn: Arc<Mutex<Connection>>,
    shard: String,
}

impl SqliteConsensus {
    /// Open a sqlite-backed [Consensus] instance at `path`, for the collection
    /// named `shard`.
    pub fn open<P: AsRef<Path>>(path: P, shard: String) -> Result<Self, Error> {
        let mut conn = Connection::open(path)?;
        let tx = conn.transaction()?;
        let app_id: i32 = tx.query_row("PRAGMA application_id", params![], |row| row.get(0))?;
        if app_id == 0 {
            tx.execute_batch(&format!(
                "PRAGMA application_id = {APPLICATION_ID};
                 PRAGMA user_version = 1;"
            ))?;
            tx.execute_batch(SCHEMA)?;
        } else if app_id != APPLICATION_ID {
            return Err(Error::from(format!("invalid application id: {}", app_id)));
        }
        tx.commit()?;
        Ok(SqliteConsensus {
            conn: Arc::new(Mutex::new(conn)),
            shard,
        })
    }

    /// Remove all versions of data inserted at sequence numbers < `sequence_number`.
    ///
    /// TODO: We probably are going to move this function directly into the [Consensus]
    /// trait itself.
    async fn truncate(&mut self, sequence_number: SeqNo) -> Result<(), Error> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "DELETE FROM consensus
             WHERE shard = $shard AND sequence_number < $sequence_number",
        )?;

        stmt.execute(named_params! {"$shard": self.shard, "$sequence_number": sequence_number.0})?;

        Ok(())
    }
}

#[async_trait]
impl Consensus for SqliteConsensus {
    async fn head(&self, _deadline: Instant) -> Result<Option<VersionedData>, ExternalError> {
        let conn = self.conn.lock().await;
        let mut stmt = conn.prepare(
            "SELECT sequence_number, data FROM consensus
                 WHERE shard = $shard ORDER BY sequence_number DESC LIMIT 1",
        )?;
        stmt.query_row(named_params! {"$shard": self.shard}, |row| {
            let sequence_number = row.get("sequence_number")?;
            let data: Vec<_> = row.get("data")?;
            Ok(VersionedData {
                seqno: SeqNo(sequence_number),
                data,
            })
        })
        .optional()
        .map_err(|e| e.into())
    }

    async fn compare_and_set(
        &mut self,
        deadline: Instant,
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

        let result = if let Some(expected) = expected {
            let conn = self.conn.lock().await;

            // Only insert the new row if:
            // - sequence number expected is already present
            // - expected corresponds to the most recent sequence number
            //   i.e. there is no other sequence number > expected already
            //   present.
            let mut stmt = conn
                .prepare_cached(
                    "INSERT INTO consensus SELECT $shard, $sequence_number, $data WHERE
                     EXISTS (
                        SELECT * FROM consensus WHERE shard = $shard AND sequence_number = $expected
                     )
                     AND NOT EXISTS (
                         SELECT * FROM consensus WHERE shard = $shard AND sequence_number > $expected
                     )
                     ON CONFLICT DO NOTHING",
                )?;

            stmt.execute(named_params! {
                "$shard": self.shard,
                "$sequence_number": new.seqno.0,
                "$data": new.data,
                "$expected": expected.0,
            })?
        } else {
            let conn = self.conn.lock().await;

            // Insert the new row as long as no other row exists for the same shard.
            let mut stmt = conn.prepare_cached(
                "INSERT INTO consensus SELECT $shard, $sequence_number, $data WHERE
                     NOT EXISTS (
                         SELECT * FROM consensus WHERE shard = $shard
                     )
                     ON CONFLICT DO NOTHING",
            )?;
            stmt.execute(named_params! {
                "$shard": self.shard,
                "$sequence_number": new.seqno.0,
                "$data": new.data,
            })?
        };

        if result == 1 {
            // Truncate everything strictly less than the row we just inserted.
            // We're doing this as a best-effort measure to avoid unbounded
            // memory growth while using this API, so don't take the result into
            // account.
            //
            // TODO: remove this call once truncate becomes a full featured member
            // of [Consensus] or, restructure this implementation to not keep historical
            // data around if we don't need it.
            let _ = self.truncate(new.seqno).await;
            Ok(Ok(()))
        } else {
            // It's safe to call head in a subsequent transaction rather than doing
            // so directly in the same transaction because, once a given (seqno, data)
            // pair exists for our shard, we enforce the invariants that
            // 1. Our shard will always have _some_ data mapped to it.
            // 2. All operations that modify the (seqno, data) can only increase
            //    the sequence number.
            let current = self.head(deadline).await?;
            Ok(Err(current))
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::location::tests::consensus_impl_test;

    use super::*;

    #[tokio::test]
    async fn sqlite_consensus() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;
        consensus_impl_test(|| {
            let path = temp_dir.path().join("sqlite_consensus");
            SqliteConsensus::open(&path, "test_shard".to_string())
        })
        .await
    }
}
