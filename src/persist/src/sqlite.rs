// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Consensus] backed by SQLite.

use std::sync::Arc;

use anyhow::anyhow;
use async_stream::try_stream;
use async_trait::async_trait;
use bytes::Bytes;
use mz_ore::cast::CastFrom;
use rusqlite::params;
use tokio::sync::Mutex;

use crate::error::Error;
use crate::location::{CaSResult, Consensus, ExternalError, ResultStream, SeqNo, VersionedData};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS consensus (
    shard TEXT NOT NULL,
    sequence_number INTEGER NOT NULL,
    data BLOB NOT NULL,
    PRIMARY KEY (shard, sequence_number)
)
";

/// Configuration to connect to a SQLite-backed implementation of [Consensus].
#[derive(Clone, Debug)]
pub struct SqliteConsensusConfig {
    path: String,
}

impl SqliteConsensusConfig {
    /// Returns a new [SqliteConsensusConfig] for the given path.
    pub fn new(path: String) -> Self {
        SqliteConsensusConfig { path }
    }

    /// Extract the path from a `sqlite://` URL.
    pub fn from_url(url: &url::Url) -> Result<Self, anyhow::Error> {
        // sqlite:///absolute/path -> path = /absolute/path
        // sqlite://relative/path -> path = relative/path
        let path = if let Some(host) = url.host_str() {
            // sqlite://relative/path: host = "relative", path = "/path"
            format!("{}{}", host, url.path())
        } else {
            // sqlite:///absolute/path: host = None, path = "/absolute/path"
            url.path().to_string()
        };
        if path.is_empty() {
            anyhow::bail!("missing path in sqlite URL: {}", url);
        }
        Ok(SqliteConsensusConfig { path })
    }

    /// Returns a new [SqliteConsensusConfig] for use in unit tests.
    #[cfg(test)]
    pub fn new_for_test() -> Result<Self, ExternalError> {
        let dir = tempfile::tempdir().map_err(|e| ExternalError::from(anyhow!(e)))?;
        #[allow(deprecated)]
        let path = dir.into_path().join("consensus_test.db");
        Ok(SqliteConsensusConfig {
            path: path.to_string_lossy().into_owned(),
        })
    }
}

/// Implementation of [Consensus] over a SQLite database.
pub struct SqliteConsensus {
    conn: Arc<Mutex<rusqlite::Connection>>,
}

impl std::fmt::Debug for SqliteConsensus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteConsensus").finish_non_exhaustive()
    }
}

impl SqliteConsensus {
    /// Open a SQLite [Consensus] instance with `config`.
    pub async fn open(config: SqliteConsensusConfig) -> Result<Self, ExternalError> {
        let path = config.path.clone();
        let conn = mz_ore::task::spawn_blocking(
            || "persist::sqlite::open",
            move || -> Result<_, ExternalError> {
                let conn = rusqlite::Connection::open(&path)
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                conn.execute_batch("PRAGMA journal_mode = WAL;")
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                conn.execute_batch("PRAGMA busy_timeout = 5000;")
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                conn.execute_batch("PRAGMA synchronous = OFF;")
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                conn.execute_batch("PRAGMA journal_size_limit = 0;")
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                conn.execute_batch("PRAGMA mmap_size = 67108864;")
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                conn.execute_batch("PRAGMA cache_size = -8000;")
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                conn.execute_batch("PRAGMA temp_store = MEMORY;")
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                conn.execute_batch(SCHEMA)
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                Ok(conn)
            },
        )
        .await?;

        Ok(SqliteConsensus {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

#[async_trait]
impl Consensus for SqliteConsensus {
    fn list_keys(&self) -> ResultStream<'_, String> {
        let conn = Arc::clone(&self.conn);

        Box::pin(try_stream! {
            let keys: Vec<String> = mz_ore::task::spawn_blocking(
                || "persist::sqlite::list_keys",
                move || -> Result<Vec<String>, ExternalError> {
                    let conn = conn.blocking_lock();
                    let mut stmt = conn
                        .prepare("SELECT DISTINCT shard FROM consensus")
                        .map_err(|e| ExternalError::from(anyhow!(e)))?;
                    let rows = stmt
                        .query_map([], |row| row.get(0))
                        .map_err(|e| ExternalError::from(anyhow!(e)))?;
                    let mut keys = Vec::new();
                    for row in rows {
                        keys.push(row.map_err(|e| ExternalError::from(anyhow!(e)))?);
                    }
                    Ok(keys)
                },
            )
            .await?;

            for key in keys {
                yield key;
            }
        })
    }

    async fn head(&self, key: &str) -> Result<Option<VersionedData>, ExternalError> {
        let conn = Arc::clone(&self.conn);
        let key = key.to_owned();

        mz_ore::task::spawn_blocking(
            || "persist::sqlite::head",
            move || {
                let conn = conn.blocking_lock();
                let mut stmt = conn
                    .prepare(
                        "SELECT sequence_number, data FROM consensus
                     WHERE shard = ?1 ORDER BY sequence_number DESC LIMIT 1",
                    )
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                let result = stmt
                    .query_row(params![key], |row| {
                        let seqno: i64 = row.get(0)?;
                        let data: Vec<u8> = row.get(1)?;
                        Ok((seqno, data))
                    })
                    .optional()
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;

                match result {
                    None => Ok(None),
                    Some((seqno, data)) => {
                        let seqno =
                            u64::try_from(seqno).map_err(|e| ExternalError::from(anyhow!(e)))?;
                        Ok(Some(VersionedData {
                            seqno: SeqNo(seqno),
                            data: Bytes::from(data),
                        }))
                    }
                }
            },
        )
        .await
    }

    async fn compare_and_set(
        &self,
        key: &str,
        expected: Option<SeqNo>,
        new: VersionedData,
    ) -> Result<CaSResult, ExternalError> {
        if let Some(expected) = expected {
            if new.seqno <= expected {
                return Err(Error::from(format!(
                    "new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                    new.seqno, expected
                ))
                .into());
            }
        }

        let new_seqno = i64::try_from(new.seqno.0).map_err(|_| {
            ExternalError::from(anyhow!(
                "sequence number {} is too large for SQLite",
                new.seqno.0
            ))
        })?;

        let conn = Arc::clone(&self.conn);
        let key = key.to_owned();
        let new_data = new.data.to_vec();

        mz_ore::task::spawn_blocking(
            || "persist::sqlite::compare_and_set",
            move || {
                let conn = conn.blocking_lock();
                let txn = conn
                    .unchecked_transaction()
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;

                // Read the current head within the transaction.
                let current_seqno: Option<i64> = txn
                    .query_row(
                        "SELECT sequence_number FROM consensus
                     WHERE shard = ?1 ORDER BY sequence_number DESC LIMIT 1",
                        params![key],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;

                let matches = match (expected, current_seqno) {
                    (None, None) => true,
                    (Some(expected), Some(current)) => {
                        let current =
                            u64::try_from(current).map_err(|e| ExternalError::from(anyhow!(e)))?;
                        current == expected.0
                    }
                    _ => false,
                };

                if !matches {
                    txn.rollback()
                        .map_err(|e| ExternalError::from(anyhow!(e)))?;
                    return Ok(CaSResult::ExpectationMismatch);
                }

                txn.execute(
                    "INSERT INTO consensus (shard, sequence_number, data) VALUES (?1, ?2, ?3)",
                    params![key, new_seqno, new_data],
                )
                .map_err(|e| ExternalError::from(anyhow!(e)))?;

                txn.commit().map_err(|e| ExternalError::from(anyhow!(e)))?;

                Ok(CaSResult::Committed)
            },
        )
        .await
    }

    async fn scan(
        &self,
        key: &str,
        from: SeqNo,
        limit: usize,
    ) -> Result<Vec<VersionedData>, ExternalError> {
        let Ok(limit) = i64::try_from(limit) else {
            return Err(ExternalError::from(anyhow!(
                "limit must be [0, i64::MAX]. was: {:?}",
                limit
            )));
        };
        let from_seqno = i64::try_from(from.0).map_err(|_| {
            ExternalError::from(anyhow!(
                "sequence number {} is too large for SQLite",
                from.0
            ))
        })?;

        let conn = Arc::clone(&self.conn);
        let key = key.to_owned();

        mz_ore::task::spawn_blocking(
            || "persist::sqlite::scan",
            move || {
                let conn = conn.blocking_lock();
                let mut stmt = conn
                    .prepare(
                        "SELECT sequence_number, data FROM consensus
                     WHERE shard = ?1 AND sequence_number >= ?2
                     ORDER BY sequence_number ASC LIMIT ?3",
                    )
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;
                let rows = stmt
                    .query_map(params![key, from_seqno, limit], |row| {
                        let seqno: i64 = row.get(0)?;
                        let data: Vec<u8> = row.get(1)?;
                        Ok((seqno, data))
                    })
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;

                let mut results = Vec::new();
                for row in rows {
                    let (seqno, data) = row.map_err(|e| ExternalError::from(anyhow!(e)))?;
                    let seqno =
                        u64::try_from(seqno).map_err(|e| ExternalError::from(anyhow!(e)))?;
                    results.push(VersionedData {
                        seqno: SeqNo(seqno),
                        data: Bytes::from(data),
                    });
                }
                Ok(results)
            },
        )
        .await
    }

    async fn truncate(&self, key: &str, seqno: SeqNo) -> Result<Option<usize>, ExternalError> {
        let seqno_i64 = i64::try_from(seqno.0).map_err(|_| {
            ExternalError::from(anyhow!(
                "sequence number {} is too large for SQLite",
                seqno.0
            ))
        })?;

        let conn = Arc::clone(&self.conn);
        let key = key.to_owned();

        mz_ore::task::spawn_blocking(
            || "persist::sqlite::truncate",
            move || {
                let conn = conn.blocking_lock();
                let result = conn
                    .execute(
                        "DELETE FROM consensus
                     WHERE shard = ?1 AND sequence_number < ?2 AND
                     EXISTS (
                         SELECT 1 FROM consensus WHERE shard = ?1 AND sequence_number >= ?2
                     )",
                        params![key, seqno_i64],
                    )
                    .map_err(|e| ExternalError::from(anyhow!(e)))?;

                if result == 0 {
                    // Check if the request was valid by inspecting head.
                    let current: Option<i64> = conn
                        .query_row(
                            "SELECT sequence_number FROM consensus
                         WHERE shard = ?1 ORDER BY sequence_number DESC LIMIT 1",
                            params![key],
                            |row| row.get(0),
                        )
                        .optional()
                        .map_err(|e| ExternalError::from(anyhow!(e)))?;

                    let current_seqno = current.map(|s| {
                        u64::try_from(s).expect("sequence numbers should be non-negative")
                    });

                    if current_seqno.map_or(true, |s| s < seqno.0) {
                        return Err(ExternalError::from(anyhow!(
                            "upper bound too high for truncate: {:?}",
                            seqno
                        )));
                    }
                }

                Ok(Some(usize::cast_from(u64::cast_from(result))))
            },
        )
        .await
    }
}

// Import the `optional` extension for rusqlite.
use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use crate::location::tests::consensus_impl_test;

    use super::*;

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn sqlite_consensus() -> Result<(), ExternalError> {
        let config = SqliteConsensusConfig::new_for_test()?;
        consensus_impl_test(|| SqliteConsensus::open(config.clone())).await?;

        Ok(())
    }
}
