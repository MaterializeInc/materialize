// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::vec;

use rusqlite::{params, Connection};

use persist_types::Codec;

use crate::{Stash, StashError, StashOp};

const APPLICATION_ID: i32 = 0x0872_e898;

const SCHEMA: &str = "CREATE TABLE stash (
    key blob NOT NULL UNIQUE,
    value blob NOT NULL
);";

type Error = StashError<rusqlite::Error>;

/// A stash backed by a SQLite database.
#[derive(Debug)]
pub struct SqliteStash {
    conn: Connection,
}

impl SqliteStash {
    /// Opens the stash stored at the specified path.
    pub fn open<P>(path: P) -> Result<SqliteStash, Error>
    where
        P: AsRef<Path>,
    {
        let mut conn = Connection::open(path)?;
        let tx = conn.transaction()?;
        let app_id: i32 = tx.query_row("PRAGMA application_id", params![], |row| row.get(0))?;
        if app_id == 0 {
            tx.execute_batch(&format!("PRAGMA application_id = {}", APPLICATION_ID))?;
            tx.execute_batch("PRAGMA user_version = 1")?;
            tx.execute_batch(SCHEMA)?;
        } else if app_id != APPLICATION_ID {
            return Err(Error::Corruption(format!(
                "invalid application id: {}",
                app_id
            )));
        }
        tx.commit()?;
        Ok(SqliteStash { conn })
    }
}

impl<K, V> Stash<K, V> for SqliteStash
where
    K: Codec,
    V: Codec,
{
    type EngineError = rusqlite::Error;

    type ReplayIterator = vec::IntoIter<(K, V)>;

    fn write_batch(&mut self, ops: Vec<StashOp<K, V>>) -> Result<(), Error> {
        let tx = self.conn.transaction()?;
        for op in ops {
            match op {
                StashOp::Put(key, val) => {
                    let mut key_buf = vec![];
                    let mut val_buf = vec![];
                    key.encode(&mut key_buf);
                    val.encode(&mut val_buf);
                    tx.execute(
                        "INSERT INTO stash (key, value) VALUES (?, ?)
                        ON CONFLICT (key) DO UPDATE SET value = excluded.value",
                        params![key_buf, val_buf],
                    )?;
                }
                StashOp::Delete(key) => {
                    let mut key_buf = vec![];
                    key.encode(&mut key_buf);
                    tx.execute("DELETE FROM stash WHERE key = ?", params![key_buf])?;
                }
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn replay(&self) -> Result<Self::ReplayIterator, Error> {
        let mut out = vec![];
        let mut stmt = self.conn.prepare("SELECT key, value FROM stash")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let key_buf: Vec<u8> = row.get("key")?;
            let val_buf: Vec<u8> = row.get("value")?;
            let key = K::decode(&key_buf).map_err(|e| Error::Codec(e))?;
            let val = V::decode(&val_buf).map_err(|e| Error::Codec(e))?;
            out.push((key, val));
        }
        Ok(out.into_iter())
    }
}
