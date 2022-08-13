// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable metadata storage.

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::path::Path;

use async_trait::async_trait;
use mz_ore::collections::CollectionExt;
use rusqlite::{named_params, params, Connection, OptionalExtension, Transaction};

use mz_persist_types::Codec;

use crate::{
    Append, AppendBatch, Data, Diff, Id, InternalStashError, Stash, StashCollection, StashError,
};

const APPLICATION_ID: i32 = 0x0872_e898; // chosen randomly

const SCHEMA: &str = "
CREATE TABLE collections (
    collection_id integer PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE data (
    collection_id integer NOT NULL REFERENCES collections (collection_id),
    key blob NOT NULL,
    value blob NOT NULL,
    PRIMARY KEY (collection_id, key)
);
";

/// A Stash whose data is stored in a single file on disk. The format of this
/// file is not specified and should not be relied upon. The only promise is
/// stability. Any changes to the on-disk format will be accompanied by a clear
/// migration path.
#[derive(Debug)]
pub struct Sqlite {
    conn: Connection,
}

impl Sqlite {
    /// Opens the stash stored at the specified path. If `None`, opens an in-memory
    /// instance.
    pub fn open(path: Option<&Path>) -> Result<Sqlite, StashError> {
        let mut conn = match path {
            Some(path) => Connection::open(path)?,
            None => Connection::open_in_memory()?,
        };
        let tx = conn.transaction()?;
        let app_id: i32 = tx.query_row("PRAGMA application_id", params![], |row| row.get(0))?;
        if app_id == 0 {
            tx.execute_batch(&format!(
                "PRAGMA application_id = {APPLICATION_ID};
                 PRAGMA user_version = 1;"
            ))?;
            tx.execute_batch(SCHEMA)?;
        } else if app_id != APPLICATION_ID {
            return Err(StashError::from(format!(
                "invalid application id: {}",
                app_id
            )));
        }
        tx.commit()?;
        Ok(Sqlite { conn })
    }

    fn update_many_tx<I>(tx: &Transaction, collection_id: Id, entries: I) -> Result<(), StashError>
    where
        I: Iterator<Item = ((Vec<u8>, Vec<u8>), Diff)>,
    {
        let mut insert_stmt = tx.prepare(
            "INSERT OR IGNORE
             INTO data (collection_id, key, value)
             VALUES ($collection_id, $key, $value)",
        )?;
        let mut delete_stmt = tx.prepare(
            "DELETE FROM data
              WHERE collection_id = $collection_id AND key = $key",
        )?;
        for ((key, value), diff) in entries {
            match diff {
                Diff::Insert => {
                    let count = insert_stmt.execute(named_params! {
                        "$collection_id": collection_id,
                        "$key": key,
                        "$value": value,
                    })?;
                    if count == 0 {
                        return Err(StashError::from(InternalStashError::InsertDuplicateKey));
                    }
                }
                Diff::Delete => {
                    let count = delete_stmt.execute(named_params! {
                        "$collection_id": collection_id,
                        "$key": key,
                    })?;
                    if count == 0 {
                        return Err(StashError::from(InternalStashError::DeleteNotFound));
                    }
                }
            };
        }
        drop(insert_stmt);
        Ok(())
    }
}

#[async_trait]
impl Stash for Sqlite {
    async fn collection<K, V>(&mut self, name: &str) -> Result<StashCollection<K, V>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord,
    {
        let tx = self.conn.transaction()?;

        let collection_id_opt = tx
            .query_row(
                "SELECT collection_id FROM collections WHERE name = $name",
                named_params! {"$name": name},
                |row| row.get("collection_id"),
            )
            .optional()?;

        let collection_id = match collection_id_opt {
            Some(id) => id,
            None => {
                let collection_id = tx.query_row(
                    "INSERT INTO collections (name) VALUES ($name) RETURNING collection_id",
                    named_params! {"$name": name},
                    |row| row.get("collection_id"),
                )?;
                collection_id
            }
        };

        tx.commit()?;
        Ok(StashCollection {
            id: collection_id,
            _kv: PhantomData,
        })
    }

    async fn iter<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<BTreeMap<K, V>, StashError>
    where
        K: Data,
        V: Data,
    {
        let tx = self.conn.transaction()?;
        let rows = tx
            .prepare(
                "SELECT key, value FROM data
                 WHERE collection_id = $collection_id",
            )?
            .query_and_then(named_params! {"$collection_id": collection.id}, |row| {
                let key_buf: Vec<_> = row.get("key")?;
                let value_buf: Vec<_> = row.get("value")?;
                let key = K::decode(&key_buf)?;
                let value = V::decode(&value_buf)?;
                Ok::<_, StashError>((key, value))
            })?
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(rows)
    }

    async fn get_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Option<V>, StashError>
    where
        K: Data,
        V: Data,
    {
        let mut key_buf = vec![];
        key.encode(&mut key_buf);
        let tx = self.conn.transaction()?;
        let rows = tx
            .prepare(
                "SELECT value FROM data
                 WHERE collection_id = $collection_id AND key = $key",
            )?
            .query_and_then(
                named_params! {
                    "$collection_id": collection.id,
                    "$key": key_buf,
                },
                |row| {
                    let value_buf: Vec<_> = row.get("value")?;
                    let value = V::decode(&value_buf)?;
                    Ok::<_, StashError>(value)
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        if rows.is_empty() {
            Ok(None)
        } else {
            Ok(Some(rows.into_element()))
        }
    }

    async fn update_many<K: Codec, V: Codec, I>(
        &mut self,
        collection: StashCollection<K, V>,
        entries: I,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
        I: IntoIterator<Item = ((K, V), Diff)> + Send,
        I::IntoIter: Send,
    {
        let entries = entries.into_iter().map(|((key, value), diff)| {
            let mut key_buf = vec![];
            let mut value_buf = vec![];
            key.encode(&mut key_buf);
            value.encode(&mut value_buf);
            ((key_buf, value_buf), diff)
        });
        let tx = self.conn.transaction()?;
        Self::update_many_tx(&tx, collection.id, entries)?;
        tx.commit()?;
        Ok(())
    }

    async fn confirm_leadership(&mut self) -> Result<(), StashError> {
        // SQLite doesn't have a concept of leadership
        Ok(())
    }
}

impl From<rusqlite::Error> for StashError {
    fn from(e: rusqlite::Error) -> StashError {
        StashError {
            inner: InternalStashError::Sqlite(e),
        }
    }
}

#[async_trait]
impl Append for Sqlite {
    async fn append<I>(&mut self, batches: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = AppendBatch> + Send,
        I::IntoIter: Send,
    {
        let tx = self.conn.transaction()?;
        for batch in batches {
            Self::update_many_tx(&tx, batch.collection_id, batch.entries.into_iter())?;
        }
        tx.commit()?;
        Ok(())
    }
}
