// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable metadata storage.

use std::marker::PhantomData;
use std::path::Path;
use std::{cmp, collections::BTreeSet};

use async_trait::async_trait;
use rusqlite::{named_params, params, Connection, OptionalExtension, Transaction};
use serde_json::Value;
use timely::progress::Antichain;
use timely::PartialOrder;

use timely::progress::frontier::AntichainRef;

use crate::{
    AntichainFormatter, Append, AppendBatch, Data, Diff, Id, InternalStashError, Stash,
    StashCollection, StashError, Timestamp,
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
    time integer NOT NULL,
    diff integer NOT NULL
);

CREATE INDEX data_time_idx ON data (collection_id, time);

CREATE TABLE sinces (
    collection_id NOT NULL UNIQUE REFERENCES collections (collection_id),
    since integer
);

CREATE TABLE uppers (
    collection_id NOT NULL UNIQUE REFERENCES collections (collection_id),
    upper integer
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

    fn since_tx(tx: &Transaction, collection_id: Id) -> Result<Antichain<Timestamp>, StashError> {
        let since: Option<Timestamp> = tx.query_row(
            "SELECT since FROM sinces WHERE collection_id = $collection_id",
            named_params! {"$collection_id": collection_id},
            |row| row.get("since"),
        )?;
        Ok(Antichain::from_iter(since))
    }

    fn upper_tx(tx: &Transaction, collection_id: Id) -> Result<Antichain<Timestamp>, StashError> {
        let upper: Option<Timestamp> = tx.query_row(
            "SELECT upper FROM uppers WHERE collection_id = $collection_id",
            named_params! {"$collection_id": collection_id},
            |row| row.get("upper"),
        )?;
        Ok(Antichain::from_iter(upper))
    }

    fn seal_batch_tx<'a, I>(tx: &Transaction, seals: I) -> Result<(), StashError>
    where
        I: Iterator<Item = (Id, &'a Antichain<Timestamp>)>,
    {
        let mut update_stmt =
            tx.prepare("UPDATE uppers SET upper = $upper WHERE collection_id = $collection_id")?;
        for (collection_id, new_upper) in seals {
            let upper = Self::upper_tx(tx, collection_id)?;
            if PartialOrder::less_than(new_upper, &upper) {
                return Err(StashError::from(format!(
                    "seal request {} is less than the current upper frontier {}",
                    AntichainFormatter(new_upper),
                    AntichainFormatter(&upper),
                )));
            }
            update_stmt.execute(
                named_params! {"$upper": new_upper.as_option(), "$collection_id": collection_id},
            )?;
        }
        drop(update_stmt);
        Ok(())
    }

    fn update_many_tx<I>(tx: &Transaction, collection_id: Id, entries: I) -> Result<(), StashError>
    where
        I: Iterator<Item = ((Value, Value), Timestamp, Diff)>,
    {
        let upper = Self::upper_tx(tx, collection_id)?;
        let mut insert_stmt = tx.prepare(
            "INSERT INTO data (collection_id, key, value, time, diff)
             VALUES ($collection_id, $key, $value, $time, $diff)",
        )?;
        for ((key, value), time, diff) in entries {
            if !upper.less_equal(&time) {
                return Err(StashError::from(format!(
                    "entry time {} is less than the current upper frontier {}",
                    time,
                    AntichainFormatter(&upper)
                )));
            }
            let key = serde_json::to_vec(&key)?;
            let value = serde_json::to_vec(&value)?;
            insert_stmt.execute(named_params! {
                "$collection_id": collection_id,
                "$key": key,
                "$value": value,
                "$time": time,
                "$diff": diff,
            })?;
        }
        drop(insert_stmt);
        Ok(())
    }

    fn compact_batch_tx<'a, I>(tx: &Transaction, compactions: I) -> Result<(), StashError>
    where
        I: Iterator<Item = (Id, &'a Antichain<Timestamp>)>,
    {
        let mut compact_stmt =
            tx.prepare("UPDATE sinces SET since = $since WHERE collection_id = $collection_id")?;
        for (collection_id, new_since) in compactions {
            let since = Self::since_tx(tx, collection_id)?;
            let upper = Self::upper_tx(tx, collection_id)?;
            if PartialOrder::less_than(&upper, new_since) {
                return Err(StashError::from(format!(
                    "compact request {} is greater than the current upper frontier {}",
                    AntichainFormatter(new_since),
                    AntichainFormatter(&upper)
                )));
            }
            if PartialOrder::less_than(new_since, &since) {
                return Err(StashError::from(format!(
                    "compact request {} is less than the current since frontier {}",
                    AntichainFormatter(new_since),
                    AntichainFormatter(&since)
                )));
            }
            compact_stmt.execute(
                named_params! {"$since": new_since.as_option(), "$collection_id": collection_id},
            )?;
        }
        drop(compact_stmt);
        Ok(())
    }

    fn consolidate_batch_tx(tx: &Transaction, collections: &[Id]) -> Result<(), StashError> {
        let mut consolidation_stmt = tx.prepare(
            "DELETE FROM data
             WHERE collection_id = $collection_id AND time <= $since
             RETURNING key, value, diff",
        )?;
        let mut insert_stmt = tx.prepare(
            "INSERT INTO data (collection_id, key, value, time, diff)
             VALUES ($collection_id, $key, $value, $time, $diff)",
        )?;
        let mut drop_stmt = tx.prepare("DELETE FROM data WHERE collection_id = $collection_id")?;

        for collection_id in collections {
            let since = Self::since_tx(tx, *collection_id)?.into_option();
            match since {
                Some(since) => {
                    let mut updates = consolidation_stmt
                        .query_and_then(
                            named_params! {
                                "$collection_id": collection_id,
                                "$since": since,
                            },
                            |row| {
                                let key = row.get("key")?;
                                let value = row.get("value")?;
                                let diff = row.get("diff")?;
                                Ok::<_, StashError>(((key, value), since, diff))
                            },
                        )?
                        .collect::<Result<Vec<((Vec<u8>, Vec<u8>), i64, i64)>, _>>()?;
                    differential_dataflow::consolidation::consolidate_updates(&mut updates);
                    for ((key, value), time, diff) in updates {
                        insert_stmt.execute(named_params! {
                            "$collection_id": collection_id,
                            "$key": key,
                            "$value": value,
                            "$time": time,
                            "$diff": diff,
                        })?;
                    }
                }
                None => {
                    drop_stmt.execute(named_params! {
                        "$collection_id": collection_id,
                    })?;
                }
            }
        }
        drop((consolidation_stmt, insert_stmt, drop_stmt));
        Ok(())
    }
}

#[async_trait]
impl Stash for Sqlite {
    async fn collection<K, V>(&mut self, name: &str) -> Result<StashCollection<K, V>, StashError>
    where
        K: Data,
        V: Data,
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
                tx.execute(
                    "INSERT INTO sinces (collection_id, since) VALUES ($collection_id, $since)",
                    named_params! {"$collection_id": collection_id, "$since": Timestamp::MIN},
                )?;
                tx.execute(
                    "INSERT INTO uppers (collection_id, upper) VALUES ($collection_id, $upper)",
                    named_params! {"$collection_id": collection_id, "$upper": Timestamp::MIN},
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

    async fn collections(&mut self) -> Result<BTreeSet<String>, StashError> {
        let tx = self.conn.transaction()?;
        let mut stmt = tx.prepare("SELECT name FROM collections")?;
        let mut rows = stmt.query([])?;
        let mut names = Vec::new();
        while let Some(row) = rows.next()? {
            names.push(row.get(0)?);
        }
        Ok(BTreeSet::from_iter(names))
    }

    async fn iter<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<((K, V), Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        let tx = self.conn.transaction()?;
        let since = match Self::since_tx(&tx, collection.id)?.into_option() {
            Some(since) => since,
            None => {
                return Err(StashError::from(
                    "cannot iterate collection with empty since frontier",
                ));
            }
        };
        let mut rows = tx
            .prepare(
                "SELECT key, value, time, diff FROM data
                 WHERE collection_id = $collection_id",
            )?
            .query_and_then(named_params! {"$collection_id": collection.id}, |row| {
                let key_buf: Vec<_> = row.get("key")?;
                let value_buf: Vec<_> = row.get("value")?;
                let key: K = serde_json::from_slice(&key_buf)?;
                let value: V = serde_json::from_slice(&value_buf)?;
                let time = row.get("time")?;
                let diff = row.get("diff")?;
                Ok::<_, StashError>(((key, value), cmp::max(time, since), diff))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        differential_dataflow::consolidation::consolidate_updates(&mut rows);
        Ok(rows)
    }

    async fn iter_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Vec<(V, Timestamp, Diff)>, StashError>
    where
        K: Data,
        V: Data,
    {
        let key = serde_json::to_vec(key).expect("must serialize");
        let tx = self.conn.transaction()?;
        let since = match Self::since_tx(&tx, collection.id)?.into_option() {
            Some(since) => since,
            None => {
                return Err(StashError::from(
                    "cannot iterate collection with empty since frontier",
                ));
            }
        };
        let mut rows = tx
            .prepare(
                "SELECT value, time, diff FROM data
                 WHERE collection_id = $collection_id AND key = $key",
            )?
            .query_and_then(
                named_params! {
                    "$collection_id": collection.id,
                    "$key": key,
                },
                |row| {
                    let value_buf: Vec<_> = row.get("value")?;
                    let value: V = serde_json::from_slice(&value_buf)?;
                    let time = row.get("time")?;
                    let diff = row.get("diff")?;
                    Ok::<_, StashError>((value, cmp::max(time, since), diff))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        differential_dataflow::consolidation::consolidate_updates(&mut rows);
        Ok(rows)
    }

    async fn update_many<K, V, I>(
        &mut self,
        collection: StashCollection<K, V>,
        entries: I,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
        I: IntoIterator<Item = ((K, V), Timestamp, Diff)> + Send,
        I::IntoIter: Send,
    {
        let entries = entries.into_iter().map(|((key, value), time, diff)| {
            let key = serde_json::to_value(&key).expect("must serialize");
            let value = serde_json::to_value(&value).expect("must serialize");
            ((key, value), time, diff)
        });
        let tx = self.conn.transaction()?;
        Self::update_many_tx(&tx, collection.id, entries)?;
        tx.commit()?;
        Ok(())
    }

    async fn seal<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        new_upper: AntichainRef<'_, Timestamp>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.seal_batch(&[(collection, new_upper.to_owned())]).await
    }

    async fn seal_batch<K, V>(
        &mut self,
        seals: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        let seals = seals
            .iter()
            .map(|(collection, frontier)| (collection.id, frontier));
        let tx = self.conn.transaction()?;
        Self::seal_batch_tx(&tx, seals)?;
        tx.commit()?;
        Ok(())
    }

    async fn compact<'a, K, V>(
        &'a mut self,
        collection: StashCollection<K, V>,
        new_since: AntichainRef<'a, Timestamp>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.compact_batch(&[(collection, new_since.to_owned())])
            .await
    }

    async fn compact_batch<K, V>(
        &mut self,
        compactions: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        let tx = self.conn.transaction()?;
        Self::compact_batch_tx(
            &tx,
            compactions
                .iter()
                .map(|(collection, since)| (collection.id, since)),
        )?;
        tx.commit()?;
        Ok(())
    }

    async fn consolidate(&mut self, collection: Id) -> Result<(), StashError> {
        self.consolidate_batch(&[collection]).await
    }

    async fn consolidate_batch(&mut self, collections: &[Id]) -> Result<(), StashError> {
        let tx = self.conn.transaction()?;
        Self::consolidate_batch_tx(&tx, collections)?;
        tx.commit()?;
        Ok(())
    }

    /// Reports the current since frontier.
    async fn since<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data,
    {
        let tx = self.conn.transaction()?;
        let since = Self::since_tx(&tx, collection.id)?;
        tx.commit()?;
        Ok(since)
    }

    /// Reports the current upper frontier.
    async fn upper<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError>
    where
        K: Data,
        V: Data,
    {
        let tx = self.conn.transaction()?;
        let upper = Self::upper_tx(&tx, collection.id)?;
        tx.commit()?;
        Ok(upper)
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
    async fn append_batch(&mut self, batches: &[AppendBatch]) -> Result<(), StashError> {
        let tx = self.conn.transaction()?;
        for batch in batches {
            let upper = Self::upper_tx(&tx, batch.collection_id)?;
            if upper != batch.lower {
                return Err("unexpected lower".into());
            }
            Self::update_many_tx(&tx, batch.collection_id, batch.entries.clone().into_iter())?;
            Self::seal_batch_tx(&tx, std::iter::once((batch.collection_id, &batch.upper)))?;
            Self::compact_batch_tx(&tx, std::iter::once((batch.collection_id, &batch.compact)))?;
        }
        tx.commit()?;
        Ok(())
    }
}
