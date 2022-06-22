// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Durable metadata storage.

use std::cmp;
use std::marker::PhantomData;
use std::path::Path;

use async_trait::async_trait;
use rusqlite::{named_params, params, Connection, OptionalExtension, Transaction};
use timely::progress::Antichain;
use timely::PartialOrder;

use mz_persist_types::Codec;
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
            let upper = Self::upper_tx(&tx, collection_id)?;
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
        I: Iterator<Item = ((Vec<u8>, Vec<u8>), Timestamp, Diff)>,
    {
        let upper = Self::upper_tx(&tx, collection_id)?;
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
            let since = Self::since_tx(&tx, collection_id)?;
            let upper = Self::upper_tx(&tx, collection_id)?;
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

    fn consolidate_batch_tx<'a, I>(tx: &Transaction<'a>, collections: I) -> Result<(), StashError>
    where
        I: Iterator<Item = Id>,
    {
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
            let since = Self::since_tx(&tx, collection_id)?.into_option();
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
                let key = K::decode(&key_buf)?;
                let value = V::decode(&value_buf)?;
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
        let mut key_buf = vec![];
        key.encode(&mut key_buf);
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
                    "$key": key_buf,
                },
                |row| {
                    let value_buf: Vec<_> = row.get("value")?;
                    let value = V::decode(&value_buf)?;
                    let time = row.get("time")?;
                    let diff = row.get("diff")?;
                    Ok::<_, StashError>((value, cmp::max(time, since), diff))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        differential_dataflow::consolidation::consolidate_updates(&mut rows);
        Ok(rows)
    }

    async fn update_many<K: Codec, V: Codec, I>(
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
            let mut key_buf = vec![];
            let mut value_buf = vec![];
            key.encode(&mut key_buf);
            value.encode(&mut value_buf);
            ((key_buf, value_buf), time, diff)
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

    async fn consolidate<'a, K, V>(
        &'a mut self,
        collection: StashCollection<K, V>,
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        self.consolidate_batch(&[collection]).await
    }

    async fn consolidate_batch<K, V>(
        &mut self,
        collections: &[StashCollection<K, V>],
    ) -> Result<(), StashError>
    where
        K: Data,
        V: Data,
    {
        let tx = self.conn.transaction()?;
        Self::consolidate_batch_tx(&tx, collections.iter().map(|collection| collection.id))?;
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
        let mut consolidate = Vec::new();
        for batch in batches {
            consolidate.push(batch.collection_id);
            let upper = Self::upper_tx(&tx, batch.collection_id)?;
            if upper != batch.lower {
                return Err("unexpected lower".into());
            }
            Self::update_many_tx(&tx, batch.collection_id, batch.entries.into_iter())?;
            Self::seal_batch_tx(&tx, std::iter::once((batch.collection_id, &batch.upper)))?;
            Self::compact_batch_tx(&tx, std::iter::once((batch.collection_id, &batch.compact)))?;
        }
        Self::consolidate_batch_tx(&tx, consolidate.iter().map(|id| *id))?;
        tx.commit()?;
        Ok(())
    }
}
