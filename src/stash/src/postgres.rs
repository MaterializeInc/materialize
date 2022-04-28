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

use postgres::{Client, Transaction};
use timely::progress::Antichain;
use timely::PartialOrder;

use mz_persist_types::Codec;
use timely::progress::frontier::AntichainRef;

use crate::{
    AntichainFormatter, Append, AppendBatch, Diff, Id, InternalStashError, Stash, StashCollection,
    StashError, Timestamp,
};

const SCHEMA: &str = "
CREATE TABLE fence (
    epoch bigint PRIMARY KEY
);
INSERT INTO fence VALUES (1);

CREATE TABLE collections (
    collection_id bigserial PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE data (
    collection_id bigint NOT NULL REFERENCES collections (collection_id),
    key bytea NOT NULL,
    value bytea NOT NULL,
    time bigint NOT NULL,
    diff bigint NOT NULL
);

CREATE INDEX data_time_idx ON data (collection_id, time);

CREATE TABLE sinces (
    collection_id bigint NOT NULL UNIQUE REFERENCES collections (collection_id),
    since bigint
);

CREATE TABLE uppers (
    collection_id bigint NOT NULL UNIQUE REFERENCES collections (collection_id),
    upper bigint
);
";

/// A Stash whose data is stored in a Postgres database. The format of the
/// tables are not specified and should not be relied upon. The only promise is
/// stability. Any changes to the table schemas will be accompanied by a clear
pub struct Postgres {
    conn: Client,
    epoch: i64,
}

impl Postgres {
    /// Opens the stash stored at the specified path.
    pub fn open(mut conn: Client) -> Result<Postgres, StashError> {
        conn.batch_execute("SET default_transaction_isolation = serializable")?;
        let mut tx = conn.transaction()?;
        let fence_oid: Option<u32> = tx
            // `to_regclass` returns the regclass (OID) of the named object (table) or NULL
            // if it doesn't exist. This is a check for "does the fence table exist".
            .query_one("SELECT to_regclass('fence')::oid", &[])?
            .get(0);
        if fence_oid.is_none() {
            tx.batch_execute(SCHEMA)?;
        }
        // Bump the epoch, which will cause any previous connection to fail.
        let epoch = tx
            .query_one("UPDATE fence SET epoch=epoch+1 RETURNING epoch", &[])?
            .get(0);
        tx.commit()?;
        Ok(Postgres { conn, epoch })
    }

    /// Construct a fenced transaction.
    fn transact<F, T>(&mut self, f: F) -> Result<T, StashError>
    where
        F: FnOnce(&mut Transaction) -> Result<T, StashError>,
    {
        let mut tx = self.conn.transaction()?;
        let current: i64 = tx.query_one("SELECT epoch FROM fence", &[])?.get(0);
        if current != self.epoch {
            return Err(InternalStashError::Fence(format!(
                "unexpected fence {}, expected {}",
                current, self.epoch
            ))
            .into());
        }
        let res = f(&mut tx)?;
        tx.commit()?;
        Ok(res)
    }

    fn since_tx(
        tx: &mut Transaction,
        collection_id: Id,
    ) -> Result<Antichain<Timestamp>, StashError> {
        let since: Option<Timestamp> = tx
            .query_one(
                "SELECT since FROM sinces WHERE collection_id = $1",
                &[&collection_id],
            )?
            .get("since");
        Ok(Antichain::from_iter(since))
    }

    fn upper_tx(
        tx: &mut Transaction,
        collection_id: Id,
    ) -> Result<Antichain<Timestamp>, StashError> {
        let upper: Option<Timestamp> = tx
            .query_one(
                "SELECT upper FROM uppers WHERE collection_id = $1",
                &[&collection_id],
            )?
            .get("upper");
        Ok(Antichain::from_iter(upper))
    }

    fn seal_batch_tx<'a, I>(tx: &mut Transaction, seals: I) -> Result<(), StashError>
    where
        I: Iterator<Item = (Id, &'a Antichain<Timestamp>)>,
    {
        let update_stmt = tx.prepare("UPDATE uppers SET upper = $1 WHERE collection_id = $2")?;
        for (collection_id, new_upper) in seals {
            let upper = Self::upper_tx(tx, collection_id)?;
            if PartialOrder::less_than(new_upper, &upper) {
                return Err(StashError::from(format!(
                    "seal request {} is less than the current upper frontier {}",
                    AntichainFormatter(new_upper),
                    AntichainFormatter(&upper),
                )));
            }
            tx.execute(&update_stmt, &[&new_upper.as_option(), &collection_id])?;
        }
        Ok(())
    }

    fn update_many_tx<I>(
        tx: &mut Transaction,
        collection_id: Id,
        entries: I,
    ) -> Result<(), StashError>
    where
        I: Iterator<Item = ((Vec<u8>, Vec<u8>), Timestamp, Diff)>,
    {
        let upper = Self::upper_tx(tx, collection_id)?;
        let insert_stmt = tx.prepare(
            "INSERT INTO data (collection_id, key, value, time, diff)
             VALUES ($1, $2, $3, $4, $5)",
        )?;
        for ((key, value), time, diff) in entries {
            if !upper.less_equal(&time) {
                return Err(StashError::from(format!(
                    "entry time {} is less than the current upper frontier {}",
                    time,
                    AntichainFormatter(&upper)
                )));
            }
            tx.execute(&insert_stmt, &[&collection_id, &key, &value, &time, &diff])?;
        }
        Ok(())
    }
}

impl Stash for Postgres {
    fn collection<K, V>(&mut self, name: &str) -> Result<StashCollection<K, V>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord,
    {
        self.transact(|tx| {
            let collection_id_opt: Option<_> = tx
                .query_one(
                    "SELECT collection_id FROM collections WHERE name = $1",
                    &[&name],
                )
                .map(|row| row.get("collection_id"))
                .ok();

            let collection_id = match collection_id_opt {
                Some(id) => id,
                None => {
                    let collection_id = tx
                        .query_one(
                            "INSERT INTO collections (name) VALUES ($1) RETURNING collection_id",
                            &[&name],
                        )?
                        .get("collection_id");
                    tx.execute(
                        "INSERT INTO sinces (collection_id, since) VALUES ($1, $2)",
                        &[&collection_id, &Timestamp::MIN],
                    )?;
                    tx.execute(
                        "INSERT INTO uppers (collection_id, upper) VALUES ($1, $2)",
                        &[&collection_id, &Timestamp::MIN],
                    )?;
                    collection_id
                }
            };

            Ok(StashCollection {
                id: collection_id,
                _kv: PhantomData,
            })
        })
    }

    fn iter<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Vec<((K, V), Timestamp, Diff)>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord,
    {
        self.transact(|tx| {
            let since = match Self::since_tx(tx, collection.id)?.into_option() {
                Some(since) => since,
                None => {
                    return Err(StashError::from(
                        "cannot iterate collection with empty since frontier",
                    ));
                }
            };
            let mut rows = tx
                .query(
                    "SELECT key, value, time, diff FROM data
                    WHERE collection_id = $1",
                    &[&collection.id],
                )?
                .into_iter()
                .map(|row| {
                    let key_buf: Vec<_> = row.try_get("key")?;
                    let value_buf: Vec<_> = row.try_get("value")?;
                    let key = K::decode(&key_buf)?;
                    let value = V::decode(&value_buf)?;
                    let time = row.try_get("time")?;
                    let diff = row.try_get("diff")?;
                    Ok::<_, StashError>(((key, value), cmp::max(time, since), diff))
                })
                .collect::<Result<Vec<_>, _>>()?;
            differential_dataflow::consolidation::consolidate_updates(&mut rows);
            Ok(rows)
        })
    }

    fn iter_key<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        key: &K,
    ) -> Result<Vec<(V, Timestamp, Diff)>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord,
    {
        let mut key_buf = vec![];
        key.encode(&mut key_buf);
        self.transact(|tx| {
            let since = match Self::since_tx(tx, collection.id)?.into_option() {
                Some(since) => since,
                None => {
                    return Err(StashError::from(
                        "cannot iterate collection with empty since frontier",
                    ));
                }
            };
            let mut rows = tx
                .query(
                    "SELECT value, time, diff FROM data
                    WHERE collection_id = $1 AND key = $2",
                    &[&collection.id, &key_buf],
                )?
                .into_iter()
                .map(|row| {
                    let value_buf: Vec<_> = row.try_get("value")?;
                    let value = V::decode(&value_buf)?;
                    let time = row.try_get("time")?;
                    let diff = row.try_get("diff")?;
                    Ok::<_, StashError>((value, cmp::max(time, since), diff))
                })
                .collect::<Result<Vec<_>, _>>()?;
            differential_dataflow::consolidation::consolidate_updates(&mut rows);
            Ok(rows)
        })
    }

    fn update_many<K: Codec, V: Codec, I>(
        &mut self,
        collection: StashCollection<K, V>,
        entries: I,
    ) -> Result<(), StashError>
    where
        I: IntoIterator<Item = ((K, V), Timestamp, Diff)>,
    {
        let entries = entries.into_iter().map(|((key, value), time, diff)| {
            let mut key_buf = vec![];
            let mut value_buf = vec![];
            key.encode(&mut key_buf);
            value.encode(&mut value_buf);
            ((key_buf, value_buf), time, diff)
        });
        self.transact(|tx| Self::update_many_tx(tx, collection.id, entries))
    }

    fn seal<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        new_upper: AntichainRef<Timestamp>,
    ) -> Result<(), StashError> {
        self.seal_batch(&[(collection, new_upper.to_owned())])
    }

    fn seal_batch<K, V>(
        &mut self,
        seals: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError> {
        let seals = seals
            .iter()
            .map(|(collection, frontier)| (collection.id, frontier));
        self.transact(|tx| Self::seal_batch_tx(tx, seals))
    }

    fn compact<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
        new_since: AntichainRef<Timestamp>,
    ) -> Result<(), StashError> {
        self.compact_batch(&[(collection, new_since.to_owned())])
    }

    fn compact_batch<K, V>(
        &mut self,
        compactions: &[(StashCollection<K, V>, Antichain<Timestamp>)],
    ) -> Result<(), StashError> {
        self.transact(|tx| {
            let compact_stmt =
                tx.prepare("UPDATE sinces SET since = $1 WHERE collection_id = $2")?;
            for (collection, new_since) in compactions {
                let since = Self::since_tx(tx, collection.id)?;
                let upper = Self::upper_tx(tx, collection.id)?;
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
                tx.execute(&compact_stmt, &[&new_since.as_option(), &collection.id])?;
            }
            Ok(())
        })
    }

    fn consolidate<K, V>(&mut self, collection: StashCollection<K, V>) -> Result<(), StashError> {
        self.consolidate_batch(&[collection])
    }

    fn consolidate_batch<K, V>(
        &mut self,
        collections: &[StashCollection<K, V>],
    ) -> Result<(), StashError> {
        self.transact(|tx| {
            let consolidation_stmt = tx.prepare(
                "DELETE FROM data
                WHERE collection_id = $1 AND time <= $2
                RETURNING key, value, diff",
            )?;
            let insert_stmt = tx.prepare(
                "INSERT INTO data (collection_id, key, value, time, diff)
                VALUES ($1, $2, $3, $4, $5)",
            )?;
            let drop_stmt = tx.prepare("DELETE FROM data WHERE collection_id = $1")?;

            for collection in collections {
                let since = Self::since_tx(tx, collection.id)?.into_option();
                match since {
                    Some(since) => {
                        let mut updates = tx
                            .query(&consolidation_stmt, &[&collection.id, &since])?
                            .into_iter()
                            .map(|row| {
                                let key = row.try_get("key")?;
                                let value = row.try_get("value")?;
                                let diff = row.try_get("diff")?;
                                Ok::<_, StashError>(((key, value), since, diff))
                            })
                            .collect::<Result<Vec<((Vec<u8>, Vec<u8>), i64, i64)>, _>>()?;
                        differential_dataflow::consolidation::consolidate_updates(&mut updates);
                        for ((key, value), time, diff) in updates {
                            tx.execute(
                                &insert_stmt,
                                &[&collection.id, &key, &value, &time, &diff],
                            )?;
                        }
                    }
                    None => {
                        tx.execute(&drop_stmt, &[&collection.id])?;
                    }
                }
            }

            Ok(())
        })
    }

    /// Reports the current since frontier.
    fn since<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError> {
        self.transact(|tx| Self::since_tx(tx, collection.id))
    }

    /// Reports the current upper frontier.
    fn upper<K, V>(
        &mut self,
        collection: StashCollection<K, V>,
    ) -> Result<Antichain<Timestamp>, StashError> {
        self.transact(|tx| Self::upper_tx(tx, collection.id))
    }
}

impl From<postgres::Error> for StashError {
    fn from(e: postgres::Error) -> StashError {
        StashError {
            inner: InternalStashError::Postgres(e),
        }
    }
}

impl Append for Postgres {
    fn append<I>(&mut self, batches: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = AppendBatch>,
    {
        self.transact(|tx| {
            for batch in batches {
                let upper = Self::upper_tx(tx, batch.collection_id)?;
                if upper != batch.lower {
                    return Err("unexpected lower".into());
                }
                Self::update_many_tx(tx, batch.collection_id, batch.entries.into_iter())?;
                Self::seal_batch_tx(tx, std::iter::once((batch.collection_id, &batch.upper)))?;
            }
            Ok(())
        })
    }
}
