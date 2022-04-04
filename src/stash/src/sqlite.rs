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
use std::sync::{Arc, Mutex};

use rusqlite::{named_params, params, Connection, OptionalExtension, Transaction};
use timely::progress::Antichain;
use timely::PartialOrder;

use mz_persist_types::Codec;
use timely::progress::frontier::AntichainRef;

use crate::{
    AntichainFormatter, Diff, Id, InternalStashError, StashCollection, StashConn, StashError,
    Timestamp,
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
    diff integer NOT NULL,
    UNIQUE (collection_id, key, value, time)
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
    conn: Arc<Mutex<Connection>>,
}

impl Sqlite {
    /// Opens the stash stored at the specified path.
    pub fn open(path: &Path) -> Result<Sqlite, StashError> {
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
            return Err(StashError::from(format!(
                "invalid application id: {}",
                app_id
            )));
        }
        tx.commit()?;
        Ok(Sqlite {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn since_tx(
        &self,
        tx: &Transaction,
        collection_id: Id,
    ) -> Result<Antichain<Timestamp>, StashError> {
        let since: Option<Timestamp> = tx.query_row(
            "SELECT since FROM sinces WHERE collection_id = $collection_id",
            named_params! {"$collection_id": collection_id},
            |row| row.get("since"),
        )?;
        Ok(Antichain::from_iter(since))
    }

    fn upper_tx(
        &self,
        tx: &Transaction,
        collection_id: Id,
    ) -> Result<Antichain<Timestamp>, StashError> {
        let upper: Option<Timestamp> = tx.query_row(
            "SELECT upper FROM uppers WHERE collection_id = $collection_id",
            named_params! {"$collection_id": collection_id},
            |row| row.get("upper"),
        )?;
        Ok(Antichain::from_iter(upper))
    }
}

impl StashConn for Sqlite {
    fn collection<K, V>(&self, name: &str) -> Result<StashCollection<Self, K, V>, StashError>
    where
        K: Codec + Ord,
        V: Codec + Ord,
    {
        let mut conn = self.conn.lock().expect("lock poisoned");
        let tx = conn.transaction()?;

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
            conn: Self {
                conn: Arc::clone(&self.conn),
            },
            collection_id,
            _kv: PhantomData,
        })
    }

    fn iter(
        &self,
        collection_id: Id,
    ) -> Result<Vec<((Vec<u8>, Vec<u8>), Timestamp, Diff)>, StashError> {
        let mut conn = self.conn.lock().expect("lock poisoned");
        let tx = conn.transaction()?;
        let since = match self.since_tx(&tx, collection_id)?.into_option() {
            Some(since) => since,
            None => {
                return Err(StashError::from(
                    "cannot iterate collection with empty since frontier",
                ));
            }
        };
        let rows = tx
            .prepare(
                "SELECT key, value, time, diff FROM data
                 WHERE collection_id = $collection_id",
            )?
            .query_and_then(named_params! {"$collection_id": collection_id}, |row| {
                let key = row.get("key")?;
                let value = row.get("value")?;
                let time = row.get("time")?;
                let diff = row.get("diff")?;
                Ok::<_, StashError>(((key, value), cmp::max(time, since), diff))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn iter_key(
        &self,
        collection_id: Id,
        key: &[u8],
    ) -> Result<Vec<(Vec<u8>, Timestamp, Diff)>, StashError> {
        let mut conn = self.conn.lock().expect("lock poisoned");
        let tx = conn.transaction()?;
        let since = match self.since_tx(&tx, collection_id)?.into_option() {
            Some(since) => since,
            None => {
                return Err(StashError::from(
                    "cannot iterate collection with empty since frontier",
                ));
            }
        };
        let rows = tx
            .prepare(
                "SELECT value, time, diff FROM data
                 WHERE collection_id = $collection_id AND key = $key",
            )?
            .query_and_then(
                named_params! {
                    "$collection_id": collection_id,
                    "$key": key,
                },
                |row| {
                    let value = row.get("value")?;
                    let time = row.get("time")?;
                    let diff = row.get("diff")?;
                    Ok::<_, StashError>((value, cmp::max(time, since), diff))
                },
            )?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    fn update_many<I>(&self, collection_id: Id, entries: I) -> Result<(), StashError>
    where
        I: IntoIterator<Item = ((Vec<u8>, Vec<u8>), Timestamp, Diff)>,
    {
        let mut conn = self.conn.lock().expect("lock poisoned");
        let tx = conn.transaction()?;
        let upper = self.upper_tx(&tx, collection_id)?;
        let mut insert_stmt = tx.prepare(
            "INSERT INTO data (collection_id, key, value, time, diff)
             VALUES ($collection_id, $key, $value, $time, $diff)
             ON CONFLICT (collection_id, key, value, time) DO UPDATE SET diff = diff + excluded.diff",
        )?;
        let mut delete_stmt = tx.prepare(
            "DELETE FROM data
             WHERE collection_id = $collection_id AND key = $key AND value = $value AND time = $time AND diff = 0",
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
            delete_stmt.execute(named_params! {
                "$collection_id": collection_id,
                "$key": key,
                "$value": value,
                "$time": time,
            })?;
        }
        drop(insert_stmt);
        drop(delete_stmt);
        tx.commit()?;
        Ok(())
    }

    fn seal(
        &self,
        collection_id: Id,
        new_upper: AntichainRef<Timestamp>,
    ) -> Result<(), StashError> {
        let mut conn = self.conn.lock().expect("lock poisoned");
        let tx = conn.transaction()?;
        let upper = self.upper_tx(&tx, collection_id)?;
        if PartialOrder::less_than(&new_upper, &upper.borrow()) {
            return Err(StashError::from(format!(
                "seal request {} is less than the current upper frontier {}",
                AntichainFormatter(&new_upper),
                AntichainFormatter(&upper),
            )));
        }
        tx.execute(
            "UPDATE uppers SET upper = $upper WHERE collection_id = $collection_id",
            named_params! {"$upper": new_upper.as_option(), "$collection_id": collection_id},
        )?;
        tx.commit()?;
        Ok(())
    }

    fn compact(
        &self,
        collection_id: Id,
        new_since: AntichainRef<Timestamp>,
    ) -> Result<(), StashError> {
        let mut conn = self.conn.lock().expect("lock poisoned");
        let tx = conn.transaction()?;
        let since = self.since_tx(&tx, collection_id)?;
        let upper = self.upper_tx(&tx, collection_id)?;
        if PartialOrder::less_than(&upper.borrow(), &new_since) {
            return Err(StashError::from(format!(
                "compact request {} is greater than the current upper frontier {}",
                AntichainFormatter(&new_since),
                AntichainFormatter(&upper)
            )));
        }
        if PartialOrder::less_than(&new_since, &since.borrow()) {
            return Err(StashError::from(format!(
                "compact request {} is less than the current since frontier {}",
                AntichainFormatter(&new_since),
                AntichainFormatter(&since)
            )));
        }
        tx.execute(
            "UPDATE sinces SET since = $since WHERE collection_id = $collection_id",
            named_params! {"$since": new_since.as_option(), "$collection_id": collection_id},
        )?;
        tx.commit()?;
        Ok(())
    }

    fn consolidate(&self, collection_id: Id) -> Result<(), StashError> {
        let mut conn = self.conn.lock().expect("lock poisoned");
        let tx = conn.transaction()?;
        let since = self.since_tx(&tx, collection_id)?.into_option();
        match since {
            Some(since) => {
                tx.execute(
                    "INSERT INTO data (collection_id, key, value, time, diff)
                     SELECT collection_id, key, value, $since, sum(diff) FROM data
                     WHERE collection_id = $collection_id AND time < $since
                     GROUP BY key, value
                     ON CONFLICT (collection_id, key, value, time) DO UPDATE SET diff = diff + excluded.diff",
                    named_params! {
                        "$collection_id": collection_id,
                        "$since": since,
                    },
                )?;
                tx.execute(
                    "DELETE FROM data WHERE collection_id = $collection_id AND time < $since",
                    named_params! {
                        "$collection_id": collection_id,
                        "$since": since,
                    },
                )?;
            }
            None => {
                tx.execute(
                    "DELETE FROM data WHERE collection_id = $collection_id",
                    named_params! {
                        "$collection_id": collection_id,
                    },
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    /// Reports the current since frontier.
    fn since(&self, collection_id: Id) -> Result<Antichain<Timestamp>, StashError> {
        let mut conn = self.conn.lock().expect("lock poisoned");
        let tx = conn.transaction()?;
        let since = self.since_tx(&tx, collection_id)?;
        tx.commit()?;
        Ok(since)
    }

    /// Reports the current upper frontier.
    fn upper(&self, collection_id: Id) -> Result<Antichain<Timestamp>, StashError> {
        let mut conn = self.conn.lock().expect("lock poisoned");
        let tx = conn.transaction()?;
        let upper = self.upper_tx(&tx, collection_id)?;
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
