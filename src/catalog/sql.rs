// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use failure::bail;
use rusqlite::params;
use rusqlite::types::{FromSql, FromSqlError, ToSql, ToSqlOutput, Value, ValueRef};
use serde::{Deserialize, Serialize};

use expr::GlobalId;

use crate::names::{DatabaseSpecifier, FullName};

const APPLICATION_ID: i32 = 0x1854_47dc;

const SCHEMA: &str = "
CREATE TABLE gid_alloc (
    next_gid integer NOT NULL
);

CREATE TABLE databases (
    id   integer PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE schemas (
    id          integer PRIMARY KEY,
    database_id integer REFERENCES databases,
    name        text NOT NULL,
    UNIQUE (database_id, name)
);

CREATE TABLE items (
    gid        blob PRIMARY KEY,
    schema_id  integer REFERENCES schemas,
    name       text NOT NULL,
    definition blob NOT NULL,
    UNIQUE (schema_id, name)
);

INSERT INTO gid_alloc VALUES (1);
INSERT INTO databases VALUES (1, 'materialize');
INSERT INTO schemas VALUES
    (1, NULL, 'mz_catalog'),
    (2, NULL, 'pg_catalog'),
    (3, 1, 'public');
";

#[derive(Debug)]
pub struct Connection {
    inner: rusqlite::Connection,
}

impl Connection {
    pub fn open(path: Option<&Path>) -> Result<Connection, failure::Error> {
        let mut sqlite = match path {
            Some(path) => rusqlite::Connection::open(path)?,
            None => rusqlite::Connection::open_in_memory()?,
        };
        let tx = sqlite.transaction()?;
        let app_id: i32 = tx.query_row("PRAGMA application_id", params![], |row| row.get(0))?;
        if app_id == 0 {
            tx.execute(
                &format!("PRAGMA application_id = {}", APPLICATION_ID),
                params![],
            )?;
            // Create the on-disk schema, since it doesn't already exist.
            tx.execute_batch(&SCHEMA)?;
            true
        } else if app_id == APPLICATION_ID {
            false
        } else {
            bail!("incorrect application_id in catalog");
        };
        tx.commit()?;

        Ok(Connection { inner: sqlite })
    }

    pub fn load_databases(&self) -> Result<Vec<(i64, String)>, failure::Error> {
        self.inner
            .prepare("SELECT id, name FROM databases")?
            .query_and_then(params![], |row| -> Result<_, failure::Error> {
                let id: i64 = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((id, name))
            })?
            .collect()
    }

    pub fn load_schemas(&self) -> Result<Vec<(i64, Option<String>, String)>, failure::Error> {
        self.inner
            .prepare(
                "SELECT schemas.id, databases.name, schemas.name
                FROM schemas
                LEFT JOIN databases ON schemas.database_id = databases.id",
            )?
            .query_and_then(params![], |row| -> Result<_, failure::Error> {
                let id: i64 = row.get(0)?;
                let database_name: Option<String> = row.get(1)?;
                let schema_name: String = row.get(2)?;
                Ok((id, database_name, schema_name))
            })?
            .collect()
    }

    pub fn load_items(&self) -> Result<Vec<(GlobalId, FullName, Vec<u8>)>, failure::Error> {
        self.inner
            .prepare(
                "SELECT items.gid, databases.name, schemas.name, items.name, items.definition
                FROM items
                JOIN schemas ON items.schema_id = schemas.id
                JOIN databases ON schemas.database_id = databases.id
                ORDER BY items.rowid",
            )?
            .query_and_then(params![], |row| -> Result<_, failure::Error> {
                let id: SqlVal<GlobalId> = row.get(0)?;
                let database: Option<String> = row.get(1)?;
                let schema: String = row.get(2)?;
                let item: String = row.get(3)?;
                let definition: Vec<u8> = row.get(4)?;
                Ok((
                    id.0,
                    FullName {
                        database: DatabaseSpecifier::from(database),
                        schema,
                        item,
                    },
                    definition,
                ))
            })?
            .collect()
    }

    pub fn allocate_id(&mut self) -> Result<GlobalId, failure::Error> {
        let tx = self.inner.transaction()?;
        // SQLite doesn't support u64s, so we constrain ourselves to the more
        // limited range of positive i64s.
        let id: i64 = tx.query_row("SELECT next_gid FROM gid_alloc", params![], |row| {
            row.get(0)
        })?;
        if id == i64::max_value() {
            bail!("catalog id exhaustion: id counter overflows an i64");
        }
        tx.execute("UPDATE gid_alloc SET next_gid = ?", params![id + 1])?;
        tx.commit()?;
        Ok(GlobalId::User(id as u64))
    }

    pub fn transaction(&mut self) -> Result<Transaction, failure::Error> {
        Ok(Transaction {
            inner: self.inner.transaction()?,
        })
    }
}

pub struct Transaction<'a> {
    inner: rusqlite::Transaction<'a>,
}

impl Transaction<'_> {
    pub fn load_database_id(&self, database_name: &str) -> Result<i64, failure::Error> {
        match self
            .inner
            .prepare_cached("SELECT id FROM databases WHERE name = ?")?
            .query_row(params![database_name], |row| row.get(0))
        {
            Ok(id) => Ok(id),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                bail!("unknown database '{}'", database_name);
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn load_schema_id(
        &self,
        database_id: i64,
        schema_name: &str,
    ) -> Result<i64, failure::Error> {
        match self
            .inner
            .prepare_cached("SELECT id FROM schemas WHERE database_id = ? AND name = ?")?
            .query_row(params![database_id, schema_name], |row| row.get(0))
        {
            Ok(id) => Ok(id),
            Err(rusqlite::Error::QueryReturnedNoRows) => bail!("unknown schema '{}'", schema_name),
            Err(err) => Err(err.into()),
        }
    }

    pub fn insert_database(&mut self, database_name: &str) -> Result<i64, failure::Error> {
        match self
            .inner
            .prepare_cached("INSERT INTO databases (name) VALUES (?)")?
            .execute(params![database_name])
        {
            Ok(_) => Ok(self.inner.last_insert_rowid()),
            Err(err) if is_constraint_violation(&err) => {
                bail!("database '{}' already exists", database_name);
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn insert_schema(
        &mut self,
        database_id: i64,
        schema_name: &str,
    ) -> Result<i64, failure::Error> {
        match self
            .inner
            .prepare_cached("INSERT INTO schemas (database_id, name) VALUES (?, ?)")?
            .execute(params![database_id, schema_name])
        {
            Ok(_) => Ok(self.inner.last_insert_rowid()),
            Err(err) if is_constraint_violation(&err) => {
                bail!("schema '{}' already exists", schema_name);
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn insert_item(
        &self,
        id: GlobalId,
        schema_id: i64,
        item_name: &str,
        item: &[u8],
    ) -> Result<(), failure::Error> {
        match self
            .inner
            .prepare_cached(
                "INSERT INTO items (gid, schema_id, name, definition) VALUES (?, ?, ?, ?)",
            )?
            .execute(params![SqlVal(&id), schema_id, item_name, item])
        {
            Ok(_) => Ok(()),
            Err(err) if is_constraint_violation(&err) => {
                bail!("catalog item '{}' already exists", item_name);
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn remove_database(&self, name: &str) -> Result<(), failure::Error> {
        let n = self
            .inner
            .prepare_cached("DELETE FROM databases WHERE name = ?")?
            .execute(params![name])?;
        assert!(n <= 1);
        if n != 1 {
            bail!("database '{}' does not exist", name);
        }
        Ok(())
    }

    pub fn remove_schema(&self, database_id: i64, schema_name: &str) -> Result<(), failure::Error> {
        let n = self
            .inner
            .prepare_cached("DELETE FROM schemas WHERE database_id = ? AND name = ?")?
            .execute(params![database_id, schema_name])?;
        assert!(n <= 1);
        if n != 1 {
            bail!("schema '{}' does not exist", schema_name);
        }
        Ok(())
    }

    pub fn remove_item(&self, id: GlobalId) -> Result<(), failure::Error> {
        let n = self
            .inner
            .prepare_cached("DELETE FROM items WHERE gid = ?")?
            .execute(params![SqlVal(id)])?;
        assert!(n <= 1);
        if n != 1 {
            bail!("item {} does not exist", id);
        }
        Ok(())
    }

    pub fn commit(self) -> Result<(), rusqlite::Error> {
        self.inner.commit()
    }
}

fn is_constraint_violation(err: &rusqlite::Error) -> bool {
    match err {
        rusqlite::Error::SqliteFailure(err, _) => {
            err.code == rusqlite::ErrorCode::ConstraintViolation
        }
        _ => false,
    }
}

pub struct SqlVal<T>(pub T);

impl<T> ToSql for SqlVal<T>
where
    T: Serialize,
{
    fn to_sql(&self) -> Result<ToSqlOutput, rusqlite::Error> {
        let bytes = serde_json::to_vec(&self.0)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        Ok(ToSqlOutput::Owned(Value::Blob(bytes)))
    }
}

impl<T> FromSql for SqlVal<T>
where
    T: for<'de> Deserialize<'de>,
{
    fn column_result(val: ValueRef) -> Result<Self, FromSqlError> {
        let bytes = match val {
            ValueRef::Blob(bytes) => bytes,
            _ => return Err(FromSqlError::InvalidType),
        };
        Ok(SqlVal(
            serde_json::from_slice(bytes).map_err(|err| FromSqlError::Other(Box::new(err)))?,
        ))
    }
}
