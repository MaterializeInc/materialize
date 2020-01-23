// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::path::Path;

use failure::bail;
use rusqlite::params;
use rusqlite::types::{FromSql, FromSqlError, ToSql, ToSqlOutput, Value, ValueRef};
use serde::{Deserialize, Serialize};

use expr::GlobalId;

use crate::names::{DatabaseSpecifier, FullName};
use crate::CatalogItem;

const APPLICATION_ID: i32 = 0x1854_47dc;

const SCHEMA: &str = "
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

INSERT INTO databases VALUES (1, 'materialize');
INSERT INTO schemas VALUES
    (1, NULL, 'mz_catalog'),
    (2, NULL, 'pg_catalog'),
    (3, 1, 'public');
";

#[derive(Debug)]
pub struct Connection {
    inner: rusqlite::Connection,
    bootstrapped: bool,
}

impl Connection {
    pub fn open(path: Option<&Path>) -> Result<Connection, failure::Error> {
        let mut sqlite = match path {
            Some(path) => rusqlite::Connection::open(path)?,
            None => rusqlite::Connection::open_in_memory()?,
        };

        let tx = sqlite.transaction()?;
        let app_id: i32 = tx.query_row("PRAGMA application_id", params![], |row| row.get(0))?;
        let bootstrapped = if app_id == 0 {
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

        Ok(Connection {
            inner: sqlite,
            bootstrapped,
        })
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

    pub fn load_items(&self) -> Result<Vec<(GlobalId, FullName, CatalogItem)>, failure::Error> {
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
                let definition: SqlVal<CatalogItem> = row.get(4)?;
                Ok((
                    id.0,
                    FullName {
                        database: DatabaseSpecifier::from(database),
                        schema,
                        item,
                    },
                    definition.0,
                ))
            })?
            .collect()
    }

    /// Inserts a new database with a default schema named "public". Returns
    /// the ID of the new database and the ID of the public schema.
    ///
    // TODO(benesch): if we exposed a transactional API, the caller could
    // manually call `insert_schema`, so we wouldn't need to hardcode the
    // creation of the public schema here.
    pub fn insert_database(&mut self, database_name: &str) -> Result<(i64, i64), failure::Error> {
        self.inner
            .prepare_cached("INSERT INTO databases (name) VALUES (?)")?
            .execute(params![database_name])?;
        let database_id = self.inner.last_insert_rowid();
        self.inner
            .prepare_cached("INSERT INTO schemas (database_id, name) VALUES (?, 'public')")?
            .execute(params![database_id])?;
        let schema_id = self.inner.last_insert_rowid();
        Ok((database_id, schema_id))
    }

    pub fn insert_schema(
        &mut self,
        database_id: i64,
        schema_name: &str,
    ) -> Result<i64, failure::Error> {
        let mut stmt = self
            .inner
            .prepare_cached("INSERT INTO schemas (database_id, name) VALUES (?, ?)")?;
        stmt.execute(params![database_id, schema_name])?;
        Ok(self.inner.last_insert_rowid())
    }

    pub fn insert_item(
        &self,
        id: GlobalId,
        schema_id: i64,
        item_name: &str,
        item: &CatalogItem,
    ) -> Result<(), failure::Error> {
        let mut stmt = self.inner.prepare_cached(
            "INSERT INTO items (gid, schema_id, name, definition)
                VALUES (?, ?, ?, ?)",
        )?;
        stmt.execute(params![SqlVal(&id), schema_id, item_name, SqlVal(item)])?;
        Ok(())
    }

    pub fn remove_item(&self, id: GlobalId) {
        let mut stmt = self
            .inner
            .prepare_cached("DELETE FROM items WHERE gid = ?")
            .expect("catalog: sqlite failed");
        stmt.execute(params![SqlVal(id)])
            .expect("catalog: sqlite failed");
    }

    pub fn bootstrapped(&self) -> bool {
        self.bootstrapped
    }
}

struct SqlVal<T>(pub T);

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
