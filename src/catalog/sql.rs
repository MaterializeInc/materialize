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
CREATE TABLE IF NOT EXISTS databases (
    id   int PRIMARY KEY,
    name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS schemas (
    id          int PRIMARY KEY,
    database_id int REFERENCES databases,
    name        text NOT NULL,
    UNIQUE (database_id, name)
);

CREATE TABLE IF NOT EXISTS items (
    gid        blob PRIMARY KEY,
    schema_id  int REFERENCES schemas,
    name       text NOT NULL,
    definition blob NOT NULL,
    UNIQUE (schema_id, name)
);

REPLACE INTO databases VALUES (1, 'materialize');
REPLACE INTO schemas VALUES
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
        let sqlite = match path {
            Some(path) => rusqlite::Connection::open(path)?,
            None => rusqlite::Connection::open_in_memory()?,
        };

        let app_id: i32 = sqlite.query_row("PRAGMA application_id", params![], |row| row.get(0))?;
        let bootstrapped = if app_id == 0 {
            sqlite.execute(
                &format!("PRAGMA application_id = {}", APPLICATION_ID),
                params![],
            )?;
            true
        } else if app_id == APPLICATION_ID {
            false
        } else {
            bail!("incorrect application_id in catalog");
        };

        // Create the on-disk schema, if it doesn't already exist.
        sqlite.execute_batch(&SCHEMA)?;

        Ok(Connection {
            inner: sqlite,
            bootstrapped,
        })
    }

    pub fn load_databases(&self) -> Result<Vec<(i32, String)>, failure::Error> {
        self.inner
            .prepare("SELECT id, name FROM databases")?
            .query_and_then(params![], |row| -> Result<_, failure::Error> {
                let id: i32 = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((id, name))
            })?
            .collect()
    }

    pub fn load_schemas(&self) -> Result<Vec<(i32, Option<String>, String)>, failure::Error> {
        self.inner
            .prepare(
                "SELECT schemas.id, databases.name, schemas.name
                FROM schemas
                LEFT JOIN databases ON schemas.database_id = databases.id",
            )?
            .query_and_then(params![], |row| -> Result<_, failure::Error> {
                let id: i32 = row.get(0)?;
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

    pub fn insert_item(
        &self,
        id: GlobalId,
        schema_id: i32,
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
