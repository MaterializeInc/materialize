// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use itertools::Itertools;
use rusqlite::params;
use rusqlite::types::{FromSql, FromSqlError, ToSql, ToSqlOutput, Value, ValueRef};
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};

use mz_dataflow_types::sources::MzOffset;
use mz_expr::{GlobalId, PartitionId};
use mz_ore::cast::CastFrom;
use mz_ore::soft_assert_eq;
use mz_repr::Timestamp;
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_sql::names::{DatabaseSpecifier, FullName};
use mz_sql::plan::ComputeInstanceConfig;
use uuid::Uuid;

use crate::catalog::error::{Error, ErrorKind};

const APPLICATION_ID: i32 = 0x1854_47dc;

/// Schema migrations for the on-disk state.
const MIGRATIONS: &[&str] = &[
    // Creates initial schema.
    //
    // Introduced for v0.1.0.
    "CREATE TABLE gid_alloc (
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

     CREATE TABLE timestamps (
         sid blob NOT NULL,
         vid blob NOT NULL,
         timestamp integer NOT NULL,
         offset blob NOT NULL,
         PRIMARY KEY (sid, vid, timestamp)
     );

     INSERT INTO gid_alloc VALUES (1);
     INSERT INTO databases VALUES (1, 'materialize');
     INSERT INTO schemas VALUES
         (1, NULL, 'mz_catalog'),
         (2, NULL, 'pg_catalog'),
         (3, 1, 'public');",
    // Adjusts timestamp table to support multi-partition Kafka topics.
    //
    // Introduced for v0.1.4.
    //
    // ATTENTION: this migration blows away data and must not be used as a model
    // for future migrations! It is only acceptable now because we have not yet
    // made any consistency promises to users.
    "DROP TABLE timestamps;
     CREATE TABLE timestamps (
        sid blob NOT NULL,
        vid blob NOT NULL,
        pcount blob NOT NULL,
        pid blob NOT NULL,
        timestamp integer NOT NULL,
        offset blob NOT NULL,
        PRIMARY KEY (sid, vid, pid, timestamp)
    );",
    // Introduces settings table to support persistent node settings.
    //
    // Introduced in v0.4.0.
    "CREATE TABLE settings (
        name TEXT PRIMARY KEY,
        value TEXT
    );",
    // Creates the roles table and a default "materialize" user.
    //
    // Introduced in v0.7.0.
    "CREATE TABLE roles (
        id   integer PRIMARY KEY,
        name text NOT NULL UNIQUE
    );
    INSERT INTO roles VALUES (1, 'materialize');",
    // Makes the mz_internal schema literal so it can store functions.
    //
    // Introduced in v0.7.0.
    "INSERT INTO schemas (database_id, name) VALUES
        (NULL, 'mz_internal');",
    // Adjusts timestamp table to support replayable source timestamp bindings.
    //
    // Introduced for v0.7.4
    //
    // ATTENTION: this migration blows away data and must not be used as a model
    // for future migrations! It is only acceptable now because we have not yet
    // made any consistency promises to users.
    "DROP TABLE timestamps;
     CREATE TABLE timestamps (
        sid blob NOT NULL,
        pid blob NOT NULL,
        timestamp integer NOT NULL,
        offset blob NOT NULL,
        PRIMARY KEY (sid, pid, timestamp, offset)
    );",
    // Makes the information_schema schema literal so it can store functions.
    //
    // Introduced in v0.9.12.
    "INSERT INTO schemas (database_id, name) VALUES
        (NULL, 'information_schema');",
    // Adds index to timestamp table to more efficiently compact timestamps.
    //
    // Introduced in v0.12.0.
    "CREATE INDEX timestamps_sid_timestamp ON timestamps (sid, timestamp)",
    // Adds table to track users' compute instances.
    //
    // Introduced in v0.22.0.
    "CREATE TABLE compute_instances (
        id   integer PRIMARY KEY,
        name text NOT NULL UNIQUE
    );
    INSERT INTO compute_instances VALUES (1, 'default');",
    // Introduced in v0.24.0.
    "ALTER TABLE compute_instances ADD COLUMN config text",
    // Add new migrations here.
    //
    // Migrations should be preceded with a comment of the following form:
    //
    //     > Short summary of migration's purpose.
    //     >
    //     > Introduced in <VERSION>.
    //     >
    //     > Optional additional commentary about safety or approach.
    //
    // Please include @benesch on any code reviews that add or edit migrations.
    // Migrations must preserve backwards compatibility with all past releases
    // of materialized. Migrations can be edited up until they ship in a
    // release, after which they must never be removed, only patched by future
    // migrations.
];

#[derive(Debug)]
pub struct Connection {
    inner: rusqlite::Connection,
    experimental_mode: bool,
    cluster_id: Uuid,
}

impl Connection {
    pub fn open(path: &Path, experimental_mode: Option<bool>) -> Result<Connection, Error> {
        let mut sqlite = rusqlite::Connection::open(path)?;

        // Validate application ID.
        let tx = sqlite.transaction()?;
        let app_id: i32 = tx.query_row("PRAGMA application_id", params![], |row| row.get(0))?;
        if app_id == 0 {
            // Fresh catalog, so install the correct ID. We also apply the
            // zeroth migration for historical reasons: the default
            // `user_version` of zero indicates that the zeroth migration has
            // been applied.
            tx.execute_batch(&format!("PRAGMA application_id = {}", APPLICATION_ID))?;
            tx.execute_batch(MIGRATIONS[0])?;
        } else if app_id != APPLICATION_ID {
            return Err(Error::new(ErrorKind::Corruption {
                detail: "catalog file has incorrect application_id".into(),
            }));
        };
        tx.commit()?;

        // Run unapplied migrations. The `user_version` field stores the index
        // of the last migration that was run.
        let version: u32 = sqlite.query_row("PRAGMA user_version", params![], |row| row.get(0))?;
        for (i, sql) in MIGRATIONS
            .iter()
            .enumerate()
            .skip(usize::cast_from(version) + 1)
        {
            let tx = sqlite.transaction()?;
            tx.execute_batch(sql)?;
            tx.execute_batch(&format!("PRAGMA user_version = {}", i))?;
            tx.commit()?;
        }

        Ok(Connection {
            experimental_mode: Self::set_or_get_experimental_mode(&mut sqlite, experimental_mode)?,
            cluster_id: Self::set_or_get_cluster_id(&mut sqlite)?,
            inner: sqlite,
        })
    }

    /// Sets catalog's `experimental_mode` setting on initialization or gets
    /// that value.
    ///
    /// Note that using `None` for `experimental_mode` is appropriate when
    /// reading the catalog outside the context of starting the server.
    ///
    /// # Errors
    ///
    /// - If server was initialized and `experimental_mode.unwrap()` does not
    ///   match the initialized value.
    ///
    ///   This means that experimental mode:
    ///   - Can only be enabled on initialization
    ///   - Cannot be disabled once enabled
    ///
    /// # Panics
    ///
    /// - If server has not been initialized and `experimental_mode.is_none()`.
    fn set_or_get_experimental_mode(
        sqlite: &mut rusqlite::Connection,
        experimental_mode: Option<bool>,
    ) -> Result<bool, Error> {
        let tx = sqlite.transaction()?;
        let current_setting: Option<String> = tx
            .query_row(
                "SELECT value FROM settings WHERE name = 'experimental_mode';",
                params![],
                |row| row.get(0),
            )
            .optional()?;

        let res = match (current_setting, experimental_mode) {
            // Server init
            (None, Some(experimental_mode)) => {
                tx.execute(
                    "INSERT INTO settings VALUES ('experimental_mode', ?);",
                    params![experimental_mode],
                )?;
                Ok(experimental_mode)
            }
            // Server reboot
            (Some(cs), Some(experimental_mode)) => {
                let current_setting = cs.parse::<usize>().unwrap() != 0;
                if current_setting && !experimental_mode {
                    // Setting is true but was not given `--experimental` flag.
                    Err(Error::new(ErrorKind::ExperimentalModeRequired))
                } else if !current_setting && experimental_mode {
                    // Setting is false but was given `--experimental` flag.
                    Err(Error::new(ErrorKind::ExperimentalModeUnavailable))
                } else {
                    Ok(experimental_mode)
                }
            }
            // Reading existing catalog
            (Some(cs), None) => Ok(cs.parse::<usize>().unwrap() != 0),
            // Test code that doesn't care. Just disable experimental mode.
            (None, None) => Ok(false),
        };
        tx.commit()?;
        res
    }

    /// Sets catalog's `cluster_id` setting on initialization or gets that value.
    fn set_or_get_cluster_id(sqlite: &mut rusqlite::Connection) -> Result<Uuid, Error> {
        let tx = sqlite.transaction()?;
        let current_setting: Option<SqlVal<Uuid>> = tx
            .query_row(
                "SELECT value FROM settings WHERE name = 'cluster_id';",
                params![],
                |row| row.get(0),
            )
            .optional()?;

        let res = match current_setting {
            // Server init
            None => {
                // Generate a new version 4 UUID. These are generated from random input.
                let cluster_id = Uuid::new_v4();
                tx.execute(
                    "INSERT INTO settings VALUES ('cluster_id', ?);",
                    params![SqlVal(cluster_id)],
                )?;
                Ok(cluster_id)
            }
            // Server reboot
            Some(cs) => Ok(cs.0),
        };
        tx.commit()?;
        res
    }

    pub fn get_catalog_content_version(&mut self) -> Result<String, Error> {
        let tx = self.inner.transaction()?;
        let current_setting: Option<String> = tx
            .query_row(
                "SELECT value FROM settings WHERE name = 'catalog_content_version';",
                params![],
                |row| row.get(0),
            )
            .optional()?;
        let version = match current_setting {
            Some(v) => match v.parse::<u32>() {
                // Prior to v0.8.4 catalog content versions was stored as a u32
                Ok(_) => "pre-v0.8.4".to_string(),
                Err(_) => v,
            },
            None => "new".to_string(),
        };
        tx.commit()?;
        Ok(version)
    }

    pub fn set_catalog_content_version(&mut self, new_version: &str) -> Result<(), Error> {
        let tx = self.inner.transaction()?;
        tx.execute(
            "INSERT INTO settings (name, value) VALUES ('catalog_content_version', ?)
                    ON CONFLICT (name) DO UPDATE SET value=excluded.value;",
            params![new_version],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn load_databases(&self) -> Result<Vec<(i64, String)>, Error> {
        self.inner
            .prepare("SELECT id, name FROM databases")?
            .query_and_then(params![], |row| -> Result<_, Error> {
                let id: i64 = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((id, name))
            })?
            .collect()
    }

    pub fn load_schemas(&self) -> Result<Vec<(i64, Option<String>, String)>, Error> {
        self.inner
            .prepare(
                "SELECT schemas.id, databases.name, schemas.name
                FROM schemas
                LEFT JOIN databases ON schemas.database_id = databases.id",
            )?
            .query_and_then(params![], |row| -> Result<_, Error> {
                let id: i64 = row.get(0)?;
                let database_name: Option<String> = row.get(1)?;
                let schema_name: String = row.get(2)?;
                Ok((id, database_name, schema_name))
            })?
            .collect()
    }

    pub fn load_roles(&self) -> Result<Vec<(i64, String)>, Error> {
        self.inner
            .prepare("SELECT id, name FROM roles")?
            .query_and_then(params![], |row| -> Result<_, Error> {
                let id: i64 = row.get(0)?;
                let name: String = row.get(1)?;
                Ok((id, name))
            })?
            .collect()
    }

    pub fn load_compute_instances(
        &self,
    ) -> Result<Vec<(i64, String, ComputeInstanceConfig)>, Error> {
        self.inner
            .prepare("SELECT id, name, config FROM compute_instances")?
            .query_and_then(params![], |row| -> Result<_, Error> {
                let id: i64 = row.get(0)?;
                let name: String = row.get(1)?;
                let config: Option<String> = row.get(2)?;
                let config: ComputeInstanceConfig = match config {
                    None => ComputeInstanceConfig::Virtual,
                    Some(config) => serde_json::from_str(&config)
                        .map_err(|err| rusqlite::Error::from(FromSqlError::Other(Box::new(err))))?,
                };
                Ok((id, name, config))
            })?
            .collect()
    }

    pub fn allocate_id(&mut self) -> Result<GlobalId, Error> {
        let tx = self.inner.transaction()?;
        // SQLite doesn't support u64s, so we constrain ourselves to the more
        // limited range of positive i64s.
        let id: i64 = tx.query_row("SELECT next_gid FROM gid_alloc", params![], |row| {
            row.get(0)
        })?;
        if id == i64::max_value() {
            return Err(Error::new(ErrorKind::IdExhaustion));
        }
        tx.execute("UPDATE gid_alloc SET next_gid = ?", params![id + 1])?;
        tx.commit()?;
        Ok(GlobalId::User(id as u64))
    }

    pub fn transaction(&mut self) -> Result<Transaction, Error> {
        Ok(Transaction {
            inner: self.inner.transaction()?,
        })
    }

    pub fn cluster_id(&self) -> Uuid {
        self.cluster_id
    }

    pub fn experimental_mode(&self) -> bool {
        self.experimental_mode
    }
}

pub struct Transaction<'a> {
    inner: rusqlite::Transaction<'a>,
}

impl Transaction<'_> {
    pub fn load_items(&self) -> Result<Vec<(GlobalId, FullName, Vec<u8>)>, Error> {
        // Order user views by their GlobalId
        self.inner
            .prepare(
                "SELECT items.gid, databases.name, schemas.name, items.name, items.definition
                FROM items
                JOIN schemas ON items.schema_id = schemas.id
                JOIN databases ON schemas.database_id = databases.id
                ORDER BY json_extract(items.gid, '$.User')",
            )?
            .query_and_then(params![], |row| -> Result<_, Error> {
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

    pub fn load_database_id(&self, database_name: &str) -> Result<i64, Error> {
        match self
            .inner
            .prepare_cached("SELECT id FROM databases WHERE name = ?")?
            .query_row(params![database_name], |row| row.get(0))
        {
            Ok(id) => Ok(id),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(SqlCatalogError::UnknownDatabase(database_name.to_owned()).into())
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn load_schema_id(&self, database_id: i64, schema_name: &str) -> Result<i64, Error> {
        match self
            .inner
            .prepare_cached("SELECT id FROM schemas WHERE database_id = ? AND name = ?")?
            .query_row(params![database_id, schema_name], |row| row.get(0))
        {
            Ok(id) => Ok(id),
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                Err(SqlCatalogError::UnknownSchema(schema_name.to_owned()).into())
            }
            Err(err) => Err(err.into()),
        }
    }

    fn validate_timestamp_bindings(&self, source_id: &GlobalId) -> Result<(), String> {
        let bindings_vec = self
            .load_timestamp_bindings(*source_id)
            .map_err(|e| format!("{}", e))?;

        let bindings_by_pid = bindings_vec.iter().group_by(|(pid, _ts, _offset)| pid);

        for (pid, bindings) in &bindings_by_pid {
            let mut latest_offset = 0;
            let mut latest_ts = 0;
            for (_pid, ts, offset) in bindings {
                if offset.offset < latest_offset {
                    return Err(format!(
                        "Unexpected offset {} for pid {}. All bindings: {:?}",
                        offset, pid, bindings_vec
                    ));
                }
                if *ts < latest_ts {
                    return Err(format!(
                        "Timestamps should not be decreasing but got {} for pid {}. All bindings: {:?}",
                        ts, pid, bindings_vec
                    ));
                }
                latest_offset = offset.offset;
                latest_ts = *ts;
            }
        }

        Ok(())
    }

    pub fn load_timestamp_bindings(
        &self,
        source_id: GlobalId,
    ) -> Result<Vec<(PartitionId, Timestamp, MzOffset)>, Error> {
        self.inner
            .prepare_cached(
                "SELECT pid, timestamp, offset from timestamps where sid = ? order by pid, timestamp")?
            .query_and_then(params![SqlVal(&source_id)], |row| -> Result<_, Error> {
                let partition: PartitionId = row.get::<_, String>(0)?.parse().expect("parsing partition id from string cannot fail");
                let timestamp: Timestamp = row.get(1)?;
                let offset = MzOffset {
                    offset: row.get(2)?,
                };

                Ok((partition, timestamp, offset))
            })?
            .collect()
    }

    pub fn insert_database(&mut self, database_name: &str) -> Result<i64, Error> {
        match self
            .inner
            .prepare_cached("INSERT INTO databases (name) VALUES (?)")?
            .execute(params![database_name])
        {
            Ok(_) => Ok(self.inner.last_insert_rowid()),
            Err(err) if is_constraint_violation(&err) => Err(Error::new(
                ErrorKind::DatabaseAlreadyExists(database_name.to_owned()),
            )),
            Err(err) => Err(err.into()),
        }
    }

    pub fn insert_schema(&mut self, database_id: i64, schema_name: &str) -> Result<i64, Error> {
        match self
            .inner
            .prepare_cached("INSERT INTO schemas (database_id, name) VALUES (?, ?)")?
            .execute(params![database_id, schema_name])
        {
            Ok(_) => Ok(self.inner.last_insert_rowid()),
            Err(err) if is_constraint_violation(&err) => Err(Error::new(
                ErrorKind::SchemaAlreadyExists(schema_name.to_owned()),
            )),
            Err(err) => Err(err.into()),
        }
    }

    pub fn insert_role(&mut self, role_name: &str) -> Result<i64, Error> {
        match self
            .inner
            .prepare_cached("INSERT INTO roles (name) VALUES (?)")?
            .execute(params![role_name])
        {
            Ok(_) => Ok(self.inner.last_insert_rowid()),
            Err(err) if is_constraint_violation(&err) => Err(Error::new(
                ErrorKind::RoleAlreadyExists(role_name.to_owned()),
            )),
            Err(err) => Err(err.into()),
        }
    }

    pub fn insert_compute_instance(
        &mut self,
        cluster_name: &str,
        config: &ComputeInstanceConfig,
    ) -> Result<i64, Error> {
        let config = serde_json::to_string(config)
            .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))?;
        match self
            .inner
            .prepare_cached("INSERT INTO compute_instances (name, config) VALUES (?, ?)")?
            .execute(params![cluster_name, config])
        {
            Ok(_) => Ok(self.inner.last_insert_rowid()),
            Err(err) if is_constraint_violation(&err) => Err(Error::new(
                ErrorKind::ClusterAlreadyExists(cluster_name.to_owned()),
            )),
            Err(err) => Err(err.into()),
        }
    }

    pub fn insert_item(
        &self,
        id: GlobalId,
        schema_id: i64,
        item_name: &str,
        item: &[u8],
    ) -> Result<(), Error> {
        match self
            .inner
            .prepare_cached(
                "INSERT INTO items (gid, schema_id, name, definition) VALUES (?, ?, ?, ?)",
            )?
            .execute(params![SqlVal(&id), schema_id, item_name, item])
        {
            Ok(_) => Ok(()),
            Err(err) if is_constraint_violation(&err) => Err(Error::new(
                ErrorKind::ItemAlreadyExists(item_name.to_owned()),
            )),
            Err(err) => Err(err.into()),
        }
    }

    pub fn insert_timestamp_binding(
        &self,
        source_id: &GlobalId,
        partition_id: &str,
        timestamp: Timestamp,
        offset: i64,
    ) -> Result<(), Error> {
        let result = self
            .inner
            .prepare_cached(
                "INSERT OR IGNORE INTO timestamps (sid, pid, timestamp, offset) VALUES (?, ?, ?, ?)",
            )?
              .execute(params![SqlVal(source_id), partition_id, timestamp, offset]);

        soft_assert_eq!(self.validate_timestamp_bindings(source_id), Ok(()));

        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub fn delete_timestamp_bindings(&self, source_id: GlobalId) -> Result<(), Error> {
        let result = self
            .inner
            .prepare_cached("DELETE FROM timestamps WHERE sid = ?")?
            .execute(params![SqlVal(&source_id)]);

        match result {
            Ok(_) => Ok(()),
            Err(err) => Err(err.into()),
        }
    }

    pub fn compact_timestamp_bindings(
        &self,
        source_id: GlobalId,
        frontier: Timestamp,
    ) -> Result<(), Error> {
        // we need to keep one binding that is not beyond the frontier, so that
        // on restart we don't emit timestamps that are beyond the previously
        // written consistency frontier. Otherwise, data with those timestamps
        // would get written again.
        let latest_not_beyond_compaction: Option<Timestamp> = self
            .inner
            .prepare_cached(
                "SELECT max(timestamp) FROM timestamps WHERE sid = ? AND timestamp <= ?",
            )?
            .query_row(params![SqlVal(&source_id), frontier], |row| row.get(0))?;

        if let Some(latest_not_beyond_compaction) = latest_not_beyond_compaction {
            let result = match self
                .inner
                .prepare_cached("DELETE FROM timestamps WHERE sid = ? AND timestamp < ?")?
                .execute(params![SqlVal(&source_id), latest_not_beyond_compaction])
            {
                Ok(_) => Ok(()),
                Err(err) => Err(err.into()),
            };

            result
        } else {
            Ok(())
        }
    }

    pub fn remove_database(&self, name: &str) -> Result<(), Error> {
        let n = self
            .inner
            .prepare_cached("DELETE FROM databases WHERE name = ?")?
            .execute(params![name])?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownDatabase(name.to_owned()).into())
        }
    }

    pub fn remove_schema(&self, database_id: i64, schema_name: &str) -> Result<(), Error> {
        let n = self
            .inner
            .prepare_cached("DELETE FROM schemas WHERE database_id = ? AND name = ?")?
            .execute(params![database_id, schema_name])?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownSchema(schema_name.to_owned()).into())
        }
    }

    pub fn remove_role(&self, name: &str) -> Result<(), Error> {
        let n = self
            .inner
            .prepare_cached("DELETE FROM roles WHERE name = ?")?
            .execute(params![name])?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownRole(name.to_owned()).into())
        }
    }

    pub fn remove_compute_instance(&self, name: &str) -> Result<(), Error> {
        let n = self
            .inner
            .prepare_cached("DELETE FROM compute_instances WHERE name = ?")?
            .execute(params![name])?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownComputeInstance(name.to_owned()).into())
        }
    }

    pub fn remove_item(&self, id: GlobalId) -> Result<(), Error> {
        let n = self
            .inner
            .prepare_cached("DELETE FROM items WHERE gid = ?")?
            .execute(params![SqlVal(id)])?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
    }

    pub fn update_item(&self, id: GlobalId, item_name: &str, item: &[u8]) -> Result<(), Error> {
        let n = self
            .inner
            .prepare_cached("UPDATE items SET name = ?, definition = ? WHERE gid = ?")?
            .execute(params![item_name, item, SqlVal(id)])?;
        assert!(n <= 1);
        if n == 1 {
            Ok(())
        } else {
            Err(SqlCatalogError::UnknownItem(id.to_string()).into())
        }
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
