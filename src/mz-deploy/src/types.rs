// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data-contract system for external dependencies.
//!
//! When a project references objects it does not own (e.g. tables created by an
//! upstream ingestion pipeline), mz-deploy needs to know their column schemas so
//! it can type-check views that depend on them. This module manages that contract
//! through the `types.lock` file.
//!
//! ## Lock File Lifecycle
//!
//! 1. **Capture** — Column schemas are queried from the live environment and
//!    written to `types.lock`.
//! 2. **Compile** — The lock file is loaded and its schemas are used to resolve
//!    external dependency columns during compilation.
//! 3. **Validate** — During incremental typechecking, external dependency
//!    schemas are provided to the validation backend when dirty objects
//!    reference them.
//!
//! ## Compiler Integration
//!
//! Incremental runtime typechecking is owned by
//! [`crate::project::compiler::typecheck`]. That subsystem persists per-object
//! validation artifacts for consumers such as `explain` and the LSP.
//!
//! This module owns:
//!
//! - the `types.lock` contract format
//! - shared type/schema utilities such as [`type_hash`]
//!
//! ## Key Types
//!
//! - [`Types`] — In-memory representation of a `types.lock` file: a versioned
//!   map from fully-qualified object names to column schemas, plus optional
//!   object-level comments from `COMMENT ON` in the source database.
//! - [`ColumnType`] — A single column's type name, nullability, and optional
//!   `COMMENT ON COLUMN` description.

use crate::project::ir::object_id::ObjectId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use thiserror::Error;

/// The kind of database object recorded in a `types.lock` entry.
///
/// `TableFromSource` is treated as `Table` from a contract perspective — both
/// represent row-producing relations that can serve as FK targets.
#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum ObjectKind {
    Table,
    View,
    MaterializedView,
    Source,
    Sink,
    Secret,
    Connection,
}

impl FromStr for ObjectKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "table" => Ok(ObjectKind::Table),
            "view" => Ok(ObjectKind::View),
            "materialized-view" => Ok(ObjectKind::MaterializedView),
            "source" => Ok(ObjectKind::Source),
            "sink" => Ok(ObjectKind::Sink),
            "secret" => Ok(ObjectKind::Secret),
            "connection" => Ok(ObjectKind::Connection),
            _ => Err(format!("unknown object kind: {}", s)),
        }
    }
}

impl ObjectKind {
    /// Parse from the kebab-case string stored in SQLite.
    pub fn from_db_str(s: &str) -> Self {
        match s {
            "table" => ObjectKind::Table,
            "view" => ObjectKind::View,
            "materialized-view" => ObjectKind::MaterializedView,
            "source" => ObjectKind::Source,
            "sink" => ObjectKind::Sink,
            "secret" => ObjectKind::Secret,
            "connection" => ObjectKind::Connection,
            _ => ObjectKind::Table,
        }
    }

    /// Returns the kebab-case string matching the serde serialization format.
    pub fn as_str(self) -> &'static str {
        match self {
            ObjectKind::Table => "table",
            ObjectKind::View => "view",
            ObjectKind::MaterializedView => "materialized-view",
            ObjectKind::Source => "source",
            ObjectKind::Sink => "sink",
            ObjectKind::Secret => "secret",
            ObjectKind::Connection => "connection",
        }
    }
}

impl fmt::Display for ObjectKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectKind::Table => write!(f, "table"),
            ObjectKind::View => write!(f, "view"),
            ObjectKind::MaterializedView => write!(f, "materialized view"),
            ObjectKind::Source => write!(f, "source"),
            ObjectKind::Sink => write!(f, "sink"),
            ObjectKind::Secret => write!(f, "secret"),
            ObjectKind::Connection => write!(f, "connection"),
        }
    }
}

/// Directory name for mz-deploy build artifacts.
pub(crate) const BUILD_DIR: &str = "target";

/// Errors that can occur when reading, writing, or parsing `types.lock` files.
#[derive(Error, Debug)]
pub enum TypesError {
    #[error(transparent)]
    BuildArtifactFailed(#[from] crate::project::compiler::cache::CacheError),

    #[error("failed to read types.lock at {path}")]
    FileReadFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to write types.lock at {path}")]
    FileWriteFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse types.lock at {path}")]
    ParseFailed {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to create directory {path}")]
    DirectoryCreationFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    DependencyError(#[from] crate::project::error::DependencyError),
}

/// A single column's type name, nullability, and optional comment in a data contract.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ColumnType {
    /// SQL type syntax used when recreating cached dependencies as stub tables.
    pub r#type: String,
    pub nullable: bool,
    /// Original column position from the database schema.
    #[serde(default)]
    pub position: usize,
    /// Optional `COMMENT ON COLUMN` description from the source database.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// In-memory representation of a `types.lock` file.
///
/// Maps `ObjectId` (fully-qualified `database.schema.object`) to column
/// schemas. Used for type-checking views against external dependencies.
/// Optionally includes object-level and column-level comments from
/// `COMMENT ON` statements in the source database.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Types {
    pub version: u8,
    pub tables: BTreeMap<ObjectId, BTreeMap<String, ColumnType>>,
    pub kinds: BTreeMap<ObjectId, ObjectKind>,
    /// Object-level comments from `COMMENT ON` in the source database.
    #[serde(default)]
    pub comments: BTreeMap<ObjectId, String>,
}

impl Default for Types {
    fn default() -> Self {
        Types {
            version: 1,
            tables: BTreeMap::new(),
            kinds: BTreeMap::new(),
            comments: BTreeMap::new(),
        }
    }
}

/// TOML serialization format for types.lock
#[derive(Serialize, Deserialize)]
struct TypesLock {
    version: u8,
    #[serde(default)]
    table: Vec<ObjectLock>,
    #[serde(default)]
    view: Vec<ObjectLock>,
    #[serde(default, rename = "materialized-view")]
    materialized_view: Vec<ObjectLock>,
    #[serde(default)]
    source: Vec<ObjectLock>,
    #[serde(default)]
    sink: Vec<ObjectLock>,
    #[serde(default)]
    secret: Vec<ObjectLock>,
    #[serde(default)]
    connection: Vec<ObjectLock>,
}

impl Default for TypesLock {
    fn default() -> Self {
        Self {
            version: 1,
            table: vec![],
            view: vec![],
            materialized_view: vec![],
            source: vec![],
            sink: vec![],
            secret: vec![],
            connection: vec![],
        }
    }
}

impl TypesLock {
    /// Collect all objects paired with their kind into a vec.
    fn all_objects(&self) -> Vec<(ObjectKind, &ObjectLock)> {
        let mut result = Vec::new();
        for obj in &self.table {
            result.push((ObjectKind::Table, obj));
        }
        for obj in &self.view {
            result.push((ObjectKind::View, obj));
        }
        for obj in &self.materialized_view {
            result.push((ObjectKind::MaterializedView, obj));
        }
        for obj in &self.source {
            result.push((ObjectKind::Source, obj));
        }
        for obj in &self.sink {
            result.push((ObjectKind::Sink, obj));
        }
        for obj in &self.secret {
            result.push((ObjectKind::Secret, obj));
        }
        for obj in &self.connection {
            result.push((ObjectKind::Connection, obj));
        }
        result
    }

    /// Return a mutable reference to the vec for a given kind.
    fn vec_for_kind(&mut self, kind: ObjectKind) -> &mut Vec<ObjectLock> {
        match kind {
            ObjectKind::Table => &mut self.table,
            ObjectKind::View => &mut self.view,
            ObjectKind::MaterializedView => &mut self.materialized_view,
            ObjectKind::Source => &mut self.source,
            ObjectKind::Sink => &mut self.sink,
            ObjectKind::Secret => &mut self.secret,
            ObjectKind::Connection => &mut self.connection,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct ObjectLock {
    name: ObjectId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
    columns: Vec<ColumnLock>,
}

#[derive(Serialize, Deserialize)]
struct ColumnLock {
    name: String,
    #[serde(rename = "type")]
    r#type: String,
    nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
}

impl From<&Types> for TypesLock {
    fn from(types: &Types) -> Self {
        let mut lock = TypesLock {
            version: types.version,
            table: Vec::new(),
            view: Vec::new(),
            materialized_view: Vec::new(),
            source: Vec::new(),
            sink: Vec::new(),
            secret: Vec::new(),
            connection: Vec::new(),
        };

        for (id, columns) in &types.tables {
            let mut cols: Vec<_> = columns.iter().collect();
            cols.sort_by_key(|(_, ct)| ct.position);
            let cols: Vec<ColumnLock> = cols
                .into_iter()
                .map(|(col_name, col_type)| ColumnLock {
                    name: col_name.clone(),
                    r#type: col_type.r#type.clone(),
                    nullable: col_type.nullable,
                    comment: col_type.comment.clone(),
                })
                .collect();

            let kind = types
                .kinds
                .get(id)
                .unwrap_or_else(|| panic!("no kind for type {}", id.clone()));
            let comment = types.comments.get(id).cloned();

            let obj = ObjectLock {
                name: id.clone(),
                comment,
                columns: cols,
            };

            lock.vec_for_kind(*kind).push(obj);
        }

        lock
    }
}

impl From<TypesLock> for Types {
    fn from(lock: TypesLock) -> Self {
        let mut tables = BTreeMap::new();
        let mut kinds = BTreeMap::new();
        let mut comments = BTreeMap::new();

        for (kind, obj) in lock.all_objects() {
            let id = obj.name.clone();
            let mut columns = BTreeMap::new();
            for (position, col) in obj.columns.iter().enumerate() {
                columns.insert(
                    col.name.clone(),
                    ColumnType {
                        r#type: col.r#type.clone(),
                        nullable: col.nullable,
                        position,
                        comment: col.comment.clone(),
                    },
                );
            }
            kinds.insert(id.clone(), kind);
            if let Some(comment) = &obj.comment {
                comments.insert(id.clone(), comment.clone());
            }
            tables.insert(id, columns);
        }

        Types {
            version: lock.version,
            tables,
            kinds,
            comments,
        }
    }
}

/// Escape a string for use as a TOML basic string value.
fn escape_toml_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => {
                out.push_str(&format!("\\u{:04X}", u32::from(c)));
            }
            c => out.push(c),
        }
    }
    out
}

/// Hand-format a `TypesLock` as TOML with per-kind sections and inline columns.
fn write_toml(lock: &TypesLock) -> String {
    let mut out = String::new();
    out.push_str("# This file is automatically @generated by mz-deploy.\n");
    out.push_str("# It is not intended for manual editing.\n");
    out.push_str(&format!("version = {}\n", lock.version));

    let sections: &[(ObjectKind, &Vec<ObjectLock>)] = &[
        (ObjectKind::Secret, &lock.secret),
        (ObjectKind::Connection, &lock.connection),
        (ObjectKind::Source, &lock.source),
        (ObjectKind::Table, &lock.table),
        (ObjectKind::View, &lock.view),
        (ObjectKind::MaterializedView, &lock.materialized_view),
        (ObjectKind::Sink, &lock.sink),
    ];

    for (kind, objs) in sections {
        for obj in *objs {
            out.push('\n');
            out.push_str(&format!("[[{}]]\n", kind.as_str()));
            out.push_str(&format!(
                "name = \"{}\"\n",
                escape_toml_string(&obj.name.to_string())
            ));
            if let Some(comment) = &obj.comment {
                out.push_str(&format!("comment = \"{}\"\n", escape_toml_string(comment)));
            }
            out.push_str("columns = [\n");
            for col in &obj.columns {
                let mut parts = format!(
                    "name = \"{}\", type = \"{}\", nullable = {}",
                    escape_toml_string(&col.name),
                    escape_toml_string(&col.r#type),
                    col.nullable,
                );
                if let Some(comment) = &col.comment {
                    parts.push_str(&format!(", comment = \"{}\"", escape_toml_string(comment)));
                }
                out.push_str(&format!("    {{ {} }},\n", parts));
            }
            out.push_str("]\n");
        }
    }

    out
}

/// Load the types.lock file from the specified directory.
/// Returns an error if the file doesn't exist or cannot be parsed.
pub(crate) fn load_types_lock(directory: &Path) -> Result<Types, TypesError> {
    let path = directory.join("types.lock");

    let contents = fs::read_to_string(&path).map_err(|source| TypesError::FileReadFailed {
        path: path.clone(),
        source,
    })?;

    let lock: TypesLock =
        toml::from_str(&contents).map_err(|source| TypesError::ParseFailed { path, source })?;
    Ok(lock.into())
}

impl Types {
    /// Write the types.lock file to the specified directory.
    /// Overwrites any existing file at that location.
    pub fn write_types_lock(&self, directory: &Path) -> Result<(), TypesError> {
        let path = directory.join("types.lock");

        let lock = TypesLock::from(self);
        let contents = write_toml(&lock);

        fs::write(&path, contents).map_err(|source| TypesError::FileWriteFailed { path, source })
    }

    /// Get the column schema for an object.
    pub fn get_table(&self, id: &ObjectId) -> Option<&BTreeMap<String, ColumnType>> {
        self.tables.get(id)
    }

    /// Get the object kind for an object.
    ///
    /// Returns `Table` if the id is not in the kinds map, which can happen
    /// when `Types` is constructed programmatically (e.g., from `type_info`).
    pub fn get_kind(&self, id: &ObjectId) -> ObjectKind {
        self.kinds.get(id).copied().unwrap_or(ObjectKind::Table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn test_write_and_read_types_lock_round_trip() {
        let mut tables = BTreeMap::new();

        let mut order_cols = BTreeMap::new();
        order_cols.insert(
            "amount".to_string(),
            ColumnType {
                r#type: "numeric".to_string(),
                nullable: true,
                position: 0,
                comment: None,
            },
        );
        order_cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
                position: 1,
                comment: None,
            },
        );
        order_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: true,
                position: 2,
                comment: None,
            },
        );
        tables.insert("app.ingest.orders".parse::<ObjectId>().unwrap(), order_cols);

        let mut user_cols = BTreeMap::new();
        user_cols.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 0,
                comment: None,
            },
        );
        user_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
                position: 1,
                comment: None,
            },
        );
        tables.insert("app.ingest.users".parse::<ObjectId>().unwrap(), user_cols);

        let mut kinds = BTreeMap::new();
        kinds.insert(
            "app.ingest.orders".parse::<ObjectId>().unwrap(),
            ObjectKind::Table,
        );
        kinds.insert(
            "app.ingest.users".parse::<ObjectId>().unwrap(),
            ObjectKind::Table,
        );

        let types = Types {
            version: 1,
            tables,
            kinds,
            comments: BTreeMap::new(),
        };

        let dir = tempfile::tempdir().expect("failed to create temp dir");
        types
            .write_types_lock(dir.path())
            .expect("failed to write types.lock");

        let loaded = load_types_lock(dir.path()).expect("failed to load types.lock");
        assert_eq!(types, loaded);
    }

    #[test]
    fn test_round_trip_with_kind() {
        let mut tables = BTreeMap::new();
        let mut cols = BTreeMap::new();
        cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
                position: 0,
                comment: None,
            },
        );
        tables.insert(
            "app.ingest.orders".parse::<ObjectId>().unwrap(),
            cols.clone(),
        );
        tables.insert(
            "app.ingest.order_summary".parse::<ObjectId>().unwrap(),
            cols,
        );

        let mut kinds = BTreeMap::new();
        kinds.insert(
            "app.ingest.orders".parse::<ObjectId>().unwrap(),
            ObjectKind::Table,
        );
        kinds.insert(
            "app.ingest.order_summary".parse::<ObjectId>().unwrap(),
            ObjectKind::MaterializedView,
        );

        let types = Types {
            version: 1,
            tables,
            kinds,
            comments: BTreeMap::new(),
        };

        let dir = tempfile::tempdir().expect("failed to create temp dir");
        types
            .write_types_lock(dir.path())
            .expect("failed to write types.lock");

        let loaded = load_types_lock(dir.path()).expect("failed to load types.lock");
        assert_eq!(types, loaded);
    }

    #[test]
    fn test_round_trip_with_comments() {
        let mut tables = BTreeMap::new();
        let mut cols = BTreeMap::new();
        cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
                position: 0,
                comment: Some("Primary key".to_string()),
            },
        );
        cols.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
                position: 1,
                comment: None,
            },
        );
        tables.insert("app.ingest.orders".parse::<ObjectId>().unwrap(), cols);

        let mut kinds = BTreeMap::new();
        kinds.insert(
            "app.ingest.orders".parse::<ObjectId>().unwrap(),
            ObjectKind::Table,
        );

        let mut comments = BTreeMap::new();
        comments.insert(
            "app.ingest.orders".parse::<ObjectId>().unwrap(),
            "All incoming customer orders".to_string(),
        );

        let types = Types {
            version: 1,
            tables,
            kinds,
            comments,
        };

        let dir = tempfile::tempdir().expect("failed to create temp dir");
        types
            .write_types_lock(dir.path())
            .expect("failed to write types.lock");

        let loaded = load_types_lock(dir.path()).expect("failed to load types.lock");
        assert_eq!(types, loaded);
    }

    #[test]
    fn test_backward_compat_no_comments() {
        // A types.lock file without comment fields should parse successfully
        let toml = r#"
version = 1

[[table]]
name = "app.ingest.orders"
columns = [
    { name = "id", type = "integer", nullable = false },
]
"#;
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        fs::write(dir.path().join("types.lock"), toml).unwrap();

        let loaded = load_types_lock(dir.path()).expect("should parse without comments");
        assert_eq!(loaded.tables.len(), 1);
        assert!(loaded.comments.is_empty());
        let cols = loaded
            .tables
            .get(&"app.ingest.orders".parse::<ObjectId>().unwrap())
            .unwrap();
        assert!(cols.get("id").unwrap().comment.is_none());
    }
}
