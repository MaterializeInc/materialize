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
//! This module owns the `types.lock` contract format and the type vocabulary
//! it records.
//!
//! ## Key Types
//!
//! - [`Types`] — In-memory representation of a `types.lock` file: a map from
//!   fully-qualified object names to column schemas, plus optional
//!   object-level comments from `COMMENT ON` in the source database.
//! - [`ColumnType`] — A single column's type, nullability, and optional
//!   `COMMENT ON COLUMN` description.
//! - [`data_type::DataType`] — A column's type. Structural rather than a type
//!   name, because a record, an anonymous list, and an anonymous map have no
//!   spelling the SQL grammar accepts.
//! - [`stub`] — Turns a recorded schema back into a relation.

pub(crate) mod data_type;
pub(crate) mod stub;

pub(crate) use data_type::{DataType, RecordField};

use crate::project::ir::object_id::ObjectId;
use data_type::{FieldLock, TypeLock, TypeLockError};
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

/// Highest `types.lock` format this binary understands.
///
/// Version 2 records structured types; version 1 recorded a bare SQL type name
/// per column, which cannot express a record. A version 1 file still loads: its
/// type names parse as [`DataType::Named`], and only the columns whose name was
/// a pseudo-type token are unreconstructible.
pub(crate) const LOCK_VERSION: u8 = 2;

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
    #[error(
        "types.lock at {path} was written by a newer mz-deploy (format version {version}, this binary understands {supported}); upgrade mz-deploy"
    )]
    UnsupportedLockVersion {
        path: PathBuf,
        version: u8,
        supported: u8,
    },
    #[error("types.lock at {path}: column `{column}` of `{object}` has an invalid type")]
    InvalidColumnType {
        path: PathBuf,
        object: String,
        column: String,
        #[source]
        source: TypeLockError,
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

/// A single column's type, nullability, and optional comment in a data contract.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnType {
    /// The column's type, structured so that it survives the round trip through
    /// the lock file and the build artifact.
    pub r#type: DataType,
    pub nullable: bool,
    /// Original column position from the database schema.
    pub position: usize,
    /// Optional `COMMENT ON COLUMN` description from the source database.
    pub comment: Option<String>,
}

/// In-memory representation of a `types.lock` file.
///
/// Maps `ObjectId` (fully-qualified `database.schema.object`) to column
/// schemas. Used for type-checking views against external dependencies.
/// Optionally includes object-level and column-level comments from
/// `COMMENT ON` statements in the source database.
#[derive(Debug, Clone, PartialEq)]
pub struct Types {
    pub tables: BTreeMap<ObjectId, BTreeMap<String, ColumnType>>,
    pub kinds: BTreeMap<ObjectId, ObjectKind>,
    /// Object-level comments from `COMMENT ON` in the source database.
    pub comments: BTreeMap<ObjectId, String>,
}

impl Default for Types {
    fn default() -> Self {
        Types {
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
            version: LOCK_VERSION,
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
    /// Collect all objects paired with their kind, consuming the lock.
    fn into_objects(self) -> Vec<(ObjectKind, ObjectLock)> {
        let kinds = [
            (ObjectKind::Table, self.table),
            (ObjectKind::View, self.view),
            (ObjectKind::MaterializedView, self.materialized_view),
            (ObjectKind::Source, self.source),
            (ObjectKind::Sink, self.sink),
            (ObjectKind::Secret, self.secret),
            (ObjectKind::Connection, self.connection),
        ];
        kinds
            .into_iter()
            .flat_map(|(kind, objs)| objs.into_iter().map(move |obj| (kind, obj)))
            .collect()
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

/// On-disk form of a column: its name and nullability widened over the
/// `type`/`of`/`fields` keys of [`crate::types::data_type`].
#[derive(Serialize, Deserialize)]
struct ColumnLock {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    comment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    of: Option<Box<TypeLock>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    fields: Vec<FieldLock>,
}

impl ColumnLock {
    fn new(name: String, column: &ColumnType) -> Self {
        let (type_name, of, fields) = data_type::split_data_type(&column.r#type);
        ColumnLock {
            name,
            type_name,
            nullable: column.nullable,
            comment: column.comment.clone(),
            of,
            fields,
        }
    }

    fn into_column_type(self, position: usize) -> Result<(String, ColumnType), TypeLockError> {
        let r#type = data_type::join_data_type(self.type_name, self.of, self.fields)?;
        Ok((
            self.name,
            ColumnType {
                r#type,
                nullable: self.nullable,
                position,
                comment: self.comment,
            },
        ))
    }
}

impl From<&Types> for TypesLock {
    fn from(types: &Types) -> Self {
        let mut lock = TypesLock {
            version: LOCK_VERSION,
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
                .map(|(col_name, col_type)| ColumnLock::new(col_name.clone(), col_type))
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

impl TypesLock {
    /// Convert into the in-memory form, reporting the object and column of any
    /// type whose structural payload does not match its tag.
    fn into_types(self, path: &Path) -> Result<Types, TypesError> {
        let mut tables = BTreeMap::new();
        let mut kinds = BTreeMap::new();
        let mut comments = BTreeMap::new();
        for (kind, obj) in self.into_objects() {
            let id = obj.name;
            let mut columns = BTreeMap::new();
            for (position, col) in obj.columns.into_iter().enumerate() {
                let object = id.to_string();
                let column = col.name.clone();
                let (name, column_type) = col.into_column_type(position).map_err(|source| {
                    TypesError::InvalidColumnType {
                        path: path.to_path_buf(),
                        object,
                        column,
                        source,
                    }
                })?;
                columns.insert(name, column_type);
            }
            kinds.insert(id.clone(), kind);
            if let Some(comment) = obj.comment {
                comments.insert(id.clone(), comment);
            }
            tables.insert(id, columns);
        }

        Ok(Types {
            tables,
            kinds,
            comments,
        })
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
                write_column(&mut out, col, COLUMN_INDENT);
            }
            out.push_str("]\n");
        }
    }

    out
}

/// Indentation of a column entry inside a `columns` array.
const COLUMN_INDENT: usize = 4;

/// Render one column as a TOML inline table, breaking a record's field list
/// across lines.
///
/// A newline inside the nested `fields` array is legal TOML: it sits within an
/// array value, not between the inline table's own braces.
fn write_column(out: &mut String, col: &ColumnLock, indent: usize) {
    let pad = " ".repeat(indent);
    out.push_str(&pad);
    out.push_str(&format!(
        "{{ name = \"{}\", type = \"{}\", nullable = {}",
        escape_toml_string(&col.name),
        escape_toml_string(&col.type_name),
        col.nullable,
    ));
    if let Some(comment) = &col.comment {
        out.push_str(&format!(", comment = \"{}\"", escape_toml_string(comment)));
    }
    if let Some(of) = &col.of {
        out.push_str(", of = ");
        write_inline_type(out, of);
    }
    if !col.fields.is_empty() {
        out.push_str(", fields = [\n");
        for field in &col.fields {
            write_field(out, field, indent + COLUMN_INDENT);
        }
        out.push_str(&pad);
        out.push(']');
    }
    out.push_str(" },\n");
}

/// Render one record field. Fields carry no comment, so this is [`write_column`]
/// over the field's own keys.
fn write_field(out: &mut String, field: &FieldLock, indent: usize) {
    write_column(
        out,
        &ColumnLock {
            name: field.name.clone(),
            type_name: field.type_name.clone(),
            nullable: field.nullable,
            comment: None,
            of: field.of.clone(),
            fields: field.fields.clone(),
        },
        indent,
    );
}

/// Render a container's element type as a single-line inline table.
fn write_inline_type(out: &mut String, ty: &TypeLock) {
    out.push_str(&format!("{{ type = \"{}\"", escape_toml_string(&ty.name)));
    if let Some(of) = &ty.of {
        out.push_str(", of = ");
        write_inline_type(out, of);
    }
    if !ty.fields.is_empty() {
        out.push_str(", fields = [");
        for (i, field) in ty.fields.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let mut buf = String::new();
            write_field(&mut buf, field, 0);
            out.push_str(buf.trim_end().trim_end_matches(','));
        }
        out.push(']');
    }
    out.push_str(" }");
}

/// Load the types.lock file from the specified directory.
/// Returns an error if the file doesn't exist or cannot be parsed.
pub(crate) fn load_types_lock(directory: &Path) -> Result<Types, TypesError> {
    let path = directory.join("types.lock");

    let contents = fs::read_to_string(&path).map_err(|source| TypesError::FileReadFailed {
        path: path.clone(),
        source,
    })?;

    let lock: TypesLock = toml::from_str(&contents).map_err(|source| TypesError::ParseFailed {
        path: path.clone(),
        source,
    })?;
    if lock.version > LOCK_VERSION {
        return Err(TypesError::UnsupportedLockVersion {
            path,
            version: lock.version,
            supported: LOCK_VERSION,
        });
    }
    lock.into_types(&path)
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

    #[mz_ore::test]
    fn test_write_and_read_types_lock_round_trip() {
        let mut tables = BTreeMap::new();

        let mut order_cols = BTreeMap::new();
        order_cols.insert(
            "amount".to_string(),
            ColumnType {
                r#type: DataType::named("numeric"),
                nullable: true,
                position: 0,
                comment: None,
            },
        );
        order_cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: DataType::named("integer"),
                nullable: false,
                position: 1,
                comment: None,
            },
        );
        order_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: DataType::named("integer"),
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
                r#type: DataType::named("text"),
                nullable: true,
                position: 0,
                comment: None,
            },
        );
        user_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: DataType::named("integer"),
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

    #[mz_ore::test]
    fn test_round_trip_with_kind() {
        let mut tables = BTreeMap::new();
        let mut cols = BTreeMap::new();
        cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: DataType::named("integer"),
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

    #[mz_ore::test]
    fn test_round_trip_with_comments() {
        let mut tables = BTreeMap::new();
        let mut cols = BTreeMap::new();
        cols.insert(
            "id".to_string(),
            ColumnType {
                r#type: DataType::named("integer"),
                nullable: false,
                position: 0,
                comment: Some("Primary key".to_string()),
            },
        );
        cols.insert(
            "name".to_string(),
            ColumnType {
                r#type: DataType::named("text"),
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

    #[mz_ore::test]
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

    /// A record's fields are written as an array nested inside the column's
    /// inline table. The newlines are inside an array value, which TOML allows;
    /// this pins that the `toml` crate agrees.
    #[mz_ore::test]
    fn structured_types_round_trip_through_the_lock_file() {
        let payload = DataType::Record(vec![
            RecordField {
                name: "a".into(),
                r#type: DataType::named("integer"),
                nullable: false,
            },
            RecordField {
                name: "n".into(),
                r#type: DataType::Record(vec![RecordField {
                    name: "x".into(),
                    r#type: DataType::List(Box::new(DataType::named("uint8"))),
                    nullable: true,
                }]),
                nullable: true,
            },
        ]);
        let columns = BTreeMap::from([
            (
                "payload".to_string(),
                ColumnType {
                    r#type: payload,
                    nullable: false,
                    position: 0,
                    comment: Some("nested".into()),
                },
            ),
            (
                "tags".to_string(),
                ColumnType {
                    r#type: DataType::List(Box::new(DataType::named("text"))),
                    nullable: true,
                    position: 1,
                    comment: None,
                },
            ),
            (
                "grid".to_string(),
                ColumnType {
                    r#type: DataType::Array(Box::new(DataType::Map(Box::new(DataType::named(
                        "int4",
                    ))))),
                    nullable: true,
                    position: 2,
                    comment: None,
                },
            ),
        ]);

        let id: ObjectId = "app.public.events".parse().unwrap();
        let types = Types {
            tables: BTreeMap::from([(id.clone(), columns)]),
            kinds: BTreeMap::from([(id, ObjectKind::Table)]),
            comments: BTreeMap::new(),
        };

        let dir = tempfile::tempdir().expect("failed to create temp dir");
        types
            .write_types_lock(dir.path())
            .expect("failed to write types.lock");
        let loaded = load_types_lock(dir.path()).expect("failed to load types.lock");
        assert_eq!(types, loaded);
    }

    /// A lock file written before structured types still loads: its type names
    /// become plain named types.
    #[mz_ore::test]
    fn version_1_lock_file_loads() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        fs::write(
            dir.path().join("types.lock"),
            "version = 1\n\n[[table]]\nname = \"app.public.events\"\ncolumns = [\n    \
             { name = \"id\", type = \"integer\", nullable = true },\n]\n",
        )
        .unwrap();

        let loaded = load_types_lock(dir.path()).expect("a version 1 file still loads");
        let events = &loaded.tables[&"app.public.events".parse::<ObjectId>().unwrap()];
        assert_eq!(events["id"].r#type, DataType::named("integer"));
    }

    #[mz_ore::test]
    fn newer_lock_file_is_refused() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        fs::write(
            dir.path().join("types.lock"),
            format!("version = {}\n", LOCK_VERSION + 1),
        )
        .unwrap();

        let err = load_types_lock(dir.path()).expect_err("a newer format is refused");
        assert!(
            err.to_string().contains("upgrade mz-deploy"),
            "unexpected error: {err}"
        );
    }

    /// `lock` leaves a column at its pseudo-type token when it cannot probe the
    /// column, so that token has to survive a write and read back. Rejecting it
    /// would make the file `lock` just wrote unreadable, and every caller
    /// defaults a load failure to an empty contract.
    #[mz_ore::test]
    fn pseudo_type_tokens_round_trip() {
        let columns: BTreeMap<String, ColumnType> = ["record", "list", "map"]
            .into_iter()
            .enumerate()
            .map(|(position, token)| {
                (
                    token.to_string(),
                    ColumnType {
                        r#type: DataType::named(token),
                        nullable: true,
                        position,
                        comment: None,
                    },
                )
            })
            .collect();

        let id: ObjectId = "app.public.wide".parse().unwrap();
        let types = Types {
            tables: BTreeMap::from([(id.clone(), columns)]),
            kinds: BTreeMap::from([(id, ObjectKind::View)]),
            comments: BTreeMap::new(),
        };

        let dir = tempfile::tempdir().expect("failed to create temp dir");
        types
            .write_types_lock(dir.path())
            .expect("failed to write types.lock");
        let loaded = load_types_lock(dir.path()).expect("a pseudo token must load back");
        assert_eq!(types, loaded);
    }

    /// A version 1 file records those same tokens, since that is what
    /// `mz_columns.type` reported.
    #[mz_ore::test]
    fn version_1_pseudo_type_tokens_load() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        fs::write(
            dir.path().join("types.lock"),
            "version = 1\n\n[[table]]\nname = \"app.public.events\"\ncolumns = [\n    \
             { name = \"payload\", type = \"record\", nullable = true },\n    \
             { name = \"tags\", type = \"list\", nullable = true },\n]\n",
        )
        .unwrap();

        let loaded = load_types_lock(dir.path()).expect("a version 1 file still loads");
        let events = &loaded.tables[&"app.public.events".parse::<ObjectId>().unwrap()];
        assert_eq!(events["payload"].r#type, DataType::named("record"));
        assert_eq!(events["tags"].r#type, DataType::named("list"));
    }

    #[mz_ore::test]
    fn structural_payload_on_the_wrong_type_is_rejected() {
        let dir = tempfile::tempdir().expect("failed to create temp dir");
        fs::write(
            dir.path().join("types.lock"),
            "version = 2\n\n[[table]]\nname = \"app.public.events\"\ncolumns = [\n    \
             { name = \"id\", type = \"integer\", nullable = true, fields = [\n        \
             { name = \"a\", type = \"int4\", nullable = true },\n    ] },\n]\n",
        )
        .unwrap();

        let err = load_types_lock(dir.path()).expect_err("fields on a scalar is rejected");
        let message = err.to_string();
        assert!(
            message.contains("events") && message.contains("id"),
            "error should name the object and column: {message}"
        );
    }
}
