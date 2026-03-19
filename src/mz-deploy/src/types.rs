//! Data-contract system for external dependencies.
//!
//! When a project references objects it does not own (e.g. tables created by an
//! upstream ingestion pipeline), mz-deploy needs to know their column schemas so
//! it can type-check views that depend on them. This module manages that contract
//! through the `types.lock` file.
//!
//! ## Lock File Flow
//!
//! 1. `gen-data-contracts` queries the live region via [`client::type_info`] and
//!    writes `types.lock`.
//! 2. During `compile`, the lock file is loaded and its schemas are used to
//!    resolve external dependency columns.
//! 3. During `compile --type-check`, the [`TypeChecker`] spins up a Materialize
//!    Docker container, stages external dependencies as temporary tables using
//!    the lock file schemas, and validates every project view in topological
//!    order.
//!
//! ## Incremental Type Checking
//!
//! To avoid re-validating every view on each compile, mz-deploy supports
//! incremental type checking via a `typecheck.snapshot` file stored in the
//! build directory alongside `types.cache`.
//!
//! - **[`type_hash`]** — Computes a deterministic SHA-256 hash of a column
//!   schema map. Used to detect whether a view's output type changed after
//!   re-checking, which determines whether downstream dependents need
//!   re-validation.
//! - **[`IncrementalState`]** — Bundles the cached column types and dirty set
//!   for [`typecheck_with_client`]. Clean objects are stubbed as temporary
//!   tables from cached types; dirty objects are validated and their output
//!   columns queried inline.
//! - **[`load_typecheck_snapshot`]** / **[`write_typecheck_snapshot`]** —
//!   Read/write the `typecheck.snapshot` file, which maps fully-qualified
//!   object names to AST content hashes. The compile command diffs current
//!   hashes against this snapshot to determine the initial dirty set.
//!
//! ## Key Types
//!
//! - [`Types`] — In-memory representation of a `types.lock` (or `types.cache`)
//!   file: a versioned map from fully-qualified object names to column schemas.
//! - [`ColumnType`] — A single column's type name and nullability.
//! - [`TypeChecker`] — Trait for validating a planned project's SQL against a
//!   real Materialize instance (implemented via a Docker container).
//! - [`IncrementalState`] — Cached types plus dirty set for incremental
//!   type checking.
//!
//! ## Submodules
//!
//! - **[`typechecker`]** — [`TypeChecker`] trait definition, the
//!   [`typecheck_with_client`] helper, [`IncrementalState`], and structured
//!   error types for per-object type-check failures.
//! - **Build artifacts** — The `target/` directory holds `types.cache` (column
//!   schemas after type checking) and `typecheck.snapshot` (AST hashes for
//!   incremental diffing).

pub mod docker_runtime;
mod typechecker;

pub use typechecker::{
    IncrementalState, ObjectTypeCheckError, TypeCheckError, TypeCheckErrors, TypeChecker,
    typecheck_with_client,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
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

impl Default for ObjectKind {
    fn default() -> Self {
        ObjectKind::Table
    }
}

impl ObjectKind {
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

/// Directory name for mz-deploy build artifacts (types.cache, etc.).
pub const BUILD_DIR: &str = "target";

/// Errors that can occur when reading, writing, or parsing `types.lock`
/// and `types.cache` files.
#[derive(Error, Debug)]
pub enum TypesError {
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
    #[error("failed to parse types.cache at {path}")]
    CacheParseFailed {
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
}

/// A single column's type name and nullability in a data contract.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ColumnType {
    pub r#type: String,
    pub nullable: bool,
}

/// In-memory representation of a `types.lock` or `types.cache` file.
///
/// Maps fully-qualified object names (`database.schema.object`) to their
/// column schemas. Used for type-checking views against external dependencies.
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Types {
    pub version: u8,
    pub tables: BTreeMap<String, BTreeMap<String, ColumnType>>,
    /// Object kind for each fully-qualified name. Missing entries default to `Table`.
    #[serde(default, skip_serializing)]
    pub kinds: BTreeMap<String, ObjectKind>,
}

impl Default for Types {
    fn default() -> Self {
        Types {
            version: 1,
            tables: BTreeMap::new(),
            kinds: BTreeMap::new(),
        }
    }
}

/// TOML serialization format for types.lock
#[derive(Serialize, Deserialize)]
struct TypesLock {
    version: u8,
    #[serde(default)]
    table: Vec<TableLock>,
}

#[derive(Serialize, Deserialize)]
struct TableLock {
    database: String,
    schema: String,
    name: String,
    #[serde(default)]
    kind: ObjectKind,
    #[serde(default, alias = "column")]
    columns: Vec<ColumnLock>,
}

#[derive(Serialize, Deserialize)]
struct ColumnLock {
    name: String,
    #[serde(rename = "type")]
    r#type: String,
    nullable: bool,
}

impl From<&Types> for TypesLock {
    fn from(types: &Types) -> Self {
        let mut tables: Vec<TableLock> = types
            .tables
            .iter()
            .map(|(fqn, columns)| {
                let parts: Vec<&str> = fqn.splitn(3, '.').collect();
                let (database, schema, name) = (
                    parts[0].to_string(),
                    parts[1].to_string(),
                    parts[2].to_string(),
                );

                let mut cols: Vec<ColumnLock> = columns
                    .iter()
                    .map(|(col_name, col_type)| ColumnLock {
                        name: col_name.clone(),
                        r#type: col_type.r#type.clone(),
                        nullable: col_type.nullable,
                    })
                    .collect();
                cols.sort_by(|a, b| a.name.cmp(&b.name));

                let kind = types.kinds.get(fqn).copied().unwrap_or(ObjectKind::Table);

                TableLock {
                    database,
                    schema,
                    name,
                    kind,
                    columns: cols,
                }
            })
            .collect();
        tables.sort_by(|a, b| {
            (&a.database, &a.schema, &a.name).cmp(&(&b.database, &b.schema, &b.name))
        });

        TypesLock {
            version: types.version,
            table: tables,
        }
    }
}

impl From<TypesLock> for Types {
    fn from(lock: TypesLock) -> Self {
        let mut tables = BTreeMap::new();
        let mut kinds = BTreeMap::new();
        for obj in lock.table {
            let fqn = format!("{}.{}.{}", obj.database, obj.schema, obj.name);
            let mut columns = BTreeMap::new();
            for col in obj.columns {
                columns.insert(
                    col.name,
                    ColumnType {
                        r#type: col.r#type,
                        nullable: col.nullable,
                    },
                );
            }
            kinds.insert(fqn.clone(), obj.kind);
            tables.insert(fqn, columns);
        }
        Types {
            version: lock.version,
            tables,
            kinds,
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

/// Hand-format a `TypesLock` as TOML with inline column tables.
fn write_toml(lock: &TypesLock) -> String {
    let mut out = String::new();
    out.push_str("# This file is automatically @generated by mz-deploy.\n");
    out.push_str("# It is not intended for manual editing.\n");
    out.push_str(&format!("version = {}\n", lock.version));

    for obj in &lock.table {
        out.push('\n');
        out.push_str("[[table]]\n");
        out.push_str(&format!(
            "database = \"{}\"\n",
            escape_toml_string(&obj.database)
        ));
        out.push_str(&format!(
            "schema = \"{}\"\n",
            escape_toml_string(&obj.schema)
        ));
        out.push_str(&format!("name = \"{}\"\n", escape_toml_string(&obj.name)));
        if obj.kind != ObjectKind::Table {
            out.push_str(&format!("kind = \"{}\"\n", obj.kind.as_str()));
        }
        out.push_str("columns = [\n");
        for col in &obj.columns {
            out.push_str(&format!(
                "    {{ name = \"{}\", type = \"{}\", nullable = {} }},\n",
                escape_toml_string(&col.name),
                escape_toml_string(&col.r#type),
                col.nullable,
            ));
        }
        out.push_str("]\n");
    }

    out
}

/// Load the types.lock file from the specified directory.
/// Returns an error if the file doesn't exist or cannot be parsed.
pub fn load_types_lock(directory: &Path) -> Result<Types, TypesError> {
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

    /// Write the types.cache file to the target directory.
    ///
    /// This cache stores the column types of internal project views after type checking.
    /// It is used by the test command to validate unit tests without re-typechecking.
    pub fn write_types_cache(&self, directory: &Path) -> Result<(), TypesError> {
        let cache_dir = directory.join(BUILD_DIR);

        // Create target directory if it doesn't exist
        if !cache_dir.exists() {
            fs::create_dir_all(&cache_dir).map_err(|source| {
                TypesError::DirectoryCreationFailed {
                    path: cache_dir.clone(),
                    source,
                }
            })?;
        }

        let path = cache_dir.join("types.cache");
        let lock = TypesLock::from(self);
        let contents = write_toml(&lock);

        fs::write(&path, contents).map_err(|source| TypesError::FileWriteFailed { path, source })
    }

    /// Merge another Types instance into this one.
    ///
    /// Objects from `other` will be added to this Types. If the same object
    /// exists in both, the one from `other` will overwrite.
    pub fn merge(&mut self, other: &Types) {
        for (key, value) in &other.tables {
            self.tables.insert(key.clone(), value.clone());
        }
        for (key, value) in &other.kinds {
            self.kinds.insert(key.clone(), *value);
        }
    }

    /// Get the column schema for an object by its fully qualified name.
    pub fn get_table(&self, fqn: &str) -> Option<&BTreeMap<String, ColumnType>> {
        self.tables.get(fqn)
    }

    /// Get the object kind for a fully-qualified name, defaulting to `Table`.
    pub fn get_kind(&self, fqn: &str) -> ObjectKind {
        self.kinds.get(fqn).copied().unwrap_or(ObjectKind::Table)
    }
}

/// Compute a deterministic hash of a column schema map.
///
/// Since `BTreeMap` iterates in sorted key order, the hash is stable across runs.
/// Each `(column_name, type, nullable)` triple is fed into SHA-256 in order.
/// The output format matches `compute_typed_hash`: `sha256:<hex>`.
pub fn type_hash(columns: &BTreeMap<String, ColumnType>) -> String {
    let mut hasher = Sha256::new();
    for (name, col_type) in columns {
        hasher.update(name.as_bytes());
        hasher.update(col_type.r#type.as_bytes());
        hasher.update(if col_type.nullable { b"1" } else { b"0" });
    }
    let result = hasher.finalize();
    format!("sha256:{:x}", result)
}

/// The filename for the typecheck snapshot inside the build directory.
const TYPECHECK_SNAPSHOT_FILE: &str = "typecheck.snapshot";

/// Serialization wrapper for the typecheck snapshot file.
#[derive(Serialize, Deserialize)]
struct TypecheckSnapshot {
    /// Map from fully-qualified object name to AST content hash.
    hashes: BTreeMap<String, String>,
}

/// Load a previously written typecheck snapshot from the build directory.
///
/// Returns `None` if the file does not exist. Returns an error only on parse failures.
pub fn load_typecheck_snapshot(
    directory: &Path,
) -> Result<Option<BTreeMap<String, String>>, TypesError> {
    let path = directory.join(BUILD_DIR).join(TYPECHECK_SNAPSHOT_FILE);

    let contents = match fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => {
            return Err(TypesError::FileReadFailed { path, source });
        }
    };

    let snapshot: TypecheckSnapshot = toml::from_str(&contents)
        .map_err(|source| TypesError::CacheParseFailed { path, source })?;
    Ok(Some(snapshot.hashes))
}

/// Write a typecheck snapshot to the build directory.
///
/// Creates the build directory if it does not exist.
pub fn write_typecheck_snapshot(
    directory: &Path,
    hashes: &BTreeMap<String, String>,
) -> Result<(), TypesError> {
    let cache_dir = directory.join(BUILD_DIR);
    if !cache_dir.exists() {
        fs::create_dir_all(&cache_dir).map_err(|source| TypesError::DirectoryCreationFailed {
            path: cache_dir.clone(),
            source,
        })?;
    }

    let snapshot = TypecheckSnapshot {
        hashes: hashes.clone(),
    };
    let contents = toml::to_string_pretty(&snapshot)
        .expect("TypecheckSnapshot should always serialize to valid TOML");

    let path = cache_dir.join(TYPECHECK_SNAPSHOT_FILE);
    fs::write(&path, contents).map_err(|source| TypesError::FileWriteFailed { path, source })
}

/// Load the types.cache file from the target directory.
///
/// This cache contains column types for internal project views, generated during type checking.
/// Returns an error if the file doesn't exist or cannot be parsed.
pub fn load_types_cache(directory: &Path) -> Result<Types, TypesError> {
    let path = directory.join(BUILD_DIR).join("types.cache");

    let contents = fs::read_to_string(&path).map_err(|source| TypesError::FileReadFailed {
        path: path.clone(),
        source,
    })?;

    let lock: TypesLock = toml::from_str(&contents)
        .map_err(|source| TypesError::CacheParseFailed { path, source })?;
    Ok(lock.into())
}

/// Check if the types.cache is stale compared to the project source files.
///
/// Returns true if any SQL file in the project directory is newer than the cache file.
pub fn is_types_cache_stale(directory: &Path) -> bool {
    let cache_path = directory.join(BUILD_DIR).join("types.cache");

    // If cache doesn't exist, it's considered stale
    let cache_metadata = match fs::metadata(&cache_path) {
        Ok(m) => m,
        Err(_) => return true,
    };

    let cache_modified = match cache_metadata.modified() {
        Ok(t) => t,
        Err(_) => return true,
    };

    // Check all SQL files in the project
    check_files_newer_than(directory, cache_modified)
}

/// Recursively check if any .sql file is newer than the given time.
fn check_files_newer_than(dir: &Path, threshold: std::time::SystemTime) -> bool {
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(_) => return false,
    };

    for entry in entries.flatten() {
        let path = entry.path();

        // Skip hidden directories/files and build artifact directory
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.') || n == BUILD_DIR)
        {
            continue;
        }

        if path.is_dir() {
            if check_files_newer_than(&path, threshold) {
                return true;
            }
        } else if path.extension().is_some_and(|ext| ext == "sql") {
            if let Ok(metadata) = fs::metadata(&path) {
                if let Ok(modified) = metadata.modified() {
                    if modified > threshold {
                        return true;
                    }
                }
            }
        }
    }

    false
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
            "id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
            },
        );
        order_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: true,
            },
        );
        order_cols.insert(
            "amount".to_string(),
            ColumnType {
                r#type: "numeric".to_string(),
                nullable: true,
            },
        );
        tables.insert("app.ingest.orders".to_string(), order_cols);

        let mut user_cols = BTreeMap::new();
        user_cols.insert(
            "user_id".to_string(),
            ColumnType {
                r#type: "integer".to_string(),
                nullable: false,
            },
        );
        user_cols.insert(
            "name".to_string(),
            ColumnType {
                r#type: "text".to_string(),
                nullable: true,
            },
        );
        tables.insert("app.ingest.users".to_string(), user_cols);

        let mut kinds = BTreeMap::new();
        kinds.insert("app.ingest.orders".to_string(), ObjectKind::Table);
        kinds.insert("app.ingest.users".to_string(), ObjectKind::Table);

        let types = Types {
            version: 1,
            tables,
            kinds,
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
            },
        );
        tables.insert("app.ingest.orders".to_string(), cols.clone());
        tables.insert("app.ingest.order_summary".to_string(), cols);

        let mut kinds = BTreeMap::new();
        kinds.insert("app.ingest.orders".to_string(), ObjectKind::Table);
        kinds.insert(
            "app.ingest.order_summary".to_string(),
            ObjectKind::MaterializedView,
        );

        let types = Types {
            version: 1,
            tables,
            kinds,
        };

        let dir = tempfile::tempdir().expect("failed to create temp dir");
        types
            .write_types_lock(dir.path())
            .expect("failed to write types.lock");

        let loaded = load_types_lock(dir.path()).expect("failed to load types.lock");
        assert_eq!(types, loaded);
    }

    #[test]
    fn test_old_lock_without_kind_defaults_to_table() {
        let toml = r#"
version = 1

[[table]]
database = "app"
schema = "ingest"
name = "orders"
columns = [
    { name = "id", type = "integer", nullable = false },
]
"#;
        let lock: TypesLock = toml::from_str(toml).expect("parse TOML");
        let types: Types = lock.into();
        assert_eq!(types.get_kind("app.ingest.orders"), ObjectKind::Table);
    }
}
