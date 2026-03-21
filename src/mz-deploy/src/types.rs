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
//! build directory alongside `types.cache`. The incremental logic is
//! encapsulated in the [`incremental`] submodule with a minimal public API:
//!
//! - **[`plan_typecheck`]** — Compares current AST hashes against the snapshot
//!   and returns a [`TypecheckPlan`] that is either up-to-date (skip Docker)
//!   or carries the dirty set for [`typecheck_with_client`].
//! - **[`write_snapshot`]** — Writes the `typecheck.snapshot` after a
//!   successful typecheck.
//! - **[`type_hash`]** — Computes a deterministic SHA-256 hash of a column
//!   schema map. Used internally by dirty propagation to detect whether a
//!   view's output type changed.
//!
//! ## Key Types
//!
//! - [`Types`] — In-memory representation of a `types.lock` (or `types.cache`)
//!   file: a versioned map from fully-qualified object names to column schemas.
//! - [`ColumnType`] — A single column's type name and nullability.
//! - [`TypeChecker`] — Trait for validating a planned project's SQL against a
//!   real Materialize instance (implemented via a Docker container).
//! - [`TypecheckPlan`] — The result of comparing current project state against
//!   the typecheck snapshot. Callers inspect it to decide whether Docker is
//!   needed.
//!
//! ## Submodules
//!
//! - **[`incremental`]** — Plan computation, dirty propagation, and snapshot
//!   management for incremental type checking.
//! - **[`typechecker`]** — [`TypeChecker`] trait definition, the
//!   [`typecheck_with_client`] helper, and structured error types for
//!   per-object type-check failures.
//! - **Build artifacts** — The `target/` directory holds `types.cache` (column
//!   schemas after type checking) and `typecheck.snapshot` (AST hashes for
//!   incremental diffing).

pub mod docker_runtime;
mod incremental;
mod typechecker;

pub use incremental::{TypecheckPlan, plan_typecheck, write_snapshot, write_snapshot_from_plan};
pub use typechecker::{
    ObjectTypeCheckError, TypeCheckError, TypeCheckErrors, TypeChecker, typecheck_with_client,
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
    #[error("failed to deserialize binary cache at {path}")]
    BincodeParseFailed {
        path: PathBuf,
        #[source]
        source: bincode::Error,
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

    /// Write the types.cache file to the target directory in bincode format.
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

        let path = cache_dir.join(TYPES_CACHE_BIN_FILE);
        let bin = TypesCacheBin::from(self);
        let bytes =
            bincode::serialize(&bin).expect("TypesCacheBin should always serialize to bincode");

        fs::write(&path, bytes).map_err(|source| TypesError::FileWriteFailed { path, source })
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

/// Cache filename for types.cache (bincode format).
const TYPES_CACHE_BIN_FILE: &str = "types.cache.bin";

/// Snapshot filename for typecheck.snapshot (bincode format).
const TYPECHECK_SNAPSHOT_BIN_FILE: &str = "typecheck.snapshot.bin";

/// Bincode-friendly representation of [`Types`] for the types.cache.
///
/// The main [`Types`] struct uses `#[serde(skip_serializing)]` on its `kinds`
/// field, which works for self-describing formats (TOML/JSON) but breaks
/// bincode's positional deserialization. This wrapper carries only the fields
/// that the cache actually needs (version + tables), making bincode
/// round-trips safe. The `kinds` map is always empty in cache files.
#[derive(Serialize, Deserialize)]
struct TypesCacheBin {
    version: u8,
    tables: BTreeMap<String, BTreeMap<String, ColumnType>>,
}

impl From<&Types> for TypesCacheBin {
    fn from(types: &Types) -> Self {
        Self {
            version: types.version,
            tables: types.tables.clone(),
        }
    }
}

impl From<TypesCacheBin> for Types {
    fn from(bin: TypesCacheBin) -> Self {
        Types {
            version: bin.version,
            tables: bin.tables,
            kinds: BTreeMap::new(),
        }
    }
}

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
    let path = directory.join(BUILD_DIR).join(TYPECHECK_SNAPSHOT_BIN_FILE);

    match fs::read(&path) {
        Ok(bytes) => {
            let snapshot: TypecheckSnapshot = bincode::deserialize(&bytes)
                .map_err(|source| TypesError::BincodeParseFailed { path, source })?;
            Ok(Some(snapshot.hashes))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(TypesError::FileReadFailed { path, source }),
    }
}

/// Write a typecheck snapshot to the build directory in bincode format.
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
    let bytes = bincode::serialize(&snapshot)
        .expect("TypecheckSnapshot should always serialize to bincode");

    let path = cache_dir.join(TYPECHECK_SNAPSHOT_BIN_FILE);
    fs::write(&path, bytes).map_err(|source| TypesError::FileWriteFailed { path, source })
}

/// Load the types.cache file from the target directory.
///
/// Returns an error if the file does not exist or cannot be parsed.
pub fn load_types_cache(directory: &Path) -> Result<Types, TypesError> {
    let path = directory.join(BUILD_DIR).join(TYPES_CACHE_BIN_FILE);

    let bytes = fs::read(&path).map_err(|source| TypesError::FileReadFailed {
        path: path.clone(),
        source,
    })?;

    let bin: TypesCacheBin = bincode::deserialize(&bytes)
        .map_err(|source| TypesError::BincodeParseFailed { path, source })?;
    Ok(bin.into())
}

/// Check if the types.cache is stale compared to the project source files.
///
/// Returns true if any SQL file in the project directory is newer than the cache file.
pub fn is_types_cache_stale(directory: &Path) -> bool {
    let cache_path = directory.join(BUILD_DIR).join(TYPES_CACHE_BIN_FILE);

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
