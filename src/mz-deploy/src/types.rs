//! Data-contract system for external dependencies.
//!
//! When a project references objects it does not own (e.g. tables created by an
//! upstream ingestion pipeline), mz-deploy needs to know their column schemas so
//! it can type-check views that depend on them. This module manages that contract
//! through the `types.lock` file.
//!
//! ## Key Types
//!
//! - [`Types`] — In-memory representation of a `types.lock` (or `types.cache`)
//!   file: a versioned map from fully-qualified object names to column schemas.
//! - [`ColumnType`] — A single column's type name and nullability.
//! - [`TypeChecker`] — Trait for validating a planned project's SQL against a
//!   real Materialize instance (implemented via a Docker container).
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
//! ## Submodules
//!
//! - **[`typechecker`]** — [`TypeChecker`] trait definition, the
//!   [`typecheck_with_client`] helper, and structured error types for
//!   per-object type-check failures.

pub mod docker_runtime;
mod typechecker;

pub use typechecker::{
    ObjectTypeCheckError, TypeCheckError, TypeCheckErrors, TypeChecker, typecheck_with_client,
};

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

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
        source: serde_json::Error,
    },
    #[error("failed to serialize types.cache")]
    CacheSerializeFailed {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to create directory {path}")]
    DirectoryCreationFailed {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ColumnType {
    pub r#type: String,
    pub nullable: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct Types {
    pub version: u8,
    pub tables: BTreeMap<String, BTreeMap<String, ColumnType>>,
}

impl Default for Types {
    fn default() -> Self {
        Types {
            version: 1,
            tables: BTreeMap::new(),
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

                TableLock {
                    database,
                    schema,
                    name,
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
            tables.insert(fqn, columns);
        }
        Types {
            version: lock.version,
            tables,
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
                out.push_str(&format!("\\u{:04X}", c as u32));
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

    /// Write the types.cache file to the .mz-deploy directory.
    ///
    /// This cache stores the column types of internal project views after type checking.
    /// It is used by the test command to validate unit tests without re-typechecking.
    pub fn write_types_cache(&self, directory: &Path) -> Result<(), TypesError> {
        let cache_dir = directory.join(".mz-deploy");

        // Create .mz-deploy directory if it doesn't exist
        if !cache_dir.exists() {
            fs::create_dir_all(&cache_dir).map_err(|source| {
                TypesError::DirectoryCreationFailed {
                    path: cache_dir.clone(),
                    source,
                }
            })?;
        }

        let path = cache_dir.join("types.cache");
        let contents = serde_json::to_string_pretty(self)
            .map_err(|source| TypesError::CacheSerializeFailed { source })?;

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
    }

    /// Get the column schema for an object by its fully qualified name.
    pub fn get_table(&self, fqn: &str) -> Option<&BTreeMap<String, ColumnType>> {
        self.tables.get(fqn)
    }
}

/// Load the types.cache file from the .mz-deploy directory.
///
/// This cache contains column types for internal project views, generated during type checking.
/// Returns an error if the file doesn't exist or cannot be parsed.
pub fn load_types_cache(directory: &Path) -> Result<Types, TypesError> {
    let path = directory.join(".mz-deploy").join("types.cache");

    let contents = fs::read_to_string(&path).map_err(|source| TypesError::FileReadFailed {
        path: path.clone(),
        source,
    })?;

    serde_json::from_str(&contents).map_err(|source| TypesError::CacheParseFailed { path, source })
}

/// Check if the types.cache is stale compared to the project source files.
///
/// Returns true if any SQL file in the project directory is newer than the cache file.
pub fn is_types_cache_stale(directory: &Path) -> bool {
    let cache_path = directory.join(".mz-deploy").join("types.cache");

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

        // Skip hidden directories and files
        if path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.starts_with('.'))
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

        let types = Types { version: 1, tables };

        let dir = tempfile::tempdir().expect("failed to create temp dir");
        types
            .write_types_lock(dir.path())
            .expect("failed to write types.lock");

        let loaded = load_types_lock(dir.path()).expect("failed to load types.lock");
        assert_eq!(types, loaded);
    }
}
