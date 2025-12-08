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
        source: serde_json::Error,
    },
    #[error("failed to serialize types.lock")]
    SerializeFailed {
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

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ColumnType {
    pub r#type: String,
    pub nullable: bool,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct Types {
    pub version: u8,
    pub objects: BTreeMap<String, BTreeMap<String, ColumnType>>,
}

impl Default for Types {
    fn default() -> Self {
        Types {
            version: 1,
            objects: BTreeMap::new(),
        }
    }
}

/// Load the types.lock file from the specified directory.
/// Returns an error if the file doesn't exist or cannot be parsed.
pub fn load_types_lock(directory: &Path) -> Result<Types, TypesError> {
    let path = directory.join("types.lock");

    let contents = fs::read_to_string(&path).map_err(|source| TypesError::FileReadFailed {
        path: path.clone(),
        source,
    })?;

    serde_json::from_str(&contents).map_err(|source| TypesError::ParseFailed { path, source })
}

impl Types {
    /// Write the types.lock file to the specified directory.
    /// Overwrites any existing file at that location.
    pub fn write_types_lock(&self, directory: &Path) -> Result<(), TypesError> {
        let path = directory.join("types.lock");

        let contents = serde_json::to_string_pretty(self)
            .map_err(|source| TypesError::SerializeFailed { source })?;

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
            .map_err(|source| TypesError::SerializeFailed { source })?;

        fs::write(&path, contents).map_err(|source| TypesError::FileWriteFailed { path, source })
    }

    /// Merge another Types instance into this one.
    ///
    /// Objects from `other` will be added to this Types. If the same object
    /// exists in both, the one from `other` will overwrite.
    pub fn merge(&mut self, other: &Types) {
        for (key, value) in &other.objects {
            self.objects.insert(key.clone(), value.clone());
        }
    }

    /// Get the column schema for an object by its fully qualified name.
    pub fn get_object(&self, fqn: &str) -> Option<&BTreeMap<String, ColumnType>> {
        self.objects.get(fqn)
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

    serde_json::from_str(&contents).map_err(|source| TypesError::ParseFailed { path, source })
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
