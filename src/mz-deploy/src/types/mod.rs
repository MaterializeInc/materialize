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
}
