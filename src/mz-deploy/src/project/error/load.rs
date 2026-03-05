//! Load errors for project file I/O operations.
//!
//! This module defines errors that occur during project file loading,
//! directory traversal, and file system operations.

use std::path::PathBuf;
use thiserror::Error;

/// Errors that occur during project file loading and I/O operations.
#[derive(Debug, Error)]
pub enum LoadError {
    /// Project root directory does not exist
    #[error("Project root directory does not exist: {path}")]
    RootNotFound {
        /// The path that was not found
        path: PathBuf,
    },

    /// Project root path is not a directory
    #[error("Project root is not a directory: {path}")]
    RootNotDirectory {
        /// The path that is not a directory
        path: PathBuf,
    },

    /// models/ subdirectory does not exist
    #[error("models/ directory not found in project root: {path}")]
    ModelsNotFound {
        /// The expected models/ path
        path: PathBuf,
    },

    /// Failed to read a directory
    #[error("Failed to read directory: {path}")]
    DirectoryReadFailed {
        /// The directory that couldn't be read
        path: PathBuf,
        /// The underlying I/O error
        #[source]
        source: std::io::Error,
    },

    /// Failed to read a directory entry
    #[error("Failed to read directory entry in: {directory}")]
    EntryReadFailed {
        /// The directory containing the entry
        directory: PathBuf,
        /// The underlying I/O error
        #[source]
        source: std::io::Error,
    },

    /// Failed to read a SQL file
    #[error("Failed to read SQL file: {path}")]
    FileReadFailed {
        /// The file that couldn't be read
        path: PathBuf,
        /// The underlying I/O error
        #[source]
        source: std::io::Error,
    },

    /// Invalid file name (couldn't extract stem)
    #[error("Invalid file name: {path}")]
    InvalidFileName {
        /// The file with the invalid name
        path: PathBuf,
    },

    /// Failed to extract schema name from path
    #[error("Failed to extract schema from path: {path}")]
    SchemaExtractionFailed {
        /// The path where extraction failed
        path: PathBuf,
    },

    /// Failed to extract database name from path
    #[error("Failed to extract database from path: {path}")]
    DatabaseExtractionFailed {
        /// The path where extraction failed
        path: PathBuf,
    },
}
