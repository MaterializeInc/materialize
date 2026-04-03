//! Dependency errors for dependency graph analysis.
//!
//! This module defines errors that occur during dependency graph
//! construction and cycle detection.

use crate::project::object_id::ObjectId;
use thiserror::Error;

/// Errors that occur during dependency graph analysis.
#[derive(Debug, Error)]
pub enum DependencyError {
    /// Circular dependency detected in the object dependency graph
    #[error("Circular dependency detected: {object}")]
    CircularDependency {
        /// The fully qualified name of the object involved in the circular dependency
        object: ObjectId,
    },
}
