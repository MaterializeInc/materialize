//! Error types for Materialize project operations.
//!
//! This module provides structured error types using `thiserror` that capture rich
//! contextual information about failures during project loading, parsing, and validation.
//!
//! # Error Hierarchy
//!
//! ```text
//! ProjectError
//!   ├── Load(LoadError)              - File I/O and directory traversal errors
//!   ├── Parse(ParseError)            - SQL parsing errors
//!   ├── Validation(ValidationError)  - Semantic validation errors with context
//!   └── Dependency(DependencyError)  - Dependency graph analysis errors
//! ```
//!
//! # Error Context
//!
//! Validation errors are wrapped with `ErrorContext` that captures:
//! - File path where the error occurred
//! - SQL statement that caused the error (when available)
//!
//! This design avoids duplicating context fields across all error variants.

mod dependency;
mod load;
mod parse;
mod validation;

pub use dependency::DependencyError;
pub use load::LoadError;
pub use parse::ParseError;
pub use validation::{ErrorContext, ValidationError, ValidationErrorKind, ValidationErrors};

use thiserror::Error;

/// Top-level error type for all project operations.
///
/// This is the main error type returned by project loading and validation functions.
/// It wraps more specific error types that provide detailed context.
#[derive(Debug, Error)]
pub enum ProjectError {
    /// Error occurred while loading project files from disk
    #[error(transparent)]
    Load(#[from] LoadError),

    /// Error occurred while parsing SQL statements
    #[error(transparent)]
    Parse(#[from] ParseError),

    /// Error occurred during semantic validation (may contain multiple errors)
    #[error(transparent)]
    Validation(#[from] ValidationErrors),

    /// Error occurred during dependency analysis
    #[error(transparent)]
    Dependency(#[from] DependencyError),
}
