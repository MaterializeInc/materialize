// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
pub(crate) mod validation;

pub(crate) use dependency::DependencyError;
pub(crate) use load::LoadError;
pub(crate) use parse::ParseError;
pub(crate) use validation::{ValidationError, ValidationErrorKind, ValidationErrors};

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
