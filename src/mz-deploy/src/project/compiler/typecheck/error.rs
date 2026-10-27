// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error types surfaced by the typechecker.

use crate::project::ir::object_id::ObjectId;
use crate::types::TypesError;
use mz_sql::catalog::CatalogError;
use mz_sql::plan::PlanError;
use mz_sql_parser::parser::ParserStatementError;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use thiserror::Error;

/// Errors that can occur during runtime typechecking.
#[derive(Debug, Error)]
pub enum TypeCheckError {
    #[error("{}", format_multiple(.0))]
    Multiple(Vec<ObjectTypeCheckError>),

    #[error("database error during setup: {0}")]
    DatabaseSetupError(String),

    #[error("failed to get sorted objects: {0}")]
    SortError(#[from] crate::project::error::DependencyError),

    #[error("failed to write types cache: {0}")]
    TypesCacheWriteFailed(#[from] TypesError),
}

fn format_multiple(errors: &[ObjectTypeCheckError]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for (idx, error) in errors.iter().enumerate() {
        if idx > 0 {
            out.push('\n');
        }
        let _ = write!(&mut out, "{}", error);
    }
    let _ = writeln!(&mut out);
    let _ = writeln!(
        &mut out,
        "could not type check due to {} previous error{}",
        errors.len(),
        if errors.len() == 1 { "" } else { "s" }
    );
    out
}

/// A single typecheck error for a specific object, rendered in rustc style.
#[derive(Debug, Clone)]
pub struct ObjectTypeCheckError {
    pub object_id: ObjectId,
    pub file_path: PathBuf,
    pub kind: ObjectTypeCheckErrorKind,
}

/// Underlying error from one of the typecheck pipeline stages.
///
/// Holding the structured upstream error (rather than its string rendering)
/// lets the LSP pull out identifiers and offsets to underline the offending
/// token in the source file.
#[derive(Debug, Clone)]
pub enum ObjectTypeCheckErrorKind {
    /// Parser failure from `mz_sql_parser::parser::parse_statements`. Carries
    /// a byte offset (`error.error.pos`) into the SQL string.
    Parser(ParserStatementError),
    /// Resolution or planning failure from `mz_sql::names::resolve` or
    /// `mz_sql::plan::plan`. Wrapped in `Arc` because `PlanError` does not
    /// implement `Clone`.
    Plan(Arc<PlanError>),
    /// Catalog failure from `insert_item_from_plan`.
    Catalog(CatalogError),
    /// Internal/synthetic error (empty-statement check, AST-conversion
    /// failure, dependency-stub failure). No locatable position.
    Internal(String),
}

impl ObjectTypeCheckError {
    /// Build an internal-error variant with no SQL snippet attached.
    pub(super) fn internal(object_id: ObjectId, file_path: PathBuf, error_message: String) -> Self {
        Self {
            object_id,
            file_path,
            kind: ObjectTypeCheckErrorKind::Internal(error_message),
        }
    }

    /// The primary error message, rendered from the underlying error's
    /// `Display` impl (or the inner string for `Internal`).
    pub fn error_message(&self) -> String {
        match &self.kind {
            ObjectTypeCheckErrorKind::Parser(e) => e.to_string(),
            ObjectTypeCheckErrorKind::Plan(e) => e.to_string(),
            ObjectTypeCheckErrorKind::Catalog(e) => e.to_string(),
            ObjectTypeCheckErrorKind::Internal(msg) => msg.clone(),
        }
    }

    /// Optional `detail:` line, populated for `PlanError` variants that
    /// expose extra context.
    pub fn detail(&self) -> Option<String> {
        match &self.kind {
            ObjectTypeCheckErrorKind::Plan(e) => e.detail(),
            _ => None,
        }
    }

    /// Optional `hint:` line, populated for `PlanError` and `CatalogError`
    /// variants that suggest a fix.
    pub fn hint(&self) -> Option<String> {
        match &self.kind {
            ObjectTypeCheckErrorKind::Plan(e) => e.hint(),
            ObjectTypeCheckErrorKind::Catalog(e) => e.hint(),
            _ => None,
        }
    }
}

impl fmt::Display for ObjectTypeCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "type check failed for '{}': {}",
            self.object_id,
            self.error_message()
        )
    }
}

impl std::error::Error for ObjectTypeCheckError {}
