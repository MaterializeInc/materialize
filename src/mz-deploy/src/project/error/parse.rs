// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Parse errors for SQL parsing operations.
//!
//! This module defines errors that occur when parsing SQL statements
//! from project files.

use crate::project::syntax::variables::VariableError;
use std::fmt;
use std::path::PathBuf;

/// Errors that occur during SQL parsing.
#[derive(Debug)]
pub enum ParseError {
    /// Failed to parse SQL statements
    SqlParseFailed {
        /// The file containing the SQL
        path: PathBuf,
        /// The SQL text that failed to parse
        sql: String,
        /// The underlying parser error
        source: mz_sql_parser::parser::ParserStatementError,
    },

    /// Failed to parse SQL statements from multiple sources
    StatementsParseFailed {
        /// Error message
        message: String,
    },

    /// SQL file contains variable references with no definition
    UnresolvedVariables(VariableError),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::SqlParseFailed { source, .. } => write!(f, "{}", source.error),
            ParseError::StatementsParseFailed { message } => write!(f, "{}", message),
            ParseError::UnresolvedVariables(inner) => {
                let names: Vec<String> = inner
                    .unresolved
                    .iter()
                    .map(|v| format!(":{}", v.name))
                    .collect();
                write!(
                    f,
                    "unresolved variables in {}: {}",
                    inner.path.display(),
                    names.join(", ")
                )
            }
        }
    }
}

impl std::error::Error for ParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParseError::SqlParseFailed { source, .. } => Some(source),
            ParseError::StatementsParseFailed { .. } => None,
            ParseError::UnresolvedVariables(_) => None,
        }
    }
}
