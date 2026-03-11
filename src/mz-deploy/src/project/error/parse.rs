//! Parse errors for SQL parsing operations.
//!
//! This module defines errors that occur when parsing SQL statements
//! from project files.

use owo_colors::OwoColorize;
use std::fmt;
use std::path::PathBuf;
use crate::project::variables::VariableError;

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
            ParseError::SqlParseFailed { path, sql, source } => {
                // Extract database/schema/file for path display
                let path_components: Vec<_> = path.components().collect();
                let len = path_components.len();

                let relative_path = if len >= 3 {
                    format!(
                        "{}/{}/{}",
                        path_components[len - 3].as_os_str().to_string_lossy(),
                        path_components[len - 2].as_os_str().to_string_lossy(),
                        path_components[len - 1].as_os_str().to_string_lossy()
                    )
                } else {
                    path.display().to_string()
                };

                // Format like rustc: error: <message>
                writeln!(f, "{}: {}", "error".bright_red().bold(), source.error)?;

                // Show file location: --> path
                writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;

                // Show SQL content
                writeln!(f, "  {}", "|".bright_blue().bold())?;
                for line in sql.lines() {
                    writeln!(f, "  {} {}", "|".bright_blue().bold(), line)?;
                }
                writeln!(f, "  {}", "|".bright_blue().bold())?;

                Ok(())
            }
            ParseError::StatementsParseFailed { message } => {
                write!(f, "{}: {}", "error".bright_red().bold(), message)
            }
            ParseError::UnresolvedVariables(inner) => {
                writeln!(
                    f,
                    "{}: unresolved variables in {}",
                    "error".bright_red().bold(),
                    inner.path.display()
                )?;
                let formatted: Vec<String> = inner.unresolved.iter().map(|v| format!(":{}", v)).collect();
                writeln!(
                    f,
                    "  {}: {}",
                    "undefined".bright_red(),
                    formatted.join(", ")
                )?;
                writeln!(
                    f,
                    "  {}: define these in [profiles.<name>.variables] in project.toml",
                    "hint".bright_blue()
                )?;
                write!(
                    f,
                    "  {}: if these are not variables, add -- PRAGMA WARN_ON_MISSING_VARIABLES; as the first line",
                    "hint".bright_blue()
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
