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

use owo_colors::OwoColorize;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;
use crate::project::object_id::ObjectId;

/// Contextual information about where an error occurred.
///
/// This struct wraps error variants with additional context about the file
/// and SQL statement that caused the error.
#[derive(Debug, Clone)]
pub struct ErrorContext {
    /// The file where the error occurred
    pub file: PathBuf,
    /// The SQL statement that caused the error, if available
    pub sql_statement: Option<String>,
}

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
        }
    }
}

impl std::error::Error for ParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParseError::SqlParseFailed { source, .. } => Some(source),
            ParseError::StatementsParseFailed { .. } => None,
        }
    }
}

/// A validation error with contextual information.
///
/// This struct wraps a `ValidationErrorKind` with context about where
/// the error occurred (file path, SQL statement).
#[derive(Debug)]
pub struct ValidationError {
    /// The underlying error kind
    pub kind: ValidationErrorKind,
    /// Context about where the error occurred
    pub context: ErrorContext,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Extract database/schema/file for path display
        let path_components: Vec<_> = self.context.file.components().collect();
        let len = path_components.len();

        let relative_path = if len >= 3 {
            format!(
                "{}/{}/{}",
                path_components[len - 3].as_os_str().to_string_lossy(),
                path_components[len - 2].as_os_str().to_string_lossy(),
                path_components[len - 1].as_os_str().to_string_lossy()
            )
        } else {
            self.context.file.display().to_string()
        };

        // Format like rustc: error: <message>
        writeln!(
            f,
            "{}: {}",
            "error".bright_red().bold(),
            self.kind.message()
        )?;

        // Show file location: --> path
        writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;

        // Add SQL statement if available
        if let Some(ref sql) = self.context.sql_statement {
            writeln!(f, "  {}", "|".bright_blue().bold())?;
            for line in sql.lines() {
                writeln!(f, "  {} {}", "|".bright_blue().bold(), line)?;
            }
            writeln!(f, "  {}", "|".bright_blue().bold())?;
        }

        // Add help text if available
        if let Some(help) = self.kind.help() {
            writeln!(
                f,
                "  {} {}",
                "=".bright_blue().bold(),
                format!("help: {}", help).bold()
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for ValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl ValidationError {
    /// Create a new validation error with context
    pub fn with_context(kind: ValidationErrorKind, context: ErrorContext) -> Self {
        Self { kind, context }
    }

    /// Create a new validation error with just a file path
    pub fn with_file(kind: ValidationErrorKind, file: PathBuf) -> Self {
        Self {
            kind,
            context: ErrorContext {
                file,
                sql_statement: None,
            },
        }
    }

    /// Create a new validation error with file and SQL statement
    pub fn with_file_and_sql(kind: ValidationErrorKind, file: PathBuf, sql: String) -> Self {
        Self {
            kind,
            context: ErrorContext {
                file,
                sql_statement: Some(sql),
            },
        }
    }
}

/// The specific kind of validation error that occurred.
///
/// This enum contains the actual error variants without contextual information.
/// Context (file path, SQL statement) is stored in the wrapping `ValidationError`.
#[derive(Debug)]
pub enum ValidationErrorKind {
    /// A file contains multiple primary CREATE statements
    MultipleMainStatements { object_name: String },
    /// A file contains no primary CREATE statement
    NoMainStatement { object_name: String },
    /// Object name in statement doesn't match file name
    ObjectNameMismatch { declared: String, expected: String },
    /// Schema qualifier in statement doesn't match directory
    SchemaMismatch { declared: String, expected: String },
    /// Database qualifier in statement doesn't match directory
    DatabaseMismatch { declared: String, expected: String },
    /// An index references a different object
    IndexReferenceMismatch {
        referenced: String,
        expected: String,
    },
    /// A grant references a different object
    GrantReferenceMismatch {
        referenced: String,
        expected: String,
    },
    /// A comment references a different object
    CommentReferenceMismatch {
        referenced: String,
        expected: String,
    },
    /// A column comment references a different table
    ColumnCommentReferenceMismatch {
        referenced: String,
        expected: String,
    },
    /// Comment object type doesn't match actual object type
    CommentTypeMismatch {
        comment_type: String,
        object_type: String,
    },
    /// Grant object type doesn't match actual object type
    GrantTypeMismatch {
        grant_type: String,
        expected_type: String,
    },
    /// Unsupported statement type in object file
    UnsupportedStatement {
        object_name: String,
        statement_type: String,
    },
    /// Unsupported grant type
    ClusterGrantUnsupported,
    /// Grant doesn't target specific object
    GrantMustTargetObject,
    /// System grant not supported
    SystemGrantUnsupported,
    /// Unsupported comment type
    UnsupportedCommentType,
    /// No object type could be determined
    NoObjectType,
    /// Failed to extract schema name from file path
    SchemaExtractionFailed,
    /// Failed to extract database name from file path
    DatabaseExtractionFailed,
    /// Invalid identifier name (contains invalid characters or format)
    InvalidIdentifier { name: String, reason: String },
    /// Index missing required IN CLUSTER clause
    IndexMissingCluster { index_name: String },
    /// Materialized view missing required IN CLUSTER clause
    MaterializedViewMissingCluster { view_name: String },
    /// Invalid statement type in database mod file
    InvalidDatabaseModStatement {
        statement_type: String,
        database_name: String,
    },
    /// Comment in database mod file targets wrong object
    DatabaseModCommentTargetMismatch {
        target: String,
        database_name: String,
    },
    /// Grant in database mod file targets wrong object
    DatabaseModGrantTargetMismatch {
        target: String,
        database_name: String,
    },
    /// Invalid statement type in schema mod file
    InvalidSchemaModStatement {
        statement_type: String,
        schema_name: String,
    },
    /// Comment in schema mod file targets wrong object
    SchemaModCommentTargetMismatch { target: String, schema_name: String },
    /// Grant in schema mod file targets wrong object
    SchemaModGrantTargetMismatch { target: String, schema_name: String },
    /// ALTER DEFAULT PRIVILEGES in database mod requires IN DATABASE scope
    AlterDefaultPrivilegesRequiresDatabaseScope { database_name: String },
    /// ALTER DEFAULT PRIVILEGES in schema mod requires IN SCHEMA scope
    AlterDefaultPrivilegesRequiresSchemaScope { schema_name: String },
    /// ALTER DEFAULT PRIVILEGES IN DATABASE references wrong database
    AlterDefaultPrivilegesDatabaseMismatch {
        referenced: String,
        expected: String,
    },
    /// ALTER DEFAULT PRIVILEGES cannot use IN SCHEMA in database mod
    AlterDefaultPrivilegesSchemaNotAllowed { database_name: String },
    /// ALTER DEFAULT PRIVILEGES cannot use IN DATABASE in schema mod
    AlterDefaultPrivilegesDatabaseNotAllowed { schema_name: String },
    /// ALTER DEFAULT PRIVILEGES IN SCHEMA references wrong schema
    AlterDefaultPrivilegesSchemaMismatch {
        referenced: String,
        expected: String,
    },
}

impl ValidationErrorKind {
    /// Get the short error message for this error kind
    fn message(&self) -> String {
        match self {
            Self::MultipleMainStatements { object_name } => {
                format!(
                    "multiple main CREATE statements found for object '{}'",
                    object_name
                )
            }
            Self::NoMainStatement { object_name } => {
                format!(
                    "no main CREATE statement found for object '{}'",
                    object_name
                )
            }
            Self::ObjectNameMismatch { declared, expected } => {
                format!(
                    "object name mismatch: declared '{}', expected '{}'",
                    declared, expected
                )
            }
            Self::SchemaMismatch { declared, expected } => {
                format!(
                    "schema qualifier mismatch: declared '{}', expected '{}'",
                    declared, expected
                )
            }
            Self::DatabaseMismatch { declared, expected } => {
                format!(
                    "database qualifier mismatch: declared '{}', expected '{}'",
                    declared, expected
                )
            }
            Self::IndexReferenceMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "INDEX references wrong object: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::GrantReferenceMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "GRANT references wrong object: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::CommentReferenceMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "COMMENT references wrong object: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::ColumnCommentReferenceMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "column COMMENT references wrong table: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::CommentTypeMismatch {
                comment_type,
                object_type,
            } => {
                format!(
                    "COMMENT uses wrong object type: {} instead of {}",
                    comment_type, object_type
                )
            }
            Self::GrantTypeMismatch {
                grant_type,
                expected_type,
            } => {
                format!(
                    "GRANT uses incorrect object type: GRANT ON {} instead of GRANT ON {}",
                    grant_type, expected_type
                )
            }
            Self::UnsupportedStatement {
                object_name,
                statement_type,
            } => {
                format!(
                    "unsupported statement type in object '{}': {}",
                    object_name, statement_type
                )
            }
            Self::ClusterGrantUnsupported => "CLUSTER grants are not supported".to_string(),
            Self::GrantMustTargetObject => "GRANT must target a specific object".to_string(),
            Self::SystemGrantUnsupported => "SYSTEM grants are not supported".to_string(),
            Self::UnsupportedCommentType => "unsupported COMMENT object type".to_string(),
            Self::NoObjectType => "could not determine object type".to_string(),
            Self::SchemaExtractionFailed => {
                "failed to extract schema name from file path".to_string()
            }
            Self::DatabaseExtractionFailed => {
                "failed to extract database name from file path".to_string()
            }
            Self::InvalidIdentifier { name, reason } => {
                format!("invalid identifier '{}': {}", name, reason)
            }
            Self::IndexMissingCluster { index_name } => {
                format!(
                    "index '{}' is missing required IN CLUSTER clause",
                    index_name
                )
            }
            Self::MaterializedViewMissingCluster { view_name } => {
                format!(
                    "materialized view '{}' is missing required IN CLUSTER clause",
                    view_name
                )
            }
            Self::InvalidDatabaseModStatement {
                statement_type,
                database_name,
            } => {
                format!(
                    "invalid statement type in database mod file '{}': {}. Only COMMENT ON DATABASE, GRANT ON DATABASE, and ALTER DEFAULT PRIVILEGES are allowed",
                    database_name, statement_type
                )
            }
            Self::DatabaseModCommentTargetMismatch {
                target,
                database_name,
            } => {
                format!(
                    "comment in database mod file must target the database itself. Expected COMMENT ON DATABASE '{}', but found COMMENT ON {}",
                    database_name, target
                )
            }
            Self::DatabaseModGrantTargetMismatch {
                target,
                database_name,
            } => {
                format!(
                    "grant in database mod file must target the database itself. Expected GRANT ON DATABASE '{}', but found GRANT ON {}",
                    database_name, target
                )
            }
            Self::InvalidSchemaModStatement {
                statement_type,
                schema_name,
            } => {
                format!(
                    "invalid statement type in schema mod file '{}': {}. Only COMMENT ON SCHEMA, GRANT ON SCHEMA, and ALTER DEFAULT PRIVILEGES are allowed",
                    schema_name, statement_type
                )
            }
            Self::SchemaModCommentTargetMismatch {
                target,
                schema_name,
            } => {
                format!(
                    "comment in schema mod file must target the schema itself. Expected COMMENT ON SCHEMA '{}', but found COMMENT ON {}",
                    schema_name, target
                )
            }
            Self::SchemaModGrantTargetMismatch {
                target,
                schema_name,
            } => {
                format!(
                    "grant in schema mod file must target the schema itself. Expected GRANT ON SCHEMA '{}', but found GRANT ON {}",
                    schema_name, target
                )
            }
            Self::AlterDefaultPrivilegesRequiresDatabaseScope { database_name } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES in database mod file '{}' must specify IN DATABASE",
                    database_name
                )
            }
            Self::AlterDefaultPrivilegesRequiresSchemaScope { schema_name } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES in schema mod file '{}' must specify IN SCHEMA",
                    schema_name
                )
            }
            Self::AlterDefaultPrivilegesDatabaseMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES IN DATABASE references wrong database: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::AlterDefaultPrivilegesSchemaNotAllowed { database_name } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES in database mod file '{}' cannot use IN SCHEMA",
                    database_name
                )
            }
            Self::AlterDefaultPrivilegesDatabaseNotAllowed { schema_name } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES in schema mod file '{}' cannot use IN DATABASE",
                    schema_name
                )
            }
            Self::AlterDefaultPrivilegesSchemaMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA references wrong schema: '{}' instead of '{}'",
                    referenced, expected
                )
            }
        }
    }

    /// Get the help text for this error kind
    fn help(&self) -> Option<String> {
        match self {
            Self::MultipleMainStatements { .. } => {
                Some("each file must contain exactly one primary CREATE statement (TABLE, VIEW, SOURCE, etc.)".to_string())
            }
            Self::NoMainStatement { .. } => {
                Some("each file must contain exactly one primary CREATE statement (CREATE TABLE, CREATE VIEW, etc.)".to_string())
            }
            Self::ObjectNameMismatch { .. } => {
                Some("the object name in your CREATE statement must match the .sql file name".to_string())
            }
            Self::SchemaMismatch { .. } => {
                Some("the schema in your qualified object name must match the directory name".to_string())
            }
            Self::DatabaseMismatch { .. } => {
                Some("the database in your qualified object name must match the directory name".to_string())
            }
            Self::IndexReferenceMismatch { .. } => {
                Some("indexes must be defined in the same file as the object they're created on".to_string())
            }
            Self::GrantReferenceMismatch { .. } => {
                Some("grants must be defined in the same file as the object they apply to".to_string())
            }
            Self::CommentReferenceMismatch { .. } => {
                Some("comments must be defined in the same file as the object they describe".to_string())
            }
            Self::ColumnCommentReferenceMismatch { .. } => {
                Some("column comments must reference columns in the object defined in the file".to_string())
            }
            Self::CommentTypeMismatch { .. } => {
                Some("the COMMENT statement must use the correct object type (TABLE, VIEW, etc.)".to_string())
            }
            Self::GrantTypeMismatch { .. } => {
                Some("the GRANT statement must use the correct object type that matches the object defined in the file".to_string())
            }
            Self::UnsupportedStatement { .. } => {
                Some("only CREATE, INDEX, GRANT, and COMMENT statements are supported in object files".to_string())
            }
            Self::ClusterGrantUnsupported => {
                Some("use GRANT ON specific objects instead of CLUSTER".to_string())
            }
            Self::GrantMustTargetObject => {
                Some("use GRANT ON objectname instead of GRANT ON ALL TABLES or similar".to_string())
            }
            Self::SystemGrantUnsupported => {
                Some("use GRANT ON specific objects instead of SYSTEM".to_string())
            }
            Self::UnsupportedCommentType => {
                Some("only comments on tables, views, sources, sinks, connections, secrets, and columns are supported".to_string())
            }
            Self::NoObjectType | Self::SchemaExtractionFailed | Self::DatabaseExtractionFailed => {
                Some("this is an internal error, please report this issue".to_string())
            }
            Self::InvalidIdentifier { .. } => {
                Some("identifiers must follow SQL naming rules (alphanumeric and underscores, must not start with a digit)".to_string())
            }
            Self::IndexMissingCluster { .. } => {
                Some("add 'IN CLUSTER <cluster_name>' to your CREATE INDEX statement (e.g., CREATE INDEX idx ON table (col) IN CLUSTER quickstart)".to_string())
            }
            Self::MaterializedViewMissingCluster { .. } => {
                Some("add 'IN CLUSTER <cluster_name>' to your CREATE MATERIALIZED VIEW statement (e.g., CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT ...)".to_string())
            }
            Self::InvalidDatabaseModStatement { .. } => {
                Some("database mod files (e.g., materialize.sql) can only contain COMMENT ON DATABASE, GRANT ON DATABASE, and ALTER DEFAULT PRIVILEGES statements".to_string())
            }
            Self::DatabaseModCommentTargetMismatch { .. } => {
                Some("comments in database mod files must target the database itself using COMMENT ON DATABASE".to_string())
            }
            Self::DatabaseModGrantTargetMismatch { .. } => {
                Some("grants in database mod files must target the database itself using GRANT ON DATABASE".to_string())
            }
            Self::InvalidSchemaModStatement { .. } => {
                Some("schema mod files (e.g., materialize/public.sql) can only contain COMMENT ON SCHEMA, GRANT ON SCHEMA, and ALTER DEFAULT PRIVILEGES statements".to_string())
            }
            Self::SchemaModCommentTargetMismatch { .. } => {
                Some("comments in schema mod files must target the schema itself using COMMENT ON SCHEMA".to_string())
            }
            Self::SchemaModGrantTargetMismatch { .. } => {
                Some("grants in schema mod files must target the schema itself using GRANT ON SCHEMA".to_string())
            }
            Self::AlterDefaultPrivilegesRequiresDatabaseScope { .. } => {
                Some("add 'IN DATABASE <database_name>' to your ALTER DEFAULT PRIVILEGES statement".to_string())
            }
            Self::AlterDefaultPrivilegesRequiresSchemaScope { .. } => {
                Some("add 'IN SCHEMA <schema_name>' to your ALTER DEFAULT PRIVILEGES statement".to_string())
            }
            Self::AlterDefaultPrivilegesDatabaseMismatch { .. } => {
                Some("ALTER DEFAULT PRIVILEGES in database mod files must target the database itself".to_string())
            }
            Self::AlterDefaultPrivilegesSchemaNotAllowed { .. } => {
                Some("use IN DATABASE instead of IN SCHEMA in database mod files".to_string())
            }
            Self::AlterDefaultPrivilegesDatabaseNotAllowed { .. } => {
                Some("use IN SCHEMA instead of IN DATABASE in schema mod files".to_string())
            }
            Self::AlterDefaultPrivilegesSchemaMismatch { .. } => {
                Some("ALTER DEFAULT PRIVILEGES in schema mod files must target the schema itself".to_string())
            }
        }
    }
}

/// A collection of validation errors grouped by location.
///
/// This type holds multiple validation errors that occurred during project validation.
/// It provides formatted output that groups errors by database, schema, and file for
/// easier navigation and fixing.
#[derive(Debug)]
pub struct ValidationErrors {
    pub errors: Vec<ValidationError>,
}

impl ValidationErrors {
    /// Create a new collection from a vector of errors
    pub fn new(errors: Vec<ValidationError>) -> Self {
        Self { errors }
    }

    /// Check if there are any errors
    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    /// Get the number of errors
    pub fn len(&self) -> usize {
        self.errors.len()
    }

    /// Convert into a Result, returning Err if there are any errors
    pub fn into_result(self) -> Result<(), Self> {
        if self.is_empty() { Ok(()) } else { Err(self) }
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.errors.is_empty() {
            return Ok(());
        }

        // Group errors by file path
        let mut grouped: BTreeMap<PathBuf, Vec<&ValidationError>> = BTreeMap::new();
        for error in &self.errors {
            grouped
                .entry(error.context.file.clone())
                .or_default()
                .push(error);
        }

        // Display errors grouped by file (like rustc does)
        for (file_path, errors) in grouped.iter() {
            // Extract database/schema/file for path display
            let path_components: Vec<_> = file_path.components().collect();
            let len = path_components.len();

            let relative_path = if len >= 3 {
                format!(
                    "{}/{}/{}",
                    path_components[len - 3].as_os_str().to_string_lossy(),
                    path_components[len - 2].as_os_str().to_string_lossy(),
                    path_components[len - 1].as_os_str().to_string_lossy()
                )
            } else {
                file_path.display().to_string()
            };

            // Display each error for this file
            for error in errors {
                // Format like rustc: error: <message>
                writeln!(
                    f,
                    "{}: {}",
                    "error".bright_red().bold(),
                    error.kind.message()
                )?;

                // Show file location: --> path
                writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;

                // Add SQL statement if available
                if let Some(ref sql) = error.context.sql_statement {
                    writeln!(f, "  {}", "|".bright_blue().bold())?;
                    for line in sql.lines() {
                        writeln!(f, "  {} {}", "|".bright_blue().bold(), line)?;
                    }
                    writeln!(f, "  {}", "|".bright_blue().bold())?;
                }

                // Add help text if available
                if let Some(help) = error.kind.help() {
                    writeln!(
                        f,
                        "  {} {}",
                        "=".bright_blue().bold(),
                        format!("help: {}", help).bold()
                    )?;
                }

                writeln!(f)?;
            }
        }

        // Summary line at the end (like rustc)
        writeln!(
            f,
            "{}: could not compile due to {} previous error{}",
            "error".bright_red().bold(),
            self.errors.len(),
            if self.errors.len() == 1 { "" } else { "s" }
        )?;

        Ok(())
    }
}

impl std::error::Error for ValidationErrors {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}
