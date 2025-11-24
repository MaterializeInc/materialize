//! Type checking trait and error types for Materialize projects.

use crate::project::mir::Project;
use crate::project::object_id::ObjectId;
use owo_colors::OwoColorize;
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur during type checking
#[derive(Debug, Error)]
pub enum TypeCheckError {
    /// Failed to start Docker container
    #[error("failed to start Materialize container: {0}")]
    ContainerStartFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Failed to connect to Materialize
    #[error("failed to connect to Materialize: {0}")]
    ConnectionFailed(#[from] crate::client::ConnectionError),

    /// Failed to create temporary tables for external dependencies
    #[error("failed to create temporary table for external dependency {object}: {source}")]
    ExternalDependencyFailed {
        object: ObjectId,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// Type checking failed for an object
    #[error(transparent)]
    TypeCheckFailed(#[from] ObjectTypeCheckError),

    /// Multiple type check errors occurred
    #[error(transparent)]
    Multiple(#[from] TypeCheckErrors),

    /// Database error during setup
    #[error("database error during setup: {0}")]
    DatabaseSetupError(String),

    /// Failed to get sorted objects
    #[error("failed to get sorted objects: {0}")]
    SortError(#[from] crate::project::error::DependencyError),
}

/// A single type check error for a specific object
#[derive(Debug)]
pub struct ObjectTypeCheckError {
    /// The object that failed type checking
    pub object_id: ObjectId,
    /// The file path of the object
    pub file_path: PathBuf,
    /// The SQL statement that failed
    pub sql_statement: String,
    /// The database error message
    pub error_message: String,
    /// Optional detail from the database
    pub detail: Option<String>,
    /// Optional hint from the database
    pub hint: Option<String>,
}

impl fmt::Display for ObjectTypeCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Extract database/schema/file for path display
        let path_components: Vec<_> = self.file_path.components().collect();
        let len = path_components.len();

        let relative_path = if len >= 3 {
            format!(
                "{}/{}/{}",
                path_components[len - 3].as_os_str().to_string_lossy(),
                path_components[len - 2].as_os_str().to_string_lossy(),
                path_components[len - 1].as_os_str().to_string_lossy()
            )
        } else {
            self.file_path.display().to_string()
        };

        // Format like rustc errors
        writeln!(
            f,
            "{}: type check failed for '{}'",
            "error".bright_red().bold(),
            self.object_id
        )?;
        writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;
        writeln!(f)?;

        // Show the SQL statement (first few lines if long)
        let lines: Vec<_> = self.sql_statement.lines().collect();
        writeln!(f, "  {}", "|".bright_blue().bold())?;
        for (idx, line) in lines.iter().take(10).enumerate() {
            writeln!(f, "  {} {}", "|".bright_blue().bold(), line)?;
            if idx == 9 && lines.len() > 10 {
                writeln!(
                    f,
                    "  {} ... ({} more lines)",
                    "|".bright_blue().bold(),
                    lines.len() - 10
                )?;
                break;
            }
        }
        writeln!(f, "  {}", "|".bright_blue().bold())?;
        writeln!(f)?;

        // Show error message
        writeln!(
            f,
            "  {}: {}",
            "error".bright_red().bold(),
            self.error_message
        )?;

        if let Some(ref detail) = self.detail {
            writeln!(f, "  {}: {}", "detail".bright_cyan().bold(), detail)?;
        }

        if let Some(ref hint) = self.hint {
            writeln!(
                f,
                "  {} {}",
                "=".bright_blue().bold(),
                format!("hint: {}", hint).bold()
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for ObjectTypeCheckError {}

/// Collection of type check errors
#[derive(Debug)]
pub struct TypeCheckErrors {
    pub errors: Vec<ObjectTypeCheckError>,
}

impl fmt::Display for TypeCheckErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (idx, error) in self.errors.iter().enumerate() {
            if idx > 0 {
                writeln!(f)?;
            }
            write!(f, "{}", error)?;
        }

        writeln!(f)?;
        writeln!(
            f,
            "{}: could not type check due to {} previous error{}",
            "error".bright_red().bold(),
            self.errors.len(),
            if self.errors.len() == 1 { "" } else { "s" }
        )?;

        Ok(())
    }
}

impl std::error::Error for TypeCheckErrors {}

/// Trait for type checking Materialize projects
#[async_trait::async_trait]
pub trait TypeChecker: Send + Sync {
    /// Type check a project, validating all object definitions
    ///
    /// This method:
    /// 1. Creates temporary tables/views for external dependencies (from types.lock)
    /// 2. Creates temporary views for all project objects in topological order
    /// 3. Reports any type errors found
    ///
    /// Returns Ok(()) if all objects pass type checking, or Err with detailed errors
    async fn typecheck(&self, project: &Project) -> Result<(), TypeCheckError>;
}