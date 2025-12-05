//! Type checking trait and error types for Materialize projects.

use crate::client::Client;
use crate::project::ast::Statement;
use crate::project::normalize::NormalizingVisitor;
use crate::project::object_id::ObjectId;
use crate::project::planned::Project;
use crate::project::typed::FullyQualifiedName;
use crate::verbose;
use mz_sql_parser::ast::{
    CreateViewStatement, Ident, IfExistsBehavior, UnresolvedItemName, ViewDefinition,
};
use owo_colors::OwoColorize;
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
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

/// Type check a project using a pre-configured client
///
/// This function performs type checking by creating temporary views/tables for all project
/// objects in topological order. The client should already be connected and have external
/// dependencies staged as temporary tables.
///
/// # Arguments
/// * `client` - A connected client with external dependencies already staged
/// * `project` - The project to type check
/// * `project_root` - Root directory of the project (for error reporting)
///
/// # Returns
/// Ok(()) if all objects pass type checking, or Err with detailed errors
pub async fn typecheck_with_client(
    client: &mut Client,
    project: &Project,
    project_root: &Path,
) -> Result<(), TypeCheckError> {
    // Type check objects in topological order
    let object_paths = build_object_paths(project, project_root);
    let sorted_objects = project.get_sorted_objects()?;

    verbose!(
        "Type checking {} objects in topological order",
        sorted_objects.len()
    );
    let mut errors = Vec::new();

    for (object_id, typed_object) in sorted_objects {
        verbose!("Type checking: {}", object_id);

        // Build the FQN from the object_id
        let fqn = FullyQualifiedName::from(UnresolvedItemName(vec![
            Ident::new(&object_id.database).expect("valid database"),
            Ident::new(&object_id.schema).expect("valid schema"),
            Ident::new(&object_id.object).expect("valid object"),
        ]));

        if let Some(statement) = create_temporary_view_sql(&typed_object.stmt, &fqn) {
            let sql = statement.to_string();
            match client.execute(&sql.to_string(), &[]).await {
                Ok(_) => {
                    verbose!("  ✓ Type check passed");
                }
                Err(e) => {
                    // ConnectionError wraps tokio_postgres::Error, extract the database error
                    let (error_message, detail, hint) = match &e {
                        crate::client::ConnectionError::Query(pg_err) => {
                            if let Some(db_err) = pg_err.as_db_error() {
                                (
                                    db_err.message().to_string(),
                                    db_err.detail().map(|s| s.to_string()),
                                    db_err.hint().map(|s| s.to_string()),
                                )
                            } else {
                                (pg_err.to_string(), None, None)
                            }
                        }
                        _ => (e.to_string(), None, None),
                    };

                    verbose!("  ✗ Type check failed: {}", error_message);

                    let path = object_paths.get(&object_id).cloned().unwrap_or_else(|| {
                        project_root
                            .join(&object_id.database)
                            .join(&object_id.schema)
                            .join(format!("{}.sql", object_id.object))
                    });

                    errors.push(ObjectTypeCheckError {
                        object_id: object_id.clone(),
                        file_path: path,
                        sql_statement: sql,
                        error_message,
                        detail,
                        hint,
                    });
                }
            }
        } else {
            verbose!("  - Skipping non-view/table object");
        }
    }

    if errors.is_empty() {
        verbose!("All type checks passed!");
        Ok(())
    } else {
        Err(TypeCheckError::Multiple(TypeCheckErrors { errors }))
    }
}

/// Build object path mapping for error reporting
fn build_object_paths(project: &Project, project_root: &Path) -> BTreeMap<ObjectId, PathBuf> {
    let mut paths = BTreeMap::new();
    for obj in project.iter_objects() {
        let path = project_root
            .join(&obj.id.database)
            .join(&obj.id.schema)
            .join(format!("{}.sql", obj.id.object));
        paths.insert(obj.id.clone(), path);
    }
    paths
}

/// Create temporary view for a project object
fn create_temporary_view_sql(stmt: &Statement, fqn: &FullyQualifiedName) -> Option<Statement> {
    let visitor = NormalizingVisitor::flattening(fqn);

    match stmt {
        Statement::CreateView(view) => {
            let mut view = view.clone();
            view.temporary = true;

            let normalized = Statement::CreateView(view)
                .normalize_name_with(&visitor, &fqn.to_item_name())
                .normalize_dependencies_with(&visitor);

            Some(normalized)
        }
        Statement::CreateMaterializedView(mv) => {
            let view_stmt = CreateViewStatement {
                if_exists: IfExistsBehavior::Error,
                temporary: true,
                definition: ViewDefinition {
                    name: mv.name.clone(),
                    columns: mv.columns.clone(),
                    query: mv.query.clone(),
                },
            };
            let normalized = Statement::CreateView(view_stmt)
                .normalize_name_with(&visitor, &fqn.to_item_name())
                .normalize_dependencies_with(&visitor);

            Some(normalized)
        }
        Statement::CreateTable(_) | Statement::CreateTableFromSource(_) => {
            // loaded from types.lock
            None
        }
        _ => {
            // Other statement types (sources, sinks, connections, secrets)
            // cannot be type-checked in this way
            None
        }
    }
}
