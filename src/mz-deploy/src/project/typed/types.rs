//! Core type definitions for the typed representation.
//!
//! This module contains the primary types used to represent a validated
//! Materialize project structure.

use super::super::ast::Statement;
use super::super::normalize::{ClusterTransformer, NameTransformer, NormalizingVisitor};
use super::super::object_id::ObjectId;
use crate::project::error::ValidationError;
use crate::project::error::ValidationErrorKind;
use mz_sql_parser::ast::*;
use std::collections::BTreeSet;
use std::path::PathBuf;

/// Fully qualified name parsed from file path structure.
///
/// Represents the canonical `database.schema.object` name based on directory structure.
/// File path format: `<root>/<database>/<schema>/<object>.sql`
///
/// This struct is created during typed validation and is used to:
/// - Normalize statement names to be fully qualified
/// - Validate that SQL statement names match the directory structure
/// - Provide a consistent FQN for error messages and validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullyQualifiedName {
    id: ObjectId,
    pub path: PathBuf,
    item_name: UnresolvedItemName,
}

impl FullyQualifiedName {
    /// Get the database name.
    #[inline]
    pub fn database(&self) -> &str {
        self.id.database()
    }

    /// Get the schema name.
    #[inline]
    pub fn schema(&self) -> &str {
        self.id.schema()
    }

    /// Get the object name.
    #[inline]
    pub fn object(&self) -> &str {
        self.id.object()
    }

    /// Get the ObjectId.
    pub fn object_id(&self) -> &ObjectId {
        &self.id
    }

    /// Get the UnresolvedItemName for updating statement names.
    pub fn to_item_name(&self) -> UnresolvedItemName {
        self.item_name.clone()
    }
}

impl From<UnresolvedItemName> for FullyQualifiedName {
    fn from(value: UnresolvedItemName) -> Self {
        let id = ObjectId::new(
            value.0[0].to_string(),
            value.0[1].to_string(),
            value.0[2].to_string(),
        );
        Self {
            id,
            path: PathBuf::new(),
            item_name: value,
        }
    }
}

impl TryFrom<(&std::path::Path, &str)> for FullyQualifiedName {
    type Error = ValidationError;

    /// Extract fully qualified name from file path.
    ///
    /// Path format: `<root>/<database>/<schema>/<object>.sql`
    /// Returns error if path structure is invalid.
    fn try_from(value: (&std::path::Path, &str)) -> Result<Self, Self::Error> {
        let (path, object_name) = value;

        // Extract schema (parent directory)
        let schema = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                ValidationError::with_file(
                    ValidationErrorKind::SchemaExtractionFailed,
                    path.to_path_buf(),
                )
            })?;

        // Extract database (parent of schema directory)
        let database = path
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.file_name())
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                ValidationError::with_file(
                    ValidationErrorKind::DatabaseExtractionFailed,
                    path.to_path_buf(),
                )
            })?;

        // Create Ident instances for each component
        let database_ident = Ident::new(database).map_err(|e| {
            ValidationError::with_file(
                ValidationErrorKind::InvalidIdentifier {
                    name: database.to_string(),
                    reason: e.to_string(),
                },
                path.to_path_buf(),
            )
        })?;

        let schema_ident = Ident::new(schema).map_err(|e| {
            ValidationError::with_file(
                ValidationErrorKind::InvalidIdentifier {
                    name: schema.to_string(),
                    reason: e.to_string(),
                },
                path.to_path_buf(),
            )
        })?;

        let object_ident = Ident::new(object_name).map_err(|e| {
            ValidationError::with_file(
                ValidationErrorKind::InvalidIdentifier {
                    name: object_name.to_string(),
                    reason: e.to_string(),
                },
                path.to_path_buf(),
            )
        })?;

        // Create the UnresolvedItemName
        let item_name = UnresolvedItemName(vec![database_ident, schema_ident, object_ident]);

        // Create ObjectId
        let id = ObjectId::new(
            database.to_string(),
            schema.to_string(),
            object_name.to_string(),
        );

        Ok(FullyQualifiedName {
            id,
            path: path.to_path_buf(),
            item_name,
        })
    }
}

/// The primary CREATE statement for a database object.
impl Statement {
    /// Normalizes the statement name to be fully qualified.
    pub fn normalize_stmt(self, fqn: &FullyQualifiedName) -> Self {
        let visitor = NormalizingVisitor::fully_qualifying(fqn);
        self.normalize_name_with(&visitor, &fqn.to_item_name())
            .normalize_dependencies_with(&visitor)
    }

    /// Normalizes the statement name using a custom transformer.
    pub fn normalize_name_with<T: NameTransformer>(
        self,
        visitor: &NormalizingVisitor<T>,
        item_name: &UnresolvedItemName,
    ) -> Self {
        let transformed_name = visitor.transformer().transform_name(item_name);

        match self {
            Statement::CreateSink(mut s) => {
                s.name = Some(transformed_name);
                Statement::CreateSink(s)
            }
            Statement::CreateView(mut s) => {
                s.definition.name = transformed_name;
                Statement::CreateView(s)
            }
            Statement::CreateMaterializedView(mut s) => {
                s.name = transformed_name;
                Statement::CreateMaterializedView(s)
            }
            Statement::CreateTable(mut s) => {
                s.name = transformed_name;
                Statement::CreateTable(s)
            }
            Statement::CreateTableFromSource(mut s) => {
                s.name = transformed_name;
                Statement::CreateTableFromSource(s)
            }
        }
    }

    /// Normalizes all object references within the statement using a custom transformer.
    pub fn normalize_dependencies_with<T: NameTransformer>(
        self,
        visitor: &NormalizingVisitor<T>,
    ) -> Self {
        match self {
            Statement::CreateView(mut s) => {
                visitor.normalize_query(&mut s.definition.query);
                Statement::CreateView(s)
            }
            Statement::CreateMaterializedView(mut s) => {
                visitor.normalize_query(&mut s.query);
                Statement::CreateMaterializedView(s)
            }
            Statement::CreateTableFromSource(mut s) => {
                visitor.normalize_raw_item_name(&mut s.source);
                Statement::CreateTableFromSource(s)
            }
            Statement::CreateSink(mut s) => {
                visitor.normalize_raw_item_name(&mut s.from);
                visitor.normalize_sink_connection(&mut s.connection);
                Statement::CreateSink(s)
            }
            // These statements don't have dependencies on other database objects
            Statement::CreateTable(_) => self,
        }
    }

    /// Normalize cluster references using a ClusterTransformer.
    ///
    /// This method is separate from normalize_dependencies_with because cluster
    /// normalization is only needed for staging environments, not regular deployments.
    pub fn normalize_cluster_with<T: ClusterTransformer>(
        self,
        visitor: &NormalizingVisitor<T>,
    ) -> Self {
        match self {
            Statement::CreateMaterializedView(mut s) => {
                visitor.normalize_cluster_name(&mut s.in_cluster);
                Statement::CreateMaterializedView(s)
            }
            Statement::CreateSink(mut s) => {
                visitor.normalize_cluster_name(&mut s.in_cluster);
                Statement::CreateSink(s)
            }
            // These statements don't have cluster references
            _ => self,
        }
    }
}

/// A validated database object with its primary statement and supporting declarations.
///
/// Represents a single database object (table, view, source, etc.) that has been
/// validated to ensure:
/// - Exactly one primary CREATE statement exists
/// - The object name matches the file name
/// - All supporting statements (indexes, grants, comments) reference this object
/// - Object types are consistent across statements
///
/// # Structure
///
/// Each `DatabaseObject` is loaded from a single `.sql` file and contains:
/// - One primary statement (CREATE TABLE, CREATE VIEW, etc.)
/// - Zero or more CREATE INDEX statements (for indexable objects)
/// - Zero or more GRANT statements
/// - Zero or more COMMENT statements
///
/// # Example
///
/// For a file `my_schema/users.sql`:
/// ```sql
/// CREATE TABLE users (
///     id INT,
///     name TEXT
/// );
///
/// CREATE INDEX users_id_idx ON users (id);
/// GRANT SELECT ON users TO analyst_role;
/// COMMENT ON TABLE users IS 'User account information';
/// ```
///
/// This would be validated and represented as a single `DatabaseObject`.
#[derive(Debug)]
pub struct DatabaseObject {
    /// The primary CREATE statement for this object
    pub stmt: Statement,
    /// Indexes defined on this object
    pub indexes: Vec<CreateIndexStatement<Raw>>,
    /// Grant statements for this object
    pub grants: Vec<GrantPrivilegesStatement<Raw>>,
    /// Comment statements for this object or its columns
    pub comments: Vec<CommentStatement<Raw>>,
    /// Unit tests for this object
    pub tests: Vec<ExecuteUnitTestStatement<Raw>>,
}

impl DatabaseObject {
    pub fn clusters(&self) -> BTreeSet<String> {
        let mut cluster_set = BTreeSet::new();
        if let Statement::CreateMaterializedView(mv) = &self.stmt {
            if let Some(RawClusterName::Unresolved(cluster_name)) = &mv.in_cluster {
                cluster_set.insert(cluster_name.to_string());
            }
        }

        if let Statement::CreateSink(sink) = &self.stmt {
            if let Some(RawClusterName::Unresolved(cluster_name)) = &sink.in_cluster {
                cluster_set.insert(cluster_name.to_string());
            }
        }

        for index in &self.indexes {
            if let Some(RawClusterName::Unresolved(cluster_name)) = &index.in_cluster {
                cluster_set.insert(cluster_name.to_string());
            }
        }
        cluster_set
    }

    /// Convert the statement to a Query<Raw> for type checking purposes.
    pub fn to_query(&self) -> Option<Query<Raw>> {
        match &self.stmt {
            Statement::CreateView(stmt) => Some(stmt.definition.query.clone()),
            Statement::CreateMaterializedView(stmt) => Some(stmt.query.clone()),
            Statement::CreateTable(_) => None,
            _ => None,
        }
    }
}

/// A validated schema containing multiple database objects.
///
/// Represents a schema directory in the project structure. Each schema contains
/// multiple database objects (tables, views, sources, etc.) that have all been
/// validated.
///
/// # Directory Mapping
///
/// A schema corresponds to a directory in the project structure:
/// ```text
/// database_name/
///   schema_name/        <- Schema
///     object1.sql       <- DatabaseObject
///     object2.sql       <- DatabaseObject
///     mod.sql           (optional, not represented in HIR)
/// ```
///
/// Note: `mod.sql` files are parsed in the raw representation but not carried
/// forward to the HIR, as they typically contain schema-level setup statements.
#[derive(Debug)]
pub struct Schema {
    /// The name of the schema (directory name)
    pub name: String,
    /// All validated database objects in this schema
    pub objects: Vec<DatabaseObject>,
    /// Optional module-level statements (from schema.sql file)
    pub mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
}

/// A validated database containing multiple schemas.
///
/// Represents a database directory in the project structure. Each database contains
/// multiple schemas, each of which contains multiple database objects.
///
/// # Directory Mapping
///
/// A database corresponds to a directory in the project structure:
/// ```text
/// project_root/
///   database_name/      <- Database
///     schema1/          <- Schema
///       object1.sql
///     schema2/          <- Schema
///       object2.sql
///     mod.sql           (optional, not represented in HIR)
/// ```
#[derive(Debug)]
pub struct Database {
    /// The name of the database (directory name)
    pub name: String,
    /// All validated schemas in this database
    pub schemas: Vec<Schema>,
    /// Optional module-level statements (from database.sql file)
    pub mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
}

/// A fully validated Materialize project.
///
/// Represents the complete validated project structure, containing all databases,
/// schemas, and objects. This is the top-level typed project that should be used after
/// successfully loading and validating a project from the file system.
///
/// # Usage
///
/// ```no_run
/// use mz_deploy::project::raw;
/// use mz_deploy::project::typed::Project;
///
/// // Load raw project from file system
/// let raw_project = raw::load_project("./my_project").unwrap();
///
/// // Convert to validated typed project
/// let typed_project = Project::try_from(raw_project).unwrap();
/// ```
///
/// # Validation Guarantees
///
/// A successfully created `Project` guarantees:
/// - All object names match their file names
/// - All qualified names match the directory structure
/// - All supporting statements reference the correct objects
/// - All object types are consistent across statements
/// - No unsupported statement types are present
#[derive(Debug)]
pub struct Project {
    /// All validated databases in this project
    pub databases: Vec<Database>,
}
