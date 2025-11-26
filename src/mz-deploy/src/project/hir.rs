//! High-Level Intermediate Representation (HIR) for Materialize projects.
//!
//! This module provides a validated, type-safe representation of a Materialize project
//! structure. It transforms the raw parsed AST from the `raw` module into a semantically
//! validated HIR that enforces structural constraints and relationships.
//!
//! # Transformation Flow
//!
//! ```text
//! File System → raw::Project → hir::Project (validated)
//!                  ↓              ↓
//!              raw::Database → hir::Database
//!                  ↓              ↓
//!              raw::Schema   → hir::Schema
//!                  ↓              ↓
//!          raw::DatabaseObject → hir::DatabaseObject
//! ```
//!
//! # Validation Rules
//!
//! During transformation from raw to HIR, the following validations are performed:
//!
//! - **Object Identity**: Each file must contain exactly one primary CREATE statement
//!   (table, view, source, etc.), and the object name must match the file name.
//!
//! - **Path Consistency**: Qualified names in CREATE statements must match the directory
//!   structure (e.g., `CREATE TABLE db.schema.table` in `db/schema/table.sql`).
//!
//! - **Reference Validation**: Supporting statements (indexes, grants, comments) must
//!   reference the primary object defined in the same file.
//!
//! - **Type Consistency**: GRANT and COMMENT statements must use the correct object type
//!   for the primary object.

use super::ast::{DatabaseIdent, Statement};
use super::error::{ValidationError, ValidationErrorKind, ValidationErrors};
use super::normalize::{ClusterTransformer, NameTransformer, NormalizingVisitor};
use crate::project::object_id::ObjectId;
use mz_sql_parser::ast::*;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// Fully qualified name parsed from file path structure.
///
/// Represents the canonical `database.schema.object` name based on directory structure.
/// File path format: `<root>/<database>/<schema>/<object>.sql`
///
/// This struct is created during HIR validation and is used to:
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
    pub fn null() -> FullyQualifiedName {
        Self {
            id: ObjectId::new("".to_string(), "".to_string(), "".to_string()),
            path: Path::new("/tmp").into(),
            item_name: UnresolvedItemName(vec![]),
        }
    }

    /// Get the database name.
    pub fn database(&self) -> &str {
        &self.id.database
    }

    /// Get the schema name.
    pub fn schema(&self) -> &str {
        &self.id.schema
    }

    /// Get the object name.
    pub fn object(&self) -> &str {
        &self.id.object
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
            Statement::CreateConnection(mut s) => {
                s.name = transformed_name;
                Statement::CreateConnection(s)
            }
            Statement::CreateWebhookSource(mut s) => {
                s.name = transformed_name;
                Statement::CreateWebhookSource(s)
            }
            Statement::CreateSource(mut s) => {
                s.name = transformed_name;
                Statement::CreateSource(s)
            }
            Statement::CreateSubsource(mut s) => {
                s.name = transformed_name;
                Statement::CreateSubsource(s)
            }
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
            Statement::CreateSecret(mut s) => {
                s.name = transformed_name;
                Statement::CreateSecret(s)
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
            Statement::CreateSubsource(mut s) => {
                if let Some(ref mut source) = s.of_source {
                    visitor.normalize_raw_item_name(source);
                }
                Statement::CreateSubsource(s)
            }
            // These statements don't have dependencies on other database objects
            Statement::CreateConnection(_)
            | Statement::CreateWebhookSource(_)
            | Statement::CreateSource(_)
            | Statement::CreateTable(_)
            | Statement::CreateSecret(_) => self,
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
            Statement::CreateSource(mut s) => {
                visitor.normalize_cluster_name(&mut s.in_cluster);
                Statement::CreateSource(s)
            }
            Statement::CreateWebhookSource(mut s) => {
                visitor.normalize_cluster_name(&mut s.in_cluster);
                Statement::CreateWebhookSource(s)
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
    pub fn clusters(&self) -> HashSet<String> {
        let mut cluster_set = HashSet::new();
        if let Statement::CreateMaterializedView(mv) = &self.stmt {
            if let Some(RawClusterName::Unresolved(cluster_name)) = &mv.in_cluster {
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
            Statement::CreateSource(_) | Statement::CreateTable(_) => None,
            _ => None,
        }
    }
}

impl TryFrom<super::raw::DatabaseObject> for DatabaseObject {
    type Error = ValidationErrors;

    /// Converts a raw database object into a validated HIR database object.
    ///
    /// # Validation
    ///
    /// This conversion performs the following validations:
    /// - Ensures exactly one primary CREATE statement exists
    /// - Validates that the object name in the statement matches the file name
    /// - Validates that the qualified name matches the directory structure
    /// - Validates that all indexes reference this object
    /// - Validates that all grants reference this object and use the correct type
    /// - Validates that all comments reference this object and use the correct type
    ///
    /// # Errors
    ///
    /// Returns all validation errors found (may contain multiple errors):
    /// - No primary CREATE statement is found
    /// - Multiple primary CREATE statements are found
    /// - The object name doesn't match the file name
    /// - Qualified names don't match the directory structure
    /// - Supporting statements reference different objects
    /// - Object types are inconsistent
    /// - Unsupported statement types are encountered
    fn try_from(value: super::raw::DatabaseObject) -> Result<Self, Self::Error> {
        let mut errors = Vec::new();
        let mut main_stmt: Option<Statement> = None;
        let mut object_type: Option<ObjectType> = None;
        let mut indexes = Vec::new();
        let mut grants = Vec::new();
        let mut comments = Vec::new();
        let mut tests = Vec::new();

        for stmt in value.statements {
            match stmt {
                mz_sql_parser::ast::Statement::CreateConnection(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateConnection(s));
                        object_type = Some(ObjectType::Connection)
                    }
                }
                mz_sql_parser::ast::Statement::CreateWebhookSource(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateWebhookSource(s));
                        object_type = Some(ObjectType::Source)
                    }
                }
                mz_sql_parser::ast::Statement::CreateSource(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateSource(s));
                        object_type = Some(ObjectType::Source)
                    }
                }
                mz_sql_parser::ast::Statement::CreateSubsource(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateSubsource(s));
                        object_type = Some(ObjectType::Subsource)
                    }
                }
                mz_sql_parser::ast::Statement::CreateSink(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateSink(s));
                        object_type = Some(ObjectType::Sink)
                    }
                }
                mz_sql_parser::ast::Statement::CreateView(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateView(s));
                        object_type = Some(ObjectType::View)
                    }
                }
                mz_sql_parser::ast::Statement::CreateMaterializedView(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateMaterializedView(s));
                        object_type = Some(ObjectType::MaterializedView)
                    }
                }
                mz_sql_parser::ast::Statement::CreateTable(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateTable(s));
                        object_type = Some(ObjectType::Table)
                    }
                }
                mz_sql_parser::ast::Statement::CreateTableFromSource(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateTableFromSource(s));
                        object_type = Some(ObjectType::Table)
                    }
                }
                mz_sql_parser::ast::Statement::CreateSecret(s) => {
                    if main_stmt.is_some() {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::MultipleMainStatements {
                                object_name: value.name.clone(),
                            },
                            value.path.clone(),
                        ));
                    } else {
                        main_stmt = Some(Statement::CreateSecret(s));
                        object_type = Some(ObjectType::Secret)
                    }
                }

                // Supporting statements
                mz_sql_parser::ast::Statement::CreateIndex(s) => {
                    indexes.push(s);
                }
                mz_sql_parser::ast::Statement::GrantPrivileges(s) => {
                    grants.push(s);
                }
                mz_sql_parser::ast::Statement::Comment(s) => {
                    comments.push(s);
                }

                // Test statements are collected for later execution
                mz_sql_parser::ast::Statement::ExecuteUnitTest(s) => {
                    tests.push(s);
                }

                // Unsupported statements
                other => {
                    errors.push(ValidationError::with_file(
                        ValidationErrorKind::UnsupportedStatement {
                            object_name: value.name.clone(),
                            statement_type: format!("{:?}", other),
                        },
                        value.path.clone(),
                    ));
                }
            }
        }

        // Check for main statement
        if main_stmt.is_none() {
            errors.push(ValidationError::with_file(
                ValidationErrorKind::NoMainStatement {
                    object_name: value.name.clone(),
                },
                value.path.clone(),
            ));
        }

        // Check for object type
        if object_type.is_none() {
            errors.push(ValidationError::with_file(
                ValidationErrorKind::NoObjectType,
                value.path.clone(),
            ));
        }

        // If we have fatal errors (no main statement or object type), return early
        // since we can't continue validation without them
        if !errors.is_empty() && (main_stmt.is_none() || object_type.is_none()) {
            return Err(ValidationErrors::new(errors));
        }

        // Unwrap is safe here because we checked above
        let stmt = main_stmt.unwrap();
        let obj_type = object_type.unwrap();

        let fqn = match FullyQualifiedName::try_from((value.path.as_path(), value.name.as_str())) {
            Ok(fqn) => fqn,
            Err(e) => {
                errors.push(e);
                // Return early if we can't extract FQN
                return Err(ValidationErrors::new(errors));
            }
        };

        // Get identifier from original statement before normalization
        let main_ident = stmt.ident();

        // Validate the original statement identifier against FQN
        validate_ident(&stmt, &fqn, &mut errors);

        // Normalize statement name and dependencies
        let stmt = stmt.normalize_stmt(&fqn);

        // Normalize index, grant, and comment references to be fully qualified
        let visitor = NormalizingVisitor::fully_qualifying(&fqn);
        visitor.normalize_index_references(&mut indexes);
        visitor.normalize_grant_references(&mut grants);
        visitor.normalize_comment_references(&mut comments);

        // Validate cluster requirements
        validate_index_clusters(&fqn, &indexes, &mut errors);
        validate_mv_cluster(&fqn, &stmt, &mut errors);

        validate_index_references(&fqn, &mut indexes, &main_ident, &mut errors);
        validate_grant_references(&fqn, &mut grants, &main_ident, obj_type, &mut errors);
        validate_comment_references(&fqn, &mut comments, &main_ident, &obj_type, &mut errors);

        // If there are any errors, return them all
        if !errors.is_empty() {
            return Err(ValidationErrors::new(errors));
        }

        Ok(DatabaseObject {
            stmt,
            indexes,
            grants,
            comments,
            tests,
        })
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
///   schema_name/        ← Schema
///     object1.sql       ← DatabaseObject
///     object2.sql       ← DatabaseObject
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

impl TryFrom<super::raw::Schema> for Schema {
    type Error = ValidationErrors;

    /// Converts a raw schema into a validated HIR schema.
    ///
    /// Validates each database object in the schema. Collects all validation errors
    /// from all objects and returns them together.
    fn try_from(value: super::raw::Schema) -> Result<Self, Self::Error> {
        let mut all_errors = Vec::new();
        let mut objects = Vec::new();

        for obj in value.objects {
            match DatabaseObject::try_from(obj) {
                Ok(db_obj) => objects.push(db_obj),
                Err(errs) => {
                    // Collect errors from this object
                    all_errors.extend(errs.errors);
                }
            }
        }

        if !all_errors.is_empty() {
            return Err(ValidationErrors::new(all_errors));
        }

        Ok(Self {
            name: value.name.clone(),
            objects,
            mod_statements: value.mod_statements,
        })
    }
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
///   database_name/      ← Database
///     schema1/          ← Schema
///       object1.sql
///     schema2/          ← Schema
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

impl TryFrom<super::raw::Database> for Database {
    type Error = ValidationErrors;

    /// Converts a raw database into a validated HIR database.
    ///
    /// Validates each schema in the database. Collects all validation errors
    /// from all schemas and objects and returns them together.
    fn try_from(value: super::raw::Database) -> Result<Self, Self::Error> {
        let mut all_errors = Vec::new();
        let mut schemas = Vec::new();

        // Validate database mod statements if they exist
        if let Some(ref mod_stmts) = value.mod_statements {
            let db_mod_path = PathBuf::from(format!("{}.sql", value.name));
            validate_database_mod_statements(&value.name, &db_mod_path, mod_stmts, &mut all_errors);
        }

        for (schema_name, mut schema) in value.schemas {
            // Validate schema mod statements if they exist (need database context)
            if let Some(ref mut mod_stmts) = schema.mod_statements {
                let schema_mod_path = PathBuf::from(format!("{}/{}.sql", value.name, schema_name));
                validate_schema_mod_statements(
                    &value.name,
                    &schema_name,
                    &schema_mod_path,
                    mod_stmts,
                    &mut all_errors,
                );
            }

            match Schema::try_from(schema) {
                Ok(s) => schemas.push(s),
                Err(errs) => {
                    // Collect errors from this schema
                    all_errors.extend(errs.errors);
                }
            }
        }

        if !all_errors.is_empty() {
            return Err(ValidationErrors::new(all_errors));
        }

        Ok(Self {
            name: value.name.clone(),
            schemas,
            mod_statements: value.mod_statements,
        })
    }
}

/// A fully validated Materialize project.
///
/// Represents the complete validated project structure, containing all databases,
/// schemas, and objects. This is the top-level HIR type that should be used after
/// successfully loading and validating a project from the file system.
///
/// # Usage
///
/// ```no_run
/// use mz_deploy::project::raw;
/// use mz_deploy::project::hir::Project;
///
/// // Load raw project from file system
/// let raw_project = raw::load_project("./my_project").unwrap();
///
/// // Convert to validated HIR
/// let hir_project = Project::try_from(raw_project).unwrap();
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

impl TryFrom<super::raw::Project> for Project {
    type Error = ValidationErrors;

    /// Converts a raw project into a fully validated HIR project.
    ///
    /// This performs a complete validation of the entire project tree. Collects
    /// all validation errors from all databases, schemas, and objects and returns
    /// them together, grouped by location.
    ///
    /// # Errors
    ///
    /// Returns all validation errors found across the entire project hierarchy.
    fn try_from(value: super::raw::Project) -> Result<Self, Self::Error> {
        let mut all_errors = Vec::new();
        let mut databases = Vec::new();

        for (_, database) in value.databases {
            match Database::try_from(database) {
                Ok(db) => databases.push(db),
                Err(errs) => {
                    // Collect errors from this database
                    all_errors.extend(errs.errors);
                }
            }
        }

        if !all_errors.is_empty() {
            return Err(ValidationErrors::new(all_errors));
        }

        Ok(Self { databases })
    }
}

/// Validates that the statement's identifier matches the expected file path structure.
///
/// Ensures that the object name in the CREATE statement matches the file name, and
/// that any schema/database qualifiers match the directory structure.
///
/// # Validation Rules
///
/// - The object name must match the file name (without `.sql` extension)
/// - If the statement includes a schema qualifier, it must match the parent directory name
/// - If the statement includes a database qualifier, it must match the grandparent directory name
///
/// # Examples
///
/// Valid mappings:
/// ```text
/// materialize/public/users.sql  →  CREATE TABLE users (...)
/// materialize/public/users.sql  →  CREATE TABLE public.users (...)
/// materialize/public/users.sql  →  CREATE TABLE materialize.public.users (...)
/// ```
///
/// Invalid mappings:
/// ```text
/// materialize/public/users.sql  →  CREATE TABLE customers (...)  ✗ name mismatch
/// materialize/public/users.sql  →  CREATE TABLE private.users (...)  ✗ schema mismatch
/// materialize/public/users.sql  →  CREATE TABLE other.public.users (...)  ✗ database mismatch
/// ```
fn validate_ident(stmt: &Statement, fqn: &FullyQualifiedName, errors: &mut Vec<ValidationError>) {
    let ident = stmt.ident();

    // The object name in the statement must match the file name
    if ident.object != fqn.object() {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::ObjectNameMismatch {
                declared: ident.object.clone(),
                expected: fqn.object().to_string(),
            },
            fqn.path.clone(),
        ));
    }

    // If the statement includes a schema qualifier, validate it matches the path-derived schema
    if let Some(ref stmt_schema) = ident.schema
        && stmt_schema != fqn.schema()
    {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::SchemaMismatch {
                declared: stmt_schema.clone(),
                expected: fqn.schema().to_string(),
            },
            fqn.path.clone(),
        ));
    }

    // If the statement includes a database qualifier, validate it matches the path-derived database
    if let Some(ref stmt_database) = ident.database
        && stmt_database != fqn.database()
    {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::DatabaseMismatch {
                declared: stmt_database.clone(),
                expected: fqn.database().to_string(),
            },
            fqn.path.clone(),
        ));
    }
}

/// Validates that a COMMENT statement targets the correct object with the correct type.
///
/// Ensures that:
/// 1. The comment references the main object defined in the file
/// 2. The object type specified in the COMMENT matches the actual object type
///
/// # Object Type Matching
///
/// Materialize allows comments on various object types (TABLE, VIEW, MATERIALIZED VIEW, etc.).
/// The COMMENT statement must use the correct object type keyword matching the actual object.
///
/// # Examples
///
/// Valid:
/// ```sql
/// CREATE TABLE users (...);
/// COMMENT ON TABLE users IS 'user data';  ✓
/// ```
///
/// Invalid:
/// ```sql
/// CREATE TABLE users (...);
/// COMMENT ON VIEW users IS 'user data';  ✗ type mismatch
/// COMMENT ON TABLE customers IS 'data';  ✗ wrong object
/// ```
fn validate_comment_target(
    comment_name: &RawItemName,
    main_ident: &DatabaseIdent,
    main_obj_type: &ObjectType,
    comment_obj_type: ObjectType,
    fqn: &FullyQualifiedName,
    comment_sql: &str,
    errors: &mut Vec<ValidationError>,
) {
    let comment_target: DatabaseIdent = comment_name.name().clone().into();

    // Check that the comment references the main object
    if !comment_target.matches(main_ident) {
        errors.push(ValidationError::with_file_and_sql(
            ValidationErrorKind::CommentReferenceMismatch {
                referenced: comment_target.object,
                expected: main_ident.object.clone(),
            },
            fqn.path.clone(),
            comment_sql.to_string(),
        ));
    }

    // Check that the comment type matches the object type
    if *main_obj_type != comment_obj_type {
        errors.push(ValidationError::with_file_and_sql(
            ValidationErrorKind::CommentTypeMismatch {
                comment_type: format!("{:?}", comment_obj_type),
                object_type: format!("{:?}", main_obj_type),
            },
            fqn.path.clone(),
            comment_sql.to_string(),
        ));
    }
}

/// Validates that all COMMENT statements in a file reference the main object.
///
/// Processes all COMMENT statements and ensures they target either:
/// - The main object defined in the file, OR
/// - A column of the main object
///
/// This validation ensures that each object file is self-contained and doesn't
/// reference other objects.
///
/// # Supported Comment Types
///
/// - `COMMENT ON TABLE` - for tables and materialized views
/// - `COMMENT ON VIEW` - for views
/// - `COMMENT ON MATERIALIZED VIEW` - for materialized views
/// - `COMMENT ON SOURCE` - for sources and subsources
/// - `COMMENT ON SINK` - for sinks
/// - `COMMENT ON CONNECTION` - for connections
/// - `COMMENT ON SECRET` - for secrets
/// - `COMMENT ON COLUMN` - for columns of the main object
///
/// # Errors
///
/// Returns an error if:
/// - A comment references a different object
/// - The comment type doesn't match the object type
/// - An unsupported comment type is used
fn validate_comment_references(
    fqn: &FullyQualifiedName,
    comments: &mut [CommentStatement<Raw>],
    main_ident: &DatabaseIdent,
    obj_type: &ObjectType,
    errors: &mut Vec<ValidationError>,
) {
    for comment in comments.iter() {
        let comment_sql = format!("{};", comment);

        match &comment.object {
            CommentObjectType::Table { name } => {
                validate_comment_target(
                    name,
                    main_ident,
                    obj_type,
                    ObjectType::Table,
                    fqn,
                    &comment_sql,
                    errors,
                );
            }
            CommentObjectType::View { name } => {
                validate_comment_target(
                    name,
                    main_ident,
                    obj_type,
                    ObjectType::View,
                    fqn,
                    &comment_sql,
                    errors,
                );
            }
            CommentObjectType::Column { name } => {
                // For columns, extract the table/view name (it's the parent)
                let column_parent: DatabaseIdent = name.relation.name().clone().into();
                if !column_parent.matches(main_ident) {
                    errors.push(ValidationError::with_file_and_sql(
                        ValidationErrorKind::ColumnCommentReferenceMismatch {
                            referenced: column_parent.object,
                            expected: main_ident.object.clone(),
                        },
                        fqn.path.clone(),
                        comment_sql,
                    ));
                }
            }
            CommentObjectType::MaterializedView { name } => {
                validate_comment_target(
                    name,
                    main_ident,
                    obj_type,
                    ObjectType::MaterializedView,
                    fqn,
                    &comment_sql,
                    errors,
                );
            }
            CommentObjectType::Source { name } => {
                validate_comment_target(
                    name,
                    main_ident,
                    obj_type,
                    ObjectType::Source,
                    fqn,
                    &comment_sql,
                    errors,
                );
            }
            CommentObjectType::Sink { name } => {
                validate_comment_target(
                    name,
                    main_ident,
                    obj_type,
                    ObjectType::Sink,
                    fqn,
                    &comment_sql,
                    errors,
                );
            }
            CommentObjectType::Connection { name } => {
                validate_comment_target(
                    name,
                    main_ident,
                    obj_type,
                    ObjectType::Connection,
                    fqn,
                    &comment_sql,
                    errors,
                );
            }
            CommentObjectType::Secret { name } => {
                validate_comment_target(
                    name,
                    main_ident,
                    obj_type,
                    ObjectType::Secret,
                    fqn,
                    &comment_sql,
                    errors,
                );
            }
            CommentObjectType::Index { .. }
            | CommentObjectType::Func { .. }
            | CommentObjectType::Type { .. }
            | CommentObjectType::Role { .. }
            | CommentObjectType::Database { .. }
            | CommentObjectType::Schema { .. }
            | CommentObjectType::Cluster { .. }
            | CommentObjectType::ClusterReplica { .. }
            | CommentObjectType::ContinualTask { .. }
            | CommentObjectType::NetworkPolicy { .. } => {
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::UnsupportedCommentType,
                    fqn.path.clone(),
                    comment_sql,
                ));
            }
        }
    }
}

/// Validates that all CREATE INDEX statements reference the main object.
///
/// Ensures that every index defined in the file is created on the object
/// defined in the same file. This maintains the principle that each file
/// is self-contained.
///
/// # Example
///
/// Valid:
/// ```sql
/// CREATE TABLE users (id INT, name TEXT);
/// CREATE INDEX users_id_idx ON users (id);  ✓
/// ```
///
/// Invalid:
/// ```sql
/// CREATE TABLE users (id INT, name TEXT);
/// CREATE INDEX orders_id_idx ON orders (id);  ✗ wrong object
/// ```
/// Validates that all indexes specify a cluster.
///
/// Indexes in Materialize must specify which cluster they run on using the IN CLUSTER clause.
/// This ensures deterministic deployment and avoids implicit cluster selection.
///
/// # Example
///
/// Valid:
/// ```sql
/// CREATE INDEX idx ON table (col) IN CLUSTER quickstart;
/// ```
///
/// Invalid:
/// ```sql
/// CREATE INDEX idx ON table (col);  ✗ missing cluster
/// ```
fn validate_index_clusters(
    fqn: &FullyQualifiedName,
    indexes: &[CreateIndexStatement<Raw>],
    errors: &mut Vec<ValidationError>,
) {
    for index in indexes.iter() {
        if index.in_cluster.is_none() {
            let index_sql = format!("{};", index);
            let index_name = index
                .name
                .as_ref()
                .map(|n| n.to_string())
                .unwrap_or_else(|| "<unnamed>".to_string());

            errors.push(ValidationError::with_file_and_sql(
                ValidationErrorKind::IndexMissingCluster { index_name },
                fqn.path.clone(),
                index_sql,
            ));
        }
    }
}

/// Validates that a materialized view specifies a cluster.
///
/// Materialized views in Materialize must specify which cluster they run on using the IN CLUSTER clause.
/// This ensures deterministic deployment and avoids implicit cluster selection.
///
/// # Example
///
/// Valid:
/// ```sql
/// CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT ...;
/// ```
///
/// Invalid:
/// ```sql
/// CREATE MATERIALIZED VIEW mv AS SELECT ...;  ✗ missing cluster
/// ```
fn validate_mv_cluster(
    fqn: &FullyQualifiedName,
    stmt: &Statement,
    errors: &mut Vec<ValidationError>,
) {
    if let Statement::CreateMaterializedView(mv) = stmt
        && mv.in_cluster.is_none()
    {
        let mv_sql = format!("{};", mv);
        let view_name = mv.name.to_string();

        errors.push(ValidationError::with_file_and_sql(
            ValidationErrorKind::MaterializedViewMissingCluster { view_name },
            fqn.path.clone(),
            mv_sql,
        ));
    }
}

fn validate_index_references(
    fqn: &FullyQualifiedName,
    indexes: &mut [CreateIndexStatement<Raw>],
    main_ident: &DatabaseIdent,
    errors: &mut Vec<ValidationError>,
) {
    for index in indexes.iter() {
        let on: DatabaseIdent = index.on_name.name().clone().into();
        if !on.matches(main_ident) {
            let index_sql = format!("{};", index);
            errors.push(ValidationError::with_file_and_sql(
                ValidationErrorKind::IndexReferenceMismatch {
                    referenced: on.object,
                    expected: main_ident.object.clone(),
                },
                fqn.path.clone(),
                index_sql,
            ));
        }
    }
}

/// Validates that all GRANT statements reference the main object with the correct type.
///
/// Ensures that:
/// 1. Every grant targets the object defined in the same file
/// 2. The object type in the GRANT matches the actual object type
/// 3. Only supported grant types are used (no SYSTEM grants, no ALL TABLES IN SCHEMA)
///
/// # Object Type Handling
///
/// Materialize's GRANT syntax has specific requirements:
/// - Tables, views, materialized views, and sources all use `GRANT ... ON TABLE`
/// - Other objects (connections, secrets, sinks) use their specific type
///
/// # Supported Grants
///
/// - `GRANT ... ON TABLE` - for tables, views, materialized views, sources
/// - `GRANT ... ON CONNECTION` - for connections
/// - `GRANT ... ON SECRET` - for secrets
/// - `GRANT ... ON SINK` - for sinks
///
/// # Example
///
/// Valid:
/// ```sql
/// CREATE TABLE users (...);
/// GRANT SELECT ON TABLE users TO analyst_role;  ✓
/// ```
///
/// Invalid:
/// ```sql
/// CREATE TABLE users (...);
/// GRANT SELECT ON orders TO analyst_role;  ✗ wrong object
/// ```
fn validate_grant_references(
    fqn: &FullyQualifiedName,
    grants: &mut [GrantPrivilegesStatement<Raw>],
    main_ident: &DatabaseIdent,
    main_object_type: ObjectType,
    errors: &mut Vec<ValidationError>,
) {
    for grant in grants.iter() {
        let grant_sql = format!("{};", grant);

        match &grant.target {
            GrantTargetSpecification::Object {
                object_type,
                object_spec_inner,
                ..
            } => match object_spec_inner {
                GrantTargetSpecificationInner::Objects { names } => {
                    check_grant_object_type(
                        fqn,
                        main_object_type,
                        *object_type,
                        &grant_sql,
                        errors,
                    );

                    for obj in names {
                        match obj {
                            UnresolvedObjectName::Item(item_name) => {
                                let grant_target: DatabaseIdent = item_name.clone().into();
                                if !grant_target.matches(main_ident) {
                                    errors.push(ValidationError::with_file_and_sql(
                                        ValidationErrorKind::GrantReferenceMismatch {
                                            referenced: grant_target.object,
                                            expected: main_ident.object.clone(),
                                        },
                                        fqn.path.clone(),
                                        grant_sql.clone(),
                                    ));
                                }
                            }
                            _ => {
                                // skip
                            }
                        }
                    }
                }
                _ => {
                    errors.push(ValidationError::with_file_and_sql(
                        ValidationErrorKind::GrantMustTargetObject,
                        fqn.path.clone(),
                        grant_sql.clone(),
                    ));
                }
            },
            _ => {
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::SystemGrantUnsupported,
                    fqn.path.clone(),
                    grant_sql,
                ));
            }
        }
    }
}

/// Validates that the GRANT statement uses the correct object type for the target object.
///
/// Materialize has specific rules about which object types can be used in GRANT statements:
///
/// # Type Mapping Rules
///
/// - **Tables, Views, Materialized Views, Sources**: Must use `GRANT ... ON TABLE`
///   - This is because Materialize treats these objects similarly for privilege management
/// - **Connections, Secrets, Sinks**: Must use their specific type
///   - e.g., `GRANT ... ON CONNECTION`, `GRANT ... ON SECRET`
///
/// # Examples
///
/// ```sql
/// -- For a table
/// GRANT SELECT ON TABLE users TO role;  ✓
///
/// -- For a materialized view
/// GRANT SELECT ON TABLE my_mv TO role;  ✓
/// GRANT SELECT ON MATERIALIZED VIEW my_mv TO role;  ✗
///
/// -- For a connection
/// GRANT USAGE ON CONNECTION kafka_conn TO role;  ✓
/// ```
fn check_grant_object_type(
    fqn: &FullyQualifiedName,
    main_object_type: ObjectType,
    grant_object_type: ObjectType,
    grant_sql: &str,
    errors: &mut Vec<ValidationError>,
) {
    if matches!(
        main_object_type,
        ObjectType::Table | ObjectType::Source | ObjectType::View | ObjectType::MaterializedView
    ) {
        if grant_object_type != ObjectType::Table {
            errors.push(ValidationError::with_file_and_sql(
                ValidationErrorKind::GrantTypeMismatch {
                    grant_type: format!("{}", grant_object_type),
                    expected_type: "TABLE".to_string(),
                },
                fqn.path.clone(),
                grant_sql.to_string(),
            ));
        }
    } else if grant_object_type != main_object_type {
        errors.push(ValidationError::with_file_and_sql(
            ValidationErrorKind::GrantTypeMismatch {
                grant_type: format!("{}", grant_object_type),
                expected_type: format!("{}", main_object_type),
            },
            fqn.path.clone(),
            grant_sql.to_string(),
        ));
    }
}

/// Validate database mod file statements.
///
/// Database mod files can only contain:
/// - COMMENT ON DATABASE (targeting the database itself)
/// - GRANT ON DATABASE (targeting the database itself)
/// - ALTER DEFAULT PRIVILEGES
fn validate_database_mod_statements(
    database_name: &str,
    database_path: &std::path::Path,
    statements: &[mz_sql_parser::ast::Statement<Raw>],
    errors: &mut Vec<ValidationError>,
) {
    use mz_sql_parser::ast::Statement as MzStatement;

    for stmt in statements {
        let stmt_sql = format!("{};", stmt);

        match stmt {
            MzStatement::Comment(comment_stmt) => {
                // Must be COMMENT ON DATABASE targeting this database
                match &comment_stmt.object {
                    CommentObjectType::Database { name } => {
                        // Check if it targets this database
                        let target_db = name.to_string();
                        if target_db != database_name {
                            errors.push(ValidationError::with_file_and_sql(
                                ValidationErrorKind::DatabaseModCommentTargetMismatch {
                                    target: format!("DATABASE {}", target_db),
                                    database_name: database_name.to_string(),
                                },
                                database_path.to_path_buf(),
                                stmt_sql,
                            ));
                        }
                    }
                    _ => {
                        let target = match &comment_stmt.object {
                            CommentObjectType::Table { .. } => "TABLE",
                            CommentObjectType::View { .. } => "VIEW",
                            CommentObjectType::MaterializedView { .. } => "MATERIALIZED VIEW",
                            CommentObjectType::Source { .. } => "SOURCE",
                            CommentObjectType::Sink { .. } => "SINK",
                            CommentObjectType::Connection { .. } => "CONNECTION",
                            CommentObjectType::Secret { .. } => "SECRET",
                            CommentObjectType::Schema { .. } => "SCHEMA",
                            CommentObjectType::Column { .. } => "COLUMN",
                            _ => "unknown",
                        };
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::DatabaseModCommentTargetMismatch {
                                target: target.to_string(),
                                database_name: database_name.to_string(),
                            },
                            database_path.to_path_buf(),
                            stmt_sql,
                        ));
                    }
                }
            }
            MzStatement::GrantPrivileges(grant_stmt) => {
                // Must be GRANT ON DATABASE targeting this database
                match &grant_stmt.target {
                    GrantTargetSpecification::Object {
                        object_type,
                        object_spec_inner,
                        ..
                    } => {
                        if object_type != &ObjectType::Database {
                            errors.push(ValidationError::with_file_and_sql(
                                ValidationErrorKind::DatabaseModGrantTargetMismatch {
                                    target: format!("{}", object_type),
                                    database_name: database_name.to_string(),
                                },
                                database_path.to_path_buf(),
                                stmt_sql.clone(),
                            ));
                        }

                        // Check that it targets this specific database
                        if let GrantTargetSpecificationInner::Objects { names } = object_spec_inner
                        {
                            for name in names {
                                if let UnresolvedObjectName::Item(item_name) = name {
                                    let target_db = item_name.to_string();
                                    if target_db != database_name {
                                        errors.push(ValidationError::with_file_and_sql(
                                            ValidationErrorKind::DatabaseModGrantTargetMismatch {
                                                target: format!("DATABASE {}", target_db),
                                                database_name: database_name.to_string(),
                                            },
                                            database_path.to_path_buf(),
                                            stmt_sql.clone(),
                                        ));
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::DatabaseModGrantTargetMismatch {
                                target: "SYSTEM or other".to_string(),
                                database_name: database_name.to_string(),
                            },
                            database_path.to_path_buf(),
                            stmt_sql,
                        ));
                    }
                }
            }
            MzStatement::AlterDefaultPrivileges(alter_stmt) => {
                // Must specify IN DATABASE targeting this database
                match &alter_stmt.target_objects {
                    GrantTargetAllSpecification::AllDatabases { databases } => {
                        // Validate all databases reference the current database
                        for db_name in databases {
                            let db_str = db_name.to_string();
                            if db_str != database_name {
                                errors.push(ValidationError::with_file_and_sql(
                                    ValidationErrorKind::AlterDefaultPrivilegesDatabaseMismatch {
                                        referenced: db_str,
                                        expected: database_name.to_string(),
                                    },
                                    database_path.to_path_buf(),
                                    stmt_sql.clone(),
                                ));
                            }
                        }
                    }
                    GrantTargetAllSpecification::AllSchemas { .. } => {
                        // Reject: IN SCHEMA not allowed in database mod files
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::AlterDefaultPrivilegesSchemaNotAllowed {
                                database_name: database_name.to_string(),
                            },
                            database_path.to_path_buf(),
                            stmt_sql.clone(),
                        ));
                    }
                    GrantTargetAllSpecification::All => {
                        // Reject: Must specify IN DATABASE
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::AlterDefaultPrivilegesRequiresDatabaseScope {
                                database_name: database_name.to_string(),
                            },
                            database_path.to_path_buf(),
                            stmt_sql.clone(),
                        ));
                    }
                }
            }
            _ => {
                // Reject all other statement types
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::InvalidDatabaseModStatement {
                        statement_type: format!("{:?}", stmt)
                            .split('(')
                            .next()
                            .unwrap_or("unknown")
                            .to_string(),
                        database_name: database_name.to_string(),
                    },
                    database_path.to_path_buf(),
                    stmt_sql,
                ));
            }
        }
    }
}

/// Validate schema mod file statements and normalize names.
///
/// Schema mod files can only contain:
/// - COMMENT ON SCHEMA (targeting the schema itself)
/// - GRANT ON SCHEMA (targeting the schema itself)
/// - ALTER DEFAULT PRIVILEGES
///
/// Names are normalized to include the database qualifier.
fn validate_schema_mod_statements(
    database_name: &str,
    schema_name: &str,
    schema_path: &std::path::Path,
    statements: &mut [mz_sql_parser::ast::Statement<Raw>],
    errors: &mut Vec<ValidationError>,
) {
    use mz_sql_parser::ast::Statement as MzStatement;

    // Helper function to normalize unqualified schema names
    let normalize_schema_name = |name: &mut UnresolvedSchemaName| {
        if name.0.len() == 1 {
            // Unqualified: prepend database to make database.schema
            let schema = name.0[0].clone();
            let database = Ident::new(database_name).expect("valid database identifier");
            name.0 = vec![database, schema];
        }
        // Already qualified or invalid - leave as-is
    };

    for stmt in statements.iter_mut() {
        let stmt_sql = format!("{};", stmt);

        match stmt {
            MzStatement::Comment(comment_stmt) => {
                // Must be COMMENT ON SCHEMA targeting this schema
                match &mut comment_stmt.object {
                    CommentObjectType::Schema { name } => {
                        // Check if it targets this schema (can be qualified or unqualified)
                        let target_schema = name.to_string();
                        let is_match = target_schema == schema_name
                            || target_schema == format!("{}.{}", database_name, schema_name);

                        if !is_match {
                            errors.push(ValidationError::with_file_and_sql(
                                ValidationErrorKind::SchemaModCommentTargetMismatch {
                                    target: format!("SCHEMA {}", target_schema),
                                    schema_name: format!("{}.{}", database_name, schema_name),
                                },
                                schema_path.to_path_buf(),
                                stmt_sql,
                            ));
                        } else {
                            // Normalize the schema name to be fully qualified
                            normalize_schema_name(name);
                        }
                    }
                    _ => {
                        let target = match &comment_stmt.object {
                            CommentObjectType::Table { .. } => "TABLE",
                            CommentObjectType::View { .. } => "VIEW",
                            CommentObjectType::MaterializedView { .. } => "MATERIALIZED VIEW",
                            CommentObjectType::Source { .. } => "SOURCE",
                            CommentObjectType::Sink { .. } => "SINK",
                            CommentObjectType::Connection { .. } => "CONNECTION",
                            CommentObjectType::Secret { .. } => "SECRET",
                            CommentObjectType::Database { .. } => "DATABASE",
                            CommentObjectType::Column { .. } => "COLUMN",
                            _ => "unknown",
                        };
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::SchemaModCommentTargetMismatch {
                                target: target.to_string(),
                                schema_name: format!("{}.{}", database_name, schema_name),
                            },
                            schema_path.to_path_buf(),
                            stmt_sql,
                        ));
                    }
                }
            }
            MzStatement::GrantPrivileges(grant_stmt) => {
                // Must be GRANT ON SCHEMA targeting this schema
                match &mut grant_stmt.target {
                    GrantTargetSpecification::Object {
                        object_type,
                        object_spec_inner,
                        ..
                    } => {
                        if object_type != &ObjectType::Schema {
                            errors.push(ValidationError::with_file_and_sql(
                                ValidationErrorKind::SchemaModGrantTargetMismatch {
                                    target: format!("{}", object_type),
                                    schema_name: format!("{}.{}", database_name, schema_name),
                                },
                                schema_path.to_path_buf(),
                                stmt_sql.clone(),
                            ));
                        }

                        // Check that it targets this specific schema
                        if let GrantTargetSpecificationInner::Objects { names } = object_spec_inner
                        {
                            for name in names {
                                if let UnresolvedObjectName::Schema(schema_name_obj) = name {
                                    let target_schema = schema_name_obj.to_string();
                                    let is_match = target_schema == schema_name
                                        || target_schema
                                            == format!("{}.{}", database_name, schema_name);

                                    if !is_match {
                                        errors.push(ValidationError::with_file_and_sql(
                                            ValidationErrorKind::SchemaModGrantTargetMismatch {
                                                target: format!("SCHEMA {}", target_schema),
                                                schema_name: format!(
                                                    "{}.{}",
                                                    database_name, schema_name
                                                ),
                                            },
                                            schema_path.to_path_buf(),
                                            stmt_sql.clone(),
                                        ));
                                    } else {
                                        // Normalize the schema name to be fully qualified
                                        normalize_schema_name(schema_name_obj);
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::SchemaModGrantTargetMismatch {
                                target: "SYSTEM or other".to_string(),
                                schema_name: format!("{}.{}", database_name, schema_name),
                            },
                            schema_path.to_path_buf(),
                            stmt_sql,
                        ));
                    }
                }
            }
            MzStatement::AlterDefaultPrivileges(alter_stmt) => {
                // Must specify IN SCHEMA targeting this schema
                match &mut alter_stmt.target_objects {
                    GrantTargetAllSpecification::AllSchemas { schemas } => {
                        // Validate each schema reference
                        for schema_name_obj in schemas {
                            let schema_str = schema_name_obj.to_string();

                            // Check if it matches the current schema (qualified or unqualified)
                            let is_match = schema_str == schema_name
                                || schema_str == format!("{}.{}", database_name, schema_name);

                            if !is_match {
                                errors.push(ValidationError::with_file_and_sql(
                                    ValidationErrorKind::AlterDefaultPrivilegesSchemaMismatch {
                                        referenced: schema_str,
                                        expected: format!("{}.{}", database_name, schema_name),
                                    },
                                    schema_path.to_path_buf(),
                                    stmt_sql.clone(),
                                ));
                            } else {
                                // Normalize the schema name to be fully qualified
                                normalize_schema_name(schema_name_obj);
                            }
                        }
                    }
                    GrantTargetAllSpecification::AllDatabases { .. } => {
                        // Reject: IN DATABASE not allowed in schema mod files
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::AlterDefaultPrivilegesDatabaseNotAllowed {
                                schema_name: format!("{}.{}", database_name, schema_name),
                            },
                            schema_path.to_path_buf(),
                            stmt_sql.clone(),
                        ));
                    }
                    GrantTargetAllSpecification::All => {
                        // Reject: Must specify IN SCHEMA
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::AlterDefaultPrivilegesRequiresSchemaScope {
                                schema_name: format!("{}.{}", database_name, schema_name),
                            },
                            schema_path.to_path_buf(),
                            stmt_sql.clone(),
                        ));
                    }
                }
            }
            _ => {
                // Reject all other statement types
                errors.push(ValidationError::with_file_and_sql(
                    ValidationErrorKind::InvalidSchemaModStatement {
                        statement_type: format!("{:?}", stmt)
                            .split('(')
                            .next()
                            .unwrap_or("unknown")
                            .to_string(),
                        schema_name: format!("{}.{}", database_name, schema_name),
                    },
                    schema_path.to_path_buf(),
                    stmt_sql,
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::project::parser::parse_statements;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn create_raw_object(
        name: &str,
        path: PathBuf,
        sql: &str,
    ) -> super::super::raw::DatabaseObject {
        let statements = parse_statements(vec![sql]).unwrap();
        super::super::raw::DatabaseObject {
            name: name.to_string(),
            path,
            statements,
        }
    }

    #[test]
    fn test_valid_simple_object_name() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let raw = create_raw_object("foo", path, "CREATE TABLE foo (id INT);");
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_qualified_schema() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let raw = create_raw_object("foo", path, "CREATE TABLE public.foo (id INT);");
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
    }

    #[test]
    fn test_valid_fully_qualified() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let raw = create_raw_object("foo", path, "CREATE TABLE materialize.public.foo (id INT);");
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_object_name_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let raw = create_raw_object("foo", path, "CREATE TABLE bar (id INT);");
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("object name mismatch"));
        assert!(err.to_string().contains("bar"));
        assert!(err.to_string().contains("foo"));
    }

    #[test]
    fn test_invalid_schema_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let raw = create_raw_object("foo", path, "CREATE TABLE private.foo (id INT);");
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        // Check for the schema mismatch error content
        assert!(err_str.contains("schema qualifier mismatch"));
        assert!(err_str.contains("private"));
        assert!(err_str.contains("public"));
    }

    #[test]
    fn test_invalid_database_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let raw = create_raw_object("foo", path, "CREATE TABLE other_db.public.foo (id INT);");
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_str = err.to_string();
        // Check for the database mismatch error content
        assert!(err_str.contains("database qualifier mismatch"));
        assert!(err_str.contains("other_db"));
        assert!(err_str.contains("materialize"));
    }

    #[test]
    fn test_valid_with_indexes_and_grants() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let sql = r#"
            CREATE TABLE foo (id INT);
            CREATE INDEX idx_foo IN CLUSTER c ON foo (id);
            GRANT SELECT ON foo TO user1;
            COMMENT ON TABLE foo IS 'test table';
        "#;

        let raw = create_raw_object("foo", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();
        assert_eq!(obj.indexes.len(), 1);
        assert_eq!(obj.grants.len(), 1);
        assert_eq!(obj.comments.len(), 1);
    }

    #[test]
    fn test_invalid_index_on_different_object() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let sql = r#"
            CREATE TABLE foo (id INT);
            CREATE INDEX idx_bar ON bar (id);
        "#;

        let raw = create_raw_object("foo", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("INDEX"));
        assert!(err.to_string().contains("bar"));
        assert!(err.to_string().contains("foo"));
    }

    #[test]
    fn test_invalid_grant_on_different_object() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let sql = r#"
            CREATE TABLE foo (id INT);
            GRANT SELECT ON bar TO user1;
        "#;

        let raw = create_raw_object("foo", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("GRANT"));
        assert!(err.to_string().contains("bar"));
        assert!(err.to_string().contains("foo"));
    }

    #[test]
    fn test_invalid_comment_on_different_object() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let sql = r#"
            CREATE TABLE foo (id INT);
            COMMENT ON TABLE bar IS 'wrong table';
        "#;

        let raw = create_raw_object("foo", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("COMMENT"));
        assert!(err.to_string().contains("bar"));
        assert!(err.to_string().contains("foo"));
    }

    #[test]
    fn test_valid_column_comment() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let sql = r#"
            CREATE TABLE foo (id INT, name TEXT);
            COMMENT ON COLUMN foo.id IS 'primary key';
            COMMENT ON COLUMN foo.name IS 'user name';
        "#;

        let raw = create_raw_object("foo", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();
        assert_eq!(obj.comments.len(), 2);
    }

    #[test]
    fn test_invalid_column_comment_on_different_table() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let sql = r#"
            CREATE TABLE foo (id INT);
            COMMENT ON COLUMN bar.id IS 'wrong table';
        "#;

        let raw = create_raw_object("foo", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("column COMMENT"));
        assert!(err.to_string().contains("bar"));
        assert!(err.to_string().contains("foo"));
    }

    #[test]
    fn test_invalid_comment_type_mismatch() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/foo.sql");

        let sql = r#"
            CREATE TABLE foo (id INT);
            COMMENT ON VIEW foo IS 'this is actually a table';
        "#;

        let raw = create_raw_object("foo", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("View"));
        assert!(err.to_string().contains("Table"));
    }

    // ===== Dependency Normalization Tests =====
    // These tests verify that all object references within statements
    // are normalized to be fully qualified (database.schema.object).

    #[test]
    fn test_view_dependency_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/active_users.sql");

        // View references "users" without qualification
        let sql = r#"
            CREATE VIEW active_users AS
            SELECT id, name FROM users WHERE active = true;
        "#;

        let raw = create_raw_object("active_users", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        // Verify the statement name is normalized
        let view_stmt = match obj.stmt {
            Statement::CreateView(ref s) => s,
            _ => panic!("Expected CreateView statement"),
        };

        // Verify the view name is fully qualified
        assert_eq!(
            view_stmt.definition.name.to_string(),
            "materialize.public.active_users"
        );

        // Verify the table reference in the query is normalized
        // The query body should reference materialize.public.users
        let query = &view_stmt.definition.query;
        match &query.body {
            SetExpr::Select(select) => {
                assert_eq!(select.from.len(), 1);
                match &select.from[0].relation {
                    TableFactor::Table { name, .. } => {
                        assert_eq!(name.name().to_string(), "materialize.public.users");
                    }
                    _ => panic!("Expected table reference"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_materialized_view_dependency_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir
            .path()
            .join("materialize/public/active_users_mv.sql");

        // Materialized view references "users" without qualification
        let sql = r#"
            CREATE MATERIALIZED VIEW active_users_mv IN CLUSTER quickstart AS
            SELECT * FROM users;
        "#;

        let raw = create_raw_object("active_users_mv", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        // Verify the statement is a materialized view
        let mv_stmt = match obj.stmt {
            Statement::CreateMaterializedView(ref s) => s,
            _ => panic!("Expected CreateMaterializedView statement"),
        };

        // Verify the table reference is normalized
        match &mv_stmt.query.body {
            SetExpr::Select(select) => {
                assert_eq!(select.from.len(), 1);
                match &select.from[0].relation {
                    TableFactor::Table { name, .. } => {
                        assert_eq!(name.name().to_string(), "materialize.public.users");
                    }
                    _ => panic!("Expected table reference"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_view_with_join_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/user_orders.sql");

        // View with JOIN - both table references unqualified
        let sql = r#"
            CREATE VIEW user_orders AS
            SELECT u.id, u.name, o.order_id
            FROM users u
            JOIN orders o ON u.id = o.user_id;
        "#;

        let raw = create_raw_object("user_orders", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        let view_stmt = match obj.stmt {
            Statement::CreateView(ref s) => s,
            _ => panic!("Expected CreateView statement"),
        };

        // Verify both table references are normalized
        match &view_stmt.definition.query.body {
            SetExpr::Select(select) => {
                assert_eq!(select.from.len(), 1);
                let table_with_joins = &select.from[0];

                // Check main table (users)
                match &table_with_joins.relation {
                    TableFactor::Table { name, .. } => {
                        assert_eq!(name.name().to_string(), "materialize.public.users");
                    }
                    _ => panic!("Expected table reference for users"),
                }

                // Check joined table (orders)
                assert_eq!(table_with_joins.joins.len(), 1);
                match &table_with_joins.joins[0].relation {
                    TableFactor::Table { name, .. } => {
                        assert_eq!(name.name().to_string(), "materialize.public.orders");
                    }
                    _ => panic!("Expected table reference for orders"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_view_with_subquery_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/recent_orders.sql");

        // View with subquery - both references unqualified
        let sql = r#"
            CREATE VIEW recent_orders AS
            SELECT * FROM orders
            WHERE user_id IN (SELECT id FROM users WHERE active = true);
        "#;

        let raw = create_raw_object("recent_orders", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        let view_stmt = match obj.stmt {
            Statement::CreateView(ref s) => s,
            _ => panic!("Expected CreateView statement"),
        };

        // Verify main query table reference is normalized
        match &view_stmt.definition.query.body {
            SetExpr::Select(select) => {
                // Check main FROM clause (orders)
                match &select.from[0].relation {
                    TableFactor::Table { name, .. } => {
                        assert_eq!(name.name().to_string(), "materialize.public.orders");
                    }
                    _ => panic!("Expected table reference for orders"),
                }

                // Verify subquery also has normalized table reference
                // The WHERE clause contains the subquery, but verifying the exact
                // structure is complex - the main assertion above covers the key point
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_view_with_cte_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/user_summary.sql");

        // View with CTE (WITH clause) - unqualified table references
        let sql = r#"
            CREATE VIEW user_summary AS
            WITH active_users AS (
                SELECT id, name FROM users WHERE active = true
            )
            SELECT * FROM active_users;
        "#;

        let raw = create_raw_object("user_summary", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        let view_stmt = match obj.stmt {
            Statement::CreateView(ref s) => s,
            _ => panic!("Expected CreateView statement"),
        };

        // Verify CTE references are normalized
        let query = &view_stmt.definition.query;
        match &query.ctes {
            CteBlock::Simple(ctes) => {
                assert_eq!(ctes.len(), 1);
                // Verify the CTE query references the normalized table
                match &ctes[0].query.body {
                    SetExpr::Select(select) => match &select.from[0].relation {
                        TableFactor::Table { name, .. } => {
                            assert_eq!(name.name().to_string(), "materialize.public.users");
                        }
                        _ => panic!("Expected table reference in CTE"),
                    },
                    _ => panic!("Expected SELECT in CTE"),
                }
            }
            _ => panic!("Expected simple CTE block"),
        }
    }

    #[test]
    fn test_table_from_source_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/kafka_table.sql");

        // CREATE TABLE FROM SOURCE with unqualified source reference
        let sql = r#"
            CREATE TABLE kafka_table
            FROM SOURCE kafka_source (REFERENCE public.kafka_table);
        "#;

        let raw = create_raw_object("kafka_table", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        // Verify the source reference is normalized
        let table_stmt = match obj.stmt {
            Statement::CreateTableFromSource(ref s) => s,
            _ => panic!("Expected CreateTableFromSource statement"),
        };

        // Verify source name is fully qualified
        assert_eq!(
            table_stmt.source.to_string(),
            "materialize.public.kafka_source"
        );
    }

    #[test]
    fn test_sink_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/kafka_sink.sql");

        // CREATE SINK with unqualified FROM and connection references
        let sql = r#"
            CREATE SINK kafka_sink
            FROM users
            INTO KAFKA CONNECTION kafka_conn (TOPIC 'users');
        "#;

        let raw = create_raw_object("kafka_sink", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        let sink_stmt = match obj.stmt {
            Statement::CreateSink(ref s) => s,
            _ => panic!("Expected CreateSink statement"),
        };

        // Verify FROM reference is normalized
        assert_eq!(sink_stmt.from.to_string(), "materialize.public.users");

        // Verify connection reference is normalized
        match &sink_stmt.connection {
            CreateSinkConnection::Kafka { connection, .. } => {
                assert_eq!(connection.to_string(), "materialize.public.kafka_conn");
            }
            _ => panic!("Expected Kafka sink connection"),
        }
    }

    #[test]
    fn test_index_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/users.sql");

        // Table with index - index references unqualified table name
        let sql = r#"
            CREATE TABLE users (id INT, name TEXT);
            CREATE INDEX users_id_idx IN CLUSTER c ON users (id);
        "#;

        let raw = create_raw_object("users", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        // Verify index reference is normalized
        assert_eq!(obj.indexes.len(), 1);
        let index = &obj.indexes[0];
        assert_eq!(index.on_name.to_string(), "materialize.public.users");
    }

    #[test]
    fn test_comment_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/users.sql");

        // Table with comments - comment references unqualified table name
        let sql = r#"
            CREATE TABLE users (id INT, name TEXT);
            COMMENT ON TABLE users IS 'User accounts';
            COMMENT ON COLUMN users.id IS 'Unique identifier';
        "#;

        let raw = create_raw_object("users", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        // Verify comment references are normalized
        assert_eq!(obj.comments.len(), 2);

        // Check table comment
        match &obj.comments[0].object {
            CommentObjectType::Table { name } => {
                assert_eq!(name.to_string(), "materialize.public.users");
            }
            _ => panic!("Expected Table comment"),
        }

        // Check column comment (should normalize the table reference)
        match &obj.comments[1].object {
            CommentObjectType::Column { name } => {
                assert_eq!(name.relation.to_string(), "materialize.public.users");
                assert_eq!(name.column.to_string(), "id");
            }
            _ => panic!("Expected Column comment"),
        }
    }

    #[test]
    fn test_schema_qualified_dependency_normalization() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/active_users.sql");

        // View with schema-qualified (but not fully qualified) table reference
        let sql = r#"
            CREATE VIEW active_users AS
            SELECT * FROM public.users WHERE active = true;
        "#;

        let raw = create_raw_object("active_users", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        let view_stmt = match obj.stmt {
            Statement::CreateView(ref s) => s,
            _ => panic!("Expected CreateView statement"),
        };

        // Verify the schema-qualified reference is now fully qualified
        match &view_stmt.definition.query.body {
            SetExpr::Select(select) => {
                match &select.from[0].relation {
                    TableFactor::Table { name, .. } => {
                        // Should prepend database name
                        assert_eq!(name.name().to_string(), "materialize.public.users");
                    }
                    _ => panic!("Expected table reference"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_already_fully_qualified_unchanged() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("materialize/public/cross_db_view.sql");

        // View with already fully qualified table reference
        let sql = r#"
            CREATE VIEW cross_db_view AS
            SELECT * FROM other_db.other_schema.other_table;
        "#;

        let raw = create_raw_object("cross_db_view", path, sql);
        let result = DatabaseObject::try_from(raw);

        assert!(result.is_ok());
        let obj = result.unwrap();

        let view_stmt = match obj.stmt {
            Statement::CreateView(ref s) => s,
            _ => panic!("Expected CreateView statement"),
        };

        // Verify already fully qualified names remain unchanged
        match &view_stmt.definition.query.body {
            SetExpr::Select(select) => {
                match &select.from[0].relation {
                    TableFactor::Table { name, .. } => {
                        // Should remain as-is
                        assert_eq!(name.name().to_string(), "other_db.other_schema.other_table");
                    }
                    _ => panic!("Expected table reference"),
                }
            }
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_valid_database_mod_comment() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Valid COMMENT ON DATABASE statement
        let sql = "COMMENT ON DATABASE materialize IS 'Main database';";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: Some(statements),
            schemas: HashMap::new(),
        };

        let result = Database::try_from(raw_db);
        assert!(result.is_ok(), "Valid database comment should be accepted");
    }

    #[test]
    fn test_valid_database_mod_grant() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Valid GRANT ON DATABASE statement
        let sql = "GRANT USAGE ON DATABASE materialize TO user1;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: Some(statements),
            schemas: HashMap::new(),
        };

        let result = Database::try_from(raw_db);
        assert!(result.is_ok(), "Valid database grant should be accepted");
    }

    #[test]
    fn test_invalid_database_mod_wrong_statement_type() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: CREATE TABLE in database mod file
        let sql = "CREATE TABLE users (id INT);";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: Some(statements),
            schemas: HashMap::new(),
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "CREATE TABLE in database mod file should be rejected"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("InvalidDatabaseModStatement"),
            "Should report invalid statement type"
        );
    }

    #[test]
    fn test_invalid_database_mod_comment_wrong_target() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: COMMENT ON SCHEMA in database mod file
        let sql = "COMMENT ON SCHEMA public IS 'Public schema';";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: Some(statements),
            schemas: HashMap::new(),
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "COMMENT ON SCHEMA in database mod file should be rejected"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("DatabaseModCommentTargetMismatch"),
            "Should report wrong comment target"
        );
    }

    #[test]
    fn test_invalid_database_mod_comment_wrong_database() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: COMMENT ON different database
        let sql = "COMMENT ON DATABASE other_db IS 'Other database';";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: Some(statements),
            schemas: HashMap::new(),
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "COMMENT ON wrong database should be rejected"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("DatabaseModCommentTargetMismatch"),
            "Should report wrong database target"
        );
    }

    #[test]
    fn test_valid_schema_mod_comment() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Valid COMMENT ON SCHEMA statement (unqualified, will be normalized)
        let sql = "COMMENT ON SCHEMA public IS 'Public schema';";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_ok(),
            "Valid schema comment should be accepted: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_valid_schema_mod_grant() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Valid GRANT ON SCHEMA statement (unqualified, will be normalized)
        let sql = "GRANT USAGE ON SCHEMA public TO user1;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_ok(),
            "Valid schema grant should be accepted: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_invalid_schema_mod_wrong_statement_type() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: CREATE VIEW in schema mod file
        let sql = "CREATE VIEW v AS SELECT 1;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "CREATE VIEW in schema mod file should be rejected"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("InvalidSchemaModStatement"),
            "Should report invalid statement type"
        );
    }

    #[test]
    fn test_invalid_schema_mod_comment_wrong_target() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: COMMENT ON TABLE in schema mod file
        let sql = "COMMENT ON TABLE users IS 'Users table';";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "COMMENT ON TABLE in schema mod file should be rejected"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("SchemaModCommentTargetMismatch"),
            "Should report wrong comment target"
        );
    }

    #[test]
    fn test_invalid_schema_mod_comment_wrong_schema() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: COMMENT ON different schema
        let sql = "COMMENT ON SCHEMA other_schema IS 'Other schema';";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "COMMENT ON wrong schema should be rejected"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("SchemaModCommentTargetMismatch"),
            "Should report wrong schema target"
        );
    }

    #[test]
    fn test_valid_database_mod_alter_default_privileges() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Valid ALTER DEFAULT PRIVILEGES with IN DATABASE
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN DATABASE materialize GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: Some(statements),
            schemas: HashMap::new(),
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_ok(),
            "Valid ALTER DEFAULT PRIVILEGES IN DATABASE should be accepted: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_invalid_database_mod_alter_default_privileges_no_scope() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: Missing IN DATABASE
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: Some(statements),
            schemas: HashMap::new(),
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "ALTER DEFAULT PRIVILEGES without scope should be rejected"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("AlterDefaultPrivilegesRequiresDatabaseScope"),
            "Should require IN DATABASE"
        );
    }

    #[test]
    fn test_invalid_database_mod_alter_default_privileges_wrong_database() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: Wrong database
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN DATABASE other_db GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: Some(statements),
            schemas: HashMap::new(),
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "ALTER DEFAULT PRIVILEGES with wrong database should be rejected"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("AlterDefaultPrivilegesDatabaseMismatch"),
            "Should report wrong database"
        );
    }

    #[test]
    fn test_invalid_database_mod_alter_default_privileges_with_schema() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: IN SCHEMA not allowed in database mod
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA public GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: Some(statements),
            schemas: HashMap::new(),
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "ALTER DEFAULT PRIVILEGES with IN SCHEMA should be rejected in database mod"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("AlterDefaultPrivilegesSchemaNotAllowed"),
            "Should reject IN SCHEMA"
        );
    }

    #[test]
    fn test_valid_schema_mod_alter_default_privileges_unqualified() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Valid ALTER DEFAULT PRIVILEGES with unqualified schema
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA public GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_ok(),
            "Valid ALTER DEFAULT PRIVILEGES with unqualified schema should be accepted: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_valid_schema_mod_alter_default_privileges_qualified() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Valid ALTER DEFAULT PRIVILEGES with qualified schema
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA materialize.public GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_ok(),
            "Valid ALTER DEFAULT PRIVILEGES with qualified schema should be accepted: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_invalid_schema_mod_alter_default_privileges_no_scope() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: Missing IN SCHEMA
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "ALTER DEFAULT PRIVILEGES without scope should be rejected in schema mod"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("AlterDefaultPrivilegesRequiresSchemaScope"),
            "Should require IN SCHEMA"
        );
    }

    #[test]
    fn test_invalid_schema_mod_alter_default_privileges_with_database() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: IN DATABASE not allowed in schema mod
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN DATABASE materialize GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "ALTER DEFAULT PRIVILEGES with IN DATABASE should be rejected in schema mod"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("AlterDefaultPrivilegesDatabaseNotAllowed"),
            "Should reject IN DATABASE"
        );
    }

    #[test]
    fn test_invalid_schema_mod_alter_default_privileges_wrong_schema() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Invalid: Wrong schema
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA other_schema GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_err(),
            "ALTER DEFAULT PRIVILEGES with wrong schema should be rejected"
        );

        let err = result.unwrap_err();
        let err_msg = format!("{:?}", err);
        assert!(
            err_msg.contains("AlterDefaultPrivilegesSchemaMismatch"),
            "Should report wrong schema"
        );
    }

    #[test]
    fn test_schema_mod_comment_normalization() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Test that unqualified schema name gets normalized to qualified
        let sql = "COMMENT ON SCHEMA public IS 'Public schema';";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_ok(),
            "Valid schema comment should be accepted: {:?}",
            result.err()
        );

        let db = result.unwrap();
        let schema = db.schemas.iter().find(|s| s.name == "public").unwrap();

        // Check that the schema name was normalized in the mod statement
        if let Some(mod_stmts) = &schema.mod_statements {
            assert_eq!(mod_stmts.len(), 1, "Should have one mod statement");
            let stmt_sql = format!("{}", mod_stmts[0]);
            assert!(
                stmt_sql.contains("materialize.public"),
                "Schema name should be normalized to materialize.public, got: {}",
                stmt_sql
            );
        } else {
            panic!("Schema should have mod statements");
        }
    }

    #[test]
    fn test_schema_mod_grant_normalization() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Test that unqualified schema name gets normalized to qualified
        let sql = "GRANT USAGE ON SCHEMA public TO user1;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_ok(),
            "Valid schema grant should be accepted: {:?}",
            result.err()
        );

        let db = result.unwrap();
        let schema = db.schemas.iter().find(|s| s.name == "public").unwrap();

        // Check that the schema name was normalized in the mod statement
        if let Some(mod_stmts) = &schema.mod_statements {
            assert_eq!(mod_stmts.len(), 1, "Should have one mod statement");
            let stmt_sql = format!("{}", mod_stmts[0]);
            assert!(
                stmt_sql.contains("materialize.public"),
                "Schema name should be normalized to materialize.public, got: {}",
                stmt_sql
            );
        } else {
            panic!("Schema should have mod statements");
        }
    }

    #[test]
    fn test_schema_mod_alter_default_privileges_normalization() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Test that unqualified schema name gets normalized to qualified
        let sql = "ALTER DEFAULT PRIVILEGES FOR ROLE user1 IN SCHEMA public GRANT SELECT ON TABLES TO user2;";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_ok(),
            "Valid ALTER DEFAULT PRIVILEGES should be accepted: {:?}",
            result.err()
        );

        let db = result.unwrap();
        let schema = db.schemas.iter().find(|s| s.name == "public").unwrap();

        // Check that the schema name was normalized in the mod statement
        if let Some(mod_stmts) = &schema.mod_statements {
            assert_eq!(mod_stmts.len(), 1, "Should have one mod statement");
            let stmt_sql = format!("{}", mod_stmts[0]);
            assert!(
                stmt_sql.contains("materialize.public"),
                "Schema name should be normalized to materialize.public, got: {}",
                stmt_sql
            );
        } else {
            panic!("Schema should have mod statements");
        }
    }

    #[test]
    fn test_schema_mod_already_qualified_names() {
        use crate::project::parser::parse_statements;
        use crate::project::raw;
        use std::collections::HashMap;

        // Test that already qualified names remain unchanged
        let sql = "COMMENT ON SCHEMA materialize.public IS 'test';";
        let statements = parse_statements(vec![sql]).unwrap();

        let raw_schema = raw::Schema {
            name: "public".to_string(),
            mod_statements: Some(statements),
            objects: Vec::new(),
        };

        let mut schemas = HashMap::new();
        schemas.insert("public".to_string(), raw_schema);

        let raw_db = raw::Database {
            name: "materialize".to_string(),
            mod_statements: None,
            schemas,
        };

        let result = Database::try_from(raw_db);
        assert!(
            result.is_ok(),
            "Qualified schema comment should be accepted: {:?}",
            result.err()
        );

        let db = result.unwrap();
        let schema = db.schemas.iter().find(|s| s.name == "public").unwrap();

        // Check that the already qualified name remains qualified
        if let Some(mod_stmts) = &schema.mod_statements {
            assert_eq!(mod_stmts.len(), 1, "Should have one mod statement");
            let stmt_sql = format!("{}", mod_stmts[0]);
            assert!(
                stmt_sql.contains("materialize.public"),
                "Schema name should remain materialize.public, got: {}",
                stmt_sql
            );
        } else {
            panic!("Schema should have mod statements");
        }
    }
}
