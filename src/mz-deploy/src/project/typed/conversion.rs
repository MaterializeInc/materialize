//! Conversion implementations for typed representation.
//!
//! This module contains the TryFrom implementations that convert raw
//! parsed representations into validated typed representations.

use super::super::ast::Statement;
use super::super::normalize::NormalizingVisitor;
use super::types::{Database, DatabaseObject, FullyQualifiedName, Project, Schema};
use super::validation::{
    validate_comment_references, validate_database_mod_statements, validate_fqn_identifiers,
    validate_grant_references, validate_ident, validate_index_clusters, validate_index_references,
    validate_mv_cluster, validate_no_storage_and_computation_in_schema,
    validate_schema_mod_statements, validate_sink_cluster,
};
use crate::project::error::{ValidationError, ValidationErrorKind, ValidationErrors};
use mz_sql_parser::ast::*;
use std::path::PathBuf;

impl TryFrom<super::super::raw::DatabaseObject> for DatabaseObject {
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
    fn try_from(value: super::super::raw::DatabaseObject) -> Result<Self, Self::Error> {
        let mut errors = Vec::new();
        let mut main_stmt: Option<Statement> = None;
        let mut object_type: Option<ObjectType> = None;
        let mut indexes = Vec::new();
        let mut grants = Vec::new();
        let mut comments = Vec::new();
        let mut tests = Vec::new();

        for stmt in value.statements {
            match stmt {
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

        // Validate identifier format (lowercase, valid characters)
        validate_fqn_identifiers(&fqn, &mut errors);

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
        validate_sink_cluster(&fqn, &stmt, &mut errors);

        validate_index_references(&fqn, &indexes, &main_ident, &mut errors);
        validate_grant_references(&fqn, &grants, &main_ident, obj_type, &mut errors);
        validate_comment_references(&fqn, &comments, &main_ident, &obj_type, &mut errors);

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

impl TryFrom<super::super::raw::Schema> for Schema {
    type Error = ValidationErrors;

    /// Converts a raw schema into a validated HIR schema.
    ///
    /// Validates each database object in the schema. Collects all validation errors
    /// from all objects and returns them together.
    fn try_from(value: super::super::raw::Schema) -> Result<Self, Self::Error> {
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

        validate_no_storage_and_computation_in_schema(&value.name, &objects, &mut all_errors);

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

impl TryFrom<super::super::raw::Database> for Database {
    type Error = ValidationErrors;

    /// Converts a raw database into a validated HIR database.
    ///
    /// Validates each schema in the database. Collects all validation errors
    /// from all schemas and objects and returns them together.
    fn try_from(value: super::super::raw::Database) -> Result<Self, Self::Error> {
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

impl TryFrom<super::super::raw::Project> for Project {
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
    fn try_from(value: super::super::raw::Project) -> Result<Self, Self::Error> {
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
