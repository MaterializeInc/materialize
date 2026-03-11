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
    validate_schema_mod_statements, validate_sink_cluster, validate_source_cluster,
};
use crate::project::SchemaQualifier;
use crate::project::error::{ValidationError, ValidationErrorKind, ValidationErrors};
use mz_sql_parser::ast::*;
use std::collections::BTreeSet;
use std::path::PathBuf;

/// Classify statements in a single variant and determine its object type.
/// Returns `(ObjectType, path)` on success, or errors.
fn classify_variant_object_type(
    name: &str,
    path: &std::path::Path,
    statements: &[mz_sql_parser::ast::Statement<Raw>],
) -> Result<ObjectType, Vec<ValidationError>> {
    let mut errors = Vec::new();
    let mut object_type: Option<ObjectType> = None;
    let mut main_count = 0usize;

    for stmt in statements {
        let stmt_type = match stmt {
            mz_sql_parser::ast::Statement::CreateSink(_) => Some(ObjectType::Sink),
            mz_sql_parser::ast::Statement::CreateView(_) => Some(ObjectType::View),
            mz_sql_parser::ast::Statement::CreateMaterializedView(_) => {
                Some(ObjectType::MaterializedView)
            }
            mz_sql_parser::ast::Statement::CreateTable(_) => Some(ObjectType::Table),
            mz_sql_parser::ast::Statement::CreateTableFromSource(_) => Some(ObjectType::Table),
            mz_sql_parser::ast::Statement::CreateSource(_) => Some(ObjectType::Source),
            mz_sql_parser::ast::Statement::CreateSecret(_) => Some(ObjectType::Secret),
            mz_sql_parser::ast::Statement::CreateConnection(_) => Some(ObjectType::Connection),
            mz_sql_parser::ast::Statement::CreateIndex(_)
            | mz_sql_parser::ast::Statement::GrantPrivileges(_)
            | mz_sql_parser::ast::Statement::Comment(_)
            | mz_sql_parser::ast::Statement::ExecuteUnitTest(_) => None,
            other => {
                errors.push(ValidationError::with_file(
                    ValidationErrorKind::UnsupportedStatement {
                        object_name: name.to_string(),
                        statement_type: format!("{:?}", other),
                    },
                    path.to_path_buf(),
                ));
                None
            }
        };

        if let Some(t) = stmt_type {
            main_count += 1;
            if main_count > 1 {
                errors.push(ValidationError::with_file(
                    ValidationErrorKind::MultipleMainStatements {
                        object_name: name.to_string(),
                    },
                    path.to_path_buf(),
                ));
            } else {
                object_type = Some(t);
            }
        }
    }

    if object_type.is_none() {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::NoMainStatement {
                object_name: name.to_string(),
            },
            path.to_path_buf(),
        ));
        errors.push(ValidationError::with_file(
            ValidationErrorKind::NoObjectType,
            path.to_path_buf(),
        ));
    }

    if !errors.is_empty() {
        Err(errors)
    } else {
        Ok(object_type.unwrap())
    }
}

/// A human-readable name for an ObjectType.
fn object_type_name(t: ObjectType) -> &'static str {
    match t {
        ObjectType::View => "view",
        ObjectType::MaterializedView => "materialized view",
        ObjectType::Table => "table",
        ObjectType::Source => "source",
        ObjectType::Sink => "sink",
        ObjectType::Secret => "secret",
        ObjectType::Connection => "connection",
        _ => "unknown",
    }
}

/// Validate a single variant's statements fully and produce a typed `DatabaseObject`.
///
/// This runs all existing validation (classify statements, check name/fqn, indexes,
/// grants, comments, clusters, etc.)
fn validate_single_variant(
    name: &str,
    database: &str,
    schema: &str,
    path: &std::path::Path,
    statements: Vec<mz_sql_parser::ast::Statement<Raw>>,
) -> Result<DatabaseObject, ValidationErrors> {
    let mut errors = Vec::new();
    let mut main_stmt: Option<Statement> = None;
    let mut object_type: Option<ObjectType> = None;
    let mut indexes = Vec::new();
    let mut grants = Vec::new();
    let mut comments = Vec::new();
    let mut tests = Vec::new();

    /// Try to set the main statement, recording an error if one is already set.
    fn set_main_statement(
        main_stmt: &mut Option<Statement>,
        object_type: &mut Option<ObjectType>,
        new_stmt: Statement,
        new_type: ObjectType,
        name: &str,
        path: &std::path::Path,
        errors: &mut Vec<ValidationError>,
    ) {
        if main_stmt.is_some() {
            errors.push(ValidationError::with_file(
                ValidationErrorKind::MultipleMainStatements {
                    object_name: name.to_string(),
                },
                path.to_path_buf(),
            ));
        } else {
            *main_stmt = Some(new_stmt);
            *object_type = Some(new_type);
        }
    }

    for stmt in statements {
        match stmt {
            mz_sql_parser::ast::Statement::CreateSink(s) => {
                set_main_statement(
                    &mut main_stmt,
                    &mut object_type,
                    Statement::CreateSink(s),
                    ObjectType::Sink,
                    name,
                    path,
                    &mut errors,
                );
            }
            mz_sql_parser::ast::Statement::CreateView(s) => {
                set_main_statement(
                    &mut main_stmt,
                    &mut object_type,
                    Statement::CreateView(s),
                    ObjectType::View,
                    name,
                    path,
                    &mut errors,
                );
            }
            mz_sql_parser::ast::Statement::CreateMaterializedView(s) => {
                set_main_statement(
                    &mut main_stmt,
                    &mut object_type,
                    Statement::CreateMaterializedView(s),
                    ObjectType::MaterializedView,
                    name,
                    path,
                    &mut errors,
                );
            }
            mz_sql_parser::ast::Statement::CreateTable(s) => {
                set_main_statement(
                    &mut main_stmt,
                    &mut object_type,
                    Statement::CreateTable(s),
                    ObjectType::Table,
                    name,
                    path,
                    &mut errors,
                );
            }
            mz_sql_parser::ast::Statement::CreateTableFromSource(s) => {
                set_main_statement(
                    &mut main_stmt,
                    &mut object_type,
                    Statement::CreateTableFromSource(s),
                    ObjectType::Table,
                    name,
                    path,
                    &mut errors,
                );
            }
            mz_sql_parser::ast::Statement::CreateSource(s) => {
                set_main_statement(
                    &mut main_stmt,
                    &mut object_type,
                    Statement::CreateSource(s),
                    ObjectType::Source,
                    name,
                    path,
                    &mut errors,
                );
            }
            mz_sql_parser::ast::Statement::CreateSecret(s) => {
                set_main_statement(
                    &mut main_stmt,
                    &mut object_type,
                    Statement::CreateSecret(s),
                    ObjectType::Secret,
                    name,
                    path,
                    &mut errors,
                );
            }
            mz_sql_parser::ast::Statement::CreateConnection(s) => {
                set_main_statement(
                    &mut main_stmt,
                    &mut object_type,
                    Statement::CreateConnection(s),
                    ObjectType::Connection,
                    name,
                    path,
                    &mut errors,
                );
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
                        object_name: name.to_string(),
                        statement_type: format!("{:?}", other),
                    },
                    path.to_path_buf(),
                ));
            }
        }
    }

    // Check for main statement
    if main_stmt.is_none() {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::NoMainStatement {
                object_name: name.to_string(),
            },
            path.to_path_buf(),
        ));
    }

    // Check for object type
    if object_type.is_none() {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::NoObjectType,
            path.to_path_buf(),
        ));
    }

    // If we have fatal errors (no main statement or object type), return early
    if !errors.is_empty() && (main_stmt.is_none() || object_type.is_none()) {
        return Err(ValidationErrors::new(errors));
    }

    // Unwrap is safe here because we checked above
    let stmt = main_stmt.unwrap();
    let obj_type = object_type.unwrap();

    let fqn = match FullyQualifiedName::with_names(path, name, database, schema) {
        Ok(fqn) => fqn,
        Err(e) => {
            errors.push(e);
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
    validate_source_cluster(&fqn, &stmt, &mut errors);

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

impl DatabaseObject {
    /// Validate all variants of a raw database object, check cross-variant consistency,
    /// and resolve the active variant for the given profile.
    pub fn validate(
        value: super::super::raw::DatabaseObject,
        profile: &str,
    ) -> Result<Self, ValidationErrors> {
        let mut errors = Vec::new();

        // Step 1: Classify all variants to determine their object types
        let mut variant_types: Vec<(ObjectType, &super::super::raw::ObjectVariant)> = Vec::new();
        for variant in &value.variants {
            match classify_variant_object_type(&value.name, &variant.path, &variant.statements) {
                Ok(obj_type) => variant_types.push((obj_type, variant)),
                Err(errs) => errors.extend(errs),
            }
        }

        // If we couldn't classify any variant, return errors early
        if variant_types.is_empty() && !errors.is_empty() {
            return Err(ValidationErrors::new(errors));
        }

        // Step 2: Check type consistency across variants
        // Find the default variant's type as the reference type
        let reference_type = variant_types
            .iter()
            .find(|(_, v)| v.profile.is_none())
            .or_else(|| variant_types.first())
            .map(|(t, _)| *t);

        if let Some(ref_type) = reference_type {
            let ref_variant = variant_types
                .iter()
                .find(|(_, v)| v.profile.is_none())
                .or_else(|| variant_types.first())
                .map(|(_, v)| *v)
                .unwrap();

            for (obj_type, variant) in &variant_types {
                if *obj_type != ref_type {
                    errors.push(ValidationError::with_file(
                        ValidationErrorKind::ProfileObjectTypeMismatch {
                            object_name: value.name.clone(),
                            default_type: object_type_name(ref_type).to_string(),
                            override_profile: variant
                                .profile
                                .clone()
                                .unwrap_or_else(|| "default".to_string()),
                            override_type: object_type_name(*obj_type).to_string(),
                            default_path: ref_variant.path.clone(),
                            override_path: variant.path.clone(),
                        },
                        variant.path.clone(),
                    ));
                }
            }

            // Step 3: Check view/MV restriction
            let has_overrides = value.variants.iter().any(|v| v.profile.is_some());
            if has_overrides
                && (ref_type == ObjectType::View || ref_type == ObjectType::MaterializedView)
            {
                for variant in &value.variants {
                    if let Some(ref prof) = variant.profile {
                        errors.push(ValidationError::with_file(
                            ValidationErrorKind::ProfileOverrideNotAllowed {
                                object_name: value.name.clone(),
                                object_type: object_type_name(ref_type).to_string(),
                                override_profile: prof.clone(),
                                override_path: variant.path.clone(),
                            },
                            variant.path.clone(),
                        ));
                    }
                }
            }
        }

        if !errors.is_empty() {
            return Err(ValidationErrors::new(errors));
        }

        // Step 4: Resolve active variant — pick profile match or fall back to default
        let active_variant = value
            .variants
            .iter()
            .find(|v| v.profile.as_deref() == Some(profile))
            .or_else(|| value.variants.iter().find(|v| v.profile.is_none()))
            .or_else(|| value.variants.first());

        let active_variant = match active_variant {
            Some(v) => v,
            None => {
                // Should not happen since we require at least one variant
                errors.push(ValidationError::with_file(
                    ValidationErrorKind::NoMainStatement {
                        object_name: value.name.clone(),
                    },
                    PathBuf::from(&value.name),
                ));
                return Err(ValidationErrors::new(errors));
            }
        };

        // Step 5: Fully validate the active variant
        validate_single_variant(
            &value.name,
            &value.database,
            &value.schema,
            &active_variant.path,
            active_variant.statements.clone(),
        )
    }
}

impl Schema {
    /// Validate a raw schema, converting each object using the given profile.
    pub fn validate(
        value: super::super::raw::Schema,
        profile: &str,
    ) -> Result<Self, ValidationErrors> {
        let mut all_errors = Vec::new();
        let mut objects = Vec::new();

        for obj in value.objects {
            match DatabaseObject::validate(obj, profile) {
                Ok(db_obj) => objects.push(db_obj),
                Err(errs) => {
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

impl Database {
    /// Validate a raw database, converting each schema using the given profile.
    pub fn validate(
        value: super::super::raw::Database,
        profile: &str,
    ) -> Result<Self, ValidationErrors> {
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

            match Schema::validate(schema, profile) {
                Ok(s) => schemas.push(s),
                Err(errs) => {
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
        let profile = value.profile.clone();
        let mut all_errors = Vec::new();
        let mut databases = Vec::new();

        for (_, database) in value.databases {
            match Database::validate(database, &profile) {
                Ok(db) => databases.push(db),
                Err(errs) => {
                    all_errors.extend(errs.errors);
                }
            }
        }

        // Derive replacement schemas from SET api = stable statements
        let replacement_schemas = derive_replacement_schemas(&databases);

        // Validate replacement schemas only contain MVs
        validate_replacement_schemas(&replacement_schemas, &databases, &mut all_errors);

        if !all_errors.is_empty() {
            return Err(ValidationErrors::new(all_errors));
        }

        Ok(Self {
            databases,
            replacement_schemas,
        })
    }
}

/// Scan all schemas for `SET api = stable` statements and build
/// the set of replacement schemas.
fn derive_replacement_schemas(databases: &[Database]) -> BTreeSet<SchemaQualifier> {
    let mut replacement_schemas = BTreeSet::new();
    for db in databases {
        for schema in &db.schemas {
            if let Some(mod_stmts) = &schema.mod_statements {
                for stmt in mod_stmts {
                    if matches!(stmt, mz_sql_parser::ast::Statement::SetVariable(s) if s.variable.as_str().eq_ignore_ascii_case("api"))
                    {
                        replacement_schemas
                            .insert(SchemaQualifier::new(db.name.clone(), schema.name.clone()));
                        break;
                    }
                }
            }
        }
    }
    replacement_schemas
}

/// Validate replacement schemas derived from `SET api = stable` statements.
///
/// Ensures replacement schemas only contain materialized views.
fn validate_replacement_schemas(
    replacement_schemas: &BTreeSet<SchemaQualifier>,
    databases: &[Database],
    errors: &mut Vec<ValidationError>,
) {
    if replacement_schemas.is_empty() {
        return;
    }

    // Check replacement schemas only contain MVs
    for db in databases {
        for schema in &db.schemas {
            if !replacement_schemas
                .iter()
                .any(|sq| sq.database == db.name && sq.schema == schema.name)
            {
                continue;
            }

            for obj in &schema.objects {
                if !matches!(obj.stmt, Statement::CreateMaterializedView(_)) {
                    let object_type = match &obj.stmt {
                        Statement::CreateView(_) => "view",
                        Statement::CreateTable(_) => "table",
                        Statement::CreateTableFromSource(_) => "table from source",
                        Statement::CreateSink(_) => "sink",
                        Statement::CreateSource(_) => "source",
                        Statement::CreateSecret(_) => "secret",
                        Statement::CreateConnection(_) => "connection",
                        Statement::CreateMaterializedView(_) => unreachable!(),
                    };
                    errors.push(ValidationError::with_file(
                        ValidationErrorKind::ReplacementSchemaNonMvObject {
                            database: db.name.clone(),
                            schema: schema.name.clone(),
                            object_name: obj.stmt.ident().object.clone(),
                            object_type: object_type.to_string(),
                        },
                        PathBuf::from(format!("{}/{}.sql", db.name, schema.name)),
                    ));
                }
            }
        }
    }
}
