// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Object validation and compiled-project assembly.
//!
//! This module turns source-owned objects into validated compiled objects and
//! assembles the full compiled project from those validated results.
//!
//! Validation is defined per logical object:
//!
//! - classify statements and require exactly one primary create statement
//! - validate names against the source-owned object key
//! - normalize identifiers and dependencies into canonical qualified form
//! - validate clusters, references, comments, and grants
//! - resolve active profile variants and reject incompatible overrides
//!
//! Project assembly validates database and schema setup statements on every
//! invocation, groups validated objects by `(database, schema)`, and enforces
//! schema-wide invariants before producing a compiled project.
//!
//! ## Per-Object Validation (in `validate_single_variant`)
//!
//! 1. **Statement classification** — exactly one main CREATE statement per file
//! 2. **Name validation** — object name matches file stem, FQN matches path
//! 3. **Identifier format** — lowercase, valid characters
//! 4. **Name normalization** — fully qualify all references via `NormalizingVisitor`
//! 5. **Cluster validation** — MVs, sinks, sources, indexes have required `IN CLUSTER`
//! 6. **Reference validation** — indexes, grants, comments reference the parent object
//!
//! ## Profile Variant Handling
//!
//! Objects may have multiple file variants (e.g., `conn.sql` and
//! `conn#staging.sql`). All variants are classified for type consistency,
//! then only the active variant (matching profile or default) is fully
//! validated. Views and materialized views do not allow profile overrides.

mod clusters;
mod identifiers;
mod references;
mod schema_constraints;

use clusters::{
    validate_index_clusters, validate_indexes_supported, validate_mv_cluster,
    validate_sink_cluster, validate_source_cluster,
};
use identifiers::{validate_fqn_identifiers, validate_ident};
use references::{
    validate_comment_references, validate_grant_references, validate_index_references,
};
use schema_constraints::validate_no_storage_and_computation_in_schema;

use super::super::ast::{DatabaseIdent, Statement};
use crate::project::SchemaQualifier;
use crate::project::error::{ValidationError, ValidationErrorKind, ValidationErrors};
use crate::project::ir::compiled::{Database, DatabaseObject, FullyQualifiedName, Project, Schema};
use crate::project::resolve::normalize::NormalizingVisitor;
use crate::project::syntax::input;
use crate::project::syntax::parser::{LocatedStatement, statement_type_name};
use mz_sql_parser::ast::*;
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

/// Classify statements in a single variant and determine its object type.
/// Returns `(ObjectType, path)` on success, or errors.
fn classify_variant_object_type(
    name: &str,
    path: &std::path::Path,
    statements: &[&mz_sql_parser::ast::Statement<Raw>],
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
                        statement_type: statement_type_name(other).to_string(),
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

/// Validate a single variant's statements fully and produce a compiled object.
///
/// This runs all existing validation (classify statements, check name/fqn, indexes,
/// grants, comments, clusters, etc.). Each statement carries its byte offset from
/// the source file so that validation errors can point to the exact location.
fn validate_single_variant(
    name: &str,
    database: &str,
    schema: &str,
    path: &std::path::Path,
    located_statements: Vec<LocatedStatement>,
) -> Result<DatabaseObject, ValidationErrors> {
    let mut errors = Vec::new();
    let mut main_stmt: Option<(Statement, usize)> = None;
    let mut object_type: Option<ObjectType> = None;
    let mut indexes = Vec::new();
    let mut index_offsets = Vec::new();
    let mut grants = Vec::new();
    let mut grant_offsets = Vec::new();
    let mut comments = Vec::new();
    let mut comment_offsets = Vec::new();
    let mut tests = Vec::new();

    for LocatedStatement {
        ast: stmt,
        byte_offset,
    } in located_statements
    {
        match stmt {
            // Main CREATE statements
            mz_sql_parser::ast::Statement::CreateSink(s) => {
                if main_stmt.is_some() {
                    errors.push(ValidationError::with_file_and_offset(
                        ValidationErrorKind::MultipleMainStatements {
                            object_name: name.to_string(),
                        },
                        path.to_path_buf(),
                        byte_offset,
                    ));
                } else {
                    main_stmt = Some((Statement::CreateSink(s), byte_offset));
                    object_type = Some(ObjectType::Sink);
                }
            }
            mz_sql_parser::ast::Statement::CreateView(s) => {
                if main_stmt.is_some() {
                    errors.push(ValidationError::with_file_and_offset(
                        ValidationErrorKind::MultipleMainStatements {
                            object_name: name.to_string(),
                        },
                        path.to_path_buf(),
                        byte_offset,
                    ));
                } else {
                    main_stmt = Some((Statement::CreateView(s), byte_offset));
                    object_type = Some(ObjectType::View);
                }
            }
            mz_sql_parser::ast::Statement::CreateMaterializedView(s) => {
                if main_stmt.is_some() {
                    errors.push(ValidationError::with_file_and_offset(
                        ValidationErrorKind::MultipleMainStatements {
                            object_name: name.to_string(),
                        },
                        path.to_path_buf(),
                        byte_offset,
                    ));
                } else {
                    main_stmt = Some((Statement::CreateMaterializedView(s), byte_offset));
                    object_type = Some(ObjectType::MaterializedView);
                }
            }
            mz_sql_parser::ast::Statement::CreateTable(s) => {
                if main_stmt.is_some() {
                    errors.push(ValidationError::with_file_and_offset(
                        ValidationErrorKind::MultipleMainStatements {
                            object_name: name.to_string(),
                        },
                        path.to_path_buf(),
                        byte_offset,
                    ));
                } else {
                    main_stmt = Some((Statement::CreateTable(s), byte_offset));
                    object_type = Some(ObjectType::Table);
                }
            }
            mz_sql_parser::ast::Statement::CreateTableFromSource(s) => {
                if main_stmt.is_some() {
                    errors.push(ValidationError::with_file_and_offset(
                        ValidationErrorKind::MultipleMainStatements {
                            object_name: name.to_string(),
                        },
                        path.to_path_buf(),
                        byte_offset,
                    ));
                } else {
                    main_stmt = Some((Statement::CreateTableFromSource(s), byte_offset));
                    object_type = Some(ObjectType::Table);
                }
            }
            mz_sql_parser::ast::Statement::CreateSource(s) => {
                if main_stmt.is_some() {
                    errors.push(ValidationError::with_file_and_offset(
                        ValidationErrorKind::MultipleMainStatements {
                            object_name: name.to_string(),
                        },
                        path.to_path_buf(),
                        byte_offset,
                    ));
                } else {
                    main_stmt = Some((Statement::CreateSource(s), byte_offset));
                    object_type = Some(ObjectType::Source);
                }
            }
            mz_sql_parser::ast::Statement::CreateSecret(s) => {
                if main_stmt.is_some() {
                    errors.push(ValidationError::with_file_and_offset(
                        ValidationErrorKind::MultipleMainStatements {
                            object_name: name.to_string(),
                        },
                        path.to_path_buf(),
                        byte_offset,
                    ));
                } else {
                    main_stmt = Some((Statement::CreateSecret(s), byte_offset));
                    object_type = Some(ObjectType::Secret);
                }
            }
            mz_sql_parser::ast::Statement::CreateConnection(s) => {
                if main_stmt.is_some() {
                    errors.push(ValidationError::with_file_and_offset(
                        ValidationErrorKind::MultipleMainStatements {
                            object_name: name.to_string(),
                        },
                        path.to_path_buf(),
                        byte_offset,
                    ));
                } else {
                    main_stmt = Some((Statement::CreateConnection(s), byte_offset));
                    object_type = Some(ObjectType::Connection);
                }
            }

            // Supporting statements — track byte offsets in parallel vectors
            mz_sql_parser::ast::Statement::CreateIndex(s) => {
                index_offsets.push(byte_offset);
                indexes.push(s);
            }
            mz_sql_parser::ast::Statement::GrantPrivileges(s) => {
                grant_offsets.push(byte_offset);
                grants.push(s);
            }
            mz_sql_parser::ast::Statement::Comment(s) => {
                comment_offsets.push(byte_offset);
                comments.push(s);
            }

            // Test statements are collected for later execution
            mz_sql_parser::ast::Statement::ExecuteUnitTest(s) => {
                tests.push(s);
            }

            // Unsupported statements
            other => {
                errors.push(ValidationError::with_file_and_offset(
                    ValidationErrorKind::UnsupportedStatement {
                        object_name: name.to_string(),
                        statement_type: statement_type_name(&other).to_string(),
                    },
                    path.to_path_buf(),
                    byte_offset,
                ));
            }
        }
    }

    // Check for main statement (file-level — no single statement to point at)
    if main_stmt.is_none() {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::NoMainStatement {
                object_name: name.to_string(),
            },
            path.to_path_buf(),
        ));
    }

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
    let (stmt, main_offset) = main_stmt.unwrap();
    let obj_type = object_type.unwrap();

    let fqn = match FullyQualifiedName::with_names(path, name, database, schema) {
        Ok(fqn) => fqn,
        Err(e) => {
            errors.push(e);
            return Err(ValidationErrors::new(errors));
        }
    };

    // Fully qualify the object's own identifier from the file's FQN. Supporting
    // statements are normalized to fully qualified names below, so an
    // unqualified main name (the idiomatic style) must still carry its database
    // and schema for reference matching to detect a wrong-schema target.
    let main_ident = DatabaseIdent {
        database: Some(Ident::new_unchecked(fqn.database())),
        schema: Some(Ident::new_unchecked(fqn.schema())),
        object: Ident::new_unchecked(fqn.object()),
    };

    // Validate the original statement identifier against FQN
    validate_ident(&stmt, &fqn, main_offset, &mut errors);

    // Validate identifier format (lowercase, valid characters)
    validate_fqn_identifiers(&fqn, main_offset, &mut errors);

    // Normalize statement name and dependencies
    let stmt = stmt.normalize_stmt(&fqn);

    // Normalize index, grant, and comment references to be fully qualified
    let visitor = NormalizingVisitor::fully_qualifying(&fqn);
    visitor.normalize_index_references(&mut indexes);
    visitor.normalize_grant_references(&mut grants);
    visitor.normalize_comment_references(&mut comments);

    // Validate cluster requirements
    validate_index_clusters(&fqn, &indexes, &index_offsets, &mut errors);
    validate_indexes_supported(&fqn, &stmt, &indexes, &index_offsets, &mut errors);
    validate_mv_cluster(&fqn, &stmt, main_offset, &mut errors);
    validate_sink_cluster(&fqn, &stmt, main_offset, &mut errors);
    validate_source_cluster(&fqn, &stmt, main_offset, &mut errors);

    validate_index_references(&fqn, &indexes, &index_offsets, &main_ident, &mut errors);
    validate_grant_references(
        &fqn,
        &grants,
        &grant_offsets,
        &main_ident,
        obj_type,
        &mut errors,
    );
    validate_comment_references(
        &fqn,
        &comments,
        &comment_offsets,
        &main_ident,
        &obj_type,
        &mut errors,
    );

    if !errors.is_empty() {
        return Err(ValidationErrors::new(errors));
    }

    Ok(DatabaseObject {
        path: path.to_path_buf(),
        stmt,
        indexes,
        grants,
        comments,
        tests,
    })
}

impl DatabaseObject {
    /// Validate all variants of a source-owned database object, check cross-variant consistency,
    /// and resolve the active variant for the given profile.
    pub fn validate(
        value: input::DatabaseObject,
        profile: &str,
    ) -> Result<Option<Self>, ValidationErrors> {
        let mut errors = Vec::new();

        // Step 1: Classify all variants to determine their object types
        let mut variant_types: Vec<(ObjectType, &input::ObjectVariant)> = Vec::new();
        for variant in &value.variants {
            let stmts: Vec<_> = variant.statements.iter().map(|ls| &ls.ast).collect();
            match classify_variant_object_type(&value.name, &variant.path, &stmts) {
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

        // Step 4: Resolve active variant — pick profile match or fall back to default.
        // If no matching profile variant and no default variant exist, skip this object
        // (it belongs to a different profile).
        let active_variant = value
            .variants
            .iter()
            .find(|v| v.profile.as_deref() == Some(profile))
            .or_else(|| value.variants.iter().find(|v| v.profile.is_none()));

        let active_variant = match active_variant {
            Some(v) => v,
            None => {
                // No variant matches the active profile and no default exists.
                // This object is defined only for other profiles — skip it.
                return Ok(None);
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
        .map(Some)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct SchemaBuildMeta {
    pub name: String,
    pub mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
}

#[derive(Debug, Clone)]
pub(crate) struct DatabaseBuildMeta {
    pub name: String,
    pub mod_statements: Option<Vec<mz_sql_parser::ast::Statement<Raw>>>,
    pub schemas: Vec<SchemaBuildMeta>,
}

pub(crate) fn assemble_project(
    db_metas: Vec<DatabaseBuildMeta>,
    validated_objects: Vec<(String, String, DatabaseObject)>,
) -> Result<Project, ValidationErrors> {
    let mut all_errors = Vec::new();

    let mut objects_by_location: BTreeMap<(String, String), Vec<DatabaseObject>> = BTreeMap::new();
    for (db_name, schema_name, object) in validated_objects {
        objects_by_location
            .entry((db_name, schema_name))
            .or_default()
            .push(object);
    }

    let mut databases = Vec::new();

    for meta in db_metas {
        let mut schemas = Vec::new();

        for schema_meta in &meta.schemas {
            let objects = objects_by_location
                .remove(&(meta.name.clone(), schema_meta.name.clone()))
                .unwrap_or_default();

            validate_no_storage_and_computation_in_schema(
                &schema_meta.name,
                &objects,
                &mut all_errors,
            );

            schemas.push(Schema {
                name: schema_meta.name.clone(),
                objects,
                mod_statements: schema_meta.mod_statements.clone(),
            });
        }

        databases.push(Database {
            name: meta.name,
            schemas,
            mod_statements: meta.mod_statements,
        });
    }

    let replacement_schemas = derive_replacement_schemas(&databases);
    validate_replacement_schemas(&replacement_schemas, &databases, &mut all_errors);

    if !all_errors.is_empty() {
        return Err(ValidationErrors::new(all_errors));
    }

    Ok(Project {
        databases,
        replacement_schemas,
    })
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
                    errors.push(ValidationError::with_file(
                        ValidationErrorKind::ReplacementSchemaNonMvObject {
                            database: db.name.clone(),
                            schema: schema.name.clone(),
                            object_name: obj.stmt.ident().object.to_string(),
                            object_type: obj.stmt.kind(),
                        },
                        PathBuf::from(format!("{}/{}.sql", db.name, schema.name)),
                    ));
                }
            }
        }
    }
}
