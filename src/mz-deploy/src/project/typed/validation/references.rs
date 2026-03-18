//! Reference validation for supporting statements.
//!
//! Validates that indexes, grants, and comments reference the main object
//! defined in the same file, ensuring each file is self-contained.

use super::super::super::ast::DatabaseIdent;
use super::super::types::FullyQualifiedName;
use crate::project::error::{ValidationError, ValidationErrorKind};
use mz_sql_parser::ast::*;

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
/// CREATE INDEX users_id_idx ON users (id);
/// ```
///
/// Invalid:
/// ```sql
/// CREATE TABLE users (id INT, name TEXT);
/// CREATE INDEX orders_id_idx ON orders (id);  -- wrong object
/// ```
pub(in super::super) fn validate_index_references(
    fqn: &FullyQualifiedName,
    indexes: &[CreateIndexStatement<Raw>],
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

/// Validates that all CREATE CONSTRAINT statements reference the main object.
///
/// Ensures that every constraint defined in the file is created on the object
/// defined in the same file.
pub(in super::super) fn validate_constraint_references(
    fqn: &FullyQualifiedName,
    constraints: &[CreateConstraintStatement<Raw>],
    main_ident: &DatabaseIdent,
    errors: &mut Vec<ValidationError>,
) {
    for constraint in constraints.iter() {
        let on: DatabaseIdent = constraint.on_name.name().clone().into();
        if !on.matches(main_ident) {
            let constraint_sql = format!("{};", constraint);
            errors.push(ValidationError::with_file_and_sql(
                ValidationErrorKind::ConstraintReferenceMismatch {
                    referenced: on.object,
                    expected: main_ident.object.clone(),
                },
                fqn.path.clone(),
                constraint_sql,
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
/// GRANT SELECT ON TABLE users TO analyst_role;
/// ```
///
/// Invalid:
/// ```sql
/// CREATE TABLE users (...);
/// GRANT SELECT ON orders TO analyst_role;  -- wrong object
/// ```
pub(in super::super) fn validate_grant_references(
    fqn: &FullyQualifiedName,
    grants: &[GrantPrivilegesStatement<Raw>],
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
/// GRANT SELECT ON TABLE users TO role;
///
/// -- For a materialized view
/// GRANT SELECT ON TABLE my_mv TO role;
/// GRANT SELECT ON MATERIALIZED VIEW my_mv TO role;  -- invalid
///
/// -- For a connection
/// GRANT USAGE ON CONNECTION kafka_conn TO role;
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

/// Validates that a COMMENT statement targets the correct object with the correct type.
///
/// Ensures that:
/// 1. The comment references the main object defined in the file
/// 2. The object type specified in the COMMENT matches the actual object type
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

/// Extract the target name and ObjectType from a CommentObjectType.
///
/// Returns Some((name, object_type)) for supported comment types, None for unsupported types.
/// Column comments are handled separately since they reference the parent table.
fn comment_object_to_target(obj: &CommentObjectType<Raw>) -> Option<(&RawItemName, ObjectType)> {
    match obj {
        CommentObjectType::Table { name } => Some((name, ObjectType::Table)),
        CommentObjectType::View { name } => Some((name, ObjectType::View)),
        CommentObjectType::MaterializedView { name } => Some((name, ObjectType::MaterializedView)),
        CommentObjectType::Source { name } => Some((name, ObjectType::Source)),
        CommentObjectType::Sink { name } => Some((name, ObjectType::Sink)),
        CommentObjectType::Connection { name } => Some((name, ObjectType::Connection)),
        CommentObjectType::Secret { name } => Some((name, ObjectType::Secret)),
        // Column, Index, Func, Type, Role, Database, Schema, Cluster, etc. are not supported
        // or handled separately
        _ => None,
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
pub(in super::super) fn validate_comment_references(
    fqn: &FullyQualifiedName,
    comments: &[CommentStatement<Raw>],
    main_ident: &DatabaseIdent,
    obj_type: &ObjectType,
    errors: &mut Vec<ValidationError>,
) {
    for comment in comments.iter() {
        let comment_sql = format!("{};", comment);

        // Handle column comments specially (they reference the parent table)
        if let CommentObjectType::Column { name } = &comment.object {
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
            continue;
        }

        // Handle supported object types
        if let Some((name, comment_obj_type)) = comment_object_to_target(&comment.object) {
            validate_comment_target(
                name,
                main_ident,
                obj_type,
                comment_obj_type,
                fqn,
                &comment_sql,
                errors,
            );
            continue;
        }

        // Unsupported comment type
        errors.push(ValidationError::with_file_and_sql(
            ValidationErrorKind::UnsupportedCommentType,
            fqn.path.clone(),
            comment_sql,
        ));
    }
}
