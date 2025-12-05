//! Validation functions for typed representation.
//!
//! This module contains helper functions used during the conversion from
//! raw to typed representation to validate various constraints.

use super::super::ast::DatabaseIdent;
use super::super::ast::Statement;
use super::types::{DatabaseObject, FullyQualifiedName};
use crate::project::error::{ValidationError, ValidationErrorKind};
use mz_sql_parser::ast::*;
use std::path::PathBuf;

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
/// materialize/public/users.sql  ->  CREATE TABLE users (...)
/// materialize/public/users.sql  ->  CREATE TABLE public.users (...)
/// materialize/public/users.sql  ->  CREATE TABLE materialize.public.users (...)
/// ```
///
/// Invalid mappings:
/// ```text
/// materialize/public/users.sql  ->  CREATE TABLE customers (...)  X name mismatch
/// materialize/public/users.sql  ->  CREATE TABLE private.users (...)  X schema mismatch
/// materialize/public/users.sql  ->  CREATE TABLE other.public.users (...)  X database mismatch
/// ```
pub(super) fn validate_ident(
    stmt: &Statement,
    fqn: &FullyQualifiedName,
    errors: &mut Vec<ValidationError>,
) {
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
/// COMMENT ON TABLE users IS 'user data';
/// ```
///
/// Invalid:
/// ```sql
/// CREATE TABLE users (...);
/// COMMENT ON VIEW users IS 'user data';  -- type mismatch
/// COMMENT ON TABLE customers IS 'data';  -- wrong object
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
pub(super) fn validate_comment_references(
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
/// CREATE INDEX idx ON table (col);  -- missing cluster
/// ```
pub(super) fn validate_index_clusters(
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
/// CREATE MATERIALIZED VIEW mv AS SELECT ...;  -- missing cluster
/// ```
pub(super) fn validate_mv_cluster(
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

/// Validates that a sink specifies a cluster.
///
/// Sinks in Materialize must specify which cluster they run on using the IN CLUSTER clause.
/// This ensures deterministic deployment and avoids implicit cluster selection.
///
/// # Example
///
/// Valid:
/// ```sql
/// CREATE SINK sink IN CLUSTER quickstart FROM table INTO ...;
/// ```
///
/// Invalid:
/// ```sql
/// CREATE SINK sink FROM table INTO ...;  -- missing cluster
/// ```
pub(super) fn validate_sink_cluster(
    fqn: &FullyQualifiedName,
    stmt: &Statement,
    errors: &mut Vec<ValidationError>,
) {
    if let Statement::CreateSink(sink) = stmt
        && sink.in_cluster.is_none()
    {
        let sink_sql = format!("{};", sink);
        let sink_name = sink
            .name
            .as_ref()
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<unnamed>".to_string());

        errors.push(ValidationError::with_file_and_sql(
            ValidationErrorKind::SinkMissingCluster { sink_name },
            fqn.path.clone(),
            sink_sql,
        ));
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
/// CREATE INDEX users_id_idx ON users (id);
/// ```
///
/// Invalid:
/// ```sql
/// CREATE TABLE users (id INT, name TEXT);
/// CREATE INDEX orders_id_idx ON orders (id);  -- wrong object
/// ```
pub(super) fn validate_index_references(
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
pub(super) fn validate_grant_references(
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

/// Validate database mod file statements.
///
/// Database mod files can only contain:
/// - COMMENT ON DATABASE (targeting the database itself)
/// - GRANT ON DATABASE (targeting the database itself)
/// - ALTER DEFAULT PRIVILEGES
pub(super) fn validate_database_mod_statements(
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
pub(super) fn validate_schema_mod_statements(
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

/// Validates that a schema doesn't mix storage objects with computation objects.
///
/// This validation prevents accidentally recreating tables or sinks when recreating views,
/// which would cause data loss. Storage and computation objects should be in separate schemas.
///
/// # Object Groups
///
/// - **Storage objects**: Tables, Sinks (can coexist in same schema)
/// - **Computation objects**: Views, Materialized Views (can coexist in same schema)
/// - These two groups CANNOT mix in the same schema
///
/// # Validation Rules
///
/// Valid combinations within a schema:
/// - Tables only
/// - Tables + Sinks
/// - Sinks only
/// - Views only
/// - Views + Materialized Views
/// - Materialized Views only
///
/// Invalid combinations:
/// - Tables + Views
/// - Tables + Materialized Views
/// - Sinks + Views
/// - Sinks + Materialized Views
/// - Tables + Sinks + Views
/// - (any mix of storage and computation)
///
/// # Arguments
///
/// * `schema_name` - The name of the schema being validated
/// * `objects` - All database objects in the schema
/// * `errors` - Vector to collect validation errors
pub(super) fn validate_no_storage_and_computation_in_schema(
    schema_name: &str,
    objects: &[DatabaseObject],
    errors: &mut Vec<ValidationError>,
) {
    let mut has_storage = false;
    let mut has_computation = false;
    let mut storage_names = Vec::new();
    let mut computation_names = Vec::new();

    for obj in objects {
        match &obj.stmt {
            // Storage objects (persist data)
            Statement::CreateTable(_)
            | Statement::CreateTableFromSource(_)
            | Statement::CreateSink(_) => {
                has_storage = true;
                let ident = obj.stmt.ident();
                storage_names.push(ident.object.clone());
            }
            // Computation objects (transform data)
            Statement::CreateView(_) | Statement::CreateMaterializedView(_) => {
                has_computation = true;
                let ident = obj.stmt.ident();
                computation_names.push(ident.object.clone());
            }
        }
    }

    if has_storage && has_computation {
        errors.push(ValidationError::with_file(
            ValidationErrorKind::StorageAndComputationObjectsInSameSchema {
                schema_name: schema_name.to_string(),
                storage_objects: storage_names,
                computation_objects: computation_names,
            },
            PathBuf::from(schema_name),
        ));
    }
}
