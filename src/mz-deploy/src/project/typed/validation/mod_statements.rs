//! Validation for database and schema mod file statements.
//!
//! Validates that mod files (database-level and schema-level) contain only
//! permitted statement types: comments, grants, and alter default privileges
//! targeting the correct database or schema.

use crate::project::error::{ValidationError, ValidationErrorKind};
use mz_sql_parser::ast::*;

/// Get a human-readable name for a CommentObjectType variant.
fn comment_object_type_name(obj: &CommentObjectType<Raw>) -> &'static str {
    match obj {
        CommentObjectType::Table { .. } => "TABLE",
        CommentObjectType::View { .. } => "VIEW",
        CommentObjectType::MaterializedView { .. } => "MATERIALIZED VIEW",
        CommentObjectType::Source { .. } => "SOURCE",
        CommentObjectType::Sink { .. } => "SINK",
        CommentObjectType::Connection { .. } => "CONNECTION",
        CommentObjectType::Secret { .. } => "SECRET",
        CommentObjectType::Schema { .. } => "SCHEMA",
        CommentObjectType::Database { .. } => "DATABASE",
        CommentObjectType::Column { .. } => "COLUMN",
        CommentObjectType::Index { .. } => "INDEX",
        CommentObjectType::Func { .. } => "FUNCTION",
        CommentObjectType::Type { .. } => "TYPE",
        CommentObjectType::Role { .. } => "ROLE",
        CommentObjectType::Cluster { .. } => "CLUSTER",
        CommentObjectType::ClusterReplica { .. } => "CLUSTER REPLICA",
        CommentObjectType::ContinualTask { .. } => "CONTINUAL TASK",
        CommentObjectType::NetworkPolicy { .. } => "NETWORK POLICY",
    }
}

/// Validate database mod file statements.
///
/// Database mod files can only contain:
/// - COMMENT ON DATABASE (targeting the database itself)
/// - GRANT ON DATABASE (targeting the database itself)
/// - ALTER DEFAULT PRIVILEGES
pub(in super::super) fn validate_database_mod_statements(
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
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::DatabaseModCommentTargetMismatch {
                                target: comment_object_type_name(&comment_stmt.object).to_string(),
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
pub(in super::super) fn validate_schema_mod_statements(
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
                        let is_match =
                            schema_name_matches(&target_schema, database_name, schema_name);

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
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::SchemaModCommentTargetMismatch {
                                target: comment_object_type_name(&comment_stmt.object).to_string(),
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
                                    let is_match = schema_name_matches(
                                        &target_schema,
                                        database_name,
                                        schema_name,
                                    );

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
            MzStatement::SetVariable(set_stmt) => {
                if set_stmt.variable.as_str().eq_ignore_ascii_case("api") {
                    // SET api = stable is a valid schema mod directive
                    let is_valid = match &set_stmt.to {
                        mz_sql_parser::ast::SetVariableTo::Values(values) => {
                            values.len() == 1
                                && match &values[0] {
                                    mz_sql_parser::ast::SetVariableValue::Ident(ident) => {
                                        ident.as_str().eq_ignore_ascii_case("stable")
                                    }
                                    mz_sql_parser::ast::SetVariableValue::Literal(
                                        mz_sql_parser::ast::Value::String(s),
                                    ) => s.eq_ignore_ascii_case("stable"),
                                    _ => false,
                                }
                        }
                        _ => false,
                    };

                    if !is_valid {
                        errors.push(ValidationError::with_file_and_sql(
                            ValidationErrorKind::InvalidSetVariable {
                                variable: set_stmt.variable.as_str().to_string(),
                                value: set_stmt.to.to_string(),
                            },
                            schema_path.to_path_buf(),
                            stmt_sql,
                        ));
                    }
                } else {
                    errors.push(ValidationError::with_file_and_sql(
                        ValidationErrorKind::InvalidSchemaModStatement {
                            statement_type: "SET".to_string(),
                            schema_name: format!("{}.{}", database_name, schema_name),
                        },
                        schema_path.to_path_buf(),
                        stmt_sql,
                    ));
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
                            let is_match =
                                schema_name_matches(&schema_str, database_name, schema_name);

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

/// Returns true if `target` matches `schema_name` either unqualified or as `database.schema`.
fn schema_name_matches(target: &str, database_name: &str, schema_name: &str) -> bool {
    target == schema_name || target == format!("{}.{}", database_name, schema_name)
}
