//! Validation errors for semantic validation of project definitions.
//!
//! This module defines errors that occur during semantic validation of SQL
//! statements, including object name mismatches, unsupported statement types,
//! and constraint violations. Errors carry rich contextual information
//! (file path, SQL statement) for user-friendly diagnostics.

use owo_colors::OwoColorize;
use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

/// Contextual information about where an error occurred.
///
/// This struct wraps error variants with additional context about the file
/// and SQL statement that caused the error.
#[derive(Debug, Clone)]
pub struct ErrorContext {
    /// The file where the error occurred
    pub file: PathBuf,
    /// The SQL statement that caused the error, if available
    pub sql_statement: Option<String>,
}

/// A validation error with contextual information.
///
/// This struct wraps a `ValidationErrorKind` with context about where
/// the error occurred (file path, SQL statement).
#[derive(Debug)]
pub struct ValidationError {
    /// The underlying error kind
    pub kind: ValidationErrorKind,
    /// Context about where the error occurred
    pub context: ErrorContext,
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Extract database/schema/file for path display
        let path_components: Vec<_> = self.context.file.components().collect();
        let len = path_components.len();

        let relative_path = if len >= 3 {
            format!(
                "{}/{}/{}",
                path_components[len - 3].as_os_str().to_string_lossy(),
                path_components[len - 2].as_os_str().to_string_lossy(),
                path_components[len - 1].as_os_str().to_string_lossy()
            )
        } else {
            self.context.file.display().to_string()
        };

        // Format like rustc: error: <message>
        writeln!(
            f,
            "{}: {}",
            "error".bright_red().bold(),
            self.kind.message()
        )?;

        // Show file location: --> path
        writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;

        // Add SQL statement if available
        if let Some(ref sql) = self.context.sql_statement {
            writeln!(f, "  {}", "|".bright_blue().bold())?;
            for line in sql.lines() {
                writeln!(f, "  {} {}", "|".bright_blue().bold(), line)?;
            }
            writeln!(f, "  {}", "|".bright_blue().bold())?;
        }

        // Add help text if available
        if let Some(help) = self.kind.help() {
            writeln!(
                f,
                "  {} {}",
                "=".bright_blue().bold(),
                format!("help: {}", help).bold()
            )?;
        }

        Ok(())
    }
}

impl std::error::Error for ValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl ValidationError {
    /// Create a new validation error with context
    pub fn with_context(kind: ValidationErrorKind, context: ErrorContext) -> Self {
        Self { kind, context }
    }

    /// Create a new validation error with just a file path
    pub fn with_file(kind: ValidationErrorKind, file: PathBuf) -> Self {
        Self {
            kind,
            context: ErrorContext {
                file,
                sql_statement: None,
            },
        }
    }

    /// Create a new validation error with file and SQL statement
    pub fn with_file_and_sql(kind: ValidationErrorKind, file: PathBuf, sql: String) -> Self {
        Self {
            kind,
            context: ErrorContext {
                file,
                sql_statement: Some(sql),
            },
        }
    }
}

/// The specific kind of validation error that occurred.
///
/// This enum contains the actual error variants without contextual information.
/// Context (file path, SQL statement) is stored in the wrapping `ValidationError`.
#[derive(Debug)]
pub enum ValidationErrorKind {
    /// A file contains multiple primary CREATE statements
    MultipleMainStatements { object_name: String },
    /// A file contains no primary CREATE statement
    NoMainStatement { object_name: String },
    /// Object name in statement doesn't match file name
    ObjectNameMismatch { declared: String, expected: String },
    /// Schema qualifier in statement doesn't match directory
    SchemaMismatch { declared: String, expected: String },
    /// Database qualifier in statement doesn't match directory
    DatabaseMismatch { declared: String, expected: String },
    /// An index references a different object
    IndexReferenceMismatch {
        referenced: String,
        expected: String,
    },
    /// A grant references a different object
    GrantReferenceMismatch {
        referenced: String,
        expected: String,
    },
    /// A comment references a different object
    CommentReferenceMismatch {
        referenced: String,
        expected: String,
    },
    /// A column comment references a different table
    ColumnCommentReferenceMismatch {
        referenced: String,
        expected: String,
    },
    /// Comment object type doesn't match actual object type
    CommentTypeMismatch {
        comment_type: String,
        object_type: String,
    },
    /// Grant object type doesn't match actual object type
    GrantTypeMismatch {
        grant_type: String,
        expected_type: String,
    },
    /// Unsupported statement type in object file
    UnsupportedStatement {
        object_name: String,
        statement_type: String,
    },
    /// Unsupported grant type
    ClusterGrantUnsupported,
    /// Grant doesn't target specific object
    GrantMustTargetObject,
    /// System grant not supported
    SystemGrantUnsupported,
    /// Unsupported comment type
    UnsupportedCommentType,
    /// No object type could be determined
    NoObjectType,
    /// Failed to extract schema name from file path
    SchemaExtractionFailed,
    /// Failed to extract database name from file path
    DatabaseExtractionFailed,
    /// Invalid identifier name (contains invalid characters or format)
    InvalidIdentifier { name: String, reason: String },
    /// Index missing required IN CLUSTER clause
    IndexMissingCluster { index_name: String },
    /// Materialized view missing required IN CLUSTER clause
    MaterializedViewMissingCluster { view_name: String },
    /// Sink missing required IN CLUSTER clause
    SinkMissingCluster { sink_name: String },
    /// Source missing required IN CLUSTER clause
    SourceMissingCluster { source_name: String },
    /// Source uses external references (FOR TABLES/FOR SCHEMAS/FOR ALL TABLES)
    SourceExternalReferences { source_name: String },
    /// Invalid statement type in database mod file
    InvalidDatabaseModStatement {
        statement_type: String,
        database_name: String,
    },
    /// Comment in database mod file targets wrong object
    DatabaseModCommentTargetMismatch {
        target: String,
        database_name: String,
    },
    /// Grant in database mod file targets wrong object
    DatabaseModGrantTargetMismatch {
        target: String,
        database_name: String,
    },
    /// Invalid statement type in schema mod file
    InvalidSchemaModStatement {
        statement_type: String,
        schema_name: String,
    },
    /// Comment in schema mod file targets wrong object
    SchemaModCommentTargetMismatch { target: String, schema_name: String },
    /// Grant in schema mod file targets wrong object
    SchemaModGrantTargetMismatch { target: String, schema_name: String },
    /// SET variable in schema mod file has invalid value
    InvalidSetVariable { variable: String, value: String },
    /// ALTER DEFAULT PRIVILEGES in database mod requires IN DATABASE scope
    AlterDefaultPrivilegesRequiresDatabaseScope { database_name: String },
    /// ALTER DEFAULT PRIVILEGES in schema mod requires IN SCHEMA scope
    AlterDefaultPrivilegesRequiresSchemaScope { schema_name: String },
    /// ALTER DEFAULT PRIVILEGES IN DATABASE references wrong database
    AlterDefaultPrivilegesDatabaseMismatch {
        referenced: String,
        expected: String,
    },
    /// ALTER DEFAULT PRIVILEGES cannot use IN SCHEMA in database mod
    AlterDefaultPrivilegesSchemaNotAllowed { database_name: String },
    /// ALTER DEFAULT PRIVILEGES cannot use IN DATABASE in schema mod
    AlterDefaultPrivilegesDatabaseNotAllowed { schema_name: String },
    /// ALTER DEFAULT PRIVILEGES IN SCHEMA references wrong schema
    AlterDefaultPrivilegesSchemaMismatch {
        referenced: String,
        expected: String,
    },
    /// Storage objects (tables/sinks) and computation objects (views/MVs) cannot share a schema
    StorageAndComputationObjectsInSameSchema {
        schema_name: String,
        storage_objects: Vec<String>,
        computation_objects: Vec<String>,
    },
    /// Replacement schema contains non-MV objects
    ReplacementSchemaNonMvObject {
        database: String,
        schema: String,
        object_name: String,
        object_type: String,
    },
    /// Invalid statement type in cluster definition file
    InvalidClusterStatement {
        statement_type: String,
        cluster_name: String,
    },
    /// Cluster name in CREATE CLUSTER doesn't match filename
    ClusterNameMismatch { declared: String, expected: String },
    /// Cluster file missing required CREATE CLUSTER statement
    ClusterMissingCreateStatement { cluster_name: String },
    /// Cluster file contains multiple CREATE CLUSTER statements
    ClusterMultipleCreateStatements { cluster_name: String },
    /// GRANT in cluster file targets a different cluster
    ClusterGrantTargetMismatch {
        target: String,
        cluster_name: String,
    },
    /// COMMENT in cluster file targets a different cluster
    ClusterCommentTargetMismatch {
        target: String,
        cluster_name: String,
    },
    /// Invalid statement type in role definition file
    InvalidRoleStatement {
        statement_type: String,
        role_name: String,
    },
    /// Role name in CREATE ROLE doesn't match filename
    RoleNameMismatch { declared: String, expected: String },
    /// Role file missing required CREATE ROLE statement
    RoleMissingCreateStatement { role_name: String },
    /// Role file contains multiple CREATE ROLE statements
    RoleMultipleCreateStatements { role_name: String },
    /// ALTER ROLE in role file targets a different role
    RoleAlterTargetMismatch { target: String, role_name: String },
    /// GRANT ROLE in role file targets a different role
    RoleGrantTargetMismatch { target: String, role_name: String },
    /// COMMENT in role file targets a different role
    RoleCommentTargetMismatch { target: String, role_name: String },
    /// Invalid statement type in network policy definition file
    InvalidNetworkPolicyStatement {
        statement_type: String,
        policy_name: String,
    },
    /// Network policy name in CREATE NETWORK POLICY doesn't match filename
    NetworkPolicyNameMismatch { declared: String, expected: String },
    /// Network policy file missing required CREATE NETWORK POLICY statement
    NetworkPolicyMissingCreateStatement { policy_name: String },
    /// Network policy file contains multiple CREATE NETWORK POLICY statements
    NetworkPolicyMultipleCreateStatements { policy_name: String },
    /// GRANT in network policy file targets a different policy
    NetworkPolicyGrantTargetMismatch { target: String, policy_name: String },
    /// COMMENT in network policy file targets a different policy
    NetworkPolicyCommentTargetMismatch { target: String, policy_name: String },
    /// Profile variants of an object have different primary statement types
    ProfileObjectTypeMismatch {
        object_name: String,
        default_type: String,
        override_profile: String,
        override_type: String,
        default_path: PathBuf,
        override_path: PathBuf,
    },
    /// A constraint references a different object
    ConstraintReferenceMismatch {
        referenced: String,
        expected: String,
    },
    /// Constraint on a table or table from source (not allowed)
    ConstraintNotAllowedOnTable {
        constraint_name: String,
        object_type: String,
    },
    /// Enforced constraint on a view (not allowed)
    EnforcedConstraintNotAllowed {
        constraint_name: String,
        object_type: String,
    },
    /// Enforced constraint missing required IN CLUSTER clause
    EnforcedConstraintMissingCluster { constraint_name: String },
    /// Not-enforced constraint has IN CLUSTER clause (not allowed)
    NotEnforcedConstraintHasCluster { constraint_name: String },
    /// Enforced foreign key references an object that isn't table/table-from-source/MV
    EnforcedForeignKeyInvalidTarget {
        constraint_name: String,
        target: String,
        target_type: String,
    },
    /// Enforced foreign key references an external object (not defined in the project)
    EnforcedForeignKeyExternalTarget {
        constraint_name: String,
        target: String,
    },
    /// Views and materialized views cannot have profile-specific overrides
    ProfileOverrideNotAllowed {
        object_name: String,
        object_type: String,
        override_profile: String,
        override_path: PathBuf,
    },
    /// Constraint column does not exist on its parent object
    ConstraintColumnNotFound {
        constraint_name: String,
        column: String,
        object: String,
        available_columns: Vec<String>,
    },
    /// FK reference column does not exist on the referenced object
    FkRefColumnNotFound {
        constraint_name: String,
        column: String,
        ref_object: String,
        available_columns: Vec<String>,
    },
    /// FK target object is an invalid type for the constraint's enforcement level
    FkInvalidTargetType {
        constraint_name: String,
        ref_object: String,
        ref_kind: String,
        enforced: bool,
    },
}

impl ValidationErrorKind {
    /// Get the short error message for this error kind
    pub(crate) fn message(&self) -> String {
        match self {
            Self::MultipleMainStatements { object_name } => {
                format!(
                    "multiple main CREATE statements found for object '{}'",
                    object_name
                )
            }
            Self::NoMainStatement { object_name } => {
                format!(
                    "no main CREATE statement found for object '{}'",
                    object_name
                )
            }
            Self::ObjectNameMismatch { declared, expected } => {
                format!(
                    "object name mismatch: declared '{}', expected '{}'",
                    declared, expected
                )
            }
            Self::SchemaMismatch { declared, expected } => {
                format!(
                    "schema qualifier mismatch: declared '{}', expected '{}'",
                    declared, expected
                )
            }
            Self::DatabaseMismatch { declared, expected } => {
                format!(
                    "database qualifier mismatch: declared '{}', expected '{}'",
                    declared, expected
                )
            }
            Self::IndexReferenceMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "INDEX references wrong object: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::GrantReferenceMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "GRANT references wrong object: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::CommentReferenceMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "COMMENT references wrong object: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::ColumnCommentReferenceMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "column COMMENT references wrong table: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::CommentTypeMismatch {
                comment_type,
                object_type,
            } => {
                format!(
                    "COMMENT uses wrong object type: {} instead of {}",
                    comment_type, object_type
                )
            }
            Self::GrantTypeMismatch {
                grant_type,
                expected_type,
            } => {
                format!(
                    "GRANT uses incorrect object type: GRANT ON {} instead of GRANT ON {}",
                    grant_type, expected_type
                )
            }
            Self::UnsupportedStatement {
                object_name,
                statement_type,
            } => {
                format!(
                    "unsupported statement type in object '{}': {}",
                    object_name, statement_type
                )
            }
            Self::ClusterGrantUnsupported => "CLUSTER grants are not supported".to_string(),
            Self::GrantMustTargetObject => "GRANT must target a specific object".to_string(),
            Self::SystemGrantUnsupported => "SYSTEM grants are not supported".to_string(),
            Self::UnsupportedCommentType => "unsupported COMMENT object type".to_string(),
            Self::NoObjectType => "could not determine object type".to_string(),
            Self::SchemaExtractionFailed => {
                "failed to extract schema name from file path".to_string()
            }
            Self::DatabaseExtractionFailed => {
                "failed to extract database name from file path".to_string()
            }
            Self::InvalidIdentifier { name, reason } => {
                format!("invalid identifier '{}': {}", name, reason)
            }
            Self::IndexMissingCluster { index_name } => {
                format!(
                    "index '{}' is missing required IN CLUSTER clause",
                    index_name
                )
            }
            Self::MaterializedViewMissingCluster { view_name } => {
                format!(
                    "materialized view '{}' is missing required IN CLUSTER clause",
                    view_name
                )
            }
            Self::SinkMissingCluster { sink_name } => {
                format!("sink '{}' is missing required IN CLUSTER clause", sink_name)
            }
            Self::SourceMissingCluster { source_name } => {
                format!(
                    "source '{}' is missing required IN CLUSTER clause",
                    source_name
                )
            }
            Self::SourceExternalReferences { source_name } => {
                format!(
                    "source '{}' uses FOR TABLES, FOR SCHEMAS, or FOR ALL TABLES which is not supported",
                    source_name
                )
            }
            Self::InvalidDatabaseModStatement {
                statement_type,
                database_name,
            } => {
                format!(
                    "invalid statement type in database mod file '{}': {}. Only COMMENT ON DATABASE, GRANT ON DATABASE, and ALTER DEFAULT PRIVILEGES are allowed",
                    database_name, statement_type
                )
            }
            Self::DatabaseModCommentTargetMismatch {
                target,
                database_name,
            } => {
                format!(
                    "comment in database mod file must target the database itself. Expected COMMENT ON DATABASE '{}', but found COMMENT ON {}",
                    database_name, target
                )
            }
            Self::DatabaseModGrantTargetMismatch {
                target,
                database_name,
            } => {
                format!(
                    "grant in database mod file must target the database itself. Expected GRANT ON DATABASE '{}', but found GRANT ON {}",
                    database_name, target
                )
            }
            Self::InvalidSchemaModStatement {
                statement_type,
                schema_name,
            } => {
                format!(
                    "invalid statement type in schema mod file '{}': {}. Only COMMENT ON SCHEMA, GRANT ON SCHEMA, ALTER DEFAULT PRIVILEGES, and SET api = stable are allowed",
                    schema_name, statement_type
                )
            }
            Self::SchemaModCommentTargetMismatch {
                target,
                schema_name,
            } => {
                format!(
                    "comment in schema mod file must target the schema itself. Expected COMMENT ON SCHEMA '{}', but found COMMENT ON {}",
                    schema_name, target
                )
            }
            Self::SchemaModGrantTargetMismatch {
                target,
                schema_name,
            } => {
                format!(
                    "grant in schema mod file must target the schema itself. Expected GRANT ON SCHEMA '{}', but found GRANT ON {}",
                    schema_name, target
                )
            }
            Self::InvalidSetVariable { variable, value } => {
                format!("invalid value for SET {}: {}", variable, value)
            }
            Self::AlterDefaultPrivilegesRequiresDatabaseScope { database_name } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES in database mod file '{}' must specify IN DATABASE",
                    database_name
                )
            }
            Self::AlterDefaultPrivilegesRequiresSchemaScope { schema_name } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES in schema mod file '{}' must specify IN SCHEMA",
                    schema_name
                )
            }
            Self::AlterDefaultPrivilegesDatabaseMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES IN DATABASE references wrong database: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::AlterDefaultPrivilegesSchemaNotAllowed { database_name } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES in database mod file '{}' cannot use IN SCHEMA",
                    database_name
                )
            }
            Self::AlterDefaultPrivilegesDatabaseNotAllowed { schema_name } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES in schema mod file '{}' cannot use IN DATABASE",
                    schema_name
                )
            }
            Self::AlterDefaultPrivilegesSchemaMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA references wrong schema: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::StorageAndComputationObjectsInSameSchema {
                schema_name,
                storage_objects,
                computation_objects,
            } => {
                format!(
                    "schema '{}' contains both storage objects (tables/sinks) and computation objects (views/materialized views)\n  \
                     Storage objects (tables/sinks): [{}]\n  \
                     Computation objects (views/MVs): [{}]",
                    schema_name,
                    storage_objects.join(", "),
                    computation_objects.join(", ")
                )
            }
            Self::ReplacementSchemaNonMvObject {
                database,
                schema,
                object_name,
                object_type,
            } => {
                format!(
                    "replacement schema '{}.{}' contains non-materialized-view object '{}' (type: {})",
                    database, schema, object_name, object_type
                )
            }
            Self::InvalidClusterStatement {
                statement_type,
                cluster_name,
            } => {
                format!(
                    "invalid statement type in cluster file '{}': {}. Only CREATE CLUSTER, GRANT ON CLUSTER, and COMMENT ON CLUSTER are allowed",
                    cluster_name, statement_type
                )
            }
            Self::ClusterNameMismatch { declared, expected } => {
                format!(
                    "cluster name mismatch: declared '{}', expected '{}'",
                    declared, expected
                )
            }
            Self::ClusterMissingCreateStatement { cluster_name } => {
                format!(
                    "no CREATE CLUSTER statement found in cluster file '{}'",
                    cluster_name
                )
            }
            Self::ClusterMultipleCreateStatements { cluster_name } => {
                format!(
                    "multiple CREATE CLUSTER statements found in cluster file '{}'",
                    cluster_name
                )
            }
            Self::ClusterGrantTargetMismatch {
                target,
                cluster_name,
            } => {
                format!(
                    "GRANT in cluster file '{}' targets wrong cluster: '{}'",
                    cluster_name, target
                )
            }
            Self::ClusterCommentTargetMismatch {
                target,
                cluster_name,
            } => {
                format!(
                    "COMMENT in cluster file '{}' targets wrong cluster: '{}'",
                    cluster_name, target
                )
            }
            Self::InvalidRoleStatement {
                statement_type,
                role_name,
            } => {
                format!(
                    "invalid statement type in role file '{}': {}. Only CREATE ROLE, ALTER ROLE, GRANT ROLE, and COMMENT ON ROLE are allowed",
                    role_name, statement_type
                )
            }
            Self::RoleNameMismatch { declared, expected } => {
                format!(
                    "role name mismatch: declared '{}', expected '{}'",
                    declared, expected
                )
            }
            Self::RoleMissingCreateStatement { role_name } => {
                format!(
                    "no CREATE ROLE statement found in role file '{}'",
                    role_name
                )
            }
            Self::RoleMultipleCreateStatements { role_name } => {
                format!(
                    "multiple CREATE ROLE statements found in role file '{}'",
                    role_name
                )
            }
            Self::RoleAlterTargetMismatch { target, role_name } => {
                format!(
                    "ALTER ROLE in role file '{}' targets wrong role: '{}'",
                    role_name, target
                )
            }
            Self::RoleGrantTargetMismatch { target, role_name } => {
                format!(
                    "GRANT ROLE in role file '{}' targets wrong role: '{}'",
                    role_name, target
                )
            }
            Self::RoleCommentTargetMismatch { target, role_name } => {
                format!(
                    "COMMENT in role file '{}' targets wrong role: '{}'",
                    role_name, target
                )
            }
            Self::InvalidNetworkPolicyStatement {
                statement_type,
                policy_name,
            } => {
                format!(
                    "invalid statement type in network policy file '{}': {}. Only CREATE NETWORK POLICY, GRANT ON NETWORK POLICY, and COMMENT ON NETWORK POLICY are allowed",
                    policy_name, statement_type
                )
            }
            Self::NetworkPolicyNameMismatch { declared, expected } => {
                format!(
                    "network policy name mismatch: declared '{}', expected '{}'",
                    declared, expected
                )
            }
            Self::NetworkPolicyMissingCreateStatement { policy_name } => {
                format!(
                    "no CREATE NETWORK POLICY statement found in network policy file '{}'",
                    policy_name
                )
            }
            Self::NetworkPolicyMultipleCreateStatements { policy_name } => {
                format!(
                    "multiple CREATE NETWORK POLICY statements found in network policy file '{}'",
                    policy_name
                )
            }
            Self::NetworkPolicyGrantTargetMismatch {
                target,
                policy_name,
            } => {
                format!(
                    "GRANT in network policy file '{}' targets wrong policy: '{}'",
                    policy_name, target
                )
            }
            Self::NetworkPolicyCommentTargetMismatch {
                target,
                policy_name,
            } => {
                format!(
                    "COMMENT in network policy file '{}' targets wrong policy: '{}'",
                    policy_name, target
                )
            }
            Self::ConstraintReferenceMismatch {
                referenced,
                expected,
            } => {
                format!(
                    "CONSTRAINT references wrong object: '{}' instead of '{}'",
                    referenced, expected
                )
            }
            Self::ConstraintNotAllowedOnTable {
                constraint_name,
                object_type,
            } => {
                format!(
                    "constraint '{}' is not allowed on {}",
                    constraint_name, object_type
                )
            }
            Self::EnforcedConstraintNotAllowed {
                constraint_name,
                object_type,
            } => {
                format!(
                    "enforced constraint '{}' is not allowed on {}",
                    constraint_name, object_type
                )
            }
            Self::EnforcedConstraintMissingCluster { constraint_name } => {
                format!(
                    "enforced constraint '{}' is missing required IN CLUSTER clause",
                    constraint_name
                )
            }
            Self::NotEnforcedConstraintHasCluster { constraint_name } => {
                format!(
                    "not-enforced constraint '{}' must not specify IN CLUSTER",
                    constraint_name
                )
            }
            Self::EnforcedForeignKeyInvalidTarget {
                constraint_name,
                target,
                target_type,
            } => {
                format!(
                    "enforced foreign key '{}' references '{}' which is a {} (must be table, table from source, or materialized view)",
                    constraint_name, target, target_type
                )
            }
            Self::EnforcedForeignKeyExternalTarget {
                constraint_name,
                target,
            } => {
                format!(
                    "enforced foreign key '{}' references external object '{}' (not defined in this project)",
                    constraint_name, target
                )
            }
            Self::ProfileObjectTypeMismatch {
                object_name,
                default_type,
                override_profile,
                override_type,
                ..
            } => {
                format!(
                    "profile variant type mismatch for object '{}': default is '{}' but '{}' override is '{}'",
                    object_name, default_type, override_profile, override_type
                )
            }
            Self::ProfileOverrideNotAllowed {
                object_name,
                object_type,
                override_profile,
                ..
            } => {
                format!(
                    "{} '{}' cannot have profile-specific overrides (found '{}' override)",
                    object_type, object_name, override_profile
                )
            }
            Self::ConstraintColumnNotFound {
                constraint_name,
                column,
                object,
                ..
            } => {
                format!(
                    "constraint '{}': column \"{}\" does not exist on {}",
                    constraint_name, column, object
                )
            }
            Self::FkRefColumnNotFound {
                constraint_name,
                column,
                ref_object,
                ..
            } => {
                format!(
                    "constraint '{}': column \"{}\" does not exist on referenced object {}",
                    constraint_name, column, ref_object
                )
            }
            Self::FkInvalidTargetType {
                constraint_name,
                ref_object,
                ref_kind,
                enforced,
            } => {
                let kind_desc = if *enforced {
                    "enforced foreign key"
                } else {
                    "foreign key"
                };
                format!(
                    "{} '{}' cannot reference {} ({})",
                    kind_desc, constraint_name, ref_object, ref_kind
                )
            }
        }
    }

    /// Get the help text for this error kind
    pub(crate) fn help(&self) -> Option<String> {
        match self {
            Self::MultipleMainStatements { .. } => {
                Some("each file must contain exactly one primary CREATE statement (TABLE, VIEW, SOURCE, etc.)".to_string())
            }
            Self::NoMainStatement { .. } => {
                Some("each file must contain exactly one primary CREATE statement (CREATE TABLE, CREATE VIEW, etc.)".to_string())
            }
            Self::ObjectNameMismatch { .. } => {
                Some("the object name in your CREATE statement must match the .sql file name".to_string())
            }
            Self::SchemaMismatch { .. } => {
                Some("the schema in your qualified object name must match the directory name".to_string())
            }
            Self::DatabaseMismatch { .. } => {
                Some("the database in your qualified object name must match the directory name".to_string())
            }
            Self::IndexReferenceMismatch { .. } => {
                Some("indexes must be defined in the same file as the object they're created on".to_string())
            }
            Self::GrantReferenceMismatch { .. } => {
                Some("grants must be defined in the same file as the object they apply to".to_string())
            }
            Self::CommentReferenceMismatch { .. } => {
                Some("comments must be defined in the same file as the object they describe".to_string())
            }
            Self::ColumnCommentReferenceMismatch { .. } => {
                Some("column comments must reference columns in the object defined in the file".to_string())
            }
            Self::CommentTypeMismatch { .. } => {
                Some("the COMMENT statement must use the correct object type (TABLE, VIEW, etc.)".to_string())
            }
            Self::GrantTypeMismatch { .. } => {
                Some("the GRANT statement must use the correct object type that matches the object defined in the file".to_string())
            }
            Self::UnsupportedStatement { .. } => {
                Some("only CREATE, INDEX, GRANT, and COMMENT statements are supported in object files".to_string())
            }
            Self::ClusterGrantUnsupported => {
                Some("use GRANT ON specific objects instead of CLUSTER".to_string())
            }
            Self::GrantMustTargetObject => {
                Some("use GRANT ON objectname instead of GRANT ON ALL TABLES or similar".to_string())
            }
            Self::SystemGrantUnsupported => {
                Some("use GRANT ON specific objects instead of SYSTEM".to_string())
            }
            Self::UnsupportedCommentType => {
                Some("only comments on tables, views, sources, sinks, connections, secrets, and columns are supported".to_string())
            }
            Self::NoObjectType | Self::SchemaExtractionFailed | Self::DatabaseExtractionFailed => {
                Some("this is an internal error, please report this issue".to_string())
            }
            Self::InvalidIdentifier { .. } => {
                Some("identifiers must follow SQL naming rules (alphanumeric and underscores, must not start with a digit)".to_string())
            }
            Self::IndexMissingCluster { .. } => {
                Some("add 'IN CLUSTER <cluster_name>' to your CREATE INDEX statement (e.g., CREATE INDEX idx ON table (col) IN CLUSTER quickstart)".to_string())
            }
            Self::MaterializedViewMissingCluster { .. } => {
                Some("add 'IN CLUSTER <cluster_name>' to your CREATE MATERIALIZED VIEW statement (e.g., CREATE MATERIALIZED VIEW mv IN CLUSTER quickstart AS SELECT ...)".to_string())
            }
            Self::SinkMissingCluster { .. } => {
                Some("add 'IN CLUSTER <cluster_name>' to your CREATE SINK statement (e.g., CREATE SINK sink IN CLUSTER quickstart FROM ...)".to_string())
            }
            Self::SourceMissingCluster { .. } => {
                Some("add 'IN CLUSTER <cluster_name>' to your CREATE SOURCE statement (e.g., CREATE SOURCE src IN CLUSTER quickstart FROM ...)".to_string())
            }
            Self::SourceExternalReferences { .. } => {
                Some("use CREATE TABLE FROM SOURCE to define tables individually so mz-deploy can manage their lifecycle, privileges, and comments".to_string())
            }
            Self::InvalidDatabaseModStatement { .. } => {
                Some("database mod files (e.g., materialize.sql) can only contain COMMENT ON DATABASE, GRANT ON DATABASE, and ALTER DEFAULT PRIVILEGES statements".to_string())
            }
            Self::DatabaseModCommentTargetMismatch { .. } => {
                Some("comments in database mod files must target the database itself using COMMENT ON DATABASE".to_string())
            }
            Self::DatabaseModGrantTargetMismatch { .. } => {
                Some("grants in database mod files must target the database itself using GRANT ON DATABASE".to_string())
            }
            Self::InvalidSchemaModStatement { .. } => {
                Some("schema mod files (e.g., materialize/public.sql) can only contain COMMENT ON SCHEMA, GRANT ON SCHEMA, ALTER DEFAULT PRIVILEGES, and SET api = stable statements".to_string())
            }
            Self::SchemaModCommentTargetMismatch { .. } => {
                Some("comments in schema mod files must target the schema itself using COMMENT ON SCHEMA".to_string())
            }
            Self::SchemaModGrantTargetMismatch { .. } => {
                Some("grants in schema mod files must target the schema itself using GRANT ON SCHEMA".to_string())
            }
            Self::InvalidSetVariable { .. } => {
                Some("only 'SET api = stable' is supported in schema mod files".to_string())
            }
            Self::AlterDefaultPrivilegesRequiresDatabaseScope { .. } => {
                Some("add 'IN DATABASE <database_name>' to your ALTER DEFAULT PRIVILEGES statement".to_string())
            }
            Self::AlterDefaultPrivilegesRequiresSchemaScope { .. } => {
                Some("add 'IN SCHEMA <schema_name>' to your ALTER DEFAULT PRIVILEGES statement".to_string())
            }
            Self::AlterDefaultPrivilegesDatabaseMismatch { .. } => {
                Some("ALTER DEFAULT PRIVILEGES in database mod files must target the database itself".to_string())
            }
            Self::AlterDefaultPrivilegesSchemaNotAllowed { .. } => {
                Some("use IN DATABASE instead of IN SCHEMA in database mod files".to_string())
            }
            Self::AlterDefaultPrivilegesDatabaseNotAllowed { .. } => {
                Some("use IN SCHEMA instead of IN DATABASE in schema mod files".to_string())
            }
            Self::AlterDefaultPrivilegesSchemaMismatch { .. } => {
                Some("ALTER DEFAULT PRIVILEGES in schema mod files must target the schema itself".to_string())
            }
            Self::StorageAndComputationObjectsInSameSchema { .. } => {
                Some("storage objects (tables, sinks) cannot share a schema with computation objects (views, materialized views) to prevent accidentally recreating tables or sinks when recreating views. Organize your schemas: use one schema for storage objects (e.g., 'tables') and another for computation objects (e.g., 'views' or 'public')".to_string())
            }
            Self::ReplacementSchemaNonMvObject { .. } => {
                Some("schemas with SET api = stable can only contain CREATE MATERIALIZED VIEW statements".to_string())
            }
            Self::InvalidClusterStatement { .. } => {
                Some("cluster files can only contain CREATE CLUSTER, GRANT ON CLUSTER, and COMMENT ON CLUSTER statements".to_string())
            }
            Self::ClusterNameMismatch { .. } => {
                Some("the cluster name in your CREATE CLUSTER statement must match the .sql file name".to_string())
            }
            Self::ClusterMissingCreateStatement { .. } => {
                Some("each cluster file must contain exactly one CREATE CLUSTER statement".to_string())
            }
            Self::ClusterMultipleCreateStatements { .. } => {
                Some("each cluster file must contain exactly one CREATE CLUSTER statement".to_string())
            }
            Self::ClusterGrantTargetMismatch { .. } => {
                Some("GRANT statements in a cluster file must target the cluster defined in that file".to_string())
            }
            Self::ClusterCommentTargetMismatch { .. } => {
                Some("COMMENT statements in a cluster file must target the cluster defined in that file".to_string())
            }
            Self::InvalidRoleStatement { .. } => {
                Some("role files can only contain CREATE ROLE, ALTER ROLE, GRANT ROLE, and COMMENT ON ROLE statements".to_string())
            }
            Self::RoleNameMismatch { .. } => {
                Some("the role name in your CREATE ROLE statement must match the .sql file name".to_string())
            }
            Self::RoleMissingCreateStatement { .. } => {
                Some("each role file must contain exactly one CREATE ROLE statement".to_string())
            }
            Self::RoleMultipleCreateStatements { .. } => {
                Some("each role file must contain exactly one CREATE ROLE statement".to_string())
            }
            Self::RoleAlterTargetMismatch { .. } => {
                Some("ALTER ROLE statements in a role file must target the role defined in that file".to_string())
            }
            Self::RoleGrantTargetMismatch { .. } => {
                Some("GRANT ROLE statements in a role file must grant the role defined in that file".to_string())
            }
            Self::RoleCommentTargetMismatch { .. } => {
                Some("COMMENT statements in a role file must target the role defined in that file".to_string())
            }
            Self::InvalidNetworkPolicyStatement { .. } => {
                Some("network policy files can only contain CREATE NETWORK POLICY, GRANT ON NETWORK POLICY, and COMMENT ON NETWORK POLICY statements".to_string())
            }
            Self::NetworkPolicyNameMismatch { .. } => {
                Some("the network policy name in your CREATE NETWORK POLICY statement must match the .sql file name".to_string())
            }
            Self::NetworkPolicyMissingCreateStatement { .. } => {
                Some("each network policy file must contain exactly one CREATE NETWORK POLICY statement".to_string())
            }
            Self::NetworkPolicyMultipleCreateStatements { .. } => {
                Some("each network policy file must contain exactly one CREATE NETWORK POLICY statement".to_string())
            }
            Self::NetworkPolicyGrantTargetMismatch { .. } => {
                Some("GRANT statements in a network policy file must target the policy defined in that file".to_string())
            }
            Self::NetworkPolicyCommentTargetMismatch { .. } => {
                Some("COMMENT statements in a network policy file must target the policy defined in that file".to_string())
            }
            Self::ConstraintReferenceMismatch { .. } => {
                Some("constraints must be defined in the same file as the object they're created on".to_string())
            }
            Self::ConstraintNotAllowedOnTable { .. } => {
                Some("constraints can only be defined on views (NOT ENFORCED) and materialized views".to_string())
            }
            Self::EnforcedConstraintNotAllowed { .. } => {
                Some("enforced constraints can only be created on materialized views".to_string())
            }
            Self::EnforcedConstraintMissingCluster { .. } => {
                Some("add 'IN CLUSTER <cluster_name>' to your enforced constraint statement".to_string())
            }
            Self::NotEnforcedConstraintHasCluster { .. } => {
                Some("remove the IN CLUSTER clause from your NOT ENFORCED constraint".to_string())
            }
            Self::EnforcedForeignKeyInvalidTarget { .. } => {
                Some("enforced foreign keys can only reference tables, tables from source, or materialized views".to_string())
            }
            Self::EnforcedForeignKeyExternalTarget { .. } => {
                Some("enforced foreign keys can only reference objects defined in this project".to_string())
            }
            Self::ProfileObjectTypeMismatch { .. } => {
                Some("all profile variants of an object must have the same primary statement type (e.g., all CREATE SECRET or all CREATE TABLE)".to_string())
            }
            Self::ProfileOverrideNotAllowed { .. } => {
                Some("views and materialized views cannot have profile-specific overrides because their definitions should be consistent across environments".to_string())
            }
            Self::ConstraintColumnNotFound { available_columns, .. } => {
                if available_columns.is_empty() {
                    None
                } else {
                    Some(format!("available columns: {}", available_columns.join(", ")))
                }
            }
            Self::FkRefColumnNotFound { available_columns, .. } => {
                if available_columns.is_empty() {
                    None
                } else {
                    Some(format!("available columns: {}", available_columns.join(", ")))
                }
            }
            Self::FkInvalidTargetType { enforced, .. } => {
                if *enforced {
                    Some("enforced foreign keys can only reference tables, tables from source, or materialized views".to_string())
                } else {
                    Some("foreign keys can only reference tables, tables from source, materialized views, or views".to_string())
                }
            }
        }
    }
}

/// A collection of validation errors grouped by location.
///
/// This type holds multiple validation errors that occurred during project validation.
/// It provides formatted output that groups errors by database, schema, and file for
/// easier navigation and fixing.
#[derive(Debug)]
pub struct ValidationErrors {
    pub errors: Vec<ValidationError>,
}

impl ValidationErrors {
    /// Create a new collection from a vector of errors
    pub fn new(errors: Vec<ValidationError>) -> Self {
        Self { errors }
    }

    /// Check if there are any errors
    pub fn is_empty(&self) -> bool {
        self.errors.is_empty()
    }

    /// Get the number of errors
    pub fn len(&self) -> usize {
        self.errors.len()
    }

    /// Convert into a Result, returning Err if there are any errors
    pub fn into_result(self) -> Result<(), Self> {
        if self.is_empty() { Ok(()) } else { Err(self) }
    }
}

impl fmt::Display for ValidationErrors {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.errors.is_empty() {
            return Ok(());
        }

        // Group errors by file path
        let mut grouped: BTreeMap<PathBuf, Vec<&ValidationError>> = BTreeMap::new();
        for error in &self.errors {
            grouped
                .entry(error.context.file.clone())
                .or_default()
                .push(error);
        }

        // Display errors grouped by file (like rustc does)
        for (file_path, errors) in grouped.iter() {
            // Extract database/schema/file for path display
            let path_components: Vec<_> = file_path.components().collect();
            let len = path_components.len();

            let relative_path = if len >= 3 {
                format!(
                    "{}/{}/{}",
                    path_components[len - 3].as_os_str().to_string_lossy(),
                    path_components[len - 2].as_os_str().to_string_lossy(),
                    path_components[len - 1].as_os_str().to_string_lossy()
                )
            } else {
                file_path.display().to_string()
            };

            // Display each error for this file
            for error in errors {
                // Format like rustc: error: <message>
                writeln!(
                    f,
                    "{}: {}",
                    "error".bright_red().bold(),
                    error.kind.message()
                )?;

                // Show file location: --> path
                writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;

                // Add SQL statement if available
                if let Some(ref sql) = error.context.sql_statement {
                    writeln!(f, "  {}", "|".bright_blue().bold())?;
                    for line in sql.lines() {
                        writeln!(f, "  {} {}", "|".bright_blue().bold(), line)?;
                    }
                    writeln!(f, "  {}", "|".bright_blue().bold())?;
                }

                // Add help text if available
                if let Some(help) = error.kind.help() {
                    writeln!(
                        f,
                        "  {} {}",
                        "=".bright_blue().bold(),
                        format!("help: {}", help).bold()
                    )?;
                }

                writeln!(f)?;
            }
        }

        // Summary line at the end (like rustc)
        writeln!(
            f,
            "{}: could not compile due to {} previous error{}",
            "error".bright_red().bold(),
            self.errors.len(),
            if self.errors.len() == 1 { "" } else { "s" }
        )?;

        Ok(())
    }
}

impl std::error::Error for ValidationErrors {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}
