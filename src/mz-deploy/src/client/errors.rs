// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error types for the client module.
//!
//! Two top-level enums cover different failure modes:
//!
//! - [`ConnectionError`] — Transport and query failures: connection refused,
//!   SQL errors, missing dependencies, configuration problems, and DDL
//!   failures.
//! - [`DatabaseValidationError`] — Semantic mismatches detected during
//!   pre-deployment validation (e.g., schema conflicts, unexpected objects).

use crate::config::ConfigError;
use crate::project::SchemaQualifier;
use crate::project::ir::object_id::ObjectId;
use owo_colors::{OwoColorize, Stream, Style};
use std::fmt;
use std::path::PathBuf;
use thiserror::Error;

/// Errors that can occur during database operations.
#[derive(Debug, Error)]
pub enum ConnectionError {
    #[error("configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("failed to connect to {host}:{port}: {source}")]
    Connect {
        host: String,
        port: u16,
        source: tokio_postgres::Error,
    },

    #[error(
        "TLS required by profile but server at {host}:{port} does not support TLS\n\
         \n\
         help: The server did not offer TLS. To connect without encryption, set\n\
         \x20     sslmode = \"disable\" on the profile. To use TLS if available\n\
         \x20     but fall back to plaintext otherwise, set sslmode = \"prefer\"."
    )]
    TlsRequiredNotSupported {
        host: String,
        port: u16,
        source: tokio_postgres::Error,
    },

    #[error(
        "TLS certificate verification failed for {host}:{port}: {source}\n\
         \n\
         help: The server's certificate could not be verified against the trusted\n\
         \x20     CA bundle{hostname_suffix}. To skip verification, set\n\
         \x20     sslmode = \"require\" or sslmode = \"prefer\". To use a custom\n\
         \x20     CA bundle, set sslrootcert = \"/path/to/ca.pem\" on the profile."
    )]
    TlsVerification {
        host: String,
        port: u16,
        hostname_suffix: &'static str,
        source: tokio_postgres::Error,
    },

    #[error(
        "no CA bundle found for TLS verification\n\
         \n\
         help: Set sslrootcert = \"/path/to/ca.pem\" on the profile to point at\n\
         \x20     a specific CA bundle, or install the system CA bundle at one\n\
         \x20     of: /etc/ssl/cert.pem, /etc/ssl/certs/ca-certificates.crt, or\n\
         \x20     the platform-appropriate equivalent."
    )]
    TlsCaNotFound,

    #[error("{}", format_query_error(.0))]
    Query(tokio_postgres::Error),

    #[error("dependency error: {0}")]
    Dependency(#[from] crate::project::error::DependencyError),

    #[error("failed to create database '{database}': {source}")]
    DatabaseCreationFailed {
        database: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("failed to create schema '{database}.{schema}': {source}")]
    SchemaCreationFailed {
        database: String,
        schema: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("failed to create cluster '{name}': {source}")]
    ClusterCreationFailed {
        name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("cluster '{name}' already exists")]
    ClusterAlreadyExists { name: String },

    #[error("introspection failed for {object_type}: {source}")]
    IntrospectionFailed {
        object_type: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[error("cluster '{name}' not found")]
    ClusterNotFound { name: String },

    #[error("deployment '{deploy_id}' already exists")]
    DeploymentAlreadyExists { deploy_id: String },

    #[error("deployment '{deploy_id}' not found")]
    DeploymentNotFound { deploy_id: String },

    #[error("deployment '{deploy_id}' has already been promoted to production")]
    DeploymentAlreadyPromoted { deploy_id: String },

    #[error("unsupported statement type: {0}")]
    UnsupportedStatementType(String),

    #[error("{0}")]
    Message(String),
}

fn format_query_error(error: &tokio_postgres::Error) -> String {
    if let Some(db_error) = error.as_db_error() {
        let mut parts = vec![format!("database error: {}", db_error.message())];

        if let Some(detail) = db_error.detail() {
            parts.push(format!("  Detail: {}", detail));
        }

        if let Some(hint) = db_error.hint() {
            parts.push(format!("  Hint: {}", hint));
        }

        parts.push(format!("  Code: {:?}", db_error.code()));
        parts.join("\n")
    } else {
        format!("query error: {}", error)
    }
}

impl From<tokio_postgres::Error> for ConnectionError {
    fn from(error: tokio_postgres::Error) -> Self {
        ConnectionError::Query(error)
    }
}

impl From<mz_postgres_util::PostgresError> for ConnectionError {
    fn from(error: mz_postgres_util::PostgresError) -> Self {
        match error {
            mz_postgres_util::PostgresError::Postgres(error) => ConnectionError::Query(error),
            other => ConnectionError::Message(other.to_string()),
        }
    }
}

/// One table whose reference its source does not expose.
#[derive(Debug)]
pub struct MissingSourceReference {
    /// The table the project wants to create.
    pub table: ObjectId,
    /// The reference it asks for, as written in the project.
    pub reference: String,
    /// Exposed references spelled close enough to be the intended one, best
    /// first. Empty when nothing came close.
    pub suggestions: Vec<String>,
}

/// One source whose exposed references do not cover everything the project's
/// tables ask of it.
#[derive(Debug)]
pub struct SourceReferenceMismatch {
    /// The source the tables read from.
    pub source: ObjectId,
    /// The source's catalog ID, so the hint can name a query that lists every
    /// reference it exposes.
    pub source_id: String,
    /// Tables asking for a reference the source does not expose.
    pub tables: Vec<MissingSourceReference>,
    /// How many references the source does expose. A count well below what the
    /// upstream system holds points at the source's filters rather than a typo.
    pub available_count: usize,
    /// Why the source's references could not be refreshed, when they could not
    /// be. The counts and suggestions then come from whatever the catalog last
    /// recorded, which may be out of date.
    pub unreadable: Option<String>,
}

/// Errors that can occur during project validation against the database.
#[derive(Debug)]
pub enum DatabaseValidationError {
    /// One or more databases referenced by the project do not exist.
    MissingDatabases(Vec<String>),
    /// One or more schemas referenced by the project do not exist.
    MissingSchemas(Vec<SchemaQualifier>),
    /// One or more clusters referenced by the project do not exist.
    MissingClusters(Vec<String>),
    /// A single object failed to compile due to missing external dependencies.
    CompilationFailed {
        file_path: PathBuf,
        object_name: ObjectId,
        missing_dependencies: Vec<ObjectId>,
    },
    /// Aggregation of multiple validation failures detected in a single pass.
    Multiple {
        databases: Vec<String>,
        schemas: Vec<SchemaQualifier>,
        clusters: Vec<String>,
        compilation_errors: Vec<DatabaseValidationError>,
    },
    /// A cluster contains both compute objects (indexes, materialized views) and
    /// storage objects (sources, sinks), which is not supported.
    ClusterConflict {
        cluster_name: String,
        compute_objects: Vec<String>,
        storage_objects: Vec<String>,
    },
    /// The connected role lacks privileges required for deployment.
    InsufficientPrivileges {
        missing_database_usage: Vec<String>,
        missing_createcluster: bool,
    },
    /// The connected role does not own one or more production schemas it needs to manage.
    SchemaOwnershipMismatch {
        unowned_schemas: Vec<SchemaQualifier>,
        current_user: String,
    },
    /// The connected role does not own one or more production clusters it needs to manage.
    ClusterOwnershipMismatch {
        unowned_clusters: Vec<String>,
        current_user: String,
    },
    /// Sources referenced by the project do not exist in the database.
    MissingSources(Vec<ObjectId>),
    /// Connections referenced by the project do not exist in the database.
    MissingConnections(Vec<ObjectId>),
    /// Tables reference upstream objects their source does not expose.
    MissingSourceReferences(Vec<SourceReferenceMismatch>),
    /// Objects depend on tables that have not yet been created.
    MissingTableDependencies {
        objects_needing_tables: Vec<(ObjectId, Vec<ObjectId>)>,
    },
    /// A database query failed during validation.
    QueryError(ConnectionError),
}

impl fmt::Display for DatabaseValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabaseValidationError::MissingDatabases(dbs) => {
                write!(f, "Missing databases: {}", dbs.join(", "))
            }
            DatabaseValidationError::MissingSchemas(schemas) => {
                let schema_list: Vec<String> = schemas
                    .iter()
                    .map(|sq| format!("{}.{}", sq.database, sq.schema))
                    .collect();
                write!(f, "Missing schemas: {}", schema_list.join(", "))
            }
            DatabaseValidationError::MissingClusters(clusters) => {
                write!(f, "Missing clusters: {}", clusters.join(", "))
            }
            DatabaseValidationError::CompilationFailed {
                file_path,
                object_name,
                missing_dependencies,
            } => {
                let relative_path = format_relative_path(file_path);

                let error_style = Style::new().bright_red().bold();
                let arrow_style = Style::new().bright_blue().bold();
                writeln!(
                    f,
                    "{}: failed to compile '{}': missing external dependencies",
                    "error".if_supports_color(Stream::Stderr, |t| error_style.style(t)),
                    object_name
                )?;
                writeln!(
                    f,
                    " {} {}",
                    "-->".if_supports_color(Stream::Stderr, |t| arrow_style.style(t)),
                    relative_path
                )?;
                writeln!(f)?;
                writeln!(f, "  Missing dependencies:")?;
                for dep in missing_dependencies {
                    writeln!(f, "    - {}", dep)?;
                }
                Ok(())
            }
            DatabaseValidationError::Multiple {
                databases,
                schemas,
                clusters,
                compilation_errors,
            } => {
                let mut has_errors = false;

                writeln!(f, "Missing dependencies")?;
                if !databases.is_empty() {
                    writeln!(f, "Missing databases: {}", databases.join(", "))?;
                    has_errors = true;
                }

                if !schemas.is_empty() {
                    let schema_list: Vec<String> = schemas
                        .iter()
                        .map(|sq| format!("{}.{}", sq.database, sq.schema))
                        .collect();
                    writeln!(f, "Missing schemas: {}", schema_list.join(", "))?;
                    has_errors = true;
                }

                if !clusters.is_empty() {
                    writeln!(f, "Missing clusters: {}", clusters.join(", "))?;
                    has_errors = true;
                }

                if !compilation_errors.is_empty() {
                    if has_errors {
                        writeln!(f)?;
                    }
                    for (idx, err) in compilation_errors.iter().enumerate() {
                        if idx > 0 {
                            writeln!(f)?;
                        }
                        write!(f, "{}", err)?;
                    }
                }

                Ok(())
            }
            DatabaseValidationError::ClusterConflict {
                cluster_name,
                compute_objects,
                storage_objects,
            } => {
                let error_style = Style::new().bright_red().bold();
                writeln!(
                    f,
                    "{}: cluster '{}' contains both storage and computation objects",
                    "error".if_supports_color(Stream::Stderr, |t| error_style.style(t)),
                    cluster_name
                )?;
                writeln!(f)?;
                writeln!(f, "  Computation objects (indexes, materialized views):")?;
                for obj in compute_objects {
                    writeln!(f, "    - {}", obj)?;
                }
                writeln!(f)?;
                writeln!(f, "  Storage objects (sources, sinks):")?;
                for obj in storage_objects {
                    writeln!(f, "    - {}", obj)?;
                }
                writeln!(f)?;
                let help_style = Style::new().bright_cyan().bold();
                writeln!(
                    f,
                    "  {} Move sources/sinks to a separate cluster to avoid accidental recreation",
                    "help:".if_supports_color(Stream::Stderr, |t| help_style.style(t))
                )?;
                Ok(())
            }
            DatabaseValidationError::InsufficientPrivileges {
                missing_database_usage,
                missing_createcluster,
            } => {
                let error_style = Style::new().bright_red().bold();
                let help_style = Style::new().bright_cyan().bold();
                writeln!(
                    f,
                    "{}: insufficient privileges to deploy this project",
                    "error".if_supports_color(Stream::Stderr, |t| error_style.style(t))
                )?;
                writeln!(f)?;

                if !missing_database_usage.is_empty() {
                    writeln!(f, "  Missing USAGE privilege on databases:")?;
                    for db in missing_database_usage {
                        writeln!(f, "    - {}", db)?;
                    }
                    writeln!(f)?;
                }

                if *missing_createcluster {
                    writeln!(f, "  Missing CREATECLUSTER system privilege")?;
                    writeln!(f)?;
                }

                writeln!(
                    f,
                    "  {} Ask your administrator to grant the required privileges:",
                    "help:".if_supports_color(Stream::Stderr, |t| help_style.style(t))
                )?;
                writeln!(f)?;

                if !missing_database_usage.is_empty() {
                    for db in missing_database_usage {
                        writeln!(f, "    GRANT USAGE ON DATABASE {} TO <user>;", db)?;
                    }
                }

                if *missing_createcluster {
                    writeln!(f, "    GRANT CREATECLUSTER ON SYSTEM TO <user>;")?;
                }

                Ok(())
            }
            DatabaseValidationError::SchemaOwnershipMismatch {
                unowned_schemas,
                current_user,
            } => {
                let error_style = Style::new().bright_red().bold();
                let help_style = Style::new().bright_cyan().bold();
                writeln!(
                    f,
                    "{}: current role '{}' does not own the following production schemas",
                    "error".if_supports_color(Stream::Stderr, |t| error_style.style(t)),
                    current_user
                )?;
                writeln!(f)?;
                for sq in unowned_schemas {
                    writeln!(f, "    - {}.{}", sq.database, sq.schema)?;
                }
                writeln!(f)?;
                writeln!(
                    f,
                    "  {} Grant ownership of the schemas to the current role:",
                    "help:".if_supports_color(Stream::Stderr, |t| help_style.style(t))
                )?;
                writeln!(f)?;
                for sq in unowned_schemas {
                    writeln!(
                        f,
                        "    ALTER SCHEMA {}.{} OWNER TO {};",
                        sq.database, sq.schema, current_user
                    )?;
                }
                Ok(())
            }
            DatabaseValidationError::ClusterOwnershipMismatch {
                unowned_clusters,
                current_user,
            } => {
                let error_style = Style::new().bright_red().bold();
                let help_style = Style::new().bright_cyan().bold();
                writeln!(
                    f,
                    "{}: current role '{}' does not own the following production clusters",
                    "error".if_supports_color(Stream::Stderr, |t| error_style.style(t)),
                    current_user
                )?;
                writeln!(f)?;
                for cluster in unowned_clusters {
                    writeln!(f, "    - {}", cluster)?;
                }
                writeln!(f)?;
                writeln!(
                    f,
                    "  {} Grant ownership of the clusters to the current role:",
                    "help:".if_supports_color(Stream::Stderr, |t| help_style.style(t))
                )?;
                writeln!(f)?;
                for cluster in unowned_clusters {
                    writeln!(
                        f,
                        "    ALTER CLUSTER {} OWNER TO {};",
                        cluster, current_user
                    )?;
                }
                Ok(())
            }
            DatabaseValidationError::MissingSources(sources) => {
                let error_style = Style::new().bright_red().bold();
                writeln!(
                    f,
                    "{}: The following sources are referenced but do not exist:",
                    "error".if_supports_color(Stream::Stderr, |t| error_style.style(t))
                )?;
                for source in sources {
                    writeln!(f, "  - {}", source)?;
                }
                writeln!(f)?;
                writeln!(
                    f,
                    "Please ensure all sources are created before running this command."
                )?;
                Ok(())
            }
            DatabaseValidationError::MissingConnections(connections) => {
                let error_style = Style::new().bright_red().bold();
                let help_style = Style::new().bright_cyan().bold();
                writeln!(
                    f,
                    "{}: The following connections are referenced but do not exist:",
                    "error".if_supports_color(Stream::Stderr, |t| error_style.style(t))
                )?;
                for conn in connections {
                    writeln!(f, "  - {}", conn)?;
                }
                writeln!(f)?;
                writeln!(
                    f,
                    "{} Connections are not managed by mz-deploy and must be created separately.",
                    "help:".if_supports_color(Stream::Stderr, |t| help_style.style(t))
                )?;
                Ok(())
            }
            DatabaseValidationError::MissingSourceReferences(mismatches) => {
                let help_style = Style::new().bright_cyan().bold();
                writeln!(
                    f,
                    "The following tables reference upstream objects their source does not expose:"
                )?;
                for mismatch in mismatches {
                    writeln!(f)?;
                    writeln!(f, "  from {}:", mismatch.source)?;
                    for table in &mismatch.tables {
                        writeln!(f, "    - {} ({})", table.table, table.reference)?;
                        if !table.suggestions.is_empty() {
                            writeln!(f, "      did you mean: {}?", table.suggestions.join(", "))?;
                        }
                    }
                    if let Some(reason) = &mismatch.unreadable {
                        writeln!(f)?;
                        writeln!(
                            f,
                            "    could not read the references for {}: {}",
                            mismatch.source, reason
                        )?;
                    }
                    if mismatch.available_count > 0 {
                        writeln!(f)?;
                        writeln!(
                            f,
                            "    {} exposes {} references. To see them all:",
                            mismatch.source, mismatch.available_count
                        )?;
                        writeln!(
                            f,
                            "      SELECT namespace, name FROM mz_internal.mz_source_references"
                        )?;
                        writeln!(f, "      WHERE source_id = '{}';", mismatch.source_id)?;
                    }
                }
                writeln!(f)?;
                writeln!(
                    f,
                    "{} Confirm the object exists upstream and that the source's publication,",
                    "help:".if_supports_color(Stream::Stderr, |t| help_style.style(t))
                )?;
                writeln!(f, "      schema filter, and credentials include it.")?;
                Ok(())
            }
            DatabaseValidationError::MissingTableDependencies {
                objects_needing_tables,
            } => {
                let help_style = Style::new().bright_cyan().bold();
                writeln!(
                    f,
                    "Objects depend on tables that don't exist in the database",
                )?;
                writeln!(f)?;
                for (object, missing_tables) in objects_needing_tables {
                    writeln!(
                        f,
                        "  {} {} depends on:",
                        "×".if_supports_color(Stream::Stderr, |t| t.bright_red()),
                        object
                    )?;
                    for table in missing_tables {
                        writeln!(f, "    - {}", table)?;
                    }
                }
                writeln!(f)?;
                writeln!(
                    f,
                    "{} Run 'mz-deploy apply' to create the required tables first",
                    "help:".if_supports_color(Stream::Stderr, |t| help_style.style(t))
                )?;
                Ok(())
            }
            DatabaseValidationError::QueryError(e) => {
                write!(f, "Database query failed: {}", e)
            }
        }
    }
}

impl std::error::Error for DatabaseValidationError {}

/// Extract last 3 path components for display (database/schema/file.sql).
///
/// This helper is used in error formatting to show relative paths
/// that are more readable than full absolute paths.
pub fn format_relative_path(path: &std::path::Path) -> String {
    let path_components: Vec<_> = path.components().collect();
    let len = path_components.len();
    if len >= 3 {
        format!(
            "{}/{}/{}",
            path_components[len - 3].as_os_str().to_string_lossy(),
            path_components[len - 2].as_os_str().to_string_lossy(),
            path_components[len - 1].as_os_str().to_string_lossy()
        )
    } else {
        path.display().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn object(schema: &str, object: &str) -> ObjectId {
        ObjectId::new("app".to_string(), schema.to_string(), object.to_string())
    }

    #[mz_ore::test]
    fn test_missing_source_references_error_display() {
        let error =
            DatabaseValidationError::MissingSourceReferences(vec![SourceReferenceMismatch {
                source: object("ingest", "pg_source"),
                source_id: "u1043".to_string(),
                tables: vec![MissingSourceReference {
                    table: object("ingest", "widgets"),
                    reference: "public.widgest".to_string(),
                    suggestions: vec!["public.widgets".to_string()],
                }],
                available_count: 1284,
                unreadable: None,
            }]);
        let output = error.to_string();

        assert!(
            output.contains("app.ingest.widgets (public.widgest)"),
            "{output}"
        );
        assert!(output.contains("from app.ingest.pg_source:"), "{output}");
        assert!(output.contains("did you mean: public.widgets?"), "{output}");
        assert!(
            output.contains("app.ingest.pg_source exposes 1284 references"),
            "{output}"
        );
        assert!(output.contains("WHERE source_id = 'u1043';"), "{output}");
        assert!(!output.contains("could not read"), "{output}");
    }

    #[mz_ore::test]
    fn test_missing_source_references_error_display_multiple_suggestions() {
        let error =
            DatabaseValidationError::MissingSourceReferences(vec![SourceReferenceMismatch {
                source: object("ingest", "pg_source"),
                source_id: "u1043".to_string(),
                tables: vec![MissingSourceReference {
                    table: object("ingest", "widgets"),
                    reference: "sales.widgets".to_string(),
                    suggestions: vec!["public.widgets".to_string(), "staging.widgets".to_string()],
                }],
                available_count: 2,
                unreadable: None,
            }]);
        let output = error.to_string();

        assert!(
            output.contains("did you mean: public.widgets, staging.widgets?"),
            "{output}"
        );
    }

    #[mz_ore::test]
    fn test_missing_source_references_error_display_without_suggestions() {
        let error =
            DatabaseValidationError::MissingSourceReferences(vec![SourceReferenceMismatch {
                source: object("ingest", "pg_source"),
                source_id: "u1043".to_string(),
                tables: vec![MissingSourceReference {
                    table: object("ingest", "widgets"),
                    reference: "public.widgets".to_string(),
                    suggestions: Vec::new(),
                }],
                available_count: 3,
                unreadable: None,
            }]);
        let output = error.to_string();

        // Nothing came close, so the query and the upstream advice are all the
        // error can offer.
        assert!(!output.contains("did you mean"), "{output}");
        assert!(
            output.contains("app.ingest.pg_source exposes 3 references"),
            "{output}"
        );
        assert!(
            output.contains("Confirm the object exists upstream"),
            "{output}"
        );
    }

    #[mz_ore::test]
    fn test_missing_source_references_error_display_unreadable() {
        let error =
            DatabaseValidationError::MissingSourceReferences(vec![SourceReferenceMismatch {
                source: object("ingest", "pg_source"),
                source_id: "u1043".to_string(),
                tables: vec![MissingSourceReference {
                    table: object("ingest", "widgets"),
                    reference: "public.widgets".to_string(),
                    suggestions: Vec::new(),
                }],
                available_count: 12,
                unreadable: Some("permission denied".to_string()),
            }]);
        let output = error.to_string();

        assert!(
            output.contains(
                "could not read the references for app.ingest.pg_source: permission denied"
            ),
            "{output}"
        );
    }

    #[mz_ore::test]
    fn test_missing_table_dependencies_error_display() {
        let error = DatabaseValidationError::MissingTableDependencies {
            objects_needing_tables: vec![
                (
                    ObjectId::new(
                        "materialize".to_string(),
                        "public".to_string(),
                        "my_view".to_string(),
                    ),
                    vec![
                        ObjectId::new(
                            "materialize".to_string(),
                            "tables".to_string(),
                            "users".to_string(),
                        ),
                        ObjectId::new(
                            "materialize".to_string(),
                            "tables".to_string(),
                            "orders".to_string(),
                        ),
                    ],
                ),
                (
                    ObjectId::new(
                        "materialize".to_string(),
                        "public".to_string(),
                        "another_view".to_string(),
                    ),
                    vec![ObjectId::new(
                        "materialize".to_string(),
                        "tables".to_string(),
                        "products".to_string(),
                    )],
                ),
            ],
        };

        let error_string = format!("{}", error);

        // Check that error message contains key elements
        assert!(error_string.contains("Objects depend on tables that don't exist"));
        assert!(error_string.contains("materialize.public.my_view"));
        assert!(error_string.contains("materialize.tables.users"));
        assert!(error_string.contains("materialize.tables.orders"));
        assert!(error_string.contains("materialize.public.another_view"));
        assert!(error_string.contains("materialize.tables.products"));
        assert!(error_string.contains("help"));
        assert!(error_string.contains("mz-deploy apply"));
    }

    #[mz_ore::test]
    fn test_format_relative_path() {
        let path = PathBuf::from("/home/user/project/database/schema/file.sql");
        assert_eq!(format_relative_path(&path), "database/schema/file.sql");

        let short_path = PathBuf::from("file.sql");
        assert_eq!(format_relative_path(&short_path), "file.sql");
    }

    #[mz_ore::test]
    fn test_format_relative_path_exactly_three_components() {
        let path = PathBuf::from("database/schema/file.sql");
        assert_eq!(format_relative_path(&path), "database/schema/file.sql");
    }

    #[mz_ore::test]
    fn test_format_relative_path_two_components() {
        let path = PathBuf::from("schema/file.sql");
        assert_eq!(format_relative_path(&path), "schema/file.sql");
    }

    #[mz_ore::test]
    fn test_missing_databases_error_display() {
        let error =
            DatabaseValidationError::MissingDatabases(vec!["db1".to_string(), "db2".to_string()]);
        let error_string = format!("{}", error);
        assert!(error_string.contains("Missing databases"));
        assert!(error_string.contains("db1"));
        assert!(error_string.contains("db2"));
    }

    #[mz_ore::test]
    fn test_missing_schemas_error_display() {
        let error = DatabaseValidationError::MissingSchemas(vec![
            SchemaQualifier::new("db1".to_string(), "schema1".to_string()),
            SchemaQualifier::new("db2".to_string(), "schema2".to_string()),
        ]);
        let error_string = format!("{}", error);
        assert!(error_string.contains("Missing schemas"));
        assert!(error_string.contains("db1.schema1"));
        assert!(error_string.contains("db2.schema2"));
    }

    #[mz_ore::test]
    fn test_missing_clusters_error_display() {
        let error = DatabaseValidationError::MissingClusters(vec![
            "cluster1".to_string(),
            "cluster2".to_string(),
        ]);
        let error_string = format!("{}", error);
        assert!(error_string.contains("Missing clusters"));
        assert!(error_string.contains("cluster1"));
        assert!(error_string.contains("cluster2"));
    }

    #[mz_ore::test]
    fn test_cluster_conflict_error_display() {
        let error = DatabaseValidationError::ClusterConflict {
            cluster_name: "shared_cluster".to_string(),
            compute_objects: vec!["my_index".to_string(), "my_mv".to_string()],
            storage_objects: vec!["my_source".to_string()],
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("shared_cluster"));
        assert!(error_string.contains("storage and computation objects"));
        assert!(error_string.contains("my_index"));
        assert!(error_string.contains("my_mv"));
        assert!(error_string.contains("my_source"));
        assert!(error_string.contains("help"));
    }

    #[mz_ore::test]
    fn test_insufficient_privileges_error_display() {
        let error = DatabaseValidationError::InsufficientPrivileges {
            missing_database_usage: vec!["db1".to_string(), "db2".to_string()],
            missing_createcluster: true,
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("insufficient privileges"));
        assert!(error_string.contains("db1"));
        assert!(error_string.contains("db2"));
        assert!(error_string.contains("CREATECLUSTER"));
        assert!(error_string.contains("GRANT"));
    }

    #[mz_ore::test]
    fn test_insufficient_privileges_only_database() {
        let error = DatabaseValidationError::InsufficientPrivileges {
            missing_database_usage: vec!["db1".to_string()],
            missing_createcluster: false,
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("db1"));
        assert!(!error_string.contains("CREATECLUSTER ON SYSTEM"));
    }

    #[mz_ore::test]
    fn test_missing_sources_error_display() {
        let error = DatabaseValidationError::MissingSources(vec![ObjectId::new(
            "materialize".to_string(),
            "public".to_string(),
            "kafka_source".to_string(),
        )]);
        let error_string = format!("{}", error);
        assert!(error_string.contains("sources are referenced but do not exist"));
        assert!(error_string.contains("materialize.public.kafka_source"));
    }

    #[mz_ore::test]
    fn test_multiple_validation_errors_display() {
        let error = DatabaseValidationError::Multiple {
            databases: vec!["missing_db".to_string()],
            schemas: vec![SchemaQualifier::new(
                "db".to_string(),
                "missing_schema".to_string(),
            )],
            clusters: vec!["missing_cluster".to_string()],
            compilation_errors: vec![],
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("missing_db"));
        assert!(error_string.contains("db.missing_schema"));
        assert!(error_string.contains("missing_cluster"));
    }

    #[mz_ore::test]
    fn test_connection_error_display() {
        let error = ConnectionError::Message("test error message".to_string());
        let error_string = format!("{}", error);
        assert_eq!(error_string, "test error message");
    }

    #[mz_ore::test]
    fn test_connection_error_cluster_not_found() {
        let error = ConnectionError::ClusterNotFound {
            name: "missing_cluster".to_string(),
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("missing_cluster"));
        assert!(error_string.contains("not found"));
    }

    #[mz_ore::test]
    fn test_connection_error_deployment_already_exists() {
        let error = ConnectionError::DeploymentAlreadyExists {
            deploy_id: "staging_123".to_string(),
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("staging_123"));
        assert!(error_string.contains("already exists"));
    }

    #[mz_ore::test]
    fn test_connection_error_deployment_not_found() {
        let error = ConnectionError::DeploymentNotFound {
            deploy_id: "nonexistent".to_string(),
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("nonexistent"));
        assert!(error_string.contains("not found"));
    }

    #[mz_ore::test]
    fn test_connection_error_deployment_already_promoted() {
        let error = ConnectionError::DeploymentAlreadyPromoted {
            deploy_id: "prod_deploy".to_string(),
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("prod_deploy"));
        assert!(error_string.contains("already been promoted"));
    }

    #[mz_ore::test]
    fn test_database_validation_error_is_error_trait() {
        // Verify that DatabaseValidationError implements std::error::Error
        let error: Box<dyn std::error::Error> =
            Box::new(DatabaseValidationError::MissingDatabases(vec![]));
        assert!(error.to_string().contains("Missing databases"));
    }
}
