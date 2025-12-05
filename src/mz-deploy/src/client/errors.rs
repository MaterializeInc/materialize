//! Error types for the client module.
//!
//! This module contains all error types used by the database client,
//! including connection errors and validation errors.

use crate::client::config::ConfigError;
use crate::project::object_id::ObjectId;
use owo_colors::OwoColorize;
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

    #[error("{}", format_query_error(.0))]
    Query(tokio_postgres::Error),

    #[error("dependency error: {0}")]
    Dependency(#[from] crate::project::error::DependencyError),

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

/// Errors that can occur during project validation against the database.
#[derive(Debug)]
pub enum DatabaseValidationError {
    MissingDatabases(Vec<String>),
    MissingSchemas(Vec<(String, String)>),
    MissingClusters(Vec<String>),
    CompilationFailed {
        file_path: PathBuf,
        object_name: ObjectId,
        missing_dependencies: Vec<ObjectId>,
    },
    Multiple {
        databases: Vec<String>,
        schemas: Vec<(String, String)>,
        clusters: Vec<String>,
        compilation_errors: Vec<DatabaseValidationError>,
    },
    ClusterConflict {
        cluster_name: String,
        compute_objects: Vec<String>,
        storage_objects: Vec<String>,
    },
    InsufficientPrivileges {
        missing_database_usage: Vec<String>,
        missing_createcluster: bool,
    },
    MissingSources(Vec<ObjectId>),
    MissingTableDependencies {
        objects_needing_tables: Vec<(ObjectId, Vec<ObjectId>)>,
    },
    QueryError(tokio_postgres::Error),
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
                    .map(|(db, schema)| format!("{}.{}", db, schema))
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

                writeln!(
                    f,
                    "{}: failed to compile '{}': missing external dependencies",
                    "error".bright_red().bold(),
                    object_name
                )?;
                writeln!(f, " {} {}", "-->".bright_blue().bold(), relative_path)?;
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
                        .map(|(db, schema)| format!("{}.{}", db, schema))
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
                writeln!(
                    f,
                    "{}: cluster '{}' contains both storage and computation objects",
                    "error".bright_red().bold(),
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
                writeln!(
                    f,
                    "  {} Move sources/sinks to a separate cluster to avoid accidental recreation",
                    "help:".bright_cyan().bold()
                )?;
                Ok(())
            }
            DatabaseValidationError::InsufficientPrivileges {
                missing_database_usage,
                missing_createcluster,
            } => {
                writeln!(
                    f,
                    "{}: insufficient privileges to deploy this project",
                    "error".bright_red().bold()
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
                    "help:".bright_cyan().bold()
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
            DatabaseValidationError::MissingSources(sources) => {
                writeln!(
                    f,
                    "{}: The following sources are referenced but do not exist:",
                    "error".bright_red().bold()
                )?;
                for source in sources {
                    writeln!(
                        f,
                        "  - {}.{}.{}",
                        source.database, source.schema, source.object
                    )?;
                }
                writeln!(f)?;
                writeln!(
                    f,
                    "Please ensure all sources are created before running this command."
                )?;
                Ok(())
            }
            DatabaseValidationError::MissingTableDependencies {
                objects_needing_tables,
            } => {
                writeln!(
                    f,
                    "{}: Objects depend on tables that don't exist in the database",
                    "error".bright_red().bold()
                )?;
                writeln!(f)?;
                for (object, missing_tables) in objects_needing_tables {
                    writeln!(
                        f,
                        "  {} {}.{}.{} depends on:",
                        "×".bright_red(),
                        object.database,
                        object.schema,
                        object.object
                    )?;
                    for table in missing_tables {
                        writeln!(
                            f,
                            "    - {}.{}.{}",
                            table.database, table.schema, table.object
                        )?;
                    }
                }
                writeln!(f)?;
                writeln!(
                    f,
                    "{} Run 'mz-deploy create-tables' to create the required tables first",
                    "help:".bright_cyan().bold()
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

    #[test]
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
        assert!(error_string.contains("error"));
        assert!(error_string.contains("Objects depend on tables that don't exist"));
        assert!(error_string.contains("materialize.public.my_view"));
        assert!(error_string.contains("materialize.tables.users"));
        assert!(error_string.contains("materialize.tables.orders"));
        assert!(error_string.contains("materialize.public.another_view"));
        assert!(error_string.contains("materialize.tables.products"));
        assert!(error_string.contains("help"));
        assert!(error_string.contains("mz-deploy create-tables"));
    }

    #[test]
    fn test_format_relative_path() {
        let path = PathBuf::from("/home/user/project/database/schema/file.sql");
        assert_eq!(format_relative_path(&path), "database/schema/file.sql");

        let short_path = PathBuf::from("file.sql");
        assert_eq!(format_relative_path(&short_path), "file.sql");
    }

    #[test]
    fn test_format_relative_path_exactly_three_components() {
        let path = PathBuf::from("database/schema/file.sql");
        assert_eq!(format_relative_path(&path), "database/schema/file.sql");
    }

    #[test]
    fn test_format_relative_path_two_components() {
        let path = PathBuf::from("schema/file.sql");
        assert_eq!(format_relative_path(&path), "schema/file.sql");
    }

    #[test]
    fn test_missing_databases_error_display() {
        let error = DatabaseValidationError::MissingDatabases(vec![
            "db1".to_string(),
            "db2".to_string(),
        ]);
        let error_string = format!("{}", error);
        assert!(error_string.contains("Missing databases"));
        assert!(error_string.contains("db1"));
        assert!(error_string.contains("db2"));
    }

    #[test]
    fn test_missing_schemas_error_display() {
        let error = DatabaseValidationError::MissingSchemas(vec![
            ("db1".to_string(), "schema1".to_string()),
            ("db2".to_string(), "schema2".to_string()),
        ]);
        let error_string = format!("{}", error);
        assert!(error_string.contains("Missing schemas"));
        assert!(error_string.contains("db1.schema1"));
        assert!(error_string.contains("db2.schema2"));
    }

    #[test]
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

    #[test]
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

    #[test]
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

    #[test]
    fn test_insufficient_privileges_only_database() {
        let error = DatabaseValidationError::InsufficientPrivileges {
            missing_database_usage: vec!["db1".to_string()],
            missing_createcluster: false,
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("db1"));
        assert!(!error_string.contains("CREATECLUSTER ON SYSTEM"));
    }

    #[test]
    fn test_missing_sources_error_display() {
        let error = DatabaseValidationError::MissingSources(vec![
            ObjectId::new(
                "materialize".to_string(),
                "public".to_string(),
                "kafka_source".to_string(),
            ),
        ]);
        let error_string = format!("{}", error);
        assert!(error_string.contains("sources are referenced but do not exist"));
        assert!(error_string.contains("materialize.public.kafka_source"));
    }

    #[test]
    fn test_multiple_validation_errors_display() {
        let error = DatabaseValidationError::Multiple {
            databases: vec!["missing_db".to_string()],
            schemas: vec![("db".to_string(), "missing_schema".to_string())],
            clusters: vec!["missing_cluster".to_string()],
            compilation_errors: vec![],
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("missing_db"));
        assert!(error_string.contains("db.missing_schema"));
        assert!(error_string.contains("missing_cluster"));
    }

    #[test]
    fn test_connection_error_display() {
        let error = ConnectionError::Message("test error message".to_string());
        let error_string = format!("{}", error);
        assert_eq!(error_string, "test error message");
    }

    #[test]
    fn test_connection_error_cluster_not_found() {
        let error = ConnectionError::ClusterNotFound {
            name: "missing_cluster".to_string(),
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("missing_cluster"));
        assert!(error_string.contains("not found"));
    }

    #[test]
    fn test_connection_error_deployment_already_exists() {
        let error = ConnectionError::DeploymentAlreadyExists {
            deploy_id: "staging_123".to_string(),
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("staging_123"));
        assert!(error_string.contains("already exists"));
    }

    #[test]
    fn test_connection_error_deployment_not_found() {
        let error = ConnectionError::DeploymentNotFound {
            deploy_id: "nonexistent".to_string(),
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("nonexistent"));
        assert!(error_string.contains("not found"));
    }

    #[test]
    fn test_connection_error_deployment_already_promoted() {
        let error = ConnectionError::DeploymentAlreadyPromoted {
            deploy_id: "prod_deploy".to_string(),
        };
        let error_string = format!("{}", error);
        assert!(error_string.contains("prod_deploy"));
        assert!(error_string.contains("already been promoted"));
    }

    #[test]
    fn test_database_validation_error_is_error_trait() {
        // Verify that DatabaseValidationError implements std::error::Error
        let error: Box<dyn std::error::Error> =
            Box::new(DatabaseValidationError::MissingDatabases(vec![]));
        assert!(error.to_string().contains("Missing databases"));
    }
}
