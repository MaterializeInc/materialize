use crate::client::config::{ConfigError, Profile, ProfilesConfig};
use crate::client::models::{
    Cluster, ClusterOptions, ConflictRecord, DeploymentMetadata, DeploymentObjectRecord,
    SchemaDeploymentRecord,
};
use crate::project::deployment_snapshot::DeploymentSnapshot;
use crate::project::object_id::ObjectId;
use crate::project::planned;
use crate::types::{ColumnType, Types};
use crate::utils::sql_utils::quote_identifier;
use owo_colors::OwoColorize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client as PgClient, NoTls, Row, ToStatement};

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
    #[error("deployment '{environment}' already exists")]
    DeploymentAlreadyExists { environment: String },
    #[error("deployment '{environment}' not found")]
    DeploymentNotFound { environment: String },
    #[error("deployment '{environment}' has already been promoted to production")]
    DeploymentAlreadyPromoted { environment: String },
    #[error("unsupported statement type: {0}")]
    UnsupportedStatementType(String),
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
                // Extract last 3 path components for display (database/schema/file.sql)
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

                // Format like rustc errors with colors
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
            DatabaseValidationError::QueryError(e) => {
                write!(f, "Database query failed: {}", e)
            }
        }
    }
}

impl std::error::Error for DatabaseValidationError {}

pub struct Client {
    client: PgClient,
    profile: Profile,
}

impl Client {
    /// Connect to the database using a named profile
    ///
    /// Note: This method searches for profiles.toml in the current working directory.
    /// For project-specific configuration, use ProfilesConfig::load_profile() with
    /// a project directory and then connect_with_profile().
    pub async fn connect(profile_name: Option<&str>) -> Result<Self, ConnectionError> {
        // Load profiles configuration (searches in CWD for backwards compatibility)
        let config = ProfilesConfig::load(None)?;

        // Get the requested profile or default
        let profile = if let Some(name) = profile_name {
            config.get_profile(name)?
        } else {
            config.get_default_profile()?
        };

        // Expand environment variables
        let profile = config.expand_env_vars(profile)?;

        // Connect to the database
        Self::connect_with_profile(profile).await
    }

    /// Connect to the database using a Profile directly
    pub async fn connect_with_profile(profile: Profile) -> Result<Self, ConnectionError> {
        // Build connection string
        let mut conn_str = format!("host={} port={}", profile.host, profile.port);

        if let Some(ref username) = profile.username {
            conn_str.push_str(&format!(" user={}", username));
        }

        if let Some(ref password) = profile.password {
            conn_str.push_str(&format!(" password={}", password));
        }

        // Connect to the database
        let (client, connection) =
            tokio_postgres::connect(&conn_str, NoTls)
                .await
                .map_err(|source| ConnectionError::Connect {
                    host: profile.host.clone(),
                    port: profile.port,
                    source,
                })?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Client { client, profile })
    }

    /// Execute the connection info query and log the results
    pub async fn log_connection_info(&self) -> Result<(), ConnectionError> {
        let query = "SELECT mz_version() AS version, mz_environment_id() AS environment_id, current_role() as role";

        let row = self.client.query_one(query, &[]).await?;

        let version: String = row.get("version");
        let environment_id: String = row.get("environment_id");
        let role: String = row.get("role");

        println!("Connected to Materialize:");
        println!("  Host: {}:{}", self.profile.host, self.profile.port);
        println!("  Version: {}", version);
        println!("  Environment ID: {}", environment_id);
        println!("  Role: {}", role);

        Ok(())
    }

    /// Get the profile used for this connection
    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    /// Get a reference to the underlying tokio-postgres client.
    pub fn postgres_client(&self) -> &PgClient {
        &self.client
    }

    /// Execute a SQL statement that doesn't return rows.
    pub async fn execute<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .execute(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Execute a SQL query and return the resulting rows.
    pub async fn query<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, ConnectionError>
    where
        T: ?Sized + ToStatement,
    {
        self.client
            .query(statement, params)
            .await
            .map_err(ConnectionError::Query)
    }

    /// Query SHOW COLUMNS for all external dependencies and return their schemas as a Types object
    pub async fn query_external_types(
        &mut self,
        project: &planned::Project,
    ) -> Result<Types, ConnectionError> {
        let mut objects = BTreeMap::new();
        for oid in &project.external_dependencies {
            let quoted_db = quote_identifier(&oid.database);
            let quoted_schema = quote_identifier(&oid.schema);
            let quoted_object = quote_identifier(&oid.object);

            let rows = self
                .client
                .query(
                    &format!(
                        "SHOW COLUMNS FROM {}.{}.{}",
                        quoted_db, quoted_schema, quoted_object
                    ),
                    &[],
                )
                .await?;

            let mut columns = BTreeMap::new();
            for row in rows {
                let name: String = row.get("name");
                let type_str: String = row.get("type");
                let nullable: bool = row.get("nullable");

                let column_type = ColumnType {
                    r#type: type_str,
                    nullable,
                };

                columns.insert(name, column_type);
            }

            objects.insert(oid.to_string(), columns);
        }

        Ok(Types {
            version: 1,
            objects,
        })
    }

    /// Create a schema in the specified database (idempotent).
    pub async fn create_schema(&self, database: &str, schema: &str) -> Result<(), ConnectionError> {
        let sql = format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{}",
            quote_identifier(database),
            quote_identifier(schema)
        );

        self.client.execute(&sql, &[]).await.map_err(|e| {
            ConnectionError::SchemaCreationFailed {
                database: database.to_string(),
                schema: schema.to_string(),
                source: Box::new(e),
            }
        })?;

        Ok(())
    }

    /// Create a cluster with the specified configuration.
    pub async fn create_cluster(
        &self,
        name: &str,
        options: ClusterOptions,
    ) -> Result<(), ConnectionError> {
        let sql = format!(
            "CREATE CLUSTER {} (SIZE = '{}', REPLICATION FACTOR = {})",
            quote_identifier(name),
            options.size,
            options.replication_factor
        );

        self.client.execute(&sql, &[]).await.map_err(|e| {
            if e.to_string().contains("already exists") {
                ConnectionError::ClusterAlreadyExists {
                    name: name.to_string(),
                }
            } else {
                ConnectionError::ClusterCreationFailed {
                    name: name.to_string(),
                    source: Box::new(e),
                }
            }
        })?;

        Ok(())
    }

    /// Create the deployment tracking schemas/tables for staging deployments.
    pub async fn create_deployments(&self) -> Result<(), ConnectionError> {
        self.execute("CREATE SCHEMA IF NOT EXISTS deploy;", &[])
            .await?;

        self.execute(
            r#"CREATE TABLE IF NOT EXISTS deploy.deployments (
                environment TEXT NOT NULL,
                deployed_at TIMESTAMP NOT NULL,
                promoted_at TIMESTAMP,
                database    TEXT NOT NULL,
                schema      TEXT NOT NULL,
                deployed_by TEXT NOT NULL
            ) WITH (
                PARTITION BY (environment, deployed_at, promoted_at)
            );"#,
            &[],
        )
        .await?;

        self.execute(
            r#"CREATE TABLE IF NOT EXISTS deploy.objects (
                environment TEXT NOT NULL,
                database TEXT NOT NULL,
                schema   TEXT NOT NULL,
                object   TEXT NOT NULL,
                hash     TEXT NOT NULL
            ) WITH (
                PARTITION BY (environment, database, schema)
            );"#,
            &[],
        )
        .await?;

        self.execute(
            r#"CREATE TABLE IF NOT EXISTS deploy.clusters (
                environment TEXT NOT NULL,
                name TEXT NOT NULL
            ) WITH (
                PARTITION BY (environment)
            );"#,
            &[],
        )
        .await?;

        self.execute(
            r#"
            CREATE VIEW IF NOT EXISTS deploy.production AS
            WITH candidates AS (
                SELECT DISTINCT ON (database, schema) database, schema, environment, promoted_at
                FROM deploy.deployments
                WHERE promoted_at IS NOT NULL
                ORDER BY database, schema, promoted_at DESC
            )

            SELECT c.database, c.schema, c.environment, c.promoted_at
            FROM candidates c
            JOIN mz_schemas s ON c.schema = s.name
            JOIN mz_databases d ON c.database = d.name;
        "#,
            &[],
        )
        .await?;

        Ok(())
    }

    /// Insert schema deployment records (insert-only, no DELETE).
    pub async fn insert_schema_deployments(
        &self,
        deployments: &[SchemaDeploymentRecord],
    ) -> Result<(), ConnectionError> {
        if deployments.is_empty() {
            return Ok(());
        }

        let insert_sql = r#"
            INSERT INTO deploy.deployments
                (environment, database, schema, deployed_at, deployed_by, promoted_at)
            VALUES
                ($1, $2, $3, $4, $5, $6)
        "#;

        for deployment in deployments {
            self.execute(
                insert_sql,
                &[
                    &deployment.environment,
                    &deployment.database,
                    &deployment.schema,
                    &deployment.deployed_at,
                    &deployment.deployed_by,
                    &deployment.promoted_at,
                ],
            )
            .await?;
        }

        Ok(())
    }

    /// Append deployment object records (insert-only, never update or delete).
    pub async fn append_deployment_objects(
        &self,
        objects: &[DeploymentObjectRecord],
    ) -> Result<(), ConnectionError> {
        if objects.is_empty() {
            return Ok(());
        }

        let insert_sql = r#"
            INSERT INTO deploy.objects
                (environment, database, schema, object, hash)
            VALUES
                ($1, $2, $3, $4, $5)
        "#;

        for obj in objects {
            self.execute(
                insert_sql,
                &[
                    &obj.environment,
                    &obj.database,
                    &obj.schema,
                    &obj.object,
                    &obj.object_hash,
                ],
            )
            .await?;
        }

        Ok(())
    }

    /// Insert cluster records for a staging deployment.
    pub async fn insert_deployment_clusters(
        &self,
        environment: &str,
        clusters: &[String],
    ) -> Result<(), ConnectionError> {
        if clusters.is_empty() {
            return Ok(());
        }

        let insert_sql = r#"
            INSERT INTO deploy.clusters
                (environment, name)
            VALUES
                ($1, $2)
        "#;

        for cluster in clusters {
            self.execute(insert_sql, &[&environment, &cluster]).await?;
        }

        Ok(())
    }

    /// Get cluster names for a staging deployment.
    pub async fn get_deployment_clusters(
        &self,
        environment: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        let rows = self
            .client
            .query(
                "SELECT name FROM deploy.clusters WHERE environment = $1",
                &[&environment],
            )
            .await
            .map_err(ConnectionError::Query)?;

        Ok(rows.iter().map(|row| row.get("name")).collect())
    }

    /// Delete cluster records for a staging deployment.
    pub async fn delete_deployment_clusters(
        &self,
        environment: &str,
    ) -> Result<(), ConnectionError> {
        self.execute(
            "DELETE FROM deploy.clusters WHERE environment = $1",
            &[&environment],
        )
        .await?;
        Ok(())
    }

    /// Update promoted_at timestamp for a staging environment.
    pub async fn update_promoted_at(&self, environment: &str) -> Result<(), ConnectionError> {
        let update_sql = r#"
            UPDATE deploy.deployments
            SET promoted_at = NOW()
            WHERE environment = $1
        "#;

        self.execute(update_sql, &[&environment]).await?;
        Ok(())
    }

    /// Delete all deployment records for a specific environment.
    pub async fn delete_deployment(&self, environment: &str) -> Result<(), ConnectionError> {
        self.execute(
            "DELETE FROM deploy.deployments WHERE environment = $1",
            &[&environment],
        )
        .await?;
        self.execute(
            "DELETE FROM deploy.objects WHERE environment = $1",
            &[&environment],
        )
        .await?;
        Ok(())
    }

    /// Check if a schema exists in the specified database.
    pub async fn schema_exists(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<bool, ConnectionError> {
        let query = r#"
            SELECT EXISTS(
                SELECT 1
                FROM mz_catalog.mz_schemas s
                JOIN mz_catalog.mz_databases d ON s.database_id = d.id
                WHERE s.name = $1 AND d.name = $2
            ) AS exists
        "#;

        let row = self
            .client
            .query_one(query, &[&schema, &database])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(row.get("exists"))
    }

    /// Check if a cluster exists.
    pub async fn cluster_exists(&self, name: &str) -> Result<bool, ConnectionError> {
        let query = r#"
            SELECT EXISTS(
                SELECT 1 FROM mz_catalog.mz_clusters WHERE name = $1
            ) AS exists
        "#;

        let row = self
            .client
            .query_one(query, &[&name])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(row.get("exists"))
    }

    /// Get a cluster by name.
    pub async fn get_cluster(&self, name: &str) -> Result<Option<Cluster>, ConnectionError> {
        let query = r#"
            SELECT
                id,
                name,
                size,
                replication_factor
            FROM mz_catalog.mz_clusters
            WHERE name = $1
        "#;

        let rows = self
            .client
            .query(query, &[&name])
            .await
            .map_err(ConnectionError::Query)?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = &rows[0];
        let replication_factor: Option<i64> = row
            .try_get("replication_factor")
            .or_else(|_| {
                row.try_get::<_, Option<i32>>("replication_factor")
                    .map(|v| v.map(|x| x as i64))
            })
            .or_else(|_| {
                row.try_get::<_, Option<i16>>("replication_factor")
                    .map(|v| v.map(|x| x as i64))
            })
            .unwrap_or(None);

        Ok(Some(Cluster {
            id: row.get("id"),
            name: row.get("name"),
            size: row.get("size"),
            replication_factor,
        }))
    }

    /// List all clusters.
    pub async fn list_clusters(&self) -> Result<Vec<Cluster>, ConnectionError> {
        let query = r#"
            SELECT
                id,
                name,
                size,
                replication_factor
            FROM mz_catalog.mz_clusters
            ORDER BY name
        "#;

        let rows = self
            .client
            .query(query, &[])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(rows
            .iter()
            .map(|row| Cluster {
                id: row.get("id"),
                name: row.get("name"),
                size: row.get("size"),
                replication_factor: row.get("replication_factor"),
            })
            .collect())
    }

    /// Check if the deployment tracking table exists.
    pub async fn deployment_table_exists(&self) -> Result<bool, ConnectionError> {
        let query = r#"
            SELECT EXISTS(
                SELECT 1
                FROM mz_catalog.mz_tables t
                JOIN mz_catalog.mz_schemas s ON t.schema_id = s.id
                JOIN mz_catalog.mz_databases d ON s.database_id = d.id
                WHERE t.name = 'deployments'
                    AND s.name = 'deploy'
                    AND d.name = 'materialize'
            )
        "#;

        let row = self
            .client
            .query_one(query, &[])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(row.get(0))
    }

    /// Get the current Materialize user/role.
    pub async fn get_current_user(&self) -> Result<String, ConnectionError> {
        let row = self
            .client
            .query_one("SELECT current_user()", &[])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(row.get(0))
    }

    /// Check which objects from a set exist in the production database.
    pub async fn check_objects_exist(
        &self,
        objects: &HashSet<ObjectId>,
    ) -> Result<Vec<String>, ConnectionError> {
        let fqns: Vec<String> = objects.iter().map(|o| o.to_string()).collect();
        if fqns.is_empty() {
            return Ok(Vec::new());
        }

        let placeholders: Vec<String> = (1..=fqns.len()).map(|i| format!("${}", i)).collect();
        let placeholders_str = placeholders.join(", ");

        let query = format!(
            r#"
            SELECT d.name || '.' || s.name || '.' || mo.name as fqn
            FROM mz_objects mo
            JOIN mz_schemas s ON mo.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name || '.' || s.name || '.' || mo.name IN ({})
            AND mo.type IN ('table', 'view', 'materialized-view', 'source', 'sink')
            ORDER BY fqn
        "#,
            placeholders_str
        );

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        for fqn in &fqns {
            params.push(fqn);
        }

        let rows = self
            .client
            .query(&query, &params)
            .await
            .map_err(ConnectionError::Query)?;

        Ok(rows.iter().map(|row| row.get("fqn")).collect())
    }

    /// Get schema deployment records from the database for a specific environment.
    pub async fn get_schema_deployments(
        &self,
        environment: Option<&str>,
    ) -> Result<Vec<SchemaDeploymentRecord>, ConnectionError> {
        use std::time::UNIX_EPOCH;

        let query = if environment.is_none() {
            r#"
                SELECT environment, database, schema,
                       CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as deployed_at_epoch,
                       '' as deployed_by,
                       CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch
                FROM deploy.production
                ORDER BY database, schema
            "#
        } else {
            r#"
                SELECT environment, database, schema,
                       CAST(EXTRACT(EPOCH FROM deployed_at) AS DOUBLE PRECISION) as deployed_at_epoch,
                       deployed_by,
                       CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch
                FROM deploy.deployments
                WHERE environment = $1
                ORDER BY database, schema
            "#
        };

        let rows = if environment.is_none() {
            self.client
                .query(query, &[])
                .await
                .map_err(ConnectionError::Query)?
        } else {
            self.client
                .query(query, &[&environment])
                .await
                .map_err(ConnectionError::Query)?
        };

        let mut records = Vec::new();
        for row in rows {
            let environment: String = row.get("environment");
            let database: String = row.get("database");
            let schema: String = row.get("schema");
            let deployed_at_epoch: f64 = row.get("deployed_at_epoch");
            let deployed_by: String = row.get("deployed_by");
            let promoted_at_epoch: Option<f64> = row.get("promoted_at_epoch");

            let deployed_at = UNIX_EPOCH + std::time::Duration::from_secs_f64(deployed_at_epoch);
            let promoted_at = promoted_at_epoch
                .map(|epoch| UNIX_EPOCH + std::time::Duration::from_secs_f64(epoch));

            records.push(SchemaDeploymentRecord {
                environment,
                database,
                schema,
                deployed_at,
                deployed_by,
                promoted_at,
                git_commit: None,
            });
        }

        Ok(records)
    }

    /// Get deployment object records from the database for a specific environment.
    pub async fn get_deployment_objects(
        &self,
        environment: Option<&str>,
    ) -> Result<DeploymentSnapshot, ConnectionError> {
        let query = if environment.is_none() {
            r#"
                SELECT o.database, o.schema, o.object, o.hash
                FROM deploy.objects o
                JOIN deploy.production p
                  ON o.database = p.database AND o.schema = p.schema
                WHERE o.environment = p.environment
            "#
        } else {
            r#"
                SELECT database, schema, object, hash
                FROM deploy.objects
                WHERE environment = $1
            "#
        };

        let rows = if environment.is_none() {
            self.client
                .query(query, &[])
                .await
                .map_err(ConnectionError::Query)?
        } else {
            self.client
                .query(query, &[&environment])
                .await
                .map_err(ConnectionError::Query)?
        };

        let mut objects = BTreeMap::new();
        let mut schemas = HashSet::new();
        for row in rows {
            let database: String = row.get("database");
            let schema: String = row.get("schema");
            let object: String = row.get("object");
            let object_hash: String = row.get("hash");

            let object_id = ObjectId {
                database: database.clone(),
                schema: schema.clone(),
                object,
            };
            objects.insert(object_id, object_hash);
            schemas.insert((database, schema));
        }

        Ok(DeploymentSnapshot { objects, schemas })
    }

    /// Get metadata about a deployment environment for validation.
    pub async fn get_deployment_metadata(
        &self,
        environment: &str,
    ) -> Result<Option<DeploymentMetadata>, ConnectionError> {
        use std::time::UNIX_EPOCH;

        let query = r#"
            SELECT environment,
                   CAST(EXTRACT(EPOCH FROM promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch,
                   database,
                   schema
            FROM deploy.deployments
            WHERE environment = $1
        "#;

        let rows = self
            .client
            .query(query, &[&environment])
            .await
            .map_err(ConnectionError::Query)?;

        if rows.is_empty() {
            return Ok(None);
        }

        let first_row = &rows[0];
        let environment: String = first_row.get("environment");
        let promoted_at_epoch: Option<f64> = first_row.get("promoted_at_epoch");
        let promoted_at =
            promoted_at_epoch.map(|epoch| UNIX_EPOCH + std::time::Duration::from_secs_f64(epoch));

        let mut schemas = Vec::new();
        for row in rows {
            let database: String = row.get("database");
            let schema: String = row.get("schema");
            schemas.push((database, schema));
        }

        Ok(Some(DeploymentMetadata {
            environment,
            promoted_at,
            schemas,
        }))
    }

    /// Get staging schema names for a specific environment.
    pub async fn get_staging_schemas(
        &self,
        environment: &str,
    ) -> Result<Vec<(String, String)>, ConnectionError> {
        let suffix = format!("_{}", environment);
        let pattern = format!("%{}", suffix);

        let query = r#"
            SELECT d.name as database, s.name as schema
            FROM mz_schemas s
            JOIN mz_databases d ON s.database_id = d.id
            WHERE s.name LIKE $1
        "#;

        let rows = self
            .client
            .query(query, &[&pattern])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(rows
            .iter()
            .map(|row| {
                let database: String = row.get("database");
                let schema: String = row.get("schema");
                (database, schema)
            })
            .collect())
    }

    /// Get staging cluster names for a specific environment.
    pub async fn get_staging_clusters(
        &self,
        environment: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        let suffix = format!("_{}", environment);
        let pattern = format!("%{}", suffix);

        let query = r#"
            SELECT name
            FROM mz_clusters
            WHERE name LIKE $1
        "#;

        let rows = self
            .client
            .query(query, &[&pattern])
            .await
            .map_err(ConnectionError::Query)?;

        Ok(rows.iter().map(|row| row.get("name")).collect())
    }

    /// Check for deployment conflicts (schemas updated after deployment started).
    pub async fn check_deployment_conflicts(
        &self,
        environment: &str,
    ) -> Result<Vec<ConflictRecord>, ConnectionError> {
        use std::time::{Duration, UNIX_EPOCH};

        let query = r#"
            SELECT p.database, p.schema, p.environment,
                   CAST(EXTRACT(EPOCH FROM p.promoted_at) AS DOUBLE PRECISION) as promoted_at_epoch
            FROM deploy.production p
            JOIN deploy.deployments d USING (database, schema)
            WHERE d.environment = $1 AND p.promoted_at > d.deployed_at
        "#;

        let rows = self
            .client
            .query(query, &[&environment])
            .await
            .map_err(ConnectionError::Query)?;

        let conflicts = rows
            .iter()
            .map(|row| {
                let promoted_at_epoch: f64 = row.get("promoted_at_epoch");
                let promoted_at = UNIX_EPOCH + Duration::from_secs_f64(promoted_at_epoch);

                ConflictRecord {
                    database: row.get("database"),
                    schema: row.get("schema"),
                    environment: row.get("environment"),
                    promoted_at,
                }
            })
            .collect();

        Ok(conflicts)
    }

    /// Drop all objects in a schema.
    pub async fn drop_schema_objects(
        &self,
        database: &str,
        schema: &str,
    ) -> Result<Vec<String>, ConnectionError> {
        let query = r#"
            SELECT mo.name, mo.type
            FROM mz_objects mo
            JOIN mz_schemas s ON mo.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name = $1 AND s.name = $2
            AND mo.type IN ('table', 'view', 'materialized-view', 'source', 'sink')
            ORDER BY mo.id DESC
        "#;

        let rows = self
            .client
            .query(query, &[&database, &schema])
            .await
            .map_err(ConnectionError::Query)?;

        let mut dropped = Vec::new();
        for row in rows {
            let name: String = row.get("name");
            let obj_type: String = row.get("type");

            let fqn = format!(
                "{}.{}.{}",
                quote_identifier(database),
                quote_identifier(schema),
                quote_identifier(&name)
            );
            let drop_type = match obj_type.as_str() {
                "table" => "TABLE",
                "view" => "VIEW",
                "materialized-view" => "MATERIALIZED VIEW",
                "source" => "SOURCE",
                "sink" => "SINK",
                _ => continue,
            };

            let drop_sql = format!("DROP {} IF EXISTS {} CASCADE", drop_type, fqn);
            self.execute(&drop_sql, &[]).await?;

            dropped.push(fqn);
        }

        Ok(dropped)
    }

    /// Drop specific objects by their ObjectIds.
    pub async fn drop_objects(
        &self,
        objects: &HashSet<ObjectId>,
    ) -> Result<Vec<String>, ConnectionError> {
        let mut dropped = Vec::new();

        if objects.is_empty() {
            return Ok(dropped);
        }

        let placeholders: Vec<String> = (1..=objects.len()).map(|i| format!("${}", i)).collect();
        let placeholders_str = placeholders.join(", ");

        let query = format!(
            r#"
            SELECT mo.name, s.name as schema_name, d.name as database_name, mo.type
            FROM mz_objects mo
            JOIN mz_schemas s ON mo.schema_id = s.id
            JOIN mz_databases d ON s.database_id = d.id
            WHERE d.name || '.' || s.name || '.' || mo.name IN ({})
            AND mo.type IN ('table', 'view', 'materialized-view', 'source', 'sink')
            ORDER BY mo.id DESC
        "#,
            placeholders_str
        );

        let mut params: Vec<&(dyn ToSql + Sync)> = Vec::new();
        let fqns: Vec<_> = objects.iter().map(|object| object.to_string()).collect();
        for fqn in &fqns {
            params.push(fqn);
        }

        let rows = self
            .client
            .query(&query, &params)
            .await
            .map_err(ConnectionError::Query)?;

        for row in rows {
            let name: String = row.get("name");
            let schema: String = row.get("schema_name");
            let database: String = row.get("database_name");
            let obj_type: String = row.get("type");

            let fqn = format!(
                "{}.{}.{}",
                quote_identifier(&database),
                quote_identifier(&schema),
                quote_identifier(&name)
            );
            let drop_type = match obj_type.as_str() {
                "table" => "TABLE",
                "view" => "VIEW",
                "materialized-view" => "MATERIALIZED VIEW",
                "source" => "SOURCE",
                "sink" => "SINK",
                _ => continue,
            };

            let drop_sql = format!("DROP {} IF EXISTS {} CASCADE", drop_type, fqn);
            self.execute(&drop_sql, &[]).await?;

            dropped.push(fqn);
        }

        Ok(dropped)
    }

    /// Drop staging schemas by name.
    pub async fn drop_staging_schemas(
        &self,
        schemas: &[(String, String)],
    ) -> Result<(), ConnectionError> {
        for (database, schema) in schemas {
            let drop_sql = format!(
                "DROP SCHEMA IF EXISTS {}.{} CASCADE",
                quote_identifier(database),
                quote_identifier(schema)
            );
            self.execute(&drop_sql, &[]).await?;
        }

        Ok(())
    }

    /// Drop staging clusters by name.
    pub async fn drop_staging_clusters(&self, clusters: &[String]) -> Result<(), ConnectionError> {
        for cluster in clusters {
            let drop_sql = format!(
                "DROP CLUSTER IF EXISTS {} CASCADE",
                quote_identifier(cluster)
            );
            self.execute(&drop_sql, &[]).await?;
        }

        Ok(())
    }

    /// Validate that all required databases, schemas, and external dependencies exist
    pub async fn validate_project(
        &mut self,
        planned_project: &planned::Project,
        project_root: &Path,
    ) -> Result<(), DatabaseValidationError> {
        let mut missing_databases = Vec::new();
        let mut missing_schemas = Vec::new();
        let mut missing_clusters = Vec::new();

        // Collect all required databases
        let mut required_databases = HashSet::new();
        for db in &planned_project.databases {
            required_databases.insert(db.name.clone());
        }
        for ext_dep in &planned_project.external_dependencies {
            required_databases.insert(ext_dep.database.clone());
        }

        // Collect all required schemas (database, schema) pairs
        let mut required_schemas = HashSet::new();
        for db in &planned_project.databases {
            for schema in &db.schemas {
                required_schemas.insert((db.name.clone(), schema.name.clone()));
            }
        }
        for ext_dep in &planned_project.external_dependencies {
            required_schemas.insert((ext_dep.database.clone(), ext_dep.schema.clone()));
        }

        // Check databases exist
        for database in &required_databases {
            let query = "SELECT name FROM mz_databases WHERE name = $1";
            let rows = self
                .client
                .query(query, &[database])
                .await
                .map_err(DatabaseValidationError::QueryError)?;
            if rows.is_empty() {
                missing_databases.push(database.clone());
            }
        }

        // Check schemas exist
        for (database, schema) in &required_schemas {
            let query = r#"
                SELECT s.name FROM mz_schemas s
                JOIN mz_databases d ON s.database_id = d.id
                WHERE s.name = $1 AND d.name = $2"#;
            let rows = self
                .client
                .query(query, &[schema, database])
                .await
                .map_err(DatabaseValidationError::QueryError)?;
            if rows.is_empty() {
                missing_schemas.push((database.clone(), schema.clone()));
            }
        }

        // Check clusters exist
        for cluster in &planned_project.cluster_dependencies {
            let query = "SELECT name FROM mz_clusters WHERE name = $1";
            let rows = self
                .client
                .query(query, &[&cluster.name])
                .await
                .map_err(DatabaseValidationError::QueryError)?;
            if rows.is_empty() {
                missing_clusters.push(cluster.name.clone());
            }
        }

        // Build ObjectId to file path mapping by reconstructing paths from ObjectIds
        // Path format: <root>/<database>/<schema>/<object>.sql
        let mut object_paths: HashMap<ObjectId, PathBuf> = HashMap::new();
        for db in &planned_project.databases {
            for schema in &db.schemas {
                for obj in &schema.objects {
                    let file_path = project_root
                        .join(&obj.id.database)
                        .join(&obj.id.schema)
                        .join(format!("{}.sql", obj.id.object));
                    object_paths.insert(obj.id.clone(), file_path);
                }
            }
        }

        // Check external dependencies and group missing ones by file
        let mut missing_external_deps = HashSet::new();
        for ext_dep in &planned_project.external_dependencies {
            let query = r#"
                SELECT mo.name
                FROM mz_objects mo
                JOIN mz_schemas s ON mo.schema_id = s.id
                JOIN mz_databases d ON s.database_id = d.id
                WHERE mo.name = $1 AND s.name = $2 AND d.name = $3
               "#;

            let rows = self
                .client
                .query(
                    query,
                    &[&ext_dep.object, &ext_dep.schema, &ext_dep.database],
                )
                .await
                .map_err(DatabaseValidationError::QueryError)?;
            if rows.is_empty() {
                missing_external_deps.insert(ext_dep.clone());
            }
        }

        // Group missing dependencies by the files that reference them
        let mut file_missing_deps: HashMap<PathBuf, (ObjectId, Vec<ObjectId>)> = HashMap::new();

        for db in &planned_project.databases {
            for schema in &db.schemas {
                for obj in &schema.objects {
                    let mut missing_for_this_object = Vec::new();

                    for dep in &obj.dependencies {
                        if missing_external_deps.contains(dep) {
                            missing_for_this_object.push(dep.clone());
                        }
                    }

                    if !missing_for_this_object.is_empty()
                        && let Some(file_path) = object_paths.get(&obj.id)
                    {
                        file_missing_deps
                            .insert(file_path.clone(), (obj.id.clone(), missing_for_this_object));
                    }
                }
            }
        }

        // Create compilation error for each file with missing dependencies
        let mut compilation_errors = Vec::new();
        for (file_path, (object_id, missing_deps)) in file_missing_deps {
            compilation_errors.push(DatabaseValidationError::CompilationFailed {
                file_path,
                object_name: object_id,
                missing_dependencies: missing_deps,
            });
        }

        // Return results
        if !missing_databases.is_empty()
            || !missing_schemas.is_empty()
            || !missing_clusters.is_empty()
            || !compilation_errors.is_empty()
        {
            Err(DatabaseValidationError::Multiple {
                databases: missing_databases,
                schemas: missing_schemas,
                clusters: missing_clusters,
                compilation_errors,
            })
        } else {
            Ok(())
        }
    }
}
