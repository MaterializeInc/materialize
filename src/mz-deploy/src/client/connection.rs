use crate::client::config::{ConfigError, Profile, ProfilesConfig};
use crate::client::execute::ExecuteApi;
use crate::project::ast::Statement;
use crate::project::hir::FullyQualifiedName;
use crate::project::mir;
use crate::project::normalize::NormalizingVisitor;
use crate::types::{ColumnType, Types};
use mz_sql_parser::ast::{CreateViewStatement, IfExistsBehavior, ViewDefinition};
use owo_colors::OwoColorize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::path::{Path, PathBuf};
use thiserror::Error;
use tokio_postgres::{Client as PgClient, NoTls};
use crate::project::object_id::ObjectId;

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
    pub async fn connect(profile_name: Option<&str>) -> Result<Self, ConnectionError> {
        // Load profiles configuration
        let config = ProfilesConfig::load()?;

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

        let mut client = Client { client, profile };

        // Query and log connection information
        client.log_connection_info().await?;

        Ok(client)
    }

    /// Execute the connection info query and log the results
    async fn log_connection_info(&mut self) -> Result<(), ConnectionError> {
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

    /// Get a reference to the underlying postgres client
    pub fn pg_client(&self) -> ExecuteApi<'_> {
        ExecuteApi::new(&self.client)
    }

    /// Get a mutable reference to the underlying postgres client
    pub fn pg_client_mut(&mut self) -> &mut PgClient {
        &mut self.client
    }

    /// Access the introspection API for querying catalog objects.
    ///
    /// The introspection API provides read-only methods for querying schemas,
    /// clusters, and other catalog objects using mz_catalog tables.
    pub fn introspection(&self) -> crate::client::introspection::IntrospectionApi<'_> {
        crate::client::introspection::IntrospectionApi::new(&self.client)
    }

    /// Access the creation API for creating resources.
    ///
    /// The creation API provides methods for creating schemas, clusters, and
    /// other database objects using DDL statements.
    pub fn creation(&self) -> crate::client::creation::CreationApi<'_> {
        crate::client::creation::CreationApi::new(&self.client)
    }

    /// Access the destruction API for dropping resources.
    ///
    /// The destruction API provides methods for safely dropping database objects
    /// in the correct order using DROP IF EXISTS and CASCADE.
    pub fn destruction(&self) -> crate::client::destruction::DestructionApi<'_> {
        crate::client::destruction::DestructionApi::new(&self.client)
    }

    /// Get the profile used for this connection
    pub fn profile(&self) -> &Profile {
        &self.profile
    }

    /// Query SHOW COLUMNS for all external dependencies and return their schemas as a Types object
    pub async fn query_external_types(
        &mut self,
        project: &mir::Project,
    ) -> Result<Types, ConnectionError> {
        for oid in &project.external_dependencies {
            let view = format!(
                "CREATE TEMPORARY VIEW {0}_{1}_{2} AS SELECT * FROM {0}.{1}.{2}",
                oid.database, oid.schema, oid.object
            );

            self.client.execute(&view, &[]).await?;
        }

        let mut objects = BTreeMap::new();

        let mut oids = vec![];
        for (oid, object) in project.get_sorted_objects()? {
            oids.push(oid);

            let (fqn, stmt) = match &object.stmt {
                Statement::CreateView(view) => {
                    let mut view = view.clone();
                    view.temporary = true;
                    let fqn: FullyQualifiedName = view.definition.name.clone().into();
                    let stmt = Statement::CreateView(view);
                    (fqn, stmt)
                }
                Statement::CreateMaterializedView(materialized_view) => {
                    let fqn: FullyQualifiedName = materialized_view.name.clone().into();
                    let stmt = Statement::CreateView(CreateViewStatement {
                        if_exists: IfExistsBehavior::Error,
                        temporary: true,
                        definition: ViewDefinition {
                            name: materialized_view.name.clone(),
                            columns: materialized_view.columns.clone(),
                            query: materialized_view.query.clone(),
                        },
                    });

                    (fqn, stmt)
                }
                Statement::CreateTable(table) => {
                    let mut table = table.clone();
                    table.temporary = true;
                    let fqn: FullyQualifiedName = table.name.clone().into();
                    let stmt = Statement::CreateTable(table);
                    (fqn, stmt)
                }
                _ => {
                    println!("{:#?}", object.stmt);
                    panic!("{:#?}", object.stmt)
                }
            };

            let visitor = NormalizingVisitor::flattening(&fqn);
            let stmt = stmt
                .normalize_name_with(&visitor, &fqn.to_item_name())
                .normalize_dependencies_with(&visitor);
            self.client.execute(&stmt.to_string(), &[]).await?;
        }

        oids.append(
            &mut project
                .external_dependencies
                .clone()
                .into_iter()
                .collect::<Vec<_>>(),
        );
        for object in &oids {
            let query = format!(
                "SHOW COLUMNS FROM {}_{}_{}",
                object.database, object.schema, object.object
            );

            let rows = self.client.query(&query, &[]).await?;

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

            objects.insert(object.to_string(), columns);
        }

        Ok(Types {
            version: 1,
            objects,
        })
    }

    /// Validate that all required databases, schemas, and external dependencies exist
    pub async fn validate_project(
        &mut self,
        mir_project: &mir::Project,
        project_root: &Path,
    ) -> Result<(), DatabaseValidationError> {
        let mut missing_databases = Vec::new();
        let mut missing_schemas = Vec::new();
        let mut missing_clusters = Vec::new();

        // Collect all required databases
        let mut required_databases = HashSet::new();
        for db in &mir_project.databases {
            required_databases.insert(db.name.clone());
        }
        for ext_dep in &mir_project.external_dependencies {
            required_databases.insert(ext_dep.database.clone());
        }

        // Collect all required schemas (database, schema) pairs
        let mut required_schemas = HashSet::new();
        for db in &mir_project.databases {
            for schema in &db.schemas {
                required_schemas.insert((db.name.clone(), schema.name.clone()));
            }
        }
        for ext_dep in &mir_project.external_dependencies {
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
        for cluster in &mir_project.cluster_dependencies {
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
        for db in &mir_project.databases {
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
        for ext_dep in &mir_project.external_dependencies {
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

        for db in &mir_project.databases {
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
