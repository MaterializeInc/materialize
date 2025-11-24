//! Docker-based type checker using a persistent Docker container.

use super::typechecker::{ObjectTypeCheckError, TypeCheckError, TypeCheckErrors, TypeChecker};
use super::Types;
use crate::client::{Client, Profile};
use crate::project::ast::Statement;
use crate::project::hir::FullyQualifiedName;
use crate::project::mir::Project;
use crate::project::normalize::NormalizingVisitor;
use crate::verbose;
use mz_sql_parser::ast::{CreateViewStatement, Ident, IfExistsBehavior, UnresolvedItemName, ViewDefinition};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::process::Command;
use tokio::time::{sleep, Duration};

/// Name of the persistent Docker container used for type checking
const CONTAINER_NAME: &str = "mz-deploy-typecheck";

/// Host port to bind for the persistent container
const CONTAINER_PORT: u16 = 16875;

/// Docker-based type checker using testcontainers
pub struct DockerTypeChecker {
    /// Types.lock containing external dependencies
    types: Types,
    /// Project root path for reconstructing file paths
    project_root: PathBuf,
    /// Materialize Docker image to use
    image: String,
}

impl DockerTypeChecker {
    /// Create a new Docker type checker
    ///
    /// # Arguments
    /// * `types` - The types.lock file containing external dependency schemas
    /// * `project_root` - Root directory of the project (for error reporting)
    pub fn new(types: Types, project_root: impl Into<PathBuf>) -> Self {
        Self {
            types,
            project_root: project_root.into(),
            image: "materialize/materialized:latest".to_string(),
        }
    }

    /// Set custom Docker image
    pub fn with_image(mut self, image: impl Into<String>) -> Self {
        self.image = image.into();
        self
    }

    /// Check if the persistent container exists
    async fn container_exists(&self) -> Result<bool, TypeCheckError> {
        let output = Command::new("docker")
            .args(["ps", "-a", "--filter", &format!("name=^{}$", CONTAINER_NAME), "--format", "{{.Names}}"])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        Ok(output.status.success() && !output.stdout.is_empty())
    }

    /// Check if the persistent container is running
    async fn container_is_running(&self) -> Result<bool, TypeCheckError> {
        let output = Command::new("docker")
            .args(["ps", "--filter", &format!("name=^{}$", CONTAINER_NAME), "--format", "{{.Names}}"])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        Ok(output.status.success() && !output.stdout.is_empty())
    }

    /// Check if the container is healthy by attempting a connection
    async fn container_is_healthy(&self) -> bool {
        let profile = Profile {
            name: "docker-typecheck".to_string(),
            host: "localhost".to_string(),
            port: CONTAINER_PORT,
            username: Some("materialize".to_string()),
            password: None,
        };

        // Try to connect - if this succeeds, container is healthy
        match Client::connect_with_profile(profile).await {
            Ok(_) => true,
            Err(_) => false,
        }
    }

    /// Remove the persistent container
    async fn remove_container(&self) -> Result<(), TypeCheckError> {
        verbose!("Removing existing container: {}", CONTAINER_NAME);
        let output = Command::new("docker")
            .args(["rm", "-f", CONTAINER_NAME])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(TypeCheckError::ContainerStartFailed(
                format!("Failed to remove container: {}", stderr).into()
            ));
        }

        Ok(())
    }

    /// Create and start the persistent container
    async fn create_container(&self) -> Result<(), TypeCheckError> {
        verbose!("Creating persistent container: {} (image: {})", CONTAINER_NAME, self.image);

        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name", CONTAINER_NAME,
                "-p", &format!("{}:6875", CONTAINER_PORT),
                &self.image,
            ])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(TypeCheckError::ContainerStartFailed(
                format!("Failed to create container: {}", stderr).into()
            ));
        }

        Ok(())
    }

    /// Start an existing stopped container
    async fn start_existing_container(&self) -> Result<(), TypeCheckError> {
        verbose!("Starting existing container: {}", CONTAINER_NAME);

        let output = Command::new("docker")
            .args(["start", CONTAINER_NAME])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(TypeCheckError::ContainerStartFailed(
                format!("Failed to start container: {}", stderr).into()
            ));
        }

        Ok(())
    }

    /// Wait for the container to be ready to accept connections
    async fn wait_for_container(&self) -> Result<(), TypeCheckError> {
        verbose!("Waiting for container to be ready...");

        // Wait up to 30 seconds for the container to be ready
        for i in 0..30 {
            if self.container_is_healthy().await {
                verbose!("Container is ready!");
                return Ok(());
            }
            if i < 29 {
                sleep(Duration::from_secs(1)).await;
            }
        }

        Err(TypeCheckError::ContainerStartFailed(
            "Container failed to become healthy within 30 seconds".into()
        ))
    }

    /// Ensure the persistent container is running and healthy
    async fn ensure_container(&self) -> Result<(), TypeCheckError> {
        let exists = self.container_exists().await?;
        let is_running = if exists {
            self.container_is_running().await?
        } else {
            false
        };

        if is_running {
            // Container is running, check if it's healthy
            verbose!("Found running container: {}", CONTAINER_NAME);
            if self.container_is_healthy().await {
                verbose!("Container is healthy, reusing it");
                return Ok(());
            } else {
                verbose!("Container is unhealthy, recreating it");
                self.remove_container().await?;
            }
        } else if exists {
            // Container exists but is not running, try to start it
            verbose!("Found stopped container: {}", CONTAINER_NAME);
            match self.start_existing_container().await {
                Ok(_) => {
                    self.wait_for_container().await?;
                    return Ok(());
                }
                Err(_) => {
                    verbose!("Failed to start stopped container, recreating it");
                    self.remove_container().await?;
                }
            }
        }

        // Create new container
        self.create_container().await?;
        self.wait_for_container().await?;

        Ok(())
    }

    /// Build object path mapping for error reporting
    fn build_object_paths(&self, project: &Project) -> HashMap<crate::project::object_id::ObjectId, PathBuf> {
        let mut paths = HashMap::new();
        for obj in project.iter_objects() {
            let path = self.project_root
                .join(&obj.id.database)
                .join(&obj.id.schema)
                .join(format!("{}.sql", obj.id.object));
            paths.insert(obj.id.clone(), path);
        }
        paths
    }

    /// Create temporary tables for external dependencies
    async fn create_external_dependencies(
        &self,
        client: &mut Client,
        project: &Project,
    ) -> Result<(), TypeCheckError> {
        for ext_dep in &project.external_dependencies {
            // Get column definitions from types.lock
            let fqn = ext_dep.to_string();
            let columns = self.types.objects.get(&fqn).ok_or_else(|| {
                TypeCheckError::ExternalDependencyFailed {
                    object: ext_dep.clone(),
                    source: format!("external dependency '{}' not found in types.lock", fqn).into(),
                }
            })?;

            // Build CREATE TABLE IF NOT EXISTS statement (idempotent for reuse)
            let mut col_defs = Vec::new();
            for (col_name, col_type) in columns {
                let nullable = if col_type.nullable { "" } else { " NOT NULL" };
                col_defs.push(format!("{} {}{}", col_name, col_type.r#type, nullable));
            }

            let create_sql = format!(
                "CREATE TABLE IF NOT EXISTS {}_{}_{} ({})",
                ext_dep.database,
                ext_dep.schema,
                ext_dep.object,
                col_defs.join(", ")
            );

            verbose!("Creating external dependency table: {}", ext_dep);
            client.pg_client()
                .execute(&create_sql, &[])
                .await
                .map_err(|e| TypeCheckError::ExternalDependencyFailed {
                    object: ext_dep.clone(),
                    source: Box::new(e),
                })?;
        }

        Ok(())
    }

    /// Create temporary view for a project object
    fn create_temporary_view_sql(&self, stmt: &Statement, fqn: &FullyQualifiedName) -> Option<String> {
        let visitor = NormalizingVisitor::flattening(fqn);

        match stmt {
            Statement::CreateView(view) => {
                let mut view = view.clone();
                view.temporary = true;

                let normalized = Statement::CreateView(view)
                    .normalize_name_with(&visitor, &fqn.to_item_name())
                    .normalize_dependencies_with(&visitor);

                Some(normalized.to_string())
            }
            Statement::CreateMaterializedView(mv) => {
                let view_stmt = CreateViewStatement {
                    if_exists: IfExistsBehavior::Error,
                    temporary: true,
                    definition: ViewDefinition {
                        name: mv.name.clone(),
                        columns: mv.columns.clone(),
                        query: mv.query.clone(),
                    },
                };
                let normalized = Statement::CreateView(view_stmt)
                    .normalize_name_with(&visitor, &fqn.to_item_name())
                    .normalize_dependencies_with(&visitor);

                Some(normalized.to_string())
            }
            Statement::CreateTable(table) => {
                let mut table = table.clone();
                table.temporary = true;

                let normalized = Statement::CreateTable(table)
                    .normalize_name_with(&visitor, &fqn.to_item_name())
                    .normalize_dependencies_with(&visitor);

                Some(normalized.to_string())
            }
            _ => {
                // Other statement types (sources, sinks, connections, secrets)
                // cannot be type-checked in this way
                None
            }
        }
    }
}

#[async_trait::async_trait]
impl TypeChecker for DockerTypeChecker {
    async fn typecheck(&self, project: &Project) -> Result<(), TypeCheckError> {
        // Ensure the persistent container is running and healthy
        self.ensure_container().await?;

        // Connect to Materialize (using fixed port)
        let profile = Profile {
            name: "docker-typecheck".to_string(),
            host: "localhost".to_string(),
            port: CONTAINER_PORT,
            username: Some("materialize".to_string()),
            password: None,
        };

        verbose!("Connecting to Materialize...");
        let mut client = Client::connect_with_profile(profile).await?;

        // Create external dependencies
        if !project.external_dependencies.is_empty() {
            verbose!("Creating {} external dependency tables", project.external_dependencies.len());
            self.create_external_dependencies(&mut client, project).await?;
        }

        // Type check objects in topological order
        let object_paths = self.build_object_paths(project);
        let sorted_objects = project.get_sorted_objects()?;

        verbose!("Type checking {} objects in topological order", sorted_objects.len());
        let mut errors = Vec::new();

        for (object_id, hir_object) in sorted_objects {
            verbose!("Type checking: {}", object_id);

            // Build the FQN from the object_id
            let fqn = FullyQualifiedName::from(UnresolvedItemName(vec![
                Ident::new(&object_id.database).expect("valid database"),
                Ident::new(&object_id.schema).expect("valid schema"),
                Ident::new(&object_id.object).expect("valid object"),
            ]));

            if let Some(sql) = self.create_temporary_view_sql(&hir_object.stmt, &fqn) {
                // Execute and check for errors
                match client.pg_client().execute(&sql, &[]).await {
                    Ok(_) => {
                        verbose!("  ✓ Type check passed");
                    }
                    Err(e) => {
                        // ConnectionError wraps tokio_postgres::Error, extract the database error
                        let (error_message, detail, hint) = match &e {
                            crate::client::ConnectionError::Query(pg_err) => {
                                if let Some(db_err) = pg_err.as_db_error() {
                                    (
                                        db_err.message().to_string(),
                                        db_err.detail().map(|s| s.to_string()),
                                        db_err.hint().map(|s| s.to_string()),
                                    )
                                } else {
                                    (pg_err.to_string(), None, None)
                                }
                            }
                            _ => (e.to_string(), None, None),
                        };

                        verbose!("  ✗ Type check failed: {}", error_message);

                        let path = object_paths.get(&object_id)
                            .cloned()
                            .unwrap_or_else(|| {
                                self.project_root
                                    .join(&object_id.database)
                                    .join(&object_id.schema)
                                    .join(format!("{}.sql", object_id.object))
                            });

                        errors.push(ObjectTypeCheckError {
                            object_id: object_id.clone(),
                            file_path: path,
                            sql_statement: sql,
                            error_message,
                            detail,
                            hint,
                        });
                    }
                }
            } else {
                verbose!("  - Skipping non-view/table object");
            }
        }

        if errors.is_empty() {
            verbose!("All type checks passed!");
            Ok(())
        } else {
            Err(TypeCheckError::Multiple(TypeCheckErrors { errors }))
        }
    }
}