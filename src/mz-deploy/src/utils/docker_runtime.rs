//! Docker runtime for running a Materialize container for testing and type checking.
//!
//! This module manages a persistent Docker container and provides a connected database client
//! with external dependencies already staged as temporary tables.

use crate::client::{Client, Profile};
use crate::project::planned::Project;
use crate::types::{TypeCheckError, Types};
use crate::verbose;
use tokio::process::Command;
use tokio::time::{Duration, sleep};

/// Name of the persistent Docker container
const CONTAINER_NAME: &str = "mz-deploy-typecheck";

/// Host port to bind for the persistent container
const CONTAINER_PORT: u16 = 16875;

/// Docker runtime for managing a Materialize container
pub struct DockerRuntime {
    /// Materialize Docker image to use
    image: String,
}

impl DockerRuntime {
    /// Create a new Docker runtime with the default Materialize image
    pub fn new() -> Self {
        Self {
            image: "materialize/materialized:latest".to_string(),
        }
    }

    /// Set a custom Docker image
    pub fn with_image(mut self, image: impl Into<String>) -> Self {
        self.image = image.into();
        self
    }

    /// Get a connected client with external dependencies staged
    ///
    /// This method:
    /// 1. Ensures the Docker container is running and healthy
    /// 2. Connects to the Materialize database
    /// 3. Creates temporary tables for all external dependencies from types.lock
    /// 4. Returns the connected client ready for use
    ///
    /// # Arguments
    /// * `project` - The project containing external dependencies
    /// * `types` - The types.lock data containing external dependency schemas
    ///
    /// # Returns
    /// A connected Client with external dependencies already staged
    pub async fn get_client(
        &self,
        project: &Project,
        types: &Types,
    ) -> Result<Client, TypeCheckError> {
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
            verbose!(
                "Creating {} external dependency tables",
                project.external_dependencies.len()
            );
            self.create_external_dependencies(&mut client, project, types)
                .await?;
        }

        Ok(client)
    }

    /// Check if the persistent container exists
    async fn container_exists(&self) -> Result<bool, TypeCheckError> {
        let output = Command::new("docker")
            .args([
                "ps",
                "-a",
                "--filter",
                &format!("name=^{}$", CONTAINER_NAME),
                "--format",
                "{{.Names}}",
            ])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        Ok(output.status.success() && !output.stdout.is_empty())
    }

    /// Check if the persistent container is running
    async fn container_is_running(&self) -> Result<bool, TypeCheckError> {
        let output = Command::new("docker")
            .args([
                "ps",
                "--filter",
                &format!("name=^{}$", CONTAINER_NAME),
                "--format",
                "{{.Names}}",
            ])
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
                format!("Failed to remove container: {}", stderr).into(),
            ));
        }

        Ok(())
    }

    /// Create and start the persistent container
    async fn create_container(&self) -> Result<(), TypeCheckError> {
        verbose!(
            "Creating persistent container: {} (image: {})",
            CONTAINER_NAME,
            self.image
        );

        let output = Command::new("docker")
            .args([
                "run",
                "-d",
                "--name",
                CONTAINER_NAME,
                "-p",
                &format!("{}:6875", CONTAINER_PORT),
                &self.image,
            ])
            .output()
            .await
            .map_err(|e| TypeCheckError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(TypeCheckError::ContainerStartFailed(
                format!("Failed to create container: {}", stderr).into(),
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
                format!("Failed to start container: {}", stderr).into(),
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
            "Container failed to become healthy within 30 seconds".into(),
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

    /// Create temporary tables for external dependencies
    async fn create_external_dependencies(
        &self,
        client: &mut Client,
        project: &Project,
        types: &Types,
    ) -> Result<(), TypeCheckError> {
        for ext_dep in &project.external_dependencies {
            // Get column definitions from types.lock
            let fqn = ext_dep.to_string();
            let columns = types.objects.get(&fqn).ok_or_else(|| {
                TypeCheckError::ExternalDependencyFailed {
                    object: ext_dep.clone(),
                    source: format!("external dependency '{}' not found in types.lock", fqn).into(),
                }
            })?;

            let mut col_defs = Vec::new();
            for (col_name, col_type) in columns {
                let nullable = if col_type.nullable { "" } else { " NOT NULL" };
                col_defs.push(format!("{} {}{}", col_name, col_type.r#type, nullable));
            }

            let create_sql = format!(
                "CREATE TEMPORARY TABLE {}_{}_{} ({})",
                ext_dep.database,
                ext_dep.schema,
                ext_dep.object,
                col_defs.join(", ")
            );

            verbose!("Creating external dependency table: {}", ext_dep);
            client.execute(&create_sql, &[]).await.map_err(|e| {
                TypeCheckError::ExternalDependencyFailed {
                    object: ext_dep.clone(),
                    source: Box::new(e),
                }
            })?;
        }

        Ok(())
    }
}

impl Default for DockerRuntime {
    fn default() -> Self {
        Self::new()
    }
}
