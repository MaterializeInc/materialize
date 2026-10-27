// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Docker runtime for managing a persistent Materialize container.
//!
//! Provides a connected client backed by a long-lived `materialize/materialized`
//! container. Used by the `test` command to execute unit tests against a real
//! Materialize instance.

use crate::client::{Client, ConnectionError, Profile};
use crate::config::default_docker_image;
use crate::verbose;
use thiserror::Error;
use tokio::process::Command;
use tokio::time::{Duration, sleep};

/// Errors raised while starting or connecting to the Materialize container.
#[derive(Debug, Error)]
pub(crate) enum DockerRuntimeError {
    #[error("failed to start Materialize container: {0}")]
    ContainerStartFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("failed to connect to Materialize: {0}")]
    ConnectionFailed(#[from] ConnectionError),
}

/// Possible states of Docker availability on the host system.
pub(crate) enum DockerStatus {
    Running,
    NotRunning,
    NotInstalled,
}

/// Name of the persistent Docker container used by `test` and `explain`.
const CONTAINER_NAME: &str = "mz-deploy-sandbox";

/// Host port to bind for the persistent container.
const CONTAINER_PORT: u16 = 16875;

/// Manages the Materialize container used for runtime validation.
pub(crate) struct DockerRuntime {
    image: String,
}

impl DockerRuntime {
    pub(crate) async fn check_availability() -> DockerStatus {
        let result = Command::new("docker")
            .arg("info")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .await;

        match result {
            Ok(status) if status.success() => DockerStatus::Running,
            Ok(_) => DockerStatus::NotRunning,
            Err(_) => DockerStatus::NotInstalled,
        }
    }

    pub(crate) fn new() -> Self {
        Self {
            image: default_docker_image(),
        }
    }

    pub(crate) fn with_image(mut self, image: impl Into<String>) -> Self {
        self.image = image.into();
        self
    }

    pub(crate) async fn get_client(&self) -> Result<Client, DockerRuntimeError> {
        let profile = Self::make_profile();
        // The ephemeral Docker container has no `_mz_deploy_server` cluster,
        // so bypass the usual session-cluster pin and use the server default.
        let client = match Client::connect_with_profile_no_pin(profile).await {
            Ok(client) => {
                verbose!("Fast-path connect succeeded");
                client
            }
            Err(_) => {
                verbose!("Fast-path connect failed, falling back to Docker CLI");
                self.ensure_container().await?;
                let profile = Self::make_profile();
                verbose!("Connecting to Materialize...");
                let client = Client::connect_with_profile_no_pin(profile).await?;
                verbose!("Connected");
                client
            }
        };

        Ok(client)
    }

    fn make_profile() -> Profile {
        Profile {
            name: "docker-sandbox".to_string(),
            host: Some("localhost".to_string()),
            port: CONTAINER_PORT,
            username: "materialize".to_string(),
            password: None,
            options: Default::default(),
            sslmode: None,
            sslrootcert: None,
            http_host: None,
        }
    }

    async fn container_exists(&self) -> Result<bool, DockerRuntimeError> {
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
            .map_err(|e| DockerRuntimeError::ContainerStartFailed(Box::new(e)))?;

        Ok(output.status.success() && !output.stdout.is_empty())
    }

    async fn container_is_running(&self) -> Result<bool, DockerRuntimeError> {
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
            .map_err(|e| DockerRuntimeError::ContainerStartFailed(Box::new(e)))?;

        Ok(output.status.success() && !output.stdout.is_empty())
    }

    async fn container_is_healthy(&self) -> bool {
        Client::connect_with_profile_no_pin(Self::make_profile())
            .await
            .is_ok()
    }

    async fn remove_container(&self) -> Result<(), DockerRuntimeError> {
        verbose!("Removing existing container: {}", CONTAINER_NAME);
        let output = Command::new("docker")
            .args(["rm", "-f", CONTAINER_NAME])
            .output()
            .await
            .map_err(|e| DockerRuntimeError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(DockerRuntimeError::ContainerStartFailed(
                format!("Failed to remove container: {}", stderr).into(),
            ));
        }
        Ok(())
    }

    async fn create_container(&self) -> Result<(), DockerRuntimeError> {
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
                "-e",
                "MZ_EAT_MY_DATA=1",
                "-p",
                &format!("{}:6875", CONTAINER_PORT),
                &self.image,
                "--system-parameter-default=max_tables=10000",
                "--system-parameter-default=max_objects_per_schema=10000",
            ])
            .output()
            .await
            .map_err(|e| DockerRuntimeError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(DockerRuntimeError::ContainerStartFailed(
                format!("Failed to create container: {}", stderr).into(),
            ));
        }
        Ok(())
    }

    async fn start_existing_container(&self) -> Result<(), DockerRuntimeError> {
        verbose!("Starting existing container: {}", CONTAINER_NAME);

        let output = Command::new("docker")
            .args(["start", CONTAINER_NAME])
            .output()
            .await
            .map_err(|e| DockerRuntimeError::ContainerStartFailed(Box::new(e)))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(DockerRuntimeError::ContainerStartFailed(
                format!("Failed to start container: {}", stderr).into(),
            ));
        }
        Ok(())
    }

    async fn wait_for_container(&self) -> Result<(), DockerRuntimeError> {
        verbose!("Waiting for container to be ready...");
        for i in 0..30 {
            if self.container_is_healthy().await {
                verbose!("Container is ready!");
                return Ok(());
            }
            if i < 29 {
                sleep(Duration::from_secs(1)).await;
            }
        }
        Err(DockerRuntimeError::ContainerStartFailed(
            "Container failed to become healthy within 30 seconds".into(),
        ))
    }

    async fn ensure_container(&self) -> Result<(), DockerRuntimeError> {
        let exists = self.container_exists().await?;
        let is_running = if exists {
            self.container_is_running().await?
        } else {
            false
        };

        if is_running {
            verbose!("Found running container: {}", CONTAINER_NAME);
            if self.container_is_healthy().await {
                verbose!("Container is healthy, reusing it");
                return Ok(());
            } else {
                verbose!("Container is unhealthy, recreating it");
                self.remove_container().await?;
            }
        } else if exists {
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

        self.create_container().await?;
        self.wait_for_container().await?;
        Ok(())
    }
}

impl Default for DockerRuntime {
    fn default() -> Self {
        Self::new()
    }
}
