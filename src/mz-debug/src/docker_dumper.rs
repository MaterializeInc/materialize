// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Dumps Docker resources to files.

use std::fs::{File, create_dir_all};
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context as AnyhowContext;
use mz_ore::retry::{self, RetryResult};
use tracing::{info, warn};

use crate::{ContainerDumper, Context};

static DOCKER_DUMP_DIR: &str = "docker";
static DOCKER_RESOURCE_DUMP_TIMEOUT: Duration = Duration::from_secs(30);

pub struct DockerDumper {
    container_id: String,
    directory_path: PathBuf,
}

impl DockerDumper {
    pub fn new(context: &Context, container_id: String) -> Self {
        Self {
            directory_path: context.base_path.join(DOCKER_DUMP_DIR).join(&container_id),
            container_id,
        }
    }

    /// Execute a Docker command and return (stdout, stderr).
    async fn execute_docker_command(
        &self,
        args: &[String],
    ) -> Result<(Vec<u8>, Vec<u8>), anyhow::Error> {
        retry::Retry::default()
            .max_duration(DOCKER_RESOURCE_DUMP_TIMEOUT)
            .retry_async(|_| {
                let args = args.to_vec();
                async move {
                    let output = tokio::process::Command::new("docker")
                        .args(&args)
                        .output()
                        .await;

                    match output {
                        Ok(output) if output.status.success() => {
                            RetryResult::Ok((output.stdout, output.stderr))
                        }
                        Ok(output) => {
                            let err_msg = format!(
                                "Docker command failed: {:#}. Retrying...",
                                String::from_utf8_lossy(&output.stderr)
                            );
                            warn!("{}", err_msg);
                            RetryResult::RetryableErr(anyhow::anyhow!(err_msg))
                        }
                        Err(err) => {
                            let err_msg = format!("Failed to execute Docker command: {:#}", err);
                            warn!("{}", err_msg);
                            RetryResult::RetryableErr(anyhow::anyhow!(err_msg))
                        }
                    }
                }
            })
            .await
    }

    async fn dump_logs(&self) -> Result<(), anyhow::Error> {
        let (stdout, stderr) = self
            .execute_docker_command(&["logs".to_string(), self.container_id.to_string()])
            .await?;

        write_output(stdout, &self.directory_path, "logs-stdout.txt")?;
        write_output(stderr, &self.directory_path, "logs-stderr.txt")?;

        Ok(())
    }

    async fn dump_inspect(&self) -> Result<(), anyhow::Error> {
        let (stdout, _) = self
            .execute_docker_command(&["inspect".to_string(), self.container_id.to_string()])
            .await?;

        write_output(stdout, &self.directory_path, "inspect.txt")?;

        Ok(())
    }

    async fn dump_stats(&self) -> Result<(), anyhow::Error> {
        let (stdout, _) = self
            .execute_docker_command(&[
                "stats".to_string(),
                "--no-stream".to_string(),
                self.container_id.to_string(),
            ])
            .await?;

        write_output(stdout, &self.directory_path, "stats.txt")?;

        Ok(())
    }

    async fn dump_top(&self) -> Result<(), anyhow::Error> {
        let (stdout, _) = self
            .execute_docker_command(&["top".to_string(), self.container_id.to_string()])
            .await?;

        write_output(stdout, &self.directory_path, "top.txt")?;

        Ok(())
    }
}

impl ContainerDumper for DockerDumper {
    async fn dump_container_resources(&self) {
        let _ = self.dump_logs().await;
        let _ = self.dump_inspect().await;
        let _ = self.dump_stats().await;
        let _ = self.dump_top().await;
    }
}

/// Helper closure to write output to file
fn write_output(
    output: Vec<u8>,
    directory_path: &PathBuf,
    file_name: &str,
) -> Result<(), anyhow::Error> {
    create_dir_all(&directory_path)?;
    let file_path = directory_path.join(file_name);
    let mut file = File::create(&file_path)?;
    file.write_all(&output)?;
    info!("Exported {}", file_path.display());
    Ok(())
}

/// Gets the IP address of a Docker container using the container ID.
pub async fn get_container_ip(container_id: &str) -> Result<String, anyhow::Error> {
    let output = tokio::process::Command::new("docker")
        .args([
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            container_id,
        ])
        .output()
        .await
        .with_context(|| format!("Failed to get container IP address for {}", container_id))?;

    if !output.status.success() {
        return Err(anyhow::anyhow!(
            "Docker command failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let ip = String::from_utf8(output.stdout)
        .with_context(|| "Failed to convert container IP address to string")?
        .trim()
        .to_string();
    if ip.is_empty() {
        return Err(anyhow::anyhow!("Container IP address not found"));
    }

    Ok(ip)
}
