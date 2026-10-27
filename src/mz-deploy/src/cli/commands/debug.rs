// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Debug command - test database connection.

use crate::cli::CliError;
use crate::client::{Client, SERVER_CLUSTER_NAME};
use crate::config::Settings;
use crate::docker_runtime::{DockerRuntime, DockerStatus};
use crate::log;
use owo_colors::{OwoColorize, Stream};
use std::fmt;

/// Health of the `_mz_deploy_server` cluster as observed by `debug`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ServerClusterHealth {
    /// Cluster exists and has replication_factor > 0.
    Healthy,
    /// Cluster exists but is not usable (e.g., replication_factor == 0).
    NotReady { reason: String },
    /// Cluster is not present in `mz_catalog.mz_clusters`.
    Missing,
}

async fn check_server_cluster(client: &Client) -> Result<ServerClusterHealth, CliError> {
    match client
        .introspection()
        .get_cluster(SERVER_CLUSTER_NAME)
        .await?
    {
        None => Ok(ServerClusterHealth::Missing),
        Some(c) if c.replication_factor.unwrap_or(0) > 0 => Ok(ServerClusterHealth::Healthy),
        Some(_) => Ok(ServerClusterHealth::NotReady {
            reason: "replication factor is 0".into(),
        }),
    }
}

#[derive(serde::Serialize)]
enum RemoteOutput {
    Success {
        environment_id: String,
        server_cluster_health: ServerClusterHealth,
    },

    Failure {
        host: String,
        port: u16,
    },
}

#[derive(serde::Serialize)]
struct DebugOutput {
    profile: String,
    docker_status: String,
    remote: RemoteOutput,
}

impl fmt::Display for DebugOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}: {}",
            "Profile".if_supports_color(Stream::Stderr, |t| t.green()),
            self.profile.if_supports_color(Stream::Stderr, |t| t.cyan())
        )?;
        let docker_label = match self.docker_status.as_str() {
            "running" => format!(
                "{}: {}",
                "Docker".if_supports_color(Stream::Stderr, |t| t.green()),
                "daemon running".if_supports_color(Stream::Stderr, |t| t.green())
            ),
            "not_running" => format!(
                "{}: {}",
                "Docker".if_supports_color(Stream::Stderr, |t| t.green()),
                "daemon not running".if_supports_color(Stream::Stderr, |t| t.yellow())
            ),
            _ => format!(
                "{}: {}",
                "Docker".if_supports_color(Stream::Stderr, |t| t.green()),
                "not installed".if_supports_color(Stream::Stderr, |t| t.yellow())
            ),
        };
        writeln!(f, "{}", docker_label)?;

        match &self.remote {
            RemoteOutput::Success {
                environment_id,
                server_cluster_health,
            } => {
                writeln!(
                    f,
                    "{}: {}",
                    "Environment".if_supports_color(Stream::Stderr, |t| t.green()),
                    environment_id.if_supports_color(Stream::Stderr, |t| t.cyan())
                )?;
                let cluster_line = match server_cluster_health {
                    ServerClusterHealth::Healthy => format!(
                        "{}: {} ({})",
                        "Server cluster".if_supports_color(Stream::Stderr, |t| t.green()),
                        SERVER_CLUSTER_NAME.if_supports_color(Stream::Stderr, |t| t.cyan()),
                        "healthy"
                    ),
                    ServerClusterHealth::NotReady { reason } => format!(
                        "{}: {} ({}: {})\n  hint: run `mz-deploy setup`",
                        "Server cluster".if_supports_color(Stream::Stderr, |t| t.green()),
                        SERVER_CLUSTER_NAME.if_supports_color(Stream::Stderr, |t| t.cyan()),
                        "not ready".if_supports_color(Stream::Stderr, |t| t.yellow()),
                        reason,
                    ),
                    ServerClusterHealth::Missing => format!(
                        "{}: {} ({})\n  hint: run `mz-deploy setup`",
                        "Server cluster".if_supports_color(Stream::Stderr, |t| t.green()),
                        SERVER_CLUSTER_NAME.if_supports_color(Stream::Stderr, |t| t.cyan()),
                        "missing".if_supports_color(Stream::Stderr, |t| t.red()),
                    ),
                };
                write!(f, "{}", cluster_line)?;
            }
            RemoteOutput::Failure { host, port } => {
                write!(
                    f,
                    "{} {}:{}",
                    "Failed to connect to".if_supports_color(Stream::Stderr, |t| t.red()),
                    host.if_supports_color(Stream::Stderr, |t| t.cyan()),
                    port.to_string()
                        .if_supports_color(Stream::Stderr, |t| t.cyan())
                )?;
            }
        }

        Ok(())
    }
}

/// Test database connection with the specified profile.
///
/// # Arguments
/// * `profile` - Database profile containing connection information
///
/// # Returns
/// Ok(()) if connection succeeds
///
/// # Errors
/// Returns `CliError::Connection` if connection fails
pub async fn run(settings: &Settings) -> Result<(), CliError> {
    let profile = settings.connection();
    let docker_status = DockerRuntime::check_availability().await;
    let docker_status_str = match docker_status {
        DockerStatus::Running => "running",
        DockerStatus::NotRunning => "not_running",
        DockerStatus::NotInstalled => "not_installed",
    };

    let client = Client::connect_with_profile(profile.clone()).await;

    let remote = match client {
        Ok(client) => {
            let (environment_id, cluster_result) =
                tokio::join!(query_session_info(&client), check_server_cluster(&client),);

            let environment_id = environment_id?;
            let server_cluster_health = cluster_result?;

            RemoteOutput::Success {
                environment_id,
                server_cluster_health,
            }
        }
        Err(_) => RemoteOutput::Failure {
            host: profile.require_host()?.to_string(),
            port: profile.port,
        },
    };

    let output = DebugOutput {
        profile: profile.name.clone(),
        docker_status: docker_status_str.to_string(),
        remote,
    };

    log::output(&output);

    Ok(())
}

async fn query_session_info(client: &Client) -> Result<String, CliError> {
    let row = client
        .query_one("SELECT mz_environment_id() AS environment_id", &[])
        .await?;

    let environment_id: String = row.get("environment_id");

    Ok(environment_id)
}
