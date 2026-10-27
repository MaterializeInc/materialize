// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Describe command - show detailed information about a specific deployment.

use crate::cli::CliError;
use crate::client::{Client, DeploymentDetails, DeploymentKind};
use crate::config::Settings;
use crate::log;
use crate::project::ir::object_id::ObjectId;
use chrono::{DateTime, Local};
use owo_colors::{OwoColorize, Stream, Style};
use std::collections::BTreeMap;
use std::fmt;

#[derive(serde::Serialize)]
struct DescribeOutput {
    deploy_id: String,
    details: DeploymentDetails,
    objects: BTreeMap<ObjectId, String>,
}

impl fmt::Display for DescribeOutput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let deployment_style = Style::new().yellow().bold();
        writeln!(
            f,
            "{} {} [{}]",
            "deployment".if_supports_color(Stream::Stderr, |t| deployment_style.style(t)),
            self.deploy_id
                .if_supports_color(Stream::Stderr, |t| t.cyan()),
            self.details
                .kind
                .to_string()
                .if_supports_color(Stream::Stderr, |t| t.dimmed()),
        )?;

        if let Some(commit_sha) = &self.details.git_commit {
            writeln!(
                f,
                "{}: {}",
                "Commit".if_supports_color(Stream::Stderr, |t| t.dimmed()),
                commit_sha
            )?;
        }

        writeln!(
            f,
            "{}: {}",
            "Deployed by".if_supports_color(Stream::Stderr, |t| t.dimmed()),
            self.details
                .deployed_by
                .if_supports_color(Stream::Stderr, |t| t.yellow())
        )?;

        let deployed_datetime: DateTime<Local> = self.details.deployed_at.with_timezone(&Local);
        let deployed_str = deployed_datetime
            .format("%a %b %d %H:%M:%S %Y %z")
            .to_string();
        writeln!(
            f,
            "{}: {}",
            "Deployed at".if_supports_color(Stream::Stderr, |t| t.dimmed()),
            deployed_str
        )?;

        if let Some(promoted) = self.details.promoted_at {
            if self.details.kind == DeploymentKind::Objects {
                let promoted_datetime: DateTime<Local> = promoted.with_timezone(&Local);
                let promoted_str = promoted_datetime
                    .format("%a %b %d %H:%M:%S %Y %z")
                    .to_string();
                writeln!(
                    f,
                    "{}: {}",
                    "Promoted at".if_supports_color(Stream::Stderr, |t| t.dimmed()),
                    promoted_str
                )?;
            }
        } else {
            writeln!(
                f,
                "{}: {}",
                "Status".if_supports_color(Stream::Stderr, |t| t.dimmed()),
                "staging".if_supports_color(Stream::Stderr, |t| t.yellow())
            )?;
        }

        writeln!(f)?;

        // Display schemas
        writeln!(
            f,
            "{} ({}):",
            "Schemas".if_supports_color(Stream::Stderr, |t| t.bold()),
            self.details.schemas.len()
        )?;
        for sq in &self.details.schemas {
            writeln!(
                f,
                "    {}.{}",
                sq.database
                    .if_supports_color(Stream::Stderr, |t| t.dimmed()),
                sq.schema
            )?;
        }
        writeln!(f)?;

        // Display objects
        writeln!(
            f,
            "{} ({}):",
            "Objects".if_supports_color(Stream::Stderr, |t| t.bold()),
            self.objects.len()
        )?;
        for (object_id, hash) in &self.objects {
            let short_hash = &hash[..hash.len().min(12)];
            writeln!(
                f,
                "    {}  {}",
                object_id
                    .to_string()
                    .if_supports_color(Stream::Stderr, |t| t.dimmed()),
                short_hash.if_supports_color(Stream::Stderr, |t| t.dimmed())
            )?;
        }

        Ok(())
    }
}

/// Show detailed information about a specific deployment.
///
/// This command:
/// - Queries deployment metadata (when deployed, by whom, git commit, etc.)
/// - Lists all objects included in the deployment with their hashes
///
/// Use `mz-deploy history` to see a list of deployment IDs, then use this
/// command to drill into a specific deployment's details.
///
/// # Arguments
/// * `settings` - Resolved CLI settings (profile, project directory, etc.)
/// * `deploy_id` - The deployment ID to describe
///
/// # Returns
/// Ok(()) if the deployment is found and displayed
///
/// # Errors
/// Returns `CliError::Connection` for database errors
/// Returns `CliError::Message` if deployment is not found
pub async fn run(settings: &Settings, deploy_id: &str) -> Result<(), CliError> {
    let profile = settings.connection();
    let client = Client::connect_with_profile(profile.clone())
        .await
        .map_err(CliError::Connection)?;

    super::setup::verify(&client, settings.emulator()).await?;
    super::setup::validate_connection(&client, settings.emulator()).await?;

    // Get deployment metadata
    let details = client
        .deployments()
        .get_deployment_details(deploy_id)
        .await?;
    let Some(details) = details else {
        return Err(CliError::Message(format!(
            "Deployment '{}' not found",
            deploy_id
        )));
    };

    // Get deployment objects
    let snapshot = client
        .deployments()
        .get_deployment_objects(Some(deploy_id))
        .await?;

    let output = DescribeOutput {
        deploy_id: deploy_id.to_string(),
        details,
        objects: snapshot.objects,
    };
    log::output(&output);

    Ok(())
}
