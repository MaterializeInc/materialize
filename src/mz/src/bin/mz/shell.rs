// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::os::unix::process::CommandExt;
use std::process::Command;

use anyhow::{Context, Ok, Result};
use reqwest::Client;

use mz::api::{get_provider_region_environment, CloudProviderRegion, Environment};
use mz::configuration::ValidProfile;

/// ----------------------------
/// Shell command
/// ----------------------------

/// Runs psql as a subprocess command
fn run_psql_shell(valid_profile: ValidProfile<'_>, environment: &Environment) -> Result<()> {
    let error = Command::new("psql")
        .arg(environment.sql_url(&valid_profile).to_string())
        .env("PGPASSWORD", valid_profile.app_password)
        .exec();

    Err(error).context("failed to spawn psql")
}

/// Runs pg_isready to check if an environment is healthy
pub(crate) fn check_environment_health(
    valid_profile: &ValidProfile<'_>,
    environment: &Environment,
) -> Result<bool> {
    let status = Command::new("pg_isready")
        .arg(environment.sql_url(valid_profile).to_string())
        .env("PGPASSWORD", valid_profile.app_password.clone())
        .arg("-q")
        .output()
        .context("failed to execute pg_isready")?
        .status
        .success();

    Ok(status)
}

/// Command to run a shell (psql) on a Materialize cloud instance
pub(crate) async fn shell(
    client: Client,
    valid_profile: ValidProfile<'_>,
    cloud_provider_region: CloudProviderRegion,
) -> Result<()> {
    let environment =
        get_provider_region_environment(&client, &valid_profile, &cloud_provider_region)
            .await
            .context("Retrieving cloud provider region.")?;

    run_psql_shell(valid_profile, &environment)
}
