// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::configuration::ValidProfile;
use crate::region::{get_provider_region_environment, CloudProviderRegion};
use crate::Environment;
use anyhow::{Context, Ok, Result};
use reqwest::Client;
use std::os::unix::process::CommandExt;
use std::process::Command;

/// ----------------------------
/// Shell command
/// ----------------------------

/// Parse host and port from the pgwire URL
pub(crate) fn parse_pgwire(envrionment: &Environment) -> (&str, &str) {
    let host = &envrionment.environmentd_pgwire_address
        [..envrionment.environmentd_pgwire_address.len() - 5];
    let port =
        &envrionment.environmentd_pgwire_address[envrionment.environmentd_pgwire_address.len() - 4
            ..envrionment.environmentd_pgwire_address.len()];

    (host, port)
}

/// Runs psql as a subprocess command
fn run_psql_shell(valid_profile: ValidProfile<'_>, environment: &Environment) -> Result<()> {
    let (host, port) = parse_pgwire(environment);

    let error = Command::new("psql")
        .arg("-U")
        .arg(valid_profile.profile.get_email())
        .arg("-h")
        .arg(host)
        .arg("-p")
        .arg(port)
        .arg("materialize")
        .env("PGPASSWORD", valid_profile.profile.get_app_password())
        .exec();

    Err(error).context("failed to spawn psql")
}

/// Runs pg_isready to check if an environment is healthy
pub(crate) fn check_environment_health(
    valid_profile: &ValidProfile<'_>,
    environment: &Environment,
) -> Result<bool> {
    let (host, port) = parse_pgwire(environment);

    let status = Command::new("pg_isready")
        .arg("-U")
        .arg(valid_profile.profile.get_email())
        .arg("-h")
        .arg(host)
        .arg("-p")
        .arg(port)
        .env("PGPASSWORD", valid_profile.profile.get_app_password())
        .arg("-d")
        .arg("materialize")
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
