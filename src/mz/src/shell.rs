// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::region::get_provider_region_environment;
use crate::utils::CloudProviderRegion;
use crate::{Environment, ValidProfile};
use anyhow::{Context, Result};
use reqwest::Client;
use subprocess::Exec;

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
fn run_psql_shell(valid_profile: ValidProfile, environment: &Environment) {
    let (host, port) = parse_pgwire(environment);
    let email = valid_profile.profile.email.clone();

    let output = Exec::cmd("psql")
        .arg("-U")
        .arg(email)
        .arg("-h")
        .arg(host)
        .arg("-p")
        .arg(port)
        .arg("materialize")
        .env("PGPASSWORD", valid_profile.profile.app_password)
        .join()
        .expect("failed to execute process");

    assert!(output.success());
}

/// Runs pg_isready to check if an environment is healthy
pub(crate) fn check_environment_health(
    valid_profile: ValidProfile,
    environment: &Environment,
) -> bool {
    let (host, port) = parse_pgwire(environment);
    let email = valid_profile.profile.email.clone();

    let output = Exec::cmd("pg_isready")
        .arg("-U")
        .arg(email)
        .arg("-h")
        .arg(host)
        .arg("-p")
        .arg(port)
        .env("PGPASSWORD", valid_profile.profile.app_password)
        .arg("-d")
        .arg("materialize")
        .arg("-q")
        .join()
        .unwrap();

    output.success()
}

/// Command to run a shell (psql) on a Materialize cloud instance
pub(crate) async fn shell(
    client: Client,
    valid_profile: ValidProfile,
    cloud_provider_region: CloudProviderRegion,
) -> Result<()> {
    let environment =
        get_provider_region_environment(&client, &valid_profile, &cloud_provider_region)
            .await
            .with_context(|| "Retrieving cloud provider region.")?;

    run_psql_shell(valid_profile, &environment);

    Ok(())
}
