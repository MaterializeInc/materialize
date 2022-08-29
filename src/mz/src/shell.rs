// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::regions::{cloud_provider_region_details, list_cloud_providers};
use crate::utils::{exit_with_fail_message, CloudProviderRegion};
use crate::{ExitMessage, FronteggAuthMachine, Profile, Region};
use reqwest::Client;
use std::process::exit;
use subprocess::Exec;

/// ----------------------------
/// Shell command
/// ----------------------------

/// Parse host and port from the pgwire URL
pub(crate) fn parse_pgwire(region: &Region) -> (&str, &str) {
    let host = &region.environmentd_pgwire_address[..region.environmentd_pgwire_address.len() - 5];
    let port = &region.environmentd_pgwire_address
        [region.environmentd_pgwire_address.len() - 4..region.environmentd_pgwire_address.len()];

    (host, port)
}

/// Runs psql as a subprocess command
fn run_psql_shell(profile: Profile, region: &Region) {
    let (host, port) = parse_pgwire(region);
    let email = profile.email.clone();

    let output = Exec::cmd("psql")
        .arg("-U")
        .arg(email)
        .arg("-h")
        .arg(host)
        .arg("-p")
        .arg(port)
        .arg("materialize")
        .env("PGPASSWORD", password_from_profile(profile))
        .join()
        .expect("failed to execute process");

    assert!(output.success());
}

/// Runs pg_isready to check if a region is healthy
pub(crate) fn check_region_health(profile: Profile, region: &Region) -> bool {
    let (host, port) = parse_pgwire(region);
    let email = profile.email.clone();

    let output = Exec::cmd("pg_isready")
        .arg("-U")
        .arg(email)
        .arg("-h")
        .arg(host)
        .arg("-p")
        .arg(port)
        .env("PGPASSWORD", password_from_profile(profile))
        .arg("-d")
        .arg("materialize")
        .arg("-q")
        .join()
        .unwrap();

    output.success()
}

/// Turn a profile into a Materialize cloud instance password
fn password_from_profile(profile: Profile) -> String {
    "mzp_".to_owned() + &profile.client_id + &profile.secret
}

/// Command to run a shell (psql) on a Materialize cloud instance
pub(crate) async fn shell(
    client: Client,
    profile: Profile,
    frontegg_auth_machine: FronteggAuthMachine,
    cloud_provider_region: CloudProviderRegion,
) {
    match list_cloud_providers(&client, &frontegg_auth_machine).await {
        Ok(cloud_providers) => {
            let region = cloud_provider_region;

            // TODO: A map would be more efficient.
            let selected_cloud_provider_filtered = cloud_providers
                .into_iter()
                .find(|cloud_provider| cloud_provider.region == region.region_name());

            match selected_cloud_provider_filtered {
                Some(cloud_provider) => {
                    match cloud_provider_region_details(
                        &client,
                        &cloud_provider,
                        &frontegg_auth_machine,
                    )
                    .await
                    {
                        Ok(Some(mut cloud_provider_regions)) => {
                            match cloud_provider_regions.pop() {
                                Some(region) => run_psql_shell(profile, &region),
                                None => {
                                    println!("The region is not enabled.");
                                    exit(0);
                                }
                            }
                        }
                        Err(error) => exit_with_fail_message(ExitMessage::String(format!(
                            "Error retrieving region details: {:?}",
                            error
                        ))),
                        _ => {}
                    }
                }
                None => exit_with_fail_message(ExitMessage::Str("Unknown region.")),
            }
        }
        Err(error) => exit_with_fail_message(ExitMessage::String(format!(
            "Error retrieving cloud providers: {:?}",
            error
        ))),
    }
}
