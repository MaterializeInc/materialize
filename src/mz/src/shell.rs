// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::regions::{cloud_provider_region_details, list_cloud_providers, list_regions};
use crate::utils::trim_newline;
use crate::{FronteggAuthMachine, Profile, Region};
use reqwest::Client;
use std::io::Write;
use std::process::exit;
use subprocess::Exec;

/// ----------------------------
/// Shell command
/// ----------------------------

/**
 ** Runs psql as a subprocess command
 **/
fn run_psql_shell(profile: Profile, region: Region) {
    let host = &region.environmentd_pgwire_address[..region.environmentd_pgwire_address.len() - 5];
    let port = &region.environmentd_pgwire_address
        [region.environmentd_pgwire_address.len() - 4..region.environmentd_pgwire_address.len()];
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

/**
 ** Turn a profile into a Materialize cloud instance password
 **/
fn password_from_profile(profile: Profile) -> String {
    "mzp_".to_owned() + &profile.client_id + &profile.secret
}

/**
 ** Command to run a shell (psql) on a Materialize cloud instance
 **/
pub(crate) async fn shell(
    client: Client,
    profile: Profile,
    frontegg_auth_machine: FronteggAuthMachine,
) {
    match list_cloud_providers(client.clone(), frontegg_auth_machine.clone()).await {
        Ok(cloud_providers) => {
            let regions = list_regions(
                cloud_providers.to_vec(),
                client.clone(),
                frontegg_auth_machine.clone(),
            )
            .await;

            let cloud_provider_str = cloud_providers
                .iter()
                .map(|cloud_provider| cloud_provider.region.clone())
                .collect::<Vec<String>>()
                .join(r#"", ""#);
            let mut region_input = String::new();

            // TODO: Very similar code for both cases
            if !regions.is_empty() {
                println!(
                    r#"Please, first select a region: ["{}"]"#,
                    cloud_provider_str
                );

                print!("Region: ");
                let _ = std::io::stdout().flush();
                std::io::stdin().read_line(&mut region_input).unwrap();
                trim_newline(&mut region_input);

                // TODO: A map would be more efficient.
                let selected_cloud_provider_filtered = cloud_providers
                    .into_iter()
                    .find(|cloud_provider| cloud_provider.region == region_input);

                match selected_cloud_provider_filtered {
                    Some(cloud_provider) => {
                        match cloud_provider_region_details(
                            client.clone(),
                            cloud_provider,
                            frontegg_auth_machine.clone(),
                        )
                        .await
                        {
                            Ok(Some(mut cloud_provider_regions)) => {
                                match cloud_provider_regions.pop() {
                                    Some(region) => run_psql_shell(profile, region),
                                    None => {
                                        println!("The region is not enabled.");
                                        exit(0);
                                    }
                                }
                            }
                            Err(error) => {
                                panic!("Error retrieving region details: {:?}", error);
                            }
                            _ => {}
                        }
                    }
                    None => {
                        println!("Unknown region.");
                    }
                }
            } else {
                println!("There are no regions created. Please, create one to run the shell.");
            }
        }
        Err(error) => panic!("Error retrieving cloud providers: {:?}", error),
    }
}
