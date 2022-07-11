use std::future::Future;
use reqwest::{Client, Error};
use crate::{CloudProvider, CloudProviderRegion, FronteggAuthMachine, Profile, Region};
use crate::regions::{list_cloud_providers, list_regions, enable_region, cloud_provider_region_details};
use crate::utils::{trim_newline};
use std::process::{Command, exit, Output};
use std::io::Write;

/// ----------------------------
/// Shell command
/// ----------------------------

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
    frontegg_auth_machine: FronteggAuthMachine
) -> () {
    match list_cloud_providers(
        client.clone(),
        frontegg_auth_machine.clone()
    ).await {
        Ok(cloud_providers) => {
            println!("Listing regions.");
            let regions = list_regions(
                cloud_providers.to_vec(),
                client.clone(),
                frontegg_auth_machine.clone()
            ).await;

            let cloud_provider_str = cloud_providers
                .iter()
                .map(|cloud_provider| cloud_provider.region.clone())
                .collect::<Vec<String>>()
                .join("\", \"");
            let mut region: Region = Region { coordd_pgwire_address: "".to_string(), coordd_https_address: "".to_string() };
            let mut region_input = String::new();
            let mut cloud_provider: CloudProviderRegion;

            // TODO: Very similar code for both cases
            if regions.len() > 0 {
                println!("Please, first select a default region [\"{:?}\"]", cloud_provider_str);

                let _ = std::io::stdout().flush();
                std::io::stdin().read_line(&mut region_input).unwrap();
                trim_newline(&mut region_input);

                println!("Region input: {:?}", region_input);

                if region_input == "eu-west-1" {
                    cloud_provider = CloudProviderRegion::euWest_1;
                } else if region_input == "us-east-1" {
                    cloud_provider = CloudProviderRegion::usEast_1;
                } else {
                    println!("Invalid region name.");
                    exit(0);
                }

                // TODO: A map would be more efficient.
                let mut selected_cloud_provider_filtered = cloud_providers
                    .into_iter()
                    .find(| cloud_provider| cloud_provider.region == region_input);

                match selected_cloud_provider_filtered {
                    Some(cloud_provider) => {
                        match cloud_provider_region_details(
                            client.clone(),
                            cloud_provider,
                            frontegg_auth_machine.clone()
                        ).await {
                            Ok(Some(mut cloud_provider_regions)) => {
                                match cloud_provider_regions.pop() {
                                    Some(region_details) => region = region_details,
                                    None => {}
                                }
                            }
                            Err(error) => { panic!("Error retrieving region details: {:?}", error); }
                            _ => {}
                        }
                    }
                    None => {}
                }
            } else {
                println!("There are no regions created. Please, type the default region you wish to create [{:?}]",
                         cloud_provider_str
                );

                let _ = std::io::stdout().flush();
                std::io::stdin().read_line(&mut region_input).unwrap();
                trim_newline(&mut region_input);

                println!("Region input: {:?}", region_input);

                if region_input == "eu-west-1" {
                    cloud_provider = CloudProviderRegion::euWest_1;
                } else if region_input == "us-east-1" {
                    cloud_provider = CloudProviderRegion::usEast_1;
                } else {
                    println!("Invalid region name.");
                    exit(0);
                }

                match enable_region(
                    client.clone(),
                    cloud_provider,
                    frontegg_auth_machine
                ).await {
                    Ok(new_region) => {
                        println!("Region enabled: {:?}", new_region);
                        region = new_region;
                    },
                    Err(error) => panic!("Error enabling region: {:?}", error),
                }
            }

            println!("Region details: {:?}", region);
            // TODO: Replace -U with user email
            // TODO: Control size check
            let host = &region.coordd_pgwire_address[..region.coordd_pgwire_address.len() - 5];
            let port = &region.coordd_pgwire_address[region.coordd_pgwire_address.len() - 4..region.coordd_pgwire_address.len()];
            use std::process::Command;
            let output = Command::new("psql")
                .arg("-U")
                .arg("joaquin@materialize.com")
                .arg("-h")
                .arg(host)
                .arg("-p")
                .arg(port)
                .arg("materialize")
                .env("PGPASSWORD", password_from_profile(profile))
                .output()
                .expect("failed to execute process");

            println!("status: {}", output.status);
            println!("stdout: {}", String::from_utf8_lossy(&output.stdout));
            println!("stderr: {}", String::from_utf8_lossy(&output.stderr));

            assert!(output.status.success());
        },
        Err(error) => panic!("Error retrieving cloud providers: {:?}", error)
    }
}
