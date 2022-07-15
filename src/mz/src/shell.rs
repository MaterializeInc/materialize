use reqwest::{Client};
use subprocess::Exec;
use crate::{FronteggAuthMachine, Profile, Region};
use crate::regions::{list_cloud_providers, list_regions, cloud_provider_region_details};
use crate::utils::{trim_newline};
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

            // TODO: Very similar code for both cases
            if regions.len() > 0 {
                println!("Please, first select a default region [\"{:?}\"]", cloud_provider_str);

                let _ = std::io::stdout().flush();
                std::io::stdin().read_line(&mut region_input).unwrap();
                trim_newline(&mut region_input);

                println!("Region input: {:?}", region_input);

                // TODO: A map would be more efficient.
                let selected_cloud_provider_filtered = cloud_providers
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

                println!("Region details: {:?}", region);
                // TODO: Replace -U with user email
                // TODO: Control size check
                let host = &region.coordd_pgwire_address[..region.coordd_pgwire_address.len() - 5];
                let port = &region.coordd_pgwire_address[region.coordd_pgwire_address.len() - 4..region.coordd_pgwire_address.len()];

                let output = Exec::cmd("psql")
                    .arg("-U")
                    .arg("joaquin@materialize.com")
                    .arg("-h")
                    .arg(host)
                    .arg("-p")
                    .arg(port)
                    .arg("materialize")
                    .env("PGPASSWORD", password_from_profile(profile))
                    .join()
                    .expect("failed to execute process");

                println!("status: {}", output.success());

                assert!(output.success());
            } else {
                println!("There are no regions created. Please, create one to run the shell.");
            }
        },
        Err(error) => panic!("Error retrieving cloud providers: {:?}", error)
    }
}
