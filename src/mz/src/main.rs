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

extern crate core;

mod login;
mod profiles;
mod regions;
mod shell;
mod utils;

use std::str::FromStr;

use profiles::get_profile;
use regions::{print_region_enabled, print_region_status};
use serde::{Deserialize, Serialize};

use clap::{ArgEnum, Args, Parser, Subcommand};
use reqwest::Client;
use shell::check_region_health;
use utils::{exit_with_fail_message, run_loading_spinner, CloudProviderRegion};

use crate::login::{login_with_browser, login_with_console};
use crate::profiles::{authenticate_profile, validate_profile};
use crate::regions::{enable_region, list_cloud_providers, list_regions};
use crate::shell::shell;

/// Command-line interface for Materialize.
#[derive(Debug, Parser)]
#[clap(name = "Materialize CLI")]
#[clap(about = "Command-line interface for Materialize.", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
    #[clap(short, long, env = "MZ_PROFILE", default_value = "default")]
    profile: String,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Login to Materialize
    Login {
        /// Login by typing your email and password
        #[clap(short, long)]
        interactive: bool,
    },
    /// Enable or delete a region
    Regions(Regions),
    /// Connect to a region using a SQL shell
    Shell {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
}

#[derive(Debug, Args)]
struct Login {
    #[clap(short, long, arg_enum)]
    command: Option<LoginCommands>,
}

#[derive(Debug, Clone, ArgEnum)]
enum LoginCommands {
    /// Log in via the console using email and password.
    Interactive,
}

#[derive(Debug, Args)]
struct Regions {
    #[clap(subcommand)]
    command: RegionsCommands,
}

#[derive(Debug, Subcommand)]
enum RegionsCommands {
    /// Enable a new region.
    Enable {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
    /// List all enabled regions.
    List,
    /// Check the status of a region.
    Status {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
}

/// Internal types, struct and enums
#[derive(Debug, Deserialize, Clone)]
struct Region {
    environmentd_pgwire_address: String,
    environmentd_https_address: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct CloudProvider {
    region: String,
    environment_controller_url: String,
    provider: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FronteggAuthUser {
    access_token: String,
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct FronteggAuthMachine {
    access_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FronteggAPIToken {
    client_id: String,
    secret: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserAPIToken {
    email: String,
    client_id: String,
    secret: String,
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Profile {
    name: String,
    email: String,
    client_id: String,
    secret: String,
    region: Option<String>,
}

struct CloudProviderAndRegion {
    cloud_provider: CloudProvider,
    region: Option<Region>,
}

#[derive(Debug)]
enum ExitMessage {
    String(String),
    Str(&'static str),
}

/// Constants
const PROFILES_DIR_NAME: &str = ".config/mz";
const PROFILES_FILE_NAME: &str = "profiles.toml";
const CLOUD_PROVIDERS_URL: &str = "https://cloud.materialize.com/api/cloud-providers";
const API_TOKEN_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/identity/resources/users/api-tokens/v1";
const USER_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/frontegg/identity/resources/auth/v1/user";
const MACHINE_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/identity/resources/auth/v1/api-token";
const WEB_LOGIN_URL: &str = "https://cloud.materialize.com/account/login?redirectUrl=/access/cli";
const DEFAULT_PROFILE_NAME: &str = "default";
const PROFILES_PREFIX: &str = "profiles";
const ERROR_OPENING_PROFILES_MESSAGE: &str = "Error opening the profiles file";
const ERROR_PARSING_PROFILES_MESSAGE: &str = "Error parsing the profiles";
const ERROR_AUTHENTICATING_PROFILE_MESSAGE: &str = "Error authenticating profile";
const PROFILE_NOT_FOUND_MESSAGE: &str =
    "Profile not found. Please, add one or login using `mz login`.";
const ERROR_UNKNOWN_REGION: &str = "Unknown region";

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let profile_name = args.profile;

    match args.command {
        Commands::Login { interactive } => {
            if interactive {
                login_with_console(&profile_name).await.unwrap()
            } else {
                login_with_browser(&profile_name).await.unwrap()
            }
        }

        Commands::Regions(regions_cmd) => {
            let client = Client::new();

            match regions_cmd.command {
                RegionsCommands::Enable {
                    cloud_provider_region,
                } => match validate_profile(profile_name, &client).await {
                    Some(frontegg_auth_machine) => {
                        let loading_spinner = run_loading_spinner("Enabling region...".to_string());
                        if let Ok(parsed_cloud_provider_region) =
                            CloudProviderRegion::from_str(&cloud_provider_region)
                        {
                            match enable_region(
                                client,
                                parsed_cloud_provider_region,
                                frontegg_auth_machine,
                            )
                            .await
                            {
                                Ok(_) => loading_spinner.finish_with_message("Region enabled."),
                                Err(e) => exit_with_fail_message(ExitMessage::String(format!(
                                    "Error enabling region: {:?}",
                                    e
                                ))),
                            }
                        } else {
                            exit_with_fail_message(ExitMessage::Str(ERROR_UNKNOWN_REGION))
                        }
                    }
                    None => {}
                },
                RegionsCommands::List => match validate_profile(profile_name, &client).await {
                    Some(frontegg_auth_machine) => {
                        match list_cloud_providers(&client, &frontegg_auth_machine).await {
                            Ok(cloud_providers) => {
                                let cloud_providers_and_regions =
                                    list_regions(&cloud_providers, &client, &frontegg_auth_machine)
                                        .await;
                                cloud_providers_and_regions.iter().for_each(
                                    |cloud_provider_and_region| {
                                        print_region_enabled(cloud_provider_and_region);
                                    },
                                );
                            }
                            Err(error) => exit_with_fail_message(ExitMessage::String(format!(
                                "Error retrieving cloud providers: {:?}",
                                error
                            ))),
                        }
                    }
                    None => {}
                },
                RegionsCommands::Status {
                    cloud_provider_region,
                } => match get_profile(profile_name) {
                    Some(profile) => {
                        let client = Client::new();
                        match authenticate_profile(&client, &profile).await {
                            Ok(frontegg_auth_machine) => {
                                match list_cloud_providers(&client, &frontegg_auth_machine).await {
                                    Ok(cloud_providers) => {
                                        if let Ok(parsed_cloud_provider_region) =
                                            CloudProviderRegion::from_str(&cloud_provider_region)
                                        {
                                            let filtered_providers: Vec<CloudProvider> =
                                                cloud_providers
                                                    .into_iter()
                                                    .filter(|provider| {
                                                        provider.region
                                                            == parsed_cloud_provider_region
                                                                .region_name()
                                                    })
                                                    .collect::<Vec<CloudProvider>>();

                                            let mut cloud_providers_and_regions = list_regions(
                                                &filtered_providers,
                                                &client,
                                                &frontegg_auth_machine,
                                            )
                                            .await;

                                            match cloud_providers_and_regions.pop() {
                                                Some(cloud_provider_and_region) => {
                                                    if let Some(region) =
                                                        cloud_provider_and_region.region
                                                    {
                                                        let health =
                                                            check_region_health(profile, &region);
                                                        print_region_status(region, health);
                                                    } else {
                                                        exit_with_fail_message(ExitMessage::Str(
                                                            "Region unavailable.",
                                                        ));
                                                    }
                                                }
                                                None => exit_with_fail_message(ExitMessage::Str(
                                                    "Error. Missing provider.",
                                                )),
                                            }
                                        } else {
                                            exit_with_fail_message(ExitMessage::Str(
                                                ERROR_UNKNOWN_REGION,
                                            ))
                                        }
                                    }
                                    Err(error) => exit_with_fail_message(ExitMessage::String(
                                        format!("Error listing providers: {:}", error),
                                    )),
                                }
                            }
                            Err(error) => exit_with_fail_message(ExitMessage::String(format!(
                                "{}: {:}",
                                ERROR_AUTHENTICATING_PROFILE_MESSAGE, error
                            ))),
                        }
                    }
                    None => exit_with_fail_message(ExitMessage::Str(PROFILE_NOT_FOUND_MESSAGE)),
                },
                // ------------------------------------------------------------------------
                // Delete is currently disabled. Preserving the code for once is available.
                // ------------------------------------------------------------------------
                // RegionsCommands::Delete {
                //     cloud_provider_region,
                // } => {
                // if warning_delete_region(cloud_provider_region.clone()) {
                //     match validate_profile(profile_arg, client.clone()).await {
                //         Some(frontegg_auth_machine) => {
                //             let loading_spinner = run_loading_spinner("Deleting region...".to_string());
                //             match delete_region(
                //                 client.clone(),
                //                 cloud_provider_region,
                //                 frontegg_auth_machine,
                //             )
                //             .await
                //             {
                //                 Ok(_) => loading_spinner.finish_with_message("Region deleted."),
                //                 Err(e) => panic!("Error deleting region: {:?}", e),
                //             }
                //         }
                //         None => {}
                //     }
                // }
                // }
            }
        }

        Commands::Shell {
            cloud_provider_region,
        } => {
            if let Ok(parsed_cloud_provider_region) =
                CloudProviderRegion::from_str(&cloud_provider_region)
            {
                match get_profile(profile_name) {
                    Some(profile) => {
                        let client = Client::new();
                        match authenticate_profile(&client, &profile).await {
                            Ok(frontegg_auth_machine) => {
                                shell(
                                    client,
                                    profile,
                                    frontegg_auth_machine,
                                    parsed_cloud_provider_region,
                                )
                                .await
                            }
                            Err(error) => exit_with_fail_message(ExitMessage::String(format!(
                                "{}: {:}",
                                ERROR_AUTHENTICATING_PROFILE_MESSAGE, error
                            ))),
                        }
                    }
                    None => exit_with_fail_message(ExitMessage::Str(PROFILE_NOT_FOUND_MESSAGE)),
                };
            } else {
                exit_with_fail_message(ExitMessage::Str(ERROR_UNKNOWN_REGION))
            }
        }
    }
}
