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

use anyhow::{Context, Result};
use profiles::get_profile;
use regions::{print_environment_status, print_region_enabled, region_environment_details};
use serde::{Deserialize, Serialize};

use clap::{Args, Parser, Subcommand};
use reqwest::Client;
use shell::check_environment_health;
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
    /// Specify a particular profile
    #[clap(short, long, env = "MZ_PROFILE", default_value = "default")]
    profile: String,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Open the docs
    Docs,
    /// Open the web login
    Login {
        /// Login by typing your email and password
        #[clap(short, long)]
        interactive: bool,
    },
    /// Show commands for interaction with the region
    Regions(Regions),
    /// Connect to a region using a SQL shell
    Shell {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
}

#[derive(Debug, Args)]
struct Regions {
    #[clap(subcommand)]
    command: RegionsCommands,
}

#[derive(Debug, Subcommand)]
enum RegionsCommands {
    /// Enable a region.
    Enable {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
    /// List all enabled regions.
    List,
    /// Display a region's status.
    Status {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
}

/// Internal types, struct and enums
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Region {
    environment_controller_url: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct Environment {
    environmentd_pgwire_address: String,
    environmentd_https_address: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct CloudProvider {
    region: String,
    region_controller_url: String,
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
const WEB_DOCS_URL: &str = "https://www.materialize.com/docs";
const DEFAULT_PROFILE_NAME: &str = "default";
const PROFILES_PREFIX: &str = "profiles";
const ERROR_OPENING_PROFILES_MESSAGE: &str = "Error opening the profiles file";
const ERROR_PARSING_PROFILES_MESSAGE: &str = "Error parsing the profiles";
const ERROR_AUTHENTICATING_PROFILE_MESSAGE: &str = "Error authenticating profile";
const PROFILE_NOT_FOUND_MESSAGE: &str =
    "Profile not found. Please, add one or login using `mz login`.";
const ERROR_UNKNOWN_REGION: &str = "Unknown region";

#[tokio::main]
async fn main() -> Result<()>  {
    let args = Cli::parse();
    let profile_name = args.profile;

    match args.command {
        Commands::Docs => {
            // Open the browser docs
            open::that(WEB_DOCS_URL).with_context(|| "Opening the browser.")?
        }

        Commands::Login { interactive } => {
            if interactive {
                login_with_console(&profile_name).await?
            } else {
                login_with_browser(&profile_name).await?
            }
        }

        Commands::Regions(regions_cmd) => {
            let client = Client::new();

            match regions_cmd.command {
                RegionsCommands::Enable {
                    cloud_provider_region,
                } => {
                    match CloudProviderRegion::from_str(&cloud_provider_region) {
                        Ok(cloud_provider_region) => {
                            let frontegg_auth_machine =  validate_profile(profile_name, &client).await.with_context(|| "Authenticating profile.")?;
                            let loading_spinner = run_loading_spinner("Enabling region...".to_string());
                            enable_region(
                                client,
                                cloud_provider_region,
                                frontegg_auth_machine,
                            ).await.with_context(|| "Enabling region.")?;
                            loading_spinner.finish_with_message("Region enabled.");
                        }
                        Err(_) => exit_with_fail_message(ExitMessage::Str(ERROR_UNKNOWN_REGION)),
                    }
                }

                RegionsCommands::List => {
                    let frontegg_auth_machine =  validate_profile(profile_name, &client).await.with_context(|| "Authenticating profile.")?;
                    let cloud_providers = list_cloud_providers(&client, &frontegg_auth_machine).await.with_context(|| "Retrieving cloud providers.")?;
                    let cloud_providers_regions =
                        list_regions(&cloud_providers, &client, &frontegg_auth_machine)
                            .await;
                    cloud_providers_regions
                        .iter()
                        .for_each(|cloud_provider_region| {
                            print_region_enabled(cloud_provider_region);
                        });
                }

                RegionsCommands::Status {
                    cloud_provider_region,
                } => {
                    let profile = get_profile(profile_name).with_context(|| "Profile not found. Please, add one or login using `mz login`.")?;
                    let client = Client::new();
                    let frontegg_auth_machine = authenticate_profile(&client, &profile).await.with_context(|| "Authenticating profile.")?;
                    let cloud_providers = list_cloud_providers(&client, &frontegg_auth_machine).await.with_context(|| "Retrieving cloud providers.")?;
                    match CloudProviderRegion::from_str(&cloud_provider_region) {
                        Ok(cloud_provider_region) => {
                            let filtered_providers: Vec<CloudProvider> =
                            cloud_providers
                                .into_iter()
                                .filter(|provider| {
                                    provider.region
                                        == cloud_provider_region
                                            .region_name()
                                })
                                .collect::<Vec<CloudProvider>>();

                            let mut cloud_provider_regions = list_regions(
                                &filtered_providers,
                                &client,
                                &frontegg_auth_machine,
                            )
                            .await;

                            let cloud_provider_region = cloud_provider_regions.pop().with_context(|| "Missing provider")?;
                            let region = cloud_provider_region.region.with_context(|| "Region unavailable")?;
                            let environment_details = region_environment_details(&client, &region, &frontegg_auth_machine).await.with_context(|| "Environment unavailable")?;
                            let mut environment_list = environment_details.with_context(|| "Environment unlisted")?;
                            let environment = environment_list.pop().with_context(|| "Missing environment")?;
                            let health =
                            check_environment_health(profile, &environment);
                            print_environment_status(environment, health);
                        }
                        Err(_) => exit_with_fail_message(ExitMessage::Str(ERROR_UNKNOWN_REGION)),
                    }
                }
            }
        }

        Commands::Shell {
            cloud_provider_region,
        } => {
            match CloudProviderRegion::from_str(&cloud_provider_region) {
                Ok(cloud_provider_region) => {
                    let profile = get_profile(profile_name).with_context(|| "Profile not found. Please, add one or login using `mz login`.")?;
                    let client = Client::new();
                    let frontegg_auth_machine = authenticate_profile(&client, &profile).await.with_context(|| "Authenticating profile.")?;
                    shell(
                        client,
                        profile,
                        frontegg_auth_machine,
                        cloud_provider_region,
                    )
                    .await;
                }
                Err(_) => exit_with_fail_message(ExitMessage::Str(ERROR_UNKNOWN_REGION)),
            }
        }
    }

    Ok(())
}
