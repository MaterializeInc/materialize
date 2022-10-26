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

mod auth;
mod configuration;
mod password;
mod region;
mod shell;
mod table;
mod utils;

use std::str::FromStr;

use anyhow::{Context, Ok, Result};
use auth::generate_api_token;
use configuration::Configuration;
use password::list_passwords;
use region::{
    get_provider_by_region_name, get_provider_region_environment, get_region_environment,
    print_environment_status,
};
use serde::Deserialize;

use clap::{Parser, Subcommand};
use reqwest::Client;
use shell::check_environment_health;
use table::{ShowProfile, Tabled};
use utils::{run_loading_spinner, CloudProviderRegion};

use crate::auth::{auth_with_browser, auth_with_console};
use crate::region::{enable_region_environment, list_cloud_providers, list_regions};
use crate::shell::shell;

/// Command-line interface for Materialize.
#[derive(Debug, Parser)]
#[clap(name = "Materialize CLI")]
#[clap(about = "Command-line interface for Materialize.", long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
    /// Identify using a particular profile
    #[clap(short, long, env = "MZ_PROFILE")]
    profile: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Open the docs
    Docs,
    /// Authorize the current profile
    Auth {
        /// Authorize by typing your email and password
        #[clap(short, long)]
        interactive: bool,

        /// Force a new authorization
        #[clap(short, long)]
        force: bool,
    },
    /// Show commands to interact with regions
    Region {
        #[clap(subcommand)]
        command: RegionCommand,
    },
    /// Create a new object
    Create {
        #[clap(subcommand)]
        value: Creatable,
    },
    /// Set a variable
    Set {
        #[clap(subcommand)]
        value: Settable,
    },
    /// Connect to a region using a SQL shell
    Shell {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: Option<String>,
    },
    /// Show information about a certain object type
    Show {
        #[clap(subcommand)]
        value: Showable,
    },
}

#[derive(Debug, Subcommand)]
enum Creatable {
    AppPassword {
        /// Name for the password
        name: String,
    },
    Region {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
}

#[derive(Debug, Subcommand)]
enum Settable {
    /// Set the default profile for this configuration
    Profile { profile: String },

    /// Set the default region to connect to for the current profile
    Region { region: String },
}

#[derive(Debug, Subcommand)]
enum Showable {
    /// Show all app-passwords
    AppPasswords,
    /// Show the current profile
    Profile,
    /// Show all profiles
    Profiles,
    /// Show the default region for the current profile if set
    Region,
    /// Show all enabled regions
    Regions,
}

#[derive(Debug, Subcommand)]
enum RegionCommand {
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
struct FronteggAppPassword {
    description: String,
    created_at: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BrowserAPIToken {
    email: String,
    client_id: String,
    secret: String,
}

struct CloudProviderAndRegion {
    cloud_provider: CloudProvider,
    region: Option<Region>,
}

/// Constants
const CLOUD_PROVIDERS_URL: &str = "https://cloud.materialize.com/api/cloud-providers";
const API_TOKEN_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/identity/resources/users/api-tokens/v1";
const API_FRONTEGG_TOKEN_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/frontegg/identity/resources/users/api-tokens/v1";
const USER_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/frontegg/identity/resources/auth/v1/user";
const MACHINE_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/identity/resources/auth/v1/api-token";
const WEB_LOGIN_URL: &str = "https://cloud.materialize.com/account/login?redirectUrl=/access/cli";
const WEB_DOCS_URL: &str = "https://www.materialize.com/docs";

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let profile = args.profile;
    let mut config = Configuration::load()?;
    match args.command {
        Commands::Docs => {
            // Open the browser docs
            open::that(WEB_DOCS_URL).with_context(|| "Opening the browser.")?
        }

        Commands::Auth { interactive, force } => {
            let profile_name = config.current_profile(profile.clone());
            if !force && config.get_profile(profile).is_ok() {
                println!(
                    "{} is already authorized. Use -f to reauthorize.",
                    profile_name
                );
                return Ok(());
            }

            if interactive {
                auth_with_console(&profile_name, &mut config).await?
            } else {
                auth_with_browser(&profile_name, &mut config).await?
            }
        }

        Commands::Create { value } => match value {
            Creatable::AppPassword { name } => {
                let profile = config.get_profile(profile)?;

                let client = Client::new();
                let valid_profile = profile
                    .validate(&client)
                    .await
                    .context("failed to validate profile. reauthorize using mz login")?;

                let api_token = generate_api_token(&client, valid_profile.frontegg_auth, &name)
                    .await
                    .with_context(|| "failed to create a new app password")?;

                println!("{}", api_token)
            }
            Creatable::Region {
                cloud_provider_region,
            } => {
                let client = Client::new();
                let cloud_provider_region = CloudProviderRegion::from_str(&cloud_provider_region)?;
                let profile = config.get_profile(profile)?;

                let valid_profile = profile
                    .validate(&client)
                    .await
                    .context("failed to validate profile. reauthorize using mz auth --force")?;

                let loading_spinner = run_loading_spinner("Enabling region...".to_string());
                let cloud_provider =
                    get_provider_by_region_name(&client, &valid_profile, &cloud_provider_region)
                        .await
                        .with_context(|| "Retrieving cloud provider.")?;

                let region = enable_region_environment(&client, &cloud_provider, &valid_profile)
                    .await
                    .with_context(|| "Enabling region.")?;

                let environment = get_region_environment(&client, &valid_profile, &region)
                    .await
                    .with_context(|| "Retrieving environment data.")?;

                loop {
                    if check_environment_health(&valid_profile, &environment) {
                        break;
                    }
                }

                loading_spinner.finish_with_message("Region enabled.");
            }
        },

        Commands::Region { command } => {
            let client = Client::new();

            match command {
                RegionCommand::Status {
                    cloud_provider_region,
                } => {
                    let cloud_provider_region =
                        CloudProviderRegion::from_str(&cloud_provider_region)?;

                    let profile = config.get_profile(profile)?;

                    let valid_profile = profile
                        .validate(&client)
                        .await
                        .context("failed to validate profile. reauthorize using mz auth")?;

                    let environment = get_provider_region_environment(
                        &client,
                        &valid_profile,
                        &cloud_provider_region,
                    )
                    .await
                    .with_context(|| "Retrieving cloud provider region.")?;
                    let health = check_environment_health(&valid_profile, &environment);

                    print_environment_status(environment, health);
                }
            }
        }

        Commands::Set { value } => match value {
            Settable::Profile { profile } => config.update_default_profile(profile),
            Settable::Region { region } => {
                let region = CloudProviderRegion::from_str(&region)?;
                let mut profile = config.get_profile(profile)?;
                profile.set_default_region(region)
            }
        },

        Commands::Shell {
            cloud_provider_region,
        } => {
            let profile = config.get_profile(profile)?;

            let cloud_provider_region = match cloud_provider_region {
                Some(region) => CloudProviderRegion::from_str(&region)?,
                None => profile
                    .get_default_region()
                    .context("no default region set for profile")?,
            };

            let client = Client::new();
            let valid_profile = profile
                .validate(&client)
                .await
                .context("failed to validate profile. reauthorize using mz auth")?;

            shell(client, valid_profile, cloud_provider_region)
                .await
                .with_context(|| "running shell")?;
        }

        Commands::Show { value } => {
            let client = Client::new();
            match value {
                Showable::Profile => {
                    let profile = ShowProfile {
                        name: config.current_profile(profile),
                    };
                    println!("{}", profile.table());
                }
                Showable::Profiles => {
                    let showable: Vec<_> = config
                        .get_profiles(profile)
                        .iter()
                        .map(|name| ShowProfile { name: name.clone() })
                        .collect();

                    println!("{}", showable.table());
                }
                Showable::AppPasswords => {
                    let profile = config.get_profile(profile)?;

                    let valid_profile = profile
                        .validate(&client)
                        .await
                        .context("failed to validate profile. reauthorize using mz auth")?;

                    let app_passwords = list_passwords(&client, &valid_profile)
                        .await
                        .with_context(|| "failed to retrieve app passwords")?;

                    println!("{}", app_passwords.table());
                }
                Showable::Region => {
                    let profile = config.get_profile(profile)?;
                    println!("{}", profile.get_default_region().table());
                }
                Showable::Regions => {
                    let profile = config.get_profile(profile)?;

                    let valid_profile = profile
                        .validate(&client)
                        .await
                        .context("failed to validate profile. reauthorize using mz auth --force")?;

                    let cloud_providers = list_cloud_providers(&client, &valid_profile)
                        .await
                        .with_context(|| "Retrieving cloud providers.")?;
                    let cloud_providers_regions =
                        list_regions(&cloud_providers, &client, &valid_profile)
                            .await
                            .with_context(|| "Listing regions.")?;
                    println!("{}", cloud_providers_regions.table());
                }
            }
        }
    }

    config.close()
}
