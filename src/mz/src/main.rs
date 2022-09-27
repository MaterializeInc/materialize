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
mod password;
mod profiles;
mod region;
mod shell;
mod utils;

use std::str::FromStr;

use anyhow::{Context, Result};
use login::generate_api_token;
use password::list_passwords;
use region::{
    get_provider_by_region_name, get_provider_region_environment, get_region_environment,
    print_environment_status, print_region_enabled,
};
use serde::{Deserialize, Serialize};

use clap::{Args, Parser, Subcommand};
use reqwest::Client;
use shell::check_environment_health;
use utils::{
    exit_with_fail_message, run_loading_spinner, AppPassword, CloudProviderRegion, FromTos,
};

use crate::login::{login_with_browser, login_with_console};
use crate::profiles::validate_profile;
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
    #[clap(short, long, env = "MZ_PROFILE", default_value = "default")]
    profile: String,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Show commands to interact with passwords
    AppPassword(AppPasswordCommand),
    /// Open the docs
    Docs,
    /// Open the web login
    Login {
        /// Login by typing your email and password
        #[clap(short, long)]
        interactive: bool,
    },
    /// Show commands to interact with regions
    Region {
        #[clap(subcommand)]
        command: RegionCommand,
    },
    /// Connect to a region using a SQL shell
    Shell {
        #[clap(possible_values = CloudProviderRegion::variants())]
        cloud_provider_region: String,
    },
}

#[derive(Debug, Args)]
struct AppPasswordCommand {
    #[clap(subcommand)]
    command: AppPasswordSubommand,
}

#[derive(Debug, Subcommand)]
enum AppPasswordSubommand {
    /// Create a password.
    Create {
        /// Name for the password.
        name: String,
    },
    /// List all enabled passwords.
    List,
}

#[derive(Debug, Subcommand)]
enum RegionCommand {
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

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct FronteggAuth {
    access_token: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FronteggAPIToken {
    client_id: String,
    secret: String,
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
    name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Profile {
    name: String,
    email: String,
    #[serde(rename(serialize = "app-password", deserialize = "app-password"))]
    app_password: AppPassword,
    region: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct ValidProfile {
    profile: Profile,
    frontegg_auth: FronteggAuth,
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
const API_FRONTEGG_TOKEN_AUTH_URL: &str =
    "https://admin.cloud.materialize.com/frontegg/identity/resources/users/api-tokens/v1";
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
const ERROR_PROFILE_NOT_FOUND_MESSAGE: &str =
    "Profile not found. Please, add one or login using `mz login`.";
const ERROR_INCORRECT_PROFILE_PASSWORD_MESSAGE: &str = "Invalid app-password.";
const ERROR_UNKNOWN_REGION: &str = "Unknown region";

#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    let profile_name = args.profile;

    match args.command {
        Commands::AppPassword(password_cmd) => {
            let client = Client::new();
            let valid_profile = validate_profile(profile_name, &client)
                .await
                .with_context(|| "Validating profile.")?;

            match password_cmd.command {
                AppPasswordSubommand::Create { name } => {
                    let api_token = generate_api_token(&client, valid_profile.frontegg_auth, &name)
                        .await
                        .with_context(|| "Generating password.")?;

                    let app_password: AppPassword = AppPassword::from_api_token(api_token);
                    println!("{}", app_password)
                }
                AppPasswordSubommand::List => {
                    let app_passwords = list_passwords(&client, &valid_profile)
                        .await
                        .with_context(|| "Listing passwords.")?;

                    println!("{0: <24} | {1: <24} ", "Name", "Created At");
                    println!("----------------------------------------------------");

                    app_passwords.iter().for_each(|app_password| {
                        let mut name = app_password.description.clone();

                        if name.len() > 20 {
                            let short_name = name[..20].to_string();
                            name = format!("{:}...", short_name);
                        }

                        println!("{0: <24} | {1: <24}", name, app_password.created_at);
                    })
                }
            }
        }

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

        Commands::Region { command } => {
            let client = Client::new();

            match command {
                RegionCommand::Enable {
                    cloud_provider_region,
                } => match CloudProviderRegion::from_str(&cloud_provider_region) {
                    Ok(cloud_provider_region) => {
                        let valid_profile = validate_profile(profile_name, &client)
                            .await
                            .with_context(|| "Validating profile.")?;
                        let loading_spinner = run_loading_spinner("Enabling region...".to_string());
                        let cloud_provider = get_provider_by_region_name(
                            &client,
                            &valid_profile,
                            &cloud_provider_region,
                        )
                        .await
                        .with_context(|| "Retrieving cloud provider.")?;

                        let region =
                            enable_region_environment(&client, &cloud_provider, &valid_profile)
                                .await
                                .with_context(|| "Enabling region.")?;

                        let environment = get_region_environment(&client, &valid_profile, &region)
                            .await
                            .with_context(|| "Retrieving environment data.")?;

                        loop {
                            if check_environment_health(valid_profile.clone(), &environment) {
                                break;
                            }
                        }

                        loading_spinner.finish_with_message("Region enabled.");
                    }
                    Err(_) => exit_with_fail_message(ExitMessage::Str(ERROR_UNKNOWN_REGION)),
                },

                RegionCommand::List => {
                    let valid_profile = validate_profile(profile_name, &client)
                        .await
                        .with_context(|| "Authenticating profile.")?;
                    let cloud_providers = list_cloud_providers(&client, &valid_profile)
                        .await
                        .with_context(|| "Retrieving cloud providers.")?;
                    let cloud_providers_regions =
                        list_regions(&cloud_providers, &client, &valid_profile)
                            .await
                            .with_context(|| "Listing regions.")?;
                    cloud_providers_regions
                        .iter()
                        .for_each(|cloud_provider_region| {
                            print_region_enabled(cloud_provider_region);
                        });
                }

                RegionCommand::Status {
                    cloud_provider_region,
                } => match CloudProviderRegion::from_str(&cloud_provider_region) {
                    Ok(cloud_provider_region) => {
                        let client = Client::new();
                        let valid_profile = validate_profile(profile_name, &client)
                            .await
                            .with_context(|| "Authenticating profile.")?;
                        let environment = get_provider_region_environment(
                            &client,
                            &valid_profile,
                            &cloud_provider_region,
                        )
                        .await
                        .with_context(|| "Retrieving cloud provider region.")?;
                        let health = check_environment_health(valid_profile, &environment);

                        print_environment_status(environment, health);
                    }
                    Err(_) => exit_with_fail_message(ExitMessage::Str(ERROR_UNKNOWN_REGION)),
                },
            }
        }

        Commands::Shell {
            cloud_provider_region,
        } => match CloudProviderRegion::from_str(&cloud_provider_region) {
            Ok(cloud_provider_region) => {
                let client = Client::new();
                let valid_profile = validate_profile(profile_name, &client)
                    .await
                    .with_context(|| "Authenticating profile.")?;
                shell(client, valid_profile, cloud_provider_region)
                    .await
                    .with_context(|| "Running shell")?;
            }
            Err(_) => exit_with_fail_message(ExitMessage::Str(ERROR_UNKNOWN_REGION)),
        },
    }

    Ok(())
}
