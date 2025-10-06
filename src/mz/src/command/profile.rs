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

//! Implementation of the `mz profile` command.
//!
//! Consult the user-facing documentation for details.

use std::{io::Write, str::FromStr};

use mz_frontegg_auth::AppPassword;
use mz_frontegg_client::client::app_password::CreateAppPasswordRequest;
use mz_frontegg_client::client::{Client as AdminClient, Credentials};
use mz_frontegg_client::config::{
    ClientBuilder as AdminClientBuilder, ClientConfig as AdminClientConfig,
};

use mz_cloud_api::config::DEFAULT_ENDPOINT;
use serde::{Deserialize, Serialize};
use tabled::Tabled;
use tokio::{select, sync::mpsc};
use url::Url;

use crate::ui::OptionalStr;
use crate::{
    config_file::TomlProfile,
    context::{Context, ProfileContext},
    error::Error,
    error::Error::ProfileNameAlreadyExistsError,
    server::server,
};

/// Strips the `.api` from the login endpoint.
/// The `.api` prefix will cause a failure during login
/// in the browser.
fn strip_api_from_endpoint(endpoint: Url) -> Url {
    if let Some(domain) = endpoint.domain() {
        if let Some(corrected_domain) = domain.strip_prefix("api.") {
            let mut new_endpoint = endpoint.clone();
            let _ = new_endpoint.set_host(Some(corrected_domain));

            return new_endpoint;
        }
    };

    endpoint
}

/// Opens the default web browser in the host machine
/// and awaits a single request containing the profile's app password.
pub async fn init_with_browser(cloud_endpoint: Option<Url>) -> Result<AppPassword, Error> {
    // Bind a web server to a local port to receive the app password.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (server, port) = server(tx).await;

    // Build the login URL
    let mut url =
        strip_api_from_endpoint(cloud_endpoint.unwrap_or_else(|| DEFAULT_ENDPOINT.clone()));

    url.path_segments_mut()
        .expect("constructor validated URL can be a base")
        .extend(&["account", "login"]);

    let mut query_pairs = url.query_pairs_mut();
    query_pairs.append_pair(
        "redirectUrl",
        &format!("/access/cli?redirectUri=http://localhost:{port}&tokenDescription=Materialize%20CLI%20%28mz%29"),
    );
    // The replace is a little hack to avoid asking an additional parameter
    // for a custom login.
    let open_url = &query_pairs.finish().as_str().replace("cloud", "console");

    // Open the browser to login user.
    if let Err(_err) = open::that(open_url) {
        println!(
            "Error: Unable to launch a web browser. Access the login page using this link: '{:?}', or execute `mz profile init --no-browser` in your command line.",
            open_url
        )
    }

    // Wait for the browser to send the app password to our server.
    select! {
        _ = server => unreachable!("server should not shut down"),
        result = rx.recv() => {
            match result {
                Some(app_password_result) => app_password_result,
                None => { panic!("failed to login via browser") },
            }
        }
    }
}

/// Prompts the user for the profile email and passowrd in Materialize.
/// Notice that the password is the same as the user uses to log into
/// the console, and not the app-password.
pub async fn init_without_browser(admin_endpoint: Option<Url>) -> Result<AppPassword, Error> {
    // Handle interactive user input
    let mut email = String::new();

    print!("Email: ");
    let _ = std::io::stdout().flush();
    std::io::stdin().read_line(&mut email).unwrap();

    // Trim lines
    if email.ends_with('\n') {
        email.pop();
        if email.ends_with('\r') {
            email.pop();
        }
    }

    print!("Password: ");
    let _ = std::io::stdout().flush();
    let password = rpassword::read_password().unwrap();

    // Build client
    let mut admin_client_builder = AdminClientBuilder::default();

    if let Some(admin_endpoint) = admin_endpoint {
        admin_client_builder = admin_client_builder.endpoint(admin_endpoint);
    }

    let admin_client: AdminClient = admin_client_builder.build(AdminClientConfig {
        authentication: mz_frontegg_client::client::Authentication::Credentials(Credentials {
            email,
            password,
        }),
    });

    let app_password = admin_client
        .create_app_password(CreateAppPasswordRequest {
            description: "Materialize CLI (mz)",
        })
        .await?;

    Ok(app_password)
}

/// Initiates the profile creation process.
///
/// There are only two ways to create a profile:
/// 1. By prompting your user and email.
/// 2. By opening the browser and creating the credentials in the console.
pub async fn init(
    scx: &Context,
    no_browser: bool,
    force: bool,
    admin_endpoint: Option<Url>,
    cloud_endpoint: Option<Url>,
) -> Result<(), Error> {
    let config_file = scx.config_file();
    let profile = scx
        .get_global_profile()
        .map_or(config_file.profile().to_string(), |n| n);

    if let Some(profiles) = scx.config_file().profiles() {
        if profiles.contains_key(&profile) && !force {
            return Err(ProfileNameAlreadyExistsError(profile));
        }
    }

    let app_password = match no_browser {
        true => init_without_browser(admin_endpoint.clone()).await?,
        false => init_with_browser(cloud_endpoint.clone()).await?,
    };

    let new_profile = TomlProfile {
        app_password: Some(app_password.to_string()),
        vault: None,
        region: None,
        admin_endpoint: admin_endpoint.map(|url| url.to_string()),
        cloud_endpoint: cloud_endpoint.map(|url| url.to_string()),
    };

    config_file.add_profile(profile, new_profile).await?;

    Ok(())
}

/// List all the possible config values for the profile.
pub fn list(cx: &Context) -> Result<(), Error> {
    if let Some(profiles) = cx.config_file().profiles() {
        let output = cx.output_formatter();

        // Output formatting structure.
        #[derive(Clone, Serialize, Deserialize, Tabled)]
        struct ProfileName<'a> {
            #[tabled(rename = "Name")]
            name: &'a str,
        }
        output.output_table(profiles.keys().map(|name| ProfileName { name }))?;
    }

    Ok(())
}

/// Removes the profile from the configuration file.
pub async fn remove(cx: &Context) -> Result<(), Error> {
    cx.config_file()
        .remove_profile(
            &cx.get_global_profile()
                .unwrap_or_else(|| cx.config_file().profile().to_string()),
        )
        .await
}

/// Represents the args to retrieve a profile configuration value.
pub struct ConfigGetArgs<'a> {
    /// Represents the configuration field name to retrieve the value.
    pub name: &'a str,
}

/// Represents the possible fields in a profile configuration.
#[derive(Clone, Debug)]
pub enum ConfigArg {
    /// Represents `[TomlProfile::admin_endpoint]`
    AdminAPI,
    /// Represents `[TomlProfile::app_password]`
    AppPassword,
    /// Represents `[TomlProfile::cloud_endpoint]`
    CloudAPI,
    /// Represents `[TomlProfile::region]`
    Region,
    /// Represents `[TomlProfile::vault]`
    Vault,
}

impl FromStr for ConfigArg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "admin-api" => Ok(ConfigArg::AdminAPI),
            "app-password" => Ok(ConfigArg::AppPassword),
            "cloud-api" => Ok(ConfigArg::CloudAPI),
            "region" => Ok(ConfigArg::Region),
            "vault" => Ok(ConfigArg::Vault),
            _ => Err("Invalid profile configuration parameter.".to_string()),
        }
    }
}

impl ToString for ConfigArg {
    fn to_string(&self) -> String {
        match self {
            ConfigArg::AdminAPI => "admin-api".to_string(),
            ConfigArg::AppPassword => "app-password".to_string(),
            ConfigArg::CloudAPI => "cloud-api".to_string(),
            ConfigArg::Region => "region".to_string(),
            ConfigArg::Vault => "vault".to_string(),
        }
    }
}

/// Shows the value of a profile configuration field.
pub fn config_get(
    cx: &ProfileContext,
    ConfigGetArgs { name }: ConfigGetArgs<'_>,
) -> Result<(), Error> {
    let profile = cx.get_profile();
    let value = cx.config_file().get_profile_param(name, &profile)?;
    cx.output_formatter().output_scalar(value)?;
    Ok(())
}

/// Shows all the possible field and its values in the profile configuration.
pub fn config_list(cx: &ProfileContext) -> Result<(), Error> {
    let profile_params = cx.config_file().list_profile_params(&cx.get_profile())?;
    let output = cx.output_formatter();

    // Structure to format the output. The name of the field equals the column name.
    #[derive(Serialize, Deserialize, Tabled)]
    struct ProfileParam<'a> {
        #[tabled(rename = "Name")]
        name: &'a str,
        #[tabled(rename = "Value")]
        value: OptionalStr<'a>,
    }

    output.output_table(profile_params.iter().map(|(name, value)| ProfileParam {
        name,
        value: OptionalStr(value.as_deref()),
    }))?;
    Ok(())
}

/// Represents the args to set the value of a profile configuration field.
pub struct ConfigSetArgs<'a> {
    /// Represents the name of the field to set the value.
    pub name: &'a str,
    /// Represents the new value of the field.
    pub value: &'a str,
}

/// Sets a value in the profile configuration.
pub async fn config_set(
    cx: &ProfileContext,
    ConfigSetArgs { name, value }: ConfigSetArgs<'_>,
) -> Result<(), Error> {
    cx.config_file()
        .set_profile_param(&cx.get_profile(), name, Some(value))
        .await
}

/// Represents the args to remove the value from a profile configuration field.
pub struct ConfigRemoveArgs<'a> {
    /// Represents the name of the field to remove.
    pub name: &'a str,
}

/// Removes the value from a profile configuration field.
pub async fn config_remove(
    cx: &ProfileContext,
    ConfigRemoveArgs { name }: ConfigRemoveArgs<'_>,
) -> Result<(), Error> {
    cx.config_file()
        .set_profile_param(&cx.get_profile(), name, None)
        .await
}
