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
    server::server,
};

pub async fn init_with_browser() -> Result<AppPassword, Error> {
    // Bind a web server to a local port to receive the app password.
    let (tx, mut rx) = mpsc::unbounded_channel();
    let (server, port) = server(tx);

    // Build the login URL
    let mut url = DEFAULT_ENDPOINT.clone();
    url.path_segments_mut()
        .expect("constructor validated URL can be a base")
        .extend(&["account", "login"]);

    let mut query_pairs = url.query_pairs_mut();
    query_pairs.append_pair(
        "redirectUrl",
        &format!("/access/cli?redirectUri=http://localhost:{port}"),
    );

    let open_url = query_pairs.finish().as_str();

    // Open the browser to login user.
    if let Err(_err) = open::that(open_url) {
        println!(
            "Could not open a browser to visit the login page <{:?}>: Please open the page yourself.",
            open_url
        )
    }

    // Wait for the browser to send the app password to our server.
    select! {
        _ = server => unreachable!("server should not shut down"),
        result = rx.recv() => {
            match result {
                Some(app_password) => Ok(app_password),
                None => { panic!("failed to login via browser") },
            }
        }
    }
}

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
        admin_client_builder = admin_client_builder.endpoint(admin_endpoint.clone());
    }

    let admin_client: AdminClient = admin_client_builder.build(AdminClientConfig {
        authentication: mz_frontegg_client::client::Authentication::Credentials(Credentials {
            email,
            password,
        }),
    });

    let app_password = admin_client
        .create_app_password(CreateAppPasswordRequest {
            description: "App password for the CLI",
        })
        .await?;

    Ok(app_password)
}

pub async fn init(
    scx: &mut Context,
    profile_name: Option<String>,
    no_browser: bool,
    admin_endpoint: Option<Url>,
) -> Result<(), Error> {
    let app_password = match no_browser {
        true => init_without_browser(admin_endpoint.clone()).await?,
        false => init_with_browser().await?,
    };

    let new_profile = TomlProfile {
        app_password: Some(app_password.to_string()),
        vault: None,
        region: None,
        admin_endpoint: admin_endpoint.map(|url| url.to_string()),
        cloud_endpoint: None,
    };

    scx.config_file()
        .add_profile(
            profile_name.map_or(scx.config_file().profile().to_string(), |n| n),
            new_profile,
        )
        .await?;

    Ok(())
}

pub async fn list(cx: &mut Context) -> Result<(), Error> {
    if let Some(profiles) = cx.config_file().profiles() {
        let output = cx.output_formatter();

        // Output formatting structure.
        #[derive(Clone, Serialize, Deserialize, Tabled)]
        struct ProfileName<'a> {
            #[tabled(rename = "Name")]
            name: &'a str,
        }
        output.output_table(profiles.keys().into_iter().map(|name| ProfileName { name }))?;
    }

    Ok(())
}

pub async fn remove(cx: &mut Context) -> Result<(), Error> {
    cx.config_file()
        .remove_profile(cx.config_file().profile())
        .await
}

pub struct ConfigGetArgs<'a> {
    pub name: &'a str,
}

#[derive(Clone, Debug)]
pub enum ConfigArg {
    AdminAPI,
    AppPassword,
    CloudAPI,
    Region,
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

pub async fn config_get(
    cx: &mut ProfileContext,
    ConfigGetArgs { name }: ConfigGetArgs<'_>,
) -> Result<(), Error> {
    let value = cx.config_file().get_profile_param(name).unwrap();
    cx.output_formatter().output_scalar(value.as_deref())?;
    Ok(())
}

pub async fn config_list(cx: &mut ProfileContext) -> Result<(), Error> {
    let profile_params = cx.config_file().list_profile_params()?;
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

pub struct ConfigSetArgs<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

pub async fn config_set(
    cx: &mut ProfileContext,
    ConfigSetArgs { name, value }: ConfigSetArgs<'_>,
) -> Result<(), Error> {
    cx.config_file().set_profile_param(name, Some(value)).await
}

pub struct ConfigRemoveArgs<'a> {
    pub name: &'a str,
}

pub async fn config_remove(
    cx: &mut ProfileContext,
    ConfigRemoveArgs { name }: ConfigRemoveArgs<'_>,
) -> Result<(), Error> {
    cx.config_file().set_profile_param(name.clone(), None).await
}
