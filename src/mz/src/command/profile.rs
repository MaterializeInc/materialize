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

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mz_frontegg_auth::AppPassword;
use mz_frontegg_client::client::app_password::CreateAppPasswordRequest;
use mz_frontegg_client::client::{Authentication, Client as AdminClient, Credentials};
use mz_frontegg_client::config::{
    ClientBuilder as AdminClientBuilder, ClientConfig as AdminClientConfig,
    DEFAULT_ENDPOINT as DEFAULT_ADMIN_ENDPOINT,
};
use sha2::{Digest, Sha256};

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
    server::oauth_callback_server,
};

/// Logs in through the admin endpoint's OAuth authorization server in the
/// default web browser, then creates an app password with the resulting access
/// token.
///
/// This is the authorization code flow for native apps (RFC 8252) with PKCE
/// (RFC 7636). The browser only carries a single-use authorization code, which
/// cannot be redeemed without `code_verifier`, and `code_verifier` never leaves
/// this process. The app password is created over a direct HTTPS request.
///
/// Without `client_id`, a client is registered for this login through dynamic
/// client registration (RFC 7591), with this login's exact redirect URI.
pub async fn init_with_oauth(
    admin_endpoint: Option<Url>,
    client_id: Option<&str>,
) -> Result<AppPassword, Error> {
    let admin_endpoint = admin_endpoint.unwrap_or_else(|| DEFAULT_ADMIN_ENDPOINT.clone());
    let endpoint_url = |path: [&str; 2]| {
        let mut url = admin_endpoint.clone();
        url.path_segments_mut()
            .expect("constructor validated URL can be a base")
            .clear()
            .extend(path);
        url
    };

    let code_verifier = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 32]>());
    let code_challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(code_verifier.as_bytes()));
    let state = URL_SAFE_NO_PAD.encode(rand::random::<[u8; 16]>());

    let (tx, mut rx) = mpsc::unbounded_channel();
    let (server, port) = oauth_callback_server(state.clone(), tx).await;
    // RFC 8252 §7.3: use the loopback IP literal, not `localhost`, so the
    // redirect cannot be resolved to some other interface.
    let redirect_uri = format!("http://127.0.0.1:{port}/");

    let http = reqwest::Client::new();
    let client_id = match client_id {
        Some(client_id) => client_id.to_string(),
        None => register_oauth_client(&http, endpoint_url(["oauth", "dcr"]), &redirect_uri).await?,
    };
    let client_id = client_id.as_str();

    let mut authorize_url = endpoint_url(["oauth", "authorize"]);
    authorize_url
        .query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", client_id)
        .append_pair("redirect_uri", &redirect_uri)
        .append_pair("scope", "openid")
        .append_pair("state", &state)
        .append_pair("code_challenge", &code_challenge)
        .append_pair("code_challenge_method", "S256");

    if let Err(_err) = open::that(authorize_url.as_str()) {
        println!(
            "Error: Unable to launch a web browser. Access the login page using this link: '{}', or execute `mz profile init --no-browser` in your command line.",
            authorize_url
        )
    }

    let code = select! {
        _ = server => unreachable!("server should not shut down"),
        result = rx.recv() => match result {
            Some(code) => code?,
            None => panic!("failed to login via browser"),
        },
    };

    #[derive(Deserialize)]
    struct TokenResponse {
        access_token: String,
    }

    let res = http
        .post(endpoint_url(["oauth", "token"]))
        .form(&[
            ("grant_type", "authorization_code"),
            ("code", code.as_str()),
            ("redirect_uri", redirect_uri.as_str()),
            ("client_id", client_id),
            ("code_verifier", code_verifier.as_str()),
        ])
        .send()
        .await?;
    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        return Err(Error::OAuthError(format!(
            "token endpoint returned {status}: {body}"
        )));
    }
    let TokenResponse { access_token } = res.json().await?;

    let admin_client = AdminClientBuilder::default()
        .endpoint(admin_endpoint)
        .build(AdminClientConfig {
            authentication: Authentication::AccessToken(access_token),
        });
    let app_password = admin_client
        .create_app_password(CreateAppPasswordRequest {
            description: "Materialize CLI (mz)",
        })
        .await?;

    Ok(app_password)
}

/// Registers a public OAuth client that may only redirect to `redirect_uri`,
/// and returns its client ID.
async fn register_oauth_client(
    http: &reqwest::Client,
    mut dcr_url: Url,
    redirect_uri: &str,
) -> Result<String, Error> {
    dcr_url
        .path_segments_mut()
        .expect("constructor validated URL can be a base")
        .push("register");

    #[derive(Deserialize)]
    struct RegistrationResponse {
        client_id: String,
    }

    let res = http
        .post(dcr_url)
        .json(&serde_json::json!({
            "client_name": "Materialize CLI (mz)",
            "redirect_uris": [redirect_uri],
            "grant_types": ["authorization_code"],
            "response_types": ["code"],
            "token_endpoint_auth_method": "none",
        }))
        .send()
        .await?;
    let status = res.status();
    if !status.is_success() {
        let body = res.text().await.unwrap_or_default();
        return Err(Error::OAuthError(format!(
            "client registration returned {status}: {body}"
        )));
    }
    let RegistrationResponse { client_id } = res.json().await?;
    Ok(client_id)
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
    oauth_client_id: Option<&str>,
    force: bool,
    admin_endpoint: Option<Url>,
    cloud_endpoint: Option<Url>,
) -> Result<(), Error> {
    let config_file = scx.config_file();
    let profile = scx
        .get_global_profile()
        .unwrap_or_else(|| config_file.profile().to_string());

    if let Some(profiles) = scx.config_file().profiles() {
        if profiles.contains_key(&profile) && !force {
            return Err(ProfileNameAlreadyExistsError(profile));
        }
    }

    // Fail before logging in, so that an unwritable configuration file doesn't
    // leave behind an app password that no profile records.
    config_file.ensure_writable().await?;

    let app_password = if no_browser {
        init_without_browser(admin_endpoint.clone()).await?
    } else {
        let oauth_endpoint = scx.get_admin_endpoint(
            cloud_endpoint.clone(),
            admin_endpoint.as_ref().map(Url::as_str),
        )?;
        init_with_oauth(oauth_endpoint, oauth_client_id).await?
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
