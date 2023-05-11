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

use mz_cloud_api::config::DEFAULT_ENDPOINT;
use tokio::{select, sync::mpsc};

use crate::{
    config_file::{TomlProfile, GLOBAL_PARAMS},
    context::{Context, ProfileContext},
    error::Error,
    server::server,
};

pub async fn init(scx: &mut Context, profile_name: Option<String>) -> Result<(), Error> {
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
                Some(app_password) => {
                    // TODO:
                    // * Append vault
                    // * Append region
                    let new_profile = TomlProfile {
                        app_password: Some(app_password.to_string()),
                        vault: None,
                        region: None,
                        admin_endpoint: None,
                        cloud_endpoint: None
                    };
                    // TODO:
                    // * Replace default with env/config value
                    scx.config_file().save_profile(profile_name.map_or("default".to_string(), |n| n), new_profile).await;
                },
                None => { panic!("failed to login via browser") },
            }
        }
    }

    Ok(())
}

pub async fn list(cx: &mut ProfileContext) -> Result<(), Error> {
    todo!()
}

pub async fn remove(cx: &mut ProfileContext) -> Result<(), Error> {
    todo!()
}

pub struct ConfigGetArgs<'a> {
    pub name: &'a str,
}

pub async fn config_get(
    cx: &mut ProfileContext,
    ConfigGetArgs { name }: ConfigGetArgs<'_>,
) -> Result<(), Error> {
    todo!()
}

pub async fn config_list(cx: &mut ProfileContext) -> Result<(), Error> {
    todo!()
}

pub struct ConfigSetArgs<'a> {
    pub name: &'a str,
    pub value: &'a str,
}

pub async fn config_set(
    cx: &mut ProfileContext,
    ConfigSetArgs { name, value }: ConfigSetArgs<'_>,
) -> Result<(), Error> {
    todo!()
}

pub struct ConfigRemoveArgs<'a> {
    pub name: &'a str,
}

pub async fn config_remove(
    cx: &mut ProfileContext,
    ConfigRemoveArgs { name }: ConfigRemoveArgs<'_>,
) -> Result<(), Error> {
    todo!()
}
