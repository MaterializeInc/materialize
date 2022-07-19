// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    FronteggAuthMachine, Profile, MACHINE_AUTH_URL, PROFILES_DIR_NAME, PROFILES_FILE_NAME,
};
use dirs::home_dir;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, Error};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::exit;

/// ----------------------------
///  Profiles handling
/// ----------------------------

fn get_config_path() -> PathBuf {
    match home_dir() {
        Some(mut path) => {
            path.push(PROFILES_DIR_NAME);
            path
        }
        None => panic!("Error finding $HOME directory."),
    }
}

pub(crate) async fn authenticate_profile(
    client: Client,
    profile: Profile,
) -> Result<FronteggAuthMachine, Error> {
    let mut access_token_request_body = HashMap::new();
    access_token_request_body.insert("clientId", profile.client_id);
    access_token_request_body.insert("secret", profile.secret);

    let mut headers = HeaderMap::new();
    headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let authentication_result = client
        .post(MACHINE_AUTH_URL)
        .headers(headers)
        .json(&access_token_request_body)
        .send()
        .await?;

    if authentication_result.status() == 401 {
        println!("Unauthorized. Please, update the credentials.");
        exit(0);
    } else {
        authentication_result.json::<FronteggAuthMachine>().await
    }
}

fn create_profile_dir_if_not_exists() {
    let config_path = get_config_path();

    // Check if path exists
    if fs::metadata(config_path.clone()).is_err() {
        fs::create_dir_all(config_path.as_path()).unwrap();
    };
}

fn write_profile(profile: Profile) -> std::io::Result<()> {
    let mut config_path = get_config_path();
    config_path.push(PROFILES_FILE_NAME);

    let toml = toml::to_string(&profile).unwrap();
    fs::write(config_path, toml)
}

pub(crate) fn save_profile(profile: Profile) -> std::io::Result<()> {
    create_profile_dir_if_not_exists();

    write_profile(profile)
}

pub(crate) fn get_local_profile() -> Option<Profile> {
    // Check if path exists
    create_profile_dir_if_not_exists();

    let mut config_path = get_config_path();
    config_path.push(PROFILES_FILE_NAME);

    // Check if profiles file exists
    if fs::metadata(config_path.clone()).is_err() {
        None
    } else {
        // Return profile
        match fs::read_to_string(config_path.as_path()) {
            Ok(profiles_serialized) => match toml::from_str(&*profiles_serialized) {
                Ok(profile) => Some(profile),
                Err(error) => panic!("Problem parsing the profiles: {:?}", error),
            },
            Err(error) => panic!("Problem opening the profiles file: {:?}", error),
        }
    }
}

pub(crate) async fn validate_profile(client: Client) -> Option<FronteggAuthMachine> {
    match get_local_profile() {
        Some(profile) => match authenticate_profile(client, profile).await {
            Ok(frontegg_auth_machine) => {
                return Some(frontegg_auth_machine);
            }
            Err(error) => panic!("Error authenticating profile : {:?}", error),
        },
        None => println!("Profile not found. Please, login using `mz login`."),
    }

    None
}
