// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{FronteggAuthMachine, Profile, MACHINE_AUTH_URL, PROFILES_DIR_NAME, PROFILES_FILE_NAME, DEFAULT_PROFILE_NOT_FOUND_MESSAGE};
use dirs::home_dir;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, Error};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::process::exit;

use toml_edit::{value, Document};

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

fn path_not_exist(path: PathBuf) -> bool {
    fs::metadata(path).is_err() == true
}

fn create_profile_dir_if_not_exists() {
    let config_path = get_config_path();

    if path_not_exist(config_path.clone()) {
        fs::create_dir_all(config_path.as_path()).unwrap();
    };
}

fn write_profile(profile: Profile) -> std::io::Result<()> {
    let mut config_path = get_config_path();
    config_path.push(PROFILES_FILE_NAME);

    let mut profiles_opt = get_profiles();
    let profiles_document = profiles_opt.get_or_insert(Document::new());

    let mut new_profile_table = toml_edit::table();
    new_profile_table["email"] = value(profile.email);
    new_profile_table["secret"] = value(profile.secret);
    new_profile_table["client_id"] = value(profile.client_id);
    new_profile_table["region"] = value("");
    profiles_document[format!("profiles.{}", profile.name).as_str()] = new_profile_table;

    fs::write(config_path, profiles_document.to_string())
}

pub(crate) fn save_profile(profile: Profile) -> std::io::Result<()> {
    create_profile_dir_if_not_exists();

    write_profile(profile)
}

pub(crate) fn get_profiles() -> Option<Document> {
    let mut profiles_path = get_config_path();
    profiles_path.push(PROFILES_FILE_NAME);

    // Check if profiles file exists
    if path_not_exist(get_config_path()) || path_not_exist(profiles_path.clone()) {
        None
    } else {
        // Return profile
        match fs::read_to_string(profiles_path.as_path()) {
            Ok(profiles_string) => match profiles_string.parse::<Document>() {
                Ok(profiles) => Some(profiles),
                Err(error) => panic!("Error parsing the profiles: {:?}", error),
            },
            Err(error) => panic!("Error opening the profiles file: {:?}", error),
        }
    }
}

pub(crate) fn get_default_profile() -> Option<Profile> {
    if let Some(profiles) = get_profiles() {
        if !profiles.contains_key("profiles.default") {
            return None
        }

        let mut default_profile = profiles["profiles.default"].clone();
        if default_profile.is_table() {
            default_profile["name"] = value("default");
            let string_default_profile = default_profile.to_string();
            match toml::from_str(string_default_profile.as_str()) {
                Ok(profile) => Some(profile),
                Err(error) => panic!("Error parsing the profiles: {:?}", error),
            }
        } else {
            None
        }
    } else {
        None
    }
}

pub(crate) async fn validate_profile(client: Client) -> Option<FronteggAuthMachine> {
    match get_default_profile() {
        Some(profile) => match authenticate_profile(client, profile).await {
            Ok(frontegg_auth_machine) => {
                return Some(frontegg_auth_machine);
            }
            Err(error) => panic!("Error authenticating profile : {:?}", error),
        },
        None => println!("{}", DEFAULT_PROFILE_NOT_FOUND_MESSAGE),
    }

    None
}
