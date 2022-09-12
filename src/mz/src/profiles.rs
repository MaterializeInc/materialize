// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::utils::exit_with_fail_message;
use crate::{
    ExitMessage, FronteggAuthMachine, Profile, DEFAULT_PROFILE_NAME,
    ERROR_AUTHENTICATING_PROFILE_MESSAGE, ERROR_OPENING_PROFILES_MESSAGE,
    ERROR_PARSING_PROFILES_MESSAGE, MACHINE_AUTH_URL, PROFILES_DIR_NAME, PROFILES_FILE_NAME,
    PROFILES_PREFIX, PROFILE_NOT_FOUND_MESSAGE,
};
use dirs::home_dir;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use reqwest::{Client, Error};
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use toml_edit::{value, Document};

/// ----------------------------
///  Profiles handling
///  The config path is as follows: ~/.config/mz/profiles.toml
/// ----------------------------

/// Gets the config path from the $HOME path.
fn get_config_path() -> PathBuf {
    match home_dir() {
        Some(mut path) => {
            path.push(PROFILES_DIR_NAME);
            path
        }
        None => exit_with_fail_message(ExitMessage::Str("Error finding $HOME directory.")),
    }
}

/// Authenticates a profile with Frontegg
pub(crate) async fn authenticate_profile(
    client: &Client,
    profile: &Profile,
) -> Result<FronteggAuthMachine, Error> {
    let mut access_token_request_body = HashMap::new();
    access_token_request_body.insert("clientId", profile.client_id.as_str());
    access_token_request_body.insert("secret", profile.secret.as_str());

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
        exit_with_fail_message(ExitMessage::Str(
            "Unauthorized. Please, check the credentials.",
        ));
    } else {
        authentication_result.json::<FronteggAuthMachine>().await
    }
}

/// Checks if a path does not exists.
fn path_not_exist(path: &PathBuf) -> bool {
    fs::metadata(path).is_err()
}

/// Creates the config dir if not exists.
fn create_profile_dir_if_not_exists() {
    let config_path = get_config_path();

    if path_not_exist(&config_path) {
        fs::create_dir_all(config_path.as_path()).unwrap();
    };
}

/// Write a particular profile into the config file
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
    profiles_document[format!("{}.{}", PROFILES_PREFIX, profile.name).as_str()] = new_profile_table;

    fs::write(config_path, profiles_document.to_string())
}

/// Create the config dir. and save a particular profile into the config file.
pub(crate) fn save_profile(profile: Profile) -> std::io::Result<()> {
    create_profile_dir_if_not_exists();

    write_profile(profile)
}

/// Get all the profiles from the config file.
pub(crate) fn get_profiles() -> Option<Document> {
    let mut profiles_path = get_config_path();
    profiles_path.push(PROFILES_FILE_NAME);

    // Check if profiles file exists
    if path_not_exist(&get_config_path()) || path_not_exist(&profiles_path) {
        None
    } else {
        // Return profile
        match fs::read_to_string(profiles_path.as_path()) {
            Ok(profiles_string) => match profiles_string.parse::<Document>() {
                Ok(profiles) => Some(profiles),
                Err(error) => exit_with_fail_message(ExitMessage::String(format!(
                    "{}: {:?}",
                    ERROR_PARSING_PROFILES_MESSAGE, error
                ))),
            },
            Err(error) => exit_with_fail_message(ExitMessage::String(format!(
                "{:}: {:?}",
                ERROR_OPENING_PROFILES_MESSAGE, error
            ))),
        }
    }
}

/// Get a particular profile from the config file
pub(crate) fn get_profile(profile_name: String) -> Option<Profile> {
    if let Some(profiles) = get_profiles() {
        let profile_name_key = format!("{:}.{:}", PROFILES_PREFIX, profile_name);
        if !profiles.contains_key(profile_name_key.as_str()) {
            return None;
        }

        let mut default_profile = profiles[profile_name_key.as_str()].clone();
        if default_profile.is_table() {
            default_profile["name"] = value(DEFAULT_PROFILE_NAME);
            let string_default_profile = default_profile.to_string();
            match toml::from_str(string_default_profile.as_str()) {
                Ok(profile) => Some(profile),
                Err(error) => exit_with_fail_message(ExitMessage::String(format!(
                    "{}: {:?}",
                    ERROR_PARSING_PROFILES_MESSAGE, error
                ))),
            }
        } else {
            None
        }
    } else {
        None
    }
}

/// Validate that a profile credentials USER_ID and SECRET are valid.
pub(crate) async fn validate_profile(
    profile_name: String,
    client: &Client,
) -> Option<FronteggAuthMachine> {
    match get_profile(profile_name) {
        Some(profile) => match authenticate_profile(client, &profile).await {
            Ok(frontegg_auth_machine) => {
                return Some(frontegg_auth_machine);
            }
            Err(error) => exit_with_fail_message(ExitMessage::String(format!(
                "{:}: {:?}",
                ERROR_AUTHENTICATING_PROFILE_MESSAGE, error
            ))),
        },
        None => println!("{}", PROFILE_NOT_FOUND_MESSAGE),
    }

    None
}
