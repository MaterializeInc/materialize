// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Command-line version checker.

use axum::http::HeaderValue;
use hyper::{
    header::{ACCEPT, USER_AGENT},
    HeaderMap,
};
use mz_build_info::build_info;
use serde::Deserialize;

use crate::{error::Error, VERSION};

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, path::Path};

/// This const variable represents a whole week in secs =
/// (7 days * 24 hours * 60 minutes * 60 seconds)
/// Chronos lib could fit very well here but I avoided
/// to add an additional lib to the crate.
const SECONDS_IN_A_WEEK: u64 = 7 * 24 * 60 * 60;
const TEMP_FILE_NAME: &str = ".mz.ver";

/// Writes the current timestmap in the temp file.
fn update_temp_file_timestamp() -> std::io::Result<()> {
    let mut temp_path: PathBuf = env::temp_dir();
    temp_path.push(TEMP_FILE_NAME);

    let mut file = File::create(&temp_path)?;

    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    file.write_all(current_time.to_string().as_bytes())?;

    Ok(())
}

/// Checks if the last timestamp in the temp file has more than one week old.
fn is_last_check_one_week_old() -> Result<bool, std::io::Error> {
    let mut temp_path: PathBuf = env::temp_dir();
    temp_path.push(TEMP_FILE_NAME);

    if !Path::new(&temp_path).exists() {
        // Trigger the check if the file does not exist.
        return Ok(true);
    }

    let mut file = OpenOptions::new().read(true).open(&temp_path)?;
    let mut timestamp_str = String::new();
    file.read_to_string(&mut timestamp_str)?;

    let stored_timestamp: u64 = timestamp_str
        .trim()
        .parse()
        .expect("Failed to parse timestamp");
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();

    Ok(current_time >= stored_timestamp + SECONDS_IN_A_WEEK)
}

// fn main() -> std::io::Result<()> {
//     // Write the current timestamp to the temp file
//     write_current_timestamp()?;

//     // Check if the timestamp in the temp file is more than a week old
//     match check_if_file_is_week_old() {
//         Ok(true) => println!("The stored timestamp is more than a week old."),
//         Ok(false) => println!("The stored timestamp is not more than a week old."),
//         Err(e) => println!("An error occurred: {}", e),
//     }

//     Ok(())
// }

fn create_version_update_message(major: u64, minor: u64, patch: u64) -> String {
    // TODO: Add support for linux.
    format!("New version available: v{}.{}.{}. Update now: `brew upgrade materializeInc/materialize/mz`", major, minor, patch)
}

/// Returns the version as a tuple as follows:
/// (major, minor, patch)
fn strip_version(name: String) -> Result<Option<(u64, u64, u64)>, Error> {
    if let Some(version_part) = name.strip_prefix("mz-v") {
        // split into major, minor, patch
        let parts: Vec<&str> = version_part.split('.').collect();
        let parsed_parts = (
            parts[0]
                .parse::<u64>()
                .map_err(|_err| Error::SemVerParseError)?,
            parts[1]
                .parse::<u64>()
                .map_err(|_err| Error::SemVerParseError)?,
            parts[2]
                .parse::<u64>()
                .map_err(|_err| Error::SemVerParseError)?,
        );

        return Ok(Some(parsed_parts));
    }

    Ok(None)
}

/// This fn reminds the user to update `mz` if the
/// installed version meets any of the following conditions:
/// - Less than 5 patch versions from the latest version
/// - Less than 1 minor version from the latest version
/// - Less than 1 major version from the latest version
///
/// Additionally, the check is performed once a week to avoid
/// slowing down commands and sending excessive requests to GitHub.
///
/// To keep track of when the last check was performed,
/// the function stores a timestamp in a file within the
/// temporary directory using `env::temp_dir()`.
pub async fn warn_version_if_necessary() -> Result<Option<String>, Error> {
    if is_last_check_one_week_old()? {
        update_temp_file_timestamp()?;

        #[derive(Deserialize, Debug)]
        struct GithubTag {
            name: String,
        }

        // Headers recommended by GitHub: https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#list-repository-tags
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static("application/vnd.github+json"),
        );
        // The GitHub API endpoint requires setting the user agent; otherwise, it returns a 403.
        let user_agent = format!("mz/{}", VERSION.clone());
        headers.insert(USER_AGENT, HeaderValue::from_str(&user_agent).unwrap());

        let client = reqwest::Client::new();

        // As of the creation date of this code, the last `mz-vx.y.z` tag is located on page 4 (last-one).
        // I kept the first page in the event to avoid any issues.
        let mut page = 1;

        let last_tag = loop {
            let url = format!(
                "https://api.github.com/repos/materializeInc/materialize/tags?per_page=100&page={}",
                page
            );
            let response = client
                .get(url)
                .headers(headers.clone())
                .send()
                .await
                .map_err(|err| Error::GitHubFetchError(err))?;

            let tags: Vec<GithubTag> = response
                .json()
                .await
                .map_err(|err| Error::ReqwestJsonParseError(err))?;

            if let Some(tag) = tags.into_iter().find(|tag| tag.name.starts_with("mz-")) {
                break tag;
            }
            page += 1;
        };

        let last_version = strip_version(last_tag.name)?;
        let current_version = build_info!().semver_version();

        if let Some((last_major, last_minor, last_patch)) = last_version {
            if last_major > current_version.major
                || last_minor > current_version.minor
                || last_patch > current_version.patch + 5
            {
                let msg = create_version_update_message(last_major, last_minor, last_patch);
                return Ok(Some(msg));
            }
        }
    }

    Ok(None)
}
