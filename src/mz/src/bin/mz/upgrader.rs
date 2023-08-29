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
use mz::{error::Error, VERSION};
use mz_build_info::build_info;
use serde::Deserialize;

use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, path::Path};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader},
};

/// This const variable represents a whole week in secs =
/// (7 days * 24 hours * 60 minutes * 60 seconds)
/// Chronos lib could fit very well here but I avoided
/// to add an additional lib to the crate.
const SECONDS_IN_A_WEEK: u64 = 7 * 24 * 60 * 60;
const TEMP_FILE_NAME: &str = ".mz.ver";
const MZ_V_PREFIX: &str = "mz-v";

pub struct UpgradeChecker;

impl Default for UpgradeChecker {
    fn default() -> Self {
        UpgradeChecker
    }
}

impl UpgradeChecker {
    /// Writes the current timestmap in the temp file.
    fn update_temp_file(&self, (major, minor, patch): (u64, u64, u64)) -> Result<(), Error> {
        let mut temp_path: PathBuf = env::temp_dir();
        temp_path.push(TEMP_FILE_NAME);

        let mut file = File::create(&temp_path)?;

        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::TimestampConversionError)?
            .as_secs();

        file.write(current_time.to_string().as_bytes())?;

        file.write(format!("\n{}{}.{}.{}", MZ_V_PREFIX, major, minor, patch).as_bytes())?;

        Ok(())
    }

    /// Checks if the last timestamp in the temp file has more than one week old.
    fn is_cache_older_than_a_week(&self, stored_timestamp: u64) -> Result<bool, Error> {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::TimestampConversionError)?
            .as_secs();

        if current_time >= stored_timestamp + SECONDS_IN_A_WEEK {
            return Ok(true);
        }

        Ok(false)
    }

    /// Returns the version as a tuple as follows:
    /// (major, minor, patch)
    fn strip_version(&self, name: String) -> Result<Option<(u64, u64, u64)>, Error> {
        if let Some(version_part) = name.strip_prefix(MZ_V_PREFIX) {
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

    fn get_cached_timpestamp_and_version(&self) -> Result<Option<(u64, String)>, Error> {
        let mut temp_path: PathBuf = env::temp_dir();
        temp_path.push(TEMP_FILE_NAME);

        if !Path::new(&temp_path).exists() {
            // Trigger the check if the file does not exist.
            return Ok(None);
        }

        let file = OpenOptions::new().read(true).open(&temp_path)?;
        let mut reader = BufReader::new(file);

        let mut timestamp = String::new();
        reader.read_line(&mut timestamp)?;

        let mut reg_version = String::new();
        reader.read_line(&mut reg_version)?;

        let stored_timestamp: u64 = timestamp.trim().parse().map_err(|_| Error::ParsingTimestampU64Error)?;

        Ok(Some((stored_timestamp, reg_version)))
    }

    /// Returns true if the installed version of `mz`
    /// is older than the version sent by parameter.
    fn is_installed_version_older_than(&self, (major, minor, patch): (u64, u64, u64)) -> bool {
        let local_version = build_info!().semver_version();

        if major > local_version.major {
            return true;
        }

        if minor > local_version.minor {
            return true;
        }

        if major == local_version.major
            && minor == local_version.minor
            && patch > local_version.patch + 5
        {
            return true;
        }

        false
    }

    /// Fetches and returns the latest tag version from the Materialize repository
    /// using the GitHub public API.
    async fn get_last_tag_version(&self) -> Result<Option<(u64, u64, u64)>, Error> {
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
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(&user_agent).map_err(|err| Error::HeaderParseError(err))?,
        );

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

            if let Some(tag) = tags
                .into_iter()
                .find(|tag| tag.name.starts_with(MZ_V_PREFIX))
            {
                break tag;
            }
            page += 1;
        };

        let last_version = self.strip_version(last_tag.name)?;

        Ok(last_version)
    }

    /// Formats and returns the update message depending on the user's operating system.
    fn format_version_update_message(&self) -> String {
        let os = std::env::consts::OS;

        match os {
            "linux" => {
                "New version available. Update now: `apt install --only-upgrade materialize-cli`"
            }
            "macos" => {
                "New version available. Update now: `brew upgrade materializeInc/materialize/mz`"
            }
            _ => "New version available. Update now.",
        }
        .to_string()
    }

    /// Prints the warning message to update mz if the check result is true.
    pub fn print_warning_if_needed(&self, check_result: Result<bool, Error>) {
        match check_result {
            Ok(check) => {
                if check {
                    println!("\n{}", self.format_version_update_message());
                }
            }
            _ => {}
        }
    }

    /// This function checks if `mz` needs to update. The function
    /// returns true if the installed version meets any
    /// of the following conditions:
    /// - Less than 5 patch versions from the latest version
    /// - Less than 1 minor version from the latest version
    /// - Less than 1 major version from the latest version
    ///
    /// Additionally, the check caches the results for a week
    /// to avoid slowing down commands and
    /// sending excessive requests to GitHub.
    ///
    /// The cached results contains the timestamp and
    /// version fetched in a file within the temp dir
    /// using `env::temp_dir()`.
    pub async fn check_version(&self) -> Result<bool, Error> {
        // Check cached data first
        if let Some((cache_timestamp, cached_version)) = self.get_cached_timpestamp_and_version()? {
            // If the already cached version is greater than the local version
            // Warn the user and avoid querying GitHub
            if let Some(stripped_version) = self.strip_version(cached_version)? {
                if self.is_installed_version_older_than(stripped_version) {
                    return Ok(true);
                }
            }

            // If the cache is not older than a week then there
            // is no need to check new versions yet.
            if !self.is_cache_older_than_a_week(cache_timestamp)? {
                return Ok(false);
            }
        }

        // Fetch the latest version from GitHub
        match self.get_last_tag_version().await {
            Ok(Some(tag_version)) => {
                self.update_temp_file(tag_version)?;
                if self.is_installed_version_older_than(tag_version) {
                    return Ok(true);
                }
            }
            Ok(None) => {
                return Ok(false);
            }
            Err(e) => {
                return Err(e);
            }
        }

        Ok(false)
    }
}
