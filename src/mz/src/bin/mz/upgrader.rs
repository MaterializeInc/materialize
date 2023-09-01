// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Command-line version checker.

use mz::{error::Error, ui::OutputFormatter};
use mz_build_info::build_info;
use reqwest::redirect::Policy;
use reqwest::Client;
use semver::Version;

use std::io::Write;
use std::path::PathBuf;
use std::time::SystemTime;
use std::{env, path::Path};
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader},
};

const CACHE_FILE_NAME: &str = ".mz.ver";
const BINARIES_LATEST_VERSION_URL: &str =
    "https://binaries.materialize.com/mz-latest-x86_64-unknown-linux-gnu.tar.gz";

pub struct UpgradeChecker {
    no_color: bool,
}

impl UpgradeChecker {
    pub fn new(no_color: bool) -> Self {
        UpgradeChecker { no_color }
    }

    /// Writes the current timestmap in the temp file.
    fn update_cache_file(&self, version: &Version) -> Result<(), Error> {
        let mut cache_path: PathBuf = dirs::cache_dir().ok_or(Error::CacheDirNotFoundError)?;
        cache_path.push(CACHE_FILE_NAME);

        let mut file = File::create(&cache_path)?;
        file.write_all(version.to_string().as_bytes())?;

        Ok(())
    }

    /// Checks if the last timestamp in the temp file has more than one week old.
    fn is_cache_older_than_a_week(&self, stored_timestamp: SystemTime) -> Result<bool, Error> {
        if SystemTime::now()
            .duration_since(stored_timestamp)
            .map_err(|_| Error::TimestampConversionError)?
            <= time::Duration::weeks(1)
        {
            return Ok(true);
        }

        Ok(false)
    }

    fn get_cached_timpestamp_and_version(&self) -> Result<Option<(SystemTime, String)>, Error> {
        let mut cache_path: PathBuf = dirs::cache_dir().ok_or(Error::CacheDirNotFoundError)?;
        cache_path.push(CACHE_FILE_NAME);

        if !Path::new(&cache_path).exists() {
            // Trigger the check if the file does not exist.
            return Ok(None);
        }

        let file = OpenOptions::new().read(true).open(&cache_path)?;
        let modified_time = file.metadata()?.modified()?;
        let mut reader = BufReader::new(file);
        let mut reg_version = String::new();
        reader.read_line(&mut reg_version)?;

        Ok(Some((modified_time, reg_version)))
    }

    /// Returns true if the installed version of `mz`
    /// is older than the version sent by parameter.
    fn is_installed_version_older_than(&self, version: Version) -> bool {
        let installed_version = build_info!().semver_version();

        println!("inst: {} / lat: {}", installed_version, version);
        installed_version > version
    }

    /// Fetches and returns the latest tag version from the Materialize repository
    /// using the GitHub public API.
    async fn get_latest_tag_version(&self) -> Result<Version, Error> {
        // The policy must be set to None.
        // Otherwise the client will handle the redirection.
        let client = Client::builder().redirect(Policy::none()).build()?;

        // Set an agent. Otherwise the response is not useful.
        // let mut headers = HeaderMap::new();
        // let user_agent = format!("mz/{}", VERSION.clone());
        // headers.insert(
        //     USER_AGENT,
        //     HeaderValue::from_str(&user_agent).map_err(Error::HeaderParseError)?,
        // );
        let response = client
            .get(BINARIES_LATEST_VERSION_URL)
            // .headers(headers)
            .send()
            .await?;

        // If the server returns a redirect, you can usually find the location header.
        if let Some(location) = response.headers().get("location") {
            let location_str = location.to_str().map_err(Error::HeaderToStrError)?;
            let version = location_str.split("-").collect::<Vec<&str>>()[1]
                .strip_prefix("v")
                .ok_or(Error::LocationInvalidError)?;
            let latest_version = Version::parse(&version).map_err(Error::SemVerParseError)?;
            println!("Latest version: {}", latest_version);
            return Ok(latest_version);
        }

        Err(Error::MissingLocationError)
    }

    /// Prints the warning message to update mz if the check result is true.
    pub fn print_warning_if_needed(&self, check_result: Result<bool, Error>) {
        match check_result {
            Ok(check) => {
                if check {
                    let of = OutputFormatter::new(mz::ui::OutputFormat::Text, self.no_color);

                    // We can improve this, but I'm following a more KISS (Keep It Simple..) approach.
                    // Previously, there was a proper message for each operating system on how to
                    // apply the update, e.g., 'Run `brew upgrade ...`.'
                    // By doing this, we had to differentiate if the user
                    // installed `mz` through Brew or by curling the binary.
                    // Displaying the version also requires making another
                    // request to verify if it is the latest version or not.
                    // This message is simple. It works. Nothing fancy.
                    let _ = of.output_warning("New version available. Update now.");
                }
            }
            _ => {}
        }
    }

    /// This function checks if `mz` needs an update. The function
    /// returns true if the installed version is at least one version older.
    ///
    /// Additionally, the check caches the results for a week
    /// to avoid slowing down commands and
    /// sending excessive requests to GitHub.
    ///
    /// The cached results contains the timestamp and
    /// version fetched in a file within the cache dir
    /// using `dirs::cache_dir()`.
    pub async fn check_version(&self) -> Result<bool, Error> {
        // Check cached data first
        if let Some((last_modified_time, cached_version)) =
            self.get_cached_timpestamp_and_version()?
        {
            println!(
                "Cache mod: {:?} / version: {} ",
                last_modified_time, cached_version
            );
            // If the already cached version is greater than the local version
            // Warn the user and avoid querying GitHub
            let cached_version =
                Version::parse(&cached_version).map_err(Error::SemVerParseError)?;
            if self.is_installed_version_older_than(cached_version) {
                println!("Return true");
                return Ok(true);
            }

            // If the cache is not older than a week then there
            // is no need to check new versions yet.
            if !self.is_cache_older_than_a_week(last_modified_time)? {
                println!("Return false");
                return Ok(false);
            }
        }

        // Fetch the latest version from GitHub
        match self.get_latest_tag_version().await {
            Ok(tag_version) => {
                self.update_cache_file(&tag_version)?;
                if self.is_installed_version_older_than(tag_version) {
                    return Ok(true);
                }
            }
            Err(e) => {
                println!("Error: {}", e);
                return Err(e);
            }
        }

        Ok(false)
    }
}
