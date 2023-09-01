// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    env,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
};

use mz_cloud_api::client::region::RegionInfo;
use mz_frontegg_auth::AppPassword;
use url::Url;

use crate::error::Error;

/// The [application_name](https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME)
/// which gets reported to the Postgres server we're connecting to.
const PG_APPLICATION_NAME: &str = "mz_psql";

/// Default filename for the custom .psqlrc file.
const PG_PSQLRC_MZ_FILENAME: &str = ".psqlrc-mz";

// Default content for the .psqlrc-mz file.
// It enables timing and includes all the configuration from
// the main `.psqlrc` file.
const PG_PSQLRC_MZ_DEFAULT_CONTENT: &str = "\\timing\n\\include ~/.psqlrc";

/// Configures the required parameters of a [`Client`].
pub struct ClientConfig {
    /// A singular, legitimate app password that will remain in use to identify
    /// the user throughout the client's existence.
    pub app_password: AppPassword,
}

pub struct Client {
    pub(crate) app_password: AppPassword,
}

impl Client {
    /// Creates a new `Client` from its required configuration parameters.
    pub fn new(config: ClientConfig) -> Client {
        Client {
            app_password: config.app_password,
        }
    }

    /// Build the PSQL url to connect into a environment
    fn build_psql_url(
        &self,
        region_info: &RegionInfo,
        email: String,
        cluster: Option<String>,
    ) -> Url {
        let mut url = Url::parse(&format!("postgres://{}", region_info.sql_address))
            .expect("url known to be valid");
        url.set_username(&email).unwrap();
        url.set_path("materialize");

        if let Some(cert_file) = openssl_probe::probe().cert_file {
            url.query_pairs_mut()
                .append_pair("sslmode", "verify-full")
                .append_pair("sslrootcert", &cert_file.to_string_lossy());
        } else {
            url.query_pairs_mut().append_pair("sslmode", "require");
        }

        if let Some(cluster) = cluster {
            url.query_pairs_mut()
                .append_pair("options", &format!("--cluster={}", cluster));
        }

        url
    }

    /// Creates and fills a file with content
    /// if it does not exists.
    fn create_file_with_content_if_not_exists(
        &self,
        path: &PathBuf,
        content: Option<&[u8]>,
    ) -> Result<(), Error> {
        // Create the new file and use `.create_new(true)` to avoid
        // race conditions: https://doc.rust-lang.org/stable/std/fs/struct.OpenOptions.html#method.create_new
        match OpenOptions::new().write(true).create_new(true).open(path) {
            Ok(mut file) => {
                if let Some(content) = content {
                    let _ = file.write_all(content);
                }
            }
            Err(e) => {
                if e.kind() == std::io::ErrorKind::AlreadyExists {
                    // Do nothing.
                } else {
                    return Err(Error::IOError(e));
                }
            }
        }

        Ok(())
    }

    /// This function configures an own .psqlrc-mz file
    /// to include the '\timing' function every time
    /// the user executes the `mz sql` command.
    pub fn configure_psqlrc(&self) -> Result<(), Error> {
        // Look for the '.psqlrc' file in the home dir.
        let Some(mut path) = dirs::home_dir() else {
            return Err(Error::HomeDirNotFoundError);
        };
        path.push(PG_PSQLRC_MZ_FILENAME);

        let _ = self.create_file_with_content_if_not_exists(
            &path,
            Some(PG_PSQLRC_MZ_DEFAULT_CONTENT.as_bytes()),
        );

        // Check if '.psqlrc' exists, if it doesn't, create one.
        // Otherwise the '\include ~/.psqlrc' line
        // will throw an error message in every execution.
        path.pop();
        path.push(".psqlrc");
        let _ = self.create_file_with_content_if_not_exists(&path, None);

        Ok(())
    }

    /// Returns a sql shell command associated with this context
    pub fn shell(
        &self,
        region_info: &RegionInfo,
        email: String,
        cluster: Option<String>,
    ) -> Command {
        // Feels ok to avoid stopping the executing if
        // we can't configure the file.
        // Worst case scenario timing will not be enabled.
        let _ = self.configure_psqlrc();

        let mut command = Command::new("psql");
        command
            .arg(self.build_psql_url(region_info, email, cluster).as_str())
            .env("PGPASSWORD", &self.app_password.to_string())
            .env("PGAPPNAME", PG_APPLICATION_NAME)
            .env("PSQLRC", "~/.psqlrc-mz");

        command
    }

    fn find<P>(&self, exe_name: P) -> Option<PathBuf>
    where
        P: AsRef<Path>,
    {
        env::var_os("PATH").and_then(|paths| {
            env::split_paths(&paths)
                .filter_map(|dir| {
                    let full_path = dir.join(&exe_name);
                    if full_path.is_file() {
                        Some(full_path)
                    } else {
                        None
                    }
                })
                .next()
        })
    }

    /// Runs pg_isready to check if an environment is healthy
    pub fn is_ready(&self, region_info: &RegionInfo, email: String) -> Result<bool, Error> {
        if self.find("pg_isready").is_some() {
            let mut command = Command::new("pg_isready");
            Ok(command
                .args(vec![
                    "-q",
                    "-d",
                    self.build_psql_url(region_info, email, None).as_str(),
                ])
                .env("PGPASSWORD", &self.app_password.to_string())
                .env("PGAPPNAME", PG_APPLICATION_NAME)
                .output()?
                .status
                .success())
        } else {
            panic!("the pg_isready program is not present. Make sure it is available in the $PATH.")
        }
    }
}
