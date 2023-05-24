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
    path::{Path, PathBuf},
    process::Command,
};

use mz_cloud_api::client::environment::Environment;
use mz_frontegg_auth::AppPassword;
use url::Url;

/// The [application_name](https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME)
/// which gets reported to the Postgres server we're connecting to.
const PG_APPLICATION_NAME: &str = "mz_psql";

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
    fn build_psql_url(&self, environment: &Environment, email: String) -> Url {
        let mut url = Url::parse(&format!(
            "postgres://{}",
            environment.environmentd_pgwire_address
        ))
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

        url
    }

    /// Returns a sql shell command associated with this context
    pub fn shell(&self, environment: &Environment, email: String) -> Command {
        let mut command = Command::new("psql");
        command
            .arg(self.build_psql_url(environment, email).as_str())
            .env("PGPASSWORD", &self.app_password.to_string())
            .env("PGAPPNAME", PG_APPLICATION_NAME);

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
    pub fn is_ready(&self, environment: &Environment, email: String) -> Command {
        if let Some(_) = self.find("pg_isready") {
            let mut command = Command::new("pg_isready");
            command
                .args(vec![
                    "-q",
                    "-d",
                    self.build_psql_url(environment, email).as_str(),
                ])
                .env("PGPASSWORD", &self.app_password.to_string())
                .env("PGAPPNAME", PG_APPLICATION_NAME);

            command
        } else {
            panic!("the pg_isready program is not present. Make sure it is available in the $PATH.")
        }
    }
}
