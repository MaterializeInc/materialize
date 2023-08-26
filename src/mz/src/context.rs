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

//! Context types for command implementations.
//!
//! The implementation of each command in the [crate::command] module takes exactly
//! one of these context types, depending on whether it requires access to a
//! valid authentication profile and active region.

use std::path::PathBuf;
use std::sync::Arc;

use crate::config_file::ConfigFile;
use crate::error::Error;
use crate::sql_client::{Client as SqlClient, ClientConfig as SqlClientConfig};
use crate::ui::{OutputFormat, OutputFormatter};
use crate::versioner::warn_version_if_necessary;
use mz_cloud_api::client::cloud_provider::CloudProvider;
use mz_cloud_api::client::region::{Region, RegionInfo};
use mz_cloud_api::client::Client as CloudClient;
use mz_cloud_api::config::{
    ClientBuilder as CloudClientBuilder, ClientConfig as CloudClientConfig,
};
use mz_frontegg_client::client::{Authentication, Client as AdminClient};
use mz_frontegg_client::config::{
    ClientBuilder as AdminClientBuilder, ClientConfig as AdminClientConfig,
};

/// Arguments for [`Context::load`].
pub struct ContextLoadArgs {
    /// An override for the configuration file path to laod.
    ///
    /// If unspecified, the default configuration file path is used.
    pub config_file_path: Option<PathBuf>,
    /// The output format to use.
    pub output_format: OutputFormat,
    /// Whether to suppress color output.
    pub no_color: bool,
    /// Global optional region.
    pub region: Option<String>,
}

/// Context for a basic command.
pub struct Context {
    config_file: ConfigFile,
    output_formatter: OutputFormatter,
    region: Option<String>,
    exit_message: Result<Option<String>, Error>,
}

/// Implements Drop for the context.
/// Before the `mz` command ends its life,
/// is the same time the context ends (`src/bin/mz/main.rs`),
/// it checks if there is any exit message to print,
/// like a version upgrade.
///
/// TODO: This approach doesn't work if there is an error.
/// The context is dropped before exiting and the error is print later.
impl Drop for Context {
    fn drop(&mut self) {
        match &self.exit_message {
            Ok(msg) => {
                if let Some(msg) = msg {
                    println!("\n{}", msg);
                }
            }
            Err(_) => {}
        }
    }
}

impl Context {
    /// Loads the context from the provided arguments.
    pub async fn load(
        ContextLoadArgs {
            config_file_path,
            output_format,
            no_color,
            region,
        }: ContextLoadArgs,
    ) -> Result<Context, Error> {
        // Check if we need to update the version.
        let exit_message = warn_version_if_necessary().await;

        let config_file_path = match config_file_path {
            None => ConfigFile::default_path()?,
            Some(path) => path,
        };
        let config_file = ConfigFile::load(config_file_path).await?;
        Ok(Context {
            config_file,
            output_formatter: OutputFormatter::new(output_format, no_color),
            region,
            exit_message,
        })
    }

    /// Converts this context into a [`ProfileContext`].
    ///
    /// If a profile is not specified, the default profile is activated.
    pub fn activate_profile(self, name: Option<String>) -> Result<ProfileContext, Error> {
        let profile_name = name.unwrap_or_else(|| self.config_file.profile().into());
        let config_file = self.config_file.clone();
        let profile = config_file.load_profile(&profile_name)?;

        // Build clients
        let mut admin_client_builder = AdminClientBuilder::default();

        if let Some(admin_endpoint) = profile.admin_endpoint() {
            admin_client_builder = admin_client_builder.endpoint(admin_endpoint.parse()?);
        }

        let admin_client: Arc<AdminClient> = Arc::new(
            admin_client_builder.build(AdminClientConfig {
                authentication: Authentication::AppPassword(
                    profile
                        .app_password()
                        .ok_or(Error::AppPasswordMissing)?
                        .parse()?,
                ),
            }),
        );

        let mut cloud_client_builder = CloudClientBuilder::default();

        if let Some(cloud_endpoint) = profile.cloud_endpoint() {
            cloud_client_builder = cloud_client_builder.endpoint(cloud_endpoint.parse()?);
        }

        let cloud_client = cloud_client_builder.build(CloudClientConfig {
            auth_client: Arc::clone(&admin_client),
        });

        // The sql client is created here to avoid having to handle the config around. E.g. reading config from config_file
        // this happens because profile is 'a static, and adding it to the profile context would make also the context 'a, etc.
        let sql_client = SqlClient::new(SqlClientConfig {
            app_password: profile
                .app_password()
                .ok_or(Error::AppPasswordMissing)?
                .parse()?,
        });

        Ok(ProfileContext {
            context: self,
            profile_name,
            admin_client,
            cloud_client,
            sql_client,
        })
    }

    /// Returns the configuration file loaded by this context.
    pub fn config_file(&self) -> &ConfigFile {
        &self.config_file
    }

    /// Returns the output_formatter associated with this context.
    pub fn output_formatter(&self) -> &OutputFormatter {
        &self.output_formatter
    }
}

/// Context for a command that requires a valid authentication profile.
pub struct ProfileContext {
    context: Context,
    profile_name: String,
    admin_client: Arc<AdminClient>,
    cloud_client: CloudClient,
    sql_client: SqlClient,
}

impl ProfileContext {
    /// Loads the profile and returns a region context.
    pub fn activate_region(self) -> Result<RegionContext, Error> {
        let profile = self
            .context
            .config_file
            .load_profile(&self.profile_name)
            .unwrap();
        let region_name = self
            .context
            .region
            .clone()
            .or(profile.region().map(|r| r.to_string()))
            .ok_or_else(|| panic!("no region configured"))
            .unwrap();
        Ok(RegionContext {
            context: self,
            region_name,
        })
    }

    /// Returns the admin API client associated with this context.
    pub fn admin_client(&self) -> &AdminClient {
        &self.admin_client
    }

    /// Returns the cloud API client associated with this context.
    pub fn cloud_client(&self) -> &CloudClient {
        &self.cloud_client
    }

    /// Returns the configuration file loaded by this context.
    pub fn config_file(&self) -> &ConfigFile {
        &self.context.config_file
    }

    /// Returns the output_formatter associated with this context.
    pub fn output_formatter(&self) -> &OutputFormatter {
        &self.context.output_formatter
    }
}

/// Context for a command that requires a valid authentication profile
/// and an active region.
pub struct RegionContext {
    context: ProfileContext,
    region_name: String,
}

impl RegionContext {
    /// Returns the admin API client associated with this context.
    pub fn admin_client(&self) -> &AdminClient {
        &self.context.admin_client
    }

    /// Returns the admin API client associated with this context.
    pub fn cloud_client(&self) -> &CloudClient {
        &self.context.cloud_client
    }

    /// Returns a SQL client connected to region associated with this context.
    pub fn sql_client(&self) -> &SqlClient {
        &self.context.sql_client
    }

    /// Returns the cloud provider from the profile context.
    pub async fn get_cloud_provider(&self) -> Result<CloudProvider, Error> {
        let client = &self.context.cloud_client;
        let cloud_providers = client.list_cloud_providers().await?;

        let provider = cloud_providers
            .into_iter()
            .find(|x| x.id == self.region_name)
            .ok_or(Error::CloudProviderMissing)?;

        Ok(provider)
    }

    /// Returns the cloud provider region of the context.
    pub async fn get_region(&self) -> Result<Region, Error> {
        let client = self.cloud_client();
        let cloud_provider = self.get_cloud_provider().await?;
        let region = client.get_region(cloud_provider).await?;

        Ok(region)
    }

    /// Returns the cloud provider region of the context.
    pub async fn get_region_info(&self) -> Result<RegionInfo, Error> {
        let client = self.cloud_client();
        let cloud_provider = self.get_cloud_provider().await?;
        let region = client.get_region(cloud_provider).await?;

        region.region_info.ok_or_else(|| Error::NotReadyRegion)
    }

    /// Returns the configuration file loaded by this context.
    pub fn config_file(&self) -> &ConfigFile {
        self.context.config_file()
    }

    /// Returns the output_formatter associated with this context.
    pub fn output_formatter(&self) -> &OutputFormatter {
        self.context.output_formatter()
    }
}
