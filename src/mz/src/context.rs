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
//! The implementation of each command in the [`command`] module takes exactly
//! one of these context types, depending on whether it requires access to a
//! valid authentication profile and active region.

use std::path::PathBuf;

use crate::config_file::ConfigFile;
use crate::error::Error;
use crate::ui::{OutputFormat, OutputFormatter};
use mz_cloud_api::client::Client as CloudClient;
use mz_cloud_api::config::{
    ClientBuilder as CloudClientBuilder, ClientConfig as CloudClientConfig,
};
use mz_frontegg_client::client::Client as AdminClient;
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
}

/// Context for a basic command.
pub struct Context {
    config_file: ConfigFile,
    output_formatter: OutputFormatter,
}

impl Context {
    /// Loads the context from the provided arguments.
    pub async fn load(
        ContextLoadArgs {
            config_file_path,
            output_format,
            no_color,
        }: ContextLoadArgs,
    ) -> Result<Context, Error> {
        let config_file_path = match config_file_path {
            None => ConfigFile::default_path()?,
            Some(path) => path,
        };
        let config_file = ConfigFile::load(config_file_path).await?;
        Ok(Context {
            config_file,
            output_formatter: OutputFormatter::new(output_format),
        })
    }

    /// Converts this context into a [`ProfileContext`].
    ///
    /// If a profile is not specified, the default profile is activated.
    pub async fn activate_profile(self, name: Option<String>) -> Result<ProfileContext, Error> {
        let profile_name = name.unwrap_or_else(|| self.config_file.profile().into());
        let profile = self.config_file.load_profile(&profile_name)?;
        // TODO: Only one client should do the work.
        let admin_client = AdminClientBuilder::default()
            .endpoint(profile.admin_endpoint().parse().unwrap())
            .build(AdminClientConfig {
                app_password: profile.app_password().parse()?,
            });

        let context_admin_client = AdminClientBuilder::default()
            .endpoint(profile.admin_endpoint().parse().unwrap())
            .build(AdminClientConfig {
                app_password: profile.app_password().parse()?,
            });

        let cloud_client = CloudClientBuilder::default()
            .endpoint(profile.cloud_endpoint().parse().unwrap())
            .build(CloudClientConfig {
                auth_client: admin_client,
            });

        Ok(ProfileContext {
            context: self,
            profile_name,
            admin_client: context_admin_client,
            cloud_client,
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
    admin_client: AdminClient,
    cloud_client: CloudClient,
}

impl ProfileContext {
    pub fn activate_region(self, name: Option<String>) -> Result<RegionContext, Error> {
        let profile = self
            .context
            .config_file
            .load_profile(&self.profile_name)
            .unwrap();
        let region_name = name
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
    pub fn sql_client(&self) {
        todo!()
    }

    /// Returns the `psql` arguments required to connect to the region
    /// associated with this context.
    pub fn psql_args(&self) {
        todo!()
    }

    /// Returns the configuration file loaded by this context.
    pub fn config_file(&self) -> &ConfigFile {
        &self.context.config_file()
    }

    /// Returns the output_formatter associated with this context.
    pub fn output_formatter(&self) -> &OutputFormatter {
        &self.context.output_formatter()
    }
}
