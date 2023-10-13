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

//! Driver for the `mz profile` command.

use mz::command::profile::{ConfigGetArgs, ConfigRemoveArgs, ConfigSetArgs};
use mz::context::Context;
use mz::error::Error;

use crate::mixin::EndpointArgs;

#[derive(Debug, clap::Args)]
pub struct ProfileCommand {
    #[clap(subcommand)]
    subcommand: ProfileSubcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum ProfileSubcommand {
    /// Iniitialize an authentication profile.
    Init {
        /// Prompt for a username and password on the terminal.
        #[clap(long)]
        no_browser: bool,
        /// The admin or cloud endpoint to use.
        #[clap(flatten)]
        endpoint: EndpointArgs,
        /// Force reauthentication if the profile already exists.
        #[clap(short, long)]
        force: bool,
    },
    /// List available authentication profiles.
    #[clap(alias = "ls")]
    List,
    /// Remove an authentication profile.
    #[clap(alias = "rm")]
    Remove,
    /// Configure an authentication profile.
    #[clap(subcommand)]
    Config(ProfileConfigSubcommand),
}

#[derive(Debug, clap::Subcommand)]
pub enum ProfileConfigSubcommand {
    /// Get a configuration parameter in an authentication profile.
    Get {
        /// The name of the configuration parameter to get.
        name: String,
    },
    /// List all configuration parameters in an authentication profile.
    #[clap(alias = "ls")]
    List,
    /// Set a configuration parameter in an authentication profile.
    Set {
        /// The name of the configuration parameter to set.
        name: String,
        /// The value to set the configuration parameter to.
        value: String,
    },
    /// Remove a configuration parameter from an authentication profile.
    #[clap(alias = "rm")]
    Remove {
        /// The name of the configuration parameter to remove.
        name: String,
    },
}

pub async fn run(cx: Context, cmd: ProfileCommand) -> Result<(), Error> {
    match &cmd.subcommand {
        // Initiating a profile doesn't requires an active profile.
        ProfileSubcommand::Init {
            no_browser,
            endpoint,
            force,
        } => {
            mz::command::profile::init(
                &cx,
                *no_browser,
                *force,
                endpoint.admin_endpoint.clone(),
                endpoint.cloud_endpoint.clone(),
            )
            .await
        }
        ProfileSubcommand::List => mz::command::profile::list(&cx),
        ProfileSubcommand::Remove => mz::command::profile::remove(&cx).await,
        _ => {
            let cx = cx.activate_profile()?;

            match &cmd.subcommand {
                ProfileSubcommand::Config(cmd) => match cmd {
                    ProfileConfigSubcommand::Get { name } => {
                        mz::command::profile::config_get(&cx, ConfigGetArgs { name })
                    }
                    ProfileConfigSubcommand::List => mz::command::profile::config_list(&cx),
                    ProfileConfigSubcommand::Set { name, value } => {
                        mz::command::profile::config_set(&cx, ConfigSetArgs { name, value }).await
                    }
                    ProfileConfigSubcommand::Remove { name } => {
                        mz::command::profile::config_remove(&cx, ConfigRemoveArgs { name }).await
                    }
                },
                ProfileSubcommand::Init {
                    no_browser: _,
                    endpoint: _,
                    force: _,
                } => panic!("invalid command."),
                ProfileSubcommand::List => panic!("invalid command."),
                ProfileSubcommand::Remove => panic!("invalid command."),
            }
        }
    }
}
