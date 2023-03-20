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

use mz::command::profile::{ConfigSetArgs, ConfigRemoveArgs, ConfigGetArgs};
use mz::context::Context;

use crate::mixin::ProfileArg;

#[derive(Debug, clap::Args)]
pub struct ProfileCommand {
    #[clap(flatten)]
    profile: ProfileArg,
    #[clap(subcommand)]
    subcommand: ProfileSubcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum ProfileSubcommand {
    /// Iniitialize an authentication profile.
    Init,
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
    }
}

pub async fn run(cx: Context, cmd: ProfileCommand) -> Result<(), anyhow::Error> {
    let mut cx = cx
        .activate_profile(cmd.profile.profile)
        .await?;
    match &cmd.subcommand {
        ProfileSubcommand::Init => mz::command::profile::init(&mut cx).await,
        ProfileSubcommand::List => mz::command::profile::list(&mut cx).await,
        ProfileSubcommand::Remove => mz::command::profile::remove(&mut cx).await,
        ProfileSubcommand::Config(cmd) => match cmd {
            ProfileConfigSubcommand::Get { name } => mz::command::profile::config_get(&mut cx, ConfigGetArgs { name }).await,
            ProfileConfigSubcommand::List => mz::command::profile::config_list(&mut cx).await,
            ProfileConfigSubcommand::Set { name, value } => mz::command::profile::config_set(&mut cx, ConfigSetArgs { name, value }).await,
            ProfileConfigSubcommand::Remove { name } => mz::command::profile::config_remove(&mut cx, ConfigRemoveArgs { name }).await,
        }
    }
}
