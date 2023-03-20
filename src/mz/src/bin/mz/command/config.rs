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

//! Driver for the `mz config` command.

use mz::command::config::{GetArgs, RemoveArgs, SetArgs};
use mz::context::Context;

#[derive(Debug, clap::Subcommand)]
pub enum ConfigCommand {
    /// Get the value of a configuration parameter.
    Get {
        /// The name of the configuration parameter to get.
        name: String,
    },
    /// List all configuration parameters.
    #[clap(alias = "ls")]
    List,
    /// Set a configuration parameter.
    Set {
        /// The name of the configuration parameter to set.
        name: String,
        /// The value to set the configuration parameter to.
        value: String,
    },
    /// Remove a configuration parameter.
    #[clap(alias = "rm")]
    Remove {
        /// The name of the configuration parameter to remove.
        name: String,
    },
}

pub async fn run(mut cx: Context, cmd: ConfigCommand) -> Result<(), anyhow::Error> {
    match &cmd {
        ConfigCommand::Get { name } => mz::command::config::get(&mut cx, GetArgs { name }),
        ConfigCommand::List => mz::command::config::list(&mut cx),
        ConfigCommand::Set { name, value } => {
            mz::command::config::set(&mut cx, SetArgs { name, value }).await
        }
        ConfigCommand::Remove { name } => {
            mz::command::config::remove(&mut cx, RemoveArgs { name }).await
        }
    }
}
