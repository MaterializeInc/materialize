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

//! Driver for the `mz app-password` command.

use mz::context::Context;
use mz_frontegg_client::client::app_password::CreateAppPasswordRequest;

use mz::error::Error;

#[derive(Debug, clap::Args)]
pub struct AppPasswordCommand {
    #[clap(subcommand)]
    subcommand: AppPasswordSubcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum AppPasswordSubcommand {
    /// Create an app password.
    Create {
        /// Set the name of the app password.
        ///
        /// If unspecified, `mz` automatically generates a name.
        #[clap(default_value = "Materialize CLI (mz)")]
        name: String,
    },
    /// List all app passwords.
    #[clap(alias = "ls")]
    List,
}

pub async fn run(cx: Context, cmd: AppPasswordCommand) -> Result<(), Error> {
    let cx = cx.activate_profile()?;
    match &cmd.subcommand {
        AppPasswordSubcommand::Create { name } => {
            mz::command::app_password::create(&cx, CreateAppPasswordRequest { description: name })
                .await?;
        }
        AppPasswordSubcommand::List => {
            mz::command::app_password::list(&cx).await?;
        }
    }

    Ok(())
}
