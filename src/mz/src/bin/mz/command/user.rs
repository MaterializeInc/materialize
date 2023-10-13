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

//! Driver for the `mz user` command.

use mz::command::user::{CreateArgs, RemoveArgs};
use mz::context::Context;
use mz::error::Error;

#[derive(Debug, clap::Args)]
pub struct UserCommand {
    #[clap(subcommand)]
    subcommand: UserSubcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum UserSubcommand {
    /// Invite a user to your organization.
    Create {
        /// Set the email address of the user.
        email: String,
        /// Set the name of the user.
        name: String,
    },
    /// List all users in your organization.
    #[clap(alias = "ls")]
    List,
    /// Remove a user from your organization.
    #[clap(alias = "rm")]
    Remove {
        /// The email address of the user to remove.
        email: String,
    },
}

pub async fn run(cx: Context, cmd: UserCommand) -> Result<(), Error> {
    let cx = cx.activate_profile()?;
    match &cmd.subcommand {
        UserSubcommand::Create { email, name } => {
            mz::command::user::create(&cx, CreateArgs { email, name }).await
        }
        UserSubcommand::List => mz::command::user::list(&cx).await,
        UserSubcommand::Remove { email } => {
            mz::command::user::remove(&cx, RemoveArgs { email }).await
        }
    }
}
