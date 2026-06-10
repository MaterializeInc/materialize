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

//! Driver for the `mz secret` command.

use mz::command::secret::CreateArgs;
use mz::context::Context;
use mz::error::Error;

#[derive(Debug, clap::Args)]
pub struct SecretCommand {
    #[clap(subcommand)]
    subcommand: SecretSubcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum SecretSubcommand {
    /// Create a new secret.
    Create {
        /// The database in which to create the secret.
        #[clap(long)]
        database: Option<String>,
        /// The schema in which to create the secret.
        #[clap(long)]
        schema: Option<String>,
        /// The name of the secret.
        name: String,
        /// Overwrite the existing value of the secret, if it exists.
        #[clap(long)]
        force: bool,
    },
}

pub async fn run(cx: Context, cmd: SecretCommand) -> Result<(), Error> {
    let cx = cx.activate_profile()?.activate_region()?;
    match cmd.subcommand {
        SecretSubcommand::Create {
            database,
            schema,
            name,
            force,
        } => {
            mz::command::secret::create(
                &cx,
                CreateArgs {
                    database: database.as_deref(),
                    schema: schema.as_deref(),
                    name: &name,
                    force,
                },
            )
            .await
        }
    }
}
