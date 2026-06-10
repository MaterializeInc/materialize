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

//! Driver for the `mz region` command.

use mz::context::Context;
use mz::error::Error;

#[derive(Debug, clap::Args)]
pub struct RegionCommand {
    #[clap(subcommand)]
    subcommand: RegionSubcommand,
}

#[derive(Debug, clap::Subcommand)]
pub enum RegionSubcommand {
    /// Enable a region.
    Enable {
        #[clap(hide = true, short, long)]
        version: Option<String>,
        #[clap(hide = true, short, long)]
        environmentd_extra_arg: Option<Vec<String>>,
        #[clap(hide = true, long)]
        environmentd_cpu_allocation: Option<String>,
        #[clap(hide = true, long)]
        environmentd_memory_allocation: Option<String>,
    },
    /// Disable a region.
    #[clap(hide = true)]
    Disable {
        #[clap(long)]
        hard: bool,
    },
    /// List all regions.
    #[clap(alias = "ls")]
    List,
    /// Show detailed status for a region.
    Show,
}

pub async fn run(cx: Context, cmd: RegionCommand) -> Result<(), Error> {
    let cx = cx.activate_profile()?.activate_region()?;
    match cmd.subcommand {
        RegionSubcommand::Enable {
            version,
            environmentd_extra_arg,
            environmentd_cpu_allocation,
            environmentd_memory_allocation,
        } => {
            mz::command::region::enable(
                cx,
                version,
                environmentd_extra_arg,
                environmentd_cpu_allocation,
                environmentd_memory_allocation,
            )
            .await
        }
        RegionSubcommand::Disable { hard } => mz::command::region::disable(cx, hard).await,
        RegionSubcommand::List => mz::command::region::list(cx).await,
        RegionSubcommand::Show => mz::command::region::show(cx).await,
    }
}
