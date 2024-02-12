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

//! Command-line driver for `mz`.

use std::path::PathBuf;
use std::process::exit;

use mz::error::Error;

use mz::context::{Context, ContextLoadArgs};
use mz_ore::cli::CliConfig;
use upgrader::UpgradeChecker;

use crate::command::app_password::AppPasswordCommand;
use crate::command::config::ConfigCommand;
use crate::command::profile::ProfileCommand;
use crate::command::region::RegionCommand;
use crate::command::secret::SecretCommand;
use crate::command::sql::SqlCommand;
use crate::command::user::UserCommand;
use clap_clippy_hack::*;

mod command;
mod mixin;
mod upgrader;

// Do not add anything but structs/enums with Clap derives in this module!
//
// Clap v3 sometimes triggers this warning with subcommands,
// and its unclear if it will be fixed in v3, and not just
// in v4. This can't be overridden at the macro level, and instead must be overridden
// at the module level.
//
// TODO(guswynn): remove this when we are using Clap v4.
#[allow(clippy::almost_swapped)]
mod clap_clippy_hack {

    use super::*;

    /// Command-line interface for Materialize.
    #[derive(Debug, clap::Parser)]
    #[clap(
        long_about = None,
        version = mz::VERSION.as_str(),
    )]
    pub struct Args {
        /// Set the configuration file.
        #[clap(long, global = true, value_name = "PATH", env = "CONFIG")]
        pub(crate) config: Option<PathBuf>,
        #[clap(long, global = true, env = "FORMAT", value_enum, default_value_t)]
        pub(crate) format: OutputFormat,
        #[clap(long, global = true, env = "NO_COLOR")]
        pub(crate) no_color: bool,
        #[clap(subcommand)]
        pub(crate) command: Command,
        #[clap(long, env = "REGION", global = true)]
        pub(crate) region: Option<String>,
        #[clap(long, env = "PROFILE", global = true, value_parser = mixin::validate_profile_name)]
        pub(crate) profile: Option<String>,
    }

    /// Specifies an output format.
    #[derive(Debug, Clone, Default, clap::ValueEnum)]
    pub enum OutputFormat {
        /// Text output.
        #[default]
        Text,
        /// JSON output.
        Json,
        /// CSV output.
        Csv,
    }

    impl From<OutputFormat> for mz::ui::OutputFormat {
        fn from(value: OutputFormat) -> Self {
            match value {
                OutputFormat::Text => mz::ui::OutputFormat::Text,
                OutputFormat::Json => mz::ui::OutputFormat::Json,
                OutputFormat::Csv => mz::ui::OutputFormat::Csv,
            }
        }
    }

    #[derive(Debug, clap::Subcommand)]
    pub enum Command {
        /// Manage app passwords for your user account.
        AppPassword(AppPasswordCommand),
        /// Manage global configuration parameters for the CLI.
        #[clap(subcommand)]
        Config(ConfigCommand),
        /// Manage authentication profiles for the CLI.
        Profile(ProfileCommand),
        /// Manage regions in your organization.
        Region(RegionCommand),
        /// Manage secrets in a region.
        Secret(SecretCommand),
        /// Execute SQL statements in a region.
        Sql(SqlCommand),
        /// Manage users in your organization.
        User(UserCommand),
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args: Args = mz_ore::cli::parse_args(CliConfig {
        env_prefix: Some("MZ_"),
        enable_version_flag: true,
    });

    // Check if we need to update the version.
    // The UpgradeChecker, in case of failure,
    // shouldn't block the command execution or panic.
    let upgrade_checker = UpgradeChecker::new(args.no_color);
    let check = upgrade_checker.check_version();

    let cx = Context::load(ContextLoadArgs {
        config_file_path: args.config,
        output_format: args.format.into(),
        no_color: args.no_color,
        region: args.region,
        profile: args.profile,
    })
    .await?;

    let res = match args.command {
        Command::AppPassword(cmd) => command::app_password::run(cx, cmd).await,
        Command::Config(cmd) => command::config::run(cx, cmd).await,
        Command::Profile(cmd) => command::profile::run(cx, cmd).await,
        Command::Region(cmd) => command::region::run(cx, cmd).await,
        Command::Secret(cmd) => command::secret::run(cx, cmd).await,
        Command::Sql(cmd) => command::sql::run(cx, cmd).await,
        Command::User(cmd) => command::user::run(cx, cmd).await,
    };

    if let Err(e) = res {
        println!("{}", e);

        upgrade_checker.print_warning_if_needed(check.await);
        exit(1);
    }

    upgrade_checker.print_warning_if_needed(check.await);
    Ok(())
}
