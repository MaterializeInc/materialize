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

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::collapsible_if)]
#![warn(clippy::collapsible_else_if)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

use std::path::PathBuf;

use command::profile::ProfileSubcommand;
use mz::error::Error;

use mz::context::{Context, ContextLoadArgs};
use mz_ore::cli::CliConfig;

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

    // TODO: Check for an updated version of `mz` and print a warning if one
    // is discovered. Can we use the GitHub tags API for this?
    let cx = Context::load(ContextLoadArgs {
        config_file_path: args.config,
        output_format: args.format.into(),
        // TODO: no_color: env::no_color() || args.no_color,
        no_color: args.no_color,
    })
    .await?;

    match args.command {
        Command::AppPassword(cmd) => command::app_password::run(cx, cmd).await,
        Command::Config(cmd) => command::config::run(cx, cmd).await,
        Command::Profile(cmd) => command::profile::run(cx, cmd).await,
        Command::Region(cmd) => command::region::run(cx, cmd).await,
        Command::Secret(cmd) => command::secret::run(cx, cmd).await,
        Command::Sql(cmd) => command::sql::run(cx, cmd).await,
        Command::User(cmd) => command::user::run(cx, cmd).await,
    }
}
