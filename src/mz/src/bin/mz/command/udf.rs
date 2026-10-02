// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Driver for the `mz udf` command.

use std::path::PathBuf;

use mz::command::udf::{
    CallArgs, CallOptions, CheckArgs, CreateArgs, PullArgs, ReplayArgs, SqlArgs,
};
use mz::context::Context;
use mz::error::Error;

#[derive(Debug, clap::Args)]
pub struct UdfCommand {
    #[clap(subcommand)]
    subcommand: UdfSubcommand,
}

/// Resource limits and batching for local calls, matching what a region
/// applies.
#[derive(Debug, clap::Args)]
pub struct LimitArgs {
    /// The fuel each guest call may use.
    #[clap(long)]
    fuel: Option<u64>,
    /// The memory limit of each guest call, in bytes.
    #[clap(long)]
    memory: Option<u64>,
    /// The most rows one guest call may carry.
    #[clap(long)]
    batch_rows: Option<usize>,
}

impl From<LimitArgs> for CallOptions {
    fn from(args: LimitArgs) -> Self {
        CallOptions {
            fuel: args.fuel,
            memory: args.memory,
            batch_rows: args.batch_rows,
        }
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum UdfSubcommand {
    /// Validate a module and list the functions it exports.
    Check {
        /// The WebAssembly module.
        module: PathBuf,
        /// Check that FUNCTION's result for each row of --input does not depend on
        /// how rows are batched.
        #[clap(long, requires = "input")]
        determinism: Option<String>,
        /// A headerless CSV file of argument rows.
        #[clap(long)]
        input: Option<PathBuf>,
        #[clap(flatten)]
        limits: LimitArgs,
    },
    /// Print the psql command (psql 16 or later) that creates a module's function.
    Sql {
        /// The WebAssembly module.
        module: PathBuf,
        /// The guest function, by name or by signature.
        function: String,
        /// The SQL name of the function. Defaults to the guest name.
        #[clap(long)]
        name: Option<String>,
        /// Return NULL without calling the function when an argument is NULL.
        #[clap(long)]
        strict: bool,
    },
    /// Call a function locally with the runtime regions use.
    Call {
        /// The WebAssembly module.
        module: PathBuf,
        /// The guest function, by name or by signature.
        function: String,
        /// Arguments as SQL literals, or NULL.
        args: Vec<String>,
        /// Call the function on each row of a headerless CSV file instead.
        #[clap(long, conflicts_with = "args")]
        input: Option<PathBuf>,
        #[clap(flatten)]
        limits: LimitArgs,
    },
    /// Create a function in a region from a local module.
    Create {
        /// The WebAssembly module.
        module: PathBuf,
        /// The guest function, by name or by signature.
        function: String,
        /// The SQL name of the function. Defaults to the guest name.
        #[clap(long)]
        name: Option<String>,
        /// The database in which to create the function.
        #[clap(long)]
        database: Option<String>,
        /// The schema in which to create the function.
        #[clap(long)]
        schema: Option<String>,
        /// Return NULL without calling the function when an argument is NULL.
        #[clap(long)]
        strict: bool,
    },
    /// Download the module behind a function in a region.
    Pull {
        /// The function's name.
        name: String,
        /// The database that contains the function.
        #[clap(long)]
        database: Option<String>,
        /// Where to write the module. Defaults to NAME.wasm.
        #[clap(long, short)]
        output: Option<PathBuf>,
    },
    /// Run a query in a region and call a function locally on each row it
    /// returns, reporting the rows whose calls fail.
    Replay {
        /// The WebAssembly module.
        module: PathBuf,
        /// The guest function, by name or by signature.
        function: String,
        /// A query whose columns are the function's arguments.
        #[clap(long)]
        query: String,
        /// The database to run the query in.
        #[clap(long)]
        database: Option<String>,
        #[clap(flatten)]
        limits: LimitArgs,
    },
}

pub async fn run(cx: Context, cmd: UdfCommand) -> Result<(), Error> {
    match cmd.subcommand {
        UdfSubcommand::Check {
            module,
            determinism,
            input,
            limits,
        } => mz::command::udf::check(
            &cx,
            CheckArgs {
                module: &module,
                determinism: determinism.as_deref().zip(input.as_deref()),
                options: limits.into(),
            },
        ),
        UdfSubcommand::Sql {
            module,
            function,
            name,
            strict,
        } => mz::command::udf::sql(SqlArgs {
            module: &module,
            function: &function,
            name: name.as_deref(),
            strict,
        }),
        UdfSubcommand::Call {
            module,
            function,
            args,
            input,
            limits,
        } => mz::command::udf::call(
            &cx,
            CallArgs {
                module: &module,
                function: &function,
                args: &args,
                input: input.as_deref(),
                options: limits.into(),
            },
        ),
        UdfSubcommand::Create {
            module,
            function,
            name,
            database,
            schema,
            strict,
        } => {
            let cx = cx.activate_profile()?.activate_region()?;
            mz::command::udf::create(
                &cx,
                CreateArgs {
                    module: &module,
                    function: &function,
                    name: name.as_deref(),
                    database: database.as_deref(),
                    schema: schema.as_deref(),
                    strict,
                },
            )
            .await
        }
        UdfSubcommand::Pull {
            name,
            database,
            output,
        } => {
            let cx = cx.activate_profile()?.activate_region()?;
            let output = output.unwrap_or_else(|| mz::command::udf::default_pull_path(&name));
            mz::command::udf::pull(
                &cx,
                PullArgs {
                    name: &name,
                    database: database.as_deref(),
                    output: &output,
                },
            )
            .await
        }
        UdfSubcommand::Replay {
            module,
            function,
            query,
            database,
            limits,
        } => {
            let cx = cx.activate_profile()?.activate_region()?;
            mz::command::udf::replay(
                &cx,
                ReplayArgs {
                    module: &module,
                    function: &function,
                    query: &query,
                    database: database.as_deref(),
                    options: limits.into(),
                },
            )
            .await
        }
    }
}
