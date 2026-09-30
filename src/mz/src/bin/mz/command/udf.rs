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
    CallArgs, CallOptions, CheckArgs, CheckInput, CreateArgs, PullArgs, ReplayArgs, SqlArgs,
};
use mz::context::Context;
use mz::error::Error;

#[derive(Debug, clap::Args)]
pub struct UdfCommand {
    #[clap(subcommand)]
    subcommand: UdfSubcommand,
}

/// How local calls behave, matching what a region applies to the deployed
/// function.
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
    /// Return NULL without calling the guest when an argument is NULL, as
    /// for a function declared STRICT.
    #[clap(long)]
    strict: bool,
}

impl From<LimitArgs> for CallOptions {
    fn from(args: LimitArgs) -> Self {
        CallOptions {
            fuel: args.fuel,
            memory: args.memory,
            batch_rows: args.batch_rows,
            strict: args.strict,
        }
    }
}

#[derive(Debug, clap::Subcommand)]
pub enum UdfSubcommand {
    /// Validate a module and list the functions it exports.
    Check {
        /// The WebAssembly module.
        module: PathBuf,
        /// Check that FUNCTION's result for each argument row does not depend
        /// on how rows are batched.
        #[clap(long)]
        determinism: Option<String>,
        /// A headerless CSV file of argument rows for --determinism. Without
        /// it, rows are generated from the argument types.
        #[clap(long, requires = "determinism")]
        input: Option<PathBuf>,
        /// How many rows to generate.
        #[clap(long, requires = "determinism", conflicts_with = "input", default_value_t = CheckInput::DEFAULT_ROWS)]
        rows: usize,
        /// The seed rows are generated from.
        #[clap(
            long,
            requires = "determinism",
            conflicts_with = "input",
            default_value_t = 0
        )]
        seed: u64,
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
        /// The SQL name of the module. Defaults to the module's file name
        /// without its extension.
        #[clap(long)]
        module_name: Option<String>,
        /// STRICT, and the FUEL and MEMORY to declare.
        #[clap(flatten)]
        limits: LimitArgs,
    },
    /// Call a function locally with the runtime regions use.
    Call {
        /// The WebAssembly module.
        module: PathBuf,
        /// The guest function, by name or by signature.
        function: String,
        /// Arguments in PostgreSQL text format, as COPY reads them, or NULL.
        /// Put `--` before arguments that start with a hyphen, such as
        /// negative numbers.
        args: Vec<String>,
        /// Call the function on each row of a headerless CSV file instead.
        #[clap(long, conflicts_with = "args")]
        input: Option<PathBuf>,
        #[clap(flatten)]
        limits: LimitArgs,
    },
    /// Create a function in a region from a local module. The module is
    /// created in the region first, or reused if the schema has a module of
    /// that name with the same bytes; a module of that name with other bytes
    /// is an error.
    Create {
        /// The WebAssembly module.
        module: PathBuf,
        /// The guest function, by name or by signature.
        function: String,
        /// The SQL name of the function. Defaults to the guest name.
        #[clap(long)]
        name: Option<String>,
        /// The SQL name of the module. Defaults to the module's file name
        /// without its extension.
        #[clap(long)]
        module_name: Option<String>,
        /// The database in which to create the function.
        #[clap(long)]
        database: Option<String>,
        /// The schema in which to create the function.
        #[clap(long)]
        schema: Option<String>,
        /// STRICT and the FUEL and MEMORY to declare, which the determinism
        /// check also applies.
        #[clap(flatten)]
        limits: LimitArgs,
        /// Create the function without first checking, on generated rows,
        /// that its results do not depend on batching.
        #[clap(long)]
        skip_check: bool,
    },
    /// Download a module from a region, named directly or through a function
    /// that calls into it.
    Pull {
        /// The function's name, or the module's with --module, in SQL
        /// syntax: quote a name with uppercase letters or special characters,
        /// as `create` quotes the names it is given.
        name: String,
        /// NAME names a module rather than a function.
        #[clap(long)]
        module: bool,
        /// The database that contains the function or module.
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
            rows,
            seed,
            limits,
        } => {
            // A STRICT function never sees NULL arguments, so generating them
            // would only test the host.
            let input = match &input {
                Some(path) => CheckInput::Csv(path),
                None => CheckInput::Generated {
                    rows,
                    seed,
                    nulls: !limits.strict,
                },
            };
            mz::command::udf::check(
                &cx,
                CheckArgs {
                    module: &module,
                    determinism: determinism.as_deref().map(|f| (f, input)),
                    options: limits.into(),
                },
            )
        }
        UdfSubcommand::Sql {
            module,
            function,
            name,
            module_name,
            limits,
        } => mz::command::udf::sql(SqlArgs {
            module: &module,
            function: &function,
            name: name.as_deref(),
            module_name: module_name.as_deref(),
            options: limits.into(),
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
            module_name,
            database,
            schema,
            limits,
            skip_check,
        } => {
            let cx = cx.activate_profile()?.activate_region()?;
            mz::command::udf::create(
                &cx,
                CreateArgs {
                    module: &module,
                    function: &function,
                    name: name.as_deref(),
                    module_name: module_name.as_deref(),
                    database: database.as_deref(),
                    schema: schema.as_deref(),
                    options: limits.into(),
                    skip_check,
                },
            )
            .await
        }
        UdfSubcommand::Pull {
            name,
            module,
            database,
            output,
        } => {
            let cx = cx.activate_profile()?.activate_region()?;
            let output = output.unwrap_or_else(|| mz::command::udf::default_pull_path(&name));
            mz::command::udf::pull(
                &cx,
                PullArgs {
                    name: &name,
                    module,
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
