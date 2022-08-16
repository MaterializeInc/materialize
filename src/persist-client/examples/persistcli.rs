// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_debug_implementations)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

//! Persist command-line utilities

use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::cli::{self, CliConfig};
use mz_ore::task::RuntimeExt;
use mz_ore::tracing::TracingConfig;
use tokio::runtime::Handle;
use tracing::{info_span, Instrument};

pub mod inspect;
pub mod maelstrom;
pub mod open_loop;
pub mod source_example;

#[derive(Debug, clap::Parser)]
#[clap(about = "Persist command-line utilities", long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,

    #[clap(flatten)]
    tracing: TracingCliArgs,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    Maelstrom(crate::maelstrom::Args),
    OpenLoop(crate::open_loop::Args),
    SourceExample(crate::source_example::Args),
    Inspect(crate::inspect::InspectArgs),
}

fn main() {
    let args: Args = cli::parse_args(CliConfig::default());

    // Mirror the tokio Runtime configuration in our production binaries.
    let ncpus_useful = usize::max(1, std::cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ncpus_useful)
        .enable_all()
        .build()
        .expect("Failed building the Runtime");

    let _ = runtime
        .block_on(mz_ore::tracing::configure(
            "persist-open-loop",
            TracingConfig::from(&args.tracing),
        ))
        .expect("failed to init tracing");

    let root_span = info_span!("persistcli");
    let res = match args.command {
        Command::Maelstrom(args) => runtime.block_on(async move {
            // Run the maelstrom stuff in a spawn_blocking because it internally
            // spawns tasks, so the runtime needs to be in the TLC.
            Handle::current()
                .spawn_blocking_named(
                    || "maelstrom::run",
                    move || root_span.in_scope(|| crate::maelstrom::txn::run(args)),
                )
                .await
                .expect("task failed")
        }),
        Command::OpenLoop(args) => {
            runtime.block_on(crate::open_loop::run(args).instrument(root_span))
        }
        Command::SourceExample(args) => {
            runtime.block_on(crate::source_example::run(args).instrument(root_span))
        }
        Command::Inspect(command) => {
            runtime.block_on(crate::inspect::run(command).instrument(root_span))
        }
    };

    mz_ore::tracing::shutdown();

    if let Err(err) = res {
        eprintln!("error: {:#}", err);
        std::process::exit(1);
    }
}
