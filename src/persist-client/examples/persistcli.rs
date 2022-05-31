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

use std::sync::Once;

use tracing_subscriber::{EnvFilter, FmtSubscriber};

use mz_ore::cli::{self, CliConfig};

pub mod maelstrom;
pub mod open_loop;
pub mod source_example;

#[derive(Debug, clap::Parser)]
#[clap(about = "Persist command-line utilities", long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    Maelstrom(crate::maelstrom::Args),
    OpenLoop(crate::open_loop::Args),
    SourceExample(crate::source_example::Args),
}

fn main() {
    // This doesn't use [mz_ore::test::init_logging] because Maelstrom requires
    // that all logging goes to stderr.
    init_logging();

    let args: Args = cli::parse_args(CliConfig::default());

    // Mirror the tokio Runtime configuration in our production binaries.
    let ncpus_useful = usize::max(1, std::cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(ncpus_useful)
        .enable_all()
        .build()
        .expect("Failed building the Runtime");

    let res = match args.command {
        Command::Maelstrom(args) => crate::maelstrom::txn::run(args),
        Command::OpenLoop(args) => runtime.block_on(crate::open_loop::run(args)),
        Command::SourceExample(args) => runtime.block_on(crate::source_example::run(args)),
    };
    if let Err(err) = res {
        eprintln!("error: {:#}", err);
        std::process::exit(1);
    }
}

static LOG_INIT: Once = Once::new();

fn init_logging() {
    LOG_INIT.call_once(|| {
        let default_level = "info";
        let filter = EnvFilter::try_from_env("MZ_LOG_FILTER")
            .or_else(|_| EnvFilter::try_new(default_level))
            .unwrap();
        FmtSubscriber::builder()
            .with_env_filter(filter)
            .with_writer(std::io::stderr)
            .init();
    });
}
