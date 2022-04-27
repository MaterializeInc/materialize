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

pub mod maelstrom;

#[derive(Debug, clap::Parser)]
#[clap(about = "Persist command-line utilities", long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    Maelstrom(crate::maelstrom::Args),
}

#[tokio::main]
async fn main() {
    // This doesn't use [mz_ore::test::init_logging] because Maelstrom requires
    // that all logging goes to stderr.
    init_logging();

    let args: Args = mz_ore::cli::parse_args();

    let res = match args.command {
        Command::Maelstrom(args) => crate::maelstrom::txn::run(args),
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
