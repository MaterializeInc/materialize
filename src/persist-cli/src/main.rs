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

use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;

pub mod maelstrom;
pub mod open_loop;
pub mod service;

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
    MaelstromTxn(crate::maelstrom::Args),
    OpenLoop(crate::open_loop::Args),
    Inspect(mz_persist_client::cli::inspect::InspectArgs),
    Admin(mz_persist_client::cli::admin::AdminArgs),
    Bench(mz_persist_client::cli::bench::BenchArgs),
    Service(crate::service::Args),
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

    runtime
        .block_on(args.tracing.configure_tracing(
            StaticTracingConfig {
                service_name: "persistcli",
                build_info: mz_persist_client::BUILD_INFO,
            },
            MetricsRegistry::new(),
        ))
        .expect("failed to init tracing");

    let res = match args.command {
        Command::Maelstrom(args) => runtime.block_on(crate::maelstrom::run::<
            crate::maelstrom::txn_list_append_single::TransactorService,
        >(args)),
        Command::MaelstromTxn(args) => runtime.block_on(crate::maelstrom::run::<
            crate::maelstrom::txn_list_append_multi::TransactorService,
        >(args)),
        Command::OpenLoop(args) => runtime.block_on(crate::open_loop::run(args)),
        Command::Inspect(command) => {
            runtime.block_on(mz_persist_client::cli::inspect::run(command))
        }
        Command::Admin(command) => runtime.block_on(mz_persist_client::cli::admin::run(command)),
        Command::Bench(command) => runtime.block_on(mz_persist_client::cli::bench::run(command)),
        Command::Service(args) => runtime.block_on(crate::service::run(args)),
    };

    if let Err(err) = res {
        eprintln!("persistcli: fatal: {}", err.display_with_causes());
        std::process::exit(1);
    }
}
