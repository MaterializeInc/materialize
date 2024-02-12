// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Manages a single Materialize environment.
//!
//! It listens for SQL connections on port 6875 (MTRL) and for HTTP connections
//! on port 6876.

use mz_balancerd::BUILD_INFO;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use tracing::{info_span, Instrument};

pub mod service;

#[derive(Debug, clap::Parser)]
#[clap(about = "Balancer service", long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,

    #[clap(flatten)]
    tracing: TracingCliArgs,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
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

    let (_, _tracing_guard) = runtime
        .block_on(args.tracing.configure_tracing(
            StaticTracingConfig {
                service_name: "balancerd",
                build_info: BUILD_INFO,
            },
            MetricsRegistry::new(),
        ))
        .expect("failed to init tracing");

    let root_span = info_span!("balancer");
    let res = match args.command {
        Command::Service(args) => runtime.block_on(crate::service::run(args).instrument(root_span)),
    };

    if let Err(err) = res {
        eprintln!("balancer: fatal: {}", err.display_with_causes());
        std::process::exit(1);
    }
    drop(_tracing_guard);
}
