// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
#![warn(missing_debug_implementations)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

//! Persist command-line utilities

use mz_build_info::{build_info, BuildInfo};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::task::RuntimeExt;
use tokio::runtime::Handle;
use tracing::{info_span, Instrument};

pub mod maelstrom;
pub mod open_loop;
pub mod service;

const BUILD_INFO: BuildInfo = build_info!();

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
    Inspect(mz_persist_client::cli::inspect::InspectArgs),
    Admin(mz_persist_client::cli::admin::AdminArgs),
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

    let _ = runtime
        .block_on(args.tracing.configure_tracing(
            StaticTracingConfig {
                service_name: "persist-open-loop",
                build_info: BUILD_INFO,
            },
            MetricsRegistry::new(),
        ))
        .expect("failed to init tracing");

    let root_span = info_span!("persistcli");
    let res = match args.command {
        Command::Maelstrom(args) => runtime.block_on(async move {
            // Persist internally has a bunch of sanity check assertions. If
            // maelstrom tickles one of these, we very much want to bubble this
            // up into a process exit with non-0 status. It's surprisingly
            // tricky to be confident that we're not accidentally swallowing
            // panics in async tasks (in fact there was a bug that did exactly
            // this at one point), so abort on any panics to be extra sure.
            mz_ore::panic::set_abort_on_panic();

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
        Command::Inspect(command) => {
            runtime.block_on(mz_persist_client::cli::inspect::run(command).instrument(root_span))
        }
        Command::Admin(command) => {
            runtime.block_on(mz_persist_client::cli::admin::run(command).instrument(root_span))
        }
        Command::Service(args) => runtime.block_on(crate::service::run(args).instrument(root_span)),
    };

    if let Err(err) = res {
        eprintln!("persistcli: fatal: {}", err.display_with_causes());
        std::process::exit(1);
    }
}
