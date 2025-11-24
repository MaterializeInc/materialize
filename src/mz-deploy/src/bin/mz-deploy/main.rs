use clap::{Parser, Subcommand};
use mz_build_info::{BuildInfo, build_info};
use mz_deploy::cli;
use mz_deploy::utils::log;
use std::path::PathBuf;
use std::sync::LazyLock;

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

/// Materialize deployment tool
#[derive(Parser, Debug)]
#[command(name = "mz-deploy", version = VERSION.as_str())]
#[command(about = "A tool for managing Materialize database deployments", long_about = None)]
struct Args {
    /// Path to the project root directory
    #[arg(short, long, default_value = ".", global = true)]
    directory: PathBuf,

    #[arg(short, long, global = true)]
    verbose: bool,

    /// Profile name to use (defaults to "default")
    #[arg(short, long, global = true)]
    profile: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Compile project and verify dependencies
    Compile {
        /// Skip database connection and dependency verification
        #[arg(long)]
        offline: bool,
    },
    /// Apply project to database (compile and execute SQL)
    Apply {
        #[arg(long)]
        in_place_dangerous_will_cause_downtime: bool,

        /// Force application despite conflicts (skip rebase check)
        #[arg(long)]
        force: bool,

        /// Optional staging environment to swap with (blue/green deployment)
        staging_env: Option<String>,
    },
    /// Deploy project to staging environment with renamed schemas and clusters
    Stage {
        /// Staging environment name (default: first 5 chars of git SHA if in git repo)
        #[arg(long)]
        name: Option<String>,
    },
    /// Test database connection with a profile
    Debug,
    /// Run unit tests
    Test,
    /// Abort a staged deployment (drop schemas, clusters, and deployment records)
    Abort {
        /// Staging environment name to abort
        name: String,
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    log::set_verbose(args.verbose);

    let result = match args.command {
        Some(Command::Compile { offline }) => {
            cli::commands::compile::run(args.profile.as_deref(), offline, &args.directory)
                .await
                .map(|_| ())
        }
        Some(Command::Apply {
            in_place_dangerous_will_cause_downtime,
            force,
            staging_env,
        }) => {
            cli::commands::apply::run(
                args.profile.as_deref(),
                &args.directory,
                in_place_dangerous_will_cause_downtime,
                force,
                staging_env.as_deref(),
            )
            .await
        }
        Some(Command::Stage { name }) => {
            cli::commands::stage::run(args.profile.as_deref(), name.as_deref(), &args.directory)
                .await
        }
        Some(Command::Debug) => {
            cli::commands::debug::run(args.profile.as_deref(), &args.directory).await
        }
        Some(Command::Test) => {
            cli::commands::test::run(args.profile.as_deref(), &args.directory).await
        }
        Some(Command::Abort { name }) => {
            cli::commands::abort::run(args.profile.as_deref(), &name).await
        }
        None => {
            // No command provided, do nothing
            Ok(())
        }
    };

    // Handle errors with proper display
    if let Err(e) = result {
        cli::display_error(&e);
    }
}
