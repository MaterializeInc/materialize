use clap::{Parser, Subcommand};
use mz_build_info::{build_info, BuildInfo};
use mz_deploy::cli;
use mz_deploy::cli::CliError;
use mz_deploy::client::config::ProfilesConfig;
use mz_deploy::client::ConnectionError;
use mz_deploy::utils::log;
use std::path::PathBuf;
use std::sync::LazyLock;

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

/// Materialize deployment tool for managing database schemas and objects
#[derive(Parser, Debug)]
#[command(name = "mz-deploy", version = VERSION.as_str())]
#[command(about = "Manage Materialize database deployments with blue/green staging workflows")]
#[command(long_about = "mz-deploy helps you manage Materialize database schemas and objects with \
    git-like workflows. Deploy to staging environments for testing, then promote to production \
    with atomic schema swaps for zero-downtime deployments.")]
struct Args {
    /// Path to the project root directory containing database schemas
    #[arg(short, long, default_value = ".", global = true)]
    directory: PathBuf,

    /// Enable verbose output for debugging
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Database connection profile to use (from profiles.toml)
    #[arg(short, long, global = true)]
    profile: Option<String>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Compile and validate SQL without connecting to database
    ///
    /// Parses all SQL files, validates dependencies, and optionally type-checks SQL
    /// against a local Materialize Docker container. This is useful for CI/CD pipelines
    /// to catch errors before deployment.
    #[command(visible_alias = "build")]
    Compile {
        /// Skip SQL type checking (faster but less thorough validation)
        #[arg(long)]
        skip_typecheck: bool,

        /// Materialize Docker image to use for type checking
        #[arg(long, value_name = "IMAGE")]
        docker_image: Option<String>,
    },

    /// Deploy to production directly or promote a staging environment
    ///
    /// This command has two modes:
    ///
    /// 1. Promote staging (recommended): Atomically swap a tested staging environment
    ///    with production for zero-downtime deployment.
    ///
    /// 2. Direct deployment: Apply changes directly to production. Safe for new objects
    ///    and initial deployments. Will error if it would overwrite existing objects.
    ///
    /// Examples:
    ///   # Promote staging environment to production (blue/green swap)
    ///   mz-deploy apply my-staging-env
    ///
    ///   # Deploy directly to production (safe for new objects)
    ///   mz-deploy apply
    Apply {
        /// Staging environment to promote to production (blue/green deployment)
        ///
        /// Specifies which staging environment to swap with production. The staging
        /// schemas will become production and vice versa in an atomic operation.
        #[arg(value_name = "ENV")]
        staging_env: Option<String>,

        /// Allow deployment with uncommitted git changes
        #[arg(long)]
        allow_dirty: bool,

        /// Skip conflict detection when promoting staging environments
        ///
        /// When promoting staging, apply normally checks if production schemas were
        /// modified after staging was created. This flag bypasses that check.
        #[arg(long)]
        force: bool,

        #[arg(long, hide = true)]
        in_place_dangerous_will_cause_downtime: bool,
    },

    /// Create a staging deployment for testing changes
    ///
    /// Deploys schemas and objects to a staging environment with suffixed names
    /// (e.g., 'public_abc123'). This allows testing changes in isolation before
    /// promoting to production. Staging deployments can be listed with 'deployments'
    /// command and promoted with 'apply' command.
    ///
    /// Example:
    ///   mz-deploy stage --name my-feature
    Stage {
        /// Name for this staging environment (default: first 7 chars of git commit SHA)
        ///
        /// The name will be used as a suffix for schemas and clusters. Must contain
        /// only alphanumeric characters, hyphens, and underscores.
        #[arg(long, value_name = "NAME")]
        name: Option<String>,

        /// Allow staging with uncommitted git changes
        #[arg(long)]
        allow_dirty: bool,
    },

    /// Test database connection and display environment information
    ///
    /// Connects to Materialize using the specified profile and displays version,
    /// environment ID, and current role. Useful for verifying connectivity and
    /// configuration before running deployments.
    Debug,

    /// Generate types.lock file with external dependency schemas
    ///
    /// Queries the database for schema information about external dependencies
    /// (tables/views not managed by this project but referenced in SQL). This
    /// creates a types.lock file used for offline type checking.
    #[command(name = "gen-data-contracts")]
    GenDataContracts,

    /// Run SQL unit tests defined in test files
    ///
    /// Executes all test files in the project against a temporary Materialize
    /// Docker container. Tests validate SQL logic without affecting production.
    Test,

    /// Clean up a staging deployment by dropping all resources
    ///
    /// Removes staging schemas, clusters, and deployment tracking records for
    /// the specified environment. This is the equivalent of 'git branch -D' for
    /// staging deployments.
    ///
    /// Example:
    ///   mz-deploy abort my-staging-env
    Abort {
        /// Name of the staging environment to remove
        #[arg(value_name = "ENV")]
        name: String,
    },

    /// List all active staging deployments
    ///
    /// Shows staging environments that have been deployed but not yet promoted,
    /// similar to 'git branch'. Displays environment names, schemas, who deployed
    /// them, and when.
    #[command(visible_alias = "branches")]
    Deployments,

    /// Show history of promoted deployments
    ///
    /// Displays a chronological log of deployments that have been promoted to
    /// production, similar to 'git log'. Each entry shows the environment name,
    /// who promoted it, when, and which schemas were included.
    ///
    /// Example:
    ///   mz-deploy history --limit 10
    #[command(visible_alias = "log")]
    History {
        /// Maximum number of deployments to show (default: unlimited)
        #[arg(short, long, value_name = "N")]
        limit: Option<usize>,
    },

    /// Wait for staging deployment clusters to be hydrated and ready
    ///
    /// Monitors cluster hydration status and displays progress. By default, continuously
    /// tracks hydration using Materialize's SUBSCRIBE feature with live progress bars
    /// for each cluster. Use --snapshot to check current status once without waiting.
    ///
    /// Examples:
    ///   # Wait with live progress tracking
    ///   mz-deploy ready my-staging-env
    ///
    ///   # Check status once
    ///   mz-deploy ready my-staging-env --snapshot
    ///
    ///   # With timeout
    ///   mz-deploy ready my-staging-env --timeout 300
    Ready {
        /// Name of the staging environment to check
        #[arg(value_name = "ENV")]
        name: String,

        /// Check current status once without continuous tracking
        #[arg(long)]
        snapshot: bool,

        /// Maximum time to wait in seconds (default: no timeout)
        #[arg(long, value_name = "SECONDS")]
        timeout: Option<u64>,
    },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    log::set_verbose(args.verbose);
    
    if let Err(e) = run(args).await {
        cli::display_error(&e);
    }
}

async fn run(args: Args) -> Result<(), CliError> {
    match args.command {
        Some(Command::Compile {
                 skip_typecheck,
                 docker_image,
             }) => {
            let compile_args = cli::commands::compile::CompileArgs {
                typecheck: !skip_typecheck,
                docker_image,
            };
            cli::commands::compile::run(&args.directory, compile_args)
                .await
                .map(|_| ())
        }
        Some(Command::Apply {
                 staging_env,
                 allow_dirty,
                 force,
                 in_place_dangerous_will_cause_downtime,
             }) => {
            let profile = ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                .map_err(|e| CliError::Connection(
                    ConnectionError::Config(e)
                ))?;
            match staging_env {
                None => {
                    cli::commands::apply::run(
                        &profile,
                        &args.directory,
                        in_place_dangerous_will_cause_downtime,
                        allow_dirty,
                    )
                        .await
                }

                Some(stage_env) => cli::commands::swap::run(&profile, &stage_env, force).await,
            }
        },
        Some(Command::Stage { name, allow_dirty }) => {
            let profile = ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                .map_err(|e| CliError::Connection(
                    ConnectionError::Config(e)
                ))?;
            
            cli::commands::stage::run(
                &profile,
                name.as_deref(),
                &args.directory,
                allow_dirty,
            )
                .await
        }
        Some(Command::Debug) => {
            let profile = ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                .map_err(|e| CliError::Connection(
                    ConnectionError::Config(e)
                ))?;
            
            cli::commands::debug::run(&profile).await
        },
        Some(Command::GenDataContracts) => {
            let profile = ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                .map_err(|e| CliError::Connection(
                    ConnectionError::Config(e)
                ))?;
            cli::commands::gen_data_contracts::run(&profile, &args.directory).await
        }
        Some(Command::Test) => cli::commands::test::run(&args.directory).await,
        Some(Command::Abort { name }) => {
            let profile = ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                .map_err(|e| CliError::Connection(
                    ConnectionError::Config(e)
                ))?;
            cli::commands::abort::run(&profile, &name).await
        }
        Some(Command::Deployments) => {
            let profile = ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                .map_err(|e| CliError::Connection(
                    ConnectionError::Config(e)
                ))?;
            cli::commands::deployments::run(&profile).await
        }
        Some(Command::History { limit }) => {
            let profile = ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                .map_err(|e| CliError::Connection(
                    ConnectionError::Config(e)
                ))?;
            cli::commands::history::run(&profile, limit).await
        }
        Some(Command::Ready { name, snapshot, timeout }) => {
            let profile = ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                .map_err(|e| CliError::Connection(
                    ConnectionError::Config(e)
                ))?;
            
            cli::commands::ready::run(&profile, &name, snapshot, timeout).await
        }
        None => {
            // No command provided, do nothing
            Ok(())
        }
    }
}

