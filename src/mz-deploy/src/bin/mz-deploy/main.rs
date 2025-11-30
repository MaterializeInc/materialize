use clap::{Parser, Subcommand};
use mz_build_info::{BuildInfo, build_info};
use mz_deploy::cli;
use mz_deploy::client::config::ProfilesConfig;
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
}

/// Determine if a command needs a database connection
fn needs_connection(command: &Option<Command>) -> bool {
    match command {
        Some(Command::Compile { .. }) => false,
        Some(Command::Apply { .. }) => true,
        Some(Command::Stage { .. }) => true,
        Some(Command::Debug) => true,
        Some(Command::GenDataContracts) => true,
        Some(Command::Test) => false, // Test uses Docker, not profile connection
        Some(Command::Abort { .. }) => true,
        Some(Command::Deployments) => true,
        Some(Command::History { .. }) => true,
        None => false,
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    log::set_verbose(args.verbose);

    // Load profile once if command needs database connection
    let profile = if needs_connection(&args.command) {
        match ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref()) {
            Ok(p) => Some(p),
            Err(e) => {
                cli::display_error(&cli::CliError::Connection(
                    mz_deploy::client::ConnectionError::Config(e),
                ));
                return;
            }
        }
    } else {
        None
    };

    let result = match args.command {
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
        }) => match staging_env {
            None => {
                cli::commands::apply::run(
                    profile.as_ref(),
                    &args.directory,
                    in_place_dangerous_will_cause_downtime,
                    allow_dirty,
                )
                .await
            }

            Some(stage_env) => cli::commands::swap::run(profile.as_ref(), &stage_env, force).await,
        },
        Some(Command::Stage { name, allow_dirty }) => {
            cli::commands::stage::run(
                profile.as_ref(),
                name.as_deref(),
                &args.directory,
                allow_dirty,
            )
            .await
        }
        Some(Command::Debug) => cli::commands::debug::run(profile.as_ref(), &args.directory).await,
        Some(Command::GenDataContracts) => {
            cli::commands::gen_data_contracts::run(profile.as_ref(), &args.directory).await
        }
        Some(Command::Test) => cli::commands::test::run(&args.directory).await,
        Some(Command::Abort { name }) => {
            cli::commands::abort::run(profile.as_ref(), &args.directory, &name).await
        }
        Some(Command::Deployments) => {
            cli::commands::deployments::run(profile.as_ref(), &args.directory).await
        }
        Some(Command::History { limit }) => {
            cli::commands::history::run(profile.as_ref(), &args.directory, limit).await
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
