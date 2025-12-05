use clap::{Parser, Subcommand};
use mz_build_info::{BuildInfo, build_info};
use mz_deploy::cli;
use mz_deploy::cli::CliError;
use mz_deploy::client::ConnectionError;
use mz_deploy::client::config::ProfilesConfig;
use mz_deploy::utils::log;
use std::path::PathBuf;
use std::sync::LazyLock;

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

/// Materialize deployment tool
#[derive(Parser, Debug)]
#[command(name = "mz-deploy", version = VERSION.as_str())]
#[command(about = "Safe, testable deployments for Materialize")]
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
    /// against a local Materialize Docker container. This is useful for local development
    /// and CI/CD pipelines to catch errors before deployment.
    #[command(visible_alias = "build")]
    Compile {
        /// Skip SQL type checking (faster but less thorough validation)
        #[arg(long)]
        skip_typecheck: bool,

        /// Materialize Docker image to use for type checking
        #[arg(long, value_name = "IMAGE")]
        docker_image: Option<String>,
    },

    /// Create tables that don't exist in the database
    ///
    /// Queries the database first and only creates tables that don't already exist.
    /// Tracks the deployment under a deploy ID (default: random 7-char hex).
    /// Only tables that are actually created are recorded in deployment metadata.
    ///
    /// Example:
    ///   mz-deploy create-tables                 # Use random deploy ID
    ///   mz-deploy create-tables --name abc123   # Use custom deploy ID
    CreateTables {
        /// Deploy ID for this table deployment (default: random 7-char hex)
        ///
        /// The deploy ID will be used to track this table deployment separately.
        /// Must contain only alphanumeric characters, hyphens, and underscores.
        #[arg(long, value_name = "DEPLOY_ID")]
        name: Option<String>,

        /// Allow deployment with uncommitted git changes
        #[arg(long)]
        allow_dirty: bool,
    },

    /// Promote a staging deployment to production
    ///
    /// Performs an atomic schema swap between staging and production. Before promoting,
    /// verifies that all staging clusters are fully hydrated and caught up (unless
    /// --skip-ready is specified). This ensures zero-downtime deployments.
    ///
    /// The promotion will fail if:
    /// - Any cluster is still hydrating (objects not yet materialized)
    /// - Any cluster has lag exceeding --allowed-lag threshold
    /// - Any cluster has no replicas or all replicas are OOM-looping
    /// - Production schemas were modified after staging was created (unless --force)
    ///
    /// Example:
    ///   mz-deploy apply abc123                    # Promote staging deployment
    ///   mz-deploy apply abc123 --skip-ready       # Skip hydration check
    ///   mz-deploy apply abc123 --allowed-lag 600  # Allow up to 10 min lag
    Apply {
        /// Staging deployment ID to promote to production
        ///
        /// The deployment ID was assigned when running 'mz-deploy stage'. You can
        /// find active deployments with 'mz-deploy deployments'.
        #[arg(value_name = "DEPLOY_ID")]
        deploy_id: String,

        /// Skip conflict detection when promoting
        ///
        /// Normally, apply checks if production schemas were modified after the
        /// staging deployment was created. This flag bypasses that safety check,
        /// which may overwrite recent production changes.
        #[arg(long)]
        force: bool,

        /// Skip the readiness check before promoting
        ///
        /// By default, apply verifies all staging clusters are hydrated and caught
        /// up before promoting. Use this flag to skip that check and promote
        /// immediately, which may result in stale data being served briefly.
        #[arg(long)]
        skip_ready: bool,

        /// Maximum lag threshold in seconds for readiness check
        ///
        /// During the readiness check, clusters with wallclock lag exceeding this
        /// threshold are marked as "lagging" and will block promotion. Lag measures
        /// how far behind real-time the materialized data is. Default: 300 (5 min).
        #[arg(long, value_name = "SECONDS", default_value = "300")]
        allowed_lag: i64,
    },

    /// Create a staging deployment for testing changes
    ///
    /// Deploys schemas and objects to staging with suffixed names (e.g., 'public_abc123').
    /// This allows testing changes in isolation before promoting to production.
    /// Staging deployments can be listed with 'deployments' and promoted with 'apply'.
    ///
    /// Example:
    ///   mz-deploy stage                    # Use random deploy ID
    ///   mz-deploy stage --name abc123      # Use custom deploy ID
    Stage {
        /// Deploy ID for this staging deployment (default: random 7-char hex)
        ///
        /// The deploy ID will be used as a suffix for schemas and clusters.
        /// Must contain only alphanumeric characters, hyphens, and underscores.
        #[arg(long, value_name = "DEPLOY_ID")]
        name: Option<String>,

        /// Allow staging with uncommitted git changes
        #[arg(long)]
        allow_dirty: bool,

        /// Skip automatic rollback on failure (leaves resources for debugging)
        #[arg(long)]
        no_rollback: bool,
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
    /// the specified deploy ID. This is the equivalent of 'git branch -D' for
    /// staging deployments.
    ///
    /// Example:
    ///   mz-deploy abort abc123
    Abort {
        /// Staging deploy ID to remove
        #[arg(value_name = "DEPLOY_ID")]
        deploy_id: String,
    },

    /// List all active staging deployments
    ///
    /// Shows staging environments that have been deployed but not yet promoted,
    /// similar to 'git branch'. For each deployment, displays:
    /// - Deployment ID and who created it
    /// - Git commit (if available)
    /// - Cluster readiness status (ready, hydrating, lagging, or failing)
    /// - Schemas included in the deployment
    ///
    /// Example:
    ///   mz-deploy deployments                  # List with default lag threshold
    ///   mz-deploy deployments --allowed-lag 60 # Stricter lag threshold
    #[command(visible_alias = "branches")]
    Deployments {
        /// Maximum lag threshold in seconds for cluster status
        ///
        /// Clusters with wallclock lag exceeding this threshold are shown as
        /// "lagging" instead of "ready". Lag measures how far behind real-time
        /// the materialized data is. Default: 300 (5 min).
        #[arg(long, value_name = "SECONDS", default_value = "300")]
        allowed_lag: i64,
    },

    /// Show history of promoted deployments
    ///
    /// Displays a chronological log of deployments that have been promoted to
    /// production, similar to 'git log'. Each entry shows the deploy ID,
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
    /// Monitors cluster hydration status with a live dashboard showing progress for
    /// each cluster. A cluster is considered "ready" when:
    /// - All objects are fully hydrated (materialized)
    /// - Wallclock lag is within the --allowed-lag threshold
    /// - At least one healthy replica exists (not OOM-looping)
    ///
    /// Status indicators:
    /// - ready: Fully hydrated and caught up
    /// - hydrating: Objects still being materialized
    /// - lagging: Hydrated but lag exceeds threshold
    /// - failing: No replicas or all replicas OOM-looping
    ///
    /// Examples:
    ///   mz-deploy ready abc123                    # Wait with live tracking
    ///   mz-deploy ready abc123 --snapshot         # Check once and exit
    ///   mz-deploy ready abc123 --timeout 300      # Wait up to 5 minutes
    ///   mz-deploy ready abc123 --allowed-lag 60   # Require lag under 1 min
    Ready {
        /// Staging deployment ID to monitor
        #[arg(value_name = "DEPLOY_ID")]
        name: String,

        /// Check status once and exit instead of continuous monitoring
        ///
        /// Takes a point-in-time snapshot of cluster status and exits immediately.
        /// Returns success (exit 0) only if all clusters are ready.
        #[arg(long)]
        snapshot: bool,

        /// Maximum time to wait in seconds before timing out
        ///
        /// If clusters don't become ready within this duration, the command exits
        /// with an error. By default, waits indefinitely.
        #[arg(long, value_name = "SECONDS")]
        timeout: Option<u64>,

        /// Maximum lag threshold in seconds for "ready" status
        ///
        /// Clusters with wallclock lag exceeding this threshold are marked as
        /// "lagging" and not considered ready. Lag measures how far behind
        /// real-time the materialized data is. Default: 300 (5 min).
        #[arg(long, value_name = "SECONDS", default_value = "300")]
        allowed_lag: i64,
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
        Some(Command::CreateTables { name, allow_dirty }) => {
            let profile =
                ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                    .map_err(|e| CliError::Connection(ConnectionError::Config(e)))?;

            cli::commands::create_tables::run(
                &profile,
                &args.directory,
                name.as_deref(),
                allow_dirty,
            )
            .await?;
            cli::commands::gen_data_contracts::run(&profile, &args.directory).await
        }
        Some(Command::Apply {
            deploy_id,
            force,
            skip_ready,
            allowed_lag,
        }) => {
            let profile =
                ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                    .map_err(|e| CliError::Connection(ConnectionError::Config(e)))?;
            if !skip_ready {
                cli::commands::ready::run(&profile, &deploy_id, true, None, allowed_lag).await?;
            }
            cli::commands::apply::run(&profile, &deploy_id, force).await
        }
        Some(Command::Stage {
            name,
            allow_dirty,
            no_rollback,
        }) => {
            let profile =
                ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                    .map_err(|e| CliError::Connection(ConnectionError::Config(e)))?;

            cli::commands::stage::run(
                &profile,
                name.as_deref(),
                &args.directory,
                allow_dirty,
                no_rollback,
            )
            .await
        }
        Some(Command::Debug) => {
            let profile =
                ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                    .map_err(|e| CliError::Connection(ConnectionError::Config(e)))?;

            cli::commands::debug::run(&profile).await
        }
        Some(Command::GenDataContracts) => {
            let profile =
                ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                    .map_err(|e| CliError::Connection(ConnectionError::Config(e)))?;
            cli::commands::gen_data_contracts::run(&profile, &args.directory).await
        }
        Some(Command::Test) => cli::commands::test::run(&args.directory).await,
        Some(Command::Abort { deploy_id }) => {
            let profile =
                ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                    .map_err(|e| CliError::Connection(ConnectionError::Config(e)))?;
            cli::commands::abort::run(&profile, &deploy_id).await
        }
        Some(Command::Deployments { allowed_lag }) => {
            let profile =
                ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                    .map_err(|e| CliError::Connection(ConnectionError::Config(e)))?;
            cli::commands::deployments::run(&profile, allowed_lag).await
        }
        Some(Command::History { limit }) => {
            let profile =
                ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                    .map_err(|e| CliError::Connection(ConnectionError::Config(e)))?;
            cli::commands::history::run(&profile, limit).await
        }
        Some(Command::Ready {
            name,
            snapshot,
            timeout,
            allowed_lag,
        }) => {
            let profile =
                ProfilesConfig::load_profile(Some(&args.directory), args.profile.as_deref())
                    .map_err(|e| CliError::Connection(ConnectionError::Config(e)))?;

            cli::commands::ready::run(&profile, &name, snapshot, timeout, allowed_lag).await
        }
        None => {
            // No command provided, do nothing
            Ok(())
        }
    }
}
