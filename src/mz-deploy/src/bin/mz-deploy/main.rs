//! Binary entry point for `mz-deploy`.
//!
//! Parses CLI arguments via `clap`, loads project and profile configuration,
//! and dispatches to the appropriate command in [`mz_deploy::cli`].

use clap::CommandFactory;
use clap::{Parser, Subcommand};
use mz_build_info::{BuildInfo, build_info};
use mz_deploy::cli;
use mz_deploy::cli::commands::delete;
use mz_deploy::cli::{CliError, TypeCheckMode};
use mz_deploy::client::ConnectionError;
use mz_deploy::config::{Profile, ProfilesConfig, ProjectSettings};
use mz_deploy::log;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

/// Materialize deployment tool
#[derive(Parser, Debug)]
#[command(name = "mz-deploy", version = VERSION.as_str())]
#[command(about = "Safe, testable deployments for Materialize")]
#[command(disable_help_subcommand = true)]
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

    /// Materialize Docker image to use for type checking and tests
    #[arg(long, value_name = "IMAGE", global = true)]
    docker_image: Option<String>,

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
    #[command(
        visible_alias = "build",
        after_help = "Run 'mz-deploy help compile' for a detailed usage guide."
    )]
    Compile {
        /// Skip SQL type checking (faster but less thorough validation)
        #[arg(long)]
        skip_typecheck: bool,
    },

    /// Apply infrastructure objects to Materialize (Terraform-like)
    ///
    /// Declarative, diff-based, idempotent management of infrastructure objects.
    /// Without a subcommand, applies all types in dependency order:
    /// clusters → roles → network-policies → secrets → connections → sources → tables.
    ///
    /// Subcommands:
    ///   clusters          Apply cluster definitions from clusters/ directory
    ///   roles             Apply role definitions from roles/ directory
    ///   network-policies  Apply network policy definitions from network_policies/ directory
    ///   secrets           Apply secret definitions from the project
    ///   connections  Apply connection definitions from the project
    ///   sources      Apply source definitions from the project
    ///   tables       Apply table definitions from the project
    ///
    /// Examples:
    ///   mz-deploy apply                           # Apply all infrastructure
    ///   mz-deploy apply --skip-secrets            # Skip secrets
    ///   mz-deploy apply --dry-run                 # Preview SQL
    ///   mz-deploy apply clusters                  # Apply cluster definitions only
    #[command(after_help = "Run 'mz-deploy help apply' for a detailed usage guide.")]
    Apply {
        /// Skip applying secrets (for users without secret access)
        #[arg(long)]
        skip_secrets: bool,

        /// Print SQL statements without executing them
        ///
        /// Runs the full compilation and validation pipeline but prints the SQL
        /// that would be executed instead of actually running it. Useful for
        /// reviewing changes before deployment.
        #[arg(long)]
        dry_run: bool,

        #[command(subcommand)]
        subcommand: Option<ApplyCommand>,
    },

    /// Promote a staging deployment to production
    ///
    /// Atomically swaps a staging deployment into production using ALTER SWAP.
    /// Before promoting, verifies that all staging clusters are fully hydrated
    /// and caught up (unless --skip-ready is specified).
    ///
    /// Examples:
    ///   mz-deploy deploy abc123                    # Promote staging deployment
    ///   mz-deploy deploy abc123 --skip-ready       # Skip hydration check
    ///   mz-deploy deploy abc123 --allowed-lag 600  # Allow up to 10 min lag
    #[command(
        visible_alias = "promote",
        after_help = "Run 'mz-deploy help deploy' for a detailed usage guide."
    )]
    Deploy {
        /// Staging deployment ID to promote to production
        ///
        /// The deployment ID was assigned when running 'mz-deploy stage'. You can
        /// find active deployments with 'mz-deploy deployments'.
        #[arg(value_name = "DEPLOY_ID")]
        deploy_id: String,

        /// Skip conflict detection when promoting
        ///
        /// Normally, deploy checks if production schemas were modified after the
        /// staging deployment was created. This flag bypasses that safety check,
        /// which may overwrite recent production changes.
        #[arg(long)]
        force: bool,

        /// Skip the readiness check before promoting
        ///
        /// By default, deploy verifies all staging clusters are hydrated and caught
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

        /// Preview what would happen without executing any changes
        #[arg(long)]
        dry_run: bool,
    },

    /// Create a staging deployment for testing changes
    ///
    /// Deploys schemas and objects to staging with suffixed names (e.g., 'public_abc123').
    /// This allows testing changes in isolation before promoting to production.
    /// Staging deployments can be listed with 'deployments' and promoted with 'deploy'.
    ///
    /// Example:
    ///   mz-deploy stage                    # Use random deploy ID
    ///   mz-deploy stage --name abc123      # Use custom deploy ID
    #[command(after_help = "Run 'mz-deploy help stage' for a detailed usage guide.")]
    Stage {
        /// Deploy ID for this staging deployment (default: random 7-char hex)
        ///
        /// The deploy ID will be used as a suffix for schemas and clusters.
        /// Must contain only alphanumeric characters, hyphens, and underscores.
        #[arg(long, value_name = "DEPLOY_ID")]
        deploy_id: Option<String>,

        /// Allow staging with uncommitted git changes
        #[arg(long)]
        allow_dirty: bool,

        /// Skip automatic rollback on failure (leaves resources for debugging)
        #[arg(long)]
        no_rollback: bool,

        /// Print SQL statements without executing them
        ///
        /// Runs the full compilation and validation pipeline but prints the SQL
        /// that would be executed instead of actually running it. Useful for
        /// reviewing changes before deployment.
        #[arg(long)]
        dry_run: bool,
    },

    /// Test database connection and display environment information
    ///
    /// Connects to Materialize using the specified profile and displays version,
    /// environment ID, and current role. Useful for verifying connectivity and
    /// configuration before running deployments.
    #[command(after_help = "Run 'mz-deploy help debug' for a detailed usage guide.")]
    Debug,

    /// Show detailed information about a specific deployment
    ///
    /// Displays comprehensive information about a deployment including metadata
    /// (who deployed, when, git commit) and all objects with their hashes.
    /// Use `mz-deploy history` to find deployment IDs.
    ///
    /// Example:
    ///   mz-deploy describe abc123
    #[command(
        visible_alias = "show",
        after_help = "Run 'mz-deploy help describe' for a detailed usage guide."
    )]
    Describe {
        /// Deployment ID to describe
        #[arg(value_name = "DEPLOY_ID")]
        deploy_id: String,
    },

    /// Generate types.lock file with external dependency schemas
    ///
    /// Queries the database for schema information about external dependencies
    /// (tables/views not managed by this project but referenced in SQL). This
    /// creates a types.lock file used for offline type checking.
    #[command(
        name = "gen-data-contracts",
        after_help = "Run 'mz-deploy help gen-data-contracts' for a detailed usage guide."
    )]
    GenDataContracts,

    /// Run SQL unit tests defined in test files
    ///
    /// Executes all test files in the project against a temporary Materialize
    /// Docker container. Tests validate SQL logic without affecting production.
    #[command(after_help = "Run 'mz-deploy help test' for a detailed usage guide.")]
    Test,

    /// Clean up a staging deployment by dropping all resources
    ///
    /// Removes staging schemas, clusters, and deployment tracking records for
    /// the specified deploy ID. This is the equivalent of 'git branch -D' for
    /// staging deployments.
    ///
    /// Example:
    ///   mz-deploy abort abc123
    #[command(after_help = "Run 'mz-deploy help abort' for a detailed usage guide.")]
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
    #[command(
        visible_alias = "branches",
        after_help = "Run 'mz-deploy help deployments' for a detailed usage guide."
    )]
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
    #[command(
        visible_alias = "log",
        after_help = "Run 'mz-deploy help history' for a detailed usage guide."
    )]
    History {
        /// Maximum number of deployments to show (default: unlimited)
        #[arg(short, long, value_name = "N")]
        limit: Option<usize>,
    },

    /// Create a new mz-deploy project
    ///
    /// Scaffolds a project directory with the required structure including
    /// project.toml, models/, clusters/, and roles/ directories.
    /// Initializes a git repository unless --no-git is specified.
    ///
    /// Example:
    ///   mz-deploy new my-project
    #[command(after_help = "Run 'mz-deploy help new' for a detailed usage guide.")]
    New {
        /// Name of the project directory to create
        #[arg(value_name = "NAME")]
        name: String,

        /// Skip git repository initialization
        #[arg(long)]
        no_git: bool,
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
    #[command(after_help = "Run 'mz-deploy help ready' for a detailed usage guide.")]
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

    /// Delete an object from Materialize and remove its project file
    ///
    /// Drops the specified object without CASCADE. If the object has
    /// dependents, the drop will fail with an error listing them.
    /// Requires confirmation unless --yes is passed.
    ///
    /// Examples:
    ///   mz-deploy delete cluster analytics
    ///   mz-deploy delete connection mydb.public.pg_conn
    ///   mz-deploy delete table mydb.public.users --yes
    #[command(after_help = "Run 'mz-deploy help delete' for a detailed usage guide.")]
    Delete {
        /// Skip confirmation prompt
        #[arg(short = 'y', long)]
        yes: bool,

        #[command(subcommand)]
        subcommand: DeleteCommand,
    },

    /// Show detailed usage guide for a command. Useful for LLM coding agents
    ///
    /// Prints extended documentation including behavior notes, examples,
    /// error recovery guidance, and related commands. Use --all to print
    /// the complete reference for all commands.
    Help {
        /// Command name to show help for
        #[arg(value_name = "COMMAND")]
        command: Option<String>,

        /// Print all command guides (for LLM ingestion)
        #[arg(long)]
        all: bool,
    },
}

#[derive(Subcommand, Debug)]
enum ApplyCommand {
    /// Apply cluster definitions from clusters/ directory
    ///
    /// Converges the live Materialize state to match the cluster definitions.
    /// Creates clusters that don't exist and alters ones whose configuration
    /// has drifted. Grants and comments are applied idempotently.
    ///
    /// Example:
    ///   mz-deploy apply clusters
    #[command(after_help = "Run 'mz-deploy help apply-clusters' for a detailed usage guide.")]
    Clusters,
    /// Apply role definitions from roles/ directory
    ///
    /// Converges the live Materialize state to match the role definitions.
    /// Creates roles that don't exist, then applies ALTER ROLE, GRANT ROLE,
    /// and COMMENT statements idempotently.
    ///
    /// Example:
    ///   mz-deploy apply roles
    #[command(after_help = "Run 'mz-deploy help apply-roles' for a detailed usage guide.")]
    Roles,
    /// Apply network policy definitions from network_policies/ directory
    ///
    /// Converges the live Materialize state to match the network policy definitions.
    /// Creates policies that don't exist and alters ones whose rules have changed.
    /// Grants and comments are applied idempotently.
    ///
    /// Example:
    ///   mz-deploy apply network-policies
    #[command(
        after_help = "Run 'mz-deploy help apply-network-policies' for a detailed usage guide."
    )]
    NetworkPolicies,
    /// Apply secret definitions from the project
    ///
    /// Creates missing secrets and updates existing ones to match the project
    /// definitions. For each secret, executes CREATE SECRET IF NOT EXISTS
    /// followed by ALTER SECRET to ensure the value is current.
    ///
    /// Example:
    ///   mz-deploy apply secrets
    #[command(after_help = "Run 'mz-deploy help apply-secrets' for a detailed usage guide.")]
    Secrets,
    /// Apply connection definitions from the project
    ///
    /// Creates missing connections and reconciles existing ones whose
    /// configuration has drifted. Grants and comments are applied idempotently.
    ///
    /// Example:
    ///   mz-deploy apply connections
    #[command(after_help = "Run 'mz-deploy help apply-connections' for a detailed usage guide.")]
    Connections,
    /// Apply source definitions from the project
    ///
    /// Creates sources that don't exist in the database. Existing sources
    /// are skipped (idempotent).
    ///
    /// Example:
    ///   mz-deploy apply sources
    #[command(after_help = "Run 'mz-deploy help apply-sources' for a detailed usage guide.")]
    Sources,
    /// Apply table definitions from the project
    ///
    /// Creates tables that don't exist in the database. Existing tables
    /// are skipped (idempotent).
    ///
    /// Example:
    ///   mz-deploy apply tables
    #[command(after_help = "Run 'mz-deploy help apply-tables' for a detailed usage guide.")]
    Tables,
}

#[derive(Subcommand, Debug)]
enum DeleteCommand {
    /// Delete a cluster
    Cluster {
        /// Cluster name
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Delete a connection
    Connection {
        /// Fully-qualified connection name (database.schema.name)
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Delete a network policy
    NetworkPolicy {
        /// Network policy name
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Delete a role
    Role {
        /// Role name
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Delete a secret
    Secret {
        /// Fully-qualified secret name (database.schema.name)
        #[arg(value_name = "NAME")]
        name: String,
    },
    /// Delete a table
    Table {
        /// Fully-qualified table name (database.schema.name)
        #[arg(value_name = "NAME")]
        name: String,
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
    // Handle commands that don't require an existing project
    if let Some(Command::Help { command, all }) = &args.command {
        if *all {
            print!("{}", cli::extended_help::all_help());
        } else if let Some(cmd) = command {
            match cli::extended_help::help_for(cmd) {
                Some(text) => print!("{text}"),
                None => {
                    cli::extended_help::print_unknown_command(cmd);
                    std::process::exit(1);
                }
            }
        } else {
            Args::command().print_help().unwrap();
        }
        return Ok(());
    }

    if args.command.is_none() {
        Args::command().print_help().unwrap();
        return Ok(());
    }

    if let Some(Command::New { name, no_git }) = &args.command {
        return cli::commands::new_project::run(name, !no_git);
    }

    let settings = load_project_settings(&args.directory, args.docker_image)?;

    match args.command {
        Some(Command::Compile { skip_typecheck }) => {
            let typecheck = if skip_typecheck {
                TypeCheckMode::Disabled
            } else {
                TypeCheckMode::Enabled {
                    image: settings.docker_image(),
                }
            };
            cli::commands::compile::run(&args.directory, typecheck)
                .await
                .map(|_| ())
        }
        Some(Command::Apply {
            skip_secrets,
            dry_run,
            subcommand,
        }) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;

            match subcommand {
                Some(ApplyCommand::Clusters) => {
                    cli::commands::clusters::run(&args.directory, &profile, dry_run).await
                }
                Some(ApplyCommand::Roles) => {
                    cli::commands::roles::run(&args.directory, &profile, dry_run).await
                }
                Some(ApplyCommand::NetworkPolicies) => {
                    cli::commands::apply_network_policies::run(&args.directory, &profile, dry_run)
                        .await
                }
                Some(ApplyCommand::Secrets) => {
                    cli::commands::apply_secrets::run(&args.directory, &profile, &settings, dry_run)
                        .await
                }
                Some(ApplyCommand::Connections) => {
                    cli::commands::apply_connections::run(
                        &args.directory,
                        &profile,
                        &settings,
                        dry_run,
                    )
                    .await
                }
                Some(ApplyCommand::Sources) => {
                    cli::commands::apply_tables::apply_sources(
                        &args.directory,
                        &profile,
                        dry_run,
                    )
                    .await
                }
                Some(ApplyCommand::Tables) => {
                    cli::commands::apply_tables::apply_tables(
                        &args.directory,
                        &profile,
                        dry_run,
                    )
                    .await?;
                    if !dry_run {
                        cli::commands::gen_data_contracts::run(&profile, &args.directory).await?;
                    }
                    Ok(())
                }
                None => {
                    cli::commands::apply_all::run(
                        &args.directory,
                        &profile,
                        &settings,
                        skip_secrets,
                        dry_run,
                    )
                    .await
                }
            }
        }
        Some(Command::Deploy {
            deploy_id,
            force,
            skip_ready,
            allowed_lag,
            dry_run,
        }) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;

            if !skip_ready && !dry_run {
                cli::commands::ready::run(&profile, &deploy_id, true, None, allowed_lag).await?;
            }
            cli::commands::deploy::run(&profile, &deploy_id, force, dry_run).await
        }
        Some(Command::Stage {
            deploy_id,
            allow_dirty,
            no_rollback,
            dry_run,
        }) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;

            cli::commands::stage::run(
                &profile,
                deploy_id.as_deref(),
                &args.directory,
                allow_dirty,
                no_rollback,
                dry_run,
            )
            .await
        }
        Some(Command::Debug) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;

            cli::commands::debug::run(&profile).await
        }
        Some(Command::Describe { deploy_id }) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;

            cli::commands::describe::run(&profile, &deploy_id).await
        }
        Some(Command::GenDataContracts) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;
            cli::commands::gen_data_contracts::run(&profile, &args.directory).await
        }
        Some(Command::Test) => {
            cli::commands::test::run(&args.directory, &settings.docker_image()).await
        }
        Some(Command::Abort { deploy_id }) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;
            cli::commands::abort::run(&profile, &deploy_id).await
        }
        Some(Command::Deployments { allowed_lag }) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;
            cli::commands::deployments::run(&profile, allowed_lag).await
        }
        Some(Command::History { limit }) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;
            cli::commands::history::run(&profile, limit).await
        }
        Some(Command::Ready {
            name,
            snapshot,
            timeout,
            allowed_lag,
        }) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;

            cli::commands::ready::run(&profile, &name, snapshot, timeout, allowed_lag).await
        }
        Some(Command::Delete { yes, subcommand }) => {
            let profile = load_profile(&args.directory, args.profile.as_deref(), &settings)?;
            let (kind, name) = match subcommand {
                DeleteCommand::Cluster { name } => (delete::ObjectKind::Cluster, name),
                DeleteCommand::Connection { name } => (delete::ObjectKind::Connection, name),
                DeleteCommand::NetworkPolicy { name } => (delete::ObjectKind::NetworkPolicy, name),
                DeleteCommand::Role { name } => (delete::ObjectKind::Role, name),
                DeleteCommand::Secret { name } => (delete::ObjectKind::Secret, name),
                DeleteCommand::Table { name } => (delete::ObjectKind::Table, name),
            };
            delete::run(&args.directory, &profile, kind, &name, yes).await
        }
        Some(Command::Help { .. }) => unreachable!("handled above"),
        Some(Command::New { .. }) => unreachable!("handled above"),
        None => unreachable!("handled above"),
    }
}

fn load_project_settings(
    directory: &Path,
    docker_image: Option<String>,
) -> Result<ProjectSettings, CliError> {
    ProjectSettings::load(directory)
        .map(|s| s.with_docker_image_override(docker_image))
        .map_err(CliError::Config)
}

fn load_profile(
    directory: &Path,
    cli_profile: Option<&str>,
    settings: &ProjectSettings,
) -> Result<Profile, CliError> {
    ProfilesConfig::load_profile(Some(directory), cli_profile, &settings.profile)
        .map_err(|e| CliError::Connection(ConnectionError::Config(e)))
}
