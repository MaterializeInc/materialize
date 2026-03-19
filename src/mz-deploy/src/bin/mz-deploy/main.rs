//! Binary entry point for `mz-deploy`.
//!
//! Parses CLI arguments via `clap`, loads project and profile configuration,
//! and dispatches to the appropriate command in [`mz_deploy::cli`].

use clap::CommandFactory;
use clap::{Parser, Subcommand};
use mz_build_info::{BuildInfo, build_info};
use mz_deploy::cli;
use mz_deploy::cli::CliError;
use mz_deploy::cli::commands::delete;
use mz_deploy::cli::commands::new_project::ScaffoldOpts;
use mz_deploy::config::Settings;
use mz_deploy::log;
use std::path::PathBuf;
use std::sync::LazyLock;

/// Output format for command results.
#[derive(Debug, Clone, Copy, Default, clap::ValueEnum)]
enum OutputFormat {
    #[default]
    Text,
    Json,
}

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

// Clap 4's derive API doesn't support subcommand grouping natively.
// We work around this by hiding all subcommands from the auto-generated flat
// list (`#[command(hide = true)]` on each variant) and rendering our own
// grouped listing via `after_help`. The custom `help_template` on `Args`
// replaces the default `{subcommands}` section with `{after-help}` so the
// grouped text appears where the command list normally would.
const GROUPED_HELP: &str = "\
Getting started:
  new                  Create a new mz-deploy project
  init                 Initialize current directory as an mz-deploy project
  walkthrough          Create a new project with an interactive tutorial
  profiles             List available connection profiles
  debug                Test database connection and display environment information

Develop:
  compile              Compile and validate SQL without connecting to database [aliases: build]
  test                 Run SQL unit tests defined in test files
  explore              Generate interactive project documentation

Infrastructure:
  lock                 Generate types.lock file with external dependency schemas
  apply                Apply infrastructure objects to Materialize (Terraform-like)
  delete               Delete an object from Materialize and remove its project file

Deploy:
  stage                Create a staging deployment for testing changes
  wait                 Wait for staging deployment clusters to be hydrated and ready
  promote              Promote a staging deployment to production [aliases: deploy]
  abort                Clean up a staging deployment by dropping all resources
  describe             Show detailed information about a specific deployment [aliases: show]
  list                 List all active staging deployments [aliases: deployments]
  log                  Show history of promoted deployments [aliases: history]

See 'mz-deploy help <command>' for detailed usage guides.";

/// Materialize deployment tool
#[derive(Parser, Debug)]
#[command(name = "mz-deploy", version = VERSION.as_str())]
#[command(about = "Safe, testable deployments for Materialize")]
#[command(disable_help_subcommand = true)]
#[command(override_usage = "mz-deploy [OPTIONS] <COMMAND>")]
#[command(after_help = GROUPED_HELP)]
#[command(help_template = "\
{about}

{usage-heading} {usage}

{all-args}
{after-help}")]
struct Args {
    /// Path to the project root directory containing database schemas
    #[arg(short, long, default_value = ".", global = true, value_hint = clap::ValueHint::DirPath)]
    directory: PathBuf,

    /// Enable verbose output for debugging
    #[arg(short, long, global = true)]
    verbose: bool,

    /// Database connection profile to use (from profiles.toml)
    #[arg(short, long, global = true, env = "MZ_DEPLOY_PROFILE")]
    profile: Option<String>,

    /// Materialize Docker image to use for type checking and tests
    #[arg(long, value_name = "IMAGE", global = true)]
    docker_image: Option<String>,

    /// Directory to search for profiles.toml (default: ~/.mz)
    #[arg(
        long,
        value_name = "DIR",
        global = true,
        env = "MZ_DEPLOY_PROFILES_DIR",
        value_hint = clap::ValueHint::DirPath,
    )]
    profiles_dir: Option<PathBuf>,

    /// Output format (text or json)
    #[arg(long, global = true, default_value = "text")]
    output: OutputFormat,

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
        hide = true,
        visible_alias = "build",
        after_help = "Run 'mz-deploy help compile' for a detailed usage guide."
    )]
    Compile {
        /// Skip SQL type checking (faster but less thorough validation)
        #[arg(long)]
        skip_typecheck: bool,
    },

    /// Generate interactive project documentation
    ///
    /// Compiles the project and generates a self-contained HTML file with an
    /// interactive DAG visualization, object detail pages, and search.
    /// No database connection is required.
    ///
    /// Examples:
    ///   mz-deploy explore                    # Generate and open in browser
    ///   mz-deploy explore --no-open          # Generate without opening
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help explore' for a detailed usage guide."
    )]
    Explore {
        /// Do not open the generated documentation in your browser
        #[arg(long)]
        no_open: bool,

        /// Output directory for generated documentation (default: target/docs/)
        #[arg(long, value_name = "DIR", value_hint = clap::ValueHint::DirPath)]
        output_dir: Option<PathBuf>,
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
    ///   network-policies  Apply network policy definitions from network-policies/ directory
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
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help apply' for a detailed usage guide."
    )]
    Apply {
        /// Skip applying secrets (for users without secret access)
        #[arg(long)]
        skip_secrets: bool,

        /// Preview what would be applied without executing any changes.
        /// Shows the SQL statements that would be run.
        /// Combine with --output json for machine-readable output.
        #[arg(long)]
        dry_run: bool,

        #[command(subcommand)]
        subcommand: Option<ApplyCommand>,
    },

    /// Promote a staging deployment to production
    ///
    /// Atomically swaps a staging deployment into production using ALTER SWAP.
    /// Before promoting, verifies that all staging clusters are fully hydrated
    /// and caught up (unless --no-ready-check is specified).
    ///
    /// Examples:
    ///   mz-deploy promote abc123                    # Promote staging deployment
    ///   mz-deploy promote abc123 --no-ready-check    # Skip hydration check
    ///   mz-deploy promote abc123 --allowed-lag 600  # Allow up to 10 min lag
    #[command(
        hide = true,
        name = "promote",
        visible_alias = "deploy",
        after_help = "Run 'mz-deploy help promote' for a detailed usage guide."
    )]
    Promote {
        /// Staging deployment ID to promote to production
        ///
        /// The deployment ID was assigned when running 'mz-deploy stage'. You can
        /// find active deployments with 'mz-deploy list'.
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
        no_ready_check: bool,

        /// Maximum lag threshold in seconds for readiness check
        ///
        /// During the readiness check, clusters with wallclock lag exceeding this
        /// threshold are marked as "lagging" and will block promotion. Lag measures
        /// how far behind real-time the materialized data is. Default: 300 (5 min).
        #[arg(long, value_name = "SECONDS", default_value = "300")]
        allowed_lag: i64,

        /// Preview the deployment plan without executing any changes.
        /// Shows what would be swapped, created, repointed, and dropped.
        /// Combine with --output json for machine-readable output.
        #[arg(long)]
        dry_run: bool,
    },

    /// Create a staging deployment for testing changes
    ///
    /// Deploys schemas and objects to staging with suffixed names (e.g., 'public_abc123').
    /// This allows testing changes in isolation before promoting to production.
    /// Staging deployments can be listed with 'list' and promoted with 'promote'.
    ///
    /// Example:
    ///   mz-deploy stage                    # Use random deploy ID
    ///   mz-deploy stage --name abc123      # Use custom deploy ID
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help stage' for a detailed usage guide."
    )]
    Stage {
        /// Deploy ID for this staging deployment (default: git SHA prefix, or random 7-char hex)
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

        /// Preview what would be deployed without executing any changes.
        /// Shows the staging resources that would be created.
        /// Add --output json for machine-readable output.
        #[arg(long)]
        dry_run: bool,
    },

    /// Test database connection and display environment information
    ///
    /// Connects to Materialize using the specified profile and displays version,
    /// environment ID, and current role. Useful for verifying connectivity and
    /// configuration before running deployments.
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help debug' for a detailed usage guide."
    )]
    Debug,

    /// Show detailed information about a specific deployment
    ///
    /// Displays comprehensive information about a deployment including metadata
    /// (who deployed, when, git commit) and all objects with their hashes.
    /// Use `mz-deploy log` to find deployment IDs.
    ///
    /// Example:
    ///   mz-deploy describe abc123
    #[command(
        hide = true,
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
        hide = true,
        name = "lock",
        after_help = "Run 'mz-deploy help lock' for a detailed usage guide."
    )]
    Lock,

    /// Run SQL unit tests defined in test files
    ///
    /// Executes all test files in the project against a temporary Materialize
    /// Docker container. Tests validate SQL logic without affecting production.
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help test' for a detailed usage guide."
    )]
    Test {
        /// Filter which tests to run (e.g. "db.schema.object#test_name", supports trailing *)
        filter: Option<String>,
        /// Write test results to a JUnit XML file
        #[arg(long, value_name = "FILE", value_hint = clap::ValueHint::FilePath)]
        junit_xml: Option<PathBuf>,
    },

    /// Clean up a staging deployment by dropping all resources
    ///
    /// Removes staging schemas, clusters, and deployment tracking records for
    /// the specified deploy ID. This is the equivalent of 'git branch -D' for
    /// staging deployments.
    ///
    /// Example:
    ///   mz-deploy abort abc123
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help abort' for a detailed usage guide."
    )]
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
    ///   mz-deploy list                         # List with default lag threshold
    ///   mz-deploy list --allowed-lag 60        # Stricter lag threshold
    #[command(
        hide = true,
        name = "list",
        visible_alias = "deployments",
        alias = "branches",
        after_help = "Run 'mz-deploy help list' for a detailed usage guide."
    )]
    List {
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
    ///   mz-deploy log --limit 10
    #[command(
        hide = true,
        name = "log",
        visible_alias = "history",
        after_help = "Run 'mz-deploy help log' for a detailed usage guide."
    )]
    Log {
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
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help new' for a detailed usage guide."
    )]
    New {
        /// Name of the project directory to create
        #[arg(value_name = "NAME")]
        name: String,

        /// Skip git repository initialization
        #[arg(long)]
        no_git: bool,

        /// Skip npx agent skill installation
        #[arg(long)]
        no_skill: bool,
    },

    /// Initialize current directory as an mz-deploy project
    ///
    /// Scaffolds the current directory with the required structure for mz-deploy,
    /// similar to `new` but without creating a new directory. Derives the project
    /// name from the current directory name.
    ///
    /// Example:
    ///   mkdir my-project && cd my-project && mz-deploy init
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help init' for a detailed usage guide."
    )]
    Init {
        /// Skip git repository initialization
        #[arg(long)]
        no_git: bool,

        /// Skip npx agent skill installation
        #[arg(long)]
        no_skill: bool,
    },

    /// Create a new project with an interactive tutorial
    ///
    /// Scaffolds a project directory like `new`, then adds a walkthrough skill
    /// that guides you through building a data mesh with Materialize. The result
    /// is a two-commit project: the standard scaffold plus the walkthrough.
    ///
    /// Example:
    ///   mz-deploy walkthrough my-project
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help walkthrough' for a detailed usage guide."
    )]
    Walkthrough {
        /// Name of the project directory to create
        #[arg(value_name = "NAME", default_value = "the-great-ontology")]
        name: String,

        /// Skip git repository initialization
        #[arg(long)]
        no_git: bool,
    },

    /// List available connection profiles
    ///
    /// Shows all profiles defined in profiles.toml and indicates which one
    /// is currently active. The active profile is determined by the --profile
    /// flag, or the default set in project.toml.
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help profiles' for a detailed usage guide."
    )]
    Profiles,

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
    ///   mz-deploy wait abc123                     # Wait with live tracking
    ///   mz-deploy wait abc123 --once              # Check once and exit
    ///   mz-deploy wait abc123 --timeout 300       # Wait up to 5 minutes
    ///   mz-deploy wait abc123 --allowed-lag 60    # Require lag under 1 min
    #[command(
        hide = true,
        name = "wait",
        after_help = "Run 'mz-deploy help wait' for a detailed usage guide."
    )]
    Wait {
        /// Staging deployment ID to monitor
        #[arg(value_name = "DEPLOY_ID")]
        name: String,

        /// Check status once and exit instead of continuous monitoring
        ///
        /// Takes a point-in-time snapshot of cluster status and exits immediately.
        /// Returns success (exit 0) only if all clusters are ready.
        #[arg(long)]
        once: bool,

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
    #[command(
        hide = true,
        after_help = "Run 'mz-deploy help delete' for a detailed usage guide."
    )]
    Delete {
        /// Skip confirmation prompt. Required when using --output json
        #[arg(short = 'y', long)]
        yes: bool,

        #[command(subcommand)]
        subcommand: DeleteCommand,
    },

    /// Generate shell completions
    #[command(hide = true)]
    Completions {
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },

    /// Show detailed usage guide for a command. Useful for LLM coding agents
    ///
    /// Prints extended documentation including behavior notes, examples,
    /// error recovery guidance, and related commands. Use --all to print
    /// the complete reference for all commands.
    #[command(hide = true)]
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
    /// Apply network policy definitions from network-policies/ directory
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
    /// Delete a source
    Source {
        /// Fully-qualified source name (database.schema.name)
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
    log::set_json_output(matches!(args.output, OutputFormat::Json));

    if let Err(e) = run(args).await {
        if log::json_output_enabled() {
            let error_json = serde_json::json!({
                "error": e.to_string(),
            });
            log::output_json(&error_json);
            std::process::exit(1);
        } else {
            cli::display_error(&e);
        }
    }
}

async fn run(args: Args) -> Result<(), CliError> {
    // Handle completions before anything else
    if let Some(Command::Completions { shell }) = &args.command {
        clap_complete::generate(
            *shell,
            &mut Args::command(),
            "mz-deploy",
            &mut std::io::stdout(),
        );
        return Ok(());
    }

    // Handle commands that don't require an existing project
    if let Some(Command::Help { command, all }) = &args.command {
        if *all {
            eprint!("{}", cli::extended_help::all_help());
        } else if let Some(cmd) = command {
            match cli::extended_help::help_for(cmd) {
                Some(text) => eprint!("{text}"),
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

    if let Some(Command::New {
        name,
        no_git,
        no_skill,
    }) = &args.command
    {
        return cli::commands::new_project::run(
            name,
            ScaffoldOpts {
                init_git: !no_git,
                install_skill: !no_skill,
            },
        );
    }

    if let Some(Command::Init { no_git, no_skill }) = &args.command {
        return cli::commands::new_project::init(ScaffoldOpts {
            init_git: !no_git,
            install_skill: !no_skill,
        });
    }

    if let Some(Command::Walkthrough { name, no_git }) = &args.command {
        return cli::commands::walkthrough::run(name, !no_git);
    }

    if let Some(Command::Profiles) = &args.command {
        return cli::commands::profiles::run(
            &args.directory,
            args.profile.as_deref(),
            args.profiles_dir.as_deref(),
        );
    }

    let needs_connection = !matches!(
        &args.command,
        Some(Command::Compile { .. }) | Some(Command::Test { .. }) | Some(Command::Explore { .. })
    );
    let settings = Settings::load(
        args.directory,
        args.profile.as_deref(),
        args.docker_image.as_deref(),
        needs_connection,
        args.profiles_dir.as_deref(),
    )
    .map_err(CliError::Config)?;

    match args.command {
        Some(Command::Compile { skip_typecheck }) => {
            if log::json_output_enabled() {
                return Err(CliError::Message(
                    "--output json is not supported for the 'compile' command".to_string(),
                ));
            }
            cli::commands::compile::run(&settings, skip_typecheck, true)
                .await
                .map(|_| ())
        }
        Some(Command::Explore {
            no_open,
            output_dir,
        }) => {
            if log::json_output_enabled() {
                return Err(CliError::Message(
                    "--output json is not supported for the 'explore' command".to_string(),
                ));
            }
            cli::commands::explore::run(&settings, output_dir, !no_open).await
        }
        Some(Command::Apply {
            skip_secrets,
            dry_run,
            subcommand,
        }) => {
            let plan = match subcommand {
                Some(ApplyCommand::Clusters) => {
                    cli::commands::clusters::run(&settings, dry_run).await?
                }
                Some(ApplyCommand::Roles) => cli::commands::roles::run(&settings, dry_run).await?,
                Some(ApplyCommand::NetworkPolicies) => {
                    cli::commands::apply_network_policies::run(&settings, dry_run).await?
                }
                Some(ApplyCommand::Secrets) => {
                    cli::commands::apply_secrets::run(&settings, dry_run).await?
                }
                Some(ApplyCommand::Connections) => {
                    cli::commands::apply_connections::run(&settings, dry_run).await?
                }
                Some(ApplyCommand::Sources) => {
                    cli::commands::apply_sources::run(&settings, dry_run).await?
                }
                Some(ApplyCommand::Tables) => {
                    cli::commands::apply_tables::run(&settings, dry_run).await?
                }
                None => cli::commands::apply_all::run(&settings, skip_secrets, dry_run).await?,
            };
            log::output(&plan);
            Ok(())
        }
        Some(Command::Promote {
            deploy_id,
            force,
            no_ready_check,
            allowed_lag,
            dry_run,
        }) => {
            if !no_ready_check && !dry_run {
                cli::commands::wait::run(&settings, &deploy_id, true, None, allowed_lag).await?;
            }
            cli::commands::promote::run(&settings, &deploy_id, force, dry_run).await
        }
        Some(Command::Stage {
            deploy_id,
            allow_dirty,
            no_rollback,
            dry_run,
        }) => {
            cli::commands::stage::run(
                &settings,
                deploy_id.as_deref(),
                allow_dirty,
                no_rollback,
                dry_run,
            )
            .await
        }
        Some(Command::Debug) => cli::commands::debug::run(&settings).await,
        Some(Command::Describe { deploy_id }) => {
            cli::commands::describe::run(&settings, &deploy_id).await
        }
        Some(Command::Lock) => {
            if log::json_output_enabled() {
                return Err(CliError::Message(
                    "--output json is not supported for the 'lock' command".to_string(),
                ));
            }
            cli::commands::lock::run(&settings).await
        }
        Some(Command::Test { filter, junit_xml }) => {
            if log::json_output_enabled() {
                return Err(CliError::Message(
                    "--output json is not supported for the 'test' command".to_string(),
                ));
            }
            cli::commands::test::run(&settings, filter.as_deref(), junit_xml.as_deref()).await
        }
        Some(Command::Abort { deploy_id }) => {
            cli::commands::abort::run(&settings, &deploy_id).await
        }
        Some(Command::List { allowed_lag }) => {
            cli::commands::list::run(&settings, allowed_lag).await
        }
        Some(Command::Log { limit }) => cli::commands::log::run(&settings, limit).await,
        Some(Command::Wait {
            name,
            once,
            timeout,
            allowed_lag,
        }) => cli::commands::wait::run(&settings, &name, once, timeout, allowed_lag).await,
        Some(Command::Delete { yes, subcommand }) => {
            let (kind, name) = match subcommand {
                DeleteCommand::Cluster { name } => (delete::ObjectKind::Cluster, name),
                DeleteCommand::Connection { name } => (delete::ObjectKind::Connection, name),
                DeleteCommand::NetworkPolicy { name } => (delete::ObjectKind::NetworkPolicy, name),
                DeleteCommand::Role { name } => (delete::ObjectKind::Role, name),
                DeleteCommand::Secret { name } => (delete::ObjectKind::Secret, name),
                DeleteCommand::Source { name } => (delete::ObjectKind::Source, name),
                DeleteCommand::Table { name } => (delete::ObjectKind::Table, name),
            };
            delete::run(&settings, kind, &name, yes).await
        }
        Some(Command::Completions { .. }) => unreachable!("handled above"),
        Some(Command::Help { .. }) => unreachable!("handled above"),
        Some(Command::New { .. }) => unreachable!("handled above"),
        Some(Command::Init { .. }) => unreachable!("handled above"),
        Some(Command::Walkthrough { .. }) => unreachable!("handled above"),
        Some(Command::Profiles) => unreachable!("handled above"),
        None => unreachable!("handled above"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn grouped_help_lists_all_subcommands() {
        let cmd = Args::command();
        let mut errors = Vec::new();
        for sub in cmd.get_subcommands() {
            let name = sub.get_name();
            // "help" is intentionally omitted from the grouped listing since
            // the footer line ("See 'mz-deploy help <command>'") already
            // tells users about it.
            if name == "help" || name == "completions" {
                continue;
            }

            // Find the line in GROUPED_HELP that starts with this command name.
            let line = GROUPED_HELP
                .lines()
                .find(|l| l.trim_start().starts_with(&format!("{name} ")));

            match line {
                None => errors.push(format!("missing command: {name}")),
                Some(line) => {
                    let about = sub
                        .get_about()
                        .expect("every subcommand should have an about")
                        .to_string();

                    if !line.contains(&about) {
                        errors.push(format!(
                            "description mismatch for '{name}':\n  \
                             GROUPED_HELP:{line}\n  \
                             clap about:   {about}"
                        ));
                    }
                }
            }
        }
        assert!(
            errors.is_empty(),
            "GROUPED_HELP is out of sync with Command variants:\n{}",
            errors.join("\n"),
        );
    }
}
