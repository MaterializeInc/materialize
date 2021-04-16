// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The main Materialize server.
//!
//! The name is pronounced "materialize-dee." It listens on port 6875 (MTRL).
//!
//! The design and implementation of materialized is very much in flux. See the
//! draft architecture doc for the most up-to-date plan [0]. Access is limited
//! to those with access to the Material Dropbox Paper folder.
//!
//! [0]: https://paper.dropbox.com/doc/Materialize-architecture-plans--AYSu6vvUu7ZDoOEZl7DNi8UQAg-sZj5rhJmISdZSfK0WBxAl

use std::cmp;
use std::env;
use std::ffi::CStr;
use std::fmt;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::panic;
use std::panic::PanicInfo;
use std::path::PathBuf;
use std::process;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context};
use backtrace::Backtrace;
use clap::AppSettings;
use itertools::Itertools;
use lazy_static::lazy_static;
use log::{info, warn};
use prometheus::{register_int_counter_vec, IntCounterVec};
use structopt::StructOpt;
use sysinfo::{ProcessorExt, SystemExt};

use self::tracing::MetricsRecorderLayer;
use materialized::TlsMode;

mod sys;
mod tracing;

type OptionalDuration = Option<Duration>;

fn parse_optional_duration(s: &str) -> Result<OptionalDuration, anyhow::Error> {
    match s {
        "off" => Ok(None),
        _ => Ok(Some(parse_duration::parse(s)?)),
    }
}

/// The streaming SQL materialized view engine.
#[derive(StructOpt)]
#[structopt(settings = &[AppSettings::NextLineHelp, AppSettings::UnifiedHelpMessage], usage = "materialized [OPTION]...")]
struct Args {
    // === Special modes. ===
    /// Print version information and exit.
    ///
    /// Specify twice to additionally print version information for selected
    /// dependencies.
    #[structopt(short, long, parse(from_occurrences))]
    version: usize,
    /// Allow running this dev (unoptimized) build.
    #[cfg(debug_assertions)]
    #[structopt(long)]
    dev: bool,
    // TODO(benesch): add an environment variable once we upgrade to clap v3.
    // Doesn't presently work in clap v2. See: clap-rs/clap#1476.
    /// [DANGEROUS] Enable experimental features.
    #[structopt(long)]
    experimental: bool,
    /// Whether to run in safe mode.
    ///
    /// In safe mode, features that provide access to the underlying machine,
    /// like file sources and sinks, are disabled.
    ///
    /// This option is intended for use by the cloud product
    /// (cloud.materialize.com), but may be useful in other contexts as well.
    #[structopt(long, hidden = true)]
    safe: bool,

    /// Enable persistent tables. Has to be used with --experimental.
    #[structopt(long)]
    persistent_tables: bool,

    // === Timely worker configuration. ===
    /// Number of dataflow worker threads.
    #[structopt(short, long, env = "MZ_WORKERS", value_name = "N", default_value)]
    workers: WorkerCount,
    /// Log Timely logging itself.
    #[structopt(long, hidden = true)]
    debug_introspection: bool,
    /// Retain prometheus metrics for this amount of time.
    #[structopt(short, long, hidden = true, parse(try_from_str = parse_duration::parse), default_value = "5min")]
    retain_prometheus_metrics: Duration,

    // === Performance tuning parameters. ===
    /// The frequency at which to update introspection sources.
    ///
    /// The introspection sources are the built-in sources in the mz_catalog
    /// schema, like mz_scheduling_elapsed, that reflect the internal state of
    /// Materialize's dataflow engine.
    ///
    /// Set to "off" to disable introspection.
    #[structopt(long, env = "MZ_INTROSPECTION_FREQUENCY", parse(try_from_str = parse_optional_duration), value_name = "FREQUENCY", default_value = "1s")]
    introspection_frequency: OptionalDuration,
    /// How much historical detail to maintain in arrangements.
    ///
    /// Set to "off" to disable logical compaction.
    #[structopt(long, env = "MZ_LOGICAL_COMPACTION_WINDOW", parse(try_from_str = parse_optional_duration), value_name = "DURATION", default_value = "1ms")]
    logical_compaction_window: OptionalDuration,
    /// Default frequency with which to advance timestamps
    #[structopt(long, env = "MZ_TIMESTAMP_FREQUENCY", hidden = true, parse(try_from_str = parse_duration::parse), value_name = "DURATION", default_value = "1s")]
    timestamp_frequency: Duration,

    /// Maximum number of source records to buffer in memory before flushing to
    /// disk.
    #[structopt(
        long,
        env = "MZ_CACHE_MAX_PENDING_RECORDS",
        value_name = "N",
        default_value = "1000000"
    )]
    cache_max_pending_records: usize,
    /// [ADVANCED] Timely progress tracking mode.
    #[structopt(long, env = "MZ_TIMELY_PROGRESS_MODE", value_name = "MODE", possible_values = &["eager", "demand"], default_value = "demand")]
    timely_progress_mode: timely::worker::ProgressMode,
    /// [ADVANCED] Amount of compaction to perform when idle.
    #[structopt(long, env = "MZ_DIFFERENTIAL_IDLE_MERGE_EFFORT", value_name = "N")]
    differential_idle_merge_effort: Option<isize>,

    // === Logging options. ===
    /// Where to emit log messages.
    ///
    /// The special value "stderr" will emit messages to the standard error
    /// stream. All other values are taken as file paths.
    #[structopt(long, env = "MZ_LOG_FILE", value_name = "PATH")]
    log_file: Option<String>,
    /// Which log messages to emit.
    ///
    /// This value is a comma-separated list of filter directives. Each filter
    /// directive has the following format:
    ///
    ///     [module::path=]level
    ///
    /// A directive indicates that log messages from the specified module that
    /// are at least as severe as the specified level should be emitted. If a
    /// directive omits the module, then it implicitly applies to all modules.
    /// When directives conflict, the last directive wins. If a log message does
    /// not match any directive, it is not emitted.
    ///
    /// The module path of a log message reflects its location in Materialize's
    /// source code. Choosing module paths for filter directives requires
    /// familiarity with Materialize's codebase and is intended for advanced
    /// users. Note that module paths change frequency from release to release.
    ///
    /// The valid levels for a log message are, in increasing order of severity:
    /// trace, debug, info, warn, and error. The special level "off" may be used
    /// in a directive to suppress all log messages, even errors.
    ///
    /// The default value for this option is "info".
    #[structopt(long, env = "MZ_LOG_FILTER", value_name = "FILTER")]
    log_filter: Option<String>,

    // == Connection options.
    /// The address on which to listen for connections.
    #[structopt(
        long,
        env = "MZ_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6875"
    )]
    listen_addr: SocketAddr,
    /// How stringently to demand TLS authentication and encryption.
    ///
    /// If set to "disable", then materialized rejects HTTP and PostgreSQL
    /// connections that negotiate TLS.
    ///
    /// If set to "require", then materialized requires that all HTTP and
    /// PostgreSQL connections negotiate TLS. Unencrypted connections will be
    /// rejected.
    ///
    /// If set to "verify-ca", then materialized requires that all HTTP and
    /// PostgreSQL connections negotiate TLS and supply a certificate signed by
    /// a trusted certificate authority (CA). HTTP connections will operate as
    /// the system user in this mode, while PostgreSQL connections will assume
    /// the name of whatever user is specified in the handshake.
    ///
    /// The "verify-full" mode is like "verify-ca", except that the Common Name
    /// (CN) field of the certificate must match the name of a valid user. HTTP
    /// and PostgreSQL connections will operate as this user. PostgreSQL
    /// connections must additionally specify the same username in the
    /// connection parameters.
    ///
    /// The most secure mode is "verify-full". This is the default mode when
    /// the --tls-cert option is specified. Otherwise the default is "disable".
    #[structopt(
        long, env = "MZ_TLS_MODE",
        possible_values = &["disable", "require", "verify-ca", "verify-full"],
        default_value = "disable",
        default_value_if("tls-cert", None, "verify-full"),
        value_name = "MODE",
    )]
    tls_mode: String,
    #[structopt(
        long,
        env = "MZ_TLS_CA",
        required_if("tls-mode", "verify-ca"),
        required_if("tls-mode", "verify-full"),
        value_name = "PATH"
    )]
    tls_ca: Option<PathBuf>,
    /// Certificate file for TLS connections.
    #[structopt(
        long,
        env = "MZ_TLS_CERT",
        requires = "tls-key",
        required_ifs(&[("tls-mode", "allow"), ("tls-mode", "require"), ("tls-mode", "verify-ca"), ("tls-mode", "verify-full")]),
        value_name = "PATH"
    )]
    tls_cert: Option<PathBuf>,
    /// Private key file for TLS connections.
    #[structopt(
        long,
        env = "MZ_TLS_KEY",
        requires = "tls-cert",
        required_ifs(&[("tls-mode", "allow"), ("tls-mode", "require"), ("tls-mode", "verify-ca"), ("tls-mode", "verify-full")]),
        value_name = "PATH"
    )]
    tls_key: Option<PathBuf>,

    // === Storage options. ===
    /// Where to store data.
    #[structopt(
        short = "D",
        long,
        env = "MZ_DATA_DIRECTORY",
        value_name = "PATH",
        default_value = "mzdata"
    )]
    data_directory: PathBuf,
    /// Enable symbioisis with a PostgreSQL server.
    #[structopt(long, env = "MZ_SYMBIOSIS", hidden = true)]
    symbiosis: Option<String>,

    // === Telemetry options. ===
    // TODO(benesch): add an environment variable once we upgrade to clap v3.
    // Doesn't presently work in clap v2. See: clap-rs/clap#1476.
    /// Disable telemetry reporting.
    #[structopt(long, conflicts_with = "telemetry-url")]
    disable_telemetry: bool,
    /// The URL of the telemetry server to report to.
    #[structopt(long, env = "MZ_TELEMETRY_URL", hidden = true)]
    telemetry_url: Option<String>,
}

/// This type is a hack to allow a dynamic default for the `--workers` argument,
/// which depends on the number of available CPUs. Ideally structopt would
/// expose a `default_fn` rather than accepting only string literals.
struct WorkerCount(usize);

impl Default for WorkerCount {
    fn default() -> Self {
        WorkerCount(cmp::max(1, num_cpus::get_physical() / 2))
    }
}

impl FromStr for WorkerCount {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<WorkerCount, anyhow::Error> {
        let n = s.parse()?;
        if n == 0 {
            bail!("must be greater than zero");
        }
        Ok(WorkerCount(n))
    }
}

impl fmt::Display for WorkerCount {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

fn main() {
    if let Err(err) = run(Args::from_args()) {
        eprintln!("materialized: {:#}", err);
        process::exit(1);
    }
}

fn run(args: Args) -> Result<(), anyhow::Error> {
    panic::set_hook(Box::new(handle_panic));
    sys::enable_sigbus_sigsegv_backtraces()?;

    if args.version > 0 {
        println!("materialized {}", materialized::BUILD_INFO.human_version());
        if args.version > 1 {
            for bi in build_info() {
                println!("{}", bi);
            }
        }
        return Ok(());
    }

    // Prevent accidental usage of development builds.
    //
    // TODO(benesch): offload environment variable check to clap once we upgrade
    // to clap v3. Doesn't presently work in clap v2. See: clap-rs/clap#1476.
    #[cfg(debug_assertions)]
    if !args.dev && !ore::env::is_var_truthy("MZ_DEV") {
        bail!(
            "refusing to run dev (unoptimized) binary without explicit opt-in\n\
             hint: Pass the '--dev' option or set MZ_DEV=1 in your environment to opt in.\n\
             hint: Or perhaps you meant to use a release binary?"
        );
    }

    // Configure Timely and Differential workers.
    let log_logging = args.debug_introspection;
    let retain_readings_for = args.retain_prometheus_metrics;
    let logging = args
        .introspection_frequency
        .map(|granularity| coord::LoggingConfig {
            granularity,
            log_logging,
            retain_readings_for,
        });
    if log_logging && logging.is_none() {
        bail!(
            "cannot specify --debug-introspection and --introspection-frequency=off simultaneously"
        );
    }

    // Configure connections.
    let tls = if args.tls_mode == "disable" {
        if args.tls_ca.is_some() {
            bail!("cannot specify --tls-mode=disable and --tls-ca simultaneously");
        }
        if args.tls_cert.is_some() {
            bail!("cannot specify --tls-mode=disable and --tls-cert simultaneously");
        }
        if args.tls_key.is_some() {
            bail!("cannot specify --tls-mode=disable and --tls-key simultaneously");
        }
        None
    } else {
        let mode = match args.tls_mode.as_str() {
            "require" => {
                if args.tls_ca.is_some() {
                    bail!("cannot specify --tls-mode=require and --tls-ca simultaneously");
                }
                TlsMode::Require
            }
            "verify-ca" => TlsMode::VerifyCa {
                ca: args.tls_ca.unwrap(),
            },
            "verify-full" => TlsMode::VerifyFull {
                ca: args.tls_ca.unwrap(),
            },
            _ => unreachable!(),
        };
        let cert = args.tls_cert.unwrap();
        let key = args.tls_key.unwrap();
        Some(materialized::TlsConfig { mode, cert, key })
    };

    // Configure storage.
    let data_directory = args.data_directory;
    fs::create_dir_all(&data_directory)
        .with_context(|| format!("creating data directory: {}", data_directory.display()))?;

    // Configure source caching.
    let cache = if args.experimental {
        let cache_directory = data_directory.join("cache");
        fs::create_dir_all(&cache_directory).with_context(|| {
            format!(
                "creating source caching directory: {}",
                cache_directory.display()
            )
        })?;

        Some(coord::CacheConfig {
            max_pending_records: args.cache_max_pending_records,
            path: cache_directory,
        })
    } else {
        None
    };

    let persistence = if args.experimental && args.persistent_tables {
        let traces_path = data_directory.join("table_traces");
        let wals_path = data_directory.join("table_wals");
        fs::create_dir_all(&traces_path).with_context(|| {
            format!(
                "trying to create traces directory: {}",
                traces_path.display()
            )
        })?;
        fs::create_dir_all(&wals_path)
            .with_context(|| format!("trying to create wal directory: {}", wals_path.display()))?;

        Some(coord::PersistenceConfig {
            traces_path,
            wals_path,
        })
    } else if args.persistent_tables {
        bail!("cannot specify --persistent-tables without --experimental");
    } else {
        None
    };

    // If --disable-telemetry is present, disable telemetry. Otherwise, if a
    // MZ_TELEMETRY_URL environment variable is set, use that as the telemetry
    // URL. Otherwise (the defaults), enable the production server for release mode
    // and disable telemetry in debug mode. This should allow for good defaults (on
    // in release, off in debug), but also easy development during testing of this
    // feature via the environment variable.
    let telemetry_url = match args.disable_telemetry {
        true => None,
        false => match args.telemetry_url {
            Some(url) => Some(url),
            None => match cfg!(debug_assertions) {
                true => None,
                false => Some("https://telemetry.materialize.com".into()),
            },
        },
    };

    // Configure tracing.
    {
        use tracing_subscriber::filter::{EnvFilter, LevelFilter};
        use tracing_subscriber::fmt;
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        use crate::tracing::FilterLayer;

        // TODO(benesch): remove the MZ_LOG fallback and move the default into
        // structopt when sufficient time has passed (say, June 2021).
        let directives = args
            .log_filter
            .or_else(|| env::var("MZ_LOG").ok())
            .unwrap_or_else(|| "info".into());

        let env_filter = EnvFilter::try_new(directives)
            .context("parsing --log-filter option")?
            // Ensure panics are logged, even if the user has specified
            // otherwise.
            .add_directive("panic=error".parse().unwrap());

        lazy_static! {
            static ref LOG_MESSAGE_COUNTER: IntCounterVec = register_int_counter_vec!(
                "mz_log_message_total",
                "The number of log messages produced by this materialized instance",
                &["severity"]
            )
            .unwrap();
        }

        match args.log_file.as_deref() {
            Some("stderr") => {
                // The user explicitly directed logs to stderr. Log only to stderr
                // with the user-specified `env_filter`.
                tracing_subscriber::registry()
                    .with(MetricsRecorderLayer::new(LOG_MESSAGE_COUNTER.clone()))
                    .with(env_filter)
                    .with(
                        fmt::layer()
                            .with_writer(io::stderr)
                            .with_ansi(atty::is(atty::Stream::Stderr)),
                    )
                    .init()
            }
            log_file => {
                // Logging to a file. If the user did not explicitly specify
                // a file, bubble up warnings and errors to stderr.
                let stderr_level = match log_file {
                    Some(_) => LevelFilter::OFF,
                    None => LevelFilter::WARN,
                };
                tracing_subscriber::registry()
                    .with(MetricsRecorderLayer::new(LOG_MESSAGE_COUNTER.clone()))
                    .with(env_filter)
                    .with({
                        let path = match log_file {
                            Some(log_file) => PathBuf::from(log_file),
                            None => data_directory.join("materialized.log"),
                        };
                        if let Some(parent) = path.parent() {
                            fs::create_dir_all(parent).with_context(|| {
                                format!("creating log file directory: {}", parent.display())
                            })?;
                        }
                        let file = fs::OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&path)
                            .with_context(|| format!("creating log file: {}", path.display()))?;
                        fmt::layer().with_ansi(false).with_writer(move || {
                            file.try_clone().expect("failed to clone log file")
                        })
                    })
                    .with(FilterLayer::new(
                        fmt::layer()
                            .with_writer(io::stderr)
                            .with_ansi(atty::is(atty::Stream::Stderr)),
                        stderr_level,
                    ))
                    .init()
            }
        }
    }

    // Configure prometheus process metrics.
    mz_process_collector::register_default_process_collector()?;

    // Print system information as the very first thing in the logs. The goal is
    // to increase the probability that we can reproduce a reported bug if all
    // we get is the log file.
    let mut system = sysinfo::System::new();
    system.refresh_system();
    info!(
        "booting server
materialized {mz_version}
{dep_versions}
invoked as: {invocation}
os: {os}
cpus: {ncpus_logical} logical, {ncpus_physical} physical
cpu0: {cpu0}
memory: {memory_total}KB total, {memory_used}KB used
swap: {swap_total}KB total, {swap_used}KB used",
        mz_version = materialized::BUILD_INFO.human_version(),
        dep_versions = build_info().join("\n"),
        invocation = {
            use shell_words::quote as escape;
            env::vars()
                .filter(|(name, _value)| name.starts_with("MZ_"))
                .map(|(name, value)| format!("{}={}", escape(&name), escape(&value)))
                .chain(env::args().into_iter().map(|arg| escape(&arg).into_owned()))
                .join(" ")
        },
        os = os_info::get(),
        ncpus_logical = num_cpus::get(),
        ncpus_physical = num_cpus::get_physical(),
        cpu0 = {
            let cpu0 = &system.get_processors()[0];
            format!("{} {}MHz", cpu0.get_brand(), cpu0.get_frequency())
        },
        memory_total = system.get_total_memory(),
        memory_used = system.get_used_memory(),
        swap_total = system.get_total_swap(),
        swap_used = system.get_used_swap(),
    );

    sys::adjust_rlimits();

    // Build Timely worker configuration.
    let mut timely_worker =
        timely::WorkerConfig::default().progress_mode(args.timely_progress_mode);
    differential_dataflow::configure(
        &mut timely_worker,
        &differential_dataflow::Config {
            idle_merge_effort: args.differential_idle_merge_effort,
        },
    );

    // Start Tokio runtime.
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            // The default thread name exceeds the Linux limit on thread name
            // length, so pick something shorter.
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("tokio:work-{}", id)
            })
            .enable_all()
            .build()?,
    );

    let server = runtime.block_on(materialized::serve(
        materialized::Config {
            workers: args.workers.0,
            timely_worker,
            logging,
            logical_compaction_window: args.logical_compaction_window,
            timestamp_frequency: args.timestamp_frequency,
            cache,
            persistence,
            listen_addr: args.listen_addr,
            tls,
            data_directory,
            symbiosis_url: args.symbiosis,
            experimental_mode: args.experimental,
            safe_mode: args.safe,
            telemetry_url,
        },
        runtime.clone(),
    ))?;

    eprintln!(
        "=======================================================================
Thank you for trying Materialize!

We are interested in any and all feedback you have, which may be able
to improve both our software and your queries! Please reach out at:

    Web: https://materialize.com
    GitHub issues: https://github.com/MaterializeInc/materialize/issues
    Email: support@materialize.io
    Twitter: @MaterializeInc
=======================================================================
"
    );

    if args.experimental {
        eprintln!(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                WARNING!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Starting Materialize in experimental mode means:

- This node's catalog of views and sources are unstable.

If you use any version of Materialize besides this one, you might
not be able to start the Materialize node. To fix this, you'll have
to remove all of Materialize's data (e.g. rm -rf mzdata) and start
the node anew.

- You must always start this node in experimental mode; it can no
longer be started in non-experimental/regular mode.

For more details, see https://materialize.com/docs/cli#experimental-mode
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
"
        );
    }

    // TODO(benesch): remove this message when sufficient time has passed
    // (say, June 2021).
    if env::var_os("MZ_LOG").is_some() {
        warn!(
            "The MZ_LOG environment variable is deprecated and will be removed \
            in a future release. Use the MZ_LOG_FILTER environment variable or \
            the --log-filter command-line option instead."
        );
    }

    println!(
        "materialized {} listening on {}...",
        materialized::BUILD_INFO.human_version(),
        server.local_addr(),
    );

    // Block forever.
    loop {
        thread::park();
    }
}

lazy_static! {
    static ref PANIC_MUTEX: Mutex<()> = Mutex::new(());
}

fn handle_panic(panic_info: &PanicInfo) {
    let _guard = PANIC_MUTEX.lock();

    let thr = thread::current();
    let thr_name = thr.name().unwrap_or("<unnamed>");

    let msg = match panic_info.payload().downcast_ref::<&'static str>() {
        Some(s) => *s,
        None => match panic_info.payload().downcast_ref::<String>() {
            Some(s) => &s[..],
            None => "Box<Any>",
        },
    };

    let location = if let Some(loc) = panic_info.location() {
        loc.to_string()
    } else {
        "<unknown>".to_string()
    };

    log::error!(
        target: "panic",
        "{msg}
thread: {thr_name}
location: {location}
version: {version} ({sha})
backtrace:
{backtrace:?}",
        msg = msg,
        thr_name = thr_name,
        location = location,
        version = materialized::BUILD_INFO.version,
        sha = materialized::BUILD_INFO.sha,
        backtrace = Backtrace::new(),
    );
    eprintln!(
        r#"materialized encountered an internal error and crashed.

We rely on bug reports to diagnose and fix these errors. Please
copy and paste the above details and file a report at:

    https://materialize.com/s/bug
"#,
    );
    process::exit(1);
}

fn build_info() -> Vec<String> {
    let openssl_version =
        unsafe { CStr::from_ptr(openssl_sys::OpenSSL_version(openssl_sys::OPENSSL_VERSION)) };
    let rdkafka_version = unsafe { CStr::from_ptr(rdkafka_sys::bindings::rd_kafka_version_str()) };
    vec![
        openssl_version.to_string_lossy().into_owned(),
        format!("librdkafka v{}", rdkafka_version.to_string_lossy()),
    ]
}
