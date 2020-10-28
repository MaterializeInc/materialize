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

use std::env::VarError;
use std::ffi::CStr;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::panic;
use std::panic::PanicInfo;
use std::path::PathBuf;
use std::process;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use std::{cmp, env};

use anyhow::{anyhow, bail, Context};
use backtrace::Backtrace;
use lazy_static::lazy_static;
use log::{info, warn};
use sysinfo::{ProcessorExt, SystemExt};

mod sys;
mod tracing;

fn main() {
    if let Err(err) = run() {
        eprintln!("materialized: {:#}", err);
        process::exit(1);
    }
}

fn run() -> Result<(), anyhow::Error> {
    panic::set_hook(Box::new(handle_panic));

    let args: Vec<_> = env::args().collect();
    let mut opts = getopts::Options::new();

    // Options that request informational output.
    opts.optflag("h", "help", "show this usage information");
    opts.optflagmulti(
        "v",
        "version",
        "print version and exit (use -vv for additional info)",
    );

    // Accidental debug build protection.
    if cfg!(debug_assertions) {
        opts.optflag("", "dev", "allow running this dev (unoptimized) build");
    }

    // Timely and Differential worker options.
    opts.optopt(
        "w",
        "workers",
        "number of per-process timely worker threads",
        "N",
    );
    opts.optopt("", "threads", "deprecated alias for --workers", "N");
    opts.optopt(
        "p",
        "process",
        "identity of this node when coordinating with other nodes (default 0)",
        "INDEX",
    );
    opts.optopt(
        "n",
        "processes",
        "total number of coordinating nodes (default 1)",
        "N",
    );
    opts.optopt(
        "a",
        "address-file",
        "text file whose lines are process addresses",
        "FILE",
    );

    // Performance tuning parameters.
    opts.optopt(
        "l",
        "logging-granularity",
        "dataflow logging granularity (default 1s)",
        "DURATION/\"off\"",
    );
    opts.optflag("", "debug-timely-logging", "(internal use only)");
    opts.optopt(
        "",
        "logical-compaction-window",
        "historical detail maintained for arrangements (default 60s)",
        "DURATION/\"off\"",
    );
    opts.optopt(
        "",
        "timestamp-frequency",
        "timestamp advancement frequency (default 10ms)",
        "DURATION",
    );
    opts.optopt(
        "",
        "persistence-max-pending-records",
        "maximum number of records that have to be present before materialize will persist them immediately. (default 1000000)",
        "N",
    );

    // Logging options.
    opts.optopt(
        "",
        "log-file",
        "where materialized will write logs (default <data directory>/materialized.log)",
        "PATH",
    );

    // Connection options.
    opts.optopt(
        "",
        "listen-addr",
        "the address and port on which materialized will listen for connections",
        "ADDR:PORT",
    );
    opts.optopt(
        "",
        "tls-cert",
        "certificate file for TLS connections",
        "PATH",
    );
    opts.optopt("", "tls-key", "private key for TLS connections", "PATH");

    // Storage options.
    opts.optopt(
        "D",
        "data-directory",
        "where materialized will store metadata (default mzdata)",
        "PATH",
    );
    opts.optopt("", "symbiosis", "(internal use only)", "URL");

    // Feature options.
    opts.optflag(
        "",
        "experimental",
        "enable experimental features (DANGEROUS)",
    );

    // Telemetry options.
    opts.optflag("", "disable-telemetry", "disables telemetry reporting");

    let popts = opts.parse(&args[1..])?;

    // Handle options that request informational output.
    if popts.opt_present("h") {
        print!("{}", opts.usage("usage: materialized [options]"));
        return Ok(());
    } else if popts.opt_present("v") {
        println!(
            "materialized v{} ({})",
            materialized::VERSION,
            materialized::BUILD_SHA
        );
        if popts.opt_count("v") > 1 {
            for bi in build_info() {
                println!("{}", bi);
            }
        }
        return Ok(());
    }

    // Prevent accidental usage of development builds.
    if cfg!(debug_assertions) && !popts.opt_present("dev") && !ore::env::is_var_truthy("MZ_DEV") {
        bail!(
            "refusing to run dev (unoptimized) binary without explicit opt-in\n\
             hint: Pass the '--dev' option or set the MZ_DEV environment variable to opt in.\n\
             hint: Or perhaps you meant to use a release binary?"
        );
    }

    // Configure Timely and Differential workers.
    let threads = match popts.opt_get::<usize>("workers")? {
        Some(val) => val,
        None => match popts.opt_get::<usize>("threads")? {
            Some(val) => {
                warn!("--threads is deprecated and will stop working in the future. Please use --workers.");
                val
            }
            None => match env::var("MZ_WORKERS") {
                Ok(val) => val.parse()?,
                Err(VarError::NotUnicode(_)) => bail!("non-unicode character found in MZ_WORKERS"),
                Err(VarError::NotPresent) => match env::var("MZ_THREADS") {
                    Ok(val) => {
                        warn!("MZ_THREADS is a deprecated alias for MZ_WORKERS");
                        val.parse()?
                    }
                    Err(VarError::NotUnicode(_)) => {
                        bail!("non-unicode character found in MZ_THREADS")
                    }
                    Err(VarError::NotPresent) => cmp::max(1, num_cpus::get_physical() / 2),
                },
            },
        },
    };
    if threads == 0 {
        bail!(
            "'--workers' must be greater than 0\n\
            hint: As a starting point, set the number of threads to half of the number of\n\
            cores on your system. Then, further adjust based on your performance needs.\n\
            hint: You may also set the environment variable MZ_WORKERS to the desired number\n\
            of threads."
        );
    }
    let process = popts.opt_get_default("process", 0)?;
    let processes = popts.opt_get_default("processes", 1)?;
    let address_file = popts.opt_str("address-file");
    if process >= processes {
        bail!("process ID {} is not between 0 and {}", process, processes);
    }
    let addresses = match address_file {
        None => (0..processes)
            .map(|i| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6875 + i as u16))
            .collect(),
        Some(address_file) => read_address_file(&address_file, processes)?,
    };

    // Handle performance tuning parameters.
    let logging_granularity = match popts.opt_str("logging-granularity").as_deref() {
        None => Some(Duration::from_secs(1)),
        Some("off") => None,
        Some(d) => Some(parse_duration::parse(&d)?),
    };
    let log_logging = popts.opt_present("debug-timely-logging");
    let logical_compaction_window = match popts.opt_str("logical-compaction-window").as_deref() {
        None => Some(Duration::from_secs(60)),
        Some("off") => None,
        Some(d) => Some(parse_duration::parse(&d)?),
    };
    let timestamp_frequency = match popts.opt_str("timestamp-frequency").as_deref() {
        None => Duration::from_millis(10),
        Some(d) => parse_duration::parse(&d)?,
    };
    let persistence_max_pending_records =
        popts.opt_get_default("persistence-max-pending-records", 1000000)?;

    // Configure connections.
    let listen_addr = popts.opt_get("listen-addr")?;
    let tls = match (popts.opt_str("tls-cert"), popts.opt_str("tls-key")) {
        (None, None) => None,
        (None, Some(_)) | (Some(_), None) => {
            bail!("--tls-cert and --tls-key must be specified together");
        }
        (Some(cert), Some(key)) => Some(materialized::TlsConfig {
            cert: cert.into(),
            key: key.into(),
        }),
    };

    let experimental_mode = popts.opt_present("experimental");

    // Configure storage.
    let data_directory = popts.opt_get_default("data-directory", PathBuf::from("mzdata"))?;
    let symbiosis_url = popts.opt_str("symbiosis");
    fs::create_dir_all(&data_directory)
        .with_context(|| anyhow!("creating data directory {}", data_directory.display()))?;

    // Configure source persistence.
    let persistence = if experimental_mode {
        let persistence_directory = data_directory.join("persistence/");
        fs::create_dir_all(&persistence_directory).with_context(|| {
            anyhow!(
                "creating persistence directory: {}",
                persistence_directory.display()
            )
        })?;

        Some(coord::PersistenceConfig {
            max_pending_records: persistence_max_pending_records,
            path: persistence_directory,
        })
    } else {
        None
    };

    let logging = logging_granularity.map(|granularity| coord::LoggingConfig {
        granularity,
        log_logging,
    });

    // If --disable-telemetry is present, disable telemetry. Otherwise, if a
    // MZ_TELEMETRY_URL environment variable is set, use that as the telemetry
    // URL. Otherwise (the defaults), enable the production server for release mode
    // and disable telemetry in debug mode. This should allow for good defaults (on
    // in release, off in debug), but also easy development during testing of this
    // feature via the environment variable.
    let telemetry_url = match popts.opt_present("disable-telemetry") {
        true => None,
        false => match env::var("MZ_TELEMETRY_URL") {
            Ok(url) => Some(url),
            Err(VarError::NotUnicode(_)) => {
                bail!("non-unicode character found in MZ_TELEMETRY_URL")
            }
            Err(VarError::NotPresent) => match cfg!(debug_assertions) {
                true => None,
                false => Some("https://telemetry.materialize.com/".to_string()),
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

        let env_filter = EnvFilter::try_from_env("MZ_LOG")
            .or_else(|_| EnvFilter::try_new("info")) // default log level
            .unwrap()
            .add_directive("panic=error".parse().unwrap()); // prevent suppressing logs about panics

        if popts.opt_str("log-file").as_deref() == Some("stderr") {
            // The user explicitly directed logs to stderr. Log only to stderr
            // with the user-specified `env_filter`.
            tracing_subscriber::registry()
                .with(env_filter)
                .with(fmt::layer().with_writer(io::stderr))
                .init();
        } else {
            // The user directed logs to a file. Use the user-specified
            // `env_filter` to control what gets written to the file, but bubble
            // any warnings and errors up to stderr as well.
            tracing_subscriber::registry()
                .with(env_filter)
                .with({
                    let path = match popts.opt_str("log-file") {
                        Some(path) => PathBuf::from(path),
                        None => data_directory.join("materialized.log"),
                    };
                    let file = fs::OpenOptions::new()
                        .append(true)
                        .create(true)
                        .open(path)?;
                    fmt::layer()
                        .with_ansi(false)
                        .with_writer(move || file.try_clone().expect("failed to clone log file"))
                })
                .with(FilterLayer::new(
                    fmt::layer().with_writer(io::stderr),
                    LevelFilter::WARN,
                ))
                .init();
        }
    }

    // Print system information as the very first thing in the logs. The goal is
    // to increase the probability that we can reproduce a reported bug if all
    // we get is the log file.
    let mut system = sysinfo::System::new();
    system.refresh_system();
    info!(
        "booting server
materialized {mz_version}
{dep_versions}
invoked as: {invocation:?}
influential env vars: {env:?}
os: {os}
cpus: {ncpus_logical} logical, {ncpus_physical} physical
cpu0: {cpu0}
memory: {memory_total}KB total, {memory_used}KB used
swap: {swap_total}KB total, {swap_used}KB used",
        mz_version = materialized::version(),
        dep_versions = build_info().join("\n"),
        invocation = args,
        env = {
            // TODO - make this only check for "MZ_" if #1223 is fixed.
            env::vars()
                .filter(|(name, _value)| {
                    name.starts_with("MZ_")
                        || name.starts_with("DIFFERENTIAL_")
                        || name == "DEFAULT_PROGRESS_MODE"
                })
                .map(|(name, value)| format!("{}={}", name, value))
                .collect::<Vec<_>>()
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

    // Start Tokio runtime.
    let mut runtime = tokio::runtime::Builder::new()
        .threaded_scheduler()
        // The default thread name exceeds the Linux limit on thread name
        // length, so pick something shorter.
        //
        // TODO(benesch): use `thread_name_fn` to get unique names if that
        // lands upstream: https://github.com/tokio-rs/tokio/pull/1921.
        .thread_name("tokio:worker")
        .enable_all()
        .build()?;

    let server = runtime.block_on(materialized::serve(materialized::Config {
        threads,
        process,
        addresses,
        logging,
        logical_compaction_window,
        timestamp_frequency,
        persistence,
        listen_addr,
        tls,
        data_directory: Some(data_directory),
        symbiosis_url,
        experimental_mode,
        telemetry_url,
    }))?;

    eprintln!(
        "=======================================================================
Thank you for trying Materialize!

We are interested in any and all feedback you have, which may be able
to improve both our software and your queries! Please reach out at:

    Web: https://materialize.io
    GitHub issues: https://github.com/MaterializeInc/materialize/issues
    Email: support@materialize.io
    Twitter: @MaterializeInc
=======================================================================
"
    );

    if experimental_mode {
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

For more details, see https://materialize.io/docs/cli#experimental-mode
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
"
        );
    }

    println!(
        "materialized {} listening on {}...",
        materialized::version(),
        server.local_addr(),
    );

    // Block forever.
    loop {
        thread::park();
    }
}

fn read_address_file(path: &str, n: usize) -> Result<Vec<SocketAddr>, anyhow::Error> {
    let file = File::open(path).with_context(|| format!("opening address file {}", path))?;
    let mut lines = BufReader::new(file).lines();
    let addrs = lines.by_ref().take(n).collect::<Result<Vec<_>, _>>()?;
    if addrs.len() < n || lines.next().is_some() {
        bail!("address file does not contain exactly {} lines", n);
    }
    Ok(addrs
        .into_iter()
        .map(|addr| match addr.to_socket_addrs() {
            // TODO(benesch): we should try all possible addresses, not just the
            // first (#502).
            Ok(mut addrs) => match addrs.next() {
                Some(addr) => Ok(addr),
                None => Err(anyhow!("{} did not resolve to any addresses", addr)),
            },
            Err(err) => Err(anyhow!("error resolving {}: {}", addr, err)),
        })
        .collect::<Result<Vec<_>, _>>()?)
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

    log::error!(target: "panic", "{}: {}\n{:?}", thr_name, msg, Backtrace::new());
    eprintln!(
        r#"materialized encountered an internal error and crashed.

We rely on bug reports to diagnose and fix these errors. Please
copy and paste the above details and file a report at:

    https://materialize.io/s/bug

To protect your privacy, we do not collect crash reports automatically."#,
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
