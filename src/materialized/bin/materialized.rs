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

use std::env;
use std::env::VarError;
use std::fs::{self, File};
use std::io::{self, BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
use std::panic;
use std::panic::PanicInfo;
use std::path::PathBuf;
use std::process;
use std::sync::Mutex;
use std::thread;

use backtrace::Backtrace;
use failure::{bail, format_err, ResultExt};
use lazy_static::lazy_static;
use log::trace;
use once_cell::sync::OnceCell;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

static LOG_FILE: OnceCell<File> = OnceCell::new();

fn main() {
    if let Err(err) = run() {
        eprintln!("materialized: {}", err);
        process::exit(1);
    }
}

fn run() -> Result<(), failure::Error> {
    panic::set_hook(Box::new(handle_panic));

    let args: Vec<_> = env::args().collect();

    let mut opts = getopts::Options::new();
    opts.optflag("h", "help", "show this usage information");
    opts.optflag("v", "version", "print version and exit");
    opts.optopt(
        "l",
        "logging-granularity",
        "dataflow logging granularity (default 1s)",
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
        "batch-size",
        "maximum number of messages with same timestamp (default 10000) ",
        "SIZE",
    );
    opts.optopt(
        "",
        "logical-compaction-window",
        "historical detail maintained for arrangements (default 60s)",
        "DURATION/\"off\"",
    );
    opts.optopt("w", "threads", "number of per-process worker threads", "N");
    opts.optopt(
        "p",
        "process",
        "identity of this process (default 0)",
        "INDEX",
    );
    opts.optopt(
        "n",
        "processes",
        "total number of processes (default 1)",
        "N",
    );
    opts.optopt(
        "a",
        "address-file",
        "text file whose lines are process addresses",
        "FILE",
    );
    opts.optopt(
        "D",
        "data-directory",
        "where materialized will store metadata (default mzdata)",
        "PATH",
    );
    opts.optopt(
        "",
        "log-file",
        "where materialized will write logs (default <data directory>/materialized.log)",
        "PATH",
    );
    opts.optopt(
        "",
        "listen-addr",
        "the address and port on which materialized will listen for connections",
        "ADDR:PORT",
    );
    opts.optopt("", "symbiosis", "(internal use only)", "URL");
    opts.optflag("", "no-prometheus", "do not gather prometheus metrics");
    if cfg!(debug_assertions) {
        opts.optflag("", "dev", "allow running this dev (unoptimized) build");
    }

    let popts = opts.parse(&args[1..])?;
    if popts.opt_present("h") {
        print!("{}", opts.usage("usage: materialized [options]"));
        return Ok(());
    } else if popts.opt_present("v") {
        println!(
            "materialized v{} ({})",
            materialized::VERSION,
            materialized::BUILD_SHA
        );
        return Ok(());
    }

    let logging_granularity = match popts.opt_str("logging-granularity").as_deref() {
        None => Some(std::time::Duration::new(1, 0)),
        Some("off") => None,
        Some(d) => Some(parse_duration::parse(&d)?),
    };

    let timestamp_frequency = match popts.opt_str("timestamp-frequency").as_deref() {
        None => parse_duration::parse("10ms")?,
        Some(d) => parse_duration::parse(&d)?,
    };

    let log_file = popts.opt_str("log-file");
    let max_increment_ts_size = popts.opt_get_default("batch-size", 10000_i64)?;

    let threads = match popts.opt_get::<usize>("threads")? {
        Some(val) => val,
        None => match env::var("MZ_THREADS") {
            Ok(val) => val.parse()?,
            Err(VarError::NotUnicode(_)) => bail!("non-unicode character found in MZ_THREADS"),
            Err(VarError::NotPresent) => 0,
        },
    };
    if threads == 0 {
        bail!(
            "'--threads' must be specified and greater than 0\n\
             hint: As a starting point, set the number of threads to half of the number of\n\
             cores on your system. Then, further adjust based on your performance needs.\n\
             hint: You may also set the environment variable MZ_THREADS to the desired number\n\
             of threads."
        );
    }

    let process = popts.opt_get_default("process", 0)?;
    let processes = popts.opt_get_default("processes", 1)?;
    let address_file = popts.opt_str("address-file");
    let gather_metrics = !popts.opt_present("no-prometheus");
    let listen_addr = popts.opt_get("listen-addr")?;

    if cfg!(debug_assertions) && !popts.opt_present("dev") && !ore::env::is_var_truthy("MZ_DEV") {
        bail!(
            "refusing to run dev (unoptimized) binary without explicit opt-in\n\
             hint: Pass the '--dev' option or set the MZ_DEV environment variable to opt in.\n\
             hint: Or perhaps you meant to use a release binary?"
        );
    }

    let logical_compaction_window = match popts.opt_str("logical-compaction-window").as_deref() {
        None => Some(parse_duration::parse("60s")?),
        Some("off") => None,
        Some(d) => Some(parse_duration::parse(&d)?),
    };

    if process >= processes {
        bail!("process ID {} is not between 0 and {}", process, processes);
    }

    let addresses = match address_file {
        None => (0..processes)
            .map(|i| SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 6875 + i as u16))
            .collect(),
        Some(address_file) => read_address_file(&address_file, processes)?,
    };

    let data_directory = popts.opt_get_default("data-directory", PathBuf::from("mzdata"))?;
    fs::create_dir_all(&data_directory)
        .with_context(|e| format!("creating data directory: {}", e))?;

    // Configure tracing.
    {
        let filter = EnvFilter::try_from_env("MZ_LOG")
            .or_else(|_| EnvFilter::try_new("info"))
            .unwrap();
        let subscriber = FmtSubscriber::builder().with_env_filter(filter);
        if log_file.as_deref() == Some("stderr") {
            subscriber.with_writer(io::stderr).init();
        } else {
            let path = match log_file {
                Some(path) => PathBuf::from(path),
                None => data_directory.join("materialized.log"),
            };
            let file = fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(path)?;
            // The current design of tracing-subscriber means we need to store
            // this file in a global variable. Stupid, but it works.
            LOG_FILE.set(file).unwrap();
            subscriber
                .with_ansi(false)
                .with_writer(|| LOG_FILE.get().unwrap())
                .init();
        }
    }

    adjust_rlimits();

    // Inform the user about what they are using, and how to contact us.
    beta_splash();

    let _server = materialized::serve(materialized::Config {
        logging_granularity,
        timestamp_frequency,
        max_increment_ts_size,
        logical_compaction_window,
        threads,
        process,
        addresses,
        data_directory: Some(data_directory),
        symbiosis_url: popts.opt_str("symbiosis"),
        gather_metrics,
        listen_addr,
    })?;

    // Block forever.
    loop {
        thread::park();
    }
}

fn read_address_file(path: &str, n: usize) -> Result<Vec<SocketAddr>, failure::Error> {
    let file =
        File::open(path).with_context(|err| format!("opening address file {}: {}", path, err))?;
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
                None => Err(format_err!("{} did not resolve to any addresses", addr)),
            },
            Err(err) => Err(format_err!("error resolving {}: {}", addr, err)),
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

    let backtrace = Backtrace::new();

    let crash_message = format!(
        r#"materialized encountered an internal error and crashed.

We rely on bug reports to diagnose and fix these errors. Please
copy and paste the following details and file a report at:

    https://materialize.io/s/bug

To protect your privacy, we do not collect crash reports automatically.

 thread: {}
message: {}
{:?}
"#,
        thr_name, msg, backtrace
    );

    if LOG_FILE.get().is_some() {
        log::error!("{}", crash_message);
    }
    eprintln!("{}", crash_message);
    process::exit(1);
}

/// Print to the screen information about how to contact us.
fn beta_splash() {
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
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "ios")))]
fn adjust_rlimits() {
    trace!("rlimit crate does not support this OS; not adjusting nofile limit");
}

#[cfg(any(target_os = "macos", target_os = "linux", target_os = "ios"))]
/// Attempts to increase the soft nofile rlimit to the maximum possible value.
fn adjust_rlimits() {
    // getrlimit/setrlimit can have surprisingly different behavior across
    // platforms, even with the rlimit wrapper crate that we use. This function
    // is chattier than normal at the trace log level in an attempt to ease
    // debugging of such differences.

    use log::warn;

    let (soft, hard) = match rlimit::Resource::NOFILE.get() {
        Ok(limits) => limits,
        Err(e) => {
            trace!("unable to read initial nofile rlimit: {}", e);
            return;
        }
    };
    trace!("initial nofile rlimit: ({}, {})", soft, hard);

    #[cfg(target_os = "macos")]
    let hard = {
        use std::cmp;
        use sysctl::Sysctl;

        // On macOS, getrlimit by default reports that the hard limit is
        // unlimited, but there is usually a stricter hard limit discoverable
        // via sysctl. Failing to discover this secret stricter hard limit will
        // cause the call to setrlimit below to fail.
        let res = sysctl::Ctl::new("kern.maxfilesperproc")
            .and_then(|ctl| ctl.value())
            .map_err(|e| e.to_string())
            .and_then(|v| match v {
                sysctl::CtlValue::Int(v) => Ok(v as u64),
                o => Err(format!("unexpected sysctl value type: {:?}", o)),
            });
        match res {
            Ok(v) => {
                trace!("sysctl kern.maxfilesperproc hard limit: {}", v);
                cmp::min(v, hard)
            }
            Err(e) => {
                trace!("error while reading sysctl: {}", e);
                hard
            }
        }
    };

    trace!("attempting to adjust nofile rlimit to ({0}, {0})", hard);
    if let Err(e) = rlimit::Resource::NOFILE.set(hard, hard) {
        trace!("error adjusting nofile rlimit: {}", e);
        return;
    }

    // Check whether getrlimit reflects the limit we installed with setrlimit.
    // Some platforms will silently ignore invalid values in setrlimit.
    let (soft, hard) = match rlimit::Resource::NOFILE.get() {
        Ok(limits) => limits,
        Err(e) => {
            trace!("unable to read adjusted nofile rlimit: {}", e);
            return;
        }
    };
    trace!("adjusted nofile rlimit: ({}, {})", soft, hard);

    const RECOMMENDED_SOFT_LIMIT: u64 = 1024;
    if soft < RECOMMENDED_SOFT_LIMIT {
        warn!(
            "soft nofile rlimit ({}) is dangerously low; at least {} is recommended",
            soft, RECOMMENDED_SOFT_LIMIT
        )
    }
}
