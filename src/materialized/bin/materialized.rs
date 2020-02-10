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
use std::fs::File;
use std::io::{BufRead, BufReader};
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

fn main() {
    if let Err(err) = run() {
        eprintln!("materialized: {}", err);
        process::exit(1);
    }
}

fn run() -> Result<(), failure::Error> {
    panic::set_hook(Box::new(handle_panic));
    ore::log::init();

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
        "DURATION/\"off\"",
    );
    opts.optopt(
        "",
        "batch-size",
        "maximum number of messages with same timestamp (default 5000) ",
        "SIZE",
    );
    opts.optopt(
        "w",
        "threads",
        "number of per-process worker threads (default 1)",
        "N",
    );
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
    opts.optopt("", "symbiosis", "(internal use only)", "URL");
    opts.optflag("", "no-prometheus", "Do not gather prometheus metrics");

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

    let logging_granularity = match popts
        .opt_str("logging-granularity")
        .as_ref()
        .map(|x| x.as_str())
    {
        None => Some(std::time::Duration::new(1, 0)),
        Some("off") => None,
        Some(d) => Some(parse_duration::parse(&d)?),
    };

    let timestamp_frequency = match popts
        .opt_str("timestamp-frequency")
        .as_ref()
        .map(|x| x.as_str())
    {
        None => Some(parse_duration::parse("10ms")?),
        Some("off") => None,
        Some(d) => Some(parse_duration::parse(&d)?),
    };

    let max_increment_ts_size = popts.opt_get_default("batch-size", 10000_i64)?;
    let threads = popts.opt_get_default("threads", 1)?;
    let process = popts.opt_get_default("process", 0)?;
    let processes = popts.opt_get_default("processes", 1)?;
    let address_file = popts.opt_str("address-file");
    let gather_metrics = !popts.opt_present("no-prometheus");

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
    let start_time = std::time::Instant::now();

    let _server = materialized::serve(materialized::Config {
        logging_granularity,
        timestamp_frequency,
        max_increment_ts_size,
        threads,
        process,
        addresses,
        data_directory: Some(data_directory),
        symbiosis_url: popts.opt_str("symbiosis"),
        gather_metrics,
        start_time,
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

    eprintln!(
        r#"materialized encountered an internal error and crashed.

We rely on bug reports to diagnose and fix these errors. Please
copy and paste the following details and mail them to bugs@materialize.io.
To protect your privacy, we do not collect crash reports automatically.

 thread: {}
message: {}
{:?}
"#,
        thr_name, msg, backtrace
    );

    process::exit(1);
}
