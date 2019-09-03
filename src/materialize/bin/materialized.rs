// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! The main Materialize server.
//!
//! The name is pronounced "materialize-dee." It listens on port 6875 (MTRL).
//!
//! The design and implementation of materialized is very much in flux. See the
//! draft architecture doc for the most up-to-date plan [0]. Access is limited
//! to those with access to the Material Dropbox Paper folder.
//!
//! [0]: https://paper.dropbox.com/doc/Materialize-architecture-plans--AYSu6vvUu7ZDoOEZl7DNi8UQAg-sZj5rhJmISdZSfK0WBxAl

use backtrace::Backtrace;
use failure::{bail, ResultExt};
use lazy_static::lazy_static;
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::panic;
use std::panic::PanicInfo;
use std::process;
use std::sync::Mutex;
use std::thread;

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

    let popts = opts.parse(&args[1..])?;
    if popts.opt_present("h") {
        print!("{}", opts.usage("usage: materialized [options]"));
        return Ok(());
    } else if popts.opt_present("v") {
        println!("materialized v{}", env!("CARGO_PKG_VERSION"));
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
    let threads = popts.opt_get_default("threads", 1)?;
    let process = popts.opt_get_default("process", 0)?;
    let processes = popts.opt_get_default("processes", 1)?;
    let address_file = popts.opt_str("address-file");

    if process >= processes {
        bail!("process ID {} is not between 0 and {}", process, processes);
    }

    let timely_config = if processes > 1 {
        let addresses = match address_file {
            None => (0..processes)
                .map(|i| format!("localhost:{}", 2101 + i))
                .collect(),
            Some(address_file) => read_address_file(&address_file, processes)?,
        };
        timely::Configuration::Cluster {
            threads,
            process,
            addresses,
            report: false,
            log_fn: Box::new(|_| None),
        }
    } else if threads > 1 {
        timely::Configuration::Process(threads)
    } else {
        timely::Configuration::Thread
    };

    materialize::server::serve(materialize::server::Config {
        logging_granularity,
        timely: timely_config,
    })
}

fn read_address_file(path: &str, n: usize) -> Result<Vec<String>, failure::Error> {
    let file =
        File::open(path).with_context(|err| format!("opening address file {}: {}", path, err))?;
    let mut lines = BufReader::new(file).lines();
    let addrs = lines.by_ref().take(n).collect::<Result<Vec<_>, _>>()?;
    if addrs.len() < n || lines.next().is_some() {
        bail!("address file does not contain exactly {} lines", n);
    }
    Ok(addrs)
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
