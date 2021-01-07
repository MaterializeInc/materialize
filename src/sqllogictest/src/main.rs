// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::env;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::process;
use std::sync::Arc;

use chrono::Utc;
use getopts::Options;
use tokio::runtime::Runtime;
use walkdir::WalkDir;

use sqllogictest::runner::{self, Outcomes, RunConfig, WriteFmt};
use sqllogictest::util;

const USAGE: &str = r#"usage: sqllogictest [PATH...]

Runs one or more sqllogictest files. Directories will be searched
recursively for sqllogictest files."#;

fn main() {
    let runtime = Arc::new(Runtime::new().unwrap());
    runtime.block_on(run(runtime.clone()))
}

async fn run(runtime: Arc<Runtime>) {
    ore::panic::set_abort_on_panic();
    ore::test::init_logging_default("warn");

    let args: Vec<_> = env::args().collect();
    let mut opts = Options::new();
    opts.optflagmulti(
        "v",
        "verbose",
        "-v: print every source file. \
         -vv: show each error description. \
         -vvv: show all queries executed",
    );
    opts.optflag("h", "help", "show this usage information");
    opts.optflag(
        "",
        "no-fail",
        "don't exit with a failing code if not all queries successful",
    );
    opts.optflag(
        "",
        "timestamps",
        "prefix every line of output with the current time",
    );
    opts.optflag(
        "",
        "rewrite-results",
        "rewrite expected output based on actual output",
    );
    opts.optopt(
        "",
        "json-summary-file",
        "save JSON-formatted summary to file",
        "FILE",
    );
    opts.optopt(
        "w",
        "workers",
        "number of materialized workers to use (default: 3)",
        "N",
    );

    let popts = match opts.parse(&args[1..]) {
        Ok(popts) => popts,
        Err(err) => {
            eprintln!("{}", err);
            process::exit(1);
        }
    };

    if popts.opt_present("h") || popts.free.is_empty() {
        eprint!("{}", opts.usage(USAGE));
        process::exit(1);
    }

    let config = RunConfig {
        runtime,
        stdout: &OutputStream::new(io::stdout(), popts.opt_present("timestamps")),
        stderr: &OutputStream::new(io::stderr(), popts.opt_present("timestamps")),
        verbosity: popts.opt_count("v"),
        workers: match popts.opt_get_default("workers", 3) {
            Ok(workers) => workers,
            Err(e) => {
                eprintln!("invalid --workers value: {}", e);
                process::exit(1);
            }
        },
    };

    if popts.opt_present("rewrite-results") {
        return rewrite(&config, popts).await;
    }

    let json_summary_file = match popts.opt_str("json-summary-file") {
        Some(filename) => match File::create(&filename) {
            Ok(file) => Some(file),
            Err(err) => {
                writeln!(config.stderr, "creating {}: {}", filename, err);
                process::exit(1);
            }
        },
        None => None,
    };
    let mut bad_file = false;
    let mut outcomes = Outcomes::default();
    for path in &popts.free {
        if path == "-" {
            match sqllogictest::runner::run_stdin(&config).await {
                Ok(o) => outcomes += o,
                Err(err) => {
                    writeln!(config.stderr, "error: parsing stdin: {}", err);
                    bad_file = true;
                }
            }
        } else {
            for entry in WalkDir::new(path) {
                match entry {
                    Ok(entry) if entry.file_type().is_file() => {
                        match runner::run_file(&config, entry.path()).await {
                            Ok(o) => {
                                if o.any_failed() || config.verbosity >= 1 {
                                    writeln!(config.stdout, "{}", util::indent(&o.to_string(), 4));
                                }
                                outcomes += o;
                            }
                            Err(err) => {
                                writeln!(config.stderr, "error: parsing file: {}", err);
                                bad_file = true;
                            }
                        }
                    }
                    Ok(_) => (),
                    Err(err) => {
                        writeln!(config.stderr, "error: reading directory entry: {}", err);
                        bad_file = true;
                    }
                }
            }
        }
    }
    if bad_file {
        process::exit(1);
    }

    writeln!(config.stdout, "{}", outcomes);

    if let Some(json_summary_file) = json_summary_file {
        match serde_json::to_writer(json_summary_file, &outcomes.as_json()) {
            Ok(()) => (),
            Err(err) => {
                writeln!(
                    config.stderr,
                    "error: unable to write summary file: {}",
                    err
                );
                process::exit(2);
            }
        }
    }

    if outcomes.any_failed() && !popts.opt_present("no-fail") {
        process::exit(1);
    }
}

async fn rewrite(config: &RunConfig<'_>, popts: getopts::Matches) {
    if popts.opt_present("json-summary-file") {
        writeln!(
            config.stderr,
            "--rewrite-results is not compatible with --json-summary-file"
        );
        process::exit(1);
    }

    if popts.free.iter().any(|path| path == "-") {
        writeln!(config.stderr, "--rewrite-results cannot be used with stdin");
        process::exit(1);
    }

    let mut bad_file = false;
    for path in popts.free {
        for entry in WalkDir::new(path) {
            match entry {
                Ok(entry) => {
                    if entry.file_type().is_file() {
                        if let Err(err) = runner::rewrite_file(config, entry.path()).await {
                            writeln!(config.stderr, "error: rewriting file: {}", err);
                            bad_file = true;
                        }
                    }
                }
                Err(err) => {
                    writeln!(config.stderr, "error: reading directory entry: {}", err);
                    bad_file = true;
                }
            }
        }
    }
    if bad_file {
        process::exit(1);
    }
}

struct OutputStream<W> {
    inner: RefCell<W>,
    need_timestamp: RefCell<bool>,
    timestamps: bool,
}

impl<W> OutputStream<W>
where
    W: Write,
{
    fn new(inner: W, timestamps: bool) -> OutputStream<W> {
        OutputStream {
            inner: RefCell::new(inner),
            need_timestamp: RefCell::new(true),
            timestamps,
        }
    }

    fn emit_str(&self, s: &str) {
        self.inner.borrow_mut().write_all(s.as_bytes()).unwrap();
    }
}

impl<W> WriteFmt for OutputStream<W>
where
    W: Write,
{
    fn write_fmt(&self, fmt: fmt::Arguments<'_>) {
        let s = format!("{}", fmt);
        if self.timestamps {
            // We need to prefix every line in `s` with the current timestamp.

            let timestamp = Utc::now();

            // If the last character we outputted was a newline, then output a
            // timestamp prefix at the start of this line.
            if self.need_timestamp.replace(false) {
                self.emit_str(&format!("[{}] ", timestamp));
            }

            // Emit `s`, installing a timestamp at the start of every line
            // except the last.
            let (s, last_was_timestamp) = match s.strip_suffix('\n') {
                None => (&*s, false),
                Some(s) => (s, true),
            };
            self.emit_str(&s.replace("\n", &format!("\n[{}] ", timestamp)));

            // If the line ended with a newline, output the newline but *not*
            // the timestamp prefix. We want the timestamp to reflect the moment
            // the *next* character is output. So instead we just remember that
            // the last character we output was a newline.
            if last_was_timestamp {
                *self.need_timestamp.borrow_mut() = true;
                self.emit_str("\n");
            }
        } else {
            self.emit_str(&s)
        }
    }
}
