// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::fmt;
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process;

use chrono::Utc;
use time::Instant;
use walkdir::WalkDir;

use mz_ore::cli::{self, CliConfig};
use mz_sqllogictest::runner::{self, Outcomes, RunConfig, WriteFmt};
use mz_sqllogictest::util;

/// Runs sqllogictest scripts to verify database engine correctness.
#[derive(clap::Parser)]
struct Args {
    /// Increase verbosity.
    ///
    /// If specified once, print summary for each source file.
    /// If specified twice, also show descriptions of each error.
    /// If specified thrice, also print each query before it is executed.
    #[clap(short = 'v', long = "verbose", parse(from_occurrences))]
    verbosity: usize,
    /// Don't exit with a failing code if not all queries are successful.
    #[clap(long)]
    no_fail: bool,
    /// Prefix every line of output with the current time.
    #[clap(long)]
    timestamps: bool,
    /// Rewrite expected output based on actual output.
    #[clap(long)]
    rewrite_results: bool,
    /// Generate a JUnit-compatible XML report to the specified file.
    #[clap(long, value_name = "FILE")]
    junit_report: Option<PathBuf>,
    /// Run with N materialized workers.
    #[clap(long, value_name = "N", default_value = "3")]
    workers: usize,
    /// Path to sqllogictest script to run.
    #[clap(value_name = "PATH", required = true)]
    paths: Vec<String>,
    /// Stop on first failure.
    #[clap(long)]
    fail_fast: bool,
}

#[tokio::main]
async fn main() {
    mz_ore::panic::set_abort_on_panic();
    mz_ore::test::init_logging_default("warn");

    let args: Args = cli::parse_args(CliConfig::default());

    let config = RunConfig {
        stdout: &OutputStream::new(io::stdout(), args.timestamps),
        stderr: &OutputStream::new(io::stderr(), args.timestamps),
        verbosity: args.verbosity,
        workers: args.workers,
        no_fail: args.no_fail,
        fail_fast: args.fail_fast,
    };

    if args.rewrite_results {
        return rewrite(&config, args).await;
    }

    let mut junit = match args.junit_report {
        Some(filename) => match File::create(&filename) {
            Ok(file) => Some((file, junit_report::TestSuite::new("sqllogictest"))),
            Err(err) => {
                writeln!(config.stderr, "creating {}: {}", filename.display(), err);
                process::exit(1);
            }
        },
        None => None,
    };
    let mut bad_file = false;
    let mut outcomes = Outcomes::default();
    for path in &args.paths {
        for entry in WalkDir::new(path) {
            match entry {
                Ok(entry) if entry.file_type().is_file() => {
                    let start_time = Instant::now();
                    match runner::run_file(&config, entry.path()).await {
                        Ok(o) => {
                            if o.any_failed() || config.verbosity >= 1 {
                                writeln!(
                                    config.stdout,
                                    "{}",
                                    util::indent(&o.display(config.no_fail).to_string(), 4)
                                );
                            }
                            if let Some((_, junit_suite)) = &mut junit {
                                let mut test_case = if o.any_failed() {
                                    junit_report::TestCase::failure(
                                        &entry.path().to_string_lossy(),
                                        start_time.elapsed(),
                                        "failure",
                                        &o.display(false).to_string(),
                                    )
                                } else {
                                    junit_report::TestCase::success(
                                        &entry.path().to_string_lossy(),
                                        start_time.elapsed(),
                                    )
                                };
                                test_case.set_classname("sqllogictest");
                                junit_suite.add_testcase(test_case);
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
    if bad_file {
        process::exit(1);
    }

    writeln!(config.stdout, "{}", outcomes.display(config.no_fail));

    if let Some((mut junit_file, junit_suite)) = junit {
        let report = junit_report::ReportBuilder::new()
            .add_testsuite(junit_suite)
            .build();
        match report.write_xml(&mut junit_file) {
            Ok(()) => (),
            Err(err) => {
                writeln!(
                    config.stderr,
                    "error: unable to write junit report: {}",
                    err
                );
                process::exit(2);
            }
        }
    }

    if outcomes.any_failed() && !args.no_fail {
        process::exit(1);
    }
}

async fn rewrite(config: &RunConfig<'_>, args: Args) {
    if args.junit_report.is_some() {
        writeln!(
            config.stderr,
            "--rewrite-results is not compatible with --junit-report"
        );
        process::exit(1);
    }

    if args.paths.iter().any(|path| path == "-") {
        writeln!(config.stderr, "--rewrite-results cannot be used with stdin");
        process::exit(1);
    }

    let mut bad_file = false;
    for path in args.paths {
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
            self.emit_str(&s.replace('\n', &format!("\n[{}] ", timestamp)));

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
