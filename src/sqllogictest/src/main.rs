// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::fs::File;
use std::process;

use chrono::Utc;
use getopts::Options;
use walkdir::WalkDir;

use sqllogictest::runner::{self, Outcomes};
use sqllogictest::util;

const USAGE: &str = r#"usage: sqllogictest [PATH...]

Runs one or more sqllogictest files. Directories will be searched
recursively for sqllogictest files."#;

#[tokio::main]
async fn main() {
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
        "rewrite-results",
        "rewrite expected output based on actual output",
    );
    opts.optopt(
        "",
        "json-summary-file",
        "save JSON-formatted summary to file",
        "FILE",
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

    if popts.opt_present("rewrite-results") {
        return rewrite(popts).await;
    }

    let json_summary_file = match popts.opt_str("json-summary-file") {
        Some(filename) => match File::create(&filename) {
            Ok(file) => Some(file),
            Err(err) => {
                eprintln!("creating {}: {}", filename, err);
                process::exit(1);
            }
        },
        None => None,
    };

    let verbosity = popts.opt_count("v");
    let mut bad_file = false;
    let mut outcomes = Outcomes::default();
    for path in &popts.free {
        if path == "-" {
            match sqllogictest::runner::run_stdin(verbosity).await {
                Ok(o) => outcomes += o,
                Err(err) => {
                    eprintln!("error: parsing stdin: {}", err);
                    bad_file = true;
                }
            }
        } else {
            for entry in WalkDir::new(path) {
                match entry {
                    Ok(entry) if entry.file_type().is_file() => {
                        match runner::run_file(entry.path(), verbosity).await {
                            Ok(o) => {
                                if o.any_failed() || verbosity >= 1 {
                                    println!(
                                        "[{}] {}",
                                        Utc::now(),
                                        util::indent(&o.to_string(), 4)
                                    );
                                }
                                outcomes += o;
                            }
                            Err(err) => {
                                eprintln!("error: parsing file: {}", err);
                                bad_file = true;
                            }
                        }
                    }
                    Ok(_) => (),
                    Err(err) => {
                        eprintln!("error: reading directory entry: {}", err);
                        bad_file = true;
                    }
                }
            }
        }
    }
    if bad_file {
        process::exit(1);
    }

    println!("[{}] {}", Utc::now(), outcomes);

    if let Some(json_summary_file) = json_summary_file {
        match serde_json::to_writer(json_summary_file, &outcomes.as_json()) {
            Ok(()) => (),
            Err(err) => {
                eprintln!("error: unable to write summary file: {}", err);
                process::exit(2);
            }
        }
    }

    if outcomes.any_failed() && !popts.opt_present("no-fail") {
        process::exit(1);
    }
}

async fn rewrite(popts: getopts::Matches) {
    if popts.opt_present("json-summary-file") {
        eprintln!("--rewrite-results is not compatible with --json-summary-file");
        process::exit(1);
    }

    if popts.free.iter().any(|path| path == "-") {
        eprintln!("--rewrite-results cannot be used with stdin");
        process::exit(1);
    }

    let verbosity = popts.opt_count("v");
    let mut bad_file = false;
    for path in popts.free {
        for entry in WalkDir::new(path) {
            match entry {
                Ok(entry) => {
                    if entry.file_type().is_file() {
                        if let Err(err) = runner::rewrite_file(entry.path(), verbosity).await {
                            eprintln!("error: rewriting file: {}", err);
                            bad_file = true;
                        }
                    }
                }
                Err(err) => {
                    eprintln!("error: reading directory entry: {}", err);
                    bad_file = true;
                }
            }
        }
    }
    if bad_file {
        process::exit(1);
    }
}
