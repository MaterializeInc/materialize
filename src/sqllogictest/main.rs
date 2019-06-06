// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::env;
use std::process;

use getopts::Options;
use walkdir::WalkDir;

use sqllogictest::Outcomes;

const USAGE: &str = r#"usage: sqllogictest [PATH...]

Runs one or more sqllogictest files. Directories will be searched
recursively for sqllogictest files."#;

fn main() {
    ore::panic::set_abort_on_panic();

    let args: Vec<_> = env::args().collect();
    let mut opts = Options::new();
    opts.optflagmulti("v", "verbose", "verbosity");
    opts.optflag("h", "help", "show this usage information");
    opts.optflag("", "only-parse", "only attempt to parse queries");
    opts.optopt(
        "",
        "expect-outcomes",
        "specify expected outcomes",
        "OUTCOMES",
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

    let expected_outcomes: Option<Outcomes> = match popts.opt_str("expect-outcomes") {
        Some(outcomes_str) => match outcomes_str.parse() {
            Ok(outcomes) => Some(outcomes),
            Err(err) => {
                eprintln!("{}", err);
                process::exit(1);
            }
        },
        None => None,
    };

    let verbosity = popts.opt_count("v");
    let only_parse = popts.opt_present("only-parse");
    let mut bad_file = false;
    let mut outcomes = Outcomes::default();
    for path in popts.free {
        if path == "-" {
            outcomes += sqllogictest::run_stdin(verbosity, only_parse);
        } else {
            for entry in WalkDir::new(path) {
                match entry {
                    Ok(entry) => {
                        if entry.file_type().is_file() {
                            let local_outcomes = sqllogictest::run_file(entry.path(), verbosity, only_parse);
                            if local_outcomes.any_failed() && verbosity >= 1 {
                                println!("{}", local_outcomes);
                            }
                            outcomes += local_outcomes;
                        } else {
                            continue;
                        }
                    }
                    Err(err) => {
                        eprintln!("{}", err);
                        bad_file = true;
                    }
                }
            }
        }
    }
    if bad_file {
        process::exit(1);
    }

    if verbosity >= 1 {
        println!("{}", outcomes);
    }

    if let Some(expected_outcomes) = expected_outcomes {
        if expected_outcomes != outcomes {
            if verbosity == 0 {
                // With no verbosity, outcomes have not yet been printed.
                println!("{}", outcomes);
            }
            eprintln!("outcomes did not match expectation");
            process::exit(1);
        }
    } else {
        // Without --expect-outcomes, we expect all queries to succeed.
        process::exit(if outcomes.any_failed() { 1 } else { 0 });
    }
}
