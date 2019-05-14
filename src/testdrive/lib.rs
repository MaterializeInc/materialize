// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use getopts::Options;
use std::env;
use std::fs::File;
use std::io;
use std::io::Read;

use self::error::{InputError, ResultExt};
use self::parser::LineReader;

mod action;
mod error;
mod parser;

pub use self::action::Config;
pub use self::error::Error;

pub fn run() -> Result<(), Error> {
    let args: Vec<_> = env::args().collect();

    let mut opts = Options::new();
    opts.optopt("", "kafka", "kafka bootstrap URL", "URL");
    opts.optopt("", "materialized", "materialized connection string", "URL");
    opts.optflag("h", "help", "show this usage information");
    let usage_details = opts.usage("usage: testdrive [options] FILE");
    let opts = opts
        .parse(&args[1..])
        .err_ctx("parsing options".into())
        .map_err(|e| Error::Usage {
            details: format!("{}\n{}\n", usage_details, e),
            requested: false,
        })?;

    if opts.opt_present("h") {
        return Err(Error::Usage {
            details: usage_details,
            requested: true,
        });
    }

    let config = Config {
        kafka_url: opts.opt_str("kafka"),
        materialized_url: opts.opt_str("materialized"),
    };

    if opts.free.is_empty() {
        run_stdin(&config)
    } else {
        for arg in opts.free {
            if arg == "-" {
                run_stdin(&config)?
            } else {
                run_file(&config, &arg)?
            }
        }
        Ok(())
    }
}

pub fn run_file(config: &Config, filename: &str) -> Result<(), Error> {
    let mut file = File::open(&filename).err_ctx(format!("opening {}", filename))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .err_ctx(format!("reading {}", filename))?;
    run_string(config, filename, &contents)
}

pub fn run_stdin(config: &Config) -> Result<(), Error> {
    let mut contents = String::new();
    io::stdin()
        .read_to_string(&mut contents)
        .err_ctx("reading <stdin>".into())?;
    run_string(config, "<stdin>", &contents)
}

pub fn run_string(config: &Config, filename: &str, contents: &str) -> Result<(), Error> {
    println!("==> {}", filename);
    let mut line_reader = LineReader::new(contents);
    // TODO(benesch): when `try` blocks land, use one here.
    run_line_reader(config, &mut line_reader)
        .map_err(|e| e.with_input_details(&filename, &contents, &line_reader))
}

fn run_line_reader(config: &Config, line_reader: &mut LineReader) -> Result<(), Error> {
    let cmds = parser::parse(line_reader)?;
    // TODO(benesch): consider sharing state between files, to avoid
    // reconnections for every file. For now it's nice to not open any
    // connections until after parsing.
    let mut state = action::create_state(config)?;
    let actions = action::build(cmds)?;
    for a in actions.iter().rev() {
        a.action
            .undo(&mut state)
            .map_err(|e| InputError { msg: e, pos: a.pos })?;
    }
    for a in &actions {
        a.action
            .redo(&mut state)
            .map_err(|e| InputError { msg: e, pos: a.pos })?;
    }
    Ok(())
}
