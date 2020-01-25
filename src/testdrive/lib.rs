// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::env;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::Path;

use getopts::Options;

use crate::error::ErrCtx;

use self::error::{InputError, ResultExt};
use self::parser::LineReader;

mod action;
mod error;
mod parser;
mod protobuf;

pub use self::action::Config;
pub use self::error::Error;

static TESTDRIVE_DATA_DIR: &str = "testdrive_data";

pub fn run() -> Result<(), Error> {
    let args: Vec<_> = env::args().collect();

    let mut opts = Options::new();
    opts.optopt("", "kafka-addr", "kafka bootstrap address", "HOST:PORT");
    opts.optopt("", "schema-registry-url", "schema registry URL", "URL");
    opts.optopt(
        "",
        "materialized-url",
        "materialized connection string",
        "URL",
    );
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
        kafka_addr: opts.opt_str("kafka-addr"),
        schema_registry_url: opts.opt_str("schema-registry-url"),
        materialized_url: opts.opt_str("materialized-url"),
        data_dir: find_datadir(opts.free.as_ref())?,
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

/// Find a data directory
///
/// Errors only come from permission issues, if the `TESTDRIVE_DATA`
fn find_datadir(command_files: &[impl AsRef<OsStr>]) -> Result<Option<String>, Error> {
    for cmdfile in command_files.iter().map(Path::new) {
        let dir = if !cmdfile.exists() {
            return Err(format!("error: file {:?} does not exist", cmdfile.display()).into());
        } else if cmdfile.is_dir() {
            cmdfile
        } else {
            match cmdfile.parent() {
                Some(parent) => parent,
                None => continue,
            }
        };

        for fname in fs::read_dir(dir).context("finding testdrive files")? {
            let path = fname.context("checking testdrive data dir")?.path();
            if path.file_name() == Some(OsStr::new(TESTDRIVE_DATA_DIR)) {
                return Ok(Some(path.to_string_lossy().to_string()));
            }
        }
    }

    Ok(None)
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
    state.reset_materialized()?;
    let actions = action::build(cmds, &state, config.data_dir.as_deref())?;
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
