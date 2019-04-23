// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::env;
use std::fs::File;
use std::io;
use std::io::Read;

use self::error::{InputError, ResultExt};
use self::parser::LineReader;
use ore::vec::VecExt;

mod action;
mod error;
mod parser;

pub use self::error::Error;

pub fn run() -> Result<(), Error> {
    let args: Vec<_> = env::args().collect();
    if args.len() > 2 {
        Err(Error::Usage)
    } else if args.len() == 2 {
        run_file(&args.into_last())
    } else {
        run_stdin()
    }
}

pub fn run_stdin() -> Result<(), Error> {
    let mut contents = String::new();
    io::stdin()
        .read_to_string(&mut contents)
        .err_ctx("reading <stdin>".into())?;
    run_string("<stdin>", &contents)
}

pub fn run_file(filename: &str) -> Result<(), Error> {
    let mut file = File::open(&filename).err_ctx(format!("opening {}", filename))?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .err_ctx(format!("reading {}", filename))?;
    run_string(filename, &contents)
}

pub fn run_string(filename: &str, contents: &str) -> Result<(), Error> {
    let mut line_reader = LineReader::new(contents);
    // TODO(benesch): when `try` blocks land, use one here.
    run_line_reader(&mut line_reader)
        .map_err(|e| e.with_input_details(&filename, &contents, &line_reader))
}

fn run_line_reader(line_reader: &mut LineReader) -> Result<(), Error> {
    let cmds = parser::parse(line_reader)?;
    let actions = action::build(cmds)?;
    let mut state = action::create_state()?;
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
