// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A driver for Materialize integration tests.

use std::env;
use std::fs::File;
use std::io;
use std::io::Read;
use std::process;

use self::error::{Error, InputError, ResultExt};
use self::parser::LineReader;
use ore::vec::VecExt;

mod action;
mod error;
mod parser;

fn main() {
    if let Err(err) = run() {
        // If printing the error message fails, there's not a whole lot we can
        // do.
        let _ = err.print_stderr();
        process::exit(1);
    }
}

fn run() -> Result<(), Error> {
    let args: Vec<_> = env::args().collect();
    let mut contents = String::new();
    let filename = if args.len() > 2 {
        return Err(Error::Usage);
    } else if args.len() == 2 {
        let filename = args.into_last();
        let mut file = File::open(&filename).err_ctx(format!("opening {}", filename))?;
        file.read_to_string(&mut contents)
            .err_ctx(format!("reading {}", filename))?;
        filename
    } else {
        io::stdin()
            .read_to_string(&mut contents)
            .err_ctx("reading stdin".into())?;
        "stdin".into()
    };

    // TODO(benesch): when `try` blocks land, use one here.
    let mut line_reader = LineReader::new(&contents);
    run1(&mut line_reader).map_err(|e| e.with_input_details(&filename, &contents, &line_reader))
}

fn run1(line_reader: &mut LineReader) -> Result<(), Error> {
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
