// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use crate::error::{Error, InputError};
use crate::parser::{Command, PosCommand};

mod kafka;
mod sql;

pub struct State {
    pgconn: postgres::Client,
}

pub struct PosAction {
    pub pos: usize,
    pub action: Box<Action>,
}

pub trait Action {
    fn undo(&self, state: &mut State) -> Result<(), String>;
    fn redo(&self, state: &mut State) -> Result<(), String>;
}

pub fn build(cmds: Vec<PosCommand>) -> Result<Vec<PosAction>, InputError> {
    let mut out = Vec::new();
    for cmd in cmds {
        let pos = cmd.pos;
        let wrap_err = |e| InputError { msg: e, pos };
        let action: Box<Action> = match cmd.command {
            Command::Builtin(builtin) => match builtin.name.as_ref() {
                "kafka-ingest" => Box::new(kafka::build_ingest(builtin).map_err(wrap_err)?),
                _ => {
                    return Err(InputError {
                        msg: format!("unknown built-in command {}", builtin.name),
                        pos: cmd.pos,
                    });
                }
            },
            Command::Sql(sql) => Box::new(sql::build_sql(sql).map_err(wrap_err)?),
        };
        out.push(PosAction {
            pos: cmd.pos,
            action,
        })
    }
    Ok(out)
}

pub fn create_state() -> Result<State, Error> {
    let pgurl = "postgres://ignored@localhost:6875";
    let pgconn = postgres::Client::connect(pgurl, postgres::NoTls).map_err(|e| Error::General {
        ctx: "opening SQL connection".into(),
        cause: Box::new(e),
        hints: vec![
            format!("connection string: {}", pgurl),
            "are you running the materialized server?".into(),
        ],
    })?;

    Ok(State { pgconn })
}
