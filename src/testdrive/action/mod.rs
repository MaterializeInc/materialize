// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::time::Duration;

use crate::error::{Error, InputError};
use crate::parser::{Command, PosCommand};
use ore::option::OptionExt;

mod kafka;
mod sql;

#[derive(Default)]
pub struct Config {
    pub kafka_url: Option<String>,
    pub materialized_url: Option<String>,
}

pub struct State {
    pgconn: postgres::Client,
    kafka_url: String,
    kafka_admin: rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext>,
    kafka_admin_opts: rdkafka::admin::AdminOptions,
    kafka_producer: rdkafka::producer::FutureProducer<rdkafka::client::DefaultClientContext>,
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
            Command::FailSql(sql) => Box::new(sql::build_fail_sql(sql).map_err(wrap_err)?),
        };
        out.push(PosAction {
            pos: cmd.pos,
            action,
        })
    }
    Ok(out)
}

pub fn create_state(config: &Config) -> Result<State, Error> {
    let pgconn = {
        let url = config
            .materialized_url
            .as_deref()
            .unwrap_or_else(|| "postgres://ignored@localhost:6875");
        postgres::Client::connect(&url, postgres::NoTls).map_err(|e| Error::General {
            ctx: "opening SQL connection".into(),
            cause: Box::new(e),
            hints: vec![
                format!("connection string: {}", url),
                "are you running the materialized server?".into(),
            ],
        })?
    };

    let (kafka_url, kafka_admin, kafka_admin_opts, kafka_producer) = {
        use rdkafka::admin::{AdminClient, AdminOptions};
        use rdkafka::client::DefaultClientContext;
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::FutureProducer;

        let url = config
            .kafka_url
            .as_deref()
            .unwrap_or_else(|| "localhost:9092");
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &url);

        let admin: AdminClient<DefaultClientContext> =
            config.create().map_err(|e| Error::General {
                ctx: "opening Kafka connection".into(),
                cause: Box::new(e),
                hints: vec![format!("connection string: {}", url)],
            })?;

        let admin_opts = AdminOptions::new().operation_timeout(Duration::from_secs(5));

        let producer: FutureProducer = config.create().map_err(|e| Error::General {
            ctx: "opening Kafka connection".into(),
            cause: Box::new(e),
            hints: vec![format!("connection string: {}", url)],
        })?;

        (url.to_owned(), admin, admin_opts, producer)
    };

    Ok(State {
        pgconn,
        kafka_url,
        kafka_admin,
        kafka_admin_opts,
        kafka_producer,
    })
}
