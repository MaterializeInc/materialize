// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::fs;
use std::net::ToSocketAddrs;
use std::time::Duration;

use lazy_static::lazy_static;
use protobuf::Message;
use rand::Rng;
use regex::{Captures, Regex};

use repr::strconv;

use crate::error::{Error, InputError, ResultExt};
use crate::parser::{Command, PosCommand};

mod file;
mod kafka;
mod sql;

/// User-settable configuration parameters.
#[derive(Debug, Default)]
pub struct Config {
    pub kafka_addr: Option<String>,
    pub schema_registry_url: Option<String>,
    pub materialized_url: Option<String>,
}

pub struct State {
    seed: u32,
    temp_dir: tempfile::TempDir,
    pgclient: postgres::Client,
    schema_registry_url: String,
    ccsr_client: ccsr::Client,
    kafka_addr: String,
    kafka_admin: rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext>,
    kafka_admin_opts: rdkafka::admin::AdminOptions,
    kafka_consumer: rdkafka::consumer::StreamConsumer<rdkafka::consumer::DefaultConsumerContext>,
    kafka_producer: rdkafka::producer::FutureProducer<rdkafka::client::DefaultClientContext>,
}

impl State {
    pub fn reset_materialized(&mut self) -> Result<(), Error> {
        for message in self
            .pgclient
            .simple_query("SHOW DATABASES")
            .err_ctx("resetting materialized state: SHOW DATABASES".into())?
        {
            if let postgres::SimpleQueryMessage::Row(row) = message {
                let name = row.get(0).expect("database name is not nullable");
                let query = format!("DROP DATABASE {}", name);
                sql::print_query(&query);
                self.pgclient.batch_execute(&query).err_ctx(format!(
                    "restting materialized state: DROP DATABASE {}",
                    name,
                ))?;
            }
        }
        self.pgclient
            .batch_execute("CREATE DATABASE materialize")
            .err_ctx("resetting materialized state: CREATE DATABASE materialize".into())?;
        Ok(())
    }
}

pub struct PosAction {
    pub pos: usize,
    pub action: Box<dyn Action>,
}

pub trait Action {
    fn undo(&self, state: &mut State) -> Result<(), String>;
    fn redo(&self, state: &mut State) -> Result<(), String>;
}

pub fn build(cmds: Vec<PosCommand>, state: &State) -> Result<Vec<PosAction>, Error> {
    let mut out = Vec::new();
    let mut vars = HashMap::new();
    vars.insert("testdrive.kafka-addr".into(), state.kafka_addr.clone());
    vars.insert(
        "testdrive.kafka-addr-resolved".into(),
        state
            .kafka_addr
            .to_socket_addrs()
            .ok()
            .and_then(|mut addrs| addrs.next())
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "#RESOLUTION-FAILURE#".into()),
    );
    vars.insert(
        "testdrive.schema-registry-url".into(),
        state.schema_registry_url.clone(),
    );
    vars.insert("testdrive.seed".into(), state.seed.to_string());
    vars.insert(
        "testdrive.temp-dir".into(),
        state.temp_dir.path().display().to_string(),
    );
    {
        let protobuf_descriptors = crate::protobuf::gen::descriptors()
            .write_to_bytes()
            .unwrap();
        vars.insert("testdrive.protobuf-descriptors".into(), {
            let mut out = String::new();
            strconv::format_bytes(&mut out, &protobuf_descriptors);
            out
        });
        vars.insert("testdrive.protobuf-descriptors-file".into(), {
            let path = state.temp_dir.path().join("protobuf-descriptors");
            fs::write(&path, &protobuf_descriptors)
                .err_ctx("writing protobuf descriptors file".into())?;
            path.display().to_string()
        });
    }
    for cmd in cmds {
        let pos = cmd.pos;
        let wrap_err = |e| InputError { msg: e, pos };
        let subst = |msg: &str| substitute_vars(msg, &vars).map_err(wrap_err);
        let action: Box<dyn Action> = match cmd.command {
            Command::Builtin(mut builtin) => {
                for val in builtin.args.values_mut() {
                    *val = subst(val)?;
                }
                for line in &mut builtin.input {
                    *line = subst(line)?;
                }
                match builtin.name.as_ref() {
                    "file-write" => Box::new(file::build_write(builtin).map_err(wrap_err)?),
                    "kafka-ingest" => Box::new(kafka::build_ingest(builtin).map_err(wrap_err)?),
                    "kafka-verify" => Box::new(kafka::build_verify(builtin).map_err(wrap_err)?),
                    "set" => {
                        vars.extend(builtin.args);
                        continue;
                    }
                    _ => {
                        return Err(InputError {
                            msg: format!("unknown built-in command {}", builtin.name),
                            pos: cmd.pos,
                        }
                        .into());
                    }
                }
            }
            Command::Sql(mut sql) => {
                sql.query = subst(&sql.query)?;
                for row in &mut sql.expected_rows {
                    for col in row {
                        *col = subst(col)?;
                    }
                }
                Box::new(sql::build_sql(sql).map_err(wrap_err)?)
            }
            Command::FailSql(mut sql) => {
                sql.query = subst(&sql.query)?;
                sql.expected_error = subst(&sql.expected_error)?;
                Box::new(sql::build_fail_sql(sql).map_err(wrap_err)?)
            }
        };
        out.push(PosAction {
            pos: cmd.pos,
            action,
        })
    }
    Ok(out)
}

/// Substituted `${}`-delimited variables from `vars` into `msg`
fn substitute_vars(msg: &str, vars: &HashMap<String, String>) -> Result<String, String> {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"\$\{([^}]+)\}").unwrap();
    }
    let mut err = None;
    let out = RE.replace_all(msg, |caps: &Captures| {
        let name = &caps[1];
        if let Some(val) = vars.get(name) {
            val
        } else {
            err = Some(format!("unknown variable: {}", name));
            "#VAR-MISSING#"
        }
    });
    match err {
        Some(err) => Err(err),
        None => Ok(out.into_owned()),
    }
}

pub fn create_state(config: &Config) -> Result<State, Error> {
    let seed = rand::thread_rng().gen();
    let temp_dir = tempfile::tempdir().err_ctx("creating temporary directory".into())?;

    let pgclient = {
        let url = config
            .materialized_url
            .as_deref()
            .unwrap_or_else(|| "postgres://ignored@localhost:6875");
        postgres::Client::connect(url, postgres::NoTls).map_err(|e| Error::General {
            ctx: "opening SQL connection".into(),
            cause: Some(Box::new(e)),
            hints: vec![
                format!("connection string: {}", url),
                "are you running the materialized server?".into(),
            ],
        })?
    };

    let schema_registry_url = config
        .schema_registry_url
        .as_deref()
        .unwrap_or_else(|| "http://localhost:8081")
        .to_owned();

    let ccsr_client =
        ccsr::Client::new(schema_registry_url.parse().map_err(|e| Error::General {
            ctx: "opening schema registry connection".into(),
            cause: Some(Box::new(e)),
            hints: vec![
                format!("url: {}", schema_registry_url),
                "are you running the schema registry?".into(),
            ],
        })?);

    let (kafka_addr, kafka_admin, kafka_admin_opts, kafka_consumer, kafka_producer) = {
        use rdkafka::admin::{AdminClient, AdminOptions};
        use rdkafka::client::DefaultClientContext;
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::StreamConsumer;
        use rdkafka::producer::FutureProducer;

        let addr = config
            .kafka_addr
            .as_deref()
            .unwrap_or_else(|| "localhost:9092");
        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &addr);
        config.set("group.id", "materialize-testdrive");

        let admin: AdminClient<DefaultClientContext> =
            config.create().map_err(|e| Error::General {
                ctx: "opening Kafka connection".into(),
                cause: Some(Box::new(e)),
                hints: vec![format!("connection string: {}", addr)],
            })?;

        let admin_opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

        let consumer: StreamConsumer = config.create().map_err(|e| Error::General {
            ctx: "opening Kafka consumer connection".into(),
            cause: Some(Box::new(e)),
            hints: vec![format!("connection string: {}", addr)],
        })?;

        let producer: FutureProducer = config.create().map_err(|e| Error::General {
            ctx: "opening Kafka producer connection".into(),
            cause: Some(Box::new(e)),
            hints: vec![format!("connection string: {}", addr)],
        })?;

        (addr.to_owned(), admin, admin_opts, consumer, producer)
    };

    Ok(State {
        seed,
        temp_dir,
        pgclient,
        schema_registry_url,
        ccsr_client,
        kafka_addr,
        kafka_admin,
        kafka_admin_opts,
        kafka_consumer,
        kafka_producer,
    })
}
