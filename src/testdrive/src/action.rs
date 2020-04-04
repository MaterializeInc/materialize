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
use std::path::PathBuf;
use std::time::Duration;

use lazy_static::lazy_static;
use protobuf::Message;
use rand::Rng;
use regex::{Captures, Regex};
use rusoto_core::{HttpClient, Region};
use rusoto_credential::{ChainProvider, ProvideAwsCredentials, StaticProvider};
use rusoto_kinesis::KinesisClient;
use rusoto_sts::{GetCallerIdentityRequest, Sts, StsClient};

use repr::strconv;

use crate::error::{Error, InputError, ResultExt};
use crate::parser::{Command, PosCommand, SqlExpectedResult};

mod avro_ocf;
mod file;
mod kafka;
mod kinesis;
mod sql;

const DEFAULT_SQL_TIMEOUT: Duration = Duration::from_secs(300);
// Constants to use when hitting Kinesis locally (via localstack)
const LOCALSTACK_ENDPOINT: &str = "http://localhost:4568";
const DUMMY_AWS_ACCOUNT: &str = "000000000000";
const DUMMY_AWS_REGION: &str = "custom-test";
const DUMMY_AWS_ACCESS_KEY: &str = "dummy-access-key";
const DUMMY_AWS_SECRET_ACCESS_KEY: &str = "dummy-secret-access-key";

/// User-settable configuration parameters.
#[derive(Debug, Default)]
pub struct Config {
    pub kafka_addr: Option<String>,
    pub schema_registry_url: Option<String>,
    pub kinesis_region: Option<String>,
    pub materialized_url: Option<String>,
    pub materialized_catalog_path: Option<String>,
}

pub struct State {
    seed: u32,
    temp_dir: tempfile::TempDir,
    data_dir: Option<PathBuf>,
    tokio_runtime: tokio::runtime::Runtime,
    materialized_addr: String,
    pgclient: postgres::Client,
    schema_registry_url: String,
    ccsr_client: ccsr::AsyncClient,
    kafka_addr: String,
    kafka_admin: rdkafka::admin::AdminClient<rdkafka::client::DefaultClientContext>,
    kafka_admin_opts: rdkafka::admin::AdminOptions,
    kafka_producer: rdkafka::producer::FutureProducer<rdkafka::client::DefaultClientContext>,
    kafka_topics: HashMap<String, i32>,
    kinesis_client: KinesisClient,
    aws_region: String,
    aws_account: String,
    aws_access_key: String,
    aws_secret_access_key: String,
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
    let mut sql_timeout = DEFAULT_SQL_TIMEOUT;
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
        let protobuf_descriptors = crate::format::protobuf::gen::descriptors()
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
    vars.insert("testdrive.aws-region".into(), state.aws_region.clone());
    vars.insert("testdrive.aws-account".into(), state.aws_account.clone());
    vars.insert(
        "testdrive.aws-access-key".into(),
        state.aws_access_key.clone(),
    );
    vars.insert(
        "testdrive.aws-secret-access-key".into(),
        state.aws_secret_access_key.clone(),
    );
    vars.insert(
        "testdrive.kinesis-endpoint".into(),
        LOCALSTACK_ENDPOINT.to_string(),
    );
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
                    "avro-ocf-write" => Box::new(avro_ocf::build_write(builtin).map_err(wrap_err)?),
                    "file-write" => Box::new(file::build_write(builtin).map_err(wrap_err)?),
                    "kafka-add-partitions" => {
                        Box::new(kafka::build_add_partitions(builtin).map_err(wrap_err)?)
                    }
                    "kafka-create-topic" => {
                        Box::new(kafka::build_create_topic(builtin).map_err(wrap_err)?)
                    }
                    "kafka-ingest" => Box::new(kafka::build_ingest(builtin).map_err(wrap_err)?),
                    "kafka-verify" => Box::new(kafka::build_verify(builtin).map_err(wrap_err)?),
                    "kinesis-create-stream" => {
                        Box::new(kinesis::build_create_stream(builtin).map_err(wrap_err)?)
                    }
                    "kinesis-ingest" => Box::new(kinesis::build_ingest(builtin).map_err(wrap_err)?),
                    "kinesis-verify" => Box::new(kinesis::build_verify(builtin).map_err(wrap_err)?),
                    "set-sql-timeout" => {
                        let duration = builtin.args.string("duration").map_err(wrap_err)?;
                        if duration.to_lowercase() == "default" {
                            sql_timeout = DEFAULT_SQL_TIMEOUT;
                        } else {
                            sql_timeout = parse_duration::parse(&duration)
                                .map_err(|e| wrap_err(e.to_string()))?;
                        }
                        continue;
                    }
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
                if let SqlExpectedResult::Full { expected_rows, .. } = &mut sql.expected_result {
                    for row in expected_rows {
                        for col in row {
                            *col = subst(col)?;
                        }
                    }
                }
                Box::new(sql::build_sql(sql, sql_timeout).map_err(wrap_err)?)
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

    let data_dir = if let Some(path) = &config.materialized_catalog_path {
        let mut path = PathBuf::from(path);
        if !path.ends_with("catalog") {
            path.push("catalog");
        }
        match fs::metadata(&path) {
            Ok(m) if !m.is_file() => {
                return Err(Error::General {
                    ctx: "materialized catalog path is not a regular file".into(),
                    cause: None,
                    hints: vec![],
                })
            }
            Ok(_) => {
                path.pop();
                Some(path)
            }
            Err(e) => {
                return Err(Error::General {
                    ctx: "opening materialized catalog path".into(),
                    cause: Some(Box::new(e)),
                    hints: vec![format!("is {} accessible to testdrive?", path.display())],
                })
            }
        }
    } else {
        None
    };

    let mut tokio_runtime = tokio::runtime::Runtime::new().map_err(|e| Error::General {
        ctx: "creating Tokio runtime".into(),
        cause: Some(Box::new(e)),
        hints: vec![],
    })?;

    let (materialized_addr, pgclient) = {
        let url = config
            .materialized_url
            .as_deref()
            .unwrap_or_else(|| "postgres://ignored@localhost:6875");
        let pgconfig: postgres::Config = url.parse().map_err(|e| Error::General {
            ctx: "parsing materialized url".into(),
            cause: Some(Box::new(e)),
            hints: vec![],
        })?;
        let pgclient = pgconfig
            .connect(postgres::NoTls)
            .map_err(|e| Error::General {
                ctx: "opening SQL connection".into(),
                cause: Some(Box::new(e)),
                hints: vec![
                    format!("connection string: {}", url),
                    "are you running the materialized server?".into(),
                ],
            })?;
        let materialized_addr = format!(
            "{}:{}",
            match &pgconfig.get_hosts()[0] {
                postgres::config::Host::Tcp(s) => s.to_string(),
                postgres::config::Host::Unix(p) => p.display().to_string(),
            },
            pgconfig.get_ports()[0]
        );
        (materialized_addr, pgclient)
    };

    let schema_registry_url = config
        .schema_registry_url
        .as_deref()
        .unwrap_or_else(|| "http://localhost:8081")
        .to_owned();

    let ccsr_client =
        ccsr::AsyncClient::new(schema_registry_url.parse().map_err(|e| Error::General {
            ctx: "opening schema registry connection".into(),
            cause: Some(Box::new(e)),
            hints: vec![
                format!("url: {}", schema_registry_url),
                "are you running the schema registry?".into(),
            ],
        })?);

    let (kafka_addr, kafka_admin, kafka_admin_opts, kafka_producer, kafka_topics) = {
        use rdkafka::admin::{AdminClient, AdminOptions};
        use rdkafka::client::DefaultClientContext;
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::FutureProducer;

        let addr = config
            .kafka_addr
            .as_deref()
            .unwrap_or_else(|| "localhost:9092");

        let mut config = ClientConfig::new();
        config.set("bootstrap.servers", &addr);

        let admin: AdminClient<DefaultClientContext> =
            config.create().map_err(|e| Error::General {
                ctx: "opening Kafka connection".into(),
                cause: Some(Box::new(e)),
                hints: vec![format!("connection string: {}", addr)],
            })?;

        let admin_opts = AdminOptions::new().operation_timeout(Some(Duration::from_secs(5)));

        let producer: FutureProducer = config.create().map_err(|e| Error::General {
            ctx: "opening Kafka producer connection".into(),
            cause: Some(Box::new(e)),
            hints: vec![format!("connection string: {}", addr)],
        })?;

        let topics = HashMap::new();

        (addr.to_owned(), admin, admin_opts, producer, topics)
    };

    let (kinesis_client, aws_region, aws_account, aws_access_key, aws_secret_access_key) = {
        match config.kinesis_region.clone() {
            Some(region_str) => match region_str.parse::<Region>() {
                Ok(region) => {
                    // If given a real AWS region, hit real AWS!
                    let mut chain_provider = ChainProvider::new();
                    chain_provider.set_timeout(Duration::from_secs(60));
                    let credentials = match tokio_runtime.block_on(chain_provider.credentials()) {
                        Ok(credentials) => {
                            dbg!(&credentials.token());
                            credentials
                        }
                        Err(e) => {
                            return Err(Error::General {
                                ctx: format!(
                                    "hit error trying to get AWS credentials from environment: {}",
                                    e.to_string()
                                ),
                                cause: None,
                                hints: vec![],
                            })
                        }
                    };

                    // Get the AWS account info for the Kinesis stream ARNs
                    let sts_client = StsClient::new(region.clone());
                    let account = match tokio_runtime
                        .block_on(sts_client.get_caller_identity(GetCallerIdentityRequest {}))
                    {
                        Ok(output) => match output.account {
                            Some(account) => account,
                            None => {
                                return Err(Error::General {
                                    ctx: String::from("expected to find AWS account, found none"),
                                    cause: None,
                                    hints: vec![],
                                })
                            }
                        },
                        Err(e) => {
                            return Err(Error::General {
                                ctx: format!(
                                    "hit error trying to get AWS account from environment: {}",
                                    e.to_string()
                                ),
                                cause: None,
                                hints: vec![],
                            })
                        }
                    };

                    let client =
                        KinesisClient::new_with(HttpClient::new().unwrap(), chain_provider, region);
                    (
                        client,
                        region_str,
                        account,
                        credentials.aws_access_key_id().to_string(),
                        credentials.aws_secret_access_key().to_string(),
                    )
                }
                Err(_e) => {
                    // Hit fake AWS!
                    get_kinesis_details_for_localstack()
                }
            },
            None => {
                // Hit fake AWS!
                get_kinesis_details_for_localstack()
            }
        }
    };

    Ok(State {
        seed,
        temp_dir,
        data_dir,
        tokio_runtime,
        materialized_addr,
        pgclient,
        schema_registry_url,
        ccsr_client,
        kafka_addr,
        kafka_admin,
        kafka_admin_opts,
        kafka_producer,
        kafka_topics,
        kinesis_client,
        aws_region,
        aws_account,
        aws_access_key,
        aws_secret_access_key,
    })
}

fn get_kinesis_details_for_localstack() -> (KinesisClient, String, String, String, String) {
    let region = Region::Custom {
        name: DUMMY_AWS_REGION.to_string(), // NB: This must match the region localstack is started up with!!
        endpoint: LOCALSTACK_ENDPOINT.to_string(),
    };
    // Create a new KinesisClient
    let provider = StaticProvider::new(
        DUMMY_AWS_ACCESS_KEY.to_string(),
        DUMMY_AWS_SECRET_ACCESS_KEY.to_string(),
        None,
        None,
    );
    let kinesis_client = KinesisClient::new_with(HttpClient::new().unwrap(), provider, region);
    (
        kinesis_client,
        DUMMY_AWS_REGION.to_string(),
        DUMMY_AWS_ACCOUNT.to_string(),
        DUMMY_AWS_ACCESS_KEY.to_string(),
        DUMMY_AWS_SECRET_ACCESS_KEY.to_string(),
    )
}
