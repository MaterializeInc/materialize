// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::future::Future;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use aws_types::SdkConfig;
use futures::future::FutureExt;
use itertools::Itertools;
use mz_adapter::catalog::{Catalog, ConnCatalog};
use mz_adapter::session::Session;
use mz_kafka_util::client::{create_new_client_config_simple, MzClientContext};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_stash::StashFactory;
use once_cell::sync::Lazy;
use rand::Rng;
use rdkafka::producer::Producer;
use rdkafka::ClientConfig;
use regex::{Captures, Regex};
use url::Url;

use mz_ore::display::DisplayExt;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_postgres_util::make_tls;

use crate::error::PosError;
use crate::parser::{validate_ident, Command, PosCommand, SqlExpectedError, SqlOutput};
use crate::util;
use crate::util::postgres::postgres_client;

mod file;
mod http;
mod kafka;
mod mysql;
mod postgres;
mod protobuf;
mod psql;
mod schema_registry;
mod set;
mod skip_if;
mod sleep;
mod sql;
mod sql_server;

/// User-settable configuration parameters.
#[derive(Debug)]
pub struct Config {
    // === Testdrive options. ===
    /// Variables to make available to the testdrive script.
    ///
    /// The value of each entry will be made available to the script in a
    /// variable named `arg.KEY`.
    pub arg_vars: BTreeMap<String, String>,
    /// A random number to distinguish each run of a testdrive script.
    pub seed: Option<u32>,
    /// Whether to reset Materialize state before executing each script and
    /// to clean up AWS state after each script.
    pub reset: bool,
    /// Force the use of the specified temporary directory to use.
    ///
    /// If unspecified, testdrive creates a temporary directory with a random
    /// name.
    pub temp_dir: Option<String>,
    /// The default timeout for cancellable operations.
    pub default_timeout: Duration,
    /// The default number of tries for retriable operations.
    pub default_max_tries: usize,
    /// The initial backoff interval for retry operations.
    ///
    /// Set to 0 to retry immediately on failure.
    pub initial_backoff: Duration,
    /// Backoff factor to use for retry operations.
    ///
    /// Set to 1 to retry at a steady pace.
    pub backoff_factor: f64,

    // === Materialize options. ===
    /// The pgwire connection parameters for the Materialize instance that
    /// testdrive will connect to.
    pub materialize_pgconfig: tokio_postgres::Config,
    /// The internal pgwire connection parameters for the Materialize instance that
    /// testdrive will connect to.
    pub materialize_internal_pgconfig: tokio_postgres::Config,
    /// The port for the public endpoints of the materialize instance that
    /// testdrive will connect to via HTTP.
    pub materialize_http_port: u16,
    /// The port for the internal endpoints of the materialize instance that
    /// testdrive will connect to via HTTP.
    pub materialize_internal_http_port: u16,
    /// Session parameters to set after connecting to materialize.
    pub materialize_params: Vec<(String, String)>,
    /// An optional Postgres connection string to the catalog stash.
    pub materialize_catalog_postgres_stash: Option<String>,

    // === Confluent options. ===
    /// The address of the Kafka broker that testdrive will interact with.
    pub kafka_addr: String,
    /// Default number of partitions to use for topics
    pub kafka_default_partitions: usize,
    /// Arbitrary rdkafka options for testdrive to use when connecting to the
    /// Kafka broker.
    pub kafka_opts: Vec<(String, String)>,
    /// The URL of the schema registry that testdrive will connect to.
    pub schema_registry_url: Url,
    /// An optional path to a TLS certificate that testdrive will present when
    /// performing client authentication.
    ///
    /// The keystore must be in the PKCS#12 format.
    pub cert_path: Option<String>,
    /// An optional password for the TLS certificate.
    pub cert_password: Option<String>,
    /// An optional username for basic authentication with the Confluent Schema
    /// Registry.
    pub ccsr_username: Option<String>,
    /// An optional password for basic authentication with the Confluent Schema
    /// Registry.
    pub ccsr_password: Option<String>,

    // === AWS options. ===
    /// The configuration to use when connecting to AWS.
    pub aws_config: SdkConfig,
    /// The ID of the AWS account that `aws_config` configures.
    pub aws_account: String,
}

pub struct State {
    // === Testdrive state. ===
    arg_vars: BTreeMap<String, String>,
    cmd_vars: BTreeMap<String, String>,
    seed: u32,
    temp_path: PathBuf,
    _tempfile: Option<tempfile::TempDir>,
    default_timeout: Duration,
    timeout: Duration,
    max_tries: usize,
    initial_backoff: Duration,
    backoff_factor: f64,
    regex: Option<Regex>,
    regex_replacement: String,
    postgres_factory: StashFactory,

    // === Materialize state. ===
    materialize_catalog_postgres_stash: Option<String>,
    materialize_sql_addr: String,
    materialize_internal_sql_addr: String,
    materialize_internal_http_addr: String,
    materialize_user: String,
    pgclient: tokio_postgres::Client,

    // === Confluent state. ===
    schema_registry_url: Url,
    ccsr_client: mz_ccsr::Client,
    kafka_addr: String,
    kafka_admin: rdkafka::admin::AdminClient<MzClientContext>,
    kafka_admin_opts: rdkafka::admin::AdminOptions,
    kafka_config: ClientConfig,
    kafka_default_partitions: usize,
    kafka_producer: rdkafka::producer::FutureProducer<MzClientContext>,
    kafka_topics: BTreeMap<String, usize>,

    // === AWS state. ===
    aws_account: String,
    aws_config: SdkConfig,

    // === Database driver state. ===
    mysql_clients: BTreeMap<String, mysql_async::Conn>,
    postgres_clients: BTreeMap<String, tokio_postgres::Client>,
    sql_server_clients:
        BTreeMap<String, tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>>,
}

impl State {
    pub async fn initialize_cmd_vars(&mut self) -> Result<(), anyhow::Error> {
        self.cmd_vars
            .insert("testdrive.kafka-addr".into(), self.kafka_addr.clone());
        self.cmd_vars.insert(
            "testdrive.kafka-addr-resolved".into(),
            self.kafka_addr
                .to_socket_addrs()
                .ok()
                .and_then(|mut addrs| addrs.next())
                .map(|addr| addr.to_string())
                .unwrap_or_else(|| "#RESOLUTION-FAILURE#".into()),
        );
        self.cmd_vars.insert(
            "testdrive.schema-registry-url".into(),
            self.schema_registry_url.to_string(),
        );
        self.cmd_vars
            .insert("testdrive.seed".into(), self.seed.to_string());
        self.cmd_vars.insert(
            "testdrive.temp-dir".into(),
            self.temp_path.display().to_string(),
        );
        self.cmd_vars
            .insert("testdrive.aws-region".into(), self.aws_region().into());
        self.cmd_vars
            .insert("testdrive.aws-endpoint".into(), self.aws_endpoint().into());
        self.cmd_vars
            .insert("testdrive.aws-account".into(), self.aws_account.clone());
        {
            let aws_credentials = self
                .aws_config
                .credentials_provider()
                .ok_or_else(|| anyhow!("no AWS credentials provider configured"))?
                .provide_credentials()
                .await
                .context("fetching AWS credentials")?;
            self.cmd_vars.insert(
                "testdrive.aws-access-key-id".into(),
                aws_credentials.access_key_id().to_owned(),
            );
            self.cmd_vars.insert(
                "testdrive.aws-secret-access-key".into(),
                aws_credentials.secret_access_key().to_owned(),
            );
            self.cmd_vars.insert(
                "testdrive.aws-token".into(),
                aws_credentials
                    .session_token()
                    .map(|token| token.to_owned())
                    .unwrap_or_else(String::new),
            );
        }
        self.cmd_vars.insert(
            "testdrive.materialize-sql-addr".into(),
            self.materialize_sql_addr.clone(),
        );
        self.cmd_vars.insert(
            "testdrive.materialize-internal-sql-addr".into(),
            self.materialize_internal_sql_addr.clone(),
        );
        self.cmd_vars.insert(
            "testdrive.materialize-user".into(),
            self.materialize_user.clone(),
        );

        for (key, value) in env::vars() {
            self.cmd_vars.insert(format!("env.{}", key), value);
        }

        for (key, value) in &self.arg_vars {
            validate_ident(key)?;
            self.cmd_vars
                .insert(format!("arg.{}", key), value.to_string());
        }

        Ok(())
    }
    /// Makes of copy of the stash's catalog and runs a function on its
    /// state. Returns `None` if there's no catalog information in the State.
    pub async fn with_catalog_copy<F, T>(&self, f: F) -> Result<Option<T>, anyhow::Error>
    where
        F: FnOnce(ConnCatalog) -> T,
    {
        if let Some(url) = &self.materialize_catalog_postgres_stash {
            let tls = mz_postgres_util::make_tls(&tokio_postgres::Config::new()).unwrap();
            let stash = self
                .postgres_factory
                .open_readonly(url.clone(), None, tls)
                .await?;
            let catalog = Catalog::open_debug_stash(stash, SYSTEM_TIME.clone()).await?;
            let res = f(catalog.for_session(&Session::dummy()));
            Ok(Some(res))
        } else {
            Ok(None)
        }
    }

    pub fn aws_endpoint(&self) -> &str {
        self.aws_config.endpoint_url().unwrap_or("")
    }

    pub fn aws_region(&self) -> &str {
        self.aws_config.region().map(|r| r.as_ref()).unwrap_or("")
    }

    pub async fn reset_materialize(&mut self) -> Result<(), anyhow::Error> {
        let (inner_client, _) = postgres_client(&format!(
            "postgres://mz_system:materialize@{}",
            self.materialize_internal_sql_addr
        ))
        .await?;
        inner_client
            .batch_execute("ALTER SYSTEM RESET ALL")
            .await
            .context("resetting materialize state: ALTER SYSTEM RESET ALL")?;

        for row in inner_client
            .query("SHOW DATABASES", &[])
            .await
            .context("resetting materialize state: SHOW DATABASES")?
        {
            let db_name: String = row.get(0);
            let query = format!("DROP DATABASE {}", db_name);
            sql::print_query(&query, None);
            inner_client.batch_execute(&query).await.context(format!(
                "resetting materialize state: DROP DATABASE {}",
                db_name,
            ))?;
        }
        inner_client
            .batch_execute("CREATE DATABASE materialize")
            .await
            .context("resetting materialize state: CREATE DATABASE materialize")?;

        // Attempt to remove all users but the current user. Old versions of
        // Materialize did not support roles, so this degrades gracefully if
        // mz_roles does not exist.
        if let Ok(rows) = inner_client.query("SELECT name FROM mz_roles", &[]).await {
            for row in rows {
                let role_name: String = row.get(0);
                if role_name == self.materialize_user || role_name.starts_with("mz_") {
                    continue;
                }
                let query = format!("DROP ROLE {}", role_name);
                sql::print_query(&query, None);
                inner_client.batch_execute(&query).await.context(format!(
                    "resetting materialize state: DROP ROLE {}",
                    role_name,
                ))?;
            }
        }

        Ok(())
    }

    /// Delete Kafka topics + CCSR subjects that were created in this run
    pub async fn reset_kafka(&mut self) -> Result<(), anyhow::Error> {
        let mut errors: Vec<anyhow::Error> = Vec::new();

        let metadata = self.kafka_producer.client().fetch_metadata(
            None,
            Some(std::cmp::max(Duration::from_secs(1), self.default_timeout)),
        )?;

        let testdrive_topics: Vec<_> = metadata
            .topics()
            .iter()
            .filter_map(|t| {
                if t.name().starts_with("testdrive-") {
                    Some(t.name())
                } else {
                    None
                }
            })
            .collect();

        if !testdrive_topics.is_empty() {
            match self
                .kafka_admin
                .delete_topics(&testdrive_topics, &self.kafka_admin_opts)
                .await
            {
                Ok(res) => {
                    if res.len() != testdrive_topics.len() {
                        errors.push(anyhow!(
                            "kafka topic deletion returned {} results, but exactly {} expected",
                            res.len(),
                            testdrive_topics.len()
                        ));
                    }
                    for (res, topic) in res.iter().zip(testdrive_topics.iter()) {
                        match res {
                            Ok(_)
                            | Err((_, rdkafka::types::RDKafkaErrorCode::UnknownTopicOrPartition)) => {
                                ()
                            }
                            Err((_, err)) => {
                                errors.push(anyhow!("unable to delete {}: {}", topic, err));
                            }
                        }
                    }
                }
                Err(e) => {
                    errors.push(e.into());
                }
            };
        }

        match self
            .ccsr_client
            .list_subjects()
            .await
            .context("listing schema registry subjects")
        {
            Ok(subjects) => {
                let testdrive_subjects: Vec<_> = subjects
                    .iter()
                    .filter(|s| s.starts_with("testdrive-"))
                    .collect();

                for subject in testdrive_subjects {
                    match self.ccsr_client.delete_subject(subject).await {
                        Ok(()) | Err(mz_ccsr::DeleteError::SubjectNotFound) => (),
                        Err(e) => errors.push(e.into()),
                    }
                }
            }
            Err(e) => {
                errors.push(e);
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            bail!(
                "deleting Kafka topics: {} errors: {}",
                errors.len(),
                errors.into_iter().map(|e| e.to_string_alt()).join("\n")
            );
        }
    }
}

pub enum ControlFlow {
    Continue,
    Break,
}

#[async_trait]
pub(crate) trait Run {
    async fn run(self, state: &mut State) -> Result<ControlFlow, PosError>;
}

#[async_trait]
impl Run for PosCommand {
    async fn run(self, state: &mut State) -> Result<ControlFlow, PosError> {
        let wrap_err = |e| PosError::new(e, self.pos);
        //         Substitute variables at startup except for the command-specific ones
        // Those will be substituted at runtime
        let ignore_prefix = match &self.command {
            Command::Builtin(builtin) => Some(builtin.name.clone()),
            _ => None,
        };
        let subst = |msg: &str, vars: &BTreeMap<String, String>| {
            substitute_vars(msg, vars, &ignore_prefix, false).map_err(wrap_err)
        };
        let subst_re = |msg: &str, vars: &BTreeMap<String, String>| {
            substitute_vars(msg, vars, &ignore_prefix, true).map_err(wrap_err)
        };

        let r = match self.command {
            Command::Builtin(mut builtin) => {
                for val in builtin.args.values_mut() {
                    *val = subst(val, &state.cmd_vars)?;
                }
                for line in &mut builtin.input {
                    *line = subst(line, &state.cmd_vars)?;
                }
                match builtin.name.as_ref() {
                    "file-append" => file::run_append(builtin, state).await,
                    "file-delete" => file::run_delete(builtin, state).await,
                    "http-request" => http::run_request(builtin, state).await,
                    "kafka-add-partitions" => kafka::run_add_partitions(builtin, state).await,
                    "kafka-create-topic" => kafka::run_create_topic(builtin, state).await,
                    "kafka-ingest" => kafka::run_ingest(builtin, state).await,
                    "kafka-verify-data" => kafka::run_verify_data(builtin, state).await,
                    "kafka-verify-commit" => kafka::run_verify_commit(builtin, state).await,
                    "mysql-connect" => mysql::run_connect(builtin, state).await,
                    "mysql-execute" => mysql::run_execute(builtin, state).await,
                    "postgres-connect" => postgres::run_connect(builtin, state).await,
                    "postgres-execute" => postgres::run_execute(builtin, state).await,
                    "postgres-verify-slot" => postgres::run_verify_slot(builtin, state).await,
                    "protobuf-compile-descriptors" => {
                        protobuf::run_compile_descriptors(builtin, state).await
                    }
                    "psql-execute" => psql::run_execute(builtin, state).await,
                    "schema-registry-publish" => schema_registry::run_publish(builtin, state).await,
                    "schema-registry-verify" => schema_registry::run_verify(builtin, state).await,
                    "schema-registry-wait" => schema_registry::run_wait(builtin, state).await,
                    "skip-if" => skip_if::run_skip_if(builtin, state).await,
                    "sql-server-connect" => sql_server::run_connect(builtin, state).await,
                    "sql-server-execute" => sql_server::run_execute(builtin, state).await,
                    "random-sleep" => sleep::run_random_sleep(builtin),
                    "set-regex" => set::run_regex_set(builtin, state),
                    "unset-regex" => set::run_regex_unset(builtin, state),
                    "set-sql-timeout" => set::run_sql_timeout(builtin, state),
                    "set-max-tries" => set::run_max_tries(builtin, state),
                    "sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment" => {
                        sleep::run_sleep(builtin)
                    }
                    "set" => set::set_vars(builtin, state),
                    "set-from-sql" => set::run_set_from_sql(builtin, state).await,
                    // "verify-timestamp-compaction" => Box::new(
                    //     verify_timestamp_compaction::run_verify_timestamp_compaction_action(
                    //         builtin,
                    //     )
                    //     .await,
                    // ),
                    _ => {
                        return Err(PosError::new(
                            anyhow!("unknown built-in command {}", builtin.name),
                            self.pos,
                        ));
                    }
                }
            }
            Command::Sql(mut sql) => {
                sql.query = subst(&sql.query, &state.cmd_vars)?;
                if let SqlOutput::Full { expected_rows, .. } = &mut sql.expected_output {
                    for row in expected_rows {
                        for col in row {
                            *col = subst(col, &state.cmd_vars)?;
                        }
                    }
                }
                sql::run_sql(sql, state).await
            }
            Command::FailSql(mut sql) => {
                sql.query = subst(&sql.query, &state.cmd_vars)?;
                sql.expected_error = match &sql.expected_error {
                    SqlExpectedError::Contains(s) => {
                        SqlExpectedError::Contains(subst(s, &state.cmd_vars)?)
                    }
                    SqlExpectedError::Exact(s) => {
                        SqlExpectedError::Exact(subst(s, &state.cmd_vars)?)
                    }
                    SqlExpectedError::Regex(s) => {
                        SqlExpectedError::Regex(subst_re(s, &state.cmd_vars)?)
                    }
                    SqlExpectedError::Timeout => SqlExpectedError::Timeout,
                };
                sql::run_fail_sql(sql, state).await
            }
        };

        r.map_err(wrap_err)
    }
}

/// Substituted `${}`-delimited variables from `vars` into `msg`
fn substitute_vars(
    msg: &str,
    vars: &BTreeMap<String, String>,
    ignore_prefix: &Option<String>,
    regex_escape: bool,
) -> Result<String, anyhow::Error> {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\$\{([^}]+)\}").unwrap());
    let mut err = None;
    let out = RE.replace_all(msg, |caps: &Captures| {
        let name = &caps[1];
        if let Some(ignore_prefix) = &ignore_prefix {
            if name.starts_with(format!("{}.", ignore_prefix).as_str()) {
                // Do not subsitute, leave original variable name in place
                return caps.get(0).unwrap().as_str().to_string();
            }
        }

        if let Some(val) = vars.get(name) {
            if regex_escape {
                regex::escape(val)
            } else {
                val.to_string()
            }
        } else {
            err = Some(anyhow!("unknown variable: {}", name));
            "#VAR-MISSING#".to_string()
        }
    });
    match err {
        Some(err) => Err(err),
        None => Ok(out.into_owned()),
    }
}

/// Initializes a [`State`] object by connecting to the various external
/// services specified in `config`.
///
/// Returns the initialized `State` and a cleanup future. The cleanup future
/// should be `await`ed only *after* dropping the `State` to check whether any
/// errors occured while dropping the `State`. This awkward API is a workaround
/// for the lack of `AsyncDrop` support in Rust.
pub async fn create_state(
    config: &Config,
) -> Result<(State, impl Future<Output = Result<(), anyhow::Error>>), anyhow::Error> {
    let seed = config.seed.unwrap_or_else(|| rand::thread_rng().gen());

    let (_tempfile, temp_path) = match &config.temp_dir {
        Some(temp_dir) => {
            fs::create_dir_all(temp_dir).context("creating temporary directory")?;
            (None, PathBuf::from(&temp_dir))
        }
        _ => {
            // Stash the tempfile object so that it does not go out of scope and delete
            // the tempdir prematurely
            let tempfile_handle = tempfile::tempdir().context("creating temporary directory")?;
            let temp_path = tempfile_handle.path().to_path_buf();
            (Some(tempfile_handle), temp_path)
        }
    };

    let materialize_catalog_postgres_stash = config.materialize_catalog_postgres_stash.clone();

    let (
        materialize_sql_addr,
        materialize_internal_sql_addr,
        materialize_internal_http_addr,
        materialize_user,
        pgclient,
        pgconn_task,
    ) = {
        let materialize_url = util::postgres::config_url(&config.materialize_pgconfig)?;
        let materialize_internal_url =
            util::postgres::config_url(&config.materialize_internal_pgconfig)?;

        let (pgclient, pgconn) = Retry::default()
            .max_duration(config.default_timeout)
            .retry_async_canceling(|_| async move {
                let mut pgconfig = config.materialize_pgconfig.clone();
                pgconfig.connect_timeout(config.default_timeout);
                let tls = make_tls(&pgconfig)?;
                pgconfig.connect(tls).await.map_err(|e| anyhow!(e))
            })
            .await?;
        let pgconn_task = task::spawn(|| "pgconn_task", pgconn).map(|join| {
            join.expect("pgconn_task unexpectedly canceled")
                .context("running SQL connection")
        });
        for (key, value) in &config.materialize_params {
            pgclient
                .batch_execute(&format!("SET {key} = {value}"))
                .await
                .context("setting session parameter")?;
        }

        // Old versions of Materialize did not support `current_user`, so we
        // fail gracefully.
        let materialize_user = match pgclient.query_one("SELECT current_user", &[]).await {
            Ok(row) => row.get(0),
            Err(_) => "<unknown user>".to_owned(),
        };

        let materialize_sql_addr = format!(
            "{}:{}",
            materialize_url.host_str().unwrap(),
            materialize_url.port().unwrap()
        );
        let materialize_internal_sql_addr = format!(
            "{}:{}",
            materialize_internal_url.host_str().unwrap(),
            materialize_internal_url.port().unwrap()
        );
        let materialize_internal_http_addr = format!(
            "{}:{}",
            materialize_internal_url.host_str().unwrap(),
            config.materialize_internal_http_port
        );
        (
            materialize_sql_addr,
            materialize_internal_sql_addr,
            materialize_internal_http_addr,
            materialize_user,
            pgclient,
            pgconn_task,
        )
    };

    let schema_registry_url = config.schema_registry_url.to_owned();

    let ccsr_client = {
        let mut ccsr_config = mz_ccsr::ClientConfig::new(schema_registry_url.clone());

        if let Some(cert_path) = &config.cert_path {
            let cert = fs::read(cert_path).context("reading cert")?;
            let pass = config.cert_password.as_deref().unwrap_or("").to_owned();
            let ident = mz_ccsr::tls::Identity::from_pkcs12_der(cert, pass)
                .context("reading keystore file as pkcs12")?;
            ccsr_config = ccsr_config.identity(ident);
        }

        if let Some(ccsr_username) = &config.ccsr_username {
            ccsr_config = ccsr_config.auth(ccsr_username.clone(), config.ccsr_password.clone());
        }

        ccsr_config.build().context("Creating CCSR client")?
    };

    let (kafka_addr, kafka_admin, kafka_admin_opts, kafka_producer, kafka_topics, kafka_config) = {
        use rdkafka::admin::{AdminClient, AdminOptions};
        use rdkafka::producer::FutureProducer;

        let mut kafka_config = create_new_client_config_simple();
        kafka_config.set("bootstrap.servers", &config.kafka_addr);
        kafka_config.set("group.id", "materialize-testdrive");
        kafka_config.set("auto.offset.reset", "earliest");
        kafka_config.set("isolation.level", "read_committed");
        if let Some(cert_path) = &config.cert_path {
            kafka_config.set("security.protocol", "ssl");
            kafka_config.set("ssl.keystore.location", cert_path);
            if let Some(cert_password) = &config.cert_password {
                kafka_config.set("ssl.keystore.password", cert_password);
            }
        }
        for (key, value) in &config.kafka_opts {
            kafka_config.set(key, value);
        }

        let admin: AdminClient<_> = kafka_config
            .create_with_context(MzClientContext)
            .with_context(|| format!("opening Kafka connection: {}", config.kafka_addr))?;

        let admin_opts = AdminOptions::new().operation_timeout(Some(config.default_timeout));

        kafka_config.set("message.max.bytes", "15728640");
        let producer: FutureProducer<_> = kafka_config
            .create_with_context(MzClientContext)
            .with_context(|| format!("opening Kafka producer connection: {}", config.kafka_addr))?;

        let topics = BTreeMap::new();

        (
            config.kafka_addr.to_owned(),
            admin,
            admin_opts,
            producer,
            topics,
            kafka_config,
        )
    };

    let mut state = State {
        // === Testdrive state. ===
        arg_vars: config.arg_vars.clone(),
        cmd_vars: BTreeMap::new(),
        seed,
        temp_path,
        _tempfile,
        default_timeout: config.default_timeout,
        timeout: config.default_timeout,
        max_tries: config.default_max_tries,
        initial_backoff: config.initial_backoff,
        backoff_factor: config.backoff_factor,
        regex: None,
        regex_replacement: set::DEFAULT_REGEX_REPLACEMENT.into(),
        postgres_factory: StashFactory::new(&MetricsRegistry::new()),

        // === Materialize state. ===
        materialize_catalog_postgres_stash,
        materialize_sql_addr,
        materialize_internal_sql_addr,
        materialize_internal_http_addr,
        materialize_user,
        pgclient,

        // === Confluent state. ===
        schema_registry_url,
        ccsr_client,
        kafka_addr,
        kafka_admin,
        kafka_admin_opts,
        kafka_config,
        kafka_default_partitions: config.kafka_default_partitions,
        kafka_producer,
        kafka_topics,

        // === AWS state. ===
        aws_account: config.aws_account.clone(),
        aws_config: config.aws_config.clone(),

        // === Database driver state. ===
        mysql_clients: BTreeMap::new(),
        postgres_clients: BTreeMap::new(),
        sql_server_clients: BTreeMap::new(),
    };
    state.initialize_cmd_vars().await?;
    Ok((state, pgconn_task))
}
