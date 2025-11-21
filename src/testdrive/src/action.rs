// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::ToSocketAddrs;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::time::Duration;
use std::{env, fs};

use anyhow::{Context, anyhow, bail};
use async_trait::async_trait;
use aws_credential_types::provider::ProvideCredentials;
use aws_types::SdkConfig;
use futures::future::FutureExt;
use itertools::Itertools;
use mz_adapter::catalog::{Catalog, ConnCatalog};
use mz_adapter::session::Session;
use mz_build_info::BuildInfo;
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_catalog::durable::BootstrapArgs;
use mz_kafka_util::client::{MzClientContext, create_new_client_config_simple};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_persist_client::{PersistClient, PersistLocation};
use mz_sql::catalog::EnvironmentId;
use mz_tls_util::make_tls;
use rand::Rng;
use rdkafka::ClientConfig;
use rdkafka::producer::Producer;
use regex::{Captures, Regex};
use semver::Version;
use tokio_postgres::error::{DbError, SqlState};
use tracing::info;
use url::Url;

use crate::error::PosError;
use crate::parser::{
    Command, PosCommand, SqlExpectedError, SqlOutput, VersionConstraint, validate_ident,
};
use crate::util;
use crate::util::postgres::postgres_client;

pub mod consistency;

mod file;
mod fivetran;
mod http;
mod kafka;
mod mysql;
mod nop;
mod persist;
mod postgres;
mod protobuf;
mod psql;
mod s3;
mod schema_registry;
mod set;
mod skip_end;
mod skip_if;
mod sleep;
mod sql;
mod sql_server;
mod version_check;
mod webhook;

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
    /// Source string to print out on errors.
    pub source: Option<String>,
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
    /// Should we skip coordinator and catalog consistency checks.
    pub consistency_checks: consistency::Level,
    /// Whether to automatically rewrite wrong results instead of failing.
    pub rewrite_results: bool,

    // === Materialize options. ===
    /// The pgwire connection parameters for the Materialize instance that
    /// testdrive will connect to.
    pub materialize_pgconfig: tokio_postgres::Config,
    /// The internal pgwire connection parameters for the Materialize instance that
    /// testdrive will connect to.
    pub materialize_internal_pgconfig: tokio_postgres::Config,
    /// Whether to use HTTPS instead of plain HTTP for the HTTP(S) connections.
    pub materialize_use_https: bool,
    /// The port for the public endpoints of the materialize instance that
    /// testdrive will connect to via HTTP.
    pub materialize_http_port: u16,
    /// The port for the internal endpoints of the materialize instance that
    /// testdrive will connect to via HTTP.
    pub materialize_internal_http_port: u16,
    /// The port for the password endpoints of the materialize instance that
    /// testdrive will connect to via SQL.
    pub materialize_password_sql_port: u16,
    /// The port for the SASL endpoints of the materialize instance that
    /// testdrive will connect to via SQL.
    pub materialize_sasl_sql_port: u16,
    /// Session parameters to set after connecting to materialize.
    pub materialize_params: Vec<(String, String)>,
    /// An optional catalog configuration.
    pub materialize_catalog_config: Option<CatalogConfig>,
    /// Build information
    pub build_info: &'static BuildInfo,
    /// Configured cluster replica sizes
    pub materialize_cluster_replica_sizes: ClusterReplicaSizeMap,

    // === Persist options. ===
    /// Handle to the persist consensus system.
    pub persist_consensus_url: Option<SensitiveUrl>,
    /// Handle to the persist blob storage.
    pub persist_blob_url: Option<SensitiveUrl>,

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

    // === Fivetran options. ===
    /// Address of the Fivetran Destination that is currently running.
    pub fivetran_destination_url: String,
    /// Directory that is accessible to the Fivetran Destination.
    pub fivetran_destination_files_path: String,
}

pub struct MaterializeState {
    catalog_config: Option<CatalogConfig>,

    sql_addr: String,
    use_https: bool,
    http_addr: String,
    internal_sql_addr: String,
    internal_http_addr: String,
    password_sql_addr: String,
    sasl_sql_addr: String,
    user: String,
    pgclient: tokio_postgres::Client,
    environment_id: EnvironmentId,
    bootstrap_args: BootstrapArgs,
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
    consistency_checks: consistency::Level,
    consistency_checks_adhoc_skip: bool,
    regex: Option<Regex>,
    regex_replacement: String,
    error_line_count: usize,
    error_string: String,

    // === Materialize state. ===
    materialize: MaterializeState,

    // === Persist state. ===
    persist_consensus_url: Option<SensitiveUrl>,
    persist_blob_url: Option<SensitiveUrl>,
    build_info: &'static BuildInfo,
    persist_clients: PersistClientCache,

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
    sql_server_clients: BTreeMap<String, mz_sql_server_util::Client>,

    // === Fivetran state. ===
    fivetran_destination_url: String,
    fivetran_destination_files_path: String,

    // === Rewrite state. ===
    rewrite_results: bool,
    /// Current file, results are replaced inline
    pub rewrites: Vec<Rewrite>,
    /// Start position of currently expected result
    pub rewrite_pos_start: usize,
    /// End position of currently expected result
    pub rewrite_pos_end: usize,
}

pub struct Rewrite {
    pub content: String,
    pub start: usize,
    pub end: usize,
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
            "testdrive.materialize-environment-id".into(),
            self.materialize.environment_id.to_string(),
        );
        self.cmd_vars.insert(
            "testdrive.materialize-sql-addr".into(),
            self.materialize.sql_addr.clone(),
        );
        self.cmd_vars.insert(
            "testdrive.materialize-internal-sql-addr".into(),
            self.materialize.internal_sql_addr.clone(),
        );
        self.cmd_vars.insert(
            "testdrive.materialize-password-sql-addr".into(),
            self.materialize.password_sql_addr.clone(),
        );
        self.cmd_vars.insert(
            "testdrive.materialize-sasl-sql-addr".into(),
            self.materialize.sasl_sql_addr.clone(),
        );
        self.cmd_vars.insert(
            "testdrive.materialize-user".into(),
            self.materialize.user.clone(),
        );
        self.cmd_vars.insert(
            "testdrive.fivetran-destination-url".into(),
            self.fivetran_destination_url.clone(),
        );
        self.cmd_vars.insert(
            "testdrive.fivetran-destination-files-path".into(),
            self.fivetran_destination_files_path.clone(),
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
    /// Makes of copy of the durable catalog and runs a function on its
    /// state. Returns `None` if there's no catalog information in the State.
    pub async fn with_catalog_copy<F, T>(
        &self,
        system_parameter_defaults: BTreeMap<String, String>,
        build_info: &'static BuildInfo,
        bootstrap_args: &BootstrapArgs,
        enable_expression_cache_override: Option<bool>,
        f: F,
    ) -> Result<Option<T>, anyhow::Error>
    where
        F: FnOnce(ConnCatalog) -> T,
    {
        async fn persist_client(
            persist_consensus_url: SensitiveUrl,
            persist_blob_url: SensitiveUrl,
            persist_clients: &PersistClientCache,
        ) -> Result<PersistClient, anyhow::Error> {
            let persist_location = PersistLocation {
                blob_uri: persist_blob_url,
                consensus_uri: persist_consensus_url,
            };
            Ok(persist_clients.open(persist_location).await?)
        }

        if let Some(CatalogConfig {
            persist_consensus_url,
            persist_blob_url,
        }) = &self.materialize.catalog_config
        {
            let persist_client = persist_client(
                persist_consensus_url.clone(),
                persist_blob_url.clone(),
                &self.persist_clients,
            )
            .await?;
            let catalog = Catalog::open_debug_read_only_persist_catalog_config(
                persist_client,
                SYSTEM_TIME.clone(),
                self.materialize.environment_id.clone(),
                system_parameter_defaults,
                build_info,
                bootstrap_args,
                enable_expression_cache_override,
            )
            .await?;
            let res = f(catalog.for_session(&Session::dummy()));
            catalog.expire().await;
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

    /// Resets the adhoc skip consistency check that users can toggle per-file, and returns whether
    /// the consistency checks should be skipped for this current run.
    pub fn clear_skip_consistency_checks(&mut self) -> bool {
        std::mem::replace(&mut self.consistency_checks_adhoc_skip, false)
    }

    pub async fn reset_materialize(&self) -> Result<(), anyhow::Error> {
        let (inner_client, _) = postgres_client(
            &format!(
                "postgres://mz_system:materialize@{}",
                self.materialize.internal_sql_addr
            ),
            self.default_timeout,
        )
        .await?;

        let version = inner_client
            .query_one("SELECT mz_version_num()", &[])
            .await
            .context("getting version of materialize")
            .map(|row| row.get::<_, i32>(0))?;

        let semver = inner_client
            .query_one("SELECT right(split_part(mz_version(), ' ', 1), -1)", &[])
            .await
            .context("getting semver of materialize")
            .map(|row| row.get::<_, String>(0))?
            .parse::<semver::Version>()
            .context("parsing semver of materialize")?;

        inner_client
            .batch_execute("ALTER SYSTEM RESET ALL")
            .await
            .context("resetting materialize state: ALTER SYSTEM RESET ALL")?;

        // Dangerous functions are useful for tests so we enable it for all tests.
        {
            let rename_version = Version::parse("0.128.0-dev.1").expect("known to be valid");
            let enable_unsafe_functions = if semver >= rename_version {
                "unsafe_enable_unsafe_functions"
            } else {
                "enable_unsafe_functions"
            };
            let res = inner_client
                .batch_execute(&format!("ALTER SYSTEM SET {enable_unsafe_functions} = on"))
                .await
                .context("enabling dangerous functions");
            if let Err(e) = res {
                match e.root_cause().downcast_ref::<DbError>() {
                    Some(e) if *e.code() == SqlState::CANT_CHANGE_RUNTIME_PARAM => {
                        info!(
                            "can't enable unsafe functions because the server is safe mode; \
                             testdrive scripts will fail if they use unsafe functions",
                        );
                    }
                    _ => return Err(e),
                }
            }
        }

        for row in inner_client
            .query("SHOW DATABASES", &[])
            .await
            .context("resetting materialize state: SHOW DATABASES")?
        {
            let db_name: String = row.get(0);
            if db_name.starts_with("testdrive_no_reset_") {
                continue;
            }
            let query = format!(
                "DROP DATABASE {}",
                postgres_protocol::escape::escape_identifier(&db_name)
            );
            sql::print_query(&query, None);
            inner_client.batch_execute(&query).await.context(format!(
                "resetting materialize state: DROP DATABASE {}",
                db_name,
            ))?;
        }

        // Get all user clusters not running any objects owned by users
        let inactive_user_clusters = "
        WITH
            active_user_clusters AS
            (
                SELECT DISTINCT cluster_id, object_id
                FROM
                    (
                        SELECT cluster_id, id FROM mz_catalog.mz_sources
                        UNION ALL SELECT cluster_id, id FROM mz_catalog.mz_sinks
                        UNION ALL
                            SELECT cluster_id, id
                            FROM mz_catalog.mz_materialized_views
                        UNION ALL
                            SELECT cluster_id, id FROM mz_catalog.mz_indexes
                        UNION ALL
                            SELECT cluster_id, id
                            FROM mz_internal.mz_subscriptions
                    )
                    AS t (cluster_id, object_id)
                WHERE cluster_id IS NOT NULL AND object_id LIKE 'u%'
            )
        SELECT name
        FROM mz_catalog.mz_clusters
        WHERE
            id NOT IN ( SELECT cluster_id FROM active_user_clusters ) AND id LIKE 'u%'
                AND
            owner_id LIKE 'u%';";

        let inactive_clusters = inner_client
            .query(inactive_user_clusters, &[])
            .await
            .context("resetting materialize state: inactive_user_clusters")?;

        if !inactive_clusters.is_empty() {
            println!("cleaning up user clusters from previous tests...")
        }

        for cluster_name in inactive_clusters {
            let cluster_name: String = cluster_name.get(0);
            if cluster_name.starts_with("testdrive_no_reset_") {
                continue;
            }
            let query = format!(
                "DROP CLUSTER {}",
                postgres_protocol::escape::escape_identifier(&cluster_name)
            );
            sql::print_query(&query, None);
            inner_client.batch_execute(&query).await.context(format!(
                "resetting materialize state: DROP CLUSTER {}",
                cluster_name,
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
                if role_name == self.materialize.user || role_name.starts_with("mz_") {
                    continue;
                }
                let query = format!(
                    "DROP ROLE {}",
                    postgres_protocol::escape::escape_identifier(&role_name)
                );
                sql::print_query(&query, None);
                inner_client.batch_execute(&query).await.context(format!(
                    "resetting materialize state: DROP ROLE {}",
                    role_name,
                ))?;
            }
        }

        // Alter materialize user with all system privileges.
        inner_client
            .batch_execute(&format!(
                "GRANT ALL PRIVILEGES ON SYSTEM TO {}",
                self.materialize.user
            ))
            .await?;

        // Grant initial privileges.
        inner_client
            .batch_execute("GRANT USAGE ON DATABASE materialize TO PUBLIC")
            .await?;
        inner_client
            .batch_execute(&format!(
                "GRANT ALL PRIVILEGES ON DATABASE materialize TO {}",
                self.materialize.user
            ))
            .await?;
        inner_client
            .batch_execute(&format!(
                "GRANT ALL PRIVILEGES ON SCHEMA materialize.public TO {}",
                self.materialize.user
            ))
            .await?;

        let cluster = match version {
            ..=8199 => "default",
            8200.. => "quickstart",
        };
        inner_client
            .batch_execute(&format!("GRANT USAGE ON CLUSTER {cluster} TO PUBLIC"))
            .await?;
        inner_client
            .batch_execute(&format!(
                "GRANT ALL PRIVILEGES ON CLUSTER {cluster} TO {}",
                self.materialize.user
            ))
            .await?;

        Ok(())
    }

    /// Delete Kafka topics + CCSR subjects that were created in this run
    pub async fn reset_kafka(&self) -> Result<(), anyhow::Error> {
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
                    for (res, topic) in res.iter().zip_eq(testdrive_topics.iter()) {
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
                errors
                    .into_iter()
                    .map(|e| e.to_string_with_causes())
                    .join("\n")
            );
        }
    }
}

/// Configuration for the Catalog.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// Handle to the persist consensus system.
    pub persist_consensus_url: SensitiveUrl,
    /// Handle to the persist blob storage.
    pub persist_blob_url: SensitiveUrl,
}

pub enum ControlFlow {
    Continue,
    SkipBegin,
    SkipEnd,
}

#[async_trait]
pub(crate) trait Run {
    async fn run(self, state: &mut State) -> Result<ControlFlow, PosError>;
}

#[async_trait]
impl Run for PosCommand {
    async fn run(self, state: &mut State) -> Result<ControlFlow, PosError> {
        macro_rules! handle_version {
            ($version_constraint:expr) => {
                match $version_constraint {
                    Some(VersionConstraint { min, max }) => {
                        match version_check::run_version_check(min, max, state).await {
                            Ok(true) => return Ok(ControlFlow::Continue),
                            Ok(false) => {}
                            Err(err) => return Err(PosError::new(err, self.pos)),
                        }
                    }
                    None => {}
                }
            };
        }

        let wrap_err = |e| PosError::new(e, self.pos);
        // Substitute variables at startup except for the command-specific ones
        // Those will be substituted at runtime
        let ignore_prefix = match &self.command {
            Command::Builtin(builtin, _) => Some(builtin.name.clone()),
            _ => None,
        };
        let subst = |msg: &str, vars: &BTreeMap<String, String>| {
            substitute_vars(msg, vars, &ignore_prefix, false).map_err(wrap_err)
        };
        let subst_re = |msg: &str, vars: &BTreeMap<String, String>| {
            substitute_vars(msg, vars, &ignore_prefix, true).map_err(wrap_err)
        };

        let r = match self.command {
            Command::Builtin(mut builtin, version_constraint) => {
                handle_version!(version_constraint);
                for val in builtin.args.values_mut() {
                    *val = subst(val, &state.cmd_vars)?;
                }
                for line in &mut builtin.input {
                    *line = subst(line, &state.cmd_vars)?;
                }
                match builtin.name.as_ref() {
                    "check-consistency" => consistency::run_consistency_checks(state).await,
                    "skip-consistency-checks" => {
                        consistency::skip_consistency_checks(builtin, state)
                    }
                    "check-shard-tombstone" => {
                        consistency::run_check_shard_tombstone(builtin, state).await
                    }
                    "fivetran-destination" => {
                        fivetran::run_destination_command(builtin, state).await
                    }
                    "file-append" => file::run_append(builtin, state).await,
                    "file-delete" => file::run_delete(builtin, state).await,
                    "http-request" => http::run_request(builtin, state).await,
                    "kafka-add-partitions" => kafka::run_add_partitions(builtin, state).await,
                    "kafka-create-topic" => kafka::run_create_topic(builtin, state).await,
                    "kafka-wait-topic" => kafka::run_wait_topic(builtin, state).await,
                    "kafka-delete-records" => kafka::run_delete_records(builtin, state).await,
                    "kafka-delete-topic-flaky" => kafka::run_delete_topic(builtin, state).await,
                    "kafka-ingest" => kafka::run_ingest(builtin, state).await,
                    "kafka-verify-data" => kafka::run_verify_data(builtin, state).await,
                    "kafka-verify-commit" => kafka::run_verify_commit(builtin, state).await,
                    "kafka-verify-topic" => kafka::run_verify_topic(builtin, state).await,
                    "mysql-connect" => mysql::run_connect(builtin, state).await,
                    "mysql-execute" => mysql::run_execute(builtin, state).await,
                    "nop" => nop::run_nop(),
                    "postgres-connect" => postgres::run_connect(builtin, state).await,
                    "postgres-execute" => postgres::run_execute(builtin, state).await,
                    "postgres-verify-slot" => postgres::run_verify_slot(builtin, state).await,
                    "protobuf-compile-descriptors" => {
                        protobuf::run_compile_descriptors(builtin, state).await
                    }
                    "psql-execute" => psql::run_execute(builtin, state).await,
                    "s3-verify-data" => s3::run_verify_data(builtin, state).await,
                    "s3-verify-keys" => s3::run_verify_keys(builtin, state).await,
                    "s3-file-upload" => s3::run_upload(builtin, state).await,
                    "s3-set-presigned-url" => s3::run_set_presigned_url(builtin, state).await,
                    "schema-registry-publish" => schema_registry::run_publish(builtin, state).await,
                    "schema-registry-verify" => schema_registry::run_verify(builtin, state).await,
                    "schema-registry-wait" => schema_registry::run_wait(builtin, state).await,
                    "skip-if" => skip_if::run_skip_if(builtin, state).await,
                    "skip-end" => skip_end::run_skip_end(),
                    "sql-server-connect" => sql_server::run_connect(builtin, state).await,
                    "sql-server-execute" => sql_server::run_execute(builtin, state).await,
                    "sql-server-set-from-sql" => sql_server::run_set_from_sql(builtin, state).await,
                    "persist-force-compaction" => {
                        persist::run_force_compaction(builtin, state).await
                    }
                    "random-sleep" => sleep::run_random_sleep(builtin),
                    "set-regex" => set::run_regex_set(builtin, state),
                    "unset-regex" => set::run_regex_unset(builtin, state),
                    "set-sql-timeout" => set::run_sql_timeout(builtin, state),
                    "set-max-tries" => set::run_max_tries(builtin, state),
                    "sleep-is-probably-flaky-i-have-justified-my-need-with-a-comment" => {
                        sleep::run_sleep(builtin)
                    }
                    "set" => set::set_vars(builtin, state),
                    "set-arg-default" => set::run_set_arg_default(builtin, state),
                    "set-from-sql" => set::run_set_from_sql(builtin, state).await,
                    "set-from-file" => set::run_set_from_file(builtin, state).await,
                    "webhook-append" => webhook::run_append(builtin, state).await,
                    _ => {
                        return Err(PosError::new(
                            anyhow!("unknown built-in command {}", builtin.name),
                            self.pos,
                        ));
                    }
                }
            }
            Command::Sql(mut sql, version_constraint) => {
                handle_version!(version_constraint);
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
            Command::FailSql(mut sql, version_constraint) => {
                handle_version!(version_constraint);
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
    static RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\$\{([^}]+)\}").unwrap());
    let mut err = None;
    let out = RE.replace_all(msg, |caps: &Captures| {
        let name = &caps[1];
        if let Some(ignore_prefix) = &ignore_prefix {
            if name.starts_with(format!("{}.", ignore_prefix).as_str()) {
                // Do not substitute, leave original variable name in place
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
    let seed = config.seed.unwrap_or_else(|| rand::thread_rng().r#gen());

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

    let materialize_catalog_config = config.materialize_catalog_config.clone();

    let materialize_url = util::postgres::config_url(&config.materialize_pgconfig)?;
    info!("Connecting to {}", materialize_url.as_str());
    let (pgclient, pgconn) = Retry::default()
        .max_duration(config.default_timeout)
        .retry_async_canceling(|_| async move {
            let mut pgconfig = config.materialize_pgconfig.clone();
            pgconfig.connect_timeout(config.default_timeout);
            let tls = make_tls(&pgconfig)?;
            pgconfig.connect(tls).await.map_err(|e| anyhow!(e))
        })
        .await?;

    let pgconn_task =
        task::spawn(|| "pgconn_task", pgconn).map(|join| join.context("running SQL connection"));

    let materialize_state =
        create_materialize_state(&config, materialize_catalog_config, pgclient).await?;

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
        kafka_config.set("message.max.bytes", "15728640");

        for (key, value) in &config.kafka_opts {
            kafka_config.set(key, value);
        }

        let admin: AdminClient<_> = kafka_config
            .create_with_context(MzClientContext::default())
            .with_context(|| format!("opening Kafka connection: {}", config.kafka_addr))?;

        let admin_opts = AdminOptions::new().operation_timeout(Some(config.default_timeout));

        let producer: FutureProducer<_> = kafka_config
            .create_with_context(MzClientContext::default())
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
        consistency_checks: config.consistency_checks,
        consistency_checks_adhoc_skip: false,
        regex: None,
        regex_replacement: set::DEFAULT_REGEX_REPLACEMENT.into(),
        rewrite_results: config.rewrite_results,
        error_line_count: 0,
        error_string: "".to_string(),

        // === Materialize state. ===
        materialize: materialize_state,

        // === Persist state. ===
        persist_consensus_url: config.persist_consensus_url.clone(),
        persist_blob_url: config.persist_blob_url.clone(),
        build_info: config.build_info,
        persist_clients: PersistClientCache::new(
            PersistConfig::new_default_configs(config.build_info, SYSTEM_TIME.clone()),
            &MetricsRegistry::new(),
            |_, _| PubSubClientConnection::noop(),
        ),

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

        // === Fivetran state. ===
        fivetran_destination_url: config.fivetran_destination_url.clone(),
        fivetran_destination_files_path: config.fivetran_destination_files_path.clone(),

        rewrites: Vec::new(),
        rewrite_pos_start: 0,
        rewrite_pos_end: 0,
    };
    state.initialize_cmd_vars().await?;
    Ok((state, pgconn_task))
}

async fn create_materialize_state(
    config: &&Config,
    materialize_catalog_config: Option<CatalogConfig>,
    pgclient: tokio_postgres::Client,
) -> Result<MaterializeState, anyhow::Error> {
    let materialize_url = util::postgres::config_url(&config.materialize_pgconfig)?;
    let materialize_internal_url =
        util::postgres::config_url(&config.materialize_internal_pgconfig)?;

    for (key, value) in &config.materialize_params {
        pgclient
            .batch_execute(&format!("SET {key} = {value}"))
            .await
            .context("setting session parameter")?;
    }

    let materialize_user = config
        .materialize_pgconfig
        .get_user()
        .expect("testdrive URL must contain user")
        .to_string();

    let materialize_sql_addr = format!(
        "{}:{}",
        materialize_url.host_str().unwrap(),
        materialize_url.port().unwrap()
    );
    let materialize_http_addr = format!(
        "{}:{}",
        materialize_url.host_str().unwrap(),
        config.materialize_http_port
    );
    let materialize_internal_sql_addr = format!(
        "{}:{}",
        materialize_internal_url.host_str().unwrap(),
        materialize_internal_url.port().unwrap()
    );
    let materialize_password_sql_addr = format!(
        "{}:{}",
        materialize_url.host_str().unwrap(),
        config.materialize_password_sql_port
    );
    let materialize_sasl_sql_addr = format!(
        "{}:{}",
        materialize_url.host_str().unwrap(),
        config.materialize_sasl_sql_port
    );
    let materialize_internal_http_addr = format!(
        "{}:{}",
        materialize_internal_url.host_str().unwrap(),
        config.materialize_internal_http_port
    );
    let environment_id = pgclient
        .query_one("SELECT mz_environment_id()", &[])
        .await?
        .get::<_, String>(0)
        .parse()
        .context("parsing environment ID")?;

    let bootstrap_args = BootstrapArgs {
        cluster_replica_size_map: config.materialize_cluster_replica_sizes.clone(),
        default_cluster_replica_size: "ABC".to_string(),
        default_cluster_replication_factor: 1,
        bootstrap_role: None,
    };

    let materialize_state = MaterializeState {
        catalog_config: materialize_catalog_config,
        sql_addr: materialize_sql_addr,
        use_https: config.materialize_use_https,
        http_addr: materialize_http_addr,
        internal_sql_addr: materialize_internal_sql_addr,
        internal_http_addr: materialize_internal_http_addr,
        password_sql_addr: materialize_password_sql_addr,
        sasl_sql_addr: materialize_sasl_sql_addr,
        user: materialize_user,
        pgclient,
        environment_id,
        bootstrap_args,
    };

    Ok(materialize_state)
}
