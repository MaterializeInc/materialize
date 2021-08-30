// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides parsing and convenience functions for working with Kafka from the `sql` package.

use std::collections::BTreeMap;
use std::convert::{self, TryInto};
use std::fs::File;
use std::io::Read;
use std::sync::{Arc, Mutex};

use anyhow::bail;
use log::{debug, error, info, warn};
use ore::collections::CollectionExt;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::{Offset, TopicPartitionList};
use reqwest::Url;
use tokio::task;
use tokio::time::Duration;

use ccsr::tls::{Certificate, Identity};
use sql_parser::ast::Value;

enum ValType {
    Path,
    String,
    // Number with range [lower, upper]
    Number(i32, i32),
    Boolean,
    EnvVar,
}

impl ValType {
    fn process_val(&self, val: &Value) -> Result<String, anyhow::Error> {
        Ok(match (&self, val) {
            (ValType::String, Value::String(v)) => v.to_string(),
            (ValType::Boolean, Value::Boolean(b)) => b.to_string(),
            (ValType::Path, Value::String(v)) => {
                if std::fs::metadata(&v).is_err() {
                    bail!("file does not exist")
                }
                v.to_string()
            }
            (ValType::Number(lower, upper), Value::Number(n)) => match n.parse::<i32>() {
                Ok(parsed_n) if *lower <= parsed_n && parsed_n <= *upper => n.to_string(),
                _ => bail!("must be a number between {} and {}", lower, upper),
            },
            (ValType::EnvVar, Value::String(v)) => std::env::var(v)?,
            _ => bail!("unexpected value type"),
        })
    }
}

// Describes Kafka cluster configurations users can suppply using `CREATE
// SOURCE...WITH (option_list)`.
struct Config {
    name: &'static str,
    val_type: ValType,
    transform: fn(String) -> String,
    default: Option<String>,
    // If set, look for an environment variable named `<name>_env` to possibly
    // define the named setting.
    include_env_var: bool,
}

impl Config {
    fn new(name: &'static str, val_type: ValType) -> Self {
        Config {
            name,
            val_type,
            transform: convert::identity,
            default: None,
            include_env_var: false,
        }
    }

    /// Shorthand for simple string config options.
    fn string(name: &'static str) -> Self {
        Config::new(name, ValType::String)
    }

    /// Shorthand for simple path config options.
    fn path(name: &'static str) -> Self {
        Config::new(name, ValType::Path)
    }

    /// Builds a new config that transforms the parameter according to `f` after
    /// it is validated.
    fn set_transform(mut self, f: fn(String) -> String) -> Self {
        self.transform = f;
        self
    }

    /// Performs `self`'s `transform` on `v`.
    fn do_transform(&self, v: String) -> String {
        (self.transform)(v)
    }

    /// Allows for returning a default value for this configuration option
    fn set_default(mut self, d: Option<String>) -> Self {
        assert!(
            !self.include_env_var,
            "cannot currently both set default values and include environment variables on the same config"
        );
        self.default = d;
        self
    }

    /// Allows for returning a default value for this configuration option
    fn include_env_var(mut self) -> Self {
        assert!(
            self.default.is_none(),
            "cannot currently both set default values and include environment variables on the same config"
        );
        self.include_env_var = true;
        self
    }

    /// Get the appropriate String to use as the Kafka config key.
    fn get_kafka_config_key(&self) -> String {
        self.name.replace("_", ".")
    }

    /// Gets the key to lookup for configs that support environment variable lookups.
    fn get_env_var_key(&self) -> String {
        format!("{}_env", self.name)
    }
}

fn extract(
    input: &mut BTreeMap<String, Value>,
    configs: &[Config],
) -> Result<BTreeMap<String, String>, anyhow::Error> {
    let mut out = BTreeMap::new();
    for config in configs {
        // Look for config.name
        let value = match input.remove(config.name) {
            Some(v) => match config.val_type.process_val(&v) {
                Ok(v) => {
                    // Ensure env var variant wasn't also included.
                    if config.include_env_var && input.get(&config.get_env_var_key()).is_some() {
                        bail!(
                            "Invalid WITH options: cannot specify both {} and {} options at the same time",
                            config.name,
                            config.get_env_var_key()
                        )
                    }

                    v
                }
                Err(e) => bail!("Invalid WITH option {}={}: {}", config.name, v, e),
            },
            // If config.name is not a key and config permits it, look for an
            // environment variable.
            None if config.include_env_var => match input.remove(&config.get_env_var_key()) {
                Some(v) => match ValType::EnvVar.process_val(&v) {
                    Ok(v) => v,
                    Err(e) => bail!(
                        "Invalid WITH option {}={}: {}",
                        config.get_env_var_key(),
                        v,
                        e
                    ),
                },
                None => continue,
            },
            // Check for default values
            None => match &config.default {
                Some(v) => v.to_string(),
                None => continue,
            },
        };
        let value = config.do_transform(value);
        out.insert(config.get_kafka_config_key(), value);
    }
    Ok(out)
}

/// Parse the `with_options` from a `CREATE SOURCE` or `CREATE SINK`
/// statement to determine user-supplied config options, e.g. security
/// options.
///
/// # Errors
///
/// - Invalid values for known options, such as files that do not exist for
/// expected file paths.
/// - If any of the values in `with_options` are not
///   `sql_parser::ast::Value::String`.
pub fn extract_config(
    with_options: &mut BTreeMap<String, Value>,
) -> Result<BTreeMap<String, String>, anyhow::Error> {
    extract(
        with_options,
        &[
            Config::string("acks"),
            Config::string("client_id"),
            Config::new(
                "statistics_interval_ms",
                // The range of values comes from `statistics.interval.ms` in
                // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                ValType::Number(0, 86_400_000),
            )
            .set_default(Some(
                chrono::Duration::seconds(1).num_milliseconds().to_string(),
            )),
            Config::new(
                "topic_metadata_refresh_interval_ms",
                // The range of values comes from `topic.metadata.refresh.interval.ms` in
                // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                ValType::Number(0, 3_600_000),
            ),
            Config::new("enable_auto_commit", ValType::Boolean),
            Config::string("isolation_level").set_default(Some(String::from("read_committed"))),
            Config::string("security_protocol"),
            Config::path("sasl_kerberos_keytab"),
            Config::string("sasl_username"),
            Config::string("sasl_password").include_env_var(),
            Config::string("sasl_kerberos_kinit_cmd"),
            Config::string("sasl_kerberos_min_time_before_relogin"),
            Config::string("sasl_kerberos_principal"),
            Config::string("sasl_kerberos_service_name"),
            // For historical reasons, we allow `sasl_mechanisms` to be lowercase or
            // mixed case, while librdkafka requires all uppercase (e.g., `PLAIN`,
            // not `plain`).
            Config::string("sasl_mechanisms").set_transform(|s| s.to_uppercase()),
            Config::path("ssl_ca_location"),
            Config::path("ssl_certificate_location"),
            Config::path("ssl_key_location"),
            Config::string("ssl_key_password").include_env_var(),
            Config::new("transaction_timeout_ms", ValType::Number(0, i32::MAX)),
            Config::new("enable_idempotence", ValType::Boolean),
        ],
    )
}

/// Create a new `rdkafka::ClientConfig` with the provided
/// [`options`](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md),
/// and test its ability to create an `rdkafka::consumer::BaseConsumer`.
///
/// Expected to test the output of `extract_security_config`.
///
/// # Errors
///
/// - `librdkafka` cannot create a BaseConsumer using the provided `options`.
///   For example, when using Kerberos auth, and the named principal does not
///   exist.
pub async fn create_consumer(
    broker: &str,
    topic: &str,
    options: &BTreeMap<String, String>,
) -> Result<Arc<BaseConsumer<KafkaErrCheckContext>>, anyhow::Error> {
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", broker);
    for (k, v) in options {
        config.set(k, v);
    }

    match config.create_with_context(KafkaErrCheckContext::default()) {
        Ok(consumer) => {
            let consumer: Arc<BaseConsumer<KafkaErrCheckContext>> = Arc::new(consumer);
            let context = consumer.context().clone();
            let topic = String::from(topic);
            // Wait for a metadata request for up to one second. This greatly
            // increases the probability that we'll see a connection error if
            // e.g. the hostname was mistyped. librdkafka doesn't expose a
            // better API for asking whether a connection succeeded or failed,
            // unfortunately.
            task::spawn_blocking({
                let consumer = consumer.clone();
                move || {
                    let _ = consumer.fetch_metadata(Some(&topic), Duration::from_secs(1));
                }
            })
            .await?;
            let error = context.error.lock().expect("lock poisoned");
            if let Some(error) = &*error {
                bail!("librdkafka: {}", error)
            }
            Ok(consumer)
        }
        Err(e) => {
            match e {
                rdkafka::error::KafkaError::ClientCreation(s) => {
                    // Rewrite error message to provide Materialize-specific guidance.
                    if s == "Invalid sasl.kerberos.kinit.cmd value: Property \
            not available: \"sasl.kerberos.keytab\""
                    {
                        bail!(
                            "Can't seem to find local keytab cache. With \
                             sasl_mechanisms='GSSAPI', you must provide an \
                             explicit sasl_kerberos_keytab or \
                             sasl_kerberos_kinit_cmd option."
                        )
                    } else {
                        // Pass existing error back up.
                        bail!(rdkafka::error::KafkaError::ClientCreation(s))
                    }
                }
                _ => bail!(e),
            }
        }
    }
}

/// Returns start offsets for the partitions of `topic` and the provided
/// `kafka_time_offset` option.
///
/// For each partition, the returned offset is the earliest offset whose
/// timestamp is greater than or equal to the given timestamp for the
/// partition. If no such message exists (or the Kafka broker is before
/// 0.10.0), the current end offset is returned for the partition.
///
/// The provided `kafka_time_offset` option must be a non-zero number:
/// * Non-Negative numbers will used as is (e.g. `1622659034343`)
/// * Negative numbers will be translated to a timestamp in millis
///   before now (e.g. `-10` means 10 millis ago)
///
/// If `kafka_time_offset` has not been configured, an empty Option is
/// returned.
pub async fn lookup_start_offsets(
    consumer: Arc<BaseConsumer<KafkaErrCheckContext>>,
    topic: &str,
    with_options: &BTreeMap<String, Value>,
    now: u64,
) -> Result<Option<Vec<i64>>, anyhow::Error> {
    let time_offset = with_options.get("kafka_time_offset");
    if time_offset.is_none() {
        return Ok(None);
    } else if with_options.contains_key("start_offset") {
        bail!("`start_offset` and `kafka_time_offset` cannot be set at the same time.")
    }

    // Validate and resolve `kafka_time_offset`.
    let time_offset = match time_offset.unwrap() {
        Value::Number(s) => match s.parse::<i64>() {
            // Timestamp in millis *before* now (e.g. -10 means 10 millis ago)
            Ok(ts) if ts < 0 => {
                let now: i64 = now.try_into()?;
                let ts = now - ts.abs();
                if ts <= 0 {
                    bail!("Relative `kafka_time_offset` must be smaller than current system timestamp")
                }
                ts
            }
            // Timestamp in millis (e.g. 1622659034343)
            Ok(ts) => ts,
            _ => bail!("`kafka_time_offset` must be a number"),
        },
        _ => bail!("`kafka_time_offset` must be a number"),
    };

    // Lookup offsets
    task::spawn_blocking({
        let topic = topic.to_string();
        move || {
            // There cannot be more than i32 partitions
            let num_partitions =
                get_partitions(consumer.as_ref(), &topic, Duration::from_secs(10))?.len();

            let mut tpl = TopicPartitionList::with_capacity(1);
            tpl.add_partition_range(&topic, 0, num_partitions as i32 - 1);
            tpl.set_all_offsets(Offset::Offset(time_offset))?;

            let offsets_for_times = consumer.offsets_for_times(tpl, Duration::from_secs(10))?;

            // Translate to `start_offsets`
            let start_offsets = offsets_for_times
                .elements()
                .iter()
                .map(|elem| match elem.offset() {
                    Offset::Offset(offset) => Ok(offset),
                    Offset::End => fetch_end_offset(&consumer, &topic, elem.partition()),
                    _ => bail!(
                        "Unexpected offset {:?} for partition {}",
                        elem.offset(),
                        elem.partition()
                    ),
                })
                .collect::<Result<Vec<_>, _>>()?;

            if start_offsets.len() != num_partitions {
                bail!(
                    "Expected offsets for {} partitions, but recevied {}",
                    num_partitions,
                    start_offsets.len(),
                );
            }

            Ok(Some(start_offsets))
        }
    })
    .await?
}

// Kafka supports bulk lookup of watermarks, but it is not exposed in rdkafka.
// If that ever changes, we will want to first collect all pids that have no
// offset for a given timestamp and then do a single request (instead of doing
// a request for each paritition individually).
fn fetch_end_offset(
    consumer: &BaseConsumer<KafkaErrCheckContext>,
    topic: &str,
    pid: i32,
) -> Result<i64, anyhow::Error> {
    let (_low, high) = consumer.fetch_watermarks(topic, pid, Duration::from_secs(10))?;
    Ok(high)
}

// Return the list of partition ids associated with a specific topic
pub fn get_partitions<C: ConsumerContext>(
    consumer: &BaseConsumer<C>,
    topic: &str,
    timeout: Duration,
) -> Result<Vec<i32>, anyhow::Error> {
    let meta = consumer.fetch_metadata(Some(&topic), timeout)?;
    if meta.topics().len() != 1 {
        bail!(
            "topic {} has {} metadata entries; expected 1",
            topic,
            meta.topics().len()
        );
    }
    let meta_topic = meta.topics().into_element();
    if meta_topic.name() != topic {
        bail!(
            "got results for wrong topic {} (expected {})",
            meta_topic.name(),
            topic
        );
    }

    if meta_topic.partitions().len() == 0 {
        bail!("topic {} does not exist", topic);
    }

    Ok(meta_topic.partitions().iter().map(|x| x.id()).collect())
}

/// Gets error strings from `rdkafka` when creating test consumers.
#[derive(Default, Debug)]
pub struct KafkaErrCheckContext {
    pub error: Mutex<Option<String>>,
}

impl ConsumerContext for KafkaErrCheckContext {}

impl ClientContext for KafkaErrCheckContext {
    // `librdkafka` doesn't seem to propagate all Kerberos errors up the stack,
    // but does log them, so we are currently relying on the `log` callback for
    // error handling in situations we're aware of, e.g. cannot log into
    // Kerberos.
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        use rdkafka::config::RDKafkaLogLevel::*;
        match level {
            Emerg | Alert | Critical | Error => {
                let mut error = self.error.lock().expect("lock poisoned");
                // Do not allow logging to overwrite other values if
                // present.
                if error.is_none() {
                    *error = Some(log_message.to_string());
                }
                error!(target: "librdkafka", "{} {}", fac, log_message);
            }
            Warning => warn!(target: "librdkafka", "{} {}", fac, log_message),
            Notice => info!(target: "librdkafka", "{} {}", fac, log_message),
            Info => info!(target: "librdkafka", "{} {}", fac, log_message),
            Debug => debug!(target: "librdkafka", "{} {}", fac, log_message),
        }
    }
    // Refer to the comment on the `log` callback.
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        // Allow error to overwrite value irrespective of other conditions
        // (i.e. logging).
        *self.error.lock().expect("lock poisoned") = Some(reason.to_string());
        error!("librdkafka: {}: {}", error, reason);
    }
}

// Generates a `ccsr::ClientConfig` based on the configuration extracted from
// `extract_security_config()`. Currently only supports SSL auth.
pub fn generate_ccsr_client_config(
    csr_url: Url,
    kafka_options: &BTreeMap<String, String>,
    mut ccsr_options: BTreeMap<String, Value>,
) -> Result<ccsr::ClientConfig, anyhow::Error> {
    let mut client_config = ccsr::ClientConfig::new(csr_url);

    if let Some(ca_path) = kafka_options.get("ssl.ca.location") {
        let mut ca_buf = Vec::new();
        File::open(ca_path)?.read_to_end(&mut ca_buf)?;
        let cert = Certificate::from_pem(&ca_buf)?;
        client_config = client_config.add_root_certificate(cert);
    }

    let key_path = kafka_options.get("ssl.key.location");
    let cert_path = kafka_options.get("ssl.certificate.location");
    match (key_path, cert_path) {
        (Some(key_path), Some(cert_path)) => {
            // `reqwest` expects identity `pem` files to contain one key and
            // at least one certificate. Because `librdkafka` expects these
            // as two separate arguments, we simply concatenate them for
            // `reqwest`'s sake.
            let mut ident_buf = Vec::new();
            File::open(key_path)?.read_to_end(&mut ident_buf)?;
            File::open(cert_path)?.read_to_end(&mut ident_buf)?;
            let ident = Identity::from_pem(&ident_buf)?;
            client_config = client_config.identity(ident);
        }
        (None, None) => {}
        (_, _) => bail!(
            "Reading from SSL-auth Confluent Schema Registry \
             requires both ssl.key.location and ssl.certificate.location"
        ),
    }

    let mut ccsr_options = extract(
        &mut ccsr_options,
        &[Config::string("username"), Config::string("password")],
    )?;
    if let Some(username) = ccsr_options.remove("username") {
        client_config = client_config.auth(username, ccsr_options.remove("password"));
    }

    Ok(client_config)
}
