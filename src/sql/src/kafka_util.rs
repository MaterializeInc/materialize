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
use std::sync::{Arc, Mutex};

use anyhow::bail;
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
use rdkafka::{Offset, TopicPartitionList};
use reqwest::Url;
use tokio::time::Duration;

use mz_dataflow_types::connections::{
    CsrConnection, CsrConnectionHttpAuth, CsrConnectionTlsIdentity, StringOrSecret,
};
use mz_kafka_util::client::{create_new_client_config, MzClientContext};
use mz_ore::task;
use mz_secrets::SecretsReader;
use mz_sql_parser::ast::Value;

use crate::normalize::SqlValueOrSecret;

enum ValType {
    String { transform: fn(String) -> String },
    StringOrSecret,
    Secret,
    // Number with range [lower, upper]
    Number(i32, i32),
    Boolean,
}

// Describes Kafka cluster configurations users can supply using `CREATE
// SOURCE...WITH (option_list)`.
struct Config {
    name: &'static str,
    val_type: ValType,
    default: Option<String>,
}

impl Config {
    fn new(name: &'static str, val_type: ValType) -> Self {
        Config {
            name,
            val_type,
            default: None,
        }
    }

    /// Shorthand for simple string config options.
    fn string(name: &'static str) -> Self {
        Config::new(
            name,
            ValType::String {
                transform: convert::identity,
            },
        )
    }

    /// Shorthand for a string config option with a transformation function.
    fn string_transform(name: &'static str, transform: fn(String) -> String) -> Self {
        Config::new(name, ValType::String { transform })
    }

    /// Shorthand for a config option that can be either a string or a secret.
    fn string_or_secret(name: &'static str) -> Self {
        Config::new(name, ValType::StringOrSecret)
    }

    /// Shorthand for secret config options.
    fn secret(name: &'static str) -> Self {
        Config::new(name, ValType::Secret)
    }

    /// Allows for returning a default value for this configuration option
    fn set_default(mut self, d: Option<String>) -> Self {
        self.default = d;
        self
    }

    /// Get the appropriate String to use as the Kafka config key.
    fn get_kafka_config_key(&self) -> String {
        self.name.replace('_', ".")
    }
}

fn extract(
    input: &mut BTreeMap<String, SqlValueOrSecret>,
    configs: &[Config],
) -> Result<BTreeMap<String, StringOrSecret>, anyhow::Error> {
    let mut out = BTreeMap::new();
    for config in configs {
        // Look for config.name
        let value = match (input.remove(config.name), &config.val_type) {
            (Some(SqlValueOrSecret::Value(Value::Boolean(b))), ValType::Boolean) => {
                StringOrSecret::String(b.to_string())
            }
            (Some(SqlValueOrSecret::Value(Value::Number(n))), ValType::Number(lower, upper)) => {
                match n.parse::<i32>() {
                    Ok(parsed_n) if *lower <= parsed_n && parsed_n <= *upper => {
                        StringOrSecret::String(n.to_string())
                    }
                    _ => bail!("must be a number between {} and {}", lower, upper),
                }
            }
            (Some(SqlValueOrSecret::Value(Value::String(s))), ValType::String { transform }) => {
                StringOrSecret::String(transform(s.to_string()))
            }
            (Some(SqlValueOrSecret::Value(Value::String(s))), ValType::StringOrSecret) => {
                StringOrSecret::String(s.to_string())
            }
            (Some(SqlValueOrSecret::Secret(id)), ValType::Secret)
            | (Some(SqlValueOrSecret::Secret(id)), ValType::StringOrSecret) => {
                StringOrSecret::Secret(id)
            }
            // Check for default values
            (None, _) => match &config.default {
                Some(v) => StringOrSecret::String(v.to_string()),
                None => continue,
            },
            (Some(SqlValueOrSecret::Value(v)), _) => {
                bail!(
                    "Invalid WITH option {}={}: unexpected value type",
                    config.name,
                    v
                );
            }
            (Some(SqlValueOrSecret::Secret(_)), _) => {
                bail!(
                    "WITH option {} does not accept secret references",
                    config.name
                );
            }
        };
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
    with_options: &mut BTreeMap<String, SqlValueOrSecret>,
) -> anyhow::Result<BTreeMap<String, StringOrSecret>> {
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
            Config::string_or_secret("sasl_username"),
            Config::secret("sasl_password"),
            // For historical reasons, we allow `sasl_mechanisms` to be lowercase or
            // mixed case, while librdkafka requires all uppercase (e.g., `PLAIN`,
            // not `plain`).
            Config::string_transform("sasl_mechanisms", |s| s.to_uppercase()),
            Config::string_or_secret("ssl_ca_pem"),
            Config::string_or_secret("ssl_certificate_pem"),
            Config::secret("ssl_key_pem"),
            Config::secret("ssl_key_password"),
            Config::new("transaction_timeout_ms", ValType::Number(0, i32::MAX)),
            Config::new("enable_idempotence", ValType::Boolean),
            Config::new(
                "fetch_message_max_bytes",
                // The range of values comes from `fetch.message.max.bytes` in
                // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                ValType::Number(0, 1_000_000_000),
            ),
        ],
    )
}

/// Create a new `rdkafka::ClientConfig` with the provided
/// [`options`](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md),
/// and test its ability to create an `rdkafka::consumer::BaseConsumer`.
///
/// Expected to test the output of `extract_security_config`.
///
/// # Panics
///
/// - `options` does not contain `bootstrap.servers` as a key
///
/// # Errors
///
/// - `librdkafka` cannot create a BaseConsumer using the provided `options`.
pub async fn create_consumer(
    topic: &str,
    options: &BTreeMap<String, StringOrSecret>,
    librdkafka_log_level: tracing::Level,
    secrets_reader: &SecretsReader,
) -> Result<Arc<BaseConsumer<KafkaErrCheckContext>>, anyhow::Error> {
    let mut config = create_new_client_config(librdkafka_log_level);
    for (k, v) in options {
        config.set(k, v.get_string(secrets_reader).await?);
    }

    // We need this only for logging which broker we're connecting to; the
    // setting itself makes its way into `config`.
    let broker = config
        .get("bootstrap.servers")
        .expect("callers must have already set bootstrap.servers");

    let consumer: Arc<BaseConsumer<KafkaErrCheckContext>> =
        Arc::new(config.create_with_context(KafkaErrCheckContext::default())?);
    let context = Arc::clone(&consumer.context());
    let owned_topic = String::from(topic);
    // Wait for a metadata request for up to one second. This greatly
    // increases the probability that we'll see a connection error if
    // e.g. the hostname was mistyped. librdkafka doesn't expose a
    // better API for asking whether a connection succeeded or failed,
    // unfortunately.
    task::spawn_blocking(move || format!("kafka_set_metadata:{broker}:{topic}"), {
        let consumer = Arc::clone(&consumer);
        move || {
            let _ = consumer.fetch_metadata(Some(&owned_topic), Duration::from_secs(1));
        }
    })
    .await?;
    let error = context.error.lock().expect("lock poisoned");
    if let Some(error) = &*error {
        bail!("librdkafka: {}", error)
    }
    Ok(consumer)
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
    with_options: &BTreeMap<String, SqlValueOrSecret>,
    now: u64,
) -> Result<Option<Vec<i64>>, anyhow::Error> {
    let time_offset = match with_options.get("kafka_time_offset").cloned() {
        None => return Ok(None),
        Some(_) if with_options.contains_key("start_offset") => {
            bail!("`start_offset` and `kafka_time_offset` cannot be set at the same time.")
        }
        Some(offset) => offset,
    };

    // Validate and resolve `kafka_time_offset`.
    let time_offset = match time_offset.into() {
        Some(Value::Number(s)) => match s.parse::<i64>() {
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
    // TODO(guswynn): see if we can add broker to this name
    task::spawn_blocking(|| format!("kafka_lookup_start_offsets:{topic}"), {
        let topic = topic.to_string();
        move || {
            // There cannot be more than i32 partitions
            let num_partitions = mz_kafka_util::client::get_partitions(
                consumer.as_ref().client(),
                &topic,
                Duration::from_secs(10),
            )?
            .len();

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
                    "Expected offsets for {} partitions, but received {}",
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
// a request for each partition individually).
fn fetch_end_offset(
    consumer: &BaseConsumer<KafkaErrCheckContext>,
    topic: &str,
    pid: i32,
) -> Result<i64, anyhow::Error> {
    let (_low, high) = consumer.fetch_watermarks(topic, pid, Duration::from_secs(10))?;
    Ok(high)
}

/// Gets error strings from `rdkafka` when creating test consumers.
#[derive(Default, Debug)]
pub struct KafkaErrCheckContext {
    pub error: Mutex<Option<String>>,
}

impl ConsumerContext for KafkaErrCheckContext {}

impl ClientContext for KafkaErrCheckContext {
    // `librdkafka` doesn't seem to propagate all errors up the stack, but does
    // log them, so we are currently relying on the `log` callback for error
    // handling in some situations.
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
            }
            _ => {}
        }
        MzClientContext.log(level, fac, log_message)
    }
    // Refer to the comment on the `log` callback.
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        // Allow error to overwrite value irrespective of other conditions
        // (i.e. logging).
        *self.error.lock().expect("lock poisoned") = Some(reason.to_string());
        MzClientContext.error(error, reason)
    }
}

// Generates a `CsrConnection` based on the configuration extracted from
// `extract_security_config()`.
pub fn generate_ccsr_connection(
    url: Url,
    ccsr_options: &mut BTreeMap<String, SqlValueOrSecret>,
) -> Result<CsrConnection, anyhow::Error> {
    let mut ccsr_options = extract(
        ccsr_options,
        &[
            Config::string_or_secret("ssl_ca_pem"),
            Config::secret("ssl_key_pem"),
            Config::string_or_secret("ssl_certificate_pem"),
            Config::string_or_secret("username"),
            Config::secret("password"),
        ],
    )?;

    let root_certs = match ccsr_options.remove("ssl.ca.pem") {
        None => vec![],
        Some(cert) => vec![cert],
    };
    let cert = ccsr_options.remove("ssl.certificate.pem");
    let key = ccsr_options.remove("ssl.key.pem");
    let tls_identity = match (cert, key) {
        (None, None) => None,
        (Some(cert), Some(key)) => {
            // `key` was verified to be a secret by `extract`.
            let key = key.unwrap_secret();
            Some(CsrConnectionTlsIdentity { cert, key })
        }
        _ => bail!(
            "Reading from SSL-auth Confluent Schema Registry \
             requires both ssl.key.pem and ssl.certificate.pem"
        ),
    };
    let http_auth = match ccsr_options.remove("username") {
        None => None,
        Some(username) => {
            let password = ccsr_options.remove("password");
            // `password` was verified to be a secret by `extract`.
            let password = password.map(|p| p.unwrap_secret());
            Some(CsrConnectionHttpAuth { username, password })
        }
    };
    Ok(CsrConnection {
        url,
        root_certs,
        tls_identity,
        http_auth,
    })
}
