// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides parsing and convenience functions for working with Kafka from the `sql` package.

use std::collections::BTreeMap;
use std::convert;
use std::fs::File;
use std::io::Read;
use std::sync::Mutex;

use anyhow::bail;
use log::{debug, error, info, warn};
use rdkafka::client::ClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext};
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
}

// Describes Kafka cluster configurations users can suppply using `CREATE
// SOURCE...WITH (option_list)`.
// TODO(sploiselle): Support overriding keys.
struct Config {
    name: &'static str,
    val_type: ValType,
    transform: fn(String) -> String,
    default: Option<String>,
}

impl Config {
    fn new(name: &'static str, val_type: ValType) -> Self {
        Config {
            name,
            val_type,
            transform: convert::identity,
            default: None,
        }
    }

    // Shorthand for simple string config options.
    fn string(name: &'static str) -> Self {
        Config::new(name, ValType::String)
    }

    // Shorthand for simple path config options.
    fn path(name: &'static str) -> Self {
        Config::new(name, ValType::Path)
    }

    // Builds a new config that transforms the parameter according to `f` after
    // it is validated.
    fn transform(mut self, f: fn(String) -> String) -> Self {
        self.transform = f;
        self
    }

    // Allows for returning a default value for this configuration option
    fn set_default(mut self, d: Option<String>) -> Self {
        self.default = d;
        self
    }

    // Get the appropriate String to use as the Kafka config key.
    fn get_key(&self) -> String {
        self.name.replace("_", ".")
    }

    fn validate_val(&self, val: &Value) -> Result<String, anyhow::Error> {
        let val = match (&self.val_type, val) {
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
            _ => bail!("unexpected value type"),
        };
        Ok((self.transform)(val))
    }
}

fn extract(
    input: &mut BTreeMap<String, Value>,
    configs: &[Config],
) -> Result<BTreeMap<String, String>, anyhow::Error> {
    let mut out = BTreeMap::new();
    for config in configs {
        let value = match input.remove(config.name) {
            Some(v) => match config.validate_val(&v) {
                Ok(v) => v,
                Err(e) => bail!("Invalid WITH option {}={}: {}", config.name, v, e),
            },
            None => match &config.default {
                Some(v) => v.to_string(),
                None => {
                    continue;
                }
            },
        };
        out.insert(config.get_key(), value);
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
            Config::string("security_protocol"),
            Config::path("sasl_kerberos_keytab"),
            Config::string("sasl_username"),
            Config::string("sasl_password"),
            Config::string("sasl_kerberos_kinit_cmd"),
            Config::string("sasl_kerberos_min_time_before_relogin"),
            Config::string("sasl_kerberos_principal"),
            Config::string("sasl_kerberos_service_name"),
            // For historical reasons, we allow `sasl_mechanisms` to be lowercase or
            // mixed case, while librdkafka requires all uppercase (e.g., `PLAIN`,
            // not `plain`).
            Config::string("sasl_mechanisms").transform(|s| s.to_uppercase()),
            Config::path("ssl_ca_location"),
            Config::path("ssl_certificate_location"),
            Config::path("ssl_key_location"),
            Config::string("ssl_key_password"),
            Config::new("transaction_timeout_ms", ValType::Number(0, i32::MAX)),
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
pub async fn test_config(
    broker: &str,
    options: &BTreeMap<String, String>,
) -> Result<(), anyhow::Error> {
    let mut config = rdkafka::ClientConfig::new();
    config.set("bootstrap.servers", broker);
    for (k, v) in options {
        config.set(k, v);
    }

    match config.create_with_context(KafkaErrCheckContext::default()) {
        Ok(consumer) => {
            let consumer: BaseConsumer<KafkaErrCheckContext> = consumer;
            let context = consumer.context().clone();
            // Wait for a metadata request for up to one second. This greatly
            // increases the probability that we'll see a connection error if
            // e.g. the hostname was mistyped. librdkafka doesn't expose a
            // better API for asking whether a connection succeeded or failed,
            // unfortunately.
            task::spawn_blocking(move || {
                let _ = consumer.fetch_metadata(None, Duration::from_secs(1));
            })
            .await?;
            let error = context.error.lock().expect("lock poisoned");
            if let Some(error) = &*error {
                bail!("librdkafka: {}", error)
            }
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
    Ok(())
}

/// Gets error strings from `rdkafka` when creating test consumers.
#[derive(Default)]
struct KafkaErrCheckContext {
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
