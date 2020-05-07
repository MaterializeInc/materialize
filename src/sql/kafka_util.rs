// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provides parsing and convenience functions for working with Kafka from the `sql` package.

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::sync::{Arc, Mutex};

use ccsr::tls::{Certificate, Identity};
use reqwest::Url;

use failure::bail;
use log::{debug, error, info, warn};
use rdkafka::consumer::BaseConsumer;
use sql_parser::ast::Value;

enum ValType {
    Path,
    String,
    // Number with range [lower, upper]
    Number(i32, i32),
}

// Describes Kafka cluster configurations users can suppply using `CREATE
// SOURCE...WITH (option_list)`.
// TODO(sploiselle): Support overriding keys, default values.
struct Config {
    name: &'static str,
    val_type: ValType,
}

impl Config {
    fn new(name: &'static str, val_type: ValType) -> Self {
        Config { name, val_type }
    }

    // Shorthand for simple string config options.
    fn string(name: &'static str) -> Self {
        Config {
            name,
            val_type: ValType::String,
        }
    }

    // Shorthand for simple path config options.
    fn path(name: &'static str) -> Self {
        Config {
            name,
            val_type: ValType::Path,
        }
    }

    // Get the appropriate String to use as the Kafka config key.
    fn get_key(&self) -> String {
        self.name.replace("_", ".")
    }

    fn validate_val(&self, val: &Value) -> Result<String, failure::Error> {
        match (&self.val_type, val) {
            (ValType::String, Value::SingleQuotedString(v)) => Ok(v.to_string()),
            (ValType::Path, Value::SingleQuotedString(v)) => {
                if std::fs::metadata(&v).is_err() {
                    bail!("file does not exist")
                }
                Ok(v.to_string())
            }
            (ValType::Number(lower, upper), Value::Number(n)) => match n.parse::<i32>() {
                Ok(parsed_n) if *lower <= parsed_n && parsed_n <= *upper => Ok(n.to_string()),
                _ => bail!("must be a number between {} and {}", lower, upper),
            },
            _ => bail!("unexpected value type"),
        }
    }
}

// Aggregates all of the configurations provided in `with_options`.
struct ConfigAggregator<'a> {
    input: &'a mut HashMap<String, Value>,
    output: HashMap<String, String>,
}

impl<'a> ConfigAggregator<'a> {
    fn new(input: &'a mut HashMap<String, Value>) -> Self {
        ConfigAggregator {
            input,
            output: HashMap::new(),
        }
    }
    fn extract(&mut self, configs: &[Config]) -> Result<(), failure::Error> {
        for config in configs {
            let value = match self.input.remove(config.name) {
                Some(v) => match config.validate_val(&v) {
                    Ok(v) => v,
                    Err(e) => bail!("Invalid WITH option {}={}: {}", config.name, v, e),
                },
                None => continue,
            };
            self.output.insert(config.get_key(), value);
        }

        Ok(())
    }
    fn remove_from_input(&mut self, k: &str) -> Option<Value> {
        self.input.remove(k)
    }
    fn insert_into_output(&mut self, k: String, v: String) {
        self.output.insert(k, v);
    }
    fn finish(self) -> HashMap<String, String> {
        self.output
    }
}

/// Parse the `with_options` from a `CREATE SOURCE` statement to determine
/// user-supplied config options, e.g. security options.
///
/// # Errors
///
/// - Invalid values for known options, such as files that do not exist for
/// expected file paths.
/// - If any of the values in `with_options` are not
///   `sql_parser::ast::Value::SingleQuotedString`.
pub fn extract_config(
    with_options: &mut HashMap<String, Value>,
) -> Result<HashMap<String, String>, failure::Error> {
    let mut agg = ConfigAggregator::new(with_options);

    let allowed_configs = vec![
        Config::string("client_id"),
        Config::new(
            "statistics_interval_ms",
            // The range of values comes from `statistics.interval.ms` in
            // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
            ValType::Number(0, 86_400_000),
        ),
    ];

    agg.extract(&allowed_configs)?;

    extract_security_config(&mut agg)?;

    Ok(agg.finish())
}

// Parse the `with_options` from a `CREATE SOURCE` statement to determine Kafka
// security strategy, and extract any additional supplied configurations.
fn extract_security_config(mut agg: &mut ConfigAggregator) -> Result<(), failure::Error> {
    let security_protocol = match agg.remove_from_input("security_protocol") {
        None => None,
        Some(Value::SingleQuotedString(p)) => Some(p.to_lowercase()),
        Some(_) => bail!("ssl_certificate_file must be a string"),
    };

    match security_protocol.as_deref() {
        None => {}
        Some("ssl") => ssl_settings(&mut agg)?,
        Some("sasl_plaintext") => sasl_plaintext_kerberos_settings(&mut agg)?,
        Some(invalid_protocol) => bail!(
            "Invalid WITH options: security_protocol='{}'",
            invalid_protocol
        ),
    }

    Ok(())
}

// Filters `sql_parser::ast::Statement::CreateSource.with_options` for the
// configuration to connect to an SSL-secured cluster. You can find more detail
// about these settings in
// [librdkafka's documentation](https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka).
fn ssl_settings(agg: &mut ConfigAggregator) -> Result<(), failure::Error> {
    let allowed_configs = vec![
        Config::path("ssl_ca_location"),
        Config::path("ssl_certificate_location"),
        Config::path("ssl_key_location"),
        Config::string("ssl_key_password"),
    ];

    agg.extract(&allowed_configs)?;

    agg.insert_into_output("security.protocol".to_string(), "ssl".to_string());

    Ok(())
}

// Filters `sql_parser::ast::Statement::CreateSource.with_options` for the
// configuration to connect to a Kerberized Kafka cluster. You can find more
// detail about these settings in
// [librdkafka's documentation](https://github.com/edenhill/librdkafka/wiki/Using-SASL-with-librdkafka).
fn sasl_plaintext_kerberos_settings(agg: &mut ConfigAggregator) -> Result<(), failure::Error> {
    // Represents valid `with_option` keys to connect to Kerberized Kafka
    // cluster through SASL based on
    // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
    // Currently all of these keys can be converted to their respective
    // client config settings by replacing underscores with dots.
    //
    // Each option's default value are determined by `librdkafka`, and any
    // missing-but-necessary options are surfaced by `librdkafka` either
    // erroring or logging an error.
    let allowed_configs = vec![
        Config::path("sasl_kerberos_keytab"),
        Config::string("sasl_kerberos_kinit_cmd"),
        Config::string("sasl_kerberos_min_time_before_relogin"),
        Config::string("sasl_kerberos_principal"),
        Config::string("sasl_kerberos_service_name"),
        Config::string("sasl_mechanisms"),
    ];

    agg.extract(&allowed_configs)?;

    agg.insert_into_output(
        "security.protocol".to_string(),
        "sasl_plaintext".to_string(),
    );

    Ok(())
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
pub fn test_config(options: &HashMap<String, String>) -> Result<(), failure::Error> {
    let mut config = rdkafka::ClientConfig::new();
    for (k, v) in options {
        config.set(k, v);
    }

    match config.create_with_context(RDKafkaErrCheckContext::default()) {
        Ok(consumer) => {
            let consumer: BaseConsumer<RDKafkaErrCheckContext> = consumer;
            if let Ok(err_string) = consumer.context().error.lock() {
                if !err_string.is_empty() {
                    bail!("librdkafka: {}", *err_string)
                }
            };
        }
        Err(e) => {
            match e {
                rdkafka::error::KafkaError::ClientCreation(s) => {
                    // Rewrite error message to provide Materialize-specific guidance.
                    if s == "Invalid sasl.kerberos.kinit.cmd value: Property \
            not available: \"sasl.kerberos.keytab\""
                    {
                        bail!(
                            "Can't seem to find local keytab cache. You must \
                    provide explicit sasl_kerberos_keytab or \
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
#[derive(Clone, Default)]
struct RDKafkaErrCheckContext {
    pub error: Arc<Mutex<String>>,
}

impl rdkafka::consumer::ConsumerContext for RDKafkaErrCheckContext {}

impl rdkafka::client::ClientContext for RDKafkaErrCheckContext {
    // `librdkafka` doesn't seem to propagate all Kerberos errors up the stack,
    // but does log them, so we are currently relying on the `log` callback for
    // error handling in situations we're aware of, e.g. cannot log into
    // Kerberos.
    fn log(&self, level: rdkafka::config::RDKafkaLogLevel, fac: &str, log_message: &str) {
        use rdkafka::config::RDKafkaLogLevel::*;
        match level {
            Emerg | Alert | Critical | Error => {
                let mut err_string = self.error.lock().expect("lock poisoned");
                // Do not allow logging to overwrite other values if
                // present.
                if err_string.is_empty() {
                    *err_string = log_message.to_string();
                }
                error!(target: "librdkafka", "{} {}", fac, log_message)
            }
            Warning => warn!(target: "librdkafka", "{} {}", fac, log_message),
            Notice => info!(target: "librdkafka", "{} {}", fac, log_message),
            Info => info!(target: "librdkafka", "{} {}", fac, log_message),
            Debug => debug!(target: "librdkafka", "{} {}", fac, log_message),
        }
    }
    // Refer to the comment on the `log` callback.
    fn error(&self, error: rdkafka::error::KafkaError, reason: &str) {
        let mut err_string = self.error.lock().expect("lock poisoned");
        // Allow error to overwrite value irrespective of other conditions
        // (i.e. logging).
        *err_string = reason.to_string();
        error!("librdkafka: {}: {}", error, reason);
    }
}

// Generates a `ccsr::ClientConfig` based on the configuration extracted from
// `extract_security_config()`. Currently only supports SSL auth.
pub fn generate_ccsr_client_config(
    csr_url: Url,
    config_options: &HashMap<String, String>,
) -> Result<ccsr::ClientConfig, failure::Error> {
    let mut client_config = ccsr::ClientConfig::new(csr_url);

    if let Some(ca_path) = config_options.get("ssl.ca.location") {
        let mut ca_buf = Vec::new();
        File::open(ca_path)?.read_to_end(&mut ca_buf)?;
        let cert = Certificate::from_pem(&ca_buf)?;
        client_config = client_config.add_root_certificate(cert);
    }

    let key_path = config_options.get("ssl.key.location");
    let cert_path = config_options.get("ssl.certificate.location");
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
    Ok(client_config)
}
