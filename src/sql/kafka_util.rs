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
use std::fs::metadata;
use std::sync::{Arc, Mutex};

use failure::bail;
use log::{debug, error, info, warn};
use rdkafka::consumer::BaseConsumer;
use rdkafka::ClientConfig;
use sql_parser::ast::Value;

/// Parse the `with_options` from a `CREATE SOURCE` statement to determine Kafka
/// security strategy, and extract any additional supplied configurations.
///
/// # Errors
///
/// - Invalid values for known options, such as files that do not exist for
/// expected file paths.
/// - If any of the values in `with_options` are not
///   `sql_parser::ast::Value::SingleQuotedString`.
pub fn extract_security_options(
    mut with_options: &mut HashMap<String, Value>,
) -> Result<HashMap<String, String>, failure::Error> {
    let security_protocol = match with_options.remove("security_protocol") {
        None => None,
        Some(Value::SingleQuotedString(p)) => Some(p.to_lowercase()),
        Some(_) => bail!("ssl_certificate_file must be a string"),
    };

    let options: HashMap<String, String> = match security_protocol.as_deref() {
        None => HashMap::new(),
        Some("ssl") => ssl_settings(&mut with_options)?,
        Some("sasl_plaintext") => sasl_plaintext_kerberos_settings(&mut with_options)?,
        Some(invalid_protocol) => bail!(
            "Invalid WITH options: security_protocol='{}'",
            invalid_protocol
        ),
    };

    Ok(options)
}

// Filters `sql_parser::ast::Statement::CreateSource.with_options` for the
// configuration to connect to an SSL-secured cluster. You can find more detail
// about these settings in [librdkafka's
// documentation](https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka).
fn ssl_settings(
    mut with_options: &mut HashMap<String, Value>,
) -> Result<HashMap<String, String>, failure::Error> {
    let allowed_settings = vec![
        ("ssl_ca_location", true),
        ("ssl_certificate_location", true),
        ("ssl_key_location", true),
        ("ssl_key_password", false),
    ];
    let mut specified_options =
        extract_settings_from_with_options(&mut with_options, &allowed_settings)?;

    specified_options.insert("security.protocol".to_string(), "ssl".to_string());

    Ok(specified_options)
}

// Filters `sql_parser::ast::Statement::CreateSource.with_options` for the
// configuration to connect to a
// Kerberized Kafka cluster. You can find more detail
// about these settings in [librdkafka's
// documentation](https://github.com/edenhill/librdkafka/wiki/Using-SASL-with-librdkafka).
fn sasl_plaintext_kerberos_settings(
    mut with_options: &mut HashMap<String, Value>,
) -> Result<HashMap<String, String>, failure::Error> {
    // Represents valid `with_option` keys to connect to Kerberized Kafka
    // cluster through SASL based on
    // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md.
    // Currently all of these keys can be converted to their respective
    // client config settings by replacing underscores with dots.
    //
    // Each option's default value are determined by `librdkafka`, and any
    // missing-but-necessary options are surfaced by `librdkafka` either
    // erroring or logging an error.
    let allowed_settings = vec![
        ("sasl_kerberos_keytab", true),
        ("sasl_kerberos_kinit_cmd", false),
        ("sasl_kerberos_min_time_before_relogin", false),
        ("sasl_kerberos_principal", false),
        ("sasl_kerberos_service_name", false),
        ("sasl_mechanisms", false),
    ];

    let mut specified_options =
        extract_settings_from_with_options(&mut with_options, &allowed_settings)?;
    specified_options.insert(
        "security.protocol".to_string(),
        "sasl_plaintext".to_string(),
    );

    Ok(specified_options)
}

// Filters `with_options` on `allowed_settings.0`, and performs a check if files
// exist if key exists and `allowed_settings.1==true`.
fn extract_settings_from_with_options(
    with_options: &mut HashMap<String, Value>,
    allowed_settings: &[(&str, bool)],
) -> Result<HashMap<String, String>, failure::Error> {
    let mut specified_options: HashMap<String, String> = HashMap::new();

    for setting in allowed_settings {
        // See if option name was specified.
        match with_options.remove(setting.0) {
            Some(Value::SingleQuotedString(v)) => {
                // If setting is a path, ensure that it is valid.
                if setting.1 && metadata(&v).is_err() {
                    bail!(
                        "Invalid WITH option: {}='{}', file does not exist",
                        setting.0,
                        v
                    )
                }
                // Track options and values.
                specified_options.insert(setting.0.replace("_", "."), v);
            }
            Some(_) => bail!("{}'s value must be a single-quoted string", setting.0),
            None => {}
        }
    }

    Ok(specified_options)
}

/// Create a new
/// [`rdkafka::ClientConfig`](https://docs.rs/rdkafka/latest/rdkafka/config/struct.ClientConfig.html)
/// with the provided
/// [`options`]](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md),
/// and test its ability to create an
/// [`rdkafka::consumer::BaseConsumer`](https://docs.rs/rdkafka/latest/rdkafka/consumer/base_consumer/struct.BaseConsumer.html).
///
/// Expected to test the output of `extract_security_options`.
///
/// # Errors
///
/// - `librdkafka` cannot create a BaseConsumer using the provided `options`.
///   For example, when using Kerberos auth, and the named principal does not
///   exist.
pub fn test_config(options: &HashMap<String, String>) -> Result<(), failure::Error> {
    let mut config = ClientConfig::new();
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
