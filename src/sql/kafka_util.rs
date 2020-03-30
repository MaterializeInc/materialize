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

/// Parse the `with_options` from a `CREATE SOURCE` statement to determine
/// Kafka security strategy.
///
/// # Arguments
///
/// - `with_options` should be the `with_options` field of
///   `sql_parser::ast::Statement::CreateSource`.
///
/// # Errors
///
/// If the user specified...
///
/// - Incompatible options
/// - Invalid values for known options
pub fn extract_security_options(
    mut with_options: &mut HashMap<String, Value>,
) -> Result<Vec<(String, String)>, failure::Error> {
    let security_protocol = match with_options.remove("security_protocol") {
        None => None,
        Some(Value::SingleQuotedString(p)) => Some(p),
        Some(_) => bail!("ssl_certificate_file must be a string"),
    };

    let ssl_certificate_file = match with_options.remove("ssl_certificate_file") {
        None => None,
        Some(Value::SingleQuotedString(p)) => {
            if metadata(&p).is_err() {
                bail!(
                    "Invalid WITH options: ssl_certificate_file='{}', file does not exist",
                    p
                )
            }
            Some(p)
        }
        Some(_) => bail!("ssl_certificate_file must be a string"),
    };

    let options: Vec<(String, String)> = match (security_protocol.as_deref(), ssl_certificate_file)
    {
        (None, None) => Vec::new(),
        (Some("sasl_plaintext"), None) => sasl_plaintext_kerberos_settings(&mut with_options)?,
        (Some("ssl"), Some(path)) | (None, Some(path)) => vec![
            // See https://github.com/edenhill/librdkafka/wiki/Using-SSL-with-librdkafka
            // for more details on this librdkafka option
            ("security.protocol".to_string(), "ssl".to_string()),
            ("ssl.ca.location".to_string(), path),
        ],
        (Some(invalid_protocol), Some(path)) => bail!(
            "Invalid WITH options: security_protocol='{}', ssl_certificate_file='{}'",
            invalid_protocol,
            path
        ),
        (Some(invalid_protocol), None) => bail!(
            "Invalid WITH options: security_protocol='{}'",
            invalid_protocol
        ),
    };

    Ok(options)
}

/// Return a list of key-value pairs to authenticate `rdkafka` to connect to a
/// Kerberized Kafka cluster. You can find more detail about these settings in
/// [librdkafka's
/// documentation](https://github.com/edenhill/librdkafka/wiki/Using-SASL-with-librdkafka).
///
/// # Arguments
///
/// - `with_options` should be the `with_options` field of
///   `sql_parser::ast::Statement::CreateSource`, where the user has passed in
///   their options to connect to the Kerberized Kafka cluster.
///
/// # Errors
///
/// - If any of the values in `with_options` are not
///   `sql_parser::ast::Value::SingleQuotedString`.
///
fn sasl_plaintext_kerberos_settings(
    with_options: &mut HashMap<String, Value>,
) -> Result<Vec<(String, String)>, failure::Error> {
    let mut specified_options: Vec<(String, String)> = vec![(
        "security.protocol".to_string(),
        "sasl_plaintext".to_string(),
    )];

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
        "sasl_kerberos_keytab",
        "sasl_kerberos_kinit_cmd",
        "sasl_kerberos_min_time_before_relogin",
        "sasl_kerberos_principal",
        "sasl_kerberos_service_name",
        "sasl_mechanisms",
    ];
    for setting in allowed_settings {
        match with_options.remove(&setting.to_string()) {
            Some(Value::SingleQuotedString(v)) => {
                specified_options.push((setting.replace("_", "."), v));
            }
            Some(_) => bail!("{}'s value must be a single-quoted string", setting),
            None => {}
        };
    }

    Ok(specified_options)
}

/// Create a new
/// [`rdkafka::ClientConfig`](https://docs.rs/rdkafka/latest/rdkafka/config/struct.ClientConfig.html)
/// with the provided `options,` and test its ability to create an
/// [`rdkafka::consumer::BaseConsumer`](https://docs.rs/rdkafka/latest/rdkafka/consumer/base_consumer/struct.BaseConsumer.html).
///
/// # Arguments
///
/// - `options` should be slice of tuples with the structure `(setting name,
///   value)`. You can find valid setting names in [`librdkafka`'s
///   documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md).
///
///   `extract_security_options`' output is viable for input.
///
/// # Errors
///
/// - `librdkafka` cannot create a BaseConsumer using the provided `options`. For
///   example, when using Kerberos auth, and the named principal does not exist.
pub fn test_config(options: &[(String, String)]) -> Result<(), failure::Error> {
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
