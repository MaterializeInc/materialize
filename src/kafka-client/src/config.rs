// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Producer and consumer configuration.
//!
//! [`ClientConfig`] is an untyped map of librdkafka property names to string
//! values, as in rust-rdkafka. Client creation parses it into the typed
//! settings of the `settings` module, applying librdkafka's validation,
//! defaults and error texts.

use std::collections::BTreeMap;
use std::fmt;
use std::iter::FromIterator;

use tracing::Level;

use crate::client::ClientContext;
use crate::error::KafkaResult;

pub(crate) mod settings;

pub(crate) use settings::{ClientKind, ClientSettings};

/// The log levels supported by librdkafka, in syslog order.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum RDKafkaLogLevel {
    /// More severe than [`Level::ERROR`].
    Emerg = 0,
    /// More severe than [`Level::ERROR`].
    Alert = 1,
    /// More severe than [`Level::ERROR`].
    Critical = 2,
    /// Equivalent to [`Level::ERROR`].
    Error = 3,
    /// Equivalent to [`Level::WARN`].
    Warning = 4,
    /// More severe than [`Level::INFO`].
    Notice = 5,
    /// Equivalent to [`Level::INFO`].
    Info = 6,
    /// Equivalent to [`Level::DEBUG`].
    Debug = 7,
}

impl RDKafkaLogLevel {
    /// The syslog severity: lower is more severe.
    pub(crate) fn severity(self) -> u8 {
        match self {
            RDKafkaLogLevel::Emerg => 0,
            RDKafkaLogLevel::Alert => 1,
            RDKafkaLogLevel::Critical => 2,
            RDKafkaLogLevel::Error => 3,
            RDKafkaLogLevel::Warning => 4,
            RDKafkaLogLevel::Notice => 5,
            RDKafkaLogLevel::Info => 6,
            RDKafkaLogLevel::Debug => 7,
        }
    }
}

/// Client configuration: librdkafka property names mapped to string values.
///
/// Properties are validated when a client is created, not when they are set.
/// Unknown properties are rejected with librdkafka's message, except for a
/// documented set of librdkafka tuning properties that are accepted and
/// ignored (see the `settings` module).
#[derive(Clone)]
pub struct ClientConfig {
    conf_map: BTreeMap<String, String>,
    /// The librdkafka logging level. Refer to [`RDKafkaLogLevel`] for the list
    /// of available levels.
    pub log_level: RDKafkaLogLevel,
}

// The clippy.toml ban on `ClientConfig::new` is meant for users of this crate.
#[allow(clippy::disallowed_methods)]
impl Default for ClientConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for ClientConfig {
    /// Prints the configuration with the values of sensitive properties
    /// (passwords, private keys) redacted.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Redacted<'a>(&'a BTreeMap<String, String>);
        impl fmt::Debug for Redacted<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_map()
                    .entries(self.0.iter().map(|(k, v)| {
                        let v = if settings::is_sensitive(k) {
                            "[redacted]"
                        } else {
                            v.as_str()
                        };
                        (k, v)
                    }))
                    .finish()
            }
        }
        f.debug_struct("ClientConfig")
            .field("conf_map", &Redacted(&self.conf_map))
            .field("log_level", &self.log_level)
            .finish()
    }
}

impl ClientConfig {
    /// Creates a new empty configuration.
    ///
    /// The log level is the most verbose level enabled for the tracing target
    /// `librdkafka`, but no more verbose than [`RDKafkaLogLevel::Debug`] and no
    /// less than [`RDKafkaLogLevel::Error`].
    pub fn new() -> ClientConfig {
        ClientConfig {
            conf_map: BTreeMap::new(),
            log_level: log_level_from_global_config(),
        }
    }

    /// Gets the value of a parameter in the configuration.
    ///
    /// This method reflects only the state of the `ClientConfig` itself, and
    /// does not reflect defaults or aliases.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.conf_map.get(key).map(|val| val.as_str())
    }

    /// Sets a parameter in the configuration.
    ///
    /// If there is an existing value for `key` in the configuration, it is
    /// overridden with the new `value`.
    pub fn set<K, V>(&mut self, key: K, value: V) -> &mut ClientConfig
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.conf_map.insert(key.into(), value.into());
        self
    }

    /// Removes a parameter from the configuration.
    pub fn remove<'a>(&'a mut self, key: &str) -> &'a mut ClientConfig {
        self.conf_map.remove(key);
        self
    }

    /// Sets the log level of the client. If not specified, the log level will
    /// be calculated based on the global log level of the tracing target
    /// `librdkafka`.
    pub fn set_log_level(&mut self, log_level: RDKafkaLogLevel) -> &mut ClientConfig {
        self.log_level = log_level;
        self
    }

    /// Uses the current configuration to create a new Consumer or Producer.
    pub fn create<T: FromClientConfig>(&self) -> KafkaResult<T> {
        T::from_config(self)
    }

    /// Uses the current configuration and the provided context to create a new
    /// Consumer or Producer.
    pub fn create_with_context<C, T>(&self, context: C) -> KafkaResult<T>
    where
        C: ClientContext,
        T: FromClientConfigAndContext<C>,
    {
        T::from_config_and_context(self, context)
    }

    /// Iterates over all set properties in key order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.conf_map.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}

impl FromIterator<(String, String)> for ClientConfig {
    fn from_iter<I>(iter: I) -> ClientConfig
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let mut config = ClientConfig::default();
        config.extend(iter);
        config
    }
}

impl Extend<(String, String)> for ClientConfig {
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (String, String)>,
    {
        self.conf_map.extend(iter)
    }
}

fn log_level_from_global_config() -> RDKafkaLogLevel {
    if tracing::enabled!(target: "librdkafka", Level::DEBUG) {
        RDKafkaLogLevel::Debug
    } else if tracing::enabled!(target: "librdkafka", Level::INFO) {
        RDKafkaLogLevel::Info
    } else if tracing::enabled!(target: "librdkafka", Level::WARN) {
        RDKafkaLogLevel::Warning
    } else {
        RDKafkaLogLevel::Error
    }
}

/// Create a new client based on the provided configuration.
pub trait FromClientConfig: Sized {
    /// Creates a client from a client configuration. The default client context
    /// will be used.
    fn from_config(_: &ClientConfig) -> KafkaResult<Self>;
}

/// Create a new client based on the provided configuration and context.
pub trait FromClientConfigAndContext<C: ClientContext>: Sized {
    /// Creates a client from a client configuration and a client context.
    fn from_config_and_context(_: &ClientConfig, _: C) -> KafkaResult<Self>;
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn set_get_remove_extend() {
        let mut config: ClientConfig = vec![("a".into(), "1".into()), ("b".into(), "1".into())]
            .into_iter()
            .collect();
        config.extend([("b".into(), "2".into()), ("c".into(), "3".into())]);
        assert_eq!(config.get("a").unwrap(), "1");
        assert_eq!(config.get("b").unwrap(), "2");
        assert_eq!(config.get("c").unwrap(), "3");
        config.remove("a").set("d", "4");
        assert_eq!(config.get("a"), None);
        assert_eq!(config.get("d"), Some("4"));
        config.set_log_level(RDKafkaLogLevel::Warning);
        assert_eq!(config.log_level, RDKafkaLogLevel::Warning);
        let keys: Vec<_> = config.iter().map(|(k, _)| k).collect();
        assert_eq!(keys, ["b", "c", "d"]);
    }

    #[mz_ore::test]
    fn debug_redacts_secrets() {
        let mut config = ClientConfig::new();
        config
            .set("sasl.password", "hunter2")
            .set("ssl.key.pem", "-----BEGIN PRIVATE KEY-----")
            .set("sasl.username", "user");
        let debug = format!("{config:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(!debug.contains("PRIVATE KEY"), "{debug}");
        assert!(debug.contains("user"), "{debug}");
    }
}
