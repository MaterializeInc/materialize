// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Type-erased access to the user's context.
//!
//! The public types are generic over their context (`BaseConsumer<C>`), the
//! engines are not: they hold an `Arc<dyn Hooks>` (or `Arc<dyn ConsumerHooks>`)
//! obtained by coercing the client's `Arc<C>`. The threading contract of
//! [`ClientContext`] applies: `log`, `error`, `stats` and the consumer
//! callbacks are called inline and must not block; `resolve_broker_addr` and
//! `generate_oauth_token` may block and must be called through
//! [`resolve_broker_addr`] and [`generate_oauth_token`], which run them on a
//! blocking thread.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use crate::client::{ClientContext, OAuthToken};
use crate::config::RDKafkaLogLevel;
use crate::config::settings::{ClientSettings, DebugContext, DebugContexts};
use crate::consumer::{ConsumerContext, Rebalance};
use crate::error::{KafkaError, KafkaResult};
use crate::internal::runtime;
use crate::statistics::Statistics;
use crate::topic_partition_list::TopicPartitionList;

/// The [`ClientContext`] callbacks, object safe.
pub(crate) trait Hooks: Send + Sync + 'static {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str);
    fn error(&self, error: KafkaError, reason: &str);
    fn stats(&self, statistics: Statistics);
    /// May block. Call through [`resolve_broker_addr`].
    fn resolve_broker_addr(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>>;
    /// May block. Call through [`generate_oauth_token`]. Errors are rendered
    /// to strings because `Box<dyn Error>` is not `Send`.
    fn generate_oauth_token(&self, oauthbearer_config: Option<&str>) -> Result<OAuthToken, String>;
    /// [`ClientContext::ENABLE_REFRESH_OAUTH_TOKEN`].
    fn oauth_enabled(&self) -> bool;
}

impl<C: ClientContext> Hooks for C {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        ClientContext::log(self, level, fac, log_message)
    }

    fn error(&self, error: KafkaError, reason: &str) {
        ClientContext::error(self, error, reason)
    }

    fn stats(&self, statistics: Statistics) {
        ClientContext::stats(self, statistics)
    }

    fn resolve_broker_addr(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
        ClientContext::resolve_broker_addr(self, host, port)
    }

    fn generate_oauth_token(&self, oauthbearer_config: Option<&str>) -> Result<OAuthToken, String> {
        ClientContext::generate_oauth_token(self, oauthbearer_config).map_err(|e| e.to_string())
    }

    fn oauth_enabled(&self) -> bool {
        C::ENABLE_REFRESH_OAUTH_TOKEN
    }
}

/// The [`ConsumerContext`] callbacks, object safe. Called inline; must not
/// block.
pub(crate) trait ConsumerHooks: Hooks {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>);
    fn post_rebalance(&self, rebalance: &Rebalance<'_>);
    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList);
}

impl<C: ConsumerContext> ConsumerHooks for C {
    fn pre_rebalance(&self, rebalance: &Rebalance<'_>) {
        ConsumerContext::pre_rebalance(self, rebalance)
    }

    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        ConsumerContext::post_rebalance(self, rebalance)
    }

    fn commit_callback(&self, result: KafkaResult<()>, offsets: &TopicPartitionList) {
        ConsumerContext::commit_callback(self, result, offsets)
    }
}

/// Calls [`Hooks::resolve_broker_addr`] on a blocking thread.
pub(crate) async fn resolve_broker_addr(
    hooks: &Arc<dyn Hooks>,
    host: String,
    port: u16,
) -> io::Result<Vec<SocketAddr>> {
    let hooks = Arc::clone(hooks);
    runtime::run_blocking(move || hooks.resolve_broker_addr(&host, port)).await
}

/// Calls [`Hooks::generate_oauth_token`] on a blocking thread.
pub(crate) async fn generate_oauth_token(
    hooks: &Arc<dyn Hooks>,
    oauthbearer_config: Option<String>,
) -> Result<OAuthToken, String> {
    let hooks = Arc::clone(hooks);
    runtime::run_blocking(move || hooks.generate_oauth_token(oauthbearer_config.as_deref())).await
}

/// Forwards log lines to [`Hooks::log`] after librdkafka's filtering.
///
/// A line is delivered iff its level is at least as severe as the client's
/// log level. Debug lines additionally need their context enabled in the
/// `debug` setting.
#[derive(Clone)]
pub(crate) struct Logger {
    hooks: Arc<dyn Hooks>,
    level: RDKafkaLogLevel,
    debug: DebugContexts,
}

impl Logger {
    /// A logger with the client's configured level and debug contexts.
    pub(crate) fn new(hooks: Arc<dyn Hooks>, settings: &ClientSettings) -> Logger {
        Logger {
            hooks,
            level: settings.log_level,
            debug: settings.debug,
        }
    }

    /// A logger for client creation, which librdkafka gates by the `log_level`
    /// property instead of `ClientConfig::log_level`.
    pub(crate) fn for_creation(hooks: Arc<dyn Hooks>, settings: &ClientSettings) -> Logger {
        Logger {
            hooks,
            level: settings.creation_log_level,
            debug: settings.debug,
        }
    }

    /// Logs `settings.warnings` with facility `CONFWARN`.
    pub(crate) fn log_conf_warnings(&self, settings: &ClientSettings) {
        for (level, warning) in &settings.warnings {
            self.log(*level, "CONFWARN", warning);
        }
    }

    pub(crate) fn enabled(&self, level: RDKafkaLogLevel) -> bool {
        level.severity() <= self.level.severity()
    }

    pub(crate) fn debug_enabled(&self, ctx: DebugContext) -> bool {
        self.enabled(RDKafkaLogLevel::Debug) && self.debug.contains(ctx)
    }

    pub(crate) fn log(&self, level: RDKafkaLogLevel, fac: &str, msg: &str) {
        if self.enabled(level) {
            self.hooks.log(level, fac, msg);
        }
    }

    /// Like [`Logger::log`], formatting only if the line is delivered.
    pub(crate) fn log_fmt(&self, level: RDKafkaLogLevel, fac: &str, args: fmt::Arguments<'_>) {
        if self.enabled(level) {
            self.hooks.log(level, fac, &args.to_string());
        }
    }

    /// Logs a debug line in context `ctx`, formatting only if delivered.
    pub(crate) fn debug(&self, ctx: DebugContext, fac: &str, args: fmt::Arguments<'_>) {
        if self.debug_enabled(ctx) {
            self.hooks
                .log(RDKafkaLogLevel::Debug, fac, &args.to_string());
        }
    }

    /// The hooks lines are forwarded to.
    pub(crate) fn hooks(&self) -> &Arc<dyn Hooks> {
        &self.hooks
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use std::error::Error;
    use std::sync::Mutex;

    use super::*;
    use crate::config::ClientConfig;
    use crate::config::settings::ClientKind;
    use crate::consumer::DefaultConsumerContext;

    #[derive(Default)]
    struct Recorder {
        lines: Mutex<Vec<(RDKafkaLogLevel, String, String)>>,
    }

    impl ClientContext for Recorder {
        const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

        fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
            self.lines
                .lock()
                .unwrap()
                .push((level, fac.to_string(), log_message.to_string()));
        }

        fn resolve_broker_addr(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
            assert!(!runtime::is_io_thread());
            Ok(vec![SocketAddr::from((
                [127, 0, 0, 1],
                port + u16::try_from(host.len()).unwrap(),
            ))])
        }

        fn generate_oauth_token(&self, config: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
            match config {
                Some(c) => Ok(OAuthToken {
                    token: c.to_string(),
                    principal_name: String::new(),
                    lifetime_ms: 1,
                }),
                None => Err("no config".into()),
            }
        }
    }

    fn settings(pairs: &[(&str, &str)], level: RDKafkaLogLevel) -> ClientSettings {
        let mut config = ClientConfig::new();
        config.set_log_level(level);
        for (k, v) in pairs {
            config.set(*k, *v);
        }
        ClientSettings::from_config(&config, ClientKind::Consumer).unwrap()
    }

    #[mz_ore::test]
    fn logger_filters() {
        let recorder = Arc::new(Recorder::default());
        let hooks: Arc<dyn Hooks> = Arc::<Recorder>::clone(&recorder);

        let logger = Logger::new(Arc::clone(&hooks), &settings(&[], RDKafkaLogLevel::Warning));
        logger.log(RDKafkaLogLevel::Error, "FAIL", "e");
        logger.log(RDKafkaLogLevel::Warning, "OFFSET", "w");
        logger.log(RDKafkaLogLevel::Info, "X", "dropped");
        logger.log_fmt(RDKafkaLogLevel::Critical, "Y", format_args!("{}", 1));
        logger.debug(DebugContext::Broker, "Z", format_args!("dropped"));

        let debug_settings = settings(&[("debug", "broker")], RDKafkaLogLevel::Debug);
        let logger = Logger::new(Arc::clone(&hooks), &debug_settings);
        logger.debug(DebugContext::Broker, "BROKER", format_args!("kept"));
        logger.debug(DebugContext::Fetch, "FETCH", format_args!("dropped"));
        assert!(logger.debug_enabled(DebugContext::Broker));

        // Debug contexts without the debug level are dropped.
        let logger = Logger::new(
            Arc::clone(&hooks),
            &settings(&[("debug", "all")], RDKafkaLogLevel::Info),
        );
        logger.debug(DebugContext::Broker, "BROKER", format_args!("dropped"));

        let lines: Vec<_> = recorder
            .lines
            .lock()
            .unwrap()
            .iter()
            .map(|(_, fac, msg)| format!("{fac} {msg}"))
            .collect();
        assert_eq!(lines, ["FAIL e", "OFFSET w", "Y 1", "BROKER kept"]);
    }

    #[mz_ore::test]
    fn conf_warnings_use_creation_level() {
        let recorder = Arc::new(Recorder::default());
        let hooks: Arc<dyn Hooks> = Arc::<Recorder>::clone(&recorder);
        let s = settings(&[], RDKafkaLogLevel::Error);
        Logger::for_creation(hooks, &s).log_conf_warnings(&s);
        let lines = recorder.lines.lock().unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].0, RDKafkaLogLevel::Notice);
        assert_eq!(lines[0].1, "CONFWARN");
    }

    #[mz_ore::test]
    fn blocking_hooks() {
        let hooks: Arc<dyn Hooks> = Arc::new(Recorder::default());
        assert!(hooks.oauth_enabled());
        let addrs = runtime::block_on({
            let hooks = Arc::clone(&hooks);
            async move { resolve_broker_addr(&hooks, "abc".into(), 9000).await }
        })
        .unwrap();
        assert_eq!(addrs, [SocketAddr::from(([127, 0, 0, 1], 9003))]);

        let token = runtime::block_on({
            let hooks = Arc::clone(&hooks);
            async move { generate_oauth_token(&hooks, Some("tok".into())).await }
        })
        .unwrap();
        assert_eq!(token.token, "tok");
        let err = runtime::block_on({
            let hooks = Arc::clone(&hooks);
            async move { generate_oauth_token(&hooks, None).await }
        });
        assert_eq!(err.err(), Some("no config".to_string()));

        let default: Arc<dyn ConsumerHooks> = Arc::new(DefaultConsumerContext);
        assert!(!default.oauth_enabled());
        assert_eq!(
            default.generate_oauth_token(None).err().as_deref(),
            Some("Default implementation of generate_oauth_token must be overridden")
        );
        default.commit_callback(Ok(()), &TopicPartitionList::new());
        let upcast: Arc<dyn Hooks> = default;
        assert!(!upcast.oauth_enabled());
    }
}
