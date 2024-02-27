// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Service orchestration for tracing-aware services.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
use std::sync::Arc;
#[cfg(feature = "tokio-console")]
use std::time::Duration;

use async_trait::async_trait;
use clap::{FromArgMatches, IntoApp};
use futures_core::stream::BoxStream;
use http::header::{HeaderName, HeaderValue};
use mz_build_info::BuildInfo;
#[cfg(feature = "tokio-console")]
use mz_orchestrator::ServicePort;
use mz_orchestrator::{
    NamespacedOrchestrator, Orchestrator, Service, ServiceConfig, ServiceEvent,
    ServiceProcessMetrics,
};
use mz_ore::cli::KeyValueArg;
use mz_ore::metrics::MetricsRegistry;
#[cfg(feature = "tokio-console")]
use mz_ore::netio::SocketAddr;
#[cfg(feature = "tokio-console")]
use mz_ore::tracing::TokioConsoleConfig;
use mz_ore::tracing::{
    OpenTelemetryConfig, SentryConfig, StderrLogConfig, StderrLogFormat, TracingConfig,
    TracingGuard, TracingHandle,
};
use mz_tracing::CloneableEnvFilter;
use opentelemetry::KeyValue;
use opentelemetry_sdk::resource::Resource;

/// Command line arguments for application tracing.
///
/// These arguments correspond directly to parameters in [`TracingConfig`], and
/// this type can be directly converted into a `TracingConfig` via the supplied
/// `From<TracingCliArgs>` implementation.
///
/// This logic is separated from `mz_ore::tracing` because the details of how
/// these command-line arguments are parsed and unparsed is specific to
/// orchestrators and does not belong in a foundational crate like `mz_ore`.
#[derive(Debug, Clone, clap::Parser)]
pub struct TracingCliArgs {
    /// Which tracing events to log to stderr.
    ///
    /// This value is a comma-separated list of filter directives. Each filter
    /// directive has the following format:
    ///
    /// ```text
    /// [module::path=]level
    /// ```
    ///
    /// A directive indicates that log messages from the specified module that
    /// are at least as severe as the specified level should be emitted. If a
    /// directive omits the module, then it implicitly applies to all modules.
    /// When directives conflict, the last directive wins. If a log message does
    /// not match any directive, it is not emitted.
    ///
    /// The module path of a log message reflects its location in Materialize's
    /// source code. Choosing module paths for filter directives requires
    /// familiarity with Materialize's codebase and is intended for advanced
    /// users. Note that module paths change frequency from release to release.
    ///
    /// The valid levels for a log message are, in increasing order of severity:
    /// trace, debug, info, warn, and error. The special level "off" may be used
    /// in a directive to suppress all log messages, even errors.
    ///
    /// The default value for this option is "info".
    #[clap(
        long,
        env = "STARTUP_LOG_FILTER",
        value_name = "FILTER",
        default_value = "info"
    )]
    pub startup_log_filter: CloneableEnvFilter,
    /// The format to use for stderr log messages.
    #[clap(long, env = "LOG_FORMAT", default_value_t, value_enum)]
    pub log_format: LogFormat,
    /// An optional prefix for each stderr log line.
    ///
    /// Only respected when `--log-format` is `text`.
    #[clap(long, env = "LOG_PREFIX")]
    pub log_prefix: Option<String>,
    /// Export OpenTelemetry tracing events to the provided endpoint.
    ///
    /// The specified endpoint should speak the OTLP/HTTP protocol. If the
    /// backend requires authentication, you can pass authentication metadata
    /// via the `--opentelemetry-header` option.
    #[clap(long, env = "OPENTELEMETRY_ENDPOINT")]
    pub opentelemetry_endpoint: Option<String>,
    /// A header to pass with every request to the OpenTelemetry endpoint
    /// specified by `--opentelemetry-endpoint` in the form `NAME=VALUE`.
    ///
    /// Requires that the `--opentelemetry-endpoint` option is specified.
    /// To specify multiple headers, either specify this option multiple times,
    /// or specify it once with multiple `NAME=VALUE` pairs separated by commas.
    #[clap(
        long,
        env = "OPENTELEMETRY_HEADER",
        requires = "opentelemetry-endpoint",
        value_name = "NAME=VALUE",
        use_value_delimiter = true
    )]
    pub opentelemetry_header: Vec<KeyValueArg<HeaderName, HeaderValue>>,
    /// Which tracing events to export to the OpenTelemetry endpoint specified
    /// by `--opentelemetry-endpoint`.
    ///
    /// The syntax of this option is the same as the syntax of the
    /// `--startup-log-filter` option.
    ///
    /// Requires that the `--opentelemetry-endpoint` option is specified.
    #[clap(
        long,
        env = "STARTUP_OPENTELEMETRY_FILTER",
        requires = "opentelemetry-endpoint",
        default_value = "off"
    )]
    pub startup_opentelemetry_filter: CloneableEnvFilter,
    /// Additional key-value pairs to send with all opentelemetry traces.
    /// Also used as Sentry tags.
    ///
    /// Requires that one of the `--opentelemetry-endpoint` or `--sentry-dsn`
    /// options is specified.
    #[clap(
        long,
        env = "OPENTELEMETRY_RESOURCE",
        value_name = "NAME=VALUE",
        use_value_delimiter = true
    )]
    pub opentelemetry_resource: Vec<KeyValueArg<String, String>>,
    /// The address on which to listen for Tokio console connections.
    ///
    /// For details about Tokio console, see: <https://github.com/tokio-rs/console>
    ///
    /// Requires that the `--tokio-console` option is specified.
    #[cfg(feature = "tokio-console")]
    #[clap(long, env = "TOKIO_CONSOLE_LISTEN_ADDR")]
    pub tokio_console_listen_addr: Option<SocketAddr>,
    /// How frequently to publish updates to Tokio console clients.
    ///
    /// Requires that the `--tokio-console` option is specified.
    #[cfg(feature = "tokio-console")]
    #[clap(
        long,
        env = "TOKIO_CONSOLE_PUBLISH_INTERVAL",
        requires = "tokio-console-listen-addr",
        parse(try_from_str = humantime::parse_duration),
        default_value = "1s",
    )]
    pub tokio_console_publish_interval: Duration,
    /// How long Tokio console data is retained for completed tasks.
    ///
    /// Requires that the `--tokio-console` option is specified.
    #[cfg(feature = "tokio-console")]
    #[clap(
        long,
        env = "TOKIO_CONSOLE_RETENTION",
        requires = "tokio-console-listen-addr",
        parse(try_from_str = humantime::parse_duration),
        default_value = "1h",
    )]
    pub tokio_console_retention: Duration,
    /// Sentry data source to submit events and exceptions (e.g. panics) to.
    #[clap(long, env = "SENTRY_DSN")]
    pub sentry_dsn: Option<String>,
    /// The environment name to report to Sentry.
    ///
    /// Ignored unless the `--sentry-dsn` option is specified.
    ///
    /// See: <https://docs.sentry.io/platforms/rust/configuration/options/#environment>
    #[clap(long, env = "SENTRY_ENVIRONMENT")]
    pub sentry_environment: Option<String>,
    /// Tags to send with all Sentry events, in addition to the tags specified by
    /// `--opentelemetry-resource`.
    ///
    /// Requires that the `--sentry-dsn` option is specified.
    #[clap(
        long,
        env = "SENTRY_TAG",
        value_name = "NAME=VALUE",
        use_value_delimiter = true
    )]
    pub sentry_tag: Vec<KeyValueArg<String, String>>,
}

impl Default for TracingCliArgs {
    fn default() -> TracingCliArgs {
        let matches = TracingCliArgs::command().get_matches_from::<_, OsString>([]);
        TracingCliArgs::from_arg_matches(&matches)
            .expect("no arguments produce valid TracingCliArgs")
    }
}

impl TracingCliArgs {
    pub async fn configure_tracing(
        &self,
        StaticTracingConfig {
            service_name,
            build_info,
        }: StaticTracingConfig,
        registry: MetricsRegistry,
    ) -> Result<(TracingHandle, TracingGuard), anyhow::Error> {
        mz_ore::tracing::configure(TracingConfig {
            service_name,
            stderr_log: StderrLogConfig {
                format: match self.log_format {
                    LogFormat::Text => StderrLogFormat::Text {
                        prefix: self.log_prefix.clone(),
                    },
                    LogFormat::Json => StderrLogFormat::Json,
                },
                filter: self.startup_log_filter.clone().into(),
            },
            opentelemetry: self.opentelemetry_endpoint.clone().map(|endpoint| {
                OpenTelemetryConfig {
                    endpoint,
                    headers: self
                        .opentelemetry_header
                        .iter()
                        .map(|header| (header.key.clone(), header.value.clone()))
                        .collect(),
                    filter: self.startup_opentelemetry_filter.clone().into(),
                    resource: Resource::new(
                        self.opentelemetry_resource
                            .iter()
                            .cloned()
                            .map(|kv| KeyValue::new(kv.key, kv.value)),
                    ),
                }
            }),
            #[cfg(feature = "tokio-console")]
            tokio_console: self.tokio_console_listen_addr.clone().map(|listen_addr| {
                TokioConsoleConfig {
                    listen_addr,
                    publish_interval: self.tokio_console_publish_interval,
                    retention: self.tokio_console_retention,
                }
            }),
            sentry: self.sentry_dsn.clone().map(|dsn| SentryConfig {
                dsn,
                environment: self.sentry_environment.clone(),
                tags: self
                    .opentelemetry_resource
                    .iter()
                    .cloned()
                    .chain(self.sentry_tag.iter().cloned())
                    .map(|kv| (kv.key, kv.value))
                    .collect(),
                event_filter: mz_service::tracing::mz_sentry_event_filter,
            }),
            build_version: build_info.version,
            build_sha: build_info.sha,
            build_time: build_info.time,
            registry,
        })
        .await
    }
}

/// The fields of [`TracingConfig`] that are not set by command-line arguments.
pub struct StaticTracingConfig {
    /// See [`TracingConfig::service_name`].
    pub service_name: &'static str,
    /// The build information for this service.
    pub build_info: BuildInfo,
}

/// Wraps an [`Orchestrator`] to inject tracing into all created services.
#[derive(Debug)]
pub struct TracingOrchestrator {
    inner: Arc<dyn Orchestrator>,
    tracing_args: TracingCliArgs,
}

impl TracingOrchestrator {
    /// Constructs a new tracing orchestrator.
    ///
    /// The orchestrator wraps the provided `inner` orchestrator. It mutates
    /// [`ServiceConfig`]s to inject the tracing configuration specified by
    /// `tracing_args`.
    ///
    /// All services created by the orchestrator **must** embed the
    /// [`TracingCliArgs`] in their command-line argument parser.
    pub fn new(inner: Arc<dyn Orchestrator>, tracing_args: TracingCliArgs) -> TracingOrchestrator {
        TracingOrchestrator {
            inner,
            tracing_args,
        }
    }
}

impl Orchestrator for TracingOrchestrator {
    fn namespace(&self, namespace: &str) -> Arc<dyn NamespacedOrchestrator> {
        Arc::new(NamespacedTracingOrchestrator {
            namespace: namespace.to_string(),
            inner: self.inner.namespace(namespace),
            tracing_args: self.tracing_args.clone(),
        })
    }
}

#[derive(Debug)]
struct NamespacedTracingOrchestrator {
    namespace: String,
    inner: Arc<dyn NamespacedOrchestrator>,
    tracing_args: TracingCliArgs,
}

#[async_trait]
impl NamespacedOrchestrator for NamespacedTracingOrchestrator {
    async fn fetch_service_metrics(
        &self,
        id: &str,
    ) -> Result<Vec<ServiceProcessMetrics>, anyhow::Error> {
        self.inner.fetch_service_metrics(id).await
    }

    async fn ensure_service(
        &self,
        id: &str,
        mut service_config: ServiceConfig<'_>,
    ) -> Result<Box<dyn Service>, anyhow::Error> {
        let args_fn = |listen_addrs: &BTreeMap<String, String>| {
            #[cfg(feature = "tokio-console")]
            let tokio_console_listen_addr = listen_addrs.get("tokio-console");
            let mut args = (service_config.args)(listen_addrs);
            let TracingCliArgs {
                startup_log_filter,
                log_prefix,
                log_format,
                opentelemetry_endpoint,
                opentelemetry_header,
                startup_opentelemetry_filter: _,
                opentelemetry_resource,
                #[cfg(feature = "tokio-console")]
                    tokio_console_listen_addr: _,
                #[cfg(feature = "tokio-console")]
                tokio_console_publish_interval,
                #[cfg(feature = "tokio-console")]
                tokio_console_retention,
                sentry_dsn,
                sentry_environment,
                sentry_tag,
            } = &self.tracing_args;
            args.push(format!("--startup-log-filter={startup_log_filter}"));
            args.push(format!("--log-format={log_format}"));
            if log_prefix.is_some() {
                args.push(format!("--log-prefix={}-{}", self.namespace, id));
            }
            if let Some(endpoint) = opentelemetry_endpoint {
                args.push(format!("--opentelemetry-endpoint={endpoint}"));
                for kv in opentelemetry_header {
                    args.push(format!(
                        "--opentelemetry-header={}={}",
                        kv.key,
                        kv.value
                            .to_str()
                            .expect("opentelemetry-header had non-ascii value"),
                    ));
                }
            }
            #[cfg(feature = "tokio-console")]
            if let Some(tokio_console_listen_addr) = tokio_console_listen_addr {
                args.push(format!(
                    "--tokio-console-listen-addr={}",
                    tokio_console_listen_addr,
                ));
                args.push(format!(
                    "--tokio-console-publish-interval={} us",
                    tokio_console_publish_interval.as_micros(),
                ));
                args.push(format!(
                    "--tokio-console-retention={} us",
                    tokio_console_retention.as_micros(),
                ));
            }
            if let Some(dsn) = sentry_dsn {
                args.push(format!("--sentry-dsn={dsn}"));
                for kv in sentry_tag {
                    args.push(format!("--sentry-tag={}={}", kv.key, kv.value));
                }
            }
            if let Some(environment) = sentry_environment {
                args.push(format!("--sentry-environment={environment}"));
            }

            if opentelemetry_endpoint.is_some() || sentry_dsn.is_some() {
                for kv in opentelemetry_resource {
                    args.push(format!("--opentelemetry-resource={}={}", kv.key, kv.value));
                }
            }

            args
        };
        service_config.args = &args_fn;
        #[cfg(feature = "tokio-console")]
        if self.tracing_args.tokio_console_listen_addr.is_some() {
            service_config.ports.push(ServicePort {
                name: "tokio-console".into(),
                port_hint: 6669,
            });
        }
        self.inner.ensure_service(id, service_config).await
    }

    async fn drop_service(&self, id: &str) -> Result<(), anyhow::Error> {
        self.inner.drop_service(id).await
    }

    async fn list_services(&self) -> Result<Vec<String>, anyhow::Error> {
        self.inner.list_services().await
    }

    fn watch_services(&self) -> BoxStream<'static, Result<ServiceEvent, anyhow::Error>> {
        self.inner.watch_services()
    }

    fn update_scheduling_config(
        &self,
        config: mz_orchestrator::scheduling_config::ServiceSchedulingConfig,
    ) {
        self.inner.update_scheduling_config(config)
    }
}

/// Specifies the format of a stderr log message.
#[derive(Debug, Clone, Default, clap::ValueEnum)]
pub enum LogFormat {
    /// Format as human readable, optionally colored text.
    ///
    /// Best suited for direct human consumption in a terminal.
    #[default]
    Text,
    /// Format as JSON (in reality, JSONL).
    ///
    /// Best suited for ingestion in structured logging aggregators.
    Json,
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogFormat::Text => f.write_str("text"),
            LogFormat::Json => f.write_str("json"),
        }
    }
}
