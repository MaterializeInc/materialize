// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Tracing utilities.
//!
//! This module contains application tracing utilities built on top of the
//! [`tracing`] and [`opentelemetry`] libraries. The key exports are:
//!
//!  * The **[`configure`]** function, which configures the `tracing` and
//!    `opentelemetry` crates with sensible defaults and should be called during
//!    initialization of every Materialize binary.
//!
//!  * The **[`OpenTelemetryContext`]** type, which carries a tracing span
//!    across thread or task boundaries within a process.

use std::borrow::Cow;
use std::collections::HashMap;
use std::io;
#[cfg(feature = "tokio-console")]
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::bail;
#[cfg(feature = "tokio-console")]
use console_subscriber::ConsoleLayer;
use http::HeaderMap;
use hyper::client::HttpConnector;
use hyper_tls::HttpsConnector;
use opentelemetry::global;
use opentelemetry::propagation::{Extractor, Injector};
use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::{trace, Resource};
use opentelemetry::KeyValue;
use tonic::metadata::MetadataMap;
use tonic::transport::Endpoint;
use tracing::{Event, Level, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::Targets;
use tracing_subscriber::fmt::format::{format, Writer};
use tracing_subscriber::fmt::{self, FmtContext, FormatEvent, FormatFields};
use tracing_subscriber::layer::{Layer, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::reload;
use tracing_subscriber::util::SubscriberInitExt;

/// Application tracing configuration.
///
/// See the [`configure`] function for details.
#[derive(Debug, Clone)]
pub struct TracingConfig {
    /// Configuration of the stderr log.
    pub stderr_log: StderrLogConfig,
    /// Optional configuration for the [`opentelemetry`] library.
    /// The boolean specifies if you wish to enable the OpenTelemetry
    /// collector by default
    pub opentelemetry: Option<OpenTelemetryConfig>,
    /// Optional configuration for the [Tokio console] integration.
    ///
    /// [Tokio console]: https://github.com/tokio-rs/console
    #[cfg_attr(nightly_doc_features, doc(cfg(feature = "tokio-console")))]
    #[cfg(feature = "tokio-console")]
    pub tokio_console: Option<TokioConsoleConfig>,
    /// Optional Sentry configuration.
    pub sentry: Option<SentryConfig>,
}

/// Configures Sentry reporting.
#[derive(Debug, Clone)]
pub struct SentryConfig {
    /// Sentry data source name to submit events to.
    pub dsn: String,
    /// Additional tags to include on each Sentry event/exception.
    pub tags: HashMap<String, String>,
}

/// Configures the stderr log.
#[derive(Debug, Clone)]
pub struct StderrLogConfig {
    /// Whether to prefix each log line with the service name.
    /// An optional prefix for each stderr log line.
    pub prefix: Option<String>,
    /// A filter which determines which events are emitted to the log.
    pub filter: Targets,
}

/// Callbacks used to dynamically modify tracing-related filters
#[derive(Debug, Clone)]
pub struct TracingTargetCallbacks {
    /// Modifies filter targets for OpenTelemetry tracing
    pub tracing: DynamicTargetsCallback,
    /// Modifies filter targets for stderr logging
    pub stderr: DynamicTargetsCallback,
}

impl Default for TracingTargetCallbacks {
    fn default() -> Self {
        Self {
            tracing: DynamicTargetsCallback::none(),
            stderr: DynamicTargetsCallback::none(),
        }
    }
}

/// A callback used to modify tracing filters
pub struct DynamicTargetsCallback {
    callback: Arc<dyn Fn(Targets) -> Result<(), anyhow::Error> + Send + Sync>,
}

impl DynamicTargetsCallback {
    /// Updates stderr log filtering with `targets`
    pub fn call(&self, targets: Targets) -> Result<(), anyhow::Error> {
        (self.callback)(targets)
    }

    /// A callback that does nothing. Useful for tests.
    pub fn none() -> Self {
        Self {
            callback: Arc::new(|_| Ok(())),
        }
    }
}

impl std::fmt::Debug for DynamicTargetsCallback {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("StderrLogFilterCallback")
            .finish_non_exhaustive()
    }
}

impl Clone for DynamicTargetsCallback {
    fn clone(&self) -> Self {
        DynamicTargetsCallback {
            callback: Arc::clone(&self.callback),
        }
    }
}

/// Configuration for the [`opentelemetry`] library.
#[derive(Debug, Clone)]
pub struct OpenTelemetryConfig {
    /// The [OTLP/HTTP] endpoint to export OpenTelemetry data to.
    ///
    /// [OTLP/HTTP]: https://github.com/open-telemetry/opentelemetry-specification/blob/b13c1648bae16323868a5caf614bc10c917cc6ca/specification/protocol/otlp.md#otlphttp
    pub endpoint: String,
    /// Additional headers to send with every request to the endpoint.
    pub headers: HeaderMap,
    /// A filter which determines which events are exported.
    pub filter: Targets,
    /// `opentelemetry::sdk::resource::Resource` to include with all
    /// traces.
    pub resource: Resource,
    /// Whether to startup with the dynamic OpenTelemetry layer enabled
    pub start_enabled: bool,
}

/// Configuration of the [Tokio console] integration.
///
/// [Tokio console]: https://github.com/tokio-rs/console
#[cfg_attr(nightly_doc_features, doc(cfg(feature = "tokio-console")))]
#[cfg(feature = "tokio-console")]
#[derive(Debug, Clone)]
pub struct TokioConsoleConfig {
    /// The address on which to listen for Tokio console connections.
    ///
    /// See [`console_subscriber::Builder::server_addr`].
    pub listen_addr: SocketAddr,
    /// How frequently to publish updates to clients.
    ///
    /// See [`console_subscriber::Builder::publish_interval`].
    pub publish_interval: Duration,
    /// How long data is retained for completed tasks.
    ///
    /// See [`console_subscriber::Builder::retention`].
    pub retention: Duration,
}

/// Enables application tracing via the [`tracing`] and [`opentelemetry`]
/// libraries.
///
/// The `tracing` library is configured to emit events as textual log lines to
/// stderr. [`StderrLogConfig`] offer a small degree of control over this
/// behavior.
///
/// If the `opentelemetry` parameter is `Some`, the `tracing` library is
/// additionally configured to export events to an observability backend, like
/// [Jaeger] or [Honeycomb].
///
/// The `tokio_console` parameter enables integration with the [Tokio console].
/// When enabled, `tracing` events are collected and made available to the Tokio
/// console via a server running on port
///
/// [Jaeger]: https://jaegertracing.io
/// [Honeycomb]: https://www.honeycomb.io
/// [Tokio console]: https://github.com/tokio-rs/console
// Setting up OpenTelemetry in the background requires we are in a Tokio runtime
// context, hence the `async`.
#[allow(clippy::unused_async)]
pub async fn configure<C, F>(
    service_name: &str,
    config: C,
    // _Effectively_ unused if `sentry_config` is not set in the `config`,
    // as its not dynamically configured and is likely a closure with
    // an opaque type.
    sentry_event_filter: F,
    (build_version, build_sha, build_time): (&str, &str, &str),
) -> Result<(TracingTargetCallbacks, Option<sentry::ClientInitGuard>), anyhow::Error>
where
    C: Into<TracingConfig>,
    F: Fn(&tracing::Metadata<'_>) -> sentry_tracing::EventFilter + Send + Sync + 'static,
{
    let service_name = service_name.to_string();

    let config = config.into();
    // See: https://no-color.org/
    let no_color = std::env::var_os("NO_COLOR").unwrap_or_else(|| "".into()) != "";
    let stderr_log_layer = fmt::layer()
        .event_format(PrefixFormat {
            inner: format(),
            prefix: config.stderr_log.prefix,
        })
        .with_writer(io::stderr)
        .with_ansi(!no_color && atty::is(atty::Stream::Stderr))
        .with_filter(config.stderr_log.filter);

    let (stderr_log_layer, stderr_reloader) = reload::Layer::new(stderr_log_layer);
    let stderr_callback = DynamicTargetsCallback {
        callback: Arc::new(move |targets| {
            stderr_reloader.modify(|layer| *layer.filter_mut() = targets)?;
            Ok(())
        }),
    };

    let (otel_layer, otel_reloader) = if let Some(otel_config) = config.opentelemetry {
        // TODO(guswynn): figure out where/how to call
        // opentelemetry::global::shutdown_tracer_provider();
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        // Manually set up an OpenSSL-backed, h2, proxied `Channel`,
        // with the timeout configured according to:
        // https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/struct.TonicExporterBuilder.html#method.with_channel
        let channel = Endpoint::from_shared(otel_config.endpoint)?
            .timeout(Duration::from_secs(
                opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
            ))
            // TODO(guswynn): investigate if this should be non-lazy.
            .connect_with_connector_lazy({
                let mut http = HttpConnector::new();
                http.enforce_http(false);
                HttpsConnector::from((
                    http,
                    // This is the same as the default, plus an h2 ALPN request.
                    tokio_native_tls::TlsConnector::from(
                        native_tls::TlsConnector::builder()
                            .request_alpns(&["h2"])
                            .build()
                            .unwrap(),
                    ),
                ))
            });
        let exporter = opentelemetry_otlp::new_exporter()
            .tonic()
            .with_channel(channel)
            .with_metadata(MetadataMap::from_headers(otel_config.headers));
        let tracer = opentelemetry_otlp::new_pipeline()
            .tracing()
            .with_trace_config(
                trace::config().with_resource(
                    // The latter resources wins, so if the user specifies `service.name` on the
                    // cli, it wins
                    Resource::new([KeyValue::new("service.name", service_name.clone())])
                        .merge(&otel_config.resource),
                ),
            )
            .with_exporter(exporter)
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();
        let (filter, filter_handle) = reload::Layer::new(if otel_config.start_enabled {
            otel_config.filter
        } else {
            // the default `Targets` has everything disabled
            Targets::default()
        });
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(filter);
        let reloader = DynamicTargetsCallback {
            callback: Arc::new(move |targets| {
                // This code should be kept panic-free to
                // avoid weird poisoning issues in our subscriber
                // stack.
                filter_handle.modify(|filter| *filter = targets)?;
                Ok(())
            }),
        };
        (Some(layer), reloader)
    } else {
        let reloader = DynamicTargetsCallback {
            callback: Arc::new(move |_targets| {
                bail!("Tried to set targets for otel collector but there is no endpoint");
            }),
        };
        (None, reloader)
    };

    #[cfg(feature = "tokio-console")]
    let tokio_console_layer = if let Some(console_config) = config.tokio_console.clone() {
        let layer = ConsoleLayer::builder()
            .server_addr(console_config.listen_addr)
            .publish_interval(console_config.publish_interval)
            .retention(console_config.retention)
            .spawn();
        Some(layer)
    } else {
        None
    };

    let sentry_guard = if let Some(sentry_config) = config.sentry {
        let mut sentry_client_options = sentry::ClientOptions {
            attach_stacktrace: true,
            ..Default::default()
        };
        sentry_client_options.release = Some(Cow::Owned(build_version.to_string()));

        let guard = sentry::init((sentry_config.dsn, sentry_client_options));

        sentry::configure_scope(|scope| {
            scope.set_tag("service_name", service_name);
            scope.set_tag("build_sha", build_sha.to_string());
            scope.set_tag("build_time", build_time.to_string());
            for (k, v) in sentry_config.tags {
                scope.set_tag(&k, v);
            }
        });

        Some(guard)
    } else {
        None
    };

    let stack = tracing_subscriber::registry();
    let stack = stack.with(stderr_log_layer);
    let stack = stack.with(otel_layer);
    #[cfg(feature = "tokio-console")]
    let stack = stack.with(tokio_console_layer);
    let stack = stack.with(sentry_tracing::layer().event_filter(sentry_event_filter));
    stack.init();

    #[cfg(feature = "tokio-console")]
    if let Some(console_config) = config.tokio_console {
        tracing::info!(
            "starting tokio console on http://{}",
            console_config.listen_addr
        );
    }

    Ok((
        TracingTargetCallbacks {
            tracing: otel_reloader,
            stderr: stderr_callback,
        },
        sentry_guard,
    ))
}

/// Shutdown any tracing infra, if any.
pub fn shutdown() {
    opentelemetry::global::shutdown_tracer_provider();
}

/// Returns the level of a specific target from a [`Targets`].
pub fn target_level(targets: &Targets, target: &str) -> Level {
    if targets.would_enable(target, &Level::TRACE) {
        Level::TRACE
    } else if targets.would_enable(target, &Level::DEBUG) {
        Level::DEBUG
    } else if targets.would_enable(target, &Level::INFO) {
        Level::INFO
    } else if targets.would_enable(target, &Level::WARN) {
        Level::WARN
    } else {
        Level::ERROR
    }
}

/// A wrapper around a [`FormatEvent`] that adds an optional prefix to each
/// event.
#[derive(Debug)]
pub struct PrefixFormat<F> {
    inner: F,
    prefix: Option<String>,
}

impl<F, C, N> FormatEvent<C, N> for PrefixFormat<F>
where
    C: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<C, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, C, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> std::fmt::Result {
        match &self.prefix {
            None => self.inner.format_event(ctx, writer, event)?,
            Some(prefix) => {
                let style = ansi_term::Style::new();
                let target_style = if writer.has_ansi_escapes() {
                    style.bold()
                } else {
                    style
                };
                write!(
                    writer,
                    "{}{}:{} ",
                    target_style.prefix(),
                    prefix,
                    target_style.infix(style)
                )?;
                self.inner.format_event(ctx, writer, event)?;
            }
        }
        Ok(())
    }
}

/// An OpenTelemetry context.
///
/// Allows associating [`tracing`] spans across task or thread boundaries.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OpenTelemetryContext {
    inner: HashMap<String, String>,
}

impl OpenTelemetryContext {
    /// Attaches this `Context` to the current [`tracing`] span,
    /// as its parent.
    ///
    /// If there is not enough information in this `OpenTelemetryContext`
    /// to create a context, then the current thread's `Context` is used
    /// defaulting to the default `Context`.
    pub fn attach_as_parent(&self) {
        let parent_cx = global::get_text_map_propagator(|prop| prop.extract(&self.inner));
        tracing::Span::current().set_parent(parent_cx);
    }

    /// Obtains a `Context` from the current [`tracing`] span.
    pub fn obtain() -> Self {
        let mut map = std::collections::HashMap::new();
        global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&tracing::Span::current().context(), &mut map)
        });

        Self { inner: map }
    }

    /// Obtains an empty `Context`.
    pub fn empty() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }
}

impl Extractor for OpenTelemetryContext {
    fn get(&self, key: &str) -> Option<&str> {
        Extractor::get(&self.inner, key)
    }
    fn keys(&self) -> Vec<&str> {
        Extractor::keys(&self.inner)
    }
}

impl Injector for OpenTelemetryContext {
    fn set(&mut self, key: &str, value: String) {
        Injector::set(&mut self.inner, key, value)
    }
}

impl From<OpenTelemetryContext> for HashMap<String, String> {
    fn from(ctx: OpenTelemetryContext) -> Self {
        ctx.inner
    }
}

impl From<HashMap<String, String>> for OpenTelemetryContext {
    fn from(map: HashMap<String, String>) -> Self {
        Self { inner: map }
    }
}
