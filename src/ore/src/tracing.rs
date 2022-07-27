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

use std::collections::HashMap;
use std::io;
#[cfg(feature = "tokio-console")]
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
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
    pub opentelemetry: Option<OpenTelemetryConfig>,
    /// Optional configuration for the [Tokio console] integration.
    ///
    /// [Tokio console]: https://github.com/tokio-rs/console
    #[cfg_attr(nightly_doc_features, doc(cfg(feature = "tokio-console")))]
    #[cfg(feature = "tokio-console")]
    pub tokio_console: Option<TokioConsoleConfig>,
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
    /// A resource to include with each trace.
    pub resource: Resource,
    /// Whether to start with the dynamic OpenTelemetry layer enabled.
    ///
    /// OpenTelemetry can be dynamically enabled and disabled via the
    /// [`TracingHandle`] returned by [`configure`].
    pub enabled: bool,
}

/// A handle to the tracing stack that can be used to dynamically change a
/// subset of the tracing configuration.
#[derive(Clone)]
pub struct TracingHandle {
    set_opentelemetry_enabled: Arc<dyn Fn(bool) -> Result<(), anyhow::Error> + Send + Sync>,
    opentelemetry_enabled: Arc<AtomicBool>,
}

impl TracingHandle {
    /// Sets whether the OpenTelemetry layer is enabled.
    ///
    /// Returns an error if the tracing configuration provided to [`configure`]
    /// did not configure OpenTelemetry.
    pub fn set_opentelemetry_enabled(&self, enabled: bool) -> Result<(), anyhow::Error> {
        (self.set_opentelemetry_enabled)(enabled)?;

        // Note that this happens AFTER we reset the callback.
        // This is fine, as long as users of `current_enabled`
        // don't expect to read a fresh value before this
        // function returns.
        //
        // Note that the `Ordering`s are chosen using the same
        // logic as <https://github.com/tokio-rs/tracing/blob/2aa0cb010d8a7fa0de610413b5acd4557a00dd34/tracing-core/src/metadata.rs#L646-L694>
        // but could probably all be `Relaxed`, as we don't expect
        // any strict ordering guarantees with enabling/disabling
        // the OpenTelemetry collector
        self.opentelemetry_enabled.swap(enabled, Ordering::AcqRel);
        Ok(())
    }

    /// Reports whether the OpenTelemetry layer is enabled.
    pub fn opentelemetry_enabled(&self) -> bool {
        self.opentelemetry_enabled.load(Ordering::Relaxed)
    }

    /// Gracefully shuts down tracing.
    ///
    /// Calling this method ensures that any pending OpenTelemetry spans are
    /// flushed to the configured collector, if any.
    pub fn shutdown(&self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

impl std::fmt::Debug for TracingHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("OpenTelemetryHandle")
            .field("enabled", &self.opentelemetry_enabled())
            .finish_non_exhaustive()
    }
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
/// console via a server at the configured address.
///
/// [Jaeger]: https://jaegertracing.io
/// [Honeycomb]: https://www.honeycomb.io
/// [Tokio console]: https://github.com/tokio-rs/console
// Setting up OpenTelemetry in the background requires we are in a Tokio runtime
// context, hence the `async`.
#[allow(clippy::unused_async)]
pub async fn configure<C>(service_name: &str, config: C) -> Result<TracingHandle, anyhow::Error>
where
    C: Into<TracingConfig>,
{
    let service_name = service_name.to_string();

    let config = config.into();
    let stderr_log_layer = fmt::layer()
        .event_format(PrefixFormat {
            inner: format(),
            prefix: config.stderr_log.prefix,
        })
        .with_writer(io::stderr)
        .with_ansi(atty::is(atty::Stream::Stderr))
        .with_filter(config.stderr_log.filter);

    let (otel_layer, otel_reloader) = if let Some(otel_config) = config.opentelemetry {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        // Manually set up an OpenSSL-backed, h2, proxied `Channel`,
        // with the timeout configured according to:
        // https://docs.rs/opentelemetry-otlp/latest/opentelemetry_otlp/struct.TonicExporterBuilder.html#method.with_channel
        let channel = Endpoint::from_shared(otel_config.endpoint)?
            .timeout(Duration::from_secs(
                opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT,
            ))
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
                    Resource::new([KeyValue::new("service.name", service_name)])
                        .merge(&otel_config.resource),
                ),
            )
            .with_exporter(exporter)
            .install_batch(opentelemetry::runtime::Tokio)
            .unwrap();
        let (filter, filter_handle) = reload::Layer::new(if otel_config.enabled {
            otel_config.filter.clone()
        } else {
            // The default `Targets` disables all targets.
            Targets::default()
        });
        let layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(filter);
        let reloader = TracingHandle {
            set_opentelemetry_enabled: Arc::new(move |enable| {
                filter_handle.modify(|filter| {
                    // This code should be kept panic-free to
                    // avoid weird poisoning issues in our subscriber
                    // stack.
                    if enable {
                        *filter = otel_config.filter.clone()
                    } else {
                        *filter = Targets::default()
                    }
                })?;
                Ok(())
            }),
            opentelemetry_enabled: Arc::new(AtomicBool::new(otel_config.enabled)),
        };
        (Some(layer), reloader)
    } else {
        let reloader = TracingHandle {
            set_opentelemetry_enabled: Arc::new(move |_| {
                bail!("OpenTelemetry is not configured");
            }),
            opentelemetry_enabled: Arc::new(AtomicBool::new(false)),
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

    let stack = tracing_subscriber::registry();
    let stack = stack.with(stderr_log_layer);
    let stack = stack.with(otel_layer);
    #[cfg(feature = "tokio-console")]
    let stack = stack.with(tokio_console_layer);
    stack.init();

    #[cfg(feature = "tokio-console")]
    if let Some(console_config) = config.tokio_console {
        tracing::info!(
            "starting tokio console on http://{}",
            console_config.listen_addr
        );
    }

    Ok(otel_reloader)
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
